"""OCR workflow orchestration and background job helpers."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from illdashboard.config import settings
from illdashboard.copilot import extraction as copilot_extraction
from illdashboard.models import LabFile, Measurement
from illdashboard.services.ocr_ingestion import persist_ocr_result, persist_ocr_result_with_fresh_session


logger = logging.getLogger(__name__)

MAX_OCR_CONCURRENCY = 4
OCR_STREAM_KEEPALIVE_INTERVAL = 10
OCR_JOB_TTL_SECONDS = 600
FILE_STATUS_QUEUED = "queued"
FILE_STATUS_EXTRACTING = "extracting"
FILE_STATUS_EXTRACTED = "extracted"
FILE_STATUS_PERSISTING = "persisting"
FILE_STATUS_DONE = "done"
FILE_STATUS_ERROR = "error"


@dataclass
class OcrJobProgress:
    file_id: int
    filename: str
    index: int
    total: int
    status: str
    error: str | None = None


@dataclass
class OcrFileTiming:
    queued_at_monotonic: float = field(default_factory=time.perf_counter)
    extraction_started_at_monotonic: float | None = None
    extraction_finished_at_monotonic: float | None = None
    persist_started_at_monotonic: float | None = None
    finished_at_monotonic: float | None = None


@dataclass
class OcrJobState:
    job_id: str
    status: str
    total: int
    progress_by_file: dict[int, OcrJobProgress] = field(default_factory=dict)
    file_timings: dict[int, OcrFileTiming] = field(default_factory=dict)
    completed_count: int = 0
    error_count: int = 0
    created_at: float = field(default_factory=time.time)
    last_updated_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    task: asyncio.Task | None = None


@dataclass
class OcrExtractionOutcome:
    index: int
    lab: LabFile
    result: dict | None = None
    error: Exception | None = None


_ocr_jobs: dict[str, OcrJobState] = {}


def _prune_ocr_jobs(*, now: float | None = None) -> None:
    current_time = time.time() if now is None else now
    expired_job_ids = [
        job_id
        for job_id, job in _ocr_jobs.items()
        if current_time - job.last_updated_at >= OCR_JOB_TTL_SECONDS
    ]
    for job_id in expired_job_ids:
        _ocr_jobs.pop(job_id, None)


def _touch_job(job: OcrJobState, *, now: float | None = None) -> None:
    job.last_updated_at = time.time() if now is None else now


def _ensure_file_timing(job: OcrJobState, file_id: int) -> OcrFileTiming:
    timing = job.file_timings.get(file_id)
    if timing is None:
        timing = OcrFileTiming()
        job.file_timings[file_id] = timing
    return timing


def _elapsed_ms(started_at: float | None, finished_at: float | None = None) -> float | None:
    if started_at is None:
        return None
    end_time = time.perf_counter() if finished_at is None else finished_at
    return (end_time - started_at) * 1000


async def extract_ocr_result(lab: LabFile) -> dict:
    file_path = Path(settings.UPLOAD_DIR) / lab.filepath
    resolved_path = str(file_path.resolve())
    started_at = time.perf_counter()
    logger.info(
        "OCR extraction start file_id=%s filename=%s path=%s mime_type=%s",
        lab.id,
        lab.filename,
        resolved_path,
        lab.mime_type,
    )
    # The Copilot OCR pipeline keeps structured extraction separate from
    # free-form OCR + translation. That split is intentional and must remain.
    result = await copilot_extraction.ocr_extract(resolved_path, filename=lab.filename)
    logger.info(
        "OCR extraction finished file_id=%s filename=%s duration=%.2fs measurements=%s",
        lab.id,
        lab.filename,
        time.perf_counter() - started_at,
        len(result.get("measurements", [])),
    )
    return result


async def run_ocr_for_file(lab: LabFile, db: AsyncSession) -> list[Measurement]:
    result = await extract_ocr_result(lab)
    return await persist_ocr_result(lab, result, db)


def progress_payload(
    *,
    lab: LabFile,
    index: int,
    total: int,
    status: str,
    error: str | None = None,
) -> str:
    payload = {
        "type": "progress",
        "file_id": lab.id,
        "filename": lab.filename,
        "index": index,
        "total": total,
        "status": status,
    }
    if error is not None:
        payload["error"] = error
    return json.dumps(payload) + "\n"


def keepalive_payload() -> str:
    return json.dumps({"type": "keepalive"}) + "\n"


def _set_job_file_progress(
    job: OcrJobState,
    *,
    lab: LabFile,
    index: int,
    total: int,
    status: str,
    error: str | None = None,
    now: float | None = None,
) -> None:
    current_time = time.time() if now is None else now
    job.progress_by_file[lab.id] = OcrJobProgress(
        file_id=lab.id,
        filename=lab.filename,
        index=index,
        total=total,
        status=status,
        error=error,
    )
    _touch_job(job, now=current_time)


def _job_prefix(job_id: str | None) -> str:
    return f"job_id={job_id} " if job_id is not None else ""


async def _extract_ocr_outcome(
    *,
    index: int,
    lab: LabFile,
    total: int,
    semaphore: asyncio.Semaphore,
    job: OcrJobState | None = None,
    job_id: str | None = None,
) -> OcrExtractionOutcome:
    async with semaphore:
        started_at = time.perf_counter()
        prefix = _job_prefix(job_id)
        queue_wait_ms: float | None = None
        if job is not None:
            timing = _ensure_file_timing(job, lab.id)
            timing.extraction_started_at_monotonic = started_at
            queue_wait_ms = _elapsed_ms(timing.queued_at_monotonic, started_at)
            _set_job_file_progress(job, lab=lab, index=index, total=total, status=FILE_STATUS_EXTRACTING)
        logger.info(
            "%sOCR worker acquired file_id=%s filename=%s queue_index=%s/%s queue_wait_ms=%s",
            prefix,
            lab.id,
            lab.filename,
            index + 1,
            total,
            f"{queue_wait_ms:.1f}" if queue_wait_ms is not None else "n/a",
        )
        try:
            result = await extract_ocr_result(lab)
            finished_at = time.perf_counter()
            if job is not None:
                timing = _ensure_file_timing(job, lab.id)
                timing.extraction_finished_at_monotonic = finished_at
                _set_job_file_progress(job, lab=lab, index=index, total=total, status=FILE_STATUS_EXTRACTED)
            logger.info(
                "%sOCR worker completed file_id=%s filename=%s duration=%.2fs",
                prefix,
                lab.id,
                lab.filename,
                finished_at - started_at,
            )
            return OcrExtractionOutcome(index=index, lab=lab, result=result)
        except Exception as exc:
            if job is not None:
                timing = _ensure_file_timing(job, lab.id)
                timing.finished_at_monotonic = time.perf_counter()
                _set_job_file_progress(
                    job,
                    lab=lab,
                    index=index,
                    total=total,
                    status=FILE_STATUS_ERROR,
                    error=str(exc),
                )
            logger.exception(
                "%sOCR extraction failed file_id=%s filename=%r path=%r",
                prefix,
                lab.id,
                lab.filename,
                lab.filepath,
            )
            return OcrExtractionOutcome(index=index, lab=lab, error=exc)


def _create_extraction_tasks(
    labs: list[LabFile],
    *,
    total: int,
    semaphore: asyncio.Semaphore,
    job: OcrJobState | None = None,
    job_id: str | None = None,
) -> list[asyncio.Task[OcrExtractionOutcome]]:
    return [
        asyncio.create_task(
            _extract_ocr_outcome(
                index=index,
                lab=lab,
                total=total,
                semaphore=semaphore,
                job=job,
                job_id=job_id,
            )
        )
        for index, lab in enumerate(labs)
    ]


def _record_job_error(job: OcrJobState, *, lab: LabFile, index: int, total: int, error: Exception) -> None:
    timing = _ensure_file_timing(job, lab.id)
    if timing.finished_at_monotonic is None:
        timing.finished_at_monotonic = time.perf_counter()
    logger.warning(
        "OCR job extraction error job_id=%s file_id=%s filename=%s queue_index=%s/%s error=%s total_ms=%s",
        job.job_id,
        lab.id,
        lab.filename,
        index + 1,
        total,
        error,
        f"{_elapsed_ms(timing.queued_at_monotonic, timing.finished_at_monotonic):.1f}",
    )
    _set_job_file_progress(
        job,
        lab=lab,
        index=index,
        total=total,
        status=FILE_STATUS_ERROR,
        error=str(error),
    )
    job.error_count += 1


def _record_job_success(job: OcrJobState, *, lab: LabFile, index: int, total: int) -> None:
    timing = _ensure_file_timing(job, lab.id)
    timing.finished_at_monotonic = time.perf_counter()
    persist_ms = _elapsed_ms(timing.persist_started_at_monotonic, timing.finished_at_monotonic)
    total_ms = _elapsed_ms(timing.queued_at_monotonic, timing.finished_at_monotonic)
    logger.info(
        "OCR job file complete job_id=%s file_id=%s filename=%s queue_index=%s/%s persist_ms=%s total_ms=%s",
        job.job_id,
        lab.id,
        lab.filename,
        index + 1,
        total,
        f"{persist_ms:.1f}" if persist_ms is not None else "n/a",
        f"{total_ms:.1f}" if total_ms is not None else "n/a",
    )
    _set_job_file_progress(job, lab=lab, index=index, total=total, status=FILE_STATUS_DONE)
    job.completed_count += 1


def _job_status_payload(job: OcrJobState) -> dict:
    progress = [asdict(item) for item in sorted(job.progress_by_file.values(), key=lambda current: current.index)]
    return {
        "job_id": job.job_id,
        "status": job.status,
        "total": job.total,
        "completed_count": job.completed_count,
        "error_count": job.error_count,
        "last_updated_at": job.last_updated_at,
        "progress": progress,
    }


def get_ocr_job_status(job_id: str) -> dict:
    _prune_ocr_jobs()
    job = _ocr_jobs.get(job_id)
    if job is None:
        raise HTTPException(404, "OCR job not found")
    return _job_status_payload(job)


async def _run_ocr_job(job: OcrJobState, labs: list[LabFile], session_factory: async_sessionmaker[AsyncSession]) -> None:
    stream_started_at = time.perf_counter()
    total = len(labs)
    job.status = "running"
    job.started_at = time.time()
    _touch_job(job, now=job.started_at)
    logger.info(
        "Starting OCR job job_id=%s total_files=%s max_concurrency=%s file_order=%s",
        job.job_id,
        total,
        MAX_OCR_CONCURRENCY,
        [{"file_id": lab.id, "filename": lab.filename} for lab in labs],
    )

    semaphore = asyncio.Semaphore(MAX_OCR_CONCURRENCY)
    completed_extractions: dict[int, OcrExtractionOutcome] = {}
    next_index_to_persist = 0
    tasks = _create_extraction_tasks(labs, total=total, semaphore=semaphore, job=job, job_id=job.job_id)

    try:
        for future in asyncio.as_completed(tasks):
            outcome = await future
            completed_extractions[outcome.index] = outcome

            if outcome.error is None and outcome.index != next_index_to_persist:
                timing = _ensure_file_timing(job, outcome.lab.id)
                extracted_wait_ms = _elapsed_ms(timing.extraction_finished_at_monotonic)
                logger.info(
                    "OCR job file waiting for turn job_id=%s file_id=%s filename=%s queue_index=%s/%s next_index_to_persist=%s extracted_wait_ms=%s",
                    job.job_id,
                    outcome.lab.id,
                    outcome.lab.filename,
                    outcome.index + 1,
                    total,
                    next_index_to_persist + 1,
                    f"{extracted_wait_ms:.1f}" if extracted_wait_ms is not None else "n/a",
                )

            while next_index_to_persist in completed_extractions:
                pending = completed_extractions.pop(next_index_to_persist)

                if pending.error is not None:
                    _record_job_error(job, lab=pending.lab, index=next_index_to_persist, total=total, error=pending.error)
                    next_index_to_persist += 1
                    continue

                try:
                    assert pending.result is not None
                    timing = _ensure_file_timing(job, pending.lab.id)
                    timing.persist_started_at_monotonic = time.perf_counter()
                    extracted_wait_ms = _elapsed_ms(
                        timing.extraction_finished_at_monotonic,
                        timing.persist_started_at_monotonic,
                    )
                    _set_job_file_progress(
                        job,
                        lab=pending.lab,
                        index=next_index_to_persist,
                        total=total,
                        status=FILE_STATUS_PERSISTING,
                    )
                    logger.info(
                        "OCR job file persisting job_id=%s file_id=%s filename=%s queue_index=%s/%s extracted_wait_ms=%s",
                        job.job_id,
                        pending.lab.id,
                        pending.lab.filename,
                        next_index_to_persist + 1,
                        total,
                        f"{extracted_wait_ms:.1f}" if extracted_wait_ms is not None else "n/a",
                    )
                    await persist_ocr_result_with_fresh_session(
                        pending.lab.id,
                        pending.result,
                        session_factory,
                        job_id=job.job_id,
                    )
                    _record_job_success(job, lab=pending.lab, index=next_index_to_persist, total=total)
                except Exception as exc:
                    timing = _ensure_file_timing(job, pending.lab.id)
                    timing.finished_at_monotonic = time.perf_counter()
                    logger.exception(
                        "Persisting OCR result failed for job_id=%s file id=%s filename=%r path=%r",
                        job.job_id,
                        pending.lab.id,
                        pending.lab.filename,
                        pending.lab.filepath,
                    )
                    _set_job_file_progress(
                        job,
                        lab=pending.lab,
                        index=next_index_to_persist,
                        total=total,
                        status=FILE_STATUS_ERROR,
                        error=str(exc),
                    )
                    job.error_count += 1

                next_index_to_persist += 1

        job.status = "completed"
        job.finished_at = time.time()
        _touch_job(job, now=job.finished_at)
        logger.info(
            "OCR job complete job_id=%s total_files=%s completed=%s errors=%s duration=%.2fs",
            job.job_id,
            total,
            job.completed_count,
            job.error_count,
            time.perf_counter() - stream_started_at,
        )
    except Exception:
        job.status = "failed"
        job.finished_at = time.time()
        _touch_job(job, now=job.finished_at)
        logger.exception("OCR job failed job_id=%s", job.job_id)
        raise


def start_ocr_job(labs: list[LabFile], session_factory: async_sessionmaker[AsyncSession]) -> dict:
    _prune_ocr_jobs()
    job_id = uuid.uuid4().hex
    job = OcrJobState(job_id=job_id, status="queued", total=len(labs))
    _ocr_jobs[job_id] = job

    if not labs:
        job.status = "completed"
        job.started_at = time.time()
        job.finished_at = time.time()
        _touch_job(job, now=job.finished_at)
        return _job_status_payload(job)

    for index, lab in enumerate(labs):
        _ensure_file_timing(job, lab.id)
        _set_job_file_progress(job, lab=lab, index=index, total=len(labs), status=FILE_STATUS_QUEUED)

    job.task = asyncio.create_task(_run_ocr_job(job, labs, session_factory))
    return _job_status_payload(job)


async def stream_ocr_for_labs(labs: list[LabFile], db: AsyncSession):
    if not labs:
        yield json.dumps({"type": "complete"}) + "\n"
        return

    stream_started_at = time.perf_counter()
    total = len(labs)
    logger.info("Starting OCR stream total_files=%s file_ids=%s", total, [lab.id for lab in labs])
    for index, lab in enumerate(labs):
        yield progress_payload(lab=lab, index=index, total=total, status=FILE_STATUS_QUEUED)

    semaphore = asyncio.Semaphore(MAX_OCR_CONCURRENCY)
    tasks = _create_extraction_tasks(labs, total=total, semaphore=semaphore)

    pending = set(tasks)
    while pending:
        done, pending = await asyncio.wait(
            pending,
            timeout=OCR_STREAM_KEEPALIVE_INTERVAL,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done:
            logger.info(
                "OCR stream keepalive pending=%s elapsed=%.2fs",
                len(pending),
                time.perf_counter() - stream_started_at,
            )
            yield keepalive_payload()
            continue

        for future in done:
            outcome = await future
            if outcome.error is not None:
                logger.warning(
                    "OCR stream extraction error file_id=%s filename=%s queue_index=%s/%s error=%s",
                    outcome.lab.id,
                    outcome.lab.filename,
                    outcome.index + 1,
                    total,
                    outcome.error,
                )
                yield progress_payload(
                    lab=outcome.lab,
                    index=outcome.index,
                    total=total,
                    status=FILE_STATUS_ERROR,
                    error=str(outcome.error),
                )
                continue

            assert outcome.result is not None
            try:
                yield progress_payload(lab=outcome.lab, index=outcome.index, total=total, status=FILE_STATUS_EXTRACTED)
                yield progress_payload(lab=outcome.lab, index=outcome.index, total=total, status=FILE_STATUS_PERSISTING)
                await persist_ocr_result(outcome.lab, outcome.result, db)
                await db.commit()
                logger.info(
                    "OCR stream file complete file_id=%s filename=%s queue_index=%s/%s",
                    outcome.lab.id,
                    outcome.lab.filename,
                    outcome.index + 1,
                    total,
                )
                yield progress_payload(lab=outcome.lab, index=outcome.index, total=total, status=FILE_STATUS_DONE)
            except Exception as exc:
                await db.rollback()
                logger.exception(
                    "Persisting OCR result failed for file id=%s filename=%r path=%r",
                    outcome.lab.id,
                    outcome.lab.filename,
                    outcome.lab.filepath,
                )
                yield progress_payload(lab=outcome.lab, index=outcome.index, total=total, status=FILE_STATUS_ERROR, error=str(exc))

    logger.info("OCR stream complete total_files=%s duration=%.2fs", total, time.perf_counter() - stream_started_at)
    yield json.dumps({"type": "complete"}) + "\n"


async def load_labs_for_ocr(
    db: AsyncSession,
    *,
    file_ids: list[int] | None = None,
    only_unprocessed: bool = False,
) -> list[LabFile]:
    if file_ids is not None:
        labs: list[LabFile] = []
        seen_file_ids: set[int] = set()
        for file_id in file_ids:
            if file_id in seen_file_ids:
                continue
            seen_file_ids.add(file_id)
            lab = await db.get(LabFile, file_id)
            if not lab:
                raise HTTPException(404, f"File {file_id} not found")
            labs.append(lab)
        return labs

    if only_unprocessed:
        result = await db.execute(select(LabFile).where(LabFile.ocr_raw.is_(None)).order_by(LabFile.id.asc()))
        return list(result.scalars().all())

    return []
