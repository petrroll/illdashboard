from __future__ import annotations

import asyncio
import json
import logging
import math
import mimetypes
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar

import fitz
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from illdashboard.config import settings
from illdashboard.copilot import extraction as copilot_extraction
from illdashboard.copilot import normalization as copilot_normalization
from illdashboard.copilot.client import COPILOT_REQUEST_TIMEOUT, get_copilot_request_load, shutdown_client
from illdashboard.models import (
    DEFAULT_GROUP_NAME,
    READY_FILE_STATUS,
    UPLOADED_FILE_STATUS,
    Job,
    LabFile,
    LabFileTag,
    MarkerGroup,
    Measurement,
    MeasurementType,
    QualitativeRule,
    utc_now,
)
from illdashboard.services import jobs as job_service
from illdashboard.services import qualitative_values, rescaling
from illdashboard.services import search as search_service
from illdashboard.services.markers import (
    SOURCE_TAG_PREFIX,
    build_source_tag,
    ensure_marker_groups,
    ensure_measurement_type_aliases,
    load_group_order,
    load_measurement_type_aliases,
    normalize_marker_alias_key,
    normalize_source_tag_value,
    source_tag_value,
)

logger = logging.getLogger(__name__)

FILE_STATUS_UPLOADED = UPLOADED_FILE_STATUS
FILE_STATUS_QUEUED = "queued"
FILE_STATUS_PROCESSING = "processing"
FILE_STATUS_ERROR = "error"
FILE_STAGE_QUEUED = "queued"
FILE_STAGE_RUNNING = "running"
FILE_STAGE_DONE = "done"
FILE_STAGE_ERROR = "error"
MEASUREMENT_STATE_PENDING = "pending"
MEASUREMENT_STATE_RESOLVED = "resolved"
MEASUREMENT_STATE_ERROR = "error"

TASK_EXTRACT_MEASUREMENT = "extract.measurement"
TASK_EXTRACT_TEXT = "extract.text"
TASK_RECONCILE_FILE = "file.reconcile"
TASK_GENERATE_SUMMARY = "file.summary"
TASK_PUBLISH_FILE = "file.publish"
TASK_NORMALIZE_MARKER = "normalize.measurement_type"
TASK_NORMALIZE_GROUP = "normalize.measurement_group"
TASK_NORMALIZE_CANONICAL_UNIT = "normalize.canonical_unit"
TASK_NORMALIZE_UNIT_CONVERSION = "normalize.unit_conversion"
TASK_NORMALIZE_QUALITATIVE = "normalize.qualitative_value"
TASK_NORMALIZE_SOURCE = "normalize.source"

# Earlier 300s validation forced durable OCR jobs to stay per-page. With the
# longer request budget and the retry/split path still in place, queue two-page
# chunks by default to reduce request overhead without losing the fallback.
MEASUREMENT_BATCH_SIZE = 2
DEFAULT_OCR_DPI = 144
MIN_OCR_DPI = 96
MAX_JOB_ATTEMPTS = 3
# Durable workers hold a lease for the lifetime of a single Copilot request, so
# keep the lease aligned with the maximum request timeout to avoid mid-flight
# reclaims of still-running jobs.
JOB_LEASE_SECONDS = COPILOT_REQUEST_TIMEOUT
WORKER_IDLE_SECONDS = 1.0
MEASUREMENT_EXTRACT_WORKER_CONCURRENCY = 4
TEXT_EXTRACT_WORKER_CONCURRENCY = 2
TEXT_BATCH_SIZE = 2

PRIORITY_RECONCILE = 5
PRIORITY_MEASUREMENT_EXTRACT = 10
PRIORITY_NORMALIZE = 20
PRIORITY_TEXT_EXTRACT = 60
PRIORITY_SUMMARY = 70
PRIORITY_PUBLISH = 80

_runtime: PipelineRuntime | None = None
_runtime_reset_lock = asyncio.Lock()
_T = TypeVar("_T")

_PRELOADABLE_MIME_TYPES: dict[str, str] = {
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
}


async def preload_uploaded_files(session: AsyncSession) -> int:
    """Seed LabFile rows for files on disk that are missing from the database.

    This reconciles the upload folder against the DB at startup so that
    manually placed files (or files surviving a DB rebuild) get picked up
    by the normal processing pipeline.
    """
    upload_dir = Path(settings.UPLOAD_DIR)
    if not upload_dir.is_dir():
        return 0

    result = await session.execute(select(LabFile.filepath))
    known_paths = {Path(p).resolve() for p in result.scalars().all()}

    added = 0
    for file_path in sorted(p for p in upload_dir.iterdir() if p.is_file()):
        if file_path.resolve() in known_paths:
            continue

        mime_type = _PRELOADABLE_MIME_TYPES.get(file_path.suffix.lower())
        if mime_type is None:
            guessed, _ = mimetypes.guess_type(file_path.name)
            if guessed not in _PRELOADABLE_MIME_TYPES.values():
                continue
            mime_type = guessed

        page_count = 1
        if mime_type == "application/pdf":
            try:
                with fitz.open(str(file_path)) as doc:
                    page_count = doc.page_count
            except Exception:
                logger.warning("Could not read page count for %s, defaulting to 1", file_path.name)

        session.add(
            LabFile(
                filename=file_path.name,
                filepath=str(file_path.resolve()),
                mime_type=mime_type,
                page_count=page_count,
            )
        )
        known_paths.add(file_path.resolve())
        added += 1

    if added:
        await session.commit()
        logger.info("Preloaded %d files from upload folder", added)

    return added


class PipelineRuntime:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory
        self.runtime_id = uuid.uuid4().hex[:12]
        self.stop_event = asyncio.Event()
        self.tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        async with self.session_factory() as session:
            await ensure_marker_groups(session)
            await search_service.ensure_search_schema(session)
            await session.commit()

        async with self.session_factory() as session:
            await job_service.prune_jobs(session)

        # Seed DB rows for files on disk not yet tracked in the database.
        async with self.session_factory() as session:
            await preload_uploaded_files(session)

        async with self.session_factory() as session:
            result = await session.execute(select(LabFile).where(LabFile.status != READY_FILE_STATUS))
            for file in result.scalars().all():
                if _file_is_pristine(file):
                    if file.status == FILE_STATUS_QUEUED:
                        file.status = FILE_STATUS_UPLOADED
                    continue
                await enqueue_file_reconcile(session, file.id)
            await session.commit()

        # Scale durable OCR workers to match the wider extraction lane, but rely
        # on Copilot lane reservations in the client so extraction does not
        # starve normalization and summary work.
        self._spawn_workers(
            "measurement-extract",
            [TASK_EXTRACT_MEASUREMENT],
            1,
            MEASUREMENT_EXTRACT_WORKER_CONCURRENCY,
            self._handle_measurement_jobs,
        )
        self._spawn_workers(
            "text-extract",
            [TASK_EXTRACT_TEXT],
            1,
            TEXT_EXTRACT_WORKER_CONCURRENCY,
            self._handle_text_jobs,
        )
        self._spawn_workers("reconcile", [TASK_RECONCILE_FILE], 1, 2, self._handle_reconcile_jobs)
        self._spawn_workers("summary", [TASK_GENERATE_SUMMARY], 1, 1, self._handle_summary_jobs)
        self._spawn_workers("publish", [TASK_PUBLISH_FILE], 1, 1, self._handle_publish_jobs)
        self._spawn_workers("normalize-source", [TASK_NORMALIZE_SOURCE], 8, 2, self._handle_source_jobs)
        self._spawn_workers(
            "normalize-marker",
            [TASK_NORMALIZE_MARKER],
            copilot_normalization.MARKER_NORMALIZATION_BATCH_SIZE,
            1,
            self._handle_marker_jobs,
        )
        self._spawn_workers(
            "normalize-group",
            [TASK_NORMALIZE_GROUP],
            copilot_normalization.MARKER_GROUP_CLASSIFICATION_BATCH_SIZE,
            1,
            self._handle_group_jobs,
        )
        self._spawn_workers(
            "normalize-unit",
            [TASK_NORMALIZE_CANONICAL_UNIT],
            copilot_normalization.UNIT_NORMALIZATION_BATCH_SIZE,
            1,
            self._handle_canonical_unit_jobs,
        )
        self._spawn_workers(
            "normalize-conversion",
            [TASK_NORMALIZE_UNIT_CONVERSION],
            copilot_normalization.UNIT_NORMALIZATION_BATCH_SIZE,
            1,
            self._handle_conversion_jobs,
        )
        self._spawn_workers(
            "normalize-qualitative",
            [TASK_NORMALIZE_QUALITATIVE],
            copilot_normalization.QUALITATIVE_NORMALIZATION_BATCH_SIZE,
            1,
            self._handle_qualitative_jobs,
        )

    async def stop(self, *, abort_copilot_requests: bool = False) -> None:
        self.stop_event.set()
        for task in self.tasks:
            task.cancel()
        if abort_copilot_requests:
            # Clean reruns need a hard cut-over. Some Copilot SDK calls do not
            # unwind on task cancellation alone, so closing the shared client
            # forces those requests to exit before we delete and recreate jobs.
            await shutdown_client()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

    def _spawn_workers(
        self,
        worker_name: str,
        task_types: list[str],
        claim_limit: int,
        concurrency: int,
        handler,
    ) -> None:
        for worker_index in range(concurrency):
            task = asyncio.create_task(
                self._worker_loop(
                    f"{worker_name}:{worker_index}",
                    task_types,
                    claim_limit,
                    handler,
                ),
                name=f"pipeline:{worker_name}:{worker_index}",
            )
            self.tasks.append(task)

    async def _worker_loop(self, worker_name: str, task_types: list[str], claim_limit: int, handler) -> None:
        lease_owner = f"{self.runtime_id}:{worker_name}"
        while not self.stop_event.is_set():
            try:
                async with self.session_factory() as session:
                    jobs = await job_service.claim_jobs(
                        session,
                        task_types=task_types,
                        lease_owner=lease_owner,
                        limit=claim_limit,
                        lease_seconds=JOB_LEASE_SECONDS,
                    )
                    if not jobs:
                        await asyncio.sleep(WORKER_IDLE_SECONDS)
                        continue

                    try:
                        await handler(session, jobs)
                        await session.commit()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.exception("Worker failed worker=%s task_types=%s", worker_name, task_types)
                        await session.rollback()
                        await self._handle_worker_failure(session, jobs, exc)
                        await session.commit()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Worker loop crashed worker=%s task_types=%s", worker_name, task_types)
                await asyncio.sleep(WORKER_IDLE_SECONDS)

    async def _handle_worker_failure(self, session: AsyncSession, jobs: list[Job], exc: Exception) -> None:
        for job in jobs:
            refreshed = await session.get(Job, job.id)
            if refreshed is None or refreshed.status != job_service.JOB_STATUS_LEASED:
                continue
            if refreshed.attempt_count < MAX_JOB_ATTEMPTS:
                await job_service.release_job(session, refreshed, delay_seconds=15, error_text=str(exc))
            else:
                await job_service.mark_job_failed(session, refreshed, error_text=str(exc))

    async def _handle_measurement_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _process_extraction_job(session, jobs[0], measurement_mode=True)

    async def _handle_text_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _process_extraction_job(session, jobs[0], measurement_mode=False)

    async def _handle_reconcile_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _reconcile_file(session, jobs[0])

    async def _handle_summary_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _generate_file_summary(session, jobs[0])

    async def _handle_publish_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _publish_file(session, jobs[0])

    async def _handle_source_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _resolve_source_jobs(session, jobs)

    async def _handle_marker_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _resolve_marker_jobs(session, jobs)

    async def _handle_group_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _resolve_group_jobs(session, jobs)

    async def _handle_canonical_unit_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _resolve_canonical_unit_jobs(session, jobs)

    async def _handle_conversion_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _resolve_conversion_jobs(session, jobs)

    async def _handle_qualitative_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _resolve_qualitative_jobs(session, jobs)


async def start_pipeline_runtime(session_factory: async_sessionmaker[AsyncSession]) -> None:
    global _runtime
    if _runtime is not None:
        return
    runtime = PipelineRuntime(session_factory)
    await runtime.start()
    _runtime = runtime


async def stop_pipeline_runtime(*, abort_copilot_requests: bool = False) -> None:
    global _runtime
    if _runtime is None:
        return
    await _runtime.stop(abort_copilot_requests=abort_copilot_requests)
    _runtime = None


async def queue_file(session: AsyncSession, file_id: int) -> LabFile:
    result = await session.execute(select(LabFile).options(selectinload(LabFile.tags)).where(LabFile.id == file_id))
    file = result.scalar_one_or_none()
    if file is None:
        raise ValueError(f"Unknown file id {file_id}")

    await job_service.delete_jobs_for_file(session, file.id)
    await _reset_file_processing_state(session, file, file_status=FILE_STATUS_QUEUED)
    await _enqueue_file_extraction_jobs(session, file)
    _refresh_file_status(file)
    await session.flush()
    return file


async def queue_files(session: AsyncSession, file_ids: list[int]) -> list[int]:
    if not file_ids:
        return []
    await reset_incomplete_processing(session)
    return await _queue_selected_files(session, file_ids)


async def _queue_selected_files(session: AsyncSession, file_ids: list[int]) -> list[int]:
    queued: list[int] = []
    for file_id in dict.fromkeys(file_ids):
        file = await queue_file(session, file_id)
        queued.append(file.id)
    await session.commit()
    return queued


async def queue_unprocessed_files(session: AsyncSession) -> list[int]:
    await reset_incomplete_processing(session)
    result = await session.execute(
        select(LabFile.id)
        .where(LabFile.status.in_([FILE_STATUS_UPLOADED, FILE_STATUS_ERROR]))
        .order_by(LabFile.uploaded_at.asc())
    )
    file_ids = result.scalars().all()
    if not file_ids:
        await session.commit()
        return []
    return await _queue_selected_files(session, file_ids)


async def queue_files_from_clean_runtime(session: AsyncSession, file_ids: list[int]) -> list[int]:
    return await _run_with_clean_runtime(session, lambda current_session: queue_files(current_session, file_ids))


async def queue_unprocessed_files_from_clean_runtime(session: AsyncSession) -> list[int]:
    return await _run_with_clean_runtime(session, queue_unprocessed_files)


async def cancel_processing(session: AsyncSession) -> None:
    await reset_incomplete_processing(session)
    await session.commit()


async def cancel_processing_from_clean_runtime(session: AsyncSession) -> None:
    await _run_with_clean_runtime(session, cancel_processing)


async def _run_with_clean_runtime(
    session: AsyncSession,
    operation: Callable[[AsyncSession], Awaitable[_T]],
) -> _T:
    async with _runtime_reset_lock:
        session_factory = _runtime.session_factory if _runtime is not None else None
        runtime_was_running = session_factory is not None
        if runtime_was_running:
            await stop_pipeline_runtime(abort_copilot_requests=True)
        try:
            return await operation(session)
        except Exception:
            await session.rollback()
            raise
        finally:
            if runtime_was_running and session_factory is not None:
                await start_pipeline_runtime(session_factory)


async def reset_incomplete_processing(session: AsyncSession) -> None:
    await job_service.delete_all_jobs(session)
    result = await session.execute(
        select(LabFile).options(selectinload(LabFile.tags)).where(LabFile.status != READY_FILE_STATUS)
    )
    for file in result.scalars().unique().all():
        await _reset_file_processing_state(session, file, file_status=FILE_STATUS_UPLOADED)


async def _reset_file_processing_state(session: AsyncSession, file: LabFile, *, file_status: str) -> None:
    await session.execute(delete(Measurement).where(Measurement.lab_file_id == file.id))
    await search_service.remove_lab_search_document(file.id, session)

    if file.tags:
        for tag in list(file.tags):
            if tag.tag.casefold().startswith(SOURCE_TAG_PREFIX):
                await session.delete(tag)

    # Leave stage columns in their initial queued state, but let the caller pick
    # whether the row should appear as freshly uploaded or already queued for a
    # new run in the UI.
    file.status = file_status
    file.measurement_status = FILE_STAGE_QUEUED
    file.normalization_status = FILE_STAGE_QUEUED
    file.text_status = FILE_STAGE_QUEUED
    file.summary_status = FILE_STAGE_QUEUED
    file.publish_status = FILE_STAGE_QUEUED
    file.processing_error = None
    file.source_name = None
    file.ocr_raw = None
    file.ocr_text_raw = None
    file.ocr_text_english = None
    file.ocr_summary_english = None
    file.published_at = None


async def _enqueue_file_extraction_jobs(session: AsyncSession, file: LabFile) -> None:
    page_count = max(1, file.page_count)
    for batch_index, (start_page, stop_page) in enumerate(
        copilot_extraction.build_page_ranges(page_count, MEASUREMENT_BATCH_SIZE)
    ):
        await job_service.enqueue_job(
            session,
            task_type=TASK_EXTRACT_MEASUREMENT,
            task_key=_batch_task_key("measurement", file.id, start_page, stop_page, DEFAULT_OCR_DPI),
            payload={
                "file_id": file.id,
                "batch_index": batch_index,
                "start_page": start_page,
                "stop_page": stop_page,
                "dpi": DEFAULT_OCR_DPI,
            },
            file_id=file.id,
            priority=PRIORITY_MEASUREMENT_EXTRACT,
        )

async def enqueue_file_reconcile(session: AsyncSession, file_id: int) -> None:
    await job_service.enqueue_job(
        session,
        task_type=TASK_RECONCILE_FILE,
        task_key=f"file:{file_id}",
        payload={"file_id": file_id},
        file_id=file_id,
        priority=PRIORITY_RECONCILE,
        replace_existing=False,
    )


def _batch_task_key(kind: str, file_id: int, start_page: int, stop_page: int, dpi: int) -> str:
    return f"file:{file_id}:{kind}:{start_page}:{stop_page}:{dpi}"


def _marker_task_key(marker_key: str) -> str:
    return marker_key


def _group_task_key(normalized_key: str) -> str:
    return normalized_key


def _canonical_unit_task_key(normalized_key: str) -> str:
    return normalized_key


def _conversion_task_key(measurement_type_key: str, original_unit_key: str, canonical_unit_key: str) -> str:
    return f"{measurement_type_key}|{original_unit_key}|{canonical_unit_key}"


def _qualitative_task_key(value_key: str) -> str:
    return value_key


def _refresh_file_status(file: LabFile) -> None:
    stages = [
        file.measurement_status,
        file.normalization_status,
        file.text_status,
        file.summary_status,
        file.publish_status,
    ]
    if any(stage == FILE_STAGE_ERROR for stage in stages):
        file.status = FILE_STATUS_ERROR
        return
    if file.publish_status == FILE_STAGE_DONE:
        file.status = READY_FILE_STATUS
        return
    if all(stage == FILE_STAGE_QUEUED for stage in stages):
        file.status = FILE_STATUS_QUEUED
        return
    file.status = FILE_STATUS_PROCESSING


def _file_is_pristine(file: LabFile) -> bool:
    return (
        file.status in {FILE_STATUS_UPLOADED, FILE_STATUS_QUEUED}
        and file.measurement_status == FILE_STAGE_QUEUED
        and file.normalization_status == FILE_STAGE_QUEUED
        and file.text_status == FILE_STAGE_QUEUED
        and file.summary_status == FILE_STAGE_QUEUED
        and file.publish_status == FILE_STAGE_QUEUED
        and file.processing_error is None
        and file.source_name is None
        and file.ocr_raw is None
        and file.ocr_text_raw is None
        and file.ocr_text_english is None
        and file.ocr_summary_english is None
        and file.published_at is None
    )


async def _process_extraction_job(session: AsyncSession, job: Job, *, measurement_mode: bool) -> None:
    payload = job_service.json_loads(job.payload_json)
    file_id = int(payload.get("file_id", 0))
    file = await session.get(LabFile, file_id, options=[selectinload(LabFile.tags)])
    if file is None:
        await job_service.delete_job(session, job)
        return

    stage_name = "measurement" if measurement_mode else "text"
    started_at = time.perf_counter()
    if measurement_mode:
        file.measurement_status = FILE_STAGE_RUNNING
    else:
        file.text_status = FILE_STAGE_RUNNING
    _refresh_file_status(file)
    await session.commit()

    start_page = int(payload.get("start_page", 0))
    stop_page = int(payload.get("stop_page", max(1, file.page_count)))
    dpi = int(payload.get("dpi", DEFAULT_OCR_DPI))
    file_path = Path(file.filepath)
    queued_requests, active_requests = get_copilot_request_load()
    logger.info(
        "Extraction job start stage=%s job_id=%s file_id=%s filename=%s pages=%s-%s dpi=%s attempts=%s "
        "queued_requests=%s active_requests=%s",
        stage_name,
        job.id,
        file.id,
        file.filename,
        start_page + 1,
        stop_page,
        dpi,
        job.attempt_count,
        queued_requests,
        active_requests,
    )

    try:
        if measurement_mode:
            result = await copilot_extraction.extract_measurement_batch(
                str(file_path),
                start_page=start_page,
                stop_page=stop_page,
                dpi=dpi,
                filename=file.filename,
            )
        else:
            result = await copilot_extraction.extract_text_batch(
                str(file_path),
                start_page=start_page,
                stop_page=stop_page,
                dpi=dpi,
                filename=file.filename,
            )
    except Exception as exc:
        if measurement_mode and copilot_extraction.is_retryable_batch_error(exc):
            fallback_ranges = _fallback_batch_ranges(start_page, stop_page, dpi)
            if fallback_ranges:
                logger.warning(
                    "Extraction job retry split stage=%s job_id=%s file_id=%s filename=%s pages=%s-%s dpi=%s "
                    "fallback_ranges=%s duration=%.2fs error=%s",
                    stage_name,
                    job.id,
                    file.id,
                    file.filename,
                    start_page + 1,
                    stop_page,
                    dpi,
                    [
                        (fallback_start + 1, fallback_stop, fallback_dpi)
                        for fallback_start, fallback_stop, fallback_dpi in fallback_ranges
                    ],
                    time.perf_counter() - started_at,
                    exc,
                )
                for index, (fallback_start, fallback_stop, fallback_dpi) in enumerate(fallback_ranges):
                    await job_service.enqueue_job(
                        session,
                        task_type=job.task_type,
                        task_key=_batch_task_key(
                            stage_name,
                            file.id,
                            fallback_start,
                            fallback_stop,
                            fallback_dpi,
                        ),
                        payload={
                            "file_id": file.id,
                            "batch_index": int(payload.get("batch_index", 0)) * 10 + index,
                            "start_page": fallback_start,
                            "stop_page": fallback_stop,
                            "dpi": fallback_dpi,
                        },
                        file_id=file.id,
                        priority=job.priority,
                    )
                await job_service.delete_job(session, job)
                return
        logger.error(
            "Extraction job failed stage=%s job_id=%s file_id=%s filename=%s "
            "pages=%s-%s dpi=%s duration=%.2fs error=%s",
            stage_name,
            job.id,
            file.id,
            file.filename,
            start_page + 1,
            stop_page,
            dpi,
            time.perf_counter() - started_at,
            exc,
        )
        if measurement_mode:
            file.measurement_status = FILE_STAGE_ERROR
        else:
            file.text_status = FILE_STAGE_ERROR
            file.summary_status = FILE_STAGE_ERROR
        file.processing_error = str(exc)
        _refresh_file_status(file)
        await job_service.mark_job_failed(session, job, error_text=str(exc))
        return

    if measurement_mode:
        await _persist_measurement_batch(session, file, job, result)
        await job_service.delete_job(session, job)
        logger.info(
            "Extraction job finished stage=%s job_id=%s file_id=%s filename=%s "
            "pages=%s-%s dpi=%s duration=%.2fs measurements=%s",
            stage_name,
            job.id,
            file.id,
            file.filename,
            start_page + 1,
            stop_page,
            dpi,
            time.perf_counter() - started_at,
            len(result.get("measurements", [])),
        )
    else:
        await job_service.mark_job_resolved(session, job, payload=result)
        logger.info(
            "Extraction job finished stage=%s job_id=%s file_id=%s filename=%s pages=%s-%s dpi=%s duration=%.2fs "
            "raw_text_chars=%s translated_text_chars=%s",
            stage_name,
            job.id,
            file.id,
            file.filename,
            start_page + 1,
            stop_page,
            dpi,
            time.perf_counter() - started_at,
            len(str(result.get("raw_text") or "")),
            len(str(result.get("translated_text_english") or "")),
        )
    await enqueue_file_reconcile(session, file.id)


async def _persist_measurement_batch(session: AsyncSession, file: LabFile, job: Job, result: dict) -> None:
    await session.execute(
        delete(Measurement).where(
            Measurement.lab_file_id == file.id,
            Measurement.batch_key == job.task_key,
        )
    )

    parsed_lab_date = _parse_datetime(result.get("lab_date"))
    if parsed_lab_date is not None:
        file.lab_date = parsed_lab_date

    raw_source = result.get("source")
    if isinstance(raw_source, str) and raw_source.strip() and not file.source_name:
        file.source_name = raw_source.strip()
        await job_service.enqueue_job(
            session,
            task_type=TASK_NORMALIZE_SOURCE,
            task_key=f"file:{file.id}",
            payload={"file_id": file.id},
            file_id=file.id,
            priority=PRIORITY_NORMALIZE,
            replace_existing=True,
        )

    for raw_measurement in result.get("measurements", []):
        if not isinstance(raw_measurement, dict):
            continue
        raw_name = str(raw_measurement.get("marker_name") or "").strip()
        normalized_key = normalize_marker_alias_key(raw_name)
        if not raw_name or not normalized_key:
            continue

        numeric_value, qualitative_value = _parse_measurement_value(raw_measurement.get("value"))
        original_unit = _normalize_optional_text(raw_measurement.get("unit"))
        measured_at = _parse_datetime(raw_measurement.get("measured_at")) or file.lab_date
        page_number = _parse_int(raw_measurement.get("page_number"))

        session.add(
            Measurement(
                lab_file_id=file.id,
                raw_marker_name=raw_name,
                normalized_marker_key=normalized_key,
                original_value=numeric_value,
                original_qualitative_value=qualitative_value,
                original_unit=original_unit,
                normalized_original_unit=rescaling.normalize_unit_key(original_unit),
                original_reference_low=_parse_numeric_value(raw_measurement.get("reference_low")),
                original_reference_high=_parse_numeric_value(raw_measurement.get("reference_high")),
                measured_at=measured_at,
                page_number=page_number,
                batch_key=job.task_key,
                normalization_status=MEASUREMENT_STATE_PENDING,
            )
        )


async def _set_file_source_tag(session: AsyncSession, file: LabFile, source_name: str) -> None:
    source_tag = build_source_tag(source_name)
    if source_tag is None:
        return
    existing_tags = file.tags if file.tags else []
    if any(tag.tag == source_tag for tag in existing_tags):
        return
    for tag in list(existing_tags):
        if tag.tag.casefold().startswith(SOURCE_TAG_PREFIX):
            await session.delete(tag)
    session.add(LabFileTag(lab_file_id=file.id, tag=source_tag))


async def _reconcile_file(session: AsyncSession, job: Job) -> None:
    payload = job_service.json_loads(job.payload_json)
    file_id = int(payload.get("file_id", 0))
    file = await session.get(LabFile, file_id, options=[selectinload(LabFile.tags)])
    if file is None:
        await job_service.delete_job(session, job)
        return

    measurements_result = await session.execute(
        select(Measurement)
        .options(selectinload(Measurement.measurement_type))
        .where(Measurement.lab_file_id == file.id)
        .order_by(Measurement.id.asc())
    )
    measurements = list(measurements_result.scalars().all())

    await _merge_resolved_text_jobs(session, file)
    await _apply_measurement_normalization(session, file, measurements)
    await _refresh_file_stages(session, file, measurements)

    if _file_ready_for_text_extraction(file):
        await _enqueue_text_extraction_jobs(session, file)

    if _file_ready_for_summary(file):
        await job_service.enqueue_job(
            session,
            task_type=TASK_GENERATE_SUMMARY,
            task_key=f"file:{file.id}",
            payload={"file_id": file.id},
            file_id=file.id,
            priority=PRIORITY_SUMMARY,
        )

    if _file_ready_for_publish(file):
        await job_service.enqueue_job(
            session,
            task_type=TASK_PUBLISH_FILE,
            task_key=f"file:{file.id}",
            payload={"file_id": file.id},
            file_id=file.id,
            priority=PRIORITY_PUBLISH,
        )

    _refresh_file_status(file)
    await job_service.delete_job(session, job)


async def _merge_resolved_text_jobs(session: AsyncSession, file: LabFile) -> None:
    open_result = await session.execute(
        select(Job.id).where(
            Job.file_id == file.id,
            Job.task_type == TASK_EXTRACT_TEXT,
            Job.status.in_([job_service.JOB_STATUS_PENDING, job_service.JOB_STATUS_LEASED]),
        )
    )
    if open_result.first() is not None:
        if file.text_status != FILE_STAGE_ERROR:
            file.text_status = FILE_STAGE_RUNNING
        return

    failed_result = await session.execute(
        select(Job.id).where(
            Job.file_id == file.id,
            Job.task_type == TASK_EXTRACT_TEXT,
            Job.status == job_service.JOB_STATUS_FAILED,
        )
    )
    if failed_result.first() is not None:
        file.text_status = FILE_STAGE_ERROR
        file.summary_status = FILE_STAGE_ERROR
        return

    resolved_result = await session.execute(
        select(Job)
        .where(
            Job.file_id == file.id,
            Job.task_type == TASK_EXTRACT_TEXT,
            Job.status == job_service.JOB_STATUS_RESOLVED,
        )
        .order_by(Job.created_at.asc())
    )
    resolved_jobs = list(resolved_result.scalars().all())
    if not resolved_jobs:
        if file.ocr_text_raw or file.ocr_text_english or file.ocr_summary_english:
            if file.text_status != FILE_STAGE_ERROR:
                file.text_status = FILE_STAGE_DONE
            if file.summary_status != FILE_STAGE_ERROR:
                file.summary_status = FILE_STAGE_DONE if file.ocr_summary_english else FILE_STAGE_QUEUED
        else:
            if file.text_status != FILE_STAGE_ERROR:
                file.text_status = FILE_STAGE_QUEUED
            if file.summary_status != FILE_STAGE_ERROR:
                file.summary_status = FILE_STAGE_QUEUED
        return

    ordered_results = []
    for resolved_job in sorted(
        resolved_jobs, key=lambda current: int(job_service.json_loads(current.payload_json).get("batch_index", 0))
    ):
        ordered_results.append(job_service.json_loads(resolved_job.resolved_json))
    merged = copilot_extraction.merge_text_results(ordered_results)
    file.ocr_text_raw = _normalize_document_text(merged.get("raw_text"))
    file.ocr_text_english = _normalize_document_text(merged.get("translated_text_english"))
    file.text_status = FILE_STAGE_DONE
    if file.summary_status != FILE_STAGE_ERROR:
        file.summary_status = FILE_STAGE_DONE if file.ocr_summary_english else FILE_STAGE_QUEUED

    for resolved_job in resolved_jobs:
        await job_service.delete_job(session, resolved_job)


async def _apply_measurement_normalization(
    session: AsyncSession, file: LabFile, measurements: list[Measurement]
) -> None:
    if not measurements:
        file.normalization_status = FILE_STAGE_DONE if file.measurement_status == FILE_STAGE_DONE else FILE_STAGE_QUEUED
        return

    file.normalization_status = FILE_STAGE_RUNNING

    alias_map = await load_measurement_type_aliases(
        session, [measurement.raw_marker_name for measurement in measurements]
    )
    qualitative_rule_map = await qualitative_values.load_qualitative_rules(
        session,
        [
            measurement.original_qualitative_value
            for measurement in measurements
            if measurement.original_qualitative_value
        ],
    )
    marker_job_statuses = await _load_job_statuses(
        session,
        TASK_NORMALIZE_MARKER,
        [measurement.normalized_marker_key for measurement in measurements],
    )

    for measurement in measurements:
        measurement.normalization_error = None
        measurement.qualitative_value = None
        measurement.qualitative_bool = None
        measurement.canonical_value = None
        measurement.canonical_unit = None
        measurement.canonical_reference_low = None
        measurement.canonical_reference_high = None

        measurement_type = alias_map.get(measurement.raw_marker_name)
        if measurement_type is None:
            task_key = _marker_task_key(measurement.normalized_marker_key)
            if marker_job_statuses.get(task_key) == job_service.JOB_STATUS_FAILED:
                measurement.normalization_status = MEASUREMENT_STATE_ERROR
                measurement.normalization_error = f"Marker normalization failed for {measurement.raw_marker_name}"
                continue
            await job_service.enqueue_job(
                session,
                task_type=TASK_NORMALIZE_MARKER,
                task_key=task_key,
                payload={"raw_name": measurement.raw_marker_name},
                priority=PRIORITY_NORMALIZE,
            )
            measurement.normalization_status = MEASUREMENT_STATE_PENDING
            continue
        measurement.measurement_type_id = measurement_type.id

    await session.flush()

    type_ids = sorted(
        {measurement.measurement_type_id for measurement in measurements if measurement.measurement_type_id is not None}
    )
    type_result = await session.execute(
        select(MeasurementType).options(selectinload(MeasurementType.group)).where(MeasurementType.id.in_(type_ids))
    )
    measurement_types = {measurement_type.id: measurement_type for measurement_type in type_result.scalars().all()}

    group_job_statuses = await _load_job_statuses(
        session,
        TASK_NORMALIZE_GROUP,
        [measurement_type.normalized_key for measurement_type in measurement_types.values()],
    )
    unit_job_statuses = await _load_job_statuses(
        session,
        TASK_NORMALIZE_CANONICAL_UNIT,
        [measurement_type.normalized_key for measurement_type in measurement_types.values()],
    )

    conversion_requests: list[tuple[int, str, str]] = []
    conversion_task_keys: set[str] = set()
    qualitative_task_keys: set[str] = set()
    for measurement in measurements:
        measurement_type = measurement_types.get(measurement.measurement_type_id)
        if measurement_type is None:
            continue
        if measurement.original_value is not None and measurement.original_unit and measurement_type.canonical_unit:
            original_unit_key = rescaling.normalize_unit_key(measurement.original_unit)
            canonical_unit_key = rescaling.normalize_unit_key(measurement_type.canonical_unit)
            if original_unit_key and canonical_unit_key and original_unit_key != canonical_unit_key:
                conversion_requests.append(
                    (measurement_type.id, measurement.original_unit, measurement_type.canonical_unit)
                )
                conversion_task_keys.add(
                    _conversion_task_key(measurement_type.normalized_key, original_unit_key, canonical_unit_key)
                )
        if measurement.original_qualitative_value:
            qualitative_key = qualitative_values.normalize_qualitative_key(measurement.original_qualitative_value)
            if qualitative_key:
                qualitative_task_keys.add(_qualitative_task_key(qualitative_key))

    conversion_rule_map = await rescaling.load_rescaling_rules(session, conversion_requests)
    conversion_job_statuses = await _load_job_statuses(
        session, TASK_NORMALIZE_UNIT_CONVERSION, list(conversion_task_keys)
    )
    qualitative_job_statuses = await _load_job_statuses(
        session, TASK_NORMALIZE_QUALITATIVE, list(qualitative_task_keys)
    )

    any_error = False
    for measurement in measurements:
        measurement_type = measurement_types.get(measurement.measurement_type_id)
        if measurement_type is None:
            any_error = any_error or measurement.normalization_status == MEASUREMENT_STATE_ERROR
            continue

        resolved = True
        if measurement_type.group_id is None:
            task_key = _group_task_key(measurement_type.normalized_key)
            if group_job_statuses.get(task_key) == job_service.JOB_STATUS_FAILED:
                measurement.normalization_status = MEASUREMENT_STATE_ERROR
                measurement.normalization_error = f"Group normalization failed for {measurement_type.name}"
                any_error = True
                continue
            await job_service.enqueue_job(
                session,
                task_type=TASK_NORMALIZE_GROUP,
                task_key=task_key,
                payload={"measurement_type_id": measurement_type.id},
                priority=PRIORITY_NORMALIZE,
            )
            resolved = False

        if measurement.original_value is not None:
            if measurement.original_unit and not measurement_type.canonical_unit:
                task_key = _canonical_unit_task_key(measurement_type.normalized_key)
                if unit_job_statuses.get(task_key) == job_service.JOB_STATUS_FAILED:
                    measurement.normalization_status = MEASUREMENT_STATE_ERROR
                    measurement.normalization_error = f"Canonical unit normalization failed for {measurement_type.name}"
                    any_error = True
                    continue
                await job_service.enqueue_job(
                    session,
                    task_type=TASK_NORMALIZE_CANONICAL_UNIT,
                    task_key=task_key,
                    payload={"measurement_type_id": measurement_type.id},
                    priority=PRIORITY_NORMALIZE,
                )
                resolved = False
            else:
                measurement.canonical_unit = measurement_type.canonical_unit
                if measurement.original_unit is None or measurement_type.canonical_unit is None:
                    measurement.canonical_value = measurement.original_value
                    measurement.canonical_reference_low = measurement.original_reference_low
                    measurement.canonical_reference_high = measurement.original_reference_high
                elif rescaling.units_equivalent(measurement.original_unit, measurement_type.canonical_unit):
                    measurement.canonical_value = measurement.original_value
                    measurement.canonical_reference_low = measurement.original_reference_low
                    measurement.canonical_reference_high = measurement.original_reference_high
                else:
                    original_key = rescaling.normalize_unit_key(measurement.original_unit)
                    canonical_key = rescaling.normalize_unit_key(measurement_type.canonical_unit)
                    if original_key is None or canonical_key is None:
                        resolved = False
                    else:
                        task_key = _conversion_task_key(measurement_type.normalized_key, original_key, canonical_key)
                        rule = conversion_rule_map.get((measurement_type.id, original_key, canonical_key))
                        if rule is None or rule.scale_factor is None:
                            if conversion_job_statuses.get(task_key) == job_service.JOB_STATUS_FAILED:
                                measurement.normalization_status = MEASUREMENT_STATE_ERROR
                                measurement.normalization_error = (
                                    f"Unit conversion normalization failed for {measurement_type.name}"
                                )
                                any_error = True
                                continue
                            await job_service.enqueue_job(
                                session,
                                task_type=TASK_NORMALIZE_UNIT_CONVERSION,
                                task_key=task_key,
                                payload={
                                    "measurement_type_id": measurement_type.id,
                                    "original_unit": measurement.original_unit,
                                    "canonical_unit": measurement_type.canonical_unit,
                                },
                                priority=PRIORITY_NORMALIZE,
                            )
                            resolved = False
                        else:
                            measurement.canonical_value = rescaling.apply_scale_factor(
                                measurement.original_value,
                                rule.scale_factor,
                            )
                            measurement.canonical_reference_low = rescaling.apply_scale_factor(
                                measurement.original_reference_low,
                                rule.scale_factor,
                            )
                            measurement.canonical_reference_high = rescaling.apply_scale_factor(
                                measurement.original_reference_high,
                                rule.scale_factor,
                            )
        elif measurement.original_qualitative_value:
            qualitative_key = qualitative_values.normalize_qualitative_key(measurement.original_qualitative_value)
            rule = qualitative_rule_map.get(qualitative_key) if qualitative_key else None
            if rule is None:
                task_key = _qualitative_task_key(qualitative_key or measurement.original_qualitative_value)
                if qualitative_job_statuses.get(task_key) == job_service.JOB_STATUS_FAILED:
                    measurement.normalization_status = MEASUREMENT_STATE_ERROR
                    measurement.normalization_error = (
                        f"Qualitative normalization failed for {measurement.original_qualitative_value}"
                    )
                    any_error = True
                    continue
                await job_service.enqueue_job(
                    session,
                    task_type=TASK_NORMALIZE_QUALITATIVE,
                    task_key=task_key,
                    payload={"original_value": measurement.original_qualitative_value},
                    priority=PRIORITY_NORMALIZE,
                )
                resolved = False
            else:
                measurement.qualitative_value = rule.canonical_value
                measurement.qualitative_bool = rule.boolean_value
        else:
            measurement.canonical_value = measurement.original_value
            measurement.canonical_reference_low = measurement.original_reference_low
            measurement.canonical_reference_high = measurement.original_reference_high

        measurement.normalization_status = MEASUREMENT_STATE_RESOLVED if resolved else MEASUREMENT_STATE_PENDING
        if resolved:
            measurement.normalization_error = None

    if any_error:
        file.normalization_status = FILE_STAGE_ERROR
        file.processing_error = next(
            (
                measurement.normalization_error
                for measurement in measurements
                if measurement.normalization_status == MEASUREMENT_STATE_ERROR and measurement.normalization_error
            ),
            file.processing_error,
        )
    elif all(measurement.normalization_status == MEASUREMENT_STATE_RESOLVED for measurement in measurements):
        file.normalization_status = FILE_STAGE_DONE
    else:
        file.normalization_status = FILE_STAGE_RUNNING


async def _refresh_file_stages(session: AsyncSession, file: LabFile, measurements: list[Measurement]) -> None:
    file.measurement_status = await _derive_stage_status(
        session, file.id, TASK_EXTRACT_MEASUREMENT, done_when_no_jobs=True
    )
    if file.measurement_status == FILE_STAGE_ERROR:
        file.normalization_status = FILE_STAGE_ERROR
    elif not measurements and file.measurement_status == FILE_STAGE_DONE:
        file.normalization_status = FILE_STAGE_DONE
    elif (
        file.normalization_status != FILE_STAGE_ERROR
        and file.normalization_status == FILE_STAGE_QUEUED
        and measurements
    ):
        file.normalization_status = FILE_STAGE_RUNNING

    file.text_status = await _derive_stage_status(
        session,
        file.id,
        TASK_EXTRACT_TEXT,
        done_when_no_jobs=(
            file.text_status == FILE_STAGE_DONE
            or bool(file.ocr_text_raw)
            or bool(file.ocr_text_english)
        ),
        current=file.text_status,
    )

    if file.summary_status != FILE_STAGE_ERROR:
        if _summary_is_skippable(file, measurements):
            file.summary_status = FILE_STAGE_DONE
        else:
            file.summary_status = await _derive_stage_status(
                session,
                file.id,
                TASK_GENERATE_SUMMARY,
                done_when_no_jobs=file.summary_status == FILE_STAGE_DONE or bool(file.ocr_summary_english),
                current=file.summary_status,
            )

    if file.publish_status != FILE_STAGE_DONE:
        file.publish_status = await _derive_stage_status(
            session,
            file.id,
            TASK_PUBLISH_FILE,
            done_when_no_jobs=file.status == READY_FILE_STATUS,
            current=file.publish_status,
        )


async def _derive_stage_status(
    session: AsyncSession,
    file_id: int,
    task_type: str,
    *,
    done_when_no_jobs: bool,
    current: str | None = None,
) -> str:
    result = await session.execute(select(Job.status).where(Job.file_id == file_id, Job.task_type == task_type))
    statuses = result.scalars().all()
    if any(status == job_service.JOB_STATUS_FAILED for status in statuses):
        return FILE_STAGE_ERROR
    if any(status in {job_service.JOB_STATUS_PENDING, job_service.JOB_STATUS_LEASED} for status in statuses):
        return FILE_STAGE_RUNNING
    if any(status == job_service.JOB_STATUS_RESOLVED for status in statuses):
        return FILE_STAGE_RUNNING
    if done_when_no_jobs:
        return FILE_STAGE_DONE
    return current or FILE_STAGE_QUEUED


def _file_ready_for_summary(file: LabFile) -> bool:
    return (
        file.measurement_status == FILE_STAGE_DONE
        and file.normalization_status == FILE_STAGE_DONE
        and file.text_status == FILE_STAGE_DONE
        and file.summary_status == FILE_STAGE_QUEUED
    )


def _file_ready_for_text_extraction(file: LabFile) -> bool:
    return (
        file.measurement_status == FILE_STAGE_DONE
        and file.text_status == FILE_STAGE_QUEUED
        and file.summary_status == FILE_STAGE_QUEUED
    )


def _summary_is_skippable(file: LabFile, measurements: list[Measurement]) -> bool:
    return (
        file.measurement_status == FILE_STAGE_DONE
        and file.text_status == FILE_STAGE_DONE
        and not measurements
        and not file.ocr_text_raw
        and not file.ocr_text_english
    )


def _file_ready_for_publish(file: LabFile) -> bool:
    return (
        file.measurement_status == FILE_STAGE_DONE
        and file.normalization_status == FILE_STAGE_DONE
        and file.text_status == FILE_STAGE_DONE
        and file.summary_status == FILE_STAGE_DONE
        and file.publish_status != FILE_STAGE_DONE
    )


async def _generate_file_summary(session: AsyncSession, job: Job) -> None:
    payload = job_service.json_loads(job.payload_json)
    file_id = int(payload.get("file_id", 0))
    file = await session.get(LabFile, file_id)
    if file is None:
        await job_service.delete_job(session, job)
        return

    measurements_result = await session.execute(
        select(Measurement)
        .options(selectinload(Measurement.measurement_type))
        .where(Measurement.lab_file_id == file.id)
        .order_by(Measurement.id.asc())
    )
    measurements = list(measurements_result.scalars().all())
    if _summary_is_skippable(file, measurements):
        file.summary_status = FILE_STAGE_DONE
        await job_service.delete_job(session, job)
        await enqueue_file_reconcile(session, file.id)
        return

    file.summary_status = FILE_STAGE_RUNNING
    _refresh_file_status(file)
    await session.commit()

    medical_payload = _build_medical_payload(file, measurements)
    text_payload = {
        "raw_text": file.ocr_text_raw,
        "translated_text_english": file.ocr_text_english,
    }
    summary_text = await copilot_extraction.generate_summary(
        medical_payload,
        text_payload,
        filename=file.filename,
    )
    file.ocr_summary_english = _normalize_document_text(summary_text)
    file.summary_status = FILE_STAGE_DONE
    await job_service.delete_job(session, job)
    await enqueue_file_reconcile(session, file.id)


async def _enqueue_text_extraction_jobs(session: AsyncSession, file: LabFile) -> None:
    page_count = max(1, file.page_count)
    # Summary stays as its own stage so the text OCR jobs can stay small and
    # resilient while the summary call sees the fully merged document text.
    for batch_index, (start_page, stop_page) in enumerate(
        copilot_extraction.build_page_ranges(page_count, TEXT_BATCH_SIZE)
    ):
        await job_service.enqueue_job(
            session,
            task_type=TASK_EXTRACT_TEXT,
            task_key=_batch_task_key("text", file.id, start_page, stop_page, DEFAULT_OCR_DPI),
            payload={
                "file_id": file.id,
                "batch_index": batch_index,
                "start_page": start_page,
                "stop_page": stop_page,
                "dpi": DEFAULT_OCR_DPI,
            },
            file_id=file.id,
            priority=PRIORITY_TEXT_EXTRACT,
        )


async def _publish_file(session: AsyncSession, job: Job) -> None:
    payload = job_service.json_loads(job.payload_json)
    file_id = int(payload.get("file_id", 0))
    file = await session.get(LabFile, file_id, options=[selectinload(LabFile.tags)])
    if file is None:
        await job_service.delete_job(session, job)
        return

    measurements_result = await session.execute(
        select(Measurement)
        .options(selectinload(Measurement.measurement_type))
        .where(Measurement.lab_file_id == file.id)
        .order_by(Measurement.id.asc())
    )
    measurements = list(measurements_result.scalars().all())
    if not _file_ready_for_publish(file):
        await job_service.delete_job(session, job)
        await enqueue_file_reconcile(session, file.id)
        return

    file.publish_status = FILE_STAGE_RUNNING
    _refresh_file_status(file)
    await session.commit()

    file.ocr_raw = json.dumps(
        {
            **_build_medical_payload(file, measurements),
            "raw_text": file.ocr_text_raw,
            "translated_text_english": file.ocr_text_english,
            "summary_english": file.ocr_summary_english,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    file.publish_status = FILE_STAGE_DONE
    file.status = READY_FILE_STATUS
    file.published_at = utc_now()
    file.processing_error = None
    await search_service.refresh_lab_search_document(file.id, session)
    await job_service.delete_resolved_jobs_for_file(session, file.id)
    await job_service.delete_job(session, job)


async def _resolve_marker_jobs(session: AsyncSession, jobs: list[Job]) -> None:
    samples_by_key: dict[str, list[str]] = {}
    for job in jobs:
        measurement_result = await session.execute(
            select(Measurement.raw_marker_name)
            .where(Measurement.normalized_marker_key == job.task_key)
            .distinct()
            .order_by(Measurement.raw_marker_name.asc())
            .limit(8)
        )
        raw_names = [name for name in measurement_result.scalars().all() if name]
        if raw_names:
            samples_by_key[job.task_key] = raw_names

    if not samples_by_key:
        for job in jobs:
            await job_service.delete_job(session, job)
        return

    existing_canonical_result = await session.execute(select(MeasurementType.name).order_by(MeasurementType.name.asc()))
    existing_canonical = existing_canonical_result.scalars().all()

    prompt_names = [names[0] for names in samples_by_key.values()]
    normalized_map = await copilot_normalization.normalize_marker_names(prompt_names, existing_canonical)
    canonical_names = [normalized_map.get(name, name) for name in prompt_names]
    measurement_types = await _ensure_measurement_types(session, canonical_names)
    alias_pairs_by_key: dict[str, tuple[str, MeasurementType]] = {}

    for raw_names in samples_by_key.values():
        canonical_name = normalized_map.get(raw_names[0], raw_names[0])
        measurement_type = measurement_types[canonical_name]
        # Different raw marker jobs in one batch can collapse to the same alias key
        # (for example "CRP+" and "CRP"), so deduplicate the whole batch before
        # touching the alias table.
        for raw_name in raw_names:
            normalized_alias_key = normalize_marker_alias_key(raw_name)
            if not normalized_alias_key:
                continue
            alias_pairs_by_key[normalized_alias_key] = (raw_name, measurement_type)
        if measurement_type.group_id is None:
            await job_service.enqueue_job(
                session,
                task_type=TASK_NORMALIZE_GROUP,
                task_key=_group_task_key(measurement_type.normalized_key),
                payload={"measurement_type_id": measurement_type.id},
                priority=PRIORITY_NORMALIZE,
            )

    if alias_pairs_by_key:
        await ensure_measurement_type_aliases(session, list(alias_pairs_by_key.values()))

    await _enqueue_reconcile_for_measurement_keys(session, list(samples_by_key.keys()))
    for job in jobs:
        await job_service.delete_job(session, job)


async def _resolve_source_jobs(session: AsyncSession, jobs: list[Job]) -> None:
    for job in jobs:
        payload = job_service.json_loads(job.payload_json)
        file_id = payload.get("file_id")
        if not isinstance(file_id, int):
            await job_service.delete_job(session, job)
            continue

        file = await session.get(LabFile, file_id, options=[selectinload(LabFile.tags)])
        if file is None or not file.source_name:
            await job_service.delete_job(session, job)
            continue

        existing_source_result = await session.execute(
            select(LabFileTag.tag).where(LabFileTag.tag.like("source:%")).distinct().order_by(LabFileTag.tag.asc())
        )
        existing_sources = [
            value for tag in existing_source_result.scalars().all() if (value := source_tag_value(tag)) is not None
        ]
        normalized_source = await copilot_normalization.normalize_source_name(
            file.source_name,
            file.filename,
            existing_sources,
        )
        canonical_source = normalize_source_tag_value(normalized_source or file.source_name)
        if canonical_source:
            file.source_name = canonical_source
            await _set_file_source_tag(session, file, canonical_source)

        await job_service.delete_job(session, job)


async def _resolve_group_jobs(session: AsyncSession, jobs: list[Job]) -> None:
    measurement_types = await _load_measurement_types_for_jobs(session, jobs)
    if not measurement_types:
        for job in jobs:
            await job_service.delete_job(session, job)
        return

    group_names = await load_group_order(session)
    resolved = await copilot_normalization.classify_marker_groups(
        [measurement_type.name for measurement_type in measurement_types.values()],
        group_names,
    )
    groups_by_name = await _load_or_create_groups(session, list(resolved.values()))
    for measurement_type in measurement_types.values():
        group_name = resolved.get(measurement_type.name, DEFAULT_GROUP_NAME)
        group = groups_by_name[group_name]
        measurement_type.group_name = group.name
        measurement_type.group_id = group.id

    await _enqueue_reconcile_for_measurement_types(session, list(measurement_types.values()))
    for job in jobs:
        await job_service.delete_job(session, job)


async def _resolve_canonical_unit_jobs(session: AsyncSession, jobs: list[Job]) -> None:
    measurement_types = await _load_measurement_types_for_jobs(session, jobs)
    groups: list[copilot_normalization.MarkerUnitGroup] = []
    for measurement_type in measurement_types.values():
        result = await session.execute(
            select(Measurement)
            .where(Measurement.measurement_type_id == measurement_type.id, Measurement.original_value.is_not(None))
            .order_by(Measurement.id.asc())
            .limit(24)
        )
        measurements = result.scalars().all()
        observations = [
            copilot_normalization.MarkerObservation(
                id=str(measurement.id),
                value=measurement.original_value,
                unit=measurement.original_unit,
                reference_low=measurement.original_reference_low,
                reference_high=measurement.original_reference_high,
            )
            for measurement in measurements
            if measurement.original_value is not None
        ]
        if not observations:
            continue
        groups.append(
            copilot_normalization.MarkerUnitGroup(
                marker_name=measurement_type.name,
                existing_canonical_unit=measurement_type.canonical_unit,
                observations=observations,
            )
        )

    if groups:
        canonical_units = await copilot_normalization.choose_canonical_units(groups)
        for measurement_type in measurement_types.values():
            if measurement_type.name in canonical_units:
                measurement_type.canonical_unit = canonical_units[measurement_type.name]

    await _enqueue_reconcile_for_measurement_types(session, list(measurement_types.values()))
    for job in jobs:
        await job_service.delete_job(session, job)


async def _resolve_conversion_jobs(session: AsyncSession, jobs: list[Job]) -> None:
    requests: list[copilot_normalization.UnitConversionRequest] = []
    job_by_request_id: dict[str, Job] = {}
    measurement_types: dict[int, MeasurementType] = {}

    for job in jobs:
        payload = job_service.json_loads(job.payload_json)
        measurement_type_id = payload.get("measurement_type_id")
        original_unit = payload.get("original_unit")
        canonical_unit = payload.get("canonical_unit")
        if (
            not isinstance(measurement_type_id, int)
            or not isinstance(original_unit, str)
            or not isinstance(canonical_unit, str)
        ):
            continue
        measurement_type = measurement_types.get(measurement_type_id)
        if measurement_type is None:
            measurement_type = await session.get(MeasurementType, measurement_type_id)
            if measurement_type is None:
                continue
            measurement_types[measurement_type_id] = measurement_type

        result = await session.execute(
            select(Measurement)
            .where(
                Measurement.measurement_type_id == measurement_type_id,
                Measurement.normalized_original_unit == rescaling.normalize_unit_key(original_unit),
                Measurement.original_value.is_not(None),
            )
            .order_by(Measurement.id.asc())
            .limit(3)
        )
        sample = result.scalars().first()
        if sample is None or sample.original_value is None:
            continue

        request = copilot_normalization.UnitConversionRequest(
            id=job.task_key,
            marker_name=measurement_type.name,
            original_unit=original_unit,
            canonical_unit=canonical_unit,
            example_value=sample.original_value,
            reference_low=sample.original_reference_low,
            reference_high=sample.original_reference_high,
        )
        requests.append(request)
        job_by_request_id[job.task_key] = job

    if requests:
        scale_factors = await copilot_normalization.infer_rescaling_factors(requests)
        await rescaling.upsert_rescaling_rules(
            session,
            [
                {
                    "measurement_type": measurement_types[
                        job_service.json_loads(job_by_request_id[request.id].payload_json)["measurement_type_id"]
                    ],
                    "original_unit": request.original_unit,
                    "canonical_unit": request.canonical_unit,
                    "scale_factor": scale_factors.get(request.id),
                }
                for request in requests
            ],
        )
        await _enqueue_reconcile_for_conversion_jobs(session, requests, measurement_types)

    for job in jobs:
        await job_service.delete_job(session, job)


async def _resolve_qualitative_jobs(session: AsyncSession, jobs: list[Job]) -> None:
    requests: list[copilot_normalization.QualitativeNormalizationRequest] = []
    for job in jobs:
        result = await session.execute(
            select(Measurement)
            .options(selectinload(Measurement.measurement_type))
            .where(Measurement.original_qualitative_value.is_not(None))
            .order_by(Measurement.id.asc())
        )
        sample = next(
            (
                measurement
                for measurement in result.scalars().all()
                if qualitative_values.normalize_qualitative_key(measurement.original_qualitative_value) == job.task_key
            ),
            None,
        )
        if sample is None or sample.original_qualitative_value is None:
            continue
        requests.append(
            copilot_normalization.QualitativeNormalizationRequest(
                id=job.task_key,
                marker_name=sample.marker_name,
                original_value=sample.original_qualitative_value,
            )
        )

    if requests:
        existing_values_result = await session.execute(
            select(QualitativeRule.canonical_value).distinct().order_by(QualitativeRule.canonical_value.asc())
        )
        existing_values = existing_values_result.scalars().all()
        resolved = await copilot_normalization.normalize_qualitative_values(requests, existing_values)
        await qualitative_values.upsert_qualitative_rules(
            session,
            [
                {
                    "original_value": request.original_value,
                    "canonical_value": resolved.get(request.id, (None, None))[0],
                    "boolean_value": resolved.get(request.id, (None, None))[1],
                }
                for request in requests
            ],
        )
        await _enqueue_reconcile_for_qualitative_jobs(session, requests)

    for job in jobs:
        await job_service.delete_job(session, job)


async def _load_measurement_types_for_jobs(
    session: AsyncSession,
    jobs: list[Job],
) -> dict[int, MeasurementType]:
    measurement_type_ids = []
    for job in jobs:
        payload = job_service.json_loads(job.payload_json)
        measurement_type_id = payload.get("measurement_type_id")
        if isinstance(measurement_type_id, int):
            measurement_type_ids.append(measurement_type_id)
    if not measurement_type_ids:
        return {}

    result = await session.execute(select(MeasurementType).where(MeasurementType.id.in_(measurement_type_ids)))
    return {measurement_type.id: measurement_type for measurement_type in result.scalars().all()}


async def _ensure_measurement_types(session: AsyncSession, names: list[str]) -> dict[str, MeasurementType]:
    unique_names = [name.strip() for name in dict.fromkeys(names) if isinstance(name, str) and name.strip()]
    if not unique_names:
        return {}

    result = await session.execute(select(MeasurementType).where(MeasurementType.name.in_(unique_names)))
    by_name = {measurement_type.name: measurement_type for measurement_type in result.scalars().all()}
    new_names = [name for name in unique_names if name not in by_name]
    for name in new_names:
        measurement_type = MeasurementType(
            name=name,
            normalized_key=normalize_marker_alias_key(name),
            group_name=DEFAULT_GROUP_NAME,
            group_id=None,
            canonical_unit=None,
        )
        session.add(measurement_type)
        by_name[name] = measurement_type
    await session.flush()
    await ensure_measurement_type_aliases(session, [(name, by_name[name]) for name in unique_names])
    return by_name


async def _load_or_create_groups(session: AsyncSession, names: list[str]) -> dict[str, MarkerGroup]:
    await ensure_marker_groups(session)
    result = await session.execute(select(MarkerGroup))
    groups_by_name = {group.name: group for group in result.scalars().all()}
    max_order = max((group.display_order for group in groups_by_name.values()), default=1000)
    for name in dict.fromkeys(
        group_name.strip() for group_name in names if isinstance(group_name, str) and group_name.strip()
    ):
        if name in groups_by_name:
            continue
        max_order += 10
        group = MarkerGroup(name=name, display_order=max_order)
        session.add(group)
        await session.flush()
        groups_by_name[name] = group
    return groups_by_name


async def _enqueue_reconcile_for_measurement_keys(session: AsyncSession, keys: list[str]) -> None:
    if not keys:
        return
    result = await session.execute(
        select(Measurement.lab_file_id).where(Measurement.normalized_marker_key.in_(keys)).distinct()
    )
    for file_id in result.scalars().all():
        await enqueue_file_reconcile(session, file_id)


async def _enqueue_reconcile_for_measurement_types(
    session: AsyncSession,
    measurement_types: list[MeasurementType],
) -> None:
    type_ids = [measurement_type.id for measurement_type in measurement_types if measurement_type.id is not None]
    if not type_ids:
        return
    result = await session.execute(
        select(Measurement.lab_file_id).where(Measurement.measurement_type_id.in_(type_ids)).distinct()
    )
    for file_id in result.scalars().all():
        await enqueue_file_reconcile(session, file_id)


async def _enqueue_reconcile_for_conversion_jobs(
    session: AsyncSession,
    requests: list[copilot_normalization.UnitConversionRequest],
    measurement_types: dict[int, MeasurementType],
) -> None:
    for request in requests:
        measurement_type = next(
            (item for item in measurement_types.values() if item.name == request.marker_name),
            None,
        )
        if measurement_type is None:
            continue
        result = await session.execute(
            select(Measurement.lab_file_id)
            .where(
                Measurement.measurement_type_id == measurement_type.id,
                Measurement.normalized_original_unit == rescaling.normalize_unit_key(request.original_unit),
            )
            .distinct()
        )
        for file_id in result.scalars().all():
            await enqueue_file_reconcile(session, file_id)


async def _enqueue_reconcile_for_qualitative_jobs(
    session: AsyncSession,
    requests: list[copilot_normalization.QualitativeNormalizationRequest],
) -> None:
    keys = [qualitative_values.normalize_qualitative_key(request.original_value) for request in requests]
    normalized_keys = [key for key in keys if key]
    if not normalized_keys:
        return
    result = await session.execute(
        select(Measurement.lab_file_id, Measurement.original_qualitative_value).where(
            Measurement.original_qualitative_value.is_not(None)
        )
    )
    for file_id, original_value in result.all():
        normalized_value = qualitative_values.normalize_qualitative_key(original_value)
        if normalized_value in normalized_keys:
            await enqueue_file_reconcile(session, file_id)


async def _load_job_statuses(
    session: AsyncSession,
    task_type: str,
    task_keys: list[str],
) -> dict[str, str]:
    unique_task_keys = [task_key for task_key in dict.fromkeys(task_keys) if task_key]
    if not unique_task_keys:
        return {}
    result = await session.execute(
        select(Job.task_key, Job.status).where(Job.task_type == task_type, Job.task_key.in_(unique_task_keys))
    )
    return {task_key: status for task_key, status in result.all()}


def _build_medical_payload(file: LabFile, measurements: list[Measurement]) -> dict:
    return {
        "lab_date": file.lab_date.isoformat() if file.lab_date else None,
        "source": file.source_name,
        "measurements": [
            {
                "marker_name": measurement.marker_name,
                "value": measurement.original_value
                if measurement.original_value is not None
                else measurement.qualitative_value,
                "unit": measurement.original_unit,
                "reference_low": measurement.original_reference_low,
                "reference_high": measurement.original_reference_high,
                "measured_at": measurement.measured_at.isoformat() if measurement.measured_at else None,
                "page_number": measurement.page_number,
            }
            for measurement in measurements
        ],
    }


def _fallback_batch_ranges(start_page: int, stop_page: int, dpi: int) -> list[tuple[int, int, int]]:
    page_count = stop_page - start_page
    if page_count > 1:
        # By the time the durable job reaches this fallback path, the in-request
        # retry logic has already tried the larger batch and its own recursive
        # splits. Requeueing per page avoids repeating the same slow batch.
        return [(page, page + 1, dpi) for page in range(start_page, stop_page)]
    smaller_dpi = max(MIN_OCR_DPI, dpi - 24)
    if smaller_dpi != dpi:
        return [(start_page, stop_page, smaller_dpi)]
    return []


def _normalize_optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_document_text(raw: object) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def _parse_numeric_value(raw: object) -> float | None:
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        return value if math.isfinite(value) else None
    if not isinstance(raw, str):
        return None

    value = raw.strip()
    if not value:
        return None
    value = re.sub(r"(\d)\s+(\d{3})(?!\d)", r"\1\2", value)
    value = re.sub(r"(\d)\s+(\d)", r"\1.\2", value)
    if value.count(",") == 1:
        value = re.sub(r"(\d),(\d)", r"\1.\2", value)
    value = value.replace(" ", "")

    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _clean_qualitative_value(raw: object) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return str(raw).lower()
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if not value:
        return None
    value = re.sub(r"\s+", " ", value)
    value = value.strip(".:;,()[]{}")
    normalized = value.casefold().strip()
    return normalized or None


def _parse_measurement_value(raw: object) -> tuple[float | None, str | None]:
    numeric_value = _parse_numeric_value(raw)
    if numeric_value is not None:
        return numeric_value, None
    return None, _clean_qualitative_value(raw)


def _parse_datetime(raw: object) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        if raw.tzinfo is not None:
            return raw
        return raw.replace(tzinfo=UTC)
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed
    return parsed.replace(tzinfo=UTC)


def _parse_int(raw: object) -> int | None:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None
