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
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar

import fitz
from sqlalchemy import delete, func, inspect, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from illdashboard.config import settings
from illdashboard.copilot import extraction as copilot_extraction
from illdashboard.copilot import normalization as copilot_normalization
from illdashboard.copilot.client import COPILOT_REQUEST_TIMEOUT, get_copilot_request_load, shutdown_client
from illdashboard.models import (
    COMPLETE_FILE_STATUS,
    DEFAULT_GROUP_NAME,
    ERROR_FILE_STATUS,
    PROCESSING_FILE_STATUS,
    QUEUED_FILE_STATUS,
    UPLOADED_FILE_STATUS,
    Job,
    LabFile,
    LabFileTag,
    MarkerGroup,
    Measurement,
    MeasurementBatch,
    MeasurementType,
    QualitativeRule,
    SourceAlias,
    TextBatch,
    utc_now,
)
from illdashboard.services import jobs as job_service
from illdashboard.services import qualitative_values, rescaling, upload_metadata
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
)

logger = logging.getLogger(__name__)

FILE_STATUS_UPLOADED = UPLOADED_FILE_STATUS
FILE_STATUS_QUEUED = QUEUED_FILE_STATUS
FILE_STATUS_PROCESSING = PROCESSING_FILE_STATUS
FILE_STATUS_COMPLETE = COMPLETE_FILE_STATUS
FILE_STATUS_ERROR = ERROR_FILE_STATUS

MEASUREMENT_STATE_PENDING = "pending"
MEASUREMENT_STATE_RESOLVED = "resolved"
MEASUREMENT_STATE_ERROR = "error"

TASK_ENSURE_FILE = "ensure.file"
TASK_ENSURE_MEASUREMENT_EXTRACTION = "ensure.measurement-extraction"
TASK_ENSURE_TEXT = "ensure.text"
TASK_EXTRACT_MEASUREMENTS = "extract.measurements"
TASK_EXTRACT_TEXT = "extract.text"
TASK_ASSEMBLE_TEXT = "assemble.text"
TASK_PROCESS_MEASUREMENTS = "process.measurements"
TASK_GENERATE_SUMMARY = "generate.summary"
TASK_REFRESH_SEARCH = "refresh.search"
TASK_CANONIZE_MARKER = "canonize.marker"
TASK_CANONIZE_GROUP = "canonize.group"
TASK_CANONIZE_UNIT = "canonize.unit"
TASK_CANONIZE_CONVERSION = "canonize.conversion"
TASK_CANONIZE_QUALITATIVE = "canonize.qualitative"
TASK_CANONIZE_SOURCE = "canonize.source"

MEASUREMENT_BATCH_SIZE = 2
TEXT_BATCH_SIZE = 2
DEFAULT_OCR_DPI = 144
MIN_OCR_DPI = 96
MAX_JOB_ATTEMPTS = 3
JOB_LEASE_SECONDS = COPILOT_REQUEST_TIMEOUT
# Keep polling latency low enough that controller->worker handoffs do not add
# noticeable lag or cause clean-runtime restart tests to become timing-sensitive.
WORKER_IDLE_SECONDS = 0.2
WORKER_STOP_GRACE_SECONDS = 1.0
MEASUREMENT_EXTRACT_WORKER_CONCURRENCY = 4
TEXT_EXTRACT_WORKER_CONCURRENCY = 2
PROCESS_MEASUREMENTS_WORKER_CONCURRENCY = 2
SUMMARY_WORKER_CONCURRENCY = 1
ENSURE_WORKER_CONCURRENCY = 2
# This is a correctness invariant for the current no-lock design: each
# normalization lane must run only one claimed batch at a time.
CANONIZE_WORKER_CONCURRENCY = 1
SEARCH_WORKER_CONCURRENCY = 1
CANONIZE_MARKER_CLAIM_LIMIT = copilot_normalization.MARKER_NORMALIZATION_BATCH_SIZE
CANONIZE_GROUP_CLAIM_LIMIT = copilot_normalization.MARKER_GROUP_CLASSIFICATION_BATCH_SIZE
CANONIZE_UNIT_CLAIM_LIMIT = copilot_normalization.UNIT_NORMALIZATION_BATCH_SIZE
CANONIZE_CONVERSION_CLAIM_LIMIT = copilot_normalization.UNIT_NORMALIZATION_BATCH_SIZE
CANONIZE_QUALITATIVE_CLAIM_LIMIT = copilot_normalization.QUALITATIVE_NORMALIZATION_BATCH_SIZE

PRIORITY_ENSURE_FILE = 5
PRIORITY_ENSURE_LANE = 10
PRIORITY_EXTRACT_MEASUREMENTS = 20
PRIORITY_PROCESS_MEASUREMENTS = 30
PRIORITY_CANONIZE = 40
PRIORITY_EXTRACT_TEXT = 50
PRIORITY_ASSEMBLE_TEXT = 60
PRIORITY_SUMMARY = 70
PRIORITY_SEARCH = 80
RUNTIME_RESTART_RETRY_DELAYS = (0.05, 0.1, 0.2)

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


@dataclass(frozen=True)
class FileProgressSnapshot:
    measurement_pages_done: int
    measurement_pages_total: int
    text_pages_done: int
    text_pages_total: int
    ready_measurements: int
    total_measurements: int
    summary_ready: bool
    source_ready: bool
    search_ready: bool
    measurement_error_count: int
    is_complete: bool


async def preload_uploaded_files(session: AsyncSession) -> int:
    upload_dir = Path(settings.UPLOAD_DIR)
    if not upload_dir.is_dir():
        return 0

    result = await session.execute(select(LabFile.filepath))
    known_paths = {Path(path).resolve() for path in result.scalars().all()}

    added = 0
    for file_path in sorted(path for path in upload_dir.iterdir() if path.is_file()):
        if upload_metadata.is_original_name_sidecar(file_path):
            continue
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
                with fitz.open(str(file_path)) as document:
                    page_count = document.page_count
            except Exception:
                logger.warning("Could not read page count for %s, defaulting to 1", file_path.name)

        session.add(
            LabFile(
                filename=upload_metadata.read_original_name_sidecar(file_path) or file_path.name,
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

        async with self.session_factory() as session:
            await preload_uploaded_files(session)

        async with self.session_factory() as session:
            result = await session.execute(select(LabFile).order_by(LabFile.id.asc()))
            for file in result.scalars().all():
                if await _should_resume_file_on_startup(session, file):
                    await _request_file_ensure(session, file.id)
                elif file.status == FILE_STATUS_COMPLETE and await _file_needs_search_refresh(session, file):
                    await _request_search_refresh(session, file.id)
            await session.commit()

        self._spawn_workers(
            "ensure-file", [TASK_ENSURE_FILE], 1, ENSURE_WORKER_CONCURRENCY, self._handle_ensure_file_jobs
        )
        self._spawn_workers(
            "ensure-measurement-extraction",
            [TASK_ENSURE_MEASUREMENT_EXTRACTION],
            1,
            ENSURE_WORKER_CONCURRENCY,
            self._handle_ensure_measurement_extraction_jobs,
        )
        self._spawn_workers(
            "ensure-text", [TASK_ENSURE_TEXT], 1, ENSURE_WORKER_CONCURRENCY, self._handle_ensure_text_jobs
        )
        self._spawn_workers(
            "extract-measurements",
            [TASK_EXTRACT_MEASUREMENTS],
            1,
            MEASUREMENT_EXTRACT_WORKER_CONCURRENCY,
            self._handle_extract_measurements_jobs,
        )
        self._spawn_workers(
            "extract-text", [TASK_EXTRACT_TEXT], 1, TEXT_EXTRACT_WORKER_CONCURRENCY, self._handle_extract_text_jobs
        )
        self._spawn_workers("assemble-text", [TASK_ASSEMBLE_TEXT], 1, 1, self._handle_assemble_text_jobs)
        self._spawn_workers(
            "process-measurements",
            [TASK_PROCESS_MEASUREMENTS],
            1,
            PROCESS_MEASUREMENTS_WORKER_CONCURRENCY,
            self._handle_process_measurements_jobs,
        )
        self._spawn_workers(
            "summary", [TASK_GENERATE_SUMMARY], 1, SUMMARY_WORKER_CONCURRENCY, self._handle_summary_jobs
        )
        self._spawn_workers("search", [TASK_REFRESH_SEARCH], 1, SEARCH_WORKER_CONCURRENCY, self._handle_search_jobs)
        self._spawn_workers(
            "canonize-source", [TASK_CANONIZE_SOURCE], 1, CANONIZE_WORKER_CONCURRENCY, self._handle_source_jobs
        )
        self._spawn_workers(
            "canonize-marker",
            [TASK_CANONIZE_MARKER],
            CANONIZE_MARKER_CLAIM_LIMIT,
            CANONIZE_WORKER_CONCURRENCY,
            self._handle_marker_jobs,
        )
        self._spawn_workers(
            "canonize-group",
            [TASK_CANONIZE_GROUP],
            CANONIZE_GROUP_CLAIM_LIMIT,
            CANONIZE_WORKER_CONCURRENCY,
            self._handle_group_jobs,
        )
        self._spawn_workers(
            "canonize-unit",
            [TASK_CANONIZE_UNIT],
            CANONIZE_UNIT_CLAIM_LIMIT,
            CANONIZE_WORKER_CONCURRENCY,
            self._handle_unit_jobs,
        )
        self._spawn_workers(
            "canonize-conversion",
            [TASK_CANONIZE_CONVERSION],
            CANONIZE_CONVERSION_CLAIM_LIMIT,
            CANONIZE_WORKER_CONCURRENCY,
            self._handle_conversion_jobs,
        )
        self._spawn_workers(
            "canonize-qualitative",
            [TASK_CANONIZE_QUALITATIVE],
            CANONIZE_QUALITATIVE_CLAIM_LIMIT,
            CANONIZE_WORKER_CONCURRENCY,
            self._handle_qualitative_jobs,
        )

    async def stop(self, *, abort_copilot_requests: bool = False) -> None:
        self.stop_event.set()
        if abort_copilot_requests:
            await shutdown_client()
        if self.tasks:
            done, pending = await asyncio.wait(self.tasks, timeout=WORKER_STOP_GRACE_SECONDS)
            for task in pending:
                task.cancel()
            await asyncio.gather(*done, *pending, return_exceptions=True)
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
        # AsyncSession.rollback() expires ORM attributes, so derive stable identity
        # keys from the instance state before refreshing the rows in this session.
        job_ids = [identity[0] for job in jobs if (identity := inspect(job).identity) and isinstance(identity[0], int)]
        for job_id in job_ids:
            refreshed = await session.get(Job, job_id)
            if refreshed is None or refreshed.status != job_service.JOB_STATUS_LEASED:
                continue
            if refreshed.attempt_count < MAX_JOB_ATTEMPTS:
                await job_service.release_job(session, refreshed, delay_seconds=15, error_text=str(exc))
            else:
                await job_service.mark_job_failed(session, refreshed, error_text=str(exc))
                await _handle_terminal_job_failure(session, refreshed, str(exc))

    async def _run_jobs_in_fresh_session(
        self,
        job_ids: list[int],
        handler: Callable[[AsyncSession, list[Job]], Awaitable[None]],
    ) -> None:
        requested_ids = [job_id for job_id in dict.fromkeys(job_ids) if isinstance(job_id, int)]
        if not requested_ids:
            return

        async with self.session_factory() as session:
            result = await session.execute(select(Job).where(Job.id.in_(requested_ids)).order_by(Job.id.asc()))
            jobs_by_id = {job.id: job for job in result.scalars().all()}
            fresh_jobs = [
                jobs_by_id[job_id]
                for job_id in requested_ids
                if job_id in jobs_by_id and jobs_by_id[job_id].status == job_service.JOB_STATUS_LEASED
            ]
            if not fresh_jobs:
                return
            try:
                await handler(session, fresh_jobs)
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def _handle_ensure_file_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _ensure_file(session, jobs[0])

    async def _handle_ensure_measurement_extraction_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _ensure_measurement_extraction(session, jobs[0])

    async def _handle_ensure_text_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _ensure_text(session, jobs[0])

    async def _handle_extract_measurements_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _process_extraction_job(session, jobs[0], measurement_mode=True)

    async def _handle_extract_text_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _process_extraction_job(session, jobs[0], measurement_mode=False)

    async def _handle_assemble_text_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _assemble_text(session, jobs[0])

    async def _handle_process_measurements_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _process_measurements(session, jobs[0])

    async def _handle_summary_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _generate_summary(session, jobs[0])

    async def _handle_search_jobs(self, session: AsyncSession, jobs: list[Job]) -> None:
        await _refresh_search(session, jobs[0])

    async def _handle_source_jobs(self, _session: AsyncSession, jobs: list[Job]) -> None:
        await self._run_jobs_in_fresh_session([job.id for job in jobs], _canonize_source_jobs)

    async def _handle_marker_jobs(self, _session: AsyncSession, jobs: list[Job]) -> None:
        await self._run_jobs_in_fresh_session([job.id for job in jobs], _canonize_marker_jobs)

    async def _handle_group_jobs(self, _session: AsyncSession, jobs: list[Job]) -> None:
        await self._run_jobs_in_fresh_session([job.id for job in jobs], _canonize_group_jobs)

    async def _handle_unit_jobs(self, _session: AsyncSession, jobs: list[Job]) -> None:
        await self._run_jobs_in_fresh_session([job.id for job in jobs], _canonize_unit_jobs)

    async def _handle_conversion_jobs(self, _session: AsyncSession, jobs: list[Job]) -> None:
        await self._run_jobs_in_fresh_session([job.id for job in jobs], _canonize_conversion_jobs)

    async def _handle_qualitative_jobs(self, _session: AsyncSession, jobs: list[Job]) -> None:
        await self._run_jobs_in_fresh_session([job.id for job in jobs], _canonize_qualitative_jobs)


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


async def _should_resume_file_on_startup(session: AsyncSession, file: LabFile) -> bool:
    # Startup should only resume files the user had already scheduled, not
    # opportunistically start every uploaded or errored file in the database.
    if file.status in {FILE_STATUS_QUEUED, FILE_STATUS_PROCESSING}:
        return True

    active_job_result = await session.execute(
        select(Job.id)
        .where(
            Job.file_id == file.id,
            Job.status.in_([job_service.JOB_STATUS_PENDING, job_service.JOB_STATUS_LEASED]),
        )
        .limit(1)
    )
    return active_job_result.scalar_one_or_none() is not None


async def queue_file(session: AsyncSession, file_id: int) -> LabFile:
    result = await session.execute(select(LabFile).options(selectinload(LabFile.tags)).where(LabFile.id == file_id))
    file = result.scalar_one_or_none()
    if file is None:
        raise ValueError(f"Unknown file id {file_id}")

    if file.source_candidate_key:
        # Source canonization is keyed globally by normalized candidate, so an
        # explicit file reset must also clear the shared source job to allow a
        # fresh retry from rebuilt file artifacts.
        await session.execute(
            delete(Job).where(Job.task_type == TASK_CANONIZE_SOURCE, Job.task_key == file.source_candidate_key)
        )
    await job_service.delete_jobs_for_file(session, file.id)
    await _reset_file_processing_state(session, file, file_status=FILE_STATUS_QUEUED)
    await _request_file_ensure(session, file.id)
    await session.flush()
    return file


async def queue_files(session: AsyncSession, file_ids: list[int]) -> list[int]:
    if not file_ids:
        return []
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


async def run_with_pipeline_runtime_stopped(operation: Callable[[], Awaitable[_T]]) -> _T:
    async with _runtime_reset_lock:
        session_factory = _runtime.session_factory if _runtime is not None else None
        runtime_was_running = session_factory is not None
        if runtime_was_running:
            await stop_pipeline_runtime(abort_copilot_requests=True)
        try:
            return await operation()
        finally:
            if runtime_was_running and session_factory is not None:
                await _restart_pipeline_runtime(session_factory)


async def _run_with_clean_runtime(
    session: AsyncSession,
    operation: Callable[[AsyncSession], Awaitable[_T]],
) -> _T:
    async def run_operation() -> _T:
        try:
            return await operation(session)
        except Exception:
            await session.rollback()
            raise

    return await run_with_pipeline_runtime_stopped(run_operation)


async def _restart_pipeline_runtime(session_factory: async_sessionmaker[AsyncSession]) -> None:
    for attempt_index, delay_seconds in enumerate(RUNTIME_RESTART_RETRY_DELAYS, start=1):
        try:
            await start_pipeline_runtime(session_factory)
            return
        except OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt_index == len(RUNTIME_RESTART_RETRY_DELAYS):
                raise
            logger.warning(
                "Pipeline runtime restart hit a locked SQLite database; retrying in %.2fs",
                delay_seconds,
            )
            await asyncio.sleep(delay_seconds)


async def reset_incomplete_processing(session: AsyncSession) -> None:
    await job_service.delete_all_jobs(session)
    result = await session.execute(
        select(LabFile).options(selectinload(LabFile.tags)).where(LabFile.status != FILE_STATUS_COMPLETE)
    )
    for file in result.scalars().unique().all():
        await _reset_file_processing_state(session, file, file_status=FILE_STATUS_UPLOADED)


async def _reset_file_processing_state(session: AsyncSession, file: LabFile, *, file_status: str) -> None:
    await session.execute(delete(Measurement).where(Measurement.lab_file_id == file.id))
    await session.execute(delete(MeasurementBatch).where(MeasurementBatch.file_id == file.id))
    await session.execute(delete(TextBatch).where(TextBatch.file_id == file.id))
    await search_service.remove_lab_search_document(file.id, session)

    if file.tags:
        for tag in list(file.tags):
            if tag.tag.casefold().startswith(SOURCE_TAG_PREFIX):
                await session.delete(tag)

    file.status = file_status
    file.processing_error = None
    file.source_candidate = None
    file.source_candidate_key = None
    file.source_name = None
    file.ocr_raw = None
    file.ocr_text_raw = None
    file.ocr_text_english = None
    file.ocr_summary_english = None
    file.lab_date = None
    file.text_assembled_at = None
    file.summary_generated_at = None
    file.source_resolved_at = None
    file.search_indexed_at = None


async def _ensure_file(session: AsyncSession, job: Job) -> None:
    payload = job_service.json_loads(job.payload_json)
    file_id = int(payload.get("file_id", 0))
    file = await session.get(LabFile, file_id, options=[selectinload(LabFile.tags)])
    if file is None:
        await job_service.delete_job(session, job)
        return

    progress = await get_file_progress(session, file)
    measurements = await _load_file_measurements(session, file.id)

    if progress.measurement_pages_done < progress.measurement_pages_total:
        await _request_measurement_extraction_ensure(session, file.id)

    if progress.text_pages_done < progress.text_pages_total or file.text_assembled_at is None:
        await _request_text_ensure(session, file.id)

    if progress.total_measurements > progress.ready_measurements:
        await _request_process_measurements(session, file.id)

    if file.text_assembled_at is not None and file.summary_generated_at is None:
        await _request_summary(session, file.id)

    if file.summary_generated_at is not None and file.source_resolved_at is None:
        if file.source_candidate_key:
            await _request_source_canonization(session, file)
        else:
            await _mark_source_resolved(session, file, canonical_source=None)

    progress = await get_file_progress(session, file)
    if progress.is_complete:
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
        if await _file_needs_search_refresh(session, file):
            await _request_search_refresh(session, file.id)

    await _refresh_file_status_projection(session, file, progress=progress)
    await job_service.mark_job_resolved(session, job)


async def _ensure_measurement_extraction(session: AsyncSession, job: Job) -> None:
    payload = job_service.json_loads(job.payload_json)
    file_id = int(payload.get("file_id", 0))
    file = await session.get(LabFile, file_id)
    if file is None:
        await job_service.delete_job(session, job)
        return

    existing_ranges = await _load_measurement_batch_ranges(session, file.id)
    scheduled_ranges = await _load_scheduled_extraction_ranges(session, file.id, measurement_mode=True)
    missing_ranges = _missing_page_ranges(file.page_count, existing_ranges + scheduled_ranges, MEASUREMENT_BATCH_SIZE)
    for start_page, stop_page in missing_ranges:
        await _request_measurement_extraction_batch(session, file.id, start_page, stop_page, DEFAULT_OCR_DPI)
    await _refresh_file_status_projection(session, file)
    await job_service.mark_job_resolved(session, job)


async def _ensure_text(session: AsyncSession, job: Job) -> None:
    payload = job_service.json_loads(job.payload_json)
    file_id = int(payload.get("file_id", 0))
    file = await session.get(LabFile, file_id)
    if file is None:
        await job_service.delete_job(session, job)
        return

    existing_ranges = await _load_text_batch_ranges(session, file.id)
    scheduled_ranges = await _load_scheduled_extraction_ranges(session, file.id, measurement_mode=False)
    missing_ranges = _missing_page_ranges(file.page_count, existing_ranges + scheduled_ranges, TEXT_BATCH_SIZE)
    for start_page, stop_page in missing_ranges:
        await _request_text_extraction_batch(session, file.id, start_page, stop_page, DEFAULT_OCR_DPI)

    if not missing_ranges and file.text_assembled_at is None:
        await _request_assemble_text(session, file.id)

    await _refresh_file_status_projection(session, file)
    await job_service.mark_job_resolved(session, job)


async def _process_extraction_job(session: AsyncSession, job: Job, *, measurement_mode: bool) -> None:
    payload = job_service.json_loads(job.payload_json)
    file_id = int(payload.get("file_id", 0))
    start_page = int(payload.get("start_page", 0))
    stop_page = int(payload.get("stop_page", 0))
    dpi = int(payload.get("dpi", DEFAULT_OCR_DPI))
    file = await session.get(LabFile, file_id, options=[selectinload(LabFile.tags)])
    if file is None:
        await job_service.delete_job(session, job)
        return

    if measurement_mode:
        if await _page_range_is_fully_covered(session, file.id, start_page, stop_page, measurement_mode=True):
            await job_service.mark_job_resolved(session, job, {"skipped": True})
            await _request_file_ensure(session, file.id)
            return
        stage_name = "measurements"
    else:
        if await _page_range_is_fully_covered(session, file.id, start_page, stop_page, measurement_mode=False):
            await job_service.mark_job_resolved(session, job, {"skipped": True})
            await _request_text_ensure(session, file.id)
            await _request_file_ensure(session, file.id)
            return
        stage_name = "text"

    file.status = FILE_STATUS_PROCESSING
    await session.commit()

    started_at = time.perf_counter()
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
        if copilot_extraction.is_retryable_batch_error(exc):
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
                for fallback_start, fallback_stop, fallback_dpi in fallback_ranges:
                    if measurement_mode:
                        await _request_measurement_extraction_batch(
                            session, file.id, fallback_start, fallback_stop, fallback_dpi
                        )
                    else:
                        await _request_text_extraction_batch(
                            session, file.id, fallback_start, fallback_stop, fallback_dpi
                        )
                await job_service.mark_job_resolved(session, job, {"split": True})
                await _request_file_ensure(session, file.id)
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
        file.processing_error = str(exc)
        await job_service.mark_job_failed(session, job, error_text=str(exc))
        await _refresh_file_status_projection(session, file)
        return

    if measurement_mode:
        await _persist_measurement_batch(
            session, file, job, result, start_page=start_page, stop_page=stop_page, dpi=dpi
        )
        await _request_process_measurements(session, file.id)
    else:
        await _persist_text_batch(session, file, job, result, start_page=start_page, stop_page=stop_page, dpi=dpi)
        await _request_text_ensure(session, file.id)

    await job_service.mark_job_resolved(session, job)
    await _request_file_ensure(session, file.id)


async def _persist_measurement_batch(
    session: AsyncSession,
    file: LabFile,
    job: Job,
    result: dict,
    *,
    start_page: int,
    stop_page: int,
    dpi: int,
) -> None:
    await session.execute(
        delete(Measurement).where(
            Measurement.lab_file_id == file.id,
            Measurement.batch_key == job.task_key,
        )
    )
    existing_batch_result = await session.execute(
        select(MeasurementBatch).where(MeasurementBatch.file_id == file.id, MeasurementBatch.task_key == job.task_key)
    )
    existing_batch = existing_batch_result.scalar_one_or_none()
    if existing_batch is None:
        session.add(
            MeasurementBatch(
                file_id=file.id,
                task_key=job.task_key,
                start_page=start_page,
                stop_page=stop_page,
                dpi=dpi,
            )
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


async def _persist_text_batch(
    session: AsyncSession,
    file: LabFile,
    job: Job,
    result: dict,
    *,
    start_page: int,
    stop_page: int,
    dpi: int,
) -> None:
    existing_result = await session.execute(
        select(TextBatch).where(TextBatch.file_id == file.id, TextBatch.task_key == job.task_key)
    )
    batch = existing_result.scalar_one_or_none()
    raw_text = _normalize_document_text(result.get("raw_text"))
    translated_text = _normalize_document_text(result.get("translated_text_english"))
    if batch is None:
        batch = TextBatch(
            file_id=file.id,
            task_key=job.task_key,
            start_page=start_page,
            stop_page=stop_page,
            dpi=dpi,
            raw_text=raw_text,
            translated_text_english=translated_text,
        )
        session.add(batch)
        return
    batch.raw_text = raw_text
    batch.translated_text_english = translated_text


def _task_log_files(files: list[LabFile]) -> tuple[list[int], list[str]]:
    return [file.id for file in files], [file.filename for file in files]


def _log_task_span_start(job: Job, files: list[LabFile]) -> float:
    # Emit explicit file lists so the run-log viewer can place shared task spans
    # under every related file without guessing from surrounding lines.
    file_ids, filenames = _task_log_files(files)
    started_at = time.perf_counter()
    logger.info(
        "Task span start task_type=%s job_id=%s task_key=%s file_ids=%s filenames=%s",
        job.task_type,
        job.id,
        job.task_key,
        json.dumps(file_ids),
        json.dumps(filenames, ensure_ascii=False),
    )
    return started_at


def _log_task_span_finish(job: Job, files: list[LabFile], started_at: float, *, outcome: str = "success") -> None:
    file_ids, filenames = _task_log_files(files)
    logger.info(
        "Task span finish task_type=%s job_id=%s task_key=%s file_ids=%s filenames=%s duration=%.2fs outcome=%s",
        job.task_type,
        job.id,
        job.task_key,
        json.dumps(file_ids),
        json.dumps(filenames, ensure_ascii=False),
        time.perf_counter() - started_at,
        outcome,
    )


# Batch canonize handlers operate on measurement types, not files, so they
# use a lightweight span variant that logs the batch size instead of file lists.
def _log_batch_span_start(task_type: str, jobs: list[Job]) -> float:
    started_at = time.perf_counter()
    logger.info(
        "Task span start task_type=%s job_id=%s task_key=%s file_ids=%s filenames=%s",
        task_type,
        jobs[0].id,
        jobs[0].task_key,
        json.dumps([]),
        json.dumps([f"batch({len(jobs)})"], ensure_ascii=False),
    )
    return started_at


def _log_batch_span_finish(
    task_type: str, jobs: list[Job], started_at: float, *, outcome: str = "success"
) -> None:
    logger.info(
        "Task span finish task_type=%s job_id=%s task_key=%s file_ids=%s filenames=%s duration=%.2fs outcome=%s",
        task_type,
        jobs[0].id,
        jobs[0].task_key,
        json.dumps([]),
        json.dumps([f"batch({len(jobs)})"], ensure_ascii=False),
        time.perf_counter() - started_at,
        outcome,
    )


async def _assemble_text(session: AsyncSession, job: Job) -> None:
    payload = job_service.json_loads(job.payload_json)
    file_id = int(payload.get("file_id", 0))
    file = await session.get(LabFile, file_id)
    if file is None:
        await job_service.delete_job(session, job)
        return

    if file.text_assembled_at is not None:
        await job_service.mark_job_resolved(session, job, {"skipped": True})
        await _request_file_ensure(session, file.id)
        return

    text_ranges = await _load_text_batch_ranges(session, file.id)
    if not _coverage_complete(file.page_count, text_ranges):
        await job_service.mark_job_resolved(session, job, {"waiting": True})
        await _request_text_ensure(session, file.id)
        return

    started_at = _log_task_span_start(job, [file])
    result = await session.execute(
        select(TextBatch)
        .where(TextBatch.file_id == file.id)
        .order_by(TextBatch.start_page.asc(), TextBatch.stop_page.asc(), TextBatch.id.asc())
    )
    batches = result.scalars().all()
    merged = copilot_extraction.merge_text_results(
        [
            {
                "raw_text": batch.raw_text,
                "translated_text_english": batch.translated_text_english,
            }
            for batch in batches
        ]
    )
    file.ocr_text_raw = _normalize_document_text(merged.get("raw_text"))
    file.ocr_text_english = _normalize_document_text(merged.get("translated_text_english"))
    file.text_assembled_at = utc_now()
    await job_service.mark_job_resolved(session, job)
    _log_task_span_finish(job, [file], started_at)
    await _request_summary(session, file.id)
    await _request_file_ensure(session, file.id)


def _measurement_processing_snapshot(measurements: list[Measurement]) -> list[tuple]:
    # Capture the measurement-facing artifacts that process.measurements owns so
    # repeated passes that neither change artifacts nor queue new follow-up work
    # can be logged as filterable no-ops in the waterfall viewer.
    return [
        (
            measurement.id,
            measurement.measurement_type_id,
            measurement.normalization_status,
            measurement.normalization_error,
            measurement.qualitative_value,
            measurement.qualitative_bool,
            measurement.canonical_unit,
            measurement.canonical_value,
            measurement.canonical_reference_low,
            measurement.canonical_reference_high,
        )
        for measurement in measurements
    ]


async def _process_measurements(session: AsyncSession, job: Job) -> None:
    payload = job_service.json_loads(job.payload_json)
    file_id = int(payload.get("file_id", 0))
    file = await session.get(LabFile, file_id)
    if file is None:
        await job_service.delete_job(session, job)
        return

    measurements = await _load_file_measurements(session, file.id)
    if not measurements:
        await job_service.mark_job_resolved(session, job, {"skipped": True})
        await _request_file_ensure(session, file.id)
        return

    started_at = _log_task_span_start(job, [file])
    before_snapshot = _measurement_processing_snapshot(measurements)
    requested_new_work = await _apply_known_measurement_rules(session, measurements)
    after_snapshot = _measurement_processing_snapshot(measurements)
    await job_service.mark_job_resolved(session, job)
    outcome = "noop" if not requested_new_work and before_snapshot == after_snapshot else "success"
    _log_task_span_finish(job, [file], started_at, outcome=outcome)
    # Only re-trigger file ensure when no measurements are still waiting for
    # canonization.  When measurements remain pending, the canonization
    # completion handlers already call _request_process_measurements and
    # _request_file_ensure, so triggering ensure here would create a busy
    # loop (ensure re-enqueues process.measurements which finds the same
    # pending state and re-enqueues ensure, repeatedly).
    still_pending = any(m.normalization_status == MEASUREMENT_STATE_PENDING for m in measurements)
    if not still_pending:
        await _request_file_ensure(session, file.id)


async def _generate_summary(session: AsyncSession, job: Job) -> None:
    payload = job_service.json_loads(job.payload_json)
    file_id = int(payload.get("file_id", 0))
    file = await session.get(LabFile, file_id)
    if file is None:
        await job_service.delete_job(session, job)
        return

    if file.summary_generated_at is not None:
        await job_service.mark_job_resolved(session, job, {"skipped": True})
        await _request_file_ensure(session, file.id)
        return

    if file.text_assembled_at is None:
        await job_service.mark_job_resolved(session, job, {"waiting": True})
        await _request_text_ensure(session, file.id)
        return

    file.status = FILE_STATUS_PROCESSING
    await session.commit()
    started_at = _log_task_span_start(job, [file])

    if not file.ocr_text_raw:
        file.ocr_summary_english = None
        file.source_candidate = None
        file.source_candidate_key = None
        file.summary_generated_at = utc_now()
        await job_service.mark_job_resolved(session, job)
        _log_task_span_finish(job, [file], started_at, outcome="no_text")
        await _request_file_ensure(session, file.id)
        return

    summary_payload = await copilot_extraction.generate_summary(
        raw_text=file.ocr_text_raw,
        filename=file.filename,
    )
    file.ocr_summary_english = _normalize_document_text(summary_payload.get("summary_english"))
    summary_date = _parse_datetime(summary_payload.get("lab_date"))
    file.lab_date = summary_date
    source_candidate = _normalize_optional_text(summary_payload.get("source"))
    file.source_candidate = source_candidate
    file.source_candidate_key = normalize_source_tag_value(source_candidate) if source_candidate else None
    file.summary_generated_at = utc_now()
    await job_service.mark_job_resolved(session, job)
    _log_task_span_finish(job, [file], started_at)
    await _request_file_ensure(session, file.id)


async def _canonize_source(session: AsyncSession, job: Job) -> None:
    source_key = job.task_key
    if not source_key:
        await job_service.mark_job_resolved(session, job, {"skipped": True})
        return

    file_result = await session.execute(
        select(LabFile).options(selectinload(LabFile.tags)).where(LabFile.source_candidate_key == source_key)
    )
    files = file_result.scalars().unique().all()
    if not files:
        await job_service.mark_job_resolved(session, job, {"skipped": True})
        return

    started_at = _log_task_span_start(job, files)
    alias_result = await session.execute(select(SourceAlias).where(SourceAlias.normalized_key == source_key))
    alias = alias_result.scalar_one_or_none()
    if alias is None:
        sample_file = sorted(files, key=lambda current: current.id)[0]
        sample_candidate = sample_file.source_candidate or ""
        existing_sources_result = await session.execute(
            select(SourceAlias.canonical_name).distinct().order_by(SourceAlias.canonical_name.asc())
        )
        existing_sources = existing_sources_result.scalars().all()
        await _commit_before_external_normalization_call(session)
        normalized_source = await copilot_normalization.normalize_source_name(
            sample_candidate,
            sample_file.filename,
            existing_sources,
        )
        canonical_source = normalize_source_tag_value(normalized_source or sample_candidate)
        if canonical_source:
            alias = SourceAlias(
                alias_name=sample_candidate,
                normalized_key=source_key,
                canonical_name=canonical_source,
            )
            session.add(alias)
            await session.flush()

    canonical_name = alias.canonical_name if alias is not None else None
    for file in files:
        await _mark_source_resolved(session, file, canonical_name)
        await _request_file_ensure(session, file.id)

    await job_service.mark_job_resolved(session, job)
    _log_task_span_finish(job, files, started_at)


async def _canonize_source_jobs(session: AsyncSession, jobs: list[Job]) -> None:
    for job in jobs:
        await _canonize_source(session, job)


async def _canonize_marker_jobs(session: AsyncSession, jobs: list[Job]) -> None:
    marker_jobs: list[tuple[str, Job]] = []
    for job in jobs:
        marker_key = job.task_key
        if not marker_key:
            await job_service.mark_job_resolved(session, job, {"skipped": True})
            continue
        marker_jobs.append((marker_key, job))

    marker_keys = list(dict.fromkeys(marker_key for marker_key, _ in marker_jobs))
    if not marker_keys:
        return

    started_at = _log_batch_span_start(TASK_CANONIZE_MARKER, [job for _, job in marker_jobs])
    raw_name_result = await session.execute(
        select(Measurement.normalized_marker_key, Measurement.raw_marker_name, Measurement.original_unit)
        .where(Measurement.normalized_marker_key.in_(marker_keys))
        .order_by(
            Measurement.normalized_marker_key.asc(),
            Measurement.raw_marker_name.asc(),
            Measurement.original_unit.asc(),
        )
    )
    raw_names_by_key: dict[str, list[str]] = {}
    observed_units_by_key: dict[str, list[str]] = {}
    for marker_key, raw_name, original_unit in raw_name_result.all():
        if not raw_name:
            continue
        names = raw_names_by_key.setdefault(marker_key, [])
        if raw_name not in names and len(names) < 8:
            names.append(raw_name)
        if original_unit:
            units = observed_units_by_key.setdefault(marker_key, [])
            if original_unit not in units and len(units) < 8:
                units.append(original_unit)

    all_raw_names = [name for names in raw_names_by_key.values() for name in names]
    unresolved_names_by_key: dict[str, str] = {}
    if all_raw_names:
        alias_map = await load_measurement_type_aliases(session, all_raw_names)
        for marker_key in marker_keys:
            raw_names = raw_names_by_key.get(marker_key, [])
            if raw_names and not all(alias_map.get(name) is not None for name in raw_names):
                unresolved_names_by_key[marker_key] = raw_names[0]

    if unresolved_names_by_key:
        existing_canonical_result = await session.execute(
            select(MeasurementType.name).order_by(MeasurementType.name.asc())
        )
        existing_canonical = existing_canonical_result.scalars().all()
        representative_names = list(unresolved_names_by_key.values())
        raw_examples_by_name = {
            representative_name: raw_names_by_key.get(marker_key, [representative_name])
            for marker_key, representative_name in unresolved_names_by_key.items()
        }
        observed_units_by_name = {
            representative_name: observed_units_by_key.get(marker_key, [])
            for marker_key, representative_name in unresolved_names_by_key.items()
        }
        await _commit_before_external_normalization_call(session)
        normalized_map = await copilot_normalization.normalize_marker_names(
            representative_names,
            existing_canonical,
            raw_examples_by_name=raw_examples_by_name,
            observed_units_by_name=observed_units_by_name,
        )
        canonical_names = [
            normalized_map.get(representative_name, representative_name)
            for representative_name in representative_names
        ]
        measurement_types = await _ensure_measurement_types(session, canonical_names)
        alias_pairs: dict[str, tuple[str, MeasurementType]] = {}
        for marker_key, representative_name in unresolved_names_by_key.items():
            canonical_name = normalized_map.get(representative_name, representative_name)
            measurement_type = measurement_types[canonical_name]
            for raw_name in raw_names_by_key.get(marker_key, [representative_name]):
                normalized_alias_key = normalize_marker_alias_key(raw_name)
                if normalized_alias_key:
                    alias_pairs[normalized_alias_key] = (raw_name, measurement_type)
        if alias_pairs:
            await ensure_measurement_type_aliases(session, list(alias_pairs.values()))

    for _, job in marker_jobs:
        await job_service.mark_job_resolved(session, job)
    await _request_processing_for_measurement_keys(session, marker_keys)
    _log_batch_span_finish(TASK_CANONIZE_MARKER, [job for _, job in marker_jobs], started_at)


async def _canonize_group_jobs(session: AsyncSession, jobs: list[Job]) -> None:
    group_jobs: list[tuple[MeasurementType, Job]] = []
    payload_type_ids: dict[int, int] = {}
    for job in jobs:
        measurement_type_id = job_service.json_loads(job.payload_json).get("measurement_type_id")
        if not isinstance(measurement_type_id, int):
            await job_service.mark_job_resolved(session, job, {"skipped": True})
            continue
        payload_type_ids[job.id] = measurement_type_id

    measurement_type_ids = list(dict.fromkeys(payload_type_ids.values()))
    measurement_types_by_id: dict[int, MeasurementType] = {}
    if measurement_type_ids:
        result = await session.execute(select(MeasurementType).where(MeasurementType.id.in_(measurement_type_ids)))
        measurement_types_by_id = {measurement_type.id: measurement_type for measurement_type in result.scalars().all()}

    for job in jobs:
        measurement_type_id = payload_type_ids.get(job.id)
        if measurement_type_id is None:
            continue
        measurement_type = measurement_types_by_id.get(measurement_type_id)
        if measurement_type is None:
            await job_service.delete_job(session, job)
            continue
        group_jobs.append((measurement_type, job))

    group_type_ids = list(dict.fromkeys(measurement_type.id for measurement_type, _ in group_jobs))
    if not group_jobs:
        return
    started_at = _log_batch_span_start(TASK_CANONIZE_GROUP, [job for _, job in group_jobs])
    unresolved_types = [
        measurement_types_by_id[measurement_type_id]
        for measurement_type_id in group_type_ids
        if measurement_types_by_id[measurement_type_id].group_id is None
    ]
    if unresolved_types:
        group_names = await load_group_order(session)
        await _commit_before_external_normalization_call(session)
        resolved = await copilot_normalization.classify_marker_groups(
            [measurement_type.name for measurement_type in unresolved_types],
            group_names,
        )
        groups_by_name = await _load_or_create_groups(session, list(resolved.values()))
        for measurement_type in unresolved_types:
            group_name = resolved.get(measurement_type.name, DEFAULT_GROUP_NAME)
            group = groups_by_name[group_name]
            measurement_type.group_name = group.name
            measurement_type.group_id = group.id

    for _, job in group_jobs:
        await job_service.mark_job_resolved(session, job)
    await _request_processing_for_measurement_type_ids(session, group_type_ids)
    _log_batch_span_finish(TASK_CANONIZE_GROUP, [job for _, job in group_jobs], started_at)


async def _canonize_unit_jobs(session: AsyncSession, jobs: list[Job]) -> None:
    unit_jobs: list[tuple[MeasurementType, Job]] = []
    payload_type_ids: dict[int, int] = {}
    for job in jobs:
        measurement_type_id = job_service.json_loads(job.payload_json).get("measurement_type_id")
        if not isinstance(measurement_type_id, int):
            await job_service.mark_job_resolved(session, job, {"skipped": True})
            continue
        payload_type_ids[job.id] = measurement_type_id

    measurement_type_ids = list(dict.fromkeys(payload_type_ids.values()))
    measurement_types_by_id: dict[int, MeasurementType] = {}
    if measurement_type_ids:
        result = await session.execute(select(MeasurementType).where(MeasurementType.id.in_(measurement_type_ids)))
        measurement_types_by_id = {measurement_type.id: measurement_type for measurement_type in result.scalars().all()}

    for job in jobs:
        measurement_type_id = payload_type_ids.get(job.id)
        if measurement_type_id is None:
            continue
        measurement_type = measurement_types_by_id.get(measurement_type_id)
        if measurement_type is None:
            await job_service.delete_job(session, job)
            continue
        unit_jobs.append((measurement_type, job))

    unit_type_ids = list(dict.fromkeys(measurement_type.id for measurement_type, _ in unit_jobs))
    if not unit_jobs:
        return
    started_at = _log_batch_span_start(TASK_CANONIZE_UNIT, [job for _, job in unit_jobs])
    unresolved_type_ids = [
        measurement_type_id
        for measurement_type_id in unit_type_ids
        if not measurement_types_by_id[measurement_type_id].canonical_unit
    ]
    if unresolved_type_ids:
        measurement_result = await session.execute(
            select(Measurement)
            .where(
                Measurement.measurement_type_id.in_(unresolved_type_ids),
                Measurement.original_value.is_not(None),
            )
            .order_by(Measurement.measurement_type_id.asc(), Measurement.id.asc())
        )
        observations_by_type_id: dict[int, list[copilot_normalization.MarkerObservation]] = {
            measurement_type_id: [] for measurement_type_id in unresolved_type_ids
        }
        for measurement in measurement_result.scalars().all():
            if measurement.measurement_type_id not in observations_by_type_id:
                continue
            observations = observations_by_type_id[measurement.measurement_type_id]
            if len(observations) >= 24:
                continue
            observations.append(
                copilot_normalization.MarkerObservation(
                    id=str(measurement.id),
                    value=measurement.original_value,
                    unit=measurement.original_unit,
                    reference_low=measurement.original_reference_low,
                    reference_high=measurement.original_reference_high,
                )
            )

        marker_groups = [
            copilot_normalization.MarkerUnitGroup(
                marker_name=measurement_types_by_id[measurement_type_id].name,
                existing_canonical_unit=measurement_types_by_id[measurement_type_id].canonical_unit,
                observations=observations_by_type_id.get(measurement_type_id, []),
            )
            for measurement_type_id in unresolved_type_ids
        ]
        if marker_groups:
            await _commit_before_external_normalization_call(session)
            canonical_units = await copilot_normalization.choose_canonical_units(marker_groups)
            for measurement_type_id in unresolved_type_ids:
                measurement_type = measurement_types_by_id[measurement_type_id]
                if measurement_type.name in canonical_units:
                    measurement_type.canonical_unit = canonical_units[measurement_type.name]

    for _, job in unit_jobs:
        await job_service.mark_job_resolved(session, job)
    await _request_processing_for_measurement_type_ids(session, unit_type_ids)
    _log_batch_span_finish(TASK_CANONIZE_UNIT, [job for _, job in unit_jobs], started_at)


async def _canonize_conversion_jobs(session: AsyncSession, jobs: list[Job]) -> None:
    conversion_jobs: list[tuple[Job, MeasurementType, str, str]] = []
    payloads: dict[int, tuple[int, str, str]] = {}
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
            await job_service.mark_job_resolved(session, job, {"skipped": True})
            continue
        payloads[job.id] = (measurement_type_id, original_unit, canonical_unit)

    measurement_type_ids = list(dict.fromkeys(measurement_type_id for measurement_type_id, _, _ in payloads.values()))
    measurement_types_by_id: dict[int, MeasurementType] = {}
    if measurement_type_ids:
        result = await session.execute(select(MeasurementType).where(MeasurementType.id.in_(measurement_type_ids)))
        measurement_types_by_id = {measurement_type.id: measurement_type for measurement_type in result.scalars().all()}

    for job in jobs:
        payload = payloads.get(job.id)
        if payload is None:
            continue
        measurement_type_id, original_unit, canonical_unit = payload
        measurement_type = measurement_types_by_id.get(measurement_type_id)
        if measurement_type is None:
            await job_service.delete_job(session, job)
            continue
        conversion_jobs.append((job, measurement_type, original_unit, canonical_unit))

    if not conversion_jobs:
        return
    started_at = _log_batch_span_start(TASK_CANONIZE_CONVERSION, [job for job, _, _, _ in conversion_jobs])

    existing_rule_map = await rescaling.load_rescaling_rules(
        session,
        [
            (measurement_type.id, original_unit, canonical_unit)
            for _, measurement_type, original_unit, canonical_unit in conversion_jobs
        ],
    )
    llm_requests: list[copilot_normalization.UnitConversionRequest] = []
    upsert_entries: list[dict] = []
    for job, measurement_type, original_unit, canonical_unit in conversion_jobs:
        original_key = rescaling.normalize_unit_key(original_unit)
        canonical_key = rescaling.normalize_unit_key(canonical_unit)
        if (
            original_key is None
            or canonical_key is None
            or (measurement_type.id, original_key, canonical_key) in existing_rule_map
        ):
            continue
        result = await session.execute(
            select(Measurement)
            .where(
                Measurement.measurement_type_id == measurement_type.id,
                Measurement.normalized_original_unit == original_key,
                Measurement.original_value.is_not(None),
            )
            .order_by(Measurement.id.asc())
            .limit(3)
        )
        sample = result.scalars().first()
        if sample is None or sample.original_value is None:
            continue
        llm_requests.append(
            copilot_normalization.UnitConversionRequest(
                id=job.task_key,
                marker_name=measurement_type.name,
                original_unit=original_unit,
                canonical_unit=canonical_unit,
                example_value=sample.original_value,
                reference_low=sample.original_reference_low,
                reference_high=sample.original_reference_high,
            )
        )
        upsert_entries.append(
            {
                "task_key": job.task_key,
                "measurement_type": measurement_type,
                "original_unit": original_unit,
                "canonical_unit": canonical_unit,
            }
        )
    if llm_requests:
        await _commit_before_external_normalization_call(session)
        scale_factors = await copilot_normalization.infer_rescaling_factors(llm_requests)
        for entry in upsert_entries:
            entry["scale_factor"] = scale_factors.get(entry["task_key"])
        await rescaling.upsert_rescaling_rules(session, upsert_entries)

    for job, _, _, _ in conversion_jobs:
        await job_service.mark_job_resolved(session, job)
    await _request_processing_for_conversions(
        session,
        [(measurement_type.id, original_unit) for _, measurement_type, original_unit, _ in conversion_jobs],
    )
    _log_batch_span_finish(TASK_CANONIZE_CONVERSION, [job for job, _, _, _ in conversion_jobs], started_at)


async def _canonize_qualitative_jobs(session: AsyncSession, jobs: list[Job]) -> None:
    qualitative_jobs: list[tuple[str, Job]] = []
    for job in jobs:
        qualitative_key = job.task_key
        if not qualitative_key:
            await job_service.mark_job_resolved(session, job, {"skipped": True})
            continue
        qualitative_jobs.append((qualitative_key, job))

    qualitative_keys = list(dict.fromkeys(qualitative_key for qualitative_key, _ in qualitative_jobs))
    if not qualitative_keys:
        return

    started_at = _log_batch_span_start(TASK_CANONIZE_QUALITATIVE, [job for _, job in qualitative_jobs])
    existing_rules = await qualitative_values.load_qualitative_rules(session, qualitative_keys)
    unresolved_keys = [key for key in qualitative_keys if key not in existing_rules]
    if unresolved_keys:
        measurement_result = await session.execute(
            select(Measurement)
            .options(selectinload(Measurement.measurement_type))
            .where(Measurement.original_qualitative_value.is_not(None))
            .order_by(Measurement.id.asc())
        )
        samples_by_key: dict[str, Measurement] = {}
        for measurement in measurement_result.scalars().all():
            key = qualitative_values.normalize_qualitative_key(measurement.original_qualitative_value)
            if key in unresolved_keys and key not in samples_by_key:
                samples_by_key[key] = measurement
                if len(samples_by_key) == len(unresolved_keys):
                    break

        requests: list[copilot_normalization.QualitativeNormalizationRequest] = []
        for qualitative_key in unresolved_keys:
            sample = samples_by_key.get(qualitative_key)
            if sample is None or sample.original_qualitative_value is None:
                continue
            threshold_result = qualitative_values.infer_threshold_qualitative_result(
                sample.original_qualitative_value,
                reference_low=sample.original_reference_low,
                reference_high=sample.original_reference_high,
            )
            if threshold_result is not None:
                continue
            requests.append(
                copilot_normalization.QualitativeNormalizationRequest(
                    id=qualitative_key,
                    marker_name=sample.marker_name,
                    original_value=sample.original_qualitative_value,
                    reference_low=sample.original_reference_low,
                    reference_high=sample.original_reference_high,
                )
            )
        if requests:
            existing_values_result = await session.execute(
                select(QualitativeRule.canonical_value).distinct().order_by(QualitativeRule.canonical_value.asc())
            )
            existing_values = existing_values_result.scalars().all()
            await _commit_before_external_normalization_call(session)
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

    for _, job in qualitative_jobs:
        await job_service.mark_job_resolved(session, job)
    await _request_processing_for_qualitative_keys(session, qualitative_keys)
    _log_batch_span_finish(TASK_CANONIZE_QUALITATIVE, [job for _, job in qualitative_jobs], started_at)


async def _refresh_search(session: AsyncSession, job: Job) -> None:
    payload = job_service.json_loads(job.payload_json)
    file_id = int(payload.get("file_id", 0))
    file = await session.get(LabFile, file_id, options=[selectinload(LabFile.tags)])
    if file is None:
        await job_service.delete_job(session, job)
        return
    progress = await get_file_progress(session, file)
    if not progress.is_complete:
        await search_service.remove_lab_search_document(file.id, session)
        await job_service.mark_job_resolved(session, job, {"skipped": True})
        return
    if not await _file_needs_search_refresh(session, file):
        await job_service.mark_job_resolved(session, job, {"skipped": True})
        return
    started_at = _log_task_span_start(job, [file])
    await search_service.refresh_lab_search_document(file.id, session)
    file.search_indexed_at = utc_now()
    await job_service.mark_job_resolved(session, job)
    _log_task_span_finish(job, [file], started_at)


async def get_file_progress(session: AsyncSession, file: LabFile) -> FileProgressSnapshot:
    measurement_ranges = await _load_measurement_batch_ranges(session, file.id)
    text_ranges = await _load_text_batch_ranges(session, file.id)
    measurement_pages_done = _covered_page_count(file.page_count, measurement_ranges)
    text_pages_done = _covered_page_count(file.page_count, text_ranges)

    count_result = await session.execute(
        select(Measurement.normalization_status, func.count(Measurement.id))
        .where(Measurement.lab_file_id == file.id)
        .group_by(Measurement.normalization_status)
    )
    counts = {status: count for status, count in count_result.all()}
    ready_measurements = counts.get(MEASUREMENT_STATE_RESOLVED, 0)
    total_measurements = sum(counts.values())
    measurement_error_count = counts.get(MEASUREMENT_STATE_ERROR, 0)
    summary_ready = file.summary_generated_at is not None
    source_ready = file.source_resolved_at is not None
    is_complete = (
        measurement_pages_done >= file.page_count
        and text_pages_done >= file.page_count
        and file.text_assembled_at is not None
        and summary_ready
        and source_ready
        and measurement_error_count == 0
        and ready_measurements == total_measurements
    )
    search_ready = file.search_indexed_at is not None and not await _search_needs_refresh_by_timestamp(
        session,
        file,
        is_complete=is_complete,
    )
    return FileProgressSnapshot(
        measurement_pages_done=measurement_pages_done,
        measurement_pages_total=file.page_count,
        text_pages_done=text_pages_done,
        text_pages_total=file.page_count,
        ready_measurements=ready_measurements,
        total_measurements=total_measurements,
        summary_ready=summary_ready,
        source_ready=source_ready,
        search_ready=search_ready,
        measurement_error_count=measurement_error_count,
        is_complete=is_complete,
    )


async def _refresh_file_status_projection(
    session: AsyncSession,
    file: LabFile,
    *,
    progress: FileProgressSnapshot | None = None,
) -> None:
    progress = progress or await get_file_progress(session, file)
    failed_jobs_result = await session.execute(
        select(Job.error_text)
        .where(Job.file_id == file.id, Job.status == job_service.JOB_STATUS_FAILED)
        .order_by(Job.updated_at.desc(), Job.id.desc())
        .limit(1)
    )
    failed_error = failed_jobs_result.scalars().first()
    source_failed_error = None
    if file.source_candidate_key and file.source_resolved_at is None:
        source_failed_result = await session.execute(
            select(Job.error_text)
            .where(
                Job.task_type == TASK_CANONIZE_SOURCE,
                Job.task_key == file.source_candidate_key,
                Job.status == job_service.JOB_STATUS_FAILED,
            )
            .order_by(Job.updated_at.desc(), Job.id.desc())
            .limit(1)
        )
        source_failed_error = source_failed_result.scalars().first()
    job_status_result = await session.execute(select(Job.status).where(Job.file_id == file.id))
    job_statuses = job_status_result.scalars().all()
    has_pending = any(status == job_service.JOB_STATUS_PENDING for status in job_statuses)
    has_leased = any(status == job_service.JOB_STATUS_LEASED for status in job_statuses)
    has_failed = (
        any(status == job_service.JOB_STATUS_FAILED for status in job_statuses)
        or source_failed_error is not None
    )

    if progress.is_complete:
        file.status = FILE_STATUS_COMPLETE
        file.processing_error = None
        return

    if progress.measurement_error_count > 0 or has_failed:
        file.status = FILE_STATUS_ERROR
        if failed_error:
            file.processing_error = failed_error
        elif source_failed_error:
            file.processing_error = source_failed_error
        elif file.processing_error is None:
            measurement_error_result = await session.execute(
                select(Measurement.normalization_error)
                .where(
                    Measurement.lab_file_id == file.id,
                    Measurement.normalization_status == MEASUREMENT_STATE_ERROR,
                    Measurement.normalization_error.is_not(None),
                )
                .order_by(Measurement.updated_at.desc(), Measurement.id.desc())
                .limit(1)
            )
            file.processing_error = measurement_error_result.scalars().first()
        return

    if has_leased:
        file.status = FILE_STATUS_PROCESSING
        return
    if has_pending:
        file.status = FILE_STATUS_QUEUED
        return
    if (
        progress.measurement_pages_done > 0
        or progress.text_pages_done > 0
        or progress.total_measurements > 0
        or file.text_assembled_at is not None
        or file.summary_generated_at is not None
        or file.source_resolved_at is not None
    ):
        file.status = FILE_STATUS_PROCESSING
        return
    file.status = FILE_STATUS_UPLOADED
    file.processing_error = None


async def _apply_known_measurement_rules(session: AsyncSession, measurements: list[Measurement]) -> bool:
    requested_new_work = False
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
        TASK_CANONIZE_MARKER,
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
                measurement.normalization_error = f"Marker canonization failed for {measurement.raw_marker_name}"
                continue
            requested_new_work = (
                await _request_job(
                    session,
                    task_type=TASK_CANONIZE_MARKER,
                    task_key=task_key,
                    payload={"raw_name": measurement.raw_marker_name},
                    priority=PRIORITY_CANONIZE,
                )
                or requested_new_work
            )
            measurement.normalization_status = MEASUREMENT_STATE_PENDING
            continue
        measurement.measurement_type_id = measurement_type.id

    await session.flush()

    type_ids = sorted(
        {measurement.measurement_type_id for measurement in measurements if measurement.measurement_type_id is not None}
    )
    if not type_ids:
        return requested_new_work
    type_result = await session.execute(
        select(MeasurementType).options(selectinload(MeasurementType.group)).where(MeasurementType.id.in_(type_ids))
    )
    measurement_types = {measurement_type.id: measurement_type for measurement_type in type_result.scalars().all()}

    group_job_statuses = await _load_job_statuses(
        session,
        TASK_CANONIZE_GROUP,
        [measurement_type.normalized_key for measurement_type in measurement_types.values()],
    )
    unit_job_statuses = await _load_job_statuses(
        session,
        TASK_CANONIZE_UNIT,
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
            threshold_result = qualitative_values.infer_threshold_qualitative_result(
                measurement.original_qualitative_value,
                reference_low=measurement.original_reference_low,
                reference_high=measurement.original_reference_high,
            )
            if threshold_result is None:
                qualitative_key = qualitative_values.normalize_qualitative_key(measurement.original_qualitative_value)
                if qualitative_key:
                    qualitative_task_keys.add(_qualitative_task_key(qualitative_key))

    conversion_rule_map = await rescaling.load_rescaling_rules(session, conversion_requests)
    conversion_job_statuses = await _load_job_statuses(session, TASK_CANONIZE_CONVERSION, list(conversion_task_keys))
    qualitative_job_statuses = await _load_job_statuses(session, TASK_CANONIZE_QUALITATIVE, list(qualitative_task_keys))

    for measurement in measurements:
        measurement_type = measurement_types.get(measurement.measurement_type_id)
        if measurement_type is None:
            continue

        resolved = True
        if measurement_type.group_id is None:
            task_key = _group_task_key(measurement_type.normalized_key)
            if group_job_statuses.get(task_key) == job_service.JOB_STATUS_FAILED:
                measurement.normalization_status = MEASUREMENT_STATE_ERROR
                measurement.normalization_error = f"Group canonization failed for {measurement_type.name}"
                continue
            requested_new_work = (
                await _request_job(
                    session,
                    task_type=TASK_CANONIZE_GROUP,
                    task_key=task_key,
                    payload={"measurement_type_id": measurement_type.id},
                    priority=PRIORITY_CANONIZE,
                )
                or requested_new_work
            )
            resolved = False

        if measurement.original_value is not None:
            if measurement.original_unit and not measurement_type.canonical_unit:
                task_key = _canonical_unit_task_key(measurement_type.normalized_key)
                if unit_job_statuses.get(task_key) == job_service.JOB_STATUS_FAILED:
                    measurement.normalization_status = MEASUREMENT_STATE_ERROR
                    measurement.normalization_error = f"Canonical unit canonization failed for {measurement_type.name}"
                    continue
                requested_new_work = (
                    await _request_job(
                        session,
                        task_type=TASK_CANONIZE_UNIT,
                        task_key=task_key,
                        payload={"measurement_type_id": measurement_type.id},
                        priority=PRIORITY_CANONIZE,
                    )
                    or requested_new_work
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
                        measurement.normalization_status = MEASUREMENT_STATE_ERROR
                        measurement.normalization_error = (
                            f"Unsupported unit normalization for {measurement_type.name}"
                        )
                        continue
                    else:
                        task_key = _conversion_task_key(measurement_type.normalized_key, original_key, canonical_key)
                        rule = conversion_rule_map.get((measurement_type.id, original_key, canonical_key))
                        if rule is None:
                            if conversion_job_statuses.get(task_key) == job_service.JOB_STATUS_FAILED:
                                measurement.normalization_status = MEASUREMENT_STATE_ERROR
                                measurement.normalization_error = (
                                    f"Conversion canonization failed for {measurement_type.name}"
                                )
                                continue
                            requested_new_work = (
                                await _request_job(
                                    session,
                                    task_type=TASK_CANONIZE_CONVERSION,
                                    task_key=task_key,
                                    payload={
                                        "measurement_type_id": measurement_type.id,
                                        "original_unit": measurement.original_unit,
                                        "canonical_unit": measurement_type.canonical_unit,
                                    },
                                    priority=PRIORITY_CANONIZE,
                                )
                                or requested_new_work
                            )
                            resolved = False
                        elif rule.scale_factor is None:
                            # A persisted null scale factor means the
                            # normalization lane already concluded there is no
                            # safe simple multiplicative conversion for this
                            # unit pair, so re-enqueueing would only spin.
                            # Keep the measurement resolved and let the API/UI
                            # fall back to the original value/unit with a
                            # conversion-missing warning.
                            measurement.normalization_status = MEASUREMENT_STATE_RESOLVED
                            measurement.normalization_error = None
                            continue
                        else:
                            measurement.canonical_value = rescaling.apply_scale_factor(
                                measurement.original_value, rule.scale_factor
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
            # Comparator cutoffs such as ">1.5" are only meaningful alongside
            # the measurement's own reference bounds, so resolve those directly
            # here instead of persisting a global raw-value rule that could be
            # wrong for a different assay.
            threshold_result = qualitative_values.infer_threshold_qualitative_result(
                measurement.original_qualitative_value,
                reference_low=measurement.original_reference_low,
                reference_high=measurement.original_reference_high,
            )
            if threshold_result is not None:
                measurement.qualitative_value, measurement.qualitative_bool = threshold_result
            else:
                qualitative_key = qualitative_values.normalize_qualitative_key(measurement.original_qualitative_value)
                if qualitative_key is None:
                    measurement.normalization_status = MEASUREMENT_STATE_ERROR
                    measurement.normalization_error = (
                        f"Unsupported qualitative normalization for {measurement.original_qualitative_value}"
                    )
                    continue
                rule = qualitative_rule_map.get(qualitative_key)
                if rule is None:
                    task_key = _qualitative_task_key(qualitative_key)
                    if qualitative_job_statuses.get(task_key) == job_service.JOB_STATUS_FAILED:
                        measurement.normalization_status = MEASUREMENT_STATE_ERROR
                        measurement.normalization_error = (
                            f"Qualitative canonization failed for {measurement.original_qualitative_value}"
                        )
                        continue
                    requested_new_work = (
                        await _request_job(
                            session,
                            task_type=TASK_CANONIZE_QUALITATIVE,
                            task_key=task_key,
                            payload={"original_value": measurement.original_qualitative_value},
                            priority=PRIORITY_CANONIZE,
                        )
                        or requested_new_work
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

    return requested_new_work


async def _handle_terminal_job_failure(session: AsyncSession, job: Job, error_text: str) -> None:
    if job.task_type == TASK_REFRESH_SEARCH:
        return
    if job.file_id is not None:
        file = await session.get(LabFile, job.file_id)
        if file is not None:
            file.processing_error = error_text
            if file.status != FILE_STATUS_COMPLETE:
                file.status = FILE_STATUS_ERROR

    payload = job_service.json_loads(job.payload_json)
    if job.task_type == TASK_CANONIZE_MARKER:
        await _request_processing_for_measurement_keys(session, [job.task_key])
    elif job.task_type in {TASK_CANONIZE_GROUP, TASK_CANONIZE_UNIT}:
        measurement_type_id = payload.get("measurement_type_id")
        if isinstance(measurement_type_id, int):
            await _request_processing_for_measurement_type_ids(session, [measurement_type_id])
    elif job.task_type == TASK_CANONIZE_CONVERSION:
        measurement_type_id = payload.get("measurement_type_id")
        original_unit = payload.get("original_unit")
        if isinstance(measurement_type_id, int) and isinstance(original_unit, str):
            await _request_processing_for_conversion(session, measurement_type_id, original_unit)
    elif job.task_type == TASK_CANONIZE_QUALITATIVE:
        await _request_processing_for_qualitative_keys(session, [job.task_key])
    elif job.task_type == TASK_CANONIZE_SOURCE:
        await _mark_source_failure(session, job.task_key, error_text)


async def _commit_before_external_normalization_call(session: AsyncSession) -> None:
    # Release the current SQLite write transaction before waiting on Copilot so
    # other workers are not blocked by a long-lived write lock.
    await session.commit()


async def _request_job(
    session: AsyncSession,
    *,
    task_type: str,
    task_key: str,
    payload: dict | None = None,
    file_id: int | None = None,
    priority: int,
) -> bool:
    task_key = task_key.strip()
    if not task_key:
        raise ValueError(f"Cannot enqueue {task_type} with an empty task key")
    existing_status_result = await session.execute(
        select(Job.status, Job.rerun_requested)
        .where(Job.task_type == task_type, Job.task_key == task_key)
        .limit(1)
    )
    existing = existing_status_result.one_or_none()
    existing_status = existing[0] if existing is not None else None
    existing_rerun_requested = bool(existing[1]) if existing is not None else False
    if existing_status in {job_service.JOB_STATUS_FAILED, job_service.JOB_STATUS_CANCELLED}:
        return False
    await job_service.enqueue_job(
        session,
        task_type=task_type,
        task_key=task_key,
        payload=payload,
        file_id=file_id,
        priority=priority,
    )
    return (
        existing_status is None
        or existing_status == job_service.JOB_STATUS_RESOLVED
        or (existing_status == job_service.JOB_STATUS_LEASED and not existing_rerun_requested)
    )


async def _request_file_ensure(session: AsyncSession, file_id: int) -> None:
    await _request_job(
        session,
        task_type=TASK_ENSURE_FILE,
        task_key=f"file:{file_id}",
        payload={"file_id": file_id},
        file_id=file_id,
        priority=PRIORITY_ENSURE_FILE,
    )


async def _request_measurement_extraction_ensure(session: AsyncSession, file_id: int) -> None:
    await _request_job(
        session,
        task_type=TASK_ENSURE_MEASUREMENT_EXTRACTION,
        task_key=f"file:{file_id}",
        payload={"file_id": file_id},
        file_id=file_id,
        priority=PRIORITY_ENSURE_LANE,
    )


async def _request_text_ensure(session: AsyncSession, file_id: int) -> None:
    await _request_job(
        session,
        task_type=TASK_ENSURE_TEXT,
        task_key=f"file:{file_id}",
        payload={"file_id": file_id},
        file_id=file_id,
        priority=PRIORITY_ENSURE_LANE,
    )


async def _request_measurement_extraction_batch(
    session: AsyncSession, file_id: int, start_page: int, stop_page: int, dpi: int
) -> None:
    await _request_job(
        session,
        task_type=TASK_EXTRACT_MEASUREMENTS,
        task_key=_batch_task_key("measurements", file_id, start_page, stop_page, dpi),
        payload={
            "file_id": file_id,
            "start_page": start_page,
            "stop_page": stop_page,
            "dpi": dpi,
        },
        file_id=file_id,
        priority=PRIORITY_EXTRACT_MEASUREMENTS,
    )


async def _request_text_extraction_batch(
    session: AsyncSession, file_id: int, start_page: int, stop_page: int, dpi: int
) -> None:
    await _request_job(
        session,
        task_type=TASK_EXTRACT_TEXT,
        task_key=_batch_task_key("text", file_id, start_page, stop_page, dpi),
        payload={
            "file_id": file_id,
            "start_page": start_page,
            "stop_page": stop_page,
            "dpi": dpi,
        },
        file_id=file_id,
        priority=PRIORITY_EXTRACT_TEXT,
    )


async def _request_assemble_text(session: AsyncSession, file_id: int) -> None:
    await _request_job(
        session,
        task_type=TASK_ASSEMBLE_TEXT,
        task_key=f"file:{file_id}:assemble-text",
        payload={"file_id": file_id},
        file_id=file_id,
        priority=PRIORITY_ASSEMBLE_TEXT,
    )


async def _request_process_measurements(session: AsyncSession, file_id: int) -> None:
    await _request_job(
        session,
        task_type=TASK_PROCESS_MEASUREMENTS,
        task_key=f"file:{file_id}:process-measurements",
        payload={"file_id": file_id},
        file_id=file_id,
        priority=PRIORITY_PROCESS_MEASUREMENTS,
    )


async def _request_summary(session: AsyncSession, file_id: int) -> None:
    await _request_job(
        session,
        task_type=TASK_GENERATE_SUMMARY,
        task_key=f"file:{file_id}:summary",
        payload={"file_id": file_id},
        file_id=file_id,
        priority=PRIORITY_SUMMARY,
    )


async def _request_search_refresh(session: AsyncSession, file_id: int) -> None:
    await _request_job(
        session,
        task_type=TASK_REFRESH_SEARCH,
        task_key=f"file:{file_id}:search",
        payload={"file_id": file_id},
        file_id=file_id,
        priority=PRIORITY_SEARCH,
    )


async def _request_source_canonization(session: AsyncSession, file: LabFile) -> None:
    if not file.source_candidate_key:
        return
    await _request_job(
        session,
        task_type=TASK_CANONIZE_SOURCE,
        task_key=file.source_candidate_key,
        payload={
            "file_id": file.id,
            "source_candidate": file.source_candidate,
            "source_candidate_key": file.source_candidate_key,
        },
        priority=PRIORITY_CANONIZE,
    )


async def _request_processing_for_measurement_keys(session: AsyncSession, keys: list[str]) -> None:
    if not keys:
        return
    result = await session.execute(
        select(Measurement.lab_file_id).where(Measurement.normalized_marker_key.in_(keys)).distinct()
    )
    for file_id in result.scalars().all():
        await _request_process_measurements(session, file_id)
        await _request_file_ensure(session, file_id)


async def _request_processing_for_measurement_type_ids(session: AsyncSession, measurement_type_ids: list[int]) -> None:
    if not measurement_type_ids:
        return
    result = await session.execute(
        select(Measurement.lab_file_id).where(Measurement.measurement_type_id.in_(measurement_type_ids)).distinct()
    )
    for file_id in result.scalars().all():
        await _request_process_measurements(session, file_id)
        await _request_file_ensure(session, file_id)


async def _request_processing_for_conversion(
    session: AsyncSession, measurement_type_id: int, original_unit: str
) -> None:
    original_key = rescaling.normalize_unit_key(original_unit)
    if original_key is None:
        return
    result = await session.execute(
        select(Measurement.lab_file_id)
        .where(
            Measurement.measurement_type_id == measurement_type_id,
            Measurement.normalized_original_unit == original_key,
        )
        .distinct()
    )
    for file_id in result.scalars().all():
        await _request_process_measurements(session, file_id)
        await _request_file_ensure(session, file_id)


async def _request_processing_for_conversions(
    session: AsyncSession,
    requests: list[tuple[int, str]],
) -> None:
    for measurement_type_id, original_unit in dict.fromkeys(requests):
        await _request_processing_for_conversion(session, measurement_type_id, original_unit)


async def _request_processing_for_qualitative_keys(session: AsyncSession, keys: list[str]) -> None:
    normalized_keys = [key for key in keys if key]
    if not normalized_keys:
        return
    result = await session.execute(
        select(Measurement.lab_file_id, Measurement.original_qualitative_value).where(
            Measurement.original_qualitative_value.is_not(None)
        )
    )
    affected_file_ids: set[int] = set()
    for file_id, original_value in result.all():
        normalized_value = qualitative_values.normalize_qualitative_key(original_value)
        if normalized_value in normalized_keys:
            affected_file_ids.add(file_id)
    for file_id in sorted(affected_file_ids):
        await _request_process_measurements(session, file_id)
        await _request_file_ensure(session, file_id)


async def _mark_source_resolved(session: AsyncSession, file: LabFile, canonical_source: str | None) -> None:
    file.source_name = canonical_source
    file.source_resolved_at = utc_now()
    if canonical_source:
        await _set_file_source_tag(session, file, canonical_source)
        return
    if file.tags:
        for tag in list(file.tags):
            if tag.tag.casefold().startswith(SOURCE_TAG_PREFIX):
                await session.delete(tag)


async def _mark_source_failure(session: AsyncSession, source_key: str, error_text: str) -> None:
    if not source_key:
        return
    result = await session.execute(
        select(LabFile).options(selectinload(LabFile.tags)).where(LabFile.source_candidate_key == source_key)
    )
    for file in result.scalars().unique().all():
        file.processing_error = error_text
        file.status = FILE_STATUS_ERROR
        await _request_file_ensure(session, file.id)


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


async def _load_file_measurements(session: AsyncSession, file_id: int) -> list[Measurement]:
    result = await session.execute(
        select(Measurement)
        .options(selectinload(Measurement.measurement_type))
        .where(Measurement.lab_file_id == file_id)
        .order_by(Measurement.id.asc())
    )
    return list(result.scalars().all())


async def _load_measurement_batch_ranges(session: AsyncSession, file_id: int) -> list[tuple[int, int]]:
    result = await session.execute(
        select(MeasurementBatch.start_page, MeasurementBatch.stop_page).where(MeasurementBatch.file_id == file_id)
    )
    return [(start_page, stop_page) for start_page, stop_page in result.all()]


async def _load_text_batch_ranges(session: AsyncSession, file_id: int) -> list[tuple[int, int]]:
    result = await session.execute(
        select(TextBatch.start_page, TextBatch.stop_page).where(TextBatch.file_id == file_id)
    )
    return [(start_page, stop_page) for start_page, stop_page in result.all()]


async def _load_scheduled_extraction_ranges(
    session: AsyncSession,
    file_id: int,
    *,
    measurement_mode: bool,
) -> list[tuple[int, int]]:
    task_type = TASK_EXTRACT_MEASUREMENTS if measurement_mode else TASK_EXTRACT_TEXT
    result = await session.execute(
        select(Job.payload_json).where(
            Job.file_id == file_id,
            Job.task_type == task_type,
            Job.status.in_([job_service.JOB_STATUS_PENDING, job_service.JOB_STATUS_LEASED]),
        )
    )
    ranges: list[tuple[int, int]] = []
    for payload_json in result.scalars().all():
        payload = job_service.json_loads(payload_json)
        start_page = payload.get("start_page")
        stop_page = payload.get("stop_page")
        if isinstance(start_page, int) and isinstance(stop_page, int) and start_page < stop_page:
            ranges.append((start_page, stop_page))
    return ranges


async def _page_range_is_fully_covered(
    session: AsyncSession,
    file_id: int,
    start_page: int,
    stop_page: int,
    *,
    measurement_mode: bool,
) -> bool:
    ranges = await (
        _load_measurement_batch_ranges(session, file_id)
        if measurement_mode
        else _load_text_batch_ranges(session, file_id)
    )
    covered = _covered_pages(0 if not ranges else max(stop for _, stop in ranges), ranges)
    return all(page in covered for page in range(start_page, stop_page))


async def _file_needs_search_refresh(session: AsyncSession, file: LabFile) -> bool:
    progress = await get_file_progress(session, file)
    return await _search_needs_refresh_by_timestamp(session, file, is_complete=progress.is_complete)


async def _search_needs_refresh_by_timestamp(
    session: AsyncSession,
    file: LabFile,
    *,
    is_complete: bool,
) -> bool:
    if not is_complete:
        return False
    if file.search_indexed_at is None:
        return True
    latest_measurement_result = await session.execute(
        select(func.max(Measurement.updated_at)).where(
            Measurement.lab_file_id == file.id,
            Measurement.normalization_status == MEASUREMENT_STATE_RESOLVED,
        )
    )
    latest_measurement_update = latest_measurement_result.scalar_one_or_none()
    freshness_candidates = [
        candidate
        for candidate in [
            file.text_assembled_at,
            file.summary_generated_at,
            file.source_resolved_at,
            latest_measurement_update,
        ]
        if candidate is not None
    ]
    latest_input = max(freshness_candidates, default=file.search_indexed_at)
    return latest_input > file.search_indexed_at


def _coverage_complete(page_count: int, ranges: list[tuple[int, int]]) -> bool:
    return _covered_page_count(page_count, ranges) >= page_count


def _covered_pages(page_count: int, ranges: list[tuple[int, int]]) -> set[int]:
    covered: set[int] = set()
    upper_bound = max(page_count, max((stop_page for _, stop_page in ranges), default=0))
    for start_page, stop_page in ranges:
        for page in range(max(0, start_page), min(stop_page, upper_bound)):
            covered.add(page)
    return covered


def _covered_page_count(page_count: int, ranges: list[tuple[int, int]]) -> int:
    return len(_covered_pages(page_count, ranges))


def _missing_page_ranges(page_count: int, ranges: list[tuple[int, int]], batch_size: int) -> list[tuple[int, int]]:
    covered = _covered_pages(page_count, ranges)
    ranges_to_enqueue: list[tuple[int, int]] = []
    page = 0
    while page < page_count:
        if page in covered:
            page += 1
            continue
        start_page = page
        stop_page = min(page_count, start_page + batch_size)
        while stop_page > start_page and any(candidate in covered for candidate in range(start_page, stop_page)):
            stop_page -= 1
        if stop_page == start_page:
            stop_page = start_page + 1
        ranges_to_enqueue.append((start_page, stop_page))
        page = stop_page
    return ranges_to_enqueue


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


async def _load_job_statuses(session: AsyncSession, task_type: str, task_keys: list[str]) -> dict[str, str]:
    unique_task_keys = [task_key for task_key in dict.fromkeys(task_keys) if task_key]
    if not unique_task_keys:
        return {}
    result = await session.execute(
        select(Job.task_key, Job.status).where(Job.task_type == task_type, Job.task_key.in_(unique_task_keys))
    )
    return {task_key: status for task_key, status in result.all()}


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
            if measurement.normalization_status == MEASUREMENT_STATE_RESOLVED
        ],
    }


def _fallback_batch_ranges(start_page: int, stop_page: int, dpi: int) -> list[tuple[int, int, int]]:
    page_count = stop_page - start_page
    if page_count > 1:
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
