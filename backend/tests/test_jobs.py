from __future__ import annotations

import asyncio
import sqlite3
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.exc import OperationalError

from illdashboard.models import Job, LabFile, Measurement, TextBatch, utc_now
from illdashboard.services import jobs as job_service
from illdashboard.services import pipeline

TASK_TYPE = "test.task"
TASK_KEY = "key-1"


@pytest.mark.asyncio
@pytest.mark.parametrize("completed_pages", [0, 1, 2])
async def test_ensure_text_waits_for_persisted_coverage(session_factory, completed_pages):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="coverage.pdf", filepath="/tmp/coverage.pdf", mime_type="application/pdf", page_count=2
        )
        session.add(lab_file)
        await session.flush()
        if completed_pages:
            session.add(
                TextBatch(
                    file_id=lab_file.id,
                    task_key="completed-text",
                    start_page=0,
                    stop_page=completed_pages,
                    dpi=144,
                    raw_text="Synthetic report",
                )
            )
        if completed_pages < 2:
            await pipeline._request_text_extraction_batch(session, lab_file.id, completed_pages, 2, 144)
        await pipeline._request_text_ensure(session, lab_file.id)
        await session.commit()
        jobs = await job_service.claim_jobs(
            session,
            task_types=[pipeline.TASK_ENSURE_TEXT],
            lease_owner="test",
            limit=1,
            lease_seconds=60,
        )
        await pipeline._ensure_text(session, jobs[0])
        await session.commit()

        assembly = await session.scalar(select(Job).where(Job.task_type == pipeline.TASK_ASSEMBLE_TEXT))
        assert (assembly is not None) == (completed_pages == 2)


@pytest.mark.asyncio
async def test_premature_text_assembly_does_not_requeue_controller(session_factory):
    async with session_factory() as session:
        lab_file = LabFile(filename="waiting.png", filepath="/tmp/waiting.png", mime_type="image/png")
        session.add(lab_file)
        await session.flush()
        await pipeline._request_assemble_text(session, lab_file.id)
        await session.commit()
        jobs = await job_service.claim_jobs(
            session,
            task_types=[pipeline.TASK_ASSEMBLE_TEXT],
            lease_owner="test",
            limit=1,
            lease_seconds=60,
        )
        await pipeline._assemble_text(session, jobs[0])
        await session.commit()

        assert jobs[0].status == job_service.JOB_STATUS_RESOLVED
        assert job_service.json_loads(jobs[0].resolved_json) == {"waiting": True}
        assert await session.scalar(select(Job).where(Job.task_type == pipeline.TASK_ENSURE_TEXT)) is None


@pytest.mark.asyncio
async def test_text_workers_fill_shared_extraction_capacity(session_factory, monkeypatch):
    started = 0
    capacity_filled = asyncio.Event()
    release = asyncio.Event()

    async def extract_text(*args, **kwargs):
        nonlocal started
        started += 1
        if started == pipeline.COPILOT_EXTRACTION_CONCURRENCY:
            capacity_filled.set()
        await release.wait()
        return {"raw_text": "Synthetic report"}

    monkeypatch.setattr(pipeline.copilot_extraction, "extract_text_batch", extract_text)
    monkeypatch.setattr(pipeline.copilot_extraction, "extract_measurement_batch", AsyncMock(return_value={}))
    monkeypatch.setattr(pipeline.copilot_extraction, "generate_summary", AsyncMock(return_value={}))
    runtime = pipeline.PipelineRuntime(session_factory)
    await runtime.start()
    try:
        async with session_factory() as session:
            for index in range(pipeline.COPILOT_EXTRACTION_CONCURRENCY + 1):
                lab_file = LabFile(
                    filename=f"capacity-{index}.png",
                    filepath=f"/tmp/capacity-{index}.png",
                    mime_type="image/png",
                )
                session.add(lab_file)
                await session.flush()
                await pipeline._request_text_extraction_batch(session, lab_file.id, 0, 1, 144)
            await session.commit()

        await asyncio.wait_for(capacity_filled.wait(), timeout=5)
        async with session_factory() as session:
            counts = dict(
                (
                    await session.execute(
                        select(Job.status, func.count())
                        .where(Job.task_type == pipeline.TASK_EXTRACT_TEXT)
                        .group_by(Job.status)
                    )
                ).all()
            )
        assert started == pipeline.COPILOT_EXTRACTION_CONCURRENCY
        assert counts == {
            job_service.JOB_STATUS_LEASED: pipeline.COPILOT_EXTRACTION_CONCURRENCY,
            job_service.JOB_STATUS_PENDING: 1,
        }
    finally:
        await runtime.stop()


async def _enqueue(session_factory) -> None:
    async with session_factory() as session:
        await job_service.enqueue_job(session, task_type=TASK_TYPE, task_key=TASK_KEY, payload={"n": 1})
        await session.commit()


async def _claim(session, owner: str, *, lease_seconds: int = 60) -> Job:
    claimed = await job_service.claim_jobs(
        session, task_types=[TASK_TYPE], lease_owner=owner, limit=1, lease_seconds=lease_seconds
    )
    assert len(claimed) == 1
    return claimed[0]


async def _load_job(session_factory) -> Job:
    async with session_factory() as session:
        return (await session.execute(select(Job).where(Job.task_key == TASK_KEY))).scalar_one()


@pytest.mark.asyncio
async def test_mark_job_resolved_honors_rerun_requested_from_another_session(session_factory):
    await _enqueue(session_factory)
    async with session_factory() as worker_session:
        job = await _claim(worker_session, "worker-a")

        # A second session asks for a rerun while the worker still holds its stale ORM copy.
        await _enqueue(session_factory)

        await job_service.mark_job_resolved(worker_session, job, {"done": True})
        await worker_session.commit()

    stored = await _load_job(session_factory)
    assert stored.status == job_service.JOB_STATUS_PENDING
    assert stored.rerun_requested is False
    assert stored.resolved_json is None
    assert stored.lease_owner is None


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["resolve", "release", "fail"])
async def test_stale_owner_cannot_finish_job_after_lease_theft(session_factory, transition):
    await _enqueue(session_factory)
    async with session_factory() as stale_session:
        stale_job = await _claim(stale_session, "worker-a", lease_seconds=-1)

        async with session_factory() as thief_session:
            await _claim(thief_session, "worker-b")

        with pytest.raises(job_service.JobLeaseLostError):
            if transition == "resolve":
                await job_service.mark_job_resolved(stale_session, stale_job, {"stale": True})
            elif transition == "release":
                await job_service.release_job(stale_session, stale_job, delay_seconds=0, error_text="boom")
            else:
                await job_service.mark_job_failed(stale_session, stale_job, error_text="boom")
        await stale_session.rollback()

    stored = await _load_job(session_factory)
    assert stored.status == job_service.JOB_STATUS_LEASED
    assert stored.lease_owner == "worker-b"
    assert stored.resolved_json is None
    assert stored.error_text is None


@pytest.mark.asyncio
async def test_renew_job_leases_extends_own_lease_and_reports_stolen_jobs(session_factory):
    await _enqueue(session_factory)
    async with session_factory() as session:
        job = await _claim(session, "worker-a", lease_seconds=5)
        original_until = job.lease_until

    async with session_factory() as session:
        stolen = await job_service.renew_job_leases(
            session, job_ids=[job.id], lease_owner="worker-a", lease_seconds=600
        )
    assert stolen == []
    renewed = await _load_job(session_factory)
    assert renewed.lease_until is not None and original_until is not None
    assert renewed.lease_until > original_until

    async with session_factory() as session:
        stolen = await job_service.renew_job_leases(
            session, job_ids=[job.id], lease_owner="worker-z", lease_seconds=600
        )
    assert stolen == [job.id]


def _locked_error() -> OperationalError:
    return OperationalError("UPDATE jobs", {}, sqlite3.OperationalError("database is locked"))


@pytest.mark.asyncio
async def test_lock_error_releases_job_without_spending_last_attempt(session_factory):
    runtime = pipeline.PipelineRuntime(session_factory)
    await _enqueue(session_factory)
    async with session_factory() as session:
        await session.execute(
            update(Job).where(Job.task_key == TASK_KEY).values(attempt_count=pipeline.MAX_JOB_ATTEMPTS - 1)
        )
        await session.commit()
        job = await _claim(session, "worker-a")
        assert job.attempt_count == pipeline.MAX_JOB_ATTEMPTS

        await runtime._record_worker_failure(session, [job], _locked_error(), lease_owner="worker-a")

    stored = await _load_job(session_factory)
    assert stored.status == job_service.JOB_STATUS_PENDING
    assert stored.attempt_count == pipeline.MAX_JOB_ATTEMPTS - 1
    assert stored.lease_owner is None


@pytest.mark.asyncio
async def test_retry_failed_jobs_resets_attempts_and_rechecks_files(session_factory):
    async with session_factory() as session:
        lab_file = LabFile(filename="failed.png", filepath="/tmp/failed.png", mime_type="image/png")
        session.add(lab_file)
        await session.flush()
        session.add_all(
            [
                Job(
                    file_id=lab_file.id,
                    task_type=pipeline.TASK_PROCESS_MEASUREMENTS,
                    task_key=f"file:{lab_file.id}:process-measurements",
                    status=job_service.JOB_STATUS_FAILED,
                    attempt_count=pipeline.MAX_JOB_ATTEMPTS,
                    error_text="database is locked",
                ),
                Job(
                    task_type=pipeline.TASK_CANONIZE_MARKER,
                    task_key="marker:ferritin",
                    status=job_service.JOB_STATUS_FAILED,
                    attempt_count=pipeline.MAX_JOB_ATTEMPTS,
                    error_text="database is locked",
                ),
            ]
        )
        await session.commit()

        assert await pipeline.retry_failed_jobs(session) == 2
        await session.commit()
        jobs = {job.task_type: job for job in (await session.execute(select(Job))).scalars()}

    assert jobs[pipeline.TASK_PROCESS_MEASUREMENTS].status == job_service.JOB_STATUS_PENDING
    assert jobs[pipeline.TASK_PROCESS_MEASUREMENTS].attempt_count == 0
    assert jobs[pipeline.TASK_CANONIZE_MARKER].status == job_service.JOB_STATUS_PENDING
    assert jobs[pipeline.TASK_CANONIZE_MARKER].error_text is None
    assert jobs[pipeline.TASK_ENSURE_FILE].status == job_service.JOB_STATUS_PENDING


@pytest.mark.asyncio
async def test_ensure_file_does_not_rerun_finished_process_measurements_job(session_factory):
    async with session_factory() as session:
        lab_file = LabFile(filename="loop.png", filepath="/tmp/loop.png", mime_type="image/png")
        session.add(lab_file)
        await session.flush()
        process_key = f"file:{lab_file.id}:process-measurements"
        session.add_all(
            [
                Measurement(
                    lab_file_id=lab_file.id,
                    raw_marker_name="Ferritin",
                    normalized_marker_key="ferritin",
                    normalization_status=pipeline.MEASUREMENT_STATE_ERROR,
                    normalization_error="Marker canonization failed for Ferritin",
                ),
                Job(
                    file_id=lab_file.id,
                    task_type=pipeline.TASK_PROCESS_MEASUREMENTS,
                    task_key=process_key,
                    status=job_service.JOB_STATUS_RESOLVED,
                    payload_json=job_service.json_dumps({"file_id": lab_file.id}),
                ),
                Job(
                    file_id=lab_file.id,
                    task_type=pipeline.TASK_ENSURE_FILE,
                    task_key=f"file:{lab_file.id}",
                    status=job_service.JOB_STATUS_LEASED,
                    lease_owner="worker-a",
                    lease_until=utc_now(),
                    payload_json=job_service.json_dumps({"file_id": lab_file.id}),
                ),
            ]
        )
        await session.commit()
        ensure_job = (
            await session.execute(select(Job).where(Job.task_type == pipeline.TASK_ENSURE_FILE))
        ).scalar_one()

        await pipeline._ensure_file(session, ensure_job)
        await session.commit()
        process_job = (await session.execute(select(Job).where(Job.task_key == process_key))).scalar_one()

    assert process_job.status == job_service.JOB_STATUS_RESOLVED
