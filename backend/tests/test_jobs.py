from __future__ import annotations

import pytest
from sqlalchemy import select

from illdashboard.models import Job
from illdashboard.services import jobs as job_service

TASK_TYPE = "test.task"
TASK_KEY = "key-1"


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
