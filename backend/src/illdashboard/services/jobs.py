from __future__ import annotations

import json
from datetime import timedelta

from sqlalchemy import and_, case, delete, or_, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from illdashboard.models import Job, utc_now

JOB_STATUS_PENDING = "pending"
JOB_STATUS_LEASED = "leased"
JOB_STATUS_RESOLVED = "resolved"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_CANCELLED = "cancelled"

TERMINAL_JOB_STATUSES = {JOB_STATUS_RESOLVED, JOB_STATUS_FAILED, JOB_STATUS_CANCELLED}


def json_dumps(payload: dict | list | None) -> str:
    return json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)


def json_loads(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


async def enqueue_job(
    session: AsyncSession,
    *,
    task_type: str,
    task_key: str,
    payload: dict | None = None,
    file_id: int | None = None,
    priority: int = 100,
) -> None:
    now = utc_now()
    payload_blob = json_dumps(payload)
    stmt = sqlite_insert(Job).values(
        file_id=file_id,
        task_type=task_type,
        task_key=task_key,
        status=JOB_STATUS_PENDING,
        priority=priority,
        payload_json=payload_blob,
        resolved_json=None,
        error_text=None,
        rerun_requested=False,
        attempt_count=0,
        available_at=now,
        lease_owner=None,
        lease_until=None,
        created_at=now,
        updated_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[Job.task_type, Job.task_key],
        set_={
            "file_id": file_id,
            "priority": priority,
            "payload_json": payload_blob,
            "resolved_json": case((Job.status == JOB_STATUS_LEASED, Job.resolved_json), else_=None),
            "error_text": case((Job.status == JOB_STATUS_LEASED, Job.error_text), else_=None),
            "rerun_requested": case((Job.status == JOB_STATUS_LEASED, True), else_=False),
            "attempt_count": case((Job.status == JOB_STATUS_LEASED, Job.attempt_count), else_=0),
            "available_at": case((Job.status == JOB_STATUS_LEASED, Job.available_at), else_=now),
            "lease_owner": case((Job.status == JOB_STATUS_LEASED, Job.lease_owner), else_=None),
            "lease_until": case((Job.status == JOB_STATUS_LEASED, Job.lease_until), else_=None),
            "status": case((Job.status == JOB_STATUS_LEASED, Job.status), else_=JOB_STATUS_PENDING),
            "updated_at": now,
        },
    )
    await session.execute(stmt)


async def claim_jobs(
    session: AsyncSession,
    *,
    task_types: list[str],
    lease_owner: str,
    limit: int,
    lease_seconds: int,
) -> list[Job]:
    if not task_types or limit <= 0:
        return []

    now = utc_now()
    lease_until = now + timedelta(seconds=lease_seconds)
    candidate_ids = (
        select(Job.id)
        .where(
            Job.task_type.in_(task_types),
            Job.available_at <= now,
            or_(
                Job.status == JOB_STATUS_PENDING,
                and_(Job.status == JOB_STATUS_LEASED, Job.lease_until.is_not(None), Job.lease_until < now),
            ),
        )
        .order_by(Job.priority.asc(), Job.created_at.asc(), Job.id.asc())
        .limit(limit)
    )
    result = await session.execute(
        update(Job)
        .where(Job.id.in_(candidate_ids))
        .values(
            status=JOB_STATUS_LEASED,
            lease_owner=lease_owner,
            lease_until=lease_until,
            attempt_count=Job.attempt_count + 1,
            updated_at=now,
        )
        .returning(Job.id)
    )
    claimed_ids = [row[0] for row in result.all()]
    if not claimed_ids:
        await session.rollback()
        return []

    await session.commit()
    claimed = await session.execute(
        select(Job).where(Job.id.in_(claimed_ids)).order_by(Job.priority.asc(), Job.id.asc())
    )
    return list(claimed.scalars().all())


async def mark_job_resolved(session: AsyncSession, job: Job, payload: dict | None = None) -> None:
    now = utc_now()
    if job.rerun_requested:
        job.status = JOB_STATUS_PENDING
        job.resolved_json = None
        job.available_at = now
        job.rerun_requested = False
    else:
        job.status = JOB_STATUS_RESOLVED
        job.resolved_json = json_dumps(payload)
    job.error_text = None
    job.lease_owner = None
    job.lease_until = None
    job.updated_at = now
    await session.flush()


async def release_job(session: AsyncSession, job: Job, *, delay_seconds: int, error_text: str | None = None) -> None:
    job.status = JOB_STATUS_PENDING
    job.error_text = error_text
    job.rerun_requested = False
    job.lease_owner = None
    job.lease_until = None
    job.available_at = utc_now() + timedelta(seconds=delay_seconds)
    job.updated_at = utc_now()
    await session.flush()


async def mark_job_failed(session: AsyncSession, job: Job, *, error_text: str) -> None:
    job.status = JOB_STATUS_FAILED
    job.error_text = error_text
    job.rerun_requested = False
    job.lease_owner = None
    job.lease_until = None
    job.updated_at = utc_now()
    await session.flush()


async def delete_job(session: AsyncSession, job: Job) -> None:
    await session.delete(job)
    await session.flush()


async def delete_jobs_for_file(session: AsyncSession, file_id: int) -> None:
    await session.execute(delete(Job).where(Job.file_id == file_id))


async def delete_all_jobs(session: AsyncSession) -> None:
    await session.execute(delete(Job))


async def prune_jobs(session: AsyncSession) -> None:
    now = utc_now()
    stale_resolved_before = now - timedelta(hours=6)
    stale_failed_before = now - timedelta(days=2)

    await session.execute(delete(Job).where(Job.status == JOB_STATUS_RESOLVED, Job.updated_at < stale_resolved_before))
    await session.execute(
        delete(Job).where(
            Job.status.in_([JOB_STATUS_FAILED, JOB_STATUS_CANCELLED]), Job.updated_at < stale_failed_before
        )
    )
    await session.execute(
        update(Job)
        .where(Job.status == JOB_STATUS_LEASED)
        .values(status=JOB_STATUS_PENDING, lease_owner=None, lease_until=None, updated_at=now)
    )
    await session.commit()
