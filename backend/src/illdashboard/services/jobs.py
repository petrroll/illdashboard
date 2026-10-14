from __future__ import annotations

import json
from datetime import timedelta

from sqlalchemy import and_, case, delete, null, or_, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from illdashboard.models import Job, utc_now

JOB_STATUS_PENDING = "pending"
JOB_STATUS_LEASED = "leased"
JOB_STATUS_RESOLVED = "resolved"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_CANCELLED = "cancelled"

TERMINAL_JOB_STATUSES = {JOB_STATUS_RESOLVED, JOB_STATUS_FAILED, JOB_STATUS_CANCELLED}

# Session.info key collecting task types enqueued in the current transaction, so
# idle workers can be woken once the rows are actually committed and claimable.
ENQUEUED_TASK_TYPES_INFO_KEY = "illdashboard.enqueued_task_types"


class JobLeaseLostError(RuntimeError):
    """Raised when a worker tries to finish a job whose lease it no longer holds."""


def is_database_locked_error(exc: BaseException) -> bool:
    return isinstance(exc, OperationalError) and "database is locked" in str(exc).lower()


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
    session.info.setdefault(ENQUEUED_TASK_TYPES_INFO_KEY, set()).add(task_type)


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
    claimable = and_(
        Job.task_type.in_(task_types),
        Job.available_at <= now,
        or_(
            Job.status == JOB_STATUS_PENDING,
            and_(Job.status == JOB_STATUS_LEASED, Job.lease_until.is_not(None), Job.lease_until < now),
        ),
    )
    # Probe with a plain SELECT first: an UPDATE takes the SQLite write lock even
    # when it matches nothing, and idle workers probe constantly.
    candidate_result = await session.execute(
        select(Job.id)
        .where(claimable)
        .order_by(Job.priority.asc(), Job.created_at.asc(), Job.id.asc())
        .limit(limit)
    )
    candidate_ids = list(candidate_result.scalars().all())
    if not candidate_ids:
        await session.rollback()
        return []

    # Re-check claimability so a concurrent worker that won the race is not overwritten.
    result = await session.execute(
        update(Job)
        .where(Job.id.in_(candidate_ids), claimable)
        .values(
            status=JOB_STATUS_LEASED,
            lease_owner=lease_owner,
            lease_until=lease_until,
            attempt_count=Job.attempt_count + 1,
            updated_at=now,
        )
        .returning(Job.id)
        .execution_options(synchronize_session=False)
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


async def _finish_leased_job(session: AsyncSession, job: Job, values: dict) -> None:
    # Transitions are conditional SQL on the claimed lease so a worker whose
    # lease expired or was reassigned cannot overwrite the new owner's state,
    # and so rerun_requested is read from the row instead of a stale ORM copy
    # (enqueue_job sets it from other sessions while the job is leased).
    result = await session.execute(
        update(Job)
        .where(Job.id == job.id, Job.status == JOB_STATUS_LEASED, Job.lease_owner == job.lease_owner)
        .values(**values, lease_owner=None, lease_until=None, updated_at=utc_now())
        .execution_options(synchronize_session=False)
    )
    if result.rowcount != 1:
        # Raising makes the worker roll back every output written for this job.
        raise JobLeaseLostError(f"Lease lost for job id={job.id} owner={job.lease_owner}")
    await session.refresh(job)


async def mark_job_resolved(session: AsyncSession, job: Job, payload: dict | None = None) -> None:
    rerun = Job.rerun_requested.is_(True)
    await _finish_leased_job(
        session,
        job,
        {
            "status": case((rerun, JOB_STATUS_PENDING), else_=JOB_STATUS_RESOLVED),
            "resolved_json": case((rerun, null()), else_=json_dumps(payload)),
            "available_at": case((rerun, utc_now()), else_=Job.available_at),
            "rerun_requested": False,
            "error_text": None,
        },
    )


async def release_job(
    session: AsyncSession,
    job: Job,
    *,
    delay_seconds: float,
    error_text: str | None = None,
    refund_attempt: bool = False,
) -> None:
    values = {
        "status": JOB_STATUS_PENDING,
        "error_text": error_text,
        "rerun_requested": False,
        "available_at": utc_now() + timedelta(seconds=delay_seconds),
    }
    if refund_attempt:
        # claim_jobs counts the attempt up front; undo it when the failure was not the job's fault.
        values["attempt_count"] = case((Job.attempt_count > 0, Job.attempt_count - 1), else_=0)
    await _finish_leased_job(session, job, values)


async def mark_job_failed(session: AsyncSession, job: Job, *, error_text: str) -> None:
    await _finish_leased_job(
        session,
        job,
        {"status": JOB_STATUS_FAILED, "error_text": error_text, "rerun_requested": False},
    )


async def renew_job_leases(
    session: AsyncSession,
    *,
    job_ids: list[int],
    lease_owner: str,
    lease_seconds: int,
) -> list[int]:
    """Extend leases still held by lease_owner and return ids now leased by someone else."""
    if not job_ids:
        return []
    now = utc_now()
    await session.execute(
        update(Job)
        .where(Job.id.in_(job_ids), Job.status == JOB_STATUS_LEASED, Job.lease_owner == lease_owner)
        .values(lease_until=now + timedelta(seconds=lease_seconds), updated_at=now)
        .execution_options(synchronize_session=False)
    )
    # Jobs this worker already resolved or released are no longer its concern;
    # only a lease reassigned to another owner means the work must be abandoned.
    stolen = await session.execute(
        select(Job.id).where(Job.id.in_(job_ids), Job.status == JOB_STATUS_LEASED, Job.lease_owner != lease_owner)
    )
    stolen_ids = list(stolen.scalars().all())
    await session.commit()
    return stolen_ids


async def delete_job(session: AsyncSession, job: Job) -> None:
    await session.delete(job)
    await session.flush()


async def delete_jobs_for_file(session: AsyncSession, file_id: int) -> None:
    await session.execute(delete(Job).where(Job.file_id == file_id))


async def delete_all_jobs(session: AsyncSession) -> None:
    await session.execute(delete(Job))


async def retry_failed_jobs(session: AsyncSession) -> int:
    now = utc_now()
    result = await session.execute(
        update(Job)
        .where(Job.status == JOB_STATUS_FAILED)
        .values(
            status=JOB_STATUS_PENDING,
            attempt_count=0,
            error_text=None,
            rerun_requested=False,
            available_at=now,
            lease_owner=None,
            lease_until=None,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    return result.rowcount


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
    # Reset attempt_count so that server restarts (especially via
    # --reload) do not permanently burn through the retry budget for
    # long-running jobs like Copilot normalization calls.
    await session.execute(
        update(Job)
        .where(Job.status == JOB_STATUS_LEASED)
        .values(
            status=JOB_STATUS_PENDING,
            lease_owner=None,
            lease_until=None,
            attempt_count=0,
            error_text=None,
            updated_at=now,
        )
    )
    await session.commit()
