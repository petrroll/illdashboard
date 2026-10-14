"""Optional atomic write preconditions for timeline records."""

from datetime import UTC, datetime, timedelta

from fastapi import HTTPException
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from illdashboard.medications_models import Medication, TimelineEvent
from illdashboard.models import utc_now


async def claim_timeline_write(
    db: AsyncSession,
    record: Medication | TimelineEvent,
    expected_updated_at: datetime | None,
) -> None:
    model = type(record)
    statement = update(model).where(model.id == record.id).execution_options(synchronize_session="fetch")
    if expected_updated_at is not None:
        expected = expected_updated_at
        if expected.tzinfo is None:
            expected = expected.replace(tzinfo=UTC)
        statement = statement.where(model.updated_at == expected.astimezone(UTC))
    previous = record.updated_at.replace(tzinfo=UTC)
    next_updated_at = max(utc_now(), previous + timedelta(microseconds=1))
    # The conditional UPDATE claims the row in the same transaction as the history
    # replacement. Touch the parent even when only children change, so its token advances.
    claimed_id = await db.scalar(statement.values(updated_at=next_updated_at).returning(model.id))
    if claimed_id is None:
        if await db.scalar(select(model.id).where(model.id == record.id)) is None:
            raise HTTPException(404, "Record not found")
        raise HTTPException(409, "Record changed since it was read. Fetch it again before replacing or deleting it.")
    # An unconditional replacement must also see children committed before it acquired the write lock.
    history = "episodes" if isinstance(record, Medication) else "occurrences"
    await db.refresh(record, attribute_names=[history])
    # Keep the claimed revision explicit when a name change would otherwise invoke column onupdate again.
    flag_modified(record, "updated_at")