from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.api.preconditions import claim_timeline_write
from illdashboard.medications_database import get_medications_db
from illdashboard.medications_models import TimelineEvent, TimelineEventOccurrence
from illdashboard.schemas import TimelineEventOut, TimelineEventWrite

router = APIRouter(prefix="", tags=["events"])


async def get_event_or_404(event_id: int, db: AsyncSession) -> TimelineEvent:
    result = await db.execute(
        select(TimelineEvent)
        .options(selectinload(TimelineEvent.occurrences))
        .where(TimelineEvent.id == event_id)
    )
    event = result.scalar_one_or_none()
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


def apply_event_payload(event: TimelineEvent, payload: TimelineEventWrite) -> None:
    event.name = payload.name
    event.occurrences.clear()
    event.occurrences.extend(
        TimelineEventOccurrence(
            position=index,
            start_on=occurrence.start_on,
            end_on=occurrence.end_on,
            is_ongoing=occurrence.is_ongoing,
            notes=occurrence.notes,
        )
        for index, occurrence in enumerate(payload.occurrences)
    )


@router.get("/events", response_model=list[TimelineEventOut])
async def list_events(db: AsyncSession = Depends(get_medications_db)):
    """List all timeline events and occurrences, ordered by name and ID."""
    result = await db.execute(
        select(TimelineEvent)
        .options(selectinload(TimelineEvent.occurrences))
        .order_by(TimelineEvent.name.asc(), TimelineEvent.id.asc())
    )
    return list(result.scalars().unique().all())


@router.get("/events/{event_id}", response_model=TimelineEventOut)
async def get_event(event_id: int, db: AsyncSession = Depends(get_medications_db)):
    """Read a timeline event and its complete occurrence history."""
    return await get_event_or_404(event_id, db)


@router.post("/events", response_model=TimelineEventOut, status_code=status.HTTP_201_CREATED)
async def create_event(payload: TimelineEventWrite, db: AsyncSession = Depends(get_medications_db)):
    """Create an event with at least one occurrence; repeated calls create separate records.

    Dates accept YYYY-MM or YYYY-MM-DD. An absent end date with is_ongoing=false
    describes a point event. An ongoing occurrence cannot have an end date.
    """
    event = TimelineEvent(name=payload.name)
    apply_event_payload(event, payload)
    db.add(event)
    await db.commit()
    return await get_event_or_404(event.id, db)


@router.put("/events/{event_id}", response_model=TimelineEventOut)
async def update_event(
    event_id: int,
    payload: TimelineEventWrite,
    db: AsyncSession = Depends(get_medications_db),
    expected_updated_at: datetime | None = Query(
        None, description="The updated_at value from your last read; stale writes return 409 without changing data."
    ),
):
    """Replace name and all occurrences, deleting any omitted occurrences.

    Fetch first and include everything to retain. Occurrence IDs are regenerated
    and must not be included in the write payload.
    Supply expected_updated_at from that read to reject concurrent changes with
    409. Without it, replacement is unconditional. Refetch and merge after a conflict.
    """
    event = await get_event_or_404(event_id, db)
    await claim_timeline_write(db, event, expected_updated_at)
    apply_event_payload(event, payload)
    await db.commit()
    return await get_event_or_404(event_id, db)


@router.delete("/events/{event_id}")
async def delete_event(
    event_id: int,
    db: AsyncSession = Depends(get_medications_db),
    expected_updated_at: datetime | None = Query(
        None, description="Reject deletion if updated_at changed since this read."
    ),
):
    """Permanently delete an event and all its occurrences; repeating returns 404.

    Supply expected_updated_at from a prior read to reject deletion of a newer
    version with 409. Without it, deletion is unconditional.
    """
    event = await get_event_or_404(event_id, db)
    await claim_timeline_write(db, event, expected_updated_at)
    await db.delete(event)
    await db.commit()
    return {"ok": True}
