from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

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
            notes=occurrence.notes,
        )
        for index, occurrence in enumerate(payload.occurrences)
    )


@router.get("/events", response_model=list[TimelineEventOut])
async def list_events(db: AsyncSession = Depends(get_medications_db)):
    result = await db.execute(
        select(TimelineEvent)
        .options(selectinload(TimelineEvent.occurrences))
        .order_by(TimelineEvent.name.asc(), TimelineEvent.id.asc())
    )
    return list(result.scalars().unique().all())


@router.get("/events/{event_id}", response_model=TimelineEventOut)
async def get_event(event_id: int, db: AsyncSession = Depends(get_medications_db)):
    return await get_event_or_404(event_id, db)


@router.post("/events", response_model=TimelineEventOut, status_code=status.HTTP_201_CREATED)
async def create_event(payload: TimelineEventWrite, db: AsyncSession = Depends(get_medications_db)):
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
):
    event = await get_event_or_404(event_id, db)
    apply_event_payload(event, payload)
    await db.commit()
    return await get_event_or_404(event_id, db)


@router.delete("/events/{event_id}")
async def delete_event(event_id: int, db: AsyncSession = Depends(get_medications_db)):
    event = await get_event_or_404(event_id, db)
    await db.delete(event)
    await db.commit()
    return {"ok": True}
