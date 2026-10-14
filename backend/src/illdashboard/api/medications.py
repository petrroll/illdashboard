from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.api.preconditions import claim_timeline_write
from illdashboard.medications_database import get_medications_db
from illdashboard.medications_models import Medication, MedicationEpisode
from illdashboard.schemas import MedicationOut, MedicationWrite

router = APIRouter(prefix="", tags=["medications"])


async def get_medication_or_404(medication_id: int, db: AsyncSession) -> Medication:
    result = await db.execute(
        select(Medication)
        .options(selectinload(Medication.episodes))
        .where(Medication.id == medication_id)
    )
    medication = result.scalar_one_or_none()
    if medication is None:
        raise HTTPException(status_code=404, detail="Medication not found")
    return medication


def apply_medication_payload(medication: Medication, payload: MedicationWrite) -> None:
    medication.name = payload.name
    medication.episodes.clear()
    medication.episodes.extend(
        MedicationEpisode(
            position=index,
            start_on=episode.start_on,
            end_on=episode.end_on,
            still_taking=episode.still_taking,
            dose=episode.dose,
            frequency=episode.frequency,
            notes=episode.notes,
        )
        for index, episode in enumerate(payload.episodes)
    )


@router.get("/medications", response_model=list[MedicationOut])
async def list_medications(db: AsyncSession = Depends(get_medications_db)):
    """List all medications with complete episode histories, ordered by name and ID."""
    result = await db.execute(
        select(Medication)
        .options(selectinload(Medication.episodes))
        .order_by(Medication.name.asc(), Medication.id.asc())
    )
    return list(result.scalars().unique().all())


@router.get("/medications/{medication_id}", response_model=MedicationOut)
async def get_medication(medication_id: int, db: AsyncSession = Depends(get_medications_db)):
    """Read a medication and its complete episode history."""
    return await get_medication_or_404(medication_id, db)


@router.post("/medications", response_model=MedicationOut, status_code=status.HTTP_201_CREATED)
async def create_medication(payload: MedicationWrite, db: AsyncSession = Depends(get_medications_db)):
    """Create a medication with at least one episode. Repeated calls create separate records.

    Dates accept YYYY-MM or YYYY-MM-DD. Each episode requires an end date or
    still_taking=true. Frequency defaults to daily. No AI is involved.
    """
    medication = Medication(name=payload.name)
    apply_medication_payload(medication, payload)
    db.add(medication)
    await db.commit()
    return await get_medication_or_404(medication.id, db)


@router.put("/medications/{medication_id}", response_model=MedicationOut)
async def update_medication(
    medication_id: int,
    payload: MedicationWrite,
    db: AsyncSession = Depends(get_medications_db),
    expected_updated_at: datetime | None = Query(
        None, description="The updated_at value from your last read; stale writes return 409 without changing data."
    ),
):
    """Replace name and the entire episode history, deleting omitted episodes.

    Fetch the current record first and include every episode to retain. Episode
    IDs are regenerated and must not be sent in the write payload.
    Supply expected_updated_at from that read to reject concurrent changes with
    409. Without it, replacement is unconditional. Refetch and merge after a conflict.
    """
    medication = await get_medication_or_404(medication_id, db)
    await claim_timeline_write(db, medication, expected_updated_at)
    apply_medication_payload(medication, payload)
    await db.commit()
    return await get_medication_or_404(medication_id, db)


@router.delete("/medications/{medication_id}")
async def delete_medication(
    medication_id: int,
    db: AsyncSession = Depends(get_medications_db),
    expected_updated_at: datetime | None = Query(
        None, description="Reject deletion if updated_at changed since this read."
    ),
):
    """Permanently delete a medication and all its episodes; repeating returns 404.

    Supply expected_updated_at from a prior read to reject deletion of a newer
    version with 409. Without it, deletion is unconditional.
    """
    medication = await get_medication_or_404(medication_id, db)
    await claim_timeline_write(db, medication, expected_updated_at)
    await db.delete(medication)
    await db.commit()
    return {"ok": True}
