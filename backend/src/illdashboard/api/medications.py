from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

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
    result = await db.execute(
        select(Medication)
        .options(selectinload(Medication.episodes))
        .order_by(Medication.name.asc(), Medication.id.asc())
    )
    return list(result.scalars().unique().all())


@router.get("/medications/{medication_id}", response_model=MedicationOut)
async def get_medication(medication_id: int, db: AsyncSession = Depends(get_medications_db)):
    return await get_medication_or_404(medication_id, db)


@router.post("/medications", response_model=MedicationOut, status_code=status.HTTP_201_CREATED)
async def create_medication(payload: MedicationWrite, db: AsyncSession = Depends(get_medications_db)):
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
):
    medication = await get_medication_or_404(medication_id, db)
    apply_medication_payload(medication, payload)
    await db.commit()
    return await get_medication_or_404(medication_id, db)


@router.delete("/medications/{medication_id}")
async def delete_medication(medication_id: int, db: AsyncSession = Depends(get_medications_db)):
    medication = await get_medication_or_404(medication_id, db)
    await db.delete(medication)
    await db.commit()
    return {"ok": True}
