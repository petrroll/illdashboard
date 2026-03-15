"""File and marker tag endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.database import get_db
from illdashboard.models import LabFile, LabFileTag, MarkerTag, Measurement, MeasurementType
from illdashboard.schemas import TagsUpdate
from illdashboard.services import markers as marker_service


router = APIRouter(prefix="")


@router.get("/tags/files", response_model=list[str], tags=["tags"])
async def list_file_tags(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(LabFileTag.tag).distinct().order_by(LabFileTag.tag))
    return result.scalars().all()


@router.get("/tags/markers", response_model=list[str], tags=["tags"])
async def list_marker_tags(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Measurement)
        .join(Measurement.measurement_type)
        .options(selectinload(Measurement.measurement_type))
        .order_by(MeasurementType.name.asc(), Measurement.measured_at.asc(), Measurement.id.asc())
    )
    measurements = result.scalars().all()
    by_marker = marker_service.build_marker_histories(measurements)
    stored_marker_tags = await marker_service.load_stored_marker_tags(db)

    all_tags: set[str] = set()
    for marker_tags in marker_service.build_marker_tag_map(by_marker, stored_marker_tags).values():
        all_tags.update(marker_tags)

    return sorted(all_tags)


@router.put("/files/{file_id}/tags", response_model=list[str], tags=["tags"])
async def set_file_tags(file_id: int, body: TagsUpdate, db: AsyncSession = Depends(get_db)):
    lab = await db.get(LabFile, file_id)
    if not lab:
        raise HTTPException(404, "File not found")

    existing = await db.execute(select(LabFileTag).where(LabFileTag.lab_file_id == file_id))
    for tag in existing.scalars().all():
        await db.delete(tag)
    await db.flush()

    unique_tags = marker_service.normalize_unique_tags(body.tags)
    for tag in unique_tags:
        db.add(LabFileTag(lab_file_id=file_id, tag=tag))
    await db.commit()
    return unique_tags


@router.put("/markers/{marker_name:path}/tags", response_model=list[str], tags=["tags"])
async def set_marker_tags(marker_name: str, body: TagsUpdate, db: AsyncSession = Depends(get_db)):
    measurement_type = await marker_service.get_measurement_type_by_name(db, marker_name)
    if measurement_type is None:
        raise HTTPException(404, "Marker not found")

    existing = await db.execute(select(MarkerTag).where(MarkerTag.measurement_type_id == measurement_type.id))
    for tag in existing.scalars().all():
        await db.delete(tag)
    await db.flush()

    unique_tags = [
        tag
        for tag in marker_service.normalize_unique_tags(body.tags)
        if tag not in marker_service.all_reserved_marker_tags(measurement_type.group_name)
    ]
    for tag in unique_tags:
        db.add(MarkerTag(measurement_type_id=measurement_type.id, tag=tag))
    await db.commit()

    measurements = await marker_service.load_measurements_for_marker(db, marker_name)
    return marker_service.combine_marker_tags(unique_tags, measurement_type.group_name, len(measurements))
