"""File and marker tag endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.database import get_db
from illdashboard.models import LabFile, LabFileTag, MarkerTag, Measurement, MeasurementType, utc_now
from illdashboard.schemas import TagsUpdate
from illdashboard.services import markers as marker_service
from illdashboard.services import pipeline
from illdashboard.services import search as search_service

router = APIRouter(prefix="")
VISIBLE_MEASUREMENT_STATUS = "resolved"


@router.get("/tags/files", response_model=list[str], tags=["tags"])
async def list_file_tags(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(LabFileTag.tag).distinct().order_by(LabFileTag.tag))
    return result.scalars().all()


@router.get("/tags/markers", response_model=list[str], tags=["tags"])
async def list_marker_tags(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Measurement)
        .join(Measurement.measurement_type)
        .join(Measurement.lab_file)
        .options(
            selectinload(Measurement.measurement_type),
            selectinload(Measurement.lab_file).selectinload(LabFile.tags),
        )
        .where(Measurement.normalization_status == VISIBLE_MEASUREMENT_STATUS)
        .order_by(MeasurementType.name.asc(), Measurement.measured_at.asc(), Measurement.id.asc())
    )
    measurements = result.scalars().all()
    by_marker = marker_service.build_marker_histories(measurements)
    stored_marker_tags = await marker_service.load_stored_marker_tags(db)
    marker_tag_map = marker_service.build_marker_tag_map(by_marker, stored_marker_tags)
    marker_file_tag_map = marker_service.build_marker_file_tag_map(by_marker)

    all_tags: set[str] = set()
    for marker_name in by_marker:
        all_tags.update(
            marker_service.combine_search_tags(
                marker_tag_map.get(marker_name, []),
                marker_file_tag_map.get(marker_name, []),
            )
        )

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
    await db.flush()
    progress = await pipeline.get_file_progress(db, lab)
    if progress.is_complete:
        await search_service.refresh_lab_search_document(file_id, db)
        lab.search_indexed_at = utc_now()
    else:
        await search_service.remove_lab_search_document(file_id, db)
        lab.search_indexed_at = None
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

    reserved_tags = await marker_service.all_reserved_marker_tags(db, measurement_type.group_name)
    unique_tags = [tag for tag in marker_service.normalize_unique_tags(body.tags) if tag not in reserved_tags]
    for tag in unique_tags:
        db.add(MarkerTag(measurement_type_id=measurement_type.id, tag=tag))
    await db.commit()

    measurements = [
        measurement
        for measurement in await marker_service.load_measurements_for_marker(db, marker_name)
        if measurement.normalization_status == VISIBLE_MEASUREMENT_STATUS
    ]
    return marker_service.combine_marker_tags(unique_tags, measurement_type.group_name, measurements)
