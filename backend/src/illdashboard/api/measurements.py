"""Measurement, overview, insight, and sparkline endpoints."""

from __future__ import annotations

from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.database import get_db
from illdashboard.models import LabFile, MarkerTag, Measurement, MeasurementType
from illdashboard.schemas import (
    MarkerDetailResponse,
    MarkerInsightResponse,
    MarkerOverviewGroup,
    MarkerOverviewItem,
    MeasurementOut,
)
from illdashboard.services import insights as insight_service
from illdashboard.services import markers as marker_service
from illdashboard.sparkline import generate_sparkline, get_cached_sparkline


router = APIRouter(prefix="")


@router.get("/measurements", response_model=list[MeasurementOut], tags=["measurements"])
async def list_measurements(
    marker_name: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    query = (
        select(Measurement)
        .join(Measurement.measurement_type)
        .options(selectinload(Measurement.measurement_type))
        .order_by(Measurement.measured_at.asc(), Measurement.id.asc())
    )
    if marker_name:
        query = query.where(MeasurementType.name == marker_name)
    result = await db.execute(query)
    return result.scalars().all()


@router.get("/measurements/overview", response_model=list[MarkerOverviewGroup], tags=["measurements"])
async def measurement_overview(
    tags: list[str] | None = Query(None, description="Filter markers having ALL of these tags"),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Measurement)
        .join(Measurement.measurement_type)
        .where(Measurement.qualitative_value.is_(None))
        .options(
            selectinload(Measurement.measurement_type),
            selectinload(Measurement.lab_file).selectinload(LabFile.tags),
        )
        .order_by(MeasurementType.name.asc(), Measurement.measured_at.asc(), Measurement.id.asc())
    )
    measurements = result.scalars().all()
    by_marker = marker_service.build_marker_histories(measurements)

    stored_marker_tags = await marker_service.load_stored_marker_tags(db)
    marker_tag_map = marker_service.build_marker_tag_map(by_marker, stored_marker_tags)
    marker_file_tag_map = marker_service.build_marker_file_tag_map(by_marker)
    marker_search_tag_map = {
        marker_name: marker_service.combine_search_tags(
            marker_tag_map.get(marker_name, []),
            marker_file_tag_map.get(marker_name, []),
        )
        for marker_name in by_marker
    }

    if tags:
        tag_set = set(tags)
        by_marker = {
            name: entries
            for name, entries in by_marker.items()
            if tag_set <= set(marker_search_tag_map.get(name, []))
        }

    grouped_items: dict[str, list[MarkerOverviewItem]] = defaultdict(list)
    for marker_name in sorted(by_marker):
        payload = marker_service.build_marker_payload(by_marker[marker_name])
        payload["tags"] = marker_search_tag_map.get(marker_name, [])
        payload["marker_tags"] = marker_tag_map.get(marker_name, [])
        payload["file_tags"] = marker_file_tag_map.get(marker_name, [])
        grouped_items[payload["group_name"]].append(MarkerOverviewItem(**payload))

    groups: list[MarkerOverviewGroup] = []
    for group_name in marker_service.GROUP_ORDER:
        if group_name not in grouped_items:
            continue
        groups.append(
            MarkerOverviewGroup(
                group_name=group_name,
                markers=sorted(grouped_items[group_name], key=lambda item: item.marker_name),
            )
        )
    return groups


@router.get("/measurements/markers", response_model=list[str], tags=["measurements"])
async def list_marker_names(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(MeasurementType.name)
        .join(MeasurementType.measurements)
        .where(Measurement.qualitative_value.is_(None))
        .distinct()
        .order_by(MeasurementType.name)
    )
    return result.scalars().all()


@router.get("/measurements/detail", response_model=MarkerDetailResponse, tags=["measurements"])
async def measurement_detail(
    marker_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    measurement_type = await marker_service.get_measurement_type_by_name(db, marker_name)
    if measurement_type is None:
        raise HTTPException(404, "Marker not found")

    measurements = await marker_service.load_measurements_for_marker(db, marker_name)
    measurements = [measurement for measurement in measurements if measurement.qualitative_value is None]
    if not measurements:
        raise HTTPException(404, "Marker not found")

    payload = marker_service.build_marker_payload(measurements)
    explanation, explanation_cached = await insight_service.get_cached_insight(
        measurement_type,
        measurements,
        db,
    )

    tag_result = await db.execute(select(MarkerTag.tag).where(MarkerTag.measurement_type_id == measurement_type.id))
    marker_tags = marker_service.combine_marker_tags(
        tag_result.scalars().all(),
        measurement_type.group_name,
        len(measurements),
    )
    file_tags = marker_service.build_marker_file_tag_map({marker_name: measurements}).get(marker_name, [])

    return MarkerDetailResponse(
        **payload,
        measurements=measurements,
        explanation=explanation,
        explanation_cached=explanation_cached,
        tags=marker_service.combine_search_tags(marker_tags, file_tags),
        marker_tags=marker_tags,
        file_tags=file_tags,
    )


@router.get("/measurements/insight", response_model=MarkerInsightResponse, tags=["measurements"])
async def measurement_insight(
    marker_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    measurement_type = await marker_service.get_measurement_type_by_name(db, marker_name)
    if measurement_type is None:
        raise HTTPException(404, "Marker not found")

    measurements = await marker_service.load_measurements_for_marker(db, marker_name)
    measurements = [measurement for measurement in measurements if measurement.qualitative_value is None]
    if not measurements:
        raise HTTPException(404, "Marker not found")

    explanation, explanation_cached = await insight_service.get_cached_or_generated_insight(
        measurement_type,
        measurements,
        db,
    )
    return MarkerInsightResponse(
        marker_name=marker_name,
        explanation=explanation,
        explanation_cached=explanation_cached,
    )


@router.get("/files/{file_id}/measurements", response_model=list[MeasurementOut], tags=["measurements"])
async def file_measurements(file_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Measurement)
        .join(Measurement.measurement_type)
        .options(selectinload(Measurement.measurement_type))
        .where(Measurement.lab_file_id == file_id)
        .order_by(MeasurementType.name.asc(), Measurement.id.asc())
    )
    return result.scalars().all()


@router.get("/measurements/sparkline", tags=["measurements"])
async def measurement_sparkline(
    marker_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    measurements = await marker_service.load_measurements_for_marker(db, marker_name)
    measurements = [measurement for measurement in measurements if measurement.qualitative_value is None]
    if not measurements:
        raise HTTPException(404, "Marker not found")

    signature = insight_service.marker_signature(measurements)
    cached = get_cached_sparkline(marker_name, signature)
    if cached:
        return Response(content=cached, media_type="image/png", headers={"Cache-Control": "no-store"})

    png_bytes = generate_sparkline(
        values=[measurement.canonical_value for measurement in measurements],
        ref_low=measurements[-1].canonical_reference_low,
        ref_high=measurements[-1].canonical_reference_high,
        signature=signature,
        marker_name=marker_name,
    )
    return Response(content=png_bytes, media_type="image/png", headers={"Cache-Control": "no-store"})
