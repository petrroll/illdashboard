"""Measurement, overview, insight, and sparkline endpoints."""

from __future__ import annotations

from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy import func, select
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
from illdashboard.services.rescaling import annotate_missing_rescaling_measurements
from illdashboard.sparkline import generate_sparkline, get_cached_sparkline

router = APIRouter(prefix="")
VISIBLE_MEASUREMENT_STATUS = "resolved"


@router.get("/measurements", response_model=list[MeasurementOut], tags=["measurements"])
async def list_measurements(
    marker_name: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    query = (
        select(Measurement)
        .join(Measurement.measurement_type)
        .join(Measurement.lab_file)
        .options(
            selectinload(Measurement.measurement_type),
            selectinload(Measurement.lab_file).selectinload(LabFile.tags),
        )
        .where(Measurement.normalization_status == VISIBLE_MEASUREMENT_STATUS)
        .order_by(
            func.coalesce(Measurement.measured_at, LabFile.lab_date, LabFile.uploaded_at).asc(),
            Measurement.id.asc(),
        )
    )
    if marker_name:
        query = query.where(MeasurementType.name == marker_name)
    result = await db.execute(query)
    measurements = result.scalars().all()
    await annotate_missing_rescaling_measurements(db, measurements)
    return measurements


@router.get("/measurements/overview", response_model=list[MarkerOverviewGroup], tags=["measurements"])
async def measurement_overview(
    tags: list[str] | None = Query(None, description="Filter markers having ALL of these tags"),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Measurement)
        .join(Measurement.measurement_type)
        .join(Measurement.lab_file)
        .options(
            selectinload(Measurement.measurement_type).selectinload(MeasurementType.aliases),
            selectinload(Measurement.lab_file).selectinload(LabFile.tags),
        )
        .where(Measurement.normalization_status == VISIBLE_MEASUREMENT_STATUS)
        .order_by(
            MeasurementType.name.asc(),
            func.coalesce(Measurement.measured_at, LabFile.lab_date, LabFile.uploaded_at).asc(),
            Measurement.id.asc(),
        )
    )
    measurements = list(result.scalars().all())
    await annotate_missing_rescaling_measurements(db, measurements)
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
            name: entries for name, entries in by_marker.items() if tag_set <= set(marker_search_tag_map.get(name, []))
        }

    grouped_items: dict[str, list[MarkerOverviewItem]] = defaultdict(list)
    for marker_name in sorted(by_marker):
        payload = marker_service.build_marker_payload(by_marker[marker_name])
        payload["tags"] = marker_search_tag_map.get(marker_name, [])
        payload["marker_tags"] = marker_tag_map.get(marker_name, [])
        payload["file_tags"] = marker_file_tag_map.get(marker_name, [])
        grouped_items[payload["group_name"]].append(MarkerOverviewItem(**payload))

    group_order = await marker_service.load_group_order(db)
    group_order_set = set(group_order)
    groups: list[MarkerOverviewGroup] = []
    for group_name in group_order:
        if group_name not in grouped_items:
            continue
        groups.append(
            MarkerOverviewGroup(
                group_name=group_name,
                markers=sorted(grouped_items[group_name], key=lambda item: item.marker_name),
            )
        )
    for group_name in sorted(grouped_items.keys() - group_order_set):
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
        .where(Measurement.normalization_status == VISIBLE_MEASUREMENT_STATUS)
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

    measurements = [
        measurement
        for measurement in await marker_service.load_measurements_for_marker(db, marker_name)
        if measurement.normalization_status == VISIBLE_MEASUREMENT_STATUS
    ]
    if not measurements:
        raise HTTPException(404, "Marker not found")

    await annotate_missing_rescaling_measurements(db, measurements)

    payload = marker_service.build_marker_payload(measurements)
    explanation, explanation_cached = await insight_service.get_cached_insight(
        measurement_type,
        measurements,
        db,
    )

    tag_result = await db.execute(select(MarkerTag.tag).where(MarkerTag.measurement_type_id == measurement_type.id))
    marker_tags = marker_service.combine_marker_tags(
        list(tag_result.scalars().all()),
        measurement_type.group_name,
        measurements,
    )
    file_tags = marker_service.build_marker_file_tag_map({marker_name: measurements}).get(marker_name, [])

    return MarkerDetailResponse(
        **payload,
        measurements=[MeasurementOut.model_validate(measurement) for measurement in measurements],
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

    measurements = [
        measurement
        for measurement in await marker_service.load_measurements_for_marker(db, marker_name)
        if measurement.normalization_status == VISIBLE_MEASUREMENT_STATUS
    ]
    if not measurements:
        raise HTTPException(404, "Marker not found")

    await annotate_missing_rescaling_measurements(db, measurements)

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
    file = await db.get(LabFile, file_id)
    if file is None:
        raise HTTPException(404, "File not found")

    result = await db.execute(
        select(Measurement)
        .join(Measurement.measurement_type)
        .options(
            selectinload(Measurement.measurement_type),
            selectinload(Measurement.lab_file).selectinload(LabFile.tags),
        )
        .where(
            Measurement.lab_file_id == file_id,
            Measurement.normalization_status == VISIBLE_MEASUREMENT_STATUS,
        )
        .order_by(MeasurementType.name.asc(), Measurement.id.asc())
    )
    measurements = result.scalars().all()
    await annotate_missing_rescaling_measurements(db, measurements)
    return measurements


@router.get("/measurements/sparkline", tags=["measurements"])
async def measurement_sparkline(
    marker_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    measurements = [
        measurement
        for measurement in await marker_service.load_measurements_for_marker(db, marker_name)
        if measurement.normalization_status == VISIBLE_MEASUREMENT_STATUS
    ]
    if not measurements:
        raise HTTPException(404, "Marker not found")

    await annotate_missing_rescaling_measurements(db, measurements)

    sparkline_measurements = [
        measurement
        for measurement in measurements
        if measurement.canonical_value is not None and not getattr(measurement, "unit_conversion_missing", False)
    ]
    if not sparkline_measurements:
        sparkline_measurements = [
            measurement for measurement in measurements if measurement.canonical_value is not None
        ]
    if sparkline_measurements:
        signature = insight_service.marker_signature(sparkline_measurements)
        cached = get_cached_sparkline(marker_name, signature)
        if cached:
            return Response(content=cached, media_type="image/png", headers={"Cache-Control": "no-store"})

        ref_low, ref_high = marker_service.latest_reference_range_for_history(sparkline_measurements)
        png_bytes = generate_sparkline(
            values=[
                measurement.canonical_value
                for measurement in sparkline_measurements
                if measurement.canonical_value is not None
            ],
            ref_low=ref_low,
            ref_high=ref_high,
            signature=signature,
            marker_name=marker_name,
        )
        return Response(content=png_bytes, media_type="image/png", headers={"Cache-Control": "no-store"})

    qualitative_sparkline_measurements = [
        measurement for measurement in measurements if measurement.qualitative_bool is not None
    ]
    if len(qualitative_sparkline_measurements) < 2:
        raise HTTPException(404, "Marker not found")

    signature = insight_service.marker_signature(qualitative_sparkline_measurements)
    cached = get_cached_sparkline(marker_name, signature)
    if cached:
        return Response(content=cached, media_type="image/png", headers={"Cache-Control": "no-store"})

    png_bytes = generate_sparkline(
        values=[1.0 if measurement.qualitative_bool else 0.0 for measurement in qualitative_sparkline_measurements],
        ref_low=None,
        ref_high=0.5,
        signature=signature,
        marker_name=marker_name,
        qualitative_mode=True,
    )
    return Response(content=png_bytes, media_type="image/png", headers={"Cache-Control": "no-store"})
