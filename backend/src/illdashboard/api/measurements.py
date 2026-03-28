"""Measurement, overview, insight, and sparkline endpoints."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.api.files import refresh_search_projection
from illdashboard.database import get_db
from illdashboard.models import LabFile, MarkerTag, Measurement, MeasurementType
from illdashboard.schemas import (
    MarkerDetailResponse,
    MarkerInsightResponse,
    MarkerOverviewGroup,
    MarkerOverviewItem,
    MarkerPatchRequest,
    MeasurementOut,
    MeasurementPatchRequest,
    parse_qualitative_expression,
)
from illdashboard.services import insights as insight_service
from illdashboard.services import markers as marker_service
from illdashboard.services import rescaling
from illdashboard.services.rescaling import annotate_missing_rescaling_measurements
from illdashboard.sparkline import generate_sparkline, get_cached_sparkline

router = APIRouter(prefix="")
VISIBLE_MEASUREMENT_STATUS = "resolved"


async def _recalculate_marker_history_units(
    db: AsyncSession,
    measurement_type: MeasurementType,
    measurements: list[Measurement],
) -> None:
    conversion_requests: list[tuple[int, str, str]] = []
    for measurement in measurements:
        target_unit = measurement_type.canonical_unit
        value_source_unit = measurement.effective_original_unit
        previous_canonical_unit = measurement.canonical_unit
        reference_source_unit = (
            value_source_unit
            if measurement.original_reference_low is not None or measurement.original_reference_high is not None
            else previous_canonical_unit
        )

        if (
            measurement.original_value is not None
            and value_source_unit is not None
            and target_unit is not None
            and not rescaling.units_equivalent(value_source_unit, target_unit)
        ):
            conversion_requests.append((measurement_type.id, value_source_unit, target_unit))

        if (
            (measurement.original_reference_low is not None or measurement.original_reference_high is not None)
            or (measurement.canonical_reference_low is not None or measurement.canonical_reference_high is not None)
        ) and (
            reference_source_unit is not None
            and target_unit is not None
            and not rescaling.units_equivalent(reference_source_unit, target_unit)
        ):
            conversion_requests.append((measurement_type.id, reference_source_unit, target_unit))

    conversion_rule_map = await rescaling.load_rescaling_rules(db, conversion_requests)

    for measurement in measurements:
        measurement.normalization_error = None
        measurement.normalization_status = VISIBLE_MEASUREMENT_STATUS

        target_unit = measurement_type.canonical_unit
        previous_canonical_unit = measurement.canonical_unit
        value_source_unit = measurement.effective_original_unit
        reference_source_low = (
            measurement.original_reference_low
            if measurement.original_reference_low is not None
            else measurement.canonical_reference_low
        )
        reference_source_high = (
            measurement.original_reference_high
            if measurement.original_reference_high is not None
            else measurement.canonical_reference_high
        )
        reference_source_unit = (
            value_source_unit
            if measurement.original_reference_low is not None or measurement.original_reference_high is not None
            else previous_canonical_unit
        )

        if measurement.original_value is None:
            measurement.canonical_unit = target_unit
            continue

        measurement.canonical_unit = target_unit
        if value_source_unit is None or target_unit is None:
            measurement.canonical_value = measurement.original_value
            measurement.canonical_reference_low = reference_source_low
            measurement.canonical_reference_high = reference_source_high
            continue

        if rescaling.units_equivalent(value_source_unit, target_unit):
            measurement.canonical_value = measurement.original_value
        else:
            original_key = rescaling.normalize_unit_key(value_source_unit)
            canonical_key = rescaling.normalize_unit_key(target_unit)
            if original_key is None or canonical_key is None:
                measurement.canonical_value = None
            else:
                rule = conversion_rule_map.get((measurement_type.id, original_key, canonical_key))
                if rule is None or rule.scale_factor is None:
                    # Shared unit edits should complete synchronously. When there is no
                    # known safe conversion yet, keep the row resolved and surface the
                    # missing-conversion warning instead of queueing background work.
                    measurement.canonical_value = None
                else:
                    measurement.canonical_value = rescaling.apply_scale_factor(
                        measurement.original_value,
                        rule.scale_factor,
                    )

        if reference_source_low is None and reference_source_high is None:
            measurement.canonical_reference_low = None
            measurement.canonical_reference_high = None
            continue

        if reference_source_unit is None or target_unit is None:
            measurement.canonical_reference_low = reference_source_low
            measurement.canonical_reference_high = reference_source_high
            continue

        if rescaling.units_equivalent(reference_source_unit, target_unit):
            measurement.canonical_reference_low = reference_source_low
            measurement.canonical_reference_high = reference_source_high
            continue

        original_key = rescaling.normalize_unit_key(reference_source_unit)
        canonical_key = rescaling.normalize_unit_key(target_unit)
        if original_key is None or canonical_key is None:
            measurement.canonical_reference_low = None
            measurement.canonical_reference_high = None
            continue

        rule = conversion_rule_map.get((measurement_type.id, original_key, canonical_key))
        if rule is None or rule.scale_factor is None:
            measurement.canonical_reference_low = None
            measurement.canonical_reference_high = None
            continue

        measurement.canonical_reference_low = rescaling.apply_scale_factor(
            reference_source_low,
            rule.scale_factor,
        )
        measurement.canonical_reference_high = rescaling.apply_scale_factor(
            reference_source_high,
            rule.scale_factor,
        )


async def get_measurement_or_404(measurement_id: int, db: AsyncSession) -> Measurement:
    result = await db.execute(
        select(Measurement)
        .options(
            selectinload(Measurement.measurement_type).selectinload(MeasurementType.aliases),
            selectinload(Measurement.lab_file).selectinload(LabFile.tags),
        )
        .where(Measurement.id == measurement_id)
    )
    measurement = result.scalar_one_or_none()
    if measurement is None:
        raise HTTPException(404, "Measurement not found")
    return measurement


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
            marker_service.effective_measurement_timestamp_sql().asc(),
            Measurement.id.asc(),
        )
    )
    if marker_name:
        query = query.where(MeasurementType.name == marker_name)
    result = await db.execute(query)
    measurements = result.scalars().all()
    await annotate_missing_rescaling_measurements(db, measurements)
    return measurements


@router.patch("/measurements/{measurement_id}", response_model=MeasurementOut, tags=["measurements"])
async def update_measurement(measurement_id: int, body: MeasurementPatchRequest, db: AsyncSession = Depends(get_db)):
    measurement = await get_measurement_or_404(measurement_id, db)

    reset_fields = set(body.reset_fields)

    if "canonical_value" in reset_fields:
        measurement.user_canonical_value_override = False
        measurement.user_canonical_value = None
    elif "canonical_value" in body.model_fields_set:
        measurement.user_canonical_value_override = True
        measurement.user_canonical_value = body.canonical_value

    if "canonical_unit" in reset_fields:
        measurement.user_canonical_unit_override = False
        measurement.user_canonical_unit = None
    elif "canonical_unit" in body.model_fields_set:
        measurement.user_canonical_unit_override = True
        measurement.user_canonical_unit = body.canonical_unit

    if "original_unit" in reset_fields:
        measurement.user_original_unit_override = False
        measurement.user_original_unit = None
    elif "original_unit" in body.model_fields_set:
        measurement.user_original_unit_override = True
        measurement.user_original_unit = body.original_unit

    if "canonical_reference_low" in reset_fields:
        measurement.user_canonical_reference_low_override = False
        measurement.user_canonical_reference_low = None
    elif "canonical_reference_low" in body.model_fields_set:
        measurement.user_canonical_reference_low_override = True
        measurement.user_canonical_reference_low = body.canonical_reference_low

    if "canonical_reference_high" in reset_fields:
        measurement.user_canonical_reference_high_override = False
        measurement.user_canonical_reference_high = None
    elif "canonical_reference_high" in body.model_fields_set:
        measurement.user_canonical_reference_high_override = True
        measurement.user_canonical_reference_high = body.canonical_reference_high

    if "measured_at" in reset_fields:
        measurement.user_measured_at_override = False
        measurement.user_measured_at = None
    elif "measured_at" in body.model_fields_set:
        measurement.user_measured_at_override = True
        measurement.user_measured_at = body.measured_at

    if "qualitative" in reset_fields:
        measurement.user_qualitative_value_override = False
        measurement.user_qualitative_value = None
        measurement.user_qualitative_bool_override = False
        measurement.user_qualitative_bool = None
    elif "qualitative_expression" in body.model_fields_set:
        qualitative_value, qualitative_bool = parse_qualitative_expression(body.qualitative_expression)
        measurement.user_qualitative_value_override = True
        measurement.user_qualitative_value = qualitative_value
        measurement.user_qualitative_bool_override = True
        measurement.user_qualitative_bool = qualitative_bool

    if any(
        field in body.model_fields_set or field in reset_fields
        for field in {
            "canonical_value",
            "canonical_unit",
            "original_unit",
            "canonical_reference_low",
            "canonical_reference_high",
            "measured_at",
            "qualitative_expression",
            "qualitative",
        }
    ):
        measurement.user_edited_at = datetime.now(UTC)

    await annotate_missing_rescaling_measurements(db, [measurement])
    await refresh_search_projection(measurement.lab_file, db)
    await db.commit()
    refreshed_measurement = await get_measurement_or_404(measurement_id, db)
    await annotate_missing_rescaling_measurements(db, [refreshed_measurement])
    return MeasurementOut.model_validate(refreshed_measurement)


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
            marker_service.effective_measurement_timestamp_sql().asc(),
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


@router.patch("/markers/{marker_name:path}", response_model=MarkerDetailResponse, tags=["measurements"])
async def update_marker(marker_name: str, body: MarkerPatchRequest, db: AsyncSession = Depends(get_db)):
    measurement_type = await marker_service.get_measurement_type_by_name(db, marker_name)
    if measurement_type is None:
        raise HTTPException(404, "Marker not found")

    current_name = measurement_type.name
    current_canonical_unit = measurement_type.canonical_unit
    next_name = current_name
    if "name" in body.model_fields_set:
        if body.name is None:
            raise HTTPException(400, "Marker name is required")
        next_name = body.name
    next_canonical_unit = body.canonical_unit if "canonical_unit" in body.model_fields_set else current_canonical_unit
    marker_changed = next_name != current_name or next_canonical_unit != current_canonical_unit

    if next_name == current_name and next_canonical_unit == current_canonical_unit:
        return await measurement_detail(marker_name=current_name, db=db)

    if next_name != current_name:
        conflict = await marker_service.get_measurement_type_by_name(db, next_name)
        if conflict is not None and conflict.id != measurement_type.id:
            raise HTTPException(409, "Marker name already exists")

        measurement_type.name = next_name
        measurement_type.normalized_key = marker_service.normalize_marker_alias_key(next_name)
        await marker_service.ensure_measurement_type_aliases(
            db,
            [(current_name, measurement_type), (next_name, measurement_type)],
        )

    if next_canonical_unit != current_canonical_unit:
        measurement_type.canonical_unit = next_canonical_unit
        marker_measurements = await marker_service.load_measurements_for_marker(db, measurement_type.name)
        resolved_measurements = [
            measurement
            for measurement in marker_measurements
            if measurement.normalization_status == VISIBLE_MEASUREMENT_STATUS
        ]
        await _recalculate_marker_history_units(db, measurement_type, resolved_measurements)

    affected_file_result = await db.execute(
        select(LabFile).join(Measurement).where(Measurement.measurement_type_id == measurement_type.id).distinct()
    )
    affected_files = affected_file_result.scalars().all()
    for lab_file in affected_files:
        await refresh_search_projection(lab_file, db)

    if marker_changed:
        await insight_service.invalidate_cached_insight(measurement_type, db)

    await db.commit()
    return await measurement_detail(marker_name=measurement_type.name, db=db)


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
        if marker_service.effective_measurement_value(measurement) is not None
        and not getattr(measurement, "unit_conversion_missing", False)
    ]
    if not sparkline_measurements:
        sparkline_measurements = [
            measurement
            for measurement in measurements
            if marker_service.effective_measurement_value(measurement) is not None
        ]
    if sparkline_measurements:
        signature = insight_service.marker_signature(sparkline_measurements)
        cached = get_cached_sparkline(marker_name, signature)
        if cached:
            return Response(content=cached, media_type="image/png", headers={"Cache-Control": "no-store"})

        ref_low, ref_high = marker_service.latest_reference_range_for_history(sparkline_measurements)
        numeric_values = [
            value
            for measurement in sparkline_measurements
            if (value := marker_service.effective_measurement_value(measurement)) is not None
        ]
        png_bytes = generate_sparkline(
            values=numeric_values,
            ref_low=ref_low,
            ref_high=ref_high,
            signature=signature,
            marker_name=marker_name,
        )
        return Response(content=png_bytes, media_type="image/png", headers={"Cache-Control": "no-store"})

    qualitative_sparkline_measurements = [
        measurement
        for measurement in measurements
        if marker_service.effective_measurement_qualitative_bool(measurement) is not None
    ]
    if len(qualitative_sparkline_measurements) < 2:
        raise HTTPException(404, "Marker not found")

    signature = insight_service.marker_signature(qualitative_sparkline_measurements)
    cached = get_cached_sparkline(marker_name, signature)
    if cached:
        return Response(content=cached, media_type="image/png", headers={"Cache-Control": "no-store"})

    png_bytes = generate_sparkline(
        values=[
            1.0 if marker_service.effective_measurement_qualitative_bool(measurement) else 0.0
            for measurement in qualitative_sparkline_measurements
        ],
        ref_low=None,
        ref_high=0.5,
        signature=signature,
        marker_name=marker_name,
        qualitative_mode=True,
    )
    return Response(content=png_bytes, media_type="image/png", headers={"Cache-Control": "no-store"})
