"""OCR result ingestion and normalization."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime

from fastapi import HTTPException
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import illdashboard.services.search as search_service
from illdashboard.copilot import normalization as copilot_normalization
from illdashboard.copilot.normalization import (
    MarkerObservation,
    MarkerUnitGroup,
    QualitativeNormalizationRequest,
    UnitConversionRequest,
)
from illdashboard.models import LabFile, LabFileTag, Measurement, MeasurementType, QualitativeRule, RescalingRule
from illdashboard.services.markers import (
    build_source_tag,
    ensure_measurement_type_aliases,
    ensure_measurement_types,
    is_source_tag,
    load_measurement_type_aliases,
    normalize_source_tag_value,
    source_tag_value,
)
from illdashboard.services.qualitative_values import load_qualitative_rules, normalize_qualitative_key, upsert_qualitative_rules
from illdashboard.services.rescaling import apply_scale_factor, load_rescaling_rules, normalize_unit_key, units_equivalent, upsert_rescaling_rules


logger = logging.getLogger(__name__)

_ocr_persist_lock = asyncio.Lock()


@dataclass
class ParsedMeasurement:
    index: int
    measurement: dict
    canonical_name: str
    value: float | None
    original_qualitative_value: str | None
    original_unit: str | None
    original_reference_low: float | None
    original_reference_high: float | None
    canonical_qualitative_value: str | None = None
    qualitative_bool: bool | None = None


@dataclass
class PreparedMeasurements:
    measurement_types: dict[str, MeasurementType]
    parsed_measurements: list[ParsedMeasurement]

    def build_conversion_requests(self) -> list[UnitConversionRequest]:
        return _build_conversion_requests(self.parsed_measurements, self.measurement_types)


def _clean_qualitative_value(raw) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return str(raw).lower()
    if not isinstance(raw, str):
        return None

    value = raw.strip()
    if not value:
        return None

    value = re.sub(r"\s+", " ", value)
    value = value.strip(".:;,()[]{}")
    normalized = value.casefold().strip()
    return normalized or None


def parse_numeric_value(raw) -> float | None:
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int | float):
        return float(raw) if math.isfinite(raw) else None
    if not isinstance(raw, str):
        return None

    value = raw.strip()
    if not value:
        return None

    value = re.sub(r"(\d)\s+(\d{3})(?!\d)", r"\1\2", value)
    value = re.sub(r"(\d)\s+(\d)", r"\1.\2", value)
    if value.count(",") == 1:
        value = re.sub(r"(\d),(\d)", r"\1.\2", value)
    value = value.replace(" ", "")

    try:
        parsed = float(value)
    except (ValueError, OverflowError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def parse_measurement_value(raw) -> tuple[float | None, str | None]:
    numeric_value = parse_numeric_value(raw)
    if numeric_value is not None:
        return numeric_value, None
    return None, _clean_qualitative_value(raw)


def normalize_marker_name_deterministic(name: str) -> str:
    name = re.sub(r"(?<!\s)\[", " [", name)
    name = re.sub(r"\s+-\s*|\s*-\s+", " - ", name)
    name = re.sub(r"  +", " ", name)
    return name.strip()


def normalize_document_text(raw) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def _reset_ocr_fields(lab: LabFile) -> None:
    lab.ocr_raw = None
    lab.ocr_text_raw = None
    lab.ocr_text_english = None
    lab.ocr_summary_english = None
    lab.lab_date = None


def _apply_ocr_metadata(lab: LabFile, result: dict) -> None:
    _reset_ocr_fields(lab)
    lab.ocr_raw = json.dumps(result)
    lab.ocr_text_raw = normalize_document_text(result.get("raw_text"))
    lab.ocr_text_english = normalize_document_text(result.get("translated_text_english") or result.get("translated_text"))
    if lab.ocr_text_english is None:
        lab.ocr_text_english = lab.ocr_text_raw
    lab.ocr_summary_english = normalize_document_text(result.get("summary_english"))
    if lab.ocr_summary_english is None:
        lab.ocr_summary_english = lab.ocr_text_english
    if result.get("lab_date"):
        try:
            lab.lab_date = datetime.fromisoformat(result["lab_date"])
        except (ValueError, TypeError):
            pass


async def _resolve_canonical_measurement_types(
    result: dict,
    db: AsyncSession,
) -> tuple[dict[str, MeasurementType], dict[str, str]]:
    raw_names = [measurement["marker_name"] for measurement in result.get("measurements", [])]
    deterministic_map = {name: normalize_marker_name_deterministic(name) for name in raw_names}
    resolved_cleaned_names = await _resolve_canonical_marker_names(
        raw_names=raw_names,
        deterministic_map=deterministic_map,
        db=db,
    )

    canonical_map = {
        raw_name: resolved_cleaned_names.get(deterministic_map[raw_name], deterministic_map[raw_name])
        for raw_name in raw_names
    }
    measurement_types = await ensure_measurement_types(db, [canonical_map[raw_name] for raw_name in raw_names])
    await ensure_measurement_type_aliases(
        db,
        [
            (alias_name, measurement_types[canonical_map[raw_name]])
            for raw_name in raw_names
            for alias_name in (raw_name, deterministic_map[raw_name])
        ],
    )
    await db.flush()
    return measurement_types, canonical_map


async def _resolve_canonical_marker_names(
    *,
    raw_names: list[str],
    deterministic_map: dict[str, str],
    db: AsyncSession,
) -> dict[str, str]:
    cleaned_names = list(dict.fromkeys(deterministic_map.values()))
    alias_matches = await load_measurement_type_aliases(db, [*raw_names, *cleaned_names])
    resolved_cleaned_names: dict[str, str] = {}

    for alias_name in [*raw_names, *cleaned_names]:
        alias_match = alias_matches.get(alias_name)
        if alias_match is None:
            continue
        cleaned_name = deterministic_map.get(alias_name, alias_name)
        resolved_cleaned_names[cleaned_name] = alias_match.name

    unresolved_cleaned_names = [name for name in cleaned_names if name not in resolved_cleaned_names]
    if not unresolved_cleaned_names:
        return resolved_cleaned_names

    existing_result = await db.execute(select(MeasurementType.name).order_by(MeasurementType.name))
    existing_canonical = list(existing_result.scalars().all())
    try:
        llm_map = await copilot_normalization.normalize_marker_names(unresolved_cleaned_names, existing_canonical)
    except Exception:
        llm_map = {name: name for name in unresolved_cleaned_names}

    return {**llm_map, **resolved_cleaned_names}


def _prepare_measurements_for_persistence(
    result: dict,
    measurement_types: dict[str, MeasurementType],
    canonical_map: dict[str, str],
) -> tuple[list[ParsedMeasurement], list[MarkerUnitGroup]]:
    parsed_measurements: list[ParsedMeasurement] = []
    numeric_groups: dict[str, MarkerUnitGroup] = {}

    for index, measurement in enumerate(result.get("measurements", [])):
        value, qualitative_value = parse_measurement_value(measurement.get("value"))
        if value is None and qualitative_value is None:
            logger.warning(
                "Skipping measurement %r: invalid value %r",
                measurement.get("marker_name"),
                measurement.get("value"),
            )
            continue

        ref_low = parse_numeric_value(measurement.get("reference_low"))
        ref_high = parse_numeric_value(measurement.get("reference_high"))
        canonical_name = canonical_map.get(measurement["marker_name"], measurement["marker_name"])
        if canonical_name is None:
            continue

        parsed = ParsedMeasurement(
            index=index,
            measurement=measurement,
            canonical_name=canonical_name,
            value=value,
            original_qualitative_value=qualitative_value,
            original_unit=measurement.get("unit"),
            original_reference_low=ref_low,
            original_reference_high=ref_high,
        )
        parsed_measurements.append(parsed)

        if value is None:
            continue

        group = numeric_groups.setdefault(
            canonical_name,
            MarkerUnitGroup(
                marker_name=canonical_name,
                existing_canonical_unit=measurement_types[canonical_name].canonical_unit,
                observations=[],
            ),
        )
        group.observations.append(
            MarkerObservation(
                id=str(index),
                value=value,
                unit=measurement.get("unit"),
                reference_low=ref_low,
                reference_high=ref_high,
            )
        )

    return parsed_measurements, list(numeric_groups.values())


async def _choose_and_apply_canonical_units(
    measurement_types: dict[str, MeasurementType],
    numeric_groups: list[MarkerUnitGroup],
    db: AsyncSession,
) -> None:
    try:
        canonical_units = await copilot_normalization.choose_canonical_units(numeric_groups)
    except Exception:
        canonical_units = {}

    for canonical_name, measurement_type in measurement_types.items():
        canonical_unit = canonical_units.get(canonical_name) or measurement_type.canonical_unit
        if canonical_unit and measurement_type.canonical_unit != canonical_unit:
            measurement_type.canonical_unit = canonical_unit

    await db.flush()


def _build_conversion_requests(
    parsed_measurements: list[ParsedMeasurement],
    measurement_types: dict[str, MeasurementType],
) -> list[UnitConversionRequest]:
    conversion_requests: list[UnitConversionRequest] = []
    seen_request_ids: set[str] = set()

    for parsed in parsed_measurements:
        if parsed.value is None:
            continue

        canonical_unit = measurement_types[parsed.canonical_name].canonical_unit or parsed.original_unit
        original_unit = parsed.original_unit
        if original_unit is None or canonical_unit is None or units_equivalent(original_unit, canonical_unit):
            continue

        request_id = _rescaling_request_id(original_unit, canonical_unit)
        if request_id in seen_request_ids:
            continue

        seen_request_ids.add(request_id)
        conversion_requests.append(
            UnitConversionRequest(
                id=request_id,
                marker_name=parsed.canonical_name,
                original_unit=original_unit,
                canonical_unit=canonical_unit,
                example_value=parsed.value,
                reference_low=parsed.original_reference_low,
                reference_high=parsed.original_reference_high,
            )
        )

    return conversion_requests


def _build_qualitative_normalization_requests(
    parsed_measurements: list[ParsedMeasurement],
) -> list[QualitativeNormalizationRequest]:
    requests: list[QualitativeNormalizationRequest] = []
    seen_request_ids: set[str] = set()

    for parsed in parsed_measurements:
        if parsed.original_qualitative_value is None:
            continue

        request_id = normalize_qualitative_key(parsed.original_qualitative_value)
        if request_id is None or request_id in seen_request_ids:
            continue

        seen_request_ids.add(request_id)
        requests.append(
            QualitativeNormalizationRequest(
                id=request_id,
                marker_name=parsed.canonical_name,
                original_value=parsed.original_qualitative_value,
            )
        )

    return requests


async def _resolve_qualitative_rule_map(
    db: AsyncSession,
    requests: list[QualitativeNormalizationRequest],
    measurement_types: dict[str, MeasurementType],
) -> dict[str, QualitativeRule]:
    if not requests:
        return {}

    requested_values = [request.original_value for request in requests]
    rule_map = await load_qualitative_rules(db, requested_values)

    missing_requests: list[QualitativeNormalizationRequest] = []
    for request in requests:
        request_key = normalize_qualitative_key(request.original_value)
        if request_key is None:
            continue
        rule = rule_map.get(request_key)
        if rule is None or rule.boolean_value is None:
            missing_requests.append(request)

    if missing_requests:
        existing_result = await db.execute(
            select(Measurement.qualitative_value)
            .where(Measurement.qualitative_value.is_not(None))
            .distinct()
            .order_by(Measurement.qualitative_value.asc())
        )
        existing_canonical = [value for value in existing_result.scalars().all() if isinstance(value, str)]

        try:
            normalized_values = await copilot_normalization.normalize_qualitative_values(missing_requests, existing_canonical)
        except Exception:
            normalized_values = {}

        await upsert_qualitative_rules(
            db,
            [
                {
                    "original_value": request.original_value,
                    "canonical_value": canonical_value,
                    "boolean_value": boolean_value,
                    "measurement_type": measurement_types.get(request.marker_name),
                }
                for request in missing_requests
                if isinstance((decision := normalized_values.get(request.id)), tuple)
                if isinstance((canonical_value := decision[0]), str) and canonical_value.strip()
                if (boolean_value := decision[1]) is None or isinstance(boolean_value, bool)
            ],
        )

        await db.flush()
        rule_map = await load_qualitative_rules(db, requested_values)

    return rule_map


def _apply_qualitative_rules(
    parsed_measurements: list[ParsedMeasurement],
    rule_map: dict[str, QualitativeRule],
) -> None:
    for parsed in parsed_measurements:
        if parsed.original_qualitative_value is None:
            continue

        request_key = normalize_qualitative_key(parsed.original_qualitative_value)
        rule = rule_map.get(request_key) if request_key is not None else None
        parsed.canonical_qualitative_value = getattr(rule, "canonical_value", None) or parsed.original_qualitative_value
        parsed.qualitative_bool = getattr(rule, "boolean_value", None)


def _build_measurement_model(
    *,
    lab: LabFile,
    parsed: ParsedMeasurement,
    measurement_type: MeasurementType,
    rule_map: dict[tuple[str, str], RescalingRule],
) -> Measurement:
    measurement = parsed.measurement
    measured_at = None
    if measurement.get("measured_at"):
        try:
            measured_at = datetime.fromisoformat(measurement["measured_at"])
        except (ValueError, TypeError):
            measured_at = lab.lab_date

    canonical_unit = measurement_type.canonical_unit or parsed.original_unit
    original_unit = _prefer_canonical_unit_text(parsed.original_unit, canonical_unit)
    normalized_value, normalized_ref_low, normalized_ref_high = _apply_rescaling_rule(
        value=parsed.value,
        reference_low=parsed.original_reference_low,
        reference_high=parsed.original_reference_high,
        original_unit=original_unit,
        canonical_unit=canonical_unit,
        rule_map=rule_map,
    )

    return Measurement(
        lab_file_id=lab.id,
        measurement_type=measurement_type,
        canonical_value=normalized_value,
        original_value=parsed.value,
        original_qualitative_value=parsed.original_qualitative_value,
        qualitative_bool=parsed.qualitative_bool,
        qualitative_value=parsed.canonical_qualitative_value,
        original_unit=original_unit,
        canonical_reference_low=normalized_ref_low,
        canonical_reference_high=normalized_ref_high,
        original_reference_low=parsed.original_reference_low,
        original_reference_high=parsed.original_reference_high,
        measured_at=measured_at or lab.lab_date,
        page_number=int(measurement["page_number"]) if measurement.get("page_number") is not None else None,
    )


async def normalize_lab_source(lab: LabFile, result: dict, db: AsyncSession) -> str | None:
    existing_source_result = await db.execute(
        select(LabFileTag.tag).where(LabFileTag.tag.like("source:%")).distinct().order_by(LabFileTag.tag)
    )
    existing_sources = [value for tag in existing_source_result.scalars().all() if (value := source_tag_value(tag))]

    try:
        normalized_source = await copilot_normalization.normalize_source_name(result.get("source"), lab.filename, existing_sources)
    except Exception:
        normalized_source = None

    if normalized_source:
        normalized_source = normalize_source_tag_value(normalized_source)
    elif result.get("source"):
        normalized_source = normalize_source_tag_value(str(result["source"]))

    return normalized_source or None


async def sync_lab_source_tag(lab: LabFile, source_value: str | None, db: AsyncSession) -> None:
    tag_result = await db.execute(select(LabFileTag).where(LabFileTag.lab_file_id == lab.id))
    existing_tags = tag_result.scalars().all()
    existing_source_tags = [tag for tag in existing_tags if is_source_tag(tag.tag)]

    source_tag = build_source_tag(source_value) if source_value else None
    if len(existing_source_tags) == 1 and existing_source_tags[0].tag == source_tag:
        return

    if existing_source_tags:
        await db.execute(
            delete(LabFileTag).where(
                LabFileTag.id.in_([tag.id for tag in existing_source_tags if tag.id is not None])
            )
        )
        await db.flush()

    if source_tag is None:
        return

    db.add(LabFileTag(lab_file_id=lab.id, tag=source_tag))
    await db.flush()


async def _sync_ocr_source(lab: LabFile, result: dict, db: AsyncSession) -> str | None:
    source_value = await normalize_lab_source(lab, result, db)
    await sync_lab_source_tag(lab, source_value, db)
    return source_value


async def _prepare_measurements_for_lab(result: dict, db: AsyncSession) -> PreparedMeasurements:
    measurement_types, canonical_map = await _resolve_canonical_measurement_types(result, db)
    parsed_measurements, numeric_groups = _prepare_measurements_for_persistence(result, measurement_types, canonical_map)

    await _choose_and_apply_canonical_units(measurement_types, numeric_groups, db)

    qualitative_requests = _build_qualitative_normalization_requests(parsed_measurements)
    qualitative_rule_map = await _resolve_qualitative_rule_map(db, qualitative_requests, measurement_types)
    _apply_qualitative_rules(parsed_measurements, qualitative_rule_map)

    return PreparedMeasurements(
        measurement_types=measurement_types,
        parsed_measurements=parsed_measurements,
    )


def _build_measurement_models(
    *,
    lab: LabFile,
    prepared: PreparedMeasurements,
    rule_map: dict[tuple[str, str], RescalingRule],
) -> list[Measurement]:
    return [
        _build_measurement_model(
            lab=lab,
            parsed=parsed,
            measurement_type=prepared.measurement_types[parsed.canonical_name],
            rule_map=rule_map,
        )
        for parsed in prepared.parsed_measurements
    ]


async def _clear_persisted_ocr_result(lab: LabFile, db: AsyncSession) -> None:
    await db.execute(delete(Measurement).where(Measurement.lab_file_id == lab.id))
    _reset_ocr_fields(lab)
    await db.flush()


async def apply_ocr_result(
    lab: LabFile,
    result: dict,
    db: AsyncSession,
    *,
    job_id: str | None = None,
) -> list[Measurement]:
    started_at = time.perf_counter()
    raw_measurement_count = len(result.get("measurements", []))
    logger.info(
        "Applying OCR result start job_id=%s file_id=%s filename=%s raw_measurements=%s",
        job_id,
        lab.id,
        lab.filename,
        raw_measurement_count,
    )
    _apply_ocr_metadata(lab, result)
    source_sync_started_at = time.perf_counter()
    source_value = await _sync_ocr_source(lab, result, db)
    logger.info(
        "Applying OCR result source synced job_id=%s file_id=%s filename=%s source=%s duration=%.2fs",
        job_id,
        lab.id,
        lab.filename,
        source_value,
        time.perf_counter() - source_sync_started_at,
    )

    prepare_started_at = time.perf_counter()
    prepared = await _prepare_measurements_for_lab(result, db)
    conversion_requests = prepared.build_conversion_requests()
    qualitative_request_count = sum(
        1 for parsed in prepared.parsed_measurements if parsed.original_qualitative_value is not None
    )
    logger.info(
        "Applying OCR result prepared measurements job_id=%s file_id=%s filename=%s parsed_measurements=%s measurement_types=%s conversion_requests=%s qualitative_requests=%s duration=%.2fs",
        job_id,
        lab.id,
        lab.filename,
        len(prepared.parsed_measurements),
        len(prepared.measurement_types),
        len(conversion_requests),
        qualitative_request_count,
        time.perf_counter() - prepare_started_at,
    )

    rescaling_started_at = time.perf_counter()
    rule_map = await _resolve_rescaling_rule_map(
        db,
        conversion_requests,
        prepared.measurement_types,
    )
    logger.info(
        "Applying OCR result resolved rescaling job_id=%s file_id=%s filename=%s conversion_requests=%s resolved_rules=%s duration=%.2fs",
        job_id,
        lab.id,
        lab.filename,
        len(conversion_requests),
        len(rule_map),
        time.perf_counter() - rescaling_started_at,
    )

    flush_started_at = time.perf_counter()
    new_measurements = _build_measurement_models(lab=lab, prepared=prepared, rule_map=rule_map)
    db.add_all(new_measurements)
    await db.flush()
    logger.info(
        "Applying OCR result flushed measurements job_id=%s file_id=%s filename=%s saved_measurements=%s flush_duration=%.2fs total_duration=%.2fs",
        job_id,
        lab.id,
        lab.filename,
        len(new_measurements),
        time.perf_counter() - flush_started_at,
        time.perf_counter() - started_at,
    )
    return new_measurements


async def persist_ocr_result(
    lab: LabFile,
    result: dict,
    db: AsyncSession,
    *,
    job_id: str | None = None,
) -> list[Measurement]:
    started_at = time.perf_counter()
    logger.info("Persist OCR result start job_id=%s file_id=%s filename=%s", job_id, lab.id, lab.filename)
    clear_started_at = time.perf_counter()
    await _clear_persisted_ocr_result(lab, db)
    logger.info(
        "Persist OCR result cleared previous job_id=%s file_id=%s filename=%s duration=%.2fs",
        job_id,
        lab.id,
        lab.filename,
        time.perf_counter() - clear_started_at,
    )

    new_measurements = await apply_ocr_result(lab, result, db, job_id=job_id)
    search_started_at = time.perf_counter()
    await search_service.refresh_lab_search_document(lab.id, db)
    logger.info(
        "Persist OCR result refreshed search job_id=%s file_id=%s filename=%s duration=%.2fs",
        job_id,
        lab.id,
        lab.filename,
        time.perf_counter() - search_started_at,
    )
    logger.info(
        "Persist OCR result finished job_id=%s file_id=%s filename=%s duration=%.2fs saved_measurements=%s",
        job_id,
        lab.id,
        lab.filename,
        time.perf_counter() - started_at,
        len(new_measurements),
    )
    return new_measurements


async def persist_ocr_result_with_fresh_session(
    file_id: int,
    result: dict,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: str | None = None,
) -> list[int]:
    lock_wait_started_at = time.perf_counter()
    logger.info("Persist OCR session waiting for lock job_id=%s file_id=%s", job_id, file_id)
    async with _ocr_persist_lock:
        lock_acquired_at = time.perf_counter()
        logger.info(
            "Persist OCR session acquired lock job_id=%s file_id=%s wait_ms=%.1f",
            job_id,
            file_id,
            (lock_acquired_at - lock_wait_started_at) * 1000,
        )
        async with session_factory() as session:
            session_started_at = time.perf_counter()
            persistent_lab = await session.get(LabFile, file_id)
            if persistent_lab is None:
                raise HTTPException(404, f"File {file_id} not found")

            new_measurements = await persist_ocr_result(persistent_lab, result, session, job_id=job_id)
            measurement_ids = [measurement.id for measurement in new_measurements if measurement.id is not None]
            commit_started_at = time.perf_counter()
            await session.commit()
            logger.info(
                "Persist OCR session committed job_id=%s file_id=%s filename=%s commit_ms=%.1f lock_hold_ms=%.1f total_session_ms=%.1f saved_measurements=%s",
                job_id,
                file_id,
                persistent_lab.filename,
                (time.perf_counter() - commit_started_at) * 1000,
                (time.perf_counter() - lock_acquired_at) * 1000,
                (time.perf_counter() - session_started_at) * 1000,
                len(measurement_ids),
            )
            return measurement_ids


def _rescaling_request_id(original_unit: str, canonical_unit: str) -> str:
    original_key = normalize_unit_key(original_unit) or original_unit.strip()
    canonical_key = normalize_unit_key(canonical_unit) or canonical_unit.strip()
    return f"{original_key}=>{canonical_key}"


async def _resolve_rescaling_rule_map(
    db: AsyncSession,
    conversion_requests: list[UnitConversionRequest],
    measurement_types: dict[str, MeasurementType],
) -> dict[tuple[str, str], RescalingRule]:
    if not conversion_requests:
        return {}

    requested_pairs = [(request.original_unit, request.canonical_unit) for request in conversion_requests]
    rule_map = await load_rescaling_rules(db, requested_pairs)

    missing_requests: list[UnitConversionRequest] = []
    for request in conversion_requests:
        original_key = normalize_unit_key(request.original_unit)
        canonical_key = normalize_unit_key(request.canonical_unit)
        if original_key is None or canonical_key is None:
            continue
        if (original_key, canonical_key) not in rule_map:
            missing_requests.append(request)

    if missing_requests:
        try:
            inferred_factors = await copilot_normalization.infer_rescaling_factors(missing_requests)
        except Exception:
            inferred_factors = {}

        await upsert_rescaling_rules(
            db,
            [
                {
                    "original_unit": request.original_unit,
                    "canonical_unit": request.canonical_unit,
                    "scale_factor": inferred_factors.get(request.id),
                    "measurement_type": measurement_types.get(request.marker_name),
                }
                for request in missing_requests
                if inferred_factors.get(request.id) is not None
            ],
        )

        await db.flush()
        rule_map = await load_rescaling_rules(db, requested_pairs)

    return rule_map


def _apply_rescaling_rule(
    *,
    value: float | None,
    reference_low: float | None,
    reference_high: float | None,
    original_unit: str | None,
    canonical_unit: str | None,
    rule_map: dict[tuple[str, str], RescalingRule],
) -> tuple[float | None, float | None, float | None]:
    if units_equivalent(original_unit, canonical_unit) or original_unit is None or canonical_unit is None:
        return value, reference_low, reference_high

    original_key = normalize_unit_key(original_unit)
    canonical_key = normalize_unit_key(canonical_unit)
    if original_key is None or canonical_key is None:
        return value, reference_low, reference_high

    rule = rule_map.get((original_key, canonical_key))
    scale_factor = getattr(rule, "scale_factor", None)
    if scale_factor is None:
        logger.warning(
            "No rescaling factor available for original_unit=%s canonical_unit=%s; keeping original numeric values",
            original_unit,
            canonical_unit,
        )
        return value, reference_low, reference_high

    return (
        apply_scale_factor(value, scale_factor),
        apply_scale_factor(reference_low, scale_factor),
        apply_scale_factor(reference_high, scale_factor),
    )


def _prefer_canonical_unit_text(original_unit: str | None, canonical_unit: str | None) -> str | None:
    if canonical_unit is not None and units_equivalent(original_unit, canonical_unit):
        return canonical_unit
    return original_unit
