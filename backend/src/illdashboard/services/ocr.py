"""OCR processing, marker normalization, and OCR streaming helpers."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import time
import unicodedata
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from illdashboard.config import settings
from illdashboard.copilot_service import choose_canonical_units, infer_rescaling_factors, normalize_marker_names, normalize_source_name, ocr_extract
from illdashboard.models import LabFile, LabFileTag, Measurement, MeasurementType, RescalingRule
import illdashboard.services.search as search_service
from illdashboard.services.markers import (
    backfill_measurement_type_aliases,
    build_source_tag,
    classify_marker_group,
    ensure_measurement_type_aliases,
    ensure_measurement_types,
    get_measurement_type_by_name,
    is_source_tag,
    load_measurement_type_aliases,
    merge_measurement_types,
    normalize_source_tag_value,
    source_tag_value,
)
from illdashboard.services.rescaling import apply_scale_factor, load_rescaling_rules, normalize_unit_key, units_equivalent, upsert_rescaling_rule


logger = logging.getLogger(__name__)

MAX_OCR_CONCURRENCY = 4
OCR_STREAM_KEEPALIVE_INTERVAL = 10
OCR_JOB_TTL_SECONDS = 600
_ocr_persist_lock = asyncio.Lock()


@dataclass
class OcrJobProgress:
    file_id: int
    filename: str
    index: int
    total: int
    status: str
    error: str | None = None


@dataclass
class OcrJobState:
    job_id: str
    status: str
    total: int
    progress_by_file: dict[int, OcrJobProgress] = field(default_factory=dict)
    completed_count: int = 0
    error_count: int = 0
    created_at: float = field(default_factory=time.time)
    last_updated_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    task: asyncio.Task | None = None


_ocr_jobs: dict[str, OcrJobState] = {}


def _prune_ocr_jobs(*, now: float | None = None) -> None:
    current_time = time.time() if now is None else now
    expired_job_ids = [
        job_id
        for job_id, job in _ocr_jobs.items()
        if current_time - job.last_updated_at >= OCR_JOB_TTL_SECONDS
    ]
    for job_id in expired_job_ids:
        _ocr_jobs.pop(job_id, None)


def _touch_job(job: OcrJobState, *, now: float | None = None) -> None:
    job.last_updated_at = time.time() if now is None else now

QUALITATIVE_TRUE_VALUES = {
    "positive",
    "pozitivni",
    "pozitivny",
    "reactive",
    "reaktivni",
    "detected",
    "present",
    "true",
    "pos",
}
QUALITATIVE_FALSE_VALUES = {
    "negative",
    "negativni",
    "negativny",
    "non reactive",
    "non-reactive",
    "nonreactive",
    "nereaktivni",
    "not detected",
    "undetected",
    "absent",
    "false",
    "neg",
}
QUALITATIVE_INDETERMINATE_VALUES = {
    "equivocal",
    "borderline",
    "indeterminate",
    "inconclusive",
}


def normalize_qualitative_value(raw) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return "positive" if raw else "negative"
    if not isinstance(raw, str):
        return None

    value = raw.strip()
    if not value:
        return None

    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.casefold().strip()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = normalized.strip(".:;,()[]{}")

    if normalized in QUALITATIVE_TRUE_VALUES:
        return "positive"
    if normalized in QUALITATIVE_FALSE_VALUES:
        return "negative"
    if normalized in QUALITATIVE_INDETERMINATE_VALUES:
        return "indeterminate"
    return value


def parse_numeric_value(raw) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        if math.isfinite(raw):
            return float(raw)
        return None
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
    return None, normalize_qualitative_value(raw)


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


async def normalize_lab_source(lab: LabFile, result: dict, db: AsyncSession) -> str | None:
    existing_source_result = await db.execute(
        select(LabFileTag.tag).where(LabFileTag.tag.like("source:%")).distinct().order_by(LabFileTag.tag)
    )
    existing_sources = [value for tag in existing_source_result.scalars().all() if (value := source_tag_value(tag))]

    try:
        normalized_source = await normalize_source_name(result.get("source"), lab.filename, existing_sources)
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

    for tag in existing_source_tags:
        await db.delete(tag)

    await db.flush()

    if not source_value:
        return

    if source_tag is None:
        return

    db.add(LabFileTag(lab_file_id=lab.id, tag=source_tag))
    await db.flush()


async def apply_ocr_result(lab: LabFile, result: dict, db: AsyncSession) -> list[Measurement]:
    logger.info(
        "Applying OCR result file_id=%s filename=%s measurements=%s",
        lab.id,
        lab.filename,
        len(result.get("measurements", [])),
    )
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

    source_value = await normalize_lab_source(lab, result, db)
    await sync_lab_source_tag(lab, source_value, db)

    raw_names = [measurement["marker_name"] for measurement in result.get("measurements", [])]
    deterministic_map = {name: normalize_marker_name_deterministic(name) for name in raw_names}
    cleaned_names = list(dict.fromkeys(deterministic_map.values()))
    alias_matches = await load_measurement_type_aliases(db, [*raw_names, *cleaned_names])

    alias_resolved_names: dict[str, str] = {}
    for raw_name in raw_names:
        alias_match = alias_matches.get(raw_name)
        if alias_match is not None:
            alias_resolved_names[deterministic_map[raw_name]] = alias_match.name

    for cleaned_name in cleaned_names:
        alias_match = alias_matches.get(cleaned_name)
        if alias_match is not None:
            alias_resolved_names[cleaned_name] = alias_match.name

    existing_result = await db.execute(select(MeasurementType.name).order_by(MeasurementType.name))
    existing_canonical = list(existing_result.scalars().all())
    unresolved_cleaned_names = [name for name in cleaned_names if name not in alias_resolved_names]

    if unresolved_cleaned_names:
        try:
            llm_map = await normalize_marker_names(unresolved_cleaned_names, existing_canonical)
        except Exception:
            llm_map = {name: name for name in unresolved_cleaned_names}
    else:
        llm_map = {}

    resolved_cleaned_names = {**llm_map, **alias_resolved_names}

    canonical_map = {
        raw: resolved_cleaned_names.get(deterministic_map[raw], deterministic_map[raw])
        for raw in raw_names
    }
    measurement_types = await ensure_measurement_types(db, [canonical_map[raw] for raw in raw_names])
    await ensure_measurement_type_aliases(
        db,
        [
            (alias_name, measurement_types[canonical_map[raw_name]])
            for raw_name in raw_names
            for alias_name in (raw_name, deterministic_map[raw_name])
        ],
    )
    await db.flush()
    measurement_types = await _load_measurement_types_by_name(db, [canonical_map[raw] for raw in raw_names])

    parsed_measurements: list[dict] = []
    numeric_groups: dict[str, dict] = {}
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

        parsed_measurements.append(
            {
                "index": index,
                "measurement": measurement,
                "canonical_name": canonical_name,
                "value": value,
                "qualitative_value": qualitative_value,
                "original_unit": measurement.get("unit"),
                "original_reference_low": ref_low,
                "original_reference_high": ref_high,
            }
        )

        if value is None:
            continue

        group = numeric_groups.setdefault(
            canonical_name,
            {
                "marker_name": canonical_name,
                "existing_canonical_unit": measurement_types[canonical_name].canonical_unit,
                "observations": [],
            },
        )
        group["observations"].append(
            {
                "id": str(index),
                "value": value,
                "unit": measurement.get("unit"),
                "reference_low": ref_low,
                "reference_high": ref_high,
            }
        )

    try:
        canonical_units = await choose_canonical_units(list(numeric_groups.values()))
    except Exception:
        canonical_units = {}

    for canonical_name, measurement_type in measurement_types.items():
        canonical_unit = canonical_units.get(canonical_name) or measurement_type.canonical_unit
        if canonical_unit and measurement_type.canonical_unit != canonical_unit:
            measurement_type.canonical_unit = canonical_unit

    await db.flush()
    measurement_types = await _load_measurement_types_by_name(db, [canonical_map[raw] for raw in raw_names])

    conversion_requests: list[dict] = []
    seen_request_ids: set[str] = set()
    for parsed in parsed_measurements:
        if parsed["value"] is None:
            continue
        canonical_unit = measurement_types[parsed["canonical_name"]].canonical_unit or parsed["original_unit"]
        original_unit = parsed["original_unit"]
        if original_unit is None or canonical_unit is None or units_equivalent(original_unit, canonical_unit):
            continue

        request_id = _rescaling_request_id(original_unit, canonical_unit)
        if request_id in seen_request_ids:
            continue
        seen_request_ids.add(request_id)
        conversion_requests.append(
            {
                "id": request_id,
                "marker_name": parsed["canonical_name"],
                "original_unit": original_unit,
                "canonical_unit": canonical_unit,
                "example_value": parsed["value"],
                "reference_low": parsed["original_reference_low"],
                "reference_high": parsed["original_reference_high"],
            }
        )

    rule_map = await _resolve_rescaling_rule_map(db, conversion_requests, measurement_types)

    new_measurements: list[Measurement] = []
    for parsed in parsed_measurements:
        measurement = parsed["measurement"]
        measured_at = None
        if measurement.get("measured_at"):
            try:
                measured_at = datetime.fromisoformat(measurement["measured_at"])
            except (ValueError, TypeError):
                measured_at = lab.lab_date

        canonical_name = parsed["canonical_name"]
        value = parsed["value"]
        qualitative_value = parsed["qualitative_value"]
        ref_low = parsed["original_reference_low"]
        ref_high = parsed["original_reference_high"]

        canonical_unit = measurement_types[canonical_name].canonical_unit or parsed["original_unit"]
        original_unit = _prefer_canonical_unit_text(parsed["original_unit"], canonical_unit)
        normalized_value, normalized_ref_low, normalized_ref_high = _apply_rescaling_rule(
            value=value,
            reference_low=ref_low,
            reference_high=ref_high,
            original_unit=original_unit,
            canonical_unit=canonical_unit,
            rule_map=rule_map,
        )

        model = Measurement(
            lab_file_id=lab.id,
            measurement_type=measurement_types[canonical_name],
            canonical_value=normalized_value,
            original_value=value,
            qualitative_value=qualitative_value,
            original_unit=original_unit,
            canonical_reference_low=normalized_ref_low,
            canonical_reference_high=normalized_ref_high,
            original_reference_low=ref_low,
            original_reference_high=ref_high,
            measured_at=measured_at or lab.lab_date,
            page_number=int(measurement["page_number"]) if measurement.get("page_number") is not None else None,
        )
        db.add(model)
        new_measurements.append(model)

    await db.flush()
    return new_measurements


async def extract_ocr_result(lab: LabFile) -> dict:
    file_path = Path(settings.UPLOAD_DIR) / lab.filepath
    resolved_path = str(file_path.resolve())
    started_at = time.perf_counter()
    logger.info(
        "OCR extraction start file_id=%s filename=%s path=%s mime_type=%s",
        lab.id,
        lab.filename,
        resolved_path,
        lab.mime_type,
    )
    result = await ocr_extract(resolved_path, filename=lab.filename)
    logger.info(
        "OCR extraction finished file_id=%s filename=%s duration=%.2fs measurements=%s",
        lab.id,
        lab.filename,
        time.perf_counter() - started_at,
        len(result.get("measurements", [])),
    )
    return result


async def persist_ocr_result(lab: LabFile, result: dict, db: AsyncSession) -> list[Measurement]:
    started_at = time.perf_counter()
    logger.info("Persist OCR result start file_id=%s filename=%s", lab.id, lab.filename)
    existing = await db.execute(select(Measurement).where(Measurement.lab_file_id == lab.id))
    for measurement in existing.scalars().all():
        await db.delete(measurement)
    lab.ocr_raw = None
    lab.ocr_text_raw = None
    lab.ocr_text_english = None
    lab.ocr_summary_english = None
    await db.flush()

    new_measurements = await apply_ocr_result(lab, result, db)
    await db.flush()
    await search_service.refresh_lab_search_document(lab.id, db)
    logger.info(
        "Persist OCR result finished file_id=%s filename=%s duration=%.2fs saved_measurements=%s",
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
) -> list[int]:
    async with _ocr_persist_lock:
        async with session_factory() as session:
            persistent_lab = await session.get(LabFile, file_id)
            if persistent_lab is None:
                raise HTTPException(404, f"File {file_id} not found")

            new_measurements = await persist_ocr_result(persistent_lab, result, session)
            measurement_ids = [measurement.id for measurement in new_measurements]
            await session.commit()
            return measurement_ids


async def run_ocr_for_file(lab: LabFile, db: AsyncSession) -> list[Measurement]:
    result = await extract_ocr_result(lab)
    return await persist_ocr_result(lab, result, db)


def progress_payload(
    *,
    lab: LabFile,
    index: int,
    total: int,
    status: str,
    error: str | None = None,
) -> str:
    payload = {
        "type": "progress",
        "file_id": lab.id,
        "filename": lab.filename,
        "index": index,
        "total": total,
        "status": status,
    }
    if error is not None:
        payload["error"] = error
    return json.dumps(payload) + "\n"


def keepalive_payload() -> str:
    return json.dumps({"type": "keepalive"}) + "\n"


def _make_progress(*, lab: LabFile, index: int, total: int, status: str, error: str | None = None) -> OcrJobProgress:
    return OcrJobProgress(
        file_id=lab.id,
        filename=lab.filename,
        index=index,
        total=total,
        status=status,
        error=error,
    )


async def _load_measurement_types_by_name(db: AsyncSession, names: list[str]) -> dict[str, MeasurementType]:
    unique_names = list(dict.fromkeys(name for name in names if name))
    if not unique_names:
        return {}

    result = await db.execute(
        select(MeasurementType)
        .where(MeasurementType.name.in_(unique_names))
        .order_by(MeasurementType.id.asc())
    )
    return {measurement_type.name: measurement_type for measurement_type in result.scalars().all()}


def _rescaling_request_id(original_unit: str, canonical_unit: str) -> str:
    original_key = normalize_unit_key(original_unit) or original_unit.strip()
    canonical_key = normalize_unit_key(canonical_unit) or canonical_unit.strip()
    return f"{original_key}=>{canonical_key}"


async def _resolve_rescaling_rule_map(
    db: AsyncSession,
    conversion_requests: list[dict],
    measurement_types: dict[str, MeasurementType],
) -> dict[tuple[str, str], RescalingRule]:
    if not conversion_requests:
        return {}

    requested_pairs = [(request["original_unit"], request["canonical_unit"]) for request in conversion_requests]
    rule_map = await load_rescaling_rules(db, requested_pairs)

    missing_requests = []
    for request in conversion_requests:
        original_key = normalize_unit_key(request["original_unit"])
        canonical_key = normalize_unit_key(request["canonical_unit"])
        if original_key is None or canonical_key is None:
            continue
        if (original_key, canonical_key) not in rule_map:
            missing_requests.append(request)

    if missing_requests:
        try:
            inferred_factors = await infer_rescaling_factors(missing_requests)
        except Exception:
            inferred_factors = {}

        for request in missing_requests:
            scale_factor = inferred_factors.get(request["id"])
            if scale_factor is None:
                continue
            await upsert_rescaling_rule(
                db,
                original_unit=request["original_unit"],
                canonical_unit=request["canonical_unit"],
                scale_factor=scale_factor,
                measurement_type=measurement_types.get(request["marker_name"]),
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


def _job_status_payload(job: OcrJobState) -> dict:
    progress = [
        asdict(item)
        for item in sorted(job.progress_by_file.values(), key=lambda current: current.index)
    ]
    return {
        "job_id": job.job_id,
        "status": job.status,
        "total": job.total,
        "completed_count": job.completed_count,
        "error_count": job.error_count,
        "last_updated_at": job.last_updated_at,
        "progress": progress,
    }


def get_ocr_job_status(job_id: str) -> dict:
    _prune_ocr_jobs()
    job = _ocr_jobs.get(job_id)
    if job is None:
        raise HTTPException(404, "OCR job not found")
    return _job_status_payload(job)


async def _run_ocr_job(job: OcrJobState, labs: list[LabFile], session_factory: async_sessionmaker[AsyncSession]) -> None:
    stream_started_at = time.perf_counter()
    total = len(labs)
    job.status = "running"
    job.started_at = time.time()
    _touch_job(job, now=job.started_at)
    logger.info("Starting OCR job job_id=%s total_files=%s file_ids=%s", job.job_id, total, [lab.id for lab in labs])

    for index, lab in enumerate(labs):
        job.progress_by_file[lab.id] = _make_progress(
            lab=lab,
            index=index,
            total=total,
            status="processing",
        )
    _touch_job(job)

    semaphore = asyncio.Semaphore(MAX_OCR_CONCURRENCY)
    completed_extractions: dict[int, tuple[LabFile, dict | None, Exception | None]] = {}
    next_index_to_persist = 0

    async def extract_one(index: int, lab: LabFile):
        async with semaphore:
            started_at = time.perf_counter()
            logger.info(
                "OCR worker acquired job_id=%s file_id=%s filename=%s queue_index=%s/%s",
                job.job_id,
                lab.id,
                lab.filename,
                index + 1,
                total,
            )
            try:
                result = await extract_ocr_result(lab)
                logger.info(
                    "OCR worker completed extraction job_id=%s file_id=%s filename=%s duration=%.2fs",
                    job.job_id,
                    lab.id,
                    lab.filename,
                    time.perf_counter() - started_at,
                )
                return index, lab, result, None
            except Exception as exc:
                logger.exception(
                    "OCR extraction failed for job_id=%s file id=%s filename=%r path=%r",
                    job.job_id,
                    lab.id,
                    lab.filename,
                    lab.filepath,
                )
                return index, lab, None, exc

    tasks = [asyncio.create_task(extract_one(index, lab)) for index, lab in enumerate(labs)]

    try:
        for future in asyncio.as_completed(tasks):
            index, lab, result, error = await future
            completed_extractions[index] = (lab, result, error)

            while next_index_to_persist in completed_extractions:
                pending_lab, pending_result, pending_error = completed_extractions.pop(next_index_to_persist)

                if pending_error:
                    logger.warning(
                        "OCR job extraction error job_id=%s file_id=%s filename=%s queue_index=%s/%s error=%s",
                        job.job_id,
                        pending_lab.id,
                        pending_lab.filename,
                        next_index_to_persist + 1,
                        total,
                        pending_error,
                    )
                    job.progress_by_file[pending_lab.id] = _make_progress(
                        lab=pending_lab,
                        index=next_index_to_persist,
                        total=total,
                        status="error",
                        error=str(pending_error),
                    )
                    job.error_count += 1
                    _touch_job(job)
                    next_index_to_persist += 1
                    continue

                try:
                    assert pending_result is not None
                    await persist_ocr_result_with_fresh_session(pending_lab.id, pending_result, session_factory)
                    logger.info(
                        "OCR job file complete job_id=%s file_id=%s filename=%s queue_index=%s/%s",
                        job.job_id,
                        pending_lab.id,
                        pending_lab.filename,
                        next_index_to_persist + 1,
                        total,
                    )
                    job.progress_by_file[pending_lab.id] = _make_progress(
                        lab=pending_lab,
                        index=next_index_to_persist,
                        total=total,
                        status="done",
                    )
                    job.completed_count += 1
                    _touch_job(job)
                except Exception as exc:
                    logger.exception(
                        "Persisting OCR result failed for job_id=%s file id=%s filename=%r path=%r",
                        job.job_id,
                        pending_lab.id,
                        pending_lab.filename,
                        pending_lab.filepath,
                    )
                    job.progress_by_file[pending_lab.id] = _make_progress(
                        lab=pending_lab,
                        index=next_index_to_persist,
                        total=total,
                        status="error",
                        error=str(exc),
                    )
                    job.error_count += 1
                    _touch_job(job)

                next_index_to_persist += 1

        job.status = "completed"
        job.finished_at = time.time()
        _touch_job(job, now=job.finished_at)
        logger.info(
            "OCR job complete job_id=%s total_files=%s completed=%s errors=%s duration=%.2fs",
            job.job_id,
            total,
            job.completed_count,
            job.error_count,
            time.perf_counter() - stream_started_at,
        )
    except Exception:
        job.status = "failed"
        job.finished_at = time.time()
        _touch_job(job, now=job.finished_at)
        logger.exception("OCR job failed job_id=%s", job.job_id)
        raise


def start_ocr_job(labs: list[LabFile], session_factory: async_sessionmaker[AsyncSession]) -> dict:
    _prune_ocr_jobs()
    job_id = uuid.uuid4().hex
    job = OcrJobState(job_id=job_id, status="queued", total=len(labs))
    _ocr_jobs[job_id] = job

    if not labs:
        job.status = "completed"
        job.started_at = time.time()
        job.finished_at = time.time()
        _touch_job(job, now=job.finished_at)
        return _job_status_payload(job)

    job.task = asyncio.create_task(_run_ocr_job(job, labs, session_factory))
    return _job_status_payload(job)


async def stream_ocr_for_labs(labs: list[LabFile], db: AsyncSession):
    if not labs:
        yield json.dumps({"type": "complete"}) + "\n"
        return

    stream_started_at = time.perf_counter()
    total = len(labs)
    logger.info("Starting OCR stream total_files=%s file_ids=%s", total, [lab.id for lab in labs])
    for index, lab in enumerate(labs):
        yield progress_payload(lab=lab, index=index, total=total, status="processing")

    semaphore = asyncio.Semaphore(MAX_OCR_CONCURRENCY)

    async def extract_one(index: int, lab: LabFile):
        async with semaphore:
            started_at = time.perf_counter()
            logger.info(
                "OCR worker acquired file_id=%s filename=%s queue_index=%s/%s",
                lab.id,
                lab.filename,
                index + 1,
                total,
            )
            try:
                result = await extract_ocr_result(lab)
                logger.info(
                    "OCR worker completed extraction file_id=%s filename=%s duration=%.2fs",
                    lab.id,
                    lab.filename,
                    time.perf_counter() - started_at,
                )
                return index, lab, result, None
            except Exception as exc:
                logger.exception(
                    "OCR extraction failed for file id=%s filename=%r path=%r",
                    lab.id,
                    lab.filename,
                    lab.filepath,
                )
                return index, lab, None, exc

    tasks = [asyncio.create_task(extract_one(index, lab)) for index, lab in enumerate(labs)]

    pending = set(tasks)
    while pending:
        done, pending = await asyncio.wait(
            pending,
            timeout=OCR_STREAM_KEEPALIVE_INTERVAL,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done:
            logger.info(
                "OCR stream keepalive pending=%s elapsed=%.2fs",
                len(pending),
                time.perf_counter() - stream_started_at,
            )
            yield keepalive_payload()
            continue

        for future in done:
            index, lab, result, error = await future
            if error:
                logger.warning(
                    "OCR stream extraction error file_id=%s filename=%s queue_index=%s/%s error=%s",
                    lab.id,
                    lab.filename,
                    index + 1,
                    total,
                    error,
                )
                yield progress_payload(lab=lab, index=index, total=total, status="error", error=str(error))
                continue

            try:
                assert result is not None
                await persist_ocr_result(lab, result, db)
                await db.commit()
                logger.info(
                    "OCR stream file complete file_id=%s filename=%s queue_index=%s/%s",
                    lab.id,
                    lab.filename,
                    index + 1,
                    total,
                )
                yield progress_payload(lab=lab, index=index, total=total, status="done")
            except Exception as exc:
                await db.rollback()
                logger.exception(
                    "Persisting OCR result failed for file id=%s filename=%r path=%r",
                    lab.id,
                    lab.filename,
                    lab.filepath,
                )
                yield progress_payload(lab=lab, index=index, total=total, status="error", error=str(exc))

    logger.info("OCR stream complete total_files=%s duration=%.2fs", total, time.perf_counter() - stream_started_at)
    yield json.dumps({"type": "complete"}) + "\n"


async def load_labs_for_ocr(
    db: AsyncSession,
    *,
    file_ids: list[int] | None = None,
    only_unprocessed: bool = False,
) -> list[LabFile]:
    if file_ids is not None:
        labs: list[LabFile] = []
        seen_file_ids: set[int] = set()
        for file_id in file_ids:
            if file_id in seen_file_ids:
                continue
            seen_file_ids.add(file_id)
            lab = await db.get(LabFile, file_id)
            if not lab:
                raise HTTPException(404, f"File {file_id} not found")
            labs.append(lab)
        return labs

    if only_unprocessed:
        result = await db.execute(select(LabFile).where(LabFile.ocr_raw.is_(None)).order_by(LabFile.id.asc()))
        return list(result.scalars().all())

    return []
