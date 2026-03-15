"""OCR processing, marker normalization, and OCR streaming helpers."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import unicodedata
from datetime import datetime
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from illdashboard.config import settings
from illdashboard.copilot_service import normalize_marker_names, normalize_source_name, ocr_extract
from illdashboard.models import LabFile, LabFileTag, Measurement, MeasurementType
from illdashboard.services.markers import (
    build_source_tag,
    classify_marker_group,
    ensure_measurement_types,
    get_measurement_type_by_name,
    is_source_tag,
    merge_measurement_types,
    normalize_source_tag_value,
    source_tag_value,
)


logger = logging.getLogger(__name__)

MAX_OCR_CONCURRENCY = 4

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

    for tag in existing_tags:
        if is_source_tag(tag.tag):
            await db.delete(tag)

    if not source_value:
        await db.flush()
        return

    source_tag = build_source_tag(source_value)
    if source_tag is None:
        await db.flush()
        return

    db.add(LabFileTag(lab_file_id=lab.id, tag=source_tag))
    await db.flush()


async def apply_ocr_result(lab: LabFile, result: dict, db: AsyncSession) -> list[Measurement]:
    lab.ocr_raw = json.dumps(result)
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

    existing_result = await db.execute(select(MeasurementType.name).order_by(MeasurementType.name))
    existing_canonical = list(existing_result.scalars().all())

    try:
        llm_map = await normalize_marker_names(cleaned_names, existing_canonical)
    except Exception:
        llm_map = {name: name for name in cleaned_names}

    canonical_map = {raw: llm_map.get(deterministic_map[raw], deterministic_map[raw]) for raw in raw_names}
    measurement_types = await ensure_measurement_types(db, [canonical_map[raw] for raw in raw_names])

    new_measurements: list[Measurement] = []
    for measurement in result.get("measurements", []):
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

        measured_at = None
        if measurement.get("measured_at"):
            try:
                measured_at = datetime.fromisoformat(measurement["measured_at"])
            except (ValueError, TypeError):
                measured_at = lab.lab_date

        canonical_name = canonical_map.get(measurement["marker_name"], measurement["marker_name"])
        if canonical_name is None:
            continue
        model = Measurement(
            lab_file_id=lab.id,
            measurement_type=measurement_types[canonical_name],
            value=value,
            qualitative_value=qualitative_value,
            unit=measurement.get("unit"),
            reference_low=ref_low,
            reference_high=ref_high,
            measured_at=measured_at or lab.lab_date,
            page_number=int(measurement["page_number"]) if measurement.get("page_number") is not None else None,
        )
        db.add(model)
        new_measurements.append(model)

    await db.flush()
    return new_measurements


async def extract_ocr_result(lab: LabFile) -> dict:
    file_path = Path(settings.UPLOAD_DIR) / lab.filepath
    return await ocr_extract(str(file_path.resolve()), filename=lab.filename)


async def persist_ocr_result(lab: LabFile, result: dict, db: AsyncSession) -> list[Measurement]:
    existing = await db.execute(select(Measurement).where(Measurement.lab_file_id == lab.id))
    for measurement in existing.scalars().all():
        await db.delete(measurement)
    lab.ocr_raw = None
    await db.flush()

    new_measurements = await apply_ocr_result(lab, result, db)
    await db.flush()
    return new_measurements


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


async def stream_ocr_for_labs(labs: list[LabFile], db: AsyncSession):
    if not labs:
        yield json.dumps({"type": "complete"}) + "\n"
        return

    total = len(labs)
    for index, lab in enumerate(labs):
        yield progress_payload(lab=lab, index=index, total=total, status="processing")

    semaphore = asyncio.Semaphore(MAX_OCR_CONCURRENCY)

    async def extract_one(index: int, lab: LabFile):
        async with semaphore:
            try:
                result = await extract_ocr_result(lab)
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

    for future in asyncio.as_completed(tasks):
        index, lab, result, error = await future
        if error:
            yield progress_payload(lab=lab, index=index, total=total, status="error", error=str(error))
            continue

        try:
            assert result is not None
            await persist_ocr_result(lab, result, db)
            await db.commit()
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
        result = await db.execute(select(LabFile).where(LabFile.ocr_raw.is_(None)))
        return list(result.scalars().all())

    return []


async def normalize_existing_measurements(db: AsyncSession) -> int:
    result = await db.execute(select(MeasurementType).order_by(MeasurementType.id.asc()))
    measurement_types = result.scalars().all()

    deterministic_map = {
        measurement_type.name: normalize_marker_name_deterministic(measurement_type.name)
        for measurement_type in measurement_types
    }
    cleaned_names = list(dict.fromkeys(deterministic_map.values()))

    try:
        llm_map = await normalize_marker_names(
            cleaned_names,
            [measurement_type.name for measurement_type in measurement_types],
        )
    except Exception:
        llm_map = {name: name for name in cleaned_names}

    canonical_map = {
        raw_name: llm_map.get(deterministic_map[raw_name], deterministic_map[raw_name])
        for raw_name in deterministic_map
    }
    type_by_name = {measurement_type.name: measurement_type for measurement_type in measurement_types}
    updated = 0

    await ensure_measurement_types(db, list(canonical_map.values()))

    for raw_name, canonical_name in canonical_map.items():
        measurement_type = type_by_name[raw_name]
        if canonical_name == raw_name:
            expected_group = classify_marker_group(canonical_name)
            if measurement_type.group_name != expected_group:
                measurement_type.group_name = expected_group
                updated += 1
            continue

        target = await get_measurement_type_by_name(db, canonical_name)
        if target is None:
            measurement_type.name = canonical_name
            measurement_type.group_name = classify_marker_group(canonical_name)
            updated += 1
            type_by_name[canonical_name] = measurement_type
            continue

        await merge_measurement_types(measurement_type, target, db)
        updated += 1

    await db.commit()
    return updated
