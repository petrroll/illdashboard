"""OCR processing, marker normalization, and OCR streaming helpers."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from illdashboard.config import settings
from illdashboard.copilot_service import normalize_marker_names, ocr_extract
from illdashboard.models import LabFile, Measurement, MeasurementType
from illdashboard.services.markers import (
    classify_marker_group,
    ensure_measurement_types,
    get_measurement_type_by_name,
    merge_measurement_types,
)


logger = logging.getLogger(__name__)

MAX_OCR_CONCURRENCY = 4


def parse_numeric_value(raw) -> float | None:
    if raw is None:
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


def normalize_marker_name_deterministic(name: str) -> str:
    name = re.sub(r"(?<!\s)\[", " [", name)
    name = re.sub(r"\s+-\s*|\s*-\s+", " - ", name)
    name = re.sub(r"  +", " ", name)
    return name.strip()


async def apply_ocr_result(lab: LabFile, result: dict, db: AsyncSession) -> list[Measurement]:
    lab_record = cast(Any, lab)
    lab_record.ocr_raw = json.dumps(result)
    if result.get("lab_date"):
        try:
            lab_record.lab_date = datetime.fromisoformat(result["lab_date"])
        except (ValueError, TypeError):
            pass

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
        value = parse_numeric_value(measurement.get("value"))
        if value is None:
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
    return await ocr_extract(str(file_path.resolve()))


async def persist_ocr_result(lab: LabFile, result: dict, db: AsyncSession) -> list[Measurement]:
    existing = await db.execute(select(Measurement).where(Measurement.lab_file_id == lab.id))
    for measurement in existing.scalars().all():
        await db.delete(measurement)
    cast(Any, lab).ocr_raw = None
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
        cast(str, measurement_type.name): normalize_marker_name_deterministic(cast(str, measurement_type.name))
        for measurement_type in measurement_types
    }
    cleaned_names = list(dict.fromkeys(deterministic_map.values()))

    try:
        llm_map = await normalize_marker_names(
            cleaned_names,
            [cast(str, measurement_type.name) for measurement_type in measurement_types],
        )
    except Exception:
        llm_map = {name: name for name in cleaned_names}

    canonical_map = {
        raw_name: llm_map.get(deterministic_map[raw_name], deterministic_map[raw_name])
        for raw_name in deterministic_map
    }
    type_by_name = {cast(str, measurement_type.name): measurement_type for measurement_type in measurement_types}
    updated = 0

    await ensure_measurement_types(db, list(canonical_map.values()))

    for raw_name, canonical_name in canonical_map.items():
        measurement_type = type_by_name[raw_name]
        if canonical_name == raw_name:
            expected_group = classify_marker_group(canonical_name)
            if cast(str, measurement_type.group_name) != expected_group:
                cast(Any, measurement_type).group_name = expected_group
                updated += 1
            continue

        target = await get_measurement_type_by_name(db, canonical_name)
        if target is None:
            measurement_type_record = cast(Any, measurement_type)
            measurement_type_record.name = canonical_name
            measurement_type_record.group_name = classify_marker_group(canonical_name)
            updated += 1
            type_by_name[canonical_name] = measurement_type
            continue

        await merge_measurement_types(measurement_type, target, db)
        updated += 1

    await db.commit()
    return updated
