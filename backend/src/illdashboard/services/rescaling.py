from __future__ import annotations

import re
from collections.abc import Sequence

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.models import Measurement, MeasurementType, RescalingRule


def normalize_unit_key(unit: str | None) -> str | None:
    if unit is None:
        return None

    normalized = unit.strip()
    if not normalized:
        return None

    normalized = re.sub(r"(?<=\d),(?=\d)", ".", normalized)
    normalized = re.sub(r"\s+", "", normalized)
    normalized = normalized.casefold()
    normalized = normalized.replace("μ", "u")
    normalized = normalized.replace("µ", "u")
    return normalized or None


def units_equivalent(left: str | None, right: str | None) -> bool:
    left_key = normalize_unit_key(left)
    right_key = normalize_unit_key(right)
    return left_key is not None and left_key == right_key


def apply_scale_factor(value: float | None, scale_factor: float | None) -> float | None:
    if value is None or scale_factor is None:
        return value
    return value * scale_factor


async def load_rescaling_rules(
    db: AsyncSession,
    requests: list[tuple[int, str, str]],
) -> dict[tuple[int, str, str], RescalingRule]:
    normalized_requests = list(
        dict.fromkeys(
            (
                measurement_type_id,
                original_key,
                canonical_key,
            )
            for measurement_type_id, original_unit, canonical_unit in requests
            if measurement_type_id is not None
            if (original_key := normalize_unit_key(original_unit)) is not None
            if (canonical_key := normalize_unit_key(canonical_unit)) is not None
        )
    )
    if not normalized_requests:
        return {}

    filters = [
        and_(
            RescalingRule.measurement_type_id == measurement_type_id,
            RescalingRule.normalized_original_unit == original_key,
            RescalingRule.normalized_canonical_unit == canonical_key,
        )
        for measurement_type_id, original_key, canonical_key in normalized_requests
    ]
    result = await db.execute(
        select(RescalingRule).options(selectinload(RescalingRule.measurement_type)).where(or_(*filters))
    )
    return {
        (
            rule.measurement_type_id,
            rule.normalized_original_unit,
            rule.normalized_canonical_unit,
        ): rule
        for rule in result.scalars().all()
    }


async def upsert_rescaling_rules(
    db: AsyncSession,
    entries: list[dict],
) -> dict[tuple[int, str, str], RescalingRule]:
    normalized_entries: dict[tuple[int, str, str], dict] = {}
    for entry in entries:
        measurement_type = entry.get("measurement_type")
        measurement_type_id = getattr(measurement_type, "id", measurement_type)
        if not isinstance(measurement_type_id, int):
            continue

        original_unit = entry.get("original_unit")
        canonical_unit = entry.get("canonical_unit")
        original_key = normalize_unit_key(original_unit)
        canonical_key = normalize_unit_key(canonical_unit)
        if original_key is None or canonical_key is None:
            continue

        normalized_entries[(measurement_type_id, original_key, canonical_key)] = {
            "measurement_type_id": measurement_type_id,
            "measurement_type": measurement_type,
            "original_unit": str(original_unit).strip(),
            "canonical_unit": str(canonical_unit).strip(),
            "scale_factor": entry.get("scale_factor"),
        }

    if not normalized_entries:
        return {}

    existing_rules = await load_rescaling_rules(
        db,
        [
            (measurement_type_id, entry["original_unit"], entry["canonical_unit"])
            for (measurement_type_id, _, _), entry in normalized_entries.items()
        ],
    )

    for key, entry in normalized_entries.items():
        rule = existing_rules.get(key)
        if rule is None:
            rule = RescalingRule(
                measurement_type_id=entry["measurement_type_id"],
                original_unit=entry["original_unit"],
                canonical_unit=entry["canonical_unit"],
                scale_factor=entry["scale_factor"],
                normalized_original_unit=key[1],
                normalized_canonical_unit=key[2],
            )
            if isinstance(entry["measurement_type"], MeasurementType):
                rule.measurement_type = entry["measurement_type"]
            db.add(rule)
            existing_rules[key] = rule
            continue

        rule.original_unit = entry["original_unit"]
        rule.canonical_unit = entry["canonical_unit"]
        rule.scale_factor = entry["scale_factor"]
        if isinstance(entry["measurement_type"], MeasurementType):
            rule.measurement_type = entry["measurement_type"]

    await db.flush()
    return existing_rules


async def upsert_rescaling_rule(
    db: AsyncSession,
    *,
    original_unit: str,
    canonical_unit: str,
    scale_factor: float | None,
    measurement_type: MeasurementType,
) -> RescalingRule | None:
    rules = await upsert_rescaling_rules(
        db,
        [
            {
                "measurement_type": measurement_type,
                "original_unit": original_unit,
                "canonical_unit": canonical_unit,
                "scale_factor": scale_factor,
            }
        ],
    )
    original_key = normalize_unit_key(original_unit)
    canonical_key = normalize_unit_key(canonical_unit)
    if original_key is None or canonical_key is None:
        return None
    return rules.get((measurement_type.id, original_key, canonical_key))


async def missing_rescaling_measurement_ids(
    db: AsyncSession,
    measurements: Sequence[Measurement],
) -> set[int]:
    requested_rules: list[tuple[int, str, str]] = []
    requested_pairs_by_measurement_id: dict[int, tuple[int, str, str]] = {}

    for measurement in measurements:
        if measurement.id is None or measurement.original_value is None or measurement.measurement_type_id is None:
            continue
        if measurement.original_unit is None or measurement.canonical_unit is None:
            continue
        if units_equivalent(measurement.original_unit, measurement.canonical_unit):
            continue

        requested_rules.append((measurement.measurement_type_id, measurement.original_unit, measurement.canonical_unit))
        requested_pairs_by_measurement_id[measurement.id] = (
            measurement.measurement_type_id,
            measurement.original_unit,
            measurement.canonical_unit,
        )

    if not requested_pairs_by_measurement_id:
        return set()

    rule_map = await load_rescaling_rules(db, requested_rules)
    missing_ids: set[int] = set()
    for measurement_id, (
        measurement_type_id,
        original_unit,
        canonical_unit,
    ) in requested_pairs_by_measurement_id.items():
        original_key = normalize_unit_key(original_unit)
        canonical_key = normalize_unit_key(canonical_unit)
        if original_key is None or canonical_key is None:
            missing_ids.add(measurement_id)
            continue

        rule = rule_map.get((measurement_type_id, original_key, canonical_key))
        if rule is None or rule.scale_factor is None:
            missing_ids.add(measurement_id)

    return missing_ids


async def annotate_missing_rescaling_measurements(
    db: AsyncSession,
    measurements: Sequence[Measurement],
) -> set[int]:
    missing_ids = await missing_rescaling_measurement_ids(db, measurements)
    for measurement in measurements:
        setattr(measurement, "unit_conversion_missing", measurement.id in missing_ids)
    return missing_ids
