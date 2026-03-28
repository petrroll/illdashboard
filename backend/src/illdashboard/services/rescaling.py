from __future__ import annotations

import math
import re
from collections.abc import Sequence
from dataclasses import dataclass

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


@dataclass(frozen=True)
class MeasurementHistoryEnvelope:
    measurement_type_id: int
    value_count: int
    file_count: int
    value_min: float | None
    value_max: float | None
    reference_low_count: int
    reference_low_min: float | None
    reference_low_max: float | None
    reference_high_count: int
    reference_high_min: float | None
    reference_high_max: float | None


def _finite_number(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return value


def positive_ratio(min_value: float | None, max_value: float | None) -> float | None:
    left = _finite_number(min_value)
    right = _finite_number(max_value)
    if left is None or right is None or left <= 0 or right <= 0:
        return None
    return max(left, right) / min(left, right)


def value_outside_order_of_magnitude(
    value: float | None,
    min_value: float | None,
    max_value: float | None,
    *,
    factor: float = 10.0,
) -> bool:
    numeric_value = _finite_number(value)
    envelope_min = _finite_number(min_value)
    envelope_max = _finite_number(max_value)
    if (
        numeric_value is None
        or envelope_min is None
        or envelope_max is None
        or numeric_value <= 0
        or envelope_min <= 0
        or envelope_max <= 0
    ):
        return False
    return numeric_value < envelope_min / factor or numeric_value > envelope_max * factor


def value_within_envelope(
    value: float | None,
    min_value: float | None,
    max_value: float | None,
    *,
    margin_ratio: float = 0.1,
    absolute_margin: float = 1e-9,
) -> bool:
    numeric_value = _finite_number(value)
    envelope_min = _finite_number(min_value)
    envelope_max = _finite_number(max_value)
    if numeric_value is None or envelope_min is None or envelope_max is None:
        return False

    lower_bound = min(envelope_min, envelope_max)
    upper_bound = max(envelope_min, envelope_max)
    lower_bound -= max(abs(lower_bound) * margin_ratio, absolute_margin)
    upper_bound += max(abs(upper_bound) * margin_ratio, absolute_margin)
    return lower_bound <= numeric_value <= upper_bound


async def load_measurement_history_envelopes(
    db: AsyncSession,
    measurement_type_ids: Sequence[int],
    *,
    exclude_file_id: int | None = None,
) -> dict[int, MeasurementHistoryEnvelope]:
    # The anomaly gate reasons over already-normalized history so it can compare a
    # new provisional canonical value/range against the stable marker envelope.
    type_ids = [
        measurement_type_id
        for measurement_type_id in dict.fromkeys(measurement_type_ids)
        if measurement_type_id
    ]
    if not type_ids:
        return {}

    query = (
        select(
            Measurement.measurement_type_id,
            Measurement.lab_file_id,
            Measurement.canonical_value,
            Measurement.canonical_reference_low,
            Measurement.canonical_reference_high,
        )
        .where(
            Measurement.measurement_type_id.in_(type_ids),
            Measurement.normalization_status == "resolved",
            Measurement.canonical_value.is_not(None),
        )
        .order_by(Measurement.measurement_type_id.asc(), Measurement.id.asc())
    )
    if exclude_file_id is not None:
        query = query.where(Measurement.lab_file_id != exclude_file_id)

    result = await db.execute(query)
    envelope_data: dict[int, dict[str, object]] = {
        measurement_type_id: {
            "file_ids": set(),
            "value_count": 0,
            "value_min": None,
            "value_max": None,
            "reference_low_count": 0,
            "reference_low_min": None,
            "reference_low_max": None,
            "reference_high_count": 0,
            "reference_high_min": None,
            "reference_high_max": None,
        }
        for measurement_type_id in type_ids
    }

    for measurement_type_id, file_id, canonical_value, reference_low, reference_high in result.all():
        if measurement_type_id is None:
            continue
        entry = envelope_data.setdefault(
            measurement_type_id,
            {
                "file_ids": set(),
                "value_count": 0,
                "value_min": None,
                "value_max": None,
                "reference_low_count": 0,
                "reference_low_min": None,
                "reference_low_max": None,
                "reference_high_count": 0,
                "reference_high_min": None,
                "reference_high_max": None,
            },
        )
        file_ids = entry["file_ids"]
        if isinstance(file_ids, set):
            file_ids.add(file_id)

        numeric_value = _finite_number(canonical_value)
        if numeric_value is not None:
            entry["value_count"] = int(entry["value_count"]) + 1
            current_min = _finite_number(entry["value_min"])  # type: ignore[arg-type]
            current_max = _finite_number(entry["value_max"])  # type: ignore[arg-type]
            entry["value_min"] = numeric_value if current_min is None else min(current_min, numeric_value)
            entry["value_max"] = numeric_value if current_max is None else max(current_max, numeric_value)

        numeric_low = _finite_number(reference_low)
        if numeric_low is not None:
            entry["reference_low_count"] = int(entry["reference_low_count"]) + 1
            current_min = _finite_number(entry["reference_low_min"])  # type: ignore[arg-type]
            current_max = _finite_number(entry["reference_low_max"])  # type: ignore[arg-type]
            entry["reference_low_min"] = numeric_low if current_min is None else min(current_min, numeric_low)
            entry["reference_low_max"] = numeric_low if current_max is None else max(current_max, numeric_low)

        numeric_high = _finite_number(reference_high)
        if numeric_high is not None:
            entry["reference_high_count"] = int(entry["reference_high_count"]) + 1
            current_min = _finite_number(entry["reference_high_min"])  # type: ignore[arg-type]
            current_max = _finite_number(entry["reference_high_max"])  # type: ignore[arg-type]
            entry["reference_high_min"] = numeric_high if current_min is None else min(current_min, numeric_high)
            entry["reference_high_max"] = numeric_high if current_max is None else max(current_max, numeric_high)

    return {
        measurement_type_id: MeasurementHistoryEnvelope(
            measurement_type_id=measurement_type_id,
            value_count=int(entry["value_count"]),
            file_count=len(entry["file_ids"]) if isinstance(entry["file_ids"], set) else 0,
            value_min=_finite_number(entry["value_min"]),  # type: ignore[arg-type]
            value_max=_finite_number(entry["value_max"]),  # type: ignore[arg-type]
            reference_low_count=int(entry["reference_low_count"]),
            reference_low_min=_finite_number(entry["reference_low_min"]),  # type: ignore[arg-type]
            reference_low_max=_finite_number(entry["reference_low_max"]),  # type: ignore[arg-type]
            reference_high_count=int(entry["reference_high_count"]),
            reference_high_min=_finite_number(entry["reference_high_min"]),  # type: ignore[arg-type]
            reference_high_max=_finite_number(entry["reference_high_max"]),  # type: ignore[arg-type]
        )
        for measurement_type_id, entry in envelope_data.items()
        if int(entry["value_count"]) > 0
    }


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


async def load_rescaling_rule_guides(
    db: AsyncSession,
    requests: list[tuple[int, str, str]],
    *,
    limit_per_request: int = 3,
) -> dict[tuple[int, str, str], list[RescalingRule]]:
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

    unit_pairs = list(
        dict.fromkeys(
            (original_key, canonical_key)
            for _, original_key, canonical_key in normalized_requests
        )
    )
    filters = [
        and_(
            RescalingRule.normalized_original_unit == original_key,
            RescalingRule.normalized_canonical_unit == canonical_key,
        )
        for original_key, canonical_key in unit_pairs
    ]
    result = await db.execute(
        select(RescalingRule)
        .options(selectinload(RescalingRule.measurement_type))
        .where(or_(*filters))
        .order_by(RescalingRule.id.asc())
    )

    rules_by_pair: dict[tuple[str, str], list[RescalingRule]] = {}
    for rule in result.scalars().all():
        if rule.scale_factor is None:
            continue
        rules_by_pair.setdefault(
            (rule.normalized_original_unit, rule.normalized_canonical_unit),
            [],
        ).append(rule)

    return {
        (measurement_type_id, original_key, canonical_key): [
            rule
            for rule in rules_by_pair.get((original_key, canonical_key), [])
            if rule.measurement_type_id != measurement_type_id
        ][:limit_per_request]
        for measurement_type_id, original_key, canonical_key in normalized_requests
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
