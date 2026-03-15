from __future__ import annotations

import re

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.models import MeasurementType, RescalingRule


def _normalized_unit_pairs(unit_pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    return list(
        dict.fromkeys(
            (
                original_key,
                canonical_key,
            )
            for original_unit, canonical_unit in unit_pairs
            if (original_key := normalize_unit_key(original_unit)) is not None
            if (canonical_key := normalize_unit_key(canonical_unit)) is not None
        )
    )


def normalize_unit_key(unit: str | None) -> str | None:
    if unit is None:
        return None

    normalized = unit.strip()
    if not normalized:
        return None

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
    unit_pairs: list[tuple[str, str]],
) -> dict[tuple[str, str], RescalingRule]:
    normalized_pairs = _normalized_unit_pairs(unit_pairs)
    if not normalized_pairs:
        return {}

    pair_filters = [
        and_(
            RescalingRule.normalized_original_unit == original_key,
            RescalingRule.normalized_canonical_unit == canonical_key,
        )
        for original_key, canonical_key in normalized_pairs
    ]

    result = await db.execute(
        select(RescalingRule)
        .options(selectinload(RescalingRule.measurement_type))
        .where(or_(*pair_filters))
    )
    return {
        (rule.normalized_original_unit, rule.normalized_canonical_unit): rule
        for rule in result.scalars().all()
    }


async def upsert_rescaling_rules(
    db: AsyncSession,
    entries: list[dict],
) -> dict[tuple[str, str], RescalingRule]:
    normalized_entries: dict[tuple[str, str], dict] = {}
    for entry in entries:
        original_unit = entry.get("original_unit")
        canonical_unit = entry.get("canonical_unit")
        original_key = normalize_unit_key(original_unit)
        canonical_key = normalize_unit_key(canonical_unit)
        if original_key is None or canonical_key is None:
            continue
        normalized_entries[(original_key, canonical_key)] = {
            "original_unit": str(original_unit).strip(),
            "canonical_unit": str(canonical_unit).strip(),
            "scale_factor": entry.get("scale_factor"),
            "measurement_type": entry.get("measurement_type"),
        }

    if not normalized_entries:
        return {}

    existing_rules = await load_rescaling_rules(
        db,
        [(entry["original_unit"], entry["canonical_unit"]) for entry in normalized_entries.values()],
    )

    for normalized_pair, entry in normalized_entries.items():
        rule = existing_rules.get(normalized_pair)
        if rule is None:
            rule = RescalingRule(
                original_unit=entry["original_unit"],
                canonical_unit=entry["canonical_unit"],
                scale_factor=entry["scale_factor"],
                normalized_original_unit=normalized_pair[0],
                normalized_canonical_unit=normalized_pair[1],
                measurement_type=entry["measurement_type"],
            )
            db.add(rule)
            existing_rules[normalized_pair] = rule
            continue

        rule.original_unit = entry["original_unit"]
        rule.canonical_unit = entry["canonical_unit"]
        rule.scale_factor = entry["scale_factor"]
        if entry["measurement_type"] is not None:
            rule.measurement_type = entry["measurement_type"]

    await db.flush()
    return existing_rules


async def upsert_rescaling_rule(
    db: AsyncSession,
    *,
    original_unit: str,
    canonical_unit: str,
    scale_factor: float | None,
    measurement_type: MeasurementType | None = None,
) -> RescalingRule | None:
    rules = await upsert_rescaling_rules(
        db,
        [
            {
                "original_unit": original_unit,
                "canonical_unit": canonical_unit,
                "scale_factor": scale_factor,
                "measurement_type": measurement_type,
            }
        ],
    )
    normalized_original_unit = normalize_unit_key(original_unit)
    normalized_canonical_unit = normalize_unit_key(canonical_unit)
    if normalized_original_unit is None or normalized_canonical_unit is None:
        return None
    return rules.get((normalized_original_unit, normalized_canonical_unit))