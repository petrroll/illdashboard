from __future__ import annotations

import re

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.models import MeasurementType, RescalingRule


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
    normalized_pairs = list(
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


async def upsert_rescaling_rule(
    db: AsyncSession,
    *,
    original_unit: str,
    canonical_unit: str,
    scale_factor: float | None,
    measurement_type: MeasurementType | None = None,
) -> RescalingRule | None:
    normalized_original_unit = normalize_unit_key(original_unit)
    normalized_canonical_unit = normalize_unit_key(canonical_unit)
    if normalized_original_unit is None or normalized_canonical_unit is None:
        return None

    existing_rules = await load_rescaling_rules(db, [(original_unit, canonical_unit)])
    rule = existing_rules.get((normalized_original_unit, normalized_canonical_unit))
    if rule is None:
        rule = RescalingRule(
            original_unit=original_unit.strip(),
            canonical_unit=canonical_unit.strip(),
            scale_factor=scale_factor,
            normalized_original_unit=normalized_original_unit,
            normalized_canonical_unit=normalized_canonical_unit,
            measurement_type=measurement_type,
        )
        db.add(rule)
    else:
        rule.original_unit = original_unit.strip()
        rule.canonical_unit = canonical_unit.strip()
        rule.scale_factor = scale_factor
        if measurement_type is not None:
            rule.measurement_type = measurement_type

    await db.flush()
    return rule