"""Marker classification, tags, and measurement query helpers."""

from __future__ import annotations

import logging
import re
import unicodedata
from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.models import BiomarkerInsight, LabFile, MarkerGroup, MarkerTag, Measurement, MeasurementAlias, MeasurementType


logger = logging.getLogger(__name__)

DEFAULT_GROUP_NAME = "Other"

SEED_GROUPS: list[tuple[str, int]] = [
    ("Blood Function", 10),
    ("Iron Status", 20),
    ("Inflammation & Infection", 30),
    ("Metabolic", 40),
    ("Kidney Function", 50),
    ("Electrolytes", 60),
    ("Urinalysis", 70),
    ("Lipids", 80),
    ("Liver Function", 90),
    ("Thyroid", 100),
    ("Vitamins & Minerals", 110),
    ("Hormones", 120),
    ("Immunity & Serology", 130),
    ("Allergens", 140),
    ("Other", 1000),
]


async def ensure_marker_groups(db: AsyncSession) -> dict[str, MarkerGroup]:
    """Seed canonical marker groups if they don't exist. Return all groups by name."""
    result = await db.execute(select(MarkerGroup))
    existing = {group.name: group for group in result.scalars().all()}

    for name, display_order in SEED_GROUPS:
        if name in existing:
            if existing[name].display_order != display_order:
                existing[name].display_order = display_order
            continue
        group = MarkerGroup(name=name, display_order=display_order)
        db.add(group)
        existing[name] = group

    await db.flush()
    return existing


async def load_group_order(db: AsyncSession) -> list[str]:
    """Return group names ordered by display_order."""
    result = await db.execute(select(MarkerGroup).order_by(MarkerGroup.display_order.asc()))
    return [group.name for group in result.scalars().all()]


async def load_marker_groups(db: AsyncSession) -> dict[str, MarkerGroup]:
    """Return all marker groups keyed by name."""
    result = await db.execute(select(MarkerGroup))
    return {group.name: group for group in result.scalars().all()}


SINGLE_MEASUREMENT_TAG = "singlemeasurement"
MULTIPLE_MEASUREMENTS_TAG = "multiplemeasurements"
SOURCE_TAG_PREFIX = "source:"


def normalize_source_tag_value(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.casefold().strip()
    normalized = re.sub(r"[\s_./]+", "-", normalized)
    normalized = re.sub(r"[^a-z0-9-]+", "", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized)
    return normalized.strip("-")


def build_source_tag(value: str) -> str | None:
    normalized_value = normalize_source_tag_value(value)
    if not normalized_value:
        return None
    return f"{SOURCE_TAG_PREFIX}{normalized_value}"


def is_source_tag(tag: str) -> bool:
    return tag.casefold().startswith(SOURCE_TAG_PREFIX)


def source_tag_value(tag: str) -> str | None:
    if not is_source_tag(tag):
        return None
    _, _, value = tag.partition(":")
    normalized_value = normalize_source_tag_value(value)
    return normalized_value or None


def normalize_tag(raw_tag: str) -> str:
    tag = raw_tag.strip()
    if not tag:
        return ""

    match = re.match(r"(?i)^source\s*:\s*(.+)$", tag)
    if match:
        return build_source_tag(match.group(1)) or ""

    return tag


def normalize_unique_tags(tags: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()

    for raw_tag in tags:
        tag = normalize_tag(raw_tag)
        if not tag or tag in seen:
            continue
        seen.add(tag)
        normalized.append(tag)

    return normalized


def marker_group_tag(group_name: str) -> str:
    return f"group:{group_name}"


def derived_marker_tags(group_name: str, measurement_count: int) -> list[str]:
    derived_tags = [marker_group_tag(group_name)] if group_name else []
    if measurement_count == 1:
        derived_tags.append(SINGLE_MEASUREMENT_TAG)
    elif measurement_count > 1:
        derived_tags.append(MULTIPLE_MEASUREMENTS_TAG)
    return derived_tags


def combine_marker_tags(stored_tags: list[str], group_name: str, measurement_count: int) -> list[str]:
    return normalize_unique_tags([*stored_tags, *derived_marker_tags(group_name, measurement_count)])


async def all_reserved_marker_tags(db: AsyncSession, group_name: str | None = None) -> set[str]:
    group_names = await load_group_order(db)
    reserved_tags = {
        SINGLE_MEASUREMENT_TAG,
        MULTIPLE_MEASUREMENTS_TAG,
        *(marker_group_tag(group) for group in group_names),
    }
    if group_name:
        reserved_tags.add(marker_group_tag(group_name))
    return reserved_tags


async def load_stored_marker_tags(db: AsyncSession) -> dict[str, list[str]]:
    tag_result = await db.execute(select(MarkerTag).options(selectinload(MarkerTag.measurement_type)))
    marker_tag_map: dict[str, list[str]] = defaultdict(list)
    for marker_tag in tag_result.scalars().all():
        marker_tag_map[marker_tag.marker_name].append(marker_tag.tag)
    return marker_tag_map


def normalized_marker_key(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return normalized.casefold()


def normalize_marker_alias_key(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.casefold()
    normalized = re.sub(r"(?<!\s)\[", " [", normalized)
    normalized = re.sub(r"\s+-\s*|\s*-\s+", " ", normalized)
    normalized = normalized.replace("+", "")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


async def load_measurement_type_aliases(db: AsyncSession, names: list[str]) -> dict[str, MeasurementType]:
    unique_names = list(dict.fromkeys(name for name in names if name))
    if not unique_names:
        return {}

    keys_by_name = {name: normalize_marker_alias_key(name) for name in unique_names}
    unique_keys = list(dict.fromkeys(key for key in keys_by_name.values() if key))
    if not unique_keys:
        return {}

    result = await db.execute(
        select(MeasurementAlias)
        .options(selectinload(MeasurementAlias.measurement_type))
        .where(MeasurementAlias.normalized_key.in_(unique_keys))
    )
    alias_by_key = {alias.normalized_key: alias.measurement_type for alias in result.scalars().all()}
    return {
        name: alias_by_key[key]
        for name, key in keys_by_name.items()
        if key in alias_by_key
    }


async def ensure_measurement_type_aliases(
    db: AsyncSession,
    alias_pairs: list[tuple[str, MeasurementType]],
) -> None:
    normalized_pairs: list[tuple[str, str, MeasurementType]] = []
    seen_keys: set[str] = set()
    for alias_name, measurement_type in alias_pairs:
        cleaned_alias = alias_name.strip()
        normalized_key = normalize_marker_alias_key(cleaned_alias)
        if not cleaned_alias or not normalized_key or normalized_key in seen_keys:
            continue
        seen_keys.add(normalized_key)
        normalized_pairs.append((cleaned_alias, normalized_key, measurement_type))

    if not normalized_pairs:
        return

    result = await db.execute(
        select(MeasurementAlias).where(
            MeasurementAlias.normalized_key.in_([normalized_key for _, normalized_key, _ in normalized_pairs])
        )
    )
    existing_by_key = {alias.normalized_key: alias for alias in result.scalars().all()}

    for alias_name, normalized_key, measurement_type in normalized_pairs:
        alias = existing_by_key.get(normalized_key)
        if alias is None:
            db.add(
                MeasurementAlias(
                    alias_name=alias_name,
                    normalized_key=normalized_key,
                    measurement_type_id=measurement_type.id,
                )
            )
            continue

        if alias.alias_name != alias_name:
            alias.alias_name = alias_name
        if alias.measurement_type_id != measurement_type.id:
            alias.measurement_type_id = measurement_type.id

    await db.flush()


async def backfill_measurement_type_aliases(db: AsyncSession) -> None:
    result = await db.execute(select(MeasurementType).order_by(MeasurementType.id.asc()))
    measurement_types = result.scalars().all()
    await ensure_measurement_type_aliases(
        db,
        [(measurement_type.name, measurement_type) for measurement_type in measurement_types],
    )


async def classify_marker_groups(
    names: list[str],
    existing_groups: list[str],
) -> dict[str, str]:
    """Use the LLM to classify marker names into groups.

    Falls back to DEFAULT_GROUP_NAME when the LLM is unavailable.
    """
    if not names:
        return {}

    from illdashboard.copilot.normalization import classify_marker_groups as llm_classify

    try:
        return await llm_classify(names, existing_groups)
    except Exception:
        logger.warning("LLM marker group classification failed; defaulting to '%s'", DEFAULT_GROUP_NAME)
        return {name: DEFAULT_GROUP_NAME for name in names}


async def _resolve_marker_group_names(
    db: AsyncSession,
    names: list[str],
) -> dict[str, str]:
    """Resolve group names for a list of marker names.

    Uses existing MeasurementType assignments first, then falls back to LLM.
    """
    result = await db.execute(
        select(MeasurementType.name, MeasurementType.group_name)
        .where(MeasurementType.name.in_(names))
    )
    known = {row[0]: row[1] for row in result.all()}

    unclassified = [name for name in names if name not in known]
    if not unclassified:
        return known

    group_names = await load_group_order(db)
    llm_groups = await classify_marker_groups(unclassified, group_names)
    return {**known, **llm_groups}


async def _ensure_group_exists(db: AsyncSession, group_name: str, groups_by_name: dict[str, MarkerGroup]) -> MarkerGroup:
    """Return an existing MarkerGroup or create a new one."""
    group = groups_by_name.get(group_name)
    if group is not None:
        return group

    max_order_result = await db.execute(
        select(MarkerGroup.display_order).order_by(MarkerGroup.display_order.desc()).limit(1)
    )
    max_order = max_order_result.scalar_one_or_none() or 0
    group = MarkerGroup(name=group_name, display_order=max_order + 10)
    db.add(group)
    await db.flush()
    groups_by_name[group_name] = group
    return group


async def ensure_measurement_types(db: AsyncSession, names: list[str]) -> dict[str, MeasurementType]:
    unique_names = list(dict.fromkeys(name for name in names if name))
    if not unique_names:
        return {}

    result = await db.execute(select(MeasurementType).where(MeasurementType.name.in_(unique_names)))
    by_name = {measurement_type.name: measurement_type for measurement_type in result.scalars().all()}

    new_names = [name for name in unique_names if name not in by_name]
    if new_names:
        group_assignments = await _resolve_marker_group_names(db, new_names)
        groups_by_name = await load_marker_groups(db)

        for name in new_names:
            group_name = group_assignments.get(name, DEFAULT_GROUP_NAME)
            group = await _ensure_group_exists(db, group_name, groups_by_name)
            measurement_type = MeasurementType(name=name, group_name=group_name, group_id=group.id)
            db.add(measurement_type)
            by_name[name] = measurement_type

        await db.flush()

    await ensure_measurement_type_aliases(db, [(name, by_name[name]) for name in unique_names])
    return by_name


async def get_measurement_type_by_name(db: AsyncSession, marker_name: str) -> MeasurementType | None:
    result = await db.execute(select(MeasurementType).where(MeasurementType.name == marker_name))
    measurement_type = result.scalar_one_or_none()
    if measurement_type is not None:
        return measurement_type

    aliases = await load_measurement_type_aliases(db, [marker_name])
    return aliases.get(marker_name)


async def load_measurements_for_marker(db: AsyncSession, marker_name: str) -> list[Measurement]:
    measurement_type = await get_measurement_type_by_name(db, marker_name)
    if measurement_type is None:
        return []

    result = await db.execute(
        select(Measurement)
        .options(
            selectinload(Measurement.measurement_type),
            selectinload(Measurement.lab_file).selectinload(LabFile.tags),
        )
        .where(Measurement.measurement_type_id == measurement_type.id)
        .order_by(Measurement.measured_at.asc(), Measurement.id.asc())
    )
    return list(result.scalars().all())


async def merge_measurement_types(source: MeasurementType, target: MeasurementType, db: AsyncSession) -> None:
    if source.id == target.id:
        return

    measurements_result = await db.execute(select(Measurement).where(Measurement.measurement_type_id == source.id))
    for measurement in measurements_result.scalars().all():
        measurement.measurement_type_id = target.id

    source_tags_result = await db.execute(select(MarkerTag).where(MarkerTag.measurement_type_id == source.id))
    source_tags = source_tags_result.scalars().all()

    source_alias_result = await db.execute(select(MeasurementAlias).where(MeasurementAlias.measurement_type_id == source.id))
    source_aliases = source_alias_result.scalars().all()

    target_alias_result = await db.execute(
        select(MeasurementAlias.normalized_key).where(MeasurementAlias.measurement_type_id == target.id)
    )
    existing_target_alias_keys = set(target_alias_result.scalars().all())

    target_tags_result = await db.execute(select(MarkerTag.tag).where(MarkerTag.measurement_type_id == target.id))
    existing_target_tags = set(target_tags_result.scalars().all())

    for tag in source_tags:
        if tag.tag in existing_target_tags:
            await db.delete(tag)
            continue
        tag.measurement_type_id = target.id
        existing_target_tags.add(tag.tag)

    for alias in source_aliases:
        if alias.normalized_key in existing_target_alias_keys:
            await db.delete(alias)
            continue
        alias.measurement_type_id = target.id
        existing_target_alias_keys.add(alias.normalized_key)

    source_insight_result = await db.execute(
        select(BiomarkerInsight).where(BiomarkerInsight.measurement_type_id == source.id)
    )
    source_insight = source_insight_result.scalar_one_or_none()
    if source_insight is not None:
        target_insight_result = await db.execute(
            select(BiomarkerInsight).where(BiomarkerInsight.measurement_type_id == target.id)
        )
        target_insight = target_insight_result.scalar_one_or_none()
        if target_insight is None:
            source_insight.measurement_type_id = target.id
        else:
            await db.delete(source_insight)

    await db.delete(source)
    await db.flush()


def measurement_status(measurement: Measurement) -> str:
    return measurement_status_for_range(
        measurement,
        measurement.canonical_reference_low,
        measurement.canonical_reference_high,
    )


def measurement_status_for_range(
    measurement: Measurement,
    reference_low: float | None,
    reference_high: float | None,
) -> str:
    if getattr(measurement, "unit_conversion_missing", False):
        return "no_range"

    value = measurement.canonical_value
    if value is None:
        return "no_range"

    if reference_low is not None and value < reference_low:
        return "low"
    if reference_high is not None and value > reference_high:
        return "high"
    if reference_low is not None or reference_high is not None:
        return "in_range"
    return "no_range"


def range_position(measurement: Measurement) -> float | None:
    return range_position_for_range(
        measurement,
        measurement.canonical_reference_low,
        measurement.canonical_reference_high,
    )


def range_position_for_range(
    measurement: Measurement,
    reference_low: float | None,
    reference_high: float | None,
) -> float | None:
    if getattr(measurement, "unit_conversion_missing", False):
        return None

    value = measurement.canonical_value

    if value is None or reference_low is None or reference_high is None or reference_high <= reference_low:
        return None
    return (value - reference_low) / (reference_high - reference_low)


def latest_reference_range_for_history(measurements: list[Measurement]) -> tuple[float | None, float | None]:
    if not measurements:
        return None, None

    latest = measurements[-1]
    if latest.canonical_value is None or getattr(latest, "unit_conversion_missing", False):
        return None, None

    # Follow-up reports often omit the range even though the biomarker still has a
    # stable canonical interval, so marker-level views reuse the newest usable one.
    for measurement in reversed(measurements):
        if getattr(measurement, "unit_conversion_missing", False):
            continue
        if measurement.canonical_reference_low is not None or measurement.canonical_reference_high is not None:
            return measurement.canonical_reference_low, measurement.canonical_reference_high

    return None, None


def build_marker_payload(measurements: list[Measurement]) -> dict:
    latest = measurements[-1]
    previous = measurements[-2] if len(measurements) > 1 else None
    reference_low, reference_high = latest_reference_range_for_history(measurements)
    has_numeric_history = any(measurement.canonical_value is not None for measurement in measurements)
    has_qualitative_trend = sum(measurement.qualitative_bool is not None for measurement in measurements) > 1
    values = [
        measurement.canonical_value
        for measurement in measurements
        if measurement.canonical_value is not None and not getattr(measurement, "unit_conversion_missing", False)
    ]
    return {
        "marker_name": latest.marker_name,
        "group_name": latest.group_name,
        "canonical_unit": latest.canonical_unit,
        "latest_measurement": latest,
        "previous_measurement": previous,
        "reference_low": reference_low,
        "reference_high": reference_high,
        "status": measurement_status_for_range(latest, reference_low, reference_high),
        "range_position": range_position_for_range(latest, reference_low, reference_high),
        "has_numeric_history": has_numeric_history,
        "has_qualitative_trend": has_qualitative_trend,
        "total_count": len(measurements),
        "value_min": min(values) if values else None,
        "value_max": max(values) if values else None,
    }


def build_marker_histories(measurements: list[Measurement]) -> dict[str, list[Measurement]]:
    by_marker: dict[str, list[Measurement]] = defaultdict(list)
    for measurement in measurements:
        by_marker[measurement.marker_name].append(measurement)
    return by_marker


def build_marker_tag_map(
    by_marker: dict[str, list[Measurement]],
    stored_marker_tags: dict[str, list[str]],
) -> dict[str, list[str]]:
    return {
        marker_name: combine_marker_tags(
            stored_marker_tags.get(marker_name, []),
            marker_measurements[-1].group_name,
            len(marker_measurements),
        )
        for marker_name, marker_measurements in by_marker.items()
    }


def build_marker_file_tag_map(by_marker: dict[str, list[Measurement]]) -> dict[str, list[str]]:
    file_tag_map: dict[str, list[str]] = {}

    for marker_name, marker_measurements in by_marker.items():
        file_tags: list[str] = []
        for measurement in marker_measurements:
            file_tags.extend(tag.tag for tag in measurement.lab_file.tags)
        file_tag_map[marker_name] = sorted(normalize_unique_tags(file_tags), key=str.casefold)

    return file_tag_map


def combine_search_tags(marker_tags: list[str], file_tags: list[str]) -> list[str]:
    return normalize_unique_tags([*marker_tags, *file_tags])
