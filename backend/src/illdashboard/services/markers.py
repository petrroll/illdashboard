"""Marker classification, tags, and measurement query helpers."""

from __future__ import annotations

import re
import unicodedata
from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.models import BiomarkerInsight, MarkerTag, Measurement, MeasurementType


GROUP_ORDER = [
    "Blood Function",
    "Iron Status",
    "Inflammation & Infection",
    "Metabolic",
    "Kidney Function",
    "Electrolytes",
    "Urinalysis",
    "Lipids",
    "Liver Function",
    "Thyroid",
    "Vitamins & Minerals",
    "Hormones",
    "Immunity & Serology",
    "Other",
]
SINGLE_MEASUREMENT_TAG = "singlemeasurement"
MULTIPLE_MEASUREMENTS_TAG = "multiplemeasurements"


def normalize_unique_tags(tags: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()

    for raw_tag in tags:
        tag = raw_tag.strip()
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


def all_reserved_marker_tags(group_name: str | None = None) -> set[str]:
    reserved_tags = {
        SINGLE_MEASUREMENT_TAG,
        MULTIPLE_MEASUREMENTS_TAG,
        *(marker_group_tag(group) for group in GROUP_ORDER),
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


def marker_matches(name: str, keywords: tuple[str, ...], patterns: tuple[str, ...] = ()) -> bool:
    return any(keyword in name for keyword in keywords) or any(re.search(pattern, name) for pattern in patterns)


def classify_marker_group(name: str) -> str:
    marker = normalized_marker_key(name)

    if marker_matches(
        marker,
        (
            "wbc",
            "white blood",
            "neutroph",
            "lymph",
            "monocyt",
            "eosinoph",
            "basoph",
            "platelet",
            "hemoglobin",
            "hematocrit",
            "mcv",
            "mch",
            "mchc",
            "rdw",
            "reticul",
            "red blood",
            "rbc",
            "lymfocyt",
            "neutrofil",
            "bazofil",
            "eozinofil",
            "hematokrit",
            "tromb",
            "granulocyt",
        ),
    ):
        return "Blood Function"
    if marker_matches(marker, ("ferritin", "iron", "transferrin", "tibc", "uibc")):
        return "Iron Status"
    if marker_matches(marker, ("crp", "sedimentation", "procalcitonin", "esr")):
        return "Inflammation & Infection"
    if marker_matches(marker, ("glucose", "hba1c", "insulin", "c peptide", "c-peptide")):
        return "Metabolic"
    if marker_matches(marker, ("creatin", "urea", "egfr", "uric acid", "albumin/creatinine")):
        return "Kidney Function"
    if marker_matches(
        marker,
        (
            "sodium",
            "potassium",
            "chloride",
            "bicarbonate",
            "carbon dioxide",
            "anion gap",
            "osmolality",
            "bicarb",
            "magnesium",
            "horcik",
        ),
        (r"\bna(?:\+)?\b", r"\bk(?:\+)?\b", r"\bcl(?:-)?\b", r"\bhco3(?:-)??\b", r"\bco2\b", r"\bmg\b"),
    ):
        return "Electrolytes"
    if marker_matches(marker, ("urine", "leukocyte esterase", "nitrite", "specific gravity", "ketone", "proteinuria", "moci")):
        return "Urinalysis"
    if marker_matches(marker, ("cholesterol", "triglycer", "hdl", "ldl", "apolipoprotein", "lipoprotein")):
        return "Lipids"
    if marker_matches(marker, ("alt", "ast", "ggt", "alp", "bilirubin", "albumin", "protein")):
        return "Liver Function"
    if marker_matches(marker, ("tsh", "ft4", "free t4", "ft3", "free t3", "thyroid")):
        return "Thyroid"
    if marker_matches(
        marker,
        ("vitamin", "folate", "folic", "b12", "zinc", "selenium", "calcium", "phosphate", "phosphorus", "vapnik"),
    ):
        return "Vitamins & Minerals"
    if marker_matches(marker, ("testosterone", "estradiol", "progesterone", "lh", "fsh", "cortisol", "prolactin", "dhea", "hcg")):
        return "Hormones"
    if marker_matches(marker, ("igg", "igm", "iga", "ige", "antibod", "protilatk")):
        return "Immunity & Serology"
    return "Other"


async def ensure_measurement_types(db: AsyncSession, names: list[str]) -> dict[str, MeasurementType]:
    unique_names = list(dict.fromkeys(name for name in names if name))
    if not unique_names:
        return {}

    result = await db.execute(select(MeasurementType).where(MeasurementType.name.in_(unique_names)))
    by_name = {measurement_type.name: measurement_type for measurement_type in result.scalars().all()}

    for name in unique_names:
        if name in by_name:
            measurement_type = by_name[name]
            expected_group = classify_marker_group(name)
            if measurement_type.group_name != expected_group:
                measurement_type.group_name = expected_group
            continue

        measurement_type = MeasurementType(name=name, group_name=classify_marker_group(name))
        db.add(measurement_type)
        by_name[name] = measurement_type

    await db.flush()
    return by_name


async def get_measurement_type_by_name(db: AsyncSession, marker_name: str) -> MeasurementType | None:
    result = await db.execute(select(MeasurementType).where(MeasurementType.name == marker_name))
    return result.scalar_one_or_none()


async def load_measurements_for_marker(db: AsyncSession, marker_name: str) -> list[Measurement]:
    result = await db.execute(
        select(Measurement)
        .join(Measurement.measurement_type)
        .options(selectinload(Measurement.measurement_type))
        .where(MeasurementType.name == marker_name)
        .order_by(Measurement.measured_at.asc(), Measurement.id.asc())
    )
    return list(result.scalars().all())


async def merge_measurement_types(source: MeasurementType, target: MeasurementType, db: AsyncSession) -> None:
    if source.id == target.id:
        return

    target.group_name = classify_marker_group(target.name)

    measurements_result = await db.execute(select(Measurement).where(Measurement.measurement_type_id == source.id))
    for measurement in measurements_result.scalars().all():
        measurement.measurement_type_id = target.id

    source_tags_result = await db.execute(select(MarkerTag).where(MarkerTag.measurement_type_id == source.id))
    source_tags = source_tags_result.scalars().all()

    target_tags_result = await db.execute(select(MarkerTag.tag).where(MarkerTag.measurement_type_id == target.id))
    existing_target_tags = set(target_tags_result.scalars().all())

    for tag in source_tags:
        if tag.tag in existing_target_tags:
            await db.delete(tag)
            continue
        tag.measurement_type_id = target.id
        existing_target_tags.add(tag.tag)

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
    reference_low = measurement.reference_low
    reference_high = measurement.reference_high
    value = measurement.value

    if reference_low is not None and value < reference_low:
        return "low"
    if reference_high is not None and value > reference_high:
        return "high"
    if reference_low is not None or reference_high is not None:
        return "in_range"
    return "no_range"


def range_position(measurement: Measurement) -> float | None:
    reference_low = measurement.reference_low
    reference_high = measurement.reference_high
    value = measurement.value

    if reference_low is None or reference_high is None or reference_high <= reference_low:
        return None
    return (value - reference_low) / (reference_high - reference_low)


def build_marker_payload(measurements: list[Measurement]) -> dict:
    latest = measurements[-1]
    previous = measurements[-2] if len(measurements) > 1 else None
    values = [measurement.value for measurement in measurements]
    return {
        "marker_name": latest.marker_name,
        "group_name": latest.group_name,
        "latest_measurement": latest,
        "previous_measurement": previous,
        "status": measurement_status(latest),
        "range_position": range_position(latest),
        "total_count": len(measurements),
        "value_min": min(values),
        "value_max": max(values),
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
