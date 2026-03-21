from __future__ import annotations

import re
import unicodedata

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.models import QualitativeRule

_SYMBOLIC_QUALITATIVE_VALUE_RE = re.compile(r"[+\-/]+")


def normalize_qualitative_key(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = value.strip()
    if not normalized:
        return None

    normalized = unicodedata.normalize("NFKD", normalized).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.casefold().strip()
    # Standalone symbolic results like "-", "++", or "+/-" carry the entire
    # meaning, so replacing "-" with whitespace would collapse their job keys.
    symbolic_value = normalized.strip(".:;,()[]{}").strip()
    if _SYMBOLIC_QUALITATIVE_VALUE_RE.fullmatch(symbolic_value):
        return symbolic_value
    normalized = normalized.replace("-", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = normalized.strip(".:;,()[]{}").strip()
    return normalized or None


async def load_qualitative_rules(
    db: AsyncSession,
    values: list[str],
) -> dict[str, QualitativeRule]:
    normalized_values = list(
        dict.fromkeys(
            normalized_value for value in values if (normalized_value := normalize_qualitative_key(value)) is not None
        )
    )
    if not normalized_values:
        return {}

    result = await db.execute(
        select(QualitativeRule)
        .options(selectinload(QualitativeRule.measurement_type))
        .where(QualitativeRule.normalized_original_value.in_(normalized_values))
    )
    return {rule.normalized_original_value: rule for rule in result.scalars().all()}


async def upsert_qualitative_rules(
    db: AsyncSession,
    entries: list[dict],
) -> dict[str, QualitativeRule]:
    normalized_entries: dict[str, dict] = {}
    for entry in entries:
        original_value = entry.get("original_value")
        canonical_value = entry.get("canonical_value")
        normalized_value = normalize_qualitative_key(original_value)
        if normalized_value is None or not isinstance(canonical_value, str) or not canonical_value.strip():
            continue
        normalized_entries[normalized_value] = {
            "original_value": str(original_value).strip(),
            "canonical_value": canonical_value.strip(),
            "boolean_value": entry.get("boolean_value"),
            "measurement_type": entry.get("measurement_type"),
        }

    if not normalized_entries:
        return {}

    existing_rules = await load_qualitative_rules(
        db,
        [entry["original_value"] for entry in normalized_entries.values()],
    )

    for normalized_value, entry in normalized_entries.items():
        rule = existing_rules.get(normalized_value)
        if rule is None:
            rule = QualitativeRule(
                original_value=entry["original_value"],
                canonical_value=entry["canonical_value"],
                boolean_value=entry["boolean_value"],
                normalized_original_value=normalized_value,
                measurement_type=entry["measurement_type"],
            )
            db.add(rule)
            existing_rules[normalized_value] = rule
            continue

        rule.original_value = entry["original_value"]
        rule.canonical_value = entry["canonical_value"]
        rule.boolean_value = entry["boolean_value"]
        if entry["measurement_type"] is not None:
            rule.measurement_type = entry["measurement_type"]

    await db.flush()
    return existing_rules
