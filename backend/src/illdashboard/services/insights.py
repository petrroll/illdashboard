"""Insight generation and caching helpers for biomarker histories."""

from __future__ import annotations

import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from illdashboard.copilot_service import explain_marker_history
from illdashboard.models import BiomarkerInsight, Measurement, MeasurementType
from illdashboard.services.markers import measurement_status


def marker_signature(measurements: list[Measurement]) -> str:
    latest = measurements[-1]
    previous = measurements[-2] if len(measurements) > 1 else None
    payload = {
        "count": len(measurements),
        "latest": {
            "id": latest.id,
            "value": latest.value,
            "measured_at": latest.measured_at.isoformat() if latest.measured_at else None,
            "reference_low": latest.reference_low,
            "reference_high": latest.reference_high,
        },
        "previous": {
            "id": previous.id,
            "value": previous.value,
            "measured_at": previous.measured_at.isoformat() if previous and previous.measured_at else None,
        }
        if previous
        else None,
    }
    return json.dumps(payload, sort_keys=True)


def serialize_history_for_ai(measurements: list[Measurement]) -> list[dict]:
    return [
        {
            "date": measurement.measured_at.date().isoformat() if measurement.measured_at else "unknown date",
            "value": measurement.value,
            "unit": measurement.unit,
            "reference_low": measurement.reference_low,
            "reference_high": measurement.reference_high,
        }
        for measurement in measurements[-8:]
    ]


def fallback_marker_explanation(marker_name: str, measurements: list[Measurement]) -> str:
    latest = measurements[-1]
    previous = measurements[-2] if len(measurements) > 1 else None
    status = measurement_status(latest).replace("_", " ")
    unit_suffix = f" {latest.unit}" if latest.unit else ""
    parts = [
        f"## {marker_name}",
        f"Latest value: **{latest.value:g}{unit_suffix}**. Status: **{status}**.",
    ]

    if latest.reference_low is not None and latest.reference_high is not None:
        parts.append(
            f"Reference range from the report: **{latest.reference_low:g} to {latest.reference_high:g}{unit_suffix}**."
        )

    if status == "low":
        parts.append(
            "The latest result is below the reported range, which means it is lower than the lab's usual reference interval for this marker."
        )
    elif status == "high":
        parts.append(
            "The latest result is above the reported range, which means it is higher than the lab's usual reference interval for this marker."
        )
    elif status == "in range":
        parts.append(
            "The latest result is within the reported range, which means it falls inside the lab's usual reference interval for this marker."
        )

    if previous is not None:
        delta = latest.value - previous.value
        direction = "up" if delta > 0 else "down" if delta < 0 else "unchanged"
        parts.append(
            f"Compared with the previous result, the marker is **{direction}** by **{abs(delta):g}{unit_suffix}**."
        )

    return "\n\n".join(parts)


async def get_cached_or_generated_insight(
    measurement_type: MeasurementType,
    measurements: list[Measurement],
    db: AsyncSession,
) -> tuple[str, bool]:
    signature = marker_signature(measurements)
    result = await db.execute(
        select(BiomarkerInsight).where(BiomarkerInsight.measurement_type_id == measurement_type.id)
    )
    cached_insight = result.scalar_one_or_none()
    if cached_insight and cached_insight.measurement_signature == signature:
        return cached_insight.summary_markdown, True

    try:
        explanation = await explain_marker_history(measurement_type.name, serialize_history_for_ai(measurements))
    except Exception:
        explanation = fallback_marker_explanation(measurement_type.name, measurements)

    if cached_insight is None:
        cached_insight = BiomarkerInsight(
            measurement_type_id=measurement_type.id,
            measurement_signature=signature,
            summary_markdown=explanation,
        )
        db.add(cached_insight)
    else:
        cached_insight.measurement_signature = signature
        cached_insight.summary_markdown = explanation

    await db.commit()
    return explanation, False


async def get_cached_insight(
    measurement_type: MeasurementType,
    measurements: list[Measurement],
    db: AsyncSession,
) -> tuple[str | None, bool]:
    signature = marker_signature(measurements)
    result = await db.execute(
        select(BiomarkerInsight).where(BiomarkerInsight.measurement_type_id == measurement_type.id)
    )
    cached_insight = result.scalar_one_or_none()
    if cached_insight and cached_insight.measurement_signature == signature:
        return cached_insight.summary_markdown, True
    return None, False