"""Insight generation and caching helpers for biomarker histories."""

from __future__ import annotations

import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from illdashboard.copilot.explanations import explain_marker_history
from illdashboard.models import BiomarkerInsight, Measurement, MeasurementType
from illdashboard.services.markers import measurement_status


def _measurement_snapshot(measurement: Measurement | None) -> dict | None:
    if measurement is None:
        return None

    return {
        "id": measurement.id,
        "numeric_value": measurement.canonical_value,
        "qualitative_value": measurement.qualitative_value,
        "qualitative_bool": measurement.qualitative_bool,
        "measured_at": measurement.measured_at.isoformat() if measurement.measured_at else None,
        "unit": measurement.canonical_unit,
        "reference_low": measurement.canonical_reference_low,
        "reference_high": measurement.canonical_reference_high,
        "unit_conversion_missing": bool(getattr(measurement, "unit_conversion_missing", False)),
    }


def _measurement_value_for_ai(measurement: Measurement) -> str | float | None:
    if measurement.canonical_value is not None:
        return measurement.canonical_value
    return measurement.qualitative_value


def _measurement_display_unit(measurement: Measurement) -> str | None:
    return measurement.canonical_unit or measurement.original_unit


def _measurement_display_value(measurement: Measurement) -> str:
    unit = _measurement_display_unit(measurement)
    unit_suffix = f" {unit}" if unit else ""

    if measurement.qualitative_value is not None:
        return f"{measurement.qualitative_value}{unit_suffix}"
    if measurement.canonical_value is not None:
        return f"{measurement.canonical_value:g}{unit_suffix}"
    return f"unavailable{unit_suffix}".strip()


def marker_signature(measurements: list[Measurement]) -> str:
    latest = measurements[-1]
    previous = measurements[-2] if len(measurements) > 1 else None
    payload = {
        "count": len(measurements),
        "latest": _measurement_snapshot(latest),
        "previous": _measurement_snapshot(previous),
    }
    return json.dumps(payload, sort_keys=True)


def serialize_history_for_ai(measurements: list[Measurement]) -> list[dict]:
    return [
        {
            "date": measurement.measured_at.date().isoformat() if measurement.measured_at else "unknown date",
            "value": _measurement_value_for_ai(measurement),
            "unit": _measurement_display_unit(measurement),
            "reference_low": measurement.canonical_reference_low,
            "reference_high": measurement.canonical_reference_high,
        }
        for measurement in measurements[-8:]
    ]


def fallback_marker_explanation(marker_name: str, measurements: list[Measurement]) -> str:
    latest = measurements[-1]
    previous = measurements[-2] if len(measurements) > 1 else None
    status = measurement_status(latest).replace("_", " ")
    latest_value = _measurement_display_value(latest)
    parts = [f"## {marker_name}"]

    if latest.canonical_value is None and latest.qualitative_value is not None:
        parts.append(f"Latest result: **{latest_value}**.")

        if latest.qualitative_bool is True:
            parts.append("The latest result is reported as positive.")
        elif latest.qualitative_bool is False:
            parts.append("The latest result is reported as negative.")
        else:
            parts.append("The latest result is qualitative, so there is no numeric reference range to compare against.")

        if previous is not None and previous.qualitative_value is not None:
            if previous.qualitative_value == latest.qualitative_value:
                parts.append(f"Compared with the previous result, it is **unchanged** at **{latest.qualitative_value}**.")
            else:
                parts.append(
                    "Compared with the previous result, it changed "
                    f"from **{previous.qualitative_value}** to **{latest.qualitative_value}**."
                )

        return "\n\n".join(parts)

    unit = _measurement_display_unit(latest)
    unit_suffix = f" {unit}" if unit else ""
    parts.append(f"Latest value: **{latest_value}**. Status: **{status}**.")

    if latest.canonical_reference_low is not None and latest.canonical_reference_high is not None:
        parts.append(
            f"Reference range from the report: **{latest.canonical_reference_low:g} to {latest.canonical_reference_high:g}{unit_suffix}**."
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

    if previous is not None and latest.canonical_value is not None and previous.canonical_value is not None:
        delta = latest.canonical_value - previous.canonical_value
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