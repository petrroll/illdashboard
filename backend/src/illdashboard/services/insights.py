"""Insight generation and caching helpers for biomarker histories."""

from __future__ import annotations

import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from illdashboard.copilot.explanations import explain_marker_history
from illdashboard.models import BiomarkerInsight, Measurement, MeasurementType
from illdashboard.services.markers import (
    effective_measurement_qualitative_bool,
    effective_measurement_qualitative_value,
    effective_measurement_reference_high,
    effective_measurement_reference_low,
    effective_measurement_unit,
    effective_measurement_value,
    latest_reference_range_for_history,
    measurement_status_for_range,
)


def _measurement_snapshot(measurement: Measurement | None) -> dict | None:
    if measurement is None:
        return None

    return {
        "id": measurement.id,
        "numeric_value": effective_measurement_value(measurement),
        "qualitative_value": effective_measurement_qualitative_value(measurement),
        "qualitative_bool": effective_measurement_qualitative_bool(measurement),
        "measured_at": measurement.effective_measured_at.isoformat() if measurement.effective_measured_at else None,
        "unit": effective_measurement_unit(measurement),
        "reference_low": effective_measurement_reference_low(measurement),
        "reference_high": effective_measurement_reference_high(measurement),
        "unit_conversion_missing": bool(getattr(measurement, "unit_conversion_missing", False)),
    }


def _measurement_value_for_ai(measurement: Measurement) -> str | float | None:
    numeric_value = effective_measurement_value(measurement)
    if numeric_value is not None:
        return numeric_value
    return effective_measurement_qualitative_value(measurement)


def _measurement_display_unit(measurement: Measurement) -> str | None:
    return effective_measurement_unit(measurement) or measurement.original_unit


def _measurement_display_value(measurement: Measurement) -> str:
    unit = _measurement_display_unit(measurement)
    unit_suffix = f" {unit}" if unit else ""

    qualitative_value = effective_measurement_qualitative_value(measurement)
    if qualitative_value is not None:
        return f"{qualitative_value}{unit_suffix}"
    numeric_value = effective_measurement_value(measurement)
    if numeric_value is not None:
        return f"{numeric_value:g}{unit_suffix}"
    return f"unavailable{unit_suffix}".strip()


def marker_signature(measurements: list[Measurement]) -> str:
    latest = measurements[-1]
    previous = measurements[-2] if len(measurements) > 1 else None
    effective_reference_low, effective_reference_high = latest_reference_range_for_history(measurements)
    payload = {
        "count": len(measurements),
        "latest": _measurement_snapshot(latest),
        "previous": _measurement_snapshot(previous),
        "effective_reference_low": effective_reference_low,
        "effective_reference_high": effective_reference_high,
    }
    return json.dumps(payload, sort_keys=True)


def serialize_history_for_ai(measurements: list[Measurement]) -> list[dict]:
    latest = measurements[-1]
    effective_reference_low, effective_reference_high = latest_reference_range_for_history(measurements)
    return [
        {
            "date": measurement.effective_measured_at.date().isoformat()
            if measurement.effective_measured_at
            else "unknown date",
            "value": _measurement_value_for_ai(measurement),
            "unit": _measurement_display_unit(measurement),
            "reference_low": (
                effective_reference_low
                if measurement is latest
                else effective_measurement_reference_low(measurement)
            ),
            "reference_high": effective_reference_high
            if measurement is latest
            else effective_measurement_reference_high(measurement),
        }
        for measurement in measurements[-8:]
    ]


def fallback_marker_explanation(marker_name: str, measurements: list[Measurement]) -> str:
    latest = measurements[-1]
    previous = measurements[-2] if len(measurements) > 1 else None
    effective_reference_low, effective_reference_high = latest_reference_range_for_history(measurements)
    status = measurement_status_for_range(latest, effective_reference_low, effective_reference_high).replace("_", " ")
    latest_value = _measurement_display_value(latest)
    parts = [f"## {marker_name}"]

    latest_value_number = effective_measurement_value(latest)
    latest_qualitative_value = effective_measurement_qualitative_value(latest)
    if latest_value_number is None and latest_qualitative_value is not None:
        parts.append(f"Latest result: **{latest_value}**.")

        if effective_measurement_qualitative_bool(latest) is True:
            parts.append("The latest result is reported as positive.")
        elif effective_measurement_qualitative_bool(latest) is False:
            parts.append("The latest result is reported as negative.")
        else:
            parts.append("The latest result is qualitative, so there is no numeric reference range to compare against.")

        previous_qualitative_value = (
            effective_measurement_qualitative_value(previous) if previous is not None else None
        )
        if previous is not None and previous_qualitative_value is not None:
            if previous_qualitative_value == latest_qualitative_value:
                parts.append(
                    f"Compared with the previous result, it is **unchanged** at **{latest_qualitative_value}**."
                )
            else:
                parts.append(
                    "Compared with the previous result, it changed "
                    f"from **{previous_qualitative_value}** to **{latest_qualitative_value}**."
                )

        return "\n\n".join(parts)

    unit = _measurement_display_unit(latest)
    unit_suffix = f" {unit}" if unit else ""
    parts.append(f"Latest value: **{latest_value}**. Status: **{status}**.")

    if effective_reference_low is not None and effective_reference_high is not None:
        parts.append(
            "Reference range from the report: "
            f"**{effective_reference_low:g} to {effective_reference_high:g}{unit_suffix}**."
        )

    if status == "low":
        parts.append(
            "The latest result is below the reported range, which means it is "
            "lower than the lab's usual reference interval for this marker."
        )
    elif status == "high":
        parts.append(
            "The latest result is above the reported range, which means it is "
            "higher than the lab's usual reference interval for this marker."
        )
    elif status == "in range":
        parts.append(
            "The latest result is within the reported range, which means it "
            "falls inside the lab's usual reference interval for this marker."
        )

    previous_value = effective_measurement_value(previous) if previous is not None else None
    if previous is not None and latest_value_number is not None and previous_value is not None:
        delta = latest_value_number - previous_value
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


async def invalidate_cached_insight(
    measurement_type: MeasurementType,
    db: AsyncSession,
) -> None:
    result = await db.execute(
        select(BiomarkerInsight).where(BiomarkerInsight.measurement_type_id == measurement_type.id)
    )
    cached_insight = result.scalar_one_or_none()
    if cached_insight is None:
        return

    await db.delete(cached_insight)
    await db.flush()


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
