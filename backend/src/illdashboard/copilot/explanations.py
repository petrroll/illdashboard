"""Copilot prompts for human-readable lab explanations."""

from __future__ import annotations

from illdashboard.copilot.client import _ask


EXPLAIN_SYSTEM_PROMPT = """\
You are a knowledgeable medical lab advisor. The user will give you one or more \
lab markers with their values, units, and reference ranges. For each marker:

1. Explain what the marker measures and why it matters.
2. Interpret the value: is it within range, low, or high?
3. Mention possible clinical implications in plain language.

Be concise but thorough. Use markdown formatting.\
"""


MARKER_HISTORY_SYSTEM_PROMPT = """\
You are a knowledgeable medical lab advisor. The user will provide the history of \
a single biomarker across time, including units and reference ranges when available.

Write a short markdown explanation with these sections:
1. What this marker measures.
2. What the latest value means relative to its range. If the latest value is below or above the
    reference range, explicitly explain in plain language what being below or above the limit means.
3. What the recent trend suggests, based only on the supplied values.

Do not add a generic caution or disclaimer section. Do not say that this is not a diagnosis and do
not tell the user to ask a clinician unless the supplied data itself requires a specific limitation
to be mentioned. If only a single value is supplied, do not dwell on the lack of a trend; focus on
explaining what that value means.

Keep the language plain, factual, and concise. Do not invent causes that are not \
supported by the data.\
"""


async def explain_markers(markers: list[dict]) -> str:
    """Ask Copilot to explain one or more lab markers."""
    user_text = "Please explain these lab results:\n\n"
    for marker in markers:
        value = marker.get("qualitative_value")
        if value is None:
            value = marker.get("value")

        line = f"- **{marker['marker_name']}**: {value}"
        if marker.get("unit"):
            line += f" {marker['unit']}"
        if marker.get("reference_low") is not None and marker.get("reference_high") is not None:
            line += f" (ref {marker['reference_low']}–{marker['reference_high']})"
        user_text += line + "\n"

    return await _ask(EXPLAIN_SYSTEM_PROMPT, user_text)


async def explain_marker_history(marker_name: str, measurements: list[dict]) -> str:
    """Ask Copilot to explain one biomarker with historical context."""
    user_text = f"Please explain the history of {marker_name}.\n\n"
    for measurement in measurements:
        line = f"- {measurement['date']}: {measurement['value']}"
        if measurement.get("unit"):
            line += f" {measurement['unit']}"
        if measurement.get("reference_low") is not None and measurement.get("reference_high") is not None:
            line += f" (ref {measurement['reference_low']}–{measurement['reference_high']})"
        user_text += line + "\n"

    return await _ask(MARKER_HISTORY_SYSTEM_PROMPT, user_text)