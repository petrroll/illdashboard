"""Integration with GitHub Copilot SDK for OCR and explanations.

Uses the official Copilot SDK (github-copilot-sdk) which manages the
copilot CLI subprocess, authentication, and model communication.
"""

import json
import os
import tempfile
from pathlib import Path

import fitz  # PyMuPDF
from copilot import CopilotClient, PermissionHandler

from illdashboard.config import settings

# ── Client management ────────────────────────────────────────────────────────

_client: CopilotClient | None = None


async def _get_client() -> CopilotClient:
    """Return a shared CopilotClient, starting it on first use."""
    global _client
    if _client is None:
        token = settings.GITHUB_TOKEN or os.environ.get("GITHUB_TOKEN", "")
        opts = {"github_token": token} if token else None
        _client = CopilotClient(opts)
        await _client.start()
    return _client


async def shutdown_client() -> None:
    """Stop the Copilot client (call on app shutdown)."""
    global _client
    if _client is not None:
        await _client.stop()
        _client = None


async def _ask(system_prompt: str, user_prompt: str, *, attachments: list | None = None, timeout: float = 120) -> str:
    """Create an ephemeral session, send one prompt, return the response text."""
    client = await _get_client()
    session = await client.create_session(
        {
            "model": settings.COPILOT_MODEL,
            "system_message": {"mode": "replace", "content": system_prompt},
            "available_tools": [],  # pure chat, no tool use
            "on_permission_request": PermissionHandler.approve_all,
        }
    )
    try:
        msg_opts: dict = {"prompt": user_prompt}
        if attachments:
            msg_opts["attachments"] = attachments
        response = await session.send_and_wait(msg_opts, timeout=timeout)
        return response.data.content if response else ""
    finally:
        await session.disconnect()


# ── OCR ──────────────────────────────────────────────────────────────────────

OCR_SYSTEM_PROMPT = """\
You are a medical lab report OCR assistant. The user will provide an image or \
PDF of a lab report as a file attachment. Your job is to:

1. Identify every measured lab marker (e.g. Hemoglobin, WBC, Glucose…).
2. For each marker return a JSON array of objects with keys:
   "marker_name", "value" (numeric), "unit", "reference_low" (numeric or null),
   "reference_high" (numeric or null), "measured_at" (ISO date string or null).
3. Also return "lab_date" (ISO date string or null) for the report date.

Return ONLY valid JSON: {"lab_date": "...", "measurements": [...]}.
Do not include any commentary outside the JSON.\
"""


def _pdf_to_images(pdf_path: str) -> list[str]:
    """Convert each page of a PDF to a temporary PNG file.

    Returns a list of temporary file paths. Caller is responsible for cleanup.
    """
    doc = fitz.open(pdf_path)
    paths: list[str] = []
    for page in doc:
        pix = page.get_pixmap(dpi=200)
        tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        pix.save(tmp.name)
        paths.append(tmp.name)
    doc.close()
    return paths


async def ocr_extract(file_path: str) -> dict:
    """Send a lab file to Copilot SDK for OCR extraction.

    For images, sends the file directly. For PDFs, converts each page
    to a PNG image first so the vision model can read the content.

    Args:
        file_path: Absolute path to the uploaded file (PDF or image).

    Returns:
        Parsed JSON dict with ``lab_date`` and ``measurements`` list.
    """
    temp_images: list[str] = []
    try:
        if Path(file_path).suffix.lower() == ".pdf":
            temp_images = _pdf_to_images(file_path)
            attachments = [{"type": "file", "path": p} for p in temp_images]
        else:
            attachments = [{"type": "file", "path": file_path}]

        raw = await _ask(
            OCR_SYSTEM_PROMPT,
            "Extract all lab values from the attached file.",
            attachments=attachments,
        )
    finally:
        for p in temp_images:
            try:
                os.unlink(p)
            except OSError:
                pass

    # Strip markdown code fences if present
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
    if raw.endswith("```"):
        raw = raw.rsplit("```", 1)[0]
    return json.loads(raw.strip())


# ── Marker name normalization ─────────────────────────────────────────────────

NORMALIZE_SYSTEM_PROMPT = """\
You are a medical lab data normalization assistant. The user will give you:
1. A list of EXISTING canonical marker names already in the database.
2. A list of NEW marker names extracted from an OCR result.

For each new marker name, decide:
- If it matches an existing canonical name (same test, just different formatting, \
spacing, abbreviation, or punctuation), map it to that existing canonical name.
- If it is genuinely new (no match in the existing list), return it cleaned up \
(consistent spacing, consistent punctuation style) as the canonical form.

Return ONLY valid JSON: a mapping object where keys are the original new names \
and values are the canonical names.
Example: {"Lymfocyty -abs.počet": "Lymfocyty - abs.počet", "Hemoglobin": "Hemoglobin"}
Do not include any commentary outside the JSON.\
"""


async def normalize_marker_names(new_names: list[str], existing_canonical: list[str]) -> dict[str, str]:
    """Use the LLM to map raw marker names to canonical forms.

    Args:
        new_names: Marker names freshly extracted from OCR.
        existing_canonical: Distinct marker names already in the database.

    Returns:
        Dict mapping each *new_name* → *canonical_name*.
    """
    if not new_names:
        return {}

    user_text = "EXISTING canonical marker names:\n"
    if existing_canonical:
        for n in existing_canonical:
            user_text += f"- {n}\n"
    else:
        user_text += "(none yet)\n"
    user_text += "\nNEW marker names to normalize:\n"
    for n in new_names:
        user_text += f"- {n}\n"

    raw = await _ask(NORMALIZE_SYSTEM_PROMPT, user_text)

    # Strip markdown code fences if present
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
    if raw.endswith("```"):
        raw = raw.rsplit("```", 1)[0]

    try:
        mapping = json.loads(raw.strip())
    except (json.JSONDecodeError, ValueError):
        # Fallback: return identity mapping
        mapping = {n: n for n in new_names}

    # Ensure every new_name has an entry
    for n in new_names:
        if n not in mapping:
            mapping[n] = n

    return mapping


# ── Explanations ─────────────────────────────────────────────────────────────

EXPLAIN_SYSTEM_PROMPT = """\
You are a knowledgeable medical lab advisor. The user will give you one or more \
lab markers with their values, units, and reference ranges. For each marker:

1. Explain what the marker measures and why it matters.
2. Interpret the value: is it within range, low, or high?
3. Mention possible clinical implications in plain language.

Be concise but thorough. Use markdown formatting.\
"""


async def explain_markers(markers: list[dict]) -> str:
    """Ask Copilot to explain a set of lab markers."""
    user_text = "Please explain these lab results:\n\n"
    for m in markers:
        line = f"- **{m['marker_name']}**: {m['value']}"
        if m.get("unit"):
            line += f" {m['unit']}"
        if m.get("reference_low") is not None and m.get("reference_high") is not None:
            line += f" (ref {m['reference_low']}–{m['reference_high']})"
        user_text += line + "\n"

    return await _ask(EXPLAIN_SYSTEM_PROMPT, user_text)
