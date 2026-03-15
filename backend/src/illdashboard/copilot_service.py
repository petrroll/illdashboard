"""Integration with GitHub Copilot SDK for OCR and explanations.

Uses the official Copilot SDK (github-copilot-sdk) which manages the
copilot CLI subprocess, authentication, and model communication.
"""

import json
import math
import os
import tempfile
from pathlib import Path

import fitz  # PyMuPDF
from copilot import CopilotClient, PermissionHandler

from illdashboard.config import settings
from illdashboard.metrics import store_premium_requests

# ── Client management ────────────────────────────────────────────────────────

_client: CopilotClient | None = None

OCR_PDF_BATCH_SIZE = 2
OCR_PDF_RENDER_DPI = 144
OCR_PDF_MIN_RENDER_DPI = 96


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
        content = response.data.content if response else ""
    finally:
        await session.disconnect()

    # Update persisted premium request count from the SDK quota API (best-effort).
    try:
        quota_result = await client.rpc.account.get_quota()
        snapshot = quota_result.quota_snapshots.get("premium_interactions")
        if snapshot is not None:
            store_premium_requests(snapshot.used_requests)
    except Exception:
        pass

    return content


def _parse_json_response(raw: str) -> dict:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
    if raw.endswith("```"):
        raw = raw.rsplit("```", 1)[0]
    return json.loads(raw.strip())


def _is_request_too_large_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "413" in message or "failed to parse request" in message


# ── OCR ──────────────────────────────────────────────────────────────────────

OCR_SYSTEM_PROMPT = """\
You are a medical lab report OCR assistant. The user will provide an image or \
PDF of a lab report as a file attachment. Your job is to:

1. Identify every measured lab marker (e.g. Hemoglobin, WBC, Glucose…).
2. Identify the lab/report source when possible (for example Synlab, Jaeger, Unilabs).
3. For each marker return a JSON array of objects with keys:
   "marker_name", "value" (numeric), "unit", "reference_low" (numeric or null),
   "reference_high" (numeric or null), "measured_at" (ISO date string or null),
   "page_number" (integer, 1-indexed – which page/image the value appears on).
4. Also return "lab_date" (ISO date string or null) for the report date.
5. Also return "source" as a short raw source/provider name string, or null if unclear.

CRITICAL rules for numeric values:
- "value", "reference_low", and "reference_high" MUST be JSON numbers (not strings).
- Use a dot (.) as the decimal separator, never a comma or space. E.g. 0.1, not "0,1" or "0 1".
- Do NOT insert spaces into numbers. E.g. 1500, not "1 500".
- If a value is less than 1, include the leading zero: 0.1, not .1.
- Read decimal points carefully – "0.1" (zero point one) is very different from "1".

When multiple pages/images are attached, number them starting from 1 in the \
order they are provided and set "page_number" accordingly for every measurement.
If there is only one page/image, set "page_number" to 1 for all measurements.

Use the provided original filename as an additional hint for the source only when it helps.

Return ONLY valid JSON: {"lab_date": "...", "source": "...", "measurements": [...]}.
Do not include any commentary outside the JSON.\
"""


def _pdf_to_images(pdf_path: str, *, start_page: int = 0, stop_page: int | None = None, dpi: int = OCR_PDF_RENDER_DPI) -> list[str]:
    """Convert selected PDF pages to temporary PNG files.

    Returns a list of temporary file paths. Caller is responsible for cleanup.
    """
    doc = fitz.open(pdf_path)
    paths: list[str] = []
    if stop_page is None:
        stop_page = doc.page_count

    try:
        for page_index in range(start_page, stop_page):
            page = doc.load_page(page_index)
            pix = page.get_pixmap(dpi=dpi, colorspace=fitz.csGRAY, alpha=False)
            tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
            tmp.close()
            pix.save(tmp.name)
            paths.append(tmp.name)
    finally:
        doc.close()
    return paths


async def _ocr_extract_from_attachments(attachments: list[dict], *, filename: str | None = None) -> dict:
    prompt = "Extract all lab values from the attached file."
    if filename:
        prompt = f"Original filename: {filename}\n\n{prompt}"

    raw = await _ask(
        OCR_SYSTEM_PROMPT,
        prompt,
        attachments=attachments,
    )
    return _parse_json_response(raw)


def _offset_result_page_numbers(result: dict, page_offset: int) -> dict:
    measurements: list[dict] = []
    for measurement in result.get("measurements", []):
        shifted = dict(measurement)
        page_number = shifted.get("page_number")
        try:
            shifted["page_number"] = int(page_number) + page_offset
        except (TypeError, ValueError):
            pass
        measurements.append(shifted)

    return {
        "lab_date": result.get("lab_date"),
        "measurements": measurements,
    }


def _merge_ocr_results(results: list[dict]) -> dict:
    merged = {"lab_date": None, "measurements": []}
    for result in results:
        if merged["lab_date"] is None and result.get("lab_date"):
            merged["lab_date"] = result["lab_date"]
        merged["measurements"].extend(result.get("measurements", []))
    return merged


async def _ocr_extract_pdf_batch(
    pdf_path: str,
    *,
    start_page: int,
    stop_page: int,
    dpi: int,
    filename: str | None = None,
) -> dict:
    temp_images: list[str] = []
    try:
        temp_images = _pdf_to_images(pdf_path, start_page=start_page, stop_page=stop_page, dpi=dpi)
        attachments = [{"type": "file", "path": path} for path in temp_images]
        return await _ocr_extract_from_attachments(attachments, filename=filename)
    finally:
        for path in temp_images:
            try:
                os.unlink(path)
            except OSError:
                pass


async def _ocr_extract_pdf_range(
    pdf_path: str,
    *,
    start_page: int,
    stop_page: int,
    dpi: int = OCR_PDF_RENDER_DPI,
    filename: str | None = None,
) -> dict:
    try:
        result = await _ocr_extract_pdf_batch(
            pdf_path,
            start_page=start_page,
            stop_page=stop_page,
            dpi=dpi,
            filename=filename,
        )
        return _offset_result_page_numbers(result, start_page)
    except Exception as exc:
        if not _is_request_too_large_error(exc):
            raise

        page_count = stop_page - start_page
        if page_count > 1:
            midpoint = start_page + math.ceil(page_count / 2)
            left = await _ocr_extract_pdf_range(
                pdf_path,
                start_page=start_page,
                stop_page=midpoint,
                dpi=dpi,
                filename=filename,
            )
            right = await _ocr_extract_pdf_range(
                pdf_path,
                start_page=midpoint,
                stop_page=stop_page,
                dpi=dpi,
                filename=filename,
            )
            return _merge_ocr_results([left, right])

        smaller_dpi = max(OCR_PDF_MIN_RENDER_DPI, dpi - 24)
        if smaller_dpi != dpi:
            return await _ocr_extract_pdf_range(
                pdf_path,
                start_page=start_page,
                stop_page=stop_page,
                dpi=smaller_dpi,
                filename=filename,
            )
        raise


async def _ocr_extract_pdf(pdf_path: str, *, filename: str | None = None) -> dict:
    with fitz.open(pdf_path) as doc:
        page_count = doc.page_count

    results: list[dict] = []
    for start_page in range(0, page_count, OCR_PDF_BATCH_SIZE):
        stop_page = min(start_page + OCR_PDF_BATCH_SIZE, page_count)
        results.append(
            await _ocr_extract_pdf_range(
                pdf_path,
                start_page=start_page,
                stop_page=stop_page,
                filename=filename,
            )
        )

    return _merge_ocr_results(results)


async def ocr_extract(file_path: str, *, filename: str | None = None) -> dict:
    """Send a lab file to Copilot SDK for OCR extraction.

    For images, sends the file directly. For PDFs, converts each page
    to a PNG image first so the vision model can read the content.

    Args:
        file_path: Absolute path to the uploaded file (PDF or image).

    Returns:
        Parsed JSON dict with ``lab_date``, optional ``source``, and ``measurements`` list.
    """
    if Path(file_path).suffix.lower() == ".pdf":
        return await _ocr_extract_pdf(file_path, filename=filename)

    attachments = [{"type": "file", "path": file_path}]
    return await _ocr_extract_from_attachments(attachments, filename=filename)


# ── Marker name normalization ─────────────────────────────────────────────────

NORMALIZE_SYSTEM_PROMPT = """\
You are a medical lab data normalization assistant. The user will give you:
1. A list of EXISTING canonical marker names already in the database.
2. A list of NEW marker names extracted from an OCR result.

For each new marker name, decide:
- If it matches an existing canonical name (same test, just different formatting, \
spacing, abbreviation, or punctuation), map it to that existing canonical name.
- If it is genuinely new (no match in the existing list), return a cleaned-up, \
standard English canonical lab marker name when you can translate it confidently.
- Prefer concise English medical names such as \"White Blood Cell (WBC) Count\" \
or \"Platelet Count\" over local-language labels.

Return ONLY valid JSON: a mapping object where keys are the original new names \
and values are the canonical names.
Example: {"Lymfocyty -abs.počet": "Lymfocyty - abs.počet", "Hemoglobin": "Hemoglobin"}
Do not include any commentary outside the JSON.\
"""


SOURCE_NORMALIZE_SYSTEM_PROMPT = """\
You are a normalization assistant for lab file source tags. The user will give you:
1. A list of EXISTING canonical source values already used in the database.
2. A raw source/provider name detected from OCR, which may be null.
3. The original filename of the uploaded file.

Your job is to return one canonical source value or null.
- Reuse an existing canonical value when it clearly refers to the same source.
- Prefer short lowercase names such as "synlab" or "jaeger".
- Use the filename as a hint when it helps disambiguate the source.
- Return null if the source is too uncertain.

Return ONLY valid JSON: {"source": "..."} or {"source": null}.
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


async def normalize_source_name(
    source_name: str | None,
    filename: str | None,
    existing_canonical: list[str],
) -> str | None:
    """Use the LLM to normalize a lab file source/provider name."""
    if not source_name and not filename:
        return None

    user_text = "EXISTING canonical source values:\n"
    if existing_canonical:
        for name in existing_canonical:
            user_text += f"- {name}\n"
    else:
        user_text += "(none yet)\n"

    user_text += f"\nOCR-detected source: {source_name or '(none)'}\n"
    user_text += f"Original filename: {filename or '(none)'}\n"

    raw = await _ask(SOURCE_NORMALIZE_SYSTEM_PROMPT, user_text)

    try:
        payload = _parse_json_response(raw)
    except (json.JSONDecodeError, ValueError):
        return None

    normalized_source = payload.get("source")
    if normalized_source is None:
        return None
    if not isinstance(normalized_source, str):
        return None

    normalized_source = normalized_source.strip()
    return normalized_source or None


# ── Explanations ─────────────────────────────────────────────────────────────

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
2. What the latest value means relative to its range.
3. What the recent trend suggests, based only on the supplied values.
4. A short caution that this is not a diagnosis and should be interpreted with a clinician.

Keep the language plain, factual, and concise. Do not invent causes that are not \
supported by the data.\
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


async def explain_marker_history(marker_name: str, measurements: list[dict]) -> str:
    """Ask Copilot to explain one biomarker with historical context."""
    user_text = f"Please explain the history of {marker_name}.\n\n"
    for measurement in measurements:
        line = f"- {measurement['date']}: {measurement['value']}"
        if measurement.get("unit"):
            line += f" {measurement['unit']}"
        if (
            measurement.get("reference_low") is not None
            and measurement.get("reference_high") is not None
        ):
            line += (
                f" (ref {measurement['reference_low']}–{measurement['reference_high']})"
            )
        user_text += line + "\n"

    return await _ask(MARKER_HISTORY_SYSTEM_PROMPT, user_text)
