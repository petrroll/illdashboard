"""Integration with GitHub Copilot SDK for OCR and explanations.

Uses the official Copilot SDK (github-copilot-sdk) which manages the
copilot CLI subprocess, authentication, and model communication.
"""

import asyncio
import json
import logging
import math
import os
import tempfile
import time
from pathlib import Path
from collections.abc import Awaitable, Callable

import fitz  # PyMuPDF
from copilot import CopilotClient, PermissionHandler
from copilot.generated.session_events import SessionEventType
from copilot.types import CopilotClientOptions, MessageOptions

from illdashboard.config import settings
from illdashboard.metrics import add_premium_requests


logger = logging.getLogger(__name__)

# ── Client management ────────────────────────────────────────────────────────

_client: CopilotClient | None = None

OCR_PDF_BATCH_SIZE = 2
OCR_PDF_RENDER_DPI = 144
OCR_PDF_MIN_RENDER_DPI = 96
OCR_ASK_TIMEOUT = 180
OCR_RETRY_DELAY = 3
OCR_PDF_BATCH_CONCURRENCY = 4
MARKER_NORMALIZATION_BATCH_SIZE = 40
MARKER_NORMALIZATION_CONCURRENCY = 2


async def _get_client() -> CopilotClient:
    """Return a shared CopilotClient, starting it on first use."""
    global _client
    if _client is None:
        token = settings.GITHUB_TOKEN or os.environ.get("GITHUB_TOKEN", "")
        opts: CopilotClientOptions | None = {"github_token": token} if token else None
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
    started_at = time.perf_counter()
    attachment_count = len(attachments or [])
    logger.info(
        "Copilot request starting model=%s timeout=%ss attachments=%s prompt_chars=%s",
        settings.COPILOT_MODEL,
        timeout,
        attachment_count,
        len(user_prompt),
    )
    client = await _get_client()
    session = await client.create_session(
        {
            "model": settings.COPILOT_MODEL,
            "system_message": {"mode": "replace", "content": system_prompt},
            "available_tools": [],  # pure chat, no tool use
            "on_permission_request": PermissionHandler.approve_all,
        }
    )
    content = ""
    observed_usage_cost = 0.0
    request_error: Exception | None = None

    def handle_session_event(event) -> None:
        nonlocal observed_usage_cost
        if event.type != SessionEventType.ASSISTANT_USAGE:
            return

        cost = getattr(event.data, "cost", None)
        if isinstance(cost, int | float) and cost > 0:
            observed_usage_cost += float(cost)

    unsubscribe = session.on(handle_session_event)
    try:
        msg_opts: MessageOptions = {"prompt": user_prompt}
        if attachments:
            msg_opts["attachments"] = attachments
        response = await session.send_and_wait(msg_opts, timeout=timeout)
        content = getattr(response.data, "content", "") if response else ""
        if content is None:
            content = ""
    except Exception as exc:
        request_error = exc
    finally:
        unsubscribe()
        await session.disconnect()

    duration = time.perf_counter() - started_at

    add_premium_requests(observed_usage_cost)

    if request_error is not None:
        logger.warning(
            "Copilot request failed after %.2fs model=%s attachments=%s error=%s",
            duration,
            settings.COPILOT_MODEL,
            attachment_count,
            request_error,
        )
        raise request_error

    logger.info(
        "Copilot request finished in %.2fs model=%s attachments=%s response_chars=%s usage_cost=%.4f",
        duration,
        settings.COPILOT_MODEL,
        attachment_count,
        len(content),
        observed_usage_cost,
    )

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


def _is_request_timeout_error(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):
        return True

    message = str(exc).lower()
    return "timeout" in message and "session.idle" in message


def _is_rate_limited_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "429" in message or "rate limit" in message or "too many requests" in message


def _is_retryable_pdf_error(exc: Exception) -> bool:
    return _is_request_too_large_error(exc) or _is_request_timeout_error(exc) or _is_rate_limited_error(exc)


def _retryable_pdf_error_reason(exc: Exception) -> str:
    if _is_request_timeout_error(exc):
        return "timeout"
    if _is_rate_limited_error(exc):
        return "rate-limit"
    if _is_request_too_large_error(exc):
        return "request-too-large"
    return exc.__class__.__name__


# ── OCR ──────────────────────────────────────────────────────────────────────

MEDICAL_OCR_SYSTEM_PROMPT = """\
You are a medical lab report extraction assistant. The user will provide an image or \
PDF of a document. Your job is to:

1. Identify every measured lab marker (e.g. Hemoglobin, WBC, Glucose…).
    This includes qualitative serology and immunology markers reported as positive/negative/reactive/not detected.
2. Identify the lab/report source when possible (for example Synlab, Jaeger, Unilabs).
3. For each marker return a JSON array of objects with keys:
    "marker_name", "value" (number for numeric results, string for qualitative results), "unit", "reference_low" (numeric or null),
    "reference_high" (numeric or null), "measured_at" (ISO date string or null),
    "page_number" (integer, 1-indexed – which page/image the value appears on).
4. Also return "lab_date" (ISO date string or null) for the report date.
5. Also return "source" as a short raw source/provider name string, or null if unclear.

If the document is not a lab report, return an empty "measurements" array instead of inventing measurements.

CRITICAL rules for values:
- Use a JSON number in "value" when the report shows a numeric result.
- Use a short JSON string in "value" when the report shows a qualitative result such as "positive", "negative", "reactive", "non-reactive", "detected", or "not detected".
- Do NOT omit a marker just because its value is qualitative.
- "reference_low" and "reference_high" MUST be JSON numbers or null.
- Use a dot (.) as the decimal separator, never a comma or space. E.g. 0.1, not "0,1" or "0 1".
- Do NOT insert spaces into numbers. E.g. 1500, not "1 500".
- If a value is less than 1, include the leading zero: 0.1, not .1.
- Read decimal points carefully – "0.1" (zero point one) is very different from "1".

CRITICAL extraction rules:
- Do not skip dense semicolon-separated or comma-separated sections. Split every assay/result pair into its own measurement object.
- Serology and immunology pages often list many markers inline on one line. Extract every marker, even when many share the same sentence.
- Keep marker names specific. For example, include the organism plus antibody class such as "Chlamydia psittaci IgG" rather than only "IgG".

When multiple pages/images are attached, number them starting from 1 in the \
order they are provided and set "page_number" accordingly for every measurement.
If there is only one page/image, set "page_number" to 1 for all measurements.

Use the provided original filename as an additional hint for the source only when it helps.

Return ONLY valid JSON: {"lab_date": "...", "source": "...", "measurements": [...]}.
Do not include any commentary outside the JSON.\
"""


TEXT_OCR_SYSTEM_PROMPT = """\
You are a document OCR assistant. The user will provide an image or PDF of a document.

Your job is to:
1. Transcribe ALL visible text into "raw_text" in the original document language.
2. Translate the full document text into English in "translated_text_english".

Rules:
- Always include all visible text, even when the document is administrative or not medical.
- Preserve the document order of the text.
- If the document is already in English, "translated_text_english" may match "raw_text".

Return ONLY valid JSON: {"raw_text": "...", "translated_text_english": "..."}.
Do not include any commentary outside the JSON.\
"""


MEDICAL_SUMMARY_SYSTEM_PROMPT = """\
You are a medical summarization assistant.

The user will provide:
1. Structured medical extraction from a lab or related document.
2. English-translated document text when available.
3. The original filename.

Your job is to write a short factual English summary in 2-4 sentences.
- Focus on the medical meaning of the extracted content when measurements are present.
- Mention notable abnormal or clearly important findings when they are visible.
- If there are no measurements, summarize what kind of document it appears to be.
- Do not repeat the entire OCR text.
- Do not add generic cautions or boilerplate.

Return ONLY valid JSON: {"summary_english": "..."}.
Do not include any commentary outside the JSON.\
"""


def _pdf_to_images(pdf_path: str, *, start_page: int = 0, stop_page: int | None = None, dpi: int = OCR_PDF_RENDER_DPI) -> list[str]:
    """Convert selected PDF pages to temporary PNG files.

    Returns a list of temporary file paths. Caller is responsible for cleanup.
    """
    doc = fitz.open(pdf_path)
    paths: list[str] = []
    page_stop: int = stop_page if stop_page is not None else doc.page_count
    started_at = time.perf_counter()

    logger.info(
        "Rendering PDF pages path=%s pages=%s-%s dpi=%s",
        pdf_path,
        start_page + 1,
        page_stop,
        dpi,
    )

    try:
        for page_index in range(start_page, page_stop):
            page = doc.load_page(page_index)
            pix = page.get_pixmap(dpi=dpi, colorspace=fitz.csGRAY, alpha=False)
            tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
            tmp.close()
            pix.save(tmp.name)
            paths.append(tmp.name)
    finally:
        doc.close()

    logger.info(
        "Rendered PDF pages path=%s pages=%s-%s dpi=%s image_count=%s duration=%.2fs",
        pdf_path,
        start_page + 1,
        page_stop,
        dpi,
        len(paths),
        time.perf_counter() - started_at,
    )
    return paths


async def _extract_structured_medical_data_from_attachments(
    attachments: list[dict], *, filename: str | None = None
) -> dict:
    prompt = "Extract all lab values from the attached file, including qualitative serology and immunology results."
    if filename:
        prompt = f"Original filename: {filename}\n\n{prompt}"

    raw = await _ask(
        MEDICAL_OCR_SYSTEM_PROMPT,
        prompt,
        attachments=attachments,
        timeout=OCR_ASK_TIMEOUT,
    )
    return _parse_json_response(raw)


async def _extract_document_text_from_attachments(attachments: list[dict], *, filename: str | None = None) -> dict:
    prompt = "Transcribe all visible text from the attached document and translate it to English."
    if filename:
        prompt = f"Original filename: {filename}\n\n{prompt}"

    raw = await _ask(
        TEXT_OCR_SYSTEM_PROMPT,
        prompt,
        attachments=attachments,
        timeout=OCR_ASK_TIMEOUT,
    )
    return _parse_json_response(raw)


async def _generate_medical_summary(
    medical_result: dict,
    text_result: dict | None,
    *,
    filename: str | None = None,
) -> str | None:
    user_payload = {
        "filename": filename,
        "translated_text_english": (text_result or {}).get("translated_text_english"),
        "medical_extraction": medical_result,
    }
    raw = await _ask(
        MEDICAL_SUMMARY_SYSTEM_PROMPT,
        json.dumps(user_payload, ensure_ascii=False, indent=2),
    )
    parsed = _parse_json_response(raw)
    summary = parsed.get("summary_english")
    return summary.strip() if isinstance(summary, str) and summary.strip() else None


def _offset_result_page_numbers(result: dict, page_offset: int) -> dict:
    measurements: list[dict] = []
    for measurement in result.get("measurements", []):
        shifted = dict(measurement)
        page_number = shifted.get("page_number")
        if page_number is not None:
            try:
                shifted["page_number"] = int(page_number) + page_offset
            except (TypeError, ValueError):
                pass
        measurements.append(shifted)

    return {
        "lab_date": result.get("lab_date"),
        "source": result.get("source"),
        "measurements": measurements,
    }


def _merge_structured_medical_results(results: list[dict]) -> dict:
    merged = {"lab_date": None, "source": None, "measurements": []}
    for result in results:
        if merged["lab_date"] is None and result.get("lab_date"):
            merged["lab_date"] = result["lab_date"]
        if merged["source"] is None and result.get("source"):
            merged["source"] = result["source"]
        merged["measurements"].extend(result.get("measurements", []))
    return merged


def _merge_document_text_results(results: list[dict]) -> dict:
    raw_text_parts = [str(result.get("raw_text", "")).strip() for result in results if result.get("raw_text")]
    translated_parts = [
        str(result.get("translated_text_english", "")).strip()
        for result in results
        if result.get("translated_text_english")
    ]
    merged: dict[str, str] = {}
    if raw_text_parts:
        merged["raw_text"] = "\n\n".join(part for part in raw_text_parts if part)
    if translated_parts:
        merged["translated_text_english"] = "\n\n".join(part for part in translated_parts if part)
    return merged


def _combine_ocr_outputs(medical_result: dict, text_result: dict | None, summary_english: str | None) -> dict:
    combined = {
        "lab_date": medical_result.get("lab_date"),
        "source": medical_result.get("source"),
        "measurements": medical_result.get("measurements", []),
    }
    if text_result and text_result.get("raw_text"):
        combined["raw_text"] = text_result["raw_text"]
    if text_result and text_result.get("translated_text_english"):
        combined["translated_text_english"] = text_result["translated_text_english"]
    if summary_english:
        combined["summary_english"] = summary_english
    return combined


async def _pdf_batch_extract(
    pdf_path: str,
    *,
    start_page: int,
    stop_page: int,
    dpi: int,
    filename: str | None = None,
    extract_fn: Callable[..., Awaitable[dict]],
) -> dict:
    temp_images: list[str] = []
    started_at = time.perf_counter()
    batch_label = getattr(extract_fn, "__name__", "attachment_extract")
    logger.info(
        "Starting PDF batch extract kind=%s path=%s filename=%s pages=%s-%s dpi=%s",
        batch_label,
        pdf_path,
        filename,
        start_page + 1,
        stop_page,
        dpi,
    )
    try:
        temp_images = _pdf_to_images(pdf_path, start_page=start_page, stop_page=stop_page, dpi=dpi)
        attachments = [{"type": "file", "path": path} for path in temp_images]
        result = await extract_fn(attachments, filename=filename)
        logger.info(
            "Finished PDF batch extract kind=%s path=%s filename=%s pages=%s-%s dpi=%s duration=%.2fs",
            batch_label,
            pdf_path,
            filename,
            start_page + 1,
            stop_page,
            dpi,
            time.perf_counter() - started_at,
        )
        return result
    except Exception:
        logger.exception(
            "PDF batch extract failed kind=%s path=%s filename=%s pages=%s-%s dpi=%s after %.2fs",
            batch_label,
            pdf_path,
            filename,
            start_page + 1,
            stop_page,
            dpi,
            time.perf_counter() - started_at,
        )
        raise
    finally:
        for path in temp_images:
            try:
                os.unlink(path)
            except OSError:
                pass


async def _extract_structured_medical_data_pdf_batch(
    pdf_path: str,
    *,
    start_page: int,
    stop_page: int,
    dpi: int,
    filename: str | None = None,
) -> dict:
    return await _pdf_batch_extract(
        pdf_path, start_page=start_page, stop_page=stop_page, dpi=dpi,
        filename=filename, extract_fn=_extract_structured_medical_data_from_attachments,
    )


async def _extract_document_text_from_pdf_batch(
    pdf_path: str,
    *,
    start_page: int,
    stop_page: int,
    dpi: int,
    filename: str | None = None,
) -> dict:
    return await _pdf_batch_extract(
        pdf_path, start_page=start_page, stop_page=stop_page, dpi=dpi,
        filename=filename, extract_fn=_extract_document_text_from_attachments,
    )


async def _pdf_range_with_retries(
    pdf_path: str,
    *,
    start_page: int,
    stop_page: int,
    dpi: int = OCR_PDF_RENDER_DPI,
    filename: str | None = None,
    batch_fn: Callable[..., Awaitable[dict]],
    merge_fn: Callable[[list[dict]], dict],
    post_process_fn: Callable[[dict, int], dict] | None = None,
    label: str,
) -> dict:
    page_count = stop_page - start_page
    started_at = time.perf_counter()
    logger.info(
        "%s range start path=%s filename=%s pages=%s-%s dpi=%s page_count=%s",
        label,
        pdf_path,
        filename,
        start_page + 1,
        stop_page,
        dpi,
        page_count,
    )

    async def _retry(**overrides) -> dict:
        logger.info(
            "%s retry scheduled path=%s filename=%s pages=%s-%s dpi=%s overrides=%s delay=%ss",
            label,
            pdf_path,
            filename,
            start_page + 1,
            stop_page,
            dpi,
            overrides,
            OCR_RETRY_DELAY,
        )
        await asyncio.sleep(OCR_RETRY_DELAY)
        kw = dict(start_page=start_page, stop_page=stop_page, dpi=dpi)
        kw.update(overrides)
        return await _pdf_range_with_retries(
            pdf_path,
            filename=filename,
            batch_fn=batch_fn,
            merge_fn=merge_fn,
            post_process_fn=post_process_fn,
            label=label,
            **kw,
        )

    try:
        result = await batch_fn(
            pdf_path,
            start_page=start_page,
            stop_page=stop_page,
            dpi=dpi,
            filename=filename,
        )
        logger.info(
            "%s range success path=%s filename=%s pages=%s-%s dpi=%s duration=%.2fs",
            label,
            pdf_path,
            filename,
            start_page + 1,
            stop_page,
            dpi,
            time.perf_counter() - started_at,
        )
        return post_process_fn(result, start_page) if post_process_fn else result
    except Exception as exc:
        retry_reason = _retryable_pdf_error_reason(exc)
        if not _is_retryable_pdf_error(exc):
            logger.exception(
                "%s extraction failed for %s (filename=%s, pages=%s-%s, dpi=%s)",
                label, pdf_path, filename, start_page + 1, stop_page, dpi,
            )
            raise

        if page_count > 1 and (_is_request_timeout_error(exc) or _is_rate_limited_error(exc)):
            logger.warning(
                "%s batch request failed for %s (filename=%s, pages=%s-%s, dpi=%s, reason=%s); falling back to single-page retries",
                label, pdf_path, filename, start_page + 1, stop_page, dpi, retry_reason,
            )
            results = [await _retry(start_page=p, stop_page=p + 1) for p in range(start_page, stop_page)]
            return merge_fn(results)

        if page_count > 1:
            logger.warning(
                "%s batch request failed for %s (filename=%s, pages=%s-%s, dpi=%s, reason=%s); splitting batch",
                label, pdf_path, filename, start_page + 1, stop_page, dpi, retry_reason,
            )
            midpoint = start_page + math.ceil(page_count / 2)
            left = await _retry(start_page=start_page, stop_page=midpoint)
            right = await _retry(start_page=midpoint, stop_page=stop_page)
            return merge_fn([left, right])

        smaller_dpi = max(OCR_PDF_MIN_RENDER_DPI, dpi - 24)
        if smaller_dpi != dpi:
            logger.warning(
                "%s page request failed for %s (filename=%s, page=%s, dpi=%s, reason=%s); retrying at dpi=%s",
                label, pdf_path, filename, start_page + 1, dpi, retry_reason, smaller_dpi,
            )
            return await _retry(dpi=smaller_dpi)

        logger.exception(
            "%s extraction failed at minimum DPI for %s (filename=%s, page=%s, dpi=%s)",
            label, pdf_path, filename, start_page + 1, dpi,
        )
        raise


async def _extract_structured_medical_data_pdf_range(pdf_path: str, **kwargs) -> dict:
    return await _pdf_range_with_retries(
        pdf_path,
        batch_fn=_extract_structured_medical_data_pdf_batch,
        merge_fn=_merge_structured_medical_results,
        post_process_fn=_offset_result_page_numbers,
        label="Structured medical PDF",
        **kwargs,
    )


async def _extract_document_text_from_pdf_range(pdf_path: str, **kwargs) -> dict:
    return await _pdf_range_with_retries(
        pdf_path,
        batch_fn=_extract_document_text_from_pdf_batch,
        merge_fn=_merge_document_text_results,
        label="OCR text",
        **kwargs,
    )


async def _run_pdf_batches_parallel(
    pdf_path: str,
    *,
    page_count: int,
    filename: str | None,
    range_fn: Callable[..., Awaitable[dict]],
    merge_fn: Callable[[list[dict]], dict],
) -> dict:
    semaphore = asyncio.Semaphore(OCR_PDF_BATCH_CONCURRENCY)

    async def _run_single_batch(start_page: int, stop_page: int) -> dict:
        async with semaphore:
            return await range_fn(
                pdf_path,
                start_page=start_page,
                stop_page=stop_page,
                filename=filename,
            )

    tasks = [
        asyncio.create_task(
            _run_single_batch(start_page, min(start_page + OCR_PDF_BATCH_SIZE, page_count))
        )
        for start_page in range(0, page_count, OCR_PDF_BATCH_SIZE)
    ]
    return merge_fn(await asyncio.gather(*tasks))


async def _extract_structured_medical_data_from_pdf(pdf_path: str, *, filename: str | None = None) -> dict:
    with fitz.open(pdf_path) as doc:
        page_count = doc.page_count

    started_at = time.perf_counter()
    logger.info("Structured medical PDF start path=%s filename=%s page_count=%s", pdf_path, filename, page_count)
    result = await _run_pdf_batches_parallel(
        pdf_path,
        page_count=page_count,
        filename=filename,
        range_fn=_extract_structured_medical_data_pdf_range,
        merge_fn=_merge_structured_medical_results,
    )

    logger.info(
        "Structured medical PDF finished path=%s filename=%s page_count=%s duration=%.2fs",
        pdf_path,
        filename,
        page_count,
        time.perf_counter() - started_at,
    )
    return result


async def _extract_document_text_from_pdf(pdf_path: str, *, filename: str | None = None) -> dict:
    with fitz.open(pdf_path) as doc:
        page_count = doc.page_count

    started_at = time.perf_counter()
    logger.info("Document text PDF start path=%s filename=%s page_count=%s", pdf_path, filename, page_count)
    result = await _run_pdf_batches_parallel(
        pdf_path,
        page_count=page_count,
        filename=filename,
        range_fn=_extract_document_text_from_pdf_range,
        merge_fn=_merge_document_text_results,
    )

    logger.info(
        "Document text PDF finished path=%s filename=%s page_count=%s duration=%.2fs",
        pdf_path,
        filename,
        page_count,
        time.perf_counter() - started_at,
    )
    return result


async def _extract_structured_medical_data(file_path: str, *, filename: str | None = None) -> dict:
    started_at = time.perf_counter()
    logger.info("Medical extraction start path=%s filename=%s", file_path, filename)
    if Path(file_path).suffix.lower() == ".pdf":
        result = await _extract_structured_medical_data_from_pdf(file_path, filename=filename)
        logger.info("Medical extraction finished path=%s filename=%s duration=%.2fs", file_path, filename, time.perf_counter() - started_at)
        return result

    attachments = [{"type": "file", "path": file_path}]
    result = await _extract_structured_medical_data_from_attachments(attachments, filename=filename)
    logger.info("Medical extraction finished path=%s filename=%s duration=%.2fs", file_path, filename, time.perf_counter() - started_at)
    return result


async def _extract_document_text(file_path: str, *, filename: str | None = None) -> dict:
    started_at = time.perf_counter()
    logger.info("Document text extraction start path=%s filename=%s", file_path, filename)
    if Path(file_path).suffix.lower() == ".pdf":
        result = await _extract_document_text_from_pdf(file_path, filename=filename)
        logger.info("Document text extraction finished path=%s filename=%s duration=%.2fs", file_path, filename, time.perf_counter() - started_at)
        return result

    attachments = [{"type": "file", "path": file_path}]
    result = await _extract_document_text_from_attachments(attachments, filename=filename)
    logger.info("Document text extraction finished path=%s filename=%s duration=%.2fs", file_path, filename, time.perf_counter() - started_at)
    return result


async def ocr_extract(file_path: str, *, filename: str | None = None) -> dict:
    """Send a lab file to Copilot SDK for OCR extraction.

    For images, sends the file directly. For PDFs, converts each page
    to a PNG image first so the vision model can read the content.

    Args:
        file_path: Absolute path to the uploaded file (PDF or image).

    Returns:
        Parsed JSON dict with structured medical data plus OCR text and summary when available.
    """
    started_at = time.perf_counter()
    logger.info("OCR pipeline start path=%s filename=%s", file_path, filename)
    medical_task = asyncio.create_task(_extract_structured_medical_data(file_path, filename=filename))
    text_task = asyncio.create_task(_extract_document_text(file_path, filename=filename))

    medical_result_or_error, text_result_or_error = await asyncio.gather(
        medical_task,
        text_task,
        return_exceptions=True,
    )

    if isinstance(medical_result_or_error, BaseException):
        logger.warning(
            "OCR pipeline medical extraction failed path=%s filename=%s after %.2fs error=%s",
            file_path,
            filename,
            time.perf_counter() - started_at,
            medical_result_or_error,
        )
        raise medical_result_or_error

    medical_result = medical_result_or_error

    usable_text_result: dict | None
    if isinstance(text_result_or_error, BaseException):
        logger.exception(
            "Document text extraction failed for %s (filename=%s)",
            file_path,
            filename,
        )
        usable_text_result = None
    else:
        usable_text_result = text_result_or_error

    summary_english: str | None = None
    try:
        summary_english = await _generate_medical_summary(
            medical_result,
            usable_text_result,
            filename=filename,
        )
    except Exception:
        logger.exception(
            "Medical summary generation failed for %s (filename=%s)",
            file_path,
            filename,
        )

    logger.info(
        "OCR pipeline finished path=%s filename=%s measurements=%s raw_text=%s summary=%s duration=%.2fs",
        file_path,
        filename,
        len(medical_result.get("measurements", [])),
        bool((usable_text_result or {}).get("raw_text")),
        bool(summary_english),
        time.perf_counter() - started_at,
    )
    return _combine_ocr_outputs(medical_result, usable_text_result, summary_english)


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
- If the source label is in another language, including Czech, prefer the English \
canonical medical name instead of preserving the source-language wording.
- Prefer concise English medical names such as \"White Blood Cell (WBC) Count\" \
or \"Platelet Count\" over local-language labels.
- Treat abbreviations like \"Abs\" carefully: in immunology or assay contexts it often \
means \"Absorbance\", not \"Absolute\". Only expand it to \"Absolute\" when the \
source label clearly indicates an absolute count.

Return ONLY valid JSON: a mapping object where keys are the original new names \
and values are the canonical names.
Example: {"Lymfocyty -abs.počet": "Absolute Lymphocyte Count", "Hemoglobin": "Hemoglobin"}
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

    async def _normalize_batch(batch_names: list[str]) -> dict[str, str]:
        user_text = "EXISTING canonical marker names:\n"
        if existing_canonical:
            for n in existing_canonical:
                user_text += f"- {n}\n"
        else:
            user_text += "(none yet)\n"
        user_text += "\nNEW marker names to normalize:\n"
        for n in batch_names:
            user_text += f"- {n}\n"

        raw = await _ask(NORMALIZE_SYSTEM_PROMPT, user_text)

        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1]
        if raw.endswith("```"):
            raw = raw.rsplit("```", 1)[0]

        try:
            mapping = json.loads(raw.strip())
        except (json.JSONDecodeError, ValueError):
            mapping = {n: n for n in batch_names}

        for n in batch_names:
            if n not in mapping:
                mapping[n] = n
        return mapping

    batches = [
        new_names[index:index + MARKER_NORMALIZATION_BATCH_SIZE]
        for index in range(0, len(new_names), MARKER_NORMALIZATION_BATCH_SIZE)
    ]
    semaphore = asyncio.Semaphore(MARKER_NORMALIZATION_CONCURRENCY)

    async def _run_batch(batch_names: list[str]) -> dict[str, str]:
        async with semaphore:
            return await _normalize_batch(batch_names)

    merged_mapping: dict[str, str] = {}
    for batch_mapping in await asyncio.gather(*[_run_batch(batch) for batch in batches]):
        merged_mapping.update(batch_mapping)

    return merged_mapping


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
    """Ask Copilot to explain a set of lab markers."""
    user_text = "Please explain these lab results:\n\n"
    for m in markers:
        value = m.get("qualitative_value")
        if value is None:
            value = m.get("value")
        line = f"- **{m['marker_name']}**: {value}"
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
