"""Copilot-backed document extraction pipeline."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import tempfile
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import fitz

from illdashboard.config import settings
from illdashboard.copilot import mistral_client
from illdashboard.copilot.client import (
    COPILOT_REQUEST_TIMEOUT,
    get_copilot_request_load,
)
from illdashboard.copilot.client import (
    _ask_json as copilot_ask_json,
)
from illdashboard.services import file_types

logger = logging.getLogger(__name__)

# Earlier 300s validation forced single-page OCR. With the larger request
# budget and the existing retry path still ready to split or lower DPI, start
# from two-page batches again to reduce request overhead.
OCR_PDF_BATCH_SIZE = 2
OCR_PDF_MAX_BATCH_SIZE = 2
OCR_PDF_RENDER_DPI = 144
OCR_PDF_MIN_RENDER_DPI = 72
OCR_ASK_TIMEOUT = COPILOT_REQUEST_TIMEOUT
OCR_RETRY_DELAY = 3
OCR_PDF_BATCH_CONCURRENCY = 4
OCR_PDF_BACKPRESSURE_BATCH_CONCURRENCY = 2
OCR_PDF_BACKPRESSURE_MIN_PAGE_COUNT = 8
OCR_PDF_BACKPRESSURE_QUEUE_THRESHOLD = 6
A4_WIDTH_INCHES = 210 / 25.4
A4_HEIGHT_INCHES = 297 / 25.4


def _unlink_paths(paths: list[str]) -> None:
    for path in paths:
        try:
            os.unlink(path)
        except OSError:
            pass


def _a4_pixel_bounds(*, dpi: int, landscape: bool) -> tuple[int, int]:
    short_edge = max(1, round(A4_WIDTH_INCHES * dpi))
    long_edge = max(1, round(A4_HEIGHT_INCHES * dpi))
    return (long_edge, short_edge) if landscape else (short_edge, long_edge)


def _resize_scale_for_bounds(width: int, height: int, *, max_width: int, max_height: int) -> float:
    if width <= 0 or height <= 0:
        return 1.0
    return min(1.0, max_width / width, max_height / height)


def _a4_capped_dimensions(width: int, height: int, *, dpi: int) -> tuple[int, int]:
    max_width, max_height = _a4_pixel_bounds(dpi=dpi, landscape=width >= height)
    resize_scale = _resize_scale_for_bounds(width, height, max_width=max_width, max_height=max_height)
    return (
        max(1, min(max_width, round(width * resize_scale))),
        max(1, min(max_height, round(height * resize_scale))),
    )


def _new_temp_png_path() -> str:
    temp_file = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    temp_file.close()
    return temp_file.name


def _save_pixmap_to_temp_png(pixmap: fitz.Pixmap) -> str:
    temp_path = _new_temp_png_path()
    pixmap.save(temp_path)
    return temp_path


class _PdfRenderCache:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self._lock = asyncio.Lock()
        self._images_by_batch: dict[tuple[int, int, int], list[str]] = {}

    async def attachments_for_range(self, *, start_page: int, stop_page: int, dpi: int) -> list[dict]:
        key = (start_page, stop_page, dpi)
        async with self._lock:
            paths = self._images_by_batch.get(key)
            if paths is None:
                paths = _pdf_to_images(self.pdf_path, start_page=start_page, stop_page=stop_page, dpi=dpi)
                self._images_by_batch[key] = paths
        return [{"type": "file", "path": path} for path in paths]

    async def aclose(self) -> None:
        async with self._lock:
            cached_paths = [path for paths in self._images_by_batch.values() for path in paths]
            self._images_by_batch.clear()

        _unlink_paths(cached_paths)


class _ImageRenderCache:
    def __init__(self, image_path: str):
        self.image_path = image_path
        self._lock = asyncio.Lock()
        self._images_by_dpi: dict[int, str] = {}

    async def attachments_for_dpi(self, *, dpi: int) -> list[dict]:
        async with self._lock:
            path = self._images_by_dpi.get(dpi)
            if path is None:
                path = _image_to_png(self.image_path, dpi=dpi)
                self._images_by_dpi[dpi] = path
        return [{"type": "file", "path": path}]

    async def aclose(self) -> None:
        async with self._lock:
            cached_paths = list(self._images_by_dpi.values())
            self._images_by_dpi.clear()

        _unlink_paths(cached_paths)


@dataclass(frozen=True)
class _PdfBatchPlan:
    batch_size: int
    concurrency: int
    queue_depth: int
    active_requests: int


@dataclass(frozen=True)
class _ExtractionBatch:
    batch_index: int
    start_page: int
    stop_page: int
    result: dict


def _select_pdf_batch_plan(page_count: int) -> _PdfBatchPlan:
    queue_depth, active_requests = get_copilot_request_load()
    batch_size = OCR_PDF_BATCH_SIZE
    concurrency = OCR_PDF_BATCH_CONCURRENCY

    if page_count >= OCR_PDF_BACKPRESSURE_MIN_PAGE_COUNT and queue_depth >= OCR_PDF_BACKPRESSURE_QUEUE_THRESHOLD:
        batch_size = OCR_PDF_MAX_BATCH_SIZE
        concurrency = OCR_PDF_BACKPRESSURE_BATCH_CONCURRENCY

    return _PdfBatchPlan(
        batch_size=batch_size,
        concurrency=concurrency,
        queue_depth=queue_depth,
        active_requests=active_requests,
    )


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


MEDICAL_OCR_SYSTEM_PROMPT = """\
You are a medical lab report extraction assistant. The user will provide an image or \
PDF of a document. Your job is to:

1. Identify every measured lab marker (e.g. Hemoglobin, WBC, Glucose…).
    This includes qualitative serology and immunology markers reported as positive/negative/reactive/not detected.
2. Identify the lab/report source when possible (for example Synlab, Jaeger, Unilabs).
3. For each marker return a JSON array of objects with keys:
    "marker_name", "value" (number for numeric results, string for qualitative
    results), "unit", "reference_low" (numeric or null), "reference_high"
    (numeric or null), "measured_at" (ISO date string or null), "page_number"
    (integer, 1-indexed – which page/image the value appears on).
4. Also return "lab_date" (ISO date string or null) for the report date.
5. Also return "source" as a short raw source/provider name string, or null if unclear.

If the document is not a lab report, return an empty "measurements" array instead of inventing measurements.

CRITICAL rules for values:
- Use a JSON number in "value" when the report shows a numeric result.
 - Use a short JSON string in "value" when the report shows a qualitative
   result such as "positive", "negative", "reactive", "non-reactive",
   "detected", or "not detected".
- Do NOT omit a marker just because its value is qualitative.
- "reference_low" and "reference_high" MUST be JSON numbers or null.
- Use a dot (.) as the decimal separator, never a comma or space. E.g. 0.1, not "0,1" or "0 1".
- Do NOT insert spaces into numbers. E.g. 1500, not "1 500".
- If a value is less than 1, include the leading zero: 0.1, not .1.
- Read decimal points carefully – "0.1" (zero point one) is very different from "1".

CRITICAL extraction rules:
 - Do not skip dense semicolon-separated or comma-separated sections. Split
   every assay/result pair into its own measurement object.
 - Serology and immunology pages often list many markers inline on one line.
   Extract every marker, even when many share the same sentence.
 - Keep marker names specific. For example, include the organism plus antibody
   class such as "Chlamydia psittaci IgG" rather than only "IgG".

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


MISTRAL_TRANSLATE_TEXT_SYSTEM_PROMPT = """\
You translate OCR text into English.

Rules:
- Preserve the original ordering and meaning of the document.
- Keep marker names, numbers, ranges, and units exact.
- If the source text is already English, return it unchanged except for obvious OCR cleanup.
- Return only the translated text and nothing else.\
"""


MISTRAL_MEDICAL_ANNOTATION_FORMAT = {
    "type": "json_schema",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "lab_date": {
                "type": ["string", "null"],
                "description": "Report date as an ISO date string when visible, otherwise null.",
            },
            "source": {
                "type": ["string", "null"],
                "description": "Short raw lab or provider name when visible, otherwise null.",
            },
            "measurements": {
                "type": "array",
                "description": "Every measured lab value found in the document, including qualitative results.",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "marker_name": {
                            "type": "string",
                            "description": (
                                "Specific lab marker name, including organism and antibody class when present."
                            ),
                        },
                        "value": {
                            "description": "Numeric results as numbers and qualitative results as short strings.",
                            "anyOf": [{"type": "number"}, {"type": "string"}],
                        },
                        "unit": {
                            "type": ["string", "null"],
                            "description": "Reported unit or null when none is shown.",
                        },
                        "reference_low": {
                            "type": ["number", "null"],
                            "description": "Lower reference bound when present, otherwise null.",
                        },
                        "reference_high": {
                            "type": ["number", "null"],
                            "description": "Upper reference bound when present, otherwise null.",
                        },
                        "measured_at": {
                            "type": ["string", "null"],
                            "description": "ISO date string for the measurement when explicitly shown, otherwise null.",
                        },
                        "page_number": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "1-indexed page number within the attached batch.",
                        },
                    },
                    "required": [
                        "marker_name",
                        "value",
                        "unit",
                        "reference_low",
                        "reference_high",
                        "measured_at",
                        "page_number",
                    ],
                },
            },
        },
        "required": ["lab_date", "source", "measurements"],
    },
}


MISTRAL_MEDICAL_ANNOTATION_PROMPT = """\
Extract all lab values from this lab report.

Requirements:
- Include every measured marker, including qualitative serology and immunology results.
- Keep marker names specific.
- Use a JSON number for numeric values and a short string for qualitative values.
- Set page_number relative to the attached batch, starting from 1.
- Return an empty measurements array when the document is not a lab report.\
"""


MEDICAL_SUMMARY_SYSTEM_PROMPT = """\
You are a medical document summarization assistant.

The user will provide:
1. Raw OCR text from a document in its original language.
2. The original filename.

Your job is to:
1. Write a short factual English summary in 2-4 sentences.
2. Extract "lab_date" as an ISO date string or null when the report date is not clear.
3. Extract "source" as a short raw lab/provider name string or null when unclear.

Rules:
- Use only the provided raw text and filename.
- Do not rely on any structured measurements or translated text.
- Keep the summary factual and concise.
- If the document is not clearly a lab report, briefly describe what kind of document it seems to be.
- Do not add generic cautions or boilerplate.

Return ONLY valid JSON: {"summary_english": "...", "lab_date": "...", "source": "..."}.
Do not include any commentary outside the JSON.\
"""


MEDICAL_TEXT_SYSTEM_PROMPT = """\
You are a medical lab report extraction assistant.

The user will provide:
1. Raw text copied from a plain-text or Markdown document.
2. The original filename.

Your job is to:
1. Identify every measured lab marker.
2. Extract "lab_date" as an ISO date string or null when the report date is not clear.
3. Extract "source" as a short raw lab/provider name string or null when unclear.
4. Return each measurement with keys:
   "marker_name", "value", "unit", "reference_low", "reference_high",
   "measured_at", and "page_number".

Rules:
- Use a JSON number in "value" for numeric results and a short JSON string for qualitative results.
- "reference_low" and "reference_high" must be JSON numbers or null.
- If the document is not a lab report, return an empty "measurements" array.
- Because the source is a text document, always set "page_number" to 1.
- Use only the provided raw text and filename.

Return ONLY valid JSON: {"lab_date": "...", "source": "...", "measurements": [...]}.
Do not include any commentary outside the JSON.\
"""


TEXT_DOCUMENT_TRANSLATION_SYSTEM_PROMPT = """\
You translate plain-text or Markdown documents into English.

The user will provide raw text and the original filename.

Rules:
- Preserve headings, bullets, tables, document order, and line breaks when practical.
- Keep marker names, numbers, ranges, and units exact.
- If the source text is already English, return it unchanged except for obvious cleanup.

Return ONLY valid JSON: {"translated_text_english": "..."}.
Do not include any commentary outside the JSON.\
"""


def _read_text_document(file_path: str) -> str:
    # Plain text and Markdown uploads already contain machine-readable content,
    # so keep their native bytes instead of forcing them through image OCR.
    return Path(file_path).read_text(encoding="utf-8-sig")


def _normalize_text_document(raw_text: str) -> str | None:
    normalized = raw_text.replace("\r\n", "\n").strip()
    return normalized or None


def _text_document_request_context(filename: str | None) -> str:
    return _mistral_request_context(filename)


async def _text_document_json_request(
    system_prompt: str,
    user_prompt: str,
    *,
    request_name: str,
    request_context: str = "",
) -> dict:
    # Text/Markdown uploads bypass OCR, but they should still respect the active
    # extraction provider instead of silently depending on the other backend.
    if settings.EXTRACTION_PROVIDER == "mistral":
        return await mistral_client._ask_json(
            system_prompt,
            user_prompt,
            request_name=request_name,
            request_context=request_context,
            timeout=OCR_ASK_TIMEOUT,
        )
    return await copilot_ask_json(
        system_prompt,
        user_prompt,
        request_name=request_name,
        request_context=request_context,
        timeout=OCR_ASK_TIMEOUT,
    )


async def _translate_text_document_to_english(
    raw_text: str,
    *,
    filename: str | None = None,
    request_context: str = "",
) -> str | None:
    payload = {"filename": filename, "raw_text": raw_text}
    parsed = await _text_document_json_request(
        TEXT_DOCUMENT_TRANSLATION_SYSTEM_PROMPT,
        json.dumps(payload, ensure_ascii=False, indent=2),
        request_name="document_text_extraction",
        request_context=request_context,
    )
    translated = parsed.get("translated_text_english")
    return translated.strip() if isinstance(translated, str) and translated.strip() else None


async def _extract_text_from_text_document(
    file_path: str,
    *,
    filename: str | None = None,
    request_context: str = "",
) -> dict:
    raw_text = _normalize_text_document(_read_text_document(file_path))
    if raw_text is None:
        return {"raw_text": None, "translated_text_english": None}
    translated = await _translate_text_document_to_english(
        raw_text,
        filename=filename,
        request_context=request_context,
    )
    return {
        "raw_text": raw_text,
        "translated_text_english": translated,
    }


async def _extract_structured_medical_data_from_text_document(
    file_path: str,
    *,
    filename: str | None = None,
    request_context: str = "",
) -> dict:
    raw_text = _normalize_text_document(_read_text_document(file_path))
    if raw_text is None:
        return {"lab_date": None, "source": None, "measurements": []}

    payload = {"filename": filename, "raw_text": raw_text}
    parsed = await _text_document_json_request(
        MEDICAL_TEXT_SYSTEM_PROMPT,
        json.dumps(payload, ensure_ascii=False, indent=2),
        request_name="structured_medical_extraction",
        request_context=request_context,
    )
    result = _normalized_medical_annotation(parsed)
    result["measurements"] = [{**measurement, "page_number": 1} for measurement in result["measurements"]]
    return result


def _render_pdf_page_to_temp_png(page: fitz.Page, *, dpi: int) -> str:
    requested_width = max(1, round(page.rect.width * dpi / 72))
    requested_height = max(1, round(page.rect.height * dpi / 72))
    target_width, target_height = _a4_capped_dimensions(requested_width, requested_height, dpi=dpi)
    # Cap every OCR attachment to an A4-sized pixel budget so retries reduce
    # payload size predictably without warping the underlying page geometry.
    render_scale = min(target_width / requested_width, target_height / requested_height) * (dpi / 72)
    pixmap = page.get_pixmap(matrix=fitz.Matrix(render_scale, render_scale), colorspace=fitz.csGRAY, alpha=False)
    return _save_pixmap_to_temp_png(pixmap)


def _image_to_png(image_path: str, *, dpi: int) -> str:
    source = fitz.Pixmap(image_path)
    source_width = source.width
    source_height = source.height
    target_width, target_height = _a4_capped_dimensions(source_width, source_height, dpi=dpi)
    resize_scale = min(target_width / source_width, target_height / source_height)

    with fitz.open(image_path) as document:
        page = document.load_page(0)
        native_scale = min(source_width / page.rect.width, source_height / page.rect.height)
        pixmap = page.get_pixmap(
            matrix=fitz.Matrix(native_scale * resize_scale, native_scale * resize_scale),
            colorspace=fitz.csGRAY,
            alpha=False,
        )

    logger.info(
        "Rendered image path=%s dpi=%s source_pixels=%sx%s output_pixels=%sx%s",
        image_path,
        dpi,
        source_width,
        source_height,
        pixmap.width,
        pixmap.height,
    )
    return _save_pixmap_to_temp_png(pixmap)


def _pdf_to_images(
    pdf_path: str, *, start_page: int = 0, stop_page: int | None = None, dpi: int = OCR_PDF_RENDER_DPI
) -> list[str]:
    """Convert selected PDF pages to temporary PNG files."""
    document = fitz.open(pdf_path)
    paths: list[str] = []
    page_stop = stop_page if stop_page is not None else document.page_count
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
            page = document.load_page(page_index)
            paths.append(_render_pdf_page_to_temp_png(page, dpi=dpi))
    finally:
        document.close()

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
    attachments: list[dict],
    *,
    filename: str | None = None,
    request_context: str = "",
) -> dict:
    prompt = "Extract all lab values from the attached file, including qualitative serology and immunology results."
    if filename:
        prompt = f"Original filename: {filename}\n\n{prompt}"

    return await copilot_ask_json(
        MEDICAL_OCR_SYSTEM_PROMPT,
        prompt,
        attachments=attachments,
        timeout=OCR_ASK_TIMEOUT,
        request_name="structured_medical_extraction",
        request_context=request_context,
    )


async def _extract_document_text_from_attachments(
    attachments: list[dict],
    *,
    filename: str | None = None,
    request_context: str = "",
) -> dict:
    prompt = "Transcribe all visible text from the attached document and translate it to English."
    if filename:
        prompt = f"Original filename: {filename}\n\n{prompt}"

    return await copilot_ask_json(
        TEXT_OCR_SYSTEM_PROMPT,
        prompt,
        attachments=attachments,
        timeout=OCR_ASK_TIMEOUT,
        request_name="document_text_extraction",
        request_context=request_context,
    )


def _normalized_medical_annotation(annotation: dict) -> dict:
    measurements = annotation.get("measurements")
    if not isinstance(measurements, list):
        measurements = []
    return {
        "lab_date": annotation.get("lab_date"),
        "source": annotation.get("source"),
        "measurements": [measurement for measurement in measurements if isinstance(measurement, dict)],
    }


async def _extract_structured_medical_data_from_file_mistral(
    file_path: str,
    *,
    start_page: int | None = None,
    stop_page: int | None = None,
    filename: str | None = None,
    request_context: str = "",
) -> dict:
    prompt = MISTRAL_MEDICAL_ANNOTATION_PROMPT
    if filename:
        prompt = f"Original filename: {filename}\n\n{prompt}"
    result = await mistral_client.process_ocr_file(
        file_path,
        start_page=start_page,
        stop_page=stop_page,
        request_name="structured_medical_extraction",
        request_context=request_context,
        document_annotation_format=MISTRAL_MEDICAL_ANNOTATION_FORMAT,
        document_annotation_prompt=prompt,
        timeout=OCR_ASK_TIMEOUT,
    )
    return _normalized_medical_annotation(mistral_client.document_annotation(result))


async def _translate_document_text_to_english_mistral(
    raw_text: str,
    *,
    filename: str | None = None,
    request_context: str = "",
) -> str:
    prompt = raw_text.strip()
    if filename:
        prompt = f"Original filename: {filename}\n\nOCR text:\n{prompt}"
    translated = await mistral_client.ask_text(
        MISTRAL_TRANSLATE_TEXT_SYSTEM_PROMPT,
        prompt,
        request_name="document_text_translation",
        request_context=request_context,
        timeout=OCR_ASK_TIMEOUT,
    )
    cleaned = translated.strip()
    if not cleaned:
        raise RuntimeError("Mistral translation returned empty text")
    return cleaned


async def _extract_document_text_from_file_mistral(
    file_path: str,
    *,
    start_page: int | None = None,
    stop_page: int | None = None,
    filename: str | None = None,
    request_context: str = "",
) -> dict:
    result = await mistral_client.process_ocr_file(
        file_path,
        start_page=start_page,
        stop_page=stop_page,
        request_name="document_text_extraction",
        request_context=request_context,
        timeout=OCR_ASK_TIMEOUT,
    )
    raw_text = mistral_client.document_markdown_text(result).strip()
    if not raw_text:
        return {"raw_text": None, "translated_text_english": None}
    translated = await _translate_document_text_to_english_mistral(
        raw_text,
        filename=filename,
        request_context=request_context,
    )
    return {
        "raw_text": raw_text,
        "translated_text_english": translated,
    }


async def _generate_medical_summary(raw_text: str | None, *, filename: str | None = None) -> dict[str, str | None]:
    started_at = time.perf_counter()
    logger.info(
        "Medical summary start filename=%s raw_text=%s",
        filename,
        bool(raw_text),
    )
    if not raw_text:
        return {"summary_english": None, "lab_date": None, "source": None}
    user_payload = {
        "filename": filename,
        "raw_text": raw_text,
    }
    parsed = await copilot_ask_json(
        MEDICAL_SUMMARY_SYSTEM_PROMPT,
        json.dumps(user_payload, ensure_ascii=False, indent=2),
        request_name="medical_summary",
    )
    summary = parsed.get("summary_english")
    cleaned_summary = summary.strip() if isinstance(summary, str) and summary.strip() else None
    lab_date = parsed.get("lab_date")
    cleaned_lab_date = lab_date.strip() if isinstance(lab_date, str) and lab_date.strip() else None
    source = parsed.get("source")
    cleaned_source = source.strip() if isinstance(source, str) and source.strip() else None
    logger.info(
        "Medical summary finished filename=%s has_summary=%s has_lab_date=%s has_source=%s duration=%.2fs",
        filename,
        bool(cleaned_summary),
        bool(cleaned_lab_date),
        bool(cleaned_source),
        time.perf_counter() - started_at,
    )
    return {
        "summary_english": cleaned_summary,
        "lab_date": cleaned_lab_date,
        "source": cleaned_source,
    }


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


def _combine_ocr_outputs(medical_result: dict, text_result: dict | None, summary_result: dict | None) -> dict:
    combined = {
        "lab_date": medical_result.get("lab_date") or (summary_result or {}).get("lab_date"),
        "source": medical_result.get("source") or (summary_result or {}).get("source"),
        "measurements": medical_result.get("measurements", []),
    }
    if text_result and text_result.get("raw_text"):
        combined["raw_text"] = text_result["raw_text"]
    if text_result and text_result.get("translated_text_english"):
        combined["translated_text_english"] = text_result["translated_text_english"]
    if summary_result and summary_result.get("summary_english"):
        combined["summary_english"] = summary_result["summary_english"]
    return combined


def _pdf_page_count(file_path: str) -> int:
    with fitz.open(file_path) as document:
        return document.page_count


def _mistral_request_context(
    filename: str | None,
    *,
    start_page: int | None = None,
    stop_page: int | None = None,
) -> str:
    label = filename or "document"
    if start_page is not None and stop_page is not None:
        return f"{label}:p{start_page + 1}-{stop_page}"
    return f"{label}:full"


@dataclass(frozen=True)
class _ExtractionKind:
    """Configuration for a type of OCR extraction."""

    label: str
    extract_fn: Callable[..., Awaitable[dict]]
    merge_fn: Callable[[list[dict]], dict]
    post_process_fn: Callable[[dict, int], dict] | None = None


async def _pdf_batch_extract(
    pdf_path: str,
    kind: _ExtractionKind,
    *,
    start_page: int,
    stop_page: int,
    dpi: int,
    filename: str | None = None,
    render_cache: _PdfRenderCache | None = None,
) -> dict:
    temp_images: list[str] = []
    started_at = time.perf_counter()
    logger.info(
        "PDF batch extract start kind=%s path=%s filename=%s pages=%s-%s dpi=%s",
        kind.label,
        pdf_path,
        filename,
        start_page + 1,
        stop_page,
        dpi,
    )

    try:
        if render_cache is None:
            temp_images = _pdf_to_images(pdf_path, start_page=start_page, stop_page=stop_page, dpi=dpi)
            attachments = [{"type": "file", "path": path} for path in temp_images]
        else:
            attachments = await render_cache.attachments_for_range(start_page=start_page, stop_page=stop_page, dpi=dpi)

        batch_context = f"{filename}:p{start_page + 1}-{stop_page}:{dpi}dpi"
        result = await kind.extract_fn(attachments, filename=filename, request_context=batch_context)
        logger.info(
            "PDF batch extract done kind=%s path=%s filename=%s pages=%s-%s dpi=%s duration=%.2fs",
            kind.label,
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
            kind.label,
            pdf_path,
            filename,
            start_page + 1,
            stop_page,
            dpi,
            time.perf_counter() - started_at,
        )
        raise
    finally:
        _unlink_paths(temp_images)


async def _image_extract(
    image_path: str,
    kind: _ExtractionKind,
    *,
    dpi: int,
    filename: str | None = None,
    render_cache: _ImageRenderCache | None = None,
) -> dict:
    temp_images: list[str] = []
    started_at = time.perf_counter()
    logger.info("%s image extract start path=%s filename=%s dpi=%s", kind.label, image_path, filename, dpi)

    try:
        if render_cache is None:
            temp_images = [_image_to_png(image_path, dpi=dpi)]
            attachments = [{"type": "file", "path": temp_images[0]}]
        else:
            attachments = await render_cache.attachments_for_dpi(dpi=dpi)

        result = await kind.extract_fn(attachments, filename=filename, request_context=f"{filename}:{dpi}dpi")
        logger.info(
            "%s image extract done path=%s filename=%s dpi=%s duration=%.2fs",
            kind.label,
            image_path,
            filename,
            dpi,
            time.perf_counter() - started_at,
        )
        return result
    except Exception:
        logger.exception(
            "%s image extract failed path=%s filename=%s dpi=%s after %.2fs",
            kind.label,
            image_path,
            filename,
            dpi,
            time.perf_counter() - started_at,
        )
        raise
    finally:
        _unlink_paths(temp_images)


async def _pdf_range_with_retries(
    pdf_path: str,
    kind: _ExtractionKind,
    *,
    start_page: int,
    stop_page: int,
    dpi: int = OCR_PDF_RENDER_DPI,
    filename: str | None = None,
    render_cache: _PdfRenderCache | None = None,
) -> dict:
    page_count = stop_page - start_page
    started_at = time.perf_counter()
    logger.info(
        "%s range start path=%s filename=%s pages=%s-%s dpi=%s page_count=%s",
        kind.label,
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
            kind.label,
            pdf_path,
            filename,
            start_page + 1,
            stop_page,
            dpi,
            overrides,
            OCR_RETRY_DELAY,
        )
        await asyncio.sleep(OCR_RETRY_DELAY)
        arguments = {"start_page": start_page, "stop_page": stop_page, "dpi": dpi}
        arguments.update(overrides)
        return await _pdf_range_with_retries(
            pdf_path,
            kind,
            filename=filename,
            render_cache=render_cache,
            **arguments,
        )

    try:
        result = await _pdf_batch_extract(
            pdf_path,
            kind,
            start_page=start_page,
            stop_page=stop_page,
            dpi=dpi,
            filename=filename,
            render_cache=render_cache,
        )
        logger.info(
            "%s range success path=%s filename=%s pages=%s-%s dpi=%s duration=%.2fs",
            kind.label,
            pdf_path,
            filename,
            start_page + 1,
            stop_page,
            dpi,
            time.perf_counter() - started_at,
        )
        return kind.post_process_fn(result, start_page) if kind.post_process_fn else result
    except Exception as exc:
        retry_reason = _retryable_pdf_error_reason(exc)
        if not _is_retryable_pdf_error(exc):
            logger.exception(
                "%s extraction failed for %s (filename=%s, pages=%s-%s, dpi=%s)",
                kind.label,
                pdf_path,
                filename,
                start_page + 1,
                stop_page,
                dpi,
            )
            raise

        if page_count > 1 and (_is_request_timeout_error(exc) or _is_rate_limited_error(exc)):
            logger.warning(
                "%s batch request failed for %s (filename=%s, pages=%s-%s, dpi=%s, reason=%s); "
                "falling back to single-page retries",
                kind.label,
                pdf_path,
                filename,
                start_page + 1,
                stop_page,
                dpi,
                retry_reason,
            )
            results = [await _retry(start_page=page, stop_page=page + 1) for page in range(start_page, stop_page)]
            return kind.merge_fn(results)

        if page_count > 1:
            logger.warning(
                "%s batch request failed for %s (filename=%s, pages=%s-%s, dpi=%s, reason=%s); splitting batch",
                kind.label,
                pdf_path,
                filename,
                start_page + 1,
                stop_page,
                dpi,
                retry_reason,
            )
            midpoint = start_page + math.ceil(page_count / 2)
            left = await _retry(start_page=start_page, stop_page=midpoint)
            right = await _retry(start_page=midpoint, stop_page=stop_page)
            return kind.merge_fn([left, right])

        if dpi != OCR_PDF_MIN_RENDER_DPI:
            logger.warning(
                "%s page request failed for %s (filename=%s, page=%s, dpi=%s, reason=%s); retrying at dpi=%s",
                kind.label,
                pdf_path,
                filename,
                start_page + 1,
                dpi,
                retry_reason,
                OCR_PDF_MIN_RENDER_DPI,
            )
            return await _retry(dpi=OCR_PDF_MIN_RENDER_DPI)

        logger.exception(
            "%s extraction failed at minimum DPI for %s (filename=%s, page=%s, dpi=%s)",
            kind.label,
            pdf_path,
            filename,
            start_page + 1,
            dpi,
        )
        raise


async def _image_with_retries(
    image_path: str,
    kind: _ExtractionKind,
    *,
    dpi: int = OCR_PDF_RENDER_DPI,
    filename: str | None = None,
    render_cache: _ImageRenderCache | None = None,
) -> dict:
    started_at = time.perf_counter()
    logger.info("%s image start path=%s filename=%s dpi=%s", kind.label, image_path, filename, dpi)

    try:
        result = await _image_extract(
            image_path,
            kind,
            dpi=dpi,
            filename=filename,
            render_cache=render_cache,
        )
        logger.info(
            "%s image success path=%s filename=%s dpi=%s duration=%.2fs",
            kind.label,
            image_path,
            filename,
            dpi,
            time.perf_counter() - started_at,
        )
        return kind.post_process_fn(result, 0) if kind.post_process_fn else result
    except Exception as exc:
        retry_reason = _retryable_pdf_error_reason(exc)
        if not _is_retryable_pdf_error(exc):
            logger.exception("%s extraction failed for %s (filename=%s, dpi=%s)", kind.label, image_path, filename, dpi)
            raise

        if dpi != OCR_PDF_MIN_RENDER_DPI:
            logger.warning(
                "%s image request failed for %s (filename=%s, dpi=%s, reason=%s); retrying at dpi=%s",
                kind.label,
                image_path,
                filename,
                dpi,
                retry_reason,
                OCR_PDF_MIN_RENDER_DPI,
            )
            await asyncio.sleep(OCR_RETRY_DELAY)
            return await _image_with_retries(
                image_path,
                kind,
                dpi=OCR_PDF_MIN_RENDER_DPI,
                filename=filename,
                render_cache=render_cache,
            )

        logger.exception(
            "%s extraction failed at minimum DPI for %s (filename=%s, dpi=%s)",
            kind.label,
            image_path,
            filename,
            dpi,
        )
        raise


async def _extract_file(
    file_path: str,
    kind: _ExtractionKind,
    *,
    filename: str | None = None,
    render_cache: _PdfRenderCache | _ImageRenderCache | None = None,
) -> dict:
    """Extract data from a file using the given extraction kind."""
    started_at = time.perf_counter()
    logger.info("%s start path=%s filename=%s", kind.label, file_path, filename)

    batches: list[_ExtractionBatch] = []
    async for batch in _extract_file_batches(
        file_path,
        kind,
        filename=filename,
        render_cache=render_cache,
    ):
        batches.append(batch)

    result = kind.merge_fn([batch.result for batch in sorted(batches, key=lambda current: current.batch_index)])

    logger.info(
        "%s finished path=%s filename=%s duration=%.2fs",
        kind.label,
        file_path,
        filename,
        time.perf_counter() - started_at,
    )
    return result


async def _extract_file_batches(
    file_path: str,
    kind: _ExtractionKind,
    *,
    filename: str | None = None,
    render_cache: _PdfRenderCache | _ImageRenderCache | None = None,
):
    """Yield finished extraction batches as soon as they complete.

    This is the core primitive that lets downstream code start normalizing the
    first extracted measurement pages before the whole file has finished OCR.
    """

    if Path(file_path).suffix.lower() != ".pdf":
        logger.info("%s single-image path=%s filename=%s", kind.label, file_path, filename)
        image_cache = render_cache if isinstance(render_cache, _ImageRenderCache) else None
        result = await _image_with_retries(
            file_path,
            kind,
            filename=filename,
            render_cache=image_cache,
        )
        yield _ExtractionBatch(
            batch_index=0,
            start_page=0,
            stop_page=1,
            result=result,
        )
        return

    with fitz.open(file_path) as document:
        page_count = document.page_count

    batch_plan = _select_pdf_batch_plan(page_count)
    batch_count = math.ceil(page_count / batch_plan.batch_size)
    logger.info(
        "%s pdf batches path=%s filename=%s page_count=%s batch_size=%s "
        "batch_count=%s concurrency=%s queue_depth=%s active_requests=%s",
        kind.label,
        file_path,
        filename,
        page_count,
        batch_plan.batch_size,
        batch_count,
        batch_plan.concurrency,
        batch_plan.queue_depth,
        batch_plan.active_requests,
    )

    semaphore = asyncio.Semaphore(batch_plan.concurrency)

    async def _run_range(batch_index: int, start_page: int, stop_page: int) -> _ExtractionBatch:
        async with semaphore:
            result = await _pdf_range_with_retries(
                file_path,
                kind,
                start_page=start_page,
                stop_page=stop_page,
                filename=filename,
                render_cache=render_cache,
            )
        return _ExtractionBatch(
            batch_index=batch_index,
            start_page=start_page,
            stop_page=stop_page,
            result=result,
        )

    tasks = [
        asyncio.create_task(
            _run_range(
                batch_index,
                start_page,
                min(start_page + batch_plan.batch_size, page_count),
            )
        )
        for batch_index, start_page in enumerate(range(0, page_count, batch_plan.batch_size))
    ]

    for task in asyncio.as_completed(tasks):
        yield await task


_MEDICAL_EXTRACTION = _ExtractionKind(
    label="Structured medical",
    extract_fn=_extract_structured_medical_data_from_attachments,
    merge_fn=_merge_structured_medical_results,
    post_process_fn=_offset_result_page_numbers,
)

_TEXT_EXTRACTION = _ExtractionKind(
    label="Document text",
    extract_fn=_extract_document_text_from_attachments,
    merge_fn=_merge_document_text_results,
)


async def ocr_extract(
    file_path: str,
    *,
    filename: str | None = None,
    on_medical_batch: Callable[[int, dict], Awaitable[None]] | None = None,
) -> dict:
    """Run the full OCR extraction pipeline for one uploaded file."""
    started_at = time.perf_counter()
    logger.info("OCR pipeline start path=%s filename=%s", file_path, filename)
    usable_text_result: dict | None = None
    summary_result: dict | None = None
    medical_task: asyncio.Task[dict] | None = None
    text_task: asyncio.Task[dict] | None = None
    render_cache: _PdfRenderCache | _ImageRenderCache | None = None
    if file_types.is_text_document_path(file_path):
        request_context = _text_document_request_context(filename)

        async def _extract_medical_result() -> dict:
            result = await _extract_structured_medical_data_from_text_document(
                file_path,
                filename=filename,
                request_context=request_context,
            )
            if on_medical_batch is not None:
                await on_medical_batch(0, result)
            return result

        medical_task = asyncio.create_task(_extract_medical_result(), name="ocr:medical")
        text_task = asyncio.create_task(
            _extract_text_from_text_document(
                file_path,
                filename=filename,
                request_context=request_context,
            ),
            name="ocr:text",
        )
    elif settings.EXTRACTION_PROVIDER == "mistral":

        async def _extract_medical_result() -> dict:
            start_page = 0 if Path(file_path).suffix.lower() == ".pdf" else None
            stop_page = _pdf_page_count(file_path) if start_page is not None else None
            result = await _extract_structured_medical_data_from_file_mistral(
                file_path,
                start_page=start_page,
                stop_page=stop_page,
                filename=filename,
                request_context=_mistral_request_context(filename, start_page=start_page, stop_page=stop_page),
            )
            # Mistral OCR processes the requested file range directly, so the
            # optional callback becomes one whole-file batch notification.
            if on_medical_batch is not None:
                await on_medical_batch(0, result)
            return result

        medical_task = asyncio.create_task(_extract_medical_result(), name="ocr:medical")
        text_task = asyncio.create_task(
            _extract_document_text_from_file_mistral(
                file_path,
                filename=filename,
                request_context=_mistral_request_context(filename),
            ),
            name="ocr:text",
        )
    else:
        render_cache = (
            _PdfRenderCache(file_path) if Path(file_path).suffix.lower() == ".pdf" else _ImageRenderCache(file_path)
        )

        async def _extract_medical_result() -> dict:
            if on_medical_batch is None:
                return await _extract_file(file_path, _MEDICAL_EXTRACTION, filename=filename, render_cache=render_cache)

            medical_batches: list[_ExtractionBatch] = []
            async for batch in _extract_file_batches(
                file_path,
                _MEDICAL_EXTRACTION,
                filename=filename,
                render_cache=render_cache,
            ):
                medical_batches.append(batch)
                await on_medical_batch(batch.batch_index, batch.result)

            return _MEDICAL_EXTRACTION.merge_fn(
                [batch.result for batch in sorted(medical_batches, key=lambda current: current.batch_index)]
            )

        medical_task = asyncio.create_task(_extract_medical_result(), name="ocr:medical")
        text_task = asyncio.create_task(
            extract_text(
                file_path,
                filename=filename,
                render_cache=render_cache,
            ),
            name="ocr:text",
        )

    try:
        try:
            medical_result = await medical_task
        except Exception:
            if text_task is not None:
                if not text_task.done():
                    text_task.cancel()
                await asyncio.gather(text_task, return_exceptions=True)
            raise

        logger.info(
            "OCR pipeline medical extraction ready path=%s filename=%s measurements=%s elapsed=%.2fs",
            file_path,
            filename,
            len(medical_result.get("measurements", [])),
            time.perf_counter() - started_at,
        )

        try:
            usable_text_result = await text_task
        except Exception as exc:
            logger.error(
                "Document text extraction failed for %s (filename=%s): %s",
                file_path,
                filename,
                exc,
            )
            usable_text_result = None
        else:
            logger.info(
                "OCR pipeline text extraction ready path=%s filename=%s has_raw_text=%s "
                "has_translated_text=%s elapsed=%.2fs",
                file_path,
                filename,
                bool(usable_text_result.get("raw_text")),
                bool(usable_text_result.get("translated_text_english")),
                time.perf_counter() - started_at,
            )
            try:
                # Keep text OCR and summarization in separate calls so summary
                # always sees the merged document text, independent of provider.
                summary_result = await generate_summary(
                    (usable_text_result or {}).get("raw_text"),
                    filename=filename,
                )
            except Exception as exc:
                logger.error(
                    "Medical summary generation failed for %s (filename=%s): %s",
                    file_path,
                    filename,
                    exc,
                )
                summary_result = None
            else:
                logger.info(
                    "OCR pipeline summary ready path=%s filename=%s has_summary=%s elapsed=%.2fs",
                    file_path,
                    filename,
                    bool((summary_result or {}).get("summary_english")),
                    time.perf_counter() - started_at,
                )
    finally:
        if render_cache is not None:
            await render_cache.aclose()

    logger.info(
        "OCR pipeline finished path=%s filename=%s measurements=%s raw_text=%s summary=%s duration=%.2fs",
        file_path,
        filename,
        len(medical_result.get("measurements", [])),
        bool((usable_text_result or {}).get("raw_text")),
        bool((summary_result or {}).get("summary_english")),
        time.perf_counter() - started_at,
    )
    return _combine_ocr_outputs(medical_result, usable_text_result, summary_result)


async def extract_text(
    file_path: str,
    *,
    filename: str | None = None,
    render_cache: _PdfRenderCache | _ImageRenderCache | None = None,
) -> dict:
    started_at = time.perf_counter()
    logger.info(
        "Document text extraction start path=%s filename=%s",
        file_path,
        filename,
    )

    try:
        if file_types.is_text_document_path(file_path):
            result = await _extract_text_from_text_document(
                file_path,
                filename=filename,
                request_context=_text_document_request_context(filename),
            )
        elif settings.EXTRACTION_PROVIDER == "mistral":
            result = await _extract_document_text_from_file_mistral(
                file_path,
                filename=filename,
                request_context=_mistral_request_context(filename),
            )
        else:
            result = await _extract_file(file_path, _TEXT_EXTRACTION, filename=filename, render_cache=render_cache)
        logger.info(
            "Document text extraction finished path=%s filename=%s has_raw_text=%s has_translated_text=%s "
            "duration=%.2fs",
            file_path,
            filename,
            bool(result.get("raw_text")),
            bool(result.get("translated_text_english")),
            time.perf_counter() - started_at,
        )
        return result
    except Exception:
        logger.exception(
            "Document text extraction failed path=%s filename=%s after %.2fs",
            file_path,
            filename,
            time.perf_counter() - started_at,
        )
        raise


async def extract_measurement_batch(
    file_path: str,
    *,
    start_page: int,
    stop_page: int,
    dpi: int,
    filename: str | None = None,
) -> dict:
    if file_types.is_text_document_path(file_path):
        return await _extract_structured_medical_data_from_text_document(
            file_path,
            filename=filename,
            request_context=_text_document_request_context(filename),
        )
    if settings.EXTRACTION_PROVIDER == "mistral":
        pdf_range = Path(file_path).suffix.lower() == ".pdf"
        return await _extract_structured_medical_data_from_file_mistral(
            file_path,
            start_page=start_page if pdf_range else None,
            stop_page=stop_page if pdf_range else None,
            filename=filename,
            request_context=_mistral_request_context(
                filename,
                start_page=start_page if pdf_range else None,
                stop_page=stop_page if pdf_range else None,
            ),
        )
    if Path(file_path).suffix.lower() != ".pdf":
        return await _image_with_retries(
            file_path,
            _MEDICAL_EXTRACTION,
            dpi=dpi,
            filename=filename,
        )

    # The DB-backed pipeline calls this wrapper directly for each durable OCR
    # job, so it must go through the retry/split/lower-DPI path instead of the
    # raw single-attempt batch call.
    return await _pdf_range_with_retries(
        file_path,
        _MEDICAL_EXTRACTION,
        start_page=start_page,
        stop_page=stop_page,
        dpi=dpi,
        filename=filename,
    )


async def extract_text_batch(
    file_path: str,
    *,
    start_page: int,
    stop_page: int,
    dpi: int,
    filename: str | None = None,
) -> dict:
    if file_types.is_text_document_path(file_path):
        return await _extract_text_from_text_document(
            file_path,
            filename=filename,
            request_context=_text_document_request_context(filename),
        )
    if settings.EXTRACTION_PROVIDER == "mistral":
        pdf_range = Path(file_path).suffix.lower() == ".pdf"
        return await _extract_document_text_from_file_mistral(
            file_path,
            start_page=start_page if pdf_range else None,
            stop_page=stop_page if pdf_range else None,
            filename=filename,
            request_context=_mistral_request_context(
                filename,
                start_page=start_page if pdf_range else None,
                stop_page=stop_page if pdf_range else None,
            ),
        )
    if Path(file_path).suffix.lower() != ".pdf":
        return await _image_with_retries(
            file_path,
            _TEXT_EXTRACTION,
            dpi=dpi,
            filename=filename,
        )

    # Keep the durable text-extraction jobs on the same resilient path as the
    # full OCR flow so oversized or slow batches fan out instead of failing the
    # whole file immediately.
    return await _pdf_range_with_retries(
        file_path,
        _TEXT_EXTRACTION,
        start_page=start_page,
        stop_page=stop_page,
        dpi=dpi,
        filename=filename,
    )


def merge_text_results(results: list[dict]) -> dict:
    return _merge_document_text_results(results)


async def generate_summary(
    raw_text: str | None,
    *,
    filename: str | None = None,
) -> dict[str, str | None]:
    return await _generate_medical_summary(raw_text, filename=filename)


def is_retryable_batch_error(exc: Exception) -> bool:
    return _is_retryable_pdf_error(exc)
