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

from illdashboard.copilot.client import _ask_json


logger = logging.getLogger(__name__)

OCR_PDF_BATCH_SIZE = 2
OCR_PDF_RENDER_DPI = 144
OCR_PDF_MIN_RENDER_DPI = 96
OCR_ASK_TIMEOUT = 180
OCR_RETRY_DELAY = 3
OCR_PDF_BATCH_CONCURRENCY = 4


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

        for path in cached_paths:
            try:
                os.unlink(path)
            except OSError:
                pass


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
            pix = page.get_pixmap(dpi=dpi, colorspace=fitz.csGRAY, alpha=False)
            temp_file = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
            temp_file.close()
            pix.save(temp_file.name)
            paths.append(temp_file.name)
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
) -> dict:
    prompt = "Extract all lab values from the attached file, including qualitative serology and immunology results."
    if filename:
        prompt = f"Original filename: {filename}\n\n{prompt}"

    return await _ask_json(
        MEDICAL_OCR_SYSTEM_PROMPT,
        prompt,
        attachments=attachments,
        timeout=OCR_ASK_TIMEOUT,
        request_name="structured_medical_extraction",
    )


async def _extract_document_text_from_attachments(attachments: list[dict], *, filename: str | None = None) -> dict:
    prompt = "Transcribe all visible text from the attached document and translate it to English."
    if filename:
        prompt = f"Original filename: {filename}\n\n{prompt}"

    return await _ask_json(
        TEXT_OCR_SYSTEM_PROMPT,
        prompt,
        attachments=attachments,
        timeout=OCR_ASK_TIMEOUT,
        request_name="document_text_extraction",
    )


async def _generate_medical_summary(
    medical_result: dict,
    text_result: dict | None,
    *,
    filename: str | None = None,
) -> str | None:
    started_at = time.perf_counter()
    logger.info(
        "Medical summary start filename=%s measurements=%s translated_text=%s",
        filename,
        len(medical_result.get("measurements", [])),
        bool((text_result or {}).get("translated_text_english")),
    )
    user_payload = {
        "filename": filename,
        "translated_text_english": (text_result or {}).get("translated_text_english"),
        "medical_extraction": medical_result,
    }
    parsed = await _ask_json(
        MEDICAL_SUMMARY_SYSTEM_PROMPT,
        json.dumps(user_payload, ensure_ascii=False, indent=2),
        request_name="medical_summary",
    )
    summary = parsed.get("summary_english")
    cleaned_summary = summary.strip() if isinstance(summary, str) and summary.strip() else None
    logger.info(
        "Medical summary finished filename=%s has_summary=%s duration=%.2fs",
        filename,
        bool(cleaned_summary),
        time.perf_counter() - started_at,
    )
    return cleaned_summary


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

        result = await kind.extract_fn(attachments, filename=filename)
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
        for path in temp_images:
            try:
                os.unlink(path)
            except OSError:
                pass


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
                "%s batch request failed for %s (filename=%s, pages=%s-%s, dpi=%s, reason=%s); falling back to single-page retries",
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

        smaller_dpi = max(OCR_PDF_MIN_RENDER_DPI, dpi - 24)
        if smaller_dpi != dpi:
            logger.warning(
                "%s page request failed for %s (filename=%s, page=%s, dpi=%s, reason=%s); retrying at dpi=%s",
                kind.label,
                pdf_path,
                filename,
                start_page + 1,
                dpi,
                retry_reason,
                smaller_dpi,
            )
            return await _retry(dpi=smaller_dpi)

        logger.exception(
            "%s extraction failed at minimum DPI for %s (filename=%s, page=%s, dpi=%s)",
            kind.label,
            pdf_path,
            filename,
            start_page + 1,
            dpi,
        )
        raise


async def _extract_file(
    file_path: str,
    kind: _ExtractionKind,
    *,
    filename: str | None = None,
    render_cache: _PdfRenderCache | None = None,
) -> dict:
    """Extract data from a file using the given extraction kind."""
    started_at = time.perf_counter()
    logger.info("%s start path=%s filename=%s", kind.label, file_path, filename)

    if Path(file_path).suffix.lower() != ".pdf":
        logger.info("%s single-image path=%s filename=%s", kind.label, file_path, filename)
        result = await kind.extract_fn([{"type": "file", "path": file_path}], filename=filename)
    else:
        with fitz.open(file_path) as document:
            page_count = document.page_count

        batch_count = math.ceil(page_count / OCR_PDF_BATCH_SIZE)
        logger.info(
            "%s pdf batches path=%s filename=%s page_count=%s batch_size=%s batch_count=%s concurrency=%s",
            kind.label,
            file_path,
            filename,
            page_count,
            OCR_PDF_BATCH_SIZE,
            batch_count,
            OCR_PDF_BATCH_CONCURRENCY,
        )

        semaphore = asyncio.Semaphore(OCR_PDF_BATCH_CONCURRENCY)

        async def _run_range(start_page: int, stop_page: int) -> dict:
            async with semaphore:
                return await _pdf_range_with_retries(
                    file_path,
                    kind,
                    start_page=start_page,
                    stop_page=stop_page,
                    filename=filename,
                    render_cache=render_cache,
                )

        tasks = [
            asyncio.create_task(_run_range(start_page, min(start_page + OCR_PDF_BATCH_SIZE, page_count)))
            for start_page in range(0, page_count, OCR_PDF_BATCH_SIZE)
        ]
        result = kind.merge_fn(await asyncio.gather(*tasks))

    logger.info(
        "%s finished path=%s filename=%s duration=%.2fs",
        kind.label,
        file_path,
        filename,
        time.perf_counter() - started_at,
    )
    return result


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


async def ocr_extract(file_path: str, *, filename: str | None = None) -> dict:
    """Run the full OCR extraction pipeline for one uploaded file."""
    started_at = time.perf_counter()
    logger.info("OCR pipeline start path=%s filename=%s", file_path, filename)
    render_cache = _PdfRenderCache(file_path) if Path(file_path).suffix.lower() == ".pdf" else None

    # Keep structured extraction and free-form OCR + translation as separate
    # stages. Do not merge them. They intentionally serve different prompts and
    # failure modes even when they reuse the same rendered pages.
    medical_task = asyncio.create_task(_extract_file(file_path, _MEDICAL_EXTRACTION, filename=filename, render_cache=render_cache))
    text_task = asyncio.create_task(_extract_file(file_path, _TEXT_EXTRACTION, filename=filename, render_cache=render_cache))

    try:
        medical_result_or_error, text_result_or_error = await asyncio.gather(
            medical_task,
            text_task,
            return_exceptions=True,
        )
    finally:
        if render_cache is not None:
            await render_cache.aclose()

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
    logger.info(
        "OCR pipeline medical extraction ready path=%s filename=%s measurements=%s elapsed=%.2fs",
        file_path,
        filename,
        len(medical_result.get("measurements", [])),
        time.perf_counter() - started_at,
    )

    usable_text_result: dict | None
    if isinstance(text_result_or_error, BaseException):
        logger.error(
            "Document text extraction failed for %s (filename=%s): %s",
            file_path,
            filename,
            text_result_or_error,
        )
        usable_text_result = None
    else:
        usable_text_result = text_result_or_error
        logger.info(
            "OCR pipeline document text ready path=%s filename=%s has_raw_text=%s has_translated_text=%s elapsed=%.2fs",
            file_path,
            filename,
            bool(usable_text_result.get("raw_text")),
            bool(usable_text_result.get("translated_text_english")),
            time.perf_counter() - started_at,
        )

    summary_english: str | None = None
    try:
        summary_english = await _generate_medical_summary(
            medical_result,
            usable_text_result,
            filename=filename,
        )
    except Exception:
        logger.exception("Medical summary generation failed for %s (filename=%s)", file_path, filename)

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