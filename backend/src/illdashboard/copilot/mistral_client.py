"""Mistral OCR and chat helpers for extraction and normalization."""

from __future__ import annotations

import base64
import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import fitz
from mistralai.client import Mistral

from illdashboard.config import settings
from illdashboard.copilot.client import JSON_REPAIR_SYSTEM_PROMPT, _format_json_user_prompt, _parse_json_response

logger = logging.getLogger(__name__)

MISTRAL_REQUEST_TIMEOUT = 900


def _mistral_api_key() -> str:
    api_key = settings.MISTRAL_API_KEY or os.environ.get("MISTRAL_API_KEY", "")
    if not api_key:
        raise RuntimeError("Mistral API key is not configured")
    return api_key


def _mistral_timeout_ms(timeout: float) -> int:
    return max(1_000, int(timeout * 1_000))


def _sdk_client(timeout: float) -> Mistral:
    return Mistral(
        api_key=_mistral_api_key(),
        server_url=settings.MISTRAL_API_BASE_URL,
        timeout_ms=_mistral_timeout_ms(timeout),
    )


def _mime_type_for_path(path: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix == ".pdf":
        return "application/pdf"
    if suffix == ".png":
        return "image/png"
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    raise ValueError(f"Unsupported Mistral document type for {path}")


def _data_url_for_path(path: str) -> str:
    with open(path, "rb") as handle:
        encoded = base64.b64encode(handle.read()).decode("ascii")
    return f"data:{_mime_type_for_path(path)};base64,{encoded}"


def _slice_pdf_to_temp_pdf(pdf_path: str, *, start_page: int, stop_page: int) -> str:
    if stop_page <= start_page:
        raise ValueError(f"Invalid PDF slice {start_page}:{stop_page}")

    temp_file = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    temp_file.close()
    source = fitz.open(pdf_path)
    target = fitz.open()
    try:
        target.insert_pdf(source, from_page=start_page, to_page=stop_page - 1)
        target.save(temp_file.name)
    finally:
        target.close()
        source.close()
    return temp_file.name


def _ocr_input_path(
    file_path: str,
    *,
    start_page: int | None = None,
    stop_page: int | None = None,
) -> tuple[str, str | None]:
    if Path(file_path).suffix.lower() != ".pdf" or start_page is None or stop_page is None:
        return file_path, None
    temp_path = _slice_pdf_to_temp_pdf(file_path, start_page=start_page, stop_page=stop_page)
    return temp_path, temp_path


def _ocr_document_for_path(path: str) -> dict[str, str]:
    data_url = _data_url_for_path(path)
    if Path(path).suffix.lower() == ".pdf":
        return {"type": "document_url", "document_url": data_url}
    return {"type": "image_url", "image_url": data_url}


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool, dict, list)):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    return value


def _message_text(response: Any, *, request_name: str) -> str:
    choices = _field(response, "choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError(f"Mistral chat response for {request_name} did not include choices")
    message = _field(choices[0], "message")
    if message is None:
        raise RuntimeError(f"Mistral chat response for {request_name} did not include a message")
    content = _field(message, "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, dict):
                if block.get("type") == "text" and isinstance(block.get("text"), str):
                    parts.append(block["text"])
                continue
            text = getattr(block, "text", None)
            if isinstance(text, str):
                parts.append(text)
        return "\n".join(part for part in parts if part)
    raise RuntimeError(f"Mistral chat response for {request_name} had unsupported content")


async def ask_text(
    system_prompt: str,
    user_prompt: str,
    *,
    request_name: str,
    request_context: str = "",
    timeout: float = MISTRAL_REQUEST_TIMEOUT,
) -> str:
    started_at = time.perf_counter()
    logger.info(
        "Mistral chat starting request_name=%s model=%s timeout=%ss context=%s",
        request_name,
        settings.MISTRAL_CHAT_MODEL,
        timeout,
        request_context or "none",
    )
    async with _sdk_client(timeout) as client:
        response = await client.chat.complete_async(
            model=settings.MISTRAL_CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            stream=False,
            response_format={"type": "text"},
        )
    logger.info(
        "Mistral chat finished request_name=%s model=%s duration=%.2fs context=%s",
        request_name,
        settings.MISTRAL_CHAT_MODEL,
        time.perf_counter() - started_at,
        request_context or "none",
    )
    return _message_text(response, request_name=request_name)


async def _repair_json_response(raw: str, *, request_name: str) -> str:
    logger.warning(
        "Mistral JSON parse failed request_name=%s response_chars=%s; attempting structural repair",
        request_name,
        len(raw),
    )
    return await ask_text(
        JSON_REPAIR_SYSTEM_PROMPT,
        f"Malformed response:\n\n{raw}",
        request_name="repair_json_response",
        request_context=request_name,
    )


async def _ask_json(
    system_prompt: str,
    user_prompt: str,
    *,
    request_name: str,
    request_context: str = "",
    timeout: float = MISTRAL_REQUEST_TIMEOUT,
    default: dict | None = None,
) -> dict:
    raw = await ask_text(
        system_prompt,
        _format_json_user_prompt(user_prompt),
        request_name=request_name,
        request_context=request_context,
        timeout=timeout,
    )
    try:
        return _parse_json_response(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        try:
            repaired = await _repair_json_response(raw, request_name=request_name)
            return _parse_json_response(repaired)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
        if default is None:
            raise
        return default


async def process_ocr_file(
    file_path: str,
    *,
    start_page: int | None = None,
    stop_page: int | None = None,
    request_name: str,
    request_context: str = "",
    document_annotation_format: dict | None = None,
    document_annotation_prompt: str | None = None,
    timeout: float = MISTRAL_REQUEST_TIMEOUT,
) -> Any:
    source_path, temp_path = _ocr_input_path(file_path, start_page=start_page, stop_page=stop_page)
    started_at = time.perf_counter()
    logger.info(
        "Mistral OCR starting request_name=%s path=%s source_path=%s model=%s context=%s",
        request_name,
        file_path,
        source_path,
        settings.MISTRAL_OCR_MODEL,
        request_context or "none",
    )
    try:
        async with _sdk_client(timeout) as client:
            request_kwargs = {
                "model": settings.MISTRAL_OCR_MODEL,
                "document": _ocr_document_for_path(source_path),
            }
            if document_annotation_format is not None:
                request_kwargs["document_annotation_format"] = document_annotation_format
            if document_annotation_prompt:
                request_kwargs["document_annotation_prompt"] = document_annotation_prompt
            response = await client.ocr.process_async(**request_kwargs)
    finally:
        if temp_path is not None:
            try:
                os.unlink(temp_path)
            except OSError:
                pass
    logger.info(
        "Mistral OCR finished request_name=%s path=%s duration=%.2fs context=%s",
        request_name,
        file_path,
        time.perf_counter() - started_at,
        request_context or "none",
    )
    return response


def document_markdown_text(result: Any) -> str:
    pages = _field(result, "pages", [])
    if not isinstance(pages, list):
        return ""
    ordered_pages = sorted(pages, key=lambda page: int(_field(page, "index", 0)))
    parts = [str(_field(page, "markdown", "")).strip() for page in ordered_pages if _field(page, "markdown")]
    return "\n\n".join(part for part in parts if part)


def document_annotation(result: Any) -> dict:
    annotation = _jsonable(_field(result, "document_annotation"))
    if isinstance(annotation, dict):
        return annotation

    pages = _field(result, "pages", [])
    if isinstance(pages, list):
        for page in pages:
            page_annotation = _jsonable(_field(page, "document_annotation")) or _jsonable(_field(page, "annotations"))
            if isinstance(page_annotation, dict):
                return page_annotation
    raise RuntimeError("Mistral OCR response did not include a document annotation object")
