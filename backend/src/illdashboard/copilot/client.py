"""Shared GitHub Copilot SDK client helpers."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import AsyncExitStack
from dataclasses import dataclass

from copilot import CopilotClient, PermissionHandler
from copilot.generated.session_events import SessionEventType
from copilot.types import CopilotClientOptions, MessageOptions, ReasoningEffort

from illdashboard.config import settings
from illdashboard.metrics import add_premium_requests

logger = logging.getLogger(__name__)

# Recent runs still hit the 10-minute ceiling for document OCR, so keep a
# larger shared request budget while lane reservations prevent extraction from
# starving normalization, summary, and interactive work.
COPILOT_REQUEST_TIMEOUT = 900
COPILOT_REQUEST_CONCURRENCY = 10
COPILOT_EXTRACTION_CONCURRENCY = 6
COPILOT_SUMMARY_CONCURRENCY = 1
COPILOT_INTERACTIVE_CONCURRENCY = 2
COPILOT_TRANSIENT_RETRY_ATTEMPTS = 2
COPILOT_TRANSIENT_RETRY_DELAY = 3
COPILOT_REQUEST_PROGRESS_INTERVAL = 30.0

_EXTRACTION_REQUEST_NAMES = {
    "structured_medical_extraction",
    "document_text_extraction",
}
_NORMALIZATION_REQUEST_NAMES = {
    "normalize_marker_names",
    "normalize_source_name",
    "choose_canonical_units",
    "infer_rescaling_factors",
    "normalize_qualitative_values",
    "classify_marker_groups",
}
_NORMALIZATION_LANE_LIMITS = {
    # Each normalization domain keeps its own serialized lane so prompts always
    # read a stable snapshot of the shared canonical data they depend on, while
    # unrelated normalization kinds can still make progress in parallel.
    "normalize_marker_names": 1,
    "normalize_source_name": 1,
    "choose_canonical_units": 1,
    "infer_rescaling_factors": 1,
    "normalize_qualitative_values": 1,
    "classify_marker_groups": 1,
}

_client: CopilotClient | None = None
_client_start_lock = asyncio.Lock()
_request_semaphore: asyncio.Semaphore | None = None
_request_semaphore_limit = 0
_lane_semaphores: dict[str, asyncio.Semaphore] = {}
_lane_semaphore_limits: dict[str, int] = {}
_queued_request_count = 0
_active_request_count = 0


@dataclass(frozen=True)
class _RequestSessionSettings:
    model: str
    reasoning_effort: ReasoningEffort | None = None


def _request_session_settings(request_name: str) -> _RequestSessionSettings:
    if request_name == "structured_medical_extraction":
        return _RequestSessionSettings(
            model=settings.COPILOT_MEASUREMENT_EXTRACTION_MODEL,
            reasoning_effort=settings.COPILOT_MEASUREMENT_EXTRACTION_REASONING_EFFORT,
        )
    if request_name == "document_text_extraction":
        return _RequestSessionSettings(
            model=settings.COPILOT_TEXT_EXTRACTION_MODEL,
            reasoning_effort=settings.COPILOT_TEXT_EXTRACTION_REASONING_EFFORT,
        )
    if request_name in _NORMALIZATION_REQUEST_NAMES:
        return _RequestSessionSettings(
            model=settings.COPILOT_NORMALIZATION_MODEL,
            reasoning_effort=settings.COPILOT_NORMALIZATION_REASONING_EFFORT,
        )
    return _RequestSessionSettings(model=settings.COPILOT_DEFAULT_MODEL)


def _configured_model_summary() -> str:
    return (
        f"default={settings.COPILOT_DEFAULT_MODEL} "
        f"measurement={settings.COPILOT_MEASUREMENT_EXTRACTION_MODEL} "
        f"measurement_reasoning={settings.COPILOT_MEASUREMENT_EXTRACTION_REASONING_EFFORT} "
        f"text={settings.COPILOT_TEXT_EXTRACTION_MODEL} "
        f"text_reasoning={settings.COPILOT_TEXT_EXTRACTION_REASONING_EFFORT} "
        f"normalization={settings.COPILOT_NORMALIZATION_MODEL} "
        f"normalization_reasoning={settings.COPILOT_NORMALIZATION_REASONING_EFFORT}"
    )


JSON_RESPONSE_INSTRUCTIONS = """\
Return exactly one valid JSON object that matches the requested schema.
- Do not wrap the JSON in Markdown or code fences.
- Escape embedded double quotes, backslashes, and newlines inside JSON strings.
- Close every string, array, and object before finishing the response.
"""

JSON_REPAIR_SYSTEM_PROMPT = """\
You repair malformed JSON responses.

You will receive a raw model response that was intended to be a single JSON object.
Return only one valid JSON object and nothing else.

Rules:
- Preserve the existing keys and content whenever possible.
- Do not add commentary, Markdown, or code fences.
- If a string is truncated, close it cleanly at the last sensible character rather than inventing missing content.
- If trailing text exists outside the JSON object, drop that trailing text.
"""


async def _get_client() -> CopilotClient:
    """Return a shared CopilotClient, starting it on first use."""
    global _client

    if _client is not None:
        return _client

    async with _client_start_lock:
        if _client is not None:
            return _client

        started_at = time.perf_counter()
        token = settings.GITHUB_TOKEN or os.environ.get("GITHUB_TOKEN", "")
        options: CopilotClientOptions | None = {"github_token": token} if token else None
        client = CopilotClient(options)
        logger.info(
            "Copilot client starting models=%s token_configured=%s",
            _configured_model_summary(),
            bool(token),
        )
        try:
            await client.start()
        except Exception:
            logger.exception(
                "Copilot client start failed models=%s duration=%.2fs",
                _configured_model_summary(),
                time.perf_counter() - started_at,
            )
            raise

        _client = client
        logger.info(
            "Copilot client ready models=%s token_configured=%s duration=%.2fs",
            _configured_model_summary(),
            bool(token),
            time.perf_counter() - started_at,
        )
    return _client


def _get_request_semaphore() -> asyncio.Semaphore:
    global _request_semaphore, _request_semaphore_limit

    if _request_semaphore is None or _request_semaphore_limit != COPILOT_REQUEST_CONCURRENCY:
        _request_semaphore = asyncio.Semaphore(COPILOT_REQUEST_CONCURRENCY)
        _request_semaphore_limit = COPILOT_REQUEST_CONCURRENCY
    return _request_semaphore


def _request_lane_name(request_name: str) -> str:
    if request_name == "medical_summary":
        return "summary"
    if request_name in _EXTRACTION_REQUEST_NAMES:
        return "extraction"
    if request_name in _NORMALIZATION_REQUEST_NAMES:
        return request_name
    return "interactive"


def _request_lane_limit(lane_name: str) -> int:
    if lane_name == "summary":
        return COPILOT_SUMMARY_CONCURRENCY
    if lane_name == "extraction":
        return COPILOT_EXTRACTION_CONCURRENCY
    if lane_name in _NORMALIZATION_LANE_LIMITS:
        return _NORMALIZATION_LANE_LIMITS[lane_name]
    if lane_name == "interactive":
        return COPILOT_INTERACTIVE_CONCURRENCY
    raise ValueError(f"Unknown request lane {lane_name}")


def _get_request_lane_semaphore(lane_name: str) -> asyncio.Semaphore:
    limit = _request_lane_limit(lane_name)

    if lane_name not in _lane_semaphores or _lane_semaphore_limits.get(lane_name) != limit:
        _lane_semaphores[lane_name] = asyncio.Semaphore(limit)
        _lane_semaphore_limits[lane_name] = limit
    return _lane_semaphores[lane_name]


def get_copilot_request_load() -> tuple[int, int]:
    return _queued_request_count, _active_request_count


def _is_retryable_session_error(exc: Exception) -> bool:
    return "failed to list models" in str(exc).lower()


async def shutdown_client() -> None:
    """Stop the shared Copilot client."""
    global _client

    if _client is not None:
        try:
            await _client.stop()
        except Exception as exc:
            logger.warning("Copilot client shutdown had errors: %s", exc)
        _client = None


async def prewarm_client() -> bool:
    """Start the shared Copilot client during application startup."""
    started_at = time.perf_counter()
    try:
        await _get_client()
    except Exception as exc:
        logger.warning("Copilot client prewarm failed after %.2fs: %s", time.perf_counter() - started_at, exc)
        return False

    logger.info("Copilot client prewarm complete duration=%.2fs", time.perf_counter() - started_at)
    return True


async def _ask(
    system_prompt: str,
    user_prompt: str,
    *,
    attachments: list[dict] | None = None,
    timeout: float = COPILOT_REQUEST_TIMEOUT,
    request_name: str = "copilot_request",
) -> str:
    """Create an ephemeral Copilot session, send one prompt, return text."""
    started_at = time.perf_counter()
    attachment_count = len(attachments or [])
    request_id = uuid.uuid4().hex[:12]
    request_settings = _request_session_settings(request_name)
    logger.info(
        "Copilot request starting request_name=%s request_id=%s model=%s reasoning_effort=%s timeout=%ss "
        "attachments=%s prompt_chars=%s",
        request_name,
        request_id,
        request_settings.model,
        request_settings.reasoning_effort,
        timeout,
        attachment_count,
        len(user_prompt),
    )

    lane_name = _request_lane_name(request_name)
    lane_semaphore = _get_request_lane_semaphore(lane_name)
    observed_usage_cost_total = 0.0

    for attempt in range(1, COPILOT_TRANSIENT_RETRY_ATTEMPTS + 1):
        content = ""
        observed_usage_cost = 0.0
        request_error: Exception | None = None
        queued_registered = False

        try:
            global _queued_request_count, _active_request_count

            _queued_request_count += 1
            queued_registered = True
            lane_wait_started_at = time.perf_counter()
            async with AsyncExitStack() as stack:
                await stack.enter_async_context(lane_semaphore)
                lane_wait_ms = (time.perf_counter() - lane_wait_started_at) * 1000
                semaphore_wait_started_at = time.perf_counter()
                await stack.enter_async_context(_get_request_semaphore())
                semaphore_wait_ms = (time.perf_counter() - semaphore_wait_started_at) * 1000
                _queued_request_count -= 1
                queued_registered = False
                _active_request_count += 1
                try:
                    client_wait_started_at = time.perf_counter()
                    client = await _get_client()
                    client_wait_ms = (time.perf_counter() - client_wait_started_at) * 1000
                    session_create_started_at = time.perf_counter()
                    session_config = {
                        "model": request_settings.model,
                        "system_message": {"mode": "replace", "content": system_prompt},
                        "available_tools": [],
                        "on_permission_request": PermissionHandler.approve_all,
                    }
                    if request_settings.reasoning_effort is not None:
                        session_config["reasoning_effort"] = request_settings.reasoning_effort
                    session = await client.create_session(session_config)
                    session_create_ms = (time.perf_counter() - session_create_started_at) * 1000
                    logger.info(
                        "Copilot request ready request_name=%s request_id=%s attempt=%s/%s lane=%s "
                        "lane_wait_ms=%.1f semaphore_wait_ms=%.1f client_wait_ms=%.1f "
                        "session_create_ms=%.1f queued_requests=%s active_requests=%s",
                        request_name,
                        request_id,
                        attempt,
                        COPILOT_TRANSIENT_RETRY_ATTEMPTS,
                        lane_name,
                        lane_wait_ms,
                        semaphore_wait_ms,
                        client_wait_ms,
                        session_create_ms,
                        _queued_request_count,
                        _active_request_count,
                    )

                    def handle_session_event(event) -> None:
                        nonlocal observed_usage_cost

                        if event.type == SessionEventType.ASSISTANT_USAGE:
                            cost = getattr(event.data, "cost", None)
                            if isinstance(cost, int | float) and cost > 0:
                                observed_usage_cost += float(cost)
                            return

                        if event.type == SessionEventType.SESSION_WARNING:
                            logger.warning(
                                "Copilot session warning request_name=%s request_id=%s event_type=%s "
                                "warning_type=%s status_code=%s message=%s reason=%s error_reason=%s",
                                request_name,
                                request_id,
                                getattr(event.type, "value", event.type),
                                getattr(event.data, "warning_type", None),
                                getattr(event.data, "status_code", None),
                                getattr(event.data, "message", None),
                                getattr(event.data, "reason", None),
                                getattr(event.data, "error_reason", None),
                            )
                            return

                        if event.type == SessionEventType.SESSION_INFO:
                            logger.info(
                                "Copilot session info request_name=%s request_id=%s event_type=%s "
                                "info_type=%s status_code=%s message=%s",
                                request_name,
                                request_id,
                                getattr(event.type, "value", event.type),
                                getattr(event.data, "info_type", None),
                                getattr(event.data, "status_code", None),
                                getattr(event.data, "message", None),
                            )
                            return

                        if event.type == SessionEventType.SESSION_ERROR:
                            logger.warning(
                                "Copilot session error event request_name=%s request_id=%s event_type=%s "
                                "error_type=%s status_code=%s message=%s reason=%s error_reason=%s",
                                request_name,
                                request_id,
                                getattr(event.type, "value", event.type),
                                getattr(event.data, "error_type", None),
                                getattr(event.data, "status_code", None),
                                getattr(event.data, "message", None),
                                getattr(event.data, "reason", None),
                                getattr(event.data, "error_reason", None),
                            )

                    unsubscribe = session.on(handle_session_event)
                    progress_task: asyncio.Task[None] | None = None
                    try:
                        async def _log_request_progress() -> None:
                            while True:
                                await asyncio.sleep(COPILOT_REQUEST_PROGRESS_INTERVAL)
                                logger.info(
                                    "Copilot request still running request_name=%s request_id=%s lane=%s "
                                    "attempt=%s/%s elapsed=%.2fs queued_requests=%s active_requests=%s",
                                    request_name,
                                    request_id,
                                    lane_name,
                                    attempt,
                                    COPILOT_TRANSIENT_RETRY_ATTEMPTS,
                                    time.perf_counter() - started_at,
                                    _queued_request_count,
                                    _active_request_count,
                                )

                        if COPILOT_REQUEST_PROGRESS_INTERVAL > 0:
                            progress_task = asyncio.create_task(_log_request_progress())
                        message_options: MessageOptions = {"prompt": user_prompt}
                        if attachments:
                            message_options["attachments"] = attachments
                        response = await session.send_and_wait(message_options, timeout=timeout)
                        content = getattr(response.data, "content", "") if response else ""
                        if content is None:
                            content = ""
                    except Exception as exc:
                        request_error = exc
                    finally:
                        if progress_task is not None:
                            progress_task.cancel()
                            try:
                                await progress_task
                            except asyncio.CancelledError:
                                pass
                        unsubscribe()
                        try:
                            await session.disconnect()
                        except Exception as exc:
                            logger.warning("Copilot session disconnect had errors: %s", exc)
                finally:
                    _active_request_count -= 1
        finally:
            if queued_registered:
                _queued_request_count -= 1

        observed_usage_cost_total += observed_usage_cost
        duration = time.perf_counter() - started_at

        if request_error is None:
            add_premium_requests(observed_usage_cost_total)
            logger.info(
                "Copilot request finished request_name=%s request_id=%s model=%s reasoning_effort=%s "
                "attachments=%s duration=%.2fs response_chars=%s usage_cost=%.4f",
                request_name,
                request_id,
                request_settings.model,
                request_settings.reasoning_effort,
                attachment_count,
                duration,
                len(content),
                observed_usage_cost_total,
            )
            return content

        if _is_retryable_session_error(request_error) and attempt < COPILOT_TRANSIENT_RETRY_ATTEMPTS:
            logger.warning(
                "Copilot request retrying request_name=%s request_id=%s attempt=%s/%s model=%s "
                "reasoning_effort=%s "
                "attachments=%s duration=%.2fs delay=%ss error=%s",
                request_name,
                request_id,
                attempt,
                COPILOT_TRANSIENT_RETRY_ATTEMPTS,
                request_settings.model,
                request_settings.reasoning_effort,
                attachment_count,
                duration,
                COPILOT_TRANSIENT_RETRY_DELAY,
                request_error,
            )
            await shutdown_client()
            await asyncio.sleep(COPILOT_TRANSIENT_RETRY_DELAY)
            continue

        add_premium_requests(observed_usage_cost_total)
        logger.warning(
            "Copilot request failed request_name=%s request_id=%s model=%s reasoning_effort=%s "
            "attachments=%s duration=%.2fs error=%s",
            request_name,
            request_id,
            request_settings.model,
            request_settings.reasoning_effort,
            attachment_count,
            duration,
            request_error,
        )
        raise request_error

    raise RuntimeError("Copilot request retry loop exited unexpectedly")


def _format_json_user_prompt(user_prompt: str) -> str:
    prompt = user_prompt.rstrip()
    if prompt:
        return f"{prompt}\n\n{JSON_RESPONSE_INSTRUCTIONS}"
    return JSON_RESPONSE_INSTRUCTIONS


def _strip_markdown_fences(raw: str) -> str:
    candidate = raw.strip()
    if candidate.startswith("```"):
        first_newline = candidate.find("\n")
        if first_newline != -1:
            candidate = candidate[first_newline + 1 :]
    if candidate.endswith("```"):
        candidate = candidate.rsplit("```", 1)[0]
    return candidate.strip()


def _extract_first_json_object(raw: str) -> str | None:
    start = raw.find("{")
    if start == -1:
        return None

    depth = 0
    in_string = False
    escaped = False
    for index, character in enumerate(raw[start:], start=start):
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue

        if character == '"':
            in_string = True
        elif character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return raw[start : index + 1]

    return None


def _parse_json_response(raw: str) -> dict:
    candidate = _strip_markdown_fences(raw)
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        extracted = _extract_first_json_object(candidate)
        if extracted is None or extracted == candidate:
            raise
        return json.loads(extracted)


async def _repair_json_response(raw: str, *, request_name: str) -> str:
    logger.warning(
        "Copilot JSON parse failed request_name=%s response_chars=%s; attempting structural repair",
        request_name,
        len(raw),
    )
    return await _ask(
        JSON_REPAIR_SYSTEM_PROMPT,
        f"Original request name: {request_name}\n\nMalformed response:\n{raw}",
        request_name="repair_json_response",
    )


async def _ask_json(
    system_prompt: str,
    user_prompt: str,
    *,
    attachments: list[dict] | None = None,
    timeout: float = COPILOT_REQUEST_TIMEOUT,
    default: dict | None = None,
    request_name: str = "copilot_json_request",
) -> dict:
    raw = await _ask(
        system_prompt,
        _format_json_user_prompt(user_prompt),
        attachments=attachments,
        timeout=timeout,
        request_name=request_name,
    )
    try:
        return _parse_json_response(raw)
    except (json.JSONDecodeError, TypeError, ValueError) as parse_error:
        try:
            repaired = await _repair_json_response(raw, request_name=request_name)
            return _parse_json_response(repaired)
        except (json.JSONDecodeError, TypeError, ValueError) as repair_error:
            logger.warning(
                "Copilot JSON repair failed request_name=%s original_error=%s repair_error=%s",
                request_name,
                parse_error,
                repair_error,
            )
        except Exception as repair_error:
            logger.warning(
                "Copilot JSON repair request failed request_name=%s original_error=%s repair_error=%s",
                request_name,
                parse_error,
                repair_error,
            )
        if default is None:
            raise
        return default
