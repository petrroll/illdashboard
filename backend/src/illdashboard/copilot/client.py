"""Shared GitHub Copilot SDK client helpers."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time

from copilot import CopilotClient, PermissionHandler
from copilot.generated.session_events import SessionEventType
from copilot.types import CopilotClientOptions, MessageOptions

from illdashboard.config import settings
from illdashboard.metrics import add_premium_requests


logger = logging.getLogger(__name__)

COPILOT_REQUEST_CONCURRENCY = 6

_client: CopilotClient | None = None
_request_semaphore: asyncio.Semaphore | None = None
_request_semaphore_limit = 0


async def _get_client() -> CopilotClient:
    """Return a shared CopilotClient, starting it on first use."""
    global _client

    if _client is None:
        token = settings.GITHUB_TOKEN or os.environ.get("GITHUB_TOKEN", "")
        options: CopilotClientOptions | None = {"github_token": token} if token else None
        _client = CopilotClient(options)
        await _client.start()
    return _client


def _get_request_semaphore() -> asyncio.Semaphore:
    global _request_semaphore, _request_semaphore_limit

    if _request_semaphore is None or _request_semaphore_limit != COPILOT_REQUEST_CONCURRENCY:
        _request_semaphore = asyncio.Semaphore(COPILOT_REQUEST_CONCURRENCY)
        _request_semaphore_limit = COPILOT_REQUEST_CONCURRENCY
    return _request_semaphore


async def shutdown_client() -> None:
    """Stop the shared Copilot client."""
    global _client

    if _client is not None:
        try:
            await _client.stop()
        except Exception as exc:
            logger.warning("Copilot client shutdown had errors: %s", exc)
        _client = None


async def _ask(
    system_prompt: str,
    user_prompt: str,
    *,
    attachments: list[dict] | None = None,
    timeout: float = 120,
) -> str:
    """Create an ephemeral Copilot session, send one prompt, return text."""
    started_at = time.perf_counter()
    attachment_count = len(attachments or [])
    logger.info(
        "Copilot request starting model=%s timeout=%ss attachments=%s prompt_chars=%s",
        settings.COPILOT_MODEL,
        timeout,
        attachment_count,
        len(user_prompt),
    )

    content = ""
    observed_usage_cost = 0.0
    request_error: Exception | None = None

    async with _get_request_semaphore():
        client = await _get_client()
        session = await client.create_session(
            {
                "model": settings.COPILOT_MODEL,
                "system_message": {"mode": "replace", "content": system_prompt},
                "available_tools": [],
                "on_permission_request": PermissionHandler.approve_all,
            }
        )

        def handle_session_event(event) -> None:
            nonlocal observed_usage_cost

            if event.type != SessionEventType.ASSISTANT_USAGE:
                return

            cost = getattr(event.data, "cost", None)
            if isinstance(cost, int | float) and cost > 0:
                observed_usage_cost += float(cost)

        unsubscribe = session.on(handle_session_event)
        try:
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
            unsubscribe()
            try:
                await session.disconnect()
            except Exception as exc:
                logger.warning("Copilot session disconnect had errors: %s", exc)

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


async def _ask_json(
    system_prompt: str,
    user_prompt: str,
    *,
    attachments: list[dict] | None = None,
    timeout: float = 120,
    default: dict | None = None,
) -> dict:
    raw = await _ask(system_prompt, user_prompt, attachments=attachments, timeout=timeout)
    try:
        return _parse_json_response(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        if default is None:
            raise
        return default