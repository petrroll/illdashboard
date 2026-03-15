"""Simple JSON-based persistence for app-level metrics."""

from __future__ import annotations

import json
from pathlib import Path

from illdashboard.config import settings

_METRICS_FILE = Path(settings.UPLOAD_DIR).parent / "metrics.json"

_PREMIUM_REQUESTS_KEY = "premium_requests_used"


def _coerce_float(value: object) -> float | None:
    if isinstance(value, int | float):
        return float(value)
    return None


def _load() -> dict:
    try:
        data = json.loads(_METRICS_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

    if not isinstance(data, dict):
        return {}
    return data


def _save(data: dict) -> None:
    _METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)
    _METRICS_FILE.write_text(json.dumps(data))


def store_premium_requests(used: float) -> None:
    """Persist the cumulative premium request units used by this app."""
    data = _load()
    data[_PREMIUM_REQUESTS_KEY] = used
    _save(data)


def add_premium_requests(used: float) -> None:
    """Add premium request units consumed by a single Copilot call."""
    if used <= 0:
        return

    data = _load()
    current = _coerce_float(data.get(_PREMIUM_REQUESTS_KEY)) or 0.0
    data[_PREMIUM_REQUESTS_KEY] = current + used
    _save(data)


def get_premium_requests_used() -> float | None:
    """Return the cumulative premium request units used by this app."""
    return _coerce_float(_load().get(_PREMIUM_REQUESTS_KEY))
