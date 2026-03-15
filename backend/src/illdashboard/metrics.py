"""Simple JSON-based persistence for app-level metrics."""

from __future__ import annotations

import json
from pathlib import Path

from illdashboard.config import settings

_METRICS_FILE = Path(settings.UPLOAD_DIR).parent / "metrics.json"

_PREMIUM_REQUESTS_KEY = "premium_requests_used"


def _load() -> dict:
    try:
        return json.loads(_METRICS_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save(data: dict) -> None:
    _METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)
    _METRICS_FILE.write_text(json.dumps(data))


def store_premium_requests(used: float) -> None:
    """Persist the SDK-reported premium_interactions used_requests count."""
    data = _load()
    data[_PREMIUM_REQUESTS_KEY] = used
    _save(data)


def get_premium_requests_used() -> float | None:
    """Return the last known premium_interactions used_requests count, or None if unavailable."""
    return _load().get(_PREMIUM_REQUESTS_KEY)
