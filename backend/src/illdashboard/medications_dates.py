from __future__ import annotations

import calendar
from datetime import date

EPISODE_DATE_FORMAT_HINT = "Use YYYY-MM or YYYY-MM-DD."


def normalize_episode_date(
    value: str | None,
    *,
    field_name: str,
    allow_blank: bool = False,
) -> str | None:
    if value is None:
        if allow_blank:
            return None
        raise ValueError(f"{field_name} is required.")

    normalized = value.strip()
    if not normalized:
        if allow_blank:
            return None
        raise ValueError(f"{field_name} is required.")

    _parse_episode_date(normalized, as_end=False)
    return normalized


def parse_episode_start(value: str) -> date:
    return _parse_episode_date(value, as_end=False)


def parse_episode_end(value: str) -> date:
    return _parse_episode_date(value, as_end=True)


def _parse_episode_date(value: str, *, as_end: bool) -> date:
    if len(value) == 7:
        year_text, month_text = value.split("-", 1)
        year = int(year_text)
        month = int(month_text)
        day = calendar.monthrange(year, month)[1] if as_end else 1
        return date(year, month, day)

    if len(value) == 10:
        year_text, month_text, day_text = value.split("-", 2)
        return date(int(year_text), int(month_text), int(day_text))

    raise ValueError(EPISODE_DATE_FORMAT_HINT)
