"""Helpers for storing original upload names alongside hashed disk paths."""

from __future__ import annotations

import logging
import re
from datetime import UTC, date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

ORIGINAL_NAME_SIDECAR_SUFFIX = ".originalName"


def original_name_sidecar_path(file_path: Path) -> Path:
    return file_path.with_name(f"{file_path.name}{ORIGINAL_NAME_SIDECAR_SUFFIX}")


def is_original_name_sidecar(file_path: Path) -> bool:
    return file_path.name.endswith(ORIGINAL_NAME_SIDECAR_SUFFIX)


def write_original_name_sidecar(file_path: Path, original_name: str | None) -> None:
    if not original_name:
        return
    # Uploaded files live on disk under UUID-based names, so the sidecar lets
    # preload/reset restore the human-readable client filename later.
    original_name_sidecar_path(file_path).write_text(original_name, encoding="utf-8")


def read_original_name_sidecar(file_path: Path) -> str | None:
    sidecar_path = original_name_sidecar_path(file_path)
    if not sidecar_path.is_file():
        return None
    try:
        original_name = sidecar_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        logger.warning("Could not read original-name sidecar for %s: %s", file_path.name, exc)
        return None
    return original_name or None


def delete_original_name_sidecar(file_path: Path) -> None:
    original_name_sidecar_path(file_path).unlink(missing_ok=True)


_ISO_FILENAME_DATE = re.compile(r"(?<!\d)((?:19|20)\d{2})[-_.]?(\d{2})[-_.]?(\d{2})(?!\d)")
_DAY_FIRST_FILENAME_DATE = re.compile(r"(?<!\d)(\d{1,2})[-_.](\d{1,2})[-_.]((?:19|20)\d{2})(?!\d)")


def filename_date(filename: str | None) -> datetime | None:
    """Return the single full date in a user-chosen filename, or None when absent or ambiguous."""
    if not filename:
        return None
    stem = Path(filename).stem
    candidates: set[date] = set()
    for pattern, order in ((_ISO_FILENAME_DATE, (0, 1, 2)), (_DAY_FIRST_FILENAME_DATE, (2, 1, 0))):
        for match in pattern.finditer(stem):
            year, month, day = (int(match.group(index + 1)) for index in order)
            try:
                candidates.add(date(year, month, day))
            except ValueError:
                continue
    # Month-only names like "2024-01-report" do not match: inventing a day would fake precision.
    if len(candidates) != 1:
        return None
    (found,) = candidates
    return datetime(found.year, found.month, found.day, tzinfo=UTC)
