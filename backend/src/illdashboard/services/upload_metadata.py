"""Helpers for storing original upload names alongside hashed disk paths."""

from __future__ import annotations

import logging
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
