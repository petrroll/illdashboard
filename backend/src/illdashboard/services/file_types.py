"""Supported upload-type helpers shared across upload, processing, and export."""

from __future__ import annotations

import mimetypes
from pathlib import Path

# Browsers are inconsistent about local-file MIME types, especially for
# Markdown, so uploads normalize to a canonical app MIME using the filename
# suffix first and only fall back to the reported content type when needed.
UPLOADABLE_SUFFIX_TO_MIME: dict[str, str] = {
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".txt": "text/plain",
    ".md": "text/markdown",
}

_UPLOADABLE_MIME_ALIASES: dict[str, str] = {
    "application/pdf": "application/pdf",
    "image/png": "image/png",
    "image/jpeg": "image/jpeg",
    "image/webp": "image/webp",
    "text/plain": "text/plain",
    "text/markdown": "text/markdown",
    "text/x-markdown": "text/markdown",
}

TEXT_DOCUMENT_MIME_TYPES = frozenset({"text/plain", "text/markdown"})
TEXT_DOCUMENT_SUFFIXES = frozenset({".txt", ".md"})


def normalize_content_type(content_type: str | None) -> str | None:
    if content_type is None:
        return None
    normalized = content_type.split(";", 1)[0].strip().lower()
    return normalized or None


def canonical_upload_mime_type(filename: str | None, content_type: str | None) -> str | None:
    suffix = Path(filename or "file").suffix.lower()
    suffix_mime = UPLOADABLE_SUFFIX_TO_MIME.get(suffix)
    if suffix_mime is not None:
        return suffix_mime
    if suffix:
        return None

    normalized_content_type = normalize_content_type(content_type)
    if normalized_content_type is None:
        return None
    return _UPLOADABLE_MIME_ALIASES.get(normalized_content_type)


def guess_preloadable_mime_type(file_path: str | Path) -> str | None:
    path = Path(file_path)
    suffix_mime = UPLOADABLE_SUFFIX_TO_MIME.get(path.suffix.lower())
    if suffix_mime is not None:
        return suffix_mime

    guessed, _ = mimetypes.guess_type(path.name)
    normalized_content_type = normalize_content_type(guessed)
    if normalized_content_type is None:
        return None
    return _UPLOADABLE_MIME_ALIASES.get(normalized_content_type)


def is_text_document_mime_type(mime_type: str | None) -> bool:
    normalized_content_type = normalize_content_type(mime_type)
    if normalized_content_type is None:
        return False
    canonical_mime = _UPLOADABLE_MIME_ALIASES.get(normalized_content_type, normalized_content_type)
    return canonical_mime in TEXT_DOCUMENT_MIME_TYPES


def is_text_document_path(file_path: str | Path) -> bool:
    return Path(file_path).suffix.lower() in TEXT_DOCUMENT_SUFFIXES
