"""Off-event-loop execution for PyMuPDF work."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import ParamSpec, TypeVar

import fitz

_P = ParamSpec("_P")
_T = TypeVar("_T")

# PyMuPDF is not thread-safe, even across separate documents, so every call
# shares one thread. That still keeps page rendering off the event loop, where
# it would otherwise stall the API and every pipeline worker.
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pymupdf")


async def run_pymupdf(fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs) -> _T:
    return await asyncio.get_running_loop().run_in_executor(_executor, functools.partial(fn, *args, **kwargs))


def _read_page_count(file_path: Path, mime_type: str) -> int:
    if mime_type == "application/pdf":
        with fitz.open(str(file_path)) as document:
            return document.page_count
    return 1


async def read_page_count(file_path: Path, mime_type: str) -> int:
    return await run_pymupdf(_read_page_count, file_path, mime_type)
