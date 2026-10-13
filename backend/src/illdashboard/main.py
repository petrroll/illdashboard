"""FastAPI application entry point."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from logging.config import dictConfig
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from illdashboard.api import router
from illdashboard.config import settings
from illdashboard.copilot.client import prewarm_client, shutdown_client
from illdashboard.database import dispose_database_engine, get_async_session_factory
from illdashboard.medications_database import dispose_medications_engine, init_medications_database
from illdashboard.services.pipeline import start_pipeline_runtime, stop_pipeline_runtime


def configure_logging() -> None:
    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
                "access": {
                    "()": "uvicorn.logging.AccessFormatter",
                    "fmt": '%(asctime)s %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
            },
            "handlers": {
                "default": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                    "stream": "ext://sys.stderr",
                },
                "access": {
                    "class": "logging.StreamHandler",
                    "formatter": "access",
                    "stream": "ext://sys.stdout",
                },
            },
            "loggers": {
                "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
                "uvicorn.error": {"handlers": ["default"], "level": "INFO", "propagate": False},
                "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
            },
            "root": {"handlers": ["default"], "level": "INFO"},
        }
    )


configure_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    prewarm_task: asyncio.Task[bool] | None = None
    upload_dir = Path(settings.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)
    session_factory = get_async_session_factory()

    await init_medications_database()
    await start_pipeline_runtime(session_factory)
    prewarm_task = asyncio.create_task(prewarm_client())
    logger.info("Pipeline runtime started")
    try:
        yield
    finally:
        if prewarm_task is not None and not prewarm_task.done():
            prewarm_task.cancel()
            try:
                await prewarm_task
            except asyncio.CancelledError:
                pass
        await stop_pipeline_runtime()
        await shutdown_client()
        await dispose_database_engine()
        await dispose_medications_engine()


app = FastAPI(
    title="Health Dashboard API",
    version="0.1.0",
    lifespan=lifespan,
)

TRUSTED_BROWSER_ORIGINS = frozenset({"http://localhost:5173", "http://localhost:3000"})
SAFE_HTTP_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})


def is_untrusted_browser_request(request: Request) -> bool:
    # CORS alone does not stop "simple" cross-site requests (e.g. bodyless POST): the browser sends them
    # without a preflight and only hides the response, so the side effect still happens. This rejects
    # unsafe requests that a browser marks as coming from another origin. It is not authentication.
    if request.method in SAFE_HTTP_METHODS:
        return False

    # Sec-Fetch-Site is set by the browser and cannot be forged by page scripts. "same-origin" also covers
    # the Vite proxy and private Codespaces forwarding, where changeOrigin rewrites Host but the page and
    # the /api request share one origin.
    fetch_site = request.headers.get("sec-fetch-site")
    if fetch_site in {"same-origin", "none"}:
        return False

    origin = request.headers.get("origin")
    if origin is None:
        # Browsers send Origin on cross-origin unsafe requests, so a missing Origin on a cross-site fetch
        # is suspicious; with no fetch metadata at all this is a non-browser client (CLI, tests).
        return fetch_site is not None
    if origin in TRUSTED_BROWSER_ORIGINS:
        return False
    # Direct (non-proxied) access: the page origin matches the host it is talking to. Scheme is ignored
    # because TLS-terminating port forwarding presents https to the browser but http to uvicorn.
    origin_host = urlsplit(origin).netloc.lower()
    request_host = request.headers.get("host", "").lower()
    return not origin_host or origin_host != request_host


@app.middleware("http")
async def reject_cross_site_unsafe_requests(request: Request, call_next):
    if is_untrusted_browser_request(request):
        logger.warning(
            "Rejected cross-site %s %s (origin=%r, sec-fetch-site=%r)",
            request.method,
            request.url.path,
            request.headers.get("origin"),
            request.headers.get("sec-fetch-site"),
        )
        return JSONResponse({"detail": "Cross-site request rejected"}, status_code=403)
    return await call_next(request)


# Added after the cross-site middleware so CORS stays outermost and still answers preflights itself.
app.add_middleware(
    CORSMiddleware,
    allow_origins=sorted(TRUSTED_BROWSER_ORIGINS),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")

upload_path = Path(settings.UPLOAD_DIR)
upload_path.mkdir(parents=True, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=str(upload_path)), name="uploads")
