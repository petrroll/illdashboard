"""FastAPI application entry point."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from logging.config import dictConfig
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")

upload_path = Path(settings.UPLOAD_DIR)
upload_path.mkdir(parents=True, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=str(upload_path)), name="uploads")
