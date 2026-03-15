"""FastAPI application entry point."""

import logging
from logging.config import dictConfig
import mimetypes
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select

from illdashboard.config import settings
from illdashboard.copilot_service import shutdown_client
from illdashboard.api import router
from illdashboard.database import async_session, engine
from illdashboard.models import Base, LabFile
from illdashboard.services.markers import backfill_measurement_type_aliases
from illdashboard.services.schema import ensure_runtime_schema
import illdashboard.services.search as search_service


PRELOADABLE_MIME_TYPES = {
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
}


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
                    "fmt": "%(asctime)s %(levelprefix)s %(client_addr)s - \"%(request_line)s\" %(status_code)s",
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


async def preload_uploaded_files() -> int:
    """Seed missing lab file rows from files already present in the upload folder."""
    upload_dir = Path(settings.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)

    async with async_session() as session:
        result = await session.execute(select(LabFile.filepath))
        existing_paths = set(result.scalars().all())

        added = 0
        for file_path in sorted(path for path in upload_dir.iterdir() if path.is_file()):
            if file_path.name in existing_paths:
                continue

            mime_type = PRELOADABLE_MIME_TYPES.get(file_path.suffix.lower())
            if mime_type is None:
                guessed_mime_type, _ = mimetypes.guess_type(file_path.name)
                if guessed_mime_type not in PRELOADABLE_MIME_TYPES.values():
                    continue
                mime_type = guessed_mime_type

            session.add(
                LabFile(
                    filename=file_path.name,
                    filepath=file_path.name,
                    mime_type=mime_type,
                )
            )
            existing_paths.add(file_path.name)
            added += 1

        if added:
            await session.commit()

    return added


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await ensure_runtime_schema(engine)
    # Create FTS5 virtual table for search (not handled by SQLAlchemy metadata)
    async with async_session() as session:
        await backfill_measurement_type_aliases(session)
        await search_service.ensure_search_schema(session)
        await session.commit()
    # Ensure upload directory exists
    Path(settings.UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    preloaded_files = await preload_uploaded_files()
    if preloaded_files:
        logger.info("Preloaded %s uploaded files from disk", preloaded_files)
    async with async_session() as session:
        await search_service.rebuild_lab_search_index(session)
        await session.commit()
    yield
    # Shutdown Copilot SDK client
    await shutdown_client()


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

# Serve uploaded files
upload_path = Path(settings.UPLOAD_DIR)
upload_path.mkdir(parents=True, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=str(upload_path)), name="uploads")
