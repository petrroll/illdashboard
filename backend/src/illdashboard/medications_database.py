from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from illdashboard.config import settings
from illdashboard.database import create_database_engine
from illdashboard.medications_models import MedicationsBase

_medications_engine: AsyncEngine | None = None
_medications_session_factory: async_sessionmaker[AsyncSession] | None = None
_medications_database_url: str | None = None


def get_medications_engine() -> AsyncEngine:
    global _medications_database_url, _medications_engine, _medications_session_factory

    database_url = settings.MEDICATIONS_DATABASE_URL
    if _medications_engine is None or _medications_database_url != database_url:
        _ensure_sqlite_parent_dir(database_url)
        _medications_engine = create_database_engine(database_url)
        _medications_session_factory = async_sessionmaker(
            _medications_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        _medications_database_url = database_url

    return _medications_engine


def get_medications_session_factory() -> async_sessionmaker[AsyncSession]:
    get_medications_engine()
    if _medications_session_factory is None:
        raise RuntimeError("Medication session factory was not initialized.")
    return _medications_session_factory


async def get_medications_db() -> AsyncGenerator[AsyncSession, None]:
    session_factory = get_medications_session_factory()
    async with session_factory() as session:
        yield session


async def init_medications_database() -> None:
    async with get_medications_engine().begin() as conn:
        await conn.run_sync(MedicationsBase.metadata.create_all)


async def reset_medications_database() -> None:
    async with get_medications_engine().begin() as conn:
        await conn.run_sync(MedicationsBase.metadata.drop_all)
        await conn.run_sync(MedicationsBase.metadata.create_all)


async def dispose_medications_engine() -> None:
    global _medications_database_url, _medications_engine, _medications_session_factory

    if _medications_engine is not None:
        await _medications_engine.dispose()
    _medications_engine = None
    _medications_session_factory = None
    _medications_database_url = None


def _ensure_sqlite_parent_dir(database_url: str) -> None:
    url = make_url(database_url)
    if url.get_backend_name() != "sqlite" or not url.database or url.database == ":memory:":
        return
    Path(url.database).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
