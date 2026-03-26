from collections.abc import AsyncGenerator
from pathlib import Path

from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from illdashboard.config import settings

_engine: AsyncEngine | None = None
_async_session_factory: async_sessionmaker[AsyncSession] | None = None
_database_url: str | None = None


def create_database_engine(database_url: str) -> AsyncEngine:
    engine = create_async_engine(database_url, echo=False)

    if engine.url.get_backend_name() == "sqlite":
        @event.listens_for(engine.sync_engine, "connect")
        def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.close()

    return engine


def get_database_engine() -> AsyncEngine:
    global _database_url, _engine, _async_session_factory

    database_url = settings.DATABASE_URL
    if _engine is None or _database_url != database_url:
        _ensure_sqlite_parent_dir(database_url)
        _engine = create_database_engine(database_url)
        _async_session_factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
        _database_url = database_url

    return _engine


def get_async_session_factory() -> async_sessionmaker[AsyncSession]:
    get_database_engine()
    if _async_session_factory is None:
        raise RuntimeError("Database session factory was not initialized.")
    return _async_session_factory


async def dispose_database_engine() -> None:
    global _database_url, _engine, _async_session_factory

    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _async_session_factory = None
    _database_url = None


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with get_async_session_factory()() as session:
        yield session


def _ensure_sqlite_parent_dir(database_url: str) -> None:
    url = make_url(database_url)
    if url.get_backend_name() != "sqlite" or not url.database or url.database == ":memory:":
        return
    Path(url.database).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
