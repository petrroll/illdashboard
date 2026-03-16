"""Shared test fixtures."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from illdashboard import config
from illdashboard.database import create_database_engine, get_db
from illdashboard.models import Base
from illdashboard.services.markers import ensure_marker_groups
from illdashboard.services.search import ensure_search_schema


@pytest.fixture
async def session_factory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "test.db"
    db_url = f"sqlite+aiosqlite:///{db_path}"
    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(upload_dir))
    monkeypatch.setattr(config.settings, "DATABASE_URL", db_url)

    engine = create_database_engine(db_url)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with factory() as session:
        await ensure_marker_groups(session)
        await ensure_search_schema(session)
        await session.commit()

    try:
        yield factory
    finally:
        await engine.dispose()


@pytest.fixture
async def client(session_factory):
    async def _get_db():
        async with session_factory() as session:
            yield session

    with (
        patch("illdashboard.main.start_pipeline_runtime", new=AsyncMock()),
        patch(
            "illdashboard.main.stop_pipeline_runtime",
            new=AsyncMock(),
        ),
        patch(
            "illdashboard.main.prewarm_client",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "illdashboard.main.shutdown_client",
            new=AsyncMock(),
        ),
    ):
        from illdashboard.main import app

        app.dependency_overrides[get_db] = _get_db
        transport = ASGITransport(app=app)
        try:
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                yield ac
        finally:
            app.dependency_overrides.clear()
