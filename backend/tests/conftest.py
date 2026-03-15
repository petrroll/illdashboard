"""Shared test fixtures."""

import os

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from illdashboard.database import create_database_engine, get_db
from illdashboard.models import Base
from illdashboard.services.search import ensure_search_schema


@pytest.fixture
async def client(tmp_path):
    """Create a test client with a temporary SQLite database."""
    db_path = tmp_path / "test.db"
    db_url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_database_engine(db_url)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        await ensure_search_schema(session)
        await session.commit()

    async def _get_db():
        async with session_factory() as session:
            yield session

    # Patch upload dir to temp
    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir, exist_ok=True)

    from illdashboard import config

    original_upload = config.settings.UPLOAD_DIR
    original_db = config.settings.DATABASE_URL
    config.settings.UPLOAD_DIR = upload_dir
    config.settings.DATABASE_URL = db_url

    from illdashboard.main import app

    app.dependency_overrides[get_db] = _get_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()
    config.settings.UPLOAD_DIR = original_upload
    config.settings.DATABASE_URL = original_db
    await engine.dispose()
