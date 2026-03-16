"""Shared test fixtures."""

import os
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from illdashboard.database import create_database_engine, get_db
from illdashboard.models import Base
from illdashboard.services.markers import DEFAULT_GROUP_NAME, ensure_marker_groups
from illdashboard.services.search import ensure_search_schema


def _fake_classify_marker_groups(new_names: list[str], existing_groups: list[str]) -> dict[str, str]:
    """Deterministic test classification matching the previous hardcoded behavior."""
    import re

    _TEST_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
        ("Blood Function", ("wbc", "white blood", "neutroph", "lymph", "monocyt", "eosinoph", "basoph",
                            "platelet", "hemoglobin", "hematocrit", "mcv", "mch", "mchc", "rdw",
                            "reticul", "red blood", "rbc")),
        ("Iron Status", ("ferritin", "iron", "transferrin", "tibc", "uibc")),
        ("Inflammation & Infection", ("crp", "sedimentation", "procalcitonin", "esr")),
        ("Metabolic", ("glucose", "hba1c", "insulin",)),
        ("Kidney Function", ("creatin", "urea", "egfr", "uric acid")),
        ("Electrolytes", ("sodium", "potassium", "chloride", "bicarbonate", "magnesium")),
        ("Urinalysis", ("urine",)),
        ("Lipids", ("cholesterol", "triglycer", "hdl", "ldl")),
        ("Liver Function", ("alt", "ast", "ggt", "alp", "bilirubin", "albumin", "protein")),
        ("Thyroid", ("tsh", "ft4", "free t4", "ft3", "free t3", "thyroid")),
        ("Vitamins & Minerals", ("vitamin", "folate", "folic", "b12", "zinc", "selenium", "calcium")),
        ("Hormones", ("testosterone", "estradiol", "progesterone", "cortisol", "prolactin")),
        ("Immunity & Serology", ("igg", "igm", "iga", "ige", "antibod")),
    ]

    result: dict[str, str] = {}
    for name in new_names:
        lower = name.casefold()
        group = DEFAULT_GROUP_NAME
        for group_name, keywords in _TEST_KEYWORDS:
            if any(kw in lower for kw in keywords):
                group = group_name
                break
        result[name] = group
    return result


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
        await ensure_marker_groups(session)
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
    with patch(
        "illdashboard.services.markers.classify_marker_groups",
        new=AsyncMock(side_effect=_fake_classify_marker_groups),
    ):
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac

    app.dependency_overrides.clear()
    config.settings.UPLOAD_DIR = original_upload
    config.settings.DATABASE_URL = original_db
    await engine.dispose()
