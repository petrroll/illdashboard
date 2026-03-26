from pathlib import Path

import pytest

from illdashboard import config
from illdashboard.database import dispose_database_engine, get_database_engine


@pytest.mark.asyncio
async def test_get_database_engine_uses_monkeypatched_url_after_module_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    await dispose_database_engine()
    db_path = tmp_path / "lazy-test.db"
    monkeypatch.setattr(config.settings, "DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")

    engine = get_database_engine()

    assert Path(engine.url.database) == db_path.resolve()
    await dispose_database_engine()