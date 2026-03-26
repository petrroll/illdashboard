from pathlib import Path

from sqlalchemy.engine import make_url

from illdashboard.config import BACKEND_DIR, _normalize_sqlite_url


def test_normalize_sqlite_url_resolves_relative_paths_from_backend_root():
    normalized = _normalize_sqlite_url("sqlite+aiosqlite:///./data/health.db")

    url = make_url(normalized)
    assert url.get_backend_name() == "sqlite"
    assert Path(url.database) == (BACKEND_DIR / "data" / "health.db").resolve()


def test_normalize_sqlite_url_leaves_memory_databases_unchanged():
    database_url = "sqlite+aiosqlite:///:memory:"

    assert _normalize_sqlite_url(database_url) == database_url