from __future__ import annotations

import sqlite3

import pytest

from illdashboard import cli, config


def test_snapshot_copies_data_and_repoints_uploads(tmp_path, monkeypatch: pytest.MonkeyPatch):
    source = tmp_path / "main"
    uploads = source / "uploads"
    uploads.mkdir(parents=True)
    (uploads / "report.pdf").write_bytes(b"pdf")
    with sqlite3.connect(source / "health.db") as conn:
        conn.execute("CREATE TABLE lab_files (id INTEGER PRIMARY KEY, filepath TEXT)")
        conn.execute("INSERT INTO lab_files (filepath) VALUES (?)", (str(uploads / "report.pdf"),))
        conn.execute("INSERT INTO lab_files (filepath) VALUES (?)", ("/elsewhere/other.pdf",))
    conn.close()
    monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(uploads))
    monkeypatch.setattr(config.settings, "DATABASE_URL", f"sqlite+aiosqlite:///{source / 'health.db'}")
    monkeypatch.setattr(config.settings, "MEDICATIONS_DATABASE_URL", f"sqlite+aiosqlite:///{source / 'missing.db'}")

    target = tmp_path / "instance"
    assert cli.main(["snapshot", str(target)]) == 0

    copied_upload = target / "uploads" / "report.pdf"
    with sqlite3.connect(target / "health.db") as conn:
        paths = [row[0] for row in conn.execute("SELECT filepath FROM lab_files ORDER BY id")]
    conn.close()
    assert paths == [str(copied_upload), "/elsewhere/other.pdf"]
    assert copied_upload.read_bytes() == b"pdf"
    # Deleting the instance copy must leave the main upload intact.
    copied_upload.unlink()
    assert (uploads / "report.pdf").exists()
    assert not (target / "medications.db").exists()

    with pytest.raises(SystemExit):
        cli.main(["snapshot", str(uploads)])
