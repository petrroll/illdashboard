from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import select

from illdashboard.config import settings
from illdashboard.models import Job, LabFile
from illdashboard.services import admin as admin_service
from illdashboard.services import pipeline
from illdashboard.services.upload_metadata import original_name_sidecar_path


@pytest.mark.asyncio
async def test_admin_database_reset_uses_request_database(client, session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    staged_path = upload_dir / "23b11191f78b49e5b98e2f7cf816f706.png"
    staged_path.write_bytes(b"reset-image")
    original_name_sidecar_path(staged_path).write_text("reset-preloaded.png", encoding="utf-8")

    await pipeline.stop_pipeline_runtime()
    async with session_factory() as session:
        session.add(
            LabFile(
                filename="stale-db-only.png",
                filepath=str((upload_dir / "stale-db-only.png").resolve()),
                mime_type="image/png",
                page_count=1,
                status="complete",
            )
        )
        await session.commit()

    with patch.object(admin_service, "purge_sparkline_cache", return_value=0):
        response = await client.delete("/api/admin/database")

    assert response.status_code == 200
    assert response.json() == {"status": "database_reset", "deleted_sparklines": 0}

    files_response = await client.get("/api/files")
    assert files_response.status_code == 200
    files = files_response.json()
    assert [(file["filename"], file["status"]) for file in files] == [("reset-preloaded.png", "uploaded")]

    async with session_factory() as session:
        jobs_result = await session.execute(select(Job).order_by(Job.id.asc()))
        jobs = jobs_result.scalars().all()

    assert jobs == []