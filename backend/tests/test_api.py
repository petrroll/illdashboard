"""Smoke tests for the API."""

import json
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from illdashboard.main import preload_uploaded_files
from illdashboard.models import Base, LabFile


@pytest.mark.asyncio
async def test_list_files_empty(client):
    resp = await client.get("/api/files")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_preload_uploaded_files_adds_supported_disk_files_without_duplicates(tmp_path):
    db_path = tmp_path / "preload.db"
    db_url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(db_url, echo=False)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir()
    (upload_dir / "existing.pdf").write_bytes(b"%PDF-1.4 existing")
    (upload_dir / "scan.png").write_bytes(b"png")
    (upload_dir / "notes.txt").write_text("ignore me")

    from illdashboard import config, main

    original_upload = config.settings.UPLOAD_DIR
    original_db = config.settings.DATABASE_URL
    original_session = main.async_session

    config.settings.UPLOAD_DIR = str(upload_dir)
    config.settings.DATABASE_URL = db_url
    main.async_session = session_factory

    try:
        async with session_factory() as session:
            session.add(
                LabFile(
                    filename="original-name.pdf",
                    filepath="existing.pdf",
                    mime_type="application/pdf",
                )
            )
            await session.commit()

        added = await preload_uploaded_files()
        assert added == 1

        async with session_factory() as session:
            result = await session.execute(select(LabFile).order_by(LabFile.filepath.asc()))
            files = result.scalars().all()

        assert [file.filepath for file in files] == ["existing.pdf", "scan.png"]
        assert files[0].filename == "original-name.pdf"
        assert files[1].filename == "scan.png"
        assert files[1].mime_type == "image/png"
    finally:
        main.async_session = original_session
        config.settings.UPLOAD_DIR = original_upload
        config.settings.DATABASE_URL = original_db
        await engine.dispose()


@pytest.mark.asyncio
async def test_list_markers_empty(client):
    resp = await client.get("/api/measurements/markers")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_upload_bad_type(client):
    resp = await client.post(
        "/api/files/upload",
        files={"file": ("test.txt", b"hello", "text/plain")},
    )
    assert resp.status_code == 400


# ── Re-processing duplicate validation ──────────────────────────────────────

OCR_RESULT = {
    "lab_date": "2025-09-05",
    "measurements": [
        {"marker_name": "Sodium", "value": 140, "unit": "mmol/l", "reference_low": 136, "reference_high": 145, "measured_at": "2025-09-05"},
        {"marker_name": "Potassium", "value": 4.2, "unit": "mmol/l", "reference_low": 3.5, "reference_high": 5.1, "measured_at": "2025-09-05"},
    ],
}

MAGNESIUM_RESULT = {
    "lab_date": "2025-09-05",
    "measurements": [
        {"marker_name": "Magnesium", "value": 0.85, "unit": "mmol/l", "reference_low": 0.7, "reference_high": 1.0, "measured_at": "2025-09-05"},
    ],
}

OVERVIEW_RESULT = {
    "lab_date": "2025-09-05",
    "measurements": [
        {
            "marker_name": "Platelet Count",
            "value": 148,
            "unit": "10^9/L",
            "reference_low": 150,
            "reference_high": 400,
            "measured_at": "2025-09-05",
        },
        {
            "marker_name": "Hemoglobin",
            "value": 156,
            "unit": "g/L",
            "reference_low": 135,
            "reference_high": 175,
            "measured_at": "2025-09-05",
        },
    ],
}

OVERVIEW_UPDATED_RESULT = {
    "lab_date": "2025-10-05",
    "measurements": [
        {
            "marker_name": "Platelet Count",
            "value": 179,
            "unit": "10^9/L",
            "reference_low": 150,
            "reference_high": 400,
            "measured_at": "2025-10-05",
        },
        {
            "marker_name": "Hemoglobin",
            "value": 154,
            "unit": "g/L",
            "reference_low": 135,
            "reference_high": 175,
            "measured_at": "2025-10-05",
        },
    ],
}


async def _upload_pdf(client):
    """Helper: upload a dummy PDF and return the file_id."""
    resp = await client.post(
        "/api/files/upload",
        files={"file": ("lab.pdf", b"%PDF-1.4 fake", "application/pdf")},
    )
    assert resp.status_code == 200
    return resp.json()["id"]


def _parse_ndjson(text: str) -> list[dict]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


@pytest.mark.asyncio
async def test_reprocess_file_no_duplicates(client):
    """Running OCR twice on the same file should NOT create duplicate measurements."""
    file_id = await _upload_pdf(client)

    with patch("illdashboard.routes.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
        # First OCR run
        resp1 = await client.post(f"/api/files/{file_id}/ocr")
        assert resp1.status_code == 200
        first_run = resp1.json()
        assert len(first_run) == 2

        # Second OCR run (re-process)
        resp2 = await client.post(f"/api/files/{file_id}/ocr")
        assert resp2.status_code == 200
        second_run = resp2.json()
        assert len(second_run) == 2

    # Verify via measurements endpoint — should still be exactly 2
    resp = await client.get(f"/api/files/{file_id}/measurements")
    assert resp.status_code == 200
    measurements = resp.json()
    assert len(measurements) == 2, f"Expected 2 measurements but got {len(measurements)} — duplicates detected!"

    marker_names = sorted(m["marker_name"] for m in measurements)
    assert marker_names == ["Potassium", "Sodium"]


@pytest.mark.asyncio
async def test_reprocess_replaces_old_values(client):
    """Re-processing with different OCR results should replace old measurements."""
    file_id = await _upload_pdf(client)

    updated_result = {
        "lab_date": "2025-09-05",
        "measurements": [
            {"marker_name": "Sodium", "value": 142, "unit": "mmol/l", "reference_low": 136, "reference_high": 145, "measured_at": "2025-09-05"},
        ],
    }

    with patch("illdashboard.routes.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
        resp1 = await client.post(f"/api/files/{file_id}/ocr")
        assert resp1.status_code == 200
        assert len(resp1.json()) == 2

    with patch("illdashboard.routes.ocr_extract", new_callable=AsyncMock, return_value=updated_result):
        resp2 = await client.post(f"/api/files/{file_id}/ocr")
        assert resp2.status_code == 200
        assert len(resp2.json()) == 1

    resp = await client.get(f"/api/files/{file_id}/measurements")
    measurements = resp.json()
    assert len(measurements) == 1, f"Expected 1 measurement but got {len(measurements)}"
    assert measurements[0]["marker_name"] == "Sodium"
    assert measurements[0]["value"] == 142


@pytest.mark.asyncio
async def test_batch_and_unprocessed_share_streaming_behavior(client):
    """Both streaming OCR endpoints should emit completion and persist processed data."""
    first_file_id = await _upload_pdf(client)
    second_file_id = await _upload_pdf(client)

    with patch("illdashboard.routes.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
        unprocessed_resp = await client.post("/api/files/ocr/unprocessed")

    assert unprocessed_resp.status_code == 200
    unprocessed_messages = _parse_ndjson(unprocessed_resp.text)
    assert unprocessed_messages[-1] == {"type": "complete"}
    unprocessed_done_ids = sorted(
        message["file_id"]
        for message in unprocessed_messages
        if message.get("type") == "progress" and message.get("status") == "done"
    )
    assert unprocessed_done_ids == [first_file_id, second_file_id]

    batch_result = {
        "lab_date": "2025-09-06",
        "measurements": [
            {
                "marker_name": "Sodium",
                "value": 141,
                "unit": "mmol/l",
                "reference_low": 136,
                "reference_high": 145,
                "measured_at": "2025-09-06",
            }
        ],
    }
    with patch("illdashboard.routes.ocr_extract", new_callable=AsyncMock, return_value=batch_result):
        batch_resp = await client.post("/api/files/ocr/batch", json={"file_ids": [first_file_id]})

    assert batch_resp.status_code == 200
    batch_messages = _parse_ndjson(batch_resp.text)
    assert batch_messages[-1] == {"type": "complete"}
    batch_done_ids = [
        message["file_id"]
        for message in batch_messages
        if message.get("type") == "progress" and message.get("status") == "done"
    ]
    assert batch_done_ids == [first_file_id]

    measurements_resp = await client.get(f"/api/files/{first_file_id}/measurements")
    assert measurements_resp.status_code == 200
    measurements = measurements_resp.json()
    assert len(measurements) == 1
    assert measurements[0]["value"] == 141


@pytest.mark.asyncio
async def test_measurement_overview_groups_latest_and_previous_values(client):
    first_file_id = await _upload_pdf(client)
    second_file_id = await _upload_pdf(client)

    with patch("illdashboard.routes.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_RESULT):
        resp = await client.post(f"/api/files/{first_file_id}/ocr")
        assert resp.status_code == 200

    with patch("illdashboard.routes.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_UPDATED_RESULT):
        resp = await client.post(f"/api/files/{second_file_id}/ocr")
        assert resp.status_code == 200

    overview_resp = await client.get("/api/measurements/overview")
    assert overview_resp.status_code == 200
    overview = overview_resp.json()

    assert len(overview) == 1
    assert overview[0]["group_name"] == "Blood Function"

    platelet = next(item for item in overview[0]["markers"] if item["marker_name"] == "Platelet Count")
    assert platelet["status"] == "in_range"
    assert platelet["latest_measurement"]["value"] == 179
    assert platelet["previous_measurement"]["value"] == 148


@pytest.mark.asyncio
async def test_measurement_overview_groups_electrolytes_separately(client):
    file_id = await _upload_pdf(client)

    with patch("illdashboard.routes.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
        resp = await client.post(f"/api/files/{file_id}/ocr")
        assert resp.status_code == 200

    overview_resp = await client.get("/api/measurements/overview")
    assert overview_resp.status_code == 200
    overview = overview_resp.json()

    assert len(overview) == 1
    assert overview[0]["group_name"] == "Electrolytes"
    assert [item["marker_name"] for item in overview[0]["markers"]] == ["Potassium", "Sodium"]


@pytest.mark.asyncio
async def test_measurement_overview_groups_magnesium_as_electrolyte(client):
    file_id = await _upload_pdf(client)

    with patch("illdashboard.routes.ocr_extract", new_callable=AsyncMock, return_value=MAGNESIUM_RESULT):
        resp = await client.post(f"/api/files/{file_id}/ocr")
        assert resp.status_code == 200

    overview_resp = await client.get("/api/measurements/overview")
    assert overview_resp.status_code == 200
    overview = overview_resp.json()

    assert len(overview) == 1
    assert overview[0]["group_name"] == "Electrolytes"
    assert [item["marker_name"] for item in overview[0]["markers"]] == ["Magnesium"]


@pytest.mark.asyncio
async def test_measurement_detail_uses_cached_explanation_until_values_change(client):
    first_file_id = await _upload_pdf(client)
    second_file_id = await _upload_pdf(client)

    with patch("illdashboard.routes.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_RESULT):
        resp = await client.post(f"/api/files/{first_file_id}/ocr")
        assert resp.status_code == 200

    with patch(
        "illdashboard.routes.explain_marker_history",
        new_callable=AsyncMock,
        side_effect=AssertionError("detail endpoint should not block on insight generation"),
    ) as explain_mock:
        detail_resp = await client.get(
            "/api/measurements/detail",
            params={"marker_name": "Platelet Count"},
        )

    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["explanation"] is None
    assert detail["explanation_cached"] is False
    explain_mock.assert_not_awaited()

    with patch(
        "illdashboard.routes.explain_marker_history",
        new_callable=AsyncMock,
        return_value="cached platelet explanation",
    ) as explain_mock:
        insight_resp = await client.get(
            "/api/measurements/insight",
            params={"marker_name": "Platelet Count"},
        )

    assert insight_resp.status_code == 200
    insight = insight_resp.json()
    assert insight["explanation"] == "cached platelet explanation"
    assert insight["explanation_cached"] is False
    explain_mock.assert_awaited_once()

    with patch(
        "illdashboard.routes.explain_marker_history",
        new_callable=AsyncMock,
        side_effect=AssertionError("cache should prevent regeneration"),
    ) as explain_mock:
        cached_resp = await client.get(
            "/api/measurements/insight",
            params={"marker_name": "Platelet Count"},
        )

    assert cached_resp.status_code == 200
    cached_insight = cached_resp.json()
    assert cached_insight["explanation"] == "cached platelet explanation"
    assert cached_insight["explanation_cached"] is True
    explain_mock.assert_not_awaited()

    cached_detail_resp = await client.get(
        "/api/measurements/detail",
        params={"marker_name": "Platelet Count"},
    )
    assert cached_detail_resp.status_code == 200
    cached_detail = cached_detail_resp.json()
    assert cached_detail["explanation"] == "cached platelet explanation"
    assert cached_detail["explanation_cached"] is True

    with patch("illdashboard.routes.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_UPDATED_RESULT):
        resp = await client.post(f"/api/files/{second_file_id}/ocr")
        assert resp.status_code == 200

    with patch(
        "illdashboard.routes.explain_marker_history",
        new_callable=AsyncMock,
        return_value="fresh platelet explanation",
    ) as explain_mock:
        refreshed_resp = await client.get(
            "/api/measurements/insight",
            params={"marker_name": "Platelet Count"},
        )

    assert refreshed_resp.status_code == 200
    refreshed_detail = refreshed_resp.json()
    assert refreshed_detail["explanation"] == "fresh platelet explanation"
    assert refreshed_detail["explanation_cached"] is False
    explain_mock.assert_awaited_once()
