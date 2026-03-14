"""Smoke tests for the API."""

import json
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_list_files_empty(client):
    resp = await client.get("/api/files")
    assert resp.status_code == 200
    assert resp.json() == []


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
