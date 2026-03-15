"""Smoke tests for the API."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from illdashboard import config
from illdashboard.database import create_database_engine
from illdashboard.main import preload_uploaded_files
from illdashboard.models import Base, LabFile, LabFileTag, MarkerTag, Measurement, MeasurementType


@pytest.mark.asyncio
async def test_list_files_empty(client):
    resp = await client.get("/api/files")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_preload_uploaded_files_adds_supported_disk_files_without_duplicates(tmp_path):
    db_path = tmp_path / "preload.db"
    db_url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_database_engine(db_url)
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


@pytest.mark.asyncio
async def test_upload_same_file_twice_returns_existing_record(client):
    payload = b"%PDF-1.4 dedupe"

    first = await client.post(
        "/api/files/upload",
        files={"file": ("lab.pdf", payload, "application/pdf")},
    )
    assert first.status_code == 200

    second = await client.post(
        "/api/files/upload",
        files={"file": ("lab-copy.pdf", payload, "application/pdf")},
    )
    assert second.status_code == 200

    first_body = first.json()
    second_body = second.json()
    assert second_body["id"] == first_body["id"]
    assert second_body["filepath"] == first_body["filepath"]

    files_resp = await client.get("/api/files")
    assert files_resp.status_code == 200
    files = files_resp.json()
    assert len(files) == 1

    stored_files = [path for path in Path(config.settings.UPLOAD_DIR).iterdir() if path.is_file()]
    assert len(stored_files) == 1


# ── Re-processing duplicate validation ──────────────────────────────────────

OCR_RESULT = {
    "lab_date": "2025-09-05",
    "raw_text": "Sodium 140 mmol/l\nPotassium 4.2 mmol/l",
    "translated_text_english": "Sodium 140 mmol/l\nPotassium 4.2 mmol/l",
    "summary_english": "This report shows sodium and potassium within the stated reference ranges. No abnormal electrolyte finding is visible in the extracted results.",
    "measurements": [
        {"marker_name": "Sodium", "value": 140, "unit": "mmol/l", "reference_low": 136, "reference_high": 145, "measured_at": "2025-09-05"},
        {"marker_name": "Potassium", "value": 4.2, "unit": "mmol/l", "reference_low": 3.5, "reference_high": 5.1, "measured_at": "2025-09-05"},
    ],
}

MAGNESIUM_RESULT = {
    "lab_date": "2025-09-05",
    "raw_text": "Magnesium 0.85 mmol/l",
    "translated_text_english": "Magnesium 0.85 mmol/l",
    "summary_english": "This report contains a magnesium result in the expected range.",
    "measurements": [
        {"marker_name": "Magnesium", "value": 0.85, "unit": "mmol/l", "reference_low": 0.7, "reference_high": 1.0, "measured_at": "2025-09-05"},
    ],
}

OVERVIEW_RESULT = {
    "lab_date": "2025-09-05",
    "raw_text": "Platelet Count 148 10^9/L\nHemoglobin 156 g/L",
    "translated_text_english": "Platelet Count 148 10^9/L\nHemoglobin 156 g/L",
    "summary_english": "Platelet count is slightly below the stated reference range while hemoglobin is within range.",
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
    "raw_text": "Platelet Count 179 10^9/L\nHemoglobin 154 g/L",
    "translated_text_english": "Platelet Count 179 10^9/L\nHemoglobin 154 g/L",
    "summary_english": "Platelet count and hemoglobin are both within the stated reference ranges on this follow-up.",
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


async def _upload_pdf(client, filename: str = "lab.pdf"):
    """Helper: upload a dummy PDF and return the file_id."""
    payload = f"%PDF-1.4 fake {uuid4().hex}".encode()
    resp = await client.post(
        "/api/files/upload",
        files={"file": (filename, payload, "application/pdf")},
    )
    assert resp.status_code == 200
    return resp.json()["id"]


def _parse_ndjson(text: str) -> list[dict]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _open_current_test_db():
    engine = create_database_engine(config.settings.DATABASE_URL)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return engine, session_factory


@pytest.mark.asyncio
async def test_measurement_requires_existing_measurement_type(client):
    engine, session_factory = _open_current_test_db()

    try:
        async with session_factory() as session:
            lab_file = LabFile(
                filename="lab.pdf",
                filepath="lab.pdf",
                mime_type="application/pdf",
            )
            session.add(lab_file)
            await session.flush()

            session.add(
                Measurement(
                    lab_file_id=lab_file.id,
                    measurement_type_id=999999,
                    value=1.23,
                )
            )

            with pytest.raises(IntegrityError):
                await session.commit()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_reprocess_file_no_duplicates(client):
    """Running OCR twice on the same file should NOT create duplicate measurements."""
    file_id = await _upload_pdf(client)

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
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

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
        resp1 = await client.post(f"/api/files/{file_id}/ocr")
        assert resp1.status_code == 200
        assert len(resp1.json()) == 2

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=updated_result):
        resp2 = await client.post(f"/api/files/{file_id}/ocr")
        assert resp2.status_code == 200
        assert len(resp2.json()) == 1

    resp = await client.get(f"/api/files/{file_id}/measurements")
    measurements = resp.json()
    assert len(measurements) == 1, f"Expected 1 measurement but got {len(measurements)}"
    assert measurements[0]["marker_name"] == "Sodium"
    assert measurements[0]["value"] == 142


@pytest.mark.asyncio
async def test_set_file_tags_allows_extending_existing_tag_list(client):
    file_id = await _upload_pdf(client)

    first_resp = await client.put(f"/api/files/{file_id}/tags", json={"tags": ["baseline"]})
    assert first_resp.status_code == 200
    assert first_resp.json() == ["baseline"]

    second_resp = await client.put(
        f"/api/files/{file_id}/tags",
        json={"tags": ["baseline", "fasting", " fasting ", ""]},
    )
    assert second_resp.status_code == 200
    assert second_resp.json() == ["baseline", "fasting"]

    detail_resp = await client.get(f"/api/files/{file_id}")
    assert detail_resp.status_code == 200
    assert detail_resp.json()["tags"] == ["baseline", "fasting"]


@pytest.mark.asyncio
async def test_set_file_tags_normalizes_source_tags(client):
    file_id = await _upload_pdf(client)

    resp = await client.put(
        f"/api/files/{file_id}/tags",
        json={"tags": [" Source: Synlab ", "baseline", "source:synlab", ""]},
    )

    assert resp.status_code == 200
    assert resp.json() == ["source:synlab", "baseline"]

    detail_resp = await client.get(f"/api/files/{file_id}")
    assert detail_resp.status_code == 200
    assert sorted(detail_resp.json()["tags"]) == ["baseline", "source:synlab"]


@pytest.mark.asyncio
async def test_ocr_adds_normalized_source_tag_and_preserves_existing_tags(client):
    file_id = await _upload_pdf(client, filename="jaeger-report.pdf")

    initial_tag_resp = await client.put(
        f"/api/files/{file_id}/tags",
        json={"tags": ["fasting"]},
    )
    assert initial_tag_resp.status_code == 200

    source_result = {
        **OCR_RESULT,
        "source": "Dr. Jaeger Lab",
    }

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=source_result), patch(
        "illdashboard.services.ocr.normalize_source_name",
        new_callable=AsyncMock,
        return_value="jaeger",
    ):
        resp = await client.post(f"/api/files/{file_id}/ocr")

    assert resp.status_code == 200

    detail_resp = await client.get(f"/api/files/{file_id}")
    assert detail_resp.status_code == 200
    assert detail_resp.json()["tags"] == ["fasting", "source:jaeger"]


@pytest.mark.asyncio
async def test_ocr_persists_raw_and_translated_text_for_non_lab_documents(client):
    file_id = await _upload_pdf(client, filename="admin-note.pdf")

    non_lab_result = {
        "lab_date": None,
        "source": None,
        "raw_text": "Vystavena faktura za administrativni poplatek.",
        "translated_text_english": "Invoice issued for an administrative fee.",
        "summary_english": "This document is an administrative invoice rather than a lab report.",
        "measurements": [],
    }

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=non_lab_result):
        resp = await client.post(f"/api/files/{file_id}/ocr")

    assert resp.status_code == 200
    assert resp.json() == []

    detail_resp = await client.get(f"/api/files/{file_id}")
    assert detail_resp.status_code == 200
    body = detail_resp.json()
    assert body["ocr_text_raw"] == "Vystavena faktura za administrativni poplatek."
    assert body["ocr_text_english"] == "Invoice issued for an administrative fee."
    assert body["ocr_summary_english"] == "This document is an administrative invoice rather than a lab report."


@pytest.mark.asyncio
async def test_search_finds_files_by_translated_text_tags_and_measurements(client):
    lab_file_id = await _upload_pdf(client, filename="iron-panel.pdf")
    note_file_id = await _upload_pdf(client, filename="admin-note.pdf")

    ferritin_result = {
        "lab_date": "2025-09-05",
        "source": None,
        "raw_text": "Feritin 414 ug/l\nPacient nalacno.",
        "translated_text_english": "Ferritin 414 ug/l\nPatient fasting.",
        "summary_english": "This fasting lab report shows an elevated ferritin result.",
        "measurements": [
            {
                "marker_name": "Ferritin",
                "value": 414,
                "unit": "ug/l",
                "reference_low": 30,
                "reference_high": 400,
                "measured_at": "2025-09-05",
            }
        ],
    }
    note_result = {
        "lab_date": None,
        "source": None,
        "raw_text": "Doporuceni k administrativni kontrole.",
        "translated_text_english": "Recommendation for an administrative review.",
        "summary_english": "This document contains an administrative follow-up recommendation.",
        "measurements": [],
    }

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, side_effect=[ferritin_result, note_result]):
        first_resp = await client.post(f"/api/files/{lab_file_id}/ocr")
        second_resp = await client.post(f"/api/files/{note_file_id}/ocr")

    assert first_resp.status_code == 200
    assert second_resp.status_code == 200

    fasting_tag_resp = await client.put(f"/api/files/{lab_file_id}/tags", json={"tags": ["fasting", "iron"]})
    assert fasting_tag_resp.status_code == 200

    ferritin_search = await client.get("/api/search", params={"q": "ferritin"})
    assert ferritin_search.status_code == 200
    ferritin_body = ferritin_search.json()
    assert len(ferritin_body) == 1
    assert ferritin_body[0]["file_id"] == lab_file_id
    assert ferritin_body[0]["marker_names"] == ["Ferritin"]
    snippet_sources = [s["source"] for s in ferritin_body[0]["snippets"]]
    assert any(s in {"summary", "translated_text", "measurements"} for s in snippet_sources)

    summary_search = await client.get("/api/search", params={"q": "elevated ferritin"})
    assert summary_search.status_code == 200
    summary_body = summary_search.json()
    assert len(summary_body) == 1
    assert summary_body[0]["file_id"] == lab_file_id
    assert any(s["source"] == "summary" for s in summary_body[0]["snippets"])

    translated_search = await client.get("/api/search", params={"q": "administrative"})
    assert translated_search.status_code == 200
    translated_body = translated_search.json()
    assert len(translated_body) == 1
    assert translated_body[0]["file_id"] == note_file_id
    translated_sources = [s["source"] for s in translated_body[0]["snippets"]]
    assert any(s in {"summary", "translated_text"} for s in translated_sources)

    tagged_search = await client.get("/api/search", params=[("q", "patient"), ("tags", "fasting")])
    assert tagged_search.status_code == 200
    tagged_body = tagged_search.json()
    assert len(tagged_body) == 1
    assert tagged_body[0]["file_id"] == lab_file_id
    assert tagged_body[0]["tags"] == ["fasting", "iron"]


@pytest.mark.asyncio
async def test_ocr_source_normalization_uses_filename_and_seen_sources(client):
    existing_file_id = await _upload_pdf(client)
    new_file_id = await _upload_pdf(client, filename="synlab_followup.pdf")

    existing_tag_resp = await client.put(
        f"/api/files/{existing_file_id}/tags",
        json={"tags": ["source:synlab"]},
    )
    assert existing_tag_resp.status_code == 200

    source_result = {
        **OCR_RESULT,
        "source": "Syn Lab CZ",
    }

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=source_result), patch(
        "illdashboard.services.ocr.normalize_source_name",
        new_callable=AsyncMock,
        return_value="synlab",
    ) as normalize_source_mock:
        resp = await client.post(f"/api/files/{new_file_id}/ocr")

    assert resp.status_code == 200
    normalize_source_mock.assert_awaited_once_with("Syn Lab CZ", "synlab_followup.pdf", ["synlab"])

    engine, session_factory = _open_current_test_db()
    try:
        async with session_factory() as session:
            result = await session.execute(
                select(LabFileTag.tag).where(LabFileTag.lab_file_id == new_file_id).order_by(LabFileTag.id.asc())
            )
            assert result.scalars().all() == ["source:synlab"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_ocr_persists_qualitative_measurements_and_excludes_them_from_numeric_overview(client):
    file_id = await _upload_pdf(client, filename="immunology.pdf")

    qualitative_result = {
        "lab_date": "2023-01-20",
        "measurements": [
            {
                "marker_name": "Chlamydia psittaci IgG",
                "value": "negative",
                "unit": None,
                "reference_low": None,
                "reference_high": None,
                "measured_at": "2023-01-20",
                "page_number": 2,
            },
            {
                "marker_name": "Varicella-zoster IgG",
                "value": True,
                "unit": None,
                "reference_low": None,
                "reference_high": None,
                "measured_at": "2023-01-20",
                "page_number": 2,
            },
            {
                "marker_name": "Ferritin",
                "value": 414,
                "unit": "ug/l",
                "reference_low": None,
                "reference_high": None,
                "measured_at": "2023-01-20",
                "page_number": 2,
            },
        ],
    }

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=qualitative_result):
        resp = await client.post(f"/api/files/{file_id}/ocr")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 3
    assert body[0]["marker_name"] == "Chlamydia psittaci IgG"
    assert body[0]["value"] is None
    assert body[0]["qualitative_value"] == "negative"
    assert body[1]["marker_name"] == "Varicella-zoster IgG"
    assert body[1]["value"] is None
    assert body[1]["qualitative_value"] == "positive"
    assert body[2]["marker_name"] == "Ferritin"
    assert body[2]["value"] == 414
    assert body[2]["qualitative_value"] is None

    file_measurements_resp = await client.get(f"/api/files/{file_id}/measurements")
    assert file_measurements_resp.status_code == 200
    file_measurements = file_measurements_resp.json()
    assert [item["marker_name"] for item in file_measurements] == [
        "Chlamydia psittaci IgG",
        "Ferritin",
        "Varicella-zoster IgG",
    ]

    overview_resp = await client.get("/api/measurements/overview")
    assert overview_resp.status_code == 200
    overview = overview_resp.json()
    marker_names = [marker["marker_name"] for group in overview for marker in group["markers"]]
    assert marker_names == ["Ferritin"]


@pytest.mark.asyncio
async def test_set_marker_tags_allows_extending_existing_tag_list(client):
    file_id = await _upload_pdf(client)

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
        ocr_resp = await client.post(f"/api/files/{file_id}/ocr")
    assert ocr_resp.status_code == 200

    first_resp = await client.put("/api/markers/Sodium/tags", json={"tags": ["electrolyte"]})
    assert first_resp.status_code == 200
    assert first_resp.json() == ["electrolyte", "group:Electrolytes", "singlemeasurement"]

    second_resp = await client.put(
        "/api/markers/Sodium/tags",
        json={"tags": ["electrolyte", "fasting", "fasting"]},
    )
    assert second_resp.status_code == 200
    assert second_resp.json() == ["electrolyte", "fasting", "group:Electrolytes", "singlemeasurement"]

    detail_resp = await client.get("/api/measurements/detail", params={"marker_name": "Sodium"})
    assert detail_resp.status_code == 200
    assert detail_resp.json()["tags"] == ["electrolyte", "fasting", "group:Electrolytes", "singlemeasurement"]


@pytest.mark.asyncio
async def test_batch_and_unprocessed_share_streaming_behavior(client):
    """Both streaming OCR endpoints should emit completion and persist processed data."""
    first_file_id = await _upload_pdf(client)
    second_file_id = await _upload_pdf(client)

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
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
    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=batch_result):
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

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_RESULT):
        resp = await client.post(f"/api/files/{first_file_id}/ocr")
        assert resp.status_code == 200

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_UPDATED_RESULT):
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

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
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

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=MAGNESIUM_RESULT):
        resp = await client.post(f"/api/files/{file_id}/ocr")
        assert resp.status_code == 200

    overview_resp = await client.get("/api/measurements/overview")
    assert overview_resp.status_code == 200
    overview = overview_resp.json()

    assert len(overview) == 1
    assert overview[0]["group_name"] == "Electrolytes"
    assert [item["marker_name"] for item in overview[0]["markers"]] == ["Magnesium"]


@pytest.mark.asyncio
async def test_measurement_overview_exposes_and_filters_by_derived_tags(client):
    first_file_id = await _upload_pdf(client)
    second_file_id = await _upload_pdf(client)

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_RESULT):
        resp = await client.post(f"/api/files/{first_file_id}/ocr")
        assert resp.status_code == 200

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_UPDATED_RESULT):
        resp = await client.post(f"/api/files/{second_file_id}/ocr")
        assert resp.status_code == 200

    overview_resp = await client.get(
        "/api/measurements/overview",
        params=[("tags", "group:Blood Function"), ("tags", "multiplemeasurements")],
    )
    assert overview_resp.status_code == 200

    overview = overview_resp.json()
    assert len(overview) == 1

    platelet = next(item for item in overview[0]["markers"] if item["marker_name"] == "Platelet Count")
    assert "group:Blood Function" in platelet["tags"]
    assert "multiplemeasurements" in platelet["tags"]

    detail_resp = await client.get(
        "/api/measurements/detail",
        params={"marker_name": "Platelet Count"},
    )
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert "group:Blood Function" in detail["tags"]
    assert "multiplemeasurements" in detail["tags"]


@pytest.mark.asyncio
async def test_measurement_overview_and_detail_include_source_file_tags(client):
    first_file_id = await _upload_pdf(client)
    second_file_id = await _upload_pdf(client)

    first_tag_resp = await client.put(
        f"/api/files/{first_file_id}/tags",
        json={"tags": ["fasting", "clinic:a"]},
    )
    assert first_tag_resp.status_code == 200

    second_tag_resp = await client.put(
        f"/api/files/{second_file_id}/tags",
        json={"tags": ["fasting", "home"]},
    )
    assert second_tag_resp.status_code == 200

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_RESULT):
        resp = await client.post(f"/api/files/{first_file_id}/ocr")
        assert resp.status_code == 200

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_UPDATED_RESULT):
        resp = await client.post(f"/api/files/{second_file_id}/ocr")
        assert resp.status_code == 200

    overview_resp = await client.get("/api/measurements/overview", params=[("tags", "clinic:a")])
    assert overview_resp.status_code == 200
    overview = overview_resp.json()

    assert len(overview) == 1
    platelet = next(item for item in overview[0]["markers"] if item["marker_name"] == "Platelet Count")
    assert platelet["file_tags"] == ["clinic:a", "fasting", "home"]
    assert "clinic:a" in platelet["tags"]
    assert "home" in platelet["tags"]
    assert "multiplemeasurements" in platelet["marker_tags"]

    detail_resp = await client.get(
        "/api/measurements/detail",
        params={"marker_name": "Platelet Count"},
    )
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["file_tags"] == ["clinic:a", "fasting", "home"]
    assert "clinic:a" in detail["tags"]
    assert "home" in detail["tags"]
    assert "multiplemeasurements" in detail["marker_tags"]


@pytest.mark.asyncio
async def test_marker_tags_endpoint_includes_derived_tags_and_does_not_persist_them(client):
    file_id = await _upload_pdf(client)

    file_tag_resp = await client.put(f"/api/files/{file_id}/tags", json={"tags": ["fasting"]})
    assert file_tag_resp.status_code == 200

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
        resp = await client.post(f"/api/files/{file_id}/ocr")
        assert resp.status_code == 200

    tags_resp = await client.get("/api/tags/markers")
    assert tags_resp.status_code == 200
    assert "group:Electrolytes" in tags_resp.json()
    assert "singlemeasurement" in tags_resp.json()
    assert "fasting" in tags_resp.json()

    update_resp = await client.put(
        "/api/markers/Sodium/tags",
        json={"tags": ["group:Electrolytes", "singlemeasurement", "custom-tag"]},
    )
    assert update_resp.status_code == 200
    assert update_resp.json() == ["custom-tag", "group:Electrolytes", "singlemeasurement"]

    engine, session_factory = _open_current_test_db()
    try:
        async with session_factory() as session:
            measurement_type_result = await session.execute(
                select(MeasurementType).where(MeasurementType.name == "Sodium")
            )
            measurement_type = measurement_type_result.scalar_one()

            tags_result = await session.execute(
                select(MarkerTag.tag).where(MarkerTag.measurement_type_id == measurement_type.id)
            )
            assert tags_result.scalars().all() == ["custom-tag"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_measurements_persist_with_measurement_type_references(client):
    file_id = await _upload_pdf(client)

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
        resp = await client.post(f"/api/files/{file_id}/ocr")
        assert resp.status_code == 200

    engine, session_factory = _open_current_test_db()
    try:
        async with session_factory() as session:
            measurements_result = await session.execute(select(Measurement).order_by(Measurement.id.asc()))
            measurements = measurements_result.scalars().all()
            assert len(measurements) == 2
            assert all(measurement.measurement_type_id is not None for measurement in measurements)

            types_result = await session.execute(select(MeasurementType).order_by(MeasurementType.name.asc()))
            measurement_types = types_result.scalars().all()
            assert [measurement_type.name for measurement_type in measurement_types] == ["Potassium", "Sodium"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_normalize_existing_markers_merges_measurement_types_and_tags(client):
    first_file_id = await _upload_pdf(client)
    second_file_id = await _upload_pdf(client)

    na_result = {
        "lab_date": "2025-09-05",
        "measurements": [
            {
                "marker_name": "Na",
                "value": 140,
                "unit": "mmol/l",
                "reference_low": 136,
                "reference_high": 145,
                "measured_at": "2025-09-05",
            }
        ],
    }
    sodium_result = {
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

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=na_result), patch(
        "illdashboard.services.ocr.normalize_marker_names",
        new_callable=AsyncMock,
        return_value={"Na": "Na"},
    ):
        resp = await client.post(f"/api/files/{first_file_id}/ocr")
        assert resp.status_code == 200

    tag_resp = await client.put("/api/markers/Na/tags", json={"tags": ["electrolyte"]})
    assert tag_resp.status_code == 200
    assert tag_resp.json() == ["electrolyte", "group:Electrolytes", "singlemeasurement"]

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=sodium_result), patch(
        "illdashboard.services.ocr.normalize_marker_names",
        new_callable=AsyncMock,
        return_value={"Sodium": "Sodium"},
    ):
        resp = await client.post(f"/api/files/{second_file_id}/ocr")
        assert resp.status_code == 200

    with patch(
        "illdashboard.services.ocr.normalize_marker_names",
        new_callable=AsyncMock,
        return_value={"Na": "Sodium", "Sodium": "Sodium"},
    ):
        normalize_resp = await client.post("/api/measurements/normalize")

    assert normalize_resp.status_code == 200
    assert normalize_resp.json() == {"updated": 1}

    engine, session_factory = _open_current_test_db()
    try:
        async with session_factory() as session:
            types_result = await session.execute(select(MeasurementType).order_by(MeasurementType.name.asc()))
            measurement_types = types_result.scalars().all()
            assert [measurement_type.name for measurement_type in measurement_types] == ["Sodium"]

            measurements_result = await session.execute(select(Measurement).order_by(Measurement.id.asc()))
            measurements = measurements_result.scalars().all()
            assert len(measurements) == 2
            assert len({measurement.measurement_type_id for measurement in measurements}) == 1

            tags_result = await session.execute(select(MarkerTag))
            tags = tags_result.scalars().all()
            assert len(tags) == 1
            assert tags[0].tag == "electrolyte"
            assert tags[0].measurement_type_id == measurements[0].measurement_type_id
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_measurement_detail_uses_cached_explanation_until_values_change(client):
    first_file_id = await _upload_pdf(client)
    second_file_id = await _upload_pdf(client)

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_RESULT):
        resp = await client.post(f"/api/files/{first_file_id}/ocr")
        assert resp.status_code == 200

    with patch(
        "illdashboard.services.insights.explain_marker_history",
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
        "illdashboard.services.insights.explain_marker_history",
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
        "illdashboard.services.insights.explain_marker_history",
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

    with patch("illdashboard.services.ocr.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_UPDATED_RESULT):
        resp = await client.post(f"/api/files/{second_file_id}/ocr")
        assert resp.status_code == 200

    with patch(
        "illdashboard.services.insights.explain_marker_history",
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
