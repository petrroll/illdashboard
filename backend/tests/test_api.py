"""Smoke tests for the API."""

import asyncio
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
from illdashboard.models import Base, LabFile, LabFileTag, MarkerTag, Measurement, MeasurementType, RescalingRule
from illdashboard.services import ocr_workflow as ocr_service


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

OCR_RESULT_WITH_SOURCE = {
    **OCR_RESULT,
    "source": "jaeger",
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

LATE_RANGELESS_RESULT = {
    "lab_date": "2023-01-10",
    "measurements": [
        {
            "marker_name": "Ferritin",
            "value": 280,
            "unit": "ug/l",
            "reference_low": None,
            "reference_high": None,
            "measured_at": "2023-01-10",
        },
    ],
}

LATE_RANGE_RESULT = {
    "lab_date": "2023-02-10",
    "measurements": [
        {
            "marker_name": "Ferritin",
            "value": 320,
            "unit": "ug/l",
            "reference_low": 30,
            "reference_high": 400,
            "measured_at": "2023-02-10",
        },
    ],
}

FOLLOWUP_WITHOUT_RANGE_RESULT = {
    "lab_date": "2023-03-10",
    "measurements": [
        {
            "marker_name": "Ferritin",
            "value": 414,
            "unit": "ug/l",
            "reference_low": None,
            "reference_high": None,
            "measured_at": "2023-03-10",
        },
    ],
}

CANONICAL_UNIT_RESULT = {
    "lab_date": "2024-10-29",
    "measurements": [
        {
            "marker_name": "Absolute CD4+ T-Helper Cell Count",
            "value": 0.38,
            "unit": "10^9/L",
            "reference_low": None,
            "reference_high": None,
            "measured_at": "2023-02-17",
        },
        {
            "marker_name": "Absolute CD4+ T-Helper Cell Count",
            "value": 432,
            "unit": "Zellen/µl",
            "reference_low": 440,
            "reference_high": 2160,
            "measured_at": "2024-10-29",
        },
    ],
}

CASE_ONLY_CANONICAL_UNIT_RESULT = {
    "lab_date": "2024-10-29",
    "measurements": [
        {
            "marker_name": "Absolute CD4+ T-Helper Cell Count",
            "value": 0.38,
            "unit": "10^9/l",
            "reference_low": None,
            "reference_high": None,
            "measured_at": "2023-02-17",
        },
    ],
}

PLATELETCRIT_RESULT = {
    "lab_date": "2023-02-17",
    "measurements": [
        {
            "marker_name": "Plateletcrit (PCT)",
            "value": 1.03,
            "unit": "ml/l",
            "reference_low": None,
            "reference_high": None,
            "measured_at": "2023-02-17",
        },
    ],
}

EGFR_SECONDS_RESULT = {
    "lab_date": "2024-11-25",
    "measurements": [
        {
            "marker_name": "Estimated Glomerular Filtration Rate (eGFR)",
            "value": 2.07,
            "unit": "ml/s/1.73 m2",
            "reference_low": None,
            "reference_high": None,
            "measured_at": "2024-11-25",
        },
    ],
}

EGFR_SECONDS_COMMA_RESULT = {
    "lab_date": "2024-11-25",
    "measurements": [
        {
            "marker_name": "Estimated Glomerular Filtration Rate (eGFR)",
            "value": 2.07,
            "unit": "ml/s/1,73 m2",
            "reference_low": None,
            "reference_high": None,
            "measured_at": "2024-11-25",
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


async def _wait_for_ocr_job(client, job_id: str, *, timeout: float = 5.0) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    last_payload: dict | None = None

    while asyncio.get_running_loop().time() < deadline:
        response = await client.get(f"/api/files/ocr/jobs/{job_id}")
        assert response.status_code == 200
        payload = response.json()
        last_payload = payload
        if payload["status"] in {"completed", "failed"}:
            return payload
        await asyncio.sleep(0.01)

    raise AssertionError(f"OCR job {job_id} did not finish in time: {last_payload}")


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
                    canonical_value=1.23,
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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
        resp1 = await client.post(f"/api/files/{file_id}/ocr")
        assert resp1.status_code == 200
        assert len(resp1.json()) == 2

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=updated_result):
        resp2 = await client.post(f"/api/files/{file_id}/ocr")
        assert resp2.status_code == 200
        assert len(resp2.json()) == 1

    resp = await client.get(f"/api/files/{file_id}/measurements")
    measurements = resp.json()
    assert len(measurements) == 1, f"Expected 1 measurement but got {len(measurements)}"
    assert measurements[0]["marker_name"] == "Sodium"
    assert measurements[0]["canonical_value"] == 142


@pytest.mark.asyncio
async def test_ocr_persists_original_values_while_marker_views_use_canonical_units(client):
    file_id = await _upload_pdf(client)

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=CANONICAL_UNIT_RESULT), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(return_value={"Absolute CD4+ T-Helper Cell Count": "Absolute CD4+ T-Helper Cell Count"}),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Absolute CD4+ T-Helper Cell Count": "10^9/L"}),
    ), patch(
        "illdashboard.copilot.normalization.infer_rescaling_factors",
        new=AsyncMock(return_value={"zellen/ul=>10^9/l": 0.001}),
    ):
        resp = await client.post(f"/api/files/{file_id}/ocr")

    assert resp.status_code == 200

    file_measurements = await client.get(f"/api/files/{file_id}/measurements")
    assert file_measurements.status_code == 200
    rows = file_measurements.json()
    assert len(rows) == 2
    assert rows[0]["canonical_value"] == 0.38
    assert rows[0]["canonical_unit"] == "10^9/L"
    assert rows[0]["original_value"] == 0.38
    assert rows[0]["original_unit"] == "10^9/L"
    assert rows[1]["canonical_value"] == 0.432
    assert rows[1]["canonical_unit"] == "10^9/L"

    marker_detail = await client.get(
        "/api/measurements/detail",
        params={"marker_name": "Absolute CD4+ T-Helper Cell Count"},
    )
    assert marker_detail.status_code == 200
    detail = marker_detail.json()
    assert detail["canonical_unit"] == "10^9/L"
    assert detail["latest_measurement"]["canonical_value"] == 0.432
    assert detail["previous_measurement"]["canonical_value"] == 0.38
    assert detail["measurements"][0]["original_value"] == 0.38
    assert detail["measurements"][0]["original_unit"] == "10^9/L"


@pytest.mark.asyncio
async def test_ocr_rewrites_equivalent_original_unit_to_canonical_spelling(client):
    file_id = await _upload_pdf(client)

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=CASE_ONLY_CANONICAL_UNIT_RESULT), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(return_value={"Absolute CD4+ T-Helper Cell Count": "Absolute CD4+ T-Helper Cell Count"}),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Absolute CD4+ T-Helper Cell Count": "10^9/L"}),
    ), patch(
        "illdashboard.copilot.normalization.infer_rescaling_factors",
        new=AsyncMock(return_value={}),
    ):
        resp = await client.post(f"/api/files/{file_id}/ocr")

    assert resp.status_code == 200

    file_measurements = await client.get(f"/api/files/{file_id}/measurements")
    assert file_measurements.status_code == 200
    rows = file_measurements.json()
    assert len(rows) == 1
    assert rows[0]["canonical_unit"] == "10^9/L"
    assert rows[0]["original_unit"] == "10^9/L"


@pytest.mark.asyncio
async def test_ocr_persists_and_reuses_rescaling_rules(client):
    first_file_id = await _upload_pdf(client, filename="first.pdf")
    second_file_id = await _upload_pdf(client, filename="second.pdf")

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=CANONICAL_UNIT_RESULT), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(return_value={"Absolute CD4+ T-Helper Cell Count": "Absolute CD4+ T-Helper Cell Count"}),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Absolute CD4+ T-Helper Cell Count": "10^9/L"}),
    ), patch(
        "illdashboard.copilot.normalization.infer_rescaling_factors",
        new=AsyncMock(return_value={"zellen/ul=>10^9/l": 0.001}),
    ) as infer_mock:
        resp = await client.post(f"/api/files/{first_file_id}/ocr")
        assert resp.status_code == 200
        infer_mock.assert_awaited_once()

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=CANONICAL_UNIT_RESULT), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(return_value={"Absolute CD4+ T-Helper Cell Count": "Absolute CD4+ T-Helper Cell Count"}),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Absolute CD4+ T-Helper Cell Count": "10^9/L"}),
    ), patch(
        "illdashboard.copilot.normalization.infer_rescaling_factors",
        new=AsyncMock(side_effect=AssertionError("stored rule should be reused before calling LLM")),
    ) as infer_mock:
        resp = await client.post(f"/api/files/{second_file_id}/ocr")
        assert resp.status_code == 200
        infer_mock.assert_not_awaited()

    rules_resp = await client.get("/api/admin/rescaling-rules")
    assert rules_resp.status_code == 200
    assert rules_resp.json() == [
        {
            "id": 1,
            "original_unit": "Zellen/µl",
            "canonical_unit": "10^9/L",
            "scale_factor": 0.001,
            "marker_name": "Absolute CD4+ T-Helper Cell Count",
        }
    ]

    engine, session_factory = _open_current_test_db()
    try:
        async with session_factory() as session:
            result = await session.execute(select(RescalingRule))
            rules = result.scalars().all()
            assert len(rules) == 1
            assert rules[0].normalized_original_unit == "zellen/ul"
            assert rules[0].normalized_canonical_unit == "10^9/l"
            assert rules[0].scale_factor == 0.001
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_ocr_reuses_rescaling_rule_for_decimal_comma_unit_variant(client):
    first_file_id = await _upload_pdf(client, filename="synlab-first.pdf")
    second_file_id = await _upload_pdf(client, filename="synlab-second.pdf")

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=EGFR_SECONDS_RESULT), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(return_value={"Estimated Glomerular Filtration Rate (eGFR)": "Estimated Glomerular Filtration Rate (eGFR)"}),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Estimated Glomerular Filtration Rate (eGFR)": "mL/min/1.73 m²"}),
    ), patch(
        "illdashboard.copilot.normalization.infer_rescaling_factors",
        new=AsyncMock(return_value={"ml/s/1.73m2=>ml/min/1.73m²": 60.0}),
    ) as infer_mock:
        resp = await client.post(f"/api/files/{first_file_id}/ocr")
        assert resp.status_code == 200
        infer_mock.assert_awaited_once()
        first_payload = resp.json()
        assert first_payload[0]["original_value"] == 2.07
        assert first_payload[0]["canonical_value"] == pytest.approx(124.2)

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=EGFR_SECONDS_COMMA_RESULT), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(return_value={"Estimated Glomerular Filtration Rate (eGFR)": "Estimated Glomerular Filtration Rate (eGFR)"}),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Estimated Glomerular Filtration Rate (eGFR)": "mL/min/1.73 m²"}),
    ), patch(
        "illdashboard.copilot.normalization.infer_rescaling_factors",
        new=AsyncMock(side_effect=AssertionError("stored rule should be reused for decimal-comma unit variants")),
    ) as infer_mock:
        resp = await client.post(f"/api/files/{second_file_id}/ocr")
        assert resp.status_code == 200
        infer_mock.assert_not_awaited()
        second_payload = resp.json()
        assert second_payload[0]["original_value"] == 2.07
        assert second_payload[0]["canonical_value"] == pytest.approx(124.2)
        assert second_payload[0]["original_unit"] == "ml/s/1,73 m2"
        assert second_payload[0]["canonical_unit"] == "mL/min/1.73 m²"


@pytest.mark.asyncio
async def test_ocr_flags_measurements_with_missing_unit_conversion_rules(client):
    file_id = await _upload_pdf(client, filename="missing-conversion.pdf")

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=CANONICAL_UNIT_RESULT), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(return_value={"Absolute CD4+ T-Helper Cell Count": "Absolute CD4+ T-Helper Cell Count"}),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Absolute CD4+ T-Helper Cell Count": "10^9/L"}),
    ), patch(
        "illdashboard.copilot.normalization.infer_rescaling_factors",
        new=AsyncMock(return_value={}),
    ):
        resp = await client.post(f"/api/files/{file_id}/ocr")

    assert resp.status_code == 200
    created_rows = resp.json()
    assert created_rows[0]["unit_conversion_missing"] is False
    assert created_rows[1]["unit_conversion_missing"] is True
    assert created_rows[1]["canonical_value"] == 432
    assert created_rows[1]["canonical_unit"] == "10^9/L"
    assert created_rows[1]["original_unit"] == "Zellen/µl"

    file_measurements_resp = await client.get(f"/api/files/{file_id}/measurements")
    assert file_measurements_resp.status_code == 200
    file_measurements = file_measurements_resp.json()
    assert file_measurements[1]["unit_conversion_missing"] is True

    detail_resp = await client.get(
        "/api/measurements/detail",
        params={"marker_name": "Absolute CD4+ T-Helper Cell Count"},
    )
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["latest_measurement"]["unit_conversion_missing"] is True
    assert detail["status"] == "no_range"

    overview_resp = await client.get("/api/measurements/overview")
    assert overview_resp.status_code == 200
    overview = overview_resp.json()
    marker = next(item for group in overview for item in group["markers"] if item["marker_name"] == "Absolute CD4+ T-Helper Cell Count")
    assert marker["latest_measurement"]["unit_conversion_missing"] is True
    assert marker["status"] == "no_range"
    assert marker["value_min"] == 0.38
    assert marker["value_max"] == 0.38


@pytest.mark.asyncio
async def test_sparkline_ignores_measurements_with_missing_unit_conversion_rules(client):
    file_id = await _upload_pdf(client, filename="missing-conversion-sparkline.pdf")

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=CANONICAL_UNIT_RESULT), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(return_value={"Absolute CD4+ T-Helper Cell Count": "Absolute CD4+ T-Helper Cell Count"}),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Absolute CD4+ T-Helper Cell Count": "10^9/L"}),
    ), patch(
        "illdashboard.copilot.normalization.infer_rescaling_factors",
        new=AsyncMock(return_value={}),
    ):
        resp = await client.post(f"/api/files/{file_id}/ocr")

    assert resp.status_code == 200

    with patch("illdashboard.api.measurements.get_cached_sparkline", return_value=None), patch(
        "illdashboard.api.measurements.generate_sparkline",
        return_value=b"png",
    ) as generate_sparkline_mock:
        sparkline_resp = await client.get(
            "/api/measurements/sparkline",
            params={"marker_name": "Absolute CD4+ T-Helper Cell Count"},
        )

    assert sparkline_resp.status_code == 200
    assert sparkline_resp.content == b"png"
    generate_sparkline_mock.assert_called_once()
    assert generate_sparkline_mock.call_args.kwargs["values"] == [0.38]


@pytest.mark.asyncio
async def test_sparkline_uses_boolean_scale_for_qualitative_marker_history(client):
    file_id = await _upload_pdf(client, filename="qualitative-trend.pdf")

    qualitative_trend_result = {
        "lab_date": "2024-10-29",
        "measurements": [
            {
                "marker_name": "ANA Screening",
                "value": False,
                "unit": None,
                "reference_low": None,
                "reference_high": None,
                "measured_at": "2023-01-20",
            },
            {
                "marker_name": "ANA Screening",
                "value": True,
                "unit": None,
                "reference_low": None,
                "reference_high": None,
                "measured_at": "2024-10-29",
            },
        ],
    }

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=qualitative_trend_result), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new_callable=AsyncMock,
        side_effect=lambda names, _existing: {name: name for name in names},
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new_callable=AsyncMock,
        return_value={},
    ), patch(
        "illdashboard.copilot.normalization.normalize_qualitative_values",
        new_callable=AsyncMock,
        return_value={
            "false": ("negative", False),
            "true": ("positive", True),
        },
    ):
        resp = await client.post(f"/api/files/{file_id}/ocr")

    assert resp.status_code == 200

    overview_resp = await client.get("/api/measurements/overview")
    assert overview_resp.status_code == 200
    overview = overview_resp.json()
    ana_overview = next(
        marker
        for group in overview
        for marker in group["markers"]
        if marker["marker_name"] == "ANA Screening"
    )
    assert ana_overview["has_numeric_history"] is False
    assert ana_overview["has_qualitative_trend"] is True
    assert ana_overview["latest_measurement"]["qualitative_bool"] is True

    with patch("illdashboard.api.measurements.get_cached_sparkline", return_value=None), patch(
        "illdashboard.api.measurements.generate_sparkline",
        return_value=b"png",
    ) as generate_sparkline_mock:
        sparkline_resp = await client.get(
            "/api/measurements/sparkline",
            params={"marker_name": "ANA Screening"},
        )

    assert sparkline_resp.status_code == 200
    assert sparkline_resp.content == b"png"
    generate_sparkline_mock.assert_called_once()
    assert generate_sparkline_mock.call_args.kwargs["values"] == [0.0, 1.0]
    assert generate_sparkline_mock.call_args.kwargs["ref_low"] is None
    assert generate_sparkline_mock.call_args.kwargs["ref_high"] == 0.5


@pytest.mark.asyncio
async def test_marker_views_reuse_latest_known_reference_range_across_history(client):
    first_file_id = await _upload_pdf(client, filename="ferritin-initial.pdf")
    second_file_id = await _upload_pdf(client, filename="ferritin-range.pdf")
    third_file_id = await _upload_pdf(client, filename="ferritin-followup.pdf")

    with patch(
        "illdashboard.copilot.extraction.ocr_extract",
        new_callable=AsyncMock,
        side_effect=[LATE_RANGELESS_RESULT, LATE_RANGE_RESULT, FOLLOWUP_WITHOUT_RANGE_RESULT],
    ), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new_callable=AsyncMock,
        side_effect=lambda names, _existing: {name: name for name in names},
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new_callable=AsyncMock,
        return_value={"Ferritin": "ug/l"},
    ):
        for file_id in (first_file_id, second_file_id, third_file_id):
            resp = await client.post(f"/api/files/{file_id}/ocr")
            assert resp.status_code == 200

    detail_resp = await client.get(
        "/api/measurements/detail",
        params={"marker_name": "Ferritin"},
    )
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["reference_low"] == 30
    assert detail["reference_high"] == 400
    assert detail["status"] == "high"
    assert detail["latest_measurement"]["canonical_reference_low"] is None
    assert detail["latest_measurement"]["canonical_reference_high"] is None
    assert detail["measurements"][-1]["canonical_reference_low"] is None
    assert detail["measurements"][-1]["canonical_reference_high"] is None

    overview_resp = await client.get("/api/measurements/overview")
    assert overview_resp.status_code == 200
    overview = overview_resp.json()
    ferritin = next(item for group in overview for item in group["markers"] if item["marker_name"] == "Ferritin")
    assert ferritin["reference_low"] == 30
    assert ferritin["reference_high"] == 400
    assert ferritin["status"] == "high"

    with patch("illdashboard.api.measurements.get_cached_sparkline", return_value=None), patch(
        "illdashboard.api.measurements.generate_sparkline",
        return_value=b"png",
    ) as generate_sparkline_mock:
        sparkline_resp = await client.get(
            "/api/measurements/sparkline",
            params={"marker_name": "Ferritin"},
        )

    assert sparkline_resp.status_code == 200
    assert sparkline_resp.content == b"png"
    generate_sparkline_mock.assert_called_once()
    assert generate_sparkline_mock.call_args.kwargs["ref_low"] == 30
    assert generate_sparkline_mock.call_args.kwargs["ref_high"] == 400


@pytest.mark.asyncio
async def test_ocr_deterministically_converts_ml_per_l_to_percent(client):
    file_id = await _upload_pdf(client, filename="plateletcrit.pdf")

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=PLATELETCRIT_RESULT), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(return_value={"Plateletcrit (PCT)": "Plateletcrit (PCT)"}),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Plateletcrit (PCT)": "%"}),
    ), patch(
        "illdashboard.copilot.normalization.normalize_source_name",
        new=AsyncMock(return_value=None),
    ), patch(
        "illdashboard.copilot.normalization._ask",
        new=AsyncMock(side_effect=AssertionError("dimensionless ratio conversion should not call the LLM")),
    ) as ask_mock:
        resp = await client.post(f"/api/files/{file_id}/ocr")

    assert resp.status_code == 200
    ask_mock.assert_not_awaited()

    payload = resp.json()
    assert len(payload) == 1
    assert payload[0]["original_value"] == 1.03
    assert payload[0]["original_unit"] == "ml/l"
    assert payload[0]["canonical_value"] == pytest.approx(0.103)
    assert payload[0]["canonical_unit"] == "%"

    rules_resp = await client.get("/api/admin/rescaling-rules")
    assert rules_resp.status_code == 200
    rules = rules_resp.json()
    assert len(rules) == 1
    assert rules[0]["original_unit"] == "ml/l"
    assert rules[0]["canonical_unit"] == "%"
    assert rules[0]["scale_factor"] == pytest.approx(0.1)
    assert rules[0]["marker_name"] == "Plateletcrit (PCT)"


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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=source_result), patch(
        "illdashboard.copilot.normalization.normalize_source_name",
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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=non_lab_result):
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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, side_effect=[ferritin_result, note_result]):
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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=source_result), patch(
        "illdashboard.copilot.normalization.normalize_source_name",
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
async def test_ocr_persists_qualitative_measurements_and_includes_them_in_biomarker_views(client):
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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=qualitative_result), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new_callable=AsyncMock,
        side_effect=lambda names, _existing: {name: name for name in names},
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new_callable=AsyncMock,
        return_value={},
    ), patch(
        "illdashboard.copilot.normalization.normalize_qualitative_values",
        new_callable=AsyncMock,
        return_value={
            "negative": ("negative", False),
            "true": ("positive", True),
        },
    ) as normalize_qualitative_mock:
        resp = await client.post(f"/api/files/{file_id}/ocr")

    assert resp.status_code == 200
    normalize_qualitative_mock.assert_awaited_once()
    body = resp.json()
    assert len(body) == 3
    assert body[0]["marker_name"] == "Chlamydia psittaci IgG"
    assert body[0]["canonical_value"] is None
    assert body[0]["original_qualitative_value"] == "negative"
    assert body[0]["qualitative_bool"] is False
    assert body[0]["qualitative_value"] == "negative"
    assert body[1]["marker_name"] == "Varicella-zoster IgG"
    assert body[1]["canonical_value"] is None
    assert body[1]["original_qualitative_value"] == "true"
    assert body[1]["qualitative_bool"] is True
    assert body[1]["qualitative_value"] == "positive"
    assert body[2]["marker_name"] == "Ferritin"
    assert body[2]["canonical_value"] == 414
    assert body[2]["original_qualitative_value"] is None
    assert body[2]["qualitative_bool"] is None
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
    assert marker_names == ["Ferritin", "Chlamydia psittaci IgG", "Varicella-zoster IgG"]

    chlamydia_overview = next(
        marker
        for group in overview
        for marker in group["markers"]
        if marker["marker_name"] == "Chlamydia psittaci IgG"
    )
    assert chlamydia_overview["latest_measurement"]["qualitative_value"] == "negative"
    assert chlamydia_overview["has_numeric_history"] is False
    assert chlamydia_overview["has_qualitative_trend"] is False
    assert chlamydia_overview["status"] == "no_range"

    markers_resp = await client.get("/api/measurements/markers")
    assert markers_resp.status_code == 200
    assert markers_resp.json() == ["Chlamydia psittaci IgG", "Ferritin", "Varicella-zoster IgG"]

    detail_resp = await client.get(
        "/api/measurements/detail",
        params={"marker_name": "Chlamydia psittaci IgG"},
    )
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["marker_name"] == "Chlamydia psittaci IgG"
    assert detail["has_numeric_history"] is False
    assert detail["has_qualitative_trend"] is False
    assert detail["latest_measurement"]["qualitative_value"] == "negative"
    assert len(detail["measurements"]) == 1

    with patch(
        "illdashboard.services.insights.explain_marker_history",
        new_callable=AsyncMock,
        return_value="qualitative explanation",
    ) as explain_mock:
        insight_resp = await client.get(
            "/api/measurements/insight",
            params={"marker_name": "Chlamydia psittaci IgG"},
        )

    assert insight_resp.status_code == 200
    insight = insight_resp.json()
    assert insight["explanation"] == "qualitative explanation"
    assert insight["explanation_cached"] is False
    explain_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_set_marker_tags_allows_extending_existing_tag_list(client):
    file_id = await _upload_pdf(client)

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
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
async def test_batch_and_unprocessed_share_job_behavior(client):
    """Both batch OCR endpoints should return a job id and persist processed data."""
    first_file_id = await _upload_pdf(client)
    second_file_id = await _upload_pdf(client)

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(return_value={"Sodium": "Sodium", "Potassium": "Potassium"}),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Sodium": "mmol/l", "Potassium": "mmol/l"}),
    ), patch(
        "illdashboard.copilot.normalization.infer_rescaling_factors",
        new=AsyncMock(return_value={}),
    ), patch("illdashboard.copilot.normalization.normalize_source_name", new=AsyncMock(return_value=None)):
        unprocessed_resp = await client.post("/api/files/ocr/unprocessed")

        assert unprocessed_resp.status_code == 200
        unprocessed_job = await _wait_for_ocr_job(client, unprocessed_resp.json()["job_id"])
        assert unprocessed_job["status"] == "completed"
        assert sorted(
            item["file_id"] for item in unprocessed_job["progress"] if item["status"] == "done"
        ) == [first_file_id, second_file_id]

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
    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=batch_result), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(return_value={"Sodium": "Sodium"}),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Sodium": "mmol/l"}),
    ), patch(
        "illdashboard.copilot.normalization.infer_rescaling_factors",
        new=AsyncMock(return_value={}),
    ), patch("illdashboard.copilot.normalization.normalize_source_name", new=AsyncMock(return_value=None)):
        batch_resp = await client.post("/api/files/ocr/batch", json={"file_ids": [first_file_id]})

        assert batch_resp.status_code == 200
        batch_job = await _wait_for_ocr_job(client, batch_resp.json()["job_id"])
        assert batch_job["status"] == "completed"
        assert [item["file_id"] for item in batch_job["progress"] if item["status"] == "done"] == [first_file_id]

    measurements_resp = await client.get(f"/api/files/{first_file_id}/measurements")
    assert measurements_resp.status_code == 200
    measurements = measurements_resp.json()
    assert len(measurements) == 1
    assert measurements[0]["canonical_value"] == 141


@pytest.mark.asyncio
async def test_batch_ocr_job_reports_processing_before_completion(client):
    file_id = await _upload_pdf(client)

    async def slow_extract(_lab):
        await asyncio.sleep(0.02)
        return OCR_RESULT

    with patch("illdashboard.services.ocr_workflow.extract_ocr_result", side_effect=slow_extract), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(return_value={"Sodium": "Sodium", "Potassium": "Potassium"}),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Sodium": "mmol/l", "Potassium": "mmol/l"}),
    ), patch(
        "illdashboard.copilot.normalization.infer_rescaling_factors",
        new=AsyncMock(return_value={}),
    ), patch("illdashboard.copilot.normalization.normalize_source_name", new=AsyncMock(return_value=None)):
        response = await client.post("/api/files/ocr/batch", json={"file_ids": [file_id]})

        assert response.status_code == 200
        job_id = response.json()["job_id"]

        status_resp = await client.get(f"/api/files/ocr/jobs/{job_id}")
        assert status_resp.status_code == 200
        status_payload = status_resp.json()
        assert status_payload["status"] in {"queued", "running"}
        assert any(item["status"] == "processing" for item in status_payload["progress"])

        final_payload = await _wait_for_ocr_job(client, job_id)
        assert final_payload["status"] == "completed"
        assert final_payload["completed_count"] == 1


@pytest.mark.asyncio
async def test_batch_ocr_persists_in_request_order_for_progressive_canonicalization(client):
    first_file_id = await _upload_pdf(client, filename="canonical.pdf")
    second_file_id = await _upload_pdf(client, filename="alias.pdf")

    canonical_result = {
        "lab_date": "2025-09-05",
        "measurements": [
            {
                "marker_name": "Canonical Marker",
                "value": 140,
                "unit": "mmol/l",
                "reference_low": 136,
                "reference_high": 145,
                "measured_at": "2025-09-05",
            }
        ],
    }
    alias_result = {
        "lab_date": "2025-09-06",
        "measurements": [
            {
                "marker_name": "Alias Marker",
                "value": 141,
                "unit": "mmol/l",
                "reference_low": 136,
                "reference_high": 145,
                "measured_at": "2025-09-06",
            }
        ],
    }

    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def extract_side_effect(lab: LabFile):
        if lab.id == first_file_id:
            first_started.set()
            await release_first.wait()
            return canonical_result

        await first_started.wait()
        release_first.set()
        return alias_result

    async def normalize_side_effect(new_names: list[str], existing_canonical: list[str]):
        if new_names == ["Canonical Marker"]:
            return {"Canonical Marker": "Canonical Marker"}
        if new_names == ["Alias Marker"]:
            if "Canonical Marker" in existing_canonical:
                return {"Alias Marker": "Canonical Marker"}
            return {"Alias Marker": "Alias Marker"}
        return {name: name for name in new_names}

    with patch("illdashboard.services.ocr_workflow.extract_ocr_result", side_effect=extract_side_effect), patch(
        "illdashboard.copilot.normalization.normalize_marker_names",
        new=AsyncMock(side_effect=normalize_side_effect),
    ), patch(
        "illdashboard.copilot.normalization.choose_canonical_units",
        new=AsyncMock(return_value={"Canonical Marker": "mmol/l"}),
    ), patch(
        "illdashboard.copilot.normalization.infer_rescaling_factors",
        new=AsyncMock(return_value={}),
    ), patch("illdashboard.copilot.normalization.normalize_source_name", new=AsyncMock(return_value=None)):
        response = await client.post("/api/files/ocr/batch", json={"file_ids": [first_file_id, second_file_id]})

        assert response.status_code == 200
        final_payload = await _wait_for_ocr_job(client, response.json()["job_id"])
        assert final_payload["status"] == "completed"
        assert final_payload["completed_count"] == 2

        engine, session_factory = _open_current_test_db()
        try:
            async with session_factory() as session:
                types_result = await session.execute(select(MeasurementType).order_by(MeasurementType.name.asc()))
                measurement_types = types_result.scalars().all()
                assert [measurement_type.name for measurement_type in measurement_types] == ["Canonical Marker"]

                measurements_result = await session.execute(select(Measurement).order_by(Measurement.id.asc()))
                measurements = measurements_result.scalars().all()
                assert len(measurements) == 2
                assert len({measurement.measurement_type_id for measurement in measurements}) == 1
        finally:
            await engine.dispose()


@pytest.mark.asyncio
async def test_reprocessing_same_source_tag_does_not_fail_or_duplicate(client):
    file_id = await _upload_pdf(client, filename="jaeger.pdf")

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT_WITH_SOURCE):
        first_resp = await client.post(f"/api/files/{file_id}/ocr")
        assert first_resp.status_code == 200

        second_resp = await client.post(f"/api/files/{file_id}/ocr")
        assert second_resp.status_code == 200

    file_resp = await client.get(f"/api/files/{file_id}")
    assert file_resp.status_code == 200
    assert file_resp.json()["tags"].count("source:jaeger") == 1


def test_get_ocr_job_status_prunes_expired_finished_jobs():
    old_job = ocr_service.OcrJobState(
        job_id="expired-job",
        status="completed",
        total=1,
        last_updated_at=100,
        finished_at=100,
    )
    fresh_job = ocr_service.OcrJobState(
        job_id="fresh-job",
        status="completed",
        total=1,
        last_updated_at=1000,
        finished_at=1000,
    )

    original_jobs = dict(ocr_service._ocr_jobs)
    try:
        ocr_service._ocr_jobs.clear()
        ocr_service._ocr_jobs[old_job.job_id] = old_job
        ocr_service._ocr_jobs[fresh_job.job_id] = fresh_job

        with patch("illdashboard.services.ocr_workflow.time.time", return_value=100 + ocr_service.OCR_JOB_TTL_SECONDS + 1):
            with pytest.raises(ocr_service.HTTPException) as exc_info:
                ocr_service.get_ocr_job_status(old_job.job_id)

        assert exc_info.value.status_code == 404
        assert old_job.job_id not in ocr_service._ocr_jobs
        assert fresh_job.job_id in ocr_service._ocr_jobs
    finally:
        ocr_service._ocr_jobs.clear()
        ocr_service._ocr_jobs.update(original_jobs)


def test_get_ocr_job_status_exposes_last_updated_at():
    now = 1235.0
    job = ocr_service.OcrJobState(
        job_id="job-with-timestamp",
        status="running",
        total=1,
        last_updated_at=now - 0.5,
    )

    original_jobs = dict(ocr_service._ocr_jobs)
    try:
        ocr_service._ocr_jobs.clear()
        ocr_service._ocr_jobs[job.job_id] = job

        with patch("illdashboard.services.ocr_workflow.time.time", return_value=now):
            payload = ocr_service.get_ocr_job_status(job.job_id)

        assert payload["last_updated_at"] == now - 0.5
    finally:
        ocr_service._ocr_jobs.clear()
        ocr_service._ocr_jobs.update(original_jobs)


@pytest.mark.asyncio
async def test_measurement_overview_groups_latest_and_previous_values(client):
    first_file_id = await _upload_pdf(client)
    second_file_id = await _upload_pdf(client)

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_RESULT):
        resp = await client.post(f"/api/files/{first_file_id}/ocr")
        assert resp.status_code == 200

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_UPDATED_RESULT):
        resp = await client.post(f"/api/files/{second_file_id}/ocr")
        assert resp.status_code == 200

    overview_resp = await client.get("/api/measurements/overview")
    assert overview_resp.status_code == 200
    overview = overview_resp.json()

    assert len(overview) == 1
    assert overview[0]["group_name"] == "Blood Function"

    platelet = next(item for item in overview[0]["markers"] if item["marker_name"] == "Platelet Count")
    assert platelet["status"] == "in_range"
    assert platelet["latest_measurement"]["canonical_value"] == 179
    assert platelet["previous_measurement"]["canonical_value"] == 148


@pytest.mark.asyncio
async def test_measurement_overview_groups_electrolytes_separately(client):
    file_id = await _upload_pdf(client)

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=MAGNESIUM_RESULT):
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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_RESULT):
        resp = await client.post(f"/api/files/{first_file_id}/ocr")
        assert resp.status_code == 200

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_UPDATED_RESULT):
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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_RESULT):
        resp = await client.post(f"/api/files/{first_file_id}/ocr")
        assert resp.status_code == 200

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_UPDATED_RESULT):
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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OCR_RESULT):
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
async def test_measurement_detail_uses_cached_explanation_until_values_change(client):
    first_file_id = await _upload_pdf(client)
    second_file_id = await _upload_pdf(client)

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_RESULT):
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

    with patch("illdashboard.copilot.extraction.ocr_extract", new_callable=AsyncMock, return_value=OVERVIEW_UPDATED_RESULT):
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
