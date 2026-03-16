from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from illdashboard.config import settings
from illdashboard.models import READY_FILE_STATUS, Job, LabFile, Measurement, MeasurementAlias, MeasurementType
from illdashboard.services import pipeline, rescaling


async def _wait_for_file_ready(session_factory, file_id: int, *, timeout_seconds: float = 10.0) -> LabFile:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        async with session_factory() as session:
            result = await session.execute(
                select(LabFile)
                .options(selectinload(LabFile.measurements).selectinload(Measurement.measurement_type))
                .where(LabFile.id == file_id)
            )
            file = result.scalar_one()
            if file.status == READY_FILE_STATUS:
                return file
            if file.status == "error":
                raise AssertionError(file.processing_error or "file entered error state")
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("timed out waiting for file to become ready")
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_upload_and_queue_processing_creates_durable_jobs(client, session_factory):
    upload_response = await client.post(
        "/api/files/upload",
        files={"file": ("report.png", b"fake-image", "image/png")},
    )
    assert upload_response.status_code == 200
    uploaded_file = upload_response.json()
    assert uploaded_file["status"] == "uploaded"
    assert uploaded_file["page_count"] == 1

    queue_response = await client.post(f"/api/files/{uploaded_file['id']}/ocr")
    assert queue_response.status_code == 200
    assert queue_response.json() == {"queued_file_ids": [uploaded_file["id"]]}

    async with session_factory() as session:
        result = await session.execute(
            select(Job).where(Job.file_id == uploaded_file["id"]).order_by(Job.task_type.asc())
        )
        jobs = result.scalars().all()

    assert [job.task_type for job in jobs] == [
        pipeline.TASK_EXTRACT_MEASUREMENT,
        pipeline.TASK_EXTRACT_TEXT,
    ]


@pytest.mark.asyncio
async def test_file_measurements_stay_hidden_until_publish(client):
    upload_response = await client.post(
        "/api/files/upload",
        files={"file": ("report.png", b"fake-image", "image/png")},
    )
    file_id = upload_response.json()["id"]

    queue_response = await client.post(f"/api/files/{file_id}/ocr")
    assert queue_response.status_code == 200

    measurements_response = await client.get(f"/api/files/{file_id}/measurements")
    assert measurements_response.status_code == 200
    assert measurements_response.json() == []


@pytest.mark.asyncio
async def test_pipeline_runtime_processes_file_end_to_end(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    file_path = upload_dir / "runtime-report.png"
    file_path.write_bytes(b"runtime-image")

    async with session_factory() as session:
        lab_file = LabFile(
            filename="runtime-report.png",
            filepath=str(file_path),
            mime_type="image/png",
            page_count=1,
        )
        session.add(lab_file)
        await session.commit()
        await session.refresh(lab_file)
        file_id = lab_file.id

    runtime = pipeline.PipelineRuntime(session_factory)
    await runtime.start()
    try:
        with (
            patch(
                "illdashboard.services.pipeline.copilot_extraction.extract_measurement_batch",
                new=AsyncMock(
                    return_value={
                        "lab_date": "2026-03-15T00:00:00+00:00",
                        "source": "synlab",
                        "measurements": [
                            {
                                "marker_name": "CRP",
                                "value": 15.0,
                                "unit": "mg/L",
                                "reference_low": 0.0,
                                "reference_high": 5.0,
                                "measured_at": "2026-03-15T00:00:00+00:00",
                                "page_number": 1,
                            }
                        ],
                    }
                ),
            ),
            patch(
                "illdashboard.services.pipeline.copilot_extraction.extract_text_batch",
                new=AsyncMock(return_value={"raw_text": "CRP 15 mg/L", "translated_text_english": "CRP 15 mg/L"}),
            ),
            patch(
                "illdashboard.services.pipeline.copilot_extraction.generate_summary",
                new=AsyncMock(return_value="Inflammation marker is elevated."),
            ),
            patch(
                "illdashboard.services.pipeline.copilot_normalization.normalize_marker_names",
                new=AsyncMock(return_value={"CRP": "CRP"}),
            ),
            patch(
                "illdashboard.services.pipeline.copilot_normalization.normalize_source_name",
                new=AsyncMock(return_value="Synlab"),
            ),
            patch(
                "illdashboard.services.pipeline.copilot_normalization.classify_marker_groups",
                new=AsyncMock(return_value={"CRP": "Inflammation & Infection"}),
            ),
            patch(
                "illdashboard.services.pipeline.copilot_normalization.choose_canonical_units",
                new=AsyncMock(return_value={"CRP": "mg/L"}),
            ),
            patch(
                "illdashboard.services.pipeline.copilot_normalization.infer_rescaling_factors",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "illdashboard.services.pipeline.copilot_normalization.normalize_qualitative_values",
                new=AsyncMock(return_value={}),
            ),
        ):
            async with session_factory() as session:
                queued_file_ids = await pipeline.queue_files(session, [file_id])
            assert queued_file_ids == [file_id]

            ready_file = await _wait_for_file_ready(session_factory, file_id)
    finally:
        await runtime.stop()

    assert ready_file.status == READY_FILE_STATUS
    assert ready_file.measurement_status == "done"
    assert ready_file.normalization_status == "done"
    assert ready_file.text_status == "done"
    assert ready_file.summary_status == "done"
    assert ready_file.publish_status == "done"
    assert ready_file.ocr_summary_english == "Inflammation marker is elevated."
    assert ready_file.ocr_text_english == "CRP 15 mg/L"
    assert ready_file.lab_date is not None
    assert len(ready_file.measurements) == 1
    measurement = ready_file.measurements[0]
    assert measurement.marker_name == "CRP"
    assert measurement.canonical_value == 15.0
    assert measurement.canonical_unit == "mg/L"
    assert measurement.measurement_type is not None
    assert measurement.measurement_type.group_name == "Inflammation & Infection"

    async with session_factory() as session:
        file_result = await session.execute(select(LabFile).where(LabFile.id == file_id))
        refreshed_file = file_result.scalar_one()
        alias_result = await session.execute(select(MeasurementAlias).join(MeasurementAlias.measurement_type))
        aliases = alias_result.scalars().all()
        jobs_result = await session.execute(select(Job))
        remaining_jobs = jobs_result.scalars().all()

    assert refreshed_file.source_name == "synlab"
    assert any(alias.alias_name == "CRP" for alias in aliases)
    assert remaining_jobs == []


@pytest.mark.asyncio
async def test_rescaling_rules_are_type_specific(session_factory):
    async with session_factory() as session:
        glucose = MeasurementType(name="Glucose", normalized_key="glucose", group_name="Metabolic")
        cholesterol = MeasurementType(name="Cholesterol", normalized_key="cholesterol", group_name="Lipids")
        session.add_all([glucose, cholesterol])
        await session.flush()

        await rescaling.upsert_rescaling_rules(
            session,
            [
                {
                    "measurement_type": glucose,
                    "original_unit": "mg/dL",
                    "canonical_unit": "mmol/L",
                    "scale_factor": 0.0555,
                },
                {
                    "measurement_type": cholesterol,
                    "original_unit": "mg/dL",
                    "canonical_unit": "mmol/L",
                    "scale_factor": 0.0259,
                },
            ],
        )
        await session.commit()

        rules = await rescaling.load_rescaling_rules(
            session,
            [
                (glucose.id, "mg/dL", "mmol/L"),
                (cholesterol.id, "mg/dL", "mmol/L"),
            ],
        )

    assert rules[(glucose.id, "mg/dl", "mmol/l")].scale_factor == pytest.approx(0.0555)
    assert rules[(cholesterol.id, "mg/dl", "mmol/l")].scale_factor == pytest.approx(0.0259)


@pytest.mark.asyncio
async def test_preload_uploaded_files_seeds_missing_disk_files(session_factory):
    """Files in the upload folder that are not in the DB should be added on startup."""
    upload_dir = Path(settings.UPLOAD_DIR)

    # Already-tracked file
    existing_path = upload_dir / "already-tracked.png"
    existing_path.write_bytes(b"tracked-image")
    async with session_factory() as session:
        session.add(
            LabFile(
                filename="already-tracked.png",
                filepath=str(existing_path.resolve()),
                mime_type="image/png",
            )
        )
        await session.commit()

    # Untracked files: one supported, one unsupported
    (upload_dir / "new-scan.png").write_bytes(b"new-png")
    (upload_dir / "notes.txt").write_text("ignore me")

    async with session_factory() as session:
        added = await pipeline.preload_uploaded_files(session)

    assert added == 1

    async with session_factory() as session:
        result = await session.execute(select(LabFile).order_by(LabFile.filepath.asc()))
        files = result.scalars().all()

    paths = [Path(f.filepath).name for f in files]
    assert "already-tracked.png" in paths
    assert "new-scan.png" in paths
    assert "notes.txt" not in paths

    new_file = next(f for f in files if Path(f.filepath).name == "new-scan.png")
    assert new_file.mime_type == "image/png"
    assert new_file.status == "uploaded"
