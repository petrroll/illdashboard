from __future__ import annotations

import asyncio
import base64
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from illdashboard.config import settings
from illdashboard.models import (
    BiomarkerInsight,
    Job,
    LabFile,
    LabFileTag,
    MarkerTag,
    Measurement,
    MeasurementAlias,
    MeasurementBatch,
    MeasurementType,
    TextBatch,
    utc_now,
)
from illdashboard.services import admin as admin_service
from illdashboard.services import jobs as job_service
from illdashboard.services import pipeline, rescaling
from illdashboard.services import search as search_service
from illdashboard.services.insights import marker_signature
from illdashboard.services.markers import normalize_marker_alias_key
from illdashboard.services.upload_metadata import original_name_sidecar_path


async def _wait_for_file_complete(session_factory, file_id: int, *, timeout_seconds: float = 10.0) -> LabFile:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        async with session_factory() as session:
            result = await session.execute(
                select(LabFile)
                .options(
                    selectinload(LabFile.tags),
                    selectinload(LabFile.measurements).selectinload(Measurement.measurement_type),
                )
                .where(LabFile.id == file_id)
            )
            file = result.scalar_one()
            if file.status == "complete" and file.search_indexed_at is not None:
                return file
            if file.status == "error":
                raise AssertionError(file.processing_error or "file entered error state")
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("timed out waiting for file completion")
        await asyncio.sleep(0.05)


async def _wait_for_jobs_resolved(session_factory, *, timeout_seconds: float = 10.0) -> list[str]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        async with session_factory() as session:
            job_statuses = list((await session.execute(select(Job.status))).scalars())
            if job_statuses and all(status == job_service.JOB_STATUS_RESOLVED for status in job_statuses):
                return job_statuses
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("timed out waiting for jobs to resolve")
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_upload_and_queue_processing_creates_ensure_file_job(client, session_factory):
    upload_response = await client.post(
        "/api/files/upload",
        files={"file": ("report.png", b"fake-image", "image/png")},
    )
    assert upload_response.status_code == 200
    uploaded_file = upload_response.json()
    assert uploaded_file["status"] == "uploaded"
    assert uploaded_file["page_count"] == 1
    assert uploaded_file["progress"]["measurement_pages_total"] == 1
    assert uploaded_file["progress"]["text_pages_total"] == 1
    assert uploaded_file["progress"]["is_complete"] is False

    queue_response = await client.post(f"/api/files/{uploaded_file['id']}/ocr")
    assert queue_response.status_code == 200
    assert queue_response.json() == {"queued_file_ids": [uploaded_file["id"]]}

    async with session_factory() as session:
        jobs_result = await session.execute(
            select(Job).where(Job.file_id == uploaded_file["id"]).order_by(Job.task_type.asc(), Job.task_key.asc())
        )
        jobs = jobs_result.scalars().all()
        refreshed_file = await session.get(LabFile, uploaded_file["id"])

    assert refreshed_file is not None
    assert refreshed_file.status == "queued"
    stored_path = Path(refreshed_file.filepath)
    assert stored_path.name != "report.png"
    assert original_name_sidecar_path(stored_path).read_text(encoding="utf-8") == "report.png"
    assert [(job.task_type, job.task_key) for job in jobs] == [
        (pipeline.TASK_ENSURE_FILE, f"file:{uploaded_file['id']}"),
    ]


@pytest.mark.asyncio
async def test_upload_markdown_file_exposes_text_preview(client):
    markdown = "# Lab report\n\nCRP 15 mg/L\n"
    upload_response = await client.post(
        "/api/files/upload",
        files={"file": ("report.md", markdown.encode("utf-8"), "text/plain")},
    )
    assert upload_response.status_code == 200
    uploaded_file = upload_response.json()
    assert uploaded_file["mime_type"] == "text/markdown"
    assert uploaded_file["page_count"] == 1

    page_response = await client.get(f"/api/files/{uploaded_file['id']}/pages/1")
    assert page_response.status_code == 200
    assert page_response.headers["content-type"].startswith("text/markdown")
    assert page_response.text == markdown


@pytest.mark.asyncio
async def test_delete_file_removes_original_name_sidecar(client, session_factory):
    upload_response = await client.post(
        "/api/files/upload",
        files={"file": ("delete-me.png", b"fake-image", "image/png")},
    )
    assert upload_response.status_code == 200
    uploaded_file = upload_response.json()

    async with session_factory() as session:
        stored_file = await session.get(LabFile, uploaded_file["id"])

    assert stored_file is not None
    stored_path = Path(stored_file.filepath)
    sidecar_path = original_name_sidecar_path(stored_path)
    assert stored_path.exists()
    assert sidecar_path.exists()

    delete_response = await client.delete(f"/api/files/{uploaded_file['id']}")
    assert delete_response.status_code == 200
    assert delete_response.json() == {"ok": True}
    assert not stored_path.exists()
    assert not sidecar_path.exists()

    async with session_factory() as session:
        assert await session.get(LabFile, uploaded_file["id"]) is None


@pytest.mark.asyncio
async def test_share_export_html_embeds_assets_and_omits_summary_content(
    client,
    session_factory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    dist_dir = tmp_path / "frontend-dist"
    dist_dir.mkdir(parents=True, exist_ok=True)
    (dist_dir / "share-export-shell.html").write_text(
        (
            "<!doctype html><html><body>"
            '<script id="illdashboard-export-bundle" type="application/octet-stream">'
            "__ILLDASHBOARD_EXPORT_BASE64__"
            "</script>"
            "</body></html>"
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "FRONTEND_DIST_DIR", str(dist_dir))

    file_path = Path(settings.UPLOAD_DIR) / "share-report.png"
    file_path.write_bytes(
        base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+yF9kAAAAASUVORK5CYII=")
    )

    async with session_factory() as session:
        lab_file = LabFile(
            filename="share-report.png",
            filepath=str(file_path),
            mime_type="image/png",
            page_count=1,
            status="complete",
            ocr_text_raw="Ferritin 12 ug/L",
            ocr_text_english="Ferritin 12 ug/L",
            ocr_summary_english="Generated summary should stay out of the export.",
            search_indexed_at=utc_now(),
        )
        measurement_type = MeasurementType(
            name="Ferritin",
            normalized_key="ferritin",
            group_name="Inflammation & Infection",
            canonical_unit="ug/L",
        )
        session.add_all([lab_file, measurement_type])
        await session.flush()
        session.add(LabFileTag(lab_file_id=lab_file.id, tag="doctor"))
        session.add(
            Measurement(
                lab_file_id=lab_file.id,
                measurement_type_id=measurement_type.id,
                raw_marker_name="Ferritin",
                normalized_marker_key="ferritin",
                canonical_value=12.0,
                canonical_unit="ug/L",
                canonical_reference_low=20.0,
                canonical_reference_high=200.0,
                measured_at=utc_now(),
                normalization_status="resolved",
            )
        )
        await session.commit()

    with patch(
        "illdashboard.api.export.measurements_api.measurement_sparkline",
        new=AsyncMock(return_value=Response(content=b"sparkline-bytes", media_type="image/png")),
    ):
        response = await client.get("/api/export/share-html")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "attachment;" in response.headers["content-disposition"]

    match = re.search(
        r'<script id="illdashboard-export-bundle" type="application/octet-stream">([^<]+)</script>',
        response.text,
    )
    assert match is not None

    bundle = json.loads(base64.b64decode(match.group(1)).decode("utf-8"))
    assert bundle["kind"] == "share-export-v1"
    assert bundle["files"][0]["filename"] == "share-report.png"
    assert "ocr_summary_english" not in bundle["files"][0]
    assert bundle["files"][0]["filepath"] == ""
    assert "ocr_raw" not in bundle["files"][0]
    assert "original_file_data_url" not in bundle["file_assets"][str(bundle["files"][0]["id"])]
    assert bundle["file_assets"][str(bundle["files"][0]["id"])]["page_image_urls"][0].startswith("data:image/")
    assert "explanation" not in bundle["marker_details"]["Ferritin"]
    assert bundle["marker_details"]["Ferritin"]["explanation_cached"] is False
    assert bundle["marker_sparkline_urls"]["Ferritin"].startswith("data:image/png;base64,")
    assert bundle["search_documents"][0]["translated_text"] == "Ferritin 12 ug/L"
    assert "summary" not in bundle["search_documents"][0]


@pytest.mark.asyncio
async def test_share_export_html_embeds_text_preview_for_text_documents(
    client,
    session_factory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    dist_dir = tmp_path / "frontend-dist"
    dist_dir.mkdir(parents=True, exist_ok=True)
    (dist_dir / "share-export-shell.html").write_text(
        (
            "<!doctype html><html><body>"
            '<script id="illdashboard-export-bundle" type="application/octet-stream">'
            "__ILLDASHBOARD_EXPORT_BASE64__"
            "</script>"
            "</body></html>"
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "FRONTEND_DIST_DIR", str(dist_dir))

    markdown = "# Lab report\n\nCRP 15 mg/L\n"
    file_path = Path(settings.UPLOAD_DIR) / "share-report.md"
    file_path.write_text(markdown, encoding="utf-8")

    async with session_factory() as session:
        lab_file = LabFile(
            filename="share-report.md",
            filepath=str(file_path),
            mime_type="text/markdown",
            page_count=1,
            status="complete",
            ocr_text_raw=markdown,
            ocr_text_english="CRP 15 mg/L",
            search_indexed_at=utc_now(),
        )
        session.add(lab_file)
        await session.commit()

    response = await client.get("/api/export/share-html")
    assert response.status_code == 200

    match = re.search(
        r'<script id="illdashboard-export-bundle" type="application/octet-stream">([^<]+)</script>',
        response.text,
    )
    assert match is not None

    bundle = json.loads(base64.b64decode(match.group(1)).decode("utf-8"))
    assets = bundle["file_assets"][str(bundle["files"][0]["id"])]
    assert assets["page_image_urls"] == []
    assert assets["text_preview"] == markdown


@pytest.mark.asyncio
async def test_file_measurements_are_visible_progressively(client, session_factory):
    async with session_factory() as session:
        file = LabFile(
            filename="partial.png",
            filepath="/tmp/partial.png",
            mime_type="image/png",
            page_count=1,
            status="processing",
        )
        measurement_type = MeasurementType(
            name="CRP",
            normalized_key="crp",
            group_name="Inflammation & Infection",
            canonical_unit="mg/L",
        )
        session.add_all([file, measurement_type])
        await session.flush()
        session.add_all(
            [
                Measurement(
                    lab_file_id=file.id,
                    measurement_type_id=measurement_type.id,
                    raw_marker_name="CRP",
                    normalized_marker_key="crp",
                    original_value=12.0,
                    original_unit="mg/L",
                    canonical_value=12.0,
                    canonical_unit="mg/L",
                    normalization_status="resolved",
                ),
                Measurement(
                    lab_file_id=file.id,
                    raw_marker_name="ALT",
                    normalized_marker_key="alt",
                    normalization_status="pending",
                ),
            ]
        )
        await session.commit()
        file_id = file.id

    response = await client.get(f"/api/files/{file_id}/measurements")
    assert response.status_code == 200
    measurements = response.json()
    assert [measurement["marker_name"] for measurement in measurements] == ["CRP"]

    list_response = await client.get("/api/measurements")
    assert list_response.status_code == 200
    listed = list_response.json()
    assert [measurement["marker_name"] for measurement in listed] == ["CRP"]


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
                new=AsyncMock(
                    return_value={
                        "raw_text": "CRP 15 mg/L",
                        "translated_text_english": "CRP 15 mg/L",
                    }
                ),
            ),
            patch(
                "illdashboard.services.pipeline.copilot_extraction.generate_summary",
                new=AsyncMock(
                    return_value={
                        "summary_english": "Inflammation marker is elevated.",
                        "lab_date": "2026-03-15T00:00:00+00:00",
                        "source": "Synlab",
                    }
                ),
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

            complete_file = await _wait_for_file_complete(session_factory, file_id)
            await _wait_for_jobs_resolved(session_factory)
    finally:
        await runtime.stop()

    assert complete_file.status == "complete"
    assert complete_file.ocr_summary_english == "Inflammation marker is elevated."
    assert complete_file.ocr_text_english == "CRP 15 mg/L"
    assert complete_file.lab_date is not None
    assert complete_file.source_name == "synlab"
    assert any(tag.tag == "source:synlab" for tag in complete_file.tags)
    assert complete_file.ocr_raw is not None
    assert len(complete_file.measurements) == 1
    measurement = complete_file.measurements[0]
    assert measurement.marker_name == "CRP"
    assert measurement.canonical_value == 15.0
    assert measurement.canonical_unit == "mg/L"
    assert measurement.measurement_type is not None
    assert measurement.measurement_type.group_name == "Inflammation & Infection"

    async with session_factory() as session:
        alias_result = await session.execute(select(MeasurementAlias).join(MeasurementAlias.measurement_type))
        aliases = alias_result.scalars().all()
        jobs_result = await session.execute(select(Job.status))
        job_statuses = jobs_result.scalars().all()
        search_results = await search_service.search_lab_files("crp", [], session)
        refreshed_file = await session.get(LabFile, file_id)
        assert refreshed_file is not None
        progress = await pipeline.get_file_progress(session, refreshed_file)

    assert any(alias.alias_name == "CRP" for alias in aliases)
    assert all(status == job_service.JOB_STATUS_RESOLVED for status in job_statuses)
    assert [result["file_id"] for result in search_results] == [file_id]
    assert progress.search_ready is True


@pytest.mark.asyncio
async def test_pipeline_runtime_processes_text_file_end_to_end(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    file_path = upload_dir / "runtime-report.md"
    file_path.write_text("# Lab report\n\nCRP 15 mg/L\n", encoding="utf-8")

    async with session_factory() as session:
        lab_file = LabFile(
            filename="runtime-report.md",
            filepath=str(file_path),
            mime_type="text/markdown",
            page_count=1,
        )
        session.add(lab_file)
        await session.commit()
        await session.refresh(lab_file)
        file_id = lab_file.id

    async def fake_copilot_ask_json(_system_prompt, _user_prompt, *, request_name: str, **_kwargs):
        if request_name == "structured_medical_extraction":
            return {
                "lab_date": "2026-03-15T00:00:00+00:00",
                "source": "Synlab",
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
        if request_name == "document_text_extraction":
            return {"translated_text_english": "# Lab report\n\nCRP 15 mg/L"}
        if request_name == "medical_summary":
            return {
                "summary_english": "Inflammation marker is elevated.",
                "lab_date": "2026-03-15T00:00:00+00:00",
                "source": "Synlab",
            }
        raise AssertionError(f"Unexpected request_name: {request_name}")

    runtime = pipeline.PipelineRuntime(session_factory)
    await runtime.start()
    try:
        with (
            patch(
                "illdashboard.copilot.extraction.copilot_ask_json",
                new=AsyncMock(side_effect=fake_copilot_ask_json),
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

            complete_file = await _wait_for_file_complete(session_factory, file_id)
            await _wait_for_jobs_resolved(session_factory)
    finally:
        await runtime.stop()

    assert complete_file.status == "complete"
    assert complete_file.ocr_text_raw == "# Lab report\n\nCRP 15 mg/L"
    assert complete_file.ocr_text_english == "# Lab report\n\nCRP 15 mg/L"
    assert complete_file.ocr_summary_english == "Inflammation marker is elevated."
    assert complete_file.source_name == "synlab"
    assert len(complete_file.measurements) == 1
    assert complete_file.measurements[0].page_number == 1


@pytest.mark.asyncio
async def test_measurement_chronology_prefers_measurement_then_file_lab_date_then_upload_date(client, session_factory):
    upload_only_file_upload = datetime(2026, 3, 19, 14, 35, tzinfo=UTC)
    lab_date_file_upload = datetime(2026, 3, 22, 9, 10, tzinfo=UTC)
    lab_date_file_lab_date = datetime(2026, 3, 20, 8, 0, tzinfo=UTC)
    explicit_measurement_date = datetime(2026, 3, 21, 7, 45, tzinfo=UTC)
    explicit_file_upload = datetime(2026, 3, 18, 11, 20, tzinfo=UTC)

    async with session_factory() as session:
        upload_only_file = LabFile(
            filename="upload-only.txt",
            filepath="/tmp/upload-only.txt",
            mime_type="text/plain",
            page_count=1,
            uploaded_at=upload_only_file_upload,
            lab_date=None,
        )
        lab_date_file = LabFile(
            filename="lab-date.txt",
            filepath="/tmp/lab-date.txt",
            mime_type="text/plain",
            page_count=1,
            uploaded_at=lab_date_file_upload,
            lab_date=lab_date_file_lab_date,
        )
        explicit_file = LabFile(
            filename="explicit-date.txt",
            filepath="/tmp/explicit-date.txt",
            mime_type="text/plain",
            page_count=1,
            uploaded_at=explicit_file_upload,
            lab_date=None,
        )
        marker_type = MeasurementType(
            name="WBC",
            normalized_key="wbc",
            canonical_unit="10^9/L",
            group_name="Blood Count",
        )
        session.add_all([upload_only_file, lab_date_file, explicit_file, marker_type])
        await session.flush()
        session.add_all(
            [
                Measurement(
                    lab_file_id=upload_only_file.id,
                    measurement_type_id=marker_type.id,
                    raw_marker_name="WBC",
                    normalized_marker_key="wbc",
                    original_value=4.0,
                    original_unit="10^9/L",
                    canonical_value=4.0,
                    canonical_unit="10^9/L",
                    measured_at=None,
                    normalization_status="resolved",
                ),
                Measurement(
                    lab_file_id=lab_date_file.id,
                    measurement_type_id=marker_type.id,
                    raw_marker_name="WBC",
                    normalized_marker_key="wbc",
                    original_value=9.0,
                    original_unit="10^9/L",
                    canonical_value=9.0,
                    canonical_unit="10^9/L",
                    measured_at=None,
                    normalization_status="resolved",
                ),
                Measurement(
                    lab_file_id=explicit_file.id,
                    measurement_type_id=marker_type.id,
                    raw_marker_name="WBC",
                    normalized_marker_key="wbc",
                    original_value=12.0,
                    original_unit="10^9/L",
                    canonical_value=12.0,
                    canonical_unit="10^9/L",
                    measured_at=explicit_measurement_date,
                    normalization_status="resolved",
                ),
            ]
        )
        await session.commit()

    files_response = await client.get("/api/files")
    assert files_response.status_code == 200
    files = {
        item["filename"]: item
        for item in files_response.json()
        if item["filename"] in {"upload-only.txt", "lab-date.txt", "explicit-date.txt"}
    }
    assert files["upload-only.txt"]["lab_date"] is None
    assert files["lab-date.txt"]["lab_date"].startswith("2026-03-20T08:00:00")
    assert files["explicit-date.txt"]["lab_date"] is None

    list_response = await client.get("/api/measurements", params={"marker_name": "WBC"})
    assert list_response.status_code == 200
    listed = list_response.json()
    assert [measurement["canonical_value"] for measurement in listed] == [4.0, 9.0, 12.0]
    assert [measurement["measured_at"] for measurement in listed[:2]] == [None, None]
    assert listed[2]["measured_at"].startswith("2026-03-21T07:45:00")
    assert listed[0]["effective_measured_at"].startswith("2026-03-19T14:35:00")
    assert listed[1]["effective_measured_at"].startswith("2026-03-20T08:00:00")
    assert listed[2]["effective_measured_at"].startswith("2026-03-21T07:45:00")

    overview_response = await client.get("/api/measurements/overview")
    assert overview_response.status_code == 200
    overview_markers = [marker for group in overview_response.json() for marker in group["markers"]]
    wbc_overview = next(marker for marker in overview_markers if marker["marker_name"] == "WBC")
    assert wbc_overview["latest_measurement"]["canonical_value"] == 12.0
    assert wbc_overview["latest_measurement"]["measured_at"].startswith("2026-03-21T07:45:00")
    assert wbc_overview["latest_measurement"]["effective_measured_at"].startswith("2026-03-21T07:45:00")
    assert wbc_overview["previous_measurement"]["canonical_value"] == 9.0
    assert wbc_overview["previous_measurement"]["measured_at"] is None
    assert wbc_overview["previous_measurement"]["effective_measured_at"].startswith("2026-03-20T08:00:00")

    detail_response = await client.get("/api/measurements/detail", params={"marker_name": "WBC"})
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert [measurement["canonical_value"] for measurement in detail["measurements"]] == [4.0, 9.0, 12.0]
    assert detail["latest_measurement"]["canonical_value"] == 12.0
    assert detail["latest_measurement"]["measured_at"].startswith("2026-03-21T07:45:00")
    assert detail["latest_measurement"]["effective_measured_at"].startswith("2026-03-21T07:45:00")
    assert detail["previous_measurement"]["canonical_value"] == 9.0
    assert detail["previous_measurement"]["measured_at"] is None
    assert detail["previous_measurement"]["effective_measured_at"].startswith("2026-03-20T08:00:00")


@pytest.mark.asyncio
async def test_patch_file_updates_filename_and_lab_date_override(client, session_factory):
    pipeline_lab_date = datetime(2026, 3, 20, 8, 0, tzinfo=UTC)
    override_lab_date = datetime(2026, 3, 21, 9, 15, tzinfo=UTC)

    async with session_factory() as session:
        lab_file = LabFile(
            filename="original-name.pdf",
            filepath="/tmp/original-name.pdf",
            mime_type="application/pdf",
            page_count=1,
            lab_date=pipeline_lab_date,
        )
        measurement_type = MeasurementType(
            name="CRP",
            normalized_key="crp",
            canonical_unit="mg/L",
            group_name="Inflammation & Infection",
        )
        session.add_all([lab_file, measurement_type])
        await session.flush()
        session.add(
            Measurement(
                lab_file_id=lab_file.id,
                measurement_type_id=measurement_type.id,
                raw_marker_name="CRP",
                normalized_marker_key="crp",
                original_value=10.0,
                original_unit="mg/L",
                canonical_value=10.0,
                canonical_unit="mg/L",
                measured_at=None,
                normalization_status="resolved",
            )
        )
        await session.commit()
        file_id = lab_file.id

    response = await client.patch(
        f"/api/files/{file_id}",
        json={
            "filename": "edited-name.pdf",
            "lab_date": override_lab_date.isoformat(),
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["filename"] == "edited-name.pdf"
    assert body["lab_date"].startswith("2026-03-21T09:15:00")
    assert body["has_user_edits"] is True
    assert body["user_edited_fields"] == ["lab_date"]

    list_response = await client.get("/api/measurements", params={"marker_name": "CRP"})
    assert list_response.status_code == 200
    measurements = list_response.json()
    assert measurements[0]["lab_file_filename"] == "edited-name.pdf"
    assert measurements[0]["effective_measured_at"].startswith("2026-03-21T09:15:00")

    reset_response = await client.patch(
        f"/api/files/{file_id}",
        json={"reset_fields": ["lab_date"]},
    )
    assert reset_response.status_code == 200
    reset_body = reset_response.json()
    assert reset_body["lab_date"].startswith("2026-03-20T08:00:00")
    assert reset_body["has_user_edits"] is False
    assert reset_body["user_edited_fields"] == []


@pytest.mark.asyncio
async def test_patch_measurement_applies_and_resets_manual_overrides(client, session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="measurement-edit.pdf",
            filepath="/tmp/measurement-edit.pdf",
            mime_type="application/pdf",
            page_count=1,
        )
        measurement_type = MeasurementType(
            name="Ferritin",
            normalized_key="ferritin",
            canonical_unit="ug/L",
            group_name="Iron Status",
        )
        session.add_all([lab_file, measurement_type])
        await session.flush()
        measurement = Measurement(
            lab_file_id=lab_file.id,
            measurement_type_id=measurement_type.id,
            raw_marker_name="Ferritin",
            normalized_marker_key="ferritin",
            original_value=12.0,
            original_unit="ug/L",
            canonical_value=12.0,
            canonical_unit="ug/L",
            canonical_reference_low=20.0,
            canonical_reference_high=200.0,
            normalization_status="resolved",
        )
        session.add(measurement)
        await session.commit()
        measurement_id = measurement.id

    response = await client.patch(
        f"/api/measurements/{measurement_id}",
        json={
            "canonical_value": 15.5,
            "canonical_unit": "mg/L",
            "canonical_reference_low": 2.0,
            "canonical_reference_high": 7.0,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["canonical_value"] == 15.5
    assert body["canonical_unit"] == "mg/L"
    assert body["canonical_reference_low"] == 2.0
    assert body["canonical_reference_high"] == 7.0
    assert body["has_user_edits"] is True
    assert sorted(body["user_edited_fields"]) == [
        "canonical_reference_high",
        "canonical_reference_low",
        "canonical_unit",
        "canonical_value",
    ]

    detail_response = await client.get("/api/measurements/detail", params={"marker_name": "Ferritin"})
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["latest_measurement"]["canonical_value"] == 15.5
    assert detail["reference_low"] == 2.0
    assert detail["reference_high"] == 7.0

    reset_response = await client.patch(
        f"/api/measurements/{measurement_id}",
        json={
            "reset_fields": [
                "canonical_value",
                "canonical_unit",
                "canonical_reference_low",
                "canonical_reference_high",
            ]
        },
    )
    assert reset_response.status_code == 200
    reset_body = reset_response.json()
    assert reset_body["canonical_value"] == 12.0
    assert reset_body["canonical_unit"] == "ug/L"
    assert reset_body["canonical_reference_low"] == 20.0
    assert reset_body["canonical_reference_high"] == 200.0
    assert reset_body["has_user_edits"] is False
    assert reset_body["user_edited_fields"] == []


@pytest.mark.asyncio
async def test_patch_measurement_updates_and_resets_measurement_date_override(client, session_factory):
    pipeline_measured_at = datetime(2026, 3, 18, 8, 30, tzinfo=UTC)
    override_measured_at = datetime(2026, 3, 22, 12, 0, tzinfo=UTC)

    async with session_factory() as session:
        lab_file = LabFile(
            filename="measurement-date-edit.pdf",
            filepath="/tmp/measurement-date-edit.pdf",
            mime_type="application/pdf",
            page_count=1,
            lab_date=datetime(2026, 3, 20, 9, 0, tzinfo=UTC),
        )
        measurement_type = MeasurementType(
            name="Ferritin",
            normalized_key="ferritin",
            canonical_unit="ug/L",
            group_name="Iron Status",
        )
        session.add_all([lab_file, measurement_type])
        await session.flush()
        measurement = Measurement(
            lab_file_id=lab_file.id,
            measurement_type_id=measurement_type.id,
            raw_marker_name="Ferritin",
            normalized_marker_key="ferritin",
            original_value=12.0,
            original_unit="ug/L",
            canonical_value=12.0,
            canonical_unit="ug/L",
            normalization_status="resolved",
            measured_at=pipeline_measured_at,
        )
        session.add(measurement)
        await session.commit()
        file_id = lab_file.id
        measurement_id = measurement.id

    response = await client.patch(
        f"/api/measurements/{measurement_id}",
        json={"measured_at": override_measured_at.isoformat()},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["measured_at"].startswith("2026-03-18T08:30:00")
    assert body["effective_measured_at"].startswith("2026-03-22T12:00:00")
    assert body["has_user_edits"] is True
    assert "measured_at" in body["user_edited_fields"]

    detail_response = await client.get("/api/measurements/detail", params={"marker_name": "Ferritin"})
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["latest_measurement"]["effective_measured_at"].startswith("2026-03-22T12:00:00")

    file_response = await client.get(f"/api/files/{file_id}")
    assert file_response.status_code == 200
    assert file_response.json()["has_measurement_edits"] is True

    reset_response = await client.patch(
        f"/api/measurements/{measurement_id}",
        json={"reset_fields": ["measured_at"]},
    )
    assert reset_response.status_code == 200
    reset_body = reset_response.json()
    assert reset_body["effective_measured_at"].startswith("2026-03-18T08:30:00")
    assert "measured_at" not in reset_body["user_edited_fields"]

    reset_file_response = await client.get(f"/api/files/{file_id}")
    assert reset_file_response.status_code == 200
    assert reset_file_response.json()["has_measurement_edits"] is False


@pytest.mark.asyncio
async def test_patch_measurement_parses_qualitative_expression(client, session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="qualitative-edit.pdf",
            filepath="/tmp/qualitative-edit.pdf",
            mime_type="application/pdf",
            page_count=1,
        )
        measurement_type = MeasurementType(
            name="Hepatitis B Surface Antigen",
            normalized_key="hepatitis-b-surface-antigen",
            canonical_unit=None,
            group_name="Immunity & Serology",
        )
        session.add_all([lab_file, measurement_type])
        await session.flush()
        measurement = Measurement(
            lab_file_id=lab_file.id,
            measurement_type_id=measurement_type.id,
            raw_marker_name="HBsAg",
            normalized_marker_key="hbsag",
            original_qualitative_value="Positive",
            qualitative_value="Positive",
            qualitative_bool=True,
            normalization_status="resolved",
        )
        session.add(measurement)
        await session.commit()
        measurement_id = measurement.id

    response = await client.patch(
        f"/api/measurements/{measurement_id}",
        json={"qualitative_expression": 'false("Not detected")'},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["qualitative_value"] == "Not detected"
    assert body["qualitative_bool"] is False
    assert body["has_user_edits"] is True

    reset_response = await client.patch(
        f"/api/measurements/{measurement_id}",
        json={"reset_fields": ["qualitative"]},
    )
    assert reset_response.status_code == 200
    reset_body = reset_response.json()
    assert reset_body["qualitative_value"] == "Positive"
    assert reset_body["qualitative_bool"] is True
    assert reset_body["has_user_edits"] is False


@pytest.mark.asyncio
async def test_patch_marker_renames_type_and_preserves_old_alias(client, session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="marker-rename.pdf",
            filepath="/tmp/marker-rename.pdf",
            mime_type="application/pdf",
            page_count=1,
        )
        measurement_type = MeasurementType(
            name="CRP",
            normalized_key="crp",
            canonical_unit="mg/L",
            group_name="Inflammation & Infection",
        )
        session.add_all([lab_file, measurement_type])
        await session.flush()
        session.add(
            Measurement(
                lab_file_id=lab_file.id,
                measurement_type_id=measurement_type.id,
                raw_marker_name="CRP",
                normalized_marker_key="crp",
                original_value=18.0,
                original_unit="mg/L",
                canonical_value=18.0,
                canonical_unit="mg/L",
                normalization_status="resolved",
            )
        )
        await session.commit()

    response = await client.patch("/api/markers/CRP", json={"name": "C-Reactive Protein"})
    assert response.status_code == 200
    body = response.json()
    assert body["marker_name"] == "C-Reactive Protein"
    assert "CRP" in body["aliases"]

    renamed_detail = await client.get("/api/measurements/detail", params={"marker_name": "C-Reactive Protein"})
    assert renamed_detail.status_code == 200
    assert renamed_detail.json()["marker_name"] == "C-Reactive Protein"

    alias_detail = await client.get("/api/measurements/detail", params={"marker_name": "CRP"})
    assert alias_detail.status_code == 200
    alias_body = alias_detail.json()
    assert alias_body["marker_name"] == "C-Reactive Protein"
    assert "CRP" in alias_body["aliases"]

    markers_response = await client.get("/api/measurements/markers")
    assert markers_response.status_code == 200
    assert markers_response.json() == ["C-Reactive Protein"]


@pytest.mark.asyncio
async def test_patch_marker_updates_canonical_unit_and_recanonicalizes_measurements(client, session_factory):
    older_measured_at = datetime(2026, 3, 20, 8, 0, tzinfo=UTC)
    latest_measured_at = datetime(2026, 4, 20, 8, 0, tzinfo=UTC)

    async with session_factory() as session:
        first_file = LabFile(
            filename="marker-unit-first.pdf",
            filepath="backend/data/marker-unit-first.pdf",
            mime_type="application/pdf",
            page_count=1,
        )
        second_file = LabFile(
            filename="marker-unit-second.pdf",
            filepath="backend/data/marker-unit-second.pdf",
            mime_type="application/pdf",
            page_count=1,
        )
        measurement_type = MeasurementType(
            name="Ferritin",
            normalized_key="ferritin",
            canonical_unit="ug/L",
            group_name="Iron Status",
        )
        session.add_all([first_file, second_file, measurement_type])
        await session.flush()
        await rescaling.upsert_rescaling_rule(
            session,
            original_unit="ug/L",
            canonical_unit="mg/L",
            scale_factor=0.001,
            measurement_type=measurement_type,
        )
        older_measurement = Measurement(
            lab_file_id=first_file.id,
            measurement_type_id=measurement_type.id,
            raw_marker_name="Ferritin",
            normalized_marker_key="ferritin",
            original_value=80.0,
            original_unit="ug/L",
            canonical_value=80.0,
            canonical_unit="ug/L",
            canonical_reference_low=30.0,
            canonical_reference_high=400.0,
            user_canonical_value_override=True,
            user_canonical_value=999.0,
            user_canonical_unit_override=True,
            user_canonical_unit="custom-unit",
            normalization_status="resolved",
            measured_at=older_measured_at,
        )
        latest_measurement = Measurement(
            lab_file_id=second_file.id,
            measurement_type_id=measurement_type.id,
            raw_marker_name="Ferritin",
            normalized_marker_key="ferritin",
            original_value=120.0,
            original_unit="ug/L",
            canonical_value=120.0,
            canonical_unit="ug/L",
            canonical_reference_low=30.0,
            canonical_reference_high=400.0,
            normalization_status="resolved",
            measured_at=latest_measured_at,
        )
        session.add_all([older_measurement, latest_measurement])
        await session.commit()
        older_measurement_id = older_measurement.id
        latest_measurement_id = latest_measurement.id

    response = await client.patch("/api/markers/Ferritin", json={"canonical_unit": "mg/L"})
    assert response.status_code == 200
    body = response.json()
    assert body["canonical_unit"] == "mg/L"
    assert body["latest_measurement"]["id"] == latest_measurement_id
    assert body["latest_measurement"]["canonical_unit"] == "mg/L"
    assert body["latest_measurement"]["canonical_value"] == pytest.approx(0.12)
    assert body["reference_low"] == pytest.approx(0.03)
    assert body["reference_high"] == pytest.approx(0.4)
    previous_measurement = next(
        measurement for measurement in body["measurements"] if measurement["id"] == older_measurement_id
    )
    assert previous_measurement["canonical_unit"] == "custom-unit"
    assert previous_measurement["canonical_value"] == 999.0

    list_response = await client.get("/api/measurements", params={"marker_name": "Ferritin"})
    assert list_response.status_code == 200
    measurements = {measurement["id"]: measurement for measurement in list_response.json()}
    assert measurements[latest_measurement_id]["canonical_unit"] == "mg/L"
    assert measurements[latest_measurement_id]["canonical_value"] == pytest.approx(0.12)
    assert measurements[latest_measurement_id]["canonical_reference_low"] == pytest.approx(0.03)
    assert measurements[latest_measurement_id]["canonical_reference_high"] == pytest.approx(0.4)
    assert measurements[older_measurement_id]["canonical_unit"] == "custom-unit"

    reset_response = await client.patch(
        f"/api/measurements/{older_measurement_id}",
        json={"reset_fields": ["canonical_value", "canonical_unit"]},
    )
    assert reset_response.status_code == 200
    reset_body = reset_response.json()
    assert reset_body["canonical_unit"] == "mg/L"
    assert reset_body["canonical_value"] == pytest.approx(0.08)
    assert reset_body["has_user_edits"] is False

    async with session_factory() as session:
        refreshed_type = await session.get(MeasurementType, measurement_type.id)
        assert refreshed_type is not None
        assert refreshed_type.canonical_unit == "mg/L"


@pytest.mark.asyncio
async def test_patch_marker_invalidates_cached_insight_and_defers_regeneration(client, session_factory):
    measured_at = datetime(2026, 4, 20, 8, 0, tzinfo=UTC)

    async with session_factory() as session:
        lab_file = LabFile(
            filename="marker-insight.pdf",
            filepath="backend/data/marker-insight.pdf",
            mime_type="application/pdf",
            page_count=1,
        )
        measurement_type = MeasurementType(
            name="Ferritin",
            normalized_key="ferritin",
            canonical_unit="ug/L",
            group_name="Iron Status",
        )
        session.add_all([lab_file, measurement_type])
        await session.flush()
        await rescaling.upsert_rescaling_rule(
            session,
            original_unit="ug/L",
            canonical_unit="mg/L",
            scale_factor=0.001,
            measurement_type=measurement_type,
        )
        measurement = Measurement(
            lab_file_id=lab_file.id,
            measurement_type_id=measurement_type.id,
            raw_marker_name="Ferritin",
            normalized_marker_key="ferritin",
            original_value=120.0,
            original_unit="ug/L",
            canonical_value=120.0,
            canonical_unit="ug/L",
            canonical_reference_low=30.0,
            canonical_reference_high=400.0,
            normalization_status="resolved",
            measured_at=measured_at,
        )
        session.add(measurement)
        await session.flush()
        session.add(
            BiomarkerInsight(
                measurement_type_id=measurement_type.id,
                measurement_signature=marker_signature([measurement]),
                summary_markdown="stale cached insight",
            )
        )
        await session.commit()

    response = await client.patch("/api/markers/Ferritin", json={"canonical_unit": "mg/L"})
    assert response.status_code == 200
    body = response.json()
    assert body["canonical_unit"] == "mg/L"
    assert body["explanation"] is None
    assert body["explanation_cached"] is False

    async with session_factory() as session:
        result = await session.execute(select(BiomarkerInsight))
        assert result.scalar_one_or_none() is None

    with patch("illdashboard.services.insights.explain_marker_history", AsyncMock(return_value="fresh insight")):
        insight_response = await client.get("/api/measurements/insight", params={"marker_name": "Ferritin"})

    assert insight_response.status_code == 200
    insight_body = insight_response.json()
    assert insight_body["explanation"] == "fresh insight"
    assert insight_body["explanation_cached"] is False

    detail_response = await client.get("/api/measurements/detail", params={"marker_name": "Ferritin"})
    assert detail_response.status_code == 200
    detail_body = detail_response.json()
    assert detail_body["explanation"] == "fresh insight"
    assert detail_body["explanation_cached"] is True


@pytest.mark.asyncio
async def test_patch_measurement_original_unit_override_updates_conversion_warning(client, session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="glucose-override.pdf",
            filepath="/tmp/glucose-override.pdf",
            mime_type="application/pdf",
            page_count=1,
        )
        measurement_type = MeasurementType(
            name="Glucose",
            normalized_key="glucose",
            canonical_unit="mmol/L",
            group_name="Metabolic",
        )
        session.add_all([lab_file, measurement_type])
        await session.flush()
        measurement = Measurement(
            lab_file_id=lab_file.id,
            measurement_type_id=measurement_type.id,
            raw_marker_name="Glucose",
            normalized_marker_key="glucose",
            original_value=90.0,
            original_unit="mg/dL",
            canonical_value=None,
            canonical_unit="mmol/L",
            normalization_status="resolved",
        )
        session.add(measurement)
        await session.commit()
        measurement_id = measurement.id

    list_response = await client.get("/api/measurements", params={"marker_name": "Glucose"})
    assert list_response.status_code == 200
    assert list_response.json()[0]["unit_conversion_missing"] is True

    patch_response = await client.patch(
        f"/api/measurements/{measurement_id}",
        json={"original_unit": "mmol/L"},
    )
    assert patch_response.status_code == 200
    assert patch_response.json()["unit_conversion_missing"] is False


@pytest.mark.asyncio
async def test_queue_file_reset_clears_summary_derived_lab_date(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    file_path = upload_dir / "stale-date.png"
    file_path.write_bytes(b"stale-date")

    async with session_factory() as session:
        lab_file = LabFile(
            filename="stale-date.png",
            filepath=str(file_path),
            mime_type="image/png",
            page_count=1,
            status="complete",
            lab_date=utc_now(),
            ocr_summary_english="old summary",
            text_assembled_at=utc_now(),
            summary_generated_at=utc_now(),
            source_resolved_at=utc_now(),
            search_indexed_at=utc_now(),
        )
        session.add(lab_file)
        await session.commit()
        await session.refresh(lab_file)
        file_id = lab_file.id

        await pipeline.queue_file(session, file_id)
        await session.commit()

        refreshed_file = await session.get(LabFile, file_id)

    assert refreshed_file is not None
    assert refreshed_file.lab_date is None


@pytest.mark.asyncio
async def test_ensure_measurement_extraction_does_not_revive_failed_batch_job(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    file_path = upload_dir / "failed-batch.png"
    file_path.write_bytes(b"failed-batch")

    async with session_factory() as session:
        lab_file = LabFile(
            filename="failed-batch.png",
            filepath=str(file_path),
            mime_type="image/png",
            page_count=1,
            status="error",
            processing_error="batch failed",
        )
        session.add(lab_file)
        await session.flush()
        failed_task_key = pipeline._batch_task_key("measurements", lab_file.id, 0, 1, pipeline.DEFAULT_OCR_DPI)
        session.add_all(
            [
                Job(
                    file_id=lab_file.id,
                    task_type=pipeline.TASK_EXTRACT_MEASUREMENTS,
                    task_key=failed_task_key,
                    status=job_service.JOB_STATUS_FAILED,
                    error_text="boom",
                ),
                Job(
                    file_id=lab_file.id,
                    task_type=pipeline.TASK_ENSURE_MEASUREMENT_EXTRACTION,
                    task_key=f"file:{lab_file.id}",
                    status=job_service.JOB_STATUS_LEASED,
                    lease_owner="test-runtime",
                    lease_until=utc_now(),
                    payload_json=job_service.json_dumps({"file_id": lab_file.id}),
                ),
            ]
        )
        await session.commit()

        ensure_job = (
            await session.execute(
                select(Job).where(Job.task_type == pipeline.TASK_ENSURE_MEASUREMENT_EXTRACTION).limit(1)
            )
        ).scalar_one()
        await pipeline._ensure_measurement_extraction(session, ensure_job)
        await session.commit()

        failed_job = (
            await session.execute(
                select(Job).where(
                    Job.task_type == pipeline.TASK_EXTRACT_MEASUREMENTS,
                    Job.task_key == failed_task_key,
                )
            )
        ).scalar_one()
        refreshed_file = await session.get(LabFile, lab_file.id)

    assert failed_job.status == job_service.JOB_STATUS_FAILED
    assert refreshed_file is not None
    assert refreshed_file.status == "error"


@pytest.mark.asyncio
async def test_ensure_measurement_extraction_respects_split_child_jobs(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    file_path = upload_dir / "split-batch.png"
    file_path.write_bytes(b"split-batch")

    async with session_factory() as session:
        lab_file = LabFile(
            filename="split-batch.png",
            filepath=str(file_path),
            mime_type="image/png",
            page_count=2,
            status="processing",
        )
        session.add(lab_file)
        await session.flush()
        file_id = lab_file.id
        session.add_all(
            [
                Job(
                    file_id=file_id,
                    task_type=pipeline.TASK_EXTRACT_MEASUREMENTS,
                    task_key=pipeline._batch_task_key("measurements", file_id, 0, 1, pipeline.DEFAULT_OCR_DPI),
                    status=job_service.JOB_STATUS_PENDING,
                    payload_json=job_service.json_dumps(
                        {"file_id": file_id, "start_page": 0, "stop_page": 1, "dpi": pipeline.DEFAULT_OCR_DPI}
                    ),
                ),
                Job(
                    file_id=file_id,
                    task_type=pipeline.TASK_EXTRACT_MEASUREMENTS,
                    task_key=pipeline._batch_task_key("measurements", file_id, 1, 2, pipeline.DEFAULT_OCR_DPI),
                    status=job_service.JOB_STATUS_PENDING,
                    payload_json=job_service.json_dumps(
                        {"file_id": file_id, "start_page": 1, "stop_page": 2, "dpi": pipeline.DEFAULT_OCR_DPI}
                    ),
                ),
                Job(
                    file_id=file_id,
                    task_type=pipeline.TASK_ENSURE_MEASUREMENT_EXTRACTION,
                    task_key=f"file:{file_id}",
                    status=job_service.JOB_STATUS_LEASED,
                    lease_owner="test-runtime",
                    lease_until=utc_now(),
                    payload_json=job_service.json_dumps({"file_id": file_id}),
                ),
            ]
        )
        await session.commit()

        ensure_job = (
            await session.execute(
                select(Job).where(Job.task_type == pipeline.TASK_ENSURE_MEASUREMENT_EXTRACTION).limit(1)
            )
        ).scalar_one()
        await pipeline._ensure_measurement_extraction(session, ensure_job)
        await session.commit()

        extract_jobs = list(
            (
                await session.execute(
                    select(Job).where(Job.task_type == pipeline.TASK_EXTRACT_MEASUREMENTS).order_by(Job.task_key.asc())
                )
            ).scalars()
        )

    assert [job.task_key for job in extract_jobs] == [
        pipeline._batch_task_key("measurements", file_id, 0, 1, pipeline.DEFAULT_OCR_DPI),
        pipeline._batch_task_key("measurements", file_id, 1, 2, pipeline.DEFAULT_OCR_DPI),
    ]


@pytest.mark.asyncio
async def test_ensure_file_keeps_source_failure_terminal(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    file_path = upload_dir / "source-terminal.png"
    file_path.write_bytes(b"source-terminal")

    async with session_factory() as session:
        lab_file = LabFile(
            filename="source-terminal.png",
            filepath=str(file_path),
            mime_type="image/png",
            page_count=1,
            status="error",
            processing_error="source failed",
            source_candidate="Synlab",
            source_candidate_key="synlab",
            ocr_text_raw="text",
            ocr_text_english="text",
            ocr_summary_english="summary",
            text_assembled_at=utc_now(),
            summary_generated_at=utc_now(),
        )
        session.add(lab_file)
        await session.flush()
        session.add_all(
            [
                MeasurementBatch(
                    file_id=lab_file.id,
                    task_key=pipeline._batch_task_key("measurements", lab_file.id, 0, 1, pipeline.DEFAULT_OCR_DPI),
                    start_page=0,
                    stop_page=1,
                    dpi=pipeline.DEFAULT_OCR_DPI,
                ),
                TextBatch(
                    file_id=lab_file.id,
                    task_key=pipeline._batch_task_key("text", lab_file.id, 0, 1, pipeline.DEFAULT_OCR_DPI),
                    start_page=0,
                    stop_page=1,
                    dpi=pipeline.DEFAULT_OCR_DPI,
                    raw_text="text",
                    translated_text_english="text",
                ),
                Job(
                    task_type=pipeline.TASK_CANONIZE_SOURCE,
                    task_key="synlab",
                    status=job_service.JOB_STATUS_FAILED,
                    error_text="source boom",
                ),
                Job(
                    file_id=lab_file.id,
                    task_type=pipeline.TASK_ENSURE_FILE,
                    task_key=f"file:{lab_file.id}",
                    status=job_service.JOB_STATUS_LEASED,
                    lease_owner="test-runtime",
                    lease_until=utc_now(),
                    payload_json=job_service.json_dumps({"file_id": lab_file.id}),
                ),
            ]
        )
        await session.commit()

        ensure_job = (
            await session.execute(select(Job).where(Job.task_type == pipeline.TASK_ENSURE_FILE).limit(1))
        ).scalar_one()
        await pipeline._ensure_file(session, ensure_job)
        await session.commit()

        source_job = (
            await session.execute(
                select(Job).where(Job.task_type == pipeline.TASK_CANONIZE_SOURCE, Job.task_key == "synlab")
            )
        ).scalar_one()
        refreshed_file = await session.get(LabFile, lab_file.id)

    assert source_job.status == job_service.JOB_STATUS_FAILED
    assert refreshed_file is not None
    assert refreshed_file.status == "error"
    assert refreshed_file.processing_error == "source boom"


@pytest.mark.asyncio
async def test_prune_jobs_resets_leased_jobs_after_restart(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    file_path = upload_dir / "leased.png"
    file_path.write_bytes(b"leased-image")

    async with session_factory() as session:
        lab_file = LabFile(
            filename="leased.png",
            filepath=str(file_path),
            mime_type="image/png",
            page_count=1,
        )
        session.add(lab_file)
        await session.flush()
        leased_job = Job(
            file_id=lab_file.id,
            task_type=pipeline.TASK_ENSURE_FILE,
            task_key=f"file:{lab_file.id}",
            status=job_service.JOB_STATUS_LEASED,
            priority=10,
            lease_owner="old-runtime",
            lease_until=utc_now(),
        )
        session.add(leased_job)
        await session.commit()

    async with session_factory() as session:
        await job_service.prune_jobs(session)

    async with session_factory() as session:
        result = await session.execute(select(Job).where(Job.task_type == pipeline.TASK_ENSURE_FILE))
        refreshed_job = result.scalar_one()

    assert refreshed_job.status == job_service.JOB_STATUS_PENDING
    assert refreshed_job.lease_owner is None
    assert refreshed_job.lease_until is None


@pytest.mark.asyncio
async def test_runtime_start_only_resumes_previously_scheduled_files(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    uploaded_path = upload_dir / "startup-uploaded.png"
    scheduled_path = upload_dir / "startup-scheduled.png"
    queued_path = upload_dir / "startup-queued.png"
    processing_path = upload_dir / "startup-processing.png"
    error_path = upload_dir / "startup-error.png"
    uploaded_path.write_bytes(b"uploaded-image")
    scheduled_path.write_bytes(b"scheduled-image")
    queued_path.write_bytes(b"queued-image")
    processing_path.write_bytes(b"processing-image")
    error_path.write_bytes(b"error-image")

    async with session_factory() as session:
        uploaded_file = LabFile(
            filename="startup-uploaded.png",
            filepath=str(uploaded_path),
            mime_type="image/png",
            page_count=1,
        )
        scheduled_file = LabFile(
            filename="startup-scheduled.png",
            filepath=str(scheduled_path),
            mime_type="image/png",
            page_count=1,
        )
        queued_file = LabFile(
            filename="startup-queued.png",
            filepath=str(queued_path),
            mime_type="image/png",
            page_count=1,
            status="queued",
        )
        processing_file = LabFile(
            filename="startup-processing.png",
            filepath=str(processing_path),
            mime_type="image/png",
            page_count=1,
            status="processing",
        )
        error_file = LabFile(
            filename="startup-error.png",
            filepath=str(error_path),
            mime_type="image/png",
            page_count=1,
            status="error",
        )
        session.add_all([uploaded_file, scheduled_file, queued_file, processing_file, error_file])
        await session.flush()
        session.add(
            Job(
                file_id=scheduled_file.id,
                task_type=pipeline.TASK_ENSURE_FILE,
                task_key=f"file:{scheduled_file.id}",
                status=job_service.JOB_STATUS_PENDING,
                priority=10,
            )
        )
        await session.commit()

    with patch.object(pipeline.PipelineRuntime, "_spawn_workers", return_value=None):
        runtime = pipeline.PipelineRuntime(session_factory)
        await runtime.start()
        await runtime.stop()

    async with session_factory() as session:
        jobs_result = await session.execute(select(Job).order_by(Job.file_id.asc(), Job.task_type.asc()))
        jobs = jobs_result.scalars().all()

    assert [(job.file_id, job.task_type) for job in jobs] == [
        (scheduled_file.id, pipeline.TASK_ENSURE_FILE),
        (queued_file.id, pipeline.TASK_ENSURE_FILE),
        (processing_file.id, pipeline.TASK_ENSURE_FILE),
    ]


@pytest.mark.asyncio
async def test_queue_files_from_clean_runtime_cancels_inflight_work(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    file_path = upload_dir / "restart-clean.png"
    file_path.write_bytes(b"runtime-image")

    first_run_started = asyncio.Event()
    first_run_cancelled = asyncio.Event()
    shutdown_requested = asyncio.Event()
    extract_attempts = 0

    async def fake_extract_measurement_batch(*_args, **_kwargs):
        nonlocal extract_attempts
        extract_attempts += 1
        if extract_attempts == 1:
            first_run_started.set()
            await shutdown_requested.wait()
            first_run_cancelled.set()
            raise RuntimeError("aborted during clean-runtime reset")
        return {
            "measurements": [
                {
                    "marker_name": "CRP",
                    "value": 42.0,
                    "unit": "mg/L",
                    "reference_low": 0.0,
                    "reference_high": 5.0,
                    "measured_at": "2026-03-15T00:00:00+00:00",
                    "page_number": 1,
                }
            ],
        }

    async def fake_shutdown_client():
        shutdown_requested.set()

    with (
        patch(
            "illdashboard.services.pipeline.copilot_extraction.extract_measurement_batch",
            new=AsyncMock(side_effect=fake_extract_measurement_batch),
        ),
        patch(
            "illdashboard.services.pipeline.copilot_extraction.extract_text_batch",
            new=AsyncMock(
                return_value={
                    "raw_text": "CRP 42 mg/L",
                    "translated_text_english": "CRP 42 mg/L",
                }
            ),
        ),
        patch(
            "illdashboard.services.pipeline.copilot_extraction.generate_summary",
            new=AsyncMock(
                return_value={
                    "summary_english": "Fresh rerun summary.",
                    "lab_date": "2026-03-15T00:00:00+00:00",
                    "source": "Synlab",
                }
            ),
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
        patch(
            "illdashboard.services.pipeline.shutdown_client",
            new=AsyncMock(side_effect=fake_shutdown_client),
        ) as shutdown_client_mock,
    ):
        async with session_factory() as session:
            lab_file = LabFile(
                filename="restart-clean.png",
                filepath=str(file_path),
                mime_type="image/png",
                page_count=1,
            )
            session.add(lab_file)
            await session.commit()
            await session.refresh(lab_file)
            file_id = lab_file.id

        await pipeline.start_pipeline_runtime(session_factory)
        try:
            async with session_factory() as session:
                queued_file_ids = await pipeline.queue_files(session, [file_id])
            assert queued_file_ids == [file_id]

            await asyncio.wait_for(first_run_started.wait(), timeout=5.0)

            async with session_factory() as session:
                rerun_file_ids = await asyncio.wait_for(
                    pipeline.queue_files_from_clean_runtime(session, [file_id]),
                    timeout=5.0,
                )
            assert rerun_file_ids == [file_id]
            assert first_run_cancelled.is_set()
            shutdown_client_mock.assert_awaited_once()

            complete_file = await _wait_for_file_complete(session_factory, file_id)
        finally:
            await pipeline.stop_pipeline_runtime()

    assert extract_attempts >= 2
    assert complete_file.ocr_summary_english == "Fresh rerun summary."
    assert len(complete_file.measurements) == 1
    assert complete_file.measurements[0].canonical_value == 42.0


@pytest.mark.asyncio
async def test_preload_uploaded_files_seeds_missing_disk_files(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)

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

    hashed_path = upload_dir / "2f4ce2f590f64dfabf99c3955b417d55.png"
    hashed_path.write_bytes(b"new-png")
    original_name_sidecar_path(hashed_path).write_text("new-scan.png", encoding="utf-8")
    fallback_path = upload_dir / "missing-sidecar.png"
    fallback_path.write_bytes(b"fallback-png")
    notes_path = upload_dir / "notes.txt"
    notes_path.write_text("ignore me", encoding="utf-8")

    async with session_factory() as session:
        added = await pipeline.preload_uploaded_files(session)

    assert added == 3

    async with session_factory() as session:
        result = await session.execute(select(LabFile).order_by(LabFile.filepath.asc()))
        files = result.scalars().all()

    paths = [Path(file.filepath).name for file in files]
    assert "already-tracked.png" in paths
    assert hashed_path.name in paths
    assert fallback_path.name in paths
    assert notes_path.name in paths
    assert original_name_sidecar_path(hashed_path).name not in paths

    new_file = next(file for file in files if Path(file.filepath).name == hashed_path.name)
    assert new_file.mime_type == "image/png"
    assert new_file.filename == "new-scan.png"
    assert new_file.status == "uploaded"
    fallback_file = next(file for file in files if Path(file.filepath).name == fallback_path.name)
    assert fallback_file.filename == fallback_path.name
    assert fallback_file.status == "uploaded"
    notes_file = next(file for file in files if Path(file.filepath).name == notes_path.name)
    assert notes_file.mime_type == "text/plain"
    assert notes_file.filename == notes_path.name
    assert notes_file.status == "uploaded"


@pytest.mark.asyncio
async def test_reset_database_reloads_upload_dir_as_uploaded_files(client, session_factory):
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

    with (
        patch.object(admin_service, "purge_sparkline_cache", return_value=0),
    ):
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


@pytest.mark.asyncio
async def test_queue_files_only_requeues_selected_files(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    complete_path = upload_dir / "complete.png"
    stuck_path = upload_dir / "stuck.png"
    error_path = upload_dir / "error.png"
    complete_path.write_bytes(b"complete-image")
    stuck_path.write_bytes(b"stuck-image")
    error_path.write_bytes(b"error-image")

    async with session_factory() as session:
        complete_file = LabFile(
            filename="complete.png",
            filepath=str(complete_path),
            mime_type="image/png",
            page_count=1,
            status="complete",
            ocr_raw="published",
            ocr_text_raw="text",
            ocr_summary_english="summary",
            source_name="synlab",
            text_assembled_at=utc_now(),
            summary_generated_at=utc_now(),
            source_resolved_at=utc_now(),
            search_indexed_at=utc_now(),
            tags=[LabFileTag(tag="source:synlab")],
        )
        stuck_file = LabFile(
            filename="stuck.png",
            filepath=str(stuck_path),
            mime_type="image/png",
            page_count=1,
            status="processing",
            processing_error="timed out",
            source_name="legacy-lab",
            ocr_text_raw="partial text",
            tags=[LabFileTag(tag="source:legacy-lab")],
        )
        error_file = LabFile(
            filename="error.png",
            filepath=str(error_path),
            mime_type="image/png",
            page_count=1,
            status="error",
            processing_error="bad OCR",
            source_name="broken-lab",
        )
        session.add_all([complete_file, stuck_file, error_file])
        await session.flush()

        session.add(
            Measurement(
                lab_file_id=stuck_file.id,
                raw_marker_name="CRP",
                normalized_marker_key="crp",
            )
        )
        session.add_all(
            [
                Job(
                    file_id=stuck_file.id,
                    task_type=pipeline.TASK_EXTRACT_MEASUREMENTS,
                    task_key=f"file:{stuck_file.id}:measurements:0:1:144",
                    status=job_service.JOB_STATUS_PENDING,
                    priority=10,
                ),
                Job(
                    file_id=None,
                    task_type=pipeline.TASK_CANONIZE_MARKER,
                    task_key="crp",
                    status=job_service.JOB_STATUS_PENDING,
                    priority=20,
                ),
            ]
        )
        await session.commit()
        selected_id = complete_file.id

    async with session_factory() as session:
        queued_file_ids = await pipeline.queue_files(session, [selected_id])

    assert queued_file_ids == [selected_id]

    async with session_factory() as session:
        files_result = await session.execute(
            select(LabFile).options(selectinload(LabFile.tags)).order_by(LabFile.id.asc())
        )
        complete_file, stuck_file, error_file = files_result.scalars().all()
        jobs_result = await session.execute(select(Job).order_by(Job.task_type.asc(), Job.task_key.asc()))
        jobs = jobs_result.scalars().all()
        measurements_result = await session.execute(select(Measurement).order_by(Measurement.id.asc()))
        measurements = measurements_result.scalars().all()

    assert complete_file.status == "queued"
    assert complete_file.source_name is None
    assert complete_file.ocr_text_raw is None
    assert complete_file.tags == []

    assert stuck_file.status == "processing"
    assert stuck_file.processing_error == "timed out"
    assert stuck_file.source_name == "legacy-lab"
    assert stuck_file.ocr_text_raw == "partial text"
    assert [tag.tag for tag in stuck_file.tags] == ["source:legacy-lab"]

    assert error_file.status == "error"
    assert error_file.processing_error == "bad OCR"
    assert error_file.source_name == "broken-lab"

    assert [measurement.lab_file_id for measurement in measurements] == [stuck_file.id]
    assert {(job.file_id, job.task_type) for job in jobs} == {
        (selected_id, pipeline.TASK_ENSURE_FILE),
        (stuck_file.id, pipeline.TASK_EXTRACT_MEASUREMENTS),
        (None, pipeline.TASK_CANONIZE_MARKER),
    }


@pytest.mark.asyncio
async def test_queue_unprocessed_files_requeues_all_non_complete_files(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    uploaded_path = upload_dir / "uploaded.png"
    retry_path = upload_dir / "retry.png"
    error_path = upload_dir / "error.png"
    complete_path = upload_dir / "complete.png"
    uploaded_path.write_bytes(b"uploaded-image")
    retry_path.write_bytes(b"retry-image")
    error_path.write_bytes(b"error-image")
    complete_path.write_bytes(b"complete-image")

    async with session_factory() as session:
        uploaded_file = LabFile(
            filename="uploaded.png",
            filepath=str(uploaded_path),
            mime_type="image/png",
            page_count=1,
        )
        retry_file = LabFile(
            filename="retry.png",
            filepath=str(retry_path),
            mime_type="image/png",
            page_count=1,
            status="processing",
        )
        error_file = LabFile(
            filename="error.png",
            filepath=str(error_path),
            mime_type="image/png",
            page_count=1,
            status="error",
        )
        complete_file = LabFile(
            filename="complete.png",
            filepath=str(complete_path),
            mime_type="image/png",
            page_count=1,
            status="complete",
            text_assembled_at=utc_now(),
            summary_generated_at=utc_now(),
            source_resolved_at=utc_now(),
            search_indexed_at=utc_now(),
        )
        session.add_all([uploaded_file, retry_file, error_file, complete_file])
        await session.flush()
        session.add(
            Job(
                file_id=retry_file.id,
                task_type=pipeline.TASK_ENSURE_FILE,
                task_key=f"file:{retry_file.id}",
                status=job_service.JOB_STATUS_PENDING,
                priority=10,
            )
        )
        await session.commit()

    async with session_factory() as session:
        queued_file_ids = await pipeline.queue_unprocessed_files(session)

    assert set(queued_file_ids) == {1, 2, 3}

    async with session_factory() as session:
        files_result = await session.execute(select(LabFile).order_by(LabFile.id.asc()))
        files = files_result.scalars().all()
        jobs_result = await session.execute(select(Job).order_by(Job.file_id.asc(), Job.task_type.asc()))
        jobs = jobs_result.scalars().all()

    assert [file.status for file in files] == ["queued", "queued", "queued", "complete"]
    assert [(job.file_id, job.task_type) for job in jobs] == [
        (1, pipeline.TASK_ENSURE_FILE),
        (2, pipeline.TASK_ENSURE_FILE),
        (3, pipeline.TASK_ENSURE_FILE),
    ]


@pytest.mark.asyncio
async def test_cancel_ocr_clears_jobs_and_resets_active_files(client, session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    complete_path = upload_dir / "cancel-complete.png"
    queued_path = upload_dir / "cancel-queued.png"
    processing_path = upload_dir / "cancel-processing.png"
    complete_path.write_bytes(b"complete-image")
    queued_path.write_bytes(b"queued-image")
    processing_path.write_bytes(b"processing-image")

    async with session_factory() as session:
        complete_file = LabFile(
            filename="cancel-complete.png",
            filepath=str(complete_path),
            mime_type="image/png",
            page_count=1,
            status="complete",
            ocr_raw="published",
            text_assembled_at=utc_now(),
            summary_generated_at=utc_now(),
            source_resolved_at=utc_now(),
            search_indexed_at=utc_now(),
        )
        queued_file = LabFile(
            filename="cancel-queued.png",
            filepath=str(queued_path),
            mime_type="image/png",
            page_count=1,
            status="queued",
            source_name="queued-lab",
            ocr_text_raw="queued text",
            ocr_summary_english="queued summary",
            tags=[LabFileTag(tag="source:queued-lab")],
        )
        processing_file = LabFile(
            filename="cancel-processing.png",
            filepath=str(processing_path),
            mime_type="image/png",
            page_count=1,
            status="processing",
            processing_error="still running",
            source_name="processing-lab",
            ocr_text_raw="partial text",
            tags=[LabFileTag(tag="source:processing-lab")],
        )
        session.add_all([complete_file, queued_file, processing_file])
        await session.flush()

        session.add(
            Measurement(
                lab_file_id=processing_file.id,
                raw_marker_name="CRP",
                normalized_marker_key="crp",
            )
        )
        session.add_all(
            [
                Job(
                    file_id=queued_file.id,
                    task_type=pipeline.TASK_ENSURE_FILE,
                    task_key=f"file:{queued_file.id}",
                    status=job_service.JOB_STATUS_PENDING,
                    priority=10,
                ),
                Job(
                    file_id=processing_file.id,
                    task_type=pipeline.TASK_EXTRACT_TEXT,
                    task_key=f"file:{processing_file.id}:text:0:1:144",
                    status=job_service.JOB_STATUS_LEASED,
                    priority=20,
                    lease_owner="runtime:test",
                    lease_until=utc_now(),
                ),
                Job(
                    file_id=None,
                    task_type=pipeline.TASK_CANONIZE_MARKER,
                    task_key="crp",
                    status=job_service.JOB_STATUS_PENDING,
                    priority=30,
                ),
            ]
        )
        await session.commit()

    response = await client.post("/api/files/ocr/cancel")
    assert response.status_code == 200
    assert response.json() == {"ok": True}

    async with session_factory() as session:
        files_result = await session.execute(
            select(LabFile).options(selectinload(LabFile.tags)).order_by(LabFile.id.asc())
        )
        complete_file, queued_file, processing_file = files_result.scalars().all()
        jobs_result = await session.execute(select(Job).order_by(Job.id.asc()))
        jobs = jobs_result.scalars().all()
        measurements_result = await session.execute(select(Measurement).order_by(Measurement.id.asc()))
        measurements = measurements_result.scalars().all()

    assert complete_file.status == "complete"
    assert complete_file.ocr_raw == "published"

    assert queued_file.status == "uploaded"
    assert queued_file.processing_error is None
    assert queued_file.source_name is None
    assert queued_file.ocr_text_raw is None
    assert queued_file.ocr_summary_english is None
    assert queued_file.tags == []

    assert processing_file.status == "uploaded"
    assert processing_file.processing_error is None
    assert processing_file.source_name is None
    assert processing_file.ocr_text_raw is None
    assert processing_file.tags == []

    assert measurements == []
    assert jobs == []


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
async def test_marker_tag_endpoints_expose_and_filter_range_derived_tags(client, session_factory):
    async with session_factory() as session:
        file = LabFile(
            filename="markers.pdf",
            filepath="/tmp/markers.pdf",
            mime_type="application/pdf",
        )
        crp = MeasurementType(
            name="CRP",
            normalized_key="crp",
            group_name="Inflammation & Infection",
            canonical_unit="mg/L",
        )
        hemoglobin = MeasurementType(
            name="Hemoglobin",
            normalized_key="hemoglobin",
            group_name="Blood Function",
            canonical_unit="g/dL",
        )
        session.add_all([file, crp, hemoglobin])
        await session.flush()
        session.add(
            MeasurementAlias(
                alias_name="C-Reactive Protein",
                normalized_key=normalize_marker_alias_key("C-Reactive Protein"),
                measurement_type_id=crp.id,
            )
        )
        session.add_all(
            [
                Measurement(
                    lab_file_id=file.id,
                    measurement_type_id=crp.id,
                    raw_marker_name="CRP",
                    normalized_marker_key="crp",
                    canonical_value=15.0,
                    canonical_unit="mg/L",
                    canonical_reference_low=0.0,
                    canonical_reference_high=5.0,
                    measured_at=utc_now(),
                    normalization_status="resolved",
                ),
                Measurement(
                    lab_file_id=file.id,
                    measurement_type_id=hemoglobin.id,
                    raw_marker_name="Hemoglobin",
                    normalized_marker_key="hemoglobin",
                    canonical_value=14.0,
                    canonical_unit="g/dL",
                    canonical_reference_low=13.0,
                    canonical_reference_high=17.0,
                    measured_at=utc_now(),
                    normalization_status="resolved",
                ),
            ]
        )
        await session.commit()

    tags_response = await client.get("/api/tags/markers")
    assert tags_response.status_code == 200
    assert "range:onlyOutOfRange" in tags_response.json()
    assert "range:mostlyOutOfRange" in tags_response.json()
    assert "range:someOutOfRange" in tags_response.json()
    assert "range:onlyInRange" in tags_response.json()

    overview_response = await client.get(
        "/api/measurements/overview",
        params=[("tags", "range:onlyOutOfRange")],
    )
    assert overview_response.status_code == 200

    groups = overview_response.json()
    markers = [marker for group in groups for marker in group["markers"]]
    assert [marker["marker_name"] for marker in markers] == ["CRP"]
    assert markers[0]["aliases"] == ["C-Reactive Protein"]
    assert "range:onlyOutOfRange" in markers[0]["marker_tags"]
    assert "range:mostlyOutOfRange" in markers[0]["marker_tags"]
    assert "range:someOutOfRange" in markers[0]["marker_tags"]

    mostly_response = await client.get(
        "/api/measurements/overview",
        params=[("tags", "range:mostlyOutOfRange")],
    )
    assert mostly_response.status_code == 200
    mostly_markers = [marker for group in mostly_response.json() for marker in group["markers"]]
    assert [marker["marker_name"] for marker in mostly_markers] == ["CRP"]

    some_response = await client.get(
        "/api/measurements/overview",
        params=[("tags", "range:someOutOfRange")],
    )
    assert some_response.status_code == 200
    some_markers = [marker for group in some_response.json() for marker in group["markers"]]
    assert [marker["marker_name"] for marker in some_markers] == ["CRP"]


@pytest.mark.asyncio
async def test_set_marker_tags_strips_reserved_range_tags_but_returns_derived_tags(client, session_factory):
    async with session_factory() as session:
        file = LabFile(
            filename="marker-tags.pdf",
            filepath="/tmp/marker-tags.pdf",
            mime_type="application/pdf",
        )
        crp = MeasurementType(
            name="CRP",
            normalized_key="crp",
            group_name="Inflammation & Infection",
            canonical_unit="mg/L",
        )
        session.add_all([file, crp])
        await session.flush()
        session.add(
            Measurement(
                lab_file_id=file.id,
                measurement_type_id=crp.id,
                raw_marker_name="CRP",
                normalized_marker_key="crp",
                canonical_value=15.0,
                canonical_unit="mg/L",
                canonical_reference_low=0.0,
                canonical_reference_high=5.0,
                measured_at=utc_now(),
                normalization_status="resolved",
            )
        )
        await session.commit()

    response = await client.put(
        "/api/markers/CRP/tags",
        json={"tags": ["range:onlyOutOfRange", "manual-tag", "range:noRange"]},
    )
    assert response.status_code == 200
    assert "manual-tag" in response.json()
    assert "range:onlyOutOfRange" in response.json()
    assert "range:mostlyOutOfRange" in response.json()
    assert "range:someOutOfRange" in response.json()
    assert "range:noRange" not in response.json()

    async with session_factory() as session:
        result = await session.execute(
            select(MarkerTag.tag)
            .join(MarkerTag.measurement_type)
            .where(MeasurementType.name == "CRP")
            .order_by(MarkerTag.tag.asc())
        )
        assert result.scalars().all() == ["manual-tag"]


@pytest.mark.asyncio
async def test_marker_detail_reuses_history_reference_for_missing_range_rows(client, session_factory):
    async with session_factory() as session:
        first_file = LabFile(
            filename="v1.pdf",
            filepath="/tmp/v1.pdf",
            mime_type="application/pdf",
        )
        second_file = LabFile(
            filename="v2.pdf",
            filepath="/tmp/v2.pdf",
            mime_type="application/pdf",
        )
        marker_type = MeasurementType(
            name="Varicella Zoster Virus (VZV) IgG Antibodies Abs",
            normalized_key="vzg-igg-abs",
            group_name="Immunity & Serology",
            canonical_unit="IU/mL",
        )
        session.add_all([first_file, second_file, marker_type])
        await session.flush()
        session.add(
            MeasurementAlias(
                alias_name="VZV IgG abs.",
                normalized_key=normalize_marker_alias_key("VZV IgG abs."),
                measurement_type_id=marker_type.id,
            )
        )
        session.add_all(
            [
                Measurement(
                    lab_file_id=first_file.id,
                    measurement_type_id=marker_type.id,
                    raw_marker_name="VZV IgG abs.",
                    normalized_marker_key="vzg-igg-abs",
                    canonical_value=1826.0,
                    canonical_unit="IU/mL",
                    measured_at=utc_now(),
                    normalization_status="resolved",
                ),
                Measurement(
                    lab_file_id=second_file.id,
                    measurement_type_id=marker_type.id,
                    raw_marker_name="VZV IgG abs.",
                    normalized_marker_key="vzg-igg-abs",
                    canonical_value=1712.0,
                    canonical_unit="IU/mL",
                    canonical_reference_high=150.0,
                    measured_at=utc_now(),
                    normalization_status="resolved",
                ),
            ]
        )
        await session.commit()

    response = await client.get(
        "/api/measurements/detail",
        params={"marker_name": "Varicella Zoster Virus (VZV) IgG Antibodies Abs"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["aliases"] == ["VZV IgG abs."]
    assert body["reference_high"] == 150.0
    assert "range:onlyOutOfRange" in body["marker_tags"]
    assert "range:mostlyOutOfRange" in body["marker_tags"]
    assert "range:someOutOfRange" in body["marker_tags"]
    assert "range:noRange" not in body["marker_tags"]
