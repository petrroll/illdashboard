from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from illdashboard.config import settings
from illdashboard.models import (
    READY_FILE_STATUS,
    Job,
    LabFile,
    LabFileTag,
    Measurement,
    MeasurementAlias,
    MeasurementType,
    utc_now,
)
from illdashboard.services import jobs as job_service
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
    ]


@pytest.mark.asyncio
async def test_enqueue_file_extraction_jobs_batches_measurements_two_pages_by_default():
    file = LabFile(
        id=42,
        filename="report.pdf",
        filepath="/tmp/report.pdf",
        mime_type="application/pdf",
        page_count=5,
    )

    with patch("illdashboard.services.pipeline.job_service.enqueue_job", new=AsyncMock()) as enqueue_job_mock:
        await pipeline._enqueue_file_extraction_jobs(object(), file)

    assert [
        (
            call.kwargs["task_type"],
            call.kwargs["payload"]["start_page"],
            call.kwargs["payload"]["stop_page"],
        )
        for call in enqueue_job_mock.await_args_list
    ] == [
        (pipeline.TASK_EXTRACT_MEASUREMENT, 0, 2),
        (pipeline.TASK_EXTRACT_MEASUREMENT, 2, 4),
        (pipeline.TASK_EXTRACT_MEASUREMENT, 4, 5),
    ]


@pytest.mark.asyncio
async def test_reconcile_enqueues_batched_text_jobs_after_measurements_finish(session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="report.pdf",
            filepath="/tmp/report.pdf",
            mime_type="application/pdf",
            page_count=5,
            status="processing",
            measurement_status="done",
            normalization_status="queued",
            text_status="queued",
            summary_status="queued",
            publish_status="queued",
        )
        session.add(lab_file)
        await session.flush()
        reconcile_job = Job(
            file_id=lab_file.id,
            task_type=pipeline.TASK_RECONCILE_FILE,
            task_key=f"file:{lab_file.id}",
            status=job_service.JOB_STATUS_PENDING,
            priority=5,
            payload_json=job_service.json_dumps({"file_id": lab_file.id}),
        )
        session.add(reconcile_job)
        await session.commit()
        file_id = lab_file.id

    async with session_factory() as session:
        job_result = await session.execute(select(Job).where(Job.task_type == pipeline.TASK_RECONCILE_FILE))
        reconcile_job = job_result.scalar_one()
        await pipeline._reconcile_file(session, reconcile_job)
        await session.commit()

        jobs_result = await session.execute(
            select(Job).where(Job.file_id == file_id).order_by(Job.task_type.asc(), Job.task_key.asc())
        )
        jobs = jobs_result.scalars().all()

    assert [job.task_type for job in jobs] == [
        pipeline.TASK_EXTRACT_TEXT,
        pipeline.TASK_EXTRACT_TEXT,
        pipeline.TASK_EXTRACT_TEXT,
    ]
    assert [
        (
            job_service.json_loads(job.payload_json)["start_page"],
            job_service.json_loads(job.payload_json)["stop_page"],
        )
        for job in jobs
    ] == [(0, 2), (2, 4), (4, 5)]


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
async def test_single_file_ocr_does_not_reset_other_incomplete_files(client, session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    first_path = upload_dir / "first.png"
    second_path = upload_dir / "second.png"
    first_path.write_bytes(b"first-image")
    second_path.write_bytes(b"second-image")

    async with session_factory() as session:
        first_file = LabFile(
            filename="first.png",
            filepath=str(first_path),
            mime_type="image/png",
            page_count=1,
        )
        second_file = LabFile(
            filename="second.png",
            filepath=str(second_path),
            mime_type="image/png",
            page_count=1,
            status="processing",
            measurement_status="running",
            normalization_status="queued",
            text_status="queued",
            summary_status="queued",
            publish_status="queued",
            ocr_text_raw="partial text",
        )
        session.add_all([first_file, second_file])
        await session.flush()
        session.add(
            Job(
                file_id=second_file.id,
                task_type=pipeline.TASK_EXTRACT_MEASUREMENT,
                task_key="file:second:measurement",
                status="pending",
                priority=10,
            )
        )
        await session.commit()
        first_file_id = first_file.id
        second_file_id = second_file.id

    queue_response = await client.post(f"/api/files/{first_file_id}/ocr")
    assert queue_response.status_code == 200
    assert queue_response.json() == {"queued_file_ids": [first_file_id]}

    async with session_factory() as session:
        files_result = await session.execute(select(LabFile).order_by(LabFile.id.asc()))
        first_file, second_file = files_result.scalars().all()
        jobs_result = await session.execute(select(Job).order_by(Job.file_id.asc(), Job.task_type.asc()))
        jobs = jobs_result.scalars().all()

    assert first_file.id == first_file_id
    assert first_file.status == "queued"
    assert second_file.id == second_file_id
    assert second_file.status == "processing"
    assert second_file.ocr_text_raw == "partial text"
    assert [job.file_id for job in jobs] == [first_file_id, second_file_id]


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
                new=AsyncMock(
                    return_value={
                        "raw_text": "CRP 15 mg/L",
                        "translated_text_english": "CRP 15 mg/L",
                    }
                ),
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
            task_type=pipeline.TASK_EXTRACT_MEASUREMENT,
            task_key="file:leased:measurement",
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
        result = await session.execute(select(Job).where(Job.task_key == "file:leased:measurement"))
        refreshed_job = result.scalar_one()

    assert refreshed_job.status == job_service.JOB_STATUS_PENDING
    assert refreshed_job.lease_owner is None
    assert refreshed_job.lease_until is None


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
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await shutdown_requested.wait()
                first_run_cancelled.set()
                raise
        return {
            "lab_date": "2026-03-15T00:00:00+00:00",
            "source": "synlab",
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
            new=AsyncMock(return_value="Fresh rerun summary."),
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

            await asyncio.wait_for(first_run_started.wait(), timeout=3.0)

            async with session_factory() as session:
                rerun_file_ids = await asyncio.wait_for(
                    pipeline.queue_files_from_clean_runtime(session, [file_id]),
                    timeout=3.0,
                )
            assert rerun_file_ids == [file_id]
            assert first_run_cancelled.is_set()
            shutdown_client_mock.assert_awaited_once()

            ready_file = await _wait_for_file_ready(session_factory, file_id)
        finally:
            await pipeline.stop_pipeline_runtime()

    assert extract_attempts >= 2
    assert ready_file.ocr_summary_english == "Fresh rerun summary."
    assert len(ready_file.measurements) == 1
    assert ready_file.measurements[0].canonical_value == 42.0


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


@pytest.mark.asyncio
async def test_queue_files_resets_old_jobs_and_incomplete_files(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    ready_path = upload_dir / "ready.png"
    stuck_path = upload_dir / "stuck.png"
    error_path = upload_dir / "error.png"
    ready_path.write_bytes(b"ready-image")
    stuck_path.write_bytes(b"stuck-image")
    error_path.write_bytes(b"error-image")

    async with session_factory() as session:
        ready_file = LabFile(
            filename="ready.png",
            filepath=str(ready_path),
            mime_type="image/png",
            page_count=1,
            status=READY_FILE_STATUS,
            measurement_status="done",
            normalization_status="done",
            text_status="done",
            summary_status="done",
            publish_status="done",
            ocr_raw="published",
            published_at=utc_now(),
        )
        stuck_file = LabFile(
            filename="stuck.png",
            filepath=str(stuck_path),
            mime_type="image/png",
            page_count=1,
            status="processing",
            measurement_status="running",
            normalization_status="queued",
            text_status="queued",
            summary_status="queued",
            publish_status="queued",
            processing_error="timed out",
            source_name="Legacy Lab",
            ocr_text_raw="partial text",
            tags=[LabFileTag(tag="source:legacy-lab")],
        )
        error_file = LabFile(
            filename="error.png",
            filepath=str(error_path),
            mime_type="image/png",
            page_count=1,
            status="error",
            measurement_status="error",
            normalization_status="queued",
            text_status="queued",
            summary_status="queued",
            publish_status="queued",
            processing_error="bad OCR",
            source_name="Broken Lab",
        )
        session.add_all([ready_file, stuck_file, error_file])
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
                    task_type=pipeline.TASK_EXTRACT_MEASUREMENT,
                    task_key="file:stuck:measurement",
                    status="pending",
                    priority=10,
                ),
                Job(
                    file_id=None,
                    task_type=pipeline.TASK_NORMALIZE_MARKER,
                    task_key="crp",
                    status="pending",
                    priority=20,
                ),
            ]
        )
        await session.commit()

    async with session_factory() as session:
        queued_file_ids = await pipeline.queue_files(session, [1])

    assert queued_file_ids == [1]

    async with session_factory() as session:
        files_result = await session.execute(
            select(LabFile).options(selectinload(LabFile.tags)).order_by(LabFile.id.asc())
        )
        ready_file, stuck_file, error_file = files_result.scalars().all()
        jobs_result = await session.execute(select(Job).order_by(Job.task_type.asc(), Job.task_key.asc()))
        jobs = jobs_result.scalars().all()
        measurements_result = await session.execute(select(Measurement).order_by(Measurement.id.asc()))
        measurements = measurements_result.scalars().all()

    assert ready_file.status == "queued"
    assert ready_file.measurement_status == "queued"
    assert ready_file.text_status == "queued"

    assert stuck_file.status == "uploaded"
    assert stuck_file.measurement_status == "queued"
    assert stuck_file.normalization_status == "queued"
    assert stuck_file.text_status == "queued"
    assert stuck_file.processing_error is None
    assert stuck_file.source_name is None
    assert stuck_file.ocr_text_raw is None
    assert stuck_file.tags == []

    assert error_file.status == "uploaded"
    assert error_file.measurement_status == "queued"
    assert error_file.processing_error is None
    assert error_file.source_name is None

    assert measurements == []
    assert [job.task_type for job in jobs] == [
        pipeline.TASK_EXTRACT_MEASUREMENT,
    ]
    assert all(job.file_id == ready_file.id for job in jobs)


@pytest.mark.asyncio
async def test_queue_unprocessed_files_requeues_reset_incomplete_rows(session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    uploaded_path = upload_dir / "uploaded.png"
    stuck_path = upload_dir / "retry.png"
    uploaded_path.write_bytes(b"uploaded-image")
    stuck_path.write_bytes(b"retry-image")

    async with session_factory() as session:
        uploaded_file = LabFile(
            filename="uploaded.png",
            filepath=str(uploaded_path),
            mime_type="image/png",
            page_count=1,
        )
        stuck_file = LabFile(
            filename="retry.png",
            filepath=str(stuck_path),
            mime_type="image/png",
            page_count=1,
            status="processing",
            measurement_status="running",
            normalization_status="queued",
            text_status="queued",
            summary_status="queued",
            publish_status="queued",
        )
        session.add_all([uploaded_file, stuck_file])
        await session.flush()
        session.add(
            Job(
                file_id=stuck_file.id,
                task_type=pipeline.TASK_EXTRACT_MEASUREMENT,
                task_key="file:retry:measurement",
                status="pending",
                priority=10,
            )
        )
        await session.commit()

    async with session_factory() as session:
        queued_file_ids = await pipeline.queue_unprocessed_files(session)

    assert set(queued_file_ids) == {1, 2}

    async with session_factory() as session:
        files_result = await session.execute(select(LabFile).order_by(LabFile.id.asc()))
        files = files_result.scalars().all()
        jobs_result = await session.execute(select(Job).order_by(Job.file_id.asc(), Job.task_type.asc()))
        jobs = jobs_result.scalars().all()

    assert all(file.status == "queued" for file in files)
    assert [job.file_id for job in jobs] == [1, 2]


@pytest.mark.asyncio
async def test_cancel_ocr_clears_jobs_and_resets_active_files(client, session_factory):
    upload_dir = Path(settings.UPLOAD_DIR)
    ready_path = upload_dir / "cancel-ready.png"
    queued_path = upload_dir / "cancel-queued.png"
    processing_path = upload_dir / "cancel-processing.png"
    ready_path.write_bytes(b"ready-image")
    queued_path.write_bytes(b"queued-image")
    processing_path.write_bytes(b"processing-image")

    async with session_factory() as session:
        ready_file = LabFile(
            filename="cancel-ready.png",
            filepath=str(ready_path),
            mime_type="image/png",
            page_count=1,
            status=READY_FILE_STATUS,
            measurement_status="done",
            normalization_status="done",
            text_status="done",
            summary_status="done",
            publish_status="done",
            ocr_raw="published",
            published_at=utc_now(),
        )
        queued_file = LabFile(
            filename="cancel-queued.png",
            filepath=str(queued_path),
            mime_type="image/png",
            page_count=1,
            status="queued",
            measurement_status="queued",
            normalization_status="queued",
            text_status="queued",
            summary_status="queued",
            publish_status="queued",
            source_name="Queued Lab",
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
            measurement_status="running",
            normalization_status="queued",
            text_status="running",
            summary_status="queued",
            publish_status="queued",
            processing_error="still running",
            source_name="Processing Lab",
            ocr_text_raw="partial text",
            tags=[LabFileTag(tag="source:processing-lab")],
        )
        session.add_all([ready_file, queued_file, processing_file])
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
                    task_type=pipeline.TASK_EXTRACT_MEASUREMENT,
                    task_key="file:cancel:queued:measurement",
                    status="pending",
                    priority=10,
                ),
                Job(
                    file_id=processing_file.id,
                    task_type=pipeline.TASK_EXTRACT_TEXT,
                    task_key="file:cancel:processing:text",
                    status=job_service.JOB_STATUS_LEASED,
                    priority=20,
                    lease_owner="runtime:test",
                    lease_until=utc_now(),
                ),
                Job(
                    file_id=None,
                    task_type=pipeline.TASK_NORMALIZE_MARKER,
                    task_key="crp",
                    status="pending",
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
        ready_file, queued_file, processing_file = files_result.scalars().all()
        jobs_result = await session.execute(select(Job).order_by(Job.id.asc()))
        jobs = jobs_result.scalars().all()
        measurements_result = await session.execute(select(Measurement).order_by(Measurement.id.asc()))
        measurements = measurements_result.scalars().all()

    assert ready_file.status == READY_FILE_STATUS
    assert ready_file.publish_status == "done"

    assert queued_file.status == "uploaded"
    assert queued_file.measurement_status == "queued"
    assert queued_file.text_status == "queued"
    assert queued_file.processing_error is None
    assert queued_file.source_name is None
    assert queued_file.ocr_text_raw is None
    assert queued_file.ocr_summary_english is None
    assert queued_file.tags == []

    assert processing_file.status == "uploaded"
    assert processing_file.measurement_status == "queued"
    assert processing_file.normalization_status == "queued"
    assert processing_file.text_status == "queued"
    assert processing_file.processing_error is None
    assert processing_file.source_name is None
    assert processing_file.ocr_text_raw is None
    assert processing_file.tags == []

    assert measurements == []
    assert jobs == []
