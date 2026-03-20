from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from illdashboard.models import (
    Job,
    LabFile,
    MarkerGroup,
    Measurement,
    MeasurementAlias,
    MeasurementType,
    QualitativeRule,
    RescalingRule,
    SourceAlias,
    utc_now,
)
from illdashboard.services import jobs as job_service
from illdashboard.services import pipeline, qualitative_values, rescaling
from illdashboard.services.markers import normalize_marker_alias_key


@pytest.mark.asyncio
async def test_marker_canonization_deduplicates_alias_rules_for_one_normalized_key(session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="markers.png",
            filepath="/tmp/markers.png",
            mime_type="image/png",
        )
        session.add(lab_file)
        await session.flush()

        normalized_key = normalize_marker_alias_key("CRP")
        session.add_all(
            [
                Measurement(
                    lab_file_id=lab_file.id,
                    raw_marker_name="CRP+",
                    normalized_marker_key=normalized_key,
                ),
                Measurement(
                    lab_file_id=lab_file.id,
                    raw_marker_name="CRP",
                    normalized_marker_key=normalized_key,
                ),
            ]
        )
        job = Job(
            task_type=pipeline.TASK_CANONIZE_MARKER,
            task_key=normalized_key,
            status=job_service.JOB_STATUS_LEASED,
            lease_owner="test-runtime",
            lease_until=utc_now(),
            payload_json=job_service.json_dumps({"raw_name": "CRP"}),
        )
        session.add(job)
        await session.commit()

        async def fake_normalize_marker_names(*_args, **_kwargs):
            assert session.in_transaction() is False
            return {"CRP+": "CRP"}

        with patch(
            "illdashboard.services.pipeline.copilot_normalization.normalize_marker_names",
            new=AsyncMock(side_effect=fake_normalize_marker_names),
        ):
            await pipeline._canonize_marker_jobs(session, [job])
            await session.commit()

        measurement_types = list(
            (
                await session.execute(
                    select(MeasurementType).where(MeasurementType.name == "CRP").order_by(MeasurementType.id.asc())
                )
            ).scalars()
        )
        aliases = list((await session.execute(select(MeasurementAlias).order_by(MeasurementAlias.id.asc()))).scalars())
        jobs = list((await session.execute(select(Job).order_by(Job.task_type.asc(), Job.id.asc()))).scalars())

    assert len(measurement_types) == 1
    assert len(aliases) == 1
    assert aliases[0].measurement_type_id == measurement_types[0].id
    assert aliases[0].normalized_key == normalize_marker_alias_key("CRP")
    assert any(job.task_type == pipeline.TASK_PROCESS_MEASUREMENTS for job in jobs)


@pytest.mark.asyncio
async def test_marker_canonization_batches_multiple_jobs_in_one_llm_call(session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="markers-batch.png",
            filepath="/tmp/markers-batch.png",
            mime_type="image/png",
        )
        session.add(lab_file)
        await session.flush()

        crp_key = normalize_marker_alias_key("CRP")
        alt_key = normalize_marker_alias_key("ALT")
        session.add_all(
            [
                Measurement(
                    lab_file_id=lab_file.id,
                    raw_marker_name="CRP",
                    normalized_marker_key=crp_key,
                ),
                Measurement(
                    lab_file_id=lab_file.id,
                    raw_marker_name="ALT",
                    normalized_marker_key=alt_key,
                ),
            ]
        )
        jobs = [
            Job(
                task_type=pipeline.TASK_CANONIZE_MARKER,
                task_key=crp_key,
                status=job_service.JOB_STATUS_LEASED,
                lease_owner="test-runtime",
                lease_until=utc_now() + timedelta(minutes=5),
                payload_json=job_service.json_dumps({}),
            ),
            Job(
                task_type=pipeline.TASK_CANONIZE_MARKER,
                task_key=alt_key,
                status=job_service.JOB_STATUS_LEASED,
                lease_owner="test-runtime",
                lease_until=utc_now() + timedelta(minutes=5),
                payload_json=job_service.json_dumps({}),
            ),
        ]
        session.add_all(jobs)
        await session.commit()

        with patch(
            "illdashboard.services.pipeline.copilot_normalization.normalize_marker_names",
            new=AsyncMock(return_value={"CRP": "CRP", "ALT": "ALT"}),
        ) as normalize_mock:
            await pipeline._canonize_marker_jobs(session, jobs)
            await session.commit()

    normalize_mock.assert_awaited_once()
    observed_names = normalize_mock.await_args.args[0]
    assert set(observed_names) == {"CRP", "ALT"}


@pytest.mark.asyncio
async def test_source_canonization_releases_transaction_before_copilot_call(session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="source.pdf",
            filepath="/tmp/source.pdf",
            mime_type="application/pdf",
            source_candidate="Synlab",
            source_candidate_key="synlab",
        )
        session.add(lab_file)
        await session.flush()
        job = Job(
            task_type=pipeline.TASK_CANONIZE_SOURCE,
            task_key="synlab",
            status=job_service.JOB_STATUS_LEASED,
            lease_owner="test-runtime",
            lease_until=utc_now() + timedelta(minutes=5),
            payload_json=job_service.json_dumps({}),
        )
        session.add(job)
        await session.commit()

        async def fake_normalize_source_name(*_args, **_kwargs):
            assert session.in_transaction() is False
            return "Synlab"

        with patch(
            "illdashboard.services.pipeline.copilot_normalization.normalize_source_name",
            new=AsyncMock(side_effect=fake_normalize_source_name),
        ):
            await pipeline._canonize_source(session, job)
            await session.commit()

        aliases = list((await session.execute(select(SourceAlias).order_by(SourceAlias.id.asc()))).scalars())
        refreshed_file = await session.get(LabFile, lab_file.id)

    assert [alias.canonical_name for alias in aliases] == ["synlab"]
    assert refreshed_file is not None
    assert refreshed_file.source_name == "synlab"
    assert refreshed_file.source_resolved_at is not None


@pytest.mark.asyncio
async def test_source_canonization_rolls_back_cleanly_when_copilot_call_fails(session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="source-fail.pdf",
            filepath="/tmp/source-fail.pdf",
            mime_type="application/pdf",
            source_candidate="Synlab",
            source_candidate_key="synlab",
        )
        session.add(lab_file)
        await session.flush()
        job = Job(
            task_type=pipeline.TASK_CANONIZE_SOURCE,
            task_key="synlab",
            status=job_service.JOB_STATUS_LEASED,
            lease_owner="test-runtime",
            lease_until=utc_now() + timedelta(minutes=5),
            payload_json=job_service.json_dumps({}),
        )
        session.add(job)
        await session.commit()

        with patch(
            "illdashboard.services.pipeline.copilot_normalization.normalize_source_name",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            with pytest.raises(RuntimeError, match="boom"):
                await pipeline._canonize_source(session, job)

        await session.rollback()
        aliases = list((await session.execute(select(SourceAlias).order_by(SourceAlias.id.asc()))).scalars())
        refreshed_file = await session.get(LabFile, lab_file.id)

    assert aliases == []
    assert refreshed_file is not None
    assert refreshed_file.source_name is None
    assert refreshed_file.source_resolved_at is None


@pytest.mark.asyncio
async def test_common_canonize_handlers_use_fresh_job_session(session_factory):
    runtime = pipeline.PipelineRuntime(session_factory)

    async with session_factory() as outer_session:
        normalized_keys = [normalize_marker_alias_key("CRP"), normalize_marker_alias_key("ALT")]
        outer_session.add_all(
            [
                Job(
                    task_type=pipeline.TASK_CANONIZE_MARKER,
                    task_key=normalized_keys[0],
                    status=job_service.JOB_STATUS_LEASED,
                    lease_owner="test-runtime",
                    lease_until=utc_now() + timedelta(minutes=5),
                    payload_json=job_service.json_dumps({}),
                ),
                Job(
                    task_type=pipeline.TASK_CANONIZE_MARKER,
                    task_key=normalized_keys[1],
                    status=job_service.JOB_STATUS_LEASED,
                    lease_owner="test-runtime",
                    lease_until=utc_now() + timedelta(minutes=5),
                    payload_json=job_service.json_dumps({}),
                ),
            ]
        )
        await outer_session.commit()

        jobs = list(
            (
                await outer_session.execute(
                    select(Job)
                    .where(Job.task_type == pipeline.TASK_CANONIZE_MARKER, Job.task_key.in_(normalized_keys))
                    .order_by(Job.id.asc())
                )
            ).scalars()
        )

        async def fake_canonize_marker_jobs(inner_session, fresh_jobs):
            assert inner_session is not outer_session
            assert [fresh_job.id for fresh_job in fresh_jobs] == [job.id for job in jobs]
            assert all(fresh_job.status == job_service.JOB_STATUS_LEASED for fresh_job in fresh_jobs)

        with patch(
            "illdashboard.services.pipeline._canonize_marker_jobs",
            new=AsyncMock(side_effect=fake_canonize_marker_jobs),
        ):
            await runtime._handle_marker_jobs(outer_session, jobs)


@pytest.mark.asyncio
async def test_enqueue_job_marks_rerun_requested_while_task_is_leased(session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="rerun.png",
            filepath="/tmp/rerun.png",
            mime_type="image/png",
        )
        session.add(lab_file)
        await session.flush()
        job = Job(
            file_id=lab_file.id,
            task_type=pipeline.TASK_PROCESS_MEASUREMENTS,
            task_key=f"file:{lab_file.id}:process-measurements",
            status=job_service.JOB_STATUS_LEASED,
            priority=10,
            lease_owner="worker-1",
            lease_until=utc_now(),
        )
        session.add(job)
        await session.commit()

        await job_service.enqueue_job(
            session,
            task_type=pipeline.TASK_PROCESS_MEASUREMENTS,
            task_key=f"file:{lab_file.id}:process-measurements",
            payload={"file_id": lab_file.id},
            file_id=lab_file.id,
            priority=10,
        )
        await session.flush()
        await session.refresh(job)

        assert job.status == job_service.JOB_STATUS_LEASED
        assert job.rerun_requested is True

        await job_service.mark_job_resolved(session, job)
        await session.commit()

        await session.refresh(job)

    assert job.status == job_service.JOB_STATUS_PENDING
    assert job.rerun_requested is False
    assert job.lease_owner is None
    assert job.lease_until is None


@pytest.mark.asyncio
async def test_process_measurements_logs_noop_outcome_when_pass_changes_nothing(session_factory, caplog):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="noop.png",
            filepath="/tmp/noop.png",
            mime_type="image/png",
        )
        session.add(lab_file)
        await session.flush()
        session.add(
            Measurement(
                lab_file_id=lab_file.id,
                raw_marker_name="CRP",
                normalized_marker_key=normalize_marker_alias_key("CRP"),
                normalization_status=pipeline.MEASUREMENT_STATE_PENDING,
            )
        )
        job = Job(
            file_id=lab_file.id,
            task_type=pipeline.TASK_PROCESS_MEASUREMENTS,
            task_key=f"file:{lab_file.id}:process-measurements",
            status=job_service.JOB_STATUS_LEASED,
            priority=10,
            lease_owner="worker-1",
            lease_until=utc_now(),
            payload_json=job_service.json_dumps({"file_id": lab_file.id}),
        )
        session.add(job)
        await session.commit()

        caplog.set_level("INFO", logger="illdashboard.services.pipeline")
        with patch(
            "illdashboard.services.pipeline._apply_known_measurement_rules",
            new=AsyncMock(return_value=False),
        ):
            await pipeline._process_measurements(session, job)

    assert any(
        "Task span finish task_type=process.measurements" in record.getMessage()
        and "outcome=noop" in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_rescaling_rule_upsert_deduplicates_equivalent_units(session_factory):
    async with session_factory() as session:
        measurement_type = MeasurementType(name="Glucose", normalized_key="glucose", group_name="Metabolic")
        session.add(measurement_type)
        await session.flush()

        await rescaling.upsert_rescaling_rules(
            session,
            [
                {
                    "measurement_type": measurement_type,
                    "original_unit": "mg/dL",
                    "canonical_unit": "mmol/L",
                    "scale_factor": 0.0555,
                },
                {
                    "measurement_type": measurement_type,
                    "original_unit": " mg / dL ",
                    "canonical_unit": " mmol / L ",
                    "scale_factor": 0.0555,
                },
            ],
        )
        await session.commit()

        rules = list(
            (
                await session.execute(
                    select(RescalingRule)
                    .where(RescalingRule.measurement_type_id == measurement_type.id)
                    .order_by(RescalingRule.id.asc())
                )
            ).scalars()
        )

    assert len(rules) == 1
    assert rules[0].normalized_original_unit == rescaling.normalize_unit_key("mg/dL")
    assert rules[0].normalized_canonical_unit == rescaling.normalize_unit_key("mmol/L")


@pytest.mark.asyncio
async def test_apply_known_measurement_rules_marks_unusable_unit_normalization_as_error(session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="unit-error.png",
            filepath="/tmp/unit-error.png",
            mime_type="image/png",
        )
        group = MarkerGroup(name="Metabolic Test", display_order=10)
        measurement_type = MeasurementType(
            name="Glucose",
            normalized_key="glucose",
            group_name=group.name,
            group=group,
            canonical_unit="   ",
        )
        alias = MeasurementAlias(
            alias_name="Glucose",
            normalized_key=normalize_marker_alias_key("Glucose"),
            measurement_type=measurement_type,
        )
        measurement = Measurement(
            lab_file=lab_file,
            raw_marker_name="Glucose",
            normalized_marker_key=normalize_marker_alias_key("Glucose"),
            original_value=5.2,
            original_unit="mg/dL",
        )
        session.add_all([lab_file, group, measurement_type, alias, measurement])
        await session.commit()

        await pipeline._apply_known_measurement_rules(session, [measurement])
        await session.commit()
        await session.refresh(measurement)

    assert measurement.normalization_status == pipeline.MEASUREMENT_STATE_ERROR
    assert measurement.normalization_error == "Unsupported unit normalization for Glucose"


@pytest.mark.asyncio
async def test_apply_known_measurement_rules_marks_unusable_qualitative_normalization_as_error(session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="qual-error.png",
            filepath="/tmp/qual-error.png",
            mime_type="image/png",
        )
        group = MarkerGroup(name="Serology Test", display_order=20)
        measurement_type = MeasurementType(
            name="COVID",
            normalized_key="covid",
            group_name=group.name,
            group=group,
        )
        alias = MeasurementAlias(
            alias_name="COVID",
            normalized_key=normalize_marker_alias_key("COVID"),
            measurement_type=measurement_type,
        )
        measurement = Measurement(
            lab_file=lab_file,
            raw_marker_name="COVID",
            normalized_marker_key=normalize_marker_alias_key("COVID"),
            original_qualitative_value="[]",
        )
        session.add_all([lab_file, group, measurement_type, alias, measurement])
        await session.commit()

        await pipeline._apply_known_measurement_rules(session, [measurement])
        await session.commit()
        await session.refresh(measurement)

    assert measurement.normalization_status == pipeline.MEASUREMENT_STATE_ERROR
    assert measurement.normalization_error == "Unsupported qualitative normalization for []"


@pytest.mark.asyncio
async def test_qualitative_rule_upsert_deduplicates_equivalent_values(session_factory):
    async with session_factory() as session:
        await qualitative_values.upsert_qualitative_rules(
            session,
            [
                {
                    "original_value": "Positive",
                    "canonical_value": "Positive",
                    "boolean_value": True,
                },
                {
                    "original_value": " positive ",
                    "canonical_value": "Positive",
                    "boolean_value": True,
                },
            ],
        )
        await session.commit()

        rules = list((await session.execute(select(QualitativeRule).order_by(QualitativeRule.id.asc()))).scalars())

    assert len(rules) == 1
    assert rules[0].normalized_original_value == qualitative_values.normalize_qualitative_key("Positive")
    assert rules[0].canonical_value == "Positive"
