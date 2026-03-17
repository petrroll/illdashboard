from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from illdashboard.models import (
    Job,
    LabFile,
    Measurement,
    MeasurementAlias,
    MeasurementType,
    QualitativeRule,
    RescalingRule,
)
from illdashboard.services import pipeline, qualitative_values, rescaling
from illdashboard.services.markers import normalize_marker_alias_key, normalized_marker_key


@pytest.mark.asyncio
async def test_marker_resolution_deduplicates_alias_rules_within_batch(session_factory):
    async with session_factory() as session:
        lab_file = LabFile(
            filename="markers.png",
            filepath="/tmp/markers.png",
            mime_type="image/png",
        )
        session.add(lab_file)
        await session.flush()

        marker_jobs: list[Job] = []
        for raw_name in ("CRP+", "CRP"):
            measurement = Measurement(
                lab_file_id=lab_file.id,
                raw_marker_name=raw_name,
                normalized_marker_key=normalized_marker_key(raw_name),
            )
            job = Job(
                file_id=lab_file.id,
                task_type=pipeline.TASK_NORMALIZE_MARKER,
                task_key=measurement.normalized_marker_key,
            )
            session.add_all([measurement, job])
            marker_jobs.append(job)

        await session.flush()

        with patch(
            "illdashboard.services.pipeline.copilot_normalization.normalize_marker_names",
            new=AsyncMock(return_value={"CRP+": "CRP", "CRP": "CRP"}),
        ):
            await pipeline._resolve_marker_jobs(session, marker_jobs)

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
    assert [job.task_type for job in jobs].count(pipeline.TASK_NORMALIZE_GROUP) == 1


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
