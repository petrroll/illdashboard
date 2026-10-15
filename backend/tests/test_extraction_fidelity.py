"""Synthetic contract tests for how extracted results are parsed and persisted."""

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from illdashboard.copilot import normalization as copilot_normalization
from illdashboard.models import LabFile, Measurement, MeasurementBatch
from illdashboard.schemas import MeasurementOut
from illdashboard.services import extraction_eval, pipeline
from illdashboard.services.upload_metadata import filename_date, write_original_name_sidecar


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (141, (141.0, None, None)),
        ("1,75", (1.75, None, None)),
        ("1 000", (1000.0, None, None)),
        ("12 345,6", (12345.6, None, None)),
        ("<0.1", (0.1, None, "<")),
        ("< 5", (5.0, None, "<")),
        ("<=5", (5.0, None, "<=")),
        ("≤5", (5.0, None, "<=")),
        (">150", (150.0, None, ">")),
        (">=150", (150.0, None, ">=")),
        ("≥150", (150.0, None, ">=")),
        ("<0,1", (0.1, None, "<")),
        # Ambiguous digit groups stay visible instead of becoming a guessed decimal.
        ("1 75", (None, "1 75", None)),
        # Titers are dilutions, not scalar bounds.
        ("<1:80", (None, "<1:80", None)),
        ("1:160", (None, "1:160", None)),
        ("Not detected", (None, "not detected", None)),
    ],
)
def test_parse_measurement_value_preserves_source_semantics(raw, expected):
    assert pipeline._parse_measurement_value(raw) == expected


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("2025-03-14 blood panel.pdf", datetime(2025, 3, 14, tzinfo=UTC)),
        ("lab_20250314.pdf", datetime(2025, 3, 14, tzinfo=UTC)),
        ("results 14.03.2025.pdf", datetime(2025, 3, 14, tzinfo=UTC)),
        ("results_3-2-2023.png", datetime(2023, 2, 3, tzinfo=UTC)),
        # Month-only names would need an invented day.
        ("2024-01-blood-panel.pdf", None),
        # Two different dates are ambiguous.
        ("2025-03-14 vs 2024-02-05.pdf", None),
        ("2024-13-40.pdf", None),
        ("report.pdf", None),
        (None, None),
    ],
)
def test_filename_date_only_accepts_one_unambiguous_full_date(filename, expected):
    assert filename_date(filename) == expected


@pytest.mark.asyncio
async def test_persist_measurement_batch_applies_guardrails(session_factory):
    future = (datetime.now(UTC) + timedelta(days=30)).date().isoformat()
    result = {
        "measurements": [
            {
                "marker_name": "Specific IgE",
                "value": "<0.1",
                "unit": "kUA/l",
                "page_number": 3,
                "source_text": "Specific IgE <0,1 kUA/l",
            },
            {"marker_name": "Amylase", "value": "14.03.2025", "unit": "ukat/l", "page_number": 3},
            {"marker_name": "CRP", "value": 4, "unit": "mg/l", "page_number": 9, "measured_at": future},
            {"marker_name": "TSH", "value": "1,73", "unit": "mU/l", "page_number": 4, "measured_at": "2025-03-14"},
        ]
    }

    async with session_factory() as session:
        lab_file = LabFile(filename="report.pdf", filepath="/tmp/report.pdf", mime_type="application/pdf", page_count=4)
        session.add(lab_file)
        await session.flush()
        job = SimpleNamespace(task_key="file:1:measurements:2:4:144")
        await pipeline._persist_measurement_batch(session, lab_file, job, result, start_page=2, stop_page=4, dpi=144)
        await session.commit()
        batch = (await session.execute(select(MeasurementBatch))).scalar_one()

        rows = (
            (
                await session.execute(
                    select(Measurement)
                    .options(selectinload(Measurement.lab_file), selectinload(Measurement.measurement_type))
                    .order_by(Measurement.id)
                )
            )
            .scalars()
            .all()
        )

    by_name = {row.raw_marker_name: row for row in rows}
    assert set(by_name) == {"Specific IgE", "CRP", "TSH"}

    ige = by_name["Specific IgE"]
    assert (ige.original_value, ige.original_value_comparator, ige.original_qualitative_value) == (0.1, "<", None)
    assert ige.page_number == 3
    assert MeasurementOut.model_validate(ige).original_value_comparator == "<"
    assert MeasurementOut.model_validate(ige).source_text == "Specific IgE <0,1 kUA/l"
    assert json.loads(batch.raw_response_json) == result

    crp = by_name["CRP"]
    assert crp.page_number is None
    assert crp.measured_at is None

    tsh = by_name["TSH"]
    assert tsh.original_value == 1.73
    assert tsh.original_value_comparator is None
    assert tsh.measured_at.date().isoformat() == "2025-03-14"


def test_marker_normalization_prompt_shows_result_kind_evidence():
    user_text = copilot_normalization._build_marker_name_normalization_user_text(
        ["Lymfocyty"],
        [],
        raw_examples_by_name={"Lymfocyty": ["Lymfocyty"]},
        observed_units_by_name={"Lymfocyty": ["%"]},
        example_results_by_name={"Lymfocyty": ["31.2 %", "<1 %"]},
    )

    assert "Example results:\n  - 31.2 %\n  - <1 %" in user_text
    assert '"B - Lymphocytes" is total lymphocytes' in copilot_normalization.NORMALIZE_SYSTEM_PROMPT


@pytest.mark.asyncio
async def test_evaluator_scores_binding_comparators_and_dates_from_cached_replies(tmp_path):
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    stored = uploads / "0a1b.pdf"
    stored.write_bytes(b"%PDF synthetic")
    write_original_name_sidecar(stored, "panel.pdf")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "id": "dense-row",
                        "file": "panel.pdf",
                        "pages": [1],
                        "expected": [
                            {"label": ["amyl"], "value": 0.82, "measured_at": "2025-03-14"},
                            {"label": ["IgE"], "value": 0.1, "comparator": "<"},
                            {"label": ["TSH"], "value": 1.73},
                        ],
                        "forbidden": [{"label": ["amyl"], "value": 14}],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    reply = {
        "lab_date": None,
        "source": None,
        "measurements": [
            {"marker_name": "AMS", "value": 14, "unit": "ukat/l", "source_text": "AMS amylase (14.03.2025) 0,82"},
            {"marker_name": "Amylase", "value": 0.82, "unit": "ukat/l", "measured_at": "2025-03-14"},
            {"marker_name": "Specific IgE", "value": "<0.1", "unit": "kUA/l"},
            {"marker_name": "TSH", "value": 4.2, "unit": "mU/l"},
        ],
    }
    extract_mock = AsyncMock(return_value=reply)
    out_dir = tmp_path / "out"
    with patch.object(extraction_eval.copilot_extraction, "extract_measurement_batch", new=extract_mock):
        first = await extraction_eval.evaluate(manifest, files_dir=uploads, out_dir=out_dir)
        second = await extraction_eval.evaluate(manifest, files_dir=uploads, out_dir=out_dir)

    assert extract_mock.await_count == 1
    assert extract_mock.await_args.args[0] == str(stored)
    assert first["summary"]["found"] == 2
    assert first["summary"]["expected"] == 3
    assert first["summary"]["date_correct"] == 1
    assert first["summary"]["forbidden_hits"] == 1
    tsh = first["cases"][0]["expected"][2]
    assert tsh["found"] is False
    assert [candidate["value"] for candidate in tsh["same_label_candidates"]] == [4.2]
    assert second["summary"]["extraction_requests"] == 0
    assert second["summary"]["cached_pages"] == 1
    assert (out_dir / "report.json").is_file()
