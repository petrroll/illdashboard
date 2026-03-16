import asyncio
import json
from collections.abc import Callable
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from illdashboard.copilot import client as copilot_client
from illdashboard.copilot import explanations as copilot_explanations
from illdashboard.copilot import normalization as copilot_normalization
from illdashboard.copilot import extraction as copilot_ocr


class DummySession:
    def __init__(self, *, response=None, send_error: Exception | None = None, usage_cost: float | None = None):
        self._handler: Callable | None = None
        self._response = response
        self._send_error = send_error
        self._usage_cost = usage_cost
        self.disconnect = AsyncMock()

    def on(self, handler: Callable):
        self._handler = handler

        def unsubscribe():
            self._handler = None

        return unsubscribe

    async def send_and_wait(self, *_args, **_kwargs):
        if self._usage_cost is not None and self._handler is not None:
            self._handler(
                SimpleNamespace(
                    type=copilot_client.SessionEventType.ASSISTANT_USAGE,
                    data=SimpleNamespace(cost=self._usage_cost),
                )
            )

        if self._send_error is not None:
            raise self._send_error

        return self._response


class DummyDoc:
    def __init__(self, page_count: int):
        self.page_count = page_count

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@pytest.fixture(autouse=True)
def _no_retry_delay():
    with patch.object(copilot_ocr, "OCR_RETRY_DELAY", 0):
        yield


def _medical_calls(mock) -> list:
    return [args for args in mock.await_args_list if args.args[1] is copilot_ocr._MEDICAL_EXTRACTION]


@pytest.mark.asyncio
async def test_ocr_extract_splits_oversized_batches_and_preserves_page_numbers():
    async def fake_batch(
        pdf_path: str,
        kind: copilot_ocr._ExtractionKind,
        *,
        start_page: int,
        stop_page: int,
        dpi: int,
        filename: str | None = None,
        render_cache=None,
    ):
        if kind is copilot_ocr._TEXT_EXTRACTION:
            return {"raw_text": "text", "translated_text_english": "text"}
        if stop_page - start_page > 1:
            raise Exception("CAPIError: 413 failed to parse request")
        return {
            "lab_date": "2025-09-05",
            "source": "synlab",
            "measurements": [
                {
                    "marker_name": f"Marker {start_page + 1}",
                    "value": start_page + 1,
                    "unit": "mmol/l",
                    "reference_low": None,
                    "reference_high": None,
                    "measured_at": None,
                    "page_number": 1,
                }
            ],
        }

    with patch("illdashboard.copilot.extraction.fitz.open", return_value=DummyDoc(page_count=4)), patch(
        "illdashboard.copilot.extraction._pdf_batch_extract",
        new=AsyncMock(side_effect=fake_batch),
    ) as batch_mock, patch("illdashboard.copilot.extraction._generate_medical_summary", new=AsyncMock(return_value=None)):
        result = await copilot_ocr.ocr_extract("/tmp/report.pdf")

    assert result["lab_date"] == "2025-09-05"
    assert result["source"] == "synlab"
    assert [measurement["page_number"] for measurement in result["measurements"]] == [1, 2, 3, 4]
    observed_calls = sorted(
        [
            (args.args[0], args.kwargs["start_page"], args.kwargs["stop_page"], args.kwargs["dpi"], args.kwargs["filename"])
            for args in _medical_calls(batch_mock)
        ],
        key=lambda item: (item[1], item[2], item[3]),
    )
    assert observed_calls == [
        ("/tmp/report.pdf", 0, 1, 144, None),
        ("/tmp/report.pdf", 0, 2, 144, None),
        ("/tmp/report.pdf", 1, 2, 144, None),
        ("/tmp/report.pdf", 2, 3, 144, None),
        ("/tmp/report.pdf", 2, 4, 144, None),
        ("/tmp/report.pdf", 3, 4, 144, None),
    ]


@pytest.mark.asyncio
async def test_ocr_extract_retries_single_page_at_lower_dpi_after_413():
    async def fake_batch(
        pdf_path: str,
        kind: copilot_ocr._ExtractionKind,
        *,
        start_page: int,
        stop_page: int,
        dpi: int,
        filename: str | None = None,
        render_cache=None,
    ):
        if kind is copilot_ocr._TEXT_EXTRACTION:
            return {"raw_text": "Sodium 141 mmol/l", "translated_text_english": "Sodium 141 mmol/l"}
        if dpi == copilot_ocr.OCR_PDF_RENDER_DPI:
            raise Exception("CAPIError: 413 failed to parse request")
        return {
            "lab_date": None,
            "source": None,
            "measurements": [
                {
                    "marker_name": "Sodium",
                    "value": 141,
                    "unit": "mmol/l",
                    "reference_low": 136,
                    "reference_high": 145,
                    "measured_at": None,
                    "page_number": 1,
                }
            ],
        }

    with patch("illdashboard.copilot.extraction.fitz.open", return_value=DummyDoc(page_count=1)), patch(
        "illdashboard.copilot.extraction._pdf_batch_extract",
        new=AsyncMock(side_effect=fake_batch),
    ) as batch_mock, patch("illdashboard.copilot.extraction._generate_medical_summary", new=AsyncMock(return_value=None)):
        result = await copilot_ocr.ocr_extract("/tmp/report.pdf")

    assert result == {
        "lab_date": None,
        "source": None,
        "raw_text": "Sodium 141 mmol/l",
        "translated_text_english": "Sodium 141 mmol/l",
        "measurements": [
            {
                "marker_name": "Sodium",
                "value": 141,
                "unit": "mmol/l",
                "reference_low": 136,
                "reference_high": 145,
                "measured_at": None,
                "page_number": 1,
            }
        ],
    }
    medical = _medical_calls(batch_mock)
    assert [(c.args[0], c.kwargs["start_page"], c.kwargs["stop_page"], c.kwargs["dpi"], c.kwargs["filename"]) for c in medical] == [
        ("/tmp/report.pdf", 0, 1, 144, None),
        ("/tmp/report.pdf", 0, 1, 120, None),
    ]


@pytest.mark.asyncio
async def test_extract_file_processes_page_batches_in_parallel():
    first_batch_started = asyncio.Event()
    allow_first_batch_finish = asyncio.Event()
    second_batch_started = asyncio.Event()

    async def fake_batch(
        pdf_path: str,
        kind: copilot_ocr._ExtractionKind,
        *,
        start_page: int,
        stop_page: int,
        dpi: int,
        filename: str | None = None,
        render_cache=None,
    ):
        if start_page == 0:
            first_batch_started.set()
            await allow_first_batch_finish.wait()
        else:
            await first_batch_started.wait()
            second_batch_started.set()
            allow_first_batch_finish.set()

        return {
            "lab_date": None,
            "source": None,
            "measurements": [
                {
                    "marker_name": f"Marker {start_page + 1}",
                    "value": start_page + 1,
                    "unit": "mmol/l",
                    "reference_low": None,
                    "reference_high": None,
                    "measured_at": None,
                    "page_number": 1,
                }
            ],
        }

    with patch("illdashboard.copilot.extraction.fitz.open", return_value=DummyDoc(page_count=4)), patch.object(
        copilot_ocr,
        "OCR_PDF_BATCH_CONCURRENCY",
        2,
    ), patch(
        "illdashboard.copilot.extraction._pdf_batch_extract",
        new=AsyncMock(side_effect=fake_batch),
    ):
        result = await copilot_ocr._extract_file("/tmp/report.pdf", copilot_ocr._MEDICAL_EXTRACTION)

    assert second_batch_started.is_set()
    assert [measurement["page_number"] for measurement in result["measurements"]] == [1, 3]


@pytest.mark.asyncio
async def test_normalize_marker_names_splits_large_batches():
    responses = [
        '{"Marker 1": "Canonical 1", "Marker 2": "Canonical 2"}',
        '{"Marker 3": "Canonical 3"}',
    ]

    with patch.object(copilot_normalization, "MARKER_NORMALIZATION_BATCH_SIZE", 2), patch.object(
        copilot_normalization,
        "MARKER_NORMALIZATION_CONCURRENCY",
        1,
    ), patch("illdashboard.copilot.normalization._ask", new=AsyncMock(side_effect=responses)) as ask_mock:
        result = await copilot_normalization.normalize_marker_names(
            ["Marker 1", "Marker 2", "Marker 3"],
            ["Existing Marker"],
        )

    assert result == {
        "Marker 1": "Canonical 1",
        "Marker 2": "Canonical 2",
        "Marker 3": "Canonical 3",
    }
    assert ask_mock.await_count == 2


@pytest.mark.asyncio
async def test_choose_canonical_units_skips_homogeneous_units_without_llm():
    with patch("illdashboard.copilot.normalization._ask", new=AsyncMock()) as ask_mock:
        result = await copilot_normalization.choose_canonical_units(
            [
                copilot_normalization.MarkerUnitGroup(
                    marker_name="Sodium",
                    existing_canonical_unit="mmol/L",
                    observations=[
                        copilot_normalization.MarkerObservation(
                            id="0",
                            value=141,
                            unit="mmol/l",
                            reference_low=136,
                            reference_high=145,
                        ),
                        copilot_normalization.MarkerObservation(
                            id="1",
                            value=139,
                            unit="mmol/L",
                            reference_low=136,
                            reference_high=145,
                        ),
                    ],
                )
            ]
        )

    assert result == {"Sodium": "mmol/L"}
    ask_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_normalize_qualitative_values_includes_existing_canonical_values_in_prompt():
    requests = [
        copilot_normalization.QualitativeNormalizationRequest(
            id="negative",
            marker_name="Chlamydia psittaci IgG",
            original_value="NEGATIVE",
        )
    ]
    response = json.dumps({"negative": {"canonical_value": "negative", "boolean_value": False}})

    with patch("illdashboard.copilot.normalization._ask", new=AsyncMock(return_value=response)) as ask_mock:
        result = await copilot_normalization.normalize_qualitative_values(requests, ["negative"])

    assert result == {"negative": ("negative", False)}
    ask_mock.assert_awaited_once()
    _system_prompt, user_prompt = ask_mock.await_args.args
    assert "EXISTING canonical qualitative values:\n- negative\n" in user_prompt


@pytest.mark.asyncio
async def test_infer_rescaling_factors_splits_large_batches():
    responses = [
        json.dumps(
            {
                "req-1": {"scale_factor": 0.001},
                "req-2": {"scale_factor": 1},
            }
        ),
        json.dumps({"req-3": {"scale_factor": 10}}),
    ]

    conversion_requests = [
        copilot_normalization.UnitConversionRequest(
            id="req-1",
            marker_name="Marker 1",
            original_unit="cells/µL",
            canonical_unit="10^9/L",
            example_value=380,
            reference_low=None,
            reference_high=None,
        ),
        copilot_normalization.UnitConversionRequest(
            id="req-2",
            marker_name="Marker 2",
            original_unit="mmol/l",
            canonical_unit="mmol/L",
            example_value=4.2,
            reference_low=3.5,
            reference_high=5.1,
        ),
        copilot_normalization.UnitConversionRequest(
            id="req-3",
            marker_name="Marker 3",
            original_unit="g/dL",
            canonical_unit="g/L",
            example_value=15.6,
            reference_low=13.5,
            reference_high=17.5,
        ),
    ]

    with patch.object(copilot_normalization, "UNIT_NORMALIZATION_BATCH_SIZE", 2), patch.object(
        copilot_normalization,
        "UNIT_NORMALIZATION_CONCURRENCY",
        1,
    ), patch("illdashboard.copilot.normalization._ask", new=AsyncMock(side_effect=responses)) as ask_mock:
        result = await copilot_normalization.infer_rescaling_factors(conversion_requests)

    assert result == {
        "req-1": 0.001,
        "req-2": 1.0,
        "req-3": 10.0,
    }
    assert ask_mock.await_count == 2


@pytest.mark.asyncio
async def test_ask_adds_observed_premium_usage_cost():
    session = DummySession(
        response=SimpleNamespace(data=SimpleNamespace(content="ok")),
        usage_cost=1.0,
    )
    client = SimpleNamespace(create_session=AsyncMock(return_value=session))

    with patch("illdashboard.copilot.client._get_client", new=AsyncMock(return_value=client)), patch(
        "illdashboard.copilot.client.add_premium_requests"
    ) as add_mock:
        result = await copilot_client._ask("system", "user")

    assert result == "ok"
    add_mock.assert_called_once_with(1.0)
    session.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_ask_adds_observed_usage_even_when_send_fails():
    session = DummySession(
        send_error=RuntimeError("boom"),
        usage_cost=1.0,
    )
    client = SimpleNamespace(create_session=AsyncMock(return_value=session))

    with patch("illdashboard.copilot.client._get_client", new=AsyncMock(return_value=client)), patch(
        "illdashboard.copilot.client.add_premium_requests"
    ) as add_mock:
        with pytest.raises(RuntimeError, match="boom"):
            await copilot_client._ask("system", "user")

    add_mock.assert_called_once_with(1.0)
    session.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_explain_marker_history_prompt_avoids_generic_caution_and_trend_filler():
    with patch("illdashboard.copilot.explanations._ask", new=AsyncMock(return_value="ok")) as ask_mock:
        result = await copilot_explanations.explain_marker_history(
            "Potassium",
            [
                {
                    "date": "2026-03-15",
                    "value": 3.2,
                    "unit": "mmol/L",
                    "reference_low": 3.5,
                    "reference_high": 5.1,
                }
            ],
        )

    assert result == "ok"
    system_prompt, user_prompt = ask_mock.await_args.args
    assert "Do not add a generic caution or disclaimer section" in system_prompt
    assert "do not dwell on the lack of a trend" in system_prompt
    assert "Please explain the history of Potassium." in user_prompt
