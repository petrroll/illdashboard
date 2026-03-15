import asyncio
import json
from collections.abc import Callable
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, call, patch

import pytest

from illdashboard import copilot_service


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
                    type=copilot_service.SessionEventType.ASSISTANT_USAGE,
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
    with patch.object(copilot_service, "OCR_RETRY_DELAY", 0):
        yield


@pytest.mark.asyncio
async def test_extract_structured_medical_data_from_pdf_splits_oversized_batches_and_preserves_page_numbers():
    async def fake_batch(
        pdf_path: str,
        *,
        start_page: int,
        stop_page: int,
        dpi: int,
        filename: str | None = None,
        render_cache=None,
    ):
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

    with patch("illdashboard.copilot_service.fitz.open", return_value=DummyDoc(page_count=4)), patch(
        "illdashboard.copilot_service._extract_structured_medical_data_pdf_batch",
        new=AsyncMock(side_effect=fake_batch),
    ) as batch_mock, patch(
        "illdashboard.copilot_service._extract_document_text",
        new=AsyncMock(return_value={"raw_text": "text", "translated_text_english": "text"}),
    ), patch("illdashboard.copilot_service._generate_medical_summary", new=AsyncMock(return_value=None)):
        result = await copilot_service.ocr_extract("/tmp/report.pdf")

    assert result["lab_date"] == "2025-09-05"
    assert result["source"] == "synlab"
    assert [measurement["page_number"] for measurement in result["measurements"]] == [1, 2, 3, 4]
    assert [measurement["marker_name"] for measurement in result["measurements"]] == [
        "Marker 1",
        "Marker 2",
        "Marker 3",
        "Marker 4",
    ]
    observed_calls = sorted(
        [
            (args.args[0], args.kwargs["start_page"], args.kwargs["stop_page"], args.kwargs["dpi"], args.kwargs["filename"])
            for args in batch_mock.await_args_list
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
async def test_extract_structured_medical_data_from_pdf_splits_timed_out_batches_and_preserves_page_numbers():
    async def fake_batch(
        pdf_path: str,
        *,
        start_page: int,
        stop_page: int,
        dpi: int,
        filename: str | None = None,
        render_cache=None,
    ):
        if stop_page - start_page > 1:
            raise TimeoutError("Timeout after 120s waiting for session.idle")
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

    with patch("illdashboard.copilot_service.fitz.open", return_value=DummyDoc(page_count=2)), patch(
        "illdashboard.copilot_service._extract_structured_medical_data_pdf_batch",
        new=AsyncMock(side_effect=fake_batch),
    ) as batch_mock, patch(
        "illdashboard.copilot_service._extract_document_text",
        new=AsyncMock(return_value={"raw_text": "text", "translated_text_english": "text"}),
    ), patch("illdashboard.copilot_service._generate_medical_summary", new=AsyncMock(return_value=None)):
        result = await copilot_service.ocr_extract("/tmp/report.pdf")

    assert result["lab_date"] == "2025-09-05"
    assert result["source"] == "synlab"
    assert [measurement["page_number"] for measurement in result["measurements"]] == [1, 2]
    assert [measurement["marker_name"] for measurement in result["measurements"]] == [
        "Marker 1",
        "Marker 2",
    ]
    assert batch_mock.await_args_list == [
        call("/tmp/report.pdf", start_page=0, stop_page=2, dpi=144, filename=None, render_cache=ANY),
        call("/tmp/report.pdf", start_page=0, stop_page=1, dpi=144, filename=None, render_cache=ANY),
        call("/tmp/report.pdf", start_page=1, stop_page=2, dpi=144, filename=None, render_cache=ANY),
    ]


@pytest.mark.asyncio
async def test_extract_structured_medical_data_from_pdf_falls_back_to_single_pages_after_429():
    async def fake_batch(
        pdf_path: str,
        *,
        start_page: int,
        stop_page: int,
        dpi: int,
        filename: str | None = None,
        render_cache=None,
    ):
        if stop_page - start_page > 1:
            raise RuntimeError("429 Too Many Requests")
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

    with patch("illdashboard.copilot_service.fitz.open", return_value=DummyDoc(page_count=2)), patch(
        "illdashboard.copilot_service._extract_structured_medical_data_pdf_batch",
        new=AsyncMock(side_effect=fake_batch),
    ) as batch_mock, patch(
        "illdashboard.copilot_service._extract_document_text",
        new=AsyncMock(return_value={"raw_text": "text", "translated_text_english": "text"}),
    ), patch("illdashboard.copilot_service._generate_medical_summary", new=AsyncMock(return_value=None)):
        result = await copilot_service.ocr_extract("/tmp/report.pdf")

    assert result["lab_date"] == "2025-09-05"
    assert result["source"] == "synlab"
    assert [measurement["page_number"] for measurement in result["measurements"]] == [1, 2]
    assert batch_mock.await_args_list == [
        call("/tmp/report.pdf", start_page=0, stop_page=2, dpi=144, filename=None, render_cache=ANY),
        call("/tmp/report.pdf", start_page=0, stop_page=1, dpi=144, filename=None, render_cache=ANY),
        call("/tmp/report.pdf", start_page=1, stop_page=2, dpi=144, filename=None, render_cache=ANY),
    ]


@pytest.mark.asyncio
async def test_extract_structured_medical_data_from_pdf_retries_single_page_at_lower_dpi_after_413():
    async def fake_batch(
        pdf_path: str,
        *,
        start_page: int,
        stop_page: int,
        dpi: int,
        filename: str | None = None,
        render_cache=None,
    ):
        if dpi == copilot_service.OCR_PDF_RENDER_DPI:
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

    with patch("illdashboard.copilot_service.fitz.open", return_value=DummyDoc(page_count=1)), patch(
        "illdashboard.copilot_service._extract_structured_medical_data_pdf_batch",
        new=AsyncMock(side_effect=fake_batch),
    ) as batch_mock, patch(
        "illdashboard.copilot_service._extract_document_text",
        new=AsyncMock(return_value={"raw_text": "Sodium 141 mmol/l", "translated_text_english": "Sodium 141 mmol/l"}),
    ), patch("illdashboard.copilot_service._generate_medical_summary", new=AsyncMock(return_value=None)):
        result = await copilot_service.ocr_extract("/tmp/report.pdf")

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
    assert batch_mock.await_args_list == [
        call("/tmp/report.pdf", start_page=0, stop_page=1, dpi=144, filename=None, render_cache=ANY),
        call("/tmp/report.pdf", start_page=0, stop_page=1, dpi=120, filename=None, render_cache=ANY),
    ]


@pytest.mark.asyncio
async def test_extract_structured_medical_data_from_pdf_processes_page_batches_in_parallel():
    first_batch_started = asyncio.Event()
    allow_first_batch_finish = asyncio.Event()
    second_batch_started = asyncio.Event()

    async def fake_batch(
        pdf_path: str,
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

    with patch("illdashboard.copilot_service.fitz.open", return_value=DummyDoc(page_count=4)), patch.object(
        copilot_service,
        "OCR_PDF_BATCH_CONCURRENCY",
        2,
    ), patch(
        "illdashboard.copilot_service._extract_structured_medical_data_pdf_batch",
        new=AsyncMock(side_effect=fake_batch),
    ):
        result = await copilot_service._extract_structured_medical_data_from_pdf("/tmp/report.pdf")

    assert second_batch_started.is_set()
    assert [measurement["page_number"] for measurement in result["measurements"]] == [1, 3]


@pytest.mark.asyncio
async def test_normalize_marker_names_splits_large_batches():
    responses = [
        '{"Marker 1": "Canonical 1", "Marker 2": "Canonical 2"}',
        '{"Marker 3": "Canonical 3"}',
    ]

    with patch.object(copilot_service, "MARKER_NORMALIZATION_BATCH_SIZE", 2), patch.object(
        copilot_service,
        "MARKER_NORMALIZATION_CONCURRENCY",
        1,
    ), patch("illdashboard.copilot_service._ask", new=AsyncMock(side_effect=responses)) as ask_mock:
        result = await copilot_service.normalize_marker_names(
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
async def test_choose_canonical_units_splits_large_batches():
    responses = [
        json.dumps(
            {
                "Marker 1": {"canonical_unit": "10^9/L"},
                "Marker 2": {"canonical_unit": "mmol/L"},
            }
        ),
        json.dumps(
            {
                "Marker 3": {"canonical_unit": "g/L"}
            }
        ),
    ]

    marker_groups = [
        copilot_service.MarkerUnitGroup(
            marker_name="Marker 1",
            observations=[
                copilot_service.MarkerObservation(
                    id="0",
                    value=0.38,
                    unit="10^9/L",
                    reference_low=None,
                    reference_high=None,
                )
            ],
        ),
        copilot_service.MarkerUnitGroup(
            marker_name="Marker 2",
            existing_canonical_unit="mmol/L",
            observations=[
                copilot_service.MarkerObservation(
                    id="1",
                    value=75.6,
                    unit="mg/dL",
                    reference_low=63.0,
                    reference_high=91.8,
                )
            ],
        ),
        copilot_service.MarkerUnitGroup(
            marker_name="Marker 3",
            existing_canonical_unit="g/L",
            observations=[
                copilot_service.MarkerObservation(
                    id="2",
                    value=15.6,
                    unit="g/dL",
                    reference_low=13.5,
                    reference_high=17.5,
                )
            ],
        ),
    ]

    with patch.object(copilot_service, "UNIT_NORMALIZATION_BATCH_SIZE", 2), patch.object(
        copilot_service,
        "UNIT_NORMALIZATION_CONCURRENCY",
        1,
    ), patch("illdashboard.copilot_service._ask", new=AsyncMock(side_effect=responses)) as ask_mock:
        result = await copilot_service.choose_canonical_units(marker_groups)

    assert result["Marker 1"] == "10^9/L"
    assert result["Marker 2"] == "mmol/L"
    assert result["Marker 3"] == "g/L"
    assert ask_mock.await_count == 1


@pytest.mark.asyncio
async def test_normalize_qualitative_values_splits_large_batches():
    responses = [
        json.dumps(
            {
                "negative": {"canonical_value": "negative", "boolean_value": False},
                "true": {"canonical_value": "positive", "boolean_value": True},
            }
        ),
        json.dumps(
            {
                "reaktivni": {"canonical_value": "reactive", "boolean_value": True}
            }
        ),
    ]

    requests = [
        copilot_service.QualitativeNormalizationRequest(
            id="negative",
            marker_name="Chlamydia psittaci IgG",
            original_value="negative",
        ),
        copilot_service.QualitativeNormalizationRequest(
            id="true",
            marker_name="Varicella-zoster IgG",
            original_value="true",
        ),
        copilot_service.QualitativeNormalizationRequest(
            id="reaktivni",
            marker_name="EBV VCA IgM",
            original_value="reaktívní",
        ),
    ]

    with patch.object(copilot_service, "QUALITATIVE_NORMALIZATION_BATCH_SIZE", 2), patch.object(
        copilot_service,
        "QUALITATIVE_NORMALIZATION_CONCURRENCY",
        1,
    ), patch("illdashboard.copilot_service._ask", new=AsyncMock(side_effect=responses)) as ask_mock:
        result = await copilot_service.normalize_qualitative_values(requests, [])

    assert result == {
        "negative": ("negative", False),
        "true": ("positive", True),
        "reaktivni": ("reactive", True),
    }
    assert ask_mock.await_count == 2


@pytest.mark.asyncio
async def test_normalize_qualitative_values_includes_existing_canonical_values_in_prompt():
    requests = [
        copilot_service.QualitativeNormalizationRequest(
            id="negative",
            marker_name="Chlamydia psittaci IgG",
            original_value="NEGATIVE",
        )
    ]

    response = json.dumps({"negative": {"canonical_value": "negative", "boolean_value": False}})

    with patch("illdashboard.copilot_service._ask", new=AsyncMock(return_value=response)) as ask_mock:
        result = await copilot_service.normalize_qualitative_values(requests, ["negative"])

    assert result == {"negative": ("negative", False)}
    ask_mock.assert_awaited_once()
    assert ask_mock.await_args is not None
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
        json.dumps(
            {
                "req-3": {"scale_factor": 10}
            }
        ),
    ]

    conversion_requests = [
        copilot_service.UnitConversionRequest(
            id="req-1",
            marker_name="Marker 1",
            original_unit="cells/µL",
            canonical_unit="10^9/L",
            example_value=380,
            reference_low=None,
            reference_high=None,
        ),
        copilot_service.UnitConversionRequest(
            id="req-2",
            marker_name="Marker 2",
            original_unit="mmol/l",
            canonical_unit="mmol/L",
            example_value=4.2,
            reference_low=3.5,
            reference_high=5.1,
        ),
        copilot_service.UnitConversionRequest(
            id="req-3",
            marker_name="Marker 3",
            original_unit="g/dL",
            canonical_unit="g/L",
            example_value=15.6,
            reference_low=13.5,
            reference_high=17.5,
        ),
    ]

    with patch.object(copilot_service, "UNIT_NORMALIZATION_BATCH_SIZE", 2), patch.object(
        copilot_service,
        "UNIT_NORMALIZATION_CONCURRENCY",
        1,
    ), patch("illdashboard.copilot_service._ask", new=AsyncMock(side_effect=responses)) as ask_mock:
        result = await copilot_service.infer_rescaling_factors(conversion_requests)

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

    with patch("illdashboard.copilot_service._get_client", new=AsyncMock(return_value=client)), patch(
        "illdashboard.copilot_service.add_premium_requests"
    ) as add_mock:
        result = await copilot_service._ask("system", "user")

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

    with patch("illdashboard.copilot_service._get_client", new=AsyncMock(return_value=client)), patch(
        "illdashboard.copilot_service.add_premium_requests"
    ) as add_mock:
        with pytest.raises(RuntimeError, match="boom"):
            await copilot_service._ask("system", "user")

    add_mock.assert_called_once_with(1.0)
    session.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_normalize_marker_names_prompt_prefers_english_for_czech_labels():
    response = '{"Leukocyty": "White Blood Cell (WBC) Count"}'

    with patch("illdashboard.copilot_service._ask", new=AsyncMock(return_value=response)) as ask_mock:
        result = await copilot_service.normalize_marker_names(["Leukocyty"], ["Hemoglobin"])

    assert result == {"Leukocyty": "White Blood Cell (WBC) Count"}
    assert ask_mock.await_count == 1

    assert ask_mock.await_args is not None
    system_prompt, user_prompt = ask_mock.await_args.args
    assert "including Czech, prefer the English canonical medical name" in system_prompt
    assert "If multiple NEW marker names refer to the same biomarker" in system_prompt
    assert "Ignore specimen prefixes, analyzer noise, sample annotations" in system_prompt
    assert "When a label includes a standard lab abbreviation such as MCHC" in system_prompt
    assert 'it often means "Absorbance", not "Absolute"' in system_prompt
    assert '"Lymfocyty -abs.počet": "Absolute Lymphocyte Count"' in system_prompt
    assert "NEW marker names to normalize:\n- Leukocyty\n" in user_prompt


@pytest.mark.asyncio
async def test_normalize_marker_names_reuses_existing_alias_key_without_llm():
    with patch("illdashboard.copilot_service._ask", new=AsyncMock()) as ask_mock:
        result = await copilot_service.normalize_marker_names(
            ["Sodium [Serum]", "Sodium"],
            ["Sodium"],
        )

    assert result == {
        "Sodium [Serum]": "Sodium",
        "Sodium": "Sodium",
    }
    ask_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_choose_canonical_units_prompt_mentions_count_conversion_example():
    response = json.dumps(
        {
            "Absolute CD4+ T-Helper Cell Count": {"canonical_unit": "10^9/L"}
        }
    )

    with patch("illdashboard.copilot_service._ask", new=AsyncMock(return_value=response)) as ask_mock:
        result = await copilot_service.choose_canonical_units(
            [
                copilot_service.MarkerUnitGroup(
                    marker_name="Absolute CD4+ T-Helper Cell Count",
                    observations=[
                        copilot_service.MarkerObservation(
                            id="0",
                            value=380,
                            unit="Zellen/µl",
                            reference_low=None,
                            reference_high=None,
                        )
                    ],
                )
            ]
        )

    assert result["Absolute CD4+ T-Helper Cell Count"] == "10^9/L"

    assert ask_mock.await_args is not None
    system_prompt, user_prompt = ask_mock.await_args.args
    assert "Prefer language-neutral, internationally recognizable units such as 10^9/L" in system_prompt
    assert "prefer 10^9/L over Zellen/µl, cells/µL, tys./µl, or tis./ul" in system_prompt
    assert "380 /µL, 380 cells/µL, and 0.38 10^9/L should canonicalize to 0.38 10^9/L" in system_prompt
    assert "Marker: Absolute CD4+ T-Helper Cell Count" in user_prompt
    assert "value=380; unit=Zellen/µl" in user_prompt


@pytest.mark.asyncio
async def test_choose_canonical_units_skips_homogeneous_units_without_llm():
    with patch("illdashboard.copilot_service._ask", new=AsyncMock()) as ask_mock:
        result = await copilot_service.choose_canonical_units(
            [
                copilot_service.MarkerUnitGroup(
                    marker_name="Sodium",
                    existing_canonical_unit="mmol/L",
                    observations=[
                        copilot_service.MarkerObservation(
                            id="0",
                            value=141,
                            unit="mmol/l",
                            reference_low=136,
                            reference_high=145,
                        ),
                        copilot_service.MarkerObservation(
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
async def test_infer_rescaling_factors_prompt_mentions_count_conversion_example():
    response = json.dumps({"zellen/ul=>10^9/l": {"scale_factor": 0.001}})

    with patch("illdashboard.copilot_service._ask", new=AsyncMock(return_value=response)) as ask_mock:
        result = await copilot_service.infer_rescaling_factors(
            [
                copilot_service.UnitConversionRequest(
                    id="zellen/ul=>10^9/l",
                    marker_name="Absolute CD4+ T-Helper Cell Count",
                    original_unit="Zellen/µl",
                    canonical_unit="10^9/L",
                    example_value=380,
                    reference_low=440,
                    reference_high=2160,
                )
            ]
        )

    assert result == {"zellen/ul=>10^9/l": 0.001}

    assert ask_mock.await_args is not None
    system_prompt, user_prompt = ask_mock.await_args.args
    assert "converting /µL to 10^9/L uses a factor of 0.001" in system_prompt
    assert "id=zellen/ul=>10^9/l" in user_prompt
    assert "original_unit=Zellen/µl" in user_prompt
    assert "canonical_unit=10^9/L" in user_prompt


@pytest.mark.asyncio
async def test_normalize_qualitative_values_prompt_mentions_boolean_context():
    response = json.dumps({"true": {"canonical_value": "positive", "boolean_value": True}})

    with patch("illdashboard.copilot_service._ask", new=AsyncMock(return_value=response)) as ask_mock:
        result = await copilot_service.normalize_qualitative_values(
            [
                copilot_service.QualitativeNormalizationRequest(
                    id="true",
                    marker_name="Varicella-zoster IgG",
                    original_value="true",
                )
            ],
            [],
        )

    assert result == {"true": ("positive", True)}

    assert ask_mock.await_args is not None
    system_prompt, user_prompt = ask_mock.await_args.args
    assert "literal boolean-like value such as \"true\" or \"false\"" in system_prompt
    assert "Use true for outcomes like positive, reactive, or detected" in system_prompt
    assert "Translate non-English qualitative result words to concise English" in system_prompt
    assert '"canonical_value": string or null' in system_prompt
    assert '"boolean_value": boolean or null' in system_prompt
    assert "id=true; marker=Varicella-zoster IgG; original_value=true" in user_prompt


@pytest.mark.asyncio
async def test_extract_structured_medical_data_pdf_range_logs_context_on_non_retryable_failure():
    with patch(
        "illdashboard.copilot_service._extract_structured_medical_data_pdf_batch",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ), patch("illdashboard.copilot_service.logger") as logger_mock:
        with pytest.raises(RuntimeError, match="boom"):
            await copilot_service._extract_structured_medical_data_pdf_range(
                "/tmp/2023-2-immunology.pdf",
                start_page=0,
                stop_page=2,
                dpi=144,
                filename="2023-2-immunology.pdf",
            )

    logger_mock.exception.assert_called_once_with(
        "%s extraction failed for %s (filename=%s, pages=%s-%s, dpi=%s)",
        "Structured medical PDF",
        "/tmp/2023-2-immunology.pdf",
        "2023-2-immunology.pdf",
        1,
        2,
        144,
    )


@pytest.mark.asyncio
async def test_explain_marker_history_prompt_avoids_generic_caution_and_trend_filler():
    with patch("illdashboard.copilot_service._ask", new=AsyncMock(return_value="ok")) as ask_mock:
        result = await copilot_service.explain_marker_history(
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

    assert ask_mock.await_args is not None
    system_prompt, user_prompt = ask_mock.await_args.args
    assert "Do not add a generic caution or disclaimer section" in system_prompt
    assert "do not dwell on the lack of a trend" in system_prompt
    assert "explicitly explain in plain language what being below or above the limit means" in system_prompt
    assert "Please explain the history of Potassium." in user_prompt