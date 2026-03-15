from collections.abc import Callable
from types import SimpleNamespace
from unittest.mock import AsyncMock, call, patch

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
async def test_ocr_extract_pdf_splits_oversized_batches_and_preserves_page_numbers():
    async def fake_batch(
        pdf_path: str,
        *,
        start_page: int,
        stop_page: int,
        dpi: int,
        filename: str | None = None,
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
        "illdashboard.copilot_service._ocr_extract_pdf_batch",
        new=AsyncMock(side_effect=fake_batch),
    ) as batch_mock:
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
    assert batch_mock.await_args_list == [
        call("/tmp/report.pdf", start_page=0, stop_page=2, dpi=144, filename=None),
        call("/tmp/report.pdf", start_page=0, stop_page=1, dpi=144, filename=None),
        call("/tmp/report.pdf", start_page=1, stop_page=2, dpi=144, filename=None),
        call("/tmp/report.pdf", start_page=2, stop_page=4, dpi=144, filename=None),
        call("/tmp/report.pdf", start_page=2, stop_page=3, dpi=144, filename=None),
        call("/tmp/report.pdf", start_page=3, stop_page=4, dpi=144, filename=None),
    ]


@pytest.mark.asyncio
async def test_ocr_extract_pdf_splits_timed_out_batches_and_preserves_page_numbers():
    async def fake_batch(
        pdf_path: str,
        *,
        start_page: int,
        stop_page: int,
        dpi: int,
        filename: str | None = None,
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
        "illdashboard.copilot_service._ocr_extract_pdf_batch",
        new=AsyncMock(side_effect=fake_batch),
    ) as batch_mock:
        result = await copilot_service.ocr_extract("/tmp/report.pdf")

    assert result["lab_date"] == "2025-09-05"
    assert result["source"] == "synlab"
    assert [measurement["page_number"] for measurement in result["measurements"]] == [1, 2]
    assert [measurement["marker_name"] for measurement in result["measurements"]] == [
        "Marker 1",
        "Marker 2",
    ]
    assert batch_mock.await_args_list == [
        call("/tmp/report.pdf", start_page=0, stop_page=2, dpi=144, filename=None),
        call("/tmp/report.pdf", start_page=0, stop_page=1, dpi=144, filename=None),
        call("/tmp/report.pdf", start_page=1, stop_page=2, dpi=144, filename=None),
    ]


@pytest.mark.asyncio
async def test_ocr_extract_pdf_falls_back_to_single_pages_after_429():
    async def fake_batch(
        pdf_path: str,
        *,
        start_page: int,
        stop_page: int,
        dpi: int,
        filename: str | None = None,
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
        "illdashboard.copilot_service._ocr_extract_pdf_batch",
        new=AsyncMock(side_effect=fake_batch),
    ) as batch_mock:
        result = await copilot_service.ocr_extract("/tmp/report.pdf")

    assert result["lab_date"] == "2025-09-05"
    assert result["source"] == "synlab"
    assert [measurement["page_number"] for measurement in result["measurements"]] == [1, 2]
    assert batch_mock.await_args_list == [
        call("/tmp/report.pdf", start_page=0, stop_page=2, dpi=144, filename=None),
        call("/tmp/report.pdf", start_page=0, stop_page=1, dpi=144, filename=None),
        call("/tmp/report.pdf", start_page=1, stop_page=2, dpi=144, filename=None),
    ]


@pytest.mark.asyncio
async def test_ocr_extract_pdf_retries_single_page_at_lower_dpi_after_413():
    async def fake_batch(
        pdf_path: str,
        *,
        start_page: int,
        stop_page: int,
        dpi: int,
        filename: str | None = None,
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
        "illdashboard.copilot_service._ocr_extract_pdf_batch",
        new=AsyncMock(side_effect=fake_batch),
    ) as batch_mock:
        result = await copilot_service.ocr_extract("/tmp/report.pdf")

    assert result == {
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
    assert batch_mock.await_args_list == [
        call("/tmp/report.pdf", start_page=0, stop_page=1, dpi=144, filename=None),
        call("/tmp/report.pdf", start_page=0, stop_page=1, dpi=120, filename=None),
    ]


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

    system_prompt, user_prompt = ask_mock.await_args.args
    assert "including Czech, prefer the English canonical medical name" in system_prompt
    assert 'it often means "Absorbance", not "Absolute"' in system_prompt
    assert '"Lymfocyty -abs.počet": "Absolute Lymphocyte Count"' in system_prompt
    assert "NEW marker names to normalize:\n- Leukocyty\n" in user_prompt


@pytest.mark.asyncio
async def test_ocr_extract_pdf_range_logs_context_on_non_retryable_failure():
    with patch(
        "illdashboard.copilot_service._ocr_extract_pdf_batch",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ), patch("illdashboard.copilot_service.logger") as logger_mock:
        with pytest.raises(RuntimeError, match="boom"):
            await copilot_service._ocr_extract_pdf_range(
                "/tmp/2023-2-immunology.pdf",
                start_page=0,
                stop_page=2,
                dpi=144,
                filename="2023-2-immunology.pdf",
            )

    logger_mock.exception.assert_called_once_with(
        "%s extraction failed for %s (filename=%s, pages=%s-%s, dpi=%s)",
        "OCR PDF",
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

    system_prompt, user_prompt = ask_mock.await_args.args
    assert "Do not add a generic caution or disclaimer section" in system_prompt
    assert "do not dwell on the lack of a trend" in system_prompt
    assert "explicitly explain in plain language what being below or above the limit means" in system_prompt
    assert "Please explain the history of Potassium." in user_prompt