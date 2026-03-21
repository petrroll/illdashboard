import asyncio
import base64
import json
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import fitz
import pytest

from illdashboard.copilot import client as copilot_client
from illdashboard.copilot import explanations as copilot_explanations
from illdashboard.copilot import extraction as copilot_ocr
from illdashboard.copilot import mistral_client
from illdashboard.copilot import normalization as copilot_normalization
from illdashboard.services import pipeline


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


class BlockingSession(DummySession):
    def __init__(self, release_event: asyncio.Event):
        super().__init__(response=SimpleNamespace(data=SimpleNamespace(content="ok")))
        self._release_event = release_event

    async def send_and_wait(self, *_args, **_kwargs):
        await self._release_event.wait()
        return await super().send_and_wait(*_args, **_kwargs)


class WarningSession(DummySession):
    async def send_and_wait(self, *_args, **_kwargs):
        if self._handler is not None:
            self._handler(
                SimpleNamespace(
                    type=copilot_client.SessionEventType.SESSION_WARNING,
                    data=SimpleNamespace(
                        message="rate limited",
                        warning_type="rate_limit",
                        status_code=429,
                        reason="too_many_requests",
                        error_reason=None,
                    ),
                )
            )
        return await super().send_and_wait(*_args, **_kwargs)


class DummyDoc:
    def __init__(self, page_count: int):
        self.page_count = page_count

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyAsyncMistralClient:
    def __init__(self, *, ocr_response=None, chat_response=None):
        self.ocr = SimpleNamespace(process_async=AsyncMock(return_value=ocr_response))
        self.chat = SimpleNamespace(complete_async=AsyncMock(return_value=chat_response))

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.fixture(autouse=True)
def _no_retry_delay():
    with patch.object(copilot_ocr, "OCR_RETRY_DELAY", 0):
        yield


def test_ocr_and_normalization_share_the_longer_request_budget():
    assert copilot_client.COPILOT_REQUEST_TIMEOUT == 900
    assert copilot_ocr.OCR_ASK_TIMEOUT == copilot_client.COPILOT_REQUEST_TIMEOUT
    assert copilot_normalization.NORMALIZATION_ASK_TIMEOUT == copilot_client.COPILOT_REQUEST_TIMEOUT
    assert copilot_normalization.MARKER_NORMALIZATION_BATCH_SIZE == 100
    assert pipeline.JOB_LEASE_SECONDS == copilot_client.COPILOT_REQUEST_TIMEOUT


def test_normalization_requests_use_separate_serialized_lanes():
    assert copilot_client._request_lane_name("structured_medical_extraction") == "extraction"
    assert copilot_client._request_lane_name("medical_summary") == "summary"
    assert copilot_client._request_lane_name("normalize_source_name") == "normalize_source_name"
    assert copilot_client._request_lane_name("normalize_qualitative_values") == "normalize_qualitative_values"
    assert copilot_client._request_lane_limit("normalize_source_name") == 1
    assert copilot_client._request_lane_limit("normalize_qualitative_values") == 1
    assert copilot_client._request_lane_limit("classify_marker_groups") == 1


def _medical_calls(mock) -> list:
    return [args for args in mock.await_args_list if args.args[1] is copilot_ocr._MEDICAL_EXTRACTION]


async def _wait_for(condition, *, timeout: float = 1.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("timed out waiting for condition")
        await asyncio.sleep(0.01)


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

    with (
        patch("illdashboard.copilot.extraction.fitz.open", return_value=DummyDoc(page_count=4)),
        patch.object(copilot_ocr, "OCR_PDF_BATCH_SIZE", 2),
        patch.object(copilot_ocr, "OCR_PDF_MAX_BATCH_SIZE", 2),
        patch(
            "illdashboard.copilot.extraction._pdf_batch_extract",
            new=AsyncMock(side_effect=fake_batch),
        ) as batch_mock,
        patch(
            "illdashboard.copilot.extraction.extract_text",
            new=AsyncMock(return_value={"raw_text": "text", "translated_text_english": "text"}),
        ),
        patch("illdashboard.copilot.extraction.generate_summary", new=AsyncMock(return_value=None)),
    ):
        result = await copilot_ocr.ocr_extract("/tmp/report.pdf")

    assert result["lab_date"] == "2025-09-05"
    assert result["source"] == "synlab"
    assert [measurement["page_number"] for measurement in result["measurements"]] == [1, 2, 3, 4]
    observed_calls = sorted(
        [
            (
                args.args[0],
                args.kwargs["start_page"],
                args.kwargs["stop_page"],
                args.kwargs["dpi"],
                args.kwargs["filename"],
            )
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

    with (
        patch("illdashboard.copilot.extraction.fitz.open", return_value=DummyDoc(page_count=1)),
        patch(
            "illdashboard.copilot.extraction._pdf_batch_extract",
            new=AsyncMock(side_effect=fake_batch),
        ) as batch_mock,
        patch(
            "illdashboard.copilot.extraction.extract_text",
            new=AsyncMock(
                return_value={
                    "raw_text": "Sodium 141 mmol/l",
                    "translated_text_english": "Sodium 141 mmol/l",
                }
            ),
        ),
        patch("illdashboard.copilot.extraction.generate_summary", new=AsyncMock(return_value=None)),
    ):
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
    assert [
        (
            call.args[0],
            call.kwargs["start_page"],
            call.kwargs["stop_page"],
            call.kwargs["dpi"],
            call.kwargs["filename"],
        )
        for call in medical
    ] == [
        ("/tmp/report.pdf", 0, 1, 144, None),
        ("/tmp/report.pdf", 0, 1, 72, None),
    ]


@pytest.mark.asyncio
async def test_extract_measurement_batch_retries_image_at_lower_dpi_after_413():
    async def fake_image_extract(
        image_path: str,
        kind: copilot_ocr._ExtractionKind,
        *,
        dpi: int,
        filename: str | None = None,
        render_cache=None,
    ):
        assert image_path == "/tmp/report.png"
        assert kind is copilot_ocr._MEDICAL_EXTRACTION
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

    with patch(
        "illdashboard.copilot.extraction._image_extract",
        new=AsyncMock(side_effect=fake_image_extract),
    ) as image_mock:
        result = await copilot_ocr.extract_measurement_batch(
            "/tmp/report.png",
            start_page=0,
            stop_page=1,
            dpi=144,
        )

    assert result["measurements"][0]["page_number"] == 1
    assert [
        (
            call.args[0],
            call.kwargs["dpi"],
            call.kwargs["filename"],
        )
        for call in image_mock.await_args_list
    ] == [
        ("/tmp/report.png", 144, None),
        ("/tmp/report.png", 72, None),
    ]


def test_pdf_to_images_caps_render_to_a4_pixels(tmp_path):
    pdf_path = tmp_path / "oversized.pdf"
    document = fitz.open()
    document.new_page(width=1200, height=1600)
    document.save(pdf_path)
    document.close()

    paths = copilot_ocr._pdf_to_images(str(pdf_path), dpi=copilot_ocr.OCR_PDF_RENDER_DPI)
    try:
        assert len(paths) == 1
        rendered = fitz.Pixmap(paths[0])
        max_width, max_height = copilot_ocr._a4_pixel_bounds(dpi=copilot_ocr.OCR_PDF_RENDER_DPI, landscape=False)
        assert rendered.width <= max_width
        assert rendered.height <= max_height
        assert abs((rendered.width / rendered.height) - (1200 / 1600)) < 0.02
    finally:
        for path in paths:
            Path(path).unlink(missing_ok=True)


def test_image_to_png_caps_render_to_a4_pixels(tmp_path):
    image_path = tmp_path / "oversized.png"
    pixmap = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 3000, 2000), False)
    pixmap.clear_with(200)
    pixmap.save(image_path)

    rendered_path = copilot_ocr._image_to_png(str(image_path), dpi=copilot_ocr.OCR_PDF_RENDER_DPI)
    try:
        rendered = fitz.Pixmap(rendered_path)
        max_width, max_height = copilot_ocr._a4_pixel_bounds(dpi=copilot_ocr.OCR_PDF_RENDER_DPI, landscape=True)
        assert rendered.width <= max_width
        assert rendered.height <= max_height
        assert abs((rendered.width / rendered.height) - (3000 / 2000)) < 0.02
    finally:
        Path(rendered_path).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_extract_measurement_batch_uses_retrying_pdf_range_path():
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
        assert pdf_path == "/tmp/report.pdf"
        assert kind is copilot_ocr._MEDICAL_EXTRACTION
        if stop_page - start_page > 1:
            raise Exception("CAPIError: 413 failed to parse request")
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

    with patch(
        "illdashboard.copilot.extraction._pdf_batch_extract",
        new=AsyncMock(side_effect=fake_batch),
    ) as batch_mock:
        result = await copilot_ocr.extract_measurement_batch(
            "/tmp/report.pdf",
            start_page=2,
            stop_page=4,
            dpi=144,
        )

    assert [measurement["page_number"] for measurement in result["measurements"]] == [3, 4]
    assert [
        (
            call.args[0],
            call.kwargs["start_page"],
            call.kwargs["stop_page"],
            call.kwargs["dpi"],
        )
        for call in _medical_calls(batch_mock)
    ] == [
        ("/tmp/report.pdf", 2, 4, 144),
        ("/tmp/report.pdf", 2, 3, 144),
        ("/tmp/report.pdf", 3, 4, 144),
    ]


@pytest.mark.asyncio
async def test_extract_text_batch_uses_retrying_pdf_range_path():
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
        assert pdf_path == "/tmp/report.pdf"
        assert kind is copilot_ocr._TEXT_EXTRACTION
        if stop_page - start_page > 1:
            raise Exception("CAPIError: 413 failed to parse request")
        page_number = start_page + 1
        return {
            "raw_text": f"raw page {page_number}",
            "translated_text_english": f"translated page {page_number}",
        }

    with patch(
        "illdashboard.copilot.extraction._pdf_batch_extract",
        new=AsyncMock(side_effect=fake_batch),
    ) as batch_mock:
        result = await copilot_ocr.extract_text_batch(
            "/tmp/report.pdf",
            start_page=0,
            stop_page=2,
            dpi=144,
        )

    assert result == {
        "raw_text": "raw page 1\n\nraw page 2",
        "translated_text_english": "translated page 1\n\ntranslated page 2",
    }
    assert [
        (
            call.args[0],
            call.kwargs["start_page"],
            call.kwargs["stop_page"],
            call.kwargs["dpi"],
        )
        for call in batch_mock.await_args_list
    ] == [
        ("/tmp/report.pdf", 0, 2, 144),
        ("/tmp/report.pdf", 0, 1, 144),
        ("/tmp/report.pdf", 1, 2, 144),
    ]


@pytest.mark.asyncio
async def test_extract_measurement_batch_uses_direct_mistral_file_path_when_configured():
    with (
        patch.object(copilot_ocr.settings, "EXTRACTION_PROVIDER", "mistral"),
        patch(
            "illdashboard.copilot.extraction._extract_structured_medical_data_from_file_mistral",
            new=AsyncMock(
                return_value={
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
            ),
        ) as extract_mock,
    ):
        result = await copilot_ocr.extract_measurement_batch(
            "/tmp/report.pdf",
            start_page=0,
            stop_page=1,
            dpi=144,
        )

    assert result["measurements"][0]["marker_name"] == "Sodium"
    extract_mock.assert_awaited_once_with(
        "/tmp/report.pdf",
        start_page=0,
        stop_page=1,
        filename=None,
        request_context="document:p1-1",
    )


@pytest.mark.asyncio
async def test_extract_text_batch_uses_direct_mistral_file_path_when_configured():
    with (
        patch.object(copilot_ocr.settings, "EXTRACTION_PROVIDER", "mistral"),
        patch(
            "illdashboard.copilot.extraction._extract_document_text_from_file_mistral",
            new=AsyncMock(return_value={"raw_text": "rohtext", "translated_text_english": "raw text"}),
        ) as extract_mock,
    ):
        result = await copilot_ocr.extract_text_batch(
            "/tmp/report.pdf",
            start_page=0,
            stop_page=1,
            dpi=144,
        )

    assert result == {
        "raw_text": "rohtext",
        "translated_text_english": "raw text",
    }
    extract_mock.assert_awaited_once_with(
        "/tmp/report.pdf",
        start_page=0,
        stop_page=1,
        filename=None,
        request_context="document:p1-1",
    )


@pytest.mark.asyncio
async def test_mistral_process_ocr_file_slices_requested_pdf_range(tmp_path):
    pdf_path = tmp_path / "report.pdf"
    document = fitz.open()
    for _ in range(3):
        document.new_page(width=200, height=200)
    document.save(pdf_path)
    document.close()

    sdk_client = DummyAsyncMistralClient(ocr_response={"pages": []})
    with patch(
        "illdashboard.copilot.mistral_client._sdk_client",
        return_value=sdk_client,
    ):
        result = await mistral_client.process_ocr_file(
            str(pdf_path),
            start_page=1,
            stop_page=3,
            request_name="structured_medical_extraction",
        )

    assert result == {"pages": []}
    document_payload = sdk_client.ocr.process_async.await_args.kwargs["document"]
    assert document_payload["type"] == "document_url"
    encoded_pdf = document_payload["document_url"].split(",", 1)[1]
    sliced_pdf = fitz.open(stream=base64.b64decode(encoded_pdf), filetype="pdf")
    try:
        assert sliced_pdf.page_count == 2
    finally:
        sliced_pdf.close()


@pytest.mark.asyncio
async def test_mistral_process_ocr_file_uses_image_url_for_images(tmp_path):
    image_path = tmp_path / "page.png"
    pixmap = fitz.Pixmap(fitz.csGRAY, fitz.IRect(0, 0, 16, 16), False)
    pixmap.clear_with(200)
    pixmap.save(image_path)

    sdk_client = DummyAsyncMistralClient(ocr_response={"pages": []})
    with patch(
        "illdashboard.copilot.mistral_client._sdk_client",
        return_value=sdk_client,
    ):
        await mistral_client.process_ocr_file(
            str(image_path),
            request_name="document_text_extraction",
        )

    document_payload = sdk_client.ocr.process_async.await_args.kwargs["document"]
    assert document_payload["type"] == "image_url"
    assert document_payload["image_url"].startswith("data:image/png;base64,")


def test_mistral_document_annotation_falls_back_to_page_annotations():
    result = {
        "pages": [
            {
                "index": 1,
                "annotations": {
                    "lab_date": None,
                    "source": "Example Lab",
                    "measurements": [],
                },
            }
        ]
    }

    assert mistral_client.document_annotation(result)["source"] == "Example Lab"


@pytest.mark.asyncio
async def test_normalization_ask_json_routes_to_mistral_when_configured():
    with (
        patch.object(copilot_normalization.settings, "NORMALIZATION_PROVIDER", "mistral"),
        patch(
            "illdashboard.copilot.normalization.mistral_client._ask_json",
            new=AsyncMock(return_value={"ok": True}),
        ) as ask_mock,
    ):
        result = await copilot_normalization._ask_json("system", "user", request_name="normalize_marker_names")

    assert result == {"ok": True}
    ask_mock.assert_awaited_once()


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

    with (
        patch("illdashboard.copilot.extraction.fitz.open", return_value=DummyDoc(page_count=4)),
        patch.object(copilot_ocr, "OCR_PDF_BATCH_SIZE", 2),
        patch.object(copilot_ocr, "OCR_PDF_MAX_BATCH_SIZE", 2),
        patch.object(
            copilot_ocr,
            "OCR_PDF_BATCH_CONCURRENCY",
            2,
        ),
        patch(
            "illdashboard.copilot.extraction._pdf_batch_extract",
            new=AsyncMock(side_effect=fake_batch),
        ),
    ):
        result = await copilot_ocr._extract_file("/tmp/report.pdf", copilot_ocr._MEDICAL_EXTRACTION)

    assert second_batch_started.is_set()
    assert [measurement["page_number"] for measurement in result["measurements"]] == [1, 3]


@pytest.mark.asyncio
async def test_ocr_extract_streams_medical_batches_before_combining_result():
    observed_batches: list[tuple[int, list[int]]] = []

    async def fake_extract_batches(
        file_path: str,
        kind: copilot_ocr._ExtractionKind,
        *,
        filename: str | None = None,
        render_cache=None,
    ):
        assert file_path == "/tmp/report.pdf"
        assert kind is copilot_ocr._MEDICAL_EXTRACTION
        yield copilot_ocr._ExtractionBatch(
            batch_index=1,
            start_page=2,
            stop_page=4,
            result={
                "lab_date": None,
                "source": None,
                "measurements": [
                    {
                        "marker_name": "Marker 2",
                        "value": 2,
                        "unit": "mmol/l",
                        "reference_low": None,
                        "reference_high": None,
                        "measured_at": None,
                        "page_number": 3,
                    }
                ],
            },
        )
        yield copilot_ocr._ExtractionBatch(
            batch_index=0,
            start_page=0,
            stop_page=2,
            result={
                "lab_date": "2025-09-05",
                "source": "synlab",
                "measurements": [
                    {
                        "marker_name": "Marker 1",
                        "value": 1,
                        "unit": "mmol/l",
                        "reference_low": None,
                        "reference_high": None,
                        "measured_at": None,
                        "page_number": 1,
                    }
                ],
            },
        )

    async def fake_extract_text(
        file_path: str,
        *,
        filename: str | None = None,
        render_cache=None,
    ):
        assert file_path == "/tmp/report.pdf"
        return {"raw_text": "text", "translated_text_english": "text"}

    async def on_medical_batch(batch_index: int, batch_result: dict) -> None:
        observed_batches.append(
            (
                batch_index,
                [measurement["page_number"] for measurement in batch_result["measurements"]],
            )
        )

    with (
        patch.object(copilot_ocr, "_extract_file_batches", side_effect=fake_extract_batches),
        patch.object(
            copilot_ocr,
            "extract_text",
            side_effect=fake_extract_text,
        ),
        patch.object(copilot_ocr, "generate_summary", new=AsyncMock(return_value=None)),
    ):
        result = await copilot_ocr.ocr_extract(
            "/tmp/report.pdf",
            on_medical_batch=on_medical_batch,
        )

    assert observed_batches == [(1, [3]), (0, [1])]
    assert result["lab_date"] == "2025-09-05"
    assert result["source"] == "synlab"
    assert [measurement["page_number"] for measurement in result["measurements"]] == [1, 3]


@pytest.mark.asyncio
async def test_ocr_extract_runs_text_ocr_in_parallel_with_medical_extraction():
    medical_started = asyncio.Event()
    text_started = asyncio.Event()
    allow_medical_finish = asyncio.Event()

    async def fake_extract_batches(
        file_path: str,
        kind: copilot_ocr._ExtractionKind,
        *,
        filename: str | None = None,
        render_cache=None,
    ):
        assert file_path == "/tmp/report.pdf"
        assert kind is copilot_ocr._MEDICAL_EXTRACTION
        medical_started.set()
        await text_started.wait()
        await allow_medical_finish.wait()
        yield copilot_ocr._ExtractionBatch(
            batch_index=0,
            start_page=0,
            stop_page=1,
            result={
                "lab_date": None,
                "source": None,
                "measurements": [
                    {
                        "marker_name": "CRP",
                        "value": 15,
                        "unit": "mg/L",
                        "reference_low": 0,
                        "reference_high": 5,
                        "measured_at": None,
                        "page_number": 1,
                    }
                ],
            },
        )

    async def fake_extract_text(
        file_path: str,
        *,
        filename: str | None = None,
        render_cache=None,
    ):
        assert file_path == "/tmp/report.pdf"
        await medical_started.wait()
        text_started.set()
        return {"raw_text": "CRP 15 mg/L", "translated_text_english": "CRP 15 mg/L"}

    with (
        patch.object(copilot_ocr, "_extract_file_batches", side_effect=fake_extract_batches),
        patch.object(copilot_ocr, "extract_text", side_effect=fake_extract_text),
        patch.object(copilot_ocr, "generate_summary", new=AsyncMock(return_value=None)),
    ):
        task = asyncio.create_task(copilot_ocr.ocr_extract("/tmp/report.pdf"))
        await _wait_for(lambda: text_started.is_set())
        allow_medical_finish.set()
        result = await task

    assert result["raw_text"] == "CRP 15 mg/L"
    assert result["translated_text_english"] == "CRP 15 mg/L"
    assert [measurement["marker_name"] for measurement in result["measurements"]] == ["CRP"]


@pytest.mark.asyncio
async def test_normalize_marker_names_splits_large_batches():
    responses = [
        '{"Marker 1": "Canonical 1", "Marker 2": "Canonical 2"}',
        '{"Marker 3": "Canonical 3"}',
    ]

    with (
        patch.object(copilot_normalization, "MARKER_NORMALIZATION_BATCH_SIZE", 2),
        patch.object(
            copilot_normalization,
            "MARKER_NORMALIZATION_CONCURRENCY",
            1,
        ),
        patch("illdashboard.copilot.normalization._ask", new=AsyncMock(side_effect=responses)) as ask_mock,
    ):
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
async def test_normalize_marker_names_reuses_new_canonical_names_across_batches():
    responses = [
        '{"HbA1c": "HbA1c"}',
        '{"Glykovaný hemoglobin": "HbA1c"}',
    ]

    with (
        patch.object(copilot_normalization, "MARKER_NORMALIZATION_BATCH_SIZE", 1),
        patch.object(
            copilot_normalization,
            "MARKER_NORMALIZATION_CONCURRENCY",
            1,
        ),
        patch("illdashboard.copilot.normalization._ask", new=AsyncMock(side_effect=responses)) as ask_mock,
    ):
        result = await copilot_normalization.normalize_marker_names(
            ["HbA1c", "Glykovaný hemoglobin"],
            [],
        )

    assert result == {
        "HbA1c": "HbA1c",
        "Glykovaný hemoglobin": "HbA1c",
    }
    assert ask_mock.await_count == 2
    assert ask_mock.await_args_list[0].args[1].startswith("EXISTING canonical marker names:\n(none yet)")
    assert "\n- HbA1c\n" in ask_mock.await_args_list[1].args[1]


@pytest.mark.asyncio
async def test_normalize_marker_names_includes_raw_examples_and_units_in_prompt():
    with patch(
        "illdashboard.copilot.normalization._ask",
        new=AsyncMock(return_value='{"Lymphozyten gesamt": "Absolute Lymphocyte Count"}'),
    ) as ask_mock:
        result = await copilot_normalization.normalize_marker_names(
            ["Lymphozyten gesamt"],
            ["Lymphocytes", "Absolute Lymphocyte Count"],
            raw_examples_by_name={"Lymphozyten gesamt": ["Lymphozyten gesamt", "Lymphocytes total"]},
            observed_units_by_name={"Lymphozyten gesamt": ["Zellen/µl", "10^9/L"]},
        )

    assert result == {"Lymphozyten gesamt": "Absolute Lymphocyte Count"}
    user_prompt = ask_mock.await_args.args[1]
    assert "Raw examples:" in user_prompt
    assert "Observed units:" in user_prompt
    assert "Lymphocytes total" in user_prompt
    assert "Zellen/µl" in user_prompt


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

    with (
        patch.object(copilot_normalization, "UNIT_NORMALIZATION_BATCH_SIZE", 2),
        patch.object(
            copilot_normalization,
            "UNIT_NORMALIZATION_CONCURRENCY",
            1,
        ),
        patch("illdashboard.copilot.normalization._ask", new=AsyncMock(side_effect=responses)) as ask_mock,
    ):
        result = await copilot_normalization.infer_rescaling_factors(conversion_requests)

    assert result == {
        "req-1": 0.001,
        "req-2": 1.0,
        "req-3": 10.0,
    }
    assert ask_mock.await_count == 2


@pytest.mark.asyncio
async def test_infer_rescaling_factors_handles_dimensionless_ratio_units_without_llm():
    conversion_requests = [
        copilot_normalization.UnitConversionRequest(
            id="ml/l=>%",
            marker_name="Plateletcrit (PCT)",
            original_unit="ml/l",
            canonical_unit="%",
            example_value=1.03,
            reference_low=None,
            reference_high=None,
        )
    ]

    with patch("illdashboard.copilot.normalization._ask", new=AsyncMock()) as ask_mock:
        result = await copilot_normalization.infer_rescaling_factors(conversion_requests)

    assert result == {"ml/l=>%": pytest.approx(0.1)}
    ask_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_ask_adds_observed_premium_usage_cost():
    session = DummySession(
        response=SimpleNamespace(data=SimpleNamespace(content="ok")),
        usage_cost=1.0,
    )
    client = SimpleNamespace(create_session=AsyncMock(return_value=session))

    with (
        patch("illdashboard.copilot.client._get_client", new=AsyncMock(return_value=client)),
        patch("illdashboard.copilot.client.add_premium_requests") as add_mock,
    ):
        result = await copilot_client._ask("system", "user")

    assert result == "ok"
    add_mock.assert_called_once_with(1.0)
    session.disconnect.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("request_name", "expected_model", "expected_reasoning_effort", "supports_reasoning_effort"),
    [
        ("structured_medical_extraction", "measurement-model", None, False),
        ("document_text_extraction", "text-model", None, False),
        ("normalize_marker_names", "normalization-model", "high", True),
        ("medical_summary", "default-model", None, False),
    ],
)
async def test_ask_uses_request_specific_session_settings(
    request_name: str,
    expected_model: str,
    expected_reasoning_effort: str | None,
    supports_reasoning_effort: bool,
):
    session = DummySession(response=SimpleNamespace(data=SimpleNamespace(content="ok")))
    client = SimpleNamespace(
        create_session=AsyncMock(return_value=session),
        list_models=AsyncMock(
            return_value=[
                SimpleNamespace(
                    id="normalization-model",
                    capabilities=SimpleNamespace(
                        supports=SimpleNamespace(reasoning_effort=supports_reasoning_effort)
                    ),
                )
            ]
        ),
    )

    with (
        patch.object(copilot_client.settings, "COPILOT_DEFAULT_MODEL", "default-model"),
        patch.object(copilot_client.settings, "COPILOT_MEASUREMENT_EXTRACTION_MODEL", "measurement-model"),
        patch.object(copilot_client.settings, "COPILOT_MEASUREMENT_EXTRACTION_REASONING_EFFORT", None),
        patch.object(copilot_client.settings, "COPILOT_TEXT_EXTRACTION_MODEL", "text-model"),
        patch.object(copilot_client.settings, "COPILOT_TEXT_EXTRACTION_REASONING_EFFORT", None),
        patch.object(copilot_client.settings, "COPILOT_NORMALIZATION_MODEL", "normalization-model"),
        patch.object(copilot_client.settings, "COPILOT_NORMALIZATION_REASONING_EFFORT", "high"),
        patch("illdashboard.copilot.client._get_client", new=AsyncMock(return_value=client)),
    ):
        result = await copilot_client._ask("system", "user", request_name=request_name)

    assert result == "ok"
    session_config = client.create_session.await_args.args[0]
    assert session_config["model"] == expected_model
    assert session_config.get("reasoning_effort") == expected_reasoning_effort
    if expected_reasoning_effort is None:
        client.list_models.assert_not_awaited()
    else:
        client.list_models.assert_awaited_once()


@pytest.mark.asyncio
async def test_ask_omits_reasoning_effort_for_models_without_support():
    session = DummySession(response=SimpleNamespace(data=SimpleNamespace(content="ok")))
    client = SimpleNamespace(
        create_session=AsyncMock(return_value=session),
        list_models=AsyncMock(
            return_value=[
                SimpleNamespace(
                    id="normalization-model",
                    capabilities=SimpleNamespace(
                        supports=SimpleNamespace(reasoning_effort=False)
                    ),
                )
            ]
        ),
    )

    with (
        patch.object(copilot_client.settings, "COPILOT_NORMALIZATION_MODEL", "normalization-model"),
        patch.object(copilot_client.settings, "COPILOT_NORMALIZATION_REASONING_EFFORT", "high"),
        patch("illdashboard.copilot.client._get_client", new=AsyncMock(return_value=client)),
    ):
        result = await copilot_client._ask("system", "user", request_name="normalize_marker_names")

    assert result == "ok"
    session_config = client.create_session.await_args.args[0]
    assert session_config["model"] == "normalization-model"
    assert "reasoning_effort" not in session_config
    client.list_models.assert_awaited_once()


@pytest.mark.asyncio
async def test_ask_retries_session_create_without_reasoning_when_model_rejects_it():
    session = DummySession(response=SimpleNamespace(data=SimpleNamespace(content="ok")))
    client = SimpleNamespace(
        create_session=AsyncMock(
            side_effect=[
                RuntimeError(
                    "JSON-RPC Error -32603: Request session.create failed with message: "
                    "Model 'gpt-5.4-mini' does not support reasoning effort configuration."
                ),
                session,
            ]
        ),
        list_models=AsyncMock(
            return_value=[
                SimpleNamespace(
                    id="normalization-model",
                    capabilities=SimpleNamespace(
                        supports=SimpleNamespace(reasoning_effort=True)
                    ),
                )
            ]
        ),
    )

    with (
        patch.object(copilot_client.settings, "COPILOT_NORMALIZATION_MODEL", "normalization-model"),
        patch.object(copilot_client.settings, "COPILOT_NORMALIZATION_REASONING_EFFORT", "high"),
        patch("illdashboard.copilot.client._get_client", new=AsyncMock(return_value=client)),
    ):
        result = await copilot_client._ask("system", "user", request_name="normalize_marker_names")

    assert result == "ok"
    assert client.create_session.await_count == 2
    first_config = client.create_session.await_args_list[0].args[0]
    second_config = client.create_session.await_args_list[1].args[0]
    assert first_config["reasoning_effort"] == "high"
    assert "reasoning_effort" not in second_config
    session.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_ask_logs_session_warning_payloads():
    session = WarningSession(response=SimpleNamespace(data=SimpleNamespace(content="ok")))
    client = SimpleNamespace(create_session=AsyncMock(return_value=session))

    with (
        patch("illdashboard.copilot.client._get_client", new=AsyncMock(return_value=client)),
        patch.object(copilot_client.logger, "warning") as warning_mock,
    ):
        result = await copilot_client._ask("system", "user")

    assert result == "ok"
    warning_calls = [
        call for call in warning_mock.call_args_list if call.args and call.args[0].startswith("Copilot session warning")
    ]
    assert warning_calls
    assert "rate_limit" in warning_calls[0].args
    assert 429 in warning_calls[0].args
    assert "rate limited" in warning_calls[0].args
    session.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_ask_logs_heartbeat_while_waiting_for_idle():
    release_event = asyncio.Event()
    client = SimpleNamespace(
        create_session=AsyncMock(side_effect=lambda *_args, **_kwargs: BlockingSession(release_event))
    )

    with (
        patch("illdashboard.copilot.client._get_client", new=AsyncMock(return_value=client)),
        patch.object(copilot_client, "COPILOT_REQUEST_PROGRESS_INTERVAL", 0.01),
        patch.object(copilot_client.logger, "info") as info_mock,
    ):
        task = asyncio.create_task(copilot_client._ask("system", "user"))
        await _wait_for(
            lambda: any(
                call.args and call.args[0].startswith("Copilot request still running")
                for call in info_mock.call_args_list
            )
        )
        release_event.set()
        result = await task

    assert result == "ok"


@pytest.mark.asyncio
async def test_ask_adds_observed_usage_even_when_send_fails():
    session = DummySession(
        send_error=RuntimeError("boom"),
        usage_cost=1.0,
    )
    client = SimpleNamespace(create_session=AsyncMock(return_value=session))

    with (
        patch("illdashboard.copilot.client._get_client", new=AsyncMock(return_value=client)),
        patch("illdashboard.copilot.client.add_premium_requests") as add_mock,
    ):
        with pytest.raises(RuntimeError, match="boom"):
            await copilot_client._ask("system", "user")

    add_mock.assert_called_once_with(1.0)
    session.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_ask_retries_failed_to_list_models_with_fresh_client():
    failing_session = DummySession(
        send_error=Exception("Session error: Execution failed: Error: Failed to list models")
    )
    success_session = DummySession(response=SimpleNamespace(data=SimpleNamespace(content="ok")))
    client = SimpleNamespace(create_session=AsyncMock(side_effect=[failing_session, success_session]))

    with (
        patch("illdashboard.copilot.client._get_client", new=AsyncMock(return_value=client)),
        patch("illdashboard.copilot.client.shutdown_client", new=AsyncMock()) as shutdown_mock,
        patch.object(copilot_client, "COPILOT_TRANSIENT_RETRY_DELAY", 0),
    ):
        result = await copilot_client._ask("system", "user")

    assert result == "ok"
    assert client.create_session.await_count == 2
    shutdown_mock.assert_awaited_once()
    failing_session.disconnect.assert_awaited_once()
    success_session.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_ask_json_adds_strict_json_instructions():
    with patch("illdashboard.copilot.client._ask", new=AsyncMock(return_value='{"ok": true}')) as ask_mock:
        result = await copilot_client._ask_json("system", "user", request_name="document_text_extraction")

    assert result == {"ok": True}
    assert ask_mock.await_args is not None
    assert "Return exactly one valid JSON object" in ask_mock.await_args.args[1]


@pytest.mark.asyncio
async def test_ask_json_repairs_malformed_json_responses():
    with patch(
        "illdashboard.copilot.client._ask",
        new=AsyncMock(side_effect=['{"raw_text": "hello', '{"raw_text": "hello"}']),
    ) as ask_mock:
        result = await copilot_client._ask_json("system", "user", request_name="document_text_extraction")

    assert result == {"raw_text": "hello"}
    assert ask_mock.await_count == 2
    assert ask_mock.await_args_list[1].kwargs["request_name"] == "repair_json_response"
    assert "Malformed response" in ask_mock.await_args_list[1].args[1]


@pytest.mark.asyncio
async def test_ask_allows_distinct_normalization_lanes_while_extraction_is_busy():
    release_event = asyncio.Event()
    client = SimpleNamespace(
        create_session=AsyncMock(side_effect=lambda *_args, **_kwargs: BlockingSession(release_event))
    )

    copilot_client._request_semaphore = None
    copilot_client._request_semaphore_limit = 0
    copilot_client._lane_semaphores.clear()
    copilot_client._lane_semaphore_limits.clear()
    copilot_client._queued_request_count = 0
    copilot_client._active_request_count = 0

    with patch("illdashboard.copilot.client._get_client", new=AsyncMock(return_value=client)):
        extraction_tasks = []
        source_normalization_task = None
        qualitative_normalization_task = None
        blocked_same_lane_task = None
        summary_task = None
        try:
            extraction_tasks = [
                asyncio.create_task(
                    copilot_client._ask("system", "user", request_name="structured_medical_extraction")
                )
                for _ in range(copilot_client.COPILOT_EXTRACTION_CONCURRENCY + 1)
            ]
            await _wait_for(lambda: client.create_session.await_count == copilot_client.COPILOT_EXTRACTION_CONCURRENCY)

            source_normalization_task = asyncio.create_task(
                copilot_client._ask("system", "user", request_name="normalize_source_name")
            )
            qualitative_normalization_task = asyncio.create_task(
                copilot_client._ask("system", "user", request_name="normalize_qualitative_values")
            )
            blocked_same_lane_task = asyncio.create_task(
                copilot_client._ask("system", "user", request_name="normalize_qualitative_values")
            )
            summary_task = asyncio.create_task(copilot_client._ask("system", "user", request_name="medical_summary"))

            await _wait_for(
                lambda: client.create_session.await_count == copilot_client.COPILOT_EXTRACTION_CONCURRENCY + 3
            )
            assert not extraction_tasks[-1].done()
            assert not blocked_same_lane_task.done()
        finally:
            release_event.set()

        results = await asyncio.gather(
            *extraction_tasks,
            *(
                task
                for task in [
                    source_normalization_task,
                    qualitative_normalization_task,
                    blocked_same_lane_task,
                    summary_task,
                ]
                if task is not None
            ),
        )

    assert results == ["ok"] * (copilot_client.COPILOT_EXTRACTION_CONCURRENCY + 5)


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
    assert ask_mock.await_args is not None
    system_prompt, user_prompt = ask_mock.await_args.args
    assert "Do not add a generic caution or disclaimer section" in system_prompt
    assert "no disclaimer is necessary" in system_prompt
    assert "do not dwell on the lack of a trend" in system_prompt
    assert "Please explain what the Potassium values mean based on these results." in user_prompt
    assert "whether that is commonly seen" in user_prompt
    assert "We understand this is not medical advice, so no disclaimer is necessary." in user_prompt
