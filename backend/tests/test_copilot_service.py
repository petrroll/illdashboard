from unittest.mock import AsyncMock, call, patch

import pytest

from illdashboard import copilot_service


class DummyDoc:
    def __init__(self, page_count: int):
        self.page_count = page_count

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_ocr_extract_pdf_splits_oversized_batches_and_preserves_page_numbers():
    async def fake_batch(pdf_path: str, *, start_page: int, stop_page: int, dpi: int):
        if stop_page - start_page > 1:
            raise Exception("CAPIError: 413 failed to parse request")
        return {
            "lab_date": "2025-09-05",
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
    assert [measurement["page_number"] for measurement in result["measurements"]] == [1, 2, 3, 4]
    assert [measurement["marker_name"] for measurement in result["measurements"]] == [
        "Marker 1",
        "Marker 2",
        "Marker 3",
        "Marker 4",
    ]
    assert batch_mock.await_args_list == [
        call("/tmp/report.pdf", start_page=0, stop_page=2, dpi=144),
        call("/tmp/report.pdf", start_page=0, stop_page=1, dpi=144),
        call("/tmp/report.pdf", start_page=1, stop_page=2, dpi=144),
        call("/tmp/report.pdf", start_page=2, stop_page=4, dpi=144),
        call("/tmp/report.pdf", start_page=2, stop_page=3, dpi=144),
        call("/tmp/report.pdf", start_page=3, stop_page=4, dpi=144),
    ]


@pytest.mark.asyncio
async def test_ocr_extract_pdf_retries_single_page_at_lower_dpi_after_413():
    async def fake_batch(pdf_path: str, *, start_page: int, stop_page: int, dpi: int):
        if dpi == copilot_service.OCR_PDF_RENDER_DPI:
            raise Exception("CAPIError: 413 failed to parse request")
        return {
            "lab_date": None,
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
        call("/tmp/report.pdf", start_page=0, stop_page=1, dpi=144),
        call("/tmp/report.pdf", start_page=0, stop_page=1, dpi=120),
    ]