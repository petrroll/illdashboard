from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from illdashboard.services import ocr_workflow as ocr_service


class DummyDB:
    commit = AsyncMock()
    rollback = AsyncMock()


@pytest.mark.asyncio
async def test_stream_ocr_for_labs_logs_file_context_on_extraction_failure():
    lab = SimpleNamespace(id=7, filename="2023-2-immunology.pdf", filepath="2023-2-immunology.pdf")
    db = DummyDB()

    with patch(
        "illdashboard.services.ocr_workflow.extract_ocr_result",
        new=AsyncMock(side_effect=RuntimeError("ocr failed")),
    ), patch("illdashboard.services.ocr_workflow.logger") as logger_mock:
        events = [event async for event in ocr_service.stream_ocr_for_labs([lab], db)]

    assert any('"status": "error"' in event for event in events)
    logger_mock.exception.assert_called_once_with(
        "%sOCR extraction failed file_id=%s filename=%r path=%r",
        "",
        7,
        "2023-2-immunology.pdf",
        "2023-2-immunology.pdf",
    )
    db.commit.assert_not_awaited()
    db.rollback.assert_not_awaited()