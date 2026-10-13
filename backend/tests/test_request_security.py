"""Cross-site protection for unsafe browser requests and the insight generation verb."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

CANCEL_TARGET = "illdashboard.api.files.pipeline.cancel_processing_from_clean_runtime"
QUEUE_TARGET = "illdashboard.api.files.pipeline.queue_unprocessed_files_from_clean_runtime"


@pytest.mark.parametrize(
    "headers",
    [
        {"origin": "https://evil.example", "sec-fetch-site": "cross-site"},
        {"origin": "https://evil.example"},
        {"origin": "http://localhost:9999", "sec-fetch-site": "same-site"},
        {"origin": "null", "sec-fetch-site": "cross-site"},
        {"sec-fetch-site": "cross-site"},
        # Attacker page posting through the Vite proxy: changeOrigin rewrites Host, Origin stays foreign.
        {"origin": "https://evil.example", "sec-fetch-site": "cross-site", "host": "localhost:8000"},
    ],
)
async def test_cross_site_bodyless_post_is_rejected_before_side_effects(client, headers):
    with patch(CANCEL_TARGET, new=AsyncMock()) as cancel, patch(QUEUE_TARGET, new=AsyncMock(return_value=[])) as queue:
        cancel_response = await client.post("/api/files/ocr/cancel", headers=headers)
        queue_response = await client.post("/api/files/ocr/unprocessed", headers=headers)

    assert cancel_response.status_code == 403
    assert queue_response.status_code == 403
    cancel.assert_not_awaited()
    queue.assert_not_awaited()


@pytest.mark.parametrize("method", ["put", "patch", "delete"])
async def test_cross_site_other_unsafe_methods_are_rejected(client, method):
    response = await client.request(
        method.upper(),
        "/api/measurements/1",
        headers={"origin": "https://evil.example", "sec-fetch-site": "cross-site"},
    )
    assert response.status_code == 403


async def test_cross_site_safe_read_is_not_blocked(client):
    response = await client.get(
        "/api/files",
        headers={"origin": "https://evil.example", "sec-fetch-site": "cross-site"},
    )
    assert response.status_code == 200


@pytest.mark.parametrize(
    "headers",
    [
        # Vite dev server calling the backend directly on another port of localhost.
        {"origin": "http://localhost:5173", "sec-fetch-site": "same-site", "host": "localhost:8000"},
        {"origin": "http://localhost:3000", "host": "localhost:8000"},
        # Same-origin page behind the Vite proxy (local or private Codespaces forwarded host).
        {"origin": "http://localhost:5173", "sec-fetch-site": "same-origin", "host": "localhost:8000"},
        {
            "origin": "https://example-5173.app.github.dev",
            "sec-fetch-site": "same-origin",
            "host": "localhost:8000",
        },
        # Direct access to a forwarded backend port: TLS ends at the forwarder, so schemes differ.
        {"origin": "https://example-8000.app.github.dev", "host": "example-8000.app.github.dev"},
        # Non-browser clients (CLI, scripts, httpx tests) send no fetch metadata.
        {},
    ],
)
async def test_trusted_unsafe_requests_are_allowed(client, headers):
    with patch(CANCEL_TARGET, new=AsyncMock()) as cancel:
        response = await client.post("/api/files/ocr/cancel", headers=headers)

    assert response.status_code == 200
    cancel.assert_awaited_once()


async def test_cors_preflight_still_answered_for_trusted_origin(client):
    response = await client.options(
        "/api/files/ocr/cancel",
        headers={"origin": "http://localhost:5173", "access-control-request-method": "POST"},
    )
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:5173"


async def test_get_insight_cannot_trigger_generation(client):
    with (
        patch("illdashboard.services.insights.get_cached_or_generated_insight", new=AsyncMock()) as generate,
        patch("illdashboard.services.insights.explain_marker_history", new=AsyncMock()) as explain,
    ):
        response = await client.get("/api/measurements/insight", params={"marker_name": "Ferritin"})

    assert response.status_code == 405
    generate.assert_not_awaited()
    explain.assert_not_awaited()


async def test_cross_site_post_insight_is_rejected_before_generation(client):
    with patch("illdashboard.services.insights.get_cached_or_generated_insight", new=AsyncMock()) as generate:
        response = await client.post(
            "/api/measurements/insight",
            params={"marker_name": "Ferritin"},
            headers={"origin": "https://evil.example", "sec-fetch-site": "cross-site"},
        )

    assert response.status_code == 403
    generate.assert_not_awaited()
