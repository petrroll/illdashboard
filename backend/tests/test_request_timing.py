from __future__ import annotations

import asyncio
import re

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError
from starlette.responses import Response

from illdashboard import request_timing
from illdashboard.database import create_database_engine
from illdashboard.request_timing import ServerTimingMiddleware


def duration(response, name: str) -> float:
    match = re.search(rf"(?:^|,\s*){name};dur=(\d+\.\d+)", response.headers["server-timing"])
    assert match is not None, response.headers["server-timing"]
    return float(match.group(1))


async def test_timing_measures_to_headers_not_stream_completion_and_preserves_existing_metrics(monkeypatch):
    clock = [10.0]
    monkeypatch.setattr(request_timing, "perf_counter", lambda: clock[0])

    async def app(_scope, _receive, send):
        clock[0] += 0.01234
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"server-timing", b"cache;dur=1.00"), (b"x-existing", b"kept")],
        })
        clock[0] += 9.0
        await send({"type": "http.response.body", "body": b"streamed"})

    async with AsyncClient(
        transport=ASGITransport(app=ServerTimingMiddleware(app)), base_url="http://test",
    ) as client:
        response = await client.get("/")

    assert response.content == b"streamed"
    assert response.headers["x-existing"] == "kept"
    assert "cache;dur=1.00" in response.headers["server-timing"]
    assert duration(response, "app") == 12.34
    assert duration(response, "db") == 0.0
    assert "SQL execution (0 statements)" in response.headers["server-timing"]
    assert request_timing._current_timing.get() is None


async def test_sql_timing_counts_success_and_failure_without_recording_statement_or_parameters(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(request_timing, "perf_counter", lambda: clock[0])
    engine = create_database_engine("sqlite+aiosqlite:///:memory:")

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def simulate_execution_time(*_args):
        clock[0] += 0.005

    async def app(scope, receive, send):
        async with engine.connect() as connection:
            await connection.execute(text("SELECT :private_value"), {"private_value": "private-health-value"})
            with pytest.raises(OperationalError):
                await connection.execute(text("SELECT FROM private_table_name"))
        await Response("ok")(scope, receive, send)

    try:
        async with AsyncClient(
            transport=ASGITransport(app=ServerTimingMiddleware(app)), base_url="http://test",
        ) as client:
            response = await client.get("/?marker_name=private-marker")
        assert duration(response, "app") == 10.0
        assert duration(response, "db") == 10.0
        header = response.headers["server-timing"]
        assert "SQL execution (2 statements)" in header
        for private_data in ("private-health-value", "private_table_name", "private-marker", "SELECT"):
            assert private_data not in header
    finally:
        await engine.dispose()


async def test_concurrent_requests_have_independent_sql_counts_and_ignore_outside_queries():
    engine = create_database_engine("sqlite+aiosqlite:///:memory:")

    async def app(scope, receive, send):
        async with engine.connect() as connection:
            for _ in range(int(scope["path"].strip("/"))):
                await connection.execute(text("SELECT 1"))
                await asyncio.sleep(0)
        await Response("ok")(scope, receive, send)

    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        async with AsyncClient(
            transport=ASGITransport(app=ServerTimingMiddleware(app)), base_url="http://test",
        ) as client:
            responses = await asyncio.gather(client.get("/1"), client.get("/3"), client.get("/0"))
        for response, count in zip(responses, (1, 3, 0), strict=True):
            assert f"SQL execution ({count} statements)" in response.headers["server-timing"]
            assert duration(response, "app") >= 0
            assert duration(response, "db") >= 0
        assert request_timing._current_timing.get() is None
    finally:
        await engine.dispose()


async def test_unhandled_errors_propagate_and_reset_request_context():
    async def app(_scope, _receive, _send):
        assert request_timing._current_timing.get() is not None
        raise RuntimeError("endpoint failed")

    async with AsyncClient(
        transport=ASGITransport(app=ServerTimingMiddleware(app)), base_url="http://test",
    ) as client:
        with pytest.raises(RuntimeError, match="endpoint failed"):
            await client.get("/")
    assert request_timing._current_timing.get() is None


async def test_non_http_scopes_pass_through_unchanged():
    messages = []

    async def app(scope, _receive, send):
        assert scope["type"] == "websocket"
        assert request_timing._current_timing.get() is None
        await send({"type": "websocket.close", "code": 1000})

    async def receive():
        return {"type": "websocket.connect"}

    async def send(message):
        messages.append(message)

    await ServerTimingMiddleware(app)({"type": "websocket"}, receive, send)
    assert messages == [{"type": "websocket.close", "code": 1000}]


@pytest.mark.parametrize(
    ("url", "status", "uses_database"),
    [
        ("/api/measurements/markers", 200, True),
        ("/api/medications", 200, True),
        ("/api/measurements/detail?marker_name=timing-missing", 404, True),
        ("/api/measurements/detail", 422, False),
        ("/missing-route", 404, False),
    ],
)
async def test_real_api_responses_include_timing_for_both_databases_and_errors(client, url, status, uses_database):
    response = await client.get(url, headers={"Origin": "http://localhost:5173"})
    assert response.status_code == status
    assert duration(response, "app") >= 0
    assert duration(response, "db") >= 0
    count = re.search(r"SQL execution \((\d+) statements\)", response.headers["server-timing"])
    assert count is not None
    assert (int(count.group(1)) > 0) == uses_database
    assert "Server-Timing" in response.headers["access-control-expose-headers"]


async def test_cors_preflight_and_cross_site_rejections_are_timed(client):
    preflight = await client.options(
        "/api/measurements/insight",
        headers={"Origin": "http://localhost:5173", "Access-Control-Request-Method": "POST"},
    )
    rejected = await client.post(
        "/api/measurements/insight?marker_name=never-generated",
        headers={"Origin": "https://untrusted.example", "Sec-Fetch-Site": "cross-site"},
    )
    assert preflight.status_code == 200
    assert rejected.status_code == 403
    for response in (preflight, rejected):
        assert duration(response, "app") >= 0
        assert duration(response, "db") == 0
