"""Smoke tests for the API."""

import pytest


@pytest.mark.asyncio
async def test_list_files_empty(client):
    resp = await client.get("/api/files")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_list_markers_empty(client):
    resp = await client.get("/api/measurements/markers")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_upload_bad_type(client):
    resp = await client.post(
        "/api/files/upload",
        files={"file": ("test.txt", b"hello", "text/plain")},
    )
    assert resp.status_code == 400
