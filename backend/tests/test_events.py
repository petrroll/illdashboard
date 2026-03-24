from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_events_crud_supports_point_ranged_and_ongoing_occurrences(client):
    create_response = await client.post(
        "/api/events",
        json={
            "name": "COVID infection",
            "occurrences": [
                {
                    "start_on": "2024-02",
                    "end_on": "2024-03",
                    "notes": "Lingering fatigue.",
                },
                {
                    "start_on": "2025-01-10",
                    "notes": "Positive rapid test.",
                },
                {
                    "start_on": "2025-02",
                    "is_ongoing": True,
                    "notes": "Recovery dragged on.",
                },
            ],
        },
    )
    assert create_response.status_code == 201
    created = create_response.json()
    assert created["name"] == "COVID infection"
    assert len(created["occurrences"]) == 3
    assert created["occurrences"][1]["end_on"] is None
    assert created["occurrences"][1]["is_ongoing"] is False
    assert created["occurrences"][2]["end_on"] is None
    assert created["occurrences"][2]["is_ongoing"] is True

    event_id = created["id"]
    get_response = await client.get(f"/api/events/{event_id}")
    assert get_response.status_code == 200
    assert len(get_response.json()["occurrences"]) == 3

    update_response = await client.put(
        f"/api/events/{event_id}",
        json={
            "name": "COVID / illness",
            "occurrences": [
                {
                    "start_on": "2025-01-10",
                    "is_ongoing": True,
                    "notes": "Still dealing with symptoms.",
                }
            ],
        },
    )
    assert update_response.status_code == 200
    updated = update_response.json()
    assert updated["name"] == "COVID / illness"
    assert len(updated["occurrences"]) == 1
    assert updated["occurrences"][0]["is_ongoing"] is True

    list_response = await client.get("/api/events")
    assert list_response.status_code == 200
    assert [entry["name"] for entry in list_response.json()] == ["COVID / illness"]

    delete_response = await client.delete(f"/api/events/{event_id}")
    assert delete_response.status_code == 200
    assert delete_response.json() == {"ok": True}

    after_delete = await client.get("/api/events")
    assert after_delete.status_code == 200
    assert after_delete.json() == []


@pytest.mark.asyncio
async def test_events_reject_invalid_occurrence_ranges(client):
    response = await client.post(
        "/api/events",
        json={
            "name": "Bad month",
            "occurrences": [
                {
                    "start_on": "2024-08",
                    "end_on": "2024-07",
                }
            ],
        },
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_events_reject_ongoing_occurrences_with_explicit_end_dates(client):
    response = await client.post(
        "/api/events",
        json={
            "name": "Contradictory event",
            "occurrences": [
                {
                    "start_on": "2024-08",
                    "end_on": "2024-09",
                    "is_ongoing": True,
                }
            ],
        },
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_reset_database_clears_events_store(client):
    create_response = await client.post(
        "/api/events",
        json={
            "name": "Moved house",
            "occurrences": [
                {
                    "start_on": "2024-11",
                    "end_on": "2024-12",
                }
            ],
        },
    )
    assert create_response.status_code == 201

    reset_response = await client.delete("/api/admin/database")
    assert reset_response.status_code == 200

    events_response = await client.get("/api/events")
    assert events_response.status_code == 200
    assert events_response.json() == []
