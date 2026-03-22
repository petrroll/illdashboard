from __future__ import annotations

import pytest
from sqlalchemy import select

from illdashboard.medications_database import get_medications_db


@pytest.mark.asyncio
async def test_medications_crud_supports_multiple_episodes_and_default_daily_frequency(client):
    create_response = await client.post(
        "/api/medications",
        json={
            "name": "Metformin",
            "episodes": [
                {
                    "start_on": "2023-01",
                    "still_taking": False,
                    "end_on": "2023-07",
                    "dose": "500 mg",
                    "notes": "Started with dinner.",
                },
                {
                    "start_on": "2024-02-15",
                    "still_taking": True,
                    "dose": "850 mg",
                    "frequency": "twice daily",
                    "notes": "Restarted after labs.",
                },
            ],
        },
    )
    assert create_response.status_code == 201
    created = create_response.json()
    assert created["name"] == "Metformin"
    assert created["episodes"][0]["frequency"] == "daily"
    assert created["episodes"][1]["frequency"] == "twice daily"

    medication_id = created["id"]
    get_response = await client.get(f"/api/medications/{medication_id}")
    assert get_response.status_code == 200
    assert len(get_response.json()["episodes"]) == 2

    update_response = await client.put(
        f"/api/medications/{medication_id}",
        json={
            "name": "Metformin XR",
            "episodes": [
                {
                    "start_on": "2024-02-15",
                    "still_taking": True,
                    "dose": "1000 mg",
                    "frequency": "daily",
                    "notes": "Extended release.",
                }
            ],
        },
    )
    assert update_response.status_code == 200
    updated = update_response.json()
    assert updated["name"] == "Metformin XR"
    assert len(updated["episodes"]) == 1
    assert updated["episodes"][0]["dose"] == "1000 mg"

    list_response = await client.get("/api/medications")
    assert list_response.status_code == 200
    medications = list_response.json()
    assert [entry["name"] for entry in medications] == ["Metformin XR"]

    delete_response = await client.delete(f"/api/medications/{medication_id}")
    assert delete_response.status_code == 200
    assert delete_response.json() == {"ok": True}

    after_delete = await client.get("/api/medications")
    assert after_delete.status_code == 200
    assert after_delete.json() == []


@pytest.mark.asyncio
async def test_medications_reject_invalid_episode_ranges(client):
    response = await client.post(
        "/api/medications",
        json={
            "name": "Vitamin D",
            "episodes": [
                {
                    "start_on": "2024-08",
                    "still_taking": False,
                    "end_on": "2024-07",
                    "dose": "2000 IU",
                }
            ],
        },
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_reset_database_clears_medications_store(client):
    create_response = await client.post(
        "/api/medications",
        json={
            "name": "Levothyroxine",
            "episodes": [
                {
                    "start_on": "2022-03",
                    "still_taking": True,
                    "dose": "50 mcg",
                }
            ],
        },
    )
    assert create_response.status_code == 201

    reset_response = await client.delete("/api/admin/database")
    assert reset_response.status_code == 200

    medications_response = await client.get("/api/medications")
    assert medications_response.status_code == 200
    assert medications_response.json() == []


@pytest.mark.asyncio
async def test_get_medications_db_opens_session_from_factory(medications_session_factory):
    generator = get_medications_db()
    session = await anext(generator)
    try:
        result = await session.execute(select(1))
        assert result.scalar_one() == 1
    finally:
        await generator.aclose()
