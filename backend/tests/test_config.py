from pathlib import Path

import pytest
from sqlalchemy.engine import make_url

from illdashboard.config import BACKEND_DIR, Settings, _normalize_sqlite_url
from illdashboard.copilot import client as copilot_client


def test_normalize_sqlite_url_resolves_relative_paths_from_backend_root():
    normalized = _normalize_sqlite_url("sqlite+aiosqlite:///./data/health.db")

    url = make_url(normalized)
    assert url.get_backend_name() == "sqlite"
    assert Path(url.database) == (BACKEND_DIR / "data" / "health.db").resolve()


def test_normalize_sqlite_url_leaves_memory_databases_unchanged():
    database_url = "sqlite+aiosqlite:///:memory:"

    assert _normalize_sqlite_url(database_url) == database_url


@pytest.fixture
def default_settings(monkeypatch):
    for setting_name in Settings.model_fields:
        if setting_name.startswith("COPILOT_") or setting_name in {"EXTRACTION_PROVIDER", "NORMALIZATION_PROVIDER"}:
            monkeypatch.delenv(setting_name, raising=False)
    return Settings(_env_file=None)


def test_copilot_model_defaults(default_settings):
    assert default_settings.EXTRACTION_PROVIDER == "copilot"
    assert default_settings.NORMALIZATION_PROVIDER == "copilot"
    assert default_settings.COPILOT_DEFAULT_MODEL == "gpt-5.6-terra"
    assert default_settings.COPILOT_MEASUREMENT_EXTRACTION_MODEL == "gpt-6.1-sol"
    assert default_settings.COPILOT_TEXT_EXTRACTION_MODEL == "gpt-6-luna"
    assert default_settings.COPILOT_NORMALIZATION_MODEL == "gpt-6-luna"


def test_copilot_settings_reasoning_defaults(default_settings):
    assert default_settings.COPILOT_MEASUREMENT_EXTRACTION_REASONING_EFFORT == "medium"
    reasoning_settings = (
        "COPILOT_TEXT_EXTRACTION_REASONING_EFFORT",
        "COPILOT_NORMALIZATION_REASONING_EFFORT",
    )
    for setting_name in reasoning_settings:
        assert getattr(default_settings, setting_name) is None


@pytest.mark.parametrize(
    ("request_name", "model_setting"),
    [
        ("structured_medical_extraction", "COPILOT_MEASUREMENT_EXTRACTION_MODEL"),
        ("document_text_extraction", "COPILOT_TEXT_EXTRACTION_MODEL"),
        ("normalize_marker_names", "COPILOT_NORMALIZATION_MODEL"),
        ("normalize_source_name", "COPILOT_NORMALIZATION_MODEL"),
        ("choose_canonical_units", "COPILOT_NORMALIZATION_MODEL"),
        ("infer_rescaling_factors", "COPILOT_NORMALIZATION_MODEL"),
        ("review_anomalous_rescaling", "COPILOT_NORMALIZATION_MODEL"),
        ("normalize_qualitative_values", "COPILOT_NORMALIZATION_MODEL"),
        ("classify_marker_groups", "COPILOT_NORMALIZATION_MODEL"),
        ("medical_summary", "COPILOT_DEFAULT_MODEL"),
        ("general_request", "COPILOT_DEFAULT_MODEL"),
    ],
)
def test_copilot_model_routing_and_environment_override(default_settings, monkeypatch, request_name, model_setting):
    monkeypatch.setattr(copilot_client, "settings", default_settings)

    routed = copilot_client._request_session_settings(request_name)

    assert routed.model == getattr(default_settings, model_setting)
    expected_effort = (
        default_settings.COPILOT_MEASUREMENT_EXTRACTION_REASONING_EFFORT
        if model_setting == "COPILOT_MEASUREMENT_EXTRACTION_MODEL"
        else None
    )
    assert routed.reasoning_effort == expected_effort

    monkeypatch.setenv(model_setting, "test-model-override")
    monkeypatch.setattr(copilot_client, "settings", Settings(_env_file=None))

    assert copilot_client._request_session_settings(request_name).model == "test-model-override"