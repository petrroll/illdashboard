import json

from illdashboard import metrics


def test_get_premium_requests_used_returns_premium_value(tmp_path, monkeypatch):
    metrics_path = tmp_path / "metrics.json"
    metrics_path.write_text(json.dumps({"premium_requests_used": 8.0}))
    monkeypatch.setattr(metrics, "_METRICS_FILE", metrics_path)

    value = metrics.get_premium_requests_used()

    assert value == 8.0


def test_add_premium_requests_accumulates_existing_value(tmp_path, monkeypatch):
    metrics_path = tmp_path / "metrics.json"
    metrics_path.write_text(json.dumps({"premium_requests_used": 8.0}))
    monkeypatch.setattr(metrics, "_METRICS_FILE", metrics_path)

    metrics.add_premium_requests(1.0)

    assert json.loads(metrics_path.read_text())["premium_requests_used"] == 9.0


def test_get_premium_requests_used_ignores_legacy_key(tmp_path, monkeypatch):
    metrics_path = tmp_path / "metrics.json"
    metrics_path.write_text(json.dumps({"copilot_request_count": 8}))
    monkeypatch.setattr(metrics, "_METRICS_FILE", metrics_path)

    assert metrics.get_premium_requests_used() is None