from __future__ import annotations

import pytest

from illdashboard import sparkline


@pytest.mark.asyncio
async def test_generate_sparkline_renders_in_worker_process_and_caches(tmp_path, monkeypatch):
    monkeypatch.setattr(sparkline, "SPARKLINE_CACHE_DIR", tmp_path)
    try:
        png = await sparkline.generate_sparkline([1.0, 3.0, 2.0], 1.5, 2.5, signature="sig", marker_name="Ferritin")
        gauge = await sparkline.generate_sparkline([2.0], None, None, signature="one", marker_name="Iron")
    finally:
        sparkline.shutdown_sparkline_worker()

    assert png.startswith(b"\x89PNG")
    assert gauge.startswith(b"\x89PNG")
    assert sparkline.get_cached_sparkline("Ferritin", "sig") == png
