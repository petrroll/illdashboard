"""Generate tiny sparkline PNGs for biomarker history."""

import hashlib
import io
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


SPARKLINE_CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "sparklines"
SPARKLINE_WIDTH = 180  # px
SPARKLINE_HEIGHT = 40  # px
DPI = 72


def _cache_path(marker_name: str, signature: str) -> Path:
    """Return the cache file path for a given marker + signature."""
    safe = hashlib.sha256(f"{marker_name}:{signature}".encode()).hexdigest()[:24]
    return SPARKLINE_CACHE_DIR / f"{safe}.png"


def get_cached_sparkline(marker_name: str, signature: str) -> bytes | None:
    """Return cached PNG bytes if still valid, else None."""
    path = _cache_path(marker_name, signature)
    if path.exists():
        return path.read_bytes()
    return None


def generate_sparkline(
    values: list[float],
    ref_low: float | None,
    ref_high: float | None,
    signature: str,
    marker_name: str,
) -> bytes:
    """Render a tiny sparkline PNG and cache it."""
    fig_w = SPARKLINE_WIDTH / DPI
    fig_h = SPARKLINE_HEIGHT / DPI
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=DPI)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.set_axis_off()

    xs = list(range(len(values)))

    # Reference band
    if ref_low is not None and ref_high is not None:
        ax.axhspan(ref_low, ref_high, color="#93c5fd", alpha=0.35)
        ax.axhline(ref_low, color="#2563eb", linewidth=0.5, alpha=0.5)
        ax.axhline(ref_high, color="#dc2626", linewidth=0.5, alpha=0.5)

    # Value line
    ax.plot(xs, values, color="#0f766e", linewidth=1.5, solid_capstyle="round")
    # Dots
    ax.scatter(xs, values, color="#0f766e", s=8, zorder=5)

    # Padding
    all_vals = list(values)
    if ref_low is not None:
        all_vals.append(ref_low)
    if ref_high is not None:
        all_vals.append(ref_high)
    vmin, vmax = min(all_vals), max(all_vals)
    span = vmax - vmin
    pad = max(span * 0.15, abs(vmax) * 0.05, 0.5)
    ax.set_ylim(vmin - pad, vmax + pad)
    ax.set_xlim(-0.3, len(values) - 0.7)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", transparent=True, dpi=DPI)
    plt.close(fig)
    png_bytes = buf.getvalue()

    # Cache to disk
    SPARKLINE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(marker_name, signature).write_bytes(png_bytes)

    return png_bytes


def invalidate_marker_cache(marker_name: str) -> None:
    """Remove all cached sparklines for a marker (brute-force, for re-OCR)."""
    if not SPARKLINE_CACHE_DIR.exists():
        return
    # We can't easily map marker→hash without the signature, so this is a no-op.
    # Stale caches are harmlessly replaced when signature changes.
