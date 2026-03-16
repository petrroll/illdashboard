"""Generate tiny sparkline PNGs for biomarker history."""

import hashlib
import io
from pathlib import Path

from matplotlib import pyplot as plt

plt.switch_backend("Agg")


SPARKLINE_CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "sparklines"
SPARKLINE_WIDTH = 180  # px
SPARKLINE_HEIGHT = 40  # px
DPI = 72
LINE_WIDTH = 2.8  # shared between sparkline segments and gauge bars
STYLE_VERSION = "v8"  # bump when changing sparkline colors/styling


def _cache_path(marker_name: str, signature: str) -> Path:
    """Return the cache file path for a given marker + signature."""
    safe = hashlib.sha256(f"{STYLE_VERSION}:{marker_name}:{signature}".encode()).hexdigest()[:24]
    return SPARKLINE_CACHE_DIR / f"{safe}.png"


def get_cached_sparkline(marker_name: str, signature: str) -> bytes | None:
    """Return cached PNG bytes if still valid, else None."""
    path = _cache_path(marker_name, signature)
    if path.exists():
        return path.read_bytes()
    return None


def _draw_gauge(ax, value, ref_low, ref_high, color_ok, color_oor, is_oor):
    """Draw a horizontal gauge bar for a single measurement."""
    if ref_low is not None and ref_high is not None:
        ref_span = ref_high - ref_low
        margin = max(ref_span * 0.4, 0.5)
        gauge_min = ref_low - margin
        gauge_max = ref_high + margin
    else:
        margin = max(abs(value) * 0.2, 1.0)
        gauge_min = value - margin
        gauge_max = value + margin

    gauge_min = min(gauge_min, value - 0.5)
    gauge_max = max(gauge_max, value + 0.5)

    y = 0.5

    # Draw full green bar as base (provides rounded caps at both ends)
    ax.plot([gauge_min, gauge_max], [y, y], color=color_ok, linewidth=LINE_WIDTH, solid_capstyle="round")

    if ref_low is not None and ref_high is not None:
        # Red zone below ref_low
        if ref_low > gauge_min:
            ax.plot([gauge_min, ref_low], [y, y], color=color_oor, linewidth=LINE_WIDTH, solid_capstyle="round")
        # Red zone above ref_high
        if ref_high < gauge_max:
            ax.plot([ref_high, gauge_max], [y, y], color=color_oor, linewidth=LINE_WIDTH, solid_capstyle="round")

    # Circle indicator at the value
    ax.plot(value, y, "o", color="white", markersize=6, markeredgecolor="#888888", markeredgewidth=1.2, zorder=10)

    pad = (gauge_max - gauge_min) * 0.06
    ax.set_xlim(gauge_min - pad, gauge_max + pad)
    ax.set_ylim(0, 1)


def _draw_sparkline(ax, values, ref_low, ref_high, color_ok, color_oor, is_oor):
    """Draw the multi-point sparkline chart."""
    xs = list(range(len(values)))

    # Reference band
    if ref_low is not None and ref_high is not None:
        ax.axhspan(ref_low, ref_high, color="#12c78e", alpha=0.22)
        ax.axhline(ref_low, color="#12c78e", linewidth=0.7, alpha=0.6)
        ax.axhline(ref_high, color="#f85149", linewidth=0.7, alpha=0.6)

    # Draw line segments colored by out-of-range status
    for i in range(len(values) - 1):
        x0, x1 = xs[i], xs[i + 1]
        v0, v1 = values[i], values[i + 1]
        oor0, oor1 = is_oor(v0), is_oor(v1)

        if not oor0 and not oor1:
            ax.plot([x0, x1], [v0, v1], color=color_ok, linewidth=LINE_WIDTH, solid_capstyle="round")
        elif oor0 and oor1:
            ax.plot([x0, x1], [v0, v1], color=color_oor, linewidth=LINE_WIDTH, solid_capstyle="round")
        else:
            xm = (x0 + x1) / 2
            vm = (v0 + v1) / 2
            c0 = color_oor if oor0 else color_ok
            c1 = color_oor if oor1 else color_ok
            ax.plot([x0, xm], [v0, vm], color=c0, linewidth=LINE_WIDTH, solid_capstyle="round")
            ax.plot([xm, x1], [vm, v1], color=c1, linewidth=LINE_WIDTH, solid_capstyle="round")

    # Dots
    dot_colors = [color_oor if is_oor(v) else color_ok for v in values]
    ax.scatter(xs, values, c=dot_colors, s=20, zorder=5, edgecolors="none")

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


def generate_sparkline(
    values: list[float],
    ref_low: float | None,
    ref_high: float | None,
    signature: str,
    marker_name: str,
    qualitative_mode: bool = False,
) -> bytes:
    """Render a tiny sparkline PNG and cache it."""
    fig_w = SPARKLINE_WIDTH / DPI
    fig_h = SPARKLINE_HEIGHT / DPI
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=DPI)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.set_axis_off()

    COLOR_OK = "#22d9a0"
    COLOR_OOR = "#f5a254"

    def _is_oor(v: float) -> bool:
        if ref_low is not None and v < ref_low:
            return True
        if ref_high is not None and v > ref_high:
            return True
        return False

    if len(values) == 1:
        # Single measurement — draw a horizontal gauge bar
        _draw_gauge(ax, values[0], ref_low, ref_high, COLOR_OK, COLOR_OOR, _is_oor)
    else:
        _draw_sparkline(ax, values, ref_low, ref_high, COLOR_OK, COLOR_OOR, _is_oor)

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
