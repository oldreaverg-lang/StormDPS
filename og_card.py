"""Per-storm Open Graph share cards (1200x630 PNG).

Social platforms scrape og:image when a /storm/<id> link is shared. The site
shipped a single static logo, so every storm looked identical in a tweet/iMessage
preview. This renders a branded, storm-specific card — name, DPS, band, category —
which dramatically improves click-through.

Fail-open by contract: render_storm_card_png returns None on ANY problem (missing
Pillow, missing fonts, bad data); the caller then falls back to the static logo.
"""
from __future__ import annotations

import io
import os
import threading
from typing import Optional

_FONT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend", "og", "fonts")
_BG = (13, 27, 42)          # dark navy
_FG = (255, 255, 255)
_MUTED = (159, 179, 200)
_DIM = (107, 130, 153)

# Canonical DPS bands (must match getDPSBand / _rating_from_dps): label + accent.
_BANDS = [
    (90, "HISTORIC", (185, 28, 28)),
    (80, "DEVASTATING", (220, 38, 38)),
    (60, "EXTREME", (234, 88, 12)),
    (40, "SEVERE", (217, 119, 6)),
    (20, "MODERATE", (234, 179, 8)),
    (10, "LOW", (59, 130, 246)),
    (0, "MINIMAL", (100, 116, 139)),
]

_cache: dict = {}            # storm_id -> (mtime_key, png_bytes)
_lock = threading.Lock()


def _band(dps: float):
    for thr, label, color in _BANDS:
        if dps >= thr:
            return label, color
    return "MINIMAL", (100, 116, 139)


def _storm_type(category, basin_name: str) -> str:
    b = (basin_name or "").lower()
    try:
        cat = int(category)
    except (TypeError, ValueError):
        cat = 0
    if cat < 1:
        return "Tropical Storm"
    if "atlantic" in b or "pacific" in b and "west" not in b:
        return "Hurricane"
    if "west" in b:  # Western Pacific
        return "Typhoon"
    if "indian" in b or "south" in b:
        return "Cyclone"
    return "Hurricane"


def _truncate(draw, text, font, max_w):
    if draw.textlength(text, font=font) <= max_w:
        return text
    while text and draw.textlength(text + "…", font=font) > max_w:
        text = text[:-1]
    return (text + "…") if text else text


def render_storm_card_png(storm_id: str, storm: dict) -> Optional[bytes]:
    """Render a 1200x630 PNG card for a storm. Returns None on any failure.

    Cached in-process, keyed by the storm fields the card depends on, so a re-bake
    (which changes dps/name/category) transparently invalidates the cached image.
    """
    if not storm:
        return None
    try:
        dps = storm.get("dps")
        if dps is None:
            return None
        dps = float(dps)
    except (TypeError, ValueError):
        return None

    key = (storm.get("dps"), storm.get("name"), storm.get("category"),
           storm.get("basin_name"), storm.get("year"))
    with _lock:
        hit = _cache.get(storm_id)
        if hit and hit[0] == key:
            return hit[1]

    try:
        from PIL import Image, ImageDraw, ImageFont

        def font(bold, size):
            f = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
            return ImageFont.truetype(os.path.join(_FONT_DIR, f), size)

        W, H = 1200, 630
        img = Image.new("RGB", (W, H), _BG)
        d = ImageDraw.Draw(img)

        label, accent = _band(dps)

        # Top accent bar (severity color) + faint baseline rule.
        d.rectangle([0, 0, W, 14], fill=accent)

        pad = 72
        # Wordmark
        d.text((pad, 54), "STORM DPS", font=font(True, 34), fill=_MUTED)

        # Storm name (big)
        name = str(storm.get("name") or storm_id)
        year = storm.get("year")
        title = f"{name} ({year})" if year else name
        d.text((pad, 132), _truncate(d, title, font(True, 78), W - 2 * pad), font=font(True, 78), fill=_FG)

        # Subline: type · basin · category
        cat = storm.get("category")
        basin = storm.get("basin_name") or ""
        parts = [_storm_type(cat, basin)]
        if basin:
            parts.append(basin)
        try:
            if int(cat) >= 1:
                parts.append(f"Category {int(cat)}")
        except (TypeError, ValueError):
            pass
        d.text((pad, 236), "  ·  ".join(parts), font=font(False, 36), fill=_MUTED)

        # Score block: small label, then the big number, with "/100" + band
        # stacked to its right.
        d.text((pad, 312), "DESTRUCTIVE POWER SCORE", font=font(True, 26), fill=_DIM)
        num = str(int(round(dps)))
        num_font = font(True, 188)
        d.text((pad - 4, 348), num, font=num_font, fill=accent)
        num_w = d.textlength(num, font=num_font)
        d.text((pad + num_w + 28, 398), "/100", font=font(True, 54), fill=_MUTED)
        d.text((pad + num_w + 30, 470), label, font=font(True, 58), fill=accent)

        # Footer
        d.text((pad, H - 54), "stormdps.com  ·  a 0–100 hurricane rating beyond the Category scale",
               font=font(False, 28), fill=_DIM)

        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        data = buf.getvalue()
        with _lock:
            _cache[storm_id] = (key, data)
        return data
    except Exception:
        return None
