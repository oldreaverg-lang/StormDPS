"""Server-side first state of the homepage shell (index.html).

When storms are active the SPA opens on the overview map, but the shell
shipped with the welcome screen visible and the overview hidden, so the page
first painted the welcome screen and only swapped to the overview once the
~450 KB inline script had run. On a slow phone that swap came ~1 s after
first paint, and it pulled the footer into view as the LCP element
(PageSpeed mobile 2026-09-25: LCP 3.6 s, render delay 2.3 s).

``overview_first`` serves the shell already in that state: welcome hidden,
overview visible, the count and the storm cards filled in. The cards are
byte-for-byte what ``_renderOverviewCards`` in index.html draws from the same
``__ACTIVE_HINT__`` rows, so the SPA's own render on boot changes nothing on
screen (no layout shift). Keep the two in lockstep.

Stdlib only (CI cannot import main). Fail-open: if any required marker is
missing the shell is returned unchanged and the SPA swaps states as before.
"""
from __future__ import annotations

import math

# Mirrors of the index.html helpers used by _renderOverviewCards.
_OV_BASIN = {"AL": "Atlantic", "EP": "E. Pacific", "CP": "C. Pacific",
             "WP": "W. Pacific", "IO": "N. Indian", "SH": "S. Hemisphere"}

_WELCOME = ('class="welcome-state" id="welcomeState"',
            'class="welcome-state hidden" id="welcomeState"')
_OVERVIEW = ('class="overview-state hidden" id="overviewState"',
             'class="overview-state" id="overviewState"')
_COUNT = '<span class="ov-count" id="ovCount"></span>'
_CARDS = '<div class="ov-cards" id="overviewCards"></div>'

# Optional: the welcome logo is invisible in this state, so don't spend a
# high-priority preload on it (lazy <img> inside display:none never loads).
_LOGO_PRELOAD = ('<link rel="preload" as="image" href="/frontend/logo-180.webp" '
                 'type="image/webp" fetchpriority="high">')
_LOGO_IMG = ('alt="StormDPS" width="180" height="180" fetchpriority="high"',
             'alt="StormDPS" width="180" height="180" loading="lazy"')


def js_round(x: float) -> int:
    """JavaScript Math.round (halves round up; Python's round() is banker's)."""
    return int(math.floor(x + 0.5))


def escape_html(v) -> str:
    """index.html escapeHtml."""
    return (str("" if v is None else v)
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;").replace("'", "&#39;"))


def dps_color(score: float) -> str:
    """index.html dpsColor."""
    if score >= 80:
        return "#ef4444"
    if score >= 60:
        return "#f97316"
    if score >= 40:
        return "#fbbf24"
    if score >= 20:
        return "#34d399"
    if score >= 10:
        return "#60a5fa"
    return "#64748b"


def dps_band(score: float) -> str:
    """index.html getDPSBand."""
    if score >= 90:
        return "Historic"
    if score >= 80:
        return "Devastating"
    if score >= 60:
        return "Extreme"
    if score >= 40:
        return "Severe"
    if score >= 20:
        return "Moderate"
    if score >= 10:
        return "Low"
    return "Minimal"


def badge_text_color(hex_color: str) -> str:
    """index.html badgeTextColor for a #rrggbb colour: white or near-black,
    whichever contrasts more (WCAG relative luminance)."""
    try:
        h = hex_color.lstrip("#")
        chans = [int(h[i:i + 2], 16) / 255 for i in (0, 2, 4)]
    except (ValueError, AttributeError):
        return "#fff"
    r, g, b = (c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in chans)
    lum = 0.2126 * r + 0.7152 * g + 0.0722 * b
    return "#fff" if (1.05 / (lum + 0.05)) >= ((lum + 0.05) / 0.05) else "#111"


def _num(v):
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v) if math.isfinite(v) else None


def render_card(s: dict) -> str:
    """One overview card — mirror of the template in _renderOverviewCards."""
    dps = _num(s.get("dps"))
    col = dps_color(dps) if dps is not None else "#64748b"
    band = s.get("dps_label") or (dps_band(dps) if dps is not None else "Not scored yet")
    basin = _OV_BASIN.get(str(s.get("id") or "")[:2].upper(), "")
    kt_raw = s.get("intensity_knots")
    kt = ""
    if kt_raw is not None:
        try:
            kt = f"{js_round(float(kt_raw))} kt"
        except (TypeError, ValueError):
            kt = ""
    meta = " · ".join(p for p in (band, kt, basin) if p)
    name = s.get("name") or s.get("id")
    value = str(js_round(dps)) if dps is not None else "—"
    return (f'<button class="ov-card" type="button" data-storm-id="{escape_html(s.get("id"))}" '
            f'title="Open {escape_html(name)} — {escape_html(meta)}">'
            f'<span class="ov-dps" style="background:{col};color:{badge_text_color(col)}">'
            f'<small>DPS</small>{value}</span>'
            f'<span class="ov-body"><span class="ov-name">{escape_html(name)}</span>'
            f'<span class="ov-meta">{escape_html(meta)}</span></span></button>')


def overview_first(html: str, storms: list) -> str:
    """The homepage shell in its overview-first state for these (presented,
    DPS-sorted) active-storm rows. Unchanged when there are no storms or the
    shell's markers are missing."""
    if not storms:
        return html
    for marker in (_WELCOME[0], _OVERVIEW[0], _COUNT, _CARDS):
        if html.count(marker) != 1:
            return html
    cards = "".join(render_card(s) for s in storms if isinstance(s, dict))
    out = (html.replace(_WELCOME[0], _WELCOME[1], 1)
               .replace(_OVERVIEW[0], _OVERVIEW[1], 1)
               .replace(_COUNT, _COUNT.replace("></span>", f">· {len(storms)}</span>"), 1)
               .replace(_CARDS, _CARDS.replace("></div>", f">{cards}</div>"), 1))
    if out.count(_LOGO_PRELOAD) == 1 and out.count(_LOGO_IMG[0]) == 1:
        out = out.replace(_LOGO_PRELOAD, "", 1).replace(_LOGO_IMG[0], _LOGO_IMG[1], 1)
    return out
