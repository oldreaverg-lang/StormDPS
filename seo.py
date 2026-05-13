"""
SEO helpers: server-side rendering of per-storm landing pages and shared
JSON-LD blocks. The goal is for every storm to have its own indexable URL
(/storm/{ATCF_ID}) with a unique <title>, <meta description>, canonical
link, and Article + Dataset schema — without having to migrate the full
SPA to a server-rendered framework.

The approach is intentionally minimal: read frontend/index.html once,
patch a few well-known regions per request, and return the result. The
SPA still hydrates the interactive UI; the SSR'd HTML just gives the
crawler something to read before JS runs.
"""

from __future__ import annotations

import html
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

FRONTEND_DIR = Path(__file__).parent / "frontend"
_INDEX_PATH = FRONTEND_DIR / "index.html"
_COMPILED_BUNDLE_PATH = FRONTEND_DIR / "compiled_bundle.json"

# Module-scoped caches. The index.html template + bundle rarely change at
# runtime (only on redeploy), so we read them once and patch per request.
_INDEX_CACHE: Optional[str] = None
_INDEX_MTIME: float = 0.0
_BUNDLE_CACHE: Optional[dict] = None
_BUNDLE_MTIME: float = 0.0

_BASE_URL = "https://stormdps.com"


def _read_index_template() -> str:
    """Return the current index.html template, hot-reloading on disk change."""
    global _INDEX_CACHE, _INDEX_MTIME
    try:
        mtime = _INDEX_PATH.stat().st_mtime
    except OSError:
        return ""
    if _INDEX_CACHE is None or mtime > _INDEX_MTIME:
        _INDEX_CACHE = _INDEX_PATH.read_text(encoding="utf-8")
        _INDEX_MTIME = mtime
    return _INDEX_CACHE


def _read_compiled_bundle() -> dict:
    """Return the compiled bundle dict; hot-reloads on disk change."""
    global _BUNDLE_CACHE, _BUNDLE_MTIME
    try:
        mtime = _COMPILED_BUNDLE_PATH.stat().st_mtime
    except OSError:
        return {}
    if _BUNDLE_CACHE is None or mtime > _BUNDLE_MTIME:
        try:
            _BUNDLE_CACHE = json.loads(_COMPILED_BUNDLE_PATH.read_text(encoding="utf-8"))
            _BUNDLE_MTIME = mtime
        except (OSError, json.JSONDecodeError):
            _BUNDLE_CACHE = {}
    return _BUNDLE_CACHE


def lookup_storm(storm_id: str) -> Optional[dict]:
    """Look up a storm by ATCF ID (e.g. AL122005) or IBTrACS SID."""
    if not storm_id:
        return None
    bundle = _read_compiled_bundle()
    storms = bundle.get("storms", {}) if isinstance(bundle, dict) else {}
    # Try exact key, then upper, then lower
    for key in (storm_id, storm_id.upper(), storm_id.lower()):
        if key in storms and isinstance(storms[key], dict):
            return storms[key]
    return None


# ---------------------------------------------------------------------------
# Per-storm SSR
# ---------------------------------------------------------------------------

# Pre-compiled regexes for the patches we apply.
_RE_TITLE = re.compile(r"<title>[^<]*</title>", re.IGNORECASE)
_RE_DESCRIPTION = re.compile(
    r'<meta\s+name="description"\s+content="[^"]*"\s*/?>',
    re.IGNORECASE,
)
_RE_CANONICAL = re.compile(
    r'<link\s+rel="canonical"\s+href="[^"]*"\s*/?>',
    re.IGNORECASE,
)
_RE_OG_TITLE = re.compile(
    r'<meta\s+property="og:title"\s+content="[^"]*"\s*/?>',
    re.IGNORECASE,
)
_RE_OG_DESCRIPTION = re.compile(
    r'<meta\s+property="og:description"\s+content="[^"]*"\s*/?>',
    re.IGNORECASE,
)
_RE_OG_URL = re.compile(
    r'<meta\s+property="og:url"\s+content="[^"]*"\s*/?>',
    re.IGNORECASE,
)
_RE_TWITTER_TITLE = re.compile(
    r'<meta\s+name="twitter:title"\s+content="[^"]*"\s*/?>',
    re.IGNORECASE,
)
_RE_TWITTER_DESCRIPTION = re.compile(
    r'<meta\s+name="twitter:description"\s+content="[^"]*"\s*/?>',
    re.IGNORECASE,
)
_RE_HEAD_END = re.compile(r"</head>", re.IGNORECASE)
_RE_SSR_CONTENT_MARKER = re.compile(r"<!--SSR_STORM_CONTENT-->")


# Inline styles for the SSR'd storm summary card. Self-contained so the card
# renders correctly even before the SPA's main stylesheet finishes loading.
_SSR_STYLES = """
<style>
.ssr-storm-summary { padding: 1.5rem 1rem 1.2rem; max-width: 980px; margin: 0 auto; }
.ssr-storm-summary h1 { font-size: clamp(1.5rem, 3vw, 2.1rem); font-weight: 800; letter-spacing: -.03em; line-height: 1.15; color: #f1f5f9; margin-bottom: .35rem; }
.ssr-storm-summary .ssr-subhead { color: #94a3b8; font-size: .92rem; margin-bottom: 1rem; }
.ssr-card { background: #111827; border: 1px solid #1e293b; border-radius: 14px; padding: 1.2rem 1.4rem; display: grid; grid-template-columns: minmax(140px, auto) 1fr; gap: 1.2rem 1.8rem; align-items: start; }
@media (max-width: 640px) { .ssr-card { grid-template-columns: 1fr; } }
.ssr-score-block { display: flex; flex-direction: column; align-items: center; justify-content: center; padding: .5rem; background: #0d1221; border: 1px solid #1e293b; border-radius: 12px; min-width: 140px; }
.ssr-score-value { font-size: 3rem; font-weight: 800; letter-spacing: -.04em; line-height: 1; color: #f1f5f9; font-variant-numeric: tabular-nums; }
.ssr-score-of { font-size: 1rem; color: #64748b; font-weight: 600; margin-left: 2px; }
.ssr-score-label { font-size: .7rem; text-transform: uppercase; letter-spacing: .08em; color: #94a3b8; font-weight: 700; margin-top: .35rem; }
.ssr-score-rating { font-size: .85rem; font-weight: 700; margin-top: .25rem; color: #6366f1; }
.ssr-body p { color: #cbd5e1; font-size: .98rem; line-height: 1.65; margin-bottom: .7rem; }
.ssr-stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: .5rem .9rem; margin-top: .9rem; }
.ssr-stat { background: #0d1221; border: 1px solid #1e293b; border-radius: 8px; padding: .5rem .65rem; }
.ssr-stat-label { font-size: .68rem; text-transform: uppercase; letter-spacing: .05em; color: #64748b; font-weight: 600; }
.ssr-stat-value { font-size: 1.05rem; font-weight: 700; color: #f1f5f9; font-variant-numeric: tabular-nums; margin-top: 1px; }
.ssr-landfalls { margin-top: .9rem; font-size: .9rem; color: #94a3b8; }
.ssr-landfalls strong { color: #cbd5e1; }
.ssr-cta { margin-top: 1rem; font-size: .85rem; color: #64748b; }
.ssr-cta a { color: #6366f1; text-decoration: underline; text-decoration-color: rgba(99,102,241,.4); }
.ssr-cta a:hover { text-decoration-color: #6366f1; }
.ssr-storm-summary.ssr-hidden { display: none; }
</style>
"""


_DPS_LABEL_RATING = {
    "historic": "Historic",
    "catastrophic": "Catastrophic",
    "severe": "Severe",
    "notable": "Notable",
    "minimal": "Minimal",
}


def _rating_from_dps(dps: Optional[float]) -> str:
    if not isinstance(dps, (int, float)):
        return ""
    if dps >= 90: return "Historic"
    if dps >= 75: return "Catastrophic"
    if dps >= 50: return "Severe"
    if dps >= 25: return "Notable"
    return "Minimal"


def _format_landfalls(landfalls: list) -> str:
    """Render landfall regions as 'Louisiana, Mississippi (2 landfalls)' etc."""
    if not isinstance(landfalls, list) or not landfalls:
        return ""
    regions: list[str] = []
    seen = set()
    for lf in landfalls:
        if not isinstance(lf, dict):
            continue
        r = (lf.get("region") or "").strip()
        if r and r not in seen:
            seen.add(r)
            regions.append(r)
    if not regions:
        return ""
    if len(landfalls) > 1:
        return f"{', '.join(regions)} ({len(landfalls)} landfalls)"
    return regions[0]


def _build_storm_summary_html(storm_id: str, storm: dict, canonical: str) -> str:
    """Render the visible SSR card. Real content that Google reads."""
    name = storm.get("name") or storm_id
    year = storm.get("year")
    dps = storm.get("dps")
    label = storm.get("dps_label") or ""
    cat = storm.get("category_lifetime") or storm.get("category")
    basin_name = storm.get("basin_name") or ""
    peak_wind_kt = storm.get("peak_wind_kt")
    peak_wind_ms = storm.get("peak_wind_ms")
    min_pressure = storm.get("min_pressure_hpa")
    peak_ike = storm.get("peak_ike_tj") or storm.get("peak_ike")
    landfalls = storm.get("landfalls") or []
    rating = _rating_from_dps(dps) or _DPS_LABEL_RATING.get((label or "").lower(), "")

    name_e = html.escape(name)
    year_str = f" ({year})" if year else ""
    dps_str = f"{dps:.0f}" if isinstance(dps, (int, float)) else "—"

    # Headline: "Hurricane Katrina (2005)"
    head_word = "Hurricane" if (isinstance(cat, int) and cat >= 1) else "Storm"
    if basin_name and "Pacific" in basin_name and isinstance(cat, int) and cat >= 1:
        head_word = "Typhoon" if "West" in basin_name else "Hurricane"
    headline = f"{head_word} {name_e}{html.escape(year_str)}"

    # Subhead: category + basin
    sub_pieces = []
    if isinstance(cat, int) and cat >= 1:
        sub_pieces.append(f"Peak Category {cat}")
    elif isinstance(cat, int):
        sub_pieces.append("Tropical Storm intensity")
    if basin_name:
        sub_pieces.append(html.escape(basin_name))
    subhead = " · ".join(sub_pieces)

    # Prose paragraph — a real explanation, not just stats. This is the SEO meat.
    prose_parts = []
    prose_parts.append(
        f"{headline} scored <strong>{dps_str}/100</strong> on the "
        f'<a href="/methodology">Destructive Power Score</a> scale'
        + (f" — a <strong>{rating}</strong> event" if rating else "")
        + "."
    )
    # Saffir-Simpson contrast hook
    if isinstance(cat, int) and cat >= 1 and isinstance(dps, (int, float)):
        if dps >= 75 and cat <= 2:
            prose_parts.append(
                f"Although the Saffir-Simpson scale rated this storm a "
                f"Category {cat}, its DPS score of {dps_str} reflects a far "
                f"more destructive footprint than wind speed alone suggests — "
                f"driven by storm size, surge potential, duration of coastal "
                f"exposure, and geographic reach."
            )
        elif dps < 40 and cat >= 4:
            prose_parts.append(
                f"Although the Saffir-Simpson scale rated this a Category {cat}, "
                f"its DPS score of {dps_str} reflects limited destructive impact "
                f"— typical of an intense but compact or open-ocean storm."
            )
        else:
            prose_parts.append(
                f"Saffir-Simpson rated this a Category {cat} based on peak wind alone. "
                f"DPS combines intensity with storm size, surge potential, duration "
                f"of coastal exposure, and geographic reach for a fuller picture of "
                f"destructive potential."
            )
    elif isinstance(dps, (int, float)):
        prose_parts.append(
            "DPS combines peak intensity with storm size, surge potential, "
            "duration of coastal exposure, and geographic reach — capturing "
            "what the traditional Saffir-Simpson Category scale misses."
        )

    landfall_text = _format_landfalls(landfalls)
    if landfall_text:
        prose_parts.append(f"Landfall: <strong>{html.escape(landfall_text)}</strong>.")

    prose_html = "".join(f"<p>{p}</p>" for p in prose_parts)

    # Stats grid
    stats: list[tuple[str, str]] = []
    if peak_wind_kt:
        mph = round(peak_wind_kt * 1.15078)
        stats.append(("Peak Winds", f"{mph} mph · {round(peak_wind_kt)} kt"))
    elif peak_wind_ms:
        mph = round(peak_wind_ms * 2.23694)
        stats.append(("Peak Winds", f"{mph} mph"))
    if min_pressure:
        stats.append(("Min Pressure", f"{round(min_pressure)} mb"))
    if peak_ike and isinstance(peak_ike, (int, float)):
        stats.append(("Peak IKE", f"{peak_ike:.1f} TJ"))
    if basin_name:
        stats.append(("Basin", html.escape(basin_name)))
    if rating:
        stats.append(("DPS Rating", rating))

    stats_html = ""
    if stats:
        stats_html = '<div class="ssr-stats">' + "".join(
            f'<div class="ssr-stat"><div class="ssr-stat-label">{lbl}</div>'
            f'<div class="ssr-stat-value">{val}</div></div>'
            for lbl, val in stats
        ) + "</div>"

    cta_html = (
        '<div class="ssr-cta">Explore the full interactive analysis below — '
        'wind field, track, score components, and side-by-side comparisons. '
        '<a href="/methodology">How DPS works</a>.</div>'
    )

    return (
        _SSR_STYLES
        + '<section class="ssr-storm-summary" id="ssrStormSummary" aria-label="Storm summary">'
        + f"<h1>{headline} — DPS {dps_str}/100</h1>"
        + (f'<div class="ssr-subhead">{subhead}</div>' if subhead else "")
        + '<div class="ssr-card">'
        + '<div class="ssr-score-block">'
        + f'<div><span class="ssr-score-value">{dps_str}</span>'
        + '<span class="ssr-score-of">/100</span></div>'
        + '<div class="ssr-score-label">Destructive Power Score</div>'
        + (f'<div class="ssr-score-rating">{rating}</div>' if rating else "")
        + "</div>"
        + f'<div class="ssr-body">{prose_html}{stats_html}{cta_html}</div>'
        + "</div>"
        + "</section>"
    )


def _category_word(cat: Optional[int]) -> str:
    if not isinstance(cat, int) or cat < 1:
        return "Tropical Storm"
    return f"Category {cat}"


def _storm_breadcrumb_jsonld(storm_id: str, storm: dict, canonical: str) -> str:
    """Breadcrumb trail for the storm page: Home > Historic Storms > {Name}.
    Google uses this to render breadcrumbs in search results."""
    name = storm.get("name") or storm_id
    year = storm.get("year")
    label = f"{name} ({year})" if year else name
    payload = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "StormDPS", "item": _BASE_URL + "/"},
            {"@type": "ListItem", "position": 2, "name": "Historic Storms", "item": _BASE_URL + "/historic-storms"},
            {"@type": "ListItem", "position": 3, "name": label, "item": canonical},
        ],
    }
    return (
        '<script type="application/ld+json">'
        + json.dumps(payload, separators=(",", ":"))
        + "</script>"
    )


def _storm_article_jsonld(storm_id: str, storm: dict, canonical: str) -> str:
    """Build Article schema for one storm — gives Google rich-result context."""
    name = storm.get("name") or storm_id
    year = storm.get("year")
    dps = storm.get("dps")
    label = storm.get("dps_label", "")
    cat = storm.get("category_lifetime") or storm.get("category")
    basin_name = storm.get("basin_name") or ""
    peak_wind_kt = storm.get("peak_wind_kt")
    min_pressure = storm.get("min_pressure_hpa")

    dps_str = f"{dps:.0f}" if isinstance(dps, (int, float)) else "—"
    pieces = [f"Destructive Power Score: {dps_str}/100"]
    if label:
        pieces.append(f"rated {label}")
    if isinstance(cat, int) and cat >= 1:
        pieces.append(f"peak Category {cat}")
    if peak_wind_kt:
        pieces.append(f"peak winds {round(peak_wind_kt)} kt")
    if min_pressure:
        pieces.append(f"minimum pressure {round(min_pressure)} hPa")
    if basin_name:
        pieces.append(f"in the {basin_name}")
    headline = (
        f"Hurricane {name} ({year}) — DPS {dps_str}/100"
        if year and dps_str != "—"
        else f"Storm {name} — StormDPS profile"
    )
    description = " · ".join(pieces) + "."

    payload = {
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": headline,
        "description": description,
        "url": canonical,
        "datePublished": "2026-01-01",
        "dateModified": datetime.utcnow().strftime("%Y-%m-%d"),
        "author": {"@type": "Organization", "name": "StormDPS"},
        "publisher": {
            "@type": "Organization",
            "name": "StormDPS",
            "url": _BASE_URL,
        },
        "about": {
            "@type": "Event",
            "name": f"Hurricane {name}" if year is None else f"Hurricane {name} ({year})",
        },
        "mainEntityOfPage": canonical,
        "image": f"{_BASE_URL}/frontend/logo-512.png",
    }
    return (
        '<script type="application/ld+json">'
        + json.dumps(payload, separators=(",", ":"))
        + "</script>"
    )


def render_storm_page(storm_id: str) -> str:
    """Return SSR'd index.html with per-storm meta tags. If the storm is
    unknown, falls back to generic-but-better-than-nothing meta."""
    template = _read_index_template()
    if not template:
        return ""

    safe_id = re.sub(r"[^A-Za-z0-9_-]", "", storm_id or "")[:32]
    canonical = f"{_BASE_URL}/storm/{safe_id}"

    storm = lookup_storm(safe_id)
    if storm:
        name = storm.get("name") or safe_id
        year = storm.get("year")
        dps = storm.get("dps")
        label = storm.get("dps_label", "")
        cat = storm.get("category_lifetime") or storm.get("category")

        dps_str = f"{dps:.0f}" if isinstance(dps, (int, float)) else "—"
        year_str = f" ({year})" if year else ""
        cat_str = _category_word(cat) if cat else ""
        title = (
            f"Hurricane {name}{year_str} — DPS {dps_str}/100 "
            f"{('· ' + cat_str) if cat_str else ''}| StormDPS"
        ).strip()
        description = (
            f"Hurricane {name}{year_str} scored {dps_str}/100 on the "
            f"Destructive Power Score scale"
            + (f" ({label})" if label else "")
            + ". A modern alternative to Saffir-Simpson that accounts for "
            "storm size, surge potential, duration, and geographic reach."
        )
        og_title = f"Hurricane {name}{year_str} — DPS {dps_str}/100"
        article_jsonld = _storm_article_jsonld(safe_id, storm, canonical) + _storm_breadcrumb_jsonld(safe_id, storm, canonical)
    else:
        title = f"Storm {safe_id} — StormDPS Destructive Power Score"
        description = (
            f"Destructive Power Score profile for storm {safe_id}. "
            "An improved 0–100 alternative to the Saffir-Simpson scale."
        )
        og_title = title
        article_jsonld = ""

    title_e = html.escape(title)
    description_e = html.escape(description, quote=True)
    og_title_e = html.escape(og_title, quote=True)
    canonical_e = html.escape(canonical, quote=True)
    initial_storm_e = html.escape(safe_id, quote=True)

    out = template
    out = _RE_TITLE.sub(f"<title>{title_e}</title>", out, count=1)
    out = _RE_DESCRIPTION.sub(
        f'<meta name="description" content="{description_e}">', out, count=1
    )
    out = _RE_CANONICAL.sub(
        f'<link rel="canonical" href="{canonical_e}">', out, count=1
    )
    out = _RE_OG_TITLE.sub(
        f'<meta property="og:title" content="{og_title_e}">', out, count=1
    )
    out = _RE_OG_DESCRIPTION.sub(
        f'<meta property="og:description" content="{description_e}">', out, count=1
    )
    out = _RE_OG_URL.sub(
        f'<meta property="og:url" content="{canonical_e}">', out, count=1
    )
    out = _RE_TWITTER_TITLE.sub(
        f'<meta name="twitter:title" content="{og_title_e}">', out, count=1
    )
    out = _RE_TWITTER_DESCRIPTION.sub(
        f'<meta name="twitter:description" content="{description_e}">', out, count=1
    )

    # Inject the per-storm Article JSON-LD and the initial-storm hint
    # immediately before </head> so the SPA picks it up on startup.
    inject = (
        article_jsonld
        + f'<script>window.__INITIAL_STORM_ID={json.dumps(safe_id)};</script>'
    )
    out = _RE_HEAD_END.sub(inject + "</head>", out, count=1)

    # Inject the visible storm-summary card at the SSR marker. Real H1 +
    # prose + stats that the crawler reads before any JS runs. The SPA
    # hides this once the interactive stats row hydrates (see below).
    if storm:
        summary_html = _build_storm_summary_html(safe_id, storm, canonical)
        out = _RE_SSR_CONTENT_MARKER.sub(summary_html, out, count=1)

    return out
