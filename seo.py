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


def _category_word(cat: Optional[int]) -> str:
    if not isinstance(cat, int) or cat < 1:
        return "Tropical Storm"
    return f"Category {cat}"


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
        article_jsonld = _storm_article_jsonld(safe_id, storm, canonical)
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

    return out
