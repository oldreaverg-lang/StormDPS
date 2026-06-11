"""
Auto-ingest the current season into the catalog — every basin, real data only.

Why this exists
---------------
The global catalog is built from IBTrACS (quality-controlled, but lags the
in-season storms by *months*) plus a small manual ``custom_storms.csv``. The
live active-storms feed only knows about storms that are *currently* active. So
a storm that has already dissipated this season falls into a blind spot: its
track is fetchable on demand by ATCF id, but it has no catalog row, so it has
no name and can't be browsed or searched.

This module closes that gap automatically for every basin:

  * NHC (AL Atlantic, EP East Pacific): the best-track working directory
    ``ftp.nhc.noaa.gov/atcf/btk/``.
  * JTWC (WP West Pacific, IO North Indian, SH Southern Hemisphere): the UCAR
    RAL realtime mirror, one listable directory per region.

Both are Apache autoindexes of per-storm files/folders. For each *named* storm
we derive a lightweight catalog row (id, name, year, basin, peak wind,
category) via the shared b-deck client and hand them back so the catalog
builder merges them like the manual custom storms — no hand-editing a CSV, and
no fabricated placeholder data.

Everything is fail-open: a network/parse error for one source yields an empty
contribution from that source so the catalog never degrades because an upstream
had a hiccup. The caller must *not* overwrite a previously-good season file
with an empty result.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Optional

import httpx

from services.atcf_bdeck_client import ATCFBDeckClient

logger = logging.getLogger(__name__)

# --- NHC (Atlantic + East Pacific) ------------------------------------------
_BTK_INDEX_URL = "https://ftp.nhc.noaa.gov/atcf/btk/"
# b{basin}{nn}{yyyy}.dat for the NHC basins only.
_BDECK_RE = re.compile(r"b(al|ep)(\d{2})(\d{4})\.dat", re.IGNORECASE)

# --- JTWC (UCAR RAL realtime mirror) ----------------------------------------
# Each region dir is a listable Apache autoindex of per-storm folders named
# {prefix}{nn}{year}; (region, ATCF prefix) mirrors atcf_bdeck_client's map.
_JTWC_INDEX_BASE = "https://hurricanes.ral.ucar.edu/realtime/plots/"
_JTWC_REGIONS = [
    ("northwestpacific", "WP"),
    ("northindian", "IO"),
    ("southernhemisphere", "SH"),
]

# ATCF prefix -> catalog basin code. SH is split into SI/SP by longitude below.
_PREFIX_TO_BASIN = {"AL": "NA", "EP": "EP", "WP": "WP", "IO": "NI"}

_INVEST_THRESHOLD = 90   # storm numbers >= 90 are invests / genesis areas
_MIN_TS_KT = 34          # filter sub-TS, matching the custom-storm loader
_KT_PER_MS = 1.94384
# Opaque marker for "auto-ingested current-season storm" — covers JTWC too.
# Kept as "nhc-current" for back-compat with the catalog republish dedup and
# the frontend chip ("not yet active") logic, which key off this exact string.
_SOURCE = "nhc-current"


# Unnamed systems carry a spelled-out number as their "name" in the b-deck
# (ONE, SIX, TWENTY-FIVE, ...), often truncated by the fixed-width field
# (TWENTY-FI). Treat any name whose tokens are all prefixes of a number word
# as unnamed and skip it — we only catalog properly named storms.
_NUMWORDS = (
    "ONE TWO THREE FOUR FIVE SIX SEVEN EIGHT NINE TEN ELEVEN TWELVE THIRTEEN "
    "FOURTEEN FIFTEEN SIXTEEN SEVENTEEN EIGHTEEN NINETEEN TWENTY THIRTY FORTY "
    "FIFTY SIXTY"
).split()


def _is_number_name(name: str) -> bool:
    toks = name.upper().replace("-", " ").split()
    return bool(toks) and all(
        any(w.startswith(t) for w in _NUMWORDS) for t in toks
    )


def _saffir_category(peak_kt: int) -> int:
    if peak_kt >= 137:
        return 5
    if peak_kt >= 113:
        return 4
    if peak_kt >= 96:
        return 3
    if peak_kt >= 83:
        return 2
    if peak_kt >= 64:
        return 1
    return 0


def _catalog_basin(prefix: str, snaps) -> str:
    """Catalog basin code for an ATCF prefix. JTWC's Southern Hemisphere (SH)
    is one ATCF basin, but the catalog/browser splits it into South Indian
    (SI) and South Pacific (SP) — assign by a representative track longitude
    (signed degrees; SI ≈ 20°E–135°E per the JTWC/BoM boundary, else SP)."""
    if prefix != "SH":
        return _PREFIX_TO_BASIN.get(prefix, prefix)
    lons = [s.lon for s in snaps if getattr(s, "lon", None) is not None]
    lon = lons[len(lons) // 2] if lons else 0.0   # mid-track point
    return "SI" if 20.0 <= lon < 135.0 else "SP"


async def _list_nhc_ids(http: httpx.AsyncClient, year: int) -> list[str]:
    """ATCF ids (EP012026, AL012026, ...) for *year*'s NHC storms from the btk
    index, excluding invests. Empty list on any failure (fail-open)."""
    try:
        resp = await http.get(_BTK_INDEX_URL, timeout=20.0)
    except Exception:
        logger.info("[SEASON] NHC btk index fetch failed", exc_info=True)
        return []
    if resp.status_code != 200:
        logger.info("[SEASON] NHC btk index -> HTTP %s", resp.status_code)
        return []
    ids: list[str] = []
    seen: set[str] = set()
    for m in _BDECK_RE.finditer(resp.text or ""):
        basin, nn, yyyy = m.group(1).upper(), m.group(2), m.group(3)
        if yyyy != str(year) or int(nn) >= _INVEST_THRESHOLD:
            continue
        aid = f"{basin}{nn}{yyyy}"
        if aid not in seen:
            seen.add(aid)
            ids.append(aid)
    return ids


async def _list_jtwc_ids(http: httpx.AsyncClient, year: int) -> list[str]:
    """ATCF ids (WP012026, IO012026, SH012026, ...) for *year*'s JTWC storms
    from the UCAR realtime region indexes, excluding invests. Per-region
    fail-open — one region's failure doesn't drop the others."""
    ids: list[str] = []
    seen: set[str] = set()
    for region, prefix in _JTWC_REGIONS:
        url = f"{_JTWC_INDEX_BASE}{region}/{year}/"
        try:
            resp = await http.get(url, timeout=20.0)
        except Exception:
            logger.info("[SEASON] JTWC index fetch failed: %s", url, exc_info=True)
            continue
        if resp.status_code != 200:
            logger.info("[SEASON] JTWC index %s -> HTTP %s", url, resp.status_code)
            continue
        rx = re.compile(rf"{prefix.lower()}(\d{{2}}){year}", re.IGNORECASE)
        for m in rx.finditer(resp.text or ""):
            nn = m.group(1)
            if int(nn) >= _INVEST_THRESHOLD:
                continue
            aid = f"{prefix}{nn}{year}"
            if aid not in seen:
                seen.add(aid)
                ids.append(aid)
    return ids


async def fetch_current_season_storms(
    http_client: Optional[httpx.AsyncClient] = None,
    year: Optional[int] = None,
) -> list[dict]:
    """Build catalog rows for the current season across every basin (NHC +
    JTWC).

    Returns a list of ``{id, name, year, basin, peak_wind_kt, category,
    source}`` dicts — the same shape ``_load_custom_storms`` produces — or an
    empty list on total failure (fail-open). Pass the app's shared
    ``httpx.AsyncClient`` to reuse its connection pool.
    """
    year = year or datetime.now(timezone.utc).year
    own_client = http_client is None
    http = http_client or httpx.AsyncClient(
        headers={"User-Agent": "StormDPS/1.0 (research)"},
        follow_redirects=True,
        timeout=30.0,
    )
    try:
        nhc_ids = await _list_nhc_ids(http, year)
        jtwc_ids = await _list_jtwc_ids(http, year)
        atcf_ids = nhc_ids + jtwc_ids
        if not atcf_ids:
            logger.info("[SEASON] no current-season storms found for %s", year)
            return []

        entries: list[dict] = []
        async with ATCFBDeckClient(http_client=http) as bdeck:
            for aid in atcf_ids:
                try:
                    snaps = await bdeck.get_storm_track(aid)
                except Exception:
                    logger.info("[SEASON] b-deck fetch failed for %s", aid, exc_info=True)
                    continue
                if not snaps:
                    continue

                # The b-deck parser falls back to the id when a storm is unnamed
                # (still an invest / "ONE"); skip those — no point cataloging a
                # storm under its own id.
                name = (snaps[-1].name or "").strip()
                if not name or name.upper() == aid.upper() or _is_number_name(name):
                    continue

                peak_ms = max((s.max_wind_ms or 0.0) for s in snaps)
                peak_kt = round(peak_ms * _KT_PER_MS)
                if peak_kt < _MIN_TS_KT:
                    continue

                entries.append({
                    "id": aid,
                    "name": name.title(),
                    "year": year,
                    "basin": _catalog_basin(aid[:2].upper(), snaps),
                    "peak_wind_kt": peak_kt,
                    "category": _saffir_category(peak_kt),
                    "source": _SOURCE,
                })

        logger.info(
            "[SEASON] ingested %d current-season storm(s): %s",
            len(entries),
            ", ".join(e["id"] for e in entries) or "none",
        )
        return entries
    except Exception:
        logger.exception("[SEASON] current-season ingest failed")
        return []
    finally:
        if own_client:
            await http.aclose()
