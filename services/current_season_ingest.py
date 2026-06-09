"""
Auto-ingest the current NHC season (Atlantic + East Pacific) into the catalog.

Why this exists
---------------
The global catalog is built from IBTrACS (quality-controlled, but lags the
in-season NA/EP storms by *months*) plus a manual ``custom_storms.csv``. The
live active-storms feed only knows about storms that are *currently* active.
So a storm that has already dissipated this season — e.g. the first East
Pacific storm before later ones form — falls into a blind spot: its track is
fetchable on demand by ATCF id, but it has no catalog row, so it has no name
and can't be browsed or searched.

This module closes that gap automatically. It reads NHC's best-track working
directory (``atcf/btk/``), which holds every current-season b-deck, derives a
lightweight catalog row (id, name, year, basin, peak wind, category) for each
*named* storm, and hands them back so the catalog builder can merge them just
like the manual custom storms — no hand-editing a CSV every time a storm forms.

Everything is fail-open: any network or parse error yields an empty list so the
catalog never degrades because NHC had a hiccup. The caller is expected to
*not* overwrite a previously-good season file with an empty result.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Optional

import httpx

from services.atcf_bdeck_client import ATCFBDeckClient

logger = logging.getLogger(__name__)

# NHC best-track working directory — holds the current season's b-deck files
# (bal012026.dat, bep012026.dat, ...). This is the same host the b-deck client
# already pulls individual storms from.
_BTK_INDEX_URL = "https://ftp.nhc.noaa.gov/atcf/btk/"

# ATCF basin code -> catalog basin code. NHC issues AL (Atlantic) + EP (East
# Pacific). The catalog/browser uses "NA" for the Atlantic, "EP" for East Pac.
_ATCF_TO_CATALOG_BASIN = {"AL": "NA", "EP": "EP"}

# b{basin}{nn}{yyyy}.dat for the NHC basins only.
_BDECK_RE = re.compile(r"b(al|ep)(\d{2})(\d{4})\.dat", re.IGNORECASE)

# Storm numbers >= 90 are invests / genesis areas, not named systems — skip.
_INVEST_THRESHOLD = 90
# Sub-tropical-storm peaks are filtered out, matching the custom-storm loader.
_MIN_TS_KT = 34
_KT_PER_MS = 1.94384


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


async def _list_season_atcf_ids(http: httpx.AsyncClient, year: int) -> list[str]:
    """Return ATCF ids (e.g. ``EP012026``) for *year*'s AL/EP storms from the
    NHC btk index, excluding invests. Empty list on any failure."""
    try:
        resp = await http.get(_BTK_INDEX_URL, timeout=20.0)
    except Exception:
        logger.info("[SEASON] btk index fetch failed", exc_info=True)
        return []

    if resp.status_code != 200:
        logger.info("[SEASON] btk index -> HTTP %s", resp.status_code)
        return []

    ids: list[str] = []
    seen: set[str] = set()
    for m in _BDECK_RE.finditer(resp.text or ""):
        basin, nn, yyyy = m.group(1).upper(), m.group(2), m.group(3)
        if yyyy != str(year):
            continue
        if int(nn) >= _INVEST_THRESHOLD:
            continue
        aid = f"{basin}{nn}{yyyy}"
        if aid not in seen:
            seen.add(aid)
            ids.append(aid)
    return sorted(ids)


async def fetch_current_season_storms(
    http_client: Optional[httpx.AsyncClient] = None,
    year: Optional[int] = None,
) -> list[dict]:
    """Build catalog rows for the current NHC season (AL + EP).

    Returns a list of ``{id, name, year, basin, peak_wind_kt, category,
    source}`` dicts — the same shape ``_load_custom_storms`` produces — or an
    empty list on any failure (fail-open). Pass the app's shared
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
        atcf_ids = await _list_season_atcf_ids(http, year)
        if not atcf_ids:
            logger.info("[SEASON] no current-season AL/EP storms found for %s", year)
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

                # The b-deck parser falls back to the id when a storm is
                # unnamed (still an invest / "ONE-E"); skip those — there's no
                # point cataloging "EP012026" under its own id.
                name = (snaps[-1].name or "").strip()
                if not name or name.upper() == aid.upper():
                    continue

                peak_ms = max((s.max_wind_ms or 0.0) for s in snaps)
                peak_kt = round(peak_ms * _KT_PER_MS)
                if peak_kt < _MIN_TS_KT:
                    continue

                entries.append({
                    "id": aid,
                    "name": name.title(),
                    "year": year,
                    "basin": _ATCF_TO_CATALOG_BASIN.get(aid[:2].upper(), aid[:2].upper()),
                    "peak_wind_kt": peak_kt,
                    "category": _saffir_category(peak_kt),
                    "source": "nhc-current",
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
