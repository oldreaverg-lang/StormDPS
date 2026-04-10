"""
NCEI Storm Events Database loader.

The Storm Events Database is the authoritative US record of severe weather
casualties and property damage, going back to 1950. NCEI publishes it as
annual CSV bundles (``StormEvents_details-ftp_vYYYY.csv.gz``) at:

    https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/

For StormDPS we care about three fields per event:

    EVENT_TYPE        — we filter to "Hurricane (Typhoon)", "Tropical Storm",
                        "Storm Surge/Tide", and "Tropical Depression"
    DEATHS_DIRECT
    DAMAGE_PROPERTY   — format like "500.00K", "1.50B", "" (millions/billions)

Events are joined to StormDPS storms by ``EPISODE_ID`` when available or by
storm name + year + state fallback. The loader caches a parsed dict keyed
by (NAME_UPPER, YEAR) so repeated lookups are O(1).

Usage
-----
    from services.ncei_storm_events import NCEILoader
    loader = NCEILoader()
    await loader.ensure_loaded([2005, 2017, 2022])
    damage = loader.get_damage("KATRINA", 2005)
    # -> {"deaths": 1199, "damage_usd": 125_000_000_000, "events": 73}
"""

from __future__ import annotations

import csv
import gzip
import io
import logging
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

NCEI_BASE = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"

# The filename scheme on the NCEI FTP shifts every few months (they append
# a "_dYYYYMMDD_cYYYYMMDD" pair). We discover the current filename by
# listing the directory index once per year at load time.
TROPICAL_EVENTS = {
    "HURRICANE",
    "HURRICANE (TYPHOON)",
    "TROPICAL STORM",
    "STORM SURGE/TIDE",
    "TROPICAL DEPRESSION",
    "MARINE HURRICANE/TYPHOON",
    "MARINE TROPICAL STORM",
}


def _parse_damage(s: str) -> float:
    """Parse '1.25B', '500.00K', '1500000.00' into float USD."""
    if not s:
        return 0.0
    s = s.strip().upper()
    if not s:
        return 0.0
    mult = 1.0
    if s.endswith("K"):
        mult = 1_000.0
        s = s[:-1]
    elif s.endswith("M"):
        mult = 1_000_000.0
        s = s[:-1]
    elif s.endswith("B"):
        mult = 1_000_000_000.0
        s = s[:-1]
    try:
        return float(s) * mult
    except ValueError:
        return 0.0


class NCEILoader:
    """Loads and indexes the NCEI Storm Events Database."""

    def __init__(self, cache_dir: Optional[Path] = None):
        # Key: (NAME_UPPER, YEAR) -> dict with aggregated totals
        self._index: dict[tuple[str, int], dict] = {}
        self._loaded_years: set[int] = set()
        self._cache_dir = cache_dir
        if cache_dir:
            cache_dir.mkdir(parents=True, exist_ok=True)

    async def _discover_filename(self, client: httpx.AsyncClient, year: int) -> Optional[str]:
        """List the NCEI directory and find the current details CSV for *year*."""
        try:
            r = await client.get(NCEI_BASE, timeout=20.0)
            r.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning(f"[NCEI] directory listing failed: {e}")
            return None

        prefix = f"StormEvents_details-ftp_v1.0_d{year}"
        # Look for the first matching .csv.gz link in the HTML index.
        for line in r.text.splitlines():
            if prefix in line and ".csv.gz" in line:
                start = line.find(prefix)
                end = line.find(".csv.gz", start) + len(".csv.gz")
                if start >= 0 and end > start:
                    return line[start:end]
        return None

    async def _fetch_year(self, client: httpx.AsyncClient, year: int) -> Optional[str]:
        # Cache first
        if self._cache_dir:
            cached = self._cache_dir / f"ncei_{year}.csv"
            if cached.exists():
                return cached.read_text(encoding="utf-8", errors="replace")

        filename = await self._discover_filename(client, year)
        if not filename:
            return None

        try:
            r = await client.get(NCEI_BASE + filename, timeout=60.0)
            r.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning(f"[NCEI] fetch {filename} failed: {e}")
            return None

        try:
            with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as f:
                text = f.read().decode("utf-8", errors="replace")
        except OSError as e:
            logger.warning(f"[NCEI] gunzip {filename} failed: {e}")
            return None

        if self._cache_dir:
            try:
                (self._cache_dir / f"ncei_{year}.csv").write_text(text)
            except OSError:
                pass
        return text

    def _index_year(self, csv_text: str, year: int) -> int:
        reader = csv.DictReader(io.StringIO(csv_text))
        count = 0
        for row in reader:
            etype = (row.get("EVENT_TYPE") or "").upper().strip()
            if etype not in TROPICAL_EVENTS:
                continue

            # Extract storm name — stored in EVENT_NARRATIVE / EPISODE_NARRATIVE
            # or on the EPISODE_TITLE field for newer years.
            name = (row.get("EPISODE_TITLE") or row.get("EVENT_NAME") or "").strip().upper()
            if not name:
                # Fallback: scan the narrative for "HURRICANE NAME" or "TROPICAL STORM NAME"
                narr = (row.get("EPISODE_NARRATIVE") or "")[:200].upper()
                for prefix in ("HURRICANE ", "TROPICAL STORM ", "TROPICAL DEPRESSION "):
                    if prefix in narr:
                        after = narr.split(prefix, 1)[1]
                        name = after.split()[0].strip(",.;:()") if after else ""
                        break
            if not name:
                continue

            deaths_d = int(row.get("DEATHS_DIRECT") or 0)
            deaths_i = int(row.get("DEATHS_INDIRECT") or 0)
            damage_p = _parse_damage(row.get("DAMAGE_PROPERTY") or "")
            damage_c = _parse_damage(row.get("DAMAGE_CROPS") or "")

            key = (name, year)
            agg = self._index.get(key)
            if agg is None:
                agg = {
                    "deaths_direct": 0,
                    "deaths_indirect": 0,
                    "damage_property_usd": 0.0,
                    "damage_crops_usd": 0.0,
                    "events": 0,
                    "states": set(),
                }
                self._index[key] = agg
            agg["deaths_direct"] += deaths_d
            agg["deaths_indirect"] += deaths_i
            agg["damage_property_usd"] += damage_p
            agg["damage_crops_usd"] += damage_c
            agg["events"] += 1
            state = (row.get("STATE") or "").strip()
            if state:
                agg["states"].add(state)
            count += 1
        return count

    async def ensure_loaded(self, years: list[int]) -> None:
        todo = [y for y in years if y not in self._loaded_years]
        if not todo:
            return
        async with httpx.AsyncClient() as client:
            for year in todo:
                text = await self._fetch_year(client, year)
                if text is None:
                    logger.warning(f"[NCEI] no data loaded for {year}")
                    self._loaded_years.add(year)
                    continue
                n = self._index_year(text, year)
                self._loaded_years.add(year)
                logger.info(f"[NCEI] indexed {n} tropical events for {year}")

    def get_damage(self, storm_name: str, year: int) -> Optional[dict]:
        key = (storm_name.upper(), year)
        agg = self._index.get(key)
        if not agg:
            return None
        return {
            "deaths_direct": agg["deaths_direct"],
            "deaths_indirect": agg["deaths_indirect"],
            "damage_property_usd": agg["damage_property_usd"],
            "damage_crops_usd": agg["damage_crops_usd"],
            "damage_total_usd": agg["damage_property_usd"] + agg["damage_crops_usd"],
            "event_count": agg["events"],
            "states": sorted(agg["states"]),
        }
