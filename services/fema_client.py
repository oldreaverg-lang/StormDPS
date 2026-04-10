"""
OpenFEMA client — federal disaster declarations for tropical cyclones.

OpenFEMA is free, requires no API key, and returns JSON. The relevant
dataset is **DisasterDeclarationsSummariesV2**, which lists every federally
declared disaster since 1953 with county-level records.

For StormDPS we filter to ``incidentType IN ("Hurricane", "Tropical Storm")``
and aggregate by (storm name, year) to produce:

    counties_declared     — number of unique county declarations
    states                — unique states with declarations
    declaration_date      — earliest federal declaration for this storm
    major_disaster        — whether a "DR" (major disaster) declaration exists

This runs off a public HTTPS endpoint and a cached local JSON file so
subsequent requests don't hit FEMA.

API reference
-------------
https://www.fema.gov/openfema-data-page/disaster-declarations-summaries-v2
"""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

OPENFEMA_BASE = (
    "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
)


class FEMAClient:
    def __init__(self, cache_dir: Optional[Path] = None, timeout: float = 30.0):
        self._cache_dir = cache_dir
        self._timeout = timeout
        self._index: dict[tuple[str, int], dict] = {}
        self._loaded = False
        if cache_dir:
            cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self) -> Optional[Path]:
        return self._cache_dir / "fema_tropical_declarations.json" if self._cache_dir else None

    async def _fetch_all_tropical(self) -> list[dict]:
        """
        Fetch every hurricane/TS declaration. OpenFEMA paginates at 1000
        records by default; this pulls all pages.
        """
        records: list[dict] = []
        page = 0
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            while True:
                params = {
                    "$filter": (
                        "incidentType eq 'Hurricane' or "
                        "incidentType eq 'Tropical Storm'"
                    ),
                    "$skip": page * 1000,
                    "$top": 1000,
                    "$format": "json",
                    "$orderby": "declarationDate",
                }
                try:
                    r = await client.get(OPENFEMA_BASE, params=params)
                    r.raise_for_status()
                except httpx.HTTPError as e:
                    logger.warning(f"[FEMA] page {page} fetch failed: {e}")
                    break

                data = r.json()
                chunk = data.get("DisasterDeclarationsSummaries", [])
                if not chunk:
                    break
                records.extend(chunk)
                if len(chunk) < 1000:
                    break
                page += 1
                if page > 50:  # hard safety cap — ~50k records
                    break
        return records

    @staticmethod
    def _extract_storm_name(title: str) -> Optional[str]:
        """
        OpenFEMA titles look like:
            "Hurricane Ian"
            "Hurricane Katrina"
            "Tropical Storm Harvey"
            "Hurricane Irma Flooding"
        """
        if not title:
            return None
        m = re.search(
            r"(?:HURRICANE|TROPICAL STORM|TROPICAL DEPRESSION|TYPHOON)\s+([A-Z][A-Za-z]+)",
            title,
            re.IGNORECASE,
        )
        if m:
            return m.group(1).upper()
        return None

    def _build_index(self, records: list[dict]) -> None:
        buckets: dict[tuple[str, int], dict] = {}
        for r in records:
            title = r.get("declarationTitle") or r.get("incidentTitle") or ""
            name = self._extract_storm_name(title)
            if not name:
                continue
            year = None
            begin = r.get("incidentBeginDate") or r.get("declarationDate") or ""
            if begin and len(begin) >= 4:
                try:
                    year = int(begin[:4])
                except ValueError:
                    year = None
            if year is None:
                continue
            key = (name, year)

            agg = buckets.get(key)
            if agg is None:
                agg = {
                    "name": name,
                    "year": year,
                    "counties": set(),
                    "states": set(),
                    "declaration_types": set(),
                    "declaration_dates": [],
                    "titles": set(),
                    "ih_program": False,   # individual assistance
                    "ia_program": False,
                    "pa_program": False,
                }
                buckets[key] = agg

            fips = r.get("placeCode") or ""
            state = r.get("state") or ""
            if fips:
                agg["counties"].add(fips)
            if state:
                agg["states"].add(state)
            dtype = r.get("declarationType") or ""
            if dtype:
                agg["declaration_types"].add(dtype)
            ddate = r.get("declarationDate")
            if ddate:
                agg["declaration_dates"].append(ddate)
            if title:
                agg["titles"].add(title)
            if r.get("ihProgramDeclared"):
                agg["ih_program"] = True
            if r.get("iaProgramDeclared"):
                agg["ia_program"] = True
            if r.get("paProgramDeclared"):
                agg["pa_program"] = True

        for key, agg in buckets.items():
            self._index[key] = {
                "name": agg["name"],
                "year": agg["year"],
                "counties_declared": len(agg["counties"]),
                "states": sorted(agg["states"]),
                "major_disaster": "DR" in agg["declaration_types"],
                "emergency": "EM" in agg["declaration_types"],
                "earliest_declaration": min(agg["declaration_dates"]) if agg["declaration_dates"] else None,
                "individual_assistance": agg["ia_program"],
                "public_assistance": agg["pa_program"],
                "title_sample": next(iter(agg["titles"]), ""),
            }

    async def load(self, force: bool = False) -> None:
        if self._loaded and not force:
            return
        cache = self._cache_path()
        if cache and cache.exists() and not force:
            try:
                raw = json.loads(cache.read_text())
                self._index = {
                    (k.split("|")[0], int(k.split("|")[1])): v
                    for k, v in raw.items()
                }
                self._loaded = True
                logger.info(f"[FEMA] loaded {len(self._index)} storms from cache")
                return
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                logger.debug(f"[FEMA] cache invalid, refetching: {e}")

        records = await self._fetch_all_tropical()
        self._build_index(records)
        self._loaded = True
        logger.info(f"[FEMA] indexed {len(self._index)} (storm, year) pairs from {len(records)} declarations")

        if cache:
            try:
                serializable = {
                    f"{k[0]}|{k[1]}": v for k, v in self._index.items()
                }
                cache.write_text(json.dumps(serializable, separators=(",", ":")))
            except OSError as e:
                logger.debug(f"[FEMA] cache write failed: {e}")

    def get_declaration(self, name: str, year: int) -> Optional[dict]:
        return self._index.get((name.upper(), year))
