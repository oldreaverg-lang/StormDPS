"""
USGS STN (Short-Term Network) client — high water marks for flood events.

The USGS Short-Term Network deploys temporary sensors and field teams to
measure actual peak flood levels during and after a storm. The results
are published at:

    https://stn.wim.usgs.gov/STNServices/

as a REST/JSON API. The key endpoints are:

  * /Events                          — list all flood events
  * /Events/{event_id}/HWMs          — high water marks for an event
  * /Events/{event_id}/Sensors       — deployed sensors and their peaks
  * /HWMs/FilteredHWMs               — query HWMs by state/event/date

High water marks from STN are the definitive post-event record of how
high water actually got — they're what NOAA and FEMA use for damage
assessments after a storm.

For StormDPS we use STN to:
  1. Look up the peak HWM during a known storm event.
  2. Show the top-N highest HWMs on the map as reference markers.
  3. Compute an average HWM by county for calibration.

Reference:
  https://stn.wim.usgs.gov/STNDataPortal/
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

STN_BASE = "https://stn.wim.usgs.gov/STNServices"


@dataclass
class HighWaterMark:
    hwm_id: int
    event_id: int
    event_name: str
    latitude: float
    longitude: float
    elev_ft: Optional[float]         # elevation of the mark above ground
    hwm_quality: Optional[str]       # Excellent / Good / Fair / Poor
    hwm_environment: Optional[str]   # Coastal / Riverine
    location_description: Optional[str]
    state: Optional[str]
    county: Optional[str]
    marker_date: Optional[str]


@dataclass
class STNEvent:
    event_id: int
    event_name: str
    event_type: Optional[str]
    event_start: Optional[str]
    event_end: Optional[str]
    state: Optional[str]


class USGSSTNClient:
    def __init__(self, timeout: float = 30.0):
        self._timeout = timeout
        self._events_cache: Optional[list[STNEvent]] = None

    async def _get_json(self, client: httpx.AsyncClient, path: str, params: Optional[dict] = None) -> Optional[list]:
        url = STN_BASE + path
        try:
            r = await client.get(url, params=params or {})
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                return data
            return [data]
        except httpx.HTTPError as e:
            logger.warning(f"[STN] {path} failed: {e}")
            return None
        except ValueError as e:
            logger.warning(f"[STN] {path} json parse failed: {e}")
            return None

    async def list_events(self, force: bool = False) -> list[STNEvent]:
        if self._events_cache is not None and not force:
            return self._events_cache
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            raw = await self._get_json(client, "/Events.json")
        if not raw:
            return []
        out: list[STNEvent] = []
        for e in raw:
            if not isinstance(e, dict):
                continue
            out.append(
                STNEvent(
                    event_id=int(e.get("event_id") or 0),
                    event_name=(e.get("event_name") or "").strip(),
                    event_type=e.get("event_type"),
                    event_start=e.get("event_start_date"),
                    event_end=e.get("event_end_date"),
                    state=e.get("state"),
                )
            )
        self._events_cache = out
        return out

    async def find_event(self, name: str, year: Optional[int] = None) -> Optional[STNEvent]:
        """Find a STN event by fuzzy name match (and optionally year)."""
        events = await self.list_events()
        needle = name.upper().strip()
        matches = []
        for e in events:
            ename = e.event_name.upper()
            if needle in ename or ename in needle:
                if year and e.event_start and not e.event_start.startswith(str(year)):
                    continue
                matches.append(e)
        if not matches:
            return None
        # Prefer exact substring match over partial
        matches.sort(key=lambda e: (-len(set(needle) & set(e.event_name.upper())), e.event_id))
        return matches[0]

    async def get_hwms_for_event(self, event_id: int, limit: int = 500) -> list[HighWaterMark]:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            raw = await self._get_json(client, f"/Events/{event_id}/HWMs.json")
        if not raw:
            return []
        out: list[HighWaterMark] = []
        for h in raw[:limit]:
            if not isinstance(h, dict):
                continue
            try:
                lat = float(h.get("latitude_dd") or h.get("latitude") or 0)
                lon = float(h.get("longitude_dd") or h.get("longitude") or 0)
            except (TypeError, ValueError):
                continue
            try:
                elev = float(h.get("elev_ft") or h.get("height_above_gnd") or 0) or None
            except (TypeError, ValueError):
                elev = None
            out.append(
                HighWaterMark(
                    hwm_id=int(h.get("hwm_id") or 0),
                    event_id=event_id,
                    event_name=(h.get("event_name") or "").strip(),
                    latitude=lat,
                    longitude=lon,
                    elev_ft=elev,
                    hwm_quality=h.get("hwm_quality_name") or h.get("hwm_quality"),
                    hwm_environment=h.get("hwm_environment"),
                    location_description=h.get("hwm_locationdescription") or h.get("description"),
                    state=h.get("stateName") or h.get("state"),
                    county=h.get("countyName") or h.get("county"),
                    marker_date=h.get("flag_date") or h.get("survey_date"),
                )
            )
        return out

    async def peak_hwm_for_storm(
        self,
        storm_name: str,
        year: Optional[int] = None,
    ) -> Optional[HighWaterMark]:
        """
        Return the single highest high-water mark recorded for a named storm.
        """
        event = await self.find_event(storm_name, year)
        if event is None:
            return None
        hwms = await self.get_hwms_for_event(event.event_id, limit=1000)
        if not hwms:
            return None
        scored = [h for h in hwms if h.elev_ft is not None]
        if not scored:
            return None
        return max(scored, key=lambda h: h.elev_ft or 0.0)

    async def top_hwms_for_storm(
        self,
        storm_name: str,
        year: Optional[int] = None,
        n: int = 10,
    ) -> list[HighWaterMark]:
        event = await self.find_event(storm_name, year)
        if event is None:
            return []
        hwms = await self.get_hwms_for_event(event.event_id, limit=2000)
        hwms = [h for h in hwms if h.elev_ft is not None]
        hwms.sort(key=lambda h: h.elev_ft or 0.0, reverse=True)
        return hwms[:n]
