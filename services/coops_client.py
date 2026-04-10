"""
NOAA CO-OPS Tides & Currents client.

Fetches observed peak water levels at NOS tide gauge stations during a given
storm window. This is the ground-truth layer for the DPS surge component —
instead of modeling surge from wind + pressure + forward speed, we can ask
CO-OPS what was actually observed at Waveland, Galveston, Atlantic City, etc.

No API key required. Free for public use. JSON responses.
Docs: https://api.tidesandcurrents.noaa.gov/api/prod/

Typical usage
-------------
    from services.coops_client import COOPSClient
    async with COOPSClient() as c:
        peak = await c.get_peak_water_level("8747437", "20050828", "20050830")
        # peak = {"station": "8747437", "peak_ft_mllw": 24.1, "peak_time": ...}

Returned water levels are in **feet above MLLW** by default — that matches
NOAA's own storm-tide reporting convention and the feet-based scale the
frontend already uses for surge. Meters are available via the ``units``
parameter if ever needed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

COOPS_API = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
APP_NAME = "StormDPS"  # Required by CO-OPS terms of service.

# Curated list of gauges along the US Gulf + Atlantic coast that are
# historically active during hurricanes. Keyed by coast region so we can
# narrow the search for a given storm instead of polling all ~200 gauges.
# (lat, lon, station_id, name, region)
HURRICANE_GAUGES: list[tuple[float, float, str, str, str]] = [
    # Gulf of Mexico
    (29.480, -94.393, "8771341", "Galveston Pier 21, TX", "TX"),
    (29.310, -94.725, "8771510", "Galveston Pleasure Pier, TX", "TX"),
    (29.367, -89.673, "8761305", "Shell Beach, LA", "LA"),
    (30.027, -90.113, "8761927", "New Canal Station, LA", "LA"),
    (29.868, -89.673, "8761724", "Grand Isle, LA", "LA"),
    (30.325, -88.564, "8741533", "Pascagoula NOAA Lab, MS", "MS"),
    (30.392, -88.778, "8740166", "Dauphin Island, AL", "AL"),
    (30.652, -87.907, "8729840", "Pensacola, FL", "FL-GULF"),
    (30.153, -85.667, "8729108", "Panama City, FL", "FL-GULF"),
    (29.143, -83.032, "8727520", "Cedar Key, FL", "FL-GULF"),
    (28.416, -82.663, "8726607", "Old Port Tampa, FL", "FL-GULF"),
    (27.761, -82.627, "8726520", "St. Petersburg, FL", "FL-GULF"),
    (26.132, -81.808, "8725110", "Naples, FL", "FL-GULF"),
    (25.732, -80.132, "8723214", "Virginia Key, FL", "FL-SE"),
    # Atlantic
    (24.555, -81.808, "8724580", "Key West, FL", "FL-KEYS"),
    (26.613, -80.034, "8722670", "Lake Worth Pier, FL", "FL-ATL"),
    (28.415, -80.593, "8721604", "Trident Pier Port Canaveral, FL", "FL-ATL"),
    (30.672, -81.465, "8720030", "Fernandina Beach, FL", "FL-ATL"),
    (32.034, -80.902, "8670870", "Fort Pulaski, GA", "GA"),
    (32.781, -79.925, "8665530", "Charleston, SC", "SC"),
    (34.213, -77.787, "8658120", "Wilmington, NC", "NC"),
    (35.213, -75.704, "8654467", "Hatteras, NC", "NC"),
    (36.947, -76.330, "8638610", "Sewells Point, VA", "VA"),
    (38.978, -74.960, "8534720", "Atlantic City, NJ", "NJ"),
    (40.467, -74.009, "8531680", "Sandy Hook, NJ", "NJ"),
    (40.701, -74.014, "8518750", "The Battery, NY", "NY"),
    (41.807, -71.401, "8452660", "Newport, RI", "RI"),
    (41.505, -71.326, "8447930", "Woods Hole, MA", "MA"),
]


@dataclass
class GaugeReading:
    station: str
    name: str
    lat: float
    lon: float
    peak_ft_mllw: float
    peak_time_utc: str  # ISO timestamp
    sample_count: int


class COOPSClient:
    """Thin async wrapper around the CO-OPS data getter."""

    def __init__(self, timeout: float = 15.0):
        self._client: Optional[httpx.AsyncClient] = None
        self._timeout = timeout

    async def __aenter__(self) -> "COOPSClient":
        self._client = httpx.AsyncClient(timeout=self._timeout)
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.aclose()

    async def _get(self, params: dict) -> dict:
        assert self._client is not None
        params = {**params, "application": APP_NAME, "format": "json"}
        r = await self._client.get(COOPS_API, params=params)
        r.raise_for_status()
        return r.json()

    async def get_peak_water_level(
        self,
        station: str,
        begin_yyyymmdd: str,
        end_yyyymmdd: str,
        datum: str = "MLLW",
    ) -> Optional[GaugeReading]:
        """
        Return the peak observed water level at *station* between the two
        dates (inclusive). Dates are ``YYYYMMDD`` strings (CO-OPS convention).

        Uses the ``water_level`` product for verified data when available,
        falling back to ``hourly_height`` if the verified product isn't yet
        published (CO-OPS verifies data on a ~30 day lag).
        """
        for product in ("water_level", "hourly_height"):
            try:
                data = await self._get({
                    "product": product,
                    "station": station,
                    "begin_date": begin_yyyymmdd,
                    "end_date": end_yyyymmdd,
                    "datum": datum,
                    "units": "english",
                    "time_zone": "gmt",
                })
            except httpx.HTTPError as e:
                logger.debug(f"[CO-OPS] {station} {product} HTTP error: {e}")
                continue

            if "error" in data or not data.get("data"):
                continue

            samples = data["data"]
            best = None
            for s in samples:
                try:
                    v = float(s.get("v", ""))
                except (ValueError, TypeError):
                    continue
                if best is None or v > best[0]:
                    best = (v, s.get("t", ""))

            if best is None:
                continue

            # Look up station metadata
            meta = _find_gauge_meta(station)
            return GaugeReading(
                station=station,
                name=meta[3] if meta else station,
                lat=meta[0] if meta else 0.0,
                lon=meta[1] if meta else 0.0,
                peak_ft_mllw=round(best[0], 2),
                peak_time_utc=best[1],
                sample_count=len(samples),
            )
        return None

    async def find_peak_along_path(
        self,
        track_points: list[dict],
        window_days: int = 3,
        radius_deg: float = 2.5,
    ) -> list[GaugeReading]:
        """
        Given a storm track (list of ``{lat, lon, timestamp}`` dicts), find
        every gauge within *radius_deg* of any track point and return the
        peak water level observed during the window.

        The window runs from (first_track_point - 1 day) to
        (last_track_point + window_days) to capture pre-storm setup and
        post-storm drainage.
        """
        if not track_points:
            return []

        try:
            t0 = datetime.fromisoformat(track_points[0]["timestamp"].replace("Z", ""))
            t1 = datetime.fromisoformat(track_points[-1]["timestamp"].replace("Z", ""))
        except (KeyError, ValueError):
            return []

        begin = (t0 - timedelta(days=1)).strftime("%Y%m%d")
        end = (t1 + timedelta(days=window_days)).strftime("%Y%m%d")

        candidates: set[str] = set()
        for pt in track_points:
            try:
                plat = float(pt["lat"])
                plon = float(pt["lon"])
            except (KeyError, ValueError):
                continue
            for lat, lon, sid, _name, _region in HURRICANE_GAUGES:
                if abs(lat - plat) <= radius_deg and abs(lon - plon) <= radius_deg:
                    candidates.add(sid)

        readings: list[GaugeReading] = []
        for sid in candidates:
            try:
                r = await self.get_peak_water_level(sid, begin, end)
                if r is not None:
                    readings.append(r)
            except Exception as e:
                logger.debug(f"[CO-OPS] skipped {sid}: {e}")

        # Sort by peak height descending so the biggest surge gauge is first.
        readings.sort(key=lambda r: r.peak_ft_mllw, reverse=True)
        return readings


def _find_gauge_meta(station_id: str):
    for row in HURRICANE_GAUGES:
        if row[2] == station_id:
            return row
    return None
