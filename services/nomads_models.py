"""
NOAA NOMADS model client — forecast ensemble for active storm DPS.

NOMADS (NOAA Operational Model Archive and Distribution System) publishes
every NWS/NCEP model run as GRIB2 and, more usefully for web services, as
a filtered subset via the g2sub Tool / OpenDAP / JSON endpoints at:

    https://nomads.ncep.noaa.gov/

For StormDPS we only need a handful of point forecasts around an active
storm to build a "forecast DPS" estimate that complements the observed
track. Specifically:

  * GFS   0.25° global, 384h — macro environment, steering
  * HRRR  3 km CONUS, 48h — high-res near landfall
  * HWRF / HAFS hurricane models — authoritative intensity/track
  * HMON — dynamical statistical blend

The full GRIB2 pipeline is too heavy for a FastAPI request handler, so we
stay within the JSON g2sub endpoints and the NOMADS OpenDAP "ascii" flavor,
which return small plain-text arrays we can parse without cfgrib/xarray.

Caveats:
  * NOMADS rate-limits aggressively. We cache per-run-hour in memory.
  * HWRF/HAFS are only run for active NHC-named storms; availability
    mirrors NHC's active list. The NHC GIS client drives discovery.

This is a thin scaffold — the live DPS Active Storm endpoint uses
``forecast_intensity_trend()`` which returns a list of (lead_hour, wind_kt,
pressure_mb) tuples suitable for plotting.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# g2sub tool base URLs. Each one takes &file=, &lev_*=on, &var_*=on, &lon=, etc.
NOMADS_G2SUB = {
    "gfs":  "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl",
    "hrrr": "https://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_2d.pl",
    "hwrf": "https://nomads.ncep.noaa.gov/cgi-bin/filter_hwrf.pl",
    "hafs": "https://nomads.ncep.noaa.gov/cgi-bin/filter_hafsa.pl",
}

# Plain-text / OpenDAP ascii endpoints for point extraction.
NOMADS_ASCII = "https://nomads.ncep.noaa.gov/dods"


@dataclass
class ModelForecastPoint:
    model: str                 # "gfs", "hwrf", "hafs", "hrrr"
    lead_hours: int
    wind_kt: Optional[float]
    pressure_mb: Optional[float]
    lat: Optional[float]
    lon: Optional[float]
    valid_time_utc: Optional[str] = None


@dataclass
class ModelForecast:
    model: str
    run_cycle_utc: str         # e.g. "2026040900" (yyyymmddHH)
    points: list[ModelForecastPoint] = field(default_factory=list)
    source_url: Optional[str] = None


class NOMADSClient:
    """
    Light wrapper over the NOMADS g2sub tool. Async-friendly and cache-first.
    """

    def __init__(self, cache_minutes: int = 30, timeout: float = 60.0):
        self._timeout = timeout
        self._cache: dict[str, tuple[datetime, ModelForecast]] = {}
        self._cache_minutes = cache_minutes

    def _cached(self, key: str) -> Optional[ModelForecast]:
        hit = self._cache.get(key)
        if not hit:
            return None
        when, value = hit
        if datetime.utcnow() - when > timedelta(minutes=self._cache_minutes):
            self._cache.pop(key, None)
            return None
        return value

    def _store(self, key: str, value: ModelForecast) -> None:
        self._cache[key] = (datetime.utcnow(), value)

    @staticmethod
    def latest_cycle(hour_step: int = 6) -> str:
        now = datetime.utcnow()
        # Model runs are typically available ~3-4h after the cycle hour
        cycle_hour = (now.hour // hour_step) * hour_step
        cycle = now.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)
        if (now - cycle) < timedelta(hours=4):
            cycle -= timedelta(hours=hour_step)
        return cycle.strftime("%Y%m%d%H")

    async def gfs_point_forecast(
        self,
        lat: float,
        lon: float,
        *,
        lead_hours: list[int] = None,
    ) -> Optional[ModelForecast]:
        """
        Pull GFS 0.25° forecast wind/pressure at a single point using the
        NOMADS OpenDAP ascii extractor. Returns a ModelForecast with one
        point per requested lead hour.
        """
        lead_hours = lead_hours or [0, 6, 12, 24, 36, 48, 72, 96, 120]
        cycle = self.latest_cycle(hour_step=6)
        key = f"gfs|{cycle}|{lat:.2f},{lon:.2f}"
        cached = self._cached(key)
        if cached:
            return cached

        # OpenDAP ascii URL for the GFS 0.25° grid. We request a tiny
        # lat/lon window centered on the point to minimize payload size.
        run = cycle[:8]
        hr = cycle[8:10]
        base = f"{NOMADS_ASCII}/gfs_0p25/gfs{run}/gfs_0p25_{hr}z.ascii"

        points: list[ModelForecastPoint] = []
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            for lh in lead_hours:
                # GFS stores output every 3h for the first 120h, then 6h.
                idx_t = lh // 3 if lh <= 120 else (120 // 3) + (lh - 120) // 6
                # ascii subset query — one time index, one point.
                query = (
                    f"?ugrd10m[{idx_t}][{_lat_idx(lat)}][{_lon_idx(lon)}],"
                    f"vgrd10m[{idx_t}][{_lat_idx(lat)}][{_lon_idx(lon)}],"
                    f"prmslmsl[{idx_t}][{_lat_idx(lat)}][{_lon_idx(lon)}]"
                )
                try:
                    r = await client.get(base + query)
                    if r.status_code != 200:
                        continue
                    u, v, pmsl = _parse_gfs_ascii(r.text)
                    if u is None or v is None:
                        continue
                    wind_ms = (u * u + v * v) ** 0.5
                    wind_kt = wind_ms / 0.514444
                    points.append(
                        ModelForecastPoint(
                            model="gfs",
                            lead_hours=lh,
                            wind_kt=round(wind_kt, 1),
                            pressure_mb=round(pmsl / 100.0, 1) if pmsl else None,
                            lat=lat,
                            lon=lon,
                            valid_time_utc=_add_hours_iso(cycle, lh),
                        )
                    )
                except httpx.HTTPError as e:
                    logger.debug(f"[NOMADS] gfs lead {lh} failed: {e}")

        forecast = ModelForecast(
            model="gfs",
            run_cycle_utc=cycle,
            points=points,
            source_url=base,
        )
        self._store(key, forecast)
        return forecast

    async def forecast_intensity_trend(
        self,
        lat: float,
        lon: float,
    ) -> list[ModelForecastPoint]:
        """
        Convenience wrapper returning just the GFS intensity trend points.
        Used by the Active Storm DPS endpoint to compute a forecast DPS
        envelope alongside the NHC advisory track.
        """
        forecast = await self.gfs_point_forecast(lat, lon)
        return forecast.points if forecast else []


def _lat_idx(lat: float) -> int:
    """GFS 0.25° grid — lat from 90 to -90 in 0.25° steps (721 rows)."""
    return int(round((90.0 - lat) / 0.25))


def _lon_idx(lon: float) -> int:
    """GFS 0.25° grid — lon from 0 to 359.75 in 0.25° steps (1440 cols)."""
    if lon < 0:
        lon += 360.0
    return int(round(lon / 0.25))


def _parse_gfs_ascii(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """Extract u/v/pmsl from the single-value OpenDAP ascii response."""
    u = v = pmsl = None
    lines = text.splitlines()
    for i, ln in enumerate(lines):
        if "ugrd10m" in ln.lower():
            u = _first_float(lines, i)
        elif "vgrd10m" in ln.lower():
            v = _first_float(lines, i)
        elif "prmslmsl" in ln.lower():
            pmsl = _first_float(lines, i)
    return u, v, pmsl


def _first_float(lines: list[str], start: int) -> Optional[float]:
    for ln in lines[start + 1 : start + 8]:
        for token in ln.replace(",", " ").split():
            try:
                return float(token)
            except ValueError:
                continue
    return None


def _add_hours_iso(cycle_yyyymmddhh: str, hours: int) -> str:
    try:
        t = datetime.strptime(cycle_yyyymmddhh, "%Y%m%d%H") + timedelta(hours=hours)
        return t.isoformat() + "Z"
    except ValueError:
        return ""
