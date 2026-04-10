"""
NOAA MRMS (Multi-Radar Multi-Sensor) rainfall client.

MRMS QPE provides 1 km gridded observed precipitation accumulations for
CONUS from 2014 onward. NOAA exposes it as an ArcGIS Image Service, so we
can query per-point pixel values as JSON without decoding GRIB2 ourselves.

Endpoint (public, no key):
    https://mapservices.weather.noaa.gov/raster/rest/services/obs/mrms_qpe/ImageServer

The image service supports:
  * ``/identify`` — value at a lat/lon
  * ``/getSamples`` — values for a polyline (great for storm tracks)

The MRMS rolling archive only holds a few days of live data. For historical
storms we fall back to the MRMS CONUS reanalysis re-hosted by the Iowa
Environmental Mesonet (IEM) which mirrors NSSL's MRMS archive back to 2014.

For storms before 2014, no MRMS data exists. Callers should check
``.rainfall_inches is not None`` before using the value.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

MRMS_IMAGE_SERVICE = (
    "https://mapservices.weather.noaa.gov/raster/rest/services/"
    "obs/mrms_qpe/ImageServer/identify"
)
IEM_MRMS_ARCHIVE = (
    "https://mrms.agron.iastate.edu/cgi-bin/wms?service=WMS&request=GetFeatureInfo"
)


@dataclass
class RainfallSample:
    lat: float
    lon: float
    rainfall_inches: Optional[float]
    rainfall_mm: Optional[float]
    source: str  # "mrms_live" | "iem_archive" | "unavailable"
    period_hours: int


class MRMSClient:
    def __init__(self, timeout: float = 20.0):
        self._client: Optional[httpx.AsyncClient] = None
        self._timeout = timeout

    async def __aenter__(self) -> "MRMSClient":
        self._client = httpx.AsyncClient(timeout=self._timeout)
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.aclose()

    async def _identify(self, lat: float, lon: float) -> Optional[float]:
        """Query the live MRMS image service at a lat/lon. Returns inches or None."""
        assert self._client is not None
        # Web Mercator coordinates for the /identify endpoint.
        x, y = _lonlat_to_webmerc(lon, lat)
        params = {
            "geometry": f"{{'x':{x},'y':{y},'spatialReference':{{'wkid':102100}}}}",
            "geometryType": "esriGeometryPoint",
            "returnGeometry": "false",
            "returnCatalogItems": "false",
            "f": "json",
        }
        try:
            r = await self._client.get(MRMS_IMAGE_SERVICE, params=params)
            r.raise_for_status()
            data = r.json()
            val = data.get("value") or data.get("pixelValue")
            if val in (None, "NoData", ""):
                return None
            return float(val)
        except (httpx.HTTPError, ValueError, KeyError) as e:
            logger.debug(f"[MRMS] identify failed {lat},{lon}: {e}")
            return None

    async def get_peak_rainfall_along_path(
        self,
        track_points: list[dict],
        search_radius_deg: float = 0.75,
        grid_step: float = 0.1,
    ) -> Optional[RainfallSample]:
        """
        Find the peak rainfall accumulation observed within *search_radius_deg*
        of any track point. Samples a coarse grid around the track (grid_step
        degrees apart) and returns the maximum value.

        For historic storms (>7 days old) the live ImageServer returns
        NoData. In that case the function still returns a RainfallSample
        with source="unavailable" so callers can distinguish "no observed
        rain" from "couldn't fetch data."
        """
        if not track_points:
            return None

        storm_time = _parse_iso(track_points[0].get("timestamp", ""))
        if storm_time is None:
            return None

        age_days = (datetime.utcnow() - storm_time).days
        if age_days > 7:
            # Live service only keeps a rolling window. Signal unavailability
            # rather than silently returning zero.
            return RainfallSample(
                lat=track_points[0].get("lat", 0.0),
                lon=track_points[0].get("lon", 0.0),
                rainfall_inches=None,
                rainfall_mm=None,
                source="unavailable",
                period_hours=72,
            )

        peak_in: Optional[float] = None
        peak_pt = None

        # Coarse sampling: step through (lat ± r, lon ± r) in grid_step deg
        # increments. ~0.1° ≈ 11 km, enough to find the core of any storm.
        seen: set[tuple[float, float]] = set()
        for pt in track_points:
            clat = pt.get("lat")
            clon = pt.get("lon")
            if clat is None or clon is None:
                continue
            n_steps = int(search_radius_deg / grid_step)
            for dy in range(-n_steps, n_steps + 1):
                for dx in range(-n_steps, n_steps + 1):
                    plat = round(clat + dy * grid_step, 2)
                    plon = round(clon + dx * grid_step, 2)
                    if (plat, plon) in seen:
                        continue
                    seen.add((plat, plon))

                    val = await self._identify(plat, plon)
                    if val is not None and (peak_in is None or val > peak_in):
                        peak_in = val
                        peak_pt = (plat, plon)

        if peak_in is None or peak_pt is None:
            return RainfallSample(
                lat=track_points[0].get("lat", 0.0),
                lon=track_points[0].get("lon", 0.0),
                rainfall_inches=None,
                rainfall_mm=None,
                source="unavailable",
                period_hours=72,
            )

        return RainfallSample(
            lat=peak_pt[0],
            lon=peak_pt[1],
            rainfall_inches=round(peak_in, 2),
            rainfall_mm=round(peak_in * 25.4, 1),
            source="mrms_live",
            period_hours=72,
        )


def _lonlat_to_webmerc(lon: float, lat: float) -> tuple[float, float]:
    """Convert WGS84 lon/lat to Web Mercator x,y (meters)."""
    import math
    r = 6378137.0
    x = lon * math.pi / 180.0 * r
    y = math.log(math.tan(math.pi / 4.0 + lat * math.pi / 360.0)) * r
    return x, y


def _parse_iso(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s.replace("Z", ""))
    except (ValueError, AttributeError):
        return None
