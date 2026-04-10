"""
NDBC (National Data Buoy Center) client.

Fetches observed wind/wave peaks from coastal buoys during a given storm
window. NDBC does not publish a JSON API; data is served as fixed-column
text files under https://www.ndbc.noaa.gov/data/.

We support two paths:
  * Real-time (last 45 days) — ``data/realtime2/<station>.txt``
  * Historical (annual archives) — ``data/historical/stdmet/<station>h<year>.txt.gz``

Both formats have a stable header and whitespace-delimited columns, so a
regex-free parser works. Wind speed is in m/s, wave height in meters; we
convert to knots / feet in the return struct so downstream code in StormDPS
speaks the same units as HURDAT2.
"""

from __future__ import annotations

import gzip
import io
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

NDBC_REALTIME = "https://www.ndbc.noaa.gov/data/realtime2/{station}.txt"
NDBC_HISTORIC = "https://www.ndbc.noaa.gov/data/historical/stdmet/{station}h{year}.txt.gz"

# Curated hurricane-track buoys. Lat/lon used to find buoys within range of a
# storm track. (lat, lon, station, name)
HURRICANE_BUOYS: list[tuple[float, float, str, str]] = [
    # Gulf of Mexico
    (25.705, -90.010, "42001", "Mid Gulf"),
    (25.888, -94.419, "42002", "West Gulf"),
    (26.044, -85.616, "42003", "East Gulf"),
    (27.907, -90.482, "42040", "Luke Offshore"),
    (29.063, -88.045, "42039", "Pensacola"),
    (29.232, -87.538, "42012", "Orange Beach"),
    (28.513, -84.508, "42036", "West Tampa"),
    (28.420, -80.533, "41009", "Canaveral"),
    # Atlantic
    (25.825, -80.097, "41114", "Fort Pierce"),
    (27.520, -80.225, "41113", "Cape Canaveral Nearshore"),
    (30.000, -79.700, "41008", "Grays Reef"),
    (32.501, -79.099, "41004", "Edisto"),
    (34.714, -72.220, "41002", "South Hatteras"),
    (35.010, -75.400, "41025", "Diamond Shoals"),
    (38.461, -70.434, "44004", "Hotel"),
    (39.584, -73.703, "44025", "Long Island"),
    (40.500, -69.240, "44011", "Georges Bank"),
    (40.969, -68.996, "44008", "Nantucket"),
]


@dataclass
class BuoyPeak:
    station: str
    name: str
    lat: float
    lon: float
    peak_wind_ms: float
    peak_gust_ms: float
    peak_wave_m: float
    peak_time_utc: str
    source: str  # "realtime" or "historical"


class NDBCClient:
    def __init__(self, timeout: float = 20.0):
        self._client: Optional[httpx.AsyncClient] = None
        self._timeout = timeout

    async def __aenter__(self) -> "NDBCClient":
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            headers={"User-Agent": "StormDPS/1.0 (research; weather)"}
        )
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.aclose()

    async def _fetch_text(self, url: str, gz: bool = False) -> Optional[str]:
        assert self._client is not None
        try:
            r = await self._client.get(url)
            if r.status_code != 200:
                return None
            if gz:
                with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as f:
                    return f.read().decode("utf-8", errors="replace")
            return r.text
        except (httpx.HTTPError, OSError) as e:
            logger.debug(f"[NDBC] fetch failed {url}: {e}")
            return None

    def _parse_stdmet(self, text: str) -> list[dict]:
        """
        Parse a standard meteorological file. Columns (as of 2007+):

            #YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS  TIDE

        Earlier archives use slightly different columns but the stable ones
        we care about (WSPD, GST, WVHT) appear in every version.
        """
        rows: list[dict] = []
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if not lines:
            return rows
        header = lines[0].lstrip("#").split()
        # Skip the units line (#yr mo dy hr mn ...)
        start = 1
        if len(lines) > 1 and lines[1].startswith("#"):
            start = 2

        # Resolve column indexes — fall back gracefully if a column is missing.
        def idx(col: str) -> int:
            try:
                return header.index(col)
            except ValueError:
                return -1

        i_yy = idx("YY")  # 4-digit year in new format; 2-digit in old
        i_mm = idx("MM")
        i_dd = idx("DD")
        i_hh = idx("hh")
        i_mn = idx("mm")
        i_ws = idx("WSPD")
        i_gs = idx("GST")
        i_wv = idx("WVHT")

        for ln in lines[start:]:
            parts = ln.split()
            if len(parts) < max(i_ws, i_gs, i_wv) + 1:
                continue
            try:
                yr = int(parts[i_yy]) if i_yy >= 0 else 1970
                if yr < 100:
                    yr += 1900 if yr > 50 else 2000
                ts = datetime(
                    yr,
                    int(parts[i_mm]) if i_mm >= 0 else 1,
                    int(parts[i_dd]) if i_dd >= 0 else 1,
                    int(parts[i_hh]) if i_hh >= 0 else 0,
                    int(parts[i_mn]) if i_mn >= 0 else 0,
                )
            except (ValueError, IndexError):
                continue

            def f(i):
                if i < 0 or i >= len(parts):
                    return None
                try:
                    v = float(parts[i])
                    if v >= 99.0:  # NDBC missing-value sentinel
                        return None
                    return v
                except ValueError:
                    return None

            rows.append({
                "t": ts,
                "wspd": f(i_ws),
                "gst": f(i_gs),
                "wvht": f(i_wv),
            })
        return rows

    async def get_peak_during(
        self,
        station: str,
        start_utc: datetime,
        end_utc: datetime,
    ) -> Optional[BuoyPeak]:
        """Return the peak wind/wave observation at *station* during the window."""
        is_realtime = (datetime.utcnow() - end_utc) < timedelta(days=45)

        text = None
        source = "realtime"
        if is_realtime:
            text = await self._fetch_text(NDBC_REALTIME.format(station=station))
        if text is None:
            # Historical archive by year
            for yr in {start_utc.year, end_utc.year}:
                t = await self._fetch_text(
                    NDBC_HISTORIC.format(station=station, year=yr),
                    gz=True,
                )
                if t:
                    text = (text or "") + t
                    source = "historical"

        if not text:
            return None

        rows = self._parse_stdmet(text)
        in_window = [r for r in rows if start_utc <= r["t"] <= end_utc]
        if not in_window:
            return None

        def best(key):
            vals = [(r[key], r["t"]) for r in in_window if r.get(key) is not None]
            if not vals:
                return (0.0, None)
            return max(vals, key=lambda x: x[0])

        bw, bw_t = best("wspd")
        bg, _ = best("gst")
        bv, _ = best("wvht")

        meta = _find_buoy_meta(station)
        return BuoyPeak(
            station=station,
            name=meta[3] if meta else station,
            lat=meta[0] if meta else 0.0,
            lon=meta[1] if meta else 0.0,
            peak_wind_ms=round(bw, 1),
            peak_gust_ms=round(bg, 1),
            peak_wave_m=round(bv, 1),
            peak_time_utc=bw_t.isoformat() + "Z" if bw_t else "",
            source=source,
        )

    async def find_peaks_along_path(
        self,
        track_points: list[dict],
        radius_deg: float = 2.5,
    ) -> list[BuoyPeak]:
        if not track_points:
            return []
        try:
            t0 = datetime.fromisoformat(track_points[0]["timestamp"].replace("Z", ""))
            t1 = datetime.fromisoformat(track_points[-1]["timestamp"].replace("Z", ""))
        except (KeyError, ValueError):
            return []

        start = t0 - timedelta(days=1)
        end = t1 + timedelta(days=1)

        candidates: set[str] = set()
        for pt in track_points:
            try:
                plat = float(pt["lat"])
                plon = float(pt["lon"])
            except (KeyError, ValueError):
                continue
            for lat, lon, sid, _ in HURRICANE_BUOYS:
                if abs(lat - plat) <= radius_deg and abs(lon - plon) <= radius_deg:
                    candidates.add(sid)

        peaks: list[BuoyPeak] = []
        for sid in candidates:
            try:
                p = await self.get_peak_during(sid, start, end)
                if p is not None and p.peak_wind_ms > 0:
                    peaks.append(p)
            except Exception as e:
                logger.debug(f"[NDBC] skipped {sid}: {e}")

        peaks.sort(key=lambda p: p.peak_wind_ms, reverse=True)
        return peaks


def _find_buoy_meta(station_id: str):
    for row in HURRICANE_BUOYS:
        if row[2] == station_id:
            return row
    return None
