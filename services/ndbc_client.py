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

Station coverage is two-tiered:
  1. A curated list of long-lived hurricane-track buoys (``HURRICANE_BUOYS``)
     — the guaranteed baseline, and the backbone for historical storms.
  2. NDBC's live active-station list (``activestations.xml``), discovered once
     per process and cached. This is purely additive: any storm passing near
     any currently-reporting met buoy gets coverage, not just the curated set.
     If discovery fails, we fall back to the curated list alone.

All candidate stations near a track are fetched **concurrently** with a
per-station timeout, so one slow/flaky feed can't starve the others and the
layer degrades to partial results instead of silently returning empty.
"""

from __future__ import annotations

import asyncio
import gzip
import io
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta
from timeutil import utcnow
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

NDBC_REALTIME = "https://www.ndbc.noaa.gov/data/realtime2/{station}.txt"
NDBC_HISTORIC = "https://www.ndbc.noaa.gov/data/historical/stdmet/{station}h{year}.txt.gz"
NDBC_ACTIVE_STATIONS = "https://www.ndbc.noaa.gov/activestations.xml"

# Curated long-lived hurricane-track buoys / C-MAN stations. Lat/lon are used
# to find stations within range of a storm track. (lat, lon, station, name)
# These run for decades, so they double as the historical backbone — a storm
# from 2005 will still find 42001/42003 etc. even though discovery only sees
# stations that are active *today*.
HURRICANE_BUOYS: list[tuple[float, float, str, str]] = [
    # Gulf of Mexico
    (25.705, -90.010, "42001", "Mid Gulf"),
    (25.888, -94.419, "42002", "West Gulf"),
    (26.044, -85.616, "42003", "East Gulf"),
    (27.907, -90.482, "42040", "Luke Offshore"),
    (29.063, -88.045, "42039", "Pensacola"),
    (29.232, -87.538, "42012", "Orange Beach"),
    (28.513, -84.508, "42036", "West Tampa"),
    (28.350, -95.345, "42019", "Freeport, TX"),
    (26.968, -96.694, "42020", "Corpus Christi"),
    (29.232, -94.413, "42035", "Galveston"),
    (22.120, -93.960, "42055", "Bay of Campeche"),
    # Atlantic — Florida / Southeast
    (28.420, -80.533, "41009", "Canaveral"),
    (28.878, -78.485, "41010", "Canaveral East"),
    (25.825, -80.097, "41114", "Fort Pierce"),
    (27.520, -80.225, "41113", "Cape Canaveral Nearshore"),
    (30.000, -79.700, "41008", "Grays Reef"),
    (32.501, -79.099, "41004", "Edisto"),
    # Atlantic — Carolinas / Mid-Atlantic / Northeast
    (34.714, -72.220, "41002", "South Hatteras"),
    (34.700, -72.317, "41001", "East of Cape Hatteras"),
    (35.010, -75.400, "41025", "Diamond Shoals"),
    (36.611, -74.842, "44014", "Virginia Beach"),
    (38.457, -74.702, "44009", "Delaware Bay"),
    (38.461, -70.434, "44004", "Hotel"),
    (39.584, -73.703, "44025", "Long Island"),
    (40.369, -73.703, "44065", "NY Harbor Entrance"),
    (40.694, -72.049, "44017", "Montauk Point"),
    (40.500, -69.240, "44011", "Georges Bank"),
    (40.969, -68.996, "44008", "Nantucket"),
    (42.346, -70.651, "44013", "Boston"),
    # Tropical Atlantic / Caribbean / Puerto Rico
    (14.559, -53.073, "41040", "NE Extension"),
    (14.352, -46.000, "41041", "Mid Atlantic"),
    (21.000, -64.800, "41043", "NE Puerto Rico"),
    (18.476, -66.099, "41053", "San Juan, PR"),
    (16.908, -81.422, "42057", "Western Caribbean"),
    (14.923, -74.918, "42058", "Central Caribbean"),
    (15.252, -67.483, "42059", "Eastern Caribbean"),
    # Central Pacific (Hawaii approaches)
    (24.453, -162.000, "51001", "NW Hawaii"),
    (17.094, -157.808, "51002", "SW Hawaii"),
    (19.196, -160.639, "51003", "Western Hawaii"),
    (17.604, -152.364, "51004", "SE Hawaii"),
    (23.445, -153.871, "51101", "NNW Hawaii"),
]

# Process-wide cache for discovered active stations. NDBC's roster changes
# slowly (stations added/retired over months), so a long TTL is fine and keeps
# us from refetching a ~1 MB XML on every storm load.
_DISCOVERY_TTL = timedelta(hours=6)
_discovery_cache: dict = {"ts": None, "stations": []}


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


def _clean_station_name(raw: Optional[str], station: str) -> str:
    """activestations.xml names look like 'Station 41001 - 150 NM East of...'.
    Keep the descriptive tail and cap the length so popups stay tidy."""
    if not raw:
        return station
    name = raw.split(" - ", 1)[1] if " - " in raw else raw
    name = name.strip()
    if len(name) > 48:
        name = name[:45].rstrip() + "…"
    return name or station


class NDBCClient:
    def __init__(self, timeout: float = 15.0):
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

    async def discover_stations(self) -> list[tuple[float, float, str, str]]:
        """Fetch and cache NDBC's active met-reporting buoys / C-MAN stations.

        Returns (lat, lon, station, name) tuples. Best-effort: on any failure
        we return the last good cache (possibly empty) so callers can fall back
        to the curated list. Only ``buoy`` and ``fixed`` types with ``met=y``
        are kept — those are the stations that publish a standard-met feed our
        parser can read.
        """
        now = utcnow()
        cached_ts = _discovery_cache["ts"]
        if cached_ts is not None and (now - cached_ts) < _DISCOVERY_TTL:
            return _discovery_cache["stations"]

        text = await self._fetch_text(NDBC_ACTIVE_STATIONS)
        if not text:
            return _discovery_cache["stations"]

        try:
            root = ET.fromstring(text)
        except ET.ParseError as e:
            logger.debug(f"[NDBC] activestations.xml parse failed: {e}")
            return _discovery_cache["stations"]

        out: list[tuple[float, float, str, str]] = []
        for st in root.findall("station"):
            stype = (st.get("type") or "").lower()
            if stype not in ("buoy", "fixed"):
                continue
            if (st.get("met") or "").lower() != "y":
                continue
            sid = st.get("id")
            if not sid:
                continue
            try:
                lat = float(st.get("lat"))
                lon = float(st.get("lon"))
            except (TypeError, ValueError):
                continue
            out.append((lat, lon, sid, _clean_station_name(st.get("name"), sid)))

        if out:
            _discovery_cache["ts"] = now
            _discovery_cache["stations"] = out
            logger.info(f"[NDBC] discovered {len(out)} active met stations")
        return out or _discovery_cache["stations"]

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
        meta: Optional[tuple[float, float, str]] = None,
    ) -> Optional[BuoyPeak]:
        """Return the peak wind/wave observation at *station* during the window.

        *meta* is (lat, lon, name) for the station; when omitted we fall back to
        the curated registry (so discovered stations must pass their own meta).
        """
        is_realtime = (utcnow() - end_utc) < timedelta(days=45)

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
        bv, bv_t = best("wvht")

        if meta is None:
            m = _find_buoy_meta(station)
            meta = (m[0], m[1], m[3]) if m else (0.0, 0.0, station)

        # Wind drives the peak timestamp; if the anemometer failed (wind all
        # missing) but waves logged, fall back to the wave peak time so the
        # popup still shows when it happened.
        peak_t = bw_t or bv_t
        return BuoyPeak(
            station=station,
            name=meta[2],
            lat=meta[0],
            lon=meta[1],
            peak_wind_ms=round(bw, 1),
            peak_gust_ms=round(bg, 1),
            peak_wave_m=round(bv, 1),
            peak_time_utc=peak_t.isoformat() + "Z" if peak_t else "",
            source=source,
        )

    async def find_peaks_along_path(
        self,
        track_points: list[dict],
        radius_deg: float = 2.5,
        max_stations: int = 24,
        concurrency: int = 12,
        per_station_timeout: float = 7.0,
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

        # Build the station registry: curated first (stable names + historical
        # backbone), then discovered active stations layered in additively.
        # setdefault keeps the curated entry when a station appears in both.
        registry: dict[str, tuple[float, float, str]] = {}
        for lat, lon, sid, name in HURRICANE_BUOYS:
            registry[sid] = (lat, lon, name)
        try:
            for lat, lon, sid, name in await self.discover_stations():
                registry.setdefault(sid, (lat, lon, name))
        except Exception as e:
            logger.debug(f"[NDBC] discovery skipped: {e}")

        # Find stations within radius of any track point; track the minimum
        # distance so we can cap to the nearest N (bounds request fan-out).
        near: dict[str, float] = {}
        for pt in track_points:
            try:
                plat = float(pt["lat"])
                plon = float(pt["lon"])
            except (KeyError, ValueError):
                continue
            for sid, (lat, lon, _name) in registry.items():
                dlat = lat - plat
                dlon = lon - plon
                if abs(dlat) <= radius_deg and abs(dlon) <= radius_deg:
                    d2 = dlat * dlat + dlon * dlon
                    if sid not in near or d2 < near[sid]:
                        near[sid] = d2

        if not near:
            return []
        candidates = sorted(near, key=lambda s: near[s])[:max_stations]

        # Fetch concurrently with a bounded pool and a hard per-station cap, so
        # a slow feed is dropped individually instead of starving the batch.
        sem = asyncio.Semaphore(concurrency)

        async def _one(sid: str) -> Optional[BuoyPeak]:
            async with sem:
                try:
                    return await asyncio.wait_for(
                        self.get_peak_during(sid, start, end, meta=registry[sid]),
                        timeout=per_station_timeout,
                    )
                except (asyncio.TimeoutError, Exception) as e:
                    logger.debug(f"[NDBC] skipped {sid}: {e}")
                    return None

        results = await asyncio.gather(*[_one(s) for s in candidates])

        # Accept a buoy if it logged wind OR waves — in extreme wind the
        # anemometer often fails first while the wave sensor keeps reporting,
        # so requiring wind would drop the most intense stations.
        peaks = [
            p for p in results
            if p is not None and (p.peak_wind_ms > 0 or p.peak_wave_m > 0)
        ]
        peaks.sort(key=lambda p: (p.peak_wind_ms, p.peak_wave_m), reverse=True)
        return peaks


def _find_buoy_meta(station_id: str):
    for row in HURRICANE_BUOYS:
        if row[2] == station_id:
            return row
    return None
