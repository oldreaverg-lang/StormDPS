"""
NOAA / National Hurricane Center data service.

Pulls hurricane data from NOAA's public APIs and data feeds:
  - NHC GIS data (shapefiles, KML) for active storms
  - ATCF best-track / forecast advisories with full quadrant wind radii
  - HURDAT2 historical database with extended wind radii columns
  - GFS 0.25° gridded wind fields via NOMADS (decoded from GRIB2)

This service fetches raw data and transforms it into our internal
HurricaneSnapshot model for IKE computation.
"""

import asyncio
import csv
import io
import json
import logging
import math
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from timeutil import utcnow
from typing import Optional
from pathlib import Path

import httpx
import numpy as np

from models.hurricane import HurricaneSnapshot, WindFieldGrid
from core.ike import knots_to_ms, nm_to_meters, calculate_dps
from services.grib2_decoder import (
    decode_grib2,
    extract_wind_components,
    compute_wind_speed_grid,
)

logger = logging.getLogger(__name__)


# NOAA API endpoints
NHC_ACTIVE_STORMS_URL = "https://www.nhc.noaa.gov/CurrentStorms.json"
NHC_GIS_BASE_URL = "https://www.nhc.noaa.gov/gis"
HURDAT2_URL = "https://www.nhc.noaa.gov/data/hurdat/hurdat2-1851-2024-040425.txt"

# East Pacific HURDAT2 file. NHC publishes a parallel reanalysis covering
# the EP basin (1949-present), updated on a separate cadence from the
# Atlantic file — the Atlantic file above is the April 2025 reanalysis
# (data through 2024), the EP file below is the February 2026 reanalysis
# (data through 2025). Each filename embeds the publication date in
# MMDDYY format; the URL has to be updated annually after NHC pushes the
# new reanalysis. If the date stamp drifts, the URL 404s and the caller
# falls back to IBTrACS (see get_historical_track + api/routes
# get_storm_track for the fallback chain), so a stale URL degrades
# gracefully rather than breaking.
HURDAT2_EPAC_URL = "https://www.nhc.noaa.gov/data/hurdat/hurdat2-nepac-1949-2025-022726.txt"


def _hurdat2_url_for(storm_id: str) -> tuple[str, str]:
    """Pick the right HURDAT2 file + cache filename based on storm prefix.

    Returns (download_url, cache_filename). NHC publishes Atlantic
    (AL prefix) and Pacific HURDAT2 reanalyses as separate files. The
    Pacific file is "nepac" — NORTHEAST AND NORTH CENTRAL Pacific — so
    it carries CP-prefixed CPHC storms too (e.g. Iniki CP111992); an
    earlier version of this docstring claimed CP had no HURDAT2 file,
    which was wrong and kept CP storms IBTrACS-only.
    """
    prefix = (storm_id or "")[:2].upper()
    if prefix in ("EP", "CP"):
        return (HURDAT2_EPAC_URL, "hurdat2_epac.txt")
    # Default: Atlantic file. Covers AL prefix; for WP / IO / SH the
    # caller falls back to IBTrACS after a HURDAT2 parse miss.
    return (HURDAT2_URL, "hurdat2_atl.txt")

# Extended Best Track (EBTRK) — historical wind radii data
# NOTE: The AOML EBTRK URLs are no longer available (404). The dataset was last
# updated with 2018 data. RAMMB/CIRA hosts an archive, but for storms after 2018
# we rely on HURDAT2 extended format (which includes quadrant radii post-2004)
# and IBTrACS as fallback sources.
EBTRK_URL = "https://rammb2.cira.colostate.edu/research/tropical-cyclones/tc_extended_best_track_dataset/data/ebtrk_atlc.txt"  # Atlantic basin
EBTRK_EP_URL = "https://rammb2.cira.colostate.edu/research/tropical-cyclones/tc_extended_best_track_dataset/data/ebtrk_epac.txt"  # East Pacific

# NOAA operational gridded products (GFS-based tropical cyclone fields)
NOAA_NOMADS_BASE = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"

# IBTrACS — International Best Track Archive for Climate Stewardship
IBTRACS_CSV_URL = "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/csv/ibtracs.ALL.list.v04r01.csv"
IBTRACS_LAST3_URL = "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/csv/ibtracs.last3years.list.v04r01.csv"

# Earth radius for lat/lon → meter conversions
EARTH_RADIUS_M = 6_371_000.0


# ──────────────────────────────────────────────────────────────────────
#  NHC TCM (forecast advisory) text parsing
#
#  The TCM is a rigidly formatted product, e.g.:
#      TROPICAL STORM CENTER LOCATED NEAR 29.3N  86.8W AT 21/2100Z
#      MAX SUSTAINED WINDS  50 KT WITH GUSTS  60 KT.
#      ...
#      FORECAST VALID 22/0600Z 30.4N  87.0W
#      MAX WIND  50 KT...GUSTS  60 KT.
#      ...
#      OUTLOOK VALID 25/1800Z...DISSIPATED   (no position → skipped)
# ──────────────────────────────────────────────────────────────────────

_TCM_CENTER_RE = re.compile(
    r"CENTER LOCATED NEAR\s+(\d{1,2}\.\d)\s*([NS])\s+(\d{1,3}\.\d)\s*([EW])"
    r"\s+AT\s+(\d{2})/(\d{4})Z")
_TCM_INIT_WIND_RE = re.compile(
    r"MAX SUSTAINED WINDS\s+(\d+)\s+KT WITH GUSTS(?:\s+TO)?\s+(\d+)\s+KT")
_TCM_FCST_RE = re.compile(
    r"(?:FORECAST|OUTLOOK) VALID\s+(\d{2})/(\d{4})Z\s+"
    r"(\d{1,2}\.\d)\s*([NS])\s+(\d{1,3}\.\d)\s*([EW])")
_TCM_FCST_WIND_RE = re.compile(
    r"MAX WIND\s+(\d+)\s+KT\.\.\.GUSTS\s+(\d+)\s+KT")
# Current-conditions fields from the advisory header (tau=0 only), so the UI can
# show a self-consistent CURRENT snapshot (position + intensity + pressure +
# movement + class) all from the same fresh advisory, instead of pairing the
# fresh position with staleable /storms/active wind/pressure/movement.
_TCM_MOVE_RE = re.compile(
    r"PRESENT MOVEMENT TOWARD[^\n]*?(\d{1,3})\s+DEGREES\s+AT\s+(\d{1,3})\s+KT")
_TCM_PRES_RE = re.compile(r"MINIMUM CENTRAL PRESSURE\s+(\d{3,4})\s+MB")


def _tcm_storm_class(text: str) -> Optional[str]:
    """Short storm-type code from the TCM header line (HURRICANE HERNAN
    FORECAST/ADVISORY ...). Matches the short codes NHC's CurrentStorms feed
    uses (TD/TS/HU), so it drops into the chip badge unchanged. More specific
    phrases are checked first (SUBTROPICAL STORM before TROPICAL STORM)."""
    head = text[:600].upper()
    for needle, short in (
        ("POTENTIAL TROPICAL CYCLONE", "PTC"),
        ("SUBTROPICAL DEPRESSION", "STD"),
        ("SUBTROPICAL STORM", "STS"),
        ("TROPICAL DEPRESSION", "TD"),
        ("TROPICAL STORM", "TS"),
        ("POST-TROPICAL CYCLONE", "PT"),
        ("REMNANTS OF", "REM"),
        ("HURRICANE", "HU"),
        ("TYPHOON", "TY"),
    ):
        if needle in head:
            return short
    return None


def _tcm_datetime(day: int, hhmm: str, ref: datetime) -> Optional[datetime]:
    """
    Resolve a TCM 'DD/HHMMZ' stamp (day-of-month only) to a full UTC
    datetime by picking the candidate month — previous, same, or next
    relative to ``ref`` — whose date lands closest to ``ref``. Forecast
    taus max out at 120h, so closest-month is always unambiguous.
    """
    try:
        hh, mm = int(hhmm[:2]), int(hhmm[2:])
    except ValueError:
        return None
    candidates = []
    for months_off in (-1, 0, 1):
        y, m = ref.year, ref.month + months_off
        if m < 1:
            y, m = y - 1, 12
        elif m > 12:
            y, m = y + 1, 1
        try:
            candidates.append(datetime(y, m, day, hh, mm))
        except ValueError:
            continue  # e.g. day 31 in a 30-day month
    if not candidates:
        return None
    return min(candidates, key=lambda d: abs((d - ref).total_seconds()))


def _parse_tcm_forecast(text: str) -> list[dict]:
    """
    Parse an NHC TCM forecast advisory into forecast-track points shaped
    like the retired GeoJSON product: lat / lon / hour / max_wind_kt /
    gust_kt / time. Fail-open: any structural surprise returns [].
    """
    try:
        center = _TCM_CENTER_RE.search(text)
        if not center:
            return []
        base = _tcm_datetime(int(center.group(5)), center.group(6), utcnow().replace(tzinfo=None))
        if base is None:
            return []

        def _signed(val: str, hemi: str) -> float:
            v = float(val)
            return -v if hemi in ("S", "W") else v

        points = []
        init_wind = _TCM_INIT_WIND_RE.search(text)
        move = _TCM_MOVE_RE.search(text)
        pres = _TCM_PRES_RE.search(text)
        points.append({
            "lat": _signed(center.group(1), center.group(2)),
            "lon": _signed(center.group(3), center.group(4)),
            "hour": 0,
            "max_wind_kt": int(init_wind.group(1)) if init_wind else None,
            "gust_kt": int(init_wind.group(2)) if init_wind else None,
            # Display-only label (frontend tooltips) — nothing parses it.
            "time": base.strftime("%a %b %d %H:%MZ"),
            # Machine-readable valid time of this analysis (tau=0) center. `base`
            # is naive UTC, so stamp an explicit +00:00 offset or JS Date() would
            # read it as browser-local. Lets consumers pick the freshest current
            # position by comparing this against the best-track tail timestamp.
            "valid_time_utc": base.replace(tzinfo=timezone.utc).isoformat(),
            # Current-conditions fields (tau=0 only) so the sidebar chip can show a
            # self-consistent snapshot from this same fresh advisory rather than
            # pairing the fresh position with staleable /storms/active values.
            "min_pressure_mb": int(pres.group(1)) if pres else None,
            "movement_dir_deg": int(move.group(1)) if move else None,
            "movement_speed_kt": int(move.group(2)) if move else None,
            "classification": _tcm_storm_class(text),
        })

        matches = list(_TCM_FCST_RE.finditer(text))
        for i, m in enumerate(matches):
            valid = _tcm_datetime(int(m.group(1)), m.group(2), base)
            if valid is None:
                continue
            hour = round((valid - base).total_seconds() / 3600)
            if hour <= 0 or hour > 168:
                continue
            # MAX WIND line lives between this VALID line and the next one.
            seg_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            wind = _TCM_FCST_WIND_RE.search(text, m.end(), seg_end)
            points.append({
                "lat": _signed(m.group(3), m.group(4)),
                "lon": _signed(m.group(5), m.group(6)),
                "hour": hour,
                "max_wind_kt": int(wind.group(1)) if wind else None,
                "gust_kt": int(wind.group(2)) if wind else None,
                "time": valid.strftime("%a %b %d %H:%MZ"),
                "valid_time_utc": valid.replace(tzinfo=timezone.utc).isoformat(),
            })
        return points
    except Exception as e:
        logger.warning(f"[forecast] TCM parse failed: {e}")
        return []


def _parse_cone_kmz(blob: bytes) -> list[list[float]]:
    """
    Extract the uncertainty-cone polygon from an NHC cone KMZ (a zip
    holding one KML). Returns [[lat, lon], ...] — the largest coordinate
    ring found, matching the retired GeoJSON product's shape. Fail-open:
    returns [] on any parse issue.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            kml_names = [n for n in zf.namelist() if n.lower().endswith(".kml")]
            if not kml_names:
                return []
            xml_text = zf.read(kml_names[0]).decode("utf-8", "replace")
        root = ET.fromstring(xml_text)
        best: list[list[float]] = []
        for el in root.iter():
            if not el.tag.endswith("coordinates") or not el.text:
                continue
            ring = []
            for token in el.text.split():
                parts = token.split(",")
                if len(parts) >= 2:
                    ring.append([float(parts[1]), float(parts[0])])  # lat, lon
            if len(ring) > len(best):
                best = ring
        return best
    except Exception as e:
        logger.warning(f"[forecast] cone KMZ parse failed: {e}")
        return []


class NOAAClientError(Exception):
    """Raised when NOAA data retrieval fails."""
    pass


class NOAAClient:
    """
    Client for retrieving hurricane data from NOAA/NHC and IBTrACS.

    Class-level caches survive across requests within the same server process.

    Usage:
        async with NOAAClient() as client:
            storms = await client.get_active_storms()
            snapshot = await client.get_storm_snapshot("AL092024")
            # Or from IBTrACS for global coverage:
            track = await client.get_ibtracs_track("2005236N23285")
    """

    def __init__(self, timeout: float = 30.0, cache_dir: Optional[str] = None, http_client: Optional[httpx.AsyncClient] = None):
        self.timeout = timeout
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self._http_client: Optional[httpx.AsyncClient] = http_client
        self._external_http_client = http_client is not None  # Track if client was provided externally

    async def __aenter__(self):
        if self._http_client is None:
            # Create a new client if none was provided
            self._http_client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={"User-Agent": "HurricaneIKE-App/1.0 (research)"},
                follow_redirects=True,
            )
        return self

    async def __aexit__(self, *args):
        # Only close the client if we created it (not if it was provided externally)
        if self._http_client and not self._external_http_client:
            await self._http_client.aclose()

    # Retry configuration
    RETRY_ATTEMPTS = 3
    RETRY_BACKOFF_BASE = 1.0  # exponential: 1s, 2s, 4s

    # HTTP status codes that warrant retry
    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    # NHC reachability cache: avoid repeated 30s timeouts when NHC is down.
    # Stores the timestamp of the last connection failure. If a failure happened
    # within the TTL, we skip NHC entirely and go straight to IBTrACS.
    _nhc_last_failure: Optional[datetime] = None
    _nhc_failure_ttl = timedelta(minutes=5)

    @classmethod
    def nhc_is_down(cls) -> bool:
        """Check if NHC was recently unreachable (skip to avoid timeout)."""
        if cls._nhc_last_failure is None:
            return False
        return (utcnow() - cls._nhc_last_failure) < cls._nhc_failure_ttl

    @classmethod
    def mark_nhc_down(cls):
        """Record that NHC is unreachable."""
        cls._nhc_last_failure = utcnow()
        logger.warning(f"[NHC] Marked as down — skipping for {cls._nhc_failure_ttl.total_seconds():.0f}s")

    @classmethod
    def mark_nhc_up(cls):
        """Clear the NHC failure cache."""
        cls._nhc_last_failure = None

    @property
    def http(self) -> httpx.AsyncClient:
        if self._http_client is None:
            raise RuntimeError("NOAAClient must be used as async context manager")
        return self._http_client

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> httpx.Response:
        """
        Make HTTP request with exponential backoff retry logic.

        Retries on: connection errors, timeouts, or specific status codes (429, 5xx).
        On 429 (rate limit), respects Retry-After header or uses 10s backoff.
        On 4xx errors (except 429), fails immediately without retry.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL to request
            **kwargs: Additional arguments for httpx.request()

        Returns:
            Response object on success

        Raises:
            NOAAClientError: If all retry attempts fail
        """
        last_error: Optional[Exception] = None

        for attempt in range(self.RETRY_ATTEMPTS):
            try:
                resp = await self.http.request(method, url, **kwargs)

                # Immediate return on success or non-retryable client error (4xx)
                if resp.status_code < 400 or (400 <= resp.status_code < 429) or (429 < resp.status_code < 500):
                    return resp

                # Handle 429 (rate limit) with Retry-After header
                if resp.status_code == 429 and attempt < self.RETRY_ATTEMPTS - 1:
                    retry_after = resp.headers.get("Retry-After", "10")
                    try:
                        backoff = float(retry_after)
                    except (ValueError, TypeError):
                        backoff = 10.0
                    logger.warning(
                        f"NOAA API rate limited (429) for {url}; "
                        f"retrying in {backoff:.1f}s (attempt {attempt + 1}/{self.RETRY_ATTEMPTS})"
                    )
                    await asyncio.sleep(backoff)
                    continue

                # Retry on 5xx server errors
                if resp.status_code >= 500 and attempt < self.RETRY_ATTEMPTS - 1:
                    backoff = self.RETRY_BACKOFF_BASE ** attempt
                    logger.warning(
                        f"NOAA API returned {resp.status_code} for {url}; "
                        f"retrying in {backoff:.1f}s (attempt {attempt + 1}/{self.RETRY_ATTEMPTS})"
                    )
                    await asyncio.sleep(backoff)
                    continue

                # Return non-retryable response
                return resp

            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_error = e
                if attempt < self.RETRY_ATTEMPTS - 1:
                    backoff = self.RETRY_BACKOFF_BASE ** attempt
                    logger.warning(
                        f"NOAA API connection error: {type(e).__name__}; "
                        f"retrying in {backoff:.1f}s (attempt {attempt + 1}/{self.RETRY_ATTEMPTS})"
                    )
                    await asyncio.sleep(backoff)
                    continue
                # Fall through to raise error below

        # All retries exhausted
        if last_error:
            raise NOAAClientError(f"NOAA API request failed after {self.RETRY_ATTEMPTS} attempts: {last_error}") from last_error
        raise NOAAClientError(f"NOAA API request to {url} failed after {self.RETRY_ATTEMPTS} attempts")

    # ==================================================================
    #  SEA SURFACE TEMPERATURE (ERDDAP griddap)
    # ==================================================================

    # Multiple ERDDAP SST sources with automatic fallback.
    # Each entry: (base_url, variable_name, label, lon_360)
    # lon_360=True means the dataset uses 0–360° longitude (must convert from -180/180)
    # Tried in order until one responds successfully.
    ERDDAP_SST_SOURCES = [
        # CoralTemp daily SST: 1985-present, global 5km (primary)
        ("https://coastwatch.noaa.gov/erddap/griddap/noaacrwsstDaily", "analysed_sst", "CoralTemp/CoastWatch", False),
        # MUR SST: 2002-present, global 1km (West Coast ERDDAP mirror)
        ("https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41", "analysed_sst", "MUR/PFEG", False),
        # CoralTemp on PacIOOS ERDDAP: 1985-present, global 5km (uses 0–360° lon)
        ("https://oceanwatch.pifsc.noaa.gov/erddap/griddap/CRW_sst_v3_1", "analysed_sst", "CoralTemp/PacIOOS", True),
    ]

    # Which SST source index is currently working (start with primary)
    _sst_source_idx = 0

    # In-memory SST cache: {(round(lat,1), round(lon,1)): (sst_c, datetime)}
    # Used as stale fallback when all ERDDAP sources are down. Values expire
    # after 24h but are returned as stale data if no live source is available.
    _sst_cache: dict[tuple, tuple] = {}
    _SST_CACHE_TTL = timedelta(hours=24)
    _SST_CACHE_MAX = 2000  # max entries — evict oldest when exceeded

    @staticmethod
    def _convert_lon(lon: float, use_360: bool) -> float:
        """Convert longitude to 0-360 range if the dataset requires it."""
        if use_360 and lon < 0:
            return lon + 360.0
        return lon

    async def _probe_sst_source(self, source_idx: int, date_str: str, lat: float, lon: float) -> bool:
        """Quick health check: can this ERDDAP source respond at all?"""
        base_url, var_name, label, lon_360 = self.ERDDAP_SST_SOURCES[source_idx]
        query_lon = self._convert_lon(lon, lon_360)
        url = f"{base_url}.json?{var_name}[({date_str})][({lat})][({query_lon})]"
        try:
            resp = await self.http.get(url, timeout=6.0)
            if resp.status_code == 200:
                logger.debug(f"[SST] Probe OK: {label} is responding")
                return True
            logger.warning(f"[SST] Probe failed: {label} returned HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"[SST] Probe failed: {label} — {type(e).__name__}")
        return False

    async def get_sst_along_track(self, points: list[dict]) -> list[dict]:
        """
        Fetch SST from ERDDAP for a list of track points.

        Tries multiple ERDDAP servers with automatic fallback if the
        primary source is down (503, timeout, etc.).

        Args:
            points: list of dicts with 'lat', 'lon', 'timestamp' keys

        Returns:
            list of dicts with 'timestamp', 'lat', 'lon', 'sst_c' (°C or None)
        """
        import asyncio

        if not points:
            return []

        # Probe date MUST be a date the SST datasets have already published.
        # The CoralTemp / MUR datasets typically run 1–3 days behind real time,
        # and active-storm tracks include forecast points that extend 5 days
        # into the future. If we probe with the track's first timestamp, a
        # forecast point would trigger ERDDAP's "Start is greater than axis
        # maximum" 404 even though the dataset is healthy — and the probe
        # would falsely conclude every source is down, falling back to
        # stale-cached or null SST and spamming the log every poll.
        #
        # The probe is only checking "can the dataset respond?" — it doesn't
        # consume the response. Using today-minus-3-days guarantees a date
        # that's safely inside the published window for all three sources.
        probe_dt = utcnow() - timedelta(days=3)
        sample_date = probe_dt.strftime("%Y-%m-%dT12:00:00Z")

        # Use a known-good probe location too — first track point's lat/lon
        # is fine since SST coverage is global, but fall back to a tropical
        # Atlantic point if the track sample is missing coords.
        sample = points[0]
        probe_lat = sample.get("lat") if sample.get("lat") is not None else 25.0
        probe_lon = sample.get("lon") if sample.get("lon") is not None else -80.0

        # Find a working SST source (probe starting from current preferred)
        working_idx = None
        for offset in range(len(self.ERDDAP_SST_SOURCES)):
            idx = (self._sst_source_idx + offset) % len(self.ERDDAP_SST_SOURCES)
            if await self._probe_sst_source(idx, sample_date, probe_lat, probe_lon):
                working_idx = idx
                break

        if working_idx is None:
            # All ERDDAP sources failed — try returning stale cached SST
            now = utcnow()
            stale_results = []
            stale_hits = 0
            for pt in points:
                lat, lon = pt.get("lat"), pt.get("lon")
                ts = pt.get("timestamp", "")
                cache_key = (round(lat, 1), round(lon, 1)) if lat and lon else None
                cached = self._sst_cache.get(cache_key) if cache_key else None
                if cached:
                    stale_results.append({"timestamp": ts, "lat": lat, "lon": lon, "sst_c": cached[0]})
                    stale_hits += 1
                else:
                    stale_results.append({"timestamp": ts, "lat": lat, "lon": lon, "sst_c": None})
            if stale_hits > 0:
                logger.warning(f"[SST] All ERDDAP down — returning {stale_hits}/{len(points)} stale cached SST values")
            else:
                logger.warning("[SST] All ERDDAP sources down, no cached SST available")
            return stale_results

        # Update preferred source for future calls
        if working_idx != self._sst_source_idx:
            old_label = self.ERDDAP_SST_SOURCES[self._sst_source_idx][2]  # label is index 2
            new_label = self.ERDDAP_SST_SOURCES[working_idx][2]
            logger.info(f"[SST] Switching from {old_label} to {new_label}")
            NOAAClient._sst_source_idx = working_idx

        base_url, var_name, label, lon_360 = self.ERDDAP_SST_SOURCES[working_idx]
        logger.debug(f"[SST] Using source: {label} ({base_url})")

        async def fetch_single_sst(pt):
            lat = pt.get("lat")
            lon = pt.get("lon")
            ts = pt.get("timestamp", "")

            if lat is None or lon is None or not ts:
                return {"timestamp": ts, "lat": lat, "lon": lon, "sst_c": None}

            # Parse timestamp to date string for ERDDAP
            try:
                if isinstance(ts, str):
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                else:
                    dt = ts
                date_str = dt.strftime("%Y-%m-%dT12:00:00Z")
            except Exception:
                return {"timestamp": ts, "lat": lat, "lon": lon, "sst_c": None}

            query_lon = self._convert_lon(lon, lon_360)
            url = f"{base_url}.json?{var_name}[({date_str})][({lat})][({query_lon})]"

            try:
                resp = await self.http.get(url, timeout=10.0)
                if resp.status_code != 200:
                    logger.debug(f"SST HTTP {resp.status_code} for ({lat},{lon},{date_str})")
                    return {"timestamp": ts, "lat": lat, "lon": lon, "sst_c": None}
                data = resp.json()
                rows = data.get("table", {}).get("rows", [])
                if rows and len(rows[0]) >= 4:
                    sst_val = rows[0][3]  # 4th column is the SST value
                    # ERDDAP returns SST in Celsius; filter out fill values
                    if sst_val is not None and -5 < sst_val < 45:
                        sst_rounded = round(sst_val, 2)
                        # Cache for stale fallback when ERDDAP goes down
                        cache_key = (round(lat, 1), round(lon, 1))
                        NOAAClient._sst_cache[cache_key] = (sst_rounded, utcnow())
                        # Evict oldest if cache is full
                        if len(NOAAClient._sst_cache) > NOAAClient._SST_CACHE_MAX:
                            oldest = min(NOAAClient._sst_cache, key=lambda k: NOAAClient._sst_cache[k][1])
                            del NOAAClient._sst_cache[oldest]
                        return {"timestamp": ts, "lat": lat, "lon": lon, "sst_c": sst_rounded}
            except Exception as e:
                logger.debug(f"SST fetch error for ({lat},{lon}): {type(e).__name__}")

            return {"timestamp": ts, "lat": lat, "lon": lon, "sst_c": None}

        # Fetch in parallel batches (limit concurrency to avoid overwhelming ERDDAP)
        semaphore = asyncio.Semaphore(10)

        async def bounded_fetch(pt):
            async with semaphore:
                return await fetch_single_sst(pt)

        tasks = [bounded_fetch(pt) for pt in points]
        results = await asyncio.gather(*tasks)

        valid = sum(1 for r in results if r.get("sst_c") is not None)
        logger.debug(f"[SST] Done: {valid}/{len(results)} points with valid SST from {label}")

        return list(results)

    # ==================================================================
    #  ACTIVE STORMS
    # ==================================================================

    async def get_active_storms(self, include_jtwc: bool = True) -> list[dict]:
        """
        Fetch currently active tropical cyclones across all basins.

        NHC covers Atlantic (AL) and Eastern Pacific (EP) storms only.
        JTWC covers Western Pacific (WP), North Indian Ocean (IO), and
        Southern Hemisphere (SH) storms. By default we merge both so the
        live pipeline surfaces typhoons like Sinlaku, not just hurricanes.

        Set `include_jtwc=False` to preserve the legacy NHC-only behavior.

        Returns a list of dicts with basic storm info including wind radii
        by quadrant when available. Each dict carries a `source` field
        ("NHC" or "JTWC") and a `basin` field so callers can route further.
        """
        # Fire NHC and JTWC in parallel — neither blocks the other.
        nhc_task = asyncio.create_task(self._fetch_nhc_active_storms())
        jtwc_task: Optional[asyncio.Task] = None
        if include_jtwc:
            jtwc_task = asyncio.create_task(self._fetch_jtwc_active_storms())

        storms: list[dict] = []
        try:
            nhc_storms = await nhc_task
            storms.extend(nhc_storms)
        except (httpx.HTTPError, NOAAClientError) as e:
            # If JTWC is also going to run, don't fail the whole call on NHC
            if jtwc_task is None:
                raise NOAAClientError(f"Failed to fetch active storms: {e}") from e
            logger.warning(f"[ACTIVE_STORMS] NHC fetch failed: {e}")

        if jtwc_task is not None:
            try:
                jtwc_storms = await jtwc_task
                # De-dupe by storm id, case-insensitively: NHC ids are lowercase
                # ("ep032026") while JTWC's are uppercase ("EP032026"), and JTWC
                # sometimes carries NHC-basin storms — a raw-string compare let
                # the same storm through twice (seen live: Cristina duplicated).
                seen = {s["id"].upper() for s in storms if s.get("id")}
                for s in jtwc_storms:
                    sid = (s.get("id") or "").upper()
                    if sid and sid not in seen:
                        storms.append(s)
                        seen.add(sid)
            except Exception as e:
                logger.warning(f"[ACTIVE_STORMS] JTWC fetch failed: {e}")

        return storms

    async def _fetch_nhc_active_storms(self) -> list[dict]:
        """NHC CurrentStorms.json — Atlantic, Eastern Pacific, and Central
        Pacific (CPHC storms appear here too, e.g. cp012026 Lala)."""
        resp = await self._request_with_retry("GET", NHC_ACTIVE_STORMS_URL)
        resp.raise_for_status()
        data = resp.json()
        storms = []
        for feature in data.get("activeStorms", []):
            storm_id = feature.get("id", "")
            basin = storm_id[:2].upper() if len(storm_id) >= 2 else ""
            storms.append({
                "id": storm_id,
                "name": feature.get("name", ""),
                "classification": feature.get("classification", ""),
                "lat": feature.get("latitudeNumeric") or feature.get("lat"),
                "lon": feature.get("longitudeNumeric") or feature.get("lon"),
                "intensity_knots": feature.get("intensity"),
                "pressure_mb": feature.get("pressure"),
                "movement": feature.get("movement", ""),
                "movement_speed_knots": feature.get("movementSpeed"),
                "movement_direction_deg": feature.get("movementDir"),
                "basin": basin,
                "source": "NHC",
            })
        return storms

    async def _fetch_jtwc_active_storms(self) -> list[dict]:
        """JTWC — Western Pacific, North Indian, Southern Hemisphere."""
        # Import lazily so tests / environments without JTWC reachability
        # don't pay any import-time cost.
        from services.jtwc_client import JTWCClient, JTWCClientError

        try:
            async with JTWCClient(http_client=self._http_client) as jtwc:
                return await jtwc.get_active_storms()
        except JTWCClientError as e:
            logger.warning(f"[ACTIVE_STORMS] JTWC: {e}")
            return []

    # ==================================================================
    #  FORECAST TRACK (NHC GIS)
    # ==================================================================

    async def get_forecast_track(self, storm_id: str) -> dict:
        """
        Fetch NHC forecast track and cone for an active storm.

        NHC retired the gis/forecast/archive/*_5day_latest.json GeoJSON
        convention (those URLs are plain 404s now), so this reads the
        products CurrentStorms.json actually advertises per storm:

          - forecast track: the forecast advisory (TCM) text product —
            rigidly formatted, parsed with regex (same philosophy as the
            JTWC warning-bulletin path).
          - cone: the official cone KMZ (zipped KML, stdlib parse).

        NHC only covers Atlantic (AL) and Eastern Pacific (EP) basins.
        For other basins (WP, IO, SH) this returns an empty result
        immediately — those are handled by the JTWC synthesis path in
        api/routes.

        Returns a dict with forecast_track (list of positions) and
        cone_polygon (list of lat/lon pairs).
        """
        basin = storm_id[:2].lower()

        result = {"storm_id": storm_id, "forecast_track": [], "cone_polygon": []}

        if basin not in ("al", "ep", "cp"):
            logger.debug(f"Skipping NHC forecast track for non-NHC basin: {basin.upper()}")
            return result

        # Per-storm product links come from CurrentStorms.json — the one
        # authoritative index of what NHC is publishing right now.
        entry = None
        try:
            resp = await self._request_with_retry("GET", NHC_ACTIVE_STORMS_URL, timeout=10.0)
            resp.raise_for_status()
            for s in resp.json().get("activeStorms", []):
                if str(s.get("id", "")).lower() == storm_id.lower():
                    entry = s
                    break
        except (httpx.HTTPError, NOAAClientError, ValueError) as e:
            logger.warning(f"[forecast] CurrentStorms lookup failed for {storm_id}: {e}")
        if entry is None:
            logger.info(f"[forecast] {storm_id} not in CurrentStorms.json — no NHC forecast")
            return result

        # Forecast track from the TCM forecast advisory (full advisories
        # only — intermediates carry no forecast table, and CurrentStorms
        # always links the latest full one).
        adv_url = (entry.get("forecastAdvisory") or {}).get("url") or ""
        if adv_url:
            try:
                resp = await self._request_with_retry("GET", adv_url, timeout=10.0)
                resp.raise_for_status()
                result["forecast_track"] = _parse_tcm_forecast(resp.text)
            except (httpx.HTTPError, NOAAClientError) as e:
                logger.warning(f"[forecast] TCM fetch failed for {storm_id}: {e}")

        # Official uncertainty cone from the cone KMZ. If this fails the
        # route synthesizes a climatological cone from the track instead.
        kmz_url = (entry.get("trackCone") or {}).get("kmzFile") or ""
        if kmz_url:
            try:
                resp = await self._request_with_retry("GET", kmz_url, timeout=15.0)
                resp.raise_for_status()
                result["cone_polygon"] = _parse_cone_kmz(resp.content)
            except (httpx.HTTPError, NOAAClientError) as e:
                logger.warning(f"[forecast] cone KMZ fetch failed for {storm_id}: {e}")

        return result

    # ==================================================================
    #  STORM SNAPSHOT (enriched with quadrant wind radii)
    # ==================================================================

    async def get_storm_snapshot(
        self,
        storm_id: str,
        advisory: Optional[str] = None,
    ) -> HurricaneSnapshot:
        """
        Get the latest (or specific) advisory snapshot for a storm.

        Now enriched with:
          - Per-quadrant wind radii (34, 50, 64 kt) for asymmetric modeling
          - Forward speed and heading for asymmetry correction
          - RMW when available
        """
        # NHC retired the gis/forecast/archive/*_fcst_latest.json advisory
        # GeoJSON (guaranteed 404 — see get_forecast_track), so go straight
        # to the CurrentStorms-derived snapshot instead of burning a dead
        # round-trip first. Quadrant radii come from the b-deck path, not
        # from here.
        storms = await self.get_active_storms()
        match = next((s for s in storms if s["id"] == storm_id), None)
        if match is None:
            raise NOAAClientError(f"Storm {storm_id} not found")
        return self._snapshot_from_active_storm(match)

    def _snapshot_from_active_storm(self, storm: dict) -> HurricaneSnapshot:
        """Convert active storm dict to an enriched HurricaneSnapshot."""
        intensity_kt = storm.get("intensity_knots") or 0

        # Extract forward motion for asymmetry
        fwd_speed = storm.get("movement_speed_knots")
        fwd_dir = storm.get("movement_direction_deg")

        snap = HurricaneSnapshot(
            storm_id=storm["id"],
            name=storm.get("name", "UNKNOWN"),
            timestamp=utcnow(),
            lat=float(storm.get("lat", 0)),
            lon=float(storm.get("lon", 0)),
            max_wind_ms=knots_to_ms(float(intensity_kt)),
            min_pressure_hpa=storm.get("pressure_mb"),
            forward_speed_ms=knots_to_ms(float(fwd_speed)) if fwd_speed else None,
            forward_direction_deg=float(fwd_dir) if fwd_dir else None,
        )
        return snap

    def _parse_advisory_json(
        self, storm_id: str, data: dict
    ) -> HurricaneSnapshot:
        """Parse NHC advisory JSON into an enriched snapshot with quadrant radii."""
        props = data.get("properties", data)
        coords = data.get("geometry", {}).get("coordinates", [0, 0])

        max_wind_kt = float(props.get("maxSustainedWind", 0))
        rmw_nm = props.get("radiusOfMaxWind")

        # Extract per-quadrant wind radii for 34, 50, 64 kt thresholds
        wind_radii = props.get("windRadii", {})
        r34_quadrants = wind_radii.get("34", {})
        r50_quadrants = wind_radii.get("50", {})
        r64_quadrants = wind_radii.get("64", {})

        # Compute max r34 for outer extent
        r34_nm = None
        r34_ne = r34_se = r34_sw = r34_nw = None
        if r34_quadrants:
            r34_ne = r34_quadrants.get("NE") or r34_quadrants.get("ne")
            r34_se = r34_quadrants.get("SE") or r34_quadrants.get("se")
            r34_sw = r34_quadrants.get("SW") or r34_quadrants.get("sw")
            r34_nw = r34_quadrants.get("NW") or r34_quadrants.get("nw")
            r34_vals = [v for v in [r34_ne, r34_se, r34_sw, r34_nw] if v]
            if r34_vals:
                r34_nm = max(r34_vals)

        # Forward motion
        fwd_speed = props.get("movementSpeed")
        fwd_dir = props.get("movementDirection")

        snap = HurricaneSnapshot(
            storm_id=storm_id,
            name=props.get("name", "UNKNOWN"),
            timestamp=datetime.fromisoformat(
                props.get("timestamp", utcnow().isoformat())
            ),
            lat=float(coords[1]) if len(coords) > 1 else 0.0,
            lon=float(coords[0]) if len(coords) > 0 else 0.0,
            max_wind_ms=knots_to_ms(max_wind_kt),
            min_pressure_hpa=props.get("minCentralPressure"),
            rmw_m=nm_to_meters(float(rmw_nm)) if rmw_nm else None,
            r34_m=nm_to_meters(float(r34_nm)) if r34_nm else None,
            r34_quadrants_m={
                "NE": nm_to_meters(float(r34_ne)) if r34_ne else None,
                "SE": nm_to_meters(float(r34_se)) if r34_se else None,
                "SW": nm_to_meters(float(r34_sw)) if r34_sw else None,
                "NW": nm_to_meters(float(r34_nw)) if r34_nw else None,
            } if any([r34_ne, r34_se, r34_sw, r34_nw]) else None,
            r50_quadrants_m=_parse_quadrants(r50_quadrants),
            r64_quadrants_m=_parse_quadrants(r64_quadrants),
            forward_speed_ms=knots_to_ms(float(fwd_speed)) if fwd_speed else None,
            forward_direction_deg=float(fwd_dir) if fwd_dir else None,
        )
        return snap

    # ==================================================================
    #  HISTORICAL BEST-TRACK (HURDAT2) — enhanced parsing
    # ==================================================================

    async def get_historical_track(
        self, storm_id: str
    ) -> list[HurricaneSnapshot]:
        """
        Fetch historical best-track data from EBTRK (if available) falling back to HURDAT2.

        EBTRK provides more complete quadrant wind radii for historical storms,
        especially important for accurate IKE calculations of pre-2010 storms.
        Enhanced to extract full quadrant wind radii at 34, 50, and 64 kt
        thresholds from the extended data formats.

        Routes between Atlantic and East Pacific HURDAT2 files based on
        the storm_id prefix (AL → Atlantic file, EP → East Pacific file).
        """
        # EBTRK URLs at Colorado State have been 404 since ~2018.
        # Skip the attempt entirely to avoid noisy HTTP error logs.
        # The EBTRK parser code is retained in case a mirror appears.
        hurdat_text = await self._fetch_hurdat2(storm_id=storm_id)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._parse_hurdat2, hurdat_text, storm_id)

    async def _fetch_hurdat2(self, storm_id: str = "") -> str:
        """Download HURDAT2 text file (cached if cache_dir set).

        File I/O is offloaded to avoid blocking the event loop. Picks the
        Atlantic or East Pacific HURDAT2 file based on the storm_id prefix
        via _hurdat2_url_for. Each basin is cached as its own file
        (hurdat2_atl.txt vs hurdat2_epac.txt) so an Atlantic miss doesn't
        invalidate an EP cache hit and vice versa.

        Backward-compat: if storm_id is empty (older callers), defaults
        to the Atlantic file.
        """
        url, cache_name = _hurdat2_url_for(storm_id)
        log_tag = "HURDAT2-EP" if cache_name == "hurdat2_epac.txt" else "HURDAT2"

        if self.cache_dir:
            cached = self.cache_dir / cache_name
            if cached.exists():
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, cached.read_text)

        if NOAAClient.nhc_is_down():
            # NHC is down — try stale cache before giving up
            if self.cache_dir:
                stale = self.cache_dir / cache_name
                if stale.exists():
                    logger.warning(f"[{log_tag}] NHC down, returning stale cached data")
                    loop = asyncio.get_event_loop()
                    return await loop.run_in_executor(None, stale.read_text)
            raise NOAAClientError(f"NHC recently unreachable — skipping {log_tag}")

        try:
            resp = await self.http.get(url, timeout=10.0)
            resp.raise_for_status()
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            NOAAClient.mark_nhc_down()
            # Try stale cache before raising
            if self.cache_dir:
                stale = self.cache_dir / cache_name
                if stale.exists():
                    logger.warning(f"[{log_tag}] NHC unreachable ({e}), returning stale cached data")
                    loop = asyncio.get_event_loop()
                    return await loop.run_in_executor(None, stale.read_text)
            raise NOAAClientError(f"NHC unreachable: {e}") from e
        except httpx.HTTPStatusError as e:
            # 404 on the EP file most likely means the date stamp in
            # HURDAT2_EPAC_URL drifted (NHC sometimes publishes the EP
            # and Atlantic reanalyses on different days). Don't mark
            # NHC down — let the caller fall through to IBTrACS.
            raise NOAAClientError(f"{log_tag} unavailable: HTTP {e.response.status_code}") from e

        NOAAClient.mark_nhc_up()
        text = resp.text

        if self.cache_dir:
            def _write():
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                (self.cache_dir / cache_name).write_text(text)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _write)

        return text

    def _parse_hurdat2(
        self, text: str, target_storm_id: str
    ) -> list[HurricaneSnapshot]:
        """
        Parse HURDAT2 extended format with post-processing for missing quadrant data.

        HURDAT2 data lines (post-2004) contain wind radii:
          date, time, record_id, status, lat, lon, maxwind, minpres,
          r34_NE, r34_SE, r34_SW, r34_NW,
          r50_NE, r50_SE, r50_SW, r50_NW,
          r64_NE, r64_SE, r64_SW, r64_NW,
          rmw (column 20 in some versions)
        """
        snapshots = []
        lines = text.strip().split("\n")
        current_id = None
        current_name = None
        in_target = False

        for line in lines:
            parts = [p.strip() for p in line.split(",")]

            # Header line
            if len(parts) >= 3 and len(parts[0]) == 8 and parts[0][:2].isalpha():
                current_id = parts[0]
                current_name = parts[1]
                in_target = current_id == target_storm_id
                continue

            if not in_target:
                continue

            if len(parts) < 8:
                continue

            date_str = parts[0]
            time_str = parts[1]
            lat_str = parts[4]
            lon_str = parts[5]
            max_wind_kt = float(parts[6])
            min_pres = float(parts[7]) if parts[7] and parts[7] != "-999" else None

            lat = float(lat_str[:-1]) * (1 if lat_str[-1] == "N" else -1)
            lon = float(lon_str[:-1]) * (-1 if lon_str[-1] == "W" else 1)
            ts = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M")

            # Extract full quadrant wind radii (34 kt: cols 8-11, 50 kt: 12-15, 64 kt: 16-19)
            r34_quads = _extract_quadrants(parts, 8)
            r50_quads = _extract_quadrants(parts, 12)
            r64_quads = _extract_quadrants(parts, 16)

            r34_nm = None
            if r34_quads:
                r34_vals = [v for v in r34_quads.values() if v is not None]
                r34_nm = max(r34_vals) / 1852.0 if r34_vals else None  # back to nm for r34_m

            r64_nm = None
            if r64_quads:
                r64_vals = [v for v in r64_quads.values() if v is not None]
                r64_nm = max(r64_vals) / 1852.0 if r64_vals else None  # max quadrant in nm

            # RMW if present (column 20 in some extended formats)
            rmw_nm = None
            if len(parts) > 20:
                try:
                    rmw_val = float(parts[20])
                    if rmw_val > 0:
                        rmw_nm = rmw_val
                except (ValueError, IndexError):
                    pass

            snapshots.append(HurricaneSnapshot(
                storm_id=current_id,
                name=current_name,
                timestamp=ts,
                lat=lat,
                lon=lon,
                max_wind_ms=knots_to_ms(max_wind_kt),
                min_pressure_hpa=min_pres if min_pres and min_pres > 0 else None,
                rmw_m=nm_to_meters(rmw_nm) if rmw_nm else None,
                r34_m=nm_to_meters(r34_nm) if r34_nm else None,
                r64_m=nm_to_meters(r64_nm) if r64_nm else None,
                r34_quadrants_m=r34_quads,
                r50_quadrants_m=r50_quads,
                r64_quadrants_m=r64_quads,
            ))
            
            # Debug: log if quadrant data is missing
            if not r34_quads:
                logger.debug(f"{current_id} {ts}: No r34 quadrants found (cols 8-11: {parts[8:12]})")

        # NOTE: Disabled quadrant interpolation as it made IKE worse
        # (neighboring quadrant data doesn't match peak intensity wind speeds)
        # Need proper EBTRK data or alternative wind field source instead
        
        return snapshots

    # ==================================================================
    #  EXTENDED BEST TRACK (EBTRK) — improved quadrant radii
    # ==================================================================

    async def _fetch_and_parse_ebtrk(self, storm_id: str) -> Optional[list[HurricaneSnapshot]]:
        """Fetch EBTRK data and extract snapshots for target storm."""
        # Determine basin from storm_id (first 2 chars: AL, EP, WP, etc.)
        basin_code = storm_id[:2].upper()
        
        # Map basin code to EBTRK filename
        ebtrk_filemap = {
            "AL": EBTRK_URL,  # Atlantic
            "EP": EBTRK_EP_URL,  # East Pacific
        }
        
        if basin_code not in ebtrk_filemap:
            logger.debug(f"No EBTRK data for basin {basin_code}")
            return None
        
        ebtrk_url = ebtrk_filemap[basin_code]
        
        try:
            resp = await self.http.get(ebtrk_url, timeout=15.0)
            resp.raise_for_status()
            return self._parse_ebtrk(resp.text, storm_id)
        except httpx.TimeoutException:
            logger.warning(f"EBTRK timeout for {storm_id}")
            return None
        except httpx.HTTPError as e:
            logger.warning(f"EBTRK HTTP error for {storm_id}: {e}")
            return None

    def _parse_ebtrk(self, text: str, target_storm_id: str) -> Optional[list[HurricaneSnapshot]]:
        """
        Parse NOAA/AOML Extended Best Track (EBTRK) format.

        EBTRK is a fixed-width text format with complete quadrant wind radii.
        Format varies slightly by basin, but core structure is:
          - Header lines starting with "Year"
          - Data lines with: Year, Month, Day, Hour, Status, Lat, Lon, MaxWind,
            MinPress, r34_NE, r34_SE, r34_SW, r34_NW, r50_NE, r50_SE, r50_SW, r50_NW,
            r64_NE, r64_SE, r64_SW, r64_NW, ...

        CRITICAL FIX (2026-03-17): EBTRK data lines don't include storm IDs, so we filter
        by year extracted from target_storm_id, then use track continuity heuristics to
        identify which storm sequence in the file matches the target. This prevents
        mixing data from multiple different storms in the same year.
        """
        # Extract target year and storm number from ID (e.g., "AL142018" -> 2018, 14)
        try:
            target_year = int(target_storm_id[4:8])
            target_storm_num = int(target_storm_id[2:4])
        except (ValueError, IndexError):
            logger.warning(f"Invalid storm ID format: {target_storm_id}")
            return None

        all_snapshots = []
        lines = text.strip().split("\n")

        # Skip header lines until we find data
        data_started = False

        for line in lines:
            line = line.strip()
            if not line or line.startswith("Year") or line.startswith("---"):
                data_started = True
                continue

            if not data_started:
                continue

            # Parse EBTRK data line
            parts = line.split()
            if len(parts) < 9:
                continue

            try:
                year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])
                hour = int(parts[3])

                # CRITICAL: Filter by year first - this prevents mixing storms from different years
                if year != target_year:
                    continue

                status = parts[4].strip()
                lat_str = parts[5]
                lon_str = parts[6]

                # Parse lat/lon with hemisphere indicators
                if lat_str.endswith("N"):
                    lat = float(lat_str[:-1])
                else:
                    lat = float(lat_str.rstrip("S")) * -1

                if lon_str.endswith("W"):
                    lon = float(lon_str[:-1]) * -1
                else:
                    lon = float(lon_str.rstrip("E"))

                # Wind and pressure
                max_wind_kt = float(parts[7]) if parts[7] != "-" else 0
                min_pres = float(parts[8]) if parts[8] != "-" and parts[8] != "-999" else None

                # Extract quadrant wind radii if available
                # Positions vary, but try standard layout: 34kt quads at 9-12, 50kt at 13-16, 64kt at 17-20
                r34_quads = None
                r50_quads = None
                r64_quads = None

                if len(parts) >= 13:
                    r34_quads = _parse_ebtrk_quadrants(parts, 9)
                if len(parts) >= 17:
                    r50_quads = _parse_ebtrk_quadrants(parts, 13)
                if len(parts) >= 21:
                    r64_quads = _parse_ebtrk_quadrants(parts, 17)

                # Build timestamp
                ts = datetime(year, month, day, hour)

                # Extract r34/r64 max for overall storm size
                r34_nm = None
                if r34_quads:
                    r34_vals = [v for v in r34_quads.values() if v is not None]
                    r34_nm = max(r34_vals) / 1852.0 if r34_vals else None

                r64_nm = None
                if r64_quads:
                    r64_vals = [v for v in r64_quads.values() if v is not None]
                    r64_nm = max(r64_vals) / 1852.0 if r64_vals else None

                all_snapshots.append({
                    'snapshot': HurricaneSnapshot(
                        storm_id=target_storm_id,
                        name="",  # EBTRK doesn't include storm name
                        timestamp=ts,
                        lat=lat,
                        lon=lon,
                        max_wind_ms=knots_to_ms(max_wind_kt) if max_wind_kt > 0 else 0,
                        min_pressure_hpa=min_pres if min_pres and min_pres > 0 else None,
                        r34_m=nm_to_meters(r34_nm) if r34_nm else None,
                        r64_m=nm_to_meters(r64_nm) if r64_nm else None,
                        r34_quadrants_m=r34_quads,
                        r50_quadrants_m=r50_quads,
                        r64_quadrants_m=r64_quads,
                    ),
                    'wind_kt': max_wind_kt,
                    'lat': lat,
                    'lon': lon,
                    'ts': ts
                })

            except (ValueError, IndexError) as e:
                logger.debug(f"Skipping EBTRK line: {e}")
                continue

        if not all_snapshots:
            logger.debug(f"No EBTRK data found for year {target_year} (storm {target_storm_id})")
            return None

        # Sort by timestamp
        all_snapshots = sorted(all_snapshots, key=lambda x: x['ts'])

        # Segment tracks by detecting discontinuities (large position jumps)
        # that indicate one storm ended and another began
        storm_segments = []
        current_segment = [all_snapshots[0]]

        for i in range(1, len(all_snapshots)):
            prev = all_snapshots[i-1]
            curr = all_snapshots[i]

            # Check for discontinuities that indicate a new storm
            time_gap_hours = (curr['ts'] - prev['ts']).total_seconds() / 3600
            lat_delta = abs(curr['lat'] - prev['lat'])
            lon_delta = abs(curr['lon'] - prev['lon'])

            # Large time gap (>12 hours) or position jump (>5 degrees) indicates new storm
            if time_gap_hours > 12 or lat_delta > 5 or lon_delta > 5:
                if current_segment:
                    storm_segments.append(current_segment)
                current_segment = [curr]
            else:
                current_segment.append(curr)

        if current_segment:
            storm_segments.append(current_segment)

        # Select the longest segment (typically the main storm track)
        # This handles cases where the file includes adjacent storms
        if storm_segments:
            selected_segment = max(storm_segments, key=len)
            snapshots = [s['snapshot'] for s in selected_segment]
            logger.debug(
                f"EBTRK: Segmented {target_year} into {len(storm_segments)} tracks; "
                f"selected longest segment ({len(snapshots)} points) for {target_storm_id}"
            )
            return snapshots if snapshots else None

        return None

    # ==================================================================
    #  HURDAT2 CATALOG — lightweight storm listing
    # ==================================================================

    async def get_storm_catalog(
        self, min_year: int = 2015, max_year: int = 2099
    ) -> list[dict]:
        """
        Parse HURDAT2 to get a catalog of all named storms with peak intensity.

        Returns a list of dicts with: id, name, year, peak_wind_kt, category (0-5).
        Much faster than computing IKE for every storm — only reads headers
        and peak wind from data lines.
        """
        hurdat_text = await self._fetch_hurdat2()
        return self._parse_hurdat2_catalog(hurdat_text, min_year, max_year)

    def _parse_hurdat2_catalog(
        self, text: str, min_year: int, max_year: int
    ) -> list[dict]:
        """Parse HURDAT2 header + peak wind + DPS fields for each storm in year range."""
        catalog = []
        lines = text.strip().split("\n")
        current = None

        def _finalize(entry):
            """Compute DPS and add category before appending to catalog."""
            if entry and entry["peak_wind_kt"] >= 34:
                entry["category"] = _wind_to_ss_category(entry["peak_wind_kt"])
                dps = calculate_dps(
                    wind_kt=entry["peak_wind_kt"],
                    pressure_hpa=entry.get("_pres"),
                    r34_nm=entry.get("_r34_nm"),
                    r64_nm=entry.get("_r64_nm"),
                    lat=entry.get("_lat"),
                    lon=entry.get("_lon"),
                )
                entry["peak_dps"] = dps["score"]
                entry["dps_label"] = dps["label"]
                # Strip private fields
                for k in list(entry.keys()):
                    if k.startswith("_"):
                        del entry[k]
                catalog.append(entry)

        for line in lines:
            parts = [p.strip() for p in line.split(",")]

            # Header line: "AL012015,            ANA,     10,"
            if len(parts) >= 3 and len(parts[0]) == 8 and parts[0][:2].isalpha():
                _finalize(current)

                storm_id = parts[0]
                name = parts[1].strip()
                try:
                    year = int(storm_id[4:])
                except ValueError:
                    current = None
                    continue

                if year < min_year or year > max_year:
                    current = None
                    continue

                if name == "UNNAMED" or not name:
                    current = None
                    continue

                current = {
                    "id": storm_id,
                    "name": name.title(),
                    "year": year,
                    "peak_wind_kt": 0,
                }
                continue

            # Data line: extract wind (col 6), pressure (col 7), lat/lon (cols 4-5),
            # r34 quadrants (cols 8-11), r64 quadrants (cols 16-19)
            if current and len(parts) >= 7:
                try:
                    wind = float(parts[6])
                    if wind > current["peak_wind_kt"]:
                        current["peak_wind_kt"] = wind
                        # Track DPS fields at peak intensity
                        pres = float(parts[7]) if len(parts) > 7 and parts[7] and parts[7] != "-999" else None
                        if pres and pres > 0:
                            current["_pres"] = pres
                        # Lat/lon
                        try:
                            lat_str, lon_str = parts[4], parts[5]
                            current["_lat"] = float(lat_str[:-1]) * (1 if lat_str[-1] == "N" else -1)
                            current["_lon"] = float(lon_str[:-1]) * (-1 if lon_str[-1] == "W" else 1)
                        except (ValueError, IndexError):
                            pass
                        # R34 (cols 8-11) and R64 (cols 16-19) — max of quadrants
                        if len(parts) > 11:
                            r34_vals = [_safe_float(parts[i]) for i in range(8, 12)]
                            r34_vals = [v for v in r34_vals if v and v > 0]
                            if r34_vals:
                                current["_r34_nm"] = max(r34_vals)
                        if len(parts) > 19:
                            r64_vals = [_safe_float(parts[i]) for i in range(16, 20)]
                            r64_vals = [v for v in r64_vals if v and v > 0]
                            if r64_vals:
                                current["_r64_nm"] = max(r64_vals)
                except (ValueError, IndexError):
                    pass

        # Don't forget the last storm
        _finalize(current)

        return catalog

    # ==================================================================
    #  IBTrACS CATALOG — lightweight global storm listing
    # ==================================================================

    async def get_ibtracs_catalog(
        self, min_year: int = 2015, max_year: int = 2099
    ) -> list[dict]:
        """
        Parse IBTrACS CSV to get a catalog of all named storms globally.

        Returns list of dicts: {id, name, year, basin, peak_wind_kt, category}.
        Uses the last-3-years file for recent storms, full archive for older.
        """
        current_year = utcnow().year

        catalog = {}  # keyed by SID to track peak wind across rows

        # Decide which file(s) to fetch
        if min_year >= current_year - 3:
            sources = [True]  # recent only
        elif max_year >= current_year - 3:
            sources = [True, False]  # recent + archive
        else:
            sources = [False]  # archive only

        for use_recent in sources:
            loop = asyncio.get_event_loop()

            # STREAM the CSV rather than materialising it. The full archive is
            # ~315 MB on disk; read_text() + io.StringIO held ~1.6 GB for the
            # length of the parse (StringIO widens ASCII to UCS-4 at 4.00
            # bytes/char, measured). With a 6-hour refresh TTL and an executor
            # wide enough for several to overlap, that was the dominant term
            # in this service's resident memory — 8.6 GB peak against a 2.1 GB
            # floor, which exhausted the hosting credits on 2026-07-31 and
            # took the site down. Streaming holds one row at a time.
            path = await self._ibtracs_path(use_recent=use_recent)
            if path is not None:
                # Parity with the old `if not csv_text` warning: a zero-byte
                # cache file would otherwise merge silently as an empty
                # catalog instead of announcing that a source is missing.
                try:
                    if path.stat().st_size == 0:
                        logger.warning(
                            f"IBTrACS cache file is empty: {path.name}")
                        continue
                except OSError:
                    logger.warning(f"IBTrACS cache file unreadable: {path}")
                    continue
                partial_catalog = await loop.run_in_executor(
                    None, self._parse_ibtracs_catalog_file, path, min_year, max_year
                )
            else:
                # No cache dir configured — fall back to the text path.
                csv_text = await self._fetch_ibtracs(use_recent=use_recent)
                if not csv_text:
                    logger.warning(
                        f"IBTrACS fetch returned empty: use_recent={use_recent}")
                    continue
                partial_catalog = await loop.run_in_executor(
                    None, self._parse_ibtracs_catalog_chunk, csv_text,
                    min_year, max_year
                )
            for sid, entry in partial_catalog.items():
                if sid not in catalog:
                    catalog[sid] = entry
                else:
                    if entry["peak_wind_kt"] > catalog[sid]["peak_wind_kt"]:
                        catalog[sid]["peak_wind_kt"] = entry["peak_wind_kt"]
                    if entry.get("_dps_wind_kt", 0) >= catalog[sid].get("_dps_wind_kt", 0):
                        for k in ("_dps_wind_kt", "_dps_pressure", "_dps_r34_nm",
                                  "_dps_r64_nm", "_dps_fwd_kt", "_dps_lat", "_dps_lon"):
                            if entry.get(k) is not None:
                                catalog[sid][k] = entry[k]

        # Filter to named tropical storms (>= 34 kt), compute DPS, add category
        result = []
        for entry in catalog.values():
            if entry["peak_wind_kt"] >= 34:
                entry["category"] = _wind_to_ss_category(entry["peak_wind_kt"])
                # Compute peak DPS from tracked fields
                # NOTE: IBTrACS may lack pressure/R data for recent storms, use wind-based fallback
                wind_kt = entry.get("_dps_wind_kt") or entry["peak_wind_kt"]  # Use peak wind if no DPS-specific data
                has_full_data = (
                    entry.get("_dps_pressure") is not None and 
                    (entry.get("_dps_r34_nm") or entry.get("_dps_r64_nm"))
                )
                
                if has_full_data:
                    # Full DPS calculation with pressure and wind radii
                    dps = calculate_dps(
                        wind_kt=wind_kt,
                        pressure_hpa=entry.get("_dps_pressure"),
                        r34_nm=entry.get("_dps_r34_nm"),
                        r64_nm=entry.get("_dps_r64_nm"),
                        forward_speed_kt=entry.get("_dps_fwd_kt"),
                        lat=entry.get("_dps_lat"),
                        lon=entry.get("_dps_lon"),
                    )
                else:
                    # Fallback: wind-speed-only DPS when pressure/R data missing
                    # This gives sensible ordering: higher wind = higher DPS
                    # Uses same 0-100 scale as full calculation
                    v_mph = wind_kt * 1.15078
                    # Simple formula: 40% V (wind), 60% pseudo-surge from wind alone
                    V = max(0.0, min((v_mph - 40) / 117, 1.0))  # 10% of 100
                    S = min(1.0, (v_mph - 40) / 150)  # Wind-derived pseudo-surge (60% of 100)
                    raw = 60.0 * S + 40.0 * V
                    score = min(100, round(raw))
                    # Canonical banding — this fallback previously carried the
                    # pre-canon "Catastrophic"/"Minor" scheme (cross-surface
                    # score audit 2026-07-10).
                    from core.dpi import categorize_dpi
                    dps = {"score": score, "label": categorize_dpi(score)}
                
                entry["peak_dps"] = dps["score"]
                entry["dps_label"] = dps["label"]
                # Strip private DPS tracking fields
                for k in list(entry.keys()):
                    if k.startswith("_dps_"):
                        del entry[k]
                result.append(entry)

        # Sort by year descending, then DPS descending (was peak_wind_kt)
        result.sort(key=lambda s: (-s["year"], -s.get("peak_dps", 0)))
        logger.info(f"IBTrACS catalog final: {len(result)} storms returned (years {min_year}-{max_year})")
        return result

    # ==================================================================
    #  GRIDDED WIND FIELD (GFS via NOMADS) — now with GRIB2 decoding
    # ==================================================================

    async def get_gridded_wind_field(
        self,
        storm_id: str,
        lat: float,
        lon: float,
        radius_deg: float = 5.0,
    ) -> Optional[WindFieldGrid]:
        """
        Fetch and decode gridded wind field data from NOAA GFS.

        Downloads GRIB2 data from NOMADS for 10m UGRD/VGRD, decodes it,
        computes wind speed magnitudes, and converts the lat/lon grid
        to a local meter-based coordinate system centered on the storm.
        """
        params = {
            "file": "gfs.t00z.pgrb2.0p25.f000",
            "var_UGRD": "on",
            "var_VGRD": "on",
            "lev_10_m_above_ground": "on",
            "subregion": "",
            "leftlon": str(lon - radius_deg),
            "rightlon": str(lon + radius_deg),
            "toplat": str(lat + radius_deg),
            "bottomlat": str(lat - radius_deg),
            "dir": "/gfs.{}/00/atmos".format(
                utcnow().strftime("%Y%m%d")
            ),
        }

        try:
            resp = await self.http.get(NOAA_NOMADS_BASE, params=params, timeout=45.0)
            resp.raise_for_status()
            grib_data = resp.content

            if len(grib_data) < 100:
                logger.warning(f"GRIB2 response too small ({len(grib_data)} bytes)")
                return None

            logger.info(f"Received {len(grib_data)} bytes of GRIB2 for {storm_id}")

            # Decode GRIB2 messages
            messages = decode_grib2(grib_data)
            if not messages:
                logger.warning("No GRIB2 messages decoded")
                return None

            # Extract U and V wind components at 10m
            u_msg, v_msg = extract_wind_components(messages)
            if u_msg is None or v_msg is None:
                logger.warning(
                    f"Missing wind components: U={'found' if u_msg else 'missing'}, "
                    f"V={'found' if v_msg else 'missing'}"
                )
                return None

            # Compute wind speed from U, V
            lats, lons, wind_speed = compute_wind_speed_grid(u_msg, v_msg)

            # Convert lat/lon grid to local meters centered on storm
            x_m, y_m = _latlon_to_local_meters(lats, lons, lat, lon)

            return WindFieldGrid(
                x=x_m,
                y=y_m,
                wind_speed=wind_speed,
                timestamp=utcnow(),
            )

        except httpx.HTTPError as e:
            logger.warning(f"Could not fetch gridded data for {storm_id}: {e}")
            return None
        except Exception as e:
            logger.warning(f"GRIB2 decode failed for {storm_id}: {e}")
            return None

    # ==================================================================
    #  IBTrACS — International Best Track Archive
    # ==================================================================

    async def get_ibtracs_track(
        self,
        sid: str,
        use_recent: bool = True,
    ) -> list[HurricaneSnapshot]:
        """
        Fetch storm track from IBTrACS by storm ID (SID).

        IBTrACS provides global tropical cyclone data from all agencies,
        enabling cross-validation against HURDAT2 and coverage of
        non-Atlantic basins (West Pacific, Indian Ocean, etc.).

        Args:
            sid: IBTrACS storm ID, e.g. '2005236N23285' for Katrina
            use_recent: if True, use the smaller last-3-years file first

        Returns:
            List of HurricaneSnapshot from IBTrACS data
        """
        csv_text = await self._fetch_ibtracs(use_recent=use_recent)
        loop = asyncio.get_event_loop()
        snapshots = await loop.run_in_executor(None, self._parse_ibtracs_csv, csv_text, sid)

        # If not found in recent, try the full archive
        if not snapshots and use_recent:
            csv_text = await self._fetch_ibtracs(use_recent=False)
            snapshots = await loop.run_in_executor(None, self._parse_ibtracs_csv, csv_text, sid)

        return snapshots

    async def get_ibtracs_by_name_year(
        self,
        name: str,
        year: int,
        basin: Optional[str] = None,
    ) -> list[HurricaneSnapshot]:
        """
        Search IBTrACS by storm name and year.

        Args:
            name: storm name (e.g., "KATRINA")
            year: season year
            basin: optional basin filter (NA, EP, WP, NI, SI, SP, SA)
        """
        use_recent = year >= (utcnow().year - 3)
        csv_text = await self._fetch_ibtracs(use_recent=use_recent)
        if not csv_text and use_recent:
            csv_text = await self._fetch_ibtracs(use_recent=False)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._search_ibtracs_csv, csv_text, name.upper(), year, basin)

    async def get_ibtracs_by_name(
        self,
        name: str,
        basin: Optional[str] = None,
    ) -> list[HurricaneSnapshot]:
        """
        Search IBTrACS by storm name only (no year). Returns snapshots for the
        most recent storm matching the name. Searches recent file first, then
        falls back to the full archive.

        Args:
            name: storm name (e.g., "KATRINA")
            basin: optional basin filter (NA, EP, WP, NI, SI, SP, SA)
        """
        # Search recent first (faster, smaller file)
        csv_text = await self._fetch_ibtracs(use_recent=True)
        if not csv_text:
            csv_text = await self._fetch_ibtracs(use_recent=False)

        loop = asyncio.get_event_loop()
        snapshots = await loop.run_in_executor(
            None, self._search_ibtracs_name_only, csv_text, name.upper(), basin
        )

        # If nothing in recent, try full archive
        if not snapshots:
            csv_text = await self._fetch_ibtracs(use_recent=False)
            if csv_text:
                snapshots = await loop.run_in_executor(
                    None, self._search_ibtracs_name_only, csv_text, name.upper(), basin
                )

        return snapshots

    async def _fetch_ibtracs(self, use_recent: bool = True) -> str:
        """Download IBTrACS CSV (cached).

        File I/O is offloaded to avoid blocking the event loop.
        IBTrACS files can be 50-100MB+ for the full archive.

        Network resilience: if the download fails and a stale cached copy
        exists on disk, return the stale data rather than failing outright.
        """
        cache_name = "ibtracs_recent.csv" if use_recent else "ibtracs_all.csv"
        url = IBTRACS_LAST3_URL if use_recent else IBTRACS_CSV_URL
        loop = asyncio.get_event_loop()

        if self.cache_dir:
            cached = self.cache_dir / cache_name
            if cached.exists():
                return await loop.run_in_executor(None, cached.read_text)

        try:
            resp = await self.http.get(url, timeout=120.0)
            resp.raise_for_status()
            text = resp.text
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            # Network failed — try to return stale cache rather than crashing
            if self.cache_dir:
                stale = self.cache_dir / cache_name
                if stale.exists():
                    logger.warning(
                        f"[IBTrACS] Download failed ({type(e).__name__}), "
                        f"returning stale cached data from {stale}"
                    )
                    return await loop.run_in_executor(None, stale.read_text)
            raise  # No cache at all — propagate error

        if self.cache_dir:
            def _write():
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                (self.cache_dir / cache_name).write_text(text)
            await loop.run_in_executor(None, _write)

        return text

    async def _ibtracs_path(self, use_recent: bool = True):
        """Ensure the IBTrACS CSV is on disk and return its Path (or None).

        The point is what this does NOT do: it never reads the file into
        memory. Callers that only need to iterate rows should take this and
        stream, rather than calling _fetch_ibtracs and building a 315 MB str.

        Downloading is rare — the cache file has no TTL, so once present it
        is reused — which is why the download itself is left on the existing
        _fetch_ibtracs path rather than duplicated here. The recurring cost
        this avoids is the every-6-hours re-read and parse.
        """
        if not self.cache_dir:
            return None
        cache_name = "ibtracs_recent.csv" if use_recent else "ibtracs_all.csv"
        cached = self.cache_dir / cache_name
        if cached.exists():
            return cached
        # Not cached yet: let the existing path download + write it, then
        # hand back the file it just created. The one-time text
        # materialisation here is acceptable; the repeated one was not.
        await self._fetch_ibtracs(use_recent=use_recent)
        return cached if cached.exists() else None

    def _parse_ibtracs_catalog_chunk(
        self, csv_text: str, min_year: int, max_year: int
    ) -> dict:
        """Text-input wrapper. Prefer _parse_ibtracs_catalog_file.

        This holds the whole CSV as a str AND then again as an io.StringIO
        copy, and StringIO widens ASCII to UCS-4 — measured at exactly
        4.00 bytes/char. For the 315 MB full archive that is ~1.6 GB resident
        for the duration of the parse. Kept only for callers that already
        have the text in hand.
        """
        return self._parse_ibtracs_catalog_rows(
            csv.DictReader(io.StringIO(csv_text)), min_year, max_year)

    def _parse_ibtracs_catalog_file(
        self, path, min_year: int, max_year: int
    ) -> dict:
        """Stream one IBTrACS CSV off disk into a catalog dict.

        csv.DictReader pulls line by line straight from the file object, so
        peak memory is one row plus the accumulated catalog instead of
        ~5x the file size. This is the path the 6-hourly global-catalog
        refresh takes; see get_ibtracs_catalog.

        newline='' is the csv module's documented requirement (it lets the
        reader handle newlines embedded in quoted fields itself);
        errors='replace' keeps one bad byte in a 330 MB archive from taking
        the whole refresh down.
        """
        with open(path, 'r', newline='', encoding='utf-8', errors='replace') as fh:
            return self._parse_ibtracs_catalog_rows(
                csv.DictReader(fh), min_year, max_year)

    def _parse_ibtracs_catalog_rows(
        self, reader, min_year: int, max_year: int
    ) -> dict:
        """
        Parse IBTrACS rows into a storm catalog dict (sync, runs in executor).

        Returns {SID: entry_dict} for merging into the main catalog.
        """
        catalog = {}
        storm_count = 0

        for row in reader:
            sid = row.get("SID", "").strip()
            if not sid:
                continue
            season_str = row.get("SEASON", "").strip()
            try:
                year = int(season_str)
            except (ValueError, TypeError):
                continue
            if year < min_year or year > max_year:
                continue

            name = row.get("NAME", "").strip()
            if not name or name in ("NOT_NAMED", "UNNAMED", ""):
                continue

            basin = row.get("BASIN", "").strip()
            wind_kt = _safe_float(row.get("USA_WIND")) or _safe_float(row.get("WMO_WIND")) or 0
            pres = _safe_float(row.get("USA_PRES")) or _safe_float(row.get("WMO_PRES"))
            lat_val = _safe_float(row.get("LAT"))
            lon_val = _safe_float(row.get("LON"))
            storm_speed = _safe_float(row.get("STORM_SPEED"))
            r34_ne = _safe_float(row.get("USA_R34_NE"))
            r34_se = _safe_float(row.get("USA_R34_SE"))
            r34_sw = _safe_float(row.get("USA_R34_SW"))
            r34_nw = _safe_float(row.get("USA_R34_NW"))
            r34_max_nm = max(filter(None, [r34_ne, r34_se, r34_sw, r34_nw]), default=None)
            r64_ne = _safe_float(row.get("USA_R64_NE"))
            r64_se = _safe_float(row.get("USA_R64_SE"))
            r64_sw = _safe_float(row.get("USA_R64_SW"))
            r64_nw = _safe_float(row.get("USA_R64_NW"))
            r64_max_nm = max(filter(None, [r64_ne, r64_se, r64_sw, r64_nw]), default=None)

            storm_count += 1
            if sid not in catalog:
                catalog[sid] = {
                    "id": sid,
                    "name": name.title(),
                    "year": year,
                    "basin": basin,
                    "peak_wind_kt": wind_kt,
                    "_dps_wind_kt": wind_kt,
                    "_dps_pressure": pres,
                    "_dps_r34_nm": r34_max_nm,
                    "_dps_r64_nm": r64_max_nm,
                    "_dps_fwd_kt": storm_speed,
                    "_dps_lat": lat_val,
                    "_dps_lon": lon_val,
                }
            else:
                if wind_kt > catalog[sid]["peak_wind_kt"]:
                    catalog[sid]["peak_wind_kt"] = wind_kt
                if wind_kt >= catalog[sid].get("_dps_wind_kt", 0):
                    catalog[sid]["_dps_wind_kt"] = wind_kt
                    if pres:
                        catalog[sid]["_dps_pressure"] = pres
                    if r34_max_nm:
                        catalog[sid]["_dps_r34_nm"] = r34_max_nm
                    if r64_max_nm:
                        catalog[sid]["_dps_r64_nm"] = r64_max_nm
                    if storm_speed:
                        catalog[sid]["_dps_fwd_kt"] = storm_speed
                    if lat_val is not None:
                        catalog[sid]["_dps_lat"] = lat_val
                    if lon_val is not None:
                        catalog[sid]["_dps_lon"] = lon_val

        logger.info(f"IBTrACS chunk parse: {storm_count} rows -> {len(catalog)} storms")
        return catalog

    def _parse_ibtracs_csv(
        self, csv_text: str, target_sid: str
    ) -> list[HurricaneSnapshot]:
        """Parse IBTrACS CSV for a specific storm ID."""
        snapshots = []
        reader = csv.DictReader(io.StringIO(csv_text))

        for row in reader:
            if row.get("SID", "").strip() != target_sid:
                continue

            snap = self._ibtracs_row_to_snapshot(row)
            if snap is not None:
                snapshots.append(snap)

        return snapshots

    def _search_ibtracs_csv(
        self, csv_text: str, name: str, year: int, basin: Optional[str]
    ) -> list[HurricaneSnapshot]:
        """Search IBTrACS CSV by name, year, and optional basin."""
        snapshots = []
        reader = csv.DictReader(io.StringIO(csv_text))

        for row in reader:
            row_name = row.get("NAME", "").strip().upper()
            row_season = row.get("SEASON", "").strip()
            row_basin = row.get("BASIN", "").strip()

            if row_name != name:
                continue
            if row_season and int(row_season) != year:
                continue
            if basin and row_basin != basin:
                continue

            snap = self._ibtracs_row_to_snapshot(row)
            if snap is not None:
                snapshots.append(snap)

        return snapshots

    def _search_ibtracs_name_only(
        self, csv_text: str, name: str, basin: Optional[str]
    ) -> list[HurricaneSnapshot]:
        """Search IBTrACS CSV by name only, return snapshots for the most recent year match."""
        # First pass: find all years that have this storm name
        years_found: set[int] = set()
        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            row_name = row.get("NAME", "").strip().upper()
            row_season = row.get("SEASON", "").strip()
            row_basin = row.get("BASIN", "").strip()
            if row_name != name:
                continue
            if basin and row_basin != basin:
                continue
            if row_season:
                try:
                    years_found.add(int(row_season))
                except ValueError:
                    pass

        if not years_found:
            return []

        # Pick the most recent year
        most_recent_year = max(years_found)

        # Second pass: collect snapshots for that year
        snapshots = []
        reader2 = csv.DictReader(io.StringIO(csv_text))
        for row in reader2:
            row_name = row.get("NAME", "").strip().upper()
            row_season = row.get("SEASON", "").strip()
            row_basin = row.get("BASIN", "").strip()
            if row_name != name:
                continue
            if not row_season or int(row_season) != most_recent_year:
                continue
            if basin and row_basin != basin:
                continue
            snap = self._ibtracs_row_to_snapshot(row)
            if snap is not None:
                snapshots.append(snap)

        return snapshots

    def _ibtracs_row_to_snapshot(self, row: dict) -> Optional[HurricaneSnapshot]:
        """Convert a single IBTrACS CSV row to a HurricaneSnapshot."""
        try:
            sid = row.get("SID", "").strip()
            name = row.get("NAME", "UNNAMED").strip()
            iso_time = row.get("ISO_TIME", "").strip()

            lat = float(row.get("LAT", 0))
            lon = float(row.get("LON", 0))

            # IBTrACS provides wind in knots from multiple agencies
            # Use USA agency first, then WMO
            wind_kt = _safe_float(row.get("USA_WIND")) or _safe_float(row.get("WMO_WIND")) or 0
            pres = _safe_float(row.get("USA_PRES")) or _safe_float(row.get("WMO_PRES"))

            # Wind radii (IBTrACS provides USA agency radii)
            r34_ne = _safe_float(row.get("USA_R34_NE"))
            r34_se = _safe_float(row.get("USA_R34_SE"))
            r34_sw = _safe_float(row.get("USA_R34_SW"))
            r34_nw = _safe_float(row.get("USA_R34_NW"))

            r50_ne = _safe_float(row.get("USA_R50_NE"))
            r50_se = _safe_float(row.get("USA_R50_SE"))
            r50_sw = _safe_float(row.get("USA_R50_SW"))
            r50_nw = _safe_float(row.get("USA_R50_NW"))

            r64_ne = _safe_float(row.get("USA_R64_NE"))
            r64_se = _safe_float(row.get("USA_R64_SE"))
            r64_sw = _safe_float(row.get("USA_R64_SW"))
            r64_nw = _safe_float(row.get("USA_R64_NW"))

            rmw = _safe_float(row.get("USA_RMW"))

            # Compute max r34
            r34_vals = [v for v in [r34_ne, r34_se, r34_sw, r34_nw] if v and v > 0]
            r34_max_nm = max(r34_vals) if r34_vals else None

            # Storm motion
            storm_speed = _safe_float(row.get("STORM_SPEED"))  # knots
            storm_dir = _safe_float(row.get("STORM_DIR"))  # degrees

            ts = datetime.fromisoformat(iso_time) if iso_time else utcnow()

            return HurricaneSnapshot(
                storm_id=sid,
                name=name,
                timestamp=ts,
                lat=lat,
                lon=lon,
                max_wind_ms=knots_to_ms(wind_kt),
                min_pressure_hpa=pres if pres and pres > 0 else None,
                rmw_m=nm_to_meters(rmw) if rmw and rmw > 0 else None,
                r34_m=nm_to_meters(r34_max_nm) if r34_max_nm else None,
                r34_quadrants_m=_build_quadrant_dict(r34_ne, r34_se, r34_sw, r34_nw),
                r50_quadrants_m=_build_quadrant_dict(r50_ne, r50_se, r50_sw, r50_nw),
                r64_quadrants_m=_build_quadrant_dict(r64_ne, r64_se, r64_sw, r64_nw),
                forward_speed_ms=knots_to_ms(storm_speed) if storm_speed else None,
                forward_direction_deg=storm_dir,
            )
        except (ValueError, KeyError) as e:
            logger.debug(f"Skipping IBTrACS row: {e}")
            return None


# ==================================================================
#  Helper functions
# ==================================================================

def _wind_to_ss_category(wind_kt: float) -> int:
    """Convert peak wind in knots to Saffir-Simpson category (0=TS, 1-5=Cat 1-5)."""
    if wind_kt >= 137:
        return 5
    elif wind_kt >= 113:
        return 4
    elif wind_kt >= 96:
        return 3
    elif wind_kt >= 83:
        return 2
    elif wind_kt >= 64:
        return 1
    else:
        return 0  # Tropical Storm


def _safe_float(val) -> Optional[float]:
    """Safely convert to float, returning None for missing/invalid data."""
    if val is None:
        return None
    try:
        v = float(str(val).strip())
        return v if not math.isnan(v) and v > -900 else None  # IBTrACS uses -999 for missing
    except (ValueError, TypeError):
        return None


def _extract_quadrants(parts: list[str], start_idx: int) -> Optional[dict]:
    """Extract NE/SE/SW/NW wind radii from HURDAT2 columns, converting nm → meters."""
    if len(parts) <= start_idx + 3:
        return None

    quadrants = {}
    labels = ["NE", "SE", "SW", "NW"]
    any_valid = False

    for i, label in enumerate(labels):
        try:
            val = float(parts[start_idx + i])
            if val > 0:
                quadrants[label] = nm_to_meters(val)
                any_valid = True
            else:
                quadrants[label] = None
        except (ValueError, IndexError):
            quadrants[label] = None

    return quadrants if any_valid else None


def _parse_ebtrk_quadrants(parts: list[str], start_idx: int) -> Optional[dict]:
    """
    Extract NE/SE/SW/NW wind radii from EBTRK line, converting nm → meters.
    
    Similar to _extract_quadrants but handles EBTRK's fixed-width format
    where values might be "-" for missing.
    """
    if len(parts) <= start_idx + 3:
        return None

    quadrants = {}
    labels = ["NE", "SE", "SW", "NW"]
    any_valid = False

    for i, label in enumerate(labels):
        try:
            val_str = parts[start_idx + i].strip()
            if val_str == "-" or val_str == "-999" or not val_str:
                quadrants[label] = None
            else:
                val = float(val_str)
                if val > 0:
                    quadrants[label] = nm_to_meters(val)
                    any_valid = True
                else:
                    quadrants[label] = None
        except (ValueError, IndexError):
            quadrants[label] = None

    return quadrants if any_valid else None


def _parse_quadrants(quadrant_dict: dict) -> Optional[dict]:
    """Convert a quadrant dict from NHC JSON (nm values) to meters."""
    if not quadrant_dict:
        return None

    result = {}
    any_valid = False
    for key in ["NE", "SE", "SW", "NW"]:
        val = quadrant_dict.get(key) or quadrant_dict.get(key.lower())
        if val:
            result[key] = nm_to_meters(float(val))
            any_valid = True
        else:
            result[key] = None

    return result if any_valid else None


def _build_quadrant_dict(
    ne: Optional[float],
    se: Optional[float],
    sw: Optional[float],
    nw: Optional[float],
) -> Optional[dict]:
    """Build a quadrant dict from individual values (nm), converting to meters."""
    vals = {"NE": ne, "SE": se, "SW": sw, "NW": nw}
    any_valid = any(v and v > 0 for v in vals.values())
    if not any_valid:
        return None
    return {
        k: nm_to_meters(v) if v and v > 0 else None
        for k, v in vals.items()
    }


def _latlon_to_local_meters(
    lats: np.ndarray,
    lons: np.ndarray,
    center_lat: float,
    center_lon: float,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Convert 1-D lat/lon arrays to local meter-based coordinates
    centered on (center_lat, center_lon).

    Uses simple equirectangular projection (accurate within ~5° of center).
    """
    cos_lat = math.cos(math.radians(center_lat))
    x_m = (lons - center_lon) * (math.pi / 180.0) * EARTH_RADIUS_M * cos_lat
    y_m = (lats - center_lat) * (math.pi / 180.0) * EARTH_RADIUS_M
    return x_m, y_m
