"""
ATCF b-deck (best-track) client — fetches historical track data for in-season
JTWC storms from UCAR RAL's public JTWC b-deck mirror.

Why this exists
---------------
JTWC's text warning bulletins contain ONLY:
  - one T+0 observation
  - forecast positions at T+12/24/36/48/72/96/120

They do NOT contain track history. For an active storm that's been tracked
for a week, the warning text alone gives you four points in the future and
zero in the past. StormDPS is a *tracking + retrospective* application —
users want to see where the storm has been, not where it's forecast to go.

The ATCF b-deck format is the synoptic-hour best-track record, carrying the
full observation history from storm birth through the most recent analysis.
UCAR RAL rebroadcasts JTWC b-decks at a predictable URL:

  https://hurricanes.ral.ucar.edu/realtime/plots/{region}/{year}/{basin}{NN}{YY}/b{basin}{NN}{YY}.dat

where region is one of {northwestpacific, northindian, southernhemisphere}
and basin is the 2-letter ATCF code in lower case (wp, io, sh).

ATCF b-deck format
------------------
One comma-separated row per (timestamp, wind threshold). Multiple rows may
share the same timestamp — one each for RAD 34/50/64 kt thresholds with
per-quadrant radii. This client merges them into a single HurricaneSnapshot.

Field layout (0-indexed, trailing whitespace on each field):
  [0]  basin          (WP)
  [1]  cyclone number (04)
  [2]  warn time      (YYYYMMDDHH)
  [3]  techname       (usually blank/BEST for b-decks)
  [4]  tau            (forecast hour, 0 for observations)
  [5]  latitude       (e.g. "087N" = 8.7°N, tenths)
  [6]  longitude      (e.g. "1521E" = 152.1°E, tenths)
  [7]  vmax           (max sustained wind, knots)
  [8]  mslp           (minimum sea-level pressure, hPa)
  [9]  type           (TD, TS, TY, ST, etc.)
  [10] rad            (wind radius threshold: 34, 50, or 64 kt)
  [11] windcode       (NEQ = per-quadrant, AAA = symmetric)
  [12] rad1           (NE quadrant radius, nm)
  [13] rad2           (SE)
  [14] rad3           (SW)
  [15] rad4           (NW)
  ...
  [18] rmw            (radius of max winds, nm)
  ...
  [25] storm name

Output matches models.hurricane.HurricaneSnapshot so downstream IKE + DPS
pipelines work unchanged.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import httpx

from models.hurricane import HurricaneSnapshot

logger = logging.getLogger(__name__)


NM_TO_METERS = 1852.0
KT_TO_MS = 0.514444


# Map ATCF basin prefix → UCAR region directory
_BASIN_REGION = {
    "WP": "northwestpacific",
    "IO": "northindian",
    "SH": "southernhemisphere",
    "SP": "southernhemisphere",
    "SI": "southernhemisphere",
}


class ATCFBDeckClientError(Exception):
    pass


class ATCFBDeckClient:
    """
    Fetches + parses ATCF b-deck (best-track) files from UCAR RAL's JTWC
    mirror. Use as an async context manager.
    """

    def __init__(
        self,
        timeout: float = 15.0,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        self.timeout = timeout
        self._http_client = http_client
        self._external_http_client = http_client is not None

    async def __aenter__(self):
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={"User-Agent": "StormDPS/1.0 (research)"},
                follow_redirects=True,
            )
        return self

    async def __aexit__(self, *args):
        if self._http_client and not self._external_http_client:
            await self._http_client.aclose()

    @property
    def http(self) -> httpx.AsyncClient:
        if self._http_client is None:
            raise RuntimeError("ATCFBDeckClient must be used as async context manager")
        return self._http_client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_storm_track(
        self, atcf_id: str
    ) -> list[HurricaneSnapshot]:
        """
        Fetch the b-deck for an 8-char ATCF ID (e.g. WP042026) and return
        historical observations as HurricaneSnapshot instances, oldest first.

        Returns an empty list if the b-deck is unavailable or empty.
        Never raises — falls back silently so the caller can try another
        source.
        """
        aid = atcf_id.strip().upper()
        if len(aid) != 8:
            return []

        prefix = aid[:2]
        region = _BASIN_REGION.get(prefix)
        if region is None:
            return []

        try:
            nn = aid[2:4]
            yyyy = aid[4:8]
        except IndexError:
            return []

        basin_l = prefix.lower()

        url = (
            f"https://hurricanes.ral.ucar.edu/realtime/plots/"
            f"{region}/{yyyy}/{basin_l}{nn}{yyyy}/b{basin_l}{nn}{yyyy}.dat"
        )

        try:
            resp = await self.http.get(url)
        except httpx.HTTPError as e:
            logger.info(f"[BDECK] Fetch failed for {aid}: {e}")
            return []

        if resp.status_code != 200:
            logger.info(f"[BDECK] {aid} → HTTP {resp.status_code} at {url}")
            return []

        text = resp.text or ""
        if not text.strip():
            logger.info(f"[BDECK] {aid} → empty body")
            return []

        snapshots = self._parse_bdeck(text, storm_id=aid)
        logger.info(
            f"[BDECK] {aid} → {len(snapshots)} observations "
            f"({snapshots[0].timestamp.isoformat() if snapshots else 'n/a'} "
            f"→ {snapshots[-1].timestamp.isoformat() if snapshots else 'n/a'})"
        )

        # b-decks often omit central pressure. Enrich from JMA once per
        # call (single advisory pressure for the storm's current state —
        # better than leaving None everywhere and zeroing surge).
        if snapshots and any(s.min_pressure_hpa is None for s in snapshots):
            await self._enrich_latest_with_jma_pressure(snapshots, aid)

        return snapshots

    # ------------------------------------------------------------------
    # Parser
    # ------------------------------------------------------------------

    def _parse_bdeck(
        self, text: str, storm_id: str
    ) -> list[HurricaneSnapshot]:
        """
        Parse ATCF b-deck text into HurricaneSnapshot records.

        Multiple rows may share a timestamp (one per 34/50/64-kt rad band).
        We merge by timestamp, keeping the highest-quality intensity from
        any row and collecting per-quadrant radii per band.
        """
        # Intermediate accumulator keyed by timestamp
        by_ts: dict[datetime, dict] = {}
        storm_name = ""

        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            fields = [f.strip() for f in line.split(",")]
            if len(fields) < 11:
                continue

            # ATCF b-deck columns:
            #  [0] basin  [1] cy  [2] YYYYMMDDHH  [3] technum  [4] tech
            #  [5] tau    [6] lat  [7] lon  [8] vmax  [9] mslp  [10] type
            #  [11] rad (34/50/64)  [12] windcode  [13-16] NE/SE/SW/NW
            #  [19] rmw  [27] storm name
            try:
                ts_str = fields[2]
                # Only observations — tau=0. b-decks usually only have tau=0 rows.
                try:
                    tau = int(fields[5])
                except ValueError:
                    tau = 0
                if tau != 0:
                    continue

                ts = datetime.strptime(ts_str, "%Y%m%d%H").replace(tzinfo=timezone.utc)

                lat = _parse_latlon(fields[6])
                lon = _parse_latlon(fields[7])
                if lat is None or lon is None:
                    continue

                vmax_kt = _safe_int(fields[8])
                mslp = _safe_int(fields[9])
                rad_band = _safe_int(fields[11]) if len(fields) > 11 else 0
            except (ValueError, IndexError):
                continue

            # Per-quadrant radii (may be blank or 0)
            rad_ne = _safe_int(fields[13]) if len(fields) > 13 else 0
            rad_se = _safe_int(fields[14]) if len(fields) > 14 else 0
            rad_sw = _safe_int(fields[15]) if len(fields) > 15 else 0
            rad_nw = _safe_int(fields[16]) if len(fields) > 16 else 0

            rmw_nm = _safe_int(fields[19]) if len(fields) > 19 else 0

            if len(fields) > 27:
                candidate_name = fields[27].strip().strip('"')
                if candidate_name and candidate_name.upper() not in ("INVEST", "NAMELESS", "FOUR"):
                    storm_name = candidate_name

            entry = by_ts.setdefault(
                ts,
                {
                    "lat": lat,
                    "lon": lon,
                    "vmax_kt": 0,
                    "mslp": None,
                    "rmw_nm": 0,
                    "r34": {"NE": 0, "SE": 0, "SW": 0, "NW": 0},
                    "r50": {"NE": 0, "SE": 0, "SW": 0, "NW": 0},
                    "r64": {"NE": 0, "SE": 0, "SW": 0, "NW": 0},
                },
            )

            # Keep the freshest position (they should all agree within a ts)
            entry["lat"] = lat
            entry["lon"] = lon
            if vmax_kt and vmax_kt > (entry["vmax_kt"] or 0):
                entry["vmax_kt"] = vmax_kt
            if mslp and (entry["mslp"] is None or mslp < entry["mslp"]):
                entry["mslp"] = mslp
            if rmw_nm and rmw_nm > (entry["rmw_nm"] or 0):
                entry["rmw_nm"] = rmw_nm

            quad_key = f"r{rad_band}" if rad_band in (34, 50, 64) else None
            if quad_key is not None:
                q = entry[quad_key]
                if rad_ne:
                    q["NE"] = max(q["NE"], rad_ne)
                if rad_se:
                    q["SE"] = max(q["SE"], rad_se)
                if rad_sw:
                    q["SW"] = max(q["SW"], rad_sw)
                if rad_nw:
                    q["NW"] = max(q["NW"], rad_nw)

        snapshots: list[HurricaneSnapshot] = []
        for ts in sorted(by_ts.keys()):
            e = by_ts[ts]
            vmax_ms = (e["vmax_kt"] or 0) * KT_TO_MS
            rmw_m = (e["rmw_nm"] or 0) * NM_TO_METERS if e["rmw_nm"] else None

            r34_quad = _quad_to_meters(e["r34"])
            r50_quad = _quad_to_meters(e["r50"])
            r64_quad = _quad_to_meters(e["r64"])

            r34_m = max(r34_quad.values()) if r34_quad else None
            r50_m = max(r50_quad.values()) if r50_quad else None
            r64_m = max(r64_quad.values()) if r64_quad else None

            snapshots.append(
                HurricaneSnapshot(
                    storm_id=storm_id,
                    name=storm_name or storm_id,
                    timestamp=ts,
                    lat=e["lat"],
                    lon=e["lon"],
                    max_wind_ms=vmax_ms,
                    min_pressure_hpa=float(e["mslp"]) if e["mslp"] else None,
                    rmw_m=rmw_m,
                    r34_m=r34_m,
                    r50_m=r50_m,
                    r64_m=r64_m,
                    r34_quadrants_m=r34_quad or None,
                    r50_quadrants_m=r50_quad or None,
                    r64_quadrants_m=r64_quad or None,
                    forward_speed_ms=None,
                    forward_direction_deg=None,
                )
            )

        # Fill in motion from successive positions
        _infer_motion(snapshots)
        return snapshots

    async def _enrich_latest_with_jma_pressure(
        self,
        snapshots: list[HurricaneSnapshot],
        atcf_id: str,
    ) -> None:
        """Apply JMA RSMC Tokyo pressure to any snapshot missing mslp.
        JMA publishes one analysis pressure per cycle; we apply it to all
        no-pressure snapshots so the surge model has a number to work with.
        """
        try:
            from services.jma_client import JMAClient
            async with JMAClient(http_client=self.http) as jma:
                data = await jma.get_storm_data(atcf_id)
        except Exception as e:
            logger.debug(f"[BDECK+JMA] pressure enrichment failed for {atcf_id}: {e}")
            return

        if data is None or data.pressure_hpa is None:
            return

        logger.info(
            f"[BDECK+JMA] {atcf_id} — pressure {data.pressure_hpa} hPa "
            f"from {data.source} (filling nulls)"
        )
        for snap in snapshots:
            if snap.min_pressure_hpa is None:
                snap.min_pressure_hpa = data.pressure_hpa


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _safe_int(s: str) -> int:
    try:
        return int(s.strip())
    except (ValueError, AttributeError):
        return 0


def _parse_latlon(s: str) -> Optional[float]:
    """ATCF encodes lat/lon in tenths of a degree with a hemisphere suffix.
    e.g. '087N' → 8.7,  '1521E' → 152.1,  '0251W' → -25.1
    """
    s = s.strip()
    if not s:
        return None
    hemi = s[-1]
    try:
        raw = int(s[:-1])
    except ValueError:
        return None
    val = raw / 10.0
    if hemi in ("S", "W"):
        val = -val
    elif hemi not in ("N", "E"):
        return None
    return val


def _quad_to_meters(quad: dict) -> dict:
    """Convert a {NE,SE,SW,NW}-in-nm dict to meters. Returns empty dict if
    all zero (no quadrant data present for this band)."""
    if not any(quad.values()):
        return {}
    return {k: (v * NM_TO_METERS) for k, v in quad.items()}


def _infer_motion(snapshots: list[HurricaneSnapshot]) -> None:
    """Fill forward_speed_ms + forward_direction_deg on each snapshot from
    the positional delta to its successor (or predecessor for the last).
    Overwrites nothing that's already set.
    """
    import math

    n = len(snapshots)
    if n < 2:
        return

    for i in range(n):
        if (
            snapshots[i].forward_speed_ms is not None
            and snapshots[i].forward_direction_deg is not None
        ):
            continue
        a = snapshots[i]
        b = snapshots[i + 1] if i + 1 < n else snapshots[i - 1]
        if b is a:
            continue
        # ordered so that (earlier → later)
        if b.timestamp < a.timestamp:
            a, b = b, a
        dt_hours = (b.timestamp - a.timestamp).total_seconds() / 3600.0
        if dt_hours <= 0:
            continue
        # Great-circle-ish: small-angle approx is fine at TC scales (~hundreds km)
        mean_lat = math.radians((a.lat + b.lat) / 2.0)
        dx_km = (b.lon - a.lon) * 111.320 * math.cos(mean_lat)
        dy_km = (b.lat - a.lat) * 110.574
        dist_km = math.hypot(dx_km, dy_km)
        speed_ms = (dist_km * 1000.0) / (dt_hours * 3600.0)
        bearing = math.degrees(math.atan2(dx_km, dy_km)) % 360.0
        snapshots[i].forward_speed_ms = speed_ms
        snapshots[i].forward_direction_deg = bearing
