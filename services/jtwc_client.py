"""
JTWC (Joint Typhoon Warning Center) data client.

JTWC is the U.S. military forecast center responsible for tropical cyclone
warnings in basins that NHC does NOT cover:
  - WP — Western North Pacific (typhoons: e.g. Sinlaku, Haiyan, Yagi)
  - IO — North Indian Ocean (Bay of Bengal + Arabian Sea)
  - SH — Southern Hemisphere (South Indian + South Pacific)

NHC's CurrentStorms.json only lists AL/EP storms, so any live-tracker that
relies on NHC alone will silently miss every WestPac typhoon. This client
fills that gap by parsing JTWC's public products:

  1. Active-storm index (RSS feed)        — enumerate current warnings
  2. Per-storm web warning text (.txt)    — lat/lon, wind, pressure, movement

Storm IDs are normalized to the same 8-character form used elsewhere in the
codebase (e.g. WP262025 for Typhoon 26W of 2025 = Sinlaku).

This client follows the same interface shape as NOAAClient.get_active_storms()
so the two can be merged transparently in routes.py.
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from models.hurricane import HurricaneSnapshot

logger = logging.getLogger(__name__)


# Nautical-mile to meter conversion (used for wind radii)
NM_TO_METERS = 1852.0
# Knots to m/s
KT_TO_MS = 0.514444


# JTWC public endpoints
JTWC_RSS_URL = "https://www.metoc.navy.mil/jtwc/rss/jtwc.rss"
# Per-storm text warnings live at:
#   https://www.metoc.navy.mil/jtwc/products/{basin}{NN}{YY}web.txt
# where basin is one of: wp, io, sh (lowercase)
JTWC_PRODUCT_BASE = "https://www.metoc.navy.mil/jtwc/products"

# Map JTWC basin prefixes (used in their product filenames / ATCF IDs)
# to the 2-letter basin codes we use internally.
JTWC_BASIN_MAP = {
    "wp": "WP",  # Western North Pacific
    "io": "IO",  # North Indian Ocean
    "sh": "SH",  # Southern Hemisphere
    # Some JTWC products split SH further, but we collapse to SH here:
    "sp": "SH",
    "si": "SH",
}

# Classification heuristics. JTWC uses different nomenclature by basin.
# We normalize to the same vocabulary NHC uses so downstream code doesn't care.
_CLASSIFICATION_BY_WIND_KT = [
    (137, "Super Typhoon"),   # WP only, but safe everywhere
    (64,  "Typhoon"),         # ≥64 kt, outside Atlantic this is "Typhoon"/"Cyclone"
    (34,  "Tropical Storm"),
    (0,   "Tropical Depression"),
]


class JTWCClientError(Exception):
    """Raised when JTWC data retrieval fails."""
    pass


class JTWCClient:
    """
    Client for retrieving active tropical cyclones from JTWC.

    Usage:
        async with JTWCClient() as client:
            storms = await client.get_active_storms()
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
                headers={"User-Agent": "HurricaneIKE-App/1.0 (research)"},
                follow_redirects=True,
            )
        return self

    async def __aexit__(self, *args):
        if self._http_client and not self._external_http_client:
            await self._http_client.aclose()

    # ------------------------------------------------------------------
    # Failure cache (same pattern as NOAAClient — avoid 30s hangs)
    # ------------------------------------------------------------------
    _jtwc_last_failure: Optional[datetime] = None
    _jtwc_failure_ttl = timedelta(minutes=5)

    @classmethod
    def jtwc_is_down(cls) -> bool:
        if cls._jtwc_last_failure is None:
            return False
        return (datetime.utcnow() - cls._jtwc_last_failure) < cls._jtwc_failure_ttl

    @classmethod
    def mark_jtwc_down(cls):
        cls._jtwc_last_failure = datetime.utcnow()
        logger.warning(
            f"[JTWC] Marked as down — skipping for "
            f"{cls._jtwc_failure_ttl.total_seconds():.0f}s"
        )

    @classmethod
    def mark_jtwc_up(cls):
        cls._jtwc_last_failure = None

    @property
    def http(self) -> httpx.AsyncClient:
        if self._http_client is None:
            raise RuntimeError("JTWCClient must be used as async context manager")
        return self._http_client

    # ==================================================================
    #  PUBLIC API
    # ==================================================================

    async def get_active_storms(self) -> list[dict]:
        """
        Return currently active tropical cyclones tracked by JTWC.

        Output dicts match the shape of NOAAClient.get_active_storms() so
        callers can merge the two without reshaping:

            {
              "id": "WP262025",            # normalized 8-char basin+num+year
              "name": "SINLAKU",
              "classification": "Typhoon",
              "lat": 22.3,                 # deg N, positive
              "lon": 128.5,                # deg E, positive
              "intensity_knots": 85,
              "pressure_mb": 955,
              "movement": "NNW at 10 kt",
              "movement_speed_knots": 10.0,
              "movement_direction_deg": 340.0,
              "basin": "WP",               # extra field — harmless to NHC consumers
              "source": "JTWC",
            }
        """
        if self.jtwc_is_down():
            logger.info("[JTWC] Skipping — recent failure cached")
            return []

        try:
            warnings = await self._fetch_active_warning_index()
        except Exception as e:
            self.mark_jtwc_down()
            raise JTWCClientError(f"JTWC index fetch failed: {e}") from e

        if not warnings:
            self.mark_jtwc_up()
            return []

        # Fetch per-storm warning text in parallel. Each warning text has
        # the authoritative position + wind + pressure + movement.
        results = await asyncio.gather(
            *(self._fetch_warning_detail(w) for w in warnings),
            return_exceptions=True,
        )

        storms: list[dict] = []
        for w, res in zip(warnings, results):
            if isinstance(res, Exception):
                logger.warning(f"[JTWC] Detail fetch failed for {w.get('id')}: {res}")
                # Still include the storm with whatever we got from the index
                if w.get("id"):
                    storms.append(self._minimal_storm_dict(w))
                continue
            if res:
                storms.append(res)

        self.mark_jtwc_up()
        return storms

    # ==================================================================
    #  INTERNAL — RSS index
    # ==================================================================

    async def _fetch_active_warning_index(self) -> list[dict]:
        """
        Parse the JTWC RSS feed to discover active warnings.

        The JTWC feed is NOT one item-per-storm. It has exactly four <item>
        blocks — three per-region aggregates (NWPac/NIO, EPac, SH) plus one
        "significant tropical weather advisories" item. Each region item's
        <description> contains a CDATA-wrapped HTML snippet that lists the
        currently active storms for that region, e.g.:

            <p><b>Typhoon  04W (Sinlaku) Warning #25 </b>…
            <a href='…/wp0426web.txt' target='newwin'>TC Warning Text</a>…

        So we:
          1. Iterate <item> blocks to get the CDATA HTML per region
          2. Scan each HTML blob for "<b>…Warning #NN</b>" storm headers,
             paired with the adjacent wpNNYYweb.txt URL

        The URL is the source of truth for (basin, number, year) because
        the HTML header uses 2-digit storm numbers that don't include the
        year, and JTWC has had cross-year name collisions before.
        """
        resp = await self.http.get(JTWC_RSS_URL)
        resp.raise_for_status()
        text = resp.text

        items = re.findall(r"<item>(.*?)</item>", text, re.DOTALL | re.IGNORECASE)

        warnings: list[dict] = []
        seen_ids: set[str] = set()

        for item in items:
            description = _extract_tag(item, "description") or ""
            if not description:
                continue

            # Skip the "no current warnings" items to cut noise in logs.
            if "no current tropical cyclone warnings" in description.lower():
                continue

            warnings.extend(
                w for w in self._parse_region_description(description)
                if w["id"] not in seen_ids and not seen_ids.add(w["id"])
            )

        return warnings

    def _parse_region_description(self, html: str) -> list[dict]:
        """
        Extract active-storm records from one region's CDATA HTML blob.

        Pairs each "<b>Typhoon 04W (Sinlaku) Warning #25</b>" header with
        the nearest wpNNYYweb.txt link that follows it.
        """
        # Find every storm header with its position in the blob so we can
        # match it to the warning-text URL that appears right after.
        header_pattern = re.compile(
            r"<b>\s*"
            r"(?P<kind>Super\s+Typhoon|Typhoon|Tropical\s+Storm|"
            r"Tropical\s+Depression|Tropical\s+Cyclone)\s+"
            r"(?P<num>\d{2})(?P<suffix>[WBAPSEC])\s*"
            r"\((?P<name>[^)]+)\)\s*"
            r"(?:Warning|Advisory)[^<]*</b>",
            re.IGNORECASE,
        )

        url_pattern = re.compile(
            r"(?P<url>https?://[^\s'\"]+?/"
            r"(?P<basin_lower>wp|io|sh|cp|ep)"
            r"(?P<num>\d{2})"
            r"(?P<yr>\d{2})"
            r"web\.txt)",
            re.IGNORECASE,
        )

        suffix_map = {
            "W": "WP",
            "B": "IO", "A": "IO",
            "P": "SH", "S": "SH",
            "E": "EP", "C": "EP",
        }

        results: list[dict] = []
        urls = list(url_pattern.finditer(html))

        for hm in header_pattern.finditer(html):
            # Find the closest URL that appears after this header in the blob.
            pos = hm.end()
            next_url = next((u for u in urls if u.start() >= pos), None)
            if next_url is None:
                continue

            num = int(next_url.group("num"))
            yr2 = int(next_url.group("yr"))
            # JTWC product filenames use 2-digit years; resolve against the
            # current century. Since JTWC only archives recent seasons at
            # these URLs, this rule is safe.
            year = 2000 + yr2
            basin_lower = next_url.group("basin_lower").lower()
            basin = JTWC_BASIN_MAP.get(basin_lower)
            if basin is None:
                # Fall back to parsing the header suffix if the URL used an
                # unexpected prefix.
                basin = suffix_map.get(hm.group("suffix").upper())
            if basin is None:
                continue

            results.append({
                "id": f"{basin}{num:02d}{year}",
                "name": hm.group("name").strip().upper(),
                "basin": basin,
                "rss_title": hm.group(0),
                "rss_link": next_url.group("url"),
                "warning_url": next_url.group("url"),
            })

        return results

    def _parse_rss_title(self, title: str) -> Optional[dict]:
        """
        Extract storm number, basin, and name from an RSS <title>.

        Examples:
          "Typhoon 26W (Sinlaku) Warning #014"            → 26W, WP, Sinlaku
          "Tropical Cyclone 03B (Fengal) Warning #006"    → 03B, IO, Fengal
          "Tropical Cyclone 12P (Alfred) Warning #001"    → 12P, SH, Alfred
        """
        # Basin suffix → internal basin code
        suffix_map = {
            "W": "WP",   # Western North Pacific
            "B": "IO", "A": "IO",      # Bay of Bengal / Arabian Sea
            "P": "SH", "S": "SH",      # South Pacific / South Indian
        }

        m = re.search(
            r"(\d{2})([WBAPS])\s*\(([^)]+)\)",
            title,
            re.IGNORECASE,
        )
        if not m:
            return None

        number = int(m.group(1))
        suffix = m.group(2).upper()
        name = m.group(3).strip().upper()
        basin = suffix_map.get(suffix)
        if not basin:
            return None

        # JTWC uses season-year = current UTC year for almost all cases.
        # WP/IO cross-year storms are rare; we default to current year and
        # fix up if the warning detail disagrees.
        year = datetime.utcnow().year

        return {
            "number": number,
            "basin": basin,
            "name": name,
            "year": year,
        }

    # ==================================================================
    #  INTERNAL — per-storm warning text
    # ==================================================================

    async def _fetch_warning_detail(self, warning: dict) -> Optional[dict]:
        """
        Fetch and parse a single JTWC warning text product.

        These are ~40-line plain-text bulletins with fixed-format lines like:
            WARNING POSITION:
            151200Z --- NEAR 22.3N 128.5E
            MOVEMENT PAST SIX HOURS - 340 DEGREES AT 10 KTS
            POSITION ACCURATE WITHIN 030 NM
            PRESENT WIND DISTRIBUTION:
            MAX SUSTAINED WINDS - 085 KT, GUSTS 105 KT
            MINIMUM CENTRAL PRESSURE - 955 MB
        """
        url = warning["warning_url"]
        try:
            resp = await self.http.get(url)
            if resp.status_code == 404:
                logger.info(f"[JTWC] Warning text 404 at {url}")
                return self._minimal_storm_dict(warning)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning(f"[JTWC] Warning text fetch failed for {warning['id']}: {e}")
            return self._minimal_storm_dict(warning)

        return self._parse_warning_text(resp.text, warning)

    def _parse_warning_text(self, text: str, warning: dict) -> dict:
        """Parse a JTWC warning bulletin into our storm dict schema."""
        lat, lon = _parse_position(text)
        wind_kt = _parse_max_wind(text)
        pressure_mb = _parse_min_pressure(text)
        mv_dir, mv_spd = _parse_movement(text)

        classification = _classify_by_wind(wind_kt) if wind_kt is not None else ""

        # Prefer a name pulled from the warning header if present, else RSS name.
        header_name = _parse_header_name(text) or warning.get("name")

        movement_str = ""
        if mv_dir is not None and mv_spd is not None:
            movement_str = f"{_deg_to_compass(mv_dir)} at {int(round(mv_spd))} kt"

        return {
            "id": warning["id"],
            "name": header_name,
            "classification": classification,
            "lat": lat,
            "lon": lon,
            "intensity_knots": wind_kt,
            "pressure_mb": pressure_mb,
            "movement": movement_str,
            "movement_speed_knots": mv_spd,
            "movement_direction_deg": mv_dir,
            "basin": warning["basin"],
            "source": "JTWC",
        }

    def _minimal_storm_dict(self, warning: dict) -> dict:
        """Fallback record when we only have RSS-level info."""
        return {
            "id": warning["id"],
            "name": warning.get("name"),
            "classification": "",
            "lat": None,
            "lon": None,
            "intensity_knots": None,
            "pressure_mb": None,
            "movement": "",
            "movement_speed_knots": None,
            "movement_direction_deg": None,
            "basin": warning["basin"],
            "source": "JTWC",
        }

    # ==================================================================
    #  TRACK SYNTHESIS
    #
    #  IBTrACS has a multi-month publication lag, so for in-season WP/IO/SH
    #  storms we need to synthesize a track from the JTWC warning bulletin
    #  itself. Each bulletin contains:
    #    - one "WARNING POSITION" at T+0 (with current wind radii + motion)
    #    - forecast positions at T+12, 24, 36, 48, 72, 96, 120 hours
    #  We emit these as HurricaneSnapshot records, matching the shape that
    #  HURDAT2 and IBTrACS produce. That's enough for the DPS pipeline to
    #  compute IKE, surge, and everything downstream.
    # ==================================================================

    async def get_storm_track(
        self,
        storm_identifier: str,
    ) -> list[HurricaneSnapshot]:
        """
        Synthesize a track (observed T+0 + forecast snapshots) for an active
        JTWC storm, keyed by ATCF ID (e.g. WP042026) or storm name (SINLAKU).

        Returns an empty list if the identifier doesn't match any active
        JTWC warning. Callers should fall back to IBTrACS in that case.
        """
        key = storm_identifier.strip().upper()

        warnings = await self._fetch_active_warning_index()
        match = next(
            (w for w in warnings if w["id"].upper() == key or w["name"].upper() == key),
            None,
        )
        if match is None:
            logger.info(
                f"[JTWC] No active warning matches '{storm_identifier}' "
                f"(active: {[w['id'] for w in warnings]})"
            )
            return []

        try:
            resp = await self.http.get(match["warning_url"])
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning(f"[JTWC] Fetch warning text failed: {e}")
            return []

        snapshots = self._parse_warning_as_track(resp.text, match)

        # JTWC's text warnings omit central pressure — fill it in from
        # JMA RSMC Tokyo (the WMO-designated RSMC for the Northwest
        # Pacific). Without this, the frontend surge calculator guards
        # on min_pressure_hpa and silently returns 0, which zeroes out
        # the Storm Surge component of the DPS hero card.
        if snapshots and any(s.min_pressure_hpa is None for s in snapshots):
            await self._enrich_with_jma_pressure(snapshots, match)

        return snapshots

    async def _enrich_with_jma_pressure(
        self, snapshots: list[HurricaneSnapshot], match: dict
    ) -> None:
        """Attach JMA-sourced central pressure to every snapshot that
        doesn't already have one. All snapshots in a single advisory
        cycle share the same pressure value (JMA issues one pressure per
        analysis cycle; forecast pressures are not published). That's
        still better than None — it lets the surge model compute.
        """
        atcf_id = match.get("id", "")
        try:
            # Lazy import keeps the JTWC module load cheap for callers
            # that don't need JMA enrichment (e.g. historical IBTrACS).
            from services.jma_client import JMAClient
            async with JMAClient(http_client=self.http) as jma:
                data = await jma.get_storm_data(atcf_id)
        except Exception as e:
            logger.debug(f"[JTWC+JMA] pressure enrichment failed for {atcf_id}: {e}")
            return

        if data is None or data.pressure_hpa is None:
            logger.debug(f"[JTWC+JMA] no JMA pressure available for {atcf_id}")
            return

        logger.info(
            f"[JTWC+JMA] {atcf_id} ({data.name}) — pressure {data.pressure_hpa} hPa "
            f"from {data.source} slot {data.source_slot}"
        )
        for snap in snapshots:
            if snap.min_pressure_hpa is None:
                snap.min_pressure_hpa = data.pressure_hpa

    def _parse_warning_as_track(
        self,
        text: str,
        warning: dict,
    ) -> list[HurricaneSnapshot]:
        """
        Walk a JTWC bulletin and emit one HurricaneSnapshot per forecast
        position. The T+0 point carries full quadrant wind radii and the
        storm's current motion vector; forecast points carry position,
        intensity, and (when present) per-quadrant R34/R50/R64.

        Bulletin structure:
            WARNING POSITION:
            151200Z --- NEAR 22.3N 128.5E
            MOVEMENT PAST SIX HOURS - 340 DEGREES AT 10 KTS
            PRESENT WIND DISTRIBUTION:
            MAX SUSTAINED WINDS - 110 KT, GUSTS 135 KT
            RADIUS OF 064 KT WINDS - 065 NM NORTHEAST QUADRANT
                                     …
            MINIMUM CENTRAL PRESSURE - 955 MB
            ---
            12 HRS, VALID AT:
            151200Z --- 16.4N 144.8E
            MAX SUSTAINED WINDS - 105 KT, GUSTS 130 KT
            RADIUS OF 064 KT WINDS - 060 NM NORTHEAST QUADRANT
                                     …
            ---
            24 HRS, VALID AT:
            ...
        """
        # Warnings are sectioned by "---" separators. The actual bulletin
        # layout is:
        #
        #    WTPN31 PGTW 150300   <-- WMO header (issuance time, NOT obs time)
        #    MSGID/...
        #    1. TYPHOON 04W (SINLAKU) WARNING NR 025
        #    ---
        #    WARNING POSITION:    <-- this is the T+0 observation
        #    150000Z --- NEAR 15.6N 145.2E
        #    MAX SUSTAINED WINDS - 110 KT...
        #    ---
        #    12 HRS, VALID AT:
        #    ...
        #
        # So the first separator sits BEFORE T+0, not after it. We locate
        # the T+0 block by searching for "WARNING POSITION" (or the raw
        # observation timestamp pattern), then treat every section after
        # that which carries "NN HRS, VALID AT:" as a forecast.
        sections = re.split(r"\n\s*---\s*\n", text)
        if not sections:
            return []

        t0_idx = None
        for i, sec in enumerate(sections):
            if re.search(r"WARNING\s+POSITION", sec, re.IGNORECASE):
                t0_idx = i
                break
            # Fallback: any section with a bare DDHHMMZ --- observation stamp
            # that is not preceded by "HRS, VALID AT" (which is forecasts)
            if re.search(r"\d{6}Z\s*-{2,}", sec) and not re.search(
                r"HRS?,?\s*VALID\s+AT", sec, re.IGNORECASE
            ):
                t0_idx = i
                break

        if t0_idx is None:
            logger.warning(f"[JTWC] Could not locate T+0 block in bulletin for {warning.get('id')}")
            return []

        t0_section = sections[t0_idx]
        # T+0 timestamp comes from the observation line (150000Z --- ...),
        # never from the WMO header, which is the issuance time (typically
        # 3 hours later) and would shift the whole track off by +3h.
        t0_time = _parse_warning_timestamp(t0_section)
        mv_dir, mv_spd_kt = _parse_movement(t0_section)

        snapshots: list[HurricaneSnapshot] = []

        # T+0 snapshot
        t0_snap = _section_to_snapshot(
            t0_section,
            warning=warning,
            timestamp=t0_time,
            forward_speed_kt=mv_spd_kt,
            forward_direction_deg=mv_dir,
        )
        if t0_snap:
            snapshots.append(t0_snap)

        # Forecast sections: find "NN HRS, VALID AT:" sections AFTER T+0
        for section in sections[t0_idx + 1:]:
            hour_match = re.search(
                r"(\d{1,3})\s*HRS?,?\s*VALID\s+AT",
                section,
                re.IGNORECASE,
            )
            if not hour_match:
                continue
            hours_offset = int(hour_match.group(1))

            # Forecast timestamp: T+0 + N hours
            fcst_time = None
            if t0_time is not None:
                fcst_time = t0_time + timedelta(hours=hours_offset)

            # Forecast "vector to next" gives us forward motion at THIS point
            # Line looks like: "VECTOR TO 24 HR POSIT: 355 DEG/ 06 KTS"
            vec_match = re.search(
                r"VECTOR\s+TO\s+\d+\s*HR\s*POSIT:\s*(\d{1,3})\s*DEG/?\s*(\d{1,3})\s*KT",
                section,
                re.IGNORECASE,
            )
            fcst_mv_dir = float(vec_match.group(1)) if vec_match else None
            fcst_mv_spd = float(vec_match.group(2)) if vec_match else None

            snap = _section_to_snapshot(
                section,
                warning=warning,
                timestamp=fcst_time,
                forward_speed_kt=fcst_mv_spd,
                forward_direction_deg=fcst_mv_dir,
            )
            if snap:
                snapshots.append(snap)

        return snapshots


# ----------------------------------------------------------------------
# Parsing helpers (module-level, pure functions — easy to unit test)
# ----------------------------------------------------------------------

def _extract_tag(blob: str, tag: str) -> Optional[str]:
    m = re.search(fr"<{tag}[^>]*>(.*?)</{tag}>", blob, re.DOTALL | re.IGNORECASE)
    if not m:
        return None
    raw = m.group(1).strip()
    # Strip CDATA wrappers if present
    cdata = re.match(r"<!\[CDATA\[(.*?)\]\]>", raw, re.DOTALL)
    return cdata.group(1).strip() if cdata else raw


def _parse_position(text: str) -> tuple[Optional[float], Optional[float]]:
    """
    Match lines like:  NEAR 22.3N 128.5E    (or integer degrees)
    Returns (lat, lon) with east/north positive.
    """
    m = re.search(
        r"(\d{1,3}(?:\.\d+)?)\s*([NS])\s+(\d{1,3}(?:\.\d+)?)\s*([EW])",
        text,
    )
    if not m:
        return (None, None)
    lat = float(m.group(1)) * (1 if m.group(2).upper() == "N" else -1)
    lon = float(m.group(3)) * (1 if m.group(4).upper() == "E" else -1)
    return (lat, lon)


def _parse_max_wind(text: str) -> Optional[float]:
    m = re.search(r"MAX\s+SUSTAINED\s+WINDS?\s*-?\s*(\d{2,3})\s*KT", text, re.IGNORECASE)
    return float(m.group(1)) if m else None


def _parse_min_pressure(text: str) -> Optional[float]:
    m = re.search(
        r"MIN(?:IMUM)?\s+CENTRAL\s+PRESSURE\s*-?\s*(\d{3,4})\s*MB",
        text,
        re.IGNORECASE,
    )
    return float(m.group(1)) if m else None


def _parse_movement(text: str) -> tuple[Optional[float], Optional[float]]:
    """Match: MOVEMENT PAST SIX HOURS - 340 DEGREES AT 10 KTS"""
    m = re.search(
        r"MOVEMENT[^-]*-?\s*(\d{1,3})\s*DEG(?:REE)?S?\s*AT\s*(\d{1,3})\s*KT",
        text,
        re.IGNORECASE,
    )
    if not m:
        return (None, None)
    return (float(m.group(1)), float(m.group(2)))


def _parse_header_name(text: str) -> Optional[str]:
    """Pull the storm name from a bulletin header line if present."""
    m = re.search(
        r"(?:TYPHOON|SUPER\s+TYPHOON|TROPICAL\s+STORM|TROPICAL\s+DEPRESSION|"
        r"TROPICAL\s+CYCLONE)\s+(\d{2}[WBAPS])\s*\(([A-Z]+)\)",
        text,
        re.IGNORECASE,
    )
    return m.group(2).upper() if m else None


def _classify_by_wind(kt: float) -> str:
    for threshold, label in _CLASSIFICATION_BY_WIND_KT:
        if kt >= threshold:
            return label
    return "Tropical Depression"


def _deg_to_compass(deg: float) -> str:
    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    idx = int((deg % 360) / 22.5 + 0.5) % 16
    return dirs[idx]


# ----------------------------------------------------------------------
# Track-synthesis helpers
# ----------------------------------------------------------------------

# Maps "NORTHEAST QUADRANT" style labels to our internal short codes
_QUADRANT_ALIASES = {
    "NORTHEAST": "NE",
    "SOUTHEAST": "SE",
    "SOUTHWEST": "SW",
    "NORTHWEST": "NW",
}


def _parse_warning_timestamp(text: str) -> Optional[datetime]:
    """
    Parse the T+0 observation time from a JTWC bulletin.

    Two formats to handle:
      1. "WTPN31 PGTW 151200" header — day 15, 12:00 UTC (month/year implicit)
      2. "151200Z --- NEAR 22.3N 128.5E" — day 15, 12:00 UTC DMY stamp

    JTWC bulletins only carry day/hour/minute; we attach the current UTC
    month and year, resolving month rollovers (bulletin day > today's day
    means previous month).
    """
    m = re.search(r"(\d{2})(\d{2})(\d{2})Z\s*-{2,}", text)
    if not m:
        # Fall back to the WTPN header
        m = re.search(r"WTPN\d+\s+\w+\s+(\d{2})(\d{2})(\d{2})", text)
    if not m:
        return None

    day = int(m.group(1))
    hour = int(m.group(2))
    minute = int(m.group(3))

    now = datetime.now(timezone.utc)
    year, month = now.year, now.month
    # If the bulletin day is in the future relative to today, it's from
    # last month. (JTWC never back-dates by more than ~6 hours.)
    if day > now.day + 2:
        if month == 1:
            month = 12
            year -= 1
        else:
            month -= 1

    try:
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_wind_radii(text: str) -> dict:
    """
    Extract per-quadrant R34/R50/R64 wind radii (in meters) from a bulletin
    section.

    Lines look like:
        RADIUS OF 064 KT WINDS - 065 NM NORTHEAST QUADRANT
                                 065 NM SOUTHEAST QUADRANT
                                 050 NM SOUTHWEST QUADRANT
                                 065 NM NORTHWEST QUADRANT

    The first quadrant appears on the same line as the header; subsequent
    quadrants are continuation lines. We walk each "RADIUS OF NNN KT WINDS"
    block and collect NM-quadrant pairs until the next block or section end.

    Returns a dict like:
        {
          "r34": {"NE": 425876, "SE": 407440, …},  # meters
          "r50": {...},
          "r64": {...},
        }
    Keys may be missing if that wind tier isn't present.
    """
    result: dict[str, dict[str, float]] = {}

    # Find every "RADIUS OF NNN KT WINDS" header with its position in the
    # text, so we can slice out each block until the next header.
    header_re = re.compile(
        r"RADIUS\s+OF\s+(\d{2,3})\s*KT\s+WINDS",
        re.IGNORECASE,
    )
    pair_re = re.compile(
        r"(\d{2,4})\s*NM\s+(NORTHEAST|SOUTHEAST|SOUTHWEST|NORTHWEST)\s+QUADRANT",
        re.IGNORECASE,
    )

    headers = list(header_re.finditer(text))
    for i, hdr in enumerate(headers):
        tier_kt = int(hdr.group(1))
        tier_key = f"r{tier_kt:02d}"  # "r34", "r50", "r64"
        start = hdr.end()
        end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        block = text[start:end]

        quads: dict[str, float] = {}
        for pm in pair_re.finditer(block):
            nm = float(pm.group(1))
            q = _QUADRANT_ALIASES[pm.group(2).upper()]
            quads[q] = nm * NM_TO_METERS

        if quads:
            result[tier_key] = quads

    return result


def _section_to_snapshot(
    section: str,
    warning: dict,
    timestamp: Optional[datetime],
    forward_speed_kt: Optional[float],
    forward_direction_deg: Optional[float],
) -> Optional[HurricaneSnapshot]:
    """
    Build a HurricaneSnapshot from one section of a JTWC warning bulletin.

    Returns None if we can't get the bare minimum (position + wind).
    """
    lat, lon = _parse_position(section)
    wind_kt = _parse_max_wind(section)

    if lat is None or lon is None or wind_kt is None:
        return None

    pressure_mb = _parse_min_pressure(section)
    radii = _parse_wind_radii(section)

    # Compute the max-over-quadrants for r34/r50/r64 (meters)
    def _max_quad(quads: Optional[dict]) -> Optional[float]:
        if not quads:
            return None
        return max(quads.values()) if quads else None

    r34_quads = radii.get("r34")
    r50_quads = radii.get("r50")
    r64_quads = radii.get("r64")

    # Fallback timestamp if parsing failed: use now() so the pipeline still
    # has an ordered track (the forecast offsets preserve relative ordering).
    ts = timestamp or datetime.now(timezone.utc)

    return HurricaneSnapshot(
        storm_id=warning["id"],
        name=warning.get("name") or "",
        timestamp=ts,
        lat=lat,
        lon=lon,
        max_wind_ms=wind_kt * KT_TO_MS,
        min_pressure_hpa=pressure_mb,
        rmw_m=None,  # JTWC bulletins don't publish RMW directly
        r34_m=_max_quad(r34_quads),
        r50_m=_max_quad(r50_quads),
        r64_m=_max_quad(r64_quads),
        r34_quadrants_m=r34_quads,
        r50_quadrants_m=r50_quads,
        r64_quadrants_m=r64_quads,
        forward_speed_ms=(forward_speed_kt * KT_TO_MS) if forward_speed_kt is not None else None,
        forward_direction_deg=forward_direction_deg,
    )
