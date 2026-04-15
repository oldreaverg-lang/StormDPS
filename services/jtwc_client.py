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
from datetime import datetime, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


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
