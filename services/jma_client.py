"""
JMA RSMC Tokyo client — sources Western Pacific tropical cyclone central
pressure and 10-minute sustained wind from Japan Meteorological Agency.

JTWC's text warning bulletins intentionally omit central pressure, which
breaks any downstream calculation keyed on pressure (notably the
parametric surge estimator). JMA is the WMO-designated Regional
Specialized Meteorological Center for the Northwest Pacific and their
bulletins always include pressure, so this client fills that gap.

Data source
-----------
NOAA's NWS Telecommunications Gateway rebroadcasts RSMC Tokyo's WMO
bulletins as plain text under:

    https://tgftp.nws.noaa.gov/data/raw/wt/wtpq<NN>.rjtd..txt

Bulletin code families:
    WTPQ20-29  — TC Advisory (structured, per active storm)
    WTPQ30-39  — TC Prognostic Reasoning (narrative, per active storm)

A given slot (say WTPQ20) is assigned to one storm at a time by JMA
operations; slots may carry stale bulletins from an old storm between
seasons. This client scans the whole family and matches on the
storm's JMA ID (YYNN, e.g. 2604 for the 4th storm of 2026).

Parsing notes
-------------
TC Advisory body (preferred, structured):
    NAME  STS 2217 KULAP (2217)
    ANALYSIS
    PSTN  280300UTC 32.6N 146.1E GOOD
    PRES  980HPA
    MXWD  055KT
    GUST  080KT

TC Prognostic Reasoning (fallback, narrative):
    REASONING NO.27 FOR TY 2604 SINLAKU (2604)
    CENTRAL PRESSURE IS 925HPA AND MAXIMUM SUSTAINED WINDS...

Both carry PRES/CENTRAL PRESSURE in hPa. Winds are 10-minute sustained
(JMA convention) — ~88% of JTWC's 1-minute number, callers that merge
with JTWC should prefer JTWC's wind and take only JMA's pressure.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# NOAA rebroadcast of JMA RSMC Tokyo bulletins
_NOAA_WT_BASE = "https://tgftp.nws.noaa.gov/data/raw/wt"

# Advisory (structured) and prognostic reasoning (narrative) slots
_ADVISORY_SLOTS = [f"wtpq{n}" for n in range(20, 30)]
_REASONING_SLOTS = [f"wtpq{n}" for n in range(30, 40)]

# Regex to pull JMA ID + name from either bulletin type. JMA IDs are
# 4-digit YYNN — e.g. "2604" = 4th storm of 2026. Bulletins use patterns
# like:
#   NAME  STS 2217 KULAP (2217)
#   REASONING NO.27 FOR TY 2604 SINLAKU (2604)
_NAME_RE = re.compile(
    r"\b(?P<prefix>TD|TS|STS|TY|HR|H|TC)\s+(?P<jma_id>\d{4})\s+"
    r"(?P<name>[A-Z][A-Z\- ]+?)\s*\(\d{4}\)",
    re.IGNORECASE,
)

# Advisory structured fields
_ADVISORY_PRES_RE = re.compile(r"^\s*PRES\s+(\d{3,4})\s*HPA", re.MULTILINE)
_ADVISORY_WIND_RE = re.compile(r"^\s*MXWD\s+(\d{2,3})\s*KT", re.MULTILINE)
_ADVISORY_GUST_RE = re.compile(r"^\s*GUST\s+(\d{2,3})\s*KT", re.MULTILINE)
_ADVISORY_PSTN_RE = re.compile(
    r"PSTN\s+(?P<dd>\d{2})(?P<hh>\d{2})(?P<mm>\d{2})UTC\s+"
    r"(?P<lat>\d+\.?\d*)(?P<ns>[NS])\s+(?P<lon>\d+\.?\d*)(?P<ew>[EW])",
    re.IGNORECASE,
)

# Prognostic reasoning narrative fields
_REASONING_PRES_RE = re.compile(
    r"CENTRAL\s+PRESSURE\s+IS\s+(\d{3,4})\s*HPA", re.IGNORECASE
)
_REASONING_WIND_RE = re.compile(
    r"MAXIMUM\s+SUSTAINED\s+WINDS\s+(?:NEAR\s+THE\s+CENTER\s+)?ARE\s+(\d{2,3})\s*KNOTS",
    re.IGNORECASE,
)
_REASONING_PSTN_RE = re.compile(
    r"LOCATED\s+AT\s+(?P<lat>\d+\.?\d*)(?P<ns>[NS])\s*,?\s*"
    r"(?P<lon>\d+\.?\d*)(?P<ew>[EW])",
    re.IGNORECASE,
)

# WMO header carries the issuance timestamp as DDHHMM (day/hour/min UTC).
_HEADER_RE = re.compile(r"^WTPQ\d{2}\s+RJTD\s+(\d{2})(\d{2})(\d{2})", re.MULTILINE)


@dataclass
class JMAStormData:
    """A single JMA snapshot of a Western Pacific tropical cyclone."""

    jma_id: str                 # YYNN — e.g. "2604"
    name: str                   # e.g. "SINLAKU"
    classification: str         # "TY", "STS", "TS", "TD"
    pressure_hpa: Optional[float]
    wind_kt_10min: Optional[float]
    gust_kt: Optional[float]
    lat: Optional[float]
    lon: Optional[float]
    issued_at: Optional[datetime]
    source_slot: str            # e.g. "wtpq20" / "wtpq30"
    source: str                 # "advisory" | "reasoning"


class JMAClient:
    """Fetches JMA RSMC Tokyo typhoon bulletins to supply central pressure
    for Western Pacific storms whose JTWC warnings omit that field."""

    def __init__(self, http_client: Optional[httpx.AsyncClient] = None):
        self._own_http = http_client is None
        self.http = http_client or httpx.AsyncClient(
            timeout=15.0,
            headers={"User-Agent": "StormDPS/1.0 (research)", "Accept": "text/plain"},
            follow_redirects=True,
        )
        # Small in-memory cache so a batch of snapshots from one storm
        # doesn't hammer NOAA. Keyed by JMA ID, 5-minute TTL.
        self._cache: dict[str, tuple[datetime, JMAStormData]] = {}
        self._cache_ttl_seconds = 300

    async def __aenter__(self) -> "JMAClient":
        return self

    async def __aexit__(self, *exc) -> None:
        if self._own_http:
            await self.http.aclose()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    def atcf_to_jma_id(atcf_id: str) -> Optional[str]:
        """Translate an ATCF-format WP ID to a JMA 4-digit ID.

        WP042026 -> "2604"   (year 26, storm 04)
        WP112024 -> "2411"
        Returns None for non-WP IDs or bad formats.
        """
        s = (atcf_id or "").strip().upper()
        if len(s) != 8 or not s.startswith("WP"):
            return None
        try:
            num = int(s[2:4])
            year = int(s[4:8])
        except ValueError:
            return None
        return f"{year % 100:02d}{num:02d}"

    async def get_storm_data(self, jma_or_atcf_id: str) -> Optional[JMAStormData]:
        """Fetch current JMA advisory for a storm.

        Accepts either a JMA ID (4-digit "2604") or an ATCF ID ("WP042026").
        Returns None if the storm is not currently in any active slot.
        """
        jma_id = self._normalize_id(jma_or_atcf_id)
        if jma_id is None:
            return None

        cached = self._cache.get(jma_id)
        if cached:
            ts, data = cached
            if (datetime.now(timezone.utc) - ts).total_seconds() < self._cache_ttl_seconds:
                return data

        # Prefer the structured advisory; fall back to prognostic reasoning.
        data = await self._scan_slots(_ADVISORY_SLOTS, jma_id, parser="advisory")
        if data is None or data.pressure_hpa is None:
            reasoning = await self._scan_slots(
                _REASONING_SLOTS, jma_id, parser="reasoning"
            )
            if reasoning is not None:
                if data is None:
                    data = reasoning
                else:
                    # Use advisory fields when present, fill gaps from reasoning
                    data = _merge(data, reasoning)

        if data is not None:
            self._cache[jma_id] = (datetime.now(timezone.utc), data)

        return data

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _normalize_id(self, ident: str) -> Optional[str]:
        s = (ident or "").strip().upper()
        if re.fullmatch(r"\d{4}", s):
            return s
        return self.atcf_to_jma_id(s)

    async def _scan_slots(
        self, slots: list[str], jma_id: str, parser: str
    ) -> Optional[JMAStormData]:
        """Fetch each slot in parallel, return the first bulletin matching jma_id."""
        tasks = [self._fetch_slot(slot) for slot in slots]
        texts = await asyncio.gather(*tasks, return_exceptions=True)
        for slot, text in zip(slots, texts):
            if isinstance(text, Exception) or not text:
                continue
            m = _NAME_RE.search(text)
            if not m or m.group("jma_id") != jma_id:
                continue
            if parser == "advisory":
                return _parse_advisory(text, slot)
            else:
                return _parse_reasoning(text, slot)
        return None

    async def _fetch_slot(self, slot: str) -> Optional[str]:
        url = f"{_NOAA_WT_BASE}/{slot}.rjtd..txt"
        try:
            resp = await self.http.get(url)
            if resp.status_code == 200:
                return resp.text
        except Exception as e:
            logger.debug(f"[JMA] slot {slot} fetch failed: {e}")
        return None


# ----------------------------------------------------------------------
# Parsers
# ----------------------------------------------------------------------

def _parse_advisory(text: str, slot: str) -> JMAStormData:
    name_m = _NAME_RE.search(text)
    pres_m = _ADVISORY_PRES_RE.search(text)
    wind_m = _ADVISORY_WIND_RE.search(text)
    gust_m = _ADVISORY_GUST_RE.search(text)
    pstn_m = _ADVISORY_PSTN_RE.search(text)
    issued = _parse_header_timestamp(text)

    lat = lon = None
    if pstn_m:
        lat = float(pstn_m.group("lat"))
        if pstn_m.group("ns").upper() == "S":
            lat = -lat
        lon = float(pstn_m.group("lon"))
        if pstn_m.group("ew").upper() == "W":
            lon = -lon

    return JMAStormData(
        jma_id=name_m.group("jma_id") if name_m else "",
        name=(name_m.group("name").strip() if name_m else "").upper(),
        classification=(name_m.group("prefix") if name_m else "").upper(),
        pressure_hpa=float(pres_m.group(1)) if pres_m else None,
        wind_kt_10min=float(wind_m.group(1)) if wind_m else None,
        gust_kt=float(gust_m.group(1)) if gust_m else None,
        lat=lat,
        lon=lon,
        issued_at=issued,
        source_slot=slot,
        source="advisory",
    )


def _parse_reasoning(text: str, slot: str) -> JMAStormData:
    name_m = _NAME_RE.search(text)
    pres_m = _REASONING_PRES_RE.search(text)
    wind_m = _REASONING_WIND_RE.search(text)
    pstn_m = _REASONING_PSTN_RE.search(text)
    issued = _parse_header_timestamp(text)

    lat = lon = None
    if pstn_m:
        lat = float(pstn_m.group("lat"))
        if pstn_m.group("ns").upper() == "S":
            lat = -lat
        lon = float(pstn_m.group("lon"))
        if pstn_m.group("ew").upper() == "W":
            lon = -lon

    return JMAStormData(
        jma_id=name_m.group("jma_id") if name_m else "",
        name=(name_m.group("name").strip() if name_m else "").upper(),
        classification=(name_m.group("prefix") if name_m else "").upper(),
        pressure_hpa=float(pres_m.group(1)) if pres_m else None,
        wind_kt_10min=float(wind_m.group(1)) if wind_m else None,
        gust_kt=None,
        lat=lat,
        lon=lon,
        issued_at=issued,
        source_slot=slot,
        source="reasoning",
    )


def _parse_header_timestamp(text: str) -> Optional[datetime]:
    """WMO header carries DDHHMM UTC issuance time. We stitch today's
    year/month onto it, rolling the month back if DD is in the future."""
    m = _HEADER_RE.search(text)
    if not m:
        return None
    day, hour, minute = int(m.group(1)), int(m.group(2)), int(m.group(3))
    now = datetime.now(timezone.utc)
    year, month = now.year, now.month
    if day > now.day + 1:
        # Header day is in the "future" — likely previous month
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
    try:
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except ValueError:
        return None


def _merge(primary: JMAStormData, fallback: JMAStormData) -> JMAStormData:
    """Fill Nones in primary from fallback."""
    def pick(a, b):
        return a if a is not None else b
    return JMAStormData(
        jma_id=primary.jma_id or fallback.jma_id,
        name=primary.name or fallback.name,
        classification=primary.classification or fallback.classification,
        pressure_hpa=pick(primary.pressure_hpa, fallback.pressure_hpa),
        wind_kt_10min=pick(primary.wind_kt_10min, fallback.wind_kt_10min),
        gust_kt=pick(primary.gust_kt, fallback.gust_kt),
        lat=pick(primary.lat, fallback.lat),
        lon=pick(primary.lon, fallback.lon),
        issued_at=pick(primary.issued_at, fallback.issued_at),
        source_slot=f"{primary.source_slot}+{fallback.source_slot}",
        source=f"{primary.source}+{fallback.source}",
    )


__all__ = ["JMAClient", "JMAStormData"]
