"""
SHIPS-RII rapid-intensification outlook (NHC).
================================================================

NHC publishes a SHIPS diagnostic text file per storm per synoptic cycle at
``ftp.nhc.noaa.gov/atcf/stext/{YYMMDDHH}{BB}{NN}{YY}_ships.txt`` (~9 KB).
This module reads two things out of it:

1. The **SHIPS Rapid Intensification Index** — a calibrated probability that
   peak wind jumps by N kt within M hours, with the climatological base rate
   for the same threshold. Eight thresholds are published, 20 kt/12 h through
   65 kt/72 h; 30 kt/24 h is the canonical RI definition and the one
   headlined here::

       SHIPS Prob RI for 30kt/ 24hr RI threshold=   11% is  1.3 times
       climatological mean ( 8.6%)

2. **Maximum potential intensity** from the ``POT. INT. (KT)`` row at tau-0,
   with SHIPS' own analysed ``CURRENT MAX WIND`` so the headroom between them
   is self-consistent.

WHY THIS EXISTS
---------------
StormDPS's own RI signal is retrospective: ``calculate_dps`` awards its RI
bonus only after the b-deck shows the jump already happened, which is no use
as a warning. SHIPS-RII is a long-standing NHC operational product that
supplies the forward-looking half for free — one ~9 KB text fetch per storm
per cycle.

(Motivation only, NOT provenance: Rozoff et al. 2026, *Weather and
Forecasting* WAF-D-25-0076, argues for probabilistic RI at extended lead
time via GEFS-ensemble post-processing. Nothing here implements that method,
and SHIPS-RII long predates it. The paper is why we went looking; it is not
where this number comes from.)

COVERAGE: NHC basins only (AL/EP/CP). JTWC basins (WP/IO/SH) publish no
SHIPS text product, so those storms get None and every caller must fail open.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

SHIPS_BASE_URL = "https://ftp.nhc.noaa.gov/atcf/stext"

# Basins with a SHIPS text product.
_SHIPS_BASINS = {"AL", "EP", "CP"}

# Cycles to walk back before giving up: offsets 0/-6/-12/-18 h. Covers a late
# posting plus a missed cycle without turning a miss into a fetch storm.
_MAX_CYCLES_BACK = 4

# ── Cache ────────────────────────────────────────────────────────────
# SHIPS updates every 6 h, but /storms/{id}/forecast is hit on EVERY storm
# page load for an active storm, sets no Cache-Control (so Cloudflare does
# not cache it), and the experimental page fans out across every active
# storm at once. Uncached, a storm NHC runs no SHIPS for would pay the full
# 4x404 walk on every one of those loads, forever — which is both slow and a
# good way to get an app blocked by a NOAA host.
#
# In-process is sufficient and simple here: the app runs a single gunicorn
# worker (see docker-entrypoint.sh --workers 1), and a restart just costs one
# refetch. Misses are cached LONGER than hits because a storm with no SHIPS
# product tends to stay that way for its whole life.
_CACHE: dict[str, tuple[float, Optional[dict]]] = {}
_CACHE_TTL_HIT_S = 30 * 60
_CACHE_TTL_MISS_S = 60 * 60

_RI_LINE_RE = re.compile(
    r"Prob RI for\s*(\d+)\s*kt/\s*(\d+)\s*hr RI threshold=\s*(\d+)\s*%"
    r"\s*is\s*([\d.]+)\s*times climatological mean\s*\(\s*([\d.]+)\s*%\s*\)",
    re.IGNORECASE,
)
# "POT. INT. (KT)   104    99    97   101 ..." — tau-0 is the first column and
# IS the maximum potential intensity. NOTE the separate
# "POT = MPI-VMAX (KT) : 59.9" line is the 0-24 h MEAN headroom, not tau-0:
# for EP072026 at 2026-08-02 00Z that line gives 59.9 while the real tau-0
# headroom is 104 - 40 = 64. Using it as MPI understates by several knots.
_POT_INT_RE = re.compile(r"POT\.\s*INT\.\s*\(KT\)\s+(-?\d+)", re.IGNORECASE)
# SHIPS' OWN analysed intensity. Pairing MPI with this (rather than with a
# TCM vmax from a different cycle) keeps the headroom internally consistent.
_SHIPS_VMAX_RE = re.compile(
    r"CURRENT MAX WIND\s*\(KT\)\s*:\s*(-?[\d.]+)", re.IGNORECASE)

# The canonical RI definition.
_HEADLINE_KT, _HEADLINE_HR = 30, 24


def _cycle_filename(storm_id: str, when: datetime) -> Optional[str]:
    """Build the SHIPS filename for an ATCF id at a given synoptic cycle."""
    if len(storm_id or "") < 8:
        return None
    basin, nn, yyyy = storm_id[:2].upper(), storm_id[2:4], storm_id[4:8]
    if basin not in _SHIPS_BASINS or not nn.isdigit() or not yyyy.isdigit():
        return None
    return f"{when:%y%m%d%H}" + basin + nn + yyyy[2:] + "_ships.txt"


def parse_ships_ri(text: str) -> Optional[dict]:
    """Parse the RI + potential-intensity block out of a SHIPS text product.

    Returns None when the file carries no RI index (very weak or
    post-tropical systems are published without one).
    """
    text = text or ""
    thresholds = []
    for m in _RI_LINE_RE.finditer(text):
        try:
            thresholds.append({
                "threshold_kt": int(m.group(1)),
                "hours": int(m.group(2)),
                "probability_pct": int(m.group(3)),
                "climo_ratio": float(m.group(4)),
                "climatology_pct": float(m.group(5)),
            })
        except (TypeError, ValueError):
            continue
    if not thresholds:
        return None

    headline = next(
        (t for t in thresholds
         if t["threshold_kt"] == _HEADLINE_KT and t["hours"] == _HEADLINE_HR),
        None,
    )
    out: dict = {"thresholds": thresholds}
    if headline is not None:
        out["headline"] = headline
    else:
        # Do NOT silently substitute a different criterion: a 20 kt/12 h
        # number displayed under the same key would read as the canonical
        # RI figure. Publish the fallback, flagged.
        out["headline"] = thresholds[0]
        out["headline_is_canonical"] = False

    # Maximum potential intensity + SHIPS' own vmax, both from this file so
    # the headroom between them cannot mix cycles.
    mpi = _POT_INT_RE.search(text)
    vmax = _SHIPS_VMAX_RE.search(text)
    try:
        if mpi:
            out["mpi_kt"] = int(mpi.group(1))
        if vmax:
            out["ships_vmax_kt"] = round(float(vmax.group(1)), 1)
        if "mpi_kt" in out and "ships_vmax_kt" in out:
            out["headroom_kt"] = round(out["mpi_kt"] - out["ships_vmax_kt"], 1)
    except (TypeError, ValueError):
        pass
    return out


async def get_ri_outlook(
    storm_id: str,
    http_client: Optional[httpx.AsyncClient] = None,
    now: Optional[datetime] = None,
    use_cache: bool = True,
) -> Optional[dict]:
    """Fetch + parse the newest SHIPS RI outlook for an NHC storm.

    Returns None for non-NHC basins, when no recent cycle is published, or on
    any upstream/parse failure — callers must treat this as enrichment that
    can always be absent.
    """
    basin = (storm_id or "")[:2].upper()
    if basin not in _SHIPS_BASINS:
        return None

    key = storm_id.upper()
    if use_cache:
        hit = _CACHE.get(key)
        if hit and hit[0] > time.time():
            return hit[1]

    now = now or datetime.now(timezone.utc)
    cycle = now.replace(minute=0, second=0, microsecond=0)
    cycle -= timedelta(hours=cycle.hour % 6)

    owns_client = http_client is None
    client = http_client or httpx.AsyncClient(timeout=8.0, follow_redirects=True)
    try:
        for _back in range(_MAX_CYCLES_BACK):
            attempt = cycle - timedelta(hours=6 * _back)
            fname = _cycle_filename(storm_id, attempt)
            if not fname:
                return None
            try:
                resp = await client.get(f"{SHIPS_BASE_URL}/{fname}", timeout=8.0)
            except Exception as e:
                # Broad: a closed/re-used shared client raises RuntimeError,
                # not httpx.HTTPError, and that should not end the walk.
                logger.info(f"[SHIPS] {storm_id} {fname} fetch failed: {e}")
                continue
            if resp.status_code != 200:
                continue          # cycle not published (or storm not yet named)
            parsed = parse_ships_ri(resp.text)
            if not parsed:
                logger.info(f"[SHIPS] {storm_id} {fname} has no RI index block")
                continue
            parsed["storm_id"] = key
            parsed["cycle_utc"] = attempt.strftime("%Y-%m-%dT%H:%M:00Z")
            parsed["age_hours"] = round((now - attempt).total_seconds() / 3600, 1)
            parsed["source"] = "SHIPS-RII (NHC)"
            h = parsed["headline"]
            logger.info(
                f"[SHIPS] {storm_id} RI outlook {attempt:%Y-%m-%d %HZ}: "
                f"{h['probability_pct']}% at {h['threshold_kt']}kt/{h['hours']}h "
                f"({h['climo_ratio']}x climo), MPI {parsed.get('mpi_kt')} kt")
            if use_cache:
                _CACHE[key] = (time.time() + _CACHE_TTL_HIT_S, parsed)
            return parsed

        logger.info(f"[SHIPS] {storm_id} no RI outlook in the last "
                    f"{6 * (_MAX_CYCLES_BACK - 1)} h")
        if use_cache:
            _CACHE[key] = (time.time() + _CACHE_TTL_MISS_S, None)
        return None
    finally:
        if owns_client:
            await client.aclose()
