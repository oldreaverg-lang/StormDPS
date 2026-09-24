"""Live-storm current-position consistency — the judgement behind
/health/selfcheck check 7, kept free of the app (stdlib only) so CI can test
it without importing main.

For each ACTIVE storm the probe compares the /storms/active row with the
advisory's tau=0 center. Two representations of the same advisory cycle agree
to within a few km, so a large gap once meant "one feed froze". But NHC issues
INTERMEDIATE advisories (3-hourly near land) that move the /active position
ahead of the 6-hourly forecast's tau=0 — Polo was 58 km ahead on 2026-09-24
and paged as a freeze. Rows now carry their advisory time (last_update_utc),
so the test is time-aware: page only when /active is genuinely OLDER.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Optional

POS_DIVERGE_KM = 50.0          # same-cycle gap is ~0; one missed cycle is >100 km
TAU0_STALE_H = 9.0             # one synoptic + advisory cycle
ACTIVE_LAG_H = 1.0             # /active older than the advisory by this = missed a cycle
# The active cache refreshes on traffic (2-min stale-while-revalidate) and at
# least hourly from the DPS warm loop; a snapshot this old means the refresh died.
ACTIVE_CACHE_STALE_MIN = 150.0


def to_utc(v) -> Optional[datetime]:
    """ISO string / datetime -> aware UTC datetime (naive means UTC), else None."""
    if not v:
        return None
    try:
        d = v if isinstance(v, datetime) else datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def haversine_km(la1, lo1, la2, lo2) -> float:
    p = math.pi / 180.0
    h = (math.sin((la2 - la1) * p / 2) ** 2
         + math.cos(la1 * p) * math.cos(la2 * p) * math.sin((lo2 - lo1) * p / 2) ** 2)
    return 2 * 6371.0 * math.asin(math.sqrt(h))


def compare_row(active_row: dict, tau0: dict, forecast_valid=None, now=None) -> dict:
    """One storm: distance, tau0 age, and how far /active lags the advisory
    (positive = /active OLDER; negative = /active NEWER, e.g. intermediate)."""
    now = now or datetime.now(timezone.utc)
    a_lat, a_lon = active_row.get("lat"), active_row.get("lon")
    diverge_km = (round(haversine_km(a_lat, a_lon, tau0["lat"], tau0["lon"]), 1)
                  if a_lat is not None and a_lon is not None else None)
    t0_dt = to_utc(tau0.get("valid_time_utc") or forecast_valid)
    a_dt = to_utc(active_row.get("last_update_utc"))
    return {
        "id": str(active_row.get("id") or ""),
        "diverge_km": diverge_km,
        "tau0_age_h": round((now - t0_dt).total_seconds() / 3600, 1) if t0_dt else None,
        "active_lag_h": round((t0_dt - a_dt).total_seconds() / 3600, 1) if (a_dt and t0_dt) else None,
        "active_pos": [a_lat, a_lon],
        "tau0_pos": [tau0["lat"], tau0["lon"]],
    }


def row_failures(r: dict) -> list:
    """Paging messages for one compared row."""
    out = []
    lag = r.get("active_lag_h")
    diverged = r.get("diverge_km") is not None and r["diverge_km"] > POS_DIVERGE_KM
    if lag is not None:
        if lag >= ACTIVE_LAG_H and diverged:
            out.append(f"live position for {r['id']}: /active is {lag:.0f} h older than the "
                       f"advisory and {r['diverge_km']:.0f} km off (the active feed froze)")
    elif diverged:
        # No advisory time on the row (legacy / minimal rows): distance only.
        out.append(f"live position for {r['id']}: displayed /active center is "
                   f"{r['diverge_km']:.0f} km from the fresh advisory tau=0 (a position feed froze)")
    if r.get("tau0_age_h") is not None and r["tau0_age_h"] > TAU0_STALE_H:
        out.append(f"forecast stalled for {r['id']}: advisory tau=0 is {r['tau0_age_h']:.0f} h old "
                   f"(> {TAU0_STALE_H:.0f} h)")
    return out


def cache_age_failure(cache_time, active_count: int, now=None):
    """(age_min, failure message or None). cache_time is the naive-UTC
    timeutil.utcnow() stamp — to_utc() normalises it (the old inline
    aware-minus-naive subtraction raised, so the age was always None)."""
    now = now or datetime.now(timezone.utc)
    ct = to_utc(cache_time)
    if ct is None:
        return None, None
    age = round((now - ct).total_seconds() / 60, 1)
    if active_count and age > ACTIVE_CACHE_STALE_MIN:
        return age, (f"active-storm feed not refreshed for {age:.0f} min "
                     f"(> {ACTIVE_CACHE_STALE_MIN:.0f}) — positions are frozen")
    return age, None
