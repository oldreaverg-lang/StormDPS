"""How often live storms are refreshed.

Advisories and the best track change every 3-6 hours, but when a storm is at
the coast the gap between "the centre published it" and "the page shows it" is
what a reader notices: on the hourly pass a new best-track fix could take over
an hour to reach the score and the map. Storms near land get a fast lane;
everything else keeps the hourly pass.

Stdlib only, so CI can import it without the web stack. The distance function
is passed in (core.land_proximity needs numpy).
"""
from __future__ import annotations

from typing import Callable, Iterable, Optional

# ~270 nm: about a day out at 12 kt on approach, and it keeps a storm in the
# lane until it is well inland after landfall.
FAST_LANE_KM = 500.0
FAST_INTERVAL_S = 600
FULL_INTERVAL_S = 3600


def fast_lane_ids(
    rows: Optional[Iterable[dict]],
    distance_km: Callable[[float, float], Optional[float]],
    within_km: float = FAST_LANE_KM,
) -> list[str]:
    """Ids (feed spelling) of the active rows whose centre is near land.

    A row without a usable position, or a distance lookup that fails, is left
    on the hourly pass: the fast lane is an optimisation, never a requirement.
    """
    out: list[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        sid = str(row.get("id") or "").strip()
        if not sid:
            continue
        try:
            lat, lon = float(row.get("lat")), float(row.get("lon"))
        except (TypeError, ValueError):
            continue
        try:
            dist = distance_km(lat, lon)
        except Exception:
            dist = None
        if dist is not None and dist <= within_km:
            out.append(sid)
    return out


def full_pass_due(now: float, last_full: float, full_interval_s: float = FULL_INTERVAL_S,
                  tick_s: float = FAST_INTERVAL_S) -> bool:
    """True when this tick should refresh every active storm, not just the
    fast lane. Half a tick of slack so timer jitter cannot push the hourly
    pass out to 70 minutes."""
    return (now - last_full) >= (full_interval_s - tick_s / 2.0)
