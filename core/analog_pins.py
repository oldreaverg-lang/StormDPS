"""Analogs chosen by hand, shown first in "Storms like this one".

The analog finder (api/routes.get_storm_analogs) ranks the storms in the
compiled bundle and refuses weak matches: same basin, within 20 DPS points,
and a minimum similarity. Some comparisons a reader would reach for first fall
outside all of that. Opal 1995 is not a bundle storm, and at DPS 75 it sits 23
points from Isaias 2026, yet Isaias came ashore about 60 km east of where Opal
did, also in early October.

A pin is an editorial choice, so it carries its reason, and the page shows the
reason where a computed analog shows a similarity figure.

Stdlib only.
"""
from __future__ import annotations

from typing import Dict, List, Tuple

# STORM ID (upper case) -> pins, in display order.
PINNED_ANALOGS: Dict[str, Tuple[dict, ...]] = {
    "AL092026": (       # Isaias 2026, landfall near Fort Walton Beach, 10 Oct
        {"id": "1995271N19273",     # Opal 1995, landfall at Pensacola Beach, 4 Oct
         "why": "Same stretch of the Florida Panhandle, also in early October"},
    ),
}


def pins_for(storm_id: str) -> List[dict]:
    """The pins for a storm ([] for nearly every storm)."""
    return [dict(p) for p in PINNED_ANALOGS.get(str(storm_id or "").strip().upper(), ())]
