"""
NWS Alerts client — api.weather.gov active alerts and warnings.

The National Weather Service publishes all active watches, warnings, and
advisories as a public GeoJSON feed at https://api.weather.gov/alerts.
This is the authoritative source for life-safety messaging during an
active tropical event: Hurricane Warnings, Storm Surge Warnings, Tropical
Storm Warnings, Flash Flood Emergencies, Tornado Warnings, etc.

For StormDPS we filter to tropical-relevant events and surface them on
the Active Storm view:

    Hurricane Warning
    Hurricane Watch
    Tropical Storm Warning
    Tropical Storm Watch
    Storm Surge Warning
    Storm Surge Watch
    Flash Flood Warning
    Flash Flood Emergency
    Tornado Warning (spawned tornadoes)
    Extreme Wind Warning

Rules of the road (per the api.weather.gov docs):
  * No API key, but a User-Agent with contact info is required.
  * GZIP is accepted; rate limits are generous for a normal UI but heavy
    polling should use the /alerts/active feed with an ETag.
  * Alerts are GeoJSON features — each has an id, properties, and geometry.
  * Alerts have lifecycle: Actual/Exercise/Test — we filter to Actual.

Reference: https://www.weather.gov/documentation/services-web-api
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

NWS_ALERTS_ACTIVE = "https://api.weather.gov/alerts/active"

# Tropical-relevant NWS alert event types. Matches the `event` property
# exactly (NWS uses consistent title case for this field).
TROPICAL_EVENTS = {
    "Hurricane Warning",
    "Hurricane Watch",
    "Hurricane Force Wind Warning",
    "Tropical Storm Warning",
    "Tropical Storm Watch",
    "Storm Surge Warning",
    "Storm Surge Watch",
    "Extreme Wind Warning",
    "Flash Flood Warning",
    "Flash Flood Statement",
    "Tornado Warning",
    "Tornado Watch",
    "High Wind Warning",
    "Coastal Flood Warning",
    "Coastal Flood Advisory",
}

# Severity ranking for UI sorting.
_SEVERITY_RANK = {
    "Extreme": 4,
    "Severe": 3,
    "Moderate": 2,
    "Minor": 1,
    "Unknown": 0,
}


@dataclass
class NWSAlert:
    id: str
    event: str                 # e.g. "Hurricane Warning"
    headline: Optional[str]
    description: Optional[str]
    severity: str              # Extreme / Severe / Moderate / Minor / Unknown
    certainty: str             # Observed / Likely / Possible / Unlikely
    urgency: str               # Immediate / Expected / Future / Past
    sent: Optional[str]        # ISO timestamp
    effective: Optional[str]
    expires: Optional[str]
    sender_name: Optional[str]
    areas: list[str] = field(default_factory=list)
    states: list[str] = field(default_factory=list)
    geometry: Optional[dict] = None
    instruction: Optional[str] = None

    @property
    def severity_rank(self) -> int:
        return _SEVERITY_RANK.get(self.severity, 0)


class NWSAlertsClient:
    """Fetches and filters active NWS alerts for tropical events."""

    def __init__(
        self,
        user_agent: str = "StormDPS/1.0 (contact: admin@stormdps.example)",
        timeout: float = 20.0,
    ):
        self._ua = user_agent
        self._timeout = timeout
        self._etag: Optional[str] = None

    async def fetch_active(
        self,
        *,
        event_filter: Optional[set[str]] = None,
        area: Optional[str] = None,
        point: Optional[tuple[float, float]] = None,
    ) -> list[NWSAlert]:
        """
        Fetch the current active alerts feed and return parsed NWSAlert objects.

        Args:
            event_filter: If provided, only alerts whose ``event`` is in this set
                          are returned. Defaults to ``TROPICAL_EVENTS``.
            area:         Optional state/territory 2-letter code (e.g. "FL").
            point:        Optional (lat, lon) to filter to alerts covering a
                          specific location.
        """
        events = event_filter or TROPICAL_EVENTS
        params: dict[str, str] = {"status": "actual", "message_type": "alert"}
        if area:
            params["area"] = area
        if point:
            params["point"] = f"{point[0]},{point[1]}"

        headers = {
            "User-Agent": self._ua,
            "Accept": "application/geo+json",
        }
        if self._etag:
            headers["If-None-Match"] = self._etag

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                r = await client.get(NWS_ALERTS_ACTIVE, params=params, headers=headers)
                if r.status_code == 304:
                    return []  # no changes since last poll
                r.raise_for_status()
                self._etag = r.headers.get("ETag")
                data = r.json()
        except httpx.HTTPError as e:
            logger.warning(f"[NWS] fetch_active failed: {e}")
            return []

        alerts: list[NWSAlert] = []
        for feat in data.get("features", []) or []:
            props = feat.get("properties", {}) or {}
            event = props.get("event") or ""
            if event not in events:
                continue
            alerts.append(
                NWSAlert(
                    id=feat.get("id") or props.get("id") or "",
                    event=event,
                    headline=props.get("headline"),
                    description=props.get("description"),
                    severity=props.get("severity") or "Unknown",
                    certainty=props.get("certainty") or "Unknown",
                    urgency=props.get("urgency") or "Unknown",
                    sent=props.get("sent"),
                    effective=props.get("effective"),
                    expires=props.get("expires"),
                    sender_name=props.get("senderName"),
                    areas=[a.strip() for a in (props.get("areaDesc") or "").split(";") if a.strip()],
                    states=_extract_states(props),
                    geometry=feat.get("geometry"),
                    instruction=props.get("instruction"),
                )
            )

        alerts.sort(key=lambda a: (-a.severity_rank, a.expires or ""))
        return alerts

    async def alerts_for_storm(
        self,
        track_points: list[dict],
        *,
        radius_deg: float = 2.0,
    ) -> list[NWSAlert]:
        """
        Return active tropical alerts intersecting a storm's track envelope.

        Strategy: pull the full active tropical feed, then filter features
        whose centroid (approx) falls within ``radius_deg`` of any track
        point. This is coarse but avoids per-point round trips.
        """
        if not track_points:
            return []
        alerts = await self.fetch_active()
        if not alerts:
            return []

        # Build a quick bounding box around the track.
        lats = [p.get("lat") for p in track_points if p.get("lat") is not None]
        lons = [p.get("lon") for p in track_points if p.get("lon") is not None]
        if not lats or not lons:
            return alerts  # no geo — return all tropical alerts
        lat_min = min(lats) - radius_deg
        lat_max = max(lats) + radius_deg
        lon_min = min(lons) - radius_deg
        lon_max = max(lons) + radius_deg

        def _in_bbox(geom: Optional[dict]) -> bool:
            if not geom:
                return True  # zone-only alerts fall through
            coords = geom.get("coordinates")
            if not coords:
                return True
            # Depth-first scan of nested coordinate arrays
            stack = [coords]
            while stack:
                c = stack.pop()
                if isinstance(c, (list, tuple)):
                    if len(c) == 2 and all(isinstance(v, (int, float)) for v in c):
                        lon, lat = c
                        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                            return True
                    else:
                        stack.extend(c)
            return False

        return [a for a in alerts if _in_bbox(a.geometry)]


def _extract_states(props: dict) -> list[str]:
    """Extract unique 2-letter state codes from the NWS SAME/UGC geocodes."""
    codes: set[str] = set()
    geo = props.get("geocode") or {}
    for ugc in geo.get("UGC", []) or []:
        if isinstance(ugc, str) and len(ugc) >= 2:
            codes.add(ugc[:2])
    for same in geo.get("SAME", []) or []:
        # SAME codes don't carry state letters directly — skip.
        _ = same
    return sorted(codes)
