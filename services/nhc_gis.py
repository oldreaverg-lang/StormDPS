"""
NHC GIS MapServer client — authoritative forecast track, cone, wind radii.

The National Hurricane Center publishes every active advisory as a live
ArcGIS MapServer feed at:

    https://mapservices.weather.noaa.gov/tropical/rest/services/tropical/NHC_tropical_weather_summary/MapServer

and on a per-storm basis via the "ActiveAL" feeds:

    https://www.nhc.noaa.gov/gis/forecast/archive/

For StormDPS we only care about the *active* storms and use the MapServer
REST endpoints that return GeoJSON for:

  * Forecast points (layer "Forecast Positions")
  * Forecast track line (layer "Forecast Track")
  * Error cone polygon (layer "Forecast Error Cone")
  * Current wind radii (layer "Watches and Warnings" / "Wind Field")
  * Probability of tropical-storm-force winds

This client pulls those layers as JSON and converts them to a compact
dict the frontend can render directly on the Active Storm map without
having to know anything about Esri JSON.

Authoritative docs:
  https://www.nhc.noaa.gov/gis/
  https://mapservices.weather.noaa.gov/tropical/rest/services/tropical/
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

NHC_TROPICAL_SERVICE = (
    "https://mapservices.weather.noaa.gov/tropical/rest/services/"
    "tropical/NHC_tropical_weather_summary/MapServer"
)


@dataclass
class ActiveStormAdvisory:
    storm_id: str            # e.g. "AL092024"
    name: str                # "Helene"
    advisory_number: str     # "12A"
    intensity_kt: Optional[int]
    min_pressure_mb: Optional[int]
    position: Optional[tuple[float, float]]  # (lat, lon)
    motion: Optional[str]                    # e.g. "NNW at 14 kt"
    category: Optional[str]                  # "Hurricane", "Tropical Storm"
    track: list[tuple[float, float]] = field(default_factory=list)  # forecast points
    cone: Optional[dict] = None              # GeoJSON polygon
    wind_radii: Optional[dict] = None
    advisory_time_utc: Optional[str] = None


class NHCGISClient:
    def __init__(self, timeout: float = 20.0):
        self._timeout = timeout

    async def _query_layer(
        self,
        client: httpx.AsyncClient,
        layer_id: int,
        where: str = "1=1",
        out_fields: str = "*",
    ) -> Optional[dict]:
        url = f"{NHC_TROPICAL_SERVICE}/{layer_id}/query"
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "true",
            "f": "geojson",
        }
        try:
            r = await client.get(url, params=params)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            logger.debug(f"[NHC] layer {layer_id} query failed: {e}")
            return None

    async def get_active_storms(self) -> list[ActiveStormAdvisory]:
        """
        Pull all currently active tropical cyclones from the NHC tropical
        MapServer and return them as structured advisories.

        Returns an empty list when no storms are active.
        """
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            # Layer 0 is the current-position point layer in the NHC tropical
            # weather summary MapServer (layer IDs are stable per service,
            # but we probe if not found).
            service_info = await self._get_service_info(client)
            if not service_info:
                return []

            layers = {L.get("name", "").lower(): L.get("id") for L in service_info.get("layers", [])}
            position_layer = _find_layer(layers, ["current position", "active points", "position"])
            track_layer = _find_layer(layers, ["forecast track", "track line"])
            cone_layer = _find_layer(layers, ["forecast error cone", "cone", "track cone"])
            radii_layer = _find_layer(layers, ["wind field", "watches and warnings"])

            if position_layer is None:
                logger.info("[NHC] no active-storm position layer found (quiet period?)")
                return []

            positions = await self._query_layer(client, position_layer)
            if not positions or not positions.get("features"):
                return []

            tracks = await self._query_layer(client, track_layer) if track_layer is not None else None
            cones = await self._query_layer(client, cone_layer) if cone_layer is not None else None
            radii = await self._query_layer(client, radii_layer) if radii_layer is not None else None

            storms: list[ActiveStormAdvisory] = []
            for feat in positions.get("features", []):
                props = feat.get("properties", {}) or {}
                geom = feat.get("geometry") or {}
                coords = geom.get("coordinates") or []
                position = None
                if len(coords) >= 2:
                    position = (float(coords[1]), float(coords[0]))  # GeoJSON is [lon, lat]

                sid = (
                    props.get("STORMID")
                    or props.get("STORM_ID")
                    or props.get("stormid")
                    or ""
                ).upper()
                name = (
                    props.get("STORMNAME")
                    or props.get("STORM_NAME")
                    or props.get("name")
                    or ""
                ).title()

                storms.append(
                    ActiveStormAdvisory(
                        storm_id=sid,
                        name=name,
                        advisory_number=str(props.get("ADVISNUM") or props.get("advnum") or ""),
                        intensity_kt=_safe_int(props.get("INTENSITY") or props.get("MAXWIND")),
                        min_pressure_mb=_safe_int(props.get("MSLP") or props.get("MINPRESS")),
                        position=position,
                        motion=_format_motion(props),
                        category=props.get("TCDVLP") or props.get("devel") or None,
                        track=_extract_track_points(tracks, sid),
                        cone=_extract_cone(cones, sid),
                        wind_radii=_extract_radii(radii, sid),
                        advisory_time_utc=props.get("ADVDATE") or props.get("DTG") or None,
                    )
                )
        return storms

    async def _get_service_info(self, client: httpx.AsyncClient) -> Optional[dict]:
        try:
            r = await client.get(NHC_TROPICAL_SERVICE, params={"f": "json"})
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            logger.warning(f"[NHC] service info failed: {e}")
            return None


def _find_layer(layers: dict[str, Any], needles: list[str]) -> Optional[int]:
    for name, lid in layers.items():
        for n in needles:
            if n in name:
                return lid
    return None


def _safe_int(v) -> Optional[int]:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _format_motion(props: dict) -> Optional[str]:
    speed = props.get("STORMSPEED") or props.get("FSPEED") or props.get("speed")
    direction = props.get("STORMDRCT") or props.get("FDIR") or props.get("heading")
    if speed and direction:
        return f"{direction} at {speed} kt"
    return None


def _extract_track_points(tracks: Optional[dict], sid: str) -> list[tuple[float, float]]:
    if not tracks or not sid:
        return []
    out: list[tuple[float, float]] = []
    for feat in tracks.get("features", []):
        props = feat.get("properties", {}) or {}
        fid = (props.get("STORMID") or props.get("stormid") or "").upper()
        if fid and fid != sid:
            continue
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or []
        if geom.get("type") == "LineString":
            for c in coords:
                if len(c) >= 2:
                    out.append((float(c[1]), float(c[0])))
        elif geom.get("type") == "Point" and len(coords) >= 2:
            out.append((float(coords[1]), float(coords[0])))
    return out


def _extract_cone(cones: Optional[dict], sid: str) -> Optional[dict]:
    if not cones or not sid:
        return None
    for feat in cones.get("features", []):
        props = feat.get("properties", {}) or {}
        fid = (props.get("STORMID") or props.get("stormid") or "").upper()
        if fid == sid:
            return feat.get("geometry")
    return None


def _extract_radii(radii: Optional[dict], sid: str) -> Optional[dict]:
    if not radii or not sid:
        return None
    matched = []
    for feat in radii.get("features", []):
        props = feat.get("properties", {}) or {}
        fid = (props.get("STORMID") or props.get("stormid") or "").upper()
        if fid == sid:
            matched.append(feat)
    if not matched:
        return None
    return {"type": "FeatureCollection", "features": matched}
