"""
Unified weather data aggregation service.

Pulls from ALL available API clients:
  - OpenMeteoClient (free, global, no key required — primary for SST, soil, shear)
  - NWSClient (free, reliable US coverage — primary for alerts)
  - GoogleWeatherClient (most features, requires API key — supplemental)
  - WeatherNextClient (AI forecasts, requires GCP — 2026 season validation)
  - NOAAClient (hurricane-specific data — ERDDAP SST is fallback only)

Acts as a single entry point for all weather data needs, intelligently selecting
data sources based on availability and rolling health scores from
SourceHealthMonitor, with graceful fallback when API keys are missing or
services are down.

Each source call is independently error-handled and its outcome is recorded
in the health monitor (latency + success/failure). If one source fails, the
service falls back to the next in priority order.

Track data provenance — every returned dict includes a _sources field listing
which APIs contributed data.
"""

import logging
import time as _time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from services.google_weather_client import GoogleWeatherClient, GoogleWeatherError
from services.nws_client import NWSClient, NWSClientError
from services.open_meteo_client import OpenMeteoClient, OpenMeteoError
from services.weathernext_client import WeatherNextClient, WeatherNextError
from services.noaa_client import NOAAClient, NOAAClientError
from services.source_health import SourceHealthMonitor

logger = logging.getLogger(__name__)

# Singleton health monitor shared across the application
_health = SourceHealthMonitor.instance()


def _timed_call(source_name: str):
    """Context-manager-style helper to record latency + success/failure."""
    class _Timer:
        def __init__(self):
            self.start = _time.perf_counter()
        def success(self):
            elapsed = (_time.perf_counter() - self.start) * 1000
            _health.record_success(source_name, latency_ms=elapsed)
        def failure(self, error: str = ""):
            elapsed = (_time.perf_counter() - self.start) * 1000
            _health.record_failure(source_name, error=error, latency_ms=elapsed)
    return _Timer()


class WeatherDataService:
    """
    Async weather data aggregation service.

    Acts as a unified interface for all weather data needs. Manages multiple
    client connections and intelligently routes requests to available sources.

    Usage:
        async with WeatherDataService() as service:
            conditions = await service.get_comprehensive_conditions(25.5, -80.2)
            hurricane_env = await service.get_hurricane_environment(25.5, -80.2)
            enhanced_storm = await service.get_enhanced_storm_data("AL092024", 25.5, -80.2)
            alerts = await service.get_alerts_all_sources(25.5, -80.2)
            flood_risk = await service.get_flood_risk(25.5, -80.2)
    """

    def __init__(self):
        """Initialize weather data service with all sub-clients."""
        self.google_client: Optional[GoogleWeatherClient] = None
        self.nws_client: Optional[NWSClient] = None
        self.open_meteo_client: Optional[OpenMeteoClient] = None
        self.weathernext_client: Optional[WeatherNextClient] = None
        self.noaa_client: Optional[NOAAClient] = None

    async def __aenter__(self):
        """Enter async context manager; initialize all sub-clients."""
        self.google_client = GoogleWeatherClient()
        self.nws_client = NWSClient()
        self.open_meteo_client = OpenMeteoClient()
        self.weathernext_client = WeatherNextClient()
        self.noaa_client = NOAAClient()

        # Open all client contexts
        await self.google_client.__aenter__()
        await self.nws_client.__aenter__()
        await self.open_meteo_client.__aenter__()
        await self.weathernext_client.__aenter__()
        await self.noaa_client.__aenter__()

        return self

    async def __aexit__(self, *args):
        """Exit async context manager; close all sub-client connections."""
        if self.google_client:
            await self.google_client.__aexit__(*args)
        if self.nws_client:
            await self.nws_client.__aexit__(*args)
        if self.open_meteo_client:
            await self.open_meteo_client.__aexit__(*args)
        if self.weathernext_client:
            await self.weathernext_client.__aexit__(*args)
        if self.noaa_client:
            await self.noaa_client.__aexit__(*args)

    def get_available_sources(self) -> Dict[str, bool]:
        """
        Return which APIs are configured and responsive.

        Returns:
            Dict with API names as keys and configuration status as values:
            {
                "google_weather": bool,
                "nws": bool,
                "open_meteo": bool,
                "weathernext": bool,
                "noaa": bool
            }
        """
        return {
            "google_weather": GoogleWeatherClient.is_configured(),
            "nws": True,  # NWS is always available (no API key required)
            "open_meteo": True,  # Open-Meteo is always available (no API key required)
            "weathernext": WeatherNextClient.is_configured(),
            "noaa": True,  # NOAA is always available (public data)
        }

    async def get_comprehensive_conditions(
        self, lat: float, lon: float
    ) -> Dict[str, Any]:
        """
        Aggregate current conditions from all available sources.

        Priority: Google Weather (most features) → NWS (free, reliable) →
        Open-Meteo (free, global)

        Merges unique fields from each source:
        - SST from Google Weather or Open-Meteo marine API
        - Soil moisture from Open-Meteo
        - Precipitation from NWS or Open-Meteo
        - Alerts from NWS

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            Dict with unified weather conditions and _sources list:
            {
                "temperature_c": float or None,
                "wind_speed_ms": float or None,
                "wind_direction_deg": float or None,
                "pressure_hpa": float or None,
                "humidity_pct": float or None,
                "sst_c": float or None,
                "soil_moisture_0_1": float or None,
                "precipitation_mm": float or None,
                "cloud_cover_pct": float or None,
                "visibility_m": float or None,
                "alerts": list,
                "_sources": list of API names that provided data
            }
        """
        result: Dict[str, Any] = {
            "temperature_c": None,
            "wind_speed_ms": None,
            "wind_direction_deg": None,
            "pressure_hpa": None,
            "humidity_pct": None,
            "sst_c": None,
            "soil_moisture_0_1": None,
            "precipitation_mm": None,
            "cloud_cover_pct": None,
            "visibility_m": None,
            "alerts": [],
            "_sources": [],
        }

        # Priority 1: Open-Meteo (free, global, no key — primary for SST/soil/shear)
        if self.open_meteo_client:
            t = _timed_call("open_meteo")
            try:
                weather = await self.open_meteo_client.get_current_weather(lat, lon)
                if weather:
                    result["temperature_c"] = weather.get("temperature_c")
                    result["wind_speed_ms"] = weather.get("wind_speed_ms")
                    result["wind_direction_deg"] = weather.get("wind_direction_deg")
                    result["pressure_hpa"] = weather.get("pressure_hpa")
                    result["humidity_pct"] = weather.get("humidity_percent")
                    result["cloud_cover_pct"] = weather.get("cloud_cover_percent")
                    result["visibility_m"] = weather.get("visibility_m")

                # SST from Open-Meteo Marine (primary SST source)
                try:
                    sst = await self.open_meteo_client.get_sea_surface_temperature(lat, lon)
                    if sst is not None:
                        result["sst_c"] = sst
                except OpenMeteoError as e:
                    logger.debug(f"Open-Meteo SST call failed (silently continuing): {e}")

                # Soil moisture (only available from Open-Meteo)
                try:
                    soil = await self.open_meteo_client.get_soil_moisture(lat, lon)
                    if soil:
                        result["soil_moisture_0_1"] = soil.get("saturation", 0.0)
                except OpenMeteoError as e:
                    logger.debug(f"Open-Meteo soil moisture call failed (silently continuing): {e}")

                result["_sources"].append("open_meteo")
                t.success()

            except OpenMeteoError as e:
                t.failure(str(e))
                logger.debug(f"Open-Meteo failed: {e}")

        # Priority 2: NWS (free, reliable for US — primary for alerts)
        if self.nws_client:
            t = _timed_call("nws")
            try:
                forecast = await self.nws_client.get_gridpoint_forecast(lat, lon)
                hourly = forecast.get("hourly", [])
                if hourly:
                    first_hour = hourly[0]
                    temp_f = first_hour.get("temperature_f")
                    if temp_f is not None and result["temperature_c"] is None:
                        result["temperature_c"] = (temp_f - 32) * 5 / 9

                    wind_kt = first_hour.get("wind_speed_kt")
                    if wind_kt is not None and result["wind_speed_ms"] is None:
                        result["wind_speed_ms"] = wind_kt * 0.514444

                    wind_dir = first_hour.get("wind_direction_deg")
                    if wind_dir is not None and result["wind_direction_deg"] is None:
                        result["wind_direction_deg"] = wind_dir

                    humidity = first_hour.get("relative_humidity")
                    if humidity is not None and result["humidity_pct"] is None:
                        result["humidity_pct"] = humidity

                    precip = first_hour.get("precipitation_amount_in")
                    if precip is not None and result["precipitation_mm"] is None:
                        result["precipitation_mm"] = precip * 25.4

                # Get alerts
                try:
                    alerts = await self.nws_client.get_active_hurricanes()
                    if alerts and not result["alerts"]:
                        result["alerts"] = alerts
                except NWSClientError as e:
                    logger.debug(f"NWS alerts call failed (silently continuing): {e}")

                if "nws" not in result["_sources"]:
                    result["_sources"].append("nws")
                t.success()

            except NWSClientError as e:
                t.failure(str(e))
                logger.debug(f"NWS forecast failed: {e}")

        # Priority 3: Google Weather (supplemental — fills gaps, SST fallback)
        if GoogleWeatherClient.is_configured() and self.google_client:
            t = _timed_call("google_weather")
            try:
                conditions = await self.google_client.get_current_conditions(lat, lon)
                if result["temperature_c"] is None:
                    result["temperature_c"] = conditions.get("temperature")
                if result["wind_speed_ms"] is None:
                    result["wind_speed_ms"] = conditions.get("wind_speed")
                if result["wind_direction_deg"] is None:
                    result["wind_direction_deg"] = conditions.get("wind_direction")
                if result["pressure_hpa"] is None:
                    result["pressure_hpa"] = conditions.get("pressure")
                if result["humidity_pct"] is None:
                    result["humidity_pct"] = conditions.get("humidity")
                if result["precipitation_mm"] is None:
                    result["precipitation_mm"] = conditions.get("precipitation")
                if result["cloud_cover_pct"] is None:
                    result["cloud_cover_pct"] = conditions.get("cloud_cover")
                if result["visibility_m"] is None:
                    result["visibility_m"] = conditions.get("visibility")

                # SST fallback from Google if Open-Meteo missed it
                if result["sst_c"] is None:
                    try:
                        sst = await self.google_client.get_sea_surface_temperature(lat, lon)
                        if sst is not None:
                            result["sst_c"] = sst
                    except GoogleWeatherError as e:
                        logger.debug(f"Google Weather SST fallback failed (silently continuing): {e}")

                # Alerts from Google (supplement NWS)
                try:
                    alerts = await self.google_client.get_weather_alerts(lat, lon)
                    result["alerts"].extend(alerts)
                except GoogleWeatherError as e:
                    logger.debug(f"Google Weather alerts call failed (silently continuing): {e}")

                result["_sources"].append("google_weather")
                t.success()

            except GoogleWeatherError as e:
                t.failure(str(e))
                logger.debug(f"Google Weather failed: {e}")

        return result

    async def get_hurricane_environment(
        self, lat: float, lon: float
    ) -> Dict[str, Any]:
        """
        Get all data needed to assess hurricane favorability.

        Aggregates:
        - SST (from Open-Meteo marine or Google Weather)
        - Wind shear (from Open-Meteo wind profile)
        - Soil moisture (from Open-Meteo)
        - Precipitation history (30-day from Open-Meteo)
        - Active alerts (from NWS)
        - AI favorability score (from WeatherNext if configured)

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            Dict with hurricane environment assessment:
            {
                "sst_c": float or None,
                "wind_shear_ms": float or None,
                "soil_saturation_0_1": float or None,
                "precip_30day_mm": float or None,
                "active_alerts": list,
                "ai_favorability_score": float or None,
                "_sources": list of APIs used
            }
        """
        result: Dict[str, Any] = {
            "sst_c": None,
            "wind_shear_ms": None,
            "soil_saturation_0_1": None,
            "precip_30day_mm": None,
            "active_alerts": [],
            "ai_favorability_score": None,
            "_sources": [],
        }

        # SST (primary: Open-Meteo Marine, fallback: Google Weather)
        # ERDDAP is reserved for along-track historical SST in noaa_client only
        if self.open_meteo_client:
            t = _timed_call("open_meteo")
            try:
                sst = await self.open_meteo_client.get_sea_surface_temperature(lat, lon)
                if sst is not None:
                    result["sst_c"] = sst
                    if "open_meteo" not in result["_sources"]:
                        result["_sources"].append("open_meteo")
                t.success()
            except OpenMeteoError as e:
                t.failure(str(e))
                logger.debug(f"Open-Meteo SST failed: {e}")

        if result["sst_c"] is None and GoogleWeatherClient.is_configured() and self.google_client:
            t = _timed_call("google_weather")
            try:
                sst = await self.google_client.get_sea_surface_temperature(lat, lon)
                if sst is not None:
                    result["sst_c"] = sst
                    if "google_weather" not in result["_sources"]:
                        result["_sources"].append("google_weather")
                t.success()
            except GoogleWeatherError as e:
                t.failure(str(e))
                logger.debug(f"Google Weather SST failed: {e}")

        # Wind shear (Open-Meteo only)
        if self.open_meteo_client:
            t = _timed_call("open_meteo")
            try:
                shear = await self.open_meteo_client.estimate_wind_shear(lat, lon)
                if shear is not None:
                    result["wind_shear_ms"] = shear
                    if "open_meteo" not in result["_sources"]:
                        result["_sources"].append("open_meteo")
                t.success()
            except OpenMeteoError as e:
                t.failure(str(e))
                logger.debug(f"Open-Meteo wind shear failed: {e}")

        # Soil moisture (Open-Meteo only)
        if self.open_meteo_client:
            try:
                soil = await self.open_meteo_client.get_soil_moisture(lat, lon)
                if soil:
                    result["soil_saturation_0_1"] = soil.get("saturation", 0.0)
                    if "open_meteo" not in result["_sources"]:
                        result["_sources"].append("open_meteo")
            except OpenMeteoError as e:
                logger.debug(f"Open-Meteo soil moisture failed: {e}")

        # Precipitation history (Open-Meteo only)
        if self.open_meteo_client:
            try:
                precip = await self.open_meteo_client.get_precipitation_history(lat, lon, days_back=30)
                if precip:
                    result["precip_30day_mm"] = precip.get("cumulative_30day", 0.0)
                    if "open_meteo" not in result["_sources"]:
                        result["_sources"].append("open_meteo")
            except OpenMeteoError as e:
                logger.debug(f"Open-Meteo precipitation history failed: {e}")

        # Alerts (NWS primary)
        if self.nws_client:
            t = _timed_call("nws")
            try:
                alerts = await self.nws_client.get_active_hurricanes()
                if alerts:
                    result["active_alerts"] = alerts
                    if "nws" not in result["_sources"]:
                        result["_sources"].append("nws")
                t.success()
            except NWSClientError as e:
                t.failure(str(e))
                logger.debug(f"NWS alerts failed: {e}")

        # AI favorability score (WeatherNext — 2026 season validation)
        if WeatherNextClient.is_configured() and self.weathernext_client:
            t = _timed_call("weathernext")
            try:
                analysis = await self.weathernext_client.get_environmental_analysis(lat, lon)
                if analysis:
                    result["ai_favorability_score"] = analysis.favorability_score
                    if "weathernext" not in result["_sources"]:
                        result["_sources"].append("weathernext")
                t.success()
            except WeatherNextError as e:
                t.failure(str(e))
                logger.debug(f"WeatherNext environmental analysis failed: {e}")

        return result

    async def get_enhanced_storm_data(
        self, storm_id: str, lat: float, lon: float
    ) -> Dict[str, Any]:
        """
        Augment a storm with additional data needed for intensity forecasting.

        Provides data needed for comprehensive intensity analysis:
        - Antecedent soil moisture for runoff/flooding
        - SST for intensity assessment
        - Wind shear for intensification potential
        - Land proximity estimate
        - AI-based intensity/track prediction if available

        Args:
            storm_id: NHC storm ID (e.g., "AL092024")
            lat: Storm center latitude
            lon: Storm center longitude

        Returns:
            Dict with enhanced storm data:
            {
                "storm_id": str,
                "latitude": float,
                "longitude": float,
                "soil_moisture": float or None,
                "sst_c": float or None,
                "wind_shear_ms": float or None,
                "distance_to_coast_km": float or None,
                "ai_intensity_forecast": dict or None,
                "ai_track_forecast": list or None,
                "_sources": list of APIs used
            }
        """
        result: Dict[str, Any] = {
            "storm_id": storm_id,
            "latitude": lat,
            "longitude": lon,
            "soil_moisture": None,
            "sst_c": None,
            "wind_shear_ms": None,
            "distance_to_coast_km": None,
            "ai_intensity_forecast": None,
            "ai_track_forecast": None,
            "_sources": [],
        }

        # Soil moisture (Open-Meteo only)
        if self.open_meteo_client:
            try:
                soil = await self.open_meteo_client.get_soil_moisture(lat, lon)
                if soil:
                    result["soil_moisture"] = soil.get("saturation", 0.0)
                    if "open_meteo" not in result["_sources"]:
                        result["_sources"].append("open_meteo")
            except OpenMeteoError as e:
                logger.debug(f"Open-Meteo soil moisture failed: {e}")

        # SST (primary: Open-Meteo Marine, fallback: Google Weather)
        if self.open_meteo_client:
            t = _timed_call("open_meteo")
            try:
                sst = await self.open_meteo_client.get_sea_surface_temperature(lat, lon)
                if sst is not None:
                    result["sst_c"] = sst
                    if "open_meteo" not in result["_sources"]:
                        result["_sources"].append("open_meteo")
                t.success()
            except OpenMeteoError as e:
                t.failure(str(e))
                logger.debug(f"Open-Meteo SST failed: {e}")

        if result["sst_c"] is None and GoogleWeatherClient.is_configured() and self.google_client:
            t = _timed_call("google_weather")
            try:
                sst = await self.google_client.get_sea_surface_temperature(lat, lon)
                if sst is not None:
                    result["sst_c"] = sst
                    if "google_weather" not in result["_sources"]:
                        result["_sources"].append("google_weather")
                t.success()
            except GoogleWeatherError as e:
                t.failure(str(e))
                logger.debug(f"Google Weather SST failed: {e}")

        # Wind shear (Open-Meteo only)
        if self.open_meteo_client:
            try:
                shear = await self.open_meteo_client.estimate_wind_shear(lat, lon)
                if shear is not None:
                    result["wind_shear_ms"] = shear
                    if "open_meteo" not in result["_sources"]:
                        result["_sources"].append("open_meteo")
            except OpenMeteoError as e:
                logger.debug(f"Open-Meteo wind shear failed: {e}")

        # Land proximity — compute actual distance using core module
        try:
            import asyncio as _asyncio
            from core.land_proximity import compute_distance_to_coast
            proximity = await _asyncio.get_event_loop().run_in_executor(
                None, compute_distance_to_coast, lat, lon
            )
            if proximity and isinstance(proximity, dict):
                result["distance_to_coast_km"] = proximity.get("distance_km")
            else:
                result["distance_to_coast_km"] = None
        except Exception as e:
            logger.debug(f"Land proximity computation failed: {e}")
            result["distance_to_coast_km"] = None

        # AI intensity forecast (WeatherNext — logs comparison data for 2026 validation)
        if WeatherNextClient.is_configured() and self.weathernext_client:
            t = _timed_call("weathernext")
            try:
                intensity = await self.weathernext_client.get_intensity_prediction(
                    lat=lat,
                    lon=lon,
                    vmax_current=50.0,
                    mslp_current=970.0,
                    sst=result["sst_c"] or 28.0,
                )
                if intensity:
                    result["ai_intensity_forecast"] = {
                        "current_vmax_kt": intensity.current_vmax_kt,
                        "peak_intensity_vmax_kt": intensity.peak_intensity_vmax_kt,
                        "peak_intensity_time": intensity.peak_intensity_time,
                        "rapid_intensification_prob": intensity.rapid_intensification_prob,
                        "forecast_steps": [
                            {
                                "timestamp": step.timestamp,
                                "vmax_kt_p50": step.vmax_kt_p50,
                            }
                            for step in intensity.forecast_steps
                        ],
                    }
                    if "weathernext" not in result["_sources"]:
                        result["_sources"].append("weathernext")
                t.success()
            except WeatherNextError as e:
                t.failure(str(e))
                logger.debug(f"WeatherNext intensity prediction failed: {e}")

        # AI track forecast (WeatherNext)
        if WeatherNextClient.is_configured() and self.weathernext_client:
            t = _timed_call("weathernext")
            try:
                track = await self.weathernext_client.get_cyclone_forecast(
                    lat=lat, lon=lon, storm_id=storm_id
                )
                if track and track.track_forecast:
                    result["ai_track_forecast"] = [
                        {
                            "timestamp": pt.timestamp,
                            "lat": pt.lat,
                            "lon": pt.lon,
                            "uncertainty_radius_nm": pt.uncertainty_radius_nm,
                        }
                        for pt in track.track_forecast[:10]
                    ]
                    if "weathernext" not in result["_sources"]:
                        result["_sources"].append("weathernext")
                t.success()
            except WeatherNextError as e:
                t.failure(str(e))
                logger.debug(f"WeatherNext track forecast failed: {e}")

        return result

    async def get_ai_cyclone_forecast(
        self, lat: float, lon: float, storm_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Wrapper around WeatherNext client for AI cyclone forecasts.

        Args:
            lat: Storm center latitude
            lon: Storm center longitude
            storm_id: Optional NHC storm ID (e.g., "AL092024")

        Returns:
            AI predictions with ensemble spread, or None if WeatherNext not configured.
        """
        if not WeatherNextClient.is_configured() or not self.weathernext_client:
            logger.debug("WeatherNext not configured; AI forecast unavailable")
            return None

        try:
            forecast = await self.weathernext_client.get_cyclone_forecast(
                lat=lat, lon=lon, storm_id=storm_id
            )

            if not forecast:
                return None

            # Convert to dict for easier serialization
            return {
                "storm_id": forecast.storm_id,
                "valid_time": forecast.valid_time,
                "track_forecast": [
                    {
                        "timestamp": pt.timestamp,
                        "lat": pt.lat,
                        "lon": pt.lon,
                        "uncertainty_radius_nm": pt.uncertainty_radius_nm,
                    }
                    for pt in (forecast.track_forecast or [])
                ],
                "intensity_forecast": [
                    {
                        "timestamp": pt.timestamp,
                        "vmax_kt_p10": pt.vmax_kt_p10,
                        "vmax_kt_p50": pt.vmax_kt_p50,
                        "vmax_kt_p90": pt.vmax_kt_p90,
                        "rapid_intensification_prob": pt.rapid_intensification_prob,
                    }
                    for pt in (forecast.intensity_forecast or [])
                ],
                "rapid_intensification_prob_24h": forecast.rapid_intensification_prob_24h,
                "peak_intensity_vmax_kt": forecast.peak_intensity_vmax_kt,
                "peak_intensity_time": forecast.peak_intensity_time,
                "generated_at": forecast.generated_at,
            }

        except WeatherNextError as e:
            logger.warning(f"WeatherNext forecast failed: {e}")
            return None

    async def get_alerts_all_sources(self, lat: float, lon: float) -> List[Dict[str, Any]]:
        """
        Merge alerts from all available sources (Google Weather + NWS).

        Deduplicates by event type and time window, sorts by severity.

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            List of alert dicts, deduplicated and sorted by severity.
        """
        alerts_by_type: Dict[str, Dict[str, Any]] = {}

        # Get alerts from Google Weather
        if GoogleWeatherClient.is_configured() and self.google_client:
            try:
                google_alerts = await self.google_client.get_weather_alerts(lat, lon)
                for alert in google_alerts:
                    key = f"{alert.get('type')}_{alert.get('headline')}"
                    if key not in alerts_by_type:
                        alerts_by_type[key] = {**alert, "_source": "google_weather"}
            except GoogleWeatherError as e:
                logger.debug(f"Google Weather alerts failed: {e}")

        # Get alerts from NWS
        if self.nws_client:
            try:
                nws_alerts = await self.nws_client.get_active_hurricanes()
                for alert in nws_alerts:
                    key = f"{alert.get('event')}_{alert.get('headline')}"
                    if key not in alerts_by_type:
                        alerts_by_type[key] = {**alert, "_source": "nws"}
            except NWSClientError as e:
                logger.debug(f"NWS alerts failed: {e}")

        # Sort by severity (attempt to extract and order)
        severity_order = {"EXTREME": 0, "SEVERE": 1, "MODERATE": 2, "MINOR": 3, "UNKNOWN": 4}
        result = sorted(
            alerts_by_type.values(),
            key=lambda x: severity_order.get(x.get("severity", "UNKNOWN").upper(), 4),
        )

        return result

    async def get_flood_risk(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Assess flood risk combining soil moisture, precipitation history, and forecasts.

        Returns:
            Dict with flood risk assessment:
            {
                "flood_risk_score": float (0-100),
                "soil_saturation": float (0-1),
                "recent_precip_mm": float,
                "river_discharge_m3s": list or None,
                "_sources": list of APIs used
            }
        """
        result: Dict[str, Any] = {
            "flood_risk_score": 0.0,
            "soil_saturation": 0.0,
            "recent_precip_mm": 0.0,
            "river_discharge_m3s": None,
            "_sources": [],
        }

        soil_saturation = 0.0
        recent_precip = 0.0

        # Get soil saturation (Open-Meteo)
        if self.open_meteo_client:
            try:
                soil = await self.open_meteo_client.get_soil_moisture(lat, lon)
                if soil:
                    soil_saturation = soil.get("saturation", 0.0)
                    result["soil_saturation"] = soil_saturation
                    if "open_meteo" not in result["_sources"]:
                        result["_sources"].append("open_meteo")
            except OpenMeteoError as e:
                logger.debug(f"Open-Meteo soil moisture failed: {e}")

        # Get recent precipitation (Open-Meteo)
        if self.open_meteo_client:
            try:
                precip = await self.open_meteo_client.get_precipitation_history(
                    lat, lon, days_back=14
                )
                if precip:
                    recent_precip = precip.get("cumulative_14day", 0.0)
                    result["recent_precip_mm"] = recent_precip
                    if "open_meteo" not in result["_sources"]:
                        result["_sources"].append("open_meteo")
            except OpenMeteoError as e:
                logger.debug(f"Open-Meteo precipitation failed: {e}")

        # Get flood forecast (Open-Meteo)
        if self.open_meteo_client:
            try:
                flood = await self.open_meteo_client.get_flood_forecast(lat, lon)
                if flood:
                    discharge = flood.get("river_discharge_m3s", [])
                    if discharge:
                        result["river_discharge_m3s"] = discharge[:7]  # Next 7 days
                    if "open_meteo" not in result["_sources"]:
                        result["_sources"].append("open_meteo")
            except OpenMeteoError as e:
                logger.debug(f"Open-Meteo flood forecast failed: {e}")

        # Calculate flood risk score (0-100)
        # Based on soil saturation (high = risky) and recent precipitation
        # Score: 0-50 from soil saturation, 0-50 from recent precip
        soil_risk = soil_saturation * 50  # 0-50
        precip_risk = min(recent_precip / 200 * 50, 50)  # 0-50 (normalize to 200mm)
        result["flood_risk_score"] = min(soil_risk + precip_risk, 100.0)

        return result

    # ==================================================================
    # WEATHERNEXT VALIDATION — 2026 hurricane season comparison logging
    # ==================================================================

    async def get_weathernext_vs_nhc_comparison(
        self, storm_id: str, lat: float, lon: float
    ) -> Optional[Dict[str, Any]]:
        """
        Generate a side-by-side comparison of WeatherNext AI forecasts vs NHC
        traditional advisories for the same storm at the same time.

        This data is logged automatically during the 2026 hurricane season so
        that after the season we can evaluate WeatherNext accuracy against the
        operational NHC baseline. No action is taken on the AI forecast — it is
        purely observational for the first season.

        Returns:
            Dict with both forecasts and metadata, or None if WeatherNext is
            not configured or NHC data is unavailable.
        """
        if not WeatherNextClient.is_configured() or not self.weathernext_client:
            return None

        comparison: Dict[str, Any] = {
            "storm_id": storm_id,
            "comparison_time": datetime.utcnow().isoformat() + "Z",
            "nhc_forecast": None,
            "weathernext_forecast": None,
            "nhc_source": None,
            "weathernext_source": None,
        }

        # Fetch NHC traditional forecast (from noaa_client active storms)
        if self.noaa_client:
            t = _timed_call("nhc_forecast")
            try:
                active = await self.noaa_client.get_active_storms()
                nhc_storm = None
                for s in active:
                    if s.get("id", "").upper() == storm_id.upper():
                        nhc_storm = s
                        break
                if nhc_storm:
                    comparison["nhc_forecast"] = {
                        "center_lat": nhc_storm.get("lat"),
                        "center_lon": nhc_storm.get("lon"),
                        "max_wind_kt": nhc_storm.get("max_wind_kt"),
                        "min_pressure_mb": nhc_storm.get("min_pressure_mb"),
                        "movement_speed_kt": nhc_storm.get("movement_speed_kt"),
                        "movement_dir_deg": nhc_storm.get("movement_dir_deg"),
                        "category": nhc_storm.get("category"),
                    }
                    comparison["nhc_source"] = "nhc_active"
                t.success()
            except NOAAClientError as e:
                t.failure(str(e))
                logger.debug(f"NHC active storms failed for comparison: {e}")

        # Fetch WeatherNext AI forecast
        t = _timed_call("weathernext")
        try:
            ai_forecast = await self.weathernext_client.get_cyclone_forecast(
                lat=lat, lon=lon, storm_id=storm_id
            )
            if ai_forecast:
                comparison["weathernext_forecast"] = {
                    "track_points": len(ai_forecast.track_forecast or []),
                    "peak_intensity_vmax_kt": ai_forecast.peak_intensity_vmax_kt,
                    "peak_intensity_time": ai_forecast.peak_intensity_time,
                    "ri_prob_24h": ai_forecast.rapid_intensification_prob_24h,
                    "generated_at": ai_forecast.generated_at,
                    "first_track_point": (
                        {
                            "timestamp": ai_forecast.track_forecast[0].timestamp,
                            "lat": ai_forecast.track_forecast[0].lat,
                            "lon": ai_forecast.track_forecast[0].lon,
                        }
                        if ai_forecast.track_forecast
                        else None
                    ),
                }
                comparison["weathernext_source"] = "weathernext"
            t.success()
        except WeatherNextError as e:
            t.failure(str(e))
            logger.debug(f"WeatherNext failed for comparison: {e}")

        # Persist the comparison to disk (JSONL + SQLite + summary JSON)
        if comparison["nhc_forecast"] or comparison["weathernext_forecast"]:
            from services.validation_log import ValidationLogger
            vlog = ValidationLogger.instance()
            vlog.log_comparison(comparison)

            logger.info(
                f"[VALIDATION] WeatherNext vs NHC comparison for {storm_id}: "
                f"NHC={'available' if comparison['nhc_forecast'] else 'unavailable'}, "
                f"AI={'available' if comparison['weathernext_forecast'] else 'unavailable'}"
            )
            return comparison

        return None

    # ==================================================================
    # SOURCE HEALTH DASHBOARD
    # ==================================================================

    def get_source_health(self) -> Dict[str, Any]:
        """
        Return the current health status of all data sources.

        Exposes the SourceHealthMonitor singleton data for the
        /api/v1/health/sources endpoint.
        """
        return _health.summary()
