"""
FastAPI route definitions for weather data integration endpoints.

Endpoints provide comprehensive weather data from multiple integrated sources:
  - /api/v1/weather/conditions/{lat}/{lon} — current weather conditions
  - /api/v1/weather/environment/{lat}/{lon} — hurricane environment analysis
  - /api/v1/weather/storm/{storm_id}/enhanced — enhanced storm data with all sources
  - /api/v1/weather/forecast/cyclone/{lat}/{lon} — AI cyclone forecast from WeatherNext
  - /api/v1/weather/alerts/{lat}/{lon} — merged weather alerts from all sources
  - /api/v1/weather/flood-risk/{lat}/{lon} — flood risk assessment
  - /api/v1/weather/sources — availability of configured weather APIs
  - /api/v1/weather/land-proximity/{lat}/{lon} — land proximity analysis
  - /api/v1/weather/terrain/{lat}/{lon} — terrain analysis
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/weather", tags=["weather"])


@router.get("/conditions/{lat}/{lon}")
async def get_weather_conditions(
    lat: float,
    lon: float,
    sources: Optional[str] = Query(
        None,
        description="Comma-separated list of preferred sources (e.g., 'google_weather,nws')"
    )
) -> Dict[str, Any]:
    """
    Get comprehensive current weather conditions at a location.

    Returns temperature, wind, pressure, sea surface temperature, soil moisture,
    and active weather alerts. Intelligently aggregates data from all available
    sources (Google Weather, NWS, Open-Meteo) with graceful fallback.

    **Response includes:**
    - temperature_c: Current temperature in Celsius
    - wind_speed_ms: Wind speed in meters/second
    - wind_direction_deg: Wind direction in degrees (0=N)
    - pressure_hpa: Atmospheric pressure in hectopascals
    - humidity_pct: Relative humidity as percentage
    - sst_c: Sea surface temperature in Celsius (where applicable)
    - soil_moisture_0_1: Normalized soil moisture (0-1 range)
    - precipitation_mm: Recent precipitation in millimeters
    - cloud_cover_pct: Cloud cover as percentage
    - visibility_m: Visibility in meters
    - alerts: List of active weather alerts
    - _sources: Which APIs provided this data
    """
    logger.debug(f"get_weather_conditions: lat={lat}, lon={lon}, sources={sources}")

    try:
        from services.weather_data_service import WeatherDataService
        async with WeatherDataService() as service:
            result = await service.get_comprehensive_conditions(lat, lon)
            return result
    except Exception as e:
        logger.error(f"Failed to get weather conditions: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve weather conditions: {str(e)}"
        )


@router.get("/environment/{lat}/{lon}")
async def get_hurricane_environment(
    lat: float,
    lon: float
) -> Dict[str, Any]:
    """
    Get hurricane environment analysis for a location.

    Analyzes the atmospheric and oceanic environment that supports or inhibits
    cyclone formation and intensification. This is the key endpoint for enhancing
    DPI (Dynamical Potential Index) calculations.

    **Response includes:**
    - sst_c: Sea surface temperature (primary fuel for intensification)
    - wind_shear_ms: Environmental wind shear magnitude
    - soil_moisture_0_1: Soil moisture normalized to 0-1 range
    - precip_history_mm: Recent precipitation history
    - ai_favorability_score: 0-100 AI assessment of environment for intensification
    - _sources: Which APIs contributed to this analysis
    """
    logger.debug(f"get_hurricane_environment: lat={lat}, lon={lon}")

    try:
        from services.weather_data_service import WeatherDataService
        async with WeatherDataService() as service:
            result = await service.get_hurricane_environment(lat, lon)
            return result
    except Exception as e:
        logger.error(f"Failed to get hurricane environment: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve hurricane environment: {str(e)}"
        )


@router.get("/storm/{storm_id}/enhanced")
async def get_enhanced_storm_data(
    storm_id: str,
    lat: float = Query(..., description="Storm center latitude"),
    lon: float = Query(..., description="Storm center longitude")
) -> Dict[str, Any]:
    """
    Get enhanced storm data with all available API sources integrated.

    Combines official NHC data with environmental analysis from all integrated
    weather APIs to provide a complete picture of the storm and its environment.

    **Response includes:**
    - storm_id: Official storm identifier (e.g., AL092024)
    - lat, lon: Storm center position
    - soil_moisture_0_1: Soil moisture near storm center
    - sst_c: Sea surface temperature ahead of storm
    - wind_shear_ms: Environmental wind shear
    - distance_to_coast_km: Distance to nearest coastline
    - ai_predictions: Dict with intensity and track forecasts
    - _sources: Which APIs provided this data
    """
    logger.debug(f"get_enhanced_storm_data: storm_id={storm_id}, lat={lat}, lon={lon}")

    try:
        from services.weather_data_service import WeatherDataService
        async with WeatherDataService() as service:
            result = await service.get_enhanced_storm_data(storm_id, lat, lon)
            return result
    except Exception as e:
        logger.error(f"Failed to get enhanced storm data: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve enhanced storm data: {str(e)}"
        )


@router.get("/forecast/cyclone/{lat}/{lon}")
async def get_ai_cyclone_forecast(
    lat: float,
    lon: float,
    storm_id: Optional[str] = Query(None, description="Optional storm ID for context")
) -> Dict[str, Any]:
    """
    Get AI cyclone forecast from WeatherNext.

    Provides advanced AI-based forecasts for cyclone track and intensity evolution
    using ensemble methods. Requires WeatherNext API configuration (GCP credentials).

    **Response includes:**
    - track_prediction: List of forecast points with lat, lon, timestamp
    - intensity_forecast: Predicted intensity changes over time
    - ensemble_members: Multiple ensemble member predictions
    - confidence_scores: Confidence levels for track and intensity
    - _sources: ['weathernext'] if successful

    **Returns 503 if:**
    - WeatherNext API is not configured
    - GCP credentials are missing or invalid
    """
    logger.debug(f"get_ai_cyclone_forecast: lat={lat}, lon={lon}, storm_id={storm_id}")

    try:
        from services.weather_data_service import WeatherDataService
        async with WeatherDataService() as service:
            result = await service.get_ai_cyclone_forecast(lat, lon, storm_id)
            if not result or not result.get("_sources"):
                raise HTTPException(
                    status_code=503,
                    detail="WeatherNext API not configured"
                )
            return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get AI cyclone forecast: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"WeatherNext service unavailable: {str(e)}"
        )


@router.get("/alerts/{lat}/{lon}")
async def get_weather_alerts(
    lat: float,
    lon: float
) -> Dict[str, Any]:
    """
    Get all weather alerts merged from all available sources.

    Deduplicates and aggregates alerts from NWS, Google Weather, and other sources.
    Alerts are sorted by severity (highest first).

    **Response includes:**
    - alerts: List of alert objects with:
      - headline: Alert title
      - description: Full alert text
      - severity: 'Extreme', 'Severe', 'Moderate', 'Minor', 'Unknown'
      - effective: Start time
      - expires: Expiration time
      - source: Which API provided the alert
    - deduplicated_count: Number of duplicate alerts removed
    - total_sources: How many APIs were consulted
    - _sources: Which APIs had data
    """
    logger.debug(f"get_weather_alerts: lat={lat}, lon={lon}")

    try:
        from services.weather_data_service import WeatherDataService
        async with WeatherDataService() as service:
            result = await service.get_alerts_all_sources(lat, lon)
            return result
    except Exception as e:
        logger.error(f"Failed to get weather alerts: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve weather alerts: {str(e)}"
        )


@router.get("/flood-risk/{lat}/{lon}")
async def get_flood_risk_assessment(
    lat: float,
    lon: float
) -> Dict[str, Any]:
    """
    Get flood risk assessment for a location.

    Analyzes flood risk based on soil saturation, recent precipitation,
    topography, and river/coastal proximity. Provides a 0-100 risk score.

    **Response includes:**
    - flood_risk_score: 0-100 overall flood risk (0=lowest, 100=highest)
    - flood_risk_level: 'low', 'moderate', 'high', 'very_high', 'extreme'
    - soil_saturation_pct: Current soil water saturation percentage
    - precip_history_mm: Total precipitation last 24-48 hours
    - precip_forecast_mm: Expected precipitation next 24-48 hours
    - river_forecast: Nearest river stage forecast (if available)
    - coastal_proximity_km: Distance to coast (oceanside only)
    - _sources: Which APIs provided this data
    """
    logger.debug(f"get_flood_risk_assessment: lat={lat}, lon={lon}")

    try:
        from services.weather_data_service import WeatherDataService
        async with WeatherDataService() as service:
            result = await service.get_flood_risk(lat, lon)
            return result
    except Exception as e:
        logger.error(f"Failed to get flood risk assessment: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve flood risk: {str(e)}"
        )


@router.get("/sources")
async def get_available_sources() -> Dict[str, Dict[str, Any]]:
    """
    Check which weather APIs are configured and available.

    Useful for frontend to understand data source limitations and to inform
    the user when certain advanced features (like AI forecasts) are unavailable.

    **Response format:**
    ```json
    {
        "google_weather": {
            "configured": true,
            "status": "available",
            "features": ["conditions", "sst", "alerts", "soil_moisture"]
        },
        "nws": {
            "configured": true,
            "status": "available",
            "features": ["conditions", "alerts", "forecast"]
        },
        "open_meteo": {
            "configured": true,
            "status": "available",
            "features": ["conditions", "soil_moisture", "precipitation"]
        },
        "weathernext": {
            "configured": false,
            "status": "not_configured",
            "features": ["ai_forecast"]
        },
        "noaa": {
            "configured": true,
            "status": "available",
            "features": ["hurricane_data", "marine_data"]
        }
    }
    ```
    """
    logger.debug("get_available_sources")

    try:
        from services.weather_data_service import WeatherDataService
        async with WeatherDataService() as service:
            sources = service.get_available_sources()
            result = {}
            for source_name, is_configured in sources.items():
                result[source_name] = {
                    "configured": is_configured,
                    "status": "available" if is_configured else "not_configured",
                }
            return result
    except Exception as e:
        logger.error(f"Failed to get available sources: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve source information: {str(e)}"
        )


@router.get("/land-proximity/{lat}/{lon}")
async def get_land_proximity(
    lat: float,
    lon: float,
    r34_nm: Optional[float] = Query(None, description="R34 (gale-force) wind radius in nautical miles")
) -> Dict[str, Any]:
    """
    Analyze land proximity and population threat for a location.

    Uses geospatial data to determine distance to nearest land, identify
    the nearest region/coastline, and estimate population exposure.

    **Response includes:**
    - distance_km: Distance to nearest land in kilometers
    - nearest_region: Name of nearest geographic region/coast
    - proximity_factor: 0-1 normalized proximity metric
    - population_threat: Estimated population in threatened area
    - coastal_threat_level: 'ocean', 'near_coast', 'coastal', 'inland'
    - threat_summary: Human-readable threat description
    """
    logger.debug(f"get_land_proximity: lat={lat}, lon={lon}, r34_nm={r34_nm}")

    try:
        import asyncio
        from core.land_proximity import compute_distance_to_coast

        # Run sync function in thread pool
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            compute_distance_to_coast,
            lat,
            lon
        )
        return result
    except Exception as e:
        logger.error(f"Failed to analyze land proximity: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to analyze land proximity: {str(e)}"
        )


@router.get("/terrain/{lat}/{lon}")
async def get_terrain_analysis(
    lat: float,
    lon: float,
    approach_deg: Optional[float] = Query(None, description="Storm approach direction in degrees (0-360)")
) -> Dict[str, Any]:
    """
    Get terrain analysis for a location.

    Analyzes elevation, topography, and orographic effects that influence
    wind fields, precipitation, and storm intensification.

    **Response includes:**
    - orographic_factor: 0-1 factor representing terrain-induced wind enhancement
    - elevation_m: Mean elevation in grid cell
    - elevation_std_m: Standard deviation of elevation (terrain roughness)
    - elevation_vulnerability: 0-1 score of elevation-based vulnerability
    - terrain_profile: Dict with elevation profile data
    - major_features: List of significant terrain features nearby
    - terrain_type: 'ocean', 'coastal_plain', 'elevated', 'mountainous'
    """
    logger.debug(f"get_terrain_analysis: lat={lat}, lon={lon}, approach_deg={approach_deg}")

    try:
        import asyncio
        from core.terrain import get_terrain_profile

        # Run sync function in thread pool
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            get_terrain_profile,
            lat,
            lon
        )
        return result
    except Exception as e:
        logger.error(f"Failed to analyze terrain: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to analyze terrain: {str(e)}"
        )
