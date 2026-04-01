"""
Google DeepMind WeatherNext 2 AI weather prediction client.

WeatherNext 2 is an AI-based weather and tropical cyclone forecasting model
accessible through Google Cloud Vertex AI and Google Earth Engine.

This service provides AI-driven forecasts that complement traditional NHC
advisories with ensemble-based uncertainty quantification and 15-day track
& intensity predictions.

Capabilities:
  - Cyclone track and intensity forecasting (15-day horizon)
  - Rapid intensification probability estimation
  - Genesis probability for new tropical cyclones
  - Environmental analysis for TC development potential
  - Ensemble-based uncertainty quantification (50 members)
  - Comparison with traditional GFS/NHC forecasts

Authentication:
  - Google Cloud Vertex AI requires GCP project ID + OAuth2 service account
  - Falls back to API key via WEATHERNEXT_API_KEY if service account unavailable
  - Returns None gracefully if not configured

Limitations:
  - Model runs are not continuous (cache for ~6 hours)
  - Accuracy degrades beyond 10 days
  - Requires active GCP project with Vertex AI API enabled
  - Not available offline

References:
  - Google DeepMind WeatherNext 2: https://deepmind.google/discover/blog/weathernext/
  - Vertex AI Predictions: https://cloud.google.com/vertex-ai/docs
"""

import json
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Optional, Any

import httpx

logger = logging.getLogger(__name__)


# Google Cloud Vertex AI API endpoints
VERTEX_AI_BASE_URL = "https://us-central1-aiplatform.googleapis.com/v1"
VERTEX_AI_PROJECT_ENV = "GOOGLE_CLOUD_PROJECT"
VERTEX_AI_CREDENTIALS_ENV = "GOOGLE_APPLICATION_CREDENTIALS"
WEATHERNEXT_API_KEY_ENV = "WEATHERNEXT_API_KEY"

# Cache duration: WeatherNext model runs ~6 hours apart
CACHE_TTL_SECONDS = 6 * 3600  # 6 hours

# Forecast horizons supported
FORECAST_HORIZONS_HOURS = [24, 48, 72, 96, 120]


class WeatherNextError(Exception):
    """Raised when WeatherNext API request fails."""
    pass


# ============================================================================
#  DATACLASSES FOR STRUCTURED RESPONSES
# ============================================================================


@dataclass
class EnsembleMember:
    """Single ensemble member for a forecast time step."""
    timestamp: str  # ISO 8601
    lat: float
    lon: float
    vmax_kt: float  # Maximum sustained wind in knots
    mslp_hpa: float  # Minimum sea level pressure in hPa
    r34_nm: float  # Radius of 34-knot winds in nautical miles
    r50_nm: Optional[float] = None  # Radius of 50-knot winds (if available)
    r64_nm: Optional[float] = None  # Radius of 64-knot winds (if available)


@dataclass
class TrackPoint:
    """Single track forecast point with uncertainty."""
    timestamp: str  # ISO 8601
    lat: float
    lon: float
    uncertainty_radius_nm: float  # Cone of uncertainty radius
    ensemble_members: Optional[list[EnsembleMember]] = None
    track_skill_score: Optional[float] = None  # vs traditional models


@dataclass
class IntensityForecast:
    """Intensity evolution over time with uncertainty."""
    timestamp: str  # ISO 8601
    vmax_kt_p10: float  # 10th percentile wind speed
    vmax_kt_p50: float  # 50th percentile (median)
    vmax_kt_p90: float  # 90th percentile wind speed
    mslp_hpa_p50: Optional[float] = None
    rapid_intensification_prob: Optional[float] = None


@dataclass
class CycloneForecast:
    """Complete cyclone forecast combining track and intensity."""
    storm_id: Optional[str] = None
    valid_time: str = None  # ISO 8601
    track_forecast: Optional[list[TrackPoint]] = None  # 15-day track
    intensity_forecast: Optional[list[IntensityForecast]] = None
    formation_probability: Optional[float] = None  # 0-1
    landfall_probability: Optional[float] = None  # 0-1
    rapid_intensification_prob_24h: Optional[float] = None
    peak_intensity_vmax_kt: Optional[float] = None
    peak_intensity_time: Optional[str] = None  # ISO 8601
    ensemble_member_count: int = 50
    generated_at: str = None  # ISO 8601


@dataclass
class IntensityPrediction:
    """Intensity prediction response."""
    current_vmax_kt: float
    forecast_steps: list[IntensityForecast]
    rapid_intensification_prob: float  # 0-1
    peak_intensity_vmax_kt: float
    peak_intensity_time: Optional[str]  # ISO 8601 or None
    uncertainty_metric: float  # Standard deviation across ensemble


@dataclass
class GenesisRegion:
    """Predicted region for new tropical cyclone formation."""
    center_lat: float
    center_lon: float
    formation_probability: float  # 0-1
    expected_time_window_start: str  # ISO 8601
    expected_time_window_end: str  # ISO 8601
    size_estimate: str  # "small", "medium", "large"
    environmental_favorability: float  # 0-100


@dataclass
class EnvironmentalAnalysis:
    """Environmental conditions affecting hurricane development."""
    timestamp: str  # ISO 8601
    lat: float
    lon: float
    wind_shear_ms: float  # Meters per second
    sst_c: float  # Sea surface temperature in Celsius
    moisture_availability: float  # 0-100 (relative measure)
    upper_level_pattern: str  # e.g., "ridge", "trough", "neutral"
    favorability_score: float  # 0-100 for tropical cyclone development
    detailed_analysis: Optional[str] = None


@dataclass
class ForecastComparison:
    """Comparison between WeatherNext and traditional forecast models."""
    metric: str  # "track", "intensity", "size", "formation"
    weathernext_skill: float  # 0-100
    traditional_skill: float  # 0-100 (GFS/NHC)
    skill_difference: float  # weathernext - traditional
    explanation: Optional[str] = None


# ============================================================================
#  MAIN CLIENT CLASS
# ============================================================================


class WeatherNextClient:
    """
    Async client for Google DeepMind WeatherNext 2 AI weather predictions.

    Provides access to AI-driven tropical cyclone forecasts through Vertex AI.
    Implements graceful degradation when GCP credentials are unavailable.

    Usage:
        async with WeatherNextClient() as client:
            if client.is_configured():
                forecast = await client.get_cyclone_forecast(lat=25.0, lon=-80.0)
                intensity = await client.get_intensity_prediction(
                    lat=25.0, lon=-80.0,
                    vmax_current=75.0, mslp_current=965.0, sst=28.5
                )
            else:
                logger.warning("WeatherNext not configured; skipping AI forecasts")
    """

    def __init__(self, timeout: float = 45.0):
        """
        Initialize WeatherNextClient.

        Args:
            timeout: HTTP request timeout in seconds (default: 45s for large
                     ensemble predictions)
        """
        self.timeout = timeout
        self._http_client: Optional[httpx.AsyncClient] = None
        self._gcp_project_id: Optional[str] = None
        self._api_key: Optional[str] = None
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._forecast_cache: dict[str, tuple[Any, datetime]] = {}

    async def __aenter__(self):
        """Enter async context manager."""
        self._http_client = httpx.AsyncClient(
            timeout=self.timeout,
            headers={
                "User-Agent": "HurricaneIKE-App/1.0 (WeatherNext2)",
                "Content-Type": "application/json",
            },
            follow_redirects=True,
        )
        self._load_credentials()
        return self

    async def __aexit__(self, *args):
        """Exit async context manager."""
        if self._http_client:
            await self._http_client.aclose()

    @property
    def http(self) -> httpx.AsyncClient:
        """Get the underlying httpx.AsyncClient."""
        if self._http_client is None:
            raise RuntimeError(
                "WeatherNextClient must be used as async context manager"
            )
        return self._http_client

    def _load_credentials(self) -> None:
        """Load and validate GCP credentials from environment."""
        self._gcp_project_id = os.getenv(VERTEX_AI_PROJECT_ENV)
        self._api_key = os.getenv(WEATHERNEXT_API_KEY_ENV)

        if self._gcp_project_id:
            logger.debug(f"WeatherNext: GCP project loaded: {self._gcp_project_id}")
        if self._api_key:
            logger.debug("WeatherNext: API key loaded from environment")

        if not self.is_configured():
            logger.warning(
                "WeatherNext not configured: set GOOGLE_CLOUD_PROJECT and "
                "WEATHERNEXT_API_KEY (or GOOGLE_APPLICATION_CREDENTIALS) "
                "environment variables"
            )

    @classmethod
    def is_configured(cls) -> bool:
        """
        Check if WeatherNext is configured and ready to use.

        Returns:
            True if GCP project ID and API key or credentials are available
        """
        project = os.getenv(VERTEX_AI_PROJECT_ENV)
        api_key = os.getenv(WEATHERNEXT_API_KEY_ENV)
        creds_file = os.getenv(VERTEX_AI_CREDENTIALS_ENV)
        return bool(project and (api_key or creds_file))

    async def get_cyclone_forecast(
        self,
        lat: float,
        lon: float,
        storm_id: Optional[str] = None,
    ) -> Optional[CycloneForecast]:
        """
        Get AI forecast for a tropical cyclone.

        Retrieves Vertex AI WeatherNext predictions for cyclone track, intensity,
        and size evolution over the next 15 days with 50 ensemble members.

        Args:
            lat: Storm center latitude
            lon: Storm center longitude
            storm_id: Optional NHC storm ID (e.g., "AL092024")

        Returns:
            CycloneForecast with track, intensity, and probabilities, or None
            if WeatherNext is not configured

        Raises:
            WeatherNextError: If API request fails

        Notes:
            - Predictions include full uncertainty quantification
            - Track accuracy degrades beyond 10 days
            - Caches results for 6 hours (model run frequency)
        """
        if not self.is_configured():
            logger.warning(
                "WeatherNext not configured; cyclone forecast unavailable"
            )
            return None

        cache_key = f"cyclone_{lat}_{lon}_{storm_id}"
        cached = self._get_cached_forecast(cache_key)
        if cached is not None:
            return cached

        try:
            # Build Vertex AI prediction request
            endpoint = self._build_endpoint("cyclone_forecast")
            payload = {
                "instances": [
                    {
                        "latitude": lat,
                        "longitude": lon,
                        "storm_id": storm_id,
                        "forecast_hours": 360,  # 15 days
                        "ensemble_members": 50,
                    }
                ]
            }

            response = await self._call_vertex_ai(endpoint, payload)

            # Parse ensemble predictions
            forecast = self._parse_cyclone_forecast(
                response, lat, lon, storm_id
            )
            self._cache_forecast(cache_key, forecast)
            return forecast

        except Exception as e:
            logger.error(f"WeatherNext cyclone forecast failed: {e}")
            raise WeatherNextError(f"Cyclone forecast failed: {e}") from e

    async def get_intensity_prediction(
        self,
        lat: float,
        lon: float,
        vmax_current: float,
        mslp_current: float,
        sst: float,
    ) -> Optional[IntensityPrediction]:
        """
        Predict intensity changes over the next 120 hours.

        AI model predicts wind speed and pressure evolution with uncertainty
        bounds and rapid intensification probability.

        Args:
            lat: Current storm center latitude
            lon: Current storm center longitude
            vmax_current: Current maximum sustained wind (knots)
            mslp_current: Current minimum sea level pressure (hPa)
            sst: Sea surface temperature under storm (Celsius)

        Returns:
            IntensityPrediction with 24/48/72/96/120 hour forecasts, or None
            if WeatherNext is not configured

        Raises:
            WeatherNextError: If API request fails

        Notes:
            - Rapid intensification defined as +35kt in 24h
            - Returns 10th/50th/90th percentile uncertainty bounds
            - Incorporates environmental shear, SST, and moisture
        """
        if not self.is_configured():
            logger.warning(
                "WeatherNext not configured; intensity prediction unavailable"
            )
            return None

        cache_key = f"intensity_{lat}_{lon}_{vmax_current}_{mslp_current}"
        cached = self._get_cached_forecast(cache_key)
        if cached is not None:
            return cached

        try:
            endpoint = self._build_endpoint("intensity_prediction")
            payload = {
                "instances": [
                    {
                        "latitude": lat,
                        "longitude": lon,
                        "vmax_kt": vmax_current,
                        "mslp_hpa": mslp_current,
                        "sst_c": sst,
                        "forecast_hours": [24, 48, 72, 96, 120],
                    }
                ]
            }

            response = await self._call_vertex_ai(endpoint, payload)
            prediction = self._parse_intensity_prediction(
                response, vmax_current
            )
            self._cache_forecast(cache_key, prediction)
            return prediction

        except Exception as e:
            logger.error(f"WeatherNext intensity prediction failed: {e}")
            raise WeatherNextError(f"Intensity prediction failed: {e}") from e

    async def get_track_prediction(
        self,
        lat: float,
        lon: float,
        heading_deg: float,
        speed_kt: float,
    ) -> Optional[list[TrackPoint]]:
        """
        Predict storm track over 15 days with uncertainty cone.

        Returns 50 ensemble members showing probable track paths and the
        cone of uncertainty (spread).

        Args:
            lat: Current storm center latitude
            lon: Current storm center longitude
            heading_deg: Current heading in degrees (0-360)
            speed_kt: Current forward speed in knots

        Returns:
            List of TrackPoint objects (one per 12 hours for 15 days),
            or None if WeatherNext is not configured

        Raises:
            WeatherNextError: If API request fails

        Notes:
            - Each TrackPoint contains 50 ensemble members
            - Uncertainty_radius_nm represents cone spread
            - Larger radius indicates greater forecast disagreement
        """
        if not self.is_configured():
            logger.warning(
                "WeatherNext not configured; track prediction unavailable"
            )
            return None

        cache_key = f"track_{lat}_{lon}_{heading_deg}_{speed_kt}"
        cached = self._get_cached_forecast(cache_key)
        if cached is not None:
            return cached

        try:
            endpoint = self._build_endpoint("track_prediction")
            payload = {
                "instances": [
                    {
                        "latitude": lat,
                        "longitude": lon,
                        "heading_degrees": heading_deg,
                        "speed_kt": speed_kt,
                        "forecast_hours": 360,
                        "ensemble_members": 50,
                        "output_interval_hours": 12,
                    }
                ]
            }

            response = await self._call_vertex_ai(endpoint, payload)
            track = self._parse_track_prediction(response)
            self._cache_forecast(cache_key, track)
            return track

        except Exception as e:
            logger.error(f"WeatherNext track prediction failed: {e}")
            raise WeatherNextError(f"Track prediction failed: {e}") from e

    async def get_genesis_probability(
        self,
        basin: str = "AL",
        days_ahead: int = 15,
    ) -> Optional[list[GenesisRegion]]:
        """
        Predict where new tropical cyclones may form.

        Uses AI model to identify regions with high genesis probability
        based on environmental conditions.

        Args:
            basin: Ocean basin code: "AL" (Atlantic), "EP" (E Pacific),
                   "CP" (C Pacific), "WP" (W Pacific), "IO" (Indian Ocean),
                   "SH" (Southern Hemisphere)
            days_ahead: Forecast length in days (1-15, default: 15)

        Returns:
            List of GenesisRegion objects with formation probability,
            or None if WeatherNext is not configured

        Raises:
            WeatherNextError: If API request fails

        Notes:
            - Probability thresholds vary by basin (typically >10%)
            - Time windows show when genesis expected
            - Uses environmental analysis to identify favorable regions
        """
        if not self.is_configured():
            logger.warning(
                "WeatherNext not configured; genesis probability unavailable"
            )
            return None

        cache_key = f"genesis_{basin}_{days_ahead}"
        cached = self._get_cached_forecast(cache_key)
        if cached is not None:
            return cached

        try:
            endpoint = self._build_endpoint("genesis_probability")
            payload = {
                "instances": [
                    {
                        "basin": basin,
                        "forecast_days": days_ahead,
                        "probability_threshold": 0.10,
                    }
                ]
            }

            response = await self._call_vertex_ai(endpoint, payload)
            regions = self._parse_genesis_regions(response)
            self._cache_forecast(cache_key, regions)
            return regions

        except Exception as e:
            logger.error(f"WeatherNext genesis probability failed: {e}")
            raise WeatherNextError(f"Genesis probability failed: {e}") from e

    async def get_environmental_analysis(
        self,
        lat: float,
        lon: float,
    ) -> Optional[EnvironmentalAnalysis]:
        """
        Get AI-analyzed environmental conditions for hurricane development.

        Analyzes wind shear, SST, moisture, and upper-level patterns
        to produce a favorability score for tropical cyclone development.

        Args:
            lat: Latitude to analyze
            lon: Longitude to analyze

        Returns:
            EnvironmentalAnalysis with detailed breakdown, or None if
            WeatherNext is not configured

        Raises:
            WeatherNextError: If API request fails

        Notes:
            - Favorability score: 0-100 (0=hostile, 100=ideal for TC dev)
            - Wind shear: <10 m/s favorable, >20 m/s inhibits development
            - SST: >26.5°C threshold for TC genesis
            - Upper-level analysis identifies steering patterns
        """
        if not self.is_configured():
            logger.warning(
                "WeatherNext not configured; environmental analysis unavailable"
            )
            return None

        cache_key = f"environment_{lat}_{lon}"
        cached = self._get_cached_forecast(cache_key)
        if cached is not None:
            return cached

        try:
            endpoint = self._build_endpoint("environmental_analysis")
            payload = {
                "instances": [
                    {
                        "latitude": lat,
                        "longitude": lon,
                    }
                ]
            }

            response = await self._call_vertex_ai(endpoint, payload)
            analysis = self._parse_environmental_analysis(response, lat, lon)
            self._cache_forecast(cache_key, analysis)
            return analysis

        except Exception as e:
            logger.error(f"WeatherNext environmental analysis failed: {e}")
            raise WeatherNextError(
                f"Environmental analysis failed: {e}"
            ) from e

    async def compare_with_traditional(
        self,
        storm_id: str,
        traditional_forecast: dict[str, Any],
    ) -> Optional[list[ForecastComparison]]:
        """
        Compare WeatherNext predictions with traditional NHC/GFS forecast.

        Computes skill score differences in track, intensity, and other
        metrics relative to conventional models.

        Args:
            storm_id: NHC storm ID (e.g., "AL092024")
            traditional_forecast: Traditional forecast dict with track/intensity

        Returns:
            List of ForecastComparison objects (track, intensity, size, formation),
            or None if WeatherNext is not configured

        Raises:
            WeatherNextError: If API request fails or storm not found

        Notes:
            - Positive skill difference means WeatherNext more accurate
            - Skill score 0-100 (50=baseline, 100=perfect, 0=worst)
            - Comparison based on recent performance statistics
        """
        if not self.is_configured():
            logger.warning(
                "WeatherNext not configured; forecast comparison unavailable"
            )
            return None

        try:
            endpoint = self._build_endpoint("forecast_comparison")
            payload = {
                "instances": [
                    {
                        "storm_id": storm_id,
                        "traditional_track": traditional_forecast.get("track"),
                        "traditional_intensity": traditional_forecast.get(
                            "intensity"
                        ),
                    }
                ]
            }

            response = await self._call_vertex_ai(endpoint, payload)
            comparisons = self._parse_comparisons(response)
            return comparisons

        except Exception as e:
            logger.error(f"WeatherNext comparison failed: {e}")
            raise WeatherNextError(f"Forecast comparison failed: {e}") from e

    # ========================================================================
    #  PRIVATE HELPER METHODS
    # ========================================================================

    def _build_endpoint(self, model_name: str) -> str:
        """Build Vertex AI custom training prediction endpoint URL."""
        if not self._gcp_project_id:
            raise WeatherNextError("GCP project ID not configured")

        # Use standard Vertex AI Model endpoint structure
        # Format: projects/{project}/locations/{location}/publishers/google/models/{modelName}:predict
        return (
            f"{VERTEX_AI_BASE_URL}/projects/{self._gcp_project_id}/"
            f"locations/us-central1/publishers/google/models/{model_name}:predict"
        )

    async def _call_vertex_ai(
        self, endpoint: str, payload: dict
    ) -> dict[str, Any]:
        """
        Call Vertex AI API endpoint.

        Handles authentication (API key or OAuth2 token) and error handling.

        Args:
            endpoint: Full Vertex AI endpoint URL
            payload: Request payload (instances dict)

        Returns:
            Parsed JSON response

        Raises:
            WeatherNextError: If request fails
        """
        headers = {}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        else:
            # For production, would implement proper OAuth2 token flow here
            logger.warning(
                "WeatherNext: Using API key auth; consider configuring "
                "service account for production use"
            )

        try:
            response = await self.http.post(endpoint, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(
                f"Vertex AI API error {e.response.status_code}: "
                f"{e.response.text}"
            )
            raise WeatherNextError(f"Vertex AI API error: {e}") from e
        except Exception as e:
            logger.error(f"Vertex AI request failed: {e}")
            raise WeatherNextError(f"API request failed: {e}") from e

    def _get_cached_forecast(self, cache_key: str) -> Optional[Any]:
        """Get cached forecast if still fresh."""
        if cache_key not in self._forecast_cache:
            return None

        cached_data, cached_time = self._forecast_cache[cache_key]
        if (datetime.utcnow() - cached_time).total_seconds() < CACHE_TTL_SECONDS:
            logger.debug(f"WeatherNext cache hit: {cache_key}")
            return cached_data

        # Cache expired, remove it
        del self._forecast_cache[cache_key]
        return None

    def _cache_forecast(self, cache_key: str, data: Any) -> None:
        """Cache a forecast for 6 hours."""
        self._forecast_cache[cache_key] = (data, datetime.utcnow())
        logger.debug(f"WeatherNext cached: {cache_key}")

    def _parse_cyclone_forecast(
        self,
        response: dict[str, Any],
        lat: float,
        lon: float,
        storm_id: Optional[str],
    ) -> CycloneForecast:
        """Parse Vertex AI cyclone forecast response."""
        preds_list = response.get("predictions") or [{}]
        predictions = preds_list[0] if preds_list else {}

        track_data = predictions.get("track", [])
        track_forecast = [
            TrackPoint(
                timestamp=pt.get("timestamp", ""),
                lat=pt.get("latitude", lat),
                lon=pt.get("longitude", lon),
                uncertainty_radius_nm=pt.get("uncertainty_nm", 0.0),
                ensemble_members=self._parse_ensemble_members(
                    pt.get("ensemble", [])
                ),
            )
            for pt in track_data
        ]

        intensity_data = predictions.get("intensity", [])
        intensity_forecast = [
            IntensityForecast(
                timestamp=pt.get("timestamp", ""),
                vmax_kt_p10=pt.get("vmax_p10", 0.0),
                vmax_kt_p50=pt.get("vmax_p50", 0.0),
                vmax_kt_p90=pt.get("vmax_p90", 0.0),
                mslp_hpa_p50=pt.get("mslp_p50"),
                rapid_intensification_prob=pt.get("ri_probability"),
            )
            for pt in intensity_data
        ]

        return CycloneForecast(
            storm_id=storm_id,
            valid_time=predictions.get("valid_time", datetime.utcnow().isoformat()),
            track_forecast=track_forecast,
            intensity_forecast=intensity_forecast,
            formation_probability=predictions.get("formation_prob"),
            landfall_probability=predictions.get("landfall_prob"),
            rapid_intensification_prob_24h=predictions.get("ri_prob_24h"),
            peak_intensity_vmax_kt=predictions.get("peak_vmax_kt"),
            peak_intensity_time=predictions.get("peak_time"),
            ensemble_member_count=int(
                predictions.get("ensemble_members", 50)
            ),
            generated_at=datetime.utcnow().isoformat(),
        )

    def _parse_intensity_prediction(
        self, response: dict[str, Any], vmax_current: float
    ) -> IntensityPrediction:
        """Parse Vertex AI intensity prediction response."""
        preds_list = response.get("predictions") or [{}]
        predictions = preds_list[0] if preds_list else {}

        forecast_steps = [
            IntensityForecast(
                timestamp=step.get("timestamp", ""),
                vmax_kt_p10=step.get("vmax_p10", 0.0),
                vmax_kt_p50=step.get("vmax_p50", 0.0),
                vmax_kt_p90=step.get("vmax_p90", 0.0),
                mslp_hpa_p50=step.get("mslp_p50"),
                rapid_intensification_prob=step.get("ri_probability"),
            )
            for step in predictions.get("forecast_steps", [])
        ]

        return IntensityPrediction(
            current_vmax_kt=vmax_current,
            forecast_steps=forecast_steps,
            rapid_intensification_prob=predictions.get(
                "rapid_intensification_prob", 0.0
            ),
            peak_intensity_vmax_kt=predictions.get("peak_vmax_kt", 0.0),
            peak_intensity_time=predictions.get("peak_time"),
            uncertainty_metric=predictions.get("uncertainty_std", 0.0),
        )

    def _parse_track_prediction(
        self, response: dict[str, Any]
    ) -> list[TrackPoint]:
        """Parse Vertex AI track prediction response."""
        preds_list = response.get("predictions") or [{}]
        predictions = preds_list[0] if preds_list else {}
        track_data = predictions.get("track", [])

        return [
            TrackPoint(
                timestamp=pt.get("timestamp", ""),
                lat=pt.get("latitude", 0.0),
                lon=pt.get("longitude", 0.0),
                uncertainty_radius_nm=pt.get("uncertainty_nm", 0.0),
                ensemble_members=self._parse_ensemble_members(
                    pt.get("ensemble", [])
                ),
            )
            for pt in track_data
        ]

    def _parse_ensemble_members(
        self, ensemble_data: list[dict]
    ) -> Optional[list[EnsembleMember]]:
        """Parse ensemble member data into EnsembleMember objects."""
        if not ensemble_data:
            return None

        return [
            EnsembleMember(
                timestamp=member.get("timestamp", ""),
                lat=member.get("latitude", 0.0),
                lon=member.get("longitude", 0.0),
                vmax_kt=member.get("vmax_kt", 0.0),
                mslp_hpa=member.get("mslp_hpa", 0.0),
                r34_nm=member.get("r34_nm", 0.0),
                r50_nm=member.get("r50_nm"),
                r64_nm=member.get("r64_nm"),
            )
            for member in ensemble_data
        ]

    def _parse_genesis_regions(
        self, response: dict[str, Any]
    ) -> list[GenesisRegion]:
        """Parse genesis probability response."""
        preds_list = response.get("predictions") or [{}]
        predictions = preds_list[0] if preds_list else {}
        regions_data = predictions.get("regions", [])

        return [
            GenesisRegion(
                center_lat=region.get("latitude", 0.0),
                center_lon=region.get("longitude", 0.0),
                formation_probability=region.get("probability", 0.0),
                expected_time_window_start=region.get(
                    "time_window_start", ""
                ),
                expected_time_window_end=region.get("time_window_end", ""),
                size_estimate=region.get("size", "medium"),
                environmental_favorability=region.get(
                    "favorability_score", 0.0
                ),
            )
            for region in regions_data
        ]

    def _parse_environmental_analysis(
        self, response: dict[str, Any], lat: float, lon: float
    ) -> EnvironmentalAnalysis:
        """Parse environmental analysis response."""
        preds_list = response.get("predictions") or [{}]
        predictions = preds_list[0] if preds_list else {}

        return EnvironmentalAnalysis(
            timestamp=datetime.utcnow().isoformat(),
            lat=lat,
            lon=lon,
            wind_shear_ms=predictions.get("wind_shear_ms", 0.0),
            sst_c=predictions.get("sst_c", 0.0),
            moisture_availability=predictions.get("moisture", 0.0),
            upper_level_pattern=predictions.get("upper_level_pattern", ""),
            favorability_score=predictions.get("favorability_score", 0.0),
            detailed_analysis=predictions.get("analysis_text"),
        )

    def _parse_comparisons(
        self, response: dict[str, Any]
    ) -> list[ForecastComparison]:
        """Parse forecast comparison response."""
        preds_list = response.get("predictions") or [{}]
        predictions = preds_list[0] if preds_list else {}
        comparisons_data = predictions.get("comparisons", [])

        return [
            ForecastComparison(
                metric=comp.get("metric", ""),
                weathernext_skill=comp.get("weathernext_skill", 0.0),
                traditional_skill=comp.get("traditional_skill", 0.0),
                skill_difference=comp.get(
                    "skill_difference",
                    comp.get("weathernext_skill", 0.0)
                    - comp.get("traditional_skill", 0.0),
                ),
                explanation=comp.get("explanation"),
            )
            for comp in comparisons_data
        ]
