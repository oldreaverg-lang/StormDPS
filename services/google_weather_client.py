"""
Google Maps Platform Weather API client.

Retrieves real-time and forecast weather data from Google's Weather API
for hurricane tracking applications.

Endpoints:
  - GET /v1/currentConditions:lookup - Current conditions at a location
  - GET /v1/forecast/hours:lookup - Hourly forecasts
  - GET /v1/forecast/days:lookup - Daily forecasts
  - GET /v1/publicAlerts:lookup - Active weather alerts

This service caches responses in-memory with configurable TTL:
  - Current conditions: 15 minutes (default)
  - Forecasts: 1 hour (default)
  - Alerts: 15 minutes (default)
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Optional
from collections import defaultdict

import httpx

logger = logging.getLogger(__name__)


class GoogleWeatherError(Exception):
    """Raised when Google Weather API requests fail."""
    pass


class CacheEntry:
    """Simple cache entry with TTL support."""

    def __init__(self, data: Any, ttl_seconds: int):
        """
        Initialize a cache entry.

        Args:
            data: The data to cache
            ttl_seconds: Time-to-live in seconds
        """
        self.data = data
        self.created_at = datetime.utcnow()
        self.ttl_seconds = ttl_seconds

    def is_expired(self) -> bool:
        """Check if this cache entry has expired."""
        age = (datetime.utcnow() - self.created_at).total_seconds()
        return age > self.ttl_seconds


class GoogleWeatherClient:
    """
    Async client for Google Maps Platform Weather API.

    Provides methods to fetch current conditions, forecasts, and alerts
    for hurricane tracking applications. Includes in-memory caching with
    configurable TTL.

    Usage:
        async with GoogleWeatherClient() as client:
            conditions = await client.get_current_conditions(lat=25.5, lon=-80.2)
            hourly = await client.get_hourly_forecast(lat=25.5, lon=-80.2)
            alerts = await client.get_weather_alerts(lat=25.5, lon=-80.2)

    Class-level caches survive across requests within the same server process.
    """

    BASE_URL = "https://weather.googleapis.com"

    def __init__(
        self,
        timeout: float = 15.0,
        cache_ttl_conditions: int = 900,  # 15 minutes
        cache_ttl_forecasts: int = 3600,  # 1 hour
        cache_ttl_alerts: int = 900,  # 15 minutes
    ):
        """
        Initialize GoogleWeatherClient.

        Args:
            timeout: Request timeout in seconds (default: 15.0)
            cache_ttl_conditions: TTL for current conditions cache in seconds (default: 900)
            cache_ttl_forecasts: TTL for forecast cache in seconds (default: 3600)
            cache_ttl_alerts: TTL for alerts cache in seconds (default: 900)
        """
        self.timeout = timeout
        self.cache_ttl_conditions = cache_ttl_conditions
        self.cache_ttl_forecasts = cache_ttl_forecasts
        self.cache_ttl_alerts = cache_ttl_alerts
        self._http_client: Optional[httpx.AsyncClient] = None

        # Class-level caches shared across all instances
        if not hasattr(GoogleWeatherClient, "_cache"):
            GoogleWeatherClient._cache: dict[str, CacheEntry] = {}

    async def __aenter__(self):
        """Enter async context manager."""
        self._http_client = httpx.AsyncClient(
            timeout=self.timeout,
            headers={
                "User-Agent": "HurricaneIKE-App/1.0 (weather-client)",
            },
        )
        return self

    async def __aexit__(self, *args):
        """Exit async context manager."""
        if self._http_client:
            await self._http_client.aclose()

    @property
    def http(self) -> httpx.AsyncClient:
        """Get the HTTP client, raising if not in async context."""
        if self._http_client is None:
            raise RuntimeError("GoogleWeatherClient must be used as async context manager")
        return self._http_client

    @classmethod
    def is_configured(cls) -> bool:
        """
        Check if Google Maps API key is configured.

        Returns:
            True if GOOGLE_MAPS_API_KEY environment variable is set, False otherwise.
        """
        api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
        return bool(api_key)

    @classmethod
    def _get_api_key(cls) -> str:
        """
        Get API key from environment, raising if not configured.

        Returns:
            The API key string

        Raises:
            GoogleWeatherError: If GOOGLE_MAPS_API_KEY is not set or empty
        """
        api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
        if not api_key:
            raise GoogleWeatherError(
                "GOOGLE_MAPS_API_KEY environment variable is not set. "
                "Please configure your API key to use GoogleWeatherClient."
            )
        return api_key

    def _get_cache_key(self, method: str, lat: float, lon: float, **kwargs) -> str:
        """
        Generate a cache key for a request.

        Args:
            method: Method name (e.g., 'current_conditions')
            lat: Latitude
            lon: Longitude
            **kwargs: Additional parameters to include in cache key

        Returns:
            Cache key string
        """
        key_parts = [method, f"{lat:.4f}", f"{lon:.4f}"]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")
        return "|".join(key_parts)

    def _get_cached(self, key: str) -> Optional[Any]:
        """
        Retrieve data from cache if not expired.

        Args:
            key: Cache key

        Returns:
            Cached data if valid, None if expired or not found
        """
        if key in self._cache:
            entry = self._cache[key]
            if not entry.is_expired():
                return entry.data
            else:
                del self._cache[key]
        return None

    def _set_cached(self, key: str, data: Any, ttl_seconds: int):
        """
        Store data in cache.

        Args:
            key: Cache key
            data: Data to cache
            ttl_seconds: Time-to-live in seconds
        """
        self._cache[key] = CacheEntry(data, ttl_seconds)

    async def _make_request(
        self, endpoint: str, params: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        """
        Make an authenticated request to the Google Weather API.

        Args:
            endpoint: API endpoint path (e.g., '/v1/currentConditions:lookup')
            params: Query parameters

        Returns:
            JSON response as dict

        Raises:
            GoogleWeatherError: If request fails
        """
        try:
            api_key = self._get_api_key()
        except GoogleWeatherError:
            raise

        if params is None:
            params = {}

        # Add API key to params
        params["key"] = api_key

        url = f"{self.BASE_URL}{endpoint}"

        try:
            response = await self.http.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            raise GoogleWeatherError(
                f"Google Weather API HTTP error {e.response.status_code} "
                f"for {endpoint}: {e.response.text}"
            )
        except httpx.RequestError as e:
            raise GoogleWeatherError(f"Google Weather API request failed for {endpoint}: {e}")
        except Exception as e:
            raise GoogleWeatherError(f"Failed to parse response from {endpoint}: {e}")

    async def get_current_conditions(
        self, lat: float, lon: float, use_cache: bool = True
    ) -> dict[str, Any]:
        """
        Fetch current weather conditions at the specified location.

        Args:
            lat: Latitude (-90 to 90)
            lon: Longitude (-180 to 180)
            use_cache: Use cached response if available (default: True)

        Returns:
            dict with keys:
                - temperature: Current temperature in Celsius
                - humidity: Relative humidity (0-100%)
                - pressure: Atmospheric pressure in hPa
                - wind_speed: Wind speed in m/s
                - wind_direction: Wind direction in degrees (0-360)
                - precipitation: Precipitation in mm
                - uv_index: UV index (0+)
                - visibility: Visibility in km
                - cloud_cover: Cloud cover percentage (0-100%)
                - condition: Weather condition description
                - timestamp: UTC timestamp of observation

        Raises:
            GoogleWeatherError: If API request fails or conditions are missing
        """
        if not self.is_configured():
            raise GoogleWeatherError("Google Weather API is not configured")

        cache_key = self._get_cache_key("current_conditions", lat, lon)
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        try:
            response = await self._make_request(
                "/v1/currentConditions:lookup",
                params={"location.latitude": lat, "location.longitude": lon},
            )

            # Extract the current condition from response
            result = response.get("currentConditions", {})

            # Normalize field names and ensure all expected fields are present
            normalized = {
                "temperature": result.get("temperature"),
                "humidity": result.get("humidity"),
                "pressure": result.get("pressure"),
                "wind_speed": result.get("windSpeed"),
                "wind_direction": result.get("windDirection"),
                "precipitation": result.get("precipitation"),
                "uv_index": result.get("uvIndex"),
                "visibility": result.get("visibility"),
                "cloud_cover": result.get("cloudCover"),
                "condition": result.get("condition"),
                "timestamp": result.get("observationTime"),
            }

            self._set_cached(cache_key, normalized, self.cache_ttl_conditions)
            logger.info(f"Fetched current conditions for ({lat}, {lon})")
            return normalized

        except GoogleWeatherError:
            raise
        except Exception as e:
            raise GoogleWeatherError(f"Error parsing current conditions response: {e}")

    async def get_hourly_forecast(
        self,
        lat: float,
        lon: float,
        hours: int = 240,
        use_cache: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Fetch hourly forecast for the specified location.

        Args:
            lat: Latitude (-90 to 90)
            lon: Longitude (-180 to 180)
            hours: Number of hours to forecast (max 240, default 240)
            use_cache: Use cached response if available (default: True)

        Returns:
            List of hourly forecast dicts, each with:
                - time: ISO 8601 timestamp
                - wind_speed: Wind speed in m/s
                - wind_direction: Wind direction in degrees (0-360)
                - pressure: Atmospheric pressure in hPa
                - precipitation_probability: Probability 0-1
                - precipitation: Precipitation in mm
                - temperature: Temperature in Celsius
                - humidity: Relative humidity (0-100%)
                - condition: Weather condition description

        Raises:
            GoogleWeatherError: If API request fails
        """
        if not self.is_configured():
            raise GoogleWeatherError("Google Weather API is not configured")

        # Clamp hours to max 240
        hours = min(hours, 240)

        cache_key = self._get_cache_key("hourly_forecast", lat, lon, hours=hours)
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        try:
            response = await self._make_request(
                "/v1/forecast/hours:lookup",
                params={
                    "location.latitude": lat,
                    "location.longitude": lon,
                    "forecastHours": hours,
                },
            )

            forecasts = response.get("hourly", [])
            normalized = []

            for forecast in forecasts:
                normalized.append(
                    {
                        "time": forecast.get("time"),
                        "wind_speed": forecast.get("windSpeed"),
                        "wind_direction": forecast.get("windDirection"),
                        "pressure": forecast.get("pressure"),
                        "precipitation_probability": forecast.get("precipitationProbability"),
                        "precipitation": forecast.get("precipitation"),
                        "temperature": forecast.get("temperature"),
                        "humidity": forecast.get("humidity"),
                        "condition": forecast.get("condition"),
                    }
                )

            self._set_cached(cache_key, normalized, self.cache_ttl_forecasts)
            logger.info(f"Fetched {len(normalized)} hourly forecasts for ({lat}, {lon})")
            return normalized

        except GoogleWeatherError:
            raise
        except Exception as e:
            raise GoogleWeatherError(f"Error parsing hourly forecast response: {e}")

    async def get_daily_forecast(
        self,
        lat: float,
        lon: float,
        days: int = 10,
        use_cache: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Fetch daily forecast for the specified location.

        Args:
            lat: Latitude (-90 to 90)
            lon: Longitude (-180 to 180)
            days: Number of days to forecast (max 10, default 10)
            use_cache: Use cached response if available (default: True)

        Returns:
            List of daily forecast dicts, each with:
                - date: ISO 8601 date string (YYYY-MM-DD)
                - high_temperature: Daytime high in Celsius
                - low_temperature: Nighttime low in Celsius
                - humidity: Average humidity (0-100%)
                - wind_speed: Average wind speed in m/s
                - wind_direction: Prevailing wind direction (0-360)
                - precipitation_probability: Probability 0-1
                - precipitation: Expected precipitation in mm
                - condition: Weather condition description
                - sunrise: Time of sunrise
                - sunset: Time of sunset

        Raises:
            GoogleWeatherError: If API request fails
        """
        if not self.is_configured():
            raise GoogleWeatherError("Google Weather API is not configured")

        # Clamp days to max 10
        days = min(days, 10)

        cache_key = self._get_cache_key("daily_forecast", lat, lon, days=days)
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        try:
            response = await self._make_request(
                "/v1/forecast/days:lookup",
                params={
                    "location.latitude": lat,
                    "location.longitude": lon,
                    "forecastDays": days,
                },
            )

            forecasts = response.get("daily", [])
            normalized = []

            for forecast in forecasts:
                normalized.append(
                    {
                        "date": forecast.get("date"),
                        "high_temperature": forecast.get("maxTemperature"),
                        "low_temperature": forecast.get("minTemperature"),
                        "humidity": forecast.get("humidity"),
                        "wind_speed": forecast.get("windSpeed"),
                        "wind_direction": forecast.get("windDirection"),
                        "precipitation_probability": forecast.get("precipitationProbability"),
                        "precipitation": forecast.get("precipitation"),
                        "condition": forecast.get("condition"),
                        "sunrise": forecast.get("sunrise"),
                        "sunset": forecast.get("sunset"),
                    }
                )

            self._set_cached(cache_key, normalized, self.cache_ttl_forecasts)
            logger.info(f"Fetched {len(normalized)} daily forecasts for ({lat}, {lon})")
            return normalized

        except GoogleWeatherError:
            raise
        except Exception as e:
            raise GoogleWeatherError(f"Error parsing daily forecast response: {e}")

    async def get_weather_alerts(
        self,
        lat: float,
        lon: float,
        use_cache: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Fetch active weather alerts for the specified location.

        Filters for hurricane, tropical storm, storm surge, and flood alerts.

        Args:
            lat: Latitude (-90 to 90)
            lon: Longitude (-180 to 180)
            use_cache: Use cached response if available (default: True)

        Returns:
            List of alert dicts, each with:
                - type: Alert type (HURRICANE, TROPICAL_STORM, STORM_SURGE, FLOOD, etc.)
                - severity: Alert severity (EXTREME, SEVERE, MODERATE, MINOR)
                - urgency: Urgency level (IMMEDIATE, EXPECTED, FUTURE, PAST, UNKNOWN)
                - certainty: Certainty of alert (OBSERVED, LIKELY, POSSIBLE, UNLIKELY, UNKNOWN)
                - headline: Short alert headline
                - description: Detailed alert description
                - effective_time: When alert becomes effective (ISO 8601)
                - expires_time: When alert expires (ISO 8601)
                - instruction: Recommended actions
                - areas_affected: Geographic areas affected

        Raises:
            GoogleWeatherError: If API request fails
        """
        if not self.is_configured():
            raise GoogleWeatherError("Google Weather API is not configured")

        cache_key = self._get_cache_key("weather_alerts", lat, lon)
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        try:
            response = await self._make_request(
                "/v1/publicAlerts:lookup",
                params={
                    "location.latitude": lat,
                    "location.longitude": lon,
                },
            )

            raw_alerts = response.get("publicAlerts", [])

            # Filter for hurricane-related alerts
            hurricane_alert_types = {
                "HURRICANE",
                "TROPICAL_STORM",
                "STORM_SURGE",
                "FLOOD",
            }

            normalized = []
            for alert in raw_alerts:
                alert_type = alert.get("type", "").upper()

                # Only include hurricane-related alerts
                if alert_type not in hurricane_alert_types:
                    continue

                normalized.append(
                    {
                        "type": alert_type,
                        "severity": alert.get("severity", "UNKNOWN").upper(),
                        "urgency": alert.get("urgency", "UNKNOWN").upper(),
                        "certainty": alert.get("certainty", "UNKNOWN").upper(),
                        "headline": alert.get("headline"),
                        "description": alert.get("description"),
                        "effective_time": alert.get("effectiveTime"),
                        "expires_time": alert.get("expiresTime"),
                        "instruction": alert.get("instruction"),
                        "areas_affected": alert.get("areas", []),
                    }
                )

            self._set_cached(cache_key, normalized, self.cache_ttl_alerts)
            logger.info(
                f"Fetched {len(normalized)} hurricane-related alerts for ({lat}, {lon})"
            )
            return normalized

        except GoogleWeatherError:
            raise
        except Exception as e:
            raise GoogleWeatherError(f"Error parsing weather alerts response: {e}")

    async def get_sea_surface_temperature(
        self,
        lat: float,
        lon: float,
        use_cache: bool = True,
    ) -> Optional[float]:
        """
        Fetch sea surface temperature (SST) for the specified location.

        Attempts to extract SST from marine/ocean conditions data.
        Returns None if not available or data is invalid.

        Args:
            lat: Latitude (-90 to 90)
            lon: Longitude (-180 to 180)
            use_cache: Use cached response if available (default: True)

        Returns:
            Sea surface temperature in Celsius, or None if not available

        Raises:
            GoogleWeatherError: If API request fails
        """
        if not self.is_configured():
            logger.debug("Google Weather API not configured; returning None for SST")
            return None

        cache_key = self._get_cache_key("sea_surface_temperature", lat, lon)
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        try:
            # Attempt to fetch current conditions which may include SST
            response = await self._make_request(
                "/v1/currentConditions:lookup",
                params={
                    "location.latitude": lat,
                    "location.longitude": lon,
                },
            )

            conditions = response.get("currentConditions", {})

            # Try to extract SST from marine/water-related fields.
            # Google Weather API returns temperatures as nested objects:
            #   {"temperature": {"value": 28.5, "units": "CELSIUS"}}
            # or sometimes as bare floats. Handle both formats.
            sst = None

            for field_name in ("waterTemperature", "seaTemperature", "oceanTemperature"):
                raw = conditions.get(field_name)
                if raw is None:
                    continue
                # Handle nested object format: {"value": N, "units": "CELSIUS"}
                if isinstance(raw, dict):
                    sst = raw.get("value") or raw.get("celsius") or raw.get("degrees")
                else:
                    sst = raw
                if sst is not None:
                    break

            # Validate SST is in reasonable range for ocean water (-2 to 35°C)
            if sst is not None:
                try:
                    sst = float(sst)
                    if -2 <= sst <= 35:
                        self._set_cached(cache_key, sst, self.cache_ttl_conditions)
                        logger.debug(f"Fetched SST {sst}°C for ({lat}, {lon})")
                        return sst
                except (ValueError, TypeError):
                    pass

            # SST not available
            self._set_cached(cache_key, None, self.cache_ttl_conditions)
            logger.debug(f"SST not available for ({lat}, {lon})")
            return None

        except GoogleWeatherError as e:
            logger.debug(f"Failed to fetch SST: {e}")
            return None
        except Exception as e:
            logger.debug(f"Error parsing SST data: {e}")
            return None

    @classmethod
    def clear_cache(cls):
        """Clear all cached data (useful for testing or manual refresh)."""
        if hasattr(cls, "_cache"):
            cls._cache.clear()
            logger.info("Cleared Google Weather API cache")

    @classmethod
    def get_cache_stats(cls) -> dict[str, int]:
        """
        Get cache statistics.

        Returns:
            dict with:
                - total_entries: Total number of cache entries
                - expired_entries: Number of expired (but not cleaned) entries
        """
        if not hasattr(cls, "_cache"):
            return {"total_entries": 0, "expired_entries": 0}

        total = len(cls._cache)
        expired = sum(1 for entry in cls._cache.values() if entry.is_expired())
        return {"total_entries": total, "expired_entries": expired}
