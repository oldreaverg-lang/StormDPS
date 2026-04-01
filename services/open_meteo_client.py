"""
Open-Meteo API client for weather and marine data enrichment.

Open-Meteo provides FREE weather, marine, and flood forecasts with NO API key required.
Useful for hurricane intensity forecasting: soil moisture (antecedent conditions),
sea surface temperature (intensity driver), precipitation history, and wind shear.

Base URLs:
  - Weather: https://api.open-meteo.com/v1/forecast
  - Historical: https://archive-api.open-meteo.com/v1/archive
  - Marine: https://marine-api.open-meteo.com/v1/marine
  - Flood: https://flood-api.open-meteo.com/v1/flood

Usage:
    async with OpenMeteoClient() as client:
        sst = await client.get_sea_surface_temperature(25.5, -80.2)
        moisture = await client.get_soil_moisture(25.5, -80.2)
        wind_profile = await client.get_wind_profile(25.5, -80.2)
        shear = await client.estimate_wind_shear(25.5, -80.2)
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from functools import wraps
import httpx
import os

logger = logging.getLogger(__name__)

# Open-Meteo API endpoints
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_MARINE_URL = "https://marine-api.open-meteo.com/v1/marine"
OPEN_METEO_FLOOD_URL = "https://flood-api.open-meteo.com/v1/flood"

# Rate limits: Open-Meteo free tier allows 600 requests per minute
MAX_REQUESTS_PER_MINUTE = 600


class OpenMeteoError(Exception):
    """Raised when Open-Meteo API request fails."""
    pass


def cache_result(ttl_seconds: int, max_entries: int = 256):
    """
    TTL-based cache decorator for async methods with LRU eviction.

    Args:
        ttl_seconds: Cache time-to-live in seconds.
        max_entries: Maximum cached entries before least-recently-used
                     items are evicted. Prevents unbounded memory growth
                     during a long-running hurricane season.
    """
    def decorator(func):
        cache: dict[str, Any] = {}
        cache_times: dict[str, datetime] = {}
        _lock = asyncio.Lock()

        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            cache_key = f"{func.__name__}:{args}:{sorted(kwargs.items())}"

            async with _lock:
                # Check if cached result is still valid
                if cache_key in cache_times:
                    age = (datetime.utcnow() - cache_times[cache_key]).total_seconds()
                    if age < ttl_seconds:
                        logger.debug(f"Cache hit for {func.__name__} (age: {age:.0f}s)")
                        return cache[cache_key]
                    else:
                        # Expired — remove stale entry
                        del cache[cache_key]
                        del cache_times[cache_key]

            # Call the function outside the lock to avoid blocking
            result = await func(self, *args, **kwargs)

            async with _lock:
                # Evict oldest entries if at capacity
                if len(cache) >= max_entries:
                    # Remove the oldest 25% to amortize eviction cost
                    evict_count = max(1, max_entries // 4)
                    oldest_keys = sorted(cache_times, key=cache_times.get)[:evict_count]
                    for k in oldest_keys:
                        cache.pop(k, None)
                        cache_times.pop(k, None)
                    logger.debug(
                        f"Cache eviction for {func.__name__}: removed {len(oldest_keys)} "
                        f"entries ({len(cache)} remaining)"
                    )

                cache[cache_key] = result
                cache_times[cache_key] = datetime.utcnow()
                logger.debug(f"Cached {func.__name__} result (TTL: {ttl_seconds}s, entries: {len(cache)})")

            return result

        # Expose cache stats for monitoring
        wrapper.cache_info = lambda: {
            "entries": len(cache),
            "max_entries": max_entries,
            "ttl_seconds": ttl_seconds,
        }
        wrapper.cache_clear = lambda: (cache.clear(), cache_times.clear())

        return wrapper
    return decorator


class OpenMeteoClient:
    """
    Async client for Open-Meteo weather, marine, and flood APIs.

    Open-Meteo provides free forecasts and historical data with no API key.
    Useful for hurricane intensity forecasting through:
    - Soil moisture (antecedent conditions / Formula Shortcoming #14)
    - Sea surface temperature (SST > 26.5°C fuels intensification)
    - Precipitation history (antecedent flooding risk)
    - Marine conditions (wave height, swell during approach)
    - Wind profile and vertical wind shear (high shear inhibits intensification)
    - Flood forecasts (river discharge for inland flooding)

    Usage:
        async with OpenMeteoClient(timeout=30.0) as client:
            sst = await client.get_sea_surface_temperature(25.5, -80.2)
            shear = await client.estimate_wind_shear(25.5, -80.2)
    """

    # Retry configuration (Open-Meteo is free and can be flaky)
    RETRY_ATTEMPTS = 2
    RETRY_BACKOFF_BASE = 1.0  # 1s, 2s backoff

    # HTTP status codes that warrant retry
    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(self, timeout: float = 30.0):
        """
        Initialize Open-Meteo client.

        Args:
            timeout: HTTP request timeout in seconds (default: 30.0)
        """
        self.timeout = timeout
        self._http_client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        """Async context manager entry: create httpx client."""
        self._http_client = httpx.AsyncClient(
            timeout=self.timeout,
            headers={"User-Agent": "HurricaneIKE-App/1.0 (research)"},
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args):
        """Async context manager exit: close httpx client."""
        if self._http_client:
            await self._http_client.aclose()

    @property
    def http(self) -> httpx.AsyncClient:
        """Get or raise error if client not initialized."""
        if self._http_client is None:
            raise RuntimeError("OpenMeteoClient must be used as async context manager")
        return self._http_client

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> httpx.Response:
        """
        Make HTTP request with exponential backoff retry logic.

        Open-Meteo is a free API that can be flaky. Retries on connection errors,
        timeouts, or specific status codes (429, 5xx).

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL to request
            **kwargs: Additional arguments for httpx.request()

        Returns:
            Response object on success

        Raises:
            OpenMeteoError: If all retry attempts fail
        """
        last_error: Optional[Exception] = None

        for attempt in range(self.RETRY_ATTEMPTS):
            try:
                resp = await self.http.request(method, url, **kwargs)

                # Immediate return on success or non-retryable client error (4xx)
                if resp.status_code < 400 or (400 <= resp.status_code < 429) or (429 < resp.status_code < 500):
                    return resp

                # Handle 429 (rate limit)
                if resp.status_code == 429 and attempt < self.RETRY_ATTEMPTS - 1:
                    retry_after = resp.headers.get("Retry-After", "10")
                    try:
                        backoff = float(retry_after)
                    except (ValueError, TypeError):
                        backoff = 10.0
                    logger.warning(
                        f"Open-Meteo rate limited (429) for {url}; "
                        f"retrying in {backoff:.1f}s (attempt {attempt + 1}/{self.RETRY_ATTEMPTS})"
                    )
                    await asyncio.sleep(backoff)
                    continue

                # Retry on 5xx server errors
                if resp.status_code >= 500 and attempt < self.RETRY_ATTEMPTS - 1:
                    backoff = self.RETRY_BACKOFF_BASE ** attempt
                    logger.warning(
                        f"Open-Meteo API returned {resp.status_code} for {url}; "
                        f"retrying in {backoff:.1f}s (attempt {attempt + 1}/{self.RETRY_ATTEMPTS})"
                    )
                    await asyncio.sleep(backoff)
                    continue

                # Return non-retryable response
                return resp

            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_error = e
                if attempt < self.RETRY_ATTEMPTS - 1:
                    backoff = self.RETRY_BACKOFF_BASE ** attempt
                    logger.warning(
                        f"Open-Meteo API connection error: {type(e).__name__}; "
                        f"retrying in {backoff:.1f}s (attempt {attempt + 1}/{self.RETRY_ATTEMPTS})"
                    )
                    await asyncio.sleep(backoff)
                    continue
                # Fall through to raise error below

        # All retries exhausted
        if last_error:
            raise OpenMeteoError(f"Open-Meteo API request failed after {self.RETRY_ATTEMPTS} attempts: {last_error}") from last_error
        raise OpenMeteoError(f"Open-Meteo API request to {url} failed after {self.RETRY_ATTEMPTS} attempts")

    # ====================================================================
    # SOIL MOISTURE: Antecedent conditions (Formula Shortcoming #14)
    # ====================================================================

    @staticmethod
    def normalize_soil_moisture(raw_values: List[float]) -> float:
        """
        Normalize soil moisture values to 0-1 saturation scale.

        Open-Meteo returns soil moisture in m³/m³ (volume fraction).
        Typical ranges: 0.0 (dry) to ~0.4 (saturated, varies by soil type).

        Args:
            raw_values: List of soil moisture measurements (m³/m³)

        Returns:
            Saturation estimate on 0-1 scale (0=dry, 1=saturated)
        """
        if not raw_values:
            return 0.0

        # Remove None values
        values = [v for v in raw_values if v is not None]
        if not values:
            return 0.0

        # Average the values
        avg_moisture = sum(values) / len(values)

        # Normalize to 0-1 scale
        # Typical saturation point ~0.4 m³/m³ for most soils
        # Wilting point ~0.1-0.15 m³/m³
        saturation_max = 0.4
        wilting_point = 0.1

        normalized = (avg_moisture - wilting_point) / (saturation_max - wilting_point)
        return max(0.0, min(1.0, normalized))  # Clamp to [0, 1]

    @cache_result(ttl_seconds=6 * 3600)  # 6-hour cache for historical data
    async def get_soil_moisture(
        self, lat: float, lon: float, days_back: int = 7
    ) -> Dict[str, Any]:
        """
        Fetch soil moisture data for antecedent condition assessment.

        Soil moisture is critical for runoff generation and flooding risk.
        Dry soils (low saturation) → high infiltration → low runoff.
        Wet soils (high saturation) → low infiltration → high runoff.

        Args:
            lat: Latitude (degrees)
            lon: Longitude (degrees)
            days_back: Number of days of historical data to retrieve (default: 7)

        Returns:
            Dict with keys:
                - current: Current soil moisture saturation (0-1)
                - avg_7day: 7-day average saturation (0-1)
                - saturation: Overall saturation estimate (0-1)
                - layers: Dict with saturation for each soil layer
                - raw_data: Raw hourly values for 0-7cm, 7-28cm, 28-100cm layers
            Returns empty dict on API failure.
        """
        try:
            # Calculate date range
            end_date = datetime.utcnow().date()
            start_date = end_date - timedelta(days=days_back)

            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "hourly": "soil_moisture_0_to_7cm,soil_moisture_7_to_28cm,soil_moisture_28_to_100cm",
                "timezone": "UTC",
            }

            response = await self._request_with_retry(
                "GET", OPEN_METEO_HISTORICAL_URL, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            hourly_data = data.get("hourly", {})
            times = hourly_data.get("time", [])
            moisture_0_7 = hourly_data.get("soil_moisture_0_to_7cm", [])
            moisture_7_28 = hourly_data.get("soil_moisture_7_to_28cm", [])
            moisture_28_100 = hourly_data.get("soil_moisture_28_to_100cm", [])

            if not times:
                logger.warning(f"No soil moisture data for ({lat}, {lon})")
                return {}

            # Compute saturation for each layer
            sat_0_7 = self.normalize_soil_moisture(moisture_0_7)
            sat_7_28 = self.normalize_soil_moisture(moisture_7_28)
            sat_28_100 = self.normalize_soil_moisture(moisture_28_100)

            # Average across all layers
            all_values = moisture_0_7 + moisture_7_28 + moisture_28_100
            avg_saturation = self.normalize_soil_moisture(all_values)

            # Current values (last available reading)
            current_0_7 = moisture_0_7[-1] if moisture_0_7 else None
            current_7_28 = moisture_7_28[-1] if moisture_7_28 else None
            current_28_100 = moisture_28_100[-1] if moisture_28_100 else None
            current_saturation = self.normalize_soil_moisture(
                [current_0_7, current_7_28, current_28_100]
            )

            return {
                "current": current_saturation,
                "avg_7day": avg_saturation,
                "saturation": avg_saturation,
                "layers": {
                    "0_to_7cm": sat_0_7,
                    "7_to_28cm": sat_7_28,
                    "28_to_100cm": sat_28_100,
                },
                "raw_data": {
                    "times": times,
                    "soil_moisture_0_to_7cm": moisture_0_7,
                    "soil_moisture_7_to_28cm": moisture_7_28,
                    "soil_moisture_28_to_100cm": moisture_28_100,
                },
            }

        except Exception as e:
            logger.error(f"Error fetching soil moisture for ({lat}, {lon}): {e}")
            return {}

    # ====================================================================
    # SEA SURFACE TEMPERATURE: Critical intensity driver
    # ====================================================================

    @cache_result(ttl_seconds=30 * 60)  # 30-minute cache for forecasts
    async def get_sea_surface_temperature(self, lat: float, lon: float) -> Optional[float]:
        """
        Fetch current sea surface temperature (SST).

        SST is a primary driver of hurricane intensification.
        SST > 26.5°C (80°F) supports continued intensification.
        SST < 26.5°C typically leads to weakening.

        Args:
            lat: Latitude (degrees)
            lon: Longitude (degrees)

        Returns:
            Sea surface temperature in Celsius, or None on failure.
        """
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": "sea_surface_temperature",
                "timezone": "UTC",
            }

            response = await self._request_with_retry(
                "GET", OPEN_METEO_MARINE_URL, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            hourly_data = data.get("hourly", {})
            sst_values = hourly_data.get("sea_surface_temperature", [])

            if sst_values:
                # Return the first valid (most recent) value
                for sst in sst_values:
                    if sst is not None:
                        return float(sst)

            logger.warning(f"No SST data available for ({lat}, {lon})")
            return None

        except Exception as e:
            logger.error(f"Error fetching SST for ({lat}, {lon}): {e}")
            return None

    # ====================================================================
    # PRECIPITATION HISTORY: Antecedent flooding risk
    # ====================================================================

    @cache_result(ttl_seconds=6 * 3600)  # 6-hour cache for historical data
    async def get_precipitation_history(
        self, lat: float, lon: float, days_back: int = 30
    ) -> Dict[str, Any]:
        """
        Fetch historical precipitation to assess antecedent flooding risk.

        Cumulative precipitation over multiple time windows helps assess
        soil saturation and basin wetness (antecedent moisture conditions).
        High antecedent precipitation → higher flooding risk from hurricane rainfall.

        Args:
            lat: Latitude (degrees)
            lon: Longitude (degrees)
            days_back: Number of days of historical data (default: 30)

        Returns:
            Dict with keys:
                - cumulative_7day: 7-day total precipitation (mm)
                - cumulative_14day: 14-day total precipitation (mm)
                - cumulative_30day: 30-day total precipitation (mm)
                - daily_data: List of daily values with dates
            Returns empty dict on API failure.
        """
        try:
            end_date = datetime.utcnow().date()
            start_date = end_date - timedelta(days=days_back)

            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "daily": "precipitation_sum,rain_sum",
                "timezone": "UTC",
            }

            response = await self._request_with_retry(
                "GET", OPEN_METEO_HISTORICAL_URL, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            daily_data_raw = data.get("daily", {})
            times = daily_data_raw.get("time", [])
            precip_sum = daily_data_raw.get("precipitation_sum", [])
            rain_sum = daily_data_raw.get("rain_sum", [])

            if not times or not precip_sum:
                logger.warning(f"No precipitation data for ({lat}, {lon})")
                return {}

            # Use precipitation_sum (total) if available
            values = [p if p is not None else 0.0 for p in precip_sum]

            # Calculate cumulative totals
            cum_7day = sum(values[-7:]) if len(values) >= 7 else sum(values)
            cum_14day = sum(values[-14:]) if len(values) >= 14 else sum(values)
            cum_30day = sum(values)  # Already limited to days_back=30

            daily_list = [
                {"date": times[i], "precipitation_mm": values[i]}
                for i in range(len(times))
            ]

            return {
                "cumulative_7day": cum_7day,
                "cumulative_14day": cum_14day,
                "cumulative_30day": cum_30day,
                "daily_data": daily_list,
            }

        except Exception as e:
            logger.error(f"Error fetching precipitation for ({lat}, {lon}): {e}")
            return {}

    # ====================================================================
    # MARINE CONDITIONS: Wave height, swell during approach
    # ====================================================================

    @cache_result(ttl_seconds=30 * 60)  # 30-minute cache for forecasts
    async def get_marine_conditions(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Fetch marine/ocean conditions for coastal hurricane approach.

        Wave height, swell, and ocean currents indicate the ocean state
        and can contribute to surge estimates.

        Args:
            lat: Latitude (degrees)
            lon: Longitude (degrees)

        Returns:
            Dict with keys:
                - wave_height_m: Current wave height (m)
                - wave_direction_deg: Wave direction (0-360)
                - wave_period_s: Wave period (seconds)
                - swell_height_m: Swell height (m)
                - ocean_current_velocity_ms: Ocean current speed (m/s)
                - ocean_current_direction_deg: Ocean current direction (0-360)
                - hourly_data: Hourly forecast data
            Returns empty dict on API failure.
        """
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": (
                    "wave_height,wave_direction,wave_period,"
                    "ocean_current_velocity,ocean_current_direction,swell_wave_height"
                ),
                "timezone": "UTC",
            }

            response = await self._request_with_retry(
                "GET", OPEN_METEO_MARINE_URL, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            hourly_data = data.get("hourly", {})
            times = hourly_data.get("time", [])
            wave_height = hourly_data.get("wave_height", [])
            wave_direction = hourly_data.get("wave_direction", [])
            wave_period = hourly_data.get("wave_period", [])
            swell_height = hourly_data.get("swell_wave_height", [])
            ocean_velocity = hourly_data.get("ocean_current_velocity", [])
            ocean_direction = hourly_data.get("ocean_current_direction", [])

            # Extract current (first valid) values
            current_wave_height = next((v for v in wave_height if v is not None), None)
            current_wave_dir = next((v for v in wave_direction if v is not None), None)
            current_wave_period = next((v for v in wave_period if v is not None), None)
            current_swell = next((v for v in swell_height if v is not None), None)
            current_ocean_vel = next((v for v in ocean_velocity if v is not None), None)
            current_ocean_dir = next((v for v in ocean_direction if v is not None), None)

            return {
                "wave_height_m": current_wave_height,
                "wave_direction_deg": current_wave_dir,
                "wave_period_s": current_wave_period,
                "swell_height_m": current_swell,
                "ocean_current_velocity_ms": current_ocean_vel,
                "ocean_current_direction_deg": current_ocean_dir,
                "hourly_data": {
                    "times": times,
                    "wave_height": wave_height,
                    "wave_direction": wave_direction,
                    "wave_period": wave_period,
                    "swell_height": swell_height,
                    "ocean_current_velocity": ocean_velocity,
                    "ocean_current_direction": ocean_direction,
                },
            }

        except Exception as e:
            logger.error(f"Error fetching marine conditions for ({lat}, {lon}): {e}")
            return {}

    # ====================================================================
    # CURRENT WEATHER: Real-time atmospheric conditions
    # ====================================================================

    @cache_result(ttl_seconds=30 * 60)  # 30-minute cache for forecasts
    async def get_current_weather(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Fetch current weather conditions.

        Args:
            lat: Latitude (degrees)
            lon: Longitude (degrees)

        Returns:
            Dict with keys:
                - temperature_c: Air temperature (°C)
                - wind_speed_ms: Wind speed (m/s)
                - wind_direction_deg: Wind direction (0-360)
                - wind_gusts_ms: Wind gust speed (m/s)
                - precipitation_mm: Current precipitation (mm)
                - weather_code: WMO weather code
                - pressure_hpa: Mean sea level pressure (hPa)
                - humidity_percent: Relative humidity (%)
                - cloud_cover_percent: Cloud cover (%)
                - visibility_m: Visibility (m)
            Returns empty dict on API failure.
        """
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "current_weather": True,
                "hourly": (
                    "pressure_msl,relative_humidity_2m,cloud_cover,"
                    "visibility,weather_code,wind_gusts_10m"
                ),
                "timezone": "UTC",
            }

            response = await self._request_with_retry(
                "GET", OPEN_METEO_FORECAST_URL, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            current = data.get("current_weather", {})
            hourly = data.get("hourly", {})

            # Extract hourly values (first entry is most recent)
            pressures = hourly.get("pressure_msl", [])
            humidities = hourly.get("relative_humidity_2m", [])
            cloud_covers = hourly.get("cloud_cover", [])
            visibilities = hourly.get("visibility", [])
            wind_gusts = hourly.get("wind_gusts_10m", [])

            current_pressure = pressures[0] if pressures else None
            current_humidity = humidities[0] if humidities else None
            current_cloud_cover = cloud_covers[0] if cloud_covers else None
            current_visibility = visibilities[0] if visibilities else None
            current_gust = wind_gusts[0] if wind_gusts else None

            return {
                "temperature_c": current.get("temperature"),
                "wind_speed_ms": current.get("windspeed"),
                "wind_direction_deg": current.get("winddirection"),
                "wind_gusts_ms": current_gust,
                "precipitation_mm": current.get("precipitation"),
                "weather_code": current.get("weathercode"),
                "pressure_hpa": current_pressure,
                "humidity_percent": current_humidity,
                "cloud_cover_percent": current_cloud_cover,
                "visibility_m": current_visibility,
            }

        except Exception as e:
            logger.error(f"Error fetching current weather for ({lat}, {lon}): {e}")
            return {}

    # ====================================================================
    # WIND PROFILE: Critical for shear calculations
    # ====================================================================

    @cache_result(ttl_seconds=30 * 60)  # 30-minute cache for forecasts
    async def get_wind_profile(
        self, lat: float, lon: float, hours: int = 120
    ) -> List[Dict[str, Any]]:
        """
        Fetch hourly wind profile for vertical wind shear estimation.

        Wind shear is critical for hurricane intensity forecasting.
        High vertical wind shear (>10 m/s) typically inhibits intensification.
        Multiple wind heights (10m, 80m, 120m) allow shear calculation.

        Args:
            lat: Latitude (degrees)
            lon: Longitude (degrees)
            hours: Number of hours to forecast (default: 120 = 5 days)

        Returns:
            List of dicts with keys:
                - time: ISO timestamp
                - wind_speed_10m: Wind at 10m height (m/s)
                - wind_direction_10m: Wind direction at 10m (0-360)
                - wind_gusts_10m: Wind gust at 10m (m/s)
                - wind_speed_80m: Wind at 80m height (m/s)
                - wind_speed_120m: Wind at 120m height (m/s)
                - pressure_msl: Mean sea level pressure (hPa)
            Returns empty list on API failure.
        """
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": (
                    "wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
                    "wind_speed_80m,wind_speed_120m,pressure_msl"
                ),
                "forecast_hours": hours,
                "timezone": "UTC",
            }

            response = await self._request_with_retry(
                "GET", OPEN_METEO_FORECAST_URL, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            wind_10m = hourly.get("wind_speed_10m", [])
            wind_dir_10m = hourly.get("wind_direction_10m", [])
            wind_gust_10m = hourly.get("wind_gusts_10m", [])
            wind_80m = hourly.get("wind_speed_80m", [])
            wind_120m = hourly.get("wind_speed_120m", [])
            pressures = hourly.get("pressure_msl", [])

            if not times:
                logger.warning(f"No wind profile data for ({lat}, {lon})")
                return []

            profile = []
            for i in range(len(times)):
                profile.append({
                    "time": times[i],
                    "wind_speed_10m": wind_10m[i] if i < len(wind_10m) else None,
                    "wind_direction_10m": wind_dir_10m[i] if i < len(wind_dir_10m) else None,
                    "wind_gusts_10m": wind_gust_10m[i] if i < len(wind_gust_10m) else None,
                    "wind_speed_80m": wind_80m[i] if i < len(wind_80m) else None,
                    "wind_speed_120m": wind_120m[i] if i < len(wind_120m) else None,
                    "pressure_msl": pressures[i] if i < len(pressures) else None,
                })

            return profile

        except Exception as e:
            logger.error(f"Error fetching wind profile for ({lat}, {lon}): {e}")
            return []

    # ====================================================================
    # WIND SHEAR: Vertical environmental wind shear
    # ====================================================================

    @cache_result(ttl_seconds=30 * 60)  # 30-minute cache for forecasts
    async def estimate_wind_shear(self, lat: float, lon: float) -> Optional[float]:
        """
        Estimate vertical environmental wind shear over the next 48 hours.

        Wind shear is calculated as the difference between winds at upper
        levels (120m) and lower levels (10m). Higher shear inhibits
        hurricane intensification.

        Environmental shear > 10 m/s typically prevents intensification.

        Args:
            lat: Latitude (degrees)
            lon: Longitude (degrees)

        Returns:
            Average vertical wind shear over next 48 hours (m/s), or None on failure.
        """
        try:
            wind_profile = await self.get_wind_profile(lat, lon, hours=120)

            if not wind_profile:
                logger.warning(f"No wind profile available for shear calculation at ({lat}, {lon})")
                return None

            # Extract first 48 hours (2 days)
            shear_values = []
            for entry in wind_profile[:48]:
                wind_10m = entry.get("wind_speed_10m")
                wind_120m = entry.get("wind_speed_120m")

                if wind_10m is not None and wind_120m is not None:
                    # Shear is the magnitude difference between upper and lower winds
                    shear = abs(wind_120m - wind_10m)
                    shear_values.append(shear)

            if not shear_values:
                logger.warning(f"No valid wind shear values for ({lat}, {lon})")
                return None

            # Return average shear over 48 hours
            avg_shear = sum(shear_values) / len(shear_values)
            return avg_shear

        except Exception as e:
            logger.error(f"Error estimating wind shear for ({lat}, {lon}): {e}")
            return None

    # ====================================================================
    # FLOOD FORECAST: River discharge and flooding risk
    # ====================================================================

    @cache_result(ttl_seconds=6 * 3600)  # 6-hour cache for historical data
    async def get_flood_forecast(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Fetch flood forecast data from Open-Meteo Flood API.

        River discharge forecasts help assess inland flooding risk from
        hurricane rainfall. Combined with precipitation history and soil
        moisture, gives a comprehensive flood risk picture.

        Args:
            lat: Latitude (degrees)
            lon: Longitude (degrees)

        Returns:
            Dict with keys:
                - river_discharge_m3s: Daily river discharge (m³/s)
                - forecast_dates: Dates corresponding to discharge values
                - ensemble_forecast: Ensemble probability data (if available)
            Returns empty dict on API failure.
        """
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "daily": "river_discharge",
                "ensemble": True,
                "timezone": "UTC",
            }

            response = await self._request_with_retry(
                "GET", OPEN_METEO_FLOOD_URL, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            daily_data = data.get("daily", {})
            times = daily_data.get("time", [])
            discharge = daily_data.get("river_discharge", [])

            if not times or not discharge:
                logger.warning(f"No flood forecast data for ({lat}, {lon})")
                return {}

            # Ensemble data if available
            ensemble_data = {}
            for key in daily_data.keys():
                if "ensemble" in key and key != "time":
                    ensemble_data[key] = daily_data[key]

            return {
                "river_discharge_m3s": discharge,
                "forecast_dates": times,
                "ensemble_forecast": ensemble_data,
            }

        except Exception as e:
            logger.error(f"Error fetching flood forecast for ({lat}, {lon}): {e}")
            return {}
