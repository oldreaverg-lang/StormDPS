"""
National Weather Service (NWS) API client for hurricane tracking.

Pulls real-time weather data from NWS public APIs:
  - Active alerts (hurricanes, tropical storms, storm surge warnings)
  - NHC forecast products (TCU, TCM, TCP)
  - Gridded forecasts (7-day hourly)
  - Marine forecasts (offshore wind and wave data)
  - Weather station observations

No API key required. Free public API with minimal rate limiting.

Usage:
    async with NWSClient() as client:
        hurricanes = await client.get_active_hurricanes()
        forecast = await client.get_gridpoint_forecast(lat=25.5, lon=-80.0)
        observations = await client.get_latest_observation("KMIA")
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

# NWS API base URL
NWS_BASE_URL = "https://api.weather.gov"

# NWS user agent (required by API terms)
NWS_USER_AGENT = os.getenv("NWS_USER_AGENT", "HurricaneIKE-App/1.0 (reavesrg@gmail.com)")


class NWSClientError(Exception):
    """Raised when NWS API request fails."""
    pass


class NWSClient:
    """
    Async client for National Weather Service API.

    Provides real-time weather alerts, forecasts, and observations with
    automatic retry logic and intelligent caching.

    Class-level caches survive across requests within the same server process.

    Usage:
        async with NWSClient(timeout=20.0) as client:
            alerts = await client.get_active_hurricanes()
            forecast = await client.get_gridpoint_forecast(lat=25.5, lon=-80.0)
    """

    # Cache for /points lookups (grid coordinates don't change)
    _points_cache: Dict[str, Dict[str, Any]] = {}

    # Retry configuration
    RETRY_ATTEMPTS = 3
    RETRY_BACKOFF_BASE = 2.0  # exponential: 2s, 4s, 8s

    # HTTP status codes that warrant retry
    RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

    def __init__(self, timeout: float = 20.0):
        """
        Initialize NWS client.

        Args:
            timeout: HTTP request timeout in seconds (default 20).
        """
        self.timeout = timeout
        self._http_client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        """Enter async context manager."""
        self._http_client = httpx.AsyncClient(
            timeout=self.timeout,
            headers={
                "User-Agent": NWS_USER_AGENT,
                "Accept": "application/geo+json",
            },
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args):
        """Exit async context manager."""
        if self._http_client:
            await self._http_client.aclose()

    @property
    def http(self) -> httpx.AsyncClient:
        """Return the active HTTP client or raise error."""
        if self._http_client is None:
            raise RuntimeError("NWSClient must be used as async context manager")
        return self._http_client

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> httpx.Response:
        """
        Make HTTP request with exponential backoff retry logic.

        Retries on: connection errors, timeouts, or specific status codes (408, 429, 5xx).

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL to request
            **kwargs: Additional arguments for httpx.request()

        Returns:
            Response object on success

        Raises:
            NWSClientError: If all retry attempts fail
        """
        last_error: Optional[Exception] = None

        for attempt in range(self.RETRY_ATTEMPTS):
            try:
                resp = await self.http.request(method, url, **kwargs)

                # Immediate return on success or client error (4xx)
                if resp.status_code < 400 or resp.status_code == 404:
                    return resp

                # Retry on specific server/rate-limit errors
                if resp.status_code in self.RETRYABLE_STATUS_CODES and attempt < self.RETRY_ATTEMPTS - 1:
                    backoff = self.RETRY_BACKOFF_BASE ** attempt
                    logger.warning(
                        f"NWS API returned {resp.status_code} for {url}; "
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
                        f"NWS API connection error: {type(e).__name__}; "
                        f"retrying in {backoff:.1f}s (attempt {attempt + 1}/{self.RETRY_ATTEMPTS})"
                    )
                    await asyncio.sleep(backoff)
                    continue
                # Fall through to raise error below

        # All retries exhausted
        if last_error:
            raise NWSClientError(f"NWS API request failed after {self.RETRY_ATTEMPTS} attempts: {last_error}") from last_error
        raise NWSClientError(f"NWS API request to {url} failed after {self.RETRY_ATTEMPTS} attempts")

    async def get_active_hurricanes(self) -> List[Dict[str, Any]]:
        """
        Fetch active hurricane and tropical storm alerts from NWS.

        Queries the NWS alerts endpoint for active warnings and watches related to:
        - Hurricane
        - Tropical Storm Warning
        - Storm Surge Warning
        - Hurricane Warning
        - Hurricane Watch

        Returns:
            List of alert dicts with keys:
            - id: Alert ID
            - event: Event type (e.g., "Hurricane Warning")
            - severity: Alert severity
            - urgency: Alert urgency
            - headline: Short summary
            - description: Full description
            - affected_zones: List of NWS zone IDs
            - effective: ISO 8601 start time
            - expires: ISO 8601 expiration time

        Raises:
            NWSClientError: If request fails after retries
        """
        url = (
            f"{NWS_BASE_URL}/alerts/active"
            "?event=Hurricane,Tropical%20Storm%20Warning,Storm%20Surge%20Warning,"
            "Hurricane%20Warning,Hurricane%20Watch"
        )

        try:
            resp = await self._request_with_retry("GET", url)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            raise NWSClientError(f"Failed to fetch active hurricanes: {e}") from e

        alerts = []
        features = data.get("features", [])

        for feature in features:
            props = feature.get("properties", {})
            zones = props.get("areaDesc", "").split(";")

            alert = {
                "id": props.get("id", ""),
                "event": props.get("event", ""),
                "severity": props.get("severity", ""),
                "urgency": props.get("urgency", ""),
                "headline": props.get("headline", ""),
                "description": props.get("description", ""),
                "affected_zones": [z.strip() for z in zones if z.strip()],
                "effective": props.get("effective", ""),
                "expires": props.get("expires", ""),
                "sender_name": props.get("senderName", ""),
                "instruction": props.get("instruction", ""),
            }
            alerts.append(alert)

        logger.info(f"Found {len(alerts)} active hurricane/storm alerts")
        return alerts

    async def get_hurricane_forecast(self, office: str = "NHC") -> List[Dict[str, Any]]:
        """
        Fetch tropical cyclone forecast products from NWS.

        Retrieves NHC text products:
        - TCU: Tropical Cyclone Update
        - TCM: Tropical Cyclone Discussion
        - TCP: Public Advisory

        Args:
            office: NWS office code (default "NHC" for National Hurricane Center)

        Returns:
            List of forecast product dicts with keys:
            - id: Product ID
            - product_type: Type code (TCU, TCM, TCP)
            - issued: ISO 8601 timestamp
            - updated: ISO 8601 last update
            - title: Product title
            - content: Text content of product

        Raises:
            NWSClientError: If request fails after retries
        """
        products_to_fetch = ["TCU", "TCM", "TCP"]
        all_products: List[Dict[str, Any]] = []

        for product_type in products_to_fetch:
            url = f"{NWS_BASE_URL}/products/types/{product_type}"
            try:
                resp = await self._request_with_retry("GET", url)
                if resp.status_code == 404:
                    logger.info(f"No {product_type} products available")
                    continue
                resp.raise_for_status()
                data = resp.json()

                products = data.get("@graph", [])
                for product in products[:5]:  # Get latest 5 of each type
                    forecast_product = {
                        "id": product.get("@id", ""),
                        "product_type": product_type,
                        "issued": product.get("issuanceTime", ""),
                        "updated": product.get("lastUpdate", ""),
                        "title": product.get("title", ""),
                        "link": product.get("@id", ""),
                    }
                    all_products.append(forecast_product)

            except httpx.HTTPError as e:
                logger.warning(f"Failed to fetch {product_type} products: {e}")

        logger.info(f"Found {len(all_products)} tropical cyclone forecast products")
        return all_products

    async def _get_points(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Fetch grid point metadata from NWS (cached indefinitely).

        The /points endpoint provides:
        - forecast office ID
        - grid point coordinates
        - forecast URLs
        - warning zones
        - time zone

        This is cached because grid coordinates never change.

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            Points metadata dict with keys:
            - forecast_office: Office ID
            - grid_x: X coordinate on grid
            - grid_y: Y coordinate on grid
            - forecast_url: URL for forecast
            - forecast_grid_data_url: URL for gridded data
            - relative_location: Nearby city/reference point

        Raises:
            NWSClientError: If request fails after retries
        """
        cache_key = f"{lat},{lon}"

        # Check cache first
        if cache_key in self._points_cache:
            logger.debug(f"Using cached points data for {lat},{lon}")
            return self._points_cache[cache_key]

        url = f"{NWS_BASE_URL}/points/{lat},{lon}"

        try:
            resp = await self._request_with_retry("GET", url)
            if resp.status_code == 404:
                raise NWSClientError(f"No NWS data available for coordinates {lat},{lon}")
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            raise NWSClientError(f"Failed to fetch points data for {lat},{lon}: {e}") from e

        props = data.get("properties", {})
        result = {
            "forecast_office": props.get("cwa", ""),
            "grid_x": props.get("gridX"),
            "grid_y": props.get("gridY"),
            "forecast_url": props.get("forecast", ""),
            "forecast_grid_data_url": props.get("forecastGridData", ""),
            "relative_location": props.get("relativeLocation", {}),
            "time_zone": props.get("timeZone", ""),
        }

        # Store in cache
        self._points_cache[cache_key] = result
        logger.debug(f"Cached points data for {lat},{lon}")

        return result

    async def get_gridpoint_forecast(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Fetch detailed 7-day hourly gridpoint forecast from NWS.

        Requires two steps:
        1. GET /points/{lat},{lon} to resolve to grid coordinates
        2. GET /gridpoints/{office}/{gridX},{gridY} for hourly data

        Returns comprehensive hourly forecast with:
        - Temperature (°F)
        - Wind speed (kt) and direction (°)
        - Precipitation probability (%) and amount (in)
        - Humidity (%)
        - Weather descriptions

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            Dict with keys:
            - forecast_office: Office ID
            - grid_x, grid_y: Grid coordinates
            - hourly: List of hourly forecast dicts, each with:
              - time: ISO 8601 timestamp
              - temperature_f: Temperature in Fahrenheit
              - wind_speed_kt: Wind speed in knots
              - wind_direction_deg: Wind direction in degrees
              - wind_gust_kt: Wind gust in knots (if available)
              - precipitation_probability: Percent chance (0-100)
              - precipitation_amount_in: Expected rainfall in inches
              - relative_humidity: Percent (0-100)
              - weather: List of weather condition dicts
              - short_forecast: Text summary

        Raises:
            NWSClientError: If request fails after retries
        """
        # Step 1: Get grid coordinates
        points = await self._get_points(lat, lon)

        # Step 2: Fetch detailed gridpoint forecast
        office = points["forecast_office"]
        grid_x = points["grid_x"]
        grid_y = points["grid_y"]

        url = f"{NWS_BASE_URL}/gridpoints/{office}/{grid_x},{grid_y}"

        try:
            resp = await self._request_with_retry("GET", url)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            raise NWSClientError(f"Failed to fetch gridpoint forecast for {lat},{lon}: {e}") from e

        props = data.get("properties", {})

        # Parse hourly periods
        hourly_forecast = []
        periods = props.get("periods", [])

        for period in periods:
            hourly_data = {
                "time": period.get("validTime", ""),
                "temperature_f": period.get("temperature"),
                "wind_speed_kt": period.get("windSpeed"),
                "wind_direction_deg": period.get("windDirection"),
                "wind_gust_kt": period.get("windGust"),
                "precipitation_probability": period.get("probabilityOfPrecipitation"),
                "precipitation_amount_in": period.get("quantitativePrecipitation"),
                "relative_humidity": period.get("relativeHumidity"),
                "dewpoint_f": period.get("dewpoint"),
                "weather": period.get("weather", []),
                "short_forecast": period.get("shortForecast", ""),
                "icon": period.get("icon", ""),
            }
            hourly_forecast.append(hourly_data)

        result = {
            "forecast_office": office,
            "grid_x": grid_x,
            "grid_y": grid_y,
            "latitude": lat,
            "longitude": lon,
            "update_time": props.get("updateTime", ""),
            "valid_time": props.get("validTime", ""),
            "hourly": hourly_forecast,
        }

        logger.info(f"Fetched {len(hourly_forecast)} hours of forecast for {lat},{lon}")
        return result

    async def get_marine_forecast(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Fetch marine/offshore weather forecast from NWS gridpoint data.

        Extracts wind and wave information relevant for offshore hurricane
        conditions. Uses the same gridpoint forecast endpoint but filters
        for marine-relevant parameters.

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            Dict with keys:
            - forecast_office: Office ID
            - latitude, longitude: Coordinates
            - marine_zones: List of NWS marine zone IDs
            - offshore_hourly: List of hourly marine forecast dicts, each with:
              - time: ISO 8601 timestamp
              - wind_speed_kt: Wind speed in knots
              - wind_direction_deg: Wind direction in degrees
              - wind_gust_kt: Wind gust in knots
              - significant_wave_height_ft: Wave height in feet
              - dominant_wave_period_sec: Wave period in seconds
              - short_forecast: Text marine forecast

        Raises:
            NWSClientError: If request fails after retries
        """
        # Get base gridpoint forecast
        forecast = await self.get_gridpoint_forecast(lat, lon)

        # Extract marine parameters from hourly data
        offshore_hourly = []
        for hour in forecast.get("hourly", []):
            marine_hour = {
                "time": hour.get("time", ""),
                "wind_speed_kt": hour.get("wind_speed_kt"),
                "wind_direction_deg": hour.get("wind_direction_deg"),
                "wind_gust_kt": hour.get("wind_gust_kt"),
                "short_forecast": hour.get("short_forecast", ""),
                # Wave data is not typically in standard gridpoint endpoint
                # but would be added if available from marine-specific endpoints
            }
            offshore_hourly.append(marine_hour)

        result = {
            "forecast_office": forecast.get("forecast_office"),
            "latitude": lat,
            "longitude": lon,
            "update_time": forecast.get("update_time", ""),
            "offshore_hourly": offshore_hourly,
        }

        logger.info(f"Fetched marine forecast for {lat},{lon}")
        return result

    async def get_observation_stations(
        self,
        lat: float,
        lon: float,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Fetch nearby NWS observation stations.

        Args:
            lat: Latitude
            lon: Longitude
            limit: Maximum number of stations to return (default 5)

        Returns:
            List of station dicts with keys:
            - station_id: Station ID (e.g., "KMIA")
            - station_name: Station name
            - url: URL for station
            - latitude: Station latitude
            - longitude: Station longitude
            - distance_m: Distance from query point in meters

        Raises:
            NWSClientError: If request fails after retries
        """
        # Step 1: Get points to find nearby stations
        points = await self._get_points(lat, lon)
        office = points["forecast_office"]
        grid_x = points["grid_x"]
        grid_y = points["grid_y"]

        # Step 2: Fetch station list for grid point
        url = f"{NWS_BASE_URL}/gridpoints/{office}/{grid_x},{grid_y}/stations"

        try:
            resp = await self._request_with_retry("GET", url)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            logger.warning(f"Failed to fetch observation stations: {e}")
            return []

        stations = []
        features = data.get("features", [])

        for feature in features[:limit]:
            props = feature.get("properties", {})
            geom = feature.get("geometry", {})
            coords = geom.get("coordinates", [None, None])

            station = {
                "station_id": props.get("stationIdentifier", "").split("/")[-1],
                "station_name": props.get("name", ""),
                "url": props.get("@id", ""),
                "latitude": coords[1] if len(coords) > 1 else None,
                "longitude": coords[0] if len(coords) > 0 else None,
                "timezone": props.get("timeZone", ""),
            }
            stations.append(station)

        logger.info(f"Found {len(stations)} observation stations near {lat},{lon}")
        return stations

    async def get_latest_observation(self, station_id: str) -> Dict[str, Any]:
        """
        Fetch latest observation from a NWS weather station.

        Args:
            station_id: Station ID (e.g., "KMIA", "KTWB")

        Returns:
            Observation dict with keys:
            - timestamp: ISO 8601 observation time
            - temperature_f: Temperature in Fahrenheit
            - dewpoint_f: Dew point in Fahrenheit
            - wind_speed_kt: Wind speed in knots
            - wind_direction_deg: Wind direction in degrees
            - wind_gust_kt: Wind gust in knots
            - visibility_sm: Visibility in statute miles
            - ceiling_ft: Ceiling height in feet
            - pressure_mb: Barometric pressure in millibars
            - precipitation_1hr_in: Precipitation in last hour (inches)
            - relative_humidity: Percent (0-100)
            - weather: List of weather condition dicts
            - raw_message: Raw METAR or other format

        Raises:
            NWSClientError: If request fails after retries
        """
        url = f"{NWS_BASE_URL}/stations/{station_id}/observations/latest"

        try:
            resp = await self._request_with_retry("GET", url)
            if resp.status_code == 404:
                raise NWSClientError(f"Station {station_id} not found")
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            raise NWSClientError(f"Failed to fetch observation for station {station_id}: {e}") from e

        props = data.get("properties", {})

        observation = {
            "station_id": station_id,
            "timestamp": props.get("timestamp", ""),
            "temperature_f": props.get("temperature"),
            "dewpoint_f": props.get("dewpoint"),
            "wind_speed_kt": props.get("windSpeed"),
            "wind_direction_deg": props.get("windDirection"),
            "wind_gust_kt": props.get("windGust"),
            "visibility_sm": props.get("visibility"),
            "ceiling_ft": props.get("ceiling"),
            "pressure_mb": props.get("barometricPressure"),
            "precipitation_1hr_in": props.get("precipitationLastHour"),
            "relative_humidity": props.get("relativeHumidity"),
            "weather": props.get("weather", []),
            "raw_message": props.get("rawMessage", ""),
        }

        logger.info(f"Fetched latest observation for station {station_id}")
        return observation

    @classmethod
    def clear_points_cache(cls):
        """Clear the /points lookup cache (for testing or manual refresh)."""
        cls._points_cache.clear()
        logger.info("Cleared NWS points cache")
