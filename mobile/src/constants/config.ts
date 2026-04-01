import Constants from "expo-constants";
import { Platform } from "react-native";

/**
 * API base URL resolution:
 * - Web: same origin (relative /api/v1)
 * - iOS simulator: localhost mapped
 * - Android emulator: 10.0.2.2 (host loopback)
 * - Physical device: use the configured production URL
 *
 * Override via EXPO_PUBLIC_API_URL in .env
 */
function resolveApiUrl(): string {
  const envUrl = Constants.expoConfig?.extra?.apiUrl;
  if (envUrl) return envUrl;

  if (__DEV__) {
    if (Platform.OS === "web") return "http://localhost:8000/api/v1";
    if (Platform.OS === "android") return "http://10.0.2.2:8000/api/v1";
    // iOS simulator uses localhost
    return "http://localhost:8000/api/v1";
  }

  // Production — replace with your deployed API URL
  return "https://api.hurricane-dpi.com/api/v1";
}

function getApiRootUrl(): string {
  const baseUrl = resolveApiUrl();
  // Remove /api/v1 suffix to get root URL
  return baseUrl.endsWith("/api/v1") ? baseUrl.slice(0, -7) : baseUrl;
}

export const CONFIG = {
  API_BASE_URL: resolveApiUrl(),
  API_ROOT_URL: getApiRootUrl(),

  /** Cache TTL for storm list (ms) */
  STORM_LIST_CACHE_TTL: 60_000,

  /** Cache TTL for IKE computations (ms) */
  IKE_CACHE_TTL: 300_000,

  /** Cache TTL for weather data (ms) */
  WEATHER_CACHE_TTL: 900_000,

  /** Map default center (Gulf of Mexico) */
  MAP_DEFAULT_CENTER: { latitude: 25.0, longitude: -80.0 },
  MAP_DEFAULT_ZOOM: 5,

  /** Animation scrubber settings */
  SCRUBBER_MIN_SPEED: 0.5,
  SCRUBBER_MAX_SPEED: 4.0,
  SCRUBBER_DEFAULT_SPEED: 1.0,

  /** Max concurrent API requests */
  MAX_CONCURRENT_REQUESTS: 4,
} as const;
