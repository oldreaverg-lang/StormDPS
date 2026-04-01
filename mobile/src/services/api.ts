/**
 * HTTP client for the Hurricane DPI FastAPI backend.
 *
 * Uses axios for cross-platform HTTP (works identically on web, iOS, Android).
 * All endpoints match the FastAPI routes in api/routes.py and api/weather_routes.py.
 */

import axios, { AxiosInstance, AxiosError } from "axios";
import { CONFIG } from "@/constants/config";
import type {
  StormSummary,
  ValuationResult,
  IKEResponse,
  StormHistoryPoint,
  WeatherConditions,
  LandProximityResult,
  TerrainResult,
} from "@/types/storm";

// ---------------------------------------------------------------------------
// Client singleton
// ---------------------------------------------------------------------------

const client: AxiosInstance = axios.create({
  baseURL: CONFIG.API_BASE_URL,
  timeout: 30_000,
  headers: { "Content-Type": "application/json" },
});

// Response interceptor — normalize errors
client.interceptors.response.use(
  (res) => res,
  (err: AxiosError) => {
    const status = err.response?.status;
    const message =
      (err.response?.data as any)?.detail ||
      err.message ||
      "Network request failed";
    console.warn(`[API] ${err.config?.method?.toUpperCase()} ${err.config?.url} → ${status}: ${message}`);
    return Promise.reject(new ApiError(message, status));
  }
);

export class ApiError extends Error {
  status?: number;
  constructor(message: string, status?: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

// ---------------------------------------------------------------------------
// Storm endpoints (api/routes.py)
// ---------------------------------------------------------------------------

/** List active tropical cyclones from NHC */
export async function getActiveStorms(): Promise<StormSummary[]> {
  const { data } = await client.get("/storms/active");
  return data;
}

/** Search storm catalog by name, year, or basin */
export async function searchStorms(params: {
  query?: string;
  basin?: string;
  year?: number;
  limit?: number;
}): Promise<StormSummary[]> {
  const { data } = await client.get("/storms/search", { params });
  return data;
}

/** Get full IKE computation for a storm snapshot */
export async function getStormIKE(
  stormId: string,
  gridResolutionKm: number = 5.0
): Promise<IKEResponse> {
  const { data } = await client.get(`/storms/${stormId}/ike`, {
    params: { grid_resolution_km: gridResolutionKm },
  });
  return data;
}

/** Get full valuation for a storm */
export async function getStormValuation(
  stormId: string,
  gridResolutionKm: number = 5.0
): Promise<ValuationResult> {
  const { data } = await client.get(`/storms/${stormId}/value`, {
    params: { grid_resolution_km: gridResolutionKm },
  });
  return data;
}

/** Get historical track + IKE timeline for a storm */
export async function getStormHistory(
  stormId: string
): Promise<StormHistoryPoint[]> {
  const { data } = await client.get(`/storms/${stormId}/history`);
  return data;
}

/** Get the preloaded bundle (cached IKE for preset storms) */
export async function getPreloadBundle(): Promise<Record<string, any>> {
  const { data } = await client.get("/preload");
  return data;
}

// ---------------------------------------------------------------------------
// Weather endpoints (api/weather_routes.py)
// ---------------------------------------------------------------------------

/** Get current weather conditions at a location */
export async function getWeatherConditions(
  lat: number,
  lon: number
): Promise<WeatherConditions> {
  const { data } = await client.get(`/weather/conditions/${lat}/${lon}`);
  return data;
}

/** Get environmental analysis for storm context */
export async function getEnvironment(
  lat: number,
  lon: number
): Promise<Record<string, any>> {
  const { data } = await client.get(`/weather/environment/${lat}/${lon}`);
  return data;
}

/** Get enhanced storm data from multiple weather APIs */
export async function getEnhancedStorm(
  stormId: string
): Promise<Record<string, any>> {
  const { data } = await client.get(`/weather/storm/${stormId}/enhanced`);
  return data;
}

/** Get weather alerts for a location */
export async function getWeatherAlerts(
  lat: number,
  lon: number
): Promise<any[]> {
  const { data } = await client.get(`/weather/alerts/${lat}/${lon}`);
  return data;
}

/** Get flood risk assessment */
export async function getFloodRisk(
  lat: number,
  lon: number
): Promise<Record<string, any>> {
  const { data } = await client.get(`/weather/flood-risk/${lat}/${lon}`);
  return data;
}

/** Get land proximity analysis */
export async function getLandProximity(
  lat: number,
  lon: number,
  r34Nm?: number
): Promise<LandProximityResult> {
  const url = r34Nm !== undefined
    ? `/weather/land-proximity/${lat}/${lon}?r34_nm=${r34Nm}`
    : `/weather/land-proximity/${lat}/${lon}`;
  const { data } = await client.get(url);
  return data;
}

/** Get terrain analysis */
export async function getTerrainAnalysis(
  lat: number,
  lon: number,
  approachDeg?: number
): Promise<TerrainResult> {
  const url = approachDeg !== undefined
    ? `/weather/terrain/${lat}/${lon}?approach_deg=${approachDeg}`
    : `/weather/terrain/${lat}/${lon}`;
  const { data } = await client.get(url);
  return data;
}

/** Get data source status */
export async function getDataSources(): Promise<Record<string, any>> {
  const { data } = await client.get("/weather/sources");
  return data;
}

// ---------------------------------------------------------------------------
// AI & Comparison endpoints
// ---------------------------------------------------------------------------

export async function getAIComparison(
  stormId: string
): Promise<Record<string, any>> {
  const { data } = await client.get(`/storms/${stormId}/ai-comparison`);
  return data;
}

export async function getSourceHealth(): Promise<Record<string, any>> {
  const { data } = await client.get("/health/sources");
  return data;
}

// ---------------------------------------------------------------------------
// Health check
// ---------------------------------------------------------------------------

export async function healthCheck(): Promise<boolean> {
  try {
    const { data } = await client.get("/health", {
      baseURL: CONFIG.API_ROOT_URL,
    });
    return data?.status === "ok";
  } catch {
    return false;
  }
}

export default client;
