/**
 * Core type definitions matching the FastAPI backend response schemas.
 * These mirror the Pydantic models in api/schemas.py and models/hurricane.py.
 */

export interface StormSummary {
  id: string;
  name: string;
  classification: string;
  lat: number | null;
  lon: number | null;
  intensity_knots: number | null;
  pressure_mb: number | null;
  movement: string | null;
  movement_speed_knots: number | null;
  movement_direction_deg: number | null;
}

export interface WindFieldPoint {
  lat: number;
  lon: number;
  wind_speed_ms: number;
}

export interface IKEResponse {
  storm_id: string;
  timestamp: string | null;
  ike_total_tj: number;
  ike_hurricane_tj: number;
  ike_tropical_storm_tj: number;
  ike_pretty: string;
  lat: number | null;
  lon: number | null;
  wind_field_source: string;
  max_wind_ms: number | null;
  min_pressure_hpa: number | null;
  rmw_nm: number | null;
  r34_nm: number | null;
  r64_nm: number | null;
  r34_quadrants: QuadrantRadii | null;
  forward_speed_knots: number | null;
  forward_direction_deg: number | null;
  radii_confidence: number | null;
}

export interface SurgeRainfallResult {
  surge_height_m: number;
  surge_score: number;
  rainfall_total_mm: number;
  rainfall_score: number;
  compound_flood_score: number;
  region_key: string;
}

export interface EconomicImpactResult {
  economic_score: number;
  estimated_damage_billion_usd: number;
  damage_to_gdp_ratio: number;
  wind_damage_fraction: number;
  surge_damage_fraction: number;
  rain_damage_fraction: number;
  vulnerability_score: number;
  exposure_score: number;
  region_key: string;
}

export interface DPIResult {
  dpi_score: number;
  dpi_category: string;
  ike_score: number;
  surge_rain_score: number;
  economic_score: number;
  land_proximity_factor: number;
  distance_to_coast_km: number | null;
  population_threat: number;
  region_key: string;
}

export interface ValuationResult {
  storm_id: string;
  name: string;
  timestamp: string | null;
  ike: IKEResponse;
  destructive_potential: number;
  surge_threat: number | null;
  overall_value: number | null;
  category: string;
}

// Note: History endpoint returns list[IKEResponse], same as /storms/{storm_id}/ike
// This alias is kept for backwards compatibility in mobile code
export type StormHistoryPoint = IKEResponse;

export interface WeatherConditions {
  temperature_c: number;
  sst_c: number | null;
  wind_speed_ms: number;
  wind_direction_deg: number;
  humidity_pct: number;
  pressure_hpa: number;
}

export interface LandProximityResult {
  distance_to_coast_km: number;
  land_proximity_factor: number;
  nearest_region: string;
  population_threat: number;
}

export interface TerrainResult {
  orographic_factor: number;
  elevation_m: number;
  valley_flooding_risk: number;
  slope_runoff_factor: number;
}
