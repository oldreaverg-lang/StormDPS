/**
 * React Query hooks for storm data fetching.
 * Provides automatic caching, background refetch, and error handling
 * across all three platforms (web, iOS, Android).
 */

import { useQuery } from "@tanstack/react-query";
import { CONFIG } from "@/constants/config";
import * as api from "@/services/api";

/** Fetch active storms — auto-refreshes every 60s */
export function useActiveStorms() {
  return useQuery({
    queryKey: ["storms", "active"],
    queryFn: api.getActiveStorms,
    refetchInterval: CONFIG.STORM_LIST_CACHE_TTL,
    staleTime: CONFIG.STORM_LIST_CACHE_TTL,
  });
}

/** Search storms by name/year/basin */
export function useStormSearch(query: string, basin?: string, year?: number) {
  return useQuery({
    queryKey: ["storms", "search", query, basin, year],
    queryFn: () => api.searchStorms({ query, basin, year, limit: 50 }),
    enabled: query.length >= 2,
    staleTime: CONFIG.STORM_LIST_CACHE_TTL,
  });
}

/** Full valuation for a single storm */
export function useStormValuation(stormId: string | null) {
  return useQuery({
    queryKey: ["storms", stormId, "valuation"],
    queryFn: () => api.getStormValuation(stormId!),
    enabled: !!stormId,
    staleTime: CONFIG.IKE_CACHE_TTL,
  });
}

/** Storm track history */
export function useStormHistory(stormId: string | null) {
  return useQuery({
    queryKey: ["storms", stormId, "history"],
    queryFn: () => api.getStormHistory(stormId!),
    enabled: !!stormId,
    staleTime: CONFIG.IKE_CACHE_TTL,
  });
}

/** Weather alerts near a location */
export function useWeatherAlerts(lat: number | null, lon: number | null) {
  return useQuery({
    queryKey: ["weather", "alerts", lat, lon],
    queryFn: () => api.getWeatherAlerts(lat!, lon!),
    enabled: lat != null && lon != null,
    staleTime: CONFIG.WEATHER_CACHE_TTL,
  });
}

/** API health check */
export function useHealthCheck() {
  return useQuery({
    queryKey: ["health"],
    queryFn: api.healthCheck,
    refetchInterval: 30_000,
    staleTime: 15_000,
  });
}

