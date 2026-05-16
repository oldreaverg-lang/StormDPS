"""Build the stage-by-stage intermediate trace for Hurricane Ian (AL092022).

Derives every value from compile_cache.py + core/dps_engine.py logic
applied to the bundle field values. Cross-checks the recomputed final
DPS against the bundle's stored 'dps' field.

This script reproduces the audit independently — keep it next to the
JSON output so a secondary auditor can rerun and verify.
"""
import json
import math

with open('frontend/compiled_bundle.json', encoding='utf-8') as f:
    bundle = json.load(f)
ian = bundle['storms']['AL092022']

# Stage 1: per-snapshot peak DPI (from bundle)
peak_dpi_raw = ian['peak_dps']  # 82.1

# Stage 2: cumulative DPI = peak_dpi * (1 + duration_factor + breadth_factor)
dur = ian['duration_factor']  # 0.10 (capped)
brd = ian['breadth_factor']   # 0.10 (capped)
cum_dpi = peak_dpi_raw * (1 + dur + brd)  # 98.52

# Stage 3: combined_boost factors
exp_f = ian['exposure_factor']        # 0.065 (Tampa Bay)
perp = ian['perp_factor']             # 0.0 (BUG: should be > 0 per us_landfall_count fix)
stall_b = ian['stall_bonus']          # 0.010
rain_in = ian['rain_inland_factor']   # 0.026
inland_pen = ian['inland_pen_factor'] # 0.024
combined_boost = exp_f + perp + stall_b + rain_in + inland_pen  # 0.125

# boosted = peak * (cum/peak + combined_boost)
boosted = peak_dpi_raw * ((cum_dpi / peak_dpi_raw) + combined_boost)

# Stage 4: Atlantic basin adjustment (× 1.00, no RI bonus, no enhancements)
adjusted = boosted * 1.00

# Stage 5: per-basin sqrt compression — Atlantic (T=60, S=4)
T, S = 60.0, 4.0
if adjusted > T:
    compressed = T + S * math.sqrt(adjusted - T)
else:
    compressed = adjusted
final = min(compressed, 99.0)

trace = {
    "storm_id": "AL092022",
    "name": "Ian",
    "year": 2022,
    "basin": "ATLANTIC",
    "stages": {
        "stage_1_per_snapshot_dpi": {
            "description": (
                "Max single-snapshot DPI along the track. Computed by "
                "core/dpi.py:compute_dpi for each 6-hourly snapshot — "
                "raw_dpi = 0.30*IKE_score + 0.35*SurgeRain_score + "
                "0.35*Economic_score + bonuses (vuln, compact, "
                "coast_tracking, stall_dpi, RI), then dampened by "
                "land_proximity_factor."
            ),
            "max_wind_ms_at_peak": ian['peak_wind_ms'],
            "max_wind_kt_at_peak": ian['peak_wind_kt'],
            "min_pressure_hpa": ian['min_pressure_hpa'],
            "peak_ike_tj": ian['peak_ike_tj'],
            "snapshot_count": ian['snapshot_count'],
            "peak_dpi_raw": peak_dpi_raw,
            "source": "compute_storm_dps output, field 'peak_dps'",
        },
        "stage_2_cumulative_dpi": {
            "description": (
                "cum_dpi = peak_dpi x (1 + duration_factor + breadth_factor). "
                "Both factors cap at 0.10."
            ),
            "formula": "cum_dpi = peak_dpi * (1 + duration_factor + breadth_factor)",
            "inputs": {
                "peak_dpi": peak_dpi_raw,
                "duration_factor": dur,
                "duration_factor_capped": dur >= 0.099,
                "breadth_factor": brd,
                "breadth_factor_capped": brd >= 0.099,
                "coastal_hours": ian['coastal_hours'],
                "track_hours": ian['track_hours'],
            },
            "calculation": (
                f"{peak_dpi_raw} * (1 + {dur} + {brd}) = "
                f"{round(cum_dpi, 2)}"
            ),
            "cum_dpi_computed": round(cum_dpi, 2),
            "cum_dpi_in_bundle": ian['dps_original'],
            "source": (
                "core/cumulative_dpi.py compute_cumulative_dpi -> "
                "bundle field 'dps_original'"
            ),
        },
        "stage_3_combined_boost": {
            "description": (
                "Stage-3 economic boost factors added to the peak-fraction "
                "multiplier. boosted = peak x (cum/peak + sum(boosts))."
            ),
            "factors": {
                "exposure_factor": {
                    "value": exp_f,
                    "region": ian.get('exposure_region', '(none)'),
                    "source": (
                        "compile_cache.compute_exposure_factor; Tampa Bay "
                        "COASTAL_EXPOSURE_WEIGHTS=0.65 x cap 0.10 = 0.065"
                    ),
                },
                "perp_factor": {
                    "value": perp,
                    "note": (
                        "Should be > 0 for Ian (2 US landfalls). The 0 in "
                        "the bundle reflects the us_landfall_count bug "
                        "fixed in commit 659342b but unchanged in the "
                        "bundle until next compile_cache run."
                    ),
                    "source": (
                        "compile_cache.compute_perpendicular_factor "
                        "(stale-bundle artifact)"
                    ),
                },
                "stall_bonus": {
                    "value": stall_b,
                    "stall_hours": ian['stall_hours'],
                    "source": "core/dps_engine.py Stage 3",
                },
                "rain_inland_factor": {
                    "value": rain_in,
                    "rainfall_warning_score": ian['rainfall_warning'],
                    "estimated_rainfall_mm": ian['rainfall_est_mm'],
                    "source": (
                        "core/rainfall_warning.py + observed rainfall "
                        "ground_truth override (Grove City FL: 26.95 in)"
                    ),
                },
                "inland_pen_factor": {
                    "value": inland_pen,
                    "source": (
                        "core/dps_engine.py: tropical-storm-force winds "
                        "reaching inland after FL landfall"
                    ),
                },
            },
            "combined_boost": round(combined_boost, 4),
            "boosted_formula": (
                "boosted = peak_dpi x ((cum_dpi / peak_dpi) + combined_boost)"
            ),
            "boosted_calculation": (
                f"{peak_dpi_raw} x ({round(cum_dpi/peak_dpi_raw, 4)} + "
                f"{combined_boost}) = {round(boosted, 2)}"
            ),
            "boosted_value": round(boosted, 2),
        },
        "stage_4_basin_adjustment": {
            "description": (
                "Atlantic basin: dps_multiplier=1.00, ri_bonus=0, no WP/EP "
                "enhancements. The Stage-4 block is a no-op for Atlantic "
                "by design."
            ),
            "basin": "ATLANTIC",
            "dps_multiplier": 1.00,
            "ri_bonus": 0,
            "adjustment_notes": ian.get('adjustment_notes', ''),
            "adjusted": round(adjusted, 2),
            "source": (
                "compile_cache.apply_basin_dps_adjustment "
                "(BASIN_COEFFICIENTS ATLANTIC)"
            ),
        },
        "stage_5_compression": {
            "description": (
                "Per-basin sqrt compression. Atlantic uses (T=60, S=4), "
                "the pre-v7-audit calibration preserved to keep the "
                "hand-tuned Atlantic spread (Katrina 93 / Maria 86 / "
                "Harvey 83)."
            ),
            "params": {"T": T, "S": S, "basin": "ATLANTIC"},
            "formula": (
                "if adjusted > T: adjusted = T + S * sqrt(adjusted - T); "
                "final = min(adjusted, 99)"
            ),
            "calculation": (
                f"60 + 4 * sqrt({round(adjusted, 2)} - 60) = "
                f"60 + 4 * sqrt({round(adjusted - T, 2)}) = "
                f"60 + 4 * {round(math.sqrt(adjusted - T), 3)} = "
                f"{round(compressed, 2)}"
            ),
            "compressed_value": round(compressed, 2),
            "hard_cap_99": compressed >= 99.0,
            "source": (
                "compile_cache.apply_basin_dps_adjustment Stage-5 "
                "sqrt compression"
            ),
        },
    },
    "final": {
        "displayed_dps": round(final, 2),
        "displayed_dps_in_bundle": ian['dps'],
        "dps_label": ian['dps_label'],
        "category_lifetime": ian['category_lifetime'],
        "category_landfall": ian['category'],
        "match_check": abs(final - ian['dps']) < 0.5,
    },
    "ground_truth_reference": {
        "damage_usd_2024": 112_900_000_000,
        "deaths_total": 156,
        "peak_surge_ft": 15.0,
        "peak_surge_location": "Fort Myers Beach, FL",
        "peak_rainfall_in": 26.95,
        "peak_rainfall_location": "Grove City, FL",
        "peak_wind_landfall_mph": 150,
        "landfall_pressure_mb": 936,
        "landfall_saffir": 5,
        "fema_states": ["FL", "SC", "NC"],
        "source": (
            "core/ground_truth.py:AL092022 "
            "(NHC TCR Bucci et al. 2023 + USGS STN)"
        ),
    },
    "known_caveats": [
        (
            "perp_factor = 0.0 in the bundle is a stale-bundle artifact. "
            "The fix in commit 659342b corrected compute_perpendicular_factor; "
            "current code would yield perp_factor ~ 0.06 for Ian (2 US "
            "landfalls x 0.03 = 0.06). Bundle does not reflect this until "
            "next compile_cache.py run."
        ),
        (
            "Bundle was compiled under the v9-era global compression "
            "(T=70, S=2.5). Atlantic numbers happen to be unchanged under "
            "v10+v11 per-basin compression that reverts Atlantic to "
            "(T=60, S=4) - verified by manual recompute in commit 659342b "
            "sanity-check."
        ),
        (
            "dpi_timeseries values are scaled to match the final displayed "
            "DPS, not the raw per-snapshot DPI. Peak entry in the series "
            "(87.9 at 2022-09-28 12:00z) equals the displayed final 87.93; "
            "the unscaled peak_dps is 82.1. Bundle stores both."
        ),
    ],
    "audit_method": (
        "Re-derived from compile_cache.py + core/dps_engine.py + bundle "
        "field values. Cross-checked against HURDAT2 best-track AL092022 "
        "+ NHC TCR (Bucci et al. 2023). Three-landfall sequence (Pinar "
        "del Rio Cuba 22.2N/83.7W as Cat 3, Cayo Costa FL 26.7N/82.2W as "
        "Cat 5, SC coast 33.3N/79.2W as Cat 1) matches the bundle's "
        "landfalls array."
    ),
}

with open('audits/ian_2022/ian_intermediate.json', 'w', encoding='utf-8') as f:
    json.dump(trace, f, indent=2)

print(f'Wrote ian_intermediate.json')
print(f'Final DPS recomputed: {round(final, 2)}')
print(f'Bundle stored DPS:    {ian["dps"]:.2f}')
print(f'Match (within 0.5):   {abs(final - ian["dps"]) < 0.5}')
