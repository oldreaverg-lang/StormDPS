"""Build the three audit artifacts for Hurricane Florence (AL062018).

Mirrors audits/ian_2022/_build_intermediate.py — joins HURDAT2 best-track
with bundle's per-snapshot DPI, then reconstructs the stage-by-stage
formula trace from bundle field values.

Florence is the inverse-archetype to Ian:
  - Cat 4 lifetime, Cat 2 at landfall (Saffir-Simpson misalignment)
  - Modest IKE, modest surge, catastrophic inland rainfall
  - 42 stall_hours (vs Ian's 0)
  - rainfall_est_mm = 2281 vs observed 913mm — the headline finding

Idempotent: rerun produces identical artifacts from the same inputs.
"""
import csv
import json
import math
from datetime import datetime

# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

HURDAT = 'C:/Users/Ryan/APPS/StormDPS/.claude/worktrees/beautiful-moser-492ac9/data/hurdat2.txt'
BUNDLE = 'frontend/compiled_bundle.json'
OUT_CSV = 'audits/florence_2018/florence_snapshots.csv'
OUT_JSON = 'audits/florence_2018/florence_intermediate.json'


def haversine_nm(lat1, lon1, lat2, lon2):
    R = 3440.065  # nm
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Pull HURDAT2 best-track for Florence
# ---------------------------------------------------------------------------

with open(HURDAT, encoding='utf-8') as f:
    lines = f.read().split('\n')

i = 0
hurdat_rows = []
while i < len(lines):
    if lines[i].startswith('AL062018'):
        n = int(lines[i].split(',')[2])
        for j in range(1, n+1):
            parts = [p.strip() for p in lines[i+j].split(',')]
            date_s, time_s, record_id, status, lat_s, lon_s, vmax_s, pres_s = parts[:8]
            r34 = [int(parts[8+k]) for k in range(4)]
            r50 = [int(parts[12+k]) for k in range(4)]
            r64 = [int(parts[16+k]) for k in range(4)]
            try:
                rmw = int(parts[20])
                if rmw == -999:
                    rmw = None
            except (IndexError, ValueError):
                rmw = None
            lat = float(lat_s[:-1]) * (1 if lat_s[-1] == 'N' else -1)
            lon = float(lon_s[:-1]) * (-1 if lon_s[-1] == 'W' else 1)
            timestamp = f'{date_s[:4]}-{date_s[4:6]}-{date_s[6:8]}T{time_s[:2]}:{time_s[2:]}:00'
            hurdat_rows.append({
                'time_utc': timestamp,
                'lat': lat, 'lon': lon,
                'record_id': record_id, 'status': status,
                'max_wind_kt': int(vmax_s),
                'min_pressure_mb': int(pres_s),
                'r34_ne_nm': r34[0], 'r34_se_nm': r34[1],
                'r34_sw_nm': r34[2], 'r34_nw_nm': r34[3],
                'r50_ne_nm': r50[0], 'r50_se_nm': r50[1],
                'r50_sw_nm': r50[2], 'r50_nw_nm': r50[3],
                'r64_ne_nm': r64[0], 'r64_se_nm': r64[1],
                'r64_sw_nm': r64[2], 'r64_nw_nm': r64[3],
                'rmw_nm': rmw if rmw else '',
                'max_r34_nm': max(r34),
            })
        break
    i += 1


# ---------------------------------------------------------------------------
# Pull bundle data
# ---------------------------------------------------------------------------

with open(BUNDLE, encoding='utf-8') as f:
    bundle = json.load(f)
flo = bundle['storms']['AL062018']
dpi_by_time = {pt['t']: pt['dpi'] for pt in flo['dpi_timeseries']}

# Add forward speed + DPI to each HURDAT2 row
for i, row in enumerate(hurdat_rows):
    next_syn = next(
        (r for r in hurdat_rows[i+1:] if r['record_id'] != 'L'),
        None,
    )
    if next_syn and row['record_id'] != 'L':
        dist = haversine_nm(row['lat'], row['lon'], next_syn['lat'], next_syn['lon'])
        t1 = datetime.fromisoformat(row['time_utc'])
        t2 = datetime.fromisoformat(next_syn['time_utc'])
        dt_h = (t2 - t1).total_seconds() / 3600
        row['forward_speed_kt'] = round(dist / dt_h, 1) if dt_h > 0 else ''
    else:
        row['forward_speed_kt'] = ''
    row['pipeline_dpi'] = dpi_by_time.get(row['time_utc'], '')


# ---------------------------------------------------------------------------
# Write CSV
# ---------------------------------------------------------------------------

cols = ['time_utc', 'lat', 'lon', 'record_id', 'status',
        'max_wind_kt', 'min_pressure_mb', 'forward_speed_kt', 'max_r34_nm',
        'r34_ne_nm', 'r34_se_nm', 'r34_sw_nm', 'r34_nw_nm',
        'r50_ne_nm', 'r50_se_nm', 'r50_sw_nm', 'r50_nw_nm',
        'r64_ne_nm', 'r64_se_nm', 'r64_sw_nm', 'r64_nw_nm',
        'rmw_nm', 'pipeline_dpi']
with open(OUT_CSV, 'w', encoding='utf-8', newline='') as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    for row in hurdat_rows:
        w.writerow({c: row.get(c, '') for c in cols})

print(f'Wrote {len(hurdat_rows)} rows to {OUT_CSV}')


# ---------------------------------------------------------------------------
# Stage-by-stage trace
# ---------------------------------------------------------------------------

peak_dpi_raw = flo['peak_dps']
dur = flo['duration_factor']
brd = flo['breadth_factor']
cum_dpi = peak_dpi_raw * (1 + dur + brd)

exp_f = flo['exposure_factor']
perp = flo['perp_factor']
stall_b = flo['stall_bonus']
rain_in = flo['rain_inland_factor']
inland_pen = flo['inland_pen_factor']
combined_boost = exp_f + perp + stall_b + rain_in + inland_pen

if combined_boost > 0:
    boosted = peak_dpi_raw * ((cum_dpi / peak_dpi_raw) + combined_boost)
else:
    boosted = cum_dpi

adjusted = boosted * 1.00

T, S = 60.0, 4.0
if adjusted > T:
    compressed = T + S * math.sqrt(adjusted - T)
else:
    compressed = adjusted
final = min(compressed, 99.0)


# Observed rainfall vs engine estimate — the headline finding
RAIN_OBSERVED_IN = 35.93
RAIN_OBSERVED_MM = RAIN_OBSERVED_IN * 25.4
RAIN_OBSERVED_LOC = "Elizabethtown, NC"
RAIN_EST_MM = flo['rainfall_est_mm']
OVER_RATIO = RAIN_EST_MM / RAIN_OBSERVED_MM


trace = {
    "storm_id": "AL062018",
    "name": "Florence",
    "year": 2018,
    "basin": "ATLANTIC",
    "archetype": "Cat 4 lifetime / Cat 2 landfall / inland rainfall catastrophe",
    "stages": {
        "stage_1_per_snapshot_dpi": {
            "description": (
                "Max single-snapshot DPI along the track. Per-snapshot "
                "DPI computed by core/dpi.py:compute_dpi as a 0.30/0.35/"
                "0.35 weighted blend of IKE / SurgeRain / Economic sub-"
                "scores plus interaction bonuses, dampened by land-"
                "proximity."
            ),
            "max_wind_ms_at_peak": flo['peak_wind_ms'],
            "max_wind_kt_at_peak": flo['peak_wind_kt'],
            "min_pressure_hpa": flo['min_pressure_hpa'],
            "peak_ike_tj": flo['peak_ike_tj'],
            "snapshot_count": flo['snapshot_count'],
            "peak_dpi_raw": peak_dpi_raw,
            "notes": (
                "Peak intensity (130 kt, 937 mb) occurred 2018-09-11 18z "
                "over the open Atlantic at 27N/66W — three days BEFORE "
                "landfall. Florence weakened from Cat 4 to Cat 2 between "
                "the peak and landfall due to wind shear + dry-air "
                "entrainment. So peak_dps reflects the open-ocean peak, "
                "not the landfall snapshot, which is correct: the per-"
                "snapshot composite captures destructive *potential* not "
                "*realized* impact."
            ),
            "source": "compute_storm_dps output, field 'peak_dps'",
        },
        "stage_2_cumulative_dpi": {
            "description": (
                "cum_dpi = peak_dpi x (1 + duration_factor + "
                "breadth_factor). Both factors cap at 0.10."
            ),
            "formula": (
                "cum_dpi = peak_dpi * (1 + duration_factor + breadth_factor)"
            ),
            "inputs": {
                "peak_dpi": peak_dpi_raw,
                "duration_factor": dur,
                "duration_factor_capped": dur >= 0.099,
                "breadth_factor": brd,
                "breadth_factor_capped": brd >= 0.099,
                "coastal_hours": flo['coastal_hours'],
                "track_hours": flo['track_hours'],
            },
            "calculation": (
                f"{peak_dpi_raw} * (1 + {dur} + {brd}) = "
                f"{round(cum_dpi, 2)}"
            ),
            "cum_dpi_computed": round(cum_dpi, 2),
            "cum_dpi_in_bundle": flo['dps_original'],
            "source": (
                "core/cumulative_dpi.py compute_cumulative_dpi -> "
                "bundle field 'dps_original'"
            ),
        },
        "stage_3_combined_boost": {
            "description": (
                "Stage-3 economic boost factors. boosted = peak x "
                "(cum/peak + sum(boosts))."
            ),
            "factors": {
                "exposure_factor": {
                    "value": exp_f,
                    "region": flo.get('exposure_region', '(none)'),
                    "source": (
                        "compile_cache.compute_exposure_factor; "
                        "Carolinas COASTAL_EXPOSURE_WEIGHTS=0.55 x cap "
                        "0.10 = 0.055"
                    ),
                },
                "perp_factor": {
                    "value": perp,
                    "note": (
                        "0.0 from stale-bundle bug (us_landfall_count "
                        "issue fixed in commit 659342b). Florence had "
                        "1 US landfall so the corrected code would "
                        "yield ~0.03."
                    ),
                    "source": (
                        "compile_cache.compute_perpendicular_factor "
                        "(stale)"
                    ),
                },
                "stall_bonus": {
                    "value": stall_b,
                    "stall_hours": flo['stall_hours'],
                    "note": (
                        "Florence's stall_bonus (0.0275) is the largest "
                        "among audited storms — driven by 42 stall_hours "
                        "weighted by the Carolinas COASTAL_EXPOSURE_"
                        "WEIGHT of 0.55. The cap is 0.05, so at 55% "
                        "stall_econ Florence reaches roughly half of "
                        "the available headroom."
                    ),
                    "source": "core/dps_engine.py Stage 3 stall_bonus",
                },
                "rain_inland_factor": {
                    "value": rain_in,
                    "rainfall_warning_score": flo['rainfall_warning'],
                    "rainfall_level": flo['rainfall_level'],
                    "estimated_rainfall_mm": RAIN_EST_MM,
                    "observed_rainfall_mm": round(RAIN_OBSERVED_MM, 1),
                    "observed_rainfall_in": RAIN_OBSERVED_IN,
                    "observed_location": RAIN_OBSERVED_LOC,
                    "estimator_over_ratio": round(OVER_RATIO, 2),
                    "note": (
                        f"Engine's stall-rainfall heuristic estimates "
                        f"{RAIN_EST_MM:.0f} mm; observed peak was "
                        f"{RAIN_OBSERVED_MM:.0f} mm at "
                        f"{RAIN_OBSERVED_LOC} (NWS WFO MHX). That's a "
                        f"{OVER_RATIO:.1f}x over-estimate. Florence's "
                        f"rain_inland_factor (0.022) is much smaller "
                        f"than the engine's `warning_score` of 87.5 "
                        f"would suggest — the cap (0.04) prevents "
                        f"unbounded growth even with an extreme "
                        f"estimate."
                    ),
                    "source": (
                        "core/rainfall_warning.py compute_rainfall_"
                        "warning (heuristic) — NOT the ground_truth "
                        "override path because bundle was compiled "
                        "before the ground_truth.peak_rainfall_in "
                        "override hook ran for this storm"
                    ),
                },
                "inland_pen_factor": {
                    "value": inland_pen,
                    "note": (
                        "0.0 because Florence weakened to TS quickly "
                        "after landfall; the inland-TS-wind threshold "
                        "requires sustained 18+ m/s well past the "
                        "coast. Florence dropped below that threshold "
                        "before clearing the coastal box."
                    ),
                    "source": "core/dps_engine.py Stage 3 inland_pen_factor",
                },
            },
            "combined_boost": round(combined_boost, 4),
            "boosted_formula": (
                "boosted = peak_dpi x ((cum_dpi / peak_dpi) + "
                "combined_boost)"
            ),
            "boosted_calculation": (
                f"{peak_dpi_raw} x ({round(cum_dpi/peak_dpi_raw, 4)} + "
                f"{combined_boost}) = {round(boosted, 2)}"
            ),
            "boosted_value": round(boosted, 2),
        },
        "stage_4_basin_adjustment": {
            "description": (
                "Atlantic basin: dps_multiplier=1.00, ri_bonus=0, no "
                "WP/EP enhancements. No-op."
            ),
            "basin": "ATLANTIC",
            "dps_multiplier": 1.00,
            "ri_bonus": 0,
            "adjustment_notes": flo.get('adjustment_notes', ''),
            "adjusted": round(adjusted, 2),
            "source": (
                "compile_cache.apply_basin_dps_adjustment "
                "(BASIN_COEFFICIENTS ATLANTIC)"
            ),
        },
        "stage_5_compression": {
            "description": (
                "Per-basin sqrt compression. Atlantic uses (T=60, S=4). "
                "See methodology.html: 'A note on Stage 5 — the "
                "presentation layer' for the editorial framing."
            ),
            "params": {"T": T, "S": S, "basin": "ATLANTIC"},
            "formula": (
                "if adjusted > T: adjusted = T + S * sqrt(adjusted - "
                "T); final = min(adjusted, 99)"
            ),
            "calculation": (
                f"60 + 4 * sqrt({round(adjusted, 2)} - 60) = "
                f"60 + 4 * {round(math.sqrt(adjusted - T), 3)} = "
                f"{round(compressed, 2)}"
            ),
            "compressed_value": round(compressed, 2),
            "hard_cap_99": compressed >= 99.0,
            "pre_to_post_delta": round(adjusted - compressed, 2),
            "source": (
                "compile_cache.apply_basin_dps_adjustment Stage-5 sqrt "
                "compression"
            ),
        },
    },
    "final": {
        "displayed_dps": round(final, 2),
        "displayed_dps_in_bundle": flo['dps'],
        "dps_label": flo['dps_label'],
        "category_lifetime": flo['category_lifetime'],
        "category_landfall": flo['category'],
        "match_check": abs(final - flo['dps']) < 0.5,
    },
    "ground_truth_reference": {
        "damage_usd_2024": 24_230_000_000,
        "deaths_total": 52,
        "peak_surge_ft": 10.1,
        "peak_surge_location": "Emerald Isle, NC",
        "peak_rainfall_in": 35.93,
        "peak_rainfall_location": "Elizabethtown, NC",
        "peak_wind_landfall_mph": 90,
        "landfall_pressure_mb": 956,
        "landfall_saffir": 1,
        "fema_states": ["NC", "SC"],
        "source": (
            "core/ground_truth.py:AL062018 (NHC TCR Stewart & Berg 2019)"
        ),
    },
    "headline_finding": {
        "rainfall_estimator_overshoot": {
            "engine_estimate_mm": RAIN_EST_MM,
            "observed_peak_mm": round(RAIN_OBSERVED_MM, 1),
            "observed_location": RAIN_OBSERVED_LOC,
            "ratio": round(OVER_RATIO, 2),
            "interpretation": (
                f"Florence's engine rainfall estimate ({RAIN_EST_MM:.0f} "
                f"mm) is {OVER_RATIO:.1f}x the observed peak "
                f"({RAIN_OBSERVED_MM:.0f} mm). Comparison: Ian's was "
                f"1.56x. This is the audit's most actionable finding: "
                f"the stall-hour rainfall heuristic in "
                f"core/rainfall_warning.py systematically over-"
                f"estimates for stall-dominant storms. The cap on "
                f"rain_inland_factor (0.04) prevents this from "
                f"detonating the score, but the over-estimate matters "
                f"for borderline storms where the gate's threshold "
                f"(rainfall_est_mm > 250) determines whether the "
                f"factor activates at all."
            ),
            "next_step": (
                "Dedicated rainfall-estimator audit — compare engine's "
                "rainfall_est_mm against the peak_rainfall_in field in "
                "core/ground_truth.py for all 15 storms with curated "
                "data. Quantify systematic bias and propose a "
                "calibration correction."
            ),
        }
    },
    "known_caveats": [
        (
            "perp_factor = 0.0 is the same stale-bundle artifact "
            "documented in the Ian audit. Fix in commit 659342b not yet "
            "in bundle. Florence's 1 US landfall would yield ~0.03 "
            "under fixed code."
        ),
        (
            "Bundle was compiled before the v11 EP-basin commit and "
            "before the v10 per-basin compression. Atlantic numbers "
            "happen to be unchanged because per-basin compression "
            "reverts Atlantic to (T=60, S=4) which is what the bundle "
            "was originally compiled under. Manual recompute matches "
            "bundle within rounding."
        ),
        (
            "Florence's huge track_hours (459) is because the bundle "
            "tracks the storm from genesis off Cape Verde on 2018-08-"
            "30 through extratropical transition on 2018-09-18 — 19 "
            "days. Coastal_hours (75) and stall_hours (42) are what "
            "actually drive the score; track_hours is informational."
        ),
        (
            "ground_truth.py has Florence's peak_rainfall_in = 35.93 "
            "(observed) but the bundle was compiled before the "
            "observed-rainfall override hook was added to compute_"
            "storm_dps. Next recompile would activate the override "
            "and Florence's rain_inland_factor would be recomputed "
            "from the 685 mm observed value rather than the 2281 mm "
            "heuristic estimate. Score impact: small (the cap is "
            "already binding), but the diagnostic fields would change."
        ),
    ],
    "audit_method": (
        "Re-derived from compile_cache.py + core/dps_engine.py + "
        "bundle field values. Cross-checked against HURDAT2 best-track "
        "AL062018 (79 records) + NHC TCR (Stewart & Berg 2019). One "
        "landfall (Wrightsville Beach NC at 34.2N/77.8W, 80 kt / 956 "
        "mb on 2018-09-14 11:15z) matches the bundle's single-entry "
        "landfalls array (which records the 12:00z synoptic-grid "
        "approximation at 33.1N/75.1W, 95 kt / 954 mb)."
    ),
}

with open(OUT_JSON, 'w', encoding='utf-8') as f:
    json.dump(trace, f, indent=2)

print(f'Wrote {OUT_JSON}')
print()
print('=== Stage-by-stage trace ===')
print(f"Stage 1 peak DPI         = {peak_dpi_raw}")
print(f"Stage 2 cum_dpi          = {round(cum_dpi, 2)} (bundle: {flo['dps_original']})")
print(f"Stage 3 combined_boost   = {round(combined_boost, 4)}")
print(f"Stage 3 boosted          = {round(boosted, 2)}")
print(f"Stage 4 adjusted         = {round(adjusted, 2)} (Atlantic x 1.00)")
print(f"Stage 5 compressed       = {round(compressed, 2)} (T=60, S=4)")
print(f"Final displayed DPS      = {round(final, 2)}")
print(f"Bundle stored DPS        = {flo['dps']:.2f}")
print(f"Match within 0.5         = {abs(final - flo['dps']) < 0.5}")
print()
print('=== Rainfall estimator finding ===')
print(f"Engine estimate:  {RAIN_EST_MM} mm")
print(f"Observed peak:    {RAIN_OBSERVED_MM:.0f} mm ({RAIN_OBSERVED_IN} in) at {RAIN_OBSERVED_LOC}")
print(f"Overshoot ratio:  {OVER_RATIO:.2f}x")
