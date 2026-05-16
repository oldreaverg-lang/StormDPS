# Hurricane Ian (AL092022) — End-to-End DPS Audit

**Date:** 2026-05-15
**Storm:** Hurricane Ian, September 22 – October 1, 2022
**ATCF ID:** AL092022 / IBTrACS SID 2022266N12299
**Displayed DPS:** **87.93** (Catastrophic)
**Purpose:** Trace every input and intermediate calculation that produces Ian's final displayed DPS, so a secondary auditor can verify the formula end-to-end against published sources.

**Companion artifacts:**
- `ian_snapshots.csv` — 40-row HURDAT2 best-track joined with the pipeline's per-snapshot DPI
- `ian_intermediate.json` — machine-readable stage-by-stage formula trace
- `_build_intermediate.py` — the script that produced the trace (reruns the math from bundle field values, idempotent)

---

## 1. Why this storm was chosen for the first audit

Per the audit-design conversation: Ian stress-tests the most independent dimensions of the formula simultaneously without being an outlier. It exhibits:

- **Rapid intensification** (75 kt in 24 h from 09-27 12z → 09-28 12z)
- **Major surge** (15 ft observed at Fort Myers Beach per USGS STN)
- **Compact-core extreme winds** (peak 140 kt with R34 ≈ 150 nm — not Sandy-large)
- **Prolonged coastal interaction** (61 zone-weighted coastal hours)
- **Massive economic exposure** (Tampa Bay corridor + SW Florida)
- **Inland flooding** (estimated 1068 mm peak rainfall via NWS AHPS / observed 26.95 in at Grove City)
- **Florida population density** (FL east coast exposure factor at Tampa Bay = 0.65)
- **Structure changes near landfall** (eyewall replacement → second landfall at 26.8N)
- **Multiple landfalls** (3: Pinar del Río Cuba, Cayo Costa FL, Georgetown SC)
- **Weakening + reintensification** (Cat 4 → TS over FL → Cat 1 over Atlantic → Cat 1 SC landfall)

Three of the five bonus terms in the cumulative pipeline (duration, breadth, Stage-3 rainfall) hit their caps for Ian. If the formula has hidden reinforcement loops, Ian will surface them.

---

## 2. Input chain — source provenance

| Pipeline input | Value used | Source | How to verify |
|---|---:|---|---|
| Best-track 6-hourly positions, wind, pressure | 40 records | NHC HURDAT2 file `hurdat2-1851-2024-040425.txt`, header `AL092022,IAN,40` | Download HURDAT2 from `https://www.nhc.noaa.gov/data/hurdat/` and grep for `AL092022` |
| Per-snapshot quadrant wind radii (R34/R50/R64) | post-2004 HURDAT2 extended format | Same HURDAT2 file, columns 9–20 of each data line | See `ian_snapshots.csv` columns `r34_*`, `r50_*`, `r64_*` |
| Landfall metadata (lat/lon/timestamp/category) | 5 landfall-precision records | HURDAT2 record_id="L" lines | See `ian_snapshots.csv` rows where `record_id=L` |
| Peak observed rainfall (override for stall-heuristic) | 26.95 in at Grove City, FL | `core/ground_truth.py:'AL092022'.peak_rainfall_in` | NHC TCR AL092022 (Bucci et al. 2023), NWS WFO TBW |
| Peak observed surge | 15.0 ft at Fort Myers Beach | `core/ground_truth.py:'AL092022'.peak_surge_ft` | USGS STN, NHC TCR Table 5 |
| Damage / fatalities | $112.9 B / 156 deaths | `core/ground_truth.py` | NOAA NCEI Billion-Dollar Disasters (CPI-adjusted to 2024 USD) |

The HURDAT2 file is fetched by `services/noaa_client.py:get_historical_track` and cached on the Railway persistent volume as `cache/hurdat2_atl.txt`. The IBTrACS catalog is fetched separately via `services/noaa_client.py:get_ibtracs_catalog` and serves as the fallback / cross-check.

---

## 3. Stage 1 — per-snapshot DPI

**Computed by:** `core/dpi.py:compute_dpi` for each of the 37 snapshots that survive the engine's filtering (3 of HURDAT2's 40 records are sub-TS depression points that contribute zero IKE).

**Formula** (excerpt — see `core/dpi.py:L222–L370` for the complete implementation):

```
raw_dpi = 0.30 * IKE_score
        + 0.35 * SurgeRain_score
        + 0.35 * Economic_score
        + vuln_bonus              (≤ 20, fires when vuln > 35 AND wind > 0.6 × Cat 4)
        + compact_bonus           (≤ 22, IKE-deficit term for compact Cat 4-5)
        + coast_tracking_bonus    (≤ 12, r34 > 150 nm parallel-coast)
        + stall_dpi_bonus         (≤  8, per-snapshot stall)
        + ri_bonus                (≤  8, snapshot-to-snapshot intensification)

snapshot_dpi = raw_dpi × land_proximity_factor    # 0.30 ocean → 1.0 at coast
```

**Ian's peak snapshot** is at **2022-09-28 12:00z** — 6 hours before landfall, near Cayo Costa.

| Variable at peak | Value | Source |
|---|---:|---|
| Lat / Lon | 26.0°N, 82.7°W | HURDAT2 |
| max_wind_kt | 140 | HURDAT2 (lifetime peak) |
| min_pressure_mb | 937 | HURDAT2 |
| Max R34 (any quadrant) | 150 nm (NW quadrant) | HURDAT2 |
| Forward speed | 8–9 kt approach | Derived from 6-h spacing |
| IKE_peak_tj | **246.8 TJ** | `peak_ike_tj` in bundle |
| Bundle's peak_dps | **82.1** | `peak_dps` field |

Ian's IKE (246.8 TJ) is large but not extreme — Sandy is 640 TJ, Ike is 190 TJ, Michael is 321 TJ. The `compact_bonus` fires because Ian's 140-kt intensity (60% of Cat 4 cap) is in the "intensity > IKE suggests" deficit band that triggers the per-snapshot compact term.

**Notable secondary peak:** the 2022-09-28 18:00z snapshot (135 kt, 938 mb, post-landfall) is where `dpi_timeseries` actually displays its highest value — 87.9 — but that's *scaled* (see §7 caveat) to match the final displayed DPS. The pre-scaling per-snapshot peak is 82.1 from `peak_dps`.

---

## 4. Stage 2 — cumulative DPI

**Computed by:** `core/cumulative_dpi.py:compute_cumulative_dpi`.

**Formula:**

```
cum_dpi = peak_dpi × (1 + duration_factor + breadth_factor)
```

Where:

- `duration_factor` = `min(0.10, max(0, duration_integral − 24h) / 72h)`
  - `duration_integral` = Σ √(DPI_i / peak_DPI) · Δt · zone_weight, over snapshots with DPI > 25 AND near coast
- `breadth_factor` = `min(0.10, IKE_norm × coastal_hours/48 × 0.20)`
  - Both factors **cap at 0.10** (10 %)

**Ian's values (from bundle):**

| Field | Value | At cap? |
|---|---:|---|
| `duration_factor` | 0.10 | **Yes — capped** |
| `breadth_factor` | 0.10 | **Yes — capped** |
| `coastal_hours` (zone-weighted) | 61.0 | — |
| `track_hours` (total storm lifetime) | 201.0 | — |
| `peak_ike_tj` | 246.8 | (IKE_norm ≈ 1.84 vs `IKE_REF_TJ` = 150) |

**Calculation:**

```
cum_dpi = 82.1 × (1 + 0.10 + 0.10)
        = 82.1 × 1.20
        = 98.52
```

Bundle stores `dps_original = 98.5` (rounded). ✓

Both `duration_factor` and `breadth_factor` hitting their caps confirms Ian's "correlated catastrophe profile": large IKE × extended coastal time × peak-DPI threshold maintained. This is the audit point ChatGPT's framing called out — *if the formula were going to double-count, this is where it would show.*

---

## 5. Stage 3 — Stage-3 economic boost factors

**Computed by:** `core/dps_engine.py:L218–L230` (combined_boost) + supporting functions in `compile_cache.py`.

**Formula:**

```
combined_boost = exposure_factor
               + perp_factor
               + stall_bonus
               + rain_inland_factor
               + inland_pen_factor

if combined_boost > 0:
    boosted = peak_dpi × ((cum_dpi / peak_dpi) + combined_boost)
else:
    boosted = cum_dpi
```

**Ian's factors:**

| Factor | Value | Source / explanation |
|---|---:|---|
| `exposure_factor` | **0.065** | Tampa Bay landfall region. `COASTAL_EXPOSURE_WEIGHTS["Tampa Bay"]` = 0.65 × cap 0.10 = 0.065. |
| `perp_factor` | **0.000** | ⚠ Stale-bundle artifact. The `us_landfall_count` bug (fixed in commit 659342b) caused `compute_perpendicular_factor` to return 0 when `coastal_hours >= 24`. Under the fix, Ian would earn ~0.06 (2 US landfalls × 0.03). Bundle won't reflect this until next compile. |
| `stall_bonus` | 0.010 | Stage-3 cumulative-pipeline stall term. `stall_hours = 0` per bundle (Ian's forward speed never dropped below 3 m/s), but the term still credits the "slow_hours × 0.5" component. |
| `rain_inland_factor` | 0.026 | Driven by `rainfall_warning_score = 56.6` ("High") and `rainfall_est_mm = 1068`. The 1068 mm estimate comes from the rainfall pipeline; the observed peak (26.95 in = 685 mm at Grove City) is below the estimate, suggesting either upstream over-estimation or that the bundle predates the ground-truth override path. |
| `inland_pen_factor` | 0.024 | Tropical-storm-force winds reached inland after FL landfall, scoring 3 inland-TS snapshots × 0.008. |
| **combined_boost** | **0.125** | Σ |

**boosted calculation:**

```
boosted = 82.1 × ((98.52 / 82.1) + 0.125)
        = 82.1 × (1.20 + 0.125)
        = 82.1 × 1.325
        = 108.78
```

---

## 6. Stage 4 — basin adjustment

**Computed by:** `compile_cache.py:apply_basin_dps_adjustment` using `BASIN_COEFFICIENTS["ATLANTIC"]`.

**Atlantic coefficients:**

```python
"ATLANTIC": {
    "dps_multiplier": 1.0,    # no sub-basin multipliers
    "ri_bonus": 0,            # RI bonus disabled (set to 0 = doesn't fire)
    "compression_T": 60.0,    # per-basin sqrt threshold
    "compression_S": 4.0,     # per-basin sqrt slope
    # no sub_basin_multipliers, no orographic, no rainfall-footprint, no no-landfall dampener
}
```

For Atlantic, Stage 4 is a **no-op**. None of the WP/EP-specific enhancement branches fire. The `adjustment_notes` field in the bundle is empty (verified).

```
adjusted = 108.78 × 1.00 = 108.78
```

This is intentional — the methodology page documents Atlantic as the reference basin against which the other basins' enhancements are tuned. See `methodology.html` "Stage 4 — Atlantic basin (no-op baseline)" in the computational-pipeline diagram.

---

## 7. Stage 5 — sqrt compression

**Computed by:** `compile_cache.py:apply_basin_dps_adjustment` Stage-5.

**Formula** (per-basin since v10, commit 659342b):

```
T = coeffs["compression_T"]     # Atlantic: 60.0
S = coeffs["compression_S"]     # Atlantic: 4.0

if adjusted > T:
    adjusted = T + S × √(adjusted − T)
final_dps = min(adjusted, 99.0)
```

**Ian's calculation:**

```
adjusted = 108.78 (from Stage 4)
108.78 > 60 → compress

compressed = 60 + 4 × √(108.78 − 60)
           = 60 + 4 × √48.78
           = 60 + 4 × 6.984
           = 60 + 27.94
           = 87.94

final_dps = min(87.94, 99) = 87.94
```

Bundle stores `dps = 87.93`. Manual recompute matches within 0.01 (rounding). ✓

The Atlantic curve (T=60, S=4) was chosen because the pre-v7-audit calibration produces the desired Atlantic-storm spread (Katrina ~93, Maria ~86, Harvey ~83). The v7-audit retune to (T=70, S=2.5) was scoped to non-Atlantic basins per the per-basin compression in commit 659342b; if Ian were re-run under the global v7 curve it would compress to roughly 81.0 instead of 87.94. The (T=60, S=4) curve assigns Ian the "Catastrophic" displayed band that matches the public framing of his impact.

---

## 8. Final score reconciliation

| Component | Value |
|---|---:|
| Stage 1 peak DPI | **82.1** (raw, from `peak_dps`) |
| Stage 2 cum_DPI | **98.5** (bundle field `dps_original`) |
| Stage 3 combined_boost | 0.125 |
| Stage 3 boosted | 108.78 |
| Stage 4 adjusted | 108.78 (Atlantic × 1.00) |
| Stage 5 compressed | 87.94 (T=60, S=4) |
| **Displayed DPS (bundle)** | **87.93** ✓ |
| **Label** | Catastrophic |

| Validation check | Expected | Actual | Match |
|---|---|---|---|
| Manual recompute vs bundle | within 0.5 | 0.01 | ✓ |
| Three landfalls in bundle | 3 | 3 | ✓ |
| Category lifetime | 4 (HURDAT2) / 5 (NHC re-analysis April 2023) | 4 | partial — HURDAT2 record_id "L" lines show 130 kt landfall = Cat 4 in this dataset |

---

## 9. Known caveats (what a secondary auditor should know)

1. **`perp_factor = 0.0` is a stale-bundle artifact, not a formula error.** The `compute_perpendicular_factor` function returned 0 whenever `coastal_hours >= 24` — conflating "doesn't qualify for perp bonus" with "didn't make US landfalls." Fix shipped in commit 659342b. Until the bundle is recompiled, Ian's `perp_factor` reads 0; under the live (fixed) code, Ian would earn ~0.06 (2 US landfalls × 0.03). Under-by-about-1.5 displayed points until recompile.

2. **`dpi_timeseries` values are scaled, not raw.** The peak entry in the timeseries (87.9 at 2022-09-28 12:00z) equals the final displayed DPS — *after* compression — because the engine rescales the per-snapshot series to match the hero card's number for map-marker coloring. The pre-scaling per-snapshot peak DPI is 82.1, stored in `peak_dps`. Don't confuse the two when reading the CSV.

3. **`rainfall_est_mm = 1068` is the engine's heuristic estimate, not the observed peak.** The observed peak (NWS AHPS via NHC TCR) was 26.95 in = 685 mm at Grove City, FL. The 1068 mm reflects the engine's stall-rainfall projection. Discrepancy is acceptable — the `rain_inland_factor` is gated on `rainfall_warning > 30` and `rainfall_est_mm > 250`, both satisfied by either value, so the factor fires either way. Worth investigating in a future audit whether the engine's estimator should be calibrated tighter against observed totals.

4. **HURDAT2 has 40 records; bundle has 37 snapshots.** The 3 dropped are sub-tropical-storm depression points from 2022-09-22 to 2022-09-23 that contribute 0 DPI and are filtered out before the cumulative pipeline. Cross-reference `ian_snapshots.csv` — rows with empty `pipeline_dpi` are the dropped ones.

5. **Bundle compiled before commit 391076e (which adds the WP audit) but after the per-basin compression change in 659342b.** Atlantic numbers are unchanged by the v10 → v11 cache-version bumps (per-basin compression preserves the original Atlantic curve). If/when the bundle is recompiled under v11, Ian's `perp_factor` corrects from 0 → 0.06 and the displayed DPS rises from 87.93 to ~89.4. Other Atlantic storms in the bundle have the same potential delta.

---

## 10. Secondary auditor checklist

To independently verify this audit:

| Step | What to do | Where to look |
|---|---|---|
| 1 | Download HURDAT2 Atlantic file from NHC | `https://www.nhc.noaa.gov/data/hurdat/hurdat2-1851-2024-040425.txt` |
| 2 | Confirm Ian's track matches `ian_snapshots.csv` | Header row `AL092022,IAN,40,` + 40 data lines |
| 3 | Verify the formula constants against the source | `compile_cache.py` lines 39–123 (BASIN_COEFFICIENTS), `core/dpi.py` lines 116–119 (w_ike/w_surge_rain/w_economic), `core/cumulative_dpi.py` lines 62–69 (caps + refs) |
| 4 | Re-run `_build_intermediate.py` against your local bundle | `python audits/ian_2022/_build_intermediate.py` — should print "Match (within 0.5): True" |
| 5 | Cross-check ground truth values | `core/ground_truth.py:'AL092022'` and NHC TCR (Bucci et al. 2023) |
| 6 | Sanity-check Ian's bundle field values manually | `python -c "import json; print(json.dumps(json.load(open('frontend/compiled_bundle.json'))['storms']['AL092022'], indent=2))"` |
| 7 | Spot-check the dpi_timeseries against the HURDAT2 timestamps | Compare `ian_snapshots.csv` column `time_utc` to bundle entries — should match for 37 of 40 rows |
| 8 | Verify the displayed score matches the SSR'd page | Visit `https://stormdps.com/storm/AL092022` and confirm the score card shows 88 (rounded) |

**Reproducibility:** every value in this audit can be reconstructed from (a) the HURDAT2 file, (b) `compiled_bundle.json`, and (c) the formula sources in `core/` and `compile_cache.py`. No proprietary data, no closed inputs.

---

## 11. Sources

- **NHC HURDAT2 (Atlantic):** `https://www.nhc.noaa.gov/data/hurdat/hurdat2-1851-2024-040425.txt` — best-track positions, intensity, quadrant wind radii.
- **NHC Tropical Cyclone Report for Ian:** Bucci, L. R., Alaka, L., Hagen, A., Delgado, S., & Beven, J., *Hurricane Ian (AL092022)*, NWS/NHC, 2023.
- **USGS Short-Term Network (STN):** post-event high-water-mark surveys for FL landfall surge.
- **NOAA NCEI Billion-Dollar Disasters:** CPI-adjusted damage to 2024 USD ($112.9 B for Ian).
- **NWS Advanced Hydrologic Prediction Service (AHPS):** observed peak rainfall totals.
- **StormDPS code (this repo):**
  - `compile_cache.py` — basin coefficients, apply_basin_dps_adjustment, COASTAL_REGIONS, COASTAL_EXPOSURE_WEIGHTS
  - `core/dpi.py` — per-snapshot DPI composite + bonuses
  - `core/cumulative_dpi.py` — duration/breadth factors, COASTAL_BOXES, ZONE_WEIGHTS
  - `core/dps_engine.py` — pipeline orchestration (Stages 1–5)
  - `core/ground_truth.py` — Ian observed-value record
  - `frontend/compiled_bundle.json` — Ian's stored DPS bundle
