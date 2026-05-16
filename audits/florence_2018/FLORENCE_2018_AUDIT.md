# Hurricane Florence (AL062018) — End-to-End DPS Audit

**Date:** 2026-05-15
**Storm:** Hurricane Florence, August 30 – September 18, 2018
**ATCF ID:** AL062018 / IBTrACS SID 2018242N12348
**Displayed DPS:** **75.89** (Devastating)
**Purpose:** Second storm-level audit following Ian (AL092022). Florence is the inverse-archetype — modest peak intensity at landfall (Cat 1/2), Cat 4 lifetime, and the worst inland-rainfall catastrophe in North Carolina history. Stress-tests the rainfall-flood pathway specifically.

**Companion artifacts:**
- `florence_snapshots.csv` — 79-row HURDAT2 best-track joined with the pipeline's per-snapshot DPI
- `florence_intermediate.json` — machine-readable stage-by-stage formula trace
- `_build_florence_audit.py` — reproduces both files from bundle field values (idempotent)

---

## 1. Why Florence as the second audit

After Ian validated the peak-major-hurricane scoring, Florence tests the opposite archetype:

- **Cat 4 lifetime but Cat 1/2 at landfall** — the canonical Saffir-Simpson misalignment case
- **Rainfall-dominant disaster** ($24 B damage, 52 deaths, almost entirely from inland flooding)
- **Stalled** — 42 hours below 3 kt forward speed, the bundle's `stall_hours` field maxes
- **Modest IKE** (218.8 TJ vs Ian's 246.8 — similar magnitude)
- **Modest surge** (10.1 ft observed at Emerald Isle vs Ian's 15.0 ft at Fort Myers Beach)
- **No compact-Cat-5 fingerprint** — Florence weakened gradually, no extreme RI window at landfall

If the formula systematically underweights rainfall-dominant catastrophes, Florence is where it'll show.

---

## 2. Input chain — source provenance

| Pipeline input | Value used | Source |
|---|---:|---|
| Best-track 6-hourly positions, wind, pressure | 79 records | NHC HURDAT2 file, `AL062018,FLORENCE,79,` header |
| Quadrant wind radii (R34/R50/R64) per snapshot | post-2004 HURDAT2 extended format | Same file, columns 9–20 of each data line |
| Landfall metadata | 1 record_id="L" entry | HURDAT2 `2018-09-14 11:15z, 34.2N/77.8W, 80 kt/956 mb` |
| Peak observed rainfall | 35.93 in at Elizabethtown, NC | `core/ground_truth.py:'AL062018'.peak_rainfall_in` |
| Peak observed surge | 10.1 ft at Emerald Isle, NC | `core/ground_truth.py:'AL062018'.peak_surge_ft` |
| Damage / fatalities | $24.23 B (2024 USD) / 52 deaths | `core/ground_truth.py` (NOAA NCEI) |

NHC TCR: Stewart, S. R., & Berg, R., *Hurricane Florence (AL062018)*, NWS/NHC, 2019.

---

## 3. Stage 1 — per-snapshot peak DPI = **58.1**

The peak occurred at **2018-09-11 18:00z, 27.2°N / 66.4°W** — three days *before* landfall, over the open Atlantic. Florence weakened from Cat 4 (130 kt) at this peak to Cat 1 (80 kt) at landfall due to wind shear and dry-air entrainment over the western Atlantic.

| Variable at peak | Value | Source |
|---|---:|---|
| Lat / Lon | 27.2°N, 66.4°W | HURDAT2 record_id=`` |
| max_wind_kt | 130 | HURDAT2 (lifetime peak — Cat 4) |
| min_pressure_mb | 937 | HURDAT2 |
| Max R34 (any quadrant) | 150 nm (NE) | HURDAT2 |
| peak_ike_tj | 218.8 | bundle |
| Bundle's peak_dps | **58.1** | bundle |

This is **substantially below Ian's 82.1** even though Florence's peak wind (130 kt) was within a few knots of Ian's lifetime peak (140 kt). The gap reflects the per-snapshot composite's emphasis on land-proximity dampening: Florence's peak was at 27N/66W — ~700 nm from the US East Coast — and the `land_proximity_factor` applies a heavy multiplier (~0.30) to the surge/economic sub-scores at that distance. Ian's peak was 6 hours before landfall, on the SW Florida shelf, with `land_proximity_factor ≈ 1.0`.

So Florence's "peak" reflects what the wind field *could* have done if it had been near land — heavily discounted because it wasn't.

---

## 4. Stage 2 — cumulative DPI = **69.72** (bundle: 69.7)

Both `duration_factor` and `breadth_factor` hit their **0.10 caps**:

```
cum_dpi = 58.1 × (1 + 0.10 + 0.10) = 58.1 × 1.20 = 69.72
```

| Input | Value | Notes |
|---|---:|---|
| `duration_factor` | 0.10 (capped) | Driven by Σ √(DPI/peak) · Δt · zone_weight over 79 snapshots × 19 days |
| `breadth_factor` | 0.10 (capped) | IKE_norm · coastal_hours/48 · 0.20 — Florence's 75 coastal hours × IKE 218.8 saturates the cap |
| `coastal_hours` (zone-weighted) | 75.0 | 22% higher than Ian's 61 |
| `track_hours` (lifetime) | 459.0 | 19-day track from Cape Verde to NY — Ian's was 201 hours / 8 days |

Cumulative gives Florence a 20% lift on the peak, same as Ian — but Ian's peak was 82.1 so a 20% lift means a lot more in absolute points. **The cumulative pipeline cannot compensate for Florence's depressed Stage-1 peak.** Whatever the bonus stack does, it can't push Florence above her ~70 base ceiling.

---

## 5. Stage 3 — combined_boost = **0.1045**

| Factor | Value | Notes |
|---|---:|---|
| `exposure_factor` | 0.055 | Carolinas zone weight = 0.55 × cap 0.10 |
| `perp_factor` | **0.000** | ⚠ Stale-bundle artifact (us_landfall_count bug fixed in commit 659342b). Under fixed code: ~0.03 |
| `stall_bonus` | **0.0275** | Largest in any audited storm. Driven by 42 stall_hours × 0.01 + slow_hours × 0.005, weighted by Carolinas econ 0.55, capped at 0.05 |
| `rain_inland_factor` | 0.022 | Capped at 0.04; engine's rainfall_warning_score = 87.5 ("Historic") would push this to the cap, but the gate scales by exposure (Carolinas 0.55 × max) |
| `inland_pen_factor` | **0.000** | Florence weakened to TS quickly post-landfall — sustained 18+ m/s winds inland didn't materialize for the required 2+ snapshots |

**boosted calculation:**

```
boosted = 58.1 × ((69.72 / 58.1) + 0.1045)
        = 58.1 × (1.20 + 0.1045)
        = 58.1 × 1.3045
        = 75.79
```

Stage 3 adds **6.07 raw points** (0.1045 × 58.1) to Florence's score. For comparison, Ian's Stage 3 added 10.26 points (0.125 × 82.1). The percentage boost is similar (~10–13%), but the absolute lift differs because Ian had a higher base.

---

## 6. Stage 4 — Atlantic basin adjustment = **× 1.00 (no-op)**

Identical to Ian's Stage 4. The Atlantic basin has `dps_multiplier=1.0, ri_bonus=0`, and no WP/EP enhancement branches fire.

```
adjusted = 75.79 × 1.00 = 75.79
```

---

## 7. Stage 5 — sqrt compression = **75.90**

```
60 + 4 × √(75.79 − 60) = 60 + 4 × √15.79 = 60 + 4 × 3.974 = 75.9
```

**Quirk worth flagging:** for Florence's pre-compression value (75.79), the Stage-5 mapping is essentially a no-op — the output (75.9) is within 0.1 of the input. Working out the breakeven for the Atlantic (T=60, S=4) curve: the compression function maps `x → 60 + 4√(x−60)`. Setting `x = 60 + 4√(x−60)` and solving gives `x = 76`. So at pre-comp ≈ 76, compression is identity; below 76 (and above 60) it slightly *increases* the value; above 76 it decreases it.

Florence at pre-comp 75.79 sits **right at this pivot point.** The "compression" carries virtually no editorial weight for storms in the 70–80 range — it only really kicks in above 80. This is a structural property of the (T=60, S=4) curve choice, and it means **Florence's displayed 75.9 is a near-direct readout of her pre-compression value**, unlike Ian (108.78 → 87.94, a 21-point editorial transform).

Bundle stores `dps = 75.89`. Manual recompute matches within 0.01. ✓

---

## 8. Final score reconciliation

| Stage | Value |
|---|---:|
| Stage 1 peak DPI | **58.1** |
| Stage 2 cum_dpi | 69.72 (bundle: 69.7) |
| Stage 3 combined_boost | 0.1045 |
| Stage 3 boosted | 75.79 |
| Stage 4 adjusted | 75.79 (Atlantic × 1.00) |
| Stage 5 compressed | 75.9 (essentially no compression effect) |
| **Displayed DPS (bundle)** | **75.89** ✓ |
| **Label** | Devastating |

| Validation check | Match? |
|---|---|
| Manual recompute vs bundle | within 0.01 ✓ |
| Single landfall in bundle | matches HURDAT2 record_id="L" entry ✓ |
| Category landfall | 1 (NHC TCR), 2 in bundle (uses 95 kt synoptic value, not 80 kt landfall L-record) — minor data-precision artifact |

---

## 9. Headline finding — rainfall estimator over-shoots **2.5× observed**

The engine's stall-hour heuristic estimates Florence's peak rainfall at **2281 mm**. The NWS-observed peak (Elizabethtown, NC; via NHC TCR Stewart & Berg 2019) was **913 mm** (35.93 in).

The engine over-estimates by **a factor of 2.5×.**

For comparison: Ian's overshoot was 1.56× (1068 mm engine / 685 mm observed). Florence is worse.

**Why Florence's overshoot is larger than Ian's:** the engine's stall-rainfall estimator multiplies `stall_hours × stall_rainfall_rate_mm_per_hour`. Florence stalled for 42 hours; Ian for 0. The estimator's rate constant is calibrated against Harvey-class generational stalls, where the rate over 100+ stall hours is physically plausible. Florence stalled for less than half that duration in a much smaller storm, and the per-hour rate doesn't appropriately decay for moderate stalls. Result: an estimated rainfall amount that's plausible for a 90-inch-rain event when the actual event was 35.93 in.

**Why this doesn't break Florence's displayed score:** the `rain_inland_factor` is capped at 0.04 and gated multiplicatively by the regional `COASTAL_EXPOSURE_WEIGHT` (Carolinas = 0.55). At any rainfall_warning_score above ~80, the factor saturates the cap. Both 2281 mm and 913 mm produce the same saturated factor, so Florence's final DPS is unchanged whether the estimator over-shoots or matches observation. Ian's audit reached the same conclusion.

**Why this matters anyway:** the `rainfall_warning_score` and `rainfall_level` ("Historic" for Florence) are displayed *user-facing values* that don't get re-derived from observed data — they reflect the engine's heuristic. Florence's stormpage will show **"Generational rainfall disaster. Multi-day flooding comparable to Harvey (2017)... Est. 2281mm (90in) rainfall"** — which over-states the actual event. The "comparable to Harvey" framing is accurate (both stalled, both rainfall-dominant) but the 90 in is 2.5× off. This is misleading for a user reading the storm card.

**Recommended next action:** dedicated rainfall-estimator audit. Cross-check engine's `rainfall_est_mm` against `peak_rainfall_in` for all 15 storms in `core/ground_truth.py`. Calibrate the stall-rainfall-rate constant downward, or add a duration-dependent decay term so moderate stalls don't extrapolate Harvey-class rates.

---

## 10. Comparison with Ian — what the two audits jointly establish

| Property | Ian (AL092022) | Florence (AL062018) |
|---|---:|---:|
| Archetype | Compact Cat 5 perpendicular landfall | Cat 4 lifetime / Cat 1-2 landfall / inland flood |
| Peak intensity at land | 140 kt at landfall | 80 kt at landfall (130 kt 3 days offshore) |
| Stage 1 peak_dps | 82.1 | **58.1** |
| Stage 2 cum_dpi | 98.5 | 69.7 |
| Stage 3 combined_boost | 0.125 | 0.1045 |
| Stage 4 adjusted (pre-comp) | 108.78 | 75.79 |
| Stage 5 compression effect | −20.8 pts | **+0.1 pts** (near no-op) |
| Final displayed DPS | 87.93 | 75.89 |
| Observed damage (2024 USD) | $112.9 B | $24.2 B |
| Observed deaths | 156 | 52 |
| Rainfall estimator overshoot | 1.56× | **2.50×** |
| Stall hours | 0 | 42 |
| Peak rainfall observed (in) | 26.95 | 35.93 |
| Compression carries editorial weight | Yes (Ian's case) | **No** (Florence's case) |

**Key methodological observations from running both audits:**

1. **The peak_dps anchor dominates the final score.** Florence's bonus stack works as designed and adds ~30% to her cum_dpi, but it can't compensate for a Stage-1 peak that's already 24 points below Ian. This is a structural property of the cumulative pipeline (peak × multiplier rather than peak + additive integration), and it means storms whose damage profile is rainfall-dominated rather than wind-dominated will systematically score lower regardless of inland flooding magnitude. **This is by design, not a bug** — DPS measures destructive *potential* anchored to peak intensity, and Florence's potential was Cat 4 (which her peak captures) even though her realized impact was Cat 1.

2. **Stage 5 compression only carries editorial weight above pre-comp ~76.** For Atlantic-storm scores in the 70–80 displayed band, the curve is effectively transparent. Compression's editorial role kicks in for the catastrophic / historic bands (80+), which is where the spacing between Katrina / Maria / Ian / Harvey is dictated by the curve choice. This is consistent with the methodology page's framing of Stage 5 as the public-facing presentation layer (added in commit 8e5e8c3).

3. **The rainfall estimator over-shoots systematically and the size of the overshoot scales with stall duration.** Ian's 0 stall hours → 1.56× overshoot; Florence's 42 stall hours → 2.50× overshoot. The third audit in this sequence — the rainfall estimator audit against all 15 ground_truth storms — should quantify the scaling relationship and recommend a calibration correction.

---

## 11. Known caveats

1. **`perp_factor = 0.0` is the stale-bundle us_landfall_count artifact.** Same as Ian. Florence's 1 US landfall would yield ~0.03 under fixed code; displayed DPS would rise from 75.89 to ~76.7 after recompile.

2. **Bundle compiled before observed-rainfall override hook.** The `ground_truth.peak_rainfall_in` field (35.93 in for Florence) exists in `core/ground_truth.py` but the override path in `compute_storm_dps` (L112–129) wasn't active when this bundle was last compiled. Next recompile activates it and `rain_result.estimated_total_mm` switches from 2281 mm to 913 mm. Score impact is small (cap is binding either way) but `rainfall_text` and `rainfall_warning_score` would change to more accurate displayed values.

3. **Category landfall = 2 in bundle vs Cat 1 in NHC TCR.** The bundle records the 12:00z synoptic-grid snapshot as landfall (95 kt → Cat 2) rather than the 11:15z record_id="L" entry (80 kt → Cat 1). Standard 6-hourly-grid artifact of the landfall detection. Not material to the score.

4. **`inland_pen_factor = 0` is correct, not a bug.** Florence weakened to TS within hours of landfall — the inland-TS-force-wind gate (`max_wind_ms ≥ 18 AND 25–48°N AND −100 to −66°W AND not near coast`) requires 2+ consecutive inland snapshots above the threshold. Florence dropped below 18 m/s before clearing the coastal box.

---

## 12. Secondary auditor checklist

| Step | What to do |
|---|---|
| 1 | Download HURDAT2 Atlantic file from NHC and verify the AL062018 block has 79 records |
| 2 | Run `python audits/florence_2018/_build_florence_audit.py` against your local bundle — should print "Match within 0.5 = True" |
| 3 | Cross-check `florence_intermediate.json` stage-by-stage against the formulas in `core/dps_engine.py` (Stages 1, 3, 4) and `core/cumulative_dpi.py` (Stage 2) and `compile_cache.py:apply_basin_dps_adjustment` (Stages 4, 5) |
| 4 | Verify ground truth against `core/ground_truth.py:AL062018` and NHC TCR (Stewart & Berg 2019) |
| 5 | Spot-check Stage 5 quirk: confirm that `60 + 4*sqrt(75.79-60) = 75.9` and that this is near-identity behavior in the Devastating band |
| 6 | Verify the rainfall overshoot: compute `2281 / (35.93 * 25.4)` and confirm 2.50× |
| 7 | Validate the displayed page: visit `https://stormdps.com/storm/AL062018` and confirm 76 (rounded) appears on the score card |

---

## 13. Sources

- **NHC HURDAT2 (Atlantic)** — `https://www.nhc.noaa.gov/data/hurdat/hurdat2-1851-2024-040425.txt`, header `AL062018,FLORENCE,79,`
- **NHC TCR for Florence** — Stewart, S. R., & Berg, R., *Hurricane Florence (AL062018)*, NWS/NHC, 2019.
- **USGS STN** — post-event high-water-mark surveys (Emerald Isle 10.1 ft surge)
- **NOAA NCEI Billion-Dollar Disasters** — $24.23 B CPI-adjusted to 2024
- **NWS WFO MHX (Newport/Morehead City)** — peak observed rainfall 35.93 in at Elizabethtown, NC
- **StormDPS code** — `compile_cache.py`, `core/dpi.py`, `core/cumulative_dpi.py`, `core/dps_engine.py`, `core/ground_truth.py`, `core/rainfall_warning.py`, `frontend/compiled_bundle.json`
