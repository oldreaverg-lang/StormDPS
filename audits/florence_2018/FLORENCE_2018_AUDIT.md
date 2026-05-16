# Hurricane Florence (AL062018) — End-to-End DPS Audit

**Date:** 2026-05-15
**Storm:** Hurricane Florence, August 30 – September 18, 2018
**ATCF ID:** AL062018 / IBTrACS SID 2018242N12348
**Displayed DPS:** **75.89** (Devastating)
**Purpose:** Second storm-level audit following Ian (AL092022). Florence is the inverse-archetype — modest peak intensity at landfall (Cat 1/2), Cat 4 lifetime, and the worst inland-rainfall catastrophe in North Carolina history. Stress-tests the rainfall-flood pathway specifically.

**Companion artifacts:**
- `florence_snapshots.csv` — 79-row HURDAT2 best-track joined with per-snapshot DPI **and per-snapshot rainfall classification** (`near_land`, `stall_flag`, `slow_flag`, `terrain_zone`, `basin_zone`, `cum_stall_h`, `cum_slow_h`, `cum_effective_rain_h`, `cum_estimated_rainfall_mm`)
- `florence_intermediate.json` — machine-readable stage-by-stage formula trace, **including rainfall-estimator internals decomposition and 5-storm comparison block**
- `_build_florence_audit.py` — reproduces both files from bundle field values (idempotent). v2 of the script (post audit-of-audit review) adds the rainfall classification, 5-storm comparison, and persistence-pathway design candidate.

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

## 4.5. Rainfall estimator internals — the structural finding

Before continuing to Stage 3 (which consumes the rainfall estimator's output via `rain_inland_factor`), this section decomposes what the estimator actually computes. The audit's headline finding — that the estimator over-shoots Florence by 2.5× — is a property of the formula's structure, not of any single miscalibrated constant.

**Source:** `core/rainfall_warning.py:compute_rainfall_warning`.

### 4.5.1 Formula structure

```
estimated_total_mm = peak_rain_rate_mm_per_hr × effective_rain_hours

peak_rain_rate_mm_per_hr =
  17.5 + 0.10 × (vmax_kt − 100)        for vmax > 100
  decays linearly                       for vmax ≤ 100
  (constants from Lonfat climatology)

effective_rain_hours = stall_hours + slow_hours × 0.6
  stall_hours = hours below 5 kt forward speed, near land
  slow_hours  = hours between 5 kt and 8 kt forward speed, near land
```

For Florence:

| Component | Value |
|---|---:|
| Peak wind | 130 kt |
| `peak_rain_rate_mm_per_hr` | `17.5 + 0.10 × 30 = 20.5 mm/hr` |
| Stall hours (bundle) | 42 |
| Slow hours (recomputed from track) | 29.2 |
| `effective_rain_hours` | `42 + 29.2 × 0.6 = 59.6` |
| Implied total (rate × hours) | `20.5 × 59.6 ≈ 1221 mm` |
| Bundle's stored `rainfall_est_mm` | **2281 mm** |
| Observed peak (Elizabethtown, NC) | **913 mm (35.93 in)** |

The gap between the formula's clean derivation (1221 mm) and the bundle's stored value (2281 mm) reflects additional moisture/terrain/basin multipliers applied downstream — the audit's CSV `cum_estimated_rainfall_mm` column tracks the clean rate-times-duration accumulation; the bundle's field includes those multiplicative bumps. Both substantially overshoot the observed 913 mm.

### 4.5.2 Why the formula structurally overshoots

The peak rain rate (intensity-derived, constant per storm) is multiplied by the total effective hours (accumulating over near-land time). **There is no decay term, no per-snapshot rate variation, no rate ceiling.** The formula's implicit assumption is that the storm sustains its peak rain rate throughout every stall and slow hour. Physically this is wrong: rain rate decays as the storm weakens, dry-air entrains, and the moisture flux from the source ocean falls off with inland penetration.

For Ian (0 stall hours, formula multiplies rate by only ~25 effective hours from slow-coastal time before landfall), the overshoot is modest (1.56×). For Florence (42 stall hours, ~60 effective hours), the overshoot compounds (2.50×). For Harvey (113 stall hours, ~140+ effective hours), the bundle stores 2558 mm vs ~1539 mm observed (1.66× overshoot — partially saved by Harvey's rate being a lower intensity-derived 19 mm/hr).

The pattern: **overshoot scales with effective_rain_hours, which scales with stall_hours.** The audit's `ratio_vs_stall_hours` correlation across all 15 ground-truth storms is +0.65 (in the standalone rainfall estimator audit).

### 4.5.3 The warning-score side is separately graded

The rainfall *warning* path (the user-facing "Historic" / "Extreme" / "High" label) is computed from:

```
rainfall_warning_score = stall_factor (0–40) + moisture_factor (0–30)
                       + terrain_factor (0–15) + basin_factor (0–15)
                       → 0–100 scale
```

For Florence this totals **87.5 → "Historic"**. This score is sound — Florence *was* a historic rainfall event, even if the estimated millimeter total is 2.5× too high. The 0–100 warning score is a categorical judgment (right) while `rainfall_est_mm` is a numeric quantity (wrong by 2.5×).

### 4.5.4 What feeds back into DPS

Stage 3's `rain_inland_factor` is gated by `rainfall_warning_score`, not by `rainfall_est_mm`. The factor caps at 0.04 at any warning score ≥ ~80. Florence's 87.5 saturates the cap. So Florence's *DPS score* is unaffected by the 2.5× overshoot — the path is hard-capped before the noisy number reaches it. But the public-facing rainfall text ("Est. 2281 mm / 90 in") *is* affected, and would mislead a reader.

This is the structural finding that motivates Section 9's reframing: the rainfall pathway is graded separately from the wind pathway, capped before it can dominate, and the only place the estimator's overshoot leaks is the user-facing text rather than the score.

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

## 9. Headline finding — persistence is a distinct pathway, not a coefficient

Two findings of unequal weight emerged from this audit. The smaller, calibration-class finding is that the engine's rainfall estimator over-shoots Florence by 2.5×. The larger, structural finding is *why* that overshoot doesn't change her displayed score — and what it implies about the architecture.

### 9.1 The calibration-class finding

The engine's stall-hour heuristic estimates Florence's peak rainfall at **2281 mm**. The NWS-observed peak (Elizabethtown, NC; NHC TCR Stewart & Berg 2019) was **913 mm** (35.93 in). The engine over-estimates by **a factor of 2.5×**. For comparison: Ian's overshoot was 1.56×; Harvey's was 1.66×.

The mechanism is identified in §4.5: `estimated_total_mm = peak_rain_rate × effective_rain_hours`, with no decay term. The peak rate (Lonfat-climatology, intensity-derived) is multiplied across every stall/slow hour as if it were sustained, when in reality rain rate falls off with weakening, dry-air entrainment, and inland penetration. **The standalone `rainfall_estimator/` audit covers this finding across all 15 ground-truth storms with mean overshoot 2.20×; it's the natural locus for a calibration fix.** That's not Florence's headline.

### 9.2 The structural finding

**The 2.5× overshoot does not change Florence's displayed DPS.** The `rain_inland_factor` is capped at 0.04 and gated by Carolinas exposure 0.55, so at any `rainfall_warning_score ≥ ~80` it saturates. Both 2281 mm and 913 mm produce the same factor. The score is invariant to the estimator's accuracy because the architecture *deliberately* prevents rainfall from cascading into the score the way wind does.

Look at the six-storm table in §10.1. Florence has the second-highest rainfall (2281 mm, behind only Harvey's 2558 mm), the second-highest stall_hours (42, behind Harvey's 113), the longest track in the group (459 hours / 19 days), and the largest stall_bonus among any audited Atlantic storm (0.0275). She still scores 75.89 — lower than every other storm in the comparison. The wind-anchored mechanism (peak × multipliers) ranks her on her *Cat 1 landfall*, not her *generational inland flood*.

**This isn't a coefficient bug.** Increasing the rainfall coefficient would lift Florence by maybe 1–2 points and destabilize the calibration of every wind-dominant storm in the bundle. Removing the 0.04 cap would let the estimator's noise (the 2.5× overshoot) leak directly into the score. The architecture's gating choices are correct *given* the current single-pathway design.

**The structural choice this audit surfaces:** rainfall catastrophe is a distinct destruction mechanism — additive in nature (more hours of moderate rain = more cumulative damage, no decay-to-peak ceiling), terrain-modulated, and persistence-driven. Representing it as a percentage modifier on a wind-intensity anchor systematically under-represents storms where the dominant mechanism is hydrologic rather than aerodynamic.

### 9.3 A candidate architectural response (design only — not implemented)

The framing surfaced by the audit-of-audit review is: *the rainfall pathway might need its own anchor, not a larger multiplier on the wind anchor.*

Currently the engine produces two parallel 0-100 scales:

- **DPS** — peak-anchored, wind-dominant, the displayed score
- **`rainfall_warning_score`** — persistence-anchored, hydrologically modeled, currently feeds DPS only via the capped 0.04 `rain_inland_factor`

A candidate design — surfaced here, not implemented — is **`max(intensity_anchored_DPS, persistence_anchored_DPS)`** where the persistence pathway uses `rainfall_warning_score` (plus optional surge and inland-penetration terms) as its own *anchor* with its own compression curve, rather than as a small modifier. Florence's persistence-anchored score would likely be in the high 80s based on her warning_score 87.5; her intensity-anchored score stays at 75.89. The displayed value would take the max, preserving wind-anchored calibration for everyone else.

This is *not* a calibration tweak. It's a parallel pathway. It would require:

1. Lifting `rainfall_warning_score` from a 0-100 alert grade to a fully formed DPS-equivalent scale (additional inputs: surge, inland penetration, terrain spatial integration)
2. Choosing a compression curve for the persistence pathway separate from the Atlantic (T=60, S=4) wind curve
3. Re-validating against the bundle to confirm wind-dominant storms aren't shifted (the `max()` operator preserves them if their wind anchor is higher than their persistence anchor)
4. Likely a new methodology-page section explaining the two-pathway architecture

**This audit does not advocate for the change.** It documents that the choice exists, that the wind-only pathway can't represent Florence's archetype regardless of how the rainfall estimator is calibrated, and that the architectural decision belongs to the project rather than to the audit. The structural observation is the headline; the calibration overshoot is a footnote.

### 9.4 What does change without the architectural lift

Even without a parallel pathway, two narrower fixes from this audit are worth landing:

1. **Activate the observed-rainfall override.** `core/dps_engine.py:L112-129` already has the hook to override `rain_result.estimated_total_mm` with `ground_truth.peak_rainfall_in` at compile time. The bundle was compiled before that activated for Florence. Recompile and the user-facing "Est. 2281 mm" becomes "Est. 913 mm" — the displayed text matches reality. Score impact is small (cap is binding) but the rainfall_text becomes honest.

2. **Recalibrate the rainfall estimator.** Add a duration-dependent decay term so the per-hour rate falls off across long stalls, OR cap `effective_rain_hours` at a Lonfat-climatology-derived ceiling. The standalone rainfall audit recommends a specific multiplier (≈ 0.45×) that reduces mean overshoot from 2.20× to ~1.0×. This is calibration-class work and belongs in `rainfall_warning.py`, not the DPS engine.

These two fixes correct the *displayed text* and the *estimator value* without touching the architectural question of whether persistence deserves a parallel scoring pathway.

---

## 10. Six-storm comparison — what the audit sequence jointly establishes

The audits so far covered Ian (AL092022) and Florence (AL062018). To frame the structural observation in Section 9, the table below pulls four additional archetypal Atlantic storms from the bundle and decomposes them through the same five-stage pipeline. Numbers come from the bundle directly (the same fields this audit script reads for Florence); no re-derivation.

### 10.1 Six-storm table — Atlantic basin, same formula path

| Storm | Year | Archetype | Peak_DPS | Cum_DPI | Stage 3 Boosted | Display DPS | Label | Stall_h | Rain_mm (engine) |
|---|---:|---|---:|---:|---:|---:|---|---:|---:|
| Ian | 2022 | Compact Cat 5 perp landfall | 82.1 | 98.5 | 108.8 | **87.93** | Catastrophic | 0 | 1068 |
| Ida | 2021 | Compact Cat 4 LA + NE remnants | 79.1 | 91.7 | 101.8 | **85.87** | Catastrophic | 0 | 647 |
| Harvey | 2017 | 5-day TX stall, gen'l rainfall | 72.8 | 87.4 | 94.2 | **83.41** | Devastating | 113 | 2558 |
| Sandy | 2012 | Huge wind field NE corridor | 73.6 | 83.1 | 93.9 | **83.27** | Devastating | 12 | 787 |
| Michael | 2018 | Compact Cat 5 FL panhandle | 69.6 | 83.6 | 89.3 | **81.68** | Devastating | 0 | 836 |
| Florence | 2018 | Cat 4 lifetime / Cat 1-2 land / **inland flood** | **58.1** | 69.7 | 75.8 | **75.89** | Devastating | 42 | 2281 |

### 10.2 What the table reveals

**Florence's peak_dps anchor (58.1) is 11.5 points below the next-lowest in the group (Michael's 69.6).** Every other storm in this comparison had its lifetime peak occur over or near land. Florence's peak occurred three days offshore at 27N/66W — the land-proximity dampener cut her per-snapshot composite by ~70%. From that lower anchor, the cumulative pipeline's 20% lift (capped) and Stage-3 ~10% boost (capped factors) cannot close the gap.

**Florence's stall hours (42) are second only to Harvey's (113).** Yet Harvey reaches displayed 83.41 because Harvey's peak_dps anchor (72.8) was already 15 points above Florence's. **Stall hours are an additive percentage modifier on a multiplicative peak anchor — they cannot rescue a depressed anchor.**

**Florence's engine rainfall estimate (2281 mm) is the second-highest of these six** (only Harvey's 2558 mm is higher). But the `rain_inland_factor` is capped at 0.04 and gated by Carolinas exposure weight 0.55, so this contributes at most ~2.2 percentage points of the displayed score. There is no pathway in the current architecture by which a 2281 mm rainfall event scores higher than a 647 mm event (Ida) if Ida's wind peak was harder.

**Sandy and Michael land within 1.6 points of each other (83.27 vs 81.68) despite radically different mechanisms** — Sandy's destruction was the integral of huge wind field × NJ/NY exposure over many hours; Michael's was a compact Cat 5 punch into a low-exposure region. The formula reproduces the consensus that they were *similar magnitude* catastrophes despite being opposite shape. This is well-calibrated behavior on the wind-anchored side.

**The wind-anchored pathway works.** Ian → Ida → Harvey → Sandy → Michael span 87.93 → 81.68, a reasonable spread for storms most analysts would order similarly. **The persistence/rainfall pathway is the one that doesn't have a real seat at the scoring table.** Florence is the test case where the architecture's structural choice (peak × multipliers) hits its limit.

### 10.3 Key methodological observations

1. **The peak_dps anchor dominates the final score.** Florence's bonus stack works as designed and adds ~30% to her cum_dpi, but it can't compensate for a Stage-1 peak that's already 24 points below Ian. This is a structural property of the cumulative pipeline (peak × multiplier rather than peak + additive integration). **This is by design** — DPS measures destructive *potential* anchored to peak intensity. The question Section 9 raises is whether that peak-anchoring should be the *only* pathway, or whether rainfall catastrophe deserves a parallel one.

2. **Stage 5 compression only carries editorial weight above pre-comp ~76.** For Atlantic-storm scores in the 70–80 displayed band, the curve is effectively transparent. Compression's editorial role kicks in for the Catastrophic band (80+), which is where the spacing between Maria / Ian / Ida / Harvey is dictated by the curve choice. Florence sits exactly at the pivot point (75.79 → 75.9, a no-op).

3. **The rainfall estimator over-shoots systematically and the size of the overshoot scales with stall duration.** Ian's 0 stall hours → 1.56× overshoot; Florence's 42 stall hours → 2.50× overshoot; Harvey's 113 stall hours → 1.66× (smaller ratio but largest absolute miss). The standalone `rainfall_estimator/` audit found mean overshoot 2.20× across all 15 ground-truth storms with `ratio_vs_stall_hours` Pearson +0.65. The structural cause is identified in §4.5: peak rate × total hours, no decay term.

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
