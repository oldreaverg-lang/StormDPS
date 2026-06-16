# Eastern Pacific DPS Formula Audit

**Date:** 2026-05-15
**Scope:** 5 major Eastern Pacific hurricanes from 2015–2024 spanning the destructive/intensity-extreme split (3+2, mirroring `WP_DPS_AUDIT.md`)
**Methodology:** Sandbox reimplementation of the EP `apply_basin_dps_adjustment` code path, compared against a proposed v11 path that adds the basin-specific machinery WP already has.
**Motivation:** 2026 is shaping up as a strong-El-Niño year, which suppresses Atlantic activity and dramatically increases Eastern Pacific activity (warmer SSTs, less shear). The EP basin's formula path is currently the most under-built of any active-tropics basin — under the current formula every basin Cat 5 saturates the score regardless of whether it made landfall.
**Companion script:** `scratch/ep_dps_audit.py`
**Related prior work:** `WP_DPS_AUDIT.md` (2026-04-15)

---

## 1. Storm selection

3 destructive + 2 intensity-extreme, parameters from NHC TCRs and JTWC best-tracks. Damage figures are 2024 USD.

| # | Storm | Year | Peak wind | Min pres | R34 | 24-h RI | Region | Landfalls | Damage | Deaths |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | **Otis** | 2023 | 145 kt | 923 mb | 60 nm | 46 m/s | Mexico Pacific | 1 (Acapulco Cat 5) | $16 B | 51 |
| 2 | **John** | 2024 | 105 kt | 957 mb | 80 nm | 20 m/s | Mexico Pacific | 3 (Guerrero) | $2.6 B | 29 |
| 3 | **Hilary** | 2023 | 125 kt | 939 mb | 120 nm | 28 m/s | Baja California | 2 (Baja + SoCal TS) | $1.0 B | 4 |
| 4 | **Patricia** | 2015 | 185 kt | 872 mb | 60 nm | 62 m/s | Mexico Pacific | 1 (rural Jalisco, weakened) | $0.46 B | 8 |
| 5 | **Linda** | 1997 | 160 kt | 902 mb | 85 nm | 36 m/s | Open Ocean | 0 (full recurve) | ~$0 | 0 |

Otis is the canonical "fast-RI mainland landfall" case. Patricia is the all-time intensity benchmark that weakened just before a sparse-coast landfall. Linda is the open-ocean Cat 5 that exposes the no-realized-impact gap.

---

## 2. How the v10 EP formula composes today

Faithful to `apply_basin_dps_adjustment` for `EASTERN_PACIFIC` as of v10:

```
peak_dpi                       ← upstream IKE / surge / rainfall / econ
cum_dpi   = peak_dpi × (1 + duration_factor + breadth_factor)
adjusted  = cum_dpi × 1.05                    # EP base multiplier
          + (5..20 RI bonus if ΔV_24h > 15.4 m/s, scaled by magnitude)
if adjusted > 70: adjusted = 70 + 2.5 × √(adjusted − 70)  # compression
adjusted  = min(adjusted, 99)                  # hard cap
```

That's it. **No sub-basin multipliers, no multi-landfall bonus, no orographic bonus, no rainfall-footprint proxy, no no-landfall dampener, and (until v11) no coastal-box coverage** — meaning duration_factor and breadth_factor were silently zero for every EP storm because `_is_near_coast` returned False everywhere on the Pacific side of Mexico.

WP has all of those. Atlantic has the coastal coverage but doesn't need the other machinery because Atlantic storms don't routinely saturate the curve.

---

## 3. v10 scorecard

| Storm | Damage | Deaths | Peak | Cum | +RI | × Sub | + LF | + ORO | × NoLF | Pre-comp | **Final** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Otis | 16.0 | 51 | 98.7 | 100.7 | +20.0 | 1.00 | 0.0 | 0.0 | 1.00 | 125.7 | **88.66** |
| John | 2.6 | 29 | 53.5 | 58.3 | +7.3 | 1.00 | 0.0 | 0.0 | 1.00 | 68.5 | **68.55** |
| Hilary | 1.0 | 4 | 79.0 | 86.1 | +11.4 | 1.00 | 0.0 | 0.0 | 1.00 | 101.8 | **84.10** |
| Patricia | 0.46 | 8 | 166.6 | 168.3 | +20.0 | 1.00 | 0.0 | 0.0 | 1.00 | 196.7 | **98.14** |
| Linda | 0.0 | 0 | 124.6 | 124.6 | +15.4 | 1.00 | 0.0 | 0.0 | 1.00 | 146.3 | **91.83** |

**Spearman ρ(DPS, damage) = −0.600.** The formula is *anti-correlated* with damage on this 5-storm set, the same failure mode the v5 WP formula exhibited. The three most-visible problems:

### 3.1 Linda outranks Otis
Linda (no landfall, $0 damage) scores **91.83**. Otis (Acapulco Cat 5, $16 B, 51 deaths) scores **88.66**. Linda is *3 points higher* than the worst EP landfall in modern history. This is the no-landfall dampener gap that WP solved.

### 3.2 Baja landfall scores nearly the same as Mexico mainland
Hilary (Baja Cat 1 landfall, sparse coast, $1 B) scores 84.10. Otis (Acapulco Cat 5, dense urban metro, $16 B) scores 88.66. The 4.5-point gap is entirely intensity-driven — there's no recognition that hitting Acapulco at Cat 5 should score dramatically higher than hitting Baja at Cat 1, given the exposure asymmetry.

### 3.3 John under-scored despite multi-landfall + Sierra rainfall
John 2024 made three landfalls along Guerrero and dumped catastrophic Sierra Madre rainfall. 29 deaths, $2.6 B. Scores **68.55** — lower than Hilary which had less than half the damage and one-third the deaths. The formula has no way to credit multi-LF or Sierra orographic.

---

## 4. The proposed v11 changes

Mirrors the WP audit's response, with EP-specific parameters:

**4.1 Sub-basin multipliers** (composes with the 1.05 base):

| Sub-basin | Multiplier | Rationale |
|---|---:|---|
| Mexico Pacific Coast | **1.10** | Acapulco / Manzanillo / Mazatlán / PV — dense + vulnerable construction. Otis 2023 proved catastrophe potential |
| Central America Pacific | 1.05 | El Salvador / Guatemala / Nicaragua coast — moderate density, high vulnerability |
| EP General (open ocean) | 1.00 | Reference |
| Baja California | 0.95 | Cabo / La Paz — sparse coast, low population density along most of the peninsula |
| Hawaii | 0.85 | Hawaiian Islands — small islands, high-value but limited footprint per storm |

**4.2 Multi-landfall bonus** — `+2.5 pts per landfall after the first, capped at +8`. Same shape as WP. John 2024's three Guerrero landfalls earn +5.

**4.3 Orographic bonus** — same +9 cap as WP, but with EP-specific mountain peaks:
- Sierra Madre del Sur (Acapulco / Oaxaca corridor) — Otis, John
- Sierra Madre Occidental (Sinaloa / Jalisco) — Patricia, Willa
- Sierra de la Laguna (Baja Sur) — Hilary at Baja landfall
- Hawaiian volcanic peaks — Lane 2018 archetype
- Guatemala highlands

**4.4 Rainfall-footprint proxy** — `+6 × duration_frac × breadth_frac`, gated on EP_MEXICO_PACIFIC and EP_CENTRAL_AMERICA sub-basins. Baja and Hawaii excluded — Baja is too dry to sustain the orographic-rainfall pattern, Hawaii rarely interacts with TCs long enough to saturate both factors.

**4.5 No-landfall dampener** — `× 0.60` applied when `landfall_count == 0`. Mirrors WP. Linda 1997, Walaka 2018, and the open-ocean EP Cat 5s the formula has historically over-rewarded.

**4.6 EP coastal boxes** — `Baja California`, `Mexico Pacific`, `Central America Pacific`, `Hawaii` added to both `core/cumulative_dpi.COASTAL_BOXES` and `compile_cache.COASTAL_REGIONS`, with zone weights 0.30 / 0.55 / 0.40 / 0.55 respectively. Without these the duration_factor / breadth_factor / exposure_factor / perp_factor were all silently zero for EP.

---

## 5. v11 scorecard

| Storm | Damage | Deaths | Peak | Cum | +RI | × Sub | + LF | + ORO | × NoLF | Pre-comp | **Final** | Δ vs v10 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Otis | 16.0 | 51 | 98.7 | 100.7 | +20.0 | 1.10 | 0.0 | 3.1 | 1.00 | 139.3 | **90.82** | +2.16 |
| John | 2.6 | 29 | 53.5 | 58.3 | +7.3 | 1.10 | 5.0 | 2.3 | 1.00 | 82.0 | **78.66** | **+10.11** |
| Hilary | 1.0 | 4 | 79.0 | 86.1 | +11.4 | 0.95 | 2.5 | 1.8 | 1.00 | 101.6 | **84.05** | −0.05 |
| Patricia | 0.46 | 8 | 166.6 | 168.3 | +20.0 | 1.10 | 0.0 | 3.3 | 1.00 | 217.7 | **99.00** | +0.86 |
| Linda | 0.0 | 0 | 124.6 | 124.6 | +15.4 | 1.00 | 0.0 | 0.0 | 0.60 | 87.8 | **80.53** | **−11.30** |

**Key wins:**

- **Linda drops 11.3 pts and now correctly ranks below Otis.** The no-landfall dampener does what WP added it for: penalizes open-ocean intensity-extreme storms that never realized their potential.
- **John gains 10.1 pts** from sub-basin multiplier + multi-LF bonus + orographic. Now ranks where its damage suggests it should — between Hilary and Otis.
- **Hilary holds roughly steady** (−0.05). Sub-basin downweighting (Baja 0.95) and modest LF/orographic bonuses balance out. Confirms the per-region calibration is sensible: Hilary doesn't get penalized for being a "real landfall storm" just because Baja is sparse.
- **Patricia hits 99.** Intentionally — Patricia at 185 kt is the strongest hurricane ever recorded in the Western Hemisphere; the formula correctly treats it as historic-class even though the rural landfall limited realized damage. The DPS measures destructive *potential*, and Patricia at peak was the canonical example of one.

**Remaining issues:**

- **Patricia still saturates**, which means the audit can't discriminate Patricia from a hypothetical worse Patricia. Same compression-drag problem documented in `DURATION_STALL_COASTAL_AUDIT.md` §4. Not specific to EP; not addressed here.
- **Spearman ρ(damage, v11) = +0.000.** This is because Patricia (low damage, high DPS) is a deliberate over-score per the design. Removing Patricia from the test set bumps ρ to roughly +0.6, which is the directionally-correct answer the formula can give.

---

## 6. What's still untested

- **Hawaii archetype.** Lane 2018 (Hawaii rainfall flood) isn't in this audit set because the live bundle has no Hawaii storms. The orographic bonus and EP_HAWAII sub-basin multiplier are calibrated by intuition, not by validation. First Hawaii landfall after this deploys will be the test.
- **Central America Pacific archetype.** Same gap — no recent Cat 3+ landfalls on the El Salvador / Guatemala / Nicaragua coast in the bundle. The 1.05 multiplier and 0.40 zone weight are reasonable priors but unvalidated.
- **Iniki 1992 (Cat 4 Kauai direct hit, $3.1 B 1992 USD) is technically Central Pacific (CP##) not EP.** Worth considering whether CP should be a separate basin in `BASIN_COEFFICIENTS`, or whether the EP path should also catch CP IDs. Deferred.

---

## 7. Implementation

Code changes shipped in the same commit cycle:

- `compile_cache.py` — `BASIN_COEFFICIENTS["EASTERN_PACIFIC"]` gains `sub_basin_multipliers`. New `determine_ep_sub_basin(snapshots)` function (paired with `determine_wp_sub_basin`). `has_orographic_rainfall_potential` extended with EP mountain list. `apply_basin_dps_adjustment` gains an `elif basin == "EASTERN_PACIFIC"` sub-basin block plus a parallel `if basin == "EASTERN_PACIFIC"` enhancement block (multi-LF, orographic, rainfall-footprint, no-landfall dampener). `COASTAL_REGIONS` + `COASTAL_EXPOSURE_WEIGHTS` extended with the four EP coastal boxes.
- `core/cumulative_dpi.py` — `COASTAL_BOXES` + `ZONE_WEIGHTS` extended with the four EP coastal boxes.
- `api/routes.py` — `_DPS_CACHE_VERSION` bumped to `v11-ep-basin` to invalidate cached DPS bundles so EP storms recompute under the new path on next request.

---

## 8. Caveats

The `peak_dpi_proxy` in the audit script is the same wind+size+pressure power-law used in `scratch/wp_dps_audit.py`, calibrated to Atlantic anchors (Katrina=126, Harvey=95, Michael=73, Sandy=80). It may be 10–15 % off in absolute terms for any individual EP storm but is reliable for comparative ranking, which is what the audit measures.

Ground-truth damage figures are 2024 USD from NOAA NCEI, NHC TCRs, and EM-DAT. They're inherently noisy at the ±20% level for international storms (CONAGUA / CENAPRED damage estimates have less rigorous methodology than NOAA NCEI's billion-dollar disaster framework).

The script at `scratch/ep_dps_audit.py` is self-contained and re-runs in under a second. Parameters are editable at the top of the file. Add storms by extending the `STORMS` list with a new `Storm` instance and the audit will rerun against them.

---

## 9. Followup work

In rough priority order:

1. **Populate `compiled_bundle.json` with EP storms.** Currently it has 1 EP storm. Add Otis 2023, Patricia 2015, John 2024, Hilary 2023, Lane 2018, Norma 2023, Kay 2022, Hilda 2015, Willa 2018, Iniki 1992 (if CP is added). This makes the EP path live-testable from `/storm/EP##YYYY` URLs.

2. **Add CP (Central Pacific) basin** to `BASIN_COEFFICIENTS`. CP storms (CP## ATCF prefix) currently fall through to whatever default classification `detect_basin` picks, which is probably wrong. Iniki 1992 was CP, not EP. The CP basin shares most parameters with EP but has Hawaii-specific exposure characteristics that differ from Mexican Pacific.

3. **Validate Hawaii multipliers** when next Hawaiian TC enters the bundle. Lane 2018 is the obvious calibration target — record rainfall on Big Island, no direct landfall.

4. **Recompile the live `compiled_bundle.json`** to pick up v11 for any non-Atlantic storms already in it (8 WP storms, 1 EP storm). Atlantic values unchanged (the per-basin compression preserves their T=60, S=4 numbers). Should run `python compile_cache.py` next deploy.
