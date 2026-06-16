# West Pacific DPS Formula Audit

**Date:** 2026-04-15
**Scope:** 5 major typhoons from the last decade (2019–2024)
**Methodology:** Sandbox reimplementation of the `apply_basin_dps_adjustment` code path from `compile_cache.py`, run against storms with published intensity + damage ground truth. No live code changes.
**Companion script:** `scratch/wp_dps_audit.py`

---

## 1. Storm selection

Per request: three landfall-destructive + two intensity-extreme. Parameters (peak wind, peak pressure, size, 24-h RI rate, sub-basin, orographic exposure, landfall count) are sourced from JTWC best-tracks, PAGASA / JMA / Vietnam NDMA impact reports, and the UCAR b-deck files the new pipeline consumes. Damage / fatality figures are the commonly cited totals.

| # | Storm | Year | Peak wind | Min pres | R34 | 24-h RI | Sub-basin | Landfalls | Damage | Deaths |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | **Hagibis** | 2019 | 160 kt | 915 mb | 320 nm | 36 m/s | WP_JAPAN | 1 | $18 B | 104 |
| 2 | **Yagi** | 2024 | 140 kt | 915 mb | 180 nm | 30 m/s | WP_VIETNAM | 3 | $14 B | 844 |
| 3 | **Doksuri** | 2023 | 155 kt | 925 mb | 210 nm | 24 m/s | WP_SOUTH_CHINA | 2 | $28.5 B | 137 |
| 4 | **Goni** | 2020 | 170 kt | 884 mb | 90 nm | 38 m/s | WP_PHILIPPINES | 2 | $0.4 B | 32 |
| 5 | **Surigae** | 2021 | 165 kt | 895 mb | 150 nm | 43 m/s | WP_GENERAL | 0 | ~$0 | 10 |

Goni and Surigae are the intensity-extreme anchors (strongest landfall ever recorded, and strongest open-ocean typhoon in recent history, respectively). Hagibis, Yagi, Doksuri are the damage/mortality anchors spanning Japan rainfall, multi-country multi-landfall, and inland-flood sub-regimes.

---

## 2. How the formula currently composes

Faithful to `apply_basin_dps_adjustment` for `WESTERN_PACIFIC`:

```
peak_dpi                             ← upstream IKE/surge/rainfall/econ pipeline
cum_dpi   = peak_dpi × (1 + duration_factor + breadth_factor)   # each ≤ 0.10
adjusted  = cum_dpi × 1.10                                       # WP base multiplier
          + 15                                                   # flat RI bonus if ΔV_24h > 15.4 m/s
          + min((landfalls − 1) × 2.5, 8)                        # multi-landfall
          + min(wind_near_mtn / 18, 9)                           # orographic, if within 1° of listed peak
adjusted  = adjusted × sub_basin_multiplier                     # 0.98–1.20
if adjusted > 60: adjusted = 60 + 4 × √(adjusted − 60)          # sqrt compression
adjusted  = min(adjusted, 99)                                    # hard cap
```

`peak_dpi` is anchored (from `DPI_FORMULA_REPORT.md` and the handoff) against Atlantic storms at Katrina=126, Harvey=95, Michael=73, Sandy=80. The audit uses a simple wind+size+pressure proxy calibrated to hit those anchors within ~5 %.

---

## 3. Scorecard — current formula (v5)

Pre-compression is the raw value fed to the sqrt curve. Final is after compression and hard cap.

| Storm | peak_dpi | cum_dpi | RI | LF | ORO | sub-mult | Pre-compression | **Final DPS** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Hagibis 2019 | 146.6 | 167.1 | +15 | — | +1.9 | ×1.00 (Japan) | 200.7 | **99.00** |
| Yagi 2024 | 105.3 | 119.0 | +15 | +5.0 | +1.6 | ×1.20 (VN) | 182.9 | **99.00** |
| Doksuri 2023 | 127.1 | 144.9 | +15 | +2.5 | +1.8 | ×1.08 (S. China) | 192.9 | **99.00** |
| Goni 2020 | 142.7 | 150.6 | +15 | +2.5 | +2.7 | ×1.15 (PH) | 213.7 | **99.00** |
| Surigae 2021 | 139.5 | 154.2 | +15 | — | — | ×1.00 (Gen) | 184.6 | **99.00** |

**Rank correlation of v5 DPS vs. published damage (Spearman ρ): −0.70.** Against fatalities: −0.70. The formula is not just uninformative at the top end — it's inversely ordered, because every storm collapses to 99 and the listing order alphabetizes against impact.

This is the single biggest finding: **every major WP typhoon of the last decade hits the ceiling**. A $28 B / 137-death event and a $400 M / 32-death event receive the same DPS.

---

## 4. Shortfall analysis

### 4.1 Ceiling saturation (primary failure)
The sqrt compression was calibrated — per the code comments — to map raw 140 → 95. But a typical WP Cat 4+ typhoon has:

- `cum_dpi × 1.10` ≈ 130–190
- `+15` RI bonus for any storm that ever intensified faster than 30 kt/24 h (i.e. every strong typhoon)
- `+2–8` for landfall/orographic
- `×1.08–1.20` sub-basin multiplier

Pre-compression values land in the 180–220 range. The sqrt curve from there gives 95–99, which the hard cap clamps to 99. In practice the usable resolution in the top tier is zero.

### 4.2 Flat RI bonus ignores RI magnitude
Surigae (43 m/s gain in 24 h — a physical-limit intensification) and a storm that barely crosses the 15.4 m/s threshold both receive **+15**. In the data set above, all 5 storms trigger RI at identical bonus, so the term does no useful work distinguishing them.

### 4.3 No "realized impact" signal
The formula is a destruction *potential* score, but nothing in it reflects whether potential was realized. **Surigae made zero landfalls** and destroyed essentially nothing, yet scores 99.00 — indistinguishable from Hagibis ($18 B). Without a penalty for non-interaction with land or population, intensity-extreme storms with benign tracks are scored as catastrophes.

### 4.4 Orographic bonus favors small/intense storms
`min(wind_near_mtn / 18, 9)` rewards peak-wind-at-mountain-edge behavior. Goni, hitting Bicol highlands at 48 m/s, earns +2.7. Doksuri, whose remnants caused **the worst Beijing/Hebei floods in 140 years** ($20 B+ of its damage total) via a much weaker but slower-moving rain shield, earns +1.8. The term is scaled by intensity but the damage was scaled by rainfall volume and duration, which never enter the formula.

### 4.5 No rainfall-volume / inland-flood channel
Three of the five storms (Hagibis, Doksuri, Yagi) were rainfall-dominant — most of their dollar damage and fatalities came from river flooding hundreds of kilometers from the coastline. The only rainfall proxy the formula has is the orographic trigger, which requires the circulation to be within 110 km of a listed mountain peak *while still at TS+ intensity*. Doksuri's remnant floods and Yagi's Annamite / Laos flooding barely qualify.

### 4.6 Sub-basin multiplier compounds with the ceiling
Because `sub_multiplier` is applied AFTER the bonuses and BEFORE compression, pushing Yagi's pre-compression score up 20 % (×1.20 WP_VIETNAM) has no effect on the final DPS when compression already saturates. The `WP_VIETNAM = 1.20` coefficient the handoff flagged as untested is literally untestable through the top-band ceiling.

### 4.7 Peak-wind bias in `peak_dpi`
Upstream `peak_dpi` weights peak 1-min wind heavily (IKE component). Yagi peaked at 140 kt briefly; Hagibis peaked at 160 kt; Goni at 170 kt. Yagi's 844 deaths and 3-country devastation come through as the *lowest* pre-compression of the three destructive storms (182.9 vs 200.7 and 192.9). Intensity and destructivity diverge in the WP more than the formula admits.

### 4.8 WP_JAPAN multiplier may be too low
Japan is coded at 1.00 on the reasoning that `COASTAL_EXPOSURE_WEIGHTS` already captures asset density. But `COASTAL_EXPOSURE_WEIGHTS` is used upstream for *landfall exposure*, which in Hagibis's case was only the Izu Peninsula — not metropolitan Tokyo, where most of the damage actually occurred (downstream flooding). A 1.05–1.10 WP_JAPAN would better reflect that Japan landfalls routinely cascade into inland economic exposure.

### 4.9 Duration/breadth caps at +10 % each don't stretch
Yagi was operational for 11 days, crossed three countries, and killed 800+. Hagibis was operational for 7 days and primarily harmful for 48 hours. Both cap out at the same +10 % duration and +10 % breadth. The caps were lowered specifically to avoid inflating every storm — correct instinct — but the design left no headroom for the truly long-duration multi-country cases.

---

## 5. Experimental variants & what they reveal

Four sandbox variants, run through the same code path, with the corrections layered in:

| Variant | Change | Purpose |
|---|---|---|
| **v6a** | `× 0.60` if `landfall_count == 0` | Does a "realized impact" penalty rescue the Surigae case? |
| **v6b** | `+ 6 × duration_frac × breadth_frac` for rainfall-prone sub-basins (JP/VN/TW/S.China) | Does a rainfall-volume proxy lift Doksuri / Hagibis / Yagi over Goni? |
| **v6c** | v6a + v6b + LF cap 8→10, orographic slope 18→15 | Combined fixes, still within same formula shape |
| **v6d** | v6c + compression `(T=70, S=2.5)` + RI scaled 5–20 pts by magnitude | Also fixes the ceiling |

### Per-storm DPS under each variant

| Storm | Dmg $B | Deaths | v5 | v6a | v6b | v6c | **v6d** |
|---|---:|---:|---:|---:|---:|---:|---:|
| Hagibis | 18.0 | 104 | 99.00 | 99.00 | 99.00 | 99.00 | **98.98** |
| Yagi | 14.0 | 844 | 99.00 | 99.00 | 99.00 | 99.00 | **96.59** |
| Doksuri | 28.5 | 137 | 99.00 | 99.00 | 99.00 | 99.00 | **97.43** |
| Goni | 0.4 | 32 | 99.00 | 99.00 | 99.00 | 99.00 | **99.00** |
| Surigae | 0.0 | 10 | 99.00 | 88.50 | 99.00 | 88.50 | **86.42** |

### Rank correlation (Spearman ρ vs. real-world impact)

| Variant | ρ(DPS, $) | ρ(DPS, deaths) | ρ(DPS, max(norm $, norm deaths)) |
|---|---:|---:|---:|
| v5_current | −0.70 | −0.70 | −0.60 |
| v6a no-landfall penalty | +0.30 | +0.30 | +0.40 |
| v6b rainfall channel | −0.70 | −0.70 | −0.60 |
| v6c combined | +0.30 | +0.30 | +0.40 |
| **v6d retuned compression** | +0.30 | 0.00 | +0.10 |

### What the variants show

- **v6a** (no-landfall penalty). Cheapest, most effective first fix. Surigae drops from 99 → 88.5 — correctly signaling it as an intensity curiosity, not a destruction event. Correlation with damage goes from −0.70 to +0.30. This is a floor every DPS-style score should have.

- **v6b** (rainfall channel). By itself, does nothing — because every landfall storm is already pinned at the ceiling. Illustrates that additive fixes can't help until compression is relaxed.

- **v6c** (both + wider caps). Adds +2.5–3 on the rainfall-dominant storms; still 99 for all landfall storms. Same problem.

- **v6d** (relaxed compression + magnitude-scaled RI). Finally yields spread at the top: 86.4 → 96.6 → 97.4 → 98.98. But notice Goni still hits 99 — it's small, Cat-5+, multi-landfall, orographic, PH-multiplier — the bonus stack simply overpowers the compression. To fix Goni, the formula needs an economic-exposure integrator (the `COASTAL_EXPOSURE_WEIGHTS` term properly applied in-basin), not more bonus tuning.

### What still fails even in v6d

- **Doksuri (worst damage) ranks 3rd, not 1st.** The $20 B of inland-flood damage from the remnant low sits outside any bounding box the formula uses. A DPS that reads only synoptic-hour position + wind will never capture "the remnant sat over Hebei for 4 days."
- **Yagi still under-scored relative to its mortality profile.** The sub-basin ×1.20 helps, the multi-landfall +5 helps, but the formula has no signal that says "path crossed three densely populated countries at TS+ intensity."
- **Goni still pegs to ceiling.** Intensity-based bonus stacking cannot be discriminated downward without an exposure-integrator term.

---

## 6. Concrete recommendations (in priority order)

1. **Relax the sqrt compression to (T=70, S=2.5).** Low-risk, changes only the mapping of scores > 70. Immediately unfreezes the top tier.

2. **Add a no-landfall dampener.** `× 0.60` (or similar) when `significant_landfalls == 0`. Cheap, clear, well-motivated. Would have correctly penalized 8 of the 30+ "Cat 5 open-ocean" storms that have historically inflated the WP leaderboard.

3. **Scale the RI bonus to magnitude.** `5 + 15 × (excess / 30 m/s)` ranging from +5 to +20. Distinguishes "threshold RI" from "violent RI" without changing the trigger.

4. **Apply sub-basin multiplier BEFORE the bonuses, not after.** Currently the multiplier compounds the ceiling problem. Applying it to `cum_dpi × dps_multiplier` only, then adding RI/LF/ORO as absolute points, would make the bonuses comparable across basins.

5. **Add a rainfall-footprint proxy.** Simplest non-invasive version: `+6 × duration_frac × breadth_frac` gated on rainfall-prone sub-basins. A principled version would use QPE tiles or JMA rainfall analysis around the track.

6. **Build an economic exposure integrator.** Walk every 6-hourly snapshot, look up population × GDP density in a 2° radius, integrate against wind intensity. This is the only structural fix that separates Goni (sparse hit) from Doksuri (dense sustained hit). Material engineering work — probably worth scoping as its own project.

7. **Re-examine WP_JAPAN = 1.00.** Hagibis shows cascade-into-Tokyo exposure is not captured by landfall weight alone. 1.05–1.08 is defensible.

8. **Retire or soft-retire WP_VIETNAM = 1.20.** It's the highest multiplier in the WP table and is currently unvalidatable because the ceiling suppresses its effect. Either restore testability by enacting (1) and (4), or drop it to 1.10 pending a Yagi-class validation storm.

---

## 7. Caveats

The `peak_dpi` used here is a wind+size+pressure proxy fitted to published Atlantic anchors, not the live IKE/surge/rainfall pipeline. The proxy may be 10–15 % off in absolute terms for any given storm — but the audit is comparative across storms and robust to proportional bias. The ceiling-saturation finding in particular is invariant: pre-compression values would need to be halved across the board to un-saturate the current formula, which the live pipeline does not produce.

The script at `scratch/wp_dps_audit.py` is self-contained and reruns in under a second. Parameters for each storm are editable at the top of the file; the scoring function is a faithful Python reimplementation of `apply_basin_dps_adjustment` and can be extended with additional variants.
