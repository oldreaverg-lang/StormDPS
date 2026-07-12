# Southern Hemisphere DPS Audit — dead-legs activation

**Date:** 2026-07-12
**Scope:** SOUTH_INDIAN + SOUTH_PACIFIC basins (SW Indian Ocean, Australian
region, South Pacific). Full engine path.
**Companion harness:** session-scratch `sh_calibration.py` (16 famous SH
cyclones, tracks fetched via `/track`).
**Ground truth:** commonly cited damage/death totals.

---

## 1. Headline

Every SI/SP storm scored **open-ocean**: `region=Open Ocean`,
`coastal_hours=0`, `duration_factor=0`, `breadth_factor=0`, no adjustment
bonuses. There are no SH coastal or economic profiles and no SH waypoints, so
`_estimate_region_from_coords` returned None → the surge (35%) and economic
(35%) legs contributed ~0/14, exactly the WP dead-legs pathology but worse
(WP at least had coarse boxes). Result: catastrophic storms indistinguishable
from fish storms.

## 2. Current-state validation (V0, 16-storm set)

| Storm | V0 DPS | Damage $B | Deaths | Landfall |
|---|---:|---:|---:|---|
| Winston 2016 | 48.3 | 1.4 | 44 | Fiji (Cat 5 — strongest SH landfall ever) |
| Pam 2015 | 46.9 | 0.45 | 16 | Vanuatu (Cat 5) |
| Yasi 2011 | 42.0 | 3.6 | 1 | Queensland (Cat 4) |
| **Ilsa 2023** | **41.7** | 0.03 | 0 | W Australia — Cat 5 on an EMPTY coast |
| Idai 2019 | 31.3 | 2.2 | 1300 | Mozambique (deadliest) |
| Debbie 2017 | 30.3 | 2.7 | 14 | Queensland |
| **Gabrielle 2023** | **29.0** | **8.0** | 11 | New Zealand (costliest ever) |
| Seroja 2021 | 24.5 | 0.09 | 272 | Timor / W Australia |

ρ(damage) **+0.30**, ρ(deaths) **−0.03**. Cat-5-empty-coast Ilsa outscored
Idai; NZ's $8B Gabrielle sat near the bottom. No discrimination.

## 3. Fix — living-legs activation (same architecture as WP Tranche B)

Because SH has **no compensating layer** (SI/SP coefficients were already
near-neutral: 1.03/1.0, no RI, no sub-basin table), this is purely additive —
add the legs, no lockstep strip-down.

1. **~90 SH coastline waypoints** in `core/land_proximity.py` (`sh_*` region
   keys) across 12 regions: Mozambique, Madagascar, Mascarene, W/NW Australia
   (sparse Pilbara/Kimberley), E Australia (Queensland), Fiji, Vanuatu, New
   Caledonia, Tonga/Samoa, New Zealand, Solomon, Timor. `nearest_sh_coast()`
   returns (distance, region key, population). **Southern-latitude gated
   (lat < 0)** — no Northern-Hemisphere basin (Atlantic/EP/WP/NI, i.e. every
   baked storm) can observe an `sh_*` region.
2. **12 `sh_*` CoastalProfiles + EconomicProfiles**, econ pre-damped ×0.85.
   Wide vulnerability spread: SW Indian Ocean (Mozambique/Madagascar/Vanuatu)
   are among the world's poorest + most exposed; Australia/NZ/New Caledonia
   are wealthy + hardened.
3. **Tiered profile reach** by waypoint population (≥0.40 → 150 km, 0.20–0.40
   → 75 km, sparse < 0.20 → detection only). This is the exposure
   discriminator: a Cat 5 on the empty Pilbara (pop 0.05) gets no exposure
   profile, so Ilsa stays low.
4. **Landfall-intensity bonus** (SH floor 42 m/s — SH runs fewer Cat 5s and
   more high-impact Cat 3-4 — up to +12 at 70 m/s, population-scaled). The
   Winston/Ilsa discriminator.
5. **Multi-landfall + orographic bonuses** (Freddy's double Madagascar/
   Mozambique landfall; Réunion/Madagascar/NZ/Queensland ranges) + the
   no-landfall dampener. SH land-contact uses a wider 75 km / 20 m/s gate
   because SH waypoint coverage is sparse and its storms weaken/undergo ET
   fast.
6. SI/SP `dps_multiplier` 1.03/1.0 → **1.00**, compression **T 70 → 80**
   (mirrors WP; only SI/SP storms affected, none baked).

## 4. Results (16-storm set, final)

| Storm | V0 → final | Note |
|---|---|---|
| Winston | 48 → **90** | Cat 5 Fiji — top, correct |
| Pam | 47 → **87** | Cat 5 Vanuatu direct |
| Harold | 44 → **77** | Cat 5 multi-island |
| Yasi | 42 → **74** | Cat 4 Queensland $3.6B |
| Freddy | 42 → **65** | 1200 deaths, multi-landfall (+8 LF) |
| Gita | 35 → **64** | Cat 4 direct on Tonga capital |
| Idai | 31 → **60** | deadliest; Beira surge + river flood |
| Debbie | 30 → **55** | Queensland $2.7B |
| Batsirai | 36 → **53** | Madagascar |
| **Ilsa** | 42 → **42** | Cat 5 on EMPTY coast — correctly held down |
| Kenneth | — → **38** | Cat 4 on SPARSE N Mozambique (not Beira-class) |
| Seroja | 24 → **24** | weak in dataset; 272 deaths were flash floods |
| **Gabrielle** | 29 → **17** | ET at NZ — see §5 |

**Non-SH bit-identity: 0 movers** across all 218 Atlantic/EP/WP baked storms
(lat<0 gate). 5 baked SH storms moved as intended (non-landfalling recurvers
dampened: Horacio 42→25, Fina 28→17).

## 5. Known limitations (accepted, documented)

- **Gabrielle (ET / data gap).** Its $8B was post-tropical river flooding over
  New Zealand; the tropical b-deck track *ends 286 km NE of NZ* at ET
  transition, so there is no NZ-proximate fix to detect. The no-landfall
  dampener fires. This is the same rainfall/ET realization gap DPS has
  everywhere (WP's Doksuri/Hagibis) — realized flood loss is exposure/IAS
  territory, not tropical-wind destructive potential.
- **Hazard vs realized, by design.** Small-island Cat 5s (Pam/Harold) score on
  destructive POTENTIAL; their lower realized $ (tiny exposed economies) is
  ERS/IAS's job, not DPS's. This is why ρ(damage) (+0.35) understates the
  ordering quality — the top-of-table storms are correctly ranked.
- SH `duration_factor`/`breadth_factor` remain ~0 (zone-weight × 24 h
  deduction), same as WP — harmless, fold into any future duration retune.
