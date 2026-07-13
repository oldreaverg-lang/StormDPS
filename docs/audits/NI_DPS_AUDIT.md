# North Indian DPS Audit — dead-legs activation (the surge basin)

**Date:** 2026-07-12
**Scope:** NORTH_INDIAN basin (Bay of Bengal + Arabian Sea). Full engine path.
**Companion harness:** session-scratch `ni_v0.py` (14 famous cyclones,
tracks via `/track`).
**Ground truth:** commonly cited damage/death totals.

---

## 1. Headline

NI was the last dead-legs basin — the same pathology WP and SH had, in the
basin where it matters most. Every NI cyclone scored `reg=Open Ocean`,
`coastal_hours=0`, no bonuses. A flat 1.15 "surge dominance" multiplier
hand-waved at surge without ever modeling it — in the *deadliest, most
surge-dominated basin on Earth* (Bhola 1970 ~300k+ dead, 1991 Bangladesh
~138k, Nargis 2008 ~138k). Result: no NI cyclone could reach the Extreme
band; the $14B Amphan scored 50, Nargis capped at 68.

## 2. Current-state validation (V0)

ρ(damage) +0.63 (higher than SH's +0.30 only because these storms are
intensity-homogeneous and the 1.15 multiplier happens to help), but every
storm open-ocean, jammed 10–68, surge — the defining hazard — contributing
nothing. Amphan ($14B, costliest NI cyclone) 50.8; Nargis (138,000 dead) 67.6.

## 3. Fix — living-legs activation (same architecture as WP/SH)

Additive, like SH (NI had no compensating layer worth keeping):

1. **~52 NI coastline waypoints** in `core/land_proximity.py` (`ni_*` keys):
   Bangladesh/Sundarbans, Odisha, Andhra, Tamil Nadu, Sri Lanka, Kerala,
   Myanmar/Irrawaddy, Gujarat/Pakistan, Oman/Yemen, Somalia. `nearest_ni_coast()`.
   Gated lat 0–31 N, lon 42–97 E; **checked BEFORE WP** so the 95–97 E
   Andaman/Myanmar overlap resolves to NI (Gulf of Thailand at 99 E+ stays WP).
2. **10 `ni_*` CoastalProfiles + EconomicProfiles**, econ ×0.85. The Bay of
   Bengal deltas carry the **highest `surge_amplification` (1.75) and
   `bathymetric_concavity` (0.42) in the whole system** — the wide shallow
   shelf + head-of-bay funnel + macro-tides. Arabian Sea steeper/drier.
   Population is delta-dense (Bangladesh 9.7) and delta-vulnerable (elevation
   0.9, building code 0.20).
3. Tiered profile reach, landfall-intensity bonus, multi-landfall, orographic
   (Western Ghats / Sri Lanka / Myanmar Arakan / Meghalaya), no-landfall
   dampener. Land-contact uses the wider 75 km gate (NI cyclones parallel the
   coast offshore — Tauktae — and weaken fast over the deltas).
4. NORTH_INDIAN `dps_multiplier` 1.15 → **1.00**, compression **T 70 → 80**.

## 4. Results (14-storm set, final)

| Cyclone | V0 → final | Note |
|---|---|---|
| Fani 2019 | 53 → **85** | Cat 5 Odisha strike |
| Mocha 2023 | 52 → **84** | Cat 5 Myanmar/Bangladesh, 463 dead |
| Nargis 2008 | 68 → **75** | Irrawaddy — 138,000 dead |
| Phailin 2013 | 50 → **73** | Cat 5 Odisha |
| Amphan 2020 | 51 → **69** | $14B — costliest NI cyclone |
| Hudhud 2014 | 50 → **64** | Vizag |
| Tauktae 2021 | 34 → **63** | Gujarat (needed a Una waypoint — parallels the coast) |
| Titli 2018 | 30 → **55** | Odisha |
| Yaas / Chapala | ~22–39 | moderate |
| Biparjoy / Remal / Gaja / Ockhi | 24–29 | weak/managed; Ockhi a marine (at-sea) disaster, near-fish for land |

ρ(damage) +0.63 → **+0.77**, ρ(deaths) → +0.45.

**Non-NI bit-identity: 0 movers** — there are no baked NI storms, and no WP
storm dips into the 95–97 E overlap, so the bundle is bit-identical (no
rebake/refreeze needed). Activation affects only live NI storms.
`_DPS_CACHE_VERSION` → v16 so warmed NI storms recompute.

## 5. Known limitations (accepted)

- **Hazard vs realized deaths.** Modern NI death tolls are far below the surge
  potential because India/Bangladesh now evacuate millions (Fani/Amphan/
  Biparjoy). DPS scores the surge *potential* (high); the evacuation success
  is an ERS/IAS story, not a DPS discount. This is why the deadliest storms
  don't always top the list and ρ(deaths) < ρ(damage).
- **Ockhi (marine disaster).** Its 245 deaths were fishermen caught at sea by
  a warning failure; it never made a significant India landfall (closest
  approach ~80 km) and correctly takes the no-landfall dampener. That gap is
  an at-sea-exposure/warning problem, not a coastal-hazard one.
- The landfall panel now covers NI (was "no coverage" — the `INDIAN_GAP` rule
  added during the WP tranche).
