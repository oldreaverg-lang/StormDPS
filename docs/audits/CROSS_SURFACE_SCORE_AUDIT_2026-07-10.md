# Cross-surface DPS consistency audit — 2026-07-10

**Trigger:** Sinlaku (WP042026 / 2026099N09152) shows a wildly different score
in the hamburger sidebar vs the hero card. Operator asked how many storms share
the inconsistency.

**Method:** joined the live `/storms/catalog?min_year=1980` (3,787 rows — the
sidebar's data source) against `frontend/compiled_bundle.json` (223 storms —
the hero's data source) via `data/storm_aliases.json`; 208 storms joined.
Audit script: session scratchpad `score_audit.py` (stdlib only, re-runnable).

## Sinlaku, the worked example

| Surface | Value shown | Where it comes from |
|---|---|---|
| Sidebar (hamburger) | **36 "Moderate"** | catalog `peak_dps` — single-fix estimate, `core/ike.calculate_dps` |
| Map storm card (Peak tab) | **49.4** | engine `peak_dps` — peak single-snapshot, full engine |
| Hero card | **82.29 "Devastating"** | engine `dps` — cumulative lifetime score (incl. +18.3 RI, ×1.05 WP_MARIANA) |

Also: Sinlaku appears **twice** in the sidebar — the scored SID row plus a
scoreless `WP042026` "nhc-current" row (catalog dedup seam).

## Findings (quantified)

1. **Band disagreement sidebar↔hero: 131 / 208 storms (62%).**
   Numeric drift: mean Δ11.5 pts; ≥10 pts for 42%; ≥20 pts for 41 storms.
   Worst: Ike 30→89, Sandy 33→87, Irma 53→91 (Severe→Historic), Dorian 57→90,
   Ragasa 44→81. Drift is bidirectional (Rebekah 53→24).
2. **Sidebar vs map-peak (the like-for-like comparison — both nominally
   "peak"): mean Δ9.8 pts, ≥10 pts for 36%.** So even against the same
   quantity, the catalog's estimator disagrees badly.
3. **Off-canon labels: 1,034 / 3,731 scored catalog rows** — "Minor" (895)
   and "Catastrophic" (139) don't exist in the canonical 7-band scheme
   (`core/dpi.categorize_dpi`). The catalog can NEVER say Historic,
   Devastating, or Low: both of its label paths predate the canon.
4. **Duplicate identities: 21 storms** appear as two sidebar rows (IBTrACS SID
   + current-season ATCF id): Sinlaku 2026, Fina 2026, Awo/Blossom/Chenge/
   Bakung/Grant/Hayley (SH 2026), etc.
5. **56 rows have no score at all** (custom/nhc-current rows) — sidebar falls
   back to a category label.

## Root causes (three distinct defects)

A. **Different quantity under the same name.** Catalog `peak_dps` is a
   single-fix estimate; hero `dps` is the cumulative lifetime score with
   duration, RI, exposure and basin adjustments. Both render as "DPS n".
   (The map's snapshot-DPS vs the hero's lifetime score is the same semantic
   split, but that one is by design — the storm card is a timeline view.)

B. **Different formula.** `services/noaa_client.py` scores the catalog from
   the max-wind fix via `core/ike.calculate_dps` (no duration/RI/basin, crude
   surge proxy) — or a wind-only fallback when IBTrACS lacks pressure/radii
   at that fix. Finding 2 shows this estimator is unreliable even for "peak".

C. **Pre-canon label schemes.** BOTH `core/ike.calculate_dps` (ike.py ~1474)
   and the wind-only fallback (noaa_client.py ~1298) band as
   80+ Catastrophic / 10-20 Minor — the scheme `core/dpi.categorize_dpi`
   replaced. `_dpsBandLabel`'s comment in index.html ("bands must stay in
   lockstep so no surface disagrees") was honored on the frontend but never
   propagated to these two server functions.

## Recommended fix — **SHIPPED 2026-07-10 (`67d1c2d`), operator-approved**

All four fixes below are live and verified: catalog 3,766 rows (21 twins
collapsed), 0 off-canon labels, Sinlaku = one row at 82 "Devastating"
(engine) == hero, selfcheck `score_consistency` probe green (208 joined,
0 mismatched). Implementation: `core/storm_identity.harmonize_catalog`
applied at all three catalog serve boundaries (engine scores from the
compiled bundle first, then the live DPS volume cache for unbaked
current-season storms); five copies of the pre-canon label scheme
(core/ike.calculate_dps, noaa_client fallback, index.html calculateDPS,
DPS_ZONES, DPS chart tooltip) now route through categorize_dpi/getDPSBand;
faq.html's stale "Ike 84 (Catastrophic)" corrected to 89 (Devastating).
Original plan follows.


Ranked, all scoring-neutral (no formula change, no bake):

1. **Bundle wins where baked:** when serving `/storms/catalog`, overlay
   `peak_dps`/`dps_label` from the compiled bundle (via the alias table) for
   the ~208 storms that have real engine scores. Sidebar, hero, and SSR then
   read the same number by construction. (Serve-time decoration or
   catalog-build-time; volume snapshot has 6h TTL — force `admin/warm-ibtracs`
   after deploy.)
2. **Canonical labels everywhere:** make `core/ike.calculate_dps` and the
   noaa_client fallback call `core/dpi.categorize_dpi` instead of their local
   schemes. Kills "Minor"/"Catastrophic" (backlog quick-win #3).
3. **Dedup via alias table** (DATA_ARCHITECTURE roadmap #5): collapse the 21
   SID+ATCF twins at catalog build; keep the scored row, merge the fresher
   name/position metadata.
4. **Seam-3 selfcheck probe** (roadmap #3): extend `/health/selfcheck` to
   assert catalog score == bundle score for a sample of baked storms, so this
   class of drift pages the healthcheck cron instead of a user noticing.
5. (UX polish, optional) Distinguish the two quantities visually: hero label
   is the lifetime score; map storm card could say "DPS at this fix" — the
   62% disagreement above is NOT this, but the vocabulary invites confusion.

**Not recommended:** trying to make the single-fix estimator match the full
engine — finding 2 shows it can't; replacing its output with engine numbers
(fix 1) is both cheaper and exact.
