# Loose-ends audit — built but not implemented

**Date:** 2026-07-02
**Method:** clean GitHub clone → Python import-graph reachability from entry points (main, api/*, compile_cache, scripts, tests), JS function-definition vs call-site analysis, `getElementById` targets vs HTML ids, frontend `${API_BASE}` fetches vs backend route decorators, marker grep (zero TODO/FIXME exist — all findings are structural).

---

## A. Unfinished features (significant work exists, feature unreachable)

### A1. Map overlay control row — satellite / IR / pressure / precip / wind toggles ⚠️ biggest find
Six toggle-button ids are managed extensively by JS but **do not exist anywhere in the HTML**:
`satToggleBtn`, `irToggleBtn`, `pressureToggleBtn`, `precipToggleBtn`, `windToggleBtn`, `windfieldToggleBtn` — plus 14 companion ids (`satelliteSliderInput`, `satelliteFrameLabel`, `satelliteName`, `satelliteProductLabel`, `windLegend`, `pressureLegend`, `precipLegend`, `*OpacityInput`, `*OpacityLabel`).

What exists and works: backend `/satellite/frames/{sat}` + `/satellite/tile/.../{z}/{x}/{y}.png` routes (api/satellite_routes.py), the full frontend layer machinery (frame fetching, tile URL construction, `onSatelliteFrameScrub`, `togglePrecipLayer`, `togglePressureLayer`, historic-storm row hiding at ~line 5180, active-storm row restore at ~line 4941, per-button enable/disable styling in ~30 places). The JS manipulates `btn.parentElement` — the design assumed a button-row container that never landed (or was lost in one of the historical index.html truncation incidents).

**Fix:** add the overlay row markup next to the existing `windDirToggleBtn`/`observedToggleBtn` buttons (~line 756) with the 6 buttons + slider/labels, or delete the ~500 lines of layer JS. Given the backend is live and the JS is complete, adding the markup is days-of-work already banked for an afternoon of wiring.

### A2. `frontend/basin_dps_coefficients.js` — never loaded, drifted duplicate
No `<script src>` anywhere references it. The same `BASIN_COEFFICIENTS`/`applyBasinDPS`/`window.BasinDPS` logic is **inlined** in index.html (~line 880), and the two copies have drifted (external: EP `ri_bonus: 15`; inline: `ri_bonus:0` everywhere). The compile-cache-bake skill even lists this file as something to keep in sync — a trap for future changes. **Fix:** delete the file (inline copy is canonical) and remove the mention from the bake skill.

### A3. `services/mrms_client.py` — orphaned, but CLAUDE.md says it's active
Imported by nothing. Rainfall uses `imerg_rainfall` + the curated ground-truth registry instead. CLAUDE.md's "Active ones" list includes `mrms_client` — doc is wrong or the MRMS integration was never finished (the 2026-07 formula audit's rainfall follow-up could be its purpose). **Fix:** either wire MRMS QPE in as the rainfall-footprint data source (see ATLANTIC_RI_COMPRESSION_AUDIT.md follow-ups) or move to archive/ and fix CLAUDE.md.

### A4. Orphaned service clients: `services/ncei_storm_events.py`, `services/fema_client.py`
Zero imports. NCEI is superseded by the hand-curated `core/ground_truth.py`; FEMA impact goes through `build_actual_impact.py`/OpenFEMA instead. **Fix:** archive both, or note their intended role.

## B. Dead code (safe deletes)

- `ikeColor()` in index.html (~line 1749) — never called; track/chart coloring uses `dpsColor`. (Both were just updated for the TD-gray contrast fix — only `dpsColor` mattered.)
- `isAtcfId()` / `isIbtracsSid()` — validation helpers never called (lookup accepts IDs via other paths).
- `onSatelliteFrameScrub` / `togglePrecipLayer` / `togglePressureLayer` — dead only because A1 is unfinished; keep if restoring the overlay row.
- Root-level one-off pipeline scripts (`build_preload.py`, `batch_fetch_storms.py`, `batch_compute_ike.py`, `prewarm_catalog.py`, `rebake_ike_cache.py`) — unreferenced by code or docs. Probably intentional manual utilities; suggest a "Pipeline utilities" section in CLAUDE.md or move under scripts/ so the next audit doesn't re-flag them.

## C. Not loose ends (checked, fine)

- `validate_*` / `audit_*` / `verify_rebake` root scripts — wired into pytest as `--run-integration` tests (tests/test_validators.py).
- `ssrStormSummary` / `.ssr-hidden` — emitted by seo.py SSR, hidden by the SPA on boot; fully wired.
- `dispatchInitialStorm` — named function expression passed to `_onDomReady`; not dead.
- archive/ and audits/ directories — explicitly archival.
- Backend routes with no frontend caller (`/audit/radii/*`, `/validation/*`, `/cache/*`, weather_routes' `/alerts|/conditions|/terrain|...`) — public/ops API surface, reachable by design even if the SPA doesn't call them. Worth a docs/API page eventually, not a bug.

## D. Doc drift to fix in CLAUDE.md

- "Active ones: … `mrms_client` …" — remove (A3).
- Open items 1–2 (PageSpeed re-test after 724571e; IBTrACS warm deferral) — verify whether still open; lifespan comments suggest the warm staggering already landed.
