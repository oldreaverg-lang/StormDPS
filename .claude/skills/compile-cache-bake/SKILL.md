---
name: compile-cache-bake
description: Rebake the pre-computed DPS bundle (frontend/compiled_bundle.json) after any change to the scoring formulas. Use when core/dps_engine, core/ike, cumulative_dpi, compile_cache.py, ground truth, or basin coefficients change and the baked historical scores need regenerating.
---

# Rebake the compiled DPS bundle

`frontend/compiled_bundle.json` (~3.5 MB, ~200 storms) is the pre-computed DPS bundle the frontend loads instead of recomputing. It is BAKED by `compile_cache.py` and must be regenerated whenever scoring logic changes, or the displayed historical scores drift from the engine.

## When to run

After editing any of: `core/dps_engine.py`, `core/ike.py`, `core/cumulative_dpi.py`, `compile_cache.py`, `core/ground_truth.py`, or the basin adjustment path / `frontend/basin_dps_coefficients.js`.

**Timing caveat:** don't bake while a WP/EP storm with a <7-day-old track and no
land contact is present in the preload data. `apply_basin_dps_adjustment` DEFERS
the ×0.60 no-landfall dampener for in-progress tracks (`track_is_in_progress`),
so baking mid-storm would freeze the undampened score into `compiled_bundle.json`
until the next bake. Wait until the storm's last fix is >7 days old (the deferral
expires on its own), or exclude the storm from the bake.

## Steps

1. **One command** (preferred) from the repo root:
   ```
   python scripts/rebake.py
   ```
   This chains the whole ritual: `compile_cache.py` (rebakes
   `frontend/compiled_bundle.json` and, via its own hooks, refreshes the
   methodology validation section + `sitemap.xml`) → `tests/gen_scoring_baseline.py`
   (re-freezes the golden-master baseline so the drift-lock test passes) → bumps
   `frontend/sw.js` `CACHE_NAME` (so returning visitors get the new bundle — it's
   fetched cache-first, else stale). It prints the git commands and does NOT push.

   To run a step alone, just run that script directly (e.g. `python compile_cache.py`).
   NB the bake reads `preload_bundle.json` and writes `compiled_bundle.json` only.

   **Then (bundle diet, 2026-07-10): chain the derived artifacts** — the SPA no
   longer loads the monolith; it eager-loads `frontend/bundle_index.json` and
   fetches `frontend/bundle_storm/<id>.json` per storm. Both are DERIVED from
   the monolith and MUST be regenerated in the same bake (the offline suite's
   `tests/test_bundle_split.py` fails if they drift):
   ```
   python build_actual_impact.py          # re-attach observed damage/FEMA
   python scripts/build_bundle_split.py   # regenerate index + detail files
   ```
   Also bump `BUNDLE_VERSION` in `frontend/index.html` (the SPA fetches both
   artifacts with `?v=<BUNDLE_VERSION>`; they get the standard /frontend
   .json cache headers) and add
   `frontend/bundle_index.json frontend/bundle_storm/` to the commit.
   (`scripts/rebake.py` and `.github/workflows/auto-rebake.yml` already
   chain all of this — prefer them over running steps by hand.)
2. Verify structural invariants. `pytest` may be absent in the embedded Python, so
   either `pytest tests/test_compiled_bundle.py tests/test_scoring_baseline.py -q`
   or inline-check: storm_count == len(storms); every storm has a numeric `dps` in
   [0,100]; `actual_impact` count did NOT drop; no duplicate storms.
3. Spot-check: Katrina/Ian/Irma high, weak systems low; Maria rainfall ~38 in.
4. **Commit the bundle from native Windows git (PowerShell `git -C`), not the bash
   mount** — `compiled_bundle.json` exceeds the mount's safe-read size (~50 KB) and
   can be silently truncated. For normal-sized source edits use `github-safe-push`.
   ```
   git add frontend/compiled_bundle.json frontend/methodology.html \
           frontend/sitemap.xml tests/data/scoring_baseline.json frontend/sw.js
   git commit -m "Rebake compiled DPS bundle"
   git push origin main
   ```
5. After the push deploys, run the `deploy-verify` skill.

## Reliability notes (2026-06-26)

The full bake was hardened so it can't silently degrade the bundle:
- **Dedup is genesis-fingerprint based** (`_genesis_fingerprint`), so IBTrACS-SID
  duplicates of AL/EP storms are dropped even when the local catalog cache is
  absent (which makes `_auto_detect_meta` return the SID instead of the name).
  Previously a local bake leaked 19 duplicate storms.
- **Fail-closed on names and FEMA `actual_impact`**: the bake reads the prior
  bundle first and carries forward any storm name or `actual_impact` block this run
  can't reproduce (no catalog → SID-name; a flaky OpenFEMA fetch → fewer storms).
  A bake therefore never reduces name or impact coverage.
