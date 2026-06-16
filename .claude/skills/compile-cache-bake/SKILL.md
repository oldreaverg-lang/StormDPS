---
name: compile-cache-bake
description: Rebake the pre-computed DPS bundle (frontend/compiled_bundle.json) after any change to the scoring formulas. Use when core/dps_engine, core/ike, cumulative_dpi, compile_cache.py, ground truth, or basin coefficients change and the baked historical scores need regenerating.
---

# Rebake the compiled DPS bundle

`frontend/compiled_bundle.json` (~3.5 MB, ~200 storms) is the pre-computed DPS bundle the frontend loads instead of recomputing. It is BAKED by `compile_cache.py` and must be regenerated whenever scoring logic changes, or the displayed historical scores drift from the engine.

## When to run

After editing any of: `core/dps_engine.py`, `core/ike.py`, `core/cumulative_dpi.py`, `compile_cache.py`, `core/ground_truth.py`, or the basin adjustment path / `frontend/basin_dps_coefficients.js`.

## Steps

1. From the repo root:
   ```
   python compile_cache.py
   ```
   Writes `frontend/compiled_bundle.json` and `frontend/preload_bundle.json`.
2. Verify structural invariants before committing:
   ```
   pytest tests/test_compiled_bundle.py -q
   ```
   (storm_count matches len(storms); every storm has a numeric `dps` in [0,100]; identity fields present.)
3. Spot-check known storms: high for Katrina/Ian, low for weak systems. Active storms are scored client-side, so confirm baked presets still agree with the engine within the expected ~1-4 pt tolerance.
4. **Commit the bundle from native Windows git, not the sandbox.** `compiled_bundle.json` and `preload_bundle.json` exceed the mount's safe-read size (~50 KB) and can be silently truncated if read through the mount:
   ```
   git add frontend/compiled_bundle.json frontend/preload_bundle.json
   git commit -m "Rebake compiled DPS bundle"
   git push
   ```
   For normal-sized source edits, use the `github-safe-push` skill instead.
5. After the push deploys, run the `deploy-verify` skill.
