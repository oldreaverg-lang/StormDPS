---
name: basin-dps-audit
description: Run a basin-specific DPS formula audit - sandbox-reimplement a basin's scoring path, compare against ground-truth storms, and write a report. Use when validating or calibrating the DPS formula for a basin (Atlantic, EP, WP, NI, SH) before changing live coefficients.
---

# Basin DPS formula audit

The repeatable pattern behind every prior basin/formula audit in `docs/audits/`. Goal: validate a proposed formula change against published ground truth WITHOUT editing live code first.

## Steps

1. **Pick basin + storms.** Choose 5+ ground-truth storms spanning the split, e.g. 3 "destructive but lower-category" + 2 "intensity-extreme". Use storms with published damage + intensity already in `core/ground_truth.py`.
2. **Sandbox-reimplement.** Write a companion script `scratch/<basin>_dps_audit.py` that reimplements the relevant `apply_basin_dps_adjustment` / scoring path in isolation - no live edits. Anchor it to the production-cached values in `frontend/compiled_bundle.json` so results are reproducible.
3. **Compare.** Run current vs proposed scores against ground truth; quantify error (e.g. mean absolute error vs published damage tiers; flag any basin Cat-5 that saturates the score regardless of landfall).
4. **Write the report** to `docs/audits/<BASIN>_DPS_AUDIT.md`, mirroring the existing reports' structure: Date, Scope, Methodology, Companion script, Related prior work, results table, recommendation.
5. **Only if the audit supports the change:** edit the live path (`core/`, `compile_cache.py`), then run the `compile-cache-bake` skill and re-verify the affected storms.

## References

- `docs/audits/` - prior audits (EP, WP, basin summary, duration/stall, storm-level Ian & Florence). Reuse their structure and tolerances.
- `core/ground_truth.py` - the ground-truth storm set.
