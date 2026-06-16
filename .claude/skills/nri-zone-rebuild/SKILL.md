---
name: nri-zone-rebuild
description: Rebuild frontend/nri_zones.json from the FEMA National Risk Index and audit the result. Use when the active/forecast Economic Risk Score (ERS) calibration changes, FEMA NRI data updates, or nri_zones.json needs regenerating.
---

# Rebuild NRI economic-risk zones

`frontend/nri_zones.json` holds the per-coastal-zone `exposure x vuln` weights used by the active/forecast Economic Risk Score (`core/ike.get_economic_exposure(..., use_nri=True)`). Historical **presets** use the hand-tuned `_ECON_ZONES` table in `core/ike.py` instead - do not change those here.

## Steps

1. From the repo root, rebuild the zones:
   ```
   python build_nri_zones.py
   ```
   Writes `frontend/nri_zones.json`. It also writes a gitignored `nri_build_cache.json` (per-zone FEMA stats) so re-runs converge despite the flaky FEMA ArcGIS host - leave that cache in place between runs.
2. ALWAYS audit immediately after a rebuild:
   ```
   python audit_nri_zones.py
   ```
   Checks vuln ranges, bbox-overlap contamination, divergence from the hand-tuned anchor, fragile-zone undercount, and effective-exposure ranking.
3. Read the audit output. Expect these known flags - do NOT "fix" them:
   - FEMA SOVI undercounts the physical fragility of low-population barrier islands (Keys, Outer Banks, Big Bend).
   - The Florida Keys bbox overlaps Miami-Dade and inherits its vulnerability.
   - Territories (PR/USVI) lack SOVI, so their vuln stays hand-tuned.
4. If the audit passes, commit **only** `frontend/nri_zones.json` (never `nri_build_cache.json`). Use the `github-safe-push` skill.

## Design invariants (do not violate)

- **Exposure** stays hand-tuned. FEMA building-value exposure compresses the scale (NYC metro dominates) and deflates active ERS below the presets.
- **Vulnerability** is data-driven from FEMA SOVI + Historic Loss Ratio. Community Resilience is deliberately excluded (it inverted post-levee New Orleans to a "resilient" 0.84).
- NRI feeds **ERS only** - never the physical damage model (Formula 3).

## Reference

- `docs/NRI_CALIBRATION.md` - full procedure, design decisions, and known limitations.
