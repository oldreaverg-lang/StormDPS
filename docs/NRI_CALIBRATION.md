# FEMA NRI zone calibration

`frontend/nri_zones.json` holds per-coastal-zone `exposure × vuln` weights used by the **Economic Risk Score for active/forecast storms** (`get_economic_exposure(..., use_nri=True)` in `core/ike.py`). Historical presets use the hand-tuned `_ECON_ZONES` table in `core/ike.py` instead.

## To rebuild

```
C:\Python314\python.exe build_nri_zones.py
```

Queries FEMA NRI Counties ArcGIS layer per zone bbox. Writes a gitignored `nri_build_cache.json` so re-runs converge despite the flaky ArcGIS host.

**Always run audit after rebuild:**
```
C:\Python314\python.exe audit_nri_zones.py
```

## Design decisions

- **Exposure**: hand-tuned (FEMA's real building-value exposure compresses the scale — NYC metro dominates and deflates active ERS below presets).
- **Vulnerability**: data-driven from FEMA SOVI + Historic Loss Ratio.
- Community Resilience deliberately excluded — inverted post-levee New Orleans to a "resilient" 0.84.
- Territories (PR/USVI) lack SOVI → keep hand-tuned vuln.

## Known limitations (flagged by audit script)

- FEMA SOVI undercounts physical fragility of low-population barrier-island zones (Keys, Outer Banks, Big Bend).
- Florida Keys bbox overlaps Miami-Dade (inherits its vuln).
- NRI feeds **only** ERS. An orphaned NRI overlay in the economic-impact model (Formula 3) was removed (commit `bc17aa9`) so social vulnerability can't leak into the physical damage model.
