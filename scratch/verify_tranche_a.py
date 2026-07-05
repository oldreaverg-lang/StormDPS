"""Verify Tranche A live edits (R2 hygiene + WP geometry + wind^2 labels).

Runs the LIVE modules as edited on disk — no monkeypatches. Checks:
  1. 18-storm WP set: new scores, rho, Surigae dampened, Haiyan label fixed.
  2. All 244 preload storms: recompute vs compiled_bundle.json, report every
     mover by basin (Atlantic movers = sustained-IKE/radii-gate side effects
     that must be individually justifiable before the bake).
"""
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scratch"))

from wp_calibration import STORMS, load_snaps, spearman
from core.dps_engine import compute_storm_dps

PRE = json.loads((REPO / "frontend" / "preload_bundle.json").read_text(encoding="utf-8"))["storms"]
COMP = json.loads((REPO / "frontend" / "compiled_bundle.json").read_text(encoding="utf-8"))["storms"]

print("=== 18-storm WP set (LIVE modules, Tranche A) ===")
results = {}
for sid, (name, year, dmg, deaths) in STORMS.items():
    b = compute_storm_dps(storm_id=sid, snapshots=load_snaps(sid),
                          storm_name=name, storm_year=year)
    results[sid] = b
for sid in sorted(STORMS, key=lambda s: -results[s]["dps"]):
    name, year, dmg, deaths = STORMS[sid]
    b = results[sid]
    old = COMP.get(sid, {}).get("dps")
    old_s = f"{old:5.1f}" if old is not None else "  new"
    print(f"  {name:9s}: {old_s} -> {b['dps']:5.1f}  peak={b['peak_dps']:5.1f} "
          f"ike={b['peak_ike_tj']:6.1f} [{b['adjustment_notes']}] (${dmg}B/{deaths}d)")
dps = [results[s]["dps"] for s in STORMS]
r_d = spearman(dps, [STORMS[s][2] for s in STORMS])
r_k = spearman(dps, [STORMS[s][3] for s in STORMS])
print(f"  rho damage {r_d:+.3f} | deaths {r_k:+.3f}   (V0 was +0.176 / +0.073)")

sur = results["2021102N06144"]
assert "no-landfall" in sur["adjustment_notes"], "Surigae still escapes the dampener!"
hai = results["WP312013"]
assert "WP_PHILIPPINES" in hai["adjustment_notes"], f"Haiyan label wrong: {hai['adjustment_notes']}"
print("  checks: Surigae dampened OK; Haiyan -> WP_PHILIPPINES OK")

print("\n=== Full preload recompute vs compiled bundle (movers > 0.05) ===")
NAMES = {sid: (s.get("name", sid), s.get("year", 0)) for sid, s in COMP.items()}
movers = []
for sid, snaps in PRE.items():
    if sid not in COMP:
        continue
    nm, yr = NAMES[sid]
    b = compute_storm_dps(storm_id=sid, snapshots=snaps, storm_name=nm, storm_year=yr)
    delta = b["dps"] - COMP[sid]["dps"]
    if abs(delta) > 0.05:
        movers.append((abs(delta), delta, nm, yr, COMP[sid]["dps"], b["dps"],
                       COMP[sid].get("basin"), b["adjustment_notes"]))
movers.sort(reverse=True)
by_basin = {}
for _, d, nm, yr, o, n, bas, notes in movers:
    by_basin.setdefault(bas, []).append((nm, yr, o, n, d, notes))
for bas, rows in sorted(by_basin.items()):
    print(f"  {bas}: {len(rows)} mover(s)")
    for nm, yr, o, n, d, notes in rows[:15]:
        print(f"    {nm} {yr}: {o:.1f} -> {n:.1f} ({d:+.1f})  [{notes}]")
print(f"\n  total movers: {len(movers)} / {len(PRE)}")
atl = [m for m in movers if m[6] == "ATLANTIC"]
print(f"  ATLANTIC movers: {len(atl)}, max |delta| = {max((m[0] for m in atl), default=0):.2f}")
