"""
Sanity-audit the rebuilt nri_zones.json (active/forecast ERS calibration).

Checks the FEMA-derived values against domain expectations and the hand-tuned
expert anchor. Flags:
  RANGE / CAP    - vuln outside [0.6,1.5] or pinned at a cap
  DUP-VULN       - zones sharing an identical vuln (tell-tale of a bbox overlap
                   pulling the same counties into two zones)
  VULN-DIVERGE   - FEMA vuln disagrees with the hand-tuned expert value by >=0.30
  FRAGILE-LOW    - a physically fragile zone (barrier island / below sea level /
                   single evac route / isolated grid) rated vuln < 1.15
  RANK           - a rural zone's effective exposure outranks a major metro
"""
import json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from build_nri_zones import ZONES   # (name, latmin,latmax,lonmin,lonmax, h_exp, h_vuln)

new = json.loads((ROOT / "frontend" / "nri_zones.json").read_text())
hand = {z[0]: (z[5], z[6]) for z in ZONES}

KNOWN_FRAGILE = {
    "Florida Keys", "Outer Banks NC", "Houma / Terrebonne LA", "Big Bend FL (rural)",
    "Nature Coast FL (rural)", "San Juan PR Metro", "US Virgin Islands",
    "Matagorda / Victoria TX", "South Padre / Brownsville", "Panama City FL",
}
RURAL = {
    "Nature Coast FL (rural)", "Big Bend FL (rural)", "Matagorda / Victoria TX",
    "South Padre / Brownsville", "Houma / Terrebonne LA", "Outer Banks NC",
}
MAJOR = {
    "New Orleans Metro", "Miami-Dade Metro", "NYC Metro / Long Island",
    "Houston / Galveston Metro", "Tampa Bay Metro",
}

flags = []

# 1. range / caps
for z, v in new.items():
    if not (0 <= v["exposure"] <= 1.0):
        flags.append(("RANGE", z, f"exposure {v['exposure']} out of [0,1]"))
    if not (0.6 <= v["vuln"] <= 1.5):
        flags.append(("RANGE", z, f"vuln {v['vuln']} out of [0.6,1.5]"))
    if v["vuln"] in (0.6, 1.5):
        flags.append(("CAP", z, f"vuln pinned at cap {v['vuln']}"))

# 2. duplicate vuln (bbox overlap pulling the same counties)
byv = {}
for z, v in new.items():
    byv.setdefault(round(v["vuln"], 2), []).append(z)
for vv, zs in sorted(byv.items()):
    if len(zs) > 1:
        flags.append(("DUP-VULN", " / ".join(zs), f"identical vuln {vv}"))

# 3. vuln divergence from hand-tuned expert anchor
for z, v in new.items():
    if z in hand:
        d = v["vuln"] - hand[z][1]
        if abs(d) >= 0.30:
            flags.append(("VULN-DIVERGE", z, f"FEMA {v['vuln']} vs expert {hand[z][1]} (delta {d:+.2f})"))

# 4. fragile zone rated low
for z in sorted(KNOWN_FRAGILE):
    if z in new and new[z]["vuln"] < 1.15:
        flags.append(("FRAGILE-LOW", z, f"vuln {new[z]['vuln']} for a physically fragile zone (expert {hand.get(z,('?','?'))[1]})"))

# 5. ranking sanity
eff = sorted(((z, round(v["exposure"] * v["vuln"], 3)) for z, v in new.items()), key=lambda x: -x[1])
major_effs = [e for z, e in eff if z in MAJOR]
major_min = min(major_effs) if major_effs else 0
for z, e in eff:
    if z in RURAL and e > major_min:
        flags.append(("RANK", z, f"rural effective {e} exceeds a major metro (min major {major_min})"))

print("=== effective-exposure ranking (exposure x vuln), high -> low ===")
for z, e in eff:
    tag = " [MAJOR]" if z in MAJOR else (" [rural]" if z in RURAL else "")
    print(f"  {e:5.3f}  {z}{tag}")

print(f"\n=== {len(flags)} FLAG(S) ===")
for cat, z, msg in flags:
    print(f"  [{cat:13}] {z}: {msg}")
