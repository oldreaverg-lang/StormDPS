"""
Audit + sanity check of the preloaded historical storms in
frontend/compiled_bundle.json (post IKE re-bake).

Checks counts/basins, DPS/IKE/SDP distributions, top & bottom rankings,
cross-field consistency (DPS vs wind, category vs wind, data completeness),
flags anomalies for review, and prints the 14 named presets in detail.
"""
import json
import math
import statistics
from collections import Counter
from pathlib import Path

B = json.load(open(Path(__file__).parent / "frontend" / "compiled_bundle.json", encoding="utf-8"))
S = B["storms"]

PRESETS = {
    "AL122005": "Katrina", "AL092024": "Helene", "AL152017": "Maria",
    "AL112017": "Irma", "AL092022": "Ian", "AL142018": "Michael",
    "AL142024": "Milton", "AL182012": "Sandy", "AL092008": "Ike",
    "AL092017": "Harvey", "AL052019": "Dorian", "AL062018": "Florence",
    "AL102023": "Idalia", "AL022024": "Beryl",
}


def sdp_from_ike(ike):
    if not ike or ike <= 0:
        return 0.0
    r = math.sqrt(ike)
    return max(0.0, min(5.99, 0.676 + 0.43 * r - 0.0176 * (r - 6.5) ** 2))


def sdp_label(s):
    return ("Extreme" if s >= 5 else "Very High" if s >= 4 else "High" if s >= 3
            else "Moderate" if s >= 2 else "Low" if s >= 1 else "Minimal")


def cat_from_kt(kt):
    if not kt:
        return "?"
    for thr, lab in [(137, "5"), (113, "4"), (96, "3"), (83, "2"), (64, "1"), (34, "TS")]:
        if kt >= thr:
            return lab
    return "TD"


rows = []
for sid, st in S.items():
    r = {
        "id": sid, "name": st.get("name") or sid, "year": st.get("year"),
        "basin": st.get("basin"), "peak_dps": st.get("peak_dps") or 0,
        "ike": st.get("peak_ike_tj") or 0, "wind_kt": st.get("peak_wind_kt") or 0,
        "cat": st.get("category"), "snaps": st.get("snapshot_count") or 0,
        "pres": st.get("min_pressure_hpa"),
    }
    r["sdp"] = sdp_from_ike(r["ike"])
    rows.append(r)


def line(r):
    return (f"{r['name'][:15]:15} {str(r['year']):>4}  DPS={r['peak_dps']:5.1f}  "
            f"{r['wind_kt']:3.0f}kt(C{cat_from_kt(r['wind_kt']):>2})  IKE={r['ike']:5.0f}TJ  "
            f"SDP={r['sdp']:.1f}")


print(f"=== {len(rows)} preloaded storms ===")
print("basins:", dict(Counter(r["basin"] for r in rows)))
yrs = [r["year"] for r in rows if r["year"]]
print(f"years: {min(yrs)}-{max(yrs)}")

dps = [r["peak_dps"] for r in rows]
bands = Counter()
for d in dps:
    bands["80-100" if d >= 80 else "60-79" if d >= 60 else "40-59" if d >= 40
          else "20-39" if d >= 20 else "0-19"] += 1
print(f"\npeak DPS: min={min(dps):.0f} median={statistics.median(dps):.0f} "
      f"mean={statistics.mean(dps):.0f} max={max(dps):.0f}")
print("DPS bands:", dict(sorted(bands.items(), reverse=True)))
ike = [r["ike"] for r in rows]
print(f"peak IKE: min={min(ike):.0f} median={statistics.median(ike):.0f} max={max(ike):.0f} TJ")
print("SDP bands:", dict(Counter(sdp_label(r["sdp"]) for r in rows)))

print("\nTop 15 by peak DPS:")
for r in sorted(rows, key=lambda x: -x["peak_dps"])[:15]:
    print("  " + line(r))
print("\nBottom 8 by peak DPS:")
for r in sorted(rows, key=lambda x: x["peak_dps"])[:8]:
    print("  " + line(r) + f"  snaps={r['snaps']}")

print("\n=== ANOMALY CHECKS ===")
a = sorted([r for r in rows if r["peak_dps"] >= 70 and r["wind_kt"] < 96], key=lambda x: -x["peak_dps"])
print(f"[1] DPS>=70 but < Cat3 wind ({len(a)}) — large/surge-driven, verify plausible:")
for r in a:
    print("    " + line(r))
b2 = sorted([r for r in rows if r["wind_kt"] >= 113 and r["peak_dps"] < 45], key=lambda x: -x["wind_kt"])
print(f"[2] Cat4+ wind but DPS<45 ({len(b2)}) — possible under-score:")
for r in b2:
    print("    " + line(r))
c = [r for r in rows if r["peak_dps"] >= 40 and r["ike"] < 1]
print(f"[3] DPS>=40 but ~0 IKE ({len(c)}):", [r["name"] for r in c][:12])
e = [r for r in rows if r["snaps"] < 4]
print(f"[4] Sparse (<4 snapshots) ({len(e)}):", [(r["name"], r["snaps"]) for r in e][:12])
f = sorted([r for r in rows if r["ike"] > 220], key=lambda x: -x["ike"])
print(f"[5] IKE > 220 TJ ({len(f)}):", [(r["name"], round(r["ike"])) for r in f])
g = [r for r in rows if r["peak_dps"] <= 0]
print(f"[6] zero/negative DPS ({len(g)}):", [r["name"] for r in g][:12])

print("\n=== 14 NAMED PRESETS ===")
for pid, nm in PRESETS.items():
    r = next((x for x in rows if x["id"] == pid), None)
    if not r:
        print(f"  {nm}: MISSING from bundle")
        continue
    print(f"  {nm:9} DPS={r['peak_dps']:5.1f}  {r['wind_kt']:3.0f}kt(C{cat_from_kt(r['wind_kt']):>2})  "
          f"IKE={r['ike']:5.0f}TJ  SDP={r['sdp']:.1f} ({sdp_label(r['sdp'])})  "
          f"pres={r['pres']}  cat_field={r['cat']}")
