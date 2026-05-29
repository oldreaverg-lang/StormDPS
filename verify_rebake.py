"""
Before/after verifier for the IKE re-bake.

  python verify_rebake.py snapshot baseline_ike.json   # capture current bundle
  python verify_rebake.py compare  baseline_ike.json   # diff current vs baseline

Extracts peak_ike_tj per storm from frontend/compiled_bundle.json and reports
preset deltas + across-the-board change/sanity stats.
"""
import json
import sys
from pathlib import Path

BUNDLE = Path(__file__).parent / "frontend" / "compiled_bundle.json"

PRESETS = {
    "AL122005": "Katrina", "AL092024": "Helene", "AL152017": "Maria",
    "AL112017": "Irma", "AL092022": "Ian", "AL142018": "Michael",
    "AL142024": "Milton", "AL182012": "Sandy", "AL092008": "Ike",
    "AL092017": "Harvey", "AL052019": "Dorian", "AL062018": "Florence",
    "AL102023": "Idalia", "AL022024": "Beryl",
}


def extract():
    b = json.load(open(BUNDLE, encoding="utf-8"))
    storms = b["storms"]
    return {sid: {"name": st.get("name"), "year": st.get("year"),
                  "basin": st.get("basin"), "peak_ike_tj": st.get("peak_ike_tj")}
            for sid, st in storms.items()}


def main():
    mode, path = sys.argv[1], sys.argv[2]
    if mode == "snapshot":
        data = extract()
        json.dump(data, open(path, "w", encoding="utf-8"))
        print(f"baseline captured: {len(data)} storms -> {path}")
        return

    before = json.load(open(path, encoding="utf-8"))
    after = extract()
    rows = []
    for sid, a in after.items():
        ov = (before.get(sid) or {}).get("peak_ike_tj")
        nv = a.get("peak_ike_tj")
        if ov is None or nv is None:
            continue
        rows.append((sid, a.get("name"), a.get("basin"), ov, nv))

    print("PRESETS (peak IKE_TS, TJ):")
    print(f"  {'id':10} {'name':9} {'basin':10} {'old':>8} {'new':>8}  change")
    for sid in PRESETS:
        m = next((r for r in rows if r[0] == sid), None)
        if not m:
            print(f"  {sid:10} {PRESETS[sid]:9} (not in bundle)")
            continue
        _, name, basin, ov, nv = m
        pct = (nv - ov) / ov * 100 if ov else float("inf")
        print(f"  {sid:10} {(name or '')[:9]:9} {(basin or '')[:10]:10} {ov:8.1f} {nv:8.1f}  {pct:+6.0f}%")

    changed = sum(1 for r in rows if abs(r[4] - r[3]) > 0.05)
    unchanged = len(rows) - changed
    negs = [r for r in rows if r[4] < 0]
    big = [r for r in rows if r[4] > 600]
    print(f"\nAcross all {len(rows)} storms with old+new values:")
    print(f"  changed:   {changed}")
    print(f"  unchanged: {unchanged}")
    print(f"  sanity -> negative IKE: {len(negs)} | >600 TJ (suspicious): {len(big)}")
    if big:
        print("    >600 TJ:", [(r[0], round(r[4])) for r in big][:12])
    if unchanged:
        print("  UNCHANGED sample (formula may not have reached these):",
              [r[0] for r in rows if abs(r[4] - r[3]) <= 0.05][:12])


if __name__ == "__main__":
    main()
