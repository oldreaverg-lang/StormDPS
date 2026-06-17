#!/usr/bin/env python3
"""Regenerate tests/data/scoring_baseline.json from frontend/compiled_bundle.json.

Run ONLY when an intentional, approved scoring change lands - it re-freezes the
golden master that tests/test_scoring_baseline.py checks against.

    python tests/gen_scoring_baseline.py
"""
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
bundle = json.loads((ROOT / "frontend" / "compiled_bundle.json").read_text(encoding="utf-8"))
baseline = {
    sid: {"dps": s["dps"], "category": s.get("category"), "dps_label": s.get("dps_label")}
    for sid, s in bundle["storms"].items()
}
out = ROOT / "tests" / "data" / "scoring_baseline.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(baseline, indent=0, sort_keys=True) + "\n", encoding="utf-8")
print("wrote %d storms to %s" % (len(baseline), out))
