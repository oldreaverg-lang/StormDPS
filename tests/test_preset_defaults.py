"""
The sidebar's historic-storm chips paint from the hard-coded PRESETS list in
frontend/index.html, then refreshPresetDPSFromCompiled() swaps in the
compiled_bundle.json scores. When the two disagree, every page load shows the
chips change value and re-sort (Katrina 93 -> 94, Sandy 83 -> 87 before
2026-09-23). A rebake that moves a preset's score must update PRESETS too.
"""
import json
import os
import re

_ROOT = os.path.dirname(os.path.dirname(__file__))
_PRESET_RE = re.compile(
    r"\{id:'(?P<id>[A-Z0-9]+)',name:'(?P<name>[^']+)',year:\d+,cat:\d+,dps:(?P<dps>\d+)")


def _presets():
    html = open(os.path.join(_ROOT, "frontend", "index.html"), encoding="utf-8").read()
    block = html[html.index("const PRESETS=["):]
    block = block[:block.index("];")]
    rows = [(m["id"], m["name"], int(m["dps"])) for m in _PRESET_RE.finditer(block)]
    assert rows, "no PRESETS entries parsed from frontend/index.html"
    return rows


def _compiled():
    with open(os.path.join(_ROOT, "frontend", "compiled_bundle.json"), encoding="utf-8") as fh:
        return json.load(fh)["storms"]


def test_preset_defaults_match_compiled_scores():
    storms = _compiled()
    stale = []
    for sid, name, dps in _presets():
        assert sid in storms, f"preset {name} ({sid}) missing from compiled_bundle.json"
        want = round(storms[sid]["dps"])
        if dps != want:
            stale.append(f"{name} ({sid}): PRESETS has {dps}, compiled is {want}")
    assert not stale, ("update PRESETS in frontend/index.html to the compiled "
                       "scores: " + "; ".join(stale))


def test_presets_are_in_descending_order():
    # rerenderPresetChips() sorts by dps; an unsorted first paint re-orders visibly.
    scores = [dps for _, _, dps in _presets()]
    assert scores == sorted(scores, reverse=True), scores
