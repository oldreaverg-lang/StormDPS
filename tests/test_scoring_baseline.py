"""Golden-master regression lock for the baked DPS scores.

Freezes every storm's displayed DPS + category from frontend/compiled_bundle.json
into tests/data/scoring_baseline.json. The scoring single-source-of-truth refactor
(SCORING_SSOT_REFACTOR_PLAN) must keep these identical: after a rebake, this test
fails if any storm's score or category moved. Re-freeze intentionally with
`python tests/gen_scoring_baseline.py` only when an approved scoring change lands.
"""
import json
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
BUNDLE = ROOT / "frontend" / "compiled_bundle.json"
BASELINE = pathlib.Path(__file__).resolve().parent / "data" / "scoring_baseline.json"
TOL = 1e-6


@pytest.fixture(scope="module")
def current():
    return json.loads(BUNDLE.read_text(encoding="utf-8"))["storms"]


@pytest.fixture(scope="module")
def baseline():
    return json.loads(BASELINE.read_text(encoding="utf-8"))


def test_baseline_nonempty(baseline):
    assert len(baseline) > 100


def test_no_storms_dropped(current, baseline):
    missing = sorted(set(baseline) - set(current))
    assert not missing, "%d baselined storms missing from bundle: %s" % (len(missing), missing[:10])


def test_scores_match_baseline(current, baseline):
    drift = []
    for sid, base in baseline.items():
        cur = current.get(sid, {})
        if "dps" not in cur:
            drift.append((sid, "missing dps")); continue
        if abs(float(cur["dps"]) - float(base["dps"])) > TOL:
            drift.append((sid, "dps", base["dps"], cur["dps"]))
        elif cur.get("category") != base["category"]:
            drift.append((sid, "category", base["category"], cur.get("category")))
    assert not drift, "%d storm(s) drifted from baseline, e.g. %s" % (len(drift), drift[:10])
