"""Offline tests for the cross-surface score consistency fix
(docs/audits/CROSS_SURFACE_SCORE_AUDIT_2026-07-10.md).

Covers core/storm_identity.harmonize_catalog and the canonical-label
requirement on core/ike.calculate_dps. No FastAPI import.
"""
import sys
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
BUNDLE = ROOT / "frontend" / "compiled_bundle.json"

from core.dpi import categorize_dpi
from core.ike import calculate_dps
from core.storm_identity import (
    cross_site_score_drift,
    harmonize_catalog,
    storm_identity,
)

CANON = {"Historic", "Devastating", "Extreme", "Severe", "Moderate", "Low", "Minimal"}


def test_calculate_dps_labels_are_canonical():
    # sweep intensities with and without radii — the pre-canon
    # "Catastrophic"/"Minor" labels must never come back
    for wind in range(34, 175, 5):
        for radii in ({}, {"r34_nm": 250, "r64_nm": 40, "pressure_hpa": 920,
                           "lat": 27.0, "lon": -90.0}):
            out = calculate_dps(wind_kt=wind, **radii)
            assert out["label"] in CANON, (wind, radii, out)
            assert out["label"] == categorize_dpi(out["score"])


def test_categorize_dpi_has_seven_canonical_bands():
    assert {categorize_dpi(s) for s in (95, 85, 70, 50, 30, 15, 5)} == CANON


def _bundle():
    return {
        "AL142018": {"dps": 89.2, "dps_label": "Devastating", "name": "Michael"},
    }


def test_harmonize_overlays_engine_score_by_exact_key():
    rows = [{"id": "AL142018", "name": "Michael", "year": 2018,
             "peak_dps": 30, "dps_label": "Moderate"}]
    out = harmonize_catalog(rows, _bundle())
    assert out[0]["peak_dps"] == 89
    assert out[0]["dps_label"] == "Devastating"
    assert out[0]["score_source"] == "engine"
    # input row untouched (no in-place mutation of the cached catalog)
    assert rows[0]["peak_dps"] == 30


def test_harmonize_overlays_via_alias_form():
    # Michael's IBTrACS SID must reach the ATCF-keyed bundle entry
    sid = storm_identity("AL142018")["sid"]
    rows = [{"id": sid, "name": "Michael", "year": 2018,
             "peak_dps": 41, "dps_label": "Severe"}]
    out = harmonize_catalog(rows, _bundle())
    assert out[0]["peak_dps"] == 89 and out[0]["dps_label"] == "Devastating"


def test_harmonize_rebands_off_canon_estimates():
    rows = [
        {"id": "WP991999", "name": "Nobody", "year": 1999, "peak_dps": 15, "dps_label": "Minor"},
        {"id": "WP981999", "name": "Bigone", "year": 1999, "peak_dps": 85, "dps_label": "Catastrophic"},
    ]
    out = harmonize_catalog(rows, {})
    labels = {r["id"]: r["dps_label"] for r in out}
    assert labels["WP991999"] == "Low"
    assert labels["WP981999"] == "Devastating"


def test_harmonize_collapses_sid_atcf_twins():
    sid = storm_identity("AL142018")["sid"]
    rows = [
        {"id": sid, "name": "Michael", "year": 2018, "peak_dps": 36, "dps_label": "Moderate"},
        {"id": "AL142018", "name": "Michael", "year": 2018, "source": "nhc-current"},
    ]
    out = harmonize_catalog(rows, _bundle())
    assert len(out) == 1
    assert out[0]["peak_dps"] == 89  # the engine-scored row won


def test_harmonize_sorts_year_then_score_and_fails_open():
    rows = [
        {"id": "A", "year": 2020, "peak_dps": 10, "dps_label": "Low"},
        {"id": "B", "year": 2021, "peak_dps": 5, "dps_label": "Minimal"},
        {"id": "C", "year": 2021, "peak_dps": 50, "dps_label": "Severe"},
    ]
    out = harmonize_catalog(rows, {})
    assert [r["id"] for r in out] == ["C", "B", "A"]
    assert harmonize_catalog([], {}) == []
    assert harmonize_catalog(None, {}) == []


def test_cross_site_drift_detects_stale_snapshot():
    bundle = {
        "AL092008": {"name": "Ike", "year": 2008, "dps": 88.8501},
        "AL122005": {"name": "Katrina", "year": 2005, "dps": 94.03},
    }
    rows = [
        # matches canon within round(x,1) tolerance — not drift
        {"storm_id": "katrina_2005", "name": "Hurricane Katrina", "year": 2005, "dps_score": 94.0},
        # the actual 2026-07-10 drift case
        {"storm_id": "ike_2008", "name": "Hurricane Ike", "year": 2008, "dps_score": 86.5},
        # no canonical match → skipped, not flagged
        {"storm_id": "x_1999", "name": "Hurricane Nobody", "year": 1999, "dps_score": 50},
        # MISSING score (None) → skipped, not flagged (contrast with 0.0 below)
        {"storm_id": "z_2005", "name": "Hurricane Katrina", "year": 2005},
    ]
    out = cross_site_score_drift(rows, bundle)
    assert out["compared"] == 2
    assert len(out["drifted"]) == 1
    d = out["drifted"][0]
    assert d["id"] == "ike_2008" and d["theirs"] == 86.5 and d["canonical"] == 88.9


def test_cross_site_drift_zero_score_is_real_drift():
    # 0.0 is a legitimate score, not "unscored": a storm showing 0 when canon
    # says 88.9 is exactly the drift class the probe exists to catch.
    bundle = {"AL092008": {"name": "Ike", "year": 2008, "dps": 88.8501}}
    rows = [{"storm_id": "ike_2008", "name": "Hurricane Ike", "year": 2008, "dps_score": 0.0}]
    out = cross_site_score_drift(rows, bundle)
    assert out["compared"] == 1
    assert len(out["drifted"]) == 1 and out["drifted"][0]["theirs"] == 0.0


def test_cross_site_drift_against_real_repos():
    # The synced SurgeDPS catalog (2026-07-10) must agree with the real
    # bundle — guards this repo's side of the contract offline.
    import json
    surge_catalog = pathlib.Path(
        r"C:\Users\Ryan\APPS\SurgeDPS-recovered\src\storm_catalog\catalog.py")
    if not surge_catalog.exists():
        return  # other machines: the live selfcheck probe covers it
    import re as _re
    rows = []
    src = surge_catalog.read_text(encoding="utf-8")
    for m in _re.finditer(
            r'storm_id="([a-z0-9_-]+)"(.{0,600}?)name="([^"]+)"(.{0,600}?)'
            r'year=(\d{4})(.{0,600}?)dps_score=([\d.]+)', src, _re.DOTALL):
        rows.append({"storm_id": m.group(1), "name": m.group(3),
                     "year": int(m.group(5)), "dps_score": float(m.group(7))})
    bundle = json.loads(BUNDLE.read_text(encoding="utf-8"))["storms"]
    out = cross_site_score_drift(rows, bundle)
    assert out["compared"] >= 10, "regex should find the curated storms"
    assert not out["drifted"], f"SurgeDPS curated catalog drifted: {out['drifted'][:3]}"


def test_live_shapes_stay_canonical_against_real_bundle():
    # smoke: harmonizing a Sinlaku-shaped pair against the real bundle+aliases
    import json
    bundle = json.loads((ROOT / "frontend" / "compiled_bundle.json").read_text(encoding="utf-8"))["storms"]
    rows = [
        {"id": "2026099N09152", "name": "Sinlaku", "year": 2026, "peak_dps": 36, "dps_label": "Moderate"},
        {"id": "WP042026", "name": "Sinlaku", "year": 2026, "source": "nhc-current"},
    ]
    out = harmonize_catalog(rows, bundle)
    assert len(out) == 1  # twins collapsed even when the storm isn't baked
    assert out[0]["dps_label"] in CANON


def test_harmonize_collapses_twins_without_alias_via_name_year_basin():
    # A current-season storm whose SID+ATCF pair the IBTrACS-built alias table
    # doesn't know yet must still collapse — by (name, year, basin). Use an
    # unmistakably fake pair so neither id resolves through the alias table.
    rows = [
        {"id": "2026400N09152", "name": "Teststorm", "year": 2026, "basin": "WP",
         "peak_dps": 20, "dps_label": "Moderate", "source": "nhc-current"},
        {"id": "WP932026", "name": "Teststorm", "year": 2026, "basin": "WP"},
    ]
    out = harmonize_catalog(rows, {})
    assert len(out) == 1, "SID+ATCF twins should collapse via (name,year,basin)"
    # the SID-form, scored row wins the merge
    assert out[0]["id"] == "2026400N09152"
    assert out[0]["peak_dps"] == 20


def test_harmonize_name_year_basin_does_not_over_collapse_distinct_storms():
    # Same name reused across DIFFERENT basins (a real cross-basin collision)
    # or different years must NOT collapse.
    rows = [
        {"id": "WP932026", "name": "Same", "year": 2026, "basin": "WP", "peak_dps": 30},
        {"id": "EP932026", "name": "Same", "year": 2026, "basin": "EP", "peak_dps": 40},
        {"id": "WP932025", "name": "Same", "year": 2025, "basin": "WP", "peak_dps": 50},
    ]
    out = harmonize_catalog(rows, {})
    assert len(out) == 3


def test_harmonize_prefers_sid_row_on_equal_score():
    # Two equally-(un)scored twins → keep the durable IBTrACS SID identity.
    rows = [
        {"id": "WP932026", "name": "Tie", "year": 2026, "basin": "WP", "peak_dps": 25},
        {"id": "2026400N09152", "name": "Tie", "year": 2026, "basin": "WP", "peak_dps": 25},
    ]
    out = harmonize_catalog(rows, {})
    assert len(out) == 1
    assert out[0]["id"] == "2026400N09152"
