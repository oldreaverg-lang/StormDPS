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

from core.dpi import categorize_dpi
from core.ike import calculate_dps
from core.storm_identity import harmonize_catalog, storm_identity

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
