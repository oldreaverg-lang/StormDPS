"""Recorded damage reaches storms the compiled bundle does not carry.

2026-10-10: the Isaias-vs-Opal comparison (same coast, same week of October)
showed "al092026" as Isaias's name and a dash for Opal's recorded damage.
"""
import pytest

import api.routes as routes
from core import recorded_damage as rd

OPAL_SID, OPAL_ATCF = "1995271N19273", "AL171995"


def test_display_rule_matches_the_bake():
    assert rd.fmt_usd_billions(4.7) == "$4.7B"
    assert rd.fmt_usd_billions(25.5) == "$26B"
    assert rd.fmt_usd_billions(0.35) == "$350M"
    assert rd.fmt_usd_billions(0) is None


def test_rows_parse_and_bad_rows_are_skipped():
    rows = rd.parse_rows([
        {"storm_id": "1995271n19273", "damage_billions_usd": "4.7", "source": "NCEI", "note": "US nominal"},
        {"storm_id": "", "damage_billions_usd": "9"},
        {"storm_id": "X1", "damage_billions_usd": "n/a"},
        {"storm_id": "X2", "damage_billions_usd": "0"},
    ])
    assert list(rows) == [OPAL_SID]
    assert rows[OPAL_SID] == {"damage_billions": 4.7, "source": "NCEI", "note": "US nominal"}


def test_estimates_are_marked_like_the_bake():
    assert rd.impact_from({"damage_billions": 1.6, "source": "est."})["damage_display"] == "~$1.6B (est.)"


def test_opal_is_in_the_dataset_under_any_id_form():
    hit = rd.lookup([OPAL_ATCF, OPAL_SID])
    assert hit and hit["damage_display"] == "$4.7B" and hit["damage_source"] == "NCEI"
    assert rd.lookup([None, "", "AL999999"]) is None


def test_dps_overlay_attaches_opal_damage_for_both_id_forms():
    for sid in (OPAL_SID, OPAL_ATCF, OPAL_ATCF.lower()):
        out = routes._overlay_bundle_identity({"name": sid, "dps": 86.0}, sid)
        assert out["name"] == "Opal", sid
        assert out["actual_impact"]["damage_display"] == "$4.7B", sid


def test_bundle_storms_keep_the_bundle_figure():
    # Michael 2018 is a bundle storm: its actual_impact (with FEMA fields)
    # comes from the bundle, never from the CSV fallback.
    import json
    import pathlib
    bundle = json.loads((pathlib.Path(routes.__file__).resolve().parent.parent
                         / "frontend" / "compiled_bundle.json").read_text(encoding="utf-8"))
    out = routes._overlay_bundle_identity({"name": "AL142018", "dps": 80.0}, "AL142018")
    assert out["actual_impact"]["damage_display"] == "$26B"
    assert out["actual_impact"] == bundle["storms"]["AL142018"]["actual_impact"]


def test_a_live_storm_gets_its_name_from_the_active_list(monkeypatch):
    monkeypatch.setattr(routes, "_active_storms_cache", [
        {"id": "al092026", "name": "Isaias"}, {"id": "ep202026", "name": "Simon"},
    ])
    for sid in ("al092026", "AL092026"):
        out = routes._overlay_bundle_identity({"name": "al092026", "dps": 70.8}, sid)
        assert out["name"] == "Isaias"
        assert "actual_impact" not in out            # no damage on record yet
    # An unknown storm with nothing to go on keeps what it had.
    assert routes._overlay_bundle_identity({"name": "al992026"}, "al992026")["name"] == "al992026"


def test_overlay_survives_a_missing_active_list(monkeypatch):
    monkeypatch.setattr(routes, "_active_storms_cache", None)
    assert routes._overlay_bundle_identity({"name": "al092026"}, "al092026")["name"] == "al092026"
