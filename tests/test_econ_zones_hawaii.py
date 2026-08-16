"""
Central Pacific / Hawaii economic-exposure coverage for the ERS.

Before this, econ_zones.json covered only the US Atlantic/Gulf + PR/USVI + West
Pacific, with a hard longitude gap that swallowed Hawaii — any CP storm scored the
0.05 "Open Ocean / Uncharted" floor (Lala 2026 showed peak ERS 3 as a 70kt
hurricane by the Big Island). These lock in the coverage and the file lockstep.
"""
import ast
import json
import os

from core.ike import get_economic_exposure, _ECON_ZONES

_ROOT = os.path.dirname(os.path.dirname(__file__))


def _econ_names():
    with open(os.path.join(_ROOT, "frontend", "econ_zones.json")) as fh:
        return {z["name"] for z in json.load(fh)}


def _build_nri_zone_names():
    src = open(os.path.join(_ROOT, "build_nri_zones.py")).read()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Assign) and any(
                getattr(t, "id", None) == "ZONES" for t in node.targets):
            return [row[0] for row in ast.literal_eval(node.value)]
    raise AssertionError("ZONES not found in build_nri_zones.py")


def test_hawaii_zones_present():
    names = _econ_names()
    for z in ("Honolulu / Oahu (HI)", "Maui County (HI)",
              "Hawaii / Big Island (HI)", "Kauai / Niihau (HI)"):
        assert z in names, f"missing econ zone: {z}"


def test_build_nri_zones_are_a_subset_of_econ_zones():
    # Every FEMA-query zone must exist in the canonical econ_zones.json (name+bbox
    # lockstep) or its NRI override would key on a name no zone can match.
    econ = _econ_names()
    missing = [n for n in _build_nri_zone_names() if n not in econ]
    assert not missing, f"build_nri_zones ZONES not in econ_zones.json: {missing}"


def test_big_island_pass_matches_not_open_ocean():
    # Lala's near-Big-Island track points must resolve to a Hawaii zone, not the
    # open-ocean floor. 19.1N/-156.5 is ~23 nm off the Big Island bbox edge.
    e = get_economic_exposure(19.1, -156.5, use_nri=False, r34_nm=60)
    assert e["name"] == "Hawaii / Big Island (HI)"
    assert e["exposure"] > 0.05  # tapered zone exposure, above the floor


def test_oahu_direct_hit_is_high_exposure():
    e = get_economic_exposure(21.3, -157.9, use_nri=False)
    assert e["name"] == "Honolulu / Oahu (HI)"
    assert e["exposure"] >= 0.5


def test_distant_atlantic_storm_still_open_ocean():
    # A mid-Atlantic point must be unaffected by the new Pacific zones.
    e = get_economic_exposure(30.0, -45.0, use_nri=False, r34_nm=60)
    assert e["name"] == "Open Ocean / Uncharted"


def test_hawaii_zones_isolated_in_longitude():
    # The Hawaii boxes live at lon <= -154, far from every other basin's zones,
    # so they can't contaminate an existing storm's match.
    for name, exposure, vuln, depth_nm, lat_min, lat_max, lon_min, lon_max in _ECON_ZONES:
        if "(HI)" in name:
            assert lon_max <= -154.0 and lon_min >= -161.0
            assert 18.0 <= lat_min and lat_max <= 23.0
