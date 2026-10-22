"""Far offshore with no coast ahead: the engine's land dampening, not the
nearest coast's profile at full strength (2026-10-21).

Subtropical Storm Josephine AL102026, 55 kt and 460-550 km off the Carolinas
and New Jersey on a forecast track away from the US, read DPS 58 "Severe ...
comparable to a large Cat 2-3 hurricane": every fix was scored with a US
coast's surge and economic profile at full strength. Its Impact Area read 48
"Elevated" because the shelf box "US Mid-Atlantic" runs out to 69W, and its
energy was clamped to 20 TJ by a gate written for West Pacific data glitches.
"""
import json
import pathlib
import re
import shutil
import subprocess

import pytest

from core import coast_distance
from core import cumulative_dpi as cd
from core.dps_engine import compute_storm_dps
from core.landfall_forecast import compute_forecast_landfall

ROOT = pathlib.Path(__file__).resolve().parent.parent
KT = 0.514444

# NHC best track (6-hourly; EX = the Invest AL93 low, SS from 21/18Z).
# time, lat, lon, vmax m/s, pressure, rmw nm, r34 NE/SE/SW/NW nm, fwd kt, IKE TJ
JOSEPHINE_FIXES = [
    ("2026-10-18T18:00", 31.00, -71.40, 20.6, 1010.0, 150.0, (300, 0, 120, 200), 8.7, 92.43),
    ("2026-10-19T00:00", 31.80, -71.00, 20.6, 1010.0, 150.0, (300, 0, 150, 200), 6.0, 101.18),
    ("2026-10-19T06:00", 32.40, -70.90, 23.1, 1008.0, 200.0, (420, 0, 220, 330), 5.0, 176.61),
    ("2026-10-19T12:00", 32.90, -70.80, 23.1, 1007.0, 200.0, (420, 0, 220, 330), 9.1, 176.61),
    ("2026-10-19T18:00", 33.80, -70.60, 23.1, 1002.0, 120.0, (400, 0, 200, 300), 15.1, 176.61),
    ("2026-10-20T00:00", 35.30, -70.90, 23.1, 1002.0, 120.0, (350, 0, 170, 250), 3.3, 140.98),
    ("2026-10-20T06:00", 35.30, -70.50, 23.1, 1002.0, 100.0, (300, 0, 130, 240), 3.9, 107.28),
    ("2026-10-20T12:00", 35.60, -70.20, 23.1, 1000.0, 100.0, (300, 0, 130, 240), 9.5, 107.28),
    ("2026-10-20T18:00", 36.30, -69.40, 23.1, 997.0, 100.0, (300, 70, 100, 240), 3.0, 69.66),
    ("2026-10-21T00:00", 36.60, -69.40, 23.1, 997.0, 100.0, (300, 70, 100, 240), 2.0, 69.66),
    ("2026-10-21T06:00", 36.80, -69.40, 23.1, 997.0, 100.0, (300, 70, 100, 240), 1.3, 69.66),
    ("2026-10-21T12:00", 36.90, -69.50, 25.7, 992.0, 100.0, (300, 100, 100, 270), 2.1, 80.86),
    ("2026-10-21T18:00", 37.10, -69.60, 28.3, 990.0, 70.0, (360, 100, 130, 260), 2.1, 97.48),
]

# NHC advisory 1 forecast (21Z 21 Oct): northeast, south of Nova Scotia and Newfoundland.
JOSEPHINE_FORECAST = [
    ("2026-10-21T21:00:00", 37.4, -69.5, 55), ("2026-10-22T06:00:00", 38.1, -69.0, 60),
    ("2026-10-22T18:00:00", 39.6, -66.2, 65), ("2026-10-23T06:00:00", 41.4, -61.6, 70),
    ("2026-10-23T18:00:00", 43.3, -56.9, 60), ("2026-10-24T06:00:00", 44.1, -53.5, 50),
    ("2026-10-24T18:00:00", 44.4, -50.4, 45), ("2026-10-25T18:00:00", 43.5, -37.5, 40),
]


def _snap(t, lat, lon, vmax, pres, rmw, quads, fwd, ike):
    ne, se, sw, nw = quads
    return {"storm_id": "AL102026", "timestamp": t + ":00Z", "lat": lat, "lon": lon,
            "max_wind_ms": vmax, "min_pressure_hpa": pres, "rmw_nm": rmw,
            "r34_nm": float(max(quads)), "r34_quadrants": {"NE": ne, "SE": se, "SW": sw, "NW": nw},
            "r64_nm": 0.0, "forward_speed_knots": fwd, "ike_total_tj": ike}


JOSEPHINE = [_snap(*f) for f in JOSEPHINE_FIXES]


def _track(points, kt=100, start="2026-09-01T00:00"):
    """6-hourly synthetic track."""
    from datetime import datetime, timedelta
    t0 = datetime.fromisoformat(start)
    return [{"timestamp": (t0 + timedelta(hours=6 * i)).isoformat() + "Z", "lat": la, "lon": lo,
             "max_wind_ms": kt * KT, "min_pressure_hpa": 960.0, "rmw_nm": 25.0, "r34_nm": 150.0,
             "r64_nm": 40.0, "forward_speed_knots": 12.0, "ike_total_tj": 60.0}
            for i, (la, lo) in enumerate(points)]


def _forecast_hint():
    return cd.forecast_track_hint([{"timestamp": t, "lat": la, "lon": lo} for t, la, lo, _ in JOSEPHINE_FORECAST])


# ── the land grid ────────────────────────────────────────────────────────────

def test_the_land_grid_is_shipped_and_reads_sensibly():
    assert coast_distance.available(), "data/coast_distance.bin missing"
    assert coast_distance.coast_km(41.28, -70.10) == 0          # Nantucket
    assert coast_distance.coast_km(14.6, -87.2) == 0            # inland Honduras
    assert coast_distance.coast_km(32.3, -64.78) == 0           # Bermuda (stamped from the waypoints)
    assert coast_distance.coast_km(24.4, -78.0) == 0            # Andros, Bahamas
    assert 420 < coast_distance.coast_km(37.1, -69.6) < 500     # Josephine at 18Z (~460 km off Nantucket)
    assert 80 < coast_distance.coast_km(35.0, -74.5) < 130      # ~100 km off Cape Hatteras
    assert coast_distance.coast_km(15.0, 130.0) is None         # West Pacific: outside the grid
    assert coast_distance.coast_km(-15.0, -150.0) is None


# ── item 1: offshore damping ─────────────────────────────────────────────────

def test_josephine_is_scored_as_far_offshore():
    hint = _forecast_hint()
    assert hint["region_key"] is None and hint["coast_utc"] is None   # the forecast reaches no land
    assert all(cd.offshore_fixes(JOSEPHINE, hint))
    out = compute_storm_dps("AL102026", JOSEPHINE, "Josephine", 2026, landfall_hint=hint)
    assert 20 <= out["dps"] < 40 and out["dps_label"] == "Moderate", out["dps"]


def test_an_unreadable_forecast_leaves_a_live_storm_undamped():
    out = compute_storm_dps("AL102026", JOSEPHINE, "Josephine", 2026, landfall_hint={"unknown": True})
    assert out["dps"] > 50                                       # the old nearest-coast reading
    assert cd.forecast_track_hint([]) == {"unknown": True}
    assert cd.forecast_track_hint([{"timestamp": "2026-10-21T21:00:00", "lat": 37.4, "lon": -69.5}]) == {"unknown": True}


def test_a_storm_with_a_landfall_ahead_is_not_damped():
    # 300-700 km off the Carolinas, heading in to Cape Hatteras within 120 h
    track = _track([(30.0, -68.0), (31.0, -69.5), (32.0, -71.0), (33.0, -72.5), (34.0, -74.0), (35.0, -75.3)])
    assert not any(cd.offshore_fixes(track))


def test_a_storm_bound_for_nova_scotia_has_a_coast_ahead():
    track = _track([(36.0, -64.0), (38.0, -63.5), (40.0, -63.0), (42.0, -62.5), (44.0, -62.0), (45.3, -61.5)])
    assert not any(cd.offshore_fixes(track))


def test_land_and_the_near_coast_are_never_offshore():
    # Leaving the Carolinas for the open Atlantic: inland, at the coast, ~100 km
    # out (all kept), then far out with nothing ahead (damped).
    track = _track([(35.5, -78.5), (35.6, -76.0), (35.4, -74.2), (36.0, -70.0), (37.0, -66.0), (38.0, -62.0)])
    assert cd.offshore_fixes(track) == [False, False, False, True, True, True]
    # Eta 2020 inland over Honduras, Joaquin 2015 among the southern Bahamas
    assert not any(cd.offshore_fixes(_track([(14.0, -86.0), (14.6, -87.2), (15.0, -88.0)])))
    assert not any(cd.offshore_fixes(_track([(23.0, -73.5), (23.2, -74.1), (23.5, -74.6)])))


def test_west_pacific_and_southern_hemisphere_are_untouched():
    assert not any(cd.offshore_fixes(_track([(15.0, 135.0), (16.0, 134.0), (17.0, 133.0)])))
    assert not any(cd.offshore_fixes(_track([(-15.0, -150.0), (-16.0, -151.0), (-17.0, -152.0)])))


def test_a_damped_fix_keeps_its_ike_and_the_60_percent_floor():
    s = JOSEPHINE[-1]
    full, _ = cd.compute_snapshot_dpi(s, None, False)
    damped, res = cd.compute_snapshot_dpi(s, None, True)
    assert res.land_proximity_factor == pytest.approx(0.0, abs=1e-3)
    assert damped == pytest.approx(full * 0.60, rel=0.02)


# ── item 3b: the IKE plausibility gate ───────────────────────────────────────

def test_nhc_gale_fields_are_not_clamped():
    s = dict(JOSEPHINE[-1])                                      # 55 kt, r34 360 nm
    assert cd._plausible_ike_tj(s) == pytest.approx(97.48)
    out = compute_storm_dps("AL102026", JOSEPHINE, "Josephine", 2026, landfall_hint=_forecast_hint())
    assert out["peak_ike_tj"] > 90                               # was 20


def test_the_west_pacific_glitch_gate_still_clamps():
    glitch = {"lat": 15.0, "lon": 130.0, "max_wind_ms": 18.0, "r34_nm": 320.0, "ike_total_tj": 196.31}
    assert cd._plausible_ike_tj(glitch) == 20.0
    # no 34-kt wind at all but a gale radius: a glitch everywhere
    assert cd._plausible_ike_tj({"lat": 30.0, "lon": -70.0, "max_wind_ms": 15.0,
                                 "r34_nm": 200.0, "ike_total_tj": 80.0}) == 20.0


# ── item 3a: closest approach / landfall panel ───────────────────────────────

def _fc_track(points):
    return [{"lat": la, "lon": lo, "hour": h, "max_wind_kt": kt} for h, la, lo, kt in points]


def test_josephines_closest_approach_is_atlantic_canada():
    t0 = JOSEPHINE_FORECAST[0][0]
    from datetime import datetime
    base = datetime.fromisoformat(t0)
    pts = [((datetime.fromisoformat(t) - base).total_seconds() / 3600, la, lo, kt)
           for t, la, lo, kt in JOSEPHINE_FORECAST]
    lf = compute_forecast_landfall(_fc_track(pts))
    assert lf["expected"] is False
    assert lf["nearest_name"].endswith(", NL"), lf["nearest_name"]
    assert 250 < lf["min_distance_km"] < 330


def test_a_nova_scotia_landfall_is_seen_and_has_no_profile():
    lf = compute_forecast_landfall(_fc_track([(0, 40.0, -61.5, 100), (12, 43.0, -61.0, 90),
                                              (24, 45.5, -60.5, 80), (36, 48.0, -60.0, 60)]))
    assert lf["expected"] is True and lf["nearest_name"].endswith(", NS")
    assert lf["region_key"] is None


def test_a_pass_south_of_nantucket_names_massachusetts():
    lf = compute_forecast_landfall(_fc_track([(0, 39.0, -72.0, 60), (12, 40.3, -70.0, 60), (24, 41.0, -67.0, 55)]))
    assert lf["expected"] is False
    assert lf["nearest_name"].endswith(", MA"), lf["nearest_name"]
    assert lf["min_distance_km"] < 150


def test_a_us_town_landfall_still_resolves_a_profile_region():
    # Keaton Beach (Big Bend): the first coast point within 50 km is a town
    lf = compute_forecast_landfall(_fc_track([(0, 27.5, -85.0, 100), (12, 28.8, -84.3, 105), (24, 30.3, -83.5, 80)]))
    assert lf["expected"] is True and lf["region_key"]


# ── item 2: the Impact Area near-land test ───────────────────────────────────

def _js_function(text, name):
    start = text.index("function " + name + "(")
    i = text.index("{", start)
    depth = 0
    for j in range(i, len(text)):
        if text[j] == "{":
            depth += 1
        elif text[j] == "}":
            depth -= 1
            if depth == 0:
                return text[start:j + 1]
    raise AssertionError(name)


def test_the_impact_area_fades_to_open_ocean_offshore():
    node = shutil.which("node")
    if not node:
        pytest.skip("node not available")
    html = (ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
    consts = re.search(r"const COAST_NEAR_KM = .*?;", html).group(0)
    shelf = json.loads((ROOT / "frontend" / "shelf_regions.json").read_text(encoding="utf-8"))
    script = ("let SHELF_REGIONS = " + json.dumps(shelf) + ";" + consts +
              "".join(_js_function(html, n) for n in
                      ("getShelfFactor", "coastReach", "calculateSurgeParametric", "calculateIAS")) +
              "const p = {lat: 37.1, lon: -69.6, max_wind_ms: 28.3, min_pressure_hpa: 990, r34_nm: 360,"
              " forward_speed_knots: 2.1};"
              "const out = {old: calculateIAS(p).score, far: calculateIAS(Object.assign({}, p, {coast_km: 462})).score,"
              " near: calculateIAS(Object.assign({}, p, {coast_km: 120})).score,"
              " mid: calculateIAS(Object.assign({}, p, {coast_km: 225})).score,"
              " reach: [coastReach({coast_km: 0}), coastReach({coast_km: 150}), coastReach({coast_km: 225}),"
              "         coastReach({coast_km: 300}), coastReach({}), coastReach({coast_km: null})]};"
              "console.log(JSON.stringify(out));")
    out = json.loads(subprocess.run([node, "-e", script], capture_output=True, text=True, check=True).stdout)
    assert out["reach"] == [1, 1, 0.5, 0, 1, 1]
    assert out["old"] >= 40                      # the box alone: "Elevated" over open water
    assert out["near"] == out["old"]             # within 150 km nothing changes
    assert out["far"] < 20                       # ~460 km out: open ocean
    assert out["far"] < out["mid"] < out["old"]


def test_baked_snapshots_carry_coast_km():
    detail = json.loads((ROOT / "frontend" / "bundle_storm" / "AL122005.json").read_text(encoding="utf-8"))
    rows = detail["snapshots"]
    assert all("coast_km" in r for r in rows)
    assert min(r["coast_km"] for r in rows) == 0             # Katrina came ashore
