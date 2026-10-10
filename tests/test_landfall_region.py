"""A fix far from any coast is scored against the coast the storm reaches.

Hurricane Isaias (AL092026), 2026-10-09: at 27.0N 87.6W the nearest coastline
waypoint was Louisiana's, 317 km away, so the fix took Louisiana's surge and
economic profile and scored 68 on a track into Fort Walton Beach. Three hours
later the nearest waypoint was the Panhandle's and the same storm scored 46.
The 68 was the storm's peak and set its DPS (71 "Extreme").
"""
from datetime import datetime, timedelta

import pytest

from core import cumulative_dpi as cd
from core.dps_engine import compute_storm_dps

T0 = datetime(2026, 10, 9, 0)


def _snap(hours, lat, lon, kt=105, pres=959, r34=180, r64=30, rmw=20, fwd=15, ike=41.5):
    return {
        "timestamp": (T0 + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S"),
        "lat": lat, "lon": lon, "max_wind_ms": kt * 0.514444, "min_pressure_hpa": pres,
        "r34_nm": r34, "r64_nm": r64, "rmw_nm": rmw, "forward_speed_knots": fwd, "ike_total_tj": ike,
    }


# Isaias's last day at sea, as the best track had it (constant storm for clarity).
ISAIAS = [
    _snap(0, 24.8, -89.0), _snap(6, 25.8, -88.4), _snap(12, 27.0, -87.6),
    _snap(15, 27.75, -87.35), _snap(18, 28.5, -87.1), _snap(21, 29.35, -86.85),
    _snap(24, 30.2, -86.6), _snap(30, 31.4, -86.5, kt=45),
]


def _pts(snaps):
    return [(cd._parse_timestamp(s["timestamp"]), s["lat"], s["lon"]) for s in snaps]


# ── finding the coast the track reaches ─────────────────────────────────────

def test_the_track_reaches_the_florida_panhandle_once():
    hits = cd.coast_contacts(_pts(ISAIAS))
    assert [k for _t, k in hits] == ["gulf_fl_panhandle"]
    assert T0 + timedelta(hours=22) <= hits[0][0] <= T0 + timedelta(hours=27)


def test_a_track_that_stays_offshore_reaches_no_coast():
    offshore = [_snap(h, 27.0, -92.0 + h * 0.4) for h in range(0, 24, 6)]      # eastward, ~250 km out
    assert cd.coast_contacts(_pts(offshore)) == []
    assert cd.landfall_regions(offshore) == [None] * len(offshore)


def test_the_big_bend_landfall_is_seen():
    # Helene 2024: no coastline waypoint within 110 km of the landfall point;
    # the town list is what makes the contact visible.
    helene = [_snap(0, 26.6, -85.0), _snap(6, 28.7, -84.3), _snap(9, 30.0, -83.7), _snap(12, 31.3, -83.3)]
    assert [k for _t, k in cd.coast_contacts(_pts(helene))][:1] == ["gulf_fl_panhandle"]


# ── which fixes take the landfall region ────────────────────────────────────

def test_fixes_far_from_any_coast_take_the_landfall_region():
    regions = cd.landfall_regions(ISAIAS)
    # 12Z, 15Z, 18Z of the 9th: 300, 260 and 200 km out.
    assert regions[2:5] == ["gulf_fl_panhandle"] * 3
    # 21Z is 113 km from the coast: it is AT that coast, nearest-coast rule kept.
    assert regions[5] is None
    # At the coast and inland afterwards: no coast ahead.
    assert regions[6] is None and regions[7] is None


def test_the_look_ahead_is_the_length_of_the_forecast_cone():
    early = [_snap(-130, 20.0, -75.0), _snap(-110, 21.0, -78.0)] + ISAIAS
    regions = cd.landfall_regions(early)
    assert regions[0] is None                      # landfall is 154 h away: outside the cone
    assert regions[4] == "gulf_fl_panhandle"       # 12 h away


def test_a_live_storm_uses_its_forecast_landfall():
    at_sea = ISAIAS[:4]                            # newest fix 15Z, still 260 km out
    assert cd.landfall_regions(at_sea) == [None] * 4            # nothing observed yet
    hint = {"region_key": "gulf_fl_panhandle", "time_utc": "2026-10-10T01:00:00"}
    assert cd.landfall_regions(at_sea, hint) == ["gulf_fl_panhandle"] * 4
    # A forecast landfall beyond the cone's length, or a useless hint, changes nothing.
    assert cd.landfall_regions(at_sea, {"region_key": "gulf_la", "time_utc": "2026-10-20T00:00:00"}) == [None] * 4
    assert cd.landfall_regions(at_sea, {"region_key": "open_ocean", "time_utc": "2026-10-10T01:00:00"}) == [None] * 4
    assert cd.landfall_regions(at_sea, {"region_key": "gulf_la", "time_utc": "soon"}) == [None] * 4
    # What the track has already done outranks the forecast.
    assert cd.landfall_regions(ISAIAS, {"region_key": "gulf_la", "time_utc": "2026-10-10T01:00:00"})[2] == "gulf_fl_panhandle"


def test_forecast_landfall_hint_reads_the_forecast_track():
    fc = [{"timestamp": "2026-10-09T21:00:00+00:00", "lat": 29.2, "lon": -87.0},
          {"timestamp": "2026-10-10T06:00:00+00:00", "lat": 31.2, "lon": -86.8},
          {"timestamp": "2026-10-10T18:00:00+00:00", "lat": 33.4, "lon": -87.2}]
    hint = cd.forecast_landfall_hint(fc)
    assert hint["region_key"] == "gulf_fl_panhandle"
    assert "2026-10-10T00:00:00" <= hint["time_utc"] <= "2026-10-10T04:00:00"
    out_to_sea = [{"timestamp": "2026-10-09T21:00:00", "lat": 27.0, "lon": -90.0},
                  {"timestamp": "2026-10-10T09:00:00", "lat": 26.0, "lon": -88.0}]
    assert cd.forecast_landfall_hint(out_to_sea) is None
    assert cd.forecast_landfall_hint([]) is None and cd.forecast_landfall_hint([{"lat": 1}]) is None


# ── what it does to the score ───────────────────────────────────────────────

def test_the_same_storm_no_longer_scores_22_points_apart_three_hours_apart():
    # Identical storm at the 12Z and 15Z positions.
    old_12, r12 = cd.compute_snapshot_dpi(ISAIAS[2])
    old_15, r15 = cd.compute_snapshot_dpi(ISAIAS[3])
    assert r12.region_key == "gulf_la" and r15.region_key == "gulf_fl_panhandle"
    assert old_12 - old_15 > 15                    # the cliff, as it was

    regions = cd.landfall_regions(ISAIAS)
    new_12, n12 = cd.compute_snapshot_dpi(ISAIAS[2], regions[2])
    new_15, _ = cd.compute_snapshot_dpi(ISAIAS[3], regions[3])
    assert n12.region_key == "gulf_fl_panhandle"
    assert abs(new_12 - new_15) < 3
    assert new_15 == pytest.approx(old_15)         # the fix that was already right is untouched

    result = cd.compute_cumulative_dpi(ISAIAS, storm_name="Isaias", storm_year=2026, basin="ATLANTIC")
    assert result.peak_dpi == pytest.approx(max(new_12, new_15), abs=3)
    assert result.peak_dpi < old_12 - 15


def test_a_storm_headed_for_the_coast_it_is_nearest_does_not_move():
    # Katrina's Gulf leg: nearest Louisiana, and it hit Louisiana.
    katrina = [_snap(0, 26.0, -88.2, kt=148, pres=905), _snap(6, 26.7, -88.9, kt=145, pres=904),
               _snap(12, 27.7, -89.4, kt=133, pres=910), _snap(18, 28.8, -89.6, kt=116, pres=917),
               _snap(21, 29.5, -89.6, kt=110, pres=923)]
    regions = cd.landfall_regions(katrina)
    assert regions[0] == "gulf_la"
    for snap, region in zip(katrina, regions):
        assert cd.compute_snapshot_dpi(snap, region)[0] == pytest.approx(cd.compute_snapshot_dpi(snap)[0])


def test_a_fix_with_its_own_coast_ignores_the_landfall_region():
    at_coast = ISAIAS[6]                           # 30.2N 86.6W, inside the Panhandle box
    assert cd.compute_snapshot_dpi(at_coast, "gulf_la")[0] == pytest.approx(cd.compute_snapshot_dpi(at_coast)[0])
    # Western Pacific fixes carry explicit regions (or an explicit open ocean).
    for lat, lon in ((14.6, 121.5), (20.0, 135.0)):
        wp = _snap(0, lat, lon, kt=120, pres=930)
        assert cd._estimate_region_from_coords(lat, lon) is not None
        assert cd.compute_snapshot_dpi(wp, "gulf_la")[0] == pytest.approx(cd.compute_snapshot_dpi(wp)[0])


def test_the_engine_takes_the_forecast_landfall_for_a_live_storm():
    at_sea = ISAIAS[:4]
    plain = compute_storm_dps(storm_id="AL092026", snapshots=at_sea, storm_name="Isaias", storm_year=2026)
    hinted = compute_storm_dps(storm_id="AL092026", snapshots=at_sea, storm_name="Isaias", storm_year=2026,
                               landfall_hint={"region_key": "gulf_fl_panhandle", "time_utc": "2026-10-10T01:00:00"})
    assert hinted["dps"] < plain["dps"] - 10       # Louisiana's profile no longer sets the peak
    series = [x["dpi"] for x in hinted["dpi_timeseries"]]
    assert max(series) - min(series[2:]) < 6       # 12Z and 15Z agree
