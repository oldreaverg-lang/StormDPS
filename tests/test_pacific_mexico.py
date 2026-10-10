"""Mexico's Pacific coast has its own scoring regions and profiles.

2026-10-10, Hurricane Simon (EP202026), a 130 kt Cat 4 forecast into Jalisco.
The coastline waypoint DB has two points on the whole Pacific side of Mexico
(both near La Paz), so a fix off Jalisco took "mex_baja" once it came within
930 km of La Paz and "open_ocean" before that: a 23-point jump between two
fixes 20 km apart. And neither "mex_baja" nor "mex_veracruz" (what Acapulco
resolved to, across the isthmus) had a profile at all, so both were scored
with a generic default built on US property values.
"""
from datetime import datetime, timedelta

import pytest

from core import cumulative_dpi as cd
from core import pacific_coast as pc
from core.dps_engine import compute_storm_dps
from core.economic_vulnerability import ECONOMIC_PROFILES
from core.storm_surge import COASTAL_PROFILES

T0 = datetime(2026, 10, 9, 0)


def _snap(hours, lat, lon, kt=120, pres=945, r34=90, r64=30, rmw=15, fwd=5, ike=12.0):
    return {
        "timestamp": (T0 + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S"),
        "lat": lat, "lon": lon, "max_wind_ms": kt * 0.514444, "min_pressure_hpa": pres,
        "r34_nm": r34, "r64_nm": r64, "rmw_nm": rmw, "forward_speed_knots": fwd, "ike_total_tj": ike,
    }


# ── the point list keeps serving its presentation callers ───────────────────

def test_point_groups_add_up_to_the_list_the_site_already_used():
    assert pc.PACIFIC_COAST_POINTS == (pc.MEXICO_PACIFIC_MAINLAND_POINTS + pc.BAJA_CALIFORNIA_POINTS
                                       + pc.CENTRAL_AMERICA_PACIFIC_POINTS + pc.HAWAII_POINTS)
    assert len(pc.PACIFIC_COAST_POINTS) == 51
    assert pc.PACIFIC_COAST_POINTS[0][2] == "Puerto Chiapas, Mexico"
    assert pc.PACIFIC_COAST_POINTS[-1][2] == "Lihue, HI"
    assert dict(pc.PACIFIC_MEXICO_SCORING_REGIONS) == {
        "mex_pacific": pc.MEXICO_PACIFIC_MAINLAND_POINTS, "mex_baja": pc.BAJA_CALIFORNIA_POINTS}
    names = [n for _la, _lo, n in pc.MEXICO_PACIFIC_MAINLAND_POINTS]
    assert "Acapulco, Mexico" in names and "Barra de Navidad, Mexico" in names
    assert all(n.endswith("Mexico") for n in names)


# ── which fixes belong to the coast ─────────────────────────────────────────

@pytest.mark.parametrize("lat, lon, want", [
    (19.2, -104.7, "mex_pacific"),     # Barra de Navidad
    (16.85, -99.9, "mex_pacific"),     # Acapulco
    (18.4, -104.9, "mex_pacific"),     # 90 km off Jalisco
    (23.2, -106.4, "mex_pacific"),     # Mazatlan
    (22.9, -109.9, "mex_baja"),        # Cabo San Lucas
    (24.1, -110.3, "mex_baja"),        # La Paz
])
def test_fixes_at_the_coast_take_its_region(lat, lon, want):
    assert cd._estimate_region_from_coords(lat, lon) == want


@pytest.mark.parametrize("lat, lon", [
    (17.0, -104.7),      # Simon, 250 km out: not at the coast yet
    (15.0, -115.0),      # open Pacific
    (18.6, -94.4),       # Bay of Campeche: Gulf side of the isthmus
    (19.2, -96.1),       # Veracruz
    (13.9, -90.8),       # Guatemala's Pacific coast: not a Mexico group
    (21.3, -157.9),      # Honolulu
])
def test_other_places_do_not(lat, lon):
    assert cd._pacific_mexico_region(lat, lon) is None


def test_us_and_atlantic_boxes_are_untouched():
    assert cd._estimate_region_from_coords(29.3, -94.8) == "gulf_central_tx"
    assert cd._estimate_region_from_coords(30.3, -87.1) == "gulf_fl_panhandle"
    assert cd._estimate_region_from_coords(14.6, 121.5) is not None      # WP keeps its own regions


# ── the profiles exist and are not a US coast ───────────────────────────────

def test_both_regions_have_both_profiles():
    for key in ("mex_pacific", "mex_baja"):
        assert key in COASTAL_PROFILES and key in ECONOMIC_PROFILES


def test_profiles_sit_with_their_neighbours():
    surge, econ = COASTAL_PROFILES["mex_pacific"], ECONOMIC_PROFILES["mex_pacific"]
    # A narrow, steep shelf: less surge than the wide Gulf shelves...
    assert surge.surge_amplification < COASTAL_PROFILES["mex_gulf"].surge_amplification
    assert surge.shelf_width_km < 30
    # ...and mountains behind it: more rain than the Gulf side or Baja.
    assert surge.rain_enhancement > COASTAL_PROFILES["mex_gulf"].rain_enhancement
    assert surge.rain_enhancement > COASTAL_PROFILES["mex_baja"].rain_enhancement
    # Exposure between Central America and the Mexican Gulf coast, far from a US coast.
    assert ECONOMIC_PROFILES["central_am"].exposed_value_index < econ.exposed_value_index
    assert econ.exposed_value_index <= ECONOMIC_PROFILES["mex_gulf"].exposed_value_index
    assert econ.gdp_per_capita_usd < 20000 < ECONOMIC_PROFILES["gulf_fl_panhandle"].gdp_per_capita_usd
    baja = ECONOMIC_PROFILES["mex_baja"]
    assert baja.population_density_factor < econ.population_density_factor   # a sparse peninsula
    assert baja.total_asset_ceiling_billion < econ.total_asset_ceiling_billion


# ── Simon ───────────────────────────────────────────────────────────────────

SIMON = [_snap(0, 15.5, -104.2, kt=65), _snap(12, 15.8, -104.5, kt=80), _snap(21, 16.3, -104.6, kt=98),
         _snap(24, 16.6, -104.6, kt=105), _snap(27, 16.8, -104.7, kt=113), _snap(30, 17.0, -104.7, kt=120),
         _snap(33, 17.1, -104.8, kt=125), _snap(36, 17.3, -104.8, kt=130)]
SIMON_FORECAST = [{"timestamp": "2026-10-10T09:00:00+00:00", "lat": 17.2, "lon": -104.6},
                  {"timestamp": "2026-10-10T18:00:00+00:00", "lat": 17.8, "lon": -104.7},
                  {"timestamp": "2026-10-11T06:00:00+00:00", "lat": 18.8, "lon": -104.9},
                  {"timestamp": "2026-10-11T18:00:00+00:00", "lat": 20.0, "lon": -105.1}]


def test_the_forecast_landfall_on_jalisco_is_seen():
    hint = cd.forecast_landfall_hint(SIMON_FORECAST)
    assert hint["region_key"] == "mex_pacific"
    assert "2026-10-11T06:00:00" <= hint["time_utc"] <= "2026-10-11T18:00:00"


def test_every_fix_before_landfall_is_scored_against_that_coast():
    hint = cd.forecast_landfall_hint(SIMON_FORECAST)
    assert cd.landfall_regions(SIMON) == [None] * len(SIMON)          # nothing observed yet
    assert cd.landfall_regions(SIMON, hint) == ["mex_pacific"] * len(SIMON)
    for snap in SIMON:
        assert cd.compute_snapshot_dpi(snap, "mex_pacific")[1].region_key == "mex_pacific"


def test_no_cliff_where_the_nearest_waypoint_used_to_change():
    # 03Z and 06Z on the 10th: 20 km apart, 7 kt apart. They used to score
    # 29 ("open_ocean") and 53 ("mex_baja", the generic default).
    hint = cd.forecast_landfall_hint(SIMON_FORECAST)
    regions = cd.landfall_regions(SIMON, hint)
    before = cd.compute_snapshot_dpi(SIMON[4], regions[4])[0]
    after = cd.compute_snapshot_dpi(SIMON[5], regions[5])[0]
    assert 0 < after - before < 8
    series = [x["dpi"] for x in compute_storm_dps(
        storm_id="EP202026", snapshots=SIMON, storm_name="Simon", storm_year=2026,
        landfall_hint=hint)["dpi_timeseries"]]
    assert series == sorted(series)                                    # rises with the wind, no step
    assert max(b - a for a, b in zip(series, series[1:])) < 20


def test_an_atlantic_storm_in_the_bay_of_campeche_is_unchanged():
    gulf_side = _snap(0, 19.5, -94.5, kt=90)
    assert cd._estimate_region_from_coords(19.5, -94.5) is None
    plain, res = cd.compute_snapshot_dpi(gulf_side)
    assert res.region_key not in ("mex_pacific", "mex_baja")
