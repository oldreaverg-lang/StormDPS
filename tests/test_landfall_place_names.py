"""Landfall place names and the anchored landfall estimate (Isaias, 2026-10-09).

The landfall panel named "Mobile Bay", then "Panama City", then "Pensacola"
for a storm coming ashore at Fort Walton Beach: the coastline waypoints'
labels sit tens of km from the towns they name, and the estimate ignored the
newer intermediate-advisory position. Names now come from core.coastal_places
and the estimate restarts at the newest official position.
"""
from core.coastal_places import US_COASTAL_PLACES, _km, nearest_place, place_name
from core.landfall_forecast import anchor_track, compute_forecast_landfall

# Advisory 13 (21Z 2026-10-09) forecast track, and the 00Z intermediate position.
TRACK_21Z = [
    {"hour": 0, "lat": 29.2, "lon": -87.0, "max_wind_kt": 100},
    {"hour": 9, "lat": 31.2, "lon": -86.8, "max_wind_kt": 75},
    {"hour": 21, "lat": 33.4, "lon": -87.2, "max_wind_kt": 40},
]


def test_town_list_is_sane():
    names = [n for _, _, n in US_COASTAL_PLACES]
    assert len(names) == len(set(names)) > 100
    for lat, lon, name in US_COASTAL_PLACES:
        assert 17 < lat < 46 and -98.5 < lon < -64, name
    # Spot checks against well-known coordinates.
    for name, lat, lon in (("Pensacola, FL", 30.42, -87.22), ("Panama City, FL", 30.16, -85.66),
                           ("Destin, FL", 30.39, -86.50), ("Galveston, TX", 29.30, -94.80),
                           ("Gulfport, MS", 30.37, -89.09), ("New Orleans, LA", 29.95, -90.07)):
        p = next(x for x in US_COASTAL_PLACES if x[2] == name)
        assert _km(p[0], p[1], lat, lon) < 5, name


def test_nearest_place_and_fallback():
    assert nearest_place(30.39, -86.63)[0] == "Fort Walton Beach, FL"
    assert nearest_place(15.0, -40.0) is None                       # mid-Atlantic
    assert place_name(19.7, -155.1, fallback="Hilo, HI") == "Hilo, HI"   # not covered: caller's label
    assert place_name(None, None, fallback="x") == "x"


def test_official_line_names_the_town_where_it_crosses():
    lf = compute_forecast_landfall(TRACK_21Z)
    assert lf["expected"] and lf["nearest_name"] == "Navarre, FL"   # was "Pensacola, FL" (label near Niceville)


def test_anchored_at_the_newer_position_moves_the_landfall():
    anchored = anchor_track(TRACK_21Z, 30.1, -86.6, 3.0, 95)
    assert [p["hour"] for p in anchored] == [3.0, 9, 21] and anchored[0]["max_wind_kt"] == 95
    lf = compute_forecast_landfall(anchored)
    assert lf["expected"] and lf["nearest_name"] == "Fort Walton Beach, FL"
    assert lf["eta_hour"] == 3                                      # at the coast as of the 00Z position


def test_anchor_is_a_no_op_without_a_newer_position():
    assert anchor_track(TRACK_21Z, 30.1, -86.6, 0.2) is TRACK_21Z   # same advisory
    assert anchor_track(TRACK_21Z, None, -86.6, 3.0) is TRACK_21Z   # no position
    assert anchor_track(TRACK_21Z, 30.1, -86.6, 30.0) is TRACK_21Z  # past the end of the track
    assert anchor_track(TRACK_21Z, 30.1, -86.6, -2.0) is TRACK_21Z  # live list OLDER than the track
    assert anchor_track([], 30.1, -86.6, 3.0) == []
    # No intensity supplied: carry the last forecast value at or before that hour.
    assert anchor_track(TRACK_21Z, 30.1, -86.6, 3.0)[0]["max_wind_kt"] == 100


def test_names_outside_the_town_list_are_unchanged():
    # Pacific Mexico and Hawaii are not in the US Gulf/Atlantic list: the
    # estimator's own labels still apply.
    lf = compute_forecast_landfall([
        {"hour": 0, "lat": 17.0, "lon": -105.5, "max_wind_kt": 90},
        {"hour": 24, "lat": 19.0, "lon": -104.4, "max_wind_kt": 85},
    ])
    assert lf["nearest_name"] and lf["nearest_name"].endswith("Mexico")


def test_rain_bar_samples_real_towns():
    from services import land_rain_client as c
    c._places_cache = None
    places = c.all_places()
    names = {n for _, _, n in places}
    assert {"Fort Walton Beach, FL", "Destin, FL", "Navarre, FL", "Hilo, HI"} <= names
    # The mislabelled waypoint ("Panama City, FL" at Destin's coordinates) is gone.
    assert not [p for p in places if p[2] == "Panama City, FL" and abs(p[1] - (-86.511)) < 0.01]
    c._places_cache = None


def test_nhc_forward_speed_is_converted_from_mph():
    from services.noaa_client import _mph_to_kt
    assert _mph_to_kt(18) == 15.6 and _mph_to_kt("9") == 7.8      # advisory 13A: 18 MPH = 15 KT in the TCM
    assert _mph_to_kt(None) is None and _mph_to_kt("") is None
