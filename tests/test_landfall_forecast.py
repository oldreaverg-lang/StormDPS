"""Offline unit tests for core/landfall_forecast.py (public landfall panel).

Fully deterministic — synthetic forecast tracks against the embedded
coastline waypoint DB. No network.
"""
from core.landfall_forecast import (
    CONE_ERR_NM,
    LANDFALL_KM,
    compute_forecast_landfall,
    interp_err_nm,
)


def _pt(hour, lat, lon, wind=None, time=""):
    return {"hour": hour, "lat": lat, "lon": lon, "max_wind_kt": wind, "time": time}


# Gulf approach toward Galveston, TX (29.3, -94.8), landfalling inside +48 h.
GULF_TRACK = [
    _pt(0, 25.0, -90.0, 100),
    _pt(12, 27.0, -92.5, 105),
    _pt(24, 28.5, -94.0, 100),
    _pt(36, 29.3, -94.75, 90),
    _pt(48, 30.2, -95.6, 45),
]

# Recurving mid-Atlantic fish storm — never near any coastline waypoint.
FISH_TRACK = [
    _pt(0, 25.0, -55.0, 70),
    _pt(24, 28.0, -52.0, 75),
    _pt(48, 32.0, -48.0, 65),
    _pt(72, 38.0, -45.0, 55),
]


def test_gulf_landfall_expected():
    lf = compute_forecast_landfall(GULF_TRACK)
    assert lf["expected"] is True
    assert lf["eta_hour"] is not None and 0 < lf["eta_hour"] <= 40
    assert lf["nearest_name"], "coastline waypoint name should resolve"
    assert lf["region_key"], "COASTAL_PROFILES region should resolve"
    assert lf["wind_kt"] is not None and lf["wind_kt"] >= 34
    assert lf["min_distance_km"] <= LANDFALL_KM
    assert "coast" in lf["description"]


def test_gulf_window_sane():
    lf = compute_forecast_landfall(GULF_TRACK)
    assert lf["window_start_hour"] <= lf["eta_hour"] <= lf["window_end_hour"]
    assert lf["window_start_hour"] >= 0
    assert lf["window_end_hour"] <= 48  # clamped to forecast horizon
    # cone error / forward speed should give a non-degenerate window
    assert lf["window_end_hour"] - lf["window_start_hour"] >= 6


def test_fish_storm_stays_offshore():
    lf = compute_forecast_landfall(FISH_TRACK)
    assert lf["expected"] is False
    assert lf["coverage"] is True  # in-basin: "offshore" is a real claim here
    assert lf["eta_hour"] is None
    assert lf["min_distance_km"] is not None and lf["min_distance_km"] > 200
    assert lf["closest_hour"] is not None
    assert "offshore" in lf["description"]


def test_open_ocean_reference_waypoints_are_not_coastline():
    # The waypoint DB carries two "open_ocean" REFERENCE points (15N/30W and
    # 10N/50W). A Cape Verde track crossing 10N/50W must NOT "landfall" at
    # "Western Atlantic Deep (reference)" — review blocker 2026-07-09.
    track = [
        _pt(0, 8.0, -45.0, 60),
        _pt(24, 10.0, -50.0, 70),   # directly over the reference point
        _pt(48, 12.0, -55.0, 75),
    ]
    lf = compute_forecast_landfall(track)
    assert lf["expected"] is False
    assert lf["coverage"] is True
    assert "offshore" in lf["description"]
    assert lf["min_distance_km"] > LANDFALL_KM
    # closest-approach name must be a real coastline waypoint, never a ref
    assert lf["nearest_name"] is None or "reference" not in lf["nearest_name"].lower()


def test_east_pacific_gap_reports_no_coverage_not_offshore():
    # EP track toward Manzanillo: inside the bounding box (via the Baja
    # waypoints) but mainland Pacific Mexico has no coastline waypoints —
    # must report missing coverage, not a false "stays offshore".
    track = [
        _pt(0, 14.0, -100.0, 80),
        _pt(24, 16.5, -103.0, 90),
        _pt(48, 19.0, -104.5, 95),
    ]
    lf = compute_forecast_landfall(track)
    assert lf["coverage"] is False
    assert lf["expected"] is False
    assert lf["description"] == ""


def test_offshore_closest_approach_names_real_coast():
    # expected=False in-coverage results should carry the closest-approach
    # waypoint name so callers can say "passes ~N km off <place>".
    lf = compute_forecast_landfall(FISH_TRACK)
    assert lf["nearest_name"], "closest-approach name should be populated"
    assert "reference" not in lf["nearest_name"].lower()


def test_west_pacific_track_is_covered_and_lands_on_taiwan():
    # [Tranche B] BAVI-like WP typhoon heading for Taiwan — the WP waypoints
    # now give the panel real coverage and a forecast landfall with a wp_*
    # region key.
    wp_track = [
        _pt(0, 20.8, 127.3, 75),
        _pt(24, 22.5, 124.5, 85),
        _pt(48, 23.8, 121.8, 90),   # ~Taiwan east coast
        _pt(72, 24.5, 119.5, 70),
    ]
    lf = compute_forecast_landfall(wp_track)
    assert lf["coverage"] is True
    assert lf["expected"] is True
    assert lf["region_key"] == "wp_taiwan"
    assert lf["nearest_name"], "landfall waypoint name should be populated"
    assert 24 <= lf["eta_hour"] <= 60


def test_north_indian_track_is_covered_and_lands_on_bangladesh():
    # [NI_DPS_AUDIT] Bay of Bengal cyclone heading for the Bangladesh delta —
    # now that NI waypoints exist the panel gives real coverage + a forecast
    # landfall with an ni_* region key (was "no coverage" pre-NI-tranche).
    ni_track = [
        _pt(0, 12.0, 88.0, 70),
        _pt(24, 15.0, 89.0, 85),
        _pt(48, 18.5, 89.6, 90),
        _pt(72, 21.8, 89.6, 75),   # ~Sundarbans / Bangladesh coast
    ]
    lf = compute_forecast_landfall(ni_track)
    assert lf["coverage"] is True
    assert lf["expected"] is True
    assert lf["region_key"] == "ni_bangladesh"
    assert lf["nearest_name"], "landfall waypoint name should be populated"


def test_bay_of_bengal_deep_water_still_no_false_offshore():
    # A cyclone that never approaches a coast (central Bay of Bengal) must not
    # claim "stays offshore" when there's genuinely no near coastline in range.
    deep = [_pt(0, 10.0, 87.0, 60), _pt(24, 11.0, 87.0, 70),
            _pt(48, 12.0, 87.0, 65)]
    lf = compute_forecast_landfall(deep)
    assert lf["expected"] is False


def test_already_at_coast_reports_now():
    track = [_pt(0, 29.3, -94.8, 80), _pt(12, 30.5, -95.5, 50)]
    lf = compute_forecast_landfall(track)
    assert lf["expected"] is True
    assert lf["eta_hour"] == 0
    assert "now" in lf["description"]


def test_degenerate_tracks_fail_open():
    assert compute_forecast_landfall([]) == compute_forecast_landfall(None)
    assert compute_forecast_landfall([_pt(0, 25.0, -90.0, 60)])["expected"] is False
    # missing coordinates are filtered, not fatal
    lf = compute_forecast_landfall([
        {"hour": 0, "lat": None, "lon": -90.0},
        _pt(12, 26.0, -91.0, 60),
    ])
    assert lf["expected"] is False


def test_missing_wind_is_tolerated():
    track = [_pt(0, 27.0, -93.0), _pt(24, 29.3, -94.8)]
    lf = compute_forecast_landfall(track)
    assert lf["expected"] is True
    assert lf["wind_kt"] is None


def test_interp_err_nm_matches_table_and_interpolates():
    for h, v in CONE_ERR_NM.items():
        assert interp_err_nm(h) == v
    assert interp_err_nm(18) == (CONE_ERR_NM[12] + CONE_ERR_NM[24]) / 2
    assert interp_err_nm(999) == CONE_ERR_NM[120]
