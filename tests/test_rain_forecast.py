"""Forecast rainfall-hazard score (EXPERIMENTAL C1). See core/rain_forecast.py.

The invariant that matters: the rain axis is INDEPENDENT of wind. A fast Cat 5
(little rain) must score well below a stalling storm, and the whole thing must
fail open on junk input.
"""
import sys
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.rain_forecast import compute_forecast_rain_score


def _leg(hour, lat, lon, kt):
    return {"hour": hour, "lat": lat, "lon": lon, "max_wind_kt": kt}


def _straight_track(speed_kt, wind_kt, hours=72, step=6):
    """A storm moving due west at a constant speed and intensity."""
    pts, lon = [], -80.0
    dlon_per_h = speed_kt / 60.0  # ~1 deg lon ≈ 60 nm near the equator-ish
    for h in range(0, hours + 1, step):
        pts.append(_leg(h, 25.0, lon - dlon_per_h * h, wind_kt))
    return pts


def test_fast_cat5_scores_below_slow_mover():
    fast = compute_forecast_rain_score(_straight_track(20, 140))   # racing Cat 5
    slow = compute_forecast_rain_score(_straight_track(4, 90))     # crawling Cat 2
    assert fast["available"] and slow["available"]
    assert fast["rain_score"] < slow["rain_score"], (fast["rain_score"], slow["rain_score"])


def test_stall_pins_high():
    # a near-stationary hurricane (2 kt) should approach the top of the scale
    stalled = compute_forecast_rain_score(_straight_track(2, 90, hours=120))
    assert stalled["rain_score"] >= 80


def test_score_in_range_and_band_consistent():
    r = compute_forecast_rain_score(_straight_track(10, 100))
    assert 0 <= r["rain_score"] <= 100
    assert r["band"] in ("Low", "Moderate", "Elevated", "High", "Extreme")
    assert r["subscores"]["magnitude"] == r["rain_score"]
    # anomaly / inundation are deferred to SurgeDPS
    assert r["subscores"]["anomaly"] is None
    assert r["subscores"]["inundation"] is None


def test_does_not_touch_dps_shape():
    # experimental payload must be self-contained + serializable + flagged
    import json
    r = compute_forecast_rain_score(_straight_track(8, 110))
    json.dumps(r)  # must not raise
    assert "not used in DPS" in r["note"]


def test_fail_open():
    for bad in (None, [], [{"hour": 0, "lat": 25, "lon": -90, "max_wind_kt": 80}]):
        r = compute_forecast_rain_score(bad)
        assert r["available"] is False
        assert r["rain_score"] is None


def test_sub_ts_track_not_available():
    # a weak (< TS) system produces no meaningful organized rain score
    r = compute_forecast_rain_score(_straight_track(10, 25))
    assert r["available"] is False
