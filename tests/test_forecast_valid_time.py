"""
Locks the machine-readable valid time on TCM forecast points.

The displayed "current position" fix (freshest-by-valid-time selection on the
frontend) depends on forecast_track[0] carrying a real, timezone-aware
`valid_time_utc` — the advisory tau=0 analysis center. Before this, tau=0 had
only a display-only "time" string and the None-valued top-level field made
freshness comparison impossible, so the map/chip fell back to the staler
best-track tail (Hernan showed ~9h behind NHC). If these assertions break,
the current-position marker will silently regress to the lagging track tail.
"""
from datetime import datetime, timezone

from services.noaa_client import _parse_tcm_forecast


_SAMPLE_TCM = """
WTPZ34 KNHC 150300
TCMEP8

HERNAN FORECAST/ADVISORY NUMBER  12
CENTER LOCATED NEAR 16.2N 132.9W AT 15/0300Z
MAX SUSTAINED WINDS  30 KT WITH GUSTS  40 KT.

FORECAST VALID 15/1200Z 16.3N 134.5W
MAX WIND  30 KT...GUSTS  40 KT.

FORECAST VALID 16/0000Z 16.5N 137.0W
MAX WIND  25 KT...GUSTS  35 KT.
"""


def test_tau0_has_timezone_aware_valid_time_matching_analysis():
    pts = _parse_tcm_forecast(_SAMPLE_TCM)
    assert pts, "parser returned no points for a well-formed TCM"
    tau0 = pts[0]
    assert tau0["hour"] == 0
    # Signed decimal degrees, W negative — matches NHC's live current position.
    assert tau0["lat"] == 16.2 and tau0["lon"] == -132.9
    vt = tau0.get("valid_time_utc")
    assert vt, "tau=0 must carry a machine-readable valid_time_utc"
    # Must be explicitly UTC, else JS Date() parses it as browser-local time.
    parsed = datetime.fromisoformat(vt)
    assert parsed.tzinfo is not None, "valid_time_utc must be timezone-aware"
    assert parsed.utcoffset() == timezone.utc.utcoffset(None)
    # Equals the TCM 'AT 15/0300Z' analysis stamp.
    assert parsed.astimezone(timezone.utc).strftime("%d/%H%MZ") == "15/0300Z"


def test_forecast_points_also_carry_valid_time():
    pts = _parse_tcm_forecast(_SAMPLE_TCM)
    later = [p for p in pts if p["hour"] > 0]
    assert later, "expected forecast points beyond tau=0"
    for p in later:
        vt = p.get("valid_time_utc")
        assert vt, f"forecast point at +{p['hour']}h missing valid_time_utc"
        assert datetime.fromisoformat(vt).tzinfo is not None


def test_valid_times_are_monotonic():
    pts = _parse_tcm_forecast(_SAMPLE_TCM)
    times = [datetime.fromisoformat(p["valid_time_utc"]) for p in pts]
    assert times == sorted(times), "forecast valid times must be non-decreasing"


def test_malformed_tcm_fails_open():
    assert _parse_tcm_forecast("no center line here") == []
