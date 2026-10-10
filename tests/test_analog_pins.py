"""Hand-picked analogs lead "Storms like this one".

Opal 1995 is the storm a reader reaches for beside Isaias 2026: the same
stretch of the Florida Panhandle, also in early October. The analog finder
could not offer it: Opal is not in the compiled bundle, and at DPS 75 it sits
outside the 20-point gate around Isaias's 52.
"""
import asyncio
import json

import pytest

import api.routes as routes
from core import analog_pins

OPAL = "1995271N19273"


def _storm(name, year, dps, lat, lon, wind=100, basin="ATLANTIC"):
    return {"name": name, "year": year, "dps": dps, "dps_label": "Severe", "basin": basin,
            "category": 3, "peak_wind_kt": wind, "peak_ike_tj": 40, "rainfall_warning": 20,
            "track_hours": 120, "track_geo": {"mean_lat": lat, "mean_lon": lon},
            "landfalls": [{"lat": lat + 3, "lon": lon, "wind_kt": wind}]}


POOL = {
    "AL062024": _storm("Francine", 2024, 54, 26.0, -92.0),
    "AL192020": _storm("Sally", 2020, 39, 27.5, -86.5),
    "AL222020": _storm("Beta", 2020, 47, 26.0, -93.5, wind=55),
    "AL012024": _storm("Alberto", 2024, 44, 22.0, -95.0, wind=45),
    "AL142018": _storm("Michael", 2018, 82, 25.0, -86.0, wind=140),     # 30 points away: gated out
}
LIVE = {
    "AL092026": _storm("Isaias", 2026, 52.2, 26.5, -87.5, wind=105),
    OPAL: _storm("Opal", 1995, 74.8, 25.0, -90.0, wind=130),
}


@pytest.fixture
def analogs(monkeypatch):
    calls = []

    async def fake_dps(storm_id, name=None, year=None, grid_resolution_km=15.0, skip_points=0, force=False):
        calls.append(storm_id.upper())
        if storm_id.upper() not in LIVE:
            raise routes.HTTPException(status_code=404, detail="no such storm")
        return dict(LIVE[storm_id.upper()])

    monkeypatch.setattr(routes, "_analog_pool", lambda: POOL)
    monkeypatch.setattr(routes, "get_storm_dps", fake_dps)

    def run(storm_id, n=4):
        resp = asyncio.run(routes.get_storm_analogs(storm_id, n=n))
        return json.loads(resp.body)["analogs"], calls, resp.headers.get("cache-control")

    return run


def test_isaias_is_pinned_to_opal_under_either_capitalisation():
    for sid in ("AL092026", "al092026", " AL092026 "):
        pins = analog_pins.pins_for(sid)
        assert [p["id"] for p in pins] == [OPAL] and "Panhandle" in pins[0]["why"]
    assert analog_pins.pins_for("AL122005") == [] and analog_pins.pins_for("") == []
    # A copy: a caller cannot edit the table.
    analog_pins.pins_for("AL092026")[0]["id"] = "X"
    assert analog_pins.pins_for("AL092026")[0]["id"] == OPAL


def test_the_pin_leads_the_strip_and_says_why(analogs):
    out, calls, cache = analogs("AL092026")
    assert out[0]["id"] == OPAL and out[0]["name"] == "Opal" and out[0]["year"] == 1995
    assert out[0]["dps"] == 75 and out[0]["pinned"] is True
    assert out[0]["why"] == "Same stretch of the Florida Panhandle, also in early October"
    # 23 points from Isaias: the ranking's 20-point gate would have refused it.
    assert abs(74.8 - 52.2) > routes._ANALOG_MAX_DPS_GAP
    assert OPAL in calls                               # scored through /dps: it is not a bundle storm
    assert "600" in cache


def test_computed_analogs_follow_and_the_strip_stays_its_size(analogs):
    out, _calls, _cache = analogs("AL092026", n=4)
    assert len(out) == 4
    rest = out[1:]
    assert all(not a.get("pinned") for a in rest)
    assert {a["name"] for a in rest} <= {"Francine", "Sally", "Beta", "Alberto"}
    assert "Michael" not in {a["name"] for a in out}    # the gate still applies to the ranking
    assert [a["id"] for a in out].count(OPAL) == 1
    assert len(analogs("AL092026", n=1)[0]) == 1 and analogs("AL092026", n=1)[0][0]["id"] == OPAL


def test_storms_without_pins_are_unchanged(analogs, monkeypatch):
    monkeypatch.setitem(LIVE, "AL992026", _storm("Test", 2026, 50, 26.0, -90.0))
    out, calls, _cache = analogs("AL992026")
    assert out and all("pinned" not in a for a in out)
    assert OPAL not in calls


def test_a_pin_that_cannot_be_scored_is_skipped(analogs, monkeypatch):
    monkeypatch.delitem(LIVE, OPAL)
    out, _calls, _cache = analogs("AL092026")
    assert out and all(a["id"] != OPAL for a in out) and all("pinned" not in a for a in out)


def test_a_storm_is_never_pinned_to_itself(analogs, monkeypatch):
    monkeypatch.setitem(analog_pins.PINNED_ANALOGS, "AL092026", ({"id": "al092026", "why": "x"},))
    out, _calls, _cache = analogs("AL092026")
    assert all(a["id"] != "AL092026" for a in out)
