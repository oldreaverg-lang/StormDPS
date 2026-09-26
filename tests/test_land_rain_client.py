"""Source order + caching of services/land_rain_client (no network)."""
import asyncio
import json

import pytest

from services import land_rain_client as c
from tests.test_land_rain import NO_RAIN, NOLO_23A

NOLO_TRACK = [{"hour": 0, "lat": 16.9, "lon": -155.3}, {"hour": 21, "lat": 16.8, "lon": -156.9},
              {"hour": 45, "lat": 17.2, "lon": -160.6}]
WP_TRACK = [{"hour": 0, "lat": 22.9, "lon": 126.8}, {"hour": 48, "lat": 25.5, "lon": 126.0}]


class _Resp:
    def __init__(self, status=200, body=None, text=None):
        self.status_code, self._body = status, body
        self.text = text if text is not None else json.dumps(body)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._body


class FakeClient:
    """Routes by URL prefix; records every call."""
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    async def get(self, url, **kw):
        self.calls.append(url)
        for prefix, resp in self.routes.items():
            if url.startswith(prefix):
                return resp(url, kw) if callable(resp) else resp
        return _Resp(404, {})


def _current(storm_id, url):
    return _Resp(200, {"activeStorms": [{"id": storm_id, "publicAdvisory": {
        "url": url, "issuance": "2026-09-26T12:00:00.000Z"}}]})


@pytest.fixture(autouse=True)
def _clean():
    c._cache.clear(); c._inflight.clear(); c._nws_grid.clear()
    c._places_cache = [(19.72, -155.08, "Hilo, HI"), (19.06, -155.59, "Naalehu, HI"),
                       (26.21, 127.68, "Naha, Okinawa"), (24.34, 124.16, "Ishigaki")]
    yield
    c._places_cache = None


def test_advisory_with_rain_plus_nws_detail():
    grid = {"properties": {"quantitativePrecipitation": {"values": [
        {"validTime": "2026-01-01T00:00:00+00:00/P3650D", "value": 99999}]}}}   # spans the window
    cl = FakeClient({
        c.NHC_CURRENT_STORMS: _current("ep152026", "https://nhc/tcp"),
        "https://nhc/tcp": _Resp(text=NOLO_23A),
        "https://api.weather.gov/points/": lambda u, kw: _Resp(200, {"properties": {
            "forecastGridData": "https://api.weather.gov/gridpoints/HFO/1,1"}}),
        "https://api.weather.gov/gridpoints/": _Resp(200, grid),
    })
    res = asyncio.run(c._build("ep152026", NOLO_TRACK, cl))
    assert res["show"] and res["source"] == "advisory" and res["center"] == "CPHC"
    assert res["points_source"] == "nws" and {p["name"] for p in res["points"]} == {"Hilo, HI", "Naalehu, HI"}
    assert not any("open-meteo" in u for u in cl.calls)          # official product: no model call


def test_official_silence_skips_every_other_source():
    cl = FakeClient({c.NHC_CURRENT_STORMS: _current("al072026", "https://nhc/tcp"),
                     "https://nhc/tcp": _Resp(text=NO_RAIN)})
    res = asyncio.run(c._build("al072026", NOLO_TRACK, cl))
    assert res["available"] and not res["show"]
    assert cl.calls == [c.NHC_CURRENT_STORMS, "https://nhc/tcp"]


def test_jtwc_basin_uses_the_model_in_one_request():
    from datetime import datetime, timedelta, timezone
    t0 = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0, tzinfo=None)
    times = [(t0 + timedelta(hours=h)).isoformat(timespec="minutes") for h in range(96)]

    def om(url, kw):
        n = len(kw["params"]["latitude"].split(","))
        return _Resp(200, [{"hourly": {"time": times, "precipitation": [0.0] * 96}}] * n)
    cl = FakeClient({c.OPEN_METEO: om})
    res = asyncio.run(c._build("WP252026", WP_TRACK, cl))
    assert cl.calls == [c.OPEN_METEO]                             # no NHC, no NWS for Okinawa
    assert res["available"] and not res["show"] and res["source"] == "model"
    assert {p["name"] for p in res["points"]} == {"Naha, Okinawa", "Ishigaki"}


def test_cache_serves_and_failures_never_replace_a_good_result(monkeypatch):
    built = []

    async def ok(sid, track, client):
        built.append(sid)
        return {"available": True, "show": True, "text": "x"}

    monkeypatch.setattr(c, "_build", ok)

    async def run():
        first = await c.get_land_rain("EP152026", NOLO_TRACK, http_client=FakeClient({}))
        again = await c.get_land_rain("ep152026", NOLO_TRACK, http_client=FakeClient({}))
        return first, again

    first, again = asyncio.run(run())
    assert first == again and built == ["EP152026"]               # second call: cache hit

    async def boom(sid, track, client):
        raise RuntimeError("NHC down")

    monkeypatch.setattr(c, "_build", boom)
    c._cache["EP152026"] = (0.0, first)                           # expired
    asyncio.run(c._refresh("EP152026", "EP152026", NOLO_TRACK, FakeClient({})))
    assert c._cache["EP152026"][1] == first                       # kept the good one
