"""/storms/active/overview — every live storm for the homepage map (2026-09-25)."""
import asyncio

import api.routes as routes


def test_thin_keeps_ends_and_bounds_size():
    pts = list(range(500))
    out = routes._thin(pts, 80)
    assert len(out) == 80 and out[0] == 0 and out[-1] == 499
    assert routes._thin([1, 2, 3], 80) == [1, 2, 3]


def test_track_carries_interpolated_dps(monkeypatch):
    rows = [{"lat": 15.0, "lon": -100.0, "timestamp": "2026-09-23T00:00:00+00:00"},
            {"lat": 15.5, "lon": -101.0, "timestamp": "2026-09-23T03:00:00+00:00"},
            {"lat": 16.0, "lon": -102.0, "timestamp": "2026-09-23T06:00:00"}]    # naive = UTC
    series = [{"t": "2026-09-23T00:00:00+00:00", "dpi": 40.0},
              {"t": "2026-09-23T06:00:00+00:00", "dpi": 70.0}]
    monkeypatch.setattr(routes, "_load_ike_cache",
                        lambda sid, g, s, max_age_s=None: rows if sid == "ep172026" else None)
    monkeypatch.setattr(routes, "_load_dps_cache",
                        lambda sid: {"dpi_timeseries": series} if sid == "EP172026" else None)
    track = routes._overview_track("EP172026")         # finds the lower-case cache key
    assert [p[2] for p in track] == [40.0, 55.0, 70.0]
    assert track[0][:2] == [15.0, -100.0]


def test_build_overview_sorted_and_fail_soft(monkeypatch):
    monkeypatch.setattr(routes, "_active_storms_cache", [
        {"id": "ep162026", "name": "Odalys", "classification": "HU", "lat": 16.0, "lon": -126.0},
        {"id": "ep172026", "name": "Polo", "classification": "HU", "lat": 16.7, "lon": -104.0},
    ])
    monkeypatch.setattr(routes, "_active_dps", lambda sid: {"EP172026": 85.0, "EP162026": 15.0}.get(sid.upper()))
    monkeypatch.setattr(routes, "_stall_near_land", lambda la, lo: False)
    monkeypatch.setattr(routes, "_load_dps_cache", lambda sid: {"dps_label": "Devastating"} if sid.upper() == "EP172026" else None)
    monkeypatch.setattr(routes, "_overview_track", lambda sid: [[16.0, -103.0, 80.0]])

    async def _fc(client, sid):
        if sid.lower() == "ep162026":
            raise RuntimeError("NHC down")                # one storm failing must not sink the rest
        return {"forecast": [[16.7, -104.0, 0], [17.5, -106.0, 12]], "cone": [[1, 2], [3, 4], [5, 6]]}

    monkeypatch.setattr(routes, "_overview_forecast", _fc)
    out = asyncio.run(routes._build_overview(None))
    names = [s["name"] for s in out["storms"]]
    assert names == ["Polo", "Odalys"]                    # most destructive first
    polo, odalys = out["storms"]
    assert polo["dps"] == 85.0 and polo["dps_label"] == "Devastating"
    assert polo["forecast"] and len(polo["cone"]) == 3 and polo["track"]
    assert odalys["forecast"] == [] and odalys["cone"] == []
