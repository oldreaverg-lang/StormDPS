"""The hourly IMERG ingest must not churn DPS bundles or re-fetch settled storms.

2026-09-23 memory investigation: _set_rainfall reported every re-recorded
value as a change, so the hourly season ingest invalidated (and the hourly
DPS loop recomputed, ~600-800 MB each) every current-season storm, every hour
— and re-downloaded IMERG for storms that ended months ago.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from core import ground_truth as gt


@pytest.fixture
def clean_registry(monkeypatch):
    monkeypatch.setattr(gt, "_REGISTRY", dict(gt._REGISTRY))
    monkeypatch.setattr(gt, "_RAIN_TIER", dict(gt._RAIN_TIER))


def test_same_value_is_not_a_change(clean_registry):
    sid = "EP992026"
    assert gt._set_rainfall(sid, "Test", 2026, 12.34, (15.0, -100.0), "IMERG Late", gt._TIER_LATE)
    assert gt.has_live_rainfall(sid)
    # identical re-record (float noise below the persisted 0.01 in) -> no change
    assert not gt._set_rainfall(sid, "Test", 2026, 12.341, (15.0, -100.0), "IMERG Late", gt._TIER_LATE)


def test_new_value_place_or_tier_is_a_change(clean_registry):
    sid = "EP982026"
    gt._set_rainfall(sid, "Test", 2026, 10.0, (15.0, -100.0), "IMERG Late", gt._TIER_LATE)
    assert gt._set_rainfall(sid, "Test", 2026, 11.0, (15.0, -100.0), "IMERG Late", gt._TIER_LATE)
    assert gt._set_rainfall(sid, "Test", 2026, 11.0, (16.0, -101.0), "IMERG Late", gt._TIER_LATE)
    assert gt._set_rainfall(sid, "Test", 2026, 11.0, (16.0, -101.0), "IMERG Final", gt._TIER_FINAL)


def test_settled_storm_skips_imerg_fetch(monkeypatch):
    import api.routes as routes
    import services.imerg_rainfall as imerg
    import services.atcf_bdeck_client as bdeck_mod

    monkeypatch.setenv("IMERG_LIVE_INGEST", "1")
    monkeypatch.setattr(imerg, "imerg_available", lambda: True)
    monkeypatch.setattr(imerg, "observed_rainfall_for_track",
                        lambda *a, **k: pytest.fail("settled storm must not be re-fetched"))
    monkeypatch.setattr(routes, "_load_current_season_storms",
                        lambda: [{"id": "EP012026", "name": "Amanda", "year": 2026}])
    monkeypatch.setattr(gt, "has_live_rainfall", lambda sid: True)

    ended = datetime.now(timezone.utc) - timedelta(days=90)

    class _Snap:
        def __init__(self, h):
            self.timestamp, self.lat, self.lon = ended - timedelta(hours=h), 12.0, -100.0

    class _FakeBdeck:
        def __init__(self, *a, **k): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def get_storm_track(self, sid): return [_Snap(12), _Snap(6), _Snap(0)]

    monkeypatch.setattr(bdeck_mod, "ATCFBDeckClient", _FakeBdeck)

    class _Http:
        async def aclose(self): pass

    out = asyncio.run(routes.refresh_current_season_rainfall(_Http()))
    assert out["rainfall_skipped_settled"] == 1
    assert out["rainfall_recorded"] == 0


def test_recent_storm_still_fetched(monkeypatch):
    import api.routes as routes
    import services.imerg_rainfall as imerg
    import services.atcf_bdeck_client as bdeck_mod

    calls = []

    async def _inline(fn, *a, **k):          # the child process can't see monkeypatches
        return fn(*a, **k)

    monkeypatch.setattr(routes, "_run_isolated", _inline)
    monkeypatch.setenv("IMERG_LIVE_INGEST", "1")
    monkeypatch.setattr(imerg, "imerg_available", lambda: True)
    monkeypatch.setattr(imerg, "observed_rainfall_for_track",
                        lambda track, **k: calls.append(len(track)) or None)
    monkeypatch.setattr(routes, "_load_current_season_storms",
                        lambda: [{"id": "EP172026", "name": "Polo", "year": 2026}])
    monkeypatch.setattr(gt, "has_live_rainfall", lambda sid: True)
    now = datetime.now(timezone.utc)

    class _Snap:
        def __init__(self, h):
            self.timestamp, self.lat, self.lon = now - timedelta(hours=h), 15.0, -102.0

    class _FakeBdeck:
        def __init__(self, *a, **k): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def get_storm_track(self, sid): return [_Snap(12), _Snap(6), _Snap(0)]

    monkeypatch.setattr(bdeck_mod, "ATCFBDeckClient", _FakeBdeck)

    class _Http:
        async def aclose(self): pass

    out = asyncio.run(routes.refresh_current_season_rainfall(_Http()))
    assert calls == [3] and out["rainfall_skipped_settled"] == 0


def test_isolated_call_runs_in_a_separate_process():
    # The IMERG step runs in a throwaway child so the libraries' native memory
    # is returned to the OS; prove the call really leaves this process.
    import os
    import api.routes as routes
    child_pid = asyncio.run(routes._run_isolated(os.getpid))
    assert isinstance(child_pid, int) and child_pid != os.getpid()
