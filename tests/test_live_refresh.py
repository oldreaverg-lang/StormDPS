"""Live storms refresh fast enough to follow a landfall.

2026-10-10, Hurricane Isaias at the Florida coast: the hourly refresh loop and
a 30-minute track cache meant a new best-track fix could take over an hour to
reach the score, and the page's track trailed the score by another 20-30
minutes because the two read differently-capitalised cache entries.
"""
import json
import os
import time

import pytest

import api.routes as routes
from core import refresh_cadence as rc


# ── which storms get the fast lane ──────────────────────────────────────────

def _dist(table):
    return lambda lat, lon: table[(lat, lon)]


def test_only_storms_near_land_get_the_fast_lane():
    rows = [
        {"id": "al092026", "lat": 30.1, "lon": -86.6},     # at the coast
        {"id": "ep182026", "lat": 23.6, "lon": -123.7},    # open Pacific
        {"id": "WP272026", "lat": 20.7, "lon": 153.0},
    ]
    dist = _dist({(30.1, -86.6): 12.0, (23.6, -123.7): 1400.0, (20.7, 153.0): 500.1})
    assert rc.fast_lane_ids(rows, dist) == ["al092026"]


def test_the_boundary_is_inclusive_and_configurable():
    rows = [{"id": "A", "lat": 1.0, "lon": 2.0}]
    assert rc.fast_lane_ids(rows, lambda la, lo: rc.FAST_LANE_KM) == ["A"]
    assert rc.fast_lane_ids(rows, lambda la, lo: 300.0, within_km=250.0) == []


def test_rows_that_cannot_be_placed_stay_on_the_hourly_pass():
    def boom(lat, lon):
        raise RuntimeError("coastline lookup failed")

    rows = [
        {"id": "", "lat": 30.0, "lon": -86.0},
        {"id": "NOPOS"},
        {"id": "BADPOS", "lat": "n/a", "lon": None},
        "not a row",
        None,
    ]
    assert rc.fast_lane_ids(rows, lambda la, lo: 0.0) == []
    assert rc.fast_lane_ids([{"id": "X", "lat": 1, "lon": 2}], boom) == []
    assert rc.fast_lane_ids([{"id": "X", "lat": 1, "lon": 2}], lambda la, lo: None) == []
    assert rc.fast_lane_ids(None, lambda la, lo: 0.0) == []


def test_full_pass_stays_hourly_between_fast_ticks():
    tick, full = rc.FAST_INTERVAL_S, rc.FULL_INTERVAL_S
    due = [n for n in range(1, 13) if rc.full_pass_due(n * tick, 0.0, full, tick)]
    assert due[0] == 6                                    # the 60-minute tick, not the 70-minute one
    # Timer jitter (a tick that lands a few seconds early) must not skip it.
    assert rc.full_pass_due(6 * tick - 20, 0.0, full, tick)
    assert not rc.full_pass_due(5 * tick + 20, 0.0, full, tick)


def test_the_fast_lane_is_faster_than_the_full_pass():
    assert rc.FAST_INTERVAL_S * 3 <= rc.FULL_INTERVAL_S


# ── one track-cache entry per storm ─────────────────────────────────────────

@pytest.fixture
def ike_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(routes, "_IKE_CACHE_DIR", tmp_path)
    return tmp_path


def _write(fp, sid, rows, mtime):
    fp.write_text(json.dumps({
        "_version": routes._IKE_CACHE_VERSION, "_storm_id": sid, "_source": "nhc_bdeck",
        "results": rows,
    }), encoding="utf-8")
    os.utime(fp, (mtime, mtime))


def test_every_spelling_maps_to_one_track_file(ike_dir):
    keys = {routes._ike_cache_key(s, 15.0, 0) for s in ("al092026", "AL092026", "Al092026")}
    assert len(keys) == 1 and keys.pop().startswith("AL092026_")
    # Different parameters are still different entries.
    assert routes._ike_cache_key("AL092026", 15.0, 0) != routes._ike_cache_key("AL092026", 15.0, 1)


def test_the_loop_and_the_page_share_the_entry(ike_dir):
    rows = [{"lat": 30.1, "lon": -86.6}]
    routes._save_ike_cache("al092026", 15.0, 0, rows, source="nhc_bdeck", compute_ms=1.0)   # refresh loop
    assert routes._load_ike_cache("AL092026", 15.0, 0, max_age_s=1800) == rows              # storm page
    assert routes._load_ike_cache("al092026", 15.0, 0) == rows
    assert routes._load_ike_cache_entry("AL092026", 15.0, 0)["_source"] == "nhc_bdeck"
    assert [f.name for f in ike_dir.glob("*.json")] == [routes._ike_cache_key("AL092026", 15.0, 0)]


def test_a_newer_legacy_twin_is_read_until_the_first_save(ike_dir):
    now = time.time()
    canon = ike_dir / routes._ike_cache_key("AL092026", 15.0, 0)
    legacy = ike_dir / routes._ike_cache_name("al092026", 15.0, 0)
    assert canon.name != legacy.name
    _write(canon, "AL092026", [{"n": "page copy, 40 min old"}], now - 2400)
    _write(legacy, "al092026", [{"n": "loop copy, 5 min old"}], now - 300)
    assert routes._ike_cache_file("AL092026", 15.0, 0) == legacy
    # The live TTL is judged on the file that is read: fresh, so a hit.
    assert routes._load_ike_cache("AL092026", 15.0, 0, max_age_s=1800) == [{"n": "loop copy, 5 min old"}]

    routes._save_ike_cache("al092026", 15.0, 0, [{"n": "new"}], source="nhc_bdeck", compute_ms=1.0)
    assert routes._ike_cache_file("al092026", 15.0, 0) == canon
    assert routes._load_ike_cache("AL092026", 15.0, 0, max_age_s=1800) == [{"n": "new"}]


def test_a_storm_with_only_a_legacy_file_is_not_recomputed(ike_dir):
    legacy = ike_dir / routes._ike_cache_name("al012026", 15.0, 0)
    _write(legacy, "al012026", [{"n": 1}], time.time() - 86400 * 30)
    assert routes._load_ike_cache("al012026", 15.0, 0) == [{"n": 1}]       # historical: no age limit
    assert routes._load_ike_cache("AL012026", 15.0, 0) == [{"n": 1}]


def test_stale_and_foreign_entries_are_misses(ike_dir):
    now = time.time()
    canon = ike_dir / routes._ike_cache_key("AL092026", 15.0, 0)
    _write(canon, "AL092026", [{"n": 1}], now - 2400)
    assert routes._load_ike_cache("al092026", 15.0, 0, max_age_s=1800) is None      # past the live TTL
    assert routes._load_ike_cache("al092026", 15.0, 0, max_age_s=6 * 3600) == [{"n": 1}]
    _write(canon, "AL102026", [{"n": 1}], now)                                      # another storm's payload
    assert routes._load_ike_cache("AL092026", 15.0, 0) is None
    assert routes._load_ike_cache_entry("AL092026", 15.0, 0) is None
    assert routes._load_ike_cache("EP012026", 15.0, 0, max_age_s=1800) is None      # nothing cached


# ── the settings themselves ─────────────────────────────────────────────────

def test_live_refresh_settings():
    from services import land_rain_client
    assert routes._OVERVIEW_TTL_S <= 120
    assert land_rain_client._TTL_S <= 600
    assert routes._ACTIVE_STORMS_TTL.total_seconds() <= 120
