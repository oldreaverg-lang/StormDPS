"""The DPS cache has ONE entry per storm, whatever the id's capitalization.

2026-10-09: the hourly live refresh wrote "al092026_<ver>.json" (the NHC
feed's spelling) while the storm page asked for "AL092026" and got its own
never-refreshed file: Hurricane Isaias's page showed DPS 13 "Low" as a
105 kt Cat 3. Linux volumes are case-sensitive, so both files existed.
"""
import json
import os

import pytest

import api.routes as routes


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(routes, "_DPS_CACHE_DIR", tmp_path)
    monkeypatch.setattr(routes, "_ACTIVE_DPS_MEMO", {})
    return tmp_path


def _write(fp, bundle, mtime):
    fp.write_text(json.dumps(bundle), encoding="utf-8")
    os.utime(fp, (mtime, mtime))


def _case_sensitive(d) -> bool:
    (d / "probe_a").write_text("x")
    twin = (d / "PROBE_A").exists()
    (d / "probe_a").unlink()
    return not twin


def test_every_spelling_maps_to_one_file(cache_dir):
    paths = {routes._dps_cache_path(s) for s in ("al092026", "AL092026", "Al092026")}
    assert len(paths) == 1 and paths.pop().name.startswith("AL092026_")


def test_save_then_load_under_any_spelling(cache_dir):
    routes._save_dps_cache("al092026", {"dps": 55.2})              # the hourly loop's spelling
    assert routes._load_dps_cache("AL092026") == {"dps": 55.2}     # the storm page's spelling
    assert routes._load_dps_cache("al092026") == {"dps": 55.2}
    assert routes._active_dps("AL092026") == 55.2


def test_newest_file_wins_during_the_changeover(cache_dir, monkeypatch):
    # Simulate the legacy twin with a distinct filename so this also runs on
    # case-insensitive filesystems (Windows dev machines).
    legacy = cache_dir / f"legacytwin_{routes._DPS_CACHE_VERSION}.json"
    monkeypatch.setattr(routes, "_dps_cache_legacy_path", lambda sid: legacy)
    canon = routes._dps_cache_path("AL092026")
    _write(canon, {"dps": 14.2}, 1_000)                             # page's stale first-view copy
    _write(legacy, {"dps": 55.2}, 2_000)                            # hourly loop's fresh copy
    assert routes._load_dps_cache("AL092026")["dps"] == 55.2
    assert routes._active_dps("al092026") == 55.2
    assert routes._dps_cache_file("AL092026") == legacy

    _write(canon, {"dps": 69.2}, 3_000)                             # first post-deploy refresh
    assert routes._load_dps_cache("al092026")["dps"] == 69.2
    assert routes._active_dps("al092026") == 69.2                   # memo follows the newer file


def test_invalidation_is_a_miss_never_a_fallback(cache_dir, monkeypatch):
    legacy = cache_dir / f"legacytwin_{routes._DPS_CACHE_VERSION}.json"
    monkeypatch.setattr(routes, "_dps_cache_legacy_path", lambda sid: legacy)
    _write(legacy, {"dps": 55.2}, 1_000)
    _write(routes._dps_cache_path("AL092026"), {"dps": 69.2}, 2_000)
    routes._invalidate_dps_cache("al092026")
    assert routes._load_dps_cache("AL092026") is None
    assert routes._active_dps("AL092026") is None


def test_legacy_only_storm_counts_as_cached(cache_dir, monkeypatch):
    # A dead current-season storm that only has the old lower-case file must
    # not be recomputed by the non-forced warm pass after the deploy.
    legacy = cache_dir / f"legacytwin_{routes._DPS_CACHE_VERSION}.json"
    monkeypatch.setattr(routes, "_dps_cache_legacy_path", lambda sid: legacy)
    _write(legacy, {"dps": 33.0}, 1_000)
    assert routes._dps_cache_file("al012026") == legacy
    assert routes._load_dps_cache("AL012026") == {"dps": 33.0}


def test_real_case_twins_on_a_case_sensitive_volume(cache_dir):
    if not _case_sensitive(cache_dir):
        pytest.skip("filesystem is case-insensitive; covered by the simulated-twin tests")
    ver = routes._DPS_CACHE_VERSION
    _write(cache_dir / f"AL092026_{ver}.json", {"dps": 14.2, "dps_label": "Low"}, 1_000)
    _write(cache_dir / f"al092026_{ver}.json", {"dps": 55.2, "dps_label": "Severe"}, 2_000)
    assert routes._load_dps_cache("AL092026")["dps"] == 55.2
    assert routes._dps_cache_scores() == {"AL092026": {"dps": 55.2, "dps_label": "Severe"}}
    routes._save_dps_cache("al092026", {"dps": 69.2, "dps_label": "Extreme"})
    os.utime(cache_dir / f"AL092026_{ver}.json", (3_000, 3_000))
    assert routes._load_dps_cache("al092026")["dps"] == 69.2
    assert routes._dps_cache_scores()["AL092026"]["dps"] == 69.2


def test_score_scan_keys_are_upper_case(cache_dir):
    routes._save_dps_cache("ep182026", {"dps": 67.2, "dps_label": "Extreme"})
    routes._save_dps_cache("WP272026", {"dps": 16.0, "dps_label": "Low"})
    assert routes._dps_cache_scores() == {
        "EP182026": {"dps": 67.2, "dps_label": "Extreme"},
        "WP272026": {"dps": 16.0, "dps_label": "Low"}}
