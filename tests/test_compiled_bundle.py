"""Structural invariants for frontend/compiled_bundle.json.

This is the baked dataset of ~200 historical storms with pre-computed DPS
scores that ships with the deploy. These checks are deterministic and offline;
they catch a corrupted or partially-written bundle before it reaches users.
"""
import json
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
BUNDLE = ROOT / "frontend" / "compiled_bundle.json"


@pytest.fixture(scope="module")
def bundle():
    assert BUNDLE.exists(), f"missing bundle: {BUNDLE}"
    return json.loads(BUNDLE.read_text(encoding="utf-8"))


def test_top_level_keys(bundle):
    for key in ("version", "compiled_at", "storm_count", "storms", "raw_snapshots"):
        assert key in bundle, f"missing top-level key: {key}"


def test_storms_is_nonempty_mapping(bundle):
    assert isinstance(bundle["storms"], dict)
    assert len(bundle["storms"]) > 0


def test_storm_count_matches_storms(bundle):
    assert bundle["storm_count"] == len(bundle["storms"]), (
        f"storm_count={bundle['storm_count']} but len(storms)={len(bundle['storms'])}"
    )


def test_every_storm_has_dps_in_range(bundle):
    """Every storm must carry a numeric DPS in [0, 100]."""
    bad = []
    for sid, storm in bundle["storms"].items():
        dps = storm.get("dps")
        if not isinstance(dps, (int, float)) or isinstance(dps, bool) or not (0.0 <= dps <= 100.0):
            bad.append((sid, dps))
    assert not bad, f"{len(bad)} storm(s) with invalid dps, e.g. {bad[:10]}"


def test_every_storm_has_identity_fields(bundle):
    """Each storm needs a name and a basin so the UI/SSR can render it."""
    missing = [
        sid
        for sid, storm in bundle["storms"].items()
        if not storm.get("name") or not storm.get("basin")
    ]
    assert not missing, f"{len(missing)} storm(s) missing name/basin, e.g. {missing[:10]}"
