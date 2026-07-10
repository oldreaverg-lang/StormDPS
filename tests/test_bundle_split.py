"""Offline guards for the bundle-diet artifacts (scripts/build_bundle_split.py).

The split (frontend/bundle_index.json + frontend/bundle_storm/*.json) is
DERIVED from compiled_bundle.json at bake time. These tests fail the suite
when someone rebakes the monolith and forgets to rerun the split — the
exact drift class that bit dps_scores.json cross-site.
"""
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BUNDLE = ROOT / "frontend" / "compiled_bundle.json"
INDEX = ROOT / "frontend" / "bundle_index.json"
DETAIL_DIR = ROOT / "frontend" / "bundle_storm"

from scripts.build_bundle_split import HEAVY_FIELDS  # noqa: E402


def _load():
    bundle = json.loads(BUNDLE.read_text(encoding="utf-8"))
    index = json.loads(INDEX.read_text(encoding="utf-8"))
    return bundle, index


def test_index_matches_bundle_exactly():
    bundle, index = _load()
    storms = bundle["storms"]
    assert index.get("split") == 1
    assert set(index["storms"]) == set(storms), "index storm set drifted from bundle"
    expected_ids = sorted(set(storms) | set(bundle.get("raw_snapshots", {})))
    assert index["detail_ids"] == expected_ids, "detail_ids drifted from bundle"
    for sid, entry in storms.items():
        light = index["storms"][sid]
        assert not any(k in light for k in HEAVY_FIELDS), f"{sid}: heavy field leaked into index"
        for k, v in entry.items():
            if k not in HEAVY_FIELDS:
                assert light.get(k) == v, f"{sid}.{k} stale in index — rerun build_bundle_split.py"


def test_detail_files_match_bundle():
    bundle, index = _load()
    files = {p.stem for p in DETAIL_DIR.glob("*.json")}
    assert files == set(index["detail_ids"]), "detail files out of sync with detail_ids"
    # spot-check a heavy-field storm end to end (Ike: landfalls + snapshots)
    ike = json.loads((DETAIL_DIR / "AL092008.json").read_text(encoding="utf-8"))
    assert ike["storm"] == bundle["storms"]["AL092008"]
    assert ike["snapshots"] == bundle["raw_snapshots"]["AL092008"]
    assert ike["storm"].get("landfalls"), "detail must carry the heavy fields"
    assert len(ike["snapshots"]) > 10


def test_index_is_actually_slim():
    _, index = _load()
    index_bytes = len(json.dumps(index, separators=(",", ":")))
    bundle_bytes = BUNDLE.stat().st_size
    assert index_bytes < bundle_bytes * 0.10, (
        f"index is {index_bytes/1e3:.0f} KB — no longer slim vs the "
        f"{bundle_bytes/1e6:.1f} MB monolith; diet regressed")
