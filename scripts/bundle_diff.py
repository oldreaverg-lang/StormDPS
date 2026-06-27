#!/usr/bin/env python3
"""Detect SUBSTANTIVE changes between the working compiled bundle and the one
committed at HEAD — i.e. changes a user would actually see, ignoring cosmetic
churn like the `compiled_at` timestamp.

Used by the auto-rebake workflow to decide whether to open a PR: a weekly bake
re-stamps the timestamp every time, but we only want a PR when scores, storm set,
rainfall, or FEMA impact genuinely moved (e.g. late disaster declarations landed).

Exit 0 = substantive changes found (prints them); exit 1 = nothing meaningful.
"""
import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
NUM_FIELDS = ("dps", "rainfall_est_mm", "observed_rainfall_in")
STR_FIELDS = ("category", "rainfall_level")


def _committed_storms() -> dict:
    r = subprocess.run(
        ["git", "show", "HEAD:frontend/compiled_bundle.json"],
        cwd=str(ROOT), capture_output=True, text=True,
    )
    if r.returncode != 0:
        return {}
    try:
        return json.loads(r.stdout).get("storms", {})
    except json.JSONDecodeError:
        return {}


def main() -> int:
    new = json.loads((ROOT / "frontend" / "compiled_bundle.json").read_text(encoding="utf-8")).get("storms", {})
    old = _committed_storms()
    changes = []

    for sid in sorted(set(new) - set(old)):
        changes.append(f"added storm {sid} ({new[sid].get('name')})")
    for sid in sorted(set(old) - set(new)):
        changes.append(f"removed storm {sid} ({old[sid].get('name')})")

    for sid in sorted(set(new) & set(old)):
        a, b = old[sid], new[sid]
        for f in NUM_FIELDS:
            x, y = a.get(f), b.get(f)
            if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                if abs(x - y) > 0.5:
                    changes.append(f"{sid} {f}: {round(x, 1)} -> {round(y, 1)}")
            elif x != y:
                changes.append(f"{sid} {f}: {x} -> {y}")
        for f in STR_FIELDS:
            if a.get(f) != b.get(f):
                changes.append(f"{sid} {f}: {a.get(f)} -> {b.get(f)}")
        if (a.get("actual_impact") or {}) != (b.get("actual_impact") or {}):
            oc = (a.get("actual_impact") or {}).get("counties_declared")
            nc = (b.get("actual_impact") or {}).get("counties_declared")
            changes.append(f"{sid} actual_impact changed (counties {oc} -> {nc})")

    if changes:
        print(f"SUBSTANTIVE changes: {len(changes)}")
        for c in changes[:60]:
            print(f"  {c}")
        return 0
    print("no substantive changes (cosmetic only)")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
