#!/usr/bin/env python3
"""One-command reliable historical re-bake.

Runs, in order, every step a scoring/ground-truth change needs so the historical
bundle stays consistent and actually reaches users — the ritual that used to be
manual and easy to half-do:

  1. compile_cache.py              rebake frontend/compiled_bundle.json (now
                                   dedup-safe + fail-closed on FEMA/names; also
                                   refreshes the methodology validation section
                                   and sitemap.xml via its own hooks)
  2. tests/gen_scoring_baseline.py re-freeze the golden-master baseline so the
                                   drift-lock test passes on the new scores
  3. bump frontend/sw.js CACHE_NAME so returning visitors actually get the new
                                   bundle (it's fetched cache-first, else stale)

It does NOT commit or push — it prints the exact commands so you review the diff
first. Commit with native git (the bundle is too big to push safely through the
NTFS mount). Run with the project's Python:

    python scripts/rebake.py
"""
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def _run(*args: str) -> None:
    print(f"\n=== {' '.join([sys.executable, *args])} ===", flush=True)
    if subprocess.run([sys.executable, *args], cwd=str(ROOT)).returncode != 0:
        sys.exit(f"step failed: {' '.join(args)} — aborting re-bake")


def _bump_sw() -> None:
    sw = ROOT / "frontend" / "sw.js"
    txt = sw.read_text(encoding="utf-8")
    m = re.search(r"const CACHE_NAME = 'stormdps-v(\d+)';", txt)
    if not m:
        print("[sw] CACHE_NAME pattern not found — bump frontend/sw.js by hand")
        return
    nxt = int(m.group(1)) + 1
    sw.write_text(txt.replace(m.group(0), f"const CACHE_NAME = 'stormdps-v{nxt}';"),
                  encoding="utf-8")
    print(f"[sw] CACHE_NAME -> stormdps-v{nxt}")


def _bump_bundle_version() -> None:
    """Bump frontend/index.html BUNDLE_VERSION so the versioned bundle URL changes
    and its immutable cache can't serve a stale copy after the bake."""
    idx = ROOT / "frontend" / "index.html"
    txt = idx.read_text(encoding="utf-8")
    m = re.search(r"const BUNDLE_VERSION = (\d+);", txt)
    if not m:
        print("[index] BUNDLE_VERSION pattern not found — bump frontend/index.html by hand")
        return
    nxt = int(m.group(1)) + 1
    idx.write_text(txt.replace(m.group(0), f"const BUNDLE_VERSION = {nxt};"), encoding="utf-8")
    print(f"[index] BUNDLE_VERSION -> {nxt}")


def main() -> int:
    _run("compile_cache.py")
    _run("tests/gen_scoring_baseline.py")
    _bump_sw()
    _bump_bundle_version()
    print("\n" + "=" * 64)
    print("Re-bake complete. Review the diff, then commit with NATIVE git:")
    print("  git add frontend/compiled_bundle.json frontend/methodology.html \\")
    print("          frontend/sitemap.xml tests/data/scoring_baseline.json \\")
    print("          frontend/sw.js frontend/index.html")
    print('  git commit -m "Rebake compiled DPS bundle"')
    print("  git push origin main")
    print("=" * 64)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
