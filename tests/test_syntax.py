"""Byte-compile every project Python file.

This is the cheapest, highest-value regression guard for this repo: it has
repeatedly been hit by an NTFS-mount truncation bug that silently cut files
mid-function, producing syntax errors that only surfaced at deploy time
(routes.py, noaa_client.py, atcf_bdeck_client.py, main.py). A failing case
here means a file is truncated or otherwise unparseable.

No third-party dependencies are needed: py_compile checks syntax without
importing modules, so it runs green even without the runtime requirements.
"""
import py_compile
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Directories that are not first-party Python we maintain.
EXCLUDE_DIRS = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    "node_modules",
    "frontend",  # JS/CSS build + data, no first-party .py
    "mobile",    # Expo/React Native (JS/TS)
}


def _iter_py_files():
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT)
        if any(part in EXCLUDE_DIRS for part in rel.parts):
            continue
        yield path


PY_FILES = sorted(_iter_py_files())


def test_found_python_files():
    """Sanity: the collector actually found the project's modules."""
    assert len(PY_FILES) > 20, f"expected to find many .py files, found {len(PY_FILES)}"


@pytest.mark.parametrize("path", PY_FILES, ids=[str(p.relative_to(ROOT)) for p in PY_FILES])
def test_py_file_compiles(path):
    py_compile.compile(str(path), doraise=True)
