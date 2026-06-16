"""Run the repository's standalone validator scripts as an integration suite.

The project ships eight hand-written checks (validate_*/audit_*/verify_*.py)
that exercise the scoring formulas against ground truth. They are real, but
some need cached data or network access, so they are marked `integration`
and skipped by default. Run them with:

    pytest tests/test_validators.py --run-integration
"""
import subprocess
import sys
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent


def _discover_validators():
    names = set()
    for pattern in ("validate_*.py", "audit_*.py", "verify_*.py"):
        for path in ROOT.glob(pattern):
            names.add(path.name)
    return sorted(names)


VALIDATORS = _discover_validators()


def test_validators_discovered():
    """The validator scripts should exist at the repo root."""
    assert VALIDATORS, "no validate_*/audit_*/verify_*.py scripts found at repo root"


@pytest.mark.integration
@pytest.mark.parametrize("script", VALIDATORS)
def test_validator_exits_clean(script):
    result = subprocess.run(
        [sys.executable, script],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert result.returncode == 0, (
        f"{script} exited {result.returncode}\n"
        f"--- stdout (tail) ---\n{result.stdout[-2000:]}\n"
        f"--- stderr (tail) ---\n{result.stderr[-2000:]}"
    )
