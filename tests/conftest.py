"""Pytest configuration for the StormDPS test suite.

The default suite is fully offline and deterministic (syntax + bundle checks).
The validator scripts (validate_*/audit_*/verify_*) are marked `integration`
and skipped unless you pass --run-integration, because they may hit the network
or need cached data files.
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Mirror how the app runs: project root importable.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run integration tests that execute the validate_*/audit_* scripts "
        "(may require network access or cached data files)",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: end-to-end validator scripts; needs data/network. "
        "Opt in with --run-integration.",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration"):
        return
    skip_integration = pytest.mark.skip(reason="needs --run-integration")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
