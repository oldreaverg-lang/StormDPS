"""Every storm URL the sitemap submits must be indexable.

GSC flagged 14 sitemap URLs as "Excluded by 'noindex' tag": the SSR
noindexed every IBTrACS-SID URL on the assumption an ATCF twin was the
canonical, but non-NHC-basin storms are keyed BY SID in the bundle — the
SID page is their only scored record.
"""
import re
from pathlib import Path

import pytest

import seo

ROOT = Path(__file__).resolve().parent.parent
SITEMAP = ROOT / "frontend" / "sitemap.xml"
NOINDEX = re.compile(r'<meta name="robots" content="noindex', re.IGNORECASE)


def _sitemap_storm_ids():
    xml = SITEMAP.read_text(encoding="utf-8")
    return re.findall(r"/storm/([A-Za-z0-9_-]+)</loc>", xml)


@pytest.fixture(scope="module")
def storm_ids():
    ids = _sitemap_storm_ids()
    if not ids or not seo._read_index_template():
        pytest.skip("sitemap or index template not available")
    return ids


def test_sitemap_storm_urls_are_indexable(storm_ids):
    noindexed = [sid for sid in storm_ids if NOINDEX.search(seo.render_storm_page(sid))]
    assert not noindexed, f"sitemap lists noindexed storm pages: {noindexed}"


def test_sid_keyed_bundle_storms_are_in_sitemap(storm_ids):
    storms = seo._read_compiled_bundle().get("storms", {})
    sid_keys = {k for k in storms if seo._RE_IBTRACS_SID.match(k)}
    assert sid_keys, "expected some SID-keyed (non-NHC) bundle storms"
    assert sid_keys <= set(storm_ids)


def test_sid_duplicating_an_atcf_storm_stays_noindexed(storm_ids):
    # Katrina is bundle-keyed AL122005; its SID URL is a duplicate alias.
    assert "2005236N23285" not in seo._read_compiled_bundle().get("storms", {})
    assert NOINDEX.search(seo.render_storm_page("2005236N23285"))


def test_atcf_url_has_no_robots_tag(storm_ids):
    assert not NOINDEX.search(seo.render_storm_page("AL122005"))
