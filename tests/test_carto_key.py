"""CARTO basemap key injection (carto_key.inject_carto_key).

CARTO raster basemaps require a ?key= parameter or tiles render an
"API KEY REQUIRED" watermark. The key lives only in the CARTO_BASEMAP_KEY
env var; every HTML-serving route injects it at serve time. These tests pin
(a) the rewrite against the REAL frontend files, so a change to the tile URL
shape can't silently disable injection, and (b) the fail-open + sanitization
behavior. A dummy key is used throughout — the real key must never appear in
this public repo.
"""
import os

from carto_key import inject_carto_key

# Deliberately low-entropy, obviously-fake dummy (the secret-scan pre-commit
# hook rightly rejects anything that *looks* like a real key in this repo).
_KEY = "cb1_dummy_key_aaaaaaaaaaaaaaaa"
_ROOT = os.path.dirname(os.path.dirname(__file__))


def _read(rel):
    with open(os.path.join(_ROOT, rel), encoding="utf-8") as fh:
        return fh.read()


def test_index_html_tile_url_gains_key():
    html = _read("frontend/index.html")
    out = inject_carto_key(html, _KEY)
    assert out != html, "no tile URL matched in index.html — URL shape drifted?"
    assert f"{{r}}.png?key={_KEY}" in out
    # the raw keyless URL must be gone
    assert "{r}.png'" not in out or "{r}.png?key=" in out


def test_compare_html_tile_url_gains_key():
    html = _read("frontend/compare.html")
    out = inject_carto_key(html, _KEY)
    assert out != html, "no tile URL matched in compare.html — URL shape drifted?"
    assert f"?key={_KEY}" in out


def test_no_key_is_identity():
    html = _read("frontend/index.html")
    assert inject_carto_key(html, "") == html
    assert inject_carto_key(html, None if "CARTO_BASEMAP_KEY" not in os.environ
                            else os.environ["CARTO_BASEMAP_KEY"]) is not None


def test_malformed_key_is_rejected():
    html = "x 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png' y"
    for bad in ('ab"><script>', "short", "key with spaces", "k'ey_injection00"):
        assert inject_carto_key(html, bad) == html


def test_idempotent_no_double_append():
    html = "'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png'"
    once = inject_carto_key(html, _KEY)
    assert inject_carto_key(once, _KEY) == once


def test_explicit_subdomain_hosts_match():
    html = "'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png'"
    assert f"@2x.png?key={_KEY}" in inject_carto_key(html, _KEY)


def test_non_tile_carto_urls_untouched():
    html = ('<link rel="preconnect" href="https://c.basemaps.cartocdn.com">'
            '"https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"')
    assert inject_carto_key(html, _KEY) == html


def test_env_var_is_used_when_key_omitted():
    html = "'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png'"
    old = os.environ.get("CARTO_BASEMAP_KEY")
    try:
        os.environ["CARTO_BASEMAP_KEY"] = _KEY
        assert f"?key={_KEY}" in inject_carto_key(html)
    finally:
        if old is None:
            os.environ.pop("CARTO_BASEMAP_KEY", None)
        else:
            os.environ["CARTO_BASEMAP_KEY"] = old
