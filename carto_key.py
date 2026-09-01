"""Inject the CARTO basemap API key into served HTML.

CARTO's raster basemaps (basemaps.cartocdn.com) now require an API key —
keyless tile requests get served an "API KEY REQUIRED" watermark tile
instead of map imagery. The fix (per carto.com/basemaps/apikey) is to
append the key as a ``?key=`` query parameter on every raster tile URL.

The key lives in the ``CARTO_BASEMAP_KEY`` environment variable (set on
Railway), NOT in the repo: the repo is public, CARTO's terms ask that keys
not be shared or reused across projects, and a committed key would be
harvested by scrapers within hours (it would also trip the secret-scan
pre-commit hook). The frontend source keeps the clean keyless URLs; every
HTML-serving route passes its payload through :func:`inject_carto_key` so
the key exists only in the deployed environment.

Fail-open by design: with no key set (local dev, mis-deploy) the HTML is
returned unchanged and the map still renders — watermarked, not broken.

This module must stay dependency-free (no fastapi imports) so the test
suite can exercise it on machines without the server stack installed.
"""
import os
import re

# A raster tile URL as it appears in the frontend source: Leaflet's literal
# {s}/{z}/{x}/{y}{r} placeholders, or explicit a-d subdomains (maplibre
# configs list the four hosts out). In-source these URLs never carry a query
# string, so appending is safe; the trailing lookahead guards against
# double-appending if one ever gains a ?key upstream. Non-tile CARTO URLs
# (preconnect/dns-prefetch links, the vector style.json) don't end in .png
# and are untouched.
_TILE_URL_RE = re.compile(
    r"(https://(?:\{s\}|[a-d])\.basemaps\.cartocdn\.com/[^\s'\"?]+\.png)(?!\?)")

# Keys are opaque tokens (e.g. cb1_...). Restrict to characters that are
# inert in both URLs and HTML so a malformed env value can never inject
# markup into the served page — worst case we fall back to keyless.
_KEY_OK_RE = re.compile(r"^[A-Za-z0-9_-]{8,128}$")


def inject_carto_key(html: str, key: str | None = None) -> str:
    """Append ``?key=<key>`` to every CARTO raster tile URL in *html*.

    ``key`` defaults to the ``CARTO_BASEMAP_KEY`` env var. Returns *html*
    unchanged when there is no (valid) key or nothing to rewrite.
    """
    if key is None:
        key = os.environ.get("CARTO_BASEMAP_KEY", "")
    key = (key or "").strip()
    if not html or not _KEY_OK_RE.match(key):
        return html
    return _TILE_URL_RE.sub(lambda m: f"{m.group(1)}?key={key}", html)
