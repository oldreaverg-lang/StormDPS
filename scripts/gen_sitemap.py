#!/usr/bin/env python3
"""Regenerate frontend/sitemap.xml from frontend/compiled_bundle.json.

The sitemap had been hand-maintained and listed only ~35 of the ~223 storms, so
the long tail of SSR'd /storm/<id> pages was invisible to search engines. This
emits every storm in the bundle, with crawl priority scaled by DPS (marquee
storms rank higher), plus the static landing pages.

    python scripts/gen_sitemap.py

Idempotent. Re-run after a re-bake (or wire into compile_cache like the validation
section). Storm pages are SSR'd by seo.py, so each <loc> is a real indexable page.
"""
import datetime
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
BASE = "https://stormdps.com"
TODAY = datetime.date.today().isoformat()

# Static landing pages — (path, changefreq, priority).
STATIC = [
    ("/", "daily", "1.0"),
    ("/methodology", "monthly", "0.9"),
    ("/data", "monthly", "0.9"),
    ("/historic-storms", "monthly", "0.9"),
    # Full crawlable storm index — the internal-link hub that gives every
    # /storm/<id> page a real inbound link (GSC orphan-page fix, 2026-08).
    ("/storms", "weekly", "0.9"),
    ("/faq", "monthly", "0.8"),
    ("/about", "monthly", "0.7"),
    ("/commercial", "monthly", "0.7"),
    ("/surgedps", "weekly", "0.8"),
    ("/privacy", "yearly", "0.4"),
]


def _priority(dps: float) -> str:
    # 0.4 (DPS 0) .. 0.8 (DPS 100). Marquee storms crawl first.
    return "%.1f" % round(0.4 + 0.4 * max(0.0, min(100.0, dps)) / 100.0, 1)


def main() -> int:
    bundle = json.loads((ROOT / "frontend" / "compiled_bundle.json").read_text(encoding="utf-8"))
    storms = bundle["storms"]

    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
             "",
             "    <!-- Landing pages -->"]
    for path, freq, pri in STATIC:
        lines += ["    <url>",
                  f"        <loc>{BASE}{path}</loc>",
                  f"        <lastmod>{TODAY}</lastmod>",
                  f"        <changefreq>{freq}</changefreq>",
                  f"        <priority>{pri}</priority>",
                  "    </url>"]

    lines += ["",
              f"    <!-- Storm pages ({len(storms)}), generated from compiled_bundle.json; each is SSR'd by seo.py -->"]
    # Highest-DPS first so the most valuable pages lead the sitemap.
    ranked = sorted(storms.items(), key=lambda kv: -(kv[1].get("dps") or 0))
    for sid, e in ranked:
        dps = e.get("dps") or 0
        name = str(e.get("name") or sid).replace("--", "-")  # '--' would close the XML comment
        lines.append(
            f'    <url><loc>{BASE}/storm/{sid}</loc><lastmod>{TODAY}</lastmod>'
            f'<changefreq>monthly</changefreq><priority>{_priority(dps)}</priority></url>'
            f' <!-- {name} (DPS {round(dps)}) -->'
        )

    lines += ["", "</urlset>", ""]
    out = ROOT / "frontend" / "sitemap.xml"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {len(storms)} storm URLs + {len(STATIC)} static pages to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
