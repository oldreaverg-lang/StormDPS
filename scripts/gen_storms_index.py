#!/usr/bin/env python3
"""Generate frontend/storms.html — the full crawlable index of every storm page.

WHY THIS EXISTS
---------------
Google Search Console (2026-08) showed 12 of ~232 submitted URLs indexed, with
"Discovered - currently not indexed" and "Crawled - currently not indexed" both
at 0. The cause was structural, not quality: `/historic-storms` server-renders
links to only ~12 storms, and the tracker's storm list is JS-rendered, so ~210
of the 223 /storm/<id> pages were ORPHANS — reachable via sitemap.xml alone.
A sitemap is a discovery hint; internal links are what actually earn crawl
budget on a low-authority site.

This emits one static, JS-free page linking every storm in the compiled bundle,
grouped by season. Linked from the footer of every static page, it turns the
whole catalog into a crawlable graph.

    python scripts/gen_storms_index.py

Idempotent, deterministic (same bundle -> same bytes). Wired into
compile_cache.py's post-bake hooks so the index can never drift from the
catalog.
"""
import html
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
BUNDLE = ROOT / "frontend" / "compiled_bundle.json"
OUT = ROOT / "frontend" / "storms.html"

BASIN_LABEL = {
    "ATLANTIC": "Atlantic",
    "EASTERN_PACIFIC": "E. Pacific",
    "CENTRAL_PACIFIC": "C. Pacific",
    "WESTERN_PACIFIC": "W. Pacific",
    "NORTH_INDIAN": "N. Indian",
    "SOUTH_INDIAN": "S. Indian",
    "SOUTH_PACIFIC": "S. Pacific",
    "SOUTH_ATLANTIC": "S. Atlantic",
}


def _pill_class(dps: float) -> str:
    """Canonical bands (core/dpi.py categorize_dpi) -> existing CSS classes."""
    if dps >= 90:
        return "dps-historic"
    if dps >= 80:
        return "dps-catastrophic"
    if dps >= 40:
        return "dps-severe"
    return "dps-notable"


HEAD = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>All Storms — Complete Hurricane &amp; Typhoon Index | StormDPS</title>
    <meta name="description" content="Every tropical cyclone scored by StormDPS, listed by season with its Destructive Power Score. {n} storms across the Atlantic, Pacific, and Indian Ocean basins — each links to a full interactive analysis.">
    <meta name="author" content="StormDPS">
    <link rel="canonical" href="https://stormdps.com/storms">

    <meta property="og:type" content="website">
    <meta property="og:title" content="All Storms — Complete Hurricane &amp; Typhoon Index">
    <meta property="og:description" content="Every tropical cyclone scored by StormDPS, listed by season with its Destructive Power Score.">
    <meta property="og:url" content="https://stormdps.com/storms">
    <meta property="og:image" content="https://stormdps.com/frontend/logo-512.png">
    <meta property="og:site_name" content="StormDPS">
    <meta name="twitter:card" content="summary">

    <script type="application/ld+json">
    {{
        "@context": "https://schema.org",
        "@type": "CollectionPage",
        "name": "All Storms — Complete Hurricane & Typhoon Index",
        "description": "Every tropical cyclone scored by the StormDPS Destructive Power Score, listed by season.",
        "url": "https://stormdps.com/storms",
        "isPartOf": {{"@type": "WebSite", "name": "StormDPS", "url": "https://stormdps.com"}},
        "publisher": {{"@type": "Organization", "name": "StormDPS", "url": "https://stormdps.com"}}
    }}
    </script>
    <script type="application/ld+json">
    {{
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {{"@type": "ListItem", "position": 1, "name": "StormDPS", "item": "https://stormdps.com/"}},
            {{"@type": "ListItem", "position": 2, "name": "All Storms", "item": "https://stormdps.com/storms"}}
        ]
    }}
    </script>

    <link rel="icon" type="image/x-icon" href="/frontend/favicon.ico">
    <link rel="icon" type="image/png" sizes="32x32" href="/frontend/logo-32.png">
    <meta name="theme-color" content="#eceef0">
    <link rel="preload" as="font" type="font/woff2" href="/frontend/fonts/public-sans-latin.woff2" crossorigin>
    <style>@font-face{{font-family:'Public Sans';font-style:normal;font-weight:100 900;font-display:optional;src:url('/frontend/fonts/public-sans-latin.woff2') format('woff2');unicode-range:U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD}}
        :root {{
            --primary:#205493; --bg-dark:#eceef0; --bg-card:#ffffff; --bg-input:#f7f8fa;
            --text-main:#1b1f24; --text-muted:#404b57; --text-dim:#5c6873; --border:#d4d9de;
            --radius:10px;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family:'Public Sans',-apple-system,BlinkMacSystemFont,sans-serif;
               background:var(--bg-dark); color:var(--text-main); line-height:1.7; font-size:16px; }}
        header {{ background:rgba(255,255,255,.96); backdrop-filter:blur(12px); padding:1rem 1.5rem;
                 border-bottom:1px solid var(--border); display:flex; align-items:center;
                 justify-content:space-between; position:sticky; top:0; z-index:100; }}
        .logo {{ display:flex; align-items:center; gap:.6rem; color:var(--text-main); text-decoration:none; }}
        .logo-icon {{ width:32px; height:32px; border-radius:8px; overflow:hidden; }}
        .logo-icon img {{ width:100%; height:100%; object-fit:cover; }}
        .logo-text {{ font-size:1.1rem; font-weight:700; letter-spacing:-.03em; }}
        nav {{ display:flex; flex-wrap:wrap; gap:1.25rem; row-gap:.5rem; align-items:center; }}
        nav a {{ color:var(--text-muted); text-decoration:none; font-weight:600; font-size:.95rem; }}
        nav a:hover {{ color:var(--text-main); }}
        nav a.cta {{ color:var(--primary); }}
        main {{ max-width:980px; margin:0 auto; padding:3rem 1.5rem 6rem; }}
        h1.page-title {{ font-size:clamp(1.8rem,4vw,2.6rem); font-weight:800; letter-spacing:-.03em;
                        line-height:1.2; margin-bottom:.75rem; }}
        .lede {{ font-size:1.15rem; color:var(--text-muted); margin-bottom:1.5rem; }}
        a.inline {{ color:var(--primary); text-decoration:underline; text-decoration-color:rgba(32,84,147,.4); }}
        .jump {{ background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius);
                padding:.9rem 1.1rem; margin-bottom:2.5rem; font-size:.9rem; }}
        .jump b {{ display:block; margin-bottom:.4rem; color:var(--text-muted); font-size:.75rem;
                  text-transform:uppercase; letter-spacing:.05em; }}
        .jump a {{ color:var(--primary); text-decoration:none; font-weight:700;
                  font-variant-numeric:tabular-nums; margin-right:.85rem; }}
        .jump a:hover {{ text-decoration:underline; }}
        h2.season {{ font-size:1.4rem; font-weight:700; letter-spacing:-.02em; margin:2.5rem 0 .25rem;
                    scroll-margin-top:5rem; }}
        .season-meta {{ color:var(--text-dim); font-size:.85rem; margin-bottom:.8rem; }}
        .slist {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); gap:.35rem .9rem;
                 background:var(--bg-card); border:1px solid var(--border); border-radius:14px;
                 padding:1rem 1.2rem; }}
        .sitem {{ display:flex; align-items:baseline; gap:.55rem; padding:.3rem 0; }}
        .sitem a {{ color:var(--primary); text-decoration:none; font-weight:700; }}
        .sitem a:hover {{ text-decoration:underline; }}
        .sitem .bs {{ color:var(--text-dim); font-size:.8rem; }}
        .sitem .sp {{ margin-left:auto; }}
        .dps-pill {{ display:inline-flex; align-items:center; padding:2px 8px; border-radius:999px;
                    font-size:.78rem; font-weight:700; color:#fff; font-variant-numeric:tabular-nums; }}
        .dps-historic {{ background:#7c2d12; color:#fecaca; }}
        .dps-catastrophic {{ background:#b91c1c; color:#fff; }}
        .dps-severe {{ background:#d97706; color:#fff; }}
        .dps-notable {{ background:#047857; color:#fff; }}
        footer {{ padding:2rem 1.5rem; text-align:center; color:var(--text-dim); font-size:.85rem;
                 border-top:1px solid var(--border); }}
        footer a {{ color:var(--text-muted); text-decoration:none; margin:0 .6rem; }}
        footer a:hover {{ color:var(--text-main); }}
        @media (max-width:640px) {{
            header {{ flex-wrap:wrap; padding:.7rem 1rem; }}
            header .logo {{ width:100%; }}
            header nav {{ width:100%; margin-top:.55rem; gap:.35rem .95rem; }}
            header nav a {{ font-size:.88rem; }}
        }}
    </style>
</head>
<body>
    <header>
        <a href="/" class="logo">
            <span class="logo-icon"><img src="/frontend/logo-128.png" alt=""></span>
            <span class="logo-text">StormDPS</span>
        </a>
        <nav>
            <a href="/">Tracker</a>
            <a href="/methodology">Methodology</a>
            <a href="/historic-storms">Historic</a>
            <a href="/data">Data</a>
            <a href="/about">About</a>
            <a href="/surgedps" class="cta">SurgeDPS</a>
        </nav>
    </header>

    <main>
        <h1 class="page-title">All storms in the StormDPS catalog</h1>
        <p class="lede">Every one of the <strong>{n} tropical cyclones</strong> scored by the
            <a href="/methodology" class="inline">Destructive Power Score</a>, listed by season.
            Each links to a full interactive analysis — track, wind field, score components, and
            side-by-side comparison. Looking for the headline events instead? See the
            <a href="/historic-storms" class="inline">most destructive storms ranked</a>.</p>
"""

FOOT = """    </main>

    <footer>
        <a href="/">Tracker</a> ·
        <a href="/methodology">Methodology</a> ·
        <a href="/historic-storms">Historic</a> ·
        <a href="/storms">All storms</a> ·
        <a href="/data">Data</a> ·
        <a href="/faq">FAQ</a> ·
        <a href="/about">About</a> ·
        <a href="/commercial">Commercial</a> ·
        <a href="/privacy">Privacy</a> ·
        <a href="/surgedps">SurgeDPS</a>
        <p style="margin-top:.6rem;color:var(--text-dim)">StormDPS — open-data hurricane destructive-power scoring</p>
    </footer>
</body>
</html>
"""


def main() -> int:
    storms = json.loads(BUNDLE.read_text(encoding="utf-8"))["storms"]

    by_year: dict[int, list] = {}
    for sid, e in storms.items():
        by_year.setdefault(int(e.get("year") or 0), []).append((sid, e))

    years = sorted(by_year, reverse=True)
    parts = [HEAD.format(n=len(storms))]

    parts.append('        <div class="jump"><b>Jump to season</b>')
    parts.append("            " + " ".join(f'<a href="#s{y}">{y}</a>' for y in years))
    parts.append("        </div>\n")

    for y in years:
        entries = sorted(by_year[y], key=lambda kv: -(kv[1].get("dps") or 0))
        parts.append(f'        <h2 class="season" id="s{y}">{y} season</h2>')
        parts.append(f'        <p class="season-meta">{len(entries)} storm'
                     f'{"s" if len(entries) != 1 else ""} scored</p>')
        parts.append('        <div class="slist">')
        for sid, e in entries:
            # Band from the DISPLAYED (rounded) score, not the raw float, so a
            # storm shown as 80 can't wear the sub-80 colour (Beryl 79.5 -> 80).
            dps = round(e.get("dps") or 0)
            name = html.escape(str(e.get("name") or sid))
            basin = BASIN_LABEL.get(e.get("basin"), str(e.get("basin") or "").title() or "—")
            cat = e.get("category")
            cat_txt = f"Cat {cat}" if isinstance(cat, int) and cat > 0 else "TS/TD"
            parts.append(
                f'            <div class="sitem"><a href="/storm/{html.escape(sid)}">{name}</a>'
                f'<span class="bs">{basin} · {cat_txt}</span>'
                f'<span class="sp dps-pill {_pill_class(dps)}">{dps}</span></div>')
        parts.append("        </div>")

    parts.append("")
    parts.append(FOOT)
    OUT.write_text("\n".join(parts), encoding="utf-8")
    print(f"[storms-index] wrote {len(storms)} storm links across {len(years)} seasons to {OUT.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
