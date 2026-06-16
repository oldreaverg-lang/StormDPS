# SEO state

## Done (no further action needed)

- Path-based per-storm URLs `/storm/{ATCF_ID}` — server-rendered with unique title, meta description, canonical, OG/Twitter cards, Article + BreadcrumbList JSON-LD, H1 + DPS score card with prose.
- Long-form landing pages: `/methodology`, `/historic-storms`, `/faq`, `/data`, `/about`, `/privacy` — all with proper JSON-LD.
- Homepage Dataset JSON-LD with DOI identifier (for Google Dataset Search).
- Zenodo deposit live at https://doi.org/10.5281/zenodo.20149123.
- CITATION.cff so GitHub shows "Cite this repository" button.
- Sitemap.xml lists every URL with `<lastmod>`.
- Google Search Console + Bing Webmaster both verified.
- IBTrACS-pattern URLs get `noindex,follow` to avoid duplicate-content with ATCF URLs.
- Cloudflare: Email Address Obfuscation OFF, Browser Cache TTL respects headers, Auto Minify ON.

## Deferred (wait for first 2026 Atlantic named storm)

- Hacker News Show HN post
- Cold emails: Eye on the Tropics, Tropical Tidbits, Yale Climate Connections, Capital Weather Gang
- Twitter/X presence during active storms

Strategic decision: first named storm gives a news hook. Hurricane season starts June 1; first named storm typically mid-late June.

## Performance (last measured)

Mobile PageSpeed: **88** (up from 71 baseline). Most recent commit `724571e` async-loaded Leaflet CSS — expected to push to 91–93 but the verifying PageSpeed run hit ERR_TIMED_OUT. **Re-test after a warm Cloudflare cache hit.**

| Metric | Value |
|--------|-------|
| FCP | 3.0 s |
| LCP | 3.0 s |
| TBT | 0 ms |
| CLS | 0 |

Open wins: catalog cold-start (10.9 s on Railway warm-up — fix: defer IBTrACS warm in lifespan), WebP logo (23 KiB), Chart.js tree-shaking (62 KiB — requires build pipeline).
