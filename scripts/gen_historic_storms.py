#!/usr/bin/env python3
"""Refresh the DPS numbers on frontend/historic-storms.html from the bundle.

WHY THIS EXISTS
---------------
`frontend/historic-storms.html` is a hand-written SEO hub page (sitemap priority
0.9) whose score cells were hardcoded. `scripts/rebake.py` refreshed the
methodology validation section and sitemap.xml but never this page, so its
numbers drifted silently from the engine — by 2026-08 it published Helene 90 /
Maria 94 / Harvey 93 while the bundle said 80 / 85 / 84. Google had both this
page and the /storm/<id> pages indexed, so the site was serving two different
scores for the same storm.

WHAT IT DOES
------------
Rewrites, in place, every score cell that can be tied to a /storm/<id> link:
  * `<span class="dps-pill …">NN</span>`  (Top-10 table)
  * `<span class="sc">NN</span>`          (basin rows)
It also re-sorts and renumbers the Top-10 table (a pure score refresh would
otherwise leave the ranking non-monotonic), and re-derives each pill's band
class from the canonical bands in core/dpi.py.

Rows with no /storm/<id> link are "reference-only" storms that are not in the
compiled bundle (Haiyan, Mangkhut, Patricia, …). They CANNOT be refreshed from
the engine and are left untouched — the script lists them at the end so the
un-verifiable numbers stay visible rather than silently rotting.

    python scripts/gen_historic_storms.py

Idempotent. Wired into compile_cache.py's post-bake hooks alongside
gen_sitemap.py, so a re-bake can never leave this page stale again.
"""
import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
PAGE = ROOT / "frontend" / "historic-storms.html"
BUNDLE = ROOT / "frontend" / "compiled_bundle.json"

ID_RE = re.compile(r'href="/storm/([A-Za-z0-9]+)"')
PILL_RE = re.compile(r'(<span class="dps-pill )(dps-[a-z]+)(">)(\d+)(</span>)')
SC_RE = re.compile(r'(<span class="sc">)(\d+)(</span>)')
NUM_RE = re.compile(r'(<td class="num">)(\d+)(</td>)')
NOLINK_RE = re.compile(r'storm-nolink[^>]*>([^<]+)<')
LINKNAME_RE = re.compile(r'class="storm-link">([^<]+)<')


def _pill_class(dps: float) -> str:
    """Canonical band → existing CSS class (core/dpi.py categorize_dpi).

    Class names are legacy ('catastrophic' is the 80-90 'Devastating' band);
    only the thresholds decide what a reader actually sees.
    """
    if dps >= 90:
        return "dps-historic"
    if dps >= 80:
        return "dps-catastrophic"
    return "dps-severe"


def _score_in(line: str):
    m = PILL_RE.search(line)
    if m:
        return int(m.group(4))
    m = SC_RE.search(line)
    return int(m.group(2)) if m else None


def _label(line: str) -> str:
    m = LINKNAME_RE.search(line) or NOLINK_RE.search(line)
    return m.group(1).strip() if m else "?"


def main() -> int:
    storms = json.loads(BUNDLE.read_text(encoding="utf-8"))["storms"]
    lines = PAGE.read_text(encoding="utf-8").split("\n")

    updated, unchanged, unrefreshable, missing, linkable = [], 0, [], [], []
    exact = {}   # line index -> unrounded DPS, so ties sort by the real value

    # name+year -> dps, for spotting reference-only rows that ARE now in the
    # bundle (they should become real links, but that's an editorial change).
    by_name = {(str(e.get("name", "")).lower(), e.get("year")): (sid, e.get("dps") or 0)
               for sid, e in storms.items()}

    for i, line in enumerate(lines):
        if _score_in(line) is None:
            continue
        sid_m = ID_RE.search(line)
        if not sid_m:
            label, shown = _label(line), _score_in(line)
            unrefreshable.append((label, shown))
            yr = re.search(r"\b(19|20)\d{2}\b", line)
            key = (label.split(" (")[0].strip().lower(), int(yr.group(0)) if yr else None)
            if key in by_name:
                linkable.append((label, key[1], by_name[key][0], round(by_name[key][1])))
            continue
        sid = sid_m.group(1)
        entry = storms.get(sid)
        if not entry:
            missing.append((_label(line), sid))
            continue

        new = round(entry.get("dps") or 0)
        exact[i] = entry.get("dps") or 0
        old = _score_in(line)
        out = PILL_RE.sub(
            lambda m: f"{m.group(1)}{_pill_class(new)}{m.group(3)}{new}{m.group(5)}", line)
        out = SC_RE.sub(lambda m: f"{m.group(1)}{new}{m.group(3)}", out)
        lines[i] = out
        if old != new:
            updated.append((_label(line), old, new))
        else:
            unchanged += 1

    # ── Re-sort + renumber the Top-10 table ──
    # Refreshing scores in place would otherwise leave ranks non-monotonic
    # (e.g. Irma 91 sitting at #9 below Ike 89 at #7).
    rows = [i for i, l in enumerate(lines) if NUM_RE.search(l) and "<tr>" in l]
    if rows:
        # Sort on the unrounded score where we have it, so storms that display
        # the same integer still rank in true order (Irma 91.1 above Ian 91.0).
        block = sorted(rows, key=lambda i: -(exact.get(i, _score_in(lines[i]) or 0)))
        rebuilt = [lines[i] for i in block]
        for rank, (idx, row) in enumerate(zip(rows, rebuilt), start=1):
            lines[idx] = NUM_RE.sub(lambda m: f"{m.group(1)}{rank}{m.group(3)}", row)

    PAGE.write_text("\n".join(lines), encoding="utf-8")

    print(f"[historic-storms] {len(updated)} score(s) corrected, {unchanged} already current, "
          f"top-{len(rows)} table re-sorted")
    for name, old, new in updated:
        print(f"    {name:<12} {old:>3} -> {new:>3}  ({new - old:+d})")
    if missing:
        print("  !! linked but NOT in the bundle (broken link?):")
        for name, sid in missing:
            print(f"    {name} ({sid})")
    if unrefreshable:
        uniq = sorted(set(unrefreshable))
        print(f"  -- reference-only rows left as-is ({len(uniq)}; not in the bundle, "
              f"so not engine-verifiable):")
        for name, score in uniq:
            print(f"    {name} = {score}")
    if linkable:
        print("  ** these reference-only rows ARE in the bundle now — convert them to "
              "/storm/<id> links (editorial change, not done automatically):")
        for name, year, sid, dps in sorted(set(linkable)):
            print(f"    {name} ({year}) -> /storm/{sid}  (DPS {dps})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
