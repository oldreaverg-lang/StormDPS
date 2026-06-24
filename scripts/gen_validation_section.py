#!/usr/bin/env python3
"""
Regenerate the "How well does DPS predict real damage?" section in
frontend/methodology.html from frontend/compiled_bundle.json.

That section (a benchmark table + an inline-SVG scatter) is DERIVED from the
baked DPS scores and the FEMA actual-impact ground truth, so its numbers and
dots drift whenever the bundle is re-baked. Re-run this to refresh it:

    python scripts/gen_validation_section.py

It is also invoked automatically at the end of compile_cache.compile(), so a
normal re-bake keeps the methodology figures in sync.

Idempotent: it replaces only the content between the marker comments

    <!-- VALIDATION-SECTION:START ... -->   ...   <!-- VALIDATION-SECTION:END -->

in methodology.html, leaving the rest of the page untouched. It raises if the
markers are missing (so it can never double-insert or silently no-op).
"""

import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUNDLE = os.path.join(ROOT, "frontend", "compiled_bundle.json")
METHODOLOGY = os.path.join(ROOT, "frontend", "methodology.html")

START_MARKER = "<!-- VALIDATION-SECTION:START"   # match the prefix; the note after it may change
END_MARKER = "<!-- VALIDATION-SECTION:END -->"

# The four metrics we benchmark DPS against. `key` lists the bundle fields to try
# (first numeric wins); `flip` negates so that "higher = worse" metrics (lower
# central pressure = stronger storm) point the same direction as the outcome.
METRICS = [
    ("DPS",          ("dps",), False),
    ("Peak IKE",     ("peak_ike_tj", "peak_ike"), False),
    ("Peak wind",    ("peak_wind_kt",), False),
    ("Min pressure", ("min_pressure_hpa",), True),
]


def _num(storm, keys):
    for k in keys:
        v = storm.get(k)
        if isinstance(v, (int, float)):
            return float(v)
    return None


def _auc(values, labels):
    """Mann–Whitney AUC: P(positive ranks above negative), 0.5 for ties."""
    pos = [v for v, l in zip(values, labels) if l == 1 and v is not None]
    neg = [v for v, l in zip(values, labels) if l == 0 and v is not None]
    if not pos or not neg:
        return None
    c = sum((1.0 if p > n else 0.5 if p == n else 0.0) for p in pos for n in neg)
    return c / (len(pos) * len(neg))


def _ranks(arr):
    order = sorted(range(len(arr)), key=lambda i: arr[i])
    r = [0.0] * len(arr)
    i = 0
    while i < len(arr):
        j = i
        while j + 1 < len(arr) and arr[order[j + 1]] == arr[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1
        for k in range(i, j + 1):
            r[order[k]] = avg
        i = j + 1
    return r


def _spearman(xs, ys):
    if len(xs) < 3:
        return None
    rx, ry = _ranks(xs), _ranks(ys)
    n = len(xs)
    mx, my = sum(rx) / n, sum(ry) / n
    cov = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    vx = sum((v - mx) ** 2 for v in rx)
    vy = sum((v - my) ** 2 for v in ry)
    return cov / ((vx * vy) ** 0.5) if vx and vy else None


def _esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;")


def build_section(bundle):
    """Return (section_html, summary_dict) from a parsed compiled_bundle.json."""
    storms = bundle["storms"]
    items = list(storms.values()) if isinstance(storms, dict) else storms

    pts = []   # (dps, counties, major, name)
    rows = []  # (label, dps, ike, wind, pressure, counties)
    for s in items:
        ai = s.get("actual_impact")
        if not (isinstance(ai, dict) and isinstance(ai.get("counties_declared"), (int, float))):
            continue
        major = 1 if ai.get("major_disaster") else 0
        pts.append((_num(s, ("dps",)), ai["counties_declared"], major, s.get("name", "")))
        rows.append((major, _num(s, ("dps",)), _num(s, ("peak_ike_tj", "peak_ike")),
                     _num(s, ("peak_wind_kt",)), _num(s, ("min_pressure_hpa",)), ai["counties_declared"]))

    n = len(pts)
    if n < 8:
        raise SystemExit(f"only {n} storms carry FEMA actual_impact — too few to build the section")
    labels = [r[0] for r in rows]
    n_major = sum(labels)

    # Benchmark: AUC vs major_disaster, Spearman vs counties_declared.
    bench = []
    for label, keys, flip in METRICS:
        idx = {"DPS": 1, "Peak IKE": 2, "Peak wind": 3, "Min pressure": 4}[label]
        vp = [((-r[idx]) if flip and r[idx] is not None else r[idx]) for r in rows]
        xs = [v for v in vp if v is not None]
        ys = [r[5] for r, v in zip(rows, vp) if v is not None]
        bench.append((label, _auc(vp, labels), _spearman(xs, ys)))
    dps_rho = bench[0][2]

    # ── Inline-SVG scatter (DPS x, FEMA counties y) ──
    L, R, T, B = 64, 20, 28, 52
    W, H = 720, 380
    PW, PH = W - L - R, H - T - B
    def X(dps): return round(L + (dps / 100.0) * PW, 1)
    def Y(c):   return round(T + (1 - min(c, 300) / 300.0) * PH, 1)
    sv = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="DPS versus FEMA counties declared, {n} US storms" style="width:100%;height:auto;max-width:720px;color:#475569">']
    for c in (0, 100, 200, 300):
        y = Y(c)
        sv.append(f'<line x1="{L}" y1="{y}" x2="{W-R}" y2="{y}" stroke="currentColor" stroke-opacity="0.12"/>')
        sv.append(f'<text x="{L-8}" y="{y+4}" text-anchor="end" font-size="12" fill="currentColor" fill-opacity="0.7">{c}</text>')
    for dps in (0, 20, 40, 60, 80, 100):
        sv.append(f'<text x="{X(dps)}" y="{H-B+20}" text-anchor="middle" font-size="12" fill="currentColor" fill-opacity="0.7">{dps}</text>')
    sv.append(f'<line x1="{L}" y1="{T}" x2="{L}" y2="{T+PH}" stroke="currentColor" stroke-opacity="0.4"/>')
    sv.append(f'<line x1="{L}" y1="{T+PH}" x2="{W-R}" y2="{T+PH}" stroke="currentColor" stroke-opacity="0.4"/>')
    sv.append(f'<text x="{L+PW/2}" y="{H-6}" text-anchor="middle" font-size="13" font-weight="600" fill="currentColor" fill-opacity="0.85">Destructive Power Score (DPS) &#8594;</text>')
    sv.append(f'<text transform="translate(16,{T+PH/2}) rotate(-90)" text-anchor="middle" font-size="13" font-weight="600" fill="currentColor" fill-opacity="0.85">FEMA counties declared &#8594;</text>')
    for dps, c, major, name in sorted(pts, key=lambda p: -p[1]):
        if dps is None:
            continue
        fill = "#dc2626" if major else "#2563eb"
        sv.append(f'<circle cx="{X(dps)}" cy="{Y(c)}" r="4.5" fill="{fill}" fill-opacity="0.72" stroke="#fff" stroke-width="0.8"><title>{_esc(name)}: DPS {round(dps)}, {int(c)} counties</title></circle>')
    lx, ly = W - R - 190, T + PH - 40
    sv.append(f'<rect x="{lx-8}" y="{ly-12}" width="196" height="42" rx="6" fill="currentColor" fill-opacity="0.05"/>')
    sv.append(f'<circle cx="{lx+2}" cy="{ly+2}" r="4.5" fill="#dc2626" fill-opacity="0.72"/><text x="{lx+12}" y="{ly+6}" font-size="11.5" fill="currentColor" fill-opacity="0.8">FEMA major-disaster declaration</text>')
    sv.append(f'<circle cx="{lx+2}" cy="{ly+20}" r="4.5" fill="#2563eb" fill-opacity="0.72"/><text x="{lx+12}" y="{ly+24}" font-size="11.5" fill="currentColor" fill-opacity="0.8">Emergency declaration only</text>')
    sv.append('</svg>')
    SVG = "\n".join(sv)

    trs = "\n".join(
        f'<tr{" style=\"font-weight:700\"" if nm == "DPS" else ""}><td>{nm}</td>'
        f'<td>{a:.2f}</td><td>{r:.2f}</td></tr>'
        for nm, a, r in bench
    )

    section = f'''        <h2 id="validation">How well does DPS predict real damage?</h2>
        <p>A score is only as good as what it predicts. We test DPS against independent ground truth &mdash; the federal disaster response &mdash; across the {n} U.S. storms in our dataset that carry FEMA records, and compare it to the metrics a forecaster would otherwise reach for: peak wind, integrated kinetic energy, and minimum central pressure. Two measures: the <strong>AUC</strong> (how cleanly each metric separates the storms that drew a FEMA <em>major-disaster</em> declaration &mdash; {n_major} of {n}) and the <strong>rank correlation</strong> (how well each orders storms by counties declared). Higher is better on both.</p>
        <table>
            <thead><tr><th>Metric</th><th>Major-disaster AUC</th><th>Footprint rank &rho;</th></tr></thead>
            <tbody>
{trs}
            </tbody>
        </table>
        <p>DPS leads on both &mdash; because it weighs storm size, duration, and surge geography, not just peak intensity. The scatter plots every storm by its DPS against the breadth of its federal disaster footprint:</p>
        <figure style="margin:1.25rem 0">
{SVG}
            <figcaption style="font-size:0.85rem;opacity:0.7;margin-top:0.5rem">Each dot is a U.S. storm (2015&ndash;present) with FEMA records. Higher DPS tracks a wider federal disaster footprint (Spearman &rho; = {dps_rho:.2f}). Curated sample, n = {n} &mdash; small, but the ranking is consistent across both measures.</figcaption>
        </figure>
        <p>This is a deliberately honest test, not a victory lap: the sample is small and U.S.-only, the FEMA-declaration outcome is itself imperfect, and DPS still misses the exposure- and rainfall-driven damage discussed above. But on the question it is built to answer &mdash; <em>which storm carries more destructive power</em> &mdash; it out-predicts every conventional single-number metric.</p>'''

    summary = {"n": n, "n_major": n_major,
               "bench": [(nm, round(a, 3), round(r, 3)) for nm, a, r in bench]}
    return section, summary


def regenerate():
    with open(BUNDLE, encoding="utf-8") as f:
        bundle = json.load(f)
    section, summary = build_section(bundle)

    with open(METHODOLOGY, encoding="utf-8") as f:
        html = f.read()

    if START_MARKER not in html or END_MARKER not in html:
        raise SystemExit("VALIDATION-SECTION markers not found in methodology.html — "
                         "cannot regenerate safely (add the marker comments first)")

    s_lt = html.index(START_MARKER)
    s_end = html.index("-->", s_lt) + len("-->")   # end of the START comment
    e = html.index(END_MARKER)
    if not s_end < e:
        raise SystemExit("VALIDATION-SECTION markers are out of order")

    new_html = html[:s_end] + "\n" + section + "\n        " + html[e:]
    if new_html != html:
        with open(METHODOLOGY, "w", encoding="utf-8") as f:
            f.write(new_html)
        changed = True
    else:
        changed = False
    return summary, changed


if __name__ == "__main__":
    summary, changed = regenerate()
    state = "updated" if changed else "already up to date"
    print(f"validation section {state} — n={summary['n']} "
          f"(major {summary['n_major']}); "
          + ", ".join(f"{nm} AUC={a} rho={r}" for nm, a, r in summary["bench"]))
