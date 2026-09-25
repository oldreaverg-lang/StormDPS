"""Homepage served overview-first (home_shell.py, 2026-09-25)."""
import pathlib
import re

import home_shell as hs

INDEX = (pathlib.Path(__file__).resolve().parents[1] / "frontend" / "index.html").read_text(encoding="utf-8")

POLO = {"id": "ep172026", "name": "Polo", "dps": 92.4, "intensity_knots": 155.0}


def test_card_matches_the_js_template():
    # Golden = the live page's own _renderOverviewCards output for this row
    # (outerHTML read from stormdps.com, 2026-09-25).
    assert hs.render_card(POLO) == (
        '<button class="ov-card" type="button" data-storm-id="ep172026" '
        'title="Open Polo — Historic · 155 kt · E. Pacific">'
        '<span class="ov-dps" style="background:#ef4444;color:#111"><small>DPS</small>92</span>'
        '<span class="ov-body"><span class="ov-name">Polo</span>'
        '<span class="ov-meta">Historic · 155 kt · E. Pacific</span></span></button>')


def test_card_edge_cases():
    unscored = hs.render_card({"id": "al992026", "name": "", "dps": None})
    assert "background:#64748b" in unscored and "<small>DPS</small>—" in unscored
    assert "Not scored yet · Atlantic" in unscored            # no kt, name falls back to id
    assert '<span class="ov-name">al992026</span>' in unscored
    assert "<small>DPS</small>53<" in hs.render_card({"id": "WP01", "dps": 52.5})   # JS Math.round, not banker's
    assert "Severe · W. Pacific" in hs.render_card({"id": "WP01", "dps": 52.5})
    evil = hs.render_card({"id": 'x"><b>', "name": "<img src=x>", "dps": 10})
    assert "<img" not in evil and "&lt;img src=x&gt;" in evil and 'x&quot;&gt;&lt;b&gt;' in evil
    assert "Low" in hs.render_card({"id": "AL01", "dps": 10, "dps_label": None})
    assert "Extreme" in hs.render_card({"id": "AL01", "dps": 10, "dps_label": "Extreme"})


def test_badge_text_colour_matches_js_contrast_rule():
    # badgeTextColor() on the live page, for every dpsColor band.
    js = {"#ef4444": "#111", "#f97316": "#111", "#fbbf24": "#111",
          "#34d399": "#111", "#60a5fa": "#111", "#64748b": "#fff"}
    assert {c: hs.badge_text_color(c) for c in js} == js


def test_real_shell_is_served_overview_first():
    out = hs.overview_first(INDEX, [POLO, {"id": "AL072026", "name": "Fay", "dps": 0}])
    assert 'class="welcome-state hidden" id="welcomeState"' in out
    assert 'class="overview-state" id="overviewState"' in out
    assert '<span class="ov-count" id="ovCount">· 2</span>' in out
    cards = re.search(r'<div class="ov-cards" id="overviewCards">(.*?)</div>', out, re.S).group(1)
    assert cards.count('class="ov-card"') == 2 and cards.index("Polo") < cards.index("Fay")
    # The invisible welcome logo is no longer preloaded at high priority.
    assert 'href="/frontend/logo-180.webp" type="image/webp" fetchpriority="high"' not in out
    assert 'alt="StormDPS" width="180" height="180" loading="lazy"' in out


def test_fail_open():
    assert hs.overview_first(INDEX, []) == INDEX                   # storm-free: welcome as before
    broken = INDEX.replace('id="overviewCards"', 'id="cardsRenamed"')
    assert hs.overview_first(broken, [POLO]) == broken              # markup drifted: untouched


def test_js_template_still_matches_python_fragments():
    # If someone edits the card template in index.html without home_shell,
    # the boot re-render would shift the layout. Pin the shared fragments.
    js = INDEX[INDEX.index("function _renderOverviewCards"):]
    js = js[:js.index("\n}\n") if "\n}\n" in js else js.index("\r\n}\r\n")]
    for frag in ('<button class="ov-card" type="button" data-storm-id="',
                 '<span class="ov-dps" style="background:',
                 '<small>DPS</small>',
                 '<span class="ov-body"><span class="ov-name">',
                 '<span class="ov-meta">',
                 "'Not scored yet'", "'#64748b'", "' · '"):
        assert frag in js, frag
    assert "document.getElementById('ovCount').textContent = storms.length ? '· ' + storms.length : '';" in js
