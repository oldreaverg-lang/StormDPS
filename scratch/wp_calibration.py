"""WP recalibration harness (2026-07) — 18-storm validation set.

Phase 1: V0 baseline on the expanded set (8 baked + 10 fetched via the live
/track pipeline). Phase 2+: R2 hygiene and R1/R3/R4 variants, applied by
monkeypatch, restored between runs. Companion to docs/audits/WP_DPS_AUDIT_V2.md.

New-storm tracks live in the session scratchpad (wp_set/<id>.json) — pass the
directory as argv[1]; defaults to the committed sample under scratch/wp_set if
present.
"""
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

WP_SET_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else (REPO / "scratch" / "wp_set")

from core.dps_engine import compute_storm_dps

PRE = json.loads((REPO / "frontend" / "preload_bundle.json").read_text(encoding="utf-8"))["storms"]

# sid: (name, year, damage_$B, deaths) — commonly cited totals
BAKED = {
    "2024244N09137": ("Yagi", 2024, 14.0, 844),
    "2025262N16133": ("Ragasa", 2025, 3.8, 30),
    "2024201N12133": ("Gaemi", 2024, 3.5, 150),
    "2015180N09160": ("Chan-Hom", 2015, 1.6, 7),
    "2025305N10138": ("Kalmaegi", 2025, 0.45, 266),
    "2024270N24128": ("Krathon", 2024, 0.31, 10),
    "2025308N10143": ("Fung-Wong", 2025, 0.30, 33),
    "2024298N13150": ("Kong-Rey", 2024, 0.18, 3),
}
FETCHED = {
    "WP312013":      ("Haiyan", 2013, 10.0, 6352),
    "2019278N16165": ("Hagibis", 2019, 18.0, 104),
    "2018250N12170": ("Mangkhut", 2018, 3.77, 134),
    "2023201N13134": ("Doksuri", 2023, 28.5, 137),
    "2020299N11144": ("Goni", 2020, 0.40, 32),
    "2021346N05145": ("Rai", 2021, 1.02, 409),
    "2021102N06144": ("Surigae", 2021, 0.01, 10),
    "2022254N24143": ("Nanmadol", 2022, 1.20, 4),
    "2022264N17132": ("Noru", 2022, 0.47, 40),
    "2023234N18128": ("Saola", 2023, 0.58, 3),
}
STORMS = {**BAKED, **FETCHED}


def engine_snaps(raws):
    return [{
        "timestamp": d.get("timestamp", ""), "lat": d.get("lat") or 0.0,
        "lon": d.get("lon") or 0.0, "max_wind_ms": d.get("max_wind_ms") or 0.0,
        "min_pressure_hpa": d.get("min_pressure_hpa") or 1013.0,
        "r34_nm": d.get("r34_nm") or 0.0, "r64_nm": d.get("r64_nm") or 0.0,
        "rmw_nm": d.get("rmw_nm") or 0.0,
        "forward_speed_knots": d.get("forward_speed_knots") or 0.0,
        "ike_total_tj": d.get("ike_total_tj") or 0.0,
        "r34_quadrants": d.get("r34_quadrants"),
        "r50_quadrants": d.get("r50_quadrants"),
        "r64_quadrants": d.get("r64_quadrants"),
        "et": bool(d.get("et", False)),
    } for d in raws]


def load_snaps(sid):
    if sid in BAKED:
        return PRE[sid]
    return engine_snaps(json.loads((WP_SET_DIR / f"{sid}.json").read_text(encoding="utf-8")))


def spearman(xs, ys):
    def rank(v):
        order = sorted(range(len(v)), key=lambda i: v[i])
        r = [0.0] * len(v)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and v[order[j + 1]] == v[order[i]]:
                j += 1
            avg = (i + j) / 2.0 + 1.0
            for k in range(i, j + 1):
                r[order[k]] = avg
            i = j + 1
        return r
    rx, ry = rank(xs), rank(ys)
    n = len(xs)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    den = (sum((x - mx) ** 2 for x in rx) * sum((y - my) ** 2 for y in ry)) ** 0.5
    return num / den if den else 0.0


def score_all(tag, mutate_snaps=None):
    results = {}
    for sid, (name, year, dmg, deaths) in STORMS.items():
        snaps = load_snaps(sid)
        if mutate_snaps:
            snaps = mutate_snaps(snaps)
        results[sid] = compute_storm_dps(storm_id=sid, snapshots=snaps,
                                         storm_name=name, storm_year=year)
    print(f"\n=== {tag} ===")
    order = sorted(STORMS, key=lambda s: -results[s]["dps"])
    for sid in order:
        name, year, dmg, deaths = STORMS[sid]
        b = results[sid]
        print(f"  {name:9s} {year}: dps={b['dps']:5.1f} peak={b['peak_dps']:5.1f} "
              f"ike={b['peak_ike_tj']:6.1f} dur={b['duration_factor']:.3f} "
              f"brd={b['breadth_factor']:.3f} ch={b['coastal_hours']:5.0f} "
              f"[{b['adjustment_notes']}]  (${dmg}B, {deaths}d)")
    dps = [results[s]["dps"] for s in STORMS]
    r_d = spearman(dps, [STORMS[s][2] for s in STORMS])
    r_k = spearman(dps, [STORMS[s][3] for s in STORMS])
    print(f"  Spearman rho vs damage: {r_d:+.3f}   vs deaths: {r_k:+.3f}")
    return results, r_d, r_k


if __name__ == "__main__":
    # IKE spike diagnostic on the fetched storms
    print("=== IKE spike scan (fetched storms): vmax<26 m/s with r34>=200nm ===")
    for sid, (name, *_r) in FETCHED.items():
        snaps = load_snaps(sid)
        bad = [(i, s) for i, s in enumerate(snaps)
               if (s.get("max_wind_ms") or 0) < 26 and (s.get("r34_nm") or 0) >= 200]
        peak_ike = max((s.get("ike_total_tj") or 0) for s in snaps)
        if bad:
            worst = max(bad, key=lambda t: t[1].get("ike_total_tj") or 0)[1]
            print(f"  {name}: {len(bad)} suspect rows, worst ike={worst.get('ike_total_tj')} "
                  f"(vmax={worst.get('max_wind_ms')}, r34={worst.get('r34_nm')}); "
                  f"storm peak_ike={peak_ike:.1f}")
        else:
            print(f"  {name}: clean; peak_ike={peak_ike:.1f}")

    score_all("V0 current formula — 18-storm set")
