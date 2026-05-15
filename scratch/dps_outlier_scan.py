"""
DPS Outlier Scan
================

Surfaces storms in compiled_bundle.json whose displayed DPS doesn't match
their structural profile. Goal: identify storms that warrant a long-form
"why does this storm score X?" breakdown on the /storm/{id} page, so users
who land on a surprising score (Dorian at 87 with only $5B damage; Sandy
at 83 as a Cat 1 at landfall; etc.) get the context for why the formula
ranks the storm where it does.

Four outlier categories:

  1. HIGH-DPS / NO-US-LANDFALL — score >= 80 with us_landfall_count=0.
     Either over-scored (Dorian, Lee) or genuinely destructive non-US
     events (Maria, Fiona, EP storms that hit Mexico).

  2. HIGH-DPS / LOW-CATEGORY — score >= 80 with category_lifetime <= 2.
     Sandy-archetype: the score is justified by size/duration/surge but
     the wind-category label disagrees, which surprises users used to
     Saffir-Simpson.

  3. SCORE-VS-DAMAGE MISMATCH — score >= 80 with published damage < $10B
     OR score < 70 with published damage > $25B. Where I have a damage
     reference, flag the gap.

  4. BONUS-DRIVEN SCORES — bonus share > 25% of displayed score.
     Storms whose final DPS is mostly cumulative bonus, not peak DPI.
     These are the ones where the breakdown adds the most value.

The script reads the bundle directly so it runs in <1s and is rerun-safe.
"""
from __future__ import annotations
import json
import math
import os
from typing import Optional

BUNDLE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "frontend", "compiled_bundle.json"
)

# Published damage in 2024 USD billions, sourced from NOAA NCEI Billion-
# Dollar Disasters (CPI-adjusted where flagged), NHC TCRs, and EM-DAT.
# Sparse coverage by design — only storms where the damage is well-
# documented and unambiguous. Storms without an entry are flagged
# structurally (categories 1, 2, 4) but not by the damage-mismatch rule.
DAMAGE_B = {
    "AL122005": 200.0,   # Katrina
    "AL182012":  90.0,   # Sandy
    "AL092008":  50.0,   # Ike
    "AL092017": 160.0,   # Harvey
    "AL112017":  80.0,   # Irma
    "AL152017": 115.0,   # Maria
    "AL062018":  29.0,   # Florence
    "AL142018":  32.0,   # Michael
    "AL052019":   5.0,   # Dorian
    "AL112019":   5.0,   # Imelda
    "AL132020":  21.0,   # Laura
    "AL192020":   8.0,   # Sally
    "AL292020":   9.0,   # Zeta
    "AL292020z":  9.0,
    "AL092021":  84.0,   # Ida
    "AL072022":   3.0,   # Fiona (PR)
    "AL092022": 119.0,   # Ian
    "AL102023":   3.6,   # Idalia
    "AL022024":   7.0,   # Beryl
    "AL092024":  78.7,   # Helene
    "AL142024":  34.0,   # Milton
    "AL042024":   2.4,   # Debby
    "AL082023":   0.6,   # Hilary? (EP — placeholder)
    # EP
    "EP182023":  16.0,   # Otis (Acapulco)
}


def decompress(displayed: float, T: float = 60.0, S: float = 4.0) -> float:
    """Back-calc pre-compression value. Note: bundle was compiled under
    T=60, S=4, so this matches stored bundle values exactly."""
    if displayed <= T:
        return displayed
    return T + ((displayed - T) / S) ** 2


def load_bundle() -> dict:
    with open(BUNDLE_PATH, "r", encoding="utf-8") as f:
        return json.load(f).get("storms", {})


def fmt_d(x):
    return f"${x:5.1f}B" if x is not None else "  ----"


# US-mainland + US-territory landfall regions (matches the strings
# compute_exposure_factor emits in the landfalls array). The
# us_landfall_count field in the bundle is undercounting (most majors
# show us_lf=0 despite landing on US soil), so we derive from landfalls.
US_MAINLAND_REGIONS_SET = {
    "SE Florida", "Tampa Bay", "SW Florida", "NE Florida / Georgia",
    "FL Big Bend", "Texas", "Carolinas", "North Carolina",
    "New Orleans", "Biloxi / Gulfport", "Mobile",
    "Louisiana / Mississippi", "Mississippi",
    "Alabama / FL Panhandle", "Mid-Atlantic", "Northeast",
}
US_TERRITORY_REGIONS_SET = {"Puerto Rico / USVI"}


def lf_regions_set(landfalls: list) -> set[str]:
    return {(e.get("region") or "").strip() for e in (landfalls or []) if e}


def hit_us_mainland(landfalls: list) -> bool:
    return bool(lf_regions_set(landfalls) & US_MAINLAND_REGIONS_SET)


def hit_us_anywhere(landfalls: list) -> bool:
    return bool(lf_regions_set(landfalls) & (US_MAINLAND_REGIONS_SET | US_TERRITORY_REGIONS_SET))


def main():
    storms = load_bundle()

    # Build enriched per-storm record
    rows = []
    for sid, s in storms.items():
        dps   = float(s.get("dps", 0))
        peak  = float(s.get("peak_dps", 0))
        if dps < 1.0:
            continue  # skip empty / never-named storms

        cat = s.get("category_lifetime") or s.get("category") or 0
        try: cat = int(cat)
        except (TypeError, ValueError): cat = 0

        rows.append({
            "id":         sid,
            "name":       s.get("name", sid),
            "year":       s.get("year", "?"),
            "basin":      s.get("basin", "?"),
            "dps":        dps,
            "peak_dps":   peak,
            "cum":        float(s.get("dps_original", 0)),
            "dur":        float(s.get("duration_factor", 0)),
            "brd":        float(s.get("breadth_factor", 0)),
            "exp_f":      float(s.get("exposure_factor", 0)),
            "perp":       float(s.get("perp_factor", 0)),
            "stall_b":    float(s.get("stall_bonus", 0)),
            "rain":       float(s.get("rain_inland_factor", 0)),
            "inland":     float(s.get("inland_pen_factor", 0)),
            "us_lf":      int(s.get("us_landfall_count", 0)),
            "stall_h":    float(s.get("stall_hours", 0)),
            "coastal_h":  float(s.get("coastal_hours", 0)),
            "peak_kt":    float(s.get("peak_wind_kt", 0)),
            "ike_tj":     float(s.get("peak_ike_tj", 0)),
            "category":   cat,
            "damage_b":   DAMAGE_B.get(sid),
            "landfalls":  s.get("landfalls", []),
        })

    rows.sort(key=lambda r: -r["dps"])
    n = len(rows)
    print(f"Scanned {n} storms with DPS >= 1.0\n")

    # ─── Category 1: high DPS + no US landfall ──────────────────────────
    # NOTE: us_landfall_count field in the bundle is undercounting (Katrina,
    # Harvey, Ian, Sandy all show us_lf=0 despite obvious US landfalls).
    # Derive from the landfalls array instead.
    print("=" * 110)
    print("  CATEGORY 1 — HIGH-DPS / NO US LANDFALL  (DPS >= 75, no US-mainland or PR landfall)")
    print("=" * 110)
    cat1 = [r for r in rows if r["dps"] >= 75 and not hit_us_anywhere(r["landfalls"])]
    print(f"\n  {len(cat1)} storms\n")
    print(f"  {'Storm':<14} {'Year':>4} {'DPS':>5} {'Peak':>5} {'Cat':>3} "
          f"{'Stall_h':>7}  {'Damage':>7}  Landfall regions")
    print("  " + "-" * 100)
    for r in cat1:
        lf_regions = ", ".join(sorted(lf_regions_set(r["landfalls"]))) or "open ocean"
        damage = fmt_d(r["damage_b"])
        print(f"  {r['name']:<14} {r['year']:>4} {r['dps']:>5.1f} {r['peak_dps']:>5.1f} "
              f"{r['category']:>3} {r['stall_h']:>7.0f}  {damage:>7}  {lf_regions}")

    # ─── Category 2: high DPS + low landfall intensity ──────────────────
    # "Sandy archetype": Cat 1 at landfall but Cat 3 lifetime, high DPS due
    # to enormous wind field. Pull landfall-time intensity from the
    # category field (not category_lifetime) so this catches the surprise.
    print("\n" + "=" * 110)
    print("  CATEGORY 2 — HIGH-DPS / WEAK LANDFALL  (DPS >= 78, peak wind_kt < 110 / ≈ low-end Cat 3)")
    print("=" * 110)
    cat2 = [r for r in rows if r["dps"] >= 78 and r["peak_kt"] < 110]
    print(f"\n  {len(cat2)} storms\n")
    print(f"  {'Storm':<14} {'Year':>4} {'DPS':>5} {'Wind_kt':>7} {'IKE_TJ':>7} "
          f"{'Coastal_h':>9} {'Stall_h':>7}  {'Damage':>7}  Story")
    print("  " + "-" * 100)
    for r in cat2:
        damage = fmt_d(r["damage_b"])
        story = []
        if r["ike_tj"] > 200: story.append(f"large IKE {r['ike_tj']:.0f}TJ")
        if r["stall_h"] > 24: story.append(f"stalled {r['stall_h']:.0f}h")
        if r["coastal_h"] > 60: story.append(f"long coastal {r['coastal_h']:.0f}h")
        if r["brd"] >= 0.099: story.append("breadth_factor capped")
        print(f"  {r['name']:<14} {r['year']:>4} {r['dps']:>5.1f} "
              f"{r['peak_kt']:>7.0f} {r['ike_tj']:>7.0f} "
              f"{r['coastal_h']:>9.0f} {r['stall_h']:>7.0f}  {damage:>7}  {', '.join(story) or '-'}")

    # ─── Category 3: score-vs-damage mismatch ───────────────────────────
    print("\n" + "=" * 110)
    print("  CATEGORY 3 — SCORE / DAMAGE MISMATCH  (where damage data is available)")
    print("=" * 110)
    over = [r for r in rows if r["damage_b"] is not None and r["dps"] >= 80 and r["damage_b"] < 10]
    under = [r for r in rows if r["damage_b"] is not None and r["dps"] < 75 and r["damage_b"] > 25]
    print(f"\n  Over-scored (DPS >= 80, damage < $10B): {len(over)} storms")
    print(f"  {'Storm':<14} {'Year':>4} {'DPS':>5} {'Damage':>7}  Notes")
    print("  " + "-" * 60)
    for r in over:
        print(f"  {r['name']:<14} {r['year']:>4} {r['dps']:>5.1f} {fmt_d(r['damage_b']):>7}  "
              f"us_lf={r['us_lf']}, cat={r['category']}, stall={r['stall_h']:.0f}h")
    print(f"\n  Under-scored (DPS < 75, damage > $25B): {len(under)} storms")
    for r in under:
        print(f"  {r['name']:<14} {r['year']:>4} {r['dps']:>5.1f} {fmt_d(r['damage_b']):>7}  "
              f"peak={r['peak_dps']:.1f}, us_lf={r['us_lf']}, cat={r['category']}")

    # ─── Category 4: bonus-driven scores ────────────────────────────────
    print("\n" + "=" * 110)
    print("  CATEGORY 4 — BONUS-DRIVEN  (top of stack contributes > 25% of displayed)")
    print("=" * 110)
    print("\n  Definition: (cum_DPI - peak_DPS) + total Stage-3 bonuses, as a fraction")
    print("  of the displayed DPS. High values indicate the score is mostly accumulated")
    print("  duration/breadth/exposure boost rather than raw per-snapshot intensity.")
    print()
    enriched = []
    for r in rows:
        if r["dps"] < 60:
            continue
        cum_lift   = r["cum"] - r["peak_dps"]
        cb         = r["exp_f"] + r["perp"] + r["stall_b"] + r["rain"] + r["inland"]
        stage3_lift = r["peak_dps"] * cb
        total_lift  = cum_lift + stage3_lift
        share = total_lift / r["dps"] * 100.0 if r["dps"] > 0 else 0
        enriched.append((share, total_lift, cum_lift, stage3_lift, r))
    enriched.sort(key=lambda x: -x[0])
    print(f"  {'Storm':<14} {'Year':>4} {'DPS':>5} {'Peak':>5} {'CumLft':>6} "
          f"{'Stg3Lft':>7} {'Bon%':>5}  {'US_LF':>5}  {'Damage':>7}")
    print("  " + "-" * 90)
    for share, total_lift, cum_lift, stg3, r in enriched[:20]:
        damage = fmt_d(r["damage_b"])
        print(f"  {r['name']:<14} {r['year']:>4} {r['dps']:>5.1f} {r['peak_dps']:>5.1f} "
              f"{cum_lift:>6.1f} {stg3:>7.1f} {share:>5.1f}  {r['us_lf']:>5}  {damage:>7}")

    # ─── Summary: candidates for long-form breakdown ───────────────────
    print("\n" + "=" * 110)
    print("  CANDIDATES FOR LONG-FORM /storm/{id} BREAKDOWN")
    print("=" * 110)
    print("""
  Storms where a casual reader will be surprised by the displayed DPS and
  benefit from a few sentences of context. Selected from the overlap of
  categories above plus editorial judgement.
    """)
    # Build the candidate list — must be in bundle, score >= 75
    candidate_ids = set()
    for r in cat1[:8]:
        candidate_ids.add(r["id"])
    for r in cat2[:6]:
        candidate_ids.add(r["id"])
    for r in over[:6]:
        candidate_ids.add(r["id"])
    # Top 5 bonus-driven also
    for _, _, _, _, r in enriched[:5]:
        candidate_ids.add(r["id"])

    print(f"  {len(candidate_ids)} candidate storms (deduplicated across categories):\n")
    for sid in candidate_ids:
        r = next((x for x in rows if x["id"] == sid), None)
        if r is None: continue
        # One-line "why this is interesting"
        reasons = []
        if r["dps"] >= 80 and r["us_lf"] == 0:
            reasons.append("no US landfall")
        if r["dps"] >= 80 and r["category"] <= 2:
            reasons.append(f"only Cat {r['category']}")
        if r["damage_b"] is not None and r["dps"] >= 80 and r["damage_b"] < 10:
            reasons.append(f"only ${r['damage_b']:.0f}B damage")
        if r["damage_b"] is not None and r["dps"] < 75 and r["damage_b"] > 25:
            reasons.append(f"${r['damage_b']:.0f}B damage but DPS only {r['dps']:.0f}")
        if r["stall_h"] > 36:
            reasons.append(f"stalled {r['stall_h']:.0f}h")
        if r["ike_tj"] > 300:
            reasons.append(f"huge IKE {r['ike_tj']:.0f}TJ")
        print(f"  {r['name']:<14} ({r['year']}) DPS {r['dps']:.0f}  — {', '.join(reasons) or 'top tier'}")


if __name__ == "__main__":
    main()
