"""
Eastern Pacific DPS Formula Audit
==================================

Audits the current Eastern Pacific DPS adjustment against 5 major EP
hurricanes from 2015-2024 whose damage and fatality ground truth is
well-published. Mirrors scratch/wp_dps_audit.py — same methodology, EP
storm set, EP-specific gaps.

Why this audit now: 2026 is shaping up as a strong-El-Niño season, which
suppresses Atlantic activity and dramatically increases Eastern Pacific
activity (warmer SSTs, less shear). Patricia (2015), Otis (2023), and
Hilary (2023) were all strong-El-Niño-year storms. The EP basin's
formula path in compile_cache.py is currently minimal compared to WP:

    Feature                      Atlantic   EP         WP
    Basin multiplier             1.00       1.05       1.10
    RI bonus                     0          5-20       5-20
    Sub-basin multipliers        —          NONE       10 regions
    Multi-landfall bonus         —          NONE       up to +8
    Orographic bonus             —          NONE       up to +9
    Rainfall-footprint bonus     —          NONE       up to +6
    No-landfall dampener         —          NONE       ×0.60
    Coastal-zone coverage        Atlantic   minimal    full WP

That means a typical EP Cat 4-5 with the bonus stack would land at
pre-comp 100-130 (basin × 1.05 + RI bonus), compress to displayed 85-95,
and apply equally to both a devastating Acapulco landfall (Otis) and an
open-ocean recurver (Linda 1997). No way to discriminate.

Storm mix (3 landfall-destructive + 2 intensity-extreme, mirroring WP):
    3 destructive:
        Otis 2023      (Acapulco Cat 5, fastest RI ever, ~$16B, 51 deaths)
        John 2024      (Guerrero rainfall, $2.6B, 29 deaths)
        Hilary 2023    (Baja landfall + CA flooding, ~$1B, 4 deaths)
    2 intensity-extreme:
        Patricia 2015  (185 kt peak, rural landfall weakened, $460M, 8 deaths)
        Linda 1997     (160 kt Cat 5, full recurve, $0, 0 deaths)
"""
from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Any

# ---------------------------------------------------------------------------
# CURRENT v10 BASIN COEFFICIENTS for EP (copied verbatim from compile_cache.py)
# ---------------------------------------------------------------------------

EP_COEFFS_V10 = {
    "dps_multiplier": 1.05,
    "ri_bonus": 15,
    "duration_factor": 1.0,
    "compression_T": 70.0,
    "compression_S": 2.5,
}

# Compression params (post-v10, per-basin)
HARD_CAP = 99.0

# RI thresholds (same as WP)
RI_THRESHOLD_MS_PER_24H = 15.4   # 30 kt / 24h — standard RI definition
RI_MAX_SCALE_MS_PER_24H = 45.0   # ~87 kt / 24h — physical ceiling

# Duration & breadth caps (cumulative_dpi level)
DURATION_CAP = 0.10
BREADTH_CAP  = 0.10


# ---------------------------------------------------------------------------
# PEAK DPI PROXY  (wind-field based, calibrated to Atlantic anchors)
# ---------------------------------------------------------------------------
# Same proxy as scratch/wp_dps_audit.py so the EP and WP audits are
# directly comparable. Anchors:
#   Katrina (150 kt, R34 ≈ 230 nm)  → peak_dpi ≈ 126
#   Harvey  (115 kt, R34 ≈ 110 nm)  → peak_dpi ≈ 95
#   Michael (140 kt, R34 ≈  90 nm)  → peak_dpi ≈ 73
#   Sandy   ( 80 kt, R34 ≈ 480 nm)  → peak_dpi ≈ 80
#
# Form: peak_dpi ≈ wind_term + size_term + pres_term
#   wind_term = 35 * (V/100)^2.3
#   size_term = 10 * (R34/100)
#   pres_term = max(0, (1010 - P) / 10) * 1.2

def peak_dpi_proxy(wind_kt: float, r34_nm: float, pressure_hpa: float) -> float:
    """Wind+size proxy for per-snapshot peak DPI, anchored to Atlantic."""
    wind_term = 35.0 * (wind_kt / 100.0) ** 2.3
    size_term = 10.0 * (r34_nm / 100.0)
    pres_term = max(0.0, (1010 - pressure_hpa) / 10.0) * 1.2
    return wind_term + size_term + pres_term


# ---------------------------------------------------------------------------
# STORM PARAMETERS
# ---------------------------------------------------------------------------

@dataclass
class Storm:
    name:            str
    year:            int
    bucket:          str    # "destructive" | "intensity"
    damage_b:        float
    deaths:          int
    # Storm parameters at peak
    peak_wind_kt:    float
    peak_pressure:   float
    r34_nm:          float
    ri_24h_ms:       float   # peak 24-h intensification rate (m/s)
    # Landfall classification
    landfalls:       int     # significant landfall count
    sub_region:      str     # proposed EP sub-basin classification
    near_orographic: bool    # passed within 1° of Sierra Madre / Hawaiian peaks
    wind_at_orographic_ms: float  # wind speed during orographic encounter
    # Cumulative inputs (estimated from track narrative)
    duration_integral_hr: float
    breadth_factor_est:   float   # estimated 0..0.10
    duration_factor_est:  float   # estimated 0..0.10


STORMS = [
    # ── Destructive ─────────────────────────────────────────────────────
    Storm("Otis", 2023, "destructive", 16.0, 51,
          peak_wind_kt=145, peak_pressure=923, r34_nm=60,
          ri_24h_ms=46,   # 90 kt in 12h = ~46 m/s / 24h equivalent
          landfalls=1, sub_region="EP_MEXICO_PACIFIC",
          near_orographic=True, wind_at_orographic_ms=55,  # Sierra Madre del Sur
          duration_integral_hr=8, breadth_factor_est=0.02, duration_factor_est=0.0),

    Storm("John", 2024, "destructive", 2.6, 29,
          peak_wind_kt=105, peak_pressure=957, r34_nm=80,
          ri_24h_ms=20,
          landfalls=3, sub_region="EP_MEXICO_PACIFIC",
          near_orographic=True, wind_at_orographic_ms=42,  # Stalled near Guerrero mtns
          duration_integral_hr=36, breadth_factor_est=0.04, duration_factor_est=0.05),

    Storm("Hilary", 2023, "destructive", 1.0, 4,
          peak_wind_kt=125, peak_pressure=939, r34_nm=120,
          ri_24h_ms=28,
          landfalls=2, sub_region="EP_BAJA",
          near_orographic=True, wind_at_orographic_ms=32,  # Sierra de la Laguna, then SoCal mtns
          duration_integral_hr=30, breadth_factor_est=0.06, duration_factor_est=0.03),

    # ── Intensity-extreme ───────────────────────────────────────────────
    Storm("Patricia", 2015, "intensity", 0.46, 8,
          peak_wind_kt=185, peak_pressure=872, r34_nm=60,
          ri_24h_ms=62,   # 120 kt in 24h — physical-limit RI
          landfalls=1, sub_region="EP_MEXICO_PACIFIC",
          near_orographic=True, wind_at_orographic_ms=60,  # Sierra Madre del Sur, weakened
          duration_integral_hr=4, breadth_factor_est=0.01, duration_factor_est=0.0),

    Storm("Linda", 1997, "intensity", 0.0, 0,
          peak_wind_kt=160, peak_pressure=902, r34_nm=85,
          ri_24h_ms=36,
          landfalls=0, sub_region="EP_GENERAL",
          near_orographic=False, wind_at_orographic_ms=0,
          duration_integral_hr=0, breadth_factor_est=0.0, duration_factor_est=0.0),
]


# ---------------------------------------------------------------------------
# CURRENT v10 EP FORMULA — faithful reproduction
# ---------------------------------------------------------------------------

def score_current(s: Storm) -> Dict[str, Any]:
    """Apply the current (v10) EP formula path. Returns full breakdown."""
    peak_dpi = peak_dpi_proxy(s.peak_wind_kt, s.r34_nm, s.peak_pressure)

    # Cumulative bonuses
    cum_dpi = peak_dpi * (1.0 + s.duration_factor_est + s.breadth_factor_est)

    # Stage 3 combined_boost — EP has nothing here today
    # (exposure_factor / perp_factor / stall_bonus / rain_inland_factor /
    # inland_pen_factor are all 0 because the COASTAL_BOXES don't cover EP)
    combined_boost = 0.0
    boosted = cum_dpi  # combined_boost > 0 check fails, so boosted = cum_dpi

    # Basin multiplier
    adjusted = boosted * EP_COEFFS_V10["dps_multiplier"]

    # RI bonus — scaled 5..20 by magnitude (v7-audit logic)
    ri_pts = 0.0
    if s.ri_24h_ms > RI_THRESHOLD_MS_PER_24H:
        excess = s.ri_24h_ms - RI_THRESHOLD_MS_PER_24H
        scale = min(excess / (RI_MAX_SCALE_MS_PER_24H - RI_THRESHOLD_MS_PER_24H), 1.0)
        base_scale = EP_COEFFS_V10["ri_bonus"] / 15.0
        ri_pts = round(base_scale * (5.0 + 15.0 * scale), 1)
        adjusted += ri_pts

    pre_comp = adjusted

    # Compression
    _T = EP_COEFFS_V10["compression_T"]
    _S = EP_COEFFS_V10["compression_S"]
    if adjusted > _T:
        adjusted = _T + _S * math.sqrt(adjusted - _T)
    final = min(adjusted, HARD_CAP)

    return {
        "name":      s.name,
        "peak_dpi":  round(peak_dpi, 1),
        "cum_dpi":   round(cum_dpi, 1),
        "ri_pts":    ri_pts,
        "sub_mult":  1.00,    # no sub-basin multiplier today
        "lf_pts":    0.0,
        "oro_pts":   0.0,
        "rain_pts":  0.0,
        "dampener":  1.00,
        "pre_comp":  round(pre_comp, 1),
        "final":     round(final, 2),
        "damage_b":  s.damage_b,
        "deaths":    s.deaths,
    }


# ---------------------------------------------------------------------------
# PROPOSED v11 EP FORMULA — adds the missing pieces
# ---------------------------------------------------------------------------

# Proposed EP sub-basin multipliers. Calibrated to relative damage-per-
# unit-intensity profiles across the EP coast:
#   - Mexico Pacific Coast: dense, high vulnerability (Acapulco, Manzanillo,
#     Mazatlán). Otis 2023 demonstrated catastrophe potential.  → 1.10
#   - Baja California: low population density along most of the coast,
#     less infrastructure exposure than Mexico mainland. Hilary 2023
#     was a Cat 4 in EP but only $1B damage. → 0.95
#   - Central America Pacific: moderate density (El Salvador, Guatemala,
#     Nicaragua coast), high vulnerability. → 1.05
#   - Hawaii: low coastal density per-mile, but high asset value where
#     hit. Lane 2018 was rainfall-dominated. → 0.85
#   - EP General: open-ocean reference. → 1.00
PROPOSED_EP_SUB_BASIN_MULTIPLIERS = {
    "EP_MEXICO_PACIFIC": 1.10,
    "EP_BAJA":           0.95,
    "EP_CENTRAL_AMERICA":1.05,
    "EP_HAWAII":         0.85,
    "EP_GENERAL":        1.00,
}

# No-landfall dampener — mirrors WP. Open-ocean Cat 5 EP recurvers
# (Linda 1997, Walaka 2018, etc.) have real destructive potential but
# never realize it. Without this dampener every recurving Cat 5 scores
# in the 90s; with it, they correctly fall to the 80s.
EP_NO_LANDFALL_DAMPENER = 0.60

# Orographic bonus cap (same as WP)
ORO_CAP = 9.0


def score_proposed(s: Storm) -> Dict[str, Any]:
    """Apply the PROPOSED v11 EP formula path."""
    peak_dpi = peak_dpi_proxy(s.peak_wind_kt, s.r34_nm, s.peak_pressure)

    # Cumulative — same as current
    cum_dpi = peak_dpi * (1.0 + s.duration_factor_est + s.breadth_factor_est)

    # Stage 3 combined_boost — would be > 0 after coastal-box additions
    # but for the audit we keep it 0 (the coastal-box improvement is
    # measured separately by re-running the live pipeline against new
    # bundles).
    combined_boost = 0.0
    boosted = cum_dpi

    # Basin multiplier × sub-basin multiplier (NEW)
    sub_mult = PROPOSED_EP_SUB_BASIN_MULTIPLIERS.get(s.sub_region, 1.00)
    adjusted = boosted * EP_COEFFS_V10["dps_multiplier"] * sub_mult

    # RI bonus
    ri_pts = 0.0
    if s.ri_24h_ms > RI_THRESHOLD_MS_PER_24H:
        excess = s.ri_24h_ms - RI_THRESHOLD_MS_PER_24H
        scale = min(excess / (RI_MAX_SCALE_MS_PER_24H - RI_THRESHOLD_MS_PER_24H), 1.0)
        ri_pts = round(EP_COEFFS_V10["ri_bonus"] / 15.0 * (5.0 + 15.0 * scale), 1)
        adjusted += ri_pts

    # Multi-landfall bonus (NEW for EP — mirrors WP)
    lf_pts = 0.0
    if s.landfalls > 1:
        lf_pts = min((s.landfalls - 1) * 2.5, 8.0)
        adjusted += lf_pts

    # Orographic bonus (NEW for EP — mirrors WP)
    oro_pts = 0.0
    if s.near_orographic and s.wind_at_orographic_ms >= 20:
        oro_pts = min(s.wind_at_orographic_ms / 18, ORO_CAP)
        adjusted += oro_pts

    # No-landfall dampener (NEW for EP — mirrors WP)
    dampener = 1.0
    if s.landfalls == 0:
        dampener = EP_NO_LANDFALL_DAMPENER
        adjusted *= dampener

    pre_comp = adjusted

    # Compression — unchanged (T=70, S=2.5)
    _T = EP_COEFFS_V10["compression_T"]
    _S = EP_COEFFS_V10["compression_S"]
    if adjusted > _T:
        adjusted = _T + _S * math.sqrt(adjusted - _T)
    final = min(adjusted, HARD_CAP)

    return {
        "name":      s.name,
        "peak_dpi":  round(peak_dpi, 1),
        "cum_dpi":   round(cum_dpi, 1),
        "ri_pts":    ri_pts,
        "sub_mult":  sub_mult,
        "lf_pts":    round(lf_pts, 1),
        "oro_pts":   round(oro_pts, 1),
        "rain_pts":  0.0,
        "dampener":  dampener,
        "pre_comp":  round(pre_comp, 1),
        "final":     round(final, 2),
        "damage_b":  s.damage_b,
        "deaths":    s.deaths,
    }


# ---------------------------------------------------------------------------
# RANK CORRELATION
# ---------------------------------------------------------------------------

def spearman(a: List[float], b: List[float]) -> float:
    n = len(a)
    if n < 2:
        return 0.0
    def ranks(xs):
        order = sorted(range(n), key=lambda i: -xs[i])
        r = [0]*n
        for rank, idx in enumerate(order, 1):
            r[idx] = rank
        return r
    ra, rb = ranks(a), ranks(b)
    d_sq = sum((ra[i]-rb[i])**2 for i in range(n))
    return 1.0 - (6.0 * d_sq) / (n * (n*n - 1))


# ---------------------------------------------------------------------------
# DRIVER
# ---------------------------------------------------------------------------

def main():
    print("=" * 100)
    print("  EASTERN PACIFIC DPS FORMULA AUDIT")
    print("=" * 100)

    print(f"\n  {'Storm':<10} {'Dmg$B':>6} {'Deaths':>6} {'Wind_kt':>7} {'Pres_mb':>7} "
          f"{'R34_nm':>6} {'RI_m/s':>7} {'Region':<20} {'LF':>3}")
    print("  " + "-" * 95)
    for s in STORMS:
        print(f"  {s.name:<10} {s.damage_b:>6.1f} {s.deaths:>6} "
              f"{s.peak_wind_kt:>7.0f} {s.peak_pressure:>7.0f} {s.r34_nm:>6.0f} "
              f"{s.ri_24h_ms:>7.1f} {s.sub_region:<20} {s.landfalls:>3}")

    # ─── Scorecard under current v10 EP formula ─────────────────────────
    print("\n" + "=" * 100)
    print("  CURRENT v10 EP FORMULA — Scorecard")
    print("=" * 100)
    cur = [score_current(s) for s in STORMS]
    print(f"\n  {'Storm':<10} {'Peak':>5} {'Cum':>5} {'+RI':>5} {'×Sub':>5} "
          f"{'+LF':>4} {'+ORO':>5} {'×NoLF':>6}  {'PreComp':>7}  {'FINAL':>6}")
    print("  " + "-" * 80)
    for r in cur:
        print(f"  {r['name']:<10} {r['peak_dpi']:>5.1f} {r['cum_dpi']:>5.1f} "
              f"{r['ri_pts']:>5.1f} {r['sub_mult']:>5.2f} "
              f"{r['lf_pts']:>4.1f} {r['oro_pts']:>5.1f} {r['dampener']:>6.2f}  "
              f"{r['pre_comp']:>7.1f}  {r['final']:>6.2f}")

    damages = [s.damage_b for s in STORMS]
    deaths  = [s.deaths   for s in STORMS]
    finals_cur = [r["final"] for r in cur]
    print(f"\n  Spearman rho(damage, current) = {spearman(damages, finals_cur):>+.3f}")
    print(f"  Spearman rho(deaths, current) = {spearman(deaths,  finals_cur):>+.3f}")

    # ─── Scorecard under proposed v11 EP formula ────────────────────────
    print("\n" + "=" * 100)
    print("  PROPOSED v11 EP FORMULA — Adds sub-basin mult / multi-LF / orographic / no-LF dampener")
    print("=" * 100)
    new = [score_proposed(s) for s in STORMS]
    print(f"\n  {'Storm':<10} {'Peak':>5} {'Cum':>5} {'+RI':>5} {'×Sub':>5} "
          f"{'+LF':>4} {'+ORO':>5} {'×NoLF':>6}  {'PreComp':>7}  {'FINAL':>6}")
    print("  " + "-" * 80)
    for r in new:
        print(f"  {r['name']:<10} {r['peak_dpi']:>5.1f} {r['cum_dpi']:>5.1f} "
              f"{r['ri_pts']:>5.1f} {r['sub_mult']:>5.2f} "
              f"{r['lf_pts']:>4.1f} {r['oro_pts']:>5.1f} {r['dampener']:>6.2f}  "
              f"{r['pre_comp']:>7.1f}  {r['final']:>6.2f}")

    finals_new = [r["final"] for r in new]
    print(f"\n  Spearman rho(damage, proposed) = {spearman(damages, finals_new):>+.3f}")
    print(f"  Spearman rho(deaths, proposed) = {spearman(deaths,  finals_new):>+.3f}")

    # ─── Side-by-side delta ─────────────────────────────────────────────
    print("\n" + "=" * 100)
    print("  BEFORE vs AFTER (sorted by damage)")
    print("=" * 100)
    print(f"\n  {'Storm':<10} {'Dmg$B':>6} {'Deaths':>6}  {'v10 final':>9}  {'v11 final':>9}  {'Delta':>6}")
    print("  " + "-" * 65)
    by_damage = sorted(STORMS, key=lambda s: -s.damage_b)
    for s in by_damage:
        ci = next(r for r in cur if r["name"] == s.name)
        ni = next(r for r in new if r["name"] == s.name)
        d = ni["final"] - ci["final"]
        print(f"  {s.name:<10} {s.damage_b:>6.1f} {s.deaths:>6}  "
              f"{ci['final']:>9.2f}  {ni['final']:>9.2f}  {d:>+6.2f}")

    # ─── Specific failure cases under v10 ───────────────────────────────
    print("\n" + "=" * 100)
    print("  v10 EP FORMULA — KEY FAILURES")
    print("=" * 100)
    linda  = next(r for r in cur if r["name"] == "Linda")
    otis   = next(r for r in cur if r["name"] == "Otis")
    print(f"\n  1. Open-ocean intensity-extreme vs landfall catastrophe (no discrimination):")
    print(f"     Linda 1997  (no landfall, $0 damage)        → {linda['final']:.2f}")
    print(f"     Otis 2023   (Acapulco Cat 5, $16B, 51 deaths) → {otis['final']:.2f}")
    print(f"     Gap: {otis['final'] - linda['final']:+.2f} pts. Linda should be 10+ pts BELOW Otis.")
    hilary = next(r for r in cur if r["name"] == "Hilary")
    print(f"\n  2. Baja landfall (sparse) scores same as mainland Mexico (dense):")
    print(f"     Hilary 2023 (Baja Cat 1 landfall, $1B)       → {hilary['final']:.2f}")
    print(f"     Otis 2023   (Acapulco Cat 5,    $16B)        → {otis['final']:.2f}")
    print(f"     Gap reflects intensity only — no exposure-side discrimination.")
    john = next(r for r in cur if r["name"] == "John")
    print(f"\n  3. Rainfall-dominant Guerrero event under-scored:")
    print(f"     John 2024  ($2.6B, 29 deaths, Sierra rainfall) → {john['final']:.2f}")
    print(f"     (Cat 3 only; misses any rainfall / multi-LF / orographic credit.)")


if __name__ == "__main__":
    main()
