"""WP recalibration harness — R2 hygiene + R1/R3/R4 package, staged, sandboxed.

Builds on scratch/wp_calibration.py (18-storm set). All changes are runtime
monkeypatches restored after each run; live modules are untouched on disk.

Stages:
  S0  current formula
  S1  R2 only: radii sanity gate (simulated as IKE clamp) + sustained-IKE (rolling-3)
  S2  S1 + geography: PH coastal box 135E->127.5E overhang fix, Vietnam box moved
      to actual Vietnam, sub-basin tally weighted by wind^2
  S3  S2 + R1/R4: WP coastal+economic profiles wired into the engine's region
      mapper; WP dps_multiplier 1.10->1.00; RI halved; sub-basin table flattened
"""
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scratch"))  # embedded python omits CWD

from wp_calibration import STORMS, load_snaps, spearman  # noqa: E402  (same dir)
from core.dps_engine import compute_storm_dps
from core import cumulative_dpi as cd
from core import storm_surge as ss
from core import economic_vulnerability as ev
import compile_cache as cc

# ---------------------------------------------------------------------------
# R2 — data hygiene (sandbox simulation; live impl goes in the track pipeline)
# ---------------------------------------------------------------------------

def radii_gate_ok(vmax_ms, r34_nm):
    if not r34_nm:
        return True
    if vmax_ms < 17.5:          # no 34-kt wind exists
        return False
    if vmax_ms < 25.7 and r34_nm > 150:   # <50 kt storms: 150 nm ceiling
        return False
    if vmax_ms < 33.0 and r34_nm > 250:   # <64 kt: 250 nm ceiling
        return False
    return True


def apply_r2(snaps):
    out = [dict(s) for s in snaps]
    for s in out:
        if not radii_gate_ok(s.get("max_wind_ms") or 0, s.get("r34_nm") or 0):
            s["ike_total_tj"] = min(s.get("ike_total_tj") or 0, 20.0)
            s["_radii_gated"] = True
    ikes = [s.get("ike_total_tj") or 0 for s in out]
    for i, s in enumerate(out):
        lo, hi = max(0, i - 1), min(len(out), i + 2)
        s["ike_total_tj"] = round(sum(ikes[lo:hi]) / (hi - lo), 2)
    return out


# ---------------------------------------------------------------------------
# R1 — WP coastal + economic profiles (iteration 1)
# ---------------------------------------------------------------------------

WP_COASTAL = {
    "wp_philippines": ss.CoastalProfile(
        name="Philippines Archipelago", shelf_width_km=40, avg_slope=0.002,
        surge_amplification=1.30, rain_enhancement=1.30, tidal_range_m=1.2,
        wetland_buffer=0.06, bay_funneling=1.35, coastal_defense=0.02,
        river_basin_factor=1.10, antecedent_moisture=0.75,
        bathymetric_concavity=0.35),
    "wp_taiwan": ss.CoastalProfile(
        name="Taiwan", shelf_width_km=25, avg_slope=0.01,
        surge_amplification=0.95, rain_enhancement=1.60, tidal_range_m=2.0,
        wetland_buffer=0.02, bay_funneling=1.05, coastal_defense=0.20,
        river_basin_factor=1.15, antecedent_moisture=0.65,
        bathymetric_concavity=0.05),
    "wp_south_china": ss.CoastalProfile(
        name="South/East China Coast (PRD to Zhejiang)", shelf_width_km=150,
        avg_slope=0.0004, surge_amplification=1.45, rain_enhancement=1.15,
        tidal_range_m=1.6, wetland_buffer=0.08, bay_funneling=1.30,
        coastal_defense=0.25, river_basin_factor=1.15, antecedent_moisture=0.70,
        bathymetric_concavity=0.25),
    "wp_vietnam": ss.CoastalProfile(
        name="Vietnam Coast", shelf_width_km=120, avg_slope=0.0005,
        surge_amplification=1.35, rain_enhancement=1.25, tidal_range_m=2.2,
        wetland_buffer=0.12, bay_funneling=1.15, coastal_defense=0.08,
        river_basin_factor=1.30, antecedent_moisture=0.80,
        bathymetric_concavity=0.15),
    "wp_japan": ss.CoastalProfile(
        name="Japan (incl. Ryukyus)", shelf_width_km=60, avg_slope=0.003,
        surge_amplification=1.10, rain_enhancement=1.35, tidal_range_m=1.5,
        wetland_buffer=0.02, bay_funneling=1.30, coastal_defense=0.35,
        river_basin_factor=1.20, antecedent_moisture=0.60,
        bathymetric_concavity=0.20),
    "wp_korea": ss.CoastalProfile(
        name="Korean Peninsula", shelf_width_km=80, avg_slope=0.001,
        surge_amplification=1.20, rain_enhancement=1.15, tidal_range_m=2.5,
        wetland_buffer=0.05, bay_funneling=1.15, coastal_defense=0.25,
        river_basin_factor=1.10, antecedent_moisture=0.60,
        bathymetric_concavity=0.10),
    "wp_hainan": ss.CoastalProfile(
        name="Hainan / Leizhou", shelf_width_km=90, avg_slope=0.0008,
        surge_amplification=1.25, rain_enhancement=1.15, tidal_range_m=1.8,
        wetland_buffer=0.08, bay_funneling=1.10, coastal_defense=0.08,
        river_basin_factor=1.05, antecedent_moisture=0.70,
        bathymetric_concavity=0.10),
    "wp_marianas": ss.CoastalProfile(
        name="Mariana Islands", shelf_width_km=5, avg_slope=0.05,
        surge_amplification=0.75, rain_enhancement=1.05, tidal_range_m=0.7,
        wetland_buffer=0.02, bay_funneling=1.00, coastal_defense=0.05,
        river_basin_factor=1.00, antecedent_moisture=0.60,
        bathymetric_concavity=0.05),
}

WP_ECON = {
    "wp_philippines": ev.EconomicProfile(
        name="Philippines Archipelago", exposed_value_index=5.0,
        population_density_factor=8.5, building_code_resilience=0.30,
        flood_infrastructure=0.25, elevation_vulnerability=0.75,
        critical_infrastructure=5.0, insurance_penetration=0.15,
        historical_damage_calibration=1.10, gdp_per_capita_usd=4000,
        supply_chain_criticality=0.25, total_asset_ceiling_billion=60,
        construction_vintage_mix=0.80, regional_exposure_area_km2=90000,
        grid_fragility=0.75),
    "wp_taiwan": ev.EconomicProfile(
        name="Taiwan", exposed_value_index=7.5, population_density_factor=8.0,
        building_code_resilience=0.75, flood_infrastructure=0.70,
        elevation_vulnerability=0.45, critical_infrastructure=8.5,
        insurance_penetration=0.50, historical_damage_calibration=1.00,
        gdp_per_capita_usd=34000, supply_chain_criticality=0.85,
        construction_vintage_mix=0.45, regional_exposure_area_km2=36000,
        grid_fragility=0.30),
    "wp_south_china": ev.EconomicProfile(
        name="South/East China Coast", exposed_value_index=9.0,
        population_density_factor=9.5, building_code_resilience=0.55,
        flood_infrastructure=0.60, elevation_vulnerability=0.80,
        critical_infrastructure=9.0, insurance_penetration=0.30,
        historical_damage_calibration=1.05, gdp_per_capita_usd=16000,
        supply_chain_criticality=0.80, construction_vintage_mix=0.50,
        regional_exposure_area_km2=60000, grid_fragility=0.40),
    "wp_vietnam": ev.EconomicProfile(
        name="Vietnam Coast", exposed_value_index=4.0,
        population_density_factor=8.0, building_code_resilience=0.30,
        flood_infrastructure=0.30, elevation_vulnerability=0.85,
        critical_infrastructure=5.0, insurance_penetration=0.10,
        historical_damage_calibration=1.10, gdp_per_capita_usd=4300,
        supply_chain_criticality=0.35, total_asset_ceiling_billion=60,
        construction_vintage_mix=0.85, regional_exposure_area_km2=60000,
        grid_fragility=0.70),
    "wp_japan": ev.EconomicProfile(
        name="Japan", exposed_value_index=9.5, population_density_factor=9.0,
        building_code_resilience=0.90, flood_infrastructure=0.85,
        elevation_vulnerability=0.55, critical_infrastructure=9.0,
        insurance_penetration=0.50, historical_damage_calibration=1.00,
        gdp_per_capita_usd=40000, supply_chain_criticality=0.70,
        construction_vintage_mix=0.30, regional_exposure_area_km2=100000,
        grid_fragility=0.15),
    "wp_korea": ev.EconomicProfile(
        name="Korean Peninsula", exposed_value_index=7.0,
        population_density_factor=7.5, building_code_resilience=0.80,
        flood_infrastructure=0.75, elevation_vulnerability=0.50,
        critical_infrastructure=7.5, insurance_penetration=0.45,
        historical_damage_calibration=0.95, gdp_per_capita_usd=35000,
        supply_chain_criticality=0.60, construction_vintage_mix=0.35,
        regional_exposure_area_km2=30000, grid_fragility=0.20),
    "wp_hainan": ev.EconomicProfile(
        name="Hainan / Leizhou", exposed_value_index=4.5,
        population_density_factor=6.0, building_code_resilience=0.45,
        flood_infrastructure=0.45, elevation_vulnerability=0.70,
        critical_infrastructure=4.5, insurance_penetration=0.20,
        historical_damage_calibration=1.00, gdp_per_capita_usd=9000,
        supply_chain_criticality=0.20, total_asset_ceiling_billion=80,
        construction_vintage_mix=0.60, regional_exposure_area_km2=34000,
        grid_fragility=0.50),
    "wp_marianas": ev.EconomicProfile(
        name="Mariana Islands", exposed_value_index=3.5,
        population_density_factor=4.0, building_code_resilience=0.70,
        flood_infrastructure=0.50, elevation_vulnerability=0.40,
        critical_infrastructure=6.0, insurance_penetration=0.45,
        historical_damage_calibration=1.00, gdp_per_capita_usd=35000,
        supply_chain_criticality=0.30, total_asset_ceiling_billion=15,
        construction_vintage_mix=0.40, regional_exposure_area_km2=1000,
        grid_fragility=0.55),
}

# Land-vicinity boxes -> profile key (checked in order; tighter boxes first)
WP_PROFILE_BOXES = [
    (13.0, 20.5, 144.0, 146.5, "wp_marianas"),
    (21.7, 25.5, 119.8, 122.2, "wp_taiwan"),
    (17.9, 20.2, 108.3, 111.2, "wp_hainan"),
    (32.8, 39.5, 124.0, 130.8, "wp_korea"),
    # Philippines split into latitude bands — the archipelago's east coast
    # slants from 126.6E (Mindanao) to 122.4E (Luzon); one rectangle either
    # cuts Samar off or swallows 300 km of Philippine Sea at Luzon latitudes
    # (where Surigae-class near-misses pass).
    (4.5, 12.8, 116.9, 127.3, "wp_philippines"),    # Mindanao / Visayas / Samar
    (12.8, 15.0, 119.5, 125.3, "wp_philippines"),   # Bicol / Catanduanes
    (15.0, 18.8, 119.6, 122.9, "wp_philippines"),   # Luzon
    (18.8, 21.2, 120.4, 122.4, "wp_philippines"),   # Batanes / Babuyan
    (8.0, 22.0, 102.0, 110.4, "wp_vietnam"),
    # South China Sea approach corridor (Paracels): a major TC here is hours
    # from Hainan / N Vietnam / PRD landfall (Yagi, Rammasun). Profile-mapping
    # only — NOT counted as land contact or coastal hours.
    (15.5, 21.5, 108.0, 117.0, "wp_hainan"),
    (20.0, 32.0, 109.5, 123.0, "wp_south_china"),
    (24.0, 30.5, 122.5, 131.0, "wp_japan"),
    (30.5, 45.5, 129.0, 146.0, "wp_japan"),
]

# ONE tight land geometry for coastal-hours accrual, landfall detection, and
# the no-landfall dampener (replaces the coarse WP entries whose PH box ran
# ~800 km into open ocean and whose "Vietnam" box sat in the Taiwan Strait).
WP_TIGHT_LAND = [
    (13.0, 20.5, 144.0, 146.5, "Mariana Islands"),
    (4.5, 12.8, 116.9, 127.3, "Philippines"),       # Mindanao / Visayas / Samar
    (12.8, 15.0, 119.5, 125.3, "Philippines"),      # Bicol / Catanduanes
    (15.0, 18.8, 119.6, 122.9, "Philippines"),      # Luzon
    (18.8, 21.2, 120.4, 122.4, "Philippines"),      # Batanes / Babuyan
    (21.7, 25.5, 119.8, 122.2, "Taiwan"),
    (24.0, 45.5, 122.5, 146.0, "Japan"),
    (8.0, 21.8, 102.0, 110.4, "Vietnam / Cambodia"),
    (5.0, 15.0, 98.0, 105.2, "Thailand / Laos"),
    (20.0, 41.0, 105.5, 123.0, "China"),
]
_WP_LABELS = {"Mariana Islands", "Philippines", "Taiwan", "Japan",
              "Vietnam / Cambodia", "Thailand / Laos", "China"}

_ORIG_REGION_FN = cd._estimate_region_from_coords


def region_with_wp(lat, lon):
    r = _ORIG_REGION_FN(lat, lon)
    if r is not None:
        return r
    for lat_min, lat_max, lon_min, lon_max, key in WP_PROFILE_BOXES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return key
    return None


# ---------------------------------------------------------------------------
# Geography fixes (S2): PH coastal-box overhang + Vietnam box location
# ---------------------------------------------------------------------------

PH_BOX_FIXED_CUM = (5, 21, 117.0, 127.5, "Philippines")
VN_BOX_FIXED_CUM = (8, 23, 102.0, 110.5, "Vietnam / Cambodia")
PH_BOX_FIXED_CC = (5, 21, 117.0, 127.5, "Philippines")
VN_BOX_FIXED_CC = (8, 23, 102.0, 110.5, "Vietnam / Cambodia")

_ORIG_CUM_BOXES = list(cd.COASTAL_BOXES)
_ORIG_CC_REGIONS = list(cc.COASTAL_REGIONS)
_ORIG_SUB = cc.determine_wp_sub_basin
_ORIG_WP_COEFFS = json.loads(json.dumps(
    {k: v for k, v in cc.BASIN_COEFFICIENTS["WESTERN_PACIFIC"].items()}))


def patch_geography(tight=False):
    if tight:
        # Replace every WP entry in both geometry lists with the unified
        # tight-land envelope (same rectangles everywhere).
        cd.COASTAL_BOXES[:] = [b for b in cd.COASTAL_BOXES
                               if b[4] not in _WP_LABELS] + \
            [(a, b_, c, d, lbl) for a, b_, c, d, lbl in WP_TIGHT_LAND]
        cc.COASTAL_REGIONS[:] = [b for b in cc.COASTAL_REGIONS
                                 if b[4] not in _WP_LABELS] + \
            [(a, b_, c, d, lbl) for a, b_, c, d, lbl in WP_TIGHT_LAND]
    else:
        for i, b in enumerate(cd.COASTAL_BOXES):
            if b[4] == "Philippines":
                cd.COASTAL_BOXES[i] = PH_BOX_FIXED_CUM
            elif b[4] == "Vietnam / Cambodia":
                cd.COASTAL_BOXES[i] = VN_BOX_FIXED_CUM
        for i, b in enumerate(cc.COASTAL_REGIONS):
            if b[4] == "Philippines":
                cc.COASTAL_REGIONS[i] = PH_BOX_FIXED_CC
            elif b[4] == "Vietnam / Cambodia":
                cc.COASTAL_REGIONS[i] = VN_BOX_FIXED_CC
    cc.determine_wp_sub_basin = wp_sub_basin_wind2


def wp_sub_basin_wind2(snapshots):
    """Sub-basin tally weighted by wind^2 so the classification follows where
    the storm was STRONG in a region, not where the track loitered."""
    if not snapshots:
        return "WP_GENERAL"
    regions = [
        ("WP_MARIANA", 13.0, 20.5, 144.0, 146.5),
        ("WP_TAIWAN", 21.5, 25.5, 119.5, 122.5),
        ("WP_PHILIPPINES", 5.0, 20.0, 117.0, 127.0),
        ("WP_VIETNAM", 8.0, 22.0, 102.0, 112.0),
        ("WP_HAINAN", 17.5, 20.5, 108.0, 111.5),
        ("WP_SOUTH_CHINA", 20.0, 26.0, 108.0, 118.0),
        ("WP_NORTH_CHINA", 26.0, 41.0, 117.0, 124.5),
        ("WP_KOREA", 33.0, 39.0, 124.5, 131.5),
        ("WP_JAPAN", 24.0, 45.5, 128.0, 146.0),
    ]
    counts = {key: 0.0 for key, *_ in regions}
    for s in snapshots:
        lat, lon = s.get("lat", 0), s.get("lon", 0)
        w = (s.get("max_wind_ms", 0) or 0) ** 2
        for key, lat_min, lat_max, lon_min, lon_max in regions:
            if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                counts[key] += w
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else "WP_GENERAL"


def patch_profiles_and_coeffs():
    ss.COASTAL_PROFILES.update(WP_COASTAL)
    ev.ECONOMIC_PROFILES.update(WP_ECON)
    cd._estimate_region_from_coords = region_with_wp
    co = cc.BASIN_COEFFICIENTS["WESTERN_PACIFIC"]
    co["dps_multiplier"] = 1.00
    co["ri_bonus"] = 7.5
    co["sub_basin_multipliers"] = {k: 1.00 for k in co["sub_basin_multipliers"]}
    # With honest peak_dpi (living legs) WP scores sit ABOVE the old T=70
    # knee, where the curve flattens 85-110 raw into an 85-91 mush. Engage
    # compression later so the honest top end keeps its ordering spread.
    co["compression_T"] = 80.0


_ORIG_APPLY = cc.apply_basin_dps_adjustment


def harness_apply_basin(cum_dpi, basin, snapshots,
                        duration_factor=None, breadth_factor=None):
    """S5 WP adjustment path: mult 1.0, NO RI (undiscriminating — every WP
    major RIs, so it was +7..+10 mush on all of them), landfall-intensity
    bonus instead (wind at land contact is what separates Haiyan from a
    Batanes brush), dampener vs tight land geometry, compression T=80."""
    import math
    if basin != "WESTERN_PACIFIC":
        return _ORIG_APPLY(cum_dpi, basin, snapshots,
                           duration_factor=duration_factor,
                           breadth_factor=breadth_factor)
    notes = []
    adj = cum_dpi * 1.00
    n_lf, _ = cc.count_significant_landfalls(snapshots)
    if n_lf > 1:
        b = min((n_lf - 1) * 2.5, 8)
        adj += b
        notes.append(f"+{b:.1f}LF")
    has_oro, w_mtn = cc.has_orographic_rainfall_potential(snapshots, basin)
    if has_oro and w_mtn >= 20:
        b = min(w_mtn / 18, 9)
        adj += b
        notes.append(f"+{b:.1f}ORO")
    sb = cc.determine_wp_sub_basin(snapshots)
    if (sb in {"WP_JAPAN", "WP_SOUTH_CHINA", "WP_VIETNAM", "WP_TAIWAN"}
            and duration_factor is not None and breadth_factor is not None):
        rb = 6.0 * min(duration_factor / 0.10, 1.0) * min(breadth_factor / 0.10, 1.0)
        if rb > 0.1:
            adj += rb
            notes.append(f"+{rb:.1f}RAIN")
    # Landfall-intensity bonus: peak wind while inside the tight land boxes.
    lf_w = 0.0
    for s in snapshots:
        la, lo = s.get("lat", 0), s.get("lon", 0)
        w = s.get("max_wind_ms", 0) or 0
        if w <= lf_w:
            continue
        for a, b_, c, d, _lbl in WP_TIGHT_LAND:
            if a <= la <= b_ and c <= lo <= d:
                lf_w = w
                break
    if lf_w >= 50.0:
        b = min(12.0, 12.0 * (lf_w - 50.0) / 28.0)
        if b > 0.1:
            adj += b
            notes.append(f"+{b:.1f}LFI")
    if n_lf == 0 and not cc.storm_made_land_contact(snapshots):
        if cc.track_is_in_progress(snapshots):
            notes.append("no-LF dampener deferred (active storm)")
        else:
            adj *= 0.60
            notes.append("×0.60(no-landfall)")
    T = 80.0
    if adj > T:
        span = 99.0 - T
        adj = T + span * (1.0 - math.exp(-(adj - T) / span))
    return adj, "Western Pacific", ", ".join(notes)


def restore_all():
    cc.apply_basin_dps_adjustment = _ORIG_APPLY
    for k in WP_COASTAL:
        ss.COASTAL_PROFILES.pop(k, None)
        ev.ECONOMIC_PROFILES.pop(k, None)
    cd._estimate_region_from_coords = _ORIG_REGION_FN
    cd.COASTAL_BOXES[:] = _ORIG_CUM_BOXES
    cc.COASTAL_REGIONS[:] = _ORIG_CC_REGIONS
    cc.determine_wp_sub_basin = _ORIG_SUB
    cc.BASIN_COEFFICIENTS["WESTERN_PACIFIC"].update(_ORIG_WP_COEFFS)


def stage(tag, *, r2=False, geo=False, full=False, tight=False, s5=False):
    try:
        if geo or full:
            patch_geography(tight=tight)
        if full:
            patch_profiles_and_coeffs()
        if s5:
            for k, p in WP_ECON.items():
                p.historical_damage_calibration = round(
                    p.historical_damage_calibration * 0.85, 3)
            cc.apply_basin_dps_adjustment = harness_apply_basin
        results = {}
        for sid, (name, year, dmg, deaths) in STORMS.items():
            snaps = load_snaps(sid)
            if r2 or full:
                snaps = apply_r2(snaps)
            results[sid] = compute_storm_dps(storm_id=sid, snapshots=snaps,
                                             storm_name=name, storm_year=year)
    finally:
        restore_all()
    print(f"\n=== {tag} ===")
    for sid in sorted(STORMS, key=lambda s: -results[s]["dps"]):
        name, year, dmg, deaths = STORMS[sid]
        b = results[sid]
        print(f"  {name:9s}: dps={b['dps']:5.1f} peak={b['peak_dps']:5.1f} "
              f"ike={b['peak_ike_tj']:6.1f} dur={b['duration_factor']:.2f} "
              f"brd={b['breadth_factor']:.2f} [{b['adjustment_notes']}] "
              f"(${dmg}B/{deaths}d)")
    dps = [results[s]["dps"] for s in STORMS]
    r_d = spearman(dps, [STORMS[s][2] for s in STORMS])
    r_k = spearman(dps, [STORMS[s][3] for s in STORMS])
    print(f"  rho damage {r_d:+.3f} | deaths {r_k:+.3f}")
    return results, r_d, r_k


if __name__ == "__main__":
    s0 = stage("S0 current")
    s1 = stage("S1 +R2 hygiene", r2=True)
    s2 = stage("S2 +geography (PH overhang, VN box, wind^2 labels)", r2=True, geo=True)
    s3 = stage("S3 +R1/R4 (WP profiles, mult 1.0, RI/2, flat sub-basins)",
               r2=True, geo=True, full=True)
    s4 = stage("S4 = S3 + tight unified land geometry + corridor + T=80",
               r2=True, geo=True, full=True, tight=True)
    s5 = stage("S5 = S4 + slant PH boxes, no WP RI, +LFI bonus, econ x0.85",
               r2=True, geo=True, full=True, tight=True, s5=True)
    print("\n=== rho progression ===")
    for tag, (res, rd, rk) in [("S0", s0), ("S1", s1), ("S2", s2), ("S3", s3),
                               ("S4", s4), ("S5", s5)]:
        print(f"  {tag}: damage {rd:+.3f} | deaths {rk:+.3f}")
