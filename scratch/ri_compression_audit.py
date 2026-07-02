#!/usr/bin/env python3
"""
Sandbox audit: Atlantic RI bonus + timestamp-window RI detection + C1 compressor.

Per .claude/skills/basin-dps-audit: NO live edits — this script reimplements
apply_basin_dps_adjustment with the proposed changes, reruns the full
compute_storm_dps pipeline over preload_bundle.json, and compares variants
against core/ground_truth.py damage figures and the current live scores.

Variants
  BASELINE      current code path (reproduction check vs compiled_bundle.json)
  RI_WINDOW     RI gain = max wind increase over any <=24.5h timestamp window
                (replaces single-step delta x4); ATLANTIC ri_bonus 0 -> 15
  RI_NEARLF     RI_WINDOW, but the bonus only counts if the RI window ends
                within 72h before first land contact (realized-threat gating)
  COMP          C1 rational compressor f(x) = T + (99-T)(x-T)/((x-T)+k),
                k fitted per basin (least squares vs old curve over observed raws)
  BOTH          RI_NEARLF + COMP

Usage: python3 scratch/ri_compression_audit.py [--quick N]
"""
import argparse
import json
import math
import os
import sys
from datetime import datetime

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

import compile_cache as cc
from core.dps_engine import compute_storm_dps
from core import ground_truth as gt

# ---------------------------------------------------------------- helpers

def parse_ts(s):
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except (ValueError, TypeError):
            continue
    return None


def max_24h_gain_window(snapshots, max_window_h=24.5):
    """Max wind increase (m/s) over any timestamp window <= max_window_h.
    Returns (gain_ms_per_24h_equivalent, end_idx). Cadence-independent."""
    pts = []
    for i, s in enumerate(snapshots):
        t = parse_ts(s.get("timestamp", ""))
        w = s.get("max_wind_ms", 0) or 0
        if t is not None:
            pts.append((t, w, i))
    best, best_idx = 0.0, None
    for j in range(1, len(pts)):
        tj, wj, ij = pts[j]
        for i in range(j - 1, -1, -1):
            ti, wi, _ = pts[i]
            dh = (tj - ti).total_seconds() / 3600.0
            if dh > max_window_h:
                break
            if dh <= 0:
                continue
            gain = wj - wi
            if gain > best:
                best, best_idx = gain, ij
    return best, best_idx


def first_land_contact_time(snapshots):
    """Timestamp of first landfall event (or first near-coast hurricane-force
    contact via storm_made_land_contact fallback scan)."""
    events = cc.detect_landfall_events(snapshots)
    if events:
        idxs = [e.get("snapshot_idx") for e in events if e.get("snapshot_idx") is not None]
        if idxs:
            t = parse_ts(snapshots[min(idxs)].get("timestamp", ""))
            if t:
                return t
    return None


RI_THRESHOLD = 15.4  # m/s per 24h (30 kt) — standard NHC definition
RI_MAX = 45.0

CONTEXT = {}   # per-storm scratch: id -> dict(raw=, notes=)


def variant_adjustment(ri_mode, comp_mode, k_by_basin):
    """Build a drop-in replacement for cc.apply_basin_dps_adjustment."""

    def adj(cum_dpi, basin, snapshots, duration_factor=None, breadth_factor=None):
        coeffs = cc.BASIN_COEFFICIENTS.get(basin, cc.BASIN_COEFFICIENTS["ATLANTIC"])
        adjusted = cum_dpi * coeffs["dps_multiplier"]
        notes = []

        sub_basin = None
        if basin == "WESTERN_PACIFIC":
            sub_basin = cc.determine_wp_sub_basin(snapshots)
            m = coeffs.get("sub_basin_multipliers", {}).get(
                sub_basin, coeffs.get("sub_basin_multipliers", {}).get("WP_GENERAL", 1.0))
            if abs(m - 1.0) > 0.01:
                adjusted *= m
                notes.append(f"x{m:.2f}({sub_basin})")
        elif basin == "EASTERN_PACIFIC":
            sub_basin = cc.determine_ep_sub_basin(snapshots)
            m = coeffs.get("sub_basin_multipliers", {}).get(
                sub_basin, coeffs.get("sub_basin_multipliers", {}).get("EP_GENERAL", 1.0))
            if abs(m - 1.0) > 0.01:
                adjusted *= m
                notes.append(f"x{m:.2f}({sub_basin})")

        # ---- RI bonus ----
        ri_bonus_coeff = coeffs["ri_bonus"]
        if ri_mode in ("window", "nearlf") and basin == "ATLANTIC":
            ri_bonus_coeff = 15  # proposed: enable for Atlantic
        ri_bonus = 0.0
        if ri_bonus_coeff > 0 and len(snapshots) >= 2:
            if ri_mode == "legacy":
                gain = 0.0
                for i in range(1, len(snapshots)):
                    wp = snapshots[i - 1].get("max_wind_ms", 0) or 0
                    wc = snapshots[i].get("max_wind_ms", 0) or 0
                    gain = max(gain, (wc - wp) * 4)
            else:
                gain, end_idx = max_24h_gain_window(snapshots)
                if ri_mode == "nearlf" and gain > RI_THRESHOLD:
                    lf_t = first_land_contact_time(snapshots)
                    ri_t = parse_ts(snapshots[end_idx].get("timestamp", "")) if end_idx is not None else None
                    if lf_t is None or ri_t is None or not (0 <= (lf_t - ri_t).total_seconds() / 3600.0 <= 72):
                        gain = 0.0
            if gain > RI_THRESHOLD:
                scale = min((gain - RI_THRESHOLD) / (RI_MAX - RI_THRESHOLD), 1.0)
                ri_bonus = round((ri_bonus_coeff / 15.0) * (5.0 + 15.0 * scale), 1)
                adjusted += ri_bonus
                notes.append(f"+{ri_bonus}RI")

        # ---- WP / EP additive blocks (unchanged from live code) ----
        if basin == "WESTERN_PACIFIC":
            lf_count, _ = cc.count_significant_landfalls(snapshots)
            if lf_count > 1:
                b = min((lf_count - 1) * 2.5, 8)
                adjusted += b
                notes.append(f"+{b:.1f}LF")
            has_oro, wnm = cc.has_orographic_rainfall_potential(snapshots, basin)
            if has_oro and wnm >= 20:
                b = min(wnm / 18, 9)
                adjusted += b
                notes.append(f"+{b:.1f}ORO")
            if (sub_basin in {"WP_JAPAN", "WP_SOUTH_CHINA", "WP_VIETNAM", "WP_TAIWAN"}
                    and duration_factor is not None and breadth_factor is not None):
                dfrac = min(duration_factor / 0.10, 1.0)
                bfrac = min(breadth_factor / 0.10, 1.0)
                rb = 6.0 * dfrac * bfrac
                if rb > 0.1:
                    adjusted += rb
                    notes.append(f"+{rb:.1f}RAIN")
            if lf_count == 0 and not cc.storm_made_land_contact(snapshots):
                adjusted *= 0.60
                notes.append("x0.60(no-landfall)")
        if basin == "EASTERN_PACIFIC":
            lf_count, _ = cc.count_significant_landfalls(snapshots)
            if lf_count > 1:
                b = min((lf_count - 1) * 2.5, 8)
                adjusted += b
                notes.append(f"+{b:.1f}LF")
            has_oro, wnm = cc.has_orographic_rainfall_potential(snapshots, basin)
            if has_oro and wnm >= 20:
                b = min(wnm / 18, 9)
                adjusted += b
                notes.append(f"+{b:.1f}ORO")
            if (sub_basin in {"EP_MEXICO_PACIFIC", "EP_CENTRAL_AMERICA"}
                    and duration_factor is not None and breadth_factor is not None):
                dfrac = min(duration_factor / 0.10, 1.0)
                bfrac = min(breadth_factor / 0.10, 1.0)
                rb = 6.0 * dfrac * bfrac
                if rb > 0.1:
                    adjusted += rb
                    notes.append(f"+{rb:.1f}RAIN")
            if lf_count == 0 and not cc.storm_made_land_contact(snapshots):
                adjusted *= 0.60
                notes.append("x0.60(no-landfall)")

        # record raw pre-compression for k-fitting / diagnostics
        CONTEXT.setdefault("raws", {})[CONTEXT.get("cur")] = (adjusted, basin)

        # ---- compression ----
        T = float(coeffs.get("compression_T", 70.0))
        S = float(coeffs.get("compression_S", 2.5))
        if comp_mode == "sqrt":
            if adjusted > T:
                adjusted = T + S * math.sqrt(adjusted - T)
            adjusted = min(adjusted, 99.0)
        elif comp_mode == "rational":  # C1 at T, asymptote 99
            k = k_by_basin.get(basin, k_by_basin.get("_default", 30.0))
            if adjusted > T:
                x = adjusted - T
                adjusted = T + (99.0 - T) * x / (x + k)
        else:  # "exp": parameter-free C1 saturating curve, slope 1 at T,
               # asymptote 99. Fully determined by T — no fitted constants.
            if adjusted > T:
                x = adjusted - T
                span = 99.0 - T
                adjusted = T + span * (1.0 - math.exp(-x / span))

        return adjusted, coeffs["name"], ", ".join(notes)

    return adj


# ---------------------------------------------------------------- run

def spearman(xs, ys):
    def rank(v):
        order = sorted(range(len(v)), key=lambda i: v[i])
        r = [0.0] * len(v)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and v[order[j + 1]] == v[order[i]]:
                j += 1
            avg = (i + j) / 2.0 + 1
            for t in range(i, j + 1):
                r[order[t]] = avg
            i = j + 1
        return r
    rx, ry = rank(xs), rank(ys)
    n = len(xs)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    den = math.sqrt(sum((a - mx) ** 2 for a in rx) * sum((b - my) ** 2 for b in ry))
    return num / den if den else 0.0


def run_variant(storms, meta, ri_mode, comp_mode, k_by_basin, force_patch=False):
    orig = cc.apply_basin_dps_adjustment
    if force_patch or not (ri_mode == "legacy" and comp_mode == "sqrt"):
        cc.apply_basin_dps_adjustment = variant_adjustment(ri_mode, comp_mode, k_by_basin)
    out = {}
    try:
        for sid, snaps in storms.items():
            m = meta.get(sid, {})
            CONTEXT["cur"] = sid
            try:
                r = compute_storm_dps(sid, snaps, m.get("name", sid), m.get("year", 2024))
                out[sid] = r["dps"]
            except Exception as e:
                out[sid] = None
    finally:
        cc.apply_basin_dps_adjustment = orig
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", type=int, default=0, help="limit to N storms (plus anchors)")
    args = ap.parse_args()

    preload = json.load(open(os.path.join(REPO, "frontend", "preload_bundle.json")))
    storms = preload.get("storms", preload)
    compiled = json.load(open(os.path.join(REPO, "frontend", "compiled_bundle.json")))
    cstorms = compiled.get("storms", compiled)
    meta = {sid: {"name": v.get("name", sid), "year": v.get("year", 2024), "basin": v.get("basin")}
            for sid, v in cstorms.items()}

    anchors = ["AL122005", "AL092017", "AL112017", "AL152017", "AL092022", "AL182012",
               "AL142018", "AL092024", "AL142024", "AL052019", "AL062018", "AL102023",
               "AL022024", "AL092008"]
    if args.quick:
        keep = set(anchors) | set(list(storms.keys())[: args.quick])
        storms = {k: v for k, v in storms.items() if k in keep}

    # 1. BASELINE + reproduction check
    CONTEXT.clear()
    base = run_variant(storms, meta, "legacy", "sqrt", {})   # uses REAL live function (no patch)
    CONTEXT.clear()
    base_i = run_variant(storms, meta, "legacy", "sqrt", {}, force_patch=True)  # patched replica, captures raws
    raws = dict(CONTEXT.get("raws", {}))

    repro_bad = []
    for sid, dps in base.items():
        cd = cstorms.get(sid, {}).get("dps")
        if dps is not None and cd is not None and abs(dps - cd) > 1.5:
            repro_bad.append((sid, round(dps, 1), cd))
    replica_bad = [(sid, round(base[sid], 1), round(base_i[sid], 1))
                   for sid in base
                   if base[sid] is not None and base_i[sid] is not None
                   and abs(base[sid] - base_i[sid]) > 0.05]

    # 2. Fit k per basin (least squares vs old curve over observed raws > T)
    k_by_basin = {"_default": 30.0}
    for basin, coeffs in cc.BASIN_COEFFICIENTS.items():
        T = float(coeffs.get("compression_T", 70.0))
        S = float(coeffs.get("compression_S", 2.5))
        xs = [r for sid, (r, b) in raws.items() if b == basin and r > T]
        if len(xs) < 3:
            continue
        best_k, best_err = None, None
        k = 2.0
        while k <= 120.0:
            err = 0.0
            for x in xs:
                old = min(T + S * math.sqrt(x - T), 99.0)
                new = T + (99.0 - T) * (x - T) / ((x - T) + k)
                err += (new - old) ** 2
            if best_err is None or err < best_err:
                best_k, best_err = k, err
            k += 0.5
        k_by_basin[basin] = best_k

    # 3. Variants
    results = {"BASELINE": base}
    for name, (rm, cm) in {
        "RI_WINDOW": ("window", "sqrt"),
        "RI_NEARLF": ("nearlf", "sqrt"),
        "COMP_RAT": ("legacy", "rational"),
        "COMP_EXP": ("legacy", "exp"),
        "BOTH": ("nearlf", "exp"),
    }.items():
        CONTEXT.clear()
        results[name] = run_variant(storms, meta, rm, cm, k_by_basin)

    # 4. Report
    print("=" * 78)
    print("REPRODUCTION: |engine - compiled| > 1.5:", len(repro_bad), repro_bad[:6])
    print("REPLICA CHECK: patched copy vs live path mismatches:", len(replica_bad), replica_bad[:6])
    print("FITTED k:", {k: v for k, v in k_by_basin.items()})
    print()
    hdr = ["storm", "year"] + list(results.keys()) + ["damage($B)"]
    print(("{:<10}{:<6}" + "{:>10}" * (len(results)) + "{:>12}").format(*hdr))
    truth = {}
    for sid in storms:
        t = gt.get(sid)
        if t and t.damage_usd:
            truth[sid] = t.damage_usd
    for sid in anchors:
        if sid not in storms:
            continue
        row = [meta.get(sid, {}).get("name", sid)[:9], meta.get(sid, {}).get("year", "")]
        for v in results.values():
            row.append(round(v.get(sid), 1) if v.get(sid) is not None else "-")
        row.append(round(truth.get(sid, 0) / 1e9, 1) if sid in truth else "-")
        print(("{:<10}{:<6}" + "{:>10}" * len(results) + "{:>12}").format(*row))
    print()
    # Spearman vs damage, per basin group
    for label, pred in [("ATL", lambda b: b == "ATLANTIC"), ("EP", lambda b: b == "EASTERN_PACIFIC"),
                        ("ALL", lambda b: True)]:
        sids = [s for s in truth if s in base and pred(meta.get(s, {}).get("basin", ""))]
        if len(sids) < 4:
            continue
        line = f"Spearman vs damage ({label}, n={len(sids)}):"
        for name, v in results.items():
            xs = [v[s] for s in sids if v.get(s) is not None]
            ys = [truth[s] for s in sids if v.get(s) is not None]
            line += f"  {name}={spearman(xs, ys):+.3f}"
        print(line)
    # 99-ties
    for name, v in results.items():
        ties = sum(1 for x in v.values() if x is not None and x >= 98.95)
        print(f"{name}: storms at 99-ceiling: {ties}")
    # biggest movers BOTH vs BASELINE
    movers = sorted(
        ((sid, base[sid], results["BOTH"][sid]) for sid in base
         if base[sid] is not None and results["BOTH"].get(sid) is not None),
        key=lambda t: -abs(t[2] - t[1]))[:12]
    print("\nBiggest movers (BOTH - BASELINE):")
    for sid, b, n in movers:
        print(f"  {meta.get(sid, {}).get('name', sid):<12}{meta.get(sid, {}).get('year','')}"
              f"  {b:6.1f} -> {n:6.1f}  ({n - b:+.1f})  [{meta.get(sid, {}).get('basin','')}]")


if __name__ == "__main__":
    main()
