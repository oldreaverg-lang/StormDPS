"""
Canonical COAPS / Powell & Reinhold (2007) Integrated Kinetic Energy from
operational wind radii.

Faithful implementation of the *published* operational method — the empirical
regressions in the appendix of:

    Powell, M. D. & Reinhold, T. A. (2007). Tropical cyclone destructive
    potential by integrated kinetic energy. BAMS, 88(4), 513-526.
    DOI: 10.1175/BAMS-88-4-513

This is the same method the NOAA HRD / COAPS IKE calculator uses to estimate
IKE and Surge Destructive Potential (SDP) from advisory wind radii. Unlike a
physical 1/2*rho*v^2 band integration, these are least-squares regressions fit
to 23 H*Wind gridded analyses (r^2 = 0.90-0.95), so they reproduce the
published reference IKE values directly — with NO density constant, band-wind
assumption, size-efficiency correction, or radius cap. The size dependence is
carried by the (positive) quadratic terms in R18/R33, which make large wind
fields grow IKE super-linearly (the opposite of a capped band method).

Inputs (matching the paper's units exactly):
    vms_ms : max sustained 1-min marine surface wind (m/s)
    rmax_km: radius of maximum wind (km)
    r18_km : QUADRANT-AVERAGE radius of 34-kt (18 m/s) winds (km)
    r26_km : quadrant-average radius of 50-kt (26 m/s) winds (km)
    r33_km : quadrant-average radius of 64-kt (33 m/s) winds (km)

Radii are quadrant averages over quadrants that actually report a radius
(quadrants without a radius, or over land, are excluded) per the appendix.
Negative results clamp to 0; SDP caps at 5.99.

CAVEATS (stated by the authors — important for StormDPS):
  * Atlantic-calibrated. The paper explicitly warns other basins "may have
    different wind averaging specifications and different wind field radii
    characteristics, necessitating wind radii relationships tailored to the
    basin." Applying this to W. Pacific storms (e.g. JANGMI) is precisely the
    case the authors caution about.
  * Fit range: R18 <= ~532 km, R33 <= ~217 km (Table 1). Storms larger than the
    fit set (e.g. Sandy 2012) EXTRAPOLATE via the positive quadratics —
    directionally right (big storm -> big IKE) but quantitatively uncertain.
  * Built for storms with hurricane-force winds. For sub-hurricane systems
    (R33 ~ 0) the R33 quadratic in A1 misbehaves; guarded below.
"""
from typing import Optional


def _avg_radii_km(quadrants_km: Optional[dict]) -> Optional[float]:
    """Quadrant-average radius (km), excluding missing/zero quadrants."""
    if not quadrants_km:
        return None
    vals = [v for v in quadrants_km.values() if v and v > 0]
    return sum(vals) / len(vals) if vals else None


def ike_ts_tj(r18_km: float, r33_km: float, vms_ms: float) -> float:
    """IKE for winds >= 18 m/s (34 kt) — the headline surge-proxy IKE. Eq (A1)."""
    if vms_ms < 18.0 or not r18_km:
        return 0.0
    r33 = r33_km or 0.0
    val = (-46.42 + 0.352 * r18_km + 0.0007 * (r18_km - 305.97) ** 2
           + 0.187 * r33 - 0.004 * (r33 - 113.15) ** 2)
    return max(0.0, val)


def ike_h_tj(r18_km: float, r33_km: float, vms_ms: float) -> float:
    """IKE for winds >= 33 m/s (64 kt, hurricane force). Eq (A3)."""
    if vms_ms < 33.0 or not r33_km:
        return 0.0
    val = (-25.2 + 0.238 * vms_ms + 0.023 * (vms_ms - 55.87) ** 2
           + 0.235 * r33_km - 5.5e-4 * (r33_km - 113.15) ** 2
           + 0.025 * (r18_km or 0.0))
    return max(0.0, val)


def ike_25_40_tj(r18_km: float, r26_km: float, vms_ms: float) -> float:
    """IKE for winds 25-40 m/s. Eq (A2)."""
    if vms_ms < 25.0 or not r18_km:
        return 0.0
    val = -23.3 + 0.05 * r18_km + 0.245 * (r26_km or 0.0)
    return max(0.0, val)


def sdp_from_ike_ts(ike_ts: float) -> float:
    """Surge Destructive Potential (0-6) from IKE_TS — P&R 2007 Eq (4).

    More accurate than the direct-from-radii A8 because it chains off the fitted
    IKE_TS; reproduces the Table 1 SDP column.
    """
    if ike_ts <= 0:
        return 0.0
    root = ike_ts ** 0.5
    sdp = 0.676 + 0.43 * root - 0.0176 * (root - 6.5) ** 2
    return min(5.99, max(0.0, sdp))


def sdp_from_radii(r18_km: float, r33_km: float) -> float:
    """Surge Destructive Potential (0-6) directly from radii — Eq (A8)."""
    if not r18_km:
        return 0.0
    r33 = r33_km or 0.0
    sdp = (0.959 + 0.009 * r18_km - 8.88e-6 * (r18_km - 305.98) ** 2
           + 0.005 * r33 - 1.04e-4 * (r33 - 113.15) ** 2)
    return min(5.99, max(0.0, sdp))


def compute_ike_coaps(
    vms_ms: float,
    r34_quadrants_km: Optional[dict] = None,
    r50_quadrants_km: Optional[dict] = None,
    r64_quadrants_km: Optional[dict] = None,
    rmax_km: Optional[float] = None,
) -> dict:
    """Faithful COAPS / P&R-2007 IKE + SDP from quadrant wind radii (km).

    Returns dict: ike_ts_tj, ike_h_tj, ike_25_40_tj, sdp, sdp_radii, r18_km,
    r26_km, r33_km.
    """
    r18 = _avg_radii_km(r34_quadrants_km)
    r26 = _avg_radii_km(r50_quadrants_km)
    r33 = _avg_radii_km(r64_quadrants_km)
    if not r18:
        return {"ike_ts_tj": 0.0, "ike_h_tj": 0.0, "ike_25_40_tj": 0.0,
                "sdp": 0.0, "sdp_radii": 0.0,
                "r18_km": None, "r26_km": None, "r33_km": None}
    ts = ike_ts_tj(r18, r33 or 0.0, vms_ms)
    return {
        "ike_ts_tj": ts,
        "ike_h_tj": ike_h_tj(r18, r33 or 0.0, vms_ms),
        "ike_25_40_tj": ike_25_40_tj(r18, r26 or 0.0, vms_ms),
        "sdp": sdp_from_ike_ts(ts),
        "sdp_radii": sdp_from_radii(r18, r33 or 0.0),
        "r18_km": r18, "r26_km": r26, "r33_km": r33,
    }
