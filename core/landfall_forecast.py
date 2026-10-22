"""
Forecast landfall-window estimation for the public landfall panel.

Given an active storm's forecast track (list of {lat, lon, hour, max_wind_kt,
time} dicts, as served by /storms/{id}/forecast), estimate whether and when
the forecast center reaches a coastline:

  1. Densify each track segment at 1 h steps (linear interpolation).
  2. Distance-to-coast at every sample against core.land_proximity's
     coastline waypoint DB — the same data the DPS scoring path uses, so the
     panel and the score can never disagree about geography. The DB's two
     "open_ocean" REFERENCE points (Central/Western Atlantic) are NOT
     coastline and are excluded from coast matching — without that filter a
     Cape Verde track crossing 10N/50W would "landfall" at "Western Atlantic
     Deep (reference)".
  3. Landfall candidate = the FIRST sample within LANDFALL_KM of a real
     coastline waypoint (the DB is discretized to tens of km, so "at the
     waypoint" is the resolution-honest definition of "at the coast"). If no
     sample gets that close, the track stays offshore: expected=False plus
     the closest-approach metadata so the caller can still say "passes
     ~120 km off Bermuda around +36 h".
  4. Window = ETA ± (climatological track error at that lead time / local
     forward speed) — i.e. how much earlier/later the center could plausibly
     arrive given the official cone — clamped to [±MIN_HALF_WINDOW_H,
     ±MAX_HALF_WINDOW_H] and to the forecast horizon.

COVERAGE: the waypoint DB spans the Atlantic basin (Gulf, US East Coast,
Caribbean, Mexico/Central America — lon ≈ −110..−30) and, since the
WP_DPS_AUDIT_V2 Tranche B ceremony (2026-07), the Western Pacific
(Philippines/Taiwan/Japan/China/Vietnam/Korea/Marianas — lon ≈ 99..146).
For a track wholly outside those envelopes we must NOT claim "stays
offshore" (a Bay of Bengal cyclone about to hit Bangladesh is not
offshore); instead `coverage` is False and the frontend falls back to the
global economic-zone machinery (computeForecastERS), which covers every
basin. Do NOT extend the waypoint DB casually — it feeds the DPS scoring
path (compute_land_proximity_factor and the WP Tranche B distance gates),
so new waypoints change scores and require the basin-audit +
golden-master ceremony.

The output is DESCRIPTIVE ONLY. It feeds the public landfall panel; the
no-protective-action-directives copy policy is enforced at the render layer
(frontend/index.html renderLandfallPanel).
"""

import math
import logging

logger = logging.getLogger(__name__)

# Climatological track-error radii, nm by forecast hour (recent JTWC
# annual-report five-year WestPac means; also a fair NHC proxy at these lead
# times). SINGLE SERVER-SIDE COPY — api/routes.py imports it for JTWC cone
# synthesis. Keep in lockstep with CONE_ERR_NM in frontend/index.html
# (cone-aware forecast ERS sampling).
CONE_ERR_NM = {0: 0, 12: 22, 24: 38, 36: 50, 48: 64, 72: 95, 96: 125, 120: 165}

# "At the coast" for a waypoint DB discretized to tens of km. Matches the
# land_proximity sigmoid's full-amplification knee (<50 km = at coast).
LANDFALL_KM = 50.0

# Window half-width clamps (hours). The floor keeps a near-coast ETA from
# claiming false precision; the ceiling keeps a stalled storm's window from
# swallowing the whole forecast period.
MIN_HALF_WINDOW_H = 3.0
MAX_HALF_WINDOW_H = 18.0

# Forward-speed floor (kt) for the window math so a stall segment (speed→0)
# can't divide the track error into an unbounded window.
MIN_FWD_SPEED_KT = 4.0

# Margin (degrees) around the waypoint DB's bounding box when deciding
# whether a track is inside the covered basin at all. The box is computed
# over ALL waypoints including the open-ocean references — they deliberately
# widen it so mid-Atlantic fish storms stay in-coverage and get an honest
# "stays offshore".
COVERAGE_MARGIN_DEG = 5.0

# East-Pacific gap: the waypoint DB has only two Baja points on the Pacific
# side of the Americas, so this estimator supplements it with
# core.pacific_coast (mainland Pacific Mexico, Baja, Central America, Hawaii)
# — the DB itself feeds DPS scoring and is not extended here. A closest
# approach beyond COASTAL_GAP_KM on the Pacific side (west of
# PACIFIC_GAP_LON) is still reported as "no coastline data", not "offshore".
COASTAL_GAP_KM = 500.0
PACIFIC_GAP_LON = -85.0

# Indian-Ocean gap [Tranche B]: with WP waypoints in the DB the coverage
# bounding box now spans the whole Indian Ocean, but there are no NI/Arabian
# Sea coastline waypoints. A closest approach far from the westernmost WP
# points (Gulf of Thailand, ~99.2E) at eastern longitudes below
# INDIAN_GAP_LON_MAX is "no coastline data", never "offshore".
INDIAN_GAP_LON_MAX = 99.0


def interp_err_nm(hour: float) -> float:
    """Linear interpolation of CONE_ERR_NM between tabulated lead times."""
    keys = sorted(CONE_ERR_NM)
    if hour <= keys[0]:
        return CONE_ERR_NM[keys[0]]
    if hour >= keys[-1]:
        return CONE_ERR_NM[keys[-1]]
    for i in range(1, len(keys)):
        if hour <= keys[i]:
            h0, h1 = keys[i - 1], keys[i]
            f = (hour - h0) / (h1 - h0)
            return CONE_ERR_NM[h0] + f * (CONE_ERR_NM[h1] - CONE_ERR_NM[h0])
    return CONE_ERR_NM[keys[-1]]


def _haversine_nm(lat1, lon1, lat2, lon2):
    r_nm = 3440.065  # earth radius, nautical miles
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r_nm * math.asin(min(1.0, math.sqrt(a)))


def _densify(points):
    """1 h linear interpolation between forecast fixes.

    Returns list of (hour, lat, lon, wind_kt) with wind_kt possibly None
    when neither bracketing fix carries an intensity.
    """
    samples = []
    for a, b in zip(points, points[1:]):
        h0, h1 = float(a.get("hour") or 0), float(b.get("hour") or 0)
        if h1 <= h0:
            continue
        w0 = a.get("max_wind_kt")
        w1 = b.get("max_wind_kt")
        h = h0
        while h < h1:
            f = (h - h0) / (h1 - h0)
            lat = a["lat"] + f * (b["lat"] - a["lat"])
            lon = a["lon"] + f * (b["lon"] - a["lon"])
            if w0 is not None and w1 is not None:
                wk = float(w0) + f * (float(w1) - float(w0))
            else:
                wk = float(w1) if w1 is not None else (float(w0) if w0 is not None else None)
            samples.append((h, lat, lon, wk))
            h += 1.0
    if points:
        p = points[-1]
        samples.append((float(p.get("hour") or 0), p["lat"], p["lon"],
                        float(p["max_wind_kt"]) if p.get("max_wind_kt") is not None else None))
    return samples


def _forward_speed_kt(points, eta_hour):
    """Implied forward speed (kt) of the segment containing eta_hour."""
    for a, b in zip(points, points[1:]):
        h0, h1 = float(a.get("hour") or 0), float(b.get("hour") or 0)
        if h1 <= h0:
            continue
        if h0 <= eta_hour <= h1:
            dist = _haversine_nm(a["lat"], a["lon"], b["lat"], b["lon"])
            return dist / (h1 - h0)
    return 10.0  # typical TC translation speed when the segment is degenerate


def _nearest_coastal_wp(lat, lon, coastal_wps):
    """Nearest REAL-coastline waypoint (open-ocean refs pre-filtered out).

    Own scan instead of land_proximity.compute_distance_to_coast because
    that helper matches ALL waypoints including the open-ocean references.
    Returns (waypoint, distance_km).
    """
    best, best_d = None, float("inf")
    for w in coastal_wps:
        d = _haversine_nm(lat, lon, w.lat, w.lon) * 1.852
        if d < best_d:
            best_d, best = d, w
    return best, best_d


# Pseudo region keys for the supplement points (not COASTAL_PROFILES keys).
_US_TOWN = "us_coastal_place"
_CANADA = "atlantic_canada"


def _profile_region(wp):
    """The COASTAL_PROFILES region for a landfall at this point: the
    waypoint's own key, the region a fix at a US town is scored against, and
    None for Atlantic Canada (no profile)."""
    if wp is None or wp.region_key == _CANADA:
        return None
    if wp.region_key == _US_TOWN:
        try:
            from core.cumulative_dpi import _region_at
            return _region_at(wp.lat, wp.lon)
        except Exception:  # pragma: no cover
            return None
    return wp.region_key


def _wp_display_name(wp):
    """Display name for a coastline waypoint: the real town at its
    coordinates where core.coastal_places covers the area (the waypoint's own
    label is often a different town tens of km away — see that module),
    else the label."""
    if wp is None:
        return None
    try:
        from core.coastal_places import place_name
        return place_name(wp.lat, wp.lon, fallback=wp.name)
    except Exception:  # pragma: no cover — naming must never break the estimate
        return wp.name


def _landfall_display_name(profile, eta_h, hit_wp):
    """Name the landfall by the real town nearest the point where the track
    passes closest to the coast point it is heading for. The "hit" sample is
    up to LANDFALL_KM offshore, so naming from it (or from the waypoint's
    label) can be a town or two off; the closest pass over the next few hours
    is where the centre actually comes ashore."""
    if hit_wp is None:
        return None
    try:
        from core.coastal_places import place_name
        ahead = [r for r in profile if eta_h <= r[0] <= eta_h + 8.0]
        if ahead:
            _h, lat, lon, _wk, _d, _wp = min(
                ahead, key=lambda r: _haversine_nm(r[1], r[2], hit_wp.lat, hit_wp.lon))
            name = place_name(lat, lon)
            if name:
                return name
    except Exception:  # pragma: no cover
        pass
    return _wp_display_name(hit_wp)


def anchor_track(forecast_track, cur_lat, cur_lon, hours_after_tau0, wind_kt=None):
    """The forecast track restarted at a NEWER official position.

    A full advisory's track starts at its valid time (tau0). NHC then issues
    intermediate advisories with a fresh centre every 3 h (hourly near
    landfall) but no new track. When that newer position has drifted off the
    forecast line, landfall worked out from the old line is in the wrong
    place: Isaias 2026 was at 30.1N 86.6W at 00Z, 30 km east of the 21Z line,
    heading for Fort Walton Beach while the 21Z line crossed the coast near
    Navarre. This drops the forecast points at or before the newer time and
    starts from the current position; hours stay counted from tau0 so every
    consumer's "hours from now" maths is unchanged.

    Returns the original track when there is nothing newer to anchor on
    (missing position, under 30 min or over 12 h after tau0, or no forecast
    point left after it).
    """
    try:
        h = float(hours_after_tau0)
        la, lo = float(cur_lat), float(cur_lon)
    except (TypeError, ValueError):
        return forecast_track
    pts = sorted((p for p in (forecast_track or [])
                  if p.get("lat") is not None and p.get("lon") is not None),
                 key=lambda p: float(p.get("hour") or 0))
    later = [p for p in pts if float(p.get("hour") or 0) > h + 0.5]
    if not (0.5 <= h <= 12.0) or not later:
        return forecast_track
    first = {"hour": round(h, 2), "lat": la, "lon": lo}
    if wind_kt is not None:
        first["max_wind_kt"] = wind_kt
    else:
        prev = [p for p in pts if float(p.get("hour") or 0) <= h]
        w = (prev[-1] if prev else later[0]).get("max_wind_kt")
        if w is not None:
            first["max_wind_kt"] = w
    return [first] + later


def compute_forecast_landfall(forecast_track) -> dict:
    """Estimate the forecast landfall window from a forecast track.

    Returns a dict that is ALWAYS safe to serialize:
      expected            bool — center forecast to reach the coast
      eta_hour            forecast hour of first coastal arrival (0 = now)
      window_start_hour / window_end_hour — cone-error timing window
      lat / lon           interpolated arrival point
      nearest_name        human-readable nearest coastline waypoint
      region_key          COASTAL_PROFILES key at the arrival point
      wind_kt             interpolated intensity at arrival (may be None)
      min_distance_km / closest_hour — closest approach (also set when
                          expected=False so "passes offshore" is sayable)
      description         one descriptive sentence (no directives)
    """
    base = {
        "expected": False, "coverage": True,
        "eta_hour": None, "window_start_hour": None, "window_end_hour": None,
        "lat": None, "lon": None, "nearest_name": None, "region_key": None,
        "wind_kt": None, "min_distance_km": None, "closest_hour": None,
        "closest_wind_kt": None,
        "description": "",
    }
    pts = [p for p in (forecast_track or [])
           if p.get("lat") is not None and p.get("lon") is not None]
    pts.sort(key=lambda p: float(p.get("hour") or 0))
    if len(pts) < 2:
        return base

    try:
        from core.land_proximity import _get_coastline_db
    except Exception as e:  # pragma: no cover — import failure = fail open
        logger.warning(f"[landfall] land_proximity unavailable: {e}")
        return base

    # Basin-coverage gate: a track wholly outside the waypoint DB's bounding
    # box (± margin) is in a basin we have no coastline data for. Say so
    # instead of misreporting a WP/IO/SH storm as "offshore".
    db = _get_coastline_db()
    wps = list(db.waypoints)
    try:
        from core.land_proximity import CoastlineWaypoint
        from core.pacific_coast import PACIFIC_COAST_POINTS
        wps += [CoastlineWaypoint(la, lo, "pacific_coast", 0.0, nm)
                for la, lo, nm in PACIFIC_COAST_POINTS]
    except Exception as e:  # pragma: no cover — supplement is best-effort
        logger.warning(f"[landfall] Pacific coast supplement unavailable: {e}")
    # The US coastal towns and Atlantic Canada (2026-10-21). With waypoints
    # alone, Subtropical Storm Josephine AL102026 passing ~365 km south of
    # Nantucket read "closest approach ~440 km from New London, CT", and a
    # storm bound for Nova Scotia or Newfoundland could only ever "stay
    # offshore". The scoring path's land grid (core/coast_distance.py) includes
    # these points too.
    try:
        from core.land_proximity import CoastlineWaypoint
        from core.coastal_places import US_COASTAL_PLACES
        from core.atlantic_canada import ATLANTIC_CANADA_POINTS
        wps += [CoastlineWaypoint(la, lo, _US_TOWN, 0.0, nm) for la, lo, nm in US_COASTAL_PLACES]
        wps += [CoastlineWaypoint(la, lo, _CANADA, 0.0, nm) for la, lo, nm in ATLANTIC_CANADA_POINTS]
    except Exception as e:  # pragma: no cover — supplement is best-effort
        logger.warning(f"[landfall] town / Canada supplement unavailable: {e}")
    coastal = [w for w in wps if w.region_key != "open_ocean"]
    if not wps or not coastal:
        return base
    lat_min = min(w.lat for w in wps) - COVERAGE_MARGIN_DEG
    lat_max = max(w.lat for w in wps) + COVERAGE_MARGIN_DEG
    lon_min = min(w.lon for w in wps) - COVERAGE_MARGIN_DEG
    lon_max = max(w.lon for w in wps) + COVERAGE_MARGIN_DEG
    if not any(lat_min <= p["lat"] <= lat_max and lon_min <= p["lon"] <= lon_max
               for p in pts):
        base["coverage"] = False
        return base

    samples = _densify(pts)
    if not samples:
        return base

    profile = []
    for (h, lat, lon, wk) in samples:
        wp, d_km = _nearest_coastal_wp(lat, lon, coastal)
        profile.append((h, lat, lon, wk, d_km, wp))

    min_i = min(range(len(profile)), key=lambda i: profile[i][4])
    min_h, _min_lat, min_lon, _min_wk, min_d, min_wp = profile[min_i]
    base["min_distance_km"] = round(min_d, 1)
    base["closest_hour"] = round(min_h)
    base["closest_wind_kt"] = round(_min_wk) if _min_wk is not None else None
    base["nearest_name"] = _wp_display_name(min_wp)

    # East-Pacific gap (see constants above): far from every known coastline
    # on the Pacific side means "no data", never "offshore".
    if min_d > COASTAL_GAP_KM and min_lon < PACIFIC_GAP_LON:
        base["coverage"] = False
        base["nearest_name"] = None
        return base

    # Indian-Ocean gap (see constants above): eastern-longitude tracks west
    # of the Gulf-of-Thailand waypoints with no close coastline are in the
    # un-waypointed NI basin, not "offshore".
    if min_d > COASTAL_GAP_KM and 0.0 <= min_lon < INDIAN_GAP_LON_MAX:
        base["coverage"] = False
        base["nearest_name"] = None
        return base

    hit = next((row for row in profile if row[4] <= LANDFALL_KM), None)
    if hit is None:
        base["description"] = (
            f"Forecast track stays offshore through +{round(profile[-1][0])} h "
            f"(closest approach ~{round(min_d)} km at +{round(min_h)} h)."
        )
        return base

    eta_h, lat, lon, wk, dist_km, hit_wp = hit
    last_h = profile[-1][0]

    spd = max(_forward_speed_kt(pts, eta_h), MIN_FWD_SPEED_KT)
    half = min(MAX_HALF_WINDOW_H, max(MIN_HALF_WINDOW_H, interp_err_nm(eta_h) / spd))
    w_start = max(0.0, eta_h - half)
    w_end = min(last_h, eta_h + half)

    base.update({
        "expected": True,
        "eta_hour": round(eta_h),
        "window_start_hour": round(w_start),
        "window_end_hour": round(w_end),
        "lat": round(lat, 2), "lon": round(lon, 2),
        "nearest_name": _landfall_display_name(profile, eta_h, hit_wp),
        "region_key": _profile_region(hit_wp),
        "wind_kt": round(wk) if wk is not None else None,
    })
    nearest_name = base["nearest_name"]
    where = f" near {nearest_name}" if nearest_name else ""

    if eta_h <= 0:
        base["description"] = f"Forecast center is at or near the coast{where} now."
    else:
        base["description"] = (
            f"Forecast center reaches the coast{where} around +{round(eta_h)} h "
            f"(window +{round(w_start)} to +{round(w_end)} h, based on the "
            f"official track-error cone)."
        )
    return base


def _point_in_polygon(lat: float, lon: float, poly) -> bool:
    """Ray casting on (lat, lon) pairs; the cone never spans the dateline."""
    inside = False
    j = len(poly) - 1
    for i in range(len(poly)):
        yi, xi = poly[i][0], poly[i][1]
        yj, xj = poly[j][0], poly[j][1]
        if (yi > lat) != (yj > lat):
            x_cross = xi + (lat - yi) * (xj - xi) / (yj - yi)
            if lon < x_cross:
                inside = not inside
        j = i
    return inside


def cone_reaches_coast(cone_polygon) -> bool:
    """True when any known coastal point lies inside the forecast cone.

    A center line that stays offshore is not the same as "no landfall": Trudy
    EP212026 (2026-10-14) was forecast to pass ~80 km off Jalisco as a major
    hurricane, with the cone well over the coast, and the page only said
    "Stays offshore". Coastal points: the waypoint DB, the Pacific supplement,
    the US coastal towns and Atlantic Canada.
    """
    poly = [p for p in (cone_polygon or []) if p and len(p) >= 2]
    if len(poly) < 3:
        return False
    lats = [p[0] for p in poly]
    lons = [p[1] for p in poly]
    la0, la1, lo0, lo1 = min(lats), max(lats), min(lons), max(lons)
    points = []
    try:
        from core.land_proximity import _get_coastline_db
        points += [(w.lat, w.lon) for w in _get_coastline_db().waypoints
                   if w.region_key != "open_ocean"]
    except Exception:  # pragma: no cover
        pass
    try:
        from core.pacific_coast import PACIFIC_COAST_POINTS
        points += [(la, lo) for la, lo, _ in PACIFIC_COAST_POINTS]
    except Exception:  # pragma: no cover
        pass
    try:
        from core.coastal_places import US_COASTAL_PLACES
        points += [(la, lo) for la, lo, _ in US_COASTAL_PLACES]
    except Exception:  # pragma: no cover
        pass
    try:
        from core.atlantic_canada import ATLANTIC_CANADA_POINTS
        points += [(la, lo) for la, lo, _ in ATLANTIC_CANADA_POINTS]
    except Exception:  # pragma: no cover
        pass
    return any(la0 <= la <= la1 and lo0 <= lo <= lo1 and _point_in_polygon(la, lo, poly)
               for la, lo in points)
