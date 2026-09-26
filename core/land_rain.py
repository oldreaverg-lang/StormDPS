"""Forecast rainfall OVER LAND for an active storm — the rain bar's data.

The rain bar used to be driven by the kinematic rainfall estimate
(core.rainfall_warning: intensity x size x forward speed along the TRACK) and
was only shown on a forecast landfall or near-land stall. That measures rain
the storm produces, not rain that falls on anyone, so an offshore storm could
not trigger it — Nolo (EP152026, Sep 2026) sat ~150 mi south of the Big Island
while CPHC forecast 8-12 in (max 16 in) of rain there and the bar stayed off.

Three sources, best first, all basins:

  1. The official NHC / CPHC public advisory "RAINFALL:" section (AL/EP/CP):
     per-area inch ranges written by the forecasters. Official silence (no
     RAINFALL section) means no land-rain threat — nothing else is consulted.
  2. NWS forecast grids (api.weather.gov quantitativePrecipitation) at named
     US places near the forecast track — CONUS, Hawaii, Puerto Rico/USVI,
     Guam/CNMI. Town-level numbers; detail under an advisory, headline for a
     storm near US land without one.
  3. Global model guidance (Open-Meteo "best_match", free non-commercial tier,
     CC BY 4.0 attribution) at named places near the track — JTWC basins
     (WP/IO/SH) publish no rainfall statement. Coarse models understate
     terrain rain (Hilo, Nolo: model 5.1 in vs NWS 10.4 in vs CPHC 8-12 in),
     and the text says so.

This module is the pure part (stdlib only, CI-testable): parsing, place
selection, QPF summing and the bar's wording. services/land_rain_client does
the fetching and caching. Wording describes the hazard and credits the source;
it never tells anyone what to do (StormDPS is not an emergency authority).
"""
from __future__ import annotations

import html as _html
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

HORIZON_H = 72            # points sum the next 3 days of forecast rain
RADIUS_KM = 400.0         # places this close to the 0-72 h track are sampled
MAX_PLACES = 10
SHOW_MIN_IN = 4.0         # below this the bar stays hidden

# (min inches, level) — highest first. Levels reuse the bar's existing names.
LEVELS = ((25.0, "HISTORIC"), (15.0, "EXTREME"), (8.0, "HIGH"), (SHOW_MIN_IN, "ELEVATED"))
LEVEL_WORD = {
    "ELEVATED": "Heavy rain",
    "HIGH": "Flooding rain",
    "EXTREME": "Extreme rain",
    "HISTORIC": "Historic rain",
}
# Generic hazard sentences for grid/model sources (the advisory brings its own).
GENERIC_IMPACT = {
    "ELEVATED": "Flash flooding is possible in low-lying and urban areas.",
    "HIGH": "Flash flooding and mudslides are possible, especially in steep terrain.",
    "EXTREME": "Life-threatening flash flooding and mudslides are possible.",
    "HISTORIC": "Catastrophic, life-threatening flooding is possible.",
}
MODEL_CAVEAT = "global models understate rain over mountains"
MODEL_ATTRIBUTION = "model guidance via Open-Meteo.com (CC BY 4.0)"

# Rain-relevant places the coastline waypoint lists lack (windward / south
# Big Island and east Maui take the heaviest orographic rain).
EXTRA_PLACES: Tuple[Tuple[float, float, str], ...] = (
    (19.49, -154.95, "Pahoa, HI"),
    (19.06, -155.59, "Naalehu, HI"),
    (20.08, -155.47, "Honokaa, HI"),
    (20.76, -155.99, "Hana, HI"),
)

# Places the NWS grids cover (lat_min, lat_max, lon_min, lon_max). A place in a
# box but outside NWS land (Bahamas, northern Mexico) simply 404s at /points
# and falls back to the model.
_US_BOXES = (
    (24.0, 50.0, -125.0, -66.5),     # CONUS
    (18.5, 22.6, -160.8, -154.5),    # Hawaii
    (17.5, 18.7, -67.6, -64.3),      # Puerto Rico + USVI
    (13.0, 20.8, 144.4, 146.3),      # Guam + CNMI
)


def is_us_place(lat: float, lon: float) -> bool:
    return any(a <= lat <= b and c <= lon <= d for a, b, c, d in _US_BOXES)


def level_for(max_in: Optional[float]) -> Optional[str]:
    if max_in is None:
        return None
    for floor, name in LEVELS:
        if max_in >= floor:
            return name
    return None


def fmt_in(v: float) -> str:
    """16.0 -> '16', 10.44 -> '10.4'."""
    r = round(float(v), 1)
    return str(int(r)) if r == int(r) else f"{r:g}"


# ---------------------------------------------------------------------------
# 1. Official advisory
# ---------------------------------------------------------------------------

def advisory_text(raw: str) -> str:
    """Plain text of an NHC .shtml product (the <pre> block, tags and
    entities removed). Plain text passes through."""
    m = re.search(r"<pre[^>]*>(.*?)</pre>", raw or "", re.S | re.I)
    body = m.group(1) if m else (raw or "")
    body = re.sub(r"<[^>]+>", "", body)
    return _html.unescape(body).replace("\r\n", "\n")


def issuing_center(text: str) -> Optional[str]:
    # CPHC first: CPHC products issued by NHC as backup name both centers.
    if re.search(r"Central Pacific Hurricane Center", text, re.I):
        return "CPHC"
    if re.search(r"National Hurricane Center", text, re.I):
        return "NHC"
    return None


def advisory_label(text: str) -> Optional[str]:
    m = re.search(r"Advisory Number\s+([0-9]+[A-Z]?)", text, re.I)
    return f"Advisory {m.group(1)}" if m else None


# End of the RAINFALL section: the next labelled hazard paragraph, or the
# product's closing lines.
_SECTION_END = re.compile(
    r"\n\s*\n\s*(?:[A-Z][A-Z /-]{2,}:|NEXT ADVISORY|\$\$|Forecaster\b)")
_ABBREV = re.compile(r"\b(U\.S|St|Mt|Ft|Pt|Is)\.")
_RANGE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:to|-|–)\s*(\d+(?:\.\d+)?)\s*inch", re.I)
_UPTO = re.compile(r"(?:up to|around|near|about)\s+(\d+(?:\.\d+)?)\s*inch", re.I)
_MAX = re.compile(
    r"\b(?:maximum|isolated|locally|local|storm[- ]total)\b[^.;]*?"
    r"(\d+(?:\.\d+)?)\s*inch", re.I)
_LEAD_AREA = re.compile(r"^(?:For|Across|Over|In)\s+(?:the\s+)?([^,]{3,120}),", re.I)
_TAIL_AREA = re.compile(
    r"\b(?:across|over|in|for|along)\s+(?:the\s+)?(.{3,120}?)"
    r"(?=\s*(?:\.|;|,|$|\bthrough\b|\binto\b|\bfrom\b|\bduring\b|\bthis\b|"
    r"\btonight\b|\btoday\b|\bby\b|\bon\b|\bwith\b|\bare\b|\bis\b))", re.I)
_DIRECTIVE = re.compile(
    r"\b(should|must|evacuat\w*|follow|heed|urge\w*|advis\w*|prepare|"
    r"stay|take action|seek)\b", re.I)


def rainfall_section(text: str) -> Optional[str]:
    i = text.find("RAINFALL:")
    if i < 0:
        return None
    sec = text[i + len("RAINFALL:"):]
    m = _SECTION_END.search(sec)
    return sec[: m.start()] if m else sec[:2500]


def _sentences(block: str) -> List[str]:
    flat = " ".join(block.split())
    flat = _ABBREV.sub(lambda m: m.group(0).replace(".", "§"), flat)
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z])", flat)
    return [p.replace("§", ".").strip() for p in parts if p.strip()]


def _clean_area(a: str) -> str:
    a = re.sub(r"^(?:the)\s+", "", a.strip(), flags=re.I)
    return a.rstrip(" .,;")


def parse_rainfall(text: str) -> dict:
    """{'areas': [{area, lo_in, hi_in, max_in}], 'impact': str|None}.
    areas is empty when the advisory has no RAINFALL section or no amounts."""
    sec = rainfall_section(text)
    out = {"areas": [], "impact": None}
    if not sec:
        return out
    for s in _sentences(sec):
        rng = _RANGE.search(s)
        lo = hi = None
        if rng:
            lo, hi = float(rng.group(1)), float(rng.group(2))
        else:
            up = _UPTO.search(s)
            if up:
                hi = float(up.group(1))
        if hi is None:
            if out["impact"] is None and re.search(r"flood", s, re.I) and not _DIRECTIVE.search(s):
                out["impact"] = s
            continue
        after = s[rng.end():] if rng else s
        mx = _MAX.search(after)
        max_in = max(hi, float(mx.group(1))) if mx else hi
        area = None
        lead = _LEAD_AREA.match(s)
        if lead:
            area = lead.group(1)
        else:
            tail_from = (mx.end() if mx else 0)
            tail = _TAIL_AREA.search(after[tail_from:]) or _TAIL_AREA.search(after)
            if tail:
                area = tail.group(1)
        out["areas"].append({
            "area": _clean_area(area) if area else None,
            "lo_in": lo, "hi_in": hi, "max_in": max_in,
        })
    return out


# ---------------------------------------------------------------------------
# 2/3. Places near the forecast track + QPF sums
# ---------------------------------------------------------------------------

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(min(1.0, math.sqrt(a)))


def _track_samples(track: Sequence[dict], horizon_h: float, step_h: float = 3.0) -> List[Tuple[float, float]]:
    pts = sorted(
        (p for p in track or [] if p.get("lat") is not None and p.get("lon") is not None),
        key=lambda p: float(p.get("hour") or 0))
    pts = [p for p in pts if float(p.get("hour") or 0) <= horizon_h] or pts[:1]
    out: List[Tuple[float, float]] = []
    for a, b in zip(pts, pts[1:]):
        h0, h1 = float(a.get("hour") or 0), float(b.get("hour") or 0)
        la0, lo0, la1, lo1 = float(a["lat"]), float(a["lon"]), float(b["lat"]), float(b["lon"])
        if lo1 - lo0 > 180:          # antimeridian: interpolate the short way
            lo1 -= 360
        elif lo0 - lo1 > 180:
            lo1 += 360
        n = max(1, int(math.ceil((h1 - h0) / step_h))) if h1 > h0 else 1
        for i in range(n):
            f = i / n
            lo = lo0 + (lo1 - lo0) * f
            out.append((la0 + (la1 - la0) * f, ((lo + 180) % 360) - 180))
    if pts:
        out.append((float(pts[-1]["lat"]), float(pts[-1]["lon"])))
    return out


def select_places(track: Sequence[dict], places: Iterable[Tuple[float, float, str]],
                  radius_km: float = RADIUS_KM, horizon_h: float = HORIZON_H,
                  limit: int = MAX_PLACES) -> List[dict]:
    """Named places within radius_km of the 0-horizon_h forecast track,
    nearest first, de-duplicated by name."""
    samples = _track_samples(track, horizon_h)
    if not samples:
        return []
    seen, hits = set(), []
    for la, lo, name in places:
        if name in seen:
            continue
        d = min(haversine_km(la, lo, s_la, s_lo) for s_la, s_lo in samples)
        if d <= radius_km:
            seen.add(name)
            hits.append({"name": name, "lat": la, "lon": lo, "dist_km": round(d)})
    hits.sort(key=lambda h: h["dist_km"])
    return hits[:limit]


_DUR = re.compile(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?)?")


def _parse_valid_time(v: str) -> Optional[Tuple[datetime, float]]:
    """'2026-09-26T02:00:00+00:00/PT4H' -> (start, hours)."""
    try:
        start_s, dur_s = v.split("/")
        start = datetime.fromisoformat(start_s.replace("Z", "+00:00"))
        m = _DUR.fullmatch(dur_s)
        if not m:
            return None
        hours = int(m.group(1) or 0) * 24 + int(m.group(2) or 0) + int(m.group(3) or 0) / 60
        return (start if start.tzinfo else start.replace(tzinfo=timezone.utc)), hours
    except (ValueError, AttributeError):
        return None


def sum_nws_qpf(values: Sequence[dict], now: datetime, horizon_h: float = HORIZON_H) -> Optional[float]:
    """Inches of forecast rain in [now, now+horizon] from an api.weather.gov
    quantitativePrecipitation series (mm per interval), pro-rating intervals
    that straddle either edge. None when the series is empty."""
    end = now + timedelta(hours=horizon_h)
    total, any_val = 0.0, False
    for v in values or []:
        vt = _parse_valid_time(v.get("validTime", ""))
        if not vt or v.get("value") is None:
            continue
        start, hours = vt
        if hours <= 0:
            continue
        stop = start + timedelta(hours=hours)
        overlap = (min(stop, end) - max(start, now)).total_seconds() / 3600
        if overlap <= 0:
            continue
        any_val = True
        total += float(v["value"]) * overlap / hours
    return round(total / 25.4, 2) if any_val else None


def sum_hourly(times: Sequence[str], values: Sequence[Optional[float]], now: datetime,
               horizon_h: float = HORIZON_H) -> Optional[float]:
    """Inches in [now's hour, +horizon) from Open-Meteo hourly precipitation
    (mm, GMT timestamps)."""
    start = now.replace(minute=0, second=0, microsecond=0)
    end = start + timedelta(hours=horizon_h)
    total, any_val = 0.0, False
    for t, v in zip(times or [], values or []):
        try:
            ts = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if start <= ts < end and v is not None:
            total += float(v)
            any_val = True
    return round(total / 25.4, 2) if any_val else None


# ---------------------------------------------------------------------------
# The bar
# ---------------------------------------------------------------------------

def _points_phrase(points: Sequence[dict], n: int = 3) -> str:
    top = [p for p in points if (p.get("total_in") or 0) >= 1.0][:n]
    bits = [f"{p['name']} {fmt_in(p['total_in'])} in" for p in top]
    return ", ".join(bits)


def build(advisory: Optional[dict], points: Sequence[dict], points_source: Optional[str]) -> dict:
    """Assemble the bar payload.

    advisory: {'center', 'label', 'issued_utc', 'areas', 'impact'} or None
              (None = no advisory for this storm or it could not be read).
    points:   [{name, lat, lon, dist_km, total_in}] sorted by total desc.
    points_source: 'nws' | 'model' | 'mixed' | None.
    """
    points = sorted((p for p in points if p.get("total_in") is not None),
                    key=lambda p: -p["total_in"])
    res = {
        "available": True, "show": False, "source": "none", "level": None,
        "max_in": None, "score_text": None, "text": None,
        "areas": [], "points": points, "points_source": points_source,
        "center": None, "product": None, "issued_utc": None,
        "hours": HORIZON_H, "attribution": None,
    }

    if advisory is not None:
        # Official product read: it alone decides whether there is a land
        # rain threat (no RAINFALL section = none). Grid points are detail.
        areas = sorted(advisory.get("areas") or [], key=lambda a: -(a.get("max_in") or 0))
        res.update(source="advisory", center=advisory.get("center"),
                   product=advisory.get("label"), issued_utc=advisory.get("issued_utc"),
                   areas=areas)
        if not areas:
            return res
        max_in = max(a["max_in"] for a in areas)
        level = level_for(max_in)
        res.update(max_in=max_in, level=level)
        if not level:
            return res
        parts = []
        for a in [a for a in areas if (a.get("max_in") or 0) >= 3][:3]:
            amt = (f"{fmt_in(a['lo_in'])}–{fmt_in(a['hi_in'])} in" if a.get("lo_in") is not None
                   else f"up to {fmt_in(a['hi_in'])} in")
            if a["max_in"] > (a.get("hi_in") or 0):
                amt += f" (locally {fmt_in(a['max_in'])} in)"
            parts.append((f"{a['area']} " if a.get("area") else "") + amt)
        text = f"{LEVEL_WORD[level]} forecast: " + "; ".join(parts) + "."
        impact = advisory.get("impact") or GENERIC_IMPACT[level]
        text += " " + impact
        if points_source == "nws" and _points_phrase(points):
            text += f" NWS forecast, next 3 days: {_points_phrase(points)}."
        src = " ".join(x for x in (advisory.get("center"), advisory.get("label")) if x)
        text += f" — {src or 'official advisory'}"
        res.update(show=True, text=text, score_text=f"Rain up to {fmt_in(max_in)} in —",
                   attribution=src or None)
        return res

    if not points:
        res["available"] = bool(points_source)
        return res
    max_in = points[0]["total_in"]
    level = level_for(max_in)
    res.update(source=points_source, max_in=max_in, level=level)
    if not level:
        return res
    top = [p for p in points if p["total_in"] >= 1.0][:3]
    listed = ", ".join(f"{fmt_in(p['total_in'])} in at {p['name']}" for p in top)
    if points_source == "model":
        src, caveat = MODEL_ATTRIBUTION, f" ({MODEL_CAVEAT})"
    elif points_source == "mixed":
        src, caveat = "NWS forecast grids + " + MODEL_ATTRIBUTION, f" ({MODEL_CAVEAT})"
    else:
        src, caveat = "NWS forecast grids", ""
    text = (f"{LEVEL_WORD[level]} forecast: up to {listed} over the next 3 days. "
            f"{GENERIC_IMPACT[level]} — {src}{caveat}")
    res.update(show=True, text=text, score_text=f"Rain up to {fmt_in(max_in)} in —",
               attribution=src)
    return res
