"""Where, when and how hard a storm came ashore. Display only, never scoring.

The score payload's `landfalls` list is scoring machinery: coastal-box
events at the peak wind inside a box (Opal 1995: 120 kt at a point 190 km
offshore; Katrina: 125 kt five hours before Buras). A reader comparing two
landfalls needs the landfall itself, so this module reads it from the best
track, best source first:

  official   The agency's own landfall record. NHC publishes one per landfall
             (IBTrACS USA_RECORD "L"): the time to the minute and the analysed
             wind and pressure at the coast.
  crossing   IBTrACS's over-land flag for every other agency: the fix where
             DIST2LAND goes from >0 to 0, interpolated back to the coast
             between the two fixes (3 h apart at most).
  estimate   A storm too recent to be in IBTrACS (a live one): the closest
             pass of the track to a known coastal point. An estimate, and
             labelled as one, until the best track publishes the record.

Stdlib only; the coastline points are passed in.
"""
from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

MIN_WIND_KT = 25.0        # crossings / estimates below this are remnant noise
NEAR_KM = 50.0            # a live track is "at the coast" inside this distance of a coast point
HIT_KM = 25.0             # ...and came ashore if it passed this close
STEP_MIN = 10             # densification step for the estimate

CoastPoint = Tuple[float, float, str]

SID_RE = re.compile(r"^[0-9]{7}[NS][0-9]{5}$")
ATCF_RE = re.compile(r"^[A-Z]{2}[0-9]{6}$")


def _f(v) -> Optional[float]:
    try:
        x = float(str(v).strip())
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0088 * math.asin(min(1.0, math.sqrt(h)))


def _norm_lon(lon: float) -> float:
    return ((lon + 180.0) % 360.0) - 180.0


def _parse_time(text) -> Optional[datetime]:
    s = str(text or "").strip().replace("T", " ").replace("Z", "")
    if not s:
        return None
    s = s.split("+")[0].strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _iso(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _round10(t: datetime) -> datetime:
    """An interpolated time is not known to the second: nearest 10 minutes."""
    secs = t.minute * 60 + t.second + t.microsecond / 1e6
    return t.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=10 * int(secs / 600.0 + 0.5))


def _lerp(a: Optional[float], b: Optional[float], f: float) -> Optional[float]:
    if a is None:
        return b
    if b is None:
        return a
    return a + (b - a) * f


def _landfall(t: datetime, lat: float, lon: float, wind, pres, kind: str) -> dict:
    return {
        "time_utc": _iso(t),
        "lat": round(lat, 2),
        "lon": round(_norm_lon(lon), 2),
        "wind_kt": None if wind is None else int(round(wind)),
        "pressure_mb": None if pres is None else int(round(pres)),
        "kind": kind,
    }


# ── IBTrACS ────────────────────────────────────────────────────────────────

def scan_ibtracs(path, sid: Optional[str] = None, atcf: Optional[str] = None) -> List[dict]:
    """One storm's rows from an IBTrACS CSV on disk, as dicts keyed by header.

    A storm's rows are one contiguous block, so this is a plain line scan that
    stops at the end of the block (about half a second for the 330 MB
    archive, against 10 s+ for parsing every row). Match on the SID (first
    column) when known, otherwise on the USA_ATCF_ID column.
    """
    import csv

    sid = (sid or "").strip().upper()
    atcf = (atcf or "").strip().upper()
    if not sid and not atcf:
        return []
    with open(path, "r", newline="", encoding="utf-8", errors="replace") as fh:
        header = next(csv.reader([fh.readline()]), [])
        if "SID" not in header:
            return []
        if not sid:
            # Find the storm's SID from the first row carrying the ATCF id,
            # then read its whole block (early fixes can lack the id).
            col = header.index("USA_ATCF_ID") if "USA_ATCF_ID" in header else None
            if col is None:
                return []
            for line in fh:
                if atcf in line:
                    row = next(csv.reader([line]), [])
                    if len(row) > col and row[col].strip().upper() == atcf:
                        sid = row[0].strip().upper()
                        break
            if not sid:
                return []
            fh.seek(0)
            fh.readline()
        prefix = sid + ","
        hit: List[str] = []
        for line in fh:
            if line.startswith(prefix):
                hit.append(line)
            elif hit:
                break
        return [dict(zip(header, row)) for row in csv.reader(hit)]


def points_from_ibtracs(rows: Iterable[dict]) -> List[dict]:
    """Main-track fixes in time order: {t, lat, lon, wind, pres, record, d2l}."""
    out: List[dict] = []
    for r in rows or []:
        if str(r.get("TRACK_TYPE") or "main").strip().lower() not in ("main", ""):
            continue                                   # spur tracks are alternates
        t = _parse_time(r.get("ISO_TIME"))
        lat, lon = _f(r.get("LAT")), _f(r.get("LON"))
        if t is None or lat is None or lon is None:
            continue
        wind = _f(r.get("USA_WIND"))
        if wind is None or wind <= 0:
            wind = _f(r.get("WMO_WIND"))
        pres = _f(r.get("USA_PRES"))
        if pres is None or pres <= 0:
            pres = _f(r.get("WMO_PRES"))
        out.append({
            "t": t, "lat": lat, "lon": lon,
            "wind": wind if (wind is not None and wind > 0) else None,
            "pres": pres if (pres is not None and pres > 0) else None,
            "record": str(r.get("USA_RECORD") or "").strip().upper(),
            "d2l": _f(r.get("DIST2LAND")),
        })
    out.sort(key=lambda p: p["t"])
    return out


def official(points: Sequence[dict]) -> List[dict]:
    """The agency's landfall records (USA_RECORD carries an "L")."""
    return [_landfall(p["t"], p["lat"], p["lon"], p["wind"], p["pres"], "official")
            for p in points if "L" in p.get("record", "")]


def crossings(points: Sequence[dict]) -> List[dict]:
    """Sea-to-land transitions of the over-land flag, interpolated to the coast.

    The last fix over water is `d2l` km from land; the crossing is placed that
    far along the leg to the first fix over land (the nearest land may not lie
    dead ahead, so this is the earliest the centre could have reached it).
    """
    out: List[dict] = []
    for a, b in zip(points, points[1:]):
        da, db = a.get("d2l"), b.get("d2l")
        if da is None or db is None or not (da > 0 and db == 0):
            continue
        if (b["t"] - a["t"]) > timedelta(hours=12):
            continue                                   # a gap in the record, not a leg
        leg = _km(a["lat"], a["lon"], b["lat"], b["lon"])
        f = 1.0 if leg <= 0 else max(0.0, min(1.0, da / leg))
        wind = _lerp(a["wind"], b["wind"], f)
        if wind is None or wind < MIN_WIND_KT:
            continue
        dlon = _norm_lon(b["lon"] - a["lon"])
        out.append(_landfall(
            _round10(a["t"] + (b["t"] - a["t"]) * f),
            a["lat"] + (b["lat"] - a["lat"]) * f, a["lon"] + dlon * f,
            wind, _lerp(a["pres"], b["pres"], f), "crossing"))
    return out


def from_ibtracs(rows: Iterable[dict]) -> dict:
    """{"source", "landfalls"} from a storm's IBTrACS rows. `source` is None
    when the rows hold no track at all (the storm is not in IBTrACS yet)."""
    pts = points_from_ibtracs(rows)
    if not pts:
        return {"source": None, "landfalls": []}
    lf = official(pts)
    if lf:
        return {"source": "official", "landfalls": lf}
    return {"source": "crossing", "landfalls": crossings(pts)}


# ── live storms ────────────────────────────────────────────────────────────

def _densify(points: Sequence[dict], step_min: int = STEP_MIN) -> List[dict]:
    out: List[dict] = []
    for a, b in zip(points, points[1:]):
        span = (b["t"] - a["t"]).total_seconds()
        if span <= 0 or span > 12 * 3600:
            out.append(dict(a, gap=True))
            continue
        n = max(1, int(round(span / (step_min * 60.0))))
        dlon = _norm_lon(b["lon"] - a["lon"])
        for i in range(n):
            f = i / n
            out.append({
                "t": a["t"] + (b["t"] - a["t"]) * f,
                "lat": a["lat"] + (b["lat"] - a["lat"]) * f,
                "lon": a["lon"] + dlon * f,
                "wind": _lerp(a["wind"], b["wind"], f),
                "pres": _lerp(a["pres"], b["pres"], f),
            })
    if points:
        out.append(dict(points[-1]))
    return out


def points_from_track(track: Iterable[dict]) -> List[dict]:
    """Fixes from a /track payload (timestamp, lat, lon, max_wind_ms, min_pressure_hpa)."""
    out: List[dict] = []
    for r in track or []:
        t = _parse_time(r.get("timestamp"))
        lat, lon = _f(r.get("lat")), _f(r.get("lon"))
        if t is None or lat is None or lon is None:
            continue
        ms = _f(r.get("max_wind_ms"))
        out.append({"t": t, "lat": lat, "lon": lon,
                    "wind": None if ms is None else ms / 0.514444,
                    "pres": _f(r.get("min_pressure_hpa"))})
    out.sort(key=lambda p: p["t"])
    return out


def estimate(points: Sequence[dict], coast: Sequence[CoastPoint],
             near_km: float = NEAR_KM, hit_km: float = HIT_KM) -> List[dict]:
    """Landfall estimates for a track with no agency record yet.

    Each stretch the track spends within `near_km` of a coastal point is one
    encounter; it counts as a landfall when the track passed within `hit_km`
    of a point, and the landfall is placed at that closest pass. A storm that
    brushes the coast this closely without crossing it would also be counted:
    that is why the result is labelled an estimate.
    """
    if len(points) < 2 or not coast:
        return []
    lats = [p["lat"] for p in points]
    lons = [_norm_lon(p["lon"]) for p in points]
    near = [(la, lo, nm) for la, lo, nm in coast
            if min(lats) - 1.5 <= la <= max(lats) + 1.5
            and min(lons) - 2.0 <= _norm_lon(lo) <= max(lons) + 2.0]
    if not near:
        return []

    out: List[dict] = []
    run: List[tuple] = []

    def close_run():
        if not run:
            return
        d, s, name = min(run, key=lambda x: x[0])
        if d <= hit_km and (s.get("wind") or 0) >= MIN_WIND_KT:
            lf = _landfall(_round10(s["t"]), s["lat"], s["lon"], s.get("wind"), s.get("pres"), "estimate")
            lf["place"] = name
            lf["distance_km"] = round(d, 1)
            out.append(lf)
        run.clear()

    for s in _densify(points):
        d, name = min(((_km(s["lat"], s["lon"], la, lo), nm) for la, lo, nm in near),
                      key=lambda x: x[0])
        if d <= near_km and not s.get("gap"):
            run.append((d, s, name))
        else:
            close_run()
    close_run()
    return out


# ── summary ────────────────────────────────────────────────────────────────

NOTES = {
    "official": "Agency best-track landfall record (NHC), via IBTrACS.",
    "crossing": "Coast crossing from the IBTrACS best track, interpolated between fixes up to 3 h apart.",
    "estimate": "Estimate: the track's closest pass to the coast. The official best-track "
                "landfall record is not published yet.",
}


def summary(source: Optional[str], landfalls: Sequence[dict]) -> dict:
    """The API payload: every landfall, and the strongest one called out."""
    lfs = sorted(landfalls, key=lambda lf: lf["time_utc"])
    return {
        "source": source,
        "note": NOTES.get(source or "", None),
        "count": len(lfs),
        "landfalls": lfs,
        "strongest": strongest(lfs),
    }


def strongest(landfalls: Sequence[dict]) -> Optional[dict]:
    """The landfall with the highest wind (the earliest of equals)."""
    ranked = [lf for lf in landfalls if lf.get("wind_kt") is not None]
    if not ranked:
        return landfalls[0] if landfalls else None
    return max(ranked, key=lambda lf: (lf["wind_kt"], -_parse_time(lf["time_utc"]).timestamp()))
