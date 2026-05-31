"""
Rebuild frontend/nri_zones.json from the FEMA National Risk Index (NRI).

WHY THIS EXISTS
---------------
The previous nri_zones.json deflated active/forecast ERS badly. Two faults:
  1. Exposure was linearly normalized against a right-skewed national
     distribution, compressing every non-mega-metro down near zero.
  2. Vulnerability was derived from FEMA's *Community Resilience* (RESL)
     score. Post-Katrina New Orleans scores RESL ~99.9 (top-percentile
     "resilient"), so the most surge-fragile metro in the US was rated
     vuln 0.84 (below average). Exactly backwards for a surge-risk tool.

METHODOLOGY (this build)
------------------------
  exposure (0-1): the hand-tuned _ECON_ZONES value is kept. FEMA's actual
                  exposed building value (computed here as `femaexp` for
                  reference) is defensible but compresses the scale — NYC's
                  20-county metro dominates, so single-county zones like New
                  Orleans collapse to ~0.29 vs the hand-tuned 0.90. That
                  deflates active ERS far below the historical presets and
                  breaks the formula's calibration. The hand-tuned exposure is
                  calibrated to the ERS scale and keeps active storms
                  comparable to presets, so exposure stays hand-tuned.
  vuln (0.6-1.5): data-driven from FEMA, the dimension that was actually
                  broken. Blend of
                    - SOVI  (Social Vulnerability, 0-100) -> recovery fragility
                    - HLR   (Hurricane / Coastal-Flood Historic Loss Ratio)
                            -> structural + geographic damage susceptibility
                  Deliberately does NOT use RESL (Community Resilience), which
                  rated post-levee New Orleans "resilient" (vuln 0.84) — the
                  inversion this rebuild exists to fix.
                  Territories (PR/USVI) have no FEMA SOVI -> keep hand-tuned vuln.

SOURCE
------
FEMA NRI Counties hosted feature layer, queried by each zone's bounding box
(esriGeometryEnvelope intersect). US states + PR/USVI only; foreign coasts
(Cancun, Nassau) return no NRI counties and are omitted, so get_economic_exposure
falls back to their hand-tuned values.

Reproducible: re-run any time to refresh against the latest NRI release.
Run:  C:\\Python314\\python.exe build_nri_zones.py
NOTE: keep ZONES in sync with core/ike.py _ECON_ZONES (name + bbox).
"""
import json, math, ssl, time, urllib.parse, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "frontend" / "nri_zones.json"
NRI_Q = ("https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/"
         "National_Risk_Index_Counties/FeatureServer/0/query")
_CTX = ssl.create_default_context()

# (name, lat_min, lat_max, lon_min, lon_max, hand_exposure, hand_vuln)
# Mirrors core/ike.py _ECON_ZONES — keep in sync.
ZONES = [
    ("NYC Metro / Long Island",     40.2, 41.2, -74.3, -72.5, 1.00, 0.85),
    ("Northern NJ / Newark",        39.5, 40.8, -74.5, -73.8, 0.92, 0.90),
    ("Connecticut Coast",           40.8, 41.4, -73.7, -72.0, 0.75, 0.80),
    ("Boston Metro",                42.0, 42.7, -71.3, -70.5, 0.80, 0.80),
    ("Rhode Island / Cape Cod",     41.2, 42.0, -71.8, -69.9, 0.55, 0.95),
    ("Atlantic City / Shore",       39.0, 39.8, -74.6, -74.0, 0.50, 1.05),
    ("Delaware Bay / Philly",       38.5, 40.0, -75.6, -74.6, 0.65, 0.85),
    ("Chesapeake Bay / Norfolk",    36.5, 38.5, -77.0, -75.5, 0.70, 1.00),
    ("Outer Banks NC",              34.5, 36.5, -76.5, -75.2, 0.30, 1.25),
    ("Wilmington NC Metro",         33.7, 34.5, -78.2, -77.5, 0.35, 1.05),
    ("Myrtle Beach SC",             33.2, 33.9, -79.2, -78.5, 0.40, 1.10),
    ("Charleston SC Metro",         32.4, 33.2, -80.3, -79.5, 0.55, 1.00),
    ("Savannah GA / Hilton Head",   31.8, 32.4, -81.3, -80.5, 0.45, 0.95),
    ("Jacksonville FL Metro",       30.0, 30.8, -81.8, -81.0, 0.55, 0.90),
    ("Palm Beach / Treasure Coast", 26.5, 27.5, -80.3, -79.8, 0.70, 0.75),
    ("Fort Lauderdale / Broward",   25.9, 26.5, -80.4, -79.9, 0.85, 0.70),
    ("Miami-Dade Metro",            25.3, 25.9, -80.5, -80.0, 0.95, 0.65),
    ("Florida Keys",                24.3, 25.3, -82.0, -80.0, 0.30, 1.15),
    ("Naples / Collier Co",         25.8, 26.5, -82.0, -81.3, 0.55, 0.80),
    ("Fort Myers / Lee Co",         26.3, 26.8, -82.2, -81.7, 0.60, 1.05),
    ("Sarasota / Manatee",          26.8, 27.5, -82.8, -82.2, 0.55, 0.90),
    ("Tampa Bay Metro",             27.5, 28.3, -82.9, -82.2, 0.85, 1.00),
    ("Clearwater / Pinellas",       27.7, 28.2, -83.0, -82.5, 0.65, 0.90),
    ("Nature Coast FL (rural)",     28.3, 29.3, -83.5, -82.5, 0.10, 1.20),
    ("Big Bend FL (rural)",         29.3, 30.3, -84.5, -83.0, 0.15, 1.25),
    ("Panama City FL",              29.8, 30.5, -86.0, -85.0, 0.40, 1.30),
    ("Destin / Fort Walton",        30.2, 30.6, -87.0, -86.0, 0.35, 1.00),
    ("Pensacola Metro",             30.2, 30.7, -87.6, -86.8, 0.45, 1.00),
    ("Mobile AL Metro",             30.2, 31.0, -88.3, -87.5, 0.50, 1.00),
    ("Biloxi / Gulfport MS",        30.2, 30.7, -89.5, -88.3, 0.40, 1.15),
    ("New Orleans Metro",           29.5, 30.3, -90.5, -89.5, 0.90, 1.40),
    ("Houma / Terrebonne LA",       29.0, 29.6, -91.2, -90.3, 0.50, 1.30),
    ("Lafayette / Vermilion LA",    29.5, 30.5, -92.5, -91.2, 0.40, 1.05),
    ("Lake Charles LA (refinery)",  29.8, 30.5, -93.5, -92.5, 0.55, 1.15),
    ("Beaumont / Port Arthur TX",   29.5, 30.3, -94.5, -93.5, 0.55, 1.10),
    ("Houston / Galveston Metro",   28.8, 30.0, -95.8, -94.3, 0.95, 0.90),
    ("Freeport / Brazoria TX",      28.5, 29.0, -95.8, -95.0, 0.45, 1.05),
    ("Matagorda / Victoria TX",     28.2, 28.9, -96.8, -95.8, 0.25, 1.15),
    ("Corpus Christi TX",           27.3, 28.2, -97.5, -96.8, 0.45, 1.05),
    ("South Padre / Brownsville",   25.8, 27.3, -97.8, -96.8, 0.25, 1.15),
    ("San Juan PR Metro",           17.8, 18.6, -66.5, -65.5, 0.55, 1.45),
    ("US Virgin Islands",           17.5, 18.5, -65.5, -64.5, 0.30, 1.35),
    ("Cancun / Riviera Maya",       20.0, 21.5, -87.5, -86.5, 0.50, 1.10),  # foreign
    ("Nassau / Bahamas",            24.5, 25.5, -78.0, -77.0, 0.35, 1.30),  # foreign
]


def query_zone(lat_min, lat_max, lon_min, lon_max):
    params = {
        "where": "1=1",
        "geometry": f"{lon_min},{lat_min},{lon_max},{lat_max}",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "POPULATION,BUILDVALUE,SOVI_SCORE,HRCN_HLRB,CFLD_HLRB,CFLD_EXPB",
        "returnGeometry": "false",
        "f": "json",
    }
    url = NRI_Q + "?" + urllib.parse.urlencode(params)
    # A real User-Agent — the default "Python-urllib/x" trips the host's bot
    # filter and gets the connection reset (WinError 10054).
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; StormDPS-NRI-build/1.0)",
        "Accept": "application/json",
    })
    last = None
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=60, context=_CTX) as resp:
                d = json.loads(resp.read())
            return d.get("features", [])
        except Exception as e:
            last = e
            time.sleep(1.5 * (attempt + 1))   # linear backoff up to ~9s
    print(f"   !! query failed after 6 retries: {last}")
    return []


def main():
    old = json.loads(OUT.read_text()) if OUT.exists() else {}
    # Per-zone stats cache. The ArcGIS host intermittently resets connections,
    # so a single run may miss a zone or two. Caching every live success makes
    # the build converge to a complete, deterministic result across re-runs.
    cache_path = ROOT / "nri_build_cache.json"
    cache = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    agg = {}
    print("Aggregating NRI counties per zone...")
    for (name, lat_min, lat_max, lon_min, lon_max, h_exp, h_vuln) in ZONES:
        time.sleep(0.35)  # throttle so the ArcGIS host doesn't reset the connection
        feats = query_zone(lat_min, lat_max, lon_min, lon_max)
        ncoastal = 0
        if feats:
            bv = pop = sovi_w = sovi_pop = hlr_w = 0.0
            for f in feats:
                a = f["attributes"]
                # Coastal filter: only counties with coastal-flood exposure. Kills
                # the inland over-aggregation from wide bboxes (e.g. Chesapeake's
                # 44-county catch reaching deep into inland VA/MD).
                if (a.get("CFLD_EXPB") or 0) <= 0:
                    continue
                ncoastal += 1
                p = a.get("POPULATION") or 0
                b = a.get("BUILDVALUE") or 0.0
                bv += b
                pop += p
                s = a.get("SOVI_SCORE")
                if s is not None and s > 0:      # SOVI is 0/absent for PR/USVI territories
                    sovi_w += s * (p or 1)
                    sovi_pop += (p or 1)
                hlr = max(a.get("HRCN_HLRB") or 0.0, a.get("CFLD_HLRB") or 0.0)
                hlr_w += hlr * (p or 1)
        if ncoastal > 0:
            stats = {
                "bv": bv, "pop": pop,
                "sovi": (sovi_w / sovi_pop) if sovi_pop else None,   # None -> territory, no SOVI
                "hlr": (hlr_w / pop) if pop else 0.0, "n": ncoastal,
            }
            cache[name] = stats                                       # refresh cache on every success
            agg[name] = dict(stats, hand=(h_exp, h_vuln))
            sv = stats["sovi"]
            print(f"  {name:30} coastal={ncoastal:2}  bv=${bv/1e9:6.1f}B  "
                  f"SOVI={'n/a' if sv is None else format(sv, '5.1f')}  HLR={stats['hlr']:.5f}")
        elif name in cache:                                           # live fetch failed -> use cached
            agg[name] = dict(cache[name], hand=(h_exp, h_vuln))
            print(f"  {name:30} (live fetch unavailable -> cached NRI stats)")
        else:
            print(f"  {name:30} no NRI data -> omit (keeps hand-tuned)")
    cache_path.write_text(json.dumps(cache, indent=1))

    max_bv = max(z["bv"] for z in agg.values())
    max_hlr = max(z["hlr"] for z in agg.values()) or 1.0

    out = {}
    print(f"\n{'ZONE':30} {'FEMAexp':>8} {'NEW e/v':>12}   {'OLD e/v':>12}   {'HAND e/v':>12}")
    for name, z in agg.items():
        # EXPOSURE: use the hand-tuned value. FEMA's real building value
        # (femaexp below) is defensible but compresses the scale — NYC's
        # 20-county metro dominates, so single-county zones like New Orleans
        # land ~0.29 vs the hand-tuned 0.90, which deflates active ERS far
        # below the historical presets (Katrina 97). The hand-tuned exposure
        # is calibrated to the ERS formula and keeps active storms comparable
        # to presets. We adopt FEMA only for VULNERABILITY (the actual bug).
        femaexp = round(min(1.0, math.sqrt(z["bv"] / max_bv)), 3)
        exposure = z["hand"][0]
        # VULNERABILITY: data-driven from FEMA Social Vulnerability (SOVI) +
        # Historic Loss Ratio (structural/geographic damage susceptibility).
        # NOT Community Resilience (which inverted New Orleans to 0.84).
        if z["sovi"] is None:
            vuln = z["hand"][1]      # territory (PR/USVI): FEMA SOVI absent -> keep hand-tuned vuln
        else:
            social = z["sovi"] / 100.0
            physical = z["hlr"] / max_hlr
            vuln = round(max(0.6, min(1.5, 0.65 + 0.45 * social + 0.40 * physical)), 2)
        out[name] = {"exposure": exposure, "vuln": vuln}
        o = old.get(name, {})
        oe, ov = o.get("exposure", "-"), o.get("vuln", "-")
        he, hv = z["hand"]
        print(f"{name[:30]:30} {femaexp:8.3f} {exposure:6.2f}/{vuln:<4.2f}   "
              f"{str(oe):>6}/{str(ov):<5}   {he:6.2f}/{hv:<4.2f}")

    OUT.write_text(json.dumps(out, indent=2))
    print(f"\nWrote {len(out)} zones -> {OUT}")


if __name__ == "__main__":
    main()
