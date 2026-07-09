# Retired map overlays — archived code + restoration guide

**Retired 2026-07-08.** The tracker map moved to a fixed always-on layer set
(Satellite for active storms on desktop, Windfield, Wind flow) with a single
Legend button. The Advanced-view panel and its optional overlays were removed
in two steps:

- `cc4ee29` removed the panel UI (button, rows, sliders, scrubber, CSS) and
  the wiring that enabled/tore down the panel buttons.
- The commit that adds this document excises the then-dormant layer
  machinery itself (Wind particle / Pressure / Precip layers, the overlay
  LRU fetch, the Satellite-visibility + Infrared toggles, the scrubber
  handler, `_computeStormBbox`).

**The last fully working on-map version of everything below is git
`c841e96`** (`git show c841e96:frontend/index.html`). This document exists so
a future re-add is copy-paste plus a short wiring checklist rather than an
archaeology dig. Code blocks tagged *(current)* were captured at excision
time — they include every post-`c841e96` bug fix; blocks tagged *(c841e96)*
are the panel-era UI/wiring removed earlier.

## What was NOT retired

The always-on layers and their machinery are alive in `index.html`:
`_windfieldState`/`toggleWindfieldLayer` (Holland-B windfield),
`_windDirState`/`toggleWindDirLayer` (parametric wind flow),
`initSatelliteLayer`/`teardownSatelliteLayer`/`setSatelliteFrame`/
`syncOverlaysToTimestampMs` (satellite + timeline sync), and the Himawari
auto night-IR (`_autoIRForFrame`/`_sunElevationDeg`). The 3D portrait lives
on `/methodology`. Backend routes (`/wind/field`, `/pressure/field`,
`/precip/field`) were NOT touched — the servers still answer; only the
frontend consumers are archived here.

## Restoration checklist (in order)

1. Paste the **JS machinery** (sections 1-6) back into `index.html`'s script,
   roughly where they lived: LRU + `_computeStormBbox` near
   `initSatelliteLayer`; wind/pressure/precip blocks between the wind-flow
   module and `renderMap`.
2. Paste the **wiring touchpoints** (section 7) back into their host
   functions — each snippet names its exact anchor.
3. Paste the **panel HTML + CSS** (sections 8-9) and the **panel wiring**
   (sections 10-12: `toggleAdvancedView` + boot restore,
   `enableHistoricOverlays`, `_enableWindfieldToggle`,
   `initSatelliteLayer`'s button-enable tail). Note the panel-era
   `teardownSatelliteLayer` (section 13) is a REFERENCE — do not resurrect
   its windfield-teardown block; the always-on windfield must survive
   satellite re-inits (see the NOTE comment in today's teardown).
4. Mind the gotchas at the bottom (Open-Meteo rate limits, prefetch design,
   `keepOverlays`).

---

## 1-6: Layer machinery *(current at excision)*

### 1. Overlay fetch coalescing + LRU cache

Shared by wind/pressure/precip. Prevents burst-502s from Open-Meteo rate limiting during rapid scrubs.

```js
// ── Overlay fetch coalescing + LRU cache ─────────────────────────────────
// Prevents burst-502s from Open-Meteo rate-limiting during rapid scrubs.
// Each layer gets its own cache & inflight map; keyed by URL.
const _overlayCache = { wind: new Map(), pressure: new Map(), precip: new Map() };
const _overlayInflight = { wind: new Map(), pressure: new Map(), precip: new Map() };
const _OVERLAY_CACHE_MAX = 12; // keep last N payloads per layer

function _overlayFetch(layer, url) {
    // 1. LRU cache hit
    const cache = _overlayCache[layer];
    if (cache.has(url)) {
        const val = cache.get(url);
        cache.delete(url); cache.set(url, val); // refresh LRU order
        return Promise.resolve(structuredClone(val));
    }
    // 2. Coalesce: reuse in-flight promise for same URL
    const inflight = _overlayInflight[layer];
    if (inflight.has(url)) return inflight.get(url);
    // 3. Fetch with retry on 502/429 (rate-limit aware)
    const p = (async () => {
        for (let attempt = 0; attempt < 2; attempt++) {
            const r = await fetch(url);
            if ((r.status === 502 || r.status === 429) && attempt === 0) {
                const wait = r.status === 429
                    ? Math.min(parseFloat(r.headers.get('Retry-After') || '3'), 10) * 1000
                    : 1200;
                await new Promise(ok => setTimeout(ok, wait));
                continue;
            }
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            const data = await r.json();
            // Store in LRU cache
            if (cache.size >= _OVERLAY_CACHE_MAX) {
                cache.delete(cache.keys().next().value); // evict oldest
            }
            cache.set(url, data);
            return structuredClone(data);
        }
    })();
    inflight.set(url, p);
    p.finally(() => inflight.delete(url));
    return p;
}
```

### 2. Wind particle layer (leaflet-velocity, /wind/field)

State + frame driver + windowed/paced prefetch + opacity + toggle. Depends on leaflet-velocity (`loadLeafletVelocity()`, still in index.html for the wind-flow layer) and on satellite frames for active storms (see gotchas).

```js
// ── Wind layer state (mutable across calls) ───────────────────────────────
// leaflet-velocity overlay backed by /wind/field (Open-Meteo proxy). The
// satellite slider drives both layers — see setSatelliteFrame below.
window._windState = {
    layer: null,
    enabled: false,
    bbox: null,             // [s,w,n,e] cached by initSatelliteLayer
    currentTs: null,        // YYYYMMDDTHH (wind is hourly)
    fetchToken: 0,          // monotonic counter to drop stale responses
    debounceTimer: null,    // debounce rapid scrubs (Open-Meteo 10 req/min)
    pendingTs: null,        // most-recent satellite ts queued behind debounce
    prefetched: new Set(),  // wind timestamps known to be in the server cache
    prefetchStarted: false, // guard so we only kick off prefetch once per toggle
};

// Reset the Wind toggle button to its idle "Off" appearance. Used on fetch
// failures so the button doesn't get stuck on "Loading…".
function _windButtonReset() {
    const btn = document.getElementById('windToggleBtn');
    const legend = document.getElementById('windLegend');
    if (btn) {
        btn.style.background = 'rgba(100,116,139,.18)';
        btn.style.borderColor = 'rgba(100,116,139,.4)';
        btn.style.color = '#94a3b8';
        btn.textContent = 'Wind: Off';
        btn.disabled = false;
    }
    if (legend) legend.style.display = 'none';
}

function _satelliteTsToWindTs(ts) {
    // Map a satellite frame ts (YYYYMMDDTHH[MM]) to the wind frame's hour.
    //
    // SNAP TO THE NEAREST STORM-TRACK FIX'S HOUR. The windfield overlay draws
    // the nearest fix (setWindfieldFrameByIdx → nearest storm index), so
    // resolving the wind to the SAME fix makes the two layers show the same
    // moment and co-register exactly — even for off-synoptic fixes (landfall
    // special times) where a fixed 3h bucket would drift up to ~3h. With the
    // live 3h densification, fix hours are already ~3h apart, so this also
    // keeps the frame count bounded. Both the display (setWindFrame) and the
    // prefetch route through here, so they stay on the identical set.
    //
    // Falls back to a 3h bucket when the track isn't loaded yet (initial load).
    if (!ts) return null;
    const m = String(ts).match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(?:(\d{2}))?$/);
    if (!m) return null;
    const pad = (n) => String(n).padStart(2, '0');
    const sd = window.stormData;
    if (sd && sd.length) {
        const tsMs = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +(m[5] || 0));
        let bestMs = null, bestDiff = Infinity;
        for (const s of sd) {
            if (!s || !s.timestamp) continue;
            const sm = Date.parse(s.timestamp);
            if (!Number.isFinite(sm)) continue;
            const diff = Math.abs(sm - tsMs);
            if (diff < bestDiff) { bestDiff = diff; bestMs = sm; }
        }
        if (bestMs != null) {
            const d = new Date(bestMs);  // nearest fix; wind fetches hourly, so floor to the hour
            return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}`;
        }
    }
    const h3 = Math.floor(+m[4] / 3) * 3;  // fallback: 3h bucket
    return `${m[1]}${m[2]}${m[3]}T${pad(h3)}`;
}

// Public entry: debounces rapid scrubs (Open-Meteo free tier = 10 req/min).
// The most-recent timestamp wins; intermediate scrubs are dropped.
function setWindFrame(satelliteTs) {
    const st = window._windState;
    if (!st.enabled || !st.bbox || !currentMap) return;
    // During animation playback, only update wind for frames we've already
    // pre-fetched into the server cache. A cache-hit fetch is ~50 ms so the
    // animation stays smooth; a cache-miss would hit Open-Meteo (10 req/min
    // free tier) and at 1fps the animation would burst over the limit,
    // taking HTTP 429 firehose for the remainder of the play. The prefetch
    // started in toggleWindLayer populates st.prefetched as each frame
    // completes — so initial play shows partial wind updates and subsequent
    // plays are smooth.
    if (window.animationFrameId) {
        const windTsCheck = _satelliteTsToWindTs(satelliteTs);
        if (!windTsCheck || !st.prefetched.has(windTsCheck)) return;
    }
    st.pendingTs = satelliteTs;
    if (st.debounceTimer) clearTimeout(st.debounceTimer);
    // 500ms for manual scrubs; 200ms during animation so wind keeps up.
    const _debounce = window.animationFrameId ? 200 : 500;
    st.debounceTimer = setTimeout(() => {
        st.debounceTimer = null;
        const ts = st.pendingTs;
        st.pendingTs = null;
        if (ts) _setWindFrameNow(ts);
    }, _debounce);
}

// Button-label helper for the prefetch progress ("Wind: On (caching 3/45)").
function _windPrefetchLabel(done, total) {
    const st = window._windState;
    const b = document.getElementById('windToggleBtn');
    if (!b || !st.enabled || !b.textContent.startsWith('Wind:')) return;
    b.textContent = (total > 0 && done < total) ? `Wind: On (caching ${done}/${total})` : 'Wind: On';
}

// Warm the server-side wind cache for every frame the DISPLAY can request, so
// playback's per-frame fetches are all cache hits (~50 ms, no Open-Meteo hit).
// Fires once per wind-toggle-on.
//
// The frames are enumerated from the SAME sources the display draws from —
// the satellite frame set (what active storms snap to via syncOverlays →
// setWindFrame) AND the storm-timeline fixes (what historic storms use). The
// old version enumerated ONLY stormData, so for an ACTIVE storm — whose wind
// is driven by the far denser satellite frames — ~95% of the frames playback
// requested were never warmed, and the animation gate silently skipped them
// (the "wind won't load/cache" bug). Both sources reduce through
// _satelliteTsToWindTs to the shared 3h buckets, so coverage is exact.
async function _prefetchWindFrames() {
    const st = window._windState;
    if (!st.enabled || !st.bbox) return;
    if (st.prefetchStarted) return;
    st.prefetchStarted = true;

    const uniqueWindTs = new Set();
    const addTs = (satTs) => { const w = _satelliteTsToWindTs(satTs); if (w) uniqueWindTs.add(w); };
    const satFrames = (window._satelliteState && window._satelliteState.frames) || [];
    for (const f of satFrames) addTs(f);
    if (window._tsMsToSatTsString) {
        for (const snap of (window.stormData || [])) {
            if (!snap || !snap.timestamp) continue;
            const ms = Date.parse(snap.timestamp);
            if (Number.isFinite(ms)) addTs(window._tsMsToSatTsString(ms));
        }
    }

    // Warm only a bounded WINDOW nearest the current view. The free-tier
    // Open-Meteo budget is shared across wind/pressure/precip AND all users
    // and can't warm a whole storm's timeline — a burst just draws 429s. So
    // pre-warm the frames the user is most likely to scrub to next (nearest
    // the current frame) and let the rest load on demand: each on-demand
    // fetch retries 429s via _overlayFetch AND caches server-side (48h TTL),
    // so the timeline warms progressively as it's explored and persists for
    // later viewers.
    const _keyMs = (t) => { const m = String(t).match(/^(\d{4})(\d{2})(\d{2})T(\d{2})$/); return m ? Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4]) : 0; };
    const curMs = _keyMs(st.currentTs || [...uniqueWindTs].sort().reverse()[0] || '');
    const WINDOW = 12;   // ~36h of storm time at 3h buckets, centered on current
    const todo = [...uniqueWindTs].filter(t => !st.prefetched.has(t))
        .sort((a, b) => Math.abs(_keyMs(a) - curMs) - Math.abs(_keyMs(b) - curMs))
        .slice(0, WINDOW);
    const total = todo.length;
    if (total === 0) { _windPrefetchLabel(0, 0); return; }

    const [s, w, n, e] = st.bbox;
    const gen = (st.prefetchGen = (st.prefetchGen || 0) + 1);
    let done = 0;

    // SERIAL + PACED. The server serializes wind to ONE Open-Meteo slot
    // (open_meteo_limiter WIND semaphore = 1) and the free tier is ~10 req/min,
    // so a burst of client requests just queues, exhausts the server's 429
    // retries, and bubbles 429s the prefetch would drop — leaving frames cold.
    // Instead: one at a time, pacing ONLY on frames that actually hit the
    // upstream (X-Wind-Cache: miss) or got throttled. Server cache HITS fly
    // through with no delay, so a re-toggle or an already-warmed bbox finishes
    // instantly; a fresh storm trickles in over a few minutes without waste.
    // Each fetch keeps its own 20s timeout so one hung connection can't wedge
    // the run. Manual scrubs meanwhile fetch on demand via _overlayFetch, which
    // retries 429s with backoff — so the map stays usable while this warms.
    const PACE_MISS_MS = 5000;   // ~one cold fetch / 5s — near the 10/min ceiling
    const PACE_429_MS  = 8000;   // back off harder after an actual throttle
    async function worker() {
        while (todo.length > 0 && st.enabled && st.prefetchGen === gen) {
            const ts = todo.shift();
            if (!ts) break;
            const url = `${API_BASE}/wind/field?bbox=${s.toFixed(2)},${w.toFixed(2)},${n.toFixed(2)},${e.toFixed(2)}&ts=${ts}&res=1.0`;
            let ok = false, wasMiss = false;
            try {
                const ctrl = new AbortController();
                const timer = setTimeout(() => ctrl.abort(), 20000);
                let r;
                try { r = await fetch(url, { method: 'GET', signal: ctrl.signal }); }
                finally { clearTimeout(timer); }
                ok = !!(r && r.ok);
                if (ok) {
                    st.prefetched.add(ts);
                    wasMiss = r.headers.get('X-Wind-Cache') === 'miss';
                }
            } catch (_e) {
                // Timeout or network blip — skip; the frame still loads on demand.
            }
            done += 1;
            _windPrefetchLabel(done, total);
            // Throttle only when we actually touched Open-Meteo (cold miss) or
            // were rate-limited; warm cache hits need no pacing.
            if ((wasMiss || !ok) && st.prefetchGen === gen && todo.length > 0) {
                await new Promise(res => setTimeout(res, ok ? PACE_MISS_MS : PACE_429_MS));
            }
        }
    }
    await worker();  // single paced worker — server allows one wind slot anyway
}

async function _setWindFrameNow(satelliteTs) {
    const st = window._windState;
    if (!st.enabled || !st.bbox || !currentMap) return;
    const windTs = _satelliteTsToWindTs(satelliteTs);
    if (!windTs || windTs === st.currentTs) return;  // dedupe identical hour
    const prevTs = st.currentTs;
    st.currentTs = windTs;
    const token = ++st.fetchToken;
    const [s,w,n,e] = st.bbox;
    let payload = null;
    try {
        payload = await _overlayFetch('wind', `${API_BASE}/wind/field?bbox=${s.toFixed(2)},${w.toFixed(2)},${n.toFixed(2)},${e.toFixed(2)}&ts=${windTs}&res=1.0`);
    } catch (e) {
        console.warn('[WIND] field fetch failed:', e);
        // Roll back currentTs so a retry on the same hour isn't deduped
        // away. If no layer exists yet, the toggle is stuck on "Loading…" —
        // reset it to "Off" so the user can try again.
        if (token === st.fetchToken) st.currentTs = prevTs;
        if (!st.layer) {
            st.enabled = false;
            _windButtonReset();
        }
        return;
    }
    if (token !== st.fetchToken || !st.enabled) return;  // superseded
    // Flip button + show legend once we have data in hand
    const _wbtn = document.getElementById('windToggleBtn');
    const _wleg = document.getElementById('windLegend');
    if (_wbtn) {
        _wbtn.style.background = 'rgba(34,211,238,.22)';
        _wbtn.style.borderColor = 'rgba(34,211,238,.5)';
        _wbtn.style.color = '#67e8f9';
        _wbtn.textContent = 'Wind: On';
        _wbtn.disabled = false;
    }
    if (_wleg) _wleg.style.display = 'block';
    if (st.layer) {
        try { st.layer.setData(payload); } catch (e) { console.warn('[WIND] setData failed:', e); }
    } else {
        // leaflet-velocity is lazy-loaded — it's only needed for the wind
        // overlay, which most landing-page visitors never touch. Awaiting
        // here is usually a no-op since the user clicked the toggle a
        // moment ago and the script has had time to land.
        try { await loadLeafletVelocity(); } catch (e) { console.warn('[WIND] leaflet-velocity load failed:', e); return; }
        // Scale the colormap to THIS storm's intensity instead of a fixed
        // 60 m/s hurricane ceiling — under the old mapping a 18 m/s TS's
        // entire wind field sat in the bottom sixth of the ramp (pale blue
        // and cyan: invisible over blue ocean and pale satellite water).
        // Now the storm's own peak sits ~90% up the ramp, so the field
        // differentiates instead of rendering monochrome.
        let _peakMs = 0;
        try { (window.stormData || []).forEach(d => { if ((d.max_wind_ms || 0) > _peakMs) _peakMs = d.max_wind_ms; }); } catch (e) {}
        const _vmax = Math.max(22, Math.min(70, _peakMs * 1.1 || 60));
        st.layer = L.velocityLayer({
            displayValues: false,
            data: payload,
            maxVelocity: _vmax,     // m/s — storm-scaled colormap ceiling
            velocityScale: 0.010,
            particleAge: 56,
            lineWidth: 1.8,
            particleMultiplier: 0.0016,
            // 600-weight hues only — every stop reads against both the light
            // basemap and satellite imagery. The old ramp opened with pale
            // blue/cyan that vanished over water.
            colorScale: ['#6366f1','#0891b2','#16a34a','#ca8a04','#ea580c','#dc2626','#9d174d'],
        }).addTo(currentMap);
        try { st.layer.setZIndex && st.layer.setZIndex(250); } catch(e) {}
        // leaflet-velocity ignores `opacity` in its options — soften the
        // canvas directly so storm-track dots stay readable underneath.
        // Pulls current value from the DIM slider so the user's last choice
        // sticks across hours.
        const _opIn = document.getElementById('windOpacityInput');
        const _op   = _opIn ? (parseInt(_opIn.value, 10) / 100) : 0.70;
        try {
            const cvs = st.layer._canvasLayer && st.layer._canvasLayer._canvas;
            if (cvs) cvs.style.opacity = String(_op);
        } catch (e) {}
    }
    // First frame is now on screen. Mark its ts as prefetched (we just
    // populated the server cache by fetching it) and kick off background
    // prefetch of the rest of the storm's wind hours so the animation
    // can update wind per-snapshot. Fires once per toggle-on.
    st.prefetched.add(windTs);
    if (!st.prefetchStarted) {
        // setTimeout so the first frame's render isn't competing with
        // the prefetch fetch barrage for the same network/parse budget.
        setTimeout(() => { _prefetchWindFrames(); }, 250);
    }
}

// Live opacity slider for the wind canvas. No-op if the layer hasn't been
// created yet; the next addTo() will read the slider value.
window.setWindOpacity = function(value) {
    const pct = Math.max(10, Math.min(95, parseInt(value, 10) || 55));
    const lbl = document.getElementById('windOpacityLabel');
    if (lbl) lbl.textContent = pct + '%';
    const st = window._windState;
    if (!st || !st.layer) return;
    try {
        const cvs = st.layer._canvasLayer && st.layer._canvasLayer._canvas;
        if (cvs) cvs.style.opacity = String(pct / 100);
    } catch (e) {}
};

function toggleWindLayer() {
    const st = window._windState;
    const btn = document.getElementById('windToggleBtn');
    const legend = document.getElementById('windLegend');
    if (st.enabled) {
        st.enabled = false;
        if (st.layer && currentMap) {
            try { currentMap.removeLayer(st.layer); } catch(e) {}
        }
        st.layer = null;
        st.currentTs = null;
        // Reset the prefetch state so the next toggle-on re-evaluates the
        // current storm's track (may be a different storm now).
        st.prefetched = new Set();
        st.prefetchStarted = false;
        if (btn) {
            btn.style.background = 'rgba(100,116,139,.18)';
            btn.style.borderColor = 'rgba(100,116,139,.4)';
            btn.style.color = '#94a3b8';
            btn.textContent = 'Wind: Off';
            btn.disabled = false;
        }
        if (legend) legend.style.display = 'none';
    } else {
        if (!st.bbox) {
            console.warn('[WIND] cannot enable — no bbox cached');
            return;
        }
        st.enabled = true;
        // Show "Loading…" until the first wind frame is rendered. setWindFrame
        // flips the button to "On" via its own callback (added below).
        if (btn) {
            btn.style.background = 'rgba(250,204,21,.18)';
            btn.style.borderColor = 'rgba(250,204,21,.4)';
            btn.style.color = '#fde047';
            btn.textContent = 'Wind: Loading…';
            btn.disabled = true;
        }
        // Active storms drive off the satellite slider; historic storms fall
        // back to the current storm-timeline position (no sat slider exists).
        let satTs = null;
        const satSt = window._satelliteState;
        if (satSt && satSt.frames && satSt.frames.length) {
            satTs = satSt.frames[satSt.currentIdx];
        } else if (window.stormData && window.stormData.length && window._tsMsToSatTsString) {
            const len = window.stormData.length;
            const idx = Math.max(0, Math.min(
                Math.floor(window.currentAnimIndex != null ? window.currentAnimIndex : len - 1),
                len - 1));
            const d = window.stormData[idx];
            if (d && d.timestamp) satTs = window._tsMsToSatTsString(new Date(d.timestamp).getTime());
        }
        if (!satTs) {
            console.warn('[WIND] no timestamp source available');
            st.enabled = false;
            _windButtonReset();
            return;
        }
        setWindFrame(satTs);
    }
}
```

### 3. Pressure (MSLP) layer (/pressure/field)

Fill canvas + isobars (d3-contour via `loadD3Contour()`, still present) + station obs + opacity + toggle.

```js
// ── Pressure (MSLP) layer ─────────────────────────────────────────────────
// Three sub-layers stacked under the storm track:
//   1. Color-shaded MSLP fill        → L.imageOverlay rendered from canvas
//   2. White isobars (every 4 hPa)   → L.layerGroup of L.polylines via d3-contour
//   3. METAR station-pressure pills  → L.layerGroup of L.divIcon markers
// Sync follows the same pattern as the wind layer: setPressureFrame is
// called on every Storm Timeline scrub (debounced) and shares bbox with
// the satellite state so all three overlays line up.
window._pressureState = {
    fillLayer:    null,    // L.imageOverlay
    isobarLayer:  null,    // L.layerGroup
    stationLayer: null,    // L.layerGroup
    enabled:      false,
    showStations: true,
    bbox:         null,    // [s,w,n,e] cached by initSatelliteLayer
    currentTs:    null,    // YYYYMMDDTHH (hourly)
    fetchToken:   0,
    stationToken: 0,       // independent token for _refreshPressureStations
    debounceTimer: null,
    pendingTs:    null,
};

function _pressureButtonReset() {
    const btn = document.getElementById('pressureToggleBtn');
    const legend = document.getElementById('pressureLegend');
    if (btn) {
        btn.style.background = 'rgba(100,116,139,.18)';
        btn.style.borderColor = 'rgba(100,116,139,.4)';
        btn.style.color = '#94a3b8';
        btn.textContent = 'Pressure: Off';
        btn.disabled = false;
        btn.style.cursor = 'pointer';
        btn.style.opacity = '1';
    }
    if (legend) legend.style.display = 'none';
}

// Color ramp: 900 mb (extreme low) → 1013 mb (white) → 1048 mb (deep red).
// Extended low-end so deep tropical cyclone eyes (900-970 hPa) show a
// meaningful gradient instead of being clipped to a flat dark blue.
// Matches the Pressure legend gradient.
function _pressureColor(hpa) {
    const stops = [
        [900,  [0,   0,   60]],  // extreme low (Cat 5 eye)
        [920,  [10,  20,  110]], // very deep
        [940,  [20,  45,  155]], // deep
        [960,  [30,  80,  200]], // moderate low
        [975,  [50,  120, 240]], // low
        [990,  [80,  170, 250]], // below normal
        [1004, [148, 224, 252]], // near normal low
        [1013, [248, 250, 252]], // neutral (sea level)
        [1022, [253, 234, 160]], // slight high
        [1032, [249, 155, 50]],  // high
        [1048, [127, 29,  29]],  // strong high
    ];
    if (hpa <= stops[0][0]) return stops[0][1];
    if (hpa >= stops[stops.length-1][0]) return stops[stops.length-1][1];
    for (let i = 0; i < stops.length - 1; i++) {
        const [a, ca] = stops[i], [b, cb] = stops[i+1];
        if (hpa >= a && hpa <= b) {
            const t = (hpa - a) / (b - a);
            return [
                Math.round(ca[0] + (cb[0]-ca[0]) * t),
                Math.round(ca[1] + (cb[1]-ca[1]) * t),
                Math.round(ca[2] + (cb[2]-ca[2]) * t),
            ];
        }
    }
    return [240, 240, 240];
}

// Render the MSLP grid as a colored canvas → data URL → L.imageOverlay.
// Browser's bilinear upscaling smooths the cell edges naturally.
function _renderPressureFillCanvas(field) {
    const nx = field.nx, ny = field.ny, data = field.data;
    const cv = document.createElement('canvas');
    cv.width = nx; cv.height = ny;
    const ctx = cv.getContext('2d');
    const img = ctx.createImageData(nx, ny);
    for (let j = 0; j < ny; j++) {
        const row = data[j];
        for (let i = 0; i < nx; i++) {
            const c = _pressureColor(row[i]);
            const k = (j * nx + i) * 4;
            img.data[k]   = c[0];
            img.data[k+1] = c[1];
            img.data[k+2] = c[2];
            img.data[k+3] = 255;
        }
    }
    ctx.putImageData(img, 0, 0);
    return cv.toDataURL('image/png');
}

// Run d3-contour on the flat grid to get isobar polygons. d3 returns
// MultiPolygons in grid coordinates (x ∈ [0..nx], y ∈ [0..ny]); we
// convert each ring to lat/lon using the field's bbox + resolution.
function _buildIsobarPolylines(field) {
    if (!window.d3 || !window.d3.contours) return [];
    const nx = field.nx, ny = field.ny;
    // Flatten data row-by-row (top-to-bottom matches d3's expectation).
    const flat = new Float64Array(nx * ny);
    for (let j = 0; j < ny; j++) {
        const row = field.data[j];
        for (let i = 0; i < nx; i++) flat[j * nx + i] = row[i];
    }
    // Snap thresholds to nearest 4 hPa boundaries inside the field range.
    const lo = Math.floor(field.min / 4) * 4;
    const hi = Math.ceil(field.max / 4) * 4;
    const thresholds = [];
    for (let v = lo; v <= hi; v += 4) thresholds.push(v);
    let contours;
    try {
        contours = d3.contours().size([nx, ny]).thresholds(thresholds)(flat);
    } catch (e) {
        console.warn('[PRESSURE] d3-contour failed:', e);
        return [];
    }
    const la1 = field.la1, lo1 = field.lo1;
    const dx  = field.dx,  dy  = field.dy;
    // Convert grid (x, y) → lat/lon. y=0 is the top row (highest lat).
    // d3-contour treats integer coordinates as CELL CORNERS, not sample
    // positions — sample (i, j) appears at output coord (i+0.5, j+0.5).
    // Subtract 0.5 before scaling so isobars line up with the color-fill
    // layer (which is half-cell-expanded in _setPressureFrameNow and also
    // treats samples as cell centers).
    const toLatLng = (x, y) => [la1 - (y - 0.5) * dy, lo1 + (x - 0.5) * dx];
    const out = [];
    contours.forEach(c => {
        const value = c.value;
        // Style emphasis on multiples of 8; ICAO standard atmosphere = 1013.25.
        const isMain = (value % 8) === 0;
        const isStd  = Math.abs(value - 1013.25) < 0.5;
        c.coordinates.forEach(poly => {
            // d3-contour MultiPolygon convention: poly[0] is the outer ring,
            // poly[1..] are holes. Emit all rings as polylines, but mark only
            // the outer one as label-eligible so contours-with-holes don't
            // get stamped twice.
            poly.forEach((ring, idx) => {
                const ll = ring.map(([x,y]) => toLatLng(x, y));
                out.push({
                    latlngs: ll,
                    value,
                    isMain,
                    isStd,
                    isOuter: idx === 0,
                });
            });
        });
    });
    return out;
}

function setPressureFrame(satelliteTs) {
    const st = window._pressureState;
    if (!st.enabled || !st.bbox || !currentMap) return;
    st.pendingTs = satelliteTs;
    if (st.debounceTimer) clearTimeout(st.debounceTimer);
    st.debounceTimer = setTimeout(() => {
        st.debounceTimer = null;
        const ts = st.pendingTs;
        st.pendingTs = null;
        if (ts) _setPressureFrameNow(ts);
    }, 200);
}

async function _setPressureFrameNow(satelliteTs) {
    const st = window._pressureState;
    if (!st.enabled || !currentMap) return;
    // Pressure is hourly (same as wind). Accept either YYYYMMDDTHH (11 chars)
    // or YYYYMMDDTHHMM (13 chars) and normalize to the 11-char hourly form.
    const _presM = satelliteTs && satelliteTs.match(/^(\d{4})(\d{2})(\d{2})T(\d{2})/);
    const presTs = _presM ? `${_presM[1]}${_presM[2]}${_presM[3]}T${_presM[4]}` : null;
    if (!presTs || presTs === st.currentTs) return;
    const prevTs = st.currentTs;
    st.currentTs = presTs;
    const token = ++st.fetchToken;

    // Use a storm-centered 20°×20° bbox that follows the current storm
    // position rather than the full-track bbox. This keeps grid density
    // focused on the area of interest and avoids the massive coarsening
    // (~3°/cell) that occurs when the full track spans 30-50°.
    const _curIdx = window._windfieldState && window._windfieldState.currentIdx;
    const _curPt  = (window.stormData && _curIdx != null && _curIdx >= 0)
                    ? window.stormData[_curIdx] : null;
    let s, w, n, e;
    if (_curPt && _curPt.lat != null && _curPt.lon != null) {
        const PAD = 10; // ±10° → 20°×20° box → ~0.65°/cell at 625-pt cap
        s = Math.max(-60, _curPt.lat - PAD);
        n = Math.min( 60, _curPt.lat + PAD);
        w = _curPt.lon - PAD;
        e = _curPt.lon + PAD;
    } else {
        // Fallback to the pre-computed track bbox if no position is available.
        if (!st.bbox) return;
        [s, w, n, e] = st.bbox;
    }

    let field = null;
    try {
        field = await _overlayFetch('pressure', `${API_BASE}/pressure/field?bbox=${s.toFixed(2)},${w.toFixed(2)},${n.toFixed(2)},${e.toFixed(2)}&ts=${presTs}&res=0.5`);
    } catch (e) {
        console.warn('[PRESSURE] field fetch failed:', e);
        if (token === st.fetchToken) st.currentTs = prevTs;
        // Show error state on the button (mirrors precip behavior)
        const _pbtn = document.getElementById('pressureToggleBtn');
        if (_pbtn) {
            _pbtn.textContent = 'Pressure: Error';
            _pbtn.style.background  = 'rgba(220,38,38,.18)';
            _pbtn.style.borderColor = 'rgba(220,38,38,.4)';
            _pbtn.style.color       = '#fca5a5';
        }
        if (!st.fillLayer) {
            st.enabled = false;
            _pressureButtonReset();
        }
        return;
    }
    if (token !== st.fetchToken || !st.enabled) return;

    // Flip the button state on first successful frame.
    const btn = document.getElementById('pressureToggleBtn');
    const leg = document.getElementById('pressureLegend');
    if (btn) {
        btn.style.background = 'rgba(168,85,247,.22)';
        btn.style.borderColor = 'rgba(168,85,247,.5)';
        btn.style.color = '#d8b4fe';
        btn.textContent = 'Pressure: On';
        btn.disabled = false;
    }
    if (leg) leg.style.display = 'block';

    // Build/update the colored fill overlay.
    // Samples are at cell *centers*; Leaflet aligns the canvas to bounds
    // *corners*. Expand the bounds by half a cell in each direction so the
    // pixel centers sit on top of the sample coordinates — otherwise the
    // whole grid reads as shifted ~half-a-cell SE from its true position
    // (≈55 km at 1° resolution — the "coordinates look wrong" symptom).
    const dataUrl = _renderPressureFillCanvas(field);
    const _pdx2 = (field.dx || 0) / 2;
    const _pdy2 = (field.dy || 0) / 2;
    const bounds  = [
        [field.la2 - _pdy2, field.lo1 - _pdx2],
        [field.la1 + _pdy2, field.lo2 + _pdx2],
    ];
    const opIn    = document.getElementById('pressureOpacityInput');
    const op      = opIn ? (parseInt(opIn.value, 10) / 100) : 0.55;
    if (st.fillLayer) {
        try { currentMap.removeLayer(st.fillLayer); } catch (e) {}
    }
    st.fillLayer = L.imageOverlay(dataUrl, bounds, {
        opacity: op,
        interactive: false,
    }).addTo(currentMap);
    // L.imageOverlay ignores a `zIndex` constructor option — apply it
    // directly to the rendered <img> so the documented stacking order
    // (satellite 200 → cloud 215 → precip 218 → pressure 220 → wind 250)
    // actually holds.
    if (st.fillLayer.getElement) {
        const _pfEl = st.fillLayer.getElement();
        if (_pfEl && _pfEl.style) {
            _pfEl.style.zIndex = 220;
            _pfEl.style.imageRendering = 'auto';
        }
    }

    // Build/update the isobar layer.
    const polylines = _buildIsobarPolylines(field);
    if (st.isobarLayer) {
        try { currentMap.removeLayer(st.isobarLayer); } catch (e) {}
    }
    st.isobarLayer = L.layerGroup();
    polylines.forEach(({latlngs, value, isMain, isStd, isOuter}) => {
        const weight = isStd ? 1.4 : (isMain ? 1.1 : 0.7);
        const color  = isStd ? 'rgba(255,255,255,.85)' : (isMain ? 'rgba(255,255,255,.65)' : 'rgba(255,255,255,.4)');
        const pl = L.polyline(latlngs, {
            color, weight, opacity: 1, interactive: false,
        });
        st.isobarLayer.addLayer(pl);
        // Label every 8 hPa isobar at its midpoint. Skip hole rings so a
        // contour that wraps a low/high doesn't get stamped twice.
        if (isMain && isOuter && latlngs.length > 4) {
            const mid = latlngs[Math.floor(latlngs.length / 2)];
            const lbl = L.marker(mid, {
                interactive: false,
                icon: L.divIcon({
                    className: 'pressure-isobar-label',
                    html: `<span style="background:rgba(15,23,42,.65);color:#fff;font-size:9px;font-weight:600;padding:0 3px;border-radius:2px;font-family:Public Sans,sans-serif;font-variant-numeric:tabular-nums">${value}</span>`,
                    iconSize: [24, 12],
                    iconAnchor: [12, 6],
                }),
            });
            st.isobarLayer.addLayer(lbl);
        }
    });
    st.isobarLayer.addTo(currentMap);

    // Refresh the station pills (independent of the field timestamp).
    if (st.showStations) _refreshPressureStations();
}

async function _refreshPressureStations() {
    const st = window._pressureState;
    if (!st.bbox || !currentMap) return;
    // Dedicated token so two quick scrubs don't let a slow first fetch
    // paint its stale pills over the fresher second fetch's pills.
    const token = ++st.stationToken;
    const [s,w,n,e] = st.bbox;
    let body = null;
    try {
        const r = await fetch(`${API_BASE}/pressure/stations?bbox=${s.toFixed(2)},${w.toFixed(2)},${n.toFixed(2)},${e.toFixed(2)}`);
        if (!r.ok) return;
        body = await r.json();
    } catch (e) {
        console.warn('[PRESSURE] stations fetch failed:', e);
        return;
    }
    // Bail if a newer station refresh has already been kicked off.
    if (token !== st.stationToken || !st.enabled || !st.showStations) return;
    const stations = (body && body.stations) || [];
    if (st.stationLayer) {
        try { currentMap.removeLayer(st.stationLayer); } catch (e) {}
    }
    st.stationLayer = L.layerGroup();
    stations.forEach(s2 => {
        const m = L.marker([s2.lat, s2.lon], {
            interactive: true,
            icon: L.divIcon({
                className: 'pressure-station-pill',
                html: `<div style="background:#fff;color:#0f172a;border:1px solid rgba(15,23,42,.25);border-radius:9px;padding:1px 5px;font-size:9.5px;font-weight:700;font-family:Public Sans,sans-serif;font-variant-numeric:tabular-nums;box-shadow:0 1px 2px rgba(0,0,0,.25);white-space:nowrap">${Math.round(s2.pressure)}</div>`,
                iconSize: [30, 14],
                iconAnchor: [15, 7],
            }),
        });
        m.bindTooltip(`<b>${s2.icao}</b><br>MSLP: ${s2.pressure} mb`, {direction:'top', offset:L.point(0,-6)});
        st.stationLayer.addLayer(m);
    });
    st.stationLayer.addTo(currentMap);
}

window.setPressureOpacity = function(value) {
    const pct = Math.max(10, Math.min(95, parseInt(value, 10) || 55));
    const lbl = document.getElementById('pressureOpacityLabel');
    if (lbl) lbl.textContent = pct + '%';
    const st = window._pressureState;
    if (st && st.fillLayer && st.fillLayer.setOpacity) {
        try { st.fillLayer.setOpacity(pct / 100); } catch (e) {}
    }
};

window.togglePressureStations = function(checked) {
    const st = window._pressureState;
    if (!st) return;
    st.showStations = !!checked;
    if (!st.enabled) return;
    if (checked) {
        _refreshPressureStations();
    } else if (st.stationLayer && currentMap) {
        try { currentMap.removeLayer(st.stationLayer); } catch (e) {}
        st.stationLayer = null;
    }
};

function togglePressureLayer() {
    const st = window._pressureState;
    const btn = document.getElementById('pressureToggleBtn');
    if (st.enabled) {
        st.enabled = false;
        ['fillLayer','isobarLayer','stationLayer'].forEach(k => {
            if (st[k] && currentMap) { try { currentMap.removeLayer(st[k]); } catch(e) {} }
            st[k] = null;
        });
        st.currentTs = null;
        _pressureButtonReset();
    } else {
        if (!window._satelliteState || !window._satelliteState.frames.length || !st.bbox) {
            console.warn('[PRESSURE] cannot enable — satellite slider not initialized');
            return;
        }
        st.enabled = true;
        if (btn) {
            btn.style.background = 'rgba(250,204,21,.18)';
            btn.style.borderColor = 'rgba(250,204,21,.4)';
            btn.style.color = '#fde047';
            btn.textContent = 'Pressure: Loading…';
            btn.disabled = true;
        }
        // d3-contour is lazy-loaded — only the isobar overlay needs it.
        // _buildIsobarPolylines() returns [] gracefully if d3 isn't ready,
        // so we fire-and-forget; the next pressure-frame tick picks up
        // contours once the script lands.
        loadD3Contour().catch(e => console.warn('[PRESSURE] d3-contour load failed:', e));
        const satTs = window._satelliteState.frames[window._satelliteState.currentIdx];
        setPressureFrame(satTs);
    }
}
```

### 4. Precipitation + cloud-cover layer (/precip/field)

```js
// ── Precipitation + cloud-cover layer ─────────────────────────────────────
// Two co-registered canvas overlays:
//   - cloudLayer : greyscale alpha (cloud cover %) underneath
//   - precipLayer: multi-stop color ramp (Light → Severe mm/hr) on top
// Both fetched in one /precip/field call backed by Open-Meteo. Driven by
// the satellite scrub via setPrecipFrame, debounced 200 ms.
window._precipState = {
    cloudLayer:    null,
    precipLayer:   null,
    enabled:       false,
    showCloud:     true,
    bbox:          null,
    currentTs:     null,
    inflightTs:    null,    // guards against duplicate in-flight fetches
    fetchToken:    0,
    debounceTimer: null,
    pendingTs:     null,
    // Cache of the last successfully-fetched /precip/field payload so that
    // toggling the cloud-cover checkbox can re-render from memory without
    // re-fetching from Open-Meteo.
    lastPayload:   null,
    lastBounds:    null,
};

function _precipButtonReset() {
    const btn = document.getElementById('precipToggleBtn');
    const legend = document.getElementById('precipLegend');
    if (btn) {
        btn.style.background  = 'rgba(100,116,139,.18)';
        btn.style.borderColor = 'rgba(100,116,139,.4)';
        btn.style.color       = '#94a3b8';
        btn.textContent       = 'Precip: Off';
        btn.disabled          = false;
    }
    if (legend) legend.style.display = 'none';
}

// Precip color ramp: maps mm/hr → [r,g,b,a]. Anchored to the legend gradient
// (light blue 0.1 → blue 1 → yellow 4 → orange 10 → magenta/red 25+).
function _precipColor(mm) {
    if (mm == null || mm < 0.1) return [0, 0, 0, 0]; // transparent below threshold
    // Stops: (mm, [r,g,b])
    const stops = [
        [0.1, [191, 219, 254]],  // #bfdbfe light blue
        [1.0, [ 96, 165, 250]],  // #60a5fa medium blue
        [4.0, [ 37,  99, 235]],  // #2563eb deep blue
        [10.0,[250, 204,  21]],  // #facc15 yellow
        [25.0,[249, 115,  22]],  // #f97316 orange
        [50.0,[225,  29,  72]],  // #e11d48 magenta-red
    ];
    let lo = stops[0], hi = stops[stops.length - 1];
    if (mm >= hi[0]) {
        return [hi[1][0], hi[1][1], hi[1][2], 230];
    }
    for (let i = 0; i < stops.length - 1; i++) {
        if (mm >= stops[i][0] && mm < stops[i+1][0]) {
            lo = stops[i]; hi = stops[i+1]; break;
        }
    }
    const t = (mm - lo[0]) / (hi[0] - lo[0]);
    const r = Math.round(lo[1][0] + (hi[1][0] - lo[1][0]) * t);
    const g = Math.round(lo[1][1] + (hi[1][1] - lo[1][1]) * t);
    const b = Math.round(lo[1][2] + (hi[1][2] - lo[1][2]) * t);
    // Fade in the alpha from threshold so light drizzle isn't a sharp edge.
    const a = Math.min(230, 90 + Math.round(140 * Math.min(1, mm / 5)));
    return [r, g, b, a];
}

function _renderPrecipFillCanvas(precipRows) {
    const ny = precipRows.length;
    const nx = precipRows[0].length;
    const c = document.createElement('canvas');
    c.width = nx; c.height = ny;
    const ctx = c.getContext('2d');
    const img = ctx.createImageData(nx, ny);
    for (let j = 0; j < ny; j++) {
        const row = precipRows[j];
        for (let i = 0; i < nx; i++) {
            const [r, g, b, a] = _precipColor(row[i]);
            const k = (j * nx + i) * 4;
            img.data[k]   = r;
            img.data[k+1] = g;
            img.data[k+2] = b;
            img.data[k+3] = a;
        }
    }
    ctx.putImageData(img, 0, 0);
    return c.toDataURL('image/png');
}

function _renderCloudCanvas(cloudRows) {
    const ny = cloudRows.length;
    const nx = cloudRows[0].length;
    const c = document.createElement('canvas');
    c.width = nx; c.height = ny;
    const ctx = c.getContext('2d');
    const img = ctx.createImageData(nx, ny);
    for (let j = 0; j < ny; j++) {
        const row = cloudRows[j];
        for (let i = 0; i < nx; i++) {
            // Cloud cover 0..100 → white with alpha 0..150 (≈59%).
            const pct = Math.max(0, Math.min(100, row[i] || 0));
            const a = Math.round(pct * 1.5);
            const k = (j * nx + i) * 4;
            img.data[k]   = 235;
            img.data[k+1] = 240;
            img.data[k+2] = 245;
            img.data[k+3] = a;
        }
    }
    ctx.putImageData(img, 0, 0);
    return c.toDataURL('image/png');
}

function setPrecipFrame(satelliteTs) {
    const st = window._precipState;
    if (!st.enabled || !st.bbox || !currentMap) return;
    st.pendingTs = satelliteTs;
    if (st.debounceTimer) clearTimeout(st.debounceTimer);
    st.debounceTimer = setTimeout(() => {
        st.debounceTimer = null;
        const ts = st.pendingTs;
        st.pendingTs = null;
        if (ts) _setPrecipFrameNow(ts);
    }, 200);
}

async function _setPrecipFrameNow(satelliteTs) {
    const st = window._precipState;
    if (!st.enabled || !currentMap) return;
    // Round satellite ts (e.g. "20260413T1130") to the hour the model sample lives at.
    const m = String(satelliteTs).match(/^(\d{4})(\d{2})(\d{2})T(\d{2})/);
    if (!m) return;
    const ts = `${m[1]}${m[2]}${m[3]}T${m[4]}`;
    if (st.currentTs === ts) return;
    // Also bail if we've already kicked off (but not yet rendered) a fetch
    // for this same hour — the post-await write of st.currentTs was too
    // late to gate a second call that slipped through during the await,
    // causing a redundant network request for identical data.
    if (st.inflightTs === ts) return;
    st.inflightTs = ts;

    const token = ++st.fetchToken;

    // Use a storm-centered 20°×20° bbox (same approach as pressure) so grid
    // density stays focused on the storm rather than the full track extent.
    const _curIdx = window._windfieldState && window._windfieldState.currentIdx;
    const _curPt  = (window.stormData && _curIdx != null && _curIdx >= 0)
                    ? window.stormData[_curIdx] : null;
    let bboxArr;
    if (_curPt && _curPt.lat != null && _curPt.lon != null) {
        const PAD = 10;
        const ps = Math.max(-60, _curPt.lat - PAD);
        const pn = Math.min( 60, _curPt.lat + PAD);
        const pw = _curPt.lon - PAD;
        const pe = _curPt.lon + PAD;
        bboxArr = [ps, pw, pn, pe];
    } else {
        if (!st.bbox) return;
        bboxArr = st.bbox;
    }
    const bbox = bboxArr.map(v => v.toFixed(2)).join(',');
    const url   = `${API_BASE}/precip/field?bbox=${bbox}&ts=${ts}&res=0.5`;

    const btn = document.getElementById('precipToggleBtn');
    const leg = document.getElementById('precipLegend');
    if (btn) btn.textContent = 'Precip: Loading…';

    let payload;
    try {
        payload = await _overlayFetch('precip', url);
    } catch (e) {
        // Clear the in-flight marker so a retry for the same ts isn't
        // permanently suppressed by the early-return guard above.
        if (st.inflightTs === ts) st.inflightTs = null;
        if (token !== st.fetchToken) return;
        console.warn('[PRECIP] fetch failed:', e);
        if (btn) {
            btn.textContent = 'Precip: Error';
            btn.style.background  = 'rgba(220,38,38,.18)';
            btn.style.borderColor = 'rgba(220,38,38,.4)';
            btn.style.color       = '#fca5a5';
        }
        return;
    }
    if (token !== st.fetchToken || !st.enabled) {
        if (st.inflightTs === ts) st.inflightTs = null;
        return;
    }

    // Half-cell expand — see the matching note in _setPressureFrameNow. Samples
    // are at cell centers, so the bounds need to extend dx/2, dy/2 beyond the
    // outermost sample for the pixel centers to land on the sample positions.
    const _ppdx2 = (payload.dx || 0) / 2;
    const _ppdy2 = (payload.dy || 0) / 2;
    const bounds = [
        [payload.la2 - _ppdy2, payload.lo1 - _ppdx2],
        [payload.la1 + _ppdy2, payload.lo2 + _ppdx2],
    ];

    // Cloud cover (background, lower z-index)
    if (st.cloudLayer && currentMap) { try { currentMap.removeLayer(st.cloudLayer); } catch(e) {} st.cloudLayer = null; }
    if (st.showCloud) {
        const cloudUrl = _renderCloudCanvas(payload.cloud);
        st.cloudLayer = L.imageOverlay(cloudUrl, bounds, {
            opacity: 0.5, interactive: false, className: 'precip-cloud-overlay',
        }).addTo(currentMap);
        if (st.cloudLayer.getElement) {
            const el = st.cloudLayer.getElement();
            if (el && el.style) {
                el.style.zIndex = 215;
                el.style.imageRendering = 'auto';
            }
        }
    }

    // Precipitation rate (foreground)
    if (st.precipLayer && currentMap) { try { currentMap.removeLayer(st.precipLayer); } catch(e) {} st.precipLayer = null; }
    const opIn = document.getElementById('precipOpacityInput');
    const op   = opIn ? (parseInt(opIn.value, 10) / 100) : 0.75;
    const dataUrl = _renderPrecipFillCanvas(payload.precip);
    st.precipLayer = L.imageOverlay(dataUrl, bounds, {
        opacity: op, interactive: false, className: 'precip-fill-overlay',
    }).addTo(currentMap);
    if (st.precipLayer.getElement) {
        const el = st.precipLayer.getElement();
        if (el && el.style) {
            el.style.zIndex = 218;
            el.style.imageRendering = 'auto';
        }
    }

    st.currentTs   = ts;
    st.lastPayload = payload;
    st.lastBounds  = bounds;
    if (st.inflightTs === ts) st.inflightTs = null;
    if (btn) {
        btn.textContent       = 'Precip: On';
        btn.style.background  = 'rgba(96,165,250,.20)';
        btn.style.borderColor = 'rgba(96,165,250,.45)';
        btn.style.color       = '#bfdbfe';
        btn.disabled          = false;
    }
    if (leg) leg.style.display = 'block';
}

window.setPrecipOpacity = function(value) {
    const lbl = document.getElementById('precipOpacityLabel');
    if (lbl) lbl.textContent = value + '%';
    const st = window._precipState;
    if (!st || !st.precipLayer) return;
    try { st.precipLayer.setOpacity(parseInt(value, 10) / 100); } catch(e) {}
};

window.togglePrecipCloud = function(checked) {
    const st = window._precipState;
    if (!st) return;
    st.showCloud = !!checked;
    if (!st.enabled) return;
    if (!checked) {
        if (st.cloudLayer && currentMap) {
            try { currentMap.removeLayer(st.cloudLayer); } catch(e) {}
        }
        st.cloudLayer = null;
        return;
    }
    // Re-render cloud from the cached payload if available — no re-fetch.
    if (st.lastPayload && st.lastBounds && currentMap) {
        if (st.cloudLayer) { try { currentMap.removeLayer(st.cloudLayer); } catch(e) {} st.cloudLayer = null; }
        const cloudUrl = _renderCloudCanvas(st.lastPayload.cloud);
        st.cloudLayer = L.imageOverlay(cloudUrl, st.lastBounds, {
            opacity: 0.5, interactive: false, className: 'precip-cloud-overlay',
        }).addTo(currentMap);
        if (st.cloudLayer.getElement) {
            const el = st.cloudLayer.getElement();
            if (el && el.style) {
                el.style.zIndex = 215;
                el.style.imageRendering = 'auto';
            }
        }
    } else if (st.currentTs) {
        // No cached payload (shouldn't normally happen) — fall back to refetch.
        const wasTs = st.currentTs;
        st.currentTs = null;
        _setPrecipFrameNow(wasTs);
    }
};

function togglePrecipLayer() {
    const st = window._precipState;
    const btn = document.getElementById('precipToggleBtn');
    if (st.enabled) {
        st.enabled = false;
        ['cloudLayer','precipLayer'].forEach(k => {
            if (st[k] && currentMap) { try { currentMap.removeLayer(st[k]); } catch(e) {} }
            st[k] = null;
        });
        st.currentTs = null;
        st.inflightTs = null;
        _precipButtonReset();
    } else {
        if (!window._satelliteState || !window._satelliteState.frames.length || !st.bbox) {
            console.warn('[PRECIP] cannot enable — satellite slider not initialized');
            return;
        }
        st.enabled = true;
        if (btn) {
            btn.style.background  = 'rgba(250,204,21,.18)';
            btn.style.borderColor = 'rgba(250,204,21,.4)';
            btn.style.color       = '#fde047';
            btn.textContent       = 'Precip: Loading…';
            btn.disabled          = true;
        }
        const satTs = window._satelliteState.frames[window._satelliteState.currentIdx];
        setPrecipFrame(satTs);
    }
}
```

### 5. Satellite visibility + Infrared toggles, scrubber handler

The satellite LAYER is still always-on; these were the panel's show/hide and manual-IR controls. Automatic night-IR (`_autoIRForFrame`) is separate and still live.

```js
// Show/hide the satellite tile layer on the map.
window.toggleSatelliteVisibility = function() {
    const st = window._satelliteState;
    if (!st.layer || !currentMap) return;
    st.visible = !st.visible;
    const btn = document.getElementById('satToggleBtn');
    if (st.visible) {
        st.layer.addTo(currentMap);
        if (btn) {
            btn.textContent    = 'Satellite: On';
            btn.style.background  = 'rgba(34,211,238,.18)';
            btn.style.borderColor = 'rgba(34,211,238,.4)';
            btn.style.color       = '#67e8f9';
        }
    } else {
        currentMap.removeLayer(st.layer);
        // Also cancel any in-flight double-buffered swap so its 'load'
        // callback can't re-add imagery after the user hid the layer.
        st._swapToken = (st._swapToken || 0) + 1;
        if (st._pendingLayer) {
            try { currentMap.removeLayer(st._pendingLayer); } catch (e) {}
            st._pendingLayer = null;
        }
        if (btn) {
            btn.textContent    = 'Satellite: Off';
            btn.style.background  = 'rgba(100,116,139,.18)';
            btn.style.borderColor = 'rgba(100,116,139,.4)';
            btn.style.color       = '#94a3b8';
        }
    }
};

// Toggle between visible (GeoColor) and infrared satellite imagery.
// Re-inits the satellite layer with the new mode so tiles come from the
// correct GIBS product. IR can be toggled even when Satellite is Off —
// it will initialize the satellite layer in IR mode independently.
window.toggleIRMode = function() {
    const st = window._satelliteState;
    st.irMode = !st.irMode;
    // Update the button appearance immediately
    const btn = document.getElementById('irToggleBtn');
    if (btn) {
        if (st.irMode) {
            btn.textContent    = 'Infrared: On';
            btn.style.background  = 'rgba(168,85,247,.18)';
            btn.style.borderColor = 'rgba(168,85,247,.4)';
            btn.style.color       = '#c4b5fd';
        } else {
            btn.textContent    = 'Infrared: Off';
            btn.style.background  = 'rgba(100,116,139,.18)';
            btn.style.borderColor = 'rgba(100,116,139,.4)';
            btn.style.color       = '#94a3b8';
        }
    }
    // Re-init satellite layer — this re-fetches frames with the new mode
    // which gives us the correct max_zoom for the new GIBS product.
    // { keepOverlays: true } preserves the wind/pressure/precip layers;
    // only the satellite tile layer itself swaps products.
    if (window.stormData && window.stormData.length) {
        initSatelliteLayer(window.stormData, { keepOverlays: true });
    }
};

function onSatelliteFrameScrub(value) {
    setSatelliteFrame(parseInt(value, 10));
}

// (Satellite playback was removed — the Storm Timeline drives both layers.)
```

### 6. _computeStormBbox (dateline-aware padded bbox)

Only these three layers consumed it; generic and reusable.

```js
// Dateline-aware padded bbox for a storm track. Returns [s, w, n, e] with
// w > e signalling a dateline-crossing range (the backend's grid builder
// reads that as "walk east across 180°"). Extracted so historic-storm
// setup can reuse it without going through the satellite-layer init path.
function _computeStormBbox(data, pad) {
    const lats = data.map(d => d.lat), lons = data.map(d => d.lon);
    const _normLon = (x) => ((x + 540) % 360) - 180;
    const srt = [...lons].sort((a, b) => a - b);
    let w, e;
    if (srt.length <= 1) {
        w = e = srt[0] || 0;
    } else {
        let maxGap = 360 - (srt[srt.length - 1] - srt[0]);
        let gapIdx = -1;
        for (let i = 1; i < srt.length; i++) {
            const g = srt[i] - srt[i - 1];
            if (g > maxGap) { maxGap = g; gapIdx = i; }
        }
        if (gapIdx > 0) { w = srt[gapIdx]; e = srt[gapIdx - 1]; }
        else            { w = srt[0];      e = srt[srt.length - 1]; }
    }
    const s = Math.max(-60, Math.min(...lats) - pad);
    const n = Math.min( 60, Math.max(...lats) + pad);
    return [s, _normLon(w - pad), n, _normLon(e + pad)];
}
```

## 7: Wiring touchpoints *(current at excision — paste into the named hosts)*

### 7a. setSatelliteFrame tail — drive wind/pressure/precip from each satellite frame

Anchor: end of `setSatelliteFrame`, after the `syncedSatTs` echo block.

```js
    // Sync the wind layer (no-op if disabled or same hour)
    if (window._windState && window._windState.enabled) setWindFrame(ts);
    // Sync the pressure layer (no-op if disabled or same hour)
    if (window._pressureState && window._pressureState.enabled) setPressureFrame(ts);
    // Sync the precip layer (no-op if disabled or same hour)
    if (window._precipState && window._precipState.enabled) setPrecipFrame(ts);
```

### 7b. initSatelliteLayer tail — cache per-layer bboxes + reset wind prefetch

Anchor: end of `initSatelliteLayer`, after the `syncOverlaysToTimestampMs` first-paint snap.

```js
    // Cache the storm bbox on the wind + pressure states so both layers can
    // fetch matching frames without re-deriving it. ±5° padding gives the
    // wind particles room to flow off the storm center; pressure benefits
    // from a wider context (±8°) so the synoptic field reads correctly.
    window._windState.bbox     = _computeStormBbox(data, 5);
    window._pressureState.bbox = _computeStormBbox(data, 8);
    window._precipState.bbox   = _computeStormBbox(data, 5);
    // Bbox changed (likely new storm) — invalidate wind prefetch state so
    // the next animation play re-prefetches for this storm's bbox+timestamps.
    if (window._windState.prefetched) {
        window._windState.prefetched = new Set();
        window._windState.prefetchStarted = false;
    }
```

### 7c. teardownSatelliteLayer — wind/pressure/precip state+button resets

Anchor: inside `teardownSatelliteLayer`, between `if (keepOverlays) return;` and the windfield NOTE comment (layer resets), and just before `st.visible = true;` (button resets).

```js
    // Tear down the wind layer too — its bbox is tied to the satellite session.
    const w = window._windState;
    if (w) {
        if (w.layer && currentMap) {
            try { currentMap.removeLayer(w.layer); } catch(e) {}
        }
        w.layer = null;
        w.enabled = false;
        w.bbox = null;
        w.currentTs = null;
        const wbtn = document.getElementById('windToggleBtn');
        if (wbtn) {
            wbtn.disabled = true;
            wbtn.title = 'Loading satellite frames…';
            wbtn.style.cursor = 'not-allowed';
            wbtn.style.opacity = '.6';
            wbtn.style.background = 'rgba(100,116,139,.10)';
            wbtn.style.borderColor = 'rgba(100,116,139,.25)';
            wbtn.style.color = '#94a3b8';
            wbtn.textContent = 'Wind: Off';
        }
        const wleg = document.getElementById('windLegend');
        if (wleg) wleg.style.display = 'none';
    }
    // Tear down the pressure layer too — it's also bbox-tied to the session.
    const p = window._pressureState;
    if (p) {
        ['fillLayer','isobarLayer','stationLayer'].forEach(k => {
            if (p[k] && currentMap) { try { currentMap.removeLayer(p[k]); } catch(e) {} }
            p[k] = null;
        });
        p.enabled = false;
        p.bbox = null;
        p.currentTs = null;
        const pbtn = document.getElementById('pressureToggleBtn');
        if (pbtn) {
            pbtn.disabled = true;
            pbtn.title = 'Loading satellite frames…';
            pbtn.style.cursor = 'not-allowed';
            pbtn.style.opacity = '.6';
            pbtn.style.background = 'rgba(100,116,139,.10)';
            pbtn.style.borderColor = 'rgba(100,116,139,.25)';
            pbtn.style.color = '#94a3b8';
            pbtn.textContent = 'Pressure: Off';
        }
        const pleg = document.getElementById('pressureLegend');
        if (pleg) pleg.style.display = 'none';
    }
    // Tear down the precip layer.
    const pp = window._precipState;
    if (pp) {
        ['cloudLayer','precipLayer'].forEach(k => {
            if (pp[k] && currentMap) { try { currentMap.removeLayer(pp[k]); } catch(e) {} }
            pp[k] = null;
        });
        pp.enabled = false;
        pp.bbox = null;
        pp.currentTs = null;
        pp.inflightTs = null;
        pp.lastPayload = null;
        pp.lastBounds = null;
        const ppbtn = document.getElementById('precipToggleBtn');
        if (ppbtn) {
            ppbtn.disabled = true;
            ppbtn.title = 'Loading satellite frames…';
            ppbtn.style.cursor = 'not-allowed';
            ppbtn.style.opacity = '.6';
            ppbtn.style.background = 'rgba(100,116,139,.10)';
            ppbtn.style.borderColor = 'rgba(100,116,139,.25)';
            ppbtn.style.color = '#94a3b8';
            ppbtn.textContent = 'Precip: Off';
        }
        const ppleg = document.getElementById('precipLegend');
        if (ppleg) ppleg.style.display = 'none';
    }

    // Reset IR toggle button (but keep irMode preference — it persists across storms).
    const _irBtn = document.getElementById('irToggleBtn');
    if (_irBtn) {
        _irBtn.disabled = true;
        _irBtn.title = 'Loading satellite frames…';
        _irBtn.style.cursor = 'not-allowed';
        _irBtn.style.opacity = '.6';
        _irBtn.style.background = 'rgba(100,116,139,.10)';
        _irBtn.style.borderColor = 'rgba(100,116,139,.25)';
        _irBtn.style.color = '#94a3b8';
        _irBtn.textContent = 'Infrared: Off';
    }
    // Reset Satellite show/hide toggle.
    const _satBtn = document.getElementById('satToggleBtn');
    if (_satBtn) {
        _satBtn.disabled = true;
        _satBtn.title = 'Loading satellite frames…';
        _satBtn.style.cursor = 'not-allowed';
        _satBtn.style.opacity = '.6';
        _satBtn.style.background = 'rgba(100,116,139,.10)';
        _satBtn.style.borderColor = 'rgba(100,116,139,.25)';
        _satBtn.style.color = '#94a3b8';
        _satBtn.textContent = 'Satellite: Off';
    }
```

### 7d. Timeline scrub (syncVisualization) — historic-storm direct wind drive

Anchor: in the timeline-sync block, as the `else` branch of `if (window._isActiveStorm && window.syncOverlaysToTimestampMs)`.

```js
            } else if (window._windState && window._windState.enabled && window._tsMsToSatTsString) {
                const satTs = window._tsMsToSatTsString(new Date(d1.timestamp).getTime());
                if (satTs) setWindFrame(satTs);
            }
```

## 8-13: Panel-era UI + wiring *(c841e96)*

### 8. Map buttons + Advanced overlay panel HTML

Goes inside the map container, next to `mapLegendToggle`. Includes the Wind-flow button (now always-on, buttonless) and the Advanced-view button.

```html
                <button class="map-legend-toggle" id="windDirToggleBtn" style="bottom:38px;left:8px;display:flex" onclick="toggleWindDirLayer()" title="Animated wind-flow direction (storm-parametric)">Wind flow: Off</button>
                <button class="map-legend-toggle" id="observedToggleBtn" style="bottom:68px;left:8px;display:none" onclick="toggleObservedLayer()" title="Observed peak storm surge at NOAA tide gauges (and buoy wind/waves where available)">Observed surge: Off</button>
                <button class="map-legend-toggle" id="advViewToggleBtn" style="bottom:98px;left:8px;display:flex" onclick="toggleAdvancedView()" title="Advanced view — satellite imagery, infrared, wind field, wind particles, pressure and precipitation overlays">Advanced view &#9654;</button>
                <!-- Advanced overlay panel. Each control sits in its own .adv-row
                     because the layer JS shows/hides rows via btn.parentElement
                     (historic storms hide Satellite/IR/Pressure/Precip; active
                     storms restore them). Buttons start disabled — the layer
                     init paths enable them and manage all state/colors. -->
                <div id="advOverlayPanel" class="adv-overlay-panel">
                    <div class="adv-row adv-desktop-only"><button id="satToggleBtn" class="adv-toggle" onclick="toggleSatelliteVisibility()" disabled title="Loads when a storm with satellite coverage is displayed">Satellite: Off</button></div>
                    <div class="adv-row adv-desktop-only"><button id="irToggleBtn" class="adv-toggle" onclick="toggleIRMode()" disabled title="Switch satellite imagery between visible and infrared">Infrared: Off</button></div>
                    <div class="adv-row"><button id="windfieldToggleBtn" class="adv-toggle" onclick="toggleWindfieldLayer()" disabled title="Holland B wind-field overlay">Windfield: Off</button></div>
                    <div class="adv-row adv-desktop-only"><button id="storm3dToggleBtn" class="adv-toggle" onclick="toggleStorm3D()" disabled title="3D storm portrait — wind particles, radii stamps and cloud canopy over the satellite plane">3D view: Off</button></div>
                    <div class="adv-row adv-desktop-only"><button id="windToggleBtn" class="adv-toggle" onclick="toggleWindLayer()" disabled title="Wind particle overlay">Wind: Off</button><input type="range" id="windOpacityInput" class="adv-opacity" min="10" max="95" value="70" oninput="setWindOpacity(this.value)" title="Wind layer opacity"><span id="windOpacityLabel" class="adv-opacity-lbl">70%</span></div>
                    <div class="adv-row adv-desktop-only"><button id="pressureToggleBtn" class="adv-toggle" onclick="togglePressureLayer()" disabled title="MSLP field, isobars, and station obs">Pressure: Off</button><input type="range" id="pressureOpacityInput" class="adv-opacity" min="10" max="95" value="55" oninput="setPressureOpacity(this.value)" title="Pressure layer opacity"><span id="pressureOpacityLabel" class="adv-opacity-lbl">55%</span></div>
                    <div class="adv-row adv-desktop-only"><button id="precipToggleBtn" class="adv-toggle" onclick="togglePrecipLayer()" disabled title="Precipitation rate + cloud cover">Precip: Off</button><input type="range" id="precipOpacityInput" class="adv-opacity" min="10" max="95" value="75" oninput="setPrecipOpacity(this.value)" title="Precipitation layer opacity"><span id="precipOpacityLabel" class="adv-opacity-lbl">75%</span></div>
                    <div id="satelliteSlider" class="adv-sat-slider adv-desktop-only" style="display:none">
                        <div class="adv-sat-head"><span id="satelliteName"></span><span id="satelliteProductLabel"></span></div>
                        <input type="range" id="satelliteSliderInput" min="0" max="0" value="0" oninput="onSatelliteFrameScrub(this.value)" title="Scrub satellite frames">
                        <div id="satelliteFrameLabel" class="adv-sat-frame"></div>
                    </div>
                    <div id="windLegend" class="adv-legend" style="display:none"><div class="adv-legend-title">Wind speed</div><div class="adv-legend-bar" style="background:linear-gradient(90deg,#6366f1,#0891b2,#16a34a,#ca8a04,#ea580c,#dc2626,#9d174d)"></div><div class="adv-legend-lbls"><span>light</span><span>storm peak</span></div></div>
                    <div id="pressureLegend" class="adv-legend" style="display:none"><div class="adv-legend-title">Sea-level pressure</div><div class="adv-legend-bar" style="background:linear-gradient(90deg,rgb(0,0,60),rgb(30,80,200),rgb(148,224,252),rgb(248,250,252),rgb(249,155,50),rgb(127,29,29))"></div><div class="adv-legend-lbls"><span>900 mb</span><span>1048 mb</span></div></div>
                    <div id="precipLegend" class="adv-legend" style="display:none"><div class="adv-legend-title">Precip rate (mm/h)</div><div class="adv-legend-bar" style="background:linear-gradient(90deg,#bfdbfe,#60a5fa,#2563eb,#facc15,#f97316,#e11d48)"></div><div class="adv-legend-lbls"><span>0.1</span><span>50+</span></div></div>
                </div>
```

### 9. Panel CSS

Includes the `.adv-desktop-only` ≤900px rule that kept phones on the simplified layer set.

```css
        /* ── Advanced view — overlay control panel ──
           Satellite / IR / windfield / wind-particle / pressure / precip
           controls. The layer JS + backend routes predate this panel; the
           DOM controls were the missing piece (LOOSE_ENDS_AUDIT.md A1).
           Dark chrome to match the JS-managed button color idiom. */
        .adv-overlay-panel{position:absolute;bottom:128px;left:8px;z-index:502;background:rgba(15,23,42,.88);backdrop-filter:blur(8px);border:1px solid rgba(100,116,139,.4);border-radius:var(--radius);padding:.45rem .5rem;display:none;flex-direction:column;gap:.3rem;width:220px;max-height:65%;overflow-y:auto}
        /* Mobile keeps the simplified layer set: Windfield + Wind flow only.
           !important so the JS row show/hide (inline display writes on storm
           switch) can never resurface a desktop-only row on a phone. */
        @media(max-width:900px){.adv-overlay-panel .adv-desktop-only{display:none!important}}
        .adv-row{display:flex;align-items:center;gap:.35rem}
        .adv-toggle{flex-shrink:0;background:rgba(100,116,139,.18);border:1px solid rgba(100,116,139,.4);color:#94a3b8;font-size:.72rem;font-weight:600;padding:.28rem .5rem;border-radius:var(--radius);cursor:pointer;font-family:inherit;min-width:106px;text-align:left;transition:background .15s,color .15s}
        .adv-toggle:disabled{opacity:.5;cursor:not-allowed}
        .adv-opacity{flex:1;min-width:0;accent-color:#60a5fa;cursor:pointer}
        .adv-opacity-lbl{font-size:.65rem;color:#94a3b8;min-width:32px;text-align:right;font-variant-numeric:tabular-nums}
        .adv-sat-slider{border-top:1px solid rgba(100,116,139,.3);padding-top:.3rem}
        .adv-sat-head{font-size:.65rem;color:#cbd5e1;font-weight:700;display:flex;gap:.35rem;justify-content:space-between}
        .adv-sat-slider input{width:100%;accent-color:#22d3ee;cursor:pointer}
        .adv-sat-frame{font-size:.65rem;color:#94a3b8;font-variant-numeric:tabular-nums}
        .adv-legend{border-top:1px solid rgba(100,116,139,.3);padding-top:.3rem}
        .adv-legend-title{font-size:.62rem;font-weight:700;color:#cbd5e1;text-transform:uppercase;letter-spacing:.04em}
        .adv-legend-bar{height:8px;border-radius:3px;margin:.2rem 0}
        .adv-legend-lbls{display:flex;justify-content:space-between;font-size:.6rem;color:#94a3b8}
```

### 10. toggleAdvancedView + localStorage boot restore

Panel open/close + lazy layer wiring; persisted per browser as `stormdps_advview`.

```js
// Advanced view — reveals the overlay control panel (satellite / infrared /
// windfield / wind particles / pressure / precip). The layer machinery and
// backend routes predate this; the DOM controls were the missing piece
// (docs/audits/LOOSE_ENDS_AUDIT.md A1). Off by default so the default UI is
// untouched; the choice is remembered per browser.
function toggleAdvancedView() {
    const panel = document.getElementById('advOverlayPanel');
    const btn = document.getElementById('advViewToggleBtn');
    if (!panel) return;
    const show = getComputedStyle(panel).display === 'none';
    panel.style.display = show ? 'flex' : 'none';
    if (btn) btn.innerHTML = show ? 'Advanced view &#9660;' : 'Advanced view &#9654;';
    try { localStorage.setItem('stormdps_advview', show ? '1' : '0'); } catch (e) {}
    // Lazy layer wiring — layers only initialize when the panel is first
    // opened, so the default view does zero extra work. Active storms boot
    // the satellite/overlay stack (initSatelliteLayer enables the other
    // buttons when frames arrive); historic storms get the ERA5 wind +
    // windfield subset via enableHistoricOverlays (defined for this purpose
    // but never wired to a caller until now — LOOSE_ENDS_AUDIT.md A1).
    if (show && window.stormData && window.stormData.length) {
        try {
            const st = window._satelliteState;
            if (window._isActiveStorm) {
                if (!st || !st.layer) initSatelliteLayer(window.stormData);
            } else {
                enableHistoricOverlays(window.stormData);
            }
        } catch (e) { console.warn('[ADV] overlay wiring failed:', e); }
    }
}
// Restore the persisted choice on boot (panel markup precedes this script).
try { if (localStorage.getItem('stormdps_advview') === '1') toggleAdvancedView(); } catch (e) {}
```

### 11. enableHistoricOverlays

Historic storms can't have satellite tiles; this wired the ERA5 wind + windfield subset.

```js
// Historic/archived storms can't show satellite tiles (GIBS archive depth
// is ~14 days), so initSatelliteLayer bails before enabling any overlay
// toggles. Wind (leaflet-velocity backed by ERA5) and Windfield (pure
// client-side wind radii) don't need satellite imagery — wire them up here
// so the historic-storm workflow still gets a working wind map.
function enableHistoricOverlays(data) {
    if (!data || !data.length || !currentMap) return;
    window._windState.bbox = _computeStormBbox(data, 5);
    if (window._windState.prefetched) {
        window._windState.prefetched = new Set();
        window._windState.prefetchStarted = false;
    }

    // Panel visibility is user-controlled via the Advanced view toggle
    // (advOverlayPanel). The satellite frame scrubber (satelliteSlider)
    // stays hidden for historic storms — GIBS archive only covers ~14 days,
    // so there are no frames to scrub.

    // Hide the overlays that aren't meaningful for historic storms (satellite
    // imagery / IR only covers ~14 days of GIBS archive; pressure + precip
    // are already shown in the side charts for historic analysis). Wind +
    // Windfield are the only overlays the user wants on the map here.
    ['satToggleBtn', 'irToggleBtn', 'pressureToggleBtn', 'precipToggleBtn'].forEach(id => {
        const b = document.getElementById(id);
        if (b && b.parentElement) b.parentElement.style.display = 'none';
    });

    const wbtn = document.getElementById('windToggleBtn');
    if (wbtn) {
        wbtn.disabled = false;
        wbtn.title = 'Toggle wind particle overlay (ERA5 reanalysis)';
        wbtn.style.cursor = 'pointer';
        wbtn.style.opacity = '1';
        wbtn.style.background = 'rgba(100,116,139,.18)';
        wbtn.style.borderColor = 'rgba(100,116,139,.4)';
        wbtn.style.color = '#94a3b8';
    }
    _enableWindfieldToggle();
}
```

### 12. _enableWindfieldToggle + initSatelliteLayer button-enable tail

Enables/paints the panel buttons once frames arrive; wires the frame scrubber.

```js
// Enable the Windfield toggle button. Every overlay-wiring path (active-storm
// satellite init, historic subset, simplified mobile set) needs this. Holland B
// uses estimated R34 when per-quadrant radii are missing, so it's always safe
// once track data exists. Paint matches the layer's ACTUAL state — the layer
// auto-enables at storm load, usually before any of those paths run.
function _enableWindfieldToggle() {
    const btn = document.getElementById('windfieldToggleBtn');
    if (!btn) return;
    btn.disabled = false;
    btn.title = 'Toggle Holland B wind-field overlay (2D colored gradient, per-frame)';
    btn.style.cursor = 'pointer';
    btn.style.opacity = '1';
    const on = !!(window._windfieldState && window._windfieldState.enabled);
    btn.style.background  = on ? 'rgba(234,179,8,.18)' : 'rgba(100,116,139,.18)';
    btn.style.borderColor = on ? 'rgba(234,179,8,.45)' : 'rgba(100,116,139,.4)';
    btn.style.color       = on ? '#fde68a' : '#94a3b8';
}

    // Wire slider UI
    const slider = document.getElementById('satelliteSliderInput');
    const wrap = document.getElementById('satelliteSlider');
    const nameEl = document.getElementById('satelliteName');
    if (slider) {
        slider.min = 0;
        slider.max = frames.length - 1;
        slider.value = st.currentIdx;
    }
    if (nameEl) nameEl.textContent = sat.replace('-', ' ').toUpperCase();
    const labelEl = document.getElementById('satelliteProductLabel');
    if (labelEl) labelEl.textContent = layerLabel || '\u00a0';
    if (wrap) wrap.style.display = 'block';
    // Enable the Satellite show/hide toggle (starts "On" since layer is visible).
    const _satReady = document.getElementById('satToggleBtn');
    if (_satReady) {
        _satReady.disabled = false;
        _satReady.title = 'Show/hide satellite imagery';
        _satReady.style.cursor = 'pointer';
        _satReady.style.opacity = '1';
        _satReady.textContent = 'Satellite: On';
        _satReady.style.background  = 'rgba(34,211,238,.18)';
        _satReady.style.borderColor = 'rgba(34,211,238,.4)';
        _satReady.style.color       = '#67e8f9';
    }
    // Satellite is ready — enable the Wind + Pressure toggles.
    const _wbtnReady = document.getElementById('windToggleBtn');
    if (_wbtnReady) {
        _wbtnReady.disabled = false;
        _wbtnReady.title = 'Toggle wind particle overlay';
        _wbtnReady.style.cursor = 'pointer';
        _wbtnReady.style.opacity = '1';
        _wbtnReady.style.background = 'rgba(100,116,139,.18)';
        _wbtnReady.style.borderColor = 'rgba(100,116,139,.4)';
        _wbtnReady.style.color = '#94a3b8';
    }
    const _pbtnReady = document.getElementById('pressureToggleBtn');
    if (_pbtnReady) {
        _pbtnReady.disabled = false;
        _pbtnReady.title = 'Toggle MSLP field, isobars, and station obs';
        _pbtnReady.style.cursor = 'pointer';
        _pbtnReady.style.opacity = '1';
        _pbtnReady.style.background = 'rgba(100,116,139,.18)';
        _pbtnReady.style.borderColor = 'rgba(100,116,139,.4)';
        _pbtnReady.style.color = '#94a3b8';
    }
    const _ppbtnReady = document.getElementById('precipToggleBtn');
    if (_ppbtnReady) {
        _ppbtnReady.disabled = false;
        _ppbtnReady.title = 'Toggle precipitation rate + cloud cover';
        _ppbtnReady.style.cursor = 'pointer';
        _ppbtnReady.style.opacity = '1';
        _ppbtnReady.style.background = 'rgba(100,116,139,.18)';
        _ppbtnReady.style.borderColor = 'rgba(100,116,139,.4)';
        _ppbtnReady.style.color = '#94a3b8';
    }
    _enableWindfieldToggle();
    // Enable the 3D portrait once track data exists (desktop only — the
    // toggle itself re-checks width so a resize can't strand it).
    const _s3Ready = document.getElementById('storm3dToggleBtn');
    if (_s3Ready && window.innerWidth >= 900) {
        _s3Ready.disabled = false;
        _s3Ready.style.cursor = 'pointer';
        _s3Ready.style.opacity = '1';
    }
    // Enable the Infrared toggle and sync its visual state.
    const _irReady = document.getElementById('irToggleBtn');
    if (_irReady) {
        _irReady.disabled = false;
        _irReady.title = 'Switch satellite imagery between visible and infrared (24/7)';
        _irReady.style.cursor = 'pointer';
        _irReady.style.opacity = '1';
        if (st.irMode) {
            _irReady.textContent = 'Infrared: On';
            _irReady.style.background  = 'rgba(168,85,247,.18)';
            _irReady.style.borderColor = 'rgba(168,85,247,.4)';
            _irReady.style.color       = '#c4b5fd';
        } else {
            _irReady.textContent = 'Infrared: Off';
            _irReady.style.background  = 'rgba(100,116,139,.18)';
            _irReady.style.borderColor = 'rgba(100,116,139,.4)';
            _irReady.style.color       = '#94a3b8';
        }
    }
    const lbl = document.getElementById('satelliteFrameLabel');
    if (lbl) lbl.textContent = formatSatelliteFrameLabel(frames[st.currentIdx]);
```

### 13. teardownSatelliteLayer (panel era, REFERENCE ONLY)

**Do not paste verbatim.** Its windfield-teardown block killed the always-on windfield 50ms after auto-enable (fixed post-c841e96). Restore only the wind/pressure/precip and button-reset pieces, i.e. section 7c.

```js
function teardownSatelliteLayer(opts) {
    const keepOverlays = !!(opts && opts.keepOverlays);
    const st = window._satelliteState;
    if (st.layer && currentMap) {
        try { currentMap.removeLayer(st.layer); } catch(e) {}
    }
    st.layer = null;
    // Cancel any in-flight double-buffered frame swap so its 'load' callback
    // can't resurrect a layer after teardown.
    st._swapToken = (st._swapToken || 0) + 1;
    if (st._pendingLayer && currentMap) {
        try { currentMap.removeLayer(st._pendingLayer); } catch(e) {}
    }
    st._pendingLayer = null;
    // When only swapping modes (IR toggle), keep the frame index and satellite
    // name so initSatelliteLayer can refetch for the same session without
    // disturbing the timeline scrubber or the other overlays.
    if (!keepOverlays) {
        st.frames = [];
        st.satellite = null;
        st.currentIdx = 0;
        st.layerLabel = '';
        const slider = document.getElementById('satelliteSlider');
        if (slider) slider.style.display = 'none';
        const syncEl = document.getElementById('syncedSatTs');
        if (syncEl) syncEl.style.display = 'none';
        const nameEl = document.getElementById('satelliteName');
        if (nameEl) nameEl.textContent = '--';
        const labelEl = document.getElementById('satelliteProductLabel');
        if (labelEl) labelEl.textContent = '\u00a0';
    }
    // On a full storm teardown we also wipe wind/pressure/precip since they're
    // bbox-tied to the session. On an IR toggle (keepOverlays=true) we leave
    // them alone — they're independent data products.
    if (keepOverlays) return;
    // Tear down the wind layer too — its bbox is tied to the satellite session.
    const w = window._windState;
    if (w) {
        if (w.layer && currentMap) {
            try { currentMap.removeLayer(w.layer); } catch(e) {}
        }
        w.layer = null;
        w.enabled = false;
        w.bbox = null;
        w.currentTs = null;
        const wbtn = document.getElementById('windToggleBtn');
        if (wbtn) {
            wbtn.disabled = true;
            wbtn.title = 'Loading satellite frames…';
            wbtn.style.cursor = 'not-allowed';
            wbtn.style.opacity = '.6';
            wbtn.style.background = 'rgba(100,116,139,.10)';
            wbtn.style.borderColor = 'rgba(100,116,139,.25)';
            wbtn.style.color = '#94a3b8';
            wbtn.textContent = 'Wind: Off';
        }
        const wleg = document.getElementById('windLegend');
        if (wleg) wleg.style.display = 'none';
    }
    // Tear down the pressure layer too — it's also bbox-tied to the session.
    const p = window._pressureState;
    if (p) {
        ['fillLayer','isobarLayer','stationLayer'].forEach(k => {
            if (p[k] && currentMap) { try { currentMap.removeLayer(p[k]); } catch(e) {} }
            p[k] = null;
        });
        p.enabled = false;
        p.bbox = null;
        p.currentTs = null;
        const pbtn = document.getElementById('pressureToggleBtn');
        if (pbtn) {
            pbtn.disabled = true;
            pbtn.title = 'Loading satellite frames…';
            pbtn.style.cursor = 'not-allowed';
            pbtn.style.opacity = '.6';
            pbtn.style.background = 'rgba(100,116,139,.10)';
            pbtn.style.borderColor = 'rgba(100,116,139,.25)';
            pbtn.style.color = '#94a3b8';
            pbtn.textContent = 'Pressure: Off';
        }
        const pleg = document.getElementById('pressureLegend');
        if (pleg) pleg.style.display = 'none';
    }
    // Tear down the precip layer.
    const pp = window._precipState;
    if (pp) {
        ['cloudLayer','precipLayer'].forEach(k => {
            if (pp[k] && currentMap) { try { currentMap.removeLayer(pp[k]); } catch(e) {} }
            pp[k] = null;
        });
        pp.enabled = false;
        pp.bbox = null;
        pp.currentTs = null;
        pp.inflightTs = null;
        pp.lastPayload = null;
        pp.lastBounds = null;
        const ppbtn = document.getElementById('precipToggleBtn');
        if (ppbtn) {
            ppbtn.disabled = true;
            ppbtn.title = 'Loading satellite frames…';
            ppbtn.style.cursor = 'not-allowed';
            ppbtn.style.opacity = '.6';
            ppbtn.style.background = 'rgba(100,116,139,.10)';
            ppbtn.style.borderColor = 'rgba(100,116,139,.25)';
            ppbtn.style.color = '#94a3b8';
            ppbtn.textContent = 'Precip: Off';
        }
        const ppleg = document.getElementById('precipLegend');
        if (ppleg) ppleg.style.display = 'none';
    }
    // Close the 3D portrait if it's open — its scene is built from the
    // outgoing storm's field and would render stale data over the new one.
    try { if (window._storm3d && window._storm3d.on) toggleStorm3D(); } catch (e) {}
    // Tear down the windfield (Holland B image overlay) layer. Per-frame
    // canvas images are cached in wf.cache — drop it so a new storm doesn't
    // retain stale images from the previous one.
    const wf = window._windfieldState;
    if (wf) {
        if (wf.overlay && currentMap) {
            try { currentMap.removeLayer(wf.overlay); } catch(e) {}
        }
        wf.overlay = null;
        wf.cache = null;
        wf.enabled = false;
        wf.currentIdx = -1;
        const wfbtn = document.getElementById('windfieldToggleBtn');
        if (wfbtn) {
            wfbtn.disabled = true;
            wfbtn.title = 'Loading track…';
            wfbtn.style.cursor = 'not-allowed';
            wfbtn.style.opacity = '.6';
            wfbtn.style.background = 'rgba(100,116,139,.10)';
            wfbtn.style.borderColor = 'rgba(100,116,139,.25)';
            wfbtn.style.color = '#94a3b8';
            wfbtn.textContent = 'Windfield: Off';
        }
    }
    // Reset IR toggle button (but keep irMode preference — it persists across storms).
    const _irBtn = document.getElementById('irToggleBtn');
    if (_irBtn) {
        _irBtn.disabled = true;
        _irBtn.title = 'Loading satellite frames…';
        _irBtn.style.cursor = 'not-allowed';
        _irBtn.style.opacity = '.6';
        _irBtn.style.background = 'rgba(100,116,139,.10)';
        _irBtn.style.borderColor = 'rgba(100,116,139,.25)';
        _irBtn.style.color = '#94a3b8';
        _irBtn.textContent = 'Infrared: Off';
    }
    // Reset Satellite show/hide toggle.
    const _satBtn = document.getElementById('satToggleBtn');
    if (_satBtn) {
        _satBtn.disabled = true;
        _satBtn.title = 'Loading satellite frames…';
        _satBtn.style.cursor = 'not-allowed';
        _satBtn.style.opacity = '.6';
        _satBtn.style.background = 'rgba(100,116,139,.10)';
        _satBtn.style.borderColor = 'rgba(100,116,139,.25)';
        _satBtn.style.color = '#94a3b8';
        _satBtn.textContent = 'Satellite: Off';
    }
    st.visible = true;  // reset so next init shows satellite

    // Restore any overlay-row visibility that enableHistoricOverlays may have
    // hidden for the previous storm. Without this, switching historic → active
    // would leave Satellite/IR/Pressure/Precip rows invisible on the new storm
    // even though their buttons get re-enabled by the active-storm init path.
    ['satToggleBtn', 'irToggleBtn', 'pressureToggleBtn', 'precipToggleBtn'].forEach(id => {
        const b = document.getElementById(id);
        if (b && b.parentElement) b.parentElement.style.display = '';
    });
}
```

## Gotchas (learned the hard way — see git log + operator memory)

- **Open-Meteo free tier is the wind layer's hard ceiling**: shared
  limiter (~10 req/min) across wind/pressure/precip and all users. The
  prefetch is deliberately WINDOWED (12 buckets nearest the view) and
  SERIAL-PACED; bursting a full timeline mostly 429s. Don't "fix" that.
- **Active storms drive wind frames off the SATELLITE frame set** (via
  `setSatelliteFrame` → `setWindFrame`), historic storms off `stormData`
  (section 7d). Both display and prefetch must route through
  `_satelliteTsToWindTs`'s shared 3-hour bucket or they warm different sets.
- **`keepOverlays`**: `teardownSatelliteLayer({keepOverlays:true})` was the
  IR-toggle path (swap satellite product, keep wind/pressure/precip). With
  the IR toggle retired the flag is always false, but the mechanics remain
  in place.
- **Always-on layers must not be torn down by satellite re-inits** — see the
  NOTE comment in the live `teardownSatelliteLayer`, and the `!enabled`
  guards on the renderMap auto-enables (a double renderMap otherwise
  TOGGLES a layer off with no button to recover it).
- **Panel rows used `adv-desktop-only`** + a ≤900px CSS rule so phones kept
  the simplified set; `initSatelliteLayer`'s mobile early-return is what
  actually prevents GIBS fetches on phones and it is still live.
