// StormDPS Service Worker — Offline-first caching strategy.
// Bump CACHE_NAME on release so any stale satellite tiles cached under the
// previous version (before we excluded tiles from the SW cache) get evicted
// on the next activate.
const CACHE_NAME = 'stormdps-v2';
const STATIC_ASSETS = [
  '/',
  '/frontend/index.html',
  '/frontend/logo-32.png',
  '/frontend/logo-180.png',
  '/frontend/favicon.ico',
  'https://cdn.jsdelivr.net/npm/chart.js',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js',
  'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap'
];

// Install: cache static assets
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      console.log('[SW] Caching static assets');
      return cache.addAll(STATIC_ASSETS).catch(err => {
        // Non-fatal: some CDN assets may fail in dev
        console.warn('[SW] Some assets failed to cache:', err.message);
      });
    })
  );
  self.skipWaiting();
});

// Activate: clean old caches
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// Fetch: network-first for API calls, cache-first for static assets.
//
// IMPORTANT: satellite *tile* responses (/api/v1/satellite/tile/...) are
// deliberately NOT cached by the Service Worker. Tiles are addressed by
// (satellite, mode, ts, z/x/y) and the mode query lives in the URL's search
// string — but the upstream cache key on the server already handles this.
// SW-level caching would let a stale VIS tile bleed through when the user
// toggles to IR (or vice versa), which is exactly the "IR bleeding through
// the satellite map" symptom we saw. The server already sets a 1-hour
// Cache-Control on tiles, so the browser HTTP cache handles what's needed.
function _isSatelliteTile(url) {
  return url.pathname.includes('/satellite/tile/');
}

self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  // API calls: network-first with cache fallback (stale data > no data during hurricanes).
  if (url.pathname.startsWith('/api/')) {
    // Skip SW cache entirely for satellite tiles — rely on the browser's
    // HTTP cache + server Cache-Control. See note above.
    if (_isSatelliteTile(url)) {
      event.respondWith(fetch(event.request));
      return;
    }
    event.respondWith(
      fetch(event.request)
        .then(response => {
          // Cache successful JSON API responses for offline use.
          if (response.ok) {
            const clone = response.clone();
            caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
          }
          return response;
        })
        .catch(() => caches.match(event.request))
    );
    return;
  }

  // Static assets: cache-first
  event.respondWith(
    caches.match(event.request).then(cached => {
      if (cached) return cached;
      return fetch(event.request).then(response => {
        // Cache new static resources
        if (response.ok && (url.origin === self.location.origin || url.hostname.includes('cdn') || url.hostname.includes('unpkg'))) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
        }
        return response;
      });
    })
  );
});
