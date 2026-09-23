"""Resident-memory attribution for /health/memory (diagnostics only).

The healthcheck alarm pages on resident memory, and sampling from outside
(2026-09-23) showed two shapes nobody could name without Railway's logs: a
40-minute climb of ~1.2 GB that released all at once, and a 4.5 GB peak at
the hourly refresh. This module records WHICH work was running when memory
moved, readable from the public health endpoint:

  * spans   — every background job (and big-enough request) with its
              start/end resident MB, the process peak after it, and duration
  * samples — resident MB once a minute, with the labels of whatever spans
              and requests were in flight at that moment

Everything here is best-effort: a failure to read /proc (local Windows dev)
or to record an event is swallowed — instrumentation must never be able to
break the work it observes.
"""
from __future__ import annotations

import asyncio
import collections
import contextlib
import functools
import time
from typing import Optional

_SPANS: collections.deque = collections.deque(maxlen=120)
_SAMPLES: collections.deque = collections.deque(maxlen=240)   # 4 h at 1/min
_ACTIVE: collections.Counter = collections.Counter()          # label -> running count
_INFLIGHT: collections.Counter = collections.Counter()        # request path -> count

# A request is recorded as a span only when it moves memory this much or runs
# this long — ordinary page traffic would otherwise flush the ring.
REQ_DELTA_MB = 25.0
REQ_SECONDS = 5.0


def _status_mb(key: str) -> Optional[float]:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith(key):
                    return round(int(line.split()[1]) / 1024, 1)
    except (OSError, ValueError, IndexError):
        pass
    return None


def rss_mb() -> Optional[float]:
    return _status_mb("VmRSS:")


def hwm_mb() -> Optional[float]:
    return _status_mb("VmHWM:")


def _record(label: str, t0: float, rss0, hwm0, kind: str) -> None:
    try:
        rss1, hwm1 = rss_mb(), hwm_mb()
        _SPANS.append({
            "label": label, "kind": kind,
            "start": time.strftime("%H:%M:%S", time.gmtime(t0)),
            "secs": round(time.time() - t0, 1),
            "rss_start": rss0, "rss_end": rss1,
            "delta_mb": round(rss1 - rss0, 1) if rss0 is not None and rss1 is not None else None,
            "hwm_rose_mb": round(hwm1 - hwm0, 1) if hwm0 is not None and hwm1 is not None else None,
        })
    except Exception:
        pass


@contextlib.contextmanager
def span(label: str):
    """Mark a unit of background work. Works inside async functions too
    (it only brackets the code; it never awaits)."""
    t0, rss0, hwm0 = time.time(), rss_mb(), hwm_mb()
    _ACTIVE[label] += 1
    try:
        yield
    finally:
        _ACTIVE[label] -= 1
        if _ACTIVE[label] <= 0:
            del _ACTIVE[label]
        _record(label, t0, rss0, hwm0, "job")


def traced(label):
    """Decorator form of span(). *label* is a string, or a callable given the
    wrapped function's arguments (e.g. to name the storm being computed)."""
    def _name(args, kwargs):
        try:
            return label(*args, **kwargs) if callable(label) else label
        except Exception:
            return "traced"

    def deco(fn):
        if asyncio.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def aw(*args, **kwargs):
                with span(_name(args, kwargs)):
                    return await fn(*args, **kwargs)
            return aw

        @functools.wraps(fn)
        def w(*args, **kwargs):
            with span(_name(args, kwargs)):
                return fn(*args, **kwargs)
        return w
    return deco


def request_started(path: str):
    _INFLIGHT[path] += 1
    return time.time(), rss_mb(), hwm_mb()


def request_finished(path: str, token) -> None:
    try:
        _INFLIGHT[path] -= 1
        if _INFLIGHT[path] <= 0:
            del _INFLIGHT[path]
        t0, rss0, hwm0 = token
        rss1 = rss_mb()
        moved = (rss1 - rss0) if rss0 is not None and rss1 is not None else 0.0
        if moved >= REQ_DELTA_MB or (time.time() - t0) >= REQ_SECONDS:
            _record(path, t0, rss0, hwm0, "request")
    except Exception:
        pass


async def sampler(interval_s: int = 60) -> None:
    """Background task: resident MB + whatever is running, once a minute."""
    while True:
        try:
            _SAMPLES.append({
                "t": time.strftime("%H:%M", time.gmtime()),
                "rss": rss_mb(), "hwm": hwm_mb(),
                "jobs": sorted(_ACTIVE),
                "requests": sorted(_INFLIGHT)[:8],
            })
        except Exception:
            pass
        await asyncio.sleep(interval_s)


def snapshot() -> dict:
    return {"spans": list(_SPANS), "samples": list(_SAMPLES),
            "running_now": sorted(_ACTIVE)}


# ── glibc allocator view ─────────────────────────────────────────────────
# gc-tracked Python objects are only part of resident memory: numpy / C
# buffers are invisible to gc.get_objects(), and memory Python has FREED can
# stay resident if the allocator never hands it back to the OS. mallinfo2
# separates the two: in_use = live malloc'd bytes (heap + mmap'd blocks),
# free_retained = freed bytes still held in the heap. (glibc >= 2.33; the
# python:3.12-slim image is Debian bookworm / glibc 2.36. None elsewhere.)
class _MallInfo2(__import__("ctypes").Structure):
    _fields_ = [(n, __import__("ctypes").c_size_t) for n in (
        "arena", "ordblks", "smblks", "hblks", "hblkhd", "usmblks",
        "fsmblks", "uordblks", "fordblks", "keepcost")]


def _libc():
    import ctypes
    import ctypes.util
    return ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")


def malloc_stats() -> Optional[dict]:
    try:
        libc = _libc()
        libc.mallinfo2.restype = _MallInfo2
        mi = libc.mallinfo2()
        mb = lambda b: round(b / 1048576, 1)  # noqa: E731
        return {
            "heap_total_mb": mb(mi.arena),           # brk heap obtained from the OS
            "mmap_blocks_mb": mb(mi.hblkhd),         # large blocks, returned on free
            "in_use_mb": mb(mi.uordblks + mi.hblkhd),
            "free_retained_mb": mb(mi.fordblks),     # freed but still resident-eligible
            "trimmable_top_mb": mb(mi.keepcost),
        }
    except Exception:
        return None


def trim() -> Optional[dict]:
    """malloc_trim(0): hand freed heap pages back to the OS. Returns resident
    MB before/after — a large drop proves the RSS was allocator retention,
    not live data."""
    try:
        before = rss_mb()
        t0 = time.time()
        _libc().malloc_trim(0)
        return {"rss_before": before, "rss_after": rss_mb(), "ms": round((time.time() - t0) * 1000)}
    except Exception:
        return None
