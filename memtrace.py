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


# ── large-buffer census ──────────────────────────────────────────────────
# numpy arrays and bytes/str are not gc-tracked, so gc.get_objects() never
# lists them and mallinfo only says "N MB in use". This walks every
# gc-tracked object's referents (dicts, lists, instances, coroutine and
# thread frames) and reports each large buffer once — by its OWNING buffer,
# so views don't double-count — grouped by what holds it. The top holders are
# then traced one step further (who owns that dict / list) so the answer
# names a module global, an attribute, or a function's local variable.
def _buffer_info(obj, np):
    """(bytes, owner_id, description) for a large-buffer candidate, else None."""
    try:
        if np is not None and isinstance(obj, np.ndarray):
            a = obj
            while isinstance(a.base, np.ndarray):
                a = a.base
            if a.base is None:
                return a.nbytes, id(a), f"ndarray {a.shape} {a.dtype}"
            return (obj.nbytes, id(a.base),
                    f"ndarray view {obj.shape} {obj.dtype} on {type(a.base).__name__}")
        if isinstance(obj, (bytes, bytearray, str)):
            import sys as _sys
            return _sys.getsizeof(obj), id(obj), type(obj).__name__
        if isinstance(obj, memoryview):
            return obj.nbytes, id(obj.obj), "memoryview"
    except Exception:
        pass
    return None


def _holder_label(holder, target) -> str:
    """Name the slot inside *holder* that points at *target*."""
    import types
    try:
        if isinstance(holder, dict):
            for k, v in holder.items():
                if v is target:
                    return f"dict[{str(k)[:40]}]"
            return "dict"
        if isinstance(holder, (list, tuple, set, frozenset)):
            return f"{type(holder).__name__}(len={len(holder)})"
        if isinstance(holder, types.FrameType):
            co = holder.f_code
            return f"frame {co.co_name} ({co.co_filename.rsplit('/', 1)[-1]}:{holder.f_lineno})"
        if isinstance(holder, (types.CoroutineType, types.GeneratorType)):
            fr = getattr(holder, "cr_frame", None) or getattr(holder, "gi_frame", None)
            var = ""
            if fr is not None:
                var = next((k for k, v in fr.f_locals.items() if v is target), "")
            return (f"{type(holder).__name__} {getattr(holder, '__qualname__', '?')}"
                    + (f" local {var}" if var else ""))
        attrs = getattr(holder, "__dict__", None)
        if isinstance(attrs, dict):
            for k, v in attrs.items():
                if v is target:
                    return f"{type(holder).__qualname__}.{k}"
        return type(holder).__qualname__
    except Exception:
        return type(holder).__name__


def _owner_of(container, skip_ids, _depth=0) -> str:
    """Who holds this dict/list: a module global, class/instance attribute,
    frame, or (one more level) the dict key it sits under."""
    import gc
    import types
    try:
        refs = [r for r in gc.get_referrers(container)
                if id(r) not in skip_ids and not isinstance(r, tuple)]
        for r in refs:
            if isinstance(r, types.ModuleType):
                return f"module {r.__name__}"
            if isinstance(r, type):
                return f"class {r.__qualname__}"
            if isinstance(r, types.FrameType):
                return f"frame {r.f_code.co_name}"
            if isinstance(r, (types.CoroutineType, types.GeneratorType)):
                return f"{type(r).__name__} {getattr(r, '__qualname__', '?')}"
            if getattr(r, "__dict__", None) is container:
                return f"instance of {type(r).__qualname__}"
        for r in refs:
            if isinstance(r, dict):
                for k, v in r.items():
                    if v is container:
                        up = _owner_of(r, skip_ids | {id(refs)}, _depth + 1) if _depth < 1 else ""
                        return f"{up} [{str(k)[:40]}]".strip()
        if refs:
            return type(refs[0]).__qualname__
    except Exception:
        pass
    return "?"


_LAST_CENSUS: dict = {"t": 0.0, "result": None}
CENSUS_MIN_INTERVAL_S = 60.0


def buffer_census(min_kb: int = 256, top: int = 15, *, force: bool = False) -> dict:
    """Large buffers reachable from gc-tracked objects and thread stacks,
    grouped by holder. Seconds of CPU on a big heap, and the endpoint is
    public — so at most one real census per minute; repeats get the last one."""
    now = time.time()
    if (not force and _LAST_CENSUS["result"] is not None
            and now - _LAST_CENSUS["t"] < CENSUS_MIN_INTERVAL_S):
        return dict(_LAST_CENSUS["result"], cached=True)
    result = _buffer_census(min_kb, top)
    _LAST_CENSUS.update(t=now, result=result)
    return result


def _buffer_census(min_kb: int, top: int) -> dict:
    import gc
    import sys as _sys
    t0 = time.time()
    try:
        import numpy as np
    except Exception:
        np = None
    min_b = min_kb * 1024
    seen, groups, largest, candidates = set(), {}, [], []
    objs = gc.get_objects()
    try:
        for holder in objs:
            try:
                refs = gc.get_referents(holder)
            except Exception:
                continue
            for r in refs:
                info = _buffer_info(r, np)
                if info and info[0] >= min_b and info[1] not in seen:
                    seen.add(info[1])
                    candidates.append((holder, r, info))
        # Running threads' stacks are not always materialised as gc objects.
        for frame in _sys._current_frames().values():
            f = frame
            while f is not None:
                try:
                    for v in list(f.f_locals.values()):
                        info = _buffer_info(v, np)
                        if info and info[0] >= min_b and info[1] not in seen:
                            seen.add(info[1])
                            candidates.append((f, v, info))
                except Exception:
                    pass
                f = f.f_back

        total = 0
        for holder, r, (nbytes, _oid, what) in candidates:
            total += nbytes
            label = _holder_label(holder, r)
            g = groups.setdefault((id(holder), label),
                                  {"holder": label, "mb": 0.0, "count": 0, "_h": holder})
            g["mb"] += nbytes / 1048576
            g["count"] += 1
            largest.append((nbytes, what, label))
        ranked = sorted(groups.values(), key=lambda g: -g["mb"])[:top]
        skip = {id(objs), id(candidates), id(groups), id(ranked)} | {id(g) for g in ranked}
        by_holder = []
        for g in ranked:
            h = g.pop("_h")
            owner = _owner_of(h, skip) if isinstance(h, (dict, list, tuple, set)) else ""
            by_holder.append({"holder": g["holder"], "owner": owner,
                              "mb": round(g["mb"], 1), "count": g["count"]})
        largest.sort(key=lambda x: -x[0])
        return {
            "total_mb": round(total / 1048576, 1),
            "buffers": len(candidates),
            "by_holder": by_holder,
            "largest": [{"mb": round(b / 1048576, 1), "what": w, "holder": h}
                        for b, w, h in largest[:10]],
            "secs": round(time.time() - t0, 1),
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}
    finally:
        del objs, candidates
        groups.clear()
