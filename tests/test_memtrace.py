"""memtrace bookkeeping: spans balance, traced names, request thresholds.

(/proc is absent on Windows/macOS dev boxes, so resident numbers may be None
here — the bookkeeping must still work and never raise.)
"""
import asyncio

import memtrace


def test_span_balances_active_and_records():
    with memtrace.span("unit:a"):
        assert "unit:a" in memtrace.snapshot()["running_now"]
    assert "unit:a" not in memtrace.snapshot()["running_now"]
    assert memtrace.snapshot()["spans"][-1]["label"] == "unit:a"


def test_span_records_even_when_body_raises():
    try:
        with memtrace.span("unit:boom"):
            raise ValueError("x")
    except ValueError:
        pass
    assert "unit:boom" not in memtrace.snapshot()["running_now"]
    assert memtrace.snapshot()["spans"][-1]["label"] == "unit:boom"


def test_traced_names_async_and_sync():
    @memtrace.traced(lambda sid, **_: f"dps:{sid}")
    async def warm(sid, force=False):
        return sid

    @memtrace.traced("republish")
    def republish():
        return 7

    assert asyncio.run(warm("AL122005", force=True)) == "AL122005"
    assert memtrace.snapshot()["spans"][-1]["label"] == "dps:AL122005"
    assert republish() == 7
    assert memtrace.snapshot()["spans"][-1]["label"] == "republish"


def test_bad_label_callable_falls_back():
    @memtrace.traced(lambda: "never")   # wrong arity for the call below
    def f(x):
        return x
    assert f(3) == 3
    assert memtrace.snapshot()["spans"][-1]["label"] == "traced"


def test_quick_request_not_recorded_but_slow_one_is(monkeypatch):
    n = len(memtrace.snapshot()["spans"])
    tok = memtrace.request_started("/fast")
    memtrace.request_finished("/fast", tok)
    assert len(memtrace.snapshot()["spans"]) == n
    tok = memtrace.request_started("/slow")
    tok = (tok[0] - 10, tok[1], tok[2])      # pretend it started 10 s ago
    memtrace.request_finished("/slow", tok)
    assert memtrace.snapshot()["spans"][-1]["label"] == "/slow"
    assert "/fast" not in memtrace._INFLIGHT and "/slow" not in memtrace._INFLIGHT


def test_allocator_probes_never_raise():
    # None off-glibc (Windows/macOS dev); a dict of MB figures on Linux.
    for v in (memtrace.malloc_stats(), memtrace.trim()):
        assert v is None or isinstance(v, dict)


_PLANTED = None


def test_buffer_census_names_holders():
    import numpy as np
    global _PLANTED
    _PLANTED = np.ones((1100, 1000))          # ~8.4 MB module global

    class Box:
        def __init__(self):
            self.grid = np.zeros((900, 900))  # ~6.2 MB attribute

    box = Box()  # noqa: F841 — must stay alive during the census
    out = memtrace.buffer_census(min_kb=4096, force=True)
    holders = {h["holder"]: h for h in out["by_holder"]}
    assert "dict[_PLANTED]" in holders
    assert holders["dict[_PLANTED]"]["owner"].endswith("test_memtrace")
    assert any(k.endswith("Box.grid") for k in holders)
    assert out["total_mb"] >= 14
    # a repeat within the rate-limit window returns the cached result
    assert memtrace.buffer_census(min_kb=4096).get("cached") is True
    _PLANTED = None
