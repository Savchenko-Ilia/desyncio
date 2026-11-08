"""Tests for the trace hook and deterministic task names (LOOP-09, D015)."""

import asyncio
import contextlib
import os
import selectors
import subprocess
import sys
import time
from typing import Any

import pytest

import desyncio
from desyncio import sim
from desyncio.loop import VirtualTimeLoop
from desyncio.realtime import RealTimePacer
from desyncio.trace import ListSink, TraceRecord

R = TraceRecord


def run_traced(main_factory: Any, **kw: Any) -> list[TraceRecord]:
    sink = ListSink()
    desyncio.run(main_factory(), trace=sink, **kw)
    return sink.records


# --- exact traces ---------------------------------------------------------


def test_simple_model_exact_trace() -> None:
    async def worker() -> None:
        await asyncio.sleep(1)
        await asyncio.sleep(2)

    async def main() -> None:
        await asyncio.create_task(worker())

    assert run_traced(main) == [
        R(0.0, 0, "main", "start"),
        R(0.0, 1, "proc-1", "start"),
        R(1.0, 2, "proc-1", "resume"),
        R(3.0, 3, "proc-1", "resume"),
        R(3.0, 4, "proc-1", "finish"),
        R(3.0, 5, "main", "resume"),
        R(3.0, 6, "main", "finish"),
    ]


def test_cancel_recorded_at_cancel_time() -> None:
    async def worker() -> None:
        await asyncio.sleep(10)

    async def main() -> None:
        t = asyncio.create_task(worker())
        await asyncio.sleep(2)
        t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t

    assert run_traced(main) == [
        R(0.0, 0, "main", "start"),
        R(0.0, 1, "proc-1", "start"),
        R(2.0, 2, "main", "resume"),
        R(2.0, 3, "proc-1", "resume"),  # the throw(CancelledError) step
        R(2.0, 4, "proc-1", "cancel"),
        R(2.0, 5, "main", "resume"),
        R(2.0, 6, "main", "finish"),
    ]


def test_exception_is_finish_not_cancel() -> None:
    async def boom() -> None:
        await asyncio.sleep(1)
        raise ValueError("x")

    async def main() -> None:
        with pytest.raises(ValueError):
            await asyncio.create_task(boom())

    kinds = [(r.task_name, r.kind) for r in run_traced(main)]
    assert ("proc-1", "finish") in kinds
    assert ("proc-1", "cancel") not in kinds


def test_taskgroup_children() -> None:
    async def child(d: float) -> None:
        await asyncio.sleep(d)

    async def main() -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(child(2))
            tg.create_task(child(1))

    recs = run_traced(main)
    assert [(r.t, r.task_name, r.kind) for r in recs] == [
        (0.0, "main", "start"),
        (0.0, "proc-1", "start"),
        (0.0, "proc-2", "start"),
        (1.0, "proc-2", "resume"),
        (1.0, "proc-2", "finish"),
        (2.0, "proc-1", "resume"),
        (2.0, "proc-1", "finish"),
        (2.0, "main", "resume"),
        (2.0, "main", "finish"),
    ]
    assert [r.seq for r in recs] == list(range(len(recs)))


# --- off by default, zero cost -------------------------------------------


def test_off_by_default_runs_unwrapped_coroutine() -> None:
    seen: list[bool] = []

    async def worker() -> None:
        await asyncio.sleep(0)

    async def main() -> None:
        w = worker()
        t = asyncio.create_task(w)
        seen.append(t.get_coro() is w)
        cur = asyncio.current_task()
        assert cur is not None
        seen.append(cur.get_coro() is main_coro)
        await t

    main_coro = main()
    desyncio.run(main_coro)
    assert seen == [True, True]


def test_tracing_on_wraps_and_is_still_a_coroutine() -> None:
    seen: list[Any] = []

    async def worker() -> None:
        pass

    async def main() -> None:
        w = worker()
        t = asyncio.create_task(w)
        seen.append((t.get_coro() is w, asyncio.iscoroutine(t.get_coro())))
        await t

    run_traced(main)
    assert seen == [(False, True)]


def test_no_trace_code_runs_when_off(monkeypatch: pytest.MonkeyPatch) -> None:
    import desyncio.trace as tr

    def explode(*a: Any, **k: Any) -> None:
        raise AssertionError("tracing code ran while tracing was off")

    monkeypatch.setattr(tr._TracedCoroutine, "send", explode)
    monkeypatch.setattr(tr._TracedCoroutine, "throw", explode)

    async def main() -> None:
        await asyncio.gather(asyncio.sleep(1), asyncio.sleep(2))

    desyncio.run(main())


def test_trace_must_be_callable() -> None:
    with pytest.raises(TypeError):
        VirtualTimeLoop(trace=42)  # type: ignore[arg-type]

    async def m() -> None: ...

    with pytest.raises(TypeError):
        desyncio.run(m(), trace="no")  # type: ignore[call-overload]


def test_benchmark_loose_sanity() -> None:
    """Tracing off must not be slower than a plain loop run (loose bound).

    Both sides run the same sleeps on a VirtualTimeLoop; the baseline is the
    loop driven directly by ``asyncio.Runner``, so the only difference is the
    code in ``desyncio.run``. The bound is deliberately generous to avoid
    flakes. Real numbers are in the implementation log.
    """

    async def work() -> None:
        for _ in range(20_000):
            await asyncio.sleep(0.001)

    start = time.perf_counter()
    desyncio.run(work(), trace=None)
    off = time.perf_counter() - start
    with asyncio.Runner(loop_factory=VirtualTimeLoop) as runner:
        start = time.perf_counter()
        runner.run(work())
        plain = time.perf_counter() - start
    assert off < plain * 3 + 0.5


# --- naming ---------------------------------------------------------------


def _names_model() -> list[str]:
    names: list[str] = []

    async def child() -> None:
        cur = asyncio.current_task()
        assert cur is not None
        names.append(cur.get_name())
        await sim.timeout(1)

    async def main() -> None:
        cur = asyncio.current_task()
        assert cur is not None
        names.append(cur.get_name())
        t1 = asyncio.create_task(child())
        async with asyncio.TaskGroup() as tg:
            tg.create_task(child())
            sim.spawn(child())
            tg.create_task(child(), name="explicit")
        await t1
        await asyncio.gather(child())

    desyncio.run(main())
    return names


def test_names_identical_across_runs_and_sources() -> None:
    first = _names_model()
    asyncio.run(asyncio.sleep(0))  # burns asyncio's process-global Task-N
    second = _names_model()
    assert first == second
    assert first[0] == "main"
    assert sorted(first[1:]) == sorted(
        ["proc-1", "proc-2", "proc-3", "explicit", "proc-4"]
    )


def test_naming_applies_with_tracing_on() -> None:
    sink = ListSink()
    names: list[str] = []

    async def main() -> None:
        cur = asyncio.current_task()
        assert cur is not None
        names.append(cur.get_name())
        t = asyncio.create_task(asyncio.sleep(0))
        names.append(t.get_name())
        await t

    desyncio.run(main(), trace=sink)
    assert names == ["main", "proc-1"]
    assert {r.task_name for r in sink.records} == {"main", "proc-1"}


# --- determinism ----------------------------------------------------------

_MODEL = """
import asyncio, sys
import desyncio
from desyncio import sim
from desyncio.trace import ListSink

async def worker(i):
    rng = sim.rng(f"w{i}")
    for _ in range(3):
        await sim.timeout(rng.random())

async def main():
    async with asyncio.TaskGroup() as tg:
        for i in range(4):
            tg.create_task(worker(i))
        sim.spawn(worker(9))

def go(seed):
    sink = ListSink()
    desyncio.run(main(), seed=seed, trace=sink)
    return sink.records
"""


def test_trace_identical_across_runs_in_process() -> None:
    ns: dict[str, Any] = {}
    exec(_MODEL, ns)  # noqa: S102
    a, b = ns["go"](7), ns["go"](7)
    assert a == b
    assert len(a) > 20
    assert ns["go"](8) != a


def test_trace_identical_in_subprocess_with_other_hashseed() -> None:
    code = _MODEL + "\nprint(repr(go(7)))"
    outs = []
    for hs in ("1", "12345"):
        env = {**os.environ, "PYTHONHASHSEED": hs}
        p = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        outs.append(p.stdout)
    assert outs[0] == outs[1]
    ns: dict[str, Any] = {}
    exec(_MODEL, ns)  # noqa: S102
    assert outs[0].strip() == repr(ns["go"](7))


# --- sink errors ----------------------------------------------------------


def test_sink_exception_propagates_from_run() -> None:
    class SinkBoom(Exception):
        pass

    def sink(rec: TraceRecord) -> None:
        if rec.kind == "resume":
            raise SinkBoom(rec)

    async def main() -> None:
        await asyncio.sleep(1)

    with pytest.raises(SinkBoom):
        desyncio.run(main(), trace=sink)


# --- with until= and speed= ----------------------------------------------


class FakeWall:
    """Fake wall clock: ``wait`` advances fake time instantly (no sleeping)."""

    def __init__(self) -> None:
        self.t = 0.0

    def monotonic(self) -> float:
        return self.t

    def wait(
        self, timeout: float | None, selector: selectors.BaseSelector
    ) -> list[tuple[selectors.SelectorKey, int]]:
        assert timeout is not None
        self.t += timeout
        return []


def test_trace_with_until() -> None:
    async def main() -> None:
        for _ in range(100):
            await asyncio.sleep(1)

    recs = run_traced(main, until=2.5)
    assert [(r.t, r.kind) for r in recs] == [
        (0.0, "start"),
        (1.0, "resume"),
        (2.0, "resume"),
        (2.5, "resume"),  # cleanup cancel throws into the task at frozen clock
        (2.5, "cancel"),
    ]


def test_trace_with_pacer_and_fake_wall() -> None:
    wall = FakeWall()

    async def main() -> None:
        await asyncio.sleep(4)

    sink = ListSink()
    pacer = RealTimePacer(2, wall_clock=wall)  # type: ignore[arg-type]
    desyncio.run(main(), trace=sink, pacer=pacer)
    assert sink.records == [
        R(0.0, 0, "main", "start"),
        R(4.0, 1, "main", "resume"),
        R(4.0, 2, "main", "finish"),
    ]
    assert wall.t == 2.0


def test_trace_with_speed_and_until() -> None:
    async def main() -> None:
        await asyncio.sleep(0.0)

    sink = ListSink()
    desyncio.run(main(), trace=sink, speed=1e9, until=1.0)
    assert [r.kind for r in sink.records] == ["start", "resume", "finish"]


def test_traced_task_repr_and_stack_work() -> None:
    out: list[str] = []

    async def main() -> None:
        cur = asyncio.current_task()
        assert cur is not None
        out.append(repr(cur))
        cur.get_stack()

    run_traced(main)
    assert "main" in out[0]
