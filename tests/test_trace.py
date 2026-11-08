"""Tests for the trace hook and deterministic task names (LOOP-09, D015)."""

import asyncio
import contextlib
import os
import selectors
import subprocess
import sys
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


# Tracing off must cost nothing per step. The old timing test here compared
# two VirtualTimeLoop runs, so it could not see a slowdown of the loop itself;
# it was removed. The deterministic guard is
# tests/test_loop.py::test_loop_instance_dict_is_not_larger_than_before_loop_09
# and the measured numbers are in docs/how-the-loop-works.md section 10.


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


class SinkBoom(Exception):
    pass


class RaisingSink:
    """Raises ``SinkBoom`` (one object, kept in ``error``) on one record."""

    def __init__(self, task_name: str, kind: str) -> None:
        self.target = (task_name, kind)
        self.error = SinkBoom(task_name, kind)
        self.records: list[TraceRecord] = []

    def __call__(self, rec: TraceRecord) -> None:
        self.records.append(rec)
        if (rec.task_name, rec.kind) == self.target:
            raise self.error


def _run_sink_case(
    kind: str,
) -> tuple[RaisingSink, "asyncio.Task[Any]", list[str], VirtualTimeLoop]:
    """Run a model whose sink raises on ``kind`` of the task named ``child``.

    ``child`` logs what its own code sees; the model never awaits it, so its
    outcome is only what the model itself gives it (nothing a sink did).
    """
    sink = RaisingSink("child", kind)
    seen: list[str] = []
    box: dict[str, Any] = {}

    async def child() -> int:
        seen.append("ran")
        try:
            if kind == "finish":
                return 42
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            seen.append("cancelled")
            raise
        seen.append("woke")
        return 0

    async def main() -> str:
        box["loop"] = asyncio.get_running_loop()
        task = asyncio.create_task(child(), name="child")
        box["task"] = task
        await asyncio.sleep(0)  # child starts (and, for "finish", ends)
        if kind == "cancel":
            task.cancel()
            await asyncio.sleep(0)  # child handles its CancelledError
        seen.append("main went on")
        await asyncio.sleep(1)
        seen.append("main done")
        return "main result"

    with pytest.raises(SinkBoom) as excinfo:
        desyncio.run(main(), trace=sink)
    assert excinfo.value is sink.error
    return sink, box["task"], seen, box["loop"]


def test_sink_error_on_child_start_does_not_touch_the_child() -> None:
    sink, task, seen, loop = _run_sink_case("start")
    # The child's first step ran normally. So did main's step, which was in
    # the same loop iteration (both were already in _ready). The loop then
    # stopped: main never reached t=1. run()'s cleanup cancelled the child,
    # as it does any leftover task. The child never saw SinkBoom.
    assert seen == ["ran", "main went on", "cancelled"]
    assert task.cancelled()
    assert loop.is_closed()
    # Tracing stopped at the failing record: no later sink calls.
    assert sink.records[-1][2:] == ("child", "start")


def test_sink_error_on_child_finish_keeps_its_result() -> None:
    sink, task, seen, loop = _run_sink_case("finish")
    assert seen == ["ran", "main went on"]  # same iteration; no "main done"
    assert task.result() == 42
    assert loop.is_closed()
    assert sink.records[-1][2:] == ("child", "finish")


def test_sink_error_on_child_cancel_keeps_it_cancelled() -> None:
    sink, task, seen, loop = _run_sink_case("cancel")
    # Main's step in the iteration of the failing cancel record still ran;
    # nothing after that iteration did (no "main done").
    assert seen == ["ran", "cancelled", "main went on"]
    assert task.cancelled()  # cancelled, not failed with SinkBoom
    assert loop.is_closed()
    assert sink.records[-1][2:] == ("child", "cancel")


@pytest.mark.parametrize("kind", ["start", "resume", "finish"])
def test_sink_error_for_main_task_is_raised_by_run(kind: str) -> None:
    sink = RaisingSink("main", kind)
    seen: list[str] = []

    async def main() -> int:
        seen.append("start")
        await asyncio.sleep(1)
        seen.append("resumed")
        return 7

    with pytest.raises(SinkBoom) as excinfo:
        desyncio.run(main(), trace=sink)
    assert excinfo.value is sink.error
    assert sink.records[-1][2:] == ("main", kind)
    # main's own code was not interrupted by the sink in its step.
    assert seen == (["start"] if kind == "start" else ["start", "resumed"])


def test_sink_error_does_not_change_taskgroup_semantics() -> None:
    sink = RaisingSink("worker", "finish")
    results: list[object] = []

    async def worker() -> int:
        await asyncio.sleep(1)
        return 5

    async def main() -> None:
        async with asyncio.TaskGroup() as tg:
            t = tg.create_task(worker(), name="worker")
        results.append(t.result())

    with pytest.raises(SinkBoom) as excinfo:
        desyncio.run(main(), trace=sink)
    assert excinfo.value is sink.error
    # Not an ExceptionGroup: the TaskGroup never saw the sink's error.
    assert type(excinfo.value) is SinkBoom


def test_sink_error_during_cleanup_is_raised_after_close() -> None:
    """A sink failing on a leftover task's cleanup ``cancel`` record."""
    sink = RaisingSink("left", "cancel")
    box: dict[str, Any] = {}

    async def left() -> None:
        await asyncio.sleep(100)

    async def main() -> str:
        box["loop"] = asyncio.get_running_loop()
        box["task"] = asyncio.create_task(left(), name="left")
        await asyncio.sleep(1)
        return "done"

    with pytest.raises(SinkBoom) as excinfo:
        desyncio.run(main(), trace=sink)
    assert excinfo.value is sink.error
    assert box["task"].cancelled()
    assert box["loop"].is_closed()


def test_sink_errors_leave_no_warnings_under_w_error() -> None:
    """Run the sink-error cases in a fresh interpreter with ``-W error``.

    An unclosed loop, a task destroyed while pending or a never-awaited
    coroutine would print to stderr (or fail) there.
    """
    code = """
import asyncio, gc, sys
sys.path.insert(0, sys.argv[1])
import test_trace as t
for kind in ("start", "finish", "cancel"):
    t._run_sink_case(kind)
for kind in ("start", "resume", "finish"):
    t.test_sink_error_for_main_task_is_raised_by_run(kind)
t.test_sink_error_does_not_change_taskgroup_semantics()
t.test_sink_error_during_cleanup_is_raised_after_close()
gc.collect()
print("ok")
"""
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    out = subprocess.run(
        [sys.executable, "-W", "error", "-X", "dev", "-c", code, tests_dir],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "ok"
    assert out.stderr == ""


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


# --- docs claims (D011) ---------------------------------------------------


def _loop_doc_section(number: int) -> str:
    from pathlib import Path

    doc = Path(__file__).resolve().parent.parent / "docs" / "how-the-loop-works.md"
    text = doc.read_text(encoding="utf-8")
    start = text.index(f"\n## {number}. ")
    end = text.index("\n## ", start + 1)
    return text[start:end]


def test_docs_zero_cost_claim_is_the_measured_one() -> None:
    """Section 10 must describe the real cost of tracing off (D011)."""
    section = _loop_doc_section(10)
    # The old claim was false: create_task is a Python-level override that
    # also names the task, and the extra loop attributes slowed every step.
    assert "one\n`is None` check" not in section
    assert "is None` check in `create_task`" not in section
    assert "per task" in section
    assert "per step" in section
    assert "instance dict" in section
    assert "µs" in section


def test_docs_private_attribute_table_lists_create_task() -> None:
    section = _loop_doc_section(8)
    row = next(line for line in section.splitlines() if "`loop._check_closed`" in line)
    assert "create_task" in row
