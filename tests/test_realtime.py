"""Tests for scaled real-time mode (LOOP-11).

Almost everything here uses :class:`FakeWall`, a fake wall clock whose
``wait`` advances fake time instantly, so the suite never sleeps. Three tests
use the real default wall clock with tiny durations (a smoke test and the two
cross-thread tests); they synchronise with ``threading.Event``, not sleeps.
"""

import asyncio
import gc
import math
import selectors
import sys
import threading
import time
import warnings
from collections.abc import Callable
from typing import Any

import pytest

import desyncio
import desyncio.guards
from desyncio.loop import SimulationDeadlock, VirtualTimeLoop
from desyncio.realtime import MAX_REAL_WAIT, RealTimePacer, SystemWallClock

Events = list[tuple[selectors.SelectorKey, int]]
Trace = list[tuple[float, float]]


class FakeWall:
    """A wall clock that never sleeps.

    ``wait(timeout)`` adds ``timeout`` to the fake time and returns at once.
    Like the real one it first polls the selector, so a wake-up already in the
    self-pipe is returned instead of "waiting".

    ``on_paused_wait`` stands in for the human: when the loop waits with no
    timeout (paused), it is called and is expected to resume the pacer.
    ``interrupt`` lets a test cut one timed wait short: it is called with the
    timeout and returns ``(fraction, action)``; the fake then advances only
    ``fraction`` of the wait, runs ``action`` (which wakes the loop) and
    returns the resulting events.
    """

    def __init__(self) -> None:
        self.t = 0.0
        self.waits: list[float | None] = []
        self.on_paused_wait: Callable[[FakeWall], None] | None = None
        self.interrupt: Callable[[float], tuple[float, Callable[[], None]] | None] = (
            lambda timeout: None
        )

    def monotonic(self) -> float:
        return self.t

    def wait(self, timeout: float | None, selector: selectors.BaseSelector) -> Events:
        self.waits.append(timeout)
        events = selector.select(0)
        if events:
            return events
        if timeout is None:
            assert self.on_paused_wait is not None, "would block forever"
            self.on_paused_wait(self)
            return []
        cut = self.interrupt(timeout)
        if cut is not None:
            fraction, action = cut
            self.t += timeout * fraction
            action()
            return selector.select(0)
        self.t += timeout
        return []


async def sleeps(durations: list[float], trace: Trace, wall: Any) -> None:
    loop = asyncio.get_running_loop()
    for d in durations:
        await asyncio.sleep(d)
        trace.append((loop.time(), wall.t))


def run_fake(durations: list[float], speed: float) -> tuple[FakeWall, Trace]:
    wall = FakeWall()
    trace: Trace = []
    desyncio.run(
        sleeps(durations, trace, wall), pacer=RealTimePacer(speed, wall_clock=wall)
    )
    return wall, trace


@pytest.mark.parametrize("speed", [1, 2, 0.5, 4.0])
def test_real_dt_is_virtual_dt_over_speed(speed: float) -> None:
    wall, trace = run_fake([1, 2, 3], speed)
    assert [v for v, _ in trace] == [1, 3, 6]
    assert [w for _, w in trace] == [1 / speed, 3 / speed, 6 / speed]
    assert wall.t == 6 / speed


def test_speed_change_mid_run_changes_later_pacing() -> None:
    wall = FakeWall()
    pacer = RealTimePacer(1, wall_clock=wall)
    trace: Trace = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        await asyncio.sleep(2)
        trace.append((loop.time(), wall.t))
        pacer.speed = 4  # called from inside the simulation
        assert pacer.speed == 4
        await asyncio.sleep(4)
        trace.append((loop.time(), wall.t))

    desyncio.run(main(), pacer=pacer)
    assert trace == [(2, 2.0), (6, 3.0)]  # 2 s at speed 1, then 4 s at speed 4


def test_interrupted_wait_advances_by_elapsed_real_time_times_speed() -> None:
    wall = FakeWall()
    pacer = RealTimePacer(1, wall_clock=wall)
    calls = 0

    def interrupt(timeout: float) -> tuple[float, Callable[[], None]] | None:
        nonlocal calls
        calls += 1
        if calls == 1:
            assert timeout == 10  # first wait: 10 virtual s at speed 1
            # 4 real seconds pass, then the user doubles the speed.
            return 0.4, lambda: setattr(pacer, "speed", 2)
        return None

    wall.interrupt = interrupt
    trace: Trace = []
    desyncio.run(sleeps([10], trace, wall), pacer=pacer)
    # 4 virtual s elapsed in the first 4 real s; the remaining 6 virtual s at
    # speed 2 take 3 real s.
    assert wall.waits == [10, 3.0]
    assert trace == [(10, 7.0)]


def test_pause_and_resume_from_inside_the_simulation() -> None:
    wall = FakeWall()
    pacer = RealTimePacer(1, wall_clock=wall)
    loops: list[VirtualTimeLoop] = []
    seen: list[tuple[float, bool]] = []

    def human(w: FakeWall) -> None:
        # The loop is idle and paused: virtual time must be frozen at 1.
        seen.append((loops[0].time(), pacer.paused))
        w.t += 5  # five real seconds go by while paused
        pacer.resume()

    wall.on_paused_wait = human
    trace: Trace = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        assert isinstance(loop, VirtualTimeLoop)
        loops.append(loop)
        await asyncio.sleep(1)
        trace.append((loop.time(), wall.t))
        pacer.pause()
        assert pacer.paused
        await asyncio.sleep(1)
        trace.append((loop.time(), wall.t))

    desyncio.run(main(), pacer=pacer)
    assert seen == [(1.0, True)]
    assert trace == [(1, 1.0), (2, 7.0)]  # 1 + 5 paused + 1
    assert not pacer.paused


def test_deadlock_still_raised_when_not_paused() -> None:
    async def main() -> None:
        await asyncio.get_running_loop().create_future()

    with pytest.raises(SimulationDeadlock):
        desyncio.run(main(), speed=2)


def test_paused_loop_waits_for_resume_instead_of_deadlocking() -> None:
    wall = FakeWall()
    pacer = RealTimePacer(1, wall_clock=wall)
    pacer.pause()
    wall.on_paused_wait = lambda w: pacer.resume()

    async def main() -> None:
        await asyncio.get_running_loop().create_future()

    # It waited for the resume, and only then reported the deadlock.
    with pytest.raises(SimulationDeadlock):
        desyncio.run(main(), pacer=pacer)
    assert wall.waits == [None]


class SignallingWall(SystemWallClock):
    """The real wall clock, but tells the test when the loop is waiting."""

    def __init__(self) -> None:
        self.waiting = threading.Event()
        self.paused_waiting = threading.Event()

    def wait(self, timeout: float | None, selector: selectors.BaseSelector) -> Events:
        (self.paused_waiting if timeout is None else self.waiting).set()
        return super().wait(timeout, selector)


def test_pause_and_resume_from_another_thread_real_clock() -> None:
    wall = SignallingWall()
    pacer = RealTimePacer(1000, wall_clock=wall)  # 1 virtual s ~ 1 ms real
    pacer.pause()
    loops: list[VirtualTimeLoop] = []
    failures: list[BaseException] = []

    def ui() -> None:
        try:
            assert wall.paused_waiting.wait(5), "loop never went idle-paused"
            assert loops[0].time() == 0.0  # frozen while paused
        except BaseException as e:  # noqa: BLE001
            failures.append(e)
        finally:
            pacer.resume()  # never leave the loop hanging

    async def main() -> float:
        loop = asyncio.get_running_loop()
        assert isinstance(loop, VirtualTimeLoop)
        loops.append(loop)
        await asyncio.sleep(1)
        return loop.time()

    t = threading.Thread(target=ui)
    t.start()
    assert desyncio.run(main(), pacer=pacer) == 1.0
    t.join()
    assert failures == []


def test_speed_change_from_another_thread_interrupts_a_long_wait() -> None:
    wall = SignallingWall()
    pacer = RealTimePacer(1e-6, wall_clock=wall)  # 1 virtual s = 1e6 real s!

    def ui() -> None:
        wall.waiting.wait(5)
        pacer.speed = math.inf  # wakes the self-pipe; the long wait ends

    async def main() -> float:
        await asyncio.sleep(1)
        return asyncio.get_running_loop().time()

    t = threading.Thread(target=ui)
    t.start()
    assert desyncio.run(main(), pacer=pacer) == 1.0
    t.join()


def test_real_wall_clock_smoke() -> None:
    async def main() -> float:
        await asyncio.sleep(1)
        return asyncio.get_running_loop().time()

    t0 = time.monotonic()
    assert desyncio.run(main(), speed=1000) == 1.0
    assert time.monotonic() - t0 < 2.0  # ~1 ms expected; bound is loose


def test_speed_inf_equals_fast_mode() -> None:
    durations = [1, 2.5, 0.25, 3]
    fast: Trace = []
    desyncio.run(sleeps(durations, fast, FakeWall()))
    wall, inf_trace = run_fake(durations, math.inf)
    assert inf_trace == fast
    assert wall.waits == [] and wall.t == 0.0  # never waited


def test_speed_inf_with_default_clock_does_not_wait() -> None:
    async def main() -> float:
        await asyncio.sleep(10**9)
        return asyncio.get_running_loop().time()

    t0 = time.monotonic()
    assert desyncio.run(main(), speed=math.inf) == 10**9
    assert time.monotonic() - t0 < 2.0


@pytest.mark.parametrize("bad", [0, 0.0, -1, -0.5, math.nan])
def test_speed_value_errors(bad: float) -> None:
    with pytest.raises(ValueError):
        RealTimePacer(bad)
    pacer = RealTimePacer()
    with pytest.raises(ValueError):
        pacer.speed = bad
    assert pacer.speed == 1.0  # unchanged


@pytest.mark.parametrize("bad", ["1", None, True, [1], 1j])
def test_speed_type_errors(bad: Any) -> None:
    with pytest.raises(TypeError):
        RealTimePacer(bad)
    with pytest.raises(TypeError):
        RealTimePacer().speed = bad


def test_speed_accepts_inf_and_ints() -> None:
    assert RealTimePacer(math.inf).speed == math.inf
    assert RealTimePacer(3).speed == 3.0
    assert not RealTimePacer().paused


def test_run_rejects_speed_and_pacer_together() -> None:
    async def main() -> None: ...

    coro = main()
    with pytest.raises(TypeError, match="speed"):
        desyncio.run(coro, speed=2, pacer=RealTimePacer())  # type: ignore[call-overload]
    assert coro.cr_frame is None  # closed: no "never awaited" warning


def test_run_rejects_bad_speed_and_pacer_types() -> None:
    async def main() -> None: ...

    for kwargs in ({"speed": 0}, {"speed": "x"}, {"pacer": object()}):
        coro = main()
        with pytest.raises((TypeError, ValueError)):
            desyncio.run(coro, **kwargs)  # type: ignore[call-overload]
        assert coro.cr_frame is None


def test_pacer_binds_to_one_live_loop() -> None:
    pacer = RealTimePacer()
    loop = VirtualTimeLoop(pacer=pacer)
    try:
        with pytest.raises(ValueError, match="already"):
            VirtualTimeLoop(pacer=pacer)
    finally:
        loop.close()
    VirtualTimeLoop(pacer=pacer).close()  # fine once the first loop is closed


def test_until_caps_real_time_at_until() -> None:
    wall = FakeWall()

    async def main() -> None:
        await asyncio.sleep(10)  # next timer is far beyond `until`

    pacer = RealTimePacer(2, wall_clock=wall)
    assert desyncio.run(main(), until=3, pacer=pacer) is None
    assert wall.waits == [1.5]  # real time for 3 virtual s, not for 10
    assert wall.t == 1.5


def test_until_with_timers_at_and_after_until() -> None:
    wall = FakeWall()
    trace: Trace = []

    async def main() -> None:
        await sleeps([1] * 100, trace, wall)

    desyncio.run(main(), until=3, pacer=RealTimePacer(0.5, wall_clock=wall))
    assert trace == [(1, 2.0), (2, 4.0), (3, 6.0)]  # t == until runs, 4 does not
    assert wall.t == 6.0


def test_until_cleanup_does_not_wait_in_real_time() -> None:
    wall = FakeWall()

    async def main() -> None:
        try:
            await asyncio.sleep(10)
        finally:
            await asyncio.sleep(0)  # needs no virtual time

    desyncio.run(main(), until=3, pacer=RealTimePacer(1, wall_clock=wall))
    assert wall.t == 3.0


def _model(trace: Trace) -> Any:
    async def worker(name: int, period: float) -> None:
        loop = asyncio.get_running_loop()
        for _ in range(4):
            await asyncio.sleep(period)
            trace.append((name, loop.time()))

    async def main() -> None:
        await asyncio.gather(worker(0, 1.5), worker(1, 1.0), worker(2, 1.5))

    return main()


def test_determinism_same_trace_as_fast_mode_and_same_wall_series() -> None:
    fast: Trace = []
    desyncio.run(_model(fast))

    def paced() -> tuple[Trace, list[float | None], float]:
        wall = FakeWall()
        trace: Trace = []
        desyncio.run(_model(trace), pacer=RealTimePacer(2, wall_clock=wall))
        return trace, wall.waits, wall.t

    t1, waits1, end1 = paced()
    t2, waits2, end2 = paced()
    assert t1 == fast  # same events, same order, same virtual times
    assert (t1, waits1, end1) == (t2, waits2, end2)
    assert end1 == 3.0  # 6 virtual seconds at speed 2



def _switch_to_fast_after(wall: FakeWall, pacer: RealTimePacer, n: int) -> None:
    """After ``n`` full timed waits, the "user" sets speed to inf mid-wait.

    Without this the tiny-speed tests would wait for (fake) eons.
    """

    def interrupt(timeout: float) -> tuple[float, Callable[[], None]] | None:
        if len(wall.waits) > n:
            return 0.5, lambda: setattr(pacer, "speed", math.inf)
        return None

    wall.interrupt = interrupt


class SelectLimitWall(FakeWall):
    """A fake that, like a real ``select``, overflows on huge timeouts.

    It also records the virtual time at each timed wait.
    """

    def __init__(self) -> None:
        super().__init__()
        self.virtual_at_wait: list[float] = []

    def wait(self, timeout: float | None, selector: selectors.BaseSelector) -> Events:
        if timeout is not None:
            if timeout > 9.2e9:
                raise OverflowError("timestamp out of range for C PyTime_t")
            self.virtual_at_wait.append(asyncio.get_event_loop().time())
        return super().wait(timeout, selector)


def test_tiny_speed_waits_in_chunks_without_overflow() -> None:
    # 1 virtual s at speed 1e-300 is 1e300 real s, far beyond what select()
    # accepts (~9.2e9 s). Each wall wait must be at most one chunk.
    wall = SelectLimitWall()
    pacer = RealTimePacer(1e-300, wall_clock=wall)
    _switch_to_fast_after(wall, pacer, 3)
    trace: Trace = []
    desyncio.run(sleeps([1], trace, wall), pacer=pacer)
    assert wall.waits[:3] == [MAX_REAL_WAIT] * 3
    assert all(w is not None and 0 < w <= MAX_REAL_WAIT for w in wall.waits)
    # A chunk is not a full wait: the clock moved by elapsed * speed only
    # (strictly forward, tiny), and never jumped to the timer.
    seen = wall.virtual_at_wait
    assert seen[0] == 0.0
    assert all(a < b < 1e-290 for a, b in zip(seen, seen[1:], strict=False))
    assert trace[0][0] == 1.0


def test_slow_speed_long_wait_is_chunked_and_exact() -> None:
    # speed 1e-6: 1 virtual s is 1e6 real s, i.e. 11 full days + a remainder.
    durations = [1]
    fast: Trace = []
    desyncio.run(sleeps(durations, fast, FakeWall()))
    wall, trace = run_fake(durations, 1e-6)
    assert len(wall.waits) == 12
    assert wall.waits[:11] == [MAX_REAL_WAIT] * 11
    assert all(w is not None and w <= MAX_REAL_WAIT for w in wall.waits)
    assert wall.t == pytest.approx(1e6)
    assert [v for v, _ in trace] == [v for v, _ in fast] == [1.0]


def test_far_timer_at_speed_one_does_not_overflow() -> None:
    wall = SelectLimitWall()
    pacer = RealTimePacer(1, wall_clock=wall)
    _switch_to_fast_after(wall, pacer, 3)
    fired: list[float] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        done = loop.create_future()

        def fire() -> None:
            fired.append(loop.time())
            done.set_result(None)

        loop.call_later(1e10, fire)
        await done

    desyncio.run(main(), pacer=pacer)
    assert all(w is not None and w <= MAX_REAL_WAIT for w in wall.waits)
    assert fired == [1e10]


def test_speed_1e_minus_12_does_not_overflow() -> None:
    wall = SelectLimitWall()
    pacer = RealTimePacer(1e-12, wall_clock=wall)
    _switch_to_fast_after(wall, pacer, 2)
    trace: Trace = []
    desyncio.run(sleeps([1], trace, wall), pacer=pacer)
    assert all(w is not None and w <= MAX_REAL_WAIT for w in wall.waits)
    assert trace[0][0] == 1.0


def test_chunk_below_one_float_step_still_makes_progress() -> None:
    # At t = 2**60 one float step is 256 s. At speed 1e-30 a one-day chunk is
    # 8.64e-26 virtual s, which rounds away (now + tiny == now). The clock
    # must still move (one float step per chunk) so the loop terminates.
    wall = FakeWall()
    pacer = RealTimePacer(math.inf, wall_clock=wall)

    async def main() -> float:
        loop = asyncio.get_running_loop()
        await asyncio.sleep(2.0**60)  # fast: no waiting
        pacer.speed = 1e-30
        await asyncio.sleep(math.ulp(2.0**60))
        return loop.time()

    assert desyncio.run(main(), pacer=pacer) == 2.0**60 + 256
    assert wall.waits == [MAX_REAL_WAIT]  # one chunk, then the timer is due


def test_failed_bind_leaks_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    # A second loop on a bound pacer must not leave its selector and
    # self-pipe sockets behind ("unclosed event loop" / ResourceWarning),
    # and run() must close main (no "never awaited" warning). Those warnings
    # are emitted from __del__, so they reach sys.unraisablehook rather than
    # this frame; record them there.
    unraisable: list[str] = []
    monkeypatch.setattr(
        sys, "unraisablehook", lambda u: unraisable.append(repr(u.exc_value))
    )
    pacer = RealTimePacer()
    live = VirtualTimeLoop(pacer=pacer)

    async def main() -> None: ...

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            with pytest.raises(ValueError, match="already"):
                VirtualTimeLoop(pacer=pacer)
            gc.collect()
            coro = main()
            with pytest.raises(ValueError, match="already"):
                desyncio.run(coro, pacer=pacer)
            assert coro.cr_frame is None
            del coro
            gc.collect()
    finally:
        live.close()
    assert unraisable == []


def test_guards_docstring_does_not_claim_the_loop_never_waits() -> None:
    # D011: a paced loop does wait (scaled virtual time), so the guards
    # module must not say "never waits" without qualifying it.
    doc = " ".join((desyncio.guards.__doc__ or "").split())
    assert "VirtualTimeLoop` never waits:" not in doc
    assert "never waits for the real world in fast mode" in doc
    assert "real-time mode" in doc
