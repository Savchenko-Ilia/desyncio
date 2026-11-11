"""PERF-04 (D075, D076): ``Container`` scales; its waiting changed only as D076 allows.

``Container`` used to wait on ``asyncio.Condition``. A cancelled waiter's
``Condition.wait`` removed its future with ``deque.remove`` and rescanned the
deque in ``_notify(1)``, and the cleanup then called ``notify_all()``: each a
walk past every cancelled future, once per waiter, so cancelling n waiters was
O(n²) (about 117 s for 50,000). It now keeps one FIFO line of futures per
direction and wakes only the heads.

Scaling, measured deterministically (no clock is read): every future created
by the loop is a ``_CountingFuture`` that counts the calls a scan makes on it
(``==`` from ``deque.remove``, ``done()``/``cancelled()`` from wake-up loops),
and every deque the container module builds is a ``_CountingDeque`` that
counts the elements iterated over, searched by ``remove``/``index``, or put
into a new deque (a rebuild). That second count sees work no future method
sees: a search that compares with ``is``, a rebuild on every cancellation, a
``list(...)`` copy of a line. Each count is compared between n and 4n
waiters: linear work gives a ratio of about 4, quadratic work about 16; the
threshold, 8, is 2x away from both. Four patterns, for getters (on an empty
container) and putters (on a full one):

* mass cancel: every waiter cancelled at one instant, newest first (a search
  from the front then walks the whole line every time);
* reneging: waiter i waits inside ``asyncio.timeout(n - i)``, so they leave
  one at a time, newest first, while the others keep waiting (a rebuild or a
  scan on every departure is quadratic here, though no instant cancels many);
  between departures the line must hold at most about twice its live waiters;
* served in turn: n waiters, each served by its own level change (extra O(n)
  work per grant is quadratic here);
* teardown: the run ends at ``until`` with n waiters parked. The runner
  cancels them in address order, so these counts vary between runs; instead
  of a ratio the test bounds the work per waiter, which holds in any order.

The counts of the first three patterns are the same on every run.

Behaviour: the differential test runs random scenarios on this ``Container``
and on ``_ConditionContainer`` (the pre-PERF-04 code, on ``Condition``) and
checks what must still agree; ``test_d076_same_instant_order_changed`` pins
one scenario where the order within an instant is different, on purpose, and
the two ``test_d076_exception_*`` tests pin the documented exceptions where
even who is served, and the level, differ (a cancel issued partway through an
instant; amounts that do not add up exactly). The differential scenarios avoid
both: integer amounts, cancels from timers only.
"""

import asyncio
import contextlib
import math
import random
from collections import defaultdict, deque
from collections.abc import Callable, Iterable, Iterator
from typing import Any

import pytest

import desyncio
import desyncio.container
from desyncio import sim
from desyncio.container import Container
from desyncio.loop import VirtualTimeLoop

N = 250  # compared with 4 * N
THRESHOLD = 8.0  # linear: ~4, quadratic: ~16
PER_WAITER = 8  # teardown work bound per waiter (see its test)

# --- scaling: deterministic counts of the work on waiter futures and lines ---


class _CountingFuture(asyncio.Future[Any]):
    """A future that counts how often a scan looks at it."""

    touches = 0

    def done(self) -> bool:
        _CountingFuture.touches += 1
        return super().done()

    def cancelled(self) -> bool:
        _CountingFuture.touches += 1
        return super().cancelled()

    def __eq__(self, other: object) -> bool:  # deque.remove compares with ==
        _CountingFuture.touches += 1
        return self is other

    __hash__ = asyncio.Future.__hash__


class _CountingDeque(deque[Any]):
    """A deque that counts the elements a scan, search or rebuild steps over.

    The container builds its lines through the module's ``deque`` name, so
    patching ``desyncio.container.deque`` with this class makes every line
    count. O(1) operations (``append``, ``popleft``, ``[0]``, ``len``) count
    nothing.
    """

    steps = 0

    def __init__(self, iterable: Iterable[Any] = ()) -> None:
        super().__init__(iterable)
        _CountingDeque.steps += len(self)  # a rebuild (or copy) of this length

    def __iter__(self) -> Iterator[Any]:
        for element in super().__iter__():
            _CountingDeque.steps += 1
            yield element

    def remove(self, value: Any) -> None:
        # deque.remove scans from the left up to the first match; it compares
        # by identity first, so a match costs no ``__eq__``.
        try:
            _CountingDeque.steps += super().index(value) + 1
        except ValueError:
            _CountingDeque.steps += len(self)
        super().remove(value)

    def index(self, value: Any, *args: Any) -> int:
        found = super().index(value, *args)
        _CountingDeque.steps += found + 1
        return found


Work = tuple[int, int]  # (future touches, deque steps)


@pytest.fixture(autouse=True)
def counting(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every loop future counts, and so does every deque the container builds."""

    def create_future(self: VirtualTimeLoop) -> asyncio.Future[Any]:
        return _CountingFuture(loop=self)

    monkeypatch.setattr(VirtualTimeLoop, "create_future", create_future)
    monkeypatch.setattr(desyncio.container, "deque", _CountingDeque)


def _reset() -> None:
    _CountingFuture.touches = 0
    _CountingDeque.steps = 0


def _work() -> Work:
    return _CountingFuture.touches, _CountingDeque.steps


def _blocked(side: str) -> tuple[Container, Callable[[], Any]]:
    """A container on which every ``side`` request of 1 blocks, and the request."""
    if side == "get":
        c = Container()  # empty
        return c, lambda: c.get(1)
    c = Container(1, init=1)  # full
    return c, lambda: c.put(1)


def _line(c: Container, side: str) -> Any:
    return c._getters if side == "get" else c._putters


def _mass_cancel(side: str, n: int) -> Work:
    """n parked waiters, all cancelled at one instant, newest first."""

    async def main() -> None:
        c, request = _blocked(side)
        tasks = [sim.spawn(request()) for _ in range(n)]
        await sim.timeout(1)
        for task in reversed(tasks):
            task.cancel()
        await sim.timeout(1)
        assert all(task.cancelled() for task in tasks)

    _reset()
    desyncio.run(main())
    return _work()


def _reneging(side: str, n: int) -> Work:
    """Waiter i gives up at t = n - i: one departure per instant, newest first.

    Bounded memory: between departures a line holds at most twice its live
    waiters plus the slack. (Read through the private line on purpose:
    departed records are invisible in the public API.)
    """

    async def renege(request: Callable[[], Any], i: int) -> None:
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(n - i):
                await request()

    async def main() -> None:
        c, request = _blocked(side)
        tasks = [sim.spawn(renege(request, i)) for i in range(n)]
        await sim.timeout(0.5)
        line = _line(c, side)
        for _ in range(n):
            await sim.timeout(1)
            kept = deque.__len__(line._waiters)
            assert kept <= 2 * line.live + desyncio.container._SLACK, (
                f"{kept} records kept for {line.live} live waiters"
            )
        assert all(task.done() for task in tasks)
        assert line.live == 0

    _reset()
    desyncio.run(main())
    return _work()


def _teardown(side: str, n: int) -> Work:
    """n parked waiters when the run ends at ``until``; teardown cancels them."""

    async def main() -> None:
        _, request = _blocked(side)
        for _ in range(n):
            sim.spawn(request())
        await sim.timeout(10)

    _reset()
    desyncio.run(main(), until=5)
    return _work()


def _served_in_turn(side: str, n: int) -> Work:
    """n queued waiters, each served by its own level change, one per instant."""

    async def main() -> None:
        c, request = _blocked(side)
        tasks = [sim.spawn(request()) for _ in range(n)]
        await sim.timeout(1)
        for i in range(n):
            if side == "get":
                await c.put(1)
            else:
                await c.get(1)
            await sim.timeout(1)
            assert sum(task.done() for task in tasks) == i + 1
        assert c.level == (0 if side == "get" else 1)

    _reset()
    desyncio.run(main())
    return _work()


MEASURES = {
    "mass-cancel": _mass_cancel,
    "reneging": _reneging,
    "served-in-turn": _served_in_turn,
}


@pytest.mark.parametrize("measure", list(MEASURES))
@pytest.mark.parametrize("side", ["get", "put"])
def test_work_is_linear(side: str, measure: str) -> None:
    small = MEASURES[measure](side, N)
    large = MEASURES[measure](side, 4 * N)
    for what, s, b in zip(("future touches", "deque steps"), small, large, strict=True):
        # A count that stays tiny for both sizes is not a scan; bound it instead.
        if s < N:
            assert b < 4 * N, f"{measure} {side}: {what} {s} -> {b} for n -> 4n"
            continue
        ratio = b / s
        # linear: about 4; quadratic: about 16
        assert ratio < THRESHOLD, (
            f"{measure} {side}: {what} {s} -> {b} (x{ratio:.1f}) for n -> 4n"
        )


@pytest.mark.parametrize("side", ["get", "put"])
def test_teardown_work_is_bounded_per_waiter(side: str) -> None:
    # The runner cancels leftover tasks in address order (set iteration), so
    # these counts vary from run to run and an n -> 4n ratio could compare a
    # lucky order with an unlucky one. The bound holds in every order: a
    # departure from the middle of a line is O(1), and a rebuild of a line of
    # length L happens only once more than L/2 of it has departed and costs
    # under 3 steps per departed record. Quadratic cleanup does hundreds of
    # steps per waiter at this size (the old code: over 1,000 future touches).
    for n in (N, 4 * N):
        for what, count in zip(
            ("future touches", "deque steps"), _teardown(side, n), strict=True
        ):
            assert count <= PER_WAITER * n, f"teardown {side}: {what} {count} for {n}"


# --- behaviour: what must still agree with the Condition-based original ------


class _ConditionContainer:
    """The pre-PERF-04 ``Container`` waiting logic, on ``asyncio.Condition``."""

    def __init__(
        self, capacity: float, init: float, monitor: Callable[[Any], None]
    ) -> None:
        self._capacity = capacity
        self._level = init
        self._monitor = monitor
        self._cond = asyncio.Condition()
        self._putters: deque[object] = deque()
        self._getters: deque[object] = deque()

    @property
    def level(self) -> float:
        return self._level

    async def put(self, amount: float) -> None:
        room = self._capacity - amount
        await self._transfer(self._putters, amount, lambda: self._level <= room)

    async def get(self, amount: float) -> None:
        await self._transfer(self._getters, -amount, lambda: self._level >= amount)

    async def _transfer(
        self, queue: deque[object], delta: float, fits: Callable[[], bool]
    ) -> None:
        ticket = object()
        async with self._cond:
            queue.append(ticket)
            try:
                await self._cond.wait_for(lambda: queue[0] is ticket and fits())
                self._level += delta
            finally:
                queue.remove(ticket)
                self._cond.notify_all()
            self._monitor(self)


Log = list[tuple[Any, ...]]


def _scenario(ours: bool, seed: int) -> tuple[Log, Log]:
    """A random scenario of one-request actors; returns (service log, monitor log).

    Requests start at whole instants; timers cancel some of them, at whole
    instants (together with service) or half-way between. Each actor makes a
    single request, so no request's arrival depends on when another task
    resumed within an instant.
    """
    rng = random.Random(seed)
    capacity = rng.choice([10, 20, math.inf])
    init = rng.choice([0, 5, 10])
    served: Log = []
    snapshots: Log = []

    def monitor(c: Any) -> None:
        snapshots.append((sim.now, c.level))

    async def actor(c: Any, i: int, side: str, amount: float, start: int) -> None:
        await sim.timeout(start)
        try:
            await (c.put(amount) if side == "put" else c.get(amount))
        except asyncio.CancelledError:
            served.append((sim.now, side, i, "cancelled"))
            raise
        served.append((sim.now, side, i, "served"))

    async def cancel_at(task: asyncio.Task[None], at: float) -> None:
        await sim.timeout(at)
        task.cancel()

    async def main() -> None:
        c: Any
        if ours:
            c = Container(capacity, init, monitor=monitor)
        else:
            c = _ConditionContainer(capacity, init, monitor)
        for i in range(rng.randrange(5, 60)):
            side = rng.choice(["put", "get"])
            amount = rng.choice([1, 2, 3, 5, 8, 10])
            start = rng.randrange(15)
            task = sim.spawn(actor(c, i, side, amount, start))
            if rng.random() < 0.3:
                at = start + rng.randrange(4) + rng.choice([0, 0.5])
                sim.spawn(cancel_at(task, at))
        await sim.timeout(30)
        served.append(("left", c.level))

    desyncio.run(main(), until=40)
    return served, snapshots


def _agreed_parts(served: Log, snapshots: Log) -> dict[str, Any]:
    """The parts of a run that D076 does not allow to change here."""
    # Drop what the teardown logs after "left": it cancels in address order.
    cut = next(i for i, entry in enumerate(served) if entry[0] == "left")
    outcome = {}  # every request: when it was served or cancelled
    order: dict[str, list[int]] = defaultdict(list)  # service order per direction
    sides_at: dict[float, set[str]] = defaultdict(set)
    for now, side, i, what in served[:cut]:
        outcome[side, i] = (now, what)
        if what == "served":
            order[side].append(i)
            sides_at[now].add(side)
    levels: dict[float, float] = {}
    one_sided: dict[float, list[float]] = defaultdict(list)
    for now, level in snapshots:
        levels[now] = level  # the level at the end of each instant
        if len(sides_at[now]) == 1:
            one_sided[now].append(level)  # every snapshot, in order
    return {
        "outcome": outcome,
        "order": dict(order),
        "levels": levels,
        "one_sided": dict(one_sided),
        "left": served[cut],
    }


def _before_teardown(served: Log, snapshots: Log) -> tuple[Log, Log]:
    """The full service log up to "left", and every monitor snapshot, in order."""
    cut = next(i for i, entry in enumerate(served) if entry[0] == "left")
    return served[: cut + 1], snapshots


def test_agrees_with_the_condition_original() -> None:
    differ = 0
    outcomes: set[str] = set()
    for seed in range(40):
        old, new = _scenario(False, seed), _scenario(True, seed)
        expected = _agreed_parts(*old)
        assert _agreed_parts(*new) == expected, f"seed {seed}"
        differ += _before_teardown(*old) != _before_teardown(*new)
        outcomes |= {what for _, what in expected["outcome"].values()}
    # The scenarios mix outcomes, and in some runs the order of service and
    # level changes within an instant really differs (D076), so the
    # comparison above leaves out something real. (Compared before the
    # teardown only: it cancels in address order, which differs between any
    # two runs, even of the same code.)
    assert outcomes == {"served", "cancelled"}
    assert differ > 0


def test_d076_same_instant_order_changed() -> None:
    # D076 (owner): only the head of a line is woken now, so within one
    # instant tasks may run in a different order than with
    # Condition.notify_all, which woke every parked waiter at once. The
    # container is full; "put 2" waits, and at t=1 "put 1" queues behind it.
    # Then "get 3" is served at once (level 7). The Condition woke both
    # putters together, so "put 1" ran right after "put 2", before the
    # bystander's next step. Now "put 1" is woken by "put 2"'s level change
    # and runs one step later. Who is served when, and every level, are
    # unchanged; only the order of steps within the instant differs.
    def run(ours: bool) -> Log:
        log: Log = []

        def monitor(c: Any) -> None:
            log.append(("level", sim.now, c.level))

        async def request(c: Any, op: str, amount: float, start: float) -> None:
            await sim.timeout(start)
            await getattr(c, op)(amount)
            log.append((f"{op} {amount}", sim.now))

        async def bystander() -> None:
            await sim.timeout(1)
            for k in range(3):
                log.append((f"bystander {k}", sim.now))
                await asyncio.sleep(0)  # one step

        async def main() -> None:
            c: Any
            if ours:
                c = Container(10, init=10, monitor=monitor)
            else:
                c = _ConditionContainer(10, 10, monitor)
            sim.spawn(request(c, "put", 2, 0))  # waits: the container is full
            sim.spawn(request(c, "put", 1, 1))  # waits behind "put 2"
            sim.spawn(request(c, "get", 3, 1))  # served at once
            sim.spawn(bystander())
            await sim.timeout(5)

        desyncio.run(main())
        return log

    old, new = run(False), run(True)
    assert old == [
        ("level", 1, 7),
        ("get 3", 1),
        ("bystander 0", 1),
        ("level", 1, 9),
        ("put 2", 1),
        ("level", 1, 10),
        ("put 1", 1),  # woken by "get 3" together with "put 2"
        ("bystander 1", 1),
        ("bystander 2", 1),
    ]
    assert new == [
        ("level", 1, 7),
        ("get 3", 1),
        ("bystander 0", 1),
        ("level", 1, 9),
        ("put 2", 1),
        ("bystander 1", 1),
        ("level", 1, 10),
        ("put 1", 1),  # woken by "put 2", the head before it
        ("bystander 2", 1),
    ]


def test_heads_are_woken_oldest_park_first() -> None:
    # The documented wake order: a level change wakes the parked head of each
    # line, the one that parked (or re-parked) first runs first, as
    # Condition.notify_all woke its waiters. Capacity 10, empty, all at t=0:
    #   "get 3" parks (park 0); "put 7" is served (level 7) and wakes it;
    #   "get 5" parks behind "get 3" (1); "put 5" parks (2); "put 1" (3).
    #   "get 3" runs: level 4, wakes both heads: "get 5" (1), then "put 5" (2).
    #   "get 5" does not fit (4 < 5) and re-parks (4); "put 5" fits: level 9,
    #   wakes "put 1" (3) before "get 5" (4). "put 1": level 10; "get 5": 5.
    # Newest park first, or putters before getters (no sort), run "put 5"
    # before "get 5" after "get 3" and serve "get 5" before "put 1"; without
    # the new number on re-park, "get 5" (still 1) runs before "put 1" (3).
    def run(ours: bool) -> Log:
        log: Log = []

        async def request(c: Any, op: str, amount: float) -> None:
            await getattr(c, op)(amount)
            log.append((f"{op} {amount}", c.level))

        async def main() -> None:
            c: Any
            if ours:
                c = Container(10)
            else:
                c = _ConditionContainer(10, 0, lambda c: None)
            for op, amount in [
                ("get", 3),
                ("put", 7),
                ("get", 5),
                ("put", 5),
                ("put", 1),
            ]:
                sim.spawn(request(c, op, amount))
            await sim.timeout(1)

        desyncio.run(main())
        return log

    expected = [("put 7", 7), ("get 3", 4), ("put 5", 9), ("put 1", 10), ("get 5", 5)]
    assert run(True) == expected
    assert run(False) == expected  # the Condition's order, too


def test_d076_exception_cancel_partway_through_an_instant() -> None:
    # D076, first documented exception: a cancel issued after one
    # asyncio.sleep(0) step, not by a timer. Same setup as above: "put 2"
    # waits on a full container, "put 1" queues behind it, and at t=1 a task
    # gets 3, takes one step and cancels "put 1". The Condition had woken
    # "put 1" together with "put 2", so it was served before the cancel came;
    # now it is still parked behind "put 2" and the cancel reaches it.
    def run(ours: bool) -> Log:
        log: Log = []

        async def request(c: Any, name: str, amount: float, start: float) -> None:
            await sim.timeout(start)
            try:
                await c.put(amount)
            except asyncio.CancelledError:
                log.append((name, "cancelled", sim.now))
                raise
            log.append((name, "served", sim.now))

        async def main() -> None:
            c: Any
            if ours:
                c = Container(10, init=10)
            else:
                c = _ConditionContainer(10, 10, lambda c: None)
            sim.spawn(request(c, "put 2", 2, 0))
            put1 = sim.spawn(request(c, "put 1", 1, 0.5))
            await sim.timeout(1)
            await c.get(3)
            await asyncio.sleep(0)  # one step into the instant
            put1.cancel()
            await sim.timeout(1)
            log.append(("final", c.level))

        desyncio.run(main())
        return log

    assert run(False) == [
        ("put 2", "served", 1),
        ("put 1", "served", 1),
        ("final", 10),
    ]
    assert run(True) == [
        ("put 2", "served", 1),
        ("put 1", "cancelled", 1),
        ("final", 9),
    ]


def test_d076_exception_amounts_that_do_not_add_up_exactly() -> None:
    # D076, second documented exception: with amounts like 0.1 the level is
    # rounded after every change, and the new order of puts and gets within
    # instant 2 rounds differently. At t=2 the old code ran 0.8 - 0.1 + 0.2
    # and ended the instant at 0.9000000000000001; the new code runs
    # 0.8 + 0.2 - 0.1 and ends it at 0.9. After "get 0.2" at t=3 the old level
    # is 0.7000000000000002, so "put 0.3" (it fits while level <= 1 - 0.3, that
    # is 0.7) is never served and holds back "put 0.1" behind it; the new
    # level is 0.7 and "put 0.3" is served at t=3. Found by the PERF-04
    # reviewer's random search; no smaller case turned up.
    spec = [
        ("put", 1, 2),
        ("get", 0.1, 2),
        ("get", 0.7, 0),
        ("put", 0.3, 3),
        ("get", 0.1, 1),
        ("get", 0.1, 1),
        ("get", 0.2, 3),
        ("put", 0.2, 2),
        ("put", 0.1, 3),
        ("put", 0.2, 0),
    ]

    def run(ours: bool) -> Log:
        log: Log = []

        async def actor(c: Any, name: str, op: str, amount: float, start: int) -> None:
            await sim.timeout(start)
            await getattr(c, op)(amount)
            log.append((name, sim.now))

        async def main() -> None:
            c: Any
            if ours:
                c = Container(1, init=0.5)
            else:
                c = _ConditionContainer(1, 0.5, lambda c: None)
            for i, (op, amount, start) in enumerate(spec):
                sim.spawn(actor(c, f"{op} {amount} #{i}", op, amount, start))
            await sim.timeout(10)
            log.append(("final", c.level))

        desyncio.run(main(), until=20)
        return log

    assert run(False) == [
        ("put 0.2 #9", 0),
        ("get 0.7 #2", 0),
        ("put 1 #0", 2),
        ("get 0.1 #4", 2),
        ("get 0.1 #5", 2),
        ("get 0.1 #1", 2),
        ("put 0.2 #7", 2),
        ("get 0.2 #6", 3),
        ("final", 0.7000000000000002),  # "put 0.3 #3" never fits
    ]
    assert run(True) == [
        ("put 0.2 #9", 0),
        ("get 0.7 #2", 0),
        ("put 1 #0", 2),
        ("get 0.1 #4", 2),
        ("get 0.1 #5", 2),
        ("put 0.2 #7", 2),  # before "get 0.1 #1" now
        ("get 0.1 #1", 2),
        ("get 0.2 #6", 3),
        ("put 0.3 #3", 3),
        ("final", 1.0),
    ]


# --- loop binding, as asyncio.Condition had it --------------------------------


def test_container_rejects_a_second_event_loop() -> None:
    c = Container()

    async def park() -> None:
        task = sim.spawn(c.get(1))
        await sim.timeout(1)
        if task.done():
            task.result()  # re-raise what the parking task hit
        task.cancel()

    desyncio.run(park())  # binds the container to this run's loop
    with pytest.raises(RuntimeError, match="different event loop"):
        desyncio.run(park())
