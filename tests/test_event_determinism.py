"""Event streams are deterministic (EVT-03, D060).

The same model and seed give the same ``[e.to_dict() for e in events]``: in one
process, in a fresh interpreter, whatever the observers are. The model is the
``Resource``-based M/M/1 in ``tests/models/mm1_resource.py``.
"""

import asyncio
import hashlib
import json
import os
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import desyncio
from desyncio import Resource, sim
from desyncio.events import Event, ResourceChanged, RunFinished, RunStarted
from models.mm1_resource import Mm1Results, mm1_resource

SEED = 7
N = 300
TESTS_DIR = Path(__file__).parent
SRC_DIR = TESTS_DIR.parent / "src"

Dicts = list[dict[str, Any]]


def run_events(
    seed: int = SEED,
    *,
    kinds: tuple[type[Event], ...] | None = None,
    until: float | None = None,
) -> tuple[Mm1Results | None, list[Event]]:
    events: list[Event] = []
    subs = [(events.append, kinds)]
    result = desyncio.run(mm1_resource(N), seed=seed, until=until, subscribers=subs)
    return result, events


def dicts(events: Sequence[Event]) -> Dicts:
    return [e.to_dict() for e in events]


def test_same_seed_same_events_and_started_finished() -> None:
    results, first = run_events()
    _, second = run_events()
    assert dicts(first) == dicts(second)
    assert len(first) > 2 * N  # a real stream: queue, grant, release per customer
    assert isinstance(first[0], RunStarted)
    assert (first[0].seed, first[0].until) == (SEED, None)
    assert isinstance(first[-1], RunFinished)
    assert first[-1].reason == "main_returned"
    assert results is not None and results.served == N
    assert [e.seq for e in first] == list(range(len(first)))


def test_different_seed_gives_different_events() -> None:
    _, a = run_events(SEED)
    _, b = run_events(SEED + 1)
    assert dicts(a) != dicts(b)


_CHILD = """
import hashlib, json, sys
sys.path.insert(0, sys.argv[1])  # tests/, for models.*
sys.path.insert(0, sys.argv[2])  # src/, so desyncio is this checkout's
import desyncio
from models.mm1_resource import mm1_resource

events = []
desyncio.run(mm1_resource(int(sys.argv[4])), seed=int(sys.argv[3]),
             subscribers=[events.append])
blob = json.dumps([e.to_dict() for e in events], sort_keys=True)
print(len(events), hashlib.sha256(blob.encode()).hexdigest())
"""


def test_same_events_in_a_fresh_interpreter() -> None:
    _, events = run_events()
    blob = json.dumps(dicts(events), sort_keys=True)
    expected = f"{len(events)} {hashlib.sha256(blob.encode()).hexdigest()}"
    args = [str(TESTS_DIR), str(SRC_DIR), str(SEED), str(N)]
    for hashseed in ("0", "4242"):
        out = subprocess.run(
            [sys.executable, "-c", _CHILD, *args],
            env={**os.environ, "PYTHONHASHSEED": hashseed},  # keeps PATH etc.
            capture_output=True,
            text=True,
            check=False,
        )
        assert out.returncode == 0, out.stderr  # show the child's traceback
        assert out.stdout.strip() == expected


def test_observing_changes_neither_results_nor_seq() -> None:
    bare = desyncio.run(mm1_resource(N), seed=SEED)
    with_all, everything = run_events()
    with_kind, only_changes = run_events(kinds=(ResourceChanged,))
    with_ends, only_ends = run_events(kinds=(RunStarted, RunFinished))
    assert bare == with_all == with_kind == with_ends
    # seq ticks whether or not anyone listens, so a filter sees the same seq
    # values the unfiltered run gave those events.
    wanted = [e for e in everything if isinstance(e, ResourceChanged)]
    assert only_changes
    assert dicts(only_changes) == dicts(wanted)
    assert only_changes[0].seq >= 1  # 0 was RunStarted, filtered out but counted
    # ResourceChanged is one contiguous seq block here, so a filter that
    # renumbered delivered events densely would still pass the checks above.
    # Two kinds with a gap between them cannot be renumbered unnoticed.
    assert [type(e) for e in only_ends] == [RunStarted, RunFinished]
    assert [e.seq for e in only_ends] == [0, len(everything) - 1]
    assert dicts(only_ends) == dicts([everything[0], everything[-1]])


def test_until_cut_with_holders_and_waiters_is_repeatable() -> None:
    runs = [run_events(until=40.0)[1] for _ in range(3)]
    assert dicts(runs[0]) == dicts(runs[1]) == dicts(runs[2])
    events = runs[0]
    # This is the guard that teardown (cancelled tasks releasing the server)
    # adds nothing to the stream: RunFinished must be the last event. The
    # 3-run equality above does not guard it: teardown cancels tasks in an
    # order that can depend on object addresses, so events leaking from it
    # could differ between runs or happen to match. Do not remove this check.
    assert isinstance(events[-1], RunFinished) and events[-1].reason == "until"
    assert isinstance(events[0], RunStarted) and events[0].until == 40.0
    assert sum(isinstance(e, RunFinished) for e in events) == 1
    assert max(e.t for e in events) <= 40.0
    # Cut while a customer held the server and others waited.
    changes = [e for e in events if isinstance(e, ResourceChanged)]
    assert changes[-1].in_use == 1
    assert changes[-1].queue_len >= 1  # waiters were left when the run was cut


@dataclass(frozen=True, slots=True, kw_only=True)
class Arrived(Event):
    who: int


def _same_t(order: list[int]) -> Dicts:
    """Tasks made in ``order`` all wake at t=1 and each takes the one server."""

    async def main() -> None:
        server = Resource(1, name="s")

        async def customer(i: int) -> None:
            await sim.timeout(1.0)
            sim.emit(Arrived, who=i)
            async with server:
                await sim.timeout(0.5)

        tasks = [sim.spawn(customer(i)) for i in order]
        await asyncio.gather(*tasks)

    events: list[Event] = []
    desyncio.run(main(), subscribers=[events.append])
    return dicts(events)


def test_same_t_events_follow_task_creation_order() -> None:
    for order in ([0, 1, 2, 3, 4], [3, 1, 4, 0, 2]):
        stream = _same_t(order)
        assert stream == _same_t(order)
        assert [d["seq"] for d in stream] == list(range(len(stream)))
        at_one = [d for d in stream if d["t"] == 1.0]
        # Timers due at the same t fire in creation order (LOOP-03), so the
        # arrivals are announced in that order, not sorted by id.
        assert [d["who"] for d in at_one if d["kind"] == "Arrived"] == order
        # Each arrival requests the server in that same order: the first is
        # granted, the others queue up behind it one by one.
        changes = [d for d in at_one if d["kind"] == "ResourceChanged"]
        assert [(d["in_use"], d["queue_len"]) for d in changes] == [
            (1, 0),
            (1, 1),
            (1, 2),
            (1, 3),
            (1, 4),
        ]
