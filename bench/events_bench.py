"""Overhead of the event API (EVT-07): an M/M/1 queue, with and without subscribers.

Run by hand: ``uv run python bench/events_bench.py [--customers N] [--reps R]``.
Informative only, never a gate (D060). See ``bench/README.md`` for method and results.
"""

import argparse
import asyncio
import gc
import platform
import statistics
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import desyncio
from desyncio import Resource, sim
from desyncio.events import Event

SEED = 12345
ARRIVAL_RATE = 0.9  # lambda; with service rate 1.0 the utilisation is 0.9
SERVICE_RATE = 1.0

type Result = tuple[int, float, float]  # (customers served, total wait, final clock)


@dataclass(frozen=True, slots=True, kw_only=True)
class Unrelated(Event):
    """A custom kind that the model never emits: the "unrelated subscriber" case."""


def make_model(customers: int, use_resource: bool) -> Callable[[], Any]:
    """Build the M/M/1 main coroutine function.

    ``use_resource=True`` uses ``desyncio.Resource`` (emits ``ResourceChanged``);
    ``False`` uses ``asyncio.Semaphore(1)``, the same model on a primitive that
    cannot emit. Everything else (random streams, order of calls) is identical.
    """

    async def main() -> Result:
        arrivals = sim.random("arrivals")
        services = sim.random("services")
        server: Any = Resource(capacity=1) if use_resource else asyncio.Semaphore(1)
        waits: list[float] = []

        async def customer() -> None:
            arrived = sim.now
            async with server:
                waits.append(sim.now - arrived)
                await asyncio.sleep(services.expovariate(SERVICE_RATE))

        tasks = []
        for _ in range(customers):
            await asyncio.sleep(arrivals.expovariate(ARRIVAL_RATE))
            tasks.append(sim.spawn(customer()))
        await asyncio.gather(*tasks)
        return len(waits), sum(waits), sim.now

    return main


@dataclass
class Config:
    name: str
    use_resource: bool
    subscribers: Callable[[list[Event]], list[Any]]


def _noop(event: Event) -> None:
    return None


CONFIGS = [
    Config("semaphore baseline (no events)", False, lambda rec: []),
    Config("1. no subscriber", True, lambda rec: []),
    Config("2. subscriber, unrelated kind", True, lambda rec: [(_noop, (Unrelated,))]),
    Config("3. one no-op subscriber, all", True, lambda rec: [_noop]),
    Config("4. recording subscriber (append)", True, lambda rec: [rec.append]),
]


def time_once(
    cfg: Config, customers: int, seed: int = SEED
) -> tuple[float, Result, int]:
    """Run once; return (wall seconds, model result, events recorded).

    ``gc.collect()`` runs before the timer starts, so garbage left by the
    previous run (e.g. the recording subscriber's event objects) is not
    collected on this run's clock. The collector stays *enabled* while timing:
    collections caused by a configuration's own allocations (building and
    keeping event objects) are part of what that configuration costs.
    """
    recorded: list[Event] = []
    main = make_model(customers, cfg.use_resource)
    subscribers = cfg.subscribers(recorded)
    gc.collect()
    start = time.perf_counter()
    result = desyncio.run(main(), seed=seed, subscribers=subscribers)
    elapsed = time.perf_counter() - start
    return elapsed, result, len(recorded)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Event API overhead benchmark (EVT-07)."
    )
    parser.add_argument(
        "--customers", type=int, default=20_000, help="customers per run"
    )
    parser.add_argument(
        "--reps", type=int, default=7, help="timed repetitions per configuration"
    )
    parser.add_argument(
        "--check-fail",
        action="store_true",
        help=(
            "self-check of the identical-results assertion: run configuration 3 "
            "with a different seed, so the script must exit with FAIL"
        ),
    )
    args = parser.parse_args()
    customers: int = args.customers
    reps: int = args.reps
    if customers < 1:
        parser.error("--customers must be at least 1")
    if reps < 1:
        parser.error("--reps must be at least 1")
    # The configuration whose seed --check-fail changes (to prove the check works).
    perturbed = CONFIGS[3].name if args.check_fail else None

    print(
        f"Python {platform.python_version()} on {platform.machine()} "
        f"({platform.system()})"
    )
    print(
        f"M/M/1, lambda={ARRIVAL_RATE}, mu={SERVICE_RATE}, seed={SEED}, "
        f"{customers} customers, {reps} reps per configuration\n"
    )

    # Warm-up (imports, caches, the bus's per-class cache), not timed: one
    # untimed run of every configuration, so none of them is timed cold.
    for cfg in CONFIGS:
        time_once(cfg, min(customers, 2_000))

    # Interleave configurations round by round so that drift in machine load
    # affects all of them alike, and rotate the order each round so that no
    # configuration always runs first (or always right after the allocation-
    # heavy recording subscriber).
    times: dict[str, list[float]] = {c.name: [] for c in CONFIGS}
    results: dict[str, Result] = {}
    events_emitted = 0
    for rnd in range(reps):
        shift = rnd % len(CONFIGS)
        for cfg in CONFIGS[shift:] + CONFIGS[:shift]:
            seed = SEED + 1 if cfg.name == perturbed else SEED
            elapsed, result, recorded = time_once(cfg, customers, seed)
            times[cfg.name].append(elapsed)
            if cfg.name in results and results[cfg.name] != result:
                sys.exit(f"FAIL: {cfg.name} gave different results between repetitions")
            results[cfg.name] = result
            if recorded:
                events_emitted = recorded

    # Subscribing must not change the run: every configuration, the Semaphore
    # baseline included, must report the same served count, waits and clock.
    reference = results[CONFIGS[1].name]
    for cfg in CONFIGS:
        if results[cfg.name] != reference:
            sys.exit(
                f"FAIL: {cfg.name} changed the model: "
                f"{results[cfg.name]} != {reference}"
            )
    print(
        f"model result identical in all configurations: served={reference[0]}, "
        f"total wait={reference[1]:.3f}, final clock={reference[2]:.3f}"
    )
    print(
        f"events emitted per run (RunStarted, ResourceChanged, RunFinished): "
        f"{events_emitted}\n"
    )

    # The "vs" columns compare medians: a best-of time rewards one lucky run,
    # and on a shared machine luck is not evenly spread across configurations.
    med_ref = statistics.median(times[CONFIGS[1].name])
    med_base = statistics.median(times[CONFIGS[0].name])
    best_ref = min(times[CONFIGS[1].name])
    header = (
        f"{'configuration':<34} {'best s':>7} {'median s':>8} {'us/cust':>8} "
        f"{'Mev/s':>6} {'vs 1':>7} {'vs sem':>7} {'best vs 1':>9}"
    )
    print(header)
    print("-" * len(header))
    for cfg in CONFIGS:
        ts = times[cfg.name]
        best, median = min(ts), statistics.median(ts)
        # Events/second: events a subscriber would receive per second of run
        # (not meaningful for configurations that cannot emit).
        evps = (
            f"{events_emitted / median / 1e6:6.2f}" if cfg.use_resource else f"{'-':>6}"
        )
        print(
            f"{cfg.name:<34} {best:7.3f} {median:8.3f} "
            f"{median / customers * 1e6:8.2f} "
            f"{evps} {(median / med_ref - 1) * 100:+6.1f}% "
            f"{(median / med_base - 1) * 100:+6.1f}% "
            f"{(best / best_ref - 1) * 100:+8.1f}%"
        )
    print(
        "\nbest/median of the repetitions; 'vs 1' and 'vs sem' compare MEDIAN "
        "times ('best vs 1' compares best times, for reference); us/cust and "
        "Mev/s use the median; Mev/s = emitted events per second of the run."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
