"""Tests for the ``desyncio.sim`` facade (LOOP-05)."""

import asyncio
import warnings
from typing import Any

import pytest

import desyncio
from desyncio import sim


def test_exported() -> None:
    assert "sim" in desyncio.__all__
    assert desyncio.sim is sim


# --- now ---------------------------------------------------------------


def test_now_at_start_and_after_timeouts() -> None:
    async def main() -> list[float]:
        seen = [sim.now]
        await sim.timeout(2)
        seen.append(sim.now)
        await sim.timeout(0.5)
        seen.append(sim.now)
        return seen

    assert desyncio.run(main()) == [0.0, 2.0, 2.5]


def test_now_inside_spawned_tasks() -> None:
    async def child() -> float:
        await sim.timeout(3)
        return sim.now

    async def main() -> float:
        task = sim.spawn(child())
        await sim.timeout(1)
        assert sim.now == 1.0
        return await task

    assert desyncio.run(main()) == 3.0


# --- timeout -----------------------------------------------------------


def test_timeout_returns_none_and_optional_value() -> None:
    async def main() -> tuple[object, object]:
        return await sim.timeout(1), await sim.timeout(1, "v")

    assert desyncio.run(main()) == (None, "v")


def test_timeout_zero_yields_once() -> None:
    async def main() -> list[str]:
        log: list[str] = []

        async def other() -> None:
            log.append("other")

        sim.spawn(other())
        await sim.timeout(0)
        log.append("main")
        return log

    assert desyncio.run(main()) == ["other", "main"]


@pytest.mark.parametrize(
    ("dt", "exc"),
    [
        (-1, ValueError),
        (float("nan"), ValueError),
        (float("inf"), ValueError),
        (10**400, ValueError),
        (True, TypeError),
        ("1", TypeError),
        (None, TypeError),
    ],
)
def test_timeout_validation(dt: Any, exc: type[Exception]) -> None:
    async def main() -> None:
        with pytest.raises(exc):
            await sim.timeout(dt)

    desyncio.run(main())


# --- spawn -------------------------------------------------------------


def test_spawn_result_propagates() -> None:
    async def child() -> int:
        await sim.timeout(1)
        return 42

    async def main() -> tuple[bool, int]:
        task = sim.spawn(child())
        return isinstance(task, asyncio.Task), await task

    assert desyncio.run(main()) == (True, 42)


def test_spawn_exception_propagates() -> None:
    async def child() -> None:
        await sim.timeout(1)
        raise KeyError("boom")

    async def main() -> None:
        with pytest.raises(KeyError, match="boom"):
            await sim.spawn(child())

    desyncio.run(main())


def test_spawn_explicit_name() -> None:
    async def child() -> None:
        pass

    async def main() -> str:
        task = sim.spawn(child(), name="worker")
        await task
        return task.get_name()

    assert desyncio.run(main()) == "worker"


def _names_model() -> list[str]:
    async def child() -> None:
        await sim.timeout(1)

    async def main() -> list[str]:
        tasks = [sim.spawn(child()) for _ in range(3)]
        await asyncio.gather(*tasks)
        return [t.get_name() for t in tasks]

    return desyncio.run(main())


def test_default_names_deterministic_across_runs() -> None:
    # Burn some of asyncio's process-global Task-N counter in between.
    first = _names_model()
    asyncio.run(asyncio.sleep(0))
    second = _names_model()
    assert first == second == ["proc-1", "proc-2", "proc-3"]


def test_spawn_non_coroutine_raises_and_closes() -> None:
    async def child() -> None:
        pass

    async def main() -> None:
        with pytest.raises(TypeError):
            sim.spawn(child)  # type: ignore[arg-type]  # forgot to call
        with pytest.raises(TypeError):
            sim.spawn(None)  # type: ignore[arg-type]

    desyncio.run(main())


def test_spawn_outside_run_closes_coroutine() -> None:
    async def child() -> None:
        pass

    coro = child()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(RuntimeError, match=r"desyncio\.run"):
            sim.spawn(coro)
        del coro


# --- rng ---------------------------------------------------------------


def _draw(seed: int) -> list[float]:
    async def main() -> list[float]:
        a = sim.random("arrivals")
        first = [a.random(), a.random()]
        # Same name again continues the same stream.
        return [*first, sim.random("arrivals").random()]

    return desyncio.run(main(), seed=seed)


def test_rng_same_seed_same_sequence() -> None:
    assert _draw(7) == _draw(7)


def test_rng_different_seed_differs() -> None:
    assert _draw(7) != _draw(8)


def test_rng_continues_stream() -> None:
    async def main() -> bool:
        return sim.random("x") is sim.random("x")

    assert desyncio.run(main()) is True


# --- until -------------------------------------------------------------


def test_pending_spawned_cancelled_at_until() -> None:
    log: list[str] = []

    async def proc() -> None:
        try:
            await sim.timeout(100)
        except asyncio.CancelledError:
            log.append(f"cancelled@{sim.now}")
            raise

    async def main() -> None:
        sim.spawn(proc())
        await sim.timeout(1000)

    assert desyncio.run(main(), until=5) is None
    assert log == ["cancelled@5.0"]


# --- outside a VirtualTimeLoop ------------------------------------------


def test_outside_run_raises() -> None:
    with pytest.raises(RuntimeError, match=r"desyncio\.run"):
        _ = sim.now
    with pytest.raises(RuntimeError, match=r"desyncio\.run"):
        sim.random("x")


def test_plain_asyncio_loop_raises() -> None:
    async def main() -> None:
        with pytest.raises(RuntimeError, match=r"desyncio\.run"):
            _ = sim.now
        with pytest.raises(RuntimeError, match=r"desyncio\.run"):
            await sim.timeout(1)
        child = asyncio.sleep(0)
        with pytest.raises(RuntimeError, match=r"desyncio\.run"):
            sim.spawn(child)  # type: ignore[arg-type]
        with pytest.raises(RuntimeError, match=r"desyncio\.run"):
            sim.random("x")

    asyncio.run(main())
