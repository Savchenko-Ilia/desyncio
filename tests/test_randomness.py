"""Tests for seeded random streams (LOOP-08, RND-01)."""

import asyncio
import contextvars
import os
import random
import subprocess
import sys

import pytest

import desyncio
from desyncio import sim
from desyncio.randomness import RandomStreams

GOLDEN_SEED = 0
GOLDEN_VALUES = [0.38760878436702273, 0.7513180410827758, 0.9621625913013577]


def _draw(streams: RandomStreams, name: str, n: int = 5) -> list[float]:
    rng = streams.stream(name)
    return [rng.random() for _ in range(n)]


def test_same_seed_and_name_same_sequence() -> None:
    assert _draw(RandomStreams(42), "arrivals") == _draw(RandomStreams(42), "arrivals")


def test_different_name_or_seed_differs() -> None:
    base = _draw(RandomStreams(42), "arrivals")
    assert base != _draw(RandomStreams(42), "service")
    assert base != _draw(RandomStreams(43), "arrivals")


def test_streams_are_independent() -> None:
    expected_b = _draw(RandomStreams(7), "b")
    other = RandomStreams(7)
    for _ in range(100):
        other.stream("a").random()
    assert _draw(other, "b") == expected_b


def test_stream_is_cached_and_continues() -> None:
    reg = RandomStreams(1)
    assert reg.stream("x") is reg.stream("x")
    first = reg.stream("x").random()
    second = reg.stream("x").random()
    fresh = RandomStreams(1).stream("x")
    assert [first, second] == [fresh.random(), fresh.random()]


def test_golden_values() -> None:
    assert _draw(RandomStreams(GOLDEN_SEED), "arrivals", 3) == GOLDEN_VALUES
    assert _draw(RandomStreams(42), "service", 3) == [
        0.09437616497483836,
        0.09668817127320806,
        0.5960130800128274,
    ]


def test_stream_is_stdlib_string_seeded() -> None:
    expected = random.Random("5:x")
    got = RandomStreams(5).stream("x")
    assert [got.random() for _ in range(3)] == [expected.random() for _ in range(3)]


def test_seed_name_boundary_is_unambiguous() -> None:
    assert _draw(RandomStreams(1), "23") != _draw(RandomStreams(12), "3")
    assert _draw(RandomStreams(1), ":2") != _draw(RandomStreams(1), "2")


@pytest.mark.parametrize("seed", [True, 1.5, "1", None, -1])
def test_bad_seed_rejected(seed: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        RandomStreams(seed)  # type: ignore[arg-type]


@pytest.mark.parametrize("name", ["", 1, None, b"a"])
def test_bad_name_rejected(name: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        RandomStreams(0).stream(name)  # type: ignore[arg-type]


def _np_draw(streams: RandomStreams, name: str) -> list[float]:
    draws: list[float] = streams.np_stream(name).random(3).tolist()
    return draws


def test_np_stream_cached_independent_and_seeded() -> None:
    reg = RandomStreams(3)
    assert reg.np_stream("x") is reg.np_stream("x")
    assert reg.np_stream("x") is not reg.np_stream("y")
    assert _np_draw(RandomStreams(3), "x") == _np_draw(RandomStreams(3), "x")
    assert _np_draw(RandomStreams(3), "x") != _np_draw(RandomStreams(4), "x")
    assert _np_draw(RandomStreams(3), "x") != _np_draw(RandomStreams(3), "y")
    assert _np_draw(RandomStreams(3), "x") != _draw(RandomStreams(3), "x", 3)


def test_np_golden_values() -> None:
    assert _np_draw(RandomStreams(0), "x") == [
        0.22945626321290657,
        0.6944187036983647,
        0.6299077727128548,
    ]
    assert _np_draw(RandomStreams(42), "service") == [
        0.40560497668946327,
        0.09795317063024866,
        0.8123967060912538,
    ]


@pytest.mark.parametrize("name", ["", 1, None, b"a"])
def test_np_bad_name_rejected(name: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        RandomStreams(0).np_stream(name)  # type: ignore[arg-type]


def test_np_random_in_run_is_cached_and_errors_outside_or_fresh_context() -> None:
    with pytest.raises(RuntimeError):
        sim.np_random("x")

    async def child() -> None:
        sim.np_random("x")

    async def main() -> bool:
        loop = asyncio.get_running_loop()
        task = loop.create_task(child(), context=contextvars.Context())
        with pytest.raises(RuntimeError, match="fresh contextvars.Context"):
            await task
        return sim.np_random("x") is sim.np_random("x")

    assert desyncio.run(main(), seed=1) is True


def test_np_stream_same_draws_in_another_process() -> None:
    code = (
        "import desyncio, sys\n"
        "from desyncio import sim\n"
        "async def main():\n"
        "    return sim.np_random('x').random(3).tolist()\n"
        "print(desyncio.run(main(), seed=9))\n"
    )
    outs = [
        subprocess.run(
            [sys.executable, "-c", code],
            env={**os.environ, "PYTHONHASHSEED": h},
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for h in ("0", "777")
    ]
    assert outs[0] == outs[1]
    assert outs[0].strip() == str(RandomStreams(9).np_stream("x").random(3).tolist())
