"""Tests for seeded random streams (LOOP-08, RND-01)."""

import random
import subprocess
import sys

import pytest

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


def test_cross_process_stable() -> None:
    code = (
        "from desyncio.randomness import RandomStreams;"
        "r=RandomStreams(42).stream('arrivals');"
        "print([r.random() for _ in range(5)])"
    )
    outs = []
    for hashseed in ("1", "12345"):
        res = subprocess.run(
            [sys.executable, "-c", code],
            env={"PYTHONHASHSEED": hashseed},
            capture_output=True,
            text=True,
            check=True,
        )
        outs.append(res.stdout)
    assert outs[0] == outs[1]
    assert outs[0].strip() == str(_draw(RandomStreams(42), "arrivals"))


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


@pytest.mark.parametrize("seed", [True, False, 1.5, "1", None, -1])
def test_bad_seed_rejected(seed: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        RandomStreams(seed)  # type: ignore[arg-type]


@pytest.mark.parametrize("name", ["", 1, None, b"a"])
def test_bad_name_rejected(name: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        RandomStreams(0).stream(name)  # type: ignore[arg-type]
