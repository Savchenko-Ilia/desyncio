"""Tests for seeded RNG streams (LOOP-08)."""

import subprocess
import sys

import pytest
from hypothesis import given
from hypothesis import strategies as st

from desyncio.rng import RngStreams, derive_seed

GOLDEN_SEED = 0
GOLDEN_VALUES = [0.6233832038318391, 0.482652055381812, 0.06127641211175505]
GOLDEN_DERIVED = (
    92060955042600997910570702524198234861780185678780989879386187341818659012418
)


def _draw(rng_streams: RngStreams, name: str, n: int = 5) -> list[float]:
    rng = rng_streams.stream(name)
    return [rng.random() for _ in range(n)]


def test_same_seed_and_name_same_sequence() -> None:
    assert _draw(RngStreams(42), "arrivals") == _draw(RngStreams(42), "arrivals")


def test_different_name_or_seed_differs() -> None:
    base = _draw(RngStreams(42), "arrivals")
    assert base != _draw(RngStreams(42), "service")
    assert base != _draw(RngStreams(43), "arrivals")


def test_streams_are_independent() -> None:
    expected_b = _draw(RngStreams(7), "b")
    other = RngStreams(7)
    for _ in range(100):
        other.stream("a").random()
    assert _draw(other, "b") == expected_b


def test_stream_is_cached_and_continues() -> None:
    reg = RngStreams(1)
    assert reg.stream("x") is reg.stream("x")
    first = reg.stream("x").random()
    second = reg.stream("x").random()
    fresh = RngStreams(1).stream("x")
    assert [first, second] == [fresh.random(), fresh.random()]


def test_cross_process_stable() -> None:
    code = (
        "from desyncio.rng import RngStreams;"
        "r=RngStreams(42).stream('arrivals');"
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
    assert outs[0].strip() == str(_draw(RngStreams(42), "arrivals"))


def test_golden_values() -> None:
    assert derive_seed(GOLDEN_SEED, "arrivals") == GOLDEN_DERIVED
    assert _draw(RngStreams(GOLDEN_SEED), "arrivals", 3) == GOLDEN_VALUES


def test_encoding_has_no_collisions() -> None:
    assert derive_seed(1, "23") != derive_seed(12, "3")


@given(st.integers(min_value=0), st.text(min_size=1))
def test_derive_seed_deterministic(seed: int, name: str) -> None:
    assert derive_seed(seed, name) == derive_seed(seed, name)


@pytest.mark.parametrize("seed", [True, False, 1.5, "1", None, -1])
def test_bad_seed_rejected(seed: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        derive_seed(seed, "a")  # type: ignore[arg-type]
    with pytest.raises((TypeError, ValueError)):
        RngStreams(seed)  # type: ignore[arg-type]


@pytest.mark.parametrize("name", ["", 1, None, b"a"])
def test_bad_name_rejected(name: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        derive_seed(0, name)  # type: ignore[arg-type]
    with pytest.raises((TypeError, ValueError)):
        RngStreams(0).stream(name)  # type: ignore[arg-type]
