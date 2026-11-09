import asyncio
import random
from collections.abc import Coroutine
from typing import Any

import pytest

from desyncio import run, sim
from desyncio.experiment import confidence_interval, replicate


def _model() -> Coroutine[Any, Any, float]:
    async def main() -> float:
        await asyncio.sleep(1)
        return sim.random("x").random()

    return main()


def test_replicate_is_deterministic_and_seeds_are_distinct() -> None:
    a = replicate(_model, 5, seed=3)
    assert a == replicate(_model, 5, seed=3)
    assert a != replicate(_model, 5, seed=4)
    assert len(set(a)) == 5
    assert a[1:] == replicate(_model, 4, seed=4)  # run i uses seed + i
    assert a == [run(_model(), seed=3 + i) for i in range(5)]


def test_replicate_passes_until_through() -> None:
    assert replicate(_model, 2, until=0.5) == [None, None]


def test_replicate_validation() -> None:
    for bad in (0, -1):
        with pytest.raises(ValueError):
            replicate(_model, bad)
    for bad_t in (True, 2.0, "2"):
        with pytest.raises(TypeError):
            replicate(_model, bad_t)  # type: ignore[arg-type]


def test_confidence_interval_hand_computed() -> None:
    # mean 3, sd sqrt(2.5), sem sqrt(0.5); t_{0.975,4} = 2.7764451
    mean, hw = confidence_interval([1, 2, 3, 4, 5])
    assert type(mean) is float and type(hw) is float
    assert mean == 3.0
    assert hw == pytest.approx(2.7764451 * 0.5**0.5, rel=1e-6)


def test_confidence_interval_constant_and_level() -> None:
    assert confidence_interval([2.0, 2.0, 2.0]) == (2.0, 0.0)
    _, narrow = confidence_interval([1, 2, 3, 4, 5], level=0.5)
    _, wide = confidence_interval([1, 2, 3, 4, 5], level=0.99)
    assert narrow < wide


def test_confidence_interval_validation() -> None:
    with pytest.raises(ValueError):
        confidence_interval([1.0])
    for lvl in (0.0, 1.0, -0.1, 1.5):
        with pytest.raises(ValueError):
            confidence_interval([1.0, 2.0], level=lvl)
    for bad in (float("nan"), float("inf")):
        with pytest.raises(ValueError):
            confidence_interval([1.0, bad, 2.0])


def test_confidence_interval_coverage() -> None:
    rng = random.Random(12345)
    hits = 0
    for _ in range(200):
        mean, hw = confidence_interval([rng.gauss(10, 2) for _ in range(10)])
        hits += abs(mean - 10) <= hw
    # Binomial(200, 0.95): sd = sqrt(200*.95*.05) = 3.1 hits; band is about +-3.5 sd.
    assert 0.95 * 200 - 11 <= hits <= 200
