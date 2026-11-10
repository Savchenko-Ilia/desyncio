"""Replications across seeds and confidence intervals (STA-04).

numpy and scipy are imported lazily inside the functions, so importing this
module (and ``desyncio``) does not import them.
"""

from collections.abc import Callable, Coroutine, Sequence
from typing import Any, overload

from desyncio.realtime import RealTimePacer
from desyncio.runners import run
from desyncio.trace import TraceSink

__all__ = ["confidence_interval", "replicate"]


@overload
def replicate[T](
    model: Callable[[], Coroutine[Any, Any, T]],
    n: int,
    *,
    seed: int = 0,
    until: None = None,
    speed: float | None = None,
    pacer: RealTimePacer | None = None,
    trace: TraceSink | None = None,
) -> list[T]: ...
@overload
def replicate[T](
    model: Callable[[], Coroutine[Any, Any, T]],
    n: int,
    *,
    seed: int = 0,
    until: float,
    speed: float | None = None,
    pacer: RealTimePacer | None = None,
    trace: TraceSink | None = None,
) -> list[T | None]: ...
def replicate[T](
    model: Callable[[], Coroutine[Any, Any, T]],
    n: int,
    *,
    seed: int = 0,
    until: float | None = None,
    speed: float | None = None,
    pacer: RealTimePacer | None = None,
    trace: TraceSink | None = None,
) -> list[T] | list[T | None]:
    """Run ``model`` ``n`` times and return the results in order.

    Run ``i`` is ``desyncio.run(model(), seed=seed + i, ...)``, so each run gets
    a fresh coroutine and its own seed, and the same arguments give the same
    list. ``until``, ``speed``, ``pacer`` and ``trace`` are passed to every run.
    A run stopped by ``until`` returns ``None``, so the list then holds ``None``
    for it; the overloads type the result as ``list[T | None]`` when ``until``
    is given and as ``list[T]`` when it is not, like ``desyncio.run``.

    A ``pacer`` passed in is the same object for all ``n`` runs and keeps its
    state across them: a pacer that ends one run paused starts the next run
    paused (CLN-03, D039).

    Raises:
        TypeError: ``n`` is not an int (``bool`` is rejected).
        ValueError: ``n < 1``.
    """
    if not isinstance(n, int) or isinstance(n, bool):
        raise TypeError(f"n must be an int, not {type(n).__name__}")
    if n < 1:
        raise ValueError(f"n must be >= 1, got {n}")
    if until is None:
        return [
            run(model(), seed=seed + i, speed=speed, pacer=pacer, trace=trace)
            for i in range(n)
        ]
    return [
        run(model(), until=until, seed=seed + i, speed=speed, pacer=pacer, trace=trace)
        for i in range(n)
    ]


def confidence_interval(
    values: Sequence[float], level: float = 0.95
) -> tuple[float, float]:
    """Return ``(mean, half_width)`` of the Student t interval for the mean.

    Uses ``scipy.stats.t.interval(level, n - 1, loc=mean, scale=sem)``; the
    interval is ``mean +/- half_width``. All values equal gives a half width of 0.0.

    Raises:
        ValueError: fewer than 2 values, a non-finite value, or ``level`` not in (0, 1).
    """
    import numpy as np
    from scipy import stats

    if not 0 < level < 1:
        raise ValueError(f"level must be in (0, 1), got {level}")
    arr = np.asarray(values, dtype=float)
    if arr.ndim != 1 or arr.size < 2:
        raise ValueError("need at least 2 values")
    if not np.all(np.isfinite(arr)):
        raise ValueError("values must be finite")
    mean = float(arr.mean())
    sem = float(stats.sem(arr))
    if sem == 0.0:  # scipy returns nan for a zero scale
        return mean, 0.0
    low, high = stats.t.interval(level, arr.size - 1, loc=mean, scale=sem)
    return mean, float(high - low) / 2
