"""``TimeSeries``: time-weighted statistics, an observer outside the core (MON-01).

Principle 4: the core exposes hooks, statistics are consumers. ``TimeSeries``
imports nothing from the primitives and knows nothing about them. You wire it
up with the ``monitor=`` hook of D021/D023 v2::

    busy = TimeSeries()
    server = Resource(1, monitor=lambda r: busy.record(r.count))
    busy.record(0)            # monitors fire only on a change: record t=0 yourself
    ...
    busy.mean()               # inside the run: up to sim.now
    busy.mean(until=end)      # after the run: pass the end time explicitly

Rules (D028):

* ``record(value)`` stamps the value with ``sim.now`` and so only works inside
  ``desyncio.run(...)`` (``RuntimeError`` otherwise, as for ``sim.now``).
* Values are finite real numbers (``int`` or ``float``). ``bool``, other types
  raise ``TypeError``; NaN and infinities raise ``ValueError``. A rejected
  record leaves the series unchanged.
* Time never goes backwards (D029): ``record`` at a virtual time earlier than
  the last record raises ``ValueError`` and leaves the series unchanged. A
  series belongs to one run; a second ``desyncio.run(...)`` restarts the clock
  at 0, so use a fresh ``TimeSeries`` per run.
* ``mean(until=None)`` is the time-weighted mean over ``[first record, until]``;
  each value holds until the next record, the last one until ``until``.
  ``until=None`` means ``sim.now``. ``until`` earlier than the last record is a
  ``ValueError``; an empty series is a ``ValueError``; a zero-length span
  returns the last value.
* Several records at the same virtual time: the last one wins for the mean
  (earlier ones have zero weight), but all of them count for ``min``/``max``.
* ``time_fractions``, ``quantile`` and ``variance`` (STA-02) take the same
  ``until`` and raise the same errors as ``mean``. A value held for zero time
  (overwritten at the same instant) gets no entry in ``time_fractions`` and no
  weight elsewhere. A zero-length span counts as the last value held for all
  of the time: ``{last: 1.0}``, ``quantile`` = last, ``variance`` = 0.0.
  ``quantile(p)`` is ``numpy.quantile(..., method="inverted_cdf")`` with the
  durations as weights; ``variance`` is the population (time-weighted) one.
  numpy is imported lazily inside these methods.
* ``min``, ``max``, ``last`` are over the recorded values; empty is a
  ``ValueError``.

Warm-up (STA-03). ``TimeSeries(warmup=t)`` (a finite real >= 0, default 0)
removes the initial transient from the statistics:

* Records before ``t`` set the current value but carry no weight. The
  statistics window starts at ``t`` with the value that holds then (the last
  record at or before ``t``); ``mean``, ``min``, ``max``, ``time_fractions``,
  ``quantile`` and ``variance`` see only that window. ``min``/``max`` include
  the value that holds at ``t`` only if it is held for some time (a record
  exactly at ``t`` replaces it). ``samples``, ``len`` and ``last`` still
  cover every record.
* If no record happened before ``t``, the window starts at the first record
  (which is at or after ``t``).
* If every record so far is before ``t``, the last value holds from ``t``: the
  window is ``[t, until]``, and ``min``/``max`` are that value.
* ``until`` (or ``sim.now``) earlier than ``t`` is a ``ValueError``.
* ``warmup=0`` is the behaviour described above.

The integral is accumulated incrementally from the start of the window (the
segment that straddles ``t`` is cut at ``t``), so ``record`` and ``mean`` are
O(1). ``min``/``max`` are tracked incrementally too. The samples are also kept,
as a read-only tuple view in ``samples`` (building it is O(n)).
"""

import math

from desyncio.sim import sim


def _real(value: float, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{what} must be a real number, got {type(value).__name__}")
    try:
        result = float(value)
    except OverflowError:
        raise ValueError(f"{what} must be finite, got an int too large") from None
    if not math.isfinite(result):
        raise ValueError(f"{what} must be finite, got {result!r}")
    return result


class TimeSeries:
    """Values stamped with virtual time, with time-weighted statistics."""

    def __init__(self, warmup: float = 0.0) -> None:
        warmup = _real(warmup, "warmup")
        if warmup < 0:
            raise ValueError(f"warmup must be >= 0, got {warmup!r}")
        self._warmup = warmup
        self._samples: list[tuple[float, float]] = []
        # Start of the statistics window; None until a record at or after warmup.
        self._start: float | None = None
        self._area = 0.0  # integral over the window, from its start to the last record
        self._min = math.inf
        self._max = -math.inf

    def __len__(self) -> int:
        return len(self._samples)

    @property
    def warmup(self) -> float:
        """The warm-up time: records before it carry no weight."""
        return self._warmup

    @property
    def samples(self) -> tuple[tuple[float, float], ...]:
        """All records as ``(time, value)`` pairs, in order (a read-only copy)."""
        return tuple(self._samples)

    def record(self, value: float) -> None:
        """Record ``value`` at ``sim.now``.

        Raises:
            TypeError: ``value`` is not an ``int``/``float`` (``bool`` rejected).
            ValueError: ``value`` is NaN or infinite, or ``sim.now`` is
                earlier than the last record (e.g. the series is reused in a
                second run, D029). The series is left unchanged.
            RuntimeError: outside ``desyncio.run(...)``.
        """
        v = _real(value, "value")
        t = sim.now
        if self._samples:
            t0, v0 = self._samples[-1]
            if t < t0:
                raise ValueError(
                    f"record at t={t!r} is earlier than the last record at "
                    f"{t0!r}; a TimeSeries belongs to one run"
                )
            if self._start is not None:
                self._area += v0 * (t - t0)
            elif t >= self._warmup:
                # The first record at or after warm-up: the window opens at
                # warm-up with v0, the value that held then. A segment that
                # straddles warm-up is cut there.
                self._start = self._warmup
                self._area = v0 * (t - self._warmup)
                if t > self._warmup:
                    self._min = self._max = v0
        elif t >= self._warmup:
            self._start = t
        self._samples.append((t, v))
        if self._start is not None:
            self._min = min(self._min, v)
            self._max = max(self._max, v)

    def mean(self, until: float | None = None) -> float:
        """Time-weighted mean over ``[window start, until]``.

        ``until=None`` means ``sim.now`` (inside a run only).

        Raises:
            ValueError: empty series, or ``until`` earlier than the last record
                or than the warm-up, or ``until`` not finite.
            TypeError: ``until`` is not a real number.
            RuntimeError: ``until`` is None and called outside a run.
        """
        start, end = self._window(until, "mean")
        t_last, v_last = self._samples[-1]
        span = end - start
        if span == 0:
            return v_last
        # Before the window opens there is no area; the last value holds from start.
        area = 0.0 if self._start is None else self._area
        return (area + v_last * (end - max(t_last, start))) / span

    def _window(self, until: float | None, what: str) -> tuple[float, float]:
        """``(start, end)`` of the statistics window, after the shared checks."""
        end = sim.now if until is None else _real(until, "until")
        if not self._samples:
            raise ValueError(f"{what} of an empty TimeSeries")
        t_last = self._samples[-1][0]
        if end < t_last:
            raise ValueError(
                f"until={end!r} is earlier than the last record at {t_last!r}"
            )
        start = self._warmup if self._start is None else self._start
        if end < start:
            raise ValueError(f"until={end!r} is earlier than the warm-up at {start!r}")
        return start, end

    def _weighted(
        self, until: float | None, what: str
    ) -> tuple[list[float], list[float]]:
        """``(values, durations)`` over the window, durations > 0."""
        start, end = self._window(until, what)
        if end == start:
            return [self._samples[-1][1]], [1.0]
        times = [t for t, _ in self._samples[1:]] + [end]
        pairs = [
            (v, t1 - max(t0, start))
            for (t0, v), t1 in zip(self._samples, times, strict=True)
            if t1 > max(t0, start)
        ]
        return [v for v, _ in pairs], [d for _, d in pairs]

    def time_fractions(self, until: float | None = None) -> dict[float, float]:
        """Share of time spent at each value over ``[window start, until]``.

        Values held for zero time have no entry. The fractions sum to 1.
        Errors as for ``mean``.
        """
        values, durations = self._weighted(until, "time_fractions")
        per_value: dict[float, list[float]] = {}
        for v, d in zip(values, durations, strict=True):
            per_value.setdefault(v, []).append(d)
        total = math.fsum(durations)
        return {v: math.fsum(ds) / total for v, ds in per_value.items()}

    def quantile(self, p: float, until: float | None = None) -> float:
        """Time-weighted ``p``-quantile (``numpy.quantile``, ``inverted_cdf``).

        ``p`` must be in ``[0, 1]`` (``ValueError``). Other errors as for ``mean``.
        """
        import numpy as np

        p = _real(p, "p")
        if not 0.0 <= p <= 1.0:
            raise ValueError(f"p must be in [0, 1], got {p!r}")
        values, durations = self._weighted(until, "quantile")
        return float(np.quantile(values, p, weights=durations, method="inverted_cdf"))

    def variance(self, until: float | None = None) -> float:
        """Time-weighted population variance over ``[window start, until]``.

        Errors as for ``mean``; a zero-length span gives 0.0.
        """
        import numpy as np

        values, durations = self._weighted(until, "variance")
        mean = np.average(values, weights=durations)
        return float(np.average((np.asarray(values) - mean) ** 2, weights=durations))

    def min(self) -> float:
        """Smallest value in the statistics window (``ValueError`` if empty)."""
        self._require_samples("min")
        return self._min if self._start is not None else self._samples[-1][1]

    def max(self) -> float:
        """Largest value in the statistics window (``ValueError`` if empty)."""
        self._require_samples("max")
        return self._max if self._start is not None else self._samples[-1][1]

    def last(self) -> float:
        """Most recently recorded value (``ValueError`` if empty)."""
        self._require_samples("last")
        return self._samples[-1][1]

    def _require_samples(self, what: str) -> None:
        if not self._samples:
            raise ValueError(f"{what} of an empty TimeSeries")
