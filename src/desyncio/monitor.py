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
* ``min``, ``max``, ``last`` are over the recorded values; empty is a
  ``ValueError``.

The integral is accumulated incrementally, so ``record`` and ``mean`` are O(1).
``min``/``max`` are tracked incrementally too. The samples are also kept, as a
read-only tuple view in ``samples`` (building it is O(n)).
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

    def __init__(self) -> None:
        self._samples: list[tuple[float, float]] = []
        self._area = 0.0  # integral of the series from the first record to the last
        self._min = math.inf
        self._max = -math.inf

    def __len__(self) -> int:
        return len(self._samples)

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
            self._area += v0 * (t - t0)
        self._samples.append((t, v))
        self._min = min(self._min, v)
        self._max = max(self._max, v)

    def mean(self, until: float | None = None) -> float:
        """Time-weighted mean over ``[first record, until]``.

        ``until=None`` means ``sim.now`` (inside a run only).

        Raises:
            ValueError: empty series, or ``until`` earlier than the last record,
                or ``until`` not finite.
            TypeError: ``until`` is not a real number.
            RuntimeError: ``until`` is None and called outside a run.
        """
        end = sim.now if until is None else _real(until, "until")
        if not self._samples:
            raise ValueError("mean of an empty TimeSeries")
        t_last, v_last = self._samples[-1]
        if end < t_last:
            raise ValueError(
                f"until={end!r} is earlier than the last record at {t_last!r}"
            )
        span = end - self._samples[0][0]
        if span == 0:
            return v_last
        return (self._area + v_last * (end - t_last)) / span

    def min(self) -> float:
        """Smallest recorded value (``ValueError`` if empty)."""
        self._require_samples("min")
        return self._min

    def max(self) -> float:
        """Largest recorded value (``ValueError`` if empty)."""
        self._require_samples("max")
        return self._max

    def last(self) -> float:
        """Most recently recorded value (``ValueError`` if empty)."""
        self._require_samples("last")
        return self._samples[-1][1]

    def _require_samples(self, what: str) -> None:
        if not self._samples:
            raise ValueError(f"{what} of an empty TimeSeries")
