"""``Tally``: statistics of individual observations (STA-01).

A ``Tally`` collects observations that are not tied to a time span, such as each
customer's waiting time. For a value that holds over time (a queue length) use
``TimeSeries`` instead. Like ``TimeSeries`` it knows nothing about the primitives::

    waits = Tally()
    waits.record(3.2)
    waits.record(1.5)
    waits.mean()                # 2.35
    waits.quantile(0.9)         # numpy.quantile, linear method
    counts, edges = waits.histogram(4)

Rules:

* ``record(value)`` never reads ``sim.now``, so it works inside or outside
  ``desyncio.run(...)``.
* Values are finite real numbers (``int`` or ``float``). ``bool`` and other
  types raise ``TypeError``; NaN and infinities raise ``ValueError``. A
  rejected record leaves the tally unchanged.
* Every statistic of an empty tally is a ``ValueError``. ``variance`` and
  ``stdev`` are the sample statistics (``ddof=1``) and need two values.
* The maths is numpy's (D048); results are plain ``float``/``int``, never numpy
  scalars. numpy is imported lazily inside the methods, so ``import desyncio``
  stays numpy-free.
"""

from collections.abc import Sequence
from typing import TYPE_CHECKING

from desyncio.monitor import _real

if TYPE_CHECKING:
    import numpy as np


class Tally:
    """Observations with summary statistics, quantiles and a histogram."""

    def __init__(self) -> None:
        self._values: list[float] = []

    def __len__(self) -> int:
        return len(self._values)

    @property
    def samples(self) -> tuple[float, ...]:
        """All observations, in order (a read-only copy)."""
        return tuple(self._values)

    def record(self, value: float) -> None:
        """Record one observation.

        Raises:
            TypeError: ``value`` is not an ``int``/``float`` (``bool`` rejected).
            ValueError: ``value`` is NaN or infinite. The tally is left unchanged.
        """
        self._values.append(_real(value, "value"))

    def mean(self) -> float:
        """Arithmetic mean (``ValueError`` if empty)."""
        import numpy as np

        return float(np.mean(self._array("mean")))

    def variance(self) -> float:
        """Sample variance, ``ddof=1`` (``ValueError`` below 2 values)."""
        import numpy as np

        return float(np.var(self._array("variance", 2), ddof=1))

    def stdev(self) -> float:
        """Sample standard deviation, ``ddof=1`` (``ValueError`` below 2 values)."""
        import numpy as np

        return float(np.std(self._array("stdev", 2), ddof=1))

    def min(self) -> float:
        """Smallest observation (``ValueError`` if empty)."""
        import numpy as np

        return float(np.min(self._array("min")))

    def max(self) -> float:
        """Largest observation (``ValueError`` if empty)."""
        import numpy as np

        return float(np.max(self._array("max")))

    def quantile(self, p: float | Sequence[float]) -> float | tuple[float, ...]:
        """``numpy.quantile`` with its default linear method.

        A scalar ``p`` gives a float, a sequence of ``p`` a tuple of floats::

            t.quantile(0.5)           # the median
            t.quantile([0.9, 0.99])   # (p90, p99)

        Raises:
            ValueError: empty tally, or ``p`` outside ``[0, 1]``.
        """
        import numpy as np

        values = self._array("quantile")
        if isinstance(p, Sequence):
            return tuple(float(q) for q in np.quantile(values, list(p)))
        return float(np.quantile(values, p))

    def histogram(
        self, bins: int | Sequence[float]
    ) -> tuple[tuple[int, ...], tuple[float, ...]]:
        """``numpy.histogram``, returned as ``(counts, edges)`` tuples.

        ``bins`` is a number of equal-width bins or a sequence of edges::

            counts, edges = t.histogram(4)
            counts, edges = t.histogram([0, 5, 10])

        Raises:
            ValueError: empty tally, or invalid ``bins``.
        """
        import numpy as np

        counts, edges = np.histogram(self._array("histogram"), bins=bins)
        return tuple(int(c) for c in counts), tuple(float(e) for e in edges)

    def _array(self, what: str, minimum: int = 1) -> "np.ndarray":
        import numpy as np

        if len(self._values) < minimum:
            raise ValueError(
                f"{what} needs at least {minimum} observation(s), "
                f"got {len(self._values)}"
            )
        return np.asarray(self._values, dtype=float)
