"""Virtual clock: the simulation's notion of "now".

Time is a plain unitless ``float``, *seconds by convention*. That lines up with
``asyncio.sleep(seconds)`` and ``loop.time()``, so ``await asyncio.sleep(2)``
means "2 simulated seconds" without any conversion.

This module is deliberately independent of asyncio. The virtual event loop
(a later feature) will own one :class:`VirtualClock` and return ``clock.now``
from its ``time()`` method; when nothing is ready to run it will jump the
clock to the next timer with :meth:`VirtualClock.advance_to`.
"""

from desyncio._validate import to_float


class VirtualClock:
    """A monotonic, non-decreasing simulation clock.

    Time only moves when someone calls :meth:`advance_to`; it never follows
    the wall clock. Read the current time with the :attr:`now` property.
    """

    def __init__(self, start: float = 0.0) -> None:
        """Create a clock at ``start`` (default 0.0).

        Raises:
            TypeError: if ``start`` is not an ``int`` or ``float``.
            ValueError: if ``start`` is NaN or infinite.
        """
        self._now = self._check_finite(start, "start")

    @property
    def now(self) -> float:
        """The current simulation time (seconds by convention)."""
        return self._now

    def advance_to(self, t: float) -> None:
        """Move the clock forward to time ``t``.

        ``t`` equal to the current time is allowed and does nothing: many
        events share one timestamp, so the loop will often "advance" to where
        it already is.

        Raises:
            TypeError: if ``t`` is not an ``int`` or ``float``.
            ValueError: if ``t`` is NaN/infinite, or earlier than the current
                time (time never runs backwards).
        """
        target = self._check_finite(t, "advance_to")
        if target < self._now:
            raise ValueError(
                f"cannot move clock backwards: now={self._now!r}, requested={target!r}"
            )
        self._now = target

    @staticmethod
    def _check_finite(value: float, what: str) -> float:
        """Validate a real, finite number and return it as a float.

        ``-0.0`` is normalised to ``0.0``.

        Raises:
            TypeError: if ``value`` is not an ``int`` or ``float`` (``bool``
                is rejected too).
            ValueError: if ``value`` is NaN, infinite, or an int too large
                to represent as a float.
        """
        return to_float(
            value,
            type_error=f"{what} must be a real number (int or float), got {{type}}",
            overflow_error=(
                f"{what} must be a finite number, got an int too large for a float"
            ),
            finite_error=f"{what} must be a finite number, got {{value!r}}",
            normalise_zero=True,
        )
