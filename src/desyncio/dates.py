"""``Calendar``: map simulation time to wall-clock dates and back (CAL-01).

Sim time stays a unitless float (D004). A ``Calendar`` is a pure converter on
top of it: ``start`` is the aware ``datetime`` that sim time 0 stands for and
``unit`` is how long one sim time unit is (default one second)::

    cal = Calendar(datetime(2026, 3, 29, 1, 30, tzinfo=ZoneInfo("Europe/Berlin")))
    cal.to_datetime(3600)     # 2026-03-29 03:30 CEST: the clocks skipped 02:00-03:00
    cal.to_sim(some_dt)       # float sim time of any aware datetime, any zone
    cal.now()                 # to_datetime(sim.now), inside a run

Why conversion goes through UTC: local time is not a line. At a DST change a
wall-clock hour is skipped or repeated, so counting sim time in local time
would make it jump or repeat. Sim time is therefore always elapsed real time
since ``start``: both directions are done in UTC and only the result is shown
in ``start``'s zone. DST changes the displayed wall clock, never the sim clock.

Precision: ``datetime`` has microsecond resolution. ``to_datetime`` rounds to
the nearest microsecond (as ``timedelta * float`` does). ``to_sim`` is exact
to that resolution.

Only the stdlib (``datetime``, and ``zoneinfo`` for the caller's zones) is used.
"""

from datetime import UTC, datetime, timedelta, tzinfo

from desyncio._validate import to_float
from desyncio.sim import sim


def _check_aware(value: object, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime, got {type(value).__name__}")
    if value.utcoffset() is None:
        raise TypeError(f"{name} must be timezone-aware (naive datetime given)")
    return value


class Calendar:
    """Converts sim time to aware datetimes and back.

    Args:
        start: the aware ``datetime`` of sim time 0. Its ``tzinfo`` is the zone
            of every datetime ``to_datetime``/``now`` return.
        unit: the length of one sim time unit; a positive ``timedelta``.

    Raises:
        TypeError: ``start`` is not a ``datetime`` or is naive; ``unit`` is not
            a ``timedelta``.
        ValueError: ``unit`` is zero or negative.

    Example::

        cal = Calendar(datetime(2026, 1, 1, tzinfo=UTC), unit=timedelta(minutes=1))
        cal.to_datetime(90)   # 2026-01-01 01:30 UTC
        cal.to_sim(datetime(2026, 1, 1, 2, tzinfo=UTC))   # 120.0
    """

    def __init__(
        self, start: datetime, *, unit: timedelta = timedelta(seconds=1)
    ) -> None:
        _check_aware(start, "start")
        if not isinstance(unit, timedelta):
            raise TypeError(f"unit must be a timedelta, got {type(unit).__name__}")
        if unit <= timedelta(0):
            raise ValueError(f"unit must be positive, got {unit!r}")
        self._start = start
        self._start_utc = start.astimezone(UTC)
        self._unit = unit

    @property
    def start(self) -> datetime:
        """The aware datetime of sim time 0."""
        return self._start

    @property
    def unit(self) -> timedelta:
        """The length of one sim time unit."""
        return self._unit

    @property
    def tz(self) -> tzinfo:
        """``start``'s ``tzinfo``: the zone of the datetimes returned."""
        tz = self._start.tzinfo
        assert tz is not None  # start is aware
        return tz

    def to_datetime(self, t: float) -> datetime:
        """The aware datetime (in ``start``'s zone) at sim time ``t``.

        Computed as ``start.astimezone(UTC) + t * unit``, then converted to
        ``start``'s zone, so DST never affects the elapsed time. ``t`` may be
        negative: that gives dates before ``start``. The result is rounded to
        the microsecond.

        Raises:
            TypeError: ``t`` is not a real number (``bool`` is rejected).
            ValueError: ``t`` is NaN or infinite, or the date is out of range.
        """
        value = to_float(
            t,
            type_error="t must be a real number, got {type}",
            overflow_error="t must be finite, got an int too large",
            finite_error="t must be finite, got {value!r}",
        )
        try:
            utc = self._start_utc + value * self._unit
        except OverflowError:
            raise ValueError(f"t={t!r} is out of the range of datetime") from None
        return utc.astimezone(self._start.tzinfo)

    def to_sim(self, dt: datetime) -> float:
        """The sim time of the aware datetime ``dt`` (any zone).

        ``(dt - start) / unit``, with both sides converted to UTC first. (Aware
        subtraction already does that for different zones, but not when both
        share one ``tzinfo``, where it compares wall clocks; hence the explicit
        conversion.) Dates before ``start`` give negative values.

        Raises:
            TypeError: ``dt`` is not a ``datetime`` or is naive.
        """
        _check_aware(dt, "dt")
        return (dt.astimezone(UTC) - self._start_utc) / self._unit

    def now(self) -> datetime:
        """``to_datetime(sim.now)``.

        Raises:
            RuntimeError: outside ``desyncio.run(...)``, as ``sim.now``.
        """
        return self.to_datetime(sim.now)
