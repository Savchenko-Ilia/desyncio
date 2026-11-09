"""``Shift`` and ``Schedule``: a weekly pattern of capacity over sim time (CAL-02).

A ``Shift`` says "on these weekdays, from this wall-clock time to that one,
``capacity`` units are available". A ``Schedule`` puts shifts on a ``Calendar``
and answers two questions in sim time: ``capacity_at(t)`` and ``changes(after)``
(the endless stream of ``(time, capacity)`` changes)::

    cal = Calendar(datetime(2026, 1, 5, tzinfo=ZoneInfo("Europe/Berlin")))
    shift = Shift(range(5), time(9), time(17), 2)
    sched = Schedule(cal, [shift], closed=[date(2026, 1, 6)])
    sched.capacity_at(10 * 3600)      # 2: Monday 10:00
    next(sched.changes(0))            # (32400.0, 2): Monday 09:00

Rules:

* Weekdays are ``date.weekday()`` numbers: 0 = Monday ... 6 = Sunday. A shift
  belongs to the weekday it *starts* on.
* A shift runs on ``[start, end)``. ``end <= start`` means it ends the next day
  (overnight); ``end == start`` is 24 hours.
* Overlapping shifts add their capacities (D051 item 3).
* ``closed`` dates: no shift *starts* on them. An overnight shift that started
  the day before still finishes (D051 item 4).
* Boundaries are local wall-clock times in ``calendar.tz`` (the zone of
  ``calendar.start``), turned into sim time with ``calendar.to_sim``. DST
  (D051 item 5):

  - a boundary at a **non-existent** local time (spring forward, e.g. 02:30 on
    2026-03-29 in Europe/Berlin) moves to the **first valid instant after the
    gap** (03:00 CEST). A shift whose start and end both fall in the gap is
    empty and never runs.
  - an **ambiguous** local time (fall back, e.g. 02:30 on 2026-10-25) uses the
    **first occurrence** (``fold=0``, the summer-time one).

Pure code: no asyncio, no running simulation needed. Only the stdlib is used.
"""

import heapq
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime, time, timedelta, tzinfo

from desyncio.dates import Calendar

_DAY = timedelta(days=1)
_MICROSECOND = timedelta(microseconds=1)
_IDLE_LIMIT = timedelta(days=400)


def _local_instant(day: date, at: time, tz: tzinfo) -> datetime:
    """The UTC instant of the wall-clock time ``day`` ``at`` in ``tz``.

    Ambiguous (fall back): the first occurrence (``fold=0``). Non-existent
    (spring forward): the first valid instant after the gap.
    """
    naive = datetime.combine(day, at)
    utc = naive.replace(tzinfo=tz).astimezone(UTC)  # fold=0
    # A non-existent wall time does not survive a trip through UTC.
    if utc.astimezone(tz).replace(tzinfo=None) == naive:
        return utc
    # In a gap fold=0 reads the time with the offset from before the gap, fold=1
    # with the one after it. So `utc` (fold=0) is after the transition and `lo`
    # (fold=1) before it. Find the transition: the first instant whose offset
    # differs from the one at `lo`.
    lo = naive.replace(tzinfo=tz, fold=1).astimezone(UTC)
    before = lo.astimezone(tz).utcoffset()
    hi = utc
    while hi - lo > _MICROSECOND:
        mid = lo + (hi - lo) // 2
        if mid.astimezone(tz).utcoffset() == before:
            lo = mid
        else:
            hi = mid
    return hi


class Shift:
    """Capacity on some weekdays between two wall-clock times.

    Args:
        days: an iterable of weekday numbers 0..6 (0 = Monday, as
            ``date.weekday()``); stored as a ``frozenset``; not empty.
        start: naive ``datetime.time`` the shift starts at (inclusive).
        end: naive ``datetime.time`` it ends at (exclusive). ``end <= start``
            means the next day; ``end == start`` is 24 hours.
        capacity: ``int`` >= 1 (``bool`` is rejected).

    ``start``/``end`` are wall-clock times in the calendar's zone, so they must
    be naive: a time with a ``tzinfo`` raises ``TypeError``.

    Raises:
        TypeError: wrong type, a non-int weekday (``bool`` too), an aware time.
        ValueError: ``days`` empty or a weekday outside 0..6; ``capacity`` < 1.
    """

    __slots__ = ("_capacity", "_days", "_end", "_start")

    def __init__(
        self, days: Iterable[int], start: time, end: time, capacity: int
    ) -> None:
        try:
            items = list(days)
        except TypeError:
            raise TypeError(
                f"days must be an iterable of ints, got {type(days).__name__}"
            ) from None
        for d in items:
            if not isinstance(d, int) or isinstance(d, bool):
                raise TypeError(f"days must contain ints, got {d!r}")
            if not 0 <= d <= 6:
                raise ValueError(f"weekdays are 0..6 (0 = Monday), got {d}")
        if not items:
            raise ValueError("days must not be empty")
        for name, value in (("start", start), ("end", end)):
            if not isinstance(value, time):
                raise TypeError(f"{name} must be a time, got {type(value).__name__}")
            if value.tzinfo is not None:
                raise TypeError(
                    f"{name} must be naive: the zone is the calendar's, got {value!r}"
                )
        if not isinstance(capacity, int) or isinstance(capacity, bool):
            raise TypeError(f"capacity must be an int, got {type(capacity).__name__}")
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        self._days = frozenset(items)
        self._start = start
        self._end = end
        self._capacity = capacity

    @property
    def days(self) -> frozenset[int]:
        """The weekdays the shift starts on."""
        return self._days

    @property
    def start(self) -> time:
        """The wall-clock start time."""
        return self._start

    @property
    def end(self) -> time:
        """The wall-clock end time (next day if ``end <= start``)."""
        return self._end

    @property
    def capacity(self) -> int:
        """The units available while the shift runs."""
        return self._capacity

    def _key(self) -> tuple[frozenset[int], time, time, int]:
        return (self._days, self._start, self._end, self._capacity)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Shift) and self._key() == other._key()

    def __hash__(self) -> int:
        return hash(self._key())

    def __repr__(self) -> str:
        return (
            f"Shift(days={sorted(self._days)}, start={self._start!r}, "
            f"end={self._end!r}, capacity={self._capacity})"
        )


class Schedule:
    """Shifts placed on a ``Calendar``: capacity as a function of sim time.

    Args:
        calendar: converts local wall-clock times to sim time; its ``tz`` is
            the zone of the shift times.
        shifts: an iterable of ``Shift``; may be empty (capacity is always 0).
        closed: an iterable of ``date``; no shift *starts* on these dates (a
            shift that started the day before still finishes). ``datetime``
            is rejected, only plain dates.

    Raises:
        TypeError: ``calendar`` is not a ``Calendar``, an item of ``shifts`` is
            not a ``Shift``, an item of ``closed`` is not a ``date``.
    """

    def __init__(
        self,
        calendar: Calendar,
        shifts: Iterable[Shift],
        *,
        closed: Iterable[date] = (),
    ) -> None:
        if not isinstance(calendar, Calendar):
            raise TypeError(
                f"calendar must be a Calendar, got {type(calendar).__name__}"
            )
        shift_list = list(shifts)
        for s in shift_list:
            if not isinstance(s, Shift):
                raise TypeError(f"shifts must contain Shift, got {s!r}")
        closed_set = frozenset(closed)
        for d in closed_set:
            if not isinstance(d, date) or isinstance(d, datetime):
                raise TypeError(f"closed must contain dates, got {d!r}")
        self._calendar = calendar
        self._shifts = tuple(shift_list)
        self._closed = closed_set

    @property
    def calendar(self) -> Calendar:
        """The calendar the schedule is placed on."""
        return self._calendar

    def _starting_on(self, day: date) -> list[tuple[float, float, int]]:
        """``(start, end, capacity)`` in sim time of the shifts starting on ``day``."""
        if day in self._closed:
            return []
        cal, tz = self._calendar, self._calendar.tz
        out = []
        for s in self._shifts:
            if day.weekday() not in s.days:
                continue
            end_day = day + _DAY if s.end <= s.start else day
            out.append(
                (
                    cal.to_sim(_local_instant(day, s.start, tz)),
                    cal.to_sim(_local_instant(end_day, s.end, tz)),
                    s.capacity,
                )
            )
        return out

    def _local_date(self, t: float) -> date:
        return self._calendar.to_datetime(t).date()

    def capacity_at(self, t: float) -> int:
        """The summed capacity of the shifts running at sim time ``t``.

        A shift runs on ``[start, end)``. Raises like ``Calendar.to_datetime``
        for a ``t`` that is not a finite real number.
        """
        today = self._local_date(t)
        total = 0
        # A shift lasts at most 24 h (+1 h of DST), so it started today,
        # yesterday, or (around midnight and DST shifts) a day further back or
        # on the next local date; look at all of them.
        for back in (-2, -1, 0, 1):
            for start, end, cap in self._starting_on(today + back * _DAY):
                if start <= t < end:
                    total += cap
        return total

    def changes(self, after: float) -> Iterator[tuple[float, int]]:
        """Yield ``(t, capacity)`` for each change in capacity after sim time ``after``.

        Strictly increasing ``t > after``; ``capacity`` is the new total from
        ``t`` on. Boundaries where the total does not change (for example two
        back-to-back shifts of equal capacity) are skipped. Lazy and endless:
        it walks the calendar day by day, so take what you need
        (``itertools.islice``). Yields nothing if there are no shifts. A
        schedule can't be closed forever (``closed`` is finite), so every
        search finds a change eventually, unless the capacity is constant
        for good (for example 24 h shifts on every day, or back-to-back shifts
        of equal capacity around the clock). The weekly pattern repeats, so the
        generator then ends after 400 days without a change past the last
        closed date, instead of looping forever. It also ends at ``date.max``.
        """
        if not self._shifts:
            return
        day = self._local_date(after) - 2 * _DAY
        # Events are (time, delta). A day's shifts start at or after its local
        # midnight, so once the days up to `day` are pushed, every event before
        # the midnight of `day + 1` is in the heap and can be released in order.
        heap: list[tuple[float, int]] = []
        running = 0
        # Give up when nothing changed for a year (+ a margin) after the last
        # closed date: the weekly pattern repeats, so a capacity that is
        # constant that long is constant forever (e.g. 24 h shifts on all days).
        quiet_since = max([self._local_date(after), *self._closed])
        try:
            while day - quiet_since <= _IDLE_LIMIT:
                for start, end, cap in self._starting_on(day):
                    heapq.heappush(heap, (start, cap))
                    heapq.heappush(heap, (end, -cap))
                day += _DAY
                safe = self._calendar.to_sim(
                    _local_instant(day, time.min, self._calendar.tz)
                )
                while heap and heap[0][0] < safe:
                    t = heap[0][0]
                    before = running
                    while heap and heap[0][0] == t:
                        running += heapq.heappop(heap)[1]
                    if t > after and running != before:
                        quiet_since = max(quiet_since, day)
                        yield t, running
        except OverflowError:  # past date.max
            return
