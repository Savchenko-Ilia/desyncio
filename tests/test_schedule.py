"""Tests for ``desyncio.schedule`` (CAL-02)."""

import itertools
import math
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given
from hypothesis import strategies as st

from desyncio.dates import Calendar
from desyncio.schedule import Schedule, Shift

BERLIN = ZoneInfo("Europe/Berlin")
MON_TO_FRI = range(5)


def cal_at(y: int, m: int, d: int, tz: ZoneInfo = BERLIN) -> Calendar:
    return Calendar(datetime(y, m, d, tzinfo=tz))


def local(cal: Calendar, t: float) -> str:
    return cal.to_datetime(t).strftime("%a %H:%M")


def first(sched: Schedule, after: float, n: int) -> list[tuple[float, int]]:
    return list(itertools.islice(sched.changes(after), n))


def test_weekday_and_weekend_pattern() -> None:
    cal = cal_at(2026, 1, 5)  # a Monday
    sched = Schedule(cal, [Shift(MON_TO_FRI, time(9), time(17), 2)])
    h = 3600.0
    assert sched.capacity_at(8.99 * h) == 0
    assert sched.capacity_at(9 * h) == 2  # start is inclusive
    assert sched.capacity_at(17 * h) == 0  # end is exclusive
    assert sched.capacity_at((5 * 24 + 12) * h) == 0  # Saturday noon
    got = [(local(cal, t), c) for t, c in first(sched, 0, 13)]
    assert got[:2] == [("Mon 09:00", 2), ("Mon 17:00", 0)]
    assert got[9:] == [  # Fri close, then Monday: the weekend has no changes
        ("Fri 17:00", 0),
        ("Mon 09:00", 2),
        ("Mon 17:00", 0),
        ("Tue 09:00", 2),
    ]


def test_overnight_shift_friday_into_saturday() -> None:
    cal = cal_at(2026, 1, 5)
    sched = Schedule(cal, [Shift([4], time(22), time(6), 1)])
    assert [(local(cal, t), c) for t, c in first(sched, 0, 3)] == [
        ("Fri 22:00", 1),
        ("Sat 06:00", 0),
        ("Fri 22:00", 1),
    ]
    sat_3am = cal.to_sim(datetime(2026, 1, 10, 3, tzinfo=BERLIN))
    assert sched.capacity_at(sat_3am) == 1
    # `after` inside the shift: the end of a shift started the day before shows.
    assert first(sched, sat_3am, 1)[0][1] == 0


def test_equal_start_and_end_is_24_hours() -> None:
    cal = cal_at(2026, 1, 5)
    sched = Schedule(cal, [Shift([0], time(8), time(8), 1)])
    t0 = cal.to_sim(datetime(2026, 1, 5, 8, tzinfo=BERLIN))
    assert sched.capacity_at(t0 + 24 * 3600 - 1) == 1
    assert sched.capacity_at(t0 + 24 * 3600) == 0


def test_overlapping_shifts_add() -> None:
    cal = cal_at(2026, 1, 5)
    sched = Schedule(
        cal,
        [Shift([0], time(8), time(16), 2), Shift([0], time(12), time(20), 3)],
    )
    assert [(local(cal, t), c) for t, c in first(sched, 0, 4)] == [
        ("Mon 08:00", 2),
        ("Mon 12:00", 5),
        ("Mon 16:00", 3),
        ("Mon 20:00", 0),
    ]
    # Back-to-back shifts with equal capacity: no change at the hand-over.
    seq = Schedule(
        cal, [Shift([0], time(8), time(12), 2), Shift([0], time(12), time(16), 2)]
    )
    assert [(local(cal, t), c) for t, c in first(seq, 0, 2)] == [
        ("Mon 08:00", 2),
        ("Mon 16:00", 0),
    ]


def test_closed_date_blocks_starts_but_not_a_running_overnight_shift() -> None:
    cal = cal_at(2026, 1, 5)
    night = Shift(range(7), time(22), time(6), 1)
    sched = Schedule(cal, [night], closed=[date(2026, 1, 6)])
    got = [(local(cal, t), c) for t, c in first(sched, 0, 5)]
    assert got == [
        ("Mon 06:00", 0),  # the Sunday-night shift is running at t=0
        ("Mon 22:00", 1),
        ("Tue 06:00", 0),  # started Monday: still runs into the closed day
        ("Wed 22:00", 1),  # Tuesday 22:00 does not start
        ("Thu 06:00", 0),
    ]


def test_dst_spring_forward_gap_moves_to_first_valid_instant() -> None:
    cal = cal_at(2026, 3, 28)
    sched = Schedule(cal, [Shift([6], time(2, 30), time(5), 1)])  # Sun 29 March
    (t, c), _ = first(sched, 0, 2)
    assert c == 1
    assert cal.to_datetime(t) == datetime(2026, 3, 29, 3, 0, tzinfo=BERLIN)
    assert cal.to_datetime(t).utcoffset() == timedelta(hours=2)
    assert cal.to_datetime(t) == datetime(2026, 3, 29, 1, 0, tzinfo=UTC)


def test_dst_fall_back_uses_first_occurrence() -> None:
    cal = cal_at(2026, 10, 24)
    sched = Schedule(cal, [Shift([6], time(2, 30), time(5), 1)])  # Sun 25 Oct
    (t, _), (t_end, _) = first(sched, 0, 2)
    assert cal.to_datetime(t).astimezone(UTC) == datetime(
        2026, 10, 25, 0, 30, tzinfo=UTC
    )
    assert cal.to_datetime(t_end).astimezone(UTC) == datetime(
        2026, 10, 25, 4, 0, tzinfo=UTC
    )


def test_dst_gap_empty_shift_never_runs() -> None:
    cal = cal_at(2026, 3, 28)
    sched = Schedule(cal, [Shift([6], time(2, 15), time(2, 45), 1)])
    # Both boundaries move to 03:00, so the shift is empty: the first change is
    # a later Sunday, not the 29th.
    assert first(sched, 0, 1)[0][0] > cal.to_sim(datetime(2026, 4, 1, tzinfo=BERLIN))
    t = cal.to_sim(datetime(2026, 3, 29, 3, tzinfo=BERLIN))
    assert sched.capacity_at(t) == 0


def test_no_shifts_yields_nothing() -> None:
    assert first(Schedule(cal_at(2026, 1, 1), []), 0, 1) == []


def test_constant_capacity_ends_instead_of_looping() -> None:
    sched = Schedule(cal_at(2026, 1, 5), [Shift(range(7), time(8), time(8), 1)])
    assert list(itertools.islice(sched.changes(10 * 24 * 3600), 5)) == []


@st.composite
def shifts(draw: st.DrawFn) -> Shift:
    return Shift(
        draw(st.sets(st.integers(0, 6), min_size=1)),
        time(draw(st.integers(0, 23)), draw(st.sampled_from([0, 30]))),
        time(draw(st.integers(0, 23)), draw(st.sampled_from([0, 30]))),
        draw(st.integers(1, 4)),
    )


@given(
    shift_list=st.lists(shifts(), min_size=1, max_size=4),
    closed=st.sets(st.integers(0, 120).map(lambda n: date(2026, 1, 1) + timedelta(n))),
    after=st.floats(0, 1.5e7),
)
def test_changes_agree_with_capacity_at(
    shift_list: list[Shift], closed: set[date], after: float
) -> None:
    sched = Schedule(cal_at(2026, 1, 1), shift_list, closed=closed)
    prev_t, prev_c = after, sched.capacity_at(after)
    for t, c in first(sched, after, 8):
        assert t > prev_t
        assert c != prev_c
        assert sched.capacity_at(t) == c
        assert sched.capacity_at(math.nextafter(t, -math.inf)) == prev_c
        assert sched.capacity_at((t + prev_t) / 2) == prev_c
        prev_t, prev_c = t, c


def test_validation() -> None:
    ok = {"days": [0], "start": time(9), "end": time(17), "capacity": 1}

    def bad(exc: type[Exception], **kw: object) -> None:
        with pytest.raises(exc):
            Shift(**{**ok, **kw})  # type: ignore[arg-type]  # deliberately wrong

    bad(ValueError, days=[])
    bad(ValueError, days=[7])
    bad(ValueError, days=[-1])
    bad(TypeError, days=["mon"])
    bad(TypeError, days=[True])
    bad(TypeError, days=3)
    bad(TypeError, start=datetime(2026, 1, 1, 9))
    bad(TypeError, start=time(9, tzinfo=UTC))
    bad(TypeError, end=time(17, tzinfo=BERLIN))
    bad(ValueError, capacity=0)
    bad(TypeError, capacity=True)
    bad(TypeError, capacity=1.0)
    assert Shift([0, 0, 2], time(9), time(17), 1).days == frozenset({0, 2})

    cal = cal_at(2026, 1, 1)
    with pytest.raises(TypeError):
        Schedule("cal", [])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Schedule(cal, [object()])  # type: ignore[list-item]
    with pytest.raises(TypeError):
        Schedule(cal, [], closed=[datetime(2026, 1, 1)])
    with pytest.raises(TypeError):
        Schedule(cal, [], closed=["2026-01-01"])  # type: ignore[list-item]
