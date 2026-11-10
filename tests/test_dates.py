"""Tests for ``desyncio.dates.Calendar`` (CAL-01)."""

import math
from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given
from hypothesis import strategies as st

import desyncio
from desyncio import sim
from desyncio.dates import Calendar

BERLIN = ZoneInfo("Europe/Berlin")
NEW_YORK = ZoneInfo("America/New_York")
START = datetime(2026, 1, 1, tzinfo=UTC)


@given(t=st.floats(-1e6, 1e6), unit_s=st.sampled_from([1, 60, 3600]))
def test_round_trip_sim_time(t: float, unit_s: int) -> None:
    cal = Calendar(START.astimezone(BERLIN), unit=timedelta(seconds=unit_s))
    # The spec's 1 µs, in sim units: 1 µs is 1e-6 / unit_s units (unit_s=3600:
    # 2.8e-10 units), not 1e-6 * unit_s.
    one_us = 1e-6 / unit_s
    # Float slack: to_sim returns the float nearest to the exact quotient, so the
    # result can be off by up to half of a float's spacing at t on top of the
    # 0.5 µs rounding of to_datetime. That spacing is math.ulp(t), at most
    # math.ulp(1e6) = 1.2e-10 for |t| <= 1e6. The worst case (0.5 µs + half a
    # spacing) fits in 1 µs today; the slack keeps the test about the 1 µs
    # spec and not about how floats happen to round at the edge.
    assert cal.to_sim(cal.to_datetime(t)) == pytest.approx(
        t, rel=0, abs=one_us + math.ulp(t)
    )


@given(
    dt=st.datetimes(
        min_value=datetime(1990, 1, 1),
        max_value=datetime(2060, 1, 1),
        timezones=st.just(NEW_YORK),
    )
)
def test_round_trip_datetime_exact(dt: datetime) -> None:
    cal = Calendar(START.astimezone(BERLIN))
    back = cal.to_datetime(cal.to_sim(dt))
    # Compare as UTC: == across zones is False for times in a DST fold.
    assert back.astimezone(UTC) == dt.astimezone(UTC)
    assert back == dt.astimezone(BERLIN)
    assert back.tzinfo is BERLIN


def test_dst_spring_forward_berlin() -> None:
    cal = Calendar(datetime(2026, 3, 29, 1, 30, tzinfo=BERLIN))
    dt = cal.to_datetime(3600)
    assert (dt.hour, dt.minute) == (3, 30)
    assert dt.utcoffset() == timedelta(hours=2)  # CEST


def test_dst_fall_back_berlin() -> None:
    cal = Calendar(datetime(2026, 10, 25, 2, 30, tzinfo=BERLIN, fold=0))
    assert cal.start.utcoffset() == timedelta(hours=2)
    dt = cal.to_datetime(3600)
    assert (dt.hour, dt.minute, dt.fold) == (2, 30, 1)
    assert dt.utcoffset() == timedelta(hours=1)  # CET
    assert cal.to_sim(dt) == 3600.0  # same wall time, the fold is not lost


def test_to_sim_other_zone_and_same_zone_across_dst() -> None:
    cal = Calendar(datetime(2026, 1, 1, 12, tzinfo=BERLIN))
    ny = datetime(2026, 1, 1, 6, tzinfo=NEW_YORK)  # 12:00 Berlin
    assert cal.to_sim(ny) == 0.0
    assert cal.to_sim(ny + timedelta(hours=1)) == 3600.0
    # Same tzinfo as start: the difference must still be real elapsed time.
    summer = datetime(2026, 7, 1, 12, tzinfo=BERLIN)
    assert (
        cal.to_sim(summer)
        == (summer.astimezone(UTC) - cal.start.astimezone(UTC)).total_seconds()
    )


def test_unit_minutes_negative_and_properties() -> None:
    cal = Calendar(START, unit=timedelta(minutes=1))
    assert cal.to_datetime(90) == START + timedelta(minutes=90)
    assert cal.to_datetime(-1) == START - timedelta(minutes=1)
    assert cal.to_sim(START + timedelta(hours=1)) == 60.0
    assert (cal.start, cal.unit, cal.tz) == (START, timedelta(minutes=1), UTC)
    assert Calendar(START).unit == timedelta(seconds=1)
    with pytest.raises(AttributeError):
        cal.start = START  # type: ignore[misc]  # read-only on purpose


def test_rounds_to_microsecond() -> None:
    assert Calendar(START).to_datetime(1.0000004) == START + timedelta(seconds=1)
    assert Calendar(START).to_datetime(1.0000006) == START + timedelta(
        seconds=1, microseconds=1
    )


def test_now_inside_run() -> None:
    cal = Calendar(datetime(2026, 3, 29, 1, 30, tzinfo=BERLIN))
    seen: list[datetime] = []

    async def main() -> None:
        seen.append(cal.now())
        await sim.timeout(3600)
        seen.append(cal.now())

    desyncio.run(main())
    assert seen[0] == cal.start
    assert seen[1].hour == 3 and seen[1].utcoffset() == timedelta(hours=2)


def test_now_outside_run() -> None:
    with pytest.raises(RuntimeError):
        Calendar(START).now()


def test_validation() -> None:
    with pytest.raises(TypeError):
        Calendar(datetime(2026, 1, 1))  # naive
    with pytest.raises(TypeError):
        Calendar("2026-01-01")  # type: ignore[arg-type]  # deliberate
    with pytest.raises(TypeError):
        Calendar(START, unit=60)  # type: ignore[arg-type]  # deliberate
    for bad in (timedelta(0), timedelta(seconds=-1)):
        with pytest.raises(ValueError):
            Calendar(START, unit=bad)
    cal = Calendar(START)
    for bad_t in (True, "1", None):
        with pytest.raises(TypeError):
            cal.to_datetime(bad_t)  # type: ignore[arg-type]  # deliberate
    for bad_f in (math.nan, math.inf, -math.inf, 10**400, 1e300):
        with pytest.raises(ValueError):
            cal.to_datetime(bad_f)
    with pytest.raises(TypeError):
        cal.to_sim(datetime(2026, 1, 1))  # naive
    with pytest.raises(TypeError):
        cal.to_sim(5)  # type: ignore[arg-type]  # deliberate
    # In range locally, out of range once converted to UTC: ValueError, as
    # to_datetime, not a bare OverflowError.
    for out_of_range in (
        datetime.max.replace(tzinfo=NEW_YORK),
        datetime.max.replace(tzinfo=timezone(timedelta(hours=-5))),
        datetime.min.replace(tzinfo=timezone(timedelta(hours=5))),
    ):
        with pytest.raises(ValueError, match="out of the range"):
            cal.to_sim(out_of_range)
    odd = timezone(timedelta(hours=5))
    assert Calendar(datetime(2026, 1, 1, tzinfo=odd)).tz is odd
