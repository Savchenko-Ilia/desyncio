# Calendar guide

Simulation time is a plain number. A hospital, a call centre or a factory does not live on a
number line: it lives on a calendar, with weekdays, opening hours, holidays and a clock that
jumps twice a year. This guide shows how to attach real dates to sim time with `Calendar`, how
to describe weekly opening hours with `Shift` and `Schedule`, and how to let a resource's
capacity follow that schedule with `ScheduledResource`. The last section puts it all together in
a small clinic model.

Every `python` block below is executed by `tests/test_docs_examples.py`, and the `text` block
under it is compared with what the example really prints (the same convention as the
[primitives guide](primitives.md) and the [statistics guide](statistics.md)). All seeds are
fixed, so the output is the same on every run. All examples use top-level imports
(`from desyncio import ...`).

Contents:

1. [Calendar: sim time and dates](#calendar-sim-time-and-dates)
2. [A DST day](#a-dst-day)
3. [Schedule: weekly shifts](#schedule-weekly-shifts)
4. [Shifts across a DST change](#shifts-across-a-dst-change)
5. [ScheduledResource: a clinic](#scheduledresource-a-clinic)
6. [Limits and requirements](#limits-and-requirements)

## Calendar: sim time and dates

Inside a run, sim time is a float, and one unit is one second (D004). A `Calendar` says which
date sim time 0 stands for and, if you want, how long one sim time unit is.

`Calendar(start, *, unit=timedelta(seconds=1))`:

- `start` is a timezone-aware `datetime`. A naive one raises `TypeError`. The zone of `start`
  is the zone of every datetime the calendar returns.
- `unit` is a positive `timedelta`: how long one sim time unit lasts.
- `to_datetime(t)` gives the aware datetime at sim time `t`. `to_sim(dt)` is the inverse: it
  accepts an aware datetime in any zone and returns a float.
- `now()` is `to_datetime(sim.now)` and works only inside a run.

A `Calendar` is a pure converter. It does not change the sim clock, and you can use it outside
a run too (only `now()` needs one).

```python
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from desyncio import Calendar, run, sim

berlin = ZoneInfo("Europe/Berlin")
start = datetime(2026, 6, 1, 8, 0, tzinfo=berlin)

seconds = Calendar(start)  # one sim time unit is one second
print(seconds.to_datetime(0))
print(seconds.to_datetime(90))
print(seconds.to_sim(datetime(2026, 6, 1, 10, 0, tzinfo=berlin)))

minutes = Calendar(start, unit=timedelta(minutes=1))  # one sim time unit is a minute
print(minutes.to_datetime(90))
print(minutes.to_sim(datetime(2026, 6, 1, 10, 0, tzinfo=berlin)))

# to_sim accepts any zone: 08:00 UTC is 10:00 in Berlin (CEST, UTC+2),
# 120 minutes after the start.
print(minutes.to_sim(datetime(2026, 6, 1, 8, 0, tzinfo=UTC)))


async def main() -> None:
    await sim.timeout(150)  # 150 minutes
    print(minutes.now())


run(main())
```

```text
2026-06-01 08:00:00+02:00
2026-06-01 08:01:30+02:00
7200.0
2026-06-01 09:30:00+02:00
120.0
120.0
2026-06-01 10:30:00+02:00
```

## A DST day

Local time is not a line: when the clocks go forward, an hour of wall-clock time is skipped,
and when they go back, an hour is repeated. If sim time counted local hours, it would jump
forward or repeat. So a `Calendar` counts **elapsed time**: it adds `t * unit` to `start`
converted to UTC, and only then shows the result in the zone of `start` (D051). Sim time never
jumps. The wall clock does.

Europe/Berlin changes clocks on 2026-03-29 (02:00 CET becomes 03:00 CEST) and on 2026-10-25
(03:00 CEST becomes 02:00 CET). The example starts at local midnight on each of these days and
prints the local time after 0, 1, 2, 3 and 4 hours of sim time, and how many hours the day
lasts from midnight to midnight.

```python
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from desyncio import Calendar

berlin = ZoneInfo("Europe/Berlin")
for label, day in (
    ("spring", datetime(2026, 3, 29)),
    ("autumn", datetime(2026, 10, 25)),
):
    cal = Calendar(day.replace(tzinfo=berlin))
    print(label)
    for hours in range(5):
        dt = cal.to_datetime(hours * 3600)
        print(f"  t={hours * 3600:>5}  {dt:%H:%M} {dt.tzname()}  fold={dt.fold}")
    next_midnight = datetime.combine(day.date() + timedelta(days=1), time(), berlin)
    print("  hours in this day:", cal.to_sim(next_midnight) / 3600)
```

```text
spring
  t=    0  00:00 CET  fold=0
  t= 3600  01:00 CET  fold=0
  t= 7200  03:00 CEST  fold=0
  t=10800  04:00 CEST  fold=0
  t=14400  05:00 CEST  fold=0
  hours in this day: 23.0
autumn
  t=    0  00:00 CEST  fold=0
  t= 3600  01:00 CEST  fold=0
  t= 7200  02:00 CEST  fold=0
  t=10800  02:00 CET  fold=1
  t=14400  03:00 CET  fold=0
  hours in this day: 25.0
```

Read the output like this:

- In spring, sim time 7200 (two hours after midnight) is 03:00 CEST: 02:00 to 03:00 does not
  exist on that day, and the day is only 23 hours long.
- In autumn, the day has 25 hours. The local time 02:00 happens twice. `to_datetime` returns
  both, and tells them apart with `fold`: `fold=0` is the first (CEST, summer time), `fold=1`
  the second (CET). Two aware datetimes that share one `tzinfo` are compared by their wall-clock
  fields, ignoring the offset, so do not use local datetimes to order events: compare sim
  time (convert with `to_sim`).

## Schedule: weekly shifts

A `Shift(days, start, end, capacity)` says: on these weekdays, from this wall-clock time to that
one, `capacity` units are available.

- `days` are weekday numbers as in `date.weekday()`: 0 is Monday, 6 is Sunday. A shift belongs to
  the weekday it **starts** on.
- `start` and `end` are naive `datetime.time` values in the zone of the calendar. A shift runs on
  `[start, end)`.
- `end <= start` means the shift ends on the next day (an overnight shift). `end == start` is
  24 hours.
- `capacity` is an `int` of at least 1.

`Schedule(calendar, shifts, *, closed=())` puts shifts on a `Calendar`:

- `capacity_at(t)` is the sum of the capacities of the shifts running at sim time `t`.
  **Overlapping shifts add up.**
- `closed` is a set of dates on which no shift **starts**. An overnight shift that started the day
  before still finishes.
- `changes(after)` yields `(t, capacity)` for each change in capacity strictly after sim time
  `after`, in increasing order of `t`, lazily. It skips boundaries where the total does not
  change (for example two back-to-back shifts of equal capacity). It ends when the capacity is
  constant from some point on forever (D053), for example when shifts cover the whole week
  without a break. Take what you need with `itertools.islice`.

`Schedule` is pure code: no `asyncio`, no run needed.

The example is a week with three shifts: two people at the weekday desk from 08:00 to 16:00, one
at the weekend from 10:00 to 14:00, and a night watch from Friday 22:00 to Saturday 06:00.
Wednesday 3 June is closed. The first day of the calendar is Monday 1 June 2026.

```python
from datetime import date, datetime, time
from itertools import islice
from zoneinfo import ZoneInfo

from desyncio import Calendar, Schedule, Shift

berlin = ZoneInfo("Europe/Berlin")
cal = Calendar(datetime(2026, 6, 1, tzinfo=berlin))  # Monday 00:00
schedule = Schedule(
    cal,
    [
        Shift(range(5), time(8), time(16), 2),  # Monday to Friday
        Shift([5, 6], time(10), time(14), 1),  # Saturday and Sunday
        Shift([4], time(22), time(6), 1),  # Friday night into Saturday
    ],
    closed=[date(2026, 6, 3)],
)

for label, local in (
    ("Mon 09:00", datetime(2026, 6, 1, 9)),
    ("Mon 17:00", datetime(2026, 6, 1, 17)),
    ("Wed 09:00", datetime(2026, 6, 3, 9)),  # closed
    ("Sat 03:00", datetime(2026, 6, 6, 3)),  # the Friday night shift
):
    t = cal.to_sim(local.replace(tzinfo=berlin))
    print(f"{label}: capacity {schedule.capacity_at(t)}")

for t, capacity in islice(schedule.changes(0), 12):
    print(f"{cal.to_datetime(t):%a %d %b %H:%M} -> {capacity}")
```

```text
Mon 09:00: capacity 2
Mon 17:00: capacity 0
Wed 09:00: capacity 0
Sat 03:00: capacity 1
Mon 01 Jun 08:00 -> 2
Mon 01 Jun 16:00 -> 0
Tue 02 Jun 08:00 -> 2
Tue 02 Jun 16:00 -> 0
Thu 04 Jun 08:00 -> 2
Thu 04 Jun 16:00 -> 0
Fri 05 Jun 08:00 -> 2
Fri 05 Jun 16:00 -> 0
Fri 05 Jun 22:00 -> 1
Sat 06 Jun 06:00 -> 0
Sat 06 Jun 10:00 -> 1
Sat 06 Jun 14:00 -> 0
```

The output shows each rule: Wednesday is missing from the changes (the closed date), the night
watch starts on Friday at 22:00 and ends on Saturday at 06:00, and at Friday 22:00 the capacity
goes from 0 to 1 because the day shift ended at 16:00.

The next example shows that shifts add. The `changes` of a second schedule, with two
overlapping shifts on one day, step up and down in two stages.

```python
from datetime import datetime, time
from itertools import islice
from zoneinfo import ZoneInfo

from desyncio import Calendar, Schedule, Shift

cal = Calendar(datetime(2026, 6, 1, tzinfo=ZoneInfo("Europe/Berlin")))
overlap = Schedule(
    cal,
    [Shift([0], time(8), time(12), 2), Shift([0], time(10), time(14), 1)],
)
for t, capacity in islice(overlap.changes(0), 4):  # the first Monday
    print(f"{cal.to_datetime(t):%H:%M} -> {capacity}")
```

```text
08:00 -> 2
10:00 -> 3
12:00 -> 1
14:00 -> 0
```

## Shifts across a DST change

A shift boundary is a **local wall-clock time**, which on a DST day may not exist or may happen
twice. The rules (D051 item 5, in the `Schedule` docstring):

- A boundary at a **non-existent** local time (spring forward) moves to the first valid instant
  after the gap. A shift whose start and end are both inside the gap is empty and never runs
  (D053).
- An **ambiguous** local time (fall back) uses its first occurrence (`fold=0`, the summer-time
  one).

The example puts a shift from 01:00 to 04:00 and a shift from 02:30 to 03:30 on each of the two
Berlin DST days (both days are Sundays), and prints their start and end in sim time and as local
times, and how long they last.

```python
from datetime import date, datetime, time
from itertools import islice
from zoneinfo import ZoneInfo

from desyncio import Calendar, Schedule, Shift

berlin = ZoneInfo("Europe/Berlin")
for day in (date(2026, 3, 29), date(2026, 10, 25)):
    cal = Calendar(datetime.combine(day, time(), berlin))
    print(day)
    for start, end in ((time(1), time(4)), (time(2, 30), time(3, 30))):
        schedule = Schedule(cal, [Shift([6], start, end, 1)])
        (t_on, _), (t_off, _) = islice(schedule.changes(-1), 2)
        print(
            f"  shift {start:%H:%M}-{end:%H:%M}: "
            f"{cal.to_datetime(t_on):%H:%M %Z} to {cal.to_datetime(t_off):%H:%M %Z}, "
            f"{(t_off - t_on) / 3600} h"
        )
```

```text
2026-03-29
  shift 01:00-04:00: 01:00 CET to 04:00 CEST, 2.0 h
  shift 02:30-03:30: 03:00 CEST to 03:30 CEST, 0.5 h
2026-10-25
  shift 01:00-04:00: 01:00 CEST to 04:00 CET, 4.0 h
  shift 02:30-03:30: 02:30 CEST to 03:30 CET, 2.0 h
```

Both shifts are measured in elapsed time, so their length depends on the day. The 01:00-04:00
shift lasts 2 hours in spring and 4 hours in autumn. In spring the 02:30-03:30 shift starts at
03:00 CEST (the first valid instant after the gap, since 02:30 does not exist), so it lasts half
an hour instead of one. In autumn 02:30 happens twice and the shift starts at the first one
(02:30 CEST); it ends at 03:30 CET, which is 2 hours later because the clock hour 02:00-03:00
is lived through twice.

## ScheduledResource: a clinic

`ScheduledResource(schedule, *, monitor=None)` is a resource whose capacity follows a
`Schedule`. You create it **inside a run**, because it starts a background driver task, and use
it like the other primitives: `async with clinic:`.

- At creation, `capacity` is `schedule.capacity_at(sim.now)`. The driver task then applies each
  entry of `schedule.changes(...)` at its time. When `changes` ends (D053), the driver finishes
  and the last capacity stays.
- While capacity is 0, requests wait. When capacity **rises**, waiters are granted in first come,
  first served order.
- When capacity **drops** below the number of units in use, the holders who were granted **most
  recently** lose their unit first (D051 item 2; they have done the least work). Each one's
  `async with` raises `Interrupt(ShiftEnded(at, resource))` and the body does not continue. The
  unit is already released. `at` is the sim time of the capacity drop.
- If a visit ends at the exact instant a shift ends, the visit finishes when its own timer is
  what ends it (for example the end of a `sim.timeout`): at one instant, ordinary timers fire
  **before** the shift change (D054). A request whose own timer fires at the instant capacity
  rises is queued first and then granted by the rise. The guarantee covers work one step from a
  timer at that instant: a visit that ends only after another task woken at that instant has
  acted (say it waits for an `Event` that task sets) may still lose to the shift change and be
  interrupted.
- `capacity`, `count` (units in use) and `queue_length` can be read at any time.

Catch the interrupt **outside** the `async with`, as for `Preempted` in the
[primitives guide](primitives.md). Tell a shift end from any other interrupt by its cause:
`isinstance(exc.cause, ShiftEnded)`.

The clinic: two doctors work from 08:00 to 12:00 and one from 12:00 to 16:00 on weekdays. The
calendar counts in minutes and starts on Monday at 08:00. A patient arrives on average every
8 minutes (exponential gaps, stream `arrivals`) until sim time passes 15:30 (7.5 hours after the
start), and a treatment takes 20 minutes on average (stream `treatment`). Two doctors can treat
at most 6 patients an hour and one doctor 3, while 7.5 patients arrive per hour, so the clinic is
overloaded all day and a queue builds up. The wait of a patient (arrival to start of
treatment) goes into a `Tally`. When capacity drops, the holders of the most
recently granted units are sent away, as many as the drop requires. The model stops at 17:00 and prints what happened.

```python
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from desyncio import (
    Calendar,
    Interrupt,
    Schedule,
    ScheduledResource,
    Shift,
    ShiftEnded,
    Tally,
    run,
    sim,
)

berlin = ZoneInfo("Europe/Berlin")
cal = Calendar(datetime(2026, 6, 1, 8, 0, tzinfo=berlin), unit=timedelta(minutes=1))
schedule = Schedule(
    cal,
    [
        Shift(range(5), time(8), time(12), 2),  # two doctors in the morning
        Shift(range(5), time(12), time(16), 1),  # one in the afternoon
    ],
)


async def main() -> None:
    clinic = ScheduledResource(schedule)
    waits = Tally()
    arrivals, treatment = sim.random("arrivals"), sim.random("treatment")
    treated: dict[int, float] = {}  # patient number -> end of treatment
    granted: dict[int, float] = {}  # patient number -> start of treatment
    sent_away: list[tuple[int, datetime]] = []

    async def patient(number: int) -> None:
        arrived = sim.now
        try:
            async with clinic:
                granted[number] = sim.now
                waits.record(sim.now - arrived)
                await sim.timeout(treatment.expovariate(1 / 20))
                treated[number] = sim.now
        except Interrupt as exc:  # outside the ``async with``
            assert isinstance(exc.cause, ShiftEnded)
            sent_away.append((number, cal.to_datetime(exc.cause.at)))

    patients = []
    while sim.now < 7.5 * 60:
        await sim.timeout(arrivals.expovariate(1 / 8))
        patients.append(sim.spawn(patient(len(patients) + 1)))
    await sim.timeout(cal.to_sim(datetime(2026, 6, 1, 17, tzinfo=berlin)) - sim.now)

    print("arrived:", len(patients))
    print("treated:", len(treated))
    print("sent away:", len(sent_away))
    print("still waiting at 17:00:", clinic.queue_length)
    print(
        "mean wait (minutes):", round(waits.mean(), 1), "over", len(waits), "patients"
    )

    def label(n: int) -> str:
        return f"patient {n} (granted {cal.to_datetime(granted[n]):%H:%M})"

    for number, at in sent_away:
        t = cal.to_sim(at)
        kept = [n for n, end in treated.items() if granted[n] < t < end]
        line = f"{label(number)} interrupted at {at:%H:%M}"
        if kept:
            line += " while " + ", ".join(map(label, kept)) + " kept the unit"
        print(line)


run(main(), seed=3)
```

```text
arrived: 57
treated: 25
sent away: 2
still waiting at 17:00: 30
mean wait (minutes): 37.8 over 27 patients
patient 19 (granted 11:53) interrupted at 12:00 while patient 18 (granted 11:43) kept the unit
patient 27 (granted 15:53) interrupted at 16:00
```

Reading the output:

- `treated` plus `sent away` is the number of patients who started treatment, which is the number
  of waits in the `Tally` (the `Tally` only has waits of patients whose treatment began). The
  other patients are still in the queue at 17:00, so the mean wait of 37.8 minutes leaves them
  out: the real average wait is longer.
- Two patients lost their unit, both caught as `Interrupt` outside the `async with` with
  `isinstance(exc.cause, ShiftEnded)`. `exc.cause.at` is the sim time of the capacity drop;
  `cal.to_datetime` turns it into the local time: 12:00, when the second doctor goes home, and
  16:00, when the clinic closes.
- At 12:00 two patients are in treatment and one unit goes: patient 19, granted last (11:53),
  is sent away, and patient 18, granted earlier (11:43), keeps the unit. The most recently
  granted holder is interrupted first.
- A patient whose treatment ends before the drop is not interrupted: their `async with` has
  already released the unit. The interrupted patients are the ones still in treatment when the
  shift ends.
- After 16:00 the capacity is 0 and the queue stays until the next shift (Tuesday 08:00). The
  run is stopped at 17:00, and the patients still waiting are cancelled with it.

## Limits and requirements

- **Nested `sim.interruptible()`.** The interrupt is delivered to the innermost
  `sim.interruptible()` block. If the body of `async with clinic:` has its own
  `sim.interruptible()` and swallows an `Interrupt` whose cause is `ShiftEnded`, the body goes
  on running **without a unit** (the unit is already gone), and capacity is no longer respected.
  Re-raise such an interrupt and catch it outside the `async with`.
- **The shift change is applied at once; the interrupt arrives a moment later.** Other tasks
  that were already scheduled for the same instant may run between the capacity drop and the
  victim noticing it. The victim's body does not run again.
- **Time zone data.** `zoneinfo` needs the time-zone database of the system. Linux and macOS have
  it. On Windows install the `tzdata` package yourself: desyncio does not depend on it (D051
  item 6).
- **A schedule that keeps changing keeps the run alive.** While the driver has a change ahead,
  the loop is never "deadlocked": a `main` that waits forever on something that never happens
  (an `Event` nobody sets) does not raise `SimulationDeadlock`; the clock advances from shift
  change to shift change and the run never ends, using CPU as it goes. `SimulationDeadlock` is
  raised only once `schedule.changes` has ended (D053), for example for a schedule with no
  shifts. Give such a run an end: `asyncio.timeout(...)`, or `main` returning.
- **Precision.** `datetime` has microsecond resolution, so `to_datetime` rounds to a microsecond,
  and a float sim time has limited resolution when it is large.
