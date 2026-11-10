"""A week in a walk-in clinic, on a real calendar.

What it models: patients walk in at any hour, 24 hours a day, 7 days a week, about two
per hour. Two doctors work on weekdays from 09:00 to 17:00 and one on Saturday from
09:00 to 13:00. Sunday is closed, and so is Thursday 4 June (say, a holiday). When a
shift ends, patients still in treatment are sent home and queue up again when the
clinic reopens, to finish the rest of their treatment. Results are printed per local
date.

The clinic is overloaded on purpose: patients keep arriving around the clock while
the doctors work limited hours, with Thursday and Sunday closed, so a large queue
builds up over the week. That backlog is the point: compare the "treated" column on
the closed days with the busy days after them, and look at how many patients are
still waiting at the end.

desyncio features shown: `Calendar` (Europe/Berlin, real start date, one sim time
unit = one minute), `Shift` and `Schedule` (weekly pattern, a closed date),
`ScheduledResource` (capacity follows the schedule), `Interrupt` with `ShiftEnded`,
`sim.random`, `sim.spawn`.

How to run it: uv run python examples/clinic.py   (takes well under a second)
"""

from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from desyncio import (
    Calendar,
    Interrupt,
    Schedule,
    ScheduledResource,
    Shift,
    ShiftEnded,
    run,
    sim,
)

berlin = ZoneInfo("Europe/Berlin")
cal = Calendar(datetime(2026, 6, 1, tzinfo=berlin), unit=timedelta(minutes=1))  # Monday
schedule = Schedule(
    cal,
    [
        Shift(range(5), time(9), time(17), 2),  # Monday to Friday: two doctors
        Shift([5], time(9), time(13), 1),  # Saturday morning: one doctor
    ],
    closed=[date(2026, 6, 4)],  # no shift starts on this Thursday
)
WEEK_END = cal.to_sim(datetime(2026, 6, 8, tzinfo=berlin))  # next Monday, 00:00

Days = defaultdict[date, Counter[str]]  # per local date: "arrived", "treated", ...


async def patient(clinic: ScheduledResource, days: Days, treatment_time: float) -> None:
    days[cal.now().date()]["arrived"] += 1
    remaining = treatment_time
    while remaining > 0:
        started: float | None = None  # stays None if we never get as far as the body
        try:
            async with clinic:  # waits (possibly over a night) for a free doctor
                started = sim.now
                await sim.timeout(remaining)
                remaining = 0
        except Interrupt as exc:
            # The shift change takes the doctor away and the `async with` raises the
            # Interrupt, so we catch it outside the block; the unit is already freed.
            if not isinstance(exc.cause, ShiftEnded):
                raise  # some other interrupt: not ours to handle
            # A doctor can also be granted to us while we wait and taken back by the
            # shift end before our task resumes: then `async with` itself raises (in
            # `__aenter__`), `started` stays None and nothing is deducted. Such a
            # patient is still counted as sent home, although treatment never began.
            if started is not None:
                remaining -= sim.now - started  # keep the work already done
            days[cal.to_datetime(exc.cause.at).date()]["sent home"] += 1
            # Loop again: queue for the next shift with the remaining time.
    days[cal.now().date()]["treated"] += 1


async def week() -> tuple[Days, int]:
    # Created inside the run: it starts a background task that applies shift changes.
    clinic = ScheduledResource(schedule)
    days: Days = defaultdict(Counter)
    arrivals, treatment = sim.random("arrivals"), sim.random("treatment")
    while True:
        gap = arrivals.expovariate(1 / 30)  # about one patient per 30 minutes
        if sim.now + gap >= WEEK_END:
            break
        await sim.timeout(gap)
        sim.spawn(patient(clinic, days, treatment.expovariate(1 / 15)))  # 15 min mean
    await sim.timeout(WEEK_END - sim.now)  # let the rest of the week run
    # Read this before returning: the run cancels patients still queued when it ends.
    return days, clinic.queue_length


def main() -> None:
    days, waiting = run(week(), seed=11)
    print("date          arrived  treated  sent home")
    total: Counter[str] = Counter()
    for day in sorted(days):
        c = days[day]
        total.update(c)
        line = f"{c['arrived']:>6}  {c['treated']:>7}  {c['sent home']:>9}"
        print(f"{day:%a %d %b %Y}  {line}")
    line = f"{total['arrived']:>6}  {total['treated']:>7}  {total['sent home']:>9}"
    print(f"week total       {line}")
    print(f"still waiting for a doctor at the end of the week: {waiting}")


if __name__ == "__main__":
    main()
