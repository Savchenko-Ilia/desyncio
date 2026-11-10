# Examples

Five complete models, each commented for learners. Run one from the repository root:

```
uv run python examples/<name>.py
```

Each prints its results and finishes in about two seconds or less. The seeds are fixed, so the output
is the same every time.

- `bank.py`: a one-teller bank (M/M/1 queue) with `Resource`, `Tally` and `TimeSeries`, compared with queueing theory, then `replicate` and a 95% `confidence_interval`.
- `machine_shop.py`: machines that break down at random and repairmen as a `PreemptiveResource`; urgent repairs preempt routine maintenance (`Interrupt`, `Preempted`).
- `inventory.py`: a stock `Container` with random daily demand, an (s, S) reorder policy and a delivery lead time; stock-outs and `time_fractions`.
- `clinic.py`: a week in a Berlin clinic with a `Calendar`, a weekly `Schedule`, a `ScheduledResource`, and patients interrupted at shift end (`ShiftEnded`).
- `recorder.py`: a `ColumnarRecorder` subscribed with `run(subscribers=[...])` keeps every event in columns; a desk with two clerks emits its own entity events (`sim.emit`), and the waits (mean, max, 90% quantile) and the clerks' utilisation (from `ResourceChanged`) are computed from the stream with numpy. See the [observing guide](../docs/observing.md).
