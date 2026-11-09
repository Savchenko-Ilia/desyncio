"""The determinism invariant (AGENTS.md principle 2, LOOP-10).

The same model with the same seed must produce an identical trace and
identical results: in one process, and across processes whatever
``PYTHONHASHSEED`` is. The model is the reference M/M/1 queue in
``tests/models/mm1.py``.
"""

import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

import desyncio
from desyncio.trace import ListSink, TraceRecord
from models.mm1 import CustomerRecord, mean_time_in_system, mm1

N = 10_000
SEED = 42
TESTS_DIR = Path(__file__).parent

Outcome = tuple[list[CustomerRecord], list[TraceRecord]]


def run_mm1(seed: int, n: int = N) -> Outcome:
    sink = ListSink()
    records = desyncio.run(mm1(n_customers=n), seed=seed, trace=sink)
    return records, sink.records


@pytest.fixture(scope="module")
def baseline() -> Outcome:
    return run_mm1(SEED)


def test_same_seed_gives_identical_trace_and_results(baseline: Outcome) -> None:
    records, trace = run_mm1(SEED)
    assert len(trace) > N  # a real trace, not an empty one
    assert trace == baseline[1]
    assert records == baseline[0]


def test_different_seed_gives_different_trace(baseline: Outcome) -> None:
    records, trace = run_mm1(SEED + 1)
    assert trace != baseline[1]
    assert records != baseline[0]


def test_model_is_a_real_mm1_queue(baseline: Outcome) -> None:
    records = baseline[0]
    assert len(records) == N
    # Theory: 1 / (mu - lambda) = 10. The seed fixes the outcome, so this
    # is deterministic; the band is wide because M/M/1 at rho=0.9 is noisy.
    assert 6.0 < mean_time_in_system(records) < 14.0
    # FIFO, single server: service starts and departures follow arrival
    # order, and service never overlaps.
    for prev, cur in zip(records, records[1:], strict=False):
        assert prev.arrival <= cur.arrival
        assert cur.service_start >= prev.departure
    assert all(r.arrival <= r.service_start <= r.departure for r in records)


_CHILD = """
import hashlib, sys
sys.path.insert(0, sys.argv[1])
import desyncio
from desyncio.trace import ListSink
from models.mm1 import mm1

sink = ListSink()
main = mm1(n_customers=int(sys.argv[3]))
records = desyncio.run(main, seed=int(sys.argv[2]), trace=sink)
print(hashlib.sha256(repr(sink.records).encode()).hexdigest())
print(hashlib.sha256(repr(records).encode()).hexdigest())
"""


@pytest.mark.parametrize("hashseed", ["0", "12345"])
def test_same_trace_in_another_process_with_another_hashseed(
    baseline: Outcome, hashseed: str
) -> None:
    env = {**os.environ, "PYTHONHASHSEED": hashseed}
    out = subprocess.run(
        [sys.executable, "-c", _CHILD, str(TESTS_DIR), str(SEED), str(N)],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    records, trace = baseline
    expected = [hashlib.sha256(repr(x).encode()).hexdigest() for x in (trace, records)]
    assert out.stdout.split() == expected
