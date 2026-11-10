"""Smoke-test the example models in examples/ (EX-01, D055).

Each script runs in its own process, as a user would run it. Seeds are fixed, so a few
key lines of the output are checked exactly.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"

KEY_LINES = {
    "bank.py": [
        "customers served: 3000",
        "mean wait:      4.28   (theory 4.00)",
        "time with 0 in system: 0.169   (theory 0.200)",
        "20 replications: mean wait 3.56 +- 0.67 (95% interval)",
    ],
    "machine_shop.py": ["preemptions: 188", "maintenance jobs finished: 163"],
    "inventory.py": ["orders placed: 66", "fraction of time at zero stock: 0.110"],
    "clinic.py": [
        "Thu 04 Jun 2026      62        0          0",  # the closed day: nobody treated
        "week total          348      202          5",
    ],
}


@pytest.mark.parametrize("name", KEY_LINES)
def test_example_runs_and_prints_key_lines(name: str) -> None:
    done = subprocess.run(
        [sys.executable, str(EXAMPLES / name)],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        timeout=60,
    )
    assert done.returncode == 0, done.stderr
    lines = done.stdout.splitlines()
    for expected in KEY_LINES[name]:
        assert expected in lines
