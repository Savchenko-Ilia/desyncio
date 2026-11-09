"""Public API: exports, identity with defining modules, no import cycles."""

import ast
import importlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

import desyncio

# name -> defining submodule (None for names defined in the package itself)
EXPORTS: dict[str, str | None] = {
    "run": "runners",
    "sim": "sim",
    "__version__": None,
    "Resource": "resource",
    "PriorityResource": "resource",
    "PreemptiveResource": "resource",
    "Preempted": "resource",
    "Container": "container",
    "Store": "store",
    "FilterStore": "store",
    "PriorityStore": "store",
    "Interrupt": "interrupt",
    "TimeSeries": "monitor",
    "RealIOError": "guards",
    "SimulationDeadlock": "loop",
    "RealTimePacer": "realtime",
}

SUBMODULES = sorted(
    p.stem for p in Path(desyncio.__file__).parent.glob("*.py") if p.stem != "__init__"
)


def test_import_exposes_version() -> None:
    assert isinstance(desyncio.__version__, str)
    assert desyncio.__version__


def test_all_is_exact() -> None:
    assert set(desyncio.__all__) == set(EXPORTS)
    assert len(desyncio.__all__) == len(set(desyncio.__all__))


@pytest.mark.parametrize("name", sorted(EXPORTS))
def test_name_is_attribute_and_identical(name: str) -> None:
    assert hasattr(desyncio, name)
    module = EXPORTS[name]
    if module is not None:
        original = getattr(importlib.import_module(f"desyncio.{module}"), name)
        assert getattr(desyncio, name) is original


def _run_python(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code],
        env={**os.environ},
        capture_output=True,
        text=True,
        check=False,
    )


def test_import_desyncio_succeeds_in_a_fresh_interpreter() -> None:
    """An import cycle fails here by name, not as a collection error."""
    result = _run_python("import desyncio")
    assert result.returncode == 0, result.stderr


def test_no_from_desyncio_import_x() -> None:
    """Inside the package, `from desyncio import X` is forbidden: it can yield a
    submodule instead of the object, and it risks cycles. Import from the
    defining submodule (`from desyncio.sim import sim`)."""
    for module in SUBMODULES:
        path = Path(desyncio.__file__).parent / f"{module}.py"
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and node.level == 0:
                assert node.module != "desyncio", f"{path}: {ast.unparse(node)}"
