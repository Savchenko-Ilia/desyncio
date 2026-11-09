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


def test_star_import_yields_all() -> None:
    namespace: dict[str, object] = {}
    exec("from desyncio import *", namespace)
    namespace.pop("__builtins__")
    assert set(namespace) == set(desyncio.__all__)


def _run_python(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code],
        env={**os.environ},
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize("module", SUBMODULES)
def test_submodule_imports_first(module: str) -> None:
    result = _run_python(f"import desyncio.{module}; import desyncio")
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("module", SUBMODULES)
def test_no_from_desyncio_import_sim(module: str) -> None:
    """`from desyncio import sim` can yield the submodule; use `desyncio.sim`."""
    path = Path(desyncio.__file__).parent / f"{module}.py"
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module == "desyncio":
            assert "sim" not in [a.name for a in node.names], path
