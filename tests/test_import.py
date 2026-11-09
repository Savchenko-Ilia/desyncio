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
    "Tally": "tally",
    "replicate": "experiment",
    "confidence_interval": "experiment",
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
    defining submodule (`from desyncio.sim import sim`). The relative form
    `from . import X` is the same import and is forbidden too; `from .sim
    import sim` (a module after the dot) is fine."""
    for module in SUBMODULES:
        path = Path(desyncio.__file__).parent / f"{module}.py"
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.level == 0:
                assert node.module != "desyncio", f"{path}: {ast.unparse(node)}"
            else:
                assert node.module is not None, f"{path}: {ast.unparse(node)}"


def test_numpy_and_scipy_import() -> None:
    """Module 3 (statistics) depends on numpy >= 2 and scipy (D047, D048)."""
    import numpy
    import scipy.stats

    assert int(numpy.__version__.split(".")[0]) >= 2
    assert scipy.stats.norm.cdf(0.0) == 0.5


def test_import_desyncio_does_not_import_numpy_or_scipy() -> None:
    """The core never needs numpy or scipy; only the statistics code imports them."""
    result = _run_python(
        "import sys, desyncio; "
        "bad = [m for m in ('numpy', 'scipy') if m in sys.modules]; "
        "assert not bad, bad"
    )
    assert result.returncode == 0, result.stderr
