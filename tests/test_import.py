"""Smoke test: the package imports and exposes a version string."""

import desyncio


def test_import_exposes_version() -> None:
    assert isinstance(desyncio.__version__, str)
    assert desyncio.__version__
