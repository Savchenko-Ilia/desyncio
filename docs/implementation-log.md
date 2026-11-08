# Implementation log

How each feature was actually built: approach, asyncio internals involved, trade-offs,
surprises, and what to remember next time. Newest entry at the bottom.
`docs/features.md` says *what* is done; `docs/decisions.md` says *what was decided*;
this file says *how it works and why*.

## Entry template

```
## <ID> — <feature name> (<YYYY-MM-DD>, batch <n>)
Branch: feat/<ID> (+ fix/<ID>-<n>)  ·  Merged: <commit sha>

**Approach.** What was built and how, in a few sentences.

**asyncio internals.** Loop methods, scheduling order, cancellation or task-lifetime
behaviour this relies on, with links to CPython source where useful.

**Trade-offs.** Alternatives considered and why this one won.

**Review findings.** MUST findings from the reviewer and how they were fixed.

**Gotchas.** Anything surprising that a future contributor should know.

**Follow-ups.** Known gaps, TODOs, ideas (link issues or feature IDs).
```

---

## PKG-01 — Package skeleton (uv) (2026-11-08, batch 1)
Branch: feat/PKG-01  ·  Merged: 186a18c

**Approach.** I added a `src/`-layout package. `pyproject.toml` uses the `uv_build` backend, pinned `>=0.11,<0.12` to match uv 0.11.32. It sets `requires-python = ">=3.13"` and `license = "Apache-2.0"` with `license-files = ["LICENSE"]`. It has no runtime dependencies. The dev tools sit in `[dependency-groups] dev`. The resolved versions are pytest 9.2.0, pytest-asyncio 1.4.1.post1, hypothesis 6.170.15, ruff 0.17.4 and mypy 2.4.1. `.python-version` is `3.13`. `uv.lock` is committed and resolves from PyPI only. `src/desyncio/__init__.py` exposes `__version__ = "0.1.0"`. An empty `py.typed` marker ships, so type checkers in downstream code use our hints (PEP 561).

**asyncio internals.** None yet. pytest-asyncio runs with `asyncio_mode = "auto"`, so every `async def test_*` runs without a marker. It also sets `asyncio_default_fixture_loop_scope = "function"`, so each test gets a fresh loop and no state leaks between tests. Determinism tests need that isolation. Once LOOP-02 exists, tests will need to run on the virtual-time loop. They can do that through `desyncio.run(...)` or a loop-factory fixture. That is decided in LOOP-02/LOOP-04.

**Trade-offs.** `uv_build` was chosen over hatchling because it needs no extra tool and uv is the mandated toolchain. The narrow pin means the backend needs bumping when uv moves to 0.12. mypy runs with `strict = true`. ruff selects E, F, I, B, UP and ASYNC; ASYNC catches blocking calls inside coroutines (see D002).

**Review findings.** PASS, no MUST findings. SHOULD: (1) bump the `uv_build` pin with uv; (2) `__version__` is duplicated in `pyproject.toml` and `__init__.py`, so derive one from the other or test them with `importlib.metadata`.

**Gotchas.** The cloud sandbox prints a harmless `UV_NATIVE_TLS` deprecation warning on every uv command. `uv sync` fails until `src/desyncio/__init__.py` exists. That failure was the "red" step of the test.

**Follow-ups.** Single-source the version. Bump the `uv_build` pin.
