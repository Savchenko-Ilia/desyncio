# Features — Module 1: virtual-time event loop

Source of truth for scope and rules: `AGENTS.md`.
Package name: `desyncio`.
Status: `todo` · `in_progress` · `done` · `blocked`

| ID | Feature | Acceptance criteria | Owns files | Depends on | Status |
|----|---------|---------------------|------------|------------|--------|
| PKG-01 | Package skeleton (uv) | `pyproject.toml` with `requires-python = ">=3.13"`, `license = "Apache-2.0"`, build backend `uv_build` (or hatchling), dev tools in `[dependency-groups] dev` (pytest, pytest-asyncio, hypothesis, ruff, mypy); ruff `target-version = "py313"`, mypy `python_version = "3.13"`; `.python-version` = `3.13`; `uv.lock` committed. `uv sync --locked`, `uv run pytest -q`, `uv run ruff check .`, `uv run mypy src` all pass on an empty package | `pyproject.toml`, `uv.lock`, `.python-version`, `src/desyncio/__init__.py`, `tests/test_import.py` | — | done |
| LOOP-01 | Virtual clock | `VirtualClock` holds sim time (float, base unit documented); `advance_to(t)` refuses to go backwards with a clear error; starts at 0 or a given start time | `src/desyncio/clock.py`, `tests/test_clock.py` | PKG-01 | done |
| CI-01 | CI on pull requests | GitHub Actions workflow triggered on `pull_request` only (no `push` triggers, to save minutes) plus `workflow_dispatch`; one job on `ubuntu-latest`, Python 3.13 via uv; runs the same steps as `scripts/gate.env` (`uv sync --locked`, `uv run pytest -q`, `uv run ruff check .`, `uv run mypy src`); `concurrency` cancels superseded runs; uv cache enabled; read-only `permissions` | `.github/workflows/ci.yml` | PKG-01 | todo |
| LOOP-01b | Clock input hardening | `VirtualClock` start/`advance_to` raise `TypeError` for non-real inputs (`str`, `bytes`, `bool`, `None`, complex); ints and floats accepted; ints too large for float raise `ValueError`, not `OverflowError`; `-0.0` normalised to `0.0`; hypothesis property covers the backwards path (raises, clock unchanged) | `src/desyncio/clock.py`, `tests/test_clock.py` | LOOP-01 | todo |
| LOOP-02 | Virtual-time event loop | Subclass of `asyncio.SelectorEventLoop`; `loop.time()` returns the sim clock; when no callbacks are ready, the clock jumps to the earliest timer. `await asyncio.sleep(3600)` completes at t=3600 in under 50 ms of real time | `src/desyncio/loop.py`, `tests/test_loop.py` | LOOP-01 | todo |
| LOOP-03 | Deterministic tie-breaking | Timers at the same sim time fire in order `(time, priority, sequence)`, never in incidental heap order. 1,000 tasks sleeping to the same t wake in creation order; hypothesis test over random schedules gives identical order across runs | `src/desyncio/scheduling.py`, `tests/test_ordering.py` | LOOP-02 | todo |
| LOOP-04 | Run entry point | `desyncio.run(main(), until=None, seed=0)` creates the loop via `asyncio.Runner(loop_factory=...)` (no event loop policies), runs to completion or `until`, closes cleanly, returns `main`'s result; `until` stops with pending tasks left pending (documented) | `src/desyncio/run.py`, `tests/test_run.py` | LOOP-02 | todo |
| LOOP-05 | Sim facade | `sim.now`, `await sim.timeout(dt)`, `sim.spawn(coro, name=...)` returns an awaitable task whose result and exception propagate; names appear in traces | `src/desyncio/sim.py`, `tests/test_sim.py` | LOOP-04 | todo |
| LOOP-06 | asyncio compatibility suite | In virtual time: `asyncio.Queue` (including `shutdown()`), `Lock`, `Event`, `Semaphore`, `gather`, `TaskGroup`, `as_completed` (async iteration), `wait_for`, `asyncio.timeout()` behave as documented; cancellation lands at the right sim time | `tests/test_asyncio_compat.py` | LOOP-03, LOOP-05 | todo |
| LOOP-07 | No real I/O guard | `add_reader`, socket ops, subprocess and `run_in_executor` raise `RealIOError` with a message explaining that simulations must not do real I/O | `src/desyncio/guards.py`, `tests/test_guards.py` | LOOP-02 | todo |
| LOOP-08 | Seeded RNG streams | `sim.rng("arrivals")` returns a `random.Random` derived from master seed + stream name via a stable hash (not Python `hash()`); same seed+name → same sequence across processes and runs | `src/desyncio/rng.py`, `tests/test_rng.py` | PKG-01 | done |
| LOOP-09 | Trace hook | Loop records `(t, seq, task_name, kind)` for task start/resume/finish/cancel into an optional sink; off by default; zero cost when off (benchmarked) | `src/desyncio/trace.py`, `tests/test_trace.py` | LOOP-05 | todo |
| LOOP-10 | Determinism invariant | Reference model (M/M/1 queue, 10k customers) run twice with the same seed → identical trace; different seed → different trace. Runs in CI on every PR | `tests/test_determinism.py`, `tests/models/mm1.py` | LOOP-06, LOOP-08, LOOP-09 | todo |
| LOOP-11 | Scaled real-time mode | `speed` factor: `real_dt = virtual_dt / speed`; change speed and pause/resume mid-run; injectable wall clock so tests don't sleep; `speed=inf` equals fast mode | `src/desyncio/realtime.py`, `tests/test_realtime.py` | LOOP-04 | todo |
| LOOP-12 | Benchmark | Script comparing events/sec on M/M/1 vs SimPy and asimpy; results in `bench/README.md`. Informative only, not a gate | `bench/mm1_bench.py`, `bench/README.md` | LOOP-10 | todo |
| DOC-01 | How the loop works | Explainer of `_run_once`, timer heap, why ties need explicit ordering, what "jumping the clock" means; linked from README | `docs/how-the-loop-works.md`, `README.md` | LOOP-03 | todo |

## Suggested batches
1. PKG-01
2. LOOP-01, LOOP-08
3. LOOP-02
4. LOOP-03, LOOP-04, LOOP-07
5. LOOP-05, LOOP-11, DOC-01
6. LOOP-06, LOOP-09
7. LOOP-10, then LOOP-12

