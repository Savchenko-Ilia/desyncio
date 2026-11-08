# AGENTS.md

Context for AI agents and contributors working on **desyncio**.
Read this before making design or code changes. The build workflow for agents is in `CLAUDE.md`;
the backlog is in `docs/features.md`; decisions are logged in `docs/decisions.md`;
how each feature was built is recorded in `docs/implementation-log.md`.

---

## 1. What desyncio is

An async/await-native **discrete-event simulation (DES) library** for Python.
Simulation processes are plain `async def` coroutines running on a real asyncio event loop
whose clock is virtual. Standard asyncio (`sleep`, `Queue`, `Lock`, `Semaphore`, `TaskGroup`,
`timeout()`) works in simulated time.

Scope, in order:
1. **Virtual-time event loop**: deterministic, as fast as possible or scaled to real time.
2. **Primitives**: Resource, Container, Store, timeouts, interrupts, built on the loop.

The library has no dependency on any web, LLM, or ML framework. Keep it that way.

---

## 2. Principles

1. **Async-native.** Processes are coroutines on a real asyncio loop running in virtual time.
   SimPy is the reference for *concepts and API ergonomics* (Resource, Container, Store,
   interrupts), not for implementation (SimPy is generator-based and does not use asyncio).
2. **Determinism is mandatory.**
   - Seeded RNG streams per component.
   - Explicit tie-breaking for events scheduled at the same time. Never rely on incidental
     asyncio callback or heap ordering.
   - The same model + seed produces an identical trace. This is a tested invariant.
3. **Two run modes from one mechanism.**
   - *As fast as possible*: the clock jumps to the next scheduled event.
   - *Scaled real time*: `real_dt = virtual_dt / speed`, with speed changes and pause/resume.
4. **Observable, not opinionated.** The loop exposes a trace/event hook; statistics, logging and
   visualisation are consumers built on top, not part of the core.
5. **No real I/O inside a simulation.** Sockets, subprocesses and executors are refused with a
   clear error.

---

## 3. Virtual-time loop notes

- Subclass `asyncio.SelectorEventLoop`, override `time()` to return the sim clock, and advance the
  clock to the next timer when nothing is ready.
- Install the loop with `asyncio.Runner(loop_factory=...)`, not event loop policies
  (deprecated since 3.14).
- `nolar/looptime` (a pytest fake-time plugin) may help in tests, but the engine uses its own loop.
- Code copied or adapted from CPython (e.g. `base_events.py`) is under the PSF license: keep its
  notice in `LICENSES/PSF.txt`, mark the copied part with a comment, and flag it in the PR.
  Subclassing and overriding without copying needs nothing extra.

---

## 4. Tech stack

- **Python 3.13+**, `asyncio`. Use newer asyncio features freely: `Queue.shutdown()`,
  `as_completed()` as an async iterator, current `TaskGroup`/cancellation behaviour.
- `src/` layout, `pyproject.toml`, `[dev]` extra.
- pytest, pytest-asyncio, hypothesis for property tests; ruff; mypy.
- Runtime dependencies: none, unless agreed with the owner.

---

## 5. Working conventions

- **The owner is using this project to learn asyncio internals.** Prefer readable, idiomatic
  asyncio over clever abstractions. Explain non-obvious async behaviour (scheduling order,
  cancellation, task lifetimes, loop internals) in comments, docstrings or PR descriptions.
- Every feature ships with tests; anything touching scheduling also gets a determinism test.
- Type hints everywhere; public APIs have docstrings.
- Don't add dependencies or change the principles in section 2 without flagging it to the owner.
- When a decision here changes, update this file in the same change.

---

## 6. Decisions

- Name: **desyncio** (PyPI and GitHub).
- License: **Apache-2.0**.
- Python 3.13+.

## 7. Open questions

- Base time unit (seconds vs configurable) and calendar/shift handling.
- Public API shape for primitives (SimPy-like `request()` vs asyncio-like `async with resource`).
