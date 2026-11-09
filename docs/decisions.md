# Decisions

One entry per decision the lead or the human made. Newest last.

## D001 — Process
Features are built by `implementer` subagents in worktrees, audited by a read-only
`reviewer`, fixed by `fixer`, gated by `scripts/gate.sh`, merged into `integration`
by the lead. Only the human merges `integration` into `main`.

## D002 — PKG-01 tooling choices (batch 1)
- Build backend: `uv_build>=0.11,<0.12`, not hatchling.
- mypy `strict = true`; ruff lint rules `E, F, I, B, UP, ASYNC` (default line length 88). These go beyond the spec; the implementer added them and the lead accepted them. ASYNC flags blocking calls in coroutines, which matters for a virtual-time loop.
- pytest-asyncio `asyncio_mode = "auto"`, function-scoped loops.
- `src/desyncio/py.typed` was added in addition to PKG-01's owned files (a PEP 561 marker inside the package dir).

## D003 — Integration branch in cloud sessions
This cloud session pushes the session branch `claude/setup-batch-1-5bb5vp`. Per `docs/agent-workflow.md`, the lead merges into local `integration` and then fast-forwards the session branch to it. Both carry the same history. The owner opens or merges the PR to `main`.

## D004 — Batch 2 scope calls (lead)
- **Time unit:** `VirtualClock` time is a unitless float, documented as "seconds by convention". The owner said "go" without choosing between this and a configurable unit, so the lead applied its proposed default. Resolves the first half of AGENTS.md §7 open question 1 provisionally; calendar/shift handling stays open. Revisit if the owner prefers otherwise.
- **LOOP-08 split:** the `sim` facade does not exist yet, so LOOP-08 delivered standalone `RngStreams`/`derive_seed`. LOOP-05 must expose them as `sim.rng(name)`, backed by `RngStreams(seed)` with the seed from `desyncio.run(..., seed=)`.
- **RNG encoding v1** (SHA-256, domain tag `desyncio.rng.v1`, length-prefixed seed, UTF-8 name) is now a compatibility contract. Changing it means a new domain tag.
- **SHOULD findings not fixed:** both reviews were PASS. Per protocol, `fixer` handles only MUST findings. The SHOULDs are recorded in the implementation log for the owner to schedule.

## D005 — CI-01 and LOOP-01b added (owner approved)
- **CI-01:** GitHub Actions runs on `pull_request` (+ manual `workflow_dispatch`) only, never on `push`. The owner asked for this to save minutes. One Ubuntu job on Python 3.13; superseded runs are cancelled. It uses the `astral-sh/setup-uv` action, which is CI tooling, not a project dependency.
- **LOOP-01b:** the LOOP-01 review SHOULDs on input handling, promoted to a feature. It runs in batch 3 alongside LOOP-02. The two share no files, and LOOP-01b leaves the clock's public API unchanged (validation only).

## D006 — LOOP-02: deadlock semantics and threads (lead, batch 3)
- When nothing is ready, no timer is scheduled and no fd is ready, `VirtualTimeLoop` raises `SimulationDeadlock` immediately instead of blocking.
- Consequence: a wake-up from a real thread (`call_soon_threadsafe` from a worker, `to_thread`, `run_in_executor`) is not waited for. It deadlocks, or, if a timer is pending, virtual time jumps past it.
- The lead accepted this under principle 5 (no real I/O in a simulation); LOOP-07 will refuse executors explicitly. The owner may overrule. Supporting threads would need a real-time wait, which breaks determinism.
- The selector-wrapping approach was chosen over overriding `_run_once`, to avoid copying CPython code (no PSF notice needed).

## D007 — File ownership gap for LOOP-03 and LOOP-07 (open, for the owner)
LOOP-03 (tie-breaking) has to change how timers are ordered. That lives in `VirtualTimeLoop` (`call_at` / `_scheduled`). LOOP-07 (I/O guards) has to override loop methods (`add_reader`, `sock_*`, `subprocess_*`, `run_in_executor`). Both are listed as owning only their own new files, but both realistically need `src/desyncio/loop.py`. The suggested batch 4 (LOOP-03, LOOP-04, LOOP-07) therefore overlaps on `loop.py`. Owner decision pending.

## D007 — resolved (owner said "go", batch 4)
- LOOP-03 and LOOP-07 also own `src/desyncio/loop.py` and `tests/test_loop.py`. They therefore run in different batches: LOOP-03 in batch 4, LOOP-07 in batch 5.
- LOOP-03 absorbs the LOOP-02 review SHOULDs on scheduling: the clock-resolution window, tests for overdue and mid-heap-cancelled timers, and a robust wall-time test.
- LOOP-07 absorbs the thread/executor ones: an honest test and docstring for cross-thread wake-ups. D006 was confirmed by the owner.

## D008 — Batch 4 spec details (lead; owner may overrule)
- **Priority source (LOOP-03):** the spec orders timers by `(time, priority, sequence)` but doesn't say where priority comes from. Default priority is `0` for all stock asyncio timers (`sleep`, `call_later`, `call_at`, `timeout`). Lower values fire first. A non-default priority is set through an explicit loop method (named by the implementer and documented), not through `call_at` kwargs. `sequence` is a per-loop monotonically increasing counter assigned at scheduling time.
- **`until` semantics (LOOP-04):** timers due at `t <= until` run; nothing runs at virtual time `> until`, including during cleanup. If `main` finishes first, `run` returns its result. If `until` is reached first, `run` returns `None` (as SimPy's `env.run(until=...)` does), and other tasks are left unfinished, as documented. What happens to them during loop close is the implementer's to document and test, under the constraint that the clock never passes `until`.
- **`seed` (LOOP-04):** `run` validates `seed` with the same rules as `RngStreams`. It makes an `RngStreams(seed)` available to the running simulation by a documented mechanism, chosen by the implementer, that LOOP-05 can build `sim.rng()` on. It does not edit `loop.py`.
- LOOP-04 may edit `src/desyncio/__init__.py`, only to export `run`, so that `desyncio.run(...)` works.

## D009 — LOOP-03: adapted CPython code, fixed with a PSF notice (lead, batch 4)
`VirtualTimeLoop._schedule` must re-create `BaseEventLoop.call_at`, because stock `call_at` hardcodes `events.TimerHandle`. The reviewer judged it adapted code under AGENTS.md §3. The lead chose the §3 route over an awkward rewrite to dodge it:
- `LICENSES/PSF.txt` added (the fixer was allowed this one file outside the owned list);
- marking comments added on the adapted parts.

The `integration` → `main` PR must flag this. Open follow-up: `pyproject.toml` `license-files` must include `LICENSES/*` so the notice ships in wheels.

## D010 — Batch 5 scope (owner said "go" without answering, so the lead's recommendations apply; owner may overrule)
- Rename `run.py` → `runners.py`, done as LOOP-04b in batch 5. The `desyncio.run` function shadows a submodule of the same name: `mock.patch("desyncio.run.X")` silently patches the function, and `import desyncio.run as m` returns the function. LOOP-04b also adds the `run` overloads, the `license-files` fix from D009, and the infinite-background-task test.
- LOOP-11 (real-time mode) also owns `loop.py`/`tests/test_loop.py`, because scaled real time changes how the loop waits. It runs in batch 6, after LOOP-07. LOOP-05 and LOOP-11 now also depend on LOOP-04b, so they build on `runners.py`.
- Batch 5 = LOOP-07, LOOP-04b, DOC-01 (no file overlap).

## D006 — amended (batch 5)
LOOP-07 is merged. `run_in_executor`, `set_default_executor` and therefore `asyncio.to_thread` now raise `RealIOError`, so the deadlock/time-jump consequence above applies only to:
- a `threading.Thread` the user starts, which then calls `call_soon_threadsafe`;
- a `concurrent.futures` executor used directly with `asyncio.wrap_future`. This is a known, documented gap and is not refused.

## D011 — Lead promoted a SHOULD to MUST on DOC-01 (batch 5)
The DOC-01 reviewer rated as SHOULD that §4 still said `to_thread` and `run_in_executor` deadlock and "LOOP-07 will refuse executors". LOOP-07 had already been merged into `integration`, so the merged doc would have stated something false, and the lead made it a MUST for fix round 1. Rule going forward: a docs claim that is false on the current `integration` is a MUST.

## D012 — Batch 6 scope (owner said "go" to the lead's recommendations)
- **`sim` shape (LOOP-05):** `from desyncio import sim`. `sim` is a module-level singleton `Sim` object whose attributes read the *running* loop on every access, the way asyncio's free functions (`asyncio.sleep`, `asyncio.create_task`) do. Outside a running `VirtualTimeLoop` it raises a clear `RuntimeError`. Nothing is passed into `main`.
  - `sim.now` is the virtual time.
  - `await sim.timeout(dt)` waits `dt` of virtual time.
  - `sim.spawn(coro, name=...)` returns an `asyncio.Task`.
  - `sim.rng(name)` builds on `runners.current_rng_streams()`.
  - Default task names must be deterministic per run. asyncio's own `Task-N` counter is process-global, so two runs in one process would differ.
- **LOOP-11 ownership:** besides `realtime.py` and `loop.py`, it owns:
  - `runners.py` and `tests/test_run.py`, for the `speed` / pacer argument of `desyncio.run`;
  - `docs/how-the-loop-works.md`, for a real-time section and any claims its loop changes make stale (D011).
  It also takes the LOOP-07 SHOULD: the D006 test should use a `threading.Event` gate, with no real sleep and no wall-clock bound.
- LOOP-05 and LOOP-11 share no files. `__init__.py` belongs to LOOP-05 only. LOOP-11 adds no export; its public names are reached via `desyncio.realtime`, and can be exported later.
- PR #2 (`integration` → `main`) was opened at the owner's request after batch 5.

## D013 — CI made manual-only (owner request)
The owner has no GitHub Actions credits. The two runs on PR #2 failed within ~3 s without running any steps. CI-02 removes the `pull_request` trigger and keeps only `workflow_dispatch`. `scripts/gate.sh` remains the verification for every merge. LOOP-10's "runs in CI on every PR" becomes "runs in `scripts/gate.sh`". Restore the `pull_request` trigger when credits are available.

## D014 — LOOP-11 fix round: ownership exception and promoted SHOULD (lead, batch 6)
- The fixer was allowed to edit the `src/desyncio/guards.py` module docstring only, to fix the "never waits" claim LOOP-11 had made false (D011). That file is owned by LOOP-07.
- The lead promoted the reviewer's SHOULD "failed `pacer.bind` leaks a half-built loop (ResourceWarnings)" to a MUST: it is a resource leak on an error path, and small to fix.

## D006 — amended again (batch 6, LOOP-11)
In real-time mode, a paused pacer waits in real time with no timeout until `resume()`, which comes from another thread or a wall-clock hook. That holds even with no timers. Unpaused, the deadlock rule is unchanged: no timers and nothing ready raises `SimulationDeadlock`, and real-time mode does not wait for threads either.

## D015 — Batch 7 scope (owner said "go" to the lead's recommendations)
- **LOOP-09 ownership:** besides `trace.py`/`test_trace.py`, LOOP-09 owns:
  - `loop.py` and `test_loop.py`, to install the task factory and hooks;
  - `sim.py` and `test_sim.py`, so `sim.spawn` shares the loop's naming;
  - `runners.py` and `test_run.py`, for the `trace=` argument of `desyncio.run` and the main task's name;
  - `docs/how-the-loop-works.md`, for a tracing section and a short "What desyncio reuses from asyncio" section (asked by the owner), plus any claims LOOP-09 makes stale (D011).
  LOOP-06 owns only `tests/test_asyncio_compat.py`, so there is no overlap.
- **Deterministic names for every task:** a task created on a `VirtualTimeLoop` without an explicit name gets `proc-N` from one per-loop counter. That covers `asyncio.create_task`, `TaskGroup` and `sim.spawn`. The main task created by `desyncio.run` is named `main`. Explicit names are kept. Naming applies whether or not tracing is on.
- **Trace record:** `(t, seq, task_name, kind)`, with `kind` one of `start`, `resume`, `finish`, `cancel` (spec). A task that ends with an exception records `finish`. `seq` is a per-run counter. The sink is any callable that takes one record. It is off by default, and when off no tracing code runs per task step (naming still applies).

## D016 — Trace sinks must not alter the model (lead, batch 7)
The LOOP-09 reviewer rated as SHOULD that a sink exception became the running task's exception. The lead promoted it to MUST under principle 4: observability must not change the model. Now the first sink error is contained, tracing stops, the loop stops after the current iteration, and `desyncio.run` raises that same exception. During shutdown it is raised after close. Up to the abort point, the model behaves exactly as it would without tracing.

## D017 — Performance guard on `VirtualTimeLoop` attributes (batch 7)
Under CPython 3.13, adding instance attributes to `VirtualTimeLoop` beyond its post-construction count of 28 measurably slows every loop iteration: the inline-values / shared-keys instance-dict layout is lost. Any new per-loop state goes on `self._state` (a `__slots__` object). `tests/test_loop.py::test_loop_instance_dict_is_not_larger_than_before_loop_09` enforces this.
