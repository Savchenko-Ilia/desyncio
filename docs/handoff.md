# Lead handoff

The state a fresh lead session needs. **Read this right after `AGENTS.md` and `CLAUDE.md`.**
The lead rewrites it at the end of every batch, and whenever a session is about to end mid-batch.

_Last updated: 2026-11-09, end of batch 12. Next session starts batch 13 (DOC-02) after the owner's "go"._

## Where things are
- **`integration`** is the source of truth. All verified work is merged there. The session branch
  `claude/setup-batch-1-5bb5vp` mirrors it; fast-forward it after each push.
- **PR #2** (`integration` → `main`, https://github.com/Savchenko-Ilia/desyncio/pull/2) is open for the owner.
  It contains everything merged so far. Only the owner merges into `main`.
- **CI is manual-only (D013)**: there are no GitHub Actions credits, so `scripts/gate.sh <branch>` is the check for every merge.
- **Unmerged feature/fix branches must be pushed** so that a new session (a fresh container) can see them.
  Merged branches don't need to be: their content is in `integration`.

## Module 1: virtual-time loop — DONE
17 of 18 features are done. LOOP-12 (benchmark) is deferred by the owner (D018).
`docs/implementation-log.md` has an entry per feature; `docs/how-the-loop-works.md` is the explainer.

## Module 2: DES primitives — IN PROGRESS (batch 12 done; DOC-02 left)
The backlog is in `docs/features.md`, Module 2 section. Governing decisions:
- D019: primitives use `async with`.
- D020 (refined) and D024: interrupts are timeout-style `async with sim.interruptible():` scopes.
- D021: Container is strict FIFO; primitives have a monitor hook.
- D022 / AGENTS.md principle 6: reuse asyncio first.
- D023 v2: the monitor contract, below.
- D026: random streams are `sim.random(name)`, which is `random.Random(f"{seed}:{name}")`.
- D028–D031: batch 10 contracts and outcomes.
- D032: RES-03 preemption is SimPy-style (owner). D034: a reservation can be preempted (owner to confirm). D033: the gate runs 3.13 + 3.14 + `ruff format --check`.

**Done:** RES-01, CON-01, STO-01, INT-01 (batch 9); RND-01, MON-01, STO-02, RES-02 (batch 10); CI-03, RES-03 (batch 11); API-01 (batch 12). `integration` passes the gate with 688 tests on 3.13 and on 3.14. Nothing is in flight.

**Merged in batch 12:**
- API-01: public exports (D036, which adds `Preempted`); aaa8bba; 0 fix rounds. Its ownership was extended under D037 (the INT-01 export test was inverted, and monitor.py now uses `from desyncio.sim import sim`).

**Merged in batch 11:**
- CI-03: the gate runs the suite on Python 3.14 too, and adds `ruff format --check .`; 0 fix rounds. The 3.14 failure was an interpreter difference (stock loop: 26→27 attributes), and the test is now relative to the running interpreter.
- RES-03: `PreemptiveResource` and `Preempted`; 525deb1; 1 fix round (formatting, plus the promoted no-await guard). The merge deviated from the process (D035): the branch alone failed the new gate on the 3.14 test that CI-03 had fixed, while the merged tree passed.

**Merged in batch 10:** RND-01 (ddc6dda), MON-01 (457ab52), STO-02 (11f488a), RES-02 (40e9dca). See the log.
`feat/INT-01` on origin is a discarded design; never merge it.

**D023 v2, the monitor contract (every primitive):** `monitor(primitive)` is called synchronously after every state change, once the state has settled. An `Exception` from the monitor goes to `loop.call_exception_handler(...)`; `KeyboardInterrupt`/`SystemExit` propagate. The operation always completes, and a `CancelledError` is never masked.

**Interrupt model (D024):**
- Code that can be interrupted goes inside `async with sim.interruptible():`. That block exits with `Interrupt(cause)`, just as `asyncio.timeout()` exits with `TimeoutError`.
- Underneath, it is `task.cancel()` plus `uncancel()` at `__aexit__`.
- Interrupting a task outside a block raises `RuntimeError`.
- The code is adapted from `asyncio.Timeout` and PSF-marked.

**Release must be synchronous (D030).** Any `await` in an `__aexit__` is a cancellation point. RES-03 must keep `PriorityResource`'s release free of awaits.

## Next batches
- **Batch 13: DOC-02** (primitives guide), in a fresh lead session. It owns `docs/primitives.md`, `README.md` and `tests/test_docs_examples.py`. It covers:
  - examples: bank tellers (Resource), fuel tank (Container), producer/consumer (Store), machine breakdown (interrupts), preemption (the RES-03 machine shop), and the car wash (D026, `sim.random`);
  - a "what is asyncio, what is ours" table per primitive;
  - ordering and cancellation guarantees, including FilterStore's lack of `shutdown` (D031), the reservation preemption (D034) and the nested-`interruptible()` caveat from RES-03.
  The examples use top-level imports (`from desyncio import ...`) and are run in a test. The README should list the exports. The docs claims must be true on `integration` (D011).
- After DOC-02, module 2 is complete. Propose the clean-up SHOULD backlog and module 3 to the owner.

## Open questions for the owner
- **D034:** confirm that a granted-but-not-resumed reservation on a `PreemptiveResource` can be preempted (its `async with` raises `Interrupt(Preempted)` and its body never runs).
- **D028/D031:** confirm the `TimeSeries` wiring, `PriorityStore.put(item, priority=)`, and FilterStore with no `shutdown()`.
- **D036:** `Preempted` is exported (lead decision). Should any of these also be exported: `SimulationCleanupError`, `ListSink`/`TraceSink`, `TraceRecord`, `VirtualTimeLoop`, `PriorityRequest`/`PreemptiveRequest`?

## Open SHOULD backlog (not scheduled; propose to the owner as clean-up features)
- **Reuse audit of module 1:** no MUSTs. SHOULDs:
  - replace `SequenceCounter` with `itertools.count`;
  - unify the four float validators (clock, runners, sim, realtime);
  - check `MAX_REAL_WAIT` against `asyncio.base_events.MAXIMUM_SELECT_TIMEOUT`;
  - optionally subclass `DefaultSelector` and pass it via `SelectorEventLoop(selector=...)` instead of the hand-delegating `_JumpingSelector` (this touches the `WallClock` seam).
  The audit also confirmed a stronger reason for the `create_task` override: with a task factory installed, 3.13 renames the task to `'None'`.
- **Real-time mode:** `pause()` from inside the sim with no other thread blocks forever; add a one-line warning to the `pause()` and `run()` docstrings. A reused pacer that ended paused starts the next run paused.
- **Trace:** KeyboardInterrupt/SystemExit raised by a sink are deferred to the end of the iteration. `run()`'s `finally` can replace an in-flight exception with a pending sink error. `inspect.iscoroutine(task.get_coro())` is False when tracing is on.
- **sim:** `sim.random` from a task created with a fresh `contextvars.Context` gives a misleading error.
- **Tests:** `from models.mm1 import` depends on pytest's default `prepend` import mode.
- Per-feature SHOULDs are listed in `docs/implementation-log.md`.
- **Batch 12 SHOULDs:** API-01 (a named `import desyncio` cycle test; generalise the AST guard to any `from desyncio import X`).
- **Batch 10–11 SHOULDs:** CI-03 (add `.venv-3.14` to ruff's exclude or `.gitignore`, and an explicit `uv python install 3.14` in ci.yml); RES-03 (the AST guard can't see a helper that awaits).
- **Batch 10 SHOULDs:** listed per feature in the RND-01, MON-01, STO-02 and RES-02 log entries (for example, the RND-01 subprocess test drops PATH from `env`, and FilterStore needs a re-park seniority test).

## Lessons for the lead (read before acting)
- **`git switch integration` before committing docs.** One commit landed on the session branch by mistake and had to be merged back (`febc420`). Check with `git branch --show-current`.
- **Review on the merged tree.** Feature branches go stale while others merge. Reviewers test `integration` + `git merge --no-commit` of the branch.
- **Specify contracts that cross features up front** (for example the monitor signature). Leaving one open produced three different signatures in batch 9 (D023).
- **D011:** a docs claim that is false on `integration` is a MUST. **D017:** never add instance attributes to `VirtualTimeLoop`; put state on `self._state`. **D016:** observers never alter the model.
- Fixers do MUST items only. The lead may promote a SHOULD to MUST, and records why as a D###.
- **Give every reviewer its own scratch subfolder.** Reviewers sharing one scratchpad deleted each other's worktrees.
- Tests never sleep in real time. Use the fake wall clock pattern from `tests/test_realtime.py`.
- **Give reviewers an exact worktree command:** `mkdir -p` the scratch dir, `git worktree add --detach <dir>/wt origin/integration`, and prefix every later command with `cd <dir>/wt &&`. Without that, two reviewers in batch 10 ran their first commands in the main checkout.
- **Ask reviewers to probe concurrency with scripts, and to try mutations of the tests.** Batch 10's two real bugs (a release leak, and a statistics test that could not fail) were found that way, not by reading.
- **Never pipe `scripts/gate.sh` inside a `&&` chain** (D035): the chain sees the status of the last pipe stage. When the gate changes, gate older branches on the merged tree.
- **No history rewrites:** the environment blocks force-pushes and amends of pushed commits. Ask agents for new commits on top.
- **Statistical tests:** pick parameters where the quantities you compare differ, and give the band against a spread over several seeds.

## Starting a new lead session
Run Claude Code on Opus with `integration` checked out, and send:
```
Read AGENTS.md, CLAUDE.md and docs/handoff.md. Continue from the handoff: finish the in-flight batch if any, then report and wait for "go".
```
