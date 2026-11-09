# Lead handoff

The state a fresh lead session needs. **Read this right after `AGENTS.md` and `CLAUDE.md`.**
The lead rewrites it at the end of every batch, and whenever a session is about to end mid-batch.

_Last updated: 2026-11-09, end of batch 9. Next session starts batch 10 after the owner's "go"._

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

## Module 2: DES primitives — IN PROGRESS (batch 9 done)
The backlog is in `docs/features.md`, Module 2 section. Governing decisions:
- D019: primitives use `async with`.
- D020 (refined) and D024: interrupts are timeout-style `async with sim.interruptible():` scopes.
- D021: Container is strict FIFO; primitives have a monitor hook.
- D022 / AGENTS.md principle 6: reuse asyncio first.
- D023 v2: the monitor contract, below.

**Merged in batch 9:**
- RES-01 `Resource` (wraps `Semaphore`), f13f3bc
- CON-01 `Container` (on `Condition`), 4831169
- STO-01 `Store` (wraps `Queue`), a833dd6
- INT-01 interrupts (`sim.interruptible()` / `sim.interrupt()`), f5d2caa
Every one needed one fix round. `integration` passes the gate with 467 tests.
`feat/INT-01` on origin is a discarded design; never merge it.

**D023 v2, the monitor contract (every primitive):** `monitor(primitive)` is called synchronously after every state change, once the state has settled. An `Exception` from the monitor goes to `loop.call_exception_handler(...)`; `KeyboardInterrupt`/`SystemExit` propagate. The operation always completes, and a `CancelledError` is never masked.

**Interrupt model (D024):**
- Code that can be interrupted goes inside `async with sim.interruptible():`. That block exits with `Interrupt(cause)`, just as `asyncio.timeout()` exits with `TimeoutError`.
- Underneath, it is `task.cancel()` plus `uncancel()` at `__aexit__`.
- Interrupting a task outside a block raises `RuntimeError`.
- The code is adapted from `asyncio.Timeout` and PSF-marked.

## Next: batch 10, to be started in a FRESH lead session (D025)
Specs are in `docs/features.md`. All three own disjoint files:
- **RES-02 PriorityResource** (`resource.py`, `tests/test_resource.py`): asyncio has no priority semaphore, so build on `asyncio.Condition`, or per-request futures woken in priority order if Condition can't express it (justify the choice). It must keep RES-01's guarantees and use the D023 v2 monitor.
- **STO-02 PriorityStore + FilterStore** (`store.py`, `tests/test_store.py`): PriorityStore wraps `asyncio.PriorityQueue` with `(key, seq, item)` entries; FilterStore is built on `Condition`. D023 v2 monitor.
- **MON-01 Monitors** (`monitor.py`, `tests/test_monitor.py`): `TimeSeries` built on the D023 v2 `monitor(primitive)` hook. Check M/M/1 utilisation ≈ ρ and mean queue length ≈ ρ²/(1−ρ).

Tell each implementer about D023 v2 and D024. Then batch 11 is RES-03 (preemption: a preemptible holder's body runs inside `interruptible()`), and batch 12 is API-01, then DOC-02.

## Open SHOULD backlog (not scheduled; propose to the owner as clean-up features)
- **Reuse audit of module 1:** no MUSTs. SHOULDs:
  - replace `SequenceCounter` with `itertools.count`;
  - unify the four float validators (clock, runners, sim, realtime);
  - check `MAX_REAL_WAIT` against `asyncio.base_events.MAXIMUM_SELECT_TIMEOUT`;
  - optionally subclass `DefaultSelector` and pass it via `SelectorEventLoop(selector=...)` instead of the hand-delegating `_JumpingSelector` (this touches the `WallClock` seam).
  The audit also confirmed a stronger reason for the `create_task` override: with a task factory installed, 3.13 renames the task to `'None'`.
- **Real-time mode:** `pause()` from inside the sim with no other thread blocks forever; add a one-line warning to the `pause()` and `run()` docstrings. A reused pacer that ended paused starts the next run paused.
- **Trace:** KeyboardInterrupt/SystemExit raised by a sink are deferred to the end of the iteration. `run()`'s `finally` can replace an in-flight exception with a pending sink error. `inspect.iscoroutine(task.get_coro())` is False when tracing is on.
- **sim:** `sim.rng` from a task created with a fresh `contextvars.Context` gives a misleading error.
- **Tests:** `from models.mm1 import` depends on pytest's default `prepend` import mode.
- Per-feature SHOULDs are listed in `docs/implementation-log.md`.

## Lessons for the lead (read before acting)
- **`git switch integration` before committing docs.** One commit landed on the session branch by mistake and had to be merged back (`febc420`). Check with `git branch --show-current`.
- **Review on the merged tree.** Feature branches go stale while others merge. Reviewers test `integration` + `git merge --no-commit` of the branch.
- **Specify contracts that cross features up front** (for example the monitor signature). Leaving one open produced three different signatures in batch 9 (D023).
- **D011:** a docs claim that is false on `integration` is a MUST. **D017:** never add instance attributes to `VirtualTimeLoop`; put state on `self._state`. **D016:** observers never alter the model.
- Fixers do MUST items only. The lead may promote a SHOULD to MUST, and records why as a D###.
- **Give every reviewer its own scratch subfolder.** Reviewers sharing one scratchpad deleted each other's worktrees.
- Tests never sleep in real time. Use the fake wall clock pattern from `tests/test_realtime.py`.

## Starting a new lead session
Run Claude Code on Opus with `integration` checked out, and send:
```
Read AGENTS.md, CLAUDE.md and docs/handoff.md. Continue from the handoff: finish the in-flight batch if any, then report and wait for "go".
```
