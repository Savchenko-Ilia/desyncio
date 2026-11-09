# Lead handoff

The state a fresh lead session needs. **Read this right after `AGENTS.md` and `CLAUDE.md`.**
The lead rewrites it at the end of every batch, and whenever a session is about to end mid-batch.

_Last updated: 2026-11-09, batch 9: 3 of 4 merged, INT-01 waiting for the owner's decision._

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

## Module 2: DES primitives — IN PROGRESS
The backlog is in `docs/features.md`, Module 2 section. The governing decisions:
- D019: primitives use `async with`.
- D020: interrupts are an `Interrupt` exception, not a cancel.
- D021: Container is strict FIFO; an interrupt to a non-waiting task is delivered at its next await; primitives have a monitor hook.
- D022 / AGENTS.md principle 6: **reuse asyncio first**.
- D023 v2: monitor contract (below).

### Batch 9 (RES-01, INT-01, CON-01, STO-01)
| Feature | State |
|---|---|
| RES-01 Resource | **merged** f13f3bc (fix round 1) |
| CON-01 Container | **merged** 4831169 (fix round 1) |
| STO-01 Store | **merged** a833dd6 (fix round 1) |
| INT-01 Interrupts (`feat/INT-01`, review round 0 FAILED) | **Waiting for the owner's decision**, then fix round 1 (one round left after it). |

**INT-01: mechanism and review.**
- The spec said "cancel → translate → `uncancel()`". The implementer showed this can't work: the `CancelledError` passes through the user's `except Interrupt:` before anything can translate it. The reviewer confirmed this.
- The implemented mechanism: fail `task._fut_waiter` with `Interrupt(Exception)`. If that future is already done, the interrupt is queued per task and retried with `call_soon`.
- The review FAILED it with three MUSTs. All three are asyncio cleanup paths that only run for `CancelledError`:
  1. **Plain `asyncio.Lock`:** interrupting a waiter strands the next waiter forever, with the lock free.
  2. **`Condition.wait`'s internal lock re-acquire:** an interrupt escapes without the lock, and the user then releases another task's lock.
  3. **`TaskGroup.__aexit__`:** after the exit, the child keeps running.
- Our own Resource, Container and Store passed a 300-seed interrupt+cancel stress test.
- The owner suggested a `CancelledError` subclass. The reviewer tested it and rejects it: `Semaphore`'s cancel fix-up leaks capacity (300/300 seeds), an uncaught interrupt ends the task as cancelled, and library code treats it as a shutdown. It also contradicts D020.
- **Lead's proposal, put to the owner:** keep D024 (`Interrupt(Exception)`).
  - Raise a clear `RuntimeError` ("not interruptible here; use desyncio.Resource") when the target is blocked on a plain `Lock.acquire` or inside `TaskGroup` exit, as is already done for awaiting a Task.
  - Defer the interrupt to the next await when the target is in `Condition.wait`'s re-acquire, so Container works.
  - Add tests for all three, and docs notes on gather/shield and the `sleep(0)` spin.
  - The alternative put to the owner: defer for plain Lock too (the interrupt then arrives later in virtual time).
- **Next:** on the owner's answer, dispatch the INT-01 fixer, then re-review → gate → merge. Write D024 with the final design, mark INT-01 done, and add the log entry (mechanism, per-primitive cleanup, the rejected variant). Probes are in the old session's scratchpad `probes/`; recreate them as needed.

## Next batches (module 2)
- **Batch 10:** RES-02 (PriorityResource, `resource.py`), STO-02 (PriorityStore wraps `PriorityQueue` with a sequence number; FilterStore on `Condition`; `store.py`), MON-01 (`TimeSeries`, `monitor.py`, uses the D023 v2 hook).
- **Batch 11:** RES-03 (PreemptiveResource, via `sim.interrupt`).
- **Batch 12:** API-01 (top-level exports; the only feature that edits `__init__.py`), then DOC-02 (`docs/primitives.md` with tested examples).

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
