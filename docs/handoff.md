# Lead handoff

The state a fresh lead session needs. **Read this right after `AGENTS.md` and `CLAUDE.md`.**
The lead rewrites it at the end of every batch, and whenever a session is about to end mid-batch.

_Last updated: 2026-11-09, batch 9: 3 of 4 merged, INT-01 redesigned (D024) and waiting for "go"._

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
| INT-01 Interrupts | **Redesigned (D024).** The spec is approved; waiting for "go" to re-dispatch a fresh implementer. |

**INT-01: redesigned (D024), waiting for the owner's "go" to implement.**
The owner chose the design that is most intuitive for asyncio users: timeout-style scopes. `async with sim.interruptible():` blocks exit with `Interrupt(cause)`, delivered via `task.cancel()` and translated at `__aexit__` with `uncancel()`, exactly like `asyncio.timeout()`. The full spec is the INT-01 row in `docs/features.md`. D024 lists the rejected designs and why.
- `feat/INT-01` (the earlier future-failing design) is **discarded**. Do not merge it. INT-01 is re-dispatched to a fresh **implementer** from current `integration`. That is a new design, not a fix round, so the round count restarts.
- On "go": implementer → reviewer (probe Lock, Condition, TaskGroup, nested timeout/interruptible, real cancel, stress against Resource/Container/Store) → gate → merge → mark done → log entry → rewrite this file for batch 10.

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
