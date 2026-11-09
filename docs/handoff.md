# Lead handoff

The state a fresh lead session needs. **Read this right after `AGENTS.md` and `CLAUDE.md`.**
The lead rewrites it at the end of every batch, and whenever a session is about to end mid-batch.

_Last updated: 2026-11-09, end of batch 10. Next session starts batch 11 after the owner's "go"._

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

## Module 2: DES primitives — IN PROGRESS (batch 10 done)
The backlog is in `docs/features.md`, Module 2 section. Governing decisions:
- D019: primitives use `async with`.
- D020 (refined) and D024: interrupts are timeout-style `async with sim.interruptible():` scopes.
- D021: Container is strict FIFO; primitives have a monitor hook.
- D022 / AGENTS.md principle 6: reuse asyncio first.
- D023 v2: the monitor contract, below.
- D026: random streams are `sim.random(name)`, which is `random.Random(f"{seed}:{name}")`.
- D028–D031: batch 10 contracts and outcomes.

**Done:** RES-01, CON-01, STO-01 and INT-01 (batch 9); RND-01, MON-01, STO-02 and RES-02 (batch 10). `integration` passes the gate with 573 tests. Nothing is in flight.

**Merged in batch 10:**
- RND-01: `sim.random`, stdlib seeding; ddc6dda; 1 fix round (a PEP 649 annotation on 3.14).
- MON-01: `TimeSeries`; 457ab52; 1 fix round (an M/M/1 band that couldn't tell Lq from ρ; D029 promotions).
- STO-02: `PriorityStore(put(item, priority=))` and `FilterStore`; 11f488a; 0 fix rounds (D031).
- RES-02: `PriorityResource` on per-request futures; 40e9dca; 1 fix round (Condition release leak, D030).
`feat/INT-01` on origin is a discarded design; never merge it.

**D023 v2, the monitor contract (every primitive):** `monitor(primitive)` is called synchronously after every state change, once the state has settled. An `Exception` from the monitor goes to `loop.call_exception_handler(...)`; `KeyboardInterrupt`/`SystemExit` propagate. The operation always completes, and a `CancelledError` is never masked.

**Interrupt model (D024):**
- Code that can be interrupted goes inside `async with sim.interruptible():`. That block exits with `Interrupt(cause)`, just as `asyncio.timeout()` exits with `TimeoutError`.
- Underneath, it is `task.cancel()` plus `uncancel()` at `__aexit__`.
- Interrupting a task outside a block raises `RuntimeError`.
- The code is adapted from `asyncio.Timeout` and PSF-marked.

**Release must be synchronous (D030).** Any `await` in an `__aexit__` is a cancellation point. RES-03 must keep `PriorityResource`'s release free of awaits.

## Next batches
- **Batch 11: RES-03 PreemptiveResource**, alone (it owns `resource.py`). It extends `PriorityRequest` with `preempt=True`: the holder's body runs inside `sim.interruptible()`, and a strictly better request interrupts the worst holder with `Preempted(by=task, usage_since=t)`, taking the unit at the same instant. Tell the implementer about D023 v2, D024 and D030. It could also take the RES-02 SHOULDs (dead `_hold_round0_lock` helper, stale comment, shared `_changed`), since it owns the same files; ask the owner.
- **Batch 12:** API-01 (exports, including `PriorityStore`, `FilterStore` and `TimeSeries`; FilterStore has no `shutdown`, D031), then DOC-02 (primitives guide; the car-wash example from D026 uses `sim.random`).

## Open questions for the owner
- **D027:** add a Python 3.14 run to the gate (one test, `test_loop_instance_dict_is_not_larger_than_before_loop_09`, already fails on 3.14), or document 3.13 as the only tested version?
- **D028/D031:** confirm the `TimeSeries` wiring (a lambda on `monitor=`, explicit initial record, `mean(until=)`), `PriorityStore.put(item, priority=)`, and FilterStore with no `shutdown()`.
- **`ruff format`:** 6 files on `integration` aren't formatted (README.md, docs/how-the-loop-works.md, loop.py, sim.py, trace.py, test_realtime.py). The gate runs `ruff check` only. Should we add `ruff format --check` to the gate, plus a one-off format commit?

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
- **Statistical tests:** pick parameters where the quantities you compare differ, and give the band against a spread over several seeds.

## Starting a new lead session
Run Claude Code on Opus with `integration` checked out, and send:
```
Read AGENTS.md, CLAUDE.md and docs/handoff.md. Continue from the handoff: finish the in-flight batch if any, then report and wait for "go".
```
