# Lead handoff

The state a fresh lead session needs. **Read this right after `AGENTS.md` and `CLAUDE.md`.**
The lead rewrites it at the end of every batch, and whenever a session is about to end mid-batch.

_Last updated: 2026-11-09, during batch 9 (module 2, first batch)._

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

### Batch 9 (RES-01, INT-01, CON-01, STO-01): state at the time of writing
| Feature | Branches | State | Next step |
|---|---|---|---|
| RES-01 Resource (wraps `asyncio.Semaphore`) | `feat/RES-01`, `fix/RES-01-1` | The first review passed. The lead then promoted "a raising monitor leaks a unit" to MUST (D023). Fix round 1 is being redone under D023 v2. | Re-review `fix/RES-01-1` against D023 v2 → gate → merge |
| CON-01 Container (`asyncio.Condition` + per-direction FIFO tickets) | `feat/CON-01`, `fix/CON-01-1` | Review FAILED: it called `monitor(level)` instead of `monitor(container)`. Fix round 1 is running. | Re-review → gate → merge |
| STO-01 Store (wraps `asyncio.Queue`; `items` is a parallel deque mirror) | `feat/STO-01`, `fix/STO-01-1` | Review FAILED: a raising monitor lost the item in `get`, and no test covered it. Fix round 1 is running. | Re-review → gate → merge |
| INT-01 Interrupts | `feat/INT-01` | Implemented with a **mechanism deviation** (see below). In review. | Act on the review. If accepted, write **D024**. Then gate → merge. |

All four are at fix round ≤ 1 (the maximum is 2, CLAUDE.md). They own disjoint files. INT-01 also owns `sim.py` and `tests/test_sim.py`.
Branches cut before `febc420` lack AGENTS.md principle 6, D022 and D023; fixers merge `integration` in first.

**The INT-01 deviation (D024, to be confirmed).** The spec said "deliver via `task.cancel()`, translate to `Interrupt`, `uncancel()`". The implementer showed that this can't work. A `CancelledError` raised at the innermost await, for example inside `Semaphore.acquire`, propagates through the user's own `except Interrupt:` frames before anything can translate it. `asyncio.timeout()` can translate only because its `__aexit__` runs in the user's frame.

The actual mechanism: read the private `task._fut_waiter` and call `Future.set_exception(Interrupt(cause))` on it. asyncio's normal task wake-up then raises it at the await, and each primitive's own `finally`/`except` cleans up. `cancelling()` is never touched.
- If that future is already done (a grant race, a task not yet started, a bare `sleep(0)`), the interrupt is queued per task in a module-level WeakKeyDictionary of deques, and retried via `call_soon`.
- Interrupting a task that is directly awaiting another Task raises `RuntimeError` (workaround: `asyncio.shield`).
- A `while True: await sleep(0)` loop never receives an interrupt.
The review checks the grant race with Resource, Container and Store, the retry-chain determinism, and gather/shield semantics.

**D023 v2, the monitor contract (applies to every primitive):**
- `monitor(primitive)` is called synchronously after every state change, once the state is fully settled.
- An `Exception` from the monitor goes to `loop.call_exception_handler(...)`, which is asyncio's own callback semantics. `KeyboardInterrupt`/`SystemExit` propagate.
- The operation always completes normally for the task, and an original `CancelledError` is never masked.

### After batch 9 merges, the lead must:
1. Mark RES-01, INT-01, CON-01 and STO-01 `done` in `docs/features.md`.
2. Add four entries to `docs/implementation-log.md`, built from the implementer, fixer and reviewer reports. Key facts to keep:
   - **RES-01:** counters around `Semaphore.acquire`, with a `sem.locked()` pre-check (race-free, since there is no await before acquire's own check). "Granted but not yet resumed" counts in `queue_length` for zero virtual time. A Resource binds to the first loop that uses it. The M/M/1 cross-check against `tests/models/mm1.py` matches exactly. 240-run stress probe.
   - **CON-01:** `Condition` wakes waiters FIFO, but each waiter re-checks only its own predicate, hence the per-direction `object()` tickets (`queue[0] is ticket and fits()`). `Condition.wait` re-acquires the lock even when cancelled, so the `finally` runs locked. `notify_all` costs O(N). 300-seed conservation probe.
   - **STO-01:** in `Queue.put`/`get` the completing step is synchronous, so the mirror can't drift (600-seed probe). The reviewer prefers a documented read of `Queue._queue` over the mirror; that's an open SHOULD. `put_nowait`/`get_nowait` are not exposed.
   - **INT-01:** the mechanism above, with each primitive's cleanup path.
3. Write D024 (the INT-01 mechanism), plus any decision taken while acting on the reviews.
4. Rewrite this file for batch 10.

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
- Tests never sleep in real time. Use the fake wall clock pattern from `tests/test_realtime.py`.

## Starting a new lead session
Run Claude Code on Opus with `integration` checked out, and send:
```
Read AGENTS.md, CLAUDE.md and docs/handoff.md. Continue from the handoff: finish the in-flight batch if any, then report and wait for "go".
```
