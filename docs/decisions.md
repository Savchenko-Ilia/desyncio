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
- **RNG encoding v1** (SHA-256, domain tag `desyncio.rng.v1`, length-prefixed seed, UTF-8 name) is now a compatibility contract. Changing it means a new domain tag. _Superseded by D026: the SHA-256 encoding was removed in RND-01._
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

## D018 — LOOP-12 (benchmark) deferred (owner request)
The owner chose to skip the benchmark for now. LOOP-12 stays `todo` in `docs/features.md` but is not scheduled in any batch until the owner asks for it. When it is picked up, the open question is how to install the comparison libraries (`simpy`, `asimpy`): the lead's proposal is a separate `[dependency-groups] bench` group, which is never installed for the test gate and runs by hand only.

## D019 — Primitive API is asyncio-style context managers (owner)
Resources are used with `async with resource:`, or `async with res.request(priority=...)` for the priority variants, rather than SimPy's `req = res.request(); yield req; res.release(req)`. Release on exit, including on exception, cancellation or interrupt, is guaranteed by the context manager. This resolves AGENTS.md §7's API question; AGENTS.md is updated.

## D020 — Interrupts are a dedicated `Interrupt` exception (owner chose option B)
`sim.interrupt(task, cause)` raises `Interrupt(cause)` in the target at its current `await`. It is deliberately not asyncio cancellation: `TaskGroup`, `timeout()` and `gather` treat a cancel as "shut down", which is the wrong meaning for "a breakdown happened, react to it". Real cancellation keeps its normal asyncio meaning. Implementing it needs care (INT-01): the target's wait must be woken with `Interrupt` without disturbing `task.cancelling()`.

## D021 — Module 2 draft open points (lead proposals, pending owner review)
- **Container fairness:** strict FIFO (head-of-line blocking).
- **Interrupting a non-waiting task:** delivered at its next `await`.
- **Monitors:** an explicit `monitor=` hook on primitives.
- **Exports:** every new public name is exported only in API-01, so features don't collide on `__init__.py`.

## D022 — Reuse asyncio first (owner)
The owner chose option B, thin wrappers that delegate, and set the rule "always reuse asyncio functionality if it exists". It is now AGENTS.md principle 6, and also a check in `.claude/agents/reviewer.md`: a hand-written replacement for existing asyncio functionality is a MUST.

The Module 2 backlog was rewritten accordingly:
- `Resource` wraps `asyncio.Semaphore`; `Store` wraps `asyncio.Queue`; `PriorityStore` wraps `asyncio.PriorityQueue`, adding a sequence number for ties.
- `Container`, `FilterStore` and `PriorityResource` are built on `asyncio.Condition`, because asyncio has no equivalent.
- `Interrupt` is delivered through asyncio cancellation underneath, then translated, with `uncancel()` restoring the count. That way every asyncio primitive cleans up the interrupted wait for free, while `TaskGroup` and `timeout()` don't see a cancel (D020).

## D021 — accepted (owner said "go")
The three defaults stand: Container uses strict FIFO (head-of-line); an interrupt to a task that isn't waiting is delivered at its next `await`; primitives get an explicit `monitor=` hook. Batch 9 is RES-01, INT-01, CON-01 and STO-01. The owner also asked for an audit of module 1 code for reuse of existing asyncio functionality (principle 6).

## D023 — One monitor contract for all primitives (lead, batch 9)
The batch 9 implementers each invented a different monitor signature (`monitor(resource)`, `monitor(level)`, `monitor(store)`), because D021 didn't specify one. The contract is now:
- `monitor(primitive)`: called with the primitive itself, synchronously, after every state change, once the primitive's state is fully settled.
- The monitor reads public properties (`count`, `queue_length`, `level`, `len()`, ...).
- An exception from the monitor propagates to the task that caused the change, but it must never leave the primitive inconsistent: no leaked unit, counter, ticket or mirror entry.
- Monitors must not block, or acquire the primitive.

The lead promoted RES-01's reviewer SHOULD ("a raising monitor leaks a held unit") to a MUST under this contract. CON-01 and STO-01 are reviewed against it too.

## D023 v2 — Monitor errors go to the loop's exception handler (lead, replaces the error clause of D023)
In STO-01's review, a monitor that raised during `Store.get` lost the item: it was already off the queue, but the caller never received it. Under "propagate but stay consistent", every primitive needs its own undo logic, and for `get` there is none. asyncio already solves this for callbacks: `Handle._run` catches the exception and calls `loop.call_exception_handler(...)`, which logs by default and leaves the program running. Primitives reuse that (principle 6), and observation never alters the model (principle 4):
- The monitor is called through one helper: try `monitor(primitive)`; on `Exception`, call `loop.call_exception_handler({"message": ..., "exception": exc, <primitive>: obj})`. `KeyboardInterrupt` and `SystemExit` propagate, as in asyncio.
- The operation always completes normally for the calling task (grant, put, get, release), and an original `CancelledError` is never masked.
- `monitor(primitive)` and "called after state is fully settled" from D023 stand.
- A user who wants monitor errors to be fatal sets `loop.set_exception_handler(...)`.
- This differs from trace sinks (D016), which abort the run. A sink is the whole trace; a monitor is a per-primitive callback, so it follows asyncio's callback semantics.
Applied as fix rounds: RES-01 (round 1, redirected), CON-01 (round 1: `monitor(level)` → `monitor(container)` plus this contract) and STO-01 (round 1: no lost item, plus this contract).

## D025 — One module per lead session; `docs/handoff.md` carries the state (owner request)
The owner asked for new modules to start in new context windows. The repo now carries the lead's working state:
- `docs/handoff.md` is rewritten after every batch and before any mid-batch handoff;
- unmerged `feat/*` and `fix/*` branches are pushed;
- CLAUDE.md has a "Sessions and handoff" section;
- the start prompt in `docs/agent-workflow.md` points at the handoff.

## D020 — refined (owner, batch 9)
The user still always sees a dedicated `Interrupt` and never a `CancelledError`. Underneath, though, interrupts are delivered through asyncio cancellation *inside an explicit `async with sim.interruptible():` block*, the same contract as `asyncio.timeout()`. The original concern (TaskGroup/timeout reading an interrupt as "shut down") is handled the way `asyncio.timeout()` handles it: a TaskGroup *inside* the block cancels its children, which is documented asyncio behaviour, and `task.cancelling()` is restored on exit.

## D024 — Interrupts are timeout-style scopes (owner chose "intuitive for asyncio users"; see INT-01)
Three designs were evaluated in batch 9:
1. **Cancel and translate anywhere** (the original INT-01 spec): impossible. The `CancelledError` passes through the user's `except Interrupt:` before any translation can happen.
2. **Fail the awaited future with `Interrupt(Exception)`** (branch `feat/INT-01`): works for Semaphore, Queue, Event and sleep, but the reviewer proved three bugs, because some asyncio cleanup paths run only for `CancelledError`:
   - plain `Lock`: the next waiter is stranded;
   - `Condition.wait` re-acquire: another task's lock gets released;
   - `TaskGroup` exit: the child keeps running.
   It also needed a private attribute (`_fut_waiter`) and a retry queue.
3. **`class Interrupt(CancelledError)`** (the owner's idea): rejected by the reviewer. `Semaphore`'s cancel fix-up leaks capacity (300 of 300 seeds), an uncaught interrupt ends the task as cancelled, and library `except CancelledError` code treats it as a shutdown.

Chosen: copy `asyncio.timeout()`. `async with sim.interruptible():` marks the code; `sim.interrupt(task, cause)` marks the innermost block and calls `task.cancel()`; the block's `__aexit__` (the user's frame) calls `uncancel()` and raises `Interrupt(cause)`. Every primitive cleans up through its standard cancel path, only public APIs are used, and the mental model is the one every asyncio user already has.

Cost: interruptible code must be marked explicitly. Interrupting a task outside a block raises `RuntimeError` in the caller.

`feat/INT-01` (design 2) is discarded, and INT-01 is re-dispatched to a fresh implementer from current `integration` (a new design, not a fix round). RES-03 (preemption) opens an `interruptible()` block around a preemptible holder's body.

## D024 — outcome
Implemented as specified, in `feat/INT-01-v2`. The scope code is adapted from CPython `asyncio.Timeout`, so it carries the PSF marking, and `interrupt.py` is listed in `LICENSES/PSF.txt`, as D009 required for LOOP-03. The PR to `main` must flag it. `feat/INT-01` (design 2) stays on origin, unmerged, for reference only.

## D026 — `sim.rng` → `sim.random`, stdlib seeding (owner)
The owner asked "what is `sim.rng`?" and then "why do we need our own rng?". Two fixes follow:
- The name should explain itself. `sim.random(name)` returns a `random.Random`.
- We were already returning a stdlib `random.Random`, but derived its seed with our own SHA-256 encoding (LOOP-08). `random.Random(str)` is already deterministic across processes, so under principle 6 that derivation goes. A stream is now `random.Random(f"{seed}:{name}")`, cached per run. The `:` separator is unambiguous, because the seed is digits only.
- All random sequences change once. There are no users yet, so the fixed known-value tests are re-pinned.
- The supporting names follow: `RandomStreams`, `current_random_streams()`, `randomness.py`.
- This is scheduled as RND-01 in its own batch 10a, because it edits test files owned by RES-02 and STO-02.
The motivating example was the owner's car wash: 2 bays, washes of 2–4 min for cars under 2 t and 3–6 min otherwise, cars as agents with a weight. It already works on `integration` with `Resource(2)` and `sim.spawn`, and is a candidate example for DOC-02.

## D026 — outcome
Implemented as RND-01 (batch 10a), merged in ddc6dda after one fix round.
- **Band re-picked:** the M/M/1 sanity band in `tests/test_determinism.py` went from `8 < x < 12` to `6 < x < 14`. The new seeding gives a mean time in system of 7.86 against a theoretical 10. The lead accepted this as the permitted spec-change edit. The reviewer ran 40 seeds at n=10000: mean 9.69, sd 1.91. So 7.86 is noise, and the band still rejects swapped rates (≈518), ρ=0.8 (≈4.3) and no queueing (≈1). It would not reject ρ=0.95 (≈13.6).
- **Python 3.14:** the review found that a method named `random` annotated `-> random.Random` resolves to the method itself under PEP 649 lazy annotations. It is now annotated with `from random import Random`, with a regression test. The gate runs only 3.13, so 3.14-only bugs are invisible to it.

## D027 — Python 3.14 is supported but not gated (lead, open for the owner)
`requires-python = ">=3.13"`, but `scripts/gate.sh` runs only the locked 3.13 interpreter. During RND-01 review, `uv run --python 3.14` showed one failure already present on `integration`: `tests/test_loop.py::test_loop_instance_dict_is_not_larger_than_before_loop_09`. It is unrelated to RND-01. The lead did not fix it, because it is outside batch 10's scope. Proposed to the owner: either add a 3.14 test run to the gate (plus a feature to fix that test), or document 3.13 as the only tested version.


## D028 — Batch 10b contracts fixed before dispatch (lead)
Specified up front, following the batch 9 lesson about contracts that cross features:
- **MON-01, how it plugs in:** `TimeSeries` has no knowledge of primitives. The user connects it with the D023 v2 hook, e.g. `Resource(1, monitor=lambda r: busy.record(r.count))`. Because a monitor fires only on a change, the user records the initial value explicitly (`busy.record(0)` at t=0). No adapter class.
- **MON-01, `mean(until=None)`:** time-weighted over `[first record, until]`, where the last value holds until `until`. `until=None` means `sim.now`, so it works only inside a run; after the run, pass `until=` explicitly. `until` earlier than the last record is a `ValueError`. An empty series raises `ValueError`. If the span has zero length, `mean()` returns the last value. `min`, `max` and `last` are over the recorded values.
- **RES-02 and STO-02:** the monitor is `monitor(primitive)` per D023 v2, with the same helper pattern as `Resource._changed` and `Store._notify`. Both live beside the existing classes, in the same modules, and do not change RES-01 or STO-01 behaviour.
The owner may revise these. They are the minimal reading of the spec.

## D029 — MON-01 review: two items promoted to MUST (lead)
- **D016 test:** the reviewer listed "wiring a `TimeSeries` in does not change the model" as a SHOULD. It is promoted, because D016 is a project invariant and the feature exists to be wired into models.
- **Time going backwards:** reusing one `TimeSeries` in a second `desyncio.run` restarts the clock at 0, so `record` sees `t < last t` and silently adds negative area. This extends D028: `record` at a virtual time earlier than the last record raises `ValueError`, and the series is left unchanged. A series belongs to one run.

## D030 — RES-02 uses per-request futures, not `Condition` (lead)
RES-02 round 0 was built on `asyncio.Condition`. The review proved a leak. `Condition.notify_all()` needs the lock, so the holder's `__aexit__` must `await` the lock. After a `notify_all`, the woken waiters are queued on that lock, so the acquire really suspends. A cancel or `sim.interrupt` at that point skips the decrement, and the unit is lost for good. The probe left `count 1`, a queued waiter, and no grant. Releasing a unit must therefore be synchronous, as it is in `asyncio.Semaphore.release()`. `Condition` cannot express that, which is the case the RES-02 spec allows for: "a per-request future woken in priority order".
- Each waiter awaits its own `loop.create_future()`. The waiters are kept in key order `(priority, seq)`.
- `release()` is synchronous. It decrements the count and grants the unit to the best waiter by `fut.set_result(...)`, handing over in the same step (no barging), following `Semaphore`'s grant fix-up.
- A waiter cancelled after its future was resolved hands the unit to the next waiter instead of leaking it, as `Semaphore._acquire` does on `CancelledError`.
- The waiting itself stays on asyncio futures. Only the ordering is ours (principle 6).
This also removes the O(waiters) thundering herd.

## D031 — STO-02 API calls (lead, accepted from the implementer)
- **PriorityStore priority:** `put(item, priority=0)` (lower is served first). It follows SimPy's `PriorityItem`, but needs no wrapper class, and items are never compared. A key function is `put(x, priority=key(x))`. `items` is returned in get order.
- **FilterStore has no `shutdown()`:** `Condition.notify_all()` needs the lock, so a synchronous `shutdown` (as in `Queue.shutdown`) can't be built on it. It is omitted rather than half-done. API-01 and DOC-02 must not claim it exists. An async variant can be a follow-up if the owner wants one.
- **Same-instant barging:** a fresh `get` in the instant of a `put` can take the item ahead of a parked waiter. The reviewer showed that `Store`, `PriorityStore` and `FilterStore` behave identically here (all follow `asyncio.Queue`), so FilterStore is no less fair than STO-01. Strict arrival-order fairness would need hand-written ticket logic, which principle 6 rules out.


## D032 — RES-03 preemption semantics (owner, batch 11)
The RES-03 spec text tied `preempt=True` to the holder opting in to preemption. The owner chose SimPy's semantics instead:
- **A separate class:** `PreemptiveResource` extends `PriorityResource`. `request(priority=0, preempt=True)`.
- **Every holder is preemptible.** On a `PreemptiveResource`, the body of every holder (`request(...)` or plain `async with res:`) runs inside a `sim.interruptible()` block.
- **`preempt=True`** (the default) means this request may preempt. With `preempt=False` it waits like a `PriorityResource` request.
- **When preemption happens:** a `preempt=True` request preempts only when no unit is free, it would be at the head of the queue (no live waiter has a better-or-equal key, which keeps RES-02's no-barging rule), and its priority is strictly lower than the worst holder's. The worst holder is the one with the largest `(priority, request order)` key, i.e. the most recent among the worst priority (owner).
- **Mechanism (lead, following D030):** preemption is synchronous. It removes the victim from the holders, decrements the count, grants the unit to the preemptor in the same step, then calls `sim.interrupt(victim, Preempted(by=preemptor_task, usage_since=t))`, where `t` is the virtual time the victim got the unit. The victim's `async with` exits with `Interrupt` and does not release a second time. Release stays free of awaits.
- **The rest of the batch:** the RES-02 SHOULD clean-ups are folded into RES-03 (owner): remove the dead `_hold_round0_lock` helper, fix the stale comment, share the duplicated `_changed` monitor helper.

## D033 — The gate tests Python 3.14 and checks formatting (owner, answers D027)
The owner chose: "Test python 3.14. Add ruff format." Scheduled as CI-03 in batch 11, in parallel with RES-03 (no shared files).
- `scripts/gate.sh` runs the full test suite on 3.13 and on 3.14. Both must pass.
- The lint step adds `ruff format --check .`. The 6 files that fail it now are formatted once.
- From the merge of CI-03 onward, every branch must be `ruff format`-clean and pass on 3.14. Branches cut before it must pass the new gate when they merge.

## D034 — RES-03 review: a granted reservation can be preempted; a no-await guard is promoted (lead, for the owner to confirm)
- **Extension accepted (owner to confirm).** On a `PreemptiveResource`, a unit already granted to a waiter that has not resumed yet counts as held, as in SimPy, where a granted request is already in `users`. So it can be preempted. That waiter's `async with` raises `Interrupt(Preempted(...))` from `__aenter__`, its body never starts, and nothing is cancelled underneath (`task.cancelling()` stays 0). Without this, a better request arriving in the same instant as a release would wait behind a worse one. The reviewer found that no guarantee breaks. It is the only `Interrupt` not delivered through an `interruptible()` scope, and it is documented in the class docstring.
- **Promoted to MUST.** RES-03 being free of leaks depends on `Interruptible.__aenter__` and `__aexit__` never awaiting (D030). A test must pin that, so a future `await` there fails loudly.

## D035 — RES-03 merged when the branch alone did not pass the gate (lead, process deviation)
`scripts/gate.sh origin/fix/RES-03-1` printed `GATE FAIL` on the Python 3.14 run. The lead's command chained `gate.sh … | tail -1 && git merge …`, so the merge ran anyway, because the pipeline's status was `tail`'s. The lead investigated before going further. The only failure was `test_loop_instance_dict_is_not_larger_than_before_loop_09`. The branch was cut before CI-03, which fixed that test. The merged `integration` (525deb1) passes the full gate: 642 tests on 3.13 and on 3.14, and ruff check, ruff format and mypy are clean. The re-reviewer had run the same full gate on the merged tree. The merge is kept.
Rules from now on:
- Gate a branch on its merged tree when the gate itself changed after the branch was cut (or rebase the branch first).
- Never pipe `gate.sh` into another command in a `&&` chain. Check the exit status with `set -o pipefail`, or run the gate on its own.


## D036 — API-01 export list (lead, the owner may revise)
`desyncio` exports the API-01 row's names plus `Preempted`. Users need `Preempted` for `isinstance(exc.cause, Preempted)` in the preemption pattern (RES-03), and the owner did not answer when asked. `run`, `sim` and `__version__` stay. Other public-looking names, such as `SimulationCleanupError`, `ListSink`/`TraceSink`, `TraceRecord`, `VirtualTimeLoop` and `PriorityRequest`/`PreemptiveRequest`, are NOT added without the owner. They are listed in the batch report as a question.

## D037 — API-01 ownership extended (lead)
The API-01 implementer found two blockers in files it did not own:
- **The INT-01 test is permitted to change.** `tests/test_interrupt.py::test_not_exported_at_top_level` asserted that `Interrupt` is *not* exported. That matched INT-01's spec at the time, but API-01 (and D036) now export it. The spec changed, so the change is permitted: the test is inverted to assert that `desyncio.Interrupt is desyncio.interrupt.Interrupt`.
- **The monitor.py import is fixed.** `src/desyncio/monitor.py` did `from desyncio import sim`. Once `__init__` imports monitor, that can bind the `desyncio.sim` *submodule* instead of the `sim` facade object. mypy reports it, and it breaks at runtime depending on import order. The fix is `from desyncio.sim import sim`. The order-dependent workaround in `__init__.py` (`# isort: skip`) is then removed.
API-01 therefore also owns those lines.

## D038 — DOC-02 review: the docs-examples extractor must not skip examples silently (lead)
The DOC-02 reviewer listed it as a SHOULD that `tests/test_docs_examples.py` skips an indented fence ("  ```python") without failing. The only guard was "at least 8 examples". The lead promoted it to a MUST: DOC-02's acceptance criterion is "examples are run in a test", and an example that is silently not run makes the guide's D011 guarantee void. Fix round 1 also corrects the one false claim the review found (the value of `cancelling()` after an interrupt when a real cancel also fired) and pins it with an executed example.
The same false sentence is in the `src/desyncio/interrupt.py` module docstring, which is outside DOC-02's files. It is left as a SHOULD for a clean-up feature.

## D039 — Clean-up batch: no behaviour changes for the two open SHOULDs (lead, the owner may revise)
- **A reused `RealTimePacer` that ended paused** starts the next run paused. The lead keeps this behaviour and documents it (CLN-03). The pause state belongs to the pacer object the user created; resetting it silently on `bind()` would surprise a user who paused on purpose.
- **`sim.random` in a task with a fresh `contextvars.Context`** still raises `RuntimeError`, with a message that names the real cause (CLN-01). A fallback would need the streams on the loop (D017: `self._state`), and it would change how streams are reached. That is a design change for the owner, not a clean-up.
- The float-validator unification is a separate batch (CLN-05), because it touches the files of CLN-01 and CLN-03.


## D040 — CLN-05: `speed` with an int too large for a float raises `ValueError` (lead)
`realtime._check_speed` calls `float(value)` without catching `OverflowError`. So `run(..., speed=10**400)` raises `OverflowError`, which breaks the common rule that bad arguments raise `TypeError`/`ValueError` (module 2 rules; module 1 validators do the same). CLN-05 unifies the validators, and with that `speed` gets the same `ValueError` as the others. This is the one behaviour change that CLN-05 allows. Every other error type, message and returned value stays exactly as it is (for example, `clock` normalises `-0.0` and the others don't).

## D041 — CLN-05: trim the redundant tests before merging (lead, after the owner questioned the suite's size)
The owner said the test suite may be larger than needed, and a suite-wide audit is running. CLN-05 passed review, but it added about 129 test lines to a refactor. Most of its `to_float` helper tests repeat what the exact-message tests on each caller already prove. The lead promoted the reviewer's SHOULD to a MUST:
- drop the four redundant helper tests and merge the two option tests into one;
- move the bool check into a caller test;
- put the D040 assertion in its own test;
- use one top-level import;
- add a comment that `what` in `clock._check_finite` must be a brace-free internal constant.
The exact-message tests per caller stay. They are the only proof that messages are unchanged, because the older tests use loose `match=` patterns.

## D042 — Trim the test suite (owner: "cut tests")
The owner approved the cut proposed in `docs/test-audit.md`, with the lead's recommendations on its two cut questions:
- **Docs examples may be the only guard for a guarantee.** Each such example in `docs/primitives.md` gets a short HTML comment above it saying what it pins (`<!-- pins: ... -->`), so that an edit to it is a visible test change.
- **Tests that only re-prove asyncio are dropped** (principle 6). One exception: `test_no_barging_same_instant_newcomer` stays, as a guard in case `Resource` is ever moved off `Semaphore`.
- **Not in scope** (still open for the owner): adding `mypy tests` to the gate, and new tests for the gaps the audit found.
This is a spec change for the test files involved, so editing, merging and deleting tests is allowed in TST-01..04. Nothing else changes: every logged regression guard and every determinism test stays, and each merged test must fail under the mutations the old tests caught.

## D043 — Type-check the tests, and add the gap tests (owner: "go with 3 4 also")
The owner extended D042 with the two remaining audit points:
- **`mypy tests` joins the gate.** `LINT_CMD` type-checks `src` and `tests`. The type errors in `tests/` (34 at the time of the audit) are fixed in the tests, not silenced wholesale. A targeted `# type: ignore[code]` is allowed only where a test deliberately passes a wrong type, and it carries a comment. This makes `test_overload_types` a real check.
- **Gap tests:**
  - a late interrupt (`interrupted` is True and no `Interrupt` is raised), plus a docstring line;
  - the ExceptionGroup limit (a real child failure while interrupting propagates untranslated, with `interrupted` True and `cancelling()` restored);
  - a raising monitor on `shutdown(immediate=True)` inside a run (the error reaches the loop exception handler, and the shutdown completes);
  - moving `test_interruptible_enter_and_exit_never_await` from test_resource.py to test_interrupt.py.
- **Scheduling.** Both points touch files owned by batch 16, so they run after it: TST-06 (gap tests) in batch 17, then TST-05 (mypy) in batch 18, so that mypy checks the final tests.
- **Out of scope:** the outside-a-loop case of a raising monitor on immediate shutdown ("no running event loop", a STO-01 SHOULD). It needs a src change.

## D044 — Gate changes are verified after merge (lead)
`scripts/gate.sh` sources `scripts/gate.env` from the main checkout, not from the branch it tests. So a branch that changes `gate.env` passes the gate under the *old* commands. For TST-05 the lead merged on the reviewer's manual run of the new command on the merged tree, then ran `scripts/gate.sh origin/integration`, which used the new `mypy src tests`: GATE PASS. Until `gate.sh` is fixed (a SHOULD), any gate change is verified this way: run it by hand on the merged tree, merge, then gate `integration`.


## D045 — Batch 19: the SHOULD clean-ups; monitor errors outside a loop are logged (lead, the owner may revise)
The owner said "go" with Module 3 still undefined, so the lead ran the defined work: the four SHOULD clean-ups from the batch 18 handoff, as CLN-06..08. Module 3 still waits for the owner.
For CLN-07, a monitor that raises when there is no running loop (e.g. `Store.shutdown(immediate=True)` called outside a simulation) is logged at ERROR on the `desyncio` logger with the traceback, the way asyncio's default exception handler logs (to its `asyncio` logger). The operation still completes. Inside a loop nothing changes: `call_exception_handler` as in D023 v2.

## D046 — D044 is resolved by CLN-06; a branch's gate.env is trusted (lead)
`scripts/gate.sh` now sources `scripts/gate.env` from the tree under test, so a branch that changes its gate commands is gated by its own commands. D044's merge-then-verify procedure is no longer needed for `gate.env` changes. It is still needed for changes to `gate.sh` itself, because the script runs from the checkout it is started in. A branch's `gate.env` runs as shell code inside the gate (an `exit 0` there would end the gate early); that adds no trust the gate did not already give to the branch's tests, and the lead reads every diff before gating.

## D047 — Module 3 is statistics; module 4 is calendar/shifts (owner)
The owner chose statistics as module 3 ("B, after that C"), with:
- **Distributions, both kinds:** output distributions (quantiles, histograms, time-weighted fractions) and input distributions (only those `random.Random` lacks).
- **Standard library only:** no numpy or scipy. The Student t quantile is computed in pure Python.
- **In scope:** Tally (plain statistics of observations), replications across seeds, confidence intervals, warm-up removal.
The feature rows (STA-01..06) were drafted by the lead and shown to the owner before the first batch. Module 4 (calendar/shifts) is defined after module 3.

## D048 — numpy and scipy are required runtime dependencies (owner)
The owner: "use numpy if it's already written there", then chose:
- **numpy is required**, not an optional extra.
- **scipy is added too**, for the Student t quantile (`scipy.stats.t`) and its other distributions.
- **Input randomness:** a new `sim.np_random(name)` returns a numpy `Generator` per named stream, seeded deterministically from the run seed and the name, like `sim.random` (D026). Models use numpy's and scipy's own distributions, so STA-05 (hand-written distributions) is dropped.
Principle 6 is extended: if numpy or scipy already implements something, call it and never re-implement it. This supersedes D047's "standard library only". The core loop and primitives still import neither: `import desyncio` stays numpy-free, so a simulation that uses no statistics pays no import cost.

## D049 — The lead stops after a module, not after each batch (owner)
The owner: "Stop after module. Do not stop after batch." From module 3 on, the lead runs the module's batches back to back, with the per-batch steps of CLAUDE.md unchanged (review, max 2 fix rounds, gate, merge, log, handoff rewrite, push). It reports and waits for "go" only at the end of a module, or earlier when a feature is blocked, a spec is ambiguous, or an owner decision is needed.

## D050 — Batch 21: two reviewer SHOULDs promoted to MUST (lead)
- **RND-02:** the `np_random` seeding formula must be pinned by a literal golden value (as RND-01 does for `random`). Without it, a change to the key or the hash passes every test, and principle 2 says the same seed must give the same trace across versions.
- **STA-02:** the M/M/1 test's comment states a wrong error figure (±0.02; measured ±0.012 over seeds 1..10) and the band (0.05) is looser than needed. Fix the comment and tighten the band to 0.04. A misleading test comment is a test defect.
- **STA-01 (deferred to STA-03, which owns tally.py next):** `Tally.quantile` accepts `bool` and `str` p, while `TimeSeries.quantile` rejects them. STA-03 validates `p` the same way in both.

## D051 — Module 4 is calendar and shifts (owner, with lead proposals to confirm)
The owner chose:
- **Real, timezone-aware dates.**
- **A shift end interrupts** a holder who is still using a unit.
- **A calendar that converts sim time to dates**, as well as resources whose capacity follows a schedule.
The lead proposed these defaults, and the owner accepted all six ("go"):
1. One sim time unit is one second (D004), configurable with `Calendar(unit=...)`.
2. When capacity drops, the **most recently granted** holders are interrupted first (the ones who started last lose least work).
3. Overlapping shifts **add** their capacities.
4. `closed` dates: no shift *starts* on them (an overnight shift from the day before still finishes).
5. DST: a boundary at a non-existent local time moves to the first valid instant after the gap; an ambiguous one uses the first occurrence.
6. No new dependency. `zoneinfo` needs the system time-zone database. Linux and macOS have it; Windows needs the `tzdata` package, which is not added (this needs the owner's OK).

## D052 — CAL-01 review: two SHOULDs promoted to MUST (lead)
- The round-trip test's tolerance was `1e-6 * unit` seconds-in-sim-units, which for an hour unit allows 3.6 ms, not the row's 1 µs. A test that does not check the spec is a test defect.
- `to_sim` leaked a bare `OverflowError` for an out-of-range datetime, while `to_datetime` raises `ValueError`. Both must raise `ValueError`.

## D053 — CAL-02: two gaps in the spec, filled by the lead
- **`Schedule.changes(after)` ends** when capacity can never change again. The row said "endless", but a schedule whose capacity is constant forever (e.g. 24-hour shifts every day) would then loop forever without yielding. The generator stops after 400 days without a change past the last closed date and the date of `after`. The weekly pattern repeats, so that means no change ever again. A consumer (CAL-03) keeps the last capacity once `changes` ends.
- **A shift that lies entirely inside a DST gap** (start and end both at non-existent local times) moves to the same instant at both ends, so it is empty and never runs.

## D054 — CAL-03: a shift change runs after everything else scheduled for the same instant (lead, the owner may revise)
The CAL-03 review found that when a release and a capacity drop fall on the same virtual time, the winner depended on which timer was armed first. That is deterministic, but it is not the explicit tie-break principle 2 asks for. The driver now arms its timers with `loop.call_at_priority(t, +1, ...)`, so at time `t` every ordinary timer (priority 0) fires first. A visit that ends exactly at a shift boundary therefore finishes and releases its unit, and only then does capacity drop. Arrivals at `t` are queued before a capacity rise at `t` is applied, so they are granted in FIFO order by it.
The same review promoted two other findings to MUST: a test for a waiter cancelled after it was granted but before it resumed (two leak mutants survived), and immediate reporting of a driver that dies with an exception (it would freeze capacity silently).

## D055 — Module 5 is a quick start and an examples folder (owner: "go with all")
The owner asked whether there is a quick start and examples. There were neither. The README has only one plain-asyncio snippet, and the guides illustrate one feature per snippet. The owner approved all of the lead's proposal:
- **DOC-03:** `docs/quickstart.md` with install from git, a first model with a walk-through, and "where next".
- **EX-01:** four complete example models in `examples/` (bank, machine shop, inventory, clinic), smoke-tested by `tests/test_examples.py` and type-checked by the gate (`mypy src tests examples`).

## D056 — Module 6 is the event API core; blocks and entities live in the platform (owner)
The platform team sent "Event API requirements, draft v0.1" (2026-11-10). desyncio has no blocks or entities (Source, Queue, Service, Delay, Branch, Sink), and the owner decided they "probably" live in the platform, on top of `sim.emit`. So:
- desyncio's schema v1 is `Event`, `RunStarted`, `RunFinished` and `ResourceChanged` only. The eight entity/service event types of the draft's R4 are the platform's custom events. EVT-05 (block emission) is dropped from desyncio.
- Module 6 is EVT-01..04, EVT-06..08 and RES-04 (rows in `docs/features.md`). If blocks ever move into desyncio, that is a new module with its own spec (AGENTS §1 scope change).
- The draft was reviewed twice: by the lead and by an independent reviewer that verified its findings with probe scripts. D057–D060 record the outcome.

## D057 — Event API: owner decisions on run lifecycle, subscription and names
- **End of run:** `RunFinished` is the last event, and the bus is closed **before** teardown. Teardown cancels leftover tasks in `asyncio.all_tasks()` order, which is a set ordered by object address, so events from teardown releases would not be reproducible (probe: three identical runs, three orders). The last `ResourceChanged` may therefore show units still in use; the guide says so.
- **Reasons:** `main_returned | until | error` (`error` includes `SimulationDeadlock`). There is no stop API, so no `stopped`; a `sim.stop()` is not part of module 6.
- **Pre-run subscription:** `desyncio.run(..., subscribers=[...])`, forwarded by `replicate`. `sim.subscribe` needs a running loop, so without this nobody can see `RunStarted`, and with `until` code after the model never runs.
- **Resource names:** `name=` on the four resource classes, with an automatic default and duplicate detection. Building a resource outside a run stays legal, so the per-run part (the default `resource-N` and the duplicate check) happens when the resource is first used in a run, before any state change (D060).

## D058 — Event handler errors follow the trace-sink model (lead; the owner left it open)
The draft says a handler exception propagates at the emit point. Probes showed that this cannot meet its own goal ("fails loudly"): in a task nobody awaits the exception sat unseen and the run finished normally; a model's `try/except` swallowed it; on a resource's grant path it leaked the unit. So handler errors work as D016 does for trace sinks: the first exception is contained, no further events are delivered (later subscribers do not get that event either), the loop stops after the current iteration, and `desyncio.run` raises that exception. `KeyboardInterrupt`/`SystemExit` follow the tracer's existing rule. This differs from `monitor=` (D023 v2, log and continue): a subscriber is usually the whole record of a run.

## D059 — The observer guard reuses asyncio's debug mode (lead; the owner asked to consider reuse)
`desyncio.run(..., debug=None)` is forwarded to `asyncio.Runner(debug=...)`, so it means what it means in `asyncio.run` (and `PYTHONASYNCIODEBUG=1` works too). The guard is active when `loop.get_debug()` is true; there is no second switch. In debug mode, during dispatch, these raise `ObserverError`: creating a task (`sim.spawn`, `asyncio.create_task`), scheduling a callback (`call_soon`/`call_later`/`call_at`), `sim.interrupt`, `sim.random`/`sim.np_random` (a draw from a handler changes the model's next value; probe-verified), and `sim.emit`. `subscribe` and `Subscription.close` stay allowed (R1). The guard cannot see direct mutation (`store.shutdown()`, `task.cancel()`); the guide says so. `sim.timeout` and `async with resource:` only create objects in a sync handler and need no guard.

## D060 — Event API: spec fixes decided by the lead
- **Bus state** lives on the loop's `_state` (D017), not in a context variable (a task with a fresh `Context` would lose it). On a loop that is not a `VirtualTimeLoop`, an emit point does nothing.
- **`seq`** ticks at every emit point, also when no event is built and when nobody subscribes, so it never depends on subscriptions. "Zero cost when off" therefore means one check and one integer add, and no event object.
- **Custom events:** `sim.emit(MyEvent, **fields)`; the sim fills `t` and `seq` and builds the event only if a subscriber wants that kind (a per-class cache, rebuilt on subscribe/close). `Event` fields are keyword-only. Trade-off accepted: mypy cannot check `**fields`; the dataclass constructor checks names at run time.
- **Payload rule (primitives only, frozen, slotted)** is enforced by test for the built-in events and documented for user subclasses.
- **`EVENT_SCHEMA_VERSION`** is one int, bumped on any change to a built-in event's fields. `to_dict()["kind"]` is the class name; same-named user classes collide and the guide says so.
- **`async def` handlers** are rejected with `inspect.iscoroutinefunction`, which is best effort (a lambda returning a coroutine passes).
- **`ResourceChanged`:** exactly one per existing `_changed()` call in `Resource`, `PriorityResource`, `PreemptiveResource` and `ScheduledResource` (so a preemption swap and a shift change are one event each), emitted after the monitor call. `queue_len` is `queue_length`, which counts granted-but-not-resumed requests.
- **Names, per-run part:** on a resource's first use in a run it registers with the bus: an unnamed one gets `resource-N` in registration order, a duplicate name raises `ValueError` before any state change.
- **Benchmark:** EVT-07 has its own `Resource`-based M/M/1 and no comparison libraries; LOOP-12 stays deferred (D018). The draft's 20% is informative only (a prototype measured +19% to +28%).
- **Stale references in the draft:** AGENTS has no §3.2/§3.4/§8 (the principles are §2.2 and §2.4); the time unit is settled (D004); `resources.py` is `resource.py` plus `scheduled.py`; `tests/models/mm1.py` uses `asyncio.Lock`, so event tests need a new `Resource`-based model.

## D061 — Event API checked with the Hodos lead session (lead)
The owner asked the desyncio and Hodos lead sessions to discuss the plan. The Hodos lead read D056–D060 and answered (2026-11-10):
- Blocks in Hodos on `sim.emit` + `Resource` work for its MVP; the owner confirmed this on the Hodos side, so D056 is no longer "probably". Hodos owns entity ids, server-slot indices and balking (via `queue_length`). It needs nothing else from desyncio for the MVP, and no `Store`/`Container` events.
- `sim.emit(Kind, **fields)` is accepted; Hodos wraps its eight kinds in typed helpers.
- Cutting the stream at `RunFinished` is fine; Hodos treats open intervals as ending at `RunFinished.t`.
- Hodos pins a desyncio `integration` commit once EVT-01, EVT-02 and EVT-04 are merged (after batch 31), and moves to `main` when the owner merges.
- **Early stop without `sim.stop()`:** Hodos caps a run at 200k events or 10 s of wall time by raising its own exception from the recorder. Under D058 that is the endorsed pattern, and EVT-02 now tests it: every event up to the failing one is delivered, there is no `RunFinished`, `run()` raises that exception, and teardown completes. A `sim.stop()` with reason `stopped` would be cleaner; it is a candidate for after module 6, not blocking.
- **Later, not module 6:** thread-safe command injection in real-time mode (live runs), and decision points / "run until the next decision" for RL.

## D062 — Hodos's priority list for module 6 (lead)
The Hodos lead sent its needs in priority order (owner's request): EVT-01, RES-04, EVT-02, EVT-04, EVT-03 are on its critical path; EVT-06, EVT-07 and EVT-08 are welcome but not. It matches batches 29–32, so the order stays. Two small changes:
- Subclassing `Event` is a documented, supported public contract, tested with a user subclass (EVT-01).
- `Event`, `Subscription` and `ObserverError` are importable from `desyncio.events` before EVT-08 does the top-level exports: `Subscription` by EVT-02, `ObserverError` by EVT-06 (both now own `events.py`; they are in different batches from EVT-01 and from each other's batch partner).
The lead sends Hodos the `integration` SHA after batch 30 and after batch 31. Hodos's later list (`sim.stop()`, a WASM/no-self-pipe loop mode and a browser pacer, cross-thread commands in real time, RL decision points, correctly rounded sampling) is recorded in the handoff, unscheduled.

## D063 — Batch 29 calls (lead)
- **RES-04 test placement:** the one parametrised test over the four resource classes lives in `tests/test_scheduled.py`, not `tests/test_resource.py` as the row listed, because building a `ScheduledResource` needs a run and that file's helpers. Accepted: it is the smaller suite.
- **EVT-01 exports check is one-directional:** every `Event` subclass defined in `desyncio.events` must be in `__all__`, but `__all__` may hold other names. The fixer's first version required equality, which would have failed as soon as EVT-02 and EVT-06 add `Subscription` and `ObserverError` (D062), in files they don't own. Redirected within fix round 1.

## D064 — EVT-02 review: three SHOULDs promoted, automatic resource names skip taken ones (lead)
The EVT-02 reviewer found one MUST (a validation test that could not fail, because "never awaited" warnings go to `sys.unraisablehook`, not `warnings`). The lead promoted three SHOULDs to MUST for fix round 1:
- **Automatic names skip taken names.** D060 left open what happens when an unnamed resource's `resource-N` is already used as an explicit name. The branch raised for the *unnamed* resource and still advanced the counter. Now an automatic name takes the next `resource-N` that is free in the run, so an unnamed resource never raises; an explicit name that is already taken still raises `ValueError`, and a failed registration changes no state. Hodos names its resources after its blocks, so this only matters for mixed models, but an error blaming a resource that has no name is wrong.
- **`Bus.error`** was documented, never read and never tested (a last-wins mutation survived): remove it or test it.
- **`subscribe` after the bus closes** returns a closed subscription, as documented, but nothing tested it: add an assertion.

## D065 — EVT-04 may edit one line of tests/test_bus.py (lead)
Once resources emit `ResourceChanged`, EVT-02's `test_handler_error_inside_resource_leaks_nothing` breaks: its catch-all later subscriber now also receives the resource events before the failing `Ping`. The test means "the later subscriber missed Ping", and the spec changed (resources emit), so editing it is allowed under CLAUDE.md's rule. EVT-04's ownership is extended to that one change: the later subscriber is subscribed with `kinds=(RunStarted, Ping)` and the assertion is unchanged. EVT-06 (same batch) does not own `tests/test_bus.py`, so there is no overlap.

## D066 — EVT-06 calls (lead)
- **`replicate` forwards `debug=`,** like the other `run` options. `experiment.py` and `tests/test_experiment.py` are added to EVT-06's owned files for that change only; EVT-04 (same batch) does not own them.
- **`call_soon` during dispatch records the `ObserverError` instead of raising it.** The implementer's probe: a handler resolving a future that main awaits trips the guard inside asyncio's C `Future._schedule_callbacks`, after the future is done and its callback list cleared. Raising there lost main's wakeup, and teardown then deadlocked (`SimulationDeadlock` instead of `ObserverError`). So `call_soon` records the error (bus closed, loop stopped at the iteration boundary, `run` raises it, the caller's stack added as a note) and lets the call finish. Every other guarded call raises before changing any state. Side effect: mutation that wakes a waiter (`task.cancel()`, `event.set()`, `store.shutdown()`) is caught too; mutation that schedules nothing is not, and the docs say so.
- **`call_soon_threadsafe` is not guarded:** other threads (a real-time pacer's resume) and asyncio's async-generator finalizer hook, which GC can run in the middle of a handler, use it.
- **A handler cannot swallow `ObserverError`:** it is recorded as a handler error before it is raised, so the run ends even if the handler catches it.

## D067 — EVT-06 review: SHOULDs promoted (lead)
The EVT-06 reviewer found no false positive in debug-mode models and identical runs with debug on and off, but three MUSTs in the tests: the "names the call" check was beaten by the `call_soon` stack note (pytest's `match` searches `__notes__`), so deleting the `create_task` or `sim.interrupt` check passed; D066's `call_soon_threadsafe` exemption had no test; "no check outside debug mode" missed `create_task`. Promoted to MUST for fix round 1:
- `coro.close()` in a guarded `create_task`, and the `super()._check_callback` call, each get a test (the first is a documented promise; dropping the second silently removes asyncio's own debug check).
- Four docstring claims are false or muddled (D011): checks said to sit in *existing* `_debug` branches (four are new); `asyncio.sleep` listed as something a sync handler schedules; the `task.cancel()` "cannot see" wording; `run`'s `debug=` silent about `call_soon_threadsafe`.
- The fixer also probes, now that EVT-04 is merged, whether `ObserverError` raised from `emit_event` inside a resource's `_changed()` can leave the resource inconsistent, and makes that path record instead of raise if so.

## D068 — A primitive's emit from inside a handler records `ObserverError` instead of raising (lead, from EVT-06 fix round 1)
The D067 probe, on `integration` with EVT-04: a debug-mode handler that drives a resource's `__aenter__`/`__aexit__` by hand reaches `_changed()` during dispatch, so `emit_event` → `Bus.emit` raised `ObserverError` inside the resource, after its counters changed. On the acquire path that left a phantom waiter, and on the fast path a unit taken and never released — the half-done change D058 rules out. So, as for `call_soon` (D066): `emit_event` (the primitives' path) records the `ObserverError` (bus closed, nothing delivered, loop stops, `run` raises it, the handler's stack as a note) and returns, so the primitive finishes its state change. `sim.emit` called from handler code still raises before anything happens. Only reachable by driving dunders or private methods from a handler; `ScheduledResource`'s driver calls `_changed()` only in its own task.
