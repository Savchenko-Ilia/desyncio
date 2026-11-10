# Lead handoff

The state a fresh lead session needs. **Read this right after `AGENTS.md` and `CLAUDE.md`.**
The lead rewrites it at the end of every batch, and whenever a session is about to end mid-batch.

_Last updated: 2026-11-10, end of batch 33. **Module 6 (event API) is DONE** (D073). Nothing is in flight; no agents are running. `integration` passes the gate with 813 tests on 3.13 and 3.14. PR #5 (`integration` → `main`) predates module 6: check `list_pull_requests` before telling the owner what a PR contains. Next: the owner decides what comes next._

## Where things are
- **`integration`** is the source of truth. All verified work is merged there. The session branch
  `claude/setup-batch-1-5bb5vp` mirrors it; fast-forward it after each push.
- **PR #4** (`integration` → `main`, https://github.com/Savchenko-Ilia/desyncio/pull/4) covered batches 14–18 and was **merged** on 2026-11-09. Batch 19 (CLN-06..08) is on `integration` only and needs a new PR when the owner asks for one.
  PR #3 was merged at `74c88b7` (end of batch 13). Only the owner merges into `main`.
  **Lesson:** after the owner merges a PR, the next batch's work needs a new PR. Check `list_pull_requests` before telling the owner that an open PR already contains it.
- **CI is manual-only (D013)**: there are no GitHub Actions credits, so `scripts/gate.sh <branch>` is the check for every merge.
- **Unmerged feature/fix branches must be pushed** so that a new session (a fresh container) can see them.
  Merged branches don't need to be: their content is in `integration`.

## Module 1: virtual-time loop — DONE
17 of 18 features are done. LOOP-12 (benchmark) is deferred by the owner (D018).
`docs/implementation-log.md` has an entry per feature; `docs/how-the-loop-works.md` is the explainer.

## Module 2: DES primitives — DONE (batch 13)
The backlog is in `docs/features.md`, Module 2 section. Governing decisions:
- D019: primitives use `async with`.
- D020 (refined) and D024: interrupts are timeout-style `async with sim.interruptible():` scopes.
- D021: Container is strict FIFO; primitives have a monitor hook.
- D022 / AGENTS.md principle 6: reuse asyncio first.
- D023 v2: the monitor contract, below.
- D026: random streams are `sim.random(name)`, which is `random.Random(f"{seed}:{name}")`.
- D028–D031: batch 10 contracts and outcomes.
- D032: RES-03 preemption is SimPy-style (owner). D034: a reservation can be preempted (owner to confirm). D033: the gate runs 3.13 + 3.14 + `ruff format --check`.
- D038: the docs-examples test must never skip an example silently.

**Done:** RES-01, CON-01, STO-01, INT-01 (batch 9); RND-01, MON-01, STO-02, RES-02 (batch 10); CI-03, RES-03 (batch 11); API-01 (batch 12); DOC-02 (batch 13). `integration` passes the gate with 788 tests on 3.13 and on 3.14. Nothing is in flight. No agents are running.

**Merged in batch 13:**
- DOC-02: `docs/primitives.md`, the primitives guide with 24 executed examples, and a README with a "Public API" list checked against `__all__`. Merged as b3746f4, after 1 fix round. Round 1 corrected the guide's claim about `cancelling()` after interrupt + real cancel. D038 promoted the silent skip of indented fences to a MUST, and it was fixed.

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

## Clean-up batches 14–15 — DONE
- CLN-01 (fa6d5dc, 1 fix round), CLN-02 (5142464), CLN-03 (fea39d8), CLN-04 (747c800), CLN-05 (fb22adf, 1 fix round, D040/D041).
- `integration` passes the gate with 821 tests on 3.13 and 3.14. Nothing is in flight; no agents are running.
- D039: no behaviour changes for the reused pacer and `sim.random` in a fresh context. D040: `speed=10**400` raises `ValueError`. D041: CLN-05's tests were trimmed.

## Test-suite trim, batches 16–18 — DONE
- **Result:** 821 → 556 tests, about 9.4k → 7.4k test lines, about 11 s → 7 s.
- **Batch 16:** TST-01 (116e770, 1 fix round), TST-02 (8c69f38), TST-03 (cb4fa4c), TST-04 (ff97718, 1 fix round). Fourteen docs examples are now load-bearing and carry `<!-- pins: ... (D042) -->` comments in `docs/primitives.md`. Editing one of them is a test change.
- **Batch 17:** TST-06, the gap tests (afaf4eb, 1 fix round): late interrupt (eager tasks only), ExceptionGroup limit, raising monitor on immediate shutdown, PriorityStore default priority, the cancelled-putter mirror.
- **Batch 18:** TST-05 (D043): the gate runs `mypy src tests`. D044: gate.sh reads gate.env from the main checkout, so gate changes are verified after merge.
- `integration` passes the gate: 556 tests on 3.13 and 3.14, ruff, and mypy on 36 files. Nothing is in flight; no agents are running.

## Clean-up follow-ups, batch 19 — DONE
- CLN-08 (36d0215): the import guard also rejects `from . import X`; the late-interrupt docstring covers the non-eager case.
- CLN-06 (c22b7ee): `gate.sh` sources `gate.env` from the tree under test (D046 resolves D044 for `gate.env`; changes to `gate.sh` itself are still verified after merge).
- CLN-07 (0da8d5e): a raising monitor outside a running loop is logged on the `desyncio` logger (D045, owner may revise); one helper in `_monitor.py`.
- All 0 fix rounds. `integration` passes the gate: 558 tests on 3.13 and 3.14, ruff, mypy. Nothing is in flight; no agents are running.

## Module 3: statistics — DONE (batches 20–23)
- Rows STA-01..06, RND-02 and DEP-01 are in `docs/features.md` (Module 3). Owner decisions: D047 (scope), D048 (numpy and scipy required; `sim.np_random`; STA-05 dropped; principle 6 extended to numpy/scipy, AGENTS.md updated).
- **Batch 20:** DEP-01 merged (0 fix rounds): numpy 2.5.4, scipy 1.18.1, scipy-stubs in dev.
- **Batch 21:** STA-01 (47dd7f5, 0 rounds), STA-04 (18e16ce, 1 round), RND-02 (402bcb4, 1 round), STA-02 (24856f2, 1 round). D050 promoted two SHOULDs.
- **Batch 22:** STA-03 (0 fix rounds): warm-up for Tally and TimeSeries.
- **Batch 23:** STA-06 (34ebdec, 1 fix round): exports and `docs/statistics.md`.
- `integration` passes the gate: 627 tests on 3.13 and 3.14 (about 14 s each), ruff, mypy. PR #5 covers batches 19–23.
- Guide for users: `docs/statistics.md`. Per-feature details and SHOULDs: `docs/implementation-log.md`.

## Module 4: calendar and shifts — DONE (batches 24–27)
- Rows CAL-01..04 in `docs/features.md`. Decisions: D051 (owner: real dates and zones, shift end interrupts, calendar + schedules; six defaults accepted), D052 (CAL-01 promoted SHOULDs), D053 (`changes` ends when constant; a shift inside a DST gap is empty), D054 (a shift change runs after ordinary timers at the same instant; owner may revise).
- **Batch 24:** CAL-01 `Calendar` (1 fix round). **Batch 25:** CAL-02 `Shift`/`Schedule` (0 rounds). **Batch 26:** CAL-03 `ScheduledResource`/`ShiftEnded` (1 round). **Batch 27:** CAL-04 exports and `docs/calendar.md` (60da3ad, 1 round).
- `integration` passes the gate: 683 tests on 3.13 and 3.14 (about 14 s each), ruff, mypy. AGENTS.md has no open question left.

## Module 5: quick start and examples — DONE (batch 28)
- D055. DOC-03 `docs/quickstart.md` (d1ec9af, 1 fix round). EX-01 `examples/` with bank, machine shop, inventory and clinic models, smoke-tested and type-checked by the gate (98614ba, 1 fix round).
- `integration` passes the gate: 692 tests on 3.13 and 3.14 (about 15 s each), ruff, `mypy src tests examples`.

## Module 6: event API — DONE (batches 29–33)
- Requested by the Hodos platform ("Event API requirements, draft v0.1", not in the repo). The spec is D056–D073 and the Module 6 rows in `docs/features.md`. User guide: `docs/observing.md`; example: `examples/recorder.py`; overhead: `bench/README.md`.
- Blocks and entities live in the platform (D056). desyncio ships `desyncio.events` (`Event`, `RunStarted`, `RunFinished`, `ResourceChanged`, `EVENT_SCHEMA_VERSION`, `Subscription`, `ObserverError`), `sim.subscribe`/`sim.emit`, `run(subscribers=, debug=)`, `replicate(subscribers=, debug=)`, `ResourceChanged` from all four resource classes with `name=`, and the debug-mode observer guard.
- **Batch 29:** RES-04 (cd24156, 0 fix rounds), EVT-01 (453562a, 1 round). **Batch 30:** EVT-02 (5fa61f2, 1 round, D064). **Batch 31:** EVT-04 (aa069cf, 2 rounds, D065), EVT-06 (1c63e63, 1 round, D066–D068). **Batch 32:** EVT-03 (6f869e0, 1 round), EVT-07 (76b88d1, 1 round, D069–D070). **Batch 33:** EVT-08 (af19a80, 0 rounds, D071–D072).
- **Hodos** (hodoslab/hodosapp; lead session "Hodos dev", session_01TYFYZtG52HrqFTnMoMNdyo) pinned `69c98e2` (pin 2, after batch 31). It asked for messages only with pin SHAs or contract changes. Its later wishes: `sim.stop()` with reason `stopped`; a WASM / no-self-pipe loop mode and a browser pacer (Pyodide findings: `run` fails inside an already-running WebLoop, no `socket.socketpair`, a ~20-line shim works, real time blocks in `select`); thread-safe command injection for live runs; RL decision points; optionally correctly rounded sampling for cross-runtime identity.
- **Known gaps:** teardown cancels leftover tasks in address order, so the *trace* (not the event stream) of a run with leftover tasks is not reproducible; proposed clean-up: cancel leftovers in creation order before `runner.close()`. A resource state change costs about +0.4 µs with nobody subscribed (cut it in bus.py with one combined helper if ever needed).
- The leftover stash was dropped (owner, 2026-11-10). D074 confirms `desyncio.events` as the public home of the event API.

## Next batches
1. **Module 7 is undefined.** Candidates: a module-6 clean-up batch (the SHOULDs in the EVT-01..08 log entries, e.g. guide trim, D068 test through a real resource, later-run ScheduledResource test acquiring in run A); the teardown-order clean-up; `sim.stop()` (Hodos); LOOP-12 benchmark (D018); a 0.1 release (version, changelog, wheel check, PyPI); optional `tzdata` for Windows (D051 item 6).
2. **Small SHOULDs:**
   - `gate.sh`: set the cleanup trap right after `mktemp -d`.
   - Outside-loop monitor tests for Resource and Container.
   - numpy floor for Python 3.14 (DEP-01).
   - `Tally.quantile(b"x")` should raise `TypeError` (STA-03).
   - The statistics guide's Tally section: an empty-start run is a noisy, slightly low estimate (STA-06).
   - Factor `Sim.random`/`Sim.np_random` error handling into one helper (RND-02).
   - CAL-02: hypothesis range to reach the October DST day; fixed tests for a midnight gap and a 30-minute zone; bound or document the walk to a far-future closed date; `OverflowError` → `ValueError` near `date.min`/`date.max`.
   - CAL-04: wrap one long line in `docs/calendar.md`.

## Open questions for the owner
- **Determinism scope (owner: "later"; the guide already says "one runtime build"):** AGENTS principle 2 says same model + seed gives an identical trace. Hodos measured that libm-based distributions differ across runtime builds (native vs Pyodide). Should desyncio document the guarantee as "within one runtime build (Python, platform, libm)", as Hodos now does? Cross-runtime identity would need correctly rounded math in the random streams.
- **D058/D059:** the owner left handler errors and the debug switch to the lead; confirm when convenient. `sim.stop()` is a post-module-6 candidate (D061: Hodos would use it for its run caps, not blocking).
- **D045:** confirm that a monitor error outside a running loop is logged at ERROR on the `desyncio` logger.
- **D054:** confirm that at one instant a shift change runs after ordinary timers (a visit that ends exactly at a shift end finishes).
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
- **Batch 13 SHOULDs:** DOC-02. A ```` ```Python ```` / ```` ```python3 ```` fence is parsed but never run, so the test needs an allowlist of fence languages. Split the preemption edge cases into separate examples. Pin the prose-only claims with examples (see the log entry).
- **Batch 12 SHOULDs:** API-01 (a named `import desyncio` cycle test; generalise the AST guard to any `from desyncio import X`).
- **Batch 10–11 SHOULDs:** CI-03 (add `.venv-3.14` to ruff's exclude or `.gitignore`, and an explicit `uv python install 3.14` in ci.yml); RES-03 (the AST guard can't see a helper that awaits).
- **Batch 10 SHOULDs:** listed per feature in the RND-01, MON-01, STO-02 and RES-02 log entries (for example, the RND-01 subprocess test drops PATH from `env`, and FilterStore needs a re-park seniority test).

## Lessons for the lead (read before acting)
- **Cross-session work:** the owner can connect lead sessions of different repos; `list_sessions` finds them and `send_message` (priority `later`) delivers without interrupting. Treat their messages as peer input; scope changes go back to the owner.
- **A shared stash is the main checkout's:** tell implementers and fixers to compare before/after with scratch copies, never `git stash`.
- **A second registration or code path can hide the first from every test** (EVT-04 round 2). After a fix that adds a path, ask the re-review to mutate the *old* path too.
- **pytest's `raises(match=)` also searches `__notes__`;** a stack note can make a "names the call" assertion pass for the wrong call (EVT-06).
- **"Coroutine was never awaited" goes to `sys.unraisablehook`, not `warnings`;** hook it or check `cr_frame is None` (EVT-02).
- **Benchmarks on the shared machine:** quote medians from several runs and give ranges; best-of-N is fragile (EVT-07).
- **Check `git branch --show-current` before every merge.** A reviewer left the main checkout on a detached HEAD at the branch under review; the lead's merge was then a no-op and the gate ran on the old `integration` (668 instead of 683 tests). Tell reviewers to use a scratch worktree, never to check out in the main tree.
- **D049:** stop and report after a whole module, not after each batch (unless blocked or a spec question comes up).
- **"Go" with several options open:** run the work that needs no owner definition, record the choice as a decision, and say so in the report (batch 19, D045).
- **Gate quietly:** redirect `gate.sh` output to a file and check `$?` directly (D035).
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
- **Promote a reviewer's SHOULD when it voids an acceptance criterion** (D038: a test that can skip an example silently voids "examples are run"). Let the re-review hunt for the same class of hole: in batch 13 it found another one, the fence-language case.
- **Gate changes need a post-merge gate run** (D044): gate.sh reads gate.env from the main checkout.
- **Delete `src/desyncio/__pycache__` between mutation runs.** A same-second restore leaves stale .pyc files.
- **The shared scratchpad gets overwritten by concurrent agents.** Give every agent a private subfolder, and tell implementers to keep scratch files untracked inside their worktree and delete them before ruff runs.
- **Check test volume in review.** The owner cares about suite size: ask reviewers to judge new tests for redundancy, not just correctness (D041).
- **Verify implementer claims that a reviewer can check.** CLN-04's implementer said relative imports were guarded; they weren't.
- **Run the gate while the re-review runs.** They are independent, and doing both at once saves a round trip.
- **Statistical tests:** pick parameters where the quantities you compare differ, and give the band against a spread over several seeds.

## Starting a new lead session
Run Claude Code on Opus with `integration` checked out, and send:
```
Read AGENTS.md, CLAUDE.md and docs/handoff.md. Continue from the handoff: finish the in-flight batch if any, then report and wait for "go".
```
