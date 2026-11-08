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
