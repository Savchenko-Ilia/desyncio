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
