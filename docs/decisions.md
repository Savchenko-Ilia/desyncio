# Decisions

One entry per decision the lead or the human made. Newest last.

## D001 — Process
Features are built by `implementer` subagents in worktrees, audited by a read-only
`reviewer`, fixed by `fixer`, gated by `scripts/gate.sh`, merged into `integration`
by the lead. Only the human merges `integration` into `main`.
