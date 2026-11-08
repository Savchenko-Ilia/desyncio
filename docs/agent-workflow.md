# Agent workflow

desyncio is built in small batches by Claude Code agents. `CLAUDE.md` is the lead's protocol.

| Role | Model | Does | Edits code |
|---|---|---|---|
| lead (main session) | Opus | picks a batch from `docs/features.md`, dispatches, reads every diff, merges into `integration` | docs and merges only |
| `implementer` | Sonnet | one feature, test first, in its own worktree | its owned files only |
| `reviewer` | Sonnet | read-only audit: PASS/FAIL with MUST/SHOULD findings | no |
| `fixer` | Opus | fixes MUST findings, each with a test that fails without the fix | `fix/*` branch |
| `scripts/gate.sh` | — | fresh worktree: conflict markers, install, tests, lint | — |

## Branches
- `main`: protected; the owner merges `integration` into it via PR.
- `integration`: verified features land here.
- `feat/<ID>`, `fix/<ID>-<n>`: per feature and per fix round.

If a cloud session can only push its own `claude/*` branch, the lead uses that branch as the
integration branch and opens a PR to `main` from it.

## Running a batch
Start a Claude Code session (cloud or local, on Opus) and send:

```
Read AGENTS.md, CLAUDE.md and docs/features.md. Run the next batch.
```

The lead stops after each batch with a report and waits for "go".
For every merged feature it adds an entry to `docs/implementation-log.md`: how it was built,
the asyncio internals involved, trade-offs and review findings.
