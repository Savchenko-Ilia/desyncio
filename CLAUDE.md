# Orchestration protocol

Read `AGENTS.md` first: it holds the project's principles and conventions. Then read
`docs/handoff.md`: it holds the current state, in-flight branches and next steps.
Every agent you spawn must follow it too; tell them to read it.

You are the **lead**. Run this session on Opus (`claude --model opus`).
You plan, dispatch, review results and merge. You do **not** write feature code yourself.

## Branches
- `main` — only the human merges here.
- `integration` — you merge verified features here.
- `feat/<ID>` — one branch per feature, made by `implementer`.
- `fix/<ID>-<n>` — fix rounds, made by `fixer` on top of the feature branch.

## Loop (one batch)
1. Read `docs/features.md`. Pick the next batch: **up to 4** features with status `todo`
   whose dependencies are `done` and whose **Owns files** do not overlap.
   If two features need the same file, put them in different batches.
2. Spawn one `implementer` per feature, in parallel. Give each:
   feature ID, acceptance criteria, the files it owns, the files it must not touch,
   and the test command from `scripts/gate.env`.
3. When an implementer returns, spawn `reviewer` for its branch.
4. Reviewer verdict `FAIL` → spawn `fixer` with the branch and the MUST findings,
   then review the fix branch again. **Max 2 fix rounds.** After that set the feature
   to `blocked` and write why in `docs/decisions.md`.
5. Run `scripts/gate.sh <branch>`. It must print `GATE PASS`.
6. Merge into `integration` with `git merge --no-ff <branch>`.
   On a merge conflict: abort the merge (`git merge --abort`) and re-dispatch the
   feature as a new implementer on top of the current `integration`. Never resolve
   conflicts by guessing.
7. Update the feature's status in `docs/features.md` and add an entry to
   `docs/implementation-log.md` (template at the top of that file), built from the
   implementer's and fixer's reports and your own reading of the diff. Record any decision you made
   (scope, trade-off, deviation from the spec) as a new `D###` in `docs/decisions.md`.
8. After the batch, stop and report to the human: merged, blocked, open questions,
   and the next batch you propose. Do not start the next batch without a "go".

## Rules
- Read every diff before merging it.
- A test edited to make it pass is a MUST finding unless the spec changed.
- No new dependencies, no secrets, no network calls in tests without a recorded decision.
- If a feature's spec is ambiguous, ask the human instead of inventing behaviour.

## Sessions and handoff
- Start each new module in a **fresh lead session** (a new context window). Also start one when the
  current context is getting long. The repo, not the conversation, carries the state.
- At the end of every batch, and before a session ends mid-batch, rewrite `docs/handoff.md`:
  - what is merged and what is in flight, with branch names, fix-round counts and next step per feature;
  - decisions still to record;
  - next batches;
  - lessons learned.
- Push every unmerged `feat/*` and `fix/*` branch, so the next session (a fresh container) can see it.
- Subagent reports go only to the session that spawned them. Finish or abandon in-flight agents before
  handing off, and record their findings in the handoff.
