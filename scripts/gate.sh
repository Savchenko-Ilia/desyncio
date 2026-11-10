#!/usr/bin/env bash
# Usage: scripts/gate.sh [branch]   (default: HEAD)
# Checks a branch in a throwaway worktree: conflict markers, setup, tests (3.13, plus 3.14 if TEST_CMD_314 is set), lint.
# The commands come from scripts/gate.env in the tested tree (so a branch that changes them is gated by its own),
# else from the main checkout's gate.env, else from the built-in defaults.
# Exit 2 on failure so it can also be used as a Claude Code hook.
set -uo pipefail

BRANCH="${1:-HEAD}"
ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || { echo "GATE FAIL: not a git repo" >&2; exit 2; }

fail() { echo "GATE FAIL ($BRANCH): $*" >&2; exit 2; }

TMP="$(mktemp -d)"
git -C "$ROOT" worktree add --detach "$TMP" "$BRANCH" >/dev/null 2>&1 || fail "cannot check out branch"
trap 'git -C "$ROOT" worktree remove --force "$TMP" >/dev/null 2>&1; rm -rf "$TMP"' EXIT
cd "$TMP" || fail "cannot enter worktree"

if [ -f "$TMP/scripts/gate.env" ]; then . "$TMP/scripts/gate.env"
elif [ -f "$ROOT/scripts/gate.env" ]; then . "$ROOT/scripts/gate.env"; fi
SETUP_CMD="${SETUP_CMD:-}"
TEST_CMD="${TEST_CMD:-npm test}"
LINT_CMD="${LINT_CMD:-}"
# Extra test runs (e.g. another Python version), each in its own environment.
TEST_CMD_314="${TEST_CMD_314:-}"

if git grep -nE '^(<<<<<<<|>>>>>>>)( |$)' -- . >/dev/null 2>&1; then
  fail "conflict markers found"
fi

if [ -n "$SETUP_CMD" ]; then bash -c "$SETUP_CMD" >&2 || fail "setup failed: $SETUP_CMD"; fi
bash -c "$TEST_CMD" >&2 || fail "tests failed: $TEST_CMD"
if [ -n "$TEST_CMD_314" ]; then bash -c "$TEST_CMD_314" >&2 || fail "tests failed on Python 3.14: $TEST_CMD_314"; fi
if [ -n "$LINT_CMD" ]; then bash -c "$LINT_CMD" >&2 || fail "lint failed: $LINT_CMD"; fi

echo "GATE PASS ($BRANCH)"
