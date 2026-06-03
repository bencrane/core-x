#!/usr/bin/env bash
# PreToolUse hook — sprint-contract enforcement guard (G7 + G7.1).
#
# Blocks Write / Edit / MultiEdit / NotebookEdit tool calls that target a
# code-repo file path when no sprint contract (contract.md) exists for the
# active /scope cycle, AND the current session is the executor running that
# cycle (G7.1 cycle-scope check).
#
# Active-cycle detection algorithm:
#   1. Iterate ~/Desktop/hq/scope-status/*/validator.json sorted by mtime (newest first).
#   2. For each: check validator_frozen_at != null (validator has signed off).
#   3. For each frozen: check ~/Desktop/hq/reports/*scope-<slug>-*.md glob
#      — if any match, the cycle is closed; skip.
#   4. First match (frozen, no report) is the active slug.
#   5. If no candidate matches: fail-open (exit 0 — no active cycle to enforce).
#
# Cycle-scope check (G7.1, added 2026-05-06 after observed footgun):
#   The original G7 hook fired globally whenever any cycle had a missing
#   contract, blocking ALL sessions including unrelated ones. Empirical:
#   a stuck g9 cycle (validator stamped, no contract written) blocked an
#   unrelated FMCSA-entities session in a different worktree.
#   Fix: only enforce when the current session is the executor running the
#   active cycle. The executor registers its $PWD at
#   ~/Desktop/hq/scope-status/<slug>/executor-pwd.txt as its first action.
#   The hook reads this and compares to the current session's git toplevel.
#   If executor-pwd.txt is absent (executor hasn't registered, or this cycle
#   is in a state where no executor is active) → fail-open (exit 0).
#
# Once an active cycle is identified AND the current session is its executor:
#   - If ~/Desktop/hq/scope-status/<slug>/contract.md exists: exit 0 (allow).
#   - If not: block with exit 2 + remediation prose.
#
# Vault-path allowlist (exits 0 immediately):
#   ~/Desktop/hq/*  and  ~/.claude/*
#   Writes to ~/hq-all/... (including migration-checks/) are code-repo
#   paths and DO follow contract enforcement.
#
# Receives {tool_name, tool_input: {file_path, ...}} on stdin.
# Exit 0 = allow.  Exit 2 with stderr = block (Claude sees the message).
#
# Fail-open: if jq is missing, stdin is malformed, python3 is unavailable,
# or we cannot read any scope-status directory, we allow the call.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/_lib-remediation.sh"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/_lib-active-cycle.sh"

INPUT=$(cat)

# Parse tool_name + file_path out of the JSON payload.
PARSED=$(printf '%s' "$INPUT" | python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read())
    name = d.get('tool_name', '') or ''
    fp = (d.get('tool_input') or {}).get('file_path', '') or ''
    print(name + '\t' + fp)
except Exception:
    print('\t')
" 2>/dev/null || printf '\t')

TOOL_NAME=${PARSED%%$'\t'*}
FILE_PATH=${PARSED#*$'\t'}

# Matcher early-exit: only intercept Write/Edit/MultiEdit/NotebookEdit.
case "$TOOL_NAME" in
    Write|Edit|MultiEdit|NotebookEdit) ;;
    *) exit 0 ;;
esac

# Fail-open on empty file path.
[ -z "$FILE_PATH" ] && exit 0

# Vault-path allowlist: ~/Desktop/hq/* and ~/.claude/* are vault writes,
# not code-repo writes. Let them through unconditionally.
case "$FILE_PATH" in
    "$HOME/Desktop/hq/"*|"$HOME/.claude/"*)
        exit 0
        ;;
esac

# Require jq for on-disk JSON inspection; fail-open if missing.
command -v jq >/dev/null 2>&1 || exit 0

# ── Active-cycle detection ─────────────────────────────────────────────────
# Sourced from _lib-active-cycle.sh (shared with hook-pretooluse-executor-pwd-register.sh).
# Both hooks use the same algorithm so they always agree on which slug is active.
SCOPE_STATUS_DIR="$HOME/Desktop/hq/scope-status"
REPORTS_DIR="$HOME/Desktop/hq/reports"

ACTIVE_SLUG=$(get_active_slug "$SCOPE_STATUS_DIR" "$REPORTS_DIR")

# No active cycle found: fail-open.
[ -z "$ACTIVE_SLUG" ] && exit 0

# ── Cycle-scope check (G7.1) ───────────────────────────────────────────────
# Only enforce the contract gate on sessions running inside the cycle's
# executor worktree. Without this scope check, the hook blocks ALL sessions
# globally whenever any cycle is mid-flight — observed footgun 2026-05-06.
EXECUTOR_PWD_FILE="$SCOPE_STATUS_DIR/$ACTIVE_SLUG/executor-pwd.txt"

if [ ! -f "$EXECUTOR_PWD_FILE" ]; then
    # No executor has registered its worktree path. Fail-open: under-enforce
    # (executor may run without contract gate) is preferable to over-enforce
    # (block unrelated sessions).
    exit 0
fi

EXECUTOR_PWD=$(head -n 1 "$EXECUTOR_PWD_FILE" | sed -e 's/[[:space:]]*$//')
[ -z "$EXECUTOR_PWD" ] && exit 0  # malformed marker, fail-open.

# Resolve current PWD's git toplevel (cwd may be deeper than worktree root).
CURRENT_TOPLEVEL=$(git -C "$PWD" rev-parse --show-toplevel 2>/dev/null || echo "")
[ -z "$CURRENT_TOPLEVEL" ] && exit 0  # not in a git repo, fail-open.

# Match: current session's toplevel must equal the executor's registered worktree
# (or the current PWD is somewhere inside it).
case "$CURRENT_TOPLEVEL" in
    "$EXECUTOR_PWD"|"$EXECUTOR_PWD"/*) ;;
    *) exit 0 ;;  # different worktree; not this cycle's executor session.
esac

# ── Contract check ─────────────────────────────────────────────────────────
CONTRACT_PATH="$SCOPE_STATUS_DIR/$ACTIVE_SLUG/contract.md"

if [ -f "$CONTRACT_PATH" ]; then
    exit 0  # Contract exists — allow.
fi

# Contract missing for active cycle: block with remediation.
emit_remediation \
    "BLOCKED: /scope cycle ${ACTIVE_SLUG} has no sprint contract. A /scope cycle is active (validator.json is frozen at ${SCOPE_STATUS_DIR}/${ACTIVE_SLUG}/validator.json, no cycle-report exists yet) but the sprint contract file is missing at ${CONTRACT_PATH}. Per G7 enforcement, the Stage 3 executor's first code-repo write is blocked until contract.md exists. This prevents the executor from starting implementation work without an explicit sprint contract that defines the verification criteria and out-of-scope boundaries for this cycle." \
    "Create the sprint contract before making code changes: write ${CONTRACT_PATH} with the per-step verifier criteria for this /scope cycle. The validator (Stage 2) is responsible for writing contract.md at signoff alongside validator.json. If the validator already ran but did not write contract.md, have it write the contract now before proceeding." \
    "$SCOPE_STATUS_DIR/$ACTIVE_SLUG/validator.json"

exit 2
