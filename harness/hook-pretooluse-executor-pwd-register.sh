#!/usr/bin/env bash
# PreToolUse hook — auto-register executor-pwd.txt for /scope Stage 3 executors (F1).
#
# At the Stage 3 executor's first code-repo Write/Edit/MultiEdit/NotebookEdit, this
# hook writes the git toplevel of the current $PWD to:
#   ~/Desktop/hq/scope-status/<active-slug>/executor-pwd.txt
#
# This activates the G7 sprint-contract gate (hook-pretooluse-sprint-contract.sh),
# which fails-open when executor-pwd.txt is absent. F1 ensures the file exists by
# the time sprint-contract reads it, because this hook is wired BEFORE sprint-contract
# in ~/.claude/settings.json.
#
# Three conditions must ALL be true to trigger registration:
#   1. An active /scope cycle exists (frozen validator.json, no cycle-report).
#   2. The Write/Edit target is a code-repo path (NOT a vault path under
#      ~/Desktop/hq/ or ~/.claude/).
#   3. executor-pwd.txt does not yet exist for the active slug (idempotent).
#
# When all three are true:
#   - Resolve $PWD's git toplevel via `git rev-parse --show-toplevel`.
#   - Write the toplevel to executor-pwd.txt (one line, printf '%s\n').
#
# Always exits 0 (fail-open). Never blocks tool calls.
#
# Active-cycle detection algorithm (sourced from _lib-active-cycle.sh):
#   Identical to hook-pretooluse-sprint-contract.sh. Both hooks source the same
#   shared helper so they always agree on which slug is active. If the algorithm
#   changes (e.g., a staleness cutoff), update _lib-active-cycle.sh; both hooks
#   pick it up automatically.
#
# Vault-path allowlist (exits 0 immediately — no registration):
#   ~/Desktop/hq/*  and  ~/.claude/*
#
# Receives {tool_name, tool_input: {file_path, ...}} on stdin.
# Exit 0 = allow (always).
#
# Fail-open: if jq is missing, stdin is malformed, python3 is unavailable,
# scope-status directory is unreadable, or $PWD is not in a git repo — exit 0.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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
SCOPE_STATUS_DIR="$HOME/Desktop/hq/scope-status"
REPORTS_DIR="$HOME/Desktop/hq/reports"

ACTIVE_SLUG=$(get_active_slug "$SCOPE_STATUS_DIR" "$REPORTS_DIR")

# No active cycle found: nothing to register.
[ -z "$ACTIVE_SLUG" ] && exit 0

# ── Idempotency check ─────────────────────────────────────────────────────
EXECUTOR_PWD_FILE="$SCOPE_STATUS_DIR/$ACTIVE_SLUG/executor-pwd.txt"

if [ -f "$EXECUTOR_PWD_FILE" ]; then
    exit 0  # Already registered for this cycle.
fi

# ── Resolve git toplevel ──────────────────────────────────────────────────
GIT_TOPLEVEL=$(git -C "$PWD" rev-parse --show-toplevel 2>/dev/null || echo "")
[ -z "$GIT_TOPLEVEL" ] && exit 0  # Not in a git repo — cannot register.

# ── Write registration ────────────────────────────────────────────────────
# Use printf '%s\n' to avoid trailing-newline inconsistencies.
# sprint-contract reads this via `head -n 1 | sed -e 's/[[:space:]]*$//'`
# which tolerates a trailing newline, but we're explicit anyway.
printf '%s\n' "$GIT_TOPLEVEL" > "$EXECUTOR_PWD_FILE" 2>/dev/null || exit 0

exit 0
