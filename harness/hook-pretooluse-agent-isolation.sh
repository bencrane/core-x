#!/usr/bin/env bash
# PreToolUse hook — Agent nested-worktree isolation guard (G11).
#
# Blocks Agent tool dispatches that specify isolation="worktree" when the hook
# is already running inside a git worktree. Creating a nested worktree would
# trip the WorktreeCreate failure; the executor should instead branch inside
# the existing worktree.
#
# Receives {tool_name, tool_input} JSON on stdin.
# Exit 0 = allow.  Exit 2 with stderr = block (Claude sees the message).
#
# Fail-open: if stdin is malformed or cannot be parsed, exit 0 (allow).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/_lib-remediation.sh"

INPUT=$(cat)

# Parse tool_name and tool_input.isolation from the JSON payload.
PARSED=$(printf '%s' "$INPUT" | python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read())
    name = d.get('tool_name', '') or ''
    isolation = (d.get('tool_input') or {}).get('isolation', '') or ''
    print(name + '\t' + isolation)
except Exception:
    print('\t')
" 2>/dev/null || printf '\t')

TOOL_NAME=${PARSED%%$'\t'*}
ISOLATION=${PARSED#*$'\t'}

# Only intercept Agent tool calls.
[ "$TOOL_NAME" = "Agent" ] || exit 0

# Only intercept when isolation=worktree is requested.
[ "$ISOLATION" = "worktree" ] || exit 0

# Detect nested-worktree state via env var OR git path.
NESTED=0
if [ -n "${CLAUDE_WORKTREE:-}" ]; then
    NESTED=1
elif git rev-parse --git-dir 2>/dev/null | grep -q '/worktrees/'; then
    NESTED=1
fi

[ "$NESTED" -eq 1 ] || exit 0

# We are nested + isolation=worktree was requested — block with guidance.
CURRENT_BRANCH=$(git branch --show-current 2>/dev/null || echo "unknown")

emit_remediation \
    "Nested-worktree creation blocked (G11). The orchestrator is already running inside a git worktree at ${PWD} on branch ${CURRENT_BRANCH}. Dispatching an executor with isolation=\"worktree\" would attempt to create a nested worktree, which the WorktreeCreate hook rejects. This is the G11 scenario: the parent worktree IS the isolation boundary." \
    "Drop the isolation=\"worktree\" argument from the Agent dispatch. The executor should work directly inside this existing worktree (${PWD}) on the current branch (${CURRENT_BRANCH}), or on a new branch created with git checkout -b inside this worktree. Do NOT run git worktree add." \
    "~/.claude/skills/scope/SKILL.md §G11"

exit 2
