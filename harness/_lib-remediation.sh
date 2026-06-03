#!/usr/bin/env bash
# _lib-remediation.sh — shared remediation-message helper for HQ hook scripts.
#
# Sensors that emit prompts, not errors. See
# ~/Desktop/hq/plans/harness-master-plan-2026-05-05.md §G6.
#
# SOURCE, don't exec this file:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   # shellcheck source=/dev/null
#   source "$SCRIPT_DIR/_lib-remediation.sh"
#
# Functions defined here:
#   emit_remediation <why> <action> [reference]

# emit_remediation — write a structured remediation block to stderr.
#
# Args:
#   $1 = why       — one paragraph explaining what was detected / blocked
#   $2 = action    — concrete next step for the agent to take
#   $3 = reference — (optional) file path or doc anchor for further reading
#
# Output format (written to STDERR only):
#   ## Hook output
#   <why>
#
#   **Action:** <action>
#   **Reference:** <reference>   (only when $3 is non-empty)
#
# Invariants:
#   - No output to stdout.
#   - No globals created or modified.
#   - Safe under set -uo pipefail in the caller.
#   - No external dependencies (bash builtins + printf only).
emit_remediation() {
    local _why="${1:-}"
    local _action="${2:-}"
    local _ref="${3:-}"
    {
        printf '## Hook output\n'
        printf '%s\n' "$_why"
        printf '\n'
        printf '**Action:** %s\n' "$_action"
        if [ -n "$_ref" ]; then
            printf '**Reference:** %s\n' "$_ref"
        fi
    } >&2
}
