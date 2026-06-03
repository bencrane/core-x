#!/usr/bin/env bash
# PreToolUse hook — validator-freeze content gate (F2).
#
# When a Write|Edit|MultiEdit|NotebookEdit would transition validator_frozen_at
# from null/missing to non-null in a scope-status/<slug>/validator.json file,
# this hook validates that the projected post-write content also has:
#   - predictions[] as a non-empty array (length >= 1)
#   - success_threshold as a non-null, non-empty string
#
# If either is missing, the hook blocks (exit 2) with remediation prose that
# references SKILL.md Stage 2 and names the missing fields.
#
# Edits that do NOT set validator_frozen_at, or that already have both fields
# populated, pass through (exit 0).
#
# Cooperates with G5 (hook-pretooluse-frozen-validator.sh):
#   G5 blocks edits to ALREADY-FROZEN files (on-disk frozen_at non-null).
#   F2 blocks freeze TRANSITIONS (on-disk frozen_at null → projected non-null).
#   C13: if on-disk file is already frozen, F2 exits 0 immediately (yields to G5).
#
# Projection semantics (mirrors CC tool behavior exactly):
#   Write:     projected content = tool_input.content
#   Edit:      projected content = on_disk.replace(old_string, new_string, 1)
#   MultiEdit: sequential fold: for each {old_string,new_string} in edits[],
#              apply single-replace in order (each step's output feeds next)
#   NotebookEdit: validator.json is never a notebook → fail-open (exit 0)
#
# Fail-open: if projected content fails to parse as JSON, exit 0.
# If on-disk content can't be read (Edit on missing file), exit 0.
# If python3 missing, exit 0. If stdin malformed, exit 0.
#
# Bypass note: this hook intercepts Write|Edit|MultiEdit|NotebookEdit only.
# Bash-redirect writes (> validator.json) are NOT intercepted — documented
# bypass. See F2 directive, out-of-scope section.
#
# Schema validated: predictions[] count and success_threshold non-empty only.
# Per-prediction shape (constraint_level, id, description, etc.) is NOT
# validated here. SKILL.md prose continues to govern per-prediction shape.
#
# Receives {tool_name, tool_input: {...}} on stdin.
# Exit 0 = allow. Exit 2 with stderr = block. No other exit codes.
#
# Mirrors CC Edit semantics as of 2026-05-06; revisit if Edit tool behavior changes.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/_lib-remediation.sh"

INPUT=$(cat)

# Parse tool_name + file_path from stdin JSON. Fail-open on any parse error.
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

# Matcher: only intercept file-write tools.
case "$TOOL_NAME" in
    Write|Edit|MultiEdit|NotebookEdit) ;;
    *) exit 0 ;;
esac

# Fail-open on empty file path.
[ -z "$FILE_PATH" ] && exit 0

# Only care about files named exactly "validator.json".
BASENAME=$(basename "$FILE_PATH")
[ "$BASENAME" = "validator.json" ] || exit 0

# Only care about paths matching */scope-status/<slug>/validator.json.
# Parent dir must be a direct child of a directory named "scope-status".
PARENT=$(dirname "$FILE_PATH")
GRANDPARENT=$(dirname "$PARENT")
GRANDPARENT_BASENAME=$(basename "$GRANDPARENT")
[ "$GRANDPARENT_BASENAME" = "scope-status" ] || exit 0

# NotebookEdit: validator.json is never a notebook — fail-open.
[ "$TOOL_NAME" = "NotebookEdit" ] && exit 0

# Require jq for on-disk JSON inspection; fail-open if missing.
command -v jq >/dev/null 2>&1 || exit 0

# ── C13: on-disk pre-state check ─────────────────────────────────────────────
# If the on-disk file already has validator_frozen_at non-null, this is a
# POST-freeze edit. Yield to G5 (which owns post-freeze blocking). Exit 0.
# Uses same jq predicate as G5 so they agree on "already frozen".
if [ -f "$FILE_PATH" ]; then
    if jq -e '.validator_frozen_at != null' "$FILE_PATH" >/dev/null 2>&1; then
        exit 0  # Already frozen: G5 owns this case. F2 yields.
    fi
fi

# ── Project post-write content and validate ───────────────────────────────────
# Pass INPUT as positional arg $1 and FILE_PATH as $2 to avoid heredoc-vs-stdin
# conflict (python3 -c reads code inline; stdin is not used for the script itself).
#
# Projection algorithm:
#   Write:     content = tool_input.content (no on-disk read needed)
#   Edit:      content = on_disk.replace(old_string, new_string, 1)  [count=1: CC semantics]
#   MultiEdit: state = on_disk; for edit in edits: state = state.replace(old, new, 1)
#
# Worked example:
#   Write: content='{"validator_frozen_at":"2026-05-06T00:00:00Z","predictions":[]}' → BLOCK
#   Edit:  on_disk has frozen_at=null, Edit sets it to timestamp, predictions still [] → BLOCK
#   MultiEdit B6: edit-1 sets intermediate text, edit-2 (operates on edit-1 output) sets
#     frozen_at; because edit-2's old_string only exists after edit-1, parallel application
#     would fail-open. Sequential fold finds the text → block on empty predictions.

RESULT=$(python3 -c "
import json, sys

def main(input_json, file_path):
    try:
        d = json.loads(input_json)
        tool_name = d.get('tool_name', '') or ''
        tool_input = d.get('tool_input') or {}
    except Exception:
        return 'ALLOW'

    try:
        if tool_name == 'Write':
            content = tool_input.get('content', '')
            if content is None:
                return 'ALLOW'
            projected = content

        elif tool_name == 'Edit':
            old_str = tool_input.get('old_string', '')
            new_str = tool_input.get('new_string', '')
            if old_str is None or new_str is None:
                return 'ALLOW'
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    on_disk = f.read()
            except Exception:
                return 'ALLOW'  # File missing or unreadable: fail-open (N4).
            if old_str not in on_disk:
                return 'ALLOW'  # old_string not present: fail-open.
            projected = on_disk.replace(old_str, new_str, 1)  # count=1: CC semantics

        elif tool_name == 'MultiEdit':
            edits = tool_input.get('edits', [])
            if not isinstance(edits, list):
                return 'ALLOW'
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    state = f.read()
            except Exception:
                return 'ALLOW'
            # Sequential fold: each edit on prior step's output.
            for edit in edits:
                if not isinstance(edit, dict):
                    return 'ALLOW'
                old_str = edit.get('old_string', '')
                new_str = edit.get('new_string', '')
                if old_str is None or new_str is None:
                    return 'ALLOW'
                if old_str not in state:
                    return 'ALLOW'  # old_string not found: fail-open.
                state = state.replace(old_str, new_str, 1)
            projected = state

        else:
            return 'ALLOW'  # Unknown tool: fail-open.

        # Parse projected content as JSON.
        try:
            data = json.loads(projected)
        except Exception:
            return 'ALLOW'  # Projected content not valid JSON: fail-open (N1).

        # Is this a freeze transition? (projected frozen_at non-null)
        frozen_at = data.get('validator_frozen_at')
        if frozen_at is None:
            return 'ALLOW'  # Not setting frozen_at: not a freeze transition.

        # It's a freeze transition. Check predictions and success_threshold.
        predictions = data.get('predictions')
        success_threshold = data.get('success_threshold')

        predictions_ok = (
            isinstance(predictions, list) and len(predictions) >= 1
        )
        threshold_ok = (
            isinstance(success_threshold, str) and
            len(success_threshold.strip()) > 0
        )

        if predictions_ok and threshold_ok:
            return 'ALLOW'

        # Build specific reason for the block.
        missing = []
        if not predictions_ok:
            missing.append('predictions')
        if not threshold_ok:
            missing.append('success_threshold')
        return 'BLOCK:' + ','.join(missing)

    except Exception:
        return 'ALLOW'  # Any unhandled exception: fail-open.

print(main(sys.argv[1], sys.argv[2]))
" "$INPUT" "$FILE_PATH" 2>/dev/null || echo "ALLOW")

# Fail-open if projection script failed to run.
if [ -z "$RESULT" ]; then
    exit 0
fi

# Allow case.
if [ "$RESULT" = "ALLOW" ]; then
    exit 0
fi

# Block case: RESULT starts with "BLOCK:".
if [[ "$RESULT" == BLOCK:* ]]; then
    MISSING_FIELDS="${RESULT#BLOCK:}"
    emit_remediation \
        "F2 validator-freeze content gate: freeze transition blocked. The write to ${FILE_PATH} would set validator_frozen_at to a non-null value (freeze transition), but the projected content is missing required schema fields: ${MISSING_FIELDS}. The /scope SKILL.md Stage 2 contract requires that validator.json includes BOTH a non-empty predictions[] array AND a non-null, non-empty success_threshold string before the validator freezes the scoreboard. These fields were populated in only 1 of 8 cycles since G2 shipped (audit: ~/Desktop/hq/inventory/PHASE-1-ENFORCEMENT-AUDIT-2026-05-06.md). Required fields: predictions (non-empty array of prediction objects), success_threshold (non-empty string)." \
        "Before freezing validator.json, populate: (1) predictions[] — an array of prediction objects per SKILL.md Stage 2 step 7 (each with id, description, files, failure_pattern, predicted_fixes, risk_tasks, constraint_level); (2) success_threshold — a non-empty string describing the acceptance criteria. See SKILL.md Stage 2 contract at /Users/benjamincrane/.claude/skills/scope/SKILL.md for the full schema." \
        "${FILE_PATH}"
    exit 2
fi

# Fallthrough: fail-open.
exit 0
