#!/usr/bin/env bash
# PreToolUse hook — directive Predecessor gate (F3).
#
# Gates Write|Edit|MultiEdit|NotebookEdit calls targeting files under
# ~/Desktop/hq/directives/ (any depth, *.md). For each such call, it projects
# the post-write content according to CC tool semantics (mirrors F2/G5 shape),
# writes the projection to a temp file, and invokes:
#   ~/hq-all/scripts/scope-precheck-predecessor.sh <tmpfile>
# and translates the script's exit code into the hook's verdict:
#   rc=0  predecessor field valid (none OR resolves to existing directive)  -> exit 0 (allow)
#   rc=1  predecessor exists but Status not in {complete, runtime-verified} -> exit 0 (allow;
#         F3 validates AUTHORING, not predecessor lifecycle - the validator's
#         Pre-flight 0 owns lifecycle gating)
#   rc=2  predecessor file unreadable (path resolved, file missing)         -> exit 2 (block)
#   rc=3  Predecessor field malformed/missing                               -> exit 2 (block)
#
# Activates the cross-cycle attribution chain: 11 of 13 recent directives had
# Predecessor: none, breaking scope-attribute-prior.sh's ability to grade
# prior cycles (audit: ~/Desktop/hq/inventory/PHASE-1-ENFORCEMENT-AUDIT-2026-05-06.md).
#
# Allowed forms (script rc=0):
#   - `none` (case-insensitive, with optional parenthetical, e.g.
#     `none (first cycle of <topic>)`)
#   - Any path that resolves to an existing directive file. Script handles
#     bare absolute, file:// markdown link, tilde-prefixed, bare relative,
#     backtick relative, markdown-link relative shapes.
#
# Projection semantics (mirror F2):
#   Write:        projected content = tool_input.content
#   Edit:         projected content = on_disk.replace(old_string, new_string, 1)
#   MultiEdit:    sequential fold: state = on_disk; for each edit, single-replace
#   NotebookEdit: directives are .md, never .ipynb -> fail-open (exit 0)
#
# Fail-open:
#   - python3 missing
#   - stdin malformed
#   - on-disk read fails (Edit on missing file)
#   - projected content can't be written to temp
#   - precheck script not executable
#   - any unhandled exception
#
# Bypass note: this hook intercepts only Write|Edit|MultiEdit|NotebookEdit.
# Bash redirections (`echo > directives/foo.md`) and external editors (vim)
# bypass it entirely. Documented out-of-scope per F3 directive.
#
# Receives {tool_name, tool_input: {...}} on stdin.
# Exit 0 = allow. Exit 2 with stderr = block. No other exit codes.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/_lib-remediation.sh"

PRECHECK="$SCRIPT_DIR/scope-precheck-predecessor.sh"

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

# NotebookEdit on a directive .md is impossible (.md != .ipynb) - fail-open.
[ "$TOOL_NAME" = "NotebookEdit" ] && exit 0

# Fail-open on empty file path.
[ -z "$FILE_PATH" ] && exit 0

# File-path filter: must be under $HOME/Desktop/hq/directives/ (any depth) AND end with .md.
DIRECTIVES_PREFIX="$HOME/Desktop/hq/directives/"
case "$FILE_PATH" in
    "$DIRECTIVES_PREFIX"*.md|"$DIRECTIVES_PREFIX"*/*.md) ;;
    *) exit 0 ;;
esac

# Require precheck script.
[ -x "$PRECHECK" ] || exit 0

# Require python3.
command -v python3 >/dev/null 2>&1 || exit 0

# ── Project post-write content ───────────────────────────────────────────────
# Mirrors F2's projection logic exactly. Returns "ALLOW" or "PROJECT:<base64>"
# (base64 to avoid shell-quoting hell on multi-line markdown content).
PROJECTION=$(python3 -c "
import json, sys, base64

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
                return 'ALLOW'
            if old_str not in on_disk:
                return 'ALLOW'
            projected = on_disk.replace(old_str, new_str, 1)

        elif tool_name == 'MultiEdit':
            edits = tool_input.get('edits', [])
            if not isinstance(edits, list):
                return 'ALLOW'
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    state = f.read()
            except Exception:
                return 'ALLOW'
            for edit in edits:
                if not isinstance(edit, dict):
                    return 'ALLOW'
                old_str = edit.get('old_string', '')
                new_str = edit.get('new_string', '')
                if old_str is None or new_str is None:
                    return 'ALLOW'
                if old_str not in state:
                    return 'ALLOW'
                state = state.replace(old_str, new_str, 1)
            projected = state

        else:
            return 'ALLOW'

        # Encode projected content as base64 to safely return through shell.
        encoded = base64.b64encode(projected.encode('utf-8')).decode('ascii')
        return 'PROJECT:' + encoded

    except Exception:
        return 'ALLOW'

print(main(sys.argv[1], sys.argv[2]))
" "$INPUT" "$FILE_PATH" 2>/dev/null || echo "ALLOW")

# Fail-open if projection failed.
if [ -z "$PROJECTION" ] || [ "$PROJECTION" = "ALLOW" ]; then
    exit 0
fi

# Decode projected content into a temp file.
case "$PROJECTION" in
    PROJECT:*) ENCODED="${PROJECTION#PROJECT:}" ;;
    *) exit 0 ;;
esac

TMPFILE=$(mktemp -t f3-projected.XXXXXX 2>/dev/null) || exit 0
trap 'rm -f "$TMPFILE"' EXIT

if ! printf '%s' "$ENCODED" | base64 -d >"$TMPFILE" 2>/dev/null; then
    exit 0  # base64 decode failed: fail-open.
fi

# ── Invoke precheck script and translate exit code ──────────────────────────
SCRIPT_RC=0
"$PRECHECK" "$TMPFILE" >/dev/null 2>&1 || SCRIPT_RC=$?

case "$SCRIPT_RC" in
    0|1)
        # rc=0: predecessor OK (none or resolves complete/runtime-verified)
        # rc=1: predecessor resolves but Status not complete - F3 allows; lifecycle is validator's job
        exit 0
        ;;
    2)
        # Predecessor file unreadable (path resolved, file missing).
        emit_remediation \
            "F3 directive Predecessor gate: write to ${FILE_PATH} blocked. The directive's Predecessor field references a path that does not exist on disk. Per /scope SKILL.md Stage 1 directive template, the Predecessor field must be either an explicit first-cycle marker (none, or none (first cycle of <topic>)) OR a path that resolves to an existing directive. The path you wrote does not resolve. This blocks scope-attribute-prior.sh from loading prior-cycle predictions. Audit: 11 of 13 recent directives had Predecessor: none - F3 forces conscious authoring." \
            "Fix the Predecessor field in your directive to one of the valid forms: (1) none (first cycle of <topic>) - explicit first-cycle marker; (2) an absolute, tilde, file://, or relative path to an existing directive file. See /Users/benjamincrane/.claude/skills/scope/SKILL.md Stage 1 directive template for the full contract." \
            "${FILE_PATH}"
        exit 2
        ;;
    3)
        # Predecessor field malformed/missing entirely.
        emit_remediation \
            "F3 directive Predecessor gate: write to ${FILE_PATH} blocked. The directive's Predecessor field is missing, empty, a placeholder (TODO, <TBD>, etc.), or otherwise malformed. Per /scope SKILL.md Stage 1 directive template, every directive MUST declare a Predecessor - either an explicit first-cycle marker (none, or none (first cycle of <topic>)) OR a real predecessor path. Audit: 11 of 13 recent directives had Predecessor: none, breaking cross-cycle attribution at the directive-authoring layer. F3 forces conscious authoring." \
            "Add a valid **Predecessor:** line to your directive. Two valid forms: (1) none (first cycle of <topic>) - explicit first-cycle marker; (2) an absolute, tilde, file://, or relative path to an existing directive file. See /Users/benjamincrane/.claude/skills/scope/SKILL.md Stage 1 directive template." \
            "${FILE_PATH}"
        exit 2
        ;;
    *)
        # Unknown exit code: fail-open.
        exit 0
        ;;
esac
