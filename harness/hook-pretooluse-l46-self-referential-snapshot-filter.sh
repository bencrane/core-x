#!/usr/bin/env bash
# PreToolUse hook — L46 self-referential snapshot-filter guard.
#
# Blocks Write|Edit|MultiEdit calls that introduce a `CREATE MATERIALIZED VIEW`
# whose body contains a self-referential `WHERE col = (SELECT max(col) FROM
# same_source)` filter against the same source the MV reads from.
#
# Per DATA-FACTORY-LESSONS-LEARNED.md L46 (RW 2.8.x cluster behavior):
# self-referential subqueries on append-only S3_V2 sources trigger
# DIRTY_STREAM_JOB_CLEAR during BACKGROUND_DDL admit. Empirically observed
# in PR #314, #317, #321 (FMCSA derivation MVs cleared mid-admit, leaving
# pg_class entries with 0 rows).
#
# Detection: the projected post-write content contains both:
#   (1) a `CREATE\s+MATERIALIZED\s+VIEW` statement, AND
#   (2) inside that statement (or anywhere in the file), a `WHERE`-clause
#       subquery of shape `WHERE\s+\w+\s*=\s*\(\s*SELECT\s+max\(.*\)\s+FROM\s+\w+`
#
# This is intentionally over-eager: same-file co-occurrence is a strong
# signal even if not strictly inside the same MV statement, because per
# L46 even adjacent CREATE MV bodies that share a source name + this
# pattern hit the same failure mode.
#
# Override: if the operator/agent has verified the pattern is safe (e.g.
# RW 2.9+ with the fix, or a non-source target), include the literal
# marker `-- L46-OK-VERIFIED` in the file. The hook honors this marker.
#
# Receives {tool_name, tool_input: {file_path, content?, old_string?,
# new_string?, edits?}} on stdin. Exit 0 = allow. Exit 2 = block.
#
# Fail-open scenarios (matches predecessor-gate convention):
#   - python3 missing
#   - stdin malformed
#   - on-disk read fails (Edit on missing file)
#   - any unhandled projection exception

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/_lib-remediation.sh"

INPUT=$(cat)

# Parse tool_name + file_path. Fail-open on parse error.
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
    Write|Edit|MultiEdit) ;;
    *) exit 0 ;;
esac

[ -z "$FILE_PATH" ] && exit 0

# File-path filter: data-engine-x .py and .sql files (where RW DDL lives).
# Match any worktree under hq-all/ — `.claude/worktrees/*/apps/data-engine-x/`
# AND the canonical location `hq-all/apps/data-engine-x/`.
case "$FILE_PATH" in
    */apps/data-engine-x/*.py|*/apps/data-engine-x/*.sql) ;;
    */apps/data-engine-x/*/*.py|*/apps/data-engine-x/*/*.sql) ;;
    */apps/data-engine-x/*/*/*.py|*/apps/data-engine-x/*/*/*.sql) ;;
    *) exit 0 ;;
esac

command -v python3 >/dev/null 2>&1 || exit 0

# ── Project post-write content ───────────────────────────────────────────────
PROJECTION=$(python3 -c "
import json, sys, base64
try:
    d = json.loads(sys.argv[1])
    tool_name = d.get('tool_name', '') or ''
    tool_input = d.get('tool_input') or {}
    file_path = sys.argv[2]
    if tool_name == 'Write':
        projected = tool_input.get('content', '') or ''
    elif tool_name == 'Edit':
        old_str = tool_input.get('old_string', '') or ''
        new_str = tool_input.get('new_string', '') or ''
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                on_disk = f.read()
        except Exception:
            print('ALLOW'); sys.exit(0)
        if old_str not in on_disk:
            print('ALLOW'); sys.exit(0)
        projected = on_disk.replace(old_str, new_str, 1)
    elif tool_name == 'MultiEdit':
        edits = tool_input.get('edits', [])
        if not isinstance(edits, list):
            print('ALLOW'); sys.exit(0)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                state = f.read()
        except Exception:
            print('ALLOW'); sys.exit(0)
        for edit in edits:
            if not isinstance(edit, dict):
                print('ALLOW'); sys.exit(0)
            o = edit.get('old_string', '') or ''
            n = edit.get('new_string', '') or ''
            if o not in state:
                print('ALLOW'); sys.exit(0)
            state = state.replace(o, n, 1)
        projected = state
    else:
        print('ALLOW'); sys.exit(0)
    print('PROJECT:' + base64.b64encode(projected.encode('utf-8')).decode('ascii'))
except Exception:
    print('ALLOW')
" "$INPUT" "$FILE_PATH" 2>/dev/null || echo "ALLOW")

if [ -z "$PROJECTION" ] || [ "$PROJECTION" = "ALLOW" ]; then
    exit 0
fi

CONTENT=$(printf '%s' "${PROJECTION#PROJECT:}" | base64 --decode 2>/dev/null || echo "")
[ -z "$CONTENT" ] && exit 0

# ── Detection ────────────────────────────────────────────────────────────────
# Honor explicit override marker.
if printf '%s' "$CONTENT" | grep -qF -- '-- L46-OK-VERIFIED'; then
    exit 0
fi

# Detect: file contains BOTH a CREATE MATERIALIZED VIEW and a WHERE x = (SELECT max(...) FROM ...) shape.
HAS_CREATE_MV=$(printf '%s' "$CONTENT" | grep -ciE 'create[[:space:]]+materialized[[:space:]]+view' || true)
HAS_SELF_REF_MAX=$(printf '%s' "$CONTENT" | grep -ciE 'where[[:space:]]+[a-z_][a-z0-9_]*[[:space:]]*=[[:space:]]*\([[:space:]]*select[[:space:]]+max\(' || true)

if [ "${HAS_CREATE_MV:-0}" -gt 0 ] && [ "${HAS_SELF_REF_MAX:-0}" -gt 0 ]; then
    SAMPLE_LINE=$(printf '%s' "$CONTENT" | grep -inE 'where[[:space:]]+[a-z_][a-z0-9_]*[[:space:]]*=[[:space:]]*\([[:space:]]*select[[:space:]]+max\(' | head -1)
    emit_remediation \
        "L46 guard: file contains both CREATE MATERIALIZED VIEW and a self-referential WHERE col = (SELECT max(...) FROM ...) subquery. Per DATA-FACTORY-LESSONS-LEARNED.md L46, this pattern triggers DIRTY_STREAM_JOB_CLEAR on RW 2.8.x during BACKGROUND_DDL admit (empirically observed in PR #314, #317, #321 — MVs end up in pg_class with 0 rows after cluster recovery). Detected near: ${SAMPLE_LINE}" \
        "Either (1) move the snapshot-latest filter outside the streaming admit path (sidecar config table, static-date literal updated weekly per L37), OR (2) if you've verified this pattern is safe in the current RW version, add the literal marker '-- L46-OK-VERIFIED' to the file." \
        "${SCRIPT_DIR}/hook-pretooluse-l46-self-referential-snapshot-filter.sh + DATA-FACTORY-LESSONS-LEARNED.md L46"
    exit 2
fi

exit 0
