#!/usr/bin/env bash
# PreToolUse hook — L40 RW source over .parquet.zst guard.
#
# Blocks Write|Edit|MultiEdit calls that introduce a RisingWave `CREATE SOURCE`
# DDL whose `match_pattern` ends in `.parquet.zst`.
#
# Per DATA-FACTORY-LESSONS-LEARNED.md L40: RW's PARQUET encoder cannot read
# whole-file zstd-wrapped Parquet. Sources pointing at .parquet.zst keys
# silently catalog but error on every read with "invalid seek to a negative
# position." Empirically observed today on source_fmcsa_authhist,
# source_fmcsa_company_census, source_fmcsa_authhist_all_with_history.
#
# The right pattern (per L40): a derivation pipeline (DuckDB-on-R2) reads
# the .parquet.zst file and writes a plain .parquet sibling for the RW
# source to consume.
#
# Detection: projected post-write content contains:
#   `CREATE\s+SOURCE` followed (within ~20 lines) by
#   `match_pattern\s*=\s*['\"][^'\"]*\.parquet\.zst['\"]`
#
# Override: include the literal marker `-- L40-OK-VERIFIED` in the file
# (e.g. for legacy dead-cataloged sources documented as such, or RW
# versions where the encoder issue is fixed).
#
# Receives {tool_name, tool_input: {file_path, content?, old_string?,
# new_string?, edits?}} on stdin. Exit 0 = allow. Exit 2 = block.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/_lib-remediation.sh"

INPUT=$(cat)

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

case "$TOOL_NAME" in
    Write|Edit|MultiEdit) ;;
    *) exit 0 ;;
esac

[ -z "$FILE_PATH" ] && exit 0

case "$FILE_PATH" in
    */apps/data-engine-x/*.py|*/apps/data-engine-x/*.sql) ;;
    */apps/data-engine-x/*/*.py|*/apps/data-engine-x/*/*.sql) ;;
    */apps/data-engine-x/*/*/*.py|*/apps/data-engine-x/*/*/*.sql) ;;
    *) exit 0 ;;
esac

command -v python3 >/dev/null 2>&1 || exit 0

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

# Honor override marker.
if printf '%s' "$CONTENT" | grep -qF -- '-- L40-OK-VERIFIED'; then
    exit 0
fi

# Detect: CREATE SOURCE in file AND match_pattern referencing .parquet.zst
HAS_CREATE_SOURCE=$(printf '%s' "$CONTENT" | grep -ciE 'create[[:space:]]+source' || true)
HAS_PARQUET_ZST_MATCH=$(printf '%s' "$CONTENT" | grep -ciE "match_pattern[[:space:]]*=[[:space:]]*['\"][^'\"]*\.parquet\.zst['\"]" || true)

if [ "${HAS_CREATE_SOURCE:-0}" -gt 0 ] && [ "${HAS_PARQUET_ZST_MATCH:-0}" -gt 0 ]; then
    SAMPLE_LINE=$(printf '%s' "$CONTENT" | grep -inE "match_pattern[[:space:]]*=[[:space:]]*['\"][^'\"]*\.parquet\.zst['\"]" | head -1)
    emit_remediation \
        "L40 guard: file contains a RisingWave CREATE SOURCE whose match_pattern targets *.parquet.zst keys. Per DATA-FACTORY-LESSONS-LEARNED.md L40, RW's PARQUET encoder cannot read whole-file zstd-wrapped Parquet — the source will catalog successfully but every read will fail with 'invalid seek to a negative position'. Empirically dead-cataloged today: source_fmcsa_authhist, source_fmcsa_company_census, source_fmcsa_authhist_all_with_history. Detected near: ${SAMPLE_LINE}" \
        "Use a derivation pipeline (DuckDB-on-R2) to read the .parquet.zst file and write a plain .parquet sibling, then point CREATE SOURCE at the derived prefix. Reference: build_fmcsa_carrier_essentials.py + apply_fmcsa_pdl_match_rw.py for the canonical pattern. If this is intentionally dead-cataloged for documentation purposes, add the marker '-- L40-OK-VERIFIED' to the file." \
        "${SCRIPT_DIR}/hook-pretooluse-l40-rw-source-zst-match.sh + DATA-FACTORY-LESSONS-LEARNED.md L40"
    exit 2
fi

exit 0
