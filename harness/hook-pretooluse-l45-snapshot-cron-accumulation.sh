#!/usr/bin/env bash
# PreToolUse hook — L45 snapshot-cron accumulation guard.
#
# Blocks Write|Edit|MultiEdit calls that introduce a Modal app combining a
# cron schedule with a subprocess invocation of a `build_*.py` script that
# writes daily snapshot-keyed Parquet to R2.
#
# Per DATA-FACTORY-LESSONS-LEARNED.md L45 + PR #324/#325 incident
# 2026-05-10: RW's connector='s3' (S3_V2) treats each new file at a new
# match_pattern path as fresh streaming input. Adding a daily cron that
# writes `snapshot=YYYY-MM-DD/data.parquet` causes the downstream MV's
# row count to grow by the new file's row count every day. PR #324
# would have doubled mv_fmcsa_carrier_essentials on its first scheduled
# fire (4.4M → 8.87M); reverted in PR #325 within hours.
#
# Detection: file is under apps/data-engine-x/modal/ AND projected
# post-write content contains BOTH:
#   (1) `modal\.Cron\(` or `schedule\s*=\s*modal\.Cron`
#   (2) `subprocess\.run\(.*build_.*\.py.*--apply` (or build_*.py invocation)
#
# Override: `# L45-OK-VERIFIED` literal marker in the file. Use only after
# confirming the build script writes to a fixed key (not snapshot=YYYY-MM-DD)
# OR the downstream MV uses a snapshot-aware filter outside the streaming
# admit path (sidecar config table, weekly static-date refresh per L37).
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

# File-path filter: only Modal app files in data-engine-x.
case "$FILE_PATH" in
    */apps/data-engine-x/modal/*.py) ;;
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
if printf '%s' "$CONTENT" | grep -qF -- '# L45-OK-VERIFIED'; then
    exit 0
fi

# Detect: Modal cron schedule AND subprocess call to a build_*.py script.
HAS_CRON=$(printf '%s' "$CONTENT" | grep -cE 'modal\.Cron\(' || true)
HAS_BUILD_SUBPROC=$(printf '%s' "$CONTENT" | grep -ciE 'subprocess\.run.*build_[a-z0-9_]+\.py' || true)

if [ "${HAS_CRON:-0}" -gt 0 ] && [ "${HAS_BUILD_SUBPROC:-0}" -gt 0 ]; then
    CRON_LINE=$(printf '%s' "$CONTENT" | grep -nE 'modal\.Cron\(' | head -1)
    BUILD_LINE=$(printf '%s' "$CONTENT" | grep -inE 'build_[a-z0-9_]+\.py' | head -1)
    emit_remediation \
        "L45 guard: this Modal app combines a cron schedule with a subprocess call to a build_*.py script. Per DATA-FACTORY-LESSONS-LEARNED.md L45 + PR #324/#325 incident, RW's S3_V2 connector treats each new file at a new match_pattern path as fresh streaming input. If this build script writes daily snapshot=YYYY-MM-DD/data.parquet keys against an existing RW source, the downstream MV's row count grows by the new file's row count every day (PR #324 would have doubled mv_fmcsa_carrier_essentials from 4.4M → 8.87M on first fire; was reverted in PR #325 within hours). Detected — cron at ${CRON_LINE}; build invocation near ${BUILD_LINE}" \
        "Before deploying: confirm the build script writes to a FIXED key (not snapshot=YYYY-MM-DD) so RW's same-key dedup applies, OR confirm the downstream MV has a snapshot-aware filter pattern outside the streaming admit path (sidecar config table, weekly static-date refresh per L37). If you have verified one of these, add the marker '# L45-OK-VERIFIED' to the file. Codebase convention per L12 amendment + PR #325: derived essentials projections are operator-initiated --apply runs, NOT auto-cron." \
        "${SCRIPT_DIR}/hook-pretooluse-l45-snapshot-cron-accumulation.sh + DATA-FACTORY-LESSONS-LEARNED.md L45"
    exit 2
fi

exit 0
