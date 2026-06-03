#!/usr/bin/env bash
# PreToolUse hook — L42 R2 ContentEncoding=zstd guard.
#
# Blocks Write|Edit|MultiEdit calls that introduce an R2/S3 PutObject (or
# upload_fileobj/upload_file) call with `ContentEncoding='zstd'` against a
# `.parquet*` key.
#
# Per DATA-FACTORY-LESSONS-LEARNED.md L42 (SAM.gov ingest 2026-05-09): when
# `Content-Encoding: zstd` is set on an R2 object, RW's S3 reader (or its
# underlying client) tries to decompress the response body BEFORE feeding
# bytes to the Parquet parser, which corrupts the read with the same
# 'invalid seek to a negative position' as L40. The fix is to NEVER set
# this header — store as plain `.parquet` and let RW read natively.
#
# Latent bug currently in production at apps/data-engine-x/modal/landing/r2.py
# (R2Landing.write_streaming_to_key + R2Landing.write_batch). Documented
# "follow-up out of scope" since 2026-05-09. This hook prevents NEW writers
# from re-creating the same problem until that bug is fixed.
#
# Detection: projected post-write content contains:
#   `ContentEncoding\s*=\s*['\"]zstd['\"]` AND `\.parquet` (any parquet ref).
#
# Override: `# L42-OK-VERIFIED` literal marker in the file (e.g. for a
# fixture or test that intentionally exercises the bad path).

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

# Match .py files anywhere under apps/data-engine-x/ (modal/, scripts/, etc.)
case "$FILE_PATH" in
    */apps/data-engine-x/*.py) ;;
    */apps/data-engine-x/*/*.py) ;;
    */apps/data-engine-x/*/*/*.py) ;;
    */apps/data-engine-x/*/*/*/*.py) ;;
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
if printf '%s' "$CONTENT" | grep -qF -- '# L42-OK-VERIFIED'; then
    exit 0
fi

# Detect: ContentEncoding=zstd AND any .parquet reference in the same file.
HAS_CE_ZSTD=$(printf '%s' "$CONTENT" | grep -ciE "ContentEncoding[[:space:]]*=[[:space:]]*['\"]zstd['\"]" || true)
HAS_PARQUET_REF=$(printf '%s' "$CONTENT" | grep -ciE "\.parquet" || true)

if [ "${HAS_CE_ZSTD:-0}" -gt 0 ] && [ "${HAS_PARQUET_REF:-0}" -gt 0 ]; then
    SAMPLE_LINE=$(printf '%s' "$CONTENT" | grep -inE "ContentEncoding[[:space:]]*=[[:space:]]*['\"]zstd['\"]" | head -1)
    emit_remediation \
        "L42 guard: file sets ContentEncoding='zstd' on what appears to be a Parquet upload to R2/S3. Per DATA-FACTORY-LESSONS-LEARNED.md L42, this header makes RW's S3 reader try to decompress the response body before feeding bytes to the Parquet parser — which fails with 'invalid seek to a negative position' (same symptom as L40). The latent bug already exists in apps/data-engine-x/modal/landing/r2.py and is the reason the canonical bulk_ingest writer produces files RW cannot read. Detected near: ${SAMPLE_LINE}" \
        "DO NOT set ContentEncoding='zstd' on Parquet uploads. Use plain .parquet keys with internal column-chunk ZSTD (DuckDB COPY ... (FORMAT PARQUET, COMPRESSION ZSTD) emits this natively; pyarrow.parquet.write_table(..., compression='zstd') likewise). RW reads these without any header. If this is a fixture/test that intentionally exercises the bad path, add the marker '# L42-OK-VERIFIED' to the file." \
        "${SCRIPT_DIR}/hook-pretooluse-l42-r2-content-encoding-zstd.sh + DATA-FACTORY-LESSONS-LEARNED.md L42"
    exit 2
fi

exit 0
