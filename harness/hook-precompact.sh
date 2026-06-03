#!/usr/bin/env bash
# PreCompact hook — copy the transcript before context compaction strips
# the fine detail. Compaction can't be undone; archive first.
#
# Output: ~/Desktop/hq/raw/transcripts/{ts}-{session_id}.jsonl

set -uo pipefail
VAULT="$HOME/Desktop/hq"
ARCHIVE="$VAULT/raw/transcripts"
mkdir -p "$ARCHIVE"

INPUT=$(cat || true)

PARSED=$(echo "$INPUT" | python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read() or '{}')
except Exception:
    d = {}
print(d.get('transcript_path', '') + '\t' + (d.get('session_id') or 'unknown')[:12])
" 2>/dev/null || echo "")

TRANSCRIPT_PATH=$(echo "$PARSED" | cut -f1)
SESSION_ID=$(echo "$PARSED" | cut -f2)
TS=$(date -u +%FT%H-%M-%SZ)

if [ -n "$TRANSCRIPT_PATH" ] && [ -f "$TRANSCRIPT_PATH" ]; then
    DEST="$ARCHIVE/${TS}-${SESSION_ID}.jsonl"
    cp "$TRANSCRIPT_PATH" "$DEST"
    echo "📦 Pre-compaction transcript archived: $DEST" >&2
fi

exit 0
