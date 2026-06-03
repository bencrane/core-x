#!/usr/bin/env bash
# SessionEnd hook — fires when the session truly exits.
#
# Two responsibilities:
#   1. Move sessions/active/{session_id}.md → sessions/{ts}-{branch}-{short}.md
#      (archives the in-progress checkpoint as the final session record)
#   2. Append a JSONL line to sessions/_session-ends.jsonl

set -uo pipefail

# Recursion guard: when this hook spawns the narrative summarizer, the
# summarizer itself is a `claude -p` invocation that — at its own SessionEnd —
# would re-fire this hook. Bail out fast in that case so we don't snowball
# into infinite summarizer dispatches. Set by spawn-session-summarizer.sh.
if [ -n "${HQ_SUMMARIZER_SUBAGENT:-}" ]; then
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VAULT="$HOME/Desktop/hq"
ACTIVE_DIR="$VAULT/sessions/active"
SESSIONS_DIR="$VAULT/sessions"
LOG="$SESSIONS_DIR/_session-ends.jsonl"
mkdir -p "$ACTIVE_DIR" "$SESSIONS_DIR"

INPUT=$(cat || true)
SESSION_ID=$(echo "$INPUT" | python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read() or '{}')
    print(d.get('session_id', '') or '')
except Exception:
    print('')
" 2>/dev/null || echo "")

# Also pull transcript_path now — we need it for the post-archive transcript
# copy + summarizer dispatch below. Same accessor as PreCompact uses.
TRANSCRIPT_PATH=$(echo "$INPUT" | python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read() or '{}')
    print(d.get('transcript_path', '') or '')
except Exception:
    print('')
" 2>/dev/null || echo "")

TS=$(date -u +%FT%H-%M-%SZ)

# Determine branch for filename: prefer cwd's git branch
CWD_BRANCH=""
if git -C "$PWD" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    CWD_BRANCH=$(git -C "$PWD" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
fi
[ -z "$CWD_BRANCH" ] && CWD_BRANCH="no-branch"
SAFE_BRANCH=${CWD_BRANCH//\//-}
SHORT_ID=${SESSION_ID:0:8}
[ -z "$SHORT_ID" ] && SHORT_ID="unknown"

# Move active → archive (if there's a checkpoint to archive)
ACTIVE_FILE="$ACTIVE_DIR/${SESSION_ID}.md"
ARCHIVE_FILE="$SESSIONS_DIR/${TS}-${SAFE_BRANCH}-${SHORT_ID}.md"
if [ -n "$SESSION_ID" ] && [ -f "$ACTIVE_FILE" ]; then
    mv "$ACTIVE_FILE" "$ARCHIVE_FILE"
    echo "📁 Archived session: $ARCHIVE_FILE" >&2
fi

# Append JSONL line for session-ends ledger
echo "$INPUT" | HQ_TS="$TS" HQ_ARCHIVE="$ARCHIVE_FILE" python3 -c "
import json, sys, os
out = {
    'ts': os.environ.get('HQ_TS', ''),
    'event': 'SessionEnd',
}
try:
    raw = sys.stdin.read()
    d = json.loads(raw) if raw.strip() else {}
except Exception:
    d = {}
for k in ('session_id', 'transcript_path', 'reason'):
    if k in d:
        out[k] = d[k]
out['cwd'] = os.getcwd()
archive = os.environ.get('HQ_ARCHIVE', '')
if archive and os.path.exists(archive):
    out['archive_file'] = archive
print(json.dumps(out))
" >> "$LOG" 2>/dev/null || true

# Transcript copy + handoff-digest summarizer.
# Pattern mirrors hook-precompact.sh. Both backgrounded so SessionEnd never
# blocks (must complete in <1s regardless of summarizer status).
TRANSCRIPT_DEST=""
if [ -n "$TRANSCRIPT_PATH" ] && [ -f "$TRANSCRIPT_PATH" ]; then
    TRANSCRIPT_ARCHIVE_DIR="$VAULT/raw/transcripts"
    mkdir -p "$TRANSCRIPT_ARCHIVE_DIR"
    TRANSCRIPT_DEST="$TRANSCRIPT_ARCHIVE_DIR/${TS}-${SESSION_ID:-unknown}.jsonl"
    # Synchronous cp — small file, microseconds. Doing this synchronously
    # avoids a race where the summarizer fires before the destination exists.
    # The contract "SessionEnd <1s" still holds; cp of a few-MB JSONL is
    # well under 100ms.
    cp "$TRANSCRIPT_PATH" "$TRANSCRIPT_DEST" 2>/dev/null || TRANSCRIPT_DEST=""
    [ -n "$TRANSCRIPT_DEST" ] && echo "📜 SessionEnd transcript archived: $TRANSCRIPT_DEST" >&2
fi

# Spawn the narrative summarizer. The helper itself backgrounds the actual
# `claude -p --bare` invocation, so this call returns immediately (<100ms).
# Relocated to core-x/harness/: the summarizer is a sibling here (was the dead
# $VAULT/scripts/ path that never existed — this repoint repairs SessionEnd digests).
SUMMARIZER="$SCRIPT_DIR/spawn-session-summarizer.sh"
if [ -x "$SUMMARIZER" ] && [ -n "$TRANSCRIPT_DEST" ]; then
    "$SUMMARIZER" "$TRANSCRIPT_DEST" "${SESSION_ID:-unknown}" "$SAFE_BRANCH" "$ARCHIVE_FILE" "$TS" >/dev/null 2>&1 || true
fi

# qmd retrieval-layer freshness — embed (not just update) so new content from
# the just-ended session is actually searchable in the next session.
# `qmd update` only refreshes file metadata; `qmd embed` is incremental
# (~210ms no-op if nothing new, ~3s if there are new chunks to vectorize).
# Fire-and-forget so SessionEnd never blocks.
if command -v qmd >/dev/null 2>&1; then
    ( qmd embed >/dev/null 2>&1 & )
fi

exit 0
