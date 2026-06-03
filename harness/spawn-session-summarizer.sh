#!/usr/bin/env bash
# spawn-session-summarizer.sh
#
# Fires a Claude Code subagent (`claude -p --bare`) that reads a session
# transcript and writes a curated handoff digest to
# ~/Desktop/hq/sessions/{ts}-handoff-digest.md.
#
# Usage:
#   spawn-session-summarizer.sh <transcript_path> <session_id> <branch> <archived_record> [ts]
#
# Designed to be invoked from the SessionEnd hook with the process backgrounded
# (`spawn-session-summarizer.sh ... &`) so SessionEnd never blocks.
#
# Why `claude -p --bare`?
#   - --bare skips hooks (no SessionEnd recursion), CLAUDE.md auto-discovery,
#     plugin sync, keychain reads, attribution. Sets CLAUDE_CODE_SIMPLE=1.
#   - -p prints and exits (non-interactive).
#   - Reuses existing Claude Code auth (Anthropic OAuth via keychain or
#     ANTHROPIC_API_KEY); no new secret plumbing required.
#
# Logs to ~/Desktop/hq/raw/session-summarizer.log.

set -uo pipefail

# Resolve sibling files via SCRIPT_DIR self-location so relocations don't
# require edits inside the script (per ADR 0001 §"How the helpers find things").
# This script lives at ~/hq-all/scripts/ as of the vault → hq-all
# relocation; the prompt file is its sibling under prompts/.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

VAULT="$HOME/Desktop/hq"
LOG="$VAULT/raw/session-summarizer.log"
PROMPT_FILE="$SCRIPT_DIR/prompts/session-summarizer.md"
SESSIONS_DIR="$VAULT/sessions"

mkdir -p "$VAULT/raw" "$SESSIONS_DIR"

TRANSCRIPT_PATH="${1:-}"
SESSION_ID="${2:-unknown}"
BRANCH="${3:-no-branch}"
ARCHIVED_RECORD="${4:-}"
TS="${5:-$(date -u +%FT%H-%M-%SZ)}"

log() {
    # JSON line — easy to grep/filter later.
    local status="$1"; shift
    local msg="${1:-}"
    python3 -c "
import json, sys
print(json.dumps({
    'ts': '$(date -u +%FT%H-%M-%SZ)',
    'event': 'session-summarizer',
    'status': '$status',
    'session_id': '$SESSION_ID',
    'transcript_path': '$TRANSCRIPT_PATH',
    'output_path': '$OUTPUT_PATH',
    'msg': '''$msg''',
}))
" >> "$LOG" 2>/dev/null || echo "{\"status\":\"$status\",\"session_id\":\"$SESSION_ID\"}" >> "$LOG"
}

OUTPUT_PATH="$SESSIONS_DIR/${TS}-handoff-digest.md"

# Guard: no transcript → nothing to summarize.
if [ -z "$TRANSCRIPT_PATH" ] || [ ! -f "$TRANSCRIPT_PATH" ]; then
    log "skipped" "no transcript at $TRANSCRIPT_PATH"
    exit 0
fi

# Guard: prompt missing.
if [ ! -f "$PROMPT_FILE" ]; then
    log "failed" "prompt file missing: $PROMPT_FILE"
    exit 0
fi

# Guard: claude binary missing.
if ! command -v claude >/dev/null 2>&1; then
    log "failed" "claude binary not found in PATH"
    exit 0
fi

# Guard: digest already exists for this ts (idempotency / re-runs).
if [ -f "$OUTPUT_PATH" ]; then
    log "skipped" "digest already exists at $OUTPUT_PATH"
    exit 0
fi

# Guard: don't summarize the summarizer's own future sessions or transcripts
# that look pathologically small (test fixtures notwithstanding — those go
# through this same path in the verification step).
TRANSCRIPT_BYTES=$(wc -c < "$TRANSCRIPT_PATH" 2>/dev/null | tr -d ' ' || echo 0)
if [ "${TRANSCRIPT_BYTES:-0}" -lt 50 ]; then
    log "skipped" "transcript too small ($TRANSCRIPT_BYTES bytes)"
    exit 0
fi

log "started" "spawning claude -p --bare summarizer"

# Build the prompt: prepend metadata, then include the system prompt body.
# The summarizer reads TRANSCRIPT_PATH itself via the Read tool — we don't
# inline the transcript (it could be large).
PROMPT=$(cat <<EOF
TRANSCRIPT_PATH: $TRANSCRIPT_PATH
OUTPUT_PATH: $OUTPUT_PATH
SESSION_ID: $SESSION_ID
BRANCH: $BRANCH
ARCHIVED_RECORD: $ARCHIVED_RECORD
SESSION_END_TS: $TS

---

$(cat "$PROMPT_FILE")
EOF
)

# Run in a subshell with a generous timeout. Allow only the tools the
# summarizer actually needs: Read (transcript + reference files) and Write
# (the digest). No Bash, no network, no editing.
#
# --add-dir grants tool access to the vault and the transcripts dir so Read
# can reach the transcript regardless of cwd.
#
# Timeout 600s — generous; large transcripts may take a few minutes.
TIMEOUT_SECS=600

(
    # Detach fully from parent stdio so SessionEnd's wait/exit isn't held up.
    cd "$VAULT" || exit 0

    # gtimeout (coreutils) preferred; fall back to perl if not present.
    if command -v gtimeout >/dev/null 2>&1; then
        TIMEOUT="gtimeout $TIMEOUT_SECS"
    elif command -v timeout >/dev/null 2>&1; then
        TIMEOUT="timeout $TIMEOUT_SECS"
    else
        TIMEOUT=""
    fi

    # NOTE: NOT using --bare. --bare disables keychain reads, which means
    # OAuth login isn't seen and the subagent fails with "Not logged in".
    # Without --bare, hooks fire — including SessionEnd. To prevent recursion,
    # we export HQ_SUMMARIZER_SUBAGENT=1 here; the SessionEnd hook checks for
    # this and bails out early when set.
    export HQ_SUMMARIZER_SUBAGENT=1

    OUTPUT=$(
        $TIMEOUT claude \
            -p \
            --model haiku \
            --permission-mode bypassPermissions \
            --allowedTools "Read Write" \
            --add-dir "$VAULT" \
            --add-dir "$(dirname "$TRANSCRIPT_PATH")" \
            --max-budget-usd 2 \
            --no-session-persistence \
            --disable-slash-commands \
            "$PROMPT" 2>&1
    )
    EXIT_CODE=$?

    if [ "$EXIT_CODE" -eq 0 ] && [ -f "$OUTPUT_PATH" ]; then
        BYTES=$(wc -c < "$OUTPUT_PATH" 2>/dev/null | tr -d ' ' || echo 0)
        log "succeeded" "digest=$BYTES bytes exit=$EXIT_CODE"
    else
        # Truncate output to first 500 chars for the log (avoid huge JSON lines).
        SAFE_OUTPUT=$(echo "$OUTPUT" | head -c 500 | tr '\n' ' ' | sed "s/'/'\"'\"'/g")
        # Re-log via a fresh python call since we're in a subshell.
        python3 -c "
import json
print(json.dumps({
    'ts': '$(date -u +%FT%H-%M-%SZ)',
    'event': 'session-summarizer',
    'status': 'failed',
    'session_id': '$SESSION_ID',
    'transcript_path': '$TRANSCRIPT_PATH',
    'output_path': '$OUTPUT_PATH',
    'exit_code': $EXIT_CODE,
    'output_head': '''$SAFE_OUTPUT''',
}))
" >> "$LOG" 2>/dev/null || echo "{\"status\":\"failed\",\"exit_code\":$EXIT_CODE}" >> "$LOG"
    fi
) </dev/null >/dev/null 2>&1 &

# Disown so the child outlives this script's shell.
disown 2>/dev/null || true

exit 0
