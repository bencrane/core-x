#!/usr/bin/env bash
# Generic hook logger. Used by events whose only job is to record what happened.
# Usage in settings.json:
#   "command": "/Users/benjamincrane/hq-all/scripts/hook-log-event.sh <EVENT> <OUTPUT_JSONL>"
#
# Reads hook input JSON on stdin, prepends ts + event name, appends to OUTPUT_JSONL.

set -uo pipefail
EVENT_NAME=${1:-unknown}
LOG=${2:-/tmp/hook-log.jsonl}
mkdir -p "$(dirname "$LOG")"

INPUT=$(cat || true)
TS=$(date -u +%FT%TZ)

HQ_TS="$TS" HQ_EVENT="$EVENT_NAME" echo "$INPUT" | HQ_TS="$TS" HQ_EVENT="$EVENT_NAME" python3 -c "
import json, sys, os
ts = os.environ.get('HQ_TS', '')
event = os.environ.get('HQ_EVENT', 'unknown')
try:
    raw = sys.stdin.read()
    d = json.loads(raw) if raw.strip() else {}
except Exception as exc:
    d = {'_parse_error': str(exc)}
out = {'ts': ts, 'event': event}
# Don't blat values that may contain secrets; only carry useful keys
SAFE_KEYS = {
    'session_id', 'transcript_path', 'cwd', 'reason', 'stop_reason', 'trigger',
    'tool_name', 'subagent_type', 'description', 'file_path', 'source',
    'worktree', 'worktree_path', 'branch', 'task', 'status', 'priority',
    'error', 'error_message', 'error_type', 'returncode',
}
for k, v in d.items():
    if k in SAFE_KEYS:
        out[k] = v
print(json.dumps(out, default=str))
" >> "$LOG" 2>/dev/null || true

# Always exit 0 — these loggers never block
exit 0
