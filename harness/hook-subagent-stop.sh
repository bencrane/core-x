#!/usr/bin/env bash
# SubagentStop hook — log when a subagent (Agent tool task) completes.
# Useful for tracking parallel work and debugging "what did that agent do?"
#
# Output: ~/Desktop/hq/sessions/_subagents.jsonl

set -uo pipefail
VAULT="$HOME/Desktop/hq"
LOG="$VAULT/sessions/_subagents.jsonl"
mkdir -p "$(dirname "$LOG")"

INPUT=$(cat || true)
TS=$(date -u +%FT%TZ)

echo "$INPUT" | python3 -c "
import json, sys
ts = '$TS'
try:
    d = json.loads(sys.stdin.read() or '{}')
except Exception:
    d = {}
out = {
    'ts': ts,
    'session_id': d.get('session_id'),
    'subagent_type': d.get('subagent_type') or d.get('type'),
    'description': d.get('description'),
    'reason': d.get('stop_reason') or d.get('reason'),
}
print(json.dumps(out))
" >> "$LOG"
