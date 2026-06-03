#!/usr/bin/env bash
# Stop hook — fires when Claude finishes responding (per-turn, not per-session).
#
# Writes a per-SESSION (not per-turn) file at sessions/active/{session_id}.md.
# Overwrites each turn so a long conversation produces ONE file, not N.
#
# When the session truly ends, hook-sessionend.sh moves this file to
# sessions/{ts}-{branch}-{short_id}.md.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VAULT="$HOME/Desktop/hq"
ACTIVE_DIR="$VAULT/sessions/active"
mkdir -p "$ACTIVE_DIR"

INPUT=$(cat || true)
SESSION_ID=$(echo "$INPUT" | python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read() or '{}')
    print(d.get('session_id', '') or '')
except Exception:
    print('')
" 2>/dev/null || echo "")

# Fall back to a timestamped name if no session_id (shouldn't happen but safe)
[ -z "$SESSION_ID" ] && SESSION_ID="unknown-$(date -u +%Y%m%dT%H%M%SZ)"

TS=$(date -u +%FT%TZ)

CWD_BRANCH=""
if git -C "$PWD" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    CWD_BRANCH=$(git -C "$PWD" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
fi
[ -z "$CWD_BRANCH" ] && CWD_BRANCH="no-branch"

OUT="$ACTIVE_DIR/${SESSION_ID}.md"

{
    echo "# Active session ${SESSION_ID}"
    echo
    echo "- **Session ID**: \`${SESSION_ID}\`"
    echo "- **Last update (UTC)**: ${TS}"
    echo "- **CWD**: \`$PWD\`"
    echo "- **Branch (cwd)**: \`$CWD_BRANCH\`"
    echo
    echo "_This file is overwritten on every Stop event. SessionEnd will move it to ~/Desktop/hq/sessions/ when the session truly exits._"
    echo
    echo "## Repo state at last checkpoint"
    echo
    CWD_REPO=$(git -C "$PWD" rev-parse --show-toplevel 2>/dev/null || echo "")
    for repo in ${CWD_REPO:+"$CWD_REPO"}; do
        name=$(basename "$repo")
        if [ -d "$repo/.git" ] || [ -f "$repo/.git" ]; then
            head=$(git -C "$repo" rev-parse --short HEAD 2>/dev/null || echo "?")
            branch=$(git -C "$repo" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "?")
            modified_count=$(git -C "$repo" status --porcelain 2>/dev/null | wc -l | tr -d ' ')
            echo "### $name"
            echo
            echo "- branch: \`$branch\` · head: \`$head\` · modified: $modified_count"
            if [ "$modified_count" -gt 0 ]; then
                echo "- changes:"
                git -C "$repo" status --porcelain 2>/dev/null | sed 's/^/  - `/' | sed 's/$/`/'
            fi
            recent=$(git -C "$repo" log --oneline -5 --since="6 hours ago" 2>/dev/null || true)
            if [ -n "$recent" ]; then
                echo "- commits in last 6h:"
                echo "$recent" | sed 's/^/  - /'
            fi
            echo
        fi
    done
    echo "## Verified"
    echo
    echo "_Auto-generated. Amend before SessionEnd if you want this preserved with what was actually verified._"
    echo
    echo "## Open / Blockers"
    echo
    echo "_Auto-generated. Amend with anything still in flight or blocked._"
} > "$OUT"

# Mandatory cycle-report safety net: any /scope cycle whose scope-status
# heartbeat went stale without a cycle_report_path gets a hook-fallback
# placeholder report written to ~/Desktop/hq/reports/, then the stale status
# file is deleted. Backgrounded so this never blocks turn termination.
( "$SCRIPT_DIR/scope-cycle-report.sh" --gc-stale >/dev/null 2>&1 & ) || true

exit 0
