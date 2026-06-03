#!/usr/bin/env bash
# PostToolUse hook for Bash — side-effects fired after Bash tool calls.
#
# Receives {tool_name, tool_input, tool_response, session_id} on stdin.
# Returns 0 always — never blocks; only side-effects.
#
# Side effects:
#   1. Re-generate state digest after any snapshot-*.py invocation
#   2. Capture a git event when the agent runs git commit/merge/rebase/push/pull --rebase
#      (complements the .git/hooks/ client hooks; adds session_id + source=agent metadata)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/_lib-remediation.sh"

INPUT=$(cat)
PARSED=$(echo "$INPUT" | /opt/homebrew/bin/python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read())
    cmd = d.get('tool_input', {}).get('command', '') or ''
    sid = d.get('session_id', '') or ''
    print(cmd.replace(chr(10), ' '))
    print(sid)
except Exception:
    print('')
    print('')
" 2>/dev/null || echo "")
COMMAND=$(echo "$PARSED" | sed -n '1p')
SESSION_ID=$(echo "$PARSED" | sed -n '2p')

if [ -z "$COMMAND" ]; then
    exit 0
fi

# 1. Re-generate state digest after any snapshot-*.py runs
if [ -x "$SCRIPT_DIR/digest-snapshots.py" ] && echo "$COMMAND" | grep -qE 'snapshot-(all|db|deps|doppler|git|git-event|migrations|openapi)\.py'; then
    if ! /opt/homebrew/bin/python3 "$SCRIPT_DIR/digest-snapshots.py" >/dev/null 2>/tmp/hq-digest.err; then
        emit_remediation \
            "digest-snapshots.py failed after a snapshot invocation. Error: $(cat /tmp/hq-digest.err 2>/dev/null || echo '(no output)')" \
            "Fix the syntax or runtime issue in digest-snapshots.py or the snapshot files it reads, then re-run the snapshot command." \
            "$SCRIPT_DIR/digest-snapshots.py"
    fi
fi

# 2. Git event capture for agent-initiated git commands.
#    Match commands that change history; skip status/log/diff/fetch.
GIT_ACTION=""
if   echo "$COMMAND" | grep -qE '(^|[[:space:]]|;|&&|\|\|)git[[:space:]]+commit\b'; then GIT_ACTION="commit"
elif echo "$COMMAND" | grep -qE '(^|[[:space:]]|;|&&|\|\|)git[[:space:]]+merge\b';  then GIT_ACTION="merge"
elif echo "$COMMAND" | grep -qE '(^|[[:space:]]|;|&&|\|\|)git[[:space:]]+rebase\b'; then GIT_ACTION="rebase"
elif echo "$COMMAND" | grep -qE '(^|[[:space:]]|;|&&|\|\|)git[[:space:]]+push\b';   then GIT_ACTION="push"
elif echo "$COMMAND" | grep -qE '(^|[[:space:]]|;|&&|\|\|)git[[:space:]]+pull[[:space:]].*--rebase\b'; then GIT_ACTION="rebase"
fi

if [ -n "$GIT_ACTION" ]; then
    PROJECT=$(/opt/homebrew/bin/python3 -c '
import os, subprocess
from pathlib import Path
_HQ_ALL = Path.home() / "Desktop" / "hq-all"
PROJECTS = {
    # Monorepo (single .git/) — CWD-based per-app detection longest-prefix-wins.
    _HQ_ALL / "apps" / "data-engine-x":    "data-engine-x",
    _HQ_ALL / "apps" / "hq-x":             "hq-x",
    _HQ_ALL / "apps" / "hq-command":       "hq-command",
    _HQ_ALL / "apps" / "managed-agents-x": "managed-agents-x",
    _HQ_ALL:                               "hq-all",
    # Legacy per-repo paths (still snapshot-capable while old repos exist).
    Path.home() / "data-engine-x":     "data-engine-x",
    Path.home() / "hq-x":              "hq-x",
    Path.home() / "hq-command":        "hq-command",
    Path.home() / "managed-agents-x":  "managed-agents-x",
}
try:
    cwd_resolved = Path(os.getcwd()).resolve()
    # Longest path first so apps/<n>/ wins over hq-all.
    for p, name in sorted(PROJECTS.items(), key=lambda kv: -len(str(kv[0]))):
        try:
            p_resolved = p.resolve()
            if cwd_resolved == p_resolved or str(cwd_resolved).startswith(str(p_resolved) + os.sep):
                print(name); break
        except Exception:
            continue
except Exception:
    pass
' 2>/dev/null)

    if [ -n "$PROJECT" ] && [ -x "$SCRIPT_DIR/snapshot-git-event.py" ]; then
        ( /opt/homebrew/bin/python3 "$SCRIPT_DIR/snapshot-git-event.py" \
            --project "$PROJECT" --action "$GIT_ACTION" --source agent \
            --session-id "$SESSION_ID" \
            >/dev/null 2>&1 & ) || true
    fi
fi

# 3. DB state-mutation snapshot trigger.
#    When the agent runs a command that mutates DB state (psql -f / -c, supabase
#    db push/reset, supabase migration up, alembic upgrade), kick a fast
#    snapshot-db + snapshot-migrations for the project containing CWD.
#    Backgrounded; never blocks; no-op if not in a tracked repo.
DB_MUTATION=""
if   echo "$COMMAND" | grep -qE '(^|[[:space:]]|;|&&|\|\|)psql[[:space:]].*(-f|-c|<)'; then DB_MUTATION="psql"
elif echo "$COMMAND" | grep -qE '(^|[[:space:]]|;|&&|\|\|)supabase[[:space:]]+db[[:space:]]+(push|reset|diff)'; then DB_MUTATION="supabase-db"
elif echo "$COMMAND" | grep -qE '(^|[[:space:]]|;|&&|\|\|)supabase[[:space:]]+migration[[:space:]]+up'; then DB_MUTATION="supabase-migration"
elif echo "$COMMAND" | grep -qE '(^|[[:space:]]|;|&&|\|\|)alembic[[:space:]]+(upgrade|downgrade|stamp)'; then DB_MUTATION="alembic"
fi

if [ -n "$DB_MUTATION" ]; then
    PROJ_INFO=$(/opt/homebrew/bin/python3 -c '
import os, subprocess
from pathlib import Path
_HQ_ALL = Path.home() / "Desktop" / "hq-all"
PROJECTS = {
    # Monorepo apps/<name>/ — CWD-based detection.
    _HQ_ALL / "apps" / "data-engine-x":    ("data-engine-x",     "DEX_DB_URL_POOLED",  "supabase/migrations"),
    _HQ_ALL / "apps" / "hq-x":             ("hq-x",              "HQX_DB_URL_POOLED",  "migrations"),
    _HQ_ALL / "apps" / "managed-agents-x": ("managed-agents-x",  "MAGS_DB_URL_POOLED", ""),
    # Legacy per-repo paths.
    Path.home() / "data-engine-x":     ("data-engine-x",     "DEX_DB_URL_POOLED",  "supabase/migrations"),
    Path.home() / "hq-x":              ("hq-x",              "HQX_DB_URL_POOLED",  "migrations"),
    Path.home() / "managed-agents-x":  ("managed-agents-x",  "MAGS_DB_URL_POOLED", ""),
}
try:
    top = subprocess.run(
        ["git", "-C", os.getcwd(), "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=False, timeout=5,
    ).stdout.strip()
    if top:
        top_resolved = str(Path(top).resolve())
        for p, info in PROJECTS.items():
            try:
                p_resolved = str(p.resolve())
                if top_resolved == p_resolved or top_resolved.startswith(p_resolved + "/"):
                    print("\t".join([info[0], info[1], info[2]])); break
            except Exception:
                continue
except Exception:
    pass
' 2>/dev/null)

    if [ -n "$PROJ_INFO" ] && [ -x "$SCRIPT_DIR/snapshot-db.py" ]; then
        PROJECT=$(echo "$PROJ_INFO" | cut -f1)
        DB_ENV=$(echo "$PROJ_INFO" | cut -f2)
        MIG_DIR=$(echo "$PROJ_INFO" | cut -f3)
        VAULT="$HOME/Desktop/hq"
        TODAY=$(date -u +%Y-%m-%d)
        OUT="$VAULT/raw/snapshots/$TODAY/$PROJECT"
        # Background fast db + migrations snapshot. Doppler-injected for the DB URL.
        (
            cd "$HOME/$PROJECT" 2>/dev/null && \
            doppler run -- /opt/homebrew/bin/python3 "$SCRIPT_DIR/snapshot-db.py" \
                --project "$PROJECT" --db-url-env "$DB_ENV" --out-dir "$OUT" \
                --sample-limit 0 >/dev/null 2>&1
            MIG_FLAG=""
            [ -n "$MIG_DIR" ] && MIG_FLAG="--migrations-dir $MIG_DIR"
            cd "$HOME/$PROJECT" 2>/dev/null && \
            doppler run -- /opt/homebrew/bin/python3 "$SCRIPT_DIR/snapshot-migrations.py" \
                --project "$PROJECT" --db-url-env "$DB_ENV" --out-dir "$OUT" \
                $MIG_FLAG >/dev/null 2>&1
        ) &
    fi
fi

exit 0
