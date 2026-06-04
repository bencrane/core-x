#!/usr/bin/env bash
# scope-status-write.sh — write or update a scope-status JSON heartbeat file.
#
# USAGE
#   scope-status-write.sh <slug> <stage> <current-action> \
#     [--blocked-on <abs-path>] \
#     [--directive-path <abs-path>] \
#     [--pid <int>]
#
# JSON SCHEMA (all 8 keys required, in order):
#   {
#     "slug": "string",
#     "stage": "string (one of the enum)",
#     "stage_started_at": "ISO8601 UTC",
#     "last_heartbeat": "ISO8601 UTC",
#     "directive_path": "absolute path string OR null",
#     "current_action": "string",
#     "pid": "integer OR null",
#     "blocked_on": "absolute path to blocker file OR null"
#   }
#
# BEHAVIOR
#   - Preserves stage_started_at if stage is unchanged from the existing file.
#   - Resets stage_started_at to now if stage changes.
#   - Always rewrites last_heartbeat to now.
#
# STAGE ENUM
#   validator | executor | deploy-verifier | scope-decomposer | polling | idle | done
#
# OUTPUT
#   ~/Desktop/hq/scope-status/{slug}.json

set -euo pipefail

usage() {
  cat <<'USAGE'
scope-status-write.sh <slug> <stage> <current-action> [OPTIONS]

  <slug>             required
  <stage>            required; one of: validator|executor|deploy-verifier|scope-decomposer|polling|idle|done
  <current-action>   required; free-form description of what this session is doing right now

  --blocked-on <abs-path>    path to blocker file (or omit for null)
  --directive-path <abs-path>
  --pid <int>
USAGE
}

die() { echo "ERROR: $*" >&2; exit 1; }

# ── parse positional args ─────────────────────────────────────────────────────
[[ $# -ge 3 ]] || { usage >&2; die "slug, stage, current-action are required positional args"; }

SLUG="$1"
STAGE="$2"
CURRENT_ACTION="$3"
shift 3

# ── validate stage enum ───────────────────────────────────────────────────────
case "$STAGE" in
  validator|executor|deploy-verifier|scope-decomposer|polling|idle|done) ;;
  *) die "Invalid stage '$STAGE'; must be one of: validator|executor|deploy-verifier|scope-decomposer|polling|idle|done" ;;
esac

# ── parse optional flags ──────────────────────────────────────────────────────
BLOCKED_ON="null"
DIRECTIVE_PATH="null"
PID_VAL="null"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --blocked-on)      BLOCKED_ON="\"$2\"";     shift 2 ;;
    --directive-path)  DIRECTIVE_PATH="\"$2\""; shift 2 ;;
    --pid)             PID_VAL="$2";            shift 2 ;;
    --help|-h)         usage; exit 0 ;;
    *) die "Unknown flag: $1" ;;
  esac
done

# ── ensure output dir ─────────────────────────────────────────────────────────
mkdir -p "$HOME/Desktop/hq/scope-status"
STATUS_FILE="$HOME/Desktop/hq/scope-status/${SLUG}.json"
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# ── determine stage_started_at ────────────────────────────────────────────────
STAGE_STARTED_AT="$NOW"

if [[ -f "$STATUS_FILE" ]]; then
  # If stage is the same, preserve existing stage_started_at
  EXISTING_STAGE=$(python3 -c "
import json
try:
    d = json.load(open('$STATUS_FILE'))
    print(d.get('stage',''))
except:
    print('')
" 2>/dev/null || echo "")

  if [[ "$EXISTING_STAGE" == "$STAGE" ]]; then
    EXISTING_STARTED=$(python3 -c "
import json
try:
    d = json.load(open('$STATUS_FILE'))
    print(d.get('stage_started_at',''))
except:
    print('')
" 2>/dev/null || echo "")
    [[ -n "$EXISTING_STARTED" ]] && STAGE_STARTED_AT="$EXISTING_STARTED"
  fi
fi

# ── write JSON ────────────────────────────────────────────────────────────────
python3 - "$STATUS_FILE" "$SLUG" "$STAGE" "$STAGE_STARTED_AT" "$NOW" \
          "$DIRECTIVE_PATH" "$CURRENT_ACTION" "$PID_VAL" "$BLOCKED_ON" <<'PYEOF'
import json, sys

path = sys.argv[1]
slug = sys.argv[2]
stage = sys.argv[3]
stage_started_at = sys.argv[4]
last_heartbeat = sys.argv[5]
directive_path_raw = sys.argv[6]   # quoted string or "null"
current_action = sys.argv[7]
pid_raw = sys.argv[8]              # int string or "null"
blocked_on_raw = sys.argv[9]       # quoted path or "null"

# Parse directive_path
directive_path = None
if directive_path_raw != 'null':
    directive_path = directive_path_raw.strip('"')

# Parse pid
pid = None
if pid_raw != 'null':
    try:
        pid = int(pid_raw)
    except ValueError:
        pass

# Parse blocked_on
blocked_on = None
if blocked_on_raw != 'null':
    blocked_on = blocked_on_raw.strip('"')

doc = {
    "slug": slug,
    "stage": stage,
    "stage_started_at": stage_started_at,
    "last_heartbeat": last_heartbeat,
    "directive_path": directive_path,
    "current_action": current_action,
    "pid": pid,
    "blocked_on": blocked_on,
}

with open(path, 'w') as f:
    json.dump(doc, f, indent=2)
    f.write('\n')

print(f"wrote {path}")
PYEOF
