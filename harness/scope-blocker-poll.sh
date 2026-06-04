#!/usr/bin/env bash
# scope-blocker-poll.sh — poll for a blocker response file, then exit with the answer.
#
# USAGE
#   scope-blocker-poll.sh <slug> [--timeout <seconds>] [--interval <seconds>]
#
# DESCRIPTION
#   Polls ~/Desktop/hq/blockers/{slug}-response.md on a sleep loop.
#   On success (file appears): exits 0 and prints ONLY the answer field to stdout.
#   On timeout: exits 124 and prints "polling timed out for slug=<slug> after <N>s" to stderr.
#
# The orchestrator calls this as a synchronous (foreground) Bash tool call — it
# blocks the parent turn but matches the existing /scope "spawn subagent and wait"
# pattern (which already blocks the parent).
#
# FUTURE SEAM: if a caller wants --timeout >3600 (beyond ScheduleWakeup's 1-hour
# clamp), the helper SHOULD exit 0 with sentinel text "polling deferred — re-invoke
# with --resume" and the orchestrator switches to ScheduleWakeup.  That extension
# is OUT OF SCOPE for this directive but the seam is documented here.
#
# EXIT CODES
#   0    response file appeared; answer printed to stdout
#   1    bad arguments
#   124  timeout (matches GNU timeout(1) convention)

set -uo pipefail

# ── usage ─────────────────────────────────────────────────────────────────────
usage() {
  cat <<'USAGE'
scope-blocker-poll.sh <slug> [--timeout <seconds>] [--interval <seconds>]

  <slug>              required positional — slug of the blocker to wait on
  --timeout <seconds> default 1800 (30 min)
  --interval <seconds> default 30

EXIT CODES
  0    response appeared; answer is printed to stdout (and ONLY the answer)
  1    bad arguments
  124  timed out
USAGE
}

die() { echo "ERROR: $*" >&2; exit 1; }

# ── parse args ────────────────────────────────────────────────────────────────
SLUG=""
TIMEOUT=1800
INTERVAL=30

# First positional arg is the slug
if [[ $# -ge 1 && ! "$1" =~ ^-- ]]; then
  SLUG="$1"
  shift
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --timeout)  TIMEOUT="$2";  shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --help|-h)  usage; exit 0 ;;
    *) die "Unknown flag: $1" ;;
  esac
done

[[ -n "$SLUG" ]] || { usage >&2; die "<slug> is required"; }

RESPONSE_FILE="$HOME/Desktop/hq/blockers/${SLUG}-response.md"
DEADLINE=$(( $(date +%s) + TIMEOUT ))
ELAPSED=0

# ── poll loop ─────────────────────────────────────────────────────────────────
while true; do
  if [[ -f "$RESPONSE_FILE" ]]; then
    # Extract the 'answer' field from YAML frontmatter
    ANSWER=$(python3 - "$RESPONSE_FILE" <<'PYEOF'
import sys, re

path = sys.argv[1]
with open(path) as f:
    content = f.read()

# Extract YAML frontmatter between the first two --- lines
m = re.match(r'^---\s*\n(.*?)\n---\s*\n', content, re.DOTALL)
if not m:
    print('', end='')
    sys.exit(0)

frontmatter = m.group(1)
for line in frontmatter.splitlines():
    if line.startswith('answer:'):
        val = line[len('answer:'):].strip()
        # strip surrounding quotes if any
        if (val.startswith('"') and val.endswith('"')) or \
           (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]
        print(val, end='')
        sys.exit(0)

print('', end='')
PYEOF
    )
    printf '%s' "$ANSWER"
    exit 0
  fi

  NOW=$(date +%s)
  if [[ "$NOW" -ge "$DEADLINE" ]]; then
    echo "polling timed out for slug=${SLUG} after ${TIMEOUT}s" >&2
    exit 124
  fi

  sleep "$INTERVAL"
done
