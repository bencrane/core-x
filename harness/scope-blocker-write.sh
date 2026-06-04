#!/usr/bin/env bash
# scope-blocker-write.sh — write a structured blocker file and signal a Telegram send.
#
# USAGE
#   scope-blocker-write.sh [OPTIONS]
#   scope-blocker-write.sh --slug <slug> --record-message-id <int>
#
# MODES
#   Live mode (default): writes the blocker file, then prints a JSON line to stdout
#     that the orchestrator (the /scope agent) reads and uses to call
#     mcp__plugin_telegram_telegram__reply.  The orchestrator then calls this helper
#     again with --record-message-id to stamp the returned message_id back into the
#     frontmatter.
#
#   --dry-run: writes the blocker file and appends an audit line to
#     ~/Desktop/hq/blockers/.dry-run-log instead of printing a JSON action.
#
#   --record-message-id <int>: opens an existing blocker file (identified by
#     --slug and today's date) and stamps telegram_message_id in the frontmatter.
#     All other flags are ignored in this mode.
#
# ORCHESTRATOR ROUND-TRIP (live mode)
#   1. Agent calls scope-blocker-write.sh [flags]
#   2. Helper writes file, prints JSON to stdout:
#      {"action":"send_telegram","chat_id":"<from access.json>","text":"<msg>","slug":"<slug>"}
#   3. Agent calls mcp__plugin_telegram_telegram__reply with chat_id + text,
#      receives message_id from Telegram.
#   4. Agent calls scope-blocker-write.sh --slug <slug> --record-message-id <int>
#      to stamp the message_id into the blocker file.
#
# Constraint 8: chat_id is ALWAYS read from ~/.claude/channels/telegram/access.json
# allowFrom[0]. If allowFrom has != 1 entry, the helper exits non-zero.
#
# REQUIRED KEYS in written frontmatter (Constraint 7):
#   slug, directive_path, stage, blocker_type, created_at,
#   telegram_message_id (null initially), responded_at (null),
#   question, options (list)
#
# OPTIONAL KEYS:
#   recommended_option, repo, additional_context_paths

set -euo pipefail

# ── helpers ─────────────────────────────────────────────────────────────────
usage() {
  cat <<'USAGE'
scope-blocker-write.sh — write a structured blocker file

SYNOPSIS
  scope-blocker-write.sh \
    --slug <slug>                    # required (lowercase-kebab)
    --directive-path <abs-path>      # required
    --stage <validator|executor|deploy-verifier|scope-decomposer>
    --blocker-type <blocked-X|needs-human-input>
    --question <text>
    [--option <text>]...             # repeatable; may be empty list
    [--recommended-option <text>]
    [--repo <name>]
    [--additional-context-path <abs-path>]... # repeatable
    [--dry-run]

  scope-blocker-write.sh --slug <slug> --record-message-id <int>
    Stamps the telegram_message_id into an existing blocker file.

EXIT CODES
  0   success
  1   usage / config error
  2   access.json has wrong number of allowFrom entries

OUTPUT (live mode, stdout)
  {"action":"send_telegram","chat_id":"<chat_id>","text":"<msg>","slug":"<slug>"}
  The orchestrator uses this JSON to call mcp__plugin_telegram_telegram__reply.
USAGE
}

die() { echo "ERROR: $*" >&2; exit 1; }

# ── parse args ───────────────────────────────────────────────────────────────
SLUG=""
DIRECTIVE_PATH=""
STAGE=""
BLOCKER_TYPE=""
QUESTION=""
OPTIONS=()
RECOMMENDED_OPTION=""
REPO=""
ADDITIONAL_CONTEXT_PATHS=()
DRY_RUN=false
RECORD_MESSAGE_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --slug)                   SLUG="$2";                             shift 2 ;;
    --directive-path)         DIRECTIVE_PATH="$2";                   shift 2 ;;
    --stage)                  STAGE="$2";                            shift 2 ;;
    --blocker-type)           BLOCKER_TYPE="$2";                     shift 2 ;;
    --question)               QUESTION="$2";                         shift 2 ;;
    --option)                 OPTIONS+=("$2");                       shift 2 ;;
    --recommended-option)     RECOMMENDED_OPTION="$2";               shift 2 ;;
    --repo)                   REPO="$2";                             shift 2 ;;
    --additional-context-path) ADDITIONAL_CONTEXT_PATHS+=("$2");    shift 2 ;;
    --dry-run)                DRY_RUN=true;                          shift 1 ;;
    --record-message-id)      RECORD_MESSAGE_ID="$2";               shift 2 ;;
    --help|-h)                usage; exit 0 ;;
    *) die "Unknown flag: $1" ;;
  esac
done

# ── read access.json for chat_id (Constraint 8) ──────────────────────────────
ACCESS_JSON="$HOME/.claude/channels/telegram/access.json"
[[ -f "$ACCESS_JSON" ]] || die "access.json not found at $ACCESS_JSON"

ALLOW_COUNT=$(python3 -c "
import json, sys
d = json.load(open('$ACCESS_JSON'))
print(len(d.get('allowFrom', [])))
" 2>/dev/null) || die "Failed to parse $ACCESS_JSON"

if [[ "$ALLOW_COUNT" != "1" ]]; then
  echo "expected exactly one allowFrom entry, got $ALLOW_COUNT — run /telegram:access" >&2
  exit 2
fi

CHAT_ID=$(python3 -c "
import json
d = json.load(open('$ACCESS_JSON'))
print(d['allowFrom'][0])
" 2>/dev/null) || die "Failed to extract chat_id from $ACCESS_JSON"

# ── --record-message-id mode ─────────────────────────────────────────────────
if [[ -n "$RECORD_MESSAGE_ID" ]]; then
  [[ -n "$SLUG" ]] || die "--slug is required with --record-message-id"
  TODAY=$(date -u +%Y-%m-%d)
  BLOCKER_FILE="$HOME/Desktop/hq/blockers/${TODAY}-${SLUG}.md"
  [[ -f "$BLOCKER_FILE" ]] || die "Blocker file not found: $BLOCKER_FILE"

  # Replace 'telegram_message_id: null' with the actual value
  python3 - "$BLOCKER_FILE" "$RECORD_MESSAGE_ID" <<'PYEOF'
import sys, re
path, mid = sys.argv[1], sys.argv[2]
with open(path) as f:
    content = f.read()
content = re.sub(
    r'^telegram_message_id: null$',
    f'telegram_message_id: {mid}',
    content,
    flags=re.MULTILINE
)
with open(path, 'w') as f:
    f.write(content)
print(f"stamped telegram_message_id={mid} in {path}")
PYEOF
  exit 0
fi

# ── validate required args for write mode ────────────────────────────────────
[[ -n "$SLUG" ]]           || die "--slug is required"
[[ -n "$DIRECTIVE_PATH" ]] || die "--directive-path is required"
[[ -n "$STAGE" ]]          || die "--stage is required"
[[ -n "$BLOCKER_TYPE" ]]   || die "--blocker-type is required"
[[ -n "$QUESTION" ]]       || die "--question is required"

# Validate stage enum
case "$STAGE" in
  validator|executor|deploy-verifier|scope-decomposer) ;;
  *) die "Invalid --stage '$STAGE'; must be validator|executor|deploy-verifier|scope-decomposer" ;;
esac

# Validate blocker_type enum
case "$BLOCKER_TYPE" in
  blocked-telegram-unreachable|blocked-needs-decision|blocked-needs-data|blocked-other|needs-human-input) ;;
  *) die "Invalid --blocker-type '$BLOCKER_TYPE'; must be blocked-telegram-unreachable|blocked-needs-decision|blocked-needs-data|blocked-other|needs-human-input" ;;
esac

# ── build output paths ───────────────────────────────────────────────────────
mkdir -p "$HOME/Desktop/hq/blockers"
TODAY=$(date -u +%Y-%m-%d)
BLOCKER_FILE="$HOME/Desktop/hq/blockers/${TODAY}-${SLUG}.md"
CREATED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# ── build YAML options list ──────────────────────────────────────────────────
OPTIONS_YAML=""
if [[ ${#OPTIONS[@]} -eq 0 ]]; then
  OPTIONS_YAML="options: []"
else
  OPTIONS_YAML="options:"$'\n'
  for opt in "${OPTIONS[@]}"; do
    OPTIONS_YAML+="  - \"${opt}\""$'\n'
  done
  OPTIONS_YAML="${OPTIONS_YAML%$'\n'}"
fi

# ── build optional keys ──────────────────────────────────────────────────────
OPTIONAL_YAML=""
[[ -n "$RECOMMENDED_OPTION" ]] && OPTIONAL_YAML+="recommended_option: \"${RECOMMENDED_OPTION}\""$'\n'
[[ -n "$REPO" ]]               && OPTIONAL_YAML+="repo: \"${REPO}\""$'\n'
if [[ ${#ADDITIONAL_CONTEXT_PATHS[@]} -gt 0 ]]; then
  OPTIONAL_YAML+="additional_context_paths:"$'\n'
  for p in "${ADDITIONAL_CONTEXT_PATHS[@]}"; do
    OPTIONAL_YAML+="  - \"${p}\""$'\n'
  done
fi

# ── write the blocker file ───────────────────────────────────────────────────
cat > "$BLOCKER_FILE" <<FRONTMATTER
---
slug: ${SLUG}
directive_path: ${DIRECTIVE_PATH}
stage: ${STAGE}
blocker_type: ${BLOCKER_TYPE}
created_at: ${CREATED_AT}
telegram_message_id: null
responded_at: null
question: "${QUESTION}"
${OPTIONS_YAML}
${OPTIONAL_YAML}---

# Blocker: ${SLUG}

**Stage:** ${STAGE}
**Type:** ${BLOCKER_TYPE}
**Created:** ${CREATED_AT}

## Question

${QUESTION}

$(if [[ ${#OPTIONS[@]} -gt 0 ]]; then
  echo "## Options"
  echo ""
  for opt in "${OPTIONS[@]}"; do
    echo "- ${opt}"
  done
  if [[ -n "$RECOMMENDED_OPTION" ]]; then
    echo ""
    echo "**Recommended:** ${RECOMMENDED_OPTION}"
  fi
fi)

## How to respond

- Reply on Telegram with \`@blocker ${SLUG}: <your answer>\`
- Or reply-to the notification message with your answer.
- The /scope session will resume automatically.
FRONTMATTER

# ── telegram message text ────────────────────────────────────────────────────
MSG="[scope-blocker] ${SLUG} (${STAGE}): ${QUESTION}"
if [[ ${#OPTIONS[@]} -gt 0 ]]; then
  MSG+=$'\n'"Options: $(IFS=', '; echo "${OPTIONS[*]}")"
  [[ -n "$RECOMMENDED_OPTION" ]] && MSG+=$'\n'"Recommended: ${RECOMMENDED_OPTION}"
fi
MSG+=$'\n'"Reply: @blocker ${SLUG}: <answer>"

# ── dry-run: log to .dry-run-log ─────────────────────────────────────────────
if [[ "$DRY_RUN" == "true" ]]; then
  DRY_RUN_LOG="$HOME/Desktop/hq/blockers/.dry-run-log"
  {
    echo "---"
    echo "ts: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "slug: ${SLUG}"
    echo "would_send_to_chat_id: ${CHAT_ID}"
    echo "text: ${MSG}"
    echo "blocker_file: ${BLOCKER_FILE}"
  } >> "$DRY_RUN_LOG"
  echo "dry-run: blocker file written to ${BLOCKER_FILE}; Telegram send logged to ${DRY_RUN_LOG}" >&2
  exit 0
fi

# ── live mode: print JSON for orchestrator to act on ────────────────────────
# The ORCHESTRATOR (the /scope agent) reads this stdout and calls
# mcp__plugin_telegram_telegram__reply, then calls us again with --record-message-id.
python3 -c "
import json, sys
print(json.dumps({
    'action': 'send_telegram',
    'chat_id': sys.argv[1],
    'text': sys.argv[2],
    'slug': sys.argv[3]
}))
" "$CHAT_ID" "$MSG" "$SLUG"
