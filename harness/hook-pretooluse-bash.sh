#!/usr/bin/env bash
# PreToolUse hook for Bash — block destructive commands at the firewall.
#
# Receives {tool_name, tool_input: {command, ...}} on stdin.
# Exit 2 with stderr message = block (Claude sees the message).
# Exit 0 = allow.
#
# Forbidden commands (mechanical patterns, not exhaustive):
#   - rm -rf of absolute paths or $HOME (~)
#   - DROP TABLE / DROP DATABASE / DROP SCHEMA / TRUNCATE TABLE
#   - git push with --force (incl. --force-with-lease) targeting main/master
#   - git reset --hard origin/main|master
#   - --no-verify on any git command
#   - --no-gpg-sign on any git command
#
# To override: ask the user to run the command in their terminal directly.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/_lib-remediation.sh"

INPUT=$(cat)
COMMAND=$(echo "$INPUT" | python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read())
    print(d.get('tool_input', {}).get('command', ''))
except Exception:
    print('')
" 2>/dev/null || echo "")

[ -z "$COMMAND" ] && exit 0

block() {
    local reason=$1
    emit_remediation \
        "Bash command blocked by PreToolUse safety guard: ${reason}. Command: ${COMMAND}" \
        "This command matches a destructive or safety-bypass pattern. Ask the user to run the command directly in their terminal if it is intentional." \
        "Override patterns documented in ${SCRIPT_DIR}/hook-pretooluse-bash.sh (see Forbidden commands section)"
    exit 2
}

# rm -rf of absolute path or $HOME (~). Relative paths (./node_modules) are fine.
if echo "$COMMAND" | grep -qE 'rm[[:space:]]+(-[rRf]+[[:space:]]*)+[[:space:]]*[/~]'; then
    block "rm -rf of absolute path or \$HOME"
fi

# rm -rf with glob at path root
if echo "$COMMAND" | grep -qE 'rm[[:space:]]+(-[rRf]+[[:space:]]*)+[[:space:]]*/\*'; then
    block "rm -rf /*"
fi

# Destructive SQL DDL
if echo "$COMMAND" | grep -qiE '\b(DROP[[:space:]]+(TABLE|DATABASE|SCHEMA)|TRUNCATE[[:space:]]+TABLE)\b'; then
    block "destructive SQL DDL (DROP/TRUNCATE)"
fi

# Force push to main/master (catches --force AND --force-with-lease)
if echo "$COMMAND" | grep -qE 'git[[:space:]]+push[[:space:]].*--force.*\b(main|master)\b'; then
    block "force-push to main/master"
fi
if echo "$COMMAND" | grep -qE 'git[[:space:]]+push[[:space:]]+(origin[[:space:]]+)?(main|master)[[:space:]].*--force'; then
    block "force-push to main/master"
fi

# Hard reset to origin/main or origin/master
if echo "$COMMAND" | grep -qE 'git[[:space:]]+reset[[:space:]]+--hard[[:space:]]+origin/(main|master)\b'; then
    block "git reset --hard origin/main|master"
fi

# Bypass verifications
if echo "$COMMAND" | grep -qE '(^|[[:space:]])(--no-verify|--no-gpg-sign)([[:space:]]|$)'; then
    block "verification bypass flag (--no-verify or --no-gpg-sign)"
fi

# qmd retrieval-layer privacy guard: forbid indexing transcripts, snapshots, or code symlinks.
# Sources: reports/2026-05-02-qmd-research-findings.md §5 (privacy) and §4.2 (collection scope).
# Transcripts contain live secrets; snapshots are JSON metadata better queried at source;
# code/ symlinks would balloon the index to 65+ GB across four repos.
if echo "$COMMAND" | grep -qE 'qmd[[:space:]]+collection[[:space:]]+add[[:space:]].*(raw/transcripts|raw/snapshots|/Desktop/hq/code(/|[[:space:]]|$))'; then
    block "qmd collection add against forbidden path (raw/transcripts, raw/snapshots, or code/)"
fi

exit 0
