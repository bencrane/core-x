#!/usr/bin/env bash
# UserPromptSubmit hook — pre-Claude guardrail.
# Blocks the prompt from reaching Claude if it contains a clearly-secret
# pattern. Conservative: only blocks high-confidence matches.
#
# Receives {prompt, session_id, cwd, ...} on stdin.
# Exit 2 with stderr = block. Exit 0 = allow.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/_lib-remediation.sh"

INPUT=$(cat || true)
PROMPT=$(echo "$INPUT" | python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read() or '{}')
    print(d.get('prompt', ''))
except Exception:
    print('')
" 2>/dev/null || echo "")

[ -z "$PROMPT" ] && exit 0

# Pattern :::> human-readable label. Each pattern must be specific enough to
# NEVER match prose. Delimiter is `:::` so regex `|` doesn't conflict.
declare -a PATTERNS=(
    'sk-ant-api03-[A-Za-z0-9_-]{40,}:::Anthropic API key'
    'sk-proj-[A-Za-z0-9_-]{40,}:::OpenAI project key'
    'sk-[a-zA-Z0-9]{32,}:::generic OpenAI-style key'
    'AKIA[0-9A-Z]{16}:::AWS access key id'
    'gh[pousr]_[A-Za-z0-9]{36,}:::GitHub personal access token'
    'dp\.(st|ct|sa|pt|wt)\.[A-Za-z0-9_.-]{20,}:::Doppler token'
    'xox[abprs]-[0-9]+-[0-9]+-[a-zA-Z0-9]+:::Slack token'
    'postgres(ql)?://[^:/[:space:]]+:[^@[:space:]]+@:::Postgres URL with embedded password'
    'mysql://[^:/[:space:]]+:[^@[:space:]]+@:::MySQL URL with embedded password'
    'mongodb(\+srv)?://[^:/[:space:]]+:[^@[:space:]]+@:::MongoDB URL with embedded password'
    'redis://[^:/[:space:]]+:[^@[:space:]]+@:::Redis URL with embedded password'
    '-----BEGIN [A-Z ]+PRIVATE KEY-----:::PEM private key'
)

for entry in "${PATTERNS[@]}"; do
    pattern=${entry%:::*}
    label=${entry##*:::}
    if echo "$PROMPT" | grep -qE -e "$pattern"; then
        emit_remediation \
            "Prompt blocked: contains what looks like a ${label}. The secret pattern class was matched but the actual value is not echoed here to avoid re-injecting the secret into context." \
            "Remove or redact the secret from your message and resubmit. Use Doppler / 1Password references instead of pasting raw credentials. If this is a false positive (e.g., a literal example in documentation), strip the value before resubmitting." \
            "${SCRIPT_DIR}/hook-userpromptsubmit.sh (see PATTERNS array)"
        exit 2
    fi
done

# ── Telegram blocker-reply detection ─────────────────────────────────────────
# If the prompt contains a <channel source="telegram"...> block, check whether
# it is a reply to a blocker notification.  Two reply forms are supported:
#
#   Form (a): message body matches ^@blocker\s+<slug>:\s+<answer>$
#   Form (b): the <channel> tag has a reply_to_message_id attribute matching a
#             telegram_message_id recorded in ~/Desktop/hq/blockers/*.md
#
# On match: write ~/Desktop/hq/blockers/{slug}-response.md with YAML frontmatter
# and stamp responded_at into the original blocker file.
# Idempotent: if response file already exists, skip with a stderr note.
# Always exits 0 (we're augmenting, not gating).

if echo "$PROMPT" | grep -q '<channel source="telegram"'; then
  python3 - "$PROMPT" <<'PYEOF'
import sys, re, os, json
from datetime import datetime, timezone

prompt = sys.argv[1]
blockers_dir = os.path.expanduser("~/Desktop/hq/blockers")
now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Extract <channel ...> tag fields
tag_m = re.search(
    r'<channel\s+source="telegram"'
    r'(?:\s+chat_id="([^"]*)")?'
    r'(?:\s+message_id="([^"]*)")?'
    r'(?:\s+user="([^"]*)")?'
    r'(?:\s+ts="([^"]*)")?'
    r'(?:\s+reply_to_message_id="([^"]*)")?'
    r'[^>]*>'
    r'(.*?)</channel>',
    prompt,
    re.DOTALL
)
if not tag_m:
    sys.exit(0)

chat_id     = tag_m.group(1) or ""
message_id  = tag_m.group(2) or ""
reply_to_id = tag_m.group(5) or ""
content     = tag_m.group(6).strip()

slug   = None
answer = None

# Form (a): @blocker slug: answer
form_a = re.match(r'^@blocker\s+([A-Za-z0-9_-]+):\s*(.+)$', content, re.DOTALL)
if form_a:
    slug   = form_a.group(1)
    answer = form_a.group(2).strip()

# Form (b): reply_to_message_id lookup
if slug is None and reply_to_id:
    for fname in os.listdir(blockers_dir):
        if not fname.endswith(".md") or fname.startswith("."):
            continue
        fpath = os.path.join(blockers_dir, fname)
        try:
            with open(fpath) as fh:
                text = fh.read()
        except Exception:
            continue
        m = re.match(r'^---\s*\n(.*?)\n---\s*\n', text, re.DOTALL)
        if not m:
            continue
        for line in m.group(1).splitlines():
            if line.startswith("telegram_message_id:"):
                val = line.split(":", 1)[1].strip()
                if val == reply_to_id:
                    slug_m = re.search(r'^slug:\s*(.+)$', m.group(1), re.MULTILINE)
                    if slug_m:
                        slug   = slug_m.group(1).strip()
                        answer = content
                    break
        if slug:
            break

if slug is None or answer is None:
    sys.exit(0)

response_file = os.path.join(blockers_dir, f"{slug}-response.md")

# Idempotent: skip if response already exists
if os.path.exists(response_file):
    print(f"blocker reply already recorded for {slug}", file=sys.stderr)
    sys.exit(0)

# Write response file
frontmatter = f"""---
slug: {slug}
answer: {json.dumps(answer)}
responded_at: {now_iso}
source: telegram
chat_id: "{chat_id}"
message_id: "{message_id}"
---
"""
with open(response_file, "w") as fh:
    fh.write(frontmatter)

# Stamp responded_at into the original blocker file
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
blocker_file = os.path.join(blockers_dir, f"{today}-{slug}.md")
if os.path.exists(blocker_file):
    with open(blocker_file) as fh:
        bc = fh.read()
    bc = re.sub(
        r'^responded_at: null$',
        f'responded_at: {now_iso}',
        bc,
        flags=re.MULTILINE
    )
    with open(blocker_file, "w") as fh:
        fh.write(bc)

print(f"blocker reply recorded: slug={slug} response={response_file}", file=sys.stderr)
PYEOF
fi

exit 0
