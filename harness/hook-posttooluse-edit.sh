#!/usr/bin/env bash
# PostToolUse hook for Edit/Write — file-type-aware quick checks.
# Surfaces issues to stderr (visible to agent) but never blocks.
#
# Coverage:
#   .py        → AST syntax check (always); ruff check (if installed)
#   .sql       → sqlfluff lint (if installed)
#   .json      → jq validate
#   .yaml/.yml → PyYAML safe_load
#   .toml      → tomllib parse (Python 3.11+)
#   .ts/.tsx   → skipped (tsc per-file is fragile and slow)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/_lib-remediation.sh"

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read())
    print(d.get('tool_input', {}).get('file_path', ''))
except Exception:
    print('')
" 2>/dev/null || echo "")

[ -z "$FILE_PATH" ] && exit 0
[ ! -f "$FILE_PATH" ] && exit 0

EXT="${FILE_PATH##*.}"
BASENAME=$(basename "$FILE_PATH")
report() {
    emit_remediation \
        "[${BASENAME}] $*" \
        "Fix the syntax or lint issue in ${FILE_PATH} and retry the edit." \
        "${FILE_PATH}"
}

case "$EXT" in
    py)
        if ! python3 -c "
import ast, sys
with open(sys.argv[1]) as f:
    ast.parse(f.read(), sys.argv[1])
" "$FILE_PATH" 2>/tmp/hq-syntax.err; then
            report "Python syntax error:"
            cat /tmp/hq-syntax.err >&2
        fi
        if command -v ruff >/dev/null 2>&1; then
            if ! ruff check "$FILE_PATH" 2>/tmp/hq-ruff.err >/dev/null; then
                report "ruff:"
                cat /tmp/hq-ruff.err >&2
            fi
        fi
        ;;
    sql)
        if command -v sqlfluff >/dev/null 2>&1; then
            sqlfluff lint --dialect postgres "$FILE_PATH" 2>&1 | head -30 >&2 || true
        fi
        ;;
    json)
        if ! jq . "$FILE_PATH" >/dev/null 2>/tmp/hq-json.err; then
            report "JSON parse error:"
            cat /tmp/hq-json.err >&2
        fi
        ;;
    yaml|yml)
        if ! python3 -c "
import yaml, sys
with open(sys.argv[1]) as f:
    yaml.safe_load(f)
" "$FILE_PATH" 2>/tmp/hq-yaml.err; then
            report "YAML parse error:"
            cat /tmp/hq-yaml.err >&2
        fi
        ;;
    toml)
        if ! python3 -c "
import sys
try:
    import tomllib
except ImportError:
    sys.exit(0)
with open(sys.argv[1], 'rb') as f:
    tomllib.load(f)
" "$FILE_PATH" 2>/tmp/hq-toml.err; then
            report "TOML parse error:"
            cat /tmp/hq-toml.err >&2
        fi
        ;;
esac

exit 0
