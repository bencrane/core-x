#!/usr/bin/env bash
# Benchmark wrapper — gtm-mcp Lance retrieval code path.
#   ./gtm-mcp-lance-retrieval-opt.sh                 # deterministic local-fixture metric (JSON)
#   ./gtm-mcp-lance-retrieval-opt.sh --reps 15       # more reps
#   ./gtm-mcp-lance-retrieval-opt.sh --live          # live-R2 correctness smoke (doppler creds)
# Emits one JSON line on stdout. The optimization metric is `.median_ms` (local mode).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BENCH="$SCRIPT_DIR/gtm-mcp-lance-retrieval-opt.py"

# Python: prefer the repo-local venv, else the canonical core-x venv (worktrees share it), else python3.
PY="$REPO_ROOT/.venv/bin/python"
[ -x "$PY" ] || PY="$HOME/core-x/.venv/bin/python"
[ -x "$PY" ] || PY="python3"

MODE="local"; REPS="9"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --live) MODE="live"; shift ;;
    --reps) REPS="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [ "$MODE" = "live" ]; then
  exec doppler run --project core-x --config prd -- "$PY" "$BENCH" --mode live --reps "$REPS"
fi
exec "$PY" "$BENCH" --mode local --reps "$REPS"
