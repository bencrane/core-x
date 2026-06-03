#!/usr/bin/env bash
# SessionStart hook — loads HQ context into the new session.
#
# Surfaces:
#   1. Vault location + pointer to PROTOCOL.md
#
# Output is prepended to the new session's context (invisible to user, visible to agent).

set -uo pipefail
VAULT="$HOME/Desktop/hq"

echo "=== HQ PROTOCOL CONTEXT ==="
echo "Vault: $VAULT  ·  See $VAULT/PROTOCOL.md for the full lifecycle."
echo
echo "=== END HQ PROTOCOL CONTEXT ==="
