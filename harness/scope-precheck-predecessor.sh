#!/usr/bin/env bash
# scope-precheck-predecessor.sh — verify a /scope directive's predecessor is complete
#
# Usage:  scope-precheck-predecessor.sh <abs-path-to-directive.md>
#
# Reads the directive's `**Predecessor:**` field. If non-null, follows the cited
# predecessor file and verifies its `**Status:**` first token is in the allowed
# set {complete, runtime-verified}. Used by /scope SKILL.md Stage 2 validators.
#
# Exit codes:
#   0 — predecessor is `none`, OR predecessor's Status is complete/runtime-verified
#   1 — predecessor's Status is something else (blocked, in-progress, etc.)
#   2 — predecessor file unreadable (path resolved, but file not found / no read perms)
#   3 — Predecessor field malformed (no parseable path, empty field, GitHub URL only)
#
# Path-shape coverage (per validator's corpus survey 2026-05-04):
#   - Bare absolute path:      /Users/.../*.md             — handled
#   - file:// markdown link:   [label](file:///Users/.../*.md) — handled
#   - Tilde-prefixed:          ~/Desktop/hq/directives/X.md — handled (HOME-expanded)
#   - Bare relative path:      2026-05-02-foo.md           — handled (resolved against vault directives/)
#   - Backtick relative path:  `reports/2026-05-02-foo.md` — handled (resolved against vault root)
#   - Markdown link relative:  [label](2026-05-02-foo.md)  — handled
#   - GitHub PR/HTTPS URL:     [label](https://github.com/...) — exit 3 (out of corpus scope)
#   - Empty / no `*.md`:       exit 3 (malformed)

set -u

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <abs-path-to-directive.md>" >&2
  exit 3
fi

DIRECTIVE="$1"

if [[ ! -r "$DIRECTIVE" ]]; then
  echo "FAIL: directive file unreadable: $DIRECTIVE" >&2
  exit 2
fi

# Find the first **Predecessor:** line.
PRED_LINE=$(grep -m 1 '^\*\*Predecessor:\*\*' "$DIRECTIVE" || true)

if [[ -z "$PRED_LINE" ]]; then
  echo "FAIL: directive has no **Predecessor:** line: $DIRECTIVE" >&2
  exit 3
fi

# Strip the prefix and trim leading whitespace.
PRED_VALUE=$(echo "$PRED_LINE" | sed -E 's/^\*\*Predecessor:\*\*[[:space:]]*//')

# Trim trailing whitespace.
PRED_VALUE=$(echo "$PRED_VALUE" | sed -E 's/[[:space:]]+$//')

# Case 1: starts with `none` (case-insensitive, with optional parenthetical).
PRED_VALUE_LOWER=$(echo "$PRED_VALUE" | tr '[:upper:]' '[:lower:]')
if [[ "$PRED_VALUE_LOWER" =~ ^none($|[[:space:]]|\() ]]; then
  echo "OK: no predecessor"
  exit 0
fi

# Case 2: empty.
if [[ -z "$PRED_VALUE" ]]; then
  echo "FAIL: Predecessor field is empty" >&2
  exit 3
fi

# Case 3: extract the first path-shaped substring.
# Try in order:
#   a) bare absolute path /Users/.../X.md
#   b) file:///Users/.../X.md
#   c) tilde-prefixed ~/.../X.md
#   d) backtick-wrapped relative `reports/X.md` or `directives/X.md`
#   e) markdown-link relative target [label](X.md)
#   f) bare relative X.md (last resort)
PRED_PATH=""

# (a) bare absolute /Users path
if [[ "$PRED_VALUE" =~ (/Users/[^[:space:]]+\.md) ]]; then
  PRED_PATH="${BASH_REMATCH[1]}"
# (b) file:// URI
elif [[ "$PRED_VALUE" =~ file://(/Users/[^[:space:]]+\.md) ]]; then
  PRED_PATH="${BASH_REMATCH[1]}"
# (c) tilde-prefixed
elif [[ "$PRED_VALUE" =~ (~/[^[:space:]]+\.md) ]]; then
  PRED_PATH="${BASH_REMATCH[1]/#\~/$HOME}"
# (d) backtick-wrapped relative
elif [[ "$PRED_VALUE" =~ \`([^[:space:]\`]+\.md)\` ]]; then
  REL="${BASH_REMATCH[1]}"
  if [[ "$REL" == /* ]]; then
    PRED_PATH="$REL"
  else
    # Resolve against vault root (~/Desktop/hq/) for backtick-wrapped paths,
    # since the corpus shows them as `reports/...md` or `directives/...md`.
    PRED_PATH="$HOME/Desktop/hq/$REL"
  fi
# (e) markdown-link relative target [label](X.md) — capture group 1 is the URL
elif [[ "$PRED_VALUE" =~ \]\(([^[:space:]\)]+\.md)\) ]]; then
  REL="${BASH_REMATCH[1]}"
  if [[ "$REL" == /* ]]; then
    PRED_PATH="$REL"
  elif [[ "$REL" == file://* ]]; then
    PRED_PATH="${REL#file://}"
  elif [[ "$REL" == http*://* ]]; then
    # GitHub URL or other HTTP — out of scope.
    echo "FAIL: Predecessor markdown link points to non-file URL: $REL (out of corpus scope; use a local .md path)" >&2
    exit 3
  else
    # Markdown-link relative paths in the corpus are mostly `2026-...md` or `../reports/...md`,
    # all resolved against ~/Desktop/hq/directives/.
    PRED_PATH="$HOME/Desktop/hq/directives/$REL"
  fi
# (f) markdown-link to a non-.md URL (e.g. GitHub PR, HTTPS) — no .md in URL
elif [[ "$PRED_VALUE" =~ \]\((https?://[^[:space:]\)]+)\) ]]; then
  # GitHub URL or other HTTP link without a .md path — out of scope.
  echo "FAIL: Predecessor markdown link points to non-file URL: ${BASH_REMATCH[1]} (out of corpus scope; use a local .md path)" >&2
  exit 3
# (g) bare relative .md path (last resort)
elif [[ "$PRED_VALUE" =~ ([^[:space:]\)\]]+\.md) ]]; then
  REL="${BASH_REMATCH[1]}"
  if [[ "$REL" == /* ]]; then
    PRED_PATH="$REL"
  else
    PRED_PATH="$HOME/Desktop/hq/directives/$REL"
  fi
else
  echo "FAIL: no parseable .md path in Predecessor field: $PRED_VALUE" >&2
  exit 3
fi

# Normalize ../ in the path.
PRED_PATH=$(cd "$(dirname "$PRED_PATH")" 2>/dev/null && pwd)/$(basename "$PRED_PATH") || true

if [[ ! -r "$PRED_PATH" ]]; then
  echo "FAIL: predecessor file unreadable: $PRED_PATH" >&2
  exit 2
fi

# Find the LAST **Status:** line in the predecessor; fall back to last **Verdict:**.
# Last-wins handles the directive lifecycle: the top-of-body Status line describes
# intent (set at Stage 1), and a later Status line is appended at cycle close. Reading
# the first match silently consumed the stale top-of-body value when the cycle was
# done — observed 2026-05-06: G1 had body line 10 = "execution-ready" and line 334
# = "complete"; grep -m 1 picked the stale one and blocked G2's predecessor gate.
# Cycle reports use **Verdict:** (single line, no drift); directives use **Status:**.
STATUS_LINE=$(grep '^\*\*Status:\*\*' "$PRED_PATH" | tail -n 1 || true)
if [[ -z "$STATUS_LINE" ]]; then
  STATUS_LINE=$(grep '^\*\*Verdict:\*\*' "$PRED_PATH" | tail -n 1 || true)
fi

if [[ -z "$STATUS_LINE" ]]; then
  echo "FAIL: predecessor file has no **Status:** or **Verdict:** line: $PRED_PATH" >&2
  exit 3
fi

# Extract the status token: strip the field prefix, lowercase, then take the first
# run of letters/hyphens. Matching a letter-run rather than awk's first whitespace
# token skips leading markdown decoration (**bold**, `backticks`, ✓/✅/⚠ glyphs) —
# e.g. 2026-05-20 `**complete ✓ 2026-05-20**` left a literal `**complete` that failed.
STATUS_TOKEN=$(echo "$STATUS_LINE" | sed -E 's/^\*\*(Status|Verdict):\*\*[[:space:]]*//' | tr '[:upper:]' '[:lower:]' | grep -oE '[a-z][a-z-]*' | head -n 1)

case "$STATUS_TOKEN" in
  complete|runtime-verified|shipped)
    echo "OK: predecessor $PRED_PATH Status=$STATUS_TOKEN"
    exit 0
    ;;
  *)
    echo "FAIL: predecessor $PRED_PATH Status=$STATUS_TOKEN; expected one of: complete, runtime-verified, shipped" >&2
    exit 1
    ;;
esac
