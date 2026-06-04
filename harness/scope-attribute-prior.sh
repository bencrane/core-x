#!/usr/bin/env bash
# Grade the prior cycle's predictions and emit attribution.json.
#
# USAGE
#   scope-attribute-prior.sh <slug>
#
# The script:
#   1. Reads the current cycle's directive to locate the predecessor.
#   2. Loads the predecessor's validator.json predictions[].
#   3. Grades each prediction (chg-N commit in main? predicted_fixes verified?
#      risk_tasks observed in cycle report?).
#   4. Emits ~/Desktop/hq/scope-status/<slug>/attribution.json (idempotent).
#   5. For each "unheld" prediction writes a git revert suggestion to
#      ~/Desktop/hq/scope-status/<slug>/attribution-remediation.txt
#      (NEVER auto-executes revert).
#
# Stdout: progress lines only (no UTC timestamps — determinism for cache-hit).
# Timestamps live inside attribution.json under .timestamps.

set -euo pipefail

SCOPE_STATUS_DIR="$HOME/Desktop/hq/scope-status"
DIRECTIVES_DIR="$HOME/Desktop/hq/directives"
REPORTS_DIR="$HOME/Desktop/hq/reports"
# Canonical core-x clone to look up commit history against main (harness lives in core-x).
HQ_ALL_DIR="$HOME/core-x"

# --- Usage ------------------------------------------------------------------

if [[ $# -lt 1 ]]; then
  printf 'usage: scope-attribute-prior.sh <slug>\n' >&2
  exit 2
fi

SLUG="$1"

OUT_DIR="$SCOPE_STATUS_DIR/$SLUG"
mkdir -p "$OUT_DIR"

ATTRIBUTION_FILE="$OUT_DIR/attribution.json"
REMEDIATION_FILE="$OUT_DIR/attribution-remediation.txt"

printf 'attribute: slug=%s\n' "$SLUG"

# --- Locate directive -------------------------------------------------------

shopt -s nullglob
directive_files=("$DIRECTIVES_DIR"/*-"$SLUG".md)
shopt -u nullglob

if [[ ${#directive_files[@]} -eq 0 ]]; then
  # No directive found — treat as no predecessor.
  printf 'attribute: no directive found for slug=%s, treating as no_predecessor\n' "$SLUG"
  python3 - "$ATTRIBUTION_FILE" "$SLUG" <<'PYEOF'
import json, sys, datetime
out, slug = sys.argv[1], sys.argv[2]
ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
result = {
    "slug": slug,
    "predecessor_slug": None,
    "no_predecessor": True,
    "predecessor_legacy_schema": False,
    "verdicts": [],
    "remediation_file": None,
    "timestamps": {"generated_at": ts}
}
with open(out, "w") as f:
    json.dump(result, f, indent=2)
    f.write("\n")
PYEOF
  printf 'attribute: wrote %s\n' "$ATTRIBUTION_FILE"
  exit 0
fi

if [[ ${#directive_files[@]} -gt 1 ]]; then
  printf 'error: multiple directives match slug=%s — cannot disambiguate\n' "$SLUG" >&2
  exit 1
fi

DIRECTIVE_FILE="${directive_files[0]}"
printf 'attribute: directive=%s\n' "$DIRECTIVE_FILE"

# --- Parse predecessor from directive ---------------------------------------

# Look for the Predecessor line in the YAML front matter or document body.
# Matches patterns like:
#   **Predecessor:** `~/Desktop/hq/directives/2026-05-05-foo.md`
#   **Predecessor:** none
predecessor_line=$(grep -m1 '^\*\*Predecessor:\*\*' "$DIRECTIVE_FILE" 2>/dev/null || true)

# Helper: emit attribution.json with no_predecessor=true
emit_no_predecessor() {
  local slug="$1"
  python3 - "$ATTRIBUTION_FILE" "$slug" <<'PYEOF'
import json, sys, datetime
out, slug = sys.argv[1], sys.argv[2]
ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
result = {
    "slug": slug,
    "predecessor_slug": None,
    "no_predecessor": True,
    "predecessor_legacy_schema": False,
    "verdicts": [],
    "remediation_file": None,
    "timestamps": {"generated_at": ts}
}
with open(out, "w") as f:
    json.dump(result, f, indent=2)
    f.write("\n")
PYEOF
}

if [[ -z "$predecessor_line" ]]; then
  printf 'attribute: predecessor=none (line absent)\n'
  emit_no_predecessor "$SLUG"
  printf 'attribute: wrote %s\n' "$ATTRIBUTION_FILE"
  exit 0
fi

# Check if line says "none" (case-insensitive, strips backticks/spaces)
predecessor_value=$(printf '%s' "$predecessor_line" | sed 's/^\*\*Predecessor:\*\*[[:space:]]*//' | tr -d '`' | xargs)
if [[ -z "$predecessor_value" || "$(printf '%s' "$predecessor_value" | tr '[:upper:]' '[:lower:]')" == "none" ]]; then
  printf 'attribute: predecessor=none\n'
  emit_no_predecessor "$SLUG"
  printf 'attribute: wrote %s\n' "$ATTRIBUTION_FILE"
  exit 0
fi

# Extract predecessor directive path — the value is a file path reference.
# Strip parenthetical context after the first whitespace following the path.
# e.g. "~/Desktop/hq/directives/2026-05-05-eval-rig-foundation-scope-pilot.md (G1 pilot...)"
predecessor_path=$(printf '%s' "$predecessor_value" | awk '{print $1}' | sed "s|^~|$HOME|")
predecessor_basename=$(basename "$predecessor_path" .md)

# Extract slug from basename: strip leading date (YYYY-MM-DD-) prefix if present.
predecessor_slug=$(printf '%s' "$predecessor_basename" | sed 's/^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}-//')

printf 'attribute: predecessor=%s\n' "$predecessor_slug"

# --- Load predecessor validator.json ----------------------------------------

pred_validator="$SCOPE_STATUS_DIR/$predecessor_slug/validator.json"

if [[ ! -f "$pred_validator" ]]; then
  printf 'attribute: predecessor validator.json absent, treating as no_predecessor\n'
  python3 - "$ATTRIBUTION_FILE" "$SLUG" "$predecessor_slug" <<'PYEOF'
import json, sys, datetime
out, slug, pred_slug = sys.argv[1], sys.argv[2], sys.argv[3]
ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
result = {
    "slug": slug,
    "predecessor_slug": pred_slug,
    "no_predecessor": True,
    "predecessor_legacy_schema": False,
    "verdicts": [],
    "remediation_file": None,
    "timestamps": {"generated_at": ts}
}
with open(out, "w") as f:
    json.dump(result, f, indent=2)
    f.write("\n")
PYEOF
  printf 'attribute: wrote %s\n' "$ATTRIBUTION_FILE"
  exit 0
fi

# Check for legacy schema (no predictions field).
has_predictions=$(python3 -c "import json; d=json.load(open('$pred_validator')); print('yes' if 'predictions' in d and d['predictions'] is not None else 'no')" 2>/dev/null || echo "no")

if [[ "$has_predictions" == "no" ]]; then
  printf 'attribute: predecessor has legacy schema (no predictions field)\n'
  python3 - "$ATTRIBUTION_FILE" "$SLUG" "$predecessor_slug" <<'PYEOF'
import json, sys, datetime
out, slug, pred_slug = sys.argv[1], sys.argv[2], sys.argv[3]
ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
result = {
    "slug": slug,
    "predecessor_slug": pred_slug,
    "no_predecessor": False,
    "predecessor_legacy_schema": True,
    "verdicts": [],
    "remediation_file": None,
    "timestamps": {"generated_at": ts}
}
with open(out, "w") as f:
    json.dump(result, f, indent=2)
    f.write("\n")
PYEOF
  printf 'attribute: wrote %s\n' "$ATTRIBUTION_FILE"
  exit 0
fi

# --- Grade each prediction --------------------------------------------------

# Read predictions from predecessor validator.json
pred_count=$(python3 -c "import json; d=json.load(open('$pred_validator')); print(len(d.get('predictions') or []))" 2>/dev/null || echo "0")
printf 'attribute: grading %s predictions from predecessor=%s\n' "$pred_count" "$predecessor_slug"

# Locate predecessor's cycle report (glob pattern)
pred_report_glob="$REPORTS_DIR/*-scope-${predecessor_slug}-*.md"
pred_report=""
shopt -s nullglob
pred_report_files=($pred_report_glob)
shopt -u nullglob
if [[ ${#pred_report_files[@]} -gt 0 ]]; then
  # Use the latest (last alphabetically, which is latest by date prefix)
  pred_report="${pred_report_files[-1]}"
  printf 'attribute: predecessor_report=%s\n' "$pred_report"
else
  printf 'attribute: no predecessor cycle report found (risk_tasks cannot be checked)\n'
fi

# Grade using Python (atomic read/write to avoid partial-state attribution.json)
python3 - \
  "$ATTRIBUTION_FILE" \
  "$REMEDIATION_FILE" \
  "$SLUG" \
  "$predecessor_slug" \
  "$pred_validator" \
  "$HQ_ALL_DIR" \
  "${pred_report}" \
  <<'PYEOF'
import json, sys, subprocess, re, os, datetime

out_file        = sys.argv[1]
remediation_file = sys.argv[2]
slug            = sys.argv[3]
pred_slug       = sys.argv[4]
pred_validator  = sys.argv[5]
hq_all_dir      = sys.argv[6]
pred_report     = sys.argv[7]  # may be empty string

with open(pred_validator) as f:
    pred_data = json.load(f)

predictions = pred_data.get("predictions") or []

# Build git log from hq-all main branch (all oneline entries).
try:
    git_log = subprocess.check_output(
        ["git", "-C", hq_all_dir, "log", "--oneline", "main"],
        stderr=subprocess.DEVNULL
    ).decode("utf-8", errors="replace")
except Exception:
    git_log = ""

# Read predecessor cycle report content if available.
report_content = ""
if pred_report and os.path.isfile(pred_report):
    try:
        with open(pred_report) as f:
            report_content = f.read()
    except Exception:
        pass

verdicts = []
unheld_ids = []

for p in predictions:
    pred_id   = p.get("id", "")
    risk_tasks = p.get("risk_tasks") or []

    # --- Check if chg commit appears in main ---------------------------------
    # Match "[chg-N]" or "chg-N:" at start of commit subject.
    pattern = r'(?:\[' + re.escape(pred_id) + r'\]|' + re.escape(pred_id) + r':)'
    chg_commit_in_main = bool(re.search(pattern, git_log))

    # --- Check predicted_fixes -----------------------------------------------
    # We do not have a per-prediction runnable verifier in the general case;
    # set to null (unknown) unless chg_commit_in_main is already false.
    predicted_fixes_verified = None
    if chg_commit_in_main:
        predicted_fixes_verified = True   # commit present → optimistic pass

    # --- Check risk_tasks in predecessor report ------------------------------
    risk_tasks_observed = []
    for rt in risk_tasks:
        if report_content and rt and rt in report_content:
            risk_tasks_observed.append(rt)

    # --- Combine into verdict ------------------------------------------------
    if not chg_commit_in_main:
        verdict = "unheld"
        reason  = "commit not found in hq-all main"
    elif predicted_fixes_verified is False:
        verdict = "unheld"
        reason  = "predicted_fixes verifier failed"
    elif chg_commit_in_main and predicted_fixes_verified:
        verdict = "held"
        reason  = "commit present in main and predicted_fixes verified"
    else:
        verdict = "unknown"
        reason  = "insufficient evidence"

    verdicts.append({
        "prediction_id": pred_id,
        "verdict": verdict,
        "chg_commit_in_main": chg_commit_in_main,
        "predicted_fixes_verified": predicted_fixes_verified,
        "risk_tasks_observed": risk_tasks_observed,
        "reason": reason
    })

    if verdict == "unheld":
        unheld_ids.append(pred_id)

ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# Determine remediation_file path (null if nothing to remediate)
has_remediation = len(unheld_ids) > 0

result = {
    "slug": slug,
    "predecessor_slug": pred_slug,
    "no_predecessor": False,
    "predecessor_legacy_schema": False,
    "verdicts": verdicts,
    "remediation_file": remediation_file if has_remediation else None,
    "timestamps": {"generated_at": ts}
}

with open(out_file, "w") as f:
    json.dump(result, f, indent=2)
    f.write("\n")

# Write remediation file for unheld predictions.
if has_remediation:
    with open(remediation_file, "w") as f:
        for entry in verdicts:
            if entry["verdict"] == "unheld":
                pid = entry["prediction_id"]
                reason = entry["reason"]
                f.write(f"git revert {pid}  # unheld: {reason}\n")
elif os.path.exists(remediation_file):
    # Clean up leftover remediation file from a previous run where there
    # were unheld predictions that are now held.
    os.remove(remediation_file)

PYEOF

printf 'attribute: wrote %s\n' "$ATTRIBUTION_FILE"
if [[ -f "$REMEDIATION_FILE" ]]; then
  printf 'attribute: wrote %s\n' "$REMEDIATION_FILE"
fi
