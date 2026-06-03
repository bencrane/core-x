#!/usr/bin/env bash
# Write a /scope cycle report to ~/Desktop/hq/reports/.
#
# Two modes:
#
#   AGENT MODE (rich content) — call from Stage 4 of the orchestrator AND from
#   every blocker exit point. The agent supplies the content; the script writes
#   the canonical file and echoes the path so the agent can include it in the
#   reply to the human.
#
#     scope-cycle-report.sh \
#       --slug 2026-05-03-source-irs-bmf-and-propublica-990 \
#       --verdict complete \
#       --directive ~/Desktop/hq/directives/2026-05-03-source-irs-bmf-and-propublica-990.md \
#       --summary-file /tmp/summary.md
#
#     # Or pass summary inline:
#     scope-cycle-report.sh --slug X --verdict blocked-no-rollback --summary "..."
#
#   GC-STALE MODE (hook fallback) — invoked by hook-stop.sh each turn. Scans
#   ~/Desktop/hq/scope-status/ for any cycle whose last_heartbeat is older than
#   $STALE_THRESHOLD seconds AND whose JSON does not have cycle_report_path.
#   Writes a placeholder report for each such cycle, then deletes the stale
#   status file.
#
#     scope-cycle-report.sh --gc-stale
#
# OUTPUT
#   stdout: absolute path of the written report (one line per report in gc-stale)
#   side effect (agent mode): updates scope-status/{slug}.json adding
#     "cycle_report_path" and "cycle_report_written_at" so the GC pass knows
#     this slug already has a report.
#   log: appends one JSON line per write to ~/Desktop/hq/raw/scope-cycle-report.jsonl

set -uo pipefail

REPORTS_DIR="$HOME/Desktop/hq/reports"
STATUS_DIR="$HOME/Desktop/hq/scope-status"
LOG_FILE="$HOME/Desktop/hq/raw/scope-cycle-report.jsonl"
STALE_THRESHOLD="${SCOPE_STALE_THRESHOLD:-1800}"   # 30 min default (premortem H5 — was 600, over-fired during long DDL)

# Clamp obviously-too-low values so a bad operator-supplied env var can't
# regress to the prior over-firing behavior. 120s is the hard floor (>= 2x
# the typical heartbeat cadence noted in SKILL.md).
if [[ "$STALE_THRESHOLD" =~ ^[0-9]+$ ]] && (( STALE_THRESHOLD < 120 )); then
  printf 'warning: SCOPE_STALE_THRESHOLD=%s < 120; clamping to 120\n' "$STALE_THRESHOLD" >&2
  STALE_THRESHOLD=120
fi

mkdir -p "$REPORTS_DIR"

# --- Argument parsing -----------------------------------------------------

MODE="agent"
SLUG=""
VERDICT=""
DIRECTIVE=""
SUMMARY=""
SUMMARY_FILE=""
PREDICTIONS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --gc-stale)        MODE="gc-stale"; shift ;;
    --slug)            SLUG="$2"; shift 2 ;;
    --verdict)         VERDICT="$2"; shift 2 ;;
    --directive)       DIRECTIVE="$2"; shift 2 ;;
    --summary)         SUMMARY="$2"; shift 2 ;;
    --summary-file)    SUMMARY_FILE="$2"; shift 2 ;;
    --predictions)     PREDICTIONS="$2"; shift 2 ;;
    --hook-fallback)   MODE="hook-fallback"; shift ;;
    -h|--help)
      sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

# --- Helpers --------------------------------------------------------------

_log() {
  local entry="$1"
  printf '%s\n' "$entry" >>"$LOG_FILE" 2>/dev/null || true
}

_safe_slug() {
  # Make a slug safe for filenames; lowercase, dashes only.
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | sed -e 's/[^a-z0-9-]/-/g' -e 's/--*/-/g' -e 's/^-//' -e 's/-$//'
}

_write_report() {
  local slug="$1" verdict="$2" directive="$3" mode="$4" summary="$5" summary_file="$6" predictions_file="${7:-}"
  local ts ts_date safe_slug safe_verdict report_path
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  ts_date=$(date -u +%Y-%m-%d)
  safe_slug=$(_safe_slug "$slug")
  safe_verdict=$(_safe_slug "${verdict:-unknown}")
  report_path="$REPORTS_DIR/${ts_date}-scope-${safe_slug}-${safe_verdict}.md"

  {
    printf '# /scope cycle report — %s\n\n' "$slug"
    printf '**Date:** %s  \n' "$ts"
    printf '**Verdict:** `%s`  \n' "${verdict:-unknown}"
    printf '**Slug:** `%s`  \n' "$slug"
    printf '**Mode:** %s  \n' "$mode"
    if [[ -n "$directive" ]]; then
      printf '**Directive:** [`%s`](%s)  \n' "$(basename "$directive")" "$directive"
    fi
    if [[ -f "$STATUS_DIR/${slug}.json" ]]; then
      printf '**scope-status:** `%s`  \n' "$STATUS_DIR/${slug}.json"
    fi
    if [[ -n "$predictions_file" ]]; then
      local pred_count
      pred_count=$(python3 -c "import json; d=json.load(open('$predictions_file')); print(len(d.get('predictions') or []))" 2>/dev/null || echo "?")
      printf '**Predictions manifest:** `%s` (%s predictions)  \n' "$predictions_file" "$pred_count"
    fi
    printf '\n'

    if [[ "$mode" == "hook-fallback" ]]; then
      printf '## Auto-generated placeholder\n\n'
      printf 'The end-of-cycle hook fired but the /scope orchestrator did not invoke `scope-cycle-report.sh` itself before its scope-status heartbeat went stale (>%ds since last update). This placeholder exists so a report file always lives at a predictable path for downstream tooling.\n\n' "$STALE_THRESHOLD"
      printf '**Forensic recovery:**\n'
      [[ -n "$directive" && -f "$directive" ]] && printf -- '- directive: `%s`\n' "$directive"
      [[ -f "$STATUS_DIR/${slug}.json" ]] && printf -- '- scope-status snapshot at GC time: see log entry for `%s` in `%s`\n' "$slug" "$LOG_FILE"
      printf -- '- session record: latest `%s/sessions/*-%s*.md` (or check `%s/sessions/active/`)\n' "$HOME/Desktop/hq" "$slug" "$HOME/Desktop/hq"
      printf -- '- subagent log: grep for `%s` in `%s/sessions/_subagents.jsonl`\n' "$slug" "$HOME/Desktop/hq"
      printf '\nRoot-cause likely: agent crashed, was interrupted, or completed without invoking the cycle-report mandate. Update SKILL.md if a class of cycles is consistently slipping through.\n'
    else
      printf '## Summary\n\n'
      if [[ -n "$summary_file" && -f "$summary_file" ]]; then
        cat "$summary_file"
      elif [[ -n "$summary" ]]; then
        printf '%s\n' "$summary"
      else
        printf '_(no summary supplied)_\n'
      fi
    fi
  } > "$report_path"

  # Stamp scope-status JSON with the report path so the GC pass knows we're done.
  if [[ -f "$STATUS_DIR/${slug}.json" ]]; then
    python3 - "$STATUS_DIR/${slug}.json" "$report_path" "$ts" <<'PYEOF' 2>/dev/null || true
import json, sys
path, report_path, ts = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    with open(path) as f: d = json.load(f)
except Exception:
    d = {}
d["cycle_report_path"] = report_path
d["cycle_report_written_at"] = ts
with open(path, "w") as f: json.dump(d, f, indent=2)
PYEOF
  fi

  # Log line.
  _log "{\"ts\":\"$ts\",\"slug\":\"$slug\",\"verdict\":\"${verdict:-unknown}\",\"mode\":\"$mode\",\"path\":\"$report_path\"}"

  printf '%s\n' "$report_path"
}

# --- Mode: gc-stale -------------------------------------------------------

if [[ "$MODE" == "gc-stale" ]]; then
  [[ -d "$STATUS_DIR" ]] || exit 0

  shopt -s nullglob
  files=("$STATUS_DIR"/*.json)
  shopt -u nullglob
  (( ${#files[@]} == 0 )) && exit 0

  now_epoch=$(date +%s)

  for status_file in "${files[@]}"; do
    [[ -f "$status_file" ]] || continue
    s=$(basename "$status_file" .json)

    # Read heartbeat + report path + directive from the JSON (one-shot Python).
    read -r heartbeat report_path directive_path < <(python3 - "$status_file" <<'PYEOF' 2>/dev/null
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    print("- - -"); sys.exit(0)
hb = d.get("last_heartbeat") or "-"
rp = d.get("cycle_report_path") or "-"
dp = d.get("directive_path") or "-"
print(f"{hb} {rp} {dp}")
PYEOF
)
    [[ -z "${heartbeat:-}" || "$heartbeat" == "-" ]] && continue

    # Parse ISO8601 to epoch. BSD `date -j -f` interprets the input in the
    # process timezone, so force UTC since the heartbeat strings are UTC.
    hb_clean="${heartbeat%Z}"
    hb_epoch=$(TZ=UTC date -j -f "%Y-%m-%dT%H:%M:%S" "$hb_clean" +%s 2>/dev/null || echo 0)
    [[ "$hb_epoch" -eq 0 ]] && continue

    age=$((now_epoch - hb_epoch))
    (( age < STALE_THRESHOLD )) && continue   # still active

    if [[ "$report_path" == "-" ]]; then
      # Stale + no report → write the fallback BEFORE deleting the status file.
      directive_arg=""
      [[ "$directive_path" != "-" ]] && directive_arg="$directive_path"
      _write_report "$s" "abandoned-stale-heartbeat" "$directive_arg" "hook-fallback" "" "" >/dev/null || true
    fi

    rm -f "$status_file"
  done
  exit 0
fi

# --- Mode: agent (default) ------------------------------------------------

if [[ -z "$SLUG" ]]; then
  echo "FAIL: --slug required in agent mode" >&2
  exit 2
fi
if [[ -z "$VERDICT" ]]; then
  echo "FAIL: --verdict required in agent mode (e.g. complete | partial | blocked-<reason>)" >&2
  exit 2
fi
if [[ -z "$SUMMARY" && -z "$SUMMARY_FILE" ]]; then
  echo "FAIL: --summary or --summary-file required in agent mode" >&2
  exit 2
fi
if [[ -z "$PREDICTIONS" ]]; then
  echo "FAIL: --predictions required in agent mode (pass the validator.json path for this cycle)" >&2
  printf 'predictions required\n' >&2
  exit 2
fi
if [[ ! -f "$PREDICTIONS" ]]; then
  echo "FAIL: --predictions file not found: $PREDICTIONS" >&2
  printf 'predictions required\n' >&2
  exit 2
fi

_write_report "$SLUG" "$VERDICT" "$DIRECTIVE" "$MODE" "$SUMMARY" "$SUMMARY_FILE" "$PREDICTIONS"
