#!/usr/bin/env bash
# scope-race.sh — parallel scope-race primitive (G9)
#
# Spawns N variant subprocesses (synthetic exec_command mode OR Agent-dispatch
# mode), polls for the first winner flag, SIGTERM/SIGKILL losers, captures
# loser state, writes won.flag atomically, exits 0.
#
# Usage:
#   scope-race.sh --slug <slug> --variants <path-to-variant-spec.json> \
#                 --max-budget-min <N> --output-dir <dir>
#
# Synthetic mode (per variant with exec_command set):
#   Spawns: bash -c "$exec_command" with WIN_FLAG and LOSER_DIR exported.
#   Winner: first variant to touch its $WIN_FLAG file.
#
# Agent-dispatch mode (per variant without exec_command):
#   Spawns: claude -p "<per-variant prompt>"
#   P5 forward-compat: injects contract.md if present at
#   ~/Desktop/hq/scope-status/{slug}/contract.md
#
# Exit codes:
#   0 = winner found, won.flag written
#   4 = budget exhausted or all variants failed to produce a winner
#   2 = usage/schema error

set -euo pipefail

# ── Argument parsing ──────────────────────────────────────────────────────────

SLUG=""
VARIANTS_PATH=""
MAX_BUDGET_MIN=""
OUTPUT_DIR=""

while [ $# -gt 0 ]; do
    case "$1" in
        --slug)
            SLUG="$2"; shift 2 ;;
        --variants)
            VARIANTS_PATH="$2"; shift 2 ;;
        --max-budget-min)
            MAX_BUDGET_MIN="$2"; shift 2 ;;
        --output-dir)
            OUTPUT_DIR="$2"; shift 2 ;;
        *)
            echo "scope-race.sh: unknown flag '$1'" >&2
            echo "Usage: scope-race.sh --slug <slug> --variants <path> --max-budget-min <N> --output-dir <dir>" >&2
            exit 2 ;;
    esac
done

if [ -z "$SLUG" ] || [ -z "$VARIANTS_PATH" ] || [ -z "$MAX_BUDGET_MIN" ] || [ -z "$OUTPUT_DIR" ]; then
    echo "scope-race.sh: --slug, --variants, --max-budget-min, and --output-dir are all required" >&2
    exit 2
fi

case "$MAX_BUDGET_MIN" in
    ''|*[!0-9]*)
        echo "scope-race.sh: --max-budget-min must be a positive integer, got: $MAX_BUDGET_MIN" >&2
        exit 2 ;;
esac

# ── Idempotent re-run (P6) ────────────────────────────────────────────────────

WON_FLAG="${OUTPUT_DIR}/won.flag"

if [ -f "$WON_FLAG" ]; then
    PRIOR_WINNER=$(jq -re '.winner' "$WON_FLAG" 2>/dev/null || true)
    if [ -n "$PRIOR_WINNER" ]; then
        echo "$PRIOR_WINNER"
        exit 0
    fi
fi

# ── jq availability check ─────────────────────────────────────────────────────

if ! command -v jq >/dev/null 2>&1; then
    echo "scope-race.sh: 'jq' is required but not found in PATH" >&2
    exit 2
fi

# ── Validate variant-spec.json ────────────────────────────────────────────────

if [ ! -f "$VARIANTS_PATH" ]; then
    echo "scope-race.sh: variants file not found: $VARIANTS_PATH" >&2
    exit 2
fi

# Schema: array of objects with required 'name' (string) and 'mandatory_constraint' (string)
if ! jq -e '
    type == "array" and length > 0 and
    all(.[]; type == "object" and has("name") and has("mandatory_constraint") and
         (.name | type) == "string" and (.mandatory_constraint | type) == "string")
' "$VARIANTS_PATH" >/dev/null 2>&1; then
    echo "scope-race.sh: variant-spec.json schema invalid" >&2
    echo "  Expected: array of {name: string, mandatory_constraint: string, exec_command?: string, system_prompt_override?: string}" >&2
    exit 2
fi

VARIANT_COUNT=$(jq 'length' "$VARIANTS_PATH")
if [ "$VARIANT_COUNT" -eq 0 ]; then
    echo "scope-race.sh: variant-spec.json must contain at least one variant" >&2
    exit 2
fi

# ── Output directory layout ───────────────────────────────────────────────────

mkdir -p \
    "${OUTPUT_DIR}/winners" \
    "${OUTPUT_DIR}/losers" \
    "${OUTPUT_DIR}/variant-prompts"

# ── State files (bash 3.2 compat — no associative arrays) ────────────────────
# We store per-variant state in files under a state dir.
STATE_DIR="${OUTPUT_DIR}/.race-state"
mkdir -p "$STATE_DIR"

# ── Helper: ISO8601 timestamp ─────────────────────────────────────────────────

iso8601() {
    date -u '+%Y-%m-%dT%H:%M:%SZ'
}

# ── Helper: unix epoch in milliseconds ───────────────────────────────────────

epoch_ms() {
    python3 -c 'import time; print(int(time.time()*1000))' 2>/dev/null \
        || echo $(( $(date +%s) * 1000 ))
}

# ── Helper: format duration in seconds with 3 decimal places ─────────────────

duration_secs() {
    local start_ms="$1"
    local end_ms="$2"
    python3 -c "print('{:.3f}'.format(($end_ms - $start_ms) / 1000.0))" 2>/dev/null \
        || echo "0.000"
}

# ── Detect process-group isolation strategy ───────────────────────────────────

HAVE_SETSID=0
HAVE_PERL_SETSID=0
SCRIPT_PGID=$(ps -o pgid= -p $$ | tr -d ' ')

if command -v setsid >/dev/null 2>&1; then
    HAVE_SETSID=1
elif perl -MPOSIX -e 'POSIX::setsid(); exit 0' >/dev/null 2>&1; then
    HAVE_PERL_SETSID=1
fi

# ── Cleanup trap ──────────────────────────────────────────────────────────────

cleanup_all() {
    local pid_file name pid pgid
    for pid_file in "${STATE_DIR}/"*.pid; do
        [ -e "$pid_file" ] || continue
        pid=$(cat "$pid_file")
        name=$(basename "$pid_file" .pid)
        pgid_file="${STATE_DIR}/${name}.pgid"
        pgid=""
        [ -f "$pgid_file" ] && pgid=$(cat "$pgid_file")
        local use_pgid_cleanup=0
        [ "$HAVE_SETSID" -eq 1 ] && use_pgid_cleanup=1
        [ "$HAVE_PERL_SETSID" -eq 1 ] && use_pgid_cleanup=1
        if kill -0 "$pid" 2>/dev/null; then
            if [ "$use_pgid_cleanup" -eq 1 ] && [ -n "$pgid" ] && [ "$pgid" != "" ] && [ "$pgid" != "$SCRIPT_PGID" ]; then
                kill -TERM -"$pgid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
            else
                kill -TERM "$pid" 2>/dev/null || true
            fi
        fi
    done
}
trap cleanup_all EXIT

# ── Spawn each variant ────────────────────────────────────────────────────────

RACE_START_MS=$(epoch_ms)
RACE_START_EPOCH=$(date +%s)
MAX_BUDGET_SEC=$(( MAX_BUDGET_MIN * 60 ))

VARIANT_NAMES=""

i=0
while [ "$i" -lt "$VARIANT_COUNT" ]; do
    NAME=$(jq -re ".[$i].name" "$VARIANTS_PATH")
    EXEC_CMD=$(jq -re ".[$i].exec_command // empty" "$VARIANTS_PATH" 2>/dev/null || true)
    MANDATORY_CONSTRAINT=$(jq -re ".[$i].mandatory_constraint" "$VARIANTS_PATH")
    SYSTEM_PROMPT_OVERRIDE=$(jq -re ".[$i].system_prompt_override // empty" "$VARIANTS_PATH" 2>/dev/null || true)

    WINNER_FLAG_PATH="${OUTPUT_DIR}/winners/${NAME}.flag"
    LOSER_DIR="${OUTPUT_DIR}/losers/${NAME}"
    STDERR_FILE="${STATE_DIR}/${NAME}.stderr"

    mkdir -p "$LOSER_DIR"
    printf '%s' "$(epoch_ms)" > "${STATE_DIR}/${NAME}.start_ms"

    VARIANT_NAMES="${VARIANT_NAMES} ${NAME}"

    if [ -n "$EXEC_CMD" ]; then
        # Synthetic mode: spawn variant.
        # Wrap exec_command in a subshell that traps SIGTERM and kills its own
        # children — ensures "sleep N && touch $WIN_FLAG" patterns die cleanly
        # on macOS (no setsid) when the parent bash is SIGTERM'd.
        export WIN_FLAG="$WINNER_FLAG_PATH"
        export LOSER_DIR

        # Spawn in an isolated process group so kill -TERM -PGID cleans up
        # all descendants (e.g. sleep subprocesses in "sleep N && touch $WIN_FLAG").
        #
        # The bash wrapper traps SIGTERM and kills its own process group (kill 0)
        # which reaches all children since they're in the same PG (new PG created
        # by setsid before exec).
        # The wrapper script traps SIGTERM, kills the entire process group (-$$),
        # then runs exec_command in a subshell. Since setsid created a new PG,
        # kill -- -$$ sends SIGTERM to all processes in the PG safely.
        BASH_WRAPPER="
trap 'kill -- -\$\$ 2>/dev/null; wait; exit 143' TERM INT
$(printf '%s' "$EXEC_CMD")
"
        if [ "$HAVE_SETSID" -eq 1 ]; then
            # Linux: new session, then bash wrapper handles SIGTERM
            setsid bash -c "$BASH_WRAPPER" 2>"$STDERR_FILE" &
        elif [ "$HAVE_PERL_SETSID" -eq 1 ]; then
            # macOS: perl creates new PG (via setsid), then bash wrapper traps SIGTERM
            # and kills the whole PG (-$$) which is safe since PG != script's PG
            perl -MPOSIX -e 'POSIX::setsid(); exec "bash", "-c", $ARGV[0]' -- "$BASH_WRAPPER" 2>"$STDERR_FILE" &
        else
            # Fallback: no new PG; kill by PID only (children may survive)
            bash -c "$EXEC_CMD" 2>"$STDERR_FILE" &
        fi
        PID=$!
    else
        # Agent-dispatch mode: build per-variant prompt, spawn claude
        CONTRACT_PATH="${HOME}/Desktop/hq/scope-status/${SLUG}/contract.md"
        CONTRACT_INJECTION=""
        if [ -f "$CONTRACT_PATH" ]; then
            CONTRACT_INJECTION=$(printf '\n## Sprint contract\n%s' "$(cat "$CONTRACT_PATH")")
        fi

        VARIANT_PROMPT="You are scope-race variant '${NAME}' for slug '${SLUG}'.

Mandatory strategy constraint: ${MANDATORY_CONSTRAINT}${CONTRACT_INJECTION}

Read the directive at ~/Desktop/hq/directives/${SLUG}.md — end to end. Your mandatory_constraint is inviolable.
${SYSTEM_PROMPT_OVERRIDE:+System note: ${SYSTEM_PROMPT_OVERRIDE}}

When your work is complete and the success criteria are satisfied, write any non-empty content to:
  ${WINNER_FLAG_PATH}

Do NOT write to that path unless you have genuinely satisfied the success criteria. Writing it prematurely disqualifies all other variants and corrupts the race result.

Your per-variant working directory: ${LOSER_DIR}
"
        PROMPT_FILE="${OUTPUT_DIR}/variant-prompts/${NAME}.txt"
        printf '%s' "$VARIANT_PROMPT" > "$PROMPT_FILE"

        if command -v claude >/dev/null 2>&1; then
            if claude --help 2>&1 | grep -q '\-p\|--print'; then
                claude -p "$VARIANT_PROMPT" 2>"$STDERR_FILE" &
            else
                echo "scope-race.sh: 'claude' found but -p flag not confirmed; stub for variant '${NAME}'" >&2
                ( echo "claude -p dispatch stub: variant ${NAME}"; sleep 86400 ) 2>"$STDERR_FILE" &
            fi
        else
            echo "scope-race.sh: 'claude' not found; Agent-dispatch unavailable for variant '${NAME}'" >&2
            ( echo "claude not found; variant ${NAME} stub"; sleep 86400 ) 2>"$STDERR_FILE" &
        fi
        PID=$!
    fi

    printf '%s' "$PID" > "${STATE_DIR}/${NAME}.pid"

    # Capture PGID after setsid has had time to create the new session.
    # On macOS with perl setsid, the PGID changes from parent's PG to the
    # new PG (== PID) within ~50ms. We poll until the PGID differs from the
    # script's PGID, or until 200ms has elapsed.
    PGID=""
    _pgid_tries=0
    while [ "$_pgid_tries" -lt 4 ]; do
        _pgid_candidate=$(ps -o pgid= -p "$PID" 2>/dev/null | tr -d ' ') || _pgid_candidate=""
        if [ -n "$_pgid_candidate" ] && [ "$_pgid_candidate" != "$SCRIPT_PGID" ]; then
            PGID="$_pgid_candidate"
            break
        fi
        sleep 0.05
        _pgid_tries=$(( _pgid_tries + 1 ))
    done
    # Fallback if PGID never separated
    [ -z "$PGID" ] && PGID=$(ps -o pgid= -p "$PID" 2>/dev/null | tr -d ' ') || true
    printf '%s' "$PGID" > "${STATE_DIR}/${NAME}.pgid"

    i=$(( i + 1 ))
done

# ── Poll loop ─────────────────────────────────────────────────────────────────

WINNER=""
poll_result=0

while true; do
    NOW_EPOCH=$(date +%s)
    if [ $(( NOW_EPOCH - RACE_START_EPOCH )) -ge "$MAX_BUDGET_SEC" ]; then
        poll_result=1  # budget exhausted
        break
    fi

    # Scan winners dir for first flag
    for flag_file in "${OUTPUT_DIR}/winners/"*.flag; do
        [ -e "$flag_file" ] || continue
        WINNER=$(basename "$flag_file" .flag)
        poll_result=0
        break 2
    done

    # Check if all variants have exited
    all_dead=1
    for name in $VARIANT_NAMES; do
        pid_file="${STATE_DIR}/${name}.pid"
        [ -f "$pid_file" ] || continue
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            all_dead=0
            break
        fi
    done

    if [ "$all_dead" -eq 1 ]; then
        # Last-chance check for a flag written just before death
        for flag_file in "${OUTPUT_DIR}/winners/"*.flag; do
            [ -e "$flag_file" ] || continue
            WINNER=$(basename "$flag_file" .flag)
            poll_result=0
            break 2
        done
        poll_result=2  # all exited, no winner
        break
    fi

    sleep 0.05  # 50ms poll interval
done

RACE_END_MS=$(epoch_ms)

# ── Terminate all non-winner variants ────────────────────────────────────────

terminate_variant() {
    local name="$1"
    local pid_file="${STATE_DIR}/${name}.pid"
    local pgid_file="${STATE_DIR}/${name}.pgid"
    [ -f "$pid_file" ] || return 0
    local pid pgid
    pid=$(cat "$pid_file")
    pgid=""
    [ -f "$pgid_file" ] && pgid=$(cat "$pgid_file")

    local use_pgid=0
    [ "$HAVE_SETSID" -eq 1 ] && use_pgid=1
    [ "$HAVE_PERL_SETSID" -eq 1 ] && use_pgid=1

    if kill -0 "$pid" 2>/dev/null; then
        if [ "$use_pgid" -eq 1 ] && [ -n "$pgid" ] && [ "$pgid" != "" ] && [ "$pgid" != "$SCRIPT_PGID" ] && [ "$pgid" != "0" ]; then
            kill -TERM -"$pgid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
        else
            kill -TERM "$pid" 2>/dev/null || true
        fi
    fi
}

sigkill_variant() {
    local name="$1"
    local pid_file="${STATE_DIR}/${name}.pid"
    local pgid_file="${STATE_DIR}/${name}.pgid"
    [ -f "$pid_file" ] || return 0
    local pid pgid
    pid=$(cat "$pid_file")
    pgid=""
    [ -f "$pgid_file" ] && pgid=$(cat "$pgid_file")

    local use_pgid=0
    [ "$HAVE_SETSID" -eq 1 ] && use_pgid=1
    [ "$HAVE_PERL_SETSID" -eq 1 ] && use_pgid=1

    if kill -0 "$pid" 2>/dev/null; then
        if [ "$use_pgid" -eq 1 ] && [ -n "$pgid" ] && [ "$pgid" != "" ] && [ "$pgid" != "$SCRIPT_PGID" ] && [ "$pgid" != "0" ]; then
            kill -KILL -"$pgid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
        else
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi
}

# SIGTERM non-winners
for name in $VARIANT_NAMES; do
    [ "$name" = "$WINNER" ] && continue
    terminate_variant "$name"
done

# Wait up to 5s for graceful exit
KILL_DEADLINE=$(( $(date +%s) + 5 ))
while [ $(date +%s) -lt "$KILL_DEADLINE" ]; do
    still_alive=0
    for name in $VARIANT_NAMES; do
        [ "$name" = "$WINNER" ] && continue
        pid_file="${STATE_DIR}/${name}.pid"
        [ -f "$pid_file" ] || continue
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            still_alive=1
            break
        fi
    done
    [ "$still_alive" -eq 0 ] && break
    sleep 0.1
done

# SIGKILL stragglers
for name in $VARIANT_NAMES; do
    [ "$name" = "$WINNER" ] && continue
    sigkill_variant "$name"
done

# Reap all non-winner processes and save their exit codes
for name in $VARIANT_NAMES; do
    [ "$name" = "$WINNER" ] && continue
    pid_file="${STATE_DIR}/${name}.pid"
    [ -f "$pid_file" ] || continue
    pid=$(cat "$pid_file")
    ec=0
    wait "$pid" 2>/dev/null && ec=0 || ec=$?
    printf '%s' "$ec" > "${STATE_DIR}/${name}.exit_code"
done

# ── Capture loser state ───────────────────────────────────────────────────────

KILLED_AT=$(iso8601)

capture_loser() {
    local name="$1"
    local pid_file="${STATE_DIR}/${name}.pid"
    [ -f "$pid_file" ] || return 0
    local pid
    pid=$(cat "$pid_file")

    local start_ms
    start_ms=$(cat "${STATE_DIR}/${name}.start_ms" 2>/dev/null || echo "$RACE_START_MS")
    local duration
    duration=$(duration_secs "$start_ms" "$RACE_END_MS")

    # Determine exit code (process already reaped; read from state file)
    local exit_code=0
    local ec_file="${STATE_DIR}/${name}.exit_code"
    [ -f "$ec_file" ] && exit_code=$(cat "$ec_file") || exit_code=0

    local loser_dir="${OUTPUT_DIR}/losers/${name}"
    mkdir -p "$loser_dir"

    # Classify signal from exit code
    local signal_val="null"
    case "$exit_code" in
        143) signal_val='"SIGTERM"' ;;
        137) signal_val='"SIGKILL"' ;;
        130) signal_val='"SIGINT"'  ;;
        129) signal_val='"SIGHUP"'  ;;
    esac

    cat > "${loser_dir}/final-status.json" <<EOF
{
  "exit_code": ${exit_code},
  "signal": ${signal_val},
  "duration_seconds": ${duration},
  "killed_at": "${KILLED_AT}"
}
EOF

    # Move stderr log
    local stderr_tmp="${STATE_DIR}/${name}.stderr"
    if [ -f "$stderr_tmp" ]; then
        local size
        size=$(wc -c < "$stderr_tmp" 2>/dev/null || echo 0)
        if [ "$size" -gt 16384 ]; then
            tail -c 16384 "$stderr_tmp" > "${loser_dir}/stderr.log"
        else
            mv "$stderr_tmp" "${loser_dir}/stderr.log"
        fi
    else
        touch "${loser_dir}/stderr.log"
    fi
}

# ── Winner path ───────────────────────────────────────────────────────────────

if [ "$poll_result" -eq 0 ] && [ -n "$WINNER" ]; then
    # Capture loser state for all non-winners
    for name in $VARIANT_NAMES; do
        [ "$name" = "$WINNER" ] && continue
        capture_loser "$name"
    done

    RACE_DURATION=$(duration_secs "$RACE_START_MS" "$RACE_END_MS")
    WON_AT=$(iso8601)

    # Atomic write of won.flag
    TMP_FLAG="${WON_FLAG}.tmp.$$"
    printf '{"winner": "%s", "won_at": "%s", "duration_seconds": %s}\n' \
        "$WINNER" "$WON_AT" "$RACE_DURATION" > "$TMP_FLAG"
    mv "$TMP_FLAG" "$WON_FLAG"

    echo "$WINNER"
    trap - EXIT
    exit 0

else
    # No winner: budget exhausted or all exited
    REASON="budget_exhausted"
    [ "$poll_result" -eq 2 ] && REASON="all_exited_no_winner"

    # Capture each variant's final exit state
    VARIANTS_JSON="[]"
    for name in $VARIANT_NAMES; do
        pid_file="${STATE_DIR}/${name}.pid"
        pid=""
        [ -f "$pid_file" ] && pid=$(cat "$pid_file")

        exit_code=0
        ec_file="${STATE_DIR}/${name}.exit_code"
        [ -f "$ec_file" ] && exit_code=$(cat "$ec_file") || exit_code=0

        start_ms=$(cat "${STATE_DIR}/${name}.start_ms" 2>/dev/null || echo "$RACE_START_MS")
        duration=$(duration_secs "$start_ms" "$RACE_END_MS")

        ENTRY=$(jq -n \
            --arg name "$name" \
            --argjson exit_code "$exit_code" \
            --argjson duration "$duration" \
            --arg killed_at "$KILLED_AT" \
            '{name: $name, exit_code: $exit_code, duration_seconds: $duration, killed_at: $killed_at}')
        VARIANTS_JSON=$(printf '%s' "$VARIANTS_JSON" | jq --argjson entry "$ENTRY" '. + [$entry]')
    done

    cat > "${OUTPUT_DIR}/all-failed.json" <<EOF
{
  "reason": "${REASON}",
  "slug": "${SLUG}",
  "max_budget_min": ${MAX_BUDGET_MIN},
  "failed_at": "${KILLED_AT}",
  "variants": ${VARIANTS_JSON}
}
EOF

    echo "scope-race.sh: no winner produced for slug '${SLUG}' (${REASON})" >&2
    trap - EXIT
    exit 4
fi
