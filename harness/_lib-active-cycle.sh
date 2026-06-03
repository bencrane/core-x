#!/usr/bin/env bash
# _lib-active-cycle.sh — shared active-cycle detection helper for HQ hook scripts.
#
# SOURCE, don't exec this file:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   # shellcheck source=/dev/null
#   source "$SCRIPT_DIR/_lib-active-cycle.sh"
#
# Functions defined here:
#   get_active_slug <scope_status_dir> <reports_dir>
#
# Active-cycle detection algorithm:
#   1. Iterate <scope_status_dir>/*/validator.json sorted by mtime (newest first).
#   2. For each: check validator_frozen_at != null (validator has signed off).
#   3. For each frozen: check <reports_dir>/*scope-<slug>-*.md glob
#      — if any match, the cycle is closed; skip.
#   4. First match (frozen, no report) is the active slug.
#   5. If no candidate matches: outputs nothing (caller detects empty output).
#
# Preconditions:
#   - jq must be installed (caller should check and fail-open if not).
#   - macOS stat (-f) or GNU stat (-c) auto-detected.
#
# Output: echoes the active slug to stdout, or nothing if no active cycle found.
# Side effects: none (no globals set).

get_active_slug() {
    local scope_status_dir="$1"
    local reports_dir="$2"

    # Detect which stat variant is available.
    local stat_cmd
    if stat -f "%m" /dev/null >/dev/null 2>&1; then
        stat_cmd="stat -f %m"   # macOS
    else
        stat_cmd="stat -c %Y"   # GNU
    fi

    # Collect validator.json files sorted by mtime descending.
    local sorted_files
    sorted_files=$(
        find "$scope_status_dir" -name "validator.json" -maxdepth 2 2>/dev/null |
        while IFS= read -r f; do
            mtime=$($stat_cmd "$f" 2>/dev/null) || continue
            printf '%s %s\n' "$mtime" "$f"
        done | sort -rn | awk '{print $2}'
    )

    local vfile slug report_count
    while IFS= read -r vfile; do
        [ -f "$vfile" ] || continue

        # Check validator_frozen_at != null; skip on jq error.
        if ! jq -e '.validator_frozen_at != null' "$vfile" >/dev/null 2>&1; then
            continue  # Not frozen yet — skip.
        fi

        # Extract slug from path: .../scope-status/<slug>/validator.json
        slug=$(basename "$(dirname "$vfile")")
        [ -z "$slug" ] && continue

        # Check if a cycle-report file exists for this slug.
        report_count=$(find "$reports_dir" -maxdepth 1 -name "*scope-${slug}-*.md" 2>/dev/null | wc -l)
        if [ "$report_count" -gt 0 ]; then
            continue  # Cycle already closed — skip.
        fi

        # This is the active cycle.
        printf '%s' "$slug"
        return 0
    done <<< "$sorted_files"

    # No active cycle found.
    return 0
}
