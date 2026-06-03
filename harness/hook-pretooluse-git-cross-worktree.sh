#!/usr/bin/env bash
# PreToolUse hook for Bash — block cross-worktree git destructive operations.
#
# Protects against five classes of ops that can silently destroy WIP from
# another worktree of the same repo. Empirical motivation: PR hq-all#141
# (2026-05-06) where an agent dropped stash@{0} labeled claude/exciting-
# easley-4846d2 while operating in claude/vigorous-bose-add739.
#
# Rules:
#   1. stash drop/clear/pop: block if target stash's branch != current worktree's branch
#      Override: FORCE_FOREIGN_STASH=1
#   2. branch -D: block if the branch is checked out in another worktree
#   3. reset --hard: warn (do not block) if another worktree's HEAD would be orphaned
#   4. worktree remove --force: always block
#   5. push --force on a branch != current worktree's branch: block; --force-with-lease allowed
#
# Receives {tool_name, tool_input: {command, ...}} on stdin.
# Exit 0 = allow. Exit 2 with stderr = block (Claude sees the message).
# No state mutation — read-only git inspection only.

set -uo pipefail

INPUT=$(cat)

# Parse tool_name + command from the JSON payload (mirror hook-pretooluse-bash.sh).
PARSED=$(printf '%s' "$INPUT" | python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read())
    name = d.get('tool_name', '') or ''
    cmd = (d.get('tool_input') or {}).get('command', '') or ''
    print(name + '\t' + cmd)
except Exception:
    print('\t')
" 2>/dev/null || printf '\t')

TOOL_NAME=${PARSED%%$'\t'*}
COMMAND=${PARSED#*$'\t'}

# Belt-and-suspenders: only inspect Bash tool calls.
case "$TOOL_NAME" in
    Bash) ;;
    *) exit 0 ;;
esac

[ -z "$COMMAND" ] && exit 0

# Resolve the directory the *command* will actually run in.
#
# Precedence (most explicit wins):
#   1. `git -C <path> ...` — path is the git target verbatim.
#   2. Leading `cd <path> && ...` — the cd target is the run dir.
#   3. $PWD — the hook's own cwd (usually the shell's persisted cwd).
#
# We deliberately do NOT consult $CLAUDE_PROJECT_DIR for repo resolution: it
# is fixed for the session at Claude Code launch, so it would misfire on any
# cross-repo command (e.g. `cd /Users/x/hq-zone && git stash pop` running
# from a Claude session launched in /Users/x/hq-all would otherwise be
# analyzed against hq-all's stash list, blocking legitimate work in hq-zone).
CWD=""
GIT_C_PATH=$(printf '%s' "$COMMAND" \
    | grep -oE 'git[[:space:]]+-C[[:space:]]+[^[:space:]]+' \
    | head -1 \
    | sed -E 's/^git[[:space:]]+-C[[:space:]]+//')
if [ -n "$GIT_C_PATH" ]; then
    CWD="$GIT_C_PATH"
else
    LEAD_CD=$(printf '%s' "$COMMAND" \
        | grep -oE '^[[:space:]]*cd[[:space:]]+[^[:space:]&|;]+' \
        | head -1 \
        | sed -E 's/^[[:space:]]*cd[[:space:]]+//')
    if [ -n "$LEAD_CD" ]; then
        CWD="$LEAD_CD"
    fi
fi
[ -z "$CWD" ] && CWD="$PWD"

SELF=$(git -C "$CWD" rev-parse --show-toplevel 2>/dev/null || true)
[ -z "$SELF" ] && exit 0

# Current branch (tolerate detached HEAD by falling back to short SHA).
SELF_BRANCH=$(git -C "$CWD" symbolic-ref --short HEAD 2>/dev/null \
    || git -C "$CWD" rev-parse --short HEAD 2>/dev/null || true)
[ -z "$SELF_BRANCH" ] && exit 0

# block() helper — matches hook-pretooluse-bash.sh shape.
block() {
    local reason=$1
    local extra=${2:-}
    {
        echo "🚫 BLOCKED by HQ firewall: $reason"
        echo "Command: $COMMAND"
        [ -n "$extra" ] && echo "$extra"
        echo "Override: ask the user to run this command directly in their terminal."
    } >&2
    exit 2
}

# ---------------------------------------------------------------------------
# Rule 1: stash drop / clear / pop — block if any target stash's branch label
# doesn't match SELF_BRANCH. Override: FORCE_FOREIGN_STASH=1.
# ---------------------------------------------------------------------------
if echo "$COMMAND" | grep -qE 'git[[:space:]]+stash[[:space:]]+(drop|clear|pop)\b'; then
    STASH_VERB=$(echo "$COMMAND" | grep -oE 'git[[:space:]]+stash[[:space:]]+(drop|clear|pop)' \
        | grep -oE '(drop|clear|pop)' | head -1)

    # Read the stash list once (read-only).
    STASH_LIST=$(git -C "$CWD" stash list 2>/dev/null || true)

    if [ "$STASH_VERB" = "clear" ]; then
        # clear nukes ALL stashes — check every entry.
        FOREIGN_LABELS=""
        while IFS= read -r line; do
            [ -z "$line" ] && continue
            # Line shapes:
            #   stash@{N}: WIP on <branch>: <sha> <msg>
            #   stash@{N}: On <branch>: <msg>
            branch_label=$(echo "$line" | sed -E 's/stash@\{[0-9]+\}: (WIP on |On )([^:]+):.*/\2/')
            if [ "$branch_label" != "$SELF_BRANCH" ]; then
                FOREIGN_LABELS="$FOREIGN_LABELS\n  $line  [branch: $branch_label]"
            fi
        done <<< "$STASH_LIST"

        if [ -n "$FOREIGN_LABELS" ]; then
            if [ "${FORCE_FOREIGN_STASH:-}" = "1" ]; then
                echo "⚠ FORCE_FOREIGN_STASH=1: allowing foreign-stash clear" >&2
                exit 0
            fi
            block "foreign-stash clear would destroy stashes from other worktrees" \
"Foreign stashes that would be destroyed:
$(printf '%b' "$FOREIGN_LABELS")

Active worktree branch: $SELF_BRANCH
Rule: foreign-stash — FORCE_FOREIGN_STASH=1 overrides this check."
        fi
    else
        # drop or pop — resolve target stash index.
        # Strip flags (-q, --quiet, etc.) to find the stash@{N} positional arg.
        STASH_REF=$(echo "$COMMAND" | grep -oE 'stash@\{[0-9]+\}' | head -1 || true)
        if [ -z "$STASH_REF" ]; then
            # Bare drop/pop resolves to stash@{0} (top of stack).
            # If stash list is empty, git itself will error — let it through.
            [ -z "$STASH_LIST" ] && exit 0
            STASH_REF="stash@{0}"
        fi

        # Extract the numeric index.
        STASH_IDX=$(echo "$STASH_REF" | grep -oE '[0-9]+')

        # Find the matching line in stash list.
        TARGET_LINE=$(echo "$STASH_LIST" | grep -E "^stash@\{${STASH_IDX}\}:" | head -1 || true)
        [ -z "$TARGET_LINE" ] && exit 0  # stash not found — let git report the error

        # Parse branch label from the stash line.
        STASH_BRANCH=$(echo "$TARGET_LINE" | sed -E 's/stash@\{[0-9]+\}: (WIP on |On )([^:]+):.*/\2/')

        if [ "$STASH_BRANCH" != "$SELF_BRANCH" ]; then
            if [ "${FORCE_FOREIGN_STASH:-}" = "1" ]; then
                echo "⚠ FORCE_FOREIGN_STASH=1: allowing foreign-stash $STASH_VERB" >&2
                exit 0
            fi
            block "foreign-stash $STASH_VERB — $STASH_REF belongs to branch '$STASH_BRANCH', not '$SELF_BRANCH'" \
"Target stash: $TARGET_LINE
Active worktree branch: $SELF_BRANCH
Rule: foreign-stash — FORCE_FOREIGN_STASH=1 overrides this check.

All stashes:
$(echo "$STASH_LIST" | sed 's/^/  /')"
        fi
    fi
    exit 0
fi

# ---------------------------------------------------------------------------
# Rule 2: branch -D / branch --delete --force — block if the branch is checked
# out in another worktree. (git already refuses natively; hook adds remediation.)
# ---------------------------------------------------------------------------
if echo "$COMMAND" | grep -qE 'git[[:space:]]+branch\b'; then
    # Detect deletion flags: -D, --delete --force, --force --delete, -Df, -fD, --force -D, -D --force
    HAS_DELETE_FORCE=0
    if echo "$COMMAND" | grep -qE 'git[[:space:]]+branch\b.*[[:space:]]-D(\b|[[:space:]])'; then
        HAS_DELETE_FORCE=1
    elif echo "$COMMAND" | grep -qE 'git[[:space:]]+branch\b.*-[a-zA-Z]*D[a-zA-Z]*'; then
        HAS_DELETE_FORCE=1
    elif echo "$COMMAND" | grep -qE 'git[[:space:]]+branch\b.*(--delete[[:space:]]+--force|--force[[:space:]]+--delete)'; then
        HAS_DELETE_FORCE=1
    fi

    if [ "$HAS_DELETE_FORCE" = "1" ]; then
        # Extract the branch name: last positional argument not starting with -.
        BRANCH_NAME=$(echo "$COMMAND" | tr ' ' '\n' | grep -v '^-' | tail -1 | tr -d '\n')
        # Remove 'branch' and 'git' tokens if they ended up as the "last" word.
        if [ "$BRANCH_NAME" = "branch" ] || [ "$BRANCH_NAME" = "git" ] || [ -z "$BRANCH_NAME" ]; then
            exit 0
        fi

        # Check if any non-self worktree has this branch checked out.
        WORKTREE_LIST=$(git -C "$CWD" worktree list --porcelain 2>/dev/null || true)
        HOLDING_WT=""
        CURRENT_WT=""
        while IFS= read -r line; do
            case "$line" in
                "worktree "*)
                    CURRENT_WT="${line#worktree }"
                    ;;
                "branch refs/heads/"*)
                    wt_branch="${line#branch refs/heads/}"
                    if [ "$wt_branch" = "$BRANCH_NAME" ]; then
                        # Is this a different worktree than self?
                        SELF_NORM=$(python3 -c "import os,sys; print(os.path.normpath(sys.argv[1]))" "$SELF")
                        WT_NORM=$(python3 -c "import os,sys; print(os.path.normpath(sys.argv[1]))" "$CURRENT_WT")
                        if [ "$WT_NORM" != "$SELF_NORM" ]; then
                            HOLDING_WT="$CURRENT_WT"
                        fi
                    fi
                    ;;
            esac
        done <<< "$WORKTREE_LIST"

        if [ -n "$HOLDING_WT" ]; then
            block "branch-delete-force on branch checked out in another worktree" \
"Branch '$BRANCH_NAME' is currently checked out at: $HOLDING_WT
Rule: worktree-branch-delete — cannot force-delete a branch held by another worktree.
Remediation: exit that worktree first, or switch it to a different branch."
        fi
    fi
    exit 0
fi

# ---------------------------------------------------------------------------
# Rule 3: reset --hard — WARN (do not block) if another worktree's HEAD would
# be orphaned (unreachable from the new ref). Exit 0 always.
# ---------------------------------------------------------------------------
if echo "$COMMAND" | grep -qE 'git[[:space:]]+reset[[:space:]]+--hard\b'; then
    # Extract the target ref (first positional after --hard, if any).
    NEW_REF=$(echo "$COMMAND" | grep -oE -- '--hard[[:space:]]+[^[:space:]]+' | awk '{print $2}' | head -1 || true)

    # Only do the ancestry check if we have a new ref and can resolve it.
    if [ -n "$NEW_REF" ]; then
        NEW_SHA=$(git -C "$CWD" rev-parse --verify "$NEW_REF" 2>/dev/null || true)
        if [ -n "$NEW_SHA" ]; then
            SELF_HEAD=$(git -C "$CWD" rev-parse HEAD 2>/dev/null || true)
            # Parse worktree list in bash; compare paths by string (SELF is already canonical
            # from rev-parse --show-toplevel which returns a realpath-like absolute path).
            WORKTREE_PORCELAIN=$(git -C "$CWD" worktree list --porcelain 2>/dev/null || true)
            AT_RISK=""
            CURRENT_WT=""
            CURRENT_HEAD=""
            while IFS= read -r wt_line; do
                case "$wt_line" in
                    "worktree "*)
                        CURRENT_WT="${wt_line#worktree }"
                        CURRENT_HEAD=""
                        ;;
                    "HEAD "*)
                        CURRENT_HEAD="${wt_line#HEAD }"
                        ;;
                    "")
                        if [ -n "$CURRENT_WT" ] && [ -n "$CURRENT_HEAD" ] && [ "$CURRENT_WT" != "$SELF" ]; then
                            if git -C "$CWD" merge-base --is-ancestor "$CURRENT_HEAD" "$SELF_HEAD" 2>/dev/null; then
                                if ! git -C "$CWD" merge-base --is-ancestor "$CURRENT_HEAD" "$NEW_SHA" 2>/dev/null; then
                                    AT_RISK="${AT_RISK}  worktree: $CURRENT_WT (HEAD: ${CURRENT_HEAD:0:12})
"
                                fi
                            fi
                        fi
                        CURRENT_WT=""
                        CURRENT_HEAD=""
                        ;;
                esac
            done <<< "$WORKTREE_PORCELAIN"
            # Handle last block with no trailing blank line
            if [ -n "$CURRENT_WT" ] && [ -n "$CURRENT_HEAD" ] && [ "$CURRENT_WT" != "$SELF" ]; then
                if git -C "$CWD" merge-base --is-ancestor "$CURRENT_HEAD" "$SELF_HEAD" 2>/dev/null; then
                    if ! git -C "$CWD" merge-base --is-ancestor "$CURRENT_HEAD" "$NEW_SHA" 2>/dev/null; then
                        AT_RISK="${AT_RISK}  worktree: $CURRENT_WT (HEAD: ${CURRENT_HEAD:0:12})
"
                    fi
                fi
            fi

            if [ -n "$AT_RISK" ]; then
                {
                    echo "⚠ WARNING (reset-hard-orphan-risk): git reset --hard may orphan commits reachable from another worktree's HEAD."
                    echo "Command: $COMMAND"
                    echo "At-risk worktrees (their HEAD would no longer be reachable from $NEW_REF):"
                    echo "$AT_RISK"
                    echo "Rule: reset-hard-warn — this is a warning only; the reset is proceeding."
                    echo "Tip: ensure the at-risk worktree has pushed its commits before resetting."
                } >&2
            fi
        fi
    fi
    exit 0
fi

# ---------------------------------------------------------------------------
# Rule 4: worktree remove --force — ALWAYS block.
# ---------------------------------------------------------------------------
if echo "$COMMAND" | grep -qE 'git[[:space:]]+worktree[[:space:]]+remove\b'; then
    if echo "$COMMAND" | grep -qE '(--force|-f\b)'; then
        block "worktree-force-remove — forced worktree removal without safety check" \
"Rule: worktree-force-remove
Remediation: drop --force and run 'git worktree remove <path>' instead.
If the worktree has uncommitted changes, git will warn and the user can decide.
If you truly need to remove a dirty worktree, ask the user to run the command directly."
    fi
    exit 0
fi

# ---------------------------------------------------------------------------
# Rule 5: push --force on a branch that is NOT the current worktree's branch.
# --force-with-lease always allowed (lease check is git's own safety net).
# ---------------------------------------------------------------------------
if echo "$COMMAND" | grep -qE 'git[[:space:]]+push\b'; then
    # If --force-with-lease is present (with or without =<expect>), always allow.
    if echo "$COMMAND" | grep -qE -- '--force-with-lease(=[^[:space:]]*)?'; then
        exit 0
    fi

    # Check for bare --force or standalone -f.
    HAS_FORCE=0
    if echo "$COMMAND" | grep -qE '(^|[[:space:]])(--force)([[:space:]]|$)'; then
        HAS_FORCE=1
    elif echo "$COMMAND" | grep -qE '(^|[[:space:]])-[a-zA-Z]*f[a-zA-Z]*([[:space:]]|$)'; then
        HAS_FORCE=1
    fi

    if [ "$HAS_FORCE" = "1" ]; then
        # Parse the destination ref from the refspec.
        # Forms: "git push origin branch", "git push origin src:dst", "git push origin HEAD:dst"
        # Find the first arg after the remote name that looks like a refspec.
        DEST_REF=""
        # Strip 'git push' prefix and flags, then find the refspec argument.
        REFSPEC=$(echo "$COMMAND" | sed -E 's/git[[:space:]]+push[[:space:]]+(--[a-zA-Z0-9_=-]+[[:space:]]*)*//' \
            | tr ' ' '\n' | grep -v '^-' | grep -v '^$' | tail -1 | tr -d '\n')

        if [ -n "$REFSPEC" ]; then
            # Handle src:dst form — destination is after the colon.
            if echo "$REFSPEC" | grep -q ':'; then
                DEST_REF="${REFSPEC##*:}"
            else
                DEST_REF="$REFSPEC"
            fi
        fi

        if [ -n "$DEST_REF" ] && [ "$DEST_REF" != "$SELF_BRANCH" ]; then
            block "force-push to non-current-worktree branch '$DEST_REF'" \
"Active worktree branch: $SELF_BRANCH
Destination ref:        $DEST_REF
Rule: force-without-lease — forcing to a different branch risks overwriting another agent's work.
Remediation: use --force-with-lease instead, which checks the remote ref hasn't changed."
        fi
    fi
    exit 0
fi

# Default: allow all other commands.
exit 0
