#!/usr/bin/env bash
# cleanup_stale_worktrees.sh — list (and optionally remove) stale git worktrees.
#
# A worktree is a REMOVAL CANDIDATE only if ALL of:
#   (a) its working tree is clean (no staged/unstaged changes, no untracked files),
#   (b) its branch is fully merged into origin/main, OR the branch is gone
#       (detached HEAD whose commit is an ancestor of origin/main also qualifies),
#   (c) no running process has its cwd inside the worktree.
#
# Usage:
#   scripts/cleanup_stale_worktrees.sh            # dry run: print candidates
#   scripts/cleanup_stale_worktrees.sh --execute  # remove candidates (git worktree remove)
#
# Run from anywhere inside the main checkout. Never touches the main worktree.

set -euo pipefail

EXECUTE=0
[[ "${1:-}" == "--execute" ]] && EXECUTE=1

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
git fetch -q origin main || echo "WARN: fetch failed; using cached origin/main" >&2
MAIN_SHA="$(git rev-parse origin/main)"

# (no early-exit awk: with pipefail, awk's exit would SIGPIPE the git side)
MAIN_WT="$(git worktree list --porcelain | awk '/^worktree /{if(!p){print $2;p=1}}')"

# One pass over all process cwds (lsof +D per-worktree is a recursive scan — too slow).
ALL_CWDS="$(lsof -a -d cwd -Fn 2>/dev/null | sed -n 's/^n//p' || true)"

candidates=()
skipped=0

while IFS= read -r wt_path; do
  [[ "$wt_path" == "$MAIN_WT" ]] && continue

  if [[ ! -d "$wt_path" ]]; then
    echo "CANDIDATE (dir missing — prunable): $wt_path"
    candidates+=("$wt_path")
    continue
  fi

  # (a) clean working tree
  if [[ -n "$(git -C "$wt_path" status --porcelain 2>/dev/null)" ]]; then
    echo "SKIP (dirty):        $wt_path"
    skipped=$((skipped + 1))
    continue
  fi

  # (b) merged into origin/main, or branch gone
  head_sha="$(git -C "$wt_path" rev-parse HEAD 2>/dev/null || true)"
  if [[ -z "$head_sha" ]]; then
    echo "SKIP (unreadable):   $wt_path"
    skipped=$((skipped + 1))
    continue
  fi
  branch="$(git -C "$wt_path" rev-parse --abbrev-ref HEAD 2>/dev/null || echo HEAD)"
  if ! git merge-base --is-ancestor "$head_sha" "$MAIN_SHA"; then
    echo "SKIP (unmerged $branch): $wt_path"
    skipped=$((skipped + 1))
    continue
  fi

  # (c) no process cwd inside the worktree
  if grep -qF "$wt_path" <<<"$ALL_CWDS"; then
    echo "SKIP (in use):       $wt_path"
    skipped=$((skipped + 1))
    continue
  fi

  echo "CANDIDATE (merged, clean, idle): $wt_path  [$branch @ ${head_sha:0:8}]"
  candidates+=("$wt_path")
done < <(git worktree list --porcelain | awk '/^worktree /{print $2}')

echo
echo "candidates: ${#candidates[@]}   skipped: $skipped"

if [[ $EXECUTE -eq 1 ]]; then
  for wt in "${candidates[@]}"; do
    echo "removing: $wt"
    git worktree remove "$wt" 2>/dev/null || git worktree remove --force "$wt" || echo "FAILED: $wt" >&2
  done
  git worktree prune
  echo "done."
else
  echo "dry run — re-run with --execute to remove the candidates above."
fi
