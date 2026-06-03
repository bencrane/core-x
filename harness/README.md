# harness/ — Claude Code agent-global hooks

These are **agent-global** safety/lifecycle hooks fired by Claude Code on every
tool call and session event. They are **not** data-plane code — do **not** import
them into `pipelines/`, `core/`, or `src/`, and do not couple pipeline code to
them. They live here only because core-x is the operator's working repo; they
serve every session regardless of which repo it runs in.

## Wiring
Registered by absolute path in `~/.claude/settings.json` (untracked, local).
Each hook self-locates via `SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"`
and sources its siblings (`_lib-remediation.sh`, `_lib-active-cycle.sh`) +
runtime helpers (`spawn-session-summarizer.sh`, `scope-cycle-report.sh`,
`scope-precheck-predecessor.sh`) from this directory.

## Output plane
All hook output goes to the **HQ vault** (`~/Desktop/hq/...`): session
checkpoints, `_*.jsonl` ledgers, `scope-status/`, `reports/`, `raw/`. The vault
is stable and is **not** part of this repo.

## Relocation provenance (2026-06-03)
Lifted from `hq-all/scripts/` (operator no longer develops in hq-all). Repairs two
latent dead paths (the SessionEnd summarizer and the stale-cycle GC both pointed at
a `~/Desktop/hq/scripts/` location that never existed → now sibling-relative).

Intentionally **not** moved (hq-all data-engine-x tooling): `snapshot-*.py`,
`digest-snapshots.py`. The four `$SCRIPT_DIR/snapshot-*.py` / `digest-snapshots.py`
exec sites in `hook-posttooluse-bash.sh` are `[ -x ]`-guarded, so they no-op when
that tooling is absent (it is). Porting snapshot/git-event capture into core-x is a
separate task, as is the `/scope` skill rework (its `migration-checks` + `dex.sh`
coupling still points at hq-all).

The `l40/l42/l45/l46` guards filter on `*/apps/data-engine-x/*` — they protect
hq-all data-engine-x work (RisingWave/R2 failure modes) and are correctly inert for
core-x's DuckDB→Lance stack (no RisingWave). Left unchanged.
