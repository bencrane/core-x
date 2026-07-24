# core-x — agent substrate

The Gen-3 data/compute plane: extraction pipelines, the Lance system-of-record on R2, and the
read gateways that products consume. `ARCHITECTURE.md` is the long-form doctrine; this file is
the operating contract for agents.

## Gen-3 architecture (the invariant chain)

Modal extraction → ephemeral Parquet/CSV → DuckDB compute → **LanceDB SoR on R2**.

- **Raw is transport-only.** Payloads arrive via Modal orchestrators or manual in-session
  scripts; raw Parquet/CSV is streamed through the worker and never becomes a system of record.
  (`dex-raw-landing-zone` is retired Gen-2 — a few migration pipelines still read it; new
  ingests never write it.)
- **DuckDB performs 100% of transform** (projections, DISTINCTs, casts); Python is I/O only;
  Arrow is the only interchange.
- **Lance is the absolute SoR** at `s3://data-sink/active/` (`*_lance` datasets). No catalog
  layer: `lance.write_dataset` → R2 directly; datasets are addressed by R2 URI.
- **Every load-bearing resolution key gets a hard `BTREE` scalar index.**

## Analytical reads go to the query-sidecar FIRST

GTM analytical questions (entities, awards, transactions-by-recipient, expiring, teaming,
lookalikes, people/POCs) are served warm by the query-sidecar — milliseconds-to-seconds over
1.7B+ rows across 113 sorted tables (live truth: `/healthz` + `_sidecar_manifest`). Do NOT
scan Lance spines for questions it answers.
The map is `docs/reference/QUERY_SIDECAR_AGENT_GUIDE.md` — read it before composing SQL.
Lance remains the write-side SoR and the home of non-GTM domains and live-freshness reads.

## apps/ — the read-only-gateway doctrine

Gateways read the committed plane; pipelines materialize it. A gateway never writes a dataset.

| App | Role |
|---|---|
| `apps/catalyst_api` | The quiet Gen-3 read gateway (Railway, bearer-gated). 9 routers under `src/routers/` serve product reads: active-awards-query, entity-resolve, lender-book, list-lookalike, list-report, market-collections, market-query, market-spec, sub-dossier. Consumed cross-project by product BFFs. |
| `apps/edge_api` | High-churn automation/write plane — proposals, Documenso e-signature, bookings/deals, MCP mounts, managed-agent runs. Vendors the extracted hq-x app under `src/_hqx/`. |
| `apps/query_sidecar_api` | Warm read-only DuckDB HTTP-SQL over the sidecar artifact. Boots from `s3://data-sink/query-sidecar/LATEST.json`; every instance converges on LATEST via a background poll (`LATEST_POLL_S`, default 60s) — push refresh reaches only one instance, the poll reaches all. The Render service has `buildFilter: apps/query_sidecar_api/**`, so it redeploys only on its own changes. |
| `apps/gtm_mcp` | MCP surface (Render, Streamable HTTP `/mcp` + SSE) — Lance index pushdown + read-only DuckDB SQL for autonomous GTM agents. |

## Sidecar build doctrine

`pipelines/query_sidecar/build_query_sidecar.py` (read its docstring before touching it):

- **Blue-green LATEST pointer** — new versioned `.duckdb` file published first, pointer swap
  second, old files retained. Parity gate: every mart's DuckDB count must equal
  `ds.count_rows()` at the pinned Lance version; any mismatch fails before publish.
- **Join conditions are pure equality keys** — EXPLAIN-gate every new join in the fixture
  (assert no nested-loop join); fold probe-side gates into CASE-derived keys.
- **Fixture-test through the DISPATCH path**, never by calling SQL constants directly;
  `_preflight()` asserts every manifest flag has a dispatch branch.
- **Sidecar-gaps promotion cycle** — non-sidecar fallbacks are logged to `docs/sidecar_gaps/`
  and promoted via the `sidecar-gaps` skill (gate → adjacency sweep → parity-gated rebuild).
  Structural growth needs demand evidence; column adds ride any rebuild for free.
- Launch by SPAWNING on the deployed app (`modal deploy` first, then
  `modal.Function.from_name("query-sidecar","build").spawn(...)` — record the fc-id).
  NEVER `modal run …::run`, with or without `--detach`: the SYNC input dies ~90 s after
  client loss (8 ledger failures). Median ~32 min, observed 22–42, growing ~1.5 min/day.
  Full runbook: the `/sidecar-build` skill. Ledger: `ops.query_sidecar_runs`.

## harness/ — global Claude Code hooks

`harness/` hosts the global hook scripts wired from `~/.claude/settings.json` (they run in
EVERY repo, not just core-x). Main gate classes:

- **Destructive-bash block** (`hook-pretooluse-bash.sh`) — dangerous commands stopped pre-exec.
- **Cross-worktree git gate** (`hook-pretooluse-git-cross-worktree.sh`) — git ops scoped to the
  session's registered worktree.
- **Frozen-file validators** (`hook-pretooluse-frozen-validator.sh`, validator-freeze-gate,
  the `l4x` data-plane invariants) — protected files and R2/RW invariants enforced on edit.
- **Session/event logging** — session-start/stop/end hooks log to `~/Desktop/hq/sessions`.

Hook edits have blast radius across every session on this machine — treat as frozen unless
the task is explicitly about the harness.

## Testing

- `catalyst_api` carries the deep suite (23 test files under `apps/catalyst_api/tests/`).
- Run per-service: `python3 -m pytest apps/catalyst_api/tests -q` (same shape for
  `edge_api`, `gtm_mcp`).

## Git workflow

- **Full lifecycle, self-driven:** branch → commit → push → PR → `gh pr merge N --squash
  --delete-branch` after self-verification → pull the operator checkout `~/core-x` → verify
  `git log -1 --oneline`. Merged ≠ done until the operator checkout reflects the change.
- **Commit by explicit path** (`git add <paths>`, never `git add -A`) — parallel agent
  workstreams share this repo; staging sweeps pick up other agents' files.
- Open PRs against `main` directly; stacked PRs + squash merge drops later commits.
