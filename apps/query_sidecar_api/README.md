# query_sidecar_api — read-only HTTP-SQL gateway over the query-sidecar artifact

Phase 3 of the query-sidecar plan. Serves ad-hoc / phrase-lane analytical SQL against the
sorted DuckDB artifact ([builder](../../pipelines/query_sidecar/build_query_sidecar.py) ·
[Phase 2 benchmark](../../docs/plans/QUERY_SIDECAR_PHASE2_BENCHMARK.md): every phrase.v2
family ≤134 ms, median ~35 ms, 14/14 parity).

**Read-only by construction.** The artifact is a derived, disposable copy of the serving
marts — Lance on R2 remains the untouched system of record. The DuckDB connection is opened
`read_only=True` (hard backstop); the endpoint additionally admits a single SELECT/WITH
statement only, deny-lists side-effecting keywords, caps rows via `fetchmany`, and
interrupts queries at `QUERY_TIMEOUT_S`.

## Endpoints

| Route | Auth | Purpose |
|---|---|---|
| `POST /api/v1/sql` `{"sql", "limit"?}` | Bearer | SELECT/WITH/DESCRIBE/SHOW; rows + `elapsed_ms` + artifact key |
| `GET /api/v1/tables` | Bearer | `_sidecar_manifest` — per-table provenance (dataset, tier, sort key, pinned Lance version, rows) |
| `POST /api/v1/refresh` | Bearer | Re-read `LATEST.json`, download, blue-green swap the connection |
| `GET /healthz` | — | Artifact key, built_at, table count |

## Boot contract (fail-closed)

1. `QUERY_SIDECAR_TOKEN` unset → refuse to boot.
2. Read `s3://data-sink/query-sidecar/LATEST.json` → download the versioned artifact to
   `DATA_DIR` (skip if already on disk with matching size) → open read-only.
3. After a rebuild (`modal run pipelines/query_sidecar/build_query_sidecar.py::run`),
   `POST /api/v1/refresh` picks up the new artifact without a redeploy.

## Deploy (Render Web Service, native Python — the gtm_mcp shape)

- Build: `pip install -r apps/query_sidecar_api/requirements.txt`
- Start: `python -m apps.query_sidecar_api.main`
- Disk: persistent disk mounted at `/var/data` (`DATA_DIR`), sized ≥ 2× the artifact
  (23.8 GiB as of v2) for blue-green refresh headroom.
- Env: `QUERY_SIDECAR_TOKEN` (bearer), `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`,
  `R2_ENDPOINT` (or `R2_ACCOUNT_ID`), optional `DUCKDB_MEMORY_LIMIT` (default 1500MB),
  `QUERY_TIMEOUT_S` (default 120).

## Example

```bash
curl -s -X POST "$URL/api/v1/sql" -H "Authorization: Bearer $TOKEN" -H "content-type: application/json" \
  -d '{"sql": "SELECT count(*) FROM gtm_txn_events_slim WHERE uei = '\''ABC123DEF456'\''"}'
```
