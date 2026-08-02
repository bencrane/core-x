# Sidecar Gap Report — 2026-08-01 — nppes-spine-recon

- **Date:** 2026-08-01
- **Sidecar artifact:** `query-sidecar/query_sidecar_20260726T231318Z.duckdb` (built 2026-07-26T23:13:18Z, 133 tables, instance srv-d97gbf57vvec73c5r2a0)
- **Session topic:** Locate the NPI/NPPES data spine (Lance vs sidecar), establish refresh cadence, latest snapshot, and MoM-delta feasibility.

---

## Entry 1 — Where does the NPI/NPPES spine live?

1. **Intent** — "Find the NPI data spine — is it a Lance dataset, a sidecar table, or what, and where."
2. **Why not the sidecar** — `missing table` (by doctrine): NPPES is a non-GTM healthcare domain; no `nppes*` table exists in the 133-table manifest and none is referenced in the agent guide. Datasets involved: `s3://data-sink/active/nppes/` (raw), `active/nppes_provider`, `active/nppes_provider_taxonomy`, `active/nppes_provider_identifier` (derived).
3. **What I ran instead** — repo grep over `docs/` + `pipelines/` for `npi|nppes`, then read `pipelines/nppes/ingest.py` and `pipelines/nppes/materialize_analytical.py` headers/prefix constants. No data-plane scan.
4. **Cost** — ~4 shell probes, seconds; zero rows scanned (code/doc reads only).
5. **Recurrence** — one-off per domain, but the *shape* ("where does domain X live") recurs whenever a non-GTM domain is touched.

## Entry 2 — When did NPPES last run / how fresh is the data?

1. **Intent** — "What was the last date this was run — when is the data from?" Plus follow-ups: monthly retention, MoM-delta feasibility, August availability.
2. **Why not the sidecar** — `missing table` + `freshness required`: run ledgers (`ops.nppes_runs`, `ops.nppes_analytical_runs`) live in HQX Postgres, not the sidecar; live operational status is out of sidecar scope by design.
3. **What I ran instead** — psycopg via doppler `core-x/prd` (`HQX_DB_URL_POOLED`): `SELECT feed, snapshot_month, status, recorded_at FROM ops.nppes_analytical_runs ORDER BY recorded_at DESC LIMIT 6`; same shape against `ops.nppes_runs`; one full-row read of the latest two analytical rows for error detail. Columns used: feed, snapshot_month, status, recorded_at, error, row counts.
4. **Cost** — 2 queries + 1 failed attempt on the dev config (auth mismatch); ~10 rows returned; seconds each.
5. **Recurrence** — recurring shape: "how fresh is feed X" is asked constantly, and is correctly served by ops ledgers, not the sidecar.

---

## Ranking (recurrence × cost)

Both entries are low-cost (seconds) and correctly routed off the sidecar. No entry here evidences sidecar demand:

1. Entry 2 — recurring shape, but freshness/ops status is **correctly-on-Postgres-ledger** territory.
2. Entry 1 — NPPES is **correctly-on-Lance** (non-GTM domain) per current doctrine.

Demand only — no proposed solutions.

## Session finding (operational, not a sidecar gap)

The NPPES derived analytical builds for 2026-06 and 2026-07 both failed the local acceptance gate (`G1, G3, G4, G5, G11`) pre-publish; the serving layer is pinned at snapshot 2026-05 while raw is current through 2026-07. Tracked outside this ledger.
