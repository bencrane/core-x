# gtm-mcp — unified GTM MCP gateway

A global **data gateway + action engine** for autonomous GTM agents, served as a
Render Web Service (Ohio) over MCP — **Streamable HTTP at `/mcp`** (what the
Anthropic managed-agent connector requires) **and SSE at `/sse`**. It reads the
Gen-3 R2 sink (the Lance system-of-record) two ways against one shared context:

- **Lance index pushdown** — point-lookups push their predicate straight into
  `lance.dataset(...).scanner(filter=...)`, so a load-bearing `BTREE` answers in
  sub-100 ms without a full scan.
- **Raw DuckDB ANSI SQL** — `execute_audience_query` runs arbitrary read-only SQL
  over the datasets as named relations for cross-layer segment building.
- **Live hq-x Postgres, attached in-session** — the same DuckDB connection
  `ATTACH`es the hq-x control-plane Postgres as the `hqx` catalog, so an agent can
  JOIN Lance R2 datasets against operational `hqx.ops.*` state in one statement
  (e.g. subtract a suppression list), and manage campaign state through structured,
  audited write tools.

Built for the whole plane: the dataset registry is **discovered at runtime** by
listing the active sink — flat roots and the leaves nested under source namespaces
alike — so a pipeline that drops a new dataset shows up on the next restart with
no code change. Call `list_datasets` to inspect what's queryable.

## Datasets (R2 active sink)

**Auto-discovered** ([`src/database.py`](src/database.py) `discover_datasets`). At
first use the gateway lists `s3://data-sink/active/` and resolves every committed
Lance dataset (~100+) into an in-memory `name → uri` registry. A dataset's name is
its path relative to `active/`:

- flat root → bare name: `companies`, `people`, `firmographics_blitz`
- nested under a namespace → quoted path: `"usaspending/award_search"`, `"fmcsa/carrier"`, `"ca_ucc/filings"`

The three **indexed core datasets** that power the typed point-lookups carry a
load-bearing `BTREE` and are always resolvable (a defensive seed keeps them up even
if a scoped token can read objects but not list the bucket):

| Relation               | URI                                              | BTREE anchor(s)                   |
|------------------------|--------------------------------------------------|-----------------------------------|
| `companies`            | `s3://data-sink/active/companies/`               | `normalized_domain`               |
| `people`               | `s3://data-sink/active/people/`                  | `normalized_domain`, `company_id` |
| `awards` (alias →      | `s3://data-sink/active/contractor_award_summary/`| `recipient_uei`                   |
| `contractor_award_summary`) |                                             |                                   |

## Tools

**Audience** ([`src/tools/audience.py`](src/tools/audience.py))
- `search_company_by_domain(domain)` — BTREE pushdown on `companies.normalized_domain`
- `search_people_by_domain(domain)` — BTREE pushdown on `people.normalized_domain`
- `lookup_awards_by_uei(recipient_uei)` — BTREE pushdown on `awards.recipient_uei` (federal-spend resume)
- `search_company_by_name(name)` — canonical blocking-key match via `core.name_norm` (applied as a DuckDB SQL literal)
- `execute_audience_query(sql)` — arbitrary ANSI SQL over the full discovered plane (+ raw `s3://` Parquet); **JIT registration** binds only the datasets the SQL references, so cross-layer joins open two manifests, not ~100; capped at 1000 rows

**Catalog** ([`src/tools/catalog.py`](src/tools/catalog.py))
- `list_datasets()` — the runtime-discovered dataset names + columns (columns from the maintained `active/catalog.json` manifest, or read off the Lance schema for the edge datasets it omits); the schema an agent inspects before writing `execute_audience_query` SQL

**Operational Postgres** ([`src/tools/ops.py`](src/tools/ops.py)) — the structured read/write surface over the attached hq-x control plane (`hqx`)
- `save_campaign_audience(campaign_id, audience_name, source_query, parameters)` — upsert a campaign-audience tracking row into `ops.campaign_audiences` (parameterized psycopg upsert inside one transaction; the table self-bootstraps). The **only** write path into hq-x.
- `get_postgres_schema(schema_name)` — discover the tables + columns of an attached `hqx` schema (e.g. `ops`) via DuckDB's `information_schema`, so an agent can find operational state before writing hybrid-join SQL

### Hybrid Lance ⋈ Postgres

`execute_audience_query` resolves `hqx.<schema>.<table>` references against the
attached Postgres engine over the wire, while Lance datasets stay JIT-bound — so a
single query spans both planes:

```sql
SELECT c.* FROM companies c
LEFT JOIN hqx.ops.exclusions e ON c.normalized_domain = e.domain
WHERE e.domain IS NULL AND c.industry = 'Aerospace & Defense'
```

The JIT layer strips `hqx.*` references before Lance-name matching, so a
Postgres-qualified table never triggers a needless R2 manifest open.

### Safety (Directive 18 §4)

The `hqx` attach is read/write, but the agent-facing raw-SQL path is **read-only-gated**:
`execute_audience_query` accepts a single read statement only — `INSERT`/`UPDATE`/`DELETE`/`DROP`/`ALTER`/`TRUNCATE`/`ATTACH`/`COPY`/extension-load/transaction-control and `;`-chained statements are rejected (keywords inside string literals and comments don't trip the guard, and aren't a bypass either). All hq-x writes flow through `save_campaign_audience` — a parameterized upsert inside an explicit transaction boundary; `source_query`/`parameters` are stored as data, never executed.

**DMaaS** ([`src/tools/dmaas.py`](src/tools/dmaas.py)) — Direct-Mail action wrappers, Lob-backed (**stubs**: validate + echo, return `not_implemented`)
- `create_direct_mail_campaign`, `send_postcard`, `send_letter`, `get_fulfillment_status`

## Runtime config (Render)

- **Runtime:** native Python 3 · **Region:** Ohio (`us-east-2`)
- **Build:** `pip install -r apps/gtm_mcp/requirements.txt`
- **Start:** `python -m apps.gtm_mcp.main` (run from the repo root; binds `0.0.0.0:$PORT`)
- **Env:** `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT` (required — same names as the `r2-credentials` Modal secret / `hq-x/prd` Doppler config). `HQX_DB_URL_POOLED` attaches the hq-x control-plane Postgres for hybrid joins + the `ops.*` write tools (pooled/Supavisor DSN, TLS enforced; if unset, Lance/R2 tools still work and `hqx.*` queries fail with a clear message). `HQX_MCP_BEARER_TOKEN` gates the MCP routes. `LOB_API_KEY` + `DMAAS_MCP_BEARER_TOKEN` for the DMaaS surface when wired.
- **Auth:** `/mcp`, `/sse`, and `/messages/` require `Authorization: Bearer $HQX_MCP_BEARER_TOKEN`. `/healthz` is open. If the token is unset, the server logs a warning and runs open (local dev only).
- **Public endpoints:** `POST/GET /mcp` (Streamable HTTP — the transport managed agents require) · `GET /sse` + `POST /messages/` (SSE) · `GET /healthz` (liveness/info)

> The Python package is `gtm_mcp` (underscore) so `python -m apps.gtm_mcp.main`
> is a valid module path; the Render **service** is named `gtm-mcp`.

## Local run + verify

```bash
# serve with R2 creds + bearer token injected from Doppler
PORT=8765 doppler run -p hq-x -c prd -- python -m apps.gtm_mcp.main

# drive it with the MCP Inspector CLI (send the bearer token)
TOKEN=$(doppler secrets get HQX_MCP_BEARER_TOKEN -p hq-x -c prd --plain)
npx -y @modelcontextprotocol/inspector --cli http://127.0.0.1:8765/sse \
  --transport sse --header "Authorization: Bearer $TOKEN" --method tools/list
npx -y @modelcontextprotocol/inspector --cli http://127.0.0.1:8765/sse \
  --transport sse --header "Authorization: Bearer $TOKEN" --method tools/call \
  --tool-name search_company_by_domain --tool-arg domain=jpmorgan.com
```
