# gtm-mcp — unified GTM MCP gateway

A global **data gateway + action engine** for autonomous GTM agents, served as a
Render Web Service (Ohio) over the MCP **SSE** transport. It reads the Gen-3 R2
sink (the Lance system-of-record) two ways against one shared context:

- **Lance index pushdown** — point-lookups push their predicate straight into
  `lance.dataset(...).scanner(filter=...)`, so a load-bearing `BTREE` answers in
  sub-100 ms without a full scan.
- **Raw DuckDB ANSI SQL** — `execute_audience_query` runs arbitrary read-only SQL
  over the datasets as named relations for cross-layer segment building.

Built for multi-dataset extensibility: adding a dataset is one entry in
[`src/database.py`](src/database.py) `DATASETS` plus (optionally) a typed tool.

## Datasets (R2 active sink)

| Relation    | URI                                              | BTREE anchor(s)                       |
|-------------|--------------------------------------------------|---------------------------------------|
| `companies` | `s3://data-sink/active/companies/`               | `normalized_domain`                   |
| `people`    | `s3://data-sink/active/people/`                  | `normalized_domain`, `company_id`     |
| `awards`    | `s3://data-sink/active/contractor_award_summary/`| `recipient_uei`                       |

## Tools

**Audience** ([`src/tools/audience.py`](src/tools/audience.py))
- `search_company_by_domain(domain)` — BTREE pushdown on `companies.normalized_domain`
- `search_people_by_domain(domain)` — BTREE pushdown on `people.normalized_domain`
- `lookup_awards_by_uei(recipient_uei)` — BTREE pushdown on `awards.recipient_uei` (federal-spend resume)
- `search_company_by_name(name)` — canonical blocking-key match via `core.name_norm` (applied as a DuckDB SQL literal)
- `execute_audience_query(sql)` — arbitrary ANSI SQL over `companies`/`people`/`awards` (+ raw `s3://` Parquet); cross-layer joins; capped at 1000 rows

**DMaaS** ([`src/tools/dmaas.py`](src/tools/dmaas.py)) — Direct-Mail action wrappers, Lob-backed (**stubs**: validate + echo, return `not_implemented`)
- `create_direct_mail_campaign`, `send_postcard`, `send_letter`, `get_fulfillment_status`

## Runtime config (Render)

- **Runtime:** native Python 3 · **Region:** Ohio (`us-east-2`)
- **Build:** `pip install -r apps/gtm_mcp/requirements.txt`
- **Start:** `python -m apps.gtm_mcp.main` (run from the repo root; binds `0.0.0.0:$PORT`)
- **Env:** `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT` (required — same names as the `r2-credentials` Modal secret / `hq-x/prd` Doppler config). `HQX_MCP_BEARER_TOKEN` gates the MCP routes. `LOB_API_KEY` + `DMAAS_MCP_BEARER_TOKEN` for the DMaaS surface when wired.
- **Auth:** `/sse` and `/messages/` require `Authorization: Bearer $HQX_MCP_BEARER_TOKEN`. `/healthz` is open. If the token is unset, the server logs a warning and runs open (local dev only).
- **Public endpoint:** `GET /sse` (event stream) · `POST /messages/` (client→server) · `GET /healthz` (liveness/info)

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
