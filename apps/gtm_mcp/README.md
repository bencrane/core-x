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

- flat root → bare name: `companies`, `people`, `title_enrichment`, `firmographics_blitz`
- nested under a namespace → quoted path: `"usaspending/award_search"`, `"fmcsa/carrier"`, `"ca_ucc/filings"`

The three **indexed core datasets** that power the typed point-lookups carry a
load-bearing `BTREE` and are always resolvable (a defensive seed keeps them up even
if a scoped token can read objects but not list the bucket):

| Relation               | URI                                              | BTREE anchor(s)                   |
|------------------------|--------------------------------------------------|-----------------------------------|
| `companies`            | `s3://data-sink/active/companies/`               | `normalized_domain`               |
| `people`               | `s3://data-sink/active/people/`                  | `normalized_domain`, `company_id`, `person_linkedin_url` |
| `awards` (alias →      | `s3://data-sink/active/contractor_award_summary/`| `recipient_uei`                   |
| `contractor_award_summary`) |                                             |                                   |

**Entity-360 serving layer** (commit `e2b479c`, snapshot-partitioned, discovered — not seeded):

| Relation                              | Rows / grain            | BTREE                                                          | BITMAP                                                                                                   |
|---------------------------------------|-------------------------|---------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `provider_360/snapshot=YYYY-MM`       | 9.55M · 1/NPI           | `npi`, `practice_zip5`, `last_name`, `smallest_practice_group_enrlmt_id` | `entity_type_code`, `is_active`, `practice_state`, `primary_taxonomy_code`, `med_a1_provider_type`, `med_a1_latest_year`, `mips_final_score_year`, `is_independent_candidate` |
| `practice_group_360/snapshot=YYYY-MM` | 253.7K · 1/buyable-group | `group_enrlmt_id`, `org_name`                                 | `group_state`                                                                                            |

Surfaced by the **Provider 360** targeting tools below. Snapshot auto-resolves to the newest partition (override with `PROVIDER_360_SNAPSHOT` / `PRACTICE_GROUP_360_SNAPSHOT`).

**Audience-support datasets** (auto-discovered, not seeded):

- `people` carries `work_email` / `work_email_norm` / `verification_status` / `mv_resultcode` (13 cols total); `verification_status='verified'` (= `mv_resultcode` 1, BITMAP-indexed) is the mail-ready gate, and `enroll_leads_from_audience` sources the verified email from ops state keyed by `contact_id`.
- `companies` carries firmographics for segmentation — `industry`, `employee_size_band`, `company_type`, `hq_region` (each BITMAP-indexed), plus `employees_on_linkedin`, `founded_year`, `specialties`, `hq_city`/`hq_state`/`hq_continent`, and `uei` (federal-spend bridge key).
- `title_enrichment` — person-grain normalized seniority/function (`normalized_level`, `normalized_function`, `title_norm`, `normalized_job_title`, `confidence`), joined to `people` on the RAW LinkedIn URL: `people.person_linkedin_url = title_enrichment.person_linkedin_url`. Do NOT join on `person_linkedin_url_norm` — `people` has no normalized LinkedIn column and `_norm` has zero overlap.

## Tools

**Audience** ([`src/tools/audience.py`](src/tools/audience.py))
- `search_company_by_domain(domain)` — BTREE pushdown on `companies.normalized_domain`
- `search_people_by_domain(domain)` — BTREE pushdown on `people.normalized_domain`
- `lookup_awards_by_uei(recipient_uei)` — BTREE pushdown on `awards.recipient_uei` (federal-spend resume)
- `search_company_by_name(name)` — canonical blocking-key match via `core.name_norm` (applied as a DuckDB SQL literal)
- `execute_audience_query(sql)` — arbitrary ANSI SQL over the full discovered plane (+ raw `s3://` Parquet); **JIT registration** binds only the datasets the SQL references, so cross-layer joins open two manifests, not ~100; capped at 1000 rows

**Audience stamping & enrollment** ([`src/tools/corex.py`](src/tools/corex.py))
- `define_audience(name, source_sql, gtm_side=None, result_key='company_id', run=True)` — stamp a reusable audience (SQL selection + `{row_count, last_run_at}`). `result_key` is the 4th param ('contact_id' | 'company_id' | 'recipient_uei'); call with keyword args so it is not bound positionally to `gtm_side`.
- `define_audience_pair(initiative_id, demand_sql, supply_sql, thesis, demand_name, supply_name, demand_result_key='company_id', supply_result_key='recipient_uei', run=True)` — bind a demand + supply audience as an initiative thesis.
- `enroll_leads_from_audience(campaign_id, audience_id=None, limit=1000)` — re-run the stamped audience over the live lake, resolve each row to a contact (companies → most-senior person via the `people` graph), upsert `corex.contact` + `corex.lead`; verified email sourced from `ops.email_resolutions`. Idempotent.

**Batched point-lookups** ([`src/tools/batch_lookups.py`](src/tools/batch_lookups.py))
- `search_companies_by_domains([...])` / `search_people_by_domains([...])` — one IN(...) BTREE scan, de-duped and capped at 1000, results grouped by domain.

**Federal / GovCon / SAM** ([`src/tools/federal.py`](src/tools/federal.py), [`govcon.py`](src/tools/govcon.py), [`sub_capability.py`](src/tools/sub_capability.py), [`capability.py`](src/tools/capability.py), [`sam_entities.py`](src/tools/sam_entities.py))
- `federal_spend_by_agency` / `federal_spend_by_industry` / `federal_spend_by_state`, `federal_entities_by_filter` — federal-spend rollups over the `awards` plane.
- `govcon_companies_by_requirements`, `govcon_requirement_facets`, `search_govcon_scopes`, `search_subawardee_capabilities` — GovCon requirement matching.
- `lookup_sam_entity_by_uei` / `lookup_sam_entity_by_cage` / `lookup_sam_entities_by_ueis` / `lookup_sam_contacts_by_uei`, `search_sam_entities_by_naics`, `lookup_awards_by_uei` / `lookup_awards_by_ueis` — SAM/UEI/CAGE entity and award lookups.

**Catalog** ([`src/tools/catalog.py`](src/tools/catalog.py)) — two-level, mirroring the Postgres introspection
- `list_datasets()` — cheap "look around": every runtime-discovered dataset name + a column count (no column detail; one `active/catalog.json` GET, no Lance opens). The entry point.
- `describe_dataset(name)` — drill into one dataset's columns (with types), from the manifest or a single Lance-schema read; alias-aware (`awards` → `contractor_award_summary`)

**Operational Postgres** ([`src/tools/ops.py`](src/tools/ops.py)) — the structured read/write surface over the attached hq-x control plane (`hqx`)
- `save_campaign_audience(campaign_id, audience_name, source_query, parameters)` — upsert a campaign-audience tracking row into `ops.campaign_audiences` (parameterized psycopg upsert inside one transaction; the table self-bootstraps). The **only** write path into hq-x.
- `list_postgres_tables(schema_name="ops")` — cheap "look around": table names + column counts in a schema (no column detail), plus `available_schemas`. The progressive-disclosure entry point.
- `get_postgres_schema(schema_name, table_name=None)` — drill into **one** table's columns (`table_name` set, the preferred path after `list_postgres_tables`), or the whole schema's columns when omitted (heavy)

Introspection is two-level by design — `list_postgres_tables` → `get_postgres_schema(schema, table)` — so an agent pulls only the columns it needs instead of dumping every column of every table (for `ops`: ~28 KB full dump vs ~2.5 KB look-around vs ~0.5 KB single table).

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

**Provider 360 — entity-360 targeting** ([`src/tools/provider360.py`](src/tools/provider360.py)) — semantic GTM targeting over the `provider_360` / `practice_group_360` serving layer. Each query is driven from the selective side so the scalar indices fire as pushdown; non-indexed measures (money, growth, MIPS, group size) are thresholded on the already-pruned cohort. Every tool returns `elapsed_ms`, `index_path`, `cohort_size`, and `result_count`.

_provider_360 grain (1/NPI):_
- `get_provider_360_profile(npi)` — BTREE(`npi`) point lookup; the full entity-360 row (identity → 12-yr Medicare → panel risk → MIPS → Tier-1 rollups → dual-pole practice graph)
- `get_independent_platforms(specialty, state, min_size, max_size, rank_by, limit)` — solo / micro-practice physicians by specialty(+state), ranked by Medicare/Rx economics (the independent-platform sweet spot)
- `find_heavy_prescribers(specialty, state, single_practitioner, rank_by, limit)` — Part-D volume/cost leaders
- `find_growth_and_quality_triggers(specialty, state, min_growth_pct, min_mips, min_medicare_floor, limit)` — growth-trajectory × MIPS outbound triggers
- `find_clean_independent_practices(specialty, state, min_medicare_2024, max_manufacturer_payments, limit)` — high-Medicare, low-manufacturer-money ("non-compromised") surgical/specialty practices
- `find_dme_footprint(specialty, state, limit)` — DME supplier-economics leaders
- `get_dual_pole_leakage(state, specialty, max_secondary_group_size, limit)` — solo-PC physicians with a minor secondary group attachment

_practice_group_360 grain (1/buyable-group):_
- `find_acquisition_target_groups(specialty, state, min_size, max_size, min_panel_risk, rank_by, limit)` — buyable groups by size band / specialty / acuity / rolled-up economics
- `get_practice_group_roster(org_name | group_enrlmt_id)` — BTREE point lookup; full NPI roster + combined footprint
- `extract_practice_eins_for_matching(specialty, state, min_size, max_size)` — distinct billing-org matching anchors + `org_name` coverage gate

> **Schema note.** These tools speak the *real* serving-layer schema, not directive shorthand: specialty = `primary_taxonomy_code` (NUCC; friendly names map in `SPECIALTY_TAXONOMY`) — there is no `provider_type_desc`; "fan_in" = the dual-pole `smallest_/largest_practice_group_size`; "2024 service $" = `med_a1_latest_mdcr_pymt @ med_a1_latest_year=2024` (`svc_money_complete` is a build gate, not a metric). **No EIN/TIN exists in the substrate** — `extract_practice_eins_for_matching` returns `group_enrlmt_id` (PECOS) as the matching anchor. Materialized growth is 2019→latest (no discrete 2023→2024); DME is the supplier side (no "referring provider-years").

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
PORT=8765 doppler run -p core-x -c prd -- python -m apps.gtm_mcp.main

# drive it with the MCP Inspector CLI (send the bearer token)
TOKEN=$(doppler secrets get HQX_MCP_BEARER_TOKEN -p core-x -c prd --plain)
npx -y @modelcontextprotocol/inspector --cli http://127.0.0.1:8765/sse \
  --transport sse --header "Authorization: Bearer $TOKEN" --method tools/list
npx -y @modelcontextprotocol/inspector --cli http://127.0.0.1:8765/sse \
  --transport sse --header "Authorization: Bearer $TOKEN" --method tools/call \
  --tool-name search_company_by_domain --tool-arg domain=jpmorgan.com
```
