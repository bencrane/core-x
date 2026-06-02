# Firmographics (Blitz) → Gen-3 Lance — Materialization Plan (Directive 12)

**Status:** PLAN — for review. No worker script written, no R2 materialization performed.
**Target dataset:** `s3://data-sink/active/firmographics_blitz/` (standalone; NOT merged into `companies`).
**Source:** hq-x Postgres (shared Supabase project) · `ops.task_runs` · `result_payload` JSONB.
**Anchor:** `domain_norm` (normalized domain) — BTREE primary key.

Pattern is the GTM port (`pipelines/gtm/companies_people_bulk.py`, Directive 8): DuckDB does 100%
of the transform via a READ_ONLY Postgres ATTACH; Lance V2 is written straight to R2; no Iceberg,
no Polaris, no Supabase commit-lock.

---

## 1. Source Profile (live, 2026-06-02 — read-only)

Filter: `task_type IN ('blitz_firmo_direct','modal_hydrate_firmo_cascade') AND status='completed'`.

| Metric | Value |
|---|---|
| Completed rows (total) | **165,884** (14,645 `blitz_firmo_direct` + 151,239 `modal_hydrate_firmo_cascade`) |
| `result_payload` typeof | `object`, 100% |
| Top-level `domain` column non-null | 165,884 (**100%**) — the anchor source |
| Distinct `domain_norm` (after normalization) | **133,256** → dedup removes ~32,628 rows |
| Top-level `uei` non-null | 155,731 (93.9%) → **128,016 / 133,256 (96.1%) survive dedup** |

### 1a. The payload wraps a single Blitz `company{}` object under different keys

```
blitz_firmo_direct          : result_payload.blitz_payload.{found, company{…}}
modal_hydrate_firmo_cascade : result_payload.blitz_data.{found, company{…}}   (+ top-level uei, domain, linkedin_url, status)
```

Unify with `COALESCE(result_payload->'blitz_payload', result_payload->'blitz_data')`. For every
completed row, `found=true` and `company` is an object (165,884/165,884).

### 1b. `company{}` has a STABLE 14-key schema — every key present in 100% of rows

Value-level non-null fill (of 165,884), and the chosen target field:

| `company.<key>` | non-null | → target column | type | notes |
|---|---|---|---|---|
| `name` | 100.0% | `company_name` | string | |
| `domain` | 94.4% | *(not carried — anchor uses top-level `domain`, 100%)* | | |
| `website` | 95.3% | `website` | string | |
| `industry` | 93.2% | `industry` | string | LinkedIn taxonomy; ~filter field |
| `size` | 99.3% | `employee_size_band` | string | clean 8-bucket enum (below) |
| `employees_on_linkedin` | 93.9% | `employees_on_linkedin` | int64 | numeric headcount; max 774,506 |
| `type` | 80.1% | `company_type` | string | ~11 values (below) |
| `about` | 82.0% | `about` | string | description |
| `founded_year` | 64.8% raw → 64.6% valid | `founded_year` | int32 | **dirty** (max 20132); clamp `[1800,2026]` |
| `followers` | 97.8% | `followers` | int64 | LinkedIn followers; max 41.3M |
| `linkedin_url` | 100% | `linkedin_url` | string | company page |
| `linkedin_id` | ~100% | `linkedin_id` | int64 | max 113M |
| `specialties` | 53.9% non-empty | `specialties` | list&lt;string&gt; | JSON array → `VARCHAR[]` |
| `hq` (object) | 100% | flattened ↓ | | |

**No revenue field exists in the Blitz payload.** The directive's "revenue estimates" is not present
upstream. Size proxies: `employee_size_band`, `employees_on_linkedin`, `followers`.

### 1c. `company.hq{}` flatten

`hq.country` is **100% NULL** — dropped. Region/continent are clean enums.

| `hq.<key>` | → column | non-null | distribution |
|---|---|---|---|
| `city` | `hq_city` | high | free text |
| `state` | `hq_state` | ~88% | **full names** ("California"…); 687 distinct (dirty intl tail), concentrated in ~55 |
| `region` | `hq_region` | 92% | NORAM 147,806 · EMEA 3,645 · APAC 987 · LATAM 579 |
| `continent` | `hq_continent` | 92% | North America 148,202 · Europe 2,933 · Asia 1,090 · … |
| `country` | — | **0%** | dropped (all NULL) |

### 1d. Categorical distributions (filter fields)

- **`employee_size_band`** (8 + null): `1-10`, `11-50`, `51-200`, `201-500`, `501-1000`, `1001-5000`, `5001-10000`, `10001+`.
- **`company_type`** (~11): `Privately Held` (77.6k), `Nonprofit` (20.5k), `Public Company` (12.4k), `Educational`, `Government Agency`, `Self-Owned`, `Partnership`, `Self-Employed`, … (32.9k null).
- **`industry`** (~150–400): Construction, Non-profit Orgs, IT Services, Hospitals & Health Care, Software Development, Government Administration, … (long tail).

### 1e. Anchor normalization is required

Top-level `domain` noise: 11,147 with scheme, 6,978 `www.`, 11,187 path/`/`, 860 uppercase
(e.g. `https://acbyfcs.com`, `http://www.caughronandcompany.com`, `www.ashfordintl.com`). The fleet
`_normalized_domain()` macro (lower/trim → strip scheme → strip `www.` → strip path → strip trailing
dots → NULL if empty) resolves all of these. After it: 133,256 distinct, 0 null.

---

## 2. Target Schema (PyArrow — the exact contract)

24 columns. The DuckDB projection `TRY_CAST`s to these types; the worker then `table.cast(SCHEMA)`
to enforce field order, types, and nullability before write.

```python
import pyarrow as pa

FIRMOGRAPHICS_BLITZ_SCHEMA = pa.schema([
    # ── Resolution anchors ───────────────────────────────────────────────
    pa.field("domain_norm",           pa.string(),                  nullable=False),  # PK · BTREE
    pa.field("domain_raw",            pa.string(),                  nullable=True),   # pre-norm audit
    pa.field("uei",                   pa.string(),                  nullable=True),   # BTREE · govcon/SAM bridge
    # ── Firmographic core (Blitz company{}) ──────────────────────────────
    pa.field("company_name",          pa.string(),                  nullable=False),
    pa.field("website",               pa.string(),                  nullable=True),
    pa.field("industry",              pa.string(),                  nullable=True),   # BITMAP
    pa.field("employee_size_band",    pa.string(),                  nullable=True),   # BITMAP · "11-50"
    pa.field("employees_on_linkedin", pa.int64(),                   nullable=True),
    pa.field("company_type",          pa.string(),                  nullable=True),   # BITMAP
    pa.field("founded_year",          pa.int32(),                   nullable=True),   # clamped [1800,2026]
    pa.field("followers",             pa.int64(),                   nullable=True),
    pa.field("specialties",           pa.list_(pa.string()),        nullable=True),
    pa.field("about",                 pa.string(),                  nullable=True),
    # ── HQ geography (Blitz company.hq{}) ────────────────────────────────
    pa.field("hq_city",               pa.string(),                  nullable=True),
    pa.field("hq_state",              pa.string(),                  nullable=True),   # full name
    pa.field("hq_region",             pa.string(),                  nullable=True),   # BITMAP · NORAM/EMEA/…
    pa.field("hq_continent",          pa.string(),                  nullable=True),
    # ── LinkedIn identity ────────────────────────────────────────────────
    pa.field("linkedin_url",          pa.string(),                  nullable=True),
    pa.field("linkedin_id",           pa.int64(),                   nullable=True),
    # ── Provenance / lineage ─────────────────────────────────────────────
    pa.field("source_task_type",      pa.string(),                  nullable=False),  # which task won
    pa.field("source_run_id",         pa.string(),                  nullable=True),   # ops.task_runs.run_id
    pa.field("source_updated_at",     pa.timestamp("us", tz="UTC"), nullable=True),   # recency of winner
    pa.field("materialized_at",       pa.timestamp("us", tz="UTC"), nullable=False),  # snapshot build time
])
```

---

## 3. Indexing Strategy (canonical)

| Index | Column(s) | Type | Rationale |
|---|---|---|---|
| **Mandated anchor** | `domain_norm` | **BTREE** | Directive primary key; unique post-dedup; every GTM lookup. |
| Cross-dataset bridge | `uei` | **BTREE** | Joins firmographics → govcon `contractor_award_summary` (BTREE `recipient_uei`, #72) + SAM entity registrations. 96.1% coverage post-dedup. |
| GTM filter | `industry` | **BITMAP** | Named filter field; moderate cardinality. |
| GTM filter | `employee_size_band` | **BITMAP** | 8 values. |
| GTM filter | `company_type` | **BITMAP** | ~11 values. |
| GTM filter | `hq_region` | **BITMAP** | 4 values. |
| *(optional)* | `hq_state` | BITMAP | Heavy concentration in ~55 real values; defer unless state-filtering is a confirmed query pattern. |

BITMAP for low-cardinality categoricals follows the CO UCC precedent
(`pipelines/co_ucc/companions_bulk.py`: BITMAP on `action_type`/`record_status`/`state`). Built via
`lance.dataset(...).create_scalar_index(col, index_type="BTREE"|"BITMAP")` (idempotent, `replace=True`).

---

## 4. Key Design Decisions (opinionated; flagged for sign-off)

1. **Dedup to one row per `domain_norm` (most-recent-wins).** The directive names `domain_norm` the
   *primary key*; the source is not unique (165,884 rows → 133,256 domains; one domain has 356
   temporal re-hydrations). Rule: `ROW_NUMBER() OVER (PARTITION BY domain_norm ORDER BY updated_at
   DESC NULLS LAST, (task_type='modal_hydrate_firmo_cascade') DESC)` → keep `rn=1`. This diverges
   from the GTM port (which kept rows 1:1 because its PK was `company_id` and domain was a
   deliberately non-unique lookup). For a firmographic *reference* table, unique-per-domain is
   correct. Output: **~133,256 rows**.
2. **Anchor on the top-level `domain` column** (100% fill), not `company.domain` (94.4%). Carry
   `domain_raw` for audit.
3. **`founded_year` clamp `[1800,2026]`** → else NULL (drops 367 junk values incl. 20132).
4. **Carry `uei`** as a first-class bridge key (strategic composition with govcon/SAM).
5. **Drop `hq_country`** (100% NULL) and `company.domain` (redundant with anchor).
6. **Include `about`** (82%, bulky) — high GTM context value; cost negligible at 133k rows.

---

## 5. Architecture Blueprint (worker script — to build after approval)

**Module:** `pipelines/firmographics_blitz/materialize_blitz.py` · Modal app `firmographics-blitz`
**Sibling DDL:** `pipelines/firmographics_blitz/ops_firmographics_blitz_runs.sql`

**Image** (mirror GTM): `debian_slim(py3.12).pip_install("duckdb>=1.5,<2","pylance>=7","pyarrow>=17",
"psycopg[binary]>=3.2","requests>=2.32").env({"LANCE_BYPASS_SPILLING":"true"})`.

**Secrets:** `r2-credentials` (R2 write) + `hqx-postgres` (source READ **and** ops write — source and
control-plane are the same hq-x DB). No `dex-postgres`.

**`ingest_firmographics_blitz(trigger_callback_url=None)`** — single-shot (~133k rows = one fragment):

1. `so = _r2_storage_options()`; `dsn = _hqx_dsn()` (`HQX_DB_URL_POOLED` + `sslmode=require`).
2. `duckdb.connect(":memory:")` → `INSTALL postgres; LOAD postgres;` → `PRAGMA threads=4;` →
   `ATTACH '<dsn>' AS hqx (TYPE postgres, READ_ONLY);` (DSN single-quote-escaped, never logged).
3. Run the unified projection + dedup SQL (§5a) → `.to_arrow_table()`.
4. `table = table.cast(FIRMOGRAPHICS_BLITZ_SCHEMA)` — enforce the §2 contract.
5. `_write_lance(table, FIRMO_URI, so)` → `mode="overwrite", data_storage_version="2.1",
   max_rows_per_file=1048576, max_bytes_per_file=90*1024**3`.
6. `_create_indexes()` → BTREE(`domain_norm`,`uei`) + BITMAP(`industry`,`employee_size_band`,
   `company_type`,`hq_region`). Best-effort per index, logged loudly.
7. Terminal (success OR failure): `_record_run()` → `ops.firmographics_blitz_runs` (HQX, psycopg);
   `_post_callback()` → flat JSON to `trigger_callback_url` (no `{"data":…}` envelope, 3 retries).

**`local_entrypoints`:** `init_ops`, `run [--only]`, `reindex_only`, `verify_only` (mirror GTM).
**`verify()`:** assert `count_rows() == distinct domain_norm` (uniqueness), dump schema + committed
indexes, BTREE probe (`domain_norm='microsoft.com'` → 1 row) + `uei` probe.
**Dispatcher:** `modal deploy …` so the Universal Dispatcher (`core/modal_dispatcher.py`) resolves it.

**Legacy strip-out (explicit):** no Polaris catalog register, no Iceberg manifest, no Supabase commit
lock. Pure `lance.write_dataset` → R2 + native scalar indexes — identical clean-room to the GTM port.

### 5a. Unified projection + dedup SQL (DuckDB; the load-bearing transform)

```sql
WITH base AS (
  SELECT
    run_id, task_type, updated_at, domain AS domain_raw,
    nullif(trim(uei),'')                                                   AS uei,
    -- fleet _normalized_domain(): lower/trim → strip scheme/www/path/trailing-dots → NULL
    nullif(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
      lower(trim(domain)),'^https?://','','g'),'^www\.','','g'),'/.*$','','g'),'\.+$','','g'),'')
                                                                           AS domain_norm,
    COALESCE(result_payload->'blitz_payload', result_payload->'blitz_data') AS bp
  FROM hqx.ops.task_runs
  WHERE task_type IN ('blitz_firmo_direct','modal_hydrate_firmo_cascade')
    AND status = 'completed'
),
co AS (SELECT *, bp->'company' AS c FROM base
       WHERE json_type(bp->'company')='OBJECT' AND domain_norm IS NOT NULL),
projected AS (
  SELECT
    domain_norm, domain_raw, uei,
    nullif(trim(c->>'name'),'')                          AS company_name,
    nullif(trim(c->>'website'),'')                       AS website,
    nullif(trim(c->>'industry'),'')                      AS industry,
    nullif(trim(c->>'size'),'')                          AS employee_size_band,
    try_cast(c->>'employees_on_linkedin' AS BIGINT)      AS employees_on_linkedin,
    nullif(trim(c->>'type'),'')                          AS company_type,
    CASE WHEN try_cast(c->>'founded_year' AS INTEGER) BETWEEN 1800 AND 2026
         THEN try_cast(c->>'founded_year' AS INTEGER) END AS founded_year,
    try_cast(c->>'followers' AS BIGINT)                  AS followers,
    CASE WHEN json_type(c->'specialties')='ARRAY'
         THEN cast(c->'specialties' AS VARCHAR[]) END    AS specialties,
    nullif(trim(c->>'about'),'')                         AS about,
    nullif(trim(c->'hq'->>'city'),'')                    AS hq_city,
    nullif(trim(c->'hq'->>'state'),'')                   AS hq_state,
    nullif(trim(c->'hq'->>'region'),'')                  AS hq_region,
    nullif(trim(c->'hq'->>'continent'),'')               AS hq_continent,
    nullif(trim(c->>'linkedin_url'),'')                  AS linkedin_url,
    try_cast(c->>'linkedin_id' AS BIGINT)                AS linkedin_id,
    task_type AS source_task_type, run_id AS source_run_id, updated_at AS source_updated_at
  FROM co
),
ranked AS (
  SELECT *, row_number() OVER (
    PARTITION BY domain_norm
    ORDER BY source_updated_at DESC NULLS LAST,
             (source_task_type='modal_hydrate_firmo_cascade') DESC
  ) AS rn
  FROM projected
)
SELECT * EXCLUDE (rn), now() AS materialized_at FROM ranked WHERE rn = 1;
```

### 5b. `ops.firmographics_blitz_runs` (mirror `ops.companies_migration_runs`)

```sql
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.firmographics_blitz_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,        -- 'firmographics_blitz'
    source_db      text        NOT NULL,        -- 'hqx:ops.task_runs'
    datasets       jsonb       NOT NULL,        -- {"firmographics_blitz": 133256}
    rows_total     bigint      NOT NULL DEFAULT 0,
    rows_source    bigint      NOT NULL DEFAULT 0,  -- pre-dedup (165884)
    status         text        NOT NULL,        -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS firmographics_blitz_runs_feed_idx        ON ops.firmographics_blitz_runs (feed);
CREATE INDEX IF NOT EXISTS firmographics_blitz_runs_status_idx      ON ops.firmographics_blitz_runs (status);
CREATE INDEX IF NOT EXISTS firmographics_blitz_runs_recorded_at_idx ON ops.firmographics_blitz_runs (recorded_at DESC);
```

---

## 6. Open Items (sign-off before build)

1. **Dedup most-recent-wins** (→ ~133,256 rows) vs keep-all 1:1 (→ 165,884). *Recommend dedup* (directive: domain_norm = PK).
2. **BITMAP set** = {`industry`, `employee_size_band`, `company_type`, `hq_region`}; `hq_state` deferred. Confirm/trim.
3. **`founded_year` clamp** `[1800,2026]`. Confirm.
4. **Include `about`**. Confirm.
