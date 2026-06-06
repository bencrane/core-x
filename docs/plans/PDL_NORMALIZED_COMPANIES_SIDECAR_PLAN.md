# `pdl_normalized_companies` — Sidecar Build Plan

Plan of record for a **derived normalized blocking-key sidecar** projected off the faithful PDL
firmographic SoR `pdl_companies`. Net-new Lance dataset + Modal worker. Immediately executable:
an agent follows §11 top to bottom. The SoR is **never mutated** — this is a read-only projection
that builds a new dataset.

**Scope:** the sidecar + its base→sidecar refresh trigger. **Not** name-sentinel suppression,
**not** the SAM/GLEIF↔PDL bridges that consume it, **not** codifying the existing `bridge_sam_pdl`
builder. Those are named where they intersect (§10, §13) but none is executed here.

> **Amendment status:** incorporates the adversarial review
> (`PDL_NORMALIZED_COMPANIES_SIDECAR_PLAN_REVIEW.md`) — B1 (linkedin_slug case-fold), B2
> (parity-against-source gate), B3 (`is_generic_domain` flag), N1 (userinfo strip), N2
> (industry/size/year tiebreak inline), N6 (ship the refresh trigger), plus the N5/N7 read-path
> and memory tightenings. This is the canonical, ship-ready version.

---

## 1. Objective

Materialize a thin, fully-indexed sidecar that answers, at index speed: **"which PDL company is
this name / domain / LinkedIn?"** It is the reusable right-side surface for every
`name|domain|linkedin → pdl_company_id` bridge (SAM first; GLEIF / company-spine later).

`pdl_companies` carries trained BTREEs on the **raw** `company_name` / `domain` / `linkedin_url`,
but raw-exact never matches across spines (case, corporate suffix, `&`/`AND`, `http(s)://`,
`www.`, trailing slash, path). `PDL_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md` §3.1 proved the macro
cannot recover it at read time: `name_norm(company_name)='X'` abandons the index and full-scans
all 35,446,771 rows — **67,646×** the indexed path. The canonical normalized forms are computed
**once, here**, and BTREE-indexed, so downstream bridges pay a point-lookup, not a 35.4M-row
recompute.

**Success =** the dataset exists in R2, 5× BTREE + 1× BITMAP indexed, 1:1 with the source PK,
blocking keys **byte-identical** to every other spine (same `core.name_norm` / `core.web_norm`
builders), round-trip `name → pdl_company_id → pdl_companies` row passing, and `pdl_companies`
proven untouched.

---

## 2. Architecture decision — sidecar, not columns on the SoR

Locked (survived adversarial re-litigation — review §A). The keys are pure 1:1 row-local
functions of `pdl_companies`, but they live in a separate projection dataset, not as columns on
the SoR. Four first-principles reasons — an executor must **not** "helpfully" inline these:

1. **SoR immutability is physics here.** This pipeline has no cheap in-place column add: both
   write paths (`overwrite`, `reindex`) publish via `_replace_r2_prefix`
   (`free_company_dataset.py:274-295`), which **wipes the R2 prefix and re-uploads ~7 GB**,
   destroying prior version history. `lance.add_columns` collapses into the same full rewrite
   under R2's multipart constraint (`free_company_dataset.py:24-33`). Inlining means rewriting a
   35.4M-row SoR that already works and that other consumers (`hmda_bulk.py`,
   `cms_open_payments/ingest.py`, `overture_maps/places.py`) read. A sidecar is a pure `create`
   — the SoR is byte-for-byte untouched.
2. **The norm rule is evolving policy; the firmographics are not.** `core.name_norm` /
   `core.web_norm` exist *because the rule changes and silently breaks joins*. Coupling a mutable
   key policy to the SoR schema forces a full base re-ingest + republish on every rule revision.
   Decoupled, a rule change rebuilds only this thin sidecar; the base is touched **only when the
   vendor file changes**.
3. **Derived-from-immutable is why `overwrite` is safe for the sidecar but not the base.**
   Immutability protects un-reconstructable data. This sidecar is a deterministic projection of
   the source — always rebuildable — so an `overwrite`-rebuild of *it* is idempotent and safe,
   no duplicate-append guard needed. The base is the artifact you cannot reconstruct.
4. **No defensive snapshot, zero blast radius.** Because nothing load-bearing is mutated, there
   is no SoR to snapshot before the operation and nothing a failed/heavy index build can corrupt.
   The high-cardinality BTREE external-sort runs against the sidecar in isolation.

The one real cost — a 1:1 sync obligation to the base snapshot — is closed in this plan: a
`source_version` stamp (§9), a parity gate (§11), an enforced refresh trigger (§9/N6), and a
fail-closed consumer assert (§10). A stale sidecar becomes both detectable **and** detected.

---

## 3. Inputs & output

| | Value |
|---|---|
| **Source (read-only)** | `s3://data-sink/active/pdl_companies/` — Lance v11, 35,446,771 rows, PK `pdl_company_id` (35,446,771 distinct, verified). Read via a single Lance scanner handle with **projection pushdown** — only 10 columns, never the full 16. |
| **Output** | `s3://data-sink/active/pdl_normalized_companies/` (net-new) |
| **Grain** | 1 row / `pdl_company_id` (pure passthrough — source PK already unique; no dedup) |
| **Est. rows** | 35,446,771 (the v11 count is the *informational* expected value; gates check parity against the live source, not this constant — §11) |
| **Lance version pin** | `data_storage_version="2.1"`, `max_rows_per_file=1048576`, `max_bytes_per_file=90 GiB` |

The worker reads **only** `pdl_companies`. It writes **only** the new prefix. It never issues a
write against the source.

**Scanned source columns (projection pushdown):** `pdl_company_id`, `company_name`, `domain`,
`linkedin_url`, `locality`, `region`, `country`, `industry`, `employee_size_range`, `year_founded`.

---

## 4. Output schema (exact)

| Column | Type | Derivation | Index |
|---|---|---|---|
| `pdl_company_id` | string | `nullif(trim(pdl_company_id),'')` | **BTREE** (join key back to SoR) |
| `company_name_norm` | string | `name_norm(company_name)` — canonical macro | **BTREE** (primary name blocking key) |
| `company_legal_base` | string | `legal_name_base(name_norm(company_name))` — peels LLC/INC/CORP/CO/LTD/PLC | **BTREE** (suffix-drift key) |
| `normalized_domain` | string | `core.web_norm.normalized_domain(domain)` — true canonical host, **not** blocklist-censored | **BTREE** (domain blocking key) |
| `linkedin_slug` | string | `core.web_norm.linkedin_slug(linkedin_url)` | **BTREE** (highest-precision key; 100% fill, ~unique) |
| `is_generic_domain` | bool | `core.web_norm.is_generic_domain(normalized_domain)` — true for shared webmail/social/marketplace hosts | **BITMAP** (the safe-join filter; §B3) |
| `company_name` | string | verbatim copy (`nullif(trim(...),'')`) | — (human/audit) |
| `locality` | string | passthrough | — (geo tiebreak, inline) |
| `region` | string | passthrough | — (geo tiebreak, inline) |
| `country` | string | passthrough | — (geo tiebreak, inline) |
| `industry` | string | passthrough (152-distinct) | — (sector tiebreak, inline) |
| `employee_size_range` | string | passthrough (8 buckets) | — (size tiebreak, inline) |
| `year_founded` | int32 | passthrough | — (as-of tiebreak, inline) |
| `source_version` | int64 | the `pdl_companies` Lance version projected (staleness stamp) | — (provenance) |
| `built_at` | timestamp | `now()` | — (provenance) |

`locality`/`region`/`country`/`industry`/`employee_size_range`/`year_founded` are **denormalized
onto the row on purpose**: a resolver blocks on the name and tiebreaks on geo/sector/size/age in
the *same* query — a hydration join mid-resolution would defeat the index. They are evaluated
against the small per-name candidate set, so they are not indexed. The only hop back to
`pdl_companies` is the final hydrate-by-PK (BTREE point lookup), unavoidable in any design.

**Index set: 5 BTREE + 1 BITMAP.** `is_generic_domain` is the low-card seek key (`WHERE NOT
is_generic_domain` is the mandatory consumer filter, §10) → BITMAP is correct.

---

## 5. Canonical normalizers — single source of truth

### 5.1 `core/name_norm.py` — already canonical, imported, NOT modified
`name_norm` and `legal_name_base` used as-is. Byte-identical to `sam_normalized_entities`,
`sos_normalized_master`, the credit spines. **Never re-inline the regex.** (Engine note: the
DuckDB-`trim` write vs DataFusion-`btrim` read distinction the diagnostic shows does **not** break
byte-parity — `name_norm`'s `[^A-Z0-9 ]+→''` strip removes NBSP/unicode whitespace *before* the
outer trim, leaving pure `[A-Z0-9 ]` both trim flavors handle identically. Verified in review §"already right".)

### 5.2 `core/web_norm.py` — NEW shared builders (the canonical web-identity rule)
Pure DuckDB-SQL *string* builders, zero imports, mirroring `core/name_norm.py`'s contract (safe
at module load, shippable into a Modal image via `add_local_python_source("core.web_norm")`). The
substrate owns this rule; existing copies reconcile to it (§10).

```python
# core/web_norm.py
from __future__ import annotations

def _bare_host(expr: str) -> str:
    """lower+trim → strip scheme → strip userinfo (user[:pw]@) → strip leading www. →
    drop path/port/query/fragment → strip leading/trailing dots. SQL expression; no validity gate.
    Userinfo is stripped BEFORE the path/port cut so 'bob:pw@host.com' is not truncated at the
    ':' (N1); '^[^/@]*@' only fires when the '@' precedes any '/', so it cannot eat a path
    segment like 'medium.com/@handle'."""
    return (
        "trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        "lower(trim(CAST(" + expr + " AS VARCHAR))),"
        " '^https?://', ''), '^[^/@]*@', ''),"          # strip scheme, then leading userinfo
        " '^www\\.', ''), '[/:?#].*$', ''), '.')"       # strip www, cut path/port, trim dots
    )

def normalized_domain(host_expr: str) -> str:
    """Canonical bare host gated to a plausible registrable domain; NULL otherwise.
    Pass a column already holding _bare_host(...) output so the regex chain runs once.
    Stores the TRUE host — webmail/social/marketplace filtering is is_generic_domain (below),
    NOT a null here (the substrate keeps audit value; the consumer filters)."""
    h = host_expr
    return (
        "nullif(CASE WHEN " + h + " LIKE '%.%' "
        "AND length(" + h + ") BETWEEN 4 AND 253 "
        "AND " + h + " NOT LIKE '% %' "
        "AND regexp_matches(" + h + ", '\\.[a-z]{2,}$') "   # ASCII TLD gate; hosts stored as-is, NOT punycoded (§ contract)
        "THEN " + h + " END, '')"
    )

def linkedin_slug(expr: str) -> str:
    """Bare LinkedIn company/school slug, lowercased; NULL if absent. Lowercase the INPUT
    so scheme/host/path case all fold BEFORE the anchor match (B1 — do not lower the output)."""
    return (
        "nullif(regexp_extract(lower(CAST(" + expr + " AS VARCHAR)),"
        " 'linkedin\\.com/(?:company|school)/([^/?#]+)', 1), '')"
    )

# Generic/shared-host classifier — the policy lives next to the rule it qualifies (B3).
# Superset of sam_fmcsa_domain_spine.py CONSUMER_BLOCK + the live top-shared-host tail
# (webmail + social + link-in-bio + video + B2B-marketplace). Reconcile to that union before build.
_GENERIC_DOMAINS = (
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com", "msn.com",
    "live.com", "protonmail.com", "ymail.com", "gmx.com",
    "facebook.com", "web.facebook.com", "m.facebook.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "youtu.be", "linkedin.com", "linktr.ee", "medium.com", "wordpress.com",
    "blogspot.com", "sites.google.com", "g.page", "behance.net", "wa.me", "t.me", "calendly.com",
    "indiamart.com", "yelp.com", "etsy.com", "amazon.com", "ebay.com", "tiktok.com", "pinterest.com",
)

def is_generic_domain(host_expr: str) -> str:
    """1 when the normalized host is a shared webmail/social/marketplace host that must not be a
    1:1 join key; 0 when a real company host; NULL when the host itself is NULL. Stored,
    BITMAP-indexed, so every consumer filters `WHERE NOT is_generic_domain` instead of
    re-deriving a blocklist (B3). Pass the GATED normalized_domain value, not the bare host."""
    items = "(" + ",".join("'" + s.replace("'", "''") + "'" for s in _GENERIC_DOMAINS) + ")"
    return (
        "CASE WHEN " + host_expr + " IS NULL THEN NULL "
        "WHEN " + host_expr + " IN " + items + " THEN true ELSE false END"
    )
```

**Deliberate divergence from the buried helper:** `sam_fmcsa_domain_spine.py::_is_domain_or_null_sql`
(`:227-235`) nulls a hardcoded blocklist inline. The canonical `normalized_domain` instead stores
the true host and exposes `is_generic_domain` as a separate flag — same policy, but it travels
*with* the data and is auditable, not censored. Pre-blocklist host bytes are identical, so the
join is sound the moment the consumer adopts the builder and filters `NOT is_generic_domain` (§10).

**Host-encoding contract:** hosts are stored **as-is (raw), not punycoded** (N4). A unicode SLD
with an ASCII TLD (`münchen.de`) is stored verbatim; a full-unicode host (`сбербанк.рф`) is NULLed
by the ASCII TLD gate. Any consumer that punycodes must store raw-unicode on its side too, or it
will silently miss. This is the explicit substrate contract — revisit when an IDN-aware consumer exists.

---

## 6. The transform (exact SQL)

Pure DuckDB, clean-room. A `norm` CTE computes each heavy regex **once** per row; the final SELECT
derives `company_legal_base`/`normalized_domain` from those columns and `is_generic_domain` from
the `normalized_domain` **alias** (DuckDB resolves SELECT-list aliases left-to-right — precedent
`pipelines/sos_normalized/normalize.py:389-390`). Builders are interpolated at SQL-build time.

```python
from core.name_norm import name_norm, legal_name_base
from core.web_norm import _bare_host, normalized_domain, linkedin_slug, is_generic_domain

def build_normalized_companies_sql(source_version: int) -> str:
    """Project pdl_companies → normalized blocking-key sidecar. Reads a `src` relation
    (the scanned mirror, 10 cols). 1 row/pdl_company_id passthrough."""
    return f"""
    WITH norm AS (
        SELECT
            pdl_company_id, company_name, locality, region, country,
            industry, employee_size_range, year_founded,
            {name_norm("company_name")}      AS _cnn,
            {_bare_host("domain")}            AS _host,
            {linkedin_slug("linkedin_url")}   AS _lslug
        FROM src
    )
    SELECT
        nullif(trim(pdl_company_id), '')           AS pdl_company_id,
        _cnn                                       AS company_name_norm,
        {legal_name_base("_cnn")}                  AS company_legal_base,
        {normalized_domain("_host")}               AS normalized_domain,
        {is_generic_domain("normalized_domain")}   AS is_generic_domain,   -- references the alias above (left-to-right)
        _lslug                                     AS linkedin_slug,
        nullif(trim(company_name), '')             AS company_name,
        nullif(trim(locality), '')                 AS locality,
        nullif(trim(region), '')                   AS region,
        nullif(trim(country), '')                  AS country,
        nullif(trim(industry), '')                 AS industry,
        nullif(trim(employee_size_range), '')      AS employee_size_range,
        year_founded                               AS year_founded,
        CAST({int(source_version)} AS BIGINT)      AS source_version,
        now()                                      AS built_at
    FROM norm
    WHERE nullif(trim(pdl_company_id), '') IS NOT NULL
    """
```

**Index plan:**
```python
PDL_NORM_BTREE_INDEXES = [
    "pdl_company_id", "company_name_norm", "company_legal_base",
    "normalized_domain", "linkedin_slug",
]
PDL_NORM_BITMAP_INDEXES = ["is_generic_domain"]
```

---

## 7. Worker file (structure — clone the skeleton, swap the deltas)

Create **`pipelines/pdl_companies/pdl_normalized_companies.py`** (Modal app
`pdl-normalized-companies`). Closest skeletons to clone:
- `pipelines/sam_gov/sam_normalized_entities.py` — derived-sidecar build/ops/dry-run shape.
- `pipelines/pdl_companies/free_company_dataset.py` — reuse `_r2_storage_options`, `_s3_client`,
  `_replace_r2_prefix`, `_record_run`, the local-build→boto3-publish lifecycle, and the
  `LANCE_BYPASS_SPILLING=true` image env verbatim.

Entrypoints: `initdb` · `run` (build) · `reindex` · `show_ledger`. The image declares
`add_local_python_source("core.name_norm", "core.web_norm")`.

---

## 8. Build mechanics & resource config (out-of-core)

Modal function: `memory=32768`, `cpu=8.0`, `ephemeral_disk=524288` (512 GB local NVMe for the
index external-sort), `timeout=60*90`, secrets `r2-credentials` + `hqx-postgres`.

1. **Open the source ONCE; read version + scan off the same handle** (N7 — never re-open, or an
   overwrite-during-build races the two reads):
   ```python
   ds = lance.dataset(SRC_URI, storage_options=so)
   source_version = ds.version                      # stamp = exactly what is read
   src_rows_total = ds.count_rows()
   reader = ds.scanner(columns=SCAN_COLS).to_reader()   # 10 cols only — never the full 7 GB
   con.register("src", reader)                       # DuckDB consumes the Lance reader (precedent: sam_fmcsa_domain_spine.py:295)
   ```
2. **DuckDB transform:** `memory_limit='24GB'`, `threads=8`,
   `temp_directory='/tmp/pdl_norm/duckdb_spill'` (on the ephemeral NVMe), `enable_progress_bar=false`.
   The `name_norm` regex over 35.4M is the heavy stage → spills to disk, not RAM.
3. **Materialize → Lance, LOCAL:** `table = con.sql(build_normalized_companies_sql(source_version)).to_arrow_table()`
   (~7.7 GB for the 15-col schema — see envelope below; the streaming
   `to_arrow_reader(1048576)` fallback on `MemoryError`/`OutOfMemoryException` is kept verbatim).
   Run the §11 parity asserts on `table` **before** writing. Then
   `lance.write_dataset(table, LOCAL, mode="overwrite", data_storage_version="2.1",
   max_rows_per_file=1048576, max_bytes_per_file=90*1024**3)`.
4. **Free, THEN index LOCAL** (N5): `del table; con.close()` immediately after `write_dataset` and
   **before** `_create_indexes`, so the ~7.7 GB Arrow buffer is released before the sort builds
   (do *not* clone `free_company_dataset.py:431-478`, which leaves it resident). Then
   `create_scalar_index(col, "BTREE", replace=True)` for the 5 keys and `"BITMAP"` for
   `is_generic_domain`. `LANCE_BYPASS_SPILLING=true` keeps the high-card string BTREEs
   (`company_name_norm` ~30M distinct; `linkedin_slug` ~unique) in-memory (lance#2650); the build
   re-reads columns from the local Lance file (peak ≈ ~2 GB sort working set, well under 24 GB).
   Index build is a disk-bound external sort → bounded by the 512 GB ephemeral disk, not RAM.
5. **Publish:** `_replace_r2_prefix(s3, "active/pdl_normalized_companies/", LOCAL)` — boto3
   uniform-part upload to the **new** prefix. Build-local-then-publish dodges R2's multipart
   part-size escalation on large `page_data.lance` index files. **No write ever targets
   `active/pdl_companies/`.**

**Arrow envelope (live per-column measurement, the actual sidecar schema):** `pdl_company_id 1.14
+ company_name_norm 0.85 + company_name 0.91 + company_legal_base ~0.80 + normalized_domain ~0.50
+ linkedin_slug ~0.40 + is_generic_domain ~0.04 + locality 0.42 + region 0.43 + country 0.44 +
industry ~0.32 + employee_size_range ~0.28 + year_founded(int32) ~0.14 + source_version(int64)
0.28 + built_at(ts) 0.28 ≈ ~7.7 GB`. Fits 32 GiB with the 24 GB DuckDB limit; peak co-residency
(Arrow + one sort working set) ≈ ~10 GB. No OOM; the streaming fallback remains the guard.

Why not `lance.add_columns` on the source: it collapses into the same full wipe-republish under
this tooling and mutates the SoR (§2). Rejected.

---

## 9. Idempotency, ops ledger, staleness stamp & refresh trigger

**Idempotent by construction:** the build is a deterministic `overwrite`-from-immutable-source.
Re-running reproduces the same dataset — no append, so no duplicate-row hazard and no
ledger-guarded dedup needed. Safe to retry.

**Ledger DDL** (`pipelines/pdl_companies/ops_pdl_normalized_runs.sql`; also created at runtime by
`initdb`):
```sql
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.pdl_normalized_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,   -- 'pdl_normalized_companies'
    dataset_uri     text        NOT NULL,   -- s3://data-sink/active/pdl_normalized_companies/
    source_dataset  text        NOT NULL,   -- s3://data-sink/active/pdl_companies/
    source_version  bigint,                 -- pdl_companies Lance version projected
    source_rows     bigint,                 -- live source count_rows() at scan time
    rows_processed  bigint,                 -- committed sidecar row count
    distinct_ids    bigint,                 -- COUNT(DISTINCT pdl_company_id)
    status          text        NOT NULL,   -- 'success' | 'error'
    error           text,
    started_at      timestamptz NOT NULL,
    completed_at    timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS pdl_normalized_runs_status_idx       ON ops.pdl_normalized_runs (status);
CREATE INDEX IF NOT EXISTS pdl_normalized_runs_completed_at_idx ON ops.pdl_normalized_runs (completed_at DESC);
CREATE INDEX IF NOT EXISTS pdl_normalized_runs_src_version_idx  ON ops.pdl_normalized_runs (source_version DESC);
```

**Staleness stamp:** `source_version` (column + ledger) records which `pdl_companies` version was
projected. Because the base is whole-snapshot manual-overwrite (one version == one complete
content state, no append churn — the diagnostic confirms v11 = 1 data write + 10 index commits), a
monotonic Lance version is sufficient lineage; a content hash adds nothing.

**Refresh trigger — wired in THIS PR (N6), not deferred.** A stamp is only lineage if something
*enforces* it. In `free_company_dataset.py::ingest_pdl_companies`, on the success path (after the
existing `_post_callback` at `:489`), fan out the sidecar rebuild so a new base snapshot cannot
leave the sidecar silently stale:
```python
# free_company_dataset.py — success path, after _post_callback(...)
try:
    modal.Function.from_name("pdl-normalized-companies", "ingest_normalized_companies").spawn()
except Exception as exc:  # fan-out is best-effort; base ingest already succeeded
    print(f"WARN: could not fan out pdl_normalized_companies rebuild: {exc}")
```
Belt-and-suspenders (§10): the consumer contract also makes the version check a hard precondition,
so even a missed fan-out fails closed rather than resolving against stale firmographics.

---

## 10. Downstream reconciliation (named — NOT executed here)

The substrate is authoritative; consumers conform to it, as separate work:
- `pipelines/resolution/sam_fmcsa_domain_spine.py` — replace private `_norm_host_sql` /
  `_is_domain_or_null_sql` with `from core.web_norm import …`; reconcile its `CONSUMER_BLOCK`
  into `core.web_norm._GENERIC_DOMAINS`. Rebuild `bridge_sam_fmcsa_domain` so its
  `normalized_domain` is byte-identical.
- Future PDL bridges (SAM/GLEIF/company-spine → PDL) join `<spine>.normalized_legal_name =
  pdl_normalized_companies.company_name_norm` (or `normalized_domain` / `linkedin_slug`) →
  `pdl_company_id` → hydrate `pdl_companies` by PK only when firmographics are needed.
- **Mandatory contract clauses for any consumer:**
  1. **Domain joins MUST add `AND NOT pdl_normalized_companies.is_generic_domain`** — the
     substrate flags shared webmail/social/marketplace hosts; the consumer MUST exclude them from
     1:1 domain blocking or eat an N×M cartesian (B3: ~340k rows land on shared hosts).
  2. **A bridge MUST assert `pdl_normalized_companies.source_version ==
     lance.dataset(pdl_companies).version` before resolving, and fail closed on mismatch** (the
     fail-safe behind the §9 trigger).
  3. Any future PDL consumer **must** import `core.name_norm` / `core.web_norm` — never re-inline.

---

## 11. Execution checklist (in order)

```
# 0. Branch off main in this worktree (never commit on a shared branch).

# 1. Author core/web_norm.py (§5.2, with the B1/N1/B3 fixes) +
#    pipelines/pdl_companies/pdl_normalized_companies.py (§6-§8) + ops_pdl_normalized_runs.sql (§9) +
#    the N6 fan-out in free_company_dataset.py success path. Import the canonical builders — do NOT re-inline.

# 2. Create the ops table:        modal run …::initdb

# 3. DRY-RUN gate (zero writes) — Gates 1-2 below. STOP on any miss.

# 4. BUILD (the single authorized write of this plan — to the NEW prefix only):
#      modal run …::run
#    → confirm publish to active/pdl_normalized_companies/; SoR active/pdl_companies/ untouched.

# 5. POST-BUILD VERIFY — Gates 3-8. All green.

# 6. SHIP — commit (core/web_norm.py + worker + ops DDL + free_company_dataset.py trigger + this plan),
#    push, PR, MERGE YOURSELF (squash), then pull into the operator main checkout:
#      git -C <main-worktree> fetch && git merge --ff-only origin/main && git log -1 --oneline
```

**Validation gates (no ship without every gate green):**
1. **Builder unit check** — `core.web_norm` fixtures (assert exact):
   - `https://www.Acme.com/x?y=1` → `acme.com`; `HTTP://Acme.COM.` → `acme.com`
   - `user@example.com` → `example.com`; `https://bob:pw@host.com:8443/x` → `host.com` (N1)
   - `192.168.1.1` → `normalized_domain` NULL (N8); `not a domain` → NULL
   - `https://WWW.LINKEDIN.COM/COMPANY/Foo-Bar/` → `linkedin_slug` `foo-bar` (B1);
     `linkedin.com/in/jane` → NULL
   - `is_generic_domain`: `instagram.com`/`indiamart.com` → true; `acme.com` → false; NULL → NULL
2. **Dry-run parity/fill** (DuckDB, zero writes; **parity against live source, not a constant** — B2):
   `committed == src_valid` where `src_valid = count(*) FROM src WHERE nullif(trim(pdl_company_id),'') IS NOT NULL`;
   `committed == count(DISTINCT pdl_company_id)`; `company_name_norm` fill ≈ `company_name` fill;
   `normalized_domain` fill ≤ `domain` fill and ≥ 60%; `linkedin_slug` fill ≈ `linkedin_url` fill;
   `is_generic_domain=true` rate sane (~1-2% of non-null domains); each key `approx_count_distinct` sane.
3. **Row/PK conservation** (committed, **parity not constant** — B2):
   `count_rows() == count(DISTINCT pdl_company_id) == src_valid` (source rows surviving the same
   null/empty-PK filter). Log `source_rows`, `src_valid`, `committed` to the ledger. The `35,446,771`
   figure is informational only.
4. **Manifest** — `list_indices()` returns the **6** indices of §6 (5 BTREE + 1 BITMAP).
5. **Trained truth** — `stats.index_stats()` each: `num_indexed_rows == committed`,
   `num_unindexed_rows == 0` (the FEC-trap gate).
6. **Pushdown proof** — `explain_plan` for `WHERE company_name_norm = '<sampled>'` emits
   `ScalarIndexQuery@company_name_norm_idx(BTree)`, `refine_filter=--`; `analyze_plan`
   `rows_scanned ≈ matched` (not 35.4M). Repeat `normalized_domain`, `linkedin_slug` (BTREE) and
   `WHERE NOT is_generic_domain` (BITMAP).
7. **Round-trip** — sample a live company → its keys → seek sidecar by `company_name_norm` →
   recover `pdl_company_id` → seek `pdl_companies` by `pdl_company_id` (BTREE) → identical row.
8. **SoR untouched** — `lance.dataset("active/pdl_companies/").version == 11` (unchanged); no
   object under `active/pdl_companies/` was written this run.

> The N6 fan-out (`pdl_companies::run` → `pdl_normalized_companies::run`) is wired in this same PR
> (§9). Verify it by reading the `free_company_dataset.py` success path; it fires end-to-end on the
> next base snapshot.

---

## 12. Rollback / blast radius

Net-new and additive. Rollback = delete the `active/pdl_normalized_companies/` prefix + revert the
code PR (`core/web_norm.py`, worker, ops DDL, the `free_company_dataset.py` fan-out). **`pdl_companies`
is never touched**, so there is nothing to restore and no pre-snapshot is needed — the structural
advantage over an inline column add. No consumer depends on the sidecar until the bridges are built
(§10), so rollback is non-breaking.

---

## 13. Out of scope (explicit)

- Name-sentinel suppression (nulling `x`/`test`/`closed`/…) — the substrate stores the true
  normalized value; sentinel policy is a separate consumer concern.
- The SAM/GLEIF/company-spine ↔ PDL bridges that consume these keys.
- Codifying the existing ad-hoc `bridge_sam_pdl` builder.
- **Executing** the §10 downstream migrations (named only).
- IDN/punycode normalization beyond the explicit raw-host contract (§5.2) — revisit when an
  IDN-aware consumer exists.
- Any change whatsoever to `pdl_companies` **data** (schema, indexes, fragments, version). The only
  edit to `free_company_dataset.py` is the additive §9 fan-out in its success path — it writes nothing
  new to the SoR.
