# NPPES Analytical Layer — Implementation Directive

**Owner of record:** Principal Data Engineer (this directive is the spec; execute against it verbatim).
**Repo:** `core-x` · **Doppler:** `core-x/prd` · **Mode:** BUILD — append-only, idempotent, per-snapshot. Mutates only NEW derived prefixes; the raw NPPES SoR is read-only and untouched.
**Descends from:** [`docs/nppes_structural_diagnostic.md`](nppes_structural_diagnostic.md) (#208, #211). Every decision below traces to a measured finding there — section references are inline as `(diag §N)`.
**Hardened by:** [`docs/nppes_analytical_plan_adversarial_review.md`](nppes_analytical_plan_adversarial_review.md). This revision folds in every verified finding from that adversarial pass — the `/mnt/nvme`→`/tmp` path fix (B1), explicit schema-metadata persistence (M1), the corrected join/pruning mechanics + recalibrated latency gates (M2/M3), the `is_active` stub-hardening (m1), `FM/MH/PW` (m2), exact NDV citations (m3), and partial-publish detection (n3). The model decisions were verified correct against live data and are unchanged.

---

## 0. Premise (the measured reality this remediates)

The raw NPPES SoR at `s3://data-sink/active/nppes/snapshot=YYYY-MM/` is **physically pristine but stored in raw CMS dissemination shape, not analytical shape** (diag §7). Confirmed, with numbers:

- Pushdown across the DuckDB↔Lance boundary **works** — `count(*) WHERE npi=X` ≈100 ms vs identical-shape unindexed `entity_type_code='2'` 1,243 ms; `SELECT * WHERE npi=X` returns 1 row in 1.9 s, not 120 s (diag §6.1). **The engine is not the problem.**
- It fails because the analytical axes are structurally hostile: **dates are `MM/DD/YYYY` strings** that don't sort chronologically (naive range filter returns 0 — silent garbage; diag §6.3); **specialty is shattered across `taxonomy_code_1..15`** with no indexable form (15-col OR scan = 6.65 s; primary-slot-only undercounts 3%+; diag §6.4); the analytical columns carry **no index** (scan floor ≈97 MiB/s; specialty×geo cell = 8.73 s; diag §6.2); **`npi` is unclustered** (batch joins fan out to all 10 fragments; diag §1.1).

**This directive builds the derived analytical serving layer the diagnostic mandated.** The raw monthly snapshot stays the immutable archive. GTM/market-mapping queries hit the derived layer.

**Definition of done (operational):** the three derived datasets below exist for `snapshot=2026-05`, are scalar-indexed, and **pass the §8 acceptance gate** — i.e. the exact queries that fail/scan on raw today now push down and return correct results (specialty filter sub-second with fragment pruning; date-range returns the correct 3,292,670; `npi` batch join prunes fragments). PR merged, operator checkout pulled, `git log -1` verified.

---

## 1. Architecture decisions (baked in — do not re-litigate)

**D1 — Three derived datasets, not one.** A flat 1-row-per-NPI core plus two unpivoted long children. Rationale: the GTM killers are (a) specialty filtering and (b) the wide-null sprawl; both are solved by normalizing the repeating groups out to long tables that carry an *indexable scalar* key.

| Dataset | Grain | Purpose |
|---|---|---|
| `nppes_provider` | 1 row / NPI | typed, cleaned, geo-join-ready provider core + denormalized primary specialty |
| `nppes_provider_taxonomy` | 1 row / (NPI, populated taxonomy slot) | the specialty long table — **the single change that makes market-mapping possible** |
| `nppes_provider_identifier` | 1 row / (NPI, populated other-identifier slot) | external-ID linkage (Medicaid/Medicare/etc.) — lower priority, independently shippable |

**D2 — Taxonomy as a LONG CHILD TABLE, explicitly NOT `list<struct>`.** A `list<struct>` keeps one dataset but **cannot carry a Lance scalar index on the specialty code** (scalar indices are per-scalar-column; indexing a list element is not supported), so it reintroduces the exact scan problem this cycle exists to kill. The long table gives a scalar `taxonomy_code` → `BITMAP` index → `WHERE taxonomy_code = X` is an indexed pushdown predicate (diag §6.1 proves pushdown then works), includes secondary specialties (every populated slot → a row), and makes specialty×geo a clean indexed `GROUP BY` after an `npi` join. This is the load-bearing decision of the cycle.

**D3 — Dates → `date32`, parsed once, in the bedrock.** `try_strptime(col,'%m/%d/%Y')::DATE`. Fixes the broken temporal axis (diag §6.3) so range filters and zone-map pruning work. Parse failures → NULL, counted into the ledger and gated (<0.01%). The analytical layer carries `date32` only; the raw string stays in the archive (no duplication).

**D4 — One provider table with `entity_type_code`, not a split.** Individuals and organizations share the table; org-only fields are NULL for individuals and vice-versa. `entity_type_code` gets a `BITMAP`. Splitting fragments every join. Add a decoded `entity_type` ('individual'|'organization') and a unified `provider_name` for the common "show the provider" path.

**D5 — Deactivated providers are KEPT, flagged, never dropped.** The 343,321-row (3.594%) deactivated-stub cohort (diag §3) gets a `nppes_provider` row with `is_active=false` and mostly-NULL descriptive fields (they have no taxonomy → no child rows). Derive `is_active` so every downstream GTM query filters cleanly instead of re-deriving deactivation logic.

**D6 — Geo-join-ready, not geocoded.** No lat/lon in NPPES. Produce clean `practice_state` (USPS-validated), `practice_zip5` (derived), `practice_city`, and a `BTREE` on `practice_address_line1` so the layer can join to a geocoded reference (overture/census ZIP centroid) downstream. Actual geocoding is out of scope (§12) but unblocked.

**D7 — Per-snapshot, pure function of one raw month.** Output partitions mirror the raw: `…/nppes_provider/snapshot=YYYY-MM/`, etc. Rebuildable, idempotent (overwrite the month prefix), append-history across months. The derived layer is never hand-edited; it is always re-derivable from the raw SoR.

**D8 — Read raw ONCE, stage local on NVMe, derive all three locally.** One R2 read of the projected raw into a local out-of-core DuckDB database on NVMe; then three local `CTAS` transforms. Minimizes egress (no triple R2 scan) and keeps the build out-of-core within the 32 GiB envelope. Local Lance stage → boto3 publish (the R2 multipart rule, diag §6.6, is mandatory — never write indices straight to R2).

**D9 — Drop the noise in-transform.** Exclude the dead column (`npi_deactivation_reason_code`, 100% null), the three `'<UNAVAIL>'` redaction sentinels (`employer_identification_number_ein`, `parent_organization_tin`, `provider_other_organization_name`), and per-row provenance (carry `source_snapshot_uri`/`source_member` as Arrow schema metadata, keep `snapshot_month` as the vintage key). (diag §3, §5.7–5.8)

---

## 2. Output schemas (exact)

### 2.1 `nppes_provider` — 1 row / NPI (~9,551,447 rows)

| Column | Type | Derivation | Index |
|---|---|---|---|
| `npi` | `string` | passthrough (PK; verified unique, diag §2) | **BTREE** |
| `entity_type_code` | `string` | passthrough (`'1'`/`'2'`/NULL) | **BITMAP** |
| `entity_type` | `string` | `'1'`→`individual`, `'2'`→`organization`, else NULL | — |
| `is_active` | `bool` | `deactivation_date IS NULL OR (reactivation_date IS NOT NULL AND reactivation_date >= deactivation_date)` | **BITMAP** |
| `provider_name` | `string` | org: legal name; individual: `concat_ws(', ', last, trim(concat_ws(' ', first, middle)))` | — |
| `organization_name` | `string` | `provider_organization_name_legal_business_name` | — |
| `last_name` | `string` | `provider_last_name_legal_name` | **BTREE** |
| `first_name` | `string` | `provider_first_name` | — |
| `middle_name` | `string` | passthrough | — |
| `name_prefix` / `name_suffix` / `credential` | `string` | passthrough | — |
| `sex_code` | `string` | passthrough (`F`/`M`/`X`/`U`/NULL) | — |
| `is_sole_proprietor` | `string` | passthrough | — |
| `is_organization_subpart` | `string` | passthrough | — |
| `primary_taxonomy_code` | `string` | `coalesce(code at the slot where switch_n='Y' …, code_1)` (see §3.3) | **BITMAP** |
| `practice_address_line1` | `string` | `provider_first_line_business_practice_location_address` | **BTREE** |
| `practice_address_line2` | `string` | second line | — |
| `practice_city` | `string` | passthrough | — |
| `practice_state` | `string` | USPS-clean (§3.2), else NULL | **BITMAP** |
| `practice_zip5` | `string` | `regexp_extract(zip,'^(\d{5})',1)`, else NULL | **BTREE** |
| `practice_zip` | `string` | passthrough (full, as-stored) | — |
| `practice_country` | `string` | `…country_code_if_outside_us` | — |
| `practice_phone` / `practice_fax` | `string` | passthrough | — |
| `mailing_city` | `string` | passthrough | — |
| `mailing_state` | `string` | USPS-clean | — |
| `mailing_zip5` | `string` | derived | — |
| `enumeration_date` | `date32` | `try_strptime(…,'%m/%d/%Y')` | **BTREE** |
| `enumeration_year` | `int16` | `year(enumeration_date)` | **BITMAP** |
| `last_update_date` | `date32` | parsed | **BTREE** |
| `deactivation_date` | `date32` | parsed | — |
| `reactivation_date` | `date32` | parsed | — |
| `certification_date` | `date32` | parsed | — |
| `authorized_official_last_name` | `string` | passthrough | — |
| `authorized_official_first_name` | `string` | passthrough | — |
| `authorized_official_title` | `string` | `authorized_official_title_or_position` | — |
| `parent_organization_lbn` | `string` | passthrough | — |
| `snapshot_month` | `string` | partition/vintage key | — |

**Sort:** `ORDER BY npi` (fragment pruning for batch resolution joins; diag §1.1). **`max_rows_per_file=1048576`, `data_storage_version='2.1'`.**

### 2.2 `nppes_provider_taxonomy` — 1 row / (NPI, slot) (~12M rows)

| Column | Type | Derivation | Index |
|---|---|---|---|
| `npi` | `string` | from parent row | **BTREE** |
| `taxonomy_rank` | `int8` | slot 1..15 | — |
| `taxonomy_code` | `string` | `healthcare_provider_taxonomy_code_<n>` (NOT NULL filter) | **BITMAP** |
| `is_primary` | `bool` | `healthcare_provider_primary_taxonomy_switch_<n> = 'Y'` | **BITMAP** |
| `license_number` | `string` | `provider_license_number_<n>` | — |
| `license_state` | `string` | `provider_license_number_state_code_<n>`, USPS-clean | **BITMAP** |
| `taxonomy_group` | `string` | `healthcare_provider_taxonomy_group_<n>` | — |
| `snapshot_month` | `string` | vintage | — |

**Sort:** `ORDER BY taxonomy_code, npi` — clusters fragments by the hot predicate so `WHERE taxonomy_code=X` prunes whole `.lance` files (the direct fix for the 6.65 s scan, diag §6.4), with `npi` locally sorted for the join back to `nppes_provider`.

### 2.3 `nppes_provider_identifier` — 1 row / (NPI, slot) (~2.7M rows)

| Column | Type | Derivation | Index |
|---|---|---|---|
| `npi` | `string` | from parent | **BTREE** |
| `identifier_rank` | `int8` | slot 1..50 | — |
| `identifier_value` | `string` | `other_provider_identifier_<n>` (NOT NULL filter) | **BTREE** |
| `identifier_type_code` | `string` | `other_provider_identifier_type_code_<n>` | **BITMAP** |
| `identifier_state` | `string` | `other_provider_identifier_state_<n>`, USPS-clean | **BITMAP** |
| `identifier_issuer` | `string` | `other_provider_identifier_issuer_<n>` | — |
| `snapshot_month` | `string` | vintage | — |

**Sort:** `ORDER BY npi`.

---

## 3. Transform specification

### 3.1 Stage projected raw once (D8, out-of-core)

```python
# Modal mounts ephemeral disk at the container root; the fleet spills under /tmp
# (ingest.py:130 = "/tmp/nppes"). There is NO /mnt/nvme mount — never hardcode it.
SCRATCH_DIR = "/tmp/nppes_analytical"
SPILL_DIR   = os.path.join(SCRATCH_DIR, "duck_spill")
os.makedirs(SPILL_DIR, exist_ok=True)

con = duckdb.connect(os.path.join(SCRATCH_DIR, "nppes_build.duckdb"))   # out-of-core, on the 512 GiB ephemeral disk
con.execute("PRAGMA threads=8;")
con.execute("SET memory_limit='20GB';")
con.execute(f"SET temp_directory='{SPILL_DIR}';")
con.execute("SET max_temp_directory_size='128GB';")
con.register("raw", lance.dataset(RAW_URI, storage_options=so))   # snapshot=2026-05
# single R2 read → local out-of-core table; project all source cols EXCEPT the §D9 noise
con.execute(f"CREATE TABLE rawstage AS SELECT {PROJECTED_COLS} FROM raw")
```

> **B1 (was BLOCKER):** the `@app.function` keeps `ephemeral_disk=524288` (the 512 GiB fleet floor; the staging `.duckdb` + ~35 GiB sort spill live on that root volume). All local Lance stages go under `SCRATCH_DIR` too — never `/mnt/nvme`, which does not exist in the Modal runtime and is used by zero pipelines.

`PROJECTED_COLS` = every source column needed by §2 (npi, entity/name/address/mailing/date/flag/authorized-official/parent-org cols + `*_taxonomy_code_1..15`, `*_primary_taxonomy_switch_1..15`, `provider_license_number_1..15`, `provider_license_number_state_code_1..15`, `*_taxonomy_group_1..15`, `other_provider_identifier_1..50` + `_type_code_/_state_/_issuer_`). Exclude `npi_deactivation_reason_code`, the three `'<UNAVAIL>'` columns, `source_file/source_member/ingested_at`.

### 3.2 Cleaning helpers (DuckDB SQL macros)

```sql
-- USPS-valid 2-letter state, else NULL. Set = 50 states + DC + territories + military +
-- the Compact-of-Free-Association states (FM/MH/PW — present in the data, valid USPS; m2).
-- AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH
-- NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC AS GU MP PR VI UM
-- FM MH PW AA AE AP   (63 codes total; everything else — BC/ON/QC/MX/UK/… — is foreign → NULL)
CREATE MACRO clean_state(s) AS (
  CASE WHEN list_contains(['AL','AK',/* …full set incl FM,MH,PW… */'AA','AE','AP'], upper(trim(s)))
       THEN upper(trim(s)) ELSE NULL END);
CREATE MACRO zip5(z) AS nullif(regexp_extract(z, '^\s*(\d{5})', 1), '');
CREATE MACRO d(x) AS try_strptime(x, '%m/%d/%Y')::DATE;   -- MM/DD/YYYY -> date32, NULL on fail
```

### 3.3 `nppes_provider` derivation (key expressions)

```sql
CREATE TABLE provider AS
SELECT
  npi,
  entity_type_code,
  CASE entity_type_code WHEN '1' THEN 'individual' WHEN '2' THEN 'organization' END AS entity_type,
  -- m1: the `entity_type_code IS NOT NULL` clause makes this robust to a future
  -- re-deactivated stub (deact→react→deact, latest react>=deact but descriptive fields
  -- cleared). On snapshot=2026-05 it is exactly the 343,321 deactivation-stub cohort.
  (d(npi_deactivation_date) IS NULL
    OR (d(npi_reactivation_date) IS NOT NULL
        AND d(npi_reactivation_date) >= d(npi_deactivation_date)
        AND entity_type_code IS NOT NULL)) AS is_active,
  CASE WHEN entity_type_code='2' THEN provider_organization_name_legal_business_name
       WHEN entity_type_code='1' THEN concat_ws(', ', provider_last_name_legal_name,
                                       trim(concat_ws(' ', provider_first_name, provider_middle_name)))
       ELSE coalesce(provider_organization_name_legal_business_name, provider_last_name_legal_name) END AS provider_name,
  provider_organization_name_legal_business_name AS organization_name,
  provider_last_name_legal_name AS last_name, provider_first_name AS first_name, provider_middle_name AS middle_name,
  provider_name_prefix_text AS name_prefix, provider_name_suffix_text AS name_suffix, provider_credential_text AS credential,
  provider_sex_code AS sex_code, is_sole_proprietor, is_organization_subpart,
  -- primary specialty: the code at the slot whose switch='Y', else slot-1 fallback
  coalesce(
    CASE WHEN healthcare_provider_primary_taxonomy_switch_1='Y' THEN healthcare_provider_taxonomy_code_1 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_2='Y' THEN healthcare_provider_taxonomy_code_2 END,
    /* … slots 3..15 … */
    healthcare_provider_taxonomy_code_1
  ) AS primary_taxonomy_code,
  provider_first_line_business_practice_location_address AS practice_address_line1,
  provider_second_line_business_practice_location_address AS practice_address_line2,
  provider_business_practice_location_address_city_name AS practice_city,
  clean_state(provider_business_practice_location_address_state_name) AS practice_state,
  zip5(provider_business_practice_location_address_postal_code) AS practice_zip5,
  provider_business_practice_location_address_postal_code AS practice_zip,
  provider_business_practice_location_address_country_code_if_outside_us AS practice_country,
  provider_business_practice_location_address_telephone_number AS practice_phone,
  provider_business_practice_location_address_fax_number AS practice_fax,
  provider_business_mailing_address_city_name AS mailing_city,
  clean_state(provider_business_mailing_address_state_name) AS mailing_state,
  zip5(provider_business_mailing_address_postal_code) AS mailing_zip5,
  d(provider_enumeration_date) AS enumeration_date,
  year(d(provider_enumeration_date))::SMALLINT AS enumeration_year,
  d(last_update_date) AS last_update_date,
  d(npi_deactivation_date) AS deactivation_date,
  d(npi_reactivation_date) AS reactivation_date,
  d(certification_date) AS certification_date,
  authorized_official_last_name, authorized_official_first_name,
  authorized_official_title_or_position AS authorized_official_title,
  parent_organization_lbn,
  snapshot_month
FROM rawstage
ORDER BY npi;
```

### 3.4 Unpivot generators (taxonomy = 15 slots, identifier = 50 slots)

Generate a `UNION ALL` over slots in Python (deterministic, NULL-filtered, carries slot rank + `is_primary` from the parallel switch). Do **not** use a blind `UNPIVOT` — the parallel switch/license/group columns must travel with their code.

```python
def taxonomy_union(n=15):
    parts = []
    for i in range(1, n+1):
        parts.append(f"""
          SELECT npi, {i}::TINYINT AS taxonomy_rank,
                 healthcare_provider_taxonomy_code_{i} AS taxonomy_code,
                 (healthcare_provider_primary_taxonomy_switch_{i} = 'Y') AS is_primary,
                 provider_license_number_{i} AS license_number,
                 clean_state(provider_license_number_state_code_{i}) AS license_state,
                 healthcare_provider_taxonomy_group_{i} AS taxonomy_group,
                 snapshot_month
          FROM rawstage WHERE healthcare_provider_taxonomy_code_{i} IS NOT NULL""")
    return "CREATE TABLE taxonomy AS " + " UNION ALL ".join(parts) + " ORDER BY taxonomy_code, npi;"

def identifier_union(n=50):
    parts = []
    for i in range(1, n+1):
        parts.append(f"""
          SELECT npi, {i}::TINYINT AS identifier_rank,
                 other_provider_identifier_{i} AS identifier_value,
                 other_provider_identifier_type_code_{i} AS identifier_type_code,
                 clean_state(other_provider_identifier_state_{i}) AS identifier_state,
                 other_provider_identifier_issuer_{i} AS identifier_issuer,
                 snapshot_month
          FROM rawstage WHERE other_provider_identifier_{i} IS NOT NULL""")
    return "CREATE TABLE identifier AS " + " UNION ALL ".join(parts) + " ORDER BY npi;"
```

### 3.5 Write each table → local Lance stage → index → publish

For each of `provider`, `taxonomy`, `identifier`: stream the sorted DuckDB table to a local Lance dataset via `to_arrow_reader(131072)` (bounded RSS), **persist provenance metadata explicitly (M1, below)**, build the table's scalar indices **locally**, then `boto3` publish (wipe + uniform-part upload) to `…/<name>/snapshot=YYYY-MM/`. Identical transport pattern to `pipelines/nppes/ingest.py` (the R2 multipart-safe path).

> **M1 — schema-metadata provenance must be set explicitly (D9).** A DuckDB `to_arrow_reader` schema carries **no** key-value metadata, so passing it to `write_dataset` persists none — provenance vanishes silently with no error (verified, pylance 7.0.0). After the streaming write and **before** index/publish, set it on the committed dataset (the non-deprecated API; `replace_schema_metadata` is deprecated in pylance 7.0.0):
> ```python
> ds = lance.dataset(local_stage)
> ds.update_schema_metadata({
>     "source_snapshot_uri": RAW_URI,
>     "source_member":       source_member,
>     "pipeline":            "materialize_analytical",
>     "snapshot_month":      snapshot_month,
> }, replace=True)   # verified to round-trip through write + index + boto3 publish + reopen
> ```
> The `verify` gate (§8) reopens each published dataset and asserts `schema.metadata` contains a non-empty `source_snapshot_uri`. The `ops.nppes_analytical_runs` ledger row is the secondary provenance carrier.

---

## 4. Indexing & clustering (per table) — `INDEX_PLAN`

```python
INDEX_PLAN = {
  "nppes_provider": {
    "btree":  ["npi","last_name","practice_address_line1","practice_zip5","enumeration_date","last_update_date"],
    "bitmap": ["entity_type_code","is_active","primary_taxonomy_code","practice_state","enumeration_year"],
  },
  "nppes_provider_taxonomy": {
    "btree":  ["npi"],
    "bitmap": ["taxonomy_code","is_primary","license_state"],
  },
  "nppes_provider_identifier": {
    "btree":  ["npi","identifier_value"],
    "bitmap": ["identifier_type_code","identifier_state"],
  },
}
```

Rationale anchored to cardinality (exact, measured on the live raw — m3): BTREE for high-card resolution/range keys (npi, names, address, zip5, dates); BITMAP for the low/medium-card categorical filters (`entity_type_code` NDV **2**, `taxonomy_code` NDV **873** [exact long-form; the diagnostic's ~1,104 was the +14%-biased HLL slot-1 estimate], cleaned `practice_state` NDV **59**, `identifier_type_code` NDV **2**, `enumeration_year` ~22 [2005–2026]). `is_active`/`is_primary` are boolean → BITMAP. All remain squarely in BITMAP territory. Clustering sorts (§2) make the hot predicate prune fragments.

---

## 5. Out-of-core, memory, I/O (the §4/§6 learnings, applied)

- **Envelope:** Modal `memory=32768, cpu=8.0, ephemeral_disk=524288` (same as the raw ingest). The derived tables are *smaller* than raw (null sprawl dropped), so this is comfortable.
- **DuckDB:** `threads=8`, `memory_limit='20GB'`, `temp_directory` + the staging `.duckdb` under `SCRATCH_DIR="/tmp/nppes_analytical"` on the **Modal ephemeral disk** (B1 — *not* `/mnt/nvme`, which does not exist there; `/tmp` is the fleet convention, `ingest.py:130`). The three `ORDER BY` sorts are the only spill-heavy steps; tables are ≤~12M rows so spill is modest, but size `max_temp_directory_size='128GB'` defensively (diag §6.5).
- **`LANCE_BYPASS_SPILLING=true`** for the BTREE trains on the high-card string columns (`last_name`, `practice_address_line1`, `identifier_value`) — Lance's bounded spill sorter OOMs on these (diag §6.6); in-RAM sort is <1 GiB each ≪ 32 GiB. Trains run sequentially.
- **Egress:** one projected R2 read of the raw (~10 GiB; the populated columns dominate, nulls are cheap). Do **not** re-scan R2 per output table — stage local once (D8).
- **Write transport:** local Lance build → `boto3` uniform-part publish. A direct Lance-to-R2 write trips R2 `400 InvalidPart` once an index `page_data.lance` escalates multipart part size (diag §6.6). Non-negotiable.

---

## 6. Idempotency, ledger, blast radius

- **Ledger:** `ops.nppes_analytical_runs` (mirror file `pipelines/nppes/ops_nppes_analytical_runs.sql`, applied by an `init_state` entrypoint). One row per build run: `snapshot_month`, `source_dataset_uri`, `source_version`, per-table `{provider,taxonomy,identifier}_rows`, `date_parse_failures`, `dirty_state_nulled`, per-table `dataset_uri`, `indices_built`, `g3_cold_ms`/`g6_cold_ms` (recorded, not gated — M3), `status` (`success` | `partial` | `error`), `error`, `started_at`, `completed_at`. Best-effort write (never mask a good build).
- **Idempotent + partial-publish detection (n3):** re-running a month wipes + republishes that month's three prefixes (the `_replace_r2_prefix` pattern); distinct months accrete. The three datasets publish to independent prefixes, so a crash after publishing 1 of 3 leaves a *torn* cross-dataset state until the (healing) re-run. Make it detectable, not silent: on any mid-run failure write the ledger row `status='partial'` with the published-vs-pending set (mirror `materialize_epa.py`'s partial/error status), and have `verify` (§8 G10) reject a torn state — all three prefixes must share the same `snapshot_month` and the child npis must be ⊆ provider npis.
- **Blast radius:** a NEW Modal function/app, separate from the raw ingest. Reads raw read-only; writes only the three derived prefixes. A failure here **cannot corrupt the raw SoR**. The heavy sorts are isolated from the raw monthly capture.

---

## 7. File layout & naming

```
pipelines/nppes/
  ingest.py                         # EXISTING — raw SoR (untouched)
  materialize_analytical.py         # NEW — this directive's worker (mirrors materialize_epa.py et al.)
  ops_nppes_analytical_runs.sql     # NEW — reviewable ledger DDL (mirrored by OPS_DDL in the worker)
```

Output URIs (env-overridable, e.g. `NPPES_PROVIDER_PREFIX`):
```
s3://data-sink/active/nppes_provider/snapshot=YYYY-MM/
s3://data-sink/active/nppes_provider_taxonomy/snapshot=YYYY-MM/
s3://data-sink/active/nppes_provider_identifier/snapshot=YYYY-MM/
```

Entrypoints (mirror `ingest.py`): `init_state`, `materialize --snapshot-month 2026-05`, `verify --snapshot-month 2026-05`, `show_ledger`. Reuse `_r2_storage_options`, `_s3_client`, `_replace_r2_prefix`, `_create_indexes` patterns from `ingest.py` (extract shared helpers if clean; otherwise duplicate the proven code rather than over-abstract).

---

## 8. Acceptance gate (mirror the diagnostic — this is how "done" is proven)

The `verify` entrypoint MUST run these against the freshly published layer. **Correctness gates (G1–G5, G8–G12) are ABSOLUTE — any failure fails the build.** Latency is **measured-and-recorded, not absolute-fail** (M3): cold R2 round-trips alone exceed sub-second (measured 552 ms open + 862 ms cold BITMAP count / 3,011 ms cold projection), so gating on cold sub-second would fail a *correct* build. Protocol: open the dataset + run one warm-up `count` per indexed column, then time the warm query; assert the **warm** thresholds and record the cold figure to the ledger.

| # | Assertion | Raw baseline (diag) | Required post-build |
|---|---|---|---|
| G1 | `nppes_provider` row count == raw distinct `npi` | — | **== 9,551,447** (no loss) · *absolute* |
| G2 | `count(DISTINCT npi)` in provider == row count | — | unique (PK preserved) · *absolute* |
| G3 | Taxonomy long: `count(*) WHERE taxonomy_code='106S00000X'` == raw any-of-15 count | 582,200 via 6.65 s 15-col scan | **== 582,200**, BITMAP used, **warm < 250 ms** (cold recorded) |
| G4 | Taxonomy: `count(*) WHERE is_primary` == raw `count(switch_*='Y')`; max primaries/npi | — | **== 9,208,126**, **≤ 1 primary per npi** · *absolute* |
| G5 | Date range: `count(*) WHERE enumeration_date >= DATE '2020-01-01'` | naive raw = **0 (silently wrong)** | **== 3,292,670** (via `date32`) · *absolute* |
| G6 | Specialty×geo: `provider ⋈ taxonomy WHERE taxonomy_code=X AND practice_state='TX'` | 8.73 s on raw | correct rows; **warm < 600 ms** (cold recorded). *Mechanism (M2): taxonomy BITMAP prunes to its fragment(s); the join range-prunes the provider side via a dynamic npi min/max filter — NOT an npi-BTREE take on both sides.* |
| G7 | Batch-`npi` fragment pruning — test the **Lance-scanner prefilter** path, not the hash join (M2): `provider.scanner(filter="npi IN (<1000 ids>)")` | all 10 fragments (raw, unclustered) | reads **< all** fragments (npi-sorted zone-maps prune); assert `fragments_scanned < n_fragments` |
| G8 | `date_parse_failures / source_non_null` per date col (all 5) | — | **< 0.0001** (measured 0.0 on 2026-05) · *absolute* |
| G9 | Dirty-state: cleaned `practice_state` ∈ USPS set ∪ {NULL} | 1,063 distinct (raw) | **≤ 63 distinct** (measured 59) · *absolute* |
| G10 | Cross-dataset integrity (n3): all three prefixes share `snapshot_month`; `count(DISTINCT npi)` in taxonomy & identifier ⊆ provider npi set | — | holds (rejects a torn partial publish) · *absolute* |
| G11 | `is_active` invariant (m1): `count(*) WHERE NOT is_active` == `count(*) WHERE entity_type_code IS NULL` | — | **== 343,321** (divergence ⇒ a re-deactivation edge appeared; trip for human review) · *absolute* |
| G12 | Provenance round-trip (M1): each published dataset's `schema.metadata['source_snapshot_uri']` non-empty | — | non-empty per dataset · *absolute* |

Record the cold G3/G6 latencies (`g3_cold_ms`/`g6_cold_ms`) to the ledger for cross-month regression visibility. The **correctness** assertions (G1–G5, G8–G12) are absolute build-fail gates; the warm-latency thresholds (G3/G6) gate against the measured 4–25 ms warm floor with headroom; cold latency is recorded, never gated (M3).

---

## 9. Execution sequence (for the executor agent)

1. Branch off `main` (`git fetch && git checkout -b claude/nppes-analytical-layer origin/main`).
2. Write `pipelines/nppes/ops_nppes_analytical_runs.sql` + the `OPS_DDL` mirror; write `pipelines/nppes/materialize_analytical.py` per §2–§7.
3. Local dry-run against `snapshot=2026-05` using the pinned toolchain (`uv venv --python 3.12`; `duckdb>=1.5,<2`, `pylance>=7`, `pyarrow>=17`, `boto3`; secrets via `doppler run --project core-x --config prd`). Stage under `SCRATCH_DIR` on the local ephemeral disk (`/tmp/nppes_analytical`, B1 — not `/mnt/nvme`); build all three tables; **do not publish** until the gate passes locally.
4. Run the §8 gate locally (read the local Lance stages with `scanner(filter=…)` and DuckDB). Iterate until the correctness gates G1–G5, G8–G12 pass (G6/G7 latencies recorded, warm thresholds asserted).
5. Publish to R2 (the three `snapshot=2026-05` prefixes) via the local-stage→boto3 path; run `verify` against the published datasets; confirm the gate passes on R2.
6. Apply `ops.nppes_analytical_runs` (`init_state`) and confirm a run row landed.
7. Commit, push, open PR vs `main`, **self-merge** (`gh pr merge --squash --delete-branch`), **pull into `/Users/benjamincrane/core-x`**, verify `git log -1 --oneline`. (Base on current `main`; never stack on an unmerged branch — squash drops post-squash commits.)
8. Update `docs/nppes_structural_diagnostic.md` §5/§7 with a one-line "implemented in <PR>" pointer (optional, same PR).

---

## 10. Wiring (refresh model — after the critical path lands)

Chain the materializer after the raw monthly capture: on the raw `nppes` ingest's terminal success (Trigger callback / `src/trigger/nppes_monthly.ts`), invoke `materialize_analytical::materialize --snapshot-month <month>`. Keep it **decoupled** — the materializer is independently invokable and a raw-ingest success does not block on it. A materialize failure pages but never rolls back the raw SoR.

---

## 11. Out of scope (named next companions — do NOT build here)

- **NUCC taxonomy reference** (`nppes_taxonomy_ref`): NPPES carries taxonomy *codes*, not labels. A tiny static ingest of the NUCC Health Care Provider Taxonomy crosswalk (code → grouping/classification/specialization display name) makes `taxonomy_code` human-readable and is the **immediate next companion** — without it, specialty filtering works but reads as opaque codes. Join key is `taxonomy_code`.
- **Geocoding**: lat/lon territory mapping via a join from `practice_state`+`practice_zip5`+`practice_city`/`practice_address_line1` to a geocoded reference (overture places / census ZIP centroids). The clean keys produced here (D6) unblock it.
- **Cross-snapshot SCD / change feed** (provider deltas month-over-month) — the per-snapshot partitioning (D7) makes this a future `diff(snapshot_n, snapshot_n-1)` job.

---

## 12. Non-negotiables (summary)

1. Raw SoR is read-only; this layer is a pure derived function of it.
2. Taxonomy is a long child table with a scalar `BITMAP(taxonomy_code)` — not `list<struct>`, not slot-1-only.
3. Dates are `date32`. No string dates survive into the analytical layer.
4. Local Lance build → boto3 publish (R2 multipart rule). Never index straight to R2.
5. The §8 gate passes before merge — it is the proof the GTM failures are reversed, not a formality.
6. Full git lifecycle owned end-to-end through the operator-checkout pull.
