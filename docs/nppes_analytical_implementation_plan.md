# NPPES Analytical Layer — Implementation Directive (Canonical)

**Owner of record:** Principal Data Engineer. This is the canonical end-to-end spec; execute against it verbatim.
**Repo:** `core-x` · **Doppler:** `core-x/prd` · **Mode:** BUILD — append-only, idempotent, per-snapshot. Mutates only NEW derived prefixes; the raw NPPES SoR is read-only and untouched.
**Descends from:** [`docs/nppes_structural_diagnostic.md`](nppes_structural_diagnostic.md) (#208, #211) — every decision traces to a measured finding there (`(diag §N)` inline).
**Validated against live data:** the model decisions, runtime mechanics, and gate values below were independently verified on the live `snapshot=2026-05` (pinned `pylance 7.0.0` / `duckdb 1.5.3`); the validation record is [`docs/nppes_analytical_plan_adversarial_review.md`](nppes_analytical_plan_adversarial_review.md) (#222). Numbers stated as exact (e.g. row counts, NDV, gate targets) are measured, not estimated.

---

## 0. Premise (the measured reality this remediates)

The raw NPPES SoR at `s3://data-sink/active/nppes/snapshot=YYYY-MM/` is **physically pristine but stored in raw CMS dissemination shape, not analytical shape** (diag §7). Confirmed, with numbers:

- Pushdown across the DuckDB↔Lance boundary **works** — `count(*) WHERE npi=X` ≈100 ms vs identical-shape unindexed `entity_type_code='2'` 1,243 ms; `SELECT * WHERE npi=X` returns 1 row in 1.9 s, not 120 s (diag §6.1). **The engine is not the problem.**
- It fails because the analytical axes are structurally hostile: **dates are `MM/DD/YYYY` strings** that don't sort chronologically (naive range filter returns 0 — silent garbage; diag §6.3); **specialty is shattered across `taxonomy_code_1..15`** with no indexable form (15-col OR scan = 6.65 s; primary-slot-only undercounts 12%; diag §6.4); the analytical columns carry **no index** (scan floor ≈97 MiB/s; specialty×geo cell = 8.73 s; diag §6.2); **`npi` is unclustered** (batch joins fan out to all 10 fragments; diag §1.1).

**This directive builds the derived analytical serving layer.** The raw monthly snapshot stays the immutable archive; GTM/market-mapping queries hit the derived layer.

**Definition of done (operational):** the three derived datasets below exist for `snapshot=2026-05`, are scalar-indexed, and **pass the §8 acceptance gate** — the exact queries that fail/scan on raw today now push down and return correct results (specialty filter warm sub-250 ms with fragment pruning; date-range returns the correct 3,292,670; `npi` batch prefilter prunes fragments). PR merged, operator checkout pulled, `git log -1` verified.

---

## 1. Architecture decisions (baked in — do not re-litigate)

**D1 — Three derived datasets, not one.** A flat 1-row-per-NPI core plus two unpivoted long children. The GTM killers are (a) specialty filtering and (b) the wide-null sprawl; both are solved by normalizing the repeating groups out to long tables that carry an *indexable scalar* key.

| Dataset | Grain | Rows (measured @ 2026-05) | Purpose |
|---|---|---:|---|
| `nppes_provider` | 1 row / NPI | 9,551,447 | typed, cleaned, geo-join-ready provider core + denormalized primary specialty |
| `nppes_provider_taxonomy` | 1 row / (NPI, populated taxonomy slot) | 11,952,809 | the specialty long table — **the single change that makes market-mapping possible** |
| `nppes_provider_identifier` | 1 row / (NPI, populated other-identifier slot) | 2,759,800 | external-ID linkage (Medicaid/Medicare/etc.) — lower priority, independently shippable |

**D2 — Taxonomy as a LONG CHILD TABLE, explicitly NOT `list<struct>`.** A `list<struct>` keeps one dataset but **cannot carry a Lance scalar index on the specialty code** (scalar indices are per-scalar-column; indexing a list element is not supported), reintroducing the exact scan problem this cycle exists to kill. The long table gives a scalar `taxonomy_code` → `BITMAP` index → `WHERE taxonomy_code = X` is an indexed pushdown predicate, includes secondary specialties (every populated slot → a row), and makes specialty×geo a clean indexed `GROUP BY` after an `npi` join. This is the load-bearing decision of the cycle, and it is non-trivially correct: **1,106,232 providers (~12%) hold their primary specialty in a slot whose code differs from `code_1`**, so a slot-1 shortcut would mislabel ~12% of the market.

**D3 — Dates → `date32`, parsed once, in the bedrock.** `try_strptime(col,'%m/%d/%Y')::DATE`. Fixes the broken temporal axis (diag §6.3) so range filters and zone-map pruning work. On `snapshot=2026-05` this parses with **zero failures across all five date columns**; the build still counts failures into the ledger and gates them (<0.0001). The analytical layer carries `date32` only; the raw string stays in the archive (no duplication).

**D4 — One provider table with `entity_type_code`, not a split.** Individuals and organizations share the table; org-only fields are NULL for individuals and vice-versa. `entity_type_code` gets a `BITMAP`. Splitting fragments every join. Add a decoded `entity_type` ('individual'|'organization') and a unified `provider_name` for the common "show the provider" path.

**D5 — Deactivated providers are KEPT, flagged, never dropped.** The 343,321-row (3.594%) deactivated-stub cohort (diag §3) gets a `nppes_provider` row with `is_active=false` and mostly-NULL descriptive fields (they have no taxonomy → no child rows). Derive `is_active` so every downstream GTM query filters cleanly instead of re-deriving deactivation logic.

**D6 — Geo-join-ready, not geocoded.** No lat/lon in NPPES. Produce clean `practice_state` (USPS-validated), `practice_zip5` (derived), `practice_city`, and a `BTREE` on `practice_address_line1` so the layer can join to a geocoded reference (overture/census ZIP centroid) downstream. Actual geocoding is out of scope (§11) but unblocked.

**D7 — Per-snapshot, pure function of one raw month.** Output partitions mirror the raw: `…/nppes_provider/snapshot=YYYY-MM/`, etc. Rebuildable, idempotent (overwrite the month prefix), append-history across months. The derived layer is never hand-edited; it is always re-derivable from the raw SoR.

**D8 — Read raw ONCE, stage local, derive all three locally.** One R2 read of the projected raw into a local out-of-core DuckDB database on the Modal ephemeral disk; then three local `CTAS` transforms. Minimizes egress (no triple R2 scan) and keeps the build out-of-core within the 32 GiB envelope. Local Lance stage → boto3 publish (the R2 multipart rule, diag §6.6, is mandatory — never write indices straight to R2).

**D9 — Drop the noise in-transform.** Exclude the dead column (`npi_deactivation_reason_code`, 100% null), the three `'<UNAVAIL>'` redaction sentinels (`employer_identification_number_ein`, `parent_organization_tin`, `provider_other_organization_name`), and per-row provenance (carry `source_snapshot_uri`/`source_member` as dataset schema metadata, keep `snapshot_month` as the vintage key). (diag §3, §5.7–5.8)

---

## 2. Output schemas (exact)

### 2.1 `nppes_provider` — 1 row / NPI (9,551,447 rows)

| Column | Type | Derivation | Index |
|---|---|---|---|
| `npi` | `string` | passthrough (PK; verified unique, diag §2) | **BTREE** |
| `entity_type_code` | `string` | passthrough (`'1'`/`'2'`/NULL) | **BITMAP** |
| `entity_type` | `string` | `'1'`→`individual`, `'2'`→`organization`, else NULL | — |
| `is_active` | `bool` | active unless a deactivation with no later reactivation, treating descriptive-field stubs as inactive (§3.3) | **BITMAP** |
| `provider_name` | `string` | org: legal name; individual: `concat_ws(', ', last, trim(concat_ws(' ', first, middle)))` | — |
| `organization_name` | `string` | `provider_organization_name_legal_business_name` | — |
| `last_name` | `string` | `provider_last_name_legal_name` | **BTREE** |
| `first_name` | `string` | `provider_first_name` | — |
| `middle_name` | `string` | passthrough | — |
| `name_prefix` / `name_suffix` / `credential` | `string` | passthrough | — |
| `sex_code` | `string` | passthrough (`F`/`M`/`X`/`U`/NULL) | — |
| `is_sole_proprietor` | `string` | passthrough | — |
| `is_organization_subpart` | `string` | passthrough | — |
| `primary_taxonomy_code` | `string` | code at the slot where `switch_n='Y'`, else `code_1` (§3.3) | **BITMAP** |
| `practice_address_line1` | `string` | `provider_first_line_business_practice_location_address` | **BTREE** |
| `practice_address_line2` | `string` | second line | — |
| `practice_city` | `string` | passthrough | — |
| `practice_state` | `string` | USPS-clean (§3.2), else NULL | **BITMAP** |
| `practice_zip5` | `string` | 5-digit prefix of the postal code, else NULL | **BTREE** |
| `practice_zip` | `string` | passthrough (full, as-stored) | — |
| `practice_country` | `string` | `…country_code_if_outside_us` | — |
| `practice_phone` / `practice_fax` | `string` | passthrough | — |
| `mailing_city` | `string` | passthrough | — |
| `mailing_state` | `string` | USPS-clean | — |
| `mailing_zip5` | `string` | 5-digit prefix | — |
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

**Sort:** `ORDER BY npi` (fragment pruning for batch resolution joins; diag §1.1). **`max_rows_per_file=1048576`, `data_storage_version='2.1'`** (→ 10 fragments).

### 2.2 `nppes_provider_taxonomy` — 1 row / (NPI, slot) (11,952,809 rows)

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

**Sort:** `ORDER BY taxonomy_code, npi` — clusters fragments by the hot predicate so `WHERE taxonomy_code=X` prunes whole `.lance` files (verified: a single-specialty filter reads **1 fragment / 25.67 KB / 1 IOP** on a representative build), with `npi` locally sorted for the join back to `nppes_provider`. (→ 12 fragments.)

### 2.3 `nppes_provider_identifier` — 1 row / (NPI, slot) (2,759,800 rows)

| Column | Type | Derivation | Index |
|---|---|---|---|
| `npi` | `string` | from parent | **BTREE** |
| `identifier_rank` | `int8` | slot 1..50 | — |
| `identifier_value` | `string` | `other_provider_identifier_<n>` (NOT NULL filter) | **BTREE** |
| `identifier_type_code` | `string` | `other_provider_identifier_type_code_<n>` | **BITMAP** |
| `identifier_state` | `string` | `other_provider_identifier_state_<n>`, USPS-clean | **BITMAP** |
| `identifier_issuer` | `string` | `other_provider_identifier_issuer_<n>` | — |
| `snapshot_month` | `string` | vintage | — |

**Sort:** `ORDER BY npi`. (→ 3 fragments.)

---

## 3. Transform specification

### 3.1 Stage projected raw once (D8, out-of-core)

Modal mounts the 512 GiB ephemeral disk at the container root; the fleet spills under `/tmp` (`pipelines/nppes/ingest.py:130` = `/tmp/nppes`). There is **no `/mnt/nvme` mount** — all scratch, spill, the staging `.duckdb`, and the local Lance stages live under a `/tmp` `SCRATCH_DIR`.

```python
SCRATCH_DIR = "/tmp/nppes_analytical"
SPILL_DIR   = os.path.join(SCRATCH_DIR, "duck_spill")
os.makedirs(SPILL_DIR, exist_ok=True)

con = duckdb.connect(os.path.join(SCRATCH_DIR, "nppes_build.duckdb"))   # out-of-core, on the ephemeral disk
con.execute("PRAGMA threads=8;")
con.execute("SET memory_limit='20GB';")
con.execute(f"SET temp_directory='{SPILL_DIR}';")
con.execute("SET max_temp_directory_size='128GB';")
con.register("raw", lance.dataset(RAW_URI, storage_options=so))   # snapshot=2026-05
# single R2 read → local out-of-core table; project all source cols EXCEPT the §D9 noise
con.execute(f"CREATE TABLE rawstage AS SELECT {PROJECTED_COLS} FROM raw")
```

`PROJECTED_COLS` = every source column needed by §2 (npi, entity/name/address/mailing/date/flag/authorized-official/parent-org cols + `*_taxonomy_code_1..15`, `*_primary_taxonomy_switch_1..15`, `provider_license_number_1..15`, `provider_license_number_state_code_1..15`, `*_taxonomy_group_1..15`, `other_provider_identifier_1..50` + `_type_code_/_state_/_issuer_`). Exclude `npi_deactivation_reason_code`, the three `'<UNAVAIL>'` columns, `source_file/source_member/ingested_at`. All 308 referenced source columns resolve against the live raw schema.

### 3.2 Cleaning helpers (DuckDB SQL macros)

```sql
-- USPS-valid 2-letter state, else NULL. 63 codes: 50 states + DC + territories
-- (AS GU MP PR VI UM) + freely-associated states (FM MH PW) + military (AA AE AP).
-- Everything else 2-letter (BC/ON/QC/MX/UK/JP/…) is foreign → correctly NULLed.
CREATE MACRO clean_state(s) AS (
  CASE WHEN list_contains(
    ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS',
     'KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY',
     'NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV',
     'WI','WY','DC','AS','GU','MP','PR','VI','UM','FM','MH','PW','AA','AE','AP'],
    upper(trim(s)))
  THEN upper(trim(s)) ELSE NULL END);
CREATE MACRO zip5(z) AS nullif(regexp_extract(z, '^\s*(\d{5})', 1), '');   -- 5-digit prefix; foreign/non-numeric → NULL
CREATE MACRO d(x)   AS try_strptime(x, '%m/%d/%Y')::DATE;                  -- MM/DD/YYYY → date32, NULL on fail
```

### 3.3 `nppes_provider` derivation (key expressions)

```sql
CREATE TABLE provider AS
SELECT
  npi,
  entity_type_code,
  CASE entity_type_code WHEN '1' THEN 'individual' WHEN '2' THEN 'organization' END AS entity_type,
  -- Active unless deactivated with no later reactivation. The `entity_type_code IS NOT NULL`
  -- clause keeps it robust to a re-deactivation stub (deact→react→deact, where NPPES retains
  -- only the latest pair but clears descriptive fields): such a stub is treated inactive.
  -- On snapshot=2026-05 this is exactly the 343,321 deactivation-stub cohort.
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
  -- primary specialty: the code at the slot whose switch='Y' (exactly one per provider-with-
  -- taxonomy; verified 0 zero-switch and 0 multi-switch rows), else slot-1 fallback.
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

Generate a `UNION ALL` over slots in Python (deterministic, NULL-filtered, each arm carrying its parallel switch/license/group). A blind `UNPIVOT` is wrong here — it would orphan the parallel switch columns from their codes.

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

### 3.5 Write each table → local Lance stage → persist metadata → index → publish

For each of `provider`, `taxonomy`, `identifier`:

1. **Stream** the sorted DuckDB table to a local Lance dataset via `to_arrow_reader(131072)` (bounded RSS), `max_rows_per_file=1048576`, `data_storage_version='2.1'`.
2. **Persist provenance metadata.** A DuckDB `to_arrow_reader` schema carries no key-value metadata, so it must be set explicitly on the committed dataset before indexing (verified to round-trip through write + index + publish + reopen on `pylance 7.0.0`):
   ```python
   ds = lance.dataset(local_stage)
   ds.update_schema_metadata({
       "source_snapshot_uri": RAW_URI,
       "source_member":       source_member,
       "pipeline":            "materialize_analytical",
       "snapshot_month":      snapshot_month,
   }, replace=True)
   ```
3. **Build the scalar indices locally** (§4).
4. **`boto3` publish** (wipe + uniform-part upload) to `…/<name>/snapshot=YYYY-MM/`.

Building indices with `storage_options` directly against R2 trips the multipart rule (`400 InvalidPart`) once a BTREE `page_data.lance` escalates part size mid-upload (diag §6.6). Local build → uniform-part publish is the only R2-compliant transport — identical to `pipelines/nppes/ingest.py`.

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

Rationale anchored to measured cardinality: BTREE for high-card resolution/range keys (npi, names, address, zip5, dates); BITMAP for the low/medium-card categorical filters — `entity_type_code` NDV **2**, `taxonomy_code` NDV **873** (exact, long form), cleaned `practice_state` NDV **59**, `identifier_type_code` NDV **2**, `enumeration_year` ~22 (2005–2026). `is_active`/`is_primary` are boolean → BITMAP. All sit squarely in BITMAP territory. Clustering sorts (§2) make the hot predicate prune fragments.

---

## 5. Out-of-core, memory, I/O

- **Envelope:** Modal `memory=32768, cpu=8.0, ephemeral_disk=524288` (same as the raw ingest). The derived tables are *smaller* than raw (null sprawl dropped), so this is comfortable.
- **DuckDB:** `threads=8`, `memory_limit='20GB'`, `temp_directory` + the staging `.duckdb` under `SCRATCH_DIR="/tmp/nppes_analytical"` on the Modal ephemeral disk (§3.1). The three `ORDER BY` sorts are the only spill-heavy steps; tables are ≤~12M rows so spill is modest, but size `max_temp_directory_size='128GB'` defensively (diag §6.5).
- **`LANCE_BYPASS_SPILLING=true`** for the BTREE trains on the high-card string columns (`last_name`, `practice_address_line1`, `identifier_value`) — Lance's bounded spill sorter OOMs on these (diag §6.6); in-RAM sort is <1 GiB each ≪ 32 GiB. Trains run sequentially.
- **Egress:** one projected R2 read of the raw (~10 GiB; the populated columns dominate, nulls are cheap). Do **not** re-scan R2 per output table — stage local once (D8).
- **Write transport:** local Lance build → `boto3` uniform-part publish (the R2 multipart rule; §3.5). Non-negotiable.

---

## 6. Idempotency, ledger, blast radius

- **Ledger:** `ops.nppes_analytical_runs` (mirror file `pipelines/nppes/ops_nppes_analytical_runs.sql`, applied by an `init_state` entrypoint). One row per build: `snapshot_month`, `source_dataset_uri`, `source_version`, per-table `{provider,taxonomy,identifier}_rows`, `date_parse_failures`, `dirty_state_nulled`, per-table `dataset_uri`, `indices_built`, `g3_cold_ms`/`g6_cold_ms` (recorded for trend, not gated), `status` (`success` | `partial` | `error`), `error`, `started_at`, `completed_at`. Best-effort write (never mask a good build).
- **Idempotent + partial-publish detection.** Re-running a month wipes + republishes that month's three prefixes (the `_replace_r2_prefix` pattern); distinct months accrete. The three datasets publish to independent prefixes, so a crash after publishing 1 of 3 leaves a *torn* cross-dataset state until the (healing) re-run. Make it detectable, not silent: on any mid-run failure write `status='partial'` with the published-vs-pending set, and have `verify` (§8 G10) reject a torn state — all three prefixes must share the same `snapshot_month` and the child npis must be ⊆ provider npis.
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

The `verify` entrypoint runs these against the freshly published layer. **Correctness gates (G1–G5, G8–G12) are ABSOLUTE — any failure fails the build.** Latency is **measured-and-recorded, not absolute-fail**: cold R2 round-trips alone exceed sub-second (measured 552 ms open + 862 ms cold BITMAP count + 3,011 ms cold projection), so gating on cold sub-second would fail a *correct* build. Protocol: open the dataset + run one warm-up `count` per indexed column, then time the **warm** query; assert the warm threshold and record the cold figure to the ledger.

| # | Assertion | Raw baseline (diag) | Required post-build |
|---|---|---|---|
| G1 | `nppes_provider` row count == raw distinct `npi` | — | **== 9,551,447** (no loss) · *absolute* |
| G2 | `count(DISTINCT npi)` in provider == row count | — | unique (PK preserved) · *absolute* |
| G3 | Taxonomy long: `count(*) WHERE taxonomy_code='106S00000X'` == raw any-of-15 count | 582,200 via 6.65 s 15-col scan | **== 582,200**, BITMAP used, **warm < 250 ms** (cold recorded) |
| G4 | Taxonomy: `count(*) WHERE is_primary` == raw `count(switch_*='Y')`; max primaries/npi | — | **== 9,208,126**, **≤ 1 primary per npi** · *absolute* |
| G5 | Date range: `count(*) WHERE enumeration_date >= DATE '2020-01-01'` | naive raw = **0 (silently wrong)** | **== 3,292,670** (via `date32`) · *absolute* |
| G6 | Specialty×geo: `provider ⋈ taxonomy WHERE taxonomy_code=X AND practice_state='TX'` | 8.73 s on raw | correct rows; **warm < 600 ms** (cold recorded). Mechanism: the `taxonomy_code` BITMAP prunes to its fragment(s); the join range-prunes the provider side via a dynamic `npi` min/max filter — it is *not* an npi-BTREE take on both sides. |
| G7 | Batch-`npi` fragment pruning — exercise the Lance-scanner prefilter path (not a hash join): `provider.scanner(filter="npi IN (<1000 ids>)")` | all 10 fragments (raw, unclustered) | reads **< all** fragments (npi-sorted zone-maps prune); assert `fragments_scanned < n_fragments` |
| G8 | `date_parse_failures / source_non_null` per date col (all 5) | — | **< 0.0001** (measured 0.0 on 2026-05) · *absolute* |
| G9 | Dirty-state: cleaned `practice_state` ∈ USPS set ∪ {NULL} | 1,063 distinct (raw) | **≤ 63 distinct** (measured 59) · *absolute* |
| G10 | Cross-dataset integrity: all three prefixes share `snapshot_month`; `count(DISTINCT npi)` in taxonomy & identifier ⊆ provider npi set | — | holds (rejects a torn partial publish) · *absolute* |
| G11 | `is_active` invariant: `count(*) WHERE NOT is_active` == `count(*) WHERE entity_type_code IS NULL` | — | **== 343,321** (divergence ⇒ a re-deactivation edge appeared; trip for human review) · *absolute* |
| G12 | Provenance round-trip: each published dataset's `schema.metadata['source_snapshot_uri']` non-empty | — | non-empty per dataset · *absolute* |

The correctness assertions (G1–G5, G8–G12) are absolute build-fail gates; the warm-latency thresholds (G3/G6) gate against the measured 4–25 ms warm floor with headroom; cold latency (`g3_cold_ms`/`g6_cold_ms`) is recorded for cross-month regression visibility, never gated.

---

## 9. Execution sequence (for the executor agent)

1. Branch off `main` (`git fetch && git checkout -b claude/nppes-analytical-layer origin/main`).
2. Write `pipelines/nppes/ops_nppes_analytical_runs.sql` + the `OPS_DDL` mirror; write `pipelines/nppes/materialize_analytical.py` per §2–§7.
3. Local dry-run against `snapshot=2026-05` using the pinned toolchain (`uv venv --python 3.12`; `duckdb>=1.5,<2`, `pylance>=7`, `pyarrow>=17`, `boto3`; secrets via `doppler run --project core-x --config prd`). Stage under `SCRATCH_DIR` (`/tmp/nppes_analytical`); build all three tables; **do not publish** until the gate passes locally.
4. Run the §8 gate locally (read the local Lance stages with `scanner(filter=…)` and DuckDB). Iterate until the correctness gates G1–G5, G8–G12 pass (G6/G7 latencies recorded, warm thresholds asserted).
5. Publish to R2 (the three `snapshot=2026-05` prefixes) via the local-stage→boto3 path; run `verify` against the published datasets; confirm the gate passes on R2.
6. Apply `ops.nppes_analytical_runs` (`init_state`) and confirm a run row landed.
7. Commit, push, open PR vs `main`, **self-merge** (`gh pr merge --squash --delete-branch`), **pull into `/Users/benjamincrane/core-x`**, verify `git log -1 --oneline`. (Base on current `main`; never stack on an unmerged branch — squash drops post-squash commits.)
8. Optional: update `docs/nppes_structural_diagnostic.md` §5/§7 with a one-line "implemented in <PR>" pointer.

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
2. Taxonomy is a long child table with a scalar `BITMAP(taxonomy_code)` — not `list<struct>`, not slot-1-only (12% of providers' primary specialty would be mislabeled).
3. Dates are `date32`. No string dates survive into the analytical layer.
4. All scratch/spill/stage under `/tmp` `SCRATCH_DIR` on the Modal ephemeral disk — never `/mnt/nvme`.
5. Provenance metadata set explicitly via `update_schema_metadata` after the streaming write (the reader schema carries none) — gated by G12.
6. Local Lance build → boto3 publish (R2 multipart rule). Never index straight to R2.
7. The §8 gate passes before merge — correctness gates absolute, latency warm-asserted + cold-recorded. It is the proof the GTM failures are reversed, not a formality.
8. Full git lifecycle owned end-to-end through the operator-checkout pull.
