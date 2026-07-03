# Government Data Dictionaries — Field-Dictionary Reference

> **Verified 2026-07-03.** Three authoritative government **field dictionaries** materialized into the
> Gen-3 system of record (`s3://data-sink/active/…`, Lance v2.1, `mode=overwrite`) so this
> mission-critical metadata is queryable in-plane rather than trapped in repo files / raw landing
> spreadsheets. Each row = one source field/element. Grain is a synthetic `row_ord` (source document
> order) — the natural keys (`element` / `data_element` / `field_name`) are indexed lookups but are
> **not** globally unique, because these dictionaries legitimately repeat rows (deprecated
> placeholders; the same field across multiple file-structures). Every row carries provenance
> `source` / `source_vintage` / `ingested_at`. Do **not** hand-edit — regenerate from source and
> re-overwrite via `pipelines/reference/materialize_gov_data_dictionaries.py`.

Fidelity was verified **row-for-row** against each authoritative source by an independent re-parse
(row-count parity, per-cell spot-checks, column-alignment). Cells are preserved **verbatim** —
including the literal text `"None"` (a meaningful "no length constraint" in FAL `field_length`),
which the parser does not coerce to NULL (`keep_default_na=False`).

| Dataset (`active/…`) | Rows | Cols | Authoritative source |
|---|--:|--:|---|
| `usaspending_data_dictionary` | 457 | 21 | USAspending DATA Element Crosswalk (DEC) — fetched **live** from `files.usaspending.gov/docs/Data_Dictionary_Crosswalk.xlsx`, `Public` sheet |
| `sam_entity_extract_dictionary` | 368 | 19 | SAM.gov Entity Management master extract layout (`SAM_MASTER_EXTRACT_MAPPING_Feb2025.xlsx`) |
| `sam_fal_data_dictionary` | 84 | 9 | SAM.gov Federal Assistance Listings (`FAL_Data_Dictionary.xlsx`) |

---

## 1. `usaspending_data_dictionary` — the field dictionary of record

The USAspending **DATA Element Crosswalk (DEC)** — one row per data element (457), the authoritative
mapping of a spending data element to its definition, its FPDS name, and the exact column name it
appears under in every USAspending download file. This is the dictionary of record for the FPDS
transaction, award, subaward, and account download vocabularies.

- **Columns:** `row_ord`, `element`, `definition`, `fpds_data_dictionary_element`, `grouping`,
  `domain_values`, `domain_values_code_description`, `dl_award_file`, `dl_award_element`,
  `dl_subaward_file`, `dl_subaward_element`, `dl_account_file`, `dl_account_element`, `db_table`,
  `db_element`, `legacy_award_file`, `legacy_award_element`, `legacy_subaward_element`, + provenance.
- **Indices:** BTree(`row_ord`, `element`, `dl_award_element`, `fpds_data_dictionary_element`) ·
  Bitmap(`grouping`, `dl_award_file`, `dl_subaward_file`).
- **The load-bearing join keys:**
  - **`dl_award_element`** = the physical column name in the award/transaction download files — i.e.
    the vocabulary of our BULK `transaction_search_fpds` / `award_search` and FRESH feeds. Join a
    spine column name → its authoritative definition here.
  - **`fpds_data_dictionary_element`** = the FPDS element name (e.g. `ActionType` → `Reason for
    Modification`).
  - **`dl_subaward_element`** = the subaward download column name (subaward vocabulary).
- **Verified anchors:** `ActionType` → FPDS `Reason for Modification`, download `action_type_code`;
  `FederalActionObligation` → FPDS `Action Obligation`, download `federal_action_obligation`.

**Supersedes** the committed repo sidecar `pipelines/usaspending/fpds_field_definitions.json`. Both
derive from the same USAspending Data Dictionary origin, but the sidecar is a static, FPDS-only
`{field: {definition, type}}` snapshot bundled in the repo; this table is fetched live, covers the
full transaction + award + subaward + account vocabularies (closing the award/subaward dictionary
gap), carries domain values and every download/legacy/db crosswalk column, and is queryable +
indexed in-plane. New definition joins should target this table; the JSON sidecar is retained only
until the FPDS canonical dictionary generator is repointed.

## 2. `sam_entity_extract_dictionary` — SAM entity extract layout

The SAM.gov Entity Management master extract field layout (368 rows): each SAM entity extract data
element with datatype, format, max length, definition, mandatory flag, and CUI sensitivity
(`public` / `fouo_cui` / `sensitive_cui`).

- **Columns:** `row_ord`, `data_element`, `column_order`, `datatype`, `data_format`, `max_length`,
  `definition`, `enumeration`, `mandatory`, `substitution_for_mandatory`, `sample_values`,
  `sensitivity_level`, `sensitivity_group`, `public`, `fouo_cui`, `sensitive_cui`, + provenance.
- **Indices:** BTree(`row_ord`, `data_element`) · Bitmap(`datatype`, `mandatory`).
- **Key note:** `data_element` has 365 distinct of 368 rows — `BLANK (DEPRECATED)` appears 4× (real,
  reserved extract positions), carried faithfully via `row_ord`.

## 3. `sam_fal_data_dictionary` — SAM Federal Assistance Listings

The SAM.gov Federal Assistance Listings (grants) extract field dictionary (84 rows). The source
sheet stacks **three file-structures** — GRANTS (6 fields), DATA GOV current (38), DATA GOV archived
(40) — so each field is tagged with its `file_structure` (`… [1]/[2]/[3]`); the same field name
(`Program Title`, `URL`, …) recurs across sections by design.

- **Columns:** `row_ord`, `file_structure`, `field_name`, `field_type`, `field_length`,
  `definition`, + provenance.
- **Indices:** BTree(`row_ord`, `field_name`) · Bitmap(`field_type`, `file_structure`).

---

## Access pattern

```python
import lance
from pipelines.bls.ingest import _storage_options
so = _storage_options()
dd = lance.dataset("s3://data-sink/active/usaspending_data_dictionary/", storage_options=so)
# resolve a spine column name to its authoritative definition:
dd.scanner(columns=["element","definition","fpds_data_dictionary_element","grouping"],
           filter="dl_award_element = 'federal_action_obligation'").to_table().to_pylist()
```

## Provenance & regeneration

- **Builder:** `pipelines/reference/materialize_gov_data_dictionaries.py <build|verify>`.
- USAspending DEC is fetched **live** (public, no auth). SAM `.xlsx` are read from the R2 landing
  tier (`landing/sam-gov/data-dictionary/…`) where the authoritative SAM.gov File Extracts were
  staged verbatim.
- **Deferred:** `SAM_REPS_AND_CERTS_MAPPING.xlsx` (7 heterogeneous provision sheets — a reps & certs
  provisions catalog, not a clean field schema).
- Upstream authority for USAspending is the **GSDM** (Government-wide Spending Data Model,
  `fiscal.treasury.gov/data-transparency/GSDM-current.html`); the DEC Crosswalk is its published,
  field-level rendering.
