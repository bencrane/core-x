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
| `usaspending_search_schema_dictionary` | 354 | 16 | usaspending-api repo matview models (`delta_models`) + DEC join + derivation extraction |
| `sam_reps_certs_provisions` | 193 | 12 | SAM.gov Reps & Certs mapping (`SAM_REPS_AND_CERTS_MAPPING.xlsx`) |
| `fpds_atom_feed_spec` | 12 | 8 | FPDS-NG Atom Feed Specification V1.5.3 (page text) |

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

## 4. `usaspending_search_schema_dictionary` — the `*_search` matview column gap

The per-**column** schema dictionary for the two USAspending `*_search` reporting matviews
(`award_search`, `subaward_search`) — the denormalized `rpt.*` columns the DEC (§1) does **not**
document. It closes the *enumeration + type* gap: every column of both matviews is cataloged with its
Postgres and Delta/Spark type, joined to a prose definition where one exists.

- **Rows:** 354 — `award_search` (151) + `subaward_search` (203).
- **Columns:** `row_ord`, `dataset`, `column_name`, `postgres_type`, `delta_type`, `gold`,
  `help_text`, `dec_element`, `dec_grouping`, `definition`, `definition_source`, + provenance.
- **Indices:** BTree(`row_ord`, `column_name`, `dec_element`) · Bitmap(`dataset`, `gold`,
  `definition_source`).
- **Authoritative source:** the **usaspending-api** repo (cloned live), the contracts these matviews
  are built from — `search/delta_models/*.py` (`AWARD_SEARCH_COLUMNS` / `SUBAWARD_SEARCH_COLUMNS` →
  column list + Postgres/Delta types) and `search/models/*.py` (sparse Django `help_text`).
  Prose `definition` is filled by joining each column to the DEC (§1) on the download/db element name.
- **`definition_source`** ∈ `dec` | `help_text` | `none`. Fill: `award_search` **37/151** defined
  (114 `none`); `subaward_search` **81/203** defined (122 `none`).
- **`derivation_expr` / `derivation_source`** — for columns with no prose definition, the
  authoritative **derivation** is extracted from the repo: `award_search` from the PySpark
  `AwardSearch` DataFrame `.alias("col")` expressions (`derivation_source='dataframes_alias'`, 74
  cols), `subaward_search` from the `subaward_search_load_sql_string` `<expr> AS <col>` items
  (`derivation_source='load_sql_as'`, 81 cols). E.g. `awarding_toptier_agency_name_raw =
  sf.coalesce(fabs.awarding_agency_name, fpds.awarding_agency_name)`. This is provenance code, not
  prose — but it authoritatively answers *how the column is computed*.
- **The residual gap:** with definitions + derivations combined, **fully-undocumented columns drop
  from 236 → 116** (`award_search` 49, `subaward_search` 67) — direct source-column passthroughs with
  no alias/AS and no DEC entry, whose meaning is their (self-evident) name. Filter the true residual
  with `definition IS NULL AND derivation_expr IS NULL`.
- **Builder:** `pipelines/reference/materialize_usaspending_search_schema.py <build|verify>`.
  Verified FAITHFUL row-for-row (column counts + pg/delta types) vs the repo source, both datasets.

## 5. `sam_reps_certs_provisions` — SAM Reps & Certifications

The SAM.gov Representations & Certifications provision→question mapping (193 rows) — which FAR/DFARS
provision each entity rep/cert answers, its question text, sample value, mandatory/optional status,
and enumerated answers. Materialized from the authoritative SAM.gov File Extract
(`SAM_REPS_AND_CERTS_MAPPING.xlsx`, staged in the R2 landing tier).

- **Rows:** 193 across 5 provision families (`provision_family`): FAR (136), DFARS (24), SF330 (7),
  FINANCIAL_ASSISTANCE (2), READ_ONLY (24). The same provision recurs across families → `provision`
  is a non-unique BTREE lookup; grain is `row_ord`.
- **Columns:** `row_ord`, `provision_family`, `provision`, `answer_id`, `question_or_cert`,
  `sample_value`, `mandatory_optional`, `required_condition`, `enumeration`, + provenance.
- **Indices:** BTree(`row_ord`, `provision`) · Bitmap(`provision_family`, `mandatory_optional`).
- **Builder:** `pipelines/reference/materialize_sam_reps_certs.py <build|verify>`.
- **Deferred:** the workbook's `SF330 ARCHITECT-ENG REFERENCES` (discipline/experience/revenue code
  lists) and `DOWNLOAD URLs` sheets — structurally different, not provision rows.

## 6. `fpds_atom_feed_spec` — FPDS Atom Feed structural spec

The FPDS-NG **Atom Feed Specification V1.5.3** — the authoritative structural spec for the FPDS Atom
feed (feed XML, Atom Element Definitions, Award/IDV XML). It is a wiki-exported PDF with **no
tables**, so it is landed as a **page-level text reference** (RAG/lookup surface), not a field table.

- **Rows:** 12 (one per page); `row_ord` = `page_number`. ~68 K chars total.
- **Columns:** `row_ord`, `page_number`, `text`, `text_char_len`, `doc_name`, + provenance.
- **Builder:** `pipelines/reference/materialize_fpds_atom_feed_spec.py <build|verify>`.
- **Deferred (out of scope):** the `FPDS_NASA_Specific_Data_Dictionary.pdf` (agency-specific).

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
