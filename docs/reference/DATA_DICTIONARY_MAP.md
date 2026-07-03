# Data Dictionary Map — which dictionary resolves any field in the federal-spend + SAM estate

> **Navigation layer.** Six authoritative government **field dictionaries** are materialized into the
> Gen-3 system of record (`s3://data-sink/active/…`, Lance v2.1). This file is the *discovery* index:
> given a column from a DATA dataset (FPDS spine, `award_search`, subaward, SAM entity/FAL/reps),
> it tells you **which dictionary resolves it and on what key** — in under 30 seconds. It does not
> restate per-table detail; for that, jump to
> [`GOV_DATA_DICTIONARIES.md`](GOV_DATA_DICTIONARIES.md) (per-table schema, indices, provenance,
> fidelity notes) and [`00_ACTIVE_SINK_CATALOG.md`](00_ACTIVE_SINK_CATALOG.md) (the full sink catalog,
> Reference + SAM.gov sections). All six carry a synthetic `row_ord` grain and
> `source` / `source_vintage` / `ingested_at` provenance; natural keys are indexed but **non-unique**
> (dictionaries legitimately repeat rows). Do **not** hand-edit — regenerate from source (builders
> below).

## The six dictionaries

| Dataset (`active/…`) | Rows | Cols | What it covers | Key columns |
|---|--:|--:|---|---|
| `usaspending_data_dictionary` | 457 | 21 | USAspending **DATA Element Crosswalk (DEC)** — the field dictionary of record for the FPDS / award / subaward / account **download** vocabularies. One row per data element → definition + FPDS name + physical download column name. | `element`, `definition`, `fpds_data_dictionary_element`, `dl_award_element`, `dl_subaward_element`, `grouping` |
| `usaspending_search_schema_dictionary` | 354 | 16 | Per-**column** schema for the two `*_search` rpt matviews (the denormalized columns the DEC does **not** document). Type + prose def where one exists + authoritative derivation code. | `dataset`, `column_name`, `postgres_type`, `delta_type`, `definition`, `definition_source`, `derivation_expr`, `derivation_source`, `dec_element` |
| `sam_entity_extract_dictionary` | 368 | 19 | SAM.gov Entity Management master extract layout — every SAM entity-registration field with datatype, format, max length, mandatory flag, CUI sensitivity. | `data_element`, `datatype`, `data_format`, `max_length`, `definition`, `mandatory`, `public`/`fouo_cui`/`sensitive_cui` |
| `sam_fal_data_dictionary` | 84 | 9 | SAM.gov Federal Assistance Listings (grants) extract — 3 stacked file-structures (GRANTS / DATA GOV current / archived), tagged per `file_structure`. | `field_name`, `field_type`, `field_length`, `definition`, `file_structure` |
| `sam_reps_certs_provisions` | 193 | 12 | SAM.gov Representations & Certifications provision→question mapping — which FAR/DFARS/SF330 provision each rep/cert answers. | `provision_family`, `provision`, `question_or_cert`, `mandatory_optional`, `enumeration` |
| `fpds_atom_feed_spec` | 12 | 8 | FPDS-NG Atom Feed Specification V1.5.3 — page-level text (RAG/lookup surface, **not** a field table). | `page_number`, `text` |

## Which dictionary for which DATA dataset

Start from the column you're holding; the middle column is the dictionary; the right column is the join key.

| I have a column from… | Use this dictionary | Join on |
|---|---|---|
| `usaspending_fpds_canonical_txn` (FPDS spine) / BULK `transaction_search_fpds` / FRESH feeds | `usaspending_data_dictionary` | `dl_award_element` = the spine/download column name → definition; or `fpds_data_dictionary_element` for the FPDS element name |
| `usaspending/award_search` (award-grain matview) | `usaspending_search_schema_dictionary` (`dataset='award_search'`) | `column_name`; falls back to `usaspending_data_dictionary` via that row's `dec_element` where `definition_source='dec'` |
| `usaspending/subaward_search`, `usaspending_subaward_canonical`, `contract_subaward` | `usaspending_search_schema_dictionary` (`dataset='subaward_search'`) **+** `usaspending_data_dictionary` (`dl_subaward_element`) | `column_name` / `dl_subaward_element` |
| SAM entity registrations extract (`sam_master_entities` field layout, entity extract) | `sam_entity_extract_dictionary` | `data_element` |
| Assistance listings / grants (FAL extract) | `sam_fal_data_dictionary` | `field_name` (+ `file_structure` to disambiguate the recurring field name) |
| SAM reps & certs answers | `sam_reps_certs_provisions` | `provision` (+ `provision_family`) |
| FPDS Atom feed XML structure (element names, Award/IDV XML) | `fpds_atom_feed_spec` | full-text over `text` (page lookup, no field key) |

**Two-hop note (award/subaward matviews):** the matview columns live in
`usaspending_search_schema_dictionary`. Where a column maps to a DEC element it carries `dec_element`
(and `definition_source='dec'`) so you can hop to `usaspending_data_dictionary` for the full
definition + domain values. Coverage is partial by design — `award_search` 37/151 have a prose
`definition`, `subaward_search` 81/203 — so for the rest use `derivation_expr` (authoritative *how it's
computed* code: award_search 74 cols, subaward_search 81 cols). True residual (name is self-evident,
no def + no deriv): filter `definition IS NULL AND derivation_expr IS NULL` (award_search 49,
subaward_search 67).

## Resolution recipes (verified live)

**R1 — a physical FPDS/download column → its authoritative definition.**
Column `federal_action_obligation` from the FPDS spine / `transaction_search_fpds`:
```python
dd = lance.dataset("s3://data-sink/active/usaspending_data_dictionary/", storage_options=so)
dd.scanner(columns=["element","definition","fpds_data_dictionary_element","grouping","dl_award_element"],
           filter="dl_award_element = 'federal_action_obligation'").to_table().to_pylist()
# → element='FederalActionObligation', fpds_data_dictionary_element='Action Obligation',
#   grouping='Award Spending', definition='Amount of Federal government's obligation, de-obligation,
#   or liability, in dollars, for an award transaction.'
```

**R2 — an `award_search` matview column the DEC doesn't document → how it's derived.**
Column `awarding_toptier_agency_name_raw` (no DEC entry, so use the extracted derivation):
```python
ss = lance.dataset("s3://data-sink/active/usaspending_search_schema_dictionary/", storage_options=so)
ss.scanner(columns=["column_name","postgres_type","definition","derivation_expr","derivation_source"],
           filter="dataset='award_search' AND column_name='awarding_toptier_agency_name_raw'").to_table().to_pylist()
# → postgres_type='TEXT', definition=None, derivation_source='dataframes_alias',
#   derivation_expr='sf.coalesce( self.transaction_fabs.awarding_agency_name,
#                    self.transaction_fpds.awarding_agency_name )'
```
(If both `definition` and `derivation_expr` are NULL, the column is a source passthrough — its meaning
is its name.)

**R3 — a `subaward_search` column → def (matview dict) + DEC crosswalk (dual lookup).**
Column `sub_awardee_or_recipient_legal`:
```python
# a) matview dict — prose def + derivation:
ss.scanner(columns=["column_name","definition","derivation_expr"],
           filter="dataset='subaward_search' AND column_name='sub_awardee_or_recipient_legal'").to_table().to_pylist()
# → definition='The name of the subaward recipient …', derivation_expr='UPPER(COALESCE(
#   recipient_lookup.recipient_name, bs.sub_awardee_or_recipient_legal ))'
# b) DEC — the download-vocabulary element, join on dl_subaward_element:
dd.scanner(columns=["element","dl_subaward_element"],
           filter="dl_subaward_element = 'subawardee_name'").to_table().to_pylist()
# → element='SubAwardeeOrRecipientLegalEntityName'
```

## Access pattern

```python
import lance
from pipelines.bls.ingest import _storage_options
so = _storage_options()
dd = lance.dataset("s3://data-sink/active/usaspending_data_dictionary/", storage_options=so)
dd.scanner(columns=["element","definition","fpds_data_dictionary_element"],
           filter="dl_award_element = 'federal_action_obligation'").to_table().to_pylist()
```
Run live from `/Users/benjamincrane/core-x`:
```bash
PYTHONPATH=$(pwd) doppler run --project core-x --config prd -- \
  uv run --no-project --quiet --with 'pylance>=7' --with 'pyarrow>=17' python3 - <<'PY'
# … lance.dataset(...) …
PY
```

## Builders (regenerate — do not hand-edit)

| Dictionaries | Builder (`<build\|verify>`) |
|---|---|
| `usaspending_data_dictionary`, `sam_entity_extract_dictionary`, `sam_fal_data_dictionary` | `pipelines/reference/materialize_gov_data_dictionaries.py` |
| `usaspending_search_schema_dictionary` | `pipelines/reference/materialize_usaspending_search_schema.py` |
| `sam_reps_certs_provisions` | `pipelines/reference/materialize_sam_reps_certs.py` |
| `fpds_atom_feed_spec` | `pipelines/reference/materialize_fpds_atom_feed_spec.py` |

## See also

- [`GOV_DATA_DICTIONARIES.md`](GOV_DATA_DICTIONARIES.md) — per-table schema, index list, provenance, fidelity verification.
- [`00_ACTIVE_SINK_CATALOG.md`](00_ACTIVE_SINK_CATALOG.md) — full sink catalog (these six under **Reference** + **SAM.gov**).
- [`FPDS_CANONICAL_FIELD_DICTIONARY.md`](FPDS_CANONICAL_FIELD_DICTIONARY.md) / [`SUBAWARD_CANONICAL_FIELD_DICTIONARY.md`](SUBAWARD_CANONICAL_FIELD_DICTIONARY.md) — the canonical spine column dictionaries these resolve against.
