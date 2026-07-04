# `dec_code_domain_ref` — the canonical code dimension (Layer 0 bedrock)

**SoR:** `s3://data-sink/active/dec_code_domain_ref/` (Lance v2.1) · **Loader:** [`pipelines/reference/materialize_dec_code_domain_ref.py`](../../pipelines/reference/materialize_dec_code_domain_ref.py) · **Source:** DEC (`usaspending_data_dictionary`) v31 · **Rows:** 625 · **Built:** 2026-07-04

## Why this exists

Government codes are **namespace-scoped**: a bare `A` means "BPA Call" (`award_type`), "GWAC" (`idv_type`), "Fixed Price Redetermination" (`type_of_contract_pricing`), "Additional Work" (`action_type`/Contracts) **or** "New" (`action_type`/Assistance). The DEC holds all of it but only as unstructured `domain_values` **blobs** — one `\n`-delimited string per element, with no per-code queryability, and 2 elements (`ActionType` + its tag twin) packing two code namespaces (`Assistance:` / `Contracts:`) into one blob.

`dec_code_domain_ref` refines those blobs into a normalized `(db_element, sub_domain, code) → verbatim description` dimension. It is **Layer 0**: the bedrock every code column resolves through and every derived taxonomy validates against. The `action_type='Y' → 'nonstandard'` bug was a *guess*; against this dim it is a one-row lookup (`Y = "ADD SUBCONTRACT PLAN"`).

## Grain & schema

One row per **`(db_element, sub_domain, code)`**.

| column | note |
|---|---|
| `element` / `db_element` | DEC element name / the data column it maps to (**join key**) |
| `sub_domain` | `'Contracts'` / `'Assistance'` for the dual-namespace `action_type`; `''` otherwise |
| `code` / `description` | e.g. `Y` / `ADD SUBCONTRACT PLAN` (verbatim government string) |
| `code_description` | longer per-code explanation where the DEC supplies one |
| `is_boolean` | domain is a `Y/N` or `T/F` flag (94 such domains) |
| `fpds_element`, `grouping`, `dec_row_ord`, `dec_version`, `source`, `source_vintage`, `ingested_at` | context / provenance |

**Indices:** BTREE `db_element`, `code`, `element` · BITMAP `sub_domain`, `grouping`, `is_boolean`.

## Resolution pattern

```sql
-- resolve a code IN ITS NAMESPACE
SELECT description FROM dec_code_domain_ref
WHERE db_element = 'action_type' AND sub_domain = 'Contracts' AND code = 'Y';
--> ADD SUBCONTRACT PLAN
```

The same letter, correctly namespaced (materialized proof):

| code | `action_type` | `type_of_contract_pricing` | `idv_type` | `contract_award_type` |
|---|---|---|---|---|
| `Y` | ADD SUBCONTRACT PLAN | TIME AND MATERIALS | — | — |
| `A` | New *(Assistance)* / Additional Work *(Contracts)* | Fixed Price Redetermination | GWAC | BPA Call |

## Build & fail-closed gates

```
doppler run -p core-x -c prd -- python3 pipelines/reference/materialize_dec_code_domain_ref.py <smoke|build|verify>
```

`smoke` parses the live DEC, runs every gate, writes a `_sample` copy, and exits 1 on any failure — **no `active/` write** until the gates pass. Gates:

- **Exact per-domain counts** on the load-bearing FPDS domains — `contract_award_type=4`, `idv_type=5`, `action_type`/Contracts`=21`, `action_type`/Assistance`=5`; `type_of_contract_pricing ≥ 15`. A DEC shape drift aborts rather than mislabels.
- **`(db_element, sub_domain, code)` uniqueness** (placeholders `N/A`/`(empty)`/`Blank`/`[Future Code(s)]` dropped, not emitted).
- **`Y = "ADD SUBCONTRACT PLAN"`** on `action_type`/Contracts.
- **Reconciliation** vs the retired `fpds_action_type_ref` (must agree on the 21 Contracts codes).

## Supersedes

Retires **`fpds_action_type_ref`** — that dataset is exactly `dec_code_domain_ref WHERE db_element='action_type' AND sub_domain='Contracts'`. The old loader is kept for lineage (deprecation banner, do-not-run); the live dataset is frozen pending consumer repoint + removal.

## Downstream (Layers 1–2)

- **Layer 1** — whitepaper structural invariants (mutual exclusivity, absence-semantics) captured as cited rules; constrain the derived columns, not this dim.
- **Layer 2** — derived pipeline columns (award topology, `action_type_klass`) **validate their codes against this dim** rather than asserting meanings. This is the substrate the FPDS L2 rebuild builds on.
