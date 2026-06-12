"""GovCon GTM frozen Lance schemas — Phase 0 item 4 of the build plan.

Build plan: docs/plans/GOVCON_SCOPE_PROCESSING_AND_GTM_QUERY_BUILD_PLAN.md (column/type tables are
the authority; this module is their executable form). Every net-new dataset in the artifact map is
created HERE as an EMPTY Lance v2.1 dataset with its full frozen schema — **every column either
lane will ever write, nullable, day one** — because Lance rejects column type changes on append
(the `sensitivity→content_marking` burn class) and `_ensure_dataset` silently no-ops on existing
datasets. `assert_schema(uri)` is therefore the ONLY drift detector: every writer must call it on
open, every build must call it before its first commit.

Frozen set (plan §2 artifact map):
  * govcon_award_requirements_90day        — Phase-1 requirement grain (merge_insert sink)
  * govcon_labor_demand_90day              — spec §3.6 exact (same Phase-1 pass)
  * govcon_requirements_extract_ledger_90day — plan §5 resource-grain ledger (merge on resource_id)
  * govcon_award_capability_profiles_90day — Phase-2 award grain (overwrite build); prime/txn-sourced
        columns typed per the RAW `usaspending_api_fresh/contract_prime_txn` schema (all string —
        probed live 2026-06-12); `pop_start`/`pop_end` carry the txn period_of_performance_*_date
        strings; value fields = the four obligation/value columns named below
  * govcon_teaming_edges_90day             — Phase-0 (prime_uei, sub_uei) grain (overwrite rebuild)
  * govcon_sub_targeting_90day             — Phase-4 (award, candidate_sub_uei) grain (snapshot-overwrite)
  * govcon_sub_capability_vectors_90day    — Phase-5 (subawardee_uei, description_chunk_ix) grain

Type law: `confidence` is float32 everywhere (regex rows write 1.0, never a string); dates are
date32 where the plan tables say date32; embeddings are fixed_size_list<float32>[1024]; chunk text
is large_string (int64 offsets — same rationale as the Stage-4 chunk sinks).

Run (idempotent ensure-all + schema assert + row counts):
    doppler run -- python pipelines/sam_gov/govcon_gtm_schemas.py
    doppler run -- python pipelines/sam_gov/govcon_gtm_schemas.py --assert-only
"""
from __future__ import annotations

import argparse
import os
import sys

from pipelines.sam_gov.sam_attachment_extract_90day import _dataset_exists, _r2_storage_options

# ── URIs (plan §2 artifact map; env-overridable for smoke) ───────────────────────────────────────
REQUIREMENTS_URI = os.environ.get(
    "GOVCON_REQUIREMENTS_URI", "s3://data-sink/active/govcon_award_requirements_90day/")
LABOR_DEMAND_URI = os.environ.get(
    "GOVCON_LABOR_DEMAND_URI", "s3://data-sink/active/govcon_labor_demand_90day/")
EXTRACT_LEDGER_URI = os.environ.get(
    "GOVCON_EXTRACT_LEDGER_URI", "s3://data-sink/active/govcon_requirements_extract_ledger_90day/")
CAPABILITY_PROFILES_URI = os.environ.get(
    "GOVCON_CAPABILITY_PROFILES_URI", "s3://data-sink/active/govcon_award_capability_profiles_90day/")
TEAMING_EDGES_URI = os.environ.get(
    "GOVCON_TEAMING_EDGES_URI", "s3://data-sink/active/govcon_teaming_edges_90day/")
SUB_TARGETING_URI = os.environ.get(
    "GOVCON_SUB_TARGETING_URI", "s3://data-sink/active/govcon_sub_targeting_90day/")
SUB_CAPABILITY_VECTORS_URI = os.environ.get(
    "GOVCON_SUB_CAPABILITY_VECTORS_URI", "s3://data-sink/active/govcon_sub_capability_vectors_90day/")


# ── Frozen schemas (plan column/type tables, verbatim) ───────────────────────────────────────────
def requirements_schema():
    """govcon_award_requirements_90day — requirement grain, merge_insert sink (plan Phase 1).
    `requirement_id` = sha256(resource_id|requirement_type|value_norm)[:24] — content hash, never
    ordinal. `evidence_quote`/`requirement_detail` are NULL at write for marked resources (egress
    gate is write-side)."""
    import pyarrow as pa
    return pa.schema([
        ("requirement_id", pa.string()),
        ("resource_id", pa.string()), ("notice_id", pa.string()),
        ("solicitation_number", pa.string()), ("naics_code", pa.string()),
        ("contract_award_unique_key", pa.string()),   # inline convenience key — joins use manifest explode
        ("requirement_type", pa.string()),            # enum, plan Phase-1 table
        ("requirement_value", pa.string()), ("requirement_detail", pa.string()),
        ("mandatory", pa.bool_()),
        ("headcount", pa.int32()),
        ("clearance_level", pa.string()),             # enum-locked
        ("pop_start", pa.date32()), ("pop_end", pa.date32()),
        ("place_of_performance_text", pa.string()),
        ("wage_floor", pa.float64()),
        ("source_chunk_ids", pa.list_(pa.string())),
        ("evidence_quote", pa.string()),              # ≤300 verbatim; NULL for marked resources
        ("validated", pa.bool_()), ("marked_resource", pa.bool_()), ("coverage_truncated", pa.bool_()),
        ("extractor", pa.string()), ("extractor_version", pa.string()),
        ("confidence", pa.float32()),                 # regex = 1.0
        ("run_id", pa.string()), ("created_at", pa.timestamp("us", tz="UTC")),
    ])


def labor_demand_schema():
    """govcon_labor_demand_90day — spec §3.6 exact, emitted by the same Phase-1 pass. `demand_id`
    ordinal is assigned deterministically by rank over (labor_category_norm, first chunk_ix)."""
    import pyarrow as pa
    return pa.schema([
        ("demand_id", pa.string()), ("resource_id", pa.string()),
        ("contract_award_unique_key", pa.string()), ("notice_id", pa.string()),
        ("solicitation_number", pa.string()), ("naics_code", pa.string()),
        ("labor_category", pa.string()), ("headcount", pa.int32()),
        ("clearance_level", pa.string()),
        ("pop_start", pa.date32()), ("pop_end", pa.date32()),
        ("place_of_performance", pa.string()), ("wage_floor", pa.float64()),
        ("source_chunk_ids", pa.list_(pa.string())),
        ("extractor", pa.string()), ("confidence", pa.float32()),
        ("run_id", pa.string()), ("created_at", pa.timestamp("us", tz="UTC")),
    ])


def extract_ledger_schema():
    """govcon_requirements_extract_ledger_90day — plan §5, resource grain, merge on resource_id.
    Per-lane states: regex_state ∈ {pending, done, quarantined, failed}; llm_state ∈ {pending,
    submitted, results_fetched, done, quarantined, failed, truncated, marked_local_only}. `batch_id`
    is the Batch-API resume key (crash between submit and fetch re-polls stored ids, never
    resubmits). `marking_full_body` is AUDIT PROVENANCE of the Phase-0 full-body marking pre-pass —
    the chunk-level `content_marking` stays the single egress enforcement point."""
    import pyarrow as pa
    return pa.schema([
        ("resource_id", pa.string()),
        ("regex_state", pa.string()), ("llm_state", pa.string()),
        ("batch_id", pa.string()),
        ("marking_full_body", pa.list_(pa.string())),
        ("lexicon_hit_fullbody", pa.bool_()),
        ("n_requirements_regex", pa.int32()), ("n_requirements_llm", pa.int32()),
        ("validation_pass_rate", pa.float64()),
        ("model", pa.string()), ("prompt_hash", pa.string()), ("extractor_version", pa.string()),
        ("run_id", pa.string()), ("completed_at", pa.timestamp("us", tz="UTC")),
    ])


def capability_profiles_schema():
    """govcon_award_capability_profiles_90day — award grain, overwrite build (plan Phase 2). Grain
    key populated by exploding manifest award_keys[] per resource, never the inline scalar. The
    prime-attribute block is `contract_prime_txn` collapsed to award grain and typed per that raw
    txn schema (all string — including pop/value fields; cast at query time, not at storage)."""
    import pyarrow as pa
    return pa.schema([
        ("contract_award_unique_key", pa.string()),
        # prime/txn-sourced (raw txn schema types: string)
        ("recipient_uei", pa.string()), ("recipient_name", pa.string()),
        ("recipient_parent_uei", pa.string()),
        ("naics_code", pa.string()), ("product_or_service_code", pa.string()),
        ("type_of_set_aside", pa.string()), ("awarding_agency_name", pa.string()),
        ("primary_place_of_performance_city_name", pa.string()),
        ("primary_place_of_performance_state_code", pa.string()),
        ("primary_place_of_performance_zip_4", pa.string()),
        ("primary_place_of_performance_country_code", pa.string()),
        ("pop_start", pa.string()),                   # = txn period_of_performance_start_date
        ("pop_end", pa.string()),                     # = txn period_of_performance_current_end_date
        ("federal_action_obligation", pa.string()),
        ("total_dollars_obligated", pa.string()),
        ("base_and_all_options_value", pa.string()),
        ("current_total_value_of_award", pa.string()),
        # LLM + requirement rollups (validated rows only)
        ("scope_summary", pa.string()),
        ("capability_tags", pa.list_(pa.string())),   # controlled vocabulary
        ("requires_clearance", pa.bool_()), ("req_clearance_level_max", pa.string()),
        ("requires_cmmc", pa.bool_()),
        ("req_cert_tags", pa.list_(pa.string())),
        ("top_labor_categories", pa.list_(pa.string())),
        ("n_requirements", pa.int32()), ("n_validated", pa.int32()),
        ("source_resource_ids", pa.list_(pa.string())),
        ("has_extracted_scope", pa.bool_()),
        ("marked_award", pa.bool_()), ("coverage_truncated", pa.bool_()),
        ("is_primary_target", pa.bool_()),
        ("txn_snapshot_run_id", pa.string()), ("built_at", pa.timestamp("us", tz="UTC")),
    ])


def teaming_edges_schema():
    """govcon_teaming_edges_90day — (prime_uei, sub_uei) grain, overwrite rebuild (plan Phase 0).
    Dollars/counts sourced from the 5y usaspending/subaward_search corpus."""
    import pyarrow as pa
    return pa.schema([
        ("prime_uei", pa.string()), ("sub_uei", pa.string()),
        ("prime_name", pa.string()), ("sub_name", pa.string()),
        ("edge_dollars_5y", pa.float64()),
        ("edge_count_5y", pa.int32()), ("distinct_awards_5y", pa.int32()),
        ("first_action_date", pa.date32()), ("last_action_date", pa.date32()),
        ("top_naics", pa.string()),
        ("run_id", pa.string()), ("built_at", pa.timestamp("us", tz="UTC")),
    ])


def sub_targeting_schema():
    """govcon_sub_targeting_90day — (contract_award_unique_key, candidate_sub_uei) grain,
    snapshot-overwrite (plan Phase 4). edge_type ∈ {direct_subaward, teaming_history,
    capability_match}; edge_dollars/count NULL for capability_match; matched_requirement_ids never
    empty. POC fields never materialize here — `poc_available` is the precomputed bool."""
    import pyarrow as pa
    return pa.schema([
        ("contract_award_unique_key", pa.string()), ("candidate_sub_uei", pa.string()),
        ("prime_uei", pa.string()), ("prime_name", pa.string()),
        ("edge_type", pa.string()),
        ("edge_dollars_5y", pa.float64()), ("edge_count_5y", pa.int32()),
        ("last_subaward_action_date", pa.date32()),
        ("matched_requirement_ids", pa.list_(pa.string())),
        ("sub_top_naics", pa.string()),
        ("capability_evidence", pa.string()),         # subaward_description excerpt (sub-self-reported)
        ("poc_available", pa.bool_()),
        ("built_at", pa.timestamp("us", tz="UTC")),
    ])


def sub_capability_vectors_schema():
    """govcon_sub_capability_vectors_90day — (subawardee_uei, description_chunk_ix) grain,
    overwrite per window (plan Phase 5). chunk_id = sha256(subawardee_uei|chunk_ix|text)[:24];
    embedding L2-normalized float32[1024] at write; model id+revision pinned per row."""
    import pyarrow as pa
    return pa.schema([
        ("subawardee_uei", pa.string()), ("description_chunk_ix", pa.int32()),
        ("chunk_id", pa.string()),
        ("text", pa.large_string()),                  # ≤1,200 chars, deduped descriptions
        ("char_len", pa.int32()),
        ("n_source_subawards", pa.int32()),
        ("embedding", pa.list_(pa.float32(), 1024)),
        ("model_id", pa.string()), ("model_revision", pa.string()),
        ("run_id", pa.string()), ("created_at", pa.timestamp("us", tz="UTC")),
    ])


# name → (uri, schema factory). The single registry every ensure/assert path iterates.
FROZEN: dict[str, tuple[str, callable]] = {
    "govcon_award_requirements_90day": (REQUIREMENTS_URI, requirements_schema),
    "govcon_labor_demand_90day": (LABOR_DEMAND_URI, labor_demand_schema),
    "govcon_requirements_extract_ledger_90day": (EXTRACT_LEDGER_URI, extract_ledger_schema),
    "govcon_award_capability_profiles_90day": (CAPABILITY_PROFILES_URI, capability_profiles_schema),
    "govcon_teaming_edges_90day": (TEAMING_EDGES_URI, teaming_edges_schema),
    "govcon_sub_targeting_90day": (SUB_TARGETING_URI, sub_targeting_schema),
    "govcon_sub_capability_vectors_90day": (SUB_CAPABILITY_VECTORS_URI, sub_capability_vectors_schema),
}


def _ensure_dataset(uri: str, schema, so: dict) -> bool:
    """Create an EMPTY Lance v2.1 dataset with the frozen schema if absent (idempotent; returns True
    when newly created). SILENTLY NO-OPS on an existing dataset — which is exactly why
    `assert_schema` exists and must be called by every writer on open. No indices here."""
    import lance
    if _dataset_exists(uri, so):
        return False
    lance.write_dataset(schema.empty_table(), uri, mode="create",
                        data_storage_version="2.1", storage_options=so)
    print(f"  created {uri}", flush=True)
    return True


def assert_schema(uri: str, schema, so: dict) -> None:
    """Raise on ANY drift between the live dataset schema and the frozen schema (field set, order,
    types, nullability; metadata ignored). This is the only drift detector — `_ensure_dataset`
    no-ops on existing datasets, so a silently-evolved sink would otherwise poison every append
    (Lance rejects type changes on append; anti-pattern #1)."""
    import lance
    ds = lance.dataset(uri, storage_options=so)
    if not ds.schema.equals(schema, check_metadata=False):
        live = {f.name: str(f.type) for f in ds.schema}
        frozen = {f.name: str(f.type) for f in schema}
        missing = sorted(set(frozen) - set(live))
        extra = sorted(set(live) - set(frozen))
        retyped = sorted(n for n in set(live) & set(frozen) if live[n] != frozen[n])
        raise RuntimeError(
            f"SCHEMA DRIFT on {uri}: missing={missing} extra={extra} "
            f"retyped={[(n, live[n], '!=', frozen[n]) for n in retyped]} "
            f"(or field-order/nullability drift). Frozen schema is the law "
            f"(govcon_gtm_schemas.py); re-materialize the dataset, never bend the schema.")


def ensure_all(so: dict, *, assert_only: bool = False) -> dict:
    """Ensure (unless assert_only) + schema-assert every frozen dataset; return per-dataset status."""
    import lance
    out: dict = {}
    for name, (uri, schema_fn) in FROZEN.items():
        schema = schema_fn()
        created = False if assert_only else _ensure_dataset(uri, schema, so)
        assert_schema(uri, schema, so)
        rows = lance.dataset(uri, storage_options=so).count_rows()
        out[name] = {"uri": uri, "created": created, "rows": rows, "schema": "PASS"}
        print(f"  {name}: rows={rows:,} schema=PASS{' (created)' if created else ''}", flush=True)
    return out


def _cli() -> None:
    p = argparse.ArgumentParser(description="GovCon GTM frozen schemas — ensure + assert (P0 item 4).")
    p.add_argument("--assert-only", action="store_true",
                   help="never create; fail if any dataset is absent or drifted")
    a = p.parse_args()
    so = _r2_storage_options()
    print(f"govcon_gtm_schemas: {'asserting' if a.assert_only else 'ensuring'} "
          f"{len(FROZEN)} frozen datasets ...", flush=True)
    try:
        ensure_all(so, assert_only=a.assert_only)
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: {exc}", flush=True)
        sys.exit(1)
    print("RESULT: all frozen datasets present, schema-assert PASS.", flush=True)


if __name__ == "__main__":
    _cli()
