"""GovCon GTM frozen Lance schemas — Phase 0 item 4 of the build plan.

Build plan: docs/plans/GOVCON_SCOPE_PROCESSING_AND_GTM_QUERY_BUILD_PLAN.md (column/type tables are
the authority; this module is their executable form). Every net-new dataset in the artifact map is
created HERE as an EMPTY Lance v2.1 dataset with its full frozen schema — **every column either
lane will ever write, nullable, day one** — because Lance rejects column type changes on append
(the `sensitivity→content_marking` burn class) and `_ensure_dataset` silently no-ops on existing
datasets. `assert_schema(uri)` is therefore the ONLY drift detector: every writer must call it on
open, every build must call it before its first commit.

Frozen set (plan §2 artifact map):
  * govcon_award_requirements        — Phase-1 requirement grain (merge_insert sink)
  * govcon_labor_demand              — spec §3.6 exact (same Phase-1 pass)
  * govcon_requirements_extract_ledger — plan §5 resource-grain ledger (merge on resource_id)
  * govcon_doc_scope                 — Phase-2 DOC-grain LLM outputs (scope_summary +
        capability_tags at resource grain; plan gap resolved per hard rule 4 — the plan routes
        doc-level outputs only to the award-grain profiles overwrite build, which needs a durable
        resource-grain source to roll up from)
  * govcon_award_solicitation_profiles    — Phase-2 award grain (overwrite build). **No window
        suffix (operator naming decision 2026-06-14, overrides the plan's _90day):** the window is
        carried as DATA — denormalized `award_last_modified_date` (the canonical window column) +
        `award_action_date` + a `built_at` run stamp — so "last 90 days" is a column filter and
        widening the window later is an append, not a new table. Supersedes the empty
        `govcon_award_capability_profiles_90day` P0 shell (dropped from this registry). prime/txn-
        sourced columns typed per the RAW `usaspending_api_fresh/contract_prime_txn` schema (all
        string — probed live 2026-06-14); `pop_start`/`pop_end` carry the txn
        period_of_performance_*_date strings; value fields = the four obligation/value columns below
  * govcon_teaming_edges             — Phase-0 (prime_uei, sub_uei) grain (overwrite rebuild)
  * govcon_sub_targeting             — Phase-4 (award, candidate_sub_uei) grain (snapshot-overwrite)
  * govcon_sub_capability_vectors    — Phase-5 (subawardee_uei, description_chunk_ix) grain

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
    "GOVCON_REQUIREMENTS_URI", "s3://data-sink/active/govcon_award_requirements/")
LABOR_DEMAND_URI = os.environ.get(
    "GOVCON_LABOR_DEMAND_URI", "s3://data-sink/active/govcon_labor_demand/")
EXTRACT_LEDGER_URI = os.environ.get(
    "GOVCON_EXTRACT_LEDGER_URI", "s3://data-sink/active/govcon_requirements_extract_ledger/")
DOC_SCOPE_URI = os.environ.get(
    "GOVCON_DOC_SCOPE_URI", "s3://data-sink/active/govcon_doc_scope/")
CAPABILITY_PROFILES_URI = os.environ.get(
    "GOVCON_CAPABILITY_PROFILES_URI", "s3://data-sink/active/govcon_award_solicitation_profiles/")
TEAMING_EDGES_URI = os.environ.get(
    "GOVCON_TEAMING_EDGES_URI", "s3://data-sink/active/govcon_teaming_edges/")
SUB_TARGETING_URI = os.environ.get(
    "GOVCON_SUB_TARGETING_URI", "s3://data-sink/active/govcon_sub_targeting/")
SUB_CAPABILITY_VECTORS_URI = os.environ.get(
    "GOVCON_SUB_CAPABILITY_VECTORS_URI", "s3://data-sink/active/govcon_sub_capability_vectors/")
SUB_CAPABILITY_PROFILES_URI = os.environ.get(
    "GOVCON_SUB_CAPABILITY_PROFILES_URI", "s3://data-sink/active/govcon_subawardee_profiles/")
SUB_CERTIFICATIONS_MV_URI = os.environ.get(
    "GOVCON_SUB_CERTIFICATIONS_MV_URI", "s3://data-sink/active/govcon_sub_certifications_mv/")


# ── Frozen schemas (plan column/type tables, verbatim) ───────────────────────────────────────────
def requirements_schema():
    """govcon_award_requirements — requirement grain, merge_insert sink (plan Phase 1).
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
    """govcon_labor_demand — spec §3.6 exact, emitted by the same Phase-1 pass. `demand_id`
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
    """govcon_requirements_extract_ledger — plan §5, resource grain, merge on resource_id.
    Per-lane states: regex_state ∈ {pending, done, quarantined, failed}; llm_state ∈ {pending,
    submitted, results_fetched, done, quarantined, failed, truncated, marked_local_only,
    excluded_marked, excluded_out_of_scope}. The two excluded_* values are the Phase-2 BRACKET
    record (operator decision A): `excluded_marked` = resource is in the P2 input set but carries a
    non-empty chunk `content_marking` (derived LIVE from the chunk sinks at bracket time — never
    enters any LLM/agent context); `excluded_out_of_scope` = resource is outside the P2 input set
    (unknown-sink lexicon-miss with zero regex hits). Both are reversible by re-running
    `--phase bracket`; together with `pending` they tile the non-terminal ledger exactly, so
    coverage accounting stays honest. `batch_id` is the Batch-API resume key (crash between submit
    and fetch re-polls stored ids, never resubmits). `marking_full_body` is AUDIT PROVENANCE of the
    Phase-0 full-body marking pre-pass — the chunk-level `content_marking` stays the single egress
    enforcement point."""
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


def doc_scope_schema():
    """govcon_doc_scope — DOC (resource) grain, merge_insert on resource_id (plan Phase 2;
    plan-gap resolution per hard rule 4: the plan's only doc-level destination is the award-grain
    profiles OVERWRITE build, which needs a durable resource-grain source — this sink is it).
    Written ONLY by the LLM lane (validator-passing results); idempotency = delete-by-resource_id
    then merge. `capability_tags` are controlled-vocabulary v1 ONLY (out-of-vocab tags are dropped
    and counted in `n_capability_tags_rejected`, never written). Bracketed (marked) resources never
    reach this sink in Phase 2, so `marked_resource` is False by construction — the column exists
    for future lanes (frozen day one). `confidence` < 1.0 always (LLM lane law)."""
    import pyarrow as pa
    return pa.schema([
        ("resource_id", pa.string()),                 # grain
        ("notice_id", pa.string()), ("solicitation_number", pa.string()),
        ("naics_code", pa.string()), ("contract_award_unique_key", pa.string()),
        ("scope_summary", pa.string()),
        ("capability_tags", pa.list_(pa.string())),   # controlled vocabulary v1 only
        ("n_capability_tags_rejected", pa.int32()),
        ("source_chunk_ids", pa.list_(pa.string())),  # the staged selection the outputs derive from
        ("n_chunks_total", pa.int32()), ("n_chunks_selected", pa.int32()),
        ("coverage_truncated", pa.bool_()),
        ("marked_resource", pa.bool_()), ("validated", pa.bool_()),
        ("extractor", pa.string()),                   # llm:<engine>@<prompt_version>
        ("extractor_version", pa.string()), ("prompt_hash", pa.string()),
        ("confidence", pa.float32()),
        ("run_id", pa.string()), ("created_at", pa.timestamp("us", tz="UTC")),
    ])


def capability_profiles_schema():
    """govcon_award_solicitation_profiles — award grain, overwrite build (plan Phase 2). THE PRODUCT
    (spec §10): one row per award at the EXPLODED `award_keys[]` grain (manifest explode per
    resource, never the inline scalar — 4.1× coverage; anti-pattern #5). Joins three sources:
    `govcon_doc_scope` (scope_summary + capability_tags), `govcon_award_requirements`
    (clearance/cert/labor rollups over `validated` rows), and `contract_prime_txn` collapsed to
    award grain (row_number() over last_modified_date DESC).

    WINDOW-AS-DATA (operator naming decision 2026-06-14): no `_90day` suffix. `award_last_modified_
    date` is the canonical denormalized window column (= txn `last_modified_date`), so "last 90 days"
    is a column filter and a wider window is an append, not a new table; `award_action_date` carries
    the obligation date for recency framing; `built_at` is the run stamp.

    Prime-attribute block typed per the raw txn schema (all string — including pop/value/date
    fields; cast at query time, not at storage). CUI egress invariant (anti-pattern #10): the
    profile carries NO verbatim chunk text — `scope_summary` derives only from `govcon_doc_scope_
    90day`, which is `marked_resource=false` by construction (marked docs bracketed from the LLM
    lane); `req_cert_tags`/`top_labor_categories`/`capability_tags` are normalized/controlled-vocab
    values, never raw quotes. `evidence_quote`/`requirement_detail` are deliberately absent."""
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
        ("award_last_modified_date", pa.string()),    # = txn last_modified_date — THE window column
        ("award_action_date", pa.string()),           # = txn action_date — obligation/recency
        ("federal_action_obligation", pa.string()),
        ("total_dollars_obligated", pa.string()),
        ("base_and_all_options_value", pa.string()),
        ("current_total_value_of_award", pa.string()),
        # LLM + requirement rollups (validated rows only)
        ("scope_summary", pa.string()),
        ("solicitation_scope_tags", pa.list_(pa.string())),   # controlled vocabulary
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
    """govcon_teaming_edges — (prime_uei, sub_uei) grain, overwrite rebuild (plan Phase 0).
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
    """govcon_sub_targeting — (contract_award_unique_key, candidate_sub_uei) grain,
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
    """govcon_sub_capability_vectors — (subawardee_uei, description_chunk_ix) grain,
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


def subawardee_capability_profiles_schema():
    """govcon_subawardee_profiles — SUBAWARDEE grain (one row per sub_uei), overwrite
    build. The sub-side analog of capability_profiles: the GTM target list of subawardees connected
    to tracked prime solicitations, describing what each sub DOES with cited evidence + an outreach
    contact. Universe = distinct `subawardee_uei` in `subawardee_solicitations_bridge` (the subs
    that teamed under the harvested prime solicitations).

    Fuses three signals + a contact, deterministically:
      LEAD   `usaspending_api_fresh/contract_subaward`  — the sub's actual reported work
             (`subaward_description`), $ volume, NAICS, which primes, action dates. Sub-self-reported
             text (the firm's own SAM subaward report), NOT solicitation CUI — safe to surface.
      ENRICH `govcon_award_requirements` + `govcon_doc_scope` rolled up over the
             solicitation resources the sub worked under (bridge `notice_id` → manifest `resource_id`):
             the prime SCOPE they operated under (clearance/cert/labor/capability tags). Validated
             rows only; controlled-vocab/normalized values only.
      TEAM   `govcon_teaming_edges` — $/count/NAICS/which primes (5y teaming corpus).
      REACH  `sam_pocs` — best government-business POC for the sub_uei (name/title/city/state).

    WINDOW-AS-DATA (same discipline as the prime table): no `_90day` suffix; `sub_action_date`
    (= max subaward action date) is the canonical denormalized window column, `first_sub_action_date`
    the floor, `built_at` the run stamp — so "last N days" is a column filter and a wider window is an
    append, not a new table.

    CUI EGRESS INVARIANT (anti-pattern #10): carries NO verbatim solicitation chunk text.
    `scope_summary` derives only from `govcon_doc_scope` (marked_resource=false by construction);
    requirement rollups are normalized/controlled-vocab; `source_chunk_ids`/`source_resource_ids` are
    IDs (drill-down pointers), populated from UNMARKED validated rows only. `marked_solicitation` is
    audit provenance that a marked source existed (its verbatim text is redacted upstream).
    `subaward_*` text is sub-self-reported and is the LEAD evidence (cited by `subaward_numbers`)."""
    import pyarrow as pa
    return pa.schema([
        # grain + identity
        ("sub_uei", pa.string()),
        ("sub_name", pa.string()), ("sub_parent_uei", pa.string()),
        # LEAD — subaward work (what the sub actually did); sub-self-reported, safe
        ("n_subawards", pa.int32()), ("n_distinct_primes_subaward", pa.int32()),
        ("total_subaward_amount", pa.float64()),
        ("sub_action_date", pa.date32()),             # = max subaward action date — THE window column
        ("first_sub_action_date", pa.date32()),
        ("subaward_naics_codes", pa.list_(pa.string())),
        ("top_subaward_description", pa.string()),    # representative; sub-self-reported
        ("subaward_descriptions", pa.list_(pa.string())),
        ("subaward_numbers", pa.list_(pa.string())),  # cited evidence for the lead work
        ("subaward_prime_ueis", pa.list_(pa.string())),
        ("subaward_prime_names", pa.list_(pa.string())),
        # GEO — denormalized from contract_subaward so geo is a first-class filter (no join). HQ = the
        # sub's own reported address (dominant); PoP = where the work was performed (dominant + the full
        # set of distinct states, for "performed anywhere in X" membership filters). HQ ≠ PoP in general.
        ("hq_state", pa.string()), ("hq_city", pa.string()),
        ("pop_state", pa.string()),                   # dominant place-of-performance state (by subaward count)
        ("pop_states", pa.list_(pa.string())),        # all distinct PoP states the sub performed in
        # ENRICH — solicitation scope worked under (prime scope), validated/controlled-vocab only
        ("has_extracted_scope", pa.bool_()),
        ("n_scope_solicitations", pa.int32()),
        ("scope_summary", pa.string()),               # from govcon_doc_scope (marked-free)
        ("solicitation_scope_tags", pa.list_(pa.string())),   # controlled vocabulary
        ("requires_clearance", pa.bool_()), ("req_clearance_level_max", pa.string()),
        ("requires_cmmc", pa.bool_()),
        ("req_cert_tags", pa.list_(pa.string())),
        ("top_labor_categories", pa.list_(pa.string())),
        ("n_requirements", pa.int32()), ("n_requirements_validated", pa.int32()),
        ("source_resource_ids", pa.list_(pa.string())),  # cited drill-down (solicitation resources)
        ("source_chunk_ids", pa.list_(pa.string())),     # cited evidence chunks (unmarked, validated)
        ("source_notice_ids", pa.list_(pa.string())),
        ("marked_solicitation", pa.bool_()),             # audit: a marked source existed (redacted)
        # PATH B — self-reported capability (the sub's OWN subaward descriptions, classified into the
        # controlled vocab via LLM). SEPARATE provenance from the scope-derived `capability_tags`:
        # "what the sub says it did" vs "the solicitation scope it teamed under named". Covers the full
        # 25,450-sub universe (vs the ~6,586 bridge subs with scope). `tag_source` ∈ scope|self_reported|
        # both|none marks which provenance populated a row's tag axes. (classify_sub_self_reported_tags.py)
        ("subaward_description_tags", pa.list_(pa.string())),
        ("n_subaward_description_tags", pa.int32()),
        ("tag_source", pa.string()),
        # TEAM — 5y teaming edges
        ("n_teaming_primes", pa.int32()),
        ("teaming_dollars_5y", pa.float64()), ("teaming_edge_count_5y", pa.int32()),
        ("teaming_top_naics", pa.string()),
        ("teaming_prime_ueis", pa.list_(pa.string())),
        ("teaming_prime_names", pa.list_(pa.string())),
        ("teaming_last_action_date", pa.date32()),
        # REACH — outreach POC (sam_pocs)
        ("poc_available", pa.bool_()),
        ("poc_full_name", pa.string()), ("poc_title", pa.string()), ("poc_type", pa.string()),
        ("poc_city", pa.string()), ("poc_state", pa.string()),
        # flags / provenance
        ("coverage_truncated", pa.bool_()),
        ("snapshot_run_id", pa.string()), ("built_at", pa.timestamp("us", tz="UTC")),
    ])


def sub_certifications_mv_schema():
    """govcon_sub_certifications_mv — UEI grain (one row per firm), snapshot-overwrite serving MV.
    Fuses DSBS adjudicated certifications (system of record) with SAM.gov self-certifications across
    the subawardee + targeting + greenfield-DSBS universe. Two STRICTLY DISJOINT namespaces:

      cert_*       ADJUDICATED — sourced ONLY from the DSBS `certs` JSON (status='Active' AND not
                   expired). A firm absent from DSBS has every cert_* false — SAM claims never
                   populate cert_* (the structural override). 8a/hubzone/wosb/edwosb/vosb/sdvosb
                   (+ 8a_jv; the two JV-only programs vosb_jv/sdvosb_jv are data-absent, not carried).
      sam_self_*   SELF-CERTIFIED — sourced ONLY from SAM `business_types` decoded via
                   `sam_business_type_code_dict` WHERE sba_administered=false AND the designation has
                   no DSBS-adjudicated equivalent → exactly {minority_owned(23), self_cert_sdb(27),
                   woman_owned/general(A2), veteran_owned/general(A5)}. 8W/8C/QF are excluded (DSBS
                   owns wosb/sdvosb). The two namespaces share zero designations (verify asserts it).

    DECOUPLED PAYLOAD: email/phone/contact_person/website are pulled NATIVELY from DSBS (no
    firmographics_blitz join); poc_full_name/poc_title fall back from govcon_subawardee_profiles for
    the sub-only tier that has no DSBS record. Geo coalesces DSBS → profiles hq_*.

    INDEX (zero-join frontend filtering): BTREE uei/naics_primary/zipcode/next_cert_expiration_date;
    BITMAP every membership/cert/self-cert bool + state/universe_tier/cert_lifecycle/contact_source/
    geo_source. cert_programs is audit-only (unindexed). Per-program exit-date BTREEs are deferred."""
    import pyarrow as pa
    return pa.schema([
        # identity + universe membership
        ("uei", pa.string()),
        ("legal_business_name", pa.string()),
        ("universe_tier", pa.string()),              # greenfield_dsbs | dsbs_and_sub | sub_only
        ("in_dsbs", pa.bool_()),
        ("is_subawardee", pa.bool_()),
        ("is_targeting_candidate", pa.bool_()),
        ("is_greenfield", pa.bool_()),
        # ADJUDICATED namespace (DSBS certs JSON, status='Active' ∧ not expired)
        ("cert_any", pa.bool_()),
        ("cert_count", pa.int32()),
        ("cert_programs", pa.string()),              # pipe list, audit/display (unindexed)
        ("cert_8a", pa.bool_()), ("cert_hubzone", pa.bool_()), ("cert_wosb", pa.bool_()),
        ("cert_edwosb", pa.bool_()), ("cert_vosb", pa.bool_()), ("cert_sdvosb", pa.bool_()),
        ("cert_8a_jv", pa.bool_()),
        ("cert_8a_exit_date", pa.date32()), ("cert_hubzone_exit_date", pa.date32()),
        ("cert_wosb_exit_date", pa.date32()), ("cert_edwosb_exit_date", pa.date32()),
        ("cert_vosb_exit_date", pa.date32()), ("cert_sdvosb_exit_date", pa.date32()),
        ("next_cert_expiration_date", pa.date32()),  # min future exit across active programs
        ("cert_lifecycle", pa.string()),             # none | expiring_90d | active
        # SELF-CERTIFIED namespace (SAM business_types ∩ dict, non-adjudicated-equivalent)
        ("sam_self_any", pa.bool_()),
        ("sam_self_minority_owned", pa.bool_()), ("sam_self_sdb", pa.bool_()),
        ("sam_self_woman_owned", pa.bool_()), ("sam_self_veteran_owned", pa.bool_()),
        ("sam_self_codes", pa.string()),             # retained codes, audit (asserted ∩ adjudicated = ∅)
        ("sam_registration_active", pa.bool_()),     # from sam_master_entities.is_active (self-cert-independent)
        ("sam_present", pa.bool_()),
        # decoupled contact payload (DSBS native; profiles POC fallback)
        ("email", pa.string()), ("phone", pa.string()), ("contact_person", pa.string()),
        # web identity — raw (display) + canonical normalized domains (core.web_norm; join keys to
        # sam_master_domains / pdl_normalized_companies). normalized_domain = best-of the two; NULL if
        # neither yields a plausible registrable host. domain_is_generic flags wix/godaddy/webmail.
        ("website", pa.string()), ("normalized_website", pa.string()),
        ("additional_website", pa.string()), ("normalized_additional_website", pa.string()),
        ("normalized_domain", pa.string()), ("domain_is_generic", pa.bool_()),
        ("poc_full_name", pa.string()), ("poc_title", pa.string()),
        ("contact_source", pa.string()),             # dsbs | subaward_poc | none
        # geo / firmographic (denormalized for filtering)
        ("state", pa.string()), ("city", pa.string()), ("county", pa.string()),
        ("zipcode", pa.string()), ("naics_primary", pa.string()),
        ("geo_source", pa.string()),                 # dsbs | subaward
        # subaward footprint
        ("has_subaward_history", pa.bool_()),
        ("n_subawards", pa.int32()), ("n_targeting_edges", pa.int32()),
        ("total_subaward_amount", pa.float64()),
        ("top_subaward_description", pa.string()),
        # provenance
        ("dsbs_snapshot", pa.string()),
        ("built_at", pa.timestamp("us", tz="UTC")),
    ])


# name → (uri, schema factory). The single registry every ensure/assert path iterates.
FROZEN: dict[str, tuple[str, callable]] = {
    "govcon_award_requirements": (REQUIREMENTS_URI, requirements_schema),
    "govcon_labor_demand": (LABOR_DEMAND_URI, labor_demand_schema),
    "govcon_requirements_extract_ledger": (EXTRACT_LEDGER_URI, extract_ledger_schema),
    "govcon_doc_scope": (DOC_SCOPE_URI, doc_scope_schema),
    "govcon_award_solicitation_profiles": (CAPABILITY_PROFILES_URI, capability_profiles_schema),
    "govcon_teaming_edges": (TEAMING_EDGES_URI, teaming_edges_schema),
    "govcon_sub_targeting": (SUB_TARGETING_URI, sub_targeting_schema),
    "govcon_sub_capability_vectors": (SUB_CAPABILITY_VECTORS_URI, sub_capability_vectors_schema),
    "govcon_subawardee_profiles": (SUB_CAPABILITY_PROFILES_URI,
                                              subawardee_capability_profiles_schema),
    "govcon_sub_certifications_mv": (SUB_CERTIFICATIONS_MV_URI, sub_certifications_mv_schema),
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
