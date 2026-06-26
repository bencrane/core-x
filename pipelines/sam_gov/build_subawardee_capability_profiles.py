"""Build worker — govcon_subawardee_profiles: the SUB-side GTM target list (build plan
PHASE 4 sub pivot, extended to a durable per-sub profile).

One row per subawardee UEI in the tracked-solicitation universe, fusing three signals + a contact
into a deterministic, idempotent, citeable capability profile:

    contract_subaward                 (LEAD — what the sub actually did: subaward_description, $,
                                       NAICS, which primes, action dates; sub-self-reported)
  ⊕ govcon_award_requirements   (ENRICH — the prime SCOPE worked under: clearance/cert/labor
    + govcon_doc_scope           rollups + capability_tags + scope_summary, validated rows)
  ⊕ govcon_teaming_edges        (TEAM — $/count/NAICS/which primes over the 5y teaming corpus)
  ⊕ sam_pocs                          (REACH — best government-business POC per sub_uei)

UNIVERSE  distinct `subawardee_uei` in `subawardee_solicitations_bridge` (the subs that teamed under
        the harvested prime solicitations). The bridge also supplies `sub_action_date` (the window
        column) and the `notice_id` map; manifest resolves `notice_id → resource_id` for the scope
        rollup. Measured 2026-06-15: 6,586 sub UEIs over 692 notices.

GRAIN   exactly one row per distinct `sub_uei`. DoD asserts profile_rows == distinct bridge sub UEIs.

SoR     s3://data-sink/active/govcon_subawardee_profiles/ (Lance v2.1; derived, OVERWRITE).
        No window suffix (operator naming decision): the window is DATA — `sub_action_date` +
        `first_sub_action_date` + `built_at`. Frozen schema + assert_schema (govcon_gtm_schemas.py)
        BEFORE the first commit.

IDEMPOTENT  overwrite-mode snapshot; deterministic rollups (no wall-clock in the data except the run
        stamps). `verify --content-hash` hashes every business column EXCLUDING the two run stamps,
        so a re-run over the same upstream is provably zero-delta (DoD).

CUI EGRESS INVARIANT (anti-pattern #10)  carries NO verbatim solicitation chunk text. `scope_summary`
        sourced only from `govcon_doc_scope` (marked_resource=false by construction);
        requirement rollups are normalized/controlled-vocab; `source_chunk_ids`/`source_resource_ids`
        are populated from UNMARKED validated rows only; `marked_solicitation` is audit provenance.
        `build()` asserts (a) doc_scope has zero marked rows and (b) marked requirement rows carry no
        verbatim text, then refuses to write on violation. `subaward_description`/`subaward_numbers`
        are sub-self-reported (the firm's own SAM subaward report) — the LEAD evidence, not CUI.

INDICES  BTREE(sub_uei); BITMAP(requires_clearance, requires_cmmc, has_extracted_scope, poc_available,
        marked_solicitation, req_clearance_level_max).

    doppler run -- .venv/bin/python pipelines/sam_gov/build_subawardee_capability_profiles.py <cmd>
      build               assemble + assert_schema + overwrite + index
      verify              row count, universe parity, signal coverage, CUI checks, index list
      verify --content-hash   + business-column content hash (idempotency proof)
      query               sub north-star: do X ∧ under awards requiring A∧B ∧ teaming P ∧ reachable POC
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines.sam_gov.govcon_gtm_schemas import (  # noqa: E402
    SUB_CAPABILITY_PROFILES_URI, subawardee_capability_profiles_schema, assert_schema)
from pipelines.sam_gov.sam_attachment_extract import (  # noqa: E402
    _r2_storage_options, _dataset_exists)

ACTIVE = "s3://data-sink/active"
BRIDGE_URI = f"{ACTIVE}/subawardee_solicitations_bridge/"
MANIFEST_URI = f"{ACTIVE}/subawardee_solicitations_manifest/"
REQUIREMENTS_URI = f"{ACTIVE}/govcon_award_requirements/"
DOC_SCOPE_URI = f"{ACTIVE}/govcon_doc_scope/"
CONTRACT_SUBAWARD_URI = f"{ACTIVE}/usaspending_api_fresh/contract_subaward/"
TEAMING_URI = f"{ACTIVE}/govcon_teaming_edges/"
POCS_URI = f"{ACTIVE}/sam_pocs/"
SELF_TAGS_URI = f"{ACTIVE}/govcon_sub_self_reported_tags/"   # Path B sidecar (desc_sha → tags)
SELF_TAG_DESC_CHARS = 600   # MUST match classify_sub_self_reported_tags.MAX_DESC_CHARS (join key)

DATA_STORAGE_VERSION = "2.1"
BTREE_INDEXES = ["sub_uei"]
BITMAP_INDEXES = ["requires_clearance", "requires_cmmc", "has_extracted_scope", "poc_available",
                  "marked_solicitation", "req_clearance_level_max", "tag_source",
                  "hq_state", "pop_state"]   # geo filter axes (low-cardinality)

# Deterministic per-sub list caps (bound payload; coverage_truncated set when a cap actually clips).
TAG_CAP = 30
CERT_CAP = 25
LABOR_CAP = 15
DESC_CAP = 20
NUM_CAP = 50
PRIME_CAP = 30
NAICS_CAP = 20
SRC_RES_CAP = 200
SRC_CHUNK_CAP = 100
NOTICE_CAP = 50
TEAM_PRIME_CAP = 50

# Clearance ordinal — higher = more sensitive (enum-locked in the requirements sink).
CLEARANCE_ORD = {"TS_SCI": 5, "TOP_SECRET": 4, "SECRET": 3, "CONFIDENTIAL": 2, "PUBLIC_TRUST": 1}
_ORD_TO_LEVEL = {v: k for k, v in CLEARANCE_ORD.items()}

DUCK_MEM = "8GB"
DUCK_TMP = "/tmp/sub_capability_profiles_duckdb"


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _duck():
    import os
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    os.makedirs(DUCK_TMP, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{DUCK_TMP}'")
    return con


def _clearance_ord_agg() -> str:
    """max clearance ordinal over CLEARANCE-typed, validated requirement rows (0 otherwise) — couples
    req_clearance_level_max to requires_clearance by construction (same rule as the prime builder)."""
    whens = " ".join(f"WHEN '{lvl}' THEN {o}" for lvl, o in CLEARANCE_ORD.items())
    return ("max(CASE WHEN requirement_type = 'clearance' AND validated "
            f"THEN (CASE clearance_level {whens} ELSE 0 END) ELSE 0 END)")


def _assemble(so):
    """Deterministic assembly → (arrow_table, stats). Lance scanners (scalar/list projection only —
    never `embedding`) with pushdown filters to keep the big feeds bounded → DuckDB → Arrow, cast to
    the frozen schema."""
    import lance
    import pyarrow as pa
    import pyarrow.compute as pc

    bridge = lance.dataset(BRIDGE_URI, storage_options=so)
    man = lance.dataset(MANIFEST_URI, storage_options=so)
    rq = lance.dataset(REQUIREMENTS_URI, storage_options=so)
    dsc = lance.dataset(DOC_SCOPE_URI, storage_options=so)
    csub = lance.dataset(CONTRACT_SUBAWARD_URI, storage_options=so)
    team = lance.dataset(TEAMING_URI, storage_options=so)
    pocs = lance.dataset(POCS_URI, storage_options=so)
    selftag = lance.dataset(SELF_TAGS_URI, storage_options=so)   # Path B sidecar

    # ── universe + sub→resource map (small feeds; full scans) ────────────────────────────────────
    bridge_tbl = bridge.scanner(columns=[
        "subawardee_uei", "notice_id", "sub_action_date"]).to_table()
    man_tbl = man.scanner(columns=["notice_id", "resource_id"]).to_table()
    sub_ueis = sorted({u for u in bridge_tbl.column("subawardee_uei").to_pylist() if u})
    bridge_notices = {n for n in bridge_tbl.column("notice_id").to_pylist() if n}
    # solicitation resources reachable from the tracked sub notices (bounds the requirements feeds)
    man_notice = man_tbl.column("notice_id").to_pylist()
    man_res = man_tbl.column("resource_id").to_pylist()
    sub_res_ids = sorted({r for n, r in zip(man_notice, man_res) if r and n in bridge_notices})
    log(f"universe: sub_ueis={len(sub_ueis):,} bridge_notices={len(bridge_notices):,} "
        f"sub_solicitation_resources={len(sub_res_ids):,}")

    # ── CUI pre-flight (refuse to build on violation; anti-pattern #10) ──────────────────────────
    n_scope_marked = dsc.count_rows(filter="marked_resource = true")
    if n_scope_marked:
        raise RuntimeError(
            f"CUI INVARIANT VIOLATION: govcon_doc_scope has {n_scope_marked} marked rows — "
            f"scope_summary/capability_tags would carry marked-doc content. Build refused.")
    n_marked_leak = rq.count_rows(
        filter="marked_resource = true AND (evidence_quote IS NOT NULL OR requirement_detail IS NOT NULL)")
    if n_marked_leak:
        raise RuntimeError(
            f"CUI INVARIANT VIOLATION: {n_marked_leak} marked requirement rows carry verbatim text. "
            f"Egress gate is write-side; build refused.")

    # ── bounded scans of the big feeds (pushdown isin filters) ───────────────────────────────────
    rq_tbl = rq.scanner(columns=[
        "resource_id", "requirement_type", "requirement_value", "clearance_level", "headcount",
        "validated", "marked_resource", "source_chunk_ids"],
        filter=pc.field("resource_id").isin(sub_res_ids)).to_table() if sub_res_ids else rq.scanner(
        columns=["resource_id", "requirement_type", "requirement_value", "clearance_level",
                 "headcount", "validated", "marked_resource", "source_chunk_ids"],
        filter=pc.field("resource_id").isin(["__none__"])).to_table()
    dsc_tbl = dsc.scanner(columns=[
        "resource_id", "scope_summary", "capability_tags", "validated", "marked_resource"],
        filter=pc.field("resource_id").isin(sub_res_ids)).to_table() if sub_res_ids else dsc.scanner(
        columns=["resource_id", "scope_summary", "capability_tags", "validated", "marked_resource"],
        filter=pc.field("resource_id").isin(["__none__"])).to_table()
    # PATH B widening: csub is the LEAD AND the universe source now (every sub with a subaward), so it is
    # scanned in FULL (not bridge-bounded). The scope/requirements feeds stay bridge-resource-bounded
    # (only bridge subs reach a solicitation resource); teaming + POC + the self-tag sidecar join onto
    # the full universe.
    csub_tbl = csub.scanner(columns=[
        "subawardee_uei", "subawardee_name", "subawardee_parent_uei", "subaward_description",
        "subaward_number", "subaward_amount", "subaward_action_date", "prime_awardee_uei",
        "prime_awardee_name", "prime_award_naics_code",
        "subawardee_state_code", "subawardee_city_name",            # HQ geo
        "subaward_primary_place_of_performance_state_code"]).to_table()  # place of performance
    # full universe = distinct csub subs ∪ bridge subs (bridge ⊂ csub except a tiny edge)
    full_sub_ueis = sorted({u for u in csub_tbl.column("subawardee_uei").to_pylist() if u} | set(sub_ueis))
    team_tbl = team.scanner(columns=[
        "sub_uei", "prime_uei", "prime_name", "edge_dollars_5y", "edge_count_5y", "top_naics",
        "last_action_date"], filter=pc.field("sub_uei").isin(full_sub_ueis)).to_table()
    poc_tbl = pocs.scanner(columns=[
        "uei", "poc_type", "poc_slot_no", "full_name", "title", "city", "state"],
        filter=pc.field("uei").isin(full_sub_ueis)).to_table()
    selftag_tbl = selftag.scanner(columns=[
        "description", "self_reported_capability_tags", "n_self_reported_tags"]).to_table()
    log(f"universe: full_sub_ueis={len(full_sub_ueis):,} (bridge={len(sub_ueis):,})")
    log(f"feeds: reqs={rq_tbl.num_rows:,} doc_scope={dsc_tbl.num_rows:,} "
        f"contract_subaward={csub_tbl.num_rows:,} teaming={team_tbl.num_rows:,} pocs={poc_tbl.num_rows:,} "
        f"self_tags={selftag_tbl.num_rows:,}")

    con = _duck()
    con.execute("PRAGMA threads=1")  # deterministic aggregation order (idempotency / zero-delta DoD)
    con.register("bridge", bridge_tbl)
    con.register("man", man_tbl)
    con.register("rq", rq_tbl)
    con.register("dsc", dsc_tbl)
    con.register("csub", csub_tbl)
    con.register("team", team_tbl)
    con.register("pocs", poc_tbl)
    con.register("selftag", selftag_tbl)

    clr = _clearance_ord_agg()
    ord_case = " ".join(f"WHEN {o} THEN '{lvl}'" for o, lvl in _ORD_TO_LEVEL.items())
    sql = f"""
    -- PATH B universe: every sub with a subaward description ∪ the bridge subs (the bridge is now an
    -- enrichment join, not the universe). Non-bridge subs get NULL scope-leg fields (correct).
    WITH universe AS (
        SELECT DISTINCT sub_uei FROM (
            SELECT subawardee_uei AS sub_uei FROM csub
            WHERE subawardee_uei IS NOT NULL AND subaward_description IS NOT NULL
              AND length(trim(subaward_description)) > 0
            UNION
            SELECT subawardee_uei AS sub_uei FROM bridge WHERE subawardee_uei IS NOT NULL
        )
    ),
    -- ── PATH B: self-reported capability tags (the sub's OWN descriptions → controlled vocab) ──────
    -- join csub descriptions to the LLM sidecar on the truncated-trimmed text (the classifier's key),
    -- union the tags per sub, cap at TAG_CAP. Separate from the scope-derived solicitation_scope_tags.
    selftag_pairs AS (
        SELECT DISTINCT subawardee_uei AS sub_uei,
               substr(trim(subaward_description), 1, {SELF_TAG_DESC_CHARS}) AS d
        FROM csub WHERE subawardee_uei IS NOT NULL AND subaward_description IS NOT NULL
          AND length(trim(subaward_description)) > 0
    ),
    selftag_unnest AS (
        SELECT DISTINCT p.sub_uei, t
        FROM selftag_pairs p JOIN selftag sc ON p.d = sc.description,
             UNNEST(sc.self_reported_capability_tags) AS x(t)
        WHERE sc.n_self_reported_tags > 0 AND t IS NOT NULL
    ),
    selftag_ranked AS (
        SELECT sub_uei, t, row_number() OVER (PARTITION BY sub_uei ORDER BY t) AS rk FROM selftag_unnest
    ),
    selftag_roll AS (
        SELECT sub_uei, list(t ORDER BY rk) FILTER (WHERE rk <= {TAG_CAP}) AS subaward_description_tags,
               CAST(count(*) AS INTEGER) AS n_subaward_description_tags
        FROM selftag_ranked GROUP BY 1
    ),
    -- ── GEO: HQ (sub's own address, dominant) + PoP (place of performance, dominant + distinct set) ──
    hq_counts AS (
        SELECT subawardee_uei AS sub_uei, subawardee_state_code AS st, subawardee_city_name AS ct, count(*) AS n
        FROM csub WHERE subawardee_uei IS NOT NULL AND subawardee_state_code IS NOT NULL
        GROUP BY 1, 2, 3
    ),
    hq_ranked AS (
        SELECT sub_uei, st, ct, row_number() OVER (PARTITION BY sub_uei ORDER BY n DESC, st, ct) AS rk FROM hq_counts
    ),
    hq_roll AS (
        SELECT sub_uei, max(CASE WHEN rk = 1 THEN st END) AS hq_state,
               max(CASE WHEN rk = 1 THEN ct END) AS hq_city FROM hq_ranked GROUP BY 1
    ),
    pop_counts AS (
        SELECT subawardee_uei AS sub_uei, subaward_primary_place_of_performance_state_code AS st, count(*) AS n
        FROM csub WHERE subawardee_uei IS NOT NULL AND subaward_primary_place_of_performance_state_code IS NOT NULL
        GROUP BY 1, 2
    ),
    pop_ranked AS (
        SELECT sub_uei, st, row_number() OVER (PARTITION BY sub_uei ORDER BY n DESC, st) AS rk FROM pop_counts
    ),
    pop_top AS (SELECT sub_uei, max(CASE WHEN rk = 1 THEN st END) AS pop_state FROM pop_ranked GROUP BY 1),
    pop_states_roll AS (
        SELECT sub_uei, list(st ORDER BY st) AS pop_states FROM (SELECT DISTINCT sub_uei, st FROM pop_counts) GROUP BY 1
    ),
    -- window column + notices from the bridge
    bridge_roll AS (
        SELECT subawardee_uei AS sub_uei,
               max(sub_action_date) AS sub_action_date,
               min(sub_action_date) AS first_sub_action_date
        FROM bridge GROUP BY 1
    ),
    -- sub → solicitation resource map (bridge notice → manifest resource)
    sub_res AS (
        SELECT DISTINCT b.subawardee_uei AS sub_uei, m.resource_id, m.notice_id
        FROM bridge b JOIN man m ON b.notice_id = m.notice_id
        WHERE m.resource_id IS NOT NULL
    ),
    -- ── LEAD: contract_subaward (all rows for the sub; the firm's full reported work) ───────────
    csub_norm AS (
        SELECT subawardee_uei AS sub_uei, subawardee_name, subawardee_parent_uei,
               subaward_description, subaward_number,
               TRY_CAST(subaward_amount AS DOUBLE) AS amt,
               TRY_CAST(subaward_action_date AS DATE) AS adate,
               prime_awardee_uei, prime_awardee_name, prime_award_naics_code
        FROM csub WHERE subawardee_uei IS NOT NULL
    ),
    lead_roll AS (
        SELECT sub_uei,
               CAST(count(*) AS INTEGER) AS n_subawards,
               CAST(count(DISTINCT prime_awardee_uei) AS INTEGER) AS n_distinct_primes_subaward,
               round(sum(amt), 2) AS total_subaward_amount,
               CAST(count(DISTINCT subaward_description) AS INTEGER) AS n_desc,
               CAST(count(DISTINCT subaward_number) AS INTEGER) AS n_num,
               CAST(count(DISTINCT prime_awardee_uei) AS INTEGER) AS n_prime
        FROM csub_norm GROUP BY 1
    ),
    -- latest-reported identity, deterministic tie-break (idempotency)
    name_ranked AS (
        SELECT sub_uei, subawardee_name,
               row_number() OVER (PARTITION BY sub_uei ORDER BY adate DESC NULLS LAST, subawardee_name) AS rk
        FROM csub_norm WHERE subawardee_name IS NOT NULL
    ),
    name_roll AS (SELECT sub_uei, max(CASE WHEN rk = 1 THEN subawardee_name END) AS sub_name FROM name_ranked GROUP BY 1),
    parent_ranked AS (
        SELECT sub_uei, subawardee_parent_uei,
               row_number() OVER (PARTITION BY sub_uei ORDER BY adate DESC NULLS LAST, subawardee_parent_uei) AS rk
        FROM csub_norm WHERE subawardee_parent_uei IS NOT NULL
    ),
    parent_roll AS (SELECT sub_uei, max(CASE WHEN rk = 1 THEN subawardee_parent_uei END) AS sub_parent_uei FROM parent_ranked GROUP BY 1),
    desc_ranked AS (
        SELECT sub_uei, subaward_description AS d,
               row_number() OVER (PARTITION BY sub_uei
                   ORDER BY length(subaward_description) DESC, subaward_description) AS rk
        FROM (SELECT DISTINCT sub_uei, subaward_description FROM csub_norm
              WHERE subaward_description IS NOT NULL)
    ),
    desc_roll AS (
        SELECT sub_uei, list(d ORDER BY rk) FILTER (WHERE rk <= {DESC_CAP}) AS subaward_descriptions,
               max(CASE WHEN rk = 1 THEN d END) AS top_subaward_description
        FROM desc_ranked GROUP BY 1
    ),
    num_ranked AS (
        SELECT sub_uei, subaward_number AS v,
               row_number() OVER (PARTITION BY sub_uei ORDER BY subaward_number) AS rk
        FROM (SELECT DISTINCT sub_uei, subaward_number FROM csub_norm WHERE subaward_number IS NOT NULL)
    ),
    num_roll AS (SELECT sub_uei, list(v ORDER BY rk) FILTER (WHERE rk <= {NUM_CAP}) AS subaward_numbers
                 FROM num_ranked GROUP BY 1),
    naics_ranked AS (
        SELECT sub_uei, v, row_number() OVER (PARTITION BY sub_uei ORDER BY v) AS rk
        FROM (SELECT DISTINCT sub_uei, prime_award_naics_code AS v FROM csub_norm WHERE prime_award_naics_code IS NOT NULL)
    ),
    naics_roll AS (SELECT sub_uei, list(v ORDER BY rk) FILTER (WHERE rk <= {NAICS_CAP}) AS subaward_naics_codes
                   FROM naics_ranked GROUP BY 1),
    sprime_ranked AS (
        SELECT sub_uei, u, nm, row_number() OVER (PARTITION BY sub_uei ORDER BY u) AS rk
        FROM (SELECT DISTINCT sub_uei, prime_awardee_uei AS u, prime_awardee_name AS nm
              FROM csub_norm WHERE prime_awardee_uei IS NOT NULL)
    ),
    sprime_roll AS (
        SELECT sub_uei,
               list(u  ORDER BY rk) FILTER (WHERE rk <= {PRIME_CAP}) AS subaward_prime_ueis,
               list(nm ORDER BY rk) FILTER (WHERE rk <= {PRIME_CAP}) AS subaward_prime_names
        FROM sprime_ranked GROUP BY 1
    ),
    -- ── ENRICH: requirements rollup over the sub's solicitation resources ───────────────────────
    req_join AS (
        SELECT sr.sub_uei, r.resource_id, r.requirement_type, r.requirement_value, r.clearance_level,
               r.headcount, r.validated, r.marked_resource, r.source_chunk_ids
        FROM sub_res sr JOIN rq r ON sr.resource_id = r.resource_id
    ),
    req_roll AS (
        SELECT sub_uei,
               bool_or(requirement_type = 'clearance' AND validated) AS requires_clearance,
               {clr} AS clr_ord,
               bool_or(requirement_type = 'certification' AND validated
                       AND lower(requirement_value) IN ('cmmc','cmmc_l1','cmmc_l2','cmmc_l3')) AS requires_cmmc,
               bool_or(marked_resource) AS marked_solicitation,
               CAST(count(*) AS INTEGER) AS n_requirements,
               CAST(count(*) FILTER (WHERE validated) AS INTEGER) AS n_requirements_validated
        FROM req_join GROUP BY 1
    ),
    cert_ranked AS (
        SELECT sub_uei, v, row_number() OVER (PARTITION BY sub_uei ORDER BY v) AS rk
        FROM (SELECT DISTINCT sub_uei, requirement_value AS v FROM req_join
              WHERE requirement_type = 'certification' AND validated AND requirement_value IS NOT NULL)
    ),
    cert_roll AS (SELECT sub_uei, list(v ORDER BY rk) FILTER (WHERE rk <= {CERT_CAP}) AS req_cert_tags
                  FROM cert_ranked GROUP BY 1),
    labor_counts AS (
        SELECT sub_uei, requirement_value AS lc, count(*) AS n, sum(coalesce(headcount,0)) AS hc
        FROM req_join WHERE requirement_type = 'labor_category' AND validated AND requirement_value IS NOT NULL
        GROUP BY 1, 2
    ),
    labor_ranked AS (
        SELECT sub_uei, lc, row_number() OVER (PARTITION BY sub_uei ORDER BY n DESC, hc DESC, lc ASC) AS rk
        FROM labor_counts
    ),
    labor_roll AS (SELECT sub_uei, list(lc ORDER BY rk) FILTER (WHERE rk <= {LABOR_CAP}) AS top_labor_categories
                   FROM labor_ranked GROUP BY 1),
    -- cited evidence chunk ids — UNMARKED validated requirement rows only (CUI invariant)
    chunk_src AS (
        SELECT DISTINCT sub_uei, c AS chunk_id
        FROM (SELECT sub_uei, UNNEST(source_chunk_ids) AS c FROM req_join
              WHERE validated AND NOT coalesce(marked_resource, false))
        WHERE c IS NOT NULL
    ),
    chunk_ranked AS (SELECT sub_uei, chunk_id, row_number() OVER (PARTITION BY sub_uei ORDER BY chunk_id) AS rk FROM chunk_src),
    chunk_roll AS (SELECT sub_uei, list(chunk_id ORDER BY rk) FILTER (WHERE rk <= {SRC_CHUNK_CAP}) AS source_chunk_ids,
                          CAST(count(*) AS INTEGER) AS n_chunk FROM chunk_ranked GROUP BY 1),
    -- ── ENRICH: doc_scope rollup (capability tags + representative summary) ──────────────────────
    scope_join AS (
        SELECT sr.sub_uei, sr.resource_id, d.scope_summary, d.capability_tags, d.validated
        FROM sub_res sr JOIN dsc d ON sr.resource_id = d.resource_id
    ),
    tag_src AS (
        SELECT DISTINCT sub_uei, t FROM (SELECT sub_uei, UNNEST(capability_tags) AS t FROM scope_join WHERE validated) WHERE t IS NOT NULL
    ),
    tag_ranked AS (SELECT sub_uei, t, row_number() OVER (PARTITION BY sub_uei ORDER BY t) AS rk FROM tag_src),
    tag_roll AS (SELECT sub_uei, list(t ORDER BY rk) FILTER (WHERE rk <= {TAG_CAP}) AS solicitation_scope_tags FROM tag_ranked GROUP BY 1),
    summary_ranked AS (
        SELECT sub_uei, scope_summary,
               row_number() OVER (PARTITION BY sub_uei ORDER BY length(scope_summary) DESC, resource_id) AS rk
        FROM scope_join WHERE validated AND scope_summary IS NOT NULL
    ),
    summary_roll AS (SELECT sub_uei, max(CASE WHEN rk = 1 THEN scope_summary END) AS scope_summary FROM summary_ranked GROUP BY 1),
    scope_flags AS (
        SELECT sub_uei, CAST(count(*) FILTER (WHERE validated) AS INTEGER) AS n_scope_docs FROM scope_join GROUP BY 1
    ),
    -- distinct solicitation resources that carry ANY extracted signal (for n + cited drill-down)
    src_all AS (
        SELECT DISTINCT sub_uei, resource_id FROM (
            SELECT sub_uei, resource_id FROM req_join
            UNION SELECT sub_uei, resource_id FROM scope_join)
    ),
    src_cnt AS (SELECT sub_uei, CAST(count(*) AS INTEGER) AS n_scope_solicitations FROM src_all GROUP BY 1),
    src_ranked AS (SELECT sub_uei, resource_id, row_number() OVER (PARTITION BY sub_uei ORDER BY resource_id) AS rk FROM src_all),
    src_roll AS (SELECT sub_uei, list(resource_id ORDER BY rk) FILTER (WHERE rk <= {SRC_RES_CAP}) AS source_resource_ids FROM src_ranked GROUP BY 1),
    notice_src AS (SELECT DISTINCT sub_uei, notice_id FROM sub_res WHERE resource_id IN (SELECT resource_id FROM src_all)),
    notice_ranked AS (SELECT sub_uei, notice_id, row_number() OVER (PARTITION BY sub_uei ORDER BY notice_id) AS rk FROM notice_src),
    notice_roll AS (SELECT sub_uei, list(notice_id ORDER BY rk) FILTER (WHERE rk <= {NOTICE_CAP}) AS source_notice_ids FROM notice_ranked GROUP BY 1),
    -- ── TEAM: 5y teaming edges ──────────────────────────────────────────────────────────────────
    team_roll AS (
        SELECT sub_uei,
               CAST(count(DISTINCT prime_uei) AS INTEGER) AS n_teaming_primes,
               round(sum(edge_dollars_5y), 2) AS teaming_dollars_5y,
               CAST(sum(edge_count_5y) AS INTEGER) AS teaming_edge_count_5y,
               max(last_action_date) AS teaming_last_action_date
        FROM team GROUP BY 1
    ),
    tnaics_ranked AS (
        SELECT sub_uei, top_naics,
               row_number() OVER (PARTITION BY sub_uei ORDER BY d DESC, top_naics) AS rk
        FROM (SELECT sub_uei, top_naics, sum(edge_dollars_5y) AS d FROM team
              WHERE top_naics IS NOT NULL GROUP BY 1, 2)
    ),
    tnaics_roll AS (SELECT sub_uei, max(CASE WHEN rk = 1 THEN top_naics END) AS teaming_top_naics FROM tnaics_ranked GROUP BY 1),
    tprime_ranked AS (
        SELECT sub_uei, u, nm, row_number() OVER (PARTITION BY sub_uei ORDER BY dollars DESC, u) AS rk
        FROM (SELECT sub_uei, prime_uei AS u, min(prime_name) AS nm, sum(edge_dollars_5y) AS dollars
              FROM team WHERE prime_uei IS NOT NULL GROUP BY 1, 2)
    ),
    tprime_roll AS (
        SELECT sub_uei,
               list(u  ORDER BY rk) FILTER (WHERE rk <= {TEAM_PRIME_CAP}) AS teaming_prime_ueis,
               list(nm ORDER BY rk) FILTER (WHERE rk <= {TEAM_PRIME_CAP}) AS teaming_prime_names
        FROM tprime_ranked GROUP BY 1
    ),
    -- ── REACH: best POC per sub_uei ─────────────────────────────────────────────────────────────
    poc_ranked AS (
        SELECT uei, full_name, title, poc_type, city, state,
               row_number() OVER (PARTITION BY uei ORDER BY
                   (CASE WHEN poc_type = 'government_business' THEN 0
                         WHEN poc_type = 'electronic_business' THEN 1 ELSE 2 END),
                   coalesce(poc_slot_no, 9999), full_name) AS rk
        FROM pocs WHERE uei IS NOT NULL
    ),
    poc_roll AS (
        SELECT uei AS sub_uei, full_name AS poc_full_name, title AS poc_title, poc_type,
               city AS poc_city, state AS poc_state
        FROM poc_ranked WHERE rk = 1
    )
    SELECT
        u.sub_uei,
        nmr.sub_name, pnr.sub_parent_uei,
        coalesce(lr.n_subawards, 0) AS n_subawards,
        coalesce(lr.n_distinct_primes_subaward, 0) AS n_distinct_primes_subaward,
        lr.total_subaward_amount,
        br.sub_action_date, br.first_sub_action_date,
        nar.subaward_naics_codes,
        dr.top_subaward_description, dr.subaward_descriptions,
        nr.subaward_numbers,
        spr.subaward_prime_ueis, spr.subaward_prime_names,
        hqr.hq_state, hqr.hq_city, ptr.pop_state, psr.pop_states,
        (coalesce(sf.n_scope_docs, 0) > 0 OR coalesce(sc.n_scope_solicitations, 0) > 0) AS has_extracted_scope,
        coalesce(sc.n_scope_solicitations, 0) AS n_scope_solicitations,
        sumr.scope_summary,
        tgr.solicitation_scope_tags,
        coalesce(rr.requires_clearance, false) AS requires_clearance,
        CASE rr.clr_ord {ord_case} ELSE NULL END AS req_clearance_level_max,
        coalesce(rr.requires_cmmc, false) AS requires_cmmc,
        cr.req_cert_tags, lbr.top_labor_categories,
        coalesce(rr.n_requirements, 0) AS n_requirements,
        coalesce(rr.n_requirements_validated, 0) AS n_requirements_validated,
        srr.source_resource_ids, ckr.source_chunk_ids, ntr.source_notice_ids,
        coalesce(rr.marked_solicitation, false) AS marked_solicitation,
        srt.subaward_description_tags,
        coalesce(srt.n_subaward_description_tags, 0) AS n_subaward_description_tags,
        CASE
            WHEN tgr.solicitation_scope_tags IS NOT NULL AND len(tgr.solicitation_scope_tags) > 0
                 AND coalesce(srt.n_subaward_description_tags, 0) > 0 THEN 'both'
            WHEN tgr.solicitation_scope_tags IS NOT NULL AND len(tgr.solicitation_scope_tags) > 0 THEN 'scope'
            WHEN coalesce(srt.n_subaward_description_tags, 0) > 0 THEN 'self_reported'
            ELSE 'none'
        END AS tag_source,
        coalesce(tr.n_teaming_primes, 0) AS n_teaming_primes,
        tr.teaming_dollars_5y, coalesce(tr.teaming_edge_count_5y, 0) AS teaming_edge_count_5y,
        tnr.teaming_top_naics, tpr.teaming_prime_ueis, tpr.teaming_prime_names,
        tr.teaming_last_action_date,
        (pr.sub_uei IS NOT NULL) AS poc_available,
        pr.poc_full_name, pr.poc_title, pr.poc_type, pr.poc_city, pr.poc_state,
        (coalesce(lr.n_desc, 0) > {DESC_CAP} OR coalesce(lr.n_num, 0) > {NUM_CAP}
            OR coalesce(lr.n_prime, 0) > {PRIME_CAP} OR coalesce(sc.n_scope_solicitations, 0) > {SRC_RES_CAP}
            OR coalesce(ckr.n_chunk, 0) > {SRC_CHUNK_CAP}) AS coverage_truncated
    FROM universe u
    LEFT JOIN bridge_roll  br  USING (sub_uei)
    LEFT JOIN lead_roll    lr  USING (sub_uei)
    LEFT JOIN name_roll    nmr USING (sub_uei)
    LEFT JOIN parent_roll  pnr USING (sub_uei)
    LEFT JOIN desc_roll    dr  USING (sub_uei)
    LEFT JOIN num_roll     nr  USING (sub_uei)
    LEFT JOIN naics_roll   nar USING (sub_uei)
    LEFT JOIN sprime_roll  spr USING (sub_uei)
    LEFT JOIN req_roll     rr  USING (sub_uei)
    LEFT JOIN cert_roll    cr  USING (sub_uei)
    LEFT JOIN labor_roll   lbr USING (sub_uei)
    LEFT JOIN chunk_roll   ckr USING (sub_uei)
    LEFT JOIN tag_roll     tgr USING (sub_uei)
    LEFT JOIN summary_roll sumr USING (sub_uei)
    LEFT JOIN scope_flags  sf  USING (sub_uei)
    LEFT JOIN src_cnt      sc  USING (sub_uei)
    LEFT JOIN src_roll     srr USING (sub_uei)
    LEFT JOIN notice_roll  ntr USING (sub_uei)
    LEFT JOIN team_roll    tr  USING (sub_uei)
    LEFT JOIN tnaics_roll  tnr USING (sub_uei)
    LEFT JOIN tprime_roll  tpr USING (sub_uei)
    LEFT JOIN poc_roll     pr  USING (sub_uei)
    LEFT JOIN selftag_roll srt USING (sub_uei)
    LEFT JOIN hq_roll      hqr USING (sub_uei)
    LEFT JOIN pop_top      ptr USING (sub_uei)
    LEFT JOIN pop_states_roll psr USING (sub_uei)
    """
    tbl = con.execute(sql).to_arrow_table()

    import pyarrow.compute as pc2
    stats = {"rows": tbl.num_rows, "universe_sub_ueis": len(full_sub_ueis)}
    for c, key in [("has_extracted_scope", "has_extracted_scope"), ("requires_clearance", "requires_clearance"),
                   ("requires_cmmc", "requires_cmmc"), ("poc_available", "poc_available"),
                   ("marked_solicitation", "marked_solicitation"), ("coverage_truncated", "coverage_truncated")]:
        stats[key] = pc2.sum(tbl.column(c)).as_py() or 0
    stats["with_subaward_desc"] = pc2.sum(pc2.is_valid(tbl.column("top_subaward_description"))).as_py() or 0
    stats["with_teaming"] = int(pc2.sum(pc2.greater(tbl.column("n_teaming_primes"), 0)).as_py() or 0)
    stats["with_self_reported_tags"] = int(pc2.sum(pc2.greater(tbl.column("n_subaward_description_tags"), 0)).as_py() or 0)
    con.close()
    return tbl, stats


def _stamp(tbl, so):
    import lance
    import pyarrow as pa
    schema = subawardee_capability_profiles_schema()
    built_at = dt.datetime.now(dt.timezone.utc)
    versions = {name: lance.dataset(uri, storage_options=so).version for name, uri in (
        ("bridge", BRIDGE_URI), ("manifest", MANIFEST_URI), ("reqs", REQUIREMENTS_URI),
        ("scope", DOC_SCOPE_URI), ("csub", CONTRACT_SUBAWARD_URI), ("teaming", TEAMING_URI),
        ("pocs", POCS_URI))}
    snap = "|".join(f"{k}:v{v}" for k, v in versions.items())
    n = tbl.num_rows
    tbl = tbl.append_column("snapshot_run_id", pa.array([snap] * n, pa.string()))
    tbl = tbl.append_column("built_at", pa.array([built_at] * n, pa.timestamp("us", tz="UTC")))
    tbl = tbl.select(schema.names).cast(schema)
    return tbl, snap, built_at


def build():
    import lance
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    log("assembling subawardee capability profiles …")
    tbl, stats = _assemble(so)
    log(f"assembled {stats['rows']:,} sub rows · universe={stats['universe_sub_ueis']:,} "
        f"· with_subaward_desc={stats['with_subaward_desc']:,} with_teaming={stats['with_teaming']:,} "
        f"with_self_reported_tags={stats['with_self_reported_tags']:,} "
        f"· has_extracted_scope={stats['has_extracted_scope']:,} requires_clearance={stats['requires_clearance']:,} "
        f"requires_cmmc={stats['requires_cmmc']:,} poc_available={stats['poc_available']:,} "
        f"· marked_solicitation={stats['marked_solicitation']:,} coverage_truncated={stats['coverage_truncated']:,}")
    if stats["rows"] == 0:
        raise RuntimeError("zero sub profiles assembled")
    if stats["rows"] != stats["universe_sub_ueis"]:
        raise RuntimeError(
            f"GRAIN VIOLATION: {stats['rows']} rows != {stats['universe_sub_ueis']} distinct universe sub UEIs "
            f"(desc-subs ∪ bridge; the build must be exactly one row per sub_uei).")

    tbl, snap, built_at = _stamp(tbl, so)
    schema = subawardee_capability_profiles_schema()
    if not tbl.schema.equals(schema, check_metadata=False):
        raise RuntimeError("assembled table schema != frozen subawardee_capability_profiles_schema()")

    log(f"writing OVERWRITE → {SUB_CAPABILITY_PROFILES_URI}  (snapshot {snap})")
    lance.write_dataset(tbl, SUB_CAPABILITY_PROFILES_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    assert_schema(SUB_CAPABILITY_PROFILES_URI, schema, so)
    ds = lance.dataset(SUB_CAPABILITY_PROFILES_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True); log(f"  BTREE  ✓ {col}")
    for col in BITMAP_INDEXES:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True); log(f"  BITMAP ✓ {col}")
    elapsed = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
    log(f"DONE → {SUB_CAPABILITY_PROFILES_URI} rows={stats['rows']:,} in {elapsed:.0f}s")
    return {**stats, "snapshot": snap, "built_at": built_at.isoformat(), "elapsed_s": round(elapsed, 1)}


def _content_hash(con, rel: str) -> str:
    cols = [n for n in subawardee_capability_profiles_schema().names
            if n not in ("built_at", "snapshot_run_id")]
    proj = ", ".join(cols)
    return con.execute(
        f"WITH b AS (SELECT {proj} FROM {rel}) "
        f"SELECT md5(string_agg(md5(to_json(b)), '' ORDER BY sub_uei)) FROM b").fetchone()[0]


def verify(content_hash: bool = False):
    import lance
    so = _r2_storage_options()
    ds = lance.dataset(SUB_CAPABILITY_PROFILES_URI, storage_options=so)
    schema = subawardee_capability_profiles_schema()
    assert_schema(SUB_CAPABILITY_PROFILES_URI, schema, so)
    rows = ds.count_rows()
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []

    bridge = lance.dataset(BRIDGE_URI, storage_options=so)
    csub = lance.dataset(CONTRACT_SUBAWARD_URI, storage_options=so)
    con = _duck()
    con.register("bridge", bridge.scanner(columns=["subawardee_uei"]).to_table())
    con.register("csub", csub.scanner(columns=["subawardee_uei", "subaward_description"]).to_table())
    con.register("prof", ds.scanner().to_table())
    # PATH B universe = distinct csub desc-subs ∪ bridge subs
    universe = con.execute(
        "SELECT count(*) FROM (SELECT subawardee_uei AS u FROM csub WHERE subawardee_uei IS NOT NULL "
        "AND subaward_description IS NOT NULL AND length(trim(subaward_description))>0 "
        "UNION SELECT subawardee_uei FROM bridge WHERE subawardee_uei IS NOT NULL)").fetchone()[0]
    bridge_universe = con.execute("SELECT count(DISTINCT subawardee_uei) FROM bridge WHERE subawardee_uei IS NOT NULL").fetchone()[0]
    distinct_ueis = con.execute("SELECT count(DISTINCT sub_uei) FROM prof").fetchone()[0]
    # CUI consistency checks
    clr_no_flag = con.execute("SELECT count(*) FROM prof WHERE req_clearance_level_max IS NOT NULL AND requires_clearance = false").fetchone()[0]
    scope_no_flag = con.execute("SELECT count(*) FROM prof WHERE scope_summary IS NOT NULL AND has_extracted_scope = false").fetchone()[0]
    # tag_source distribution + self-reported coverage
    tag_src = {r[0]: r[1] for r in con.execute("SELECT tag_source, count(*) FROM prof GROUP BY 1").fetchall()}

    out = {
        "uri": SUB_CAPABILITY_PROFILES_URI, "rows": rows,
        "universe_sub_ueis": universe, "bridge_sub_ueis": bridge_universe, "distinct_sub_ueis": distinct_ueis,
        "row_eq_universe": rows == universe, "row_eq_distinct_uei": rows == distinct_ueis,
        "with_subaward_desc": ds.count_rows(filter="top_subaward_description IS NOT NULL"),
        "with_teaming": ds.count_rows(filter="n_teaming_primes > 0"),
        "with_self_reported_tags": ds.count_rows(filter="n_subaward_description_tags > 0"),
        "with_hq_state": ds.count_rows(filter="hq_state IS NOT NULL"),
        "with_pop_state": ds.count_rows(filter="pop_state IS NOT NULL"),
        "tag_source": tag_src,
        "has_extracted_scope": ds.count_rows(filter="has_extracted_scope = true"),
        "requires_clearance": ds.count_rows(filter="requires_clearance = true"),
        "requires_cmmc": ds.count_rows(filter="requires_cmmc = true"),
        "poc_available": ds.count_rows(filter="poc_available = true"),
        "marked_solicitation": ds.count_rows(filter="marked_solicitation = true"),
        "coverage_truncated": ds.count_rows(filter="coverage_truncated = true"),
        "columns": len(schema.names), "indices": sorted(idx), "schema_assert": "PASS",
        "clearance_level_without_flag": clr_no_flag,   # expect 0
        "scope_summary_without_flag": scope_no_flag,    # expect 0
    }
    if content_hash:
        out["content_hash_excl_stamps"] = _content_hash(con, "prof")
    con.close()
    print(json.dumps(out, indent=2, default=str))
    return out


def query():
    """Sub north-star (DoD): subawardees who do [capability] (tag or description ILIKE), under awards
    requiring [A ∧ B] (clearance ≥ X ∧ CMMC), teaming with [prime] (optional), reachable via [POC].
    Env-overridable: NS_TAG, NS_DESC, NS_MIN_CLEARANCE, NS_REQUIRE_CMMC, NS_PRIME_UEI, NS_REQUIRE_POC, NS_LIMIT."""
    import os
    import lance
    so = _r2_storage_options()
    tag = os.environ.get("NS_TAG", "")
    desc = os.environ.get("NS_DESC", "")
    min_clr = os.environ.get("NS_MIN_CLEARANCE", "")
    require_cmmc = os.environ.get("NS_REQUIRE_CMMC", "0") == "1"
    prime_uei = os.environ.get("NS_PRIME_UEI", "")
    require_poc = os.environ.get("NS_REQUIRE_POC", "1") == "1"
    limit = int(os.environ.get("NS_LIMIT", "10"))

    ds = lance.dataset(SUB_CAPABILITY_PROFILES_URI, storage_options=so)
    con = _duck()
    con.register("prof", ds.scanner().to_table())
    where = []
    if tag:
        where.append(f"list_contains(solicitation_scope_tags, '{tag}')")
    if desc:
        d = desc.replace("'", "''")
        where.append(f"(lower(top_subaward_description) LIKE '%{d.lower()}%' "
                     f"OR len(list_filter(subaward_descriptions, x -> lower(x) LIKE '%{d.lower()}%')) > 0)")
    if min_clr:
        allowed = sorted(l for l, o in CLEARANCE_ORD.items() if o >= CLEARANCE_ORD.get(min_clr, 3))
        where.append("requires_clearance = true")
        where.append("req_clearance_level_max IN (" + ", ".join(f"'{l}'" for l in allowed) + ")")
    if require_cmmc:
        where.append("requires_cmmc = true")
    if prime_uei:
        where.append(f"(list_contains(teaming_prime_ueis, '{prime_uei}') OR list_contains(subaward_prime_ueis, '{prime_uei}'))")
    if require_poc:
        where.append("poc_available = true")
    clause = " AND ".join(where) if where else "true"

    total = con.execute(f"SELECT count(*) FROM prof WHERE {clause}").fetchone()[0]
    rowset = con.execute(f"""
        SELECT sub_uei, sub_name, n_subawards, total_subaward_amount,
               top_subaward_description, solicitation_scope_tags, req_clearance_level_max, requires_cmmc,
               req_cert_tags, top_labor_categories, n_teaming_primes, teaming_prime_names,
               source_resource_ids, source_chunk_ids, subaward_numbers, marked_solicitation,
               poc_full_name, poc_title, poc_city, poc_state
        FROM prof WHERE {clause}
        ORDER BY n_teaming_primes DESC, total_subaward_amount DESC NULLS LAST, sub_uei
        LIMIT {limit}
    """).fetchall()
    cols = [d[0] for d in con.description]
    print(json.dumps({
        "query": {"solicitation_scope_tag": tag or None, "description_ilike": desc or None,
                  "min_clearance": min_clr or None, "require_cmmc": require_cmmc,  # query echo
                  "teaming_prime_uei": prime_uei or None, "require_poc": require_poc},
        "matched_subs": total, "showing": len(rowset)}, indent=2))
    for r in rowset:
        d = dict(zip(cols, r))
        print("\n" + "=" * 92)
        print(f"  {d['sub_name']}  (UEI {d['sub_uei']})")
        print(f"  subawards={d['n_subawards']}  total=${d['total_subaward_amount'] or 0:,.0f}  "
              f"teaming_primes={d['n_teaming_primes']}  marked={d['marked_solicitation']}")
        if d.get("top_subaward_description"):
            print(f"  does: {str(d['top_subaward_description'])[:240]}")
        print(f"  scope: tags={d['solicitation_scope_tags']} clearance={d['req_clearance_level_max']} "
              f"cmmc={d['requires_cmmc']} certs={d['req_cert_tags']} labor={d['top_labor_categories']}")
        print(f"  teaming: {d['teaming_prime_names']}")
        print(f"  CITED: subaward_numbers={(d['subaward_numbers'] or [])[:3]} "
              f"solicitation_resources={(d['source_resource_ids'] or [])[:3]} "
              f"chunks={(d['source_chunk_ids'] or [])[:3]}")
        poc = " ".join(str(x) for x in [d.get('poc_full_name'), f"({d.get('poc_title')})" if d.get('poc_title') else "",
                                        d.get('poc_city'), d.get('poc_state')] if x)
        print(f"  REACH: {poc or '(no POC)'}")
    con.close()


def main():
    p = argparse.ArgumentParser(description="Build govcon_subawardee_profiles (sub_uei grain).")
    p.add_argument("cmd", choices=["build", "verify", "query"])
    p.add_argument("--content-hash", action="store_true", help="verify: include idempotency hash")
    a = p.parse_args()
    if a.cmd == "build":
        print(json.dumps(build(), indent=2, default=str))
    elif a.cmd == "verify":
        verify(content_hash=a.content_hash)
    elif a.cmd == "query":
        query()


if __name__ == "__main__":
    main()
