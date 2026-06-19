"""Build worker — govcon_award_solicitation_profiles: THE PRODUCT (spec §10 / build plan PHASE 2).

One row per federal award at the EXPLODED `award_keys[]` grain, joining the three durable govcon
sinks into a single deterministic, idempotent, citeable capability profile:

    govcon_doc_scope            (scope_summary + controlled-vocab capability_tags; LLM lane)
  ⋈ govcon_award_requirements   (clearance / certification / labor rollups over validated rows)
  ⋈ contract_prime_txn               (recipient_uei/name, agency, NAICS/PSC, set-aside, PoP, value,
                                       dates — collapsed to award grain, never LLM-extracted)
  via sam_opps_attachment_manifest_winners (resource_id → award_keys[] explode; the 4.1×-
      coverage spine, NOT the inline scalar key — anti-pattern #5).

GRAIN   1 row per distinct EXPLODED `contract_award_unique_key` over the resources present in
        (requirements ∪ doc_scope). Measured 2026-06-14: 35,028 awards over 18,404 resources, 0
        manifest orphans. The DoD asserts profile_rows == distinct_exploded_keys and
        exploded ≥ inline-key floor (anti-pattern #5: a count near the inline floor means the
        fan-out got dropped).

SoR     s3://data-sink/active/govcon_award_solicitation_profiles/  (Lance v2.1; derived, OVERWRITE).
        **No window suffix (operator naming decision 2026-06-14, overrides the plan's _90day):** the
        window is DATA — `award_last_modified_date` (the canonical window column) + `award_action_
        date` + the `built_at` run stamp. "last 90 days" is a column filter; widening the window is
        an append, not a new table. Frozen schema + assert_schema (govcon_gtm_schemas.py) BEFORE the
        first commit; the empty `_90day` P0 shell is dropped (see `drop_legacy_shell`).

IDEMPOTENT  overwrite-mode snapshot; deterministic rollups (no Math.random, no wall-clock in the
        data except the run stamps). `verify --content-hash` hashes every business column EXCLUDING
        the two run stamps, so a re-run is provably zero-delta (DoD).

CUI EGRESS INVARIANT (anti-pattern #10)  the profile carries NO verbatim chunk text. `scope_summary`
        is sourced only from `govcon_doc_scope`, which is `marked_resource=false` by
        construction (marked docs are bracketed out of the LLM lane). `req_cert_tags` /
        `top_labor_categories` / `solicitation_scope_tags` are normalized / controlled-vocab values, never
        raw quotes. `evidence_quote` / `requirement_detail` are deliberately absent from the schema.
        `build()` asserts (a) doc_scope has zero marked rows and (b) requirements marked rows carry
        no verbatim text, then refuses to write on violation. Citations for serving resolve at query
        time: profile.source_resource_ids → govcon_award_requirements (evidence_quote +
        source_chunk_ids on UNMARKED rows only).

INDICES  BTREE(contract_award_unique_key, recipient_uei); BITMAP(requires_clearance, requires_cmmc,
        has_extracted_scope, marked_award, coverage_truncated, is_primary_target,
        req_clearance_level_max, type_of_set_aside).

    doppler run -p core-x -c prd -- .venv/bin/python \
      pipelines/sam_gov/build_award_capability_profiles.py <build|verify|query|drop_legacy_shell>

      build               assemble + assert_schema + overwrite + index
      verify              row count, exploded-key parity, txn resolution, CUI checks, index list
      verify --content-hash   + business-column content hash (idempotency proof)
      query               north-star degraded acceptance query (do X ∧ require A ∧ B → cited companies)
      drop_legacy_shell   delete the empty `..._90day` P0 shell (guarded: refuses if rows > 0)
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

# Repo root on sys.path so the shared govcon imports resolve whether run as a script or imported.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines.sam_gov.govcon_gtm_schemas import (  # noqa: E402
    CAPABILITY_PROFILES_URI, capability_profiles_schema, assert_schema)
from pipelines.sam_gov.sam_attachment_extract_90day import (  # noqa: E402
    _r2_storage_options, _dataset_exists)

ACTIVE = "s3://data-sink/active"
DOC_SCOPE_URI = f"{ACTIVE}/govcon_doc_scope/"
REQUIREMENTS_URI = f"{ACTIVE}/govcon_award_requirements/"
MANIFEST_URI = f"{ACTIVE}/sam_opps_attachment_manifest_winners/"
TXN_URI = f"{ACTIVE}/usaspending_api_fresh/contract_prime_txn/"
LEGACY_SHELL_URI = f"{ACTIVE}/govcon_award_capability_profiles_90day/"

DATA_STORAGE_VERSION = "2.1"
BTREE_INDEXES = ["contract_award_unique_key", "recipient_uei"]
BITMAP_INDEXES = ["requires_clearance", "requires_cmmc", "has_extracted_scope", "marked_award",
                  "coverage_truncated", "is_primary_target", "req_clearance_level_max",
                  "type_of_set_aside"]

# Deterministic per-award list caps (chunk-skew tail — plan §1 / anti-pattern #9). Source-resource
# fanout per award is mild at this grain (measured p99=12, max=53), so SOURCE_CAP bounds future
# growth rather than clipping today; coverage_truncated is set when SOURCE_CAP actually clips.
TAG_CAP = 30
CERT_CAP = 25
LABOR_CAP = 15
SOURCE_CAP = 200

# Clearance ordinal — higher = more sensitive (enum-locked in the requirements sink: probed values
# TS_SCI / TOP_SECRET / SECRET / CONFIDENTIAL / PUBLIC_TRUST). Used to roll req_clearance_level_max.
CLEARANCE_ORD = {"TS_SCI": 5, "TOP_SECRET": 4, "SECRET": 3, "CONFIDENTIAL": 2, "PUBLIC_TRUST": 1}
_ORD_TO_LEVEL = {v: k for k, v in CLEARANCE_ORD.items()}

# Inline-key floor (plan §1 canonical numbers): exploded distinct awards MUST exceed this, else the
# manifest fan-out was dropped (anti-pattern #5). Measured inline floor in requirements = 7,784.
INLINE_KEY_FLOOR = 7784

DUCK_MEM = "8GB"
DUCK_TMP = "/tmp/capability_profiles_duckdb"


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
    """SQL aggregate → max clearance ordinal over CLEARANCE-TYPED, validated rows only (0 otherwise).
    Gating to `requirement_type='clearance'` couples the ordinal to `requires_clearance` by
    construction: `req_clearance_level_max` non-null ⟹ `requires_clearance` true (a stray
    `clearance_level` on a non-clearance row can never advertise a clearance the award doesn't
    require). Gating to `validated` matches the cert/labor/tag lanes and the documented contract."""
    whens = " ".join(f"WHEN '{lvl}' THEN {ordv}" for lvl, ordv in CLEARANCE_ORD.items())
    return ("max(CASE WHEN requirement_type = 'clearance' AND validated "
            f"THEN (CASE clearance_level {whens} ELSE 0 END) ELSE 0 END)")


def _assemble(so):
    """Deterministic assembly → (arrow_table, stats). Mirrors the materialize_winners_map pattern:
    Lance scanners (scalar/list projection only — never `embedding`) → DuckDB → Arrow, cast to the
    frozen schema. Multi-referenced inputs are materialized as re-scannable Arrow tables; the
    1.5M-row txn feed is streamed once via a reader."""
    import lance
    import pyarrow as pa

    rq = lance.dataset(REQUIREMENTS_URI, storage_options=so)
    dsc = lance.dataset(DOC_SCOPE_URI, storage_options=so)
    man = lance.dataset(MANIFEST_URI, storage_options=so)
    txn = lance.dataset(TXN_URI, storage_options=so)

    # ── CUI pre-flight (refuse to build on violation; anti-pattern #10) ──────────────────────────
    n_scope_marked = dsc.count_rows(filter="marked_resource = true")
    if n_scope_marked:
        raise RuntimeError(
            f"CUI INVARIANT VIOLATION: govcon_doc_scope has {n_scope_marked} marked rows — "
            f"scope_summary/capability_tags would carry marked-doc content. Marked docs must stay "
            f"bracketed out of the LLM lane (PHASE 2 DECISION 2026-06-12). Build refused.")
    n_marked_leak = rq.count_rows(
        filter="marked_resource = true AND (evidence_quote IS NOT NULL OR requirement_detail IS NOT NULL)")
    if n_marked_leak:
        raise RuntimeError(
            f"CUI INVARIANT VIOLATION: {n_marked_leak} marked requirement rows carry verbatim text. "
            f"Egress gate is write-side (Phase-1 schema); build refused.")

    con = _duck()
    con.register("rq", rq.scanner(columns=[
        "resource_id", "requirement_type", "requirement_value", "clearance_level",
        "headcount", "validated", "marked_resource", "coverage_truncated"]).to_table())
    con.register("dsc", dsc.scanner(columns=[
        "resource_id", "scope_summary", "capability_tags", "validated", "coverage_truncated"]).to_table())
    con.register("man", man.scanner(columns=[
        "resource_id", "award_keys", "is_primary_target"]).to_table())
    con.register("txn", txn.scanner(columns=[
        "contract_award_unique_key", "recipient_uei", "recipient_name", "recipient_parent_uei",
        "naics_code", "product_or_service_code", "type_of_set_aside", "awarding_agency_name",
        "primary_place_of_performance_city_name", "primary_place_of_performance_state_code",
        "primary_place_of_performance_zip_4", "primary_place_of_performance_country_code",
        "period_of_performance_start_date", "period_of_performance_current_end_date",
        "last_modified_date", "action_date", "federal_action_obligation", "total_dollars_obligated",
        "base_and_all_options_value", "current_total_value_of_award", "modification_number",
        "contract_transaction_unique_key"]).to_reader())

    # ── spine: explode award_keys for resources present in (requirements ∪ doc_scope) ────────────
    con.execute("CREATE TABLE res_universe AS "
                "SELECT DISTINCT resource_id FROM rq "
                "UNION SELECT DISTINCT resource_id FROM dsc")
    con.execute("""
        CREATE TABLE r2a AS
        SELECT DISTINCT m.resource_id, k.award_key AS contract_award_unique_key, m.is_primary_target
        FROM man m
        JOIN res_universe u ON m.resource_id = u.resource_id,
             UNNEST(m.award_keys) AS k(award_key)
        WHERE m.award_keys IS NOT NULL AND len(m.award_keys) > 0
    """)
    con.execute("""
        CREATE TABLE req_join AS
        SELECT a.contract_award_unique_key, r.requirement_type, r.requirement_value,
               r.clearance_level, r.headcount, r.validated, r.marked_resource, r.coverage_truncated
        FROM r2a a JOIN rq r ON a.resource_id = r.resource_id
    """)
    con.execute("""
        CREATE TABLE scope_join AS
        SELECT a.contract_award_unique_key, a.resource_id, a.is_primary_target,
               d.scope_summary, d.capability_tags, d.validated, d.coverage_truncated
        FROM r2a a JOIN dsc d ON a.resource_id = d.resource_id
    """)

    n_exploded = con.execute("SELECT count(DISTINCT contract_award_unique_key) FROM r2a").fetchone()[0]
    if n_exploded <= INLINE_KEY_FLOOR:
        raise RuntimeError(
            f"FAN-OUT DROPPED: only {n_exploded} exploded award keys ≤ inline floor {INLINE_KEY_FLOOR} "
            f"(anti-pattern #5). The manifest award_keys[] explode collapsed to the inline scalar.")

    clr = _clearance_ord_agg()
    sql = f"""
    WITH prime_award AS (
        SELECT contract_award_unique_key, recipient_uei, recipient_name, recipient_parent_uei,
               naics_code, product_or_service_code, type_of_set_aside, awarding_agency_name,
               primary_place_of_performance_city_name, primary_place_of_performance_state_code,
               primary_place_of_performance_zip_4, primary_place_of_performance_country_code,
               period_of_performance_start_date AS pop_start,
               period_of_performance_current_end_date AS pop_end,
               last_modified_date AS award_last_modified_date, action_date AS award_action_date,
               federal_action_obligation, total_dollars_obligated,
               base_and_all_options_value, current_total_value_of_award
        FROM (
            SELECT t.*, row_number() OVER (
                       PARTITION BY t.contract_award_unique_key
                       ORDER BY TRY_CAST(t.last_modified_date AS DATE) DESC NULLS LAST,
                                TRY_CAST(t.modification_number AS INTEGER) DESC NULLS LAST,
                                t.contract_transaction_unique_key DESC) AS rn
            FROM txn t
            WHERE t.contract_award_unique_key IN (SELECT contract_award_unique_key FROM r2a)
        ) WHERE rn = 1
    ),
    req_roll AS (
        SELECT contract_award_unique_key,
               bool_or(requirement_type = 'clearance' AND validated)                  AS requires_clearance,
               {clr}                                                                  AS clr_ord,
               bool_or(requirement_type = 'certification' AND validated
                       AND lower(requirement_value)
                           IN ('cmmc', 'cmmc_l1', 'cmmc_l2', 'cmmc_l3'))              AS requires_cmmc,
               bool_or(marked_resource)                                               AS marked_award,
               bool_or(coverage_truncated)                                            AS cov_trunc_req,
               CAST(count(*) AS INTEGER)                                              AS n_requirements,
               CAST(count(*) FILTER (WHERE validated) AS INTEGER)                     AS n_validated
        FROM req_join GROUP BY 1
    ),
    cert_src AS (
        SELECT DISTINCT contract_award_unique_key, requirement_value AS v
        FROM req_join
        WHERE requirement_type = 'certification' AND validated AND requirement_value IS NOT NULL
    ),
    cert_ranked AS (
        SELECT contract_award_unique_key, v,
               row_number() OVER (PARTITION BY contract_award_unique_key ORDER BY v) AS rk
        FROM cert_src
    ),
    cert_roll AS (
        SELECT contract_award_unique_key, list(v ORDER BY rk) AS req_cert_tags
        FROM cert_ranked WHERE rk <= {CERT_CAP} GROUP BY 1
    ),
    labor_counts AS (
        SELECT contract_award_unique_key, requirement_value AS lc,
               count(*) AS n, sum(coalesce(headcount, 0)) AS hc
        FROM req_join
        WHERE requirement_type = 'labor_category' AND validated AND requirement_value IS NOT NULL
        GROUP BY 1, 2
    ),
    labor_ranked AS (
        SELECT contract_award_unique_key, lc,
               row_number() OVER (PARTITION BY contract_award_unique_key
                                  ORDER BY n DESC, hc DESC, lc ASC) AS rk
        FROM labor_counts
    ),
    labor_roll AS (
        SELECT contract_award_unique_key, list(lc ORDER BY rk) AS top_labor_categories
        FROM labor_ranked WHERE rk <= {LABOR_CAP} GROUP BY 1
    ),
    tag_src AS (
        SELECT DISTINCT contract_award_unique_key, t
        FROM (SELECT contract_award_unique_key, UNNEST(capability_tags) AS t
              FROM scope_join WHERE validated)
        WHERE t IS NOT NULL
    ),
    tag_ranked AS (
        SELECT contract_award_unique_key, t,
               row_number() OVER (PARTITION BY contract_award_unique_key ORDER BY t) AS rk
        FROM tag_src
    ),
    tag_roll AS (
        SELECT contract_award_unique_key, list(t ORDER BY rk) AS solicitation_scope_tags
        FROM tag_ranked WHERE rk <= {TAG_CAP} GROUP BY 1
    ),
    summary_ranked AS (
        SELECT contract_award_unique_key, scope_summary,
               row_number() OVER (PARTITION BY contract_award_unique_key
                   ORDER BY is_primary_target DESC, length(scope_summary) DESC, resource_id ASC) AS rk
        FROM scope_join WHERE validated AND scope_summary IS NOT NULL
    ),
    summary_roll AS (
        SELECT contract_award_unique_key, scope_summary
        FROM summary_ranked WHERE rk = 1
    ),
    scope_flags AS (
        SELECT contract_award_unique_key,
               CAST(count(*) FILTER (WHERE validated) AS INTEGER) AS n_scope_docs,
               bool_or(coverage_truncated) AS cov_trunc_scope
        FROM scope_join GROUP BY 1
    ),
    src_all AS (SELECT DISTINCT contract_award_unique_key, resource_id FROM r2a),
    src_cnt AS (SELECT contract_award_unique_key, count(*) AS n_src FROM src_all GROUP BY 1),
    src_ranked AS (
        SELECT contract_award_unique_key, resource_id,
               row_number() OVER (PARTITION BY contract_award_unique_key ORDER BY resource_id) AS rk
        FROM src_all
    ),
    src_roll AS (
        SELECT contract_award_unique_key, list(resource_id ORDER BY rk) AS source_resource_ids
        FROM src_ranked WHERE rk <= {SOURCE_CAP} GROUP BY 1
    ),
    ipt_roll AS (
        SELECT contract_award_unique_key, bool_or(is_primary_target) AS is_primary_target
        FROM r2a GROUP BY 1
    ),
    base AS (SELECT DISTINCT contract_award_unique_key FROM r2a)
    SELECT
        b.contract_award_unique_key,
        pa.recipient_uei, pa.recipient_name, pa.recipient_parent_uei,
        pa.naics_code, pa.product_or_service_code, pa.type_of_set_aside, pa.awarding_agency_name,
        pa.primary_place_of_performance_city_name, pa.primary_place_of_performance_state_code,
        pa.primary_place_of_performance_zip_4, pa.primary_place_of_performance_country_code,
        pa.pop_start, pa.pop_end, pa.award_last_modified_date, pa.award_action_date,
        pa.federal_action_obligation, pa.total_dollars_obligated,
        pa.base_and_all_options_value, pa.current_total_value_of_award,
        sr.scope_summary,
        tr.solicitation_scope_tags,
        coalesce(rr.requires_clearance, false) AS requires_clearance,
        CASE rr.clr_ord {' '.join(f"WHEN {o} THEN '{lvl}'" for o, lvl in _ORD_TO_LEVEL.items())}
             ELSE NULL END AS req_clearance_level_max,
        coalesce(rr.requires_cmmc, false) AS requires_cmmc,
        cr.req_cert_tags,
        lr.top_labor_categories,
        coalesce(rr.n_requirements, 0) AS n_requirements,
        coalesce(rr.n_validated, 0) AS n_validated,
        srr.source_resource_ids,
        (coalesce(sf.n_scope_docs, 0) > 0) AS has_extracted_scope,
        coalesce(rr.marked_award, false) AS marked_award,
        (coalesce(rr.cov_trunc_req, false) OR coalesce(sf.cov_trunc_scope, false)
            OR coalesce(sc.n_src, 0) > {SOURCE_CAP}) AS coverage_truncated,
        coalesce(ipt.is_primary_target, false) AS is_primary_target
    FROM base b
    LEFT JOIN prime_award pa USING (contract_award_unique_key)
    LEFT JOIN req_roll     rr USING (contract_award_unique_key)
    LEFT JOIN cert_roll    cr USING (contract_award_unique_key)
    LEFT JOIN labor_roll   lr USING (contract_award_unique_key)
    LEFT JOIN tag_roll     tr USING (contract_award_unique_key)
    LEFT JOIN summary_roll sr USING (contract_award_unique_key)
    LEFT JOIN scope_flags  sf USING (contract_award_unique_key)
    LEFT JOIN src_roll     srr USING (contract_award_unique_key)
    LEFT JOIN src_cnt      sc USING (contract_award_unique_key)
    LEFT JOIN ipt_roll     ipt USING (contract_award_unique_key)
    """
    tbl = con.execute(sql).to_arrow_table()
    con.close()

    # stats for the run record / DoD, computed on the assembled Arrow table (no second R2 read)
    import pyarrow.compute as pc
    stats = {"rows": tbl.num_rows, "distinct_exploded_keys": n_exploded}
    n_resolved = pc.sum(pc.is_valid(tbl.column("recipient_uei"))).as_py() or 0
    stats["txn_resolved"] = n_resolved
    stats["txn_resolution_rate"] = round(n_resolved / tbl.num_rows, 4) if tbl.num_rows else 0.0
    stats["has_extracted_scope"] = pc.sum(tbl.column("has_extracted_scope")).as_py() or 0
    stats["marked_award"] = pc.sum(tbl.column("marked_award")).as_py() or 0
    stats["coverage_truncated"] = pc.sum(tbl.column("coverage_truncated")).as_py() or 0
    stats["requires_clearance"] = pc.sum(tbl.column("requires_clearance")).as_py() or 0
    stats["requires_cmmc"] = pc.sum(tbl.column("requires_cmmc")).as_py() or 0

    return tbl, stats


def _stamp(tbl, so):
    """Append the two run-stamp columns and cast to the frozen schema (asserts column parity)."""
    import lance
    import pyarrow as pa
    schema = capability_profiles_schema()
    built_at = dt.datetime.now(dt.timezone.utc)
    # consumed-upstream provenance (plan §5): pin every source dataset version.
    versions = {name: lance.dataset(uri, storage_options=so).version for name, uri in (
        ("txn", TXN_URI), ("reqs", REQUIREMENTS_URI),
        ("scope", DOC_SCOPE_URI), ("manifest", MANIFEST_URI))}
    snap = "|".join(f"{k}:v{v}" for k, v in versions.items())
    n = tbl.num_rows
    tbl = tbl.append_column("txn_snapshot_run_id", pa.array([snap] * n, pa.string()))
    tbl = tbl.append_column("built_at", pa.array([built_at] * n, pa.timestamp("us", tz="UTC")))
    # reorder + cast to the exact frozen schema (raises on any column mismatch BEFORE write)
    tbl = tbl.select(schema.names).cast(schema)
    return tbl, snap, built_at


def build():
    import lance
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    log("assembling capability profiles …")
    tbl, stats = _assemble(so)
    log(f"assembled {stats['rows']:,} award rows · exploded_keys={stats['distinct_exploded_keys']:,} "
        f"· txn_resolution={stats['txn_resolution_rate']:.4f} "
        f"· has_extracted_scope={stats['has_extracted_scope']:,} "
        f"· requires_clearance={stats['requires_clearance']:,} requires_cmmc={stats['requires_cmmc']:,} "
        f"· marked_award={stats['marked_award']:,} coverage_truncated={stats['coverage_truncated']:,}")
    if stats["rows"] == 0:
        raise RuntimeError("zero profiles assembled")
    if stats["rows"] != stats["distinct_exploded_keys"]:
        raise RuntimeError(
            f"GRAIN VIOLATION: {stats['rows']} rows != {stats['distinct_exploded_keys']} distinct "
            f"exploded keys (the build must be exactly one row per exploded award key).")

    tbl, snap, built_at = _stamp(tbl, so)
    # schema-assert the FROZEN empty shell shape against our table before committing (frozen is law)
    schema = capability_profiles_schema()
    if not tbl.schema.equals(schema, check_metadata=False):
        raise RuntimeError("assembled table schema != frozen capability_profiles_schema()")

    log(f"writing OVERWRITE → {CAPABILITY_PROFILES_URI}  (snapshot {snap})")
    lance.write_dataset(tbl, CAPABILITY_PROFILES_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    # post-write schema assert against the live dataset (the only drift detector)
    assert_schema(CAPABILITY_PROFILES_URI, schema, so)
    ds = lance.dataset(CAPABILITY_PROFILES_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True); log(f"  BTREE  ✓ {col}")
    for col in BITMAP_INDEXES:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True); log(f"  BITMAP ✓ {col}")
    elapsed = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
    log(f"DONE → {CAPABILITY_PROFILES_URI} rows={stats['rows']:,} in {elapsed:.0f}s")
    return {**stats, "snapshot": snap, "built_at": built_at.isoformat(), "elapsed_s": round(elapsed, 1)}


def _content_hash(con, rel: str) -> str:
    """md5 over every business column EXCLUDING the run stamps (built_at, txn_snapshot_run_id),
    ordered by award key — the idempotency fingerprint (re-run zero-delta proof)."""
    cols = [n for n in capability_profiles_schema().names
            if n not in ("built_at", "txn_snapshot_run_id")]
    proj = ", ".join(cols)
    return con.execute(
        f"WITH b AS (SELECT {proj} FROM {rel}) "
        f"SELECT md5(string_agg(md5(to_json(b)), '' ORDER BY contract_award_unique_key)) FROM b"
    ).fetchone()[0]


def verify(content_hash: bool = False):
    import lance
    so = _r2_storage_options()
    ds = lance.dataset(CAPABILITY_PROFILES_URI, storage_options=so)
    schema = capability_profiles_schema()
    assert_schema(CAPABILITY_PROFILES_URI, schema, so)  # raises on drift

    rows = ds.count_rows()
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []

    # recompute distinct exploded keys from source (parity check) + CUI checks, in DuckDB
    con = _duck()
    rq = lance.dataset(REQUIREMENTS_URI, storage_options=so)
    dscd = lance.dataset(DOC_SCOPE_URI, storage_options=so)
    man = lance.dataset(MANIFEST_URI, storage_options=so)
    con.register("rq", rq.scanner(columns=["resource_id"]).to_table())
    con.register("dsc", dscd.scanner(columns=["resource_id"]).to_table())
    con.register("man", man.scanner(columns=["resource_id", "award_keys"]).to_table())
    # full projection: the parity/CUI checks below use a subset, _content_hash needs every column
    con.register("prof", ds.scanner().to_table())
    exploded = con.execute("""
        WITH res AS (SELECT DISTINCT resource_id FROM rq UNION SELECT DISTINCT resource_id FROM dsc)
        SELECT count(DISTINCT k.award_key) FROM man m JOIN res u ON m.resource_id=u.resource_id,
            UNNEST(m.award_keys) AS k(award_key) WHERE m.award_keys IS NOT NULL AND len(m.award_keys)>0
    """).fetchone()[0]
    distinct_keys = con.execute("SELECT count(DISTINCT contract_award_unique_key) FROM prof").fetchone()[0]
    cui_summary_no_scope = con.execute(
        "SELECT count(*) FROM prof WHERE scope_summary IS NOT NULL AND has_extracted_scope = false"
    ).fetchone()[0]
    clr_no_flag = con.execute(
        "SELECT count(*) FROM prof WHERE req_clearance_level_max IS NOT NULL AND requires_clearance = false"
    ).fetchone()[0]

    out = {
        "uri": CAPABILITY_PROFILES_URI,
        "rows": rows,
        "distinct_award_keys": distinct_keys,
        "source_exploded_keys": exploded,
        "row_eq_distinct_key": rows == distinct_keys,
        "row_eq_source_exploded": rows == exploded,
        "txn_resolved": ds.count_rows(filter="recipient_uei IS NOT NULL"),
        "txn_resolution_rate": round(ds.count_rows(filter="recipient_uei IS NOT NULL") / rows, 4) if rows else 0.0,
        "has_extracted_scope": ds.count_rows(filter="has_extracted_scope = true"),
        "requires_clearance": ds.count_rows(filter="requires_clearance = true"),
        "requires_cmmc": ds.count_rows(filter="requires_cmmc = true"),
        "marked_award": ds.count_rows(filter="marked_award = true"),
        "coverage_truncated": ds.count_rows(filter="coverage_truncated = true"),
        "columns": len(schema.names),
        "indices": sorted(idx),
        "schema_assert": "PASS",
        "cui_summary_without_scope_flag": cui_summary_no_scope,   # expect 0 (invariant)
        "clearance_level_without_flag": clr_no_flag,              # expect 0 (consistency)
    }
    if content_hash:
        out["content_hash_excl_stamps"] = _content_hash(con, "prof")
    con.close()
    print(json.dumps(out, indent=2, default=str))
    return out


def query():
    """North-star degraded acceptance (plan PHASE 2 verbatim gate): companies that do [capability
    tag X] AND require clearance ≥ [A] AND require [B=CMMC] → resolvable cited evidence. Tag /
    clearance / cmmc are env-overridable. Citations resolve at query time from the requirements sink
    (UNMARKED rows only — CUI invariant)."""
    import os
    import lance
    so = _r2_storage_options()
    tag = os.environ.get("NS_TAG", "electrical_systems")
    min_clr = os.environ.get("NS_MIN_CLEARANCE", "SECRET")
    require_cmmc = os.environ.get("NS_REQUIRE_CMMC", "1") == "1"
    limit = int(os.environ.get("NS_LIMIT", "10"))
    min_ord = CLEARANCE_ORD.get(min_clr, 3)
    allowed = sorted([l for l, o in CLEARANCE_ORD.items() if o >= min_ord])

    ds = lance.dataset(CAPABILITY_PROFILES_URI, storage_options=so)
    rq = lance.dataset(REQUIREMENTS_URI, storage_options=so)
    con = _duck()
    con.register("prof", ds.scanner(columns=[
        "contract_award_unique_key", "recipient_uei", "recipient_name", "awarding_agency_name",
        "naics_code", "solicitation_scope_tags", "requires_clearance", "req_clearance_level_max",
        "requires_cmmc", "req_cert_tags", "top_labor_categories", "source_resource_ids",
        "scope_summary", "marked_award"]).to_table())
    con.register("req", rq.scanner(columns=[
        "resource_id", "requirement_type", "requirement_value", "clearance_level",
        "evidence_quote", "source_chunk_ids", "marked_resource", "validated"]).to_table())

    cmmc_clause = "AND requires_cmmc = true" if require_cmmc else ""
    allowed_sql = ", ".join(f"'{l}'" for l in allowed)
    matches = con.execute(f"""
        SELECT contract_award_unique_key, recipient_uei, recipient_name, awarding_agency_name,
               naics_code, req_clearance_level_max, requires_cmmc, req_cert_tags,
               top_labor_categories, source_resource_ids, marked_award,
               substr(scope_summary, 1, 240) AS scope_excerpt
        FROM prof
        WHERE list_contains(solicitation_scope_tags, '{tag}')
          AND requires_clearance = true
          AND req_clearance_level_max IN ({allowed_sql})
          {cmmc_clause}
        ORDER BY contract_award_unique_key
        LIMIT {limit}
    """).fetchall()
    cols = [d[0] for d in con.description]

    total = con.execute(f"""
        SELECT count(*) FROM prof
        WHERE list_contains(solicitation_scope_tags, '{tag}') AND requires_clearance = true
          AND req_clearance_level_max IN ({allowed_sql}) {cmmc_clause}
    """).fetchone()[0]
    distinct_companies = con.execute(f"""
        SELECT count(DISTINCT recipient_uei) FROM prof
        WHERE list_contains(solicitation_scope_tags, '{tag}') AND requires_clearance = true
          AND req_clearance_level_max IN ({allowed_sql}) {cmmc_clause}
          AND recipient_uei IS NOT NULL
    """).fetchone()[0]

    print(json.dumps({
        "query": f"solicitation_scope_tags ∋ '{tag}' AND requires_clearance≥{min_clr}"
                 f"{' AND requires_cmmc' if require_cmmc else ''}",
        "matched_award_rows": total, "distinct_companies": distinct_companies,
        "showing": len(matches)}, indent=2))

    # resolve cited evidence for each shown award (clearance + cmmc requirement rows, UNMARKED only)
    for row in matches:
        r = dict(zip(cols, row))
        srcs = r.get("source_resource_ids") or []
        src_sql = ", ".join(f"'{s}'" for s in srcs) or "''"
        cites = con.execute(f"""
            SELECT requirement_type, requirement_value, clearance_level,
                   substr(evidence_quote, 1, 200) AS quote, source_chunk_ids
            FROM req
            WHERE resource_id IN ({src_sql}) AND validated AND marked_resource = false
              AND (requirement_type = 'clearance'
                   OR (requirement_type='certification' AND lower(requirement_value) LIKE 'cmmc%'))
              AND evidence_quote IS NOT NULL
            ORDER BY requirement_type LIMIT 3
        """).fetchall()
        print("\n" + "=" * 88)
        print(f"  {r['recipient_name']}  (UEI {r['recipient_uei']})")
        print(f"  award={r['contract_award_unique_key']}  agency={r['awarding_agency_name']}  NAICS={r['naics_code']}")
        print(f"  clearance_max={r['req_clearance_level_max']}  cmmc={r['requires_cmmc']}  "
              f"certs={r['req_cert_tags']}  labor={r['top_labor_categories']}  marked={r['marked_award']}")
        if r.get("scope_excerpt"):
            print(f"  scope: {r['scope_excerpt']}")
        if cites:
            print("  CITED EVIDENCE:")
            for c in cites:
                rtyp, rval, clvl, quote, chunks = c
                cid = (chunks[0] if chunks else None)
                print(f"    • [{rtyp}={rval}{('/' + clvl) if clvl else ''}] \"{quote}\"  (chunk {cid})")
        else:
            print("  CITED EVIDENCE: (clearance/cmmc evidence rows all marked or absent for shown sources)")
    con.close()


def drop_legacy_shell():
    """Delete every object under the empty `..._90day` P0 shell prefix via boto3 (R2). Guarded:
    refuses if the shell has any rows (it is not the empty shell it was described as)."""
    import lance
    import boto3
    from botocore.config import Config
    so = _r2_storage_options()
    if not _dataset_exists(LEGACY_SHELL_URI, so):
        log(f"legacy shell absent: {LEGACY_SHELL_URI}")
        return
    n = lance.dataset(LEGACY_SHELL_URI, storage_options=so).count_rows()
    if n != 0:
        raise RuntimeError(f"REFUSING to drop {LEGACY_SHELL_URI}: {n} rows (expected empty shell).")
    body = LEGACY_SHELL_URI[len("s3://"):]
    bucket, _, prefix = body.partition("/")
    cli = boto3.client("s3", endpoint_url=so["endpoint"], aws_access_key_id=so["aws_access_key_id"],
                       aws_secret_access_key=so["aws_secret_access_key"], region_name="auto",
                       config=Config(retries={"max_attempts": 3, "mode": "standard"}))
    deleted = 0
    paginator = cli.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            cli.delete_objects(Bucket=bucket, Delete={"Objects": objs})
            deleted += len(objs)
    log(f"dropped legacy shell {LEGACY_SHELL_URI} ({deleted} objects)")


def main():
    p = argparse.ArgumentParser(description="Build govcon_award_solicitation_profiles (plan PHASE 2).")
    p.add_argument("cmd", choices=["build", "verify", "query", "drop_legacy_shell"])
    p.add_argument("--content-hash", action="store_true", help="verify: include idempotency hash")
    a = p.parse_args()
    if a.cmd == "build":
        print(json.dumps(build(), indent=2, default=str))
    elif a.cmd == "verify":
        verify(content_hash=a.content_hash)
    elif a.cmd == "query":
        query()
    elif a.cmd == "drop_legacy_shell":
        drop_legacy_shell()


if __name__ == "__main__":
    main()
