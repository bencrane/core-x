"""Build worker — govcon_sub_targeting: the outreach list (build plan PHASE 4, closes the
north star). One row per (covered award, candidate subawardee): "do X" subs reachable under primes
that "need A,B,C," every hop citeable.

GRAIN   1 row per (contract_award_unique_key, candidate_sub_uei). One row per pair — the STRONGEST
        edge_type wins (direct_subaward > teaming_history > capability_match).
SoR     s3://data-sink/active/govcon_sub_targeting/  (Lance v2.1; derived, SNAPSHOT-OVERWRITE).
        Frozen schema + assert_schema (govcon_gtm_schemas.py) before the first commit.

EDGE SEMANTICS (deterministic v1, no vectors — Phase 5 upgrades capability_match to ANN):
  * direct_subaward  — the prime sub-let THIS covered award (contract_subaward, in-window). dollars/
                       count = the in-window subaward report. The edge witnesses "worked under P on W."
  * teaming_history  — the sub worked with this prime over the 5y corpus (govcon_teaming_edges).
                       dollars/count = 5y edge weight. Witnesses "worked under P (habitually)."
  * capability_match — the sub does the trade the award needs: 4-digit NAICS-family equality AND the
                       sub's subaward_description contains ≥1 of the award's validated labor_category
                       requirement-value tokens. dollars/count NULL. matched_requirement_ids = exactly
                       the labor requirement rows whose token hit (never empty — zero-hit pairs are
                       never written, so the DoD is verifiable for the full enum).

matched_requirement_ids (NEVER empty): direct_subaward / teaming_history carry the covered award's
validated requirement rows (clearance/cert/labor first, capped) — the "needs A,B,C" citation;
capability_match carries exactly the labor rows whose token hit. An award with zero validated
requirement rows produces NO targeting rows (nothing to cite) — correct.

CUI: this build reads NO solicitation-document text. capability_evidence is the SUB's own
subaward_description (sub-self-reported, public), never marked-doc egress. Citations to the prime's
requirements resolve by requirement_id → govcon_award_requirements at query time.

POC: poc_available is the precomputed bool (candidate_sub_uei ∈ sam_pocs); POC payloads join at
query time (sam_pocs ~89.3% fill). No POC fields materialize here.

INDICES BTREE(contract_award_unique_key, candidate_sub_uei, prime_uei).

    doppler run -p core-x -c prd -- .venv/bin/python \
      pipelines/sam_gov/../serving/materialize_sub_targeting.py <build|verify|bench|northstar>
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines.sam_gov.govcon_gtm_schemas import (  # noqa: E402
    SUB_TARGETING_URI, sub_targeting_schema, assert_schema)
from pipelines.sam_gov.sam_attachment_extract_90day import _r2_storage_options  # noqa: E402

ACTIVE = "s3://data-sink/active"
PROFILES_URI = f"{ACTIVE}/govcon_award_solicitation_profiles/"
REQUIREMENTS_URI = f"{ACTIVE}/govcon_award_requirements/"
SUBAWARD_URI = f"{ACTIVE}/usaspending_api_fresh/contract_subaward/"
TEAMING_URI = f"{ACTIVE}/govcon_teaming_edges/"
SAM_POCS_URI = f"{ACTIVE}/sam_pocs/"

DATA_STORAGE_VERSION = "2.1"
BTREE_INDEXES = ["contract_award_unique_key", "candidate_sub_uei", "prime_uei"]

MATCHED_REQ_CAP = 25          # matched_requirement_ids per row (clearance/cert/labor prioritized)
AWARD_FANOUT_CAP = 200        # candidate subs per award (mega-IDIQ umbrella bound)
EVIDENCE_CHARS = 300
DESC_SCAN_CHARS = 2000        # bound the LIKE cost on prolific subs' concatenated descriptions

DUCK_MEM = "10GB"
DUCK_TMP = "/tmp/sub_targeting_duckdb"


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


def _assemble(so):
    """Deterministic assembly → (arrow_table, stats). Reads structured columns only."""
    import lance
    con = _duck()
    prof = lance.dataset(PROFILES_URI, storage_options=so)
    req = lance.dataset(REQUIREMENTS_URI, storage_options=so)
    sub = lance.dataset(SUBAWARD_URI, storage_options=so)
    te = lance.dataset(TEAMING_URI, storage_options=so)
    poc = lance.dataset(SAM_POCS_URI, storage_options=so)

    con.register("prof", prof.scanner(columns=[
        "contract_award_unique_key", "recipient_uei", "recipient_name", "naics_code",
        "solicitation_scope_tags", "source_resource_ids"]).to_table())
    con.register("req", req.scanner(columns=[
        "resource_id", "requirement_id", "requirement_type", "requirement_value", "validated"]).to_table())
    con.register("sub", sub.scanner(columns=[
        "prime_award_unique_key", "subawardee_uei", "subaward_amount", "subaward_action_date",
        "subaward_description", "prime_award_naics_code"]).to_table())
    con.register("te", te.scanner(columns=[
        "prime_uei", "sub_uei", "edge_dollars_5y", "edge_count_5y", "last_action_date",
        "top_naics"]).to_table())
    con.register("poc", poc.scanner(columns=["uei"]).to_table())

    # ── award base (prime-resolvable covered awards) + their validated requirement rows ──────────
    con.execute("""
        CREATE TABLE award_base AS
        SELECT contract_award_unique_key AS award, recipient_uei AS prime_uei,
               recipient_name AS prime_name, naics_code, substr(naics_code, 1, 4) AS naics4,
               source_resource_ids
        FROM prof WHERE recipient_uei IS NOT NULL AND length(trim(recipient_uei)) > 0
    """)
    con.execute("""
        CREATE TABLE award_req AS
        SELECT DISTINCT a.award, r.requirement_id, r.requirement_type,
               lower(r.requirement_value) AS rv
        FROM award_base a, UNNEST(a.source_resource_ids) AS u(rid)
        JOIN req r ON r.resource_id = u.rid AND r.validated
    """)
    # award → matched_requirement_ids (clearance > certification > labor_category > license > other)
    con.execute(f"""
        CREATE TABLE award_reqids AS
        SELECT award, list(requirement_id ORDER BY rk) AS matched_requirement_ids
        FROM (
            SELECT award, requirement_id,
                   row_number() OVER (PARTITION BY award ORDER BY
                       CASE requirement_type WHEN 'clearance' THEN 0 WHEN 'certification' THEN 1
                            WHEN 'labor_category' THEN 2 WHEN 'license' THEN 3 ELSE 4 END,
                       requirement_id) AS rk
            FROM (SELECT DISTINCT award, requirement_id, requirement_type FROM award_req)
        ) WHERE rk <= {MATCHED_REQ_CAP} GROUP BY 1
    """)
    n_eligible = con.execute("SELECT count(*) FROM award_reqids").fetchone()[0]
    if n_eligible == 0:
        raise RuntimeError("no eligible awards (zero with validated requirement rows)")

    # ── edge 1: direct_subaward (in-window report) ──────────────────────────────────────────────
    con.execute(f"""
        CREATE TABLE e_direct AS
        SELECT s.prime_award_unique_key AS award, s.subawardee_uei AS sub,
               'direct_subaward' AS edge_type,
               sum(try_cast(s.subaward_amount AS DOUBLE)) AS edge_dollars_5y,
               CAST(count(*) AS INTEGER) AS edge_count_5y,
               max(try_cast(s.subaward_action_date AS DATE)) AS last_subaward_action_date,
               max(substr(s.subaward_description, 1, {EVIDENCE_CHARS})) AS capability_evidence,
               mode(substr(s.prime_award_naics_code, 1, 6)) AS sub_top_naics,
               CAST(NULL AS VARCHAR[]) AS cap_matched
        FROM sub s JOIN award_reqids ar ON s.prime_award_unique_key = ar.award
        WHERE s.subawardee_uei IS NOT NULL AND length(trim(s.subawardee_uei)) > 0
        GROUP BY 1, 2
    """)
    # ── edge 2: teaming_history (5y corpus) ─────────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE e_team AS
        SELECT a.award, t.sub_uei AS sub, 'teaming_history' AS edge_type,
               t.edge_dollars_5y, t.edge_count_5y, t.last_action_date AS last_subaward_action_date,
               CAST(NULL AS VARCHAR) AS capability_evidence, t.top_naics AS sub_top_naics,
               CAST(NULL AS VARCHAR[]) AS cap_matched
        FROM award_base a
        JOIN award_reqids ar ON a.award = ar.award
        JOIN te t ON a.prime_uei = t.prime_uei
        WHERE t.sub_uei IS NOT NULL AND length(trim(t.sub_uei)) > 0
    """)
    # ── edge 3: capability_match (NAICS-4 family + labor-token hit in sub description) ───────────
    con.execute(f"""
        CREATE TABLE sub_desc AS
        SELECT subawardee_uei AS sub,
               mode(substr(prime_award_naics_code, 1, 4)) AS naics4,
               mode(substr(prime_award_naics_code, 1, 6)) AS sub_top_naics,
               -- ORDER BY makes the concatenation (and its truncation → LIKE hits) deterministic.
               lower(substr(string_agg(coalesce(subaward_description, ''), ' '
                            ORDER BY subaward_description), 1, {DESC_SCAN_CHARS})) AS desc_text,
               max(substr(subaward_description, 1, {EVIDENCE_CHARS})) AS evidence
        FROM sub
        WHERE subawardee_uei IS NOT NULL AND length(trim(subawardee_uei)) > 0
          AND subaward_description IS NOT NULL AND length(trim(subaward_description)) > 0
        GROUP BY 1
    """)
    con.execute("""
        CREATE TABLE award_labor AS
        SELECT DISTINCT a.award, a.naics4, ar.requirement_id, ar.rv AS token
        FROM award_req ar JOIN award_base a ON ar.award = a.award
        WHERE ar.requirement_type = 'labor_category' AND length(ar.rv) >= 4
    """)
    con.execute(f"""
        CREATE TABLE e_cap AS
        SELECT h.award, h.sub, 'capability_match' AS edge_type,
               CAST(NULL AS DOUBLE) AS edge_dollars_5y, CAST(NULL AS INTEGER) AS edge_count_5y,
               CAST(NULL AS DATE) AS last_subaward_action_date,
               any_value(sd.evidence) AS capability_evidence,
               any_value(sd.sub_top_naics) AS sub_top_naics,
               (list(DISTINCT h.requirement_id))[1:{MATCHED_REQ_CAP}] AS cap_matched
        FROM (
            SELECT al.award, sd.sub, al.requirement_id
            FROM award_labor al
            JOIN sub_desc sd ON sd.naics4 = al.naics4
            WHERE sd.desc_text LIKE '%' || al.token || '%'
        ) h
        JOIN sub_desc sd ON h.sub = sd.sub
        GROUP BY 1, 2
    """)

    # ── union → strongest edge per (award, sub) → per-award fanout cap → enrich ──────────────────
    sql = f"""
    WITH all_edges AS (
        SELECT *, 0 AS prec FROM e_direct
        UNION ALL SELECT *, 1 AS prec FROM e_team
        UNION ALL SELECT *, 2 AS prec FROM e_cap
    ),
    dedup AS (
        SELECT * FROM (
            SELECT *, row_number() OVER (PARTITION BY award, sub
                       ORDER BY prec, edge_dollars_5y DESC NULLS LAST) AS r_pair
            FROM all_edges
        ) WHERE r_pair = 1
    ),
    capped AS (
        SELECT * FROM (
            SELECT *, row_number() OVER (PARTITION BY award
                       ORDER BY prec, edge_dollars_5y DESC NULLS LAST, sub) AS r_award
            FROM dedup
        ) WHERE r_award <= {AWARD_FANOUT_CAP}
    ),
    poc_ueis AS (SELECT DISTINCT uei FROM poc WHERE uei IS NOT NULL)
    SELECT c.award AS contract_award_unique_key, c.sub AS candidate_sub_uei,
           ab.prime_uei, ab.prime_name, c.edge_type,
           c.edge_dollars_5y, c.edge_count_5y, c.last_subaward_action_date,
           coalesce(c.cap_matched, ar.matched_requirement_ids) AS matched_requirement_ids,
           c.sub_top_naics, c.capability_evidence,
           (p.uei IS NOT NULL) AS poc_available
    FROM capped c
    JOIN award_base ab ON c.award = ab.award
    JOIN award_reqids ar ON c.award = ar.award
    LEFT JOIN poc_ueis p ON c.sub = p.uei
    """
    tbl = con.execute(sql).to_arrow_table()

    import pyarrow.compute as pc
    stats = {
        "rows": tbl.num_rows,
        "eligible_awards": n_eligible,
        "distinct_awards": len(pc.unique(tbl.column("contract_award_unique_key"))),
        "distinct_subs": len(pc.unique(tbl.column("candidate_sub_uei"))),
        "direct": con.execute("SELECT count(*) FROM e_direct").fetchone()[0],
        "teaming": con.execute("SELECT count(*) FROM e_team").fetchone()[0],
        "capability": con.execute("SELECT count(*) FROM e_cap").fetchone()[0],
        "poc_available": pc.sum(tbl.column("poc_available")).as_py() or 0,
    }
    con.close()
    return tbl, stats


def _stamp(tbl):
    import pyarrow as pa
    schema = sub_targeting_schema()
    built_at = dt.datetime.now(dt.timezone.utc)
    tbl = tbl.append_column("built_at", pa.array([built_at] * tbl.num_rows, pa.timestamp("us", tz="UTC")))
    return tbl.select(schema.names).cast(schema), built_at


def build():
    import lance
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    log("assembling sub-targeting edges …")
    tbl, stats = _assemble(so)
    log(f"assembled {stats['rows']:,} edges · awards={stats['distinct_awards']:,} "
        f"subs={stats['distinct_subs']:,} · direct={stats['direct']:,} teaming={stats['teaming']:,} "
        f"capability={stats['capability']:,} · poc_available={stats['poc_available']:,}")
    if stats["rows"] == 0:
        raise RuntimeError("zero targeting edges assembled")
    tbl, built_at = _stamp(tbl)
    schema = sub_targeting_schema()
    if not tbl.schema.equals(schema, check_metadata=False):
        raise RuntimeError("assembled schema != frozen sub_targeting_schema()")
    # matched_requirement_ids never empty (hard invariant)
    import pyarrow.compute as pc
    empty = pc.sum(pc.equal(pc.list_value_length(tbl.column("matched_requirement_ids")), 0)).as_py() or 0
    if empty:
        raise RuntimeError(f"{empty} rows have empty matched_requirement_ids (must never be empty)")
    log(f"writing OVERWRITE → {SUB_TARGETING_URI}")
    lance.write_dataset(tbl, SUB_TARGETING_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    assert_schema(SUB_TARGETING_URI, schema, so)
    ds = lance.dataset(SUB_TARGETING_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True); log(f"  BTREE ✓ {col}")
    elapsed = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
    log(f"DONE → {SUB_TARGETING_URI} rows={stats['rows']:,} in {elapsed:.0f}s")
    return {**stats, "built_at": built_at.isoformat(), "elapsed_s": round(elapsed, 1)}


def verify():
    import lance
    so = _r2_storage_options()
    ds = lance.dataset(SUB_TARGETING_URI, storage_options=so)
    schema = sub_targeting_schema()
    assert_schema(SUB_TARGETING_URI, schema, so)
    rows = ds.count_rows()
    try:
        idx = sorted(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                     for i in ds.list_indices())
    except Exception:  # noqa: BLE001
        idx = []
    con = _duck()
    con.register("t", ds.scanner(columns=[
        "contract_award_unique_key", "candidate_sub_uei", "edge_type", "matched_requirement_ids",
        "poc_available"]).to_table())
    out = {
        "uri": SUB_TARGETING_URI, "rows": rows,
        "columns": len(schema.names), "indices": idx, "schema_assert": "PASS",
        "distinct_awards": con.execute("SELECT count(DISTINCT contract_award_unique_key) FROM t").fetchone()[0],
        "distinct_subs": con.execute("SELECT count(DISTINCT candidate_sub_uei) FROM t").fetchone()[0],
        "by_edge_type": dict(con.execute(
            "SELECT edge_type, count(*) FROM t GROUP BY 1 ORDER BY 2 DESC").fetchall()),
        "pair_grain_unique": con.execute(
            "SELECT count(*) = count(DISTINCT (contract_award_unique_key, candidate_sub_uei)) FROM t").fetchone()[0],
        "rows_with_empty_matched": con.execute(
            "SELECT count(*) FROM t WHERE len(matched_requirement_ids) = 0").fetchone()[0],
        "poc_available": con.execute("SELECT count(*) FROM t WHERE poc_available").fetchone()[0],
    }
    con.close()
    print(json.dumps(out, indent=2, default=str))
    return out


def northstar():
    """End-to-end degraded north-star (plan PHASE 4 DoD, verbatim): companies tagged
    electrical_systems under awards requiring SECRET+ clearance → sub bench with POCs + cited reqs."""
    import os
    import lance
    so = _r2_storage_options()
    tag = os.environ.get("NS_TAG", "electrical_systems")
    limit = int(os.environ.get("NS_LIMIT", "8"))
    ds = lance.dataset(SUB_TARGETING_URI, storage_options=so)
    prof = lance.dataset(PROFILES_URI, storage_options=so)
    req = lance.dataset(REQUIREMENTS_URI, storage_options=so)
    poc = lance.dataset(SAM_POCS_URI, storage_options=so)
    con = _duck()
    con.register("t", ds.to_table())
    con.register("prof", prof.scanner(columns=[
        "contract_award_unique_key", "recipient_name", "solicitation_scope_tags",
        "requires_clearance", "req_clearance_level_max", "requires_cmmc"]).to_table())
    con.register("req", req.scanner(columns=["requirement_id", "requirement_type", "requirement_value",
                                             "clearance_level", "marked_resource", "validated"]).to_table())
    con.register("poc", poc.scanner(columns=["uei", "full_name", "title"]).to_table())
    # award set: electrical_systems ∧ clearance≥SECRET
    rows = con.execute(f"""
        WITH target_awards AS (
            SELECT contract_award_unique_key AS award, recipient_name AS prime_name
            FROM prof WHERE list_contains(solicitation_scope_tags, '{tag}') AND requires_clearance
              AND req_clearance_level_max IN ('SECRET','TOP_SECRET','TS_SCI')
        )
        SELECT ta.prime_name, t.contract_award_unique_key, t.candidate_sub_uei, t.edge_type,
               t.sub_top_naics, t.poc_available, t.matched_requirement_ids,
               substr(t.capability_evidence, 1, 140) AS evidence
        FROM t JOIN target_awards ta ON t.contract_award_unique_key = ta.award
        ORDER BY t.poc_available DESC, t.edge_type
        LIMIT {limit}
    """).fetchall()
    cols = [d[0] for d in con.description]
    n_awards, n_subs = con.execute(f"""
        WITH target_awards AS (SELECT contract_award_unique_key AS award FROM prof
            WHERE list_contains(solicitation_scope_tags, '{tag}') AND requires_clearance
              AND req_clearance_level_max IN ('SECRET','TOP_SECRET','TS_SCI'))
        SELECT count(DISTINCT t.contract_award_unique_key), count(DISTINCT t.candidate_sub_uei)
        FROM t JOIN target_awards ta ON t.contract_award_unique_key = ta.award
    """).fetchone()
    print(json.dumps({"query": f"solicitation_scope_tags ∋ '{tag}' AND clearance≥SECRET → sub bench",
                      "target_award_rows_with_subs": n_awards, "distinct_candidate_subs": n_subs,
                      "showing": len(rows)}, indent=2))
    for r in rows:
        d = dict(zip(cols, r))
        reqids = d["matched_requirement_ids"] or []
        rid_sql = ", ".join(f"'{x}'" for x in reqids[:3]) or "''"
        cites = con.execute(f"""
            SELECT requirement_type, requirement_value, clearance_level FROM req
            WHERE requirement_id IN ({rid_sql}) AND validated AND marked_resource = false LIMIT 3
        """).fetchall()
        pocs = con.execute(
            f"SELECT full_name, title FROM poc WHERE uei = '{d['candidate_sub_uei']}' "
            f"AND full_name IS NOT NULL LIMIT 2").fetchall()
        print("\n" + "=" * 88)
        print(f"  PRIME: {d['prime_name']}  · award={d['contract_award_unique_key']}")
        print(f"  SUB:   {d['candidate_sub_uei']} (naics {d['sub_top_naics']}) via {d['edge_type']}  poc={d['poc_available']}")
        if d.get("evidence"):
            print(f"    sub evidence: {d['evidence']}")
        print(f"    cited prime reqs: {[(c[0], c[1], c[2]) for c in cites]}")
        print(f"    POCs: {[f'{p[0]} ({p[1]})' for p in pocs] if pocs else '(none listed)'}")
    con.close()


def main():
    p = argparse.ArgumentParser(description="Build govcon_sub_targeting (plan PHASE 4).")
    p.add_argument("cmd", choices=["build", "verify", "northstar"])
    a = p.parse_args()
    if a.cmd == "build":
        print(json.dumps(build(), indent=2, default=str))
    elif a.cmd == "verify":
        verify()
    elif a.cmd == "northstar":
        northstar()


if __name__ == "__main__":
    main()
