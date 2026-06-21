#!/usr/bin/env python3
"""READ-ONLY sizing probe for the Stage-3 download + extraction backlog wave plan.

Throwaway (prefix `_plan_`). Sizes, per priority cohort, exactly how many harvested-link
files / awards / solicitations are (a) NOT yet downloaded and (b) downloaded but NOT yet
extracted — then estimates bonding yield and proves an even hash partition.

Funnel: Stage-2 manifest (links) -> Stage-3 sam_attachment_files(status='downloaded')
        -> extraction (govcon_award_requirements distinct resource_id).

Bridge to active awards (reuse of attach_substrate_coverage_probe.py join):
  award is in-cohort if its solicitation_identifier (FPDS) OR PIID-recovered Sol#
  matches a manifest solicitation_number (normalized), OR its award key is a winners
  manifest award key. A manifest resource_id is "cohort-bridged" if it sits on a
  solicitation/award key that maps to an in-cohort active award.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/_plan_stage3_backlog_probe.py > /tmp/_plan_stage3_backlog.json
"""
from __future__ import annotations
import datetime as dt, json, os, sys
import duckdb, lance

def so():
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}

A = "s3://data-sink/active"
MANIFESTS = [
    f"{A}/sam_opps_attachment_manifest/",
    f"{A}/sam_opps_attachment_manifest_winners/",
    f"{A}/sam_opps_attachment_manifest_remediation/shard_000/",
    f"{A}/sam_opps_attachment_manifest_equipment_rental/shard_000/",
    f"{A}/sam_opps_attachment_manifest_sb500k/",
] + [f"{A}/sam_opps_attachment_manifest_play1/shard_00{i}/" for i in range(6)]
WINNERS = f"{A}/sam_opps_attachment_manifest_winners/"
FILES = f"{A}/sam_attachment_files/"
REQ = f"{A}/govcon_award_requirements/"
GAA = f"{A}/govcon_active_awards/"
UNI_A = "s3://data-sink/sam-gov-opps/active/"
UNI_R = "s3://data-sink/sam-gov-opps/archived/"
N = "nullif(upper(regexp_replace(trim({c}), '[^A-Za-z0-9]', '', 'g')), '')"

def one(con, q):
    return con.execute(q).df().to_dict("records")[0]
def rows(con, q):
    cur = con.execute(q); cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def main():
    s = so()
    con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/_plan_spill", exist_ok=True)
    con.execute("SET memory_limit='12GB'"); con.execute("SET temp_directory='/tmp/_plan_spill'")
    out = {"as_of": dt.datetime.now(dt.timezone.utc).isoformat()}

    # ── manifest union: (sol_norm, resource_id) citations ──
    aliases = []
    for i, uri in enumerate(MANIFESTS):
        try:
            con.register(f"m{i}", lance.dataset(uri, storage_options=s)); aliases.append(f"m{i}")
        except Exception as e:
            out.setdefault("manifest_errors", []).append(f"{uri}: {str(e)[:120]}")
    usql = " UNION ALL ".join(
        f"SELECT solicitation_number, resource_id FROM {a}" for a in aliases)
    con.execute(f"""CREATE TEMP TABLE man AS
        SELECT {N.format(c='solicitation_number')} AS sol_norm, resource_id
        FROM ({usql}) WHERE resource_id IS NOT NULL""")
    con.execute("CREATE TEMP TABLE man_sol AS SELECT DISTINCT sol_norm FROM man WHERE sol_norm IS NOT NULL")
    out["manifest_distinct_files"] = con.execute("SELECT count(DISTINCT resource_id) FROM man").fetchone()[0]
    out["manifest_distinct_sol"] = con.execute("SELECT count(*) FROM man_sol").fetchone()[0]

    # winners award-key bridge (resource_id -> award key directly)
    con.register("winners", lance.dataset(WINNERS, storage_options=s))
    con.execute("""CREATE TEMP TABLE man_awardkey AS
        SELECT DISTINCT resource_id, k FROM (
            SELECT resource_id, contract_award_unique_key AS k FROM winners
              WHERE contract_award_unique_key IS NOT NULL AND resource_id IS NOT NULL
            UNION ALL
            SELECT resource_id, unnest(award_keys) AS k FROM winners
              WHERE award_keys IS NOT NULL AND resource_id IS NOT NULL
        ) WHERE k IS NOT NULL""")

    # ── PIID -> Sol# recovery from SAM universe ──
    con.register("uni_a", lance.dataset(UNI_A, storage_options=s))
    con.register("uni_r", lance.dataset(UNI_R, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE uni AS
        SELECT DISTINCT upper(trim(award_number)) AS piid, {N.format(c='solicitation_number')} AS sol
        FROM (SELECT award_number, solicitation_number FROM uni_a
              UNION ALL SELECT award_number, solicitation_number FROM uni_r)
        WHERE award_number IS NOT NULL AND trim(award_number) <> ''""")
    con.execute("CREATE TEMP TABLE piid_sol AS SELECT DISTINCT piid, sol FROM uni WHERE sol IS NOT NULL")

    # ── active awards with cohort flags + the set of in-cohort Sol#s / award keys ──
    con.register("gaa", lance.dataset(GAA, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE ga AS
        SELECT contract_award_unique_key AS k,
               coalesce(TRY_CAST(current_total_value_of_award AS DOUBLE),0) AS cur_val,
               upper(trim(business_size_code)) AS bsc,
               {N.format(c='solicitation_identifier')} AS fpds_sol,
               upper(trim(award_id_piid)) AS piid
        FROM gaa WHERE (active_current OR active_potential)""")
    # award -> resolved Sol# (fpds first, else PIID recovery)
    con.execute("""CREATE TEMP TABLE award_sol AS
        SELECT g.k, g.cur_val, g.bsc,
               coalesce(g.fpds_sol, ps.sol) AS sol_norm
        FROM ga g LEFT JOIN piid_sol ps ON g.piid = ps.piid""")

    COHORTS = {
        "all_active": "TRUE",
        "small_business": "bsc='S'",
        "sb_gt_500k": "bsc='S' AND cur_val > 5e5",
        "sb_gt_1m": "bsc='S' AND cur_val > 1e6",
        "sb_gt_5m": "bsc='S' AND cur_val > 5e6",
    }

    # downloaded + extracted resource sets (once)
    con.register("files", lance.dataset(FILES, storage_options=s))
    con.execute("CREATE TEMP TABLE dl AS SELECT DISTINCT resource_id FROM files WHERE status='downloaded'")
    con.register("req", lance.dataset(REQ, storage_options=s))
    con.execute("CREATE TEMP TABLE ex AS SELECT DISTINCT resource_id FROM req WHERE resource_id IS NOT NULL")
    out["global_downloaded_files"] = con.execute("SELECT count(*) FROM dl").fetchone()[0]
    out["global_extracted_files"] = con.execute("SELECT count(*) FROM ex").fetchone()[0]

    out["cohorts"] = {}
    for cname, cpred in COHORTS.items():
        # in-cohort Sol#s and award keys
        con.execute(f"CREATE OR REPLACE TEMP TABLE coh_sol AS SELECT DISTINCT sol_norm FROM award_sol WHERE {cpred} AND sol_norm IS NOT NULL")
        con.execute(f"CREATE OR REPLACE TEMP TABLE coh_key AS SELECT DISTINCT k FROM award_sol WHERE {cpred}")
        # cohort-bridged manifest resource_ids = via Sol# OR via winners award key
        con.execute("""CREATE OR REPLACE TEMP TABLE coh_res AS
            SELECT DISTINCT resource_id FROM (
                SELECT m.resource_id FROM man m JOIN coh_sol cs ON m.sol_norm = cs.sol_norm
                UNION
                SELECT ma.resource_id FROM man_awardkey ma JOIN coh_key ck ON ma.k = ck.k
            )""")
        # award grain: in-cohort awards whose Sol# OR key has ANY manifest file (gettable/link-covered)
        cov = one(con, """
            WITH a AS (SELECT k, sol_norm, cur_val FROM award_sol),
            covered AS (
                SELECT DISTINCT a.k FROM a
                LEFT JOIN man_sol ms ON a.sol_norm = ms.sol_norm
                LEFT JOIN coh_key_present ckp ON FALSE
                WHERE a.sol_norm IN (SELECT sol_norm FROM man_sol)
            )
            SELECT 1 AS dummy""") if False else {"dummy": 1}

        # file-grain backlog
        r = one(con, """
            SELECT
              count(*) AS link_files,
              count(*) FILTER (WHERE resource_id NOT IN (SELECT resource_id FROM dl)) AS download_pending_files,
              count(*) FILTER (WHERE resource_id IN (SELECT resource_id FROM dl)
                               AND resource_id NOT IN (SELECT resource_id FROM ex)) AS extract_pending_files,
              count(*) FILTER (WHERE resource_id NOT IN (SELECT resource_id FROM ex)) AS fully_pending_files,
              count(*) FILTER (WHERE resource_id IN (SELECT resource_id FROM dl)) AS downloaded_files,
              count(*) FILTER (WHERE resource_id IN (SELECT resource_id FROM ex)) AS extracted_files
            FROM coh_res""")
        # award grain: distinct cohort awards that have ANY link file; that are fully covered to extraction
        aw = one(con, f"""
            WITH coh AS (SELECT k, sol_norm FROM award_sol WHERE {cpred}),
            -- award -> its manifest resource_ids (via Sol# or winners key)
            ar AS (
                SELECT c.k, m.resource_id FROM coh c JOIN man m ON c.sol_norm = m.sol_norm
                UNION
                SELECT c.k, ma.resource_id FROM coh c JOIN man_awardkey ma ON ma.k = c.k
            )
            SELECT
              count(DISTINCT k) AS awards_with_link,
              count(DISTINCT k) FILTER (WHERE k IN (
                  SELECT k FROM ar a2 WHERE a2.resource_id IN (SELECT resource_id FROM dl))) AS awards_with_any_downloaded,
              count(DISTINCT k) FILTER (WHERE k IN (
                  SELECT k FROM ar a3 WHERE a3.resource_id IN (SELECT resource_id FROM ex))) AS awards_with_any_extracted,
              count(DISTINCT k) FILTER (WHERE k NOT IN (
                  SELECT k FROM ar a4 WHERE a4.resource_id IN (SELECT resource_id FROM ex))) AS awards_fully_extract_pending
            FROM ar""")
        sol = one(con, """SELECT count(DISTINCT sol_norm) AS sol_with_link FROM (
                    SELECT m.sol_norm FROM man m WHERE m.resource_id IN (SELECT resource_id FROM coh_res)
                    AND m.sol_norm IS NOT NULL)""")
        out["cohorts"][cname] = {**r, **aw, **sol}

    # ── BONDING YIELD: among already-extracted files in SB cohort, fraction with >=1 insurance_bonding ──
    con.execute(f"""CREATE TEMP TABLE sb_sol AS SELECT DISTINCT sol_norm FROM award_sol WHERE bsc='S' AND sol_norm IS NOT NULL""")
    con.execute(f"""CREATE TEMP TABLE sb_key AS SELECT DISTINCT k FROM award_sol WHERE bsc='S'""")
    con.execute("""CREATE TEMP TABLE sb_res AS
        SELECT DISTINCT resource_id FROM (
            SELECT m.resource_id FROM man m JOIN sb_sol ss ON m.sol_norm=ss.sol_norm
            UNION SELECT ma.resource_id FROM man_awardkey ma JOIN sb_key sk ON ma.k=sk.k)""")
    con.execute("""CREATE TEMP TABLE sb_ex AS
        SELECT DISTINCT resource_id FROM ex WHERE resource_id IN (SELECT resource_id FROM sb_res)""")
    bond = one(con, """
        SELECT
          (SELECT count(*) FROM sb_ex) AS sb_extracted_files,
          (SELECT count(DISTINCT resource_id) FROM req
             WHERE resource_id IN (SELECT resource_id FROM sb_ex)
               AND requirement_type='insurance_bonding') AS sb_files_with_bond,
          (SELECT count(DISTINCT resource_id) FROM req
             WHERE resource_id IN (SELECT resource_id FROM sb_ex)
               AND requirement_type='insurance_bonding'
               AND requirement_value LIKE '%bond%') AS sb_files_with_surety_bond""")
    bond["bond_value_distribution"] = rows(con, """
        SELECT requirement_value, count(*) AS rows, count(DISTINCT resource_id) AS files
        FROM req WHERE resource_id IN (SELECT resource_id FROM sb_ex)
          AND requirement_type='insurance_bonding'
        GROUP BY 1 ORDER BY rows DESC LIMIT 30""")
    bond["equipment_capability_distribution"] = rows(con, """
        SELECT requirement_value, count(*) AS rows, count(DISTINCT resource_id) AS files
        FROM req WHERE resource_id IN (SELECT resource_id FROM sb_ex)
          AND requirement_type='equipment_capability'
        GROUP BY 1 ORDER BY rows DESC LIMIT 15""")
    bond["pct_extracted_files_with_bond"] = round(
        100 * bond["sb_files_with_bond"] / bond["sb_extracted_files"], 2) if bond["sb_extracted_files"] else None
    out["bonding_yield_sb"] = bond

    # ── PARTITION VERIFICATION: pending SB resource_id set, abs(hash)%K for K in {2,3} ──
    # pending = SB-cohort manifest resource_ids NOT yet extracted (the fully-pending set)
    con.execute("""CREATE TEMP TABLE pending AS
        SELECT DISTINCT resource_id FROM sb_res WHERE resource_id NOT IN (SELECT resource_id FROM ex)""")
    out["partition"] = {"pending_sb_resource_ids": con.execute("SELECT count(*) FROM pending").fetchone()[0]}
    for K in (2, 3):
        dist = rows(con, f"""
            SELECT abs(hash(resource_id)) % {K} AS shard, count(*) AS n
            FROM pending GROUP BY 1 ORDER BY 1""")
        # disjointness is structural (one shard per id); report evenness
        tot = sum(d["n"] for d in dist) or 1
        for d in dist:
            d["pct"] = round(100 * d["n"] / tot, 3)
        spread = (max(d["n"] for d in dist) - min(d["n"] for d in dist)) / (tot / K) if dist else 0
        out["partition"][f"K{K}"] = {"shards": dist, "max_dev_from_even_pct": round(100 * spread, 3)}
    # 2-hex prefix partition (the bigthree_v2 mechanism) — show bucket spread
    pref = rows(con, """SELECT substr(resource_id,1,2) AS p2, count(*) n FROM pending GROUP BY 1 ORDER BY n DESC""")
    out["partition"]["distinct_2hex_prefixes"] = len(pref)
    out["partition"]["prefix_top5"] = pref[:5]
    out["partition"]["prefix_min_max"] = {"min": min((d["n"] for d in pref), default=0),
                                          "max": max((d["n"] for d in pref), default=0)}

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    print("DONE", file=sys.stderr)

if __name__ == "__main__":
    main()
