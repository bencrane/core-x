#!/usr/bin/env python3
"""P3/P4 verify — Stage-2 SB>$500K harvest: coverage uplift + Stage-3 handoff.

    doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/stage2_sb500k_verify.py
"""
from __future__ import annotations
import json, os, sys
import duckdb, lance

def so():
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ["R2_ENDPOINT"], "region": "auto"}

A = "s3://data-sink/active"
NEW = f"{A}/sam_opps_attachment_manifest_sb500k/"
OLD = [f"{A}/sam_opps_attachment_manifest/", f"{A}/sam_opps_attachment_manifest_winners/",
       f"{A}/sam_opps_attachment_manifest_remediation/shard_000/",
       f"{A}/sam_opps_attachment_manifest_equipment_rental/shard_000/"] + \
      [f"{A}/sam_opps_attachment_manifest_play1/shard_00{i}/" for i in range(6)]
N = "nullif(upper(regexp_replace(trim({c}), '[^A-Za-z0-9]', '', 'g')), '')"

def main():
    s = so()
    con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='10GB'"); os.makedirs("/tmp/duck_spill_v2", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill_v2'")
    out = {}

    con.register("newman", lance.dataset(NEW, storage_options=s))
    out["new_manifest"] = con.execute("""
        SELECT count(*) AS rows, count(DISTINCT resource_id) AS distinct_files,
               count(DISTINCT notice_id) AS distinct_notices,
               count(DISTINCT solicitation_number) AS distinct_solicitations
        FROM newman""").df().to_dict("records")[0]

    # old manifest union sol-set
    al = []
    for i, u in enumerate(OLD):
        try: con.register(f"o{i}", lance.dataset(u, storage_options=s)); al.append(f"o{i}")
        except Exception: pass
    osql = " UNION ALL ".join(f"SELECT solicitation_number FROM {a}" for a in al)
    con.execute(f"CREATE TEMP TABLE old_sol AS SELECT DISTINCT s FROM (SELECT {N.format(c='solicitation_number')} s FROM ({osql})) WHERE s IS NOT NULL")
    con.execute(f"CREATE TEMP TABLE new_sol AS SELECT DISTINCT {N.format(c='solicitation_number')} s FROM newman WHERE solicitation_number IS NOT NULL")
    out["new_sol_distinct"] = con.execute("SELECT count(*) FROM new_sol").fetchone()[0]
    out["new_sol_net_new"] = con.execute("SELECT count(*) FROM new_sol WHERE s NOT IN (SELECT s FROM old_sol)").fetchone()[0]

    # universe for PIID recovery
    con.register("ua", lance.dataset("s3://data-sink/sam-gov-opps/active/", storage_options=s))
    con.register("ur", lance.dataset("s3://data-sink/sam-gov-opps/archived/", storage_options=s))
    con.execute(f"""CREATE TEMP TABLE uni AS
        SELECT DISTINCT upper(trim(award_number)) AS piid, {N.format(c='solicitation_number')} AS sol
        FROM (SELECT award_number, solicitation_number FROM ua UNION ALL SELECT award_number, solicitation_number FROM ur)
        WHERE award_number IS NOT NULL""")

    # SB>$500K awards -> resolved sol, before/after covered
    con.register("gaa", lance.dataset(f"{A}/govcon_active_awards/", storage_options=s))
    con.execute(f"""CREATE TEMP TABLE sb AS
        SELECT contract_award_unique_key AS k,
               {N.format(c='solicitation_identifier')} AS fpds_sol,
               upper(trim(award_id_piid)) AS piid
        FROM gaa WHERE (active_current OR active_potential) AND upper(trim(business_size_code))='S'
          AND coalesce(TRY_CAST(current_total_value_of_award AS DOUBLE),0) > 5e5""")
    con.execute("""CREATE TEMP TABLE sb_res AS
        SELECT sb.k,
               coalesce(sb.fpds_sol, (SELECT min(u.sol) FROM uni u WHERE u.piid=sb.piid AND u.sol IS NOT NULL)) AS sol
        FROM sb""")
    out["coverage_uplift_sb_gt_500k"] = con.execute("""
        SELECT count(*) AS awards,
               count(*) FILTER (WHERE sol IS NOT NULL) AS have_resolved_sol,
               count(*) FILTER (WHERE sol IN (SELECT s FROM old_sol)) AS covered_before,
               count(*) FILTER (WHERE sol IN (SELECT s FROM old_sol) OR sol IN (SELECT s FROM new_sol)) AS covered_after,
               count(*) FILTER (WHERE sol NOT IN (SELECT s FROM old_sol) AND sol IN (SELECT s FROM new_sol)) AS newly_covered
        FROM sb_res""").df().to_dict("records")[0]

    # P4 handoff: new resource_ids not yet downloaded = Stage-3 download-pending
    con.register("files", lance.dataset(f"{A}/sam_attachment_files/", storage_options=s))
    out["stage3_handoff"] = con.execute("""
        WITH nf AS (SELECT DISTINCT resource_id FROM newman WHERE resource_id IS NOT NULL),
             dl AS (SELECT DISTINCT resource_id FROM files WHERE status='downloaded')
        SELECT (SELECT count(*) FROM nf) AS new_distinct_files,
               (SELECT count(*) FROM nf WHERE resource_id IN (SELECT resource_id FROM dl)) AS already_downloaded,
               (SELECT count(*) FROM nf WHERE resource_id NOT IN (SELECT resource_id FROM dl)) AS new_download_pending
        """).df().to_dict("records")[0]

    print(json.dumps(out, indent=2, default=str))

if __name__ == "__main__":
    main()
