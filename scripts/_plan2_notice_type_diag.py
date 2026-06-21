#!/usr/bin/env python3
"""READ-ONLY — notice-type composition of the resolved SB>$500K target notices, and the
per-value-band crawlable->resolvable->notices funnel for the sizing table.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/_plan2_notice_type_diag.py > /tmp/_plan2_notice_type.json
"""
from __future__ import annotations
import datetime as dt, json, os, sys
import duckdb, lance


def so():
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ["R2_ENDPOINT"], "region": "auto"}


A = "s3://data-sink/active"
MANIFESTS = [
    f"{A}/sam_opps_attachment_manifest/",
    f"{A}/sam_opps_attachment_manifest_winners/",
    f"{A}/sam_opps_attachment_manifest_remediation/shard_000/",
    f"{A}/sam_opps_attachment_manifest_equipment_rental/shard_000/",
] + [f"{A}/sam_opps_attachment_manifest_play1/shard_00{i}/" for i in range(6)]
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
    os.makedirs("/tmp/_plan2_spill3", exist_ok=True)
    con.execute("SET memory_limit='12GB'"); con.execute("SET temp_directory='/tmp/_plan2_spill3'")
    out = {"as_of": dt.datetime.now(dt.timezone.utc).isoformat()}

    aliases = []
    for i, uri in enumerate(MANIFESTS):
        try:
            con.register(f"m{i}", lance.dataset(uri, storage_options=s)); aliases.append(f"m{i}")
        except Exception:
            pass
    usql = " UNION ALL ".join(f"SELECT solicitation_number FROM {a}" for a in aliases)
    con.execute(f"CREATE TEMP TABLE man_sol AS SELECT DISTINCT {N.format(c='solicitation_number')} AS sol_norm FROM ({usql}) WHERE {N.format(c='solicitation_number')} IS NOT NULL")

    con.register("uni_a", lance.dataset(UNI_A, storage_options=s))
    con.register("uni_r", lance.dataset(UNI_R, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE uni AS
        SELECT notice_id, src, notice_type, base_type,
               {N.format(c='solicitation_number')} AS sol_norm, upper(trim(award_number)) AS piid
        FROM (SELECT notice_id, solicitation_number, award_number, notice_type, base_type, 'active' AS src FROM uni_a
              UNION ALL SELECT notice_id, solicitation_number, award_number, notice_type, base_type, 'archived' AS src FROM uni_r)
        WHERE notice_id IS NOT NULL""")
    con.execute("CREATE TEMP TABLE uni_sol AS SELECT DISTINCT sol_norm, notice_id, src, notice_type, base_type FROM uni WHERE sol_norm IS NOT NULL")
    con.execute("CREATE TEMP TABLE uni_piid AS SELECT DISTINCT piid, sol_norm FROM uni WHERE piid IS NOT NULL AND piid<>'' AND sol_norm IS NOT NULL")

    con.register("gaa", lance.dataset(GAA, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE ga AS
        SELECT contract_award_unique_key AS k,
               coalesce(TRY_CAST(current_total_value_of_award AS DOUBLE),0) AS cur_val,
               upper(trim(business_size_code)) AS bsc,
               {N.format(c='solicitation_identifier')} AS fpds_sol,
               upper(trim(award_id_piid)) AS piid
        FROM gaa WHERE (active_current OR active_potential) AND business_size_code='S'""")
    con.execute("""CREATE TEMP TABLE asol AS
        SELECT g.k, g.cur_val, coalesce(g.fpds_sol, ps.sol_norm) AS sol_norm
        FROM ga g LEFT JOIN uni_piid ps ON g.piid = ps.piid""")

    # per-band funnel
    BANDS = {"sb_gt_500k": "cur_val > 5e5", "sb_gt_1m": "cur_val > 1e6",
             "sb_gt_5m": "cur_val > 5e6", "sb_all": "TRUE"}
    out["bands"] = {}
    for b, pred in BANDS.items():
        con.execute(f"""CREATE OR REPLACE TEMP TABLE cs AS
            SELECT DISTINCT sol_norm FROM asol
            WHERE {pred} AND sol_norm IS NOT NULL AND sol_norm NOT IN (SELECT sol_norm FROM man_sol)""")
        con.execute("""CREATE OR REPLACE TEMP TABLE cn AS
            SELECT DISTINCT u.notice_id, u.src FROM uni_sol u JOIN cs c ON u.sol_norm=c.sol_norm
            WHERE u.notice_id NOT IN (SELECT notice_id FROM (
                SELECT notice_id FROM uni WHERE 1=0))""")  # placeholder; un-harvested handled below
        r = one(con, """SELECT
            (SELECT count(*) FROM cs) AS crawlable_sol,
            (SELECT count(*) FROM cs WHERE sol_norm IN (SELECT sol_norm FROM uni_sol)) AS resolvable_sol,
            (SELECT count(DISTINCT notice_id) FROM cn) AS target_notices,
            (SELECT count(DISTINCT notice_id) FROM cn WHERE src='active') AS notices_active,
            (SELECT count(DISTINCT notice_id) FROM cn WHERE src='archived') AS notices_archived""")
        out["bands"][b] = r

    # notice-type composition of SB>$500k resolved target notices
    con.execute("""CREATE OR REPLACE TEMP TABLE cs AS
        SELECT DISTINCT sol_norm FROM asol
        WHERE cur_val > 5e5 AND sol_norm IS NOT NULL AND sol_norm NOT IN (SELECT sol_norm FROM man_sol)""")
    con.execute("""CREATE OR REPLACE TEMP TABLE tn AS
        SELECT DISTINCT u.notice_id, u.notice_type, u.base_type
        FROM uni_sol u JOIN cs c ON u.sol_norm=c.sol_norm""")
    out["sb_gt_500k_target_notice_type"] = rows(con, """
        SELECT coalesce(base_type, notice_type, '(null)') AS type, count(DISTINCT notice_id) AS notices
        FROM tn GROUP BY 1 ORDER BY notices DESC""")

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    print("DONE", file=sys.stderr)


if __name__ == "__main__":
    main()
