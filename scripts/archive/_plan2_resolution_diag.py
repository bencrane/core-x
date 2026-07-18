#!/usr/bin/env python3
"""READ-ONLY diagnostic — WHY do so few crawlable SB>$500K Sol#s resolve to a universe
notice? Decompose the resolution funnel: FPDS Sol# vs PIID-recovered Sol#, and check
whether the universe has the Sol# at all (the real ceiling) vs a normalization miss.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/_plan2_resolution_diag.py > /tmp/_plan2_resolution_diag.json
"""
from __future__ import annotations
import datetime as dt, json, os, sys
import duckdb, lance


def so():
    ep = os.environ.get("R2_ENDPOINT")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


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


def main():
    s = so()
    con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/_plan2_spill2", exist_ok=True)
    con.execute("SET memory_limit='12GB'"); con.execute("SET temp_directory='/tmp/_plan2_spill2'")
    out = {"as_of": dt.datetime.now(dt.timezone.utc).isoformat()}

    # manifest sol set
    aliases = []
    for i, uri in enumerate(MANIFESTS):
        try:
            con.register(f"m{i}", lance.dataset(uri, storage_options=s)); aliases.append(f"m{i}")
        except Exception:
            pass
    usql = " UNION ALL ".join(f"SELECT solicitation_number FROM {a}" for a in aliases)
    con.execute(f"CREATE TEMP TABLE man_sol AS SELECT DISTINCT {N.format(c='solicitation_number')} AS sol_norm FROM ({usql}) WHERE {N.format(c='solicitation_number')} IS NOT NULL")

    # universe sol set + piid->sol
    con.register("uni_a", lance.dataset(UNI_A, storage_options=s))
    con.register("uni_r", lance.dataset(UNI_R, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE uni AS
        SELECT notice_id, {N.format(c='solicitation_number')} AS sol_norm, upper(trim(award_number)) AS piid
        FROM (SELECT notice_id, solicitation_number, award_number FROM uni_a
              UNION ALL SELECT notice_id, solicitation_number, award_number FROM uni_r)""")
    con.execute("CREATE TEMP TABLE uni_sol AS SELECT DISTINCT sol_norm FROM uni WHERE sol_norm IS NOT NULL")
    con.execute("CREATE TEMP TABLE uni_piid AS SELECT DISTINCT piid, sol_norm FROM uni WHERE piid IS NOT NULL AND piid<>'' AND sol_norm IS NOT NULL")
    out["universe_distinct_sol_norm"] = con.execute("SELECT count(*) FROM uni_sol").fetchone()[0]

    # active awards
    con.register("gaa", lance.dataset(GAA, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE ga AS
        SELECT contract_award_unique_key AS k,
               coalesce(TRY_CAST(current_total_value_of_award AS DOUBLE),0) AS cur_val,
               upper(trim(business_size_code)) AS bsc,
               {N.format(c='solicitation_identifier')} AS fpds_sol,
               upper(trim(award_id_piid)) AS piid
        FROM gaa WHERE (active_current OR active_potential) AND business_size_code='S'
                  AND coalesce(TRY_CAST(current_total_value_of_award AS DOUBLE),0) > 5e5""")
    con.execute("""CREATE TEMP TABLE asol AS
        SELECT g.k, g.fpds_sol, g.piid, ps.sol_norm AS piid_sol,
               coalesce(g.fpds_sol, ps.sol_norm) AS sol_norm
        FROM ga g LEFT JOIN uni_piid ps ON g.piid = ps.piid""")

    out["sb_gt_500k_award_funnel"] = one(con, """
        SELECT count(*) AS awards,
               count(*) FILTER (WHERE fpds_sol IS NOT NULL) AS have_fpds_sol,
               count(*) FILTER (WHERE fpds_sol IS NULL AND piid_sol IS NOT NULL) AS recovered_via_piid_only,
               count(*) FILTER (WHERE sol_norm IS NOT NULL) AS gettable,
               count(*) FILTER (WHERE sol_norm IS NOT NULL AND sol_norm IN (SELECT sol_norm FROM man_sol)) AS covered,
               count(*) FILTER (WHERE sol_norm IS NOT NULL AND sol_norm NOT IN (SELECT sol_norm FROM man_sol)) AS crawlable
        FROM asol""")

    # the crawlable Sol# set, and WHERE the resolution leaks
    con.execute("""CREATE TEMP TABLE crawl_sol AS
        SELECT DISTINCT sol_norm FROM asol
        WHERE sol_norm IS NOT NULL AND sol_norm NOT IN (SELECT sol_norm FROM man_sol)""")
    out["crawlable_sol_resolution"] = one(con, """
        SELECT count(*) AS crawlable_sol_distinct,
               count(*) FILTER (WHERE sol_norm IN (SELECT sol_norm FROM uni_sol)) AS in_universe_sol,
               count(*) FILTER (WHERE sol_norm NOT IN (SELECT sol_norm FROM uni_sol)) AS NOT_in_universe_sol
        FROM crawl_sol""")

    # decompose crawlable Sol# by origin (fpds vs piid-recovered) and universe presence
    con.execute("""CREATE TEMP TABLE crawl_sol_origin AS
        SELECT DISTINCT sol_norm,
               bool_or(fpds_sol IS NOT NULL) AS from_fpds,
               bool_or(fpds_sol IS NULL AND piid_sol IS NOT NULL) AS from_piid_only
        FROM asol
        WHERE sol_norm IS NOT NULL AND sol_norm NOT IN (SELECT sol_norm FROM man_sol)
        GROUP BY sol_norm""")
    out["crawlable_sol_by_origin"] = one(con, """
        SELECT
          count(*) FILTER (WHERE from_fpds) AS fpds_origin,
          count(*) FILTER (WHERE from_fpds AND sol_norm IN (SELECT sol_norm FROM uni_sol)) AS fpds_in_universe,
          count(*) FILTER (WHERE from_piid_only) AS piid_origin,
          count(*) FILTER (WHERE from_piid_only AND sol_norm IN (SELECT sol_norm FROM uni_sol)) AS piid_in_universe
        FROM crawl_sol_origin""")

    # SANITY: are COVERED Sol#s (already harvested) present in the universe? if YES,
    # the gap is genuine FPDS-Sol#-not-in-SAM, not a normalization bug.
    con.execute("""CREATE TEMP TABLE covered_sol AS
        SELECT DISTINCT sol_norm FROM asol
        WHERE sol_norm IS NOT NULL AND sol_norm IN (SELECT sol_norm FROM man_sol)""")
    out["covered_sol_in_universe_sanity"] = one(con, """
        SELECT count(*) AS covered_sol_distinct,
               count(*) FILTER (WHERE sol_norm IN (SELECT sol_norm FROM uni_sol)) AS in_universe
        FROM covered_sol""")

    # The notice-type breakdown of the resolved target notices (are they actually
    # solicitation-type, where attachments live? or award notices?)
    con.register("uni_a2", lance.dataset(UNI_A, storage_options=s))
    con.register("uni_r2", lance.dataset(UNI_R, storage_options=s))
    # need notice_type / base_type columns — check availability
    cols_a = set(lance.dataset(UNI_A, storage_options=s).schema.names)
    out["universe_has_notice_type"] = "notice_type" in cols_a
    out["universe_has_base_type"] = "base_type" in cols_a

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    print("DONE", file=sys.stderr)


if __name__ == "__main__":
    main()
