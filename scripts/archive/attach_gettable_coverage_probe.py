#!/usr/bin/env python3
"""READ-ONLY probe — "gettable" reframing of attachment-link coverage + dollar sizing.

Separates the un-gettable floor (no way to bridge an award to a solicitation #) from the
ADDRESSABLE population, and reports coverage as a share of the addressable set, by award
value band.

Gettability is computed two ways:
  fpds_sol   : award carries a solicitation_identifier in FPDS (the direct bridge).
  + universe : recover a Sol# for FPDS-blank awards via award_id_piid -> SAM universe
               (award_number) -> solicitation_number. Shrinks the floor.
covered = the (fpds or recovered) Sol# is present in a harvested attachment manifest.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/attach_gettable_coverage_probe.py > /tmp/gettable.json
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
    f"{A}/sam_opps_attachment_manifest_sb500k/",
] + [f"{A}/sam_opps_attachment_manifest_play1/shard_00{i}/" for i in range(6)]
SOLN = "nullif(upper(regexp_replace(trim({c}), '[^A-Za-z0-9]', '', 'g')), '')"

def one(con, q):
    return con.execute(q).df().to_dict("records")[0]

def main():
    s = so()
    con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/duck_spill_get", exist_ok=True)
    con.execute("SET memory_limit='10GB'"); con.execute("SET temp_directory='/tmp/duck_spill_get'")
    out = {"as_of": dt.datetime.now(dt.timezone.utc).date().isoformat()}

    # manifest sol# set
    aliases = []
    for i, uri in enumerate(MANIFESTS):
        try:
            con.register(f"m{i}", lance.dataset(uri, storage_options=s)); aliases.append(f"m{i}")
        except Exception as e:
            out.setdefault("manifest_errors", []).append(f"{uri}: {str(e)[:100]}")
    usql = " UNION ALL ".join(f"SELECT solicitation_number FROM {a}" for a in aliases)
    # NB: filter NULLs OUT of the set — a NULL in an IN(...) subquery poisons the
    # predicate to NULL (three-valued logic) and silently drops "NOT covered" rows.
    con.execute(f"CREATE TEMP TABLE man_sol AS SELECT DISTINCT sol FROM (SELECT {SOLN.format(c='solicitation_number')} AS sol FROM ({usql})) WHERE sol IS NOT NULL")
    out["manifest_distinct_solicitations"] = con.execute("SELECT count(*) FROM man_sol").fetchone()[0]

    # SAM universe: piid -> sol recovery
    con.register("uni_a", lance.dataset("s3://data-sink/sam-gov-opps/active/", storage_options=s))
    con.register("uni_r", lance.dataset("s3://data-sink/sam-gov-opps/archived/", storage_options=s))
    con.execute(f"""
        CREATE TEMP TABLE uni AS
        SELECT DISTINCT upper(trim(award_number)) AS piid,
               {SOLN.format(c='solicitation_number')} AS sol
        FROM (SELECT award_number, solicitation_number FROM uni_a
              UNION ALL SELECT award_number, solicitation_number FROM uni_r)
        WHERE award_number IS NOT NULL AND trim(award_number) <> ''
    """)
    # piids that recover a Sol# at all, and piids that recover a COVERED Sol#
    con.execute("CREATE TEMP TABLE piid_has_sol AS SELECT DISTINCT piid FROM uni WHERE sol IS NOT NULL")
    con.execute("CREATE TEMP TABLE piid_covered AS SELECT DISTINCT piid FROM uni WHERE sol IN (SELECT sol FROM man_sol)")
    out["universe_piid_with_sol"] = con.execute("SELECT count(*) FROM piid_has_sol").fetchone()[0]

    # active awards with flags
    con.register("gaa", lance.dataset(f"{A}/govcon_active_awards/", storage_options=s))
    con.execute(f"""
        CREATE TEMP TABLE ga AS
        WITH base AS (
            SELECT contract_award_unique_key AS k,
                   coalesce(TRY_CAST(current_total_value_of_award AS DOUBLE),0) AS cur_val,
                   upper(trim(business_size_code)) AS bsc,
                   {SOLN.format(c='solicitation_identifier')} AS fpds_sol,
                   upper(trim(award_id_piid)) AS piid
            FROM gaa WHERE (active_current OR active_potential)
        )
        SELECT *,
            (fpds_sol IS NOT NULL)                                   AS has_fpds_sol,
            COALESCE(fpds_sol IS NOT NULL OR piid IN (SELECT piid FROM piid_has_sol), FALSE) AS gettable,
            COALESCE(fpds_sol IN (SELECT sol FROM man_sol)
                OR piid IN (SELECT piid FROM piid_covered), FALSE)   AS covered
        FROM base
    """)

    THRESHOLDS = [("all", "TRUE"), ("gt_500k", "cur_val > 5e5"),
                  ("gt_1m", "cur_val > 1e6"), ("gt_5m", "cur_val > 5e6")]
    COHORTS = [("all_active", "TRUE"), ("small_business", "bsc='S'")]

    out["matrix"] = []
    for cname, cpred in COHORTS:
        for tname, tpred in THRESHOLDS:
            r = one(con, f"""
                SELECT count(*) AS awards,
                    coalesce(sum(cur_val),0) AS total_value,
                    count(*) FILTER (WHERE has_fpds_sol)               AS have_fpds_sol,
                    count(*) FILTER (WHERE gettable)                   AS gettable,
                    count(*) FILTER (WHERE NOT gettable)               AS floor_ungettable,
                    count(*) FILTER (WHERE covered)                    AS covered,
                    count(*) FILTER (WHERE gettable AND NOT covered)   AS gettable_uncovered,
                    coalesce(sum(cur_val) FILTER (WHERE gettable),0)   AS gettable_value,
                    coalesce(sum(cur_val) FILTER (WHERE covered),0)    AS covered_value,
                    coalesce(sum(cur_val) FILTER (WHERE gettable AND NOT covered),0) AS gettable_uncovered_value
                FROM ga WHERE {cpred} AND {tpred}""")
            r["cohort"] = cname; r["threshold"] = tname
            # derived shares
            r["pct_gettable_of_awards"] = round(100*r["gettable"]/r["awards"], 1) if r["awards"] else None
            r["pct_covered_of_gettable"] = round(100*r["covered"]/r["gettable"], 1) if r["gettable"] else None
            r["pct_covered_of_all"] = round(100*r["covered"]/r["awards"], 1) if r["awards"] else None
            out["matrix"].append(r)

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    print("DONE", file=sys.stderr)

if __name__ == "__main__":
    main()
