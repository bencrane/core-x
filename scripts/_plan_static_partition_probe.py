#!/usr/bin/env python3
"""READ-ONLY verification of the STATIC, deterministic, disjoint-by-construction partition.

Supersedes the dynamic-claim model: R2/Lance is append-only with eventual landing and NO
transactional claim store, so disjointness MUST be guaranteed at ASSIGNMENT, not arbitrated
at runtime. Every resource_id maps to exactly one shard by construction:

    shard = abs(hash(resource_id)) % N_TOTAL_SHARDS      (N generous: 96)

Proves:
  (1) the N shards split the pending SB backlog evenly (~within 1%);
  (2) shard assignment is provably disjoint — every pending resource_id is in exactly one
      shard, and a simulated accounts×sessions ownership map (disjoint shard RANGES per
      account; individual shards handed to sessions) owns zero id twice;
  (3) per-shard priority-band composition (SB>$5M / >$1M / >$500K / remainder) so waves can
      be priority-ordered WITHIN a shard with value landing first.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/_plan_static_partition_probe.py > /tmp/_plan_static_partition.json
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
    f"{A}/sam_opps_attachment_manifest/", f"{A}/sam_opps_attachment_manifest_winners/",
    f"{A}/sam_opps_attachment_manifest_remediation/shard_000/",
    f"{A}/sam_opps_attachment_manifest_equipment_rental/shard_000/",
] + [f"{A}/sam_opps_attachment_manifest_play1/shard_00{i}/" for i in range(6)]
WINNERS = f"{A}/sam_opps_attachment_manifest_winners/"
REQ = f"{A}/govcon_award_requirements/"
GAA = f"{A}/govcon_active_awards/"
UNI_A = "s3://data-sink/sam-gov-opps/active/"
UNI_R = "s3://data-sink/sam-gov-opps/archived/"
N = "nullif(upper(regexp_replace(trim({c}), '[^A-Za-z0-9]', '', 'g')), '')"
N_TOTAL_SHARDS = 96

def rows(con, q):
    cur = con.execute(q); cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def main():
    s = so()
    con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=8")
    out = {"as_of": dt.datetime.now(dt.timezone.utc).isoformat(), "n_total_shards": N_TOTAL_SHARDS}

    aliases = []
    for i, uri in enumerate(MANIFESTS):
        try:
            con.register(f"m{i}", lance.dataset(uri, storage_options=s)); aliases.append(f"m{i}")
        except Exception:
            pass
    usql = " UNION ALL ".join(f"SELECT solicitation_number, resource_id FROM {a}" for a in aliases)
    con.execute(f"""CREATE TEMP TABLE man AS
        SELECT {N.format(c='solicitation_number')} AS sol_norm, resource_id
        FROM ({usql}) WHERE resource_id IS NOT NULL""")
    con.register("winners", lance.dataset(WINNERS, storage_options=s))
    con.execute("""CREATE TEMP TABLE man_awardkey AS SELECT DISTINCT resource_id, k FROM (
        SELECT resource_id, contract_award_unique_key AS k FROM winners
          WHERE contract_award_unique_key IS NOT NULL AND resource_id IS NOT NULL
        UNION ALL SELECT resource_id, unnest(award_keys) AS k FROM winners
          WHERE award_keys IS NOT NULL AND resource_id IS NOT NULL) WHERE k IS NOT NULL""")
    # PIID recovery so the pending set matches the backlog probe (FPDS sol ∪ PIID-recovered sol)
    con.register("uni_a", lance.dataset(UNI_A, storage_options=s))
    con.register("uni_r", lance.dataset(UNI_R, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE piid_sol AS
        SELECT DISTINCT upper(trim(award_number)) AS piid, {N.format(c='solicitation_number')} AS sol
        FROM (SELECT award_number, solicitation_number FROM uni_a
              UNION ALL SELECT award_number, solicitation_number FROM uni_r)
        WHERE award_number IS NOT NULL AND trim(award_number) <> ''
          AND {N.format(c='solicitation_number')} IS NOT NULL""")
    con.register("gaa", lance.dataset(GAA, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE ga AS
        SELECT contract_award_unique_key AS k,
               coalesce(TRY_CAST(current_total_value_of_award AS DOUBLE),0) AS cur_val,
               upper(trim(business_size_code)) AS bsc,
               {N.format(c='solicitation_identifier')} AS fpds_sol,
               upper(trim(award_id_piid)) AS piid
        FROM gaa WHERE (active_current OR active_potential)""")
    con.execute("""CREATE TEMP TABLE award_sol AS
        SELECT g.k, g.cur_val, g.bsc, coalesce(g.fpds_sol, ps.sol) AS sol_norm
        FROM ga g LEFT JOIN piid_sol ps ON g.piid = ps.piid WHERE g.bsc='S'""")
    con.execute("CREATE TEMP TABLE sb_sol AS SELECT DISTINCT sol_norm FROM award_sol WHERE sol_norm IS NOT NULL")
    con.execute("CREATE TEMP TABLE sb_key AS SELECT DISTINCT k FROM award_sol")
    # resource_id -> best priority band (max award value across its bridged awards)
    con.execute("""CREATE TEMP TABLE res_band AS
        WITH ar AS (
            SELECT m.resource_id, a.cur_val FROM man m JOIN award_sol a ON m.sol_norm=a.sol_norm
            UNION ALL
            SELECT ma.resource_id, a.cur_val FROM man_awardkey ma JOIN award_sol a ON ma.k=a.k)
        SELECT resource_id, max(cur_val) AS max_val FROM ar GROUP BY 1""")
    con.register("req", lance.dataset(REQ, storage_options=s))
    con.execute("CREATE TEMP TABLE ex AS SELECT DISTINCT resource_id FROM req WHERE resource_id IS NOT NULL")
    con.execute("""CREATE TEMP TABLE pending AS
        SELECT resource_id, max_val,
               CASE WHEN max_val > 5e6 THEN 'sb_gt_5m'
                    WHEN max_val > 1e6 THEN 'sb_1m_5m'
                    WHEN max_val > 5e5 THEN 'sb_500k_1m'
                    ELSE 'sb_remainder' END AS prio_band,
               abs(hash(resource_id)) % {n} AS shard
        FROM res_band WHERE resource_id NOT IN (SELECT resource_id FROM ex)""".format(n=N_TOTAL_SHARDS))

    out["pending_sb_resource_ids"] = con.execute("SELECT count(*) FROM pending").fetchone()[0]
    out["pending_distinct"] = con.execute("SELECT count(DISTINCT resource_id) FROM pending").fetchone()[0]

    # (1) even split across N shards
    shard_dist = rows(con, "SELECT shard, count(*) AS n FROM pending GROUP BY 1 ORDER BY 1")
    tot = sum(d["n"] for d in shard_dist) or 1
    mn = min(d["n"] for d in shard_dist); mx = max(d["n"] for d in shard_dist)
    even = tot / N_TOTAL_SHARDS
    out["shard_even_split"] = {
        "n_shards_populated": len(shard_dist),
        "ideal_per_shard": round(even, 1),
        "min": mn, "max": mx,
        "max_dev_from_even_pct": round(100 * max(mx - even, even - mn) / even, 3),
        "all_within_2pct": (mx - even) / even < 0.02 and (even - mn) / even < 0.02,
    }

    # (2) disjoint-by-construction: each id in exactly one shard (id maps to one shard deterministically)
    out["disjointness"] = {
        "rows_eq_distinct": out["pending_sb_resource_ids"] == out["pending_distinct"],
        "ids_with_multiple_shards": con.execute(
            "SELECT count(*) FROM (SELECT resource_id FROM pending GROUP BY 1 HAVING count(DISTINCT shard) > 1)").fetchone()[0],
        "sum_of_shards_eq_total": tot == out["pending_sb_resource_ids"],
    }

    # (2b) simulate accounts×sessions ownership over DISJOINT shard RANGES; assert zero id owned twice.
    # 3 accounts get contiguous shard ranges; within an account, sessions take individual shards.
    K_ACCOUNTS = 3
    per = N_TOTAL_SHARDS // K_ACCOUNTS
    acct_ranges = {a: list(range(a * per, (a + 1) * per if a < K_ACCOUNTS - 1 else N_TOTAL_SHARDS))
                   for a in range(K_ACCOUNTS)}
    SESSIONS_PER_ACCOUNT = [4, 7, 3]   # VARIABLE per account — the concurrency asset
    # session s in account a owns shards acct_ranges[a][s::n_sessions] (round-robin within range)
    owner_of_shard: dict[int, tuple] = {}
    double_owned = 0
    for a in range(K_ACCOUNTS):
        ns = SESSIONS_PER_ACCOUNT[a]
        for sess in range(ns):
            for shard in acct_ranges[a][sess::ns]:
                if shard in owner_of_shard:
                    double_owned += 1
                owner_of_shard[shard] = (a, sess)
    # map ownership to ids and assert disjoint at id grain
    id_shard = rows(con, "SELECT shard, count(*) n FROM pending GROUP BY 1")
    owned_ids = sum(d["n"] for d in id_shard if d["shard"] in owner_of_shard)
    out["ownership_sim"] = {
        "k_accounts": K_ACCOUNTS,
        "sessions_per_account": SESSIONS_PER_ACCOUNT,
        "total_sessions": sum(SESSIONS_PER_ACCOUNT),
        "account_shard_ranges": {str(a): [r[0], r[-1]] for a, r in acct_ranges.items()},
        "shards_assigned": len(owner_of_shard),
        "shards_double_owned": double_owned,            # MUST be 0
        "ids_covered_by_ownership": owned_ids,
        "ids_covered_eq_total": owned_ids == out["pending_sb_resource_ids"],
        "every_shard_exactly_one_owner": len(owner_of_shard) == N_TOTAL_SHARDS and double_owned == 0,
    }

    # (3) per-shard priority composition (so waves can be value-ordered within a shard)
    out["priority_band_totals"] = rows(con,
        "SELECT prio_band, count(*) n, count(DISTINCT shard) shards_touched FROM pending GROUP BY 1 ORDER BY 2 DESC")
    # worst-case per-shard skew for the scarce top band
    top = rows(con, "SELECT shard, count(*) n FROM pending WHERE prio_band='sb_gt_5m' GROUP BY 1")
    out["sb_gt_5m_per_shard"] = {
        "shards_with_any": len(top),
        "min": min((d["n"] for d in top), default=0),
        "max": max((d["n"] for d in top), default=0),
        "avg": round(sum(d["n"] for d in top) / max(1, len(top)), 1),
    }

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    print("DONE", file=sys.stderr)

if __name__ == "__main__":
    main()
