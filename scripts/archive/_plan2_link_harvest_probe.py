#!/usr/bin/env python3
"""READ-ONLY sizing probe for the STAGE-2 attachment-link harvest wave plan.

Throwaway (prefix `_plan2_`). Sizes the per-notice /resources CRAWL needed to harvest the
attachment links (Stage-2 manifest rows) for the SB>$500K gettable-but-uncovered backlog
(and the full-SB crawlable extension). NO crawl, NO writes — only reads R2 Lance + DuckDB.

Funnel (Stage 2 only): active SB award (current_total_value > $500K) that is gettable
(carries an FPDS solicitation_identifier OR recovers a Sol# via PIID->SAM-universe) BUT
whose Sol# is NOT in any harvested manifest  ->  resolve to the SOLICITATION NOTICE id(s)
in the universe (Sol#<->solicitation_number AND PIID<->award_number sibling recovery)
->  the distinct target notice_ids are the crawl worklist (one /resources GET each).

Probes:
  1. RESOLVE target notices for SB>$500K and full-SB extension; split active/archived.
  2. CONFIRM target notices are genuinely un-harvested (∩ union-of-manifest notices ≈ 0).
  3. ATTACHMENT-YIELD forecast from existing manifests (attachments/notice distribution).
  4. CRAWL-SIZE + pacing estimate at the residential envelope (conc 6, ~8 req/s).
  5. PARTITION verification: abs(hash(notice_id)) % K disjoint, even worker shards.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/_plan2_link_harvest_probe.py > /tmp/_plan2_link_harvest.json
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
] + [f"{A}/sam_opps_attachment_manifest_play1/shard_00{i}/" for i in range(6)]
GAA = f"{A}/govcon_active_awards/"
UNI_A = "s3://data-sink/sam-gov-opps/active/"
UNI_R = "s3://data-sink/sam-gov-opps/archived/"
# canonical Sol# normalization (matches the reusable probes / plan brief)
N = "nullif(upper(regexp_replace(trim({c}), '[^A-Za-z0-9]', '', 'g')), '')"


def one(con, q):
    return con.execute(q).df().to_dict("records")[0]


def rows(con, q):
    cur = con.execute(q); cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def main():
    s = so()
    con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/_plan2_spill", exist_ok=True)
    con.execute("SET memory_limit='12GB'"); con.execute("SET temp_directory='/tmp/_plan2_spill'")
    out = {"as_of": dt.datetime.now(dt.timezone.utc).isoformat()}

    # ── manifest union: sol_norm + notice_id (citation grain) ──
    aliases = []
    for i, uri in enumerate(MANIFESTS):
        try:
            con.register(f"m{i}", lance.dataset(uri, storage_options=s)); aliases.append(f"m{i}")
        except Exception as e:
            out.setdefault("manifest_errors", []).append(f"{uri}: {str(e)[:120]}")
    usql = " UNION ALL ".join(
        f"SELECT solicitation_number, notice_id, resource_id FROM {a}" for a in aliases)
    con.execute(f"""CREATE TEMP TABLE man AS
        SELECT {N.format(c='solicitation_number')} AS sol_norm, notice_id, resource_id
        FROM ({usql})""")
    con.execute("CREATE TEMP TABLE man_sol  AS SELECT DISTINCT sol_norm FROM man WHERE sol_norm IS NOT NULL")
    con.execute("CREATE TEMP TABLE man_note AS SELECT DISTINCT notice_id FROM man WHERE notice_id IS NOT NULL")
    out["manifest_distinct_sol"]    = con.execute("SELECT count(*) FROM man_sol").fetchone()[0]
    out["manifest_distinct_notices"] = con.execute("SELECT count(*) FROM man_note").fetchone()[0]

    # ── SAM universe: build (sol_norm -> notice_id) and (piid -> notice_id) maps,
    #    tagged active vs archived. attachments live on the SOLICITATION notice, so the
    #    universe Sol#/PIID match IS the sibling-notice resolution. ──
    con.register("uni_a", lance.dataset(UNI_A, storage_options=s))
    con.register("uni_r", lance.dataset(UNI_R, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE uni AS
        SELECT notice_id, src,
               {N.format(c='solicitation_number')} AS sol_norm,
               upper(trim(award_number)) AS piid
        FROM (
            SELECT notice_id, solicitation_number, award_number, 'active'   AS src FROM uni_a
            UNION ALL
            SELECT notice_id, solicitation_number, award_number, 'archived' AS src FROM uni_r
        ) WHERE notice_id IS NOT NULL""")
    con.execute("CREATE TEMP TABLE uni_sol  AS SELECT DISTINCT sol_norm, notice_id, src FROM uni WHERE sol_norm IS NOT NULL")
    con.execute("CREATE TEMP TABLE uni_piid AS SELECT DISTINCT piid, sol_norm FROM uni WHERE piid IS NOT NULL AND piid <> '' AND sol_norm IS NOT NULL")
    out["universe_rows"] = {
        "active":   con.execute("SELECT count(*) FROM uni WHERE src='active'").fetchone()[0],
        "archived": con.execute("SELECT count(*) FROM uni WHERE src='archived'").fetchone()[0],
    }

    # ── active awards w/ cohort flags + resolved Sol# (fpds first, else PIID recovery) ──
    con.register("gaa", lance.dataset(GAA, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE ga AS
        SELECT contract_award_unique_key AS k,
               coalesce(TRY_CAST(current_total_value_of_award AS DOUBLE),0) AS cur_val,
               upper(trim(business_size_code)) AS bsc,
               {N.format(c='solicitation_identifier')} AS fpds_sol,
               upper(trim(award_id_piid)) AS piid
        FROM gaa WHERE (active_current OR active_potential)""")
    con.execute("""CREATE TEMP TABLE award_sol AS
        SELECT g.k, g.cur_val, g.bsc, g.fpds_sol, g.piid,
               coalesce(g.fpds_sol, ps.sol_norm) AS sol_norm
        FROM ga g LEFT JOIN uni_piid ps ON g.piid = ps.piid""")

    # gettable = resolved a Sol# at all; covered = that Sol# is in a manifest.
    con.execute("""CREATE TEMP TABLE award_flags AS
        SELECT *,
               (sol_norm IS NOT NULL) AS gettable,
               COALESCE(sol_norm IN (SELECT sol_norm FROM man_sol), FALSE) AS covered
        FROM award_sol""")

    COHORTS = {
        "sb_gt_500k": "bsc='S' AND cur_val > 5e5",
        "sb_all":     "bsc='S'",
    }

    out["cohorts"] = {}
    for cname, cpred in COHORTS.items():
        # award-grain funnel (reconcile to the established coverage doc)
        f = one(con, f"""
            SELECT count(*) AS awards,
                   coalesce(sum(cur_val),0) AS total_value,
                   count(*) FILTER (WHERE gettable) AS gettable,
                   count(*) FILTER (WHERE covered)  AS covered,
                   count(*) FILTER (WHERE gettable AND NOT covered) AS crawlable,
                   coalesce(sum(cur_val) FILTER (WHERE gettable AND NOT covered),0) AS crawlable_value
            FROM award_flags WHERE {cpred}""")

        # the crawlable Sol# set for this cohort (gettable AND NOT covered)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE coh_sol AS
            SELECT DISTINCT sol_norm FROM award_flags
            WHERE {cpred} AND gettable AND NOT covered AND sol_norm IS NOT NULL""")
        n_sol = con.execute("SELECT count(*) FROM coh_sol").fetchone()[0]

        # RESOLVE each crawlable Sol# to its SOLICITATION notice id(s) in the universe.
        # This is the Sol#-sibling resolution: the document package lives on the
        # solicitation notice, found by Sol#<->solicitation_number in the universe.
        con.execute("""CREATE OR REPLACE TEMP TABLE coh_notes AS
            SELECT DISTINCT u.notice_id, u.src
            FROM uni_sol u JOIN coh_sol c ON u.sol_norm = c.sol_norm""")
        note_split = rows(con, "SELECT src, count(DISTINCT notice_id) AS notices FROM coh_notes GROUP BY 1 ORDER BY 1")
        n_notes = con.execute("SELECT count(DISTINCT notice_id) FROM coh_notes").fetchone()[0]

        # how many crawlable Sol#s resolve to >=1 notice vs zero (un-resolvable floor)
        sol_resolved = con.execute("""SELECT count(*) FROM coh_sol c
            WHERE EXISTS (SELECT 1 FROM uni_sol u WHERE u.sol_norm=c.sol_norm)""").fetchone()[0]

        # PROBE 2: of those target notices, how many are ALREADY in a manifest (should be ~0)
        already = con.execute("""SELECT count(DISTINCT notice_id) FROM coh_notes
            WHERE notice_id IN (SELECT notice_id FROM man_note)""").fetchone()[0]

        # awards-per-notice ratio (crawlable awards in cohort with a resolvable Sol#)
        crawl_awards_resolvable = con.execute(f"""
            SELECT count(DISTINCT k) FROM award_flags af
            WHERE {cpred} AND af.gettable AND NOT af.covered
              AND af.sol_norm IN (SELECT sol_norm FROM coh_sol c
                                  WHERE EXISTS (SELECT 1 FROM uni_sol u WHERE u.sol_norm=c.sol_norm))
        """).fetchone()[0]

        out["cohorts"][cname] = {
            **f,
            "crawlable_sol_distinct": n_sol,
            "crawlable_sol_resolved_to_a_notice": sol_resolved,
            "crawlable_sol_unresolvable_floor": n_sol - sol_resolved,
            "target_notices_distinct": n_notes,
            "target_notices_split": note_split,
            "target_notices_already_in_manifest": already,
            "target_notices_genuinely_unharvested": n_notes - already,
            "crawl_awards_with_resolvable_notice": crawl_awards_resolvable,
            "awards_per_notice_ratio": round(crawl_awards_resolvable / n_notes, 3) if n_notes else None,
        }

    # ── PROBE 3: attachment-yield forecast — attachments-per-notice from existing manifests ──
    # distribution over notices that HAVE >=1 attachment row (citation grain per notice)
    yld = one(con, """
        WITH per AS (
            SELECT notice_id, count(*) AS atts FROM man WHERE notice_id IS NOT NULL GROUP BY 1)
        SELECT count(*) AS notices_with_attach,
               avg(atts) AS mean_att_per_notice,
               median(atts) AS median_att_per_notice,
               quantile_cont(atts, 0.9) AS p90_att_per_notice,
               max(atts) AS max_att_per_notice
        FROM per""")
    out["attachment_yield_per_harvested_notice"] = yld

    # ── PROBE 4: crawl-size + pacing estimate at the residential envelope ──
    # one /resources GET per distinct target notice. proven envelope: conc=6 @ ~8 req/s.
    # the manifest harvester is single-stream w/ inter_call_sleep=0.12 (~8 req/s effective);
    # the download envelope proves conc=6 global token bucket @ ~8 req/s is WAF-safe.
    def pacing(n_notices):
        # mean-yield forecast of manifest rows produced
        mean = yld["mean_att_per_notice"] or 0
        med = yld["median_att_per_notice"] or 0
        return {
            "requests": n_notices,
            "forecast_manifest_rows_mean": round(n_notices * mean),
            "forecast_manifest_rows_median": round(n_notices * med),
            "wallclock_hours_at_8rps": round(n_notices / 8 / 3600, 2),
            "wallclock_hours_at_8rps_x6workers": round(n_notices / 8 / 3600, 2),  # global bucket caps aggregate; workers hide latency, not rate
            "wallclock_hours_single_stream_0p12s": round(n_notices * 0.12 / 3600, 2),
        }
    out["crawl_size"] = {
        cname: pacing(out["cohorts"][cname]["target_notices_genuinely_unharvested"])
        for cname in COHORTS
    }

    # ── PROBE 5: partition verification — abs(hash(notice_id)) % K disjoint/even shards ──
    # use the SB>$500K target notice set (the primary crawl worklist).
    con.execute("""CREATE OR REPLACE TEMP TABLE coh_sol AS
        SELECT DISTINCT sol_norm FROM award_flags
        WHERE bsc='S' AND cur_val > 5e5 AND gettable AND NOT covered AND sol_norm IS NOT NULL""")
    con.execute("""CREATE OR REPLACE TEMP TABLE target_notes AS
        SELECT DISTINCT u.notice_id
        FROM uni_sol u JOIN coh_sol c ON u.sol_norm = c.sol_norm
        WHERE u.notice_id NOT IN (SELECT notice_id FROM man_note)""")
    tot = con.execute("SELECT count(*) FROM target_notes").fetchone()[0]
    dist_chk = con.execute("SELECT count(*), count(DISTINCT notice_id) FROM target_notes").fetchone()
    part = {"target_notices": tot, "rows_eq_distinct": (dist_chk[0] == dist_chk[1])}
    for K in (4, 8, 12):
        dist = rows(con, f"""SELECT abs(hash(notice_id)) % {K} AS shard, count(*) AS n
            FROM target_notes GROUP BY 1 ORDER BY 1""")
        t = sum(d["n"] for d in dist) or 1
        for d in dist:
            d["pct"] = round(100 * d["n"] / t, 3)
        ideal = t / K
        spread = (max(d["n"] for d in dist) - min(d["n"] for d in dist)) / ideal if dist and ideal else 0
        # disjointness: each notice maps to exactly one shard by construction → sum == total
        part[f"K{K}"] = {"shards": dist, "sum_eq_total": (t == tot),
                         "max_dev_from_even_pct": round(100 * spread, 2)}
    out["partition"] = part

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    print("DONE", file=sys.stderr)


if __name__ == "__main__":
    main()
