#!/usr/bin/env python3
"""READ-ONLY probe — SAM attachment-link SUBSTRATE coverage of active awards.

Answers: have we already harvested the attachment-link substrate (resource_id ->
download_url, Stage-2 manifest) for our active awards, or must we still go get it?

Funnel: Stage-1 universe (notices) -> Stage-2 manifest (attachment links, NO bytes)
        -> Stage-3 downloaded bytes -> extraction (govcon_award_requirements).

Bridge (the Award -> Sol# -> attachments join the reference doc said was not yet run):
  an active award is "link-covered" if its solicitation_identifier matches a
  solicitation_number present in ANY harvested manifest, OR its award key is present in
  the winners manifest's contract_award_unique_key / award_keys. Sol# normalized
  (uppercased, non-alphanumerics stripped) on both sides.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/attach_substrate_coverage_probe.py > /tmp/attach_cov.json
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
    ("m_original", f"{A}/sam_opps_attachment_manifest/"),
    ("m_winners", f"{A}/sam_opps_attachment_manifest_winners/"),
    ("m_remediation", f"{A}/sam_opps_attachment_manifest_remediation/shard_000/"),
    ("m_equip", f"{A}/sam_opps_attachment_manifest_equipment_rental/shard_000/"),
    ("m_sb500k", f"{A}/sam_opps_attachment_manifest_sb500k/"),
] + [(f"m_play1_{i}", f"{A}/sam_opps_attachment_manifest_play1/shard_00{i}/") for i in range(6)]

NORM = "nullif(upper(regexp_replace(trim({c}), '[^A-Za-z0-9]', '', 'g')), '')"

def rows(con, q):
    cur = con.execute(q); cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def one(con, q):
    return con.execute(q).df().to_dict("records")[0]

def main():
    s = so()
    con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/duck_spill_cov", exist_ok=True)
    con.execute("SET memory_limit='10GB'"); con.execute("SET temp_directory='/tmp/duck_spill_cov'")
    out = {"as_of": dt.datetime.now(dt.timezone.utc).date().isoformat()}

    # register manifests that open
    live_m = []
    for alias, uri in MANIFESTS:
        try:
            con.register(alias, lance.dataset(uri, storage_options=s)); live_m.append((alias, uri))
        except Exception as e:
            out.setdefault("manifest_open_errors", {})[uri] = str(e)[:150]
    out["manifests_registered"] = [u for _, u in live_m]

    # union all manifest attachment citations (sol# + resource_id + notice_id)
    union_sql = " UNION ALL ".join(
        f"SELECT solicitation_number, resource_id, notice_id FROM {a}" for a, _ in live_m)
    con.execute(f"CREATE TEMP TABLE man AS SELECT * FROM ({union_sql})")
    out["manifest_substrate"] = one(con, f"""
        SELECT count(*) AS total_citations,
               count(DISTINCT resource_id) AS distinct_files,
               count(DISTINCT {NORM.format(c='solicitation_number')}) AS distinct_solicitations,
               count(DISTINCT notice_id) AS distinct_notices
        FROM man""")
    con.execute(f"CREATE TEMP TABLE man_sol AS SELECT DISTINCT {NORM.format(c='solicitation_number')} AS sol_norm FROM man WHERE solicitation_number IS NOT NULL")

    # winners manifest carries award keys directly (already bridged)
    con.register("winners", lance.dataset(f"{A}/sam_opps_attachment_manifest_winners/", storage_options=s))
    con.execute("""
        CREATE TEMP TABLE man_awardkey AS
        SELECT DISTINCT k FROM (
            SELECT contract_award_unique_key AS k FROM winners WHERE contract_award_unique_key IS NOT NULL
            UNION ALL SELECT unnest(award_keys) AS k FROM winners WHERE award_keys IS NOT NULL
        ) WHERE k IS NOT NULL""")
    out["winners_award_keys"] = con.execute("SELECT count(*) FROM man_awardkey").fetchone()[0]

    # Stage 1 universe sizes
    uni = {}
    for nm, uri in [("active", f"s3://data-sink/sam-gov-opps/active/"),
                    ("archived", f"s3://data-sink/sam-gov-opps/archived/")]:
        try:
            con.register(f"uni_{nm}", lance.dataset(uri, storage_options=s))
            uni[nm] = con.execute(f"SELECT count(*) FROM uni_{nm}").fetchone()[0]
        except Exception as e:
            uni[nm] = f"err:{str(e)[:80]}"
    out["universe_notices"] = uni

    # Stage 3 downloaded ledger
    con.register("files", lance.dataset(f"{A}/sam_attachment_files/", storage_options=s))
    out["downloaded_ledger_by_status"] = rows(con, """
        SELECT status, count(*) AS rows, count(DISTINCT resource_id) AS distinct_files
        FROM files GROUP BY 1 ORDER BY rows DESC""")

    # extraction reach
    con.register("req", lance.dataset(f"{A}/govcon_award_requirements/", storage_options=s))
    out["extraction_reach"] = one(con, """
        SELECT count(DISTINCT resource_id) AS distinct_files_extracted,
               count(DISTINCT contract_award_unique_key) AS distinct_awards_with_requirements,
               count(DISTINCT notice_id) AS distinct_notices,
               count(DISTINCT solicitation_number) AS distinct_solicitations
        FROM req""")

    # ── BRIDGE: active-award coverage by the link substrate ──
    con.register("gaa", lance.dataset(f"{A}/govcon_active_awards/", storage_options=s))
    con.execute(f"""
        CREATE TEMP TABLE ga AS
        SELECT contract_award_unique_key AS k,
               {NORM.format(c='solicitation_identifier')} AS sol_norm,
               upper(trim(business_size_code)) AS bsc,
               nullif(upper(trim(psc_code)),'') AS psc,
               upper(trim(coalesce(labor_standards,''))) AS labor_std,
               upper(trim(coalesce(construction_wage_rate_requirements,''))) AS cwr
        FROM gaa WHERE (active_current OR active_potential)
    """)

    def cov_block(where_label, where_sql):
        return one(con, f"""
            SELECT
                count(*) AS awards,
                count(*) FILTER (WHERE sol_norm IS NOT NULL) AS have_solicitation_id,
                count(*) FILTER (WHERE sol_norm IN (SELECT sol_norm FROM man_sol)) AS link_covered_via_solnum,
                count(*) FILTER (WHERE k IN (SELECT k FROM man_awardkey)) AS link_covered_via_awardkey,
                count(*) FILTER (WHERE sol_norm IN (SELECT sol_norm FROM man_sol)
                                    OR k IN (SELECT k FROM man_awardkey)) AS link_covered_any,
                count(*) FILTER (WHERE k IN (SELECT DISTINCT contract_award_unique_key FROM req)) AS extracted_reach
            FROM ga WHERE {where_sql}""") | {"cohort": where_label}

    out["coverage_all_active"] = cov_block("all_active", "TRUE")
    out["coverage_small_business"] = cov_block("small_business", "bsc='S'")
    out["coverage_sb_big_three_or_sca"] = cov_block(
        "sb_whale_services", "bsc='S' AND (psc IN ('DA01','R499','R425') OR labor_std='YES')")
    out["coverage_sb_construction"] = cov_block(
        "sb_construction", "bsc='S' AND cwr='YES'")

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    print("DONE", file=sys.stderr)

if __name__ == "__main__":
    main()
