#!/usr/bin/env python3
"""P0 — resolve + freeze the Stage-2 crawl worklist for SB>$500K.

Bridges active SB>$500K awards -> resolved Sol# (FPDS solicitation_identifier, else
PIID->SAM-universe award_number recovery) -> crawlable (Sol# not in any manifest) ->
resolvable (Sol# exists in SAM universe) -> target SOLICITATION-sibling notices, minus
notices already in any manifest. Writes the target universe Lance dataset in the exact
shape sam_attachment_manifest.py reads (notice_id, solicitation_number, naics_code,
classification_code, title, posted_date, link), so the harvester crawls ONLY these notices.

Writes:  s3://data-sink/active/_stage2_target_sb500k/   (scratch worklist; underscore = non-SoR)

    doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/stage2_target_resolve_sb500k.py
"""
from __future__ import annotations
import json, os, sys
import duckdb, lance

def so():
    ep = os.environ.get("R2_ENDPOINT")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}

A = "s3://data-sink/active"
TARGET_URI = f"{A}/_stage2_target_sb500k/"
MAN = [f"{A}/sam_opps_attachment_manifest/", f"{A}/sam_opps_attachment_manifest_winners/",
       f"{A}/sam_opps_attachment_manifest_remediation/shard_000/",
       f"{A}/sam_opps_attachment_manifest_equipment_rental/shard_000/"] + \
      [f"{A}/sam_opps_attachment_manifest_play1/shard_00{i}/" for i in range(6)]
N = "nullif(upper(regexp_replace(trim({c}), '[^A-Za-z0-9]', '', 'g')), '')"
UCOLS = "notice_id, solicitation_number, naics_code, classification_code, title, posted_date, link, award_number"

def main():
    s = so()
    con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/duck_spill_s2", exist_ok=True)
    con.execute("SET memory_limit='10GB'"); con.execute("SET temp_directory='/tmp/duck_spill_s2'")

    con.register("ua", lance.dataset(f"s3://data-sink/sam-gov-opps/active/", storage_options=s))
    con.register("ur", lance.dataset(f"s3://data-sink/sam-gov-opps/archived/", storage_options=s))
    con.register("gaa", lance.dataset(f"{A}/govcon_active_awards/", storage_options=s))
    man_aliases = []
    for i, u in enumerate(MAN):
        try:
            con.register(f"m{i}", lance.dataset(u, storage_options=s)); man_aliases.append(f"m{i}")
        except Exception as e:
            print(f"manifest skip {u}: {str(e)[:80]}", file=sys.stderr)

    # universe (active first, then archived) deduped to one row per notice_id
    con.execute(f"""
        CREATE TEMP TABLE uni AS
        WITH u AS (
            SELECT {UCOLS}, 0 AS pri FROM ua
            UNION ALL
            SELECT {UCOLS}, 1 AS pri FROM ur
        ), ranked AS (
            SELECT *, row_number() OVER (PARTITION BY notice_id ORDER BY pri) rn FROM u
            WHERE notice_id IS NOT NULL
        )
        SELECT notice_id, solicitation_number, naics_code, classification_code, title,
               TRY_CAST(posted_date AS TIMESTAMP) AS posted_date, link,
               {N.format(c='solicitation_number')} AS sol_norm,
               upper(trim(award_number)) AS piid_norm
        FROM ranked WHERE rn = 1
    """)
    # manifest covered sol-set + covered notice-set
    msql = " UNION ALL ".join(f"SELECT solicitation_number, notice_id FROM {a}" for a in man_aliases)
    con.execute(f"CREATE TEMP TABLE man AS SELECT * FROM ({msql})")
    con.execute(f"CREATE TEMP TABLE man_sol AS SELECT DISTINCT s FROM (SELECT {N.format(c='solicitation_number')} s FROM man) WHERE s IS NOT NULL")
    con.execute("CREATE TEMP TABLE man_notice AS SELECT DISTINCT notice_id FROM man WHERE notice_id IS NOT NULL")

    # SB>$500K active awards -> resolved Sol#
    con.execute(f"""
        CREATE TEMP TABLE sb AS
        SELECT contract_award_unique_key AS k,
               {N.format(c='solicitation_identifier')} AS fpds_sol,
               upper(trim(award_id_piid)) AS piid
        FROM gaa
        WHERE (active_current OR active_potential) AND upper(trim(business_size_code))='S'
          AND coalesce(TRY_CAST(current_total_value_of_award AS DOUBLE),0) > 5e5
    """)
    # resolved sol = fpds_sol if present else PIID-recovered universe sol
    con.execute("""
        CREATE TEMP TABLE sb_sol AS
        SELECT DISTINCT sol FROM (
            SELECT fpds_sol AS sol FROM sb WHERE fpds_sol IS NOT NULL
            UNION
            SELECT u.sol_norm AS sol FROM sb JOIN uni u ON sb.piid = u.piid_norm
                WHERE sb.fpds_sol IS NULL AND u.sol_norm IS NOT NULL
        ) WHERE sol IS NOT NULL
    """)
    # crawlable = resolved sol not in any manifest ; resolvable = in universe (join below enforces it)
    con.execute("CREATE TEMP TABLE crawlable_sol AS SELECT sol FROM sb_sol WHERE sol NOT IN (SELECT s FROM man_sol)")

    diag = {
        "sb_gt_500k_awards": con.execute("SELECT count(*) FROM sb").fetchone()[0],
        "resolved_sol_distinct": con.execute("SELECT count(*) FROM sb_sol").fetchone()[0],
        "crawlable_sol_distinct": con.execute("SELECT count(*) FROM crawlable_sol").fetchone()[0],
        "crawlable_sol_in_universe": con.execute("SELECT count(*) FROM crawlable_sol WHERE sol IN (SELECT sol_norm FROM uni)").fetchone()[0],
    }

    # target notices = universe notices whose sol_norm is crawlable, minus notices already harvested
    con.execute("""
        CREATE TEMP TABLE target AS
        SELECT DISTINCT notice_id, solicitation_number, naics_code, classification_code,
               title, posted_date, link
        FROM uni
        WHERE sol_norm IN (SELECT sol FROM crawlable_sol)
          AND notice_id NOT IN (SELECT notice_id FROM man_notice)
    """)
    diag["target_notices"] = con.execute("SELECT count(*) FROM target").fetchone()[0]
    diag["target_already_in_manifest"] = con.execute(
        "SELECT count(*) FROM target WHERE notice_id IN (SELECT notice_id FROM man_notice)").fetchone()[0]
    diag["target_active"] = con.execute("SELECT count(*) FROM target t JOIN ua a USING(notice_id)").fetchone()[0]
    diag["target_distinct_notice_id"] = con.execute("SELECT count(DISTINCT notice_id) FROM target").fetchone()[0]

    tbl = con.execute("SELECT notice_id, solicitation_number, naics_code, classification_code, title, posted_date, link FROM target").to_arrow_table()
    lance.write_dataset(tbl, TARGET_URI, mode="overwrite", data_storage_version="2.0", storage_options=s)
    diag["written_uri"] = TARGET_URI
    diag["written_rows"] = tbl.num_rows
    print(json.dumps(diag, indent=2, default=str))

if __name__ == "__main__":
    main()
