#!/usr/bin/env python3
"""READ-ONLY probe — PSC decomposition of the "Parachute Prime" construction cohort.

Reuses the EXACT cohort predicates from scripts/archive/parachute_prime_probe.py so the
headline reconciles: parachute = 2,781, golden-civilian = 282. Decomposes by PSC
(macro 1-char category, exact 4-char code) for equipment-rental demand mapping.
NO writes — registration + SELECTs only.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/parachute_psc_probe.py > /tmp/parachute_psc.json
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

import duckdb
import lance

GAA = "s3://data-sink/active/govcon_active_awards/"


def r2_so():
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def rows(con, q):
    cur = con.execute(q)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def main():
    so = r2_so()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8; SET memory_limit='8GB'")
    con.register("gaa", lance.dataset(GAA, storage_options=so))

    # Base = active+future-end (excludes pop_unknown), with the locked parachute / golden flags.
    con.execute("""
        CREATE TEMP TABLE b AS
        SELECT
            contract_award_unique_key AS k,
            nullif(upper(trim(recipient_state_code)),'') AS hq_state,
            nullif(upper(trim(pop_state_code)),'')       AS pop_state,
            psc_code,
            nullif(trim(psc_description),'')             AS psc_desc,
            nullif(upper(substr(trim(psc_code),1,1)),'') AS psc_cat,
            current_total_value_of_award                 AS cur_val,
            has_subcontracting_plan,
            (upper(trim(coalesce(construction_wage_rate_requirements,''))) IN ('YES','Y','TRUE','T','1')
             OR upper(trim(coalesce(construction_wage_rate_requirements_code,'')))='Y') AS davis_bacon,
            (upper(trim(coalesce(awarding_agency_name,'')))<>'DEPARTMENT OF DEFENSE'
             AND upper(trim(coalesce(funding_agency_name,'')))<>'DEPARTMENT OF DEFENSE') AS not_dod
        FROM gaa WHERE (active_current OR active_potential)
    """)

    parachute = "davis_bacon AND hq_state IS NOT NULL AND pop_state IS NOT NULL AND hq_state<>pop_state"
    golden = f"{parachute} AND has_subcontracting_plan AND not_dod"
    out = {"as_of": dt.datetime.now(dt.timezone.utc).date().isoformat()}

    # ── reconciliation sanity ──
    out["cohort_reconciliation"] = {
        "parachute_total": con.execute(f"SELECT count(*) FROM b WHERE {parachute}").fetchone()[0],
        "golden_civilian_total": con.execute(f"SELECT count(*) FROM b WHERE {golden}").fetchone()[0],
        "parachute_psc_null": con.execute(f"SELECT count(*) FROM b WHERE {parachute} AND psc_code IS NULL").fetchone()[0],
    }

    # ── 1. macro categories (1-char PSC) — full breakdown so 'creep' is visible ──
    out["macro_categories"] = rows(con, f"""
        SELECT coalesce(psc_cat,'(null)') AS psc_category,
               count(*) AS award_count,
               round(100.0*count(*)/sum(count(*)) OVER (), 2) AS pct_of_cohort,
               coalesce(sum(cur_val),0) AS total_current_value
        FROM b WHERE {parachute}
        GROUP BY 1 ORDER BY award_count DESC""")
    out["macro_Y_construction"] = con.execute(
        f"SELECT count(*) FROM b WHERE {parachute} AND psc_cat='Y'").fetchone()[0]
    out["macro_Z_maintenance"] = con.execute(
        f"SELECT count(*) FROM b WHERE {parachute} AND psc_cat='Z'").fetchone()[0]

    # ── 2. micro taxonomy — top 20 exact 4-char PSC ──
    out["top20_psc_codes"] = rows(con, f"""
        SELECT psc_code,
               coalesce(any_value(psc_desc), '(no description)') AS psc_description,
               count(*) AS award_count,
               coalesce(sum(cur_val),0) AS total_current_value
        FROM b WHERE {parachute}
        GROUP BY psc_code ORDER BY award_count DESC, total_current_value DESC LIMIT 20""")
    # how much of the cohort the top-20 covers
    out["top20_coverage"] = con.execute(f"""
        WITH t AS (SELECT psc_code, count(*) c FROM b WHERE {parachute}
                   GROUP BY 1 ORDER BY c DESC LIMIT 20)
        SELECT count(*) AS top20_award_count
        FROM b WHERE {parachute} AND psc_code IN (SELECT psc_code FROM t)""").fetchone()[0]

    # ── 3. Golden 282 PSC overlay — top 5 ──
    out["golden_top5_psc"] = rows(con, f"""
        SELECT psc_code,
               coalesce(any_value(psc_desc), '(no description)') AS psc_description,
               count(*) AS award_count,
               coalesce(sum(cur_val),0) AS total_current_value
        FROM b WHERE {golden}
        GROUP BY psc_code ORDER BY award_count DESC, total_current_value DESC LIMIT 5""")
    out["golden_macro_split"] = rows(con, f"""
        SELECT coalesce(psc_cat,'(null)') AS psc_category, count(*) AS award_count
        FROM b WHERE {golden} GROUP BY 1 ORDER BY award_count DESC""")

    # who is forcing it — golden cohort by awarding agency (re-derive agency on b)
    con.execute("""CREATE TEMP TABLE bg AS
        SELECT b.*, g.awarding_agency_name AS agency
        FROM b JOIN gaa g ON b.k = g.contract_award_unique_key""")
    out["golden_by_agency"] = rows(con, f"""
        SELECT agency, count(*) AS award_count, coalesce(sum(cur_val),0) AS total_current_value
        FROM bg WHERE {golden} GROUP BY 1 ORDER BY award_count DESC LIMIT 8""")
    # top PSC within each of the leading golden agencies (what each agency forces)
    out["golden_agency_top_psc"] = rows(con, f"""
        WITH g5 AS (
            SELECT agency FROM bg WHERE {golden} GROUP BY 1 ORDER BY count(*) DESC LIMIT 5),
            ranked AS (
                SELECT agency, psc_code, any_value(psc_desc) AS psc_description, count(*) AS n,
                       row_number() OVER (PARTITION BY agency ORDER BY count(*) DESC) AS rn
                FROM bg WHERE {golden} AND agency IN (SELECT agency FROM g5)
                GROUP BY agency, psc_code)
        SELECT agency, psc_code, coalesce(psc_description,'(no description)') AS psc_description, n
        FROM ranked WHERE rn <= 3 ORDER BY agency, n DESC""")

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
