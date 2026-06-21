#!/usr/bin/env python3
"""READ-ONLY probe — "Cash Crunch" capital-lending targeting across the Small-Business
active-award cohort. Sizes payroll-mobilization, asset/equipment, and bonding/insurance
crunches, plus a 90-day recency cut ("hot leads").

Single R2 connection. Base = Small-Business primes on active awards (active_current OR
active_potential), joined LEFT to govcon_award_scope_requirements (PDF extractions) so
coverage denominators are explicit. NO writes — registration + SELECTs only.

Small Business definition (authoritative FPDS determination): business_size_code='S' OR
business_size LIKE 'SMALL%'. A diagnostic sizes the socioeconomic-flag union ("or similar
small/disadvantaged flags") but the four sections run on the COD determination.

CAVEATS surfaced in-output:
- scope-requirements coverage is PARTIAL — every "extracted from PDF" count is a floor.
- equipment_capability is a free-form LLM field (high-cardinality, NOT controlled vocab);
  its Top-10 is fragmented free-text, reported with the cardinality so noise is visible.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/cash_crunch_probe.py > /tmp/cash_crunch.json
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

import duckdb
import lance

A = "s3://data-sink/active"
GAA = f"{A}/govcon_active_awards/"
ASR = f"{A}/govcon_award_scope_requirements/"
BIG_THREE = ("DA01", "R499", "R425")
RECENCY_DAYS = 90


def r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def log(m: str) -> None:
    print(m, file=sys.stderr, flush=True)


def rows(con, q):
    cur = con.execute(q)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def one(con, q):
    return con.execute(q).df().to_dict("records")[0]


def main() -> int:
    so = r2_so()
    as_of = dt.datetime.now(dt.timezone.utc).date()
    cutoff = (as_of - dt.timedelta(days=RECENCY_DAYS)).isoformat()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/duck_spill_cc", exist_ok=True)
    con.execute("SET memory_limit='8GB'")
    con.execute("SET temp_directory='/tmp/duck_spill_cc'")
    con.register("gaa", lance.dataset(GAA, storage_options=so))
    con.register("asr", lance.dataset(ASR, storage_options=so))

    out: dict = {"as_of": as_of.isoformat(), "recency_cutoff": cutoff,
                 "recency_days": RECENCY_DAYS, "big_three": list(BIG_THREE)}

    # ── DIAGNOSTICS: confirm Small-Business literals before defining the cohort ──
    diag: dict = {}
    diag["business_size_histogram"] = rows(con, """
        SELECT upper(trim(coalesce(business_size,'')))      AS business_size,
               upper(trim(coalesce(business_size_code,'')))  AS business_size_code,
               count(*) AS n
        FROM gaa WHERE (active_current OR active_potential)
        GROUP BY 1,2 ORDER BY n DESC""")
    out["diagnostics"] = diag

    # ── BASE: Small-Business primes on active awards ──
    log("materializing Small-Business active cohort …")
    con.execute(f"""
        CREATE TEMP TABLE sb AS
        SELECT
            contract_award_unique_key                                   AS k,
            nullif(upper(trim(psc_code)), '')                           AS psc,
            upper(trim(coalesce(labor_standards, '')))                  AS labor_std,
            upper(trim(coalesce(construction_wage_rate_requirements,''))) AS cwr,
            coalesce(TRY_CAST(current_total_value_of_award   AS DOUBLE), 0) AS cur_val,
            coalesce(TRY_CAST(potential_total_value_of_award AS DOUBLE), 0) AS pot_val,
            latest_action_date                                          AS act,
            (latest_action_date >= DATE '{cutoff}')                     AS is_recent,
            small_disadvantaged_business, service_disabled_veteran_owned_business,
            women_owned_small_business,
            historically_underutilized_business_zone_hubzone_firm        AS hubzone,
            c8a_program_participant
        FROM gaa
        WHERE (active_current OR active_potential)
          AND (upper(trim(business_size_code)) = 'S' OR upper(trim(business_size)) LIKE 'SMALL%')
    """)
    sb_n = con.execute("SELECT count(*) FROM sb").fetchone()[0]
    out["sb_active_awards"] = sb_n
    out["sb_total_current_value"] = con.execute("SELECT sum(cur_val) FROM sb").fetchone()[0]
    out["sb_act_null"] = con.execute("SELECT count(*) FROM sb WHERE act IS NULL").fetchone()[0]
    log(f"SB active awards = {sb_n:,}")

    # sensitivity: union with socio set-aside flags ("or similar small/disadvantaged")
    out["sb_definition_sensitivity"] = one(con, f"""
        SELECT
            (SELECT count(*) FROM gaa WHERE (active_current OR active_potential)
               AND (upper(trim(business_size_code))='S' OR upper(trim(business_size)) LIKE 'SMALL%')) AS cod_small_business,
            (SELECT count(*) FROM gaa WHERE (active_current OR active_potential)
               AND ( upper(trim(business_size_code))='S' OR upper(trim(business_size)) LIKE 'SMALL%'
                  OR small_disadvantaged_business OR service_disabled_veteran_owned_business
                  OR women_owned_small_business
                  OR historically_underutilized_business_zone_hubzone_firm
                  OR c8a_program_participant)) AS cod_or_any_setaside_flag
    """)

    # ── scope-requirements local projection (PDF extractions) ──
    con.execute("""
        CREATE TEMP TABLE asr_local AS
        SELECT contract_award_unique_key AS k,
               has_equipment_capability,
               equipment_capability_values,
               (has_equipment_capability AND equipment_capability_values IS NOT NULL
                    AND len(equipment_capability_values) > 0)                 AS has_equip,
               has_insurance_bonding,
               insurance_bonding_values,
               (has_insurance_bonding AND insurance_bonding_values IS NOT NULL
                    AND len(insurance_bonding_values) > 0)                    AS has_bond
        FROM asr
    """)
    out["sb_in_scope_requirements"] = con.execute(
        "SELECT count(*) FROM sb JOIN asr_local USING (k)").fetchone()[0]

    # ── SECTION 1: Whale Win (payroll mobilization) ──
    # cohort = SB AND (psc in big three OR labor_standards='YES')
    con.execute(f"""
        CREATE TEMP TABLE s1 AS
        SELECT * FROM sb
        WHERE psc IN {BIG_THREE} OR labor_std = 'YES'
    """)
    s1: dict = {}
    s1["cohort_definition"] = "SB AND (psc_code IN (DA01,R499,R425) OR labor_standards='YES')"
    s1["cohort_awards"] = con.execute("SELECT count(*) FROM s1").fetchone()[0]
    s1["cohort_total_current_value"] = con.execute("SELECT sum(cur_val) FROM s1").fetchone()[0]
    s1["gt_5m"] = one(con, "SELECT count(*) AS awards, coalesce(sum(cur_val),0) AS total_current_value FROM s1 WHERE cur_val > 5e6")
    s1["gt_10m"] = one(con, "SELECT count(*) AS awards, coalesce(sum(cur_val),0) AS total_current_value FROM s1 WHERE cur_val > 1e7")
    # split by which leg of the OR (texture)
    s1["by_membership"] = one(con, f"""
        SELECT
            count(*) FILTER (WHERE psc IN {BIG_THREE})                    AS via_big_three_psc,
            count(*) FILTER (WHERE labor_std='YES')                       AS via_labor_standards,
            count(*) FILTER (WHERE psc IN {BIG_THREE} AND labor_std='YES') AS via_both
        FROM s1""")
    out["section1_whale_win"] = s1

    # ── SECTION 2: Iron Crunch (SB construction + equipment mandate) ──
    con.execute("CREATE TEMP TABLE s2 AS SELECT * FROM sb WHERE cwr = 'YES'")
    s2: dict = {}
    s2["cohort_definition"] = "SB AND construction_wage_rate_requirements='YES'"
    s2["sb_construction_awards"] = con.execute("SELECT count(*) FROM s2").fetchone()[0]
    s2["sb_construction_total_current_value"] = con.execute("SELECT sum(cur_val) FROM s2").fetchone()[0]
    s2["joined_to_scope_requirements"] = con.execute(
        "SELECT count(*) FROM s2 JOIN asr_local USING (k)").fetchone()[0]
    s2["with_populated_equipment_capability"] = con.execute(
        "SELECT count(*) FROM s2 JOIN asr_local USING (k) WHERE has_equip").fetchone()[0]
    s2["equipment_group_total_current_value"] = con.execute(
        "SELECT coalesce(sum(s2.cur_val),0) FROM s2 JOIN asr_local a USING (k) WHERE a.has_equip").fetchone()[0]
    # unnest equipment for this group; lowered+trimmed to collapse case fragmentation (field is free-form LLM text)
    con.execute("""
        CREATE TEMP TABLE s2_equip AS
        SELECT s2.k, lower(trim(unnest(a.equipment_capability_values))) AS equip
        FROM s2 JOIN asr_local a USING (k) WHERE a.has_equip
    """)
    s2["equipment_total_occurrences"] = con.execute("SELECT count(*) FROM s2_equip").fetchone()[0]
    s2["equipment_distinct_strings"] = con.execute("SELECT count(DISTINCT equip) FROM s2_equip").fetchone()[0]
    s2["equipment_top10"] = rows(con, """
        SELECT equip AS equipment, count(*) AS award_occurrences
        FROM s2_equip GROUP BY 1 ORDER BY award_occurrences DESC, equip LIMIT 10""")
    out["section2_iron_crunch"] = s2

    # ── SECTION 3: Bonding & Insurance crunch (entire SB cohort) ──
    s3: dict = {}
    s3["cohort_definition"] = "entire SB cohort (services + construction)"
    s3["sb_joined_to_scope_requirements"] = out["sb_in_scope_requirements"]
    s3["with_populated_insurance_bonding"] = con.execute(
        "SELECT count(*) FROM sb JOIN asr_local USING (k) WHERE has_bond").fetchone()[0]
    s3["bonding_group_total_current_value"] = con.execute(
        "SELECT coalesce(sum(sb.cur_val),0) FROM sb JOIN asr_local a USING (k) WHERE a.has_bond").fetchone()[0]
    con.execute("""
        CREATE TEMP TABLE s3_bond AS
        SELECT sb.k, lower(trim(unnest(a.insurance_bonding_values))) AS bond
        FROM sb JOIN asr_local a USING (k) WHERE a.has_bond
    """)
    s3["bonding_distinct_strings"] = con.execute("SELECT count(DISTINCT bond) FROM s3_bond").fetchone()[0]
    s3["bonding_top12"] = rows(con, """
        SELECT bond AS bonding_insurance, count(*) AS award_occurrences
        FROM s3_bond GROUP BY 1 ORDER BY award_occurrences DESC, bond LIMIT 12""")
    out["section3_bonding_crunch"] = s3

    # ── SECTION 4: Recency — "hot leads" in the last 90 days ──
    # Three crunch cohorts: A=High-Value Services (s1 whales >$5M), B=Equipment-Mandated
    # Construction (s2 ∩ has_equip), C=Bond-Mandated (sb ∩ has_bond). Dedup union for the
    # single "hand to a lender tomorrow" number.
    con.execute(f"""
        CREATE TEMP TABLE crunch AS
        SELECT k, cur_val, is_recent, 'A_high_value_services' AS cohort
            FROM s1 WHERE cur_val > 5e6
        UNION ALL
        SELECT s2.k, s2.cur_val, s2.is_recent, 'B_equipment_construction'
            FROM s2 JOIN asr_local a USING (k) WHERE a.has_equip
        UNION ALL
        SELECT sb.k, sb.cur_val, sb.is_recent, 'C_bond_mandated'
            FROM sb JOIN asr_local a USING (k) WHERE a.has_bond
    """)
    s4: dict = {}
    s4["per_cohort"] = rows(con, """
        SELECT cohort,
               count(*)                              AS awards,
               count(*) FILTER (WHERE is_recent)     AS recent_awards,
               coalesce(sum(cur_val),0)              AS total_current_value,
               coalesce(sum(cur_val) FILTER (WHERE is_recent),0) AS recent_current_value
        FROM crunch GROUP BY 1 ORDER BY 1""")
    # deduped union (an award may sit in >1 cohort)
    s4["union_all_crunch"] = one(con, """
        SELECT count(DISTINCT k) AS distinct_awards,
               count(DISTINCT k) FILTER (WHERE is_recent) AS distinct_recent_awards
        FROM crunch""")
    s4["union_recent_value"] = con.execute("""
        SELECT coalesce(sum(cur_val),0) FROM (
            SELECT DISTINCT ON (k) k, cur_val FROM crunch WHERE is_recent ORDER BY k)
    """).fetchone()[0]
    out["section4_recency_hot_leads"] = s4

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    print("", file=sys.stderr, flush=True)
    log("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
