#!/usr/bin/env python3
"""READ-ONLY probe — "Parachute Prime" / civilian Davis-Bacon construction density.

Single R2 connection. One filtered base materialization off govcon_active_awards
(active + future-end awards), then every aggregation runs off that temp table.
Cross-references govcon_equipment_rental_construction_match on contract_award_unique_key
(the MV carries no county; join key is the award key). NO writes to Lance — registration
+ SELECTs only.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/parachute_prime_probe.py > /tmp/parachute_probe.json

Self-validating: emits a diagnostics block (raw value histograms for
construction_wage_rate_requirements + agency strings) so the categorical literals
are confirmed, not assumed.
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
MV = f"{A}/govcon_equipment_rental_construction_match/"


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


def main() -> int:
    so = r2_so()
    as_of = dt.datetime.now(dt.timezone.utc).date().isoformat()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/duck_spill_probe", exist_ok=True)
    con.execute("SET memory_limit='8GB'")
    con.execute("SET temp_directory='/tmp/duck_spill_probe'")
    con.register("gaa", lance.dataset(GAA, storage_options=so))

    out: dict = {"as_of": as_of, "gaa_uri": GAA, "mv_uri": MV}

    # ── BASE: active + future-end awards, only the columns we need + derived flags ──
    # active_future = (active_current OR active_potential) == GREATEST(end) >= as_of
    #               == NOT pop_unknown (within this dataset's membership boundary).
    # davis_bacon   = robust over the raw FPDS field AND its code (confirmed by diagnostics below).
    # not_dod       = neither awarding NOR funding toptier agency is DoD (service branches roll
    #                 up to "Department of Defense" at toptier).
    log("materializing base (active + future-end) …")
    con.execute("""
        CREATE TEMP TABLE b AS
        SELECT
            contract_award_unique_key                                      AS k,
            recipient_uei, recipient_name,
            nullif(upper(trim(recipient_state_code)), '')                  AS hq_state,
            nullif(upper(trim(pop_state_code)), '')                        AS pop_state,
            nullif(upper(trim(pop_county)), '')                            AS pop_county,
            upper(trim(coalesce(pop_country_code, '')))                    AS pop_country,
            naics_code,
            awarding_agency_name, funding_agency_name,
            current_total_value_of_award                                   AS cur_val,
            potential_total_value_of_award                                 AS pot_val,  -- fully-populated ceiling; base_and_all_options_value is ~30% zero in this cohort
            has_subcontracting_plan,
            active_current, active_potential, pop_unknown,
            construction_wage_rate_requirements                            AS cwr,
            construction_wage_rate_requirements_code                       AS cwr_code,
            (upper(trim(coalesce(construction_wage_rate_requirements,'')))
                 IN ('YES','Y','TRUE','T','1')
             OR upper(trim(coalesce(construction_wage_rate_requirements_code,''))) = 'Y')  AS davis_bacon,
            (upper(trim(coalesce(awarding_agency_name,''))) <> 'DEPARTMENT OF DEFENSE'
             AND upper(trim(coalesce(funding_agency_name,'')))  <> 'DEPARTMENT OF DEFENSE') AS not_dod
        FROM gaa
        WHERE (active_current OR active_potential)
    """)
    base_n = con.execute("SELECT count(*) FROM b").fetchone()[0]
    out["base_active_future_rows"] = base_n
    log(f"base active+future rows = {base_n:,}")

    # ── DIAGNOSTICS: confirm categorical literals (self-validating) ──
    diag: dict = {}
    diag["cwr_raw_histogram"] = rows(con, """
        SELECT cwr AS value, count(*) n FROM b GROUP BY 1 ORDER BY n DESC""")
    diag["cwr_code_histogram"] = rows(con, """
        SELECT cwr_code AS value, count(*) n FROM b GROUP BY 1 ORDER BY n DESC""")
    diag["davis_bacon_active_future_awards"] = con.execute(
        "SELECT count(*) FROM b WHERE davis_bacon").fetchone()[0]
    # surface the cohort the "future end dates" filter omits (pop_unknown = both PoP ends NULL):
    # these are active-by-membership but have NO end date, so they are out of scope by definition.
    diag["davis_bacon_pop_unknown_excluded"] = con.execute("""
        SELECT count(*) FROM gaa WHERE pop_unknown
          AND (upper(trim(coalesce(construction_wage_rate_requirements,''))) IN ('YES','Y','TRUE','T','1')
               OR upper(trim(coalesce(construction_wage_rate_requirements_code,''))) = 'Y')""").fetchone()[0]
    diag["agency_names_containing_defense"] = rows(con, """
        SELECT awarding_agency_name AS value, count(*) n FROM b
        WHERE upper(awarding_agency_name) LIKE '%DEFENSE%' GROUP BY 1 ORDER BY n DESC""")
    # null-state exclusions in the Davis-Bacon active cohort (parachute denominator hygiene)
    diag["davis_bacon_state_nullity"] = con.execute("""
        SELECT
            count(*) FILTER (WHERE hq_state IS NULL)            AS hq_state_null,
            count(*) FILTER (WHERE pop_state IS NULL)           AS pop_state_null,
            count(*) FILTER (WHERE pop_country <> 'USA'
                              AND pop_country <> '')            AS pop_non_usa,
            count(*) FILTER (WHERE hq_state IS NOT NULL
                              AND pop_state IS NOT NULL)        AS both_states_present
        FROM b WHERE davis_bacon
    """).df().to_dict("records")[0]
    out["diagnostics"] = diag

    # ── STEP 1: civilian (non-DoD) Davis-Bacon active construction ──
    civ_where = "davis_bacon AND not_dod"
    s1: dict = {}
    s1["civilian_award_count"] = con.execute(
        f"SELECT count(*) FROM b WHERE {civ_where}").fetchone()[0]
    s1["civilian_total_current_value"] = con.execute(
        f"SELECT coalesce(sum(cur_val),0) FROM b WHERE {civ_where}").fetchone()[0]
    s1["civilian_total_potential_value"] = con.execute(
        f"SELECT coalesce(sum(pot_val),0) FROM b WHERE {civ_where}").fetchone()[0]
    s1["top5_agencies_by_award_count"] = rows(con, f"""
        SELECT awarding_agency_name AS agency,
               count(*) AS award_count,
               coalesce(sum(cur_val),0) AS total_current_value,
               coalesce(sum(pot_val),0) AS total_potential_value
        FROM b WHERE {civ_where}
        GROUP BY 1 ORDER BY award_count DESC LIMIT 5""")
    s1["top5_agencies_by_current_value"] = rows(con, f"""
        SELECT awarding_agency_name AS agency,
               count(*) AS award_count,
               coalesce(sum(cur_val),0) AS total_current_value
        FROM b WHERE {civ_where}
        GROUP BY 1 ORDER BY total_current_value DESC LIMIT 5""")
    # totals attributable to the top-5-by-count agencies (the directive asks for these specifically)
    s1["top5_by_count_combined"] = con.execute(f"""
        WITH t AS (
            SELECT awarding_agency_name AS agency, count(*) c
            FROM b WHERE {civ_where} GROUP BY 1 ORDER BY c DESC LIMIT 5)
        SELECT count(*) AS award_count, coalesce(sum(cur_val),0) AS total_current_value,
               coalesce(sum(pot_val),0) AS total_potential_value
        FROM b WHERE {civ_where}
          AND awarding_agency_name IN (SELECT agency FROM t)
    """).df().to_dict("records")[0]
    out["step1_civilian_heavyweights"] = s1

    # ── STEP 2: Parachute Prime cohort (DoD + civilian Davis-Bacon combined) ──
    db_where = "davis_bacon"
    valid_geo = "hq_state IS NOT NULL AND pop_state IS NOT NULL"
    parachute = f"{valid_geo} AND hq_state <> pop_state"
    s2: dict = {}
    s2["all_davis_bacon_active_awards"] = con.execute(
        f"SELECT count(*) FROM b WHERE {db_where}").fetchone()[0]
    s2["with_both_states_present"] = con.execute(
        f"SELECT count(*) FROM b WHERE {db_where} AND {valid_geo}").fetchone()[0]
    s2["parachute_out_of_state_awards"] = con.execute(
        f"SELECT count(*) FROM b WHERE {db_where} AND {parachute}").fetchone()[0]
    s2["parachute_with_subcontracting_plan"] = con.execute(
        f"SELECT count(*) FROM b WHERE {db_where} AND {parachute} AND has_subcontracting_plan").fetchone()[0]
    # split the golden cohort civilian vs DoD + value
    s2["parachute_breakdown"] = con.execute(f"""
        SELECT
            count(*)                                                   AS parachute_awards,
            count(*) FILTER (WHERE not_dod)                            AS parachute_civilian,
            count(*) FILTER (WHERE NOT not_dod)                        AS parachute_dod,
            count(*) FILTER (WHERE has_subcontracting_plan)            AS parachute_with_plan,
            count(*) FILTER (WHERE has_subcontracting_plan AND not_dod)     AS golden_civilian_with_plan,
            count(*) FILTER (WHERE has_subcontracting_plan AND NOT not_dod) AS golden_dod_with_plan,
            coalesce(sum(cur_val) FILTER (WHERE has_subcontracting_plan),0) AS golden_total_current_value
        FROM b WHERE {db_where} AND {parachute}
    """).df().to_dict("records")[0]
    out["step2_parachute_cohort"] = s2

    # ── STEP 3: civilian construction county hotspots ──
    s3: dict = {}
    s3["top10_counties"] = rows(con, f"""
        SELECT pop_state AS pop_state_code, pop_county,
               count(*) AS award_count,
               coalesce(sum(cur_val),0) AS total_current_value,
               count(*) FILTER (WHERE has_subcontracting_plan) AS with_plan_count
        FROM b WHERE {civ_where} AND pop_county IS NOT NULL AND pop_state IS NOT NULL
        GROUP BY 1,2 ORDER BY award_count DESC LIMIT 10""")
    s3["civilian_county_null_count"] = con.execute(
        f"SELECT count(*) FROM b WHERE {civ_where} AND (pop_county IS NULL OR pop_state IS NULL)").fetchone()[0]
    out["step3_county_hotspots"] = s3

    # ── STEP 3 BONUS: cross-reference top-10 civilian counties with the rental-match MV ──
    # MV has NO county; join on contract_award_unique_key. MV demand side is gated to
    # naics LIKE '23%' AND active_potential, so coverage = civilian-DB ∩ NAICS-23-MV.
    con.register("mv", lance.dataset(MV, storage_options=so))
    con.execute("""
        CREATE TEMP TABLE mv_local AS
        SELECT contract_award_unique_key AS k, sub_uei, tier
        FROM mv WHERE tier = 'local'
    """)
    con.execute("CREATE TEMP TABLE mv_any AS SELECT contract_award_unique_key AS k FROM mv")
    # civilian cohort restricted to the top-10 counties
    con.execute(f"""
        CREATE TEMP TABLE civ_top AS
        WITH top AS (
            SELECT pop_state, pop_county
            FROM b WHERE {civ_where} AND pop_county IS NOT NULL AND pop_state IS NOT NULL
            GROUP BY 1,2 ORDER BY count(*) DESC LIMIT 10)
        SELECT b.k, b.pop_state, b.pop_county, b.cur_val
        FROM b JOIN top USING (pop_state, pop_county)
        WHERE {civ_where}
    """)
    bonus: dict = {}
    # overall MV coverage of the FULL civilian Davis-Bacon cohort (sanity on the NAICS-23 gate)
    bonus["civilian_cohort_in_mv_any"] = con.execute(f"""
        SELECT count(*) FROM b WHERE {civ_where}
          AND k IN (SELECT k FROM mv_any)""").fetchone()[0]
    bonus["civilian_cohort_total"] = s1["civilian_award_count"]
    bonus["per_county_local_supply"] = rows(con, """
        SELECT c.pop_state AS pop_state_code, c.pop_county,
               count(DISTINCT c.k)                                         AS civilian_awards_in_county,
               count(DISTINCT ml.k)                                        AS awards_with_local_supply,
               count(DISTINCT ml.sub_uei)                                  AS distinct_local_rental_firms
        FROM civ_top c
        LEFT JOIN mv_local ml ON c.k = ml.k
        GROUP BY 1,2
        ORDER BY civilian_awards_in_county DESC""")
    out["step3_bonus_mv_local_supply"] = bonus

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    print("", file=sys.stderr, flush=True)
    log("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
