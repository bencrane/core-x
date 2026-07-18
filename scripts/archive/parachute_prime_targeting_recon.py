#!/usr/bin/env python3
"""READ-ONLY recon — "Parachute Prime" equipment-rental targeting readiness.

Validates the out-of-state-prime → local-rental thesis for two GTM profiles before it
is wired into the Catalyst-API / LLM translation layer:

  Target A (Heavy Earthmoving): awarding agency ∈ {DHS, DOI, DOT} + construction.
  Target B (Aerial & Power):    awarding agency = GSA + PSC Z2AA (office repair/alter).

Answers four questions, all off ONE filtered base materialization of
s3://data-sink/active/govcon_active_awards/ (active + future-end), cross-referenced to
s3://data-sink/active/sba_dsbs_certified_firms/ for the certified small-business rental
supply. NO writes to Lance — registration + SELECTs only. Self-validating: emits the
literal histograms (agency strings, PSC format, NAICS-all-codes coverage, state-domain
mismatch) that the predicates depend on.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/parachute_prime_targeting_recon.py > /tmp/pp_recon.json
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
DSBS = f"{A}/sba_dsbs_certified_firms/"

# Agency literal strings — CONFIRMED live (exact equality required: '%TRANSPORTATION%' leaks
# "National Transportation Safety Board").
AG_DHS = "Department of Homeland Security"
AG_DOI = "Department of the Interior"
AG_DOT = "Department of Transportation"
AG_GSA = "General Services Administration"
AG_VA = "Department of Veterans Affairs"
TARGET_A_AGENCIES = (AG_DHS, AG_DOI, AG_DOT)

# Equipment-rental NAICS (the supply-side capability codes).
RENTAL_NAICS = ("532412", "532310")  # Construction-machinery rental · General rental centers

# 2-char USPS code -> DSBS full state name (DSBS `state` is the full name; GAA is the code).
CODE_TO_NAME = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "DC": "District of Columbia",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois",
    "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana",
    "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon",
    "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
    "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia",
    "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "PR": "Puerto Rico", "GU": "Guam", "VI": "Virgin Islands", "AS": "American Samoa",
    "MP": "Northern Mariana Islands",
}


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
    return con.execute(q).fetchone()[0]


def cv(con, where: str) -> dict:
    """count + current value + potential value for a predicate over base table b."""
    r = con.execute(f"""
        SELECT count(*) AS award_count,
               coalesce(sum(cur_val),0) AS current_value,
               coalesce(sum(pot_val),0) AS potential_value
        FROM b WHERE {where}""").df().to_dict("records")[0]
    return {k: (int(v) if k == "award_count" else float(v)) for k, v in r.items()}


def main() -> int:
    so = r2_so()
    as_of = dt.datetime.now(dt.timezone.utc).date().isoformat()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/duck_spill_pp", exist_ok=True)
    con.execute("SET memory_limit='8GB'")
    con.execute("SET temp_directory='/tmp/duck_spill_pp'")
    con.register("gaa", lance.dataset(GAA, storage_options=so))
    con.register("dsbs", lance.dataset(DSBS, storage_options=so))

    out: dict = {"as_of": as_of, "gaa_uri": GAA, "dsbs_uri": DSBS}

    # ── BASE: active + future-end awards, projected columns + derived predicate flags ──
    log("materializing base (active + future-end) …")
    con.execute(f"""
        CREATE TEMP TABLE b AS
        SELECT
            contract_award_unique_key                       AS k,
            recipient_uei, recipient_name, recipient_parent_name,
            nullif(upper(trim(recipient_state_code)), '')   AS hq_state,
            nullif(upper(trim(pop_state_code)), '')         AS pop_state,
            nullif(upper(trim(pop_county)), '')             AS pop_county,
            naics_code,
            nullif(upper(trim(psc_code)), '')               AS psc_code,
            nullif(upper(trim(psc_category)), '')           AS psc_category,
            nullif(trim(psc_description), '')               AS psc_desc,
            awarding_agency_name                            AS agency,
            business_size,
            current_total_value_of_award                    AS cur_val,
            potential_total_value_of_award                  AS pot_val,
            has_subcontracting_plan,
            (upper(trim(coalesce(construction_wage_rate_requirements,''))) IN ('YES','Y','TRUE','T','1')
             OR upper(trim(coalesce(construction_wage_rate_requirements_code,''))) = 'Y') AS davis_bacon,
            (coalesce(naics_code,'') LIKE '23%')                                          AS naics23,
            (upper(trim(coalesce(psc_category,''))) IN ('Y','Z'))                         AS psc_yz,
            -- "alarm guys" + equipment-install exclusion (NULL-safe: null PSC is kept)
            (coalesce(upper(trim(psc_code)),'') <> '6350'
             AND coalesce(upper(trim(psc_code)),'') NOT LIKE 'N0%')                       AS not_alarm,
            (coalesce(awarding_agency_name,'') <> '{AG_VA}')                              AS not_va
        FROM gaa
        WHERE (active_current OR active_potential)
    """)
    base_n = one(con, "SELECT count(*) FROM b")
    distinct_k = one(con, "SELECT count(DISTINCT k) FROM b")
    out["base"] = {"active_future_rows": base_n, "distinct_award_keys": distinct_k,
                   "grain_ok": base_n == distinct_k}
    log(f"base rows = {base_n:,}  (distinct keys {distinct_k:,})")

    # ── §1 DATA READINESS / SELF-VALIDATING DIAGNOSTICS ──
    diag: dict = {}
    diag["out_of_state_predicate"] = {
        "hq_col": "recipient_state_code", "pop_col": "pop_state_code",
        "both_2char_codes": True,
        "rows_hq_null": one(con, "SELECT count(*) FROM b WHERE hq_state IS NULL"),
        "rows_pop_null": one(con, "SELECT count(*) FROM b WHERE pop_state IS NULL"),
        "rows_both_present": one(con, "SELECT count(*) FROM b WHERE hq_state IS NOT NULL AND pop_state IS NOT NULL"),
        "rows_out_of_state": one(con, "SELECT count(*) FROM b WHERE hq_state IS NOT NULL AND pop_state IS NOT NULL AND hq_state<>pop_state"),
    }
    diag["agency_strings_live"] = rows(con, f"""
        SELECT agency, count(*) n FROM b
        WHERE agency IN ('{AG_DHS}','{AG_DOI}','{AG_DOT}','{AG_GSA}','{AG_VA}')
        GROUP BY 1 ORDER BY n DESC""")
    diag["exclusion_filters"] = {
        "psc_6350_active": one(con, "SELECT count(*) FROM b WHERE psc_code='6350'"),
        "psc_N0_prefix_active": one(con, "SELECT count(*) FROM b WHERE psc_code LIKE 'N0%'"),
        "psc_Z2AA_active": one(con, "SELECT count(*) FROM b WHERE psc_code='Z2AA'"),
        "va_active": one(con, f"SELECT count(*) FROM b WHERE agency='{AG_VA}'"),
    }
    diag["construction_signals_active"] = {
        "davis_bacon": one(con, "SELECT count(*) FROM b WHERE davis_bacon"),
        "naics_23": one(con, "SELECT count(*) FROM b WHERE naics23"),
        "psc_Y_or_Z": one(con, "SELECT count(*) FROM b WHERE psc_yz"),
        "union_any": one(con, "SELECT count(*) FROM b WHERE davis_bacon OR naics23 OR psc_yz"),
    }
    out["s1_diagnostics"] = diag

    # ── Predicate fragments ──
    OOS = "hq_state IS NOT NULL AND pop_state IS NOT NULL AND hq_state<>pop_state"
    A_AGENCY = "agency IN ('{}','{}','{}')".format(*TARGET_A_AGENCIES)
    CONSTR = "(davis_bacon OR naics23 OR psc_yz)"   # primary "physical construction" gate
    # Target A broad = out-of-state + DHS/DOI/DOT + construction + (not-alarm, not-VA[moot])
    A_BROAD = f"{OOS} AND {A_AGENCY} AND {CONSTR} AND not_alarm AND not_va"
    A_STRICT = f"{A_BROAD} AND has_subcontracting_plan"
    # Target B broad = out-of-state + GSA + PSC Z2AA  (alarm/VA exclusions moot under Z2AA/GSA)
    B_BROAD = f"{OOS} AND agency='{AG_GSA}' AND psc_code='Z2AA' AND not_alarm AND not_va"
    B_STRICT = f"{B_BROAD} AND has_subcontracting_plan"

    # ── §2 VOLUME DROP-OFF (Broad vs Strict) ──
    s2: dict = {}
    s2["target_a"] = {
        "definition": "out-of-state prime · awarding agency ∈ {DHS,DOI,DOT} · construction "
                      "(NAICS 23% OR Davis-Bacon OR PSC Y/Z) · excl 6350/N0* · excl VA",
        "broad": cv(con, A_BROAD),
        "strict": cv(con, A_STRICT),
    }
    s2["target_b"] = {
        "definition": "out-of-state prime · awarding agency = GSA · PSC = Z2AA (office repair/alter)",
        "broad": cv(con, B_BROAD),
        "strict": cv(con, B_STRICT),
    }
    # Target A construction-definition sensitivity (each signal standalone, broad + strict)
    sens = {}
    for label, gate in (("davis_bacon_only", "davis_bacon"),
                        ("naics_23_only", "naics23"),
                        ("psc_yz_only", "psc_yz"),
                        ("union_any", CONSTR)):
        w = f"{OOS} AND {A_AGENCY} AND {gate} AND not_alarm AND not_va"
        sens[label] = {"broad": cv(con, w), "strict": cv(con, f"{w} AND has_subcontracting_plan")}
    s2["target_a_construction_sensitivity"] = sens
    # drop-off ratios
    def drop(d):
        b, s = d["broad"]["award_count"], d["strict"]["award_count"]
        return {"broad_n": b, "strict_n": s, "retained_pct": round(100.0*s/b, 1) if b else None,
                "lost_n": b - s, "lost_pct": round(100.0*(b-s)/b, 1) if b else None}
    s2["dropoff"] = {"target_a": drop(s2["target_a"]), "target_b": drop(s2["target_b"])}
    out["s2_volume_dropoff"] = s2

    # ── §3 GEOGRAPHIC & ENTITY DISTRIBUTION (Broad combined: A ∪ B) ──
    con.execute(f"""
        CREATE TEMP TABLE comb AS
        SELECT *,
               CASE WHEN {A_BROAD} THEN 'A_earthmoving'
                    WHEN {B_BROAD} THEN 'B_aerial_power' END AS target
        FROM b
        WHERE ({A_BROAD}) OR ({B_BROAD})
    """)
    comb_n = one(con, "SELECT count(*) FROM comb")
    s3: dict = {"combined_broad_n": comb_n,
                "split": rows(con, "SELECT target, count(*) n, coalesce(sum(cur_val),0) v FROM comb GROUP BY 1 ORDER BY n DESC")}
    s3["top5_pop_states"] = rows(con, """
        SELECT pop_state AS pop_state_code,
               count(*) AS award_count,
               coalesce(sum(cur_val),0) AS current_value,
               count(*) FILTER (WHERE target='A_earthmoving') AS a_count,
               count(*) FILTER (WHERE target='B_aerial_power') AS b_count
        FROM comb GROUP BY 1 ORDER BY award_count DESC LIMIT 8""")
    s3["top5_primes"] = rows(con, """
        WITH pf AS (  -- per-prime full active portfolio (size proxy), across ALL active awards
            SELECT recipient_uei, coalesce(sum(cur_val),0) AS portfolio_value,
                   count(*) AS portfolio_awards
            FROM b GROUP BY 1)
        SELECT c.recipient_name AS prime,
               any_value(c.recipient_parent_name) AS parent,
               count(*) AS cohort_awards,
               coalesce(sum(c.cur_val),0) AS cohort_value,
               any_value(c.business_size) AS business_size,
               any_value(pf.portfolio_awards) AS total_active_awards,
               any_value(pf.portfolio_value) AS total_active_portfolio_value,
               count(DISTINCT c.pop_state) AS distinct_pop_states,
               count(DISTINCT c.agency) AS distinct_agencies
        FROM comb c JOIN pf USING (recipient_uei)
        GROUP BY c.recipient_name
        ORDER BY cohort_awards DESC, cohort_value DESC LIMIT 10""")
    # anomaly: county concentration
    s3["top10_pop_counties"] = rows(con, """
        SELECT pop_state AS pop_state_code, pop_county,
               count(*) AS award_count,
               coalesce(sum(cur_val),0) AS current_value,
               round(100.0*count(*)/sum(count(*)) OVER (), 1) AS pct_of_combined
        FROM comb WHERE pop_county IS NOT NULL AND pop_state IS NOT NULL
        GROUP BY 1,2 ORDER BY award_count DESC LIMIT 10""")
    s3["county_null_n"] = one(con, "SELECT count(*) FROM comb WHERE pop_county IS NULL OR pop_state IS NULL")
    out["s3_geo_entity"] = s3

    # Top-5 PoP states (codes) -> names for the DSBS match
    top5_codes = [r["pop_state_code"] for r in s3["top5_pop_states"][:5]]
    out["top5_pop_state_codes"] = top5_codes

    # ── §4 DSBS SUPPLY-SIDE MATCH (certified small-business rental yards) ──
    # DSBS.state is the FULL name; match on {full-name, code} upper-cased (belt+suspenders).
    accept = []
    for c in top5_codes:
        accept.append(c.upper())
        if c in CODE_TO_NAME:
            accept.append(CODE_TO_NAME[c].upper())
    accept_sql = ",".join(f"'{x}'" for x in sorted(set(accept)))
    rental_like = " OR ".join(f"naics_all_codes LIKE '%{n}%'" for n in RENTAL_NAICS)
    rental_primary = " OR ".join(f"naics_primary='{n}'" for n in RENTAL_NAICS)

    s4: dict = {"rental_naics": list(RENTAL_NAICS),
                "naics_field_note": "naics_all_codes (pipe-delimited) — captures rental as a secondary code; naics_primary alone undercounts ~12×"}
    s4["nationwide"] = {
        "rental_by_primary_naics": one(con, f"SELECT count(*) FROM dsbs WHERE {rental_primary}"),
        "rental_by_all_codes": one(con, f"SELECT count(*) FROM dsbs WHERE {rental_like}"),
        "rental_532412_all": one(con, "SELECT count(*) FROM dsbs WHERE naics_all_codes LIKE '%532412%'"),
        "rental_532310_all": one(con, "SELECT count(*) FROM dsbs WHERE naics_all_codes LIKE '%532310%'"),
    }
    s4["top5_states_accept_strings"] = sorted(set(accept))
    s4["in_top5_pop_states"] = {
        "rental_firms_total": one(con, f"SELECT count(*) FROM dsbs WHERE ({rental_like}) AND upper(trim(state)) IN ({accept_sql})"),
        "rental_firms_primary_naics": one(con, f"SELECT count(*) FROM dsbs WHERE ({rental_primary}) AND upper(trim(state)) IN ({accept_sql})"),
    }
    s4["by_state"] = rows(con, f"""
        SELECT state,
               count(*) AS certified_rental_firms,
               count(*) FILTER (WHERE {rental_primary}) AS primary_naics_firms,
               count(*) FILTER (WHERE naics_all_codes LIKE '%532412%') AS n_532412,
               count(*) FILTER (WHERE naics_all_codes LIKE '%532310%') AS n_532310
        FROM dsbs WHERE ({rental_like}) AND upper(trim(state)) IN ({accept_sql})
        GROUP BY 1 ORDER BY certified_rental_firms DESC""")
    # certification-program overlay (the double-value angle) within the footprint
    s4["by_cert_program_in_footprint"] = rows(con, f"""
        SELECT
            count(*) FILTER (WHERE active_8a_boolean OR active_8a_jv_boolean) AS p_8a,
            count(*) FILTER (WHERE active_hz_boolean)                         AS p_hubzone,
            count(*) FILTER (WHERE active_wosb_boolean)                       AS p_wosb,
            count(*) FILTER (WHERE active_edwosb_boolean)                     AS p_edwosb,
            count(*) FILTER (WHERE active_vosb_boolean OR active_vosb_jv_boolean)   AS p_vosb,
            count(*) FILTER (WHERE active_sdvosb_boolean OR active_sdvosb_jv_boolean) AS p_sdvosb,
            count(*) FILTER (WHERE cert_programs IS NOT NULL)                 AS any_active_cert
        FROM dsbs WHERE ({rental_like}) AND upper(trim(state)) IN ({accept_sql})""")
    out["s4_dsbs_supply"] = s4

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    log("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
