#!/usr/bin/env python3
"""READ-ONLY probe — macro PSC (Product/Service Code) distribution across active awards.

Single R2 connection. One filtered base temp table off govcon_active_awards
(active cohort = active_current OR active_potential, i.e. the directive's
"active_potential = true OR pop_current_end >= current_date"); every aggregation
runs off that temp table. NO writes to Lance — registration + SELECTs only.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/psc_distribution_probe.py > /tmp/psc_probe.json

Self-validating: emits a diagnostics block (raw value histograms for
labor_standards, subcontracting_plan_code, and the PSC first-char class) so the
categorical literals are confirmed, not assumed, and internal consistency
assertions (services + products + other == base) are checked in-script.

Column note: govcon_active_awards renames the raw FPDS fields —
  product_or_service_code              -> psc_code
  product_or_service_code_description  -> psc_description
and pre-derives psc_category (first char), psc_is_service (first char [A-Z]),
has_subcontracting_plan (subcontracting_plan_code in C/D/E/F/G/H). The probe
re-derives the first-char class from raw psc_code for transparency.
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

# Standard PSC top-level service-letter legend (FPDS / PSC Manual). Letters only —
# the macro-split "Services" universe. Embedded so the JSON output is self-describing.
PSC_SERVICE_LETTER_LABEL = {
    "A": "Research & Development",
    "B": "Special Studies & Analysis (non-R&D)",
    "C": "Architect & Engineering Services",
    "D": "IT & Telecommunications Services",
    "E": "Purchase of Structures & Facilities",
    "F": "Natural Resources & Conservation Services",
    "G": "Social Services",
    "H": "Quality Control, Testing & Inspection",
    "J": "Maintenance, Repair & Rebuilding of Equipment",
    "K": "Modification of Equipment",
    "L": "Technical Representative Services",
    "M": "Operation of Government-Owned Facilities",
    "N": "Installation of Equipment",
    "P": "Salvage Services",
    "Q": "Medical Services",
    "R": "Professional, Administrative & Management Support",
    "S": "Utilities & Housekeeping Services",
    "T": "Photographic, Mapping, Printing & Publication",
    "U": "Education & Training Services",
    "V": "Transportation, Travel & Relocation Services",
    "W": "Lease/Rental of Equipment",
    "X": "Lease/Rental of Facilities",
    "Y": "Construction of Structures & Facilities",
    "Z": "Maintenance, Repair & Alteration of Real Property",
}

# subcontracting_plan_code legend (FPDS) — the mandatory-plan cohort = C/D/E/F/G/H.
SUBK_PLAN_CODE_LABEL = {
    "A": "No plan — no subcontracting possibilities",
    "B": "Plan not required (small-business prime / under threshold)",
    "C": "Plan required — incentive NOT included",
    "D": "Plan required — incentive included",
    "E": "Plan required (pre-2004)",
    "F": "Individual subcontract plan",
    "G": "Commercial subcontract plan",
    "H": "DoD comprehensive subcontract plan",
}
MANDATORY_PLAN_CODES = ("C", "D", "E", "F", "G", "H")


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
    os.makedirs("/tmp/duck_spill_psc", exist_ok=True)
    con.execute("SET memory_limit='8GB'")
    con.execute("SET temp_directory='/tmp/duck_spill_psc'")
    con.register("gaa", lance.dataset(GAA, storage_options=so))

    out: dict = {"as_of": as_of, "gaa_uri": GAA}

    # ── BASE: active cohort (active_current OR active_potential), only needed cols + derived class ──
    # psc_first = uppercased first char of trimmed psc_code.
    # psc_class = 'service' (A-Z) | 'product' (0-9) | 'other' (null/empty/non-alnum).
    log("materializing base (active cohort) …")
    con.execute("""
        CREATE TEMP TABLE b AS
        SELECT
            contract_award_unique_key                              AS k,
            nullif(upper(trim(psc_code)), '')                      AS psc,
            psc_description                                        AS psc_desc,
            substr(nullif(upper(trim(psc_code)), ''), 1, 1)        AS psc_first,
            CASE
                WHEN substr(nullif(upper(trim(psc_code)), ''), 1, 1) ~ '[A-Z]' THEN 'service'
                WHEN substr(nullif(upper(trim(psc_code)), ''), 1, 1) ~ '[0-9]' THEN 'product'
                ELSE 'other'
            END                                                    AS psc_class,
            psc_is_service,
            coalesce(TRY_CAST(current_total_value_of_award AS DOUBLE), 0)   AS cur_val,
            coalesce(TRY_CAST(potential_total_value_of_award AS DOUBLE), 0) AS pot_val,
            has_subcontracting_plan,
            nullif(upper(trim(subcontracting_plan_code)), '')      AS subk_code,
            upper(trim(coalesce(labor_standards, '')))             AS labor_std,
            active_current, active_potential, pop_unknown
        FROM gaa
        WHERE (active_current OR active_potential)
    """)
    base_n = con.execute("SELECT count(*) FROM b").fetchone()[0]
    out["base_active_rows"] = base_n
    out["base_total_current_value"] = con.execute("SELECT sum(cur_val) FROM b").fetchone()[0]
    out["base_total_potential_value"] = con.execute("SELECT sum(pot_val) FROM b").fetchone()[0]
    log(f"base active rows = {base_n:,}")

    # ── DIAGNOSTICS: self-validating literal confirmation ──
    diag: dict = {}
    diag["membership_note"] = (
        "active cohort = (active_current OR active_potential). pop_unknown awards "
        "(both PoP ends NULL) are EXCLUDED from this probe per the directive's "
        "active_potential/pop_current_end filter.")
    diag["pop_unknown_excluded"] = con.execute(
        "SELECT count(*) FROM gaa WHERE pop_unknown AND NOT (active_current OR active_potential)"
    ).fetchone()[0]
    diag["active_current_rows"] = con.execute("SELECT count(*) FROM b WHERE active_current").fetchone()[0]
    diag["active_potential_rows"] = con.execute("SELECT count(*) FROM b WHERE active_potential").fetchone()[0]
    diag["psc_class_histogram"] = rows(con, """
        SELECT psc_class, count(*) AS n,
               sum(cur_val) AS total_current_value
        FROM b GROUP BY 1 ORDER BY n DESC""")
    diag["psc_null_or_empty"] = con.execute("SELECT count(*) FROM b WHERE psc IS NULL").fetchone()[0]
    diag["labor_standards_raw_histogram"] = rows(con, """
        SELECT labor_std AS value, count(*) AS n FROM b GROUP BY 1 ORDER BY n DESC""")
    diag["subcontracting_plan_code_raw_histogram"] = rows(con, """
        SELECT coalesce(subk_code, '(null)') AS code, count(*) AS n,
               bool_or(has_subcontracting_plan) AS flagged_mandatory
        FROM b GROUP BY 1 ORDER BY n DESC""")
    # cross-check: has_subcontracting_plan flag exactly equals code in (C,D,E,F,G,H)
    diag["subk_flag_vs_code_mismatch"] = con.execute(f"""
        SELECT count(*) FROM b
        WHERE has_subcontracting_plan <> (subk_code IN {MANDATORY_PLAN_CODES})
    """).fetchone()[0]
    out["diagnostics"] = diag

    # ── INTERNAL CONSISTENCY ASSERTIONS ──
    svc_n = con.execute("SELECT count(*) FROM b WHERE psc_class='service'").fetchone()[0]
    prod_n = con.execute("SELECT count(*) FROM b WHERE psc_class='product'").fetchone()[0]
    oth_n = con.execute("SELECT count(*) FROM b WHERE psc_class='other'").fetchone()[0]
    assert svc_n + prod_n + oth_n == base_n, "class partition != base"
    # the table's psc_is_service must equal our service classification (sanity on derived col)
    is_service_mismatch = con.execute(
        "SELECT count(*) FROM b WHERE coalesce(psc_is_service,false) <> (psc_class='service')"
    ).fetchone()[0]
    out["consistency"] = {
        "service_plus_product_plus_other_eq_base": True,
        "psc_is_service_vs_first_char_mismatch": is_service_mismatch,
    }

    # ── SECTION 1: Macro split (Services vs Products) + Top 5 service categories ──
    s1: dict = {}
    s1["macro_split"] = rows(con, """
        SELECT
            CASE psc_class WHEN 'service' THEN 'Services (letter A-Z)'
                           WHEN 'product' THEN 'Products (digit 0-9)'
                           ELSE 'Other / Unclassified' END AS bucket,
            psc_class,
            count(*)              AS award_count,
            sum(cur_val)          AS total_current_value,
            sum(pot_val)          AS total_potential_value
        FROM b GROUP BY 1,2 ORDER BY award_count DESC""")
    s1["top5_service_categories_by_count"] = rows(con, """
        SELECT psc_first AS category_letter,
               count(*)     AS award_count,
               sum(cur_val) AS total_current_value
        FROM b WHERE psc_class='service'
        GROUP BY 1 ORDER BY award_count DESC LIMIT 5""")
    # full service-letter breakdown (all 23 letters present) for completeness
    s1["all_service_categories_by_count"] = rows(con, """
        SELECT psc_first AS category_letter,
               count(*)     AS award_count,
               sum(cur_val) AS total_current_value
        FROM b WHERE psc_class='service'
        GROUP BY 1 ORDER BY award_count DESC""")
    out["section1_macro_split"] = s1

    # ── SECTION 2: Top 25 exact 4-char PSC codes overall ──
    # Group by exact code; label = modal description (avoids fragmenting one code
    # across FPDS text variants). distinct_descriptions surfaces any variance.
    out["section2_top25_psc"] = rows(con, """
        SELECT psc AS psc_code,
               mode(psc_desc)            AS psc_description,
               count(DISTINCT psc_desc)  AS distinct_descriptions,
               count(*)                  AS award_count,
               sum(cur_val)              AS total_current_value,
               sum(pot_val)              AS total_potential_value
        FROM b WHERE psc IS NOT NULL
        GROUP BY psc ORDER BY award_count DESC LIMIT 25""")

    # ── SECTION 3: subcontracting-plan hotspots (has_subcontracting_plan = true) ──
    s3: dict = {}
    s3["mandatory_plan_award_count"] = con.execute(
        "SELECT count(*) FROM b WHERE has_subcontracting_plan").fetchone()[0]
    s3["mandatory_plan_total_current_value"] = con.execute(
        "SELECT sum(cur_val) FROM b WHERE has_subcontracting_plan").fetchone()[0]
    s3["top15_psc_by_mandatory_plan_count"] = rows(con, """
        SELECT psc AS psc_code,
               mode(psc_desc) AS psc_description,
               count(*)       AS mandatory_plan_award_count,
               sum(cur_val)   AS total_current_value
        FROM b WHERE has_subcontracting_plan AND psc IS NOT NULL
        GROUP BY psc ORDER BY mandatory_plan_award_count DESC LIMIT 15""")
    out["section3_subcontracting_hotspots"] = s3

    # ── SECTION 4: SCA / labor_standards = 'YES' hotspots ──
    s4: dict = {}
    s4["sca_award_count"] = con.execute(
        "SELECT count(*) FROM b WHERE labor_std = 'YES'").fetchone()[0]
    s4["sca_total_current_value"] = con.execute(
        "SELECT sum(cur_val) FROM b WHERE labor_std = 'YES'").fetchone()[0]
    s4["top10_psc_by_sca_count"] = rows(con, """
        SELECT psc AS psc_code,
               mode(psc_desc) AS psc_description,
               count(*)       AS sca_award_count,
               sum(cur_val)   AS total_current_value
        FROM b WHERE labor_std = 'YES' AND psc IS NOT NULL
        GROUP BY psc ORDER BY sca_award_count DESC LIMIT 10""")
    out["section4_sca_hotspots"] = s4

    # embed legends so the report is self-contained
    out["legends"] = {
        "psc_service_letter": PSC_SERVICE_LETTER_LABEL,
        "subcontracting_plan_code": SUBK_PLAN_CODE_LABEL,
        "mandatory_plan_codes": list(MANDATORY_PLAN_CODES),
    }

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    print("", file=sys.stderr, flush=True)
    log("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
