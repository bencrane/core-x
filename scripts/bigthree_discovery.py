#!/usr/bin/env python3
"""READ-ONLY discovery — inspect the Big Three (R499/R425/DA01) cohort + the
govcon_award_scope_requirements extraction surface BEFORE committing to keyword search.
No assumptions about array formats/case — inspect actual values. NO writes.
"""
from __future__ import annotations
import json, os, sys
import duckdb, lance, pyarrow as pa

A = "s3://data-sink/active"
GAA = f"{A}/govcon_active_awards/"
ASR = f"{A}/govcon_award_scope_requirements/"

def r2_so():
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com" if os.environ.get("R2_ACCOUNT_ID") else None)
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": ep, "region": "auto"}

def rows(con, q):
    cur = con.execute(q); cols=[d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def main():
    so = r2_so()
    con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=8; SET memory_limit='8GB'")
    gaa = lance.dataset(GAA, storage_options=so)
    asr = lance.dataset(ASR, storage_options=so)
    out = {}

    # schemas
    out["asr_schema"] = [{"name": f.name, "type": str(f.type)} for f in asr.schema]
    out["asr_rows"] = asr.count_rows()

    con.register("gaa", gaa); con.register("asr", asr)

    # Big Three base cohort (active+future)
    con.execute("""
        CREATE TEMP TABLE b AS
        SELECT contract_award_unique_key AS k, psc_code,
               nullif(trim(psc_description),'') AS psc_desc,
               business_size, type_of_set_aside, type_of_set_aside_code,
               has_subcontracting_plan, subcontracting_plan_code,
               current_total_value_of_award AS cur_val, recipient_uei, recipient_name
        FROM gaa
        WHERE (active_current OR active_potential) AND psc_code IN ('R499','R425','DA01')
    """)
    out["bigthree_by_code"] = rows(con, """
        SELECT psc_code, any_value(psc_desc) psc_description, count(*) n,
               count(*) FILTER (WHERE has_subcontracting_plan) n_plan,
               coalesce(sum(cur_val),0) cur_val
        FROM b GROUP BY psc_code ORDER BY n DESC""")
    out["bigthree_total"] = con.execute(
        "SELECT count(*) n, count(*) FILTER (WHERE has_subcontracting_plan) n_plan, coalesce(sum(cur_val),0) v FROM b").df().to_dict("records")[0]
    out["business_size_dist"] = rows(con, "SELECT coalesce(business_size,'(null)') business_size, count(*) n FROM b GROUP BY 1 ORDER BY n DESC")
    out["set_aside_dist"] = rows(con, "SELECT coalesce(type_of_set_aside,'(null)') set_aside, type_of_set_aside_code code, count(*) n FROM b GROUP BY 1,2 ORDER BY n DESC LIMIT 25")

    # join coverage to scope-requirements
    con.execute("""
        CREATE TEMP TABLE j AS
        SELECT b.*, asr.has_extracted_scope, asr.scope_summary, asr.solicitation_scope_tags,
               asr.vehicle_constraint_values, asr.certification_values, asr.staffing_constraint_values,
               asr.labor_category_values, asr.standard_compliance_values,
               (asr.contract_award_unique_key IS NOT NULL) AS in_asr
        FROM b LEFT JOIN asr ON b.k = asr.contract_award_unique_key
    """)
    out["asr_join_coverage"] = con.execute("""
        SELECT count(*) bigthree, count(*) FILTER (WHERE in_asr) joined_to_asr,
               count(*) FILTER (WHERE has_extracted_scope) has_extracted_scope
        FROM j""").df().to_dict("records")[0]

    # array content samples (top values) — what actually lives in each array for the cohort
    def arr_top(col, n=30):
        return rows(con, f"""
            SELECT lower(trim(v)) AS value, count(*) n FROM j, UNNEST({col}) AS t(v)
            WHERE {col} IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT {n}""")
    out["sample_solicitation_scope_tags"] = arr_top("solicitation_scope_tags")
    out["sample_vehicle_constraint_values"] = arr_top("vehicle_constraint_values")
    out["sample_certification_values"] = arr_top("certification_values")
    out["sample_staffing_constraint_values"] = arr_top("staffing_constraint_values")

    # does ANY socio keyword appear ANYWHERE in scope_summary text? quick prevalence
    kw = "(lower(coalesce(scope_summary,'')) LIKE '%hubzone%' OR lower(coalesce(scope_summary,'')) LIKE '%8(a)%' OR lower(coalesce(scope_summary,'')) LIKE '%8a %' OR lower(coalesce(scope_summary,'')) LIKE '%wosb%' OR lower(coalesce(scope_summary,'')) LIKE '%woman-owned%' OR lower(coalesce(scope_summary,'')) LIKE '%women-owned%' OR lower(coalesce(scope_summary,'')) LIKE '%sdvosb%' OR lower(coalesce(scope_summary,'')) LIKE '%service-disabled%' OR lower(coalesce(scope_summary,'')) LIKE '%veteran%')"
    out["scope_summary_any_socio_kw"] = con.execute(
        f"SELECT count(*) FILTER (WHERE has_extracted_scope) scoped, count(*) FILTER (WHERE {kw}) socio_kw FROM j").df().to_dict("records")[0]

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    return 0

if __name__ == "__main__":
    sys.exit(main())
