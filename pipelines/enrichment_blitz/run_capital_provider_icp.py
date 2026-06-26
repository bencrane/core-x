"""Fire the ICP find-people cycle over the capital-provider lender universe.

Builds the company list (active/capital_provider_signals ∩ active/companies for the LinkedIn URL),
defines the ICP person filter (decision-maker job titles — the find-people `people.job_title`
block, which is the LID: a big lender returns only its titled leaders, not its whole org), and
fans blitz-find-people's `run_find_people` out over ~220-company batches via Modal. Every Blitz
call is gateway-routed (≤5 RPS, blitz-gateway), free on the Unlimited plan; only Modal compute costs.

    doppler run -p core-x -c prd -- uv run --no-project \
        --with modal --with pylance --with duckdb \
        python3 pipelines/enrichment_blitz/run_capital_provider_icp.py
"""
from __future__ import annotations

import os

import duckdb
import lance
import modal

BATCH = 220
RUN_ROOT = "capital-providers-icp-2026-06-26"
PRIORITY = "low"

# ICP decision-maker titles — FTS keyword families (no brackets → variant-tolerant). The job_title
# filter alone bounds the pull (a 50k-employee bank returns only these roles, not everyone).
ICP_TITLES = [
    "CEO", "Chief Executive Officer", "President", "COO", "Chief Operating Officer",
    "CFO", "Chief Financial Officer", "Chief Revenue Officer", "Chief Credit Officer",
    "Founder", "Owner", "Managing Partner", "Managing Director", "Partner", "Principal",
    "Business Development", "Origination", "Lending", "Relationship Manager",
    "Head of Sales", "VP Sales", "VP of Sales", "Director of Sales", "Sales Director",
    "National Sales Manager", "Asset Based Lending", "Factoring", "Capital Markets",
]

ICP_PEOPLE = {
    "job_title": {"include_linkedin_headline": False, "include": ICP_TITLES, "exclude": []},
    "job_function": [], "job_level": [], "min_connections": 0,
    "location": {"city": [], "country_code": [], "continent": [], "sales_region": []},
    "education": {"include": [], "exclude": []},
}


def _lender_companies() -> list[dict]:
    so = {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
          "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
          "endpoint": os.environ["R2_ENDPOINT"], "region": "auto"}
    con = duckdb.connect(); con.execute("SET memory_limit='6GB'")
    con.register("cps_r", lance.dataset("s3://data-sink/active/capital_provider_signals/", storage_options=so)
                 .scanner(columns=["normalized_domain", "is_capital_provider"]).to_reader())
    con.register("co_r", lance.dataset("s3://data-sink/active/companies/", storage_options=so)
                 .scanner(columns=["normalized_domain", "company_linkedin_url", "firmo_linkedin_url"]).to_reader())
    con.execute("CREATE TABLE cps AS SELECT normalized_domain dom FROM cps_r WHERE is_capital_provider")
    con.execute("CREATE TABLE co AS SELECT normalized_domain dom, coalesce(company_linkedin_url, firmo_linkedin_url) li FROM co_r")
    rows = con.execute("SELECT cps.dom, any_value(co.li) li FROM cps LEFT JOIN co ON co.dom=cps.dom GROUP BY 1").fetchall()
    con.close()
    return [{"domain": d, "company_linkedin_url": li} for d, li in rows]


def main() -> None:
    comps = _lender_companies()
    batches = [comps[i:i + BATCH] for i in range(0, len(comps), BATCH)]
    with_li = sum(1 for c in comps if c["company_linkedin_url"])
    print(f"lenders: {len(comps):,}  ({with_li:,} with linkedin) → {len(batches)} batches of ≤{BATCH}")
    print(f"ICP titles: {len(ICP_TITLES)}  ·  run_root: {RUN_ROOT}")
    fn = modal.Function.from_name("blitz-find-people", "run_find_people")
    spawned = []
    for i, b in enumerate(batches):
        h = fn.spawn(b, batch_label=RUN_ROOT, run_id=f"{RUN_ROOT}-{i:03d}",
                     priority=PRIORITY, people_filter=ICP_PEOPLE)
        spawned.append(h.object_id)
    print(f"\nspawned {len(spawned)} Modal calls under run_root '{RUN_ROOT}'")
    print(f"monitor: SELECT batch_label, sum(companies), sum(people_found), sum(people_upserted), "
          f"count(*) FROM ops.blitz_find_people_runs WHERE run_root LIKE '{RUN_ROOT}%' GROUP BY 1;")


if __name__ == "__main__":
    main()
