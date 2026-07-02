#!/usr/bin/env python3
"""Build active/sam_labor_universe — SAM-native staffing/labor/professional-services firms,
enriched with federal-award history + PDL firmographics so every slice is an index filter.

Universe = sam_master_entities whose primary_naics is in the labor/services families below,
joined to a canonical domain (sam_master_domains, 1/uei). Enriched per firm:
  * in_our_staffing   — domain also in active/staffing_agencies
  * has_prime         — firmographics_company_map_serving.has_federal_awards / award_count>0
  * has_subaward      — appears as subawardee_uei in usaspending contract_subaward
  * award_active      — has_prime OR has_subaward
  * pdl_matched / pdl_employee_size_range / pdl_company_linkedin_url — PDL companies match by domain

NAICS families: 5613 Employment · 5614/5612/5619 Support · 5413 Eng · 5415 IT · 5416 Consulting ·
                238 Specialty Trade · 236 Building Construction.

TARGET: s3://data-sink/active/sam_labor_universe/  (Lance v2.1, overwrite)
INDEXES: BTREE(uei, normalized_domain) ·
         BITMAP(naics_family, in_our_staffing, award_active, has_prime, has_subaward, pdl_employee_size_range)

RUN: doppler run -p core-x -c prd -- uv run --no-project --with pylance --with duckdb --with pyarrow \
        python3 scripts/build_sam_labor_universe.py
"""
from __future__ import annotations
import os
import duckdb
import lance
import pyarrow as pa

ACTIVE = "s3://data-sink/active"
URI = f"{ACTIVE}/sam_labor_universe/"
FAMILIES = [
    ("5613", "Employment Services"), ("5614", "Business Support Services"),
    ("5612", "Facilities Support Services"), ("5619", "Other Support Services"),
    ("5413", "Architectural/Engineering"), ("5415", "Computer Systems Design/IT"),
    ("5416", "Mgmt/Sci/Tech Consulting"), ("238", "Specialty Trade Contractors"),
    ("236", "Building Construction"),
]


def _so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": ep, "region": "auto"}


def _family(naics: str) -> tuple[str, str]:
    for pfx, label in FAMILIES:
        if naics and naics.startswith(pfx):
            return pfx, label
    return "", ""


def main() -> int:
    so = _so()
    sme = lance.dataset(f"{ACTIVE}/sam_master_entities", storage_options=so)
    smd = lance.dataset(f"{ACTIVE}/sam_master_domains", storage_options=so)
    sa = lance.dataset(f"{ACTIVE}/staffing_agencies", storage_options=so)
    fc = lance.dataset(f"{ACTIVE}/firmographics_company_map_serving", storage_options=so)
    cs = lance.dataset(f"{ACTIVE}/usaspending_api_fresh/contract_subaward", storage_options=so)
    pn = lance.dataset(f"{ACTIVE}/pdl_normalized_companies", storage_options=so)
    pc = lance.dataset(f"{ACTIVE}/pdl_companies", storage_options=so)

    sa_dom = set(v for v in sa.to_table(columns=["domain_norm"]).column("domain_norm").to_pylist() if v)
    uei2dom: dict[str, str] = {}
    smd_t = smd.to_table(columns=["normalized_domain", "uei"])
    for d, u in zip(smd_t.column("normalized_domain").to_pylist(), smd_t.column("uei").to_pylist()):
        if d and u and u not in uei2dom:
            uei2dom[u] = d

    ft = fc.to_table(columns=["uei", "has_federal_awards", "award_count"])
    prime = {ft.column("uei")[i].as_py() for i in range(ft.num_rows)
             if (ft.column("has_federal_awards")[i].as_py() or (ft.column("award_count")[i].as_py() or 0) > 0)}
    sub = set(v for v in cs.to_table(columns=["subawardee_uei"]).column("subawardee_uei").to_pylist() if v)

    naics_filter = " OR ".join(f"primary_naics LIKE '{p}%'" for p, _ in FAMILIES)
    ent = sme.scanner(filter=naics_filter,
                      columns=["uei", "legal_business_name", "primary_naics", "cage_code",
                               "entity_url"]).to_table().to_pylist()
    print(f"SAM entities in labor NAICS = {len(ent):,}")

    base = []
    for r in ent:
        dom = uei2dom.get(r["uei"])
        if not dom:
            continue
        pfx, label = _family(r["primary_naics"])
        hp, hs = r["uei"] in prime, r["uei"] in sub
        base.append({"uei": r["uei"], "legal_business_name": r["legal_business_name"],
                     "normalized_domain": dom, "primary_naics": r["primary_naics"],
                     "naics_family": pfx, "naics_label": label, "cage_code": r["cage_code"],
                     "entity_url": r["entity_url"], "in_our_staffing": dom in sa_dom,
                     "has_prime": hp, "has_subaward": hs, "award_active": hp or hs})
    print(f"with a domain = {len(base):,}")

    # ── PDL match — ONE streaming pass over each 35M-row dataset (DuckDB hash join) ──
    domains = sorted({b["normalized_domain"] for b in base})
    con = duckdb.connect(); con.execute("SET memory_limit='12GB'; PRAGMA threads=8;")
    con.register("base_dom", pa.table({"normalized_domain": pa.array(domains, pa.string())}))
    con.register("pdln", pn.scanner(columns=["normalized_domain", "pdl_company_id", "linkedin_slug"]).to_reader())
    con.execute("""CREATE TEMP TABLE m AS
        SELECT normalized_domain, pdl_company_id AS pid, linkedin_slug AS slug FROM (
            SELECT n.normalized_domain, n.pdl_company_id, n.linkedin_slug,
                   row_number() OVER (PARTITION BY n.normalized_domain ORDER BY n.pdl_company_id) rn
            FROM pdln n JOIN base_dom b ON n.normalized_domain = b.normalized_domain
        ) WHERE rn = 1""")
    con.register("pdlc", pc.scanner(columns=["pdl_company_id", "employee_size_range", "linkedin_url"]).to_reader())
    rows = con.execute("""
        SELECT m.normalized_domain,
               c.employee_size_range AS band,
               coalesce(c.linkedin_url, 'https://www.linkedin.com/company/' || m.slug) AS li
        FROM m JOIN pdlc c ON m.pid = c.pdl_company_id""").fetchall()
    dom2band = {d: b for d, b, _ in rows}
    dom2li = {d: (li if li and not li.endswith("/None") else None) for d, _, li in rows}
    print(f"PDL-matched domains = {len(dom2band):,}")

    n = len(base)
    def col(key, typ):
        return pa.array([b[key] for b in base], typ)
    tbl = pa.table({
        "uei": col("uei", pa.string()), "legal_business_name": col("legal_business_name", pa.string()),
        "normalized_domain": col("normalized_domain", pa.string()), "primary_naics": col("primary_naics", pa.string()),
        "naics_family": col("naics_family", pa.string()), "naics_label": col("naics_label", pa.string()),
        "cage_code": col("cage_code", pa.string()), "entity_url": col("entity_url", pa.string()),
        "in_our_staffing": col("in_our_staffing", pa.bool_()),
        "has_prime": col("has_prime", pa.bool_()), "has_subaward": col("has_subaward", pa.bool_()),
        "award_active": col("award_active", pa.bool_()),
        "pdl_matched": pa.array([b["normalized_domain"] in dom2band for b in base], pa.bool_()),
        "pdl_employee_size_range": pa.array([dom2band.get(b["normalized_domain"]) for b in base], pa.string()),
        "pdl_company_linkedin_url": pa.array([dom2li.get(b["normalized_domain"]) for b in base], pa.string()),
    })
    lance.write_dataset(tbl, URI, mode="overwrite", data_storage_version="2.1", storage_options=so)
    ds = lance.dataset(URI, storage_options=so)
    print(f"wrote {ds.count_rows():,} rows ({len(tbl.schema)} cols) → {URI}")
    for c in ("uei", "normalized_domain"):
        ds.create_scalar_index(c, index_type="BTREE"); print(f"  BTREE  ✓ {c}")
    for c in ("naics_family", "in_our_staffing", "award_active", "has_prime", "has_subaward",
              "pdl_employee_size_range"):
        ds.create_scalar_index(c, index_type="BITMAP"); print(f"  BITMAP ✓ {c}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
