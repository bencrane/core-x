#!/usr/bin/env python3
"""Build the SAM-native facilities/support labor universe — the NAICS-561 families NOT already
in active/sam_labor_universe (office admin 5611, travel 5615, investigation/security 5616 incl.
security guards 561612, services-to-buildings 5617 incl. janitorial/landscaping). Inclusion bar =
entity_url present (domain optional). Excludes every UEI already in active/sam_labor_universe.

Split by PDL match (operator's rule — no PDL match ⇒ no employee_size_range to filter on):
  * active/sam_facilities_labor_universe  — PDL-matched (employee band + company LinkedIn)
  * active/sam_facilities_labor_no_pdl     — no PDL match (entity_url only, no band)

Both carry federal-award history (has_prime / has_subaward / award_active) + in_our_staffing.

RUN: doppler run -p core-x -c prd -- uv run --no-project --with pylance --with duckdb --with pyarrow \
        python3 scripts/build_sam_facilities_labor_universe.py
"""
from __future__ import annotations
import os
import duckdb
import lance
import pyarrow as pa

ACTIVE = "s3://data-sink/active"
URI_PDL = f"{ACTIVE}/sam_facilities_labor_universe/"
URI_NOPDL = f"{ACTIVE}/sam_facilities_labor_no_pdl/"
FAMILIES = [
    ("5611", "Office Administrative Services"), ("5615", "Travel Arrangement & Reservation"),
    ("5616", "Investigation & Security Services"), ("5617", "Services to Buildings & Dwellings"),
]


def _so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": ep, "region": "auto"}


def _family(naics: str):
    for pfx, label in FAMILIES:
        if naics and naics.startswith(pfx):
            return pfx, label
    return "", ""


def _has(x) -> bool:
    return bool(x and str(x).strip())


def main() -> int:
    so = _so()
    sme = lance.dataset(f"{ACTIVE}/sam_master_entities", storage_options=so)
    smd = lance.dataset(f"{ACTIVE}/sam_master_domains", storage_options=so)
    sa = lance.dataset(f"{ACTIVE}/staffing_agencies", storage_options=so)
    fc = lance.dataset(f"{ACTIVE}/firmographics_company_map_serving", storage_options=so)
    cs = lance.dataset(f"{ACTIVE}/usaspending_api_fresh/contract_subaward", storage_options=so)
    pn = lance.dataset(f"{ACTIVE}/pdl_normalized_companies", storage_options=so)
    pc = lance.dataset(f"{ACTIVE}/pdl_companies", storage_options=so)
    existing = set(v for v in lance.dataset(f"{ACTIVE}/sam_labor_universe", storage_options=so)
                   .to_table(columns=["uei"]).column("uei").to_pylist() if v)

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
                      columns=["uei", "legal_business_name", "primary_naics", "cage_code", "entity_url"]).to_table().to_pylist()
    base = []
    for r in ent:
        if not _has(r["entity_url"]) or r["uei"] in existing:
            continue
        pfx, label = _family(r["primary_naics"])
        dom = uei2dom.get(r["uei"])
        hp, hs = r["uei"] in prime, r["uei"] in sub
        base.append({"uei": r["uei"], "legal_business_name": r["legal_business_name"],
                     "normalized_domain": dom, "entity_url": r["entity_url"], "primary_naics": r["primary_naics"],
                     "naics_family": pfx, "naics_label": label, "cage_code": r["cage_code"],
                     "in_our_staffing": dom in sa_dom if dom else False,
                     "has_prime": hp, "has_subaward": hs, "award_active": hp or hs})
    print(f"facilities firms (entity_url, not in existing) = {len(base):,} · with a domain = {sum(1 for b in base if b['normalized_domain']):,}")

    # ── PDL match: single streaming pass ──
    domains = sorted({b["normalized_domain"] for b in base if b["normalized_domain"]})
    con = duckdb.connect(); con.execute("SET memory_limit='12GB'; PRAGMA threads=8;")
    con.register("base_dom", pa.table({"normalized_domain": pa.array(domains, pa.string())}))
    con.register("pdln", pn.scanner(columns=["normalized_domain", "pdl_company_id", "linkedin_slug"]).to_reader())
    con.execute("""CREATE TEMP TABLE m AS SELECT normalized_domain, pdl_company_id AS pid, linkedin_slug AS slug FROM (
        SELECT n.normalized_domain, n.pdl_company_id, n.linkedin_slug,
               row_number() OVER (PARTITION BY n.normalized_domain ORDER BY n.pdl_company_id) rn
        FROM pdln n JOIN base_dom b ON n.normalized_domain = b.normalized_domain) WHERE rn = 1""")
    con.register("pdlc", pc.scanner(columns=["pdl_company_id", "employee_size_range", "linkedin_url"]).to_reader())
    rows = con.execute("""SELECT m.normalized_domain, c.employee_size_range,
        coalesce(c.linkedin_url, 'https://www.linkedin.com/company/' || m.slug) AS li
        FROM m JOIN pdlc c ON m.pid = c.pdl_company_id WHERE c.employee_size_range IS NOT NULL""").fetchall()
    dom2band = {d: b for d, b, _ in rows}
    dom2li = {d: (li if li and not li.endswith("/None") else None) for d, _, li in rows}

    matched = [b for b in base if b["normalized_domain"] in dom2band]
    nopdl = [b for b in base if b["normalized_domain"] not in dom2band]
    print(f"PDL-matched = {len(matched):,} · no-PDL = {len(nopdl):,}")

    def _write(recs, uri, with_pdl):
        cols = {
            "uei": pa.array([r["uei"] for r in recs], pa.string()),
            "legal_business_name": pa.array([r["legal_business_name"] for r in recs], pa.string()),
            "normalized_domain": pa.array([r["normalized_domain"] for r in recs], pa.string()),
            "entity_url": pa.array([r["entity_url"] for r in recs], pa.string()),
            "primary_naics": pa.array([r["primary_naics"] for r in recs], pa.string()),
            "naics_family": pa.array([r["naics_family"] for r in recs], pa.string()),
            "naics_label": pa.array([r["naics_label"] for r in recs], pa.string()),
            "cage_code": pa.array([r["cage_code"] for r in recs], pa.string()),
            "in_our_staffing": pa.array([r["in_our_staffing"] for r in recs], pa.bool_()),
            "has_prime": pa.array([r["has_prime"] for r in recs], pa.bool_()),
            "has_subaward": pa.array([r["has_subaward"] for r in recs], pa.bool_()),
            "award_active": pa.array([r["award_active"] for r in recs], pa.bool_()),
        }
        if with_pdl:
            cols["pdl_employee_size_range"] = pa.array([dom2band.get(r["normalized_domain"]) for r in recs], pa.string())
            cols["pdl_company_linkedin_url"] = pa.array([dom2li.get(r["normalized_domain"]) for r in recs], pa.string())
        lance.write_dataset(pa.table(cols), uri, mode="overwrite", data_storage_version="2.1", storage_options=so)
        ds = lance.dataset(uri, storage_options=so)
        for c in ("uei", "normalized_domain"):
            ds.create_scalar_index(c, index_type="BTREE")
        bm = ["naics_family", "in_our_staffing", "award_active", "has_prime", "has_subaward"]
        if with_pdl:
            bm.append("pdl_employee_size_range")
        for c in bm:
            ds.create_scalar_index(c, index_type="BITMAP")
        print(f"  wrote {ds.count_rows():,} → {uri}")

    _write(matched, URI_PDL, True)
    _write(nopdl, URI_NOPDL, False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
