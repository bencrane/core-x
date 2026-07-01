#!/usr/bin/env python3
"""Build active/sam_labor_universe — SAM-native staffing/labor/professional-services firms.

Universe = sam_master_entities whose primary_naics is in the labor/services families below,
joined to a canonical domain (sam_master_domains, 1/uei), flagged in_our_staffing (domain also
present in active/staffing_agencies). Net-new = in_our_staffing = false.

NAICS families (primary_naics prefix → label):
    5613 Employment Services · 5614 Business Support · 5612 Facilities Support ·
    5619 Other Support · 5413 Architectural/Engineering · 5415 Computer Systems Design/IT ·
    5416 Mgmt/Sci/Tech Consulting · 238 Specialty Trade Contractors · 236 Building Construction

TARGET: s3://data-sink/active/sam_labor_universe/  (Lance v2.1, overwrite)
INDEXES: BTREE(uei, normalized_domain) · BITMAP(naics_family, in_our_staffing)

RUN: doppler run -p core-x -c prd -- uv run --no-project --with pylance --with pyarrow \
        python3 scripts/build_sam_labor_universe.py
"""
from __future__ import annotations
import os
import lance
import pyarrow as pa

ACTIVE = "s3://data-sink/active"
URI = f"{ACTIVE}/sam_labor_universe/"
FAMILIES = [  # (prefix, label) — checked longest/first; 4-digit before 3-digit
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

    sa_dom = set(v for v in sa.to_table(columns=["domain_norm"]).column("domain_norm").to_pylist() if v)
    smd_t = smd.to_table(columns=["normalized_domain", "uei"])
    uei2dom: dict[str, str] = {}
    for d, u in zip(smd_t.column("normalized_domain").to_pylist(), smd_t.column("uei").to_pylist()):
        if d and u and u not in uei2dom:
            uei2dom[u] = d

    naics_filter = " OR ".join(f"primary_naics LIKE '{p}%'" for p, _ in FAMILIES)
    ent = sme.scanner(filter=naics_filter,
                      columns=["uei", "legal_business_name", "primary_naics", "cage_code",
                               "entity_url"]).to_table().to_pylist()
    print(f"SAM entities in labor NAICS = {len(ent):,}")

    cols = {k: [] for k in ("uei", "legal_business_name", "normalized_domain", "primary_naics",
                            "naics_family", "naics_label", "cage_code", "entity_url",
                            "in_our_staffing")}
    for r in ent:
        dom = uei2dom.get(r["uei"])
        if not dom:
            continue
        pfx, label = _family(r["primary_naics"])
        cols["uei"].append(r["uei"])
        cols["legal_business_name"].append(r["legal_business_name"])
        cols["normalized_domain"].append(dom)
        cols["primary_naics"].append(r["primary_naics"])
        cols["naics_family"].append(pfx)
        cols["naics_label"].append(label)
        cols["cage_code"].append(r["cage_code"])
        cols["entity_url"].append(r["entity_url"])
        cols["in_our_staffing"].append(dom in sa_dom)

    n = len(cols["uei"])
    net_new = sum(1 for v in cols["in_our_staffing"] if not v)
    print(f"with a domain = {n:,} · net-new (not in our staffing) = {net_new:,}")

    tbl = pa.table({
        "uei": pa.array(cols["uei"], pa.string()),
        "legal_business_name": pa.array(cols["legal_business_name"], pa.string()),
        "normalized_domain": pa.array(cols["normalized_domain"], pa.string()),
        "primary_naics": pa.array(cols["primary_naics"], pa.string()),
        "naics_family": pa.array(cols["naics_family"], pa.string()),
        "naics_label": pa.array(cols["naics_label"], pa.string()),
        "cage_code": pa.array(cols["cage_code"], pa.string()),
        "entity_url": pa.array(cols["entity_url"], pa.string()),
        "in_our_staffing": pa.array(cols["in_our_staffing"], pa.bool_()),
    })
    lance.write_dataset(tbl, URI, mode="overwrite", data_storage_version="2.1", storage_options=so)
    ds = lance.dataset(URI, storage_options=so)
    print(f"wrote {ds.count_rows():,} rows → {URI}")
    for col in ("uei", "normalized_domain"):
        ds.create_scalar_index(col, index_type="BTREE"); print(f"  BTREE  ✓ {col}")
    for col in ("naics_family", "in_our_staffing"):
        ds.create_scalar_index(col, index_type="BITMAP"); print(f"  BITMAP ✓ {col}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
