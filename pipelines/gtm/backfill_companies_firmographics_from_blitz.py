"""Backfill active/companies with Blitz firmographics (idempotent, in-place).

WHY THIS EXISTS. active/companies is the standalone Gen-3 SoR (dexarchive severed;
companies_people_bulk.ingest retired), so there is no full-overwrite that would wipe an
in-place enrichment. This worker affiliate-links each company row to the Blitz firmographic
reference grain (s3://data-sink/active/firmographics_blitz/) and lands the high-value
firmographic FILTER fields (employee_size_band, industry, company_type, founded_year, hq
geography, …) directly onto the company row — so an audience built on active/companies can
filter by size / industry / type / region without a downstream join.

JOIN KEY (operator directive + coverage reality). The directive was "use company_linkedin_url
since the firmo has that". But only ~791/9,008 companies carry a company_linkedin_url, so a
linkedin-only join covers ~6%. firmographics_blitz's PRIMARY KEY is domain_norm (100% populated,
unique), and active/companies carries normalized_domain — a domain join covers ~79%. So the
join is LINKEDIN-PRIMARY, DOMAIN-FALLBACK: prefer the company_linkedin_url match (highest
precision, honors the directive); fall back to normalized_domain where linkedin is absent or
misses. ``firmo_match_key`` records which rail hit ('linkedin' | 'domain' | NULL).

COMPANY LINKEDIN BACKFILL (two sources). Blitz firmo enrichment is itself linkedin-keyed
(Workflow B: company_linkedin_url -> firmographics), so firmographics_blitz.linkedin_url is 100%
populated and authoritative. clay_find_companies (the Clay find-companies grain) is a SECOND
domain->linkedin source. active/companies carries company_linkedin_url on only ~791/9,008 rows,
so this worker COALESCES it own -> firmo -> clay: keep the company's own URL where present, else
fill from the matched firmo linkedin_url, else from clay_find_companies by domain — never
overwriting a curated value. company_linkedin_source records which rail populated it
('own' | 'firmo' | 'clay' | NULL); firmo_linkedin_url retains the firmo URL verbatim.

PRESERVATION. company_id, company_name, normalized_domain, source_platform are carried verbatim;
company_linkedin_url is null-filled from firmo/clay (never overwritten); the row set is
byte-identical (PK company_id, 1:1); only firmographic + provenance columns are new. Firmographic
values are copied from the already-derived firmographics_blitz columns, not re-interpreted.

INDEXES:
    BTREE  : normalized_domain (existing), company_id (PK)
    BITMAP : industry, employee_size_band, company_type, hq_region   (audience filters)

    doppler run --project core-x --config prd -- python3 \\
        pipelines/gtm/backfill_companies_firmographics_from_blitz.py
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import os
import re

import lance
import pyarrow as pa

# Reuse the canonical SoR constants + writer params (single source of truth).
_spec = importlib.util.spec_from_file_location(
    "cpb", os.path.join(os.path.dirname(__file__), "companies_people_bulk.py"))
_cpb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cpb)

# The pristine active/companies base columns. Re-reading ONLY these makes the worker
# idempotent: a prior run's appended firmographic columns are dropped and recomputed fresh
# (otherwise an overwrite would duplicate field names).
BASE_COLS = ["company_id", "company_name", "normalized_domain", "company_linkedin_url", "source_platform"]

COMPANIES_URI = _cpb.DATASET_URI["companies"]
FIRMO_URI = os.environ.get("FIRMOGRAPHICS_BLITZ_URI", "s3://data-sink/active/firmographics_blitz/")
CLAY_FIND_COMPANIES_URI = os.environ.get("CLAY_FIND_COMPANIES_URI", "s3://data-sink/active/clay_find_companies/")
DATA_STORAGE_VERSION = _cpb.DATA_STORAGE_VERSION
MAX_ROWS_PER_FILE = _cpb.MAX_ROWS_PER_FILE
MAX_BYTES_PER_FILE = _cpb.MAX_BYTES_PER_FILE

# Firmographic fields pulled from firmographics_blitz (the audience-filter surface).
FIRMO_FIELDS = [
    "industry", "employee_size_band", "employees_on_linkedin", "company_type",
    "founded_year", "followers", "specialties", "hq_city", "hq_state",
    "hq_region", "hq_continent", "uei",
]
INT_FIELDS = {"employees_on_linkedin": pa.int64(), "founded_year": pa.int32(), "followers": pa.int64()}
LIST_FIELDS = {"specialties": pa.list_(pa.string())}

INDEX_BTREE = ["normalized_domain", "company_id"]
INDEX_BITMAP = ["industry", "employee_size_band", "company_type", "hq_region"]

_scheme = re.compile(r"^https?://"); _www = re.compile(r"^www\.")
_locale = re.compile(r"^[a-z]{2}\.linkedin\.com/")


def _norm_li(u: str | None) -> str | None:
    s = (u or "").strip().lower()
    if not s:
        return None
    s = _locale.sub("linkedin.com/", _www.sub("", _scheme.sub("", s))).rstrip("/")
    return s if "linkedin.com/" in s else None


def _norm_dom(d: str | None) -> str | None:
    s = (d or "").strip().lower()
    return s or None


def main() -> None:
    so = _cpb._r2_storage_options()

    # 1) firmographics_blitz → linkedin/domain lookup over the firmographic field columns
    firmo = lance.dataset(FIRMO_URI, storage_options=so)
    fcols = firmo.to_table(columns=["linkedin_url", "domain_norm", *FIRMO_FIELDS]).to_pydict()
    n_firmo = len(fcols["domain_norm"])
    li_map: dict[str, int] = {}
    dom_map: dict[str, int] = {}
    for i in range(n_firmo):
        ln = _norm_li(fcols["linkedin_url"][i])
        if ln is not None and ln not in li_map:
            li_map[ln] = i
        dn = fcols["domain_norm"][i]
        if dn and dn not in dom_map:
            dom_map[dn] = i
    print(f"firmo rows={n_firmo:,}  linkedin_keys={len(li_map):,}  domain_keys={len(dom_map):,}")

    # 1b) clay_find_companies → domain → company LinkedIn (a SECOND linkedin source, after firmo)
    clay = lance.dataset(CLAY_FIND_COMPANIES_URI, storage_options=so)
    cct = clay.to_table(columns=["domain_norm", "linkedin_url"]).to_pydict()
    clay_map: dict[str, str] = {}
    for d, l in zip(cct["domain_norm"], cct["linkedin_url"]):
        if d and l and d not in clay_map:
            clay_map[d] = l
    print(f"clay_find_companies domain→linkedin keys={len(clay_map):,}")

    # 2) active/companies → join linkedin-primary, domain-fallback
    comp = lance.dataset(COMPANIES_URI, storage_options=so)
    orig = comp.to_table(columns=BASE_COLS)  # base only → idempotent re-runs
    orig_cols = list(BASE_COLS)
    n = orig.num_rows
    comp_li_raw = orig.column("company_linkedin_url").to_pylist()
    comp_dom_raw = orig.column("normalized_domain").to_pylist()

    out_fields: dict[str, list] = {f: [] for f in FIRMO_FIELDS}
    match_key: list = []
    firmo_li_col: list = []          # the authoritative firmo company LinkedIn (provenance)
    company_li_coalesced: list = []  # own → firmo → clay (fills nulls, never overwrites own)
    li_source: list = []             # which rail populated company_linkedin_url
    hit_li = hit_dom = 0
    own_li = filled_firmo = filled_clay = 0
    for i in range(n):
        idx = None
        key = None
        ln = _norm_li(comp_li_raw[i])
        if ln is not None and ln in li_map:
            idx, key = li_map[ln], "linkedin"; hit_li += 1
        else:
            dn = _norm_dom(comp_dom_raw[i])
            if dn is not None and dn in dom_map:
                idx, key = dom_map[dn], "domain"; hit_dom += 1
        match_key.append(key)
        firmo_li = fcols["linkedin_url"][idx] if idx is not None else None
        firmo_li_col.append(firmo_li)
        # coalesce company_linkedin_url: own → firmo → clay
        own = comp_li_raw[i]
        cdom = _norm_dom(comp_dom_raw[i])
        clay_li = clay_map.get(cdom) if cdom else None
        if own:
            company_li_coalesced.append(own); li_source.append("own"); own_li += 1
        elif firmo_li:
            company_li_coalesced.append(firmo_li); li_source.append("firmo"); filled_firmo += 1
        elif clay_li:
            company_li_coalesced.append(clay_li); li_source.append("clay"); filled_clay += 1
        else:
            company_li_coalesced.append(None); li_source.append(None)
        for f in FIRMO_FIELDS:
            out_fields[f].append(fcols[f][idx] if idx is not None else None)

    matched = hit_li + hit_dom
    populated = own_li + filled_firmo + filled_clay
    print(f"companies rows={n:,}  firmo-matched={matched:,} ({matched/n*100:.0f}%)  "
          f"[linkedin={hit_li:,} domain={hit_dom:,} miss={n-matched:,}]")
    print(f"company_linkedin_url populated={populated:,} ({populated/n*100:.0f}%)  "
          f"[own={own_li:,} firmo={filled_firmo:,} clay={filled_clay:,} none={n-populated:,}]")

    # 3) assemble enriched table: original columns (company_linkedin_url COALESCED with firmo) +
    #    firmographic columns + firmo_linkedin_url provenance + lineage
    arrays = []
    names = list(orig_cols)
    for c in orig_cols:
        if c == "company_linkedin_url":
            arrays.append(pa.array(company_li_coalesced, type=pa.string()))
        else:
            arrays.append(orig.column(c))
    for f in FIRMO_FIELDS:
        if f in INT_FIELDS:
            arrays.append(pa.array(out_fields[f], type=INT_FIELDS[f]))
        elif f in LIST_FIELDS:
            arrays.append(pa.array(out_fields[f], type=LIST_FIELDS[f]))
        else:
            arrays.append(pa.array(out_fields[f], type=pa.string()))
        names.append(f)
    arrays.append(pa.array(firmo_li_col, type=pa.string())); names.append("firmo_linkedin_url")
    arrays.append(pa.array(li_source, type=pa.string())); names.append("company_linkedin_source")
    arrays.append(pa.array(match_key, type=pa.string())); names.append("firmo_match_key")
    now = dt.datetime.now(dt.timezone.utc)
    arrays.append(pa.array([now] * n, type=pa.timestamp("us", tz="UTC"))); names.append("firmo_materialized_at")
    enriched = pa.table(arrays, names=names)

    # 4) Lance overwrite (canonical writer params) — same rows, +firmographic columns
    lance.write_dataset(enriched, COMPANIES_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                        storage_options=so)
    print(f"wrote active/companies (overwrite) — {enriched.num_rows:,} rows, {enriched.num_columns} cols")

    # 5) indexes — existing BTREE + new BITMAP filter accelerators
    ds = lance.dataset(COMPANIES_URI, storage_options=so)
    for col in INDEX_BTREE:
        try:
            ds.create_scalar_index(col, index_type="BTREE"); print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  BTREE  ✗ {col}: {exc}")
    for col in INDEX_BITMAP:
        try:
            ds.create_scalar_index(col, index_type="BITMAP"); print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  BITMAP ✗ {col}: {exc}")

    # 6) verify — row count preserved, PK unique, schema, indexes
    import pyarrow.compute as pc
    ds = lance.dataset(COMPANIES_URI, storage_options=so)
    rn = ds.count_rows()
    ids = ds.to_table(columns=["company_id"]).column("company_id")
    distinct = pc.count_distinct(ids).as_py()
    idx_names = sorted(
        (ix.get("name", str(ix)) if isinstance(ix, dict) else getattr(ix, "name", str(ix)))
        for ix in ds.list_indices())
    print(f"VERIFY rows={rn:,} (was {n:,}) · distinct(company_id)={distinct:,} · cols={len(ds.schema.names)}")
    print(f"  indexes={idx_names}")
    if rn != n:
        raise RuntimeError(f"row count changed: {rn} != {n} — aborting (data integrity)")
    if distinct != rn:
        raise RuntimeError(f"company_id not unique: {distinct} != {rn}")
    print("OK")


if __name__ == "__main__":
    main()
