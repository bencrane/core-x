"""Backfill active/people.person_linkedin_url from gtm.contacts (idempotent, in-place).

WHY THIS EXISTS. active/people is the STANDALONE Gen-3 SoR — dexarchive is fully severed
(companies_people_bulk.ingest is retired), so there is no full-overwrite that would wipe an
in-place enrichment. The operator-curated person LinkedIn URLs landed in gtm.contacts
(s.../api/v1/contacts) are joined onto the people grain HERE, durably.

MECHANICS. Read active/people (tiny, one fragment) → for every row whose person_linkedin_url is
NULL, fill it from gtm.contacts deduped to the LATEST url per (company-domain × first+last name)
→ Lance overwrite (canonical writer params, imported from companies_people_bulk) → rebuild the
people BTREE indexes. COALESCE never overwrites an existing url, so re-running is a safe no-op
once converged. The people row set and every other column are byte-identical — only NULL
person_linkedin_url cells change.

    doppler run -- python3 pipelines/gtm/backfill_people_person_linkedin_from_contacts.py
"""
from __future__ import annotations

import importlib.util
import os
import re

import lance
import psycopg
import pyarrow as pa

# Reuse the canonical SoR constants + writer params + index plan (single source of truth).
_spec = importlib.util.spec_from_file_location(
    "cpb", os.path.join(os.path.dirname(__file__), "companies_people_bulk.py"))
_cpb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cpb)

PEOPLE_URI = _cpb.DATASET_URI["people"]
_WS = re.compile(r"\s+")
_NONALPHA = re.compile(r"[^a-z ]")


def _name_key(full_name: str | None) -> str | None:
    """first+last match key — mirrors the SQL bridge: lower → keep [a-z ] → collapse → first+last."""
    s = _WS.sub(" ", _NONALPHA.sub(" ", (full_name or "").lower())).strip()
    if not s:
        return None
    t = s.split(" ")
    return f"{t[0]} {t[-1]}" if len(t) >= 2 else t[0]


def _source_dsn() -> str:
    dsn = os.environ["HQX_DB_URL_POOLED"]
    return dsn if "sslmode=" in dsn else dsn + ("&" if "?" in dsn else "?") + "sslmode=require"


def main() -> None:
    so = _cpb._r2_storage_options()

    # 1. gtm.contacts → latest person LinkedIn URL per (domain_norm, first+last name).
    lookup: dict[tuple[str, str], str] = {}
    with psycopg.connect(_source_dsn(), autocommit=True, prepare_threshold=None) as conn:
        rows = conn.execute(
            "SELECT domain_norm, full_name, person_linkedin_url FROM gtm.contacts "
            "WHERE person_linkedin_url IS NOT NULL AND domain_norm IS NOT NULL "
            "ORDER BY landed_at ASC"  # ASC → last write wins = latest url
        ).fetchall()
    for dom, full_name, url in rows:
        k = _name_key(full_name)
        if k:
            lookup[(dom, k)] = url
    print(f"gtm.contacts url keys (domain × first+last): {len(lookup):,}")

    # 2. Read active/people, COALESCE person_linkedin_url where NULL.
    ds = lance.dataset(PEOPLE_URI, storage_options=so)
    tbl = ds.to_table()
    n = tbl.num_rows
    nd = tbl.column("normalized_domain").to_pylist()
    fn = tbl.column("full_name").to_pylist()
    cur = tbl.column("person_linkedin_url").to_pylist()

    filled = 0
    new_url: list[str | None] = []
    for dom, name, existing in zip(nd, fn, cur):
        if existing:
            new_url.append(existing)
            continue
        hit = lookup.get((dom, _name_key(name))) if dom else None
        new_url.append(hit)
        if hit:
            filled += 1
    before = sum(1 for v in cur if v)
    print(f"active/people rows: {n:,} | person_linkedin_url before: {before:,} | newly filled: {filled:,} | after: {before+filled:,}")

    if filled == 0:
        print("nothing to fill — already converged. No write.")
        return

    # 3. Overwrite with the canonical writer params (schema byte-identical, only the column changes).
    idx = tbl.schema.get_field_index("person_linkedin_url")
    out = tbl.set_column(idx, tbl.schema.field(idx), pa.array(new_url, type=pa.string()))
    lance.write_dataset(
        out, PEOPLE_URI, mode="overwrite",
        data_storage_version=_cpb.DATA_STORAGE_VERSION,
        max_rows_per_file=_cpb.MAX_ROWS_PER_FILE, max_bytes_per_file=_cpb.MAX_BYTES_PER_FILE,
        storage_options=so,
    )
    ds2 = lance.dataset(PEOPLE_URI, storage_options=so)
    print(f"wrote active/people (overwrite, v{_cpb.DATA_STORAGE_VERSION}) — {ds2.count_rows():,} rows")
    for col in _cpb.INDEXES["people"]["BTREE"]:
        ds2.create_scalar_index(col, index_type="BTREE")
        print(f"  BTREE ✓ {col}")
    after = ds2.to_table(columns=["person_linkedin_url"]).column(0).to_pylist()
    print(f"VERIFY person_linkedin_url populated: {sum(1 for v in after if v):,} / {ds2.count_rows():,}")


if __name__ == "__main__":
    main()
