"""Backfill people_canonical.person_linkedin_url from gtm.contacts (idempotent, in-place overwrite).

WHY THIS EXISTS. active/people_canonical is the canonical person spine (one row per human, keyed on
canonical_person_id; source_platform lives in the person_source_platforms sidecar). The
operator-curated person LinkedIn URLs landed in gtm.contacts (s.../api/v1/contacts) are joined
onto the people grain HERE, durably, to fill rows that arrived without a LinkedIn URL.

MECHANICS. Read people_canonical → for every row whose person_linkedin_url is NULL, fill it from
gtm.contacts deduped to the LATEST url per (company-domain × first+last name). Filling a URL
CHANGES that row's canonical identity: a null-URL person is keyed sha256('pid:'||legacy) and MUST
be re-keyed to sha256(canonical LinkedIn) once a URL is known — so every filled row's
canonical_person_id + person_linkedin_url_norm are RECOMPUTED via _people_canonical (the one id
helper), keeping ids identical to every other writer. Then Lance overwrite (v2.1 writer params) +
rebuild the canonical people index plan. COALESCE never overwrites an existing url → re-running is
a safe no-op once converged; every other column is byte-identical.

INDEX REBUILD (the overwrite drops ALL indices — the new version starts with none). The canonical
people plan is rebuilt: BTREE [canonical_person_id, person_id, person_linkedin_url,
normalized_domain, company_id]. A DRIFT TRIPWIRE snapshots which COLUMNS the live dataset indexes
BEFORE the overwrite and diffs them against what was rebuilt — any column that lost coverage is
logged loudly, split so the remediation is correct: a column the plan tried but failed to rebuild
is a transient build error (re-run); a column the plan never lists is genuine drift.

    doppler run -- python3 pipelines/gtm/backfill_people_person_linkedin_from_contacts.py
"""
from __future__ import annotations

import os
import re

import lance
import psycopg
import pyarrow as pa

from pipelines.gtm import _people_canonical as pc

# REPOINT: fill + overwrite the canonical people dataset.
PEOPLE_URI = pc.PEOPLE_URI
# Canonical people index plan (see people_canonical schema in the design doc §Phase C).
PEOPLE_INDEXES = {
    "BTREE": [
        "canonical_person_id",
        "person_id",
        "person_linkedin_url",
        "normalized_domain",
        "company_id",
    ],
}
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


def _indexed_columns(dataset) -> set[str]:
    """Columns carrying at least one committed scalar index. Derived from lance's default index
    name ('{col}_idx'); tolerates list_indices() returning dicts or objects. Used to detect
    coverage LOSS — a column going indexed → unindexed — which is the exact bug this guards
    against, independent of index type."""
    cols: set[str] = set()
    for ix in dataset.list_indices():
        name = ix.get("name") if isinstance(ix, dict) else getattr(ix, "name", None)
        if name:
            cols.add(name[:-4] if name.endswith("_idx") else name)
    return cols


def main() -> None:
    so = pc.r2_storage_options()

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

    # 2. Read canonical people, COALESCE person_linkedin_url where NULL.
    ds = lance.dataset(PEOPLE_URI, storage_options=so)
    tbl = ds.to_table()
    n = tbl.num_rows
    nd = tbl.column("normalized_domain").to_pylist()
    fn = tbl.column("full_name").to_pylist()
    cur = tbl.column("person_linkedin_url").to_pylist()
    legacy = tbl.column("person_id").to_pylist()

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
    print(f"canonical people rows: {n:,} | person_linkedin_url before: {before:,} | newly filled: "
          f"{filled:,} | after: {before+filled:,}")

    if filled == 0:
        print("nothing to fill — already converged. No write.")
        return

    # Filling a URL re-keys the row: recompute canonical_person_id + person_linkedin_url_norm for
    # EVERY row via the one id helper, so a formerly null-URL person (keyed sha256('pid:'||legacy))
    # becomes sha256(canonical LinkedIn) — identical to every other writer. Rows that already had a
    # URL recompute to the same id (no-op), so this is safe for the whole table.
    new_cpid = [pc.canonical_person_id(u, lid) for u, lid in zip(new_url, legacy)]
    new_norm = [pc.normalize_linkedin(u) for u in new_url]

    # Snapshot the columns the LIVE dataset indexes BEFORE the overwrite (which drops every index —
    # the new version starts with none). Diffed post-rebuild to catch any column that lost coverage.
    live_cols = _indexed_columns(ds)
    print(f"live indexed columns (to be rebuilt): {sorted(live_cols)}")

    # 3. Overwrite with the canonical writer params (schema byte-identical; url + canonical id +
    #    norm columns updated). canonical_person_id / person_linkedin_url_norm are recomputed above.
    def _set(table, col, values, typ=pa.string()):
        if col in table.schema.names:
            i = table.schema.get_field_index(col)
            return table.set_column(i, table.schema.field(i), pa.array(values, type=typ))
        return table.append_column(col, pa.array(values, type=typ))

    out = _set(tbl, "person_linkedin_url", new_url)
    out = _set(out, "canonical_person_id", new_cpid)
    if "person_linkedin_url_norm" in tbl.schema.names:
        out = _set(out, "person_linkedin_url_norm", new_norm)
    lance.write_dataset(
        out, PEOPLE_URI, mode="overwrite",
        data_storage_version=pc.DATA_STORAGE_VERSION,
        max_rows_per_file=pc.MAX_ROWS_PER_FILE, max_bytes_per_file=pc.MAX_BYTES_PER_FILE,
        storage_options=so,
    )
    ds2 = lance.dataset(PEOPLE_URI, storage_options=so)
    print(f"wrote canonical people (overwrite, v{pc.DATA_STORAGE_VERSION}) — {ds2.count_rows():,} rows")

    # Rebuild the canonical people index plan. Best-effort per index: the data write is already
    # committed, so an index miss must not crash the run — it is logged loudly and tracked instead.
    rebuild_failed: set[str] = set()
    for index_type, cols in PEOPLE_INDEXES.items():
        for col in cols:
            if col not in ds2.schema.names:
                continue
            try:
                ds2.create_scalar_index(col, index_type=index_type)
                print(f"  {index_type:<6} ✓ {col}")
            except Exception as exc:  # noqa: BLE001 — an index miss must not fail a good write
                print(f"  {index_type:<6} ✗ {col}: {exc}")
                rebuild_failed.add(col)

    # DRIFT TRIPWIRE — any column indexed live but unindexed after the rebuild just lost coverage.
    # Split the diagnosis so the remediation is correct: a column the plan tried and FAILED to
    # rebuild is a transient build/R2 error (re-run), NOT drift; a column the plan never lists is
    # genuine drift (reconcile PEOPLE_INDEXES).
    lost = live_cols - _indexed_columns(lance.dataset(PEOPLE_URI, storage_options=so))
    if lost & rebuild_failed:
        print(f"  ⚠️  index BUILD FAILED for {sorted(lost & rebuild_failed)} (these ARE in the "
              f"plan) — transient build/R2 error, NOT drift. Re-run or investigate R2.")
    drift = lost - rebuild_failed
    if drift:
        print(f"  ⚠️  DRIFT — columns indexed live but NOT in PEOPLE_INDEXES, now DROPPED: "
              f"{sorted(drift)}. Reconcile PEOPLE_INDEXES and reindex.")

    after = ds2.to_table(columns=["person_linkedin_url"]).column(0).to_pylist()
    print(f"VERIFY person_linkedin_url populated: {sum(1 for v in after if v):,} / {ds2.count_rows():,}")


if __name__ == "__main__":
    main()
