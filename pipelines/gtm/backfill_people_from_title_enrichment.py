"""Backfill active/people with title_enrichment persons absent from the spine (idempotent insert).

WHY THIS EXISTS. active/title_enrichment is keyed on person_linkedin_url_norm; active/people is
the canonical person spine. A person who has an enriched title but is NOT in the spine is
invisible to every people-driven query/filter. This inserts those persons as identity rows so
the title↔people join reaches full coverage. active/people is the STANDALONE Gen-3 SoR (dexarchive
severed; companies_people_bulk.ingest retired), so a direct insert is the sanctioned growth path —
this simply adds 'title_enrichment' as one more curated source alongside work_emails /
clay_find_people / sfnet / csv / elfa / manual-seed. The Postgres SoR is untouched.

MECHANICS. Read active/title_enrichment (person key + verbatim LinkedIn + raw title) and
active/people (LinkedIn) → anti-join on the normalized LinkedIn key (the ONLY bridge; people
stores person_linkedin_url verbatim, so BOTH sides are normalized identically before the
compare) → for each missing person build a people-contract row and merge_insert
when_not_matched on person_id. Existing people rows are NEVER read-modified — pure insert.

GRAIN / IDEMPOTENCY. One inserted row per missing person_linkedin_url_norm. person_id (and its
verbatim-mirror contact_id) is DETERMINISTIC — uuid5 over an app-scoped namespace + the normalized
LinkedIn key — so a re-run is
a pure no-op once converged (the persons are now in the spine by LinkedIn → anti-join drops them,
and when_not_matched would skip the same deterministic id anyway). Double-safe.

STUB SHAPE (deliberate, reversible). title_enrichment carries NO company or name, so inserted
rows populate ONLY person_id + contact_id (identical) + person_linkedin_url + title (raw_job_title);
company_id / normalized_domain /
full_name / first_name / last_name — and every drifted work-email column — are NULL.
source_platform='title_enrichment' tags the cohort so it is identifiable and removable in one
predicate. These are identity stubs pending firmographic/name enrichment from a richer source.

SCHEMA-DRIVEN WRITE. The new rows are assembled against the LIVE people dataset schema (read at
runtime), not a hardcoded column list — active/people carries the work-email-enrichment drift
columns (work_email / work_email_norm / verification_status / mv_resultcode) on top of the original
9. Building from ds.schema means the insert matches whatever the spine currently is. Indexes are
rebuilt from cpb.INDEXES['people'] — the reconciled source of truth (3 BTREE + verification_status
BITMAP as of #693) — so the new fragment is fully covered and the plan never forks from cpb's.

    doppler run -p core-x -c prd -- python3 pipelines/gtm/backfill_people_from_title_enrichment.py
"""
from __future__ import annotations

import importlib.util
import os
import uuid

import duckdb
import lance
import pyarrow as pa

# Reuse the canonical people SoR constants + writer params (single source of truth).
_spec = importlib.util.spec_from_file_location(
    "cpb", os.path.join(os.path.dirname(__file__), "companies_people_bulk.py"))
_cpb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cpb)

PEOPLE_URI = _cpb.DATASET_URI["people"]
TITLE_URI = os.environ.get("TITLE_ENRICHMENT_URI", "s3://data-sink/active/title_enrichment/")
SOURCE_PLATFORM = "title_enrichment"

# Deterministic, app-scoped namespace → uuid5 contact_id per normalized LinkedIn key (idempotent).
_NS = uuid.uuid5(uuid.NAMESPACE_DNS, "title-enrichment.people.core-x")

# EXACTLY the normalization that builds active/title_enrichment's PK and that the title master
# already applied: lower → strip scheme → strip leading www. → strip trailing slash → NULL if
# emptied. Applied to active/people.person_linkedin_url (stored verbatim) for the anti-join.
_PLI_NORM = (
    r"nullif(rtrim(regexp_replace(regexp_replace(lower(trim(person_linkedin_url)), "
    r"'^https?://', ''), '^www\.', ''), '/'), '')"
)

# Rebuild plan = cpb.INDEXES['people'], the reconciled single source of truth (3 BTREE +
# verification_status BITMAP as of #693). Consume it directly — no private copy to drift — so a
# future index added to cpb's plan is rebuilt here too, keeping new fragments fully covered.
REINDEX: dict[str, list[str]] = _cpb.INDEXES["people"]


def main() -> None:
    so = _cpb._r2_storage_options()

    people_ds = lance.dataset(PEOPLE_URI, storage_options=so)
    schema = people_ds.schema  # LIVE schema — the insert is built against this, drift and all.
    people_tbl = people_ds.to_table(columns=["person_linkedin_url"])
    title_tbl = lance.dataset(TITLE_URI, storage_options=so).to_table(
        columns=["person_linkedin_url_norm", "person_linkedin_url", "raw_job_title"])

    con = duckdb.connect(":memory:")
    con.register("people", people_tbl)
    con.register("title", title_tbl)

    # Persons in title_enrichment whose normalized LinkedIn is absent from active/people.
    missing = con.sql(f"""
        WITH people_norm AS (
            SELECT DISTINCT {_PLI_NORM} AS pli_norm
            FROM people
            WHERE {_PLI_NORM} IS NOT NULL
        )
        SELECT t.person_linkedin_url_norm, t.person_linkedin_url, t.raw_job_title
        FROM title t
        LEFT JOIN people_norm p ON p.pli_norm = t.person_linkedin_url_norm
        WHERE p.pli_norm IS NULL
    """).to_arrow_table()

    n = missing.num_rows
    print(f"active/people rows                : {people_ds.count_rows():,}")
    print(f"active/title_enrichment persons   : {title_tbl.num_rows:,}")
    print(f"missing from spine (to insert)    : {n:,}")
    if n == 0:
        print("nothing to insert — already converged. No write.")
        return

    norm = missing.column("person_linkedin_url_norm").to_pylist()
    person_ids = [str(uuid.uuid5(_NS, k)) for k in norm]
    known = {
        # EXPAND/CONTRACT: person_id is the go-forward person key; contact_id mirrors it verbatim
        # (same deterministic uuid5) so both carry an identical value on every inserted row.
        "person_id": person_ids,
        "contact_id": person_ids,
        "title": missing.column("raw_job_title").to_pylist(),
        "person_linkedin_url": missing.column("person_linkedin_url").to_pylist(),
        "source_platform": [SOURCE_PLATFORM] * n,
    }
    # Assemble in the EXACT live schema (field order + type); unknown/stub columns → typed NULLs.
    new_rows = pa.table(
        [pa.array(known.get(f.name, [None] * n), type=f.type) for f in schema],
        schema=schema,
    )

    # Insert-only; existing rows are never touched. Deterministic person_id ⇒ idempotent re-run.
    people_ds.merge_insert("person_id").when_not_matched_insert_all().execute(new_rows)

    after = lance.dataset(PEOPLE_URI, storage_options=so)
    print(f"inserted {n:,} → active/people now {after.count_rows():,} rows "
          f"(source_platform='{SOURCE_PLATFORM}')")
    for index_type, cols in REINDEX.items():
        for col in cols:
            try:
                after.create_scalar_index(col, index_type=index_type)
                print(f"  {index_type:<6} ✓ {col}")
            except Exception as exc:  # noqa: BLE001 — an index miss must not fail a good insert
                print(f"  {index_type:<6} ✗ {col}: {exc}")

    # VERIFY: every title person now resolves into the spine (expect 0 still-missing).
    chk = duckdb.connect(":memory:")
    chk.register("people", after.to_table(columns=["person_linkedin_url"]))
    chk.register("title", title_tbl)
    still = chk.sql(f"""
        WITH pn AS (SELECT DISTINCT {_PLI_NORM} AS k FROM people WHERE {_PLI_NORM} IS NOT NULL)
        SELECT count(*) FILTER (WHERE pn.k IS NULL)
        FROM title t LEFT JOIN pn ON pn.k = t.person_linkedin_url_norm
    """).fetchone()[0]
    print(f"VERIFY title persons still missing from spine: {still:,} (expect 0)")
    if still != 0:
        raise RuntimeError(f"convergence FAILED: {still} title persons still absent after insert")


if __name__ == "__main__":
    main()
