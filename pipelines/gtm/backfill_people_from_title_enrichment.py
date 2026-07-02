"""Backfill canonical people with title_enrichment persons absent from the spine (idempotent).

WHY THIS EXISTS. active/title_enrichment is keyed on person_linkedin_url_norm; active/people_canonical
is the canonical person spine (one row per human, keyed on canonical_person_id = sha256(canonical
LinkedIn URL); source_platform lives in the person_source_platforms sidecar). A person who has an
enriched title but is NOT in the spine is invisible to every people-driven query. This lands those
persons as identity rows so the title↔people join reaches full coverage. 'title_enrichment' is one
more curated source alongside work_emails / clay_find_people / sfnet / csv / elfa / manual-seed;
its provenance is recorded in the sidecar, never as a people column. The Postgres SoR is untouched.

MECHANICS. Read active/title_enrichment (person key + verbatim LinkedIn + raw title) and canonical
people (LinkedIn) → anti-join on the normalized LinkedIn key → build a people-contract batch (NO
source_platform column) and route it through pipelines.gtm._people_canonical.land_people, which
derives canonical_person_id, merge_inserts identity when_not_matched into people_canonical, and
merge_inserts the (canonical_person_id, 'title_enrichment', legacy_person_id) sidecar row.

GRAIN / IDEMPOTENCY. One landed row per missing person_linkedin_url_norm. The historical
title_enrichment id (deterministic uuid5 over the normalized LinkedIn key) is preserved as the
sidecar legacy_person_id; the go-forward key is canonical_person_id. merge_insert on both datasets
makes a re-run a no-op once converged.

STUB SHAPE (deliberate, reversible). title_enrichment carries NO company or name, so landed rows
populate ONLY the LinkedIn URL + title (raw_job_title); company_id / normalized_domain / full_name
/ first_name / last_name are NULL. These are identity stubs pending firmographic/name enrichment.
land_people conforms the batch to the LIVE canonical-people schema (read at runtime) and NULL-fills
any unpopulated column, so the write matches whatever the spine currently is.

    doppler run -p core-x -c prd -- python3 pipelines/gtm/backfill_people_from_title_enrichment.py
"""
from __future__ import annotations

import os
import uuid

import duckdb
import lance
import pyarrow as pa

from pipelines.gtm import _people_canonical as pc

# REPOINT: identity → canonical people; provenance → sidecar (single source of truth).
PEOPLE_URI = pc.PEOPLE_URI
TITLE_URI = os.environ.get("TITLE_ENRICHMENT_URI", "s3://data-sink/active/title_enrichment/")
SOURCE_PLATFORM = "title_enrichment"
SOURCE_REF = "backfill:title_enrichment"

# Deterministic, app-scoped namespace → uuid5 legacy person_id per normalized LinkedIn key. This
# preserves the historical title_enrichment id as legacy_person_id in the sidecar; the go-forward
# person key is canonical_person_id = sha256(canonical_linkedin_url), derived inside land_people.
_NS = uuid.uuid5(uuid.NAMESPACE_DNS, "title-enrichment.people.core-x")

# EXACTLY the normalization that builds active/title_enrichment's PK: lower → strip scheme → strip
# leading www. → strip trailing slash → NULL if emptied. Applied to the CANONICAL people dataset's
# person_linkedin_url (stored verbatim) for the anti-join that finds title persons absent from the
# spine. (Distinct from the canonical sha256 key — this is only the presence anti-join.)
_PLI_NORM = (
    r"nullif(rtrim(regexp_replace(regexp_replace(lower(trim(person_linkedin_url)), "
    r"'^https?://', ''), '^www\.', ''), '/'), '')"
)


def main() -> None:
    so = pc.r2_storage_options()

    people_ds = lance.dataset(PEOPLE_URI, storage_options=so)
    people_tbl = people_ds.to_table(columns=["person_linkedin_url"])
    title_tbl = lance.dataset(TITLE_URI, storage_options=so).to_table(
        columns=["person_linkedin_url_norm", "person_linkedin_url", "raw_job_title"])

    con = duckdb.connect(":memory:")
    con.register("people", people_tbl)
    con.register("title", title_tbl)

    # Persons in title_enrichment whose normalized LinkedIn is absent from canonical people.
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
    print(f"canonical people rows             : {people_ds.count_rows():,}")
    print(f"active/title_enrichment persons   : {title_tbl.num_rows:,}")
    print(f"missing from spine (to land)      : {n:,}")
    if n == 0:
        print("nothing to land — already converged. No write.")
        return

    # Legacy per-source id (preserved as sidecar legacy_person_id). Build the people contract with
    # NO source_platform column; land_people derives canonical_person_id and routes provenance.
    norm = missing.column("person_linkedin_url_norm").to_pylist()
    legacy_ids = [str(uuid.uuid5(_NS, k)) for k in norm]
    cand = pa.table(
        {
            "person_id": pa.array(legacy_ids, type=pa.string()),
            "title": missing.column("raw_job_title").cast(pa.string()),
            "person_linkedin_url": missing.column("person_linkedin_url").cast(pa.string()),
        }
    )

    res = pc.land_people(cand, SOURCE_PLATFORM, SOURCE_REF, so)

    after = lance.dataset(PEOPLE_URI, storage_options=so)
    print(f"landed {res['people_candidates']:,} identity candidates → canonical people now "
          f"{after.count_rows():,} rows; sidecar candidates {res['sidecar_candidates']:,} "
          f"(source_platform='{SOURCE_PLATFORM}', merge_insert idempotent)")

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
        raise RuntimeError(f"convergence FAILED: {still} title persons still absent after land")


if __name__ == "__main__":
    main()
