"""Backfill canonical person identity from the contact datasets (work_emails, phone_resolutions).

Closes the affiliation gap. Contacts are keyed on the legacy ``person_id``; ``people_canonical``
is keyed on ``canonical_person_id`` (deduped on normalized LinkedIn URL). The canonicalization
was a v67 snapshot, and the contact datasets have since grown (staffing work-emails, LeadMagic
phones), so ~1/3 of contact ``person_id``s had no sidecar row and could not resolve to a
canonical person — their emails/phones floated unaffiliated.

This lands every URL-bearing contact identity through :func:`land_people` (the canonical helper):
  * SIDECAR — a ``person_source_platforms`` row ``(canonical_person_id, source_platform,
    legacy_person_id, person_linkedin_url_norm)`` so the contact resolves via BOTH the LinkedIn
    URL and the legacy ``person_id``;
  * PEOPLE  — a ``people_canonical`` row for a genuinely-new human (LinkedIn URL not yet
    canonical), identity-only (name/title null), enrichable later.

URL-less contacts are EXCLUDED: with no LinkedIn URL they have no cross-source key, so a
degenerate ``sha256('pid:'||legacy)`` id would only add noise without enabling affiliation.
After this the LinkedIn-first / ``person_id``-fallback join resolves ~100% of URL-bearing contacts.

Idempotent (``merge_insert``), non-destructive (additive; prior Lance versions retained).

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with 'psycopg[binary]>=3.2' \
      python3 pipelines/gtm/backfill_people_from_contacts.py <init_ops|build|verify>
"""
from __future__ import annotations

import datetime as dt
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from pipelines.gtm._people_canonical import (  # noqa: E402
    PEOPLE_URI,
    SIDECAR_URI,
    land_people,
    normalize_linkedin,
    r2_storage_options,
)

FEED = "people_from_contacts"
WORK_EMAILS_URI = os.environ.get("WORK_EMAILS_URI", "s3://data-sink/active/work_emails/")
PHONE_URI = os.environ.get("PHONE_RESOLUTIONS_URI", "s3://data-sink/active/phone_resolutions/")
# (source Lance URI, sidecar source_platform tag). 'work_emails' already exists in the sidecar;
# 'phone_resolutions' is net-new provenance.
SOURCES = [(WORK_EMAILS_URI, "work_emails"), (PHONE_URI, "phone_resolutions")]


def log(m: str) -> None:
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _url_bearing_contacts(con, so, uri):
    """Distinct URL-bearing (person_id, person_linkedin_url) from a contact dataset, as an
    Arrow table with exactly the two columns land_people expects. URL-less rows dropped."""
    import lance
    import pyarrow as pa

    ds = lance.dataset(uri, storage_options=so)
    con.register("c_rdr", ds.scanner(columns=["person_id", "person_linkedin_url"]).to_reader())
    con.execute("""CREATE OR REPLACE TABLE c AS
      SELECT DISTINCT nullif(trim(person_id),'')            AS person_id,
                      nullif(trim(person_linkedin_url),'')  AS person_linkedin_url
      FROM c_rdr WHERE nullif(trim(person_id),'') IS NOT NULL;""")
    con.unregister("c_rdr")
    tbl = con.execute("SELECT person_id, person_linkedin_url FROM c").arrow()
    if isinstance(tbl, pa.RecordBatchReader):
        tbl = tbl.read_all()
    # Keep only rows whose LinkedIn URL normalizes non-null (the cross-source key).
    urls = tbl.column("person_linkedin_url").to_pylist()
    keep = pa.array([normalize_linkedin(u) is not None for u in urls], type=pa.bool_())
    return tbl.filter(keep)


def build():
    import lance

    started = dt.datetime.now(dt.timezone.utc)
    so = r2_storage_options()
    con = _new_con()
    source_ref = f"contact_backfill:{started.date().isoformat()}"
    people_before = lance.dataset(PEOPLE_URI, storage_options=so).count_rows()
    try:
        for uri, tag in SOURCES:
            st = "error"
            err = None
            n_in = pc = sc = 0
            try:
                tbl = _url_bearing_contacts(con, so, uri)
                n_in = tbl.num_rows
                res = land_people(tbl, source_platform=tag, source_ref=source_ref,
                                  storage_options=so)
                pc, sc = res["people_candidates"], res["sidecar_candidates"]
                st = "success"
                log(f"{tag}: fed {n_in:,} url-bearing → people_cand {pc:,}, sidecar_cand {sc:,}")
            except Exception as e:  # noqa: BLE001 — one source must not abort the ledger of the other
                err = f"{type(e).__name__}: {e}"
                log(f"{tag}: FAILED — {err}")
            finally:
                people_after = lance.dataset(PEOPLE_URI, storage_options=so).count_rows()
                _record_run(source_platform=tag, source_uri=uri, contacts=n_in,
                            people_cand=pc, sidecar_cand=sc, people_before=people_before,
                            people_after=people_after, status=st, error=err,
                            started=started, completed=dt.datetime.now(dt.timezone.utc))
                people_before = people_after
            if st != "success":
                raise RuntimeError(f"{tag} land failed: {err}")
        final = lance.dataset(PEOPLE_URI, storage_options=so).count_rows()
        log(f"people_canonical now {final:,} rows")
    finally:
        con.close()


def _new_con():
    import duckdb

    con = duckdb.connect()
    con.execute("SET threads TO 4")
    return con


def _record_run(*, source_platform, source_uri, contacts, people_cand, sidecar_cand,
                people_before, people_after, status, error, started, completed) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED") or os.environ.get("HQX_DB_URL")
    if not dsn:
        log("WARN: no HQX dsn; skipping ops.people_from_contacts_runs row")
        return
    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute(
                """INSERT INTO ops.people_from_contacts_runs
                     (feed, source_platform, source_uri, contacts_url_bearing, people_candidates,
                      sidecar_candidates, people_before, people_after, status, error,
                      started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (FEED, source_platform, source_uri, contacts, people_cand, sidecar_cand,
                 people_before, people_after, status, error, started, completed))
            c.commit()
    except Exception as e:  # noqa: BLE001 — audit must not mask the build
        log(f"WARN: ops write failed: {e}")


def init_ops():
    import psycopg
    from pathlib import Path

    sql = Path(__file__).parent.joinpath("ops_people_from_contacts_runs.sql").read_text()
    dsn = os.environ.get("HQX_DB_URL_POOLED") or os.environ["HQX_DB_URL"]
    with psycopg.connect(dsn) as c, c.cursor() as cur:
        cur.execute(sql)
        c.commit()
    log("ops.people_from_contacts_runs DDL applied")


def verify():
    """Read-back: contact person_id → canonical resolution coverage via the sidecar, after land."""
    import json

    import duckdb
    import lance

    so = r2_storage_options()
    con = duckdb.connect()
    con.execute("SET threads TO 4")

    def reg(name, uri, cols):
        d = lance.dataset(uri, storage_options=so)
        con.register(name + "_r", d.scanner(columns=cols).to_reader())
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM {name}_r")
        con.unregister(name + "_r")

    reg("sc", SIDECAR_URI, ["legacy_person_id", "canonical_person_id"])
    reg("pc", PEOPLE_URI, ["canonical_person_id"])
    out = {"people_canonical_rows": lance.dataset(PEOPLE_URI, storage_options=so).count_rows(),
           "sidecar_rows": lance.dataset(SIDECAR_URI, storage_options=so).count_rows()}
    for uri, tag in SOURCES:
        reg("t", uri, ["person_id", "person_linkedin_url"])
        tot, res = con.execute("""SELECT count(DISTINCT person_id),
            count(DISTINCT person_id) FILTER (WHERE person_id IN (SELECT legacy_person_id FROM sc))
            FROM t WHERE nullif(trim(person_id),'') IS NOT NULL""").fetchone()
        con.execute("DROP TABLE t")
        out[tag] = {"distinct_person_id": tot, "resolve_via_sidecar": res,
                    "pct": round(100 * res / max(tot, 1), 1)}
    con.close()
    print(json.dumps(out, indent=2, default=str))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        build()
    elif cmd == "verify":
        verify()
    elif cmd == "init_ops":
        init_ops()
    else:
        print(f"unknown command: {cmd} (init_ops|build|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
