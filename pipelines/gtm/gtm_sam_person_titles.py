"""gtm_sam_person_titles — best-available title + DM classification per person.

Thin sidecar (operator-directed 2026-07-04, "don't overdo it"): 1 row per
gtm_sam_people person that has ANY title evidence. Verbatim title carried;
the only judgment is dm_class, computed by a versioned regex.

Title precedence (first non-empty wins):
    clay matched_job_title > clay latest_experience_title > blitz headline
    > dsbs_poc_linkedin match_title > gtm_sam_people.best_title
Source rows are reached through gtm_sam_person_identity.match_source record
refs — the same join proven in the 2026-07-04 title-recovery measurement
(97% coverage on the LI queue).

dm_class:
    dm       any available title matches DM_REGEX
    not_dm   titles exist, none match (informed exclusion — engineers etc.)
    unknown  no title evidence anywhere (row still written only if the person
             has identity or best_title; fully-dark people are omitted)

FFATA officer flag + compensation intentionally NOT duplicated here — they
live on gtm_sam_people (is_exec_officer_*, max_officer_amount).

Rebuild: full snapshot, Lance overwrite. Ledger: ops.gtm_sam_person_titles_runs.

Run:
    doppler run -- python3 pipelines/gtm/gtm_sam_person_titles.py
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid

_PROD_URI = "s3://data-sink/active/gtm_sam_person_titles/"
DATASET_URI = os.environ.get("GTM_SAM_PERSON_TITLES_LANCE_URI", _PROD_URI)

SRC = {
    "gtm_sam_people": "s3://data-sink/active/gtm_sam_people/",
    "gtm_sam_person_identity": "s3://data-sink/active/gtm_sam_person_identity/",
    "clay_find_people": "s3://data-sink/active/clay_find_people/",
    "blitz_find_people": "s3://data-sink/active/blitz_find_people/",
    "dsbs_poc_linkedin": "s3://data-sink/active/dsbs_poc_linkedin/",
}

DATA_STORAGE_VERSION = "2.1"
BTREE_INDEXES = ["sam_person_id", "uei"]
BITMAP_INDEXES = ["dm_class", "title_source"]

DM_REGEX_VERSION = "v1-2026-07-04"
DM_REGEX = ("(owner|founder|ceo|chief executive|president|principal"
            "|managing (member|partner|director)|partner|coo|cfo|cto|cio"
            "|chief|vice president|vp |evp|svp)")

ROW_FLOOR = 500_000

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.gtm_sam_person_titles_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    dataset_uri   text        NOT NULL,
    build_id      text,
    rows_written  bigint,
    n_dm          bigint,
    n_not_dm      bigint,
    n_unknown     bigint,
    by_source     jsonb,
    dm_regex_ver  text,
    inputs        jsonb,
    status        text        NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS gtm_sam_person_titles_runs_recorded_at_idx
    ON ops.gtm_sam_person_titles_runs (recorded_at DESC);
"""


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record(build_id, m, lineage, status, error, started_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """INSERT INTO ops.gtm_sam_person_titles_runs
                    (feed, dataset_uri, build_id, rows_written, n_dm, n_not_dm,
                     n_unknown, by_source, dm_regex_ver, inputs, status, error,
                     started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                ("gtm_sam_person_titles", DATASET_URI, build_id,
                 m.get("rows_written"), m.get("n_dm"), m.get("n_not_dm"),
                 m.get("n_unknown"), json.dumps(m.get("by_source", {})),
                 DM_REGEX_VERSION, json.dumps(lineage), status, error,
                 started_at, dt.datetime.now(dt.timezone.utc)))
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* write failed: {exc}")
    finally:
        conn.close()


def run() -> dict:
    import duckdb
    import lance

    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    build_id = f"{started:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    lineage: list[dict] = []
    metrics: dict = {}
    status, error = "error", None

    def opends(name):
        ds = lance.dataset(SRC[name], storage_options=so)
        lineage.append({"name": name, "uri": SRC[name], "version": ds.version,
                        "rows_at_read": ds.count_rows()})
        return ds

    try:
        con = duckdb.connect(":memory:")
        con.execute("SET memory_limit='16GB'")
        p = opends("gtm_sam_people")
        con.register("p", p.scanner(
            columns=["sam_person_id", "uei", "best_title"]).to_reader())
        con.execute("CREATE TEMP TABLE ppl AS SELECT * FROM p")
        i = opends("gtm_sam_person_identity")
        con.register("i", i.scanner(
            columns=["sam_person_id", "match_source"]).to_reader())
        con.execute("""CREATE TEMP TABLE idn AS
            SELECT sam_person_id,
                   regexp_extract(match_source, 'clay:([^;]+)', 1) AS clay_rid,
                   regexp_extract(match_source, 'blitz:([^;]+)', 1) AS blitz_rid,
                   regexp_extract(match_source, 'dsbs:([^;]+)', 1) AS dsbs_ref
            FROM i""")
        c = opends("clay_find_people")
        con.register("c", c.scanner(
            columns=["record_id", "matched_job_title", "latest_experience_title"]).to_reader())
        con.execute("CREATE TEMP TABLE clay AS SELECT * FROM c")
        b = opends("blitz_find_people")
        con.register("b", b.scanner(columns=["record_id", "headline"]).to_reader())
        con.execute("CREATE TEMP TABLE blitz AS SELECT * FROM b")
        d = opends("dsbs_poc_linkedin")
        con.register("d", d.scanner(columns=["subject_id", "match_title"]).to_reader())
        con.execute("CREATE TEMP TABLE serp AS SELECT * FROM d")

        con.execute(f"""CREATE TEMP TABLE t AS
            SELECT p.sam_person_id, p.uei,
                CASE
                    WHEN nullif(trim(c.matched_job_title),'') IS NOT NULL THEN 'clay_matched'
                    WHEN nullif(trim(c.latest_experience_title),'') IS NOT NULL THEN 'clay_latest'
                    WHEN nullif(trim(b.headline),'') IS NOT NULL THEN 'blitz_headline'
                    WHEN nullif(trim(s.match_title),'') IS NOT NULL THEN 'dsbs_serper'
                    WHEN nullif(trim(p.best_title),'') IS NOT NULL THEN 'sam_dsbs_poc'
                END AS title_source,
                coalesce(nullif(trim(c.matched_job_title),''),
                         nullif(trim(c.latest_experience_title),''),
                         nullif(trim(b.headline),''),
                         nullif(trim(s.match_title),''),
                         nullif(trim(p.best_title),'')) AS title_verbatim
            FROM ppl p
            LEFT JOIN idn ON idn.sam_person_id = p.sam_person_id
            LEFT JOIN clay c ON c.record_id = idn.clay_rid
            LEFT JOIN blitz b ON b.record_id = idn.blitz_rid
            LEFT JOIN serp s ON s.subject_id = idn.dsbs_ref""")
        con.execute(f"""CREATE TEMP TABLE final AS
            SELECT sam_person_id, uei, title_verbatim, title_source,
                CASE WHEN title_verbatim IS NULL THEN 'unknown'
                     WHEN lower(title_verbatim) ~ '{DM_REGEX}' THEN 'dm'
                     ELSE 'not_dm' END AS dm_class,
                '{DM_REGEX_VERSION}' AS dm_regex_ver,
                '{build_id}' AS build_id,
                TIMESTAMP '{started:%Y-%m-%d %H:%M:%S}' AS built_at
            FROM t
            WHERE title_verbatim IS NOT NULL""")

        rows, n_dm, n_not = con.execute("""
            SELECT count(*), count(*) FILTER (WHERE dm_class='dm'),
                   count(*) FILTER (WHERE dm_class='not_dm') FROM final""").fetchone()
        by_source = dict(con.execute(
            "SELECT title_source, count(*) FROM final GROUP BY 1").fetchall())
        n_unknown = con.execute(
            "SELECT count(*) FROM t WHERE title_verbatim IS NULL").fetchone()[0]
        dup = con.execute(
            "SELECT count(*) - count(DISTINCT sam_person_id) FROM final").fetchone()[0]
        metrics = {"rows_written": int(rows), "n_dm": int(n_dm),
                   "n_not_dm": int(n_not), "n_unknown": int(n_unknown),
                   "by_source": by_source}
        print(f"build_id={build_id}")
        for e in lineage:
            print(f"  input {e['name']} v{e['version']} rows={e['rows_at_read']:,}")
        print(f"metrics: {metrics}")
        if rows < ROW_FLOOR:
            raise RuntimeError(f"gate: rows {rows:,} < floor {ROW_FLOOR:,}")
        if dup != 0:
            raise RuntimeError(f"gate: sam_person_id not unique — {dup} dups")

        table = con.sql("SELECT * FROM final").to_arrow_table()
        con.close()

        try:
            v_before = lance.dataset(DATASET_URI, storage_options=so).version
        except Exception:  # noqa: BLE001
            v_before = None
        try:
            lance.write_dataset(table, DATASET_URI, mode="overwrite",
                                data_storage_version=DATA_STORAGE_VERSION,
                                storage_options=so)
            ds = lance.dataset(DATASET_URI, storage_options=so)
            for col in BTREE_INDEXES:
                ds.create_scalar_index(col, index_type="BTREE")
            for col in BITMAP_INDEXES:
                ds.create_scalar_index(col, index_type="BITMAP")
            ds = lance.dataset(DATASET_URI, storage_options=so)
            if ds.count_rows() != rows:
                raise RuntimeError(f"write-integrity: {ds.count_rows():,} != {rows:,}")
            print(f"wrote {rows:,} → {DATASET_URI} (v{ds.version})")
        except Exception as werr:  # noqa: BLE001
            if v_before is not None:
                lance.dataset(DATASET_URI, storage_options=so, version=v_before).restore()
                raise RuntimeError(f"failed → rolled back to v{v_before}: {werr}")
            raise
        status = "success"
        return {"build_id": build_id, **metrics}
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        raise
    finally:
        _record(build_id, metrics, lineage, status, error, started)


if __name__ == "__main__":
    print(run())
