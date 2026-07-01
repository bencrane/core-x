"""Compute worker — resolved DSBS-POC ↔ person matches, enrichment-ready.

Part of the ``dsbs-poc-people`` Modal app. Endpoint-less; dispatcher-resolvable or run manually.
Clean-room: DuckDB + Python read/match, Lance written straight to R2. No catalog.

WHY THIS EXISTS
    dsbs_pocs holds DSBS-declared people (owners/officers/contacts). This resolves each of those to a
    REAL person-record in clay_find_people / active/people who works at that same firm (name-matched
    within uei), and attaches the enrichment key (person_linkedin_url) + current work-email / mobile
    coverage — the ready-to-enrich list of DSBS decision-makers.

MATCH. A DSBS POC (uei, name_key) resolves to a person iff a clay/active-people person works at that
    firm (their company domain ∈ crosswalk_dsbs_sam domain universe → uei) AND their name normalizes
    to the same name_key (order-independent: lowercased alpha tokens >=2, suffixes dropped, sorted).

SOURCES (Lance, R2): crosswalk_dsbs_sam (firm + domain universe), dsbs_pocs (the POCs),
    clay_find_people + people (the people), work_emails + phone_resolutions (current coverage).
TARGET : s3://data-sink/active/dsbs_poc_people/  (native Lance v2.1; overwrite)

GRAIN : 1 row per (uei × person_id). Cols: firm (uei, legal_business_name, primary_naics,
    best_domain, company_linkedin_url), DSBS POC (name_key, poc_type, dsbs_full_name, dsbs_title),
    person (person_id, person_linkedin_url [the enrichment key], person_full_name, person_title,
    match_source), coverage (has_work_email, work_email, work_email_status, has_mobile, mobile_phone),
    materialized_at.

INDEXES: BTREE uei/person_id/person_linkedin_url ; BITMAP poc_type/match_source/has_work_email/has_mobile.

    modal run    pipelines/sba_dsbs/materialize_dsbs_poc_people.py::run
    modal deploy pipelines/sba_dsbs/materialize_dsbs_poc_people.py
    # local: doppler run -p core-x -c prd -- uv run --with pylance --with duckdb --with pyarrow \
    #        --with 'psycopg[binary]' --with modal python pipelines/sba_dsbs/materialize_dsbs_poc_people.py
"""
from __future__ import annotations

import os
import re
import unicodedata

import modal

_ACTIVE = "s3://data-sink/active"
DATASET = "dsbs_poc_people"
DATASET_URI = os.environ.get("DSBS_POC_PEOPLE_URI", f"{_ACTIVE}/{DATASET}/")
XWALK_URI = f"{_ACTIVE}/crosswalk_dsbs_sam/"
POCS_URI = f"{_ACTIVE}/dsbs_pocs/"
CLAY_URI = f"{_ACTIVE}/clay_find_people/"
PEOPLE_URI = f"{_ACTIVE}/people/"
WORK_EMAILS_URI = f"{_ACTIVE}/work_emails/"
PHONE_URI = f"{_ACTIVE}/phone_resolutions/"
FEED = "dsbs_poc_people"
SOURCE_DB = "r2:dsbs_pocs⋈(clay_find_people+people)+work_emails+phone_resolutions"

INDEXES = {"BTREE": ["uei", "person_id", "person_linkedin_url"],
           "BITMAP": ["poc_type", "match_source", "has_work_email", "has_mobile"]}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.dsbs_poc_people_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, feed text NOT NULL, source_db text NOT NULL,
    datasets jsonb NOT NULL, mode text NOT NULL DEFAULT 'overwrite', rows_total bigint NOT NULL DEFAULT 0,
    status text NOT NULL, error text, started_at timestamptz, completed_at timestamptz,
    recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dsbs_poc_people_runs_feed_idx ON ops.dsbs_poc_people_runs (feed);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "pylance>=7", "pyarrow>=17", "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})
app = modal.App("dsbs-poc-people", image=image)

_SUF = {"jr", "sr", "ii", "iii", "iv", "v", "md", "phd", "cpa", "esq", "pe", "mba", "jd", "dds"}


def _name_key(n):
    if not n:
        return None
    s = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode().lower()
    t = sorted(x for x in re.split(r"[^a-z]+", s) if len(x) >= 2 and x not in _SUF)
    return " ".join(t) or None


def _r2_storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": ep, "region": "auto"}


def build_dsbs_poc_people() -> dict:
    import datetime as dt

    import duckdb
    import lance
    import pyarrow as pa

    so = _r2_storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4; SET memory_limit='12GB';")

    def reg(name, uri, cols):
        con.register(name, lance.dataset(uri, storage_options=so).scanner(columns=cols).to_reader())

    reg("xw", XWALK_URI, ["uei", "dsbs_legal_business_name", "primary_naics", "best_domain",
                          "company_linkedin_url", "matched_domain", "domain_entity_url",
                          "domain_website", "domain_email", "domain_additional"])
    reg("dp", POCS_URI, ["uei", "name_key", "poc_type", "full_name", "title"])
    reg("cp", CLAY_URI, ["person_id", "domain_norm", "full_name", "matched_job_title", "linkedin_url_raw"])
    reg("pe", PEOPLE_URI, ["person_id", "normalized_domain", "full_name", "title", "person_linkedin_url"])
    reg("we", WORK_EMAILS_URI, ["person_id", "email", "verification_status"])
    reg("ph", PHONE_URI, ["person_id", "phone", "phone_type"])
    for t in ("xw", "dp", "cp", "pe", "we", "ph"):
        con.execute(f"CREATE TABLE {t}_t AS SELECT * FROM {t}")

    con.execute("""CREATE TABLE dom_uei AS SELECT DISTINCT uei, lower(d) dom FROM (
        SELECT uei,best_domain d FROM xw_t UNION ALL SELECT uei,matched_domain FROM xw_t UNION ALL SELECT uei,domain_entity_url FROM xw_t
        UNION ALL SELECT uei,domain_website FROM xw_t UNION ALL SELECT uei,domain_email FROM xw_t UNION ALL SELECT uei,domain_additional FROM xw_t) WHERE d IS NOT NULL""")
    # DSBS POC aggregate per (uei,name_key)
    con.execute("""CREATE TABLE poc_agg AS SELECT uei, name_key,
        max(full_name) dsbs_full_name, max(title) dsbs_title,
        CASE WHEN bool_or(poc_type='current_principal') AND bool_or(poc_type='contact_person') THEN 'both'
             WHEN bool_or(poc_type='current_principal') THEN 'current_principal' ELSE 'contact_person' END poc_type
        FROM dp_t WHERE name_key IS NOT NULL GROUP BY uei, name_key""")
    poc_keys = set(con.execute("SELECT uei, name_key FROM poc_agg").fetchall())

    # people at DSBS firms → (uei, person_id, name_key, fields, source); keep matches
    clay = con.execute("SELECT dm.uei, c.person_id, c.full_name, c.matched_job_title, c.linkedin_url_raw "
                       "FROM cp_t c JOIN dom_uei dm ON dm.dom=lower(c.domain_norm) WHERE c.full_name IS NOT NULL AND c.person_id IS NOT NULL").fetchall()
    peo = con.execute("SELECT dm.uei, p.person_id, p.full_name, p.title, p.person_linkedin_url "
                      "FROM pe_t p JOIN dom_uei dm ON dm.dom=lower(p.normalized_domain) WHERE p.full_name IS NOT NULL AND p.person_id IS NOT NULL").fetchall()
    rows: dict = {}  # (uei,person_id) -> record
    for u, pid, fn, ti, li in clay:
        k = _name_key(fn)
        if (u, k) not in poc_keys:
            continue
        r = rows.setdefault((u, pid), {"uei": u, "person_id": pid, "name_key": k, "full_name": fn,
                                       "title": ti, "linkedin": li, "src": set()})
        r["src"].add("clay")
    for u, pid, fn, ti, li in peo:
        k = _name_key(fn)
        if (u, k) not in poc_keys:
            continue
        r = rows.setdefault((u, pid), {"uei": u, "person_id": pid, "name_key": k, "full_name": fn,
                                       "title": None, "linkedin": None, "src": set()})
        # prefer active/people identity when present
        r["full_name"] = fn or r["full_name"]
        r["title"] = ti or r.get("title")
        r["linkedin"] = li or r.get("linkedin")
        r["src"].add("people")

    M = {k: [] for k in ["uei", "person_id", "name_key", "person_full_name", "person_title",
                         "person_linkedin_url", "match_source"]}
    for r in rows.values():
        M["uei"].append(r["uei"]); M["person_id"].append(r["person_id"]); M["name_key"].append(r["name_key"])
        M["person_full_name"].append(r["full_name"]); M["person_title"].append(r["title"])
        M["person_linkedin_url"].append(r["linkedin"])
        s = r["src"]
        M["match_source"].append("both" if len(s) == 2 else next(iter(s)))
    con.register("matched", pa.table(M))

    # coverage aggregates
    con.execute("""CREATE TABLE we_agg AS SELECT person_id, max(email) work_email,
        max(verification_status) work_email_status FROM we_t WHERE email IS NOT NULL GROUP BY person_id""")
    con.execute("""CREATE TABLE ph_agg AS SELECT person_id, max(phone) mobile_phone FROM ph_t
        WHERE phone_type='mobile' AND phone IS NOT NULL GROUP BY person_id""")

    mat = dt.datetime.now(dt.UTC)
    out = con.execute("""
        SELECT m.uei, x.dsbs_legal_business_name AS legal_business_name, x.primary_naics,
               x.best_domain, x.company_linkedin_url,
               m.name_key, p.poc_type, p.dsbs_full_name, p.dsbs_title,
               m.person_id, m.person_linkedin_url, m.person_full_name, m.person_title, m.match_source,
               (we.person_id IS NOT NULL) AS has_work_email, we.work_email, we.work_email_status,
               (ph.person_id IS NOT NULL) AS has_mobile, ph.mobile_phone
        FROM matched m
        LEFT JOIN xw_t x   ON x.uei = m.uei
        LEFT JOIN poc_agg p ON p.uei = m.uei AND p.name_key = m.name_key
        LEFT JOIN we_agg we ON we.person_id = m.person_id
        LEFT JOIN ph_agg ph ON ph.person_id = m.person_id
    """).to_arrow_table()
    out = out.append_column("materialized_at", pa.array([mat] * out.num_rows, pa.timestamp("us", tz="UTC")))
    n = out.num_rows

    lance.write_dataset(out, DATASET_URI, mode="overwrite", data_storage_version="2.1", storage_options=so)
    ds = lance.dataset(DATASET_URI, storage_options=so)
    assert ds.count_rows() == n
    names = set(ds.schema.names)
    for typ, cols in INDEXES.items():
        for col in cols:
            if col in names:
                try:
                    ds.create_scalar_index(col, index_type=typ)
                except Exception as exc:  # noqa: BLE001
                    print(f"  idx skip {typ} {col}: {exc}", flush=True)
    con.register("o", ds.to_table(columns=["uei", "person_id", "has_work_email", "has_mobile", "match_source"]))
    firms = con.execute("SELECT count(DISTINCT uei) FROM o").fetchone()[0]
    ppl = con.execute("SELECT count(DISTINCT person_id) FROM o").fetchone()[0]
    em = con.execute("SELECT sum(has_work_email::int) FROM o").fetchone()[0]
    mo = con.execute("SELECT sum(has_mobile::int) FROM o").fetchone()[0]
    print(f"WROTE {DATASET_URI}  rows={n:,}  firms={firms:,}  distinct_people={ppl:,}  "
          f"has_work_email={em:,}  has_mobile={mo:,}", flush=True)
    return {"uri": DATASET_URI, "rows": n, "firms": firms}


def _record_run(rows_total, status, error, started, completed):
    import psycopg
    from psycopg.types.json import Jsonb
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED unset; skipping ops ledger.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute("INSERT INTO ops.dsbs_poc_people_runs (feed,source_db,datasets,mode,rows_total,"
                        "status,error,started_at,completed_at) VALUES (%s,%s,%s,'overwrite',%s,%s,%s,%s,%s)",
                        (FEED, SOURCE_DB, Jsonb({DATASET: rows_total}), rows_total, status, error, started, completed))
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops ledger write failed: {exc}")


_SECRETS = [modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")]


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=32768, cpu=4.0)
def ingest() -> dict:
    import datetime as dt
    started = dt.datetime.now(dt.timezone.utc)
    status, error, res = "error", None, {}
    try:
        res = build_dsbs_poc_people()
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        raise
    finally:
        _record_run(res.get("rows", 0), status, error, started, dt.datetime.now(dt.timezone.utc))
    return res


@app.local_entrypoint()
def run() -> None:
    import json
    print(json.dumps(ingest.remote(), indent=2, default=str))


if __name__ == "__main__":
    import datetime as dt
    started = dt.datetime.now(dt.timezone.utc)
    status, error, res = "error", None, {}
    try:
        res = build_dsbs_poc_people()
        status = "success"
    finally:
        _record_run(res.get("rows", 0), status, error, started, dt.datetime.now(dt.timezone.utc))
    print(res)
