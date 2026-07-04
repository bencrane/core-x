"""gtm_sam_person_firm_emails — DSBS firm-email → person attribution rulings.

The email string's system of record stays sba_dsbs_certified_firms.email
(firm-grain, never copied as truth). THIS table is the materialized match
result — "this email belongs to this sam_person_id" — same doctrine as
gtm_sam_person_identity: matched value + tier + score, deterministic rebuild.

Grain: 1 row per (uei, email) with a UNIQUE-person ruling. Surname-ties and
true ambiguities are EXCLUDED from rows (counted in the ledger); generic
mailboxes excluded outright.

Matcher (measured 2026-07-04 over 58,961 emails):
    T1 0.95 full-name construction   fn+ln / ln+fn (+nickname expansion)
    T2 0.90 initial construction     f+ln / ln+f / fn+l
    T3 0.85 single-name exact        lp == ln (≥4) or lp == fn (≥4)
    T4 0.70-0.75 containment         ln (≥4) or fn (≥4) inside lp
Alpha-only local-part canon (john.doe ≡ john_doe ≡ johndoe); candidates are
ALL gtm_sam_people at the uei; best-score wins; normalization is
lower→strip_accents→[a-z] (order matters — uppercase-first strips everything).

Rebuild: full snapshot, Lance overwrite. Ledger:
ops.gtm_sam_person_firm_emails_runs (+ input lineage, tier/tie/ambig counts).

Run:
    doppler run -- python3 pipelines/gtm/gtm_sam_person_firm_emails.py
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid

_PROD_URI = "s3://data-sink/active/gtm_sam_person_firm_emails/"
DATASET_URI = os.environ.get("GTM_SAM_PERSON_FIRM_EMAILS_LANCE_URI", _PROD_URI)

SRC = {
    "sba_dsbs_certified_firms": "s3://data-sink/active/sba_dsbs_certified_firms/",
    "gtm_sam_people": "s3://data-sink/active/gtm_sam_people/",
}

DATA_STORAGE_VERSION = "2.1"
BTREE_INDEXES = ["sam_person_id", "uei", "email_norm"]
BITMAP_INDEXES = ["match_tier"]

ROW_FLOOR = 30_000            # measured unique rulings: 37,441
DELTA_GUARD = 0.25

GENERIC = ("'info','office','admin','contact','sales','hello','contracts','contracting',"
           "'accounting','hr','support','gov','govt','bids','proposals','bd','frontdesk',"
           "'mail','inquiries','service','team','billing','orders','help','careers'")

NICK = {"bill": "william", "billy": "william", "will": "william", "bob": "robert",
        "rob": "robert", "bobby": "robert", "mike": "michael", "jim": "james",
        "jimmy": "james", "dave": "david", "dan": "daniel", "danny": "daniel",
        "tom": "thomas", "tommy": "thomas", "chris": "christopher", "chuck": "charles",
        "charlie": "charles", "dick": "richard", "rick": "richard", "rich": "richard",
        "ted": "theodore", "tony": "anthony", "steve": "steven", "ed": "edward",
        "eddie": "edward", "fred": "frederick", "greg": "gregory", "jeff": "jeffrey",
        "joe": "joseph", "joey": "joseph", "john": "jonathan", "jon": "jonathan",
        "ken": "kenneth", "kenny": "kenneth", "larry": "lawrence", "matt": "matthew",
        "nick": "nicholas", "pat": "patrick", "pete": "peter", "ron": "ronald",
        "sam": "samuel", "andy": "andrew", "drew": "andrew", "tim": "timothy",
        "kate": "katherine", "kathy": "katherine", "katie": "katherine",
        "liz": "elizabeth", "beth": "elizabeth", "betty": "elizabeth",
        "peggy": "margaret", "maggie": "margaret", "sue": "susan", "suzy": "susan",
        "debbie": "deborah", "deb": "deborah", "jen": "jennifer", "jenny": "jennifer",
        "vicky": "victoria", "vicki": "victoria", "sandy": "sandra", "mandy": "amanda",
        "alex": "alexander", "cindy": "cynthia", "trish": "patricia", "patty": "patricia"}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.gtm_sam_person_firm_emails_runs (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed              text        NOT NULL,
    dataset_uri       text        NOT NULL,
    build_id          text,
    emails_total      bigint,
    generic_excluded  bigint,
    rows_written      bigint,
    n_t1              bigint,
    n_t2              bigint,
    n_t3              bigint,
    n_t4              bigint,
    surname_ties      bigint,
    true_ambiguous    bigint,
    unmatched         bigint,
    inputs            jsonb,
    status            text        NOT NULL,
    error             text,
    started_at        timestamptz,
    completed_at      timestamptz,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS gtm_sam_person_firm_emails_runs_recorded_at_idx
    ON ops.gtm_sam_person_firm_emails_runs (recorded_at DESC);
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


def _record(build_id, uri, m, lineage, status, error, started_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """INSERT INTO ops.gtm_sam_person_firm_emails_runs
                    (feed, dataset_uri, build_id, emails_total, generic_excluded,
                     rows_written, n_t1, n_t2, n_t3, n_t4, surname_ties,
                     true_ambiguous, unmatched, inputs, status, error,
                     started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                ("gtm_sam_person_firm_emails", uri, build_id, m.get("emails_total"),
                 m.get("generic_excluded"), m.get("rows_written"), m.get("n_t1"),
                 m.get("n_t2"), m.get("n_t3"), m.get("n_t4"), m.get("surname_ties"),
                 m.get("true_ambiguous"), m.get("unmatched"), json.dumps(lineage),
                 status, error, started_at, dt.datetime.now(dt.timezone.utc)))
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
        f = opends("sba_dsbs_certified_firms")
        con.register("f", f.scanner(columns=["uei", "email"]).to_reader())
        con.execute("""CREATE TEMP TABLE emails AS
            SELECT uei, email, lower(email) AS email_norm,
                   lower(regexp_replace(split_part(email,'@',1),'[^A-Za-z]','','g')) AS lp
            FROM f WHERE email IS NOT NULL AND email LIKE '%@%'""")
        p = opends("gtm_sam_people")
        con.register("p", p.scanner(
            columns=["sam_person_id", "uei", "first_name", "last_name"]).to_reader())
        con.execute("""CREATE TEMP TABLE ppl AS
            SELECT sam_person_id, uei,
                regexp_replace(lower(strip_accents(coalesce(first_name,''))),'[^a-z]','','g') AS fn,
                regexp_replace(lower(strip_accents(coalesce(last_name,''))),'[^a-z]','','g') AS ln
            FROM p WHERE first_name IS NOT NULL OR last_name IS NOT NULL""")
        nick_rows = ",".join(f"('{k}','{v}')" for k, v in NICK.items())
        con.execute("CREATE TEMP TABLE nick(n VARCHAR, fname VARCHAR)")
        con.execute(f"INSERT INTO nick VALUES {nick_rows}")
        con.execute("""CREATE TEMP TABLE ppl2 AS
            SELECT p.*, coalesce(k.fname, p.fn) AS fn_full
            FROM ppl p LEFT JOIN nick k ON k.n = p.fn""")

        con.execute(f"""CREATE TEMP TABLE scored AS
            SELECT e.uei, e.email, e.email_norm, e.lp, p.sam_person_id, p.ln,
                CASE
                  WHEN e.lp IN ({GENERIC}) THEN NULL
                  WHEN len(p.fn)>=2 AND len(p.ln)>=2 AND e.lp IN
                       (p.fn||p.ln, p.ln||p.fn, p.fn_full||p.ln, p.ln||p.fn_full) THEN 0.95
                  WHEN len(p.fn)>=1 AND len(p.ln)>=3 AND e.lp IN
                       (substr(p.fn,1,1)||p.ln, p.ln||substr(p.fn,1,1),
                        substr(p.fn_full,1,1)||p.ln) THEN 0.90
                  WHEN len(p.fn)>=3 AND len(p.ln)>=1 AND e.lp IN
                       (p.fn||substr(p.ln,1,1), p.fn_full||substr(p.ln,1,1)) THEN 0.90
                  WHEN len(p.ln)>=4 AND e.lp = p.ln THEN 0.85
                  WHEN len(p.fn)>=4 AND (e.lp = p.fn OR e.lp = p.fn_full) THEN 0.85
                  WHEN len(p.ln)>=4 AND contains(e.lp, p.ln) THEN 0.75
                  WHEN len(p.fn)>=4 AND (contains(e.lp, p.fn)
                       OR contains(e.lp, p.fn_full)) THEN 0.70
                END AS score
            FROM emails e JOIN ppl2 p USING (uei)""")

        con.execute("""CREATE TEMP TABLE best AS
            SELECT uei, email, email_norm, max(score) AS best_score,
                   count(DISTINCT sam_person_id) FILTER (WHERE score = mx) AS n_best,
                   count(DISTINCT ln) FILTER (WHERE score = mx) AS n_surnames,
                   min(sam_person_id) FILTER (WHERE score = mx) AS person
            FROM (SELECT *, max(score) OVER (PARTITION BY uei, email) AS mx FROM scored)
            GROUP BY 1, 2, 3""")

        con.execute(f"""CREATE TEMP TABLE final AS
            SELECT person AS sam_person_id, uei, email, email_norm,
                   CASE WHEN best_score >= 0.95 THEN 't1_full_name'
                        WHEN best_score >= 0.90 THEN 't2_initial'
                        WHEN best_score >= 0.85 THEN 't3_single_name'
                        ELSE 't4_containment' END AS match_tier,
                   best_score AS match_score,
                   '{build_id}' AS build_id,
                   TIMESTAMP '{started:%Y-%m-%d %H:%M:%S}' AS built_at
            FROM best WHERE best_score IS NOT NULL AND n_best = 1""")

        (total, gen) = con.execute(f"""
            SELECT count(*), count(*) FILTER (WHERE lp IN ({GENERIC})) FROM emails""").fetchone()
        (rows, t1, t2, t3, t4) = con.execute("""
            SELECT count(*),
                   count(*) FILTER (WHERE match_tier='t1_full_name'),
                   count(*) FILTER (WHERE match_tier='t2_initial'),
                   count(*) FILTER (WHERE match_tier='t3_single_name'),
                   count(*) FILTER (WHERE match_tier='t4_containment')
            FROM final""").fetchone()
        (ties, ambig) = con.execute("""
            SELECT count(*) FILTER (WHERE n_best > 1 AND n_surnames = 1),
                   count(*) FILTER (WHERE n_best > 1 AND n_surnames > 1)
            FROM best WHERE best_score IS NOT NULL""").fetchone()
        unmatched = total - gen - rows - ties - ambig
        metrics = {"emails_total": total, "generic_excluded": gen,
                   "rows_written": rows, "n_t1": t1, "n_t2": t2, "n_t3": t3,
                   "n_t4": t4, "surname_ties": ties, "true_ambiguous": ambig,
                   "unmatched": unmatched}
        print(f"build_id={build_id}")
        for e in lineage:
            print(f"  input {e['name']} v{e['version']} rows={e['rows_at_read']:,}")
        print(f"metrics: {metrics}")
        if rows < ROW_FLOOR:
            raise RuntimeError(f"gate: rows {rows:,} < floor {ROW_FLOOR:,}")
        dup = con.execute(
            "SELECT count(*) - count(DISTINCT uei || '|' || email_norm) FROM final"
        ).fetchone()[0]
        if dup != 0:
            raise RuntimeError(f"gate: (uei,email) not unique — {dup} dups")

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
            print(f"wrote {rows:,} rulings → {DATASET_URI} (v{ds.version})")
        except Exception as werr:  # noqa: BLE001
            if v_before is not None:
                lance.dataset(DATASET_URI, storage_options=so,
                              version=v_before).restore()
                raise RuntimeError(f"failed → rolled back to v{v_before}: {werr}")
            raise
        status = "success"
        return {"build_id": build_id, **metrics}
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        raise
    finally:
        _record(build_id, DATASET_URI, metrics, lineage, status, error, started)


if __name__ == "__main__":
    print(run())
