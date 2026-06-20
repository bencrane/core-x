"""Cohort builder — SBA DSBS certified firms with a domain but NO PDL match → Workflow A enrollment.

SIBLING of ``cohort_sba_dsbs_certified.py``. That builder resolved certified firms all the way to a PDL
company LinkedIn URL and fed the matched firms to Atomic **Workflow B**. This builder picks up the firms
that left behind: those that HAVE a non-generic normalized domain (from website / SAM / email suffix) but
did **not** resolve to a non-generic PDL company, so PDL gives them no company LinkedIn URL.

It emits ONE representative ``normalized_domain`` per gap firm (preference website → email → SAM) and drops
a transport Parquet for the EXISTING ``enrichment-blitz-resolve-domains`` task (Atomic **Workflow A**,
Directive 23) — the 1-HOP ``domain → company_linkedin_url`` resolver. Workflow A only. It does NOT cascade
into firmographics (that is Workflow B / C); enrolling here just back-fills the missing LinkedIn URLs.

This module ONLY assembles + lands the cohort; it spends no Blitz credits.

GAP DEFINITION
    SUPPLY   sba_dsbs_certified_firms   every certified UEI
    DOMAIN   per UEI, non-generic, from website ∪ sam_master_domains ∪ email suffix
    PDL      pdl_normalized_companies   normalized_domain (BTREE), NOT is_generic_domain
    GAP      firms whose domain(s) did NOT resolve to any non-generic PDL company
    COHORT   DISTINCT representative normalized_domain →
               s3://data-sink/cohorts/enrichment_blitz/sba_dsbs_certified_firms_gap_domains.parquet

THE CYCLE lives in the Workflow A plane: it JIT-skips domains already carrying a
``companies.company_linkedin_url`` and recent ``ops.task_runs`` misses, so re-running only re-hits stale or
newly-registered domains. Enrollment is MANUAL; size surfaced via ``preview`` before spend.

    modal deploy pipelines/enrichment_blitz/cohort_sba_dsbs_certified_gap_domains.py
    modal run    pipelines/enrichment_blitz/cohort_sba_dsbs_certified_gap_domains.py::preview
    modal run    pipelines/enrichment_blitz/cohort_sba_dsbs_certified_gap_domains.py::build
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from typing import Any

import modal

# ── Coordinates ───────────────────────────────────────────────────────────────
_ACTIVE = "s3://data-sink/active"
DSBS_URI = os.environ.get("SBA_DSBS_FIRMS_URI", f"{_ACTIVE}/sba_dsbs_certified_firms/")
SMD_URI = os.environ.get("SAM_MASTER_DOMAINS_URI", f"{_ACTIVE}/sam_master_domains/")
PNC_URI = os.environ.get("PDL_NORMALIZED_LANCE_URI", f"{_ACTIVE}/pdl_normalized_companies/")

SINK_BUCKET = "data-sink"
COHORT_NAME = "sba_dsbs_certified_firms_gap_domains"
COHORT_KEY = os.environ.get(
    "DSBS_GAP_COHORT_R2_KEY", f"cohorts/enrichment_blitz/{COHORT_NAME}.parquet")
COHORT_COLUMN = "normalized_domain"
FEED = "enrichment_cohort_sba_dsbs_gap"

LOOKUP_BATCH = 2000

_GENERIC_DOMAINS = (
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com", "msn.com",
    "live.com", "protonmail.com", "proton.me", "ymail.com", "gmx.com", "mail.com", "me.com",
    "facebook.com", "web.facebook.com", "m.facebook.com", "fb.com", "instagram.com", "twitter.com",
    "x.com", "youtube.com", "youtu.be", "linkedin.com", "linktr.ee", "medium.com", "wordpress.com",
    "blogspot.com", "sites.google.com", "g.page", "behance.net", "wa.me", "t.me", "calendly.com",
    "indiamart.com", "yelp.com", "etsy.com", "amazon.com", "ebay.com", "tiktok.com", "pinterest.com",
    "github.io", "wixsite.com", "weebly.com", "godaddysites.com",
)


def _bare_host(expr: str) -> str:
    return (
        "trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        "lower(trim(CAST(" + expr + " AS VARCHAR))),"
        " '^https?://', ''), '^[^/@]*@', ''),"
        " '^www\\.', ''), '[/:?#].*$', ''), '.')"
    )


def _normalized_domain(host_expr: str) -> str:
    h = host_expr
    return (
        "nullif(CASE WHEN " + h + " LIKE '%.%' "
        "AND length(" + h + ") BETWEEN 4 AND 253 "
        "AND " + h + " NOT LIKE '% %' "
        "AND regexp_matches(" + h + ", '\\.[a-z]{2,}$') "
        "THEN " + h + " END, '')"
    )


def _generic_in_list() -> str:
    return "(" + ",".join("'" + s.replace("'", "''") + "'" for s in _GENERIC_DOMAINS) + ")"


image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
    "boto3>=1.34", "psycopg[binary]>=3.2", "requests>=2.32",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("enrichment-blitz-cohort-sba-dsbs-gap", image=image)

SECRETS = [
    modal.Secret.from_name("r2-credentials"),
    modal.Secret.from_name("hqx-postgres"),
]

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.enrichment_cohort_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,
    cohort_name         text        NOT NULL,
    firms_total         bigint      NOT NULL DEFAULT 0,
    firms_with_domain   bigint      NOT NULL DEFAULT 0,
    firms_pdl_matched   bigint      NOT NULL DEFAULT 0,
    firms_with_linkedin bigint      NOT NULL DEFAULT 0,
    distinct_urls       bigint      NOT NULL DEFAULT 0,
    r2_key              text,
    column_name         text,
    status              text        NOT NULL,
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT enrichment_cohort_runs_status_chk CHECK (status IN ('success', 'error'))
);
CREATE INDEX IF NOT EXISTS enrichment_cohort_runs_feed_idx        ON ops.enrichment_cohort_runs (feed);
CREATE INDEX IF NOT EXISTS enrichment_cohort_runs_cohort_idx      ON ops.enrichment_cohort_runs (cohort_name);
CREATE INDEX IF NOT EXISTS enrichment_cohort_runs_recorded_at_idx ON ops.enrichment_cohort_runs (recorded_at DESC);
"""


def _r2_endpoint() -> str:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the r2-credentials secret.")
    return endpoint


def _r2_storage_options() -> dict[str, str]:
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": _r2_endpoint(),
        "region": "auto",
    }


def _hqx_dsn() -> str:
    dsn = os.environ.get("HQX_DB_URL_TRANSACTION")
    if not dsn:
        pooled = os.environ.get("HQX_DB_URL_POOLED")
        if not pooled:
            raise RuntimeError("Neither HQX_DB_URL_TRANSACTION nor HQX_DB_URL_POOLED set.")
        dsn = pooled.replace(".pooler.supabase.com:5432", ".pooler.supabase.com:6543")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _open_conn(dsn: str):
    import psycopg

    return psycopg.connect(dsn, autocommit=True, prepare_threshold=None)


def _sql_lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _lance_lookup(uri: str, key_col: str, keys: list[str], cols: list[str], so: dict) -> list[dict]:
    import lance

    keys = [k for k in keys if k]
    if not keys:
        return []
    ds = lance.dataset(uri, storage_options=so)
    out: list[dict] = []
    for i in range(0, len(keys), LOOKUP_BATCH):
        chunk = keys[i:i + LOOKUP_BATCH]
        in_list = ", ".join(_sql_lit(k) for k in chunk)
        out.extend(
            ds.scanner(filter=f"{key_col} IN ({in_list})", columns=cols).to_table().to_pylist())
    return out


# ── Cohort assembly ───────────────────────────────────────────────────────────
def _assemble(so: dict) -> dict[str, Any]:
    """Certified firms with a non-generic domain that resolves to NO non-generic PDL company →
    ONE representative domain each (website > email > sam). Returns
    {domains, firms_total, firms_with_domain, firms_pdl_matched, firms_gap}."""
    import duckdb
    import lance

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='6GB'")
    con.register("dsbs", lance.dataset(DSBS_URI, storage_options=so))
    con.register("smd", lance.dataset(SMD_URI, storage_options=so))

    nd_web = _normalized_domain(_bare_host("website"))
    nd_eml = _normalized_domain(_bare_host("regexp_extract(email, '@(.+)$', 1)"))
    gen = _generic_in_list()

    firms_total = con.execute("SELECT count(*) FROM dsbs").fetchone()[0]
    # src priority: website=0, email=1, sam=2 (for representative pick).
    rows = con.execute(f"""
        WITH cand AS (
            SELECT uei, {nd_web} AS nd, 0 AS pri FROM dsbs WHERE website IS NOT NULL
            UNION ALL SELECT uei, {nd_eml}, 1 FROM dsbs WHERE email IS NOT NULL
            UNION ALL SELECT uei, normalized_domain, 2 FROM smd WHERE uei IN (SELECT uei FROM dsbs)
        )
        SELECT DISTINCT uei, nd, pri FROM cand
        WHERE nd IS NOT NULL AND nd NOT IN {gen}
    """).fetchall()
    con.close()

    firm_cands: dict[str, list[tuple[int, str]]] = {}
    for uei, nd, pri in rows:
        firm_cands.setdefault(uei, []).append((pri, nd))
    firms_with_domain = len(firm_cands)
    domains = sorted({nd for cands in firm_cands.values() for _, nd in cands})

    # PDL — which domains resolve to a non-generic PDL company.
    pdl_hit: set[str] = set()
    for r in _lance_lookup(PNC_URI, "normalized_domain", domains,
                           ["normalized_domain", "pdl_company_id", "is_generic_domain"], so):
        if r.get("is_generic_domain") or not r.get("pdl_company_id"):
            continue
        dn = r.get("normalized_domain")
        if dn:
            pdl_hit.add(dn)

    firms_pdl_matched = sum(1 for cands in firm_cands.values() if any(nd in pdl_hit for _, nd in cands))

    # GAP firms: none of their candidate domains hit PDL → pick one representative (lowest pri, then a→z).
    gap_domains: set[str] = set()
    firms_gap = 0
    for cands in firm_cands.values():
        if any(nd in pdl_hit for _, nd in cands):
            continue
        firms_gap += 1
        rep = min(cands, key=lambda pn: (pn[0], pn[1]))[1]
        gap_domains.add(rep)

    return {
        "domains": sorted(gap_domains),
        "firms_total": firms_total,
        "firms_with_domain": firms_with_domain,
        "firms_pdl_matched": firms_pdl_matched,
        "firms_gap": firms_gap,
    }


def _publish_chunks(domains: list[str], n_chunks: int, so: dict) -> list[str]:
    """Split the gap domains into ``n_chunks`` size-balanced Parquets under
    cohorts/enrichment_blitz/<COHORT_NAME>/chunk_NN.parquet — one transport file per Workflow A
    enrollment. Chunking keeps each enrollment under the task's 1h-waitpoint × 3-attempt ceiling
    at LOW-priority gateway throughput (a 24k single-shot cohort cannot finish in that window)."""
    import math

    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq
    from botocore.config import Config

    s3 = boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"], aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=Config(retries={"max_attempts": 5, "mode": "standard"}))
    size = math.ceil(len(domains) / n_chunks)
    keys: list[str] = []
    for i in range(n_chunks):
        part = domains[i * size:(i + 1) * size]
        if not part:
            break
        local = f"/tmp/{COHORT_NAME}_chunk_{i:02d}.parquet"
        pq.write_table(pa.table({COHORT_COLUMN: pa.array(part, type=pa.string())}), local)
        key = f"cohorts/enrichment_blitz/{COHORT_NAME}/chunk_{i:02d}.parquet"
        s3.upload_file(local, SINK_BUCKET, key)
        keys.append(key)
    return keys


def _publish_parquet(domains: list[str], so: dict) -> str:
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq
    from botocore.config import Config

    local = f"/tmp/{COHORT_NAME}.parquet"
    pq.write_table(pa.table({COHORT_COLUMN: pa.array(domains, type=pa.string())}), local)
    s3 = boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"], aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=Config(retries={"max_attempts": 5, "mode": "standard"}))
    s3.upload_file(local, SINK_BUCKET, COHORT_KEY)
    return COHORT_KEY


def _record_run(stats: dict, r2_key: str | None, status: str, error: str | None,
                started_at: dt.datetime, completed_at: dt.datetime) -> bool:
    try:
        conn = _open_conn(_hqx_dsn())
        try:
            cur = conn.cursor()
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.enrichment_cohort_runs
                    (feed, cohort_name, firms_total, firms_with_domain, firms_pdl_matched,
                     firms_with_linkedin, distinct_urls, r2_key, column_name, status, error,
                     started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                # firms_with_linkedin = 0 by construction: gap firms have NO PDL company → no LinkedIn.
                # (Was the firms_gap latent bug — firms_gap was written into the firms_with_linkedin
                # column; mirrors cohort_equipment_rental_cascade.py.)
                (FEED, COHORT_NAME, stats.get("firms_total", 0), stats.get("firms_with_domain", 0),
                 stats.get("firms_pdl_matched", 0), 0,
                 len(stats.get("domains", [])), r2_key, COHORT_COLUMN if r2_key else None,
                 status, error, started_at, completed_at),
            )
        finally:
            conn.close()
        return True
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build; surface LOUDLY (stderr)
        import sys
        print(f"ERROR: ops.enrichment_cohort_runs write FAILED (cohort published to R2 but "
              f"UNJOURNALED): {exc}", file=sys.stderr)
        return False


def _post_callback(url: str | None, payload: dict, attempts: int = 3) -> None:
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import time

    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}")
                return
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


def _run(write: bool, run_id: str | None, trigger_callback_url: str | None) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    run_root = run_id or uuid.uuid4().hex
    so = _r2_storage_options()
    status, error, r2_key = "error", None, None
    stats: dict[str, Any] = {"domains": []}
    try:
        stats = _assemble(so)
        domains = stats["domains"]
        print(f"[{run_root}] firms_total={stats['firms_total']} with_domain={stats['firms_with_domain']} "
              f"pdl_matched={stats['firms_pdl_matched']} gap={stats['firms_gap']} "
              f"distinct_gap_domains={len(domains)}")
        if write and domains:
            r2_key = _publish_parquet(domains, so)
            print(f"[{run_root}] cohort → s3://{SINK_BUCKET}/{r2_key} ({len(domains)} domains)")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[{run_root}] FAILED: {error}")
    finally:
        if write:
            ledger_written = _record_run(stats, r2_key, status, error, started_at,
                                         dt.datetime.now(dt.timezone.utc))
            _post_callback(trigger_callback_url, {
                "status": status, "feed": FEED, "cohort_name": COHORT_NAME,
                "ledger_written": ledger_written,
                "r2_key": r2_key, "column": COHORT_COLUMN if r2_key else None,
                "distinct_domains": len(stats.get("domains", [])),
                "firms_total": stats.get("firms_total", 0),
                "firms_with_domain": stats.get("firms_with_domain", 0),
                "firms_pdl_matched": stats.get("firms_pdl_matched", 0),
                "firms_gap": stats.get("firms_gap", 0),
                "error": error,
            })
    if status != "success":
        raise RuntimeError(f"cohort_sba_dsbs_certified_gap_domains build failed: {error}")
    return {
        "status": status, "feed": FEED, "cohort_name": COHORT_NAME, "r2_key": r2_key,
        "column": COHORT_COLUMN if r2_key else None, "distinct_domains": len(stats.get("domains", [])),
        "firms_total": stats.get("firms_total", 0),
        "firms_with_domain": stats.get("firms_with_domain", 0),
        "firms_pdl_matched": stats.get("firms_pdl_matched", 0),
        "firms_gap": stats.get("firms_gap", 0),
    }


# ── Modal surfaces ────────────────────────────────────────────────────────────
@app.function(secrets=SECRETS, timeout=60 * 30, memory=8192, cpu=2.0)
def build_cohort(run_id: str | None = None, trigger_callback_url: str | None = None) -> dict:
    return _run(write=True, run_id=run_id, trigger_callback_url=trigger_callback_url)


@app.function(secrets=SECRETS, timeout=60 * 30, memory=8192, cpu=2.0)
def preview_cohort() -> dict:
    return _run(write=False, run_id=None, trigger_callback_url=None)


@app.local_entrypoint()
def preview() -> None:
    import json

    print(json.dumps(preview_cohort.remote(), indent=2, default=str))


@app.local_entrypoint()
def build() -> None:
    import json

    print(json.dumps(build_cohort.remote(), indent=2, default=str))
