"""Cohort builder — gtm.epd_lec_status firms → company LinkedIn URLs (from PDL) → Workflow B enrollment.

SIBLING of ``cohort_sba_dsbs_certified.py`` / ``cohort_equipment_rental.py`` (same Atomic Workflow B
contract, Directive 23): assemble the DISTINCT company-LinkedIn-URL cohort for the EPD-LEC research
universe and drop it as a transport Parquet for the EXISTING ``enrichment-blitz-enrich-linkedin``
(Workflow B: ``company_linkedin_url → firmographics``) cycle. This module ONLY assembles + lands the
cohort; it spends no Blitz credits.

Difference from the DSBS / rental builders: the supply is the EPD-LEC research landing table
``gtm.epd_lec_status`` (HQX Postgres), which is ALREADY normalized to ``domain_norm`` at the API
boundary. No three-source union — the bridge key is the row's ``domain_norm`` (verbatim mirror of
``firmographics_blitz._normalized_domain``).

    SUPPLY   gtm.epd_lec_status         DISTINCT domain_norm (the EPD-classified universe)
    PDL #1   pdl_normalized_companies   normalized_domain (BTREE) → pdl_company_id   (NOT is_generic_domain)
    PDL #2   pdl_companies              pdl_company_id (BTREE) → linkedin_url        (the literal PDL URL)
    COHORT   DISTINCT linkedin_url  →   s3://data-sink/cohorts/enrichment_blitz/epd_lec_status_firms_linkedin.parquet

THE CYCLE lives in the Workflow B plane: ``firmo_ttl_days`` JIT-skips firms already fresh in
``firmographics_blitz`` / ``ops.task_runs``, so enrolling the full matched cohort only spends credits on
firms not already enriched. Enrollment is MANUAL (no cron); size is surfaced via ``preview`` before spend.

    modal deploy pipelines/enrichment_blitz/cohort_epd_lec_status.py
    modal run    pipelines/enrichment_blitz/cohort_epd_lec_status.py::init_ops   # ops.enrichment_cohort_runs
    modal run    pipelines/enrichment_blitz/cohort_epd_lec_status.py::preview    # size only — no write, no spend
    modal run    pipelines/enrichment_blitz/cohort_epd_lec_status.py::build      # write the cohort Parquet to R2
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from typing import Any

import modal

# ── Coordinates ───────────────────────────────────────────────────────────────
_ACTIVE = "s3://data-sink/active"
PNC_URI = os.environ.get("PDL_NORMALIZED_LANCE_URI", f"{_ACTIVE}/pdl_normalized_companies/")
PCO_URI = os.environ.get("PDL_COMPANIES_LANCE_URI", f"{_ACTIVE}/pdl_companies/")

SINK_BUCKET = "data-sink"
COHORT_NAME = "epd_lec_status_firms_linkedin"
COHORT_KEY = os.environ.get(
    "EPD_LEC_COHORT_R2_KEY", f"cohorts/enrichment_blitz/{COHORT_NAME}.parquet")
COHORT_COLUMN = "company_linkedin_url"
FEED = "enrichment_cohort_epd_lec"
SOURCE_TABLE = "gtm.epd_lec_status"

LOOKUP_BATCH = 2000

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "boto3>=1.34",
    "psycopg[binary]>=3.2",
    "requests>=2.32",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("enrichment-blitz-cohort-epd-lec", image=image)

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


# ── R2 / Postgres plumbing ───────────────────────────────────────────────────
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
    """Batched Lance BTREE pushdown: rows where ``key_col IN keys`` over ``LOOKUP_BATCH`` chunks."""
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
    """Resolve EPD-LEC firms (DISTINCT domain_norm) → PDL company → DISTINCT LinkedIn URLs.

    Returns {urls, firms_total, firms_with_domain, firms_pdl_matched, firms_with_linkedin}.
    For EPD the source is already normalized; ``firms_total == firms_with_domain``.
    """
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT count(DISTINCT domain_norm) FROM hqx.{SOURCE_TABLE} "
                    "WHERE domain_norm IS NOT NULL")
        firms_total = cur.fetchone()[0]
        cur.execute(f"SELECT DISTINCT domain_norm FROM hqx.{SOURCE_TABLE} "
                    "WHERE domain_norm IS NOT NULL "
                    "ORDER BY domain_norm")
        domains = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    # The "with_domain" funnel rung is trivially the supply since EPD's bridge column IS the domain.
    firms_with_domain = len(domains)

    # PDL #1 — normalized_domain (BTREE) → pdl_company_id, dropping generic. One pid/domain (min id).
    dom_to_pid: dict[str, str] = {}
    for r in _lance_lookup(PNC_URI, "normalized_domain", domains,
                           ["normalized_domain", "pdl_company_id", "is_generic_domain"], so):
        if r.get("is_generic_domain"):
            continue
        dn, pid = r.get("normalized_domain"), r.get("pdl_company_id")
        if not dn or not pid:
            continue
        cur_val = dom_to_pid.get(dn)
        if cur_val is None or pid < cur_val:
            dom_to_pid[dn] = pid

    firms_pdl_matched = len(dom_to_pid)

    # PDL #2 — pdl_company_id (BTREE) → linkedin_url (literal PDL company URL).
    pid_to_url: dict[str, str] = {}
    for r in _lance_lookup(PCO_URI, "pdl_company_id", sorted(set(dom_to_pid.values())),
                           ["pdl_company_id", "linkedin_url"], so):
        pid, url = r.get("pdl_company_id"), r.get("linkedin_url")
        if pid and url and str(url).strip():
            pid_to_url[pid] = str(url).strip()

    urls: set[str] = set()
    firms_with_linkedin = 0
    for dn, pid in dom_to_pid.items():
        url = pid_to_url.get(pid)
        if url:
            firms_with_linkedin += 1
            urls.add(url)

    return {
        "urls": sorted(urls),
        "firms_total": firms_total,
        "firms_with_domain": firms_with_domain,
        "firms_pdl_matched": firms_pdl_matched,
        "firms_with_linkedin": firms_with_linkedin,
    }


def _publish_parquet(urls: list[str], so: dict) -> str:
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq
    from botocore.config import Config

    local = f"/tmp/{COHORT_NAME}.parquet"
    pq.write_table(pa.table({COHORT_COLUMN: pa.array(urls, type=pa.string())}), local)
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
                (FEED, COHORT_NAME, stats.get("firms_total", 0), stats.get("firms_with_domain", 0),
                 stats.get("firms_pdl_matched", 0), stats.get("firms_with_linkedin", 0),
                 len(stats.get("urls", [])), r2_key, COHORT_COLUMN if r2_key else None,
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
            print(f"Callback attempt {i + 1} non-2xx: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


def _run(write: bool, run_id: str | None, trigger_callback_url: str | None) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    run_root = run_id or uuid.uuid4().hex
    so = _r2_storage_options()
    status, error, r2_key = "error", None, None
    stats: dict[str, Any] = {"urls": []}
    try:
        stats = _assemble(so)
        urls = stats["urls"]
        print(f"[{run_root}] firms_total={stats['firms_total']} with_domain={stats['firms_with_domain']} "
              f"pdl_matched={stats['firms_pdl_matched']} with_linkedin={stats['firms_with_linkedin']} "
              f"distinct_urls={len(urls)}")
        if write and urls:
            r2_key = _publish_parquet(urls, so)
            print(f"[{run_root}] cohort → s3://{SINK_BUCKET}/{r2_key} ({len(urls)} urls)")
        elif write and not urls:
            print(f"[{run_root}] no LinkedIn URLs resolved — nothing to publish")
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
                "distinct_urls": len(stats.get("urls", [])),
                "firms_total": stats.get("firms_total", 0),
                "firms_with_domain": stats.get("firms_with_domain", 0),
                "firms_pdl_matched": stats.get("firms_pdl_matched", 0),
                "firms_with_linkedin": stats.get("firms_with_linkedin", 0),
                "error": error,
            })
    if status != "success":
        raise RuntimeError(f"cohort_epd_lec_status build failed: {error}")
    return {
        "status": status, "feed": FEED, "cohort_name": COHORT_NAME, "r2_key": r2_key,
        "column": COHORT_COLUMN if r2_key else None, "distinct_urls": len(stats.get("urls", [])),
        "firms_total": stats.get("firms_total", 0),
        "firms_with_domain": stats.get("firms_with_domain", 0),
        "firms_pdl_matched": stats.get("firms_pdl_matched", 0),
        "firms_with_linkedin": stats.get("firms_with_linkedin", 0),
    }


# ── Modal surfaces ────────────────────────────────────────────────────────────
@app.function(secrets=SECRETS, timeout=60 * 30, memory=8192, cpu=2.0)
def build_cohort(run_id: str | None = None, trigger_callback_url: str | None = None) -> dict:
    """Assemble the cohort and publish the transport Parquet to R2 (+ ops ledger + callback)."""
    return _run(write=True, run_id=run_id, trigger_callback_url=trigger_callback_url)


@app.function(secrets=SECRETS, timeout=60 * 30, memory=8192, cpu=2.0)
def preview_cohort() -> dict:
    """Size the cohort — assemble only, NO Parquet write, NO ledger row, NO Blitz spend."""
    return _run(write=False, run_id=None, trigger_callback_url=None)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"table": "ops.enrichment_cohort_runs"}


@app.local_entrypoint()
def preview() -> None:
    import json

    print(json.dumps(preview_cohort.remote(), indent=2, default=str))


@app.local_entrypoint()
def build() -> None:
    import json

    print(json.dumps(build_cohort.remote(), indent=2, default=str))


@app.local_entrypoint()
def init_ops() -> None:
    print(apply_ops_ddl.remote())
