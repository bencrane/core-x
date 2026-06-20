"""Cohort builder — Equipment-Rental firms → company LinkedIn URLs (from PDL) → Workflow B enrollment.

Assembles the DISTINCT company-LinkedIn-URL cohort for the active US equipment-rental supply base and
drops it as a transport Parquet under the data-sink bucket for the EXISTING
``enrichment-blitz-enrich-linkedin`` (Atomic Workflow B, Directive 23) firmographic enrichment cycle.
This module ONLY assembles + lands the cohort; it spends no Blitz credits and adds no enrichment logic.

RESOLUTION CHAIN (each hop a fully-indexed point-lookup; mirrors the match-table supply definition)
    SUPPLY   sam_master_entities        equip-rental NAICS {532412,532490,532310,532120} · is_active · USA
                                        (primary_naics OR a secondary naics_codes entry in the bundle)
    DOMAIN   sam_master_domains         canonical blocklist-filtered normalized entity_url → 1 domain/uei
    PDL #1   pdl_normalized_companies   normalized_domain (BTREE) → pdl_company_id   (NOT is_generic_domain)
    PDL #2   pdl_companies              pdl_company_id (BTREE) → linkedin_url        (the literal PDL URL, 100% pop.)
    COHORT   DISTINCT linkedin_url  →   s3://data-sink/cohorts/enrichment_blitz/equipment_rental_firms_linkedin.parquet

The supply definition is the canonical equip-rental firm (no centroid/geo gate — firmographics are
company-level, not yard-level — so HQ-pin national chains are INCLUDED, unlike the geo match). The
LinkedIn URL is the ACTUAL string PDL stored (base ``pdl_companies.linkedin_url``), never a slug
reconstruction, so the cohort is faithfully "firms that have a company LinkedIn URL from PDL".

THE CYCLE lives in the Workflow B plane, not here: ``firmo_ttl_days`` JIT-skips firms already fresh in
``firmographics_blitz`` / ``ops.task_runs``, so re-running this enrollment only re-hits stale or newly
registered firms. Enrollment is MANUAL (no cron) so Blitz credit consumption is observed on the first
runs — mirrors ``src/trigger/exa_websets.ts`` / ``src/trigger/sba_foia_ingest.ts``. Flip the
``enroll-equipment-rental-firms-firmo`` Trigger task to ``schedules.task`` once consumption is observed.

    modal deploy pipelines/enrichment_blitz/cohort_equipment_rental.py
    modal run    pipelines/enrichment_blitz/cohort_equipment_rental.py::init_ops   # create ops.enrichment_cohort_runs
    modal run    pipelines/enrichment_blitz/cohort_equipment_rental.py::preview    # size only — no write, no spend
    modal run    pipelines/enrichment_blitz/cohort_equipment_rental.py::build      # write the cohort Parquet to R2
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from typing import Any

import modal

# ── Coordinates ───────────────────────────────────────────────────────────────
_ACTIVE = "s3://data-sink/active"
SME_URI = os.environ.get("SAM_MASTER_ENTITIES_URI", f"{_ACTIVE}/sam_master_entities/")
SMD_URI = os.environ.get("SAM_MASTER_DOMAINS_URI", f"{_ACTIVE}/sam_master_domains/")
PNC_URI = os.environ.get("PDL_NORMALIZED_LANCE_URI", f"{_ACTIVE}/pdl_normalized_companies/")
PCO_URI = os.environ.get("PDL_COMPANIES_LANCE_URI", f"{_ACTIVE}/pdl_companies/")

SINK_BUCKET = "data-sink"
COHORT_NAME = "equipment_rental_firms_linkedin"
COHORT_KEY = os.environ.get(
    "RENTAL_COHORT_R2_KEY", f"cohorts/enrichment_blitz/{COHORT_NAME}.parquet")
COHORT_COLUMN = "company_linkedin_url"
FEED = "enrichment_cohort_rental"

# Equip-rental NAICS bundle — verbatim mirror of the match-table supply side
# (pipelines/serving/materialize_equipment_rental_construction_match.py BUNDLE).
RENTAL_NAICS = ("532412", "532490", "532310", "532120")

# Lance IN-list pushdown batch size (the BTREE point-lookup path; one IN per batch).
LOOKUP_BATCH = 2000

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",          # provides `import lance`
    "pyarrow>=17",
    "boto3>=1.34",         # R2 parquet publish (uniform-part upload, R2-multipart-safe)
    "psycopg[binary]>=3.2",  # ops.enrichment_cohort_runs
    "requests>=2.32",      # Trigger callback
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("enrichment-blitz-cohort-rental", image=image)

SECRETS = [
    modal.Secret.from_name("r2-credentials"),  # Lance reads + cohort Parquet publish
    modal.Secret.from_name("hqx-postgres"),    # ops.enrichment_cohort_runs
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


# ── R2 / Postgres plumbing (fleet-standard; mirrors enrich_blitz.py) ──────────
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
            raise RuntimeError(
                "Neither HQX_DB_URL_TRANSACTION nor HQX_DB_URL_POOLED set in the hqx-postgres secret.")
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
    """Resolve active equip-rental firms → DISTINCT PDL company LinkedIn URLs.

    Returns {urls, firms_total, firms_with_domain, firms_pdl_matched, firms_with_linkedin}."""
    import duckdb
    import lance

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='4GB'")
    con.register("sme", lance.dataset(SME_URI, storage_options=so))
    con.register("smd", lance.dataset(SMD_URI, storage_options=so))
    inb = "(" + ",".join(f"'{c}'" for c in RENTAL_NAICS) + ")"

    # SUPPLY ∩ DOMAIN — active US equip-rental firms with a canonical normalized domain.
    # One domain/uei (sam_master_domains is the blocklist-filtered canonical index).
    rows = con.execute(f"""
        WITH rental AS (
            SELECT DISTINCT uei
            FROM sme
            WHERE is_active AND physical_address_country_code = 'USA'
              AND (primary_naics IN {inb}
                   OR len(list_filter(coalesce(naics_codes, []), x -> x IN {inb})) > 0)
        ),
        dom AS (
            SELECT uei, min(normalized_domain) AS normalized_domain
            FROM smd
            WHERE nullif(trim(normalized_domain), '') IS NOT NULL
            GROUP BY uei
        )
        SELECT r.uei, d.normalized_domain
        FROM rental r LEFT JOIN dom d ON r.uei = d.uei
    """).fetchall()
    con.close()

    firms_total = len(rows)
    firm_domain = {uei: dn for uei, dn in rows if dn}
    firms_with_domain = len(firm_domain)
    domains = sorted(set(firm_domain.values()))

    # PDL #1 — normalized_domain (BTREE) → pdl_company_id, dropping generic domains.
    # One pdl_company_id per domain (deterministic: min id) so a firm enrolls one URL.
    dom_to_pid: dict[str, str] = {}
    for r in _lance_lookup(PNC_URI, "normalized_domain", domains,
                           ["normalized_domain", "pdl_company_id", "is_generic_domain"], so):
        if r.get("is_generic_domain"):
            continue
        dn, pid = r.get("normalized_domain"), r.get("pdl_company_id")
        if not dn or not pid:
            continue
        cur = dom_to_pid.get(dn)
        if cur is None or pid < cur:
            dom_to_pid[dn] = pid

    # PDL #2 — pdl_company_id (BTREE) → linkedin_url (the literal PDL company URL).
    pid_to_url: dict[str, str] = {}
    for r in _lance_lookup(PCO_URI, "pdl_company_id", sorted(set(dom_to_pid.values())),
                           ["pdl_company_id", "linkedin_url"], so):
        pid, url = r.get("pdl_company_id"), r.get("linkedin_url")
        if pid and url and str(url).strip():
            pid_to_url[pid] = str(url).strip()

    firms_pdl_matched = sum(1 for dn in firm_domain.values() if dn in dom_to_pid)
    urls: set[str] = set()
    firms_with_linkedin = 0
    for dn in firm_domain.values():
        pid = dom_to_pid.get(dn)
        url = pid_to_url.get(pid) if pid else None
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
    """Write the single-column cohort Parquet to R2 via boto3 (uniform parts, R2-multipart-safe)."""
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
                started_at: dt.datetime, completed_at: dt.datetime) -> None:
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
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build outcome
        print(f"WARN: ops.enrichment_cohort_runs write failed: {exc}")


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
            _record_run(stats, r2_key, status, error, started_at, dt.datetime.now(dt.timezone.utc))
            _post_callback(trigger_callback_url, {
                "status": status, "feed": FEED, "cohort_name": COHORT_NAME,
                "r2_key": r2_key, "column": COHORT_COLUMN if r2_key else None,
                "distinct_urls": len(stats.get("urls", [])),
                "firms_total": stats.get("firms_total", 0),
                "firms_with_domain": stats.get("firms_with_domain", 0),
                "firms_pdl_matched": stats.get("firms_pdl_matched", 0),
                "firms_with_linkedin": stats.get("firms_with_linkedin", 0),
                "error": error,
            })
    if status != "success":
        raise RuntimeError(f"cohort_equipment_rental build failed: {error}")
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
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name='enrichment_cohort_runs'
            ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    print(f"ops.enrichment_cohort_runs ready — columns: {cols}")
    return {"table": "ops.enrichment_cohort_runs", "columns": cols}


@app.local_entrypoint()
def preview() -> None:
    """Size the cohort (no write, no spend)."""
    import json

    print(json.dumps(preview_cohort.remote(), indent=2, default=str))


@app.local_entrypoint()
def build() -> None:
    """Build + publish the cohort Parquet to R2."""
    import json

    print(json.dumps(build_cohort.remote(), indent=2, default=str))


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.enrichment_cohort_runs DDL (HQX)."""
    print(apply_ops_ddl.remote())
