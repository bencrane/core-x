"""Blitz Contact Hydration — Waterfall ICP, gateway-routed (Directive 20-Final).

Decoupled, **Waterfall-only** campaign-audience contact hydration. Endpoint-less,
domain-grouped Modal worker (``pipelines/gtm/``), spawned by the Universal
Dispatcher (``core/modal_dispatcher.py``) and woken via the Trigger waitpoint
callback. Kicked off by the interactive GTM agent through the gtm-mcp
``launch_contact_hydration`` tool → the ``blitz-contact-hydration-waterfall``
Trigger task → this worker.

DECOUPLED EGRESS (Directive 20-Final §2 — the crucial mandate). This worker holds
**NO Blitz key** and makes **NO direct ``api.blitz-api.ai`` calls**. EVERY Blitz
request — the domain→linkedin bridge, the Waterfall targeting call, and the
email/phone enrichments — is routed through the single-container
``core/blitz_gateway.py`` (``blitz-gateway`` app, function ``blitz_call``), which
owns the only ``BLITZAPI_API_KEY`` and the authoritative global ≤5-RPS priority
token bucket. Calls ride the **HIGH** lane so interactive GTM hydration preempts
background bulk sweeps (enrichment-blitz Workflow A). The worker implements NO
rate logic of its own — the gateway is the sole governor.

FLOW.
  1. key-info preflight via the gateway → confirm the plan unlocks the requested
     enrichments (degrade, never crash, if a hop is plan-gated).
  2. Read ``ops.campaign_audiences`` (psycopg) → the saved ``source_query``.
  3. Resolve the audience over the Lance plane via the canonical gtm-mcp resolver
     (``apps/gtm_mcp/src/database.query`` — the SAME read-only SQL guard +
     dataset registry + hq-x attach the agent authored the query with):
     ``source_query`` → DISTINCT ``normalized_domain``, LEFT-JOINed to
     ``companies`` / ``firmographics_blitz`` to recover ``company_linkedin_url``
     locally (the bridge) — no API round-trip for the hits.
  4. Per company, via the gateway (HIGH): ``[resolve if no local URL]`` →
     ``waterfall-icp-keyword(cascade)`` → per matched person ``[email]`` ``[phone(US)]``.
  5. Land raw contact rows → R2 transport Parquet (ZSTD).
  6. Terminal: ``ops.blitz_hydration_runs`` (psycopg) + RAW callback to Trigger.

SINK. R2 transport Parquet (``dex-raw-landing-zone/blitz_contacts/…``); per-run
state in ``ops.blitz_hydration_runs``. Promotion of these raw contacts into the
Lance system-of-record (a ``campaign_contacts`` dataset / ``people`` augmentation)
is a SEPARATE directive — out of scope here (this pipeline ends at "raw payloads
landed").

    modal deploy pipelines/gtm/blitz_hydration_waterfall.py
    modal run    pipelines/gtm/blitz_hydration_waterfall.py::init_ops
    modal run    pipelines/gtm/blitz_hydration_waterfall.py::verify_resolver   # safe: no Blitz spend
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import uuid
from typing import Any

import modal

# ── Gateway coordinates (Directive 23). The ONLY egress to BlitzAPI. ─────────
GATEWAY_APP = "blitz-gateway"
GATEWAY_FN = "blitz_call"
GATEWAY_KEYINFO_FN = "key_info"

# Blitz paths used by this pipeline (passed as raw paths — the gateway maps a
# logical name OR a raw "/v2/..." path; only resolve/company/key_info have logical
# aliases, so these three go by full path).
WATERFALL_PATH = "/v2/search/waterfall-icp-keyword"
EMAIL_PATH = "/v2/enrichment/email"
PHONE_PATH = "/v2/enrichment/phone"
RESOLVE_ENDPOINT = "resolve"  # gateway logical alias → /v2/enrichment/domain-to-linkedin
# allowed_apis entries from key-info are un-versioned (no /v2 prefix).
EMAIL_API = "/enrichment/email"
PHONE_API = "/enrichment/phone"

FEED = "blitz_hydration_waterfall"

# Lance bridge sources (overridable, fleet convention) — used by the resolver's
# LEFT JOINs to recover company_linkedin_url without a Blitz round-trip.
_ACTIVE = "s3://data-sink/active"
COMPANIES_URI = os.environ.get("GTM_COMPANIES_URI", f"{_ACTIVE}/companies/")
FIRMO_URI = os.environ.get("FIRMOGRAPHICS_BLITZ_URI", f"{_ACTIVE}/firmographics_blitz/")

# R2 transport landing (raw Blitz payloads). Bucket + prefix overridable.
LANDING_BUCKET = os.environ.get("BLITZ_HYDRATION_LANDING_BUCKET", "dex-raw-landing-zone")
LANDING_PREFIX = os.environ.get("BLITZ_HYDRATION_LANDING_PREFIX", "blitz_contacts").strip("/")

# Waterfall live caps (docs "Waterfall Logic": max_results ≤25, cascade ≤8 tiers).
MAX_RESULTS_CAP = 25
MAX_CASCADE_TIERS = 8
VALID_PRIORITIES = {"high", "normal", "low"}

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",          # `import lance`
    "pyarrow>=17",
    "psycopg[binary]>=3.2",
    "boto3>=1.34",         # R2 listing (audience resolver discovery) + Parquet landing put
    "requests>=2.32",      # Trigger callback
).env({"LANCE_BYPASS_SPILLING": "true"}).add_local_python_source(
    # Reuse the canonical gtm-mcp audience resolver (read-only SQL guard + dataset
    # registry + hq-x attach) so the worker resolves an audience BYTE-IDENTICALLY
    # to how the GTM agent authored/tested its source_query. (Read-only data
    # access; the one apps/→pipelines/ import in the fleet — see the spec.)
    "apps",
)

app = modal.App("blitz-hydration-waterfall", image=image)

SECRETS = [
    modal.Secret.from_name("r2-credentials"),  # Lance reads + Parquet landing put
    modal.Secret.from_name("hqx-postgres"),    # ops.campaign_audiences read + ops.blitz_hydration_runs write
]
# NOTE: the `blitz-api` secret is deliberately ABSENT — the worker never holds the
# Blitz key. All Blitz egress is the gateway's concern (Directive 20-Final §2).

# ── ops.blitz_hydration_runs DDL — verbatim mirror of the .sql sibling. Applied
# defensively before each terminal write (idempotent). ───────────────────────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.blitz_hydration_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,
    campaign_id         text        NOT NULL,
    audience_name       text,
    priority            text        NOT NULL,
    run_root            text,
    companies_in        bigint      NOT NULL DEFAULT 0,
    linkedin_local      bigint      NOT NULL DEFAULT 0,
    linkedin_api        bigint      NOT NULL DEFAULT 0,
    linkedin_unresolved bigint      NOT NULL DEFAULT 0,
    waterfall_calls     bigint      NOT NULL DEFAULT 0,
    contacts_found      bigint      NOT NULL DEFAULT 0,
    emails_found        bigint      NOT NULL DEFAULT 0,
    phones_found        bigint      NOT NULL DEFAULT 0,
    gateway_calls       bigint      NOT NULL DEFAULT 0,
    email_gated         boolean     NOT NULL DEFAULT false,
    phone_gated         boolean     NOT NULL DEFAULT false,
    landing_uri         text,
    status              text        NOT NULL,
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT blitz_hydration_runs_status_chk   CHECK (status   IN ('success', 'error')),
    CONSTRAINT blitz_hydration_runs_priority_chk CHECK (priority IN ('high', 'normal', 'low'))
);
CREATE INDEX IF NOT EXISTS blitz_hydration_runs_campaign_idx ON ops.blitz_hydration_runs (campaign_id);
CREATE INDEX IF NOT EXISTS blitz_hydration_runs_status_idx   ON ops.blitz_hydration_runs (status);
CREATE INDEX IF NOT EXISTS blitz_hydration_runs_recorded_idx ON ops.blitz_hydration_runs (recorded_at DESC);
"""

_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")
_SLUG = re.compile(r"[^A-Za-z0-9._-]+")


def _normalize_domain(raw: str | None) -> str | None:
    d = (raw or "").strip().lower()
    d = _SCHEME.sub("", d)
    d = _WWW.sub("", d)
    d = d.split("/", 1)[0].rstrip(".")
    return d or None


def _slug(value: str) -> str:
    return _SLUG.sub("-", (value or "").strip()).strip("-") or "unknown"


# ── R2 / Postgres plumbing (fleet-standard) ──────────────────────────────────
def _r2_endpoint() -> str:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the r2-credentials secret.")
    return endpoint


def _hqx_dsn() -> str:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres Modal secret.")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _r2_s3_client():
    """boto3 S3 client bound to R2 for the Parquet landing put. The when_required
    checksum config mirrors every R2 transfer worker in the fleet — botocore's
    default flexible-checksum validation otherwise raises against R2."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=_r2_endpoint(),
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def _open_conn(dsn: str):
    import psycopg

    return psycopg.connect(dsn, autocommit=True)


# ── Audience resolution (reuse the canonical gtm-mcp resolver) ────────────────
def _read_audience(cur, campaign_id: str, audience_name: str | None) -> tuple[str, str | None]:
    """Read the saved source_query from ops.campaign_audiences. When audience_name
    is omitted, take the campaign's most-recently-updated audience."""
    if audience_name:
        cur.execute(
            "SELECT source_query, audience_name FROM ops.campaign_audiences "
            "WHERE campaign_id = %s AND audience_name = %s",
            (campaign_id, audience_name),
        )
    else:
        cur.execute(
            "SELECT source_query, audience_name FROM ops.campaign_audiences "
            "WHERE campaign_id = %s ORDER BY updated_at DESC LIMIT 1",
            (campaign_id,),
        )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(
            f"no saved audience for campaign_id={campaign_id!r} "
            f"audience_name={audience_name!r} in ops.campaign_audiences"
        )
    return row[0], row[1]


def _resolve_companies(source_query: str) -> list[dict]:
    """source_query → [{normalized_domain, company_linkedin_url|None}] via the
    canonical gtm-mcp resolver (same read-only guard + registry + hq-x attach the
    agent authored the query with). The source_query MUST project a
    ``normalized_domain`` column (the anchor contract); company_linkedin_url is
    recovered locally by LEFT JOIN to companies / firmographics_blitz."""
    from apps.gtm_mcp.src import database as gtmdb  # reuse — see image add_local_python_source

    bridged = (
        "WITH __aud AS (\n" + source_query + "\n),\n"
        "__dom AS (SELECT DISTINCT normalized_domain AS nd FROM __aud "
        "WHERE normalized_domain IS NOT NULL)\n"
        "SELECT __dom.nd AS normalized_domain,\n"
        "       COALESCE(c.url, f.linkedin_url) AS company_linkedin_url\n"
        "FROM __dom\n"
        "LEFT JOIN (SELECT normalized_domain, any_value(company_linkedin_url) AS url\n"
        "           FROM companies WHERE company_linkedin_url IS NOT NULL\n"
        "           GROUP BY normalized_domain) c ON c.normalized_domain = __dom.nd\n"
        "LEFT JOIN firmographics_blitz f ON f.domain_norm = __dom.nd"
    )
    try:
        res = gtmdb.query(bridged, datasets=gtmdb.referenced_datasets(bridged), max_rows=1_000_000)
        return res["rows"]
    except Exception as exc:  # noqa: BLE001 — degrade the LOCAL bridge, never the run
        # Bridge unavailable (e.g. a bridge dataset missing) → resolve domains only;
        # the gateway domain→linkedin hop fills every URL. The contract violation
        # (no normalized_domain column) still surfaces here, loudly.
        print(f"WARN: bridged resolve failed ({exc}); falling back to domains-only.")
        domains_only = (
            "WITH __aud AS (\n" + source_query + "\n)\n"
            "SELECT DISTINCT normalized_domain FROM __aud WHERE normalized_domain IS NOT NULL"
        )
        res = gtmdb.query(
            domains_only, datasets=gtmdb.referenced_datasets(domains_only), max_rows=1_000_000
        )
        return [{"normalized_domain": r["normalized_domain"], "company_linkedin_url": None}
                for r in res["rows"]]


# ── Gateway invocation (the ONLY Blitz egress) ───────────────────────────────
def _gateway():
    return modal.Function.from_name(GATEWAY_APP, GATEWAY_FN)


def _preflight(enrich_email: bool, enrich_phone: bool) -> tuple[bool, bool, bool, bool]:
    """Confirm the live plan unlocks the requested enrichments (OPEN ITEM A). Degrade
    a plan-gated hop to off (never crash). Returns (email_on, phone_on, email_gated,
    phone_gated)."""
    try:
        ki = modal.Function.from_name(GATEWAY_APP, GATEWAY_KEYINFO_FN).remote()
    except Exception as exc:  # noqa: BLE001 — preflight is advisory, not load-bearing
        print(f"WARN: key-info preflight failed ({exc}); proceeding with requested enrichments.")
        return enrich_email, enrich_phone, False, False
    allowed = set(ki.get("allowed_apis") or [])
    email_ok = EMAIL_API in allowed
    phone_ok = PHONE_API in allowed
    email_gated = bool(enrich_email and not email_ok)
    phone_gated = bool(enrich_phone and not phone_ok)
    if email_gated:
        print(f"WARN: email enrichment requested but plan does not unlock {EMAIL_API}; disabling.")
    if phone_gated:
        print(f"WARN: phone enrichment requested but plan does not unlock {PHONE_API}; disabling.")
    return (enrich_email and email_ok), (enrich_phone and phone_ok), email_gated, phone_gated


def _waterfall_people(rb: dict) -> list[dict]:
    """Extract matched results[] (each {icp, ranking, what_matched, person}) from a
    waterfall envelope. Empty on a miss / error (the caller counts ok separately)."""
    if not rb.get("ok"):
        return []
    data = rb.get("data") or {}
    results = data.get("results") if isinstance(data, dict) else None
    return results if isinstance(results, list) else []


def _enrich_email(gw, person_url: str, priority: str) -> tuple[str | None, dict]:
    rb = gw.remote(endpoint=EMAIL_PATH, payload={"person_linkedin_url": person_url}, priority=priority)
    data = rb.get("data") or {}
    email = data.get("email") if (rb.get("ok") and isinstance(data, dict) and data.get("found")) else None
    return email, rb


def _enrich_phone(gw, person_url: str, priority: str) -> tuple[str | None, dict]:
    rb = gw.remote(endpoint=PHONE_PATH, payload={"person_linkedin_url": person_url}, priority=priority)
    data = rb.get("data") or {}
    phone = data.get("phone") if (rb.get("ok") and isinstance(data, dict) and data.get("found")) else None
    return phone, rb


# ── Contact row shape (R2 transport Parquet) ─────────────────────────────────
def _contact_schema():
    import pyarrow as pa

    return pa.schema([
        pa.field("campaign_id", pa.string(), nullable=False),
        pa.field("audience_name", pa.string(), nullable=True),
        pa.field("run_id", pa.string(), nullable=True),
        pa.field("normalized_domain", pa.string(), nullable=True),
        pa.field("company_linkedin_url", pa.string(), nullable=True),
        pa.field("icp_tier", pa.int32(), nullable=True),
        pa.field("ranking", pa.int32(), nullable=True),
        pa.field("what_matched", pa.string(), nullable=True),       # JSON
        pa.field("person_linkedin_url", pa.string(), nullable=True),
        pa.field("first_name", pa.string(), nullable=True),
        pa.field("last_name", pa.string(), nullable=True),
        pa.field("full_name", pa.string(), nullable=True),
        pa.field("headline", pa.string(), nullable=True),
        pa.field("job_title", pa.string(), nullable=True),
        pa.field("city", pa.string(), nullable=True),
        pa.field("state_code", pa.string(), nullable=True),
        pa.field("country_code", pa.string(), nullable=True),
        pa.field("continent", pa.string(), nullable=True),
        pa.field("email", pa.string(), nullable=True),
        pa.field("email_found", pa.bool_(), nullable=True),
        pa.field("phone", pa.string(), nullable=True),
        pa.field("phone_found", pa.bool_(), nullable=True),
        pa.field("raw_person", pa.string(), nullable=True),         # JSON (full fidelity)
        pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ])


def _contact_row(campaign_id, audience_name, run_id, dom, company_url, result, fetched_at):
    person = (result.get("person") or {}) if isinstance(result, dict) else {}
    loc = person.get("location") or {}
    exps = person.get("experiences") or []
    job_title = (exps[0].get("job_title") if exps and isinstance(exps[0], dict) else None)
    return {
        "campaign_id": campaign_id,
        "audience_name": audience_name,
        "run_id": run_id,
        "normalized_domain": dom,
        "company_linkedin_url": company_url,
        "icp_tier": result.get("icp") if isinstance(result, dict) else None,
        "ranking": result.get("ranking") if isinstance(result, dict) else None,
        "what_matched": json.dumps(result.get("what_matched")) if isinstance(result, dict) and result.get("what_matched") is not None else None,
        "person_linkedin_url": person.get("linkedin_url"),
        "first_name": person.get("first_name"),
        "last_name": person.get("last_name"),
        "full_name": person.get("full_name"),
        "headline": person.get("headline"),
        "job_title": job_title,
        "city": loc.get("city"),
        "state_code": loc.get("state_code"),
        "country_code": loc.get("country_code"),
        "continent": loc.get("continent"),
        "email": None,
        "email_found": None,
        "phone": None,
        "phone_found": None,
        "raw_person": json.dumps(person, default=str),
        "fetched_at": fetched_at,
    }


def _land_parquet(rows: list[dict], campaign_id: str, run_root: str) -> str | None:
    """Write the contact rows as ZSTD Parquet to the R2 transport landing zone.
    Returns the s3:// uri, or None when there are no rows."""
    if not rows:
        return None
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pylist(rows, schema=_contact_schema())
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    key = f"{LANDING_PREFIX}/campaign_id={_slug(campaign_id)}/run={_slug(run_root)}/part-0000.parquet"
    _r2_s3_client().put_object(Bucket=LANDING_BUCKET, Key=key, Body=buf.getvalue())
    return f"s3://{LANDING_BUCKET}/{key}"


# ── Terminal state + callback ────────────────────────────────────────────────
def _record_run(cur, campaign_id, audience_name, priority, run_root, counts, landing_uri,
                status, error, started_at, completed_at) -> None:
    cur.execute(OPS_DDL)
    cur.execute(
        """
        INSERT INTO ops.blitz_hydration_runs
            (feed, campaign_id, audience_name, priority, run_root, companies_in,
             linkedin_local, linkedin_api, linkedin_unresolved, waterfall_calls,
             contacts_found, emails_found, phones_found, gateway_calls,
             email_gated, phone_gated, landing_uri, status, error, started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (FEED, campaign_id, audience_name, priority, run_root, counts["companies_in"],
         counts["linkedin_local"], counts["linkedin_api"], counts["linkedin_unresolved"],
         counts["waterfall_calls"], counts["contacts_found"], counts["emails_found"],
         counts["phones_found"], counts["gateway_calls"], counts["email_gated"],
         counts["phone_gated"], landing_uri, status, error, started_at, completed_at),
    )


def _post_callback(url: str | None, payload: dict, attempts: int = 3) -> None:
    """POST terminal counts to the Trigger waitpoint url — RAW body, no envelope."""
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


# ── The hydration runner ─────────────────────────────────────────────────────
@app.function(secrets=SECRETS, timeout=60 * 60, memory=4096, cpu=2.0)
def hydrate_campaign_waterfall(
    campaign_id: str,
    cascade: list | None = None,
    audience_name: str | None = None,
    max_results_per_company: int = 5,
    enrich_email: bool = True,
    enrich_phone: bool = False,
    priority: str = "high",
    run_id: str | None = None,
    trigger_callback_url: str | None = None,
) -> dict:
    """Hydrate a saved campaign audience with executive contacts via Waterfall ICP,
    every Blitz call routed through the gateway. See module docstring for the flow."""
    started_at = dt.datetime.now(dt.timezone.utc)
    run_root = run_id or uuid.uuid4().hex
    priority = priority if priority in VALID_PRIORITIES else "high"
    max_results = max(1, min(int(max_results_per_company or 5), MAX_RESULTS_CAP))
    counts = {"companies_in": 0, "linkedin_local": 0, "linkedin_api": 0,
              "linkedin_unresolved": 0, "waterfall_calls": 0, "contacts_found": 0,
              "emails_found": 0, "phones_found": 0, "gateway_calls": 0,
              "email_gated": False, "phone_gated": False}
    status, error, landing_uri = "error", None, None
    resolved_audience = audience_name

    try:
        if not isinstance(cascade, list) or not cascade:
            raise ValueError("cascade must be a non-empty list of Waterfall tiers")
        if len(cascade) > MAX_CASCADE_TIERS:
            print(f"WARN: cascade has {len(cascade)} tiers; Waterfall accepts ≤{MAX_CASCADE_TIERS} "
                  "— passing through, the API may reject extra tiers.")

        enrich_email, enrich_phone, counts["email_gated"], counts["phone_gated"] = _preflight(
            enrich_email, enrich_phone)

        dsn = _hqx_dsn()
        conn = _open_conn(dsn)
        try:
            cur = conn.cursor()
            source_query, resolved_audience = _read_audience(cur, campaign_id, audience_name)
        finally:
            conn.close()

        companies = _resolve_companies(source_query)
        counts["companies_in"] = len(companies)
        print(f"audience '{resolved_audience}' → {counts['companies_in']:,} companies "
              f"(campaign {campaign_id!r})")

        gw = _gateway()
        rows: list[dict] = []
        for rec in companies:
            dom = rec.get("normalized_domain")
            url = rec.get("company_linkedin_url")
            if url:
                counts["linkedin_local"] += 1
            else:
                # Bridge miss → resolve via the gateway (domain→linkedin), HIGH lane.
                counts["gateway_calls"] += 1
                rb = gw.remote(endpoint=RESOLVE_ENDPOINT, payload={"domain": dom}, priority=priority)
                data = rb.get("data") or {}
                url = (data.get("company_linkedin_url") or data.get("linkedin_url")
                       if (rb.get("ok") and isinstance(data, dict) and data.get("found")) else None)
                if url:
                    counts["linkedin_api"] += 1
                else:
                    counts["linkedin_unresolved"] += 1
                    continue  # no company URL → no Waterfall possible

            # Waterfall targeting (HIGH lane).
            counts["gateway_calls"] += 1
            counts["waterfall_calls"] += 1
            rb = gw.remote(
                endpoint=WATERFALL_PATH,
                payload={"company_linkedin_url": url, "cascade": cascade, "max_results": max_results},
                priority=priority,
            )
            fetched_at = dt.datetime.now(dt.timezone.utc)
            for result in _waterfall_people(rb):
                row = _contact_row(campaign_id, resolved_audience, run_root, dom, url, result, fetched_at)
                counts["contacts_found"] += 1
                purl = row["person_linkedin_url"]
                if purl and enrich_email:
                    counts["gateway_calls"] += 1
                    email, _ = _enrich_email(gw, purl, priority)
                    row["email"], row["email_found"] = email, bool(email)
                    if email:
                        counts["emails_found"] += 1
                if purl and enrich_phone and (row.get("country_code") == "US"):
                    counts["gateway_calls"] += 1
                    phone, _ = _enrich_phone(gw, purl, priority)
                    row["phone"], row["phone_found"] = phone, bool(phone)
                    if phone:
                        counts["phones_found"] += 1
                rows.append(row)

        landing_uri = _land_parquet(rows, campaign_id, run_root)
        print(f"hydrated {counts['contacts_found']:,} contacts "
              f"({counts['emails_found']:,} emails, {counts['phones_found']:,} phones) "
              f"in {counts['gateway_calls']:,} gateway calls → {landing_uri}")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        try:
            conn2 = _open_conn(_hqx_dsn())
            try:
                _record_run(conn2.cursor(), campaign_id, resolved_audience, priority, run_root,
                            counts, landing_uri, status, error, started_at, completed_at)
            finally:
                conn2.close()
        except Exception as exc2:  # noqa: BLE001 — audit must not mask the run outcome
            print(f"WARN: ops.blitz_hydration_runs write failed: {exc2}")
        _post_callback(
            trigger_callback_url,
            {"status": status, "feed": FEED, "campaign_id": campaign_id,
             "audience_name": resolved_audience, "landing_uri": landing_uri,
             "error": error, **counts},
        )

    if status != "success":
        raise RuntimeError(f"blitz_hydration_waterfall failed (campaign {campaign_id!r}): {error}")
    return {"feed": FEED, "campaign_id": campaign_id, "audience_name": resolved_audience,
            "status": status, "landing_uri": landing_uri, **counts}


# ── Ops / verification entrypoints ───────────────────────────────────────────
@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.blitz_hydration_runs in the HQX control-plane DB (idempotent)."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name='blitz_hydration_runs'
            ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    print(f"ops.blitz_hydration_runs ready — columns: {cols}")
    return {"table": "ops.blitz_hydration_runs", "columns": cols}


@app.function(secrets=SECRETS, timeout=60 * 5)
def verify_resolver() -> dict:
    """Safe smoke (NO Blitz spend, NO DB write): confirm the reused gtm-mcp resolver
    imports + the R2 dataset registry discovers in-image, and the two bridge datasets
    resolve. Validates the riskiest integration point — the apps/→ import + R2."""
    from apps.gtm_mcp.src import database as gtmdb

    reg = gtmdb.get_registry()
    out = {
        "resolver_import": True,
        "datasets_discovered": len(reg),
        "companies_registered": "companies" in reg,
        "firmographics_blitz_registered": "firmographics_blitz" in reg,
        "gateway_keyinfo_ok": False,
    }
    try:
        ki = modal.Function.from_name(GATEWAY_APP, GATEWAY_KEYINFO_FN).remote()
        out["gateway_keyinfo_ok"] = bool(ki.get("valid"))
        out["waterfall_allowed"] = "/search/waterfall-icp-keyword" in set(ki.get("allowed_apis") or [])
    except Exception as exc:  # noqa: BLE001
        out["gateway_error"] = str(exc)
    print(json.dumps(out, indent=2, default=str))
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def verify(limit: int = 5) -> dict:
    """Read-back the most-recent ops.blitz_hydration_runs rows."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute(
            """SELECT campaign_id, audience_name, priority, companies_in, contacts_found,
                      emails_found, phones_found, gateway_calls, status, landing_uri, recorded_at
               FROM ops.blitz_hydration_runs ORDER BY recorded_at DESC LIMIT %s""",
            (limit,),
        )
        cols = [d.name for d in cur.description]
        runs = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    return {"recent_runs": runs}


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.blitz_hydration_runs DDL (HQX)."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def verify_resolver_run() -> None:
    """Safe resolver/registry/gateway smoke (no Blitz spend)."""
    print(json.dumps(verify_resolver.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_run(limit: int = 5) -> None:
    """Read-back the most-recent hydration runs."""
    print(json.dumps(verify.remote(limit), indent=2, default=str))
