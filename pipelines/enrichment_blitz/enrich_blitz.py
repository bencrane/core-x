"""Enrichment-Blitz workers — Atomic Firmographic Workflows A/B/C (Directive 23).

Domain-grouped Modal worker for the rate-governed Blitz firmographic enrichment
plane. Three entrypoints, all spawned by the Universal Dispatcher
(``core/modal_dispatcher.py``) and woken via the Trigger waitpoint callback:

    run_resolve_domains   Workflow A  domain → company_linkedin_url
    run_enrich_linkedin   Workflow B  company_linkedin_url → firmographics
    run_cascade           Workflow C  domain → linkedin → firmographics (unified)

RATE GOVERNANCE. Every Blitz HTTP call is delegated to the single-container
``core/blitz_gateway.py`` (``blitz-gateway`` app) — the authoritative global
≤5-RPS priority egress. These workers hold NO Blitz key and implement NO rate
logic; they call ``blitz_call`` and write the result.

JIT SKIP (the warehouse guardrail, Directive 23 §4). Before any gateway call, a
batched read of the committed Lance plane (``companies.company_linkedin_url``,
``firmographics_blitz``) plus the ``ops.task_runs`` event log partitions the
cohort so a rate-limited call is never spent on data we already hold (or a known
miss / fresh firmo).

SINK (Directive 23 §5 — event-log-only; no new Lance write pattern). Per-entity
results are written to the hq-x ``ops.task_runs`` event log, preserving the
task_type / JSONB contract the ``firmographics-blitz`` materializer already reads:

    blitz_domain_resolve         (A, new)  result_payload {found, company_linkedin_url}
    blitz_firmo_direct           (B)       result_payload.blitz_payload.{found, company{}}
    modal_hydrate_firmo_cascade  (C)       result_payload.blitz_data.{found, company{}}

The materializer rolls B/C into the ``firmographics_blitz`` Lance system-of-record
on its own cadence. Per-run terminal state lands in ``ops.enrichment_blitz_runs``.

    modal deploy pipelines/enrichment_blitz/enrich_blitz.py
    modal run    pipelines/enrichment_blitz/enrich_blitz.py::init_ops
    modal run    pipelines/enrichment_blitz/enrich_blitz.py::run_c --domains openai.com,stripe.com
"""

from __future__ import annotations

import datetime as dt
import os
import re
import uuid
from typing import Any

import modal

# ── Active sink coordinates (overridable per-dataset, fleet convention) ──────
_ACTIVE = "s3://data-sink/active"
COMPANIES_URI = os.environ.get("GTM_COMPANIES_URI", f"{_ACTIVE}/companies/")
FIRMO_URI = os.environ.get("FIRMOGRAPHICS_BLITZ_URI", f"{_ACTIVE}/firmographics_blitz/")
SINK_BUCKET = "data-sink"

GATEWAY_APP, GATEWAY_FN = "blitz-gateway", "blitz_call"

FEED = "enrichment_blitz"
TASK_TYPE = {
    "A": "blitz_domain_resolve",
    "B": "blitz_firmo_direct",
    "C": "modal_hydrate_firmo_cascade",
}
DEFAULT_FIRMO_TTL_DAYS = 180
DEFAULT_NEG_TTL_DAYS = 30
# All three firmo-relevant task_types the JIT planner consults in the event log.
_LEDGER_TASK_TYPES = ["blitz_domain_resolve", "blitz_firmo_direct", "modal_hydrate_firmo_cascade"]

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",         # provides `import lance`
    "pyarrow>=17",
    "psycopg[binary]>=3.2",
    "requests>=2.32",     # Trigger callback
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("enrichment-blitz", image=image)

SECRETS = [
    modal.Secret.from_name("r2-credentials"),  # Lance reads (companies / firmographics_blitz)
    modal.Secret.from_name("hqx-postgres"),    # ops.task_runs (read+write) + ops.enrichment_blitz_runs
]

# ── ops.enrichment_blitz_runs DDL — verbatim mirror of the .sql sibling. Applied
# defensively before each terminal write (idempotent). ───────────────────────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.enrichment_blitz_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,
    workflow       text        NOT NULL,
    batch_label    text,
    priority       text        NOT NULL,
    run_root       text,
    requested      bigint      NOT NULL DEFAULT 0,
    skipped        bigint      NOT NULL DEFAULT 0,
    api_calls      bigint      NOT NULL DEFAULT 0,
    succeeded      bigint      NOT NULL DEFAULT 0,
    not_found      bigint      NOT NULL DEFAULT 0,
    failed         bigint      NOT NULL DEFAULT 0,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT enrichment_blitz_runs_workflow_chk CHECK (workflow IN ('A', 'B', 'C')),
    CONSTRAINT enrichment_blitz_runs_status_chk   CHECK (status   IN ('success', 'error'))
);
CREATE INDEX IF NOT EXISTS enrichment_blitz_runs_feed_idx        ON ops.enrichment_blitz_runs (feed);
CREATE INDEX IF NOT EXISTS enrichment_blitz_runs_workflow_idx    ON ops.enrichment_blitz_runs (workflow);
CREATE INDEX IF NOT EXISTS enrichment_blitz_runs_recorded_at_idx ON ops.enrichment_blitz_runs (recorded_at DESC);
"""

# ── Domain anchor normalization — mirrors pipelines/firmographics_blitz +
# apps/gtm_mcp/.../audience.py EXACTLY so inputs collapse to the stored anchor. ─
_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")


def _normalize_domain(raw: str | None) -> str | None:
    d = (raw or "").strip().lower()
    d = _SCHEME.sub("", d)
    d = _WWW.sub("", d)
    d = d.split("/", 1)[0].rstrip(".")
    return d or None


def _sql_lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _as_utc(value: Any) -> dt.datetime | None:
    if not isinstance(value, dt.datetime):
        return None
    return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)


# ── R2 / Postgres plumbing (fleet-standard) ──────────────────────────────────
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
    """Transaction-mode (Supavisor :6543) hq-x DSN for this rate-gated, horizontally
    scaled worker.

    The worker holds one Postgres connection for the whole run while it is mostly idle —
    blocked on the rate-governed gateway between sparse DB touches. On the SESSION pooler
    (:5432, pool_size=15) every such connection pins a backend for the entire run, so N
    concurrent workers exhaust the 15-slot ceiling (``EMAXCONNSESSION: max clients reached
    in session mode``). The TRANSACTION pooler checks a backend out only for each autocommit
    statement, returning it between the JIT read and the per-entity writes — a long-lived-
    but-idle worker costs ~0 backends. Prefer an explicit HQX_DB_URL_TRANSACTION; else derive
    it from the session DSN (Supavisor maps session→transaction by the pooler port 5432→6543)."""
    dsn = os.environ.get("HQX_DB_URL_TRANSACTION")
    if not dsn:
        pooled = os.environ.get("HQX_DB_URL_POOLED")
        if not pooled:
            raise RuntimeError(
                "Neither HQX_DB_URL_TRANSACTION nor HQX_DB_URL_POOLED set in the "
                "hqx-postgres Modal secret.")
        dsn = pooled.replace(".pooler.supabase.com:5432", ".pooler.supabase.com:6543")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _lance_lookup(uri: str, key_col: str, keys: list[str], cols: list[str], so: dict) -> list[dict]:
    """Batched Lance BTREE pushdown: rows where ``key_col IN keys`` (sub-100 ms).

    Best-effort: the JIT skip is an optimization, not a correctness gate. A missing
    dataset / column degrades to "no skip" (we simply make the API call) rather than
    sinking the whole workflow."""
    import lance

    if not keys:
        return []
    try:
        in_list = ", ".join(_sql_lit(k) for k in keys)
        ds = lance.dataset(uri, storage_options=so)
        return ds.scanner(filter=f"{key_col} IN ({in_list})", columns=cols).to_table().to_pylist()
    except Exception as exc:  # noqa: BLE001 — degrade JIT skip, preserve correctness
        print(f"WARN: JIT lookup {uri} [{key_col}] failed ({exc}); proceeding without skip.")
        return []


# ── JIT skip planner (Directive 23 §4) ───────────────────────────────────────
def _plan(cur, keys: list[str], link_keys: list[str], firmo_ttl_days: int,
          neg_ttl_days: int, so: dict) -> tuple[dict[str, str], set, set]:
    """Batched warehouse read → (known_url, fresh, neg).

    known_url : domain_norm → company_linkedin_url already on record
    fresh     : {domain_norm} ∪ {("link", url)} with firmo within FIRMO_TTL
    neg       : {domain_norm} ∪ {("link", url)} with a recent Blitz found:false
    """
    known_url: dict[str, str] = {}
    fresh: set = set()
    neg: set = set()
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=firmo_ttl_days)

    # READ 1 — companies.company_linkedin_url (Lance BTREE on normalized_domain)
    for r in _lance_lookup(COMPANIES_URI, "normalized_domain", keys,
                           ["normalized_domain", "company_linkedin_url"], so):
        if r.get("company_linkedin_url"):
            known_url.setdefault(r["normalized_domain"], r["company_linkedin_url"])

    # READ 2 — firmographics_blitz freshness + url (by domain_norm, and by linkedin_url for B)
    for r in _lance_lookup(FIRMO_URI, "domain_norm", keys,
                           ["domain_norm", "linkedin_url", "source_updated_at"], so):
        if r.get("linkedin_url"):
            known_url.setdefault(r["domain_norm"], r["linkedin_url"])
        ts = _as_utc(r.get("source_updated_at"))
        if ts and ts >= cutoff:
            fresh.add(r["domain_norm"])
    for r in _lance_lookup(FIRMO_URI, "linkedin_url", link_keys,
                           ["linkedin_url", "source_updated_at"], so):
        ts = _as_utc(r.get("source_updated_at"))
        if ts and ts >= cutoff:
            fresh.add(("link", r["linkedin_url"]))

    # READ 3 — ops.task_runs event log: negative cache + recent in-flight results
    if keys or link_keys:
        cur.execute(
            """
            SELECT domain, linkedin_url, task_type, status, result_payload, updated_at
            FROM ops.task_runs
            WHERE task_type = ANY(%s)
              AND updated_at >= now() - make_interval(days => %s)
              AND (domain = ANY(%s) OR linkedin_url = ANY(%s))
            """,
            (_LEDGER_TASK_TYPES, neg_ttl_days, keys or [""], link_keys or [""]),
        )
        for domain, link, tt, status, payload, _updated in cur.fetchall():
            dn = _normalize_domain(domain) if domain else None
            if status == "not_found":
                if dn:
                    neg.add(dn)
                if link:
                    neg.add(("link", link))
            elif status == "completed":
                if tt == "blitz_domain_resolve" and dn and isinstance(payload, dict):
                    url = payload.get("company_linkedin_url")
                    if url:
                        known_url.setdefault(dn, url)
                if tt in ("blitz_firmo_direct", "modal_hydrate_firmo_cascade"):
                    if dn:
                        fresh.add(dn)
                    if link:
                        fresh.add(("link", link))
    return known_url, fresh, neg


# ── Gateway invocation ────────────────────────────────────────────────────────
def _gateway():
    return modal.Function.from_name(GATEWAY_APP, GATEWAY_FN)


def _resolve_outcome(rb: dict) -> tuple[str, str | None, str | None]:
    """(status, company_linkedin_url|None, error|None) from a /domain-to-linkedin envelope."""
    if not rb.get("ok"):
        return "failed", None, rb.get("error")
    data = rb.get("data") or {}
    url = (data.get("company_linkedin_url") or data.get("linkedin_url")) if isinstance(data, dict) else None
    if isinstance(data, dict) and data.get("found") and url:
        return "completed", url, None
    return "not_found", None, None


def _firmo_outcome(rb: dict) -> tuple[str, str | None, str | None]:
    """(status, company_domain|None, error|None) from a /enrichment/company envelope.

    company_domain anchors the materializer's domain_norm for Workflow B rows
    (which carry no input domain) — prefer company.domain, fall back to website host.
    """
    if not rb.get("ok"):
        return "failed", None, rb.get("error")
    data = rb.get("data") or {}
    company = data.get("company") if isinstance(data, dict) else None
    if not (isinstance(data, dict) and data.get("found") and isinstance(company, dict)):
        return "not_found", None, None
    dom = (company.get("domain") or "").strip() or None
    if not dom:
        dom = _normalize_domain(company.get("website"))
    return "completed", dom, None


# ── ops.task_runs writer (preserves the materializer contract) ───────────────
def _write_task_run(cur, run_root: str, entity_key: str, task_type: str, status: str,
                    domain: str | None, linkedin_url: str | None, result_payload: dict) -> None:
    from psycopg.types.json import Jsonb

    # Per-entity run_id keeps rows distinct under the (run_id, uei) composite unique
    # even when uei is NULL (these cohorts are domain/url-keyed, not UEI-keyed).
    rid = f"{run_root}:{entity_key}"
    cur.execute(
        """
        INSERT INTO ops.task_runs
            (run_id, uei, task_type, status, domain, linkedin_url, result_payload,
             inputs_count, outputs_count)
        VALUES (%s, NULL, %s, %s, %s, %s, %s, 1, %s)
        ON CONFLICT (run_id, uei) DO UPDATE SET
            status         = EXCLUDED.status,
            domain         = EXCLUDED.domain,
            linkedin_url   = EXCLUDED.linkedin_url,
            result_payload = EXCLUDED.result_payload,
            outputs_count  = EXCLUDED.outputs_count,
            updated_at     = now()
        """,
        (rid, task_type, status, domain, linkedin_url, Jsonb(result_payload),
         1 if status == "completed" else 0),
    )


def _record_run(cur, workflow: str, priority: str, batch_label: str | None, run_root: str,
                counts: dict, status: str, error: str | None,
                started_at: dt.datetime, completed_at: dt.datetime) -> None:
    cur.execute(OPS_DDL)
    cur.execute(
        """
        INSERT INTO ops.enrichment_blitz_runs
            (feed, workflow, batch_label, priority, run_root, requested, skipped,
             api_calls, succeeded, not_found, failed, status, error, started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (FEED, workflow, batch_label, priority, run_root, counts["requested"],
         counts["skipped"], counts["api_calls"], counts["succeeded"], counts["not_found"],
         counts["failed"], status, error, started_at, completed_at),
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


# ── Cohort loading ───────────────────────────────────────────────────────────
def _load_cohort(cohort: Any, so: dict) -> list[str]:
    """Inline list, or {"r2_key": "...", "column"?: "..."} pointing at a transport
    Parquet drop under the data-sink bucket."""
    if isinstance(cohort, list):
        return [str(x).strip() for x in cohort if x and str(x).strip()]
    if isinstance(cohort, dict) and cohort.get("r2_key"):
        import duckdb

        key = str(cohort["r2_key"]).lstrip("/")
        con = duckdb.connect(":memory:")
        try:
            host = _r2_endpoint().split("://", 1)[-1].rstrip("/")
            kid = os.environ["R2_ACCESS_KEY_ID"].replace("'", "''")
            sec = os.environ["R2_SECRET_ACCESS_KEY"].replace("'", "''")
            try:
                con.execute("INSTALL httpfs; LOAD httpfs;")
            except Exception:  # noqa: BLE001 — autoloadable in modern wheels
                con.execute("LOAD httpfs;")
            con.execute(
                f"CREATE OR REPLACE SECRET r2 (TYPE S3, KEY_ID '{kid}', SECRET '{sec}', "
                f"ENDPOINT '{host}', URL_STYLE 'path', USE_SSL true, REGION 'auto');"
            )
            src = f"read_parquet('s3://{SINK_BUCKET}/{key}')"
            colname = cohort.get("column") or con.sql(f"SELECT * FROM {src} LIMIT 0").columns[0]
            rows = con.sql(
                f"SELECT DISTINCT {colname} AS v FROM {src} WHERE {colname} IS NOT NULL"
            ).fetchall()
            return [str(r[0]).strip() for r in rows if r[0] and str(r[0]).strip()]
        finally:
            con.close()
    return []


def _open_conn(dsn: str):
    import psycopg

    # prepare_threshold=None disables psycopg3 server-side prepared statements: under
    # transaction-mode pooling consecutive statements may land on different backends, so a
    # prepared statement created on one is absent on the next. autocommit=True keeps each
    # statement its own short transaction so the pooler frees the backend between gateway calls.
    return psycopg.connect(dsn, autocommit=True, prepare_threshold=None)


# ── The three workflow runners ───────────────────────────────────────────────
def _run(workflow: str, cohort: Any, priority: str, batch_label: str | None,
         run_id: str | None, firmo_ttl_days: int, neg_ttl_days: int,
         trigger_callback_url: str | None) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    run_root = run_id or uuid.uuid4().hex
    counts = {"requested": 0, "skipped": 0, "api_calls": 0,
              "succeeded": 0, "not_found": 0, "failed": 0}
    status, error = "error", None
    so = _r2_storage_options()
    dsn = _hqx_dsn()

    try:
        items = _load_cohort(cohort, so)
        counts["requested"] = len(items)
        gw = _gateway()
        conn = _open_conn(dsn)
        try:
            cur = conn.cursor()
            if workflow in ("A", "C"):
                norms = [(raw, _normalize_domain(raw)) for raw in items]
                keys = sorted({dn for _, dn in norms if dn})
                known_url, fresh, neg = _plan(cur, keys, [], firmo_ttl_days, neg_ttl_days, so)
                for raw, dn in norms:
                    if dn is None:
                        counts["skipped"] += 1            # unnormalizable input
                        continue
                    if dn in neg:
                        counts["skipped"] += 1            # negative-cached miss
                        continue
                    if workflow == "C" and dn in fresh:
                        counts["skipped"] += 1            # DONE — fresh firmo on record
                        continue
                    url = known_url.get(dn)

                    if workflow == "A":                    # ── resolve only ──
                        if url is not None:
                            counts["skipped"] += 1        # already resolved
                            continue
                        counts["api_calls"] += 1
                        rb = gw.remote(endpoint="resolve", payload={"domain": raw}, priority=priority)
                        st, url, err = _resolve_outcome(rb)
                        _write_task_run(cur, run_root, dn, TASK_TYPE["A"], st, raw, url,
                                        {"found": st == "completed", "company_linkedin_url": url,
                                         "error": err})
                        counts["succeeded" if st == "completed" else st] += 1
                        continue

                    # ── workflow C: hop A (only when no URL on record), then hop B ──
                    if url is None:
                        counts["api_calls"] += 1
                        rb = gw.remote(endpoint="resolve", payload={"domain": raw}, priority=priority)
                        st, url, err = _resolve_outcome(rb)
                        _write_task_run(cur, run_root, dn, TASK_TYPE["A"], st, raw, url,
                                        {"found": st == "completed", "company_linkedin_url": url,
                                         "error": err})
                        if st != "completed":             # unresolved → no hop B
                            counts["not_found" if st == "not_found" else "failed"] += 1
                            continue
                    counts["api_calls"] += 1
                    rb = gw.remote(endpoint="company",
                                   payload={"company_linkedin_url": url}, priority=priority)
                    st, _cdom, err = _firmo_outcome(rb)
                    _write_task_run(
                        cur, run_root, dn, TASK_TYPE["C"], st, raw, url,
                        {"status": "completed" if st == "completed" else "failed",
                         "uei": None, "domain": raw, "linkedin_url": url,
                         "blitz_data": (rb.get("data") if st != "failed" else None),
                         "error": err},
                    )
                    counts["succeeded" if st == "completed" else st] += 1

            elif workflow == "B":
                urls = [u for u in items if u]
                link_keys = sorted(set(urls))
                _known, fresh, neg = _plan(cur, [], link_keys, firmo_ttl_days, neg_ttl_days, so)
                for url in urls:
                    if ("link", url) in fresh:
                        counts["skipped"] += 1
                        continue
                    if ("link", url) in neg:
                        counts["skipped"] += 1
                        continue
                    counts["api_calls"] += 1
                    rb = gw.remote(endpoint="company",
                                   payload={"company_linkedin_url": url}, priority=priority)
                    st, cdom, err = _firmo_outcome(rb)
                    _write_task_run(
                        cur, run_root, url, TASK_TYPE["B"], st, cdom, url,
                        {"blitz_payload": (rb.get("data") if st != "failed" else {"found": False}),
                         "error": err},
                    )
                    counts["succeeded" if st == "completed" else st] += 1
            else:
                raise ValueError(f"unknown workflow {workflow!r}")

            status = "success"
            _record_run(cur, workflow, priority, batch_label, run_root, counts,
                        status, None, started_at, dt.datetime.now(dt.timezone.utc))
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — terminal handling + re-raise
        error = str(exc)
        status = "error"
        try:
            conn2 = _open_conn(dsn)
            try:
                _record_run(conn2.cursor(), workflow, priority, batch_label, run_root, counts,
                            status, error, started_at, dt.datetime.now(dt.timezone.utc))
            finally:
                conn2.close()
        except Exception as exc2:  # noqa: BLE001 — audit must not mask the failure
            print(f"WARN: ops.enrichment_blitz_runs write failed: {exc2}")
    finally:
        _post_callback(
            trigger_callback_url,
            {"status": status, "workflow": workflow, "feed": FEED, "error": error, **counts},
        )

    if status != "success":
        raise RuntimeError(f"enrichment_blitz {workflow} failed: {error}")
    return {"workflow": workflow, "feed": FEED, "status": status, **counts}


@app.function(secrets=SECRETS, timeout=60 * 60, memory=2048, cpu=1.0)
def run_resolve_domains(cohort: Any, priority: str = "low", batch_label: str | None = None,
                        run_id: str | None = None, firmo_ttl_days: int = DEFAULT_FIRMO_TTL_DAYS,
                        neg_ttl_days: int = DEFAULT_NEG_TTL_DAYS,
                        trigger_callback_url: str | None = None) -> dict:
    """Workflow A — bulk domain → company_linkedin_url."""
    return _run("A", cohort, priority, batch_label, run_id, firmo_ttl_days, neg_ttl_days,
                trigger_callback_url)


@app.function(secrets=SECRETS, timeout=60 * 60 * 2, memory=2048, cpu=1.0)
def run_enrich_linkedin(cohort: Any, priority: str = "high", batch_label: str | None = None,
                        run_id: str | None = None, firmo_ttl_days: int = DEFAULT_FIRMO_TTL_DAYS,
                        neg_ttl_days: int = DEFAULT_NEG_TTL_DAYS,
                        trigger_callback_url: str | None = None) -> dict:
    """Workflow B — company_linkedin_url → firmographics."""
    return _run("B", cohort, priority, batch_label, run_id, firmo_ttl_days, neg_ttl_days,
                trigger_callback_url)


@app.function(secrets=SECRETS, timeout=60 * 60, memory=2048, cpu=1.0)
def run_cascade(cohort: Any, priority: str = "normal", batch_label: str | None = None,
                run_id: str | None = None, firmo_ttl_days: int = DEFAULT_FIRMO_TTL_DAYS,
                neg_ttl_days: int = DEFAULT_NEG_TTL_DAYS,
                trigger_callback_url: str | None = None) -> dict:
    """Workflow C — domain → linkedin → firmographics, JIT-routed per company."""
    return _run("C", cohort, priority, batch_label, run_id, firmo_ttl_days, neg_ttl_days,
                trigger_callback_url)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.enrichment_blitz_runs in the HQX control-plane DB (idempotent)."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name='enrichment_blitz_runs'
            ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    print(f"ops.enrichment_blitz_runs ready — columns: {cols}")
    return {"table": "ops.enrichment_blitz_runs", "columns": cols}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def verify(limit: int = 6) -> dict:
    """Read-back assertion: most-recent enrichment_blitz rows in ops.task_runs (with
    the materializer's company-object presence check) + the latest run-state rows.
    Confirms the write path lands rows the firmographics-blitz materializer will read."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT task_type, status, domain, linkedin_url,
                   (COALESCE(result_payload->'blitz_payload', result_payload->'blitz_data')
                        -> 'company') IS NOT NULL                          AS has_company,
                   (result_payload->>'company_linkedin_url')               AS resolved_url,
                   updated_at
            FROM ops.task_runs
            WHERE task_type = ANY(%s)
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (_LEDGER_TASK_TYPES, limit),
        )
        cols = [d.name for d in cur.description]
        task_runs = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.execute(
            """
            SELECT workflow, priority, requested, skipped, api_calls, succeeded,
                   not_found, failed, status, recorded_at
            FROM ops.enrichment_blitz_runs ORDER BY recorded_at DESC LIMIT 3
            """
        )
        rcols = [d.name for d in cur.description]
        runs = [dict(zip(rcols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    for r in task_runs:
        print(f"  {r['task_type']:<28} {r['status']:<10} domain={r['domain']!r} "
              f"has_company={r['has_company']} resolved_url={'Y' if r['resolved_url'] else '-'}")
    return {"recent_task_runs": task_runs, "recent_runs": runs}


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.enrichment_blitz_runs DDL (HQX)."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def verify_run(limit: int = 6) -> None:
    """Read-back assertion on the most-recent enrichment_blitz writes."""
    import json

    print(json.dumps(verify.remote(limit), indent=2, default=str))


def _csv(s: str) -> list[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


@app.local_entrypoint()
def run_a(domains: str) -> None:
    """Manual Workflow A. --domains a.com,b.com"""
    import json

    print(json.dumps(run_resolve_domains.remote(_csv(domains)), indent=2, default=str))


@app.local_entrypoint()
def run_b(linkedin_urls: str) -> None:
    """Manual Workflow B. --linkedin-urls https://linkedin.com/company/x,..."""
    import json

    print(json.dumps(run_enrich_linkedin.remote(_csv(linkedin_urls)), indent=2, default=str))


@app.local_entrypoint()
def run_c(domains: str) -> None:
    """Manual Workflow C. --domains a.com,b.com"""
    import json

    print(json.dumps(run_cascade.remote(_csv(domains)), indent=2, default=str))
