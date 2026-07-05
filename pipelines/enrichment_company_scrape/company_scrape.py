"""Company-scrape worker — Icypeas /api/scrape rail (SYNCHRONOUS, Postgres-direct landing).

Scrapes LinkedIn company URLs through Icypeas and lands the results in hq-x Postgres. /api/scrape
is SYNCHRONOUS — it returns the scraped company data INLINE in the response (no file id, no webhook,
no poll), so this worker gets the data back from the single-container ``core/icypeas_gateway.py``
(``scrape_companies``) and writes it directly to ``gtm.icypeas_company_scrapes`` (the raw SoR),
exactly as the email-cascade worker writes ``ops.email_resolutions`` — no edge_api hop.

    run_company_scrape   company_urls[] → scraped rows in gtm.icypeas_company_scrapes

ZERO-READ. /api/scrape returns results inline, so this rail never touches the global 30/min
``/bulk-single-searchs/read`` ceiling the email cascade + bulk drain contend for. The account was
suspended once by ungoverned probing; the gateway governs the /api/scrape request rate.

IDEMPOTENCY. A company URL already landed with ``status='FOUND'`` is skipped on re-run (never
re-spend a scrape credit) unless ``force=True`` — a prior NOT_FOUND / failed URL is retryable.
``ops.company_scrape_runs`` records per-run terminal counts.

RAW-FIRST (Directive 28). ``gtm.icypeas_company_scrapes`` is append-only: ``raw_result`` holds the
Icypeas ``data[]`` item VERBATIM (the system of record); the flat columns are a best-effort projection
ON TOP of it, never a replacement. A downstream materializer picks the latest per company on its own
cadence and bridges via ``company_url_norm`` / ``domain_norm``.

    modal deploy pipelines/enrichment_company_scrape/company_scrape.py
    modal run    pipelines/enrichment_company_scrape/company_scrape.py::init_ops
    modal run    pipelines/enrichment_company_scrape/company_scrape.py::run_manual \\
                 --urls-json '["https://www.linkedin.com/company/nec-technologies"]'
"""
from __future__ import annotations

import datetime as dt
import os
import re
import time
import uuid

import modal

FEED = "company_scrape"
GATEWAY_APP, GATEWAY_FN = "icypeas-gateway", "scrape_companies"

# Icypeas /api/scrape hard cap (≤50 URLs/request). Mirror of the gateway's SCRAPE_MAX_BATCH.
SCRAPE_MAX_BATCH = int(os.environ.get("ICYPEAS_SCRAPE_MAX_BATCH", "50"))

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "psycopg[binary]>=3.2",  # gtm.icypeas_company_scrapes + ops.company_scrape_runs
    "requests>=2.32",        # Trigger callback
)

app = modal.App("enrichment-company-scrape", image=image)

SECRETS = [modal.Secret.from_name("hqx-postgres")]   # gtm.* + ops.* live in HQX

# ── DDL — verbatim mirror of the .sql sibling; applied defensively before writes (idempotent). ──
DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.icypeas_company_scrapes (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_url       text        NOT NULL,          -- the requested LinkedIn company URL
    company_url_norm  text,                           -- normalized (idempotency / bridge key)
    search_id         text,                           -- Icypeas searchId (provenance)
    status            text,                           -- FOUND / NOT_FOUND / … verbatim
    -- flat projection (best-effort, from result{}) — convenience OVER raw_result, never a replacement
    company_name      text,
    linkedin_url      text,                           -- result.url (canonical LinkedIn company url)
    website           text,
    domain_norm       text,                           -- normalized website domain — bridge to firmographics
    industry          text,
    headcount_range   text,
    employee_count    int,
    country           text,
    raw_result        jsonb       NOT NULL,           -- the Icypeas data[] item VERBATIM — system of record
    batch_label       text,
    run_root          text,
    scraped_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS icypeas_company_scrapes_url_norm_idx ON gtm.icypeas_company_scrapes (company_url_norm);
CREATE INDEX IF NOT EXISTS icypeas_company_scrapes_domain_idx   ON gtm.icypeas_company_scrapes (domain_norm);
CREATE INDEX IF NOT EXISTS icypeas_company_scrapes_linkedin_idx ON gtm.icypeas_company_scrapes (linkedin_url);
CREATE INDEX IF NOT EXISTS icypeas_company_scrapes_scraped_idx  ON gtm.icypeas_company_scrapes (scraped_at DESC);

CREATE TABLE IF NOT EXISTS ops.company_scrape_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    batch_label   text,
    run_root      text,
    requested     bigint      NOT NULL DEFAULT 0,
    skipped       bigint      NOT NULL DEFAULT 0,
    found         bigint      NOT NULL DEFAULT 0,
    not_found     bigint      NOT NULL DEFAULT 0,
    batches       bigint      NOT NULL DEFAULT 0,
    failed        bigint      NOT NULL DEFAULT 0,
    status        text        NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT company_scrape_runs_status_chk CHECK (status IN ('success', 'error'))
);
CREATE INDEX IF NOT EXISTS company_scrape_runs_feed_idx     ON ops.company_scrape_runs (feed);
CREATE INDEX IF NOT EXISTS company_scrape_runs_recorded_idx ON ops.company_scrape_runs (recorded_at DESC);
"""

_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")


def _norm_url(u: str | None) -> str | None:
    """Normalize a URL for idempotency/bridging: drop scheme, www, trailing slash; lowercase."""
    x = (u or "").strip().lower()
    x = _SCHEME.sub("", x)
    x = _WWW.sub("", x)
    x = x.rstrip("/")
    return x or None


def _norm_domain(u: str | None) -> str | None:
    """Normalize a website into a bare domain (bridge key to firmographics)."""
    x = (u or "").strip().lower()
    x = _SCHEME.sub("", x)
    x = _WWW.sub("", x)
    x = x.split("/", 1)[0].rstrip(".")
    return x or None


def _hqx_dsn() -> str:
    """Transaction-mode (Supavisor :6543) hq-x DSN — mirror of the email-cascade worker's discipline."""
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


def _gateway():
    return modal.Function.from_name(GATEWAY_APP, GATEWAY_FN)


def _chunk(items: list, size: int) -> list[list]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _already_found(cur, url_norms: list[str]) -> set[str]:
    """Idempotency skip-set: company_url_norm values already landed with status='FOUND' (never
    re-spend a scrape credit). A prior NOT_FOUND / failed URL is intentionally retryable."""
    if not url_norms:
        return set()
    try:
        cur.execute(
            "SELECT DISTINCT company_url_norm FROM gtm.icypeas_company_scrapes "
            "WHERE status = 'FOUND' AND company_url_norm = ANY(%s)",
            (url_norms,),
        )
        return {r[0] for r in cur.fetchall()}
    except Exception as exc:  # noqa: BLE001 — degrade skip, preserve correctness
        print(f"WARN: skip-set lookup failed ({exc}); proceeding without skip.")
        return set()


def _insert_scrape(cur, requested_url: str, item: dict, batch_label: str | None, run_root: str) -> None:
    """Append one scraped-company row. ``item`` is the Icypeas data[] element VERBATIM
    (``{result:{…}, status, searchId}``) → raw_result; the scalar columns are a best-effort projection."""
    from psycopg.types.json import Jsonb

    result = item.get("result") if isinstance(item, dict) else None
    result = result if isinstance(result, dict) else {}
    addr = result.get("address") if isinstance(result.get("address"), dict) else {}
    emp = result.get("numberOfEmployees")
    cur.execute(
        """
        INSERT INTO gtm.icypeas_company_scrapes
            (company_url, company_url_norm, search_id, status, company_name, linkedin_url, website,
             domain_norm, industry, headcount_range, employee_count, country, raw_result,
             batch_label, run_root)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            requested_url,
            _norm_url(requested_url),
            item.get("searchId") if isinstance(item, dict) else None,
            (item.get("status") if isinstance(item, dict) else None),
            result.get("name"),
            result.get("url"),
            result.get("website"),
            _norm_domain(result.get("website")),
            result.get("industry"),
            result.get("headcountRange"),
            int(emp) if isinstance(emp, (int, float)) and emp >= 0 else None,
            addr.get("addressCountry") or addr.get("addressCountryCode"),
            Jsonb(item),
            batch_label,
            run_root,
        ),
    )


def _record_run(cur, batch_label: str | None, run_root: str, counts: dict, status: str,
                error: str | None, started_at: dt.datetime, completed_at: dt.datetime) -> None:
    cur.execute(DDL)
    cur.execute(
        """
        INSERT INTO ops.company_scrape_runs
            (feed, batch_label, run_root, requested, skipped, found, not_found, batches, failed,
             status, error, started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (FEED, batch_label, run_root, counts["requested"], counts["skipped"], counts["found"],
         counts["not_found"], counts["batches"], counts["failed"], status, error,
         started_at, completed_at),
    )


def _post_callback(url: str | None, payload: dict, attempts: int = 3) -> None:
    """POST terminal counts to the Trigger waitpoint url — RAW body, no envelope."""
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
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


def _run(company_urls: list[str], batch_label: str | None, run_id: str | None,
         force: bool, trigger_callback_url: str | None) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    run_root = run_id or uuid.uuid4().hex
    counts = {"requested": 0, "skipped": 0, "found": 0, "not_found": 0, "batches": 0, "failed": 0}
    status, error = "error", None
    dsn = _hqx_dsn()

    try:
        # De-dup + normalize while preserving first-seen order.
        seen: set[str] = set()
        urls: list[str] = []
        for raw in (company_urls or []):
            u = (raw or "").strip() if isinstance(raw, str) else ""
            if u and u not in seen:
                seen.add(u)
                urls.append(u)
        counts["requested"] = len(urls)

        gw = _gateway()
        conn = _open_conn(dsn)
        try:
            cur = conn.cursor()
            cur.execute(DDL)  # ensure sink + ledger exist before first write

            skip_norms = set() if force else _already_found(cur, [_norm_url(u) for u in urls])
            todo = [u for u in urls if _norm_url(u) not in skip_norms]
            counts["skipped"] = len(urls) - len(todo)

            for batch in _chunk(todo, SCRAPE_MAX_BATCH):
                counts["batches"] += 1
                env = gw.remote(urls=batch, external_ids=batch)
                if not env.get("ok"):
                    counts["failed"] += len(batch)
                    print(f"WARN: scrape failed for {len(batch)} urls: {env.get('error')}")
                    continue
                results = env.get("results") or []
                # results[] is positionally aligned to batch[]; persist each verbatim.
                for i, item in enumerate(results):
                    requested_url = batch[i] if i < len(batch) else (
                        (item.get("result") or {}).get("url") if isinstance(item, dict) else None)
                    try:
                        _insert_scrape(cur, requested_url, item if isinstance(item, dict) else {},
                                       batch_label, run_root)
                    except Exception as exc:  # noqa: BLE001 — one row must not sink the batch
                        counts["failed"] += 1
                        print(f"WARN: insert failed for {requested_url!r}: {exc}")
                        continue
                    st = (item.get("status") if isinstance(item, dict) else None) or ""
                    if st.upper() == "FOUND":
                        counts["found"] += 1
                    else:
                        counts["not_found"] += 1
                # Icypeas returned fewer items than submitted (shouldn't happen) → count the gap failed.
                gap = len(batch) - len(results)
                if gap > 0:
                    counts["failed"] += gap

            status = "success"
            _record_run(cur, batch_label, run_root, counts, status, None,
                        started_at, dt.datetime.now(dt.timezone.utc))
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — terminal handling + re-raise
        error = str(exc)
        status = "error"
        try:
            conn2 = _open_conn(dsn)
            try:
                _record_run(conn2.cursor(), batch_label, run_root, counts, status, error,
                            started_at, dt.datetime.now(dt.timezone.utc))
            finally:
                conn2.close()
        except Exception as exc2:  # noqa: BLE001 — audit must not mask the failure
            print(f"WARN: ops.company_scrape_runs write failed: {exc2}")
    finally:
        _post_callback(
            trigger_callback_url,
            {"status": status, "feed": FEED, "batch_label": batch_label, "error": error, **counts},
        )

    if status != "success":
        raise RuntimeError(f"company_scrape failed: {error}")
    return {"feed": FEED, "status": status, "batch_label": batch_label, **counts}


@app.function(secrets=SECRETS, timeout=60 * 30, memory=1024, cpu=1.0)
def run_company_scrape(company_urls: list[str], batch_label: str | None = None,
                       run_id: str | None = None, force: bool = False,
                       trigger_callback_url: str | None = None) -> dict:
    """Scrape a chunk of LinkedIn company URLs via Icypeas /api/scrape (through the gateway) and land
    the results in gtm.icypeas_company_scrapes. Synchronous — the gateway returns the scraped data
    inline. ``company_urls`` — LinkedIn company profile URLs (e.g. linkedin.com/company/<slug>)."""
    return _run(company_urls, batch_label, run_id, force, trigger_callback_url)


@app.function(secrets=SECRETS, timeout=60 * 5)
def apply_ddl() -> dict:
    """Create gtm.icypeas_company_scrapes + ops.company_scrape_runs in HQX (idempotent)."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(DDL)
        cur.execute("""
            SELECT table_schema, table_name, count(*) AS cols
            FROM information_schema.columns
            WHERE (table_schema='gtm'  AND table_name='icypeas_company_scrapes')
               OR (table_schema='ops'  AND table_name='company_scrape_runs')
            GROUP BY table_schema, table_name ORDER BY table_schema, table_name
        """)
        tables = {f"{s}.{t}": c for s, t, c in cur.fetchall()}
    finally:
        conn.close()
    print(f"tables ready: {tables}")
    return {"tables": tables}


@app.function(secrets=SECRETS, timeout=60 * 5)
def verify(limit: int = 8) -> dict:
    """Read-back: latest scraped companies + status histogram + recent run-state."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT company_url, status, company_name, industry, headcount_range, domain_norm, scraped_at
               FROM gtm.icypeas_company_scrapes ORDER BY scraped_at DESC LIMIT %s""",
            (limit,),
        )
        scols = [d.name for d in cur.description]
        scrapes = [dict(zip(scols, r)) for r in cur.fetchall()]
        cur.execute("SELECT status, count(*) FROM gtm.icypeas_company_scrapes GROUP BY 1")
        histogram = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute(
            """SELECT batch_label, requested, skipped, found, not_found, batches, failed, status, recorded_at
               FROM ops.company_scrape_runs ORDER BY recorded_at DESC LIMIT 3""")
        rcols = [d.name for d in cur.description]
        runs = [dict(zip(rcols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    print(f"status histogram: {histogram}")
    for s in scrapes:
        print(f"  {(s['company_name'] or '-'):<32} {s['status']:<10} {s['domain_norm'] or '-'}")
    return {"histogram": histogram, "recent_scrapes": scrapes, "recent_runs": runs}


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the gtm.icypeas_company_scrapes + ops.company_scrape_runs DDL (HQX)."""
    print(apply_ddl.remote())


@app.local_entrypoint()
def verify_run(limit: int = 8) -> None:
    """Read-back assertion on the most-recent scrapes."""
    import json

    print(json.dumps(verify.remote(limit), indent=2, default=str))


@app.local_entrypoint()
def run_manual(urls_json: str, batch_label: str = "manual", force: bool = False) -> None:
    """Manual run. --urls-json '["https://www.linkedin.com/company/nec-technologies"]'"""
    import json

    urls = json.loads(urls_json)
    print(json.dumps(run_company_scrape.remote(urls, batch_label=batch_label, force=force),
                     indent=2, default=str))
