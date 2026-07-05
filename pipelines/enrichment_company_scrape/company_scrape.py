"""Company-scrape worker — Icypeas /api/scrape bulk submit rail (webhook delivery).

Decoupled, asynchronous company scraping. This worker is the SUBMIT + LEDGER half; the LANDING
half is edge_api (``/webhooks/icypeas/*`` → ``business.icypeas_webhook_events``, the raw SoR). The
worker NEVER polls Icypeas and NEVER reads results — it hands ≤50-URL batches to the single-
container ``core/icypeas_gateway.py`` (``scrape_submit``, one owner of the /api/scrape submit
governor), records what it submitted, and returns. Scraped rows arrive later, pushed to edge_api.

    run_company_scrape   company_urls[] → governed /api/scrape submits + ops ledger

WHY WEBHOOK, NOT DRAIN. The email cascade + bulk-drain rails already contend for the global 30/min
``/bulk-single-searchs/read`` ceiling. Company scrape delivers by webhook (custom.webhookUrlItem →
edge_api; custom.webhookUrlBulkDone → edge_api), so it makes ZERO reads — it never touches that
ceiling. The account was suspended once by ungoverned probing; this rail is submit-only and gently
rate-governed by construction.

IDEMPOTENCY. ``ops.company_scrape_submissions`` (PK company_url) is the submit ledger: a URL already
``submitted`` is skipped on re-run (never re-spend a scrape credit) unless ``force=True``. A prior
``submit_failed`` is retryable. ``ops.company_scrape_runs`` records per-run terminal counts.

SINK / RECONCILIATION. Results land in ``business.icypeas_webhook_events`` (edge_api), correlated by
the ``externalId`` we stamp == the requested company URL, and by the bulk ``file_id`` this worker
records per submission. A downstream materializer rolls landed rows into a company dimension on its
own cadence (Directive 28 raw-first — never inferred ahead of real landed payloads).

    modal deploy pipelines/enrichment_company_scrape/company_scrape.py
    modal run    pipelines/enrichment_company_scrape/company_scrape.py::init_ops
    modal run    pipelines/enrichment_company_scrape/company_scrape.py::run_manual \\
                 --urls-json '["https://www.linkedin.com/company/nec-technologies"]'
"""
from __future__ import annotations

import datetime as dt
import os
import time
import uuid

import modal

FEED = "company_scrape"
GATEWAY_APP, GATEWAY_FN = "icypeas-gateway", "scrape_submit"

# Icypeas /api/scrape hard cap (≤50 URLs/submit). Mirror of the gateway's SCRAPE_MAX_BATCH.
SCRAPE_MAX_BATCH = int(os.environ.get("ICYPEAS_SCRAPE_MAX_BATCH", "50"))

# edge_api raw-landing base — stable public URL; override via env only to pin a different host.
EDGE_API_BASE = os.environ.get("EDGE_API_BASE_URL", "https://api.edgeapi.run").rstrip("/")
WEBHOOK_ITEM_URL = f"{EDGE_API_BASE}/webhooks/icypeas/item"
WEBHOOK_BULKDONE_URL = f"{EDGE_API_BASE}/webhooks/icypeas/bulk-done"

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "psycopg[binary]>=3.2",  # ops.company_scrape_* ledger
    "requests>=2.32",        # Trigger callback
)

app = modal.App("enrichment-company-scrape", image=image)

SECRETS = [modal.Secret.from_name("hqx-postgres")]   # ops.company_scrape_* live in HQX

# ── ops DDL — verbatim mirror of the .sql sibling; applied defensively before writes (idempotent). ──
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.company_scrape_submissions (
    company_url   text        PRIMARY KEY,           -- the LinkedIn company URL (dedup / idempotency key)
    file_id       text,                               -- Icypeas bulk file id this url was submitted in
    external_id   text,                               -- what we stamped at submit (== company_url)
    batch_label   text,
    run_root      text,
    status        text        NOT NULL,               -- 'submitted' | 'submit_failed'
    submitted_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT company_scrape_submissions_status_chk CHECK (status IN ('submitted', 'submit_failed'))
);
CREATE INDEX IF NOT EXISTS company_scrape_submissions_file_idx      ON ops.company_scrape_submissions (file_id);
CREATE INDEX IF NOT EXISTS company_scrape_submissions_submitted_idx ON ops.company_scrape_submissions (submitted_at DESC);

CREATE TABLE IF NOT EXISTS ops.company_scrape_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    batch_label   text,
    run_root      text,
    requested     bigint      NOT NULL DEFAULT 0,
    skipped       bigint      NOT NULL DEFAULT 0,
    submitted     bigint      NOT NULL DEFAULT 0,
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


def _hqx_dsn() -> str:
    """Transaction-mode (Supavisor :6543) hq-x DSN — the worker holds one mostly-idle connection
    while blocked on the rate-governed gateway. Prefer HQX_DB_URL_TRANSACTION; else derive it from
    the session DSN (5432→6543). Mirror of the email-cascade worker's DSN discipline."""
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

    # prepare_threshold=None: transaction-mode pooling may land consecutive statements on different
    # backends. autocommit=True keeps each statement its own short txn so the pooler frees the backend.
    return psycopg.connect(dsn, autocommit=True, prepare_threshold=None)


def _gateway():
    return modal.Function.from_name(GATEWAY_APP, GATEWAY_FN)


def _normalize_url(raw: str | None) -> str | None:
    u = (raw or "").strip()
    return u or None


def _chunk(items: list, size: int) -> list[list]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _already_submitted(cur, urls: list[str]) -> set[str]:
    """Idempotency skip-set: URLs already terminally SUBMITTED (never re-spend a scrape credit).
    A prior 'submit_failed' is intentionally NOT skipped (retryable)."""
    if not urls:
        return set()
    try:
        cur.execute(
            "SELECT company_url FROM ops.company_scrape_submissions "
            "WHERE status = 'submitted' AND company_url = ANY(%s)",
            (urls,),
        )
        return {r[0] for r in cur.fetchall()}
    except Exception as exc:  # noqa: BLE001 — degrade skip, preserve correctness
        print(f"WARN: skip-set lookup failed ({exc}); proceeding without skip.")
        return set()


def _upsert_submission(cur, url: str, file_id: str | None, status: str,
                       batch_label: str | None, run_root: str) -> None:
    cur.execute(
        """
        INSERT INTO ops.company_scrape_submissions
            (company_url, file_id, external_id, batch_label, run_root, status, submitted_at)
        VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (company_url) DO UPDATE SET
            file_id      = EXCLUDED.file_id,
            external_id  = EXCLUDED.external_id,
            batch_label  = EXCLUDED.batch_label,
            run_root     = EXCLUDED.run_root,
            status       = EXCLUDED.status,
            submitted_at = now()
        """,
        (url, file_id, url, batch_label, run_root, status),
    )


def _record_run(cur, batch_label: str | None, run_root: str, counts: dict, status: str,
                error: str | None, started_at: dt.datetime, completed_at: dt.datetime) -> None:
    cur.execute(OPS_DDL)
    cur.execute(
        """
        INSERT INTO ops.company_scrape_runs
            (feed, batch_label, run_root, requested, skipped, submitted, batches, failed,
             status, error, started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (FEED, batch_label, run_root, counts["requested"], counts["skipped"], counts["submitted"],
         counts["batches"], counts["failed"], status, error, started_at, completed_at),
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
    counts = {"requested": 0, "skipped": 0, "submitted": 0, "batches": 0, "failed": 0}
    status, error = "error", None
    dsn = _hqx_dsn()

    try:
        # De-dup + normalize while preserving first-seen order.
        seen: set[str] = set()
        urls: list[str] = []
        for raw in (company_urls or []):
            u = _normalize_url(raw if isinstance(raw, str) else None)
            if u and u not in seen:
                seen.add(u)
                urls.append(u)
        counts["requested"] = len(urls)

        gw = _gateway()
        conn = _open_conn(dsn)
        try:
            cur = conn.cursor()
            cur.execute(OPS_DDL)  # ensure ledger exists before first write

            skip = set() if force else _already_submitted(cur, urls)
            todo = [u for u in urls if u not in skip]
            counts["skipped"] = len(urls) - len(todo)

            for batch in _chunk(todo, SCRAPE_MAX_BATCH):
                counts["batches"] += 1
                env = gw.remote(
                    urls=batch,
                    external_ids=batch,                    # externalId == company_url (correlation)
                    webhook_item_url=WEBHOOK_ITEM_URL,
                    webhook_bulkdone_url=WEBHOOK_BULKDONE_URL,
                )
                if env.get("ok") and env.get("file_id"):
                    counts["submitted"] += len(batch)
                    for u in batch:
                        _upsert_submission(cur, u, env["file_id"], "submitted", batch_label, run_root)
                else:
                    counts["failed"] += len(batch)
                    print(f"WARN: scrape submit failed for {len(batch)} urls: {env.get('error')}")
                    for u in batch:
                        _upsert_submission(cur, u, None, "submit_failed", batch_label, run_root)

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
    """Submit a chunk of LinkedIn company URLs to Icypeas /api/scrape via the gateway, record the
    submit ledger, and return terminal counts. Results land asynchronously at edge_api (webhook).
    ``company_urls`` — list of LinkedIn company profile URLs (e.g. linkedin.com/company/<slug>)."""
    return _run(company_urls, batch_label, run_id, force, trigger_callback_url)


@app.function(secrets=SECRETS, timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.company_scrape_submissions + ops.company_scrape_runs in HQX (idempotent)."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute("""
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name IN ('company_scrape_submissions','company_scrape_runs')
            ORDER BY table_name, ordinal_position
        """)
        cols: dict[str, list[str]] = {}
        for t, col in cur.fetchall():
            cols.setdefault(t, []).append(col)
    finally:
        conn.close()
    print(f"ops tables ready: { {k: len(v) for k, v in cols.items()} }")
    return {"tables": cols}


@app.function(secrets=SECRETS, timeout=60 * 5)
def verify(limit: int = 8) -> dict:
    """Read-back: latest submissions + run-state + landed-result reconciliation counts."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT company_url, file_id, status, batch_label, submitted_at
               FROM ops.company_scrape_submissions ORDER BY submitted_at DESC LIMIT %s""",
            (limit,),
        )
        scols = [d.name for d in cur.description]
        submissions = [dict(zip(scols, r)) for r in cur.fetchall()]
        cur.execute("SELECT status, count(*) FROM ops.company_scrape_submissions GROUP BY 1")
        sub_hist = {r[0]: r[1] for r in cur.fetchall()}
        # Reconciliation: how many submitted urls have a landed webhook row (best-effort; the landing
        # table may not exist yet if edge_api has not deployed the migration).
        landed = None
        try:
            cur.execute("SELECT count(DISTINCT external_id) FROM business.icypeas_webhook_events "
                        "WHERE kind = 'scrape_item'")
            landed = cur.fetchone()[0]
        except Exception as exc:  # noqa: BLE001
            landed = f"unavailable ({exc})"
        cur.execute(
            """SELECT batch_label, requested, skipped, submitted, batches, failed, status, recorded_at
               FROM ops.company_scrape_runs ORDER BY recorded_at DESC LIMIT 3""")
        rcols = [d.name for d in cur.description]
        runs = [dict(zip(rcols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    print(f"submission histogram: {sub_hist} · landed scrape_items: {landed}")
    return {"submission_histogram": sub_hist, "landed_scrape_items": landed,
            "recent_submissions": submissions, "recent_runs": runs}


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.company_scrape_* DDL (HQX)."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def verify_run(limit: int = 8) -> None:
    """Read-back assertion on the most-recent submissions + landing reconciliation."""
    import json

    print(json.dumps(verify.remote(limit), indent=2, default=str))


@app.local_entrypoint()
def run_manual(urls_json: str, batch_label: str = "manual", force: bool = False) -> None:
    """Manual run. --urls-json '["https://www.linkedin.com/company/nec-technologies"]'"""
    import json

    urls = json.loads(urls_json)
    print(json.dumps(run_company_scrape.remote(urls, batch_label=batch_label, force=force),
                     indent=2, default=str))
