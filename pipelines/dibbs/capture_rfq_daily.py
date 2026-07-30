"""Compute worker — DLA DIBBS daily RFQ/Awards raw artifact capture (Phase 1).

Part of the ``dibbs-pipelines`` Modal app. NOT directly exposed — spawned by the
Universal Dispatcher (core/modal_dispatcher.py). No web endpoint, no schedule
decorator (Trigger.dev v4 owns cadence: src/trigger/dibbs_rfq_daily.ts).

Phase-1 RAW ARTIFACT capture: DIBBS publishes daily acquisition artifacts and
destroys them after ~10 business days (rolling window). This worker lands them
byte-identical in R2 under a raw prefix before they roll off. NO parsing, NO
Lance, NO DuckDB transform in this phase (Phase 2 decodes against the captured
refs_help layout docs).

    RFQ side   : in{yymmdd}.txt (fixed-width index), ca{yymmdd}.zip
                 (solicitation PDFs, 245-400 MB), bq{yymmdd}.zip (batch quote)
    Awards side: daily files discovered from Awards/AwdDates.aspx (per-NSN
                 winning unit prices unavailable at FPDS grain)
    refs_help  : one-time mirror of the DIBBS RoboHelp topic tree (layout doc)

Upstream facts baked in (verified 2026-07-29, see directive Evidence):
  - Both hosts (www / dibbs2) gate on a DoD-consent ASP.NET postback; cookies
    are host-scoped so the consent flow runs per host.
  - Server rejects HEAD (404) and ignores Range (200 full stream) — full GET
    only, stream to disk.
  - Some 404s return a ~103-byte HTML page with 200-shaped headers — validate
    by CONTENT (PK magic / ASCII + solicitation pattern), never status alone.
  - Current-day files are incomplete (finalized next day). HARD filter:
    only post_date < today in US Eastern is eligible.
  - Weekends/holidays produce no files; absence on a non-business day is not
    a failure.

Idempotent by construction: work = (dates pages ∪) minus ledger rows with
status='ok' (ops.dibbs_rfq_daily_r2_ingest_runs, UNIQUE(r2_key), upsert).

    modal deploy pipelines/dibbs/capture_rfq_daily.py
    # backfill/manual: spawn on the deployed app (never `modal run`):
    #   modal.Function.from_name("dibbs-pipelines","capture_rfq_daily").spawn(backfill=True)
"""

from __future__ import annotations

import os

import modal

WWW = "https://www.dibbs.bsm.dla.mil"
DL_HOST = "https://dibbs2.bsm.dla.mil"

RFQ_DATES_CATEGORIES = ("recent", "issue", "close")
AWD_DATES_CATEGORIES = ("awddt", "post")

BUCKET = "data-sink"
R2_PREFIX = "raw/dibbs_rfq_daily"

REFS_HELP_ROOT = f"{WWW}/refs/help/"
REFS_HELP_START = "DIBBSHelp.htm"
REFS_HELP_MAX_FILES = 500

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
INTER_REQUEST_DELAY_S = 1.0  # .mil politeness: single client, low and slow

FEED = "dibbs_rfq_daily"
SCRATCH_DIR = "/tmp/dibbs"

# 13-char solicitation number: SPE + 3 alnum + 2-digit FY + type letter + serial
SOL_RX = r"^SPE[A-Z0-9]{3}\d{2}[A-Z]\d{4}"

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.dibbs_rfq_daily_r2_ingest_runs (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  post_date DATE NOT NULL,
  stream TEXT NOT NULL,
  file_name TEXT NOT NULL,
  source_url TEXT NOT NULL,
  r2_key TEXT NOT NULL,
  bytes BIGINT,
  sha256 TEXT,
  status TEXT NOT NULL CHECK (status IN ('ok','validation_failed','http_error')),
  detail TEXT,
  UNIQUE (r2_key)
);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "requests>=2.32",
    "boto3>=1.34",
    "psycopg[binary]>=3.2",
)

app = modal.App("dibbs-pipelines", image=image)


# ── consent + polite HTTP ──────────────────────────────────────────────────────────
def _consent_session(host: str, goto_path: str):
    """Establish a DoD-consent session on ``host`` (cookies are host-scoped).

    GET /dodwarning.aspx?goto={target} → parse ALL hidden inputs → POST them
    back with butAgree=OK. www 302s to the target; dibbs2 returns 200 in place
    (verified in-run 2026-07-29) — in both cases the session cookies grant all
    subsequent requests, so success is judged by the postback status alone.
    Per-file content validation is the true downstream gate.
    """
    import re
    import urllib.parse

    import requests

    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    warn_url = f"{host}/dodwarning.aspx?goto={urllib.parse.quote(goto_path, safe='')}"
    r = s.get(warn_url, timeout=60)
    r.raise_for_status()
    fields: dict[str, str] = {}
    for tag in re.findall(r"<input[^>]+>", r.text):
        if "hidden" not in tag:
            continue
        n = re.search(r"name=[\"']([^\"']+)[\"']", tag)
        v = re.search(r"value=[\"']([^\"']*)[\"']", tag)
        if n:
            fields[n.group(1)] = v.group(1) if v else ""
    if not fields:
        raise RuntimeError(f"consent page on {host} exposed no hidden fields")
    fields["butAgree"] = "OK"
    r2 = s.post(warn_url, data=fields, timeout=60, allow_redirects=True)
    r2.raise_for_status()
    return s


def _polite_get(session, url: str, **kw):
    import time

    time.sleep(INTER_REQUEST_DELAY_S)
    return session.get(url, **kw)


# ── dates-page enumeration ─────────────────────────────────────────────────────────
def _parse_dates_page(html: str, stream_for: "callable") -> dict[str, dict]:
    """Row-scoped parse of a DIBBS dates page.

    Each <tr> carries a post date (MM-DD-YYYY) and anchors to
    https://dibbs2.../Downloads/... files. Returns {file_name: {url, post_date,
    stream}}. Stream is decided by ``stream_for(file_name)``.
    """
    import datetime as dt
    import re

    out: dict[str, dict] = {}
    for row in re.split(r"<tr[\s>]", html, flags=re.I):
        m_date = re.search(r"(\d{2})-(\d{2})-(\d{4})", row)
        anchors = re.findall(
            r"href=['\"](https://dibbs2\.bsm\.dla\.mil/Downloads/[^'\"]+)['\"]", row, re.I
        )
        if not anchors:
            continue
        for url in anchors:
            fname = url.rsplit("/", 1)[-1]
            post_date = None
            m_f = re.search(r"(\d{6})", fname)
            if m_f:
                d6 = m_f.group(1)
                try:
                    post_date = dt.date(2000 + int(d6[:2]), int(d6[2:4]), int(d6[4:6]))
                except ValueError:
                    post_date = None
            if post_date is None and m_date:
                post_date = dt.date(
                    int(m_date.group(3)), int(m_date.group(1)), int(m_date.group(2))
                )
            if post_date is None:
                continue
            stream = stream_for(fname)
            if stream is None:
                continue
            out[fname] = {"url": url, "post_date": post_date, "stream": stream}
    return out


def _rfq_stream_for(fname: str):
    f = fname.lower()
    if f.startswith("in") and f.endswith(".txt"):
        return "index"
    if f.startswith("ca") and f.endswith(".zip"):
        return "solicitation_zip"
    if f.startswith("bq") and f.endswith(".zip"):
        return "batch_quote_zip"
    return None


def _enumerate_work(www_session) -> dict[str, dict]:
    """Union of (file → meta) across all RFQ + Awards dates pages."""
    work: dict[str, dict] = {}
    for cat in RFQ_DATES_CATEGORIES:
        url = f"{WWW}/RFQ/RFQDates.aspx?category={cat}"
        r = _polite_get(www_session, url, timeout=120)
        r.raise_for_status()
        work.update(_parse_dates_page(r.text, _rfq_stream_for))
    for cat in AWD_DATES_CATEGORIES:
        url = f"{WWW}/Awards/AwdDates.aspx?category={cat}"
        r = _polite_get(www_session, url, timeout=120)
        r.raise_for_status()
        # Awards-side: every dibbs2 Downloads file on these pages is awards_zip
        # regardless of extension (naming discovered in-run per directive §4).
        work.update(_parse_dates_page(r.text, lambda f: "awards_zip"))
    return work


# ── validation ─────────────────────────────────────────────────────────────────────
def _validate_file(path: str, stream: str, fname: str) -> tuple[bool, str]:
    import re
    import zipfile

    size = os.path.getsize(path)
    with open(path, "rb") as fh:
        head = fh.read(4096)

    is_zip_name = fname.lower().endswith(".zip")
    if stream == "index" or (stream == "awards_zip" and not is_zip_name):
        try:
            text = head.decode("ascii")
        except UnicodeDecodeError:
            return False, "not ASCII (404-masquerade or binary)"
        if stream == "index":
            with open(path, "rb") as fh:
                nlines = sum(1 for _ in fh)
            if nlines < 200:
                return False, f"only {nlines} lines (<200)"
            if not re.match(SOL_RX, text[:13]):
                return False, f"line 1 does not match solicitation pattern: {text[:13]!r}"
        return True, "ok"

    # zip families
    if head[:2] != b"PK":
        return False, f"missing PK magic (got {head[:8]!r}, {size} bytes)"
    if stream == "solicitation_zip":
        if size < 50 * 1024 * 1024:
            return False, f"solicitation zip only {size} bytes (<50MB)"
        try:
            with zipfile.ZipFile(path) as zf:
                if not zf.namelist():
                    return False, "zip central directory empty"
        except zipfile.BadZipFile as exc:
            return False, f"central directory unreadable (truncated?): {exc}"
    return True, "ok"


# ── R2 + ledger plumbing ───────────────────────────────────────────────────────────
def _s3_client():
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    cfg = Config(
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=cfg,
    )


def _pg_conn():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set (hqx-postgres secret).")
    return psycopg.connect(dsn)


def _ledger_ok_keys(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT r2_key FROM ops.dibbs_rfq_daily_r2_ingest_runs WHERE status = 'ok'"
        )
        return {r[0] for r in cur.fetchall()}


def _ledger_upsert(conn, post_date, stream, file_name, source_url, r2_key,
                   nbytes, sha256, status, detail) -> None:
    # A retry that succeeds MUST overwrite a prior failed row (directive §6).
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.dibbs_rfq_daily_r2_ingest_runs
                (post_date, stream, file_name, source_url, r2_key,
                 bytes, sha256, status, detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (r2_key) DO UPDATE SET
                run_started_at = now(),
                post_date = EXCLUDED.post_date,
                stream = EXCLUDED.stream,
                file_name = EXCLUDED.file_name,
                source_url = EXCLUDED.source_url,
                bytes = EXCLUDED.bytes,
                sha256 = EXCLUDED.sha256,
                status = EXCLUDED.status,
                detail = EXCLUDED.detail
            """,
            (post_date, stream, file_name, source_url, r2_key,
             nbytes, sha256, status, detail),
        )
    conn.commit()


def _sha256_file(path: str) -> str:
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ── capture one file ───────────────────────────────────────────────────────────────
def _capture_file(dl_session, s3, conn, meta: dict, fname: str) -> str:
    """Download → validate → upload → ledger. Returns terminal status."""
    stream, url, post_date = meta["stream"], meta["url"], meta["post_date"]
    r2_key = f"{R2_PREFIX}/{stream}/{post_date.year}/{fname}"
    tmp = os.path.join(SCRATCH_DIR, fname)
    nbytes = None
    sha = None
    try:
        with _polite_get(dl_session, url, stream=True, timeout=(60, 3600)) as resp:
            resp.raise_for_status()
            with open(tmp, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    if chunk:
                        fh.write(chunk)
    except Exception as exc:  # noqa: BLE001
        _ledger_upsert(conn, post_date, stream, fname, url, r2_key,
                       None, None, "http_error", str(exc)[:500])
        return "http_error"

    try:
        nbytes = os.path.getsize(tmp)
        with open(tmp, "rb") as fh:
            head = fh.read(4096)
        if b"dodwarning" in head.lower():
            _ledger_upsert(conn, post_date, stream, fname, url, r2_key,
                           nbytes, None, "http_error", "consent gate served instead of file")
            return "http_error"
        valid, detail = _validate_file(tmp, stream, fname)
        if not valid:
            _ledger_upsert(conn, post_date, stream, fname, url, r2_key,
                           nbytes, None, "validation_failed", detail)
            return "validation_failed"
        sha = _sha256_file(tmp)
        s3.upload_file(tmp, BUCKET, r2_key)
        _ledger_upsert(conn, post_date, stream, fname, url, r2_key,
                       nbytes, sha, "ok", "ok")
        print(f"  ok {stream}/{fname}: {nbytes:,} bytes sha256={sha[:12]}…")
        return "ok"
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


# ── refs_help RoboHelp mirror ──────────────────────────────────────────────────────
def _mirror_refs_help(www_session, s3, conn, ok_keys: set[str]) -> dict[str, int]:
    """Bounded same-host path-prefix crawl of the DIBBS RoboHelp topic tree."""
    import datetime as dt
    import hashlib
    import re
    import urllib.parse
    from zoneinfo import ZoneInfo

    capture_date = dt.datetime.now(ZoneInfo("America/New_York")).date()
    counts = {"ok": 0, "http_error": 0, "skipped": 0}
    seen: set[str] = set()
    queue = [urllib.parse.urljoin(REFS_HELP_ROOT, REFS_HELP_START)]

    while queue and len(seen) < REFS_HELP_MAX_FILES:
        url = queue.pop(0)
        if url in seen or not url.startswith(REFS_HELP_ROOT):
            continue
        seen.add(url)
        rel = url[len(REFS_HELP_ROOT):].split("#", 1)[0].split("?", 1)[0]
        if not rel:
            continue
        r2_key = f"{R2_PREFIX}/refs_help/{rel}"
        if r2_key in ok_keys:
            counts["skipped"] += 1
            # still crawl its links on a fresh fetch below? No — skip refetch;
            # link discovery loss is acceptable on re-runs (re-run tolerable).
            continue
        try:
            r = _polite_get(www_session, url, timeout=60)
            r.raise_for_status()
            body = r.content
        except Exception as exc:  # noqa: BLE001
            _ledger_upsert(conn, capture_date, "refs_help", rel, url, r2_key,
                           None, None, "http_error", str(exc)[:500])
            counts["http_error"] += 1
            continue
        s3.put_object(Bucket=BUCKET, Key=r2_key, Body=body)
        _ledger_upsert(conn, capture_date, "refs_help", rel, url, r2_key,
                       len(body), hashlib.sha256(body).hexdigest(), "ok", "ok")
        counts["ok"] += 1
        if rel.lower().endswith((".htm", ".html")):
            text = body.decode("utf-8", errors="replace")
            for link in re.findall(r"(?:href|src)=[\"']([^\"'#]+)[\"']", text, re.I):
                if link.lower().startswith(("http://", "https://", "mailto:", "javascript:")):
                    if not link.startswith(REFS_HELP_ROOT):
                        continue
                nxt = urllib.parse.urljoin(url, link)
                if nxt.startswith(REFS_HELP_ROOT) and nxt not in seen:
                    queue.append(nxt)
    return counts


# ── cadence-consistency helper ─────────────────────────────────────────────────────
def _business_days_between(d0, d1) -> int:
    import datetime as dt

    n, d = 0, d0
    while d < d1:
        d += dt.timedelta(days=1)
        if d.weekday() < 5:
            n += 1
    return n


# ── callback ───────────────────────────────────────────────────────────────────────
def _post_callback(url, payload, attempts: int = 3) -> None:
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


# ── the worker ─────────────────────────────────────────────────────────────────────
@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=4 * 60 * 60,   # backfill moves 3-6 GB; daily ca zip alone is 250-400 MB
    memory=4096,
    cpu=2.0,
)
def capture_rfq_daily(
    trigger_callback_url: str | None = None,
    backfill: bool = False,
    mirror_help: bool = True,
) -> dict:
    """Enumerate dates pages → diff vs ledger → capture pending files → callback.

    Backfill and daily capture are the same code path (the ledger diff is the
    only state); ``backfill`` only widens logging expectations. ``mirror_help``
    runs the one-time refs_help mirror (idempotent — already-ok keys skipped).
    """
    import datetime as dt
    from zoneinfo import ZoneInfo

    os.makedirs(SCRATCH_DIR, exist_ok=True)
    started = dt.datetime.now(dt.timezone.utc)
    today_et = dt.datetime.now(ZoneInfo("America/New_York")).date()

    final_status = "error"
    error_text: str | None = None
    counts = {"ok": 0, "validation_failed": 0, "http_error": 0, "skipped_current_day": 0,
              "already_landed": 0}
    bytes_landed = 0
    help_counts: dict[str, int] = {}

    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(OPS_DDL)
        conn.commit()

        s3 = _s3_client()
        # host-scoped consent sessions
        www = _consent_session(WWW, "/rfq/rfqdates.aspx?category=recent")
        dl = _consent_session(DL_HOST, "/Downloads/RFQ/Archive/")

        work = _enumerate_work(www)
        ok_keys = _ledger_ok_keys(conn)
        print(f"dates pages list {len(work)} files; ledger has {len(ok_keys)} ok keys")

        for fname in sorted(work):
            meta = work[fname]
            # HARD filter: current-day files are incomplete upstream (§2.6).
            if meta["post_date"] >= today_et:
                counts["skipped_current_day"] += 1
                continue
            r2_key = f"{R2_PREFIX}/{meta['stream']}/{meta['post_date'].year}/{fname}"
            if r2_key in ok_keys:
                counts["already_landed"] += 1
                continue
            status = _capture_file(dl, s3, conn, meta, fname)
            counts[status] += 1
            if status == "ok":
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT bytes FROM ops.dibbs_rfq_daily_r2_ingest_runs WHERE r2_key=%s",
                        (r2_key,),
                    )
                    row = cur.fetchone()
                    bytes_landed += row[0] or 0

        if mirror_help:
            help_counts = _mirror_refs_help(www, s3, conn, ok_keys)
            print(f"refs_help mirror: {help_counts}")

        # Cadence-consistency check: newest ok RFQ-index capture must be within
        # 2 business days of the run date (weekend/holiday tolerant).
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(post_date) FROM ops.dibbs_rfq_daily_r2_ingest_runs "
                "WHERE status='ok' AND stream='index'"
            )
            newest = cur.fetchone()[0]
        if newest is None:
            raise RuntimeError("no ok index captures exist after run — capture failed")
        lag = _business_days_between(newest, today_et)
        print(f"newest ok index post_date={newest} ({lag} business days behind today ET)")
        if lag > 2:
            raise RuntimeError(
                f"consistency check failed: newest ok index {newest} is {lag} business days old"
            )
        if counts["validation_failed"] > 0 or counts["http_error"] > 0:
            raise RuntimeError(f"capture completed with failures: {counts}")

        final_status = "success"
        conn.close()
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error_text = str(exc)
        final_status = "error"
    finally:
        payload = {
            "status": final_status,
            "feed": FEED,
            "files": counts,
            "bytes_landed": int(bytes_landed),
            "refs_help": help_counts,
            "error": error_text,
        }
        print(f"terminal: {payload}")
        _post_callback(trigger_callback_url, payload)

    if final_status != "success":
        raise RuntimeError(f"dibbs capture failed: {error_text}")
    return {"feed": FEED, "status": final_status, "files": counts,
            "bytes_landed": int(bytes_landed)}


def apply_state_schema() -> None:
    """Local one-off: apply the ledger DDL via Doppler-injected HQX_DB_URL_POOLED."""
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(OPS_DDL)
    conn.commit()
    conn.close()
    print("Applied ops.dibbs_rfq_daily_r2_ingest_runs schema.")
