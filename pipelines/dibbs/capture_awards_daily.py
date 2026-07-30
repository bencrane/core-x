"""Compute worker — DLA DIBBS daily Awards raw capture (Phase 1).

Sibling of ``capture_rfq_daily.py``. Runs as its OWN Modal app
(``dibbs-awards-pipelines``) so its deploy lifecycle never clobbers the RFQ
worker's app (``dibbs-pipelines``). Spawned by the Universal Dispatcher; no web
endpoint, no schedule decorator (Trigger.dev owns cadence:
src/trigger/dibbs_awards_daily.ts).

Phase-1 RAW ARTIFACT capture. Live probing (2026-07-29) established the DIBBS
Awards architecture — materially different from the RFQ side:

  - Awards/AwdDates.aspx?category={awddt,post} lists, per date, a link to an
    HTML VIEW page Awards/AwdRecs.aspx?Category=…&TypeSrch=cq&Value=MM-DD-YYYY —
    NOT a downloadable bulk file.
  - AwdRecs.aspx renders the day's award grid (one row per award), columns:
    Award/Basic Number · Delivery Order Number · Awardee CAGE Code ·
    Total Contract Price · Award Date · Posted Date · NSN/Part Number ·
    Nomenclature · Purchase Request · Solicitation. This grid carries the
    thesis-critical signal: per-NSN total award price + winning CAGE + the
    solicitation number that JOINS back to the RFQ index capture.
  - Some rows link a direct award-notice PDF at
    dibbs2.bsm.dla.mil/Downloads/Awards/{DDMMMYY}/{piid}.PDF; others link a
    "Delivery Order Package View" .aspx (deferred to Phase 2).
  - Those PDFs are LONG-LIVED (a single day references 01JUN21 / 06MAY24 folders
    still served) — there is NO 10-day rolling window on the awards side, so
    capture-before-destruction urgency does not apply. The value here is the
    price/CAGE/solicitation grid, captured losslessly for Phase-2 parse + join.

Two streams, both byte-identical, parse DEFERRED to Phase 2:
    records    : AwdRecs.aspx HTML per (category, date)
    award_pdf  : the direct .PDF award notices linked from the grid

Idempotent by construction (ledger diff on ops.dibbs_awards_daily_r2_ingest_runs,
UNIQUE(r2_key), upsert). No rolling window, so any PDF remainder beyond a run's
budget is swept by the next daily run.

    modal deploy pipelines/dibbs/capture_awards_daily.py
    # backfill/manual: spawn on the deployed app (never `modal run`):
    #   modal.Function.from_name("dibbs-awards-pipelines","capture_awards_daily").spawn(backfill=True)
"""

from __future__ import annotations

import os

import modal

WWW = "https://www.dibbs.bsm.dla.mil"
DL_HOST = "https://dibbs2.bsm.dla.mil"

AWD_DATES_CATEGORIES = ("awddt", "post")

BUCKET = "data-sink"
R2_PREFIX = "raw/dibbs_awards_daily"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
INTER_REQUEST_DELAY_S = 1.0  # .mil politeness: single client, low and slow

FEED = "dibbs_awards_daily"
SCRATCH_DIR = "/tmp/dibbs_awards"

# Safety ceiling on PDFs per run — the awards PDF corpus is long-lived and
# swept incrementally across daily runs, so a bounded per-run budget keeps any
# single run polite and inside the Modal timeout without losing coverage.
MAX_PDFS_PER_RUN = 4000

_MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.dibbs_awards_daily_r2_ingest_runs (
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

app = modal.App("dibbs-awards-pipelines", image=image)


# ── consent + polite HTTP ──────────────────────────────────────────────────────────
def _consent_session(host: str, goto_path: str):
    """Establish a DoD-consent session on ``host`` (cookies are host-scoped).

    GET /dodwarning.aspx?goto={target} → parse ALL hidden inputs → POST them
    back with butAgree=OK. www 302s to the target; dibbs2 returns 200 in place —
    in both cases the session cookies grant subsequent requests, so success is
    judged by the postback status and downstream content validation.
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
def _enumerate_award_dates(www_session) -> dict[tuple[str, "object"], str]:
    """{(category, date): AwdRecs_url} across awddt + post dates pages.

    Preserves each anchor's exact AwdRecs.aspx query string (Category/TypeSrch/
    Value) rather than reconstructing it.
    """
    import datetime as dt
    import re

    out: dict[tuple[str, object], str] = {}
    for cat in AWD_DATES_CATEGORIES:
        url = f"{WWW}/Awards/AwdDates.aspx?category={cat}"
        r = _polite_get(www_session, url, timeout=120)
        r.raise_for_status()
        html = r.text.replace("&amp;", "&")
        for href in re.findall(r"href=['\"]([^'\"]*AwdRecs\.aspx\?[^'\"]+)['\"]", html, re.I):
            m = re.search(r"Value=(\d{2})-(\d{2})-(\d{4})", href)
            if not m:
                continue
            d = dt.date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
            full = href if href.lower().startswith("http") else f"{WWW}{href if href.startswith('/') else '/Awards/' + href}"
            out[(cat, d)] = full
    return out


def _extract_pdf_links(html: str) -> set[str]:
    import re

    norm = html.replace("&amp;", "&")
    return set(
        re.findall(r"https://dibbs2\.bsm\.dla\.mil/Downloads/Awards/[^'\"\s<>]+\.PDF", norm, re.I)
    )


def _folder_date_from_pdf(url: str):
    """Parse the DDMMMYY archive-folder date from an award PDF URL (best effort)."""
    import datetime as dt
    import re

    m = re.search(r"/Downloads/Awards/(\d{2})([A-Z]{3})(\d{2})/", url, re.I)
    if not m:
        return None
    mon = _MONTHS.get(m.group(2).upper())
    if not mon:
        return None
    try:
        return dt.date(2000 + int(m.group(3)), mon, int(m.group(1)))
    except ValueError:
        return None


# ── validation ─────────────────────────────────────────────────────────────────────
def _validate_records(body: bytes) -> tuple[bool, str]:
    head = body[:600].lower()
    if b"dodwarning" in head:
        return False, "consent gate served instead of records page"
    if b"resource you are looking for" in head:
        return False, "404 masquerade (200 body)"
    low = body.lower()
    # Identify the awards grid by its join-key column header. "NSN/Part Number"
    # is contiguous in the raw HTML (verified across dates); the price/other
    # headers are split by nested markup, so only this marker is reliable. The
    # dates page lists only populated dates, so every valid grid carries it.
    if b"award search results" not in low:
        return False, "not an AwdRecs page (title marker absent)"
    if b"nsn/part number" not in low:
        return False, "records grid marker absent (no NSN/Part Number column)"
    return True, "ok"


def _validate_pdf(path: str) -> tuple[bool, str]:
    size = os.path.getsize(path)
    if size < 1024:
        return False, f"pdf too small ({size} bytes)"
    with open(path, "rb") as fh:
        head = fh.read(5)
        if head != b"%PDF-":
            return False, f"missing %PDF magic (got {head!r})"
        fh.seek(max(0, size - 2048))
        tail = fh.read()
    if b"%%EOF" not in tail:
        return False, "missing %%EOF trailer (truncated?)"
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
            "SELECT r2_key FROM ops.dibbs_awards_daily_r2_ingest_runs WHERE status = 'ok'"
        )
        return {r[0] for r in cur.fetchall()}


def _ledger_upsert(conn, post_date, stream, file_name, source_url, r2_key,
                   nbytes, sha256, status, detail) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.dibbs_awards_daily_r2_ingest_runs
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


def _sha256_bytes(b: bytes) -> str:
    import hashlib

    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: str) -> str:
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ── capture one award PDF (streamed) ───────────────────────────────────────────────
def _capture_pdf(dl_session, s3, conn, url: str) -> str:
    rel = url.split("/Downloads/Awards/", 1)[1]  # {DDMMMYY}/{file}.PDF
    r2_key = f"{R2_PREFIX}/award_pdf/{rel}"
    fname = rel.rsplit("/", 1)[-1]
    post_date = _folder_date_from_pdf(url)
    import datetime as dt
    from zoneinfo import ZoneInfo
    if post_date is None:
        post_date = dt.datetime.now(ZoneInfo("America/New_York")).date()
    tmp = os.path.join(SCRATCH_DIR, fname)
    try:
        with _polite_get(dl_session, url, stream=True, timeout=(60, 1800)) as resp:
            resp.raise_for_status()
            with open(tmp, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    if chunk:
                        fh.write(chunk)
    except Exception as exc:  # noqa: BLE001
        _ledger_upsert(conn, post_date, "award_pdf", fname, url, r2_key,
                       None, None, "http_error", str(exc)[:500])
        return "http_error"
    try:
        nbytes = os.path.getsize(tmp)
        valid, detail = _validate_pdf(tmp)
        if not valid:
            _ledger_upsert(conn, post_date, "award_pdf", fname, url, r2_key,
                           nbytes, None, "validation_failed", detail)
            return "validation_failed"
        sha = _sha256_file(tmp)
        s3.upload_file(tmp, BUCKET, r2_key)
        _ledger_upsert(conn, post_date, "award_pdf", fname, url, r2_key,
                       nbytes, sha, "ok", "ok")
        return "ok"
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


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


def _business_days_between(d0, d1) -> int:
    import datetime as dt

    n, d = 0, d0
    while d < d1:
        d += dt.timedelta(days=1)
        if d.weekday() < 5:
            n += 1
    return n


# ── the worker ─────────────────────────────────────────────────────────────────────
@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=4 * 60 * 60,
    memory=4096,
    cpu=2.0,
)
def capture_awards_daily(
    trigger_callback_url: str | None = None,
    backfill: bool = False,
) -> dict:
    """Enumerate award dates → capture AwdRecs HTML per (category,date) → harvest
    linked award PDFs → callback. Backfill and daily capture share this path
    (the ledger diff is the only state)."""
    import datetime as dt
    from zoneinfo import ZoneInfo

    os.makedirs(SCRATCH_DIR, exist_ok=True)
    today_et = dt.datetime.now(ZoneInfo("America/New_York")).date()

    final_status = "error"
    error_text: str | None = None
    counts = {"records_ok": 0, "records_failed": 0, "pdf_ok": 0, "pdf_failed": 0,
              "pdf_http_error": 0, "records_http_error": 0,
              "skipped_current_day": 0, "already_landed": 0, "pdf_budget_deferred": 0}
    bytes_landed = 0
    pdf_urls: set[str] = set()

    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(OPS_DDL)
        conn.commit()

        s3 = _s3_client()
        www = _consent_session(WWW, "/Awards/AwdDates.aspx?category=awddt")
        dl = _consent_session(DL_HOST, "/Downloads/Awards/")

        dates = _enumerate_award_dates(www)
        ok_keys = _ledger_ok_keys(conn)
        print(f"award dates listed: {len(dates)} (category,date) pairs; "
              f"ledger has {len(ok_keys)} ok keys")

        # ---- records stream: one AwdRecs HTML per (category, date) ----
        for (cat, d) in sorted(dates, key=lambda t: (t[0], t[1])):
            if d >= today_et:  # today's grid may still be accruing
                counts["skipped_current_day"] += 1
                continue
            url = dates[(cat, d)]
            r2_key = f"{R2_PREFIX}/records/{cat}/{d.year}/awd_{d:%y%m%d}.html"
            already = r2_key in ok_keys
            # Always fetch (to discover PDF links); upload only when new.
            try:
                r = _polite_get(www, url, timeout=120)
                r.raise_for_status()
                body = r.content
            except Exception as exc:  # noqa: BLE001
                if not already:
                    _ledger_upsert(conn, d, "records", f"awd_{d:%y%m%d}.html", url, r2_key,
                                   None, None, "http_error", str(exc)[:500])
                    counts["records_http_error"] += 1
                continue
            valid, detail = _validate_records(body)
            if not valid:
                if not already:
                    _ledger_upsert(conn, d, "records", f"awd_{d:%y%m%d}.html", url, r2_key,
                                   len(body), None, "validation_failed", detail)
                    counts["records_failed"] += 1
                continue
            pdf_urls |= _extract_pdf_links(body.decode("utf-8", errors="replace"))
            if already:
                counts["already_landed"] += 1
            else:
                s3.put_object(Bucket=BUCKET, Key=r2_key, Body=body)
                _ledger_upsert(conn, d, "records", f"awd_{d:%y%m%d}.html", url, r2_key,
                               len(body), _sha256_bytes(body), "ok", "ok")
                counts["records_ok"] += 1
                bytes_landed += len(body)

        # ---- award_pdf stream: dedup'd PDFs referenced by the grids ----
        pending_pdfs = [
            u for u in sorted(pdf_urls)
            if f"{R2_PREFIX}/award_pdf/{u.split('/Downloads/Awards/', 1)[1]}" not in ok_keys
        ]
        print(f"unique award PDFs referenced: {len(pdf_urls)}; pending: {len(pending_pdfs)}")
        if len(pending_pdfs) > MAX_PDFS_PER_RUN:
            counts["pdf_budget_deferred"] = len(pending_pdfs) - MAX_PDFS_PER_RUN
            print(f"WARN: PDF budget {MAX_PDFS_PER_RUN} < pending {len(pending_pdfs)}; "
                  f"{counts['pdf_budget_deferred']} deferred to next run (no rolling window).")
            pending_pdfs = pending_pdfs[:MAX_PDFS_PER_RUN]
        for u in pending_pdfs:
            status = _capture_pdf(dl, s3, conn, u)
            if status == "ok":
                counts["pdf_ok"] += 1
            elif status == "validation_failed":
                counts["pdf_failed"] += 1
            else:
                counts["pdf_http_error"] += 1

        # ---- consistency: newest ok records date within 2 business days ----
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(post_date) FROM ops.dibbs_awards_daily_r2_ingest_runs "
                "WHERE status='ok' AND stream='records'"
            )
            newest = cur.fetchone()[0]
        if newest is None:
            raise RuntimeError("no ok records captures exist after run — capture failed")
        lag = _business_days_between(newest, today_et)
        print(f"newest ok records date={newest} ({lag} business days behind today ET)")
        if lag > 2:
            raise RuntimeError(
                f"consistency check failed: newest ok records {newest} is {lag} business days old"
            )
        # Records stream must be clean; PDF failures are tolerated (long-lived,
        # swept next run) but surfaced.
        if counts["records_failed"] > 0 or counts["records_http_error"] > 0:
            raise RuntimeError(f"records capture completed with failures: {counts}")

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
            "error": error_text,
        }
        print(f"terminal: {payload}")
        _post_callback(trigger_callback_url, payload)

    if final_status != "success":
        raise RuntimeError(f"dibbs awards capture failed: {error_text}")
    return {"feed": FEED, "status": final_status, "files": counts,
            "bytes_landed": int(bytes_landed)}


def apply_state_schema() -> None:
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(OPS_DDL)
    conn.commit()
    conn.close()
    print("Applied ops.dibbs_awards_daily_r2_ingest_runs schema.")
