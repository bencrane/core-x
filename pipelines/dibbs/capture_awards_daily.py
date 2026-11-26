"""Compute worker — DLA DIBBS daily Awards raw capture (Phase 1).

Sibling of ``capture_rfq_daily.py``. Runs as its OWN Modal app
(``dibbs-awards-pipelines``) so its deploy lifecycle never clobbers the RFQ
worker's app (``dibbs-pipelines``). Spawned by the Universal Dispatcher; no web
endpoint, no schedule decorator (Trigger.dev owns cadence:
src/trigger/dibbs_awards_daily.ts).

Phase-1 RAW ARTIFACT capture. Live probing established the DIBBS Awards
architecture — materially different from the RFQ side:

  - Awards/AwdDates.aspx?category={awddt,post} lists, per date, a link to an
    HTML VIEW page Awards/AwdRecs.aspx?Category=…&TypeSrch=cq&Value=MM-DD-YYYY —
    NOT a downloadable bulk file.
  - AwdRecs.aspx renders the day's award grid paginated at 50 rows per page
    via ASP.NET WebForms GridView postbacks:
      __doPostBack('ctl00$cph1$grdAwardSearch', 'Page$2' .. 'Page$N')
    in 10-page pager windows (1..10 + Page$11 '...', 11..20 + Page$21 '...', etc.).
    Because AwdRecs.aspx sorts by Award/Basic Number Ascending, Page 1 only
    contains early prefixes (D/F/H/M/N/SP0/SP4), while DLA supply contracts
    (SPE1/SPE2/SPE3/SPE4/SPE7/SPE8) sit on Pages 5..N. Capturing ALL pages
    1..N is therefore mission-critical.
  - Page 1 is stored at:
      raw/dibbs_awards_daily/records/{cat}/{YYYY}/awd_{YYMMDD}.html
    with ledger detail='ok:pages={N}:records={R}'.
  - Pages 2..N are stored at:
      raw/dibbs_awards_daily/records/{cat}/{YYYY}/awd_{YYMMDD}_p{PPP}.html
    with row-number validation (first row on Page P must equal (P-1)*50 + 1).

Two streams, both byte-identical, parse DEFERRED to Phase 2:
    records    : AwdRecs.aspx HTML per (category, date, page)
    award_pdf  : the direct .PDF award notices linked from the grids

Idempotent by construction (ledger diff on ops.dibbs_awards_daily_r2_ingest_runs,
UNIQUE(r2_key), upsert).

    modal deploy pipelines/dibbs/capture_awards_daily.py
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
INTER_REQUEST_DELAY_S = 1.0  # .mil politeness for date-enumeration / PDF downloads
PAGE_POST_DELAY_S = 0.25     # polite pause between GridView postbacks on same session
DATE_WORKERS = 6             # independent ASP.NET sessions across (category, date) pairs

FEED = "dibbs_awards_daily"
SCRATCH_DIR = "/tmp/dibbs_awards"

# Safety ceilings per run so any single run stays inside Modal's 4h timeout
# while checkpointing every page and PDF immediately to R2 + Postgres.
MAX_RECORD_CAPTURE_WALL_S = 150 * 60  # 2.5 hours max spent on records pagination per run
MAX_PDFS_PER_RUN = 2500

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
    """Establish a DoD-consent session on ``host`` (cookies are host-scoped)."""
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
        if "hidden" not in tag.lower():
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


def _polite_get(session, url: str, delay_s: float = INTER_REQUEST_DELAY_S, **kw):
    import time

    if delay_s > 0:
        time.sleep(delay_s)
    return session.get(url, **kw)


def _extract_hidden_fields(html: str) -> dict[str, str]:
    """Extract all hidden <input> fields (including chunked __VIEWSTATE* and __EVENTVALIDATION)."""
    import re

    fields: dict[str, str] = {}
    for tag in re.findall(r"<input[^>]+>", html):
        if "hidden" not in tag.lower():
            continue
        n = re.search(r"name=[\"']([^\"']+)[\"']", tag)
        v = re.search(r"value=[\"']([^\"']*)[\"']", tag)
        if n:
            fields[n.group(1)] = v.group(1) if v else ""
    return fields


def _parse_records_found(html: str) -> int | None:
    """Parse 'Records Found: <strong> N</strong>' from AwdRecs.aspx HTML."""
    import re

    m = re.search(
        r'id=["\']ctl00_cph1_lblRecCount["\'][^>]*>Records Found:\s*<strong>\s*(\d+)\s*</strong>',
        html,
        re.I,
    )
    if not m:
        return None
    return int(m.group(1))


def _parse_row_nums(html: str) -> list[int]:
    """Return all integer row numbers rendered in ctl00_cph1_grdAwardSearch."""
    import re

    return [
        int(x)
        for x in re.findall(
            r'id=["\']ctl00_cph1_grdAwardSearch_ctl\d+_lblRowNum["\'][^>]*>\s*(\d+)\s*<',
            html,
        )
    ]


def _post_grid_page(session, url: str, base_fields: dict[str, str], page_num: int) -> bytes:
    """POST a GridView Page$N postback to AwdRecs.aspx with retries."""
    import time

    data = dict(base_fields)
    data["__EVENTTARGET"] = "ctl00$cph1$grdAwardSearch"
    data["__EVENTARGUMENT"] = f"Page${page_num}"
    data["ctl00$ddlNavigation"] = ""
    data["ctl00$ddlGotoDB"] = ""
    data["ctl00$txtValue"] = ""

    last_exc = None
    for attempt in range(3):
        if PAGE_POST_DELAY_S > 0:
            time.sleep(PAGE_POST_DELAY_S if attempt == 0 else 2.0 * (attempt + 1))
        try:
            r = session.post(url, data=data, timeout=120)
            r.raise_for_status()
            return r.content
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
    raise last_exc  # type: ignore[misc]


# ── dates-page enumeration ─────────────────────────────────────────────────────────
def _enumerate_award_dates(www_session) -> dict[tuple[str, "object"], str]:
    """{(category, date): AwdRecs_url} across awddt + post dates pages."""
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
def _validate_records(body: bytes, expected_first_row: int | None = None) -> tuple[bool, str]:
    head = body[:600].lower()
    if b"dodwarning" in head:
        return False, "consent gate served instead of records page"
    if b"resource you are looking for" in head:
        return False, "404 masquerade (200 body)"
    low = body.lower()
    if b"award search results" not in low:
        return False, "not an AwdRecs page (title marker absent)"
    if b"nsn/part number" not in low:
        return False, "records grid marker absent (no NSN/Part Number column)"
    if expected_first_row is not None:
        text = body.decode("utf-8", errors="replace")
        rnums = _parse_row_nums(text)
        if not rnums:
            return False, f"expected first row {expected_first_row} but no rows found"
        if rnums[0] != expected_first_row:
            return False, f"expected first row {expected_first_row}, got {rnums[0]}"
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


def _ledger_ok_map(conn) -> dict[str, str]:
    """Return {r2_key: detail} for all status='ok' rows."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT r2_key, COALESCE(detail, 'ok') FROM ops.dibbs_awards_daily_r2_ingest_runs WHERE status = 'ok'"
        )
        return {r[0]: r[1] for r in cur.fetchall()}


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


def _page_r2_key(cat: str, d, page_num: int) -> tuple[str, str]:
    """Return (r2_key, file_name) for page_num (1-indexed)."""
    if page_num == 1:
        fname = f"awd_{d:%y%m%d}.html"
    else:
        fname = f"awd_{d:%y%m%d}_p{page_num:03d}.html"
    return f"{R2_PREFIX}/records/{cat}/{d.year}/{fname}", fname


def _parse_detail_pages(detail: str) -> int | None:
    """Extract total_pages from 'ok:pages=98:records=4902' if present."""
    import re

    m = re.search(r"pages=(\d+)", detail or "")
    return int(m.group(1)) if m else None


def _capture_date_all_pages(
    cat: str,
    d,
    url: str,
    ok_map: dict[str, str],
    deadline_monotonic: float,
) -> dict:
    """Capture Page 1 .. Page N for a single (cat, d) using an independent session."""
    import time

    res = {
        "records_ok": 0,
        "records_failed": 0,
        "records_http_error": 0,
        "already_landed": 0,
        "pages_deferred": 0,
        "bytes_landed": 0,
        "pdf_urls": set(),
    }

    p1_key, p1_fname = _page_r2_key(cat, d, 1)
    p1_detail = ok_map.get(p1_key)
    known_total_pages = _parse_detail_pages(p1_detail) if p1_detail else None

    s3 = _s3_client()
    conn = _pg_conn()

    try:
        # Fast path: if Page 1 is in R2, check if we already know total_pages (or read Page 1 from R2 to learn it)
        if p1_detail is not None and known_total_pages is None:
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=p1_key)
                body1 = obj["Body"].read()
                text1 = body1.decode("utf-8", errors="replace")
                res["pdf_urls"] |= _extract_pdf_links(text1)
                rec_cnt = _parse_records_found(text1)
                if rec_cnt is not None:
                    known_total_pages = max(1, (rec_cnt + 49) // 50)
                    new_detail = f"ok:pages={known_total_pages}:records={rec_cnt}"
                    _ledger_upsert(
                        conn, d, "records", p1_fname, url, p1_key,
                        len(body1), _sha256_bytes(body1), "ok", new_detail
                    )
                    ok_map[p1_key] = new_detail
            except Exception as exc:  # noqa: BLE001
                print(f"WARN: failed reading existing R2 Page 1 {p1_key}: {exc}")

        # Check if all pages 1..known_total_pages are already in ok_map
        if known_total_pages is not None:
            missing_pages = [
                p for p in range(1, known_total_pages + 1)
                if _page_r2_key(cat, d, p)[0] not in ok_map
            ]
            if not missing_pages:
                res["already_landed"] += known_total_pages
                return res

        if time.monotonic() >= deadline_monotonic:
            res["pages_deferred"] += (known_total_pages or 1)
            return res

        # We have missing pages for (cat, d) — open an independent consent session and fetch Page 1
        www = _consent_session(WWW, f"/Awards/AwdDates.aspx?category={cat}")
        try:
            r1 = _polite_get(www, url, delay_s=0.5, timeout=120)
            r1.raise_for_status()
            body1 = r1.content
        except Exception as exc:  # noqa: BLE001
            if p1_key not in ok_map:
                _ledger_upsert(conn, d, "records", p1_fname, url, p1_key,
                               None, None, "http_error", str(exc)[:500])
                res["records_http_error"] += 1
            return res

        valid1, detail1 = _validate_records(body1, expected_first_row=1)
        if not valid1:
            if p1_key not in ok_map:
                _ledger_upsert(conn, d, "records", p1_fname, url, p1_key,
                               len(body1), None, "validation_failed", detail1)
                res["records_failed"] += 1
            return res

        text1 = body1.decode("utf-8", errors="replace")
        res["pdf_urls"] |= _extract_pdf_links(text1)
        rec_cnt = _parse_records_found(text1) or 50
        total_pages = max(1, (rec_cnt + 49) // 50)
        p1_meta_detail = f"ok:pages={total_pages}:records={rec_cnt}"

        if p1_key in ok_map:
            res["already_landed"] += 1
            if _parse_detail_pages(ok_map.get(p1_key, "")) is None:
                _ledger_upsert(conn, d, "records", p1_fname, url, p1_key,
                               len(body1), _sha256_bytes(body1), "ok", p1_meta_detail)
                ok_map[p1_key] = p1_meta_detail
        else:
            s3.put_object(Bucket=BUCKET, Key=p1_key, Body=body1)
            _ledger_upsert(conn, d, "records", p1_fname, url, p1_key,
                           len(body1), _sha256_bytes(body1), "ok", p1_meta_detail)
            ok_map[p1_key] = p1_meta_detail
            res["records_ok"] += 1
            res["bytes_landed"] += len(body1)

        if total_pages <= 1:
            return res

        # Check again if any pages 2..total_pages are missing
        missing_after_p1 = [
            p for p in range(2, total_pages + 1)
            if _page_r2_key(cat, d, p)[0] not in ok_map
        ]
        if not missing_after_p1:
            res["already_landed"] += (total_pages - 1)
            return res

        max_missing_page = max(missing_after_p1)
        win_fields = _extract_hidden_fields(text1)

        # Walk 10-page windows: w=0 (pages 1..10), w=1 (pages 11..20), ...
        num_windows = (total_pages - 1) // 10 + 1
        for w in range(num_windows):
            win_start = 10 * w + 1
            win_end = min(total_pages, 10 * w + 10)
            if win_start > max_missing_page:
                # All remaining windows are already in ok_map
                res["already_landed"] += (total_pages - win_start + 1)
                break

            # Fetch pages inside current window (win_start + 1 .. win_end)
            for p in range(win_start + 1, win_end + 1):
                pk, pfn = _page_r2_key(cat, d, p)
                if pk in ok_map:
                    res["already_landed"] += 1
                    continue
                if time.monotonic() >= deadline_monotonic:
                    res["pages_deferred"] += (total_pages - p + 1)
                    return res

                expected_row = (p - 1) * 50 + 1
                try:
                    body_p = _post_grid_page(www, url, win_fields, p)
                except Exception as exc:  # noqa: BLE001
                    _ledger_upsert(conn, d, "records", pfn, f"{url}#Page${p}", pk,
                                   None, None, "http_error", str(exc)[:500])
                    res["records_http_error"] += 1
                    continue

                valid_p, detail_p = _validate_records(body_p, expected_first_row=expected_row)
                if not valid_p:
                    _ledger_upsert(conn, d, "records", pfn, f"{url}#Page${p}", pk,
                                   len(body_p), None, "validation_failed", detail_p)
                    res["records_failed"] += 1
                    continue

                res["pdf_urls"] |= _extract_pdf_links(body_p.decode("utf-8", errors="replace"))
                s3.put_object(Bucket=BUCKET, Key=pk, Body=body_p)
                _ledger_upsert(conn, d, "records", pfn, f"{url}#Page${p}", pk,
                               len(body_p), _sha256_bytes(body_p), "ok", f"ok:page={p}/{total_pages}")
                ok_map[pk] = f"ok:page={p}/{total_pages}"
                res["records_ok"] += 1
                res["bytes_landed"] += len(body_p)

            # Advance pager window via Page$(win_end + 1) ('...') if further windows have missing pages
            next_win_page = win_end + 1
            if next_win_page <= total_pages and next_win_page <= max_missing_page:
                if time.monotonic() >= deadline_monotonic:
                    res["pages_deferred"] += (total_pages - next_win_page + 1)
                    return res
                nwk, nwfn = _page_r2_key(cat, d, next_win_page)
                expected_nw_row = (next_win_page - 1) * 50 + 1
                try:
                    body_nw = _post_grid_page(www, url, win_fields, next_win_page)
                except Exception as exc:  # noqa: BLE001
                    if nwk not in ok_map:
                        _ledger_upsert(conn, d, "records", nwfn, f"{url}#Page${next_win_page}", nwk,
                                       None, None, "http_error", str(exc)[:500])
                        res["records_http_error"] += 1
                    return res

                valid_nw, detail_nw = _validate_records(body_nw, expected_first_row=expected_nw_row)
                if not valid_nw:
                    if nwk not in ok_map:
                        _ledger_upsert(conn, d, "records", nwfn, f"{url}#Page${next_win_page}", nwk,
                                       len(body_nw), None, "validation_failed", detail_nw)
                        res["records_failed"] += 1
                    return res

                text_nw = body_nw.decode("utf-8", errors="replace")
                win_fields = _extract_hidden_fields(text_nw)
                res["pdf_urls"] |= _extract_pdf_links(text_nw)

                if nwk in ok_map:
                    res["already_landed"] += 1
                else:
                    s3.put_object(Bucket=BUCKET, Key=nwk, Body=body_nw)
                    _ledger_upsert(conn, d, "records", nwfn, f"{url}#Page${next_win_page}", nwk,
                                   len(body_nw), _sha256_bytes(body_nw), "ok", f"ok:page={next_win_page}/{total_pages}")
                    ok_map[nwk] = f"ok:page={next_win_page}/{total_pages}"
                    res["records_ok"] += 1
                    res["bytes_landed"] += len(body_nw)

        print(
            f"[{cat} {d}] complete: total_pages={total_pages} ({rec_cnt} recs), "
            f"new_ok={res['records_ok']}, already={res['already_landed']}, "
            f"failed={res['records_failed'] + res['records_http_error']}"
        )
        return res
    finally:
        conn.close()


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
    """Enumerate award dates → capture ALL paginated AwdRecs HTML pages (1..N)
    per (category,date) → harvest linked award PDFs → callback."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import datetime as dt
    import time
    from zoneinfo import ZoneInfo

    os.makedirs(SCRATCH_DIR, exist_ok=True)
    today_et = dt.datetime.now(ZoneInfo("America/New_York")).date()
    deadline_monotonic = time.monotonic() + MAX_RECORD_CAPTURE_WALL_S

    final_status = "error"
    error_text: str | None = None
    counts = {
        "records_ok": 0,
        "records_failed": 0,
        "records_http_error": 0,
        "records_pages_deferred": 0,
        "pdf_ok": 0,
        "pdf_failed": 0,
        "pdf_http_error": 0,
        "skipped_current_day": 0,
        "already_landed": 0,
        "pdf_budget_deferred": 0,
    }
    bytes_landed = 0
    pdf_urls: set[str] = set()

    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(OPS_DDL)
        conn.commit()

        s3 = _s3_client()
        www = _consent_session(WWW, "/Awards/AwdDates.aspx?category=awddt")

        dates = _enumerate_award_dates(www)
        ok_map = _ledger_ok_map(conn)
        print(f"award dates listed: {len(dates)} (category,date) pairs; "
              f"ledger has {len(ok_map)} ok keys")

        # Sort newest dates first so recent dates complete first during backfill
        work_items = []
        for (cat, d) in sorted(dates, key=lambda t: (t[1], t[0]), reverse=True):
            if d >= today_et:  # today's grid may still be accruing
                counts["skipped_current_day"] += 1
                continue
            work_items.append((cat, d, dates[(cat, d)]))

        with ThreadPoolExecutor(max_workers=DATE_WORKERS) as pool:
            futures = [
                pool.submit(_capture_date_all_pages, cat, d, url, ok_map, deadline_monotonic)
                for (cat, d, url) in work_items
            ]
            for fut in as_completed(futures):
                r = fut.result()
                counts["records_ok"] += r["records_ok"]
                counts["records_failed"] += r["records_failed"]
                counts["records_http_error"] += r["records_http_error"]
                counts["already_landed"] += r["already_landed"]
                counts["records_pages_deferred"] += r["pages_deferred"]
                bytes_landed += r["bytes_landed"]
                pdf_urls |= r["pdf_urls"]

        # ---- award_pdf stream: dedup'd PDFs referenced by the grids ----
        pending_pdfs = [
            u for u in sorted(pdf_urls)
            if f"{R2_PREFIX}/award_pdf/{u.split('/Downloads/Awards/', 1)[1]}" not in ok_map
        ]
        print(f"unique award PDFs referenced: {len(pdf_urls)}; pending: {len(pending_pdfs)}")
        if len(pending_pdfs) > MAX_PDFS_PER_RUN:
            counts["pdf_budget_deferred"] = len(pending_pdfs) - MAX_PDFS_PER_RUN
            print(f"WARN: PDF budget {MAX_PDFS_PER_RUN} < pending {len(pending_pdfs)}; "
                  f"{counts['pdf_budget_deferred']} deferred to next run (no rolling window).")
            pending_pdfs = pending_pdfs[:MAX_PDFS_PER_RUN]
        if pending_pdfs:
            dl = _consent_session(DL_HOST, "/Downloads/Awards/")
            for u in pending_pdfs:
                if time.monotonic() >= deadline_monotonic + 30 * 60:
                    counts["pdf_budget_deferred"] += 1
                    continue
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

        if counts["records_ok"] == 0 and counts["already_landed"] == 0:
            raise RuntimeError(f"no record pages landed or verified: {counts}")

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
