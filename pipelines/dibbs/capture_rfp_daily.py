"""Compute worker — DLA DIBBS daily RFP/IFB raw & structured capture.

Sibling of ``capture_rfq_daily.py`` and ``capture_awards_daily.py``. Runs as its OWN
Modal app (``dibbs-rfp-pipelines``) so its deploy lifecycle never clobbers the RFQ
(``dibbs-pipelines``) or Awards (``dibbs-awards-pipelines``) workers. Spawned by the
Universal Dispatcher (``core/modal_dispatcher.py``); cadence owned by Trigger.dev
(``src/trigger/dibbs_rfp_daily.ts``).

Architecture of DIBBS RFPs (verified live against www.dibbs.bsm.dla.mil & dibbs2.bsm.dla.mil):
  1. ``/RFP/RfpDates.aspx?category={issue,close}`` lists active dates linking to
     ``/RFP/RfpRecs.aspx?category={cat}&TypeSrch=dt&Value=MM-DD-YYYY``.
  2. ``RfpRecs.aspx`` renders ``tblSearchResults`` paginated at 50 rows per page.
     Unlike ``AwdRecs.aspx`` (which uses ``ctl00$cph1$grdAwardSearch`` ``Page$N``),
     ``RfpRecs.aspx`` paginates via ``__doPostBack('ctl00$cph1$mnuPager', '{N}')``.
  3. Each row on ``RfpRecs.aspx`` links to:
     - ``/Rfp/RfpRec.aspx?sn={SN}`` (the Solicitation Package View page)
     - ``/Rfp/Pid.aspx?sn={SN}&nsn={NSN}`` (the Procurement Item Description popup,
       which contains the item description, packaging spec, and Approved Makers
       ``{NAME} {CAGE} P/N {PART_NUMBER}``).
  4. ``RfpRec.aspx?sn={SN}`` lists:
     - Solicitation Package ZIP via ``RfpNotify.aspx?sn={SN}&fn={SN}PKG.ZIP``
     - Base Solicitation PDF via ``RfpNotify.aspx?sn={SN}&email=&fn={SN}.PDF``
     - Amendment PDFs and Attachment files (``.PDF``, ``.DOC``, ``.DOCX``, ``.XLSX``)
       linked directly from ``https://dibbs2.bsm.dla.mil/Downloads/RFP/{FILENAME}``.
     - ``RfpNotify.aspx`` is only an email-registration splash gate; every file (including
       ``{SN}PKG.ZIP`` and ``{SN}.PDF``) sits directly on ``dibbs2.bsm.dla.mil/Downloads/RFP/{FILENAME}``.
     - ``{SN}PKG.ZIP`` bundles the base solicitation PDF, all amendment PDFs, and all
       attachment files into a single archive.

Streams captured byte-identical to R2 (``s3://data-sink/raw/dibbs_rfp_daily/...``) and
checkpointed in ``ops.dibbs_rfp_daily_r2_ingest_runs``:
  - ``records``      : RfpRecs.aspx HTML per (category, date, page)
  - ``package_view`` : RfpRec.aspx HTML per solicitation_number
  - ``pid_html``     : Pid.aspx HTML per (solicitation_number, nsn)
  - ``rfp_pkg_zip``  : {SN}PKG.ZIP from dibbs2.bsm.dla.mil/Downloads/RFP/
  - ``rfp_doc``      : Any standalone RFP PDF/amendment/attachment not already inside {SN}PKG.ZIP

Inline structured Postgres tables populated on the fly:
  - ``dibbs.rfp_solicitations`` : 1 row per RFP solicitation header
  - ``dibbs.rfp_items``         : 1 row per (solicitation_number, nsn) with PID text & makers
  - ``dibbs.rfp_documents``     : 1 row per (solicitation_number, file_name)
  - ``dibbs.approved_makers``   : Upserts approved makers discovered in RFP PIDs

    modal deploy pipelines/dibbs/capture_rfp_daily.py
"""

from __future__ import annotations

import os

import modal

WWW = "https://www.dibbs.bsm.dla.mil"
DL_HOST = "https://dibbs2.bsm.dla.mil"

RFP_DATES_CATEGORIES = ("issue", "close")

BUCKET = "data-sink"
R2_PREFIX = "raw/dibbs_rfp_daily"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
INTER_REQUEST_DELAY_S = 0.85  # polite pause on single persistent session (avoids BIG-IP ASM block)
PAGE_POST_DELAY_S = 0.45

FEED = "dibbs_rfp_daily"
SCRATCH_DIR = "/tmp/dibbs_rfp"

MAX_WALL_S = 180 * 60  # 3 hours ceiling inside 4h Modal timeout
MAX_PACKAGES_PER_RUN = 1500

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS dibbs;

CREATE TABLE IF NOT EXISTS ops.dibbs_rfp_daily_r2_ingest_runs (
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

CREATE TABLE IF NOT EXISTS dibbs.rfp_solicitations (
  solicitation_number TEXT PRIMARY KEY,
  solicitation_formatted TEXT,
  instrument_type TEXT,
  total_items INT,
  issue_date DATE,
  close_date DATE,
  buyer_code TEXT,
  package_view_r2_key TEXT,
  pkg_zip_r2_key TEXT,
  amendment_count INT NOT NULL DEFAULT 0,
  attachment_count INT NOT NULL DEFAULT 0,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dibbs_rfp_sol_issue ON dibbs.rfp_solicitations (issue_date DESC);
CREATE INDEX IF NOT EXISTS idx_dibbs_rfp_sol_close ON dibbs.rfp_solicitations (close_date);

CREATE TABLE IF NOT EXISTS dibbs.rfp_items (
  solicitation_number TEXT NOT NULL,
  nsn TEXT NOT NULL,
  fsc TEXT,
  nomenclature TEXT,
  tech_docs_url TEXT,
  issue_date DATE,
  close_date DATE,
  buyer_code TEXT,
  pid_r2_key TEXT,
  pid_text TEXT,
  packaging_standard TEXT,
  has_csi BOOLEAN,
  has_fat BOOLEAN,
  has_higher_quality BOOLEAN,
  approved_makers_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (solicitation_number, nsn)
);

CREATE INDEX IF NOT EXISTS idx_dibbs_rfp_items_nsn ON dibbs.rfp_items (nsn);

CREATE TABLE IF NOT EXISTS dibbs.rfp_documents (
  solicitation_number TEXT NOT NULL,
  file_name TEXT NOT NULL,
  doc_role TEXT NOT NULL,
  source_url TEXT NOT NULL,
  r2_key TEXT NOT NULL,
  bytes BIGINT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (solicitation_number, file_name)
);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "requests>=2.32",
    "boto3>=1.34",
    "psycopg[binary]>=3.2",
)

app = modal.App("dibbs-rfp-pipelines", image=image)


# ── consent + polite HTTP ──────────────────────────────────────────────────────────
def _consent_session(host: str, goto_path: str):
    """Establish a DoD-consent session on ``host`` (cookies are host-scoped)."""
    import re
    import time
    import urllib.parse

    import requests

    warn_url = f"{host}/dodwarning.aspx?goto={urllib.parse.quote(goto_path, safe='')}"
    last_err = None
    for attempt in range(4):
        if attempt > 0:
            time.sleep(5.0 * attempt)
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
        try:
            r = s.get(warn_url, timeout=60)
            r.raise_for_status()
            if "request rejected" in r.text.lower():
                last_err = RuntimeError(f"BIG-IP ASM rejected consent GET on {host}")
                continue
            fields: dict[str, str] = {}
            for tag in re.findall(r"<input[^>]+>", r.text):
                if "hidden" not in tag.lower():
                    continue
                n = re.search(r"name=[\"']([^\"']+)[\"']", tag)
                v = re.search(r"value=[\"']([^\"']*)[\"']", tag)
                if n:
                    fields[n.group(1)] = v.group(1) if v else ""
            if not fields:
                last_err = RuntimeError(f"consent page on {host} exposed no hidden fields")
                continue
            fields["butAgree"] = "OK"
            r2 = s.post(warn_url, data=fields, timeout=60, allow_redirects=True)
            r2.raise_for_status()
            if "request rejected" in r2.text.lower():
                last_err = RuntimeError(f"BIG-IP ASM rejected consent POST on {host}")
                continue
            return s
        except Exception as exc:  # noqa: BLE001
            last_err = exc
    raise RuntimeError(f"Failed consent on {host}: {last_err}")


def _polite_get(session, url: str, delay_s: float = INTER_REQUEST_DELAY_S, **kw):
    import time

    if delay_s > 0:
        time.sleep(delay_s)
    return session.get(url, **kw)


def _extract_hidden_fields(html: str) -> dict[str, str]:
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
    import re

    m = re.search(
        r'id=["\']ctl00_cph1_lblRecCount["\'][^>]*>Records Found:\s*<strong>\s*(\d+)\s*</strong>',
        html,
        re.I,
    )
    return int(m.group(1)) if m else None


def _parse_row_nums(html: str) -> list[int]:
    import re

    return [
        int(x)
        for x in re.findall(
            r'<td\s+headers=["\']th0["\'][^>]*>\s*(\d+)\s*</td>',
            html,
            re.I,
        )
    ]


def _post_rfp_page(session, url: str, base_fields: dict[str, str], page_num: int) -> bytes:
    """POST a Menu Pager postback ('ctl00$cph1$mnuPager', str(page_num)) to RfpRecs.aspx."""
    import time

    data = dict(base_fields)
    data["__EVENTTARGET"] = "ctl00$cph1$mnuPager"
    data["__EVENTARGUMENT"] = str(page_num)
    data["ctl00$ddlNavigation"] = ""
    data["ctl00$ddlGotoDB"] = ""
    data["ctl00$txtValue"] = ""

    last_exc = None
    for attempt in range(3):
        time.sleep(PAGE_POST_DELAY_S if attempt == 0 else 2.5 * (attempt + 1))
        try:
            r = session.post(url, data=data, timeout=120)
            r.raise_for_status()
            return r.content
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
    raise last_exc  # type: ignore[misc]


# ── validation ─────────────────────────────────────────────────────────────────────
def _check_common_gates(body: bytes) -> tuple[bool, str]:
    head = body[:1000].lower()
    if b"request rejected" in head:
        return False, "WAF Request Rejected page served"
    if b"dodwarning" in head:
        return False, "consent gate served instead of content"
    if b"resource you are looking for" in head:
        return False, "404 masquerade (200 body)"
    return True, "ok"


def _validate_records(body: bytes, expected_first_row: int | None = None) -> tuple[bool, str]:
    ok, msg = _check_common_gates(body)
    if not ok:
        return False, msg
    low = body.lower()
    if b"rfp/ifb/other rfqs" not in low:
        return False, "not an RfpRecs page (title marker absent)"
    if b"tblsearchresults" not in low and b"no records found" not in low:
        return False, "tblSearchResults marker absent"
    if expected_first_row is not None and b"tblsearchresults" in low:
        text = body.decode("utf-8", errors="replace")
        rnums = _parse_row_nums(text)
        if not rnums:
            return False, f"expected first row {expected_first_row} but no rows found"
        if rnums[0] != expected_first_row:
            return False, f"expected first row {expected_first_row}, got {rnums[0]}"
    return True, "ok"


def _validate_package_view(body: bytes) -> tuple[bool, str]:
    ok, msg = _check_common_gates(body)
    if not ok:
        return False, msg
    low = body.lower()
    if b"package data" not in low:
        return False, "not an RfpRec Package View page ('Package Data' absent)"
    return True, "ok"


def _validate_pid(body: bytes) -> tuple[bool, str]:
    ok, msg = _check_common_gates(body)
    if not ok:
        return False, msg
    low = body.lower()
    if b"procurement item description" not in low:
        return False, "not a Pid.aspx page ('Procurement Item Description' absent)"
    return True, "ok"


def _validate_binary_doc(path: str, fname: str) -> tuple[bool, str]:
    import zipfile

    size = os.path.getsize(path)
    if size < 128:
        return False, f"file too small ({size} bytes)"
    with open(path, "rb") as fh:
        head = fh.read(512)
    low_head = head.lower()
    if b"request rejected" in low_head or b"dodwarning" in low_head or b"<!doctype html" in low_head or b"<html" in low_head:
        return False, "HTML page returned instead of binary document"
    up = fname.upper()
    if up.endswith(".PDF"):
        if not head.startswith(b"%PDF-"):
            return False, f"missing %PDF- magic (got {head[:8]!r})"
    elif up.endswith(".ZIP"):
        if not head.startswith(b"PK"):
            return False, f"missing PK zip magic (got {head[:8]!r})"
        try:
            with zipfile.ZipFile(path) as zf:
                bad = zf.testzip()
                if bad is not None:
                    return False, f"corrupt zip member: {bad}"
        except Exception as exc:  # noqa: BLE001
            return False, f"invalid zip archive: {exc}"
    return True, "ok"


# ── R2 + Postgres plumbing ─────────────────────────────────────────────────────────
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
    with conn.cursor() as cur:
        cur.execute(
            "SELECT r2_key, COALESCE(detail, 'ok') FROM ops.dibbs_rfp_daily_r2_ingest_runs WHERE status = 'ok'"
        )
        return {r[0]: r[1] for r in cur.fetchall()}


def _ledger_upsert(conn, post_date, stream, file_name, source_url, r2_key,
                   nbytes, sha256, status, detail) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.dibbs_rfp_daily_r2_ingest_runs
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


def _parse_mmddyyyy(s: str | None):
    import datetime as dt
    import re

    if not s:
        return None
    m = re.search(r"(\d{2})-(\d{2})-(\d{4})", s)
    if not m:
        return None
    try:
        return dt.date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
    except ValueError:
        return None


# ── HTML parsers for RfpRecs.aspx, RfpRec.aspx, and Pid.aspx ──────────────────────
def _parse_rfprecs_rows(html: str) -> list[dict]:
    """Extract all rows from tblSearchResults on RfpRecs.aspx."""
    import re

    rows: list[dict] = []
    m_tb = re.search(r'<table[^>]*id=["\']tblSearchResults["\'][^>]*>(.*?)</table>', html, re.S | re.I)
    if not m_tb:
        return rows
    tbody = m_tb.group(1)
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", tbody, re.S | re.I):
        if 'headers="th0"' not in tr and "headers='th0'" not in tr:
            continue

        def _cell(th: str) -> str:
            m = re.search(rf'<td[^>]*headers=["\']{th}["\'][^>]*>(.*?)</td>', tr, re.S | re.I)
            return m.group(1).strip() if m else ""

        c_nsn = _cell("th1")
        c_nom = _cell("th2")
        c_tech = _cell("th3")
        c_sol = _cell("th4")
        c_iss = _cell("th5")
        c_cls = _cell("th6")
        c_buy = _cell("th7")

        m_pid = re.search(r"Pid\.aspx\?sn=([A-Z0-9]+)&(?:amp;)?nsn=([A-Z0-9]+)", c_nsn, re.I)
        m_sn = re.search(r"RfpRec\.aspx\?sn=([A-Z0-9]+)", c_sol, re.I)
        sn = (m_sn.group(1) if m_sn else (m_pid.group(1) if m_pid else "")).upper()
        nsn = (m_pid.group(2) if m_pid else re.sub(r"[^0-9A-Z]", "", re.sub(r"<[^>]+>", "", c_nsn))).upper()
        if not sn:
            continue

        m_fmt = re.search(r">([A-Z0-9]{6}-\d{2}-[A-Z]-[A-Z0-9]{4})<", c_sol, re.I)
        sn_fmt = m_fmt.group(1).upper() if m_fmt else sn
        nom = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c_nom)).strip()
        m_td = re.search(r"href=['\"]([^'\"]+)['\"]", c_tech.replace("&amp;", "&"), re.I)
        tech_url = m_td.group(1).strip() if m_td else None
        iss_dt = _parse_mmddyyyy(c_iss)
        cls_dt = _parse_mmddyyyy(c_cls)
        buyer = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c_buy)).strip() or None

        rows.append(
            {
                "sn": sn,
                "sn_formatted": sn_fmt,
                "nsn": nsn,
                "nomenclature": nom,
                "tech_docs_url": tech_url,
                "issue_date": iss_dt,
                "close_date": cls_dt,
                "buyer_code": buyer,
            }
        )
    return rows


def _parse_rfprec_package(html: str, sn: str) -> dict:
    """Parse RfpRec.aspx?sn={sn} for header metadata, document filenames, and PID links."""
    import re

    norm = html.replace("&amp;", "&")

    def _span(sid: str) -> str:
        m = re.search(rf'id=["\'][^"\']*{sid}["\'][^>]*>(.*?)</span>', norm, re.S | re.I)
        if not m:
            return ""
        return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", m.group(1))).replace("&nbsp;", " ").strip()

    raw_sol = _span("lblSolicitation")
    m_fmt = re.search(r"([A-Z0-9]{6}-\d{2}-[A-Z]-[A-Z0-9]{4})", raw_sol, re.I)
    sn_fmt = m_fmt.group(1).upper() if m_fmt else sn
    tot_str = _span("lblTotItems")
    total_items = int(tot_str) if tot_str.isdigit() else None
    issue_date = _parse_mmddyyyy(_span("lblIssue"))
    close_date = _parse_mmddyyyy(_span("lblCloseDte"))
    buyer_code = _span("lblBuyerCode") or None

    # Extract notify filenames (e.g. {SN}PKG.ZIP, {SN}.PDF) and direct Downloads/RFP/{FILENAME}
    notify_fns = re.findall(r"RfpNotify\.aspx\?[^'\"\s<>]*[?&]fn=([^&'\"\s<>]+)", norm, re.I)
    direct_fns = re.findall(r"https?://dibbs2\.bsm\.dla\.mil/Downloads/RFP/([^'\"\s<>]+)", norm, re.I)

    docs: dict[str, str] = {}  # file_name -> role
    for fn in notify_fns:
        fn_clean = fn.strip()
        if not fn_clean:
            continue
        if fn_clean.upper().endswith("PKG.ZIP"):
            docs[fn_clean] = "package_zip"
        elif fn_clean.upper() == f"{sn.upper()}.PDF":
            docs[fn_clean] = "solicitation_pdf"
        else:
            docs[fn_clean] = "amendment_or_doc"

    for fn in direct_fns:
        fn_clean = fn.strip()
        if not fn_clean or fn_clean in docs:
            continue
        up = fn_clean.upper()
        # Amendments on DIBBS RFPs are named {SN}0001.PDF, {SN}0002.PDF (17 chars before .PDF)
        base = up.rsplit(".", 1)[0]
        if up.endswith(".PDF") and base.startswith(sn.upper()) and len(base) == len(sn) + 4 and base[len(sn):].isdigit():
            docs[fn_clean] = "amendment"
        else:
            docs[fn_clean] = "attachment"

    # Also parse item table on RfpRec.aspx for any additional (sn, nsn, nomenclature) rows
    pids = set(re.findall(r"Pid\.aspx\?sn=([A-Z0-9]+)&nsn=([A-Z0-9]+)", norm, re.I))

    return {
        "sn": sn.upper(),
        "sn_formatted": sn_fmt,
        "instrument_type": sn[8].upper() if len(sn) >= 9 else None,
        "total_items": total_items,
        "issue_date": issue_date,
        "close_date": close_date,
        "buyer_code": buyer_code,
        "docs": docs,
        "pids": {(a.upper(), b.upper()) for (a, b) in pids},
    }


def _parse_pid_html(html: str) -> dict:
    """Parse Pid.aspx HTML for clean text, packaging standard, flags, and Approved Makers."""
    import re

    re_cage_pn = re.compile(
        r"^([A-Z0-9][A-Z0-9\s\.\,\-\&\/\'\"()]{2,60}?)\s+([A-Z0-9]{5})\s+P/N\s+(.+)$"
    )

    # Extract the main description div after lblSolicitation
    m_body = re.search(r'id=["\']lblSolicitation["\'][^>]*>.*?</div>(.*?)</div>', html, re.S | re.I)
    raw_block = m_body.group(1) if m_body else html
    text_with_lines = re.sub(r"<br\s*/?>", "\n", raw_block, flags=re.I)
    text_with_lines = re.sub(r"<[^>]+>", " ", text_with_lines)
    text_with_lines = text_with_lines.replace("&nbsp;", " ").replace("&amp;", "&").replace("&quot;", '"')

    lines = [re.sub(r"\s+", " ", l).strip() for l in text_with_lines.splitlines()]
    lines = [l for l in lines if l]
    clean_text = "\n".join(lines)
    upper_text = clean_text.upper()

    if "MIL-STD-2073" in upper_text:
        pkg = "MIL-STD-2073"
    elif "ASTM D3951" in upper_text:
        pkg = "ASTM D3951"
    elif "MMS NO. 1" in upper_text or "MEDICAL MARKING" in upper_text:
        pkg = "MMS NO. 1"
    else:
        pkg = "OTHER"

    has_csi = ("CRITICAL SAFETY ITEM" in upper_text) or ("CRITICAL APPLICATION ITEM" in upper_text)
    has_fat = ("FIRST ARTICLE" in upper_text) or ("FAT " in upper_text)
    has_hq = (
        ("HIGHER-LEVEL CONTRACT QUALITY" in upper_text)
        or ("ISO 9001" in upper_text)
        or ("AS9100" in upper_text)
        or ("RQ001" in upper_text)
        or ("RQ002" in upper_text)
        or ("RQ009" in upper_text)
    )

    makers: list[dict] = []
    seen = set()
    for l in lines:
        mm = re_cage_pn.match(l)
        if mm:
            cname, ccage, cpn = mm.groups()
            cname = cname.strip()
            for prefix in (
                "CRITICAL APPLICATION ITEM",
                "EVALUATE ALTERNATE OFFERS.",
                "GOVERNMENT DESIGN CONTROL ACTIVITY.",
                "PERMITTED IN THIS ITEM",
            ):
                cname = cname.replace(prefix, "").strip()
            ccage = ccage.strip().upper()
            cpn = cpn.strip()
            if len(cname) >= 2 and (ccage, cpn) not in seen:
                seen.add((ccage, cpn))
                makers.append({"maker_name": cname, "maker_cage": ccage, "part_number": cpn})

    return {
        "pid_text": clean_text,
        "packaging_standard": pkg,
        "has_csi": has_csi,
        "has_fat": has_fat,
        "has_higher_quality": has_hq,
        "makers": makers,
    }


# ── dates-page enumeration ─────────────────────────────────────────────────────────
def _enumerate_rfp_dates(www_session) -> dict[tuple[str, "object"], str]:
    import datetime as dt
    import re

    out: dict[tuple[str, object], str] = {}
    for cat in RFP_DATES_CATEGORIES:
        url = f"{WWW}/RFP/RfpDates.aspx?category={cat}"
        r = _polite_get(www_session, url, timeout=120)
        r.raise_for_status()
        html = r.text.replace("&amp;", "&")
        for href in re.findall(r"href=['\"]([^'\"]*RfpRecs\.aspx\?[^'\"]+)['\"]", html, re.I):
            m = re.search(r"Value=(\d{2})-(\d{2})-(\d{4})", href)
            if not m:
                continue
            d = dt.date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
            full = href if href.lower().startswith("http") else f"{WWW}{href if href.startswith('/') else '/RFP/' + href}"
            out[(cat, d)] = full
    return out


def _page_r2_key(cat: str, d, page_num: int) -> tuple[str, str]:
    if page_num == 1:
        fname = f"rfp_{d:%y%m%d}.html"
    else:
        fname = f"rfp_{d:%y%m%d}_p{page_num:03d}.html"
    return f"{R2_PREFIX}/records/{cat}/{d.year}/{fname}", fname


def _parse_detail_pages(detail: str) -> int | None:
    import re

    m = re.search(r"pages=(\d+)", detail or "")
    return int(m.group(1)) if m else None


def _capture_date_all_pages(
    www,
    s3,
    conn,
    cat: str,
    d,
    url: str,
    ok_map: dict[str, str],
    deadline_monotonic: float,
) -> dict:
    """Capture Page 1 .. Page N for a single (cat, d) on the shared polite session."""
    import time

    res = {
        "records_ok": 0,
        "records_failed": 0,
        "records_http_error": 0,
        "already_landed": 0,
        "pages_deferred": 0,
        "bytes_landed": 0,
        "rows": [],
    }

    p1_key, p1_fname = _page_r2_key(cat, d, 1)
    p1_detail = ok_map.get(p1_key)
    known_total_pages = _parse_detail_pages(p1_detail) if p1_detail else None

    if known_total_pages is not None:
        missing_pages = [
            p for p in range(1, known_total_pages + 1)
            if _page_r2_key(cat, d, p)[0] not in ok_map
        ]
        if not missing_pages:
            # Read existing pages from R2 so we still discover all (sn, nsn) rows
            for p in range(1, known_total_pages + 1):
                pk, _ = _page_r2_key(cat, d, p)
                try:
                    body = s3.get_object(Bucket=BUCKET, Key=pk)["Body"].read()
                    res["rows"].extend(_parse_rfprecs_rows(body.decode("utf-8", errors="replace")))
                except Exception:  # noqa: BLE001
                    pass
            res["already_landed"] += known_total_pages
            return res

    if time.monotonic() >= deadline_monotonic:
        res["pages_deferred"] += (known_total_pages or 1)
        return res

    try:
        r1 = _polite_get(www, url, timeout=120)
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
    res["rows"].extend(_parse_rfprecs_rows(text1))
    rec_cnt = _parse_records_found(text1)
    if rec_cnt is None:
        rec_cnt = len(res["rows"])
    total_pages = max(1, (rec_cnt + 49) // 50)
    p1_meta_detail = f"ok:pages={total_pages}:records={rec_cnt}"

    if p1_key in ok_map:
        res["already_landed"] += 1
    else:
        s3.put_object(Bucket=BUCKET, Key=p1_key, Body=body1)
        _ledger_upsert(conn, d, "records", p1_fname, url, p1_key,
                       len(body1), _sha256_bytes(body1), "ok", p1_meta_detail)
        ok_map[p1_key] = p1_meta_detail
        res["records_ok"] += 1
        res["bytes_landed"] += len(body1)

    if total_pages <= 1:
        return res

    cur_fields = _extract_hidden_fields(text1)
    for p in range(2, total_pages + 1):
        pk, pfn = _page_r2_key(cat, d, p)
        if time.monotonic() >= deadline_monotonic:
            res["pages_deferred"] += (total_pages - p + 1)
            return res

        expected_row = (p - 1) * 50 + 1
        try:
            body_p = _post_rfp_page(www, url, cur_fields, p)
        except Exception as exc:  # noqa: BLE001
            if pk not in ok_map:
                _ledger_upsert(conn, d, "records", pfn, f"{url}#Page${p}", pk,
                               None, None, "http_error", str(exc)[:500])
                res["records_http_error"] += 1
            break

        valid_p, detail_p = _validate_records(body_p, expected_first_row=expected_row)
        if not valid_p:
            if pk not in ok_map:
                _ledger_upsert(conn, d, "records", pfn, f"{url}#Page${p}", pk,
                               len(body_p), None, "validation_failed", detail_p)
                res["records_failed"] += 1
            break

        text_p = body_p.decode("utf-8", errors="replace")
        cur_fields = _extract_hidden_fields(text_p)
        res["rows"].extend(_parse_rfprecs_rows(text_p))

        if pk in ok_map:
            res["already_landed"] += 1
        else:
            s3.put_object(Bucket=BUCKET, Key=pk, Body=body_p)
            _ledger_upsert(conn, d, "records", pfn, f"{url}#Page${p}", pk,
                           len(body_p), _sha256_bytes(body_p), "ok", f"ok:page={p}/{total_pages}")
            ok_map[pk] = f"ok:page={p}/{total_pages}"
            res["records_ok"] += 1
            res["bytes_landed"] += len(body_p)

    return res


def _download_rfp_binary(dl_session, s3, conn, post_date, stream: str, fname: str, r2_key: str) -> tuple[str, int, set[str]]:
    """Download one file from dibbs2.bsm.dla.mil/Downloads/RFP/{fname} to R2.
    If it is a PKG.ZIP, also returns the set of bundled member filenames (uppercased)."""
    import zipfile

    url = f"{DL_HOST}/Downloads/RFP/{fname}"
    tmp = os.path.join(SCRATCH_DIR, fname)
    bundled_upper: set[str] = set()
    try:
        with _polite_get(dl_session, url, stream=True, timeout=(60, 1800)) as resp:
            resp.raise_for_status()
            with open(tmp, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    if chunk:
                        fh.write(chunk)
    except Exception as exc:  # noqa: BLE001
        _ledger_upsert(conn, post_date, stream, fname, url, r2_key,
                       None, None, "http_error", str(exc)[:500])
        return "http_error", 0, bundled_upper

    try:
        nbytes = os.path.getsize(tmp)
        valid, detail = _validate_binary_doc(tmp, fname)
        if not valid:
            _ledger_upsert(conn, post_date, stream, fname, url, r2_key,
                           nbytes, None, "validation_failed", detail)
            return "validation_failed", 0, bundled_upper

        if fname.upper().endswith(".ZIP"):
            with zipfile.ZipFile(tmp) as zf:
                bundled_upper = {os.path.basename(zi.filename).upper() for zi in zf.infolist()}

        sha = _sha256_file(tmp)
        s3.upload_file(tmp, BUCKET, r2_key)
        _ledger_upsert(conn, post_date, stream, fname, url, r2_key,
                       nbytes, sha, "ok", f"ok:members={len(bundled_upper)}" if bundled_upper else "ok")
        return "ok", nbytes, bundled_upper
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


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
    timeout=4 * 60 * 60,
    memory=4096,
    cpu=2.0,
)
def capture_rfp_daily(
    trigger_callback_url: str | None = None,
    backfill: bool = False,
) -> dict:
    """Enumerate RFP dates (issue + close) → capture all RfpRecs pages (1..N) →
    capture RfpRec.aspx Package Views + Pid.aspx Item Descriptions →
    download {SN}PKG.ZIP + standalone documents from dibbs2.bsm.dla.mil/Downloads/RFP/ →
    populate dibbs.rfp_solicitations, dibbs.rfp_items, dibbs.rfp_documents, dibbs.approved_makers."""
    import datetime as dt
    import time
    from zoneinfo import ZoneInfo
    from psycopg.types.json import Jsonb

    os.makedirs(SCRATCH_DIR, exist_ok=True)
    today_et = dt.datetime.now(ZoneInfo("America/New_York")).date()
    deadline_monotonic = time.monotonic() + MAX_WALL_S

    final_status = "error"
    error_text: str | None = None
    counts = {
        "records_ok": 0,
        "records_failed": 0,
        "records_http_error": 0,
        "records_pages_deferred": 0,
        "package_view_ok": 0,
        "package_view_failed": 0,
        "pid_ok": 0,
        "pid_failed": 0,
        "pkg_zip_ok": 0,
        "pkg_zip_failed": 0,
        "doc_ok": 0,
        "doc_failed": 0,
        "solicitations_upserted": 0,
        "items_upserted": 0,
        "makers_upserted": 0,
        "skipped_current_day": 0,
        "already_landed": 0,
    }
    bytes_landed = 0

    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(OPS_DDL)
        conn.commit()

        s3 = _s3_client()
        www = _consent_session(WWW, "/RFP/RfpDates.aspx?category=issue")
        dl = _consent_session(DL_HOST, "/Downloads/RFP/")

        dates = _enumerate_rfp_dates(www)
        ok_map = _ledger_ok_map(conn)
        print(f"RFP dates listed: {len(dates)} (category,date) pairs; ledger has {len(ok_map)} ok keys")

        # Process 'issue' dates first (newest first, skipping current day in ET), then 'close' dates
        work_items = []
        for (cat, d) in sorted(dates, key=lambda t: (0 if t[0] == "issue" else 1, -t[1].toordinal())):
            if cat == "issue" and d >= today_et:
                counts["skipped_current_day"] += 1
                continue
            work_items.append((cat, d, dates[(cat, d)]))

        all_grid_rows: list[dict] = []
        for (cat, d, url) in work_items:
            r = _capture_date_all_pages(www, s3, conn, cat, d, url, ok_map, deadline_monotonic)
            counts["records_ok"] += r["records_ok"]
            counts["records_failed"] += r["records_failed"]
            counts["records_http_error"] += r["records_http_error"]
            counts["already_landed"] += r["already_landed"]
            counts["records_pages_deferred"] += r["pages_deferred"]
            bytes_landed += r["bytes_landed"]
            all_grid_rows.extend(r["rows"])

        # Group discovered grid rows by solicitation_number (sn)
        by_sn: dict[str, dict] = {}
        for row in all_grid_rows:
            sn = row["sn"]
            entry = by_sn.setdefault(
                sn,
                {
                    "sn": sn,
                    "sn_formatted": row["sn_formatted"],
                    "issue_date": row["issue_date"],
                    "close_date": row["close_date"],
                    "buyer_code": row["buyer_code"],
                    "items": {},
                },
            )
            if not entry["issue_date"] and row["issue_date"]:
                entry["issue_date"] = row["issue_date"]
            if not entry["close_date"] and row["close_date"]:
                entry["close_date"] = row["close_date"]
            if not entry["buyer_code"] and row["buyer_code"]:
                entry["buyer_code"] = row["buyer_code"]
            if row["nsn"]:
                entry["items"][row["nsn"]] = row

        print(f"Discovered {len(by_sn)} distinct RFP solicitations across {len(all_grid_rows)} grid rows")

        # Sort solicitations newest issue_date first
        sorted_sns = sorted(
            by_sn.values(),
            key=lambda x: (x["issue_date"] or dt.date(2000, 1, 1)),
            reverse=True,
        )[:MAX_PACKAGES_PER_RUN]

        for sol in sorted_sns:
            if time.monotonic() >= deadline_monotonic + 30 * 60:
                break
            sn = sol["sn"]
            post_date = sol["issue_date"] or today_et
            yr = post_date.year

            # 1. Capture or load RfpRec.aspx?sn={sn} (Package View)
            pv_fname = f"{sn}.html"
            pv_key = f"{R2_PREFIX}/package_view/{yr}/{pv_fname}"
            pv_url = f"{WWW}/Rfp/RfpRec.aspx?sn={sn}"
            pv_html: str | None = None

            if pv_key in ok_map:
                counts["already_landed"] += 1
                try:
                    pv_html = s3.get_object(Bucket=BUCKET, Key=pv_key)["Body"].read().decode("utf-8", errors="replace")
                except Exception:  # noqa: BLE001
                    pv_html = None

            if pv_html is None:
                try:
                    rpv = _polite_get(www, pv_url, timeout=120)
                    rpv.raise_for_status()
                    valid_pv, det_pv = _validate_package_view(rpv.content)
                    if not valid_pv:
                        _ledger_upsert(conn, post_date, "package_view", pv_fname, pv_url, pv_key,
                                       len(rpv.content), None, "validation_failed", det_pv)
                        counts["package_view_failed"] += 1
                        continue
                    s3.put_object(Bucket=BUCKET, Key=pv_key, Body=rpv.content)
                    _ledger_upsert(conn, post_date, "package_view", pv_fname, pv_url, pv_key,
                                   len(rpv.content), _sha256_bytes(rpv.content), "ok", "ok")
                    ok_map[pv_key] = "ok"
                    counts["package_view_ok"] += 1
                    bytes_landed += len(rpv.content)
                    pv_html = rpv.content.decode("utf-8", errors="replace")
                except Exception as exc:  # noqa: BLE001
                    _ledger_upsert(conn, post_date, "package_view", pv_fname, pv_url, pv_key,
                                   None, None, "http_error", str(exc)[:500])
                    counts["package_view_failed"] += 1
                    continue

            pkg_meta = _parse_rfprec_package(pv_html, sn)
            issue_date = pkg_meta["issue_date"] or sol["issue_date"] or today_et
            close_date = pkg_meta["close_date"] or sol["close_date"]
            buyer_code = pkg_meta["buyer_code"] or sol["buyer_code"]

            # Ensure any PID links on RfpRec.aspx are in sol["items"]
            for (_, p_nsn) in pkg_meta["pids"]:
                sol["items"].setdefault(
                    p_nsn,
                    {
                        "sn": sn,
                        "sn_formatted": pkg_meta["sn_formatted"],
                        "nsn": p_nsn,
                        "nomenclature": None,
                        "tech_docs_url": None,
                        "issue_date": issue_date,
                        "close_date": close_date,
                        "buyer_code": buyer_code,
                    },
                )

            # 2. Download {SN}PKG.ZIP first, then any unbundled docs
            docs_map = pkg_meta["docs"]
            pkg_zip_r2_key: str | None = None
            bundled_upper: set[str] = set()

            pkg_zip_fns = [fn for fn, role in docs_map.items() if role == "package_zip"]
            for zfn in pkg_zip_fns:
                zkey = f"{R2_PREFIX}/rfp_pkg_zip/{yr}/{zfn}"
                pkg_zip_r2_key = zkey
                if zkey in ok_map:
                    counts["already_landed"] += 1
                else:
                    st, nb, bset = _download_rfp_binary(dl, s3, conn, issue_date, "rfp_pkg_zip", zfn, zkey)
                    if st == "ok":
                        counts["pkg_zip_ok"] += 1
                        bytes_landed += nb
                        ok_map[zkey] = "ok"
                        bundled_upper |= bset
                    else:
                        counts["pkg_zip_failed"] += 1

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dibbs.rfp_documents
                            (solicitation_number, file_name, doc_role, source_url, r2_key, updated_at)
                        VALUES (%s, %s, %s, %s, %s, now())
                        ON CONFLICT (solicitation_number, file_name) DO UPDATE SET
                            doc_role = EXCLUDED.doc_role,
                            r2_key = EXCLUDED.r2_key,
                            updated_at = now()
                        """,
                        (sn, zfn, "package_zip", f"{DL_HOST}/Downloads/RFP/{zfn}", zkey),
                    )
                conn.commit()

            amendment_cnt = 0
            attachment_cnt = 0
            for dfn, role in docs_map.items():
                if role == "package_zip":
                    continue
                if role == "amendment":
                    amendment_cnt += 1
                elif role == "attachment":
                    attachment_cnt += 1

                # If PKG.ZIP already landed, its members are inside pkg_zip_r2_key;
                # only download standalone if PKG.ZIP is absent or failed, or if not in bundled_upper when known
                dkey = f"{R2_PREFIX}/rfp_doc/{yr}/{dfn}"
                if pkg_zip_r2_key and pkg_zip_r2_key in ok_map and (not bundled_upper or dfn.upper() in bundled_upper):
                    effective_r2_key = f"{pkg_zip_r2_key}#{dfn}"
                else:
                    effective_r2_key = dkey
                    if dkey in ok_map:
                        counts["already_landed"] += 1
                    else:
                        st, nb, _ = _download_rfp_binary(dl, s3, conn, issue_date, "rfp_doc", dfn, dkey)
                        if st == "ok":
                            counts["doc_ok"] += 1
                            bytes_landed += nb
                            ok_map[dkey] = "ok"
                        else:
                            counts["doc_failed"] += 1

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dibbs.rfp_documents
                            (solicitation_number, file_name, doc_role, source_url, r2_key, updated_at)
                        VALUES (%s, %s, %s, %s, %s, now())
                        ON CONFLICT (solicitation_number, file_name) DO UPDATE SET
                            doc_role = EXCLUDED.doc_role,
                            r2_key = EXCLUDED.r2_key,
                            updated_at = now()
                        """,
                        (sn, dfn, role, f"{DL_HOST}/Downloads/RFP/{dfn}", effective_r2_key),
                    )
                conn.commit()

            # Upsert dibbs.rfp_solicitations
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dibbs.rfp_solicitations
                        (solicitation_number, solicitation_formatted, instrument_type, total_items,
                         issue_date, close_date, buyer_code, package_view_r2_key, pkg_zip_r2_key,
                         amendment_count, attachment_count, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (solicitation_number) DO UPDATE SET
                        solicitation_formatted = COALESCE(EXCLUDED.solicitation_formatted, dibbs.rfp_solicitations.solicitation_formatted),
                        instrument_type = COALESCE(EXCLUDED.instrument_type, dibbs.rfp_solicitations.instrument_type),
                        total_items = COALESCE(EXCLUDED.total_items, dibbs.rfp_solicitations.total_items),
                        issue_date = COALESCE(EXCLUDED.issue_date, dibbs.rfp_solicitations.issue_date),
                        close_date = COALESCE(EXCLUDED.close_date, dibbs.rfp_solicitations.close_date),
                        buyer_code = COALESCE(EXCLUDED.buyer_code, dibbs.rfp_solicitations.buyer_code),
                        package_view_r2_key = EXCLUDED.package_view_r2_key,
                        pkg_zip_r2_key = COALESCE(EXCLUDED.pkg_zip_r2_key, dibbs.rfp_solicitations.pkg_zip_r2_key),
                        amendment_count = EXCLUDED.amendment_count,
                        attachment_count = EXCLUDED.attachment_count,
                        updated_at = now()
                    """,
                    (
                        sn,
                        pkg_meta["sn_formatted"],
                        pkg_meta["instrument_type"],
                        pkg_meta["total_items"] or len(sol["items"]),
                        issue_date,
                        close_date,
                        buyer_code,
                        pv_key,
                        pkg_zip_r2_key,
                        amendment_cnt,
                        attachment_cnt,
                    ),
                )
            conn.commit()
            counts["solicitations_upserted"] += 1

            # 3. Capture Pid.aspx for each (sn, nsn) item and upsert dibbs.rfp_items + dibbs.approved_makers
            for nsn, item_row in sol["items"].items():
                pid_fname = f"{sn}_{nsn}.html"
                pid_key = f"{R2_PREFIX}/pid_html/{yr}/{pid_fname}"
                pid_url = f"{WWW}/Rfp/Pid.aspx?sn={sn}&nsn={nsn}"
                pid_html: str | None = None

                if pid_key in ok_map:
                    counts["already_landed"] += 1
                    try:
                        pid_html = s3.get_object(Bucket=BUCKET, Key=pid_key)["Body"].read().decode("utf-8", errors="replace")
                    except Exception:  # noqa: BLE001
                        pid_html = None

                if pid_html is None:
                    try:
                        rpid = _polite_get(www, pid_url, timeout=120)
                        rpid.raise_for_status()
                        valid_pid, det_pid = _validate_pid(rpid.content)
                        if valid_pid:
                            s3.put_object(Bucket=BUCKET, Key=pid_key, Body=rpid.content)
                            _ledger_upsert(conn, issue_date, "pid_html", pid_fname, pid_url, pid_key,
                                           len(rpid.content), _sha256_bytes(rpid.content), "ok", "ok")
                            ok_map[pid_key] = "ok"
                            counts["pid_ok"] += 1
                            bytes_landed += len(rpid.content)
                            pid_html = rpid.content.decode("utf-8", errors="replace")
                        else:
                            _ledger_upsert(conn, issue_date, "pid_html", pid_fname, pid_url, pid_key,
                                           len(rpid.content), None, "validation_failed", det_pid)
                            counts["pid_failed"] += 1
                    except Exception as exc:  # noqa: BLE001
                        _ledger_upsert(conn, issue_date, "pid_html", pid_fname, pid_url, pid_key,
                                       None, None, "http_error", str(exc)[:500])
                        counts["pid_failed"] += 1

                pid_parsed = _parse_pid_html(pid_html) if pid_html else {
                    "pid_text": None,
                    "packaging_standard": None,
                    "has_csi": None,
                    "has_fat": None,
                    "has_higher_quality": None,
                    "makers": [],
                }

                fsc = nsn[:4] if len(nsn) >= 4 else None
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dibbs.rfp_items
                            (solicitation_number, nsn, fsc, nomenclature, tech_docs_url,
                             issue_date, close_date, buyer_code, pid_r2_key, pid_text,
                             packaging_standard, has_csi, has_fat, has_higher_quality,
                             approved_makers_json, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                        ON CONFLICT (solicitation_number, nsn) DO UPDATE SET
                            nomenclature = COALESCE(EXCLUDED.nomenclature, dibbs.rfp_items.nomenclature),
                            tech_docs_url = COALESCE(EXCLUDED.tech_docs_url, dibbs.rfp_items.tech_docs_url),
                            issue_date = COALESCE(EXCLUDED.issue_date, dibbs.rfp_items.issue_date),
                            close_date = COALESCE(EXCLUDED.close_date, dibbs.rfp_items.close_date),
                            buyer_code = COALESCE(EXCLUDED.buyer_code, dibbs.rfp_items.buyer_code),
                            pid_r2_key = COALESCE(EXCLUDED.pid_r2_key, dibbs.rfp_items.pid_r2_key),
                            pid_text = COALESCE(EXCLUDED.pid_text, dibbs.rfp_items.pid_text),
                            packaging_standard = COALESCE(EXCLUDED.packaging_standard, dibbs.rfp_items.packaging_standard),
                            has_csi = COALESCE(EXCLUDED.has_csi, dibbs.rfp_items.has_csi),
                            has_fat = COALESCE(EXCLUDED.has_fat, dibbs.rfp_items.has_fat),
                            has_higher_quality = COALESCE(EXCLUDED.has_higher_quality, dibbs.rfp_items.has_higher_quality),
                            approved_makers_json = EXCLUDED.approved_makers_json,
                            updated_at = now()
                        """,
                        (
                            sn,
                            nsn,
                            fsc,
                            item_row.get("nomenclature"),
                            item_row.get("tech_docs_url"),
                            issue_date,
                            close_date,
                            buyer_code,
                            pid_key if pid_html else None,
                            pid_parsed["pid_text"],
                            pid_parsed["packaging_standard"],
                            pid_parsed["has_csi"],
                            pid_parsed["has_fat"],
                            pid_parsed["has_higher_quality"],
                            Jsonb(pid_parsed["makers"]),
                        ),
                    )
                    for m in pid_parsed["makers"]:
                        cur.execute(
                            """
                            INSERT INTO dibbs.approved_makers
                                (nsn, maker_cage, part_number, maker_name, first_seen_date, last_seen_date)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (nsn, maker_cage, part_number) DO UPDATE SET
                                maker_name = COALESCE(EXCLUDED.maker_name, dibbs.approved_makers.maker_name),
                                first_seen_date = LEAST(dibbs.approved_makers.first_seen_date, EXCLUDED.first_seen_date),
                                last_seen_date = GREATEST(dibbs.approved_makers.last_seen_date, EXCLUDED.last_seen_date)
                            """,
                            (nsn, m["maker_cage"], m["part_number"], m["maker_name"], issue_date, issue_date),
                        )
                        counts["makers_upserted"] += 1
                conn.commit()
                counts["items_upserted"] += 1

        if counts["records_ok"] == 0 and counts["already_landed"] == 0:
            raise RuntimeError(f"no RFP record pages landed or verified: {counts}")

        final_status = "success"
        conn.close()
    except Exception as exc:  # noqa: BLE001
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
        raise RuntimeError(f"dibbs rfp capture failed: {error_text}")
    return {"feed": FEED, "status": final_status, "files": counts, "bytes_landed": int(bytes_landed)}


def apply_state_schema() -> None:
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(OPS_DDL)
    conn.commit()
    conn.close()
    print("Applied ops.dibbs_rfp_daily_r2_ingest_runs and dibbs.rfp_* schema.")
