"""Compute worker — SEC Form ADV Part 1 + ADV-W (Withdrawals) bulk ingest.

Part of the ``sec-adv-pipelines`` Modal app. Endpoint-less functions, spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints.
Clean-room data plane: no Iceberg, no Polaris — Python does acquisition / unzip / cp1252
transcode I/O only, DuckDB does 100% of the transform/cast, Lance is written straight to R2.

Two distinct firm-level Lance datasets, both keyed on the firm's CRD number:

    sec_adv_part1  ← adv-filing-data-<latest>-part1.zip :: IA_ADV_Base_A + ERA_ADV_Base
                     s3://data-sink/active/sec_adv_part1/   (one row per CRD, current state)
    sec_adv_w      ← advw-<widest>.zip :: ADVW_<dates>.csv (main withdrawal record)
                     s3://data-sink/active/sec_adv_w/       (one row per ADV-W filing)

Data acquisition (dynamic scrape + SEC FOIA compliance). The SEC edge WAF 403s any request
without a declared User-Agent (empirically confirmed: a compliant UA returns the normal app
response, an empty UA returns 403). EVERY sec.gov request — the FOIA HTML GET and the ZIP
downloads — carries ``SEC_USER_AGENT`` (env-overridable; default names a real contact per the
SEC FOIA automated-access guideline). The worker scrapes the Form ADV Data FOIA page, parses
its hrefs, and resolves the two target ZIP URLs dynamically (the SEC renames/repaths these
files over time — hyphen vs underscore, /files/ vs root — so nothing is hard-coded).

Three load-bearing findings from the live-source probe shaped this worker:
  1. The SEC ADV CSVs are **Windows-1252 (cp1252)**, not UTF-8 — DuckDB rejects cp1252's
     0x80-0x9F printables (smart quotes in firm names). Python transcodes cp1252→utf-8 while
     extracting each ZIP member (the SAM/PPP precedent: an I/O concern, lossless, single-byte
     so chunk boundaries never split a char). DuckDB then reads clean UTF-8.
  2. The CSVs are **not RFC-4180 compliant** (mixed quoting, ragged rows) — the read_csv_auto
     sniffer bails. We pin the dialect (delim/quote/escape) + strict_mode=false + null_padding
     + store_rejects, the fl_sos precedent. all_varchar=true + TRY_CAST is the casting spine.
  3. The Part 1 base header is **item-code** (1E1=CRD, 1D=SEC#, 1F1-State=office state,
     1P=LEI, 5F2c=regulatory AUM); ADV-W is **named** ("CRD Number", "Office State"). The
     LEI field (1P) is dirty — real 20-char LEIs mixed with "N/A" / mis-filed numbers — so it
     is validated to the GLEIF format ([A-Z0-9]{20}) before becoming the corporate-spine FK.
     ERA base lacks IA-only columns (e.g. 5F2c); the projection NULL-guards absent columns.

Identity resolution. crd_number (FINRA BrokerCheck PK) gets a hard BTREE on both datasets;
lei (GLEIF FK, Part 1, built only when populated) and business_address_state get BTREEs too.

Control plane (Trigger v4 durable callback): each function accepts ``trigger_callback_url``
and, on terminal state (success OR failure), (1) writes a run row to ``ops.sec_adv_runs`` via
psycopg and (2) POSTs a FLAT JSON body to that url. No ``{"data": ...}`` envelope.

    modal deploy pipelines/sec_adv/ingest.py
    modal run    pipelines/sec_adv/ingest.py::migrate                 # create ops.sec_adv_runs
    modal run    pipelines/sec_adv/ingest.py::backfill                # part1 + advw (historical)
    modal run    pipelines/sec_adv/ingest.py::ingest --dataset part1
    modal run    pipelines/sec_adv/ingest.py::reindex_one --dataset advw
    modal run    pipelines/sec_adv/ingest.py::private_funds            # Schedule D 7.B.1/7.B.2/7.A

Sibling: private_funds_transform.py holds the (Modal-free) 7.B.1/7.B.2/7.A projection + robust CSV
parser, imported by ``ingest_private_funds`` here and reused by local build scripts.
"""

from __future__ import annotations

import os

import modal

try:  # same-dir when `modal deploy pipelines/sec_adv/ingest.py`; package path from repo root
    from pipelines.sec_adv import private_funds_transform as PFT
except ImportError:  # pragma: no cover
    import private_funds_transform as PFT

SEC_BASE = "https://www.sec.gov"
# Live canonical FOIA page (the directive's /foia/docs/foia-investadviser now 404s — the SEC
# moved the Form ADV downloads here, with #part1 / #advw sections). Env-overridable.
DEFAULT_FOIA_URL = "https://www.sec.gov/foia-services/frequently-requested-documents/form-adv-data"
# SEC FOIA automated-access compliance: a declared UA naming a real contact. Env-overridable.
DEFAULT_SEC_USER_AGENT = "Core-X Data Engine (tools@substrate.build)"

SCRATCH_DIR = "/tmp"

# Lance fragment sizing — fleet-standard (NY/CA SoS, USPTO, FL SoS). 90 GiB = the Lance default.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new datasets → pin the current Lance default storage format.
DATA_STORAGE_VERSION = "2.1"

# DuckDB CSV read flags for the messy, non-RFC-4180, cp1252→utf8-transcoded SEC files.
# all_varchar = zero type-inference surprises; explicit dialect because the auto-sniffer cannot
# detect it; strict_mode=false + null_padding tolerate ragged rows; store_rejects captures the
# (near-zero) malformed remainder without aborting the load.
_CSV_FLAGS = (
    "all_varchar=true, header=true, delim=',', quote='\"', escape='\"', "
    "strict_mode=false, null_padding=true, store_rejects=true, max_line_size=8000000"
)
# Same dialect, no reject-logging — for the cheap column-probe / row-count reads so they do
# not double-count into reject_errors (only the real projection reads carry store_rejects).
_CSV_PROBE_FLAGS = (
    "all_varchar=true, header=true, delim=',', quote='\"', escape='\"', "
    "strict_mode=false, null_padding=true, ignore_errors=true, max_line_size=8000000"
)

# Per-dataset typed-spine field plan: (out_name, source_column_or_None, kind). kind ∈
# {str, lei, bigint, ts, date}. A source column absent from a given member (e.g. 5F2c in the
# ERA base) is emitted as a typed NULL so the per-member projections UNION cleanly.
PART1_FIELDS: list[tuple[str, str, str]] = [
    ("crd_number", "1E1", "str"),               # numeric CRD — FINRA BrokerCheck PK
    ("sec_number", "1D", "str"),                # 801-/802- SEC file number
    ("lei", "1P", "lei"),                        # validated 20-char GLEIF LEI (FK)
    ("legal_name", "1A", "str"),
    ("primary_business_name", "1B1", "str"),
    ("business_address_street1", "1F1-Street 1", "str"),
    ("business_address_street2", "1F1-Street 2", "str"),
    ("business_address_city", "1F1-City", "str"),
    ("business_address_state", "1F1-State", "str"),
    ("business_address_country", "1F1-Country", "str"),
    ("business_address_postal", "1F1-Postal", "str"),
    ("large_fund_adviser_flag", "1O", "str"),   # "$1B+ in assets" Y/N (NOT the LEI)
    ("regulatory_aum", "5F2c", "bigint"),        # IA-only; NULL for ERA filers
    ("filing_id", "FilingID", "str"),
    ("form_version", "FormVersion", "str"),
    ("date_submitted", "DateSubmitted", "ts"),
]

ADVW_FIELDS: list[tuple[str, str, str]] = [
    ("crd_number", "CRD Number", "str"),
    ("sec_number", "SEC File Number", "str"),
    ("legal_name", "Full Legal Name", "str"),
    ("primary_business_name", "Primary Business Name", "str"),
    ("filing_id", "Filing ID", "str"),
    ("form_type", "Form Type", "str"),
    ("filing_type", "Filing Type", "str"),
    ("filing_date", "Filing Date", "ts"),
    ("business_address_street1", "Office Street 1", "str"),
    ("business_address_street2", "Office Street 2", "str"),
    ("business_address_city", "Office City", "str"),
    ("business_address_state", "Office State", "str"),
    ("business_address_country", "Office Country", "str"),
    ("business_address_postal", "Office Postal Code", "str"),
    ("business_ceased", "Business Ceased", "str"),
    ("date_ceased", "Date Ceased", "date"),
    ("reasons_for_withdrawal", "Reasons for Withdrawal", "str"),
]

# Dataset registry. ``members`` = ordered (regex, filer_type) pairs selecting the firm-level
# CSV member(s) inside the ZIP (Part 1 unions the registered-IA + exempt-reporting-adviser
# bases; ADV-W takes only the main ADVW_<dates>.csv, excluding the _W1_/_W2_ schedules).
DATASETS: dict[str, dict] = {
    "part1": {
        "feed": "sec_adv_part1",
        "lance_uri": os.environ.get("SEC_ADV_PART1_LANCE_URI", "s3://data-sink/active/sec_adv_part1/"),
        "url_kind": "part1",
        "members": [
            (r"IA_ADV_Base_A.*\.csv$", "IA"),     # registered investment advisers
            (r"ERA_ADV_Base.*\.csv$", "ERA"),     # exempt reporting advisers
        ],
        "fields": PART1_FIELDS,
        "dedup_key": "crd_number",
        "dedup_order": "date_submitted DESC NULLS LAST, filing_id DESC NULLS LAST",
        "btree": ["crd_number", "lei", "business_address_state"],
    },
    "advw": {
        "feed": "sec_adv_w",
        "lance_uri": os.environ.get("SEC_ADV_W_LANCE_URI", "s3://data-sink/active/sec_adv_w/"),
        "url_kind": "advw",
        "members": [
            (r"ADVW_\d+_\d+\.csv$", None),        # main withdrawal record (excludes _W1_/_W2_)
        ],
        "fields": ADVW_FIELDS,
        "dedup_key": "filing_id",
        "dedup_order": "filing_date DESC NULLS LAST",
        "btree": ["crd_number", "business_address_state"],
    },
}

# Idempotent ops.* DDL — mirror of pipelines/sec_adv/ops_sec_adv_runs.sql (source of truth).
_OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sec_adv_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset         text        NOT NULL,
    feed            text        NOT NULL,
    source_url      text,
    dataset_uri     text,
    as_of           text,
    source_files    jsonb,
    rows_processed  bigint,
    rows_written    bigint,
    rejected_rows   bigint,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sec_adv_runs_dataset_idx     ON ops.sec_adv_runs (dataset);
CREATE INDEX IF NOT EXISTS sec_adv_runs_feed_idx        ON ops.sec_adv_runs (feed);
CREATE INDEX IF NOT EXISTS sec_adv_runs_status_idx      ON ops.sec_adv_runs (status);
CREATE INDEX IF NOT EXISTS sec_adv_runs_as_of_idx       ON ops.sec_adv_runs (as_of DESC);
CREATE INDEX IF NOT EXISTS sec_adv_runs_recorded_at_idx ON ops.sec_adv_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "requests>=2.32",        # sec.gov scrape + download; Trigger callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    # BTREE scalar-index builds sort the column; Lance's spill-to-disk sorter OOMs the bounded
    # DataFusion pool on high-cardinality crd_number at scale. Force the in-memory sort path.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("sec-adv-pipelines", image=image)


# ──────────────────────────────────────────────────────────────────────────────
# R2 / object-store
# ──────────────────────────────────────────────────────────────────────────────
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret."""
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


# ──────────────────────────────────────────────────────────────────────────────
# SEC acquisition (Python = I/O + compliance only)
# ──────────────────────────────────────────────────────────────────────────────
def _sec_user_agent() -> str:
    return os.environ.get("SEC_USER_AGENT", DEFAULT_SEC_USER_AGENT)


def _scrape_foia_links(page_url: str, ua: str) -> list[str]:
    """GET the Form ADV FOIA page with the compliant UA and return every absolute ZIP href.
    The UA is mandatory — sec.gov 403s an undeclared agent at the edge."""
    import re
    import urllib.parse

    import requests

    resp = requests.get(page_url, headers={"User-Agent": ua}, timeout=(30, 120))
    resp.raise_for_status()
    hrefs = re.findall(r'href="([^"]+)"', resp.text, flags=re.IGNORECASE)
    links: list[str] = []
    seen: set[str] = set()
    for h in hrefs:
        if ".zip" not in h.lower():
            continue
        absolute = urllib.parse.urljoin(SEC_BASE + "/", h)
        if absolute not in seen:
            seen.add(absolute)
            links.append(absolute)
    return links


def _date_tokens(name: str) -> tuple[int, int]:
    """(start, end) from the YYYYMMDD tokens in a file basename; (0, 0) if none. Used to pick
    the latest/widest member from a family of date-ranged ZIPs."""
    import re

    nums = [int(x) for x in re.findall(r"\d{8}", name)]
    if not nums:
        return (0, 0)
    return (min(nums), max(nums))


def _resolve_url(dataset: str, links: list[str]) -> tuple[str, str]:
    """Resolve a dataset's target ZIP URL from the scraped FOIA links + derive its as_of label.

    part1 → the current-regime Form ADV Part 1 filing-data ZIP (``adv-filing-data-*-part1.zip``),
            latest by end-date. (part2 holds only Schedule-D child tables — no base record; the
            pre-2011 legacy ZIP is a different schema generation and is deliberately excluded.)
    advw  → the ADV-W withdrawal ZIP with the latest end-date, tie-broken to the widest coverage
            (so the full-history ``advw-<start>-<end>.zip`` wins over a single monthly slice).
    """
    import re

    kind = DATASETS[dataset]["url_kind"]
    cands: list[str] = []
    for url in links:
        base = url.rsplit("/", 1)[-1].lower()
        if kind == "part1":
            if re.search(r"adv-filing-data-.*-part1\.zip$", base):
                cands.append(url)
        else:  # advw
            if re.match(r"advw[-_].*\.zip$", base) and "part2" not in base:
                cands.append(url)
    if not cands:
        raise RuntimeError(f"no {kind} ZIP found on the FOIA page ({len(links)} zip links scanned)")

    if kind == "part1":
        chosen = max(cands, key=lambda u: _date_tokens(u.rsplit("/", 1)[-1]))  # latest end-date
    else:
        # latest end-date, then widest span (earliest start) — favours the complete-history file.
        chosen = max(cands, key=lambda u: (_date_tokens(u.rsplit("/", 1)[-1])[1],
                                           -_date_tokens(u.rsplit("/", 1)[-1])[0]))
    start, end = _date_tokens(chosen.rsplit("/", 1)[-1])
    as_of = f"{start}-{end}" if start or end else chosen.rsplit("/", 1)[-1]
    return chosen, as_of


def _download(url: str, ua: str, dest: str) -> int:
    """Stream a sec.gov ZIP to local scratch with the compliant UA. Returns bytes written."""
    import requests

    total = 0
    with requests.get(url, headers={"User-Agent": ua}, stream=True, timeout=(30, 900)) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)
                    total += len(chunk)
    return total


def _extract_members(zip_path: str, member_specs: list[tuple[str, str | None]],
                     out_dir: str) -> list[tuple[str, str | None, str]]:
    """Extract the firm-level CSV member(s) from the ZIP, transcoding cp1252→utf-8 on the way to
    disk (streamed in 1 MiB chunks; cp1252 is single-byte so chunk boundaries never split a
    character — lossless). Returns [(utf8_path, filer_type, member_basename), ...].

    These ZIPs are ordinary Deflate (method 8) — stdlib zipfile decodes them (no p7zip needed)."""
    import os.path
    import re
    import zipfile

    out: list[tuple[str, str | None, str]] = []
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        for pattern, filer_type in member_specs:
            rx = re.compile(pattern, re.IGNORECASE)
            matched = sorted(n for n in names if rx.search(n.rsplit("/", 1)[-1]))
            if not matched:
                raise RuntimeError(f"no member matches /{pattern}/ in {zip_path.rsplit('/',1)[-1]}")
            for member in matched:
                base = member.rsplit("/", 1)[-1]
                utf8_path = os.path.join(out_dir, f"{filer_type or 'main'}__{base}")
                with zf.open(member) as src, open(utf8_path, "wb") as dst:
                    while True:
                        chunk = src.read(1 << 20)
                        if not chunk:
                            break
                        dst.write(chunk.decode("cp1252", errors="replace").encode("utf-8"))
                out.append((utf8_path, filer_type, base))
    return out


# ──────────────────────────────────────────────────────────────────────────────
# DuckDB transform (100%): cp1252→utf8 CSV → typed firm spine + lossless raw_filing JSON
# ──────────────────────────────────────────────────────────────────────────────
def _field_expr(source_col: str | None, kind: str, present: bool) -> str:
    """SQL expression for one typed-spine field. Absent source column → a typed NULL so the
    per-member projections UNION cleanly across heterogeneous schemas (IA 243 vs ERA 111 cols)."""
    if not present or source_col is None:
        cast = {"bigint": "BIGINT", "ts": "TIMESTAMP", "date": "DATE"}.get(kind, "VARCHAR")
        return f"CAST(NULL AS {cast})"
    qc = '"' + source_col.replace('"', '""') + '"'
    if kind == "lei":
        # GLEIF format gate: a valid LEI is exactly 20 [A-Z0-9]; "N/A", mis-filed SEC#s → NULL.
        return f"CASE WHEN regexp_full_match(upper(trim({qc})), '[A-Z0-9]{{20}}') THEN upper(trim({qc})) END"
    if kind == "bigint":
        return f"TRY_CAST(nullif(trim({qc}), '') AS BIGINT)"
    if kind == "ts":
        return (f"TRY_CAST(coalesce(TRY_STRPTIME(nullif(trim({qc}), ''), '%m/%d/%Y %I:%M:%S %p'), "
                f"TRY_STRPTIME(nullif(trim({qc}), ''), '%m/%d/%Y')) AS TIMESTAMP)")
    if kind == "date":
        return (f"TRY_CAST(coalesce(TRY_STRPTIME(nullif(trim({qc}), ''), '%m/%d/%Y %I:%M:%S %p'), "
                f"TRY_STRPTIME(nullif(trim({qc}), ''), '%m/%d/%Y'), "
                f"TRY_STRPTIME(nullif(trim({qc}), ''), '%Y-%m-%d')) AS DATE)")
    return f"nullif(trim({qc}), '')"


def _member_projection(path: str, fields: list[tuple[str, str, str]], existing: set[str],
                       filer_type: str | None, source_file: str, snapshot: str) -> str:
    """One member's projected SELECT: typed firm spine + filer_type/source provenance + the
    FULL original row preserved losslessly as raw_filing JSON (to_json over the read alias —
    no projection loss, every item code recoverable downstream)."""
    cols = ",\n        ".join(
        f"{_field_expr(src, kind, src in existing)} AS {out}" for out, src, kind in fields
    )
    ft = "CAST(NULL AS VARCHAR)" if filer_type is None else "'" + filer_type.replace("'", "''") + "'"
    src_lit = "'" + source_file.replace("'", "''") + "'"
    return (
        "    SELECT\n"
        f"        {cols},\n"
        f"        {ft} AS filer_type,\n"
        f"        {src_lit} AS source_file,\n"
        "        to_json(raw) AS raw_filing,\n"
        f"        CAST('{snapshot}' AS DATE) AS snapshot_date,\n"
        "        now() AS ingested_at\n"
        f"    FROM read_csv('{path}', {_CSV_FLAGS}) AS raw\n"
    )


def _build_dataset_arrow(con, members: list[tuple[str, str | None, str]], cfg: dict, snapshot: str):
    """Read every member, UNION the projections, collapse to one row per dedup key (latest
    snapshot wins → a clean firm spine with a unique resolution key), return the Arrow table.
    Returns (arrow_table, rows_pre_dedup)."""
    parts = []
    rows_pre = 0
    for path, filer_type, base in members:
        existing = set(con.sql(f"SELECT * FROM read_csv('{path}', {_CSV_PROBE_FLAGS}) LIMIT 0").columns)
        rows_pre += con.sql(f"SELECT count(*) AS n FROM read_csv('{path}', {_CSV_PROBE_FLAGS})").fetchone()[0]
        parts.append(_member_projection(path, cfg["fields"], existing, filer_type, base, snapshot))

    union_sql = "\n    UNION ALL BY NAME\n".join(parts)
    mk = cfg["dedup_key"]
    order = cfg["dedup_order"]
    final_sql = (
        "SELECT * EXCLUDE (__rn) FROM (\n"
        "  SELECT *, row_number() OVER (PARTITION BY "
        f'"{mk}" ORDER BY {order}) AS __rn\n'
        f"  FROM (\n{union_sql}\n  )\n"
        f'  WHERE "{mk}" IS NOT NULL\n'
        ") WHERE __rn = 1"
    )
    return con.sql(final_sql).to_arrow_table(), int(rows_pre)


# ──────────────────────────────────────────────────────────────────────────────
# Lance write / index
# ──────────────────────────────────────────────────────────────────────────────
def _create_indexes(cfg: dict, table, so: dict) -> list[str]:
    """Build BTREE scalar indexes on the resolution keys (crd_number, lei, business_address_state).
    create_scalar_index defaults to replace=True → idempotent. The lei index is built only when
    the column is populated (directive: "lei (if populated)"). An index miss is logged, never
    fatal — the Lance data write is the critical artifact."""
    import lance
    import pyarrow.compute as pc

    ds = lance.dataset(cfg["lance_uri"], storage_options=so)
    built: list[str] = []
    for col in cfg["btree"]:
        if col == "lei" and (col not in table.column_names or pc.count(table[col]).as_py() == 0):
            print("  BTREE  – lei skipped (column empty)")
            continue
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001 — index miss must not fail the load
            print(f"  WARN BTREE {col} failed: {exc}")
    return built


# ──────────────────────────────────────────────────────────────────────────────
# State + callback
# ──────────────────────────────────────────────────────────────────────────────
def _record_run(dataset, feed, source_url, dataset_uri, as_of, source_files, rows_processed,
                rows_written, rejected, status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.sec_adv_runs (psycopg). Best-effort: never let an audit-write
    failure crash an otherwise-good ingest."""
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.sec_adv_runs
                    (dataset, feed, source_url, dataset_uri, as_of, source_files,
                     rows_processed, rows_written, rejected_rows, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (dataset, feed, source_url, dataset_uri, as_of, Jsonb(source_files),
                 rows_processed, rows_written, rejected, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url. FLAT JSON body — NO
    {"data": ...} envelope, NO API key (the callbackHash in the url is the auth)."""
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


def _resolve_dataset(dataset: str) -> dict:
    key = dataset.strip().lower()
    if key not in DATASETS:
        raise ValueError(f"dataset must be one of {sorted(DATASETS)}, got {dataset!r}")
    return DATASETS[key]


# ──────────────────────────────────────────────────────────────────────────────
# Workers
# ──────────────────────────────────────────────────────────────────────────────
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,            # 702 MB download + ~600k-row transform + BTREE build
    memory=8192,               # 8 GB (directive); DuckDB spills to /tmp, Lance sorts in-RAM
    cpu=4.0,
)
def ingest_dataset(dataset: str, url: str | None = None, foia_url: str | None = None,
                   trigger_callback_url: str | None = None) -> dict:
    """Scrape → resolve URL → download ZIP (SEC UA) → cp1252→utf8 extract → DuckDB typed
    projection + dedup → Arrow → Lance overwrite → BTREE indexes → cleanup /tmp; record ops.*
    and wake Trigger. ``url`` pins an explicit ZIP (skips the scrape); ``foia_url`` overrides
    the page. mode is always overwrite — a monthly full snapshot; Lance versioning retains
    prior snapshots for time-travel. Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt
    import os.path

    import duckdb
    import lance

    cfg = _resolve_dataset(dataset)
    dskey = dataset.strip().lower()
    ua = _sec_user_agent()
    started_at = dt.datetime.now(dt.timezone.utc)
    snapshot = started_at.date().isoformat()
    rows_processed, rows_written, rejected = 0, 0, 0
    status, error = "error", None
    source_url, as_of = url, snapshot
    processed: list[str] = []
    local_paths: list[str] = []

    try:
        so = _r2_storage_options()

        # 1) Resolve the target ZIP URL (dynamic scrape unless explicitly pinned).
        if not source_url:
            links = _scrape_foia_links(foia_url or os.environ.get("SEC_ADV_FOIA_URL", DEFAULT_FOIA_URL), ua)
            source_url, as_of = _resolve_url(dskey, links)
        else:
            s, e = _date_tokens(source_url.rsplit("/", 1)[-1])
            as_of = f"{s}-{e}" if s or e else snapshot
        print(f"[{dskey}] source → {source_url} (as_of={as_of})")

        # 2) Python I/O: download the ZIP, then cp1252→utf8 extract the firm-level member(s).
        zip_path = os.path.join(SCRATCH_DIR, f"sec_adv_{dskey}.zip")
        local_paths.append(zip_path)
        nbytes = _download(source_url, ua, zip_path)
        print(f"[{dskey}] downloaded {nbytes / 1024**2:.1f} MiB → extracting members")
        members = _extract_members(zip_path, cfg["members"], SCRATCH_DIR)
        local_paths.extend(p for p, _, _ in members)
        processed = [base for _, _, base in members]
        print(f"[{dskey}] members: {processed}")

        # 3) DuckDB: 100% of the transform — typed spine + lossless raw_filing, deduped firm spine.
        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            con.execute("SET enable_progress_bar=false;")
            con.execute("SET memory_limit='6GB';")
            con.execute("SET temp_directory='/tmp/duckdb_spill';")
            table, rows_processed = _build_dataset_arrow(con, members, cfg, snapshot)
            try:
                rejected = int(con.sql("SELECT count(*) AS n FROM reject_errors").fetchone()[0])
            except Exception:  # noqa: BLE001 — no reject table ⇒ zero rejects
                rejected = 0
        finally:
            con.close()
        rows_written = table.num_rows
        print(f"[{dskey}] {rows_processed:,} rows read → {rows_written:,} deduped "
              f"({rejected:,} rejected)")

        # 4) Python: commit the Arrow table to R2 as Lance (overwrite snapshot), then index.
        lance.write_dataset(
            table, cfg["lance_uri"], mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        print(f"[{dskey}] wrote Lance dataset → {cfg['lance_uri']}; building indexes…")
        _create_indexes(cfg, table, so)
        del table
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        # 5) Cleanup: explicitly delete every local scratch file before returning.
        for p in local_paths:
            try:
                os.remove(p)
            except OSError:
                pass
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(dskey, cfg["feed"], source_url, cfg["lance_uri"], as_of, processed,
                    int(rows_processed), int(rows_written), int(rejected), status, error,
                    started_at, completed_at)
        _post_callback(trigger_callback_url, {
            "status": status, "rows": int(rows_written), "feed": cfg["feed"], "dataset": dskey,
            "dataset_uri": cfg["lance_uri"], "as_of": as_of, "source_url": source_url,
        })

    if status != "success":
        raise RuntimeError(f"sec_adv ingest failed for {dskey}: {error}")
    return {"feed": cfg["feed"], "dataset": dskey, "rows_processed": int(rows_processed),
            "rows": int(rows_written), "rejected_rows": int(rejected),
            "dataset_uri": cfg["lance_uri"], "as_of": as_of, "source_url": source_url,
            "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 30, memory=8192, cpu=4.0,
)
def reindex(dataset: str) -> dict:
    """Rebuild the BTREE scalar indexes on an existing dataset (no re-ingest)."""
    import lance

    cfg = _resolve_dataset(dataset)
    so = _r2_storage_options()
    table = lance.dataset(cfg["lance_uri"], storage_options=so).to_table()
    print(f"Reindexing {cfg['lance_uri']} — {table.num_rows:,} rows")
    built = _create_indexes(cfg, table, so)
    return {"dataset": dataset.strip().lower(), "dataset_uri": cfg["lance_uri"],
            "rows": table.num_rows, "indexes": built}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def apply_migration() -> dict:
    """Create ops.sec_adv_runs (idempotent). Mirrors ops_sec_adv_runs.sql."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(_OPS_DDL)
        conn.commit()
        cur.execute("SELECT to_regclass('ops.sec_adv_runs')")
        present = cur.fetchone()[0]
    print(f"ops.sec_adv_runs present = {present}")
    return {"table": "ops.sec_adv_runs", "present": present}


# ══════════════════════════════════════════════════════════════════════════════
# Schedule D 7.B.1 (private funds) / 7.B.2 (reliance) / 7.A (financial-industry affiliations)
#
# These child tables live in the part2 filing-data ZIP that the base ingest (above) skips.
# ONE worker self-fetches BOTH ZIPs — part2 for the 7.B.1 / 7.B.2 / 7.A families, part1 for the
# ADV base file (the complete FilingID→CRD map the full-history dataset needs) — robust-parses
# each member (private_funds_transform.csv_to_arrow; DuckDB's C CSV parser silently corrupts the
# unescaped-quote CLO / credit / VC rows) and projects FOUR Lance datasets:
#     sec_adv_private_funds           7.B.1 current-state (latest filing per adviser)
#     sec_adv_private_funds_history   7.B.1 full history  (every fund-filing, crd from the map)
#     sec_adv_private_fund_reliance   7.B.2
#     sec_adv_related_persons         7.A current-state
# Transform logic lives in private_funds_transform.py; this worker is fetch + orchestration only.
# ══════════════════════════════════════════════════════════════════════════════

PF_DATASETS = {
    "sec_adv_private_funds":         os.environ.get("SEC_ADV_PF_LANCE_URI", "s3://data-sink/active/sec_adv_private_funds/"),
    "sec_adv_private_funds_history": os.environ.get("SEC_ADV_PF_HIST_LANCE_URI", "s3://data-sink/active/sec_adv_private_funds_history/"),
    "sec_adv_private_fund_reliance": os.environ.get("SEC_ADV_PF_REL_LANCE_URI", "s3://data-sink/active/sec_adv_private_fund_reliance/"),
    "sec_adv_related_persons":       os.environ.get("SEC_ADV_RP_LANCE_URI", "s3://data-sink/active/sec_adv_related_persons/"),
}
HISTORY_BUCKETS = 12            # FilingID-hash partitions — bounds the 17 child aggregations' RAM
HISTORY_ROWS_PER_FILE = 130_000  # R2-multipart-safe Lance file size for the nested history dataset


def _resolve_part_zip_url(links: list[str], part: int) -> str:
    """Latest adv-filing-data-*-part{part}.zip from the scraped FOIA links."""
    import re

    pat = re.compile(rf"adv-filing-data-.*-part{part}\.zip$", re.IGNORECASE)
    cands = [u for u in links if pat.search(u.rsplit("/", 1)[-1])]
    if not cands:
        raise RuntimeError(f"no part{part} ZIP on the FOIA page ({len(links)} zip links scanned)")
    return max(cands, key=lambda u: _date_tokens(u.rsplit("/", 1)[-1]))


def _filing_crd_map_arrow(part1_zip: str, work: str):
    """Complete FilingID→CRD map (all history) from the ADV base members of part1.zip. Only two
    columns parsed (FilingID + item 1E1 = CRD); one row per filing — the key the full-history fund
    dataset needs to attribute historical fund-filings to advisers."""
    import csv
    import os.path
    import re
    import zipfile

    import pyarrow as pa

    csv.field_size_limit(50_000_000)
    fids: list[str] = []
    crds: list[str | None] = []
    with zipfile.ZipFile(part1_zip) as zf:
        members = [m for m in zf.namelist()
                   if re.search(r"(IA_ADV_Base_A|ERA_ADV_Base).*\.csv$", m.rsplit("/", 1)[-1], re.I)]
        for m in members:
            tmp = os.path.join(work, "map_" + m.rsplit("/", 1)[-1])
            with zf.open(m) as src, open(tmp, "wb") as o:
                while True:
                    c = src.read(1 << 20)
                    if not c:
                        break
                    o.write(c.decode("cp1252", errors="replace").encode("utf-8"))
            with open(tmp, newline="", encoding="utf-8") as fh:
                r = csv.reader(fh)
                hdr = next(r)
                fi = hdr.index("FilingID") if "FilingID" in hdr else 0
                ci = hdr.index("1E1") if "1E1" in hdr else None
                for row in r:
                    if ci is None or len(row) <= max(fi, ci):
                        continue
                    fid = row[fi].strip()
                    if fid:
                        fids.append(fid)
                        crds.append(row[ci].strip() or None)
            os.remove(tmp)
    return pa.table({"filing_id": pa.array(fids, pa.string()), "crd_number": pa.array(crds, pa.string())})


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90,      # 2 downloads (~1.1 GB) + robust parse + 4 datasets incl. bucketed history
    memory=32768,         # holds the 1.65M-row history table for the single R2-safe write
    cpu=8.0,
)
def ingest_private_funds(foia_url: str | None = None, part2_url: str | None = None,
                         part1_url: str | None = None, trigger_callback_url: str | None = None) -> dict:
    """Self-fetch part2 (+ part1 base for the history map) → robust csv parse → DuckDB projections
    → four Lance datasets → BTREE/BITMAP indexes → ops.sec_adv_runs + Trigger callback. Reads the
    freshly-built sec_adv_part1 for current-state adviser context, so run it AFTER the part1 dataset
    is current for the period."""
    import datetime as dt
    import os.path
    import shutil

    import duckdb
    import lance
    import pyarrow.parquet as pq

    ua = _sec_user_agent()
    started_at = dt.datetime.now(dt.timezone.utc)
    snapshot = started_at.date().isoformat()
    status, error = "error", None
    counts: dict[str, int] = {}
    work = os.path.join(SCRATCH_DIR, "sec_adv_pf")
    pqdir = os.path.join(work, "pq")
    histpq = os.path.join(work, "hist_pq")

    try:
        so = _r2_storage_options()
        shutil.rmtree(work, ignore_errors=True)
        os.makedirs(pqdir, exist_ok=True)
        os.makedirs(histpq, exist_ok=True)

        # 1) Resolve + download the two ZIPs (part2 = child tables, part1 = base for the crd map).
        if not (part2_url and part1_url):
            links = _scrape_foia_links(foia_url or os.environ.get("SEC_ADV_FOIA_URL", DEFAULT_FOIA_URL), ua)
            part2_url = part2_url or _resolve_part_zip_url(links, 2)
            part1_url = part1_url or _resolve_part_zip_url(links, 1)
        p2 = os.path.join(work, "part2.zip")
        p1 = os.path.join(work, "part1.zip")
        print(f"[pf] part2 → {part2_url}")
        _download(part2_url, ua, p2)
        print(f"[pf] part1 → {part1_url}")
        _download(part1_url, ua, p1)

        # 2) Extract + cp1252→utf8 transcode the 7.B.1 + 7.A families, robust-parse each → parquet
        #    on disk (keeps the DuckDB projections streaming from disk, not holding 22 tables in RAM).
        specs = [(rx, k) for k, rx in {**PFT.PF_MEMBERS, **PFT.RP_MEMBERS}.items()]
        members = _extract_members(p2, specs, work)  # [(utf8_path, short_key, base)]
        for path, short, _ in members:
            pq.write_table(PFT.csv_to_arrow(path), os.path.join(pqdir, f"{short}.parquet"), compression="zstd")
            os.remove(path)
        os.remove(p2)
        src = lambda k: f"read_parquet('{os.path.join(pqdir, k + '.parquet')}')"  # noqa: E731

        con = duckdb.connect(":memory:")
        con.execute("PRAGMA threads=6;")
        con.execute("SET memory_limit='16GB';")
        con.execute("SET preserve_insertion_order=false;")
        con.execute(f"SET temp_directory='{os.path.join(work, 'spill')}';")

        # 3) Current-filing adviser context from the freshly-built sec_adv_part1 Lance; the complete
        #    FilingID→CRD map (all history) from the part1 base file.
        part1_uri = DATASETS["part1"]["lance_uri"].rstrip("/")
        con.register("part1", lance.dataset(part1_uri, storage_options=so).to_table(
            columns=["crd_number", "filing_id", "filer_type", "legal_name", "regulatory_aum", "lei"]))
        con.register("filing_crd_map", _filing_crd_map_arrow(p1, work))
        os.remove(p1)

        def _write(name: str, table, rows_per_file: int = MAX_ROWS_PER_FILE) -> None:
            # Always a single complete-table write: 12x append corrupts the sparse marketer_websites
            # LIST<STRUCT>, and a streamed write trips R2's uniform-multipart-part rule — one
            # in-memory table write with an R2-safe max_rows_per_file avoids both.
            lance.write_dataset(table, PF_DATASETS[name], mode="overwrite",
                                data_storage_version=DATA_STORAGE_VERSION, max_rows_per_file=rows_per_file,
                                max_bytes_per_file=MAX_BYTES_PER_FILE, storage_options=so)

        def _index(name: str) -> int:
            ds = lance.dataset(PF_DATASETS[name], storage_options=so)
            plan = PFT.INDEX_PLAN[name]
            for col in plan["btree"]:
                try:
                    ds.create_scalar_index(col, index_type="BTREE", replace=True)
                except Exception as exc:  # noqa: BLE001 — index miss is never fatal
                    print(f"  WARN BTREE {name}.{col}: {exc}")
            for col in plan["bitmap"]:
                try:
                    ds.create_scalar_index(col, index_type="BITMAP", replace=True)
                except Exception as exc:  # noqa: BLE001
                    print(f"  WARN BITMAP {name}.{col}: {exc}")
            return lance.dataset(PF_DATASETS[name], storage_options=so).count_rows()

        # 4) Current-state funds + reliance + related persons (single complete-table writes).
        _write("sec_adv_private_funds", con.sql(PFT.build_funds_sql(src, snapshot)).to_arrow_table())
        counts["sec_adv_private_funds"] = _index("sec_adv_private_funds")
        _write("sec_adv_private_fund_reliance", con.sql(PFT.build_reliance_sql(src, snapshot)).to_arrow_table())
        counts["sec_adv_private_fund_reliance"] = _index("sec_adv_private_fund_reliance")
        _write("sec_adv_related_persons", con.sql(PFT.build_related_persons_sql(src, snapshot)).to_arrow_table())
        counts["sec_adv_related_persons"] = _index("sec_adv_related_persons")

        # 5) Full-history funds — project each FilingID-hash bucket to disk (bounds the 17 child
        #    aggregations' memory), then read them back as ONE table for a single R2-safe write.
        for b in range(HISTORY_BUCKETS):
            cf = (f"SELECT filing_id, crd_number, NULL::VARCHAR AS filer_type, "
                  f"NULL::VARCHAR AS adviser_legal_name, NULL::BIGINT AS adviser_regulatory_aum, "
                  f"NULL::VARCHAR AS adviser_lei FROM filing_crd_map WHERE (hash(filing_id) % {HISTORY_BUCKETS}) = {b}")
            pq.write_table(con.sql(PFT.build_funds_sql(src, snapshot, cf_sql=cf)).to_arrow_table(),
                           os.path.join(histpq, f"b{b:02d}.parquet"))
        con.close()
        _write("sec_adv_private_funds_history", pq.read_table(histpq), rows_per_file=HISTORY_ROWS_PER_FILE)
        counts["sec_adv_private_funds_history"] = _index("sec_adv_private_funds_history")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        shutil.rmtree(work, ignore_errors=True)
        completed_at = dt.datetime.now(dt.timezone.utc)
        for name, uri in PF_DATASETS.items():
            _record_run(name.replace("sec_adv_", ""), name, part2_url, uri, snapshot,
                        [str(part2_url), str(part1_url)], counts.get(name, 0), counts.get(name, 0),
                        0, status, error, started_at, completed_at)
        _post_callback(trigger_callback_url, {
            "status": status, "rows": sum(counts.values()), "feed": "sec_adv_private_funds",
            "dataset": "private_funds", "dataset_uri": PF_DATASETS["sec_adv_private_funds"],
            "as_of": snapshot, "source_url": part2_url,
        })

    if status != "success":
        raise RuntimeError(f"sec_adv private funds ingest failed: {error}")
    return {"status": status, "counts": counts, "as_of": snapshot, "source_url": part2_url}


# ──────────────────────────────────────────────────────────────────────────────
# Manual ops entrypoints (local — no callback). ops.* write still fires.
# ──────────────────────────────────────────────────────────────────────────────
@app.local_entrypoint()
def migrate() -> None:
    import json
    print(json.dumps(apply_migration.remote(), indent=2, default=str))


@app.local_entrypoint()
def ingest(dataset: str = "part1", url: str = "") -> None:
    import json
    print(json.dumps(ingest_dataset.remote(dataset, url=(url or None), trigger_callback_url=None),
                     indent=2, default=str))


@app.local_entrypoint()
def backfill() -> None:
    """Historical backfill — ingest part1 + advw in PARALLEL (distinct Lance datasets, no
    shared-writer manifest conflict). Prints final row counts."""
    import json

    calls = {d: ingest_dataset.spawn(d, trigger_callback_url=None) for d in DATASETS}
    results: dict[str, dict] = {}
    for d, call in calls.items():
        results[d] = call.get()
        print(json.dumps(results[d], default=str))
    print("\n=== FINAL ROW COUNTS ===")
    for d, r in results.items():
        print(f"  {d:8s} rows={r.get('rows', 0):>10,}  rejected={r.get('rejected_rows', 0):>6,}"
              f"  → {r.get('dataset_uri')}")


@app.local_entrypoint()
def reindex_one(dataset: str = "part1") -> None:
    import json
    print(json.dumps(reindex.remote(dataset), indent=2, default=str))


@app.local_entrypoint()
def private_funds() -> None:
    """Schedule D 7.B.1 / 7.B.2 / 7.A → sec_adv_private_funds(_history) + reliance + related_persons."""
    import json
    print(json.dumps(ingest_private_funds.remote(trigger_callback_url=None), indent=2, default=str))
