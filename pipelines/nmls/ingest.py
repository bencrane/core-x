"""Compute worker — NMLS (Nationwide Multistate Licensing System) public Business
Reports ingest. Part of the ``nmls-pipelines`` Modal app. Endpoint-less; spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints.

Two functions, two images, with the R2 landing zone as the durability boundary — the
fragile browser scrape is isolated from the deterministic data plane (a transform bug
costs a free re-run of ``ingest_target``, never another scrape):

  acquire_rosters  (Playwright image)  — Build-Step-0-pinned public surface.
    The NMLS Business Reports page (https://mortgage.nationwidelicensingsystem.org/about/
    SitePages/Reports.aspx, 301 → /knowledge/Products/nmls/aboutNMLS/SitePages/
    NMLSReports.aspx) is a client-rendered SharePoint page exposing static files at a
    stable pattern: …/aboutNMLS/BusinessReports/<Name>.<ext>  (no auth, no iframe, no
    REST dependency). Playwright enumerates the hydrated anchors; the chosen files are
    pulled with a plain anonymous GET (proven 200, Tier-1) and landed RAW to
    s3://data-sink/landing/nmls/<as_of>/ under STABLE names that decouple ingest from
    upstream filename drift:
        mcr_and_licensing_data.zip      ← "NMLS MCR and Licensing Data.zip"
        mortgage_industry_report.xlsx   ← latest "* Mortgage Industry Report.xlsx"

  ingest_target  (DuckDB → Arrow → Lance image)  — one logical dataset per call.
    Downloads its landed container from R2, parses 100% in DuckDB (read_csv for the MCR
    zip members; read_xlsx — sheet+range, Build-Step-0c-proven — for the workbook),
    exports zero-copy Arrow, writes Lance DIRECT to R2 (CA-CSLB pattern; datasets are
    ≤11k rows), and builds BITMAP/BTREE scalar indexes. Distinct datasets → the targets
    fan out in PARALLEL with no shared-writer manifest conflict.

PUBLIC-ONLY SURFACE (operator-authorized). Every dataset is a STATE × PERIOD AGGREGATE:
the public NMLS surface carries NO nmls_id / LEI / RSSD — there is no per-licensee roster
here, so there is no entity resolution key to BTREE (the original "nmls_id PK" thesis is
not realizable from public data; HMDA reconciliation is state×period analytic only).

Verified shape (R2-free verify_local run against the live public files, 2026-06-01):
    nmls_mcr_license_activity          11,240 rows   nmls_mcr_forward_by_business_line   8,130
    nmls_mcr_forward_by_purpose         8,181 rows   nmls_mcr_reverse_by_business_line   7,390
    nmls_mcr_forward_by_type           10,836 rows   nmls_mcr_applications_received      2,746
    nmls_state_entity_counts (xlsx)        59 rows (50 states + DC + territories + dual CA agencies)
All scalar indexes built; read_xlsx sheet+range+positional-alias parse confirmed.

Control plane (Trigger v4 durable callback): each function accepts ``trigger_callback_url``
and, on terminal state, (1) writes a run row to ops.nmls_runs via psycopg and (2) POSTs a
FLAT JSON body to that url. Manual/on-demand by design (NMLS refresh is irregular — quarterly
Mortgage Industry Reports; the MCR bundle updates on the same cadence).

    modal deploy pipelines/nmls/ingest.py
    modal run    pipelines/nmls/ingest.py::init_state                    # create ops.nmls_runs
    modal run    pipelines/nmls/ingest.py::acquire                       # harvest → R2 landing
    modal run    pipelines/nmls/ingest.py::ingest --target mcr_license_activity
    modal run    pipelines/nmls/ingest.py::run_all                       # acquire, then ingest every target
    modal run    pipelines/nmls/ingest.py::reindex --target mcr_license_activity
    modal run    pipelines/nmls/ingest.py::show_ledger
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
LANDING_PREFIX = "landing/nmls/"          # + <as_of>/<stable-file>
SCRATCH_DIR = "/tmp/nmls"
AS_OF_DEFAULT = "2026-06-01"

# Build-Step-0-pinned public surface.
REPORTS_PAGE = "https://mortgage.nationwidelicensingsystem.org/about/SitePages/Reports.aspx"
BUSINESS_REPORTS_PREFIX = (
    "https://mortgage.nationwidelicensingsystem.org/knowledge/Products/nmls/"
    "aboutNMLS/BusinessReports/"
)
# Stable hardcoded fallbacks (used only if the client-side enumeration fails — resilience).
MCR_ZIP_URL = BUSINESS_REPORTS_PREFIX + "NMLS%20MCR%20and%20Licensing%20Data.zip"
INDUSTRY_XLSX_FALLBACK_URL = BUSINESS_REPORTS_PREFIX + "Q3%202025%20NMLS%20Mortgage%20Industry%20Report.xlsx"

# Stable landed names (the contract between acquire_rosters and ingest_target).
LANDED_ZIP = "mcr_and_licensing_data.zip"
LANDED_XLSX = "mortgage_industry_report.xlsx"

_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
       "Chrome/131.0.0.0 Safari/537.36")


def _uri(name: str) -> str:
    return os.environ.get(f"{name.upper()}_LANCE_URI", f"s3://{BUCKET}/active/{name}/")


# logical target -> source kind + container (landed file) + member/sheet + Lance URI.
# CSV targets read a member of the landed MCR zip; the xlsx target reads one workbook sheet.
SOURCES: dict[str, dict] = {
    "nmls_mcr_license_activity": {
        "kind": "csv", "container": LANDED_ZIP,
        "member": "CSV/Mortgage License Activity.csv",
        "uri": _uri("nmls_mcr_license_activity"),
    },
    "nmls_mcr_forward_by_purpose": {
        "kind": "csv", "container": LANDED_ZIP,
        "member": "CSV/Forward Loans Closed and Funded by Purpose.csv",
        "uri": _uri("nmls_mcr_forward_by_purpose"),
    },
    "nmls_mcr_forward_by_type": {
        "kind": "csv", "container": LANDED_ZIP,
        "member": "CSV/Forward Loans Closed and Funded by Type.csv",
        "uri": _uri("nmls_mcr_forward_by_type"),
    },
    "nmls_mcr_forward_by_business_line": {
        "kind": "csv", "container": LANDED_ZIP,
        "member": "CSV/Forward Loans by Business Line.csv",
        "uri": _uri("nmls_mcr_forward_by_business_line"),
    },
    "nmls_mcr_reverse_by_business_line": {
        "kind": "csv", "container": LANDED_ZIP,
        "member": "CSV/Reverse Loans by Business Line.csv",
        "uri": _uri("nmls_mcr_reverse_by_business_line"),
    },
    "nmls_mcr_applications_received": {
        "kind": "csv", "container": LANDED_ZIP,
        "member": "CSV/Loan Applications Directly Received.csv",
        "uri": _uri("nmls_mcr_applications_received"),
    },
    "nmls_state_entity_counts": {
        "kind": "xlsx", "container": LANDED_XLSX,
        "sheet": "Counts by State Agency", "range": "A4:H80",
        "uri": _uri("nmls_state_entity_counts"),
    },
}

# Scalar index plan. No nmls_id/LEI exists in the public surface → no BTREE resolution
# key; these are aggregates. BITMAP for low-cardinality categoricals (state, quarter,
# type/line, entity_type); BTREE on the year columns for range scans.
INDEX_PLAN: dict[str, dict[str, list[str]]] = {
    "nmls_mcr_license_activity": {
        "btree": ["status_start_year"],
        "bitmap": ["state_regulator", "entity_type", "status_start_quarter"],
    },
    "nmls_mcr_forward_by_purpose": {
        "btree": ["filing_year"],
        "bitmap": ["state", "filing_quarter", "loan_purpose"],
    },
    "nmls_mcr_forward_by_type": {
        "btree": ["filing_year"],
        "bitmap": ["state", "filing_quarter", "loan_type"],
    },
    "nmls_mcr_forward_by_business_line": {
        "btree": ["filing_year"],
        "bitmap": ["state", "filing_quarter", "business_line"],
    },
    "nmls_mcr_reverse_by_business_line": {
        "btree": ["filing_year"],
        "bitmap": ["state", "filing_quarter", "business_line"],
    },
    "nmls_mcr_applications_received": {
        "btree": ["filing_year"],
        "bitmap": ["state", "filing_quarter"],
    },
    "nmls_state_entity_counts": {
        "btree": [],
        "bitmap": ["state_agency", "report_period"],
    },
}

MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"   # net-new datasets pin the current Lance default (02 §2.3)

# Mirrored verbatim by pipelines/nmls/ops_nmls_runs.sql. Applied by init_state.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.nmls_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phase          text        NOT NULL,
    target         text,
    dataset_uri    text,
    as_of          date,
    source_file    text,
    landing_key    text,
    rows_processed bigint,
    rejected_rows  bigint,
    status         text        NOT NULL,
    error          text,
    note           text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS nmls_runs_phase_idx       ON ops.nmls_runs (phase);
CREATE INDEX IF NOT EXISTS nmls_runs_target_idx      ON ops.nmls_runs (target);
CREATE INDEX IF NOT EXISTS nmls_runs_status_idx      ON ops.nmls_runs (status);
CREATE INDEX IF NOT EXISTS nmls_runs_recorded_at_idx ON ops.nmls_runs (recorded_at DESC);
"""

# ── Images ────────────────────────────────────────────────────────────────────────────
# Acquisition: Chromium via Playwright. playwright is a PIP package; system libs come
# from `playwright install-deps chromium` (NOT apt_install("playwright")). No
# ephemeral_disk — its 512 GiB floor over-provisions a job whose downloads are ≤1 MiB;
# the default container disk backs /tmp.
playwright_image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("playwright>=1.49")
    .run_commands("playwright install-deps chromium", "playwright install chromium")
    .pip_install("boto3>=1.35", "requests>=2.32", "psycopg[binary]>=3.2")
)

# Data plane: the fleet DuckDB → Arrow → Lance toolchain (mirrors ca_cslb).
data_image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "boto3>=1.35",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("nmls-pipelines")


# ── Shared R2 / Postgres / callback helpers (verbatim fleet pattern) ────────────────────
def _r2_storage_options() -> dict[str, str]:
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


def _s3_client():
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


def _record_run(phase, target, dataset_uri, as_of, source_file, landing_key, rows,
                rejected, status, error, note, started_at, completed_at) -> None:
    """Terminal run row → ops.nmls_runs (psycopg). Best-effort: never let an audit-write
    failure crash an otherwise-good run."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.nmls_runs
                    (phase, target, dataset_uri, as_of, source_file, landing_key,
                     rows_processed, rejected_rows, status, error, note,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (phase, target, dataset_uri, as_of, source_file, landing_key,
                 rows, rejected, status, error, note, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url (flat body — no
    {"data": ...} envelope; the whole body becomes result.output)."""
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


# ── DuckDB transforms — 100% in SQL. Identifiers double-quoted (source headers carry
#    spaces / parens / '$' / '#'). S = trimmed VARCHAR; I = TRY_CAST INTEGER; N = TRY_CAST
#    BIGINT (loan $ amounts run to 12+ digits); F = TRY_CAST DOUBLE (growth ratios). N/F
#    also strip ',' thousands separators before the cast (CSV measures arrive as "1,074",
#    which BIGINT/DOUBLE casts to NULL). The /tmp path, source_file and as_of are bound
#    positionally via ? — never interpolated. ──
def S(col: str, alias: str) -> str:
    return f"nullif(trim({col}), '') AS {alias}"


def I(col: str, alias: str) -> str:
    return f"TRY_CAST(nullif(trim({col}), '') AS INTEGER) AS {alias}"


def N(col: str, alias: str) -> str:
    return f"TRY_CAST(replace(nullif(trim({col}), ''), ',', '') AS BIGINT) AS {alias}"


def F(col: str, alias: str) -> str:
    return f"TRY_CAST(replace(nullif(trim({col}), ''), ',', '') AS DOUBLE) AS {alias}"


_CSV_READ = "all_varchar=true, header=true, sample_size=-1, ignore_errors=true, store_rejects=true"
_PROVENANCE = ("    ? AS source_file,\n"
               "    CAST(? AS DATE) AS snapshot_date,\n"
               "    now() AS ingested_at\n")


def _sql_license_activity() -> str:
    cols = ",\n    ".join([
        S('"Regulator"', "state_regulator"),
        I('"LicenseHistory StatusStartYear"', "status_start_year"),
        S('"LicenseHistory StatusStartQuarter"', "status_start_quarter"),
        S('"Entity Type"', "entity_type"),
        N('"New Applications"', "new_applications"),
        N('"Approved"', "approved"),
        N('"Denied"', "denied"),
        N('"Withdrawn"', "withdrawn"),
        N('"Revoked"', "revoked"),
        N('"Surrendered"', "surrendered"),
        N('"Terminated"', "terminated"),
    ])
    return (f"WITH raw AS (SELECT * FROM read_csv(?, {_CSV_READ}))\n"
            f"SELECT\n    {cols},\n{_PROVENANCE}FROM raw\n"
            "WHERE nullif(trim(\"Regulator\"), '') IS NOT NULL")


def _sql_loans(dim_col: str, dim_alias: str) -> str:
    """Forward/Reverse loan tables share the (State, Filing Year, Filing Quarter, <dim>,
    Loan Amt ($), Loan Cnt (#)) shape; only the dimension column's meaning differs."""
    cols = ",\n    ".join([
        S('"State"', "state"),
        I('"Filing Year"', "filing_year"),
        S('"Filing Quarter"', "filing_quarter"),
        S(f'"{dim_col}"', dim_alias),
        N('"Loan Amt ($)"', "loan_amount"),
        N('"Loan Cnt (#)"', "loan_count"),
    ])
    return (f"WITH raw AS (SELECT * FROM read_csv(?, {_CSV_READ}))\n"
            f"SELECT\n    {cols},\n{_PROVENANCE}FROM raw\n"
            "WHERE nullif(trim(\"State\"), '') IS NOT NULL")


def _sql_applications() -> str:
    cols = ",\n    ".join([
        S('"State"', "state"),
        I('"Filing Year"', "filing_year"),
        S('"Filing Quarter"', "filing_quarter"),
        N('"Loan Amt ($)"', "application_amount"),
        N('"Loan Cnt (#)"', "application_count"),
    ])
    return (f"WITH raw AS (SELECT * FROM read_csv(?, {_CSV_READ}))\n"
            f"SELECT\n    {cols},\n{_PROVENANCE}FROM raw\n"
            "WHERE nullif(trim(\"State\"), '') IS NOT NULL")


# CSV target -> (sql, params-need-as_of). params are [csv_path, source_file, as_of].
_CSV_SQL: dict[str, str] = {
    "nmls_mcr_license_activity": _sql_license_activity(),
    "nmls_mcr_forward_by_purpose": _sql_loans("Loan Type", "loan_purpose"),
    "nmls_mcr_forward_by_type": _sql_loans("Loan Type", "loan_type"),
    "nmls_mcr_forward_by_business_line": _sql_loans("Business Line", "business_line"),
    "nmls_mcr_reverse_by_business_line": _sql_loans("Business Line", "business_line"),
    "nmls_mcr_applications_received": _sql_applications(),
}


def _sql_state_entity_counts(xlsx_path: str, sheet: str, rng: str) -> str:
    """xlsx 'Counts by State Agency'. read_xlsx sheet+range is Build-Step-0c-proven;
    positional column aliasing (AS t(...)) immunizes against footnote-digit header drift
    in the workbook ('Annual Percentage Change2' etc.). header=true consumes A4 as the
    header row so data starts at A5; the WHERE drops banner/footnote rows where the count
    column is non-numeric. Path/sheet/range are repo-controlled constants (single-quote
    escaped); report_period + source_file + as_of bind positionally."""
    def lit(s: str) -> str:
        return s.replace("'", "''")

    projection = ",\n    ".join([
        S("state_agency", "state_agency"),
        N("companies", "companies"),
        F("company_apc", "company_annual_pct_change"),
        N("company_located", "company_located_in_state"),
        N("branches", "branches"),
        N("individuals", "individuals"),
        F("individual_apc", "individual_annual_pct_change"),
        N("individual_located", "individual_located_in_state"),
    ])
    return (
        f"WITH raw AS (\n"
        f"  SELECT * FROM read_xlsx('{lit(xlsx_path)}', sheet='{lit(sheet)}', "
        f"range='{lit(rng)}', header=true, all_varchar=true)\n"
        f"    AS t(state_agency, companies, company_apc, company_located,\n"
        f"         branches, individuals, individual_apc, individual_located)\n"
        f")\n"
        f"SELECT\n    {projection},\n"
        "    ? AS report_period,\n"
        "    ? AS source_file,\n"
        "    CAST(? AS DATE) AS snapshot_date,\n"
        "    now() AS ingested_at\n"
        "FROM raw\n"
        "WHERE nullif(trim(state_agency), '') IS NOT NULL\n"
        "  AND TRY_CAST(nullif(trim(companies), '') AS BIGINT) IS NOT NULL"
    )


def _build_indexes(target: str, ds) -> list[str]:
    built: list[str] = []
    for col in INDEX_PLAN[target]["btree"]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in INDEX_PLAN[target]["bitmap"]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    return built


# ── Phase 1 — acquisition (Playwright enumerate → anonymous GET → R2 landing) ───────────
def _select_industry_xlsx(file_links: list[dict]) -> str | None:
    """Pick the most-recent quarterly '* Mortgage Industry Report.xlsx' (exclude MSB and
    annual). Handles both 'Q3 2025 …' and '… 2025Q3' name forms; lexical fallback."""
    import re
    import urllib.parse

    cands = []
    for a in file_links:
        href = a["href"]
        name = urllib.parse.unquote(href.rsplit("/", 1)[-1])
        low = name.lower()
        if not low.endswith(".xlsx") or "mortgage industry report" not in low or "msb" in low:
            continue
        m = re.search(r"(20\d\d)\s*Q([1-4])", name) or None
        if m:
            key = int(m.group(1)) * 10 + int(m.group(2))
        else:
            m2 = re.search(r"Q([1-4])\s+(20\d\d)", name)
            key = (int(m2.group(2)) * 10 + int(m2.group(1))) if m2 else 0
        cands.append((key, name, href))
    if not cands:
        return None
    cands.sort(key=lambda t: (t[0], t[1]))
    return cands[-1][2]


def _harvest_file_links() -> list[dict]:
    """Playwright: load the (client-rendered) Business Reports page, return anchors whose
    href is under the BusinessReports static-file prefix. Empty list on any failure (the
    caller falls back to hardcoded stable URLs)."""
    from playwright.sync_api import sync_playwright

    links: list[dict] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(args=["--no-sandbox"])
            ctx = browser.new_context(user_agent=_UA, ignore_https_errors=True)
            page = ctx.new_page()
            page.goto(REPORTS_PAGE, wait_until="domcontentloaded", timeout=60_000)
            try:
                page.wait_for_load_state("networkidle", timeout=20_000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(3500)
            anchors = page.eval_on_selector_all(
                "a[href]", "els => els.map(e => ({text:(e.textContent||'').trim().slice(0,140), href:e.href}))"
            )
            links = [a for a in anchors if a.get("href", "").startswith(BUSINESS_REPORTS_PREFIX)]
            browser.close()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: Playwright enumeration failed ({exc}); using hardcoded fallbacks.")
    return links


@app.function(
    image=playwright_image,
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 20,
    memory=8192,
    cpu=4.0,
)
def acquire_rosters(as_of: str = AS_OF_DEFAULT,
                    trigger_callback_url: str | None = None) -> dict:
    """Enumerate the public Business Reports page, pull the MCR zip + the latest Mortgage
    Industry Report xlsx with an anonymous GET, and land them RAW to R2 under stable
    names. Records ops.nmls_runs (phase='acquire') + wakes Trigger. Re-raises on failure."""
    import datetime as dt
    import urllib.parse

    import requests

    started_at = dt.datetime.now(dt.timezone.utc)
    landed: list[str] = []
    status, error = "error", None

    try:
        os.makedirs(SCRATCH_DIR, exist_ok=True)
        s3 = _s3_client()
        landing_dir = f"{LANDING_PREFIX}{as_of}/"

        links = _harvest_file_links()
        mcr_url = next(
            (a["href"] for a in links
             if urllib.parse.unquote(a["href"].rsplit("/", 1)[-1]) == "NMLS MCR and Licensing Data.zip"),
            MCR_ZIP_URL,
        )
        xlsx_url = _select_industry_xlsx(links) or INDUSTRY_XLSX_FALLBACK_URL
        print(f"acquire as_of={as_of}: {len(links)} BusinessReports links; "
              f"mcr={mcr_url.rsplit('/', 1)[-1]} xlsx={urllib.parse.unquote(xlsx_url.rsplit('/', 1)[-1])}")

        for url, landed_name in ((mcr_url, LANDED_ZIP), (xlsx_url, LANDED_XLSX)):
            local = os.path.join(SCRATCH_DIR, landed_name)
            with requests.get(url, headers={"User-Agent": _UA}, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(local, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        if chunk:
                            fh.write(chunk)
            key = landing_dir + landed_name
            s3.upload_file(local, BUCKET, key)
            size = os.path.getsize(local)
            print(f"  landed {landed_name} ({size:,} bytes) -> s3://{BUCKET}/{key}")
            landed.append(f"{landed_name}<-{urllib.parse.unquote(url.rsplit('/', 1)[-1])} ({size:,}B)")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("acquire", "nmls_business_reports", None, as_of, None,
                    f"{LANDING_PREFIX}{as_of}/", len(landed), None, status, error,
                    "; ".join(landed) or None, started_at, completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "phase": "acquire", "as_of": as_of,
                        "files_landed": len(landed), "landed": landed,
                        "targets": list(SOURCES.keys())})

    if status != "success":
        raise RuntimeError(f"nmls acquire failed: {error}")
    return {"status": status, "phase": "acquire", "as_of": as_of,
            "files_landed": len(landed), "landed": landed, "targets": list(SOURCES.keys())}


# ── Phase 2 — ingest one logical target (R2 landed container → DuckDB → Lance) ──────────
@app.function(
    image=data_image,
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 20,
    memory=8192,
    cpu=4.0,
)
def ingest_target(target: str, as_of: str = AS_OF_DEFAULT,
                  trigger_callback_url: str | None = None) -> dict:
    """Download the landed container (zip member or xlsx) from R2 → DuckDB transform
    (100% in SQL) → Arrow → Lance overwrite DIRECT to R2 → BITMAP/BTREE indexes; record
    ops.* + wake Trigger. Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt
    import zipfile

    import duckdb
    import lance

    target = target.strip()
    if target not in SOURCES:
        raise ValueError(f"target must be one of {sorted(SOURCES)}, got {target!r}")

    meta = SOURCES[target]
    dataset_uri = meta["uri"]
    container = meta["container"]
    landing_key = f"{LANDING_PREFIX}{as_of}/{container}"
    started_at = dt.datetime.now(dt.timezone.utc)
    rows, rejected = 0, 0
    status, error, source_file = "error", None, None
    built: list[str] = []

    try:
        so = _r2_storage_options()
        s3 = _s3_client()
        os.makedirs(SCRATCH_DIR, exist_ok=True)
        local_container = os.path.join(SCRATCH_DIR, container)
        print(f"Downloading s3://{BUCKET}/{landing_key} -> {local_container}")
        s3.download_file(BUCKET, landing_key, local_container)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            con.execute("SET enable_progress_bar=false;")

            if meta["kind"] == "csv":
                member = meta["member"]
                source_file = member.rsplit("/", 1)[-1]
                extract_dir = os.path.join(SCRATCH_DIR, f"{target}_unzip")
                with zipfile.ZipFile(local_container) as zf:
                    zf.extract(member, extract_dir)
                csv_path = os.path.join(extract_dir, member)
                table = con.execute(_CSV_SQL[target], [csv_path, source_file, as_of]).to_arrow_table()
                try:
                    rj = con.execute("SELECT count(*) FROM reject_errors").fetchone()
                    rejected = int(rj[0]) if rj else 0
                except Exception:  # noqa: BLE001
                    rejected = 0
            else:  # xlsx
                source_file = container
                con.execute("INSTALL excel; LOAD excel;")
                report_period = f"as_of:{as_of}"
                sql = _sql_state_entity_counts(local_container, meta["sheet"], meta["range"])
                table = con.execute(sql, [report_period, source_file, as_of]).to_arrow_table()

            rows = table.num_rows
        finally:
            con.close()
        print(f"{target}: parsed {rows:,} rows, {rejected:,} rejected")

        lance.write_dataset(
            table, dataset_uri, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        del table
        built = _build_indexes(target, lance.dataset(dataset_uri, storage_options=so))
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("ingest", target, dataset_uri, as_of, source_file, landing_key,
                    int(rows), int(rejected), status, error, None, started_at, completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "phase": "ingest", "target": target,
                        "rows": int(rows), "rejected_rows": int(rejected),
                        "dataset_uri": dataset_uri, "as_of": as_of})

    if status != "success":
        raise RuntimeError(f"nmls ingest failed for target={target}: {error}")
    return {"status": status, "phase": "ingest", "target": target,
            "rows_processed": int(rows), "rejected_rows": int(rejected),
            "indices": built, "dataset_uri": dataset_uri, "as_of": as_of}


@app.function(image=data_image, secrets=[modal.Secret.from_name("r2-credentials")],
              timeout=60 * 15, memory=8192, cpu=4.0)
def reindex_target(target: str) -> dict:
    """(Re)build the scalar indexes on an already-written dataset (no re-ingest)."""
    import lance

    target = target.strip()
    if target not in SOURCES:
        raise ValueError(f"target must be one of {sorted(SOURCES)}, got {target!r}")
    so = _r2_storage_options()
    built = _build_indexes(target, lance.dataset(SOURCES[target]["uri"], storage_options=so))
    return {"target": target, "dataset_uri": SOURCES[target]["uri"],
            "indexes": built, "index_count": len(built)}


@app.function(image=data_image, timeout=60 * 15, memory=8192, cpu=4.0)
def verify_local() -> dict:
    """R2-FREE self-verification: download the public files directly, run EVERY target's
    production transform (the same _CSV_SQL / _sql_state_entity_counts builders), write
    Lance to LOCAL /tmp, build the scalar indexes, and return row counts + schema +
    committed indices. Touches NO R2, NO Postgres, NO Trigger — proves the data plane."""
    import shutil
    import zipfile

    import duckdb
    import lance
    import requests

    work = os.path.join(SCRATCH_DIR, "verify")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work, exist_ok=True)
    zip_path = os.path.join(work, LANDED_ZIP)
    xlsx_path = os.path.join(work, LANDED_XLSX)
    for url, dst in ((MCR_ZIP_URL, zip_path), (INDUSTRY_XLSX_FALLBACK_URL, xlsx_path)):
        with requests.get(url, headers={"User-Agent": _UA}, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(dst, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        fh.write(chunk)
    extract_dir = os.path.join(work, "unzip")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)

    out: dict = {}
    for target, meta in SOURCES.items():
        ds_path = os.path.join(work, "lance", target)
        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            con.execute("SET enable_progress_bar=false;")
            if meta["kind"] == "csv":
                csv_path = os.path.join(extract_dir, meta["member"])
                table = con.execute(
                    _CSV_SQL[target], [csv_path, meta["member"].rsplit("/", 1)[-1], AS_OF_DEFAULT]
                ).to_arrow_table()
            else:
                con.execute("INSTALL excel; LOAD excel;")
                sql = _sql_state_entity_counts(xlsx_path, meta["sheet"], meta["range"])
                table = con.execute(
                    sql, [f"as_of:{AS_OF_DEFAULT}", LANDED_XLSX, AS_OF_DEFAULT]
                ).to_arrow_table()
        finally:
            con.close()
        lance.write_dataset(table, ds_path, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION)
        built = _build_indexes(target, lance.dataset(ds_path))
        out[target] = {"rows": table.num_rows,
                       "columns": [f.name for f in table.schema],
                       "indices": built}
        print(f"  {target:38s} rows={table.num_rows:>8,}  cols={len(table.schema):>2}  idx={len(built)}")
        del table
    return out


@app.function(image=data_image, secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_state_schema() -> dict:
    """Apply the idempotent ops.nmls_runs DDL. Run once before the first run."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.nmls_runs schema.")
    return {"status": "success", "table": "ops.nmls_runs"}


@app.function(image=data_image, secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 12) -> list:
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, phase, target, dataset_uri, as_of, source_file, rows_processed, "
            "rejected_rows, status, error, started_at, completed_at "
            "FROM ops.nmls_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── Manual ops entrypoints ──────────────────────────────────────────────────────────────
@app.local_entrypoint()
def init_state() -> None:
    import json
    print(json.dumps(apply_state_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def acquire(as_of: str = AS_OF_DEFAULT) -> None:
    import json
    print(json.dumps(acquire_rosters.remote(as_of=as_of, trigger_callback_url=None),
                     indent=2, default=str))


@app.local_entrypoint()
def ingest(target: str, as_of: str = AS_OF_DEFAULT) -> None:
    import json
    print(json.dumps(ingest_target.remote(target, as_of=as_of, trigger_callback_url=None),
                     indent=2, default=str))


@app.local_entrypoint()
def run_all(as_of: str = AS_OF_DEFAULT) -> None:
    """End-to-end manual run: acquire, then ingest every target in PARALLEL (distinct
    Lance datasets → no shared-writer conflict)."""
    import json

    print("=== acquire ===")
    acq = acquire_rosters.remote(as_of=as_of, trigger_callback_url=None)
    print(json.dumps(acq, default=str))
    if acq.get("status") != "success":
        raise SystemExit("acquire failed; not ingesting.")

    print("\n=== parallel ingest ===")
    calls = {t: ingest_target.spawn(t, as_of=as_of, trigger_callback_url=None) for t in SOURCES}
    total = 0
    for t, call in calls.items():
        r = call.get()
        n = r.get("rows_processed", 0)
        total += n
        print(f"  {t:38s} rows={n:>8,} rejected={r.get('rejected_rows'):>4,} -> {r.get('dataset_uri')}")
    print(f"  {'TOTAL':38s} rows={total:>8,}")


@app.local_entrypoint()
def reindex(target: str = "") -> None:
    import json
    for t in ([target] if target else list(SOURCES)):
        print(json.dumps(reindex_target.remote(t), default=str))


@app.local_entrypoint()
def verify() -> None:
    """R2-free data-plane verification (no R2 / Postgres / Trigger touched)."""
    import json
    print(json.dumps(verify_local.remote(), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 12) -> None:
    import json
    print(json.dumps(ledger.remote(limit), indent=2, default=str))
