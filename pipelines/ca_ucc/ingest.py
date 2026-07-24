"""Compute worker — California Secretary of State UCC bulk filing ingest.

Part of the ``ca-ucc-filings`` Modal app. One endpoint-less worker spawned by the
Universal Dispatcher (core/modal_dispatcher.py) — no web endpoint. Clean-room data
plane: no Iceberg, no Polaris. Python does I/O ONLY (download + unzip); DuckDB does
100% of the transform; Arrow is the only interchange; Lance is the system of record
written straight to R2.

Source shape (differs from the SBA feeds — there is NO upstream fetch):
    The operator has already landed a single CA SOS "Data Request" bulk export to
    s3://data-sink/landing/ca_ucc/<...>.zip (~465 MB). The worker's source IS that
    landed ZIP. It contains four pipe-delimited, double-quoted, CRLF, UTF-8 CSVs
    (verified by structural probe): Filings, Debtors, SecuredParties, FilingAmendments.

Data plane (one Modal invocation; five Lance datasets):
    R2 landing ZIP
      → boto3 download → /tmp/ca_ucc.zip                    (Python: I/O only)
      → zipfile extract members → /tmp/ca_ucc/*.csv          (Python: I/O only)
      → DuckDB read_csv(delim='|', quote='"', all_varchar)  → project / cast  (100% in SQL)
      → Arrow table  → con.sql(...).to_arrow_table()
      → lance.write_dataset(s3://data-sink/active/ca_ucc/<dataset>/, v2.1, overwrite)
      → BTREE / BITMAP scalar indexes on the resolution / filter keys

Topology (approved):
    Tier 1 — four normalized, source-faithful datasets (system of record):
        ca_ucc_filings              (master: one row per filing EVENT; PK ≈ (UCC1_NUM, UCC3_NUM))
        ca_ucc_debtors              (N debtors per filing)
        ca_ucc_secured_parties      (N secured parties per filing)
        ca_ucc_filing_amendments    (UCC3 → UCC1 amendment bridge)
    Tier 2 — one derived firmographic resolution view:
        ca_ucc_debtor_index         (grain = one row per debtor appearance; filing attrs
                                     joined 1:1; secured parties aggregated as a nested
                                     LIST<STRUCT> — NOT bare VARIANT)

Identifier hazard (neutralized): UCC1_NUM / UCC3_NUM carry significant LEADING ZEROS
("077101475345") and POSTAL_CODE is ZIP+4 ("60048-5339"). All three are STRICT VARCHAR —
a numeric cast would destroy the resolution key. ALT_DESIGNATION_TYPE_ID / FILING_TYPE_ID
are named "_ID" but carry TEXT LABELS ("Lessee/Lessor", "UCC") — kept VARCHAR, never cast.

Parse strictness (approved): ignore_errors=false — fail loud on any ragged row. A
store_rejects quarantine can be added later if a real release needs it.

Control plane (Trigger v4 durable callback): the worker accepts ``trigger_callback_url``
and, on terminal state (success OR failure), (1) writes the run row to ``ops.ca_ucc_runs``
via psycopg and (2) POSTs a FLAT JSON body to that url to wake the suspended Trigger run.
No ``{"data": ...}`` envelope.

    modal run    pipelines/ca_ucc/ingest.py::setup           # create ops.ca_ucc_runs
    modal run    pipelines/ca_ucc/ingest.py::run             # manual ingest — deploys if stale, then
                                                             # SPAWNS ingest_ca_ucc on the DEPLOYED app
                                                             # and exits; prints FUNCTION_CALL_ID (fc-…).
                                                             # No result printed — the worker's
                                                             # ops.ca_ucc_runs row + app logs are the
                                                             # record. Follow:
                                                             #   modal app logs ca-ucc-filings
                                                             #   modal.FunctionCall.from_id('fc-…').get(timeout=0)
    modal run    pipelines/ca_ucc/ingest.py::reindex         # rebuild scalar indexes only
    modal deploy pipelines/ca_ucc/ingest.py                  # publish for dispatcher
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
LANDING_PREFIX = "landing/ca_ucc/"

# Lance system-of-record tier — five datasets under the ca_ucc/ namespace, env-overridable.
_ACTIVE = "s3://data-sink/active/ca_ucc"
DATASET_URI = {
    "filings": os.environ.get("CA_UCC_FILINGS_URI", f"{_ACTIVE}/filings/"),
    "debtors": os.environ.get("CA_UCC_DEBTORS_URI", f"{_ACTIVE}/debtors/"),
    "secured_parties": os.environ.get("CA_UCC_SECURED_PARTIES_URI", f"{_ACTIVE}/secured_parties/"),
    "filing_amendments": os.environ.get("CA_UCC_FILING_AMENDMENTS_URI", f"{_ACTIVE}/filing_amendments/"),
    "debtor_index": os.environ.get("CA_UCC_DEBTOR_INDEX_URI", f"{_ACTIVE}/debtor_index/"),
}

# Member filenames inside the ZIP (verified by the structural probe).
MEMBER = {
    "filings": "Filings.csv",
    "debtors": "Debtors.csv",
    "secured_parties": "SecuredParties.csv",
    "filing_amendments": "FilingAmendments.csv",
}

SCRATCH_DIR = "/tmp"
WORKDIR = "/tmp/ca_ucc"
LOCAL_ZIP = "/tmp/ca_ucc.zip"
FEED = "ca_ucc"

# as_of: the CA SOS Data Request fulfillment date (approved: pass the zip-request date).
AS_OF_DEFAULT = "2026-05-31"

# Lance fragment sizing (directive constraints — exact).
#   max_rows_per_file = 1048576.
#   max_bytes_per_file: the directive wrote `90 * 10243`, i.e. `90 * 1024**3` (90 GiB) —
#   Lance's documented default and the established SBA-worker constant. A literal
#   `90 * 10243` (~900 KB) would shatter each multi-million-row dataset into hundreds of
#   fragments — not the intent. Honor the stated 90 GiB.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new datasets → pin the current Lance default (per 02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan. BTREE for high-cardinality load-bearing resolution keys (equality +
# range); BITMAP for low-cardinality categoricals filtered frequently.
INDEXES: dict[str, dict[str, list[str]]] = {
    "filings": {
        "BTREE": ["ucc1_num", "ucc3_num"],
        "BITMAP": ["action_type", "filing_type", "alt_designation_type"],
    },
    "debtors": {
        "BTREE": ["ucc1_num", "ucc3_num", "org_name", "last_name", "postal_code", "city"],
        "BITMAP": ["debtor_type", "state", "country"],
    },
    "secured_parties": {
        "BTREE": ["ucc1_num", "ucc3_num", "org_name", "last_name", "postal_code", "city"],
        "BITMAP": ["secured_party_type", "state", "country"],
    },
    "filing_amendments": {
        "BTREE": ["ucc1_num", "ucc3_num"],
        "BITMAP": ["action_type"],
    },
    "debtor_index": {
        "BTREE": ["ucc1_num", "ucc3_num", "debtor_org_name", "debtor_last_name", "debtor_postal_code"],
        "BITMAP": ["debtor_type", "debtor_state", "action_type"],
    },
}

DDL_CA_UCC_RUNS = """
CREATE TABLE IF NOT EXISTS ops.ca_ucc_runs (
    id            BIGSERIAL PRIMARY KEY,
    feed          TEXT        NOT NULL DEFAULT 'ca_ucc',
    source_zip    TEXT        NOT NULL,
    as_of         DATE,
    data_through  TIMESTAMP,                 -- max(processed_date) across Filings
    datasets      JSONB       NOT NULL,      -- {"filings": n, "debtors": n, ...}
    rows_total    BIGINT      NOT NULL DEFAULT 0,
    status        TEXT        NOT NULL,       -- success | error
    error         TEXT,
    started_at    TIMESTAMPTZ NOT NULL,
    completed_at  TIMESTAMPTZ NOT NULL
);
"""

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
        "lancedb>=0.15",
        "pylance>=7",            # provides `import lance`; lancedb does not re-export it
        "pyarrow>=17",
        "boto3>=1.35",           # R2 landing read
        "requests>=2.32",        # Trigger callback
        "psycopg[binary]>=3.2",  # terminal state → ops.*
    )
    .env(
        # BTREE scalar-index builds sort the column. Lance's spill-to-disk sorter uses a
        # small bounded DataFusion memory pool that OOMs on high-cardinality string columns
        # (org_name across ~5.9M debtor rows). Force the in-memory sort path so every index
        # builds. See lance-format/lance#2650 / datafusion#10073.
        {"LANCE_BYPASS_SPILLING": "true"}
    )
)

app = modal.App("ca-ucc-filings", image=image)


# ── R2 / object-store plumbing (mirrors the SBA workers verbatim) ─────────────
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret.
    AWS-style creds + explicit endpoint and region ("auto" for R2)."""
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
    """boto3 S3 client for R2. Forces checksum behaviour to ``when_required``:
    botocore's default flexible-checksum validation does not match R2's semantics
    and otherwise raises on download."""
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


def _discover_zip(s3) -> str:
    """Resolve the single .zip object under the landing prefix. Explicit zip_key
    param wins; otherwise require exactly one .zip."""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=LANDING_PREFIX)
    zips = [o["Key"] for o in resp.get("Contents", []) if o["Key"].lower().endswith(".zip")]
    if len(zips) != 1:
        raise RuntimeError(f"Expected exactly one .zip under {LANDING_PREFIX}, found {zips}")
    return zips[0]


def _extract_members(local_zip: str) -> dict[str, str]:
    """Extract the four known members to WORKDIR. Python I/O only (no parse).
    Returns {dataset_name: local_csv_path}. Fails loud if a member is missing."""
    import zipfile

    os.makedirs(WORKDIR, exist_ok=True)
    paths: dict[str, str] = {}
    with zipfile.ZipFile(local_zip) as zf:
        names = set(zf.namelist())
        for ds, member in MEMBER.items():
            if member not in names:
                raise RuntimeError(f"ZIP missing expected member {member!r}; has {sorted(names)}")
            out = os.path.join(WORKDIR, member)
            with zf.open(member) as src, open(out, "wb") as dst:
                # streamed copy — bounded memory regardless of member size
                while True:
                    chunk = src.read(1 << 22)  # 4 MiB
                    if not chunk:
                        break
                    dst.write(chunk)
            paths[ds] = out
            print(f"extracted {member} -> {out} ({os.path.getsize(out):,} bytes)")
    return paths


# ── DuckDB read_csv source clause (shared) ────────────────────────────────────
def _lit(s: str) -> str:
    return s.replace("'", "''")


def _read_csv(path: str) -> str:
    """Canonical read_csv clause for every CA UCC member. delim='|', quote='"',
    all_varchar (defensive), strict — ignore_errors=false fails loud on ragged rows.
    encoding='utf-8' (verified: clean UTF-8, no transcode needed)."""
    return (
        f"read_csv('{_lit(path)}', "
        "delim = '|', quote = '\"', header = true, all_varchar = true, "
        "sample_size = -1, ignore_errors = false, encoding = 'utf-8')"
    )


# Provenance trailer appended to every Tier-1 projection.
def _prov(source_file: str, as_of: str) -> str:
    return (
        f"    '{_lit(source_file)}' AS source_file,\n"
        f"    CAST('{_lit(as_of)}' AS DATE) AS as_of,\n"
        "    now() AS ingested_at"
    )


# ── Per-dataset DuckDB projections (100% of the transform in SQL) ──────────────
def _sql_filings(path: str, as_of: str) -> str:
    return f"""
SELECT
    nullif(trim("UCC1_NUM"), '')                       AS ucc1_num,
    nullif(trim("UCC3_NUM"), '')                       AS ucc3_num,
    TRY_CAST(nullif(trim("FILING_DATE"), '')    AS TIMESTAMP) AS filing_date,
    TRY_CAST(nullif(trim("PROCESSED_DATE"), '') AS TIMESTAMP) AS processed_date,
    nullif(trim("ACTION_TYPE"), '')                    AS action_type,
    nullif(trim("ALT_DESIGNATION_TYPE_ID"), '')        AS alt_designation_type,
    nullif(trim("FILING_TYPE_ID"), '')                 AS filing_type,
    TRY_CAST(nullif(trim("LAPSE_DATE"), '')     AS TIMESTAMP) AS lapse_date,
    TRY_CAST(nullif(trim("PAGE_COUNT"), '')     AS INTEGER)   AS page_count,
{_prov(MEMBER['filings'], as_of)}
FROM {_read_csv(path)}
"""


def _sql_party(path: str, as_of: str, type_src: str, type_dst: str, member: str) -> str:
    """Shared 15-column projection for Debtors / SecuredParties (only the type
    column name differs)."""
    return f"""
SELECT
    nullif(trim("UCC1_NUM"), '')      AS ucc1_num,
    nullif(trim("UCC3_NUM"), '')      AS ucc3_num,
    nullif(trim("{type_src}"), '')    AS {type_dst},
    nullif(trim("ORG_NAME"), '')      AS org_name,
    nullif(trim("LAST_NAME"), '')     AS last_name,
    nullif(trim("FIRST_NAME"), '')    AS first_name,
    nullif(trim("MIDDLE_NAME"), '')   AS middle_name,
    nullif(trim("SUFFIX"), '')        AS suffix,
    nullif(trim("ADDR1"), '')         AS addr1,
    nullif(trim("ADDR2"), '')         AS addr2,
    nullif(trim("ADDR3"), '')         AS addr3,
    nullif(trim("CITY"), '')          AS city,
    nullif(trim("STATE"), '')         AS state,
    nullif(trim("POSTAL_CODE"), '')   AS postal_code,
    nullif(trim("COUNTRY"), '')       AS country,
{_prov(member, as_of)}
FROM {_read_csv(path)}
"""


def _sql_amendments(path: str, as_of: str) -> str:
    return f"""
SELECT
    nullif(trim("UCC1_NUM"), '')   AS ucc1_num,
    nullif(trim("UCC3_NUM"), '')   AS ucc3_num,
    nullif(trim("ACTION_TYPE"), '') AS action_type,
{_prov(MEMBER['filing_amendments'], as_of)}
FROM {_read_csv(path)}
"""


def _sql_debtor_index(filings_path: str, debtors_path: str, secured_path: str, as_of: str) -> str:
    """Tier 2 — debtor-centric firmographic view. Grain = one row per debtor
    appearance. Filing attributes joined 1:1 (filings deduped to one row per
    (ucc1,ucc3) via QUALIFY so the debtor row count is preserved exactly, never
    fanned out). Secured parties for the filing aggregated into a nested
    LIST<STRUCT> — NOT bare VARIANT — so to_arrow_table() emits nested binary
    Arrow that Lance stores as real nested columns."""
    return f"""
WITH d AS (
    SELECT
        nullif(trim("UCC1_NUM"), '')    AS ucc1_num,
        nullif(trim("UCC3_NUM"), '')    AS ucc3_num,
        nullif(trim("DEBTOR_TYPE"), '') AS debtor_type,
        nullif(trim("ORG_NAME"), '')    AS debtor_org_name,
        nullif(trim("LAST_NAME"), '')   AS debtor_last_name,
        nullif(trim("FIRST_NAME"), '')  AS debtor_first_name,
        nullif(trim("MIDDLE_NAME"), '') AS debtor_middle_name,
        nullif(trim("SUFFIX"), '')      AS debtor_suffix,
        nullif(trim("ADDR1"), '')       AS debtor_addr1,
        nullif(trim("ADDR2"), '')       AS debtor_addr2,
        nullif(trim("ADDR3"), '')       AS debtor_addr3,
        nullif(trim("CITY"), '')        AS debtor_city,
        nullif(trim("STATE"), '')       AS debtor_state,
        nullif(trim("POSTAL_CODE"), '') AS debtor_postal_code,
        nullif(trim("COUNTRY"), '')     AS debtor_country
    FROM {_read_csv(debtors_path)}
),
f AS (
    SELECT ucc1_num, ucc3_num, filing_date, processed_date, action_type,
           alt_designation_type, filing_type, lapse_date, page_count
    FROM (
        SELECT
            nullif(trim("UCC1_NUM"), '')                       AS ucc1_num,
            nullif(trim("UCC3_NUM"), '')                       AS ucc3_num,
            TRY_CAST(nullif(trim("FILING_DATE"), '')    AS TIMESTAMP) AS filing_date,
            TRY_CAST(nullif(trim("PROCESSED_DATE"), '') AS TIMESTAMP) AS processed_date,
            nullif(trim("ACTION_TYPE"), '')                    AS action_type,
            nullif(trim("ALT_DESIGNATION_TYPE_ID"), '')        AS alt_designation_type,
            nullif(trim("FILING_TYPE_ID"), '')                 AS filing_type,
            TRY_CAST(nullif(trim("LAPSE_DATE"), '')     AS TIMESTAMP) AS lapse_date,
            TRY_CAST(nullif(trim("PAGE_COUNT"), '')     AS INTEGER)   AS page_count
        FROM {_read_csv(filings_path)}
    )
    QUALIFY row_number() OVER (PARTITION BY ucc1_num, ucc3_num ORDER BY processed_date DESC) = 1
),
sp AS (
    SELECT
        nullif(trim("UCC1_NUM"), '')          AS ucc1_num,
        nullif(trim("UCC3_NUM"), '')          AS ucc3_num,
        nullif(trim("SECURED_PARTY_TYPE"), '') AS secured_party_type,
        nullif(trim("ORG_NAME"), '')          AS org_name,
        nullif(trim("LAST_NAME"), '')         AS last_name,
        nullif(trim("FIRST_NAME"), '')        AS first_name,
        nullif(trim("CITY"), '')              AS city,
        nullif(trim("STATE"), '')             AS state,
        nullif(trim("POSTAL_CODE"), '')       AS postal_code
    FROM {_read_csv(secured_path)}
),
sp_agg AS (
    SELECT
        ucc1_num, ucc3_num,
        list(struct_pack(
            secured_party_type := secured_party_type,
            org_name := org_name, last_name := last_name, first_name := first_name,
            city := city, state := state, postal_code := postal_code
        )) AS secured_parties,
        count(*) AS secured_party_count
    FROM sp
    GROUP BY ucc1_num, ucc3_num
)
SELECT
    d.ucc1_num, d.ucc3_num,
    d.debtor_type, d.debtor_org_name, d.debtor_last_name, d.debtor_first_name,
    d.debtor_middle_name, d.debtor_suffix, d.debtor_addr1, d.debtor_addr2, d.debtor_addr3,
    d.debtor_city, d.debtor_state, d.debtor_postal_code, d.debtor_country,
    f.filing_date, f.processed_date, f.action_type, f.alt_designation_type,
    f.filing_type, f.lapse_date, f.page_count,
    coalesce(sp_agg.secured_parties,
             []::STRUCT(secured_party_type VARCHAR, org_name VARCHAR, last_name VARCHAR,
                        first_name VARCHAR, city VARCHAR, state VARCHAR, postal_code VARCHAR)[]
    ) AS secured_parties,
    coalesce(sp_agg.secured_party_count, 0) AS secured_party_count,
    CAST('{_lit(as_of)}' AS DATE) AS as_of,
    now() AS ingested_at
FROM d
LEFT JOIN f      ON d.ucc1_num = f.ucc1_num AND d.ucc3_num = f.ucc3_num
LEFT JOIN sp_agg ON d.ucc1_num = sp_agg.ucc1_num AND d.ucc3_num = sp_agg.ucc3_num
"""


# ── Lance write + index helpers ───────────────────────────────────────────────
def _write_lance(table, uri: str, so: dict) -> None:
    import lance

    lance.write_dataset(
        table,
        uri,
        mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )


def _create_indexes(ds_name: str, so: dict) -> list[dict]:
    """Build BTREE + BITMAP scalar indexes for one dataset. Best-effort per index
    (an index miss must not fail an otherwise-good load) but logged loudly."""
    import lance

    ds = lance.dataset(DATASET_URI[ds_name], storage_options=so)
    out: list[dict] = []
    for itype in ("BTREE", "BITMAP"):
        for col in INDEXES[ds_name].get(itype, []):
            try:
                ds.create_scalar_index(col, index_type=itype)
                print(f"  {itype:6s} ✓ {ds_name}.{col}")
                out.append({"col": col, "type": itype, "ok": True})
            except Exception as exc:  # noqa: BLE001
                print(f"  {itype:6s} ✗ {ds_name}.{col}: {exc}")
                out.append({"col": col, "type": itype, "ok": False, "error": str(exc)})
    return out


# ── Terminal state + callback ─────────────────────────────────────────────────
def _record_run(source_zip, as_of, data_through, datasets, rows_total,
                status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.ca_ucc_runs (psycopg). Best-effort: never let an
    audit-write failure crash an otherwise-good ingest."""
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
            cur.execute(DDL_CA_UCC_RUNS)
            cur.execute(
                """
                INSERT INTO ops.ca_ucc_runs
                    (feed, source_zip, as_of, data_through, datasets, rows_total,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, source_zip, as_of, data_through, Jsonb(datasets), rows_total,
                 status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint URL. FLAT JSON body — no
    ``{"data": ...}`` envelope. A few retries for delivery reliability."""
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


def _duck():
    """In-memory DuckDB connection tuned for the worker container (spill to NVMe
    scratch so the big Tier-2 join never OOMs)."""
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")
    con.execute(f"PRAGMA temp_directory='{SCRATCH_DIR}/duck_spill';")
    con.execute("PRAGMA max_temp_directory_size='200GiB';")
    con.execute("PRAGMA memory_limit='30GB';")
    return con


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 120,
    memory=49152,   # 48 GiB — headroom for the Tier-2 join + nested Arrow + index builds
    cpu=8.0,
)
def ingest_ca_ucc(
    zip_key: str = "",
    as_of: str = AS_OF_DEFAULT,
    trigger_callback_url: str | None = None,
) -> dict:
    """Land ZIP → extract → DuckDB project/cast → Lance overwrite (4 normalized +
    1 derived) → BTREE/BITMAP indexes, then record ops.* state and wake Trigger.
    Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt
    import gc
    import re

    import pyarrow.compute as pc

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", as_of or ""):
        raise ValueError(f"as_of must be YYYY-MM-DD, got {as_of!r}")

    started_at = dt.datetime.now(dt.timezone.utc)
    counts: dict[str, int] = {}
    index_status: dict[str, list] = {}
    data_through = None
    status = "error"
    error: str | None = None
    source_zip = zip_key

    try:
        so = _r2_storage_options()
        s3 = _s3_client()
        source_zip = zip_key or _discover_zip(s3)

        print(f"Downloading s3://{BUCKET}/{source_zip} → {LOCAL_ZIP}")
        s3.download_file(BUCKET, source_zip, LOCAL_ZIP)
        print(f"Downloaded {os.path.getsize(LOCAL_ZIP):,} bytes")
        paths = _extract_members(LOCAL_ZIP)

        # ── Tier 1: four normalized datasets, each independently committed ──
        tier1 = [
            ("filings", _sql_filings(paths["filings"], as_of)),
            ("debtors", _sql_party(paths["debtors"], as_of, "DEBTOR_TYPE", "debtor_type", MEMBER["debtors"])),
            ("secured_parties", _sql_party(paths["secured_parties"], as_of, "SECURED_PARTY_TYPE", "secured_party_type", MEMBER["secured_parties"])),
            ("filing_amendments", _sql_amendments(paths["filing_amendments"], as_of)),
        ]
        for ds_name, sql in tier1:
            print(f"\n=== Tier 1: {ds_name} ===")
            con = _duck()
            try:
                table = con.sql(sql).to_arrow_table()
            finally:
                con.close()
            counts[ds_name] = table.num_rows
            print(f"  rows={table.num_rows:,} → {DATASET_URI[ds_name]}")
            if ds_name == "filings" and table.num_rows:
                mx = pc.max(table.column("processed_date")).as_py()
                data_through = mx
                print(f"  data_through (max processed_date) = {mx}")
            _write_lance(table, DATASET_URI[ds_name], so)
            index_status[ds_name] = _create_indexes(ds_name, so)
            del table
            gc.collect()

        # ── Tier 2: derived firmographic view (nested LIST<STRUCT>) ──
        print(f"\n=== Tier 2: debtor_index ===")
        con = _duck()
        try:
            t2 = con.sql(_sql_debtor_index(
                paths["filings"], paths["debtors"], paths["secured_parties"], as_of
            )).to_arrow_table()
        finally:
            con.close()
        counts["debtor_index"] = t2.num_rows
        print(f"  rows={t2.num_rows:,} → {DATASET_URI['debtor_index']}")
        _write_lance(t2, DATASET_URI["debtor_index"], so)
        index_status["debtor_index"] = _create_indexes("debtor_index", so)
        del t2
        gc.collect()

        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        rows_total = int(sum(counts.values()))
        _record_run(source_zip, as_of, data_through, counts, rows_total,
                    status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "feed": FEED, "source_zip": source_zip, "as_of": as_of,
             "data_through": data_through.isoformat() if data_through else None,
             "rows_total": rows_total, "datasets": counts},
        )

    if status != "success":
        raise RuntimeError(f"ca_ucc ingest failed: {error}")
    return {"feed": FEED, "source_zip": source_zip, "as_of": as_of,
            "data_through": data_through.isoformat() if data_through else None,
            "rows_total": int(sum(counts.values())), "datasets": counts,
            "indexes": index_status, "status": status}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_db() -> dict:
    """Create ops schema + ops.ca_ucc_runs (idempotent)."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
        cur.execute(DDL_CA_UCC_RUNS)
        conn.commit()
    return {"created": "ops.ca_ucc_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 90,
    memory=32768,
    cpu=8.0,
)
def reindex(only: str = "") -> dict:
    """(Re)build scalar indexes on already-written datasets without re-ingesting.
    Idempotent (create_scalar_index defaults to replace=True)."""
    so = _r2_storage_options()
    targets = [only] if only else list(DATASET_URI)
    out = {}
    for ds_name in targets:
        if ds_name not in INDEXES:
            continue
        print(f"=== reindex {ds_name} ===")
        out[ds_name] = _create_indexes(ds_name, so)
    return out


@app.local_entrypoint()
def setup() -> None:
    """Create ops.ca_ucc_runs."""
    print(init_db.remote())


@app.local_entrypoint()
def run(zip_key: str = "", as_of: str = AS_OF_DEFAULT) -> None:
    """Spawn the ingest on the deployed app (manual; no Trigger callback).

    Prints the fc-id and returns immediately — no result summary here; the
    worker's ops.ca_ucc_runs row and ``modal app logs ca-ucc-filings`` are
    the record of the run."""
    import re

    from pipelines._shared.launch import spawn_deployed

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", as_of or ""):
        raise ValueError(f"as_of must be YYYY-MM-DD, got {as_of!r}")
    spawn_deployed("ca-ucc-filings", "ingest_ca_ucc", deploy_file=__file__,
                   zip_key=zip_key, as_of=as_of, trigger_callback_url=None)


@app.local_entrypoint()
def reindex_all(only: str = "") -> None:
    import json

    print(json.dumps(reindex.remote(only=only), indent=2, default=str))
