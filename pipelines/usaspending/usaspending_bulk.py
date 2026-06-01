"""Compute worker — USAspending full database bulk ingest.

Part of the dedicated ``usaspending-bulk`` Modal app (isolated from every other
feed). Endpoint-less functions spawned by the Universal Dispatcher
(core/modal_dispatcher.py) or driven by the local entrypoints. A BOUNDED ingest
of a single point-in-time database dump (snapshot 2026-05-06, the ``20260506``
filename stamp) — there is no Trigger cron.

The upstream artifact is a 161 GiB ZIP that is a PostgreSQL ``pg_dump`` DIRECTORY
archive: ``toc.dat`` (binary catalog) + 73 ``<dump_id>.dat.gz`` members. Every
member is ZIP ``method=stored`` — the bytes inside the ZIP ARE a standalone gzip
stream of PostgreSQL COPY text (tab-delimited, ``\\N`` NULL, ``\\.`` terminator).
So each table is an independently HTTP-Range-addressable object; we never unzip
the monolith.

Two-plane network strategy (approved):

  Phase 1 — stage_member (Python: I/O only, no parse). Explode the archive into
  per-table R2 landing objects by copying each member's stored byte range:
      ZIP64 central directory (3 tiny range reads → member byte ranges)
        → ranged GET (128 MiB parts, per-part retry — blip-resilient)
        → R2 multipart PUT → s3://data-sink/landing/usaspending/20260506/<...>.dat.gz
  Idempotent: a member already landed at the right size is skipped.

  Phase 2 — ingest_table / ingest_giant_table (DuckDB does 100% of the transform):
      R2 landing gz → /tmp (giant: large ephemeral_disk, land full .gz first)
        → schema from staged toc.dat (COPY column order + CREATE TABLE types)
        → DuckDB read_csv(delim=tab, columns=VARCHAR, \\N null, drop \\. terminator)
        → defensive TRY_CAST per pg type (arrays / jsonb stay VARCHAR — NEVER VARIANT)
        → Arrow: .to_arrow_table() (standard) | .to_arrow_reader() (giant, streamed)
        → lance.write_dataset(s3://data-sink/active/usaspending/<table>/, v2.1, overwrite)
  Per-table = per-dataset, so distinct tables write concurrently with no Lance
  commit conflict. Giants stream batches → bounded RAM.

  Phase 3 — build_table_indexes. Hard BTREE on load-bearing resolution keys (+
  BITMAP on low-cardinality categoricals), built once per dataset post-ingest.

Constraint compliance (directive): ``.to_arrow_table()`` / ``.to_arrow_reader()``
only (never ``.fetch_arrow_table()``); ``max_rows_per_file=1048576`` +
``max_bytes_per_file=90 * 1024**3``; NO ``use_lsm_write``; flat JSON callback body
(no ``{"data": ...}`` envelope); no bare DuckDB VARIANT on the Arrow wire.

    modal deploy pipelines/usaspending/usaspending_bulk.py
    modal run    pipelines/usaspending/usaspending_bulk.py::init_ops     # create ops.* table
    modal run    pipelines/usaspending/usaspending_bulk.py::stage        # Phase 1: explode ZIP → R2
    modal run    pipelines/usaspending/usaspending_bulk.py::stage --dry-run
    modal run    pipelines/usaspending/usaspending_bulk.py::backfill      # Phase 2: ingest → Lance
    modal run    pipelines/usaspending/usaspending_bulk.py::index         # Phase 3: scalar indexes
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
LANDING_PREFIX = "landing/usaspending/"
SNAPSHOT_STAMP = "20260506"          # source filename stamp
SNAPSHOT_DATE = "2026-05-06"
ZIP_URL = os.environ.get(
    "USASPENDING_ZIP_URL",
    "https://files.usaspending.gov/database_download/usaspending-db_20260506.zip",
)
# Lance system-of-record tier (NOT the landing/raw zone). One dataset per table.
# Lance is written to LOCAL disk then published to R2 via boto3 (uniform multipart
# parts) — Lance's own object-store writer trips R2's "all non-trailing parts must
# have the same length" rule on large data files. ACTIVE_BASE is for reference/
# verification (s3:// URI); ACTIVE_PREFIX is the boto3 key prefix.
ACTIVE_BASE = os.environ.get("USASPENDING_LANCE_BASE", "s3://data-sink/active/usaspending/")
ACTIVE_PREFIX = "active/usaspending/"
SCRATCH_DIR = "/tmp"
FEED = "usaspending_bulk"

# Lance fragment sizing (directive constraints, verbatim).
#   max_rows_per_file = 1048576 — exact.
#   max_bytes_per_file: the directive wrote `90 * 10243`; markdown ate the `**`.
#   The only reading that equals the documented 90 GiB ceiling is `90 * 1024**3`
#   (= 96,636,764,160) — also Lance's default and the PPP/entity constant.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# New core-x datasets MUST pin "2.1" (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"

# Size class split (directive override): members >= 3 GiB gz route to a large
# ephemeral_disk container that lands the full .gz to /tmp, then streams batches
# via .to_arrow_reader() (no FIFO — network blips must not break a named pipe).
GIANT_THRESHOLD_BYTES = 3 * 1024**3

# Phase 1 multipart copy tuning.
STAGE_PART_BYTES = 128 * 1024 * 1024   # 128 MiB parts (>= 5 MiB min, well under 10k-part cap)
SINGLE_PUT_MAX = 64 * 1024 * 1024      # <= 64 MiB members → one GET + put_object

# The 53 analytical members to STAGE (Tiers 1-3). raw.* (ingest=False) are staged
# for archive completeness but NOT transformed in Phase 2 (scope decision). The
# ~20 Django/auth/ETL/empty members are excluded entirely. Generated from the live
# ZIP64 central directory + pg_restore -l; byte ranges are recomputed at runtime.
TABLE_REGISTRY = [
    {"dump_id": "5968", "schema": "rpt", "table": "transaction_search_fpds", "gz_bytes": 43288068246, "ingest": True},
    {"dump_id": "5967", "schema": "rpt", "table": "award_search", "gz_bytes": 42088811597, "ingest": True},
    {"dump_id": "5969", "schema": "rpt", "table": "transaction_search_fabs", "gz_bytes": 31856387420, "ingest": True},
    {"dump_id": "5941", "schema": "raw", "table": "source_procurement_transaction", "gz_bytes": 17302132621, "ingest": False},
    {"dump_id": "5940", "schema": "raw", "table": "source_assistance_transaction", "gz_bytes": 16028340140, "ingest": False},
    {"dump_id": "5893", "schema": "public", "table": "financial_accounts_by_awards", "gz_bytes": 14819709515, "ingest": True},
    {"dump_id": "5965", "schema": "rpt", "table": "subaward_search", "gz_bytes": 4058579332, "ingest": True},
    {"dump_id": "5962", "schema": "rpt", "table": "recipient_lookup", "gz_bytes": 1283720075, "ingest": True},
    {"dump_id": "5966", "schema": "rpt", "table": "recipient_profile", "gz_bytes": 757501496, "ingest": True},
    {"dump_id": "5905", "schema": "public", "table": "financial_accounts_by_program_activity_object_class", "gz_bytes": 471105813, "ingest": True},
    {"dump_id": "5963", "schema": "rpt", "table": "summary_state_view", "gz_bytes": 458145695, "ingest": True},
    {"dump_id": "5915", "schema": "int", "table": "duns", "gz_bytes": 200074743, "ingest": True},
    {"dump_id": "5918", "schema": "public", "table": "historic_parent_duns", "gz_bytes": 62224477, "ingest": True},
    {"dump_id": "5956", "schema": "public", "table": "gtas_sf133_balances", "gz_bytes": 55526601, "ingest": True},
    {"dump_id": "5961", "schema": "public", "table": "uei_crosswalk", "gz_bytes": 47768512, "ingest": True},
    {"dump_id": "5943", "schema": "public", "table": "uei_crosswalk_2021", "gz_bytes": 47748114, "ingest": True},
    {"dump_id": "5869", "schema": "public", "table": "appropriation_account_balances", "gz_bytes": 29325550, "ingest": True},
    {"dump_id": "5924", "schema": "rpt", "table": "parent_award", "gz_bytes": 21072907, "ingest": True},
    {"dump_id": "5937", "schema": "public", "table": "reporting_agency_tas", "gz_bytes": 10057985, "ingest": True},
    {"dump_id": "5850", "schema": "public", "table": "references_cfda", "gz_bytes": 8192588, "ingest": True},
    {"dump_id": "5945", "schema": "public", "table": "ref_city_county_state_code", "gz_bytes": 6304289, "ingest": True},
    {"dump_id": "5951", "schema": "public", "table": "historical_appropriation_account_balances", "gz_bytes": 5532169, "ingest": True},
    {"dump_id": "5921", "schema": "public", "table": "reporting_agency_missing_tas", "gz_bytes": 2745637, "ingest": True},
    {"dump_id": "5935", "schema": "public", "table": "office", "gz_bytes": 1851985, "ingest": True},
    {"dump_id": "5863", "schema": "public", "table": "ref_program_activity", "gz_bytes": 1519531, "ingest": True},
    {"dump_id": "5875", "schema": "public", "table": "treasury_appropriation_account", "gz_bytes": 719944, "ingest": True},
    {"dump_id": "5846", "schema": "public", "table": "submission_attributes", "gz_bytes": 484351, "ingest": True},
    {"dump_id": "5932", "schema": "public", "table": "zips_grouped", "gz_bytes": 336311, "ingest": True},
    {"dump_id": "5939", "schema": "public", "table": "reporting_agency_overview", "gz_bytes": 217665, "ingest": True},
    {"dump_id": "5898", "schema": "public", "table": "naics", "gz_bytes": 149168, "ingest": True},
    {"dump_id": "5953", "schema": "public", "table": "program_activity_park", "gz_bytes": 140538, "ingest": True},
    {"dump_id": "5856", "schema": "public", "table": "frec_map", "gz_bytes": 136639, "ingest": True},
    {"dump_id": "5899", "schema": "public", "table": "psc", "gz_bytes": 134920, "ingest": True},
    {"dump_id": "5964", "schema": "rpt", "table": "covid_faba_spending", "gz_bytes": 126879, "ingest": True},
    {"dump_id": "5873", "schema": "public", "table": "federal_account", "gz_bytes": 76734, "ingest": True},
    {"dump_id": "5871", "schema": "public", "table": "budget_authority", "gz_bytes": 65374, "ingest": True},
    {"dump_id": "5919", "schema": "public", "table": "rosetta", "gz_bytes": 58004, "ingest": True},
    {"dump_id": "5949", "schema": "public", "table": "ref_population_county", "gz_bytes": 43763, "ingest": True},
    {"dump_id": "5865", "schema": "public", "table": "subtier_agency", "gz_bytes": 41028, "ingest": True},
    {"dump_id": "5952", "schema": "public", "table": "bureau_title_lookup", "gz_bytes": 36185, "ingest": True},
    {"dump_id": "5852", "schema": "public", "table": "references_definition", "gz_bytes": 28459, "ingest": True},
    {"dump_id": "5848", "schema": "public", "table": "agency", "gz_bytes": 23253, "ingest": True},
    {"dump_id": "5867", "schema": "public", "table": "toptier_agency", "gz_bytes": 17871, "ingest": True},
    {"dump_id": "5914", "schema": "public", "table": "state_data", "gz_bytes": 8357, "ingest": True},
    {"dump_id": "5861", "schema": "public", "table": "ref_country_code", "gz_bytes": 5012, "ingest": True},
    {"dump_id": "5947", "schema": "public", "table": "ref_population_cong_district", "gz_bytes": 4378, "ingest": True},
    {"dump_id": "5931", "schema": "public", "table": "cgac", "gz_bytes": 3956, "ingest": True},
    {"dump_id": "5933", "schema": "public", "table": "frec", "gz_bytes": 3882, "ingest": True},
    {"dump_id": "5957", "schema": "public", "table": "dabs_submission_window_schedule", "gz_bytes": 2841, "ingest": True},
    {"dump_id": "5954", "schema": "public", "table": "disaster_emergency_fund_code", "gz_bytes": 2163, "ingest": True},
    {"dump_id": "5860", "schema": "public", "table": "overall_totals", "gz_bytes": 1970, "ingest": True},
    {"dump_id": "5858", "schema": "public", "table": "object_class", "gz_bytes": 1423, "ingest": True},
    {"dump_id": "5930", "schema": "public", "table": "award_category", "gz_bytes": 114, "ingest": True},
]

# Scalar index plan — built ONCE post-ingest (Phase 3), per dataset. BTREE for
# high-cardinality load-bearing resolution keys (equality + range); BITMAP for
# low-cardinality categoricals filtered frequently. Columns are validated against
# the committed Lance schema at build time, so an absent name is skipped, never
# fatal. Reference/dimension tables are small → full-scan cheap → not indexed.
INDEX_PLAN = {
    "transaction_search_fpds": {
        "btree": ["transaction_id", "award_id", "generated_unique_award_id", "recipient_uei",
                  "recipient_hash", "action_date", "naics_code", "product_or_service_code",
                  "awarding_agency_id", "parent_award_id"],
        "bitmap": ["fiscal_year", "type", "awarding_toptier_agency_code"],
    },
    "transaction_search_fabs": {
        "btree": ["transaction_id", "award_id", "generated_unique_award_id", "recipient_uei",
                  "recipient_hash", "action_date", "cfda_number", "awarding_agency_id"],
        "bitmap": ["fiscal_year", "type", "awarding_toptier_agency_code"],
    },
    "award_search": {
        "btree": ["award_id", "generated_unique_award_id", "recipient_uei", "recipient_hash",
                  "action_date", "period_of_performance_start_date", "naics_code", "cfda_number",
                  "awarding_agency_id", "funding_agency_id", "piid", "fain", "uri"],
        "bitmap": ["type", "category", "awarding_toptier_agency_code"],
    },
    "subaward_search": {
        "btree": ["broker_subaward_id", "award_id", "unique_award_key", "prime_award_unique_key",
                  "sub_action_date", "subawardee_uei"],
        "bitmap": ["prime_award_group", "prime_award_type"],
    },
    "financial_accounts_by_awards": {
        "btree": ["financial_accounts_by_awards_id", "award_id", "treasury_account_id",
                  "submission_id", "distinct_award_key"],
        "bitmap": [],
    },
    "recipient_lookup": {
        "btree": ["id", "recipient_hash", "uei", "duns", "legal_business_name"],
        "bitmap": [],
    },
    "recipient_profile": {
        "btree": ["id", "recipient_hash", "uei", "recipient_unique_id"],
        "bitmap": ["recipient_level"],
    },
}

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",       # >=1.5 guarantees to_arrow_table/to_arrow_reader; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",           # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "requests>=2.32",       # ranged GET from files.usaspending.gov
    "boto3>=1.35",          # R2 landing get/put/multipart
    "psycopg[binary]>=3.2",  # ops.* terminal state
)

app = modal.App("usaspending-bulk", image=image)


# ─────────────────────────── R2 plumbing ───────────────────────────

def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2 (Modal secret r2-credentials)."""
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
    """boto3 S3 client for R2. Forces checksum behaviour to ``when_required`` —
    botocore's default flexible-checksum validation does not match R2 and
    otherwise raises on get/put/multipart."""
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


# ──────────────────── ZIP64 central-directory reader ────────────────────
# A pg_dump directory ZIP stores every member `stored` (no ZIP compression), so
# a member's stored byte range IS a standalone gzip stream. We read only the
# 6.6 KB central directory (3 tiny range GETs) to map dump_id → byte range.

def _http_range(url: str, start: int, end: int) -> bytes:
    import requests
    r = requests.get(url, headers={"Range": f"bytes={start}-{end}"},
                     timeout=(30, 300), allow_redirects=True)
    r.raise_for_status()
    return r.content


def _remote_size(url: str) -> int:
    import requests
    r = requests.head(url, timeout=60, allow_redirects=True)
    r.raise_for_status()
    return int(r.headers["Content-Length"])


def _central_directory(url: str) -> dict[str, dict]:
    """Return {member_name: {csize, lho}} parsed from the ZIP64 central directory."""
    import struct

    size = _remote_size(url)
    tail = _http_range(url, size - 65536, size - 1)
    loc = tail.rfind(b"PK\x06\x07")            # ZIP64 EOCD locator
    if loc == -1:
        raise RuntimeError("no ZIP64 EOCD locator — unexpected archive layout")
    z64_off = struct.unpack_from("<Q", tail, loc + 8)[0]
    z64 = _http_range(url, z64_off, z64_off + 64)
    cd_size = struct.unpack_from("<Q", z64, 40)[0]
    cd_off = struct.unpack_from("<Q", z64, 48)[0]
    cd = _http_range(url, cd_off, cd_off + cd_size - 1)

    out: dict[str, dict] = {}
    p = 0
    while p < len(cd) and cd[p:p + 4] == b"PK\x01\x02":
        (_, _, _, _, _, _, _, _, csize, usize,
         fn, ex, cm, _, _, _, lho) = struct.unpack_from("<IHHHHHHIIIHHHHHII", cd, p)
        name = cd[p + 46:p + 46 + fn].decode("utf-8", "replace")
        extra = cd[p + 46 + fn:p + 46 + fn + ex]
        if csize == 0xFFFFFFFF or lho == 0xFFFFFFFF:
            e = 0
            while e + 4 <= len(extra):
                hid, hsz = struct.unpack_from("<HH", extra, e)
                if hid == 0x0001:                # ZIP64 extended info
                    body = extra[e + 4:e + 4 + hsz]
                    o = 8 if usize == 0xFFFFFFFF else 0
                    if csize == 0xFFFFFFFF:
                        csize = struct.unpack_from("<Q", body, o)[0]
                        o += 8
                    if lho == 0xFFFFFFFF:
                        lho = struct.unpack_from("<Q", body, o)[0]
                    break
                e += 4 + hsz
        out[name] = {"csize": csize, "lho": lho}
        p += 46 + fn + ex + cm
    return out


def _member_data_range(url: str, entry: dict) -> tuple[int, int]:
    """Skip a member's local file header to reach its stored payload; return the
    inclusive [start, end] byte range of that payload."""
    import struct
    lfh = _http_range(url, entry["lho"], entry["lho"] + 29)
    if lfh[:4] != b"PK\x03\x04":
        raise RuntimeError("bad local file header signature")
    fn_len, ex_len = struct.unpack_from("<HH", lfh, 26)
    start = entry["lho"] + 30 + fn_len + ex_len
    return start, start + entry["csize"] - 1


def _landing_key(entry: dict) -> str:
    return f"{LANDING_PREFIX}{SNAPSHOT_STAMP}/{entry['dump_id']}_{entry['schema']}_{entry['table']}.dat.gz"


def _toc_landing_key() -> str:
    return f"{LANDING_PREFIX}{SNAPSHOT_STAMP}/toc.dat"


def _size_class(gz_bytes: int) -> str:
    return "giant" if gz_bytes >= GIANT_THRESHOLD_BYTES else "standard"


def _entry_for(schema: str, table: str) -> dict:
    """Reconstruct a table's full ingest entry from the registry (single source of
    truth). Phase 2 reads from the landing key, so no byte ranges are needed."""
    for e in TABLE_REGISTRY:
        if e["schema"] == schema and e["table"] == table:
            return {**e, "landing_key": _landing_key(e), "size_class": _size_class(e["gz_bytes"])}
    raise RuntimeError(f"unknown table {schema}.{table} (not in TABLE_REGISTRY)")


# ──────────────────── toc.dat schema parsing (Phase 2) ────────────────────
# The data file's columns == the COPY statement list (generated columns are
# excluded from COPY and may be reordered vs CREATE TABLE), so COPY is the
# authoritative read schema. Types are looked up from CREATE TABLE by name.
# Reserved-word identifiers (e.g. schema "int", column "authorization") are
# double-quoted in the dump.

def _qual(schema: str, table: str) -> str:
    import re
    return r'"?' + re.escape(schema) + r'"?\.' + r'"?' + re.escape(table) + r'"?'


def _copy_columns(toc_text: str, schema: str, table: str) -> list[str]:
    import re
    m = re.search(r"COPY " + _qual(schema, table) + r" \((.*?)\) FROM stdin;", toc_text, re.S)
    if not m:
        raise RuntimeError(f"COPY statement not found in toc for {schema}.{table}")
    return [c.strip().strip('"') for c in m.group(1).split(",")]


def _ddl_types(toc_text: str, schema: str, table: str) -> dict[str, str]:
    import re
    m = re.search(r"CREATE TABLE " + _qual(schema, table) + r" \((.*?)\n\);", toc_text, re.S)
    if not m:
        return {}
    out: dict[str, str] = {}
    for line in m.group(1).split("\n"):
        line = line.strip().rstrip(",")
        parts = line.split(None, 1)
        if len(parts) >= 2:
            out[parts[0].strip('"')] = parts[1].lower()
    return out


def _duck_type(pg: str) -> str:
    """Map a Postgres type to the DuckDB TRY_CAST target. Arrays / jsonb / json /
    uuid / bytea / text / char all become VARCHAR — NEVER a DuckDB VARIANT on the
    Arrow wire (directive constraint; 01_duckdb_processing.md §3)."""
    import re
    if re.match(r"(bigint|integer|smallint|int\d?|oid|serial|bigserial)\b", pg):
        return "BIGINT"
    if re.match(r"(numeric|decimal|double precision|real|money)\b", pg):
        return "DOUBLE"
    if re.match(r"timestamp\b", pg):
        return "TIMESTAMP"
    if re.match(r"date\b", pg):
        return "DATE"
    if re.match(r"(boolean|bool)\b", pg):
        return "BOOLEAN"
    return "VARCHAR"


def _build_ingest_sql(gz_path: str, columns: list[str], types: dict[str, str],
                      schema: str, table: str) -> str:
    """100% of the transform. Read every column as VARCHAR positionally (COPY
    order), drop the COPY ``\\.`` terminator row, then defensive TRY_CAST per the
    pg type. Repo-controlled inputs only (our /tmp path, registry strings, parsed
    column names) — no user input."""
    read_cols = ", ".join(f"'{c}': 'VARCHAR'" for c in columns)
    first = columns[0]
    projections = []
    for c in columns:
        dt = _duck_type(types.get(c, "text"))
        if dt == "VARCHAR":
            projections.append(f'nullif(trim("{c}"), \'\') AS "{c}"')
        else:
            projections.append(f'TRY_CAST(nullif(trim("{c}"), \'\') AS {dt}) AS "{c}"')
    proj_sql = ",\n    ".join(projections)
    # delim is a literal TAB; nullstr '\N'; new_line '\n'; terminator row '\.'.
    return f"""
WITH raw AS (
    SELECT *
    FROM read_csv(
        '{gz_path}',
        auto_detect = false, delim = '\t', header = false, quote = '', escape = '',
        nullstr = '\\N', null_padding = true, new_line = '\\n',
        max_line_size = 268435456, ignore_errors = false,
        columns = {{{read_cols}}}
    )
    WHERE "{first}" <> '\\.'
)
SELECT
    {proj_sql},
    '{schema}' AS source_schema,
    '{table}' AS source_table,
    CAST('{SNAPSHOT_DATE}' AS DATE) AS usaspending_snapshot_date,
    now() AS ingested_at
FROM raw
"""


# ─────────────────────────── ops + callback ───────────────────────────

def _record_run(entry: dict, rows: int, status: str, error, started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.usaspending_table_runs
                    (snapshot_date, dump_id, schema_name, table_name, landing_key,
                     size_class, member_bytes, rows_processed, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (SNAPSHOT_DATE, entry["dump_id"], entry["schema"], entry["table"],
                 entry.get("landing_key"), entry.get("size_class"), entry.get("gz_bytes"),
                 rows, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url. The whole body
    becomes result.output — NO API key, NO {"data": ...} envelope."""
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


# ─────────────────────────── Phase 1 — staging ───────────────────────────

def _ranged_get(url: str, start: int, end: int, attempts: int = 5) -> bytes:
    """Fetch [start, end] with retry — the blip-resilient unit of staging."""
    import time

    import requests

    last = None
    want = end - start + 1
    for i in range(attempts):
        try:
            r = requests.get(url, headers={"Range": f"bytes={start}-{end}"},
                             timeout=(30, 300), allow_redirects=True)
            if r.status_code in (200, 206):
                body = r.content
                if len(body) == want:
                    return body
                last = f"short read {len(body)} != {want}"
            else:
                last = f"status {r.status_code}"
        except Exception as exc:  # noqa: BLE001
            last = str(exc)
        time.sleep(2 * (i + 1))
    raise RuntimeError(f"ranged GET failed for bytes {start}-{end}: {last}")


def _staged_copy(s3, url: str, start: int, end: int, bucket: str, key: str) -> None:
    """Copy a member's stored byte range → R2. Small members in one put; large
    members via multipart with per-part ranged GET (a blip retries one 128 MiB
    part, not the whole transfer). Aborts the MPU cleanly on failure."""
    total = end - start + 1
    if total <= SINGLE_PUT_MAX:
        s3.put_object(Bucket=bucket, Key=key, Body=_ranged_get(url, start, end))
        return

    mpu = s3.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = mpu["UploadId"]
    parts = []
    try:
        part_no, pos = 1, start
        while pos <= end:
            sub_end = min(pos + STAGE_PART_BYTES - 1, end)
            resp = s3.upload_part(Bucket=bucket, Key=key, PartNumber=part_no,
                                  UploadId=upload_id, Body=_ranged_get(url, pos, sub_end))
            parts.append({"PartNumber": part_no, "ETag": resp["ETag"]})
            pos = sub_end + 1
            part_no += 1
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id,
                                     MultipartUpload={"Parts": parts})
    except Exception:
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise


@app.function(timeout=300)
def plan_manifest() -> list[dict]:
    """Read the ZIP64 central directory once and resolve every registry member's
    stored byte range. Appends a toc.dat catalog entry (Phase 2 reads the schema
    from it). Returns the staging manifest."""
    cd = _central_directory(ZIP_URL)
    manifest: list[dict] = []
    for entry in TABLE_REGISTRY:
        member = f"{entry['dump_id']}.dat.gz"
        if member not in cd:
            raise RuntimeError(f"member {member} ({entry['schema']}.{entry['table']}) absent from archive")
        start, end = _member_data_range(ZIP_URL, cd[member])
        manifest.append({
            **entry,
            "member": member,
            "byte_start": start,
            "byte_end": end,
            "landing_key": _landing_key(entry),
            "size_class": _size_class(entry["gz_bytes"]),
        })
    # toc.dat — the pg_dump catalog (schema map for Phase 2).
    if "toc.dat" not in cd:
        raise RuntimeError("toc.dat absent from archive")
    t_start, t_end = _member_data_range(ZIP_URL, cd["toc.dat"])
    manifest.append({
        "dump_id": "toc", "schema": "_catalog", "table": "toc",
        "gz_bytes": cd["toc.dat"]["csize"], "ingest": False, "member": "toc.dat",
        "byte_start": t_start, "byte_end": t_end,
        "landing_key": _toc_landing_key(), "size_class": "catalog",
    })
    return manifest


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 120,
    memory=4096,
    cpu=2.0,
    retries=2,            # whole-member retry on top of per-part retry; idempotent overwrite
    max_containers=10,    # be polite to files.usaspending.gov under fan-out
)
def stage_member(entry: dict) -> dict:
    """Phase 1 — copy one member's stored byte range from the source ZIP to the
    R2 landing zone. Idempotent: skip if already landed at the exact size."""
    s3 = _s3_client()
    key = entry["landing_key"]
    total = entry["byte_end"] - entry["byte_start"] + 1

    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        if head.get("ContentLength") == total:
            print(f"skip (already landed): {key} ({total:,} B)")
            return {"member": entry["member"], "table": entry.get("table"),
                    "landing_key": key, "bytes": total, "status": "skipped"}
    except Exception:
        pass  # not present → stage it

    _staged_copy(s3, ZIP_URL, entry["byte_start"], entry["byte_end"], BUCKET, key)
    head = s3.head_object(Bucket=BUCKET, Key=key)
    landed = head.get("ContentLength")
    status = "success" if landed == total else "size_mismatch"
    print(f"{status}: {key} ({landed:,}/{total:,} B)")
    return {"member": entry["member"], "table": entry.get("table"), "landing_key": key,
            "bytes": landed, "expected": total, "status": status}


# ─────────────────────────── Phase 2 — ingest ───────────────────────────

def _load_toc_text(s3) -> str:
    obj = s3.get_object(Bucket=BUCKET, Key=_toc_landing_key())
    return obj["Body"].read().decode("latin-1")


def _replace_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Idempotent publish: wipe the R2 key prefix, then upload the local Lance
    dataset (boto3 s3transfer = uniform-part multipart, R2-compliant — unlike
    Lance's own object-store writer). Per-table prefix → other datasets untouched.
    Returns files uploaded."""
    import os

    to_del = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            to_del.append({"Key": o["Key"]})
            if len(to_del) == 1000:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})
                to_del = []
    if to_del:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})

    uploaded = 0
    for root, _, files in os.walk(local_dir):
        for fn in files:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            s3.upload_file(lp, BUCKET, prefix + rel)
            uploaded += 1
    return uploaded


def _ingest(entry: dict, trigger_callback_url) -> dict:
    """Shared ingest core. Standard tables materialize one Arrow table; giants
    stream batches via to_arrow_reader. Lance is written to LOCAL disk (no R2
    multipart) then published to R2 via boto3 (uniform parts). Per-table prefix →
    idempotent and conflict-free: re-running one table replaces only its dataset."""
    import datetime as dt
    import os.path
    import shutil

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    schema, table, size_class = entry["schema"], entry["table"], entry["size_class"]
    prefix = f"{ACTIVE_PREFIX}{table}/"
    local_ds = os.path.join(SCRATCH_DIR, f"{table}_lance")
    gz_path = os.path.join(SCRATCH_DIR, f"{entry['dump_id']}.dat.gz")
    rows = 0
    status = "error"
    error = None

    try:
        s3 = _s3_client()
        s3.download_file(BUCKET, entry["landing_key"], gz_path)

        toc_text = _load_toc_text(s3)
        columns = _copy_columns(toc_text, schema, table)
        types = _ddl_types(toc_text, schema, table)
        sql = _build_ingest_sql(gz_path, columns, types, schema, table)

        shutil.rmtree(local_ds, ignore_errors=True)
        common = dict(data_storage_version=DATA_STORAGE_VERSION,
                      max_rows_per_file=MAX_ROWS_PER_FILE,
                      max_bytes_per_file=MAX_BYTES_PER_FILE)  # local write — no storage_options

        con = duckdb.connect(":memory:")
        try:
            con.execute(f"PRAGMA threads={8 if size_class == 'giant' else 4};")
            con.execute(f"PRAGMA temp_directory='{SCRATCH_DIR}/duckdb_spill';")
            rel = con.sql(sql)
            if size_class == "giant":
                reader = rel.to_arrow_reader(batch_size=500_000)
                lance.write_dataset(reader, local_ds, mode="create",
                                    schema=reader.schema, **common)
            else:
                arrow_table = rel.to_arrow_table()
                lance.write_dataset(arrow_table, local_ds, mode="create", **common)
        finally:
            con.close()

        rows = lance.dataset(local_ds).count_rows()
        uploaded = _replace_r2_prefix(s3, prefix, local_ds)
        print(f"{schema}.{table}: {rows:,} rows → {uploaded} files at s3://{BUCKET}/{prefix}")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        shutil.rmtree(local_ds, ignore_errors=True)
        try:
            os.remove(gz_path)
        except OSError:
            pass
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(entry, int(rows), status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": FEED,
             "schema": schema, "table": table, "snapshot_date": SNAPSHOT_DATE},
        )

    if status != "success":
        raise RuntimeError(f"usaspending ingest failed for {schema}.{table}: {error}")
    return {"feed": FEED, "schema": schema, "table": table, "rows_processed": int(rows),
            "size_class": size_class, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def ingest_table(schema: str, table: str, trigger_callback_url: str | None = None) -> dict:
    """Phase 2 — standard table (< 3 GiB gz). Full Arrow table in RAM."""
    return _ingest(_entry_for(schema, table), trigger_callback_url)


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 180,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=512 * 1024,   # 512 GiB scratch (Modal floor) — land the full .gz (≤ 43 GiB) + DuckDB spill
)
def ingest_giant_table(schema: str, table: str, trigger_callback_url: str | None = None) -> dict:
    """Phase 2 — giant table (>= 3 GiB gz). Lands the full .gz to /tmp (directive
    override: no FIFO), then streams batches via to_arrow_reader → Lance."""
    return _ingest(_entry_for(schema, table), trigger_callback_url)


# ─────────────────────────── Phase 3 — indexing ───────────────────────────

def _list_committed_indices(ds) -> list:
    """Best-effort read of committed scalar indices, tolerant of pylance shape drift."""
    for attr in ("list_indices", "list_indexes"):
        fn = getattr(ds, attr, None)
        if fn is None:
            continue
        try:
            out = []
            for ix in fn():
                if isinstance(ix, dict):
                    out.append({k: ix.get(k) for k in ("name", "type", "fields")})
                else:
                    out.append({"name": getattr(ix, "name", None),
                                "type": str(getattr(ix, "type", None)),
                                "fields": getattr(ix, "fields", None)})
            return out
        except Exception as exc:  # noqa: BLE001
            return [{"error": f"{attr}: {exc}"}]
    return [{"error": "no list_indices/list_indexes method"}]


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        # BTREE training sorts the column; bypass Lance's bounded spill-to-disk
        # ExternalSorter, which under-sizes its pool and exhausts on a 100M-row
        # column even in a large container. In-memory sort is within RAM here.
        modal.Secret.from_dict({"LANCE_BYPASS_SPILLING": "true"}),
    ],
    timeout=60 * 180,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=512 * 1024,   # stage the dataset (≤ tens of GiB for giants) locally
)
def build_table_indexes(table: str) -> dict:
    """Phase 3 — build BTREE + BITMAP scalar indexes on one active dataset. The
    dataset is staged R2 → LOCAL, indexed, then republished via boto3 (uniform
    parts): indexing directly on R2 trips the same "non-trailing parts must match"
    multipart rule the data write does. Columns absent from the schema are skipped;
    create_scalar_index defaults to replace=True → idempotent rebuild."""
    import os
    import shutil

    import lance

    plan = INDEX_PLAN.get(table)
    if not plan:
        return {"table": table, "status": "no_index_plan"}

    s3 = _s3_client()
    prefix = f"{ACTIVE_PREFIX}{table}/"
    local_ds = os.path.join(SCRATCH_DIR, f"{table}_lance_idx")
    shutil.rmtree(local_ds, ignore_errors=True)

    staged = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            dst = os.path.join(local_ds, o["Key"][len(prefix):])
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            s3.download_file(BUCKET, o["Key"], dst)
            staged += 1
    if staged == 0:
        return {"table": table, "status": "dataset_not_found", "prefix": prefix}

    ds = lance.dataset(local_ds)
    present = set(ds.schema.names)
    rows = ds.count_rows()
    built = {"btree": [], "bitmap": [], "skipped": []}
    for kind, idx_type in (("btree", "BTREE"), ("bitmap", "BITMAP")):
        for col in plan.get(kind, []):
            if col not in present:
                built["skipped"].append(col)
                continue
            try:
                ds.create_scalar_index(col, index_type=idx_type)
                built[kind].append(col)
                print(f"  {idx_type:<6} ✓ {col}")
            except Exception as exc:  # noqa: BLE001
                built["skipped"].append(f"{col} ({exc})")

    uploaded = _replace_r2_prefix(s3, prefix, local_ds)
    committed = _list_committed_indices(lance.dataset(local_ds))
    shutil.rmtree(local_ds, ignore_errors=True)
    return {"table": table, "rows": rows, "uploaded": uploaded, **built,
            "committed_indices": committed}


# ─────────────────────────── ops bootstrap ───────────────────────────

@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=300)
def list_landing() -> dict:
    """List the R2 landing prefix for this snapshot — Phase 1 verification."""
    s3 = _s3_client()
    prefix = f"{LANDING_PREFIX}{SNAPSHOT_STAMP}/"
    objs = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            objs.append({"key": o["Key"], "bytes": o["Size"]})
    return {"prefix": prefix, "count": len(objs),
            "total_bytes": sum(o["bytes"] for o in objs), "objects": objs}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=300)
def active_status() -> dict:
    """List active/usaspending/ and summarize each landed Lance dataset (files +
    bytes per table) — Phase 2 dataset-landing verification."""
    s3 = _s3_client()
    tables: dict[str, dict] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=ACTIVE_PREFIX):
        for o in page.get("Contents", []):
            t = o["Key"][len(ACTIVE_PREFIX):].split("/", 1)[0]
            d = tables.setdefault(t, {"files": 0, "bytes": 0})
            d["files"] += 1
            d["bytes"] += o["Size"]
    return {"prefix": ACTIVE_PREFIX, "dataset_count": len(tables),
            "datasets": {t: tables[t] for t in sorted(tables)}}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=300)
def abort_incomplete_mpus() -> dict:
    """Abort dangling incomplete multipart uploads under the landing prefix —
    left behind when a staging container is hard-killed (the except-path abort
    cannot run). Idempotent maintenance; safe to run anytime."""
    s3 = _s3_client()
    prefix = f"{LANDING_PREFIX}{SNAPSHOT_STAMP}/"
    aborted, key_marker, upload_marker = [], None, None
    while True:
        kw = dict(Bucket=BUCKET, Prefix=prefix)
        if key_marker:
            kw["KeyMarker"] = key_marker
        if upload_marker:
            kw["UploadIdMarker"] = upload_marker
        resp = s3.list_multipart_uploads(**kw)
        for u in resp.get("Uploads", []):
            s3.abort_multipart_upload(Bucket=BUCKET, Key=u["Key"], UploadId=u["UploadId"])
            aborted.append({"key": u["Key"], "upload_id": u["UploadId"]})
        if not resp.get("IsTruncated"):
            break
        key_marker = resp.get("NextKeyMarker")
        upload_marker = resp.get("NextUploadIdMarker")
    return {"prefix": prefix, "aborted": len(aborted), "details": aborted}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60)
def ops_summary() -> dict:
    """Latest ops.* terminal row per table for this snapshot — Phase 2 ground truth."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (schema_name, table_name)
                   schema_name, table_name, status, rows_processed, error, size_class
            FROM ops.usaspending_table_runs
            WHERE snapshot_date = %s
            ORDER BY schema_name, table_name, recorded_at DESC
            """,
            (SNAPSHOT_DATE,),
        )
        out = [{"schema": r[0], "table": r[1], "status": r[2], "rows": r[3],
                "error": (r[4] or "")[:140], "size_class": r[5]} for r in cur.fetchall()]
    return {"tables": out}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def apply_ops_ddl(sql: str) -> dict:
    """Execute the ops.* DDL (idempotent CREATE … IF NOT EXISTS) via psycopg."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    return {"applied": True}


# ─────────────────────────── local entrypoints ───────────────────────────

@app.local_entrypoint()
def init_ops() -> None:
    """Create ops.usaspending_table_runs from the co-located DDL file."""
    from pathlib import Path

    sql = Path(__file__).parent.joinpath("ops_usaspending_table_runs.sql").read_text()
    print(apply_ops_ddl.remote(sql))


@app.local_entrypoint()
def stage(dry_run: bool = False, only: str = "") -> None:
    """Phase 1 — explode the ZIP into per-member R2 landing objects (parallel
    fan-out; independent keys, no serialization). ``--only SUBSTR`` filters;
    ``--dry-run`` prints the manifest."""
    manifest = plan_manifest.remote()
    if only:
        manifest = [m for m in manifest if only in m["table"] or only in m["dump_id"]]

    print(f"Phase 1 — staging {len(manifest)} object(s) → s3://{BUCKET}/{LANDING_PREFIX}{SNAPSHOT_STAMP}/")
    for m in manifest:
        print(f"  {m['member']:<16} {m['schema']}.{m['table']:<48} "
              f"{m['gz_bytes']:>14,} B  [{m['size_class']}]")
    if dry_run:
        return

    ok = mismatch = failed = 0
    table_ok = 0
    for r in stage_member.map(manifest, order_outputs=False, return_exceptions=True):
        if isinstance(r, dict) and r.get("status") in ("success", "skipped"):
            ok += 1
            if r.get("member") != "toc.dat":
                table_ok += 1
        elif isinstance(r, dict) and r.get("status") == "size_mismatch":
            mismatch += 1
            print(f"  SIZE MISMATCH: {r}")
        else:
            failed += 1
            print(f"  FAILED: {r}")

    print(f"\nStaged OK: {ok}/{len(manifest)}  (table members: {table_ok})  "
          f"mismatch={mismatch} failed={failed}")


@app.local_entrypoint()
def verify() -> None:
    """Independently verify Phase 1: list landed objects and cross-check every
    table member's size against the central-directory size."""
    res = list_landing.remote()
    print(f"Landing: s3://{BUCKET}/{res['prefix']}")
    print(f"Objects: {res['count']}   "
          f"Total: {res['total_bytes']:,} B ({res['total_bytes'] / 1024**3:.1f} GiB)")

    expected = {f"{e['dump_id']}_{e['schema']}_{e['table']}.dat.gz": e["gz_bytes"]
                for e in TABLE_REGISTRY}
    got = {o["key"].rsplit("/", 1)[-1]: o["bytes"] for o in res["objects"]}
    table_objs = [k for k in got if k != "toc.dat"]
    missing = [k for k in expected if k not in got]
    mismatch = [(k, expected[k], got[k]) for k in expected if k in got and expected[k] != got[k]]

    print(f"Table members: {len(table_objs)}/{len(expected)}   toc.dat: {'toc.dat' in got}")
    if missing:
        print(f"MISSING ({len(missing)}): {missing}")
    if mismatch:
        print(f"SIZE MISMATCH ({len(mismatch)}): {mismatch}")
    if not missing and not mismatch:
        print(f"✓ all {len(expected)} table members present at exact central-directory sizes")


@app.local_entrypoint()
def ops_status() -> None:
    """Print the latest ops.* terminal state per table for this snapshot."""
    res = ops_summary.remote()
    by = {"success": [], "error": [], "other": []}
    for t in res["tables"]:
        by.get(t["status"] if t["status"] in by else "other").append(t)
    print(f"ops rows: {len(res['tables'])}  success={len(by['success'])}  "
          f"error={len(by['error'])}  other={len(by['other'])}")
    for t in sorted(res["tables"], key=lambda x: (x["status"], x["table"])):
        flag = "" if t["status"] == "success" else "  ‹‹"
        print(f"  [{t['status']:<7}] {t['schema']}.{t['table']:<48} "
              f"{(t['rows'] or 0):>12,} rows  {t['size_class'] or ''}{flag} {t['error']}")


@app.local_entrypoint()
def active() -> None:
    """Summarize the landed Lance datasets in active/usaspending/ (Phase 2 check)."""
    res = active_status.remote()
    print(f"active/usaspending/ — {res['dataset_count']} datasets")
    for t, d in res["datasets"].items():
        print(f"  {t:<52} {d['files']:>5} files  {d['bytes']:>16,} B")


@app.local_entrypoint()
def cleanup_mpus() -> None:
    """Abort dangling incomplete multipart uploads in the landing zone."""
    import json

    print(json.dumps(abort_incomplete_mpus.remote(), indent=2, default=str))


@app.local_entrypoint()
def backfill(only: str = "", dry_run: bool = False) -> None:
    """Phase 2 (manual, no callback) — ingest landed members into per-table Lance
    datasets. Production cadence runs via the Trigger task; this is for ops. Giants
    route to the large-disk container automatically."""
    entries = [e for e in TABLE_REGISTRY if e["ingest"]]
    if only:
        entries = [e for e in entries if only in e["table"]]
    print(f"Phase 2 — ingesting {len(entries)} table(s) → {ACTIVE_BASE}<table>/")
    for e in entries:
        size_class = _size_class(e["gz_bytes"])
        if dry_run:
            print(f"  {e['schema']}.{e['table']:<48} [{size_class}]")
            continue
        fn = ingest_giant_table if size_class == "giant" else ingest_table
        print(fn.remote(e["schema"], e["table"], trigger_callback_url=None))


@app.local_entrypoint()
def index(only: str = "") -> None:
    """Phase 3 — build scalar indexes on the planned datasets."""
    import json

    tables = [t for t in INDEX_PLAN if (only in t if only else True)]
    for t in tables:
        print(json.dumps(build_table_indexes.remote(t), indent=2, default=str))
