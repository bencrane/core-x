"""Compute worker — California CSLB (Contractors State License Board) registry bulk ingest.

Part of the ``ca-cslb-pipelines`` Modal app. Endpoint-less functions, spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints.
Clean-room data plane: no Iceberg, no Polaris — DuckDB does 100% of the transform,
Lance is written straight to R2.

Topology (proven by the diagnostic probe — pipelines/ca_cslb/probe.py). Three plain
UTF-8 CSV payloads in R2 landing, three DISTINCT Lance datasets keyed on
``license_number``:

    MasterLicenseData.csv (74.1 MiB, 244,760 rows, 52 cols)  entity spine, PK license_number
        -> s3://data-sink/active/cslb_licenses/
    PersonnelData.csv     (81.8 MiB, 406,192 rows, 19 cols)  1:N child, 100% RI to master
        -> s3://data-sink/active/cslb_personnel/
    WorkerCompData.csv    (31.5 MiB, 247,732 rows, 10 cols)  coverage peer (soft key)
        -> s3://data-sink/active/cslb_workers_comp/

Load-bearing diagnostic findings shaped this worker:
  1. ENCODING — all three are GENUINE UTF-8 (strict validation passes; the handful of
     high bytes are legitimate 2-byte sequences, dirty noise in policy strings). NO
     cp1252 transcode (a transcode would corrupt valid multibyte). Python streams the
     landed bytes untouched; DuckDB reads encoding='utf-8' (default).
  2. DIALECT — comma-delimited, CRLF, RFC-4180 quoting (quoted commas exist, e.g.
     `" Leasing Firm, Temp Agency, etc"`); pervasive leading space after each delimiter
     (trim() on every projected field is mandatory).
  3. KEYS — strictly VARCHAR (license_number, sequence_number, zip_code, every bond /
     policy number). Clean numerics today, but VARCHAR safeguards leading zeros (zip
     `01095`), alphanumerics (zip `V8Z3P3`, bond `0000002920303`) and lexical-sort
     safety. NO numeric cast.
  4. WORKERS-COMP IS A PEER, NOT A CHILD — only 49.42% of WorkerComp licenses appear in
     the master spine (125,303 fall outside it). NO foreign key is enforced; the three
     datasets join on license_number at query time. WorkerComp is preserved whole.
  5. PERSONNEL REPEATING GROUP — EMP-Titl-CDE / CL-CDE / CL-CDE-STAT / ASSN-DT /
     DIS-ASSN-DT are a positionally-aligned 5-field group packed with '|' inside single
     CSV cells. Projected in SQL as an explicit LIST<STRUCT> (string_split + positional
     zip; NO VARIANT), with a per-row safety valve: when the five element counts
     disagree the struct list is NULL and the raw '|' cells (always preserved) are the
     fallback. `associations_aligned` flags which path each row took.

Single-phase ingest per target (no explode — the payloads are uncompressed CSV):
  download landed CSV -> DuckDB read_csv(all_varchar, quote-aware) project/cast 100% in
  SQL -> con.execute(...).to_arrow_table() -> lance.write_dataset(overwrite, v2.1,
  DIRECT to R2 via storage_options) -> BTREE + BITMAP scalar indexes. The three datasets
  are DISTINCT, so they fan out in PARALLEL with no shared-writer manifest conflict.

Storage: files are small (≤74 MiB; ≤406k rows) and the string indexes are well below
R2's multipart escalation threshold — so this writes Lance DIRECTLY to R2 (the FL-SoS
pattern), NOT the PDL local-stage-then-boto3-publish pattern. No staging required.

Control plane (Trigger v4 durable callback): each function accepts
``trigger_callback_url`` and, on terminal state, (1) writes a run row to
``ops.cslb_runs`` via psycopg and (2) POSTs a FLAT JSON body to that url. No
``{"data": ...}`` envelope. Manual/on-demand by design (no cron) — refresh is a manual
R2 drop.

    modal deploy pipelines/ca_cslb/ingest.py
    modal run    pipelines/ca_cslb/ingest.py::init_state                       # create ops.cslb_runs
    modal run    pipelines/ca_cslb/ingest.py::run_all                          # ingest all three in parallel
    modal run    pipelines/ca_cslb/ingest.py::ingest --target licenses|personnel|workers_comp
    modal run    pipelines/ca_cslb/ingest.py::reindex --target workers_comp    # rebuild indexes only
    modal run    pipelines/ca_cslb/ingest.py::show_ledger
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
LANDING_PREFIX = "landing/ca_cslb/"
SCRATCH_DIR = "/tmp/ca_cslb"
AS_OF_DEFAULT = "2026-05-31"

# Lance system-of-record tier (env-overridable). Three datasets joined on license_number.
LICENSES_URI = os.environ.get("CSLB_LICENSES_LANCE_URI", "s3://data-sink/active/cslb_licenses/")
PERSONNEL_URI = os.environ.get("CSLB_PERSONNEL_LANCE_URI", "s3://data-sink/active/cslb_personnel/")
WORKERS_COMP_URI = os.environ.get("CSLB_WORKERS_COMP_LANCE_URI", "s3://data-sink/active/cslb_workers_comp/")

# logical target -> landed CSV member + Lance dataset URI.
SOURCES: dict[str, dict[str, str]] = {
    "licenses": {"csv": "MasterLicenseData.csv", "uri": LICENSES_URI},
    "personnel": {"csv": "PersonnelData.csv", "uri": PERSONNEL_URI},
    "workers_comp": {"csv": "WorkerCompData.csv", "uri": WORKERS_COMP_URI},
}

# Lance fragment sizing (directive constraints).
#   max_rows_per_file = 1048576 — exact (also the Lance default).
#   max_bytes_per_file: the directive wrote `90 * 10243` annotated "(90 GiB)". The only
#   reading equal to 90 GiB is `90 * 1024**3` (= 96,636,764,160) — Lance's documented
#   default and the constant used by every other worker in this fleet. Honor 90 GiB.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new datasets → pin the current Lance default (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan (approved). BTREE = high-cardinality resolution / join keys; BITMAP =
# low-cardinality categoricals. Cardinalities measured by the probe over the full files.
INDEX_PLAN: dict[str, dict[str, list[str]]] = {
    "licenses": {
        "btree": ["license_number", "business_name", "full_business_name"],
        "bitmap": ["primary_status", "business_type", "wc_coverage_type",
                   "state", "county", "asbestos_registration"],
    },
    "personnel": {
        "btree": ["license_number", "personnel_name"],
        "bitmap": ["name_type", "surety_type"],
    },
    "workers_comp": {
        "btree": ["license_number", "wc_policy_number"],
        "bitmap": ["wc_coverage_type", "wc_insurance_company"],
    },
}

# Mirrored verbatim by pipelines/ca_cslb/ops_cslb_runs.sql. Applied by init_state.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.cslb_runs (
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
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cslb_runs_target_idx      ON ops.cslb_runs (target);
CREATE INDEX IF NOT EXISTS cslb_runs_phase_idx       ON ops.cslb_runs (phase);
CREATE INDEX IF NOT EXISTS cslb_runs_status_idx      ON ops.cslb_runs (status);
CREATE INDEX IF NOT EXISTS cslb_runs_recorded_at_idx ON ops.cslb_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",           # R2 landing read
    "requests>=2.32",        # Trigger waitpoint callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    {"LANCE_BYPASS_SPILLING": "true"}  # in-memory BTREE sort (lance-format/lance#2650); cheap at this scale
)

app = modal.App("ca-cslb-pipelines", image=image)

# read_csv options — IDENTICAL to the verified probe (guarantees the diagnostic row
# counts reproduce). quote-aware (RFC-4180), all_varchar, malformed rows quarantined to
# the rejects table rather than aborting the load.
READ_OPTS = (
    "all_varchar=true, header=true, delim=',', quote='\"', escape='\"', "
    "sample_size=-1, ignore_errors=true, null_padding=true, store_rejects=true"
)


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


def _s3_client():
    """boto3 S3 client for R2. checksum behaviour forced to ``when_required`` (R2 semantics)."""
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


# ── SQL projection helpers. Identifiers are double-quoted (CSLB headers carry hyphens,
#    parens, mixed case). `S` = trimmed VARCHAR; `D` = MM/DD/YYYY → DATE (TRY_* nulls
#    bad cells). Every value is repo-controlled; the /tmp path, source_file, and as_of
#    are bound positionally via ? (never interpolated). ──
def S(col: str, alias: str) -> str:
    return f"nullif(trim({col}), '') AS {alias}"


def D(col: str, alias: str) -> str:
    return f"TRY_CAST(TRY_STRPTIME(nullif(trim({col}), ''), '%m/%d/%Y') AS DATE) AS {alias}"


def _sql_licenses() -> str:
    cols = [
        S('"LicenseNo"', "license_number"),
        D('"LastUpdate"', "last_update_date"),
        S('"BusinessName"', "business_name"),
        S('"BUS-NAME-2"', "business_name_2"),
        S('"FullBusinessName"', "full_business_name"),
        S('"MailingAddress"', "mailing_address"),
        S('"City"', "city"),
        S('"State"', "state"),
        S('"County"', "county"),
        S('"ZIPCode"', "zip_code"),
        S('"country"', "country"),
        S('"BusinessPhone"', "business_phone"),
        S('"BusinessType"', "business_type"),
        D('"IssueDate"', "issue_date"),
        D('"ReissueDate"', "reissue_date"),
        D('"ExpirationDate"', "expiration_date"),
        D('"InactivationDate"', "inactivation_date"),
        D('"ReactivationDate"', "reactivation_date"),
        D('"PendingSuspension"', "pending_suspension_date"),
        D('"PendingClassRemoval"', "pending_class_removal_date"),
        D('"PendingClassReplace"', "pending_class_replace_date"),
        S('"PrimaryStatus"', "primary_status"),
        S('"SecondaryStatus"', "secondary_status"),
        # classifications: keep the raw pipe-packed cell (lossless) AND an explicit
        # LIST<VARCHAR> of trimmed atomic codes (no VARIANT).
        S('"Classifications(s)"', "classifications_raw"),
        ("list_transform(string_split(nullif(trim(\"Classifications(s)\"), ''), '|'), "
         "x -> nullif(trim(x), '')) AS classifications"),
        S('"AsbestosReg"', "asbestos_registration"),
        S('"WorkersCompCoverageType"', "wc_coverage_type"),
        S('"WCInsuranceCompany"', "wc_insurance_company"),
        S('"WCPolicyNumber"', "wc_policy_number"),
        D('"WCEffectiveDate"', "wc_effective_date"),
        D('"WCExpirationDate"', "wc_expiration_date"),
        D('"WCCancellationDate"', "wc_cancellation_date"),
        D('"WCSuspendDate"', "wc_suspend_date"),
        S('"CBSuretyCompany"', "contractor_bond_surety_company"),
        S('"CBNumber"', "contractor_bond_number"),
        D('"CBEffectiveDate"', "contractor_bond_effective_date"),
        D('"CBCancellationDate"', "contractor_bond_cancellation_date"),
        S('"CBAmount"', "contractor_bond_amount"),
        S('"WBSuretyCompany"', "llc_worker_bond_surety_company"),
        S('"WBNumber"', "llc_worker_bond_number"),
        D('"WBEffectiveDate"', "llc_worker_bond_effective_date"),
        D('"WBCancellationDate"', "llc_worker_bond_cancellation_date"),
        S('"WBAmount"', "llc_worker_bond_amount"),
        S('"DBSuretyCompany"', "disciplinary_bond_surety_company"),
        S('"DBNumber"', "disciplinary_bond_number"),
        D('"DBEffectiveDate"', "disciplinary_bond_effective_date"),
        D('"DBCancellationDate"', "disciplinary_bond_cancellation_date"),
        S('"DBAmount"', "disciplinary_bond_amount"),
        D('"DateRequired"', "bond_date_required"),
        S('"DiscpCaseRegion"', "disciplinary_case_region"),
        S('"DBBondReason"', "disciplinary_bond_reason"),
        S('"DBCaseNo"', "disciplinary_case_number"),
        S('"NAME-TP-2"', "name_type_2"),
    ]
    projection = ",\n    ".join(cols)
    return (
        f"WITH raw AS (SELECT * FROM read_csv(?, {READ_OPTS}))\n"
        f"SELECT\n    {projection},\n"
        "    ? AS source_file,\n"
        "    CAST(? AS DATE) AS snapshot_date,\n"
        "    now() AS ingested_at\n"
        "FROM raw"
    )


def _sql_personnel() -> str:
    # The five '|'-packed cells, split + element-trimmed (dates parsed) into aligned lists.
    parts = (
        "    list_transform(string_split(\"EMP-Titl-CDE\", '|'), x -> nullif(trim(x), '')) AS _titles,\n"
        "    list_transform(string_split(\"CL-CDE\", '|'), x -> nullif(trim(x), '')) AS _classes,\n"
        "    list_transform(string_split(\"CL-CDE-STAT\", '|'), x -> nullif(trim(x), '')) AS _statuses,\n"
        "    list_transform(string_split(\"ASSN-DT\", '|'), "
        "x -> TRY_CAST(TRY_STRPTIME(nullif(trim(x), ''), '%m/%d/%Y') AS DATE)) AS _assn,\n"
        "    list_transform(string_split(\"DIS-ASSN-DT\", '|'), "
        "x -> TRY_CAST(TRY_STRPTIME(nullif(trim(x), ''), '%m/%d/%Y') AS DATE)) AS _disassn"
    )
    aligned = ("len(_titles) = len(_classes) AND len(_titles) = len(_statuses) "
               "AND len(_titles) = len(_assn) AND len(_titles) = len(_disassn)")
    cols = [
        S('"LIC-NO"', "license_number"),
        D('"LastUpdated"', "last_update_date"),
        S('"REC-TP"', "record_type"),
        S('"SEQ-NO"', "sequence_number"),
        S('"Name-TP"', "name_type"),
        S('"Name"', "personnel_name"),
        # raw pipe-packed cells — ALWAYS preserved (lossless safety-valve fallback).
        S('"EMP-Titl-CDE"', "employee_title_codes_raw"),
        S('"CL-CDE"', "classification_codes_raw"),
        S('"CL-CDE-STAT"', "classification_status_raw"),
        S('"ASSN-DT"', "association_dates_raw"),
        S('"DIS-ASSN-DT"', "disassociation_dates_raw"),
        # explicit nested LIST<STRUCT> when the 5 groups are positionally aligned;
        # NULL (→ raw cells above) on element-count mismatch. NO VARIANT.
        (f"CASE WHEN {aligned} THEN list_transform(range(1, len(_titles) + 1),\n"
         "        i -> {'title': _titles[i], 'classification': _classes[i], "
         "'class_status': _statuses[i], 'assoc_date': _assn[i], 'disassoc_date': _disassn[i]})\n"
         "      ELSE NULL END AS personnel_associations"),
        f"({aligned}) AS associations_aligned",
        S('"SURETY-TP"', "surety_type"),
        S('"SuretyCompany"', "surety_company"),
        S('"BOND-NO"', "bond_number"),
        S('"BOND-AMT"', "bond_amount"),
        D('"EffectiveDate"', "bond_effective_date"),
        D('"CancellationDate"', "bond_cancellation_date"),
        S('"JointVentureLicenseType"', "joint_venture_license_type"),
        S('"JointVentureLicenseNumber"', "joint_venture_license_number"),
    ]
    projection = ",\n    ".join(cols)
    return (
        f"WITH raw AS (SELECT * FROM read_csv(?, {READ_OPTS})),\n"
        f"parts AS (\n  SELECT *,\n{parts}\n  FROM raw\n)\n"
        f"SELECT\n    {projection},\n"
        "    ? AS source_file,\n"
        "    CAST(? AS DATE) AS snapshot_date,\n"
        "    now() AS ingested_at\n"
        "FROM parts"
    )


def _sql_workers_comp() -> str:
    cols = [
        S('"LicenseType"', "license_type"),
        S('"LicenseNo"', "license_number"),
        D('"LastUpdate"', "last_update_date"),
        S('"WorkersCompCoverageType"', "wc_coverage_type"),
        S('"WCInsuranceCompany"', "wc_insurance_company"),
        S('"WCPolicyNo"', "wc_policy_number"),
        D('"EffectiveDate"', "wc_effective_date"),
        D('"ExpirationDate"', "wc_expiration_date"),
        D('"CancellationDate"', "wc_cancellation_date"),
        D('"WCSuspendDate"', "wc_suspend_date"),
    ]
    projection = ",\n    ".join(cols)
    return (
        f"WITH raw AS (SELECT * FROM read_csv(?, {READ_OPTS}))\n"
        f"SELECT\n    {projection},\n"
        "    ? AS source_file,\n"
        "    CAST(? AS DATE) AS snapshot_date,\n"
        "    now() AS ingested_at\n"
        "FROM raw"
    )


_SQL_BUILDERS = {
    "licenses": _sql_licenses,
    "personnel": _sql_personnel,
    "workers_comp": _sql_workers_comp,
}


def _build_indexes(target: str, so: dict) -> list[str]:
    """Build BTREE + BITMAP scalar indexes for one dataset. create_scalar_index defaults
    to replace=True → idempotent. An index miss must not fail the load."""
    import lance

    ds = lance.dataset(SOURCES[target]["uri"], storage_options=so)
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


def _record_run(phase, target, dataset_uri, as_of, source_file, landing_key, rows,
                rejected, status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.cslb_runs (psycopg). Best-effort: never let an audit-write
    failure crash an otherwise-good ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.cslb_runs
                    (phase, target, dataset_uri, as_of, source_file, landing_key,
                     rows_processed, rejected_rows, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (phase, target, dataset_uri, as_of, source_file, landing_key,
                 rows, rejected, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint URL. FLAT JSON body — no
    ``{"data": ...}`` envelope. The whole body becomes result.output."""
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


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_state_schema() -> dict:
    """Apply the idempotent ops.cslb_runs DDL. Run once before the first ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.cslb_runs schema.")
    return {"status": "success", "table": "ops.cslb_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 30,
    memory=16384,
    cpu=8.0,
)
def ingest_target(target: str, as_of: str = AS_OF_DEFAULT,
                  trigger_callback_url: str | None = None) -> dict:
    """Download the landed CSV → DuckDB project/cast (100% transform) → Arrow → Lance
    overwrite DIRECT to R2 → BTREE/BITMAP indexes; record ops.* + wake Trigger. Re-raises
    on failure so the Modal call is marked failed."""
    import datetime as dt
    import os.path

    import duckdb
    import lance

    target = target.strip().lower()
    if target not in SOURCES:
        raise ValueError(f"target must be one of {sorted(SOURCES)}, got {target!r}")

    meta = SOURCES[target]
    dataset_uri = meta["uri"]
    source_file = meta["csv"]
    landing_key = f"{LANDING_PREFIX}{source_file}"
    started_at = dt.datetime.now(dt.timezone.utc)
    rows, rejected = 0, 0
    status, error = "error", None
    built: list[str] = []

    try:
        so = _r2_storage_options()
        s3 = _s3_client()
        os.makedirs(SCRATCH_DIR, exist_ok=True)
        csv_path = os.path.join(SCRATCH_DIR, source_file)
        print(f"Downloading s3://{BUCKET}/{landing_key} -> {csv_path}")
        s3.download_file(BUCKET, landing_key, csv_path)
        print(f"  downloaded {os.path.getsize(csv_path):,} bytes")

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            con.execute("SET memory_limit='12GB';")
            con.execute("SET temp_directory='/tmp/duckdb_spill';")
            # 100% transform in SQL; zero-copy export to Arrow (to_arrow_table — NEVER
            # fetch_arrow_table). Params bound positionally: [path, source_file, as_of].
            table = con.execute(
                _SQL_BUILDERS[target](), [csv_path, source_file, as_of]
            ).to_arrow_table()
            rows = table.num_rows
            try:
                rj = con.execute("SELECT count(*) AS n FROM reject_errors").fetchone()
                rejected = int(rj[0]) if rj else 0
            except Exception:  # noqa: BLE001 — table absent ⇒ zero rejects
                rejected = 0
        finally:
            con.close()
        print(f"{target}: parsed {rows:,} rows, {rejected:,} rejected")

        # DIRECT R2 write (storage_options) — small datasets + modest string indexes are
        # well below R2's multipart escalation threshold; no local staging needed.
        lance.write_dataset(
            table,
            dataset_uri,
            mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        del table
        print(f"{target}: wrote Lance dataset -> {dataset_uri}")
        built = _build_indexes(target, so)
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("ingest", target, dataset_uri, as_of, source_file, landing_key,
                    int(rows), int(rejected), status, error, started_at, completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "phase": "ingest", "target": target,
                        "rows": int(rows), "rejected_rows": int(rejected),
                        "dataset_uri": dataset_uri, "as_of": as_of})

    if status != "success":
        raise RuntimeError(f"ca_cslb ingest failed for target={target}: {error}")
    return {"status": status, "phase": "ingest", "target": target,
            "rows_processed": int(rows), "rejected_rows": int(rejected),
            "indices": built, "dataset_uri": dataset_uri, "as_of": as_of}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")],
              timeout=60 * 30, memory=16384, cpu=8.0)
def reindex_target(target: str) -> dict:
    """(Re)build the scalar indexes on an already-written dataset (no re-ingest)."""
    target = target.strip().lower()
    if target not in SOURCES:
        raise ValueError(f"target must be one of {sorted(SOURCES)}, got {target!r}")
    built = _build_indexes(target, _r2_storage_options())
    return {"target": target, "dataset_uri": SOURCES[target]["uri"],
            "indexes": built, "index_count": len(built)}


@app.local_entrypoint()
def init_state() -> None:
    """Create ops.cslb_runs (idempotent)."""
    import json

    print(json.dumps(apply_state_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def ingest(target: str, as_of: str = AS_OF_DEFAULT) -> None:
    """Phase 2 — ingest a single target (licenses|personnel|workers_comp)."""
    import json

    print(json.dumps(ingest_target.remote(target, as_of=as_of, trigger_callback_url=None),
                     indent=2, default=str))


@app.local_entrypoint()
def run_all(as_of: str = AS_OF_DEFAULT) -> None:
    """End-to-end manual run: ingest all three targets in PARALLEL (distinct datasets →
    no shared-writer conflict). Prints final row counts."""
    import json

    print("=== parallel ingest: licenses ‖ personnel ‖ workers_comp ===")
    calls = {t: ingest_target.spawn(t, as_of=as_of, trigger_callback_url=None) for t in SOURCES}
    results: dict[str, dict] = {}
    for t, call in calls.items():
        results[t] = call.get()
        print(json.dumps(results[t], default=str))

    print("\n=== FINAL ROW COUNTS ===")
    total = 0
    for t, r in results.items():
        n = r.get("rows_processed", 0)
        total += n
        print(f"  {t:14s} rows={n:>10,}  rejected={r.get('rejected_rows'):>4,}  -> {r.get('dataset_uri')}")
    print(f"  {'TOTAL':14s} rows={total:>10,}")


@app.local_entrypoint()
def reindex(target: str = "") -> None:
    """Rebuild scalar indexes on existing dataset(s) (no re-ingest). Default: all."""
    import json

    for t in ([target] if target else list(SOURCES)):
        print(json.dumps(reindex_target.remote(t), default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 10) -> None:
    """Print the most recent ops.cslb_runs rows."""
    import json

    print(json.dumps(ledger.remote(limit), indent=2, default=str))


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 10) -> list:
    """Read the most recent ops.cslb_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, phase, target, dataset_uri, as_of, source_file, "
            "rows_processed, rejected_rows, status, error, started_at, completed_at "
            "FROM ops.cslb_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
