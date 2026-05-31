"""Compute worker — SAM.gov Entity Registrations historical backfill.

Part of the ``sam-gov-pipelines`` Modal app. Spawned by the Universal Dispatcher
(core/modal_dispatcher.py) one invocation per landing ZIP, or driven directly by
the ``backfill`` local entrypoint. This is a BOUNDED backfill, not a daily feed —
there is no Trigger cron. The durable control plane lives in
src/trigger/entity_registrations_backfill.ts; this worker has no web endpoint.

Data plane (clean-room — DuckDB does 100% of the transform):
    R2 landing ZIP (data-sink/landing/...)
      → boto3 download    → /tmp/<file>.zip               (Python: I/O only)
      → unzip + transcode → UTF-8 .dat on /tmp scratch     (Python: I/O only)
      → DuckDB read_csv   → split / project / cast (100% in SQL)
      → Arrow table       → con.sql(...).to_arrow_table()
      → lance.write_dataset(s3://data-sink/active/entity_registrations/, v2.0, append)

Two positional, pipe-delimited layouts (no column-name header) are unified into
ONE Arrow schema:
  - legacy_v1 (`SAM_PUBLIC_MONTHLY_*_MODIFIED`, 120 fields): DUNS @0 / CAGE @2
  - v2        (`SAM_PUBLIC_*_MONTHLY_V2_*`,    142 fields): UEI  @0 / CAGE @3
V2 files carry `BOF PUBLIC V2 …` / `EOF …` sentinel lines, which are filtered out.

Encoding is content-detected per file: strict UTF-8, else a lossless
Windows-1252 → UTF-8 transcode (cp1252 is single-byte, so 8 MiB chunk boundaries
never split a character and no rows are dropped). The cp1252 V2 encoding-twins
are dropped upstream (dedup) so only native UTF-8 V2 files are ingested.

    modal run    pipelines/sam_gov/entity_registrations_bulk.py             # full backfill (sequential)
    modal run    pipelines/sam_gov/entity_registrations_bulk.py --dry-run   # print the deduped key list
    modal deploy pipelines/sam_gov/entity_registrations_bulk.py
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
LANDING_PREFIX = "landing/"
# Lance system-of-record tier (NOT the landing/raw zone). Exact path per directive.
DATASET_URI = os.environ.get("ENTITY_REG_LANCE_URI", "s3://data-sink/active/entity_registrations/")
SCRATCH_DIR = "/tmp"
FEED = "sam_entity_registrations"

# Whole-line read delimiter: a control byte that never appears in SAM pipe text,
# so each record lands in one column and we split on '|' ourselves in SQL.
_LINE_DELIM = "\x1f"

# Lance fragment sizing.
# NOTE: the directive wrote `max_bytes_per_file=90 * 10243`. That is read here as
# `90 * 1024**3` (90 GiB) — Lance's documented default. A literal `90 * 10243`
# (~900 KB) would shatter each ~500 MB extract into ~600 fragments; flip the
# constant below if 900 KB was genuinely intended.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.1",
    "lancedb>=0.15",
    "pylance>=0.19",        # provides `import lance`
    "pyarrow>=17",
    "boto3>=1.35",          # R2 landing-zip download
    "psycopg[binary]>=3.2",  # ops.* terminal state
)

app = modal.App("sam-gov-pipelines", image=image)

# 1-indexed positions within the split pipe array, per layout family. Confirmed
# from one sampled record per family; positions beyond `dba_name` drift between
# layouts and are deferred to tomorrow's reconciliation — every field is retained
# losslessly in `pipe_fields`, so nothing is dropped.
FIELD_MAP = {
    "legacy_v1": {
        "uei": None, "duns": 1, "cage_code": 3, "registration_status": 5,
        "purpose_of_registration": 6, "registration_date": 7, "expiration_date": 8,
        "last_update_date": 9, "activation_date": 10, "legal_business_name": 11, "dba_name": 12,
    },
    "v2": {
        "uei": 1, "duns": 3, "cage_code": 4, "registration_status": 6,
        "purpose_of_registration": 7, "registration_date": 8, "expiration_date": 9,
        "last_update_date": 10, "activation_date": 11, "legal_business_name": 12, "dba_name": 13,
    },
}
EXPECTED_FIELDS = {"legacy_v1": 120, "v2": 142}

_STR_COLS = ["uei", "duns", "cage_code", "registration_status",
             "purpose_of_registration", "legal_business_name", "dba_name"]
_DATE_COLS = ["registration_date", "expiration_date", "last_update_date", "activation_date"]


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
    """boto3 S3 client for R2. Forces checksum behaviour to ``when_required``:
    botocore's default flexible-checksum validation does not match R2's semantics
    and otherwise raises FlexibleChecksumError on download_file."""
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


def _classify(key: str) -> tuple[str, str]:
    """Return (format_family, extract_label) from the object key."""
    import re

    name = key.rsplit("/", 1)[-1].upper()
    if "_V2_" in name:
        m = re.search(r"_V2_(\d{8})", name)
        return "v2", (m.group(1) if m else name)
    m = re.search(r"MONTHLY_(\d{4}_[A-Z]{3})_MODIFIED", name)
    return "legacy_v1", (m.group(1) if m else name)


def _materialize_utf8(zf, member_name: str, out_path: str) -> str:
    """Stream the data member to a UTF-8 file on scratch; return the detected
    encoding. Pass 1 probes strict UTF-8; pass 2 writes through (UTF-8) or
    transcodes (cp1252, single-byte so chunk-safe)."""
    import codecs

    chunk_size = 8 << 20
    dec = codecs.getincrementaldecoder("utf-8")()
    enc = "utf-8"
    with zf.open(member_name) as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                try:
                    dec.decode(b"", final=True)
                except UnicodeDecodeError:
                    enc = "cp1252"
                break
            try:
                dec.decode(chunk)
            except UnicodeDecodeError:
                enc = "cp1252"
                break

    with zf.open(member_name) as fh, open(out_path, "wb") as out:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            out.write(chunk if enc == "utf-8" else chunk.decode("cp1252").encode("utf-8"))
    return enc


def _build_sql(family: str, scratch_path: str, enc: str, label: str, key: str) -> str:
    m = FIELD_MAP[family]

    def lit(s: str) -> str:
        return s.replace("'", "''")

    def strcol(c: str) -> str:
        i = m[c]
        return f"CAST(NULL AS VARCHAR) AS {c}" if i is None else f"nullif(trim(f[{i}]), '') AS {c}"

    def datecol(c: str) -> str:
        i = m[c]
        if i is None:
            return f"CAST(NULL AS DATE) AS {c}"
        return f"TRY_CAST(TRY_STRPTIME(nullif(trim(f[{i}]), ''), '%Y%m%d') AS DATE) AS {c}"

    projections = ",\n    ".join(
        [strcol("uei"), strcol("duns"), strcol("cage_code"), strcol("registration_status"),
         strcol("purpose_of_registration"), datecol("registration_date"), datecol("expiration_date"),
         datecol("last_update_date"), datecol("activation_date"), strcol("legal_business_name"),
         strcol("dba_name")]
    )
    return f"""
WITH raw AS (
    SELECT rtrim(col0, chr(13)) AS line
    FROM read_csv('{lit(scratch_path)}', auto_detect=false, header=false,
                  delim='{_LINE_DELIM}', quote='', escape='', new_line='\\n',
                  columns={{'col0': 'VARCHAR'}}, ignore_errors=true)
    WHERE col0 IS NOT NULL
),
p AS (
    SELECT string_split(line, '|') AS f
    FROM raw
    WHERE length(trim(line)) > 0
      AND line NOT LIKE 'BOF %'
      AND line NOT LIKE 'EOF %'
)
SELECT
    {projections},
    f AS pipe_fields,
    len(f) AS field_count,
    '{family}' AS format_family,
    '{lit(enc)}' AS source_encoding,
    '{lit(label)}' AS extract_label,
    '{lit(key)}' AS source_file,
    now() AS ingested_at
FROM p
"""


def _append_idempotent(table, key: str, so: dict) -> None:
    """Append to the Lance dataset, replacing any prior rows for this source_file
    so re-runs are idempotent. Creates the dataset on first write. Run serially —
    concurrent writers to one dataset can hit Lance commit conflicts."""
    import lance

    try:
        ds = lance.dataset(DATASET_URI, storage_options=so)
    except Exception:
        ds = None

    common = dict(data_storage_version="2.0", max_rows_per_file=MAX_ROWS_PER_FILE,
                  max_bytes_per_file=MAX_BYTES_PER_FILE, storage_options=so)
    if ds is None:
        lance.write_dataset(table, DATASET_URI, mode="create", **common)
        return
    try:
        ds.delete(f"source_file = '{key.replace(chr(39), chr(39) * 2)}'")
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: pre-append delete failed (continuing): {exc}")
    lance.write_dataset(table, DATASET_URI, mode="append", **common)


def _record_run(source_file, label, family, enc, rows, status, error, started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.sam_entity_registration_runs
                    (source_file, extract_label, format_family, source_encoding,
                     rows_processed, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (source_file, label, family, enc, rows, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}")


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


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def ingest_entity_registration_extract(key: str, trigger_callback_url: str | None = None) -> dict:
    """Download one landing ZIP → transcode → DuckDB project/cast → Lance append,
    then record ops.* state and wake the Trigger run. Re-raises on failure so the
    Modal call is marked failed."""
    import datetime as dt
    import os.path
    import zipfile

    import duckdb

    started_at = dt.datetime.now(dt.timezone.utc)
    family, label = _classify(key)
    rows = 0
    enc: str | None = None
    status = "error"
    error: str | None = None

    try:
        s3 = _s3_client()
        zip_path = os.path.join(SCRATCH_DIR, key.rsplit("/", 1)[-1])
        s3.download_file(BUCKET, key, zip_path)

        with zipfile.ZipFile(zip_path) as zf:
            members = [i for i in zf.infolist() if not i.is_dir() and i.filename.lower().endswith(".dat")]
            if not members:
                members = [i for i in zf.infolist() if not i.is_dir()]
            data_member = max(members, key=lambda i: i.file_size)
            scratch = os.path.join(SCRATCH_DIR, "extract.utf8.dat")
            enc = _materialize_utf8(zf, data_member.filename, scratch)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            table = con.sql(_build_sql(family, scratch, enc, label, key)).to_arrow_table()
        finally:
            con.close()
        rows = table.num_rows

        _append_idempotent(table, key, _r2_storage_options())
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(key, label, family, enc, int(rows), status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": FEED, "source_file": key},
        )

    if status != "success":
        raise RuntimeError(f"entity_registration ingest failed for {key}: {error}")
    return {"feed": FEED, "source_file": key, "rows_processed": int(rows),
            "format_family": family, "source_encoding": enc, "status": status}


def _dedup_keys(keys: list[str]) -> list[str]:
    """Drop the cp1252 V2 encoding-twins whenever a native UTF-8 sibling for the
    same date exists; keep all historical files (they have no twins)."""
    import re

    utf8_dates = set()
    for k in keys:
        m = re.search(r"UTF-8_MONTHLY_V2_(\d{8})", k.rsplit("/", 1)[-1].upper())
        if m:
            utf8_dates.add(m.group(1))

    kept = []
    for k in keys:
        name = k.rsplit("/", 1)[-1].upper()
        if "_V2_" in name and "UTF-8" not in name:
            d = re.search(r"_V2_(\d{8})", name)
            if d and d.group(1) in utf8_dates:
                continue  # drop cp1252 twin
        kept.append(k)
    return sorted(kept)


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=120)
def select_backfill_keys() -> list[str]:
    """List data-sink/landing/ ZIPs (case-insensitive) and apply dedup."""
    s3 = _s3_client()
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=LANDING_PREFIX):
        for o in page.get("Contents", []):
            if o["Key"].lower().endswith(".zip"):
                keys.append(o["Key"])
    return _dedup_keys(keys)


@app.local_entrypoint()
def backfill(only: str = "", dry_run: bool = False) -> None:
    """Sequential backfill (sequential avoids Lance commit conflicts on one
    dataset). ``--only SUBSTR`` filters keys; ``--dry-run`` just prints them."""
    keys = select_backfill_keys.remote()
    if only:
        keys = [k for k in keys if only in k]
    print(f"Selected {len(keys)} files after dedup:")
    for k in keys:
        print("  ", k)
    if dry_run:
        return
    for k in keys:
        print(f"\n=== {k} ===")
        print(ingest_entity_registration_extract.remote(k, trigger_callback_url=None))
