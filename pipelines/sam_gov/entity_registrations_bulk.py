"""Compute worker — SAM.gov Entity Registrations historical backfill.

The ``sam-gov-entity-pipelines`` Modal app. A BOUNDED backfill, not a daily feed
(no Trigger cron). The durable control plane is
src/trigger/entity_registrations_backfill.ts; this worker has no web endpoint.

Why one orchestrating function (not per-file dispatch): Lance's direct write to
R2 trips R2's multipart rule ("all non-trailing parts must have the same length")
on the large per-extract files. So the dataset is staged on LOCAL disk (Lance
append, no multipart) and uploaded to R2 once via boto3 (s3transfer uses uniform
parts — R2-compliant). Local staging also removes concurrent-append commit races.

Data plane (clean-room — DuckDB does 100% of the transform):
    R2 landing ZIP (data-sink/landing/...)
      → boto3 download    → /tmp/<file>.zip               (Python: I/O only)
      → unzip + transcode → UTF-8 .dat on scratch          (Python: I/O only)
      → DuckDB read_csv   → split / project / cast (100% in SQL)
      → Arrow table       → con.sql(...).to_arrow_table()
      → lance.write_dataset(LOCAL, v2.0, append)           (per file)
    then: BTREE indexes → boto3 upload → s3://data-sink/active/entity_registrations/

Two positional, pipe-delimited layouts (no column-name header) unify into ONE
Arrow schema:
  - legacy_v1 (`SAM_PUBLIC_MONTHLY_*_MODIFIED`, 120 fields): DUNS @0 / CAGE @2
  - v2        (`SAM_PUBLIC_*_MONTHLY_V2_*`,    142 fields): UEI  @0 / CAGE @3
V2 carries `BOF`/`EOF` sentinel lines (filtered). Encoding is content-detected
(strict UTF-8, else lossless cp1252→UTF-8). The cp1252 V2 twins are deduped out.

The corporate URL ("Entity URL") is materialized as a flat, BTREE-indexed
`entity_url` column for the SAM↔PDL identity-resolution spine. Its pipe position
is width-dependent (120-field → f[25], 142-field → f[27]; url_purity >= 0.99)
and extracted row-wise off len(f). It remains retained losslessly in pipe_fields.

    modal run    pipelines/sam_gov/entity_registrations_bulk.py             # full backfill
    modal run    pipelines/sam_gov/entity_registrations_bulk.py --dry-run   # print deduped keys
    modal deploy pipelines/sam_gov/entity_registrations_bulk.py
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
LANDING_PREFIX = "landing/"
DATASET_PREFIX = "active/entity_registrations/"  # R2 key prefix (system-of-record tier)
DATASET_URI = f"s3://{BUCKET}/{DATASET_PREFIX}"
SCRATCH_DIR = "/tmp"
LOCAL_DATASET = "/tmp/entity_lance"
FEED = "sam_entity_registrations"

# Whole-line read delimiter: a control byte that never appears in SAM pipe text,
# so each record lands in one column and we split on '|' ourselves in SQL.
_LINE_DELIM = "\x1f"

# Lance fragment sizing. `90 * 1024**3` (90 GiB) is Lance's documented default —
# the directive's `90 * 10243` was a markdown-mangled `90 * 1024**3`.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.1",
    "lancedb>=0.15",
    "pylance>=0.19",
    "pyarrow>=17",
    "boto3>=1.35",
    "psycopg[binary]>=3.2",
)

# Isolated from the opps app ("sam-gov-pipelines"): separate files sharing an app
# name would clobber each other on `modal deploy`. One app per worker file until a
# shared-app-object module consolidates the sam_gov domain.
app = modal.App("sam-gov-entity-pipelines", image=image)

# 1-indexed positions within the split pipe array, per layout family. Confirmed
# from one sampled record per family; positions past `dba_name` drift between
# layouts and are deferred to reconciliation — every field is retained losslessly
# in `pipe_fields`, so nothing is dropped.
#
# KNOWN DEFECT (surfaced by the SAM↔PDL spine build, 2026-06): keying this map on
# `format_family` is UNSAFE. `_classify()` derives the family from a FILENAME
# token (`_V2_`); many 142-field v2-LAYOUT extracts lack that token and are
# mislabeled `legacy_v1`, so this map misprojects them — the real UEI (f[1])
# lands in `duns`, a date (f[11]) lands in `legal_business_name`, and `uei` is
# forced NULL. The reliable layout key is the record WIDTH (len(f)): 120 = legacy
# layout, 142 = v2 layout. `entity_url` below is already extracted width-aware for
# this reason. FIX (recommended before production lock): drive uei/duns/cage/
# status/dates/name off len(f) too (the verified width→position map lives in
# pipelines/resolution/spine_sql.py:SAM_FIELD_POS), then re-backfill. Until then
# the flat uei/legal_business_name columns are unreliable for the mislabeled rows;
# the spine derives them width-aware and does not depend on them.
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
    """boto3 S3 client for R2. Forces checksum behaviour to ``when_required``;
    botocore's default flexible-checksum validation otherwise raises
    FlexibleChecksumError against R2 on download."""
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
    import re

    name = key.rsplit("/", 1)[-1].upper()
    if "_V2_" in name:
        m = re.search(r"_V2_(\d{8})", name)
        return "v2", (m.group(1) if m else name)
    m = re.search(r"MONTHLY_(\d{4}_[A-Z]{3})_MODIFIED", name)
    return "legacy_v1", (m.group(1) if m else name)


def _materialize_utf8(zf, member_name: str, out_path: str) -> str:
    """Stream the data member to a UTF-8 file; return detected encoding. Pass 1
    probes strict UTF-8; pass 2 writes through (UTF-8) or transcodes (cp1252,
    single-byte so 8 MiB chunk boundaries never split a character)."""
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
        if i is None:
            return f"CAST(NULL AS VARCHAR) AS {c}"
        if c == "duns":
            # GSA redacted DUNS in the historical extracts to the literal
            # 'No longer available'; surface that sentinel as NULL.
            return f"nullif(nullif(trim(f[{i}]), ''), 'No longer available') AS {c}"
        return f"nullif(trim(f[{i}]), '') AS {c}"

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
    # Corporate URL ("Entity URL") — first-class flat column powering the
    # SAM<->PDL identity-resolution spine. Its position in the pipe array is
    # width-dependent (structural probe, url_purity >= 0.99): 120-field rows
    # -> f[25], 142-field rows -> f[27]. Extracted row-wise off len(f) so both
    # legacy widths and v2 resolve in one pass; BTREE-indexed below as a
    # resolution key. Still retained losslessly inside pipe_fields.
    entity_url = (
        "nullif(trim(CASE WHEN len(f) = 120 THEN f[25] "
        "WHEN len(f) = 142 THEN f[27] END), '') AS entity_url"
    )
    return f"""
WITH raw AS (
    SELECT rtrim(col0, chr(13)) AS line
    FROM read_csv('{lit(scratch_path)}', auto_detect=false, header=false,
                  delim='{_LINE_DELIM}', quote='', escape='', new_line='\\n',
                  strict_mode=false, columns={{'col0': 'VARCHAR'}}, ignore_errors=true)
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
    {entity_url},
    f AS pipe_fields,
    len(f) AS field_count,
    '{family}' AS format_family,
    '{lit(enc)}' AS source_encoding,
    '{lit(label)}' AS extract_label,
    '{lit(key)}' AS source_file,
    now() AS ingested_at
FROM p
"""


def _list_landing_zip_keys() -> list[str]:
    s3 = _s3_client()
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=LANDING_PREFIX):
        for o in page.get("Contents", []):
            if o["Key"].lower().endswith(".zip"):
                keys.append(o["Key"])
    return keys


def _dedup_keys(keys: list[str]) -> list[str]:
    """Drop cp1252 V2 encoding-twins when a native UTF-8 sibling for the same date
    exists; keep all historical files (no twins)."""
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
                continue
        kept.append(k)
    return sorted(kept)


def _replace_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Idempotent publish: wipe the R2 prefix, then upload the local Lance dataset
    (boto3 = uniform-part multipart, R2-compliant). Returns files uploaded."""
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


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=120)
def select_backfill_keys() -> list[str]:
    return _dedup_keys(_list_landing_zip_keys())


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=2 * 60 * 60,
    memory=16384,
    cpu=4.0,
    ephemeral_disk=524288,  # Modal's explicit floor (512 GiB); staging needs ~12 GiB
)
def run_backfill(trigger_callback_url: str | None = None, only: str = "") -> dict:
    """Stage every deduped landing extract into one LOCAL Lance dataset (append),
    BTREE-index the resolution keys, then publish to R2 via boto3. Each file's
    terminal state is recorded to ops.*; the run re-raises on the first failure."""
    import datetime as dt
    import shutil
    import zipfile

    import duckdb
    import lance

    keys = _dedup_keys(_list_landing_zip_keys())
    if only:
        keys = [k for k in keys if only in k]

    shutil.rmtree(LOCAL_DATASET, ignore_errors=True)
    s3 = _s3_client()
    per_file: list[dict] = []
    total_rows = 0
    final_status = "error"
    error_text: str | None = None

    try:
        for i, key in enumerate(keys):
            f_started = dt.datetime.now(dt.timezone.utc)
            family, label = _classify(key)
            f_rows, f_enc, f_status, f_err = 0, None, "error", None
            zip_path = os.path.join(SCRATCH_DIR, key.rsplit("/", 1)[-1])
            scratch = os.path.join(SCRATCH_DIR, "extract.utf8.dat")
            try:
                s3.download_file(BUCKET, key, zip_path)
                with zipfile.ZipFile(zip_path) as zf:
                    members = [m for m in zf.infolist() if not m.is_dir()
                               and m.filename.lower().endswith(".dat")] \
                        or [m for m in zf.infolist() if not m.is_dir()]
                    data_member = max(members, key=lambda m: m.file_size)
                    f_enc = _materialize_utf8(zf, data_member.filename, scratch)

                con = duckdb.connect(":memory:")
                try:
                    con.execute("PRAGMA threads=4;")
                    table = con.sql(_build_sql(family, scratch, f_enc, label, key)).to_arrow_table()
                finally:
                    con.close()
                f_rows = table.num_rows

                lance.write_dataset(
                    table, LOCAL_DATASET,
                    mode="create" if i == 0 else "append",
                    data_storage_version="2.0",
                    max_rows_per_file=MAX_ROWS_PER_FILE,
                    max_bytes_per_file=MAX_BYTES_PER_FILE,
                )
                f_status = "success"
                total_rows += f_rows
            except Exception as exc:  # noqa: BLE001
                f_err = str(exc)
            finally:
                _record_run(key, label, family, f_enc, int(f_rows), f_status, f_err,
                            f_started, dt.datetime.now(dt.timezone.utc))
                for p in (zip_path, scratch):
                    try:
                        os.remove(p)
                    except OSError:
                        pass

            per_file.append({"source_file": key, "extract_label": label, "format_family": family,
                             "source_encoding": f_enc, "rows": int(f_rows), "status": f_status})
            if f_status != "success":
                raise RuntimeError(f"ingest failed for {key}: {f_err}")
            print(f"[{i + 1}/{len(keys)}] {label} {family} enc={f_enc} rows={f_rows}")

        # BTREE scalar indexes on the resolution keys (best-effort per index).
        ds = lance.dataset(LOCAL_DATASET)
        for col in ("uei", "cage_code", "extract_label", "entity_url"):
            try:
                ds.create_scalar_index(col, index_type="BTREE")
            except Exception as exc:  # noqa: BLE001
                print(f"WARN: BTREE index on {col} failed: {exc}")

        uploaded = _replace_r2_prefix(s3, DATASET_PREFIX, LOCAL_DATASET)
        print(f"Published {uploaded} files to {DATASET_URI}")
        final_status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc)
    finally:
        _post_callback(
            trigger_callback_url,
            {"status": final_status, "rows": int(total_rows), "feed": FEED,
             "files": sum(1 for p in per_file if p["status"] == "success")},
        )

    if final_status != "success":
        raise RuntimeError(f"entity backfill failed: {error_text}")
    return {"feed": FEED, "files": len(keys), "rows": int(total_rows),
            "dataset": DATASET_URI, "per_file": per_file, "status": final_status}


@app.local_entrypoint()
def backfill(only: str = "", dry_run: bool = False) -> None:
    if dry_run:
        keys = select_backfill_keys.remote()
        if only:
            keys = [k for k in keys if only in k]
        print(f"Selected {len(keys)} files after dedup:")
        for k in keys:
            print("  ", k)
        return
    print(run_backfill.remote(trigger_callback_url=None, only=only))
