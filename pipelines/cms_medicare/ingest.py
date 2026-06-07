"""Compute worker — CMS Medicare full-archive ingest (landing → LanceDB SoR).

Modal app ``cms-medicare-pipelines``. Lands the CMS Medicare archive from the raw landing
tier (``s3://data-sink/landing/cms/medicare-datasets/``, 35 zips / ~85 GB logical) into the
Gen-3 LanceDB system of record (``s3://data-sink/active/``), one Lance dataset per grain,
physically clustered for the longitudinal per-provider queries a PE acquirer asks, united
ONLY on the published ``npi`` and CMS ``ENRLMT_ID`` keys.

Directive: docs/plans/medicare_ingestion_plan.md. Companion recon: docs/medicare_archive_diagnostic.md.
Every mechanic is lifted from an in-repo pattern, cited inline.

PROVENANCE / REUSE
  * Extraction  — EPA random-access ZIP member reader (pipelines/ingest_epa/materialize_epa.py
    `_S3RangeReader` / `_member_to_gz`): a stored deflate member IS a gzip body, copied between a
    10-byte gzip header and an 8-byte CRC32+ISIZE trailer. /tmp holds only the COMPRESSED member,
    never the inflated 3-4 GB CSV (the form5500 BytesIO whole-zip-into-RAM anti-pattern would OOM).
  * Transform/publish/verify — Open Payments (pipelines/cms_open_payments/ingest.py): DuckDB
    read_csv(all_varchar, parallel=false, null_padding, store_rejects); `_publish_full_swap`
    (stage→verify→swap→verify); `_verify_published` (reopen fresh from R2, assert rows+indices+key probe).
  * Clustering — NPPES analytical sorted write (pipelines/nppes/materialize_analytical.py:439):
    the final dataset is written `ORDER BY <cluster_key>`, so a per-NPI scan prunes to contiguous
    fragments instead of all 13 year-fragments (the §5.1 "clustering dividend").

INDEPENDENT-RECON CORRECTIONS to the plan (verified against landing zips' central dirs + headers):
  * QPP is NOT strictly additive — the 92-col era (2017-2021) carries 25 columns absent from the
    212-col 2024 superset (additive+RENAME). A max-year superset would DROP 5 years of those cols.
    → the reconciler builds the superset from the UNION of every year's header (QPP union = 237),
      NULL-filling each direction. Lossless. A1/C1/C2 are strictly additive (union == max-year), proven.
  * 2013 Part-D-by-drug member is byte-identical (same size AND CRC32 0x17b122e0) across both zips
    → dedup by (dataset, year), preferring the non-"-2" copy; the 2021-23 "-2" zips are the ONLY copy.

DEVIATIONS from the plan (documented):
  * Money uniformly DECIMAL(18,2) (plan: 18,2 totals / 14,2 averages). (18,2) is a strict superset
    that holds both; per-column-era width tracking is needless complexity. Width gate (§9 #2) proves
    no overflow nulls regardless.
  * Year is taken from the FOLDER segment for ALL series (present in every member path), not the
    bundle/annual split — strictly more robust; the filename DY/D token is a cross-check only.
  * Dates kept VARCHAR (lossless) in v1 — no format-guess casts; date-aware filters are downstream.

    modal run pipelines/cms_medicare/ingest.py::init_state
    modal run pipelines/cms_medicare/ingest.py::discover
    modal run pipelines/cms_medicare/ingest.py::ingest --dataset cms_physician_provider
    modal run pipelines/cms_medicare/ingest.py::ingest_giant --dataset cms_partd_provider_drug   # B2/A2 (128 GiB)
    modal run pipelines/cms_medicare/ingest.py::verify  --dataset cms_physician_provider
    modal run pipelines/cms_medicare/ingest.py::show_ledger
"""

from __future__ import annotations

import os
import re as _re

import modal

# ─────────────────────────────────────────── constants ───────────────────────────────────────────
BUCKET = "data-sink"
LANDING_PREFIX = "landing/cms/medicare-datasets/"
ACTIVE_PREFIX = "active/"
FEED = "cms_medicare"

SCRATCH_DIR = "/tmp/cms_medicare"
STAGE_DIR = os.path.join(SCRATCH_DIR, "stage")     # one compressed .csv.gz member at a time
SPILL_DIR = os.path.join(SCRATCH_DIR, "spill")     # DuckDB temp_directory (local NVMe)
LOCAL_DIR = os.path.join(SCRATCH_DIR, "lance")     # local Lance staging (pre-publish)

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
READ_BATCH_ROWS = 1 << 20
DUCKDB_THREADS = 8
DUCKDB_MEMORY_LIMIT = os.environ.get("CMS_MED_MEM_LIMIT", "24GB")

_R2_PUBLISH_ATTEMPTS = 4
_R2_RETRY_BACKOFF_S = 8
_R2_SINGLE_COPY_MAX = 5 * 1024**3

# ─────────────────────────────────────────── dataset catalog ───────────────────────────────────────────
# archive_substr: exact-enough substring of the zip basename · member_re: matches the data CSV member
# (CMS file tokens are stable: Prov / Prov_Svc / Geo / NPI / NPIBN / rfrr / supr / suphpr / geor / PPEF_*).
# partition: 'year' (folder = YYYY) | 'snapshot' (folder = YYYY-Qn). giant: npi BTREE via staged path.
def _m(token):  # member basename regex helper
    return token

DATASETS: dict[str, dict] = {
    # ── A · Physician & Other Practitioners ──
    "cms_physician_provider": {  # A1 ⭐ primary spine — NPI×year totals, 2013-2024 (56→81 additive)
        "archive_substr": "Physician & Other Practitioners - by Provider.zip",
        "member_re": r"_Prov\.csv$", "partition": "year",
        "npi_col": "rndrng_npi",
        "btree": ["rndrng_npi"],
        "bitmap": ["program_year", "rndrng_prvdr_type", "rndrng_prvdr_state_abrvtn", "rndrng_prvdr_ent_cd"],
        "cluster": "rndrng_npi, program_year", "giant": False,
    },
    "cms_physician_provider_service": {  # A2 — NPI×HCPCS×POS, 2017-2024
        "archive_substr": "Physician & Other Practitioners - by Provider and Service",
        "member_re": r"_Prov_Svc\.csv$", "partition": "year",
        "npi_col": "rndrng_npi",
        "btree": ["rndrng_npi", "hcpcs_cd"],
        "bitmap": ["program_year", "place_of_srvc", "rndrng_prvdr_state_abrvtn", "rndrng_prvdr_type"],
        "cluster": "rndrng_npi, hcpcs_cd, program_year", "giant": True,
    },
    "cms_physician_geography_service": {  # A3 — geo×HCPCS×POS, 2013-2024 (NPI-orthogonal)
        "archive_substr": "Physician & Other Practitioners - by Geography and Service",
        "member_re": r"_Geo\.csv$", "partition": "year",
        "npi_col": None,
        "btree": ["hcpcs_cd"],
        "bitmap": ["program_year", "rndrng_prvdr_geo_lvl", "place_of_srvc"],
        "cluster": "hcpcs_cd, program_year", "giant": False,
    },
    # ── B · Part D Prescribers ──
    "cms_partd_provider": {  # B1 — NPI×year totals, 2013-2020
        "archive_substr": "Part D Prescribers - by Provider.zip",
        "member_re": r"_NPI\.csv$", "partition": "year",
        "npi_col": "prscrbr_npi",
        "btree": ["prscrbr_npi"],
        "bitmap": ["program_year", "prscrbr_type", "prscrbr_state_abrvtn"],
        "cluster": "prscrbr_npi, program_year", "giant": False,
    },
    "cms_partd_provider_drug": {  # B2 — NPI×drug×year, 2013-2024 (the giant, ~44 GB / >100M rows)
        "archive_substr": "Part D Prescribers - by Provider and Drug",
        "member_re": r"_NPIBN\.csv$", "partition": "year",
        "npi_col": "prscrbr_npi",
        "btree": ["prscrbr_npi"],
        "bitmap": ["program_year", "prscrbr_type", "prscrbr_state_abrvtn"],
        "cluster": "prscrbr_npi, program_year", "giant": True,
    },
    "cms_partd_geography_drug": {  # B3 — geo×drug×year, 2014-2024
        "archive_substr": "Part D Prescribers - by Geography and Drug",
        "member_re": r"_Geo(_0)?\.csv$", "partition": "year",
        "npi_col": None,
        "btree": [],
        "bitmap": ["program_year", "prscrbr_geo_lvl"],
        "cluster": None, "giant": False,
    },
    # ── C · Durable Medical Equipment ──
    "cms_dme_referring_provider": {  # C1 — NPI×year, 2014-2023 (72→97 additive)
        "archive_substr": "Devices & Supplies - by Referring Provider",
        "member_re": r"_rfrr\.csv$", "partition": "year",
        "npi_col": "rfrg_npi",
        "btree": ["rfrg_npi"],
        "bitmap": ["program_year", "rfrg_prvdr_spclty_desc", "rfrg_prvdr_state_abrvtn"],
        "cluster": "rfrg_npi, program_year", "giant": False,
    },
    "cms_dme_supplier": {  # C2 — NPI×year, 2014-2023 (68→93 additive)
        "archive_substr": "Devices & Supplies - by Supplier.zip",
        "member_re": r"_supr\.csv$", "partition": "year",
        "npi_col": "suplr_npi",
        "btree": ["suplr_npi"],
        "bitmap": ["program_year", "suplr_prvdr_spclty_desc", "suplr_prvdr_state_abrvtn"],
        "cluster": "suplr_npi, program_year", "giant": False,
    },
    "cms_dme_supplier_service": {  # C3 — NPI×HCPCS×rental-ind×year, 2014-2023 (grain proof found
        "archive_substr": "Devices & Supplies - by Supplier and Service",  # the rental axis empirically)
        "member_re": r"_suphpr\.csv$", "partition": "year",
        "npi_col": "suplr_npi",
        "btree": ["suplr_npi", "hcpcs_cd"],
        "bitmap": ["program_year", "suplr_prvdr_state_abrvtn", "suplr_rentl_ind"],
        "cluster": "suplr_npi, hcpcs_cd, suplr_rentl_ind, program_year", "giant": False,
    },
    "cms_dme_geography_service": {  # C4 — geo×HCPCS×year, 2017-2023
        "archive_substr": "Devices & Supplies - by Geography and Service",
        "member_re": r"_geor\.csv$", "partition": "year",
        "npi_col": None,
        "btree": ["hcpcs_cd"],
        "bitmap": ["program_year", "rfrg_prvdr_geo_lvl"],
        "cluster": "hcpcs_cd, program_year", "giant": False,
    },
    # ── R · references / enrollment ──
    "cms_qpp_experience": {  # QPP — NPI×year, 2017-2024 (additive+RENAME → union=237)
        "archive_substr": "Quality Payment Program Experience",
        "member_re": r"\.csv$", "partition": "year",
        "npi_col": "npi",
        "btree": ["npi"],
        "bitmap": ["program_year", "practice_state_or_us_territory", "clinician_specialty", "participation_type"],
        "cluster": "npi, program_year", "giant": False,
    },
    "cms_provider_enrollment": {  # R1·Enrollment — 1/enrlmt_id, snapshot 2026-Q1
        "archive_substr": "Public Provider Enrollment",
        "member_re": r"PPEF_Enrollment_Extract", "partition": "snapshot",
        "npi_col": "npi",
        "btree": ["npi", "enrlmt_id", "pecos_asct_cntl_id"],
        "bitmap": ["provider_type_desc", "state_cd"],
        "cluster": "npi", "giant": False,
    },
    "cms_provider_enrollment_reassignment": {  # R1·Reassignment — the org-affiliation graph edge
        "archive_substr": "Public Provider Enrollment",
        "member_re": r"PPEF_Reassignment_Extract", "partition": "snapshot",
        "npi_col": None,
        "btree": ["reasgn_bnft_enrlmt_id", "rcv_bnft_enrlmt_id"],
        "bitmap": [],
        "cluster": "reasgn_bnft_enrlmt_id", "giant": False,
    },
    "cms_provider_enrollment_practice": {  # R1·Practice_Location
        "archive_substr": "Public Provider Enrollment",
        "member_re": r"PPEF_Practice_Location_Extract", "partition": "snapshot",
        "npi_col": None,
        "btree": ["enrlmt_id"],
        "bitmap": ["state_cd"],
        "cluster": "enrlmt_id", "giant": False,
    },
    "cms_provider_enrollment_specialty": {  # R1·Secondary_Specialty
        "archive_substr": "Public Provider Enrollment",
        "member_re": r"PPEF_Secondary_Specialty_Extract", "partition": "snapshot",
        "npi_col": None,
        "btree": ["enrlmt_id"],
        "bitmap": ["provider_type_desc"],
        "cluster": "enrlmt_id", "giant": False,
    },
    "cms_provider_enrollment_npi": {  # R1·Additional_NPIs
        "archive_substr": "Public Provider Enrollment",
        "member_re": r"PPEF_Additional_NPIs", "partition": "snapshot",
        "npi_col": "npi",
        "btree": ["npi", "enrlmt_id"],
        "bitmap": [],
        "cluster": "npi", "giant": False,
    },
    "ref_rbcs_taxonomy": {  # R3 — HCPCS→RBCS service-line reference
        "archive_substr": "Restructured BETOS Classification System",
        "member_re": r"\.csv$", "partition": "snapshot",
        "npi_col": None,
        "btree": ["hcpcs_cd"],
        "bitmap": ["rbcs_cat", "rbcs_subcat"],
        "cluster": "hcpcs_cd", "giant": False,
    },
}

# §9 #1 grain-proof hubs — the candidate PK proved before the npi BTREE is trusted as a key.
# dups>0 is a HARD STOP. None = skip the hard proof (no npi PK, or multiplicity unverified). The
# service-grain hubs include every grain axis (A2 includes place_of_srvc — review finding). The
# giants (B2/A2) and QPP are proved when their dedicated paths land; geo/snapshot have no npi PK.
GRAIN: dict[str, list] = {
    "cms_physician_provider": ["rndrng_npi", "program_year"],
    "cms_physician_provider_service": ["rndrng_npi", "hcpcs_cd", "place_of_srvc", "program_year"],
    "cms_partd_provider_drug": ["prscrbr_npi", "brnd_name", "gnrc_name", "program_year"],
    "cms_partd_provider": ["prscrbr_npi", "program_year"],
    "cms_dme_referring_provider": ["rfrg_npi", "program_year"],
    "cms_dme_supplier": ["suplr_npi", "program_year"],
    "cms_dme_supplier_service": ["suplr_npi", "hcpcs_cd", "suplr_rentl_ind", "program_year"],
}

# Ledger UPSERT targets (partial-unique-index predicates, must match OPS_DDL).
_INGEST_CONFLICT = "ON CONFLICT (dataset, source_member) WHERE phase = 'ingest'"
_VERIFY_CONFLICT = "ON CONFLICT (dataset) WHERE phase = 'verify'"

# Type allow-list (regex on snake alias). Validated by the §9 width gate; failures hard-stop.
_MONEY_RE = _re.compile(r"(_chrg|_amt|_cst)$")          # → DECIMAL(18,2)
_COUNT_RE = _re.compile(r"(_cnt|_cds|_clms|_benes|_srvcs|_fills|_suply)$")  # → BIGINT
_RATE_RE = _re.compile(r"(_pct|_scre|avg_age|_ratio)$")  # → DOUBLE
# suppression flags (*_sprsn_ind / *_flag) fall through to VARCHAR — preserved, never numericised.

READ_OPTS = ("all_varchar = true, header = true, delim = ',', quote = '\"', escape = '\"', "
             "sample_size = -1, ignore_errors = true, null_padding = true, store_rejects = true, "
             "parallel = false")
DESCRIBE_OPTS = ("all_varchar = true, header = true, delim = ',', quote = '\"', escape = '\"', "
                 "sample_size = -1, ignore_errors = true, null_padding = true, parallel = false")

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.cms_medicare_runs (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  feed text NOT NULL DEFAULT 'cms_medicare',
  phase text NOT NULL,
  dataset text, program_year smallint,
  source_archive text, source_member text, source_object_etag text,
  candidate_key_dups bigint,
  decimal_overflow_nulls bigint,
  rows_processed bigint, rejected_rows bigint,
  status text NOT NULL, error text,
  started_at timestamptz, completed_at timestamptz,
  recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cms_medicare_runs_dataset_idx  ON ops.cms_medicare_runs (dataset);
CREATE INDEX IF NOT EXISTS cms_medicare_runs_phase_idx    ON ops.cms_medicare_runs (phase);
CREATE INDEX IF NOT EXISTS cms_medicare_runs_status_idx   ON ops.cms_medicare_runs (status);
CREATE INDEX IF NOT EXISTS cms_medicare_runs_recorded_idx ON ops.cms_medicare_runs (recorded_at DESC);
-- Idempotency keys (UPSERT targets) so re-runs / Modal retries never duplicate ledger rows:
-- one row per (dataset, member) ingest, one terminal row per dataset verify.
CREATE UNIQUE INDEX IF NOT EXISTS cms_medicare_ingest_uq ON ops.cms_medicare_runs (dataset, source_member) WHERE phase = 'ingest';
CREATE UNIQUE INDEX IF NOT EXISTS cms_medicare_verify_uq ON ops.cms_medicare_runs (dataset)                WHERE phase = 'verify';
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
    "boto3>=1.35", "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})  # in-RAM BTREE sort (lance-format/lance#2650)

app = modal.App("cms-medicare-pipelines", image=image)


# ─────────────────────────────────────────── R2 / object-store ───────────────────────────────────────────
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _s3_client():
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"], aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto",
        config=Config(signature_version="s3v4", request_checksum_calculation="when_required",
                      response_checksum_validation="when_required",
                      retries={"max_attempts": 5, "mode": "standard"}))


# ── ZIP random-access (EPA pattern) ──
class _S3RangeReader:
    """Minimal seekable file-like over an R2 object via Range GETs — enough for zipfile to parse
    the central directory and locate a member (EPA materialize_epa.py:397)."""

    def __init__(self, client, bucket, key):
        self.c, self.b, self.k = client, bucket, key
        self._pos = 0
        self._size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]

    def seekable(self): return True
    def readable(self): return True
    def tell(self): return self._pos

    def seek(self, off, whence=0):
        self._pos = off if whence == 0 else (self._pos + off if whence == 1 else self._size + off)
        return self._pos

    def read(self, n=-1):
        if n is None or n < 0:
            n = self._size - self._pos
        if n <= 0:
            return b""
        end = min(self._pos + n, self._size) - 1
        if end < self._pos:
            return b""
        body = self.c.get_object(Bucket=self.b, Key=self.k, Range=f"bytes={self._pos}-{end}")["Body"].read()
        self._pos += len(body)
        return body


def _member_to_gz(client, archive_key: str, member: str, out_path: str) -> tuple[int, int]:
    """Random-access extract one ZIP member from R2 → valid .csv.gz, copying the stored deflate
    stream between a gzip header and a CRC32+ISIZE trailer (EPA materialize_epa.py:432). /tmp holds
    only the compressed member. Returns (uncompressed_size, compress_size)."""
    import struct
    import zipfile

    reader = _S3RangeReader(client, BUCKET, archive_key)
    zf = zipfile.ZipFile(reader)
    zi = zf.getinfo(member)
    if zi.compress_type != zipfile.ZIP_DEFLATED:
        raise RuntimeError(f"{archive_key}:{member} compress_type={zi.compress_type} (expected deflate)")
    reader.seek(zi.header_offset)
    lh = reader.read(30)
    if lh[:4] != b"PK\x03\x04":
        raise RuntimeError(f"{archive_key}:{member} bad local header signature")
    name_len = struct.unpack("<H", lh[26:28])[0]
    extra_len = struct.unpack("<H", lh[28:30])[0]
    data_off = zi.header_offset + 30 + name_len + extra_len

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as out:
        out.write(b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff")
        remaining, pos, chunk = zi.compress_size, data_off, 16 << 20
        while remaining > 0:
            end = pos + min(chunk, remaining) - 1
            body = client.get_object(Bucket=BUCKET, Key=archive_key, Range=f"bytes={pos}-{end}")["Body"].read()
            if not body:
                raise RuntimeError(f"{archive_key}:{member} short read at {pos}")
            out.write(body)
            pos += len(body)
            remaining -= len(body)
        out.write(struct.pack("<II", zi.CRC & 0xFFFFFFFF, zi.file_size & 0xFFFFFFFF))
    return zi.file_size, zi.compress_size


def _zip_members(client, archive_key: str) -> list:
    """Central-directory enumeration (read-only): [(member, file_size, compress_size, crc32)]."""
    import zipfile

    zf = zipfile.ZipFile(_S3RangeReader(client, BUCKET, archive_key))
    return [(zi.filename, zi.file_size, zi.compress_size, zi.CRC & 0xFFFFFFFF)
            for zi in zf.infolist() if not zi.is_dir()]


def _read_header_cols(client, archive_key: str, member: str) -> list:
    """Inflate only the first 128 KB of a member → parse the CSV header row (utf-8, latin-1 fallback)."""
    import csv
    import io
    import zipfile

    zf = zipfile.ZipFile(_S3RangeReader(client, BUCKET, archive_key))
    with zf.open(member) as fh:
        blob = fh.read(131072)
    try:
        text = blob.decode("utf-8")
    except UnicodeDecodeError:
        text = blob.decode("latin-1")
    return next(csv.reader(io.StringIO(text)))


# ── publish (Open Payments hardened swap) ──
def _staging_prefix(live_prefix: str) -> str:
    return live_prefix.rstrip("/") + "__staging/"


def _upload_rank(rel: str) -> int:
    if rel.startswith("_versions/"): return 4
    if rel.startswith("_transactions/"): return 3
    if rel.startswith("_indices/"): return 2
    return 1


def _ordered_rels(rels) -> list:
    return sorted(rels, key=lambda r: (_upload_rank(r), r))


def _list_prefix_sizes(s3, prefix: str) -> dict:
    out: dict[str, int] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if rel:
                out[rel] = o["Size"]
    return out


def _local_file_sizes(local_dir: str) -> dict:
    out: dict[str, int] = {}
    for root, _dirs, files in os.walk(local_dir):
        for fn in files:
            lp = os.path.join(root, fn)
            out[os.path.relpath(lp, local_dir).replace(os.sep, "/")] = os.path.getsize(lp)
    return out


def _upload_file_retry(s3, local_path: str, key: str) -> None:
    import time

    last = None
    for attempt in range(1, _R2_PUBLISH_ATTEMPTS + 1):
        try:
            s3.upload_file(local_path, BUCKET, key)
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            print(f"    upload {key} attempt {attempt} failed: {str(exc)[:200]}")
            if attempt < _R2_PUBLISH_ATTEMPTS:
                time.sleep(_R2_RETRY_BACKOFF_S * attempt)
    raise RuntimeError(f"upload failed after {_R2_PUBLISH_ATTEMPTS} attempts: {key}: {last}")


def _copy_key_retry(s3, src_key: str, dst_key: str) -> None:
    import time

    last = None
    for attempt in range(1, _R2_PUBLISH_ATTEMPTS + 1):
        try:
            s3.copy_object(Bucket=BUCKET, Key=dst_key, CopySource={"Bucket": BUCKET, "Key": src_key})
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            print(f"    copy {dst_key} attempt {attempt} failed: {str(exc)[:200]}")
            if attempt < _R2_PUBLISH_ATTEMPTS:
                time.sleep(_R2_RETRY_BACKOFF_S * attempt)
    raise RuntimeError(f"copy failed after {_R2_PUBLISH_ATTEMPTS} attempts: {src_key} → {dst_key}: {last}")


def _delete_keys(s3, keys: list) -> int:
    n = 0
    for i in range(0, len(keys), 1000):
        batch = [{"Key": k} for k in keys[i:i + 1000]]
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})
        n += len(batch)
    return n


def _delete_prefix(s3, prefix: str) -> int:
    keys, n = [], 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            keys.append(o["Key"])
            if len(keys) == 1000:
                n += _delete_keys(s3, keys); keys = []
    if keys:
        n += _delete_keys(s3, keys)
    return n


def _publish_full_swap(s3, live_prefix: str, local_dir: str) -> int:
    """Non-destructive full replace (Open Payments ingest.py:619): stage → verify staging==local
    (live untouched) → swap (delete live, copy staging→live, manifest LAST) → verify live==local."""
    local_sizes = _local_file_sizes(local_dir)
    if not local_sizes:
        raise RuntimeError(f"refusing to publish empty dataset to {live_prefix}")
    staging = _staging_prefix(live_prefix)
    ordered = _ordered_rels(local_sizes)

    staged_before = _list_prefix_sizes(s3, staging)
    up = reused = 0
    for rel in ordered:
        if staged_before.get(rel) == local_sizes[rel]:
            reused += 1; continue
        _upload_file_retry(s3, os.path.join(local_dir, rel.replace("/", os.sep)), staging + rel)
        up += 1
    stale = [staging + rel for rel in staged_before if rel not in local_sizes]
    if stale:
        _delete_keys(s3, stale)
    staged_after = _list_prefix_sizes(s3, staging)
    if staged_after != local_sizes:
        missing = {r: local_sizes[r] for r in local_sizes if staged_after.get(r) != local_sizes[r]}
        raise RuntimeError(f"staging verify failed for {staging}: LIVE LEFT INTACT. "
                           f"{len(missing)} missing/size-mismatch; sample={dict(list(missing.items())[:5])}")
    print(f"  staged {up} uploaded + {reused} reused → {staging}; {len(local_sizes)} verified == local")

    deleted = _delete_prefix(s3, live_prefix)
    print(f"  swap: deleted {deleted} old object(s); copying {len(ordered)} from staging")
    for rel in ordered:
        if local_sizes[rel] > _R2_SINGLE_COPY_MAX:
            _upload_file_retry(s3, os.path.join(local_dir, rel.replace("/", os.sep)), live_prefix + rel)
        else:
            _copy_key_retry(s3, staging + rel, live_prefix + rel)
    live_after = _list_prefix_sizes(s3, live_prefix)
    if live_after != local_sizes:
        bad = {r: (local_sizes[r], live_after.get(r)) for r in local_sizes if live_after.get(r) != local_sizes[r]}
        raise RuntimeError(f"live verify failed after swap for {live_prefix}: re-run. "
                           f"{len(bad)} mismatched; sample={dict(list(bad.items())[:5])}")
    try:
        _delete_prefix(s3, staging)
    except Exception as exc:  # noqa: BLE001
        print(f"  WARN: staging cleanup failed (harmless): {str(exc)[:160]}")
    print(f"  published {len(local_sizes)} files (staged-swap) → s3://{BUCKET}/{live_prefix}")
    return len(local_sizes)


def _list_committed_indices(ds) -> list:
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
                    out.append({"name": getattr(ix, "name", None), "type": str(getattr(ix, "type", None)),
                                "fields": getattr(ix, "fields", None)})
            return out
        except Exception as exc:  # noqa: BLE001
            return [{"error": f"{attr}: {exc}"}]
    return [{"error": "no list_indices/list_indexes method"}]


def _verify_published(uri: str, expected_rows: int, expected_indices: int, probe_col: str | None, so: dict) -> dict:
    """Reopen fresh from R2 (Open Payments ingest.py:711); assert rows + index count + a key point-probe."""
    import lance

    ds = lance.dataset(uri, storage_options=so)
    rows = ds.count_rows()
    if rows != expected_rows:
        raise RuntimeError(f"verify {uri}: R2 rows={rows:,} != expected {expected_rows:,}")
    healthy = [i for i in _list_committed_indices(ds) if "error" not in i]
    if expected_indices is not None and len(healthy) != expected_indices:
        raise RuntimeError(f"verify {uri}: indices={len(healthy)} != expected {expected_indices} "
                           f"(committed={[i.get('name') for i in healthy]})")
    if probe_col:
        probe = ds.scanner(filter=f"{probe_col} IS NOT NULL", columns=[probe_col],
                           limit=1, prefilter=True).to_table().num_rows
        if probe < 1:
            raise RuntimeError(f"verify {uri}: index probe on {probe_col} returned 0 rows")
    return {"rows": rows, "indices": len(healthy)}


# ─────────────────────────────────────────── transform ───────────────────────────────────────────
def _snake(name: str) -> str:
    return _re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_")


def _coltype(alias: str) -> str:
    if _MONEY_RE.search(alias): return "DECIMAL(18,2)"
    if _COUNT_RE.search(alias): return "BIGINT"
    if _RATE_RE.search(alias): return "DOUBLE"
    return "VARCHAR"


def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _present_aliases(con, gz_path: str) -> dict:
    """DESCRIBE the staged member → {snake_alias: source_col}. Hard-fail on a snake collision
    (CMS headers are clean; a collision would silently merge two columns)."""
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_csv('{gz_path}', {DESCRIBE_OPTS})").fetchall()
    src = [r[0] for r in rows]
    present: dict[str, str] = {}
    for c in src:
        a = _snake(c)
        if not a:
            continue
        if a in present:
            raise RuntimeError(f"snake collision {a!r} in {gz_path}: {present[a]!r} vs {c!r}")
        present[a] = c
    return present


def _year_select_sql(gz_path: str, union_aliases: list, present: dict, partition: str,
                     label, source_archive: str, source_member: str) -> str:
    parts = []
    for a in union_aliases:
        t = _coltype(a)
        if a in present:
            q = '"' + present[a].replace('"', '""') + '"'
            parts.append((f"nullif(trim({q}), '')" if t == "VARCHAR"
                          else f"TRY_CAST(nullif(trim({q}), '') AS {t})") + f" AS {a}")
        else:
            parts.append(f"CAST(NULL AS {t}) AS {a}")
    proj = ",\n    ".join(parts)
    if partition == "year":
        stamp = f"CAST({int(label)} AS SMALLINT) AS program_year"
    else:
        stamp = f"'{str(label)}' AS snapshot"
    sa = source_archive.replace("'", "''")
    sm = source_member.replace("'", "''")
    return (f"WITH raw AS (SELECT * FROM read_csv('{gz_path}', {READ_OPTS}))\n"
            f"SELECT\n    {proj},\n    {stamp},\n"
            f"    '{sa}' AS source_archive,\n    '{sm}' AS source_member,\n    now() AS ingested_at\n"
            f"FROM raw")


def _width_gate(con, gz_path: str, present: dict) -> int:
    """§9 #2 / §1 #5: count real (non-empty) values in MONEY/COUNT/RATE columns that fail their cast.
    Returns the total. >0 is a hard stop (a wrong type guess or a genuine overflow — never silent NULLs)."""
    checks = []
    for a, srccol in present.items():
        t = _coltype(a)
        if t == "VARCHAR":
            continue
        q = '"' + srccol.replace('"', '""') + '"'
        checks.append(f"count(*) FILTER (WHERE nullif(trim({q}),'') IS NOT NULL "
                      f"AND TRY_CAST(nullif(trim({q}),'') AS {t}) IS NULL)")
    if not checks:
        return 0
    sql = f"SELECT {' + '.join(checks)} FROM read_csv('{gz_path}', {DESCRIBE_OPTS})"
    return int(con.execute(sql).fetchone()[0] or 0)


def _append_local(reader, local_ds: str, create: bool) -> None:
    import lance

    lance.write_dataset(reader, local_ds, schema=reader.schema,
                        mode="create" if create else "append",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE)


def _sorted_rewrite(con, unsorted_ds: str, sorted_ds: str, cluster: str) -> int:
    """Cluster the dataset: read the unsorted local Lance, ORDER BY the cluster key, write a fresh
    sorted Lance (NPPES materialize_analytical.py:439 pattern). DuckDB sorts out-of-core (spill)."""
    import lance

    uds = lance.dataset(unsorted_ds)
    con.register("u", uds.scanner().to_reader())
    reader = con.execute(f"SELECT * FROM u ORDER BY {cluster}").to_arrow_reader(READ_BATCH_ROWS)
    lance.write_dataset(reader, sorted_ds, schema=reader.schema, mode="create",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE)
    con.unregister("u")
    return lance.dataset(sorted_ds).count_rows()


def _build_indexes_local(local_ds: str, cfg: dict) -> list:
    import lance

    ds = lance.dataset(local_ds)
    built = []
    for col in cfg["btree"]:
        # Resolution-key BTREE failures are FATAL and must abort BEFORE the (multi-GB) publish —
        # not surface only at the post-publish verify gate (review Minor 1).
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        built.append(f"BTREE:{col}"); print(f"  BTREE  ✓ {col}")
    for col in cfg["bitmap"]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}"); print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    return built


def _grain_proof(local_ds: str, hub_cols: list) -> int:
    """§9 #1: prove the candidate PK. Returns the number of (hub) groups with count>1 (must be 0
    before the BTREE is trusted as a key). Reads only the hub columns from the columnar Lance."""
    import duckdb
    import lance

    ds = lance.dataset(local_ds)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'; SET temp_directory='{SPILL_DIR}';")
    con.register("d", ds.scanner(columns=hub_cols).to_reader())
    hub = ", ".join(hub_cols)
    dups = con.execute(
        f"SELECT count(*) FROM (SELECT {hub} FROM d GROUP BY {hub} HAVING count(*) > 1)").fetchone()[0]
    con.close()
    return int(dups)


# ─────────────────────────────────────────── discovery ───────────────────────────────────────────
def _year_from_path(member: str) -> str | None:
    for seg in member.split("/"):
        if _re.fullmatch(r"(19|20)\d\d", seg) or _re.fullmatch(r"\d{4}-Q\d", seg):
            return seg
    return None


def _discover(client, name: str) -> list:
    """Resolve a dataset's ingest units: [(archive_key, member, label, etag, crc, file_size)], deduped
    by (label) preferring the non-'-2' archive (§3.1) and asserting byte-identity (CRC) on any twin."""
    cfg = DATASETS[name]
    mre = _re.compile(cfg["member_re"])
    units = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=LANDING_PREFIX):
        for o in page.get("Contents", []):
            base = o["Key"][len(LANDING_PREFIX):]
            if cfg["archive_substr"] not in base or not base.lower().endswith(".zip"):
                continue
            for member, fsize, csize, crc in _zip_members(client, o["Key"]):
                mb = member.rsplit("/", 1)[-1]
                if not mre.search(mb):
                    continue
                label = _year_from_path(member)
                if label is None:
                    continue
                units.append({"archive_key": o["Key"], "archive": base, "member": member,
                              "label": label, "etag": o["ETag"].strip('"'), "crc": crc, "file_size": fsize})
    # dedup by label
    bylabel: dict[str, list] = {}
    for u in units:
        bylabel.setdefault(u["label"], []).append(u)
    deduped = []
    for label, group in bylabel.items():
        if len(group) > 1:
            crcs = {g["crc"] for g in group}
            if len(crcs) > 1:
                raise RuntimeError(f"{name} {label}: {len(group)} non-identical copies (crc={crcs}) — manual review")
            group.sort(key=lambda g: (g["archive"].endswith("-2.zip"), g["archive"]))  # prefer non-'-2'
            print(f"  dedup {name} {label}: {len(group)} byte-identical copies → keep {group[0]['archive']}")
        deduped.append(group[0])
    deduped.sort(key=lambda u: u["label"])
    return deduped


# ─────────────────────────────────────────── ops ledger ───────────────────────────────────────────
def _record_run(*, conflict: str | None = None, **kw) -> None:
    """Truthful, idempotent ledger write. ``conflict`` (a partial-index ON CONFLICT predicate)
    makes the write an UPSERT so re-runs / Modal retries update the existing unit row instead of
    appending a duplicate — the §4 idempotency-key query stays exact."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write."); return
    cols = ("feed", "phase", "dataset", "program_year", "source_archive", "source_member",
            "source_object_etag", "candidate_key_dups", "decimal_overflow_nulls", "rows_processed",
            "rejected_rows", "status", "error", "started_at", "completed_at")
    vals = [kw.get(c) for c in cols]
    vals[0] = FEED
    sql = (f"INSERT INTO ops.cms_medicare_runs ({', '.join(cols)}) "
           f"VALUES ({', '.join(['%s'] * len(cols))})")
    if conflict:
        upd = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "feed")
        sql += f" {conflict} DO UPDATE SET {upd}, recorded_at = now()"
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, vals)
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* write failed: {exc}")


# ─────────────────────────────────────────── workers ───────────────────────────────────────────
@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_state_schema() -> dict:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL); conn.commit()
        cur.execute("SELECT to_regclass('ops.cms_medicare_runs')")
        present = cur.fetchone()[0]
    return {"status": "success", "table": "ops.cms_medicare_runs", "present": str(present)}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 20, memory=4096)
def discover_members(dataset: str = "") -> dict:
    """Read-only central-directory enumeration → ingest units per dataset (no download)."""
    client = _s3_client()
    names = [dataset] if dataset else list(DATASETS)
    out = {}
    for name in names:
        units = _discover(client, name)
        out[name] = {"units": len(units),
                     "labels": sorted({u["label"] for u in units}),
                     "members": [f"{u['label']}::{u['member'].rsplit('/',1)[-1]}" for u in units]}
        print(f"[discover] {name}: {len(units)} units, labels={out[name]['labels']}")
    return out


def _ingest_core(dataset: str) -> dict:
    """Land one dataset (full backfill) across all its years: discover → (pass 1) header union →
    (pass 2) per-member width-gate + project + append → sorted clustering rewrite → grain proof →
    index → publish-swap → verify gate. The ledger records per-year + terminal rows ONLY after R2 is
    verified (truthful), as idempotent UPSERTs (no duplicate rows on Modal retry). Re-raises on failure.

    Whole-dataset rebuild semantics (atomic full-swap). Per-year etag-aware no-op / single-year
    delete-reappend (directive §4 steady-state delta) is a documented follow-up (ingest_dataset_year,
    not yet wired) — the backfill is a correct atomic rebuild without it. Shared by the non-giant
    (49 GiB) and giant (128 GiB) Modal wrappers — the only difference is container RAM for the in-RAM
    npi BTREE sort over a >100M-row giant."""
    import datetime as dt
    import shutil

    import lance

    if dataset not in DATASETS:
        raise RuntimeError(f"unknown dataset {dataset!r}; known: {list(DATASETS)}")
    cfg = DATASETS[dataset]

    started = dt.datetime.now(dt.timezone.utc)
    shutil.rmtree(SCRATCH_DIR, ignore_errors=True)
    os.makedirs(STAGE_DIR, exist_ok=True); os.makedirs(SPILL_DIR, exist_ok=True)
    client = _s3_client()
    so = _r2_storage_options()
    unsorted_ds = os.path.join(LOCAL_DIR, dataset + "__unsorted")
    sorted_ds = os.path.join(LOCAL_DIR, dataset)
    live_prefix = f"{ACTIVE_PREFIX}{dataset}/"

    try:
        units = _discover(client, dataset)
        if not units:
            raise RuntimeError(f"{dataset}: no ingest units discovered")
        print(f"[{dataset}] {len(units)} units: {[u['label'] for u in units]}")

        # PASS 1 — header union (cheap 128 KB inflate per member).
        union: list[str] = []
        for u in units:
            for c in _read_header_cols(client, u["archive_key"], u["member"]):
                a = _snake(c)
                if a and a not in union:
                    union.append(a)
        print(f"[{dataset}] union superset = {len(union)} columns")

        # PASS 2 — per-member width-gate + project + append. Stats accrue in memory; the ledger
        # is written only after publish+verify so it never claims success over an unpublished dataset.
        con = _new_con()
        year_stats: list[dict] = []
        total_rows = total_rejected = total_overflow = 0
        first = True
        for u in units:
            gz = os.path.join(STAGE_DIR, "m.csv.gz")
            _member_to_gz(client, u["archive_key"], u["member"], gz)
            present = _present_aliases(con, gz)
            overflow = _width_gate(con, gz, present)
            if overflow > 0:
                raise RuntimeError(f"{dataset} {u['label']}: {overflow} numeric values fail their cast "
                                   f"(§9 width gate) — wrong type or overflow; refusing to NULL real data")
            sql = _year_select_sql(gz, union, present, cfg["partition"], u["label"], u["archive"], u["member"])
            reader = con.execute(sql).to_arrow_reader(READ_BATCH_ROWS)
            _append_local(reader, unsorted_ds, create=first)
            try:
                rej = con.execute("SELECT count(*) FROM reject_errors").fetchone()
                rej = int(rej[0]) if rej else 0
            except Exception:  # noqa: BLE001
                rej = 0
            con.execute("DROP TABLE IF EXISTS reject_errors")
            yr_rows = lance.dataset(unsorted_ds).count_rows() - total_rows
            total_rows += yr_rows; total_rejected += rej; total_overflow += overflow
            first = False
            os.remove(gz)
            year_stats.append({"label": u["label"], "archive": u["archive"],
                               "member": u["member"].rsplit("/", 1)[-1], "etag": u["etag"],
                               "rows": yr_rows, "rej": rej, "overflow": overflow})
            print(f"[{dataset}] {u['label']}: +{yr_rows:,} rows ({rej} rejected, {overflow} overflow)")

        # Clustering sorted rewrite (or pass-through when no cluster key).
        if cfg.get("cluster"):
            rows = _sorted_rewrite(con, unsorted_ds, sorted_ds, cfg["cluster"])
            print(f"[{dataset}] clustered ORDER BY {cfg['cluster']} → {rows:,} rows")
        else:
            os.rename(unsorted_ds, sorted_ds)
            rows = lance.dataset(sorted_ds).count_rows()
        con.close()

        # §9 #1 grain proof — BEFORE the index build, so the BTREE is never trusted on an unproven key.
        dups = None
        hub = GRAIN.get(dataset)
        if hub:
            dups = _grain_proof(sorted_ds, hub)
            print(f"[{dataset}] grain proof {hub}: {dups} dup groups")
            if dups > 0:
                raise RuntimeError(f"{dataset}: candidate key {hub} has {dups} dup groups — grain wrong, "
                                   f"widen before trusting the BTREE (§9 #1)")

        # Index → publish-swap → verify.
        built = _build_indexes_local(sorted_ds, cfg)
        expected_idx = len(cfg["btree"]) + len(cfg["bitmap"])
        s3 = _s3_client()
        published = _publish_full_swap(s3, live_prefix, sorted_ds)
        uri = f"s3://{BUCKET}/{live_prefix}"
        probe = cfg.get("npi_col") or (cfg["btree"][0] if cfg["btree"] else None)
        vr = _verify_published(uri, rows, expected_idx, probe, so)

        # Terminal, truthful, idempotent ledger: R2 is now verified whole.
        completed = dt.datetime.now(dt.timezone.utc)
        for ys in year_stats:
            _record_run(conflict=_INGEST_CONFLICT, phase="ingest", dataset=dataset,
                        program_year=int(ys["label"]) if cfg["partition"] == "year" else None,
                        source_archive=ys["archive"], source_member=ys["member"], source_object_etag=ys["etag"],
                        decimal_overflow_nulls=ys["overflow"], rows_processed=ys["rows"], rejected_rows=ys["rej"],
                        status="success", started_at=started, completed_at=completed)
        _record_run(conflict=_VERIFY_CONFLICT, phase="verify", dataset=dataset, candidate_key_dups=dups,
                    decimal_overflow_nulls=total_overflow, rows_processed=rows, rejected_rows=total_rejected,
                    status="success", started_at=started, completed_at=completed)
        return {"status": "success", "dataset": dataset, "rows": rows, "units": len(units),
                "union_cols": len(union), "indices": built, "grain_dups": dups,
                "overflow_nulls": total_overflow, "rejected": total_rejected,
                "published_files": published, "verify": vr, "dataset_uri": uri}
    except Exception as exc:
        _record_run(conflict=_VERIFY_CONFLICT, phase="verify", dataset=dataset, status="error",
                    error=str(exc)[:2000], started_at=started,
                    completed_at=dt.datetime.now(dt.timezone.utc))
        raise
    finally:
        shutil.rmtree(SCRATCH_DIR, ignore_errors=True)


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 8, memory=49152, cpu=8.0, ephemeral_disk=524288, retries=2,
)
def ingest_dataset(dataset: str) -> dict:
    """Non-giant landing (49 GiB). Refuses giants — their >100M-row in-RAM npi BTREE sort needs the
    128 GiB ``ingest_dataset_giant`` wrapper (review finding: in-process build OOM on 48 GiB)."""
    cfg = DATASETS.get(dataset)
    if cfg and cfg.get("giant"):
        raise RuntimeError(f"{dataset} is a GIANT — run ::ingest_giant (128 GiB), not ::ingest.")
    return _ingest_core(dataset)


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 10, memory=131072, cpu=8.0, ephemeral_disk=524288, retries=1,
)
def ingest_dataset_giant(dataset: str) -> dict:
    """Giant landing (128 GiB RAM, 10 h) for B2/A2 — high RAM absorbs the >100M-row in-RAM npi BTREE
    sort (LANCE_BYPASS_SPILLING); DuckDB ORDER BY spills the clustering rewrite to the 512 GiB /tmp.
    Same _ingest_core logic; the only difference is container size. retries=1 (a full restart is the
    recovery — publish is atomic + read-back-verified, so a retry cannot corrupt the live dataset)."""
    cfg = DATASETS.get(dataset)
    if not (cfg and cfg.get("giant")):
        raise RuntimeError(f"{dataset} is not a giant — run ::ingest (49 GiB), not ::ingest_giant.")
    return _ingest_core(dataset)


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15, memory=8192)
def verify_dataset(dataset: str) -> dict:
    import lance

    cfg = DATASETS[dataset]
    so = _r2_storage_options()
    uri = f"s3://{BUCKET}/{ACTIVE_PREFIX}{dataset}/"
    ds = lance.dataset(uri, storage_options=so)
    return {"dataset": dataset, "dataset_uri": uri, "rows": ds.count_rows(),
            "indices": _list_committed_indices(ds)}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 25) -> list:
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT id, phase, dataset, program_year, rows_processed, rejected_rows, "
                    "candidate_key_dups, decimal_overflow_nulls, status, error, recorded_at "
                    "FROM ops.cms_medicare_runs ORDER BY id DESC LIMIT %s", (limit,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ─────────────────────────────────────────── local entrypoints ───────────────────────────────────────────
@app.local_entrypoint()
def init_state() -> None:
    import json
    print(json.dumps(apply_state_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def discover(dataset: str = "") -> None:
    import json
    print(json.dumps(discover_members.remote(dataset), indent=2, default=str))


@app.local_entrypoint()
def ingest(dataset: str) -> None:
    import json
    print(json.dumps(ingest_dataset.remote(dataset), indent=2, default=str))


@app.local_entrypoint()
def ingest_giant(dataset: str) -> None:
    """Land a GIANT (B2/A2) on the 128 GiB wrapper."""
    import json
    print(json.dumps(ingest_dataset_giant.remote(dataset), indent=2, default=str))


@app.local_entrypoint()
def verify(dataset: str) -> None:
    import json
    print(json.dumps(verify_dataset.remote(dataset), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 25) -> None:
    import json
    print(json.dumps(ledger.remote(limit), indent=2, default=str))
