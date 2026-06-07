#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.11,<3.13"
# dependencies = [
#   "duckdb>=1.5,<2",
#   "pylance>=7",
#   "pyarrow>=17",
#   "requests>=2.32",
#   "boto3>=1.35",
# ]
# ///
"""Targeted ingest — DOL Form 5500 (2025) high-signal core → type-safe Lance on R2.

Executes the Targeted Ingestion Strategy from docs/form5500_relational_diagnostic.md:
bypass the high-volume / low-signal actuarial long tables and land ONLY the asset,
participant, and counterparty-fee spine as independent, indexed, type-safe `.lance`
datasets in the R2 system of record.

Data plane — R2 → (DuckDB transform) → R2. The bulk zips are ALREADY STAGED in the R2
landing zone (an operator drop); this pipeline reads them FROM R2, never from the DOL
website. Only the small per-table schema layout (which is not a stageable artifact — it is
served as text off the EBSA site) is fetched from the `secondary_url` the manifest carries.

    s3://data-sink/landing/form-5500/form5500_2025_files.csv   (manifest)
    s3://data-sink/landing/form-5500/<stem>_2025_Latest.zip    (bulk data, pre-staged)
      → per target stem:
          GET the layout text (secondary_url, EBSA site)            → field→type contract
          GET the data zip (boto3, R2 landing) → STREAM into memory  → isolate inner CSV
          pyarrow read ALL-STRING (schema forced from header)        → no inference, ever
          DuckDB typed projection (TRY_CAST numerics; keys VARCHAR)  → streaming Arrow
          lance.write_dataset → local stage → BTREE scalar indexes
          publish to s3://data-sink/active/form5500_<name>/ (boto3, manifest-last)
          read-back verification from R2 → Markdown report to stdout

Why stage-then-publish (not a direct Lance→R2 write): Cloudflare R2 rejects the
variable-size multipart parts object_store emits on larger fragments ("all non-trailing
parts must have the same length"); boto3/s3transfer uses uniform parts. This is the proven
fleet pattern (CMS Open Payments, PDL, FMCSA). Net-new datasets here, so the publish is a
clean delete-prefix → upload-ordered (data/index/txn first, `_versions/` manifest LAST, so
a reader never sees a manifest pointing at absent data) → size-census verify → read-back.

Type contract (precedence, highest first). The DOL layout is authoritative; the two
name-based rules satisfy the directive's "no strings in numeric zones" for the handful of
count fields DOL over-declares as TEXT:
  1. FORCE_STRING keys (ACK_ID, *_EIN, *_PN/PLAN_NUM, FORM_ID)  → VARCHAR, ALWAYS.
     EFAST2 keys carry structural leading zeros; a numeric cast truncates them and
     permanently corrupts the resolution graph. These never touch a numeric cast.
  2. layout TYPE == NUMERIC, or column name ~ /_AMT$/           → DOUBLE  (TRY_CAST)
  3. column name ~ /_CNT$/                                       → BIGINT  (TRY_CAST)
  4. everything else (TEXT dates, names, codes)                  → VARCHAR
TRY_CAST throughout: a malformed numeric becomes NULL (never an abort).

Index plan (directive §3): BTREE on ACK_ID for every table; head tables additionally get a
BTREE on each business-identity key. Lance scalar indexes are single-column, so the
composite (EIN, PN) identity is realized as one BTREE per column — which is exactly what
accelerates an EIN or EIN+PN resolution filter.

Run (R2 creds via Doppler):
    doppler run --project core-x --config prd -- uv run pipelines/form5500/ingest_form5500.py
    doppler run --project core-x --config prd -- uv run pipelines/form5500/ingest_form5500.py --only main,sch_h
    # local dry-run (writes Lance to a local dir instead of publishing to R2):
    doppler run --project core-x --config prd -- uv run pipelines/form5500/ingest_form5500.py --target ~/core-x-lake/active
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import os
import shutil
import sys
import tempfile
import zipfile

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")  # in-memory BTREE sort

import pyarrow as pa  # noqa: E402
import pyarrow.csv as pacsv  # noqa: E402
import requests  # noqa: E402

# ── Fleet constants ──────────────────────────────────────────────────────────────────────
BUCKET = "data-sink"
LANDING_PREFIX = "landing/form-5500/"      # operator-staged source zips + manifest
ACTIVE_PREFIX = "active/"                   # Gen-3 SoR → active/form5500_<name>/
MANIFEST_KEY = LANDING_PREFIX + "form5500_2025_files.csv"
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
LAYOUT_TIMEOUT = (30, 120)                  # EBSA site — small text only

# Keys that MUST stay VARCHAR (leading-zero integrity). Matched against any present column.
FORCE_STRING = {
    "ACK_ID",
    "SPONS_DFE_EIN", "SF_SPONS_EIN", "PROVIDER_OTHER_EIN", "INS_BROKER_EIN",
    "SPONS_DFE_PN", "SF_PLAN_NUM",
    "FORM_ID", "PROVIDER_ELIGIBLE_EIN",
    # Schedule A (carrier head) — denormalized sponsor identity + carrier identity. DOL
    # declares FORM_ID NUMERIC (above) and NAIC_CODE TEXT/5; both carry structural leading
    # zeros and are join keys, never arithmetic — pin them to VARCHAR regardless of layout.
    "SCH_A_EIN", "SCH_A_PLAN_NUM", "INS_CARRIER_EIN", "INS_CARRIER_NAIC_CODE",
    # Schedule C counterparties — indirect-comp payor + terminated accountant/actuary EINs.
    "PROVIDER_PAYOR_EIN", "PROVIDER_TERM_EIN",
}

# ── Target Archive Registry (the diagnostic's high-signal spine) ────────────────────────
#  stem     : F_* archive stem (matched on the manifest data_file).
#  name     : Lance dataset name → active/form5500_<name>/.
#  biz_keys : head-table business-identity keys → one extra BTREE each.
#  lz_col   : column asserted to retain leading-zero strings (no integer coercion). None ⇒ skip.
STEMS = [
    {"stem": "F_5500",                    "name": "main",               "biz_keys": ["SPONS_DFE_EIN", "SPONS_DFE_PN"], "lz_col": "SPONS_DFE_EIN"},
    {"stem": "F_5500_SF",                 "name": "sf",                 "biz_keys": ["SF_SPONS_EIN", "SF_PLAN_NUM"],    "lz_col": "SF_SPONS_EIN"},
    {"stem": "F_SCH_H",                   "name": "sch_h",              "biz_keys": [], "lz_col": None},
    {"stem": "F_SCH_I",                   "name": "sch_i",              "biz_keys": [], "lz_col": None},
    {"stem": "F_SCH_C_PART1_ITEM2",       "name": "sch_c_provider",     "biz_keys": [], "lz_col": "PROVIDER_OTHER_EIN"},
    {"stem": "F_SCH_C_PART1_ITEM2_CODES", "name": "sch_c_provider_code","biz_keys": [], "lz_col": None},
    # F_SCH_A_PART1 broker rows join Schedule A on the composite (ACK_ID + FORM_ID); index
    # FORM_ID here too so the carrier↔broker join is BTREE-pushed on BOTH sides.
    {"stem": "F_SCH_A_PART1",             "name": "sch_a_broker",       "biz_keys": ["FORM_ID"], "lz_col": None},
    # ── Reconciliation patch (form5500_ingestion_reconciliation.md) — stranded carrier ──
    #    identity + Schedule C counterparty-fee spine the targeted v1 ingest skipped.
    #    F_SCH_A is the insurance-contract head carrying INS_CARRIER_NAME / INS_CARRIER_EIN /
    #    INS_CARRIER_NAIC_CODE (the TiC triage key); F_SCH_A_PART1 (broker, above) is only its
    #    commission detail. Schedule C: ITEM3 = indirect comp by payor (PBM/consultant flows,
    #    PROVIDER_INDIRECT_COMP_AMT), ITEM1 = eligible-indirect-only providers, PART3 =
    #    terminated accountants/actuaries. All detail-table *_EIN are COUNTERPARTIES (resolve
    #    against the external entity graph), never a back-pointer to the plan sponsor.
    {"stem": "F_SCH_A",                   "name": "sch_a_carrier",      "biz_keys": ["FORM_ID", "SCH_A_EIN", "SCH_A_PLAN_NUM"], "lz_col": "INS_CARRIER_EIN"},
    {"stem": "F_SCH_C_PART1_ITEM1",       "name": "sch_c_eligible",     "biz_keys": [], "lz_col": "PROVIDER_ELIGIBLE_EIN"},
    {"stem": "F_SCH_C_PART1_ITEM3",       "name": "sch_c_indirect",     "biz_keys": [], "lz_col": "PROVIDER_PAYOR_EIN"},
    {"stem": "F_SCH_C_PART3",             "name": "sch_c_terminated",   "biz_keys": [], "lz_col": None},
]

EMPTY = "''"


# ── R2 client + storage options (from the Doppler-injected R2_* env) ─────────────────────
def r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account:
        endpoint = f"https://{account}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("R2_ENDPOINT (or R2_ACCOUNT_ID) not set — run under `doppler run`.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def s3_client():
    import boto3
    from botocore.config import Config

    so = r2_storage_options()
    cfg = Config(
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
        retries={"max_attempts": 5, "mode": "standard"},
        s3={"addressing_style": "path"},
    )
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


# ── Manifest ─────────────────────────────────────────────────────────────────────────────
def load_manifest(s3, explicit: str | None) -> tuple[bytes, str]:
    """Manifest bytes + a provenance label. An explicit local path wins; otherwise read the
    canonical staged copy from R2 landing."""
    if explicit and os.path.isfile(explicit):
        with open(explicit, "rb") as fh:
            return fh.read(), explicit
    body = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read()
    return body, f"s3://{BUCKET}/{MANIFEST_KEY}"


def read_targets(manifest_bytes: bytes) -> dict[str, dict]:
    """{stem: {data_file, layout_url, description}} for the targeted stems only."""
    import csv

    wanted = {s["stem"] for s in STEMS}
    out: dict[str, dict] = {}
    for row in csv.DictReader(io.StringIO(manifest_bytes.decode("utf-8-sig"))):
        data_file = (row.get("data_file") or "").strip()
        if not data_file:
            continue
        stem = data_file.rsplit(".zip", 1)[0].replace("_2025_Latest", "").replace("_2025_latest", "")
        if stem in wanted and stem not in out:
            out[stem] = {
                "data_file": data_file,
                "layout_url": (row.get("secondary_url") or "").strip(),
                "description": (row.get("description") or "").strip(),
            }
    missing = wanted - set(out)
    if missing:
        raise RuntimeError(f"targeted stems absent from manifest: {sorted(missing)}")
    return out


# ── Source reads ─────────────────────────────────────────────────────────────────────────
def r2_zip_bytes(s3, data_file: str) -> bytes:
    """Read a pre-staged zip from R2 landing fully into memory."""
    key = LANDING_PREFIX + data_file
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def layout_text(url: str) -> str:
    """Fetch the EFAST2 `*_layout.txt` schema (EBSA site). Schema metadata only — small, and
    not a stageable artifact, so the manifest's secondary_url is its source of record."""
    resp = requests.get(url, timeout=LAYOUT_TIMEOUT)
    resp.raise_for_status()
    return resp.content.decode("utf-8", "replace")


def parse_layout(text: str) -> dict[str, str]:
    """EFAST2 layout (FIELD_POSITION,FIELD_NAME,TYPE,SIZE) → {FIELD_NAME_UPPER: 'TEXT'|'NUMERIC'}."""
    types: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("FIELD_POSITION") or set(line) <= {"="}:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 3 and parts[0].isdigit():
            types[parts[1].upper()] = parts[2].upper()
    if not types:
        raise RuntimeError("layout parsed to zero columns")
    return types


# ── In-memory CSV extraction (ALL-STRING — the inference guard) ──────────────────────────
def zip_csv_to_arrow(zip_bytes: bytes, stem: str) -> pa.Table:
    """Isolate the single CSV member of an in-memory zip and read it ALL-STRING. The schema
    is forced from the CSV header, so pyarrow never type-infers a key column to int (which
    would silently drop leading zeros before DuckDB sees it). UTF-8 with latin-1 fallback."""
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
    if not members:
        raise RuntimeError(f"{stem}: no .csv member in zip ({zf.namelist()})")
    member = max(members, key=lambda m: zf.getinfo(m).file_size)
    raw = zf.read(member)

    nl = raw.find(b"\n")
    header = raw[:nl if nl != -1 else len(raw)].decode("utf-8-sig", "replace").rstrip("\r")
    names = [h.strip().strip('"') for h in header.split(",")]
    if len(set(names)) != len(names):
        seen: dict[str, int] = {}
        uniq = []
        for n in names:
            seen[n] = seen.get(n, 0) + 1
            uniq.append(n if seen[n] == 1 else f"{n}_{seen[n]}")
        names = uniq
    str_types = {n: pa.string() for n in names}

    read_opts = pacsv.ReadOptions(use_threads=True, column_names=names, skip_rows=1)
    parse_opts = pacsv.ParseOptions(newlines_in_values=True)
    conv_opts = pacsv.ConvertOptions(column_types=str_types, strings_can_be_null=True,
                                     null_values=["", "NA", "NULL"])
    try:
        tbl = pacsv.read_csv(io.BytesIO(raw), read_options=read_opts,
                             parse_options=parse_opts, convert_options=conv_opts)
    except (pa.ArrowInvalid, UnicodeDecodeError):
        clean = raw.decode("latin-1").encode("utf-8")
        tbl = pacsv.read_csv(io.BytesIO(clean), read_options=read_opts,
                             parse_options=parse_opts, convert_options=conv_opts)

    non_str = [f.name for f in tbl.schema if not pa.types.is_string(f.type)]
    if non_str:
        raise RuntimeError(f"{stem}: non-string columns after all-string read: {non_str}")
    return tbl


# ── Typed projection (DuckDB) ────────────────────────────────────────────────────────────
def col_expr(name: str, layout_types: dict[str, str]) -> tuple[str, str]:
    up = name.upper()
    q = '"' + name.replace('"', '""') + '"'
    trimmed = f"NULLIF(TRIM({q}), {EMPTY})"
    if up in FORCE_STRING:
        return f"{trimmed} AS {q}", "VARCHAR"
    lt = layout_types.get(up, "")
    if lt == "NUMERIC" or up.endswith("_AMT"):
        return f"TRY_CAST({trimmed} AS DOUBLE) AS {q}", "DOUBLE"
    if up.endswith("_CNT"):
        return f"TRY_CAST({trimmed} AS BIGINT) AS {q}", "BIGINT"
    return f"{trimmed} AS {q}", "VARCHAR"


def build_projection(arrow_cols: list[str], layout_types: dict[str, str],
                     source_file: str, source_uri: str) -> tuple[str, dict[str, str]]:
    exprs, bound = [], {}
    for c in arrow_cols:
        expr, typ = col_expr(c, layout_types)
        exprs.append(expr)
        bound[c] = typ

    def lit(s: str) -> str:
        return s.replace("'", "''")

    sql = (
        "SELECT\n    " + ",\n    ".join(exprs)
        + f",\n    '{lit(source_file)}' AS source_file"
        + f",\n    '{lit(source_uri)}' AS source_uri"
        + ",\n    now() AS ingested_at\nFROM raw"
    )
    return sql, bound


# ── R2 publish (stage-local → upload-ordered → verify) ───────────────────────────────────
def _rank(rel: str) -> int:
    """Lance dependency order: the manifest (`_versions/`) references data/index/txn files,
    so it is uploaded LAST — a reader never sees a manifest pointing at absent data."""
    if rel.startswith("_versions/"):
        return 4
    if rel.startswith("_transactions/"):
        return 3
    if rel.startswith("_indices/"):
        return 2
    return 1


def _local_sizes(local_dir: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for root, _d, files in os.walk(local_dir):
        for fn in files:
            lp = os.path.join(root, fn)
            out[os.path.relpath(lp, local_dir).replace(os.sep, "/")] = os.path.getsize(lp)
    return out


def _r2_sizes(s3, prefix: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if rel:
                out[rel] = o["Size"]
    return out


def _delete_prefix(s3, prefix: str) -> int:
    keys, n = [], 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            keys.append({"Key": o["Key"]})
            if len(keys) == 1000:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys, "Quiet": True})
                n += len(keys)
                keys = []
    if keys:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys, "Quiet": True})
        n += len(keys)
    return n


def publish_to_r2(s3, local_dir: str, prefix: str) -> int:
    """Clean publish of a net-new local Lance dataset to R2: delete any prior prefix, upload
    every file in dependency order (manifest LAST), then size-census verify. Returns files
    published. Raises on any local/R2 size mismatch."""
    local = _local_sizes(local_dir)
    if not local:
        raise RuntimeError(f"refusing to publish empty dataset → {prefix}")
    _delete_prefix(s3, prefix)
    for rel in sorted(local, key=lambda r: (_rank(r), r)):
        s3.upload_file(os.path.join(local_dir, rel.replace("/", os.sep)), BUCKET, prefix + rel)
    r2 = _r2_sizes(s3, prefix)
    if r2 != local:
        bad = {r: (local[r], r2.get(r)) for r in local if r2.get(r) != local[r]}
        raise RuntimeError(f"publish verify failed for {prefix}: {len(bad)} mismatched; "
                           f"sample={dict(list(bad.items())[:4])}")
    return len(local)


# ── Per-stem ingest ──────────────────────────────────────────────────────────────────────
def ingest_stem(s3, spec: dict, target: str) -> dict:
    """R2 landing zip → all-string parse → typed projection → local Lance stage → BTREE
    indexes → publish to R2 active (or write to a local dir if --target is a path)."""
    import duckdb
    import lance

    stem, name = spec["stem"], spec["name"]
    data_file, layout_url = spec["data_file"], spec["layout_url"]
    to_r2 = target.startswith("s3://")

    print(f"[{stem}] layout ← {layout_url.rsplit('/', 1)[-1]} (EBSA)", file=sys.stderr)
    layout_types = parse_layout(layout_text(layout_url))

    src_uri = f"s3://{BUCKET}/{LANDING_PREFIX}{data_file}"
    print(f"[{stem}] data   ← {src_uri}", file=sys.stderr)
    tbl = zip_csv_to_arrow(r2_zip_bytes(s3, data_file), stem)
    src_rows, src_cols = tbl.num_rows, tbl.column_names
    print(f"[{stem}] parsed {src_rows:,} rows × {len(src_cols)} cols (all-string)", file=sys.stderr)

    sql, bound = build_projection(src_cols, layout_types, data_file, src_uri)
    con = duckdb.connect(":memory:")
    con.execute("SET preserve_insertion_order=false;")
    con.register("raw", tbl)
    reader = con.sql(sql).to_arrow_reader(MAX_ROWS_PER_FILE)

    # Stage Lance locally (works for both targets; R2 publish needs the local tree anyway).
    stage = tempfile.mkdtemp(prefix=f"f5500_{name}_")
    local_ds = os.path.join(stage, f"form5500_{name}.lance")
    try:
        lance.write_dataset(
            reader, local_ds, schema=reader.schema, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
        )
        con.close()

        ds = lance.dataset(local_ds)
        written_cols = {f.name for f in ds.schema}
        index_cols = ["ACK_ID"] + list(spec["biz_keys"])
        for col in index_cols:
            if col not in written_cols:
                raise RuntimeError(f"{stem}: mandated index column {col!r} absent from schema")
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            print(f"[{stem}] BTREE ✓ {col}", file=sys.stderr)

        if to_r2:
            prefix = f"{target.split('s3://' + BUCKET + '/', 1)[1].rstrip('/')}/" if target != f"s3://{BUCKET}" else ACTIVE_PREFIX
            prefix = f"{prefix}form5500_{name}/"
            n = publish_to_r2(s3, local_ds, prefix)
            uri = f"s3://{BUCKET}/{prefix}"
            so = r2_storage_options()
            print(f"[{stem}] published {n} files → {uri}", file=sys.stderr)
        else:
            root = os.path.abspath(os.path.expanduser(target))
            os.makedirs(root, exist_ok=True)
            uri = os.path.join(root, f"form5500_{name}.lance")
            if os.path.exists(uri):
                shutil.rmtree(uri)
            shutil.copytree(local_ds, uri)
            so = None
            print(f"[{stem}] wrote → {uri}", file=sys.stderr)
    finally:
        shutil.rmtree(stage, ignore_errors=True)

    # Read back from the published location for the count + index census.
    ds = lance.dataset(uri, storage_options=so)
    rows = ds.count_rows()
    n_numeric = sum(1 for t in bound.values() if t in ("DOUBLE", "BIGINT"))
    return {
        "stem": stem, "name": name, "uri": uri, "description": spec["description"],
        "rows": rows, "cols": len(ds.schema), "src_rows": src_rows,
        "indexes": index_cols, "biz_keys": list(spec["biz_keys"]),
        "n_numeric": n_numeric, "n_string": len(bound) - n_numeric,
        "lz_col": spec["lz_col"], "ack_type": str(ds.schema.field("ACK_ID").type),
        "storage_options": so, "source_uri": src_uri,
    }


# ── Verification suite ───────────────────────────────────────────────────────────────────
def verify(stat: dict) -> dict:
    import lance

    ds = lance.dataset(stat["uri"], storage_options=stat["storage_options"])
    ack_nulls = ds.count_rows(filter="ACK_ID IS NULL")
    checks = {"ack_nulls": ack_nulls, "ack_ok": ack_nulls == 0,
              "row_match": ds.count_rows() == stat["src_rows"]}
    lz_col = stat["lz_col"]
    if lz_col and lz_col in {f.name for f in ds.schema}:
        lz_hits = ds.count_rows(filter=f"{lz_col} LIKE '0%'")
        checks.update(lz_col=lz_col, lz_hits=lz_hits, lz_ok=lz_hits > 0)
    else:
        checks["lz_ok"] = None
    return {**stat, **checks}


# ── Markdown report ──────────────────────────────────────────────────────────────────────
def render_report(results: list[dict], target: str, manifest_label: str,
                  started: dt.datetime) -> tuple[str, bool]:
    elapsed = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
    all_ok = all(r["ack_ok"] and r["row_match"] and (r["lz_ok"] in (True, None)) for r in results)
    total_rows = sum(r["rows"] for r in results)
    total_idx = sum(len(r["indexes"]) for r in results)
    to_r2 = target.startswith("s3://")

    L = ["# Form 5500 (2025) Targeted Ingest — Verification Report", ""]
    L.append(f"**Result:** {'✅ PASS' if all_ok else '❌ FAIL'} — {len(results)} datasets · "
             f"{total_rows:,} rows · {total_idx} BTREE indexes · {elapsed:,.1f}s")
    L.append(f"**Source:** `s3://{BUCKET}/{LANDING_PREFIX}` (R2 landing, pre-staged) · "
             f"layouts ← EBSA `secondary_url`")
    L.append(f"**Target:** `{target}` ({'R2 SoR' if to_r2 else 'local'}) · "
             f"Lance storage v{DATA_STORAGE_VERSION}")
    L.append(f"**Manifest:** `{manifest_label}`")
    L.append(f"**Run (UTC):** {started.isoformat(timespec='seconds')}")
    L.append("")

    L += ["## 1. Dataset inventory", "",
          "| Dataset | Source stem | Rows | Cols | Numeric | String | BTREE indexes |",
          "|---|---|--:|--:|--:|--:|---|"]
    for r in results:
        L.append(f"| `form5500_{r['name']}` | {r['stem']} | {r['rows']:,} | {r['cols']} | "
                 f"{r['n_numeric']} | {r['n_string']} | {', '.join(r['indexes'])} |")
    L.append("")

    L += ["## 2. Type-safety proof (keys bound to TEXT)", "",
          "| Dataset | ACK_ID dtype | Business-identity BTREE |", "|---|---|---|"]
    for r in results:
        bk = ", ".join(f"`{k}`" for k in r["biz_keys"]) or "— (joins by ACK_ID)"
        flag = "✅" if r["ack_type"] == "string" else "❌"
        L.append(f"| `form5500_{r['name']}` | `{r['ack_type']}` {flag} | {bk} |")
    L.append("")

    L += ["## 3. Verification suite", "",
          "| Dataset | Rows landed | Row-count match | ACK_ID nulls | Leading-zero | Status |",
          "|---|--:|:--:|--:|---|:--:|"]
    for r in results:
        lz = "— (n/a)" if r["lz_ok"] is None else \
            f"`{r['lz_col']} LIKE '0%'` → {r['lz_hits']:,} {'✅' if r['lz_ok'] else '❌'}"
        ok = r["ack_ok"] and r["row_match"] and (r["lz_ok"] in (True, None))
        L.append(f"| `form5500_{r['name']}` | {r['rows']:,} | {'✅' if r['row_match'] else '❌'} | "
                 f"{r['ack_nulls']} {'✅' if r['ack_ok'] else '❌'} | {lz} | "
                 f"{'PASS' if ok else 'FAIL'} |")
    L.append("")

    L += ["## 4. Checks asserted", "",
          "- **Row-count** — physical rows landed per table equal rows parsed from each source CSV.",
          "- **Key integrity** — `ACK_ID IS NULL` count is **0** on every table.",
          "- **Leading-zero** — `SPONS_DFE_EIN` / `SF_SPONS_EIN` / `PROVIDER_OTHER_EIN` retain "
          "`0…`-prefixed strings (proves no integer coercion truncated the keys).", ""]
    return "\n".join(L), all_ok


# ── Entrypoint ───────────────────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser(description="Targeted R2→R2 ingest of the DOL Form 5500 (2025) core.")
    ap.add_argument("--manifest", default=None, help="local manifest override (default: R2 landing copy)")
    ap.add_argument("--target", default=os.environ.get("FORM5500_TARGET", f"s3://{BUCKET}/active"),
                    help="dataset root: s3://data-sink/active (default, R2 SoR) or a local dir")
    ap.add_argument("--only", default=None, help="comma-separated dataset names (e.g. main,sch_h)")
    args = ap.parse_args()

    started = dt.datetime.now(dt.timezone.utc)
    s3 = s3_client()
    manifest_bytes, manifest_label = load_manifest(s3, args.manifest)
    targets = read_targets(manifest_bytes)

    specs = STEMS
    if args.only:
        keep = {s.strip() for s in args.only.split(",")}
        specs = [s for s in STEMS if s["name"] in keep]
        if not specs:
            print(f"--only matched nothing; valid: {[s['name'] for s in STEMS]}", file=sys.stderr)
            return 2

    print(f"manifest: {manifest_label}", file=sys.stderr)
    print(f"source:   s3://{BUCKET}/{LANDING_PREFIX}", file=sys.stderr)
    print(f"target:   {args.target}", file=sys.stderr)
    print(f"datasets: {len(specs)}\n", file=sys.stderr)

    results = []
    for spec in specs:
        merged = {**spec, **targets[spec["stem"]]}
        results.append(verify(ingest_stem(s3, merged, args.target)))
        print("", file=sys.stderr)

    report, all_ok = render_report(results, args.target, manifest_label, started)
    print(report)
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
