"""Compute worker — MSHA coverage-gap extensions (msha_contractors + msha_accidents).

Closes the two highest-severity gaps from the MSHA legal-entity diagnostic (PR #140,
docs/reference/MSHA_LEGAL_ENTITY_SCHEMA_DIAGNOSTIC.md): the orphaned contractor
corporate-name registry and the absent Part-50 accident/injury feed. Same clean-room
data plane as ``materialize_msha.py`` — DuckDB does 100% of the transform, Lance is
written straight to R2, no Iceberg/Polaris. This is an ADDITIVE sibling worker: it does
NOT touch the three datasets the base worker owns.

WHAT THIS WORKER DOES
    Extracts three landing archives staged in R2 and materializes two new isolated Lance
    datasets keyed on MSHA's own native primary keys. Source archives are already in R2 —
    the worker pulls .zip from R2, never the web.

    SOURCES (R2; profiled in MSHA_DATA_PROFILING_REPORT.md, audited live in
    MSHA_LEGAL_ENTITY_SCHEMA_DIAGNOSTIC.md):
        s3://data-sink/landing/msha/ContractorProdQuarterly.zip  (12 cols, 1,350,534 rows)
        s3://data-sink/landing/msha/ContractorProdYearly.zip     (10 cols,   280,142 rows)
        s3://data-sink/landing/msha/Accidents.zip                (57 cols,   273,065 rows)

    TARGETS (Gen-3 SoR — native Lance v2.1, full-snapshot overwrite):
        s3://data-sink/active/msha_contractors/   ContractorProdQuarterly ⊎ ContractorProdYearly
                                                  (UNION ALL BY NAME → unified firmographic master)
        s3://data-sink/active/msha_accidents/     Accidents (one row per DOCUMENT_NO)

ENTITY-NAME NORMALIZATION (Directive-29 isolation exception — AUTHORIZED, parity w/ base).
    Landed on native keys (no JOIN to sos_normalized_master / companies / PPP at ingest),
    but the "no name_norm" guardrail is LIFTED for crosswalk readiness: each entity legal-
    name key gets a persisted ``<COL>_norm`` sibling via core.name_norm at write-time
    (msha_contractors → CONTRACTOR_NAME; msha_accidents → CONTROLLER_NAME, OPERATOR_NAME).
    Equipment/manufacturer names (EQUIP_MFR_NAME) are NOT normalized — entity legal names
    only. Every source column otherwise keeps its exact UPPERCASE spelling; the non-native
    columns are the two provenance fields (source_file, ingested_at) + the _norm siblings.

PARSING HYGIENE — the Directive-26 recipe (quote='' + CP1252→UTF-8), verbatim parity with
    materialize_msha.py so the five shipped datasets and these two share one read contract:
    (a) quote='' (quote processing OFF): MSHA wraps strings in " but does NOT double
        unescaped interior quotes; a strict RFC-4180 read collapses. Wrapping quotes survive
        as literal text and are stripped per-field with trim(BOTH '"' FROM col).
    (b) Encoding is Windows-1252, not UTF-8: the worker transcodes CP1252→UTF-8 to scratch
        BEFORE DuckDB reads (utf-8). Single-byte CP1252 → boundary-safe chunk transcode.

    KNOWN SOURCE DEFECT (surfaced by the live dry-run, retained not masked):
        ContractorProdQuarterly carries 3 rows for contractor A5304 whose CONTRACTOR_NAME
        embeds a literal '|' ("AZZ|Central Electric"). Under the mandatory quote='' recipe an
        interior delimiter shifts those 3 rows one field rightward — so CAL_YR and
        AVG_EMPLOYEE_CNT try_cast to NULL on exactly those 3 rows (0.0002%). try_cast absorbs
        it: ZERO rows dropped, blast radius = one contractor's 3 quarterly-subunit rows. A
        bespoke RFC-4180 repair would re-break the interior-quote files, so the proven recipe
        is kept and the defect is documented rather than special-cased.

TRANSFORM (100% DuckDB). read_csv(all_varchar=true) → typed projection: every column
    retained losslessly as nullif(trim(BOTH '"' FROM col), ''); load-bearing date/numeric
    columns additionally cast. Cast targets were chosen from a live decimal/parse scan
    (dry-run, /tmp/msha_dryrun_extensions.py) — every target below verified at ZERO parse
    failures except the documented 3-row A5304 shift. EVERY id stays VARCHAR (alpha prefixes
    / leading zeros are significant: CONTRACTOR_ID 'A5304'/'1AD'; MINE_ID 7-char zero-padded;
    DOCUMENT_NO 12-digit). Time-of-day fields (ACCIDENT_TIME, SHIFT_BEGIN_TIME) stay VARCHAR
    (HHMM, leading zeros). money/production/hours/experience→DOUBLE; counts/years/quarters→
    INTEGER; event dates→DATE.

    msha_contractors is a UNION ALL BY NAME of the two contractor projections: DuckDB aligns
    the 6 shared columns by name and NULL-fills each side's non-overlapping columns
    (Quarterly-only: CAL_QTR/FISCAL_*/HOURS_WORKED/COAL_PRODUCTION; Yearly-only: CALENDAR_YR/
    ANNUAL_*/AVG_EMPLOYEE_HOURS). Result = 18 cols, 1,630,676 rows (1,350,534 + 280,142, no
    drop, no fan-out). source_file ('ContractorProd{Quarterly,Yearly}.zip') is the period
    discriminator. EFFECTIVE GRAIN = (CONTRACTOR_ID, period, SUBUNIT_CD, COAL_METAL_IND): the
    directive's "one row per CONTRACTOR_ID per reporting period" rolls up over SUBUNIT_CD ×
    COAL_METAL_IND, which the firmographic master preserves rather than aggregates away.

    msha_accidents is a single-file projection; DOCUMENT_NO is unique (273,065 distinct /
    273,065 rows, 0 null — verified live) so the directive's per-DOCUMENT_NO grain is exact.

INDEXING (every load-bearing resolution key gets a scalar index — ARCHITECTURE §4):
    msha_contractors   BTREE CONTRACTOR_ID; BITMAP COAL_METAL_IND, SUBUNIT_CD.
                       NOTE: the contractor sources carry NO state / FIPS / geographic
                       column (confirmed live) — the directive's "BITMAP on state/geographic"
                       is unsatisfiable from source; COAL_METAL_IND + SUBUNIT_CD are the only
                       low-cardinality categoricals present and are indexed in its place.
    msha_accidents     BTREE DOCUMENT_NO (grain) + MINE_ID + the corporate-namespace ids
                       (CONTROLLER_ID, OPERATOR_ID, CONTRACTOR_ID) + ACCIDENT_DT (temporal
                       distress-trigger range scans); BITMAP DEGREE_INJURY_CD (severity),
                       CLASSIFICATION_CD + ACCIDENT_TYPE_CD (classification), FIPS_STATE_CD
                       (geographic), COAL_METAL_IND.

CONTROL PLANE (Trigger v4 durable callback): identical to the base worker — accepts
    trigger_callback_url, writes the run row to ops.msha_ingest_runs (feed='msha') via
    psycopg on terminal state, and POSTs a FLAT JSON body to wake the suspended run.

    modal run    pipelines/ingest_msha/materialize_msha_extensions.py::run            # both
    modal run    pipelines/ingest_msha/materialize_msha_extensions.py::run --only msha_accidents
    modal run    pipelines/ingest_msha/materialize_msha_extensions.py::verify         # read-back proof
    modal run    pipelines/ingest_msha/materialize_msha_extensions.py::reindex_only   # rebuild indexes
    modal deploy pipelines/ingest_msha/materialize_msha_extensions.py                 # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

from core.name_norm import name_norm  # canonical blocking-key macro (write-time _norm keys)

BUCKET = "data-sink"
LANDING_PREFIX = os.environ.get("MSHA_LANDING_PREFIX", "landing/msha").strip("/") + "/"
FEED = "msha"
SCRATCH_DIR = "/tmp/msha_ext"

_ACTIVE = "s3://data-sink/active"

# Directive-26 canonical read recipe — verbatim parity with materialize_msha.py. quote=''
# is MANDATORY (interior quotes unescaped); read is always utf-8 (worker transcodes first).
READ_RECIPE = (
    r"delim='|', quote='', header=true, all_varchar=true, "
    r"new_line='\r\n', strict_mode=false, encoding='utf-8'"
)

# Lance fragment sizing + format — fleet defaults (02_lancedb_storage.md §2.3).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
READ_BATCH_ROWS = 131072

# ── Per-source typed-cast maps (keyed by NATIVE column name). Everything NOT listed is
# retained losslessly as dequoted VARCHAR. Validated by a live decimal/parse scan (zero
# parse failures bar the documented 3-row A5304 shift). IDs absent → stay VARCHAR. ──
CASTS: dict[str, dict[str, str]] = {
    # Shared across ContractorProdQuarterly + ContractorProdYearly (applied to BOTH so the
    # UNION ALL BY NAME reconciles types on the 6 shared columns and the NULL-filled sides).
    "contractor": {
        "CAL_YR": "INTEGER", "CAL_QTR": "INTEGER", "FISCAL_YR": "INTEGER",
        "FISCAL_QTR": "INTEGER", "CALENDAR_YR": "INTEGER", "AVG_EMPLOYEE_CNT": "INTEGER",
        "HOURS_WORKED": "DOUBLE", "COAL_PRODUCTION": "DOUBLE",
        "ANNUAL_COAL_PRODUCTION": "DOUBLE", "ANNUAL_HOURS": "DOUBLE",
        "AVG_EMPLOYEE_HOURS": "DOUBLE",
    },
    "Accidents.zip": {
        "ACCIDENT_DT": "DATE", "RETURN_TO_WORK_DT": "DATE", "INVEST_BEGIN_DT": "DATE",
        "CAL_YR": "INTEGER", "CAL_QTR": "INTEGER", "FISCAL_YR": "INTEGER",
        "FISCAL_QTR": "INTEGER", "NO_INJURIES": "INTEGER",
        "TOT_EXPER": "DOUBLE", "MINE_EXPER": "DOUBLE", "JOB_EXPER": "DOUBLE",
        "SCHEDULE_CHARGE": "DOUBLE", "DAYS_RESTRICT": "DOUBLE", "DAYS_LOST": "DOUBLE",
        # ID hygiene (P3/R4): normalize the lowercase-drift contractor cell (e.g. 4kk→4KK).
        "CONTRACTOR_ID": "UPPER",
    },
}

# ── Dataset specs. ``single`` = one source file. ``union`` = UNION ALL BY NAME of N source
# files sharing one cast map (name-aligned, NULL-filled, lossless). ──
DATASETS: dict[str, dict] = {
    "msha_contractors": {
        "uri": os.environ.get("MSHA_CONTRACTORS_URI", f"{_ACTIVE}/msha_contractors/"),
        "kind": "union",
        "sources": ["ContractorProdQuarterly.zip", "ContractorProdYearly.zip"],
        "cast_key": "contractor",
    },
    "msha_accidents": {
        "uri": os.environ.get("MSHA_ACCIDENTS_URI", f"{_ACTIVE}/msha_accidents/"),
        "kind": "single",
        "source": "Accidents.zip",
        "cast_key": "Accidents.zip",
    },
}

# Entity legal-name keys normalized at write-time (Directive-29 isolation exception, parity
# with the base worker). Each gets a persisted ``<COL>_norm`` sibling via core.name_norm.
NORM_COLS: dict[str, list[str]] = {
    "msha_contractors": ["CONTRACTOR_NAME"],
    "msha_accidents": ["CONTROLLER_NAME", "OPERATOR_NAME"],
}

# Scalar index plan. BTREE = high-cardinality resolution / range-scan keys; BITMAP =
# low-cardinality categoricals frequently filtered. BTREE lists now also carry the RAW
# entity-name keys; every ``<COL>_norm`` is appended from NORM_COLS below so a normalized
# column can never ship unindexed.
INDEX_PLAN: dict[str, dict[str, list[str]]] = {
    "msha_contractors": {
        "BTREE": ["CONTRACTOR_ID", "CONTRACTOR_NAME"],
        # No STATE/FIPS column exists in the contractor sources (live-confirmed) — these are
        # the only low-cardinality categoricals available.
        "BITMAP": ["COAL_METAL_IND", "SUBUNIT_CD"],
    },
    "msha_accidents": {
        "BTREE": ["DOCUMENT_NO", "MINE_ID", "CONTROLLER_ID", "OPERATOR_ID",
                  "CONTRACTOR_ID", "ACCIDENT_DT", "CONTROLLER_NAME", "OPERATOR_NAME"],
        "BITMAP": ["DEGREE_INJURY_CD", "CLASSIFICATION_CD", "ACCIDENT_TYPE_CD",
                   "FIPS_STATE_CD", "COAL_METAL_IND"],
    },
}

# Every normalized key column is BTREE-indexed — derived from NORM_COLS so the two can't drift.
for _ds, _ncols in NORM_COLS.items():
    INDEX_PLAN[_ds]["BTREE"].extend(c + "_norm" for c in _ncols)

# ── ops.msha_ingest_runs DDL — verbatim mirror of the base worker (same feed-scoped ledger).
# Applied by ``init_ops`` and (defensively) before each terminal write. ──
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.msha_ingest_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed             text        NOT NULL,
    source_bucket    text        NOT NULL,
    source_prefix    text        NOT NULL,
    datasets         jsonb       NOT NULL,
    rows_total       bigint      NOT NULL DEFAULT 0,
    bytes_downloaded bigint      NOT NULL DEFAULT 0,
    status           text        NOT NULL,
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS msha_ingest_runs_feed_idx        ON ops.msha_ingest_runs (feed);
CREATE INDEX IF NOT EXISTS msha_ingest_runs_status_idx      ON ops.msha_ingest_runs (status);
CREATE INDEX IF NOT EXISTS msha_ingest_runs_recorded_at_idx ON ops.msha_ingest_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 → to_arrow_reader + UNION ALL BY NAME; <2 below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`
    "pyarrow>=17",
    "boto3>=1.35",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env(
    # Lance BTREE spill-sorter under-sizes its DataFusion pool and OOMs on high-cardinality
    # string columns over millions of rows (lance#2650). Force the in-memory sort.
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("core.name_norm")  # canonical blocking-key macro → /root/core/

app = modal.App("msha-extensions-pipelines", image=image)


# --------------------------------------------------------------------------- #
# R2 / object-store
# --------------------------------------------------------------------------- #
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
    """boto3 S3 client for R2. checksum behaviour forced to ``when_required`` (R2 rejects
    botocore's default flexible-checksum validation)."""
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


# --------------------------------------------------------------------------- #
# Acquire: R2 download → extract member → CP1252→UTF-8 transcode (Python = I/O only)
# --------------------------------------------------------------------------- #
def _download_archive(s3, archive: str, dest_dir: str) -> tuple[str, int]:
    key = LANDING_PREFIX + archive
    out = os.path.join(dest_dir, archive)
    s3.download_file(BUCKET, key, out)
    return out, os.path.getsize(out)


def _extract_member(zip_path: str, dest_dir: str) -> tuple[str, str]:
    """Extract the single data member (streamed, no path-traversal); largest member wins."""
    import shutil
    import zipfile

    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if not n.endswith("/")]
        if not members:
            raise RuntimeError(f"no members in {zip_path}")
        member = max(members, key=lambda n: zf.getinfo(n).file_size)
        base = member.rsplit("/", 1)[-1]
        out = os.path.join(dest_dir, base)
        with zf.open(member) as src, open(out, "wb") as dst:
            shutil.copyfileobj(src, dst, length=16 << 20)
    return out, base


def _transcode_cp1252_to_utf8(src: str, dst: str, chunk: int = 1 << 20) -> int:
    """Rewrite src (decoded CP1252) to UTF-8 at dst. cp1252 is single-byte → chunk
    boundaries are safe; undefined positions → U+FFFD (errors='replace'). Returns bytes."""
    written = 0
    with open(src, "rb") as i, open(dst, "wb") as o:
        while (buf := i.read(chunk)):
            out = buf.decode("cp1252", errors="replace").encode("utf-8")
            o.write(out)
            written += len(out)
    return written


def _acquire(s3, archive: str, dest_dir: str) -> tuple[str, int]:
    """R2 .zip → extract member → transcode to UTF-8 scratch. Drops the .zip and the raw
    member as soon as consumed. Returns (utf8_path, zip_bytes)."""
    zip_path, zip_bytes = _download_archive(s3, archive, dest_dir)
    print(f"    downloaded s3://{BUCKET}/{LANDING_PREFIX}{archive} ({zip_bytes/1024**2:.1f} MiB)")
    raw_path, member = _extract_member(zip_path, dest_dir)
    _cleanup(zip_path)
    utf8_path = raw_path + ".utf8.txt"
    nbytes = _transcode_cp1252_to_utf8(raw_path, utf8_path)
    _cleanup(raw_path)
    print(f"    extracted {member} → transcoded CP1252→UTF-8 ({nbytes/1024**2:.1f} MiB)")
    return utf8_path, zip_bytes


# --------------------------------------------------------------------------- #
# Transform — DuckDB typed projection (lossless retain + load-bearing casts)
# --------------------------------------------------------------------------- #
def _q(ident: str) -> str:
    """Double-quote a SQL identifier (preserves the source's exact UPPERCASE names)."""
    return '"' + ident.replace('"', '""') + '"'


def _lit(s: str) -> str:
    return s.replace("'", "''")


def _base_expr(qualified: str) -> str:
    """Dequote (strip wrapping ") + empty→NULL — applied to EVERY column before any cast."""
    return f"nullif(trim(BOTH '\"' FROM {qualified}), '')"


def _cast_expr(qualified: str, cast: str | None) -> str:
    base = _base_expr(qualified)
    if cast == "DATE":
        return f"CAST(try_strptime({base}, '%m/%d/%Y') AS DATE)"
    if cast == "INTEGER":
        return f"try_cast({base} AS INTEGER)"
    if cast == "DOUBLE":
        return f"try_cast({base} AS DOUBLE)"
    if cast == "UPPER":
        # ID hygiene (P3/R4): fold lowercase-drift ID cells into the uppercase namespace.
        # No-op on already-upper / all-digit IDs (preserves leading zeros).
        return f"upper({base})"
    return base  # VARCHAR passthrough (lossless)


def _describe(con, path: str) -> list[str]:
    """Header-derived column names DuckDB emits for a source (never guessed). The MSHA
    header is unquoted, so names come through clean (CONTRACTOR_ID, DOCUMENT_NO, …)."""
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_csv('{_lit(path)}', {READ_RECIPE})").fetchall()
    return [r[0] for r in rows]


def _src(path: str, alias: str) -> str:
    return f"(SELECT * FROM read_csv('{_lit(path)}', {READ_RECIPE})) AS {alias}"


def _provenance(source_label: str) -> str:
    return f"    '{_lit(source_label)}' AS source_file,\n    now() AS ingested_at"


def _projection_sql(path: str, archive_label: str, cols: list[str], cast_key: str,
                    alias: str = "s") -> str:
    """Lossless typed projection for one source file (native names verbatim + provenance)."""
    casts = CASTS.get(cast_key, {})
    proj = ",\n    ".join(
        f"{_cast_expr(alias + '.' + _q(c), casts.get(c))} AS {_q(c)}" for c in cols)
    return f"SELECT\n    {proj},\n{_provenance(archive_label)}\nFROM {_src(path, alias)}"


def _union_sql(parts: list[str]) -> str:
    """UNION ALL BY NAME of N typed projections. BY NAME aligns the shared columns and
    NULL-fills each side's non-overlapping columns (verbatim names, types reconciled)."""
    return "\nUNION ALL BY NAME\n".join(f"({p})" for p in parts)


def _with_norm(inner_sql: str, norm_cols: list[str]) -> str:
    """Wrap a typed projection, appending ``core.name_norm(col) AS col_norm`` for each
    entity legal-name key. Applied to the already-dequoted aliased column, so ``col_norm``
    is byte-identical to name_norm over the raw dequoted source and NULL-safe. Raw columns
    are preserved verbatim; the _norm siblings are appended at the end of the schema."""
    if not norm_cols:
        return inner_sql
    extras = ",\n    ".join(
        f"{name_norm(_q(c))} AS {_q(c + '_norm')}" for c in norm_cols)
    return f"SELECT base.*,\n    {extras}\nFROM (\n{inner_sql}\n) AS base"


def _spine_count(con, path: str) -> int:
    """Authoritative input-grain count for one source (the no-drop anchor)."""
    return con.execute(
        f"SELECT count(*) FROM read_csv('{_lit(path)}', {READ_RECIPE})").fetchone()[0]


# --------------------------------------------------------------------------- #
# Lance — write + index (direct-R2, in place; under the Giants threshold)
# --------------------------------------------------------------------------- #
def _write_lance(reader, uri: str, so: dict) -> None:
    import lance

    lance.write_dataset(
        reader, uri,
        schema=reader.schema,           # REQUIRED when the source is a streaming reader
        mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )


def _create_indexes(ds_name: str, uri: str, so: dict) -> list[dict]:
    """Build BTREE + BITMAP scalar indexes in place on R2 (replace=True → idempotent).
    Best-effort per index (a miss never fails an otherwise-good load) but logged + recorded."""
    import lance

    ds = lance.dataset(uri, storage_options=so)
    out: list[dict] = []
    for itype in ("BTREE", "BITMAP"):
        for col in INDEX_PLAN[ds_name].get(itype, []):
            try:
                ds.create_scalar_index(col, index_type=itype)
                print(f"    {itype:6s} ✓ {ds_name}.{col}")
                out.append({"col": col, "type": itype, "ok": True})
            except Exception as exc:  # noqa: BLE001
                print(f"    {itype:6s} ✗ {ds_name}.{col}: {exc}")
                out.append({"col": col, "type": itype, "ok": False, "error": str(exc)[:200]})
    return out


def _committed_index_names(uri: str, so: dict) -> list[str]:
    import lance

    ds = lance.dataset(uri, storage_options=so)
    names = []
    for ix in ds.list_indices():
        names.append(ix.get("name", str(ix)) if isinstance(ix, dict)
                     else getattr(ix, "name", str(ix)))
    return sorted(names)


# --------------------------------------------------------------------------- #
# State + callback + cleanup
# --------------------------------------------------------------------------- #
def _cleanup(*paths: str) -> None:
    import shutil

    for p in paths:
        if not p:
            continue
        try:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            elif os.path.exists(p):
                os.remove(p)
        except OSError as exc:
            print(f"    WARN: cleanup of {p} failed: {exc}")


def _record_run(*, source_prefix, datasets, rows_total, bytes_downloaded,
                status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.msha_ingest_runs (psycopg). Best-effort: an audit-write
    failure never crashes an otherwise-good materialization."""
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.msha_ingest_runs
                    (feed, source_bucket, source_prefix, datasets, rows_total,
                     bytes_downloaded, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, BUCKET, source_prefix, Jsonb(datasets), rows_total,
                 bytes_downloaded, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url. FLAT JSON body."""
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


# --------------------------------------------------------------------------- #
# Per-dataset materialization
# --------------------------------------------------------------------------- #
def _materialize_one(con, s3, ds_name: str, so: dict) -> dict:
    """Acquire source(s) → DuckDB typed projection (streamed) → Lance overwrite on R2 →
    scalar indexes. Verifies committed Lance rows == summed spine input (no-drop / no-fan-out)."""
    spec = DATASETS[ds_name]
    uri = spec["uri"]
    print(f"\n=== {ds_name}  →  {uri} ===")

    utf8_paths: list[str] = []
    zip_bytes_total = 0
    try:
        if spec["kind"] == "single":
            archive = spec["source"]
            path, zb = _acquire(s3, archive, SCRATCH_DIR)
            utf8_paths.append(path)
            zip_bytes_total += zb
            cols = _describe(con, path)
            spine_rows = _spine_count(con, path)
            sql = _projection_sql(path, archive, cols, spec["cast_key"])
            source_archives = [archive]

        elif spec["kind"] == "union":
            parts: list[str] = []
            spine_rows = 0
            source_archives = list(spec["sources"])
            for idx, archive in enumerate(spec["sources"]):
                path, zb = _acquire(s3, archive, SCRATCH_DIR)
                utf8_paths.append(path)
                zip_bytes_total += zb
                cols = _describe(con, path)
                spine_rows += _spine_count(con, path)  # grain anchor = Σ of every input
                parts.append(_projection_sql(path, archive, cols, spec["cast_key"],
                                             alias=f"s{idx}"))
            sql = _union_sql(parts)

        else:
            raise ValueError(f"unknown kind {spec['kind']!r} for {ds_name}")

        sql = _with_norm(sql, NORM_COLS.get(ds_name, []))
        reader = con.execute(sql).to_arrow_reader(READ_BATCH_ROWS)
        n_cols = len(reader.schema)
        _write_lance(reader, uri, so)

        import lance
        lance_rows = lance.dataset(uri, storage_options=so).count_rows()
        grain_ok = lance_rows == spine_rows
        flag = "OK" if grain_ok else "!!!! GRAIN MISMATCH"
        print(f"    wrote {lance_rows:,} rows × {n_cols} cols  "
              f"(spine {spine_rows:,}) → {flag}")
        if not grain_ok:
            print(f"    WARN: {ds_name} committed {lance_rows:,} rows but spine input is "
                  f"{spine_rows:,} — investigate before trusting this dataset.")

        indexes = _create_indexes(ds_name, uri, so)
        return {
            "uri": uri, "source_archives": source_archives, "kind": spec["kind"],
            "spine_rows": int(spine_rows), "lance_rows": int(lance_rows),
            "n_cols": int(n_cols), "grain_ok": bool(grain_ok),
            "zip_bytes": int(zip_bytes_total), "indexes": indexes,
        }
    finally:
        _cleanup(*utf8_paths)


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 2,    # R2 pull + transcode + 1.6M-row union + 273K accidents + index
    memory=32768,           # 32 GiB — comfortable headroom for the contractor union
    cpu=8.0,
    ephemeral_disk=524288,  # 512 GiB Modal floor; ≫ the ~0.5 GiB transcoded working set
)
def ingest_msha_extensions(only: str = "", trigger_callback_url: str | None = None) -> dict:
    """Materialize msha_contractors + msha_accidents from the R2 landing zone. Records
    ops.msha_ingest_runs and wakes the Trigger run on terminal state. ``only`` restricts to
    one dataset. Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt

    import duckdb
    import lance  # noqa: F401 — import early so a missing dep fails fast, not mid-write

    started_at = dt.datetime.now(dt.timezone.utc)
    targets = [only] if only else list(DATASETS)
    for t in targets:
        if t not in DATASETS:
            raise ValueError(f"unknown dataset {t!r}; expected one of {list(DATASETS)}")

    os.makedirs(SCRATCH_DIR, exist_ok=True)
    os.makedirs(os.path.join(SCRATCH_DIR, "duckdb_spill"), exist_ok=True)

    detail: dict[str, dict] = {}
    status, error = "error", None

    try:
        so = _r2_storage_options()
        s3 = _s3_client()
        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
            for ds_name in targets:
                detail[ds_name] = _materialize_one(con, s3, ds_name, so)
        finally:
            con.close()
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        _cleanup(SCRATCH_DIR)
        completed_at = dt.datetime.now(dt.timezone.utc)
        rows_total = int(sum(d.get("lance_rows", 0) for d in detail.values()))
        bytes_dl = int(sum(d.get("zip_bytes", 0) for d in detail.values()))
        _record_run(source_prefix=LANDING_PREFIX, datasets=detail, rows_total=rows_total,
                    bytes_downloaded=bytes_dl, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url, {
            "status": status, "feed": FEED, "rows_total": rows_total,
            "datasets": {k: v.get("lance_rows") for k, v in detail.items()},
        })

    if status != "success":
        raise RuntimeError(f"msha extensions materialization failed: {error}")
    return {"feed": FEED, "rows_total": rows_total, "datasets": detail, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 45, memory=32768, cpu=8.0,
)
def reindex(only: str = "") -> dict:
    """(Re)build scalar indexes on already-written datasets without re-materializing."""
    so = _r2_storage_options()
    targets = [only] if only else list(DATASETS)
    out = {}
    for ds_name in targets:
        if ds_name not in INDEX_PLAN:
            continue
        print(f"=== reindex {ds_name} ===")
        _create_indexes(ds_name, DATASETS[ds_name]["uri"], so)
        out[ds_name] = _committed_index_names(DATASETS[ds_name]["uri"], so)
    return out


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15)
def verify_datasets(only: str = "") -> dict:
    """Read-back proof: open each committed Lance dataset from R2 and report row count,
    non-null counts on the indexed keys, and committed indices. Reads what actually landed."""
    import lance

    so = _r2_storage_options()
    targets = [only] if only else list(DATASETS)
    out: dict = {}
    for ds_name in targets:
        uri = DATASETS[ds_name]["uri"]
        info: dict = {"dataset_uri": uri}
        try:
            ds = lance.dataset(uri, storage_options=so)
            info["rows"] = ds.count_rows()
            # Lance filter parser: column refs MUST be bare (double-quoted tokens are STRING
            # literals). Every MSHA column is [A-Z0-9_], safe as a bare identifier.
            for col in INDEX_PLAN[ds_name]["BTREE"] + INDEX_PLAN[ds_name]["BITMAP"]:
                try:
                    info[f"{col}__non_null"] = ds.count_rows(filter=f"{col} IS NOT NULL")
                except Exception as exc:  # noqa: BLE001
                    info[f"{col}__non_null"] = f"err: {str(exc)[:80]}"
            info["indices"] = _committed_index_names(uri, so)
        except Exception as exc:  # noqa: BLE001
            info["error"] = str(exc)[:200]
        out[ds_name] = info
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.msha_ingest_runs (idempotent). Mirrors the base worker's canonical .sql."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name='msha_ingest_runs'
            ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    print(f"ops.msha_ingest_runs ready — columns: {cols}")
    return {"table": "ops.msha_ingest_runs", "columns": cols}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 10) -> list:
    """Read the most recent ops.msha_ingest_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, rows_total, bytes_downloaded, status, error, "
            "started_at, completed_at, datasets "
            "FROM ops.msha_ingest_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# --------------------------------------------------------------------------- #
# Manual ops entrypoints (local — no Trigger callback). ops.* write still fires.
# --------------------------------------------------------------------------- #
@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.msha_ingest_runs DDL (idempotent)."""
    import json

    print(json.dumps(apply_ops_ddl.remote(), indent=2, default=str))


@app.local_entrypoint()
def run(only: str = "") -> None:
    """Materialize both datasets (or one via --only). No Trigger callback; ops.* fires."""
    import json

    print(json.dumps(
        ingest_msha_extensions.remote(only=only, trigger_callback_url=None),
        indent=2, default=str))


@app.local_entrypoint()
def reindex_only(only: str = "") -> None:
    """Rebuild scalar indexes on the existing datasets (no re-materialization)."""
    import json

    print(json.dumps(reindex.remote(only=only), indent=2, default=str))


@app.local_entrypoint()
def verify(only: str = "") -> None:
    """Read-back proof of the committed datasets (rows, key non-nulls, indices)."""
    import json

    print(json.dumps(verify_datasets.remote(only=only), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 10) -> None:
    """Print the most recent ops.msha_ingest_runs rows."""
    import json

    print(json.dumps(ledger.remote(limit), indent=2, default=str))
