"""Compute worker — MSHA isolated materialization (Directive 29).

Part of the ``msha-pipelines`` Modal app. Endpoint-less; spawned by the Universal
Dispatcher (core/modal_dispatcher.py), the only proxy-authed endpoint in the fleet, or
driven by the local entrypoints. Clean-room data plane: no Iceberg, no Polaris — DuckDB
does 100% of the transform, Lance is written straight to R2.

WHAT THIS WORKER DOES
    Extracts the raw MSHA (Mine Safety & Health Administration) Open-Government-Data
    archives staged in the R2 landing zone (Directive 26 = recon; this = ingest) and
    materializes three clean, typed, isolated Lance datasets keyed on MSHA's own native
    primary keys. Source archives are already in R2 — the worker pulls .zip from R2,
    never the web.

    SOURCE (R2, profiled in docs/reference/MSHA_DATA_PROFILING_REPORT.md):
        s3://data-sink/landing/msha/<Archive>.zip      (20 single-member ZIP/DEFLATE
        archives, pipe-delimited, quoted values, unquoted header, CRLF, Windows-1252)

    TARGET (Gen-3 system of record — native Lance v2.1, full-snapshot overwrite):
        s3://data-sink/active/msha_mines/              Mines ⟕ AddressOfRecord on MINE_ID
        s3://data-sink/active/msha_corporate_history/  ControllerOperatorHistory (SCD)
        s3://data-sink/active/msha_enforcement_ledger/ Violations ⟕ AssessedViolations
                                                       on VIOLATION_NO

ENTITY-NAME NORMALIZATION (Directive-29 isolation exception — AUTHORIZED).
    The records are still landed on MSHA's own native keys — NOT row-level cross-walked or
    JOINed to sos_normalized_master / companies / PPP / SBA at ingest. The Directive-29
    "no name_norm" guardrail is LIFTED for crosswalk readiness: each entity legal-name key
    (operator / controller / business / violator name) gets a persisted ``<COL>_norm``
    sibling computed at write-time via core.name_norm — the single-source-of-truth
    blocking-key macro. Downstream bridges then exact-join the SoS spine's BTREE blocking
    key against a STORED column, never a read-time ``name_norm(col)`` wrapper (which is
    structurally non-indexable → full scan; proven in
    docs/reference/MSHA_INDEX_TOPOLOGY_PUSHDOWN_DIAGNOSTIC.md). Asset/office/equipment names
    (MINE_NAME, OFFICE_NAME, EQUIP_MFR_NAME) are NOT normalized — entity legal names only.

PARSING HYGIENE — the Directive-26 recipe (two correctness traps, both load-bearing).
    Verified live against all five source files (zero row drop, exact recon counts):
    (a) Unescaped interior double-quotes in free text (DIRECTIONS_TO_MINE, etc.) make a
        strict RFC-4180 read collapse — a naive quote='"' read drops 2–4% of the giant
        ledgers and errors out on the small files. The ONLY correct parse is
        ``quote=''`` (quote processing disabled): the wrapping quotes survive as literal
        text and are stripped per-field with ``trim(BOTH '"' FROM col)``.
    (b) Encoding is Windows-1252, NOT UTF-8 (degree signs in GPS strings; ñ/ó/é in
        Puerto-Rico operator names; smart quotes/dashes). DuckDB's latin-1 reader rejects
        the 0x80–0x9F CP1252 bytes and its utf-8 reader rejects the high bytes, so the
        worker transcodes CP1252→UTF-8 to scratch BEFORE handing bytes to DuckDB; the
        DuckDB read is then always ``encoding='utf-8'``. (cp1252 is single-byte, so the
        streaming chunk transcode is boundary-safe; the five undefined CP1252 positions
        decode as U+FFFD via errors='replace' — never a dropped byte / desynced row.)

TRANSFORM (100% DuckDB). read_csv(all_varchar=true) per the recipe → typed projection:
    every column is retained losslessly as ``nullif(trim(BOTH '"' FROM col), '')``; the
    load-bearing date/numeric columns are additionally cast (``try_strptime(…,
    '%m/%d/%Y')::DATE``; ``try_cast(… AS INTEGER|DOUBLE)``) for fast range-scans. EVERY
    id stays VARCHAR (leading zeros / alpha prefixes are significant — MINE_ID is a
    7-char zero-padded VARCHAR; VIOLATOR_ID is alpha-prefixed). Cast targets (INTEGER vs
    DOUBLE) were chosen from a live decimal/parse scan: money/geo/hours→DOUBLE,
    counts/points→INTEGER, every date→DATE — all with zero parse failures.

    The two consolidations join two MSHA files on a NATIVE MSHA key (within-universe,
    not bridging). Both right sides are unique on the join key (verified:
    AddressOfRecord 1:1 on MINE_ID; AssessedViolations 1:1 on VIOLATION_NO, every
    assessment VIOLATION_NO ∈ Violations), so a LEFT JOIN from the spine preserves the
    spine grain EXACTLY (msha_mines = 91,803; msha_enforcement_ledger = 3,076,347) and
    drops zero signal. Colliding right-side columns are namespaced (ADDR_*, ASMT_*) to
    keep the projection lossless; the join key is dropped from the right side.

SCALE & WRITE PATH. The two giants (Violations 1.43 GB, AssessedViolations 1.32 GB
    uncompressed) are well under the ~100M-row Volume-staging threshold (recon §7), so
    the fleet-default DIRECT-R2 path applies: DuckDB streams the projection
    (``to_arrow_reader`` — bounded RSS) → ``lance.write_dataset(s3://…, overwrite,
    storage_options=so)`` → BTREE/BITMAP scalar indexes built in place on R2. No local
    stage, no boto3 dataset publish (that is the Giants rule, not needed here).

INDEXING (every load-bearing resolution key gets a scalar index — ARCHITECTURE §4):
    msha_mines              BTREE MINE_ID(anchor) + CURRENT_CONTROLLER_ID/OPERATOR_ID;
                            BITMAP COAL_METAL_IND/STATE/CURRENT_MINE_STATUS.
    msha_corporate_history  BTREE CONTROLLER_ID/OPERATOR_ID/MINE_ID;
                            BITMAP CONTROLLER_TYPE/COAL_METAL_IND.
    msha_enforcement_ledger BTREE MINE_ID+VIOLATOR_ID (the directive's mandate) +
                            VIOLATION_NO/CONTROLLER_ID/EVENT_NO/ASSESS_CASE_NO +
                            VIOLATION_ISSUE_DT/PROPOSED_PENALTY_AMT (the severity/temporal
                            range-scan columns); BITMAP SIG_SUB/CIT_ORD_SAFE/
                            VIOLATOR_TYPE_CD/COAL_METAL_IND.
    PLUS (resolution spine)  BTREE on every raw entity-name key, its <COL>_norm sibling, and
                            ZIP_CD (mines) — so raw point lookups, normalized cross-registry
                            joins, and ZIP geo-blocking all bind a scalar index instead of
                            full-scanning. Built from NORM_COLS + the raw/ZIP additions in
                            INDEX_PLAN below.

CONTROL PLANE (Trigger v4 durable callback): the worker accepts ``trigger_callback_url``
    and, on terminal state (success OR failure), (1) writes the run row to
    ``ops.msha_ingest_runs`` via psycopg and (2) POSTs a FLAT JSON body to that url to
    wake the suspended Trigger run. No ``{"data": ...}`` envelope.

    modal run    pipelines/ingest_msha/materialize_msha.py::init_ops      # create ops table
    modal run    pipelines/ingest_msha/materialize_msha.py::run           # materialize all three
    modal run    pipelines/ingest_msha/materialize_msha.py::run --only msha_mines
    modal run    pipelines/ingest_msha/materialize_msha.py::verify        # read-back proof
    modal run    pipelines/ingest_msha/materialize_msha.py::reindex_only  # rebuild indexes
    modal run    pipelines/ingest_msha/materialize_msha.py::show_ledger
    modal deploy pipelines/ingest_msha/materialize_msha.py                # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

from core.name_norm import name_norm  # canonical blocking-key macro (write-time _norm keys)

BUCKET = "data-sink"
LANDING_PREFIX = os.environ.get("MSHA_LANDING_PREFIX", "landing/msha").strip("/") + "/"
FEED = "msha"
SCRATCH_DIR = "/tmp/msha"

_ACTIVE = "s3://data-sink/active"

# The Directive-26 canonical read recipe (covers every target file verbatim). quote=''
# is MANDATORY (interior quotes are unescaped); the read is always utf-8 because the
# worker transcodes CP1252→UTF-8 first. RAW string so the SQL carries the two-char
# escape ``\r\n`` (not an actual CRLF, which DuckDB rejects as a newline option value).
READ_RECIPE = (
    r"delim='|', quote='', header=true, all_varchar=true, "
    r"new_line='\r\n', strict_mode=false, encoding='utf-8'"
)

# Lance fragment sizing + format — fleet defaults (02_lancedb_storage.md §2.3).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3   # 90 GiB — Lance's documented default; never shatters these sets
DATA_STORAGE_VERSION = "2.1"        # net-new datasets pin the current Lance default
READ_BATCH_ROWS = 131072            # streaming reader batch — the 3M-row giant never materializes whole

# ── Per-source typed-cast maps (keyed by NATIVE column name). Everything NOT listed is
# retained losslessly as dequoted VARCHAR. Cast targets validated by a live decimal/parse
# scan (zero parse failures): money/geo/hours→DOUBLE, counts/points→INTEGER, dates→DATE.
# IDs are deliberately absent → they stay VARCHAR (leading-zero / alpha-prefix safety). ──
CASTS: dict[str, dict[str, str]] = {
    "Mines.zip": {
        "CURRENT_STATUS_DT": "DATE", "CURRENT_CONTROLLER_BEGIN_DT": "DATE", "CURRENT_103I_DT": "DATE",
        "NO_EMPLOYEES": "INTEGER", "LONGITUDE": "DOUBLE", "LATITUDE": "DOUBLE",
    },
    "AddressofRecord.zip": {
        "MINE_STATUS_DT": "DATE",
    },
    "ControllerOperatorHistory.zip": {
        "CONTROLLER_START_DT": "DATE", "CONTROLLER_END_DT": "DATE",
        "OPERATOR_START_DT": "DATE", "OPERATOR_END_DT": "DATE",
    },
    "Violations.zip": {
        "INSPECTION_BEGIN_DT": "DATE", "INSPECTION_END_DT": "DATE", "VIOLATION_ISSUE_DT": "DATE",
        "VIOLATION_OCCUR_DT": "DATE", "ORIG_TERM_DUE_DT": "DATE", "LATEST_TERM_DUE_DT": "DATE",
        "TERMINATION_DT": "DATE", "VACATE_DT": "DATE", "RIGHT_TO_CONF_DT": "DATE",
        "FINAL_ORDER_ISSUE_DT": "DATE", "BILL_PRINT_DT": "DATE", "LAST_ACTION_DT": "DATE",
        "CONTESTED_DT": "DATE",
        "NO_AFFECTED": "INTEGER", "VIOLATOR_VIOLATION_CNT": "INTEGER",
        "VIOLATOR_INSPECTION_DAY_CNT": "INTEGER",
        "PROPOSED_PENALTY": "DOUBLE", "AMOUNT_DUE": "DOUBLE", "AMOUNT_PAID": "DOUBLE",
    },
    "AssessedViolations.zip": {
        "ASSESS_CASE_STATUS_DT": "DATE", "OCCURRENCE_DT": "DATE", "ISSUE_DT": "DATE",
        "FINAL_ORDER_DT": "DATE", "BILL_PRINT_DT": "DATE", "DELINQUENT_DT": "DATE",
        "HISTORY_START_DT": "DATE", "HISTORY_END_DT": "DATE",
        "VIOLATOR_START_DT": "DATE", "VIOLATOR_END_DT": "DATE",
        "PROPOSED_PENALTY_AMT": "DOUBLE", "CURRENT_ASSESSMENT_AMT": "DOUBLE",
        "MINEACT_INTEREST_AMT": "DOUBLE", "EXLATE_INTEREST_AMT": "DOUBLE",
        "PAID_PROPOSED_PENALTY_AMT": "DOUBLE", "PAID_MINEACT_INTEREST_AMT": "DOUBLE",
        "PAID_EXLATE_INTEREST_AMT": "DOUBLE", "VIOLATOR_MINE_HRS": "DOUBLE",
        "VIOLATOR_PRODUCTION_AMT": "DOUBLE", "CONTROLLER_HRS": "DOUBLE",
        "CONTROLLER_PRODUCTION_AMT": "DOUBLE",
        "PENALTY_POINTS": "INTEGER", "GRAVITY_PERSONS_POINTS": "INTEGER",
        "GRAVITY_INJURY_POINTS": "INTEGER", "GRAVITY_LIKELIHOOD_POINTS": "INTEGER",
        "NEGLIGENCE_POINTS": "INTEGER", "CONTRACTOR_SIZE_POINTS": "INTEGER",
        "GOOD_FAITH_POINTS": "INTEGER", "VIOLATION_PER_INSP_DAY_POINTS": "INTEGER",
        "VIOLATOR_REPEATED_VIOL_POINTS": "INTEGER", "MINE_SIZE_POINTS": "INTEGER",
        "CONTROLLER_SIZE_POINTS": "INTEGER", "VIOLATOR_VIOLATION_CNT": "INTEGER",
        "VIOLATOR_INSPECTION_DAY_CNT": "INTEGER", "VIOLATOR_REPEATED_VIOL_CNT": "INTEGER",
    },
}

# ── Dataset specs. ``single`` = one source file; ``join`` = LEFT JOIN spine ⟕ companion
# on a native MSHA key (right side verified unique on the key → grain == spine). Colliding
# right columns are namespaced with ``right_prefix``; the join key is dropped right-side. ──
DATASETS: dict[str, dict] = {
    "msha_mines": {
        "uri": os.environ.get("MSHA_MINES_URI", f"{_ACTIVE}/msha_mines/"),
        "kind": "join",
        "left": "Mines.zip", "right": "AddressofRecord.zip",
        "join_key": "MINE_ID", "right_prefix": "ADDR_",
    },
    "msha_corporate_history": {
        "uri": os.environ.get("MSHA_CORP_HISTORY_URI", f"{_ACTIVE}/msha_corporate_history/"),
        "kind": "single",
        "source": "ControllerOperatorHistory.zip",
    },
    "msha_enforcement_ledger": {
        "uri": os.environ.get("MSHA_ENFORCEMENT_URI", f"{_ACTIVE}/msha_enforcement_ledger/"),
        "kind": "join",
        "left": "Violations.zip", "right": "AssessedViolations.zip",
        "join_key": "VIOLATION_NO", "right_prefix": "ASMT_",
    },
}

# Entity legal-name keys normalized at write-time (Directive-29 isolation exception). Each
# listed column gets a persisted ``<COL>_norm`` sibling via core.name_norm — the crosswalk
# blocking key. Entity legal names ONLY (operator/controller/business/violator) — asset/
# office/equipment names are intentionally excluded.
NORM_COLS: dict[str, list[str]] = {
    "msha_mines": ["CURRENT_OPERATOR_NAME", "CURRENT_CONTROLLER_NAME", "BUSINESS_NAME"],
    "msha_corporate_history": ["OPERATOR_NAME", "CONTROLLER_NAME"],
    "msha_enforcement_ledger": ["VIOLATOR_NAME", "CONTROLLER_NAME"],
}

# Scalar index plan. BTREE = high-cardinality resolution / range-scan keys; BITMAP =
# low-cardinality categoricals frequently filtered. MINE_ID + VIOLATOR_ID on the
# enforcement ledger are the directive's hard mandate. The BTREE lists now also carry the
# RAW entity-name keys + ZIP_CD (mines); every ``<COL>_norm`` is appended programmatically
# from NORM_COLS below so a normalized column can never ship unindexed.
INDEX_PLAN: dict[str, dict[str, list[str]]] = {
    "msha_mines": {
        "BTREE": ["MINE_ID", "CURRENT_CONTROLLER_ID", "CURRENT_OPERATOR_ID",
                  "CURRENT_OPERATOR_NAME", "CURRENT_CONTROLLER_NAME", "BUSINESS_NAME", "ZIP_CD"],
        "BITMAP": ["COAL_METAL_IND", "STATE", "CURRENT_MINE_STATUS"],
    },
    "msha_corporate_history": {
        "BTREE": ["CONTROLLER_ID", "OPERATOR_ID", "MINE_ID",
                  "OPERATOR_NAME", "CONTROLLER_NAME"],
        "BITMAP": ["CONTROLLER_TYPE", "COAL_METAL_IND"],
    },
    "msha_enforcement_ledger": {
        "BTREE": ["MINE_ID", "VIOLATOR_ID", "VIOLATION_NO", "CONTROLLER_ID",
                  "EVENT_NO", "ASSESS_CASE_NO", "VIOLATION_ISSUE_DT", "PROPOSED_PENALTY_AMT",
                  "VIOLATOR_NAME", "CONTROLLER_NAME"],
        "BITMAP": ["SIG_SUB", "CIT_ORD_SAFE", "VIOLATOR_TYPE_CD", "COAL_METAL_IND"],
    },
}

# Every normalized key column is BTREE-indexed (high-cardinality resolution keys) — derived
# from NORM_COLS so the two can never drift.
for _ds, _ncols in NORM_COLS.items():
    INDEX_PLAN[_ds]["BTREE"].extend(c + "_norm" for c in _ncols)

# ── ops.msha_ingest_runs DDL — verbatim mirror of ops_msha_ingest_runs.sql. Applied by
# the ``init_ops`` entrypoint and (defensively) before each terminal write. ──
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
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_reader; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",           # R2 landing-zone read + write plumbing
    "requests>=2.32",        # Trigger waitpoint callback
    "psycopg[binary]>=3.2",  # terminal state → ops.msha_ingest_runs
).env(
    # BTREE training sorts the column; Lance's spill-to-disk sorter under-sizes its
    # DataFusion pool and OOMs on high-cardinality string columns over millions of rows
    # (lance#2650). Force the cheap in-memory sort.
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("core.name_norm")  # canonical blocking-key macro → /root/core/

app = modal.App("msha-pipelines", image=image)


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
    """Download one landing-zone .zip to scratch. Returns (path, bytes)."""
    key = LANDING_PREFIX + archive
    out = os.path.join(dest_dir, archive)
    s3.download_file(BUCKET, key, out)
    return out, os.path.getsize(out)


def _extract_member(zip_path: str, dest_dir: str) -> tuple[str, str]:
    """Extract the single data member (streamed, no path-traversal) → (path, member_name).
    The MSHA archives are single-member; the largest member wins defensively."""
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
    """Rewrite ``src`` (decoded as CP1252) to UTF-8 at ``dst``. cp1252 is single-byte so
    fixed-size chunk boundaries are safe; the five undefined CP1252 positions decode as
    U+FFFD (errors='replace') so a stray byte never drops/desyncs a row. Returns bytes."""
    written = 0
    with open(src, "rb") as i, open(dst, "wb") as o:
        while (buf := i.read(chunk)):
            out = buf.decode("cp1252", errors="replace").encode("utf-8")
            o.write(out)
            written += len(out)
    return written


def _acquire(s3, archive: str, dest_dir: str) -> tuple[str, int]:
    """R2 .zip → extract member → transcode to UTF-8 scratch. Drops the .zip and the raw
    (pre-transcode) member as soon as they are consumed. Returns (utf8_path, zip_bytes)."""
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
    """Dequote (strip the wrapping " left by quote='') + empty→NULL — applied to EVERY
    column before any type cast."""
    return f"nullif(trim(BOTH '\"' FROM {qualified}), '')"


def _cast_expr(qualified: str, cast: str | None) -> str:
    base = _base_expr(qualified)
    if cast == "DATE":
        return f"CAST(try_strptime({base}, '%m/%d/%Y') AS DATE)"
    if cast == "INTEGER":
        return f"try_cast({base} AS INTEGER)"
    if cast == "DOUBLE":
        return f"try_cast({base} AS DOUBLE)"
    return base  # VARCHAR passthrough (lossless)


def _describe(con, path: str) -> list[str]:
    """Column names DuckDB emits for a source (header-derived — never guessed). The MSHA
    header is unquoted, so names come through clean (MINE_ID, VIOLATION_ISSUE_DT, …)."""
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_csv('{_lit(path)}', {READ_RECIPE})").fetchall()
    return [r[0] for r in rows]


def _src(path: str, alias: str) -> str:
    return f"(SELECT * FROM read_csv('{_lit(path)}', {READ_RECIPE})) AS {alias}"


def _provenance(source_label: str) -> str:
    return f"    '{_lit(source_label)}' AS source_file,\n    now() AS ingested_at"


def _single_sql(path: str, archive: str, cols: list[str]) -> str:
    """Lossless typed projection for a one-file dataset."""
    casts = CASTS.get(archive, {})
    proj = ",\n    ".join(
        f"{_cast_expr('s.' + _q(c), casts.get(c))} AS {_q(c)}" for c in cols)
    return f"SELECT\n    {proj},\n{_provenance(archive)}\nFROM {_src(path, 's')}"


def _join_sql(left_path: str, left_archive: str, left_cols: list[str],
              right_path: str, right_archive: str, right_cols: list[str],
              join_key: str, right_prefix: str) -> str:
    """Lossless typed projection for spine ⟕ companion. Left columns keep native names;
    colliding right columns are namespaced with ``right_prefix``; the join key is dropped
    right-side. Join on the dequoted key (values are quote-wrapped under quote='')."""
    left_casts = CASTS.get(left_archive, {})
    right_casts = CASTS.get(right_archive, {})
    left_set = set(left_cols)

    parts = [f"{_cast_expr('l.' + _q(c), left_casts.get(c))} AS {_q(c)}" for c in left_cols]
    for c in right_cols:
        if c == join_key:
            continue
        out = (right_prefix + c) if c in left_set else c
        parts.append(f"{_cast_expr('r.' + _q(c), right_casts.get(c))} AS {_q(out)}")
    proj = ",\n    ".join(parts)

    on = f"{_base_expr('l.' + _q(join_key))} = {_base_expr('r.' + _q(join_key))}"
    label = f"{left_archive}+{right_archive}"
    return (f"SELECT\n    {proj},\n{_provenance(label)}\n"
            f"FROM {_src(left_path, 'l')}\nLEFT JOIN {_src(right_path, 'r')} ON {on}")


def _with_norm(inner_sql: str, norm_cols: list[str]) -> str:
    """Wrap a typed projection, appending ``core.name_norm(col) AS col_norm`` for each
    entity legal-name key. The macro is applied to the already-dequoted aliased column, so
    ``col_norm`` is byte-identical to name_norm over the raw dequoted source — and NULL-safe
    (the inner column is empty→NULL; name_norm(NULL)→NULL). The raw columns are preserved
    verbatim; the _norm siblings are appended at the end of the schema."""
    if not norm_cols:
        return inner_sql
    extras = ",\n    ".join(
        f"{name_norm(_q(c))} AS {_q(c + '_norm')}" for c in norm_cols)
    return f"SELECT base.*,\n    {extras}\nFROM (\n{inner_sql}\n) AS base"


def _spine_count(con, path: str) -> int:
    """Authoritative input-grain count for the spine source (the no-drop anchor)."""
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
    """Build BTREE + BITMAP scalar indexes in place on R2. create_scalar_index defaults to
    replace=True → idempotent. Best-effort per index (an index miss must never fail an
    otherwise-good load) but logged loudly and recorded in ops.*."""
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
    """Remove scratch files/dirs (best-effort)."""
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
    """Terminal run row → ops.msha_ingest_runs (psycopg). Best-effort: never let an
    audit-write failure crash an otherwise-good materialization."""
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
    """POST the RAW terminal payload to the Trigger waitpoint url. FLAT JSON body — no
    {"data": ...} envelope, no API key (the callbackHash in the url is the auth)."""
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
    scalar indexes. Returns the per-dataset detail dict for ops.*. Verifies the committed
    Lance row count equals the spine input count (the no-drop / no-fan-out proof)."""
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
            sql = _single_sql(path, archive, cols)
            source_archives = [archive]
        else:
            la, ra = spec["left"], spec["right"]
            lpath, lzb = _acquire(s3, la, SCRATCH_DIR)
            rpath, rzb = _acquire(s3, ra, SCRATCH_DIR)
            utf8_paths += [lpath, rpath]
            zip_bytes_total += lzb + rzb
            lcols, rcols = _describe(con, lpath), _describe(con, rpath)
            spine_rows = _spine_count(con, lpath)  # grain anchor = the LEFT spine
            sql = _join_sql(lpath, la, lcols, rpath, ra, rcols,
                            spec["join_key"], spec["right_prefix"])
            source_archives = [la, ra]

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
        _cleanup(*utf8_paths)  # free scratch before the next dataset


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 3,    # R2 pull + CP1252 transcode + 3M-row join + index of 3 datasets
    memory=32768,           # 32 GiB — comfortable for the Violations⟕AssessedViolations hash join
    cpu=8.0,
    ephemeral_disk=524288,  # 512 GiB — Modal's floor when ephemeral_disk is set; ≫ the ~6 GiB
                            # transcoded-giant working set (the NPPES ingest constant)
)
def ingest_msha(only: str = "", trigger_callback_url: str | None = None) -> dict:
    """Materialize the three isolated MSHA Lance datasets from the R2 landing zone. Records
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
        raise RuntimeError(f"msha materialization failed: {error}")
    return {"feed": FEED, "rows_total": rows_total, "datasets": detail, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 45, memory=32768, cpu=8.0,
)
def reindex(only: str = "") -> dict:
    """(Re)build scalar indexes on already-written MSHA datasets without re-materializing.
    Idempotent (create_scalar_index defaults to replace=True)."""
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
    non-null counts on the indexed keys, and committed indices. Authoritative success
    check — reads what actually landed, independent of the write path's return value."""
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
            # Lance's filter parser treats double-quoted tokens as STRING LITERALS (an
            # always-true predicate), so column refs MUST be bare — Lance matches the
            # stored UPPERCASE case directly (no lowercasing). Every MSHA column is
            # [A-Z0-9_], safe as a bare identifier.
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
    """Create ops.msha_ingest_runs (idempotent). Mirrors the canonical .sql."""
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
    """Materialize all three datasets (or one via --only). No Trigger callback; ops.* fires."""
    import json

    print(json.dumps(
        ingest_msha.remote(only=only, trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def reindex_only(only: str = "") -> None:
    """Rebuild scalar indexes on the existing datasets (no re-materialization)."""
    import json

    print(json.dumps(reindex.remote(only=only), indent=2, default=str))


@app.local_entrypoint()
def verify(only: str = "") -> None:
    """Read-back proof of the committed MSHA datasets (rows, key non-nulls, indices)."""
    import json

    print(json.dumps(verify_datasets.remote(only=only), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 10) -> None:
    """Print the most recent ops.msha_ingest_runs rows."""
    import json

    print(json.dumps(ledger.remote(limit), indent=2, default=str))
