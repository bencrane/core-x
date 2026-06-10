"""Compute worker — MSHA generic landing→active MIRROR (materialize EVERY landing member).

Part of the ``msha-mirror-pipelines`` Modal app. Sibling to the two curated workers
(``materialize_msha.py``, ``materialize_msha_extensions.py``); it does NOT touch the five
datasets they own. Same clean-room data plane: DuckDB does 100% of the transform, Lance is
written straight to R2, no Iceberg/Polaris.

WHY THIS EXISTS (the EPA #375 precedent, applied to MSHA).
    ``landing/msha/`` is a materialize-EVERYTHING handoff, not a buffet. The operator drags
    the full MSHA Open-Government archive set into the landing zone so a worker never has to
    download from the web; the contract is that EVERY landing member ends up in ``active/``.
    The two curated workers cherry-picked 8 of the 20 archives → 5 datasets, leaving active/
    a partial reflection of landing and forcing a return to the zip pile for every new
    hypothesis. This worker closes that gap and keeps it closed: it enumerates every
    ``landing/msha/*.zip``, skips the archives a curated spec already owns, and materializes
    each remaining one as ``msha_<slug>`` — all-varchar passthrough (lossless; cast at query
    time), auto-BTREE on whatever resolution keys are actually present, idempotent
    skip-existing. Re-run after any new landing drop mirrors only what is new.

    SOURCE (R2): s3://data-sink/landing/msha/<Archive>.zip   (single-member ZIP/DEFLATE,
                 pipe-delimited, quoted values, unquoted header, CRLF, Windows-1252)
    TARGET (Gen-3 SoR — native Lance v2.1, full-snapshot overwrite):
                 s3://data-sink/active/msha_<slug>/           (one per non-curated archive)

PASSTHROUGH, NOT TYPED (the deliberate design difference from the curated workers).
    Curated datasets get hand-tuned per-column casts + ``<COL>_norm`` crosswalk siblings.
    The mirror does NEITHER: every column is retained as a dequoted VARCHAR
    (``nullif(trim(BOTH '"' FROM col), '')``), no date/numeric casts, no name_norm, no joins.
    This is lossless (nothing is dropped or coerced) and sidesteps every per-archive schema
    decision — grain keys, union column-maps, Excel-export column renames — by deferring them
    to query time. A mirror table that later earns a curated home graduates into one of the
    typed workers; until then it is fully present and queryable. (Directive-29 isolation holds:
    no cross-universe bridge column is introduced here.)

PARSING HYGIENE — the Directive-26 recipe, verbatim parity with the curated workers:
    quote='' (interior MSHA quotes are unescaped; a strict RFC-4180 read collapses) + a
    CP1252→UTF-8 transcode-to-scratch BEFORE DuckDB reads (degree signs / ñ / smart quotes
    break DuckDB's utf-8 and latin-1 readers). The wrapping quotes survive quote='' as literal
    text and are stripped per-field. A member whose header is non-``[A-Z0-9_]`` (e.g.
    OrdersIssued's Excel-export labels) still lands losslessly — it simply matches no auto-key.

AUTO-INDEX. After the typed projection, the present columns are intersected with
    ``MIRROR_KEY_COLS`` (MSHA's resolution-key vocabulary) and each match gets a BTREE built
    in place on R2. No bitmap (a mirror keeps it simple; categorical bitmaps are added when a
    dataset graduates to curated). LANCE_BYPASS_SPILLING=true forces the in-memory BTREE sort
    (lance#2650).

CONTROL PLANE: writes a per-dataset row + a ``__mirror__`` summary row to
    ``ops.msha_ingest_runs`` (feed='msha') and POSTs the Trigger waitpoint callback on
    terminal state — identical contract to the curated workers.

    modal run    pipelines/ingest_msha/materialize_msha_mirror.py::show     # DRY-RUN: what would materialize
    modal run    pipelines/ingest_msha/materialize_msha_mirror.py::mirror   # materialize all un-mirrored members
    modal run    pipelines/ingest_msha/materialize_msha_mirror.py::mirror --only msha_inspections
    modal run    pipelines/ingest_msha/materialize_msha_mirror.py::mirror --force   # rebuild existing
    modal run    pipelines/ingest_msha/materialize_msha_mirror.py::verify   # read-back proof
    modal deploy pipelines/ingest_msha/materialize_msha_mirror.py           # dispatcher-resolvable
"""

from __future__ import annotations

import os
import re

import modal

BUCKET = "data-sink"
LANDING_PREFIX = os.environ.get("MSHA_LANDING_PREFIX", "landing/msha").strip("/") + "/"
ACTIVE_BASE = os.environ.get("MSHA_ACTIVE_BASE", "s3://data-sink/active/")
FEED = "msha"
SCRATCH_DIR = "/tmp/msha_mirror"

# Directive-26 canonical read recipe — verbatim parity with the curated workers. quote=''
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

# Archives a CURATED worker already owns → the mirror NEVER touches them (no duplication).
# materialize_msha.py:  Mines(+AddressofRecord), ControllerOperatorHistory, Violations(+AssessedViolations)
# materialize_msha_extensions.py:  ContractorProdQuarterly(+Yearly), Accidents
MIRROR_CURATED_ARCHIVES = {
    "mines.zip", "addressofrecord.zip", "violations.zip", "assessedviolations.zip",
    "controlleroperatorhistory.zip", "accidents.zip",
    "contractorprodquarterly.zip", "contractorprodyearly.zip",
}

# MSHA resolution-key vocabulary. A mirrored member's columns ∩ this set → auto-BTREE.
# (Derived from the live DESCRIBE keys in MSHA_REMAINING_INGEST_PLAN_ADVERSARIAL_REVIEW.md:
# Inspections/Conferences/ContestedViolations/CivilPenaltyDockets/samples probed keys.)
MIRROR_KEY_COLS = {
    "MINE_ID", "EVENT_NO", "VIOLATION_NO", "CITATION_NO", "CONTROLLER_ID", "OPERATOR_ID",
    "CONTRACTOR_ID", "VIOLATOR_ID", "DOCUMENT_NO", "DOCKET_NO", "ASSESS_CASE_NO",
    "CONFERENCE_NO", "ISSUANCE_NO", "SAMPLE_NO", "LABORATORY_NO", "CASS_NUM", "ASMT_CASE_NO",
    # OrdersIssued's Excel-export controller key (sanitized, suffixed); auto-BTREE it on mirror.
    "CONTROLLER_ID_VIOLATIONS",
}

# ── ops.msha_ingest_runs DDL — verbatim mirror of the curated workers (same feed ledger). ──
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
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`
    "pyarrow>=17",
    "boto3>=1.35",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env(
    {"LANCE_BYPASS_SPILLING": "true"}  # in-memory BTREE sort for high-card string keys (lance#2650)
)

app = modal.App("msha-mirror-pipelines", image=image)


# --------------------------------------------------------------------------- #
# R2 / object-store (verbatim parity with the curated workers)
# --------------------------------------------------------------------------- #
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


# --------------------------------------------------------------------------- #
# Acquire (verbatim parity with the curated workers)
# --------------------------------------------------------------------------- #
def _download_archive(s3, archive: str, dest_dir: str) -> tuple[str, int]:
    key = LANDING_PREFIX + archive
    out = os.path.join(dest_dir, archive)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    s3.download_file(BUCKET, key, out)
    return out, os.path.getsize(out)


def _extract_member(zip_path: str, dest_dir: str) -> tuple[str, str]:
    """Extract the single data member (largest member wins); streamed, no path-traversal."""
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
    written = 0
    with open(src, "rb") as i, open(dst, "wb") as o:
        while (buf := i.read(chunk)):
            out = buf.decode("cp1252", errors="replace").encode("utf-8")
            o.write(out)
            written += len(out)
    return written


def _acquire(s3, archive: str, dest_dir: str) -> tuple[str, int]:
    """R2 .zip → extract member → transcode to UTF-8 scratch. Returns (utf8_path, zip_bytes)."""
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
# Transform — generic dequoted-VARCHAR passthrough (no casts, no norm)
# --------------------------------------------------------------------------- #
def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _lit(s: str) -> str:
    return s.replace("'", "''")


def _base_expr(qualified: str) -> str:
    """Dequote (strip the wrapping " left by quote='') + strip SURROUNDING whitespace/control
    (Excel-export tab prefixes, stray CR) + empty→NULL. Surrounding only — interior text is
    never touched. A no-op for the clean standard MSHA files; fixes OrdersIssued's '\\t'-prefixed
    values so resolution keys join cleanly."""
    return (f"nullif(trim(BOTH chr(9)||chr(10)||chr(13)||' ' "
            f"FROM trim(BOTH '\"' FROM {qualified})), '')")


def _san_col(name: str) -> str:
    """Map a raw source column name to a Lance-safe identifier: non-alnum runs → '_', upper,
    trim '_'. Lance rejects field names containing '.' (struct nesting); spaces/parens/'@' are
    tolerated but ugly. For clean MSHA headers this is a no-op (MINE_ID → MINE_ID); for
    OrdersIssued's Excel labels it recovers canon ('Violation No.' → VIOLATION_NO)."""
    s = re.sub(r"[^0-9A-Za-z]+", "_", name).strip("_").upper()
    if not s:
        s = "COL"
    if s[0].isdigit():
        s = "C_" + s
    return s


def _sanitize_cols(cols: list[str]) -> list[tuple[str, str]]:
    """(raw, sanitized) pairs, collision-deduped with a numeric suffix so two raw names can
    never collapse onto one Lance field."""
    seen: dict[str, int] = {}
    out: list[tuple[str, str]] = []
    for c in cols:
        s = _san_col(c)
        if s in seen:
            seen[s] += 1
            s = f"{s}_{seen[s]}"
        else:
            seen[s] = 1
        out.append((c, s))
    return out


def _src(path: str, alias: str) -> str:
    return f"(SELECT * FROM read_csv('{_lit(path)}', {READ_RECIPE})) AS {alias}"


def _describe(con, path: str) -> list[str]:
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_csv('{_lit(path)}', {READ_RECIPE})").fetchall()
    return [r[0] for r in rows]


def _spine_count(con, path: str) -> int:
    return con.execute(
        f"SELECT count(*) FROM read_csv('{_lit(path)}', {READ_RECIPE})").fetchone()[0]


def _passthrough_sql(path: str, archive: str, pairs: list[tuple[str, str]]) -> str:
    """Lossless dequoted-VARCHAR projection: read each raw source column, alias to its
    sanitized Lance-safe identifier, + provenance. No type casts."""
    proj = ",\n    ".join(f"{_base_expr('s.' + _q(raw))} AS {_q(san)}" for raw, san in pairs)
    prov = f"    '{_lit(archive)}' AS source_file,\n    now() AS ingested_at"
    return f"SELECT\n    {proj},\n{prov}\nFROM {_src(path, 's')}"


def _detect_keys(san_cols: list[str]) -> list[str]:
    """Sanitized columns whose name matches the MSHA resolution-key vocab → auto-BTREE."""
    return sorted(c for c in san_cols if c.upper() in MIRROR_KEY_COLS)


# --------------------------------------------------------------------------- #
# Lance — write + index (direct-R2; MSHA members are sub-Giants, narrow schemas)
# --------------------------------------------------------------------------- #
def _write_lance(reader, uri: str, so: dict) -> None:
    import lance

    lance.write_dataset(
        reader, uri,
        schema=reader.schema,
        mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )


def _build_indexes(uri: str, btree: list[str], so: dict) -> list[dict]:
    """Best-effort BTREE on each detected key (replace=True → idempotent). A miss never fails
    an otherwise-good load; it is logged and recorded."""
    import lance

    ds = lance.dataset(uri, storage_options=so)
    out: list[dict] = []
    for col in btree:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            print(f"    BTREE ✓ {col}")
            out.append({"col": col, "type": "BTREE", "ok": True})
        except Exception as exc:  # noqa: BLE001
            print(f"    BTREE ✗ {col}: {exc}")
            out.append({"col": col, "type": "BTREE", "ok": False, "error": str(exc)[:200]})
    return out


def _dataset_exists(uri: str, so: dict) -> bool:
    import lance

    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001 — absent / not found
        return False


def _committed_index_names(uri: str, so: dict) -> list[str]:
    import lance

    ds = lance.dataset(uri, storage_options=so)
    names = []
    for ix in ds.list_indices():
        names.append(ix.get("name", str(ix)) if isinstance(ix, dict)
                     else getattr(ix, "name", str(ix)))
    return sorted(names)


# --------------------------------------------------------------------------- #
# Mirror spec enumeration (the EPA #375 pattern, MSHA single-member adaptation)
# --------------------------------------------------------------------------- #
def _mirror_slug(archive: str) -> str:
    """msha_<snake>: split camelCase, lowercase, sanitize. ContestedViolations.zip →
    msha_contested_violations; MinesProdQuarterly.zip → msha_mines_prod_quarterly."""
    base = archive.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", base)
    s = re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    return "msha_" + s


def build_mirror_specs(s3, only: list[str] | None = None, force: bool = False) -> list[dict]:
    """Every landing/msha/*.zip that no curated worker owns and (unless force) is not already
    mirrored → one passthrough spec. MSHA archives are single-member, so archive == dataset
    (1:1); no zip is opened here — column/key detection happens at materialize time."""
    so = _r2_storage_options()
    archives: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=LANDING_PREFIX):
        for o in page.get("Contents", []):
            rel = o["Key"][len(LANDING_PREFIX):]
            if rel.lower().endswith(".zip"):
                archives.append(rel)

    only_set = set(only) if only else None
    specs: list[dict] = []
    for archive in sorted(archives):
        if archive.lower() in MIRROR_CURATED_ARCHIVES:
            continue
        slug = _mirror_slug(archive)
        if only_set and slug not in only_set:
            continue
        if not force and _dataset_exists(ACTIVE_BASE + slug + "/", so):
            print(f"[mirror] skip (exists): {slug}")
            continue
        specs.append({"name": slug, "archive": archive,
                      "uri": ACTIVE_BASE + slug + "/"})
    print(f"[mirror] {len(specs)} member(s) to materialize: {[s['name'] for s in specs]}")
    return specs


# --------------------------------------------------------------------------- #
# State + callback + cleanup (verbatim parity with the curated workers)
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


def _ensure_ops_ledger() -> None:
    """Run OPS_DDL exactly once from the orchestrator BEFORE fanning out materialize_member.
    The IF-NOT-EXISTS DDL is cheap but NOT concurrency-safe: N concurrent workers each running
    CREATE SCHEMA/TABLE/INDEX IF NOT EXISTS at terminal time deadlock Postgres on the shared
    catalog locks (the #377 EPA fix — same pattern). Creating it up front means every worker's
    _record_run sees the table present (to_regclass guard) and skips the DDL. Best-effort."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    except Exception as exc:  # noqa: BLE001 — bootstrap is best-effort, never fatal
        print(f"WARN: ops.msha_ingest_runs bootstrap failed: {exc}")


def _record_run(*, source_prefix, datasets, rows_total, bytes_downloaded,
                status, error, started_at, completed_at) -> None:
    """Terminal row → ops.msha_ingest_runs (psycopg). Best-effort; never masks the ingest.

    DDL is NOT on the hot path: mirror_landing calls _ensure_ops_ledger() once before fanning
    out, so concurrent materialize_member workers never race the IF-NOT-EXISTS DDL (which
    deadlocks Postgres on the catalog locks). The to_regclass guard self-bootstraps ONLY for a
    direct single-container call where the table is absent. The INSERT is retried on
    deadlock/serialization failure. Each txn uses conn.transaction() (NOT `with conn:`, which
    CLOSES the psycopg3 connection on exit) so the connection survives the guard txn + retries."""
    import time

    import psycopg
    from psycopg import errors as pg_errors
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    conn = psycopg.connect(dsn)
    try:
        with conn.cursor() as cur:
            # Self-bootstrap ONLY when absent (direct single-container runs). Fan-out workers see
            # the orchestrator-created table and skip the DDL → no concurrent IF-NOT-EXISTS deadlock.
            with conn.transaction():
                cur.execute("SELECT to_regclass('ops.msha_ingest_runs')")
                if cur.fetchone()[0] is None:
                    cur.execute(OPS_DDL)
            params = (FEED, BUCKET, source_prefix, Jsonb(datasets), rows_total,
                      bytes_downloaded, status, error, started_at, completed_at)
            for attempt in range(3):
                try:
                    with conn.transaction():
                        cur.execute(
                            """
                            INSERT INTO ops.msha_ingest_runs
                                (feed, source_bucket, source_prefix, datasets, rows_total,
                                 bytes_downloaded, status, error, started_at, completed_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            params,
                        )
                    break
                except (pg_errors.DeadlockDetected, pg_errors.SerializationFailure) as exc:
                    if attempt == 2:
                        raise
                    print(f"WARN: ops.msha_ingest_runs INSERT retry {attempt + 1}/3 "
                          f"({exc.__class__.__name__})")
                    time.sleep(0.25 * (attempt + 1))
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")
    finally:
        conn.close()


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


# --------------------------------------------------------------------------- #
# Per-dataset materialization (mapped — one container per member)
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 2,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=524288,
)
def materialize_member(spec: dict, run_id: str) -> dict:
    """Acquire one archive → dequoted-VARCHAR passthrough (streamed) → Lance overwrite on R2 →
    auto-BTREE detected keys. Verifies committed Lance rows == source rows (no-drop proof).
    Records a per-dataset ops.msha_ingest_runs row."""
    import datetime as dt

    import duckdb
    import lance  # noqa: F401 — fail fast if the wheel is missing

    name = spec["name"]
    archive = spec["archive"]
    uri = spec["uri"]
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    status, error, lance_rows, spine_rows, n_cols, indexes, zb = "error", None, 0, 0, 0, [], 0
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    os.makedirs(os.path.join(SCRATCH_DIR, "duckdb_spill"), exist_ok=True)
    utf8_path = None

    try:
        s3 = _s3_client()
        print(f"\n=== {name}  ←  {archive}  →  {uri} ===")
        utf8_path, zb = _acquire(s3, archive, SCRATCH_DIR)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
            con.execute("SET preserve_insertion_order=false;")
            cols = _describe(con, utf8_path)
            pairs = _sanitize_cols(cols)
            san_cols = [s for _, s in pairs]
            spine_rows = _spine_count(con, utf8_path)
            keys = _detect_keys(san_cols)
            renamed = [(r, s) for r, s in pairs if r != s]
            print(f"    {len(cols)} cols, {spine_rows:,} rows; auto-BTREE keys: {keys or '(none)'}"
                  + (f"; sanitized {len(renamed)} name(s): {renamed[:6]}" if renamed else ""))
            sql = _passthrough_sql(utf8_path, archive, pairs)
            reader = con.execute(sql).to_arrow_reader(READ_BATCH_ROWS)
            n_cols = len(reader.schema)
            _write_lance(reader, uri, so)
        finally:
            con.close()

        lance_rows = lance.dataset(uri, storage_options=so).count_rows()
        grain_ok = lance_rows == spine_rows
        flag = "OK" if grain_ok else "!!!! ROW MISMATCH"
        print(f"    wrote {lance_rows:,} rows × {n_cols} cols (source {spine_rows:,}) → {flag}")
        indexes = _build_indexes(uri, keys, so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[{name}] FAILED: {error}")
    finally:
        _cleanup(utf8_path)
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(
            source_prefix=LANDING_PREFIX,
            datasets={name: {"uri": uri, "source_archive": archive, "kind": "mirror",
                             "lance_rows": int(lance_rows), "source_rows": int(spine_rows),
                             "n_cols": int(n_cols), "indexes": indexes}},
            rows_total=int(lance_rows), bytes_downloaded=int(zb),
            status=status, error=error, started_at=started, completed_at=completed)

    return {"dataset": name, "uri": uri, "rows": int(lance_rows), "cols": int(n_cols),
            "status": status, "indexes": indexes, "error": error}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 300, memory=4096,
)
def mirror_landing(only: list[str] | None = None, force: bool = False,
                   trigger_callback_url: str | None = None) -> dict:
    """Mirror EVERY landing/msha member lacking a curated/mirrored table → its own Lance table
    (one materialize_member container each). Idempotent: re-run after any landing drop
    materializes only what is new."""
    import datetime as dt

    started = dt.datetime.now(dt.timezone.utc)
    run_id = started.strftime("msha_mirror_%Y%m%dT%H%M%SZ")
    s3 = _s3_client()
    specs = build_mirror_specs(s3, only=only, force=force)
    if not specs:
        print("[mirror] nothing to do — active/ already mirrors landing.")
        _post_callback(trigger_callback_url,
                       {"status": "success", "feed": FEED, "materialized": 0})
        return {"run_id": run_id, "status": "success", "materialized": 0, "datasets": {}}

    _ensure_ops_ledger()  # create the ledger ONCE before fan-out → workers skip the racing DDL
    calls = [(s["name"], materialize_member.spawn(s, run_id)) for s in specs]
    results = []
    for name, call in calls:
        try:
            results.append(call.get())
        except Exception as exc:  # noqa: BLE001 — one bad member must not sink the batch
            print(f"[mirror] {name} raised: {exc}")
            results.append({"dataset": name, "rows": 0, "status": "error", "error": str(exc)})

    ok = [r for r in results if r.get("status") == "success"]
    bad = [r for r in results if r.get("status") != "success"]
    status = "success" if not bad else ("partial" if ok else "error")
    completed = dt.datetime.now(dt.timezone.utc)
    _record_run(
        source_prefix=LANDING_PREFIX,
        datasets={"__mirror__": {r["dataset"]: {"rows": r.get("rows"), "status": r.get("status")}
                                 for r in results}},
        rows_total=sum(int(r.get("rows", 0)) for r in results), bytes_downloaded=0,
        status=status, error=(None if not bad else ",".join(r["dataset"] for r in bad)),
        started_at=started, completed_at=completed)
    _post_callback(trigger_callback_url, {
        "status": status, "feed": FEED, "materialized": len(ok),
        "failed": [r["dataset"] for r in bad],
    })
    summary = {"run_id": run_id, "status": status, "materialized": len(ok),
               "failed": [r["dataset"] for r in bad],
               "datasets": {r["dataset"]: {"rows": r.get("rows"), "cols": r.get("cols"),
                                           "status": r.get("status")} for r in results}}
    print(summary)
    return summary


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 5)
def enumerate_specs(only: list[str] | None = None, force: bool = False) -> list[dict]:
    """DRY-RUN enumeration (no writes): the members the mirror would materialize."""
    s3 = _s3_client()
    return [{"name": s["name"], "archive": s["archive"]}
            for s in build_mirror_specs(s3, only=only, force=force)]


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 20)
def verify_mirror(only: list[str] | None = None) -> dict:
    """Read-back proof: for every mirrored dataset, report rows, committed indices, and the
    detected key non-null fill. Reads what actually landed."""
    import lance

    so = _r2_storage_options()
    s3 = _s3_client()
    # enumerate mirror slugs that exist (force=True lists all targets regardless of existence)
    specs = build_mirror_specs(s3, only=only, force=True)
    out: dict = {}
    for spec in specs:
        name, uri = spec["name"], spec["uri"]
        info: dict = {"dataset_uri": uri}
        try:
            ds = lance.dataset(uri, storage_options=so)
            info["rows"] = ds.count_rows()
            info["indices"] = _committed_index_names(uri, so)
            for col in _detect_keys(list(ds.schema.names)):
                try:
                    info[f"{col}__non_null"] = ds.count_rows(filter=f"{col} IS NOT NULL")
                except Exception as exc:  # noqa: BLE001
                    info[f"{col}__non_null"] = f"err: {str(exc)[:80]}"
        except Exception as exc:  # noqa: BLE001 — absent / not yet materialized
            info["error"] = str(exc)[:200]
        out[name] = info
    return out


# --------------------------------------------------------------------------- #
# Local entrypoints
# --------------------------------------------------------------------------- #
@app.local_entrypoint()
def show(only: str = "", force: bool = False) -> None:
    """DRY-RUN (no writes): list exactly which landing members the mirror would materialize."""
    import json

    names = [n for n in only.split(",") if n] or None
    print(json.dumps(enumerate_specs.remote(names, force), indent=2, default=str))


@app.local_entrypoint()
def mirror(only: str = "", force: bool = False) -> None:
    """Materialize every landing/msha member lacking a table → msha_<slug>. `--only a,b`
    targets slugs; `--force` rebuilds existing. Idempotent (mirrors only what is new)."""
    import json

    names = [n for n in only.split(",") if n] or None
    print(json.dumps(
        mirror_landing.remote(only=names, force=force, trigger_callback_url=None),
        indent=2, default=str))


@app.local_entrypoint()
def verify(only: str = "") -> None:
    """Read-back proof of the mirrored datasets (rows, indices, key non-null fill)."""
    import json

    names = [n for n in only.split(",") if n] or None
    print(json.dumps(verify_mirror.remote(only=names), indent=2, default=str))
