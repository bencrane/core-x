"""schema_catalog — the canonical, queryable Lance system-of-record for dataset schemas.

WHAT THIS IS. A general, append-only catalog that records the EXACT, LIVE column
schema of any Lance dataset in the active R2 sink — read straight off each dataset's
committed Lance manifest, never inferred from docs. It replaces brittle `.md` schema
inference with a queryable Lance table that is byte-identical to what the dataset
actually carries on disk. Catalogue a new dataset by re-invoking this pipeline with a
different `(source_group, dataset_uri)` target — never by re-investigating.

CATALOG DATASET URI
    s3://data-sink/active/schema_catalog/
  Placed as a FLAT root under `active/`, matching the dominant active-sink convention
  (the sink is ~330 flat snake_case roots like `entity_registrations`,
  `crosswalk_sam_usaspending`; only a handful — `usaspending/`, `fmcsa/`, `nppes/`,
  `epiq/` — are one-level namespaces, and those hold MULTIPLE leaves). `schema_catalog`
  is a single meta-dataset, so a flat root is correct and keeps it discoverable by the
  gtm_mcp registry walk (which registers any prefix carrying `_versions/`). The
  directive proposed `active/factory_meta/schema_catalog`; that namespace does not
  exist in the live sink and inventing it for one leaf would diverge from convention —
  the flat root is the faithful match. Override with SCHEMA_CATALOG_LANCE_URI.

GRAIN
    ONE ROW PER (dataset, column, capture). A capture is one pipeline run against one
    dataset. Re-running re-snapshots every changed dataset as fresh Lance fragments.

SCHEMA (16 columns)
    row_uid            string                  sha256(dataset_uri | column_name | catalog_run_id)
    source_group       string                  e.g. 'sam_gov'
    dataset_name       string                  e.g. 'entity_registrations'
    dataset_uri        string                  full s3:// URI
    column_name        string                  VERBATIM column name (field.name)
    column_index       int32                   ordinal position in the schema
    arrow_type         string                  str(field.type)
    nullable           bool                    field.nullable
    field_metadata     string                  JSON of field-level metadata, else null
    dataset_n_columns  int32                   total column count (denormalized)
    dataset_n_rows     int64                   count_rows() at capture (metadata-only)
    dataset_version    int64                   Lance dataset version at capture (null if N/A)
    schema_fingerprint string                  sha256 of ordered [(name,type)] — the drift key
    catalog_run_id     string                  uuid4 per pipeline run
    captured_at        timestamp[ms, UTC]      capture wall-clock

INDEXING (Lance BTREE scalar indices, per house rules — every keyed column)
    dataset_name, source_group, column_name, catalog_run_id, schema_fingerprint, captured_at

IDEMPOTENCY CONTRACT
    APPEND-ONLY. Each run appends a fresh snapshot as new Lance fragments; the catalog
    is NEVER rewritten in place. Before appending a dataset's snapshot the pipeline
    computes its current `schema_fingerprint` and compares it to the latest snapshot's
    fingerprint already in the catalog for that `dataset_name` (max captured_at). If
    identical → the dataset is SKIPPED (logged "unchanged: <name>"), so re-runs never
    accrue duplicate rows. If changed or absent → the snapshot is appended. A run that
    appends rebuilds the BTREE indices.

"LATEST SCHEMA FOR X"
    The rows with max(captured_at) for that dataset_name. The pipeline verifies this
    query round-trips after every populating run (see --verify, default on).

HOW TO ADD A DATASET
    1. Find the dataset's live s3:// URI (e.g. from its pipeline definition).
    2. Append `("<source_group>", "s3://data-sink/active/<name>/")` to TARGETS below,
       OR pass it on the CLI without editing the file:
         doppler run -p core-x -c prd -- python -m pipelines.catalog.schema_catalog \\
             --target usaspending=s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/
    3. Run. The fingerprint guard makes it safe to re-run for every dataset at once.

RUN
    # default targets (the canonical entity/govcon substrate — SAM masters, the gold
    # mirror, USAspending leaves, the gtm_mcp substrate, crosswalks/bridges), populate +
    # index + verify. Per-dataset error isolation: a missing/corrupt leaf is logged and
    # skipped, never aborting the batch; the fingerprint guard makes the whole run a
    # safe no-op for unchanged datasets:
    doppler run -p core-x -c prd -- python -m pipelines.catalog.schema_catalog

    # catalog one more dataset (repeatable --target), then verify:
    doppler run -p core-x -c prd -- python -m pipelines.catalog.schema_catalog \\
        --target usaspending=s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/

    # read-only: print live schemas, write nothing:
    doppler run -p core-x -c prd -- python -m pipelines.catalog.schema_catalog --dry-run

CREDENTIALS
    R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_ENDPOINT (or R2_ACCOUNT_ID) — the exact
    three the fleet uses (r2-credentials Modal secret / core-x/prd Doppler config). The
    run ledger (ops.schema_catalog_runs) additionally needs HQX_DB_URL_POOLED; absent it,
    the ledger write is skipped (the in-table run metadata is the durable record).

OPS LEDGER (optional, best-effort)
    The ops.*_runs pattern is consistent across the repo, so a per-run summary row is
    written to ops.schema_catalog_runs (DDL mirrored in ops_schema_catalog_runs.sql,
    applied idempotently). The Lance row metadata (catalog_run_id, captured_at) is the
    authoritative run record; the ledger is operational convenience only.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import os
import sys
import uuid

log = logging.getLogger("pipelines.catalog.schema_catalog")

# ── Catalog coordinates ──────────────────────────────────────────────────────
_PROD_URI = "s3://data-sink/active/schema_catalog/"
CATALOG_URI = os.environ.get("SCHEMA_CATALOG_LANCE_URI", _PROD_URI)

# Net-new dataset → pin the current Lance default (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576

# Every keyed column gets a hard BTREE scalar index (house rule + 02_lancedb_storage.md §6).
BTREE_INDEXES = [
    "dataset_name", "source_group", "column_name",
    "catalog_run_id", "schema_fingerprint", "captured_at",
]

# Default target manifest: (source_group, dataset_uri). The canonical entity/govcon
# substrate — every URI below is a VERIFIED live Lance leaf (carries `_versions/`) in the
# active R2 sink, resolved by listing the sink one level deep (parent prefixes like
# `usaspending_api_fresh/` are namespaces; their LEAVES are cataloged, never the parent).
# Append future datasets here, or pass --target on the CLI. The fingerprint idempotency
# guard makes re-running the whole manifest a safe no-op for unchanged datasets, and
# per-dataset error isolation (see `run`) means a single missing/corrupt dataset is logged
# and skipped, never aborting the batch.
#
# NOTE on `active/enrichment/`: it is an EMPTY namespace prefix (Parallel writes
# spec-named sub-datasets under it at runtime), NOT a flat Lance leaf — so it is
# intentionally absent here; it auto-catalogs once a spec materializes a leaf.
TARGETS: list[tuple[str, str]] = [
    # SAM entity substrate — the positional-spine source + the structured masters.
    ("sam_gov", "s3://data-sink/active/entity_registrations/"),
    ("sam_gov", "s3://data-sink/active/sam_pocs/"),
    ("sam_gov", "s3://data-sink/active/sam_master_entities/"),
    ("sam_gov", "s3://data-sink/active/sam_normalized_entities/"),
    ("sam_gov", "s3://data-sink/active/sam_master_contacts/"),
    ("sam_gov", "s3://data-sink/active/sam_master_domains/"),
    # Gold mirror — the unified SAM identity ⋈ USAspending financial profile (1/uei).
    ("resolution", "s3://data-sink/active/entity_profile_gold/"),
    # USAspending leaves — live API-fresh override feeds, the api-catalog leaf, rollups.
    ("usaspending", "s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/"),
    ("usaspending", "s3://data-sink/active/usaspending_api_fresh/contract_subaward/"),
    ("usaspending", "s3://data-sink/active/usaspending_api_catalog/"),
    ("usaspending", "s3://data-sink/active/contractor_award_summary/"),
    ("usaspending", "s3://data-sink/active/ffata_exec_comp/"),
    # gtm_mcp current substrate (for the entity-substrate decision comparison).
    ("gtm", "s3://data-sink/active/companies/"),
    ("gtm", "s3://data-sink/active/people/"),
    # Crosswalks / bridges — the resolution edges across spines.
    ("resolution", "s3://data-sink/active/crosswalk_sam_usaspending/"),
    ("resolution", "s3://data-sink/active/crosswalk_sos_sam/"),
    ("resolution", "s3://data-sink/active/bridge_sam_pdl/"),
    ("resolution", "s3://data-sink/active/bridge_sam_fmcsa_domain/"),
]


# ── R2 / S3 (byte-identical to the worker convention) ────────────────────────
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError(
            "Set R2_ENDPOINT or R2_ACCOUNT_ID — the catalog cannot reach the R2 sink."
        )
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _dataset_name_from_uri(uri: str) -> str:
    """The catalog-canonical dataset name from an s3:// URI — the path relative to
    `active/` (so `.../active/usaspending_api_fresh/contract_prime_txn/` →
    `usaspending_api_fresh/contract_prime_txn`, mirroring the gtm_mcp registry key),
    falling back to the last path segment if `active/` is absent."""
    body = uri.split("://", 1)[-1].rstrip("/")
    marker = "/active/"
    if marker in body:
        return body.split(marker, 1)[1]
    return body.rsplit("/", 1)[-1]


# ── Catalog Arrow schema (explicit — never schema-inferred) ──────────────────
def _catalog_schema():
    import pyarrow as pa

    return pa.schema([
        pa.field("row_uid", pa.string()),
        pa.field("source_group", pa.string()),
        pa.field("dataset_name", pa.string()),
        pa.field("dataset_uri", pa.string()),
        pa.field("column_name", pa.string()),
        pa.field("column_index", pa.int32()),
        pa.field("arrow_type", pa.string()),
        pa.field("nullable", pa.bool_()),
        pa.field("field_metadata", pa.string()),
        pa.field("dataset_n_columns", pa.int32()),
        pa.field("dataset_n_rows", pa.int64()),
        pa.field("dataset_version", pa.int64()),
        pa.field("schema_fingerprint", pa.string()),
        pa.field("catalog_run_id", pa.string()),
        pa.field("captured_at", pa.timestamp("ms", tz="UTC")),
    ])


def _field_metadata_json(field) -> str | None:
    """JSON of a pyarrow field's key/value metadata (bytes decoded), else None."""
    md = field.metadata
    if not md:
        return None
    out = {}
    for k, v in md.items():
        kk = k.decode("utf-8", "replace") if isinstance(k, bytes) else str(k)
        vv = v.decode("utf-8", "replace") if isinstance(v, bytes) else str(v)
        out[kk] = vv
    return json.dumps(out, sort_keys=True, ensure_ascii=False)


def _schema_fingerprint(schema) -> str:
    """sha256 over the ordered [(name, str(type))] list — the dataset's drift key.
    Stable across runs for an unchanged schema; flips on any add/drop/reorder/retype."""
    payload = json.dumps(
        [[f.name, str(f.type)] for f in schema], ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _row_uid(dataset_uri: str, column_name: str, catalog_run_id: str) -> str:
    return hashlib.sha256(
        f"{dataset_uri}|{column_name}|{catalog_run_id}".encode("utf-8")
    ).hexdigest()


# ── Live read → catalog rows for ONE dataset ─────────────────────────────────
def capture_dataset(
    source_group: str,
    dataset_uri: str,
    catalog_run_id: str,
    captured_at: dt.datetime,
    storage_options: dict[str, str],
) -> tuple[list[dict], str, int, int, int | None]:
    """Open the LIVE Lance manifest for `dataset_uri` and build one catalog row per
    column. Manifest-only: `.schema`, `.count_rows()`, `.version` are all metadata
    reads (no data scan). Returns (rows, schema_fingerprint, n_columns, n_rows,
    version)."""
    import lance

    ds = lance.dataset(dataset_uri, storage_options=storage_options)
    schema = ds.schema  # pyarrow.Schema — the source of truth
    n_columns = len(schema)
    n_rows = ds.count_rows()
    try:
        version = int(ds.version)
    except Exception:  # noqa: BLE001 — version is best-effort; null if unavailable
        version = None
    fingerprint = _schema_fingerprint(schema)
    dataset_name = _dataset_name_from_uri(dataset_uri)

    rows: list[dict] = []
    for i, field in enumerate(schema):
        rows.append({
            "row_uid": _row_uid(dataset_uri, field.name, catalog_run_id),
            "source_group": source_group,
            "dataset_name": dataset_name,
            "dataset_uri": dataset_uri,
            "column_name": field.name,
            "column_index": i,
            "arrow_type": str(field.type),
            "nullable": bool(field.nullable),
            "field_metadata": _field_metadata_json(field),
            "dataset_n_columns": n_columns,
            "dataset_n_rows": n_rows,
            "dataset_version": version,
            "schema_fingerprint": fingerprint,
            "catalog_run_id": catalog_run_id,
            "captured_at": captured_at,
        })
    return rows, fingerprint, n_columns, n_rows, version


# ── Idempotency guard: latest fingerprint already in the catalog ─────────────
def _latest_fingerprint(dataset_name: str, storage_options: dict[str, str]) -> str | None:
    """The schema_fingerprint of the most recent snapshot for `dataset_name` already in
    the catalog (max captured_at), or None if the catalog or the dataset's first
    snapshot is absent. Pushes the dataset_name equality into the BTREE-indexed scan."""
    import lance

    try:
        cat = lance.dataset(CATALOG_URI, storage_options=storage_options)
    except Exception:  # noqa: BLE001 — catalog not created yet → nothing to compare
        return None

    esc = dataset_name.replace("'", "''")
    tbl = cat.scanner(
        columns=["schema_fingerprint", "captured_at"],
        filter=f"dataset_name = '{esc}'",
    ).to_table()
    if tbl.num_rows == 0:
        return None
    # Pick the fingerprint at the max captured_at (newest snapshot).
    import pyarrow.compute as pc

    ts = tbl.column("captured_at")
    idx = pc.index(ts, pc.max(ts)).as_py()
    return tbl.column("schema_fingerprint")[idx].as_py()


# ── Index (re)build ──────────────────────────────────────────────────────────
def _build_indexes(storage_options: dict[str, str]) -> list[str]:
    """(Re)build every BTREE scalar index on the catalog. create_scalar_index replaces
    an existing index of the same name, so this is safe to call after every append."""
    import lance

    ds = lance.dataset(CATALOG_URI, storage_options=storage_options)
    built: list[str] = []
    for col in BTREE_INDEXES:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(col)
            log.info("BTREE index built: %s", col)
        except Exception as exc:  # noqa: BLE001 — one index failure must not sink the rest
            log.warning("BTREE index on %s failed: %s", col, exc)
    return built


# ── Ops ledger (best-effort, consistent with repo ops.*_runs pattern) ────────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.schema_catalog_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    catalog_run_id   text        NOT NULL,
    catalog_uri      text        NOT NULL,
    datasets_total   int,
    datasets_appended int,
    datasets_skipped int,
    rows_appended    bigint,
    indexes_built    text,
    status           text        NOT NULL,
    error            text,
    metrics          jsonb,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS schema_catalog_runs_run_idx      ON ops.schema_catalog_runs (catalog_run_id);
CREATE INDEX IF NOT EXISTS schema_catalog_runs_status_idx   ON ops.schema_catalog_runs (status);
CREATE INDEX IF NOT EXISTS schema_catalog_runs_recorded_idx ON ops.schema_catalog_runs (recorded_at DESC);
"""


def _record_run(*, catalog_run_id, datasets_total, datasets_appended, datasets_skipped,
                rows_appended, indexes_built, status, error, metrics,
                started_at, completed_at) -> None:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log.info("HQX_DB_URL_POOLED unset — skipping ops.schema_catalog_runs write "
                 "(the in-table run metadata is the durable record).")
        return
    try:
        import psycopg

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.schema_catalog_runs
                    (catalog_run_id, catalog_uri, datasets_total, datasets_appended,
                     datasets_skipped, rows_appended, indexes_built, status, error,
                     metrics, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (catalog_run_id, CATALOG_URI, datasets_total, datasets_appended,
                 datasets_skipped, rows_appended, ",".join(indexes_built), status, error,
                 json.dumps(metrics), started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the catalog write
        log.warning("ops.schema_catalog_runs write failed: %s", exc)


# ── Verify (read back from R2, prove the contract) ───────────────────────────
def verify(targets: list[tuple[str, str]], storage_options: dict[str, str]) -> dict:
    """Re-open the catalog fresh from R2 and prove the contract for every target:
      • latest-capture catalog row count == live len(schema) == dataset_n_columns;
      • the keyed BTREE indices exist;
      • print each dataset's ordered column inventory.
    Returns a structured report; raises on any assertion failure."""
    import lance
    import pyarrow.compute as pc

    cat = lance.dataset(CATALOG_URI, storage_options=storage_options)
    idx_names = {i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                 for i in cat.list_indices()}
    expect_idx = {f"{c}_idx" for c in BTREE_INDEXES}
    missing_idx = sorted(expect_idx - idx_names)
    if missing_idx:
        raise RuntimeError(f"verify: missing BTREE indices {missing_idx} (have {sorted(idx_names)})")

    report: dict = {"catalog_uri": CATALOG_URI, "catalog_rows": cat.count_rows(),
                    "indices": sorted(idx_names), "datasets": {}}

    for source_group, dataset_uri in targets:
        dataset_name = _dataset_name_from_uri(dataset_uri)
        # Latest capture for this dataset from the catalog (max captured_at).
        esc = dataset_name.replace("'", "''")
        rows = cat.scanner(
            columns=["column_index", "column_name", "arrow_type", "nullable",
                     "captured_at", "dataset_n_columns", "schema_fingerprint"],
            filter=f"dataset_name = '{esc}'",
        ).to_table()
        if rows.num_rows == 0:
            raise RuntimeError(f"verify: no catalog rows for {dataset_name}")
        ts = rows.column("captured_at")
        latest = pc.max(ts)
        latest_rows = rows.filter(pc.equal(ts, latest))
        catalog_n = latest_rows.num_rows

        # Live read for the cross-check.
        live = lance.dataset(dataset_uri, storage_options=storage_options)
        live_n = len(live.schema)
        denorm_n = latest_rows.column("dataset_n_columns")[0].as_py()
        if not (catalog_n == live_n == denorm_n):
            raise RuntimeError(
                f"verify: column-count mismatch for {dataset_name}: "
                f"catalog_latest_rows={catalog_n} live_schema={live_n} denorm={denorm_n}"
            )

        # Ordered inventory.
        order = pc.sort_indices(latest_rows, sort_keys=[("column_index", "ascending")])
        latest_rows = latest_rows.take(order)
        inventory = [
            {"column_index": latest_rows.column("column_index")[i].as_py(),
             "column_name": latest_rows.column("column_name")[i].as_py(),
             "arrow_type": latest_rows.column("arrow_type")[i].as_py(),
             "nullable": latest_rows.column("nullable")[i].as_py()}
            for i in range(latest_rows.num_rows)
        ]
        report["datasets"][dataset_name] = {
            "source_group": source_group, "dataset_uri": dataset_uri,
            "n_columns": catalog_n,
            "schema_fingerprint": latest_rows.column("schema_fingerprint")[0].as_py(),
            "inventory": inventory,
        }
    return report


def _print_report(report: dict) -> None:
    print("\n" + "=" * 78)
    print(f"CATALOG  {report['catalog_uri']}")
    print(f"  catalog_rows : {report['catalog_rows']:,}")
    print(f"  indices      : {report['indices']}")
    for name, d in report["datasets"].items():
        print("\n" + "-" * 78)
        print(f"DATASET  {name}   ({d['n_columns']} columns)")
        print(f"  uri          : {d['dataset_uri']}")
        print(f"  source_group : {d['source_group']}")
        print(f"  fingerprint  : {d['schema_fingerprint']}")
        print(f"  {'idx':>3}  {'column_name':<28} {'arrow_type':<26} nullable")
        for col in d["inventory"]:
            print(f"  {col['column_index']:>3}  {col['column_name']:<28} "
                  f"{col['arrow_type']:<26} {col['nullable']}")
    print("=" * 78 + "\n")


# ── Orchestration ────────────────────────────────────────────────────────────
def run(targets: list[tuple[str, str]], *, dry_run: bool, do_verify: bool) -> dict:
    """Catalog every target into schema_catalog with the fingerprint idempotency guard.

    Per target: read the LIVE manifest → build rows → if the fingerprint matches the
    latest snapshot already in the catalog, SKIP; else APPEND. After any append, rebuild
    the BTREE indices and (default) verify the round-trip. APPEND-ONLY: the catalog is
    never rewritten in place."""
    import lance
    import pyarrow as pa

    so = _r2_storage_options()
    catalog_run_id = str(uuid.uuid4())
    captured_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    started_at = captured_at

    log.info("catalog_run_id=%s  catalog_uri=%s  targets=%d  dry_run=%s",
             catalog_run_id, CATALOG_URI, len(targets), dry_run)

    appended_rows: list[dict] = []
    appended_names: list[str] = []
    skipped_names: list[str] = []
    errored: list[dict] = []  # [{dataset_name, dataset_uri, error}] — per-dataset isolation
    ok_targets: list[tuple[str, str]] = []  # targets that read cleanly (drives verify)
    per_dataset: list[dict] = []

    for source_group, dataset_uri in targets:
        name = _dataset_name_from_uri(dataset_uri)
        # PER-DATASET ERROR ISOLATION. One dataset being absent, corrupt, or otherwise
        # unreadable must NEVER abort the batch — log it, record it, and continue. The
        # guaranteed substrate is large and heterogeneous; a single bad leaf is isolated,
        # never allowed to sink the whole run (and never to silently shrink it).
        try:
            rows, fp, n_cols, n_rows, version = capture_dataset(
                source_group, dataset_uri, catalog_run_id, captured_at, so)
        except Exception as exc:  # noqa: BLE001 — isolate the bad dataset, keep going
            log.warning("ERRORED %s (%s): %s — skipping, batch continues",
                        name, dataset_uri, exc)
            errored.append({"dataset_name": name, "dataset_uri": dataset_uri,
                            "error": str(exc)})
            continue

        log.info("read %s: %d cols · %s rows · v%s · fp=%s",
                 name, n_cols, f"{n_rows:,}", version, fp[:12])
        per_dataset.append({"dataset_name": name, "source_group": source_group,
                            "dataset_uri": dataset_uri, "n_columns": n_cols,
                            "n_rows": n_rows, "version": version, "fingerprint": fp})
        ok_targets.append((source_group, dataset_uri))

        if dry_run:
            print(f"\n[dry-run] {name} ({n_cols} cols, {n_rows:,} rows, v{version})")
            for r in rows:
                print(f"  {r['column_index']:>3}  {r['column_name']:<28} "
                      f"{r['arrow_type']:<26} nullable={r['nullable']}")
            continue

        try:
            prior_fp = _latest_fingerprint(name, so)
        except Exception as exc:  # noqa: BLE001 — a guard-read failure isolates this dataset only
            log.warning("ERRORED %s fingerprint-guard read (%s): %s — skipping",
                        name, dataset_uri, exc)
            errored.append({"dataset_name": name, "dataset_uri": dataset_uri,
                            "error": f"fingerprint guard: {exc}"})
            ok_targets.remove((source_group, dataset_uri))
            continue
        if prior_fp == fp:
            log.info("unchanged: %s (fingerprint %s already current) — skipping append",
                     name, fp[:12])
            skipped_names.append(name)
            continue
        log.info("changed/absent: %s (prior=%s current=%s) — appending",
                 name, (prior_fp or "∅")[:12], fp[:12])
        appended_rows.extend(rows)
        appended_names.append(name)

    status, error = "success", None
    indexes_built: list[str] = []
    try:
        if dry_run:
            log.info("dry-run: no write.")
        elif appended_rows:
            table = pa.Table.from_pylist(appended_rows, schema=_catalog_schema())
            # APPEND-ONLY. mode='create' iff the catalog does not yet exist, else 'append'.
            try:
                lance.dataset(CATALOG_URI, storage_options=so)
                mode = "append"
            except Exception:  # noqa: BLE001 — catalog absent → first create
                mode = "create"
            lance.write_dataset(
                table, CATALOG_URI, mode=mode,
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=MAX_ROWS_PER_FILE,
                storage_options=so,
            )
            log.info("appended %d rows (%d datasets, mode=%s) → %s",
                     len(appended_rows), len(appended_names), mode, CATALOG_URI)
            indexes_built = _build_indexes(so)
        else:
            log.info("no datasets changed — nothing appended. Catalog is current.")
            # Still ensure indices exist if the catalog is present (first-run-after-create safety).
            try:
                lance.dataset(CATALOG_URI, storage_options=so)
                indexes_built = _build_indexes(so)
            except Exception:  # noqa: BLE001 — no catalog yet, nothing to index
                pass
    except Exception as exc:  # noqa: BLE001
        status, error = "error", str(exc)

    if errored:
        log.warning("%d dataset(s) errored and were skipped (batch not aborted): %s",
                    len(errored), ", ".join(e["dataset_name"] for e in errored))

    completed_at = dt.datetime.now(dt.timezone.utc)
    _record_run(
        catalog_run_id=catalog_run_id,
        datasets_total=len(targets),
        datasets_appended=len(appended_names),
        datasets_skipped=len(skipped_names),
        rows_appended=len(appended_rows),
        indexes_built=indexes_built,
        status=status, error=error,
        metrics={"appended": appended_names, "skipped": skipped_names,
                 "errored": errored, "per_dataset": per_dataset},
        started_at=started_at, completed_at=completed_at,
    )

    if status != "success":
        raise RuntimeError(f"schema_catalog run failed: {error}")

    result = {"catalog_run_id": catalog_run_id, "catalog_uri": CATALOG_URI,
              "appended": appended_names, "skipped": skipped_names,
              "errored": errored, "rows_appended": len(appended_rows),
              "indexes_built": indexes_built, "per_dataset": per_dataset}

    if do_verify and not dry_run:
        # Verify ONLY the datasets that read cleanly — an errored/missing leaf was
        # already isolated above and must not re-raise here and sink an otherwise-good run.
        report = verify(ok_targets, so)
        _print_report(report)
        result["verify"] = {"catalog_rows": report["catalog_rows"],
                            "indices": report["indices"],
                            "datasets": {k: v["n_columns"] for k, v in report["datasets"].items()}}
    return result


def _parse_targets(cli_targets: list[str] | None) -> list[tuple[str, str]]:
    """CLI --target source_group=s3://... entries override the in-file TARGETS when
    provided; otherwise the default manifest is used."""
    if not cli_targets:
        return list(TARGETS)
    out: list[tuple[str, str]] = []
    for t in cli_targets:
        group, sep, uri = t.partition("=")
        if not sep or not group.strip() or not uri.strip():
            raise SystemExit(f"--target must be 'source_group=s3://...': got {t!r}")
        out.append((group.strip(), uri.strip()))
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m pipelines.catalog.schema_catalog",
        description="Catalog the live Lance schema of one or more datasets into schema_catalog.",
    )
    ap.add_argument("--target", action="append", metavar="GROUP=URI",
                    help="source_group=s3://... (repeatable). Overrides the default SAM targets.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print live schemas; write nothing.")
    ap.add_argument("--no-verify", action="store_true",
                    help="Skip the post-write read-back verification.")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    targets = _parse_targets(args.target)
    result = run(targets, dry_run=args.dry_run, do_verify=not args.no_verify)
    print(json.dumps({k: v for k, v in result.items() if k != "per_dataset"},
                     indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
