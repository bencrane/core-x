"""Compute worker — cross-state SoS entity normalization (CA · NY · FL · CO).

Part of the ``sos-normalized`` Modal app. Endpoint-less functions, driven by the
local entrypoints (or the Universal Dispatcher). Clean-room data plane: DuckDB does
100% of the transform, Lance is the system of record, written straight to R2.

This worker does NOT ingest upstream files. It reads the four already-normalized
state SoS Lance datasets (each produced by its own domain worker) and projects them
into ONE unified entity-resolution layout for a future fuzzy-matching crosswalk:

    source_state          'CA' | 'NY' | 'FL' | 'CO'   (the registry of origin)
    original_entity_id    each state's native entity/document key (VARCHAR, never cast)
    normalized_legal_name UPPER, &→AND, dash→space, punctuation-stripped, ws-collapsed name
    legal_name_base       normalized_legal_name minus trailing corp designators (LLC/INC/…)
    street_address        principal/business street (address lines joined)
    city · state · zip_code   principal/business locality (zip_code = ZIP5 blocking key)
    entity_status         source status (UPPER/trim; FL 1-char code decoded)
    + provenance: source_entity_name (raw), source_dataset, snapshot_date, ingested_at

Output: ONE combined Lance dataset s3://data-sink/active/sos_normalized_master/
(memory permits the union easily — entity grain is ~18M rows, narrow projection).
Mandatory BTREE blocking indexes on normalized_legal_name, legal_name_base and zip_code;
a BITMAP on source_state for cheap per-state slicing.

Grounding guardrail: column names are NOT assumed. ``probe`` reads each
lance.dataset(uri).schema (read-only), surveys NY's competing address blocks by
population, and lists each state's status vocabulary. The projection below is built
strictly from what the probe returns.

    modal deploy pipelines/sos_normalized/normalize.py
    modal run    pipelines/sos_normalized/normalize.py::run_probe      # Step 1 — read-only schema probe
    modal run    pipelines/sos_normalized/normalize.py::run_normalize  # Steps 2/3 — project + materialize + index
    modal run    pipelines/sos_normalized/normalize.py::run_verify     # Verification — 3-row samples, 2 states
"""

from __future__ import annotations

import os

import modal

from core.name_norm import legal_name_base as _legal_name_base
from core.name_norm import name_norm as _name_norm

BUCKET = "data-sink"
SCRATCH_DIR = "/tmp"
AS_OF_DEFAULT = "2026-05-31"

# Unified output (combined master — superior for cross-state blocking: one index per
# blocking key spans every state, candidate pairs cross state lines for free).
MASTER_URI = os.environ.get("SOS_NORMALIZED_MASTER_URI", "s3://data-sink/active/sos_normalized_master/")

# Source datasets — the four state SoS entity spines (system-of-record Lance tables
# written by the per-state domain workers), plus the two "evaluate if needed" members
# the directive named. The probe reads all six; only the four entity spines feed the
# projection (CA principals / FL events carry no entity-grain business address).
SOURCES: dict[str, str] = {
    "ca_sos_entities": "s3://data-sink/active/ca_sos_entities/",
    "ny_sos": "s3://data-sink/active/ny_sos/",
    "fl_sos_corporations": "s3://data-sink/active/fl_sos_corporations/",
    "co_sos": "s3://data-sink/active/co_sos/",
}
EVALUATE: dict[str, str] = {
    "ca_sos_principals": "s3://data-sink/active/ca_sos_principals/",
    "fl_sos_events": "s3://data-sink/active/fl_sos_events/",
}

# NY has no single business-address block — it carries four (service-of-process, CEO,
# registered agent, principal location). The probe counts each block's population so the
# choice of which feeds the unified address is evidence-driven, not assumed.
NY_ADDRESS_BLOCKS = ["dos_process", "ceo", "registered_agent", "location"]

# Lance fragment sizing — fleet constant (90 GiB; rows-per-file binds first).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"

# Mandatory blocking indexes (directive) + a cheap categorical bitmap. legal_name_base is a
# load-bearing resolution key (suffix-stripped cross-layer join), so it gets a hard BTREE too.
MASTER_BTREE_INDEXES = ["normalized_legal_name", "legal_name_base", "zip_code"]
MASTER_BITMAP_INDEXES = ["source_state"]

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sos_normalized_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phase          text        NOT NULL,
    dataset_uri    text,
    as_of          date,
    rows_processed bigint,
    per_state      jsonb,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sos_normalized_runs_phase_idx       ON ops.sos_normalized_runs (phase);
CREATE INDEX IF NOT EXISTS sos_normalized_runs_status_idx      ON ops.sos_normalized_runs (status);
CREATE INDEX IF NOT EXISTS sos_normalized_runs_recorded_at_idx ON ops.sos_normalized_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",
    "psycopg[binary]>=3.2",  # ops.* terminal state (ARCHITECTURE.md §5)
).env(
    # BTREE scalar-index builds sort the column; bypass Lance's bounded spill-to-disk
    # sorter, which OOMs on high-cardinality string columns (normalized_legal_name over
    # ~18M rows). In-memory sort is well within the container RAM. (lance-format/lance#2650)
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("core.name_norm")  # ship the canonical blocking-key macro to the container

app = modal.App("sos-normalized", image=image)


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


# ───────────────────────── Step 1 — read-only schema probe ─────────────────────────
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 30,
    memory=16384,
    cpu=4.0,
)
def probe() -> dict:
    """Read-only. For every named dataset: dump lance.dataset(uri).schema (field name +
    type) and row count. Then survey NY's four address blocks by population and list each
    state's status vocabulary — the two open questions the projection must resolve from
    evidence rather than assumption. Prints a JSON report; mutates nothing."""
    import json

    import duckdb
    import lance

    so = _r2_storage_options()
    report: dict[str, dict] = {}

    all_targets = {**SOURCES, **EVALUATE}
    for name, uri in all_targets.items():
        entry: dict = {"uri": uri}
        try:
            ds = lance.dataset(uri, storage_options=so)
            entry["n_rows"] = ds.count_rows()
            entry["fields"] = [(f.name, str(f.type)) for f in ds.schema]
        except Exception as exc:  # noqa: BLE001
            entry["error"] = str(exc)
        report[name] = entry

    print("=== SCHEMA PROBE (read-only) ===")
    print(json.dumps(report, indent=2))

    # ── NY address-block population: which block best feeds the unified address? ──
    ny_block = {}
    try:
        ds = lance.dataset(SOURCES["ny_sos"], storage_options=so)
        ny_fields = {f.name for f in ds.schema}
        want = []
        for blk in NY_ADDRESS_BLOCKS:
            for suffix in ("address_1", "city", "state", "zip"):
                col = f"{blk}_{suffix}"
                if col in ny_fields:
                    want.append(col)
        if want:
            tbl = ds.to_table(columns=want)
            con = duckdb.connect(":memory:")
            con.register("ny", tbl)
            total = con.execute("SELECT count(*) FROM ny").fetchone()[0]
            ny_block["total_rows"] = total
            for blk in NY_ADDRESS_BLOCKS:
                zc = f"{blk}_zip"
                ac = f"{blk}_address_1"
                stats = {}
                if zc in ny_fields:
                    stats["zip_nonnull"] = con.execute(
                        f'SELECT count("{zc}") FROM ny').fetchone()[0]
                if ac in ny_fields:
                    stats["addr1_nonnull"] = con.execute(
                        f'SELECT count("{ac}") FROM ny').fetchone()[0]
                ny_block[blk] = stats
            con.close()
        ny_block["status_like_fields"] = sorted(
            f for f in ny_fields if "status" in f.lower())
    except Exception as exc:  # noqa: BLE001
        ny_block["error"] = str(exc)
    print("\n=== NY ADDRESS-BLOCK POPULATION ===")
    print(json.dumps(ny_block, indent=2))

    # ── status vocabulary per state (top values by frequency) ──
    status_probe = {
        "ca_sos_entities": "entity_status",
        "fl_sos_corporations": "status",
        "co_sos": "entity_status",
    }
    vocab: dict[str, object] = {}
    for name, col in status_probe.items():
        try:
            ds = lance.dataset(SOURCES[name], storage_options=so)
            if col not in {f.name for f in ds.schema}:
                vocab[name] = {"error": f"column {col!r} not in schema"}
                continue
            tbl = ds.to_table(columns=[col])
            con = duckdb.connect(":memory:")
            con.register("t", tbl)
            rows = con.execute(
                f'SELECT "{col}" AS v, count(*) AS n FROM t '
                f'GROUP BY 1 ORDER BY n DESC LIMIT 25').fetchall()
            con.close()
            vocab[name] = {"column": col, "top_values": [[r[0], r[1]] for r in rows]}
        except Exception as exc:  # noqa: BLE001
            vocab[name] = {"error": str(exc)}
    print("\n=== STATUS VOCABULARY (top 25 by freq) ===")
    print(json.dumps(vocab, indent=2, default=str))

    return {"schema": report, "ny_address_blocks": ny_block, "status_vocab": vocab}


@app.local_entrypoint()
def run_probe() -> None:
    """Step 1 — read-only schema probe across all six named datasets."""
    probe.remote()


# ───────────── Step 2 — the normalization mapping (grounded in the probe) ─────────────
# Built strictly from the live lance.dataset(uri).schema. Every source column below was
# confirmed present by ``probe``. Decisions the probe forced:
#   · NY business address  → dos_process_* block (98.1% zip-populated; the only block
#                            with the coverage a blocking key needs — ceo 11.2%,
#                            registered_agent 20.0%, location 8.5%).
#   · NY entity_status     → literal 'ACTIVE' (no status column exists; the feed IS the
#                            "Active Corporations" extract).
#   · FL status            → decode A→ACTIVE / I→INACTIVE (vocabulary is exactly {A,I}).
#   · CA / CO entity_status → source vocabulary preserved (UPPER/trim only).
# CA principal_* / FL principal_* / CO principal_* hold the business address inline, so
# ca_sos_principals and fl_sos_events are NOT joined (sub-entity grain, no business addr).
STATE_PROJECTIONS: dict[str, dict] = {
    "CA": {
        "uri": SOURCES["ca_sos_entities"], "table": "src_ca",
        "cols": ["entity_num", "entity_name", "entity_status",
                 "principal_address", "principal_address2", "principal_city",
                 "principal_state", "principal_postal_code", "snapshot_date"],
        "id": "entity_num", "name": "entity_name",
        "a1": "principal_address", "a2": "principal_address2",
        "city": "principal_city", "state": "principal_state", "zip": "principal_postal_code",
        "status_sql": "nullif(upper(trim(CAST(entity_status AS VARCHAR))), '')",
    },
    "NY": {
        "uri": SOURCES["ny_sos"], "table": "src_ny",
        "cols": ["dos_id", "current_entity_name", "dos_process_address_1",
                 "dos_process_address_2", "dos_process_city", "dos_process_state",
                 "dos_process_zip", "snapshot_date"],
        "id": "dos_id", "name": "current_entity_name",
        "a1": "dos_process_address_1", "a2": "dos_process_address_2",
        "city": "dos_process_city", "state": "dos_process_state", "zip": "dos_process_zip",
        "status_sql": "'ACTIVE'",  # feed is the Active Corporations extract; no status col
    },
    "FL": {
        "uri": SOURCES["fl_sos_corporations"], "table": "src_fl",
        "cols": ["document_number", "corporate_name", "status",
                 "principal_address_1", "principal_address_2", "principal_city",
                 "principal_state", "principal_zip", "snapshot_date"],
        "id": "document_number", "name": "corporate_name",
        "a1": "principal_address_1", "a2": "principal_address_2",
        "city": "principal_city", "state": "principal_state", "zip": "principal_zip",
        "status_sql": ("CASE upper(trim(CAST(status AS VARCHAR))) "
                       "WHEN 'A' THEN 'ACTIVE' WHEN 'I' THEN 'INACTIVE' "
                       "ELSE nullif(upper(trim(CAST(status AS VARCHAR))), '') END"),
    },
    "CO": {
        "uri": SOURCES["co_sos"], "table": "src_co",
        "cols": ["entity_id", "entity_name", "entity_status",
                 "principal_address_1", "principal_address_2", "principal_city",
                 "principal_state", "principal_zip", "snapshot_date"],
        "id": "entity_id", "name": "entity_name",
        # CO's raw entity_name carries trailing status decorations ("… DELINQUENT MAY 1
        # 2016"); scrub them off the blocking-key input only (Task A — _co_status_scrub).
        "strip_status_decorations": True,
        "a1": "principal_address_1", "a2": "principal_address_2",
        "city": "principal_city", "state": "principal_state", "zip": "principal_zip",
        "status_sql": "nullif(upper(trim(CAST(entity_status AS VARCHAR))), '')",
    },
}


# ── SQL normalization fragments (DuckDB). \\s / \\x{..} in Python source → \s / \x{..} in SQL. ──
# _name_norm (the canonical blocking key) and _legal_name_base (its suffix-stripped base) are
# the single source of truth for the whole fleet — imported above from core/name_norm.py so
# every spine/bridge emits byte-identical SQL and the rule cannot drift across copies.


# ── Colorado status-decoration scrub (Task A) ──────────────────────────────────────────
# CO's SoS feed appends a status clause to the raw entity name, e.g.
#   "ACME WIDGETS LLC DELINQUENT MAY 1 2016"      "FOO INC DISSOLVED APRIL 20"
# The clause is [<modifier>] <STATUS> [<effective date…>] and always trails the legal name.
# It is not part of the name and detonates the blocking key, so strip it BEFORE the value
# reaches _name_norm. To spare legitimate names that merely contain a status word (e.g.
# "BIG MERGED MEDIA INC"), cut only when the status word is whitespace-led AND runs to
# end-of-string or is followed by date debris (a month name or a digit) — the structural
# signature of a real decoration. Applied case-insensitively to the raw (mixed-case) name.
_CO_STATUS_MODIFIERS = ["ADMINISTRATIVELY", "VOLUNTARILY", "JUDICIALLY"]
_CO_STATUS_KEYWORDS = [
    "DELINQUENT", "DELINQUENCY", "DISSOLVED", "DISSOLUTION", "WITHDRAWN", "REVOKED",
    "SUSPENDED", "EXPIRED", "NONCOMPLIANT", "FORFEITED", "TERMINATED", "MERGED",
]
_CO_MONTHS = [
    "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST",
    "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER",
    "JAN", "FEB", "MAR", "APR", "JUN", "JUL", "AUG", "SEPT", "SEP", "OCT", "NOV", "DEC",
]
_CO_STATUS_SCRUB_RE = (
    r"\s+(?:(?:" + "|".join(_CO_STATUS_MODIFIERS) + r")\s+)?"
    r"(?:" + "|".join(_CO_STATUS_KEYWORDS) + r")"
    r"(?:\s+(?:(?:" + "|".join(_CO_MONTHS) + r")|\d).*)?$"
)


def _co_status_scrub(col: str) -> str:
    """Strip CO's trailing status decoration from a raw entity-name column (Task A),
    case-insensitively, before the value is handed to _name_norm."""
    return "regexp_replace(CAST(%s AS VARCHAR), '%s', '', 'gi')" % (col, _CO_STATUS_SCRUB_RE)


def _raw(col: str) -> str:
    """Faithful trimmed source value (no case/punct change). NULL if empty."""
    return "nullif(trim(CAST(%s AS VARCHAR)), '')" % col


def _street(a1: str, a2: str) -> str:
    """Join address line 1 + 2 (concat_ws skips NULLs) → collapse whitespace → trim."""
    return ("nullif(trim(regexp_replace(concat_ws(' ', CAST(%s AS VARCHAR),"
            " CAST(%s AS VARCHAR)), '\\s+', ' ', 'g')), '')") % (a1, a2)


def _city(col: str) -> str:
    return "nullif(trim(regexp_replace(CAST(%s AS VARCHAR), '\\s+', ' ', 'g')), '')" % col


def _state(col: str) -> str:
    return "nullif(upper(trim(CAST(%s AS VARCHAR))), '')" % col


def _zip5(col: str) -> str:
    """ZIP5 blocking key: digits-only, left 5 (leading zeros survive — string ops). NULL if none."""
    return "nullif(left(regexp_replace(CAST(%s AS VARCHAR), '[^0-9]', '', 'g'), 5), '')" % col


def _build_state_sql(code: str, spec: dict) -> str:
    """One state's SELECT → the canonical 12-column unified layout, read from its
    registered Arrow table. Every branch emits identical column names + types so the
    UNION ALL produces one perfectly-aligned schema. legal_name_base references the
    normalized_legal_name alias (DuckDB resolves SELECT-list aliases left-to-right), so the
    name-normalization chain is evaluated once per row, not twice."""
    # CO leaks status decorations into the raw entity name; scrub them off the value that
    # feeds the blocking key (Task A), but keep source_entity_name byte-faithful to source.
    name_src = (_co_status_scrub(spec["name"])
                if spec.get("strip_status_decorations") else spec["name"])
    return (
        "SELECT\n"
        f"    '{code}' AS source_state,\n"
        f"    {_raw(spec['id'])} AS original_entity_id,\n"
        f"    {_name_norm(name_src)} AS normalized_legal_name,\n"
        f"    {_legal_name_base('normalized_legal_name')} AS legal_name_base,\n"
        f"    {_raw(spec['name'])} AS source_entity_name,\n"
        f"    {_street(spec['a1'], spec['a2'])} AS street_address,\n"
        f"    {_city(spec['city'])} AS city,\n"
        f"    {_state(spec['state'])} AS state,\n"
        f"    {_zip5(spec['zip'])} AS zip_code,\n"
        f"    {spec['status_sql']} AS entity_status,\n"
        "    CAST(snapshot_date AS DATE) AS snapshot_date,\n"
        "    CAST(now() AS TIMESTAMP) AS ingested_at\n"
        f"FROM {spec['table']}"
    )


def _build_master_indexes(so: dict) -> list[str]:
    """Build the mandatory BTREE blocking indexes (normalized_legal_name, zip_code) +
    a source_state BITMAP. replace=True → idempotent. An index miss must not fail the build."""
    import lance

    ds = lance.dataset(MASTER_URI, storage_options=so)
    built: list[str] = []
    for col in MASTER_BTREE_INDEXES:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in MASTER_BITMAP_INDEXES:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    return built


def _list_committed_indices(ds) -> list:
    """Best-effort read of committed scalar indices (tolerant of pylance shape drift)."""
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


def _record_run(phase, dataset_uri, as_of, rows, per_state, status, error,
                started_at, completed_at) -> None:
    """Terminal run row → ops.sos_normalized_runs (psycopg). Best-effort."""
    import json

    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.sos_normalized_runs
                    (phase, dataset_uri, as_of, rows_processed, per_state,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
                """,
                (phase, dataset_uri, as_of, rows, json.dumps(per_state),
                 status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* state write failed: {exc}")


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90,
    memory=32768,
    cpu=8.0,
)
def normalize(as_of: str = AS_OF_DEFAULT) -> dict:
    """Steps 2/3 — read each state's entity spine (column-projection pushdown), apply the
    grounded normalization, UNION ALL into one aligned schema, write the combined Lance
    master (overwrite), then build the BTREE/BITMAP blocking indexes. Re-raises on failure."""
    import datetime as dt

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    rows = 0
    per_state: dict[str, int] = {}
    status, error = "error", None
    built: list[str] = []

    try:
        so = _r2_storage_options()
        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET memory_limit='26GB';")
            con.execute("SET temp_directory='/tmp/duckdb_spill';")
            for code, spec in STATE_PROJECTIONS.items():
                ds = lance.dataset(spec["uri"], storage_options=so)
                tbl = ds.to_table(columns=spec["cols"])  # projection pushdown
                con.register(spec["table"], tbl)
                per_state[code] = tbl.num_rows
                print(f"loaded {code}: {tbl.num_rows:,} rows  <- {spec['uri']}")
            union_sql = "\nUNION ALL\n".join(
                _build_state_sql(c, s) for c, s in STATE_PROJECTIONS.items())
            result = con.execute(union_sql).to_arrow_table()
            rows = result.num_rows
        finally:
            con.close()
        print(f"projected unified rows: {rows:,}  (per-state: {per_state})")

        lance.write_dataset(
            result,
            MASTER_URI,
            mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        del result
        print(f"wrote Lance master -> {MASTER_URI}")
        built = _build_master_indexes(so)
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("normalize", MASTER_URI, as_of, int(rows), per_state,
                    status, error, started_at, completed_at)

    if status != "success":
        raise RuntimeError(f"sos_normalized build failed: {error}")
    return {"status": status, "dataset_uri": MASTER_URI, "rows_processed": int(rows),
            "per_state": per_state, "indexes": built, "as_of": as_of}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")],
              timeout=60 * 45, memory=32768, cpu=8.0)
def reindex() -> dict:
    """(Re)build the blocking indexes on the existing master (no re-projection)."""
    so = _r2_storage_options()
    built = _build_master_indexes(so)
    return {"dataset_uri": MASTER_URI, "indexes": built, "index_count": len(built)}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")],
              timeout=60 * 15, memory=8192, cpu=2.0)
def verify(states: str = "CA,FL", sample: int = 3) -> dict:
    """Verification — row count, per-state breakdown, committed indices, and a
    ``sample``-row sample from two states proving aligned schema + clean name normalization."""
    import json

    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(MASTER_URI, storage_options=so)
    total = ds.count_rows()
    schema = [(f.name, str(f.type)) for f in ds.schema]
    committed = _list_committed_indices(ds)

    # per-state counts (read just the source_state column)
    sc = ds.to_table(columns=["source_state"])
    con = duckdb.connect(":memory:")
    con.register("m", sc)
    breakdown = con.execute(
        "SELECT source_state, count(*) n FROM m GROUP BY 1 ORDER BY 1").fetchall()
    con.close()

    print("=== sos_normalized_master ===")
    print(f"uri:        {MASTER_URI}")
    print(f"total rows: {total:,}")
    print(f"schema:     {json.dumps(schema)}")
    print(f"per-state:  {[(r[0], r[1]) for r in breakdown]}")
    print(f"indices:    {json.dumps(committed, default=str)}")

    samples: dict[str, list] = {}
    for st in [s.strip().upper() for s in states.split(",") if s.strip()]:
        tbl = ds.to_table(filter=f"source_state = '{st}'", limit=sample)
        samples[st] = tbl.to_pylist()
        print(f"\n=== SAMPLE — {st} ({sample} rows) ===")
        for row in samples[st]:
            print(json.dumps(row, indent=2, default=str))

    return {"uri": MASTER_URI, "total_rows": total, "schema": schema,
            "per_state": [[r[0], r[1]] for r in breakdown],
            "committed_indices": committed, "samples": samples}


@app.local_entrypoint()
def run_normalize(as_of: str = AS_OF_DEFAULT) -> None:
    """Steps 2/3 — project the four state spines into the combined master + index."""
    import json

    print(json.dumps(normalize.remote(as_of=as_of), indent=2, default=str))


@app.local_entrypoint()
def run_verify(states: str = "CA,FL", sample: int = 3) -> None:
    """Verification — print schema, per-state counts, indices, and 3-row samples."""
    import json

    print(json.dumps(verify.remote(states=states, sample=sample), indent=2, default=str))
