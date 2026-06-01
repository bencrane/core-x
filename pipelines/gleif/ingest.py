"""Compute worker — GLEIF Golden Copy bulk ingest (Level 1 LEI records / Level 2 relationships).

Part of the ``gleif-pipelines`` Modal app. Endpoint-less functions, spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints.
Clean-room data plane: no Iceberg, no Polaris — Lance is written straight to R2.

THE STREAMING-XML → ARROW PATH (approved for this feed). The directive specifies a
streaming ``lxml.etree.iterparse`` parse buffered into 100k-record batches written as
PyArrow tables straight to Lance — NOT the DuckDB transform path. That is the correct
shape here: GLEIF L1/L2 yield FLAT scalar rows (LEI, names, address parts, node IDs,
relationship type) with zero casting/filter/join, so there is no transform for DuckDB
to own — the only work is a lossless XML→row extraction, which is Python's I/O role
(the same class of concern as the uspto_tm XML→NDJSON transcode). Routing 8 GB of flat
records through an NDJSON spill + DuckDB re-read would be pure overhead. So:

    discovery API (latest publish)  → resolve full_file.xml url        (requests)
      → stream zip to /tmp                                              (Python: I/O only)
      → lxml.iterparse the single XML member, tag-filtered to the record element,
        fast_iter prune (clear + drop consumed siblings → FLAT RSS over the ~8 GB
        uncompressed L1 stream)                                         (Python: I/O only)
      → buffer 100,000 rows → pa.Table under an EXPLICIT schema → lance.write_dataset
        (first batch overwrite, rest append; v2.1; DIRECT to R2)
      → BTREE scalar index(es) on the resolution key(s).

Daily full-snapshot model (matches sam_opps). GLEIF republishes a COMPLETE golden-copy
snapshot every day, so each run downloads the latest ``full_file`` and OVERWRITEs the
Lance dataset — today's dataset always equals today's published universe; Lance's
immutable-manifest MVCC retains prior daily versions for free point-in-time time-travel.
The "initial backfill" and the daily run are the SAME operation. No delta merge, no
merge-key reconciliation.

Two datasets, DISTINCT Lance manifests → they fan out in PARALLEL with no shared-writer
conflict:
    l1  lei2  LEIRecord          → s3://data-sink/active/gleif_l1_entities/        BTREE lei
    l2  rr    RelationshipRecord  → s3://data-sink/active/gleif_l2_relationships/   BTREE lei, parent_lei

The L2 ``lei`` column is the StartNode (child) LEI — the natural graph anchor and the
literal ``lei`` index the directive requires; ``parent_lei`` is the EndNode (parent) LEI,
indexed for reverse traversal so "children-of(LEI)" is as instant as "parent-of(LEI)".

Control plane (Trigger v4 durable callback): on terminal state (success OR failure) the
worker (1) writes a run row to ops.gleif_runs via psycopg and (2) POSTs a FLAT JSON body
{status, rows, feed, level, run_mode, dataset_uri, publish_date} to trigger_callback_url.
No {"data": ...} envelope.

    modal deploy pipelines/gleif/ingest.py
    modal run    pipelines/gleif/ingest.py::migrate                  # create ops.gleif_runs
    modal run    pipelines/gleif/ingest.py::backfill                 # ingest l1 + l2 in parallel
    modal run    pipelines/gleif/ingest.py::ingest  --level l1
    modal run    pipelines/gleif/ingest.py::reindex --level l2
"""

from __future__ import annotations

import os

import modal

# GLEIF Golden Copy discovery endpoint. data[0] is the most-recent publish; each carries
# lei2 (Level 1) / rr (Level 2) / repex nodes, every node exposing full_file.{csv,json,xml}.
DISCOVERY_URL = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes"

SCRATCH_DIR = "/tmp/gleif"

# Buffer size: records per PyArrow batch / Lance fragment-write (directive: ~100k).
BATCH_ROWS = 100_000

# Lance fragment sizing — fleet-standard (also the Lance defaults).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new datasets → pin the current Lance default, matching every sibling worker.
DATA_STORAGE_VERSION = "2.1"

# Idempotent ops.* DDL — mirror of pipelines/gleif/ops_gleif_runs.sql (source of truth).
_OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.gleif_runs (
    id                     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    level                  text        NOT NULL,
    feed                   text        NOT NULL,
    run_mode               text        NOT NULL,
    write_mode             text,
    dataset_uri            text,
    publish_date           text,
    source_file            text,
    record_count_published bigint,
    rows_processed         bigint,
    status                 text        NOT NULL,
    error                  text,
    started_at             timestamptz,
    completed_at           timestamptz,
    recorded_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS gleif_runs_level_idx       ON ops.gleif_runs (level);
CREATE INDEX IF NOT EXISTS gleif_runs_feed_idx        ON ops.gleif_runs (feed);
CREATE INDEX IF NOT EXISTS gleif_runs_status_idx      ON ops.gleif_runs (status);
CREATE INDEX IF NOT EXISTS gleif_runs_publish_date_idx ON ops.gleif_runs (publish_date DESC);
CREATE INDEX IF NOT EXISTS gleif_runs_recorded_at_idx ON ops.gleif_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "lxml>=5.2",             # streaming iterparse XML parse
    "requests>=2.32",        # discovery API + zip download + Trigger callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    # BTREE scalar-index builds sort the column. Lance's spill-to-disk sorter uses a small
    # bounded DataFusion pool that can OOM on the 3.3M-row high-cardinality `lei`. Force
    # the in-memory sort path (container RAM) — cheap at this scale.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("gleif-pipelines", image=image)


# ──────────────────────────────────────────────────────────────────────────────
# Per-level configuration. Each carries: discovery node key, the record element
# local/wildcard tag for iterparse, the flat extractor, the explicit Arrow schema,
# and the BTREE index plan. `{*}LocalName` is the lxml namespace wildcard — it keeps
# the parse correct across a GLEIF schema-year bump (leidata/2016 → …).
# ──────────────────────────────────────────────────────────────────────────────
def _localname(tag) -> str:
    return str(tag).split("}", 1)[-1] if isinstance(tag, str) else ""


def _extract_l1(rec) -> dict:
    """One LEIRecord → flat row. Faithful extraction only (trim + empty→None); no casting."""
    def g(path: str):
        t = rec.findtext(path)
        return t.strip() if t and t.strip() else None

    return {
        "lei": g("{*}LEI"),
        "legal_name": g("{*}Entity/{*}LegalName"),
        "legal_address_city": g("{*}Entity/{*}LegalAddress/{*}City"),
        "legal_address_region": g("{*}Entity/{*}LegalAddress/{*}Region"),
        "legal_address_country": g("{*}Entity/{*}LegalAddress/{*}Country"),
        "registration_authority_id": g("{*}Entity/{*}RegistrationAuthority/{*}RegistrationAuthorityID"),
        "registration_authority_entity_id": g("{*}Entity/{*}RegistrationAuthority/{*}RegistrationAuthorityEntityID"),
        "entity_status": g("{*}Entity/{*}EntityStatus"),
    }


def _extract_l2(rec) -> dict:
    """One RelationshipRecord → flat edge row. StartNode=child (the `lei` anchor),
    EndNode=parent. Faithful extraction only."""
    def g(path: str):
        t = rec.findtext(path)
        return t.strip() if t and t.strip() else None

    return {
        "lei": g("{*}Relationship/{*}StartNode/{*}NodeID"),
        "parent_lei": g("{*}Relationship/{*}EndNode/{*}NodeID"),
        "relationship_type": g("{*}Relationship/{*}RelationshipType"),
        "relationship_status": g("{*}Relationship/{*}RelationshipStatus"),
    }


def _l1_schema():
    import pyarrow as pa

    return pa.schema([
        ("lei", pa.string()),
        ("legal_name", pa.string()),
        ("legal_address_city", pa.string()),
        ("legal_address_region", pa.string()),
        ("legal_address_country", pa.string()),
        ("registration_authority_id", pa.string()),
        ("registration_authority_entity_id", pa.string()),
        ("entity_status", pa.string()),
        ("source_file", pa.string()),
        ("publish_date", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])


def _l2_schema():
    import pyarrow as pa

    return pa.schema([
        ("lei", pa.string()),          # StartNode — child LEI (graph anchor)
        ("parent_lei", pa.string()),   # EndNode — parent LEI
        ("relationship_type", pa.string()),
        ("relationship_status", pa.string()),
        ("source_file", pa.string()),
        ("publish_date", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])


DATASETS: dict[str, dict] = {
    "l1": {
        "feed": "gleif_l1_entities",
        "publish_key": "lei2",
        "record_tag": "{*}LEIRecord",
        "record_local": "LEIRecord",
        "lance_uri": os.environ.get("GLEIF_L1_LANCE_URI", "s3://data-sink/active/gleif_l1_entities/"),
        "extract": _extract_l1,
        "schema": _l1_schema,
        "btree": ["lei"],
    },
    "l2": {
        "feed": "gleif_l2_relationships",
        "publish_key": "rr",
        "record_tag": "{*}RelationshipRecord",
        "record_local": "RelationshipRecord",
        "lance_uri": os.environ.get("GLEIF_L2_LANCE_URI", "s3://data-sink/active/gleif_l2_relationships/"),
        "extract": _extract_l2,
        "schema": _l2_schema,
        "btree": ["lei", "parent_lei"],
    },
}


def _resolve_level(level: str) -> dict:
    key = level.strip().lower()
    if key not in DATASETS:
        raise ValueError(f"level must be one of {sorted(DATASETS)}, got {level!r}")
    return DATASETS[key]


# ──────────────────────────────────────────────────────────────────────────────
# R2 / object-store
# ──────────────────────────────────────────────────────────────────────────────
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


# ──────────────────────────────────────────────────────────────────────────────
# Discovery + download
# ──────────────────────────────────────────────────────────────────────────────
def _resolve_publish(cfg: dict) -> dict:
    """Hit the discovery API and resolve the latest publish's full_file.xml node for this
    level. Returns {url, record_count, publish_date, size, source_file}."""
    import requests

    resp = requests.get(DISCOVERY_URL, headers={"Accept": "application/json"}, timeout=(15, 60))
    resp.raise_for_status()
    publishes = (resp.json() or {}).get("data") or []
    if not publishes:
        raise RuntimeError("GLEIF discovery returned no publishes")
    # data is newest-first; pick the max publish_date defensively.
    pub = max(publishes, key=lambda p: p.get("publish_date") or "")
    node = pub.get(cfg["publish_key"], {}).get("full_file", {}).get("xml")
    if not node or not node.get("url"):
        raise RuntimeError(
            f"no full_file.xml url for '{cfg['publish_key']}' in publish {pub.get('publish_date')}"
        )
    url = node["url"]
    return {
        "url": url,
        "record_count": int(node.get("record_count") or 0),
        "publish_date": pub.get("publish_date"),
        "size": int(node.get("size") or 0),
        "source_file": url.rsplit("/", 1)[-1],
    }


def _download(url: str, dest: str) -> int:
    """Stream the zip to local scratch (Python: I/O only). Returns byte count."""
    import requests

    total = 0
    with requests.get(url, stream=True, timeout=(30, 1800)) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)
                    total += len(chunk)
    return total


# ──────────────────────────────────────────────────────────────────────────────
# Streaming parse → batched Arrow → Lance
# ──────────────────────────────────────────────────────────────────────────────
def _rss_mb() -> float:
    """Resident set size in MiB (Linux ru_maxrss is KiB). For the memory-stability log."""
    import resource

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def _build_table(rows: list[dict], schema, source_file: str, publish_date, ingested_at):
    """Stamp provenance onto the flat rows and coerce to a fixed-schema Arrow table. The
    explicit schema makes every batch byte-identical in type → Lance append never drifts."""
    import pyarrow as pa

    for r in rows:
        r["source_file"] = source_file
        r["publish_date"] = publish_date
        r["ingested_at"] = ingested_at
    return pa.Table.from_pylist(rows, schema=schema)


def _stream_to_lance(zip_path: str, cfg: dict, source_file: str, publish_date,
                     ingested_at, so: dict) -> int:
    """fast_iter stream the single XML member → 100k-row Arrow batches → Lance (first batch
    overwrite, rest append). Memory stays flat: the parsed subtree is cleared per record and
    consumed siblings are dropped, and each batch buffer is released after the write. Returns
    the total records written."""
    import zipfile

    import lance
    from lxml import etree

    schema = cfg["schema"]()
    record_tag = cfg["record_tag"]
    uri = cfg["lance_uri"]

    rows: list[dict] = []
    total = 0
    first_write = True

    def flush() -> None:
        nonlocal rows, total, first_write
        if not rows:
            return
        table = _build_table(rows, schema, source_file, publish_date, ingested_at)
        lance.write_dataset(
            table, uri,
            mode=("overwrite" if first_write else "append"),
            schema=schema,
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        total += len(rows)
        print(f"  [{cfg['feed']}] batch → {'overwrite' if first_write else 'append'}: "
              f"+{len(rows):,} (total {total:,}); peak RSS {_rss_mb():,.0f} MiB")
        first_write = False
        rows = []
        del table

    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not members:
            raise RuntimeError(f"no .xml member in {zip_path} (members={zf.namelist()[:5]})")
        member = max(members, key=lambda n: zf.getinfo(n).file_size)
        print(f"  [{cfg['feed']}] parsing member {member!r}")
        with zf.open(member) as xf:
            context = etree.iterparse(
                xf, events=("end",), tag=record_tag,
                resolve_entities=False, load_dtd=False, no_network=True,
                huge_tree=True, recover=True,
            )
            extract = cfg["extract"]
            for _, elem in context:
                rows.append(extract(elem))
                # fast_iter prune: free this record and every consumed sibling before it.
                elem.clear()
                parent = elem.getparent()
                if parent is not None:
                    while elem.getprevious() is not None:
                        del parent[0]
                if len(rows) >= BATCH_ROWS:
                    flush()
            del context
        flush()  # final partial batch (also handles a sub-batch dataset → overwrite)

    # Defensive: a tag/namespace mismatch would silently yield 0 records — never write an
    # empty dataset over a good one without shouting.
    if total == 0:
        raise RuntimeError(
            f"parsed 0 {cfg['record_local']} elements from {source_file} — tag/namespace mismatch?"
        )
    return total


def _create_indexes(cfg: dict, so: dict) -> list[str]:
    """BTREE scalar index on each resolution key (replace=True → idempotent). An index miss
    is logged, never fatal — the Lance data write is the critical artifact."""
    import lance

    ds = lance.dataset(cfg["lance_uri"], storage_options=so)
    built: list[str] = []
    for col in cfg["btree"]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    return built


# ──────────────────────────────────────────────────────────────────────────────
# State + callback
# ──────────────────────────────────────────────────────────────────────────────
def _record_run(level, feed, run_mode, write_mode, dataset_uri, publish_date, source_file,
                record_count_published, rows_processed, status, error,
                started_at, completed_at) -> None:
    """Terminal run row → ops.gleif_runs (psycopg). Best-effort: never let an audit-write
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
                INSERT INTO ops.gleif_runs
                    (level, feed, run_mode, write_mode, dataset_uri, publish_date, source_file,
                     record_count_published, rows_processed, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (level, feed, run_mode, write_mode, dataset_uri, publish_date, source_file,
                 record_count_published, rows_processed, status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url. FLAT JSON body — NO
    {"data": ...} envelope, NO API key (the callbackHash in the url is the auth)."""
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


# ──────────────────────────────────────────────────────────────────────────────
# Workers
# ──────────────────────────────────────────────────────────────────────────────
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120,   # L1 ≈ 3.3M records, ~8 GB uncompressed stream + BTREE build
    memory=16384,       # directive: 8–16 GB; iterparse keeps RSS flat, 16 GiB is headroom
    cpu=4.0,
)
def ingest_gleif(level: str, url: str | None = None,
                 trigger_callback_url: str | None = None) -> dict:
    """Resolve latest publish → stream-download zip → iterparse → 100k Arrow batches → Lance
    overwrite DIRECT to R2 → BTREE index; record ops.* + wake Trigger. Re-raises on failure
    so the Modal call is marked failed."""
    import datetime as dt
    import os.path
    import shutil

    cfg = _resolve_level(level)
    lvl = level.strip().lower()
    uri = cfg["lance_uri"]
    started_at = dt.datetime.now(dt.timezone.utc)
    ingested_at = started_at
    rows = 0
    published = 0
    publish_date = None
    source_file = None
    status = "error"
    error: str | None = None
    write_mode = "overwrite"
    built: list[str] = []

    try:
        so = _r2_storage_options()
        os.makedirs(SCRATCH_DIR, exist_ok=True)

        info = _resolve_publish(cfg) if not url else {
            "url": url, "record_count": 0, "publish_date": None,
            "size": 0, "source_file": url.rsplit("/", 1)[-1],
        }
        publish_date = info["publish_date"]
        published = info["record_count"]
        source_file = info["source_file"]
        print(f"[{lvl}] publish={publish_date} file={source_file} "
              f"published={published:,} size={info['size'] / 1e6:,.1f} MB")

        zip_path = os.path.join(SCRATCH_DIR, source_file)
        nbytes = _download(info["url"], zip_path)
        print(f"[{lvl}] downloaded {nbytes / 1e6:,.1f} MB → {zip_path}")

        rows = _stream_to_lance(zip_path, cfg, source_file, publish_date, ingested_at, so)
        print(f"[{lvl}] wrote {rows:,} rows → {uri}")
        if published and abs(rows - published) > max(1000, published * 0.01):
            print(f"WARN: parsed {rows:,} vs published {published:,} (>1% drift)")

        built = _create_indexes(cfg, so)

        try:
            os.remove(zip_path)
        except OSError:
            pass
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        shutil.rmtree(SCRATCH_DIR, ignore_errors=True)
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(lvl, cfg["feed"], "full_overwrite", write_mode, uri, publish_date,
                    source_file, int(published), int(rows), status, error,
                    started_at, completed_at)
        _post_callback(trigger_callback_url, {
            "status": status, "rows": int(rows), "feed": cfg["feed"], "level": lvl,
            "run_mode": "full_overwrite", "dataset_uri": uri, "publish_date": publish_date,
        })

    if status != "success":
        raise RuntimeError(f"gleif ingest failed for level={lvl}: {error}")
    return {"status": status, "level": lvl, "feed": cfg["feed"], "run_mode": "full_overwrite",
            "rows_processed": int(rows), "record_count_published": int(published),
            "indices": built, "dataset_uri": uri, "publish_date": publish_date}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")],
              timeout=60 * 45, memory=16384, cpu=4.0)
def reindex(level: str) -> dict:
    """Rebuild the BTREE scalar index(es) on an existing dataset (no re-ingest)."""
    import lance

    cfg = _resolve_level(level)
    so = _r2_storage_options()
    rows = lance.dataset(cfg["lance_uri"], storage_options=so).count_rows()
    print(f"Reindexing {cfg['lance_uri']} — {rows:,} rows")
    built = _create_indexes(cfg, so)
    return {"level": level.strip().lower(), "dataset_uri": cfg["lance_uri"], "rows": rows,
            "indexes": built}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def apply_migration() -> dict:
    """Create ops.gleif_runs (idempotent). Mirrors ops_gleif_runs.sql."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(_OPS_DDL)
        conn.commit()
        cur.execute("SELECT to_regclass('ops.gleif_runs')")
        present = cur.fetchone()[0]
    print(f"ops.gleif_runs present = {present}")
    return {"table": "ops.gleif_runs", "present": present}


# ──────────────────────────────────────────────────────────────────────────────
# Manual ops entrypoints (local — no callback). ops.* write still fires.
# ──────────────────────────────────────────────────────────────────────────────
@app.local_entrypoint()
def migrate() -> None:
    import json

    print(json.dumps(apply_migration.remote(), indent=2, default=str))


@app.local_entrypoint()
def ingest(level: str = "l1") -> None:
    import json

    print(json.dumps(ingest_gleif.remote(level, trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def backfill() -> None:
    """Initial backfill: ingest l1 + l2 in PARALLEL (distinct Lance datasets → no
    shared-writer conflict)."""
    import json

    print("=== parallel backfill: l1 (entities) ‖ l2 (relationships) ===")
    calls = {lvl: ingest_gleif.spawn(lvl, trigger_callback_url=None) for lvl in DATASETS}
    results: dict[str, dict] = {}
    for lvl, call in calls.items():
        results[lvl] = call.get()
        print(json.dumps(results[lvl], default=str))

    print("\n=== FINAL ROW COUNTS ===")
    for lvl, r in results.items():
        print(f"  {lvl:3s} {r.get('feed'):24s} rows={r.get('rows_processed', 0):>10,} "
              f"-> {r.get('dataset_uri')}")


@app.local_entrypoint()
def reindex_one(level: str = "l1") -> None:
    import json

    print(json.dumps(reindex.remote(level), indent=2, default=str))
