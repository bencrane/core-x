# Overture Places — Optimization Execution Directive (v2 migration)

**Audience:** an AI agent executing against the `core-x` data plane, cold, with no prior context.
**Companion:** [docs/overture_places_structural_diagnostic.md](overture_places_structural_diagnostic.md) — the read-only diagnostic this directive remediates. Read it first; every decision below traces to a measured finding there.
**Target SoR:** `s3://data-sink/active/overture_places/` (Gen-3 Lance, R2). 16,273,123 rows, single snapshot.
**Nature:** a **one-shot, re-ingest-free, in-place structural migration** of the committed SoR to schema **`overture_places.v2`**, plus go-forward parity in the ingest worker. The SoR URI does **not** change.
**Authority:** all engineering decisions in §2 are **locked** — do not re-litigate them. Execute the runbook in §6. The migration **mutates the SoR**; it is gated, idempotent, ledgered, and reversible.

---

## 1. Mission

Bring the canonical dataset to mathematical/structural optimality without re-ingesting from Overture (operator constraint: no continual re-ingest). Concretely:

1. **Demote 4 constant provenance columns** (`country`, `snapshot_date`, `release_tag`, `ingested_at`) from per-row data to **Lance schema metadata** — reclaims ~403 MiB decoded (`release_tag` alone 195 MB).
2. **Add a 1-D spatial key** (`hilbert`, `UINTEGER`) and **physically sort** the dataset by `(region, hilbert)` so fragment zone-maps prune — killing the measured **38.9 s** two-BTREE bbox pathology.
3. **Fix the index set:** drop the per-axis `longitude`/`latitude` BTREEs (wrong structure for 2-D, large), add a `hilbert` BTREE and a `category` BITMAP.
4. **Normalize `region`** (131 dirty values → ~56 clean USPS codes + NULL) and **recast `confidence`** `double→float32`.
5. **Update the ingest worker** so any future re-ingest is born in v2.

**Non-negotiables (safety contract):**
- **Row-preserving:** output row count and `DISTINCT(id)` MUST equal the source (16,273,123). No filtering, no dedup, no row drops.
- **Build → verify → publish:** the new dataset is built and fully verified on LOCAL disk; R2 is mutated only after a HARD local gate passes.
- **Backup before wipe:** the current R2 prefix is server-side-copied to a backup prefix before the publish wipe; post-publish verification failure triggers automatic restore.
- **Ledgered:** terminal state recorded in `ops.overture_places_runs` (`write_path='optimize'`).

---

## 2. Locked engineering decisions (rationale baked in)

| # | Decision | Rationale (traces to diagnostic) |
|---|---|---|
| **D1** | **Transform the committed SoR in place; do NOT re-ingest from Overture.** Read R2 → transform → republish to the same URI. | Operator: no continual re-ingest. The committed dataset already has the US filter + spatial flatten; everything needed (lon/lat, region, etc.) is present. Re-pulling risks release drift. |
| **D2** | **Spatial key = `ST_Hilbert(lon, lat, bounds[-180,-90,180,90])` → `UINTEGER`.** Sort `(region, hilbert)`; BTREE on `hilbert`. | `ST_GeoHash` does not exist in DuckDB 1.5 spatial (verified). `ST_QuadKey` is Web-Mercator, **undefined beyond ±85.05° lat** — and the diagnostic found real lat −89.9° outliers. Hilbert with global **linear** bounds maps every coordinate validly, has superior curve locality, and is a compact 4-byte integer (smaller BTREE than a 12-char quadkey string). Verified: neighbors Δ=5, distant Δ=8.8M, outlier maps clean. |
| **D3** | **Sort region-primary, spatial-secondary** (`ORDER BY region NULLS LAST, hilbert`). | Territory/by-state filtering is the dominant access pattern for a GTM places SoR; region-primary gives fragment pruning for `region=…` AND bbox-within-region pruning. (If pure cross-state bbox later dominates, flip to `ORDER BY hilbert` — a one-line change in a future rewrite.) |
| **D4** | **Drop the `longitude` & `latitude` BTREE indices.** Keep lon/lat as `double` data columns. | Measured: per-axis BTREEs serve 2-D bbox via a multi-million-row-id `AND`-intersect = **38.9 s**. They are large (near-unique doubles) and structurally wrong for the 2-D access. `hilbert` replaces them. Single-axis coordinate range is degenerate (a scan beats it over R2 — see diagnostic §5-F.1). |
| **D5** | **Add `category` BITMAP** (1,574 NDV). | Diagnostic: the primary categorical access key is unindexed; roaring BITMAP is correct for ≤~few-thousand-cardinality equality. |
| **D6** | **REJECT the `id` `string→fixed_size_binary(16)` recast. Keep `id` as the 36-char UUID string.** | `id` (GERS) is the **plane-wide cross-dataset join key**; bridges/crosswalks/downstream Lance datasets address entities by text id. Changing the key representation is an interface break across the whole plane — the ~325 MB decoded saving (which compresses well on disk anyway) does not justify fragmenting the join contract. Optimize structure, never the canonical key's type. |
| **D7** | **Recast `confidence` `double→float32`.** | Lossless for a 0..1 score at Overture's precision; halves the column; no interop cost (it is a score, not a key). |
| **D8** | **Demote constants to Lance schema metadata, not a sidecar.** | Metadata travels with the dataset, zero per-row cost, readable via `ds.schema.metadata`. A manual future re-ingest re-stamps provenance once at the dataset level. |
| **D9** | **`region` normalization = `upper(trim)` → USPS whitelist → explicit full-name aliases → else NULL.** No `region_raw` retained. | Deterministic, reviewable. Case variants auto-fix; foreign subdivisions (QC, BC, Dhaka, …) and freely-associated sovereign states (FM/MH/PW, non-US) → NULL = "no valid US region". Retaining dirt has no analytical value. |
| **D10** | **Do NOT drop coordinate-outlier rows.** | Dropping rows is destructive and breaks the row-preservation / distinct-id contract. The outliers no longer poison anything (lon/lat zone-maps are abandoned; each outlier is one Hilbert cell). Coordinate QA is a separate, non-destructive cycle — out of scope here. |
| **D11** | **One consolidated rewrite — no standalone "Tier-1 category index" pass.** | The rewrite rebuilds ALL indices (category included). A separate reindex would waste an R2 churn cycle. |
| **D12** | **Separate worker module `optimize.py` (new Modal app `overture-maps-optimize`); leave the recurring ingest path isolated.** Plus surgical go-forward edits to `places.py`. | Blast-radius separation: a heavy one-shot migration must not entangle the monthly ingest. |
| **D13** | **Same URI, same fragment sizing (1,048,576 rows / 90 GiB), same storage version (2.1).** | Stable downstream addressing; topology is already optimal — re-sort only, not a fragmentation change. |

---

## 3. Target end-state

### Schema `overture_places.v2` — 10 per-row columns (was 13)

| # | Column | Type | Change |
|---|---|---|---|
| 1 | `id` | `string` | unchanged (UUID; **not** recast — D6) |
| 2 | `longitude` | `double` | unchanged |
| 3 | `latitude` | `double` | unchanged |
| 4 | `hilbert` | `uint32` | **NEW** — `ST_Hilbert(lon,lat,[-180,-90,180,90])`, sort key |
| 5 | `region` | `string` | **normalized** (USPS or NULL) |
| 6 | `locality` | `string` | unchanged |
| 7 | `postcode` | `string` | unchanged |
| 8 | `name` | `string` | unchanged |
| 9 | `category` | `string` | unchanged |
| 10 | `confidence` | `float` (float32) | **recast** from double |

**Demoted to `schema.metadata`** (dropped as columns): `country`, `snapshot_date`, `release_tag`, `ingested_at`, plus `schema_version`, `sort_order`, `hilbert_bounds`.

### Index set — 5 BTREE + 2 BITMAP (was 6 BTREE + 1 BITMAP)

- **BTREE:** `id`, `name`, `postcode`, `locality`, **`hilbert`**  *(dropped: `longitude`, `latitude`)*
- **BITMAP:** `region`, **`category`**

### Physical order
`ORDER BY region NULLS LAST, hilbert` → fragments become region-blocked and spatially clustered within region.

### Projected impact (decoded math exact; on-disk = estimate, measure post-run)
| Quantity | Before | After | Δ |
|---|---:|---:|---:|
| Decoded payload | 2,286 MB | **~1,863 MB** | **−18.5%** (−423 constants; −65 confidence + 65 hilbert net 0) |
| Per-row columns | 13 | 10 | −3 |
| Index footprint | 1.34 GiB | **lower** (est.) | drop 2 near-unique-double BTREEs; add 1 uint32 BTREE + 1 small BITMAP |
| bbox query | 38.9 s | **sub-second** (target) | hilbert range + fragment pruning replaces 2-BTREE intersect |
| by-state scan | all 16 frags | **pruned** | region-primary sort → tight region zone-maps |

---

## 4. Verified primitives (do not re-derive — proven against the live stack 2026-06-06)

- `ST_Hilbert(longitude::DOUBLE, latitude::DOUBLE, ST_Extent(ST_MakeEnvelope(-180,-90,180,90)))` → `UINTEGER`. Clusters neighbors (Δ=5 for ~80 m apart; Δ=8.8 M Cheyenne↔LA). Maps the lat −89.9° outlier with no Mercator clamp.
- `ORDER BY region NULLS LAST, hilbert` on output aliases — valid; NULL regions sort last.
- Lance schema metadata round-trips: `pa.Table.replace_schema_metadata({...})` → `lance.write_dataset` → `lance.dataset(...).schema.metadata` recovers all keys; field types preserved (`hilbert` uint32, `confidence` float32, lon/lat double).
- Integer BTREE on `hilbert` + range predicate → physical plan contains `ScalarIndexQuery@hilbert_idx(BTree)`.
- Full `projection_sql` (region CASE + hilbert + casts + sort) executes end-to-end; `ca→CA`, `New York→NY`, `Florida→FL`, `QC→NULL` (sorts last).

---

## 5. Implementation — three artifacts

### 5.1 NEW FILE — `pipelines/overture_maps/_transform.py`

Single source of truth for the v2 transform, imported by both workers.

```python
"""Shared transform constants for the Overture Places v2 (optimized) schema.
Imported by both the one-shot migration (optimize.py) and the go-forward ingest
(places.py) so any future re-ingest is born in the v2 layout. Pure SQL fragments +
the canonical index plan. No I/O, no side effects."""

SCHEMA_VERSION = "overture_places.v2"

# Hilbert space-filling sort/spatial key. GLOBAL LINEAR bounds so every coordinate
# (including the diagnostic's |lat|>85° mislocated outliers) maps validly. ST_QuadKey
# (Web-Mercator) was REJECTED for its ±85.0511° domain limit. 3-arg DOUBLE,DOUBLE,BOX_2D
# form; BOX_2D via ST_Extent(ST_MakeEnvelope(...)). Verified against DuckDB 1.5 spatial.
HILBERT_BOUNDS_SQL = "ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90))"
HILBERT_BOUNDS_TAG = "-180,-90,180,90"
HILBERT_EXPR_SQL = f"ST_Hilbert(longitude::DOUBLE, latitude::DOUBLE, {HILBERT_BOUNDS_SQL})"

# Canonical US subdivision whitelist: 50 states + DC + 5 inhabited US territories.
# Freely-associated SOVEREIGN states (FM, MH, PW) are NON-US → NULL.
USPS_VALID = (
    "'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',"
    "'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',"
    "'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',"
    "'VA','WA','WV','WI','WY','DC','PR','VI','GU','AS','MP'"
)
# Deterministic region normalization. upper/trim auto-fixes case variants (ca→CA);
# the whitelist keeps valid codes; explicit aliases map full names → USPS; everything
# else (foreign subdivisions, garbage) → NULL = "no valid US region".
REGION_NORMALIZE_SQL = f"""CASE
  WHEN UPPER(TRIM(region)) IN ({USPS_VALID}) THEN UPPER(TRIM(region))
  WHEN UPPER(TRIM(region)) = 'CALIFORNIA'           THEN 'CA'
  WHEN UPPER(TRIM(region)) = 'CALIF'                THEN 'CA'
  WHEN UPPER(TRIM(region)) = 'TEXAS'                THEN 'TX'
  WHEN UPPER(TRIM(region)) = 'FLORIDA'              THEN 'FL'
  WHEN UPPER(TRIM(region)) = 'NEW YORK'             THEN 'NY'
  WHEN UPPER(TRIM(region)) = 'OHIO'                 THEN 'OH'
  WHEN UPPER(TRIM(region)) = 'ARIZONA'              THEN 'AZ'
  WHEN UPPER(TRIM(region)) = 'PENNSYLVANIA'         THEN 'PA'
  WHEN UPPER(TRIM(region)) = 'VIRGINIA'             THEN 'VA'
  WHEN UPPER(TRIM(region)) = 'TENNESSEE'            THEN 'TN'
  WHEN UPPER(TRIM(region)) = 'NEVADA'               THEN 'NV'
  WHEN UPPER(TRIM(region)) = 'DELAWARE'             THEN 'DE'
  WHEN UPPER(TRIM(region)) = 'WYOMING'              THEN 'WY'
  WHEN UPPER(TRIM(region)) = 'NORTH DAKOTA'         THEN 'ND'
  WHEN UPPER(TRIM(region)) = 'DISTRICT OF COLUMBIA' THEN 'DC'
  ELSE NULL
END"""

# v2 index plan: drop the per-axis lon/lat BTREEs (proven pathological for 2-D bbox,
# 38.9s) for the single integer hilbert BTREE; add the category BITMAP.
OPTIMIZED_BTREE_INDEXES = ["id", "name", "postcode", "locality", "hilbert"]
OPTIMIZED_BITMAP_INDEXES = ["region", "category"]

# Per-row v2 projection. Constants (country/snapshot_date/release_tag/ingested_at)
# are demoted to schema metadata, NOT projected. `src` is a relation exposing the flat
# source columns (the committed Lance SoR, or the ingest geo CTE). ORDER BY clusters
# fragments by region then space-filling key.
def projection_sql(src: str) -> str:
    return f"""SELECT
    id,
    longitude,
    latitude,
    CAST({HILBERT_EXPR_SQL} AS UINTEGER) AS hilbert,
    {REGION_NORMALIZE_SQL} AS region,
    locality,
    postcode,
    name,
    category,
    CAST(confidence AS FLOAT) AS confidence
FROM {src}
ORDER BY region NULLS LAST, hilbert"""
```

### 5.2 NEW FILE — `pipelines/overture_maps/optimize.py`

The one-shot migration worker. Self-contained R2 helpers (mirroring `places.py`); imports the transform from `_transform`. Build→verify→backup→publish→verify→ledger, with restore-on-failure.

```python
"""One-shot structural optimization of the committed Overture Places Lance SoR.

RE-INGEST-FREE: reads s3://data-sink/active/overture_places/ back from R2 (no
Overture re-pull), applies the v2 transform (Hilbert sort/spatial key + region
normalize + confidence→float32 + constant-column demotion to schema metadata),
rewrites SORTED by (region, hilbert), rebuilds the v2 scalar-index set, then
republishes to the SAME URI — guarded by a pre-wipe R2 backup and a HARD
build-verify gate before any publish.

Mutates the SoR. Idempotent (overwrite), ledgered (ops.overture_places_runs,
write_path='optimize'), reversible (server-side R2 backup + restore-on-failure).

    modal run pipelines/overture_maps/optimize.py::dryrun    # build+verify LOCAL only, NO mutation
    modal run pipelines/overture_maps/optimize.py::apply     # backup → publish → verify → ledger
"""
from __future__ import annotations

import os

import modal

from pipelines.overture_maps._transform import (
    HILBERT_BOUNDS_TAG,
    OPTIMIZED_BITMAP_INDEXES,
    OPTIMIZED_BTREE_INDEXES,
    SCHEMA_VERSION,
    projection_sql,
)

# ── System-of-record (R2) ──────────────────────────────────────────────────
BUCKET = "data-sink"
DATASET_PREFIX = "active/overture_places/"
DATASET_URI = f"s3://{BUCKET}/{DATASET_PREFIX}"
SCRATCH_DIR = "/tmp/overture_opt"
LOCAL_OUT = os.path.join(SCRATCH_DIR, "out_lance")
FEED = "overture_places"

# Source baseline from the 2026-06-06 diagnostic — assert no drift before mutating.
SRC_ROWS_EXPECTED = 16_273_123

# Lance fragment sizing — identical to the ingest (Lance defaults / 90 GiB ceiling).
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
STREAM_BATCH_ROWS = 1_048_576

# Source columns to read back (flat; constants captured separately for metadata).
SOURCE_COLUMNS = [
    "id", "longitude", "latitude", "region",
    "locality", "postcode", "name", "category", "confidence",
]

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",
        "lancedb>=0.15",
        "pylance>=7",
        "pyarrow>=17",
        "boto3>=1.35",
        "psycopg[binary]>=3.2",
    )
    .run_commands(
        "python -c \"import duckdb; duckdb.connect().execute('INSTALL httpfs; INSTALL spatial;')\""
    )
    .env({"LANCE_BYPASS_SPILLING": "true"})
    # Mount the local package so `from pipelines.overture_maps._transform import …`
    # resolves in the container. (Modal automounts imported local modules in current
    # versions; this is the explicit, deterministic form.)
    .add_local_python_source("pipelines")
)

app = modal.App("overture-maps-optimize", image=image)


# ── R2 helpers (self-contained; mirror pipelines/overture_maps/places.py) ────
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
        "aws_endpoint": endpoint,
        "aws_region": "auto",
        "aws_virtual_hosted_style_request": "false",
    }


def _lance_storage_options() -> dict[str, str]:
    # object_store keys for Lance reads/writes against R2 (path-style).
    return _r2_storage_options()


def _s3_client():
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client("s3", endpoint_url=so["aws_endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"],
                        region_name="auto", config=cfg)


def _list_keys(s3, prefix: str) -> list[str]:
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            keys.append(o["Key"])
    return keys


def _backup_r2_prefix(s3, src_prefix: str, bak_prefix: str) -> int:
    """Server-side CopyObject every object src→bak (no egress). Returns count."""
    n = 0
    for key in _list_keys(s3, src_prefix):
        rel = key[len(src_prefix):]
        if not rel:
            continue
        s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key},
                       Key=bak_prefix + rel)
        n += 1
    return n


def _wipe_prefix(s3, prefix: str) -> None:
    batch = []
    for key in _list_keys(s3, prefix):
        batch.append({"Key": key})
        if len(batch) == 1000:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})
            batch = []
    if batch:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})


def _upload_dir(s3, prefix: str, local_dir: str) -> tuple[int, int]:
    files = bytes_ = 0
    for root, _, fnames in os.walk(local_dir):
        for fn in fnames:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            s3.upload_file(lp, BUCKET, prefix + rel)
            files += 1
            bytes_ += os.path.getsize(lp)
    return files, bytes_


def _restore_r2_prefix(s3, bak_prefix: str, dst_prefix: str) -> int:
    """Roll back: wipe dst, copy bak→dst server-side."""
    _wipe_prefix(s3, dst_prefix)
    n = 0
    for key in _list_keys(s3, bak_prefix):
        rel = key[len(bak_prefix):]
        if not rel:
            continue
        s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key},
                       Key=dst_prefix + rel)
        n += 1
    return n


def _record_run(dataset_uri, release_tag, snapshot_date, rows, distinct_ids,
                published_files, published_bytes, write_path, status, error,
                started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.overture_places_runs
                    (feed, dataset_uri, release_tag, snapshot_date, rows_processed,
                     distinct_ids, published_files, published_bytes, write_path,
                     status, error, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (FEED, dataset_uri, release_tag, snapshot_date, rows, distinct_ids,
                 published_files, published_bytes, write_path, status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the migration
        print(f"WARN: ops.* write failed: {exc}")


# ── index build + verification ──────────────────────────────────────────────
def _build_indexes(ds) -> list[str]:
    built = []
    for col in OPTIMIZED_BTREE_INDEXES:
        ds.create_scalar_index(col, "BTREE", replace=True)
        built.append(f"BTREE:{col}")
        print(f"  BTREE  ✓ {col}")
    for col in OPTIMIZED_BITMAP_INDEXES:
        ds.create_scalar_index(col, "BITMAP", replace=True)
        built.append(f"BITMAP:{col}")
        print(f"  BITMAP ✓ {col}")
    return built


def _index_names(ds) -> set[str]:
    out = set()
    for ix in ds.list_indices():
        cols = ix.get("fields") if isinstance(ix, dict) else getattr(ix, "fields", None)
        if cols:
            out.update(cols)
    return out


def _verify_local(local_path: str, expected_rows: int) -> dict:
    """HARD pre-publish gate. Raises on any failure → SoR is never touched."""
    import lance

    ds = lance.dataset(local_path)
    rows = ds.count_rows()
    fields = {f.name: str(f.type) for f in ds.schema}
    meta = {k.decode(): v.decode() for k, v in (ds.schema.metadata or {}).items()}
    idx_cols = _index_names(ds)

    expect_fields = {
        "id": "string", "longitude": "double", "latitude": "double",
        "hilbert": "uint32", "region": "string", "locality": "string",
        "postcode": "string", "name": "string", "category": "string",
        "confidence": "float",
    }
    expect_idx = set(OPTIMIZED_BTREE_INDEXES) | set(OPTIMIZED_BITMAP_INDEXES)
    expect_meta = {"country", "release_tag", "snapshot_date", "ingested_at", "schema_version"}

    problems = []
    if rows != expected_rows:
        problems.append(f"row count {rows} != expected {expected_rows}")
    if fields != expect_fields:
        problems.append(f"schema mismatch: got {fields}")
    if not expect_idx.issubset(idx_cols):
        problems.append(f"missing indices: {expect_idx - idx_cols}")
    if {"longitude", "latitude"} & idx_cols:
        problems.append(f"stale lon/lat BTREE present: {idx_cols}")
    if not expect_meta.issubset(set(meta)):
        problems.append(f"missing metadata keys: {expect_meta - set(meta)}")

    # pushdown smoke test on the new spatial key
    plan = ds.scanner(filter="hilbert >= 0 AND hilbert <= 4294967295",
                      columns=["id"]).explain_plan(True)
    if "ScalarIndexQuery" not in plan:
        problems.append("hilbert range did not use ScalarIndexQuery")

    if problems:
        raise RuntimeError("LOCAL VERIFY FAILED:\n  - " + "\n  - ".join(problems))
    return {"rows": rows, "fields": fields, "metadata": meta, "indexed_cols": sorted(idx_cols)}


def _transform_and_build(con_threads: int = 8) -> dict:
    """Read SoR → transform (sorted) → write local Lance → build v2 indexes →
    LOCAL verify. No R2 mutation. Returns build report."""
    import shutil

    import duckdb
    import lance

    so = _lance_storage_options()
    src = lance.dataset(DATASET_URI, storage_options=so)
    src_rows = src.count_rows()
    print(f"Source rows: {src_rows:,}")
    if src_rows != SRC_ROWS_EXPECTED:
        raise RuntimeError(
            f"Source row drift: {src_rows} != baseline {SRC_ROWS_EXPECTED}. "
            "Re-run the diagnostic and update SRC_ROWS_EXPECTED before optimizing."
        )

    # capture constant provenance (one row — these are cardinality-1 columns)
    prov_tbl = src.scanner(
        columns=["country", "release_tag", "snapshot_date", "ingested_at"], limit=1
    ).to_table()
    prov = {c: prov_tbl.column(c)[0].as_py() for c in prov_tbl.column_names}
    release_tag = str(prov.get("release_tag"))
    snapshot_date = str(prov.get("snapshot_date"))
    metadata = {
        "country": str(prov.get("country")),
        "release_tag": release_tag,
        "snapshot_date": snapshot_date,
        "ingested_at": str(prov.get("ingested_at")),
        "schema_version": SCHEMA_VERSION,
        "sort_order": "region,hilbert",
        "hilbert_bounds": HILBERT_BOUNDS_TAG,
    }
    print(f"Captured provenance → schema metadata: {metadata}")

    os.makedirs(SCRATCH_DIR, exist_ok=True)
    shutil.rmtree(LOCAL_OUT, ignore_errors=True)

    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads={con_threads};")
    con.execute("SET enable_progress_bar=false;")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET memory_limit='24GB';")
    con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
    con.execute("LOAD spatial;")

    reader = src.scanner(columns=SOURCE_COLUMNS).to_reader()
    con.register("src", reader)
    sql = projection_sql("src")

    distinct_ids = None
    write_path = "materialize"
    try:
        table = con.execute(sql).to_arrow_table()
        table = table.replace_schema_metadata(
            {k.encode(): v.encode() for k, v in metadata.items()}
        )
        out_rows = table.num_rows
        con.register("proj", table)
        distinct_ids = con.execute("SELECT count(DISTINCT id) FROM proj").fetchone()[0]
        con.unregister("proj")
        if out_rows != src_rows:
            raise RuntimeError(f"row-preservation violated: {out_rows} != {src_rows}")
        if distinct_ids != out_rows:
            raise RuntimeError(f"id no longer unique: distinct {distinct_ids} != rows {out_rows}")
        print(f"  transformed {out_rows:,} rows; distinct id = {distinct_ids:,}")
        lance.write_dataset(
            table, LOCAL_OUT, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
        )
    except (MemoryError, duckdb.OutOfMemoryException) as exc:
        write_path = "stream"
        print(f"  materialize hit {type(exc).__name__}; streaming fallback: {exc}")
        con.close()
        con = duckdb.connect(":memory:")
        con.execute(f"PRAGMA threads={con_threads};")
        con.execute("SET enable_progress_bar=false;")
        con.execute("SET preserve_insertion_order=false;")
        con.execute("SET memory_limit='24GB';")
        con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
        con.execute("LOAD spatial;")
        reader = src.scanner(columns=SOURCE_COLUMNS).to_reader()
        con.register("src", reader)
        rdr = con.execute(sql).to_arrow_reader(STREAM_BATCH_ROWS)
        schema = rdr.schema.with_metadata(
            {k.encode(): v.encode() for k, v in metadata.items()}
        )
        lance.write_dataset(
            rdr, LOCAL_OUT, schema=schema, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
        )
    finally:
        con.close()

    if write_path == "stream":
        out_rows = lance.dataset(LOCAL_OUT).count_rows()
        if out_rows != src_rows:
            raise RuntimeError(f"row-preservation violated (stream): {out_rows} != {src_rows}")

    ds_out = lance.dataset(LOCAL_OUT)
    built = _build_indexes(ds_out)
    report = _verify_local(LOCAL_OUT, src_rows)
    report.update({"built": built, "write_path": write_path,
                   "release_tag": release_tag, "snapshot_date": snapshot_date,
                   "distinct_ids": distinct_ids, "src_rows": src_rows})
    print(f"LOCAL build+verify OK: {report}")
    return report


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90, memory=32768, cpu=8.0, ephemeral_disk=524288,
)
def optimize_overture_places(apply: bool = False) -> dict:
    """dryrun (apply=False): build+verify LOCAL only, NO mutation.
    apply=True: + R2 backup → publish → post-publish verify (restore-on-fail) → ledger."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    report = _transform_and_build()

    if not apply:
        return {"mode": "dryrun", "mutated": False, **report}

    s3 = _s3_client()
    ts = started_at.strftime("%Y%m%dT%H%M%SZ")
    bak_prefix = f"active/overture_places__bak_{report['release_tag']}_{ts}/"
    status, error = "error", None
    published_files = published_bytes = 0
    try:
        n_bak = _backup_r2_prefix(s3, DATASET_PREFIX, bak_prefix)
        print(f"Backed up {n_bak} objects → s3://{BUCKET}/{bak_prefix}")

        _wipe_prefix(s3, DATASET_PREFIX)
        published_files, published_bytes = _upload_dir(s3, DATASET_PREFIX, LOCAL_OUT)
        print(f"Published {published_files} files ({published_bytes:,} B) → {DATASET_URI}")

        # post-publish verify against R2; restore on any failure
        pub = lance.dataset(DATASET_URI, storage_options=_lance_storage_options())
        pub_rows = pub.count_rows()
        pub_idx = _index_names(pub)
        n_region = pub.scanner(filter="region = 'CA'", columns=["id"]).to_table().num_rows
        ok = (pub_rows == report["src_rows"]
              and set(OPTIMIZED_BTREE_INDEXES + OPTIMIZED_BITMAP_INDEXES).issubset(pub_idx)
              and n_region > 0)
        if not ok:
            raise RuntimeError(
                f"POST-PUBLISH VERIFY FAILED: rows={pub_rows} idx={sorted(pub_idx)} ca_rows={n_region}"
            )
        status = "success"
        print(f"Post-publish verify OK: rows={pub_rows:,} CA={n_region:,} idx={sorted(pub_idx)}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"FAILURE: {error} — attempting rollback from {bak_prefix}")
        try:
            n_res = _restore_r2_prefix(s3, bak_prefix, DATASET_PREFIX)
            print(f"ROLLBACK: restored {n_res} objects from backup; SoR returned to pre-optimize state.")
        except Exception as rexc:  # noqa: BLE001
            print(f"CRITICAL: rollback FAILED: {rexc}. Backup intact at s3://{BUCKET}/{bak_prefix}")
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(DATASET_URI, report["release_tag"], report["snapshot_date"],
                    int(report["src_rows"]), report.get("distinct_ids"),
                    published_files, published_bytes, "optimize", status, error,
                    started_at, completed_at)

    return {"mode": "apply", "mutated": True, "backup_prefix": bak_prefix,
            "published_files": published_files, "published_bytes": published_bytes, **report}


@app.local_entrypoint()
def dryrun() -> None:
    import json
    print(json.dumps(optimize_overture_places.remote(apply=False), indent=2, default=str))


@app.local_entrypoint()
def apply() -> None:
    import json
    print(json.dumps(optimize_overture_places.remote(apply=True), indent=2, default=str))
```

> **Status:** both files above are syntax-checked (`py_compile`) and `projection_sql` is validated end-to-end. `add_local_python_source` is the current Modal API for bundling local packages; if the deployed Modal version differs, either rely on Modal automount (default) or inline `_transform`'s constants into `optimize.py`.

### 5.3 EDIT — `pipelines/overture_maps/places.py` (go-forward parity)

So a future re-ingest is born in v2. Surgical changes only:

**(a) Import the shared transform** (top of file):
```python
from pipelines.overture_maps import _transform as T
```

**(b) Replace the index-plan constants** `OVERTURE_BTREE_INDEXES` / `OVERTURE_BITMAP_INDEXES`:
```python
OVERTURE_BTREE_INDEXES = T.OPTIMIZED_BTREE_INDEXES      # id, name, postcode, locality, hilbert
OVERTURE_BITMAP_INDEXES = T.OPTIMIZED_BITMAP_INDEXES    # region, category
```

**(c) Rewrite `_build_sql(geom_expr)`** to emit the flat source columns in a CTE, then apply the shared projection (this adds `hilbert`, normalizes `region`, casts `confidence`, sorts, and DROPS the four constants from the rows):
```python
def _build_sql(geom_expr: str) -> str:
    return f"""
WITH raw AS (
    SELECT * FROM read_parquet(?)
),
geo AS (
    SELECT id, {geom_expr} AS geom, addresses, names, categories, confidence
    FROM raw
    WHERE addresses[1].country = 'US'
),
flat AS (
    SELECT
        nullif(trim(id), '')                     AS id,
        ST_X(geom)                               AS longitude,
        ST_Y(geom)                               AS latitude,
        nullif(trim(addresses[1].region), '')    AS region,
        nullif(trim(addresses[1].locality), '')  AS locality,
        nullif(trim(addresses[1].postcode), '')  AS postcode,
        nullif(trim(names.primary), '')          AS name,
        nullif(trim(categories.primary), '')     AS category,
        TRY_CAST(confidence AS DOUBLE)           AS confidence
    FROM geo
)
{T.projection_sql("flat")}
"""
```
The two positional date params (`snapshot_date`, `release_tag`) are **removed** from `_build_sql`'s parameter list — they no longer appear in the projection. Update the `params` passed to `con.execute(sql, params)` to `params = [read_glob]` only.

**(d) Attach provenance to schema metadata** before each `lance.write_dataset` (materialize path):
```python
table = table.replace_schema_metadata({
    b"country": b"US",
    b"release_tag": release_tag.encode(),
    b"snapshot_date": snapshot_date.encode(),
    b"ingested_at": started_at.isoformat().encode(),
    b"schema_version": T.SCHEMA_VERSION.encode(),
    b"sort_order": b"region,hilbert",
    b"hilbert_bounds": T.HILBERT_BOUNDS_TAG.encode(),
})
```
And on the streaming path, `schema = reader.schema.with_metadata({...same...})` before `lance.write_dataset(reader, schema=schema, ...)`.

**(e)** Ensure `con.execute("LOAD spatial;")` precedes the transform (already present in the ingest).

> The ingest already sorts implicitly via the projection's `ORDER BY`; confirm the `to_arrow_reader` fallback still applies the sort (DuckDB completes the blocking sort before emitting — verified behavior).

---

## 6. Execution runbook

**Phase 0 — pre-flight**
1. `cd` to the `core-x` checkout; confirm `doppler` is bound (`core-x/prd`) and the Modal secrets `r2-credentials` + `hqx-postgres` exist.
2. Create `_transform.py` and `optimize.py`; apply the `places.py` edits.
3. Lint/compile: `python -m py_compile pipelines/overture_maps/_transform.py pipelines/overture_maps/optimize.py`.
4. Confirm `ops.overture_places_runs` exists (it does; `places.py::initdb` is idempotent if not).

**Phase 1 — dry run (NO mutation; mandatory gate)**
```
modal run pipelines/overture_maps/optimize.py::dryrun
```
Reads the SoR, builds the v2 dataset + indices on the worker's local disk, runs `_verify_local`, and returns the build report **without touching R2**. Inspect: `rows == 16273123`, schema == the 10-field v2 set, `indexed_cols` == the 7 v2 indices (no lon/lat), metadata keys present, `write_path`. Abort the cycle if anything is off.

**Phase 2 — apply (mutating; backup + publish + verify + ledger)**
```
modal run pipelines/overture_maps/optimize.py::apply
```
Backs up the current prefix → `active/overture_places__bak_<release>_<ts>/`, wipes + publishes the v2 dataset, runs the post-publish verify against R2 (auto-restores from backup on failure), and writes the ledger row. On success the SoR is v2 at the same URI.

**Phase 3 — independent confirmation**
- Re-run the structural diagnostic's probes (or a subset) against the live SoR: confirm fragment zone-maps are now region/spatially tight, the `hilbert` range predicate hits `ScalarIndexQuery`, and a bbox-via-hilbert-range returns in well under a second.
- `modal run pipelines/overture_maps/places.py::show_ledger` → confirm the `optimize` row, status `success`.

**Phase 4 — ship the code**
- Commit `_transform.py`, `optimize.py`, the `places.py` edits, and this directive on a branch; PR → squash-merge → pull into the operator checkout (standard lifecycle).

**Phase 5 — backup retention**
- Leave `active/overture_places__bak_*` in place until Phase 3 passes and the operator confirms. Then delete the backup prefix to reclaim ~2.5 GiB (one `_wipe_prefix(s3, bak_prefix)` call, or lifecycle rule). **Do not auto-delete inside the worker.**

---

## 7. Acceptance criteria (hard gates)

The cycle is **done** only when ALL hold:
- [ ] `dryrun` `_verify_local` passed (rows, schema, indices, metadata, hilbert pushdown).
- [ ] `apply` returned `status=success`; ledger row present with `write_path='optimize'`.
- [ ] Live SoR: `count_rows() == 16,273,123`; `DISTINCT(id)` == row count (unchanged).
- [ ] Live SoR schema == 10 v2 fields with correct types; `schema.metadata` carries the 4 demoted constants + `schema_version=overture_places.v2`.
- [ ] Live SoR indices == {id, name, postcode, locality, hilbert} BTREE + {region, category} BITMAP; **no** longitude/latitude index.
- [ ] `region = 'CA'` plan uses `ScalarIndexQuery@region_idx(Bitmap)`; `hilbert BETWEEN …` uses `ScalarIndexQuery@hilbert_idx(BTree)`; `category = …` uses `ScalarIndexQuery@category_idx(Bitmap)`.
- [ ] A representative bbox (translated to a hilbert range + residual lon/lat refine — see §9) returns the correct rows in **< 1 s**.
- [ ] `places.py` edits compile; a future ingest would emit v2 (review-confirmed).

---

## 8. Rollback

Automatic: `apply` restores from the backup prefix on any post-publish verification failure (wipe + server-side copy back), then re-raises. The SoR returns to the exact pre-optimize bytes.

Manual (if needed later): `_restore_r2_prefix(s3, "active/overture_places__bak_<release>_<ts>/", "active/overture_places/")`. The backup is a byte-identical server-side copy of the pre-migration dataset (data + v1 indices + manifests), so restore yields the original v1 SoR.

---

## 9. Consumer contract (publish to downstream after migration)

From the diagnostic §5 (measured) — how to query the v2 SoR so the indices are actually used:

1. **Never filter in DuckDB over an unfiltered Lance reader** (28× penalty, index bypassed). Pass the `LanceDataset` object to DuckDB (replacement scan pushes the predicate) **or** build `ds.scanner(filter=…, columns=[…])`.
2. **Equality / range on indexed columns** (`id`, `name`, `postcode`, `locality`, `region`, `category`, `hilbert`) push down to `ScalarIndexQuery`. Keep predicates as simple comparisons / `IN` / conjunctions — functions and complex expressions fall back to a full scan.
3. **bbox queries** — translate to a `hilbert` range + residual refine (the Hilbert range is a superset; the refine guarantees exactness, the range prunes fragments):
   ```sql
   -- compute h_lo/h_hi = min/max ST_Hilbert over the bbox corners (+ edge samples),
   -- bounds = ST_Extent(ST_MakeEnvelope(-180,-90,180,90))  -- MUST match the build bounds
   SELECT * FROM places
   WHERE hilbert BETWEEN :h_lo AND :h_hi          -- prunes fragments via the BTREE
     AND longitude BETWEEN :min_lon AND :max_lon  -- residual exactness
     AND latitude  BETWEEN :min_lat AND :max_lat;
   ```
4. **by-state** — `region = 'XX'` (or `IN (...)`); fragments are region-clustered so zone-maps prune in addition to the BITMAP.
5. **DuckDB out-of-core config** for heavy scans/joins on the SoR: `memory_limit ≈ 70–75% RAM`, dedicated NVMe `temp_directory`, `preserve_insertion_order=false`.

---

## 10. Out of scope / explicitly rejected

- **`id` → `fixed_size_binary(16)`** — REJECTED (D6): plane-wide join-key contract outweighs the saving.
- **Dropping coordinate-outlier rows** — DEFERRED (D10): destructive; separate non-destructive QA cycle. The outliers are inert under v2.
- **Append/multi-vintage history** — CLOSED (operator: no continual re-ingest). Overwrite model retained; provenance lives in metadata.
- **Vector indexing** — N/A: no embedding column exists.
- **Compaction** — N/A: topology already optimal (0 tombstones, 16 capped fragments); the v2 rewrite is a clustering re-sort, not a fragmentation remedy.
