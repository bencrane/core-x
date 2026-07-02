"""Compute worker — COMPANY DEDUP CROSSWALK (`company_id → canonical_company_id`).

A non-destructive bridge that collapses TRUE duplicate company rows (one real company
logged under two `company_id`s) WITHOUT fusing distinct subsidiaries. `companies_canonical`
is never touched; downstream joins this map for a deduped view, and the source-platform
sidecar re-groups by the same join with zero re-ingest. This is §3.1 of the GTM identity
refactors — the complement to `entity_hierarchy` (§3.2): dedup collapses one company logged
twice; hierarchy relates distinct subsidiaries under one parent.

THE RULE — UEI-first, two-tier (grain analysis 2026-07-02, 117,037 companies):
  1. Rows WITH a `uei` (62%, 72,154) → `uei` IS the authoritative federal entity key.
     Merge ONLY on exact `uei`. Distinct UEIs never merge — this is a HARD guarantee, not a
     name heuristic, so the ANC/tribal holding-company subsidiary problem (chenega.com et al.
     sharing one domain/LinkedIn across dozens of DISTINCT awardable UEIs) cannot fuse. A
     naive domain-only merge would have fused 2,522 distinct UEIs; this fuses 0.
  2. Rows WITHOUT a `uei` (38%, 44,883; non-federal staffing/sfnet/dex) → name-gated
     `coalesce(company_linkedin_url, normalized_domain) + legal_name_base(name_norm(name))`.
     Two rows merge only if they share BOTH the contact key AND the canonical name base, so
     a shared placeholder domain alone never merges (no holding-company blocklist needed).
  3. Neither key present → singleton (its own `company_id`).

`canonical_company_id` = `min(company_id)` within each group (single namespace — always a
valid `company_id`, joins straight back to `companies_canonical`). `company_id` stays the 1:1
legacy PK.

Fail-closed gates: (a) grain — 1 row per source `company_id`, no nulls; (b) FUSION invariant
— zero canonical groups span >1 distinct `uei` (the entity_hierarchy-fusion safety, satisfied
by construction and asserted). Deterministic (min company_id + canonical name macros).

Data plane (clean-room): Lance(companies_canonical) → DuckDB → Arrow →
  lance.write_dataset(R2 active, v2.1, OVERWRITE snapshot) → BTREE[company_id,
  canonical_company_id] + BITMAP[method]. Prior version retained by Lance as the anchor.
Ops: one terminal row → ops.company_dedup_map_runs.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' \
      python3 pipelines/resolution/company_dedup_map.py <init_ops|build|reindex|verify>
"""
from __future__ import annotations

import datetime as dt
import os
import sys

# Repo root on sys.path so the canonical blocking-key macros resolve on a local script run
# (the script dir, not the repo root, is sys.path[0] under `python3 pipelines/...`).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from core.name_norm import legal_name_base, name_norm  # noqa: E402

FEED = "company_dedup_map"
DATASET_URI = os.environ.get(
    "COMPANY_DEDUP_MAP_URI", "s3://data-sink/active/company_dedup_map/"
).rstrip("/") + "/"
COMPANIES_URI = os.environ.get(
    "GTM_COMPANIES_URI", "s3://data-sink/active/companies_canonical/"
)

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576
BTREE_INDEXES = ["company_id", "canonical_company_id"]
BITMAP_INDEXES = ["method"]

DUCKDB_MEMORY_LIMIT = os.environ.get("CDM_MEMORY_LIMIT", "8GB")
DUCKDB_THREADS = int(os.environ.get("CDM_THREADS", "4"))
SCRATCH = os.environ.get("CDM_SCRATCH", "/tmp/company_dedup_map")


def log(m: str) -> None:
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the r2-credentials secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _new_con():
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET temp_directory='{SCRATCH}/spill'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _materialize(con, so):
    """Read companies_canonical, assign the two-tier grouping key, resolve
    canonical_company_id = min(company_id) per group, and return (arrow, metrics)."""
    import lance

    # legal_name_base(name_norm(company_name)) — the canonical cross-spine name base key.
    basename_expr = legal_name_base(name_norm("company_name"))

    ds = lance.dataset(COMPANIES_URI, storage_options=so)
    src_rows = ds.count_rows()
    con.register("c_rdr", ds.scanner(columns=[
        "company_id", "company_name", "normalized_domain", "company_linkedin_url", "uei"]).to_reader())
    con.execute(f"""CREATE TABLE co AS
      SELECT nullif(trim(company_id),'')                    AS company_id,
             nullif(trim(uei),'')                           AS uei,
             lower(nullif(trim(normalized_domain),''))      AS dom,
             lower(nullif(trim(company_linkedin_url),''))   AS li,
             {basename_expr}                                AS basename
      FROM c_rdr;""")
    con.unregister("c_rdr")

    null_cid = con.execute("SELECT count(*) FROM co WHERE company_id IS NULL").fetchone()[0]
    if null_cid:
        raise RuntimeError(f"{null_cid} rows have a null/empty company_id (PK invariant broken)")

    # Two-tier grouping key (UEI-first). singleton key uses company_id so it is unique.
    con.execute("""CREATE TABLE keyed AS
      SELECT company_id, uei,
        CASE WHEN uei IS NOT NULL THEN 'uei'
             WHEN coalesce(li,dom) IS NOT NULL AND basename IS NOT NULL THEN 'linkedin_domain_name'
             ELSE 'singleton' END AS method,
        CASE WHEN uei IS NOT NULL THEN 'uei:'||uei
             WHEN coalesce(li,dom) IS NOT NULL AND basename IS NOT NULL
                  THEN 'lidn:'||coalesce(li,dom)||'|'||basename
             ELSE 'cid:'||company_id END AS blocking_key
      FROM co;""")

    snapshot = dt.datetime.now(dt.timezone.utc).date().isoformat()
    con.execute(f"""CREATE TABLE map AS
      SELECT company_id,
             uei,
             min(company_id) OVER (PARTITION BY blocking_key) AS canonical_company_id,
             (company_id = min(company_id) OVER (PARTITION BY blocking_key)) AS is_canonical,
             method,
             blocking_key,
             CAST(count(*) OVER (PARTITION BY blocking_key) AS INTEGER) AS group_size,
             DATE '{snapshot}' AS snapshot_date
      FROM keyed;""")

    # ── gates ──
    rows, distinct_cid = con.execute("SELECT count(*), count(DISTINCT company_id) FROM map").fetchone()
    if rows != distinct_cid:
        raise RuntimeError(f"grain: {rows} rows vs {distinct_cid} distinct company_id")
    if rows != src_rows:
        raise RuntimeError(f"grain: {rows} map rows vs {src_rows} companies_canonical rows")
    fusion = con.execute("""SELECT count(*) FROM (
        SELECT canonical_company_id FROM map WHERE uei IS NOT NULL
        GROUP BY canonical_company_id HAVING count(DISTINCT uei) > 1)""").fetchone()[0]
    if fusion:
        raise RuntimeError(f"FUSION gate: {fusion} canonical groups span >1 distinct uei")

    groups = con.execute("SELECT count(DISTINCT canonical_company_id) FROM map").fetchone()[0]
    m_uei = con.execute(
        "SELECT count(*) FROM map WHERE method='uei' AND NOT is_canonical").fetchone()[0]
    m_name = con.execute(
        "SELECT count(*) FROM map WHERE method='linkedin_domain_name' AND NOT is_canonical").fetchone()[0]
    max_grp = con.execute("SELECT max(group_size) FROM map").fetchone()[0]
    by_method = dict(con.execute("SELECT method, count(*) FROM map GROUP BY 1 ORDER BY 2 DESC").fetchall())
    metrics = {"rows": rows, "canonical_groups": groups, "merges": rows - groups,
               "merged_by_uei": m_uei, "merged_by_name": m_name, "max_group_size": max_grp,
               "by_method": by_method, "snapshot_date": snapshot}
    log(f"assembled: {metrics}")

    table = con.execute("""SELECT company_id, canonical_company_id, is_canonical, method,
        blocking_key, group_size, snapshot_date FROM map""").arrow()
    import pyarrow as pa
    if isinstance(table, pa.RecordBatchReader):
        table = table.read_all()
    return table, metrics


def _build_indexes(ds) -> None:
    present = set(ds.schema.names)
    for col in BTREE_INDEXES:
        if col in present:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            log(f"  BTREE ✓ {col}")
    for col in BITMAP_INDEXES:
        if col in present:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            log(f"  BITMAP ✓ {col}")


def build():
    import lance

    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    os.makedirs(f"{SCRATCH}/spill", exist_ok=True)
    status, error, metrics = "error", None, {}
    con = _new_con()
    try:
        table, metrics = _materialize(con, so)
        lance.write_dataset(
            table, DATASET_URI, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        log(f"wrote Lance (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")
        _build_indexes(lance.dataset(DATASET_URI, storage_options=so))
        committed = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        log(f"committed rows: {committed:,}")
        status = "success"
    except Exception as e:  # noqa: BLE001 — terminal handling below + re-raise
        error = f"{type(e).__name__}: {e}"
        raise
    finally:
        con.close()
        _record_run(metrics=metrics, status=status, error=error,
                    started=started, completed=dt.datetime.now(dt.timezone.utc))


def reindex():
    import lance

    so = _r2_so()
    _build_indexes(lance.dataset(DATASET_URI, storage_options=so))
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", None) for i in ds.list_indices()]
    log(f"reindex complete: rows={ds.count_rows():,} indices={idx}")


def _record_run(*, metrics, status, error, started, completed) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED") or os.environ.get("HQX_DB_URL")
    if not dsn:
        log("WARN: no HQX dsn; skipping ops.company_dedup_map_runs row")
        return
    if status != "success" and not error:
        error = "unknown terminal failure"
    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute(
                """INSERT INTO ops.company_dedup_map_runs
                     (feed, dataset_uri, rows_written, canonical_groups, merges,
                      merged_by_uei, merged_by_name, max_group_size, snapshot_date,
                      status, error, started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (FEED, DATASET_URI, metrics.get("rows"), metrics.get("canonical_groups"),
                 metrics.get("merges"), metrics.get("merged_by_uei"), metrics.get("merged_by_name"),
                 metrics.get("max_group_size"), metrics.get("snapshot_date"), status,
                 (error or None), started, completed))
            c.commit()
        log(f"ops row: status={status}")
    except Exception as e:  # noqa: BLE001 — audit must not mask the build
        log(f"WARN: ops.company_dedup_map_runs write failed: {e}")


def init_ops():
    import psycopg
    from pathlib import Path

    sql = Path(__file__).parent.joinpath("ops_company_dedup_map_runs.sql").read_text()
    dsn = os.environ.get("HQX_DB_URL_POOLED") or os.environ["HQX_DB_URL"]
    with psycopg.connect(dsn) as c, c.cursor() as cur:
        cur.execute(sql)
        c.commit()
    log("ops.company_dedup_map_runs DDL applied")


def verify():
    import json

    import duckdb
    import lance

    so = _r2_so()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    try:
        idx = [(i.get("name") if isinstance(i, dict) else getattr(i, "name", None),
                i.get("fields") if isinstance(i, dict) else getattr(i, "fields", None))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    con = duckdb.connect()
    con.register("m", ds.scanner(columns=[
        "company_id", "canonical_company_id", "is_canonical", "method", "group_size"]).to_reader())
    con.execute("CREATE TABLE m AS SELECT * FROM m;")
    con.unregister("m")
    rows, cid, groups, merged = con.execute("""SELECT count(*), count(DISTINCT company_id),
        count(DISTINCT canonical_company_id),
        count(*) FILTER (WHERE NOT is_canonical) FROM m;""").fetchone()
    by_method = dict(con.execute("SELECT method, count(*) FROM m GROUP BY 1 ORDER BY 2 DESC").fetchall())
    top = con.execute("""SELECT canonical_company_id, any_value(method) mth, max(group_size) gs
        FROM m WHERE group_size > 1 GROUP BY 1 ORDER BY gs DESC LIMIT 8;""").fetchall()
    con.close()
    print(json.dumps({
        "uri": DATASET_URI, "rows": ds.count_rows(), "columns": len(ds.schema.names),
        "schema": [f"{f.name}:{f.type}" for f in ds.schema], "indices": idx,
        "distinct_company_id": cid, "grain_ok": rows == cid,
        "canonical_groups": groups, "merges": merged, "by_method": by_method,
        "top_merge_groups": [{"canonical": c, "method": mth, "size": gs} for c, mth, gs in top],
    }, indent=2, default=str))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        build()
    elif cmd == "verify":
        verify()
    elif cmd == "reindex":
        reindex()
    elif cmd == "init_ops":
        init_ops()
    else:
        print(f"unknown command: {cmd} (init_ops|build|reindex|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
