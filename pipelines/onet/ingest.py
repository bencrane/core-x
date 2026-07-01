"""O*NET database ingest — landing xlsx workbook → Lance system of record (Gen-3).

LOCAL in-session compute worker. The operator drops the full O*NET Excel database
(``db_XX_X_excel.zip`` from onetcenter.org/database.html) into
``s3://data-sink/landing/o-net/``. Raw is transport-only. Every data-bearing table in the
workbook (45 in the 30.3 release, ~1.1M rows) materializes to one Lance dataset each,
``onet_<snake_table>`` — the SOC semantic layer (occupation descriptions, alternate/reported
job titles, tasks, skills, knowledge, abilities, work activities/context, technology) that
grounds the SCA↔SOC↔PSC labor-category bridge.

    doppler run -p core-x -c prd -- python3 -m pipelines.onet.ingest --init-state   # ops DDL (once)
    doppler run -p core-x -c prd -- python3 -m pipelines.onet.ingest --all          # every table
    doppler run -p core-x -c prd -- python3 -m pipelines.onet.ingest --table "Occupation Data"

DISCOVERY, NOT REGISTRY: the workbook is auto-discovered from the zip (robust to release
version + table adds/renames). Every ``*.xlsx`` member becomes a dataset — nothing is
hand-picked, nothing is dropped (no data left behind). Re-runnable on each O*NET release
(``mode="overwrite"`` — full-snapshot replace).

FIDELITY
    Every source column is projected VARCHAR with trim→empty-as-NULL and a snake_cased header;
    zero rows dropped (row count asserted against the source sheet). Numeric typing of the
    rating columns (data_value / n / standard_error / ci bounds) is a deferred downstream
    concern off the SoR — coercing risks silently nulling O*NET's suppression/not-relevant
    markers. The 8-digit O*NET-SOC key (``o_net_soc_code``, e.g. 11-1011.00) gets a derived
    6-digit ``soc_code`` (11-1011) companion so occupation tables join straight to
    bls_oews_2025.occ_code / the SCA bridge. Provenance (onet_version, source_file,
    ingested_at) appended. BTREE on resolution keys (o_net_soc_code, soc_code, element_id,
    task_id, iwa_id, dwa_id, title_id, commodity_code, job_zone); BITMAP on low-card
    categoricals (scale_id, domain_source, recommend_suppress, not_relevant, task_type).

CONTROL PLANE
    Terminal state per table → ops.onet_runs (psycopg, best-effort — an audit write failure
    never masks an otherwise-good load).
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import os
import os.path
import re
import sys
import tempfile
import zipfile

from pipelines.bls.ingest import (
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _s3_client,
    _storage_options,
)

BUCKET = "data-sink"
LANDING_PREFIX = "landing/o-net/"
ZIP_MATCH = r"^db_.*_excel\.zip$"
SCRATCH_DIR = tempfile.gettempdir()

# Release stamp is parsed from the zip name (db_30_3_excel.zip -> 30.3); overridable.
ONET_VERSION_DEFAULT = os.environ.get("ONET_VERSION", "")

# Resolution keys → BTREE (equality + range). Detected per-table by presence.
BTREE_KEYS = ("o_net_soc_code", "soc_code", "element_id", "task_id", "iwa_id", "dwa_id",
              "title_id", "commodity_code", "job_zone")
# Low-cardinality categoricals → BITMAP.
BITMAP_KEYS = ("scale_id", "domain_source", "recommend_suppress", "not_relevant", "task_type")

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.onet_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset        text        NOT NULL,
    dataset_uri    text,
    source_table   text,
    onet_version   text,
    rows_processed bigint,
    source_rows    bigint,
    indexes_built  text[],
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS onet_runs_dataset_idx     ON ops.onet_runs (dataset);
CREATE INDEX IF NOT EXISTS onet_runs_status_idx      ON ops.onet_runs (status);
CREATE INDEX IF NOT EXISTS onet_runs_recorded_at_idx ON ops.onet_runs (recorded_at DESC);
"""


# ── column-name normalization (snake_case + dedup) ─────────────────────────────────
def _snake(name: str) -> str:
    s = re.sub(r"[^0-9a-z]+", "_", str(name).strip().lower())
    return re.sub(r"_+", "_", s).strip("_") or "col"


def _norm_pairs(cols: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: dict[str, int] = {}
    for c in cols:
        n = _snake(c)
        if n in seen:
            seen[n] += 1
            n = f"{n}_{seen[n]}"
        else:
            seen[n] = 0
        out.append((c, n))
    return out


def _dataset_name(xlsx_basename: str) -> str:
    return "onet_" + _snake(xlsx_basename.rsplit(".", 1)[0])


# ── landing zip ────────────────────────────────────────────────────────────────────
def _discover_zip(s3) -> str:
    rx = re.compile(ZIP_MATCH, re.I)
    found: list[tuple[str, int, dt.datetime]] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=LANDING_PREFIX):
        for o in page.get("Contents", []):
            base = o["Key"].rsplit("/", 1)[-1]
            if rx.match(base):
                found.append((o["Key"], o["Size"], o["LastModified"]))
    if not found:
        raise RuntimeError(f"No landing object matches /{ZIP_MATCH}/ under s3://{BUCKET}/{LANDING_PREFIX}")
    found.sort(key=lambda t: (t[2], t[1]), reverse=True)
    return found[0][0]


def _version_from_zip(key: str) -> str:
    if ONET_VERSION_DEFAULT:
        return ONET_VERSION_DEFAULT
    m = re.search(r"db_(\d+)_(\d+)", key.rsplit("/", 1)[-1])
    return f"{m.group(1)}.{m.group(2)}" if m else "unknown"


def _prepare_workbook(s3) -> tuple[str, str, str]:
    """Download + extract the workbook zip; return (extract_dir, source_file, onet_version)."""
    key = _discover_zip(s3)
    source_file = key.rsplit("/", 1)[-1]
    version = _version_from_zip(key)
    zip_path = os.path.join(SCRATCH_DIR, f"onet_{source_file}")
    extract_dir = os.path.join(SCRATCH_DIR, "onet_workbook")
    print(f"landing s3://{BUCKET}/{key} (O*NET {version}) -> {zip_path}")
    s3.download_file(BUCKET, key, zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)
    return extract_dir, source_file, version


def _list_tables(extract_dir: str) -> list[str]:
    return sorted(glob.glob(os.path.join(extract_dir, "**", "*.xlsx"), recursive=True))


# ── ops.onet_runs ledger ───────────────────────────────────────────────────────────
def _record_run(dataset, uri, source_table, version, rows, source_rows, built, status,
                error, started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.onet_runs write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.onet_runs
                    (dataset, dataset_uri, source_table, onet_version, rows_processed,
                     source_rows, indexes_built, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (dataset, uri, source_table, version, rows, source_rows, built, status,
                 error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.onet_runs write failed: {exc}")


def apply_state_schema() -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set (Doppler core-x/prd).")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.onet_runs schema.")


# ── one table: xlsx → DuckDB project → Lance overwrite → indexes ──────────────────
def ingest_table(con, xlsx_path: str, source_file: str, version: str, so: dict) -> dict:
    import lance

    source_table = os.path.basename(xlsx_path).rsplit(".", 1)[0]
    dataset = _dataset_name(os.path.basename(xlsx_path))
    uri = os.environ.get(f"ONET_{dataset.upper()}_LANCE_URI", f"s3://{BUCKET}/active/{dataset}/")
    started = dt.datetime.now(dt.timezone.utc)
    rows = source_rows = 0
    built: list[str] = []
    status, error = "error", None
    try:
        read = f"read_xlsx('{xlsx_path}', all_varchar=true)"
        cols = [d[0] for d in con.execute(f"SELECT * FROM {read} LIMIT 0").description]
        pairs = _norm_pairs(cols)
        names = [n for _, n in pairs]
        source_rows = int(con.execute(f"SELECT count(*) FROM {read}").fetchone()[0])

        projection = ",\n    ".join(f"nullif(trim(\"{o}\"), '') AS \"{n}\"" for o, n in pairs)
        # Every 8-digit O*NET-SOC column -> derived 6-digit SOC companion (strip the .NN detail
        # suffix) for the OEWS/SCA join. Handles o_net_soc_code -> soc_code AND the secondary
        # related_o_net_soc_code -> related_soc_code (so onet_related_occupations bridges both sides).
        soc_targets: list[str] = []
        soc_exprs: list[str] = []
        for n in names:
            if n.endswith("o_net_soc_code"):
                tgt = n[: -len("o_net_soc_code")] + "soc_code"
                if tgt not in names and tgt not in soc_targets:
                    soc_targets.append(tgt)
                    soc_exprs.append(
                        f"regexp_replace(nullif(trim(\"{n}\"), ''), '\\.\\d+$', '') AS {tgt}")
        soc = (",\n    " + ",\n    ".join(soc_exprs)) if soc_exprs else ""
        sql = (
            "SELECT\n    " + projection + soc + ",\n"
            "    ? AS onet_version,\n"
            "    ? AS source_file,\n"
            "    now() AS ingested_at\n"
            f"FROM {read}"
        )
        table = con.execute(sql, [version, source_file]).to_arrow_table()
        rows = table.num_rows
        if rows != source_rows:
            raise RuntimeError(f"row drift {source_table}: projected {rows} != source {source_rows}")

        lance.write_dataset(table, uri, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                            storage_options=so)
        del table
        # BTREE every resolution key: the named keys + any *_id column (catches the
        # <domain>_element_id / gwa_element_id / iwa_element_id / dwa_element_id keys on the
        # many-to-many crosswalk tables), EXCEPT the low-card categoricals that go BITMAP.
        allcols = names + soc_targets
        btree = [c for c in allcols
                 if (c in BTREE_KEYS or c.endswith("_id") or c.endswith("soc_code"))
                 and c not in BITMAP_KEYS]
        bitmap = [c for c in allcols if c in BITMAP_KEYS]
        built = _build_indexes(uri, btree, bitmap, so)
        status = "success"
        print(f"[{dataset:44s}] rows={rows:>7,} idx={len(built):>2} -> {uri}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[{dataset}] FAILED: {exc}")
        raise
    finally:
        _record_run(dataset, uri, source_table, version, int(rows), int(source_rows),
                    built, status, error, started, dt.datetime.now(dt.timezone.utc))
    return {"dataset": dataset, "source_table": source_table, "rows": int(rows),
            "indexes": built, "uri": uri}


def run(only: str | None = None, keep_temp: bool = False) -> list[dict]:
    import duckdb

    so = _storage_options()
    s3 = _s3_client()
    extract_dir, source_file, version = _prepare_workbook(s3)
    tables = _list_tables(extract_dir)
    if only:
        tables = [t for t in tables if _snake(os.path.basename(t).rsplit(".", 1)[0]) == _snake(only)
                  or os.path.basename(t).rsplit(".", 1)[0].lower() == only.lower()]
        if not tables:
            raise ValueError(f"no table matches {only!r}")
    print(f"O*NET {version}: {len(tables)} table(s) to ingest\n")

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")
    con.execute(f"SET temp_directory='{os.path.join(SCRATCH_DIR, 'onet_duckdb_spill')}';")
    con.execute("INSTALL excel; LOAD excel;")
    try:
        results = [ingest_table(con, t, source_file, version, so) for t in tables]
    finally:
        con.close()
        if not keep_temp:
            for f in glob.glob(os.path.join(extract_dir, "**", "*"), recursive=True):
                if os.path.isfile(f):
                    try:
                        os.remove(f)
                    except OSError:
                        pass

    total = sum(r["rows"] for r in results)
    print(f"\n=== O*NET ingest summary === {len(results)} datasets, {total:,} rows")
    return results


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="O*NET Excel database landing → Lance ingest.")
    ap.add_argument("--all", action="store_true", help="ingest every table in the workbook")
    ap.add_argument("--table", help="single table by name (e.g. \"Occupation Data\")")
    ap.add_argument("--init-state", action="store_true", help="apply ops.onet_runs DDL and exit")
    ap.add_argument("--keep-temp", action="store_true", help="keep extracted xlsx in scratch")
    args = ap.parse_args(argv)

    if args.init_state:
        apply_state_schema()
        return 0
    if not args.all and not args.table:
        ap.error("specify --all or --table <name> (or --init-state)")

    run(only=args.table, keep_temp=args.keep_temp)
    return 0


if __name__ == "__main__":
    sys.exit(main())
