"""Materialize a canonical measure taxonomy for the CBO cell table → Lance crosswalk."""
from __future__ import annotations
import argparse, datetime as dt, json, os, uuid
os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")
os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
from pipelines.bls.ingest import (DATA_STORAGE_VERSION, MAX_BYTES_PER_FILE, MAX_ROWS_PER_FILE,
                                   _build_indexes, _storage_options)
BUCKET = "data-sink"
URI = f"s3://{BUCKET}/active/cbo_measure_taxonomy/"
ARTIFACT = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "reference", "data", "cbo_measure_classifications.json")
LABELS = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "reference", "data", "cbo_labels.json")

def _record_run(run_id, rows, families):
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn: return
    try:
        import psycopg
        with psycopg.connect(dsn, autocommit=True) as c, c.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS ops.federal_appropriations_ingest_runs (
                id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, run_id uuid NOT NULL, stream text NOT NULL,
                resolved_url text, source_bytes bigint, rows_written bigint, datasets jsonb,
                started_at timestamptz, finished_at timestamptz,
                status text NOT NULL CHECK (status IN ('running','completed','failed')),
                disposition text, notes text, recorded_at timestamptz NOT NULL DEFAULT now());""")
            cur.execute("""INSERT INTO ops.federal_appropriations_ingest_runs
                (run_id, stream, resolved_url, source_bytes, rows_written, datasets, started_at, finished_at, status, disposition, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (str(run_id), "cbo_measure_taxonomy", "derived:cbo_key_budget_economic_data", None, rows,
                 json.dumps({"cbo_measure_taxonomy": rows}), dt.datetime.now(dt.timezone.utc),
                 dt.datetime.now(dt.timezone.utc), "completed", "ok", f"CBO measure taxonomy; families={families}"))
    except Exception as exc:
        print(f"WARN ledger: {exc}", flush=True)

def materialize(smoke=False):
    import lance, pyarrow as pa, duckdb
    so = _storage_options()
    cls = json.load(open(os.path.abspath(ARTIFACT)))
    occ = {r["row_label"]: r["total"] for r in json.load(open(os.path.abspath(LABELS)))}
    ingested_at = dt.datetime.now(dt.timezone.utc)
    rows, seen = [], set()
    for c in cls:
        lbl = c.get("row_label")
        if lbl is None or lbl in seen: continue
        seen.add(lbl)
        rows.append({"row_label": lbl, "canonical_measure": (c.get("canonical_measure") or None),
            "measure_family": (c.get("measure_family") or None), "is_total": bool(c.get("is_total")),
            "is_non_measure": bool(c.get("is_non_measure")), "confidence": c.get("confidence"),
            "occurrences": int(occ.get(lbl, 0)), "source": "cbo_key_budget_economic_data", "ingested_at": ingested_at})
    schema = pa.schema([("row_label", pa.string()), ("canonical_measure", pa.string()), ("measure_family", pa.string()),
        ("is_total", pa.bool_()), ("is_non_measure", pa.bool_()), ("confidence", pa.string()),
        ("occurrences", pa.int64()), ("source", pa.string()), ("ingested_at", pa.timestamp("us", tz="UTC"))])
    uri = URI.replace("/active/", "/smoke/") if smoke else URI
    tbl = pa.Table.from_pylist(rows, schema=schema)
    lance.write_dataset(tbl, uri, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE, storage_options=so)
    _build_indexes(uri, btree=["row_label", "measure_family"], bitmap=[], so=so)
    con = duckdb.connect(); con.register("r", tbl)
    fam = con.execute("select measure_family, count(*) from r group by 1 order by 2 desc").fetchall()
    if not smoke:
        assert len(rows) > 3000, f"only {len(rows)} labels"
        assert len(fam) >= 8, f"only {len(fam)} families"
    _record_run(uuid.uuid4(), len(rows), len(fam))
    print(f"cbo_measure_taxonomy: {len(rows)} labels, {len(fam)} families -> {uri}")
    for f, n in fam: print(f"  {str(f):20s} {n}")
    return {"rows": len(rows), "families": len(fam)}

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--smoke", action="store_true")
    materialize(smoke=ap.parse_args().smoke)

if __name__ == "__main__":
    main()
