"""Reference loader — naics_psc_vertical_map: the (naics_code, psc_code) -> vertical / work_type /
equipment_intensity classification for the 279 highest-$ award combos (80% of trailing-2yr $).

This is the BRIDGE that turns the Stage-1 classified dictionary into queryable labels:
materialize_awards_map.py LEFT JOINs this table onto every award action, so vertical /
work_type / equipment_intensity become filterable, BITMAP-indexed columns on
usaspending_awards_map_serving. Award rows whose (naics_code, psc_code) pair is not in this
map (the long tail outside the top-$ region) get NULL labels (queryable as "unclassified").

GRAIN  1 row per (naics_code, psc_code) pair. 279 rows (deduped on the pair key).
SoR    s3://data-sink/active/naics_psc_vertical_map/   (Lance v2.1; derived, mode=overwrite)
SOURCE pipelines/reference/data/naics_psc_top279_classified.csv — the committed Stage-1 output
       (in-session Opus 4.8 classification; each pair enriched with official PSC/NAICS text +
       behavioral stats, then labeled). Re-run after re-classifying to refresh.
KEYS   naics_code + psc_code (BTREE) — the join key onto the awards serving table.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' \
      python3 pipelines/reference/materialize_naics_psc_vertical_map.py <build|verify>
"""
from __future__ import annotations

import collections
import csv
import datetime as dt
import json
import os
import sys
from pathlib import Path

ACTIVE = "s3://data-sink/active"
MAP_URI = os.environ.get("NAICS_PSC_VERTICAL_MAP_URI", f"{ACTIVE}/naics_psc_vertical_map/")
DEFAULT_CSV = str(Path(__file__).parent / "data" / "naics_psc_top279_classified.csv")
DATA_STORAGE_VERSION = "2.1"
SOURCE_VINTAGE = "top279_2026_06"
BTREE_INDEXES = ["naics_code", "psc_code"]

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

LABEL_COLS = ["vertical", "work_type", "equipment_intensity", "regime", "what_was_done", "confidence"]


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _assemble(csv_path: str):
    import pyarrow as pa
    rows = list(csv.DictReader(open(csv_path)))
    if not rows:
        raise RuntimeError(f"no rows in {csv_path}")
    ingested = dt.datetime.now(dt.timezone.utc).isoformat()

    out, seen, dups = [], set(), 0
    for r in rows:
        n = (r.get("naics_code") or "").strip()
        p = (r.get("psc_code") or "").strip().upper()
        if not n or not p:
            continue
        key = (n, p)
        if key in seen:  # the join must stay 1:1 — never fan out award rows
            dups += 1
            continue
        seen.add(key)
        rec = {"naics_code": n, "psc_code": p}
        for c in LABEL_COLS:
            rec[c] = ((r.get(c) or "").strip() or None)
        rec["source_vintage"] = SOURCE_VINTAGE
        rec["ingested_at"] = ingested
        out.append(rec)
    if dups:
        log(f"WARN: dropped {dups} duplicate (naics_code, psc_code) rows — kept first")

    fields = ["naics_code", "psc_code"] + LABEL_COLS + ["source_vintage", "ingested_at"]
    schema = pa.schema([(f, pa.string()) for f in fields])
    return pa.table({f: [r[f] for r in out] for f in fields}, schema=schema)


def build(csv_path: str = DEFAULT_CSV):
    import lance
    so = _r2_so()
    tbl = _assemble(csv_path)
    if tbl.num_rows == 0:
        raise RuntimeError("zero classified pairs assembled")
    log(f"assembled {tbl.num_rows} (naics_code, psc_code) -> vertical rows from {csv_path}")
    lance.write_dataset(tbl, MAP_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(MAP_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        log(f"  BTREE ✓ {col}")
    log(f"DONE → {MAP_URI} rows={tbl.num_rows}")
    return {"rows": tbl.num_rows, "uri": MAP_URI}


def verify():
    import lance
    so = _r2_so()
    ds = lance.dataset(MAP_URI, storage_options=so)
    t = ds.scanner(columns=["naics_code", "psc_code", "vertical", "work_type", "equipment_intensity"]).to_table()
    pairs = set(zip(t.column("naics_code").to_pylist(), t.column("psc_code").to_pylist()))
    try:
        idx = [getattr(i, "name", i.get("name") if isinstance(i, dict) else str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    out = {
        "uri": MAP_URI, "rows": ds.count_rows(), "distinct_pairs": len(pairs),
        "verticals": dict(collections.Counter(t.column("vertical").to_pylist())),
        "work_types": dict(collections.Counter(t.column("work_type").to_pylist())),
        "equipment_intensity": dict(collections.Counter(t.column("equipment_intensity").to_pylist())),
        "indices": idx,
    }
    print(json.dumps(out, indent=2, default=str))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        print(json.dumps(build(), indent=2, default=str))
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
