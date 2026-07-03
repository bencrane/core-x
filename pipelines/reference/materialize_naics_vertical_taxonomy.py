"""Reference loader — naics_vertical_taxonomy: naics_code -> vertical_category for every NAICS
node (2-6 digit) that carries federal award and/or SAM-registry signal, plus the behavioral
counts behind the assignment. The NAICS-grained companion to naics_psc_vertical_map: that table
is (naics_code, psc_code) pair-grained over the top-$ region; this one is pure naics_code-grained
across the full hierarchy, so a bare NAICS lookup (no PSC in hand) resolves to a vertical.

GRAIN  1 row per naics_code. 2,432 rows (naics_code is unique; levels 2-6 all present).
SoR    s3://data-sink/active/naics_vertical_taxonomy/   (Lance v2.1; derived, mode=overwrite)
SOURCE pipelines/reference/data/naics_vertical_taxonomy_full.csv — committed build input
       (each NAICS node classified to one of 24 verticals, carrying trailing award-action /
       award-company / SAM-entity counts and a presence flag). Re-run after re-classifying.
COLS   naics_code, naics_level, naics_title, vertical_category,
       n_award_actions, n_award_companies, n_sam_entities, presence, notes,
       source_vintage, ingested_at.
KEYS   naics_code (BTREE) — the join/resolution key.
       naics_level, vertical_category, presence (BITMAP) — low-cardinality pushdown filters.
PRESENCE  both = in award actions AND SAM registry | data_only = observed in data, not the
       official 2022 catalog | official = in the catalog, no observed award/registry signal.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' \
      python3 pipelines/reference/materialize_naics_vertical_taxonomy.py <build|verify>
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
TAXONOMY_URI = os.environ.get("NAICS_VERTICAL_TAXONOMY_URI", f"{ACTIVE}/naics_vertical_taxonomy/")
DEFAULT_CSV = str(Path(__file__).parent / "data" / "naics_vertical_taxonomy_full.csv")
DATA_STORAGE_VERSION = "2.1"
SOURCE_VINTAGE = "naics2022_vertical_2026_07"

BTREE_INDEXES = ["naics_code"]
BITMAP_INDEXES = ["naics_level", "vertical_category", "presence"]

STR_COLS = ["naics_title", "vertical_category", "presence", "notes"]
INT_COLS = ["n_award_actions", "n_award_companies", "n_sam_entities"]

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


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


def _int(v):
    s = (v or "").strip()
    if not s:
        return None
    return int(float(s))


def _str(v):
    return ((v or "").strip() or None)


def _assemble(csv_path: str):
    import pyarrow as pa
    rows = list(csv.DictReader(open(csv_path)))
    if not rows:
        raise RuntimeError(f"no rows in {csv_path}")
    ingested = dt.datetime.now(dt.timezone.utc).isoformat()

    out, seen, dups = [], set(), 0
    for r in rows:
        code = (r.get("naics_code") or "").strip()
        if not code:
            continue
        if code in seen:  # naics_code is the 1:1 resolution key — never fan out
            dups += 1
            continue
        seen.add(code)
        rec = {
            "naics_code": code,
            "naics_level": _int(r.get("naics_level")),
        }
        for c in STR_COLS:
            rec[c] = _str(r.get(c))
        for c in INT_COLS:
            rec[c] = _int(r.get(c))
        rec["source_vintage"] = SOURCE_VINTAGE
        rec["ingested_at"] = ingested
        out.append(rec)
    if dups:
        log(f"WARN: dropped {dups} duplicate naics_code rows — kept first")

    schema = pa.schema([
        ("naics_code", pa.string()),
        ("naics_level", pa.int32()),
        ("naics_title", pa.string()),
        ("vertical_category", pa.string()),
        ("n_award_actions", pa.int32()),
        ("n_award_companies", pa.int32()),
        ("n_sam_entities", pa.int32()),
        ("presence", pa.string()),
        ("notes", pa.string()),
        ("source_vintage", pa.string()),
        ("ingested_at", pa.string()),
    ])
    return pa.table({f.name: [r[f.name] for r in out] for f in schema}, schema=schema)


def build(csv_path: str = DEFAULT_CSV):
    import lance
    so = _r2_so()
    tbl = _assemble(csv_path)
    if tbl.num_rows == 0:
        raise RuntimeError("zero taxonomy rows assembled")
    log(f"assembled {tbl.num_rows} naics_code -> vertical rows from {csv_path}")
    lance.write_dataset(tbl, TAXONOMY_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(TAXONOMY_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        log(f"  BTREE ✓ {col}")
    for col in BITMAP_INDEXES:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        log(f"  BITMAP ✓ {col}")
    log(f"DONE → {TAXONOMY_URI} rows={tbl.num_rows}")
    return {"rows": tbl.num_rows, "uri": TAXONOMY_URI}


def verify():
    import lance
    so = _r2_so()
    ds = lance.dataset(TAXONOMY_URI, storage_options=so)
    t = ds.scanner(columns=["naics_code", "naics_level", "vertical_category", "presence"]).to_table()
    codes = t.column("naics_code").to_pylist()
    try:
        idx = [getattr(i, "name", i.get("name") if isinstance(i, dict) else str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    out = {
        "uri": TAXONOMY_URI,
        "rows": ds.count_rows(),
        "distinct_naics_code": len(set(codes)),
        "by_level": dict(sorted(collections.Counter(t.column("naics_level").to_pylist()).items())),
        "by_presence": dict(collections.Counter(t.column("presence").to_pylist())),
        "verticals": dict(collections.Counter(t.column("vertical_category").to_pylist())),
        "indices": idx,
        "spot_check": ds.scanner(
            columns=["naics_code", "naics_level", "naics_title", "vertical_category",
                     "n_award_actions", "n_sam_entities", "presence"],
            filter="naics_code IN ('336411','54','541330','332994','11')").to_table().to_pylist(),
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
