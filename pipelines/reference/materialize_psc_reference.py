"""Reference loader — psc_reference: the official FPDS Product & Service Code (PSC) manual as a
canonical Lance lookup. The source of truth for psc_code -> name / description / hierarchy, used
to resolve any PSC encountered across the award feeds and to drive the PSC->vertical map.

GRAIN  1 row per (psc_code, effective period) from the GSA PSC Manual xlsx 'PSC for MMYYYY' sheet.
       Codes carry HISTORY: a superseded code keeps its retired row (end_date set) AND its current
       row (end_date NULL). Resolve current meaning with  is_active = (end_date IS NULL).
SoR    s3://data-sink/active/psc_reference/   (Lance v2.1; derived, mode=overwrite — idempotent re-import)
SOURCE Official GSA "Product and Service Codes (PSC) Manual" Excel (FY edition), e.g.
       'PSC April 2025.xlsx'. Publicly re-obtainable from GSA/FPDS; pass via --source or
       PSC_SOURCE_XLSX. The 'Category Managers' sheet (admin contacts) and the companion .docx
       (prose manual; its 162 tables are a subset of this sheet) are intentionally NOT loaded.
KEYS   psc_code (BTREE) is the resolution key; parent_psc_code (BTREE) walks the hierarchy.
       is_active / is_product / psc_category / code_len / cm_level1 are BITMAP filter axes.
REFRESH drop the next FY edition xlsx, re-run `build --source <path>`; overwrite is deterministic.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pandas>=2' --with 'openpyxl>=3' --with 'pylance>=7' --with 'pyarrow>=17' \
      python3 pipelines/reference/materialize_psc_reference.py <build|verify> [--source PATH]
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

ACTIVE = "s3://data-sink/active"
REF_URI = os.environ.get("PSC_REFERENCE_URI", f"{ACTIVE}/psc_reference/")
DEFAULT_SOURCE = os.environ.get("PSC_SOURCE_XLSX", "/Users/benjamincrane/Downloads/PSC April 2025.xlsx")
SHEET = os.environ.get("PSC_SOURCE_SHEET", "PSC for 042025")
DATA_STORAGE_VERSION = "2.1"

BTREE_INDEXES = ["psc_code", "parent_psc_code"]
BITMAP_INDEXES = ["is_active", "is_product", "psc_category", "code_len", "cm_level1"]

# Reference table is tiny, but the scalar-index build is still an external sort; keep the fleet
# rule (sort in-RAM, never spill to the default tiny DataFusion pool). Harmless at 6k rows.
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


def _clean(v):
    import pandas as pd
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s or None


def _date(v):
    import pandas as pd
    s = _clean(v)
    if s is None:
        return None
    try:
        return pd.to_datetime(s).date().isoformat()
    except Exception:  # noqa: BLE001
        return s[:10] if len(s) >= 10 else s


def _assemble(source: str):
    import pandas as pd
    import pyarrow as pa

    log(f"reading {source!r} sheet={SHEET!r}")
    df = pd.read_excel(source, sheet_name=SHEET, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    vintage = os.path.basename(source)
    ingested = dt.datetime.now(dt.timezone.utc).isoformat()

    C = {  # source column -> normalized name
        "PSC CODE": "psc_code",
        "PRODUCT AND SERVICE CODE NAME": "psc_name",
        "PRODUCT AND SERVICE CODE FULL NAME (DESCRIPTION)": "full_description",
        "PRODUCT AND SERVICE CODE INCLUDES": "includes",
        "PRODUCT AND SERVICE CODE EXCLUDES": "excludes",
        "PRODUCT AND SERVICE CODE NOTES": "notes",
        "PSC Category: Service (S)/Product (P)": "sp_flag",
        "Level 1 Category Code": "cm_level1_code",
        "Level 1 Category": "cm_level1",
        "Level 2 Category Code": "cm_level2_code",
        "Level 2 Category": "cm_level2",
    }
    missing = [c for c in C if c not in df.columns]
    if "PSC CODE" in missing:
        raise RuntimeError(f"source missing 'PSC CODE'; got columns={list(df.columns)}")

    cols: dict[str, list] = {v: [] for v in C.values()}
    extra = ["start_date", "end_date", "is_active", "parent_psc_code", "parent_psc_raw",
             "psc_category", "code_len", "is_product", "source_vintage", "ingested_at"]
    for k in extra:
        cols[k] = []

    for _, row in df.iterrows():
        code = _clean(row.get("PSC CODE"))
        if not code:
            continue
        code = code.upper()
        for src, dst in C.items():
            cols[dst].append(_clean(row.get(src)) if src in df.columns else None)
        end = _date(row.get("END DATE"))
        cols["start_date"].append(_date(row.get("START DATE")))
        cols["end_date"].append(end)
        cols["is_active"].append(end is None)
        praw = _clean(row.get("Parent PSC Code"))
        cols["parent_psc_raw"].append(praw)
        cols["parent_psc_code"].append(praw.split(" - ", 1)[0].strip().upper() if praw else None)
        cols["psc_category"].append(code[0].upper())
        cols["code_len"].append(len(code))
        cols["is_product"].append(code[0].isdigit())
        cols["source_vintage"].append(vintage)
        cols["ingested_at"].append(ingested)
        cols["psc_code"][-1] = code  # ensure upper-cased survivor

    schema = pa.schema([
        ("psc_code", pa.string()), ("psc_name", pa.string()), ("full_description", pa.string()),
        ("includes", pa.string()), ("excludes", pa.string()), ("notes", pa.string()),
        ("start_date", pa.string()), ("end_date", pa.string()), ("is_active", pa.bool_()),
        ("parent_psc_code", pa.string()), ("parent_psc_raw", pa.string()),
        ("sp_flag", pa.string()), ("cm_level1_code", pa.string()), ("cm_level1", pa.string()),
        ("cm_level2_code", pa.string()), ("cm_level2", pa.string()),
        ("psc_category", pa.string()), ("code_len", pa.int32()), ("is_product", pa.bool_()),
        ("source_vintage", pa.string()), ("ingested_at", pa.string()),
    ])
    return pa.table({f.name: cols[f.name] for f in schema}, schema=schema)


def build(source: str = DEFAULT_SOURCE):
    import lance
    so = _r2_so()
    tbl = _assemble(source)
    rows = tbl.num_rows
    if rows == 0:
        raise RuntimeError("zero PSC rows assembled")
    log(f"assembled {rows:,} PSC rows ({len(set(tbl.column('psc_code').to_pylist()))} distinct codes)")
    lance.write_dataset(tbl, REF_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(REF_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        log(f"  BTREE ✓ {col}")
    for col in BITMAP_INDEXES:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        log(f"  BITMAP ✓ {col}")
    log(f"DONE → {REF_URI} rows={rows:,}")
    return {"rows": rows, "uri": REF_URI}


def verify():
    import lance
    so = _r2_so()
    ds = lance.dataset(REF_URI, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    codes = ds.scanner(columns=["psc_code"]).to_table().column("psc_code").to_pylist()
    out = {
        "uri": REF_URI, "rows": ds.count_rows(), "distinct_codes": len(set(codes)),
        "active": ds.count_rows(filter="is_active = true"),
        "retired": ds.count_rows(filter="is_active = false"),
        "products": ds.count_rows(filter="is_product = true"),
        "services": ds.count_rows(filter="is_product = false"),
        "with_full_description": ds.count_rows(filter="full_description IS NOT NULL"),
        "with_includes": ds.count_rows(filter="includes IS NOT NULL"),
        "columns": ds.schema.names, "indices": idx,
    }
    # spot-check resolution of a few codes the map work referenced
    sample = ds.scanner(columns=["psc_code", "psc_name", "is_active", "is_product", "psc_category",
                                 "cm_level1"],
                        filter="psc_code IN ('Y1LB','R425','5120','V111','6505') AND is_active = true"
                        ).to_table().to_pylist()
    out["spot_check"] = sample
    print(json.dumps(out, indent=2, default=str))


def main():
    args = sys.argv[1:]
    cmd = args[0] if args else "verify"
    source = DEFAULT_SOURCE
    if "--source" in args:
        source = args[args.index("--source") + 1]
    if cmd == "build":
        print(json.dumps(build(source=source), indent=2, default=str))
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
