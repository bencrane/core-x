"""Reference loader — naics_reference + naics_index: the official U.S. Census NAICS 2022 catalog
as canonical Lance lookups, mirroring psc_reference. The system of record for
naics_code -> title / description / hierarchy, plus the Census keyword index.

TABLES
  s3://data-sink/active/naics_reference/   1 row per NAICS code (2-6 digit), ~2,125 codes.
      naics_code, naics_title, description, level, code_len, sector_code, parent_code,
      is_trilateral, change_indicator, source_vintage, ingested_at.
      BTREE: naics_code, parent_code | BITMAP: level, code_len, sector_code, is_trilateral.
  s3://data-sink/active/naics_index/       1 row per Census index phrase (~20,398), 6-digit keyed.
      naics_code, index_item, index_item_lc, source_vintage, ingested_at.   BTREE: naics_code.

SOURCE Census Bureau NAICS 2022 downloadable files (census.gov/naics), fetched over HTTPS by
       default; override with --source-dir <dir> containing descriptions.xlsx / index.xlsx /
       structure.xlsx. The 'codes_2-6 digit' file is a strict subset of descriptions (skipped).
       NAICS is on a 5-year revision cycle (current 2022; next 2027) — refresh by re-running build.
NOTE   Census appends a trailing 'T' to titles of trilaterally-agreed (2-5 digit) codes; it is
       stripped into naics_title and surfaced as is_trilateral.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pandas>=2' --with 'openpyxl>=3' --with 'pylance>=7' --with 'pyarrow>=17' \
      python3 pipelines/reference/materialize_naics_reference.py <build|verify> [--source-dir DIR]
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
import tempfile
from collections import defaultdict

ACTIVE = "s3://data-sink/active"
NAICS_REF_URI = os.environ.get("NAICS_REFERENCE_URI", f"{ACTIVE}/naics_reference/")
NAICS_INDEX_URI = os.environ.get("NAICS_INDEX_URI", f"{ACTIVE}/naics_index/")
DATA_STORAGE_VERSION = "2.1"
SOURCE_VINTAGE = "NAICS_2022"

URLS = {
    "descriptions": "https://www.census.gov/naics/2022NAICS/2022_NAICS_Descriptions.xlsx",
    "index": "https://www.census.gov/naics/2022NAICS/2022_NAICS_Index_File.xlsx",
    "structure": "https://www.census.gov/naics/2022NAICS/2022_NAICS_Structure.xlsx",
}
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

LEVELS = {2: "sector", 3: "subsector", 4: "industry_group", 5: "naics_industry", 6: "national_industry"}

REF_BTREE = ["naics_code", "parent_code"]
REF_BITMAP = ["level", "code_len", "sector_code", "is_trilateral"]
INDEX_BTREE = ["naics_code"]

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


def _resolve_sources(source_dir: str | None) -> dict[str, str]:
    if source_dir:
        return {k: os.path.join(source_dir, f"{k}.xlsx") for k in URLS}
    import urllib.request
    d = tempfile.mkdtemp(prefix="naics_")
    out = {}
    for k, u in URLS.items():
        p = os.path.join(d, f"{k}.xlsx")
        req = urllib.request.Request(u, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=90) as r, open(p, "wb") as f:
            f.write(r.read())
        log(f"fetched {k}: {os.path.getsize(p):,} bytes")
        out[k] = p
    return out


def _structure_change_map(path: str) -> dict[str, str]:
    """col layout: [change_indicator, code, title]; header + a note row precede the data."""
    import pandas as pd
    try:
        raw = pd.read_excel(path, header=None, dtype=str)
    except Exception as exc:  # noqa: BLE001
        log(f"WARN: structure parse failed ({exc}); change_indicator omitted")
        return {}
    m = {}
    for _, r in raw.iterrows():
        vals = ["" if (x is None or (isinstance(x, float) and pd.isna(x))) else str(x).strip() for x in r.tolist()]
        for i, v in enumerate(vals):
            if re.fullmatch(r"\d{2,6}", v):
                chg = vals[i - 1] if i >= 1 else ""
                if chg and chg.lower() != "change indicator":
                    m[v] = chg
                break
    return m


def _assemble_reference(desc_path: str, struct_path: str):
    import pandas as pd
    import pyarrow as pa
    df = pd.read_excel(desc_path, sheet_name=0, dtype=str)
    cmap = {c.lower().strip(): c for c in df.columns}
    code_c, title_c, desc_c = cmap["code"], cmap["title"], cmap["description"]
    chg = _structure_change_map(struct_path)
    ingested = dt.datetime.now(dt.timezone.utc).isoformat()

    cols = defaultdict(list)
    for _, r in df.iterrows():
        code = _clean(r[code_c])
        if not code:
            continue
        traw = (_clean(r[title_c]) or "")
        is_tri = traw.endswith("T")
        title = traw[:-1].strip() if is_tri else traw
        L = len(code)
        cols["naics_code"].append(code)
        cols["naics_title"].append(title or None)
        cols["description"].append(_clean(r[desc_c]))
        cols["level"].append(LEVELS.get(L, f"len{L}"))
        cols["code_len"].append(L)
        cols["sector_code"].append(code[:2])
        cols["parent_code"].append(code[:-1] if L > 2 else None)
        cols["is_trilateral"].append(is_tri)
        cols["change_indicator"].append(chg.get(code))
        cols["source_vintage"].append(SOURCE_VINTAGE)
        cols["ingested_at"].append(ingested)

    schema = pa.schema([
        ("naics_code", pa.string()), ("naics_title", pa.string()), ("description", pa.string()),
        ("level", pa.string()), ("code_len", pa.int32()), ("sector_code", pa.string()),
        ("parent_code", pa.string()), ("is_trilateral", pa.bool_()),
        ("change_indicator", pa.string()), ("source_vintage", pa.string()), ("ingested_at", pa.string()),
    ])
    return pa.table({f.name: cols[f.name] for f in schema}, schema=schema)


def _assemble_index(path: str):
    import pandas as pd
    import pyarrow as pa
    df = pd.read_excel(path, sheet_name=0, dtype=str)
    code_c = next(c for c in df.columns if "naics" in c.lower())
    item_c = next(c for c in df.columns if c != code_c)
    ingested = dt.datetime.now(dt.timezone.utc).isoformat()
    cols = defaultdict(list)
    for _, r in df.iterrows():
        code = _clean(r[code_c])
        item = _clean(r[item_c])
        if not code or not item:
            continue
        cols["naics_code"].append(code)
        cols["index_item"].append(item)
        cols["index_item_lc"].append(item.lower())
        cols["source_vintage"].append(SOURCE_VINTAGE)
        cols["ingested_at"].append(ingested)
    schema = pa.schema([
        ("naics_code", pa.string()), ("index_item", pa.string()), ("index_item_lc", pa.string()),
        ("source_vintage", pa.string()), ("ingested_at", pa.string()),
    ])
    return pa.table({f.name: cols[f.name] for f in schema}, schema=schema)


def _write(tbl, uri, btree, bitmap, so):
    import lance
    lance.write_dataset(tbl, uri, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(uri, storage_options=so)
    for col in btree:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        log(f"  BTREE ✓ {col}")
    for col in bitmap:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        log(f"  BITMAP ✓ {col}")
    log(f"DONE → {uri} rows={tbl.num_rows:,}")


def build(source_dir: str | None = None):
    so = _r2_so()
    src = _resolve_sources(source_dir)
    ref = _assemble_reference(src["descriptions"], src["structure"])
    log(f"assembled naics_reference: {ref.num_rows:,} rows")
    _write(ref, NAICS_REF_URI, REF_BTREE, REF_BITMAP, so)
    idx = _assemble_index(src["index"])
    log(f"assembled naics_index: {idx.num_rows:,} rows")
    _write(idx, NAICS_INDEX_URI, INDEX_BTREE, [], so)
    return {"naics_reference_rows": ref.num_rows, "naics_index_rows": idx.num_rows}


def verify():
    import lance
    so = _r2_so()
    out = {}
    ref = lance.dataset(NAICS_REF_URI, storage_options=so)
    out["naics_reference"] = {
        "rows": ref.count_rows(),
        "by_level": {lv: ref.count_rows(filter=f"level = '{lv}'") for lv in LEVELS.values()},
        "with_description": ref.count_rows(filter="description IS NOT NULL"),
        "trilateral": ref.count_rows(filter="is_trilateral = true"),
        "indices": [getattr(i, "name", i.get("name") if isinstance(i, dict) else str(i)) for i in ref.list_indices()],
        "spot_check": ref.scanner(
            columns=["naics_code", "naics_title", "level", "parent_code"],
            filter="naics_code IN ('23','236220','336411','541330','11')").to_table().to_pylist(),
    }
    idx = lance.dataset(NAICS_INDEX_URI, storage_options=so)
    out["naics_index"] = {
        "rows": idx.count_rows(),
        "distinct_codes": len(set(idx.scanner(columns=["naics_code"]).to_table().column("naics_code").to_pylist())),
        "indices": [getattr(i, "name", i.get("name") if isinstance(i, dict) else str(i)) for i in idx.list_indices()],
        "sample_for_336411": idx.scanner(columns=["index_item"], filter="naics_code = '336411'", limit=5).to_table().column("index_item").to_pylist(),
    }
    print(json.dumps(out, indent=2, default=str))


def main():
    args = sys.argv[1:]
    cmd = args[0] if args else "verify"
    source_dir = args[args.index("--source-dir") + 1] if "--source-dir" in args else None
    if cmd == "build":
        print(json.dumps(build(source_dir=source_dir), indent=2, default=str))
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
