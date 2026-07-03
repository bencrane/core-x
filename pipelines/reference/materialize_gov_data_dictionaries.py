"""Reference loader — government field DATA DICTIONARIES → Lance, from their authoritative
sources, so this mission-critical metadata lives in the data plane (R2 Lance) rather than only
in repo files/PDFs. Three dims, each 1 row per field/element, all-string (metadata is text):

  1. usaspending_data_dictionary   — the authoritative USAspending DATA Element Crosswalk (DEC),
     downloaded LIVE from files.usaspending.gov. 457 elements. THE field dictionary of record for
     the FPDS/award/subaward/account download vocabularies — supersedes the repo sidecar
     fpds_field_definitions.json and closes the award+subaward dictionary gap.
     SoR s3://data-sink/active/usaspending_data_dictionary/
  2. sam_entity_extract_dictionary — SAM.gov Entity Management master extract layout. 370 elements
     with datatype/format/mandatory/sensitivity. SoR .../active/sam_entity_extract_dictionary/
  3. sam_fal_data_dictionary       — SAM.gov Federal Assistance Listings (grants) extract. 92 fields.
     SoR .../active/sam_fal_data_dictionary/

SAM sources are the authoritative SAM.gov File Extracts, read from the R2 landing tier
(landing/sam-gov/data-dictionary/…) where they were staged verbatim. The USAspending DEC is fetched
live (public, no auth). Lance v2.1, mode=overwrite. Deferred: SAM_REPS_AND_CERTS_MAPPING (7
heterogeneous provision sheets — not a clean field schema).

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'pandas>=2' --with 'openpyxl>=3.1' \
      --with 'requests>=2.32' --with boto3 \
      python3 pipelines/reference/materialize_gov_data_dictionaries.py <build|verify>
"""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import sys

ACTIVE = "s3://data-sink/active"
DATA_STORAGE_VERSION = "2.1"
INGEST_TS = None  # set at build start (deterministic within a run)

USA_DEC_URL = "https://files.usaspending.gov/docs/Data_Dictionary_Crosswalk.xlsx"
SAM_ENTITY_KEY = "landing/sam-gov/data-dictionary/entity-information/SAM_MASTER_EXTRACT_MAPPING_Feb2025.xlsx"
SAM_FAL_KEY = "landing/sam-gov/data-dictionary/assistance-listings/FAL_Data_Dictionary.xlsx"

USA_URI = os.environ.get("USA_DATA_DICTIONARY_URI", f"{ACTIVE}/usaspending_data_dictionary/")
SAM_ENTITY_URI = os.environ.get("SAM_ENTITY_DICT_URI", f"{ACTIVE}/sam_entity_extract_dictionary/")
SAM_FAL_URI = os.environ.get("SAM_FAL_DICT_URI", f"{ACTIVE}/sam_fal_data_dictionary/")

# canonical column names (positional) for the DEC `Public` sheet — disambiguates the duplicate
# labels across the four header groups (Schema / USA Spending Downloads / Database Download / Legacy).
DEC_COLS = [
    "element", "definition", "fpds_data_dictionary_element", "grouping",
    "domain_values", "domain_values_code_description",
    "dl_award_file", "dl_award_element", "dl_subaward_file", "dl_subaward_element",
    "dl_account_file", "dl_account_element",
    "db_table", "db_element",
    "legacy_award_file", "legacy_award_element", "legacy_subaward_element",
]
SAM_ENTITY_COLS = [
    "data_element", "column_order", "datatype", "data_format", "max_length", "definition",
    "enumeration", "mandatory", "substitution_for_mandatory", "sample_values",
    "sensitivity_level", "sensitivity_group", "public", "fouo_cui", "sensitive_cui",
]
FAL_COLS = ["file_structure", "field_name", "field_type", "field_length", "definition"]

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _s3():
    import boto3
    so = _r2_so()
    return boto3.client("s3", endpoint_url=so["endpoint"], aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"], region_name="auto")


def _cell(v):
    """xlsx cell → clean str|None (kills NaN/blank; preserves everything else verbatim)."""
    import pandas as pd
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s or None


def _to_lance(rows, cols, uri, key_col, btree, bitmap, src_url, vintage):
    """Grain = synthetic `row_ord` (sheet order), guaranteeing one faithful row per source line —
    the natural key (element/data_element/field_name) is a NON-unique BTREE lookup because these
    dictionaries legitimately repeat placeholder rows (e.g. SAM 'BLANK (DEPRECATED)' positions)."""
    import pyarrow as pa
    import lance
    if not rows:
        raise RuntimeError(f"zero rows for {uri}")
    for i, r in enumerate(rows):
        r["source"] = src_url
        r["source_vintage"] = vintage
        r["ingested_at"] = INGEST_TS
    str_fields = cols + ["source", "source_vintage", "ingested_at"]
    schema = pa.schema([("row_ord", pa.int32())] + [(c, pa.string()) for c in str_fields])
    data = {"row_ord": list(range(len(rows)))}
    data.update({c: [r.get(c) for r in rows] for c in str_fields})
    tbl = pa.table(data, schema=schema)
    lance.write_dataset(tbl, uri, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                        storage_options=_r2_so())
    ds = lance.dataset(uri, storage_options=_r2_so())
    for c in ["row_ord"] + btree:
        ds.create_scalar_index(c, index_type="BTREE", replace=True); log(f"    BTREE ✓ {c}")
    for c in bitmap:
        ds.create_scalar_index(c, index_type="BITMAP", replace=True); log(f"    BITMAP ✓ {c}")
    nnull = [r.get(key_col) for r in rows if r.get(key_col)]
    log(f"  DONE → {uri} rows={tbl.num_rows}  ({key_col}: {len(set(nnull))} distinct / {len(nnull)} non-null)")
    return {"uri": uri, "rows": tbl.num_rows, "key_col": key_col,
            "key_distinct": len(set(nnull)), "key_nonnull": len(nnull)}


def build_usaspending():
    import requests, pandas as pd
    log(f"fetch USAspending DEC crosswalk (LIVE): {USA_DEC_URL}")
    resp = requests.get(USA_DEC_URL, timeout=120); resp.raise_for_status()
    pub = pd.ExcelFile(io.BytesIO(resp.content)).parse("Public", header=None, keep_default_na=False)
    # header rows 0-1; data from row 2. 17 positional columns.
    body = pub.iloc[2:]
    rows = []
    for _, raw in body.iterrows():
        vals = [_cell(raw.iloc[i]) if i < len(raw) else None for i in range(len(DEC_COLS))]
        if not vals[0]:  # no element → skip trailing/blank rows
            continue
        rows.append(dict(zip(DEC_COLS, vals)))
    log(f"  parsed {len(rows)} DEC elements")
    return _to_lance(rows, DEC_COLS, USA_URI, "element",
                     btree=["element", "dl_award_element", "fpds_data_dictionary_element"],
                     bitmap=["grouping", "dl_award_file", "dl_subaward_file"],
                     src_url=USA_DEC_URL, vintage="usaspending_dec_crosswalk_2026_07")


def _sam_sheet(key, sheet, header_row, cols, require_idx=None):
    """require_idx: a valid data row must carry a non-blank value in this column (drops metadata
    footer rows like 'Last Updated: …' that have an element label but no schema fields)."""
    import pandas as pd
    b = _s3().get_object(Bucket="data-sink", Key=key)["Body"].read()
    df = pd.ExcelFile(io.BytesIO(b)).parse(sheet, header=None, keep_default_na=False)
    rows = []
    for _, raw in df.iloc[header_row + 1:].iterrows():
        vals = [_cell(raw.iloc[i]) if i < len(raw) else None for i in range(len(cols))]
        if not vals[0]:
            continue
        if require_idx is not None and not vals[require_idx]:
            continue
        rows.append(dict(zip(cols, vals)))
    return rows


def build_sam_entity():
    log(f"read SAM entity extract mapping from R2 landing: {SAM_ENTITY_KEY}")
    rows = _sam_sheet(SAM_ENTITY_KEY, "Data Element Mapping", 3, SAM_ENTITY_COLS, require_idx=2)
    log(f"  parsed {len(rows)} SAM entity elements")
    return _to_lance(rows, SAM_ENTITY_COLS, SAM_ENTITY_URI, "data_element",
                     btree=["data_element"], bitmap=["datatype", "mandatory"],
                     src_url="SAM.gov File Extracts / " + SAM_ENTITY_KEY.split("/")[-1],
                     vintage="sam_master_extract_feb2025")


def build_sam_fal():
    """Section-aware: the FAL sheet is 3 stacked file-structures (GRANTS, DATA GOV current, DATA GOV
    archived). Tag each field with its file_structure; skip the section + repeated column headers."""
    import pandas as pd
    log(f"read SAM FAL dictionary from R2 landing: {SAM_FAL_KEY}")
    b = _s3().get_object(Bucket="data-sink", Key=SAM_FAL_KEY)["Body"].read()
    df = pd.ExcelFile(io.BytesIO(b)).parse("Sheet1", header=None, keep_default_na=False)
    rows, section, sect_ord = [], None, 0
    for i in range(len(df)):
        c0 = _cell(df.iloc[i, 0])
        if not c0:
            continue
        if "FILE STRUCTURE" in c0.upper():       # section header → new file-structure context
            sect_ord += 1
            section = f"{c0} [{sect_ord}]"
            continue
        if c0.strip().lower() == "field name":   # repeated column header
            continue
        rows.append({"file_structure": section, "field_name": c0,
                     "field_type": _cell(df.iloc[i, 1]) if df.shape[1] > 1 else None,
                     "field_length": _cell(df.iloc[i, 2]) if df.shape[1] > 2 else None,
                     "definition": _cell(df.iloc[i, 3]) if df.shape[1] > 3 else None})
    log(f"  parsed {len(rows)} FAL fields across {sect_ord} file-structures")
    return _to_lance(rows, FAL_COLS, SAM_FAL_URI, "field_name",
                     btree=["field_name"], bitmap=["field_type", "file_structure"],
                     src_url="SAM.gov File Extracts / " + SAM_FAL_KEY.split("/")[-1],
                     vintage="sam_fal_data_dictionary")


def build():
    global INGEST_TS
    INGEST_TS = dt.datetime.now(dt.timezone.utc).isoformat()
    out = {}
    log("== usaspending_data_dictionary ==");   out["usaspending"] = build_usaspending()
    log("== sam_entity_extract_dictionary =="); out["sam_entity"] = build_sam_entity()
    log("== sam_fal_data_dictionary ==");        out["sam_fal"] = build_sam_fal()
    return out


def verify():
    import lance, collections
    so = _r2_so()
    out = {}
    for name, uri, key in [("usaspending_data_dictionary", USA_URI, "element"),
                           ("sam_entity_extract_dictionary", SAM_ENTITY_URI, "data_element"),
                           ("sam_fal_data_dictionary", SAM_FAL_URI, "field_name")]:
        ds = lance.dataset(uri, storage_options=so)
        cols = [f.name for f in ds.schema]
        keys = ds.scanner(columns=[key]).to_table().column(key).to_pylist()
        idx = [getattr(i, "name", str(i)) for i in ds.list_indices()]
        out[name] = {"uri": uri, "rows": ds.count_rows(), "cols": len(cols),
                     "key_distinct": len(set(k for k in keys if k)),
                     "grain_unique": len([k for k in keys if k]) == len(set(k for k in keys if k)),
                     "indices": idx}
    # spot-checks against known elements
    usa = lance.dataset(USA_URI, storage_options=so)
    out["spot_check_usa"] = usa.scanner(
        columns=["element", "definition", "fpds_data_dictionary_element", "grouping", "dl_award_element", "dl_subaward_element"],
        filter="element IN ('ActionType','NAICSCode','FederalActionObligation','8aProgramParticipant')").to_table().to_pylist()
    print(json.dumps(out, indent=2, default=str))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        print(json.dumps(build(), indent=2, default=str))
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)"); sys.exit(2)


if __name__ == "__main__":
    main()
