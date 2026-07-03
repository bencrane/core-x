"""Reference loader — sam_reps_certs_provisions: the SAM.gov Representations & Certifications
provision→question mapping, materialized from the authoritative SAM.gov File Extract
(SAM_REPS_AND_CERTS_MAPPING.xlsx, staged in the R2 landing tier) into Lance.

The workbook stacks provisions across sheets by regime; five share one 7-column provision schema and
are unioned here with a `provision_family` discriminator:
  FAR (138) · DFARS (26) · SF330 (9) · FINANCIAL ASSISTANCE (4) · READ-ONLY (26).
Deferred (structurally different, not provision rows): 'SF330 ARCHITECT-ENG REFERENCES' (discipline/
experience/revenue CODE lists) and 'DOWNLOAD URLs'.

GRAIN  synthetic row_ord (1 row per provision line). provision is a NON-unique BTREE lookup.
SoR    s3://data-sink/active/sam_reps_certs_provisions/   (Lance v2.1, mode=overwrite)
COLS   row_ord, provision_family, provision, answer_id, question_or_cert, sample_value,
       mandatory_optional, required_condition, enumeration, source, source_vintage, ingested_at

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'pandas>=2' --with 'openpyxl>=3.1' --with boto3 \
      python3 pipelines/reference/materialize_sam_reps_certs.py <build|verify>
"""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import sys

ACTIVE = "s3://data-sink/active"
DATA_STORAGE_VERSION = "2.1"
URI = os.environ.get("SAM_REPS_CERTS_URI", f"{ACTIVE}/sam_reps_certs_provisions/")
SRC_KEY = "landing/sam-gov/data-dictionary/entity-information/SAM_REPS_AND_CERTS_MAPPING.xlsx"
# 5 sheets sharing the 7-col provision schema; header at row index 1, data from row 2.
SHEETS = {"FAR": "FAR", "DFARS": "DFARS", "SF330": "SF330",
          "FINANCIAL ASSISTANCE": "FINANCIAL_ASSISTANCE", "READ-ONLY": "READ_ONLY"}
COLS = ["provision", "answer_id", "question_or_cert", "sample_value",
        "mandatory_optional", "required_condition", "enumeration"]

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


def _cell(v):
    import pandas as pd
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s or None


def build():
    import boto3, pandas as pd, pyarrow as pa, lance
    so = _r2_so()
    s3 = boto3.client("s3", endpoint_url=so["endpoint"], aws_access_key_id=so["aws_access_key_id"],
                      aws_secret_access_key=so["aws_secret_access_key"], region_name="auto")
    b = s3.get_object(Bucket="data-sink", Key=SRC_KEY)["Body"].read()
    xl = pd.ExcelFile(io.BytesIO(b))
    ingested = dt.datetime.now(dt.timezone.utc).isoformat()
    rows = []
    for sheet, family in SHEETS.items():
        df = xl.parse(sheet, header=None, keep_default_na=False)
        n = 0
        for _, raw in df.iloc[2:].iterrows():          # header row 1 → data from row 2
            vals = [_cell(raw.iloc[i]) if i < len(raw) else None for i in range(len(COLS))]
            if not vals[0]:                            # blank/continuation row → skip
                continue
            rec = dict(zip(COLS, vals)); rec["provision_family"] = family
            rows.append(rec); n += 1
        log(f"  {sheet}: {n} provisions")
    fields = ["provision_family"] + COLS + ["source", "source_vintage", "ingested_at"]
    for i, r in enumerate(rows):
        r["source"] = "SAM.gov File Extracts / SAM_REPS_AND_CERTS_MAPPING.xlsx"
        r["source_vintage"] = "sam_reps_and_certs_mapping"
        r["ingested_at"] = ingested
    schema = pa.schema([("row_ord", pa.int32())] + [(c, pa.string()) for c in fields])
    data = {"row_ord": list(range(len(rows)))}
    data.update({c: [r.get(c) for r in rows] for c in fields})
    tbl = pa.table(data, schema=schema)
    lance.write_dataset(tbl, URI, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(URI, storage_options=so)
    for c in ["row_ord", "provision"]:
        ds.create_scalar_index(c, index_type="BTREE", replace=True); log(f"  BTREE ✓ {c}")
    for c in ["provision_family", "mandatory_optional"]:
        ds.create_scalar_index(c, index_type="BITMAP", replace=True); log(f"  BITMAP ✓ {c}")
    log(f"DONE → {URI} rows={tbl.num_rows}")
    return {"uri": URI, "rows": tbl.num_rows}


def verify():
    import lance, collections
    ds = lance.dataset(URI, storage_options=_r2_so())
    t = ds.scanner(columns=["provision_family", "provision"]).to_table()
    out = {"uri": URI, "rows": ds.count_rows(), "cols": len([f.name for f in ds.schema]),
           "by_family": dict(collections.Counter(t.column("provision_family").to_pylist())),
           "indices": [getattr(i, "name", str(i)) for i in ds.list_indices()],
           "spot_check": ds.scanner(columns=["provision_family", "provision", "question_or_cert", "mandatory_optional"],
                                    filter="provision_family = 'FAR'").to_table().slice(0, 3).to_pylist()}
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
