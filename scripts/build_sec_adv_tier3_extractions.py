#!/usr/bin/env python3
"""Merge/finalize Tier-3 brochure extractions → sec_adv_tier3_extractions (Lance).

Compacts the per-brochure staging JSONs at
``s3://data-sink/active/_tier3_extract_staging/{crd}_{vid}.json`` into a single
indexed Lance dataset. Dedups on (crd_number, brochure_version_id), validates
each record against the frozen helper schema, joins adviser context from the
worklist, and writes with scalar indices.

Run:
  doppler run -p core-x -c prd -- uv run --no-project \
    --with pylance --with duckdb --with boto3 \
    python3 scripts/build_sec_adv_tier3_extractions.py
"""
from __future__ import annotations

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import boto3
import duckdb
import lance
import pyarrow as pa

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipelines", "sec_adv"))
import tier3_extract_helper as H  # noqa: E402

BUCKET = "data-sink"
STAGING_PREFIX = "active/_tier3_extract_staging/"
DST = f"s3://{BUCKET}/active/sec_adv_tier3_extractions/"
WORKLIST = f"s3://{BUCKET}/active/_tier3_extract_worklist/"

# list -> JSON-encode (nested arrays don't index; keep them as strings)
LIST_FIELDS = {"sub_strategies", "sectors", "client_types"}


def _clients():
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    so = {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
          "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
          "aws_endpoint": ep, "aws_region": "auto"}
    s3 = boto3.client("s3", endpoint_url=ep,
                      aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                      aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"])
    return s3, so


def main() -> None:
    s3, so = _clients()

    # 1. enumerate staging keys
    keys, tok = [], None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": STAGING_PREFIX, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in r.get("Contents", []) if o["Key"].endswith(".json")]
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    print(f"staging objects: {len(keys)}")

    # 2. parallel fetch + parse
    def fetch(k):
        return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())

    recs, invalid = [], []
    with ThreadPoolExecutor(max_workers=32) as ex:
        for rec in ex.map(fetch, keys):
            errs = H.validate(rec)
            if errs:
                invalid.append((rec.get("crd_number"), rec.get("brochure_version_id"), errs))
                continue
            recs.append(rec)
    print(f"valid: {len(recs)} | invalid: {len(invalid)}")
    if invalid:
        for c, v, e in invalid[:10]:
            print(f"  INVALID {c}_{v}: {'; '.join(e)}")

    # 3. dedup on (crd, version_id) — keep first
    seen, deduped = set(), []
    for rec in recs:
        key = (rec["crd_number"], rec["brochure_version_id"])
        if key in seen:
            continue
        seen.add(key)
        # normalize: list fields -> JSON strings; ensure all schema fields present
        for lf in LIST_FIELDS:
            rec[lf] = json.dumps(rec.get(lf) or [])
        deduped.append(rec)
    print(f"deduped: {len(deduped)}")

    # 4. build arrow table with explicit typed schema
    cols = ["crd_number", "brochure_version_id", "pc_verdict", "sub_strategies",
            "seniority", "sponsored", "borrower_segment", "sectors",
            "mgmt_fee_pct_min", "mgmt_fee_pct_max", "carry_pct", "hurdle_pct",
            "fee_notes", "client_types", "evidence_quote", "extraction_confidence"]
    num = {"mgmt_fee_pct_min", "mgmt_fee_pct_max", "carry_pct", "hurdle_pct"}
    data = {c: [] for c in cols}
    for rec in deduped:
        for c in cols:
            v = rec.get(c)
            data[c].append(float(v) if (c in num and v is not None) else v)
    arrays = {}
    for c in cols:
        arrays[c] = pa.array(data[c], type=pa.float64()) if c in num else pa.array(
            [None if v is None else str(v) for v in data[c]], type=pa.string())
    tbl = pa.table(arrays)

    # 5. join adviser context from worklist
    wl = lance.dataset(WORKLIST, storage_options=so).to_table(
        columns=["crd_number", "brochure_version_id", "adviser_legal_name", "tier", "pc_gav"])
    con = duckdb.connect()
    con.register("t", tbl)
    con.register("wl", wl)
    joined = con.execute("""
        SELECT t.*, wl.adviser_legal_name, wl.tier, wl.pc_gav
        FROM t LEFT JOIN wl USING (crd_number, brochure_version_id)
    """).fetch_arrow_table()
    # cast any large_string -> string
    fields = [pa.field(f.name, pa.string()) if pa.types.is_large_string(f.type) else f
              for f in joined.schema]
    joined = joined.cast(pa.schema(fields))

    # 6. write + index
    lance.write_dataset(joined, DST, storage_options=so, mode="overwrite")
    out = lance.dataset(DST, storage_options=so)
    for col, kind in [("crd_number", "BTREE"), ("brochure_version_id", "BTREE"), ("pc_verdict", "BITMAP")]:
        try:
            out.create_scalar_index(col, index_type=kind)
        except Exception as e:  # noqa: BLE001
            print(f"  index {col}: {e}")

    # 7. verify
    v = duckdb.connect()
    v.register("d", joined)
    print(f"\nrows: {out.count_rows():,}  ->  {DST}")
    print("indices:", [i["name"] for i in out.list_indices()])
    print("\npc_verdict distribution:")
    print(v.execute("SELECT pc_verdict, count(*) n FROM d GROUP BY 1 ORDER BY 2 DESC").fetchdf().to_string(index=False))
    print("\nby tier x verdict:")
    print(v.execute("SELECT tier, pc_verdict, count(*) n FROM d GROUP BY 1,2 ORDER BY 1,3 DESC").fetchdf().to_string(index=False))
    print(f"\nmgmt_fee populated: {v.execute('SELECT count(mgmt_fee_pct_min) FROM d').fetchone()[0]}")
    print(f"carry populated:    {v.execute('SELECT count(carry_pct) FROM d').fetchone()[0]}")
    print(f"declared + pc_verdict=yes: {v.execute(chr(39).join(['SELECT count(*) FROM d WHERE tier=', 'declared', ' AND pc_verdict=', 'yes', '']))}"
          if False else "")
    print("declared & yes:", v.execute("SELECT count(*) FROM d WHERE tier='declared' AND pc_verdict='yes'").fetchone()[0])


if __name__ == "__main__":
    main()
