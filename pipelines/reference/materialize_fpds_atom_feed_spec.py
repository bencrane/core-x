"""Reference loader — fpds_atom_feed_spec: the FPDS-NG Atom Feed Specification (V1.5.3), the
authoritative structural spec for the FPDS Atom feed (feed XML, Atom Element Definitions, Award/IDV
XML), materialized as a page-level TEXT reference so it is queryable in-plane rather than a PDF in
the landing tier. It is a wiki-exported spec (no tables) → a RAG/lookup surface, not a field table.

GRAIN  1 row per page (12). row_ord = page_number.
SoR    s3://data-sink/active/fpds_atom_feed_spec/   (Lance v2.1, mode=overwrite)
COLS   row_ord, page_number, text, text_char_len, doc_name, source, source_vintage, ingested_at

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'pdfplumber>=0.11' --with boto3 \
      python3 pipelines/reference/materialize_fpds_atom_feed_spec.py <build|verify>
"""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import sys

ACTIVE = "s3://data-sink/active"
DATA_STORAGE_VERSION = "2.1"
URI = os.environ.get("FPDS_ATOM_FEED_SPEC_URI", f"{ACTIVE}/fpds_atom_feed_spec/")
SRC_KEY = "landing/sam-gov/data-dictionary/contract-awards/atom-feed/FPDS_Atom_Feed_Specifications_V1.5.3.pdf"
DOC_NAME = "FPDS_Atom_Feed_Specifications_V1.5.3.pdf"

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


def build():
    import boto3, pdfplumber, pyarrow as pa, lance
    so = _r2_so()
    s3 = boto3.client("s3", endpoint_url=so["endpoint"], aws_access_key_id=so["aws_access_key_id"],
                      aws_secret_access_key=so["aws_secret_access_key"], region_name="auto")
    b = s3.get_object(Bucket="data-sink", Key=SRC_KEY)["Body"].read()
    ingested = dt.datetime.now(dt.timezone.utc).isoformat()
    rows = []
    with pdfplumber.open(io.BytesIO(b)) as pdf:
        for i, pg in enumerate(pdf.pages):
            text = (pg.extract_text() or "").strip()
            rows.append({"page_number": str(i), "text": text or None,
                         "text_char_len": str(len(text)), "doc_name": DOC_NAME,
                         "source": SRC_KEY, "source_vintage": "fpds_atom_feed_v1_5_3",
                         "ingested_at": ingested})
    log(f"  extracted {len(rows)} pages, total chars={sum(int(r['text_char_len']) for r in rows):,}")
    fields = ["page_number", "text", "text_char_len", "doc_name", "source", "source_vintage", "ingested_at"]
    schema = pa.schema([("row_ord", pa.int32())] + [(c, pa.string()) for c in fields])
    data = {"row_ord": list(range(len(rows)))}
    data.update({c: [r.get(c) for r in rows] for c in fields})
    tbl = pa.table(data, schema=schema)
    lance.write_dataset(tbl, URI, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(URI, storage_options=so)
    ds.create_scalar_index("row_ord", index_type="BTREE", replace=True); log("  BTREE ✓ row_ord")
    log(f"DONE → {URI} rows={tbl.num_rows}")
    return {"uri": URI, "rows": tbl.num_rows}


def verify():
    import lance
    ds = lance.dataset(URI, storage_options=_r2_so())
    t = ds.scanner(columns=["page_number", "text_char_len"]).to_table().to_pylist()
    out = {"uri": URI, "rows": ds.count_rows(), "cols": len([f.name for f in ds.schema]),
           "pages": [(r["page_number"], r["text_char_len"]) for r in t],
           "head": (ds.scanner(columns=["text"], filter="row_ord = 0").to_table().column("text").to_pylist()[0] or "")[:240]}
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
