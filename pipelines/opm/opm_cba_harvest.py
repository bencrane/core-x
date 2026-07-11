"""OPM CBA Database harvest — federal-sector agency ⇄ union contracts.

EO-13836 federal-employee CBAs: 1,248 documents across 83 agencies.
NAF (exchanges/MWR/commissaries) subset carries wage appendices for govcon labor substrates.

STAGE 1 — INDEX (--index): load pre-fetched catalog JSON → Lance index table
  Input: raw/opm_cba_catalog.json (1,248-row JSON array, pre-fetched via browser)
  Reconcile: count == 1,248; fail-closed on mismatch.
  Output: s3://data-sink/active/opm_cba_index/ (Lance)

STAGE 2 — DOCUMENTS (--documents): fetch each fileUrl -> R2 blobs
  Per-record: GET {fileUrl} → s3://data-sink/active/opm_cba_blobs/{id}.pdf
  Resume-safe: --resume skips already-fetched ids.
  Output: s3://data-sink/active/opm_cba_documents/ (Lance manifest)
           s3://data-sink/active/opm_cba_blobs/{id}.{ext} (R2 raw PDFs)

STAGE 3 — NAF SLICE ANALYSIS (--analyze-naf):
  Filter index on: agency/subAgency/fileName regex (exchange|MWR|morale|nonappropriated|commissary)
  Output: NAF subset count + list of NAF doc IDs (informs extraction scope/priority)

Usage:
  doppler run --project core-x --config prd -- \
    uv run --with pylance --with pyarrow --with requests --with boto3 \
    python pipelines/opm/opm_cba_harvest.py --index
  ... python pipelines/opm/opm_cba_harvest.py --documents --resume
  ... python pipelines/opm/opm_cba_harvest.py --analyze-naf
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import time
from pathlib import Path

try:
    import truststore
    truststore.inject_into_ssl()
    import warnings as _warnings
    from urllib3.exceptions import InsecureRequestWarning as _IRW
    _warnings.filterwarnings("ignore", category=_IRW)
except Exception:
    pass

from pipelines.bls.ingest import (
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _s3_client,
    _storage_options,
)

import pyarrow as pa
import pyarrow.parquet as pq
from lancedb.db import DBConnection

BUCKET = "data-sink"
INDEX_URI = os.environ.get("OPM_CBA_INDEX_URI", f"s3://{BUCKET}/active/opm_cba_index/")
DOCS_URI = os.environ.get("OPM_CBA_DOCS_URI", f"s3://{BUCKET}/active/opm_cba_documents/")
BLOB_PREFIX = os.environ.get("OPM_CBA_BLOB_PREFIX", "active/opm_cba_blobs/")

def load_catalog(catalog_json_path: str) -> list[dict]:
    """Load raw OPM CBA catalog JSON (pre-fetched via browser)."""
    with open(catalog_json_path) as f:
        catalog = json.load(f)

    if not isinstance(catalog, list):
        raise ValueError(f"Expected list, got {type(catalog).__name__}")

    expected_count = 1248
    if len(catalog) != expected_count:
        raise ValueError(
            f"Catalog record count mismatch: got {len(catalog)}, expected {expected_count}. "
            "Fail-closed."
        )

    print(f"✓ Loaded {len(catalog)} records from {catalog_json_path}")
    return catalog

def stage_index(catalog: list[dict]) -> None:
    """Write catalog to Lance index table."""
    # Normalize schema: extract required fields
    records = [
        {
            "id": doc.get("id"),
            "agency_name": doc.get("agencyName"),
            "sub_agency": doc.get("subAgencyOrComponent"),
            "labor_union_name": doc.get("laborUnionName"),
            "activity_office_region": doc.get("activityOfficeRegion"),
            "expiration_date": doc.get("expirationDate"),
            "file_url": doc.get("fileUrl"),
            "file_name": doc.get("fileName"),
            "file_size": doc.get("fileSize"),
        }
        for doc in catalog
    ]

    table = pa.Table.from_pylist(records)

    # Write to Lance with indexes
    db = _build_indexes(
        table,
        uri=INDEX_URI,
        index_cols=["id", "agency_name", "labor_union_name"],
        index_types={"id": "BTREE", "agency_name": "BTREE", "labor_union_name": "BITMAP"},
    )
    print(f"✓ Wrote {len(records)} records to {INDEX_URI}")

def stage_documents(catalog: list[dict], resume: bool = False, limit: int | None = None) -> None:
    """Fetch PDFs and write manifest to Lance."""
    s3 = _s3_client()
    docs = []

    count = 0
    for i, doc in enumerate(catalog):
        if limit and count >= limit:
            print(f"⊘ Stopped at --limit {limit}")
            break

        doc_id = doc.get("id")
        file_url = doc.get("fileUrl")
        file_name = doc.get("fileName", "unknown")

        if not file_url:
            print(f"[{i+1}] SKIP: no fileUrl for {doc_id}")
            continue

        # Check if already fetched (resume)
        ext = file_name.split(".")[-1] if "." in file_name else "pdf"
        blob_key = f"{BLOB_PREFIX}{doc_id}.{ext}"

        if resume:
            try:
                s3.head_object(Bucket=BUCKET, Key=blob_key)
                print(f"[{i+1}] SKIP: {doc_id} already fetched")
                docs.append({
                    "id": doc_id,
                    "r2_key": blob_key,
                    "file_name": file_name,
                    "content_type": "application/pdf",  # placeholder
                    "byte_len": 0,  # fetched, but we'll overwrite on re-read
                    "sha256": "",
                    "fetch_status": "success",
                })
                continue
            except s3.exceptions.NoSuchKey:
                pass

        # Fetch PDF
        try:
            print(f"[{i+1}] Fetching {doc_id} from {file_url}...")
            import requests
            resp = requests.get(file_url, timeout=30)
            resp.raise_for_status()

            blob_bytes = resp.content
            sha256_hash = hashlib.sha256(blob_bytes).hexdigest()

            # Write to R2
            s3.put_object(
                Bucket=BUCKET,
                Key=blob_key,
                Body=blob_bytes,
                ContentType=resp.headers.get("Content-Type", "application/octet-stream"),
                Metadata={"sha256": sha256_hash},
            )

            docs.append({
                "id": doc_id,
                "r2_key": blob_key,
                "file_name": file_name,
                "content_type": resp.headers.get("Content-Type", "application/octet-stream"),
                "byte_len": len(blob_bytes),
                "sha256": sha256_hash,
                "fetch_status": "success",
            })

            count += 1
            print(f"  ✓ {len(blob_bytes):,} bytes → {blob_key}")

            # Be polite
            time.sleep(0.5)

        except Exception as e:
            print(f"  ✗ {doc_id}: {e}")
            docs.append({
                "id": doc_id,
                "r2_key": "",
                "file_name": file_name,
                "content_type": "",
                "byte_len": 0,
                "sha256": "",
                "fetch_status": str(e)[:100],
            })

    # Write manifest to Lance
    if docs:
        table = pa.Table.from_pylist(docs)
        _build_indexes(
            table,
            uri=DOCS_URI,
            index_cols=["id", "fetch_status"],
            index_types={"id": "BTREE", "fetch_status": "BITMAP"},
        )
        print(f"✓ Wrote {len(docs)} doc manifest rows to {DOCS_URI}")

def analyze_naf(catalog: list[dict]) -> None:
    """Identify NAF slice (exchange/MWR/commissary/etc) and score."""
    naf_keywords = {
        "exchange", "mwr", "morale", "nonappropriated", "commissary",
        "billeting", "club", "afees"
    }

    naf_docs = []
    for doc in catalog:
        text_parts = [
            (doc.get("fileName") or "").lower(),
            (doc.get("agencyName") or "").lower(),
            (doc.get("subAgencyOrComponent") or "").lower(),
            (doc.get("activityOfficeRegion") or "").lower(),
        ]
        full_text = " ".join(text_parts)

        if any(kw in full_text for kw in naf_keywords):
            naf_docs.append({
                "id": doc.get("id"),
                "agency_name": doc.get("agencyName"),
                "file_name": doc.get("fileName"),
                "matched_keywords": [kw for kw in naf_keywords if kw in full_text],
            })

    print(f"\n✓ NAF Slice Analysis:")
    print(f"  Total records: {len(catalog)}")
    print(f"  NAF-flavored: {len(naf_docs)}")
    print(f"  Ratio: {len(naf_docs)/len(catalog)*100:.1f}%")

    if naf_docs:
        print(f"\n  Sample NAF docs:")
        for doc in naf_docs[:5]:
            print(f"    - {doc['id']}: {doc['agency_name']} | {doc['file_name']}")

    return len(naf_docs)

def main():
    parser = argparse.ArgumentParser(description="OPM CBA harvest pipeline")
    parser.add_argument("--index", action="store_true", help="Stage 1: load catalog → Lance index")
    parser.add_argument("--documents", action="store_true", help="Stage 2: fetch PDFs → R2 + manifest")
    parser.add_argument("--analyze-naf", action="store_true", help="Stage 3: NAF slice analysis")
    parser.add_argument("--resume", action="store_true", help="Resume fetch; skip already-fetched")
    parser.add_argument("--limit", type=int, default=None, help="Limit doc fetch count (for testing)")
    parser.add_argument("--catalog", default="raw/opm_cba_catalog.json", help="Path to catalog JSON")

    args = parser.parse_args()

    if not (args.index or args.documents or args.analyze_naf):
        parser.print_help()
        return

    # Load catalog once
    catalog = load_catalog(args.catalog)

    if args.index:
        stage_index(catalog)

    if args.documents:
        stage_documents(catalog, resume=args.resume, limit=args.limit)

    if args.analyze_naf:
        analyze_naf(catalog)

if __name__ == "__main__":
    main()
