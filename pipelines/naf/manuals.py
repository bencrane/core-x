"""Phase 4 sidecar — naf_manual_docs: OPM NAF Operating Manual reference documents.

Fetches the OPM Federal Wage System — Nonappropriated-Fund Operating Manual PDFs (the POLICY /
definitions layer: NA/NL/NS job-grading, schedule layout, wage-area geography, agency special
schedules) from opm.gov → R2 landing + a Lance reference dataset carrying per-document metadata and
extracted text. Validator-role sidecar (like dol_sca_occupations): OPM sets NAF policy; DoD DCPAS
publishes the priced schedules that naf_wage_rates parses. This lands the authoritative definitions
that ground the parse and the wage-area geography.

  source   https://www.opm.gov/.../nonappropriated-fund-operating-manual/{doc}
  landing  s3://data-sink/landing/naf/manuals/{doc}    raw PDF bytes (transport/lossless copy)
  table    s3://data-sink/active/naf_manual_docs/       1 row/doc: doc_name, category, source_url,
                                                        r2_key, byte_len, sha256, page_count, text, ...

Idempotent (mode='overwrite'). Reuses pipelines.bls.ingest R2 client + index builder; pypdfium2 for
text (fleet convention, per pipelines/dol/ingest.py); ops.naf_runs ledger.

CLI
  doppler run -p core-x -c prd -- uv run \
    --with requests --with pypdfium2 --with pylance --with pyarrow --with boto3 --with 'psycopg[binary]' \
    python3 -m pipelines.naf.manuals build
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os
import sys
import tempfile

import requests

from pipelines.bls.ingest import _build_indexes, _s3_client, _storage_options
from pipelines.naf.census import _record_run

BASE_URL = ("https://www.opm.gov/policy-data-oversight/pay-leave/pay-systems/federal-wage-system/"
            "nonappropriated-fund-operating-manual/")
BUCKET = "data-sink"
LANDING_PREFIX = "landing/naf/manuals/"
TARGET_URI = os.environ.get("NAF_MANUAL_DOCS_URI", f"s3://{BUCKET}/active/naf_manual_docs/")
DATA_STORAGE_VERSION = "2.1"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

# The 22 live manual PDFs (recon-confirmed 2026-07-03; all HTTP 200 application/pdf). appendixv is
# published with an UPPERCASE '.PDF' extension — the object storage key is case-sensitive.
DOCS = (
    [f"subchapter{n}.pdf" for n in (1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12)]
    + [f"appendix{a}.pdf" for a in ("c", "d", "e", "f", "g", "h", "i", "j", "m", "t")]
    + ["appendixv.PDF"]
)
EXPECTED_DOCS = len(DOCS)  # 22


def _category(name: str) -> str:
    return "subchapter" if name.lower().startswith("subchapter") else "appendix"


def _extract_text(pdf_path: str) -> tuple[int, str]:
    """(page_count, full text) via pypdfium2; pages joined by form-feed to preserve boundaries."""
    import pypdfium2 as pdfium

    doc = pdfium.PdfDocument(pdf_path)
    try:
        pages = []
        for i in range(len(doc)):
            tp = doc[i].get_textpage()
            pages.append((tp.get_text_range() or "").replace("\r\n", "\n").replace("\r", "\n"))
        return len(doc), "\n\f\n".join(pages)
    finally:
        doc.close()


def build() -> None:
    import lance
    import pyarrow as pa

    so = _storage_options()
    s3 = _s3_client()
    now = dt.datetime.now(dt.timezone.utc)
    sess = requests.Session()
    sess.headers.update({"User-Agent": UA})

    rows = []
    for name in DOCS:
        url = BASE_URL + name
        r = sess.get(url, timeout=60)
        if r.status_code != 200 or r.content[:4] != b"%PDF":
            raise RuntimeError(f"fetch failed {name}: status={r.status_code} magic={r.content[:8]!r}")
        body = r.content
        key = LANDING_PREFIX + name
        s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/pdf")
        tmp = os.path.join(tempfile.gettempdir(), f"naf_manual_{name.replace('/', '_')}")
        with open(tmp, "wb") as f:
            f.write(body)
        try:
            page_count, text = _extract_text(tmp)
        finally:
            try:
                os.remove(tmp)
            except OSError:
                pass
        rows.append({
            "doc_name": name, "category": _category(name), "source_url": url, "r2_key": key,
            "byte_len": len(body), "sha256": hashlib.sha256(body).hexdigest(),
            "page_count": page_count, "text": text or None, "text_char_len": len(text),
            "fetched_at": now,
        })
        print(f"  [{name:16}] {len(body):>7,}B {page_count:>2}p text={len(text):>7,}chars -> {key}")

    # Fail-closed coverage: all expected docs present, every doc has extractable text.
    if len(rows) != EXPECTED_DOCS:
        raise RuntimeError(f"GATE FAIL: fetched {len(rows)} != expected {EXPECTED_DOCS}")
    empty = [r["doc_name"] for r in rows if not r["text"] or r["page_count"] == 0]
    if empty:
        raise RuntimeError(f"GATE FAIL: docs with no extractable text: {empty}")

    schema = pa.schema([
        ("doc_name", pa.string()), ("category", pa.string()), ("source_url", pa.string()),
        ("r2_key", pa.string()), ("byte_len", pa.int64()), ("sha256", pa.string()),
        ("page_count", pa.int32()), ("text", pa.string()), ("text_char_len", pa.int64()),
        ("fetched_at", pa.timestamp("us", tz="UTC")),
    ])
    tbl = pa.Table.from_pylist(rows, schema=schema)
    lance.write_dataset(tbl, TARGET_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    built = _build_indexes(TARGET_URI, ["doc_name"], ["category"], so)
    written = lance.dataset(TARGET_URI, storage_options=so).count_rows()
    if written != len(rows):
        raise RuntimeError(f"POST-WRITE FAIL: lance {written} != {len(rows)}")
    _record_run("naf_manual_docs", TARGET_URI, BASE_URL, None, len(rows), 0, built,
                "success", None, now, dt.datetime.now(dt.timezone.utc))

    sub = sum(1 for r in rows if r["category"] == "subchapter")
    print(f"\n[naf_manual_docs] {written} docs -> {TARGET_URI}  indexes={built}")
    print(f"  subchapters={sub} appendices={len(rows)-sub} total_bytes={sum(r['byte_len'] for r in rows):,}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="OPM NAF Operating Manual → naf_manual_docs (Phase 4).")
    ap.add_argument("cmd", nargs="?", default="build", choices=["build"])
    ap.parse_args(argv)
    build()
    return 0


if __name__ == "__main__":
    sys.exit(main())
