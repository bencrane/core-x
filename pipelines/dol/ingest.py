"""DOL SCA Directory of Occupations ingest — landing PDF → Lance system of record (Gen-3).

LOCAL in-session compute worker (deliberately NOT Modal-wrapped). A single 139-page,
~1.4 MiB PDF — squarely in-core. pypdfium2 (the fleet PDF text engine, per
pipelines/sam_gov/sam_attachment_extract.py) extracts page text; pyarrow builds the
sinks directly. Re-runnable on every SCA re-issue (``mode="overwrite"`` — full replace).

    doppler run -p core-x -c prd -- uv run \\
        --with pypdfium2 --with pylance --with pyarrow --with boto3 --with 'psycopg[binary]' \\
        python -m pipelines.dol.ingest --init-state          # apply ops DDL (once)
    doppler run -p core-x -c prd -- uv run \\
        --with pypdfium2 --with pylance --with pyarrow --with boto3 --with 'psycopg[binary]' \\
        python -m pipelines.dol.ingest --all                 # all three sinks

TOPOLOGY
    The operator drops the SCA Directory export into s3://data-sink/landing/dol/. Raw is
    transport-only. One document fans out to three joinable sinks:

        blob    1 × 9    -> active/dol_sca_directory_occupations_blob/   sha256 (the raw PDF bytes)
        pages 139 × 7    -> active/dol_sca_directory_occupations/        page_no  (verbatim per-page text)
        occs  ~502 × 13  -> active/dol_sca_occupations/                  occupation_code + family_code

    The blob + per-page text are the LOSSLESS record (nothing interpreted). The structured
    occupations table is a derived, search-friendly projection of the SCA code→title→
    definition entries — the grain that joins to SCA wage determinations.

FIDELITY & COVERAGE (no data left behind)
    Page text is stored verbatim (CRLF→LF only; page-number footers retained as page
    content). The structured parse keys on the SCA body format — a 5-digit code + TITLE on
    its own line, definition running until the next code line. Definitions are whitespace-
    collapsed and page-number footers stripped for the derived table only. Coverage is
    ASSERTED at ingest: every parsed code is unique, no title is empty, every occupation's
    family (code[:2]+'000') resolves to a family header, and the entry count sits in a sane
    band — an under-parse (e.g. a re-drop in a changed layout) fails the load loudly rather
    than silently dropping entries. Provenance (source_file, doc_sha256, edition,
    ingested_at) is carried on every sink.

CONTROL PLANE
    Terminal state per sink is written to ops.dol_runs (psycopg, best-effort — an audit
    write failure never masks an otherwise-good load).
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os
import os.path
import re
import sys
import tempfile

BUCKET = "data-sink"
SCRATCH_DIR = tempfile.gettempdir()

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3

LANDING_PREFIX = "landing/dol/"
SOURCE_MATCH = r"^SCA Directory of Occupations.*\.pdf$"
DOC_TITLE = "SCA Directory of Occupations"
EDITION = "Fifth Edition"

BLOB_URI = os.environ.get("DOL_SCA_BLOB_LANCE_URI", f"s3://{BUCKET}/active/dol_sca_directory_occupations_blob/")
PAGES_URI = os.environ.get("DOL_SCA_PAGES_LANCE_URI", f"s3://{BUCKET}/active/dol_sca_directory_occupations/")
OCCS_URI = os.environ.get("DOL_SCA_OCCS_LANCE_URI", f"s3://{BUCKET}/active/dol_sca_occupations/")

# SCA body: a 5-digit code + TITLE alone on a line; definition runs to the next code line.
CODE_RE = re.compile(r"(?m)^[ \t]*(\d{5})[ \t]+(\S.*?)[ \t]*$")
# Sane band for the 5th-edition entry count (25 family headers + ~477 occupations = 502).
# A re-drop that parses far outside this band signals a layout change — fail, don't guess.
MIN_ENTRIES, MAX_ENTRIES = 400, 700


# ── R2 / object-store plumbing ─────────────────────────────────────────────────────
def _storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID (Doppler core-x/prd).")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _s3_client():
    import boto3
    from botocore.config import Config

    so = _storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client("s3", endpoint_url=so["endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"],
                        region_name="auto", config=cfg)


def _discover_key(s3, prefix: str, match: str) -> str:
    rx = re.compile(match, re.I)
    found: list[tuple[str, int, dt.datetime]] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            base = o["Key"].rsplit("/", 1)[-1]
            if rx.match(base):
                found.append((o["Key"], o["Size"], o["LastModified"]))
    if not found:
        raise RuntimeError(f"No landing object matches /{match}/ under s3://{BUCKET}/{prefix}")
    found.sort(key=lambda t: (t[2], t[1]), reverse=True)
    return found[0][0]


# ── PDF text extraction (pypdfium2 — fleet convention) ─────────────────────────────
def _extract_pages(pdf_path: str) -> list[str]:
    import pypdfium2 as pdfium

    doc = pdfium.PdfDocument(pdf_path)
    try:
        out: list[str] = []
        for i in range(len(doc)):
            page = doc[i]
            tp = page.get_textpage()
            out.append((tp.get_text_range() or "").replace("\r\n", "\n").replace("\r", "\n"))
        return out
    finally:
        doc.close()


def _strip_footer(page_lf: str) -> str:
    """Drop a trailing bare page-number line (the only per-page footer in this doc)."""
    lines = page_lf.split("\n")
    while lines and not lines[-1].strip():
        lines.pop()
    if lines and re.fullmatch(r"\d{1,3}", lines[-1].strip()):
        lines.pop()
    return "\n".join(lines)


# ── SCA structured parse (code → title → definition) ───────────────────────────────
def _parse_occupations(pages_lf: list[str]) -> tuple[list[dict], dict]:
    """Parse SCA entries from footer-stripped page text. Returns (rows, coverage-stats)."""
    stripped = [_strip_footer(p) for p in pages_lf]
    # char offset of each page's start within the '\n'-joined body → page-of(idx)
    offsets: list[tuple[int, int]] = []
    pos = 0
    for p in stripped:
        offsets.append((pos, pos + len(p)))
        pos += len(p) + 1  # + the joining '\n'
    body = "\n".join(stripped)

    def page_of(idx: int) -> int:
        for pn, (s, e) in enumerate(offsets, 1):
            if s <= idx <= e:
                return pn
        return len(stripped)

    matches = list(CODE_RE.finditer(body))
    rows: list[dict] = []
    captured = 0
    for i, m in enumerate(matches):
        code, title = m.group(1), m.group(2).strip()
        seg_start = m.end()
        seg_end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        raw_def = body[seg_start:seg_end]
        captured += len(title) + len(raw_def)
        definition = re.sub(r"\s+", " ", raw_def).strip()
        family_code = code[:2] + "000"
        entry_type = ("family" if code.endswith("000")
                      else "occupational_base" if "(Occupational Base)" in title
                      else "occupation")
        rows.append({
            "occupation_code": code,
            "occupation_title": title,
            "occupation_definition": definition or None,
            "family_code": family_code,
            "entry_type": entry_type,
            "page_start": page_of(m.start()),
            "title_char_len": len(title),
            "definition_char_len": len(definition),
        })

    fam_title = {r["occupation_code"]: r["occupation_title"]
                 for r in rows if r["entry_type"] == "family"}
    orphans = set()
    for r in rows:
        r["family_title"] = fam_title.get(r["family_code"])
        if r["family_title"] is None:
            orphans.add(r["family_code"])

    codes = [r["occupation_code"] for r in rows]
    stats = {
        "entries": len(rows),
        "distinct_codes": len(set(codes)),
        "families": len(fam_title),
        "orphan_family_codes": sorted(orphans),
        "empty_titles": sum(1 for r in rows if not r["occupation_title"]),
        "captured_chars": captured,
        "body_chars": len(body),
        "coverage_pct": round(100.0 * captured / max(len(body), 1), 2),
    }
    return rows, stats


def _assert_coverage(stats: dict) -> None:
    problems = []
    if not (MIN_ENTRIES <= stats["entries"] <= MAX_ENTRIES):
        problems.append(f"entry count {stats['entries']} outside [{MIN_ENTRIES},{MAX_ENTRIES}] — layout drift?")
    if stats["distinct_codes"] != stats["entries"]:
        problems.append(f"duplicate codes: {stats['entries']} rows / {stats['distinct_codes']} distinct")
    if stats["empty_titles"]:
        problems.append(f"{stats['empty_titles']} empty titles")
    if stats["orphan_family_codes"]:
        problems.append(f"unresolved family codes: {stats['orphan_family_codes']}")
    if problems:
        raise RuntimeError("SCA parse coverage check failed — " + "; ".join(problems))


# ── Arrow builders ─────────────────────────────────────────────────────────────────
def _blob_table(source_file, pdf_bytes, sha256, page_count, now):
    import pyarrow as pa

    schema = pa.schema([
        ("source_file", pa.string()), ("content_type", pa.string()),
        ("byte_len", pa.int64()), ("sha256", pa.string()), ("page_count", pa.int32()),
        ("doc_title", pa.string()), ("edition", pa.string()),
        ("pdf_bytes", pa.large_binary()), ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])
    return pa.table({
        "source_file": [source_file], "content_type": ["application/pdf"],
        "byte_len": [len(pdf_bytes)], "sha256": [sha256], "page_count": [page_count],
        "doc_title": [DOC_TITLE], "edition": [EDITION],
        "pdf_bytes": [pdf_bytes], "ingested_at": [now],
    }, schema=schema)


def _pages_table(source_file, sha256, pages_lf, now):
    import pyarrow as pa

    n = len(pages_lf)
    schema = pa.schema([
        ("source_file", pa.string()), ("doc_sha256", pa.string()), ("page_count", pa.int32()),
        ("page_no", pa.int32()), ("page_text", pa.string()), ("page_char_len", pa.int32()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])
    return pa.table({
        "source_file": [source_file] * n, "doc_sha256": [sha256] * n, "page_count": [n] * n,
        "page_no": list(range(1, n + 1)),
        "page_text": [p or None for p in pages_lf],
        "page_char_len": [len(p) for p in pages_lf],
        "ingested_at": [now] * n,
    }, schema=schema)


def _occs_table(source_file, sha256, rows, now):
    import pyarrow as pa

    n = len(rows)
    schema = pa.schema([
        ("occupation_code", pa.string()), ("occupation_title", pa.string()),
        ("occupation_definition", pa.string()), ("family_code", pa.string()),
        ("family_title", pa.string()), ("entry_type", pa.string()),
        ("page_start", pa.int32()), ("title_char_len", pa.int32()),
        ("definition_char_len", pa.int32()), ("source_file", pa.string()),
        ("doc_sha256", pa.string()), ("edition", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])
    col = lambda k: [r[k] for r in rows]  # noqa: E731
    return pa.table({
        "occupation_code": col("occupation_code"), "occupation_title": col("occupation_title"),
        "occupation_definition": col("occupation_definition"), "family_code": col("family_code"),
        "family_title": col("family_title"), "entry_type": col("entry_type"),
        "page_start": col("page_start"), "title_char_len": col("title_char_len"),
        "definition_char_len": col("definition_char_len"),
        "source_file": [source_file] * n, "doc_sha256": [sha256] * n, "edition": [EDITION] * n,
        "ingested_at": [now] * n,
    }, schema=schema)


# ── Lance write + scalar indexes ───────────────────────────────────────────────────
def _write(table, uri, so):
    import lance

    lance.write_dataset(table, uri, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                        storage_options=so)


def _build_indexes(uri, btree, bitmap, so) -> list[str]:
    import lance

    ds = lance.dataset(uri, storage_options=so)
    present = {f.name for f in ds.schema}
    built: list[str] = []
    for kind, cols in (("BTREE", btree), ("BITMAP", bitmap)):
        for col in cols:
            if col not in present:
                print(f"  WARN {kind} {col}: column absent — skipped")
                continue
            try:
                ds.create_scalar_index(col, index_type=kind, replace=True)
                built.append(f"{kind}:{col}")
                print(f"  {kind:6s} ✓ {col}")
            except Exception as exc:  # noqa: BLE001
                print(f"  WARN {kind} {col} failed: {exc}")
    return built


# ── ops.dol_runs ledger ────────────────────────────────────────────────────────────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.dol_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset        text        NOT NULL,
    dataset_uri    text,
    source_file    text,
    doc_sha256     text,
    rows_processed bigint,
    indexes_built  text[],
    coverage       jsonb,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dol_runs_dataset_idx     ON ops.dol_runs (dataset);
CREATE INDEX IF NOT EXISTS dol_runs_status_idx      ON ops.dol_runs (status);
CREATE INDEX IF NOT EXISTS dol_runs_recorded_at_idx ON ops.dol_runs (recorded_at DESC);
"""


def _record_run(dataset, uri, source_file, sha256, rows, built, coverage, status,
                error, started_at, completed_at) -> None:
    import json

    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.dol_runs write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.dol_runs
                    (dataset, dataset_uri, source_file, doc_sha256, rows_processed,
                     indexes_built, coverage, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (dataset, uri, source_file, sha256, rows, built,
                 json.dumps(coverage) if coverage else None, status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.dol_runs write failed: {exc}")


def apply_state_schema() -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set (Doppler core-x/prd).")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.dol_runs schema.")


# ── orchestration: landing PDF → three sinks ───────────────────────────────────────
def ingest(keep_temp: bool = False) -> list[dict]:
    so = _storage_options()
    s3 = _s3_client()
    now = dt.datetime.now(dt.timezone.utc)

    key = _discover_key(s3, LANDING_PREFIX, SOURCE_MATCH)
    source_file = key.rsplit("/", 1)[-1]
    pdf_path = os.path.join(SCRATCH_DIR, f"dol_{os.path.basename(key)}")
    print(f"landing s3://{BUCKET}/{key} -> {pdf_path}")
    s3.download_file(BUCKET, key, pdf_path)

    try:
        pdf_bytes = open(pdf_path, "rb").read()
        sha256 = hashlib.sha256(pdf_bytes).hexdigest()
        pages_lf = _extract_pages(pdf_path)
        page_count = len(pages_lf)
        rows, stats = _parse_occupations(pages_lf)
        print(f"extracted {page_count} pages; parsed {stats['entries']} SCA entries "
              f"({stats['families']} families, coverage {stats['coverage_pct']}%)")
        _assert_coverage(stats)

        sinks = [
            ("dol_sca_directory_occupations_blob", BLOB_URI,
             _blob_table(source_file, pdf_bytes, sha256, page_count, now),
             ["sha256"], []),
            ("dol_sca_directory_occupations", PAGES_URI,
             _pages_table(source_file, sha256, pages_lf, now),
             ["page_no"], []),
            ("dol_sca_occupations", OCCS_URI,
             _occs_table(source_file, sha256, rows, now),
             ["occupation_code", "family_code"], ["entry_type"]),
        ]

        results = []
        for name, uri, table, btree, bitmap in sinks:
            started = dt.datetime.now(dt.timezone.utc)
            status, error, built = "error", None, []
            try:
                _write(table, uri, so)
                print(f"[{name}] wrote {table.num_rows} rows -> {uri}")
                built = _build_indexes(uri, btree, bitmap, so)
                status = "success"
            except Exception as exc:  # noqa: BLE001
                error = str(exc)
                raise
            finally:
                _record_run(name, uri, source_file, sha256, table.num_rows, built,
                            stats if name == "dol_sca_occupations" else None,
                            status, error, started, dt.datetime.now(dt.timezone.utc))
            results.append({"dataset": name, "rows": table.num_rows, "indexes": built, "uri": uri})
        return results
    finally:
        if not keep_temp:
            try:
                os.remove(pdf_path)
            except OSError:
                pass


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="DOL SCA Directory of Occupations PDF → Lance ingest.")
    ap.add_argument("--all", action="store_true", help="build all three sinks")
    ap.add_argument("--init-state", action="store_true", help="apply ops.dol_runs DDL and exit")
    ap.add_argument("--keep-temp", action="store_true", help="keep downloaded PDF in scratch")
    args = ap.parse_args(argv)

    if args.init_state:
        apply_state_schema()
        return 0
    if not args.all:
        ap.error("specify --all (or --init-state)")

    results = ingest(keep_temp=args.keep_temp)
    print("\n=== DOL SCA ingest summary ===")
    for r in results:
        print(f"  {r['dataset']:34s} rows={r['rows']:>5,}  idx={len(r['indexes']):>2}  -> {r['uri']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
