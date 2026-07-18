#!/usr/bin/env python3
"""Zero-ingest layout + schema-drift recon of the CMS Medicare ZIP archive on R2.

Read-only. Maps the physical layout, data grain, and year-over-year schema drift
of every ZIP under ``s3://data-sink/landing/cms/medicare-datasets/`` WITHOUT
extracting or fully downloading a single object.

Anti-OOM protocol (strict):
  * Each ZIP's central directory lives at the TAIL of the object; we read it via a
    handful of HTTP Range GETs through ``_S3RangeReader`` (a seekable file-like over
    boto3 ``get_object(Range=…)``) handed to stdlib ``zipfile`` — so member listing
    costs ~tens of KB, never the 0.5–1.6 GB object.
  * Each data member is peeked by STREAM-decompressing only its leading deflate
    blocks: we read until ~50 rows or PEEK_MAX_BYTES (1 MiB) of decompressed bytes,
    whichever first, then stop — the rest of the member is never pulled.
  * No pandas, no full extraction, no Lance/DuckDB. boto3 + stdlib only.

Per ZIP it emits: member inventory (name / uncompressed / compressed / class),
the primary payload's delimiter+encoding, header, per-column inferred type, the
NPI hub column + its 10-digit validity, summary/aggregation-row suspects, and an
empirical grain read (distinct NPI vs sampled rows, presence of a code axis).
Bundled multi-year archives are peeked PER data member so cross-year drift is
captured. JSON array → stdout; human progress → stderr.

    doppler run -p core-x -c prd -- \
      python3 scripts/archive/recon_medicare_archive.py > /tmp/medicare_recon.json

Optional argv[1]: case-insensitive substring filter on the object key (re-run a
single family without re-walking the whole archive).

Required env (Doppler core-x/prd): R2_ENDPOINT (or R2_ACCOUNT_ID),
R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY.
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
import sys
import zipfile

import boto3
from botocore.config import Config

BUCKET = "data-sink"
PREFIX = "landing/cms/medicare-datasets/"
PEEK_MAX_ROWS = 50
PEEK_MAX_BYTES = 1 << 20            # 1 MiB decompressed cap per member
PEEK_DATA_MEMBERS_PER_ZIP = 30     # cap for bundled multi-year archives
csv.field_size_limit(1 << 24)

# ── member classification ──────────────────────────────────────────────────
DATA_EXT = (".csv", ".txt", ".tsv", ".dat")
DOC_EXT = (".pdf", ".xlsx", ".xls", ".doc", ".docx", ".json", ".xml", ".rtf", ".html", ".htm")
DOC_NAME_RE = re.compile(
    r"(dictionar|method|readme|glossar|codebook|to_?from|layout|technical|"
    r"notes?|license|licence|faq|reference|description|appendix|crosswalk)", re.I)

# ── value-type inference ───────────────────────────────────────────────────
RE_NPI10 = re.compile(r"^\d{10}$")
RE_INT = re.compile(r"^-?\d+$")
RE_DEC = re.compile(r"^-?(?:\d+\.\d*|\.\d+|\d+)$")
RE_MONEY = re.compile(r"^-?\$?\d{1,3}(?:,\d{3})*(?:\.\d+)?$|^-?\$?\d+(?:\.\d+)?$")
RE_DATE_MDY = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")
RE_DATE_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")

NPI_NAME_RE = re.compile(r"(^|_)npi($|_)|national_provider", re.I)
CODE_AXIS_RE = re.compile(r"hcpcs|gnrc_name|brnd_name|generic_name|brand_name|drug_name", re.I)
GEO_AXIS_RE = re.compile(r"geo_lvl|geo_desc|geo_cd|rndrng_prvdr_geo|^state$|state_cd|fips", re.I)
SUMMARY_TOKEN_RE = re.compile(
    r"^\s*(total|national|all\s+providers?|all\s+suppliers?|summary|aggregate|"
    r"grand\s+total|nation(al)?)\b", re.I)


def storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def s3_client():
    o = storage_options()
    # botocore >=1.36 validates a full-object checksum on GetObject; on a RANGE GET it
    # compares partial bytes against the whole-object header → spurious mismatch. Scope
    # checksum validation to "when_required" so central-directory + member range reads pass.
    cfg = Config(signature_version="s3v4",
                 request_checksum_calculation="when_required",
                 response_checksum_validation="when_required",
                 retries={"max_attempts": 5, "mode": "standard"})
    return boto3.client("s3", endpoint_url=o["endpoint"],
                        aws_access_key_id=o["aws_access_key_id"],
                        aws_secret_access_key=o["aws_secret_access_key"],
                        region_name="auto", config=cfg)


class _S3RangeReader:
    """Minimal seekable file-like over an R2 object via boto3 Range GETs — enough for
    zipfile to parse the central directory (a few tail reads) and stream a member."""

    def __init__(self, client, bucket: str, key: str):
        self.c, self.b, self.k = client, bucket, key
        self._pos = 0
        self._size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, off: int, whence: int = 0) -> int:
        self._pos = off if whence == 0 else (self._pos + off if whence == 1 else self._size + off)
        return self._pos

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            n = self._size - self._pos
        if n <= 0:
            return b""
        end = min(self._pos + n, self._size) - 1
        if end < self._pos:
            return b""
        body = self.c.get_object(Bucket=self.b, Key=self.k,
                                 Range=f"bytes={self._pos}-{end}")["Body"].read()
        self._pos += len(body)
        return body


# ── key → family / year ────────────────────────────────────────────────────
def parse_family(key: str) -> dict:
    name = key.rsplit("/", 1)[-1]
    m = re.match(r"^(\d{4})-(\d{4})-", name)
    if m:
        y0, y1 = m.group(1), m.group(2)
        year = y0 if y0 == y1 else f"{y0}-{y1}"
    else:
        year = None
    low = name.lower()
    if "physician & other practitioners" in low or "physician and other" in low:
        fam = "physician_other_practitioners"
    elif "part d prescribers" in low:
        fam = "part_d_prescribers"
    elif "durable medical equipment" in low:
        fam = "dme"
    elif "public provider enrollment" in low:
        fam = "provider_enrollment"
    elif "quality payment program" in low:
        fam = "qpp"
    elif "program statistics" in low:
        fam = "program_statistics"
    elif "aco reach" in low:
        fam = "aco_reach"
    elif "betos" in low:
        fam = "betos_ref"
    else:
        fam = "other"
    if "by provider and service" in low:
        sub = "by_provider_and_service"
    elif "by provider and drug" in low:
        sub = "by_provider_and_drug"
    elif "by geography and service" in low:
        sub = "by_geography_and_service"
    elif "by geography and drug" in low:
        sub = "by_geography_and_drug"
    elif "by supplier and service" in low:
        sub = "by_supplier_and_service"
    elif "by referring provider" in low:
        sub = "by_referring_provider"
    elif "by supplier" in low:
        sub = "by_supplier"
    elif "by provider" in low:
        sub = "by_provider"
    else:
        sub = None
    return {"family": fam, "subgrain": sub, "year": year}


def classify_member(name: str) -> str:
    base = name.rsplit("/", 1)[-1]
    low = base.lower()
    ext = "." + low.rsplit(".", 1)[-1] if "." in low else ""
    if ext in DOC_EXT:
        return "doc"
    if DOC_NAME_RE.search(low):
        return "dictionary"
    if ext in DATA_EXT:
        return "data"
    return "other"


# ── type inference ─────────────────────────────────────────────────────────
def infer_type(values: list[str]) -> str:
    vals = [v.strip() for v in values if v is not None and v.strip() != ""]
    if not vals:
        return "empty"
    if all(RE_NPI10.match(v) for v in vals):
        return "id10"
    if all(RE_INT.match(v) for v in vals):
        return "int"
    if all(RE_DEC.match(v) or RE_INT.match(v) for v in vals):
        return "decimal"
    if all(RE_MONEY.match(v) for v in vals) and any(("." in v or "," in v or "$" in v) for v in vals):
        return "money"
    if all(RE_DATE_MDY.match(v) for v in vals):
        return "date_mdy"
    if all(RE_DATE_ISO.match(v) for v in vals):
        return "date_iso"
    return "varchar"


def find_npi_col(header: list[str]) -> int | None:
    for i, h in enumerate(header):
        if NPI_NAME_RE.search(h or ""):
            return i
    return None


def peek_member(zf: zipfile.ZipFile, zi: zipfile.ZipInfo) -> dict:
    """Stream-decompress only the leading blocks of one member; parse ≤50 rows."""
    raw = bytearray()
    with zf.open(zi) as fh:
        while len(raw) < PEEK_MAX_BYTES and raw.count(b"\n") <= PEEK_MAX_ROWS + 5:
            chunk = fh.read(65536)
            if not chunk:
                break
            raw += chunk
    encoding = None
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = bytes(raw).decode(enc)
            encoding = enc
            break
        except UnicodeDecodeError:
            continue
    if encoding is None:
        text = bytes(raw).decode("latin-1", errors="replace")
        encoding = "latin-1/replace"

    first_line = text.split("\n", 1)[0]
    delim = max([",", "\t", "|", ";"], key=lambda d: first_line.count(d))
    delim_name = {",": "comma", "\t": "tab", "|": "pipe", ";": "semicolon"}[delim]

    rows = []
    for r in csv.reader(io.StringIO(text), delimiter=delim):
        rows.append(r)
        if len(rows) >= PEEK_MAX_ROWS + 1:
            break
    if not rows:
        return {"name": zi.filename, "error": "no-rows-decoded",
                "uncompressed": zi.file_size, "compressed": zi.compress_size}
    header = [h.strip().lstrip("﻿") for h in rows[0]]
    data = rows[1:]
    ncol = len(header)

    # per-column type + max length over sampled data
    cols = []
    for ci, hname in enumerate(header):
        col_vals = [r[ci] for r in data if ci < len(r)]
        nonblank = [v for v in col_vals if v is not None and v.strip() != ""]
        cols.append({
            "name": hname,
            "type": infer_type(col_vals),
            "max_len": max((len(v) for v in nonblank), default=0),
            "n_blank": len(col_vals) - len(nonblank),
            "samples": [v for v in nonblank[:3]],
        })

    # NPI hub column + 10-digit validity
    npi_idx = find_npi_col(header)
    npi_col = header[npi_idx] if npi_idx is not None else None
    npi_valid = None
    n_distinct_npi = None
    if npi_idx is not None:
        npi_vals = [r[npi_idx].strip() for r in data if npi_idx < len(r)]
        present = [v for v in npi_vals if v != ""]
        npi_valid = bool(present) and all(RE_NPI10.match(v) for v in present)
        n_distinct_npi = len(set(present))

    # summary / aggregation-row suspects
    suspects = []
    for ri, r in enumerate(data):
        reason = None
        npi_v = r[npi_idx].strip() if (npi_idx is not None and npi_idx < len(r)) else None
        if npi_idx is not None and (npi_v == "" or (npi_v and not RE_NPI10.match(npi_v))):
            reason = "npi blank/non-10-digit"
        else:
            joined = " | ".join(r[:3])
            if SUMMARY_TOKEN_RE.search(joined):
                reason = "summary token in leading cells"
        if reason:
            suspects.append({"row": ri, "reason": reason, "npi": npi_v,
                             "cells": [c[:40] for c in r[:4]]})
        if len(suspects) >= 5:
            break

    # grain evidence
    has_code = any(CODE_AXIS_RE.search(h) for h in header)
    code_cols = [h for h in header if CODE_AXIS_RE.search(h)]
    has_geo = any(GEO_AXIS_RE.search(h) for h in header)
    n_rows = len(data)
    if npi_idx is None and has_geo:
        grain = "geography aggregate (no NPI)"
    elif npi_idx is not None and has_code:
        repeats = (n_distinct_npi is not None and n_rows > 0 and n_distinct_npi < n_rows)
        grain = ("1 row = NPI × code [" + ", ".join(code_cols) + "]"
                 + (" — NPI repeats across sample" if repeats else " — code axis present"))
    elif npi_idx is not None and not has_code:
        oneone = (n_distinct_npi == n_rows) if (n_distinct_npi is not None) else None
        grain = ("1 row = NPI (provider-level aggregate)"
                 + (" — NPI 1:1 across sample" if oneone else ""))
    else:
        grain = "indeterminate from sample"

    return {
        "name": zi.filename,
        "uncompressed": zi.file_size,
        "compressed": zi.compress_size,
        "compress_type": "deflate" if zi.compress_type == zipfile.ZIP_DEFLATED else (
            "stored" if zi.compress_type == zipfile.ZIP_STORED else str(zi.compress_type)),
        "encoding": encoding,
        "delimiter": delim_name,
        "n_cols": ncol,
        "header": header,
        "columns": cols,
        "rows_sampled": n_rows,
        "npi_col": npi_col,
        "npi_all_valid_10digit": npi_valid,
        "n_distinct_npi_in_sample": n_distinct_npi,
        "code_axis_cols": code_cols,
        "summary_suspects": suspects,
        "grain": grain,
        "first_data_row": [c[:60] for c in data[0]] if data else [],
    }


def recon_zip(client, key: str) -> dict:
    meta = parse_family(key)
    rec = {"key": key, "name": key.rsplit("/", 1)[-1], **meta}
    reader = _S3RangeReader(client, BUCKET, key)
    rec["compressed_bytes"] = reader._size
    try:
        zf = zipfile.ZipFile(reader)
    except zipfile.BadZipFile as e:
        rec["error"] = f"bad-zip: {e}"
        return rec
    infos = [zi for zi in zf.infolist() if not zi.is_dir()]
    rec["zip64"] = any(zi.file_size >= (1 << 32) or zi.compress_size >= (1 << 32) for zi in infos)
    members = []
    for zi in infos:
        members.append({"name": zi.filename, "uncompressed": zi.file_size,
                        "compressed": zi.compress_size, "class": classify_member(zi.filename)})
    rec["members"] = members
    rec["uncompressed_total"] = sum(zi.file_size for zi in infos)
    rec["n_members"] = len(infos)

    data_infos = sorted([zi for zi in infos if classify_member(zi.filename) == "data"],
                        key=lambda z: z.file_size, reverse=True)
    rec["doc_files"] = [{"name": m["name"], "uncompressed": m["uncompressed"], "class": m["class"]}
                        for m in members if m["class"] in ("doc", "dictionary")]

    peeks = []
    for zi in data_infos[:PEEK_DATA_MEMBERS_PER_ZIP]:
        try:
            peeks.append(peek_member(zf, zi))
        except Exception as e:  # noqa: BLE001 — recon must not die on one member
            peeks.append({"name": zi.filename, "error": f"{type(e).__name__}: {str(e)[:160]}",
                          "uncompressed": zi.file_size, "compressed": zi.compress_size})
    rec["data_files"] = peeks
    if len(data_infos) > PEEK_DATA_MEMBERS_PER_ZIP:
        rec["data_files_truncated"] = len(data_infos) - PEEK_DATA_MEMBERS_PER_ZIP
    return rec


def main() -> int:
    filt = sys.argv[1].lower() if len(sys.argv) > 1 else None
    client = s3_client()
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        for o in page.get("Contents", []):
            k = o["Key"]
            if k.endswith("/"):
                continue
            keys.append((k, o["Size"]))
    keys.sort()
    results = []
    non_zip = []
    for k, sz in keys:
        rel = k.split(PREFIX, 1)[1]
        if filt and filt not in k.lower():
            continue
        if not k.lower().endswith(".zip"):
            non_zip.append({"key": k, "name": rel, "size": sz, **parse_family(k)})
            print(f"[skip non-zip] {rel} ({sz/1e6:.1f} MB)", file=sys.stderr, flush=True)
            continue
        try:
            rec = recon_zip(client, k)
        except Exception as e:  # noqa: BLE001
            rec = {"key": k, "name": rel, "error": f"{type(e).__name__}: {str(e)[:200]}"}
        results.append(rec)
        nprimary = rec.get("data_files", [{}])
        prim = nprimary[0] if nprimary else {}
        print(f"[ok] {rel:<70} members={rec.get('n_members')} "
              f"uncompressed={rec.get('uncompressed_total',0)/1e9:.2f}GB "
              f"grain={prim.get('grain','?')[:42]}", file=sys.stderr, flush=True)
    out = {"bucket": BUCKET, "prefix": PREFIX, "n_zips": len(results),
           "non_zip": non_zip, "zips": results}
    json.dump(out, sys.stdout, indent=2, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
