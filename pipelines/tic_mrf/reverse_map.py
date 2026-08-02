# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "ijson>=3.3",
#   "requests>=2.32",
#   "duckdb>=1.5,<2",
#   "pylance>=7",
#   "pyarrow>=17",
#   "boto3>=1.35",
# ]
# ///
"""TiC reverse-mapper — the canonical, out-of-core pipeline that turns payer
Transparency-in-Coverage (TiC) machine-readable files (MRFs) into a flat,
indexed, append-only Lance fact table of *negotiated commercial rates for a
target NPI cohort*.

WHY A REVERSE-MAPPER (the architecture the directive's shorthand inverts).
A TiC drop is two tiers of file:

  1. Table-of-Contents / index ("ToC")  — maps reporting_plans -> file URLs.
     It contains **NO provider NPIs.**  You cannot "scan the index for NPI
     matches": the index only tells you *which in-network file* to open.
  2. in-network rate file                — the rates live here, keyed by
     billing_code (CPT/HCPCS).  Providers are attached either inline
     (`negotiated_rates[].provider_groups[].npi[]`) or, far more commonly for
     national payers (Aetna/UHC), **by reference**: a top-level
     `provider_references[]` array (often externalised to its own file) maps a
     small integer `provider_group_id` -> the npi[] list, and each rate node
     cites the id.

So the only correct, memory-bounded way to extract "rates for these N NPIs" is a
two-source streaming join, NOT a string scan of the index:

    Pass A  (provider spine)  : stream provider_references (inline or the external
                                provider-reference file) -> keep ONLY the group ids
                                whose npi[] intersects the target cohort.
                                RAM = O(|matched groups|) ~ O(|target NPIs|).  Tiny.
    Pass B  (rate spine)      : stream in_network[] -> for each billing_code's
                                negotiated_rates, resolve provider_references-by-id
                                against the matched set (or inline npi[]); emit one
                                flat rate row per (npi x billing_code x price).
                                RAM = O(1) streaming; rows spill to Parquet, then
                                DuckDB casts -> Lance APPEND.

I/O is O(file bytes); RAM is bounded by the cohort, never the payload — a single
Aetna in-network file is tens-to-hundreds of GB and is never resident.

APPEND-ONLY SoR, ATOMIC PER FILE.  Matched rate rows stage to a LOCAL Lance
dataset while streaming and commit to s3://data-sink/active/tic_negotiated_rates/
as ONE append (single manifest commit) only on complete success — a mid-stream
failure leaves zero SoR rows, so retries never double-append. The SoR is never
rewritten in place. Scalar-index (re)build is a SEPARATE, downstream job
(blast-radius isolation: a flaky multi-GB parse can never corrupt the index).

IDEMPOTENCY.  Every (payer, source_file_url, file_version) ingest is guarded by
the ops.tic_reverse_map_runs ledger (HQX_DB_URL_POOLED). source_file_url is
TOKEN-STRIPPED (strip_url_token — SAS `sig` re-mints per index fetch; the blob
path is the identity); file_version is never NULL (derive_file_version: ETag >
Last-Modified > date-slug > bytes surrogate). A file whose version already has a
`success` row is skipped — safe to retry, safe to resume a sharded nationwide run.

POC / local mode:  `uv run reverse_map.py --toc <url> --npis a,b,c` streams real
files and prints telemetry (bytes, ms, peak RSS, throughput). No R2/Modal needed.
Production: the Modal entrypoints fan a sharded worklist across workers, each
writing append fragments + a ledger row.
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import resource
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator

# Lazy heavy imports (ijson/requests/lance/duckdb/boto3) inside the functions that
# need them — the module must import cheaply (the fail-soft pattern in this repo).

# ── Sink coordinates (mirrors database.py / pipelines/* convention) ──────────
BUCKET = "data-sink"
ACTIVE = "active/"
FEED = "tic_negotiated_rates"
DATASET_URI = f"s3://{BUCKET}/{ACTIVE}{FEED}/"

# A national in-network file can be >100 GB. The POC streams a bounded prefix to
# characterise throughput/schema without paying the full transfer; production
# workers lift the cap (stream-to-completion, never resident).
DEFAULT_PREFIX_CAP_BYTES = 250 * 1024**2  # 250 MB compressed prefix (POC)
CHUNK = 1 << 20  # 1 MiB network/gunzip chunk

# Browser-shaped UA. Measured 2026-06-07: NEITHER payer data plane is
# UA-sensitive (identical 200s for curl-UA, Chrome-UA, and empty UA). The
# browser UA is purely defensive against future CDN edge rules; auth on UHC is
# the SAS token, not the agent string. Overridable via TIC_USER_AGENT.
DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Retry posture for the fan-out regime (64 containers against Azure Front Door /
# BunnyCDN is a different regime than the POC's 5 HEADs): 429/503 responses are
# retried with Retry-After / exponential backoff before the stream starts.
RETRY_STATUSES = {429, 503}
MAX_STREAM_RETRIES = 5


def require_fast_ijson() -> None:
    """The cost model (measured 75 MB/s uncompressed per worker) is a property of
    ijson's C backend (yajl2_c). The pure-Python fallback is ~an order of magnitude
    slower — a nationwide run silently becomes ~10x the projected cost, discovered
    only mid-run. Fail fast at worker start; TIC_ALLOW_SLOW_IJSON=1 downgrades to a
    loud warning for local debugging only."""
    import ijson

    backend = getattr(ijson, "backend", "")
    if "yajl2_c" in backend:
        return
    msg = (f"ijson backend is '{backend}', not yajl2_c — throughput drops ~10x and "
           f"the fan-out cost model is void. Install a wheel with the C backend.")
    if os.environ.get("TIC_ALLOW_SLOW_IJSON") == "1":
        print(f"WARN: {msg} (TIC_ALLOW_SLOW_IJSON=1 override active)")
        return
    raise RuntimeError(msg)


def strip_url_token(url: str) -> str:
    """Canonical file identity = the URL with the query string removed. UHC
    downloadUrls carry a SAS token whose `sig` is re-minted on every master-index
    fetch; the blob PATH is the stable identity. Ledger keys and row provenance
    MUST use this form or every worklist rebuild re-ingests the whole payer."""
    return url.split("?", 1)[0]


_DATE_SLUG = None  # compiled lazily


def derive_file_version(headers: dict[str, Any], url: str) -> str:
    """Non-null file_version, always. Preference order: ETag > Last-Modified >
    the dated slug in the blob filename (every UHC/Aetna path carries one, e.g.
    2026-06-01_...) > declared content length. NULL versions silently disable
    idempotency (already_ingested returns False; the Postgres unique index treats
    NULLs as distinct so ON CONFLICT never fires) — never emit one."""
    v = headers.get("etag") or headers.get("last_modified")
    if v:
        return str(v)
    global _DATE_SLUG
    if _DATE_SLUG is None:
        import re
        _DATE_SLUG = re.compile(r"(\d{4}-\d{2}-\d{2})")
    m = _DATE_SLUG.search(strip_url_token(url))
    if m:
        return f"date:{m.group(1)}"
    cl = headers.get("content_length")
    return f"bytes:{cl}" if cl else "unversioned"


# ════════════════════════════════ telemetry ═════════════════════════════════
def _peak_rss_mb() -> float:
    """ru_maxrss is BYTES on darwin, KiB on linux — normalise to MB."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return (rss / 1024**2) if sys.platform == "darwin" else (rss / 1024)


@dataclass
class Telemetry:
    source_url: str = ""
    compressed_bytes: int = 0          # bytes actually pulled off the wire
    content_length: int | None = None  # declared full size (HEAD)
    uncompressed_bytes: int = 0        # bytes after gunzip (over the streamed prefix)
    parse_ms: float = 0.0
    peak_rss_mb: float = 0.0
    rows_emitted: int = 0
    matched_groups: int = 0
    capped: bool = False
    passes: int = 0  # network streams over the big in-network file (inline refs=2, external refs=1)

    @property
    def gzip_ratio(self) -> float:
        return (self.uncompressed_bytes / self.compressed_bytes) if self.compressed_bytes else 0.0

    @property
    def throughput_mb_s(self) -> float:
        return (self.uncompressed_bytes / 1024**2) / (self.parse_ms / 1000) if self.parse_ms else 0.0

    def est_full_uncompressed_bytes(self) -> int | None:
        if self.content_length and self.gzip_ratio:
            return int(self.content_length * self.gzip_ratio)
        return None

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_url": self.source_url,
            "compressed_bytes": self.compressed_bytes,
            "content_length": self.content_length,
            "uncompressed_bytes": self.uncompressed_bytes,
            "gzip_ratio": round(self.gzip_ratio, 3),
            "parse_ms": round(self.parse_ms, 1),
            "peak_rss_mb": round(self.peak_rss_mb, 1),
            "throughput_mb_s": round(self.throughput_mb_s, 2),
            "rows_emitted": self.rows_emitted,
            "matched_groups": self.matched_groups,
            "capped_prefix": self.capped,
            "file_passes": self.passes,
            "est_full_uncompressed_bytes": self.est_full_uncompressed_bytes(),
        }


# ════════════════════════════ streaming byte source ═════════════════════════
def _session():
    import requests

    s = requests.Session()
    s.headers.update({
        "User-Agent": os.environ.get("TIC_USER_AGENT", DEFAULT_UA),
        "Accept": "application/json, application/gzip, */*",
        "Accept-Encoding": "gzip",
    })
    return s


def head(url: str) -> dict[str, Any]:
    """Cheap probe: declared Content-Length / ETag / Last-Modified for the ledger
    idempotency key and the size projection. Falls back to a ranged GET when the
    CDN refuses HEAD (some Azure Front Door origins do)."""
    s = _session()
    try:
        r = s.head(url, allow_redirects=True, timeout=30)
        if r.status_code >= 400 or "Content-Length" not in r.headers:
            r = s.get(url, headers={"Range": "bytes=0-0"}, stream=True, timeout=30)
        cl = r.headers.get("Content-Range", "").split("/")[-1] or r.headers.get("Content-Length")
        return {
            "status": r.status_code,
            "content_length": int(cl) if (cl and cl.isdigit()) else None,
            "etag": r.headers.get("ETag"),
            "last_modified": r.headers.get("Last-Modified"),
            "content_type": r.headers.get("Content-Type"),
        }
    finally:
        s.close()


def stream_gunzip(url: str, cap_bytes: int | None, tel: Telemetry | None = None) -> Iterator[bytes]:
    """Yield decompressed byte chunks from a (gzip) URL, streaming. Counts wire +
    decompressed bytes into `tel`. `cap_bytes` bounds the COMPRESSED prefix pulled
    (POC); pass None to stream to completion (production)."""
    import requests

    s = _session()
    dec = None  # gzip incremental decompressor; created once we see the magic bytes
    raw_seen = 0
    is_gz = strip_url_token(url).endswith(".gz")

    def _open_stream():
        """GET with 429/503 backoff (Retry-After honored) BEFORE any bytes are
        yielded. Mid-body failures still raise — per-file atomicity (local stage,
        publish-on-success) makes a whole-file retry safe."""
        for attempt in range(MAX_STREAM_RETRIES):
            resp = s.get(url, stream=True, timeout=(30, 300))
            if resp.status_code not in RETRY_STATUSES:
                resp.raise_for_status()
                return resp
            retry_after = resp.headers.get("Retry-After")
            wait = float(retry_after) if (retry_after or "").replace(".", "", 1).isdigit() \
                else min(2.0 ** attempt, 60.0)
            resp.close()
            print(f"WARN: HTTP {resp.status_code} on stream open (attempt {attempt + 1}/"
                  f"{MAX_STREAM_RETRIES}); backing off {wait:.1f}s")
            time.sleep(wait)
        resp = s.get(url, stream=True, timeout=(30, 300))
        resp.raise_for_status()
        return resp

    try:
        with _open_stream() as r:
            if tel is not None:
                tel.passes += 1
                tel.content_length = tel.content_length or (
                    int(r.headers["Content-Length"]) if "Content-Length" in r.headers else None)
            enc = (r.headers.get("Content-Type", "") + r.headers.get("Content-Encoding", "")).lower()
            for chunk in r.iter_content(CHUNK):
                if not chunk:
                    continue
                raw_seen += len(chunk)
                if tel is not None:
                    # additive across passes/files so "bytes moved" is truthful:
                    # a 2-pass reverse-map (inline refs) moves 2x the file's bytes.
                    tel.compressed_bytes += len(chunk)
                if dec is None:
                    sniff_gz = is_gz or chunk[:2] == b"\x1f\x8b" or "gzip" in enc
                    if sniff_gz:
                        import zlib
                        dec = zlib.decompressobj(zlib.MAX_WBITS | 16)
                    else:
                        dec = False  # plain JSON, no decompression
                if dec is False:
                    out = chunk
                else:
                    # Multi-member gzip: payer streaming-gzip writers (UHC: 3 members
                    # on the NYU file) concatenate independent gzip members. zlib stops
                    # at the first member's end (dec.eof) and parks the rest in
                    # unused_data — roll a fresh decompressor across each boundary, or
                    # the JSON is silently truncated -> premature-EOF downstream.
                    import zlib
                    buf = bytearray()
                    data = chunk
                    while data:
                        buf += dec.decompress(data)
                        if dec.eof:
                            data = dec.unused_data
                            dec = zlib.decompressobj(zlib.MAX_WBITS | 16)
                        else:
                            data = b""
                    out = bytes(buf)
                if out:
                    if tel is not None:
                        tel.uncompressed_bytes += len(out)
                    yield out
                if cap_bytes is not None and raw_seen >= cap_bytes:
                    if tel is not None:
                        tel.capped = True
                    break
    finally:
        s.close()


class _ChunkReader(io.RawIOBase):
    """Adapt a byte-chunk iterator into a file-like object ijson can consume."""

    def __init__(self, it: Iterator[bytes]):
        self._it = it
        self._buf = b""

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        while not self._buf:
            try:
                self._buf = next(self._it)
            except StopIteration:
                return 0
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n


# ════════════════════════════ TiC schema streaming ═════════════════════════
def iter_toc(url: str, cap_bytes: int | None = None) -> Iterator[dict[str, Any]]:
    """Stream a Table-of-Contents index. Yields one record per reporting_structure
    entry: {plans:[...], in_network_files:[{description,location}], allowed_amount_file}.
    NO NPIs exist at this layer — this only resolves *which* in-network file to open."""
    import ijson

    fp = _ChunkReader(stream_gunzip(url, cap_bytes))
    for rs in ijson.items(fp, "reporting_structure.item"):
        yield {
            "plans": rs.get("reporting_plans", []),
            "in_network_files": rs.get("in_network_files", []),
            "allowed_amount_file": rs.get("allowed_amount_file"),
        }


def build_provider_spine(
    innetwork_url: str,
    target_npis: set[str],
    cap_bytes: int | None,
    tel: Telemetry | None = None,
    external_ref_urls: Iterable[str] = (),
) -> dict[str, list[dict[str, Any]]]:
    """Pass A. Stream the provider_references (inline in the in-network file header
    AND/OR externalised reference files) and return, for ONLY the groups
    intersecting `target_npis`:

        {provider_group_id -> [ {npis: set[str],
                                 tin_type, tin_value, tin_business_name}, ... ]}

    one entry per matched provider_group, carrying that group's TIN verbatim. The
    NPI->TIN edge is the only org-level identifier in the whole chain (the SoR has
    no Type-2 NPI/EIN) — dropping it here would force a full re-stream of the payer
    universe to recover it. RAM is O(matched)."""
    import ijson

    matched: dict[str, list[dict[str, Any]]] = {}

    def _scan(stream_url: str):
        fp = _ChunkReader(stream_gunzip(stream_url, cap_bytes, tel))
        # provider_references is a top-level array in the in-network file, or the
        # body of an external provider-reference file. Both expose `.item`.
        prefix = "provider_references.item" if stream_url == innetwork_url else "item"
        try:
            for ref in ijson.items(fp, prefix):
                gid = str(ref.get("provider_group_id", ""))
                for grp in ref.get("provider_groups", []):
                    hit: set[str] = set()
                    for npi in grp.get("npi", []):
                        if str(npi) in target_npis:
                            hit.add(str(npi))
                    if hit:
                        tin = grp.get("tin") or {}
                        matched.setdefault(gid, []).append({
                            "npis": hit,
                            "tin_type": tin.get("type"),
                            "tin_value": tin.get("value"),
                            "tin_business_name": tin.get("business_name"),
                        })
        except ijson.JSONError:
            pass  # ordering/variant fallthrough handled by caller's 2-pass option

    for u in external_ref_urls:
        _scan(u)
    if not external_ref_urls:
        _scan(innetwork_url)
    if tel is not None:
        tel.matched_groups = len(matched)
    return matched


def extract_rates(
    innetwork_url: str,
    matched_groups: dict[str, list[dict[str, Any]]],
    target_npis: set[str],
    payer: str,
    plan_id: str | None,
    cap_bytes: int | None,
    tel: Telemetry | None = None,
    file_version: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Pass B. Stream in_network[]; for each billing_code, resolve negotiated_rates
    against the matched group entries (by-reference) or inline provider_groups (by
    value); emit one flat row per (npi, billing_code, price). O(1) RAM streaming.

    ROW SCHEMA / JOIN RULE (load-bearing — read before joining this table):
      tin_type / tin_value / tin_business_name carry the provider_group's TIN
      verbatim (CMS TiC v2.0.0 `tin` object). The org-level join to Form 5500
      sponsors, the employer bridge, and the entity graph is:

          tin_type == 'ein'  ->  tin_value IS a 9-digit EIN; digits-normalize both
                                 sides and join (form5500_main.SPONS_DFE_EIN, the
                                 tic_employer_file_bridge.ein, ...).
          tin_type == 'npi'  ->  tin_value is the SOLE PROPRIETOR'S OWN NPI (CMS
                                 schema allows TIN=NPI). It is NOT an EIN. It MUST
                                 be excluded from every EIN-space join or it
                                 silently collides with the 9-digit EIN space.

      Rows with tin_type=='npi' are PRESERVED (the rate facts are real); the
      exclusion is enforced by predicate at join time, structurally possible
      because tin_type is a first-class column.
      source_file_url is token-stripped (stable blob path, no SAS `sig`).
      file_version stamps every row with the ingested version (ETag-derived, never
      NULL) — makes monthly re-drops distinguishable and failed-attempt cleanup
      addressable at row level."""
    import ijson

    fp = _ChunkReader(stream_gunzip(innetwork_url, cap_bytes, tel))
    captured_at = os.environ.get("TIC_CAPTURE_TS") or time.strftime("%Y-%m-%d", time.gmtime())
    source_url = strip_url_token(innetwork_url)
    for item in ijson.items(fp, "in_network.item"):
        code = item.get("billing_code")
        code_type = item.get("billing_code_type")
        for nr in item.get("negotiated_rates", []):
            # resolve npi -> TIN for this rate node (ref-by-id OR inline). A given
            # npi keeps the first TIN seen within the node (multi-TIN practices
            # appear under multiple provider_groups and emit under each group's id).
            npi_tin: dict[str, dict[str, Any]] = {}
            for gid in nr.get("provider_references", []):
                for entry in matched_groups.get(str(gid), ()):
                    for npi in entry["npis"]:
                        npi_tin.setdefault(npi, entry)
            for grp in nr.get("provider_groups", []):
                tin = grp.get("tin") or {}
                for npi in grp.get("npi", []):
                    if str(npi) in target_npis:
                        npi_tin.setdefault(str(npi), {
                            "tin_type": tin.get("type"),
                            "tin_value": tin.get("value"),
                            "tin_business_name": tin.get("business_name"),
                        })
            if not npi_tin:
                continue
            for price in nr.get("negotiated_prices", []):
                rate = price.get("negotiated_rate")
                rate = float(rate) if rate is not None else None  # ijson -> Decimal; Lance wants a stable float64
                for npi, entry in npi_tin.items():
                    if tel is not None:
                        tel.rows_emitted += 1
                    yield {
                        "payer": payer,
                        "plan_id": plan_id,
                        "npi": npi,
                        "tin_type": entry.get("tin_type"),
                        "tin_value": str(entry["tin_value"]) if entry.get("tin_value") is not None else None,
                        "tin_business_name": entry.get("tin_business_name"),
                        "billing_code": str(code) if code is not None else None,
                        "billing_code_type": code_type,
                        "negotiated_rate": rate,
                        "negotiated_type": price.get("negotiated_type"),
                        "billing_class": price.get("billing_class"),
                        "service_codes": ",".join(price.get("service_code", []) or []),
                        "expiration_date": price.get("expiration_date"),
                        "source_file_url": source_url,
                        "file_version": file_version,
                        "captured_at": captured_at,
                    }


# ════════════════════════════ Lance append sink ════════════════════════════
# tin_value is BTREE'd: it is the load-bearing org-level resolution key (join to
# Form 5500 / the employer bridge, gated on tin_type='ein' — see extract_rates).
RATE_BTREE = ["npi", "billing_code", "tin_value"]
RATE_BITMAP = ["payer", "billing_class", "billing_code_type", "tin_type"]


def append_rates_to_lance(rows: list[dict[str, Any]], local_only_dir: str | None = None) -> int:
    """Append matched rate rows as a NEW Lance fragment (mode='append'). The SoR is
    never rewritten. `local_only_dir` writes a local dataset for the POC/dry-run.

    PRODUCTION WORKERS NEVER CALL THIS AGAINST R2 MID-STREAM. Per-file ingestion is
    atomic: batches stage to a LOCAL dataset (local_only_dir) while streaming, and
    publish_stage_to_sor commits the whole file to R2 in ONE Lance append only on
    complete success — a mid-stream failure leaves zero rows in the SoR, so a retry
    can never double-append."""
    if not rows:
        return 0
    import pyarrow as pa
    import lance

    tbl = pa.Table.from_pylist(rows)
    if local_only_dir:
        uri, so = os.path.join(local_only_dir, f"{FEED}.lance"), None
    else:
        uri, so = DATASET_URI, _r2_so()
    try:
        lance.dataset(uri, storage_options=so)
        lance.write_dataset(tbl, uri, mode="append", storage_options=so)
    except (FileNotFoundError, ValueError):
        lance.write_dataset(tbl, uri, mode="create", storage_options=so)
    return len(rows)


def publish_stage_to_sor(stage_dir: str) -> int:
    """Atomic per-file commit: stream the fully-staged local dataset into the R2 SoR
    as ONE Lance append (single manifest commit — all of the file's rows become
    visible together, or none do). Called ONLY after the source stream completed
    without error. Mirrors the local-stage-then-publish pattern of
    pipelines/nppes/materialize_analytical.py (D8)."""
    import lance

    local_uri = os.path.join(stage_dir, f"{FEED}.lance")
    try:
        src = lance.dataset(local_uri)
    except (FileNotFoundError, ValueError):
        return 0  # zero matched rows — nothing to publish
    n = src.count_rows()
    if n == 0:
        return 0
    reader = src.scanner().to_reader()
    so = _r2_so()
    try:
        lance.dataset(DATASET_URI, storage_options=so)
        lance.write_dataset(reader, DATASET_URI, schema=reader.schema,
                            mode="append", storage_options=so)
    except (FileNotFoundError, ValueError):
        lance.write_dataset(reader, DATASET_URI, schema=reader.schema,
                            mode="create", storage_options=so)
    return n


def _r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


# ════════════════════════════ idempotency ledger ═══════════════════════════
# Key discipline: source_file_url is stored TOKEN-STRIPPED (strip_url_token) —
# the blob path is the stable identity; the SAS query string re-mints per
# master-index fetch. file_version is NOT NULL (derive_file_version guarantees a
# surrogate) — NULLs are distinct to the unique index, which would disable both
# the skip and the ON CONFLICT upsert silently.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.tic_reverse_map_runs (
    id                bigserial PRIMARY KEY,
    run_id            text NOT NULL,
    payer             text NOT NULL,
    source_file_url   text NOT NULL,     -- TOKEN-STRIPPED (no query string / SAS sig)
    file_version      text NOT NULL,     -- ETag > Last-Modified > date-slug > bytes surrogate; never NULL
    cohort_size       int,               -- target NPIs searched
    matched_groups    int,
    rows_emitted      int,
    compressed_bytes  bigint,
    uncompressed_bytes bigint,
    throughput_mb_s   numeric,
    peak_rss_mb       numeric,
    parse_ms          numeric,
    status            text NOT NULL,     -- success | error | skipped
    error             text,
    started_at        timestamptz,
    completed_at      timestamptz,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS tic_reverse_map_runs_uq
    ON ops.tic_reverse_map_runs (payer, source_file_url, file_version);
CREATE INDEX IF NOT EXISTS tic_reverse_map_runs_recorded_idx
    ON ops.tic_reverse_map_runs (recorded_at DESC);
CREATE INDEX IF NOT EXISTS tic_reverse_map_runs_payer_idx
    ON ops.tic_reverse_map_runs (payer);
"""


def already_ingested(payer: str, url: str, file_version: str) -> bool:
    """Idempotency guard: True if a `success` row exists for this exact file
    version. Guards duplicate appends on retry / resharded reruns. `url` is
    canonicalized (token-stripped) before the lookup; `file_version` must be the
    non-null derive_file_version output."""
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn or not file_version:
        return False
    try:
        import psycopg

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "SELECT 1 FROM ops.tic_reverse_map_runs "
                "WHERE payer=%s AND source_file_url=%s AND file_version=%s AND status='success' LIMIT 1",
                (payer, strip_url_token(url), file_version))
            return cur.fetchone() is not None
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ledger read failed (treating as not-ingested): {exc}")
        return False


def record_run(run_id: str, payer: str, url: str, file_version: str,
               cohort_size: int, tel: Telemetry, status: str, error: str | None,
               started_at, completed_at) -> None:
    """Terminal run row → ops.tic_reverse_map_runs (best-effort; audit never masks
    the ingest). `url` is canonicalized (token-stripped) on write."""
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        import psycopg

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """INSERT INTO ops.tic_reverse_map_runs
                   (run_id,payer,source_file_url,file_version,cohort_size,matched_groups,
                    rows_emitted,compressed_bytes,uncompressed_bytes,throughput_mb_s,
                    peak_rss_mb,parse_ms,status,error,started_at,completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (payer,source_file_url,file_version) DO UPDATE SET
                     status=EXCLUDED.status, rows_emitted=EXCLUDED.rows_emitted,
                     matched_groups=EXCLUDED.matched_groups, recorded_at=now()""",
                (run_id, payer, strip_url_token(url), file_version or "unversioned",
                 cohort_size, tel.matched_groups,
                 tel.rows_emitted, tel.compressed_bytes, tel.uncompressed_bytes,
                 round(tel.throughput_mb_s, 2), round(tel.peak_rss_mb, 1),
                 round(tel.parse_ms, 1), status, error, started_at, completed_at))
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* state write failed: {exc}")


# ════════════════════════ single-file POC driver ═══════════════════════════
def reverse_map_one_file(
    innetwork_url: str,
    target_npis: set[str],
    payer: str,
    plan_id: str | None = None,
    external_ref_urls: Iterable[str] = (),
    cap_bytes: int | None = DEFAULT_PREFIX_CAP_BYTES,
    sink_dir: str | None = None,
    file_version: str | None = None,
) -> tuple[list[dict[str, Any]], Telemetry]:
    """End-to-end on ONE in-network file: spine (Pass A) -> rates (Pass B) ->
    optional Lance append. Returns (rows, telemetry). The two passes share one
    Telemetry so bytes/ms/RSS reflect the whole reverse-map of the file."""
    require_fast_ijson()
    tel = Telemetry(source_url=innetwork_url)
    t0 = time.perf_counter()
    spine = build_provider_spine(innetwork_url, target_npis, cap_bytes, tel, external_ref_urls)
    rows = list(extract_rates(innetwork_url, spine, target_npis, payer, plan_id, cap_bytes, tel,
                              file_version=file_version))
    tel.parse_ms = (time.perf_counter() - t0) * 1000
    tel.peak_rss_mb = _peak_rss_mb()
    if sink_dir is not None:
        append_rates_to_lance(rows, local_only_dir=sink_dir)
    return rows, tel


def _main() -> int:
    ap = argparse.ArgumentParser(description="TiC reverse-mapper (POC local mode)")
    ap.add_argument("--innetwork", help="in-network rate file URL (.json/.json.gz)")
    ap.add_argument("--toc", help="ToC index URL; lists in-network files (no NPIs)")
    ap.add_argument("--provider-ref", action="append", default=[], help="external provider-reference file URL(s)")
    ap.add_argument("--npis", required=True, help="comma-separated target NPI cohort")
    ap.add_argument("--payer", default="unknown")
    ap.add_argument("--plan-id", default=None)
    ap.add_argument("--cap-mb", type=int, default=250, help="compressed-prefix cap (POC); 0 = stream to completion")
    ap.add_argument("--sink-dir", default=None, help="local Lance dir for append dry-run")
    args = ap.parse_args()

    npis = {n.strip() for n in args.npis.split(",") if n.strip()}
    cap = None if args.cap_mb == 0 else args.cap_mb * 1024**2

    if args.toc and not args.innetwork:
        print(f"[ToC] streaming {args.toc} — resolving in-network file list (no NPIs at this layer)")
        for i, rec in enumerate(iter_toc(args.toc, cap)):
            plans = [p.get("plan_name") for p in rec["plans"]][:2]
            files = [f.get("location") for f in rec["in_network_files"]][:2]
            print(f"  rs[{i}] plans={plans} -> in_network_files(head)={files}")
            if i >= 4:
                print("  … (truncated; pick the smallest in-network file via HEAD, then --innetwork)")
                break
        return 0

    if not args.innetwork:
        ap.error("pass --innetwork <url> (or --toc to enumerate first)")

    h = head(args.innetwork)
    fver = derive_file_version(h, args.innetwork)
    print(f"[HEAD] status={h['status']} content_length={h['content_length']} "
          f"etag={h.get('etag')} file_version={fver}")
    rows, tel = reverse_map_one_file(
        args.innetwork, npis, args.payer, args.plan_id,
        external_ref_urls=args.provider_ref, cap_bytes=cap, sink_dir=args.sink_dir,
        file_version=fver)
    tel.content_length = tel.content_length or h["content_length"]
    print(json.dumps({"telemetry": tel.as_dict(), "sample_rows": rows[:5]}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
