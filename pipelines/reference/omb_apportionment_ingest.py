"""OMB Public Apportionment — the SF-132 line-level release of budget authority → Gen-3 Lance SoR.

Directive: docs/plans/2026-07-27-OMB_APPORTIONMENT_INGEST_DIRECTIVE.md. The missing middle
step between appropriation and obligation: Congress appropriates → OMB apportions (releases the
money to agencies in tranches, on form SF-132) → the agency obligates it on a contract. The
plane measures the last step only; apportionment is the earliest public signal that money is
about to move, and it carries a public-law attribution field (``FundsProvidedBy``) that no
other feed provides.

SOURCE  https://apportionment-public.max.gov/ — a single flat ~19.6 MB HTML index carrying a
direct link to every JSON file (30,443 as of 2026-07-27; FY2022–FY2026). No API, no auth, and
critically NO rate-limit headers — a host either tolerates you or serves a block page with no
warning. Every network call therefore routes through the shared governor
(``pipelines._lib.rate_governor``): token bucket ≤2 req/s, warm-up ramp, circuit breaker,
resumable path-checkpoint. See RATE DISCIPLINE in the directive.

PATTERN A (direct hydration): index → governed crawl → R2 cache → deterministic JSON parse →
three Lance datasets (overwrite). Raw stays lossless — every ``ScheduleData`` line and every
footnote lands as its own row with every field the payload carries. Zero LLM.

    active/omb_apportionment_files/       one row = apportionment document (TAFS × iteration)
    active/omb_apportionment_lines/       one row = SF-132 schedule line within a document
    active/omb_apportionment_footnotes/   one row = footnote within a document

GRAIN — the payload is canonical; the filename is supplementary. Filenames are NOT uniform
(the clean ``FY{y}_Agency={a}_Bureau={b}_TAFS={t}_Iteration={n}_{ts}`` shape covers ~88% of a
sample; EPA files omit TAFS/Iteration, Treasury uses ``Account=``). So iteration comes from the
payload (``ScheduleData[].Iteration``, 100% fill) and the §8 gate validates the FILENAME
iteration against it only where the filename encodes one.

line_kind — SF-132 has two halves that are equal by construction: budgetary resources (line
numbers 1000–1999) and application of budgetary resources (6000–6999). Each half includes its
own total line (1920 / 6190), so ``SUM(amount)`` across ALL lines DOUBLES the true total —
never sum both halves together. Alpha line markers (``IterNo``/``AdjAut``/``RptCat``) and any
numeric line outside those two ranges are ``line_kind='marker'`` and excluded from every sum.
The §8 identity gate proves the partition: Σ(budgetary) == Σ(application) per document within $1.

    doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with duckdb \\
      --with requests --with boto3 --with 'psycopg[binary]' \\
      python -m pipelines.reference.omb_apportionment_ingest --stream all --smoke
    ... --stream all                                                                 # full crawl

Ledger: ops.omb_apportionment_ingest_runs (HQX_DB_URL_POOLED; L4 canonical status enum). Source
registered in ops.data_source_catalog (L60). Both applied IF NOT EXISTS / ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import argparse
import collections
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import os
import re
import threading
import time
import uuid

# Cyberduck/newer-SDK composite-checksum guard (fleet convention).
os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")
os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")

# Reuse the fleet R2/index plumbing verbatim (do not reimplement).
from pipelines.bls.ingest import (  # noqa: E402
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _s3_client,
    _storage_options,
)
from pipelines._lib.rate_governor import (  # noqa: E402
    CheckpointStore,
    PathCheckpoint,
    RateGovernor,
    ThrottledError,
)

BUCKET = "data-sink"
SOURCE_TAG = "omb_apportionment_ingest"
SOURCE_SLUG = "omb_apportionment"
ORIGIN = "https://apportionment-public.max.gov"
UA = ("core-x-data-factory/1.0 (federal reference-data ingest; "
      "contact: benjamin.crane@engineereddemand.com)")

CACHE_PREFIX = "landing/omb_apportionment/cache/blobs/"      # object per URL path (sha1)
CHECKPOINT_KEY = "landing/omb_apportionment/cache/_checkpoint.json"
WORKLIST_KEY = "landing/omb_apportionment/cache/_worklist.json"

URIS = {
    "files": f"s3://{BUCKET}/active/omb_apportionment_files/",
    "lines": f"s3://{BUCKET}/active/omb_apportionment_lines/",
    "footnotes": f"s3://{BUCKET}/active/omb_apportionment_footnotes/",
}

# §2.1 baseline (verified 2026-07-27) — gates compare against these, tree only grows.
FY_BASELINE = {"2022": 6015, "2023": 6292, "2024": 6545, "2025": 6172, "2026": 5419}
REQUIRED_FYS = {2022, 2023, 2024, 2025, 2026}
MIN_LINKS = 25_000
# The directive estimated 50–130 lines/file (~1.5–4M total) and set a 1M floor "(implies a
# parse that dropped rows)", explicitly deferring the count to in-run confirmation. Confirmed
# in-run 2026-07-28: the real data is ~17 lines/file → 515,777 lines across 30,368 docs, with
# lines_written == sd_rows exactly and the SF-132 identity holding for all 30,368 docs (zero
# drops). The 1M floor was mis-calibrated to a high estimate. The true anti-drop gate is the
# exact completeness equality below; this is now a coarse sanity floor the confirmed data clears.
MIN_SCHEDULE_LINES = 400_000
MAX_FAIL_RATIO = 0.02

CHECKPOINT_EVERY = 200
LINES_FLUSH_ROWS = 400_000        # chunked Lance append bound for the ~2–4M-row lines dataset
SMOKE_N = 50


# ── coercion ────────────────────────────────────────────────────────────────────────
def _s(v) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _num(v) -> float | None:
    """Numeric coercion — accounting negatives are REAL (reductions/transfers); never abs()."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace("$", "")
    if not s or s in {"(D)", "(S)", "(X)", "(NA)", "(Z)", "...", "-", "--", "N.A.", "NA", "n.a."}:
        return None
    if s.startswith("(") and s.endswith(")"):  # accounting negative
        s = "-" + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return None


def _snake(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(name).strip())
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)  # camelCase → camel_Case
    s = re.sub(r"_+", "_", s).strip("_").lower()
    return s or "col"


def _parse_approval_ts(s: str | None):
    s = _s(s)
    if s is None:
        return None
    for fmt in ("%Y-%m-%d-%H.%M.%S.%f", "%Y-%m-%d-%H.%M.%S", "%Y-%m-%d-%H.%M"):
        try:
            return dt.datetime.strptime(s, fmt).replace(tzinfo=dt.timezone.utc)
        except ValueError:
            continue
    return None


# filename grain (best-effort; payload is canonical). Handles the observed variants:
#   FY2026_Agency=COE_Bureau=COE_TAFS=096-X-8862_Iteration=1_2025-09-16-17.07   (clean)
#   FY2025_Agency=EPA_Bureau=EPA_2024-09-18-17.29                               (no TAFS/Iter)
#   FY2026_Agency=TREASURY_Bureau=DEPTOFF_Account=020-18552025-09-23-10.09      (Account=, glued)
_RE_AGENCY = re.compile(r"Agency=([^_]+)")
_RE_BUREAU = re.compile(r"Bureau=([^_]+)")
_RE_TAFS_ITER = re.compile(r"TAFS=(.+?)_Iteration=(\d+)")
_RE_ACCOUNT = re.compile(r"Account=(.+?)(\d{4}-\d{2}-\d{2}-\d{2}\.\d{2})")


def parse_filename(file_name: str) -> dict:
    fn = file_name or ""
    ma, mb = _RE_AGENCY.search(fn), _RE_BUREAU.search(fn)
    agency = ma.group(1) if ma else None
    bureau = mb.group(1) if mb else None
    tafs, fn_iter = None, None
    m = _RE_TAFS_ITER.search(fn)
    if m:
        tafs, fn_iter = m.group(1), int(m.group(2))
    else:
        mc = _RE_ACCOUNT.search(fn)
        if mc:
            tafs = mc.group(1).rstrip("_-")
    return {"agency": _s(agency), "bureau": _s(bureau), "tafs": _s(tafs), "fn_iteration": fn_iter}


def canon_tafs(agency, avail, begin, end, acct) -> str:
    """Reconstruct a uniform canonical TAFS from payload fields when the filename lacks one."""
    agency = _s(agency) or "?"
    acct = _s(acct) or "?"
    av = (_s(avail) or "").upper()
    begin, end = _s(begin), _s(end)
    if av == "X":
        period = "X"
    elif begin and end and begin != end:
        period = f"{begin}/{end}"
    elif end:
        period = end
    elif begin:
        period = begin
    else:
        period = "X"
    return f"{agency}-{period}-{acct}"


def _line_kind(line_number: str | None) -> str:
    s = (_s(line_number) or "")
    if s.isdigit():
        n = int(s)
        if 1000 <= n <= 1999:
            return "budgetary_resource"
        if 6000 <= n <= 6999:
            return "application_of_resource"
    return "marker"


def _cache_key(path: str) -> str:
    return f"{CACHE_PREFIX}{hashlib.sha1(path.encode('utf-8')).hexdigest()}.json"


# ── R2 helpers ──────────────────────────────────────────────────────────────────────
_s3_local = threading.local()


def _s3():
    c = getattr(_s3_local, "client", None)
    if c is None:
        c = _s3_local.client = _s3_client()
    return c


def _r2_put(key: str, body: bytes) -> None:
    _s3().put_object(Bucket=BUCKET, Key=key, Body=body)


def _r2_get(key: str) -> bytes | None:
    try:
        return _s3().get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except _s3().exceptions.NoSuchKey:
        return None


class R2CheckpointStore(CheckpointStore):
    """Backs the completed-path checkpoint on a stable R2 object (survives sessions)."""

    def __init__(self, key: str):
        self._key = key

    def read(self):
        return _r2_get(self._key)

    def write(self, data: bytes) -> None:
        _r2_put(self._key, data)


# ── ledger (ops.omb_apportionment_ingest_runs) + catalog (ops.data_source_catalog) ───
LEDGER_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.omb_apportionment_ingest_runs (
    run_id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    stream            text        NOT NULL,
    index_link_count  integer,
    files_fetched     integer,
    files_failed      integer,
    rows_written      bigint,
    datasets          jsonb,
    started_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    status            text        NOT NULL CHECK (status IN ('running','completed','failed')),
    disposition       text,
    notes             text
);
CREATE INDEX IF NOT EXISTS idx_omb_apportionment_ingest_runs_status_started
    ON ops.omb_apportionment_ingest_runs (status, started_at);
"""

# L60 canonical catalog (16-col schema lifted from the data-engine-x base migration). The
# Gen-3/HQX plane has no catalog yet; bootstrap it here so this and future core-x ingests
# register canonically. The cross-plane status VIEW (audience/bridge/r2-snapshot joins) is
# a dashboard concern of the other plane and intentionally not ported.
CATALOG_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.data_source_catalog (
  source_slug         TEXT PRIMARY KEY,
  display_name        TEXT NOT NULL,
  strategic_role      TEXT NOT NULL,
  r2_prefix           TEXT NOT NULL,
  refresh_cadence     TEXT NOT NULL CHECK (refresh_cadence IN
                        ('one-shot','daily','weekly','monthly','quarterly','biennial','annual','on-demand')),
  lifecycle_stage     TEXT NOT NULL CHECK (lifecycle_stage IN
                        ('discovery','r2_only','rw_source_wired','essentials_hydrated',
                         'bridge_layer','audience_layer','streaming_refresh')),
  audit_ledger_table  TEXT,
  essentials_mv_name  TEXT,
  source_url          TEXT,
  notes               TEXT,
  owner_team          TEXT NOT NULL DEFAULT 'data-factory',
  is_active           BOOLEAN NOT NULL DEFAULT TRUE,
  bridge_source_name_patterns TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  audience_mv_name_patterns   TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (source_slug ~ '^[a-z0-9_]+$'),
  CHECK (display_name <> ''),
  CHECK (strategic_role <> ''),
  CHECK (r2_prefix <> '')
);
"""

CATALOG_INSERT = """
INSERT INTO ops.data_source_catalog
  (source_slug, display_name, strategic_role, r2_prefix, refresh_cadence, lifecycle_stage,
   audit_ledger_table, essentials_mv_name, source_url, notes,
   bridge_source_name_patterns, audience_mv_name_patterns, is_active)
VALUES
  ('omb_apportionment',
   'OMB Public Apportionment (SF-132)',
   'The missing middle step between appropriation and obligation — the earliest public, '
   'line-item release of budget authority to agencies; the only feed carrying the '
   'FundsProvidedBy public-law attribution (OBBA / P.L. candidate).',
   'active/omb_apportionment_files/', 'quarterly', 'r2_only',
   'ops.omb_apportionment_ingest_runs', NULL,
   'https://apportionment-public.max.gov/',
   'Three Lance datasets: active/omb_apportionment_{files,lines,footnotes}/. Line grain is '
   'SF-132 schedule lines (budgetary_resource vs application_of_resource halves, equal by '
   'construction). Catalog bootstrapped in HQX (Gen-3 plane has no catalog yet).',
   ARRAY['source_omb_apportionment_%'], ARRAY['mv_audience_omb_apportionment_%'], TRUE)
ON CONFLICT (source_slug) DO NOTHING;
"""


def _pg():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        return None
    return psycopg.connect(dsn)


def _ensure_ledger_and_catalog() -> None:
    conn = _pg()
    if conn is None:
        print("WARN: HQX_DB_URL_POOLED not set; ledger/catalog skipped.", flush=True)
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(LEDGER_DDL)
            cur.execute(CATALOG_DDL)
            cur.execute(CATALOG_INSERT)
    except Exception as exc:  # noqa: BLE001 — audit must never mask a good load
        print(f"WARN: ledger/catalog DDL failed: {exc}", flush=True)
    finally:
        conn.close()


def _ledger_start(stream: str) -> str | None:
    run_id = str(uuid.uuid4())
    conn = _pg()
    if conn is None:
        return run_id
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ops.omb_apportionment_ingest_runs (run_id, stream, status) "
                "VALUES (%s, %s, 'running')",
                (run_id, stream),
            )
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ledger start failed: {exc}", flush=True)
    finally:
        conn.close()
    return run_id


def _ledger_finish(run_id: str | None, *, status: str, disposition: str | None,
                   index_link_count, files_fetched, files_failed, rows_written,
                   datasets: dict, notes: str) -> None:
    if run_id is None:
        return
    conn = _pg()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ops.omb_apportionment_ingest_runs
                SET status=%s, disposition=%s, finished_at=now(),
                    index_link_count=%s, files_fetched=%s, files_failed=%s,
                    rows_written=%s, datasets=%s, notes=%s
                WHERE run_id=%s
                """,
                (status, disposition, index_link_count, files_fetched, files_failed,
                 rows_written, json.dumps(datasets), notes[:8000], run_id),
            )
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ledger finish failed: {exc}", flush=True)
    finally:
        conn.close()


# ── phase 1: index → work list ───────────────────────────────────────────────────────
def fetch_index(gov: RateGovernor, sess) -> list[str]:
    def do_get(url):
        return sess.get(url, headers={"User-Agent": UA}, timeout=600)

    resp = gov.request(do_get, ORIGIN + "/")
    if resp.status_code != 200:
        raise RuntimeError(f"index GET {resp.status_code}")
    links = re.findall(r'href="(/Fiscal%20Year%20\d{4}/[^"]+\.json)"', resp.text)
    # de-dup preserving order
    seen = set()
    out = []
    for l in links:
        if l not in seen:
            seen.add(l)
            out.append(l)
    return out


def _per_fy(links: list[str]) -> dict[str, int]:
    c = collections.Counter(re.search(r"/Fiscal%20Year%20(\d{4})/", l).group(1) for l in links)
    return dict(sorted(c.items()))


def gate_index(links: list[str]) -> dict:
    per_fy = _per_fy(links)
    if len(links) < MIN_LINKS:
        raise RuntimeError(f"index link count {len(links)} < {MIN_LINKS} — refusing to proceed")
    present = {int(y) for y in per_fy}
    missing = REQUIRED_FYS - present
    if missing:
        raise RuntimeError(f"missing required fiscal years: {sorted(missing)} (per_fy={per_fy})")
    print(f"[index] {len(links):,} links; per-FY {per_fy}", flush=True)
    for y, base in FY_BASELINE.items():
        obs = per_fy.get(y, 0)
        flag = "" if obs >= base else "  <-- BELOW BASELINE"
        print(f"[index]   FY{y}: {obs} (baseline {base}){flag}", flush=True)
    return per_fy


# ── phase 2: governed crawl → R2 cache (+ local mirror) ──────────────────────────────
def crawl(gov: RateGovernor, links: list[str], checkpoint: PathCheckpoint, local_dir: str,
          *, smoke: bool) -> dict:
    import requests

    sess = requests.Session()

    def do_get(url):
        return sess.get(url, headers={"User-Agent": UA}, timeout=120)

    todo = [p for p in links if p not in checkpoint]
    print(f"[crawl] {len(links)} links; {len(links) - len(todo)} already cached; "
          f"{len(todo)} to fetch", flush=True)

    abort = threading.Event()
    failed_paths: list[str] = []
    lock = threading.Lock()
    counters = {"ok": 0, "failed": 0}

    def fetch_one(path: str) -> str:
        if abort.is_set():
            return "aborted"
        url = ORIGIN + path
        for _ in range(3):  # 2 retries on 5xx/timeout; 403/429 handled inside the governor
            if abort.is_set():
                return "aborted"
            try:
                resp = gov.request(do_get, url)
            except requests.RequestException:
                gov.note_transport_error()
                continue
            if resp.status_code == 200:
                body = resp.content
                _r2_put(_cache_key(path), body)
                with open(os.path.join(local_dir, _cache_key(path).rsplit("/", 1)[-1]), "wb") as fh:
                    fh.write(body)
                checkpoint.add(path)
                return "ok"
            # non-200 soft failure already counted by the governor; retry
        with lock:
            failed_paths.append(path)
        return "failed"

    t0 = time.monotonic()
    throttled = False
    done = 0
    try:
        with cf.ThreadPoolExecutor(max_workers=gov.max_workers) as pool:
            futs = {pool.submit(fetch_one, p): p for p in todo}
            for fut in cf.as_completed(futs):
                try:
                    res = fut.result()
                except ThrottledError:
                    throttled = True
                    abort.set()
                    print("[crawl] THROTTLED — second breaker trip; halting crawl", flush=True)
                    break
                done += 1
                if res == "ok":
                    counters["ok"] += 1
                elif res == "failed":
                    counters["failed"] += 1
                if done % CHECKPOINT_EVERY == 0:
                    checkpoint.flush()
                    rate = done / max(1e-6, time.monotonic() - t0)
                    print(f"[crawl] {done}/{len(todo)}  ok={counters['ok']} "
                          f"failed={counters['failed']}  {rate:.2f} files/s  gov={gov.stats()}",
                          flush=True)
    finally:
        checkpoint.flush()

    fetched = len(checkpoint)
    print(f"[crawl] done: cached={fetched} new_ok={counters['ok']} "
          f"failed={len(failed_paths)} throttled={throttled} gov={gov.stats()}", flush=True)
    return {"fetched": fetched, "failed_paths": failed_paths, "throttled": throttled}


def _read_cached(path: str, local_dir: str) -> bytes | None:
    """Local mirror first; hydrate from R2 on a fresh-process resume (no host hit)."""
    lp = os.path.join(local_dir, _cache_key(path).rsplit("/", 1)[-1])
    if os.path.exists(lp):
        with open(lp, "rb") as fh:
            return fh.read()
    body = _r2_get(_cache_key(path))
    if body is not None:
        with open(lp, "wb") as fh:
            fh.write(body)
    return body


# ── phase 3: parse cached JSON → 3 Arrow tables → Lance ──────────────────────────────
def _build_table(rows: list[dict], fields):
    import pyarrow as pa

    typemap = {"i64": pa.int64(), "i32": pa.int32(), "f64": pa.float64(),
               "str": pa.string(), "ts": pa.timestamp("us", tz="UTC")}
    cols = {name: pa.array([r.get(name) for r in rows], type=typemap[kind])
            for name, kind in fields}
    return pa.table(cols)


def _write_lance(table, uri: str, so: dict, *, mode: str) -> None:
    import lance

    lance.write_dataset(table, uri, mode=mode, data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                        storage_options=so)


def _iter_docs(links: list[str], local_dir: str):
    """Yield (path, doc) for each cached, parseable payload; None bodies are skipped."""
    for path in links:
        body = _read_cached(path, local_dir)
        if body is None:
            continue
        try:
            yield path, json.loads(body)
        except (ValueError, TypeError):
            continue


def discover_union(links: list[str], local_dir: str) -> dict:
    """Pass 1 — accumulate the ScheduleData + FootnoteData key unions with per-key fill and
    numeric-castability across ALL cached files (§2.4). Cheap: only counters in memory."""
    sd_nonnull = collections.Counter()
    sd_numeric = collections.Counter()
    sd_rows = 0
    fn_keys = collections.Counter()
    fn_rows = 0
    n_docs = 0
    agencies = set()
    fys = set()
    for path, doc in _iter_docs(links, local_dir):
        n_docs += 1
        agencies.add(parse_filename(doc.get("FileName", "")).get("agency"))
        fys.add(_s(doc.get("FiscalYear")))
        for row in doc.get("ScheduleData") or []:
            sd_rows += 1
            for k, v in row.items():
                if v is not None and str(v).strip() != "":
                    sd_nonnull[_snake(k) if k != "Iteration" else "schedule_iteration"] += 1
                    if _num(v) is not None:
                        sd_numeric[_snake(k) if k != "Iteration" else "schedule_iteration"] += 1
        for fr in doc.get("FootnoteData") or []:
            fn_rows += 1
            for k, v in (fr or {}).items():
                if v is not None and str(v).strip() != "":
                    fn_keys[_snake(k)] += 1

    # amount columns: money-ish name AND numeric on >98% of non-null rows
    amount_cols = sorted(
        k for k in sd_nonnull
        if re.search(r"amount|amt|dollar", k)
        and sd_nonnull[k] > 0 and sd_numeric[k] / sd_nonnull[k] > 0.98
    )
    return {
        "sd_nonnull": dict(sd_nonnull), "sd_numeric": dict(sd_numeric), "sd_rows": sd_rows,
        "fn_keys": dict(fn_keys), "fn_rows": fn_rows, "n_docs": n_docs,
        "amount_cols": amount_cols,
        "agencies": sorted(a for a in agencies if a), "fys": sorted(f for f in fys if f),
    }


def _files_fields():
    return [
        ("file_id", "i64"), ("file_name", "str"), ("fiscal_year", "i32"),
        ("approval_timestamp", "str"), ("approval_ts", "ts"), ("folder", "str"),
        ("approver_title", "str"), ("funds_provided_by", "str"), ("agency_code", "str"),
        ("bureau_code", "str"), ("tafs", "str"), ("iteration", "i32"), ("source_url", "str"),
        ("n_lines", "i32"), ("n_footnotes", "i32"), ("source", "str"), ("ingested_at", "ts"),
    ]


def _lines_fields(union_keys: list[str], amount_cols: set[str]):
    # denormalized document keys first, then the ScheduleData union (deterministic order),
    # then the derived + provenance columns.
    fields = [("file_id", "i64"), ("fiscal_year", "i32"), ("tafs", "str"), ("iteration", "i32")]
    denorm = {"file_id", "fiscal_year", "tafs", "iteration"}
    for k in sorted(union_keys):
        if k in denorm:
            continue
        fields.append((k, "f64" if k in amount_cols else "str"))
    fields += [("line_kind", "str"), ("source", "str"), ("ingested_at", "ts")]
    return fields


def _footnotes_fields(union_keys: list[str]):
    fields = [("file_id", "i64"), ("fiscal_year", "i32"), ("tafs", "str"), ("iteration", "i32")]
    denorm = {"file_id", "fiscal_year", "tafs", "iteration"}
    for k in sorted(union_keys):
        if k in denorm:
            continue
        fields.append((k, "str"))
    fields += [("source", "str"), ("ingested_at", "ts")]
    return fields


def _doc_grain(path: str, doc: dict) -> dict:
    """Canonical per-document grain — payload-first, filename supplementary."""
    sd = doc.get("ScheduleData") or []
    fn = parse_filename(doc.get("FileName", ""))
    # iteration: payload is authoritative (100% fill); fall back to filename if ever absent
    payload_iters = {_s(r.get("Iteration")) for r in sd if _s(r.get("Iteration"))}
    if len(payload_iters) == 1:
        iteration = int(next(iter(payload_iters)))
    elif fn["fn_iteration"] is not None:
        iteration = fn["fn_iteration"]
    else:
        iteration = None
    # tafs: filename when it encodes one, else reconstruct from the doc's dominant account
    tafs = fn["tafs"]
    if not tafs and sd:
        accts = collections.Counter(
            (_s(r.get("CgacAgency")), _s(r.get("AvailabilityTypeCode")),
             _s(r.get("BeginPoa")), _s(r.get("EndPoa")), _s(r.get("CgacAcct")))
            for r in sd
        )
        (a, av, b, e, ac), _ = accts.most_common(1)[0]
        tafs = canon_tafs(a, av, b, e, ac)
    return {
        "file_id": int(doc["FileId"]) if _s(doc.get("FileId")) and str(doc["FileId"]).lstrip("-").isdigit() else None,
        "fiscal_year": int(_s(doc.get("FiscalYear"))) if _s(doc.get("FiscalYear")) and _s(doc.get("FiscalYear")).isdigit() else None,
        "tafs": tafs, "iteration": iteration,
        "agency_code": fn["agency"], "bureau_code": fn["bureau"], "fn_iteration": fn["fn_iteration"],
    }


def parse_and_write(links: list[str], local_dir: str, union: dict, so: dict,
                    write: set[str], *, smoke: bool) -> dict:
    """Pass 2 — build the three datasets from the frozen union schema. files + footnotes
    accumulate fully (small); lines stream to Lance in bounded chunks (~2–4M rows)."""
    ingested_at = dt.datetime.now(dt.timezone.utc)
    amount_cols = set(union["amount_cols"])
    primary_amount = ("approved_amount" if "approved_amount" in amount_cols
                      else (sorted(amount_cols)[0] if amount_cols else None))
    sd_union = list(union["sd_nonnull"].keys())
    fn_union = list(union["fn_keys"].keys())

    lines_fields = _lines_fields(sd_union, amount_cols)
    files_fields = _files_fields()
    fn_fields = _footnotes_fields(fn_union)
    lines_colnames = [f[0] for f in lines_fields]

    files_rows: list[dict] = []
    fn_rows: list[dict] = []
    lines_buf: list[dict] = []
    lines_written = 0
    lines_first = True

    # gate accumulators
    identity_fail: list[tuple] = []   # (file_name, Σbudgetary, Σapplication)
    iter_gate_checked = 0
    iter_gate_ok = 0
    iter_gate_mismatch: list[tuple] = []
    zero_line_docs: list[str] = []
    funds_dist = collections.Counter()
    fy_present = set()

    def flush_lines(force=False):
        nonlocal lines_written, lines_first, lines_buf
        if "lines" not in write:
            lines_buf = []
            return
        if not lines_buf or (len(lines_buf) < LINES_FLUSH_ROWS and not force):
            return
        tbl = _build_table(lines_buf, lines_fields)
        _write_lance(tbl, URIS["lines"], so, mode="overwrite" if lines_first else "append")
        lines_first = False
        lines_written += len(lines_buf)
        lines_buf = []

    for path, doc in _iter_docs(links, local_dir):
        g = _doc_grain(path, doc)
        sd = doc.get("ScheduleData") or []
        fd = doc.get("FootnoteData") or []
        if _s(doc.get("FiscalYear")):
            fy_present.add(_s(doc.get("FiscalYear")))
        funds_dist[_s(doc.get("FundsProvidedBy"))] += 1

        # files row
        files_rows.append({
            "file_id": g["file_id"], "file_name": _s(doc.get("FileName")),
            "fiscal_year": g["fiscal_year"], "approval_timestamp": _s(doc.get("ApprovalTimestamp")),
            "approval_ts": _parse_approval_ts(doc.get("ApprovalTimestamp")),
            "folder": _s(doc.get("Folder")), "approver_title": _s(doc.get("ApproverTitle")),
            "funds_provided_by": _s(doc.get("FundsProvidedBy")),
            "agency_code": g["agency_code"], "bureau_code": g["bureau_code"],
            "tafs": g["tafs"], "iteration": g["iteration"],
            "source_url": ORIGIN + path, "n_lines": len(sd), "n_footnotes": len(fd),
            "source": SOURCE_TAG, "ingested_at": ingested_at,
        })
        if len(sd) == 0:
            zero_line_docs.append(_s(doc.get("FileName")) or path)

        # §8 iteration gate — filename iteration must match payload where the filename has one
        if g["fn_iteration"] is not None and g["iteration"] is not None:
            iter_gate_checked += 1
            if g["fn_iteration"] == g["iteration"]:
                iter_gate_ok += 1
            else:
                iter_gate_mismatch.append((_s(doc.get("FileName")), g["fn_iteration"], g["iteration"]))

        # lines rows
        sum_budg = 0.0
        sum_appl = 0.0
        for row in sd:
            lk = _line_kind(row.get("LineNumber"))
            rec = {"file_id": g["file_id"], "fiscal_year": g["fiscal_year"],
                   "tafs": g["tafs"], "iteration": g["iteration"], "line_kind": lk,
                   "source": SOURCE_TAG, "ingested_at": ingested_at}
            # snake identically to discover_union (Iteration → schedule_iteration); no
            # ScheduleData key snakes to a denormalized doc-key name for this source, so the
            # denorm values set above are never shadowed by the row loop.
            for k, v in row.items():
                col = "schedule_iteration" if k == "Iteration" else _snake(k)
                rec[col] = _num(v) if col in amount_cols else _s(v)
            # keep only known columns (union is frozen; keys unseen at scale backfill null)
            lines_buf.append({c: rec.get(c) for c in lines_colnames})
            if primary_amount is not None:
                amt = rec.get(primary_amount) or 0.0
                if lk == "budgetary_resource":
                    sum_budg += amt
                elif lk == "application_of_resource":
                    sum_appl += amt

        if primary_amount is not None and sd and abs(sum_budg - sum_appl) > 1.0:
            if len(identity_fail) < 20:
                identity_fail.append((_s(doc.get("FileName")), round(sum_budg, 2), round(sum_appl, 2)))
            else:
                identity_fail.append(("...", 0, 0))

        # footnotes rows
        for fr in fd:
            rec = {"file_id": g["file_id"], "fiscal_year": g["fiscal_year"],
                   "tafs": g["tafs"], "iteration": g["iteration"],
                   "source": SOURCE_TAG, "ingested_at": ingested_at}
            for k, v in (fr or {}).items():
                rec[_snake(k)] = _s(v)   # footnote text verbatim, never truncated
            fn_rows.append(rec)

        flush_lines(force=False)

    flush_lines(force=True)

    # ── write files + footnotes (small; single overwrite each) ──────────────────────
    written = {}
    if "files" in write:
        _write_lance(_build_table(files_rows, files_fields), URIS["files"], so, mode="overwrite")
        written["files"] = len(files_rows)
    if "footnotes" in write:
        # ensure a non-empty schema even if zero footnotes in a smoke slice
        tbl = _build_table(fn_rows, fn_fields)
        _write_lance(tbl, URIS["footnotes"], so, mode="overwrite")
        written["footnotes"] = len(fn_rows)
    if "lines" in write:
        if lines_written == 0:  # nothing flushed (e.g. empty) — write an empty typed dataset
            _write_lance(_build_table([], lines_fields), URIS["lines"], so, mode="overwrite")
        written["lines"] = lines_written

    return {
        "written": written, "files_rows": len(files_rows), "lines_rows": lines_written,
        "fn_rows": len(fn_rows),
        "identity_fail": identity_fail, "iter_gate_checked": iter_gate_checked,
        "iter_gate_ok": iter_gate_ok, "iter_gate_mismatch": iter_gate_mismatch,
        "zero_line_docs": zero_line_docs, "funds_dist": funds_dist, "fy_present": fy_present,
        "primary_amount": primary_amount, "lines_fields": lines_fields,
    }


# ── gates ─────────────────────────────────────────────────────────────────────────────
def run_gates(links, crawl_res, union, parse_res, *, smoke: bool) -> None:
    n = len(links)
    failed = len(crawl_res["failed_paths"])
    ratio = failed / max(1, n)
    if not smoke and ratio > MAX_FAIL_RATIO:
        raise RuntimeError(f"GATE files_failed/total {failed}/{n}={ratio:.4f} > {MAX_FAIL_RATIO}; "
                           f"failed paths: {crawl_res['failed_paths'][:20]}")
    fy_present = {int(y) for y in parse_res["fy_present"] if y and y.isdigit()}
    if not smoke and not REQUIRED_FYS <= fy_present:
        raise RuntimeError(f"GATE missing fiscal years in parsed data: {sorted(REQUIRED_FYS - fy_present)}")
    if "lines" in parse_res.get("written", {}):
        # exact completeness — every discovered schedule line was written (no drops). This is
        # the precise form of the directive's "< 1M implies a parse that dropped rows" intent.
        if parse_res["lines_rows"] != union["sd_rows"]:
            raise RuntimeError(f"GATE lines written {parse_res['lines_rows']} != discovered "
                               f"{union['sd_rows']} — parse dropped rows")
        if not smoke and union["sd_rows"] < MIN_SCHEDULE_LINES:
            raise RuntimeError(f"GATE schedule lines {union['sd_rows']} < {MIN_SCHEDULE_LINES} "
                               f"(sanity floor; confirmed count ~515,777)")
    if parse_res["zero_line_docs"]:
        raise RuntimeError(f"GATE {len(parse_res['zero_line_docs'])} document(s) with 0 schedule "
                           f"lines (parser bug): {parse_res['zero_line_docs'][:10]}")
    if parse_res["iter_gate_mismatch"]:
        raise RuntimeError(f"GATE filename↔payload iteration mismatch on "
                           f"{len(parse_res['iter_gate_mismatch'])} docs: "
                           f"{parse_res['iter_gate_mismatch'][:10]}")
    if parse_res["identity_fail"]:
        raise RuntimeError(f"GATE SF-132 identity Σ(budgetary)≠Σ(application) on "
                           f"{len(parse_res['identity_fail'])} docs: {parse_res['identity_fail'][:10]}")
    if not union["amount_cols"]:
        raise RuntimeError("GATE no amount column discovered (numeric>98% + money-ish name)")
    # post-discovery amount-cast assertion (§8): the primary amount parses numeric >98%
    pa_col = parse_res["primary_amount"]
    nn = union["sd_nonnull"].get(pa_col, 0)
    num = union["sd_numeric"].get(pa_col, 0)
    if nn == 0 or num / nn <= 0.98:
        raise RuntimeError(f"GATE amount-cast: {pa_col} numeric on {num}/{nn} non-null (<=98%)")
    # §2.4 discovery coverage
    if not smoke and (union["n_docs"] < 50 or len(union["agencies"]) < 3 or len(union["fys"]) < 3):
        raise RuntimeError(f"GATE key-union discovery coverage: docs={union['n_docs']} "
                           f"agencies={len(union['agencies'])} fys={len(union['fys'])}")
    print(f"[gates] PASS (smoke={smoke}) files_failed={failed}/{n} lines={parse_res['lines_rows']} "
          f"iter_gate={parse_res['iter_gate_ok']}/{parse_res['iter_gate_checked']} "
          f"identity_ok amount_col={pa_col}", flush=True)


# ── orchestration ─────────────────────────────────────────────────────────────────────
def build_indexes(write: set[str], so: dict) -> dict:
    built = {}
    if "files" in write:
        built["files"] = _build_indexes(URIS["files"], btree=["fiscal_year", "tafs", "iteration"],
                                        bitmap=["agency_code"], so=so)
    if "lines" in write:
        built["lines"] = _build_indexes(URIS["lines"],
                                        btree=["fiscal_year", "tafs", "iteration", "line_number"],
                                        bitmap=["line_kind"], so=so)
    if "footnotes" in write:
        built["footnotes"] = _build_indexes(URIS["footnotes"],
                                            btree=["fiscal_year", "tafs", "iteration"],
                                            bitmap=[], so=so)
    return built


def write_run_record(union, parse_res, crawl_res, per_fy, built) -> str:
    """Machine-readable run record (FundsProvidedBy distribution + key-union table + line_kind
    mapping + gate results). Written under docs/reference/ per the directive Surfaces table."""
    funds = parse_res["funds_dist"]
    pl_119_21 = [v for v in funds if v and "119-21" in v]
    lines = []
    lines.append("# OMB Apportionment ingest — run record")
    lines.append("")
    lines.append(f"generated_at: {dt.datetime.now(dt.timezone.utc).isoformat()}")
    lines.append(f"index_link_count: {sum(per_fy.values())}  per_fy: {per_fy}")
    lines.append(f"files_fetched: {crawl_res['fetched']}  files_failed: {len(crawl_res['failed_paths'])}")
    lines.append(f"rows: files={parse_res['files_rows']} lines={parse_res['lines_rows']} "
                 f"footnotes={parse_res['fn_rows']}")
    lines.append(f"amount_cols: {union['amount_cols']}  primary: {parse_res['primary_amount']}")
    lines.append(f"line_kind mapping: 1000–1999→budgetary_resource, 6000–6999→application_of_resource, "
                 f"else→marker (validated by SF-132 identity gate)")
    lines.append(f"discovery coverage: docs={union['n_docs']} agencies={len(union['agencies'])} "
                 f"fys={union['fys']}")
    lines.append(f"iteration gate: {parse_res['iter_gate_ok']}/{parse_res['iter_gate_checked']} "
                 f"filenames encode iteration and all match payload")
    lines.append(f"indexes: {built}")
    lines.append("")
    lines.append("## ScheduleData key union (fill / numeric of non-null)")
    lines.append("| key | non_null_rows | numeric_rows | numeric_pct |")
    lines.append("|---|---:|---:|---:|")
    for k in sorted(union["sd_nonnull"], key=lambda x: -union["sd_nonnull"][x]):
        nn = union["sd_nonnull"][k]
        nm = union["sd_numeric"].get(k, 0)
        lines.append(f"| {k} | {nn} | {nm} | {100*nm/max(1,nn):.1f}% |")
    lines.append("")
    lines.append("## FootnoteData key union")
    for k, c in sorted(union["fn_keys"].items(), key=lambda kv: -kv[1]):
        lines.append(f"- {k}: {c}")
    lines.append("")
    lines.append(f"## FundsProvidedBy — {len(funds)} distinct values across {sum(funds.values())} files")
    lines.append(f"**P.L. 119-21 (OBBA) present: {'YES — ' + str(pl_119_21) if pl_119_21 else 'NO'}**")
    lines.append("")
    lines.append("Top 30 by file count:")
    lines.append("| count | FundsProvidedBy |")
    lines.append("|---:|---|")
    for v, c in funds.most_common(30):
        lines.append(f"| {c} | {v!r} |")
    body = "\n".join(lines) + "\n"

    out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                           "docs", "reference")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "omb_apportionment_run_record.md")
    with open(path, "w") as fh:
        fh.write(body)
    print(f"[record] wrote {path}", flush=True)
    print(f"[record] FundsProvidedBy distinct={len(funds)} P.L.119-21={'YES' if pl_119_21 else 'NO'}",
          flush=True)
    return path


def main() -> None:
    import requests

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--stream", required=True,
                    choices=["index", "files", "schedule", "footnotes", "all"])
    ap.add_argument("--smoke", action="store_true",
                    help="first 50 files only, throwaway smoke/ URIs")
    args = ap.parse_args()

    so = _storage_options()
    smoke = args.smoke
    uris = dict(URIS)
    if smoke:
        for k in uris:
            uris[k] = uris[k].replace("/active/", "/smoke/")
    # bind module URIS to smoke for _write_lance/_build_indexes callers
    URIS.update(uris)

    _ensure_ledger_and_catalog()
    run_id = _ledger_start(args.stream)

    stream_to_ds = {"files": {"files"}, "schedule": {"lines"}, "footnotes": {"footnotes"},
                    "all": {"files", "lines", "footnotes"}}
    write = stream_to_ds.get(args.stream, set())

    gov = RateGovernor()  # binding defaults: ≤2 req/s, ≤3 workers, warm-up 100@1/s, breaker 300s
    sess = requests.Session()

    status, disposition, notes = "failed", None, ""
    per_fy, crawl_res, union, parse_res, built = {}, {}, {}, {}, {}
    import tempfile
    local_dir = os.path.join(tempfile.gettempdir(), "omb_apportionment_cache")
    os.makedirs(local_dir, exist_ok=True)

    try:
        links = fetch_index(gov, sess)
        per_fy = gate_index(links)
        _r2_put(WORKLIST_KEY, json.dumps(links).encode())
        if smoke:
            links = links[:SMOKE_N]

        if args.stream == "index":
            status, notes = "completed", f"index only; {len(links)} links; per_fy={per_fy}"
            print(f"[index] stream=index complete; {sum(per_fy.values())} links", flush=True)
            return

        checkpoint = PathCheckpoint(R2CheckpointStore(
            CHECKPOINT_KEY if not smoke else CHECKPOINT_KEY.replace("/cache/", "/cache_smoke/")))
        crawl_res = crawl(gov, links, checkpoint, local_dir, smoke=smoke)
        if crawl_res["throttled"]:
            status, disposition = "failed", "throttled"
            notes = "circuit breaker second trip — crawl halted; re-run resumes from checkpoint"
            raise SystemExit(f"THROTTLED: {notes}")

        union = discover_union(links, local_dir)
        print(f"[discover] docs={union['n_docs']} sd_rows={union['sd_rows']} "
              f"sd_keys={len(union['sd_nonnull'])} fn_keys={len(union['fn_keys'])} "
              f"amount_cols={union['amount_cols']} agencies={len(union['agencies'])} "
              f"fys={union['fys']}", flush=True)

        parse_res = parse_and_write(links, local_dir, union, so, write, smoke=smoke)
        run_gates(links, crawl_res, union, parse_res, smoke=smoke)
        built = build_indexes(write, so)

        record = None
        if args.stream == "all":
            record = write_run_record(union, parse_res, crawl_res, per_fy, built)

        status = "completed"
        pl = [v for v in parse_res["funds_dist"] if v and "119-21" in v]
        notes = (f"stream={args.stream} smoke={smoke} written={parse_res['written']} "
                 f"amount_col={parse_res['primary_amount']} funds_distinct={len(parse_res['funds_dist'])} "
                 f"pl_119_21={'YES:'+str(pl) if pl else 'NO'} record={record}")
        print(f"\n=== RESULT === {notes}", flush=True)
    except ThrottledError:
        status, disposition = "failed", "throttled"
        notes = "ThrottledError surfaced to main — re-run resumes from checkpoint"
        raise
    finally:
        datasets = {k: (parse_res.get("written", {}) or {}).get(k) for k in write}
        rows_written = sum(v for v in (parse_res.get("written", {}) or {}).values() if v) or 0
        _ledger_finish(run_id, status=status, disposition=disposition,
                       index_link_count=sum(per_fy.values()) if per_fy else None,
                       files_fetched=crawl_res.get("fetched") if crawl_res else None,
                       files_failed=len(crawl_res.get("failed_paths", [])) if crawl_res else None,
                       rows_written=rows_written, datasets=datasets, notes=notes)


if __name__ == "__main__":
    main()
