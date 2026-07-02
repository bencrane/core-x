"""SAM.gov *wage determination* metadata manifest — local/in-session runner.

Builds the current-WD reference layer: one row per active wage determination
(SCA / DBA / CBA) plus a companion (wd, state, county) coverage explosion, so
the ~10,055 currently-active WDs are queryable by reference number, type, and
geography before any per-WD rate document is fetched.

WHY THE sam.gov FRONTEND (not api.sam.gov, no SAM_API_KEY): the developer
gateway ``api.sam.gov`` (api.data.gov) caps non-federal keys at a tiny per-day
quota — useless for a full-catalog crawl. The PUBLIC website backend
``sam.gov/api/prod/sgs/v1/search`` — the same unauthenticated origin the browser
itself calls — serves the WD search index with **no api_key and no developer
quota**. Confirmed key-less from a RESIDENTIAL IP (this runner's host); the WAF
is lenient on this path at the polite serial pace used here.

    GET https://sam.gov/api/prod/sgs/v1/search?index=wd&is_active=true&q={slice}&page=0&size={n}
        -> _embedded.results[] { _id, type:{code:SCA|DBA|CBA}, fullReferenceNumber,
           shortReferenceNumber, revisionNumber, isActive, isLatest,
           publishDate, modifiedDate, location:{...}, ... }
           pagination under j["page"] { size, totalElements, totalPages, number,
           maxAllowedRecords }

Header note: the backend serves ``application/hal+json`` and 406s a strict
``Accept: application/json`` — must send ``application/json, text/plain, */*``.
Origin ``https://sam.gov`` and Referer ``https://sam.gov/wage-determination``.

TARGET / SCOPE: ALL 10,055 CURRENTLY-ACTIVE WDs (is_active=true). Historical
revisions (the 97,013 is_active=false records, 107,068 total) are NOT wanted —
is_active=true is already latest-only per (fullReferenceNumber, type); there are
NO superseded revisions inside the active set to collapse.

THE OFFSET-CEILING PROBLEM (and the proven workaround). The search index is an
Elasticsearch window capped at ``page.maxAllowedRecords=10000`` — any request
with ``(page*size)+size > 10000`` returns HTTP 400 ("result window that is too
large"). Since the active set is 10,055 > 10,000, naive offset paging of the
whole ``is_active=true`` query SILENTLY DROPS the last 55 active SCA WDs and can
never reach them. No sort/search_after cursor exists (every sort variant is
ignored) and no server-side type facet works (type=/typeCode=/... are all
ignored). The ONLY reducing filters are ``is_active``, ``is_latest``, ``state``,
and the FUZZY full-text ``q=``.

WORKAROUND (proven live to yield exactly 10,055 distinct _id, gap 0): partition
the query with ``q=`` into the three type slices — q=SCA (~1,521), q=DBA
(~4,236), q=CBA (~4,302) — each individually under the 10,000 wall, and fetch
EACH IN A SINGLE PAGE (page=0, size==that slice's totalElements). Offset-paging
the fuzzy ``q=`` query is UNSAFE (unstable tie-order drops ~10% of rows across
page boundaries); the single-page-per-slice fetch has 0 internal duplicates.
``q=`` is fuzzy, so slices overlap slightly (q=CBA pulls a handful of SCA rows):
the union is deduped on ``_id`` (universal key; fullReferenceNumber is NULL for
all CBA so it CANNOT be the cross-type key). RECONCILIATION: the run asserts the
distinct-_id count against the live ``is_active=true`` totalElements and
FAILS CLOSED on any gap before writing Lance.

LOCATION HAS THREE INCOMPATIBLE SHAPES (the county explode branches on type.code):
    SCA:  location.states[]{ isStateWide, code, name, counties:{include:[{code,value}], exclude:[]} }
    DBA:  location.state{}  SINGULAR one state: { code, name, counties:[{code,value}] }
    CBA:  location.states[]{ code, name, counties:[{code,value}] }   (flat, no include/exclude)
~25/10,000 records are nationwide (NO state) — they are kept (0 rows in the
explode, 1 row in the manifest), never dropped.

STAGE 2 — PER-WD RATE DOCUMENT (implemented; --rates). The rate tables
(labor-category → wage/fringe) live in a plaintext ``document`` field reachable
KEY-LESS at:

    GET https://sam.gov/api/prod/wdol/v1/wd/{fullReferenceNumber}/{revisionNumber}
        -> 200 application/hal+json { document (the rate register text),
           location:{mapping[...]}, year, publishDate, active, standard,
           constructionType (DBA only), _links }
    (revisionNumber is a REQUIRED path segment — omitting it 404s; the wrong
     revision 404s. Both inputs come DIRECTLY off each manifest row:
     _id == fullReferenceNumber and revision_number.)

Works for SCA + DBA only (5,757 of the 10,055 active WDs). CBA
(fullReferenceNumber=null) is NOT addressable this way — a §4(c) CBA WD has no
WHD rate register at all. It resolves via /wdol/v1/cba/{numeric _id}, which is
KEY-LESS (verified 200 from this host, NOT X-Auth-Token gated as earlier notes
claimed) but returns POINTER metadata — the employer, union, agency, locality and
effective dates that CITE the governing collective bargaining agreement — not a
rate table. The wage/fringe schedule lives in that external union contract;
SAM.gov hosts no copy (every /cba/{id}/{attachments,files,document,download}
sub-path 404s). The rate stage (run_rate_stage) is a resume-safe append crawl into
``sam_wd_rate_documents`` landing the LOSSLESS ``document`` string verbatim; the
CBA pointer stage (run_cba_stage, --cba) is the analogous key-less crawl into
``sam_wd_cba_pointers`` (+ a ``sam_wd_cba_coverage`` location explode). Parsing
either to structured rows is a further downstream dataset (raw-stays-lossless).

Runs LOCAL/in-session from a residential IP. STAGE 1 (--run) is ~3 GET calls
(the whole active catalog at slice-size pages); STAGE 2 (--rates) is one GET per
SCA/DBA WD (~5,757, bounded-concurrency polite crawl with WAF backoff). Creds for
the R2 Lance write via Doppler, deps via uv. No SAM_API_KEY.

Output (Lance v2.1, Gen-3 active/ SoR):
    s3://data-sink/active/sam_wage_determinations/    one row per WD (_id grain)        [--run]
    s3://data-sink/active/sam_wd_county_coverage/     one row per (wd, state, county)   [--run]
    s3://data-sink/active/sam_wd_rate_documents/      one row per SCA/DBA WD rate doc   [--rates]
    s3://data-sink/active/sam_wd_cba_pointers/        one row per CBA WD (pointer)      [--cba]
    s3://data-sink/active/sam_wd_cba_coverage/        one row per (cba wd, location)    [--cba]

    doppler run --project core-x --config prd -- \\
      uv run --with pylance --with pyarrow --with requests --with 'psycopg[binary]' \\
      python pipelines/sam_gov/sam_wd_manifest.py --run             # stage 1: metadata catalog
    ... python pipelines/sam_gov/sam_wd_manifest.py --rates --resume  # stage 2: SCA/DBA rate docs
    ... python pipelines/sam_gov/sam_wd_manifest.py --cba   --resume  # stage 3: CBA pointer records

Smoke (cap rows, throwaway URIs):
    ... --run   --limit 50 --wd-uri .../_smoke_sam_wd/ --county-uri .../_smoke_sam_wd_county/
    ... --rates --limit 5  --rate-uri .../_smoke_sam_wd_rates/
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time

# Reuse the fleet R2/object-store + index plumbing verbatim (all module-level,
# importable). _record_run in bls.ingest is BLS-specific (positional, ops.bls_runs)
# — a WD-local dict-based _record_run is defined below instead.
from pipelines.bls.ingest import (  # noqa: E402
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _storage_options,
)

BUCKET = "data-sink"

WD_URI = os.environ.get("SAM_WD_LANCE_URI", f"s3://{BUCKET}/active/sam_wage_determinations/")
COUNTY_URI = os.environ.get(
    "SAM_WD_COUNTY_LANCE_URI", f"s3://{BUCKET}/active/sam_wd_county_coverage/")

SEARCH_URL = "https://sam.gov/api/prod/sgs/v1/search"
# Rate-document endpoint (pinned, verified key-less). {ref}={fullReferenceNumber},
# {rev}={revision_number}; SCA/DBA only; revision segment is REQUIRED (wrong/absent rev 404s).
# RATE_DOC_URI + RATE_SOURCE are defined in the rate-stage section below.
RATE_URL = "https://sam.gov/api/prod/wdol/v1/wd/{ref}/{rev}"

FEED = "sam_wage_determinations"
SOURCE = "sam.gov/api/prod/sgs/v1/search (frontend, no api_key)"

# The three type slices used to dodge the offset>=10000 wall. Each q= value is a
# FUZZY full-text query, NOT a clean facet — slices overlap slightly and are
# authoritatively (re)labeled client-side on record type.code. Union deduped on _id.
TYPE_SLICES = ("SCA", "DBA", "CBA")

# Raw search-index records for the winning (deduped) _ids, keyed by _id. Populated
# during the dedup pass so the county explode can read each record's raw `location`
# (which the flattened manifest row drops). Reset at the top of every run_harvest.
_RAW_BY_ID: dict[str, dict] = {}

# Hard Elasticsearch window: (page*size)+size must be <= this. Read live off
# page.maxAllowedRecords too, but pinned here as the fail-closed default.
MAX_WINDOW = 10_000


# ── WD-local ops ledger (dict-based, modeled on sam_attachment_manifest._record_run) ──
def _record_run(stats: dict, dsn: str | None) -> None:
    if not dsn:
        print("WARN: no HQX_DB_URL_POOLED; skipping ops.* write.", flush=True)
        return
    try:
        import psycopg

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ops.sam_wage_determination_runs (
                    id bigserial PRIMARY KEY, feed text NOT NULL, status text NOT NULL,
                    active_total int, wd_rows int, county_rows int,
                    sca int, dba int, cba int,
                    stateless_wds int, dedup_dropped int, api_calls int,
                    wd_uri text, county_uri text, indexes_built text[],
                    stats jsonb, error text,
                    started_at timestamptz, completed_at timestamptz,
                    recorded_at timestamptz NOT NULL DEFAULT now())
                """
            )
            cur.execute(
                """
                INSERT INTO ops.sam_wage_determination_runs
                  (feed,status,active_total,wd_rows,county_rows,sca,dba,cba,
                   stateless_wds,dedup_dropped,api_calls,wd_uri,county_uri,
                   indexes_built,stats,error,started_at,completed_at)
                VALUES (%(feed)s,%(status)s,%(active_total)s,%(wd_rows)s,%(county_rows)s,
                   %(sca)s,%(dba)s,%(cba)s,%(stateless_wds)s,%(dedup_dropped)s,
                   %(api_calls)s,%(wd_uri)s,%(county_uri)s,%(indexes_built)s,
                   %(stats)s,%(error)s,%(started_at)s,%(completed_at)s)
                """,
                {**stats, "stats": json.dumps(stats.get("stats", {}))},
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must never mask a good crawl
        print(f"WARN: ops.* write failed: {exc}", flush=True)


def _headers() -> dict:
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
        "Accept": "application/json, text/plain, */*",  # hal+json 406s a strict json Accept
        "Origin": "https://sam.gov",
        "Referer": "https://sam.gov/wage-determination",
    }


# ── field coercion helpers ────────────────────────────────────────────────────────
def _s(v) -> str | None:
    if v is None:
        return None
    v = str(v).strip()
    return v or None


def _as_iso(v) -> str | None:
    """publishDate is epoch-millis (int) for SCA/DBA, ISO string for CBA. Normalize
    both to an ISO-8601 UTC string; leave anything unparseable as its str form."""
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        try:
            return dt.datetime.fromtimestamp(v / 1000.0, dt.timezone.utc).isoformat()
        except (OSError, OverflowError, ValueError):
            return str(v)
    return _s(v)


def _bool_or_none(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1"):
        return True
    if s in ("false", "0"):
        return False
    return None


def _explode_location(rec: dict, wd_id: str, type_code: str) -> list[dict]:
    """Flatten location -> [{wd_id, state_code, state_name, county_code, county_name}].

    Branches on type.code — the three shapes are incompatible:
      SCA:  states[].counties.{include:[{code,value}], exclude:[]}
      DBA:  state{} SINGULAR .counties[]{code,value}
      CBA:  states[].counties[]{code,value}
    Stateless (nationwide) records yield 0 rows here (kept as 1 manifest row).
    A statewide entry with no counties yields one (state, county=None) row so the
    state coverage is still represented.
    """
    loc = rec.get("location") or {}
    out: list[dict] = []

    def emit(state: dict) -> None:
        if not state:
            return
        sc = _s(state.get("code"))
        sn = _s(state.get("name"))
        counties = state.get("counties")
        pairs: list[dict] = []
        if isinstance(counties, dict):          # SCA include/exclude object
            pairs = list(counties.get("include") or [])
        elif isinstance(counties, list):        # DBA / CBA flat array
            pairs = counties
        if not pairs:
            out.append({"wd_id": wd_id, "state_code": sc, "state_name": sn,
                        "county_code": None, "county_name": None})
            return
        for c in pairs:
            if not isinstance(c, dict):
                continue
            out.append({"wd_id": wd_id, "state_code": sc, "state_name": sn,
                        "county_code": _s(c.get("code")), "county_name": _s(c.get("value"))})

    if type_code == "DBA":
        emit(loc.get("state") or {})           # singular 'state'
    else:                                       # SCA + CBA use plural 'states'
        for st in (loc.get("states") or []):
            if isinstance(st, dict):
                emit(st)
    return out


def _flatten(rec: dict) -> dict | None:
    """One search-index record -> one manifest row (or None if it lacks a key _id)."""
    wd_id = _s(rec.get("_id"))
    if not wd_id:
        return None
    tc = (((rec.get("type") or {}).get("code")) or "").strip().upper() or None
    return {
        "_id": wd_id,
        "full_reference_number": _s(rec.get("fullReferenceNumber")),
        "short_reference_number": _s(rec.get("shortReferenceNumber")),
        "cba_number": _s(rec.get("cbaNumber")),
        "wd_type": tc,
        "type_code": tc,
        "type_value": _s((rec.get("type") or {}).get("value")),
        "title": _s(rec.get("title")),
        "revision_number": (int(rec["revisionNumber"])
                            if str(rec.get("revisionNumber", "")).strip().lstrip("-").isdigit()
                            else None),
        "is_active": _bool_or_none(rec.get("isActive")),
        "is_latest": _bool_or_none(rec.get("isLatest")),
        "publish_date": _as_iso(rec.get("publishDate")),
        "modified_date": _s(rec.get("modifiedDate")),
        "indexed_date": _s(rec.get("indexedDate")),
    }


def run_harvest(
    *,
    storage_options: dict,
    dsn: str | None,
    wd_uri: str = WD_URI,
    county_uri: str = COUNTY_URI,
    size: int = 0,
    limit: int = 0,
    inter_call_sleep: float = 0.5,
    resume: bool = False,
) -> dict:
    import lance
    import pyarrow as pa
    import requests

    started_at = dt.datetime.now(dt.timezone.utc)
    crawled_at = started_at
    session = requests.Session()
    calls = {"n": 0}
    _RAW_BY_ID.clear()

    # ── one guarded GET (WAF backoff on 403/429/503, network/5xx retry) ────────────
    def fetch(params: dict):
        for attempt in range(6):
            calls["n"] += 1
            try:
                resp = session.get(SEARCH_URL, params=params, headers=_headers(), timeout=60)
            except requests.RequestException:
                time.sleep(min(30, 2 ** attempt)); continue
            sc = resp.status_code
            if sc == 200:
                return resp.json()
            if sc in (403, 429, 503):           # WAF / throttle → back off and retry
                w = min(120, 5 * 2 ** attempt)
                print(f"  {sc} on q={params.get('q')}; backoff {w}s (call#{calls['n']})", flush=True)
                time.sleep(w); continue
            if sc >= 500:
                time.sleep(min(30, 2 ** attempt)); continue
            # 400 (window too large) / 406 (header bug) / other → surface, do not retry
            body = (resp.text or "")[:200]
            raise RuntimeError(f"HTTP {sc} on q={params.get('q')} size={params.get('size')}: {body}")
        raise RuntimeError(f"exhausted retries on q={params.get('q')}")

    def totals(q: str | None) -> int:
        """Live totalElements for a slice (or the whole active set when q is None)."""
        p = {"index": "wd", "is_active": "true", "page": 0, "size": 1}
        if q:
            p["q"] = q
        j = fetch(p)
        return int(((j or {}).get("page") or {}).get("totalElements") or 0)

    # ── ground-truth active count for fail-closed reconciliation ───────────────────
    active_total = totals(None)
    print(f"active_total (is_active=true totalElements) = {active_total}", flush=True)

    # ── accumulate one row per _id across the three fuzzy type slices ──────────────
    by_id: dict[str, dict] = {}
    dedup_dropped = 0
    raw_seen = 0

    for slice_q in TYPE_SLICES:
        n = totals(slice_q)
        # single-page-per-slice: page=0, size==totalElements (proven drift-free).
        # Guard against the 10k window (each slice is < 10k, but stay defensive) and
        # honor --limit for smoke runs.
        want = min(n, MAX_WINDOW)
        if size:
            want = min(want, size)
        if limit:
            want = min(want, limit)
        params = {"index": "wd", "is_active": "true", "q": slice_q, "page": 0, "size": want}
        j = fetch(params)
        page = (j or {}).get("page") or {}
        max_allowed = int(page.get("maxAllowedRecords") or MAX_WINDOW)
        if want + 0 > max_allowed:              # (page*size)+size, page=0
            raise RuntimeError(f"slice q={slice_q} size={want} exceeds maxAllowedRecords={max_allowed}")
        results = ((j or {}).get("_embedded") or {}).get("results") or []
        got = len(results)
        print(f"slice q={slice_q}: totalElements={n} fetched={got}", flush=True)
        if not limit and not size and got < min(n, max_allowed):
            raise RuntimeError(
                f"slice q={slice_q} under-fetched: got {got} < expected {min(n, max_allowed)} "
                "(pagination gap — fail closed)")
        for rec in results:
            raw_seen += 1
            row = _flatten(rec)
            if row is None:
                continue
            wid = row["_id"]
            if wid in by_id:
                # first-seen wins; prefer the row whose own type.code matches the slice
                if by_id[wid].get("type_code") != slice_q and row.get("type_code") == slice_q:
                    by_id[wid] = row
                    _RAW_BY_ID[wid] = rec
                dedup_dropped += 1
                continue
            by_id[wid] = row
            _RAW_BY_ID[wid] = rec       # retain raw record for the county explode
        if inter_call_sleep:
            time.sleep(inter_call_sleep)

    distinct = len(by_id)
    print(f"raw_rows={raw_seen} distinct_id={distinct} dedup_dropped={dedup_dropped}", flush=True)

    # ── FAIL CLOSED on any reconciliation gap (unless smoke-capped) ────────────────
    if not limit and not size and distinct != active_total:
        raise RuntimeError(
            f"RECONCILIATION FAILED: distinct _id={distinct} != active_total={active_total} "
            f"(gap {active_total - distinct}). Refusing to write a partial catalog.")

    # ── build the two output tables ────────────────────────────────────────────────
    # The county explode needs each winning record's raw `location`, retained in
    # _RAW_BY_ID during the dedup pass above (the flattened row drops it).
    wd_rows = sorted(by_id.values(), key=lambda r: (r["type_code"] or "", r["_id"]))

    county_rows: list[dict] = []
    stateless = 0
    for r in wd_rows:
        raw = _RAW_BY_ID.get(r["_id"])
        exploded = _explode_location(raw or {}, r["_id"], r["type_code"] or "") if raw else []
        if not exploded:
            stateless += 1
        county_rows.extend(exploded)

    # provenance
    for r in wd_rows:
        r["source"] = SOURCE
        r["crawled_at"] = crawled_at
    for cr in county_rows:
        cr["source"] = SOURCE
        cr["crawled_at"] = crawled_at

    type_counts = {t: sum(1 for r in wd_rows if r["type_code"] == t) for t in TYPE_SLICES}
    print(f"type_breakdown={type_counts} county_rows={len(county_rows)} stateless_wds={stateless}",
          flush=True)

    wd_schema = pa.schema([
        ("_id", pa.string()),
        ("full_reference_number", pa.string()),
        ("short_reference_number", pa.string()),
        ("cba_number", pa.string()),
        ("wd_type", pa.string()),
        ("type_code", pa.string()),
        ("type_value", pa.string()),
        ("title", pa.string()),
        ("revision_number", pa.int32()),
        ("is_active", pa.bool_()),
        ("is_latest", pa.bool_()),
        ("publish_date", pa.string()),
        ("modified_date", pa.string()),
        ("indexed_date", pa.string()),
        ("source", pa.string()),
        ("crawled_at", pa.timestamp("us", tz="UTC")),
    ])
    county_schema = pa.schema([
        ("wd_id", pa.string()),
        ("state_code", pa.string()),
        ("state_name", pa.string()),
        ("county_code", pa.string()),
        ("county_name", pa.string()),
        ("source", pa.string()),
        ("crawled_at", pa.timestamp("us", tz="UTC")),
    ])

    final_status, error_text, built = "error", None, []
    try:
        wd_tbl = pa.Table.from_pylist(wd_rows, schema=wd_schema)
        lance.write_dataset(
            wd_tbl, wd_uri, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=storage_options,
        )
        print(f"wrote {wd_tbl.num_rows} WD rows -> {wd_uri}", flush=True)

        county_tbl = pa.Table.from_pylist(county_rows, schema=county_schema)
        lance.write_dataset(
            county_tbl, county_uri, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=storage_options,
        )
        print(f"wrote {county_tbl.num_rows} county rows -> {county_uri}", flush=True)

        # indexes (idempotent replace=True; missing col warns, never fails)
        built += [f"wd:{b}" for b in _build_indexes(
            wd_uri,
            btree=["_id", "full_reference_number", "short_reference_number"],
            bitmap=["type_code", "is_active", "is_latest", "revision_number"],
            so=storage_options,
        )]
        built += [f"county:{b}" for b in _build_indexes(
            county_uri,
            btree=["wd_id", "state_code", "county_code"],
            bitmap=[],
            so=storage_options,
        )]
        final_status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc)
        print(f"FATAL: {exc}", flush=True)
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        stats = {
            "feed": FEED, "status": final_status, "active_total": active_total,
            "wd_rows": len(wd_rows), "county_rows": len(county_rows),
            "sca": type_counts["SCA"], "dba": type_counts["DBA"], "cba": type_counts["CBA"],
            "stateless_wds": stateless, "dedup_dropped": dedup_dropped,
            "api_calls": calls["n"], "wd_uri": wd_uri, "county_uri": county_uri,
            "indexes_built": built, "error": error_text,
            "started_at": started_at, "completed_at": completed_at,
            "stats": {"source": SOURCE, "type_slices": list(TYPE_SLICES),
                      "reconciliation": ("bypassed(smoke)" if (limit or size)
                                         else f"{distinct}=={active_total}"),
                      "smoke_limit": limit, "size_cap": size},
        }
        _record_run(stats, dsn)
        print("SUMMARY:", {k: v for k, v in stats.items() if k != "stats"}, flush=True)

    return {"status": final_status, "wd_rows": len(wd_rows), "county_rows": len(county_rows),
            "active_total": active_total, "dedup_dropped": dedup_dropped,
            "api_calls": calls["n"], "indexes": built}


# ─────────────────────────────────────────────────────────────────────────────────
# PER-WD RATE DOCUMENT STAGE (pinned + verified key-less; SCA + DBA only)
# ─────────────────────────────────────────────────────────────────────────────────
# GET https://sam.gov/api/prod/wdol/v1/wd/{full_reference_number}/{revision_number}
#     -> 200 hal+json { document (plaintext rate register — the payload), year,
#        publishDate, active, standard, constructionType (DBA), location:{mapping} }
# revision_number is a REQUIRED path segment (both inputs are columns on the manifest;
# _id == full_reference_number). CBA WDs (fullReferenceNumber=null) have NO rate doc;
# they are harvested by run_cba_stage (--cba) off the KEY-LESS /wdol/v1/cba/{_id}
# POINTER endpoint into sam_wd_cba_pointers (see that stage).
# This stage lands the LOSSLESS `document` string; parsing it to structured
# (occupation, rate, fringe) rows is a further downstream dataset, not this one.
RATE_DOC_URI = os.environ.get("SAM_WD_RATE_LANCE_URI", f"s3://{BUCKET}/active/sam_wd_rate_documents/")
RATE_SOURCE = "sam.gov/api/prod/wdol/v1/wd (frontend, no api_key)"


def _dataset_exists(uri: str, so: dict) -> bool:
    import lance
    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001
        return False


def _rate_schema():
    import pyarrow as pa
    return pa.schema([
        ("wd_id", pa.string()), ("full_reference_number", pa.string()),
        ("revision_number", pa.int32()), ("wd_type", pa.string()),
        ("document", pa.string()), ("document_sha256", pa.string()),
        ("document_char_len", pa.int32()), ("year", pa.int32()),
        ("publish_date", pa.string()), ("active", pa.bool_()), ("standard", pa.bool_()),
        ("construction_type", pa.string()), ("fetch_status", pa.string()),
        ("http_status", pa.int32()), ("fetched_at", pa.timestamp("us", tz="UTC")),
        ("source", pa.string()),
    ])


def run_rate_stage(
    *,
    storage_options: dict,
    dsn: str | None,
    wd_uri: str = WD_URI,
    rate_uri: str = RATE_DOC_URI,
    limit: int = 0,
    workers: int = 4,
    inter_call_sleep: float = 0.05,
    resume: bool = True,
    checkpoint_every: int = 400,
) -> dict:
    """Fetch the plaintext rate register for every SCA/DBA WD on the manifest.

    Resume-safe (append): skips (full_reference_number, revision_number) already in the
    output. A per-WD 404 records fetch_status='http_404' (a genuinely-gone WD, not a run
    failure); only transient exhaustion is left to a --rates --resume re-run. Not
    fail-closed at the WD grain (individual docs can vanish) — completeness is tracked
    (fetched / not_found / retry_exhausted counts) and reconciled against the SCA+DBA total.
    """
    import hashlib
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import lance
    import pyarrow as pa
    import requests

    started_at = dt.datetime.now(dt.timezone.utc)
    so = storage_options
    man = lance.dataset(wd_uri, storage_options=so).to_table(
        columns=["full_reference_number", "revision_number", "type_code"]).to_pylist()
    work = [(r["full_reference_number"], r["revision_number"], r["type_code"]) for r in man
            if r["type_code"] in ("SCA", "DBA") and r["full_reference_number"]
            and r["revision_number"] is not None]

    done: set = set()
    if resume and _dataset_exists(rate_uri, so):
        prior = lance.dataset(rate_uri, storage_options=so).to_table(
            columns=["full_reference_number", "revision_number", "fetch_status"]).to_pylist()
        # re-fetch transient failures; keep fetched + genuine http_4xx
        done = {(p["full_reference_number"], p["revision_number"]) for p in prior
                if p["fetch_status"] and not p["fetch_status"].startswith("retry")}
    todo = [w for w in work if (w[0], w[1]) not in done]
    if limit:
        todo = todo[:limit]
    print(f"rate stage: sca+dba={len(work)} already_done={len(done)} todo={len(todo)}", flush=True)

    session = requests.Session()
    calls = {"n": 0}
    schema = _rate_schema()

    def fetch_doc(ref: str, rev: int, tc: str) -> dict:
        url = RATE_URL.format(ref=ref, rev=rev)
        base = {"wd_id": ref, "full_reference_number": ref, "revision_number": int(rev),
                "wd_type": tc, "document": None, "document_sha256": None,
                "document_char_len": 0, "year": None, "publish_date": None, "active": None,
                "standard": None, "construction_type": None, "fetch_status": "retry_exhausted",
                "http_status": -1, "fetched_at": dt.datetime.now(dt.timezone.utc), "source": RATE_SOURCE}
        for attempt in range(6):
            calls["n"] += 1
            try:
                resp = session.get(url, headers=_headers(), timeout=60)
            except requests.RequestException:
                time.sleep(min(30, 2 ** attempt)); continue
            sc = resp.status_code
            if sc == 200:
                j = resp.json() or {}
                doc = j.get("document") or ""
                yr = str(j.get("year", "")).strip()
                base.update({
                    "document": doc or None,
                    "document_sha256": (hashlib.sha256(doc.encode("utf-8", "replace")).hexdigest()
                                        if doc else None),
                    "document_char_len": len(doc),
                    "year": int(yr) if yr.lstrip("-").isdigit() else None,
                    "publish_date": _as_iso(j.get("publishDate")),
                    "active": _bool_or_none(j.get("active")), "standard": _bool_or_none(j.get("standard")),
                    "construction_type": _s(j.get("constructionType")),
                    "fetch_status": "fetched", "http_status": 200,
                    "fetched_at": dt.datetime.now(dt.timezone.utc)})
                return base
            if sc in (403, 429, 503):
                time.sleep(min(120, 5 * 2 ** attempt)); continue
            if sc >= 500:
                time.sleep(min(30, 2 ** attempt)); continue
            base.update({"fetch_status": f"http_{sc}", "http_status": sc,
                         "fetched_at": dt.datetime.now(dt.timezone.utc)})
            return base
        return base

    def flush(batch: list) -> None:
        if not batch:
            return
        tbl = pa.Table.from_pylist(batch, schema=schema)
        mode = "append" if _dataset_exists(rate_uri, so) else "create"
        lance.write_dataset(tbl, rate_uri, mode=mode, data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                            storage_options=so)

    counts = {"fetched": 0, "not_found": 0, "retry_exhausted": 0}
    batch: list = []
    done_n = 0
    status, error_text, built = "error", None, []
    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(fetch_doc, r, v, t): (r, v) for (r, v, t) in todo}
            for fut in as_completed(futs):
                row = fut.result()
                st = row["fetch_status"]
                counts["fetched" if st == "fetched" else
                       "not_found" if st.startswith("http_") else "retry_exhausted"] += 1
                batch.append(row)
                done_n += 1
                if len(batch) >= checkpoint_every:
                    flush(batch); batch = []
                    print(f"  rate {done_n}/{len(todo)}  {counts}", flush=True)
                if inter_call_sleep:
                    time.sleep(inter_call_sleep)
        flush(batch)
        # indexes (idempotent)
        built = _build_indexes(rate_uri, btree=["wd_id", "full_reference_number"],
                               bitmap=["wd_type", "active", "revision_number"], so=so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc); print(f"FATAL: {exc}", flush=True); raise
    finally:
        total_rate = lance.dataset(rate_uri, storage_options=so).count_rows() if _dataset_exists(rate_uri, so) else 0
        completed_at = dt.datetime.now(dt.timezone.utc)
        print(f"RATE SUMMARY: todo={len(todo)} {counts} total_in_dataset={total_rate} "
              f"api_calls={calls['n']} indexes={built} status={status} error={error_text}", flush=True)
        _record_run({"feed": "sam_wd_rate_documents", "status": status, "active_total": len(work),
                     "wd_rows": counts["fetched"], "county_rows": counts["not_found"],
                     "sca": None, "dba": None, "cba": None, "stateless_wds": counts["retry_exhausted"],
                     "dedup_dropped": len(done), "api_calls": calls["n"], "wd_uri": wd_uri,
                     "county_uri": rate_uri, "indexes_built": built, "error": error_text,
                     "started_at": started_at, "completed_at": completed_at,
                     "stats": {"counts": counts, "total_in_dataset": total_rate}}, dsn)
    return {"status": status, "todo": len(todo), "counts": counts, "total_in_dataset": total_rate,
            "api_calls": calls["n"], "indexes": built}


# ─────────────────────────────────────────────────────────────────────────────────
# PER-CBA POINTER STAGE (pinned + verified key-less; type=CBA WDs only)
# ─────────────────────────────────────────────────────────────────────────────────
# A §4(c) CBA WD has NO WHD rate register. GET https://sam.gov/api/prod/wdol/v1/cba/{_id}
#     -> 200 hal+json { cbaNumber, revisionNumber, contractServices (boilerplate,
#        no rates), contractorName, contractorUnion, organizationName (agency),
#        organizationId, status, latest, archived, createdBy/modifiedBy (IP or email),
#        effectiveStartDate, effectiveEndDate, publishedDate, createdOn, modifiedOn,
#        cbaLocation[]{ id, state (full NAME), county (name|"Statewide"), city?, zipCode? } }
# KEY-LESS (verified 200, no X-Auth-Token). This is POINTER metadata that CITES the
# governing union contract — the wage/fringe schedule is EXTERNAL to SAM.gov (every
# /cba/{id}/{attachments,files,document,download} sub-path 404s). Descriptive fields
# are NULL on older records (the model gained contractor/union/org/effective-dates over
# time; pre-2020s cbaLocation lacks city/zipCode) — schema is null-tolerant throughout.
# Resume-safe append, mirroring run_rate_stage. The location explode uses full state
# NAMES (not the USPS codes / SAM county codes of sam_wd_county_coverage) — a downstream
# xwalk resolves (state_name, county_name) -> FIPS. Raw stays lossless.
CBA_URI = os.environ.get("SAM_WD_CBA_LANCE_URI", f"s3://{BUCKET}/active/sam_wd_cba_pointers/")
CBA_COVERAGE_URI = os.environ.get(
    "SAM_WD_CBA_COVERAGE_LANCE_URI", f"s3://{BUCKET}/active/sam_wd_cba_coverage/")
CBA_DETAIL_URL = "https://sam.gov/api/prod/wdol/v1/cba/{id}"
CBA_SOURCE = "sam.gov/api/prod/wdol/v1/cba (frontend, no api_key)"


def _cba_schema():
    import pyarrow as pa
    return pa.schema([
        ("wd_id", pa.string()), ("cbawd_id", pa.int32()), ("cba_number", pa.string()),
        ("revision_number", pa.int32()), ("contract_services", pa.string()),
        ("contractor_name", pa.string()), ("contractor_union", pa.string()),
        ("organization_name", pa.string()), ("organization_id", pa.string()),
        ("status", pa.string()), ("latest", pa.bool_()), ("archived", pa.bool_()),
        ("created_by", pa.string()), ("modified_by", pa.string()),
        ("effective_start_date", pa.string()), ("effective_end_date", pa.string()),
        ("published_date", pa.string()), ("created_on", pa.string()), ("modified_on", pa.string()),
        ("location_count", pa.int32()),
        ("fetch_status", pa.string()), ("http_status", pa.int32()),
        ("fetched_at", pa.timestamp("us", tz="UTC")), ("source", pa.string()),
    ])


def _cba_coverage_schema():
    import pyarrow as pa
    return pa.schema([
        ("wd_id", pa.string()), ("cba_number", pa.string()),
        ("location_id", pa.int32()), ("state_name", pa.string()), ("county_name", pa.string()),
        ("city", pa.string()), ("zip_code", pa.string()),
        ("source", pa.string()), ("crawled_at", pa.timestamp("us", tz="UTC")),
    ])


def run_cba_stage(
    *,
    storage_options: dict,
    dsn: str | None,
    wd_uri: str = WD_URI,
    cba_uri: str = CBA_URI,
    cba_coverage_uri: str = CBA_COVERAGE_URI,
    limit: int = 0,
    workers: int = 4,
    inter_call_sleep: float = 0.05,
    resume: bool = True,
    checkpoint_every: int = 400,
) -> dict:
    """Fetch the POINTER record for every type=CBA WD on the manifest (key-less).

    A §4(c) CBA WD carries no rate register — the detail endpoint returns
    employer/union/agency/locality/effective-date metadata that CITES the governing
    union contract (whose wage/fringe schedule is external to SAM.gov). Resume-safe
    append into sam_wd_cba_pointers (+ a sam_wd_cba_coverage location explode),
    mirroring run_rate_stage: skip wd_ids already fetched (non-retry status);
    per-WD http_4xx is recorded, not fatal; transient exhaustion is left for --resume.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import lance
    import pyarrow as pa
    import requests

    started_at = dt.datetime.now(dt.timezone.utc)
    crawled_at = started_at
    so = storage_options
    man = lance.dataset(wd_uri, storage_options=so).to_table(
        columns=["_id", "type_code"]).to_pylist()
    work = [r["_id"] for r in man if r["type_code"] == "CBA" and r["_id"]]

    done: set = set()
    if resume and _dataset_exists(cba_uri, so):
        prior = lance.dataset(cba_uri, storage_options=so).to_table(
            columns=["wd_id", "fetch_status"]).to_pylist()
        done = {p["wd_id"] for p in prior
                if p["fetch_status"] and not p["fetch_status"].startswith("retry")}
    todo = [w for w in work if w not in done]
    if limit:
        todo = todo[:limit]
    print(f"cba stage: type=CBA={len(work)} already_done={len(done)} todo={len(todo)}", flush=True)

    session = requests.Session()
    calls = {"n": 0}
    p_schema, c_schema = _cba_schema(), _cba_coverage_schema()

    def fetch_cba(wd_id: str) -> tuple[dict, list[dict]]:
        url = CBA_DETAIL_URL.format(id=wd_id)
        row = {
            "wd_id": wd_id,
            "cbawd_id": (int(wd_id) if str(wd_id).lstrip("-").isdigit() else None),
            "cba_number": None, "revision_number": None, "contract_services": None,
            "contractor_name": None, "contractor_union": None, "organization_name": None,
            "organization_id": None, "status": None, "latest": None, "archived": None,
            "created_by": None, "modified_by": None, "effective_start_date": None,
            "effective_end_date": None, "published_date": None, "created_on": None,
            "modified_on": None, "location_count": 0, "fetch_status": "retry_exhausted",
            "http_status": -1, "fetched_at": dt.datetime.now(dt.timezone.utc), "source": CBA_SOURCE,
        }
        for attempt in range(6):
            calls["n"] += 1
            try:
                resp = session.get(url, headers=_headers(), timeout=60)
            except requests.RequestException:
                time.sleep(min(30, 2 ** attempt)); continue
            sc = resp.status_code
            if sc == 200:
                j = resp.json() or {}
                locs = j.get("cbaLocation") or []
                rev = str(j.get("revisionNumber", "")).strip()
                row.update({
                    "cba_number": _s(j.get("cbaNumber")),
                    "revision_number": int(rev) if rev.lstrip("-").isdigit() else None,
                    "contract_services": _s(j.get("contractServices")),
                    "contractor_name": _s(j.get("contractorName")),
                    "contractor_union": _s(j.get("contractorUnion")),
                    "organization_name": _s(j.get("organizationName")),
                    "organization_id": _s(j.get("organizationId")),
                    "status": _s(j.get("status")), "latest": _bool_or_none(j.get("latest")),
                    "archived": _bool_or_none(j.get("archived")),
                    "created_by": _s(j.get("createdBy")), "modified_by": _s(j.get("modifiedBy")),
                    "effective_start_date": _as_iso(j.get("effectiveStartDate")),
                    "effective_end_date": _as_iso(j.get("effectiveEndDate")),
                    "published_date": _as_iso(j.get("publishedDate")),
                    "created_on": _as_iso(j.get("createdOn")),
                    "modified_on": _as_iso(j.get("modifiedOn")),
                    "location_count": len(locs), "fetch_status": "fetched", "http_status": 200,
                    "fetched_at": dt.datetime.now(dt.timezone.utc)})
                cov: list[dict] = []
                for loc in locs:
                    if not isinstance(loc, dict):
                        continue
                    lid = str(loc.get("id", "")).strip()
                    cov.append({
                        "wd_id": wd_id, "cba_number": row["cba_number"],
                        "location_id": int(lid) if lid.lstrip("-").isdigit() else None,
                        "state_name": _s(loc.get("state")), "county_name": _s(loc.get("county")),
                        "city": _s(loc.get("city")), "zip_code": _s(loc.get("zipCode")),
                        "source": CBA_SOURCE, "crawled_at": crawled_at})
                return row, cov
            if sc in (403, 429, 503):
                time.sleep(min(120, 5 * 2 ** attempt)); continue
            if sc >= 500:
                time.sleep(min(30, 2 ** attempt)); continue
            row.update({"fetch_status": f"http_{sc}", "http_status": sc,
                        "fetched_at": dt.datetime.now(dt.timezone.utc)})
            return row, []
        return row, []

    def flush(prows: list, crows: list) -> None:
        if prows:
            ptbl = pa.Table.from_pylist(prows, schema=p_schema)
            lance.write_dataset(
                ptbl, cba_uri, mode=("append" if _dataset_exists(cba_uri, so) else "create"),
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                storage_options=so)
        if crows:
            ctbl = pa.Table.from_pylist(crows, schema=c_schema)
            lance.write_dataset(
                ctbl, cba_coverage_uri,
                mode=("append" if _dataset_exists(cba_coverage_uri, so) else "create"),
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                storage_options=so)

    counts = {"fetched": 0, "not_found": 0, "retry_exhausted": 0}
    pbatch: list = []
    cbatch: list = []
    done_n = 0
    status, error_text, built = "error", None, []
    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(fetch_cba, w): w for w in todo}
            for fut in as_completed(futs):
                row, cov = fut.result()
                st = row["fetch_status"]
                counts["fetched" if st == "fetched" else
                       "not_found" if st.startswith("http_") else "retry_exhausted"] += 1
                pbatch.append(row); cbatch.extend(cov)
                done_n += 1
                if len(pbatch) >= checkpoint_every:
                    flush(pbatch, cbatch); pbatch = []; cbatch = []
                    print(f"  cba {done_n}/{len(todo)}  {counts}", flush=True)
                if inter_call_sleep:
                    time.sleep(inter_call_sleep)
        flush(pbatch, cbatch)
        built += [f"pointers:{b}" for b in _build_indexes(
            cba_uri, btree=["wd_id", "cba_number", "organization_id"],
            bitmap=["status", "latest", "archived"], so=so)]
        if _dataset_exists(cba_coverage_uri, so):
            built += [f"coverage:{b}" for b in _build_indexes(
                cba_coverage_uri, btree=["wd_id", "cba_number"], bitmap=[], so=so)]
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc); print(f"FATAL: {exc}", flush=True); raise
    finally:
        total_cba = (lance.dataset(cba_uri, storage_options=so).count_rows()
                     if _dataset_exists(cba_uri, so) else 0)
        total_cov = (lance.dataset(cba_coverage_uri, storage_options=so).count_rows()
                     if _dataset_exists(cba_coverage_uri, so) else 0)
        completed_at = dt.datetime.now(dt.timezone.utc)
        print(f"CBA SUMMARY: todo={len(todo)} {counts} pointers_in_dataset={total_cba} "
              f"coverage_in_dataset={total_cov} api_calls={calls['n']} indexes={built} "
              f"status={status} error={error_text}", flush=True)
        _record_run({"feed": "sam_wd_cba_pointers", "status": status, "active_total": len(work),
                     "wd_rows": counts["fetched"], "county_rows": total_cov,
                     "sca": None, "dba": None, "cba": counts["fetched"],
                     "stateless_wds": counts["retry_exhausted"], "dedup_dropped": len(done),
                     "api_calls": calls["n"], "wd_uri": wd_uri, "county_uri": cba_uri,
                     "indexes_built": built, "error": error_text,
                     "started_at": started_at, "completed_at": completed_at,
                     "stats": {"counts": counts, "pointers_in_dataset": total_cba,
                               "coverage_in_dataset": total_cov}}, dsn)
    return {"status": status, "todo": len(todo), "counts": counts,
            "pointers_in_dataset": total_cba, "coverage_in_dataset": total_cov,
            "api_calls": calls["n"], "indexes": built}


def _cli() -> None:
    p = argparse.ArgumentParser(
        description="SAM.gov wage-determination metadata manifest (active WDs -> Lance).")
    p.add_argument("--run", action="store_true", default=False,
                   help="MANIFEST stage: crawl the active-WD metadata catalog (3 GETs).")
    p.add_argument("--rates", action="store_true", default=False,
                   help="RATE stage: fetch the plaintext rate document for every SCA/DBA WD "
                        "on the manifest (resume-safe append).")
    p.add_argument("--cba", action="store_true", default=False,
                   help="CBA POINTER stage: fetch the key-less /wdol/v1/cba/{_id} pointer "
                        "record (employer/union/agency/locality) for every type=CBA WD "
                        "on the manifest (resume-safe append).")
    p.add_argument("--resume", action="store_true", default=False,
                   help="manifest: no-op re-crawl. rates: skip already-fetched (ref,rev), "
                        "re-fetch only transient failures.")
    p.add_argument("--limit", type=int, default=0,
                   help="SMOKE: cap rows per type slice (manifest, bypasses reconciliation) "
                        "or docs fetched (rates).")
    p.add_argument("--size", type=int, default=0,
                   help="manifest: cap the HAL page size per slice (0 = size==slice totalElements).")
    p.add_argument("--workers", type=int, default=4, help="rates: concurrent fetch workers.")
    p.add_argument("--wd-uri", default=WD_URI)
    p.add_argument("--county-uri", default=COUNTY_URI)
    p.add_argument("--rate-uri", default=RATE_DOC_URI)
    p.add_argument("--cba-uri", default=CBA_URI)
    p.add_argument("--cba-coverage-uri", default=CBA_COVERAGE_URI)
    p.add_argument("--inter-call-sleep", type=float, default=0.5)
    p.add_argument("--keep-temp", action="store_true", default=False,
                   help="accepted for interface parity; this crawl stages no temp files.")
    a = p.parse_args()
    if sum(bool(m) for m in (a.run, a.rates, a.cba)) != 1:
        p.error("pass exactly one of --run (manifest) / --rates (rate docs) / --cba (CBA pointers).")
    so, dsn = _storage_options(), os.environ.get("HQX_DB_URL_POOLED")
    if a.rates:
        out = run_rate_stage(
            storage_options=so, dsn=dsn, wd_uri=a.wd_uri, rate_uri=a.rate_uri,
            limit=a.limit, workers=a.workers,
            inter_call_sleep=(a.inter_call_sleep if a.inter_call_sleep != 0.5 else 0.05),
            resume=a.resume or True)
    elif a.cba:
        out = run_cba_stage(
            storage_options=so, dsn=dsn, wd_uri=a.wd_uri, cba_uri=a.cba_uri,
            cba_coverage_uri=a.cba_coverage_uri, limit=a.limit, workers=a.workers,
            inter_call_sleep=(a.inter_call_sleep if a.inter_call_sleep != 0.5 else 0.05),
            resume=a.resume or True)
    else:
        out = run_harvest(
            storage_options=so, dsn=dsn, wd_uri=a.wd_uri, county_uri=a.county_uri,
            size=a.size, limit=a.limit, inter_call_sleep=a.inter_call_sleep, resume=a.resume)
    print("RESULT:", out, flush=True)


if __name__ == "__main__":
    _cli()
