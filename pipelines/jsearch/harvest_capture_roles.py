"""JSearch capture-roles harvester — the companies hiring federal capture managers.

A company posting a "capture manager" (or capture director / VP capture / …) is actively investing
in winning US government contracts — a high-intent govcon account signal. This feed harvests those
postings from JSearch (OpenWeb Ninja's Google-for-Jobs aggregate across LinkedIn/Indeed/Glassdoor/
ZipRecruiter/…) and lands them so downstream resolution can map ``employer_website`` → federal
entity (UEI/CAGE) against the existing SAM/DSBS/firmographics assets. THAT resolution is a separate,
downstream blast radius — this file lands the raw posting SoR only.

TWO-STAGE, LEADMAGIC-SHAPED. Land raw truth to HQX Postgres FIRST, THEN hydrate Lance from it:
  1. HARVEST  — title-variant fan-out through ``core.openwebninja_gateway`` (cursor pagination,
                1 credit/page). Every observed posting is upserted into
                ``ops.jsearch_capture_postings`` keyed on ``job_id`` (the natural per-publisher-
                posting key = dedup + resume guard). Per-run terminal state → ``ops.jsearch_capture_runs``.
  2. MATERIALIZE — psycopg SELECT → Apache Arrow (NEVER pandas) → ``s3://data-sink/active/
                jsearch_capture_roles/`` (Lance v2.1) + BTREE/BITMAP indexes. The downstream SoR.

LAND EVERYTHING. Recruiter/staffing postings ("Zachary Piper" fronting a DHS req) and "Confidential"
employers are NOT dropped — they are landed with non-destructive boolean flags
(``employer_is_confidential`` / ``employer_is_staffing``) so a stage-2 reader can filter or score
them. Filtering at harvest would destroy recoverable signal; the SoR carries raw truth.

DUAL-MODE. The heavy functions are plain env-driven Python. Modal ``@app.function`` wrappers expose
them to the Universal Dispatcher (production path, spawned by ``src/trigger/jsearch_capture_roles.ts``
→ callback), and a local ``argparse`` CLI runs the same functions for operator-supervised backfills.

    # production wiring
    modal deploy pipelines/jsearch/harvest_capture_roles.py

    # local ops (env from doppler; no Modal needed)
    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'psycopg[binary]>=3.2' --with 'requests>=2.32' \
      python3 pipelines/jsearch/harvest_capture_roles.py <init_ops|smoke|backfill|harvest|materialize|status|verify>

    harvest/backfill flags: --date-posted all|month|week|3days|today  --max-pages N  --run-root LABEL
                            --geo  --titles core|extended  --workers N   (backfill saturation matrix)
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import modal

# Repo root on sys.path so `core.openwebninja_gateway` resolves on a local `python3 pipelines/...` run.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from core.openwebninja_gateway import key_present, search as jsearch_search  # noqa: E402

FEED = "jsearch_capture_roles"
_ACTIVE = "s3://data-sink/active"
DATASET_URI = os.environ.get("JSEARCH_CAPTURE_URI", f"{_ACTIVE}/{FEED}/").rstrip("/") + "/"
DATA_STORAGE_VERSION = "2.1"

# Title-variant fan-out (operator-selected). Google-for-Jobs fuzzy-matches within a query, but a
# single "capture manager" query structurally misses "capture director", "VP capture", etc., so the
# distinct titles are queried explicitly. country=us gates location; dedup is by job_id across all.
QUERY_VARIANTS = [
    "capture manager",
    "capture director",
    "director of capture",
    "vice president capture",
    "capture lead",
    "business development capture manager",
    "capture management",
]

# ── expanded backfill coverage — saturation via geo × title sharding ─────────────────────────────
# Adjacent govcon capture/BD/proposal titles that surface additional distinct buyers a bare
# "capture manager" query never ranks. Used by the backfill only; the daily incremental stays on the
# lean QUERY_VARIANTS set (geo-sharding every day would just re-fetch the same live postings).
EXTENDED_TITLES = QUERY_VARIANTS + [
    "senior capture manager", "capture executive", "capture specialist", "capture analyst",
    "vp business development capture", "federal capture manager", "government capture manager",
    "defense capture manager", "proposal manager", "proposal director", "senior proposal manager",
    "capture and proposal manager", "director of proposals",
    "director of business development federal", "business development director government",
    "growth director defense", "senior director capture", "principal capture manager",
]

# Core titles to geo-shard. Google-for-Jobs caps results PER QUERY; sharding the highest-volume
# titles across govcon hubs pierces that cap and surfaces postings the national query dropped.
GEO_TITLES = [
    "capture manager", "capture director", "capture lead", "capture management",
    "business development capture manager", "proposal manager", "proposal director",
    "director of business development federal", "senior capture manager", "vice president capture",
]

# Govcon employment hubs (metro + state) — weighted to where federal contractors concentrate
# capture/BD staff. Comprehensive but not exhaustive; extend to widen the sweep further.
HUB_CITIES = [
    # DC / Northern Virginia / Maryland (densest govcon corridor)
    "Arlington VA", "Washington DC", "Reston VA", "Chantilly VA", "McLean VA", "Fairfax VA",
    "Alexandria VA", "Herndon VA", "Springfield VA", "Falls Church VA", "Vienna VA", "Dulles VA",
    "Manassas VA", "Quantico VA", "Dahlgren VA", "Fort Belvoir VA", "Newport News VA",
    "Virginia Beach VA", "Norfolk VA", "Hampton VA", "Suffolk VA", "Charlottesville VA",
    "Rockville MD", "Bethesda MD", "Columbia MD", "Aberdeen MD", "Annapolis Junction MD",
    "Lexington Park MD", "Fort Meade MD", "Patuxent River MD", "Frederick MD", "Gaithersburg MD",
    "Silver Spring MD", "Linthicum MD", "Baltimore MD", "California MD",
    # Southeast
    "Charleston SC", "Columbia SC", "Beaufort SC", "Fort Bragg NC", "Fayetteville NC", "Raleigh NC",
    "Charlotte NC", "Wilmington NC", "Durham NC", "Atlanta GA", "Augusta GA", "Columbus GA",
    "Savannah GA", "Warner Robins GA", "Marietta GA", "Huntsville AL", "Montgomery AL",
    "Birmingham AL", "Tampa FL", "Melbourne FL", "Orlando FL", "Jacksonville FL", "Pensacola FL",
    "Fort Walton Beach FL", "Panama City FL", "Cape Canaveral FL", "Miami FL", "Oak Ridge TN",
    "Nashville TN", "Knoxville TN",
    # Texas / south-central
    "San Antonio TX", "Fort Worth TX", "Austin TX", "Dallas TX", "Houston TX", "El Paso TX",
    "Killeen TX", "Corpus Christi TX", "Oklahoma City OK", "Lawton OK", "Tulsa OK", "Wichita KS",
    "Leavenworth KS", "Kansas City MO", "Saint Louis MO", "New Orleans LA",
    # Mountain / southwest
    "Colorado Springs CO", "Denver CO", "Aurora CO", "Boulder CO", "Albuquerque NM", "Las Cruces NM",
    "Phoenix AZ", "Tucson AZ", "Sierra Vista AZ", "Ogden UT", "Salt Lake City UT", "Layton UT",
    "Las Vegas NV", "Omaha NE",
    # Pacific
    "San Diego CA", "Los Angeles CA", "El Segundo CA", "San Jose CA", "Sacramento CA", "Sunnyvale CA",
    "Ridgecrest CA", "San Francisco CA", "Seattle WA", "Tacoma WA", "Bremerton WA", "Portland OR",
    "Honolulu HI", "Anchorage AK",
    # Northeast / Midwest
    "Boston MA", "Lexington MA", "Bedford MA", "Portsmouth NH", "New York NY", "Rome NY",
    "Syracuse NY", "Philadelphia PA", "King of Prussia PA", "Pittsburgh PA", "Trenton NJ",
    "Picatinny NJ", "Dayton OH", "Cincinnati OH", "Columbus OH", "Cleveland OH", "Detroit MI",
    "Warren MI", "Chicago IL", "Rock Island IL", "Indianapolis IN", "Crane IN", "Minneapolis MN",
]


def build_backfill_queries(geo: bool = True, extended: bool = True) -> list[str]:
    """Compose the backfill query matrix: national title queries (order-preserving dedup) plus, when
    geo, the core titles crossed with the govcon hubs. Each geo query pierces Google's per-query
    result cap; all results dedup by job_id at upsert."""
    titles = EXTENDED_TITLES if extended else list(QUERY_VARIANTS)
    queries = list(dict.fromkeys(titles))
    if geo:
        queries += [f"{t} in {city}" for t in GEO_TITLES for city in HUB_CITIES]
    return list(dict.fromkeys(queries))


DEFAULT_COUNTRY = "us"
DEFAULT_DATE_POSTED = "all"          # backfill default; incremental cron passes "3days"
MAX_PAGES_PER_QUERY = int(os.environ.get("JSEARCH_MAX_PAGES", "30"))  # backstop; ≤300 jobs/variant
DEFAULT_WORKERS = int(os.environ.get("JSEARCH_WORKERS", "10"))        # concurrent per-query workers

BTREE_INDEXES = ["job_id", "employer_domain", "employer_name"]
BITMAP_INDEXES = ["publisher", "job_state", "query_variant", "employment_type",
                  "employer_is_confidential", "employer_is_staffing"]

# ── classification heuristics (non-destructive flags; a stage-2 reader refines) ──────────────
# Staffing/recruiting firms post capture roles ON BEHALF OF a client — they are not the buyer. This
# curated token set flags them; it is deliberately conservative (precision over recall) and lives in
# the SoR as an annotation, never a filter.
_STAFFING = {
    "zachary piper", "insight global", "teksystems", "aerotek", "actalent", "robert half",
    "kforce", "apex systems", "the judge group", "judge group", "cybercoders", "jobot",
    "beacon hill", "randstad", "adecco", "manpower", "kelly services", "system one",
    "guidehouse staffing", "aditi", "collabera", "tekpartners", "modis", "experis",
    "korn ferry", "michael page", "lucas group", "staffing", "recruiting", "talent acquisition",
    "search firm", "headhunter",
}
_CONFIDENTIAL = {"confidential", "undisclosed", "company confidential", "not disclosed", "private"}

_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")


def log(m: str) -> None:
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _normalize_domain(raw: str | None) -> str | None:
    d = (raw or "").strip().lower()
    if not d:
        return None
    d = _WWW.sub("", _SCHEME.sub("", d)).split("/", 1)[0].strip().rstrip(".")
    return d or None


def _looks_staffing(name: str | None) -> bool:
    n = (name or "").strip().lower()
    return bool(n) and any(tok in n for tok in _STAFFING)


def _is_confidential(name: str | None) -> bool:
    n = (name or "").strip().lower()
    return (not n) or any(tok == n or tok in n for tok in _CONFIDENTIAL)


# ── env / connections ────────────────────────────────────────────────────────────────────────
def _hqx_dsn() -> str:
    """Transaction-mode (Supavisor :6543) hq-x DSN — a long-lived-but-bursty harvest must not pin a
    session-pool backend for the whole run (same rationale as the LeadMagic finder)."""
    dsn = os.environ.get("HQX_DB_URL_TRANSACTION")
    if not dsn:
        pooled = os.environ.get("HQX_DB_URL_POOLED") or os.environ.get("HQX_DB_URL")
        if not pooled:
            raise RuntimeError("Set HQX_DB_URL_TRANSACTION / HQX_DB_URL_POOLED / HQX_DB_URL (hqx-postgres).")
        dsn = pooled.replace(".pooler.supabase.com:5432", ".pooler.supabase.com:6543")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _open_conn(dsn: str | None = None):
    import psycopg
    return psycopg.connect(dsn or _hqx_dsn(), autocommit=True, prepare_threshold=None)


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID (r2-credentials).")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


# ── ops DDL — idempotent. Inline copy is AUTHORITATIVE; ops_jsearch_capture.sql mirrors it. ────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.jsearch_capture_postings (
    job_id                  text        PRIMARY KEY,
    query_variant           text,
    job_title               text,
    employer_name           text,
    employer_website        text,
    employer_domain         text,
    employer_is_confidential boolean    NOT NULL DEFAULT false,
    employer_is_staffing    boolean     NOT NULL DEFAULT false,
    publisher               text,
    apply_publishers        jsonb,
    n_apply_options         integer     NOT NULL DEFAULT 0,
    employment_type         text,
    job_is_remote           boolean,
    job_city                text,
    job_state               text,
    job_country             text,
    job_location            text,
    posted_at_ts            bigint,
    posted_at               timestamptz,
    job_min_salary          numeric,
    job_max_salary          numeric,
    salary_period           text,
    job_apply_link          text,
    onet_soc                text,
    raw_json                jsonb,
    harvest_run_root        text,
    first_seen_at           timestamptz NOT NULL DEFAULT now(),
    last_seen_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS jsearch_capture_postings_domain_idx    ON ops.jsearch_capture_postings (employer_domain);
CREATE INDEX IF NOT EXISTS jsearch_capture_postings_employer_idx  ON ops.jsearch_capture_postings (employer_name);
CREATE INDEX IF NOT EXISTS jsearch_capture_postings_state_idx     ON ops.jsearch_capture_postings (job_state);
CREATE INDEX IF NOT EXISTS jsearch_capture_postings_publisher_idx ON ops.jsearch_capture_postings (publisher);
CREATE INDEX IF NOT EXISTS jsearch_capture_postings_seen_idx      ON ops.jsearch_capture_postings (last_seen_at DESC);

CREATE TABLE IF NOT EXISTS ops.jsearch_capture_runs (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed               text        NOT NULL,
    mode               text        NOT NULL,
    run_root           text,
    date_posted        text,
    queries_run        integer     NOT NULL DEFAULT 0,
    pages_fetched      integer     NOT NULL DEFAULT 0,
    credits_spent      integer     NOT NULL DEFAULT 0,
    jobs_seen          integer     NOT NULL DEFAULT 0,
    jobs_new           integer     NOT NULL DEFAULT 0,
    jobs_updated       integer     NOT NULL DEFAULT 0,
    employers_distinct integer     NOT NULL DEFAULT 0,
    rows_materialized  integer     NOT NULL DEFAULT 0,
    status             text        NOT NULL,
    error              text,
    started_at         timestamptz,
    completed_at       timestamptz,
    recorded_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT jsearch_capture_runs_status_chk CHECK (status IN ('success', 'error')),
    CONSTRAINT jsearch_capture_runs_mode_chk   CHECK (mode IN ('backfill', 'incremental', 'materialize'))
);
CREATE INDEX IF NOT EXISTS jsearch_capture_runs_feed_idx        ON ops.jsearch_capture_runs (feed);
CREATE INDEX IF NOT EXISTS jsearch_capture_runs_recorded_at_idx ON ops.jsearch_capture_runs (recorded_at DESC);
"""

# Posting columns landed to PG, in insert order. Kept as a module constant so the INSERT, the
# _project_job dict, and the materialize SELECT can never drift.
_POSTING_COLS = [
    "job_id", "query_variant", "job_title", "employer_name", "employer_website", "employer_domain",
    "employer_is_confidential", "employer_is_staffing", "publisher", "apply_publishers",
    "n_apply_options", "employment_type", "job_is_remote", "job_city", "job_state", "job_country",
    "job_location", "posted_at_ts", "posted_at", "job_min_salary", "job_max_salary", "salary_period",
    "job_apply_link", "onet_soc", "raw_json", "harvest_run_root",
]
# Lance carries the clean columnar projection — everything except the verbatim raw_json /
# apply_publishers jsonb (those stay in PG) — plus the seen-at observation window.
_LANCE_COLS = [
    "job_id", "query_variant", "job_title", "employer_name", "employer_website", "employer_domain",
    "employer_is_confidential", "employer_is_staffing", "publisher", "n_apply_options",
    "employment_type", "job_is_remote", "job_city", "job_state", "job_country", "job_location",
    "posted_at_ts", "posted_at", "job_min_salary", "job_max_salary", "salary_period",
    "job_apply_link", "onet_soc", "harvest_run_root", "first_seen_at", "last_seen_at",
]


def _strip_nul(obj):
    """Recursively strip NUL (\\u0000) bytes from every string in a JSearch payload. Postgres text
    and jsonb CANNOT store \\u0000 (``UntranslatableCharacter``); a job description carrying one
    (legal boilerplate, truncation artifacts) would otherwise abort the whole query's upsert."""
    if isinstance(obj, str):
        return obj.replace("\x00", "") if "\x00" in obj else obj
    if isinstance(obj, dict):
        return {k: _strip_nul(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_strip_nul(v) for v in obj]
    return obj


def _project_job(job: dict, query_variant: str, run_root: str) -> dict:
    """Raw JSearch job object → the landed posting row. Derived columns are a projection over the
    verbatim ``raw_json`` (the SoR truth). NUL bytes are stripped first so the PG upsert can't abort."""
    job = _strip_nul(job)
    employer = job.get("employer_name")
    apply_options = job.get("apply_options") or []
    apply_pubs = [o.get("publisher") for o in apply_options
                  if isinstance(o, dict) and o.get("publisher")]
    website = job.get("employer_website")
    return {
        "job_id": job.get("job_id"),
        "query_variant": query_variant,
        "job_title": job.get("job_title"),
        "employer_name": employer,
        "employer_website": website,
        "employer_domain": _normalize_domain(website),
        "employer_is_confidential": _is_confidential(employer),
        "employer_is_staffing": _looks_staffing(employer),
        "publisher": job.get("job_publisher"),
        "apply_publishers": apply_pubs or None,
        "n_apply_options": len(apply_options),
        "employment_type": job.get("job_employment_type"),
        "job_is_remote": job.get("job_is_remote"),
        "job_city": job.get("job_city"),
        "job_state": job.get("job_state"),
        "job_country": job.get("job_country"),
        "job_location": job.get("job_location"),
        "posted_at_ts": job.get("job_posted_at_timestamp"),
        "posted_at": job.get("job_posted_at_datetime_utc"),
        "job_min_salary": job.get("job_min_salary"),
        "job_max_salary": job.get("job_max_salary"),
        "salary_period": job.get("job_salary_period"),
        "job_apply_link": job.get("job_apply_link"),
        "onet_soc": job.get("job_onet_soc"),
        "raw_json": job,
        "harvest_run_root": run_root,
    }


# ── harvest → ops.jsearch_capture_postings ─────────────────────────────────────────────────────
_INSERT_SQL = """
INSERT INTO ops.jsearch_capture_postings
  (job_id, query_variant, job_title, employer_name, employer_website, employer_domain,
   employer_is_confidential, employer_is_staffing, publisher, apply_publishers, n_apply_options,
   employment_type, job_is_remote, job_city, job_state, job_country, job_location, posted_at_ts,
   posted_at, job_min_salary, job_max_salary, salary_period, job_apply_link, onet_soc, raw_json,
   harvest_run_root, last_seen_at)
VALUES (%(job_id)s,%(query_variant)s,%(job_title)s,%(employer_name)s,%(employer_website)s,
   %(employer_domain)s,%(employer_is_confidential)s,%(employer_is_staffing)s,%(publisher)s,
   %(apply_publishers)s,%(n_apply_options)s,%(employment_type)s,%(job_is_remote)s,%(job_city)s,
   %(job_state)s,%(job_country)s,%(job_location)s,%(posted_at_ts)s,%(posted_at)s,%(job_min_salary)s,
   %(job_max_salary)s,%(salary_period)s,%(job_apply_link)s,%(onet_soc)s,%(raw_json)s,
   %(harvest_run_root)s, now())
ON CONFLICT (job_id) DO UPDATE SET
   -- Re-observation: refresh volatile fields + last_seen_at; PRESERVE first_seen_at and the
   -- first-attribution query_variant. Returns (xmax=0) → true only on a fresh INSERT.
   job_title                = EXCLUDED.job_title,
   employer_name            = EXCLUDED.employer_name,
   employer_website         = EXCLUDED.employer_website,
   employer_domain          = EXCLUDED.employer_domain,
   employer_is_confidential = EXCLUDED.employer_is_confidential,
   employer_is_staffing     = EXCLUDED.employer_is_staffing,
   publisher                = EXCLUDED.publisher,
   apply_publishers         = EXCLUDED.apply_publishers,
   n_apply_options          = EXCLUDED.n_apply_options,
   employment_type          = EXCLUDED.employment_type,
   job_is_remote            = EXCLUDED.job_is_remote,
   job_city                 = EXCLUDED.job_city,
   job_state                = EXCLUDED.job_state,
   job_country              = EXCLUDED.job_country,
   job_location             = EXCLUDED.job_location,
   posted_at_ts             = EXCLUDED.posted_at_ts,
   posted_at                = EXCLUDED.posted_at,
   job_min_salary           = EXCLUDED.job_min_salary,
   job_max_salary           = EXCLUDED.job_max_salary,
   salary_period            = EXCLUDED.salary_period,
   job_apply_link           = EXCLUDED.job_apply_link,
   onet_soc                 = EXCLUDED.onet_soc,
   raw_json                 = EXCLUDED.raw_json,
   harvest_run_root         = EXCLUDED.harvest_run_root,
   last_seen_at             = now()
RETURNING (xmax = 0) AS inserted
"""


def _upsert_posting(cur, row: dict) -> bool:
    """Upsert one posting; returns True if it was a fresh insert (new job_id), False on update."""
    from psycopg.types.json import Jsonb

    params = dict(row)
    params["raw_json"] = Jsonb(row["raw_json"]) if row.get("raw_json") is not None else None
    params["apply_publishers"] = (Jsonb(row["apply_publishers"])
                                  if row.get("apply_publishers") is not None else None)
    cur.execute(_INSERT_SQL, params)
    res = cur.fetchone()
    return bool(res[0]) if res else False


def _record_run(cur, mode, run_root, date_posted, counts, status, error, started, completed) -> None:
    cur.execute(OPS_DDL)
    cur.execute(
        """INSERT INTO ops.jsearch_capture_runs
             (feed, mode, run_root, date_posted, queries_run, pages_fetched, credits_spent,
              jobs_seen, jobs_new, jobs_updated, employers_distinct, rows_materialized,
              status, error, started_at, completed_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (FEED, mode, run_root, date_posted, counts["queries_run"], counts["pages_fetched"],
         counts["credits_spent"], counts["jobs_seen"], counts["jobs_new"], counts["jobs_updated"],
         counts["employers_distinct"], counts.get("rows_materialized", 0), status, error,
         started, completed))


def _post_callback(url: str | None, payload: dict, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url (the whole body becomes
    result.output). No API key (callbackHash in the path is the auth), no {data} wrapper. Completing
    an already-complete token is a no-op — retries are safe."""
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import time

    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}")
                return
            print(f"Callback attempt {i + 1} non-2xx: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


def _harvest_one_query(query: str, date_posted: str, country: str, max_pages: int,
                       run_root: str) -> dict:
    """Paginate ONE query to exhaustion (or max_pages) on its OWN connection and upsert each posting.
    Returns per-query counters + the set of employer keys observed. Runs in a ThreadPoolExecutor
    worker, so it must not share state with siblings — its own connection is the isolation."""
    local = {"pages": 0, "credits": 0, "seen": 0, "new": 0, "updated": 0}
    employers: set[str] = set()
    conn = _open_conn()
    try:
        cur = conn.cursor()
        cursor: str | None = None
        for _page in range(max_pages):
            env = jsearch_search(query, country=country, date_posted=date_posted, cursor=cursor)
            local["credits"] += env["credits"]
            if not env["ok"]:
                if env["http_status"] not in (0, 200):
                    log(f"query={query!r} page={_page} NOT ok: http={env['http_status']} "
                        f"err={env['error']!r}")
                break
            local["pages"] += 1
            for job in env["jobs"]:
                if not job.get("job_id"):
                    continue
                row = _project_job(job, query, run_root)
                local["seen"] += 1
                inserted = _upsert_posting(cur, row)
                local["new" if inserted else "updated"] += 1
                key = row.get("employer_domain") or (row.get("employer_name") or "").lower().strip()
                if key:
                    employers.add(key)
            cursor = env["cursor"]
            if not cursor or not env["jobs"]:
                break
    finally:
        conn.close()
    return {"query": query, **local, "employers": employers}


def _harvest(mode: str, date_posted: str, country: str, max_pages: int, run_root: str,
             trigger_callback_url: str | None = None, queries: list[str] | None = None,
             workers: int = DEFAULT_WORKERS) -> dict:
    """Fan out across ``queries`` (default QUERY_VARIANTS) CONCURRENTLY — one worker per query, each
    paginating to exhaustion on its own connection — and upsert every observed posting into
    ops.jsearch_capture_postings. Credits = pages fetched (1/page). Every job_id deduped at upsert,
    so overlapping geo/title queries collapse cleanly."""
    started = dt.datetime.now(dt.timezone.utc)
    queries = list(queries or QUERY_VARIANTS)
    counts = {"queries_run": 0, "pages_fetched": 0, "credits_spent": 0, "jobs_seen": 0,
              "jobs_new": 0, "jobs_updated": 0, "employers_distinct": 0}
    status, error = "error", None
    employers: set[str] = set()

    if not key_present():
        raise RuntimeError("OPENWEBNINJA_API_KEY absent — aborting before harvest")

    # Ensure the table exists once, up front — the concurrent workers assume it is there.
    ddl_conn = _open_conn()
    try:
        ddl_conn.cursor().execute(OPS_DDL)
    finally:
        ddl_conn.close()

    log(f"harvest start: mode={mode} date_posted={date_posted} queries={len(queries)} workers={workers}")
    try:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            futs = {ex.submit(_harvest_one_query, q, date_posted, country, max_pages, run_root): q
                    for q in queries}
            for fut in as_completed(futs):
                q = futs[fut]
                counts["queries_run"] += 1
                try:
                    r = fut.result()
                except Exception as exc:  # noqa: BLE001 — one query must not sink the sweep
                    log(f"query={q!r} FAILED: {type(exc).__name__}: {exc}")
                    continue
                counts["pages_fetched"] += r["pages"]
                counts["credits_spent"] += r["credits"]
                counts["jobs_seen"] += r["seen"]
                counts["jobs_new"] += r["new"]
                counts["jobs_updated"] += r["updated"]
                employers |= r["employers"]
                if counts["queries_run"] % 25 == 0 or counts["queries_run"] == len(queries):
                    log(f"progress {counts['queries_run']}/{len(queries)}: new={counts['jobs_new']} "
                        f"seen={counts['jobs_seen']} credits={counts['credits_spent']} "
                        f"employers≈{len(employers)}")
        counts["employers_distinct"] = len(employers)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
        status = "error"
        raise
    finally:
        rec_conn = _open_conn()
        try:
            _record_run(rec_conn.cursor(), mode, run_root, date_posted, counts, status, error,
                        started, dt.datetime.now(dt.timezone.utc))
        except Exception as exc2:  # noqa: BLE001
            log(f"WARN: run-ledger write failed: {exc2}")
        finally:
            rec_conn.close()
        _post_callback(trigger_callback_url,
                       {"status": status, "feed": FEED, "mode": mode, "error": error, **counts})
    return {"feed": FEED, "mode": mode, "status": status, **counts}


# ── materialize ops.jsearch_capture_postings → Lance ───────────────────────────────────────────
def _materialize(trigger_callback_url: str | None = None) -> dict:
    """Hydrate the Lance SoR from the PG landing table. psycopg SELECT → Apache Arrow → Lance v2.1.
    Full overwrite: the ops table is the accumulating truth, Lance is its columnar projection."""
    import lance
    import pyarrow as pa

    started = dt.datetime.now(dt.timezone.utc)
    counts = {"queries_run": 0, "pages_fetched": 0, "credits_spent": 0, "jobs_seen": 0,
              "jobs_new": 0, "jobs_updated": 0, "employers_distinct": 0, "rows_materialized": 0}
    status, error, run_root = "error", None, uuid.uuid4().hex
    so = _r2_storage_options()
    conn = _open_conn()
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute(f"SELECT {','.join(_LANCE_COLS)} FROM ops.jsearch_capture_postings "
                    "ORDER BY last_seen_at DESC")
        recs = cur.fetchall()
        counts["rows_materialized"] = len(recs)
        counts["employers_distinct"] = len({(r[_LANCE_COLS.index('employer_domain')]
                                             or r[_LANCE_COLS.index('employer_name')])
                                            for r in recs if any(r)})
        if not recs:
            log("no rows in ops.jsearch_capture_postings — nothing to materialize")
            status = "success"
            _record_run(cur, "materialize", run_root, None, counts, status, None, started,
                        dt.datetime.now(dt.timezone.utc))
            return {"feed": FEED, "mode": "materialize", "status": status, **counts}
        # Columnar (dict-of-lists) → pa.table. Arrow-in, no pandas (data-plane law).
        data = {c: [r[i] for r in recs] for i, c in enumerate(_LANCE_COLS)}
        tbl = pa.table(data)
        lance.write_dataset(tbl, DATASET_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
        ds = lance.dataset(DATASET_URI, storage_options=so)
        n = ds.count_rows()
        assert n == len(recs), f"write-integrity {n} != {len(recs)}"
        names = set(ds.schema.names)
        for col in BTREE_INDEXES:
            if col in names:
                ds.create_scalar_index(col, index_type="BTREE", replace=True)
        for col in BITMAP_INDEXES:
            if col in names:
                ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        log(f"WROTE {DATASET_URI} rows={n:,} employers={counts['employers_distinct']:,} + indexes")
        status = "success"
        _record_run(cur, "materialize", run_root, None, counts, status, None, started,
                    dt.datetime.now(dt.timezone.utc))
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
        status = "error"
        try:
            _record_run(conn.cursor(), "materialize", run_root, None, counts, status, error, started,
                        dt.datetime.now(dt.timezone.utc))
        except Exception as exc2:  # noqa: BLE001
            log(f"WARN: run-ledger write failed: {exc2}")
        raise
    finally:
        conn.close()
        _post_callback(trigger_callback_url,
                       {"status": status, "feed": FEED, "mode": "materialize", "error": error,
                        **counts})
    return {"feed": FEED, "mode": "materialize", "status": status, **counts}


# ── Modal execution plane — endpoint-less workers, spawned by the Universal Dispatcher ─────────
image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.1",
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).add_local_python_source("core", "pipelines")

app = modal.App("jsearch-capture-roles", image=image)

SECRETS = [
    modal.Secret.from_name("r2-credentials"),
    modal.Secret.from_name("hqx-postgres"),
    modal.Secret.from_name("openwebninja-api"),
]


@app.function(secrets=SECRETS, timeout=60 * 55, memory=4096, cpu=2.0)
def harvest_capture_roles(trigger_callback_url: str | None = None, mode: str = "incremental",
                          date_posted: str = "3days", country: str = DEFAULT_COUNTRY,
                          max_pages: int = MAX_PAGES_PER_QUERY, run_root: str | None = None,
                          queries: list[str] | None = None, workers: int = DEFAULT_WORKERS) -> dict:
    """Harvest capture-role postings → ops.jsearch_capture_postings. mode ∈ {incremental, backfill};
    incremental cron passes date_posted='3days', backfill passes 'all'. ``queries`` overrides the
    default title set (e.g. a geo-sharded backfill matrix from ``build_backfill_queries``)."""
    return _harvest(mode if mode in ("backfill", "incremental") else "incremental",
                    date_posted, country, max_pages, run_root or uuid.uuid4().hex,
                    trigger_callback_url, queries=queries, workers=workers)


@app.function(secrets=SECRETS, timeout=60 * 30, memory=4096, cpu=2.0)
def materialize_capture_roles(trigger_callback_url: str | None = None) -> dict:
    """Hydrate the Lance SoR from ops.jsearch_capture_postings (+ BTREE/BITMAP indexes)."""
    return _materialize(trigger_callback_url)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    conn = _open_conn()
    try:
        conn.cursor().execute(OPS_DDL)
    finally:
        conn.close()
    return {"tables": ["ops.jsearch_capture_postings", "ops.jsearch_capture_runs"]}


@app.function(secrets=SECRETS, timeout=60 * 5)
def verify() -> dict:
    return _verify_payload()


# ── shared read-backs / local CLI ──────────────────────────────────────────────────────────────
def _status_payload() -> dict:
    conn = _open_conn()
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute("""SELECT count(*),
              count(*) FILTER (WHERE employer_is_confidential),
              count(*) FILTER (WHERE employer_is_staffing),
              count(DISTINCT coalesce(employer_domain, lower(employer_name)))
            FROM ops.jsearch_capture_postings""")
        n, conf, staff, emp = cur.fetchone()
        cur.execute("""SELECT publisher, count(*) FROM ops.jsearch_capture_postings
                       GROUP BY 1 ORDER BY 2 DESC LIMIT 12""")
        by_pub = cur.fetchall()
        cur.execute("""SELECT mode, status, jobs_seen, jobs_new, credits_spent, recorded_at
                       FROM ops.jsearch_capture_runs ORDER BY recorded_at DESC LIMIT 5""")
        runs = [dict(zip(["mode", "status", "jobs_seen", "jobs_new", "credits_spent", "recorded_at"], r))
                for r in cur.fetchall()]
    finally:
        conn.close()
    return {"postings": n, "confidential": conf, "staffing": staff, "distinct_employers": emp,
            "by_publisher": [{"publisher": p, "n": c} for p, c in by_pub], "recent_runs": runs}


def _verify_payload() -> dict:
    import lance

    so = _r2_storage_options()
    try:
        ds = lance.dataset(DATASET_URI, storage_options=so)
    except Exception as exc:  # noqa: BLE001
        return {"uri": DATASET_URI, "exists": False, "error": str(exc)}
    idx = sorted(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                 for i in ds.list_indices())
    return {"uri": DATASET_URI, "exists": True, "rows": ds.count_rows(),
            "schema": [f.name for f in ds.schema], "indices": idx}


def cmd_init_ops(_args) -> None:
    conn = _open_conn()
    try:
        conn.cursor().execute(OPS_DDL)
    finally:
        conn.close()
    log("ops.jsearch_capture_postings + ops.jsearch_capture_runs DDL applied")


def cmd_smoke(_args) -> None:
    import json

    if not key_present():
        raise RuntimeError("OPENWEBNINJA_API_KEY absent — check doppler core-x/prd injection")
    env = jsearch_search("capture manager", country=DEFAULT_COUNTRY, date_posted="all")
    rows = [_project_job(j, "capture manager", "smoke") for j in env["jobs"]]
    print(json.dumps({"ok": env["ok"], "http_status": env["http_status"], "credits": env["credits"],
                      "cursor_present": bool(env["cursor"]), "n_jobs": len(env["jobs"]),
                      "sample": [{k: r[k] for k in ("employer_name", "employer_domain", "publisher",
                                                    "employer_is_staffing", "employer_is_confidential",
                                                    "job_title")} for r in rows[:8]]},
                     indent=2, default=str))


def cmd_harvest(args) -> None:
    import json

    mode = "backfill" if args.date_posted == "all" else "incremental"
    queries = None
    if getattr(args, "geo", False) or getattr(args, "titles", "core") == "extended":
        queries = build_backfill_queries(geo=getattr(args, "geo", False),
                                         extended=(getattr(args, "titles", "core") == "extended"))
        log(f"query matrix: {len(queries)} queries "
            f"(geo={getattr(args, 'geo', False)}, titles={getattr(args, 'titles', 'core')})")
    out = _harvest(mode, args.date_posted, DEFAULT_COUNTRY, args.max_pages,
                   args.run_root or uuid.uuid4().hex, None, queries=queries, workers=args.workers)
    print(json.dumps(out, indent=2, default=str))


def cmd_backfill(args) -> None:
    args.date_posted = "all"
    cmd_harvest(args)


def cmd_materialize(_args) -> None:
    import json

    print(json.dumps(_materialize(None), indent=2, default=str))


def cmd_status(_args) -> None:
    import json

    print(json.dumps(_status_payload(), indent=2, default=str))


def cmd_verify(_args) -> None:
    import json

    print(json.dumps(_verify_payload(), indent=2, default=str))


def main() -> None:
    ap = argparse.ArgumentParser(description="JSearch capture-roles harvester (OpenWeb Ninja).")
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("init_ops")
    sub.add_parser("smoke")
    for name in ("harvest", "backfill"):
        p = sub.add_parser(name)
        p.add_argument("--date-posted", dest="date_posted", default=DEFAULT_DATE_POSTED,
                       choices=["all", "month", "week", "3days", "today"])
        p.add_argument("--max-pages", dest="max_pages", type=int, default=MAX_PAGES_PER_QUERY)
        p.add_argument("--run-root", dest="run_root", default=None)
        p.add_argument("--geo", action="store_true",
                       help="geo-shard the core titles across govcon hubs (saturation backfill)")
        p.add_argument("--titles", choices=["core", "extended"], default="core",
                       help="core = 7 canonical titles; extended = + adjacent capture/BD/proposal titles")
        p.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                       help="concurrent per-query workers")
    sub.add_parser("materialize")
    sub.add_parser("status")
    sub.add_parser("verify")
    args = ap.parse_args()

    dispatch = {"init_ops": cmd_init_ops, "smoke": cmd_smoke, "harvest": cmd_harvest,
                "backfill": cmd_backfill, "materialize": cmd_materialize, "status": cmd_status,
                "verify": cmd_verify}
    fn = dispatch.get(args.cmd)
    if not fn:
        ap.print_help(); sys.exit(2)
    fn(args)


if __name__ == "__main__":
    main()
