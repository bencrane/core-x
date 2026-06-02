"""Compute worker — PropertyRadar property/owner harvest (POST /v1/properties) with a
two-stage Quota Governor.

Part of the ``propertyradar-pipelines`` Modal app. Endpoint-less functions, spawned by
the Universal Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints.
Clean-room data plane: no Iceberg, no Polaris — the billable rows are landed to R2 as
ephemeral ZSTD Parquet, DuckDB does 100% of the transform, Lance is written straight to R2.

This is a BILLABLE feed: PropertyRadar charges one credit per property record purchased.
Every penny of spend is gated behind a mandatory free preview, enforced in code by the
Quota Governor below — there is no path to ``Purchase=1`` that does not first clear the
threshold check.

╔══════════════════════ THE QUOTA GOVERNOR (the load-bearing contract) ══════════════════╗
║ Stage 1 — PREVIEW (free, always runs first):                                            ║
║   POST /v1/properties?Purchase=0&Fields=RadarID for the operator's Criteria. Purchase=0 ║
║   is the API's free-of-charge count path; we read ``totalResultCount`` off the envelope.║
║ Stage 2 — THRESHOLD EVALUATION:                                                         ║
║   Compare ``totalResultCount`` against the operator's ``--max-allowed-spend``.          ║
║   THE HARD STOP — if totalResultCount > max_allowed_spend the worker aborts cleanly      ║
║   (NO purchase, exit 0), printing the EXACT credit cost so the operator can tighten the  ║
║   Criteria. An over-budget query is the governor working as designed, NOT a failure.    ║
║   ``max_allowed_spend`` DEFAULTS TO 0 → an un-parameterized run is preview-only and      ║
║   spends nothing; the operator must consciously authorize a credit ceiling.             ║
║ Stage 3 — BILLABLE EXECUTION (only reachable when totalResultCount <= max_allowed_spend):║
║   Flip to Purchase=1 & Fields=All and paginate (Start/Limit) the real rows, landing each║
║   page as ZSTD Parquet. credits_consumed = rows actually retrieved under Purchase=1.    ║
╚═════════════════════════════════════════════════════════════════════════════════════════╝

Data plane (one Modal invocation; two Lance datasets):
    Stage-3 pages → /tmp ZSTD Parquet (lossless ``record`` JSON col) + R2 landing archive
      → DuckDB read_parquet → key normalization (LPAD fips5, APN_normalized, parcel_key)
      → property grain   → lance.write_dataset(.../propertyradar_property_lance/, v2.1, overwrite)
      → UNNEST(Persons)  → lance.write_dataset(.../propertyradar_person_lance/,   v2.1, overwrite)
      → BTREE(RadarID, parcel_key, fips5) on property; BTREE(RadarID, parcel_key, person_key) on person

Control plane (Trigger v4 durable callback): the worker accepts ``trigger_callback_url`` and,
on terminal state (success | aborted_over_budget | error), (1) writes the run row to
``ops.propertyradar_runs`` via psycopg — logging the parameters used, matches found, and exact
credits consumed — and (2) POSTs a FLAT JSON body to that url to wake the suspended Trigger run.

    modal run    pipelines/propertyradar/properties.py::setup                       # create ops.propertyradar_runs
    modal run    pipelines/propertyradar/properties.py::run --max-allowed-spend 0   # preview-only (free; prints cost)
    modal run    pipelines/propertyradar/properties.py::run --max-allowed-spend 500 --criteria-file crit.json
    modal run    pipelines/propertyradar/properties.py::run_verify                  # read-back proof
    modal run    pipelines/propertyradar/properties.py::reindex                     # rebuild scalar indexes only
    modal deploy pipelines/propertyradar/properties.py                              # publish for the dispatcher
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
FEED = "propertyradar"

# PropertyRadar REST API. POST /v1/properties is both the free count path (Purchase=0) and
# the billable retrieval path (Purchase=1). Base host is env-overridable so the resolver is
# not pinned to one host.
API_BASE = os.environ.get("PROPERTYRADAR_API_BASE", "https://api.propertyradar.com")
PROPERTIES_PATH = "/v1/properties"

# Governor field contracts (directive-literal; env-overridable). Preview asks for ONLY the
# RadarID so the free count call carries the smallest possible payload; purchase asks for All.
PREVIEW_FIELDS = os.environ.get("PROPERTYRADAR_PREVIEW_FIELDS", "RadarID")
PURCHASE_FIELDS = os.environ.get("PROPERTYRADAR_PURCHASE_FIELDS", "All")

# Pagination. PropertyRadar caps a single /v1/properties response at 1000 rows; Start is a
# 1-based row index. Preview needs only the envelope count → Limit=1.
PAGE_LIMIT = int(os.environ.get("PROPERTYRADAR_PAGE_LIMIT", "1000"))
PREVIEW_LIMIT = 1
MAX_PAGES = 100000  # runaway-loop backstop (≥ 100M rows before it bites)

# Lance system-of-record datasets (env-overridable). Property grain + the exploded person grain.
PROPERTY_URI = os.environ.get(
    "PROPERTYRADAR_PROPERTY_LANCE_URI", "s3://data-sink/active/propertyradar_property_lance/"
)
PERSON_URI = os.environ.get(
    "PROPERTYRADAR_PERSON_LANCE_URI", "s3://data-sink/active/propertyradar_person_lance/"
)

# R2 landing archive for the billable pages — a durable raw snapshot of purchased data so a
# transform crash NEVER forces a re-purchase (re-spend). Keyed per run.
LANDING_PREFIX = "landing/propertyradar/"

SCRATCH_DIR = "/tmp/propertyradar"
STAGE_DIR = "/tmp/propertyradar/stage"  # the staged ZSTD Parquet pages DuckDB reads

# Lance fragment sizing (fleet constants) + net-new dataset version pin.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan. BTREE on every load-bearing resolution key (directive mandate).
#   property: RadarID (property PK), parcel_key (fips5+APN composite), fips5 (county rollups).
#   person:   RadarID + parcel_key (FK back to the property), person_key (person identity).
PROPERTY_BTREE = ["RadarID", "parcel_key", "fips5"]
PERSON_BTREE = ["RadarID", "parcel_key", "person_key"]

# Governor decision vocabulary (also the ops.governor_decision values).
GOV_PREVIEW_ONLY = "preview_only"        # max_allowed_spend=0 (or totalResultCount=0): nothing purchased
GOV_OVER_BUDGET = "aborted_over_budget"  # totalResultCount > max_allowed_spend: HARD STOP, no spend
GOV_AUTHORIZED = "authorized"            # totalResultCount <= max_allowed_spend: purchase ran

# Mirrored verbatim by pipelines/propertyradar/ops_propertyradar_runs.sql. Applied by init_db
# and re-asserted idempotently on every _record_run.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.propertyradar_runs (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed              text        NOT NULL,
    phase             text        NOT NULL,     -- 'ingest' | 'reindex'
    property_uri      text,
    person_uri        text,
    criteria          jsonb,                    -- parameters used: the PropertyRadar Criteria array
    max_allowed_spend bigint,                   -- the operator's --max-allowed-spend ceiling
    page_limit        integer,                  -- pagination Limit used for billable retrieval
    preview_count     bigint,                   -- matches found: envelope totalResultCount (free preview)
    credits_consumed  bigint,                   -- exact credits spent = rows retrieved under Purchase=1
    governor_decision text,                     -- 'preview_only' | 'aborted_over_budget' | 'authorized'
    pages             bigint,                   -- billable pages paginated
    property_rows     bigint,                   -- committed property-grain Lance rows
    person_rows       bigint,                   -- committed person-grain Lance rows (exploded Persons)
    property_indexes  jsonb,                    -- BTREE columns built on the property dataset
    person_indexes    jsonb,                    -- BTREE columns built on the person dataset
    status            text        NOT NULL,     -- 'success' | 'aborted_over_budget' | 'error'
    error             text,
    started_at        timestamptz,
    completed_at      timestamptz,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS propertyradar_runs_phase_idx       ON ops.propertyradar_runs (phase);
CREATE INDEX IF NOT EXISTS propertyradar_runs_status_idx      ON ops.propertyradar_runs (status);
CREATE INDEX IF NOT EXISTS propertyradar_runs_recorded_at_idx ON ops.propertyradar_runs (recorded_at DESC);
"""

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table / JSON funcs; <2 stays below the v2.0 break
        "lancedb>=0.15",
        "pylance>=7",            # provides `import lance`; lancedb does not re-export it
        "pyarrow>=17",
        "boto3>=1.35",           # R2 landing archive of the billable pages
        "requests>=2.32",        # PropertyRadar API + Trigger waitpoint callback
        "psycopg[binary]>=3.2",  # ops.* terminal state
    )
    .env(
        # BTREE scalar-index builds sort the column; force the in-memory sort path so the
        # high-cardinality string BTREEs (RadarID, parcel_key) build deterministically
        # (fleet convention; lance-format/lance#2650). Trivial at free-trial scale.
        {"LANCE_BYPASS_SPILLING": "true"}
    )
)

app = modal.App("propertyradar-pipelines", image=image)


# ── R2 / object-store plumbing (mirrors the epiq/cms workers verbatim) ──
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the r2-credentials Modal secret."""
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _s3_client():
    """boto3 S3 client for R2. checksum behaviour forced to ``when_required`` (R2 semantics)."""
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


# ── PropertyRadar API layer ──────────────────────────────────────────────────────
def _api_token() -> str:
    """The PropertyRadar API token (Bearer). Required for both the free preview and the
    billable retrieval — fail loud if absent rather than emitting an unauthenticated call."""
    token = os.environ.get("PROPERTYRADAR_API_TOKEN")
    if not token:
        raise RuntimeError(
            "PROPERTYRADAR_API_TOKEN not set (attach the propertyradar-api Modal secret)."
        )
    return token


def _new_session():
    """A requests.Session pre-loaded with the Bearer auth + JSON headers."""
    import requests

    sess = requests.Session()
    sess.headers.update({
        "Authorization": f"Bearer {_api_token()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "core-x/propertyradar (+data-engine)",
    })
    return sess


def _request(sess, url: str, json_body: dict, attempts: int = 6, timeout=(15, 180)):
    """Rate-limit-aware POST. 429 → honor Retry-After else exponential backoff; 5xx /
    connection errors → backoff. Other 4xx (401 bad token, 400 bad Criteria) FAIL LOUD —
    those are never transient and must not be silently retried into a wall. Raises after
    ``attempts`` exhausted."""
    import random
    import time

    import requests

    backoff = (2, 5, 15, 45, 120, 300)
    last: Exception | None = None
    for i in range(attempts):
        wait = backoff[min(i, len(backoff) - 1)]
        try:
            resp = sess.post(url, json=json_body, timeout=timeout)
            sc = resp.status_code
            if sc < 300:
                return resp
            if sc == 429:
                ra = resp.headers.get("Retry-After", "")
                wait = int(ra) if ra.isdigit() else wait
                print(f"429 {url} → sleep {wait}s ({i + 1}/{attempts})")
            elif 500 <= sc < 600:
                print(f"{sc} {url} → backoff {wait}s ({i + 1}/{attempts})")
            else:
                # 400/401/403/404 — surface the body; this is a request/auth defect, not a retry.
                raise RuntimeError(f"PropertyRadar {sc}: {resp.text[:500]}")
        except requests.RequestException as exc:
            last = exc
            print(f"req error {url}: {exc} ({i + 1}/{attempts})")
        time.sleep(wait + random.random())
    raise RuntimeError(f"request failed after {attempts} attempts: POST {url} ({last})")


def _query(sess, criteria: list, *, fields: str, purchase: int, start: int, limit: int) -> dict:
    """One POST /v1/properties call. ``purchase`` (0|1) and ``fields`` are the governor's
    levers; ``Criteria`` is the operator's query body. Returns the decoded envelope
    ({success, totalResultCount, resultCount, results})."""
    from urllib.parse import urlencode

    qs = urlencode({"Fields": fields, "Purchase": purchase, "Start": start, "Limit": limit})
    url = f"{API_BASE}{PROPERTIES_PATH}?{qs}"
    resp = _request(sess, url, {"Criteria": criteria})
    env = resp.json()
    if not isinstance(env, dict):
        raise RuntimeError(f"PropertyRadar envelope is {type(env).__name__}, expected object")
    return env


def _preview_total(sess, criteria: list) -> int:
    """STAGE 1 — free preview. POST Purchase=0 & Fields=RadarID and read the authoritative
    ``totalResultCount`` off the envelope. This call costs ZERO credits."""
    env = _query(sess, criteria, fields=PREVIEW_FIELDS, purchase=0, start=1, limit=PREVIEW_LIMIT)
    total = env.get("totalResultCount")
    if total is None:
        raise RuntimeError(
            f"preview envelope missing totalResultCount; cannot govern spend. Got keys: "
            f"{sorted(env.keys())}"
        )
    return int(total)


# ── Landing: bill-once, transform-many. Each billable page → ZSTD Parquet. ──────────
def _stage_page(con, raw_bytes: bytes, page_no: int) -> tuple[str, int]:
    """Persist one billable page as a staged ZSTD Parquet shard (DuckDB COPY). The page's
    ``results`` array is exploded one-row-per-property; the WHOLE record is preserved
    losslessly in a ``record`` JSON column (so Fields=All's wide/variable schema is never
    truncated) alongside the pulled-out identity keys + the Persons array. Returns
    (parquet_path, row_count). Python writes the raw bytes; DuckDB does the shaping."""
    os.makedirs(STAGE_DIR, exist_ok=True)
    raw_path = os.path.join(STAGE_DIR, f"raw_{page_no:05d}.json")
    pq_path = os.path.join(STAGE_DIR, f"page_{page_no:05d}.parquet")
    with open(raw_path, "wb") as fh:
        fh.write(raw_bytes)

    # read_json the single envelope object → unnest results → typed keys + lossless record.
    con.execute(f"""
        COPY (
            SELECT
                nullif(trim(rec->>'RadarID'), '')                       AS radar_id,
                nullif(trim(rec->>'FIPS'), '')                          AS fips,
                nullif(trim(rec->>'APN'), '')                           AS apn,
                CASE WHEN json_type(rec->'Persons') = 'ARRAY'
                     THEN rec->'Persons' END                            AS persons,
                rec                                                     AS record
            FROM (
                SELECT unnest(results) AS rec
                FROM read_json('{raw_path}', format='auto',
                               columns={{'results': 'JSON[]'}},
                               maximum_object_size=1073741824, ignore_errors=true)
                WHERE results IS NOT NULL
            )
        ) TO '{pq_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{pq_path}')").fetchone()[0]
    try:
        os.remove(raw_path)  # raw JSON is transient; the ZSTD Parquet is the durable artifact
    except OSError:
        pass
    return pq_path, int(n)


def _land_archive(s3, run_id: str, pq_path: str) -> None:
    """Mirror a staged Parquet page to the R2 landing archive (durable raw snapshot of
    purchased data). Best-effort: a failed archive upload must not lose data we already
    have on local disk for the transform."""
    key = f"{LANDING_PREFIX}{run_id}/{os.path.basename(pq_path)}"
    try:
        s3.upload_file(pq_path, BUCKET, key)
    except Exception as exc:  # noqa: BLE001 — archive is durability insurance, not the data path
        print(f"WARN: landing archive upload failed for {pq_path}: {exc}")


def _paginate_billable(sess, con, s3, criteria: list, run_id: str, total: int,
                       limit: int) -> tuple[list[str], int, int]:
    """STAGE 3 — billable retrieval. Only called once the governor has AUTHORIZED spend.
    Flip Purchase=1 & Fields=All and walk Start/Limit until the matched set is exhausted,
    staging each page to ZSTD Parquet (+ R2 archive). Returns (parquet_paths, pages,
    rows_retrieved). rows_retrieved is the EXACT credit spend."""
    import json

    paths: list[str] = []
    pages = 0
    rows = 0
    start = 1
    while pages < MAX_PAGES:
        env = _query(sess, criteria, fields=PURCHASE_FIELDS, purchase=1, start=start, limit=limit)
        results = env.get("results") or []
        got = len(results)
        if got == 0:
            break
        pq_path, n = _stage_page(con, json.dumps({"results": results}).encode("utf-8"), pages)
        _land_archive(s3, run_id, pq_path)
        paths.append(pq_path)
        pages += 1
        rows += n
        start += got
        print(f"  page {pages}: retrieved {got} (Purchase=1), cumulative {rows}/{total}")
        if total and start > total:
            break
        if got < limit:  # short page → last page
            break
    return paths, pages, rows


# ── DuckDB key normalization → property + exploded person grains (100% in SQL) ──────
# fips5: LPAD the digit-cleaned FIPS to a 5-char string (directive). apn_normalized: strip to
# uppercase alphanumerics. parcel_key: fips5 || '-' || apn_normalized, but ONLY when both
# components are present (a half-formed composite is useless as a resolution key → NULL).
_FIPS5 = "lpad(nullif(regexp_replace(coalesce(fips, ''), '[^0-9]', '', 'g'), ''), 5, '0')"
_APN_NORM = "nullif(upper(regexp_replace(coalesce(apn, ''), '[^A-Za-z0-9]', '', 'g')), '')"


def _property_sql(stage_glob: str) -> str:
    """Property grain — one row per RadarID, normalized identity keys, a curated set of
    commonly-present address columns, and the lossless ``property_json``.

    Dedup runs FIRST, on the raw passthrough columns (the ``deduped`` CTE: a window over the
    untouched ``radar_id``), and key normalization runs in a SEPARATE later layer over the
    already-unique rows. This separation is deliberate: DuckDB mis-binds a projected
    expression (fips5 → its raw input) when a QUALIFY/window and the normalization
    expressions share one projection, so the window must never coexist with the
    normalization in the same SELECT."""
    return f"""
WITH staged AS (
    SELECT radar_id, fips, apn, persons, record
    FROM read_parquet('{stage_glob}')
    WHERE radar_id IS NOT NULL
),
deduped AS (
    SELECT radar_id, fips, apn, persons, record
    FROM staged
    QUALIFY row_number() OVER (PARTITION BY radar_id ORDER BY radar_id) = 1
),
normalized AS (
    SELECT
        radar_id                                   AS "RadarID",
        {_FIPS5}                                   AS fips5,
        apn                                        AS apn,
        {_APN_NORM}                                AS apn_normalized,
        record
    FROM deduped
)
SELECT
    "RadarID",
    fips5,
    apn,
    apn_normalized,
    CASE WHEN fips5 IS NOT NULL AND apn_normalized IS NOT NULL
         THEN fips5 || '-' || apn_normalized END   AS parcel_key,
    nullif(trim(record->>'Address'), '')           AS address,
    nullif(trim(record->>'City'), '')              AS city,
    nullif(trim(record->>'State'), '')             AS state,
    nullif(trim(record->>'ZipFive'), '')           AS zip5,
    nullif(trim(record->>'County'), '')            AS county,
    record                                         AS property_json,
    now()                                          AS ingested_at
FROM normalized
"""


def _person_sql(stage_glob: str) -> str:
    """Person grain — UNNEST the per-property Persons array into one row per (property,
    person). Carries the parent resolution keys (RadarID, parcel_key, fips5) for join-back,
    the pulled-out person identity fields, and the lossless ``person_json``.

    Properties are deduped FIRST (window over the raw ``radar_id``) so a property that
    appeared on two pages does not double its persons; key normalization + the UNNEST run in
    later layers that carry no window (see _property_sql for why that separation matters)."""
    return f"""
WITH staged AS (
    SELECT radar_id, fips, apn, persons
    FROM read_parquet('{stage_glob}')
    WHERE radar_id IS NOT NULL AND persons IS NOT NULL
),
deduped AS (
    SELECT radar_id, fips, apn, persons
    FROM staged
    QUALIFY row_number() OVER (PARTITION BY radar_id ORDER BY radar_id) = 1
),
normalized AS (
    SELECT
        radar_id                                   AS "RadarID",
        {_FIPS5}                                   AS fips5,
        {_APN_NORM}                                AS apn_normalized,
        persons
    FROM deduped
),
exploded AS (
    SELECT
        "RadarID",
        fips5,
        CASE WHEN fips5 IS NOT NULL AND apn_normalized IS NOT NULL
             THEN fips5 || '-' || apn_normalized END AS parcel_key,
        unnest(CAST(persons AS JSON[]))            AS person
    FROM normalized
    WHERE json_array_length(persons) > 0
)
SELECT
    "RadarID",
    parcel_key,
    fips5,
    nullif(trim(person->>'PersonKey'), '')         AS person_key,
    nullif(trim(person->>'EntityID'), '')          AS entity_id,
    nullif(trim(person->>'Name'), '')              AS person_name,
    nullif(trim(person->>'Type'), '')              AS person_type,
    TRY_CAST(person->>'PhoneAvailability' AS BOOLEAN) AS phone_availability,
    TRY_CAST(person->>'EmailAvailability' AS BOOLEAN) AS email_availability,
    person                                         AS person_json,
    now()                                          AS ingested_at
FROM exploded
"""


def _duck():
    """In-memory DuckDB connection (spill to NVMe scratch; trivial at free-trial scale)."""
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4;")
    os.makedirs(f"{SCRATCH_DIR}/spill", exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{SCRATCH_DIR}/spill';")
    return con


# ── Lance write + R2-direct BTREE indexing (dominant fleet pattern; free-trial volume) ──
def _write_lance(table, uri: str, so: dict) -> None:
    import lance

    lance.write_dataset(
        table, uri, mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )


def _build_indexes(uri: str, cols: list[str], so: dict) -> list[str]:
    """Build BTREE scalar indexes directly on the R2 dataset (small single-fragment dataset,
    well under R2's multipart-escalation threshold). replace=True → idempotent. An index miss
    is logged, never fatal (the data write is the critical artifact)."""
    import lance

    ds = lance.dataset(uri, storage_options=so)
    schema_cols = set(ds.schema.names)
    built: list[str] = []
    for col in cols:
        if col not in schema_cols:
            print(f"  WARN index column {col!r} not in schema; skipping")
            continue
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(col)
            print(f"  BTREE ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    return built


# ── Terminal state + callback ──────────────────────────────────────────────────────
def _record_run(*, phase, criteria, max_allowed_spend, page_limit, preview_count,
                credits_consumed, governor_decision, pages, property_rows, person_rows,
                property_indexes, person_indexes, status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.propertyradar_runs (psycopg). Logs the parameters used, matches
    found, and exact credits consumed. Best-effort: never let an audit-write failure crash an
    otherwise-good ingest."""
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.propertyradar_runs
                    (feed, phase, property_uri, person_uri, criteria, max_allowed_spend,
                     page_limit, preview_count, credits_consumed, governor_decision, pages,
                     property_rows, person_rows, property_indexes, person_indexes, status,
                     error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, phase, PROPERTY_URI, PERSON_URI,
                 Jsonb(criteria if criteria is not None else []), max_allowed_spend,
                 page_limit, preview_count, credits_consumed, governor_decision, pages,
                 property_rows, person_rows, Jsonb(property_indexes), Jsonb(person_indexes),
                 status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint URL. FLAT JSON body — no envelope;
    the whole body becomes result.output. A few retries for delivery reliability."""
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


# ── Worker functions ────────────────────────────────────────────────────────────────
@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
        modal.Secret.from_name("propertyradar-api"),
    ],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def ingest_propertyradar(
    criteria: list | None = None,
    max_allowed_spend: int = 0,
    page_limit: int | None = None,
    trigger_callback_url: str | None = None,
) -> dict:
    """Quota-governed PropertyRadar ingest. Stage 1 free preview → Stage 2 threshold check →
    (only if authorized) Stage 3 billable pagination → DuckDB key normalization → property +
    exploded-person Lance datasets → BTREE indexes, then record ops.* state and wake Trigger.

    ``max_allowed_spend=0`` (the default) is preview-only: it prints the exact credit cost and
    spends nothing. Over-budget is a CLEAN terminal state (status=aborted_over_budget), NOT a
    raised failure — the governor refusing to overspend is the system working as designed. The
    function only re-raises on a genuine error (API/transform/write fault)."""
    import datetime as dt
    import shutil

    started_at = dt.datetime.now(dt.timezone.utc)
    run_id = started_at.strftime("%Y%m%dT%H%M%SZ")
    criteria = criteria if criteria is not None else []
    if not isinstance(criteria, list):
        raise ValueError(f"criteria must be a list of PropertyRadar Criteria, got {type(criteria).__name__}")
    if not criteria:
        print("WARN: empty Criteria → matches the ENTIRE PropertyRadar universe. The governor "
              "will hard-stop unless --max-allowed-spend is enormous.")
    limit = int(page_limit or PAGE_LIMIT)
    max_allowed_spend = int(max_allowed_spend)

    preview_count = 0
    credits_consumed = 0
    pages = 0
    property_rows = person_rows = 0
    property_indexes: list[str] = []
    person_indexes: list[str] = []
    governor_decision = GOV_PREVIEW_ONLY
    status, error = "error", None

    shutil.rmtree(STAGE_DIR, ignore_errors=True)
    try:
        so = _r2_storage_options()
        s3 = _s3_client()
        sess = _new_session()

        # ── STAGE 1 — free preview ────────────────────────────────────────────────────
        preview_count = _preview_total(sess, criteria)
        print(f"STAGE 1 preview (Purchase=0, Fields={PREVIEW_FIELDS}): "
              f"totalResultCount={preview_count:,}")

        # ── STAGE 2 — threshold evaluation / THE HARD STOP ───────────────────────────
        if preview_count > max_allowed_spend:
            governor_decision = GOV_OVER_BUDGET
            status = GOV_OVER_BUDGET
            print("─" * 72)
            print(f"QUOTA GOVERNOR — HARD STOP. Matched {preview_count:,} properties; the "
                  f"required credit cost ({preview_count:,}) EXCEEDS the authorized ceiling "
                  f"--max-allowed-spend={max_allowed_spend:,}.")
            print(f"REQUIRED CREDITS: {preview_count:,}   AUTHORIZED: {max_allowed_spend:,}   "
                  f"OVER BY: {preview_count - max_allowed_spend:,}")
            print("No data was purchased. Tighten the Criteria or raise --max-allowed-spend.")
            print("─" * 72)
            # NOT an error — clean terminal state. Skip straight to the ledger + callback.
            return _finalize(
                phase="ingest", criteria=criteria, max_allowed_spend=max_allowed_spend,
                page_limit=limit, preview_count=preview_count, credits_consumed=0,
                governor_decision=governor_decision, pages=0, property_rows=0, person_rows=0,
                property_indexes=[], person_indexes=[], status=status, error=None,
                started_at=started_at, completed_at=dt.datetime.now(dt.timezone.utc),
                trigger_callback_url=trigger_callback_url, raise_on_error=False,
            )

        if preview_count == 0:
            governor_decision = GOV_PREVIEW_ONLY
            status = "success"
            print("Preview matched 0 properties; nothing to purchase.")
            return _finalize(
                phase="ingest", criteria=criteria, max_allowed_spend=max_allowed_spend,
                page_limit=limit, preview_count=0, credits_consumed=0,
                governor_decision=governor_decision, pages=0, property_rows=0, person_rows=0,
                property_indexes=[], person_indexes=[], status=status, error=None,
                started_at=started_at, completed_at=dt.datetime.now(dt.timezone.utc),
                trigger_callback_url=trigger_callback_url, raise_on_error=False,
            )

        # ── STAGE 3 — billable execution (authorized) ────────────────────────────────
        governor_decision = GOV_AUTHORIZED
        print(f"STAGE 3 authorized: {preview_count:,} <= {max_allowed_spend:,}. "
              f"Purchasing (Purchase=1, Fields={PURCHASE_FIELDS}, Limit={limit}).")
        con = _duck()
        try:
            paths, pages, credits_consumed = _paginate_billable(
                sess, con, s3, criteria, run_id, preview_count, limit
            )
            print(f"Billable retrieval done: {pages} page(s), {credits_consumed:,} rows purchased "
                  f"(= credits consumed).")
            if credits_consumed == 0:
                status = "success"
                return _finalize(
                    phase="ingest", criteria=criteria, max_allowed_spend=max_allowed_spend,
                    page_limit=limit, preview_count=preview_count, credits_consumed=0,
                    governor_decision=governor_decision, pages=pages, property_rows=0,
                    person_rows=0, property_indexes=[], person_indexes=[], status=status,
                    error=None, started_at=started_at,
                    completed_at=dt.datetime.now(dt.timezone.utc),
                    trigger_callback_url=trigger_callback_url, raise_on_error=False,
                )

            # ── Transform: staged ZSTD Parquet → property + exploded-person Arrow ──────
            stage_glob = f"{STAGE_DIR}/page_*.parquet"
            property_tbl = con.execute(_property_sql(stage_glob)).to_arrow_table()
            person_tbl = con.execute(_person_sql(stage_glob)).to_arrow_table()
            property_rows = property_tbl.num_rows
            person_rows = person_tbl.num_rows
            print(f"normalized: property_rows={property_rows:,} person_rows={person_rows:,}")
        finally:
            con.close()

        # ── Materialize both grains to R2 Lance + BTREE indexes ───────────────────────
        _write_lance(property_tbl, PROPERTY_URI, so)
        print(f"wrote property Lance → {PROPERTY_URI}")
        property_indexes = _build_indexes(PROPERTY_URI, PROPERTY_BTREE, so)
        del property_tbl

        _write_lance(person_tbl, PERSON_URI, so)
        print(f"wrote person Lance → {PERSON_URI}")
        person_indexes = _build_indexes(PERSON_URI, PERSON_BTREE, so)
        del person_tbl

        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        shutil.rmtree(STAGE_DIR, ignore_errors=True)

    return _finalize(
        phase="ingest", criteria=criteria, max_allowed_spend=max_allowed_spend,
        page_limit=limit, preview_count=preview_count, credits_consumed=credits_consumed,
        governor_decision=governor_decision, pages=pages, property_rows=property_rows,
        person_rows=person_rows, property_indexes=property_indexes,
        person_indexes=person_indexes, status=status, error=error, started_at=started_at,
        completed_at=completed_at, trigger_callback_url=trigger_callback_url,
        raise_on_error=True,
    )


def _finalize(*, phase, criteria, max_allowed_spend, page_limit, preview_count,
              credits_consumed, governor_decision, pages, property_rows, person_rows,
              property_indexes, person_indexes, status, error, started_at, completed_at,
              trigger_callback_url, raise_on_error) -> dict:
    """Single terminal path: write the ops ledger row + POST the flat Trigger callback, then
    return the result dict. Re-raises ONLY on a genuine error (status='error'); the governor's
    clean over-budget stop returns normally (exit 0)."""
    _record_run(
        phase=phase, criteria=criteria, max_allowed_spend=max_allowed_spend,
        page_limit=page_limit, preview_count=preview_count, credits_consumed=credits_consumed,
        governor_decision=governor_decision, pages=pages, property_rows=property_rows,
        person_rows=person_rows, property_indexes=property_indexes,
        person_indexes=person_indexes, status=status, error=error,
        started_at=started_at, completed_at=completed_at,
    )
    result = {
        "feed": FEED, "status": status, "governor_decision": governor_decision,
        "property_uri": PROPERTY_URI, "person_uri": PERSON_URI,
        "max_allowed_spend": max_allowed_spend, "matches_found": preview_count,
        "credits_consumed": credits_consumed, "pages": pages,
        "property_rows": property_rows, "person_rows": person_rows,
        "property_indexes": property_indexes, "person_indexes": person_indexes,
    }
    if error:
        result["error"] = error
    _post_callback(trigger_callback_url, result)

    if raise_on_error and status == "error":
        raise RuntimeError(f"propertyradar ingest failed: {error}")
    return result


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_db() -> dict:
    """Create ops schema + ops.propertyradar_runs (idempotent)."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    return {"created": "ops.propertyradar_runs"}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15,
              memory=8192, cpu=2.0)
def verify(sample: int = 3) -> dict:
    """Read both committed datasets back from R2: row counts, schema, committed scalar indices,
    and an indexed lookup proving the parcel_key BTREE resolves. Read-only; mutates nothing."""
    import json

    import lance

    so = _r2_storage_options()
    out: dict = {}
    for label, uri, probe_col in (("property", PROPERTY_URI, "parcel_key"),
                                  ("person", PERSON_URI, "RadarID")):
        try:
            ds = lance.dataset(uri, storage_options=so)
        except Exception as exc:  # noqa: BLE001 — dataset may not exist yet
            out[label] = {"uri": uri, "error": str(exc)}
            continue
        total = ds.count_rows()
        schema = [(f.name, str(f.type)) for f in ds.schema]
        committed = []
        for ix in ds.list_indices():
            committed.append({
                "name": ix.get("name") if isinstance(ix, dict) else getattr(ix, "name", None),
                "type": str(ix.get("type") if isinstance(ix, dict) else getattr(ix, "type", None)),
                "fields": ix.get("fields") if isinstance(ix, dict) else getattr(ix, "fields", None),
            })
        head = ds.to_table(limit=sample).to_pylist()
        out[label] = {"uri": uri, "total_rows": total, "schema": schema,
                      "committed_indices": committed, "sample": head}
        print(f"=== {label} === {uri}")
        print(f"total rows: {total:,}")
        print(f"indices:    {json.dumps(committed, default=str)}")
    return out


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 30,
              memory=16384, cpu=4.0)
def reindex(trigger_callback_url: str | None = None) -> dict:
    """(Re)build the BTREE scalar indexes on both existing datasets (no re-ingest, no
    re-purchase). Idempotent (replace=True)."""
    so = _r2_storage_options()
    prop = _build_indexes(PROPERTY_URI, PROPERTY_BTREE, so)
    pers = _build_indexes(PERSON_URI, PERSON_BTREE, so)
    _post_callback(trigger_callback_url,
                   {"status": "success", "feed": FEED, "phase": "reindex",
                    "property_indexes": prop, "person_indexes": pers})
    return {"feed": FEED, "property_indexes": prop, "person_indexes": pers}


# ── Local entrypoints (modal run) ────────────────────────────────────────────────────
def _load_criteria(criteria_json: str, criteria_file: str) -> list:
    """Resolve the operator's Criteria from an inline JSON string or a file path (file wins
    if both given). Empty → [] (matches everything; the governor will hard-stop)."""
    import json

    if criteria_file:
        with open(criteria_file) as fh:
            return json.load(fh)
    if criteria_json:
        return json.loads(criteria_json)
    return []


@app.local_entrypoint()
def setup() -> None:
    """Create ops.propertyradar_runs."""
    print(init_db.remote())


@app.local_entrypoint()
def run(max_allowed_spend: int = 0, criteria_json: str = "", criteria_file: str = "",
        page_limit: int = 0) -> None:
    """Quota-governed ingest (manual; no Trigger callback). ``--max-allowed-spend`` is the
    credit ceiling (0 = preview-only, spends nothing). Supply the PropertyRadar Criteria via
    ``--criteria-json '<json>'`` or ``--criteria-file <path>``. Exits 0 on a clean governor
    stop (over-budget), non-zero only on a genuine fault."""
    import json
    import sys

    criteria = _load_criteria(criteria_json, criteria_file)
    result = ingest_propertyradar.remote(
        criteria=criteria,
        max_allowed_spend=max_allowed_spend,
        page_limit=page_limit or None,
        trigger_callback_url=None,
    )
    print(json.dumps(result, indent=2, default=str))
    # Clean exit on the governor's hard stop; non-zero only on a real error.
    if result.get("status") == "error":
        sys.exit(1)
    sys.exit(0)


@app.local_entrypoint()
def run_verify(sample: int = 3) -> None:
    """Read-back verification — row counts, schema, committed indices for both datasets."""
    import json

    print(json.dumps(verify.remote(sample=sample), indent=2, default=str))


@app.local_entrypoint()
def reindex_only() -> None:
    """Rebuild scalar indexes on both datasets only (no re-ingest / re-purchase)."""
    import json

    print(json.dumps(reindex.remote(), indent=2, default=str))
