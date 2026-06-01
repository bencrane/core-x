"""Compute worker — Epiq corporate bankruptcy harvest (dm.epiq11.com getcards API).

Part of the ``epiq-pipelines`` Modal app. Endpoint-less functions, spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints.
Clean-room data plane: no Iceberg, no Polaris — DuckDB does 100% of the transform,
Lance is written straight to R2.

This is the fleet's first API HARVEST (the other sources are bulk-file/registry
downloads). Three grains land raw JSON into R2 before any transform, then DuckDB
read_json → Arrow → three DISTINCT Lance datasets:

    cases    GET  /api/search/getcases?showActive=&havingClaims=   (master universe)
        -> s3://data-sink/active/epiq_cases/        keyed on project_code
    claims   POST /api/search/getcards {type:"Claims",  ...}        (per-case register)
        -> s3://data-sink/active/epiq_claims/       keyed on (project_code, claim_number)
    dockets  POST /api/search/getcards {type:"Dockets", ...}        (per-case register)
        -> s3://data-sink/active/epiq_dockets/      keyed on (project_code, docket_number)

API contract (probe-confirmed against the production Angular bundle + live endpoint):
  • Case universe — GET /api/search/getcases?showActive={bool}&havingClaims={bool}
    returns a JSON ARRAY of {projectCode, caseNumber, text(=debtor), value(=projectId),
    defaultPage, isWebAlias, projetAliasId}. showActive=false → full universe (~946);
    showActive=true → active subset (~386). No dbSource here.
  • Per-case grain — POST /api/search/getcards. The grain selector is the top-level
    field ``type`` ∈ {"Claims","Dockets","Cases","Documents"} (NOT "searchType" — that
    is an internal liability filter). Body:
        {"type": <grain>, "term": "", "groupBy": <"claimNumber"|"docketNumber">,
         "filters": [{"name":"projectCode","values":[code]},
                     {"name":"dbSource","values":[dbSource]}],
         "sort": <"asc"|"desc">, "documentFrom": <offset>}
    Response is a JSON-ENCODED STRING (double-decode) wrapping
        {"groups":[{"$type":...,"results":[ {...record...}, ... ]}],
         "total": N, "token": <search_after|null>, "totalHitsRelation": "eq"}.
    The server reads ``type`` case-sensitively; a wrong/absent ``type`` → HTTP 400
    "Unsupported search type".
  • dbSource — GET /api/search/getprojectdbsource?projectCode={code} → plain text
    (e.g. "DM"). Required inside the getcards ``filters``. Baked into the manifest by
    harvest_cases so the fan-out never re-resolves it.
  • Pagination — ``documentFrom`` is an offset; the server caps a single response at
    ~10,000 results (Elasticsearch max_result_window) and surfaces ``total`` (which
    itself saturates at 100,000) plus a ``token``. We page by the returned batch size,
    dedup by record ``id`` (loop guard), and on the >10k window cap STOP and LOG a
    truncation warning — never a silent cap. (token/search_after continuation is the
    documented Phase-2 path; calibrating its request field needs one >10k-claims probe.)
  • Anti-bot — the dm site is PUBLIC (no auth) and served 200s to a plain browser UA
    throughout the probe. The 403 path below is defensive (transient Akamai/WAF).

PDF EXCLUSION (mandate): the docket ``docketDocuments`` and claim ``documentUrls``
references ARE captured (document ids + download names), but this worker NEVER fetches
a PDF binary. There is no requests.get against any document URL anywhere in this file.

Fan-out topology (approved):
  Phase 1 — harvest_cases: GET getcases (universe + active) → enrich with is_active +
      dbSource (bounded ThreadPool) → land raw + a project_codes manifest → DuckDB →
      epiq_cases (overwrite) → indexes → callback returns the manifest key + count.
  Phase 2 — harvest_claims ‖ harvest_dockets (Trigger fires both in parallel; distinct
      Lance datasets → no shared-writer conflict). Each reads the manifest and fans out
      ONE Modal container PER project_code via ``fetch_grain_for_case.map(...)`` capped
      at max_containers=8 (the single global politeness ceiling spanning the whole map),
      lands every page, then DuckDB read_json over all landed pages → Lance overwrite.

    modal deploy pipelines/epiq/ingest.py
    modal run    pipelines/epiq/ingest.py::init_state                              # create ops.epiq_runs
    modal run    pipelines/epiq/ingest.py::probe --grain dockets --project-code sva # single-case dry run
    modal run    pipelines/epiq/ingest.py::ingest_cases                            # Phase 1 → epiq_cases + manifest
    modal run    pipelines/epiq/ingest.py::harvest --grain claims                  # Phase 2 (reads latest manifest)
    modal run    pipelines/epiq/ingest.py::run_all                                 # cases → claims ‖ dockets
    modal run    pipelines/epiq/ingest.py::reindex --grain claims
    modal run    pipelines/epiq/ingest.py::show_ledger
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
LANDING_PREFIX = "landing/epiq/"
SCRATCH_DIR = "/tmp/epiq"

BASE_URL = "https://dm.epiq11.com"
GETCARDS_URL = f"{BASE_URL}/api/search/getcards"
GETCASES_URL = f"{BASE_URL}/api/search/getcases"
DBSOURCE_URL = f"{BASE_URL}/api/search/getprojectdbsource"

# Lance system-of-record tier (env-overridable). Three datasets joined on project_code.
CASES_URI = os.environ.get("EPIQ_CASES_LANCE_URI", "s3://data-sink/active/epiq_cases/")
CLAIMS_URI = os.environ.get("EPIQ_CLAIMS_LANCE_URI", "s3://data-sink/active/epiq_claims/")
DOCKETS_URI = os.environ.get("EPIQ_DOCKETS_LANCE_URI", "s3://data-sink/active/epiq_dockets/")

# Per-case fan-out grains (the two paginated getcards registers). ``cases`` is handled
# separately by harvest_cases (it is the getcases universe, not a per-case getcards call).
GRAINS: dict[str, dict[str, str]] = {
    "claims": {"type": "Claims", "group_by": "claimNumber", "sort": "asc",
               "uri": CLAIMS_URI, "landing": "claims"},
    "dockets": {"type": "Dockets", "group_by": "docketNumber", "sort": "desc",
                "uri": DOCKETS_URI, "landing": "dockets"},
}

# getcards saturates a single response at ~10k results; we page by the returned batch and
# stop at this offset (logging truncation) rather than walk into the ES window error.
DOCUMENT_WINDOW_CAP = 10000
MAX_PAGES_PER_CASE = 500          # runaway-loop backstop (≥ 5M records/case before it bites)
DBSOURCE_DEFAULT = "DM"           # observed default; fallback when resolution fails
DBSOURCE_WORKERS = 8              # ThreadPool width for per-case dbSource resolution

# Rotate across a few realistic desktop UAs; the 403 path re-bootstraps with the next one.
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
]

# Lance fragment sizing (fleet-fixed: max_rows_per_file exact; max_bytes_per_file = 90 GiB,
# Lance's documented default — see ca_cslb/fec). Net-new datasets pin the current version.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan. BTREE = high-cardinality resolution / join keys; BITMAP =
# low-cardinality categoricals filtered frequently (02_lancedb_storage.md §6).
INDEX_PLAN: dict[str, dict[str, list[str]]] = {
    "cases": {
        "btree": ["project_code", "case_number", "debtor_name", "project_id"],
        "bitmap": ["is_active", "default_page"],
    },
    "claims": {
        "btree": ["project_code", "claim_number", "case_number", "creditor_name", "claim_id"],
        "bitmap": ["search_type", "schedule_g"],
    },
    "dockets": {
        "btree": ["project_code", "docket_number", "case_number", "docket_id"],
        "bitmap": ["is_adversary_proceeding", "jurisdiction_name", "is_project_active"],
    },
}

# Mirrored verbatim by pipelines/epiq/ops_epiq_runs.sql. Applied by init_state.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.epiq_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,
    run_date        date        NOT NULL,
    dataset_uri     text,
    project_codes   integer,
    cases_attempted integer,
    cases_failed    integer,
    rows_processed  bigint,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS epiq_runs_feed_idx        ON ops.epiq_runs (feed);
CREATE INDEX IF NOT EXISTS epiq_runs_status_idx      ON ops.epiq_runs (status);
CREATE INDEX IF NOT EXISTS epiq_runs_run_date_idx    ON ops.epiq_runs (run_date DESC);
CREATE INDEX IF NOT EXISTS epiq_runs_recorded_at_idx ON ops.epiq_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table/reader; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",           # R2 landing read/write
    "requests>=2.32",        # getcards API + Trigger waitpoint callback
    "zstandard>=0.22",       # raw-JSON page landing compression
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    {"LANCE_BYPASS_SPILLING": "true"}  # in-memory BTREE sort (lance-format/lance#2650)
)

app = modal.App("epiq-pipelines", image=image)


# ── R2 + audit + callback helpers (copied verbatim from the nearest sibling, per the
#    fleet's deliberately-duplicative house convention; no shared module) ──
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret."""
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


def _record_run(feed, run_date, dataset_uri, project_codes, cases_attempted, cases_failed,
                rows, status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.epiq_runs (psycopg). Best-effort: never let an audit-write
    failure crash an otherwise-good harvest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.epiq_runs
                    (feed, run_date, dataset_uri, project_codes, cases_attempted,
                     cases_failed, rows_processed, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (feed, run_date, dataset_uri, project_codes, cases_attempted,
                 cases_failed, rows, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the harvest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint URL. FLAT JSON body — no
    ``{"data": ...}`` envelope. The whole body becomes result.output."""
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


# ── HTTP harvest helpers (new — the API-fetch layer) ──
def _new_session(ua: str | None = None):
    """A requests.Session with a browser-like UA. One GET to the base site first warms
    any anti-bot cookie before the JSON calls."""
    import requests

    sess = requests.Session()
    sess.headers.update({
        "User-Agent": ua or USER_AGENTS[0],
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    try:
        sess.get(BASE_URL + "/", timeout=(15, 30))
    except Exception:  # noqa: BLE001 — cookie warm-up is best-effort
        pass
    return sess


def _rotate_session(sess) -> None:
    """403 recovery: rotate the UA, drop cookies, re-warm. Treats 403 as transient WAF
    friction before giving up on a page."""
    idx = (USER_AGENTS.index(sess.headers.get("User-Agent")) + 1) % len(USER_AGENTS) \
        if sess.headers.get("User-Agent") in USER_AGENTS else 0
    sess.headers["User-Agent"] = USER_AGENTS[idx]
    sess.cookies.clear()
    try:
        sess.get(BASE_URL + "/", timeout=(15, 30))
    except Exception:  # noqa: BLE001
        pass


def _request(sess, method: str, url: str, json_body=None, attempts: int = 6,
             timeout=(15, 120)):
    """Rate-limit-aware fetch. 429 → honor Retry-After else exponential backoff; 403 →
    re-bootstrap session + backoff (transient WAF); 5xx / connection errors → backoff.
    Other 4xx (e.g. a 400 bad request) fail loud. Raises after ``attempts`` exhausted."""
    import random
    import time

    import requests

    backoff = (2, 5, 15, 45, 120, 300)
    last: Exception | None = None
    for i in range(attempts):
        wait = backoff[min(i, len(backoff) - 1)]
        try:
            if method == "GET":
                resp = sess.get(url, timeout=timeout)
            else:
                resp = sess.post(url, json=json_body, timeout=timeout)
            sc = resp.status_code
            if sc < 300:
                return resp
            if sc == 429:
                ra = resp.headers.get("Retry-After", "")
                wait = int(ra) if ra.isdigit() else wait
                print(f"429 {url} → sleep {wait}s ({i + 1}/{attempts})")
            elif sc == 403:
                print(f"403 {url} → rotate session + backoff {wait}s ({i + 1}/{attempts})")
                _rotate_session(sess)
            elif 500 <= sc < 600:
                print(f"{sc} {url} → backoff {wait}s ({i + 1}/{attempts})")
            else:
                resp.raise_for_status()  # 400/404/etc — fail loud, not a rate-limit
                return resp
        except requests.RequestException as exc:
            last = exc
            print(f"req error {url}: {exc} ({i + 1}/{attempts})")
        time.sleep(wait + random.random())
    raise RuntimeError(f"request failed after {attempts} attempts: {method} {url} ({last})")


def _getcases(sess, show_active: bool) -> list[dict]:
    """GET the case universe. Returns the JSON array of case stubs."""
    url = f"{GETCASES_URL}?showActive={str(show_active).lower()}&havingClaims=false"
    return _request(sess, "GET", url).json()


def _resolve_dbsource(sess, code: str) -> str:
    """GET the per-project dbSource (plain text). Falls back to the observed default so a
    single flaky lookup never strands a case."""
    try:
        resp = _request(sess, "GET", f"{DBSOURCE_URL}?projectCode={code}", attempts=3)
        return resp.text.strip().strip('"') or DBSOURCE_DEFAULT
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: dbSource resolve failed for {code}: {exc}; defaulting {DBSOURCE_DEFAULT}")
        return DBSOURCE_DEFAULT


def _getcards(sess, body: dict) -> dict:
    """POST getcards and return the decoded envelope. The endpoint double-encodes (a
    JSON-string-wrapped object) — unwrap it."""
    import json

    resp = _request(sess, "POST", GETCARDS_URL, json_body=body)
    obj = json.loads(resp.text)
    return json.loads(obj) if isinstance(obj, str) else obj


def _land_bytes(s3, key: str, data: bytes, compress: bool = True) -> None:
    """Write bytes to the R2 landing zone (zstd by default). put_object via the
    R2-configured client."""
    if compress:
        import zstandard as zstd
        data = zstd.ZstdCompressor(level=10).compress(data)
    s3.put_object(Bucket=BUCKET, Key=key, Body=data)


def _read_manifest(s3, key: str) -> list[dict]:
    import json
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def _harvest_getcards(sess, s3, code: str, dbsource: str, cfg: dict, run_date: str) -> dict:
    """Paginate one case's getcards register and land every raw page. Returns
    {pages, results, total, truncated}. Lands the DECODED envelope (compact one-line
    JSON) so the DuckDB read needs no double-decode; the wire data is preserved whole."""
    import json
    import random
    import time

    landing_dir = f"{LANDING_PREFIX}{cfg['landing']}/project_code={code}/run_date={run_date}/"
    body_base = {
        "type": cfg["type"], "term": "", "groupBy": cfg["group_by"],
        "filters": [{"name": "projectCode", "values": [code]},
                    {"name": "dbSource", "values": [dbsource]}],
        "sort": cfg["sort"],
    }
    seen: set = set()
    pages = results = 0
    total: int | None = None
    truncated = False
    offset = 0

    for _ in range(MAX_PAGES_PER_CASE):
        obj = _getcards(sess, {**body_base, "documentFrom": offset})
        recs = [r for g in (obj.get("groups") or []) for r in (g.get("results") or [])]
        if total is None:
            total = int(obj.get("total") or 0)
        if not recs:
            break
        fresh = [r for r in recs if r.get("id") not in seen]
        if not fresh:
            break  # overlap / non-advancing offset → stop (loop guard)
        seen.update(r.get("id") for r in fresh)

        raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        _land_bytes(s3, f"{landing_dir}page_{pages:05d}.json.zst", raw)
        pages += 1
        results += len(fresh)
        offset += len(recs)

        if total and offset >= total:
            break
        if offset >= DOCUMENT_WINDOW_CAP:
            if total and total > DOCUMENT_WINDOW_CAP:
                truncated = True
            break
        time.sleep(0.2 + random.random() * 0.3)  # politeness jitter between pages

    if truncated:
        print(f"WARN: {code}/{cfg['type']} captured {results} of total {total} "
              f"(>{DOCUMENT_WINDOW_CAP} window cap; token/search_after continuation TODO)")
    return {"pages": pages, "results": results, "total": total, "truncated": truncated}


# ── DuckDB read_json projections (explicit columns; NO read_json_auto, NO VARIANT) ──
def _read_pages_cte(glob_param: str = "?") -> str:
    """Read landed getcards pages (one compact JSON object per line) and explode
    groups→results into a JSON column ``r``. ignore_errors so a single corrupt landed
    page never aborts the whole grain."""
    return (
        "WITH pages AS (\n"
        f"  SELECT unnest(groups) AS g\n"
        f"  FROM read_json({glob_param}, format='newline_delimited',\n"
        "                 columns={'groups': 'STRUCT(results JSON[])[]'},\n"
        "                 maximum_object_size=268435456, ignore_errors=true)\n"
        "),\n"
        "recs AS (SELECT unnest(g.results) AS r FROM pages WHERE g.results IS NOT NULL)\n"
    )


def _sql_claims() -> str:
    return (
        _read_pages_cte()
        + """SELECT
    nullif(trim(r->>'projectCode'), '')                     AS project_code,
    TRY_CAST(r->>'claimId' AS BIGINT)                       AS claim_id,
    nullif(trim(r->>'claimNumber'), '')                     AS claim_number,
    nullif(trim(r->>'claimNumberSorting'), '')              AS claim_number_sorting,
    nullif(trim(r->>'caseNumber'), '')                      AS case_number,
    nullif(trim(r->>'caseName'), '')                        AS case_name,
    nullif(trim(r->>'creditorName'), '')                    AS creditor_name,
    nullif(trim(r->>'debtorName'), '')                      AS debtor_name,
    nullif(trim(r->>'debtorId'), '')                        AS debtor_id,
    nullif(trim(r->>'scheduleNumber'), '')                  AS schedule_number,
    nullif(trim(r->>'scheduleNumberDisplay'), '')           AS schedule_number_display,
    TRY_CAST(r->>'scheduleId' AS BIGINT)                    AS schedule_id,
    nullif(trim(r->>'liabilityId'), '')                     AS liability_id,
    nullif(trim(r->>'searchType'), '')                      AS search_type,
    TRY_CAST(r->>'scheduleG' AS BOOLEAN)                    AS schedule_g,
    nullif(trim(r->>'filedDateDisplay'), '')                AS filed_date_display,
    TRY_CAST(TRY_STRPTIME(nullif(trim(r->>'filedDateDisplay'), ''), '%b %d %Y') AS DATE) AS filed_date,
    nullif(trim(r->>'valueDisplay'), '')                    AS value_display,
    TRY_CAST(nullif(regexp_replace(r->>'valueDisplay', '[$,]', '', 'g'), '') AS DECIMAL(18,2)) AS claim_amount,
    TRY_CAST(r->>'isAccessible' AS BOOLEAN)                 AS is_accessible,
    nullif(trim(r->>'imageDocumentId'), '')                 AS image_document_id,
    nullif(trim(r->>'redactedDocumentId'), '')              AS redacted_document_id,
    nullif(trim(r->>'projectId'), '')                       AS project_id,
    -- nested structures captured losslessly as JSON text (explicit VARCHAR, never VARIANT).
    -- amount_list holds the full per-class amount breakdown; document refs captured, NOT fetched.
    nullif(r->>'amountList', '')                            AS amount_list,
    nullif(r->>'creditorAddressList', '')                   AS creditor_address_list,
    nullif(r->>'docketNumbers', '')                         AS docket_numbers,
    nullif(r->>'dockets', '')                               AS dockets,
    nullif(r->>'documentUrls', '')                          AS document_urls,
    CAST(? AS DATE)                                         AS run_date,
    ?                                                       AS source_feed,
    now()                                                   AS ingested_at
FROM recs"""
    )


def _sql_dockets() -> str:
    # docketDocuments shape is probe-confirmed → captured as an explicit typed LIST<STRUCT>
    # (source-cased keys to guarantee the JSON→struct cast binds). Document refs only — no
    # binary is ever fetched.
    doc_struct = (
        'CAST(r->\'docketDocuments\' AS STRUCT('
        '"documentId" BIGINT, "documentDesc" VARCHAR, "documentDownloadName" VARCHAR, '
        '"docketId" BIGINT, "projectId" BIGINT, "documentVersionId" BIGINT, '
        '"documentContentId" BIGINT, "sequenceNumber" BIGINT)[])'
    )
    return (
        _read_pages_cte()
        + f"""SELECT
    nullif(trim(r->>'projectCode'), '')                     AS project_code,
    TRY_CAST(r->>'docketId' AS BIGINT)                      AS docket_id,
    nullif(trim(r->>'docketNumber'), '')                    AS docket_number,
    nullif(trim(r->>'docketNumberSorting'), '')             AS docket_number_sorting,
    nullif(trim(r->>'docketName'), '')                      AS docket_name,
    nullif(trim(r->>'docketText'), '')                      AS docket_text,
    nullif(trim(r->>'caseName'), '')                        AS case_name,
    nullif(trim(r->>'caseNumber'), '')                      AS case_number,
    nullif(trim(r->>'debtorName'), '')                      AS debtor_name,
    nullif(trim(r->>'debtorId'), '')                        AS debtor_id,
    nullif(trim(r->>'debtorNumber'), '')                    AS debtor_number,
    nullif(trim(r->>'jurisdictionName'), '')                AS jurisdiction_name,
    nullif(trim(r->>'docketFiledDateDisplay'), '')          AS docket_filed_date_display,
    TRY_CAST(r->>'docketFiledDate' AS TIMESTAMP)            AS docket_filed_at,
    TRY_CAST(r->>'isAdversaryProceeding' AS BOOLEAN)        AS is_adversary_proceeding,
    TRY_CAST(r->>'isAccessible' AS BOOLEAN)                 AS is_accessible,
    TRY_CAST(r->>'isProjectActive' AS BOOLEAN)              AS is_project_active,
    nullif(trim(r->>'projectId'), '')                       AS project_id,
    nullif(r->>'relatedDocketsNumbers', '')                 AS related_dockets_numbers,
    {doc_struct}                                            AS docket_documents,
    CAST(? AS DATE)                                         AS run_date,
    ?                                                       AS source_feed,
    now()                                                   AS ingested_at
FROM recs"""
    )


def _sql_cases() -> str:
    # The enriched manifest (a JSON array) is both the fan-out seed AND the epiq_cases
    # source: getcases stubs + is_active + dbSource baked in by harvest_cases.
    return """SELECT
    nullif(trim(projectCode), '')                           AS project_code,
    nullif(trim(caseNumber), '')                            AS case_number,
    nullif(trim(text), '')                                  AS debtor_name,
    TRY_CAST(value AS BIGINT)                               AS project_id,
    nullif(trim(defaultPage), '')                           AS default_page,
    isWebAlias                                              AS is_web_alias,
    TRY_CAST(projetAliasId AS BIGINT)                       AS project_alias_id,
    isActive                                                AS is_active,
    nullif(trim(dbSource), '')                              AS db_source,
    CAST(? AS DATE)                                         AS run_date,
    now()                                                   AS ingested_at
FROM read_json(?, format='array',
    columns={'projectCode':'VARCHAR','caseNumber':'VARCHAR','text':'VARCHAR','value':'BIGINT',
             'defaultPage':'VARCHAR','isWebAlias':'BOOLEAN','projetAliasId':'BIGINT',
             'isActive':'BOOLEAN','dbSource':'VARCHAR'},
    maximum_object_size=268435456, ignore_errors=true)"""


_SQL_BUILDERS = {"claims": _sql_claims, "dockets": _sql_dockets, "cases": _sql_cases}


def _build_indexes(grain: str, uri: str, so: dict) -> list[str]:
    """Build BTREE + BITMAP scalar indexes for one dataset. replace=True → idempotent.
    An index miss is logged, never fatal (the data write is the critical artifact)."""
    import lance

    ds = lance.dataset(uri, storage_options=so)
    built: list[str] = []
    for col in INDEX_PLAN[grain]["btree"]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in INDEX_PLAN[grain]["bitmap"]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    return built


def _download_grain_pages(s3, sub: str, run_date: str, dest: str) -> int:
    """Pull every landed page for this grain+run_date to /tmp, zstd-decompressed (house
    rule: Python does I/O, DuckDB reads /tmp). Each output file is one compact JSON line."""
    import os.path

    import zstandard as zstd

    os.makedirs(dest, exist_ok=True)
    dctx = zstd.ZstdDecompressor()
    needle = f"/run_date={run_date}/"
    n = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{LANDING_PREFIX}{sub}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if needle not in key or not key.endswith(".json.zst"):
                continue
            raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            out = os.path.join(dest, key.replace("/", "_")[:-4])  # strip .zst → .json
            with open(out, "wb") as fh:
                fh.write(dctx.decompress(raw))
            n += 1
    return n


def _transform_grain_to_lance(grain: str, run_date: str, so: dict, s3) -> int:
    """Download landed pages → DuckDB read_json project/cast → streaming Arrow → Lance
    overwrite DIRECT to R2 → return the committed row count. No-op (keeps the prior
    dataset) when nothing landed this run_date."""
    import duckdb
    import lance

    cfg = GRAINS[grain]
    dest = f"{SCRATCH_DIR}/_read/{grain}/{run_date}"
    n_files = _download_grain_pages(s3, cfg["landing"], run_date, dest)
    print(f"{grain}: {n_files} landed page file(s) for run_date={run_date}")
    if n_files == 0:
        print(f"{grain}: nothing landed → skipping Lance write (prior dataset retained)")
        return 0

    con = duckdb.connect(":memory:")
    try:
        con.execute("PRAGMA threads=8;")
        con.execute("SET enable_progress_bar=false;")
        con.execute("SET memory_limit='12GB';")
        con.execute("SET temp_directory='/tmp/duckdb_spill';")
        reader = con.sql(
            _SQL_BUILDERS[grain](), params=[f"{dest}/*.json", run_date, grain]
        ).to_arrow_reader(MAX_ROWS_PER_FILE)
        lance.write_dataset(
            reader, cfg["uri"], mode="overwrite", schema=reader.schema,
            data_storage_version=DATA_STORAGE_VERSION, max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE, storage_options=so,
        )
    finally:
        con.close()
    rows = lance.dataset(cfg["uri"], storage_options=so).count_rows()
    print(f"{grain}: wrote {rows:,} rows -> {cfg['uri']}")
    return rows


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_state_schema() -> dict:
    """Apply the idempotent ops.epiq_runs DDL. Run once before the first harvest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.epiq_runs schema.")
    return {"status": "success", "table": "ops.epiq_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=8192,
    cpu=4.0,
    retries=2,  # the dm.epiq11.com API is the fragile hop; landing writes are idempotent overwrites
)
def harvest_cases(run_date: str = "", trigger_callback_url: str | None = None) -> dict:
    """Phase 1 — GET the case universe (+ active subset), enrich each case with is_active
    and its dbSource, land raw + a project_codes manifest, then DuckDB → epiq_cases
    (overwrite) → indexes. The manifest (R2 key returned in the callback) is the fan-out
    seed for claims/dockets. Re-raises on failure so the Modal call is marked failed."""
    import concurrent.futures as cf
    import datetime as dt
    import json

    import lance

    run_date = run_date or dt.date.today().isoformat()
    started_at = dt.datetime.now(dt.timezone.utc)
    rows = 0
    n_codes = 0
    manifest_key = f"{LANDING_PREFIX}cases/run_date={run_date}/project_codes.json"
    status, error = "error", None
    built: list[str] = []

    try:
        so = _r2_storage_options()
        s3 = _s3_client()
        sess = _new_session()

        universe = _getcases(sess, show_active=False)
        active = _getcases(sess, show_active=True)
        active_codes = {c.get("projectCode") for c in active}
        n_codes = len(universe)
        print(f"cases: universe={n_codes} active={len(active_codes)}")

        # Land the RAW getcases responses for durability (schemas drift; scrapes fail).
        _land_bytes(s3, f"{LANDING_PREFIX}cases/run_date={run_date}/getcases_all.json.zst",
                    json.dumps(universe, separators=(",", ":")).encode("utf-8"))
        _land_bytes(s3, f"{LANDING_PREFIX}cases/run_date={run_date}/getcases_active.json.zst",
                    json.dumps(active, separators=(",", ":")).encode("utf-8"))

        # Resolve dbSource per case (bounded ThreadPool) and bake is_active + dbSource into
        # the manifest so the per-case fan-out never re-resolves them.
        def _enrich(case: dict) -> dict:
            code = case.get("projectCode")
            return {**case,
                    "isActive": code in active_codes,
                    "dbSource": _resolve_dbsource(sess, code)}

        with cf.ThreadPoolExecutor(max_workers=DBSOURCE_WORKERS) as pool:
            enriched = list(pool.map(_enrich, universe))

        _land_bytes(s3, manifest_key,
                    json.dumps(enriched, separators=(",", ":")).encode("utf-8"),
                    compress=False)  # plain JSON — orchestrators read it directly

        # Transform the enriched manifest → epiq_cases (download to /tmp, DuckDB reads it).
        import duckdb
        os.makedirs(SCRATCH_DIR, exist_ok=True)
        cases_path = os.path.join(SCRATCH_DIR, f"cases_{run_date}.json")
        with open(cases_path, "wb") as fh:
            fh.write(json.dumps(enriched, separators=(",", ":")).encode("utf-8"))

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            table = con.execute(_sql_cases(), [run_date, cases_path]).to_arrow_table()
            rows = table.num_rows
        finally:
            con.close()
        lance.write_dataset(
            table, CASES_URI, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION, max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE, storage_options=so,
        )
        del table
        print(f"cases: wrote {rows:,} rows -> {CASES_URI}")
        built = _build_indexes("cases", CASES_URI, so)
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("cases", run_date, CASES_URI, int(n_codes), None, None,
                    int(rows), status, error, started_at, completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "feed": "cases", "run_date": run_date,
                        "rows": int(rows), "project_codes": int(n_codes),
                        "manifest_key": manifest_key, "dataset_uri": CASES_URI})

    if status != "success":
        raise RuntimeError(f"epiq harvest_cases failed: {error}")
    return {"status": status, "feed": "cases", "run_date": run_date, "rows": int(rows),
            "project_codes": int(n_codes), "manifest_key": manifest_key,
            "indices": built, "dataset_uri": CASES_URI}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 30,
    memory=4096,
    cpu=2.0,
    retries=2,          # whole-container safety net; per-page HTTP backoff is in _request
    max_containers=8,   # the SINGLE global politeness ceiling spanning the whole .map()
)
def fetch_grain_for_case(entry: dict, grain: str, run_date: str,
                         trigger_callback_url: str | None = None) -> dict:
    """The dynamic fan-out unit — paginate ONE case's claims|dockets register and land
    every raw page to R2. No Lance write, no callback. Returns a per-case result dict;
    a logical fetch failure is captured (not raised) so .map() collects every case and
    the orchestrator counts failures — one bad case never sinks the grain."""
    code = entry.get("projectCode") if isinstance(entry, dict) else str(entry)
    dbsource = entry.get("dbSource") if isinstance(entry, dict) else None
    cfg = GRAINS[grain]
    try:
        s3 = _s3_client()
        sess = _new_session()
        if not dbsource:
            dbsource = _resolve_dbsource(sess, code)
        out = _harvest_getcards(sess, s3, code, dbsource, cfg, run_date)
        return {"projectCode": code, "status": "success", **out}
    except Exception as exc:  # noqa: BLE001 — partial durability: report, don't abort the map
        print(f"WARN: {grain} fetch failed for {code}: {exc}")
        return {"projectCode": code, "status": "error", "error": str(exc),
                "pages": 0, "results": 0}


def _harvest_grain(grain: str, manifest_key: str, run_date: str,
                   trigger_callback_url: str | None) -> dict:
    """Phase 2 body — read the manifest, fan out one container per project_code via
    .map(), then DuckDB read_json over all landed pages → grain Lance dataset (overwrite)
    → indexes → ops.* + callback. Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt

    cfg = GRAINS[grain]
    started_at = dt.datetime.now(dt.timezone.utc)
    rows = attempted = failed = fetched = 0
    status, error = "error", None
    built: list[str] = []

    try:
        so = _r2_storage_options()
        s3 = _s3_client()
        entries = _read_manifest(s3, manifest_key)
        attempted = len(entries)
        print(f"{grain}: fanning out {attempted} project_code(s) (max_containers=8)")

        # return_exceptions: a container that dies after retries surfaces as an exception
        # object in the list rather than aborting the whole grain — count it as a failure.
        map_results = list(fetch_grain_for_case.map(
            entries, kwargs={"grain": grain, "run_date": run_date}, return_exceptions=True))
        failed = sum(1 for r in map_results
                     if not isinstance(r, dict) or r.get("status") != "success")
        fetched = sum(r.get("results", 0) for r in map_results if isinstance(r, dict))
        print(f"{grain}: fan-out complete — {attempted - failed} ok, {failed} failed, "
              f"~{fetched:,} records landed")

        rows = _transform_grain_to_lance(grain, run_date, so, s3)
        built = _build_indexes(grain, cfg["uri"], so)
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(grain, run_date, cfg["uri"], attempted, attempted, failed,
                    int(rows), status, error, started_at, completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "feed": grain, "run_date": run_date,
                        "rows": int(rows), "cases_attempted": attempted,
                        "cases_failed": failed, "dataset_uri": cfg["uri"]})

    if status != "success":
        raise RuntimeError(f"epiq harvest_{grain} failed: {error}")
    return {"status": status, "feed": grain, "run_date": run_date, "rows": int(rows),
            "cases_attempted": attempted, "cases_failed": failed,
            "indices": built, "dataset_uri": cfg["uri"]}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120,
    memory=16384,
    cpu=8.0,
    ephemeral_disk=524288,  # hold the run_date's landed claims JSON for the DuckDB read
)
def harvest_claims(manifest_key: str, run_date: str = "",
                   trigger_callback_url: str | None = None) -> dict:
    """Phase 2 (claims) — fan out per project_code → epiq_claims."""
    import datetime as dt
    return _harvest_grain("claims", manifest_key, run_date or dt.date.today().isoformat(),
                          trigger_callback_url)


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120,
    memory=16384,
    cpu=8.0,
    ephemeral_disk=524288,  # hold the run_date's landed dockets JSON for the DuckDB read
)
def harvest_dockets(manifest_key: str, run_date: str = "",
                    trigger_callback_url: str | None = None) -> dict:
    """Phase 2 (dockets) — fan out per project_code → epiq_dockets."""
    import datetime as dt
    return _harvest_grain("dockets", manifest_key, run_date or dt.date.today().isoformat(),
                          trigger_callback_url)


@app.function(secrets=[modal.Secret.from_name("r2-credentials")],
              timeout=60 * 30, memory=16384, cpu=4.0)
def reindex(grain: str) -> dict:
    """(Re)build the scalar indexes on an already-written dataset (no re-harvest)."""
    grain = grain.strip().lower()
    uri = CASES_URI if grain == "cases" else GRAINS[grain]["uri"]
    built = _build_indexes(grain, uri, _r2_storage_options())
    return {"grain": grain, "dataset_uri": uri, "indexes": built, "index_count": len(built)}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 20) -> list:
    """Read the most recent ops.epiq_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, feed, run_date, rows_processed, project_codes, cases_attempted, "
            "cases_failed, status, error, started_at, completed_at "
            "FROM ops.epiq_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ──────────────────────────────────────────────────────────────────────────────
# Manual ops entrypoints (local — no callback). ops.* write still fires.
# ──────────────────────────────────────────────────────────────────────────────
@app.local_entrypoint()
def init_state() -> None:
    """Create ops.epiq_runs (idempotent)."""
    import json

    print(json.dumps(apply_state_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def probe(grain: str = "dockets", project_code: str = "sva", run_date: str = "") -> None:
    """Single-case dry run — paginate + land one case's register (no Lance write). Verifies
    the API contract, landing keys, and pagination terminate."""
    import datetime as dt
    import json

    rd = run_date or dt.date.today().isoformat()
    print(json.dumps(
        fetch_grain_for_case.remote({"projectCode": project_code}, grain, rd,
                                    trigger_callback_url=None),
        indent=2, default=str))


@app.local_entrypoint()
def ingest_cases(run_date: str = "") -> None:
    """Phase 1 — harvest the case universe → epiq_cases + project_codes manifest."""
    import json

    print(json.dumps(harvest_cases.remote(run_date, trigger_callback_url=None),
                     indent=2, default=str))


@app.local_entrypoint()
def harvest(grain: str, manifest_key: str = "", run_date: str = "") -> None:
    """Phase 2 — harvest a grain (claims|dockets). Defaults the manifest to today's run."""
    import datetime as dt
    import json

    rd = run_date or dt.date.today().isoformat()
    key = manifest_key or f"{LANDING_PREFIX}cases/run_date={rd}/project_codes.json"
    fn = {"claims": harvest_claims, "dockets": harvest_dockets}[grain.strip().lower()]
    print(json.dumps(fn.remote(key, rd, trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def run_all(run_date: str = "") -> None:
    """End-to-end manual run: Phase 1 cases, then claims ‖ dockets in PARALLEL (distinct
    datasets → no shared-writer conflict)."""
    import datetime as dt
    import json

    rd = run_date or dt.date.today().isoformat()
    print("=== Phase 1: cases ===")
    cases = harvest_cases.remote(rd, trigger_callback_url=None)
    print(json.dumps(cases, default=str))
    key = cases["manifest_key"]

    print("\n=== Phase 2: claims ‖ dockets ===")
    calls = {"claims": harvest_claims.spawn(key, rd, trigger_callback_url=None),
             "dockets": harvest_dockets.spawn(key, rd, trigger_callback_url=None)}
    for grain, call in calls.items():
        print(json.dumps(call.get(), default=str))


@app.local_entrypoint()
def reindex_all(grain: str = "") -> None:
    """Rebuild scalar indexes on existing dataset(s) (no re-harvest). Default: all three."""
    import json

    for g in ([grain] if grain else ["cases", "claims", "dockets"]):
        print(json.dumps(reindex.remote(g), default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 20) -> None:
    """Print the most recent ops.epiq_runs rows."""
    import json

    print(json.dumps(ledger.remote(limit), indent=2, default=str))
