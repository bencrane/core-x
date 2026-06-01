"""Compute worker — SEC EDGAR structured-data cluster.

Part of the ``edgar-pipelines`` Modal app. Endpoint-less functions, spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints.
Clean-room data plane: no Iceberg, no Polaris — Python does HTTP I/O + unzip only,
DuckDB does 100% of the transform/cast, Lance is written straight to R2.

THREE datasets, ONE domain (probed against real SEC payloads — see the verified field
shapes inline). Every CIK is held VARCHAR; the forms already ship CIKs zero-padded to
10 digits, and the Identity Spine carries a matching ``cik10`` so the join is exact.

  1. CIK Identity Spine  — https://www.sec.gov/files/company_tickers.json
        (+ company_tickers_exchange.json for the exchange column when present)
        company_tickers.json is an object-of-objects {"0":{cik_str,ticker,title},...};
        DuckDB json_each() unpivots it. One row per (cik, ticker) — a CIK can carry many
        tickers (e.g. an issuer's ETF shelf). cik_str is the integer-form CIK (no leading
        zeros); cik10 = lpad(cik_str,10,'0') is the universal join key into every form.
        -> s3://data-sink/active/edgar_cik_map/        BTREE cik_str, cik10, ticker

  2. Form 4 — Insider Transactions (Forms 3/4/5 bundle; SEC ships them together as the
        "insider transactions data sets", file {YYYY}q{Q}_form345.zip — NOT _it.zip).
        SUBMISSION.tsv is the 1-row-per-filing spine (PK ACCESSION_NUMBER, carries
        ISSUERCIK + ISSUERTRADINGSYMBOL); REPORTINGOWNER / NONDERIV_* / DERIV_* / FOOTNOTES
        / OWNER_SIGNATURE are 1:N children nested as LIST<STRUCT> by accession.
        -> s3://data-sink/active/edgar_form_4/         BTREE accession_number, issuer_cik,
                                                              issuer_trading_symbol

  3. Form D — Private Placements (file {YYYY}q{Q}_d.zip).
        FORMDSUBMISSION.tsv is the spine; OFFERING.tsv is exactly 1:1 with it (verified:
        14637/14637 distinct accessions in 2025q4); ISSUERS / RECIPIENTS / RELATEDPERSONS
        / SIGNATURES are 1:N children. primary_issuer_cik is promoted from the ISSUERS row
        flagged IS_PRIMARYISSUER_FLAG='YES'.
        -> s3://data-sink/active/edgar_form_d/          BTREE accession_number,
                                                               primary_issuer_cik

Network compliance (SEC fair-access policy, STRICT): every request to sec.gov carries a
declared ``User-Agent`` (SEC_USER_AGENT) — NOT a generic browser string. A request without
it returns HTTP 403 (verified). Override the contact via the SEC_USER_AGENT env var.

Data plane per dataset:
  fetch SEC payload -> /tmp (Python: I/O only) -> unzip (forms) -> DuckDB read_csv
  (all_varchar, tab-delimited, normalize_names) project/cast/nest 100% in SQL ->
  to_arrow_reader -> lance.write_dataset(v2.1, DIRECT to R2) -> BTREE/BITMAP scalar
  indexes. Local /tmp files are rm'd per quarter (backfill disk-exhaustion guard).

Control plane (Trigger v4 durable callback): each worker accepts ``trigger_callback_url``
and, on terminal state (success OR failure), (1) writes a run row to ``ops.edgar_runs`` via
psycopg and (2) POSTs a FLAT JSON body to that url. No ``{"data": ...}`` envelope.

    modal deploy pipelines/edgar/ingest.py
    modal run    pipelines/edgar/ingest.py::migrate                          # create ops.edgar_runs
    modal run    pipelines/edgar/ingest.py::cik_map                          # ingest the Identity Spine
    modal run    pipelines/edgar/ingest.py::backfill --dataset form_4 --quarters 4
    modal run    pipelines/edgar/ingest.py::backfill --dataset form_d --quarters 4
    modal run    pipelines/edgar/ingest.py::ingest_quarter --dataset form_4 --quarter 2026q1
    modal run    pipelines/edgar/ingest.py::reindex_one --dataset form_d
    modal run    pipelines/edgar/ingest.py::show_ledger
"""

from __future__ import annotations

import os

import modal

SCRATCH_DIR = "/tmp/edgar"

# ── Network compliance. SEC's fair-access policy REQUIRES a declared, descriptive
#    User-Agent with a contact address (a missing/empty UA returns 403 — verified). This
#    is NOT browser spoofing. Contact is env-overridable but defaults to the operator's. ──
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "Core-X Data Engine (compliance@substrate.build)")

# ── Sources. ──
CIK_TICKERS_URL = os.environ.get("EDGAR_CIK_TICKERS_URL", "https://www.sec.gov/files/company_tickers.json")
CIK_EXCHANGE_URL = os.environ.get("EDGAR_CIK_EXCHANGE_URL", "https://www.sec.gov/files/company_tickers_exchange.json")
FORM_D_URL_TMPL = "https://www.sec.gov/files/structureddata/data/form-d-data-sets/{y}q{q}_d.zip"
FORM_4_URL_TMPL = "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{y}q{q}_form345.zip"

# ── Lance system-of-record tier (env-overridable). ──
CIK_MAP_URI = os.environ.get("EDGAR_CIK_MAP_LANCE_URI", "s3://data-sink/active/edgar_cik_map/")
FORM_4_URI = os.environ.get("EDGAR_FORM_4_LANCE_URI", "s3://data-sink/active/edgar_form_4/")
FORM_D_URI = os.environ.get("EDGAR_FORM_D_LANCE_URI", "s3://data-sink/active/edgar_form_d/")

# Lance fragment sizing — Lance defaults (02_lancedb_storage.md §2.2). 90 GiB = 90 * 1024**3.
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new datasets → pin the current Lance default (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"

# DuckDB read options for the SEC DERA TSVs (tab-delimited, header row, NOT quoted — a
# literal `"` in free text must NOT be treated as a quote char, hence quote=''/escape='').
# all_varchar = zero type-inference surprises; normalize_names lowercases the UPPERCASE
# headers; ignore_errors quarantines the rare malformed free-text row (FOOTNOTES) instead
# of aborting; null_padding tolerates ragged short rows. Verified: 0 footnote rows lost.
TSV_OPTS = (
    "delim='\t', header=true, all_varchar=true, normalize_names=true, "
    "quote='', escape='', ignore_errors=true, null_padding=true, sample_size=-1"
)

# Scalar index plan. BTREE = high-cardinality resolution / join keys (the directive's
# cik_str + ticker, plus the cik10 padded join key and every accession PK); BITMAP =
# low-cardinality categoricals. An index miss is logged, never fatal.
INDEX_PLAN: dict[str, dict[str, list[str]]] = {
    "cik_map": {
        "btree": ["cik_str", "cik10", "ticker"],
        "bitmap": ["exchange"],
    },
    "form_4": {
        "btree": ["accession_number", "issuer_cik", "issuer_trading_symbol"],
        "bitmap": ["document_type", "fiscal_quarter"],
    },
    "form_d": {
        "btree": ["accession_number", "primary_issuer_cik"],
        "bitmap": ["submission_type", "industry_group_type", "fiscal_quarter"],
    },
}

DATASETS: dict[str, dict] = {
    "cik_map": {"feed": "edgar_cik_map", "uri": CIK_MAP_URI},
    "form_4": {"feed": "edgar_form_4", "uri": FORM_4_URI, "url_tmpl": FORM_4_URL_TMPL},
    "form_d": {"feed": "edgar_form_d", "uri": FORM_D_URI, "url_tmpl": FORM_D_URL_TMPL},
}

# Idempotent ops.* DDL — mirror of pipelines/edgar/ops_edgar_runs.sql (source of truth).
_OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.edgar_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset          text        NOT NULL,
    feed             text        NOT NULL,
    run_mode         text        NOT NULL,
    write_mode       text,
    dataset_uri      text,
    quarter          text,
    source_files     jsonb,
    quarters_processed integer,
    rows_processed   bigint,
    rows_written     bigint,
    status           text        NOT NULL,
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS edgar_runs_dataset_idx     ON ops.edgar_runs (dataset);
CREATE INDEX IF NOT EXISTS edgar_runs_feed_idx        ON ops.edgar_runs (feed);
CREATE INDEX IF NOT EXISTS edgar_runs_status_idx      ON ops.edgar_runs (status);
CREATE INDEX IF NOT EXISTS edgar_runs_quarter_idx     ON ops.edgar_runs (quarter DESC);
CREATE INDEX IF NOT EXISTS edgar_runs_recorded_at_idx ON ops.edgar_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_reader/table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",           # R2 index/count side-reads
    "requests>=2.32",        # SEC download + Trigger callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    {"LANCE_BYPASS_SPILLING": "true"}  # in-memory BTREE sort (lance-format/lance#2650); cheap at this scale
)

app = modal.App("edgar-pipelines", image=image)


# ──────────────────────────────────────────────────────────────────────────────
# R2 / object-store
# ──────────────────────────────────────────────────────────────────────────────
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


# ──────────────────────────────────────────────────────────────────────────────
# SEC fetch (Python = I/O only) — every request carries the compliant User-Agent
# ──────────────────────────────────────────────────────────────────────────────
def _sec_headers() -> dict[str, str]:
    return {"User-Agent": SEC_USER_AGENT, "Accept-Encoding": "gzip, deflate"}


def _download(url: str, dest: str, attempts: int = 4) -> int:
    """Stream a SEC payload to local scratch with the compliant UA. Returns bytes written.
    A few retries with backoff for transient 5xx / connection resets — SEC throttles, so
    back off rather than hammer."""
    import time

    import requests

    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            with requests.get(url, headers=_sec_headers(), stream=True, timeout=(30, 600)) as resp:
                if resp.status_code == 403:
                    raise RuntimeError(f"SEC 403 for {url} — User-Agent rejected (set SEC_USER_AGENT).")
                resp.raise_for_status()
                n = 0
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1 << 20):
                        if chunk:
                            fh.write(chunk)
                            n += len(chunk)
            return n
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            print(f"download attempt {i + 1}/{attempts} failed for {url}: {exc}")
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"SEC download failed after {attempts} attempts: {url} ({last_exc})")


def _url_available(url: str) -> bool:
    """True if the SEC resource resolves (HTTP 200) under the compliant UA."""
    import requests

    try:
        resp = requests.head(url, headers=_sec_headers(), timeout=30, allow_redirects=True)
        return resp.status_code == 200
    except Exception as exc:  # noqa: BLE001
        print(f"availability probe failed for {url}: {exc}")
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Quarter resolution
# ──────────────────────────────────────────────────────────────────────────────
def _quarter_label(y: int, q: int) -> str:
    return f"{y}q{q}"


def _parse_quarter(label: str) -> tuple[int, int]:
    """'2025q4' / '2025Q4' -> (2025, 4)."""
    s = label.strip().lower()
    if "q" not in s:
        raise ValueError(f"quarter must look like 'YYYYqQ', got {label!r}")
    y, q = s.split("q", 1)
    yi, qi = int(y), int(q)
    if qi not in (1, 2, 3, 4):
        raise ValueError(f"quarter index must be 1-4, got {label!r}")
    return yi, qi


def _form_url(dataset: str, y: int, q: int) -> str:
    return DATASETS[dataset]["url_tmpl"].format(y=y, q=q)


def _resolve_recent_quarters(dataset: str, n: int) -> list[tuple[int, int]]:
    """Walk backwards from the current quarter, probing SEC availability, and return the
    most-recent ``n`` quarters that actually have a published file (newest first)."""
    import datetime as dt

    today = dt.date.today()
    y, q = today.year, (today.month - 1) // 3 + 1
    found: list[tuple[int, int]] = []
    steps = 0
    while len(found) < n and steps < n + 12:
        if _url_available(_form_url(dataset, y, q)):
            found.append((y, q))
        q -= 1
        if q == 0:
            q = 4
            y -= 1
        steps += 1
    return found


# ──────────────────────────────────────────────────────────────────────────────
# DuckDB SQL builders (100% transform). Paths are repo-controlled /tmp paths; single
# quotes are escaped defensively. The TSV reads use TSV_OPTS; nested 1:N children use the
# row-struct idiom list(<cte_alias>) — a faithful LIST<STRUCT> of every child column.
# ──────────────────────────────────────────────────────────────────────────────
def _q(path: str) -> str:
    return path.replace("'", "''")


def _rd(path: str) -> str:
    """A read_csv() over one SEC TSV under the shared options."""
    return f"read_csv('{_q(path)}', {TSV_OPTS})"


def _cik_map_sql(tickers_path: str, exchange_path: str | None, as_of: str) -> str:
    """company_tickers.json (object-of-objects) -> one row per (cik, ticker). Optionally
    LEFT JOIN company_tickers_exchange.json (columnar {fields,data:[[cik,name,ticker,exch]]})
    to attach the exchange. cik_str = integer-form CIK as VARCHAR; cik10 = zero-padded
    10-digit join key into the forms."""
    ex_cte = ""
    ex_select = "NULL AS exchange,"
    ex_join = ""
    if exchange_path:
        ex_cte = f""",
ex AS (
    SELECT
        nullif(d.value->>0, '')                AS cik_raw,
        upper(nullif(trim(d.value->>2), ''))   AS ticker,
        nullif(trim(d.value->>3), '')          AS exchange
    FROM (SELECT content FROM read_text('{_q(exchange_path)}')) ex_src,
         json_each(ex_src.content::JSON->'data') AS d
)"""
        ex_select = "ex.exchange AS exchange,"
        ex_join = "LEFT JOIN ex ON ex.cik_raw = t.cik_raw AND ex.ticker = t.ticker"
    return f"""
WITH src AS (SELECT content FROM read_text('{_q(tickers_path)}')),
t AS (
    SELECT
        nullif(v.value->>'cik_str', '')            AS cik_raw,
        upper(nullif(trim(v.value->>'ticker'), '')) AS ticker,
        nullif(trim(v.value->>'title'), '')        AS title
    FROM src, json_each(src.content::JSON) AS v
){ex_cte}
SELECT
    t.cik_raw                          AS cik_str,
    lpad(t.cik_raw, 10, '0')           AS cik10,
    t.ticker                           AS ticker,
    t.title                            AS title,
    {ex_select}
    CAST('{as_of}' AS DATE)            AS snapshot_date,
    now()                              AS ingested_at
FROM t
{ex_join}
WHERE t.cik_raw IS NOT NULL
"""


def _D(col: str) -> str:
    """SEC DERA date cell (DD-MON-YYYY, e.g. 31-DEC-2025) -> DATE (NULL on bad/empty)."""
    return f"TRY_CAST(TRY_STRPTIME(nullif(trim({col}), ''), '%d-%b-%Y') AS DATE)"


def _N(col: str) -> str:
    return f"TRY_CAST(nullif(trim({col}), '') AS DOUBLE)"


def _I(col: str) -> str:
    return f"TRY_CAST(nullif(trim({col}), '') AS BIGINT)"


def _S(col: str) -> str:
    return f"nullif(trim({col}), '')"


def _form_d_sql(files: dict[str, str], quarter: str) -> str:
    """Form D — FORMDSUBMISSION spine ⨝ OFFERING (1:1) + nested ISSUERS/RECIPIENTS/
    RELATEDPERSONS/SIGNATURES; primary_issuer_cik promoted from the primary ISSUERS row."""
    return f"""
WITH
sub AS (SELECT * FROM {_rd(files['FORMDSUBMISSION.tsv'])}),
off AS (SELECT * FROM {_rd(files['OFFERING.tsv'])}),
iss AS (SELECT * FROM {_rd(files['ISSUERS.tsv'])}),
rec AS (SELECT * FROM {_rd(files['RECIPIENTS.tsv'])}),
rel AS (SELECT * FROM {_rd(files['RELATEDPERSONS.tsv'])}),
sig AS (SELECT * FROM {_rd(files['SIGNATURES.tsv'])}),
iss_agg AS (
    SELECT accessionnumber,
        max(CASE WHEN upper(is_primaryissuer_flag) = 'YES' THEN nullif(trim(cik), '') END)       AS primary_issuer_cik,
        max(CASE WHEN upper(is_primaryissuer_flag) = 'YES' THEN nullif(trim(entityname), '') END) AS primary_issuer_name,
        list(DISTINCT nullif(trim(cik), '')) FILTER (WHERE nullif(trim(cik), '') IS NOT NULL)     AS issuer_ciks,
        list(iss) AS issuers
    FROM iss GROUP BY accessionnumber),
rec_agg AS (SELECT accessionnumber, list(rec) AS recipients      FROM rec GROUP BY accessionnumber),
rel_agg AS (SELECT accessionnumber, list(rel) AS related_persons FROM rel GROUP BY accessionnumber),
sig_agg AS (SELECT accessionnumber, list(sig) AS signatures      FROM sig GROUP BY accessionnumber)
SELECT
    {_S('sub.accessionnumber')}                  AS accession_number,
    {_S('sub.file_num')}                         AS file_num,
    {_D('sub.filing_date')}                      AS filing_date,
    {_S('sub.sic_code')}                         AS sic_code,
    {_S('sub.submissiontype')}                   AS submission_type,
    {_S('sub.testorlive')}                       AS test_or_live,
    {_S('sub.over100personsflag')}               AS over_100_persons_flag,
    {_S('sub.over100issuerflag')}                AS over_100_issuer_flag,
    {_S('sub.schemaversion')}                    AS schema_version,
    ia.primary_issuer_cik                        AS primary_issuer_cik,
    ia.primary_issuer_name                       AS primary_issuer_name,
    ia.issuer_ciks                               AS issuer_ciks,
    {_S('off.industrygrouptype')}                AS industry_group_type,
    {_S('off.investmentfundtype')}               AS investment_fund_type,
    {_S('off.is40act')}                          AS is_40_act,
    {_S('off.revenuerange')}                     AS revenue_range,
    {_N('off.totalofferingamount')}              AS total_offering_amount,
    {_N('off.totalamountsold')}                  AS total_amount_sold,
    {_N('off.totalremaining')}                   AS total_remaining,
    {_N('off.minimuminvestmentaccepted')}        AS minimum_investment_accepted,
    {_S('off.hasnonaccreditedinvestors')}        AS has_non_accredited_investors,
    {_I('off.numbernonaccreditedinvestors')}     AS number_non_accredited_investors,
    {_I('off.totalnumberalreadyinvested')}       AS total_number_already_invested,
    {_S('off.isamendment')}                      AS is_amendment,
    {_D('off.sale_date')}                        AS sale_date,
    {_S('off.yettooccur')}                       AS yet_to_occur,
    {_S('off.morethanoneyear')}                  AS more_than_one_year,
    ia.issuers                                   AS issuers,
    rec_agg.recipients                           AS recipients,
    rel_agg.related_persons                      AS related_persons,
    sig_agg.signatures                           AS signatures,
    off                                          AS offering,
    'edgar' AS source_system,
    '{quarter}' AS fiscal_quarter,
    now() AS ingested_at
FROM sub
LEFT JOIN off       ON off.accessionnumber = sub.accessionnumber
LEFT JOIN iss_agg ia ON ia.accessionnumber = sub.accessionnumber
LEFT JOIN rec_agg   ON rec_agg.accessionnumber = sub.accessionnumber
LEFT JOIN rel_agg   ON rel_agg.accessionnumber = sub.accessionnumber
LEFT JOIN sig_agg   ON sig_agg.accessionnumber = sub.accessionnumber
WHERE {_S('sub.accessionnumber')} IS NOT NULL
"""


def _form_4_sql(files: dict[str, str], quarter: str) -> str:
    """Form 4 / Insider (Forms 3/4/5) — SUBMISSION spine + nested REPORTINGOWNER /
    NONDERIV_* / DERIV_* / FOOTNOTES / OWNER_SIGNATURE. reporting_owner_ciks promoted as a
    flattened LIST<VARCHAR> for FK resolution back to the Identity Spine / to owners."""
    return f"""
WITH
sub  AS (SELECT * FROM {_rd(files['SUBMISSION.tsv'])}),
ro   AS (SELECT * FROM {_rd(files['REPORTINGOWNER.tsv'])}),
ndt  AS (SELECT * FROM {_rd(files['NONDERIV_TRANS.tsv'])}),
ndh  AS (SELECT * FROM {_rd(files['NONDERIV_HOLDING.tsv'])}),
dt   AS (SELECT * FROM {_rd(files['DERIV_TRANS.tsv'])}),
dh   AS (SELECT * FROM {_rd(files['DERIV_HOLDING.tsv'])}),
fn   AS (SELECT * FROM {_rd(files['FOOTNOTES.tsv'])}),
osig AS (SELECT * FROM {_rd(files['OWNER_SIGNATURE.tsv'])}),
ro_agg AS (
    SELECT accession_number,
        list(DISTINCT nullif(trim(rptownercik), '')) FILTER (WHERE nullif(trim(rptownercik), '') IS NOT NULL) AS reporting_owner_ciks,
        list(ro) AS reporting_owners
    FROM ro GROUP BY accession_number),
ndt_agg AS (SELECT accession_number, list(ndt)  AS nonderiv_trans    FROM ndt  GROUP BY accession_number),
ndh_agg AS (SELECT accession_number, list(ndh)  AS nonderiv_holdings FROM ndh  GROUP BY accession_number),
dt_agg  AS (SELECT accession_number, list(dt)   AS deriv_trans       FROM dt   GROUP BY accession_number),
dh_agg  AS (SELECT accession_number, list(dh)   AS deriv_holdings    FROM dh   GROUP BY accession_number),
fn_agg  AS (SELECT accession_number, list(fn)   AS footnotes         FROM fn   GROUP BY accession_number),
osig_agg AS (SELECT accession_number, list(osig) AS owner_signatures FROM osig GROUP BY accession_number)
SELECT
    {_S('sub.accession_number')}     AS accession_number,
    {_D('sub.filing_date')}          AS filing_date,
    {_D('sub.period_of_report')}     AS period_of_report,
    {_D('sub.date_of_orig_sub')}     AS date_of_orig_sub,
    {_S('sub.document_type')}        AS document_type,
    {_S('sub.issuercik')}            AS issuer_cik,
    {_S('sub.issuername')}           AS issuer_name,
    {_S('sub.issuertradingsymbol')}  AS issuer_trading_symbol,
    {_S('sub.no_securities_owned')}  AS no_securities_owned,
    {_S('sub.not_subject_sec16')}    AS not_subject_sec16,
    {_S('sub.form3_holdings_reported')} AS form3_holdings_reported,
    {_S('sub.form4_trans_reported')} AS form4_trans_reported,
    {_S('sub.remarks')}              AS remarks,
    {_S('sub.aff10b5one')}           AS aff_10b5_one,
    roa.reporting_owner_ciks         AS reporting_owner_ciks,
    roa.reporting_owners             AS reporting_owners,
    ndt_agg.nonderiv_trans           AS nonderiv_trans,
    ndh_agg.nonderiv_holdings        AS nonderiv_holdings,
    dt_agg.deriv_trans               AS deriv_trans,
    dh_agg.deriv_holdings            AS deriv_holdings,
    fn_agg.footnotes                 AS footnotes,
    osig_agg.owner_signatures        AS owner_signatures,
    'edgar' AS source_system,
    '{quarter}' AS fiscal_quarter,
    now() AS ingested_at
FROM sub
LEFT JOIN ro_agg roa ON roa.accession_number = sub.accession_number
LEFT JOIN ndt_agg  ON ndt_agg.accession_number  = sub.accession_number
LEFT JOIN ndh_agg  ON ndh_agg.accession_number  = sub.accession_number
LEFT JOIN dt_agg   ON dt_agg.accession_number   = sub.accession_number
LEFT JOIN dh_agg   ON dh_agg.accession_number   = sub.accession_number
LEFT JOIN fn_agg   ON fn_agg.accession_number   = sub.accession_number
LEFT JOIN osig_agg ON osig_agg.accession_number = sub.accession_number
WHERE {_S('sub.accession_number')} IS NOT NULL
"""


# Required TSV members per form (the spine drives row identity; metadata.json / readme are
# ignored). Used to validate the unzip and to feed the SQL builders.
_FORM_MEMBERS: dict[str, list[str]] = {
    "form_d": ["FORMDSUBMISSION.tsv", "OFFERING.tsv", "ISSUERS.tsv", "RECIPIENTS.tsv",
               "RELATEDPERSONS.tsv", "SIGNATURES.tsv"],
    "form_4": ["SUBMISSION.tsv", "REPORTINGOWNER.tsv", "NONDERIV_TRANS.tsv",
               "NONDERIV_HOLDING.tsv", "DERIV_TRANS.tsv", "DERIV_HOLDING.tsv",
               "FOOTNOTES.tsv", "OWNER_SIGNATURE.tsv"],
}
_FORM_SQL = {"form_d": _form_d_sql, "form_4": _form_4_sql}


# ──────────────────────────────────────────────────────────────────────────────
# Lance write / index
# ──────────────────────────────────────────────────────────────────────────────
def _dataset_exists(uri: str, so: dict) -> bool:
    import lance

    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001 — not yet created
        return False


def _write_dataset(data, uri: str, mode: str, so: dict, schema=None) -> None:
    import lance

    kwargs = dict(
        mode=mode,
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )
    if schema is not None:
        kwargs["schema"] = schema
    lance.write_dataset(data, uri, **kwargs)


def _build_indexes(dataset: str, uri: str, so: dict) -> list[str]:
    """Full BTREE + BITMAP scalar-index build (replace=True → idempotent). An index miss is
    logged, never fatal — the Lance data write is the critical artifact."""
    import lance

    ds = lance.dataset(uri, storage_options=so)
    built: list[str] = []
    for col in INDEX_PLAN[dataset]["btree"]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in INDEX_PLAN[dataset]["bitmap"]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    return built


def _optimize_indices(uri: str, so: dict) -> None:
    """Fold new fragments into existing scalar indexes after an incremental quarter merge."""
    import lance

    try:
        lance.dataset(uri, storage_options=so).optimize.optimize_indices()
        print("  index optimize ✓")
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: optimize_indices failed ({exc}); indexes still cover pre-merge rows.")


# ──────────────────────────────────────────────────────────────────────────────
# State + callback
# ──────────────────────────────────────────────────────────────────────────────
def _record_run(dataset, feed, run_mode, write_mode, dataset_uri, quarter, source_files,
                quarters_processed, rows_processed, rows_written, status, error,
                started_at, completed_at) -> None:
    """Terminal run row → ops.edgar_runs (psycopg). Best-effort: never let an audit-write
    failure crash an otherwise-good ingest."""
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.edgar_runs
                    (dataset, feed, run_mode, write_mode, dataset_uri, quarter, source_files,
                     quarters_processed, rows_processed, rows_written, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (dataset, feed, run_mode, write_mode, dataset_uri, quarter, Jsonb(source_files),
                 quarters_processed, rows_processed, rows_written, status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url. FLAT JSON body — NO
    {"data": ...} envelope, NO API key (the callbackHash in the url is the auth)."""
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


def _resolve_form(dataset: str) -> str:
    key = dataset.strip().lower()
    if key not in ("form_4", "form_d"):
        raise ValueError(f"dataset must be 'form_4' or 'form_d', got {dataset!r}")
    return key


# ──────────────────────────────────────────────────────────────────────────────
# Local helpers: fetch + unzip one quarter, build the projected Arrow reader
# ──────────────────────────────────────────────────────────────────────────────
def _fetch_quarter_files(dataset: str, y: int, q: int, work: str) -> tuple[dict[str, str], str]:
    """Download + unzip one quarterly ZIP into ``work``; return (member→path map, zip_basename).
    Members are located by basename anywhere under the extraction dir (Form D nests them under
    a {YYYYQ_d}/ subdir; Insider is flat)."""
    import glob
    import os.path
    import zipfile

    url = _form_url(dataset, y, q)
    zip_base = url.rsplit("/", 1)[-1]
    zip_path = os.path.join(work, zip_base)
    n = _download(url, zip_path)
    print(f"  downloaded {zip_base} ({n:,} bytes)")

    extract_dir = os.path.join(work, f"{dataset}_{y}q{q}")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)

    files: dict[str, str] = {}
    for member in _FORM_MEMBERS[dataset]:
        hits = glob.glob(os.path.join(extract_dir, "**", member), recursive=True)
        if not hits:
            raise RuntimeError(f"{zip_base}: required member {member} not found after unzip")
        files[member] = hits[0]
    return files, zip_base


def _project_quarter(con, dataset: str, files: dict[str, str], quarter: str):
    """Run the form's SQL builder and return a streaming Arrow reader (flat RSS)."""
    sql = _FORM_SQL[dataset](files, quarter)
    return con.sql(sql).to_arrow_reader(batch_size=65536)


def _cleanup(*paths: str) -> None:
    """rm local scratch (files or dirs). Disk-exhaustion guard for backfills (directive)."""
    import os.path
    import shutil

    for p in paths:
        try:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            elif os.path.exists(p):
                os.remove(p)
        except OSError as exc:
            print(f"  WARN cleanup {p}: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# Workers
# ══════════════════════════════════════════════════════════════════════════════
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 20,
    memory=8192,
    cpu=4.0,
)
def ingest_cik_map(trigger_callback_url: str | None = None, mode: str = "overwrite") -> dict:
    """EDGAR Identity Spine. Fetch company_tickers.json (+ exchange when available) → DuckDB
    json_each transform → Arrow → Lance overwrite DIRECT to R2 → BTREE(cik_str, cik10, ticker).
    A weekly full snapshot (Lance MVCC retains prior versions for time-travel)."""
    import datetime as dt
    import os.path

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    as_of = started_at.date().isoformat()
    rows, rows_written = 0, 0
    status, error = "error", None
    sources: list[str] = [CIK_TICKERS_URL]
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    tickers_path = os.path.join(SCRATCH_DIR, "company_tickers.json")
    exchange_path = os.path.join(SCRATCH_DIR, "company_tickers_exchange.json")

    try:
        so = _r2_storage_options()
        _download(CIK_TICKERS_URL, tickers_path)
        # Exchange file is best-effort enrichment ("if available" per directive).
        have_exchange = False
        try:
            _download(CIK_EXCHANGE_URL, exchange_path)
            have_exchange = True
            sources.append(CIK_EXCHANGE_URL)
        except Exception as exc:  # noqa: BLE001
            print(f"exchange file unavailable, proceeding without it: {exc}")

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            table = con.execute(
                _cik_map_sql(tickers_path, exchange_path if have_exchange else None, as_of)
            ).to_arrow_table()
            rows = table.num_rows
        finally:
            con.close()
        print(f"cik_map: parsed {rows:,} (cik,ticker) rows")

        _write_dataset(table, CIK_MAP_URI, mode=mode, so=so)
        del table
        _build_indexes("cik_map", CIK_MAP_URI, so)
        rows_written = lance.dataset(CIK_MAP_URI, storage_options=so).count_rows()
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        _cleanup(tickers_path, exchange_path)
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("cik_map", "edgar_cik_map", "cik_map", mode, CIK_MAP_URI, "weekly",
                    sources, None, int(rows), int(rows_written), status, error,
                    started_at, completed_at)
        _post_callback(trigger_callback_url, {
            "status": status, "rows": int(rows_written), "feed": "edgar_cik_map",
            "dataset": "cik_map", "run_mode": "cik_map", "dataset_uri": CIK_MAP_URI,
        })

    if status != "success":
        raise RuntimeError(f"edgar cik_map ingest failed: {error}")
    return {"status": status, "dataset": "cik_map", "feed": "edgar_cik_map",
            "rows_processed": int(rows), "rows": int(rows_written), "dataset_uri": CIK_MAP_URI}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=8192,
    cpu=4.0,
)
def ingest_form_quarter(dataset: str, quarter: str | None = None,
                        trigger_callback_url: str | None = None) -> dict:
    """Single quarter. Download → unzip → DuckDB spine+nest projection → Arrow → Lance. If the
    dataset exists, merge_insert(accession_number) (idempotent re-run / new-quarter append in
    one path); otherwise overwrite + index. Local scratch is rm'd before returning."""
    import datetime as dt
    import os.path

    import duckdb
    import lance

    dskey = _resolve_form(dataset)
    feed = DATASETS[dskey]["feed"]
    uri = DATASETS[dskey]["uri"]
    started_at = dt.datetime.now(dt.timezone.utc)
    rows, rows_written = 0, 0
    status, error = "error", None
    write_mode = "merge_insert"
    zip_base = None
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    work = os.path.join(SCRATCH_DIR, f"{dskey}_single")

    try:
        so = _r2_storage_options()
        if quarter:
            y, q = _parse_quarter(quarter)
        else:
            recent = _resolve_recent_quarters(dskey, 1)
            if not recent:
                raise RuntimeError(f"no published {dskey} quarter found near today")
            y, q = recent[0]
        qlabel = _quarter_label(y, q)
        _cleanup(work)
        os.makedirs(work, exist_ok=True)
        print(f"[{dskey}] single quarter {qlabel}")
        files, zip_base = _fetch_quarter_files(dskey, y, q, work)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            con.execute("SET temp_directory='/tmp/edgar/duckdb_spill';")
            table = con.execute(_FORM_SQL[dskey](files, qlabel)).to_arrow_table()
            rows = table.num_rows
        finally:
            con.close()
        print(f"[{dskey}] {qlabel}: projected {rows:,} filings")

        if not _dataset_exists(uri, so):
            _write_dataset(table, uri, mode="overwrite", so=so)
            write_mode = "create"
            _build_indexes(dskey, uri, so)
        else:
            (lance.dataset(uri, storage_options=so)
                .merge_insert("accession_number")
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute(table))
            _optimize_indices(uri, so)
        del table
        rows_written = lance.dataset(uri, storage_options=so).count_rows()
        print(f"[{dskey}] {write_mode}: dataset now {rows_written:,} rows")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
    finally:
        _cleanup(work)
        completed_at = dt.datetime.now(dt.timezone.utc)
        qlbl = _quarter_label(*_parse_quarter(quarter)) if quarter else None
        _record_run(dskey, feed, "quarter", write_mode, uri, qlbl,
                    [zip_base] if zip_base else [], 1 if zip_base else 0,
                    int(rows), int(rows_written), status, error, started_at, completed_at)
        _post_callback(trigger_callback_url, {
            "status": status, "rows": int(rows_written), "feed": feed, "dataset": dskey,
            "run_mode": "quarter", "dataset_uri": uri, "quarter": qlbl,
        })

    if status != "success":
        raise RuntimeError(f"edgar {dskey} quarter ingest failed: {error}")
    return {"status": status, "dataset": dskey, "feed": feed, "run_mode": "quarter",
            "rows_processed": int(rows), "rows": int(rows_written), "dataset_uri": uri}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 3,
    memory=8192,
    cpu=4.0,
)
def backfill_form(dataset: str, quarters: int = 4,
                  trigger_callback_url: str | None = None) -> dict:
    """Historical backfill: resolve the most-recent ``quarters`` published files and stream
    them oldest→newest through DuckDB into Lance (first quarter overwrite, rest append — each
    quarter's accessions are disjoint, so no merge is needed), then build indexes ONCE. Each
    quarter's local ZIP + extracted TSVs are rm'd before the next (backfill disk guard)."""
    import datetime as dt
    import os.path

    import duckdb
    import lance

    dskey = _resolve_form(dataset)
    feed = DATASETS[dskey]["feed"]
    uri = DATASETS[dskey]["uri"]
    started_at = dt.datetime.now(dt.timezone.utc)
    rows, rows_written = 0, 0
    status, error = "error", None
    write_mode = "overwrite+append"
    processed: list[str] = []
    os.makedirs(SCRATCH_DIR, exist_ok=True)

    try:
        so = _r2_storage_options()
        recent = _resolve_recent_quarters(dskey, quarters)
        if not recent:
            raise RuntimeError(f"no published {dskey} quarters found near today")
        # oldest → newest so the Lance dataset reads chronologically and overwrite lands first.
        ordered = sorted(recent)
        labels = [_quarter_label(y, q) for y, q in ordered]
        print(f"[{dskey}] backfill {len(ordered)} quarters: {labels}")

        for idx, (y, q) in enumerate(ordered):
            qlabel = _quarter_label(y, q)
            work = os.path.join(SCRATCH_DIR, f"{dskey}_{qlabel}")
            _cleanup(work)
            os.makedirs(work, exist_ok=True)
            try:
                files, zip_base = _fetch_quarter_files(dskey, y, q, work)
                con = duckdb.connect(":memory:")
                try:
                    con.execute("PRAGMA threads=4;")
                    con.execute(f"SET temp_directory='{work}/duckdb_spill';")
                    reader = _project_quarter(con, dskey, files, qlabel)
                    schema = reader.schema
                    _write_dataset(reader, uri,
                                   mode=("overwrite" if idx == 0 else "append"),
                                   so=so, schema=schema)
                finally:
                    con.close()
                n = lance.dataset(uri, storage_options=so).count_rows()
                added = n - rows_written
                rows_written = n
                rows += added
                processed.append(zip_base)
                print(f"[{dskey}] {idx + 1}/{len(ordered)} {qlabel}: "
                      f"+{added:,} rows ({'overwrite' if idx == 0 else 'append'}) → {n:,} total")
            finally:
                _cleanup(work)

        print(f"[{dskey}] backfill committed — {rows_written:,} rows. Building indexes…")
        _build_indexes(dskey, uri, so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(dskey, feed, "backfill", write_mode, uri, "backfill",
                    processed, len(processed), int(rows), int(rows_written), status, error,
                    started_at, completed_at)
        _post_callback(trigger_callback_url, {
            "status": status, "rows": int(rows_written), "feed": feed, "dataset": dskey,
            "run_mode": "backfill", "dataset_uri": uri, "quarter": "backfill",
        })

    if status != "success":
        raise RuntimeError(f"edgar {dskey} backfill failed: {error}")
    return {"status": status, "dataset": dskey, "feed": feed, "run_mode": "backfill",
            "quarters": processed, "rows_processed": int(rows), "rows": int(rows_written),
            "dataset_uri": uri}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 30,
              memory=8192, cpu=4.0)
def reindex(dataset: str) -> dict:
    """Rebuild the BTREE/BITMAP scalar indexes on an existing dataset (no re-ingest)."""
    import lance

    key = dataset.strip().lower()
    if key not in DATASETS:
        raise ValueError(f"dataset must be one of {sorted(DATASETS)}, got {dataset!r}")
    uri = DATASETS[key]["uri"]
    so = _r2_storage_options()
    rows = lance.dataset(uri, storage_options=so).count_rows()
    print(f"Reindexing {uri} — {rows:,} rows")
    built = _build_indexes(key, uri, so)
    return {"dataset": key, "dataset_uri": uri, "rows": rows, "indexes": built}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def apply_migration() -> dict:
    """Create ops.edgar_runs (idempotent). Mirrors ops_edgar_runs.sql."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(_OPS_DDL)
        conn.commit()
        cur.execute("SELECT to_regclass('ops.edgar_runs')")
        present = cur.fetchone()[0]
    print(f"ops.edgar_runs present = {present}")
    return {"table": "ops.edgar_runs", "present": str(present)}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def ledger(limit: int = 15) -> list:
    """Read the most recent ops.edgar_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, dataset, feed, run_mode, write_mode, quarter, quarters_processed, "
            "rows_processed, rows_written, status, error, started_at, completed_at "
            "FROM ops.edgar_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ──────────────────────────────────────────────────────────────────────────────
# Manual ops entrypoints (local — no callback). ops.* write still fires.
# ──────────────────────────────────────────────────────────────────────────────
@app.local_entrypoint()
def migrate() -> None:
    import json
    print(json.dumps(apply_migration.remote(), indent=2, default=str))


@app.local_entrypoint()
def cik_map() -> None:
    import json
    print(json.dumps(ingest_cik_map.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def ingest_quarter(dataset: str = "form_4", quarter: str = "") -> None:
    import json
    print(json.dumps(
        ingest_form_quarter.remote(dataset, quarter=(quarter or None), trigger_callback_url=None),
        indent=2, default=str))


@app.local_entrypoint()
def backfill(dataset: str = "form_4", quarters: int = 4) -> None:
    import json
    print(json.dumps(
        backfill_form.remote(dataset, quarters=quarters, trigger_callback_url=None),
        indent=2, default=str))


@app.local_entrypoint()
def reindex_one(dataset: str = "form_4") -> None:
    import json
    print(json.dumps(reindex.remote(dataset), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 15) -> None:
    import json
    print(json.dumps(ledger.remote(limit), indent=2, default=str))
