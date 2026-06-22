"""Compute worker — Gen-3 materialization of Prospeo bulk-company CSV exports → Lance SoR.

AD-HOC CSV-DROP PATTERN (not Postgres-sourced). Each Prospeo bulk-export CSV is uploaded
to ``s3://data-sink/landing/prospeo_company_export/<filename>`` and DuckDB-streamed into Lance
at ``s3://data-sink/active/prospeo_company_export/``.

GRAIN / PK. ``record_id = sha256(prospeo_company_id || '|' || sha256(<69-col row signature,
chr(31)-separated, NULL→'' coalesced>))`` — exact-payload idempotency. Re-ingesting the same
CSV is a no-op. Refreshed enrichment fields against the same ``prospeo_company_id`` produce
a NEW row (append-only history).

BRIDGES.
    domain_norm           BTREE → firmographics_blitz.domain_norm  (canonical normalization
                          mirrored from firmographics_blitz._normalized_domain).
    prospeo_company_id    BTREE                                    (upstream stable ID).

SCHEMA.
    6 derived/metadata cols (record_id, domain_norm, saved_at, landed_at, materialized_at,
    source_file, export_batch_id) + 69 source cols snake_cased (all VARCHAR — heterogeneity
    hazard: "Unknown" sentinel mixes with numeric values in funding/employee/job-count cols)
    + raw_payload (JSON-encoded original row).

LIFECYCLE.
    First batch  → empty dataset → lance.write_dataset(mode="overwrite") + create indexes.
    N-th batch   → merge_insert(record_id).when_not_matched_insert_all() (idempotent append).

ENTRYPOINTS.
    modal run    pipelines/gtm/materialize_prospeo_company_export.py::init_ops
    modal run    pipelines/gtm/materialize_prospeo_company_export.py::ingest_csv \\
                     --path "/local/path/to/prospeo_company_export_*.csv"
    modal run    pipelines/gtm/materialize_prospeo_company_export.py::ingest_uri \\
                     --uri "s3://data-sink/landing/prospeo_company_export/<filename>"
    modal run    pipelines/gtm/materialize_prospeo_company_export.py::reindex_only
    modal run    pipelines/gtm/materialize_prospeo_company_export.py::verify_only
    modal deploy pipelines/gtm/materialize_prospeo_company_export.py
"""

from __future__ import annotations

import os

import modal

# ── Lance SoR ────────────────────────────────────────────────────────────────────────────
_ACTIVE = "s3://data-sink/active"
_LANDING = "s3://data-sink/landing"
DATASET = "prospeo_company_export"
DATASET_URI = os.environ.get("PROSPEO_COMPANY_EXPORT_URI", f"{_ACTIVE}/{DATASET}/")
LANDING_PREFIX = f"landing/{DATASET}/"
LANDING_URI = f"{_LANDING}/{DATASET}/"
FEED = "prospeo_company_export"

MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
READ_BATCH_ROWS = 50000
MAX_INLINE_UPLOAD_BYTES = 256 * 1024 * 1024  # local entrypoint inline-upload ceiling

# ── Source-column projection (file order; snake_case map). LOCKED — drives schema, raw_payload,
#    and record_id signature simultaneously. Renaming any second-tuple value is a schema break. ─
SOURCE_COLUMNS: list[tuple[str, str]] = [
    ("Company name",                                                "company_name"),
    ("Company industry",                                            "company_industry"),
    ("Company website",                                             "company_website"),
    ("Company employee range",                                      "company_employee_range"),
    ("Company employee count",                                      "company_employee_count"),
    ("Company employee count on Prospeo",                           "company_employee_count_on_prospeo"),
    ("Company domain",                                              "company_domain"),
    ("Company LinkedIn URL",                                        "company_linkedin_url"),
    ("Company Facebook URL",                                        "company_facebook_url"),
    ("Company Twitter URL",                                         "company_twitter_url"),
    ("Company Instagram URL",                                       "company_instagram_url"),
    ("Company YouTube URL",                                         "company_youtube_url"),
    ("Company Crunchbase URL",                                      "company_crunchbase_url"),
    ("Company type",                                                "company_type"),
    ("Company HQ Phone",                                            "company_hq_phone"),
    ("Company country",                                             "company_country"),
    ("Company country code",                                        "company_country_code"),
    ("Company state",                                               "company_state"),
    ("Company city",                                                "company_city"),
    ("Company time zone",                                           "company_time_zone"),
    ("Company time zone offset",                                    "company_time_zone_offset"),
    ("Company raw address",                                         "company_raw_address"),
    ("Company keywords",                                            "company_keywords"),
    ("Company technologies",                                        "company_technologies"),
    ("Company funding total amount",                                "company_funding_total_amount"),
    ("Company funding total number of rou",                         "company_funding_total_number_of_rou"),
    ("Company last round amount",                                   "company_last_round_amount"),
    ("Company last round type",                                     "company_last_round_type"),
    ("Company last round date",                                     "company_last_round_date"),
    ("Company revenue range",                                       "company_revenue_range"),
    ("Company NAICS codes",                                         "company_naics_codes"),
    ("Company SIC codes",                                           "company_sic_codes"),
    ("Company description",                                         "company_description"),
    ("Company founded",                                             "company_founded"),
    ("Company logo URL",                                            "company_logo_url"),
    ("Company intent",                                              "company_intent"),
    ("Company email domain",                                        "company_email_domain"),
    ("Company main email pattern",                                  "company_main_email_pattern"),
    ("Prospeo Company ID",                                          "prospeo_company_id"),
    ("Company in lists",                                            "company_in_lists"),
    ("Company saved at",                                            "company_saved_at"),
    ("Active job count",                                            "active_job_count"),
    ("Active job titles",                                           "active_job_titles"),
    ("Company MX provider",                                         "company_mx_provider"),
    ("AI Description",                                              "ai_description"),
    ("Ag Financing Classification",                                 "ag_financing_classification"),
    ("Ag Financing Classification justification",                   "ag_financing_classification_justification"),
    ("Ag Financing Classification is Ag Financing Provider",        "ag_financing_classification_is_ag_financing_provider"),
    ("Geographic Scope",                                            "geographic_scope"),
    ("Geographic Scope notes",                                      "geographic_scope_notes"),
    ("Geographic Scope geographic Restriction Type",                "geographic_scope_geographic_restriction_type"),
    ("Geographic Scope operates Nationally",                        "geographic_scope_operates_nationally"),
    ("Geographic Scope website",                                    "geographic_scope_website"),
    ("Geographic Scope company Name",                               "geographic_scope_company_name"),
    ("AI One-Liner",                                                "ai_one_liner"),
    ("Capital Provider JSON",                                       "capital_provider_json"),
    ("Capital Provider JSON provides Capital",                      "capital_provider_json_provides_capital"),
    ("Capital Provider JSON capital Type",                          "capital_provider_json_capital_type"),
    ("Served Industries",                                           "served_industries"),
    ("Served Industries note",                                      "served_industries_note"),
    ("Funding Terms Extract",                                       "funding_terms_extract"),
    ("Funding Terms Extract funding Range",                         "funding_terms_extract_funding_range"),
    ("Funding Terms Extract funding Range Source Url",              "funding_terms_extract_funding_range_source_url"),
    ("Funding Terms Extract time To Funding",                       "funding_terms_extract_time_to_funding"),
    ("Funding Terms Extract time To Funding Source Url",            "funding_terms_extract_time_to_funding_source_url"),
    ("Funding Terms Extract financial Product Types Source Url",    "funding_terms_extract_financial_product_types_source_url"),
    ("Funding Terms Extract geographic Scope",                      "funding_terms_extract_geographic_scope"),
    ("Funding Terms Extract geographic Scope Source Url",           "funding_terms_extract_geographic_scope_source_url"),
    ("Funding Terms Extract notes",                                 "funding_terms_extract_notes"),
]
_SNAKE_BY_SOURCE = {src: snake for src, snake in SOURCE_COLUMNS}
_NOT_NULL = {"record_id", "prospeo_company_id", "company_domain", "source_file",
             "export_batch_id", "raw_payload", "landed_at", "materialized_at"}

INDEXES: dict[str, list[str]] = {
    "BTREE": ["record_id", "prospeo_company_id", "domain_norm", "company_domain"],
    "BITMAP": ["company_country_code", "company_state",
               "capital_provider_json_capital_type",
               "ag_financing_classification_is_ag_financing_provider"],
}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.prospeo_company_export_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,
    source_uri      text        NOT NULL,
    source_file     text        NOT NULL,
    export_batch_id text        NOT NULL,
    datasets        jsonb       NOT NULL,
    mode            text        NOT NULL,
    rows_total      bigint      NOT NULL DEFAULT 0,
    rows_source     bigint      NOT NULL DEFAULT 0,
    rows_added      bigint      NOT NULL DEFAULT 0,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS prospeo_company_export_runs_feed_idx
    ON ops.prospeo_company_export_runs (feed);
CREATE INDEX IF NOT EXISTS prospeo_company_export_runs_status_idx
    ON ops.prospeo_company_export_runs (status);
CREATE INDEX IF NOT EXISTS prospeo_company_export_runs_batch_idx
    ON ops.prospeo_company_export_runs (export_batch_id);
CREATE INDEX IF NOT EXISTS prospeo_company_export_runs_recorded_at_idx
    ON ops.prospeo_company_export_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "psycopg[binary]>=3.2",
    "boto3>=1.35",
    "requests>=2.32",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("prospeo-company-export", image=image)


# ── Schema ───────────────────────────────────────────────────────────────────────────────
def _schema():
    import pyarrow as pa

    ts = pa.timestamp("us", tz="UTC")

    def f(name, typ):
        return pa.field(name, typ, nullable=name not in _NOT_NULL)

    fields = [
        f("record_id", pa.string()),
        f("prospeo_company_id", pa.string()),
        f("company_domain", pa.string()),
        f("domain_norm", pa.string()),
    ]
    fields += [f(snake, pa.string()) for src, snake in SOURCE_COLUMNS
               if snake not in {"prospeo_company_id", "company_domain"}]
    fields += [
        f("saved_at", ts),
        f("raw_payload", pa.string()),
        f("source_file", pa.string()),
        f("export_batch_id", pa.string()),
        f("landed_at", ts),
        f("materialized_at", ts),
    ]
    return pa.schema(fields)


# ── Domain normalization (mirrors firmographics_blitz._normalized_domain verbatim) ──────
def _normalized_domain_sql(col: str) -> str:
    return (
        "nullif("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "lower(trim(CAST(" + col + " AS VARCHAR))),"
        " '^https?://', '', 'g'),"
        " '^www\\.', '', 'g'),"
        " '/.*$', '', 'g'),"
        " '\\.+$', '', 'g'),"
        " '')"
    )


# ── DuckDB projection SQL ────────────────────────────────────────────────────────────────
def _projection_sql(staging_uri: str, source_file: str, export_batch_id: str) -> str:
    """Build the DuckDB SELECT that reads the staged CSV and emits the canonical Arrow shape.

    Reads ``staging_uri`` with ``read_csv_auto(all_varchar=true, header=true)`` — every source
    column stays VARCHAR because Prospeo mixes the ``"Unknown"`` sentinel with numerics
    (funding totals, employee counts, job counts). Only ``Company saved at`` is unambiguous
    ISO 8601 and gets parsed.
    """
    quoted_sources = [f'"{src}"' for src, _ in SOURCE_COLUMNS]
    row_signature_args = ", ".join(f"coalesce({q}, '')" for q in quoted_sources)
    raw_payload_args = ", ".join(f"'{src.replace(chr(39), chr(39) * 2)}', {q}"
                                 for (src, _), q in zip(SOURCE_COLUMNS, quoted_sources))

    projections = [
        # Identity / bridges
        "sha256(\n"
        "    \"Prospeo Company ID\" || '|' || sha256(\n"
        f"        concat_ws(chr(31), {row_signature_args})\n"
        "    )\n"
        ") AS record_id",
        '"Prospeo Company ID" AS prospeo_company_id',
        '"Company domain"     AS company_domain',
        f"{_normalized_domain_sql(chr(34) + 'Company domain' + chr(34))} AS domain_norm",
    ]
    # All other source columns (snake_cased, VARCHAR)
    seen = {"prospeo_company_id", "company_domain"}
    for src, snake in SOURCE_COLUMNS:
        if snake in seen:
            continue
        projections.append(f'"{src}" AS {snake}')
    # Timestamp parse for saved_at
    projections.append(
        'try_strptime("Company saved at", \'%Y-%m-%dT%H:%M:%S.%f\') '
        "AT TIME ZONE 'UTC' AS saved_at"
    )
    # Raw payload (JSON object preserving every source col)
    projections.append(f"CAST(json_object({raw_payload_args}) AS VARCHAR) AS raw_payload")
    # File-batch lineage + materialization stamps
    src_esc = source_file.replace(chr(39), chr(39) * 2)
    batch_esc = export_batch_id.replace(chr(39), chr(39) * 2)
    projections.append(f"'{src_esc}' AS source_file")
    projections.append(f"'{batch_esc}' AS export_batch_id")
    projections.append("now() AT TIME ZONE 'UTC' AS landed_at")
    projections.append("now() AT TIME ZONE 'UTC' AS materialized_at")

    proj_block = ",\n        ".join(projections)
    uri_esc = staging_uri.replace(chr(39), chr(39) * 2)
    return f"""
        SELECT
        {proj_block}
        FROM read_csv_auto(
            '{uri_esc}',
            header=true,
            all_varchar=true,
            quote='"',
            escape='"',
            ignore_errors=false
        )
        WHERE "Prospeo Company ID" IS NOT NULL
          AND "Company domain"     IS NOT NULL
    """


# ── R2 / boto3 helpers ───────────────────────────────────────────────────────────────────
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the r2-credentials secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _r2_endpoint() -> str:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the r2-credentials secret.")
    return endpoint


def _s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=_r2_endpoint(),
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"),
    )


def _attach_duckdb(con) -> None:
    so = _r2_storage_options()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL json; LOAD json;")
    con.execute("PRAGMA threads=4;")
    secret_sql = f"""
        CREATE OR REPLACE SECRET r2 (
            TYPE s3,
            KEY_ID '{so["aws_access_key_id"]}',
            SECRET '{so["aws_secret_access_key"]}',
            ENDPOINT '{so["endpoint"].replace("https://", "").replace("http://", "")}',
            URL_STYLE 'path',
            REGION 'auto'
        );
    """
    con.execute(secret_sql)


# ── Lance helpers ────────────────────────────────────────────────────────────────────────
def _dataset_exists(so: dict) -> bool:
    import lance

    try:
        lance.dataset(DATASET_URI, storage_options=so)
        return True
    except (FileNotFoundError, ValueError):
        return False
    except Exception:  # noqa: BLE001
        return False


def _create_indexes(so: dict) -> list[dict]:
    import lance

    ds = lance.dataset(DATASET_URI, storage_options=so)
    out: list[dict] = []
    for index_type, cols in INDEXES.items():
        for col in cols:
            try:
                ds.create_scalar_index(col, index_type=index_type)
                print(f"  {index_type:<6} ✓ {DATASET}.{col}")
                out.append({"col": col, "type": index_type, "ok": True})
            except Exception as exc:  # noqa: BLE001
                print(f"  {index_type:<6} ✗ {DATASET}.{col}: {exc}")
                out.append({"col": col, "type": index_type, "ok": False, "error": str(exc)})
    return out


def _committed_index_names(so: dict) -> list[str]:
    import lance

    ds = lance.dataset(DATASET_URI, storage_options=so)
    names = []
    for ix in ds.list_indices():
        names.append(ix.get("name", str(ix)) if isinstance(ix, dict) else getattr(ix, "name", str(ix)))
    return sorted(names)


def _record_run(*, mode, source_uri, source_file, export_batch_id, rows_total, rows_source,
                rows_added, status, error, started_at, completed_at) -> None:
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
                INSERT INTO ops.prospeo_company_export_runs
                    (feed, source_uri, source_file, export_batch_id, datasets, mode,
                     rows_total, rows_source, rows_added, status, error,
                     started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (FEED, source_uri, source_file, export_batch_id,
                 Jsonb({DATASET: rows_total}), mode,
                 rows_total, rows_source, rows_added, status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
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


_SECRETS = [modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")]


# ── Modal: upload local bytes to R2 landing ──────────────────────────────────────────────
@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10, memory=4096)
def upload_landing(content: bytes, source_file: str) -> dict:
    """Upload CSV bytes to s3://data-sink/landing/prospeo_company_export/<source_file>."""
    import hashlib

    key = f"{LANDING_PREFIX}{source_file}"
    sha = hashlib.sha256(content).hexdigest()
    _s3_client().put_object(Bucket="data-sink", Key=key, Body=content,
                            ContentType="text/csv")
    uri = f"s3://data-sink/{key}"
    print(f"uploaded {len(content):,} bytes → {uri}  sha256={sha[:16]}…")
    return {"uri": uri, "key": key, "bytes": len(content), "sha256": sha,
            "source_file": source_file}


# ── Modal: ingest from a landing URI ─────────────────────────────────────────────────────
@app.function(secrets=_SECRETS, timeout=60 * 60, memory=16384, cpu=4.0)
def ingest_from_uri(staging_uri: str, source_file: str | None = None,
                    export_batch_id: str | None = None,
                    trigger_callback_url: str | None = None) -> dict:
    import datetime as dt
    import hashlib
    import os.path

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    rows_total = rows_source = rows_added = 0
    status, error = "error", None
    mode = "unknown"
    src_file = source_file or os.path.basename(staging_uri.rstrip("/"))
    batch_id = export_batch_id or hashlib.sha256(src_file.encode("utf-8")).hexdigest()

    try:
        so = _r2_storage_options()
        already = _dataset_exists(so)
        before = lance.dataset(DATASET_URI, storage_options=so).count_rows() if already else 0
        mode = "append" if already else "overwrite"
        print(f"target dataset {'exists' if already else 'EMPTY'} — mode={mode}, "
              f"rows_before={before:,}")

        con = duckdb.connect(":memory:")
        try:
            _attach_duckdb(con)
            sql = _projection_sql(staging_uri, src_file, batch_id)
            rows_source = con.sql(
                f"SELECT count(*) FROM ({sql}) t"
            ).fetchone()[0]
            print(f"source rows (CSV {staging_uri}): {rows_source:,}")
            if rows_source == 0 and not already:
                # Materialize an empty dataset so the schema/indexes go live.
                import pyarrow as pa
                empty = pa.Table.from_pylist([], schema=_schema())
                lance.write_dataset(
                    empty, DATASET_URI, mode="overwrite",
                    data_storage_version=DATA_STORAGE_VERSION,
                    max_rows_per_file=MAX_ROWS_PER_FILE,
                    max_bytes_per_file=MAX_BYTES_PER_FILE,
                    storage_options=so,
                )
                rows_total = 0
            elif rows_source > 0:
                tbl = con.sql(sql).to_arrow_table().cast(_schema())
                if already:
                    ds = lance.dataset(DATASET_URI, storage_options=so)
                    ds.merge_insert("record_id").when_not_matched_insert_all().execute(tbl)
                else:
                    lance.write_dataset(
                        tbl, DATASET_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE,
                        max_bytes_per_file=MAX_BYTES_PER_FILE,
                        storage_options=so,
                    )
        finally:
            con.close()

        rows_total = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        rows_added = rows_total - before
        print(f"{mode}: rows_before={before:,} → rows_total={rows_total:,} "
              f"(rows_added={rows_added:,})")

        if rows_total > 0:
            _create_indexes(so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(
            mode=mode, source_uri=staging_uri, source_file=src_file,
            export_batch_id=batch_id, rows_total=rows_total, rows_source=rows_source,
            rows_added=rows_added, status=status, error=error,
            started_at=started_at, completed_at=completed_at,
        )
        _post_callback(trigger_callback_url, {
            "status": status, "feed": FEED, "source_uri": staging_uri,
            "source_file": src_file, "export_batch_id": batch_id, "mode": mode,
            "rows_total": rows_total, "rows_source": rows_source, "rows_added": rows_added,
        })

    if status != "success":
        raise RuntimeError(f"prospeo_company_export ingest failed: {error}")
    return {"feed": FEED, "mode": mode, "source_uri": staging_uri,
            "source_file": src_file, "export_batch_id": batch_id,
            "rows_total": rows_total, "rows_source": rows_source,
            "rows_added": rows_added, "status": status}


# ── Modal: reindex / verify ──────────────────────────────────────────────────────────────
@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15,
              memory=8192, cpu=4.0)
def reindex() -> dict:
    so = _r2_storage_options()
    print(f"=== reindex {DATASET} ===")
    _create_indexes(so)
    return {DATASET: _committed_index_names(so)}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10, memory=8192)
def verify() -> dict:
    import lance
    import pyarrow.compute as pc

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()
    keys = ds.to_table(columns=["record_id", "prospeo_company_id", "domain_norm"])
    distinct_record = pc.count_distinct(keys.column("record_id")).as_py()
    distinct_prospeo = pc.count_distinct(keys.column("prospeo_company_id")).as_py()
    distinct_domain = pc.count_distinct(keys.column("domain_norm")).as_py()
    unique_ok = (n == distinct_record)
    sample = next((v for v in keys.column("record_id").to_pylist() if v), None)
    probe = (ds.scanner(columns=["record_id"],
                        filter=f"record_id = '{sample}'").to_table().num_rows
             if sample else -1)
    out = {
        "uri": DATASET_URI, "rows": n,
        "distinct_record_id": distinct_record,
        "distinct_prospeo_company_id": distinct_prospeo,
        "distinct_domain_norm": distinct_domain,
        "unique_invariant_ok": unique_ok,
        "schema": [f.name for f in ds.schema],
        "indexes": _committed_index_names(so),
        f"probe_record_id={sample!r}": probe,
    }
    print(f"{DATASET}: {n:,} rows · distinct(record_id)={distinct_record:,} "
          f"· distinct(prospeo_company_id)={distinct_prospeo:,} "
          f"· distinct(domain_norm)={distinct_domain:,} · unique_ok={unique_ok}")
    if n > 0 and not unique_ok:
        raise RuntimeError(
            f"uniqueness invariant FAILED: rows={n} != distinct(record_id)={distinct_record}"
        )
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name='prospeo_company_export_runs'
            ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    print(f"ops.prospeo_company_export_runs ready — columns: {cols}")
    return {"table": "ops.prospeo_company_export_runs", "columns": cols}


# ── Local entrypoints ────────────────────────────────────────────────────────────────────
@app.local_entrypoint()
def init_ops() -> None:
    import json

    print(json.dumps(apply_ops_ddl.remote(), indent=2, default=str))


@app.local_entrypoint()
def ingest_csv(path: str) -> None:
    """Stage a local CSV to R2 landing, then ingest it.

    The file MUST be ≤ MAX_INLINE_UPLOAD_BYTES (256 MiB) — beyond that, upload to R2
    yourself and call ``ingest_uri`` directly.
    """
    import hashlib
    import json
    import os.path

    if not os.path.isfile(path):
        raise RuntimeError(f"file not found: {path}")
    size = os.path.getsize(path)
    if size > MAX_INLINE_UPLOAD_BYTES:
        raise RuntimeError(
            f"{path}: {size:,} bytes exceeds the {MAX_INLINE_UPLOAD_BYTES:,}-byte inline "
            "upload ceiling. Upload to R2 yourself and call ingest_uri."
        )

    with open(path, "rb") as fh:
        content = fh.read()
    src_file = os.path.basename(path)
    batch_id = hashlib.sha256(src_file.encode("utf-8")).hexdigest()

    upload = upload_landing.remote(content, src_file)
    result = ingest_from_uri.remote(
        staging_uri=upload["uri"],
        source_file=src_file,
        export_batch_id=batch_id,
        trigger_callback_url=None,
    )
    print(json.dumps({"upload": upload, "ingest": result}, indent=2, default=str))


@app.local_entrypoint()
def ingest_uri(uri: str, source_file: str | None = None,
               export_batch_id: str | None = None) -> None:
    import json

    print(json.dumps(ingest_from_uri.remote(
        staging_uri=uri, source_file=source_file, export_batch_id=export_batch_id,
        trigger_callback_url=None,
    ), indent=2, default=str))


@app.local_entrypoint()
def reindex_only() -> None:
    import json

    print(json.dumps(reindex.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_only() -> None:
    import json

    print(json.dumps(verify.remote(), indent=2, default=str))
