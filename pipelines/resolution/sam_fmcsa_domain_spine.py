"""Compute worker — SAM.gov ↔ FMCSA deterministic domain spine (Pattern-B bridge).

The ``resolution-pipelines`` Modal app. A standalone, exact-match bridge dataset
that links public federal entities (SAM.gov Entity Registrations) to motor
carriers (FMCSA Company Census) on a single deterministic vector: the normalized
corporate **web domain**. No fuzzy matching, no probabilistic scoring — a strict
equijoin on a cleaned domain token.

Reconnaissance (approved) proved the vector: 42,621 clean corporate domains
intersect across the two datasets. This worker materializes the link table that
fan-out-expands that intersection into (entity × carrier) rows.

Source-of-truth inputs (read-only; this worker never mutates them):
  - SAM   s3://data-sink/active/entity_registrations/   (Lance v2.0, 19.3M rows)
            The entity URL is NOT a named column — it lives inside the
            ``pipe_fields`` LIST. Position drifts across layout vintages, so this
            worker is HARD-SCOPED to the current ``format_family='v2'`` snapshot,
            where the URL is deterministically at ``pipe_fields[27]`` (1-based).
            Legacy_v1 (URL at pipe_fields[25]) is excluded by construction
            (snapshot-isolation guardrail).
  - FMCSA s3://data-sink/active/fmcsa/census/           (Lance v2.0, 4.44M rows)
            ``email_address`` is a clean named column. ``carrier_docket`` is NULL
            in census (tabular feed), so the MC number is recovered from the
            ``docket{1,2,3}`` / ``docket{1,2,3}prefix`` pairs.

Data plane (clean-room — DuckDB does 100% of the transform):
    Lance(SAM v2 snapshot) ─┐
                            ├─► DuckDB: norm_host + email-domain split + symmetric
    Lance(FMCSA census)   ─┘            CONSUMER_BLOCK→NULL, then INNER equijoin
      → Arrow  → con.sql(...).to_arrow_table()
      → lance.write_dataset(LOCAL, v2.0)  + BTREE(uei), BTREE(dot_number)
      → boto3 mirror → s3://data-sink/active/bridge_sam_fmcsa_domain/

    R2 NOTE (same as the source workers): Lance's native object-store writer emits
    variable-size multipart chunks that Cloudflare R2 rejects ("All non-trailing
    parts must have the same length"). So Lance is written to LOCAL scratch and the
    directory is mirrored to R2 with boto3 (uniform 8 MiB parts). DuckDB still does
    100% of the transform; Lance is still the format and R2 the system of record.

Normalization (identical, symmetric logic on both sides — see _norm_host_sql /
_email_domain_sql): lowercase, trim, strip http(s):// , strip leading www. , drop
path/port/query/fragment, strip leading/trailing dots. A host is kept only if it
looks like a real domain (is_domain: has a dot, 4–253 chars, no spaces, ends in a
≥2-char alpha TLD) AND is not in CONSUMER_BLOCK; otherwise it is written to NULL
and therefore can never satisfy the inner equijoin (multi-tenant suppression).

    modal run    pipelines/resolution/sam_fmcsa_domain_spine.py            # build
    modal run    pipelines/resolution/sam_fmcsa_domain_spine.py --dry-run  # plan + counts, no write
    modal deploy pipelines/resolution/sam_fmcsa_domain_spine.py
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
SAM_SRC_URI = "s3://data-sink/active/entity_registrations/"
CENSUS_SRC_URI = "s3://data-sink/active/fmcsa/census/"
ACTIVE_PREFIX = os.environ.get("BRIDGE_ACTIVE_PREFIX", "active/bridge_sam_fmcsa_domain")
DATASET_URI = f"s3://{BUCKET}/{ACTIVE_PREFIX}/"
LOCAL_DATASET = "/tmp/bridge_sam_fmcsa_domain"
FEED = "bridge_sam_fmcsa_domain"

# SAM URL token position inside pipe_fields, by layout family (1-based DuckDB list
# index). v2 ONLY — legacy_v1 (idx 25) is intentionally out of scope: its layout
# drifts across monthly vintages and the snapshot-isolation guardrail forbids it.
SAM_V2_URL_IDX = 27

# Lance fragment sizing — 90 GiB is Lance's documented default. The output is small
# (one fragment), so these are effectively "never split".
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

# Columns BTREE-indexed for instant Metabase lookup (resolution keys, both sides).
INDEX_COLS = ("uei", "dot_number")

# --------------------------------------------------------------------------- #
# CONSUMER_BLOCK — domains written to NULL on BOTH sides before the join.
#
# Three tiers. A domain in any tier can never be a join key, which kills
# high-cardinality multi-tenant false positives (one ISP/mailbox shared by
# thousands of unrelated carriers + one unrelated SAM registrant).
#
# Curation rule for the regional-ISP tier: a domain qualifies as multi-tenant
# noise when MANY unrelated FMCSA carriers share it but it maps to ~1 SAM entity
# (an ISP/telco), measured during reconnaissance. Domains with high distinct-UEI
# counts (real corporate families — wm.com=446, republicservices.com=100,
# hilton.com=344, sysco.com=18, snapon.com=4) are deliberately KEPT.
# --------------------------------------------------------------------------- #
CONSUMER_MAILBOX = (
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com",
    "msn.com", "live.com", "comcast.net", "comcast.com", "sbcglobal.net", "att.net",
    "verizon.net", "bellsouth.net", "ymail.com", "me.com", "mac.com", "cox.net",
    "charter.net", "earthlink.net", "juno.com", "aim.com", "gmx.com", "protonmail.com",
    "proton.me", "mail.com", "email.com", "windstream.net", "frontier.com",
    "roadrunner.com", "rocketmail.com", "optonline.net", "optimum.net", "q.com",
    "centurylink.net", "centurytel.net", "embarqmail.com", "netzero.net", "netzero.com",
    "peoplepc.com", "hughes.net", "frontiernet.net", "citlink.net", "midco.net",
    # consumer twins seen in the Mexican-carrier long tail
    "yahoo.com.mx", "hotmail.com.mx", "live.com.mx", "prodigy.net.mx", "outlook.es",
    "hotmail.es", "yahoo.es",
)
PLACEHOLDER_JUNK = (
    "test.com", "example.com", "example.org", "example.net", "none.com", "none.gov",
    "none.net", "na.com", "xxx.com", "abc.com", "company.com", "noemail.com",
    "no-email.com", "nomail.com", "website.com", "domain.com", "sample.com",
    "email.net", "test.net", "asdf.com",
)
# Regional / rural ISP & telephone-cooperative mailbox domains empirically found
# as multi-tenant noise in the top shared-domain analysis (high carriers, ~1 UEI).
REGIONAL_ISP = (
    "fairpoint.net", "paulbunyan.net", "wowway.com", "bevcomm.net", "gvtel.com",
    "alliancecom.net", "wiktel.com", "wtrt.net", "infowest.com", "polarcomm.com",
    "3rivers.net", "nntc.net", "skybest.com", "srt.com", "nemont.net", "nemont.com",
    "marktwain.net", "nctc.com", "venturecomm.net", "westriv.com", "htc.net",
    "wwt.net", "ncn.net", "highland.net", "runestone.net", "hcinet.net", "wabash.net",
    "evertek.net", "surry.net", "hardynet.com", "socket.net", "silverstar.com",
    "twinvalley.net", "bbtel.com", "vcn.com", "mhtc.net", "wyoming.com", "pmt.org",
    "gondtc.com", "dishmail.net", "dakotacom.net", "rtconnect.net",
)
CONSUMER_BLOCK: tuple[str, ...] = tuple(
    sorted(set(CONSUMER_MAILBOX + PLACEHOLDER_JUNK + REGIONAL_ISP))
)

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=0.19",      # provides `import lance`
    "pyarrow>=17",
    "boto3>=1.35",        # R2 active-layer mirror (uniform multipart)
    "psycopg[binary]>=3.2",  # ops.* terminal state
)

app = modal.App("resolution-pipelines", image=image)


# --------------------------------------------------------------------------- #
# R2 / S3
# --------------------------------------------------------------------------- #
def _r2_storage_options() -> dict[str, str]:
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
    """boto3 S3 client for R2 with checksum behaviour forced to ``when_required``
    (botocore's default flexible-checksum validation otherwise 4xxs against R2)."""
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


def _clear_r2_prefix(s3, prefix: str) -> int:
    deleted, batch = 0, []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix + "/"):
        for o in page.get("Contents", []):
            batch.append({"Key": o["Key"]})
            if len(batch) == 1000:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})
                deleted += len(batch); batch = []
    if batch:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})
        deleted += len(batch)
    return deleted


def _upload_dir_to_r2(s3, local_dir: str, prefix: str) -> int:
    count = 0
    for root, _dirs, files in os.walk(local_dir):
        for fn in files:
            full = os.path.join(root, fn)
            rel = os.path.relpath(full, local_dir).replace(os.sep, "/")
            s3.upload_file(full, BUCKET, f"{prefix}/{rel}")
            count += 1
    return count


# --------------------------------------------------------------------------- #
# DuckDB transform — pure SQL builders (importable without modal/auth)
# --------------------------------------------------------------------------- #
def _sql_in_list(items: tuple[str, ...]) -> str:
    return "(" + ",".join("'" + s.replace("'", "''") + "'" for s in items) + ")"


def _norm_host_sql(col: str) -> str:
    """SAM entity_url → bare host. lowercase+trim → strip protocol → strip leading
    www. → drop path/port/query/fragment → strip leading/trailing dots."""
    return (
        r"trim(regexp_replace(regexp_replace(regexp_replace("
        rf"lower(trim({col})),"
        r"'^https?://',''),'^www\.',''),'[/:?#].*$',''),'.')"
    )


def _email_domain_sql(col: str) -> str:
    """FMCSA email → bare host. lowercase+trim → take everything after the LAST @
    → cut at the first char illegal in a host (handles 'a@x.com;b@y.com') → strip
    leading/trailing dots."""
    return (
        r"trim(regexp_replace("
        rf"regexp_replace(lower(trim({col})),'^.*@',''),"
        r"'[^a-z0-9.-].*$',''),'.')"
    )


def _is_domain_or_null_sql(host_expr_alias: str, block_sql: str) -> str:
    """Keep a host only if it is a plausible domain AND not blocklisted; else NULL.
    (Operates on an already-computed alias to avoid recomputing the regex chain.)"""
    h = host_expr_alias
    return (
        f"CASE WHEN {h} LIKE '%.%' AND length({h}) BETWEEN 4 AND 253 "
        f"AND {h} NOT LIKE '% %' AND regexp_matches({h},'\\.[a-z]{{2,}}$') "
        f"AND {h} NOT IN {block_sql} THEN {h} END"
    )


def build_spine_sql(block_sql: str) -> str:
    """Full DuckDB statement. Assumes two relations are registered:
      sam_src    — uei, legal_business_name, pipe_fields   (FILTERED to v2 + latest label)
      census_src — carrier_dot, docket{1,2,3}[prefix], email_address
    Returns the unified link table: uei, legal_business_name, dot_number,
    mc_number, normalized_domain."""
    sam_host = _norm_host_sql(f"pipe_fields[{SAM_V2_URL_IDX}]")
    eml_host = _email_domain_sql("email_address")
    mc = (
        "CASE WHEN docket1prefix='MC' THEN CAST(TRY_CAST(docket1 AS BIGINT) AS VARCHAR) "
        "WHEN docket2prefix='MC' THEN CAST(TRY_CAST(docket2 AS BIGINT) AS VARCHAR) "
        "WHEN docket3prefix='MC' THEN CAST(TRY_CAST(docket3 AS BIGINT) AS VARCHAR) END"
    )
    return f"""
WITH sam_h AS (
    SELECT uei, legal_business_name, {sam_host} AS host
    FROM sam_src
    WHERE pipe_fields[{SAM_V2_URL_IDX}] IS NOT NULL
      AND trim(pipe_fields[{SAM_V2_URL_IDX}]) <> ''
),
sam_dom AS (
    SELECT DISTINCT uei, legal_business_name,
           {_is_domain_or_null_sql('host', block_sql)} AS normalized_domain
    FROM sam_h
),
cen_h AS (
    SELECT carrier_dot AS dot_number, {mc} AS mc_number, {eml_host} AS host
    FROM census_src
    WHERE email_address LIKE '%@%' AND carrier_dot IS NOT NULL
),
cen_dom AS (
    SELECT DISTINCT dot_number, mc_number,
           {_is_domain_or_null_sql('host', block_sql)} AS proxy_domain
    FROM cen_h
)
SELECT
    s.uei,
    s.legal_business_name,
    f.dot_number,
    f.mc_number,
    s.normalized_domain
FROM sam_dom s
JOIN cen_dom f ON f.proxy_domain = s.normalized_domain
"""


# --------------------------------------------------------------------------- #
# Snapshot resolution
# --------------------------------------------------------------------------- #
def _resolve_latest_v2_label(so: dict[str, str]) -> str:
    """Newest current v2 monthly snapshot (lexical max of extract_label). Hard-scopes
    the build to one partition and honours the snapshot-isolation guardrail."""
    import duckdb
    import lance

    ds = lance.dataset(SAM_SRC_URI, storage_options=so)
    con = duckdb.connect()
    con.register("l", ds.scanner(columns=["format_family", "extract_label"],
                                 filter="format_family='v2'").to_reader())
    label = con.sql("SELECT max(extract_label) FROM l").fetchone()[0]
    con.close()
    if not label:
        raise RuntimeError("No v2 snapshot found in entity_registrations.")
    return label


# --------------------------------------------------------------------------- #
# ops ledger + Trigger callback
# --------------------------------------------------------------------------- #
OPS_DDL = (
    "CREATE SCHEMA IF NOT EXISTS ops",
    """
    CREATE TABLE IF NOT EXISTS ops.bridge_runs (
        id            bigserial PRIMARY KEY,
        bridge        text        NOT NULL,
        sam_label     text,
        rows_written  bigint      NOT NULL DEFAULT 0,
        distinct_uei  bigint,
        distinct_dot  bigint,
        status        text        NOT NULL,
        error         text,
        started_at    timestamptz NOT NULL,
        completed_at  timestamptz
    )
    """,
)


def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record_run(*, sam_label, rows, d_uei, d_dot, status, error, started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            for stmt in OPS_DDL:
                cur.execute(stmt)
            cur.execute(
                """
                INSERT INTO ops.bridge_runs
                    (bridge, sam_label, rows_written, distinct_uei, distinct_dot,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, sam_label, rows, d_uei, d_dot, status, error, started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* write failed: {exc}")
    finally:
        conn.close()


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
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


# --------------------------------------------------------------------------- #
# Core build
# --------------------------------------------------------------------------- #
def _materialize(con, sam, cen, label: str):
    """Register the two sources (SAM hard-filtered to the v2 snapshot), run the
    spine SQL, and return the Arrow link table."""
    con.register(
        "sam_src",
        sam.scanner(columns=["uei", "legal_business_name", "pipe_fields"],
                    filter=f"format_family='v2' AND extract_label='{label}'").to_reader(),
    )
    con.register(
        "census_src",
        cen.scanner(columns=["carrier_dot", "docket1prefix", "docket1", "docket2prefix",
                             "docket2", "docket3prefix", "docket3", "email_address"]).to_reader(),
    )
    return con.sql(build_spine_sql(_sql_in_list(CONSUMER_BLOCK))).to_arrow_table()


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def build_domain_spine(trigger_callback_url: str | None = None) -> dict:
    """Build the SAM↔FMCSA domain spine and publish it to R2 active. Idempotent:
    the active prefix is cleared and replaced (full overwrite)."""
    import datetime as dt
    import shutil

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error, rows, d_uei, d_dot, label = "error", None, 0, 0, 0, None
    try:
        label = _resolve_latest_v2_label(so)
        sam = lance.dataset(SAM_SRC_URI, storage_options=so)
        cen = lance.dataset(CENSUS_SRC_URI, storage_options=so)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            table = _materialize(con, sam, cen, label)
        finally:
            con.close()
        rows = table.num_rows
        d_uei = len(set(table.column("uei").to_pylist()))
        d_dot = len(set(table.column("dot_number").to_pylist()))

        shutil.rmtree(LOCAL_DATASET, ignore_errors=True)
        lance.write_dataset(table, LOCAL_DATASET, mode="overwrite",
                            data_storage_version="2.0",
                            max_rows_per_file=MAX_ROWS_PER_FILE,
                            max_bytes_per_file=MAX_BYTES_PER_FILE)
        ds = lance.dataset(LOCAL_DATASET)
        for col in INDEX_COLS:
            try:
                ds.create_scalar_index(col, index_type="BTREE")
                print(f"Created BTREE index on {col}")
            except Exception as exc:  # noqa: BLE001
                print(f"WARN: BTREE index on {col} failed (non-fatal): {exc}")

        s3 = _s3_client()
        cleared = _clear_r2_prefix(s3, ACTIVE_PREFIX)
        uploaded = _upload_dir_to_r2(s3, LOCAL_DATASET, ACTIVE_PREFIX)
        print(f"Published {FEED}: cleared {cleared}, uploaded {uploaded} files → {DATASET_URI}")
        shutil.rmtree(LOCAL_DATASET, ignore_errors=True)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(sam_label=label, rows=int(rows), d_uei=int(d_uei), d_dot=int(d_dot),
                    status=status, error=error, started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": int(rows), "feed": FEED, "sam_label": label})

    if status != "success":
        raise RuntimeError(f"domain spine build failed: {error}")
    return {"feed": FEED, "sam_label": label, "rows": int(rows),
            "distinct_uei": int(d_uei), "distinct_dot": int(d_dot), "dataset": DATASET_URI}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_spine() -> dict:
    """Read-back proof: open the published bridge from R2 and report row count,
    schema, distinct keys, and index presence — independent of the write path."""
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()
    idx = [i["name"] if isinstance(i, dict) else str(i) for i in ds.list_indices()]
    return {"rows": n, "schema": [f"{f.name}:{f.type}" for f in ds.schema],
            "indices": idx, "uri": DATASET_URI}


@app.local_entrypoint()
def build(dry_run: bool = False) -> None:
    if dry_run:
        # Dry-run computes the count without writing — used at the review gate.
        import duckdb
        import lance

        so = _r2_storage_options()
        label = _resolve_latest_v2_label(so)
        sam = lance.dataset(SAM_SRC_URI, storage_options=so)
        cen = lance.dataset(CENSUS_SRC_URI, storage_options=so)
        con = duckdb.connect(":memory:")
        con.execute("PRAGMA threads=4;")
        table = _materialize(con, sam, cen, label)
        print(f"[dry-run] sam_label={label} rows={table.num_rows:,} "
              f"distinct_uei={len(set(table.column('uei').to_pylist())):,} "
              f"distinct_dot={len(set(table.column('dot_number').to_pylist())):,}")
        con.close()
        return
    print(build_domain_spine.remote(trigger_callback_url=None))
    print(verify_spine.remote())
