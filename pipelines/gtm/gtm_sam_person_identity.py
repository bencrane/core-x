"""gtm_sam_person_identity — materialized person-identity match results.

Strategy: docs/plans/GTM_SAM_AUDIENCE_MART_STRATEGY.md (v2, aligned) §4.4.

Grain: 1 row per sam_person_id with a resolved identity. SHIPS EMPTY —
populated afterward by a separate, supervised, in-session match exercise
against clay_find_people (decision log #5). This module owns:

  init    — create the empty dataset with the frozen schema (refuses to
            clobber a non-empty dataset).
  append  — validated append of match-result rows from the match phase:
            schema-checked, PK-deduped against existing rows, target dataset
            version + row count recorded to the ledger. First append builds
            the scalar indices.

Contact data is NEVER copied here — phone_resolutions / work_emails / MV
verdicts stay in their own Lances and join on person_linkedin_url_norm.

Run:
    doppler run -- python3 pipelines/gtm/gtm_sam_person_identity.py     # init (empty)
"""

from __future__ import annotations

import datetime as dt
import json
import os

_PROD_URI = "s3://data-sink/active/gtm_sam_person_identity/"
DATASET_URI = os.environ.get("GTM_SAM_PERSON_IDENTITY_LANCE_URI", _PROD_URI)

DATA_STORAGE_VERSION = "2.1"

BTREE_INDEXES = ["sam_person_id", "uei", "person_linkedin_url_norm"]
BITMAP_INDEXES = ["match_method"]

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.gtm_sam_person_identity_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,
    dataset_uri         text        NOT NULL,
    op                  text        NOT NULL,   -- init | append
    rows_appended       bigint,
    rows_total_after    bigint,
    match_lineage       jsonb,                  -- matched-against uri/version/rows
    status              text        NOT NULL,
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS gtm_sam_person_identity_runs_recorded_at_idx
    ON ops.gtm_sam_person_identity_runs (recorded_at DESC);
"""


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def schema():
    import pyarrow as pa

    return pa.schema([
        pa.field("sam_person_id", pa.string()),           # PK — sha256(uei|name_key)
        pa.field("uei", pa.string()),
        pa.field("name_key", pa.string()),
        pa.field("person_linkedin_url_norm", pa.string()),
        pa.field("match_source", pa.string()),            # e.g. clay record_id
        pa.field("match_method", pa.string()),            # e.g. clay_name_domain_exact
        pa.field("match_score", pa.float64()),
        pa.field("matched_against_uri", pa.string()),
        pa.field("matched_against_version", pa.int64()),
        pa.field("matched_at", pa.timestamp("us")),
        pa.field("build_id", pa.string()),
    ])


def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record(op, uri, rows_appended, rows_total, match_lineage, status, error,
            started_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.gtm_sam_person_identity_runs
                    (feed, dataset_uri, op, rows_appended, rows_total_after,
                     match_lineage, status, error, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                ("gtm_sam_person_identity", uri, op, rows_appended, rows_total,
                 json.dumps(match_lineage) if match_lineage else None,
                 status, error, started_at, dt.datetime.now(dt.timezone.utc)))
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* write failed: {exc}")
    finally:
        conn.close()


def run_init(dataset_uri: str | None = None) -> dict:
    """Create the empty identity dataset. Refuses to overwrite a non-empty one."""
    import lance
    import pyarrow as pa

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    uri = dataset_uri or DATASET_URI
    try:
        existing = lance.dataset(uri, storage_options=so)
        n = existing.count_rows()
        if n > 0:
            raise RuntimeError(
                f"REFUSED: {uri} exists with {n:,} rows — init only creates "
                f"empty datasets. The match phase owns populated versions.")
        print(f"{uri} exists empty (v{existing.version}) — rewriting schema.")
    except RuntimeError:
        raise
    except Exception:  # noqa: BLE001 — dataset absent → net-new create
        pass

    empty = pa.table({f.name: pa.array([], type=f.type) for f in schema()},
                     schema=schema())
    lance.write_dataset(empty, uri, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION,
                        storage_options=so)
    ds = lance.dataset(uri, storage_options=so)
    # Scalar indices are built on first append — Lance index builds on an
    # empty dataset are a no-op risk, and the empty state serves no queries.
    _record("init", uri, 0, ds.count_rows(), None, "success", None, started_at)
    print(f"initialized {uri} v{ds.version} rows={ds.count_rows()} "
          f"schema={[f.name for f in schema()]}")
    return {"dataset": uri, "version": ds.version, "rows": ds.count_rows()}


def append_matches(table, match_lineage: list[dict],
                   dataset_uri: str | None = None) -> dict:
    """Validated append from the match phase. `table` must carry the frozen
    schema; rows whose sam_person_id already exists are dropped (first match
    wins — a re-match is an explicit delete+append, never a silent overwrite).
    `match_lineage` = [{'name','uri','version','rows_at_read'}, ...] for the
    matched-against dataset(s) — required (live-probe rule)."""
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    uri = dataset_uri or DATASET_URI
    if not match_lineage:
        raise ValueError("match_lineage is required (live-probe rule)")
    if table.schema.names != schema().names:
        raise ValueError(f"schema mismatch: {table.schema.names}")

    ds = lance.dataset(uri, storage_options=so)
    existing_ids = set()
    if ds.count_rows() > 0:
        existing_ids = set(
            ds.to_table(columns=["sam_person_id"])["sam_person_id"].to_pylist())
    mask = [pid not in existing_ids
            for pid in table["sam_person_id"].to_pylist()]
    fresh = table.filter(mask)
    if fresh.num_rows == 0:
        _record("append", uri, 0, ds.count_rows(), match_lineage,
                "success", "no fresh rows", started_at)
        return {"appended": 0, "total": ds.count_rows()}

    lance.write_dataset(fresh, uri, mode="append", storage_options=so)
    ds = lance.dataset(uri, storage_options=so)
    have = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
            for i in ds.list_indices()}
    for col in BTREE_INDEXES:
        if f"{col}_idx" not in have:
            ds.create_scalar_index(col, index_type="BTREE")
    for col in BITMAP_INDEXES:
        if f"{col}_idx" not in have:
            ds.create_scalar_index(col, index_type="BITMAP")
    total = ds.count_rows()
    _record("append", uri, fresh.num_rows, total, match_lineage,
            "success", None, started_at)
    print(f"appended {fresh.num_rows:,} → total {total:,} (v{ds.version})")
    return {"appended": fresh.num_rows, "total": total, "version": ds.version}


if __name__ == "__main__":
    print(run_init())
