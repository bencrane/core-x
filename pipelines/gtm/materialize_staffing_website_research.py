"""Materialize ``gtm.staffing_website_research`` (HQX Postgres) → Lance SoR on R2.

The per-agency website/LinkedIn research payloads landed by the edge_api raw
lander (``/api/v1/staffing/website-research/land``) — one row per landed
payload, EXACTLY as landed: record_id, uei, source, raw_payload (jsonb → JSON
string), landed_at. No explode, no normalization — the normalization
prototyping (geographiesServed → FIPS, rolesPlaced → SOC/SCA) reads THIS
dataset and writes derived sidecars; this dataset stays the verbatim mirror.

BTREE on ``uei`` (the connect key) and ``record_id``. Snapshot semantics:
re-run any time more payloads land (overwrite via the shared publisher).
Source pg stays the write home.

Run (in-session scale):
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/materialize_staffing_website_research.py
"""
from __future__ import annotations

import os
import sys

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

DATASET_URI = "s3://data-sink/active/staffing_website_research/"
INDEXES = [("uei", "BTREE"), ("domain", "BTREE"), ("record_id", "BTREE")]

PROJECTION_SQL = """
SELECT record_id,
       uei,
       domain,
       source,
       CAST(raw_payload AS VARCHAR) AS raw_payload,
       landed_at
FROM hqx.gtm.staffing_website_research
ORDER BY coalesce(uei, domain), landed_at
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


def main() -> None:
    # transaction pooler first: the session pooler (15-max) is consumed by live
    # lander traffic while a batch is arriving.
    dsn = os.environ.get("HQX_DB_URL_TRANSACTION") or os.environ["HQX_DB_URL_POOLED"]
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{dsn}' AS hqx (TYPE postgres, READ_ONLY)")

    src_ct = con.execute(
        "SELECT count(*) FROM hqx.gtm.staffing_website_research"
    ).fetchone()[0]
    tbl = con.execute(PROJECTION_SQL).to_arrow_table()
    assert tbl.num_rows == src_ct, f"projection {tbl.num_rows} != source {src_ct}"

    ds = write_indexed_dataset(tbl, DATASET_URI, INDEXES, _r2_storage_options())
    print(f"published {DATASET_URI} rows={ds.count_rows():,} (source {src_ct:,})")


if __name__ == "__main__":
    main()
