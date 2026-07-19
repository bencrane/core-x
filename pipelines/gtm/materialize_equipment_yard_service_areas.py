"""Materialize ``gtm.equipment_yard_service_areas`` (HQX Postgres) → Lance SoR on R2.

The per-yard website research payloads landed by the edge_api raw lander
(``/api/v1/equipment/service-areas/land``) — one row per landed payload,
EXACTLY as landed: record_id, uei, source, raw_payload (jsonb → JSON string),
landed_at. No explode, no normalization — downstream derivations (equipmentItems
→ PSC buckets, geographies → centroids) read THIS dataset and write derived
sidecars; this dataset stays the verbatim mirror. Negative verdicts ("not an
equipment provider") are rows like any other.

BTREE on ``uei`` (the connect key — the operator's roster id) and ``record_id``.
Snapshot semantics: re-run any time more payloads land (overwrite via the shared
publisher). Source pg stays the write home.

Run (in-session scale):
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/materialize_equipment_yard_service_areas.py
"""
from __future__ import annotations

import os
import sys

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

DATASET_URI = "s3://data-sink/active/equipment_yard_service_areas/"
INDEXES = [("uei", "BTREE"), ("record_id", "BTREE")]

PROJECTION_SQL = """
SELECT record_id,
       uei,
       source,
       CAST(raw_payload AS VARCHAR) AS raw_payload,
       landed_at
FROM hqx.gtm.equipment_yard_service_areas
ORDER BY uei, landed_at
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
    # Direct preferred: the pooler runs session-mode with a small client cap and
    # this is a one-shot read; fall back to the pooled DSN if direct is unset.
    dsn = os.environ.get("HQX_DB_URL_DIRECT") or os.environ["HQX_DB_URL_POOLED"]
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{dsn}' AS hqx (TYPE postgres, READ_ONLY)")

    src_ct = con.execute(
        "SELECT count(*) FROM hqx.gtm.equipment_yard_service_areas"
    ).fetchone()[0]
    tbl = con.execute(PROJECTION_SQL).to_arrow_table()
    assert tbl.num_rows == src_ct, f"projection {tbl.num_rows} != source {src_ct}"

    ds = write_indexed_dataset(tbl, DATASET_URI, INDEXES, _r2_storage_options())
    print(f"published {DATASET_URI} rows={ds.count_rows():,} (source {src_ct:,})")


if __name__ == "__main__":
    main()
