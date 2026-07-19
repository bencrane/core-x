"""Materialize ``gtm.equipment_provider_service_areas`` (HQX Postgres) → Lance SoR on R2.

The DOMAIN-keyed service-area research payloads landed by the edge_api raw
lander (``/api/v1/equipment/service-areas-by-domain/land``) — the beyond-SAM
provider plane. One row per landed payload, EXACTLY as landed: record_id,
company_domain, domain_norm, source, raw_payload (jsonb → JSON string),
landed_at. No explode — the geo derivation (parsed → states/footprint) reads
this dataset and writes the derived mart; this stays the verbatim mirror.

BTREE on ``domain_norm`` (the canonical bridge to equipment_provider /
equipment_matchmaking / firmographics_blitz) and ``record_id``. Snapshot
semantics: re-run any time more payloads land.

Run (in-session scale):
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/materialize_equipment_provider_service_areas.py
"""
from __future__ import annotations

import os
import sys

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

DATASET_URI = "s3://data-sink/active/equipment_provider_service_areas/"
INDEXES = [("domain_norm", "BTREE"), ("record_id", "BTREE")]

PROJECTION_SQL = """
SELECT record_id,
       company_domain,
       domain_norm,
       source,
       CAST(raw_payload AS VARCHAR) AS raw_payload,
       landed_at
FROM hqx.gtm.equipment_provider_service_areas
ORDER BY domain_norm, landed_at
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
    dsn = os.environ.get("HQX_DB_URL_DIRECT") or os.environ["HQX_DB_URL_POOLED"]
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{dsn}' AS hqx (TYPE postgres, READ_ONLY)")

    src_ct = con.execute(
        "SELECT count(*) FROM hqx.gtm.equipment_provider_service_areas"
    ).fetchone()[0]
    tbl = con.execute(PROJECTION_SQL).to_arrow_table()
    assert tbl.num_rows == src_ct, f"projection {tbl.num_rows} != source {src_ct}"

    ds = write_indexed_dataset(tbl, DATASET_URI, INDEXES, _r2_storage_options())
    print(f"published {DATASET_URI} rows={ds.count_rows():,} (source {src_ct:,})")


if __name__ == "__main__":
    main()
