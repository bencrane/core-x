"""Materialize ``gtm.combo_job_to_be_done`` (HQX Postgres) → Lance SoR on R2.

The combo JTBD phrases — "to: …" job sentences per (naics_code, psc_code,
model_id) — were generated in two distinct runs that MUST stay separate rows
(operator ruling 2026-07-18: the ``opus-4.8-canonical`` set is not yet trusted
for decisions; ``gpt-5.4`` is the reference generation):

    model_id             source          rows
    gpt-5.4              clay            ~21k
    opus-4.8-canonical   subagent-waves  ~11k

ONE dataset, ``model_id`` as a first-class BTREE-indexed column — query one
generation with ``model_id = '…'``, never blended by default. Affiliated to
the combo grain: BTREE on ``naics_code`` and ``psc_code`` (family rollups via
``substr()`` at query time, fleet-standard).

Source is read-only; HQX pg stays the write home of the phrases. This is a
snapshot materialization (overwrite semantics via the shared local-stage
publisher). Re-run any time the pg table grows.

Data plane: pg → DuckDB (ATTACH postgres, 100% of the projection in SQL) →
Arrow → ``write_indexed_dataset`` (local stage + boto3 publish, R2-safe) →
``s3://data-sink/active/combo_job_to_be_done/``.

Run (in-session scale — 32k rows, no Modal):
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/materialize_combo_jtbd.py
"""
from __future__ import annotations

import os
import sys

import duckdb
import lance

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

DATASET_URI = "s3://data-sink/active/combo_job_to_be_done/"
INDEXES = [("naics_code", "BTREE"), ("psc_code", "BTREE"), ("model_id", "BTREE")]

PROJECTION_SQL = """
SELECT DISTINCT
    naics_code,
    psc_code,
    model_id,
    source,
    output_sentence,
    landed_at
FROM hqx.gtm.combo_job_to_be_done
WHERE naics_code IS NOT NULL AND psc_code IS NOT NULL
  AND output_sentence IS NOT NULL AND model_id IS NOT NULL
ORDER BY naics_code, psc_code, model_id
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
    dsn = os.environ["HQX_DB_URL_POOLED"]
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{dsn}' AS hqx (TYPE postgres, READ_ONLY)")

    src_ct = con.execute(
        "SELECT count(*) FROM hqx.gtm.combo_job_to_be_done"
    ).fetchone()[0]
    tbl = con.execute(PROJECTION_SQL).to_arrow_table()
    print(f"source rows {src_ct:,} → projected {tbl.num_rows:,}")

    ds = write_indexed_dataset(tbl, DATASET_URI, INDEXES, _r2_storage_options())
    per_model = con.execute(
        "SELECT model_id, count(*) FROM tbl GROUP BY 1 ORDER BY 1"
    ).fetchall()
    print(f"published {DATASET_URI} rows={ds.count_rows():,} version={ds.version}")
    for model_id, n in per_model:
        print(f"  {model_id}: {n:,}")


if __name__ == "__main__":
    main()
