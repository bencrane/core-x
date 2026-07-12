"""build_naics_psc_equipment_needs_lance — land the combo→equipment-needs LLM
verdicts from the HQX control-plane Postgres into Lance (R2 SoR).

Source  gtm.combo_work_summary_equipment_needs  (HQX_DB_URL_DIRECT) — one row per
        (naics_code, psc_code) combo; proposed_equipment_needs + reasoning +
        confidence produced upstream in Clay (GPT), stored verbatim.
SoR     s3://data-sink/active/naics_psc_equipment_needs/
        (grain: naics_code x psc_code; snapshot-overwrite; BTREE naics_code / psc_code)

Parallel to naics_psc_labor_profile (the labor analog). Publishes via the local-stage
helper: write to local disk, build scalar indices locally (R2 rejects Lance's native
index multipart writes), then upload the tree with snapshot-overwrite.

Run:  doppler run -- python3 scripts/build_naics_psc_equipment_needs_lance.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

OUT = "s3://data-sink/active/naics_psc_equipment_needs/"
BTREE = ["naics_code", "psc_code"]
COLS = ["naics_code", "psc_code", "model_id", "source",
        "proposed_equipment_needs", "reasoning", "confidence", "landed_at"]


def so() -> dict:
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": os.environ.get("R2_ENDPOINT")
        or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        "region": "auto",
    }


def main() -> None:
    with psycopg.connect(os.environ["HQX_DB_URL_DIRECT"]) as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT naics_code, psc_code, model_id, source,
                   proposed_equipment_needs, reasoning, confidence,
                   landed_at::text
            FROM gtm.combo_work_summary_equipment_needs
            ORDER BY naics_code, psc_code
        """)
        rows = cur.fetchall()
    print(f"read {len(rows):,} combos from gtm.combo_work_summary_equipment_needs")

    cols = list(zip(*rows)) if rows else [[]] * len(COLS)
    table = pa.table({name: pa.array(col, type=pa.string()) for name, col in zip(COLS, cols)})

    ds = write_indexed_dataset(table, OUT, [(c, "BTREE") for c in BTREE], storage_options=so())
    print(f"published {ds.count_rows():,} rows -> {OUT}  (BTREE: {', '.join(BTREE)})")


if __name__ == "__main__":
    main()
