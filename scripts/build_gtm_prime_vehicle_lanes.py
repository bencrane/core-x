#!/usr/bin/env python3
"""gtm_prime_vehicle_lanes — what each prime pushes to subs, per master vehicle.

SoR  s3://data-sink/active/gtm_prime_vehicle_lanes/
     (uei × parent_piid, windowed subaward $ ISSUED under that parent vehicle;
      Lance; derived, snapshot-overwrite; BTREE on uei + parent_piid)

WHY
The vehicle gate of the diagnostic form ("I can only execute under these Parent
PIIDs") needs prime-node-level culling on parent_piid. The fact exists per-row on
usaspending_subaward_canonical (prime_award_parent_piid) but no (prime × vehicle)
rollup existed and parent_piid is not an indexed axis anywhere — a query-time gate
meant a spine scan per map re-render. This materializes the FSRS-evidence
semantics: vehicles the prime has DEMONSTRABLY pushed sub $ through (the stronger
gate), not merely vehicles it holds paper on (that weaker gate is an award_search
question, deliberately not conflated here).

Edges with no parent PIID (standalone awards) are EXCLUDED — this mart answers
the vehicle question only; standalone farm-out $ lives in
gtm_prime_farmout_combo_lanes.

WINDOWS are rolling, anchored at build-time as_of (12/24/60mo + lifetime) — same
staleness model as the other gtm marts; rerun to refresh.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_prime_vehicle_lanes.py [--verify]
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import lance

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

A = "s3://data-sink/active"
OUT = f"{A}/gtm_prime_vehicle_lanes/"
PARAM_SET_ID = "v1"
BTREE = ["uei", "parent_piid"]


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def _cx():
    con = duckdb.connect()
    con.execute("SET memory_limit='20GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")
    return con


def build() -> int:
    opt = so()
    today = date.today()
    as_of = today.isoformat()
    w12 = (today - timedelta(days=365)).isoformat()
    w24 = (today - timedelta(days=730)).isoformat()
    w60 = (today - timedelta(days=1826)).isoformat()
    con = _cx()

    se = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    con.register("_se", se.scanner(
        columns=["prime_awardee_uei", "subawardee_uei", "subaward_amount",
                 "subaward_action_date", "prime_award_parent_piid"],
        filter="prime_awardee_uei IS NOT NULL AND prime_award_parent_piid IS NOT NULL").to_reader())
    con.execute(f"""CREATE TABLE veh AS
        SELECT prime_awardee_uei AS uei,
               trim(prime_award_parent_piid) AS parent_piid,
               SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w12}') AS farmout_amt_12mo,
               SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w24}') AS farmout_amt_24mo,
               SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w60}') AS farmout_amt_60mo,
               SUM(subaward_amount) AS farmout_amt_lifetime,
               COUNT(*) FILTER (subaward_action_date >= DATE '{w24}') AS n_subawards_24mo,
               COUNT(*) AS n_subawards_lifetime,
               COUNT(DISTINCT subawardee_uei) FILTER (subaward_action_date >= DATE '{w60}') AS n_distinct_subs_60mo,
               COUNT(DISTINCT subawardee_uei) AS n_distinct_subs_lifetime,
               MIN(subaward_action_date) AS first_action_date,
               MAX(subaward_action_date) AS last_action_date
        FROM _se
        WHERE trim(prime_award_parent_piid) != ''
        GROUP BY 1, 2""")
    n = con.execute("SELECT COUNT(*) FROM veh").fetchone()[0]
    print(f"vehicle lanes: {n:,} rows", flush=True)

    res = con.execute(f"""SELECT *, DATE '{as_of}' AS as_of,
        'usaspending_subaward_canonical:v{se.version}' AS built_from_version,
        '{PARAM_SET_ID}' AS param_set_id FROM veh""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE], storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    """Spot-verify against a direct spine group-by for one heavy prime."""
    opt = so()
    probe = "YA63J5PVEZE6"  # TORCH TECHNOLOGIES
    ds = lance.dataset(OUT, storage_options=opt)
    mart = {r["parent_piid"]: float(r["farmout_amt_lifetime"] or 0)
            for r in ds.scanner(filter=f"uei = '{probe}'").to_table().to_pylist()}
    se = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    raw = se.scanner(filter=f"prime_awardee_uei = '{probe}'",
                     columns=["prime_award_parent_piid", "subaward_amount"]).to_table().to_pylist()
    agg: dict = {}
    for r in raw:
        v = (r["prime_award_parent_piid"] or "").strip()
        if not v:
            continue
        agg[v] = agg.get(v, 0.0) + float(r["subaward_amount"] or 0)
    if set(agg) != set(mart):
        print(f"FAIL vehicle sets differ: raw={len(agg)} mart={len(mart)}")
        return 1
    for k, v in agg.items():
        if abs(v - mart[k]) > 0.01:
            print(f"FAIL $ mismatch {k}: raw {v:,.2f} vs mart {mart[k]:,.2f}")
            return 1
    print(f"verify OK: {probe} {len(agg)} vehicles, lifetime $ exact match")
    return 0


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
