#!/usr/bin/env python3
"""gtm_prime_sub_pairs — the pair-complete FSRS prime↔sub edge mart.

SoR  s3://data-sink/active/gtm_prime_sub_pairs/
     (1 row per (prime_awardee_uei, subawardee_uei) pair over the FULL
      usaspending_subaward_canonical spine; Lance; derived, snapshot-overwrite;
      BTREE on prime_uei / sub_uei)

WHY
govcon_teaming_edges is NOT pair-complete (it is a curated 5y slice) — recipes
that need per-prime teaming stats (n partners, repeat depth) or per-pair edge
history were falling back to spine scans or an incomplete edge set. This mart
groups the entire canonical subaward spine by the (prime, sub) pair once, so
buyer teaming stats become one indexed scan and the sub-universe recipe drops
govcon_teaming_edges entirely.

GRAIN + WINDOWS. Per pair: edge $ and edge counts for the trailing 5y
(subaward_action_date >= as_of - 1826d) AND lifetime, any_value names,
first/last action dates. Windows are rolling, anchored at build-time as_of —
rerun to refresh. Unknown ≠ zero: 5y columns are true zero (no edges in
window), never imputed. Amounts are NET as reported on the FSRS lines
(negative/corrected lines included), not gross.

FULL GRAIN, NO PRUNING: every pair with a non-null prime UEI is carried —
support floors and windows are query-time predicates.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_prime_sub_pairs.py [--verify]
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
OUT = f"{A}/gtm_prime_sub_pairs/"
PARAM_SET_ID = "v1"
BTREE = ["prime_uei", "sub_uei"]
WINDOW_5Y_DAYS = 1826


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
    w5y = (today - timedelta(days=WINDOW_5Y_DAYS)).isoformat()
    con = _cx()

    se = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    con.register("_se", se.scanner(
        columns=["prime_awardee_uei", "subawardee_uei", "prime_awardee_name",
                 "subawardee_name", "subaward_amount", "subaward_action_date"],
        filter="prime_awardee_uei IS NOT NULL AND subawardee_uei IS NOT NULL").to_reader())
    con.execute(f"""CREATE TABLE pairs AS
        SELECT prime_awardee_uei AS prime_uei,
               subawardee_uei AS sub_uei,
               any_value(prime_awardee_name) AS prime_name,
               any_value(subawardee_name) AS sub_name,
               COALESCE(SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w5y}'), 0) AS edge_dollars_5y,
               COUNT(*) FILTER (subaward_action_date >= DATE '{w5y}') AS edge_count_5y,
               COALESCE(SUM(subaward_amount), 0) AS edge_dollars_lifetime,
               COUNT(*) AS edge_count_lifetime,
               MIN(subaward_action_date) AS first_action_date,
               MAX(subaward_action_date) AS last_action_date
        FROM _se GROUP BY 1, 2""")
    n = con.execute("SELECT COUNT(*) FROM pairs").fetchone()[0]
    print(f"prime-sub pairs: {n:,} rows", flush=True)

    res = con.execute(f"""SELECT *, DATE '{as_of}' AS as_of,
        'usaspending_subaward_canonical:v{se.version}' AS built_from_version,
        '{PARAM_SET_ID}' AS param_set_id FROM pairs""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE], storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    """Spot-verify one heavy prime: pair set + lifetime $/counts exact vs raw spine."""
    opt = so()
    probe = "YA63J5PVEZE6"  # TORCH TECHNOLOGIES — heavy sub-out engine
    ds = lance.dataset(OUT, storage_options=opt)
    mart = ds.scanner(filter=f"prime_uei = '{probe}'").to_table().to_pylist()
    se = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    raw = se.scanner(filter=f"prime_awardee_uei = '{probe}' AND subawardee_uei IS NOT NULL",
                     columns=["subawardee_uei", "subaward_amount"]).to_table().to_pylist()
    agg: dict = {}
    cnt: dict = {}
    for r in raw:
        k = r["subawardee_uei"]
        agg[k] = agg.get(k, 0.0) + float(r["subaward_amount"] or 0)
        cnt[k] = cnt.get(k, 0) + 1
    mart_usd = {r["sub_uei"]: float(r["edge_dollars_lifetime"] or 0) for r in mart}
    mart_cnt = {r["sub_uei"]: int(r["edge_count_lifetime"] or 0) for r in mart}
    if set(agg) != set(mart_usd):
        print(f"FAIL pair sets differ: raw={len(agg)} mart={len(mart_usd)}")
        return 1
    for k, v in agg.items():
        if abs(v - mart_usd[k]) > 0.01:
            print(f"FAIL $ mismatch {k}: raw {v:,.2f} vs mart {mart_usd[k]:,.2f}")
            return 1
        if cnt[k] != mart_cnt[k]:
            print(f"FAIL count mismatch {k}: raw {cnt[k]} vs mart {mart_cnt[k]}")
            return 1
    print(f"verify OK: {probe} {len(agg)} pairs, lifetime $ + counts exact match")
    return 0


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
