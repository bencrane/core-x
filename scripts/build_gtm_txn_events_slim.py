#!/usr/bin/env python3
"""gtm_txn_events_slim — full-history FPDS action grain, grammar-bound columns ONLY.

SoR  s3://data-sink/active/gtm_txn_events_slim/
     (1 row per FPDS action over the FULL usaspending_fpds_canonical_txn spine —
      ~108M rows; Lance; snapshot-overwrite; BTREE on uei / action_type_code /
      naics_code / psc_code / awarding_agency_code / action_date)

WHY
The phrase-query event lane (apps/catalyst_api/src/phrase_compiler.py) falls
through to the 108M-row × 392-col canonical txn spine at request time. This mart
is the SLIM projection of that spine — ONLY the closed-grammar columns the
compiler can bind on the transaction grain: action_type_code, subcontracting_plan,
naics_code, psc_code (product_or_service_code), awarding_agency_code, action_date,
federal_action_obligation, recipient_uei, plus the unique action + award keys. No
descriptions, no 380 unbound columns — the spine scan collapses to a projection.

This is NOT gtm_prime_demand_events: that mart is a 24-month window with FSRS
disclosure joins and 20+ descriptive columns for the demand-pulse surface. This
mart is FULL history, projection-only, and feeds Layer 2's month rollup + the
hybrid-exactness partial-month tail. gtm_prime_demand_events stays untouched.

COLUMNS ARE VERBATIM. Every value is the spine's own column, unmodified; nothing
interpreted, nothing filtered except a non-null recipient_uei (the grammar's
recipient axis — a null-UEI action cannot be attributed to a company).

DOLLARS. obligation is federal_action_obligation VERBATIM — NET obligated
(de-obligations ride as negatives). Any rollup over it is a net figure.

    doppler run --project core-x --config prd -- \
      /Users/benjamincrane/core-x/.venv/bin/python \
      scripts/build_gtm_txn_events_slim.py [--verify]
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import duckdb
import lance

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

A = "s3://data-sink/active"
OUT = f"{A}/gtm_txn_events_slim/"
PARAM_SET_ID = "v1"
BTREE = ["uei", "action_type_code", "naics_code", "psc_code",
         "awarding_agency_code", "action_date"]

# The grammar-bound projection — verified against the live schema (txn v19):
# recipient_uei, action_date, action_type_code, subcontracting_plan, naics_code,
# product_or_service_code, awarding_agency_code, federal_action_obligation, plus
# the unique action key + the award key (for hybrid joins upstream).
TXN_COLS = [
    "contract_transaction_unique_key",
    "contract_award_unique_key",
    "recipient_uei",
    "action_date",
    "action_type_code",
    "subcontracting_plan",
    "naics_code",
    "product_or_service_code",
    "awarding_agency_code",
    "federal_action_obligation",
]


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
    as_of = date.today().isoformat()
    con = _cx()

    tx = lance.dataset(f"{A}/usaspending_fpds_canonical_txn/", storage_options=opt)
    print(f"source txn v{tx.version} rows={tx.count_rows():,}", flush=True)

    # Streaming projection — no ORDER, no aggregation; scanner projects the 10
    # grammar columns and filters non-null recipient_uei at scan time. DuckDB
    # streams batches straight to the Lance writer (spill-safe).
    con.register("_tx", tx.scanner(
        columns=TXN_COLS,
        filter="recipient_uei IS NOT NULL").to_reader())

    res = con.execute(f"""SELECT
            contract_transaction_unique_key AS action_key,
            contract_award_unique_key AS award_key,
            recipient_uei AS uei,
            action_date,
            action_type_code,
            subcontracting_plan,
            naics_code,
            product_or_service_code AS psc_code,
            awarding_agency_code,
            federal_action_obligation AS obligation,
            DATE '{as_of}' AS as_of,
            'usaspending_fpds_canonical_txn:v{tx.version}' AS built_from_version,
            '{PARAM_SET_ID}' AS param_set_id
        FROM _tx""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE], storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    """For one heavy recipient UEI: per-action-type counts + $ sums from the
    slim mart must equal a direct group-by on the canonical txn spine."""
    opt = so()
    con = _cx()
    ds = lance.dataset(OUT, storage_options=opt)
    tx = lance.dataset(f"{A}/usaspending_fpds_canonical_txn/", storage_options=opt)

    # deterministic heavy probe: the uei with the most rows in the slim mart
    con.register("_m", ds.scanner(columns=["uei"]).to_reader())
    probe = con.execute(
        "SELECT uei FROM _m WHERE uei IS NOT NULL GROUP BY 1 ORDER BY count(*) DESC LIMIT 1"
    ).fetchone()[0]
    print(f"probe uei={probe}", flush=True)

    mart = ds.scanner(
        columns=["action_type_code", "obligation"],
        filter=f"uei = '{probe}'").to_table().to_pylist()
    m_agg: dict = {}
    for r in mart:
        k = r["action_type_code"]
        c, s = m_agg.get(k, (0, 0.0))
        m_agg[k] = (c + 1, s + float(r["obligation"] or 0))

    raw = tx.scanner(
        columns=["action_type_code", "federal_action_obligation"],
        filter=f"recipient_uei = '{probe}'").to_table().to_pylist()
    r_agg: dict = {}
    for r in raw:
        k = r["action_type_code"]
        c, s = r_agg.get(k, (0, 0.0))
        r_agg[k] = (c + 1, s + float(r["federal_action_obligation"] or 0))

    if set(m_agg) != set(r_agg):
        print(f"FAIL action-type key set mismatch: mart {sorted(map(str,m_agg))} vs raw {sorted(map(str,r_agg))}")
        return 1
    for k in r_agg:
        mc, ms = m_agg[k]
        rc, rs = r_agg[k]
        if mc != rc or abs(ms - rs) > 0.01:
            print(f"FAIL action_type={k}: mart ({mc}, {ms:,.2f}) vs raw ({rc}, {rs:,.2f})")
            return 1
    print(f"verify OK: {probe} — {len(mart):,} actions across {len(r_agg)} "
          f"action types; per-type counts + $ sums exact vs spine group-by")
    return 0


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
