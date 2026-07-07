#!/usr/bin/env python3
"""gtm_prime_demand_events — the FPDS action stream of ALL primes.

SoR  s3://data-sink/active/gtm_prime_demand_events/
     (one row per FPDS action, ~24mo window, UNCONSTRAINED prime scope: every
      action with a non-null recipient_uei — ~15-25M rows; Lance;
      snapshot-overwrite; BTREE on uei / naics_code / psc_code / action_type_code)

WHY
The award-event layer over the sub-universe: entity nodes say WHO buys subs; this
says WHICH prime just became obligated, on WHICH order. v1 restricted the prime
set to disclosed sub-buyers (gtm_prime_farmout_combo_lanes ueis); v2 drops that
join — the universe is now the FULL lookalike-winner set, and undisclosed winners
need the same pulse. Every demand recipe is a query-time WHERE — e.g. "needs
subs, now, on this":
    is_first_action AND award_type_code = 'C'
    AND subcontracting_plan IN ('C','D','E','F','G','H') AND NOT has_disclosed_subs
Other first-class pulses: action_type_code = 'Y' (ADD SUBCONTRACT PLAN — the
obligation crystallizing mid-award), E/F/X terminations (incumbent displaced),
H/L definitization + undefinitized_action (letter-contract urgency).

CODES ARE VERBATIM. Every code column carries the SPINE'S OWN inline description
(action_type_description etc.) — no dec_code_domain_ref join at build time, so the
Contracts-vs-Assistance doubled-code trap (A–E share letters across sub_domains)
cannot mislabel anything. dec_code_domain_ref (sub_domain='Contracts') is the UI's
selector vocabulary only. Nothing filtered, nothing interpreted; all action types
carried (operator-directed 2026-07-06: full grain, filters at query time).

FLAGS
  is_first_action     action_date == the award's first action date over FULL
                      history (not just the window) — distinguishes new awards
                      from mods; a first action inside the window = new award.
  has_disclosed_subs  the award key appears in usaspending_subaward_canonical —
                      plan-required + NOT has_disclosed_subs = goals not yet
                      visibly filled.

DOLLARS. obligation_delta is the action's federal_action_obligation VERBATIM —
NET obligated (de-obligations ride through as negatives), not gross. Any rollup
over this column is a net figure.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_prime_demand_events.py [--verify]
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
OUT = f"{A}/gtm_prime_demand_events/"
PARAM_SET_ID = "v2"  # v2: UNCONSTRAINED prime scope — the v1 join to the disclosed
                     # sub-buyer set (gtm_prime_farmout_combo_lanes ueis) is removed;
                     # scope = every FPDS action in the window with a non-null
                     # recipient_uei (audit fix H3 companion, 2026-07-06).
BTREE = ["uei", "naics_code", "psc_code", "action_type_code"]
WINDOW_DAYS = 730

TXN_COLS = [
    "recipient_uei", "contract_award_unique_key", "action_date",
    "federal_action_obligation", "naics_code", "product_or_service_code",
    "action_type_code", "action_type_description",
    "award_type_code", "contract_award_type_desc",
    "subcontracting_plan", "subcontracting_plan_desc",
    "undefinitized_action_code", "undefinitized_action_desc",
    "type_of_set_aside_code", "type_set_aside_description",
    "extent_competed", "solicitation_procedures", "solicitation_procedur_desc",
    "multi_year_contract", "multi_year_contract_desc",
    "type_of_idc", "type_of_idc_description",
    "idv_type_code", "idv_type_description", "parent_award_type_code",
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
    today = date.today()
    as_of = today.isoformat()
    cutoff = (today - timedelta(days=WINDOW_DAYS)).isoformat()
    con = _cx()

    # windowed events — ALL primes (v2: no disclosed sub-buyer join; the only
    # scope gate is a non-null recipient_uei)
    tx = lance.dataset(f"{A}/usaspending_fpds_canonical_txn/", storage_options=opt)
    con.register("_tx", tx.scanner(
        columns=TXN_COLS,
        filter=f"recipient_uei IS NOT NULL AND action_date >= DATE '{cutoff}'").to_reader())
    con.execute("CREATE TABLE ev AS SELECT t.* FROM _tx t")
    print(f"windowed events: {con.execute('SELECT count(*) FROM ev').fetchone()[0]:,}", flush=True)

    # true first action per award (FULL history, key+date only)
    con.register("_tx_first", tx.scanner(
        columns=["contract_award_unique_key", "action_date", "recipient_uei"],
        filter="recipient_uei IS NOT NULL").to_reader())
    con.execute("""CREATE TABLE first_act AS
        SELECT t.contract_award_unique_key, MIN(t.action_date) AS first_action_date
        FROM _tx_first t GROUP BY 1""")

    # awards with any disclosed FSRS subs
    se = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    con.register("_se", se.scanner(columns=["prime_award_unique_key"],
                                   filter="prime_award_unique_key IS NOT NULL").to_reader())
    con.execute("CREATE TABLE subbed AS SELECT DISTINCT prime_award_unique_key AS k FROM _se")

    con.execute("""CREATE TABLE events AS
        SELECT e.recipient_uei AS uei,
               e.contract_award_unique_key AS award_key,
               e.action_date,
               e.federal_action_obligation AS obligation_delta,
               e.naics_code,
               e.product_or_service_code AS psc_code,
               e.action_type_code, e.action_type_description,
               e.award_type_code, e.contract_award_type_desc AS award_type_description,
               e.subcontracting_plan, e.subcontracting_plan_desc,
               e.undefinitized_action_code, e.undefinitized_action_desc,
               e.type_of_set_aside_code, e.type_set_aside_description,
               e.extent_competed,
               e.solicitation_procedures, e.solicitation_procedur_desc,
               e.multi_year_contract, e.multi_year_contract_desc,
               e.type_of_idc, e.type_of_idc_description,
               e.idv_type_code, e.idv_type_description,
               e.parent_award_type_code,
               (e.action_date = f.first_action_date) AS is_first_action,
               f.first_action_date,
               (s.k IS NOT NULL) AS has_disclosed_subs
        FROM ev e
        LEFT JOIN first_act f ON f.contract_award_unique_key = e.contract_award_unique_key
        LEFT JOIN subbed s ON s.k = e.contract_award_unique_key""")
    n = con.execute("SELECT count(*) FROM events").fetchone()[0]
    print(f"events rows: {n:,}", flush=True)

    res = con.execute(f"""SELECT *, DATE '{as_of}' AS as_of,
        'usaspending_fpds_canonical_txn:v{tx.version}|usaspending_subaward_canonical:v{se.version}' AS built_from_version,
        '{PARAM_SET_ID}' AS param_set_id FROM events""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE], storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    """Spot-verify: Torch event count vs direct spine scan; Y rows exist somewhere."""
    opt = so()
    probe = "YA63J5PVEZE6"
    cutoff = (date.today() - timedelta(days=WINDOW_DAYS)).isoformat()
    ds = lance.dataset(OUT, storage_options=opt)
    mart = ds.scanner(filter=f"uei = '{probe}'").to_table().to_pylist()
    tx = lance.dataset(f"{A}/usaspending_fpds_canonical_txn/", storage_options=opt)
    raw = tx.scanner(columns=["contract_award_unique_key"],
                     filter=f"recipient_uei = '{probe}' AND action_date >= DATE '{cutoff}'"
                     ).to_table().num_rows
    if len(mart) != raw:
        print(f"FAIL count mismatch: mart={len(mart)} raw={raw}")
        return 1
    y = ds.scanner(filter="action_type_code = 'Y'").to_table().num_rows
    firsts = sum(1 for r in mart if r["is_first_action"])
    print(f"verify OK: {probe} {len(mart)} events (firsts={firsts}); Y-code rows in mart: {y:,}")
    return 0


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
