#!/usr/bin/env python3
"""gtm_txn_recipient_month_rollup — the whole-month event rollup off Layer 1.

SoR  s3://data-sink/active/gtm_txn_recipient_month_rollup/
     (grain: uei × action_type_code × plan_class × naics_code × psc_code ×
      awarding_agency_code × month; n_actions + obligation_sum; Lance;
      snapshot-overwrite; BTREE on uei / naics_code / psc_code /
      awarding_agency_code / action_type_code / month)

WHY
The phrase-query event lane collapses recipients over the 108M-row canonical txn
spine at request time. Whole months never change once closed — this mart
pre-aggregates them off Layer 1 (gtm_txn_events_slim), so a companies-by-event
collapse reads a compact rollup for closed months and the partial current month
comes off Layer 1's raw grain. That hybrid is EXACT (see --verify).

Built FROM Layer 1 (gtm_txn_events_slim), NOT the spine — Layer 1 already carries
the grammar-bound projection with non-null uei enforced.

plan_class buckets the raw subcontracting_plan code:
    'A'        subcontracting_plan = 'A'  (no subcontracting possibilities)
    'B'        subcontracting_plan = 'B'  (plan not required)
    'attached' subcontracting_plan IN ('C','D','E','F','G','H')  (FAR 19.7 set)
    NULL       subcontracting_plan is NULL / any other value  (unknown ≠ a class)
NULL ≠ zero: a null plan code buckets to plan_class NULL, never coerced to a code.

month is the first-of-month DATE of action_date (DATE_TRUNC('month', action_date)).

DOLLARS. obligation_sum is Σ federal_action_obligation — NET (de-obligations ride
as negatives), matching Layer 1.

    doppler run --project core-x --config prd -- \
      /Users/benjamincrane/core-x/.venv/bin/python \
      scripts/build_gtm_txn_recipient_month_rollup.py [--verify]
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

# Construction NAICS — frozen verbatim from phrase_compiler.CONSTRUCTION_NAICS.
# Pinned here (not imported) to keep the build script free of the app package.
CONSTRUCTION_NAICS = (
    "236115", "236116", "236117", "236118", "236210", "236220", "237110",
    "237120", "237130", "237210", "237310", "237990", "238110", "238120",
    "238130", "238140", "238150", "238160", "238170", "238190", "238210",
    "238220", "238290", "238310", "238320", "238330", "238340", "238350",
    "238390", "238910", "238990",
)

A = "s3://data-sink/active"
SRC = f"{A}/gtm_txn_events_slim/"
OUT = f"{A}/gtm_txn_recipient_month_rollup/"
PARAM_SET_ID = "v1"
BTREE = ["uei", "naics_code", "psc_code", "awarding_agency_code",
         "action_type_code", "month"]
GROUP_CEILING = 300_000_000

_PLAN_CLASS_SQL = """CASE
    WHEN subcontracting_plan = 'A' THEN 'A'
    WHEN subcontracting_plan = 'B' THEN 'B'
    WHEN subcontracting_plan IN ('C','D','E','F','G','H') THEN 'attached'
    ELSE NULL END"""


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

    src = lance.dataset(SRC, storage_options=opt)
    print(f"source {SRC} v{src.version} rows={src.count_rows():,}", flush=True)
    con.register("_ev", src.scanner(columns=[
        "uei", "action_type_code", "subcontracting_plan", "naics_code",
        "psc_code", "awarding_agency_code", "action_date", "obligation"]).to_reader())

    # distinct-group estimate BEFORE writing (grain guard). GROUP BY ALL over the
    # 7 grain dims; if it exceeds the ceiling, STOP and report — never silently
    # change the grain.
    con.execute(f"""CREATE TABLE roll AS
        SELECT uei,
               action_type_code,
               {_PLAN_CLASS_SQL} AS plan_class,
               naics_code,
               psc_code,
               awarding_agency_code,
               DATE_TRUNC('month', action_date) AS month,
               COUNT(*) AS n_actions,
               SUM(obligation) AS obligation_sum
        FROM _ev
        GROUP BY ALL""")
    n = con.execute("SELECT count(*) FROM roll").fetchone()[0]
    print(f"distinct-group rows: {n:,}", flush=True)
    if n > GROUP_CEILING:
        print(f"STOP: {n:,} groups exceeds ceiling {GROUP_CEILING:,}. "
              f"Grain change required (e.g. drop psc_code from grain) — not "
              f"changing silently. Aborting write.")
        return 2

    res = con.execute(f"""SELECT *, DATE '{as_of}' AS as_of,
        'gtm_txn_events_slim:v{src.version}' AS built_from_version,
        '{PARAM_SET_ID}' AS param_set_id FROM roll""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE], storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    """HYBRID EXACTNESS (headline fixture): the recipient set for
    {action_type_code='C', naics IN CONSTRUCTION_NAICS, last 90 days} computed as
        [whole months from Layer 2] ∪ [partial current month from Layer 1]
    must EQUAL the same set computed from Layer 1 alone.

    'last 90 days' = action_date >= today-90 (matching the compiler's <= 90 window
    on action_date). The whole-month boundary is the first of the CURRENT month:
    Layer 2 supplies closed months (month < first-of-current-month) intersected
    with the 90d floor at month granularity; Layer 1 supplies the current partial
    month AND any sub-month precision the rollup cannot express. To make the two
    sides provably equal we split at first-of-current-month:
        L2 side: months in [trunc(floor90), first_of_current_month)  (whole months)
        L1 side: actions in [first_of_current_month, today]           (partial)
    and floor both by action_date >= floor90 — the L2 side re-checks the floor on
    Layer 1 for the single boundary month that the 90d cut bisects."""
    opt = so()
    con = _cx()
    l1 = lance.dataset(SRC, storage_options=opt)
    l2 = lance.dataset(OUT, storage_options=opt)

    today = date.today()
    floor90 = (today - timedelta(days=90))
    first_of_current = today.replace(day=1)
    naics_list = "(" + ",".join(f"'{c}'" for c in CONSTRUCTION_NAICS) + ")"

    con.register("_l1", l1.scanner(
        columns=["uei", "action_type_code", "naics_code", "action_date"],
        filter=(f"action_type_code = 'C' AND action_date >= DATE '{floor90.isoformat()}'"
                f" AND naics_code IN {naics_list}")).to_reader())
    # truth: recipient set straight off Layer 1
    truth = {r[0] for r in con.execute("SELECT DISTINCT uei FROM _l1").fetchall()}

    # hybrid L1 side: partial current month (action-date exact, still >= floor90)
    l1_partial = {r[0] for r in con.execute(
        f"SELECT DISTINCT uei FROM _l1 WHERE action_date >= DATE '{first_of_current.isoformat()}'"
    ).fetchall()}

    # hybrid L2 side: whole closed months at month granularity. The boundary month
    # (the one floor90 lands in) is fetched from Layer 1 with the exact date floor
    # so no pre-floor action leaks in; all strictly-later closed months come from L2.
    boundary_month = floor90.replace(day=1)
    con.register("_l2", l2.scanner(
        columns=["uei", "action_type_code", "naics_code", "month"],
        filter=(f"action_type_code = 'C' AND month > DATE '{boundary_month.isoformat()}'"
                f" AND month < DATE '{first_of_current.isoformat()}'"
                f" AND naics_code IN {naics_list}")).to_reader())
    l2_whole = {r[0] for r in con.execute("SELECT DISTINCT uei FROM _l2").fetchall()}

    # boundary month off Layer 1 with the exact 90d floor (the month floor90 bisects)
    boundary = {r[0] for r in con.execute(
        f"SELECT DISTINCT uei FROM _l1 WHERE action_date >= DATE '{floor90.isoformat()}'"
        f" AND action_date < DATE '{(boundary_month.replace(day=28) + timedelta(days=4)).replace(day=1).isoformat()}'"
    ).fetchall()}

    hybrid = l1_partial | l2_whole | boundary
    if hybrid != truth:
        only_h = sorted(hybrid - truth)[:10]
        only_t = sorted(truth - hybrid)[:10]
        print(f"FAIL hybrid != truth: |hybrid|={len(hybrid)} |truth|={len(truth)} "
              f"hybrid_only={only_h} truth_only={only_t}")
        return 1
    print(f"verify OK: hybrid recipient set = truth for "
          f"{{action_type='C', construction NAICS, last 90d}} — "
          f"{len(truth):,} recipients; L2 whole-months ({len(l2_whole):,}) ∪ "
          f"L1 partial+boundary ({len(l1_partial | boundary):,}) = Layer-1 truth")
    return 0


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
