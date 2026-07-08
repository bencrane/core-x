#!/usr/bin/env python3
"""gtm_prime_pop_lanes — the prime WIN-SIDE place-of-performance rollup
(state + county grain), the geo dimension's audience-side substrate.

SoR  s3://data-sink/active/gtm_prime_pop_lanes/
     (grain: uei × pop_state × pop_county_fips; pop_county_name,
      n_actions_24mo/60mo, obligation_24mo/60mo, last_action_date; Lance;
      snapshot-overwrite; BTREE on uei / pop_state / pop_county_fips)

WHY (freeze §0.1 / addendum §4.2, operator-directed county grain)
Input 1 (Geographic Focus) needs BOTH sides of the footprint. The target's
sub-side footprint bakes into the targets row at pair build. The AUDIENCE side —
"primes doing work in/near my geography" — existed in no mart: win-side PoP per
prime lives only on the 108M-row txn spine. This rollup closes that gap for
S1/S3 ("primes winning work in ⟨county/state⟩") as an indexed exact-match.

Source: usaspending_fpds_canonical_txn directly — gtm_txn_events_slim carries NO
geo columns by design (closed-grammar projection); adding geo there would widen
the 108M-row hot mart for a dimension this compact rollup serves better.

GRAIN + NULLS. Trailing 60mo window from as_of (the geo dimension is a
recent-work fact, not an archaeology surface; window is a build parameter).
Rows with a NULL/blank state are excluded (no geo fact — null ≠ zero, absence
disclosed by absence). County may be NULL where the spine discloses state-only:
the state-grain fact is real and rides with pop_county_fips NULL — never a
fabricated county. n_actions_24mo/obligation_24mo are the 24mo cut of the same
window. obligation_* is Σ federal_action_obligation VERBATIM (NET; negatives —
de-obligations — pass through unclamped).

    doppler run --project core-x --config prd -- \
      /Users/benjamincrane/core-x/.venv/bin/python \
      scripts/build_gtm_prime_pop_lanes.py [--verify]
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
SRC = f"{A}/usaspending_fpds_canonical_txn/"
OUT = f"{A}/gtm_prime_pop_lanes/"
PARAM_SET_ID = "v1"
BTREE = ["uei", "pop_state", "pop_county_fips"]

TXN_COLS = [
    "recipient_uei",
    "action_date",
    "federal_action_obligation",
    "primary_place_of_performance_state_code",
    "pop_county_fips",
    "pop_county_name",
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
    as_of = date.today()
    w60 = (as_of - timedelta(days=1826)).isoformat()
    w24 = (as_of - timedelta(days=730)).isoformat()
    con = _cx()

    tx = lance.dataset(SRC, storage_options=opt)
    print(f"source {SRC} v{tx.version} rows={tx.count_rows():,}  "
          f"window >= {w60} (24mo cut >= {w24})", flush=True)
    con.register("_tx", tx.scanner(
        columns=TXN_COLS,
        filter=f"recipient_uei IS NOT NULL AND action_date >= DATE '{w60}'").to_reader())

    # state normalized (trim, upper); blank state = no geo fact = excluded;
    # county rides as disclosed (NULL when the spine is state-only).
    con.execute("""CREATE TABLE base AS
        SELECT recipient_uei AS uei,
               UPPER(TRIM(primary_place_of_performance_state_code)) AS pop_state,
               NULLIF(TRIM(pop_county_fips), '') AS pop_county_fips,
               NULLIF(TRIM(pop_county_name), '') AS pop_county_name,
               action_date,
               federal_action_obligation AS obligation
        FROM _tx
        WHERE COALESCE(TRIM(primary_place_of_performance_state_code), '') <> ''""")

    con.execute(f"""CREATE TABLE roll AS
        SELECT uei, pop_state, pop_county_fips,
               MAX(pop_county_name) AS pop_county_name,
               COUNT(*) AS n_actions_60mo,
               SUM(obligation) AS obligation_60mo,
               COUNT(*) FILTER (action_date >= DATE '{w24}') AS n_actions_24mo,
               SUM(obligation) FILTER (action_date >= DATE '{w24}') AS obligation_24mo,
               MAX(action_date) AS last_action_date
        FROM base GROUP BY 1, 2, 3""")
    n = con.execute("SELECT count(*) FROM roll").fetchone()[0]
    print(f"rollup groups: {n:,}", flush=True)

    res = con.execute(f"""SELECT *, DATE '{as_of.isoformat()}' AS as_of,
        'usaspending_fpds_canonical_txn:v{tx.version}' AS built_from_version,
        '{PARAM_SET_ID}' AS param_set_id FROM roll""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE], storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    """For one heavy prime: per-(state,county) 60mo action counts + $ off the mart
    equal a direct spine filter over the same window; the 24mo cut reconciles too."""
    opt = so()
    mart = lance.dataset(OUT, storage_options=opt)
    tx = lance.dataset(SRC, storage_options=opt)
    meta = mart.scanner(columns=["as_of"], limit=1).to_table().to_pylist()[0]
    as_of = date.fromisoformat(str(meta["as_of"])[:10])
    w60 = (as_of - timedelta(days=1826)).isoformat()
    w24 = (as_of - timedelta(days=730)).isoformat()

    con = _cx()
    con.register("_m", mart.scanner(columns=["uei", "n_actions_60mo"]).to_reader())
    probe = con.execute(
        "SELECT uei FROM _m GROUP BY 1 ORDER BY sum(n_actions_60mo) DESC LIMIT 1"
    ).fetchone()[0]
    print(f"probe uei={probe}  as_of={as_of}  window >= {w60}", flush=True)

    mm = mart.scanner(
        columns=["pop_state", "pop_county_fips", "n_actions_60mo", "obligation_60mo",
                 "n_actions_24mo", "obligation_24mo"],
        filter=f"uei = '{probe}'").to_table().to_pylist()
    mart_side = {(r["pop_state"], r["pop_county_fips"]):
                 (r["n_actions_60mo"], round(float(r["obligation_60mo"] or 0), 2),
                  r["n_actions_24mo"], round(float(r["obligation_24mo"] or 0), 2))
                 for r in mm}

    raw = tx.scanner(
        columns=TXN_COLS,
        filter=f"recipient_uei = '{probe}' AND action_date >= DATE '{w60}'"
    ).to_table().to_pylist()
    raw_side: dict = {}
    for r in raw:
        st = (r["primary_place_of_performance_state_code"] or "").strip().upper()
        if not st:
            continue
        cf = (r["pop_county_fips"] or "").strip() or None
        k = (st, cf)
        n60, o60, n24, o24 = raw_side.get(k, (0, 0.0, 0, 0.0))
        amt = float(r["federal_action_obligation"] or 0)
        in24 = str(r["action_date"])[:10] >= w24
        raw_side[k] = (n60 + 1, o60 + amt, n24 + (1 if in24 else 0), o24 + (amt if in24 else 0.0))
    raw_side = {k: (n60, round(o60, 2), n24, round(o24, 2))
                for k, (n60, o60, n24, o24) in raw_side.items()}

    if mart_side != raw_side:
        miss = {k: (mart_side.get(k), raw_side.get(k))
                for k in set(mart_side) | set(raw_side)
                if mart_side.get(k) != raw_side.get(k)}
        print(f"FAIL reconcile ({len(miss)} (state,county) cells differ): "
              f"{dict(list(miss.items())[:5])}")
        return 1
    print(f"verify OK: {probe} — {len(mart_side)} (state,county) cells reconcile "
          f"exactly vs spine (60mo counts/$ + 24mo cut)")
    return 0


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
