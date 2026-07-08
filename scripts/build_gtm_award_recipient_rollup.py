#!/usr/bin/env python3
"""gtm_award_recipient_rollup (+ gtm_award_expiry_months sidecar) — the award lane
precompute off usaspending_fpds_prime_award_state.

SoR  s3://data-sink/active/gtm_award_recipient_rollup/
     (grain: uei × naics_code × psc_code × awarding_agency_code × award_topology;
      n_awards_lifetime, obligated_lifetime, n_active, obligated_active; Lance;
      snapshot-overwrite; BTREE on uei / naics_code / psc_code /
      awarding_agency_code / award_topology)
     s3://data-sink/active/gtm_award_expiry_months/
     (sidecar grain: uei × end_month; n_awards, obligated — rows with
      current_end_date >= as_of only; BTREE on uei / end_month)

WHY
The phrase-query award lane (apps/catalyst_api/src/phrase_compiler.py) collapses
recipients over the 82.9M-row usaspending_fpds_prime_award_state at request time —
the 'active' posture and the 'expiring within N days' window both scan it live.
This mart pre-aggregates the collapse (main rollup) and the expiry window (sidecar)
so the request path reads compact rollups keyed by the grammar's filter dims.

ACTIVE (as_of-materialized): is_terminated = false AND current_end_date >= as_of.
The compiler's request-time 'current_end_date >= 0' is a relative-days convention;
the mart materializes it against the build's as_of DATE. is_terminated is nullable
bool — the spec fixes active as is_terminated = false (a null termination flag is
NOT counted active; unknown ≠ active).

DOLLARS. obligated_* is Σ life_to_date_obligated VERBATIM (NET life-to-date).

Sidecar carries ONLY forward-looking awards (current_end_date >= as_of): the
expiring-within-N window is a forward cut, so past end-dates never enter it.
end_month = DATE_TRUNC('month', current_end_date).

    doppler run --project core-x --config prd -- \
      /Users/benjamincrane/core-x/.venv/bin/python \
      scripts/build_gtm_award_recipient_rollup.py [--verify]
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
SRC = f"{A}/usaspending_fpds_prime_award_state/"
OUT = f"{A}/gtm_award_recipient_rollup/"
OUT_EXPIRY = f"{A}/gtm_award_expiry_months/"
PARAM_SET_ID = "v1"
BTREE = ["uei", "naics_code", "psc_code", "awarding_agency_code", "award_topology"]
BTREE_EXPIRY = ["uei", "end_month"]

AWARD_COLS = [
    "contract_award_unique_key",
    "recipient_uei",
    "naics_code",
    "product_or_service_code",
    "awarding_agency_code",
    "award_topology",
    "life_to_date_obligated",
    "is_terminated",
    "current_end_date",
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

    aw = lance.dataset(SRC, storage_options=opt)
    print(f"source {SRC} v{aw.version} rows={aw.count_rows():,}", flush=True)
    con.register("_aw", aw.scanner(columns=AWARD_COLS,
                                   filter="recipient_uei IS NOT NULL").to_reader())
    con.execute(f"""CREATE TABLE base AS
        SELECT recipient_uei AS uei,
               naics_code,
               product_or_service_code AS psc_code,
               awarding_agency_code,
               award_topology,
               life_to_date_obligated,
               current_end_date,
               (is_terminated = false AND current_end_date >= DATE '{as_of}') AS is_active
        FROM _aw""")

    # ── main rollup ──
    con.execute("""CREATE TABLE roll AS
        SELECT uei, naics_code, psc_code, awarding_agency_code, award_topology,
               COUNT(*) AS n_awards_lifetime,
               SUM(life_to_date_obligated) AS obligated_lifetime,
               COUNT(*) FILTER (is_active) AS n_active,
               SUM(life_to_date_obligated) FILTER (is_active) AS obligated_active
        FROM base GROUP BY ALL""")
    n = con.execute("SELECT count(*) FROM roll").fetchone()[0]
    print(f"main rollup groups: {n:,}", flush=True)

    res = con.execute(f"""SELECT *, DATE '{as_of}' AS as_of,
        'usaspending_fpds_prime_award_state:v{aw.version}' AS built_from_version,
        '{PARAM_SET_ID}' AS param_set_id FROM roll""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE], storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)

    # ── expiry sidecar (forward-looking awards only) ──
    con.execute(f"""CREATE TABLE expiry AS
        SELECT uei,
               DATE_TRUNC('month', current_end_date) AS end_month,
               COUNT(*) AS n_awards,
               SUM(life_to_date_obligated) AS obligated
        FROM base
        WHERE current_end_date >= DATE '{as_of}'
        GROUP BY ALL""")
    ne = con.execute("SELECT count(*) FROM expiry").fetchone()[0]
    print(f"expiry sidecar groups: {ne:,}", flush=True)

    res2 = con.execute(f"""SELECT *, DATE '{as_of}' AS as_of,
        'usaspending_fpds_prime_award_state:v{aw.version}' AS built_from_version,
        '{PARAM_SET_ID}' AS param_set_id FROM expiry""")
    reader2 = res2.to_arrow_reader(65536) if hasattr(res2, "to_arrow_reader") else res2.fetch_record_batch(65536)
    ds2 = write_indexed_dataset(reader2, OUT_EXPIRY, [(c, "BTREE") for c in BTREE_EXPIRY], storage_options=opt)
    print(f"wrote {OUT_EXPIRY}  v{ds2.version}  rows={ds2.count_rows():,}  "
          f"indices={[i['name'] for i in ds2.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    """For one heavy recipient: n_active + active $ from the rollup equal a direct
    award-state filter (is_terminated=false AND current_end_date>=as_of); expiry-
    month sums reconcile against the same as_of forward cut off the spine."""
    opt = so()
    con = _cx()
    roll = lance.dataset(OUT, storage_options=opt)
    exp = lance.dataset(OUT_EXPIRY, storage_options=opt)
    aw = lance.dataset(SRC, storage_options=opt)
    as_of = str(roll.scanner(columns=["as_of"], limit=1).to_table().to_pylist()[0]["as_of"])

    # heaviest recipient by lifetime award count in the rollup
    con.register("_r", roll.scanner(columns=["uei", "n_awards_lifetime"]).to_reader())
    probe = con.execute(
        "SELECT uei FROM _r WHERE uei IS NOT NULL GROUP BY 1 "
        "ORDER BY sum(n_awards_lifetime) DESC LIMIT 1").fetchone()[0]
    print(f"probe uei={probe}  as_of={as_of}", flush=True)

    # rollup side
    rr = roll.scanner(columns=["n_active", "obligated_active", "n_awards_lifetime",
                               "obligated_lifetime"],
                      filter=f"uei = '{probe}'").to_table().to_pylist()
    m_active_n = sum(r["n_active"] or 0 for r in rr)
    m_active_o = sum(float(r["obligated_active"] or 0) for r in rr)
    m_life_n = sum(r["n_awards_lifetime"] or 0 for r in rr)

    # direct spine side
    raw = aw.scanner(columns=["life_to_date_obligated", "is_terminated", "current_end_date"],
                     filter=f"recipient_uei = '{probe}'").to_table().to_pylist()
    r_life_n = len(raw)
    active = [r for r in raw if r["is_terminated"] is False
              and r["current_end_date"] is not None and str(r["current_end_date"]) >= as_of]
    r_active_n = len(active)
    r_active_o = sum(float(r["life_to_date_obligated"] or 0) for r in active)

    if m_life_n != r_life_n:
        print(f"FAIL lifetime count: rollup {m_life_n} vs spine {r_life_n}")
        return 1
    if m_active_n != r_active_n:
        print(f"FAIL active count: rollup {m_active_n} vs spine {r_active_n}")
        return 1
    if abs(m_active_o - r_active_o) > 0.01:
        print(f"FAIL active $: rollup {m_active_o:,.2f} vs spine {r_active_o:,.2f}")
        return 1

    # expiry sidecar reconcile: per-end_month n_awards + $ off the sidecar equal
    # the as_of forward cut off the spine (grouped by first-of-end-month)
    ee = exp.scanner(columns=["end_month", "n_awards", "obligated"],
                     filter=f"uei = '{probe}'").to_table().to_pylist()
    s_side = {str(r["end_month"]): (r["n_awards"], round(float(r["obligated"] or 0), 2)) for r in ee}
    fwd = [r for r in raw if r["current_end_date"] is not None
           and str(r["current_end_date"]) >= as_of]
    raw_side: dict = {}
    for r in fwd:
        m = str(r["current_end_date"])[:7] + "-01"
        c, s = raw_side.get(m, (0, 0.0))
        raw_side[m] = (c + 1, s + float(r["life_to_date_obligated"] or 0))
    raw_side = {k: (c, round(s, 2)) for k, (c, s) in raw_side.items()}
    if s_side != raw_side:
        miss = {k: (s_side.get(k), raw_side.get(k)) for k in set(s_side) | set(raw_side)
                if s_side.get(k) != raw_side.get(k)}
        print(f"FAIL expiry reconcile ({len(miss)} months differ): "
              f"{dict(list(miss.items())[:5])}")
        return 1
    print(f"verify OK: {probe} — lifetime={r_life_n}, active n={r_active_n} "
          f"${r_active_o:,.2f} exact vs spine; expiry sidecar {len(s_side)} "
          f"forward months reconcile exactly")
    return 0


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
