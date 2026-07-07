#!/usr/bin/env python3
"""gtm_subaward_prime_mod_events / gtm_subaward_prime_signal — prime-mod signal per subawardee.

SoR  s3://data-sink/active/gtm_subaward_prime_mod_events/  (sub x prime-mod event grain)
     s3://data-sink/active/gtm_subaward_prime_signal/      (subawardee rollup, 1 row/uei)

THE SIGNAL (operator-authorized build 2026-07-06): "a prime award that subawardee X sits
under just took a modification" — surfaced days after FPDS reports it, months before the
sub's own FSRS paperwork. Cold-call fact per event: which sub, which prime, the action
type (canonical description from dec_code_domain_ref, element=ActionType — NOT an
invented taxonomy), the obligation/total-value deltas, and the award's NAICS x PSC.

DESIGN
  * ALL action-type codes carried; "sharp" filtering (G option / A additional work /
    H,L definitization) happens at query time — adding a code never needs a rebuild.
  * Dates stored as facts. Recency bands are computed at query time against
    CURRENT_DATE (BTREE on action_date makes the range scan cheap). Never bake
    day-counts into rows.
  * is_after_sub_last precomputed: mod dated after the sub's most recent subaward on
    that vehicle ("before they knew" — nearly always true given FSRS lag; kept anyway).
  * Universe: subawardees active in the trailing 24mo; mods over the same window.
    FRESHNESS-CRITICAL: FPDS reports on a ~10-day lag and backfills — re-run weekly
    (the first audience mart with a cadence rather than a one-shot).

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_subaward_prime_signal.py [--verify]
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
OUT_EVENTS = f"{A}/gtm_subaward_prime_mod_events/"
OUT_SIGNAL = f"{A}/gtm_subaward_prime_signal/"
PARAM_SET_ID = "v1"
SHARP = "('G', 'A', 'H', 'L')"  # query-time convention, also used for the rollup


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def build() -> int:
    opt = so()
    as_of = date.today().isoformat()
    w24 = (date.today() - timedelta(days=730)).isoformat()
    con = duckdb.connect()
    con.execute("SET memory_limit='18GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")

    # sub <-> prime edges (24mo universe) + sub's last subaward per vehicle + prime identity
    se = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    con.register("_se", se.scanner(
        columns=["subawardee_uei", "prime_award_unique_key", "subaward_action_date",
                 "prime_awardee_uei", "prime_awardee_name"],
        filter=f"subaward_action_date >= DATE '{w24}' AND subawardee_uei IS NOT NULL").to_reader())
    con.execute("""CREATE TABLE edge AS
        SELECT subawardee_uei, prime_award_unique_key,
               MAX(subaward_action_date) AS sub_last_action_date,
               any_value(prime_awardee_uei) AS prime_awardee_uei,
               any_value(prime_awardee_name) AS prime_awardee_name
        FROM _se WHERE prime_award_unique_key IS NOT NULL
        GROUP BY 1, 2""")
    ne = con.execute("SELECT COUNT(*) FROM edge").fetchone()[0]
    print(f"edges: {ne:,}", flush=True)

    # mods on those primes (all codes, 24mo)
    md = lance.dataset(f"{A}/usaspending_fpds_mod_delta/", storage_options=opt)
    con.register("_md", md.scanner(
        columns=["contract_award_unique_key", "contract_transaction_unique_key",
                 "action_type_code", "action_date", "modification_number",
                 "delta_federal_action_obligation", "delta_current_total_value_of_award",
                 "delta_potential_ceiling", "is_scope_increase", "is_termination_event"],
        filter=f"action_date >= DATE '{w24}' AND action_type_code IS NOT NULL").to_reader())
    con.execute("""CREATE TABLE mods AS
        SELECT m.* FROM _md m
        SEMI JOIN (SELECT DISTINCT prime_award_unique_key FROM edge) e
          ON e.prime_award_unique_key = m.contract_award_unique_key""")
    nm = con.execute("SELECT COUNT(*) FROM mods").fetchone()[0]
    print(f"on-universe mods: {nm:,}", flush=True)

    # award -> combo (verbatim codes + descriptions from the txn canonical)
    tx = lance.dataset(f"{A}/usaspending_fpds_canonical_txn/", storage_options=opt)
    con.register("_tx", tx.scanner(
        columns=["contract_award_unique_key", "naics_code", "product_or_service_code",
                 "naics_description", "product_or_service_code_description"]).to_reader())
    con.execute("""CREATE TABLE combo AS
        SELECT contract_award_unique_key,
               any_value(naics_code) AS naics_code,
               any_value(product_or_service_code) AS psc_code,
               any_value(naics_description) AS naics_title,
               any_value(product_or_service_code_description) AS psc_title
        FROM _tx
        SEMI JOIN (SELECT DISTINCT prime_award_unique_key FROM edge) e
          ON e.prime_award_unique_key = _tx.contract_award_unique_key
        GROUP BY 1""")

    # canonical action-type descriptions (dec_code_domain_ref, element=ActionType)
    ref = lance.dataset(f"{A}/dec_code_domain_ref/", storage_options=opt)
    con.register("_ref", ref.scanner(
        columns=["element", "code", "description"],
        filter="element = 'ActionType'").to_reader())
    con.execute("""CREATE TABLE atref AS
        SELECT code, any_value(description) AS action_type_description
        FROM _ref GROUP BY 1""")

    # events: sub x prime-mod
    con.execute("""CREATE TABLE events AS
        SELECT e.subawardee_uei, e.prime_award_unique_key,
               e.prime_awardee_uei, e.prime_awardee_name,
               m.contract_transaction_unique_key, m.modification_number,
               m.action_type_code, r.action_type_description,
               m.action_date,
               m.delta_federal_action_obligation, m.delta_current_total_value_of_award,
               m.delta_potential_ceiling, m.is_scope_increase, m.is_termination_event,
               c.naics_code, c.naics_title, c.psc_code, c.psc_title,
               e.sub_last_action_date,
               (m.action_date > e.sub_last_action_date) AS is_after_sub_last
        FROM edge e
        JOIN mods m ON m.contract_award_unique_key = e.prime_award_unique_key
        LEFT JOIN combo c ON c.contract_award_unique_key = e.prime_award_unique_key
        LEFT JOIN atref r ON r.code = m.action_type_code""")
    nev = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    sharp = con.execute(f"SELECT COUNT(*) FROM events WHERE action_type_code IN {SHARP}").fetchone()[0]
    print(f"events: {nev:,} (sharp {sharp:,})", flush=True)

    # rollup: 1 row per subawardee — freshest sharp event + overall recency
    con.execute(f"""CREATE TABLE signal AS
        WITH sharp AS (
            SELECT subawardee_uei,
                   arg_max(action_type_code, action_date) AS freshest_sharp_code,
                   arg_max(action_type_description, action_date) AS freshest_sharp_description,
                   MAX(action_date) AS freshest_sharp_date,
                   arg_max(delta_federal_action_obligation, action_date) AS freshest_sharp_obligation,
                   arg_max(prime_awardee_name, action_date) AS freshest_sharp_prime_name,
                   arg_max(prime_award_unique_key, action_date) AS freshest_sharp_prime_award,
                   arg_max(naics_code, action_date) AS freshest_sharp_naics,
                   arg_max(psc_code, action_date) AS freshest_sharp_psc,
                   COUNT(*) AS n_sharp_events_24mo
            FROM events WHERE action_type_code IN {SHARP}
            GROUP BY 1),
        anymod AS (
            SELECT subawardee_uei, MAX(action_date) AS most_recent_mod_date,
                   COUNT(*) AS n_mod_events_24mo
            FROM events GROUP BY 1)
        SELECT a.subawardee_uei, a.most_recent_mod_date, a.n_mod_events_24mo,
               s.freshest_sharp_code, s.freshest_sharp_description, s.freshest_sharp_date,
               s.freshest_sharp_obligation, s.freshest_sharp_prime_name,
               s.freshest_sharp_prime_award, s.freshest_sharp_naics, s.freshest_sharp_psc,
               coalesce(s.n_sharp_events_24mo, 0) AS n_sharp_events_24mo
        FROM anymod a LEFT JOIN sharp s USING (subawardee_uei)""")
    nsig = con.execute("SELECT COUNT(*) FROM signal").fetchone()[0]
    dup = con.execute("""SELECT COUNT(*) FROM (
        SELECT subawardee_uei FROM signal GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup == 0, f"signal grain not unique: {dup} dups"
    print(f"signal rollup: {nsig:,} subawardees", flush=True)

    built_from = (f"usaspending_subaward_canonical:v{se.version}|usaspending_fpds_mod_delta:v{md.version}|"
                  f"usaspending_fpds_canonical_txn:v{tx.version}|dec_code_domain_ref:v{ref.version}")
    for table, uri, btree in (
        ("events", OUT_EVENTS,
         ["subawardee_uei", "action_type_code", "prime_award_unique_key", "action_date"]),
        ("signal", OUT_SIGNAL, ["subawardee_uei"]),
    ):
        res = con.execute(f"""SELECT *, DATE '{as_of}' AS as_of,
            '{built_from}' AS built_from_version, '{PARAM_SET_ID}' AS param_set_id FROM {table}""")
        reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
        ds = write_indexed_dataset(reader, uri, [(c, "BTREE") for c in btree], storage_options=opt)
        print(f"wrote {uri}  v{ds.version}  rows={ds.count_rows():,}  "
              f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    opt = so()
    ok = True
    ev = lance.dataset(OUT_EVENTS, storage_options=opt)
    rows = ev.count_rows()
    sharp45 = ev.count_rows(
        filter=f"action_type_code IN {SHARP} AND action_date >= DATE '{(date.today() - timedelta(days=45)).isoformat()}'")
    idx = [i["name"] for i in ev.list_indices()]
    good = rows > 500_000 and sharp45 > 500 and "subawardee_uei_idx" in idx and "action_date_idx" in idx
    ok &= good
    print(f"{OUT_EVENTS}: rows={rows:,} sharp_last45d={sharp45:,} indices={idx} -> {'OK' if good else 'FAIL'}", flush=True)
    sg = lance.dataset(OUT_SIGNAL, storage_options=opt)
    nsig = sg.count_rows()
    with_sharp = sg.count_rows(filter="freshest_sharp_code IS NOT NULL")
    good = nsig > 20_000 and 0 < with_sharp < nsig
    ok &= good
    print(f"{OUT_SIGNAL}: rows={nsig:,} with_sharp={with_sharp:,} -> {'OK' if good else 'FAIL'}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
