#!/usr/bin/env python3
"""gtm_prime_farmout_combo_lanes — what each prime subs out, by combo of its own awards.

SoR  s3://data-sink/active/gtm_prime_farmout_combo_lanes/
     (uei × naics_code × psc_code, windowed subaward $ ISSUED; Lance; derived,
      snapshot-overwrite; BTREE on uei/naics_code/psc_code)

WHY
The buyer-side mirror of gtm_sub_combo_lanes. That mart answers "what combos does
this SUB deliver under"; this one answers "what combos does this PRIME hire subs
into" — the demand direction. Anchor→lookalike-prime→farm-out pipelines (operator
session 2026-07-06) computed this per-UEI from usaspending_subaward_canonical at
query time; this materializes the same fact so the chain is indexed point-lookups
end to end. gtm_prime_subout_by_recipient_code does NOT serve this: its grain is
(prime × single context code × single recipient code) — summing it double-counts
per recipient-code axis, and it carries no joint NAICS×PSC combo.

COMBO KEY. naics_code × psc_code taken together from the PRIME award the subaward
rides under — the combo doctrine everywhere else. Empirically ~82% of these prime
awards are task/delivery orders, so the combo is stamped at delivered-work grain.
NULL codes are retained; combo filters simply don't match them.

FULL GRAIN, NO PRUNING (operator-directed 2026-07-06): no support floors, no
award-type filter (prime_award_type is FSRS-side context, not a gate) — filters
and weighting are expressed at query time.

WINDOWS are rolling, anchored at build-time as_of (12/24/60mo + lifetime) — same
staleness model as gtm_audience_marts; rerun to refresh.

DOLLARS. Farm-out amounts are NET obligated — subaward_amount lines are summed
verbatim, de-obligations/corrections (negative lines) included — NOT gross.
Chunk medians/quantiles likewise run over the raw signed line amounts.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_prime_farmout_combo_lanes.py [--verify]
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
OUT = f"{A}/gtm_prime_farmout_combo_lanes/"
PARAM_SET_ID = "v2"  # v2: median/p25/p75 chunk columns (60mo + lifetime) — the MVS
                     # floor-filter comparator (operator-directed 2026-07-06); means
                     # lie under skew, gates compare against per-combo medians.
BTREE = ["uei", "naics_code", "psc_code"]


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

    nref = lance.dataset(f"{A}/naics_reference/", storage_options=opt)
    con.register("_nref", nref.scanner(columns=["naics_code", "naics_title"]).to_reader())
    con.execute("""CREATE TABLE nref AS
        SELECT naics_code, any_value(naics_title) AS naics_title FROM _nref GROUP BY 1""")
    pref = lance.dataset(f"{A}/psc_reference/", storage_options=opt)
    con.register("_pref", pref.scanner(
        columns=["psc_code", "psc_name", "is_active"]).to_reader())
    con.execute("""CREATE TABLE pref AS
        SELECT psc_code, arg_max(psc_name, is_active::INT) AS psc_title
        FROM _pref WHERE psc_name IS NOT NULL GROUP BY 1""")

    se = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    con.register("_se", se.scanner(
        columns=["prime_awardee_uei", "subawardee_uei", "subaward_amount",
                 "subaward_action_date", "prime_award_naics_code",
                 "prime_award_product_or_service_code"],
        filter="prime_awardee_uei IS NOT NULL").to_reader())
    con.execute(f"""CREATE TABLE farmout AS
        SELECT prime_awardee_uei AS uei,
               prime_award_naics_code AS naics_code,
               prime_award_product_or_service_code AS psc_code,
               SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w12}') AS farmout_amt_12mo,
               SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w24}') AS farmout_amt_24mo,
               SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w60}') AS farmout_amt_60mo,
               SUM(subaward_amount) AS farmout_amt_lifetime,
               COUNT(*) FILTER (subaward_action_date >= DATE '{w24}') AS n_subawards_24mo,
               COUNT(*) AS n_subawards_lifetime,
               COUNT(DISTINCT subawardee_uei) FILTER (subaward_action_date >= DATE '{w60}') AS n_distinct_subs_60mo,
               COUNT(DISTINCT subawardee_uei) AS n_distinct_subs_lifetime,
               MEDIAN(subaward_amount) FILTER (subaward_action_date >= DATE '{w60}') AS median_chunk_60mo,
               QUANTILE_CONT(subaward_amount, 0.25) FILTER (subaward_action_date >= DATE '{w60}') AS p25_chunk_60mo,
               QUANTILE_CONT(subaward_amount, 0.75) FILTER (subaward_action_date >= DATE '{w60}') AS p75_chunk_60mo,
               MEDIAN(subaward_amount) AS median_chunk_lifetime,
               QUANTILE_CONT(subaward_amount, 0.25) AS p25_chunk_lifetime,
               QUANTILE_CONT(subaward_amount, 0.75) AS p75_chunk_lifetime,
               MIN(subaward_action_date) AS first_action_date,
               MAX(subaward_action_date) AS last_action_date
        FROM _se GROUP BY 1, 2, 3""")
    con.execute("""CREATE TABLE farmout_t AS
        SELECT l.*, n.naics_title, p.psc_title
        FROM farmout l
        LEFT JOIN nref n USING (naics_code)
        LEFT JOIN pref p USING (psc_code)""")
    con.execute("DROP TABLE farmout"); con.execute("ALTER TABLE farmout_t RENAME TO farmout")
    n = con.execute("SELECT COUNT(*) FROM farmout").fetchone()[0]
    print(f"farmout lanes: {n:,} rows", flush=True)

    res = con.execute(f"""SELECT *, DATE '{as_of}' AS as_of,
        'usaspending_subaward_canonical:v{se.version}' AS built_from_version,
        '{PARAM_SET_ID}' AS param_set_id FROM farmout""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE], storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    """Spot-verify the mart against a direct spine group-by for one heavy prime."""
    opt = so()
    probe = "YA63J5PVEZE6"  # TORCH TECHNOLOGIES — heavy sub-out engine
    ds = lance.dataset(OUT, storage_options=opt)
    mart = ds.scanner(filter=f"uei = '{probe}'").to_table().to_pylist()
    se = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    raw = se.scanner(filter=f"prime_awardee_uei = '{probe}'",
                     columns=["prime_award_naics_code", "prime_award_product_or_service_code",
                              "subaward_amount"]).to_table().to_pylist()
    agg: dict = {}
    for r in raw:
        k = (r["prime_award_naics_code"], r["prime_award_product_or_service_code"])
        agg[k] = agg.get(k, 0.0) + float(r["subaward_amount"] or 0)
    mart_map = {(r["naics_code"], r["psc_code"]): float(r["farmout_amt_lifetime"] or 0) for r in mart}
    med_map = {(r["naics_code"], r["psc_code"]): r["median_chunk_lifetime"] for r in mart}
    if set(agg) != set(mart_map):
        print(f"FAIL combo sets differ: raw={len(agg)} mart={len(mart_map)}")
        return 1
    for k, v in agg.items():
        if abs(v - mart_map[k]) > 0.01:
            print(f"FAIL $ mismatch {k}: raw {v:,.2f} vs mart {mart_map[k]:,.2f}")
            return 1
    import statistics
    med_raw: dict = {}
    for r in raw:
        k = (r["prime_award_naics_code"], r["prime_award_product_or_service_code"])
        med_raw.setdefault(k, []).append(float(r["subaward_amount"] or 0))
    for k, vals in med_raw.items():
        m = med_map.get(k)
        if m is None or abs(statistics.median(vals) - float(m)) > 0.01:
            print(f"FAIL median mismatch {k}: raw {statistics.median(vals):,.2f} vs mart {m}")
            return 1
    print(f"verify OK: {probe} {len(agg)} combos, lifetime $ + medians exact match")
    return 0


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
