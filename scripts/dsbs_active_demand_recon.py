"""RECON — schema + cardinality ground-truth for the DSBS×Active-Demand overlap audit.

Read-only. Confirms exact live column names, dataset existence, and the key filter
cardinalities BEFORE the heavy build. No Lance writes.

    cd /Users/benjamincrane/core-x && doppler run -p core-x -c prd -- \
      .venv/bin/python .claude/worktrees/adoring-turing-bbe45f/scripts/dsbs_active_demand_recon.py
"""
from __future__ import annotations

import os
import sys

import duckdb
import lance

A = "s3://data-sink/active"


def so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("no R2 endpoint")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def open_ds(name):
    return lance.dataset(f"{A}/{name}/", storage_options=so())


def dump_schema(name, want_substr=None):
    print("=" * 78)
    print(f"DATASET  {name}")
    try:
        ds = open_ds(name)
    except Exception as e:  # noqa: BLE001
        print(f"  !! OPEN FAILED: {type(e).__name__}: {str(e).splitlines()[0][:160]}")
        return None
    fields = [(f.name, str(f.type)) for f in ds.schema]
    print(f"  rows={ds.count_rows():,}  cols={len(fields)}")
    for n, t in fields:
        if want_substr is None or any(w in n.lower() for w in want_substr):
            print(f"    {n:52s} {t}")
    return ds


def main() -> int:
    O = so()

    # 1) DEMAND — govcon_active_awards
    gaa = dump_schema("govcon_active_awards", want_substr=[
        "naics", "psc", "obligat", "value", "active", "subcontract", "recipient",
        "award_unique", "set_aside", "business_size", "pop_", "state"])
    if gaa is not None:
        con = duckdb.connect(); con.execute("SET memory_limit='8GB'")
        con.register("gaa", gaa)
        cols = set(gaa.schema.names)
        pot = "potential_total_value_of_award" if "potential_total_value_of_award" in cols else "NULL"
        cur = "current_total_value_of_award" if "current_total_value_of_award" in cols else "NULL"
        q = f"""
        SELECT
          count(*) total_rows,
          count(*) FILTER (WHERE coalesce(has_subcontracting_plan,FALSE)) has_subk,
          count(*) FILTER (WHERE coalesce(active_current,FALSE)) active_cur,
          count(*) FILTER (WHERE coalesce(active_potential,FALSE)) active_pot,
          count(*) FILTER (WHERE coalesce(has_subcontracting_plan,FALSE)
                             AND (coalesce(active_current,FALSE) OR coalesce(active_potential,FALSE))) demand_universe,
          count(DISTINCT (naics_code||'|'||psc_code)) FILTER (WHERE coalesce(has_subcontracting_plan,FALSE)
                             AND (coalesce(active_current,FALSE) OR coalesce(active_potential,FALSE))) demand_combos,
          round(sum(TRY_CAST(total_dollars_obligated AS DOUBLE)) FILTER (WHERE coalesce(has_subcontracting_plan,FALSE)
                             AND (coalesce(active_current,FALSE) OR coalesce(active_potential,FALSE)))/1e9,2) demand_obl_gb,
          round(sum(TRY_CAST({pot} AS DOUBLE)) FILTER (WHERE coalesce(has_subcontracting_plan,FALSE)
                             AND (coalesce(active_current,FALSE) OR coalesce(active_potential,FALSE)))/1e9,2) demand_pot_gb
        FROM gaa
        """
        r = con.execute(q).fetchdf()
        print("  -- DEMAND cardinalities --")
        print(r.T.to_string())
        # psc/naics null rate in demand
        print("  -- naics/psc null in demand universe --")
        print(con.execute("""SELECT
          count(*) FILTER (WHERE naics_code IS NULL OR trim(naics_code)='') naics_null,
          count(*) FILTER (WHERE psc_code IS NULL OR trim(psc_code)='') psc_null
          FROM gaa WHERE coalesce(has_subcontracting_plan,FALSE)
            AND (coalesce(active_current,FALSE) OR coalesce(active_potential,FALSE))""").fetchdf().T.to_string())
        con.close()

    # 2) SUPPLY roster — sba_dsbs_certified_firms
    dsbs = dump_schema("sba_dsbs_certified_firms", want_substr=[
        "uei", "cage", "naics", "cert", "active_", "state", "legal", "name", "zip", "county"])
    if dsbs is not None:
        con = duckdb.connect(); con.register("d", dsbs)
        print("  -- DSBS roster cardinalities --")
        print(con.execute("""SELECT count(*) n_rows, count(DISTINCT upper(trim(uei))) distinct_uei,
          count(*) FILTER (WHERE uei IS NULL OR trim(uei)='') uei_null,
          count(*) FILTER (WHERE active_8a_boolean) a8a,
          count(*) FILTER (WHERE active_hz_boolean) ahz,
          count(*) FILTER (WHERE active_wosb_boolean) awosb,
          count(*) FILTER (WHERE active_sdvosb_boolean) asdvosb,
          count(*) FILTER (WHERE active_vosb_boolean) avosb
          FROM d""").fetchdf().T.to_string())
        con.close()

    # 3) SUPPLY proven-as-sub — subaward_naics_psc_wide (fallback narrow)
    sub = dump_schema("subaward_naics_psc_wide", want_substr=[
        "uei", "naics", "psc", "amount", "prime", "sub"])
    if sub is None:
        sub = dump_schema("subaward_naics_psc", want_substr=["uei", "naics", "psc", "amount", "prime", "sub"])

    # 4) SUPPLY proven-as-prime — canonical spine (metadata only; the 108M scan happens in the build)
    dump_schema("usaspending_fpds_canonical_txn", want_substr=[
        "recipient_uei", "naics_code", "product_or_service_code"])

    print("=" * 78)
    print("RECON COMPLETE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
