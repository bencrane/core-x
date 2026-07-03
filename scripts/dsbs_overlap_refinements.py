"""Critic-driven refinements to the DSBS×active-demand audit. READ-ONLY, no Lance writes.

Addresses verifier/critic P0-P1 findings:
  1. CORRECTLY-SCOPED sub-dollar sizing — realized FSRS subaward_amount (2021+), NOT the prime pool:
       (a) sub $ flowing UNDER the active demand primes (join prime_award_unique_key = cauk),
       (b) DSBS-captured share vs displaceable-to-DSBS share.
  2. SUB-PROOF-RESTRICTED overlap — addressability by proven-as-SUB evidence only (the on-point GTM motion).
  3. SYMMETRIC-WINDOW (2021+) sensitivity — re-scan spine with action_date>=2021 so both supply
       sides share the FSRS window; re-measure obligated-coverage.

    cd /Users/benjamincrane/core-x && doppler run -p core-x -c prd -- \
      .venv/bin/python .claude/worktrees/adoring-turing-bbe45f/scripts/dsbs_overlap_refinements.py
"""
from __future__ import annotations
import json
import os
import time

import duckdb
import lance

A = "s3://data-sink/active"
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports", "dsbs_overlap")
SPILL = "/tmp/_dsbs_refine_spill"


def so():
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": ep, "region": "auto"}


def rdr(name, cols, flt=None):
    sc = lance.dataset(f"{A}/{name}/", storage_options=so()).scanner(columns=cols, filter=flt)
    return sc.to_reader()


def main():
    t0 = time.time()
    os.makedirs(SPILL, exist_ok=True)
    con = duckdb.connect(); con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='16GB'"); con.execute(f"SET temp_directory='{SPILL}'")
    con.execute("SET preserve_insertion_order=false")

    def step(m): print(f"[{time.time()-t0:6.1f}s] {m}", flush=True)

    # roster + demand (award grain) + overlap combos (from prior build)
    con.register("_d", rdr("sba_dsbs_certified_firms", ["uei"]))
    con.execute("CREATE TABLE dsbs AS SELECT DISTINCT upper(trim(uei)) uei FROM _d WHERE uei IS NOT NULL AND trim(uei)<>''")
    con.unregister("_d")

    con.register("_gaa", rdr("govcon_active_awards", [
        "contract_award_unique_key", "naics_code", "psc_code",
        "total_dollars_obligated", "has_subcontracting_plan", "active_current", "active_potential"]))
    con.execute("""CREATE TABLE demand AS
        SELECT trim(contract_award_unique_key) cauk,
               trim(naics_code)||'|'||upper(trim(psc_code)) combo,
               coalesce(TRY_CAST(total_dollars_obligated AS DOUBLE),0.0) obligated
        FROM _gaa
        WHERE coalesce(has_subcontracting_plan,FALSE) AND (coalesce(active_current,FALSE) OR coalesce(active_potential,FALSE))
          AND naics_code IS NOT NULL AND trim(naics_code)<>'' AND psc_code IS NOT NULL AND trim(psc_code)<>''""")
    con.unregister("_gaa")
    demand_obl = con.execute("SELECT sum(obligated) FROM demand").fetchone()[0]
    con.execute(f"CREATE TABLE overlap AS SELECT * FROM read_parquet('{OUT}/overlap_combos.parquet')")

    out = {}

    # ── 1. CORRECTLY-SCOPED realized sub-dollar sizing (FSRS 2021+) ─────────────────
    step("1. realized FSRS sub-dollar sizing")
    con.register("_sw", rdr("subaward_naics_psc_wide",
        ["prime_award_unique_key", "subawardee_uei", "prime_naics_code", "prime_psc_code", "subaward_amount"]))
    con.execute("""CREATE TABLE fsrs AS
        SELECT trim(prime_award_unique_key) cauk, upper(trim(subawardee_uei)) sub_uei,
               trim(prime_naics_code)||'|'||upper(trim(prime_psc_code)) combo,
               coalesce(subaward_amount,0.0) amt,
               (upper(trim(subawardee_uei)) IN (SELECT uei FROM dsbs)) sub_is_dsbs
        FROM _sw""")
    con.unregister("_sw")
    fsrs_total = con.execute("SELECT count(*), sum(amt) FROM fsrs").fetchone()
    step(f"   FSRS total: {fsrs_total[0]:,} subawards  ${fsrs_total[1]/1e9:,.1f}B")

    # (a) sub $ UNDER the active demand primes (award-level join)
    under = con.execute("""
        SELECT count(DISTINCT f.cauk) primes_with_subs, count(*) n_sub, sum(f.amt) sub_usd,
               sum(f.amt) FILTER (WHERE f.sub_is_dsbs) sub_usd_dsbs,
               count(DISTINCT f.sub_uei) FILTER (WHERE f.sub_is_dsbs) dsbs_subs
        FROM fsrs f JOIN (SELECT DISTINCT cauk FROM demand) d ON f.cauk = d.cauk""").fetchone()
    step(f"   under ACTIVE demand primes: {under[0]:,} primes have realized subs; "
         f"${under[2]/1e9:,.1f}B sub$ (DSBS-captured ${(under[3] or 0)/1e9:,.1f}B, {under[4] or 0:,} DSBS subs)")

    # (b) combo-level sub $ in ADDRESSABLE demand combos (who's winning it now)
    combo = con.execute("""
        SELECT sum(f.amt) sub_usd_addr,
               sum(f.amt) FILTER (WHERE f.sub_is_dsbs) sub_usd_addr_dsbs
        FROM fsrs f WHERE f.combo IN (SELECT combo FROM overlap)""").fetchone()
    step(f"   in addressable combos (all-time FSRS): ${combo[0]/1e9:,.1f}B sub$, "
         f"DSBS-held ${(combo[1] or 0)/1e9:,.1f}B ({(combo[1] or 0)/combo[0]:.1%})")

    out["sub_dollar_sizing"] = {
        "fsrs_total_subawards": int(fsrs_total[0]), "fsrs_total_usd": float(fsrs_total[1]),
        "under_active_primes": {"primes_with_reported_subs": int(under[0]), "n_subawards": int(under[1]),
            "sub_usd": float(under[2] or 0), "sub_usd_dsbs_captured": float(under[3] or 0),
            "dsbs_distinct_subs": int(under[4] or 0),
            "dsbs_capture_pct": float((under[3] or 0) / under[2]) if under[2] else 0.0},
        "addressable_combos_alltime": {"sub_usd": float(combo[0] or 0),
            "sub_usd_dsbs_held": float(combo[1] or 0),
            "dsbs_held_pct": float((combo[1] or 0) / combo[0]) if combo[0] else 0.0},
    }

    # ── 2. SUB-PROOF-RESTRICTED overlap (on-point GTM evidence) ─────────────────────
    step("2. sub-proof-restricted overlap")
    r = con.execute(f"""
        SELECT
          (SELECT count(*) FROM overlap WHERE n_dsbs_subbed>0) combos_subproof,
          (SELECT sum(obligated) FROM overlap WHERE n_dsbs_subbed>0) obl_subproof,
          (SELECT sum(n_awards) FROM overlap WHERE n_dsbs_subbed>0) awards_subproof,
          (SELECT count(*) FROM overlap WHERE n_dsbs_primed>0) combos_primeproof,
          (SELECT sum(obligated) FROM overlap WHERE n_dsbs_primed>0) obl_primeproof
    """).fetchone()
    step(f"   sub-proof combos={r[0]:,} obligated=${r[1]/1e9:,.1f}B ({r[1]/demand_obl:.1%} of demand pool) awards={int(r[2]):,}")
    step(f"   prime-proof combos={r[3]:,} obligated=${r[4]/1e9:,.1f}B ({r[4]/demand_obl:.1%} of demand pool)")
    out["subproof_overlap"] = {"demand_obl": float(demand_obl),
        "combos_subproof": int(r[0]), "obl_subproof": float(r[1]), "obl_subproof_pct": float(r[1]/demand_obl),
        "awards_subproof": int(r[2]),
        "combos_primeproof": int(r[3]), "obl_primeproof": float(r[4]), "obl_primeproof_pct": float(r[4]/demand_obl)}

    # ── 3. SYMMETRIC-WINDOW (2021+) prime footprint sensitivity ─────────────────────
    step("3. symmetric-window (2021+) prime footprint re-scan [heavy]")
    con.register("_sp", rdr("usaspending_fpds_canonical_txn",
        ["recipient_uei", "naics_code", "product_or_service_code"],
        flt="action_date >= cast('2021-01-01' as date)"))
    con.execute("""CREATE TABLE prime21 AS
        SELECT DISTINCT upper(trim(recipient_uei)) uei,
               trim(naics_code)||'|'||upper(trim(product_or_service_code)) combo
        FROM _sp
        WHERE recipient_uei IS NOT NULL AND trim(recipient_uei)<>''
          AND naics_code IS NOT NULL AND trim(naics_code)<>''
          AND product_or_service_code IS NOT NULL AND trim(product_or_service_code)<>''
          AND upper(trim(recipient_uei)) IN (SELECT uei FROM dsbs)""")
    con.unregister("_sp")
    # sub proven combos (already 2021+)
    con.execute("""CREATE TABLE proven21_combo AS
        SELECT DISTINCT combo FROM (
            SELECT combo FROM prime21
            UNION
            SELECT combo FROM fsrs WHERE sub_is_dsbs)""")
    s = con.execute("""
        SELECT count(*) addr_combos, sum(obligated) addr_obl, sum(n_awards) addr_awards
        FROM (SELECT combo, sum(obligated) obligated, count(*) n_awards FROM demand GROUP BY combo)
        WHERE combo IN (SELECT combo FROM proven21_combo)""").fetchone()
    demand_combos = con.execute("SELECT count(DISTINCT combo) FROM demand").fetchone()[0]
    step(f"   2021+ symmetric: addressable combos={s[0]:,}/{demand_combos:,} "
         f"obligated=${s[1]/1e9:,.1f}B ({s[1]/demand_obl:.1%}) awards={int(s[2]):,}")
    out["symmetric_window_2021plus"] = {"addr_combos": int(s[0]), "demand_combos": int(demand_combos),
        "addr_obl": float(s[1]), "addr_obl_pct": float(s[1]/demand_obl), "addr_awards": int(s[2])}

    with open(f"{OUT}/refinements.json", "w") as fh:
        json.dump(out, fh, indent=2, default=str)
    step(f"DONE — wrote {OUT}/refinements.json")
    con.close()


if __name__ == "__main__":
    main()
