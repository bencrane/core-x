"""Read-only DuckDB recon — specialized equipment-finance verticals.

Sizes two expansion verticals (Agriculture/Forestry, Aerospace/Defense mfg)
against the construction "yellow iron" baseline, on the active prime-award
substrate `govcon_active_awards` (Lance SoR @ R2). NO writes, NO index changes.

Canonical access pattern: lance.dataset(...) registered into an in-memory
DuckDB. Invoke under doppler so R2_* secrets are present:

    doppler run --project core-x --config prd -- \
        python3 scripts/archive/equipment_finance_vertical_recon.py

"Active" filter is the directive's literal: pop_current_end >= CURRENT_DATE
(BTREE-indexed date col). Liveness sensitivity is printed once so the chosen
denominator is transparent. Primary value metric = current_total_value_of_award
(house standard; matches the rebuild doc's $1,607.5B headline).
"""
from __future__ import annotations
import os
import duckdb
import lance

A = "s3://data-sink/active"
DS = f"{A}/govcon_active_awards/"

# Directive's literal "active" definition. Strict floor: excludes NULL-PoP
# (pop_unknown) and option-tail-only awards. Sensitivity printed in §0.
LIVE = "pop_current_end >= CURRENT_DATE"

# Vertical filter fragments (NAICS = the contractor's industry; PSC = what's bought).
AG_NAICS = "left(naics_code,2) = '11'"                    # sector 11 Ag/Forestry/Fishing/Hunting
AG_PSC = "psc_category = 'F'"                             # Cat F Natural Resources & Conservation (svc; incl. F014 tree thinning)
AERO_NAICS = "left(naics_code,4) = '3364'"               # subsector 3364 Aerospace Product & Parts Mfg
AERO_PSC = "psc_fsg IN ('15','16','17')"                 # FSG 15 airframe / 16 components / 17 launch-land-ground
YELLOW_IRON = "construction_wage_rate_requirements = 'YES'"  # Davis-Bacon construction baseline


def so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT")
    aid = os.environ.get("R2_ACCOUNT_ID")
    if not ep and aid:
        ep = f"https://{aid}.r2.cloudflarestorage.com"
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": ep, "region": "auto",
    }


def hdr(n: str) -> None:
    print("\n" + "=" * 84)
    print(n)
    print("=" * 84)


def scalar(con, sql: str):
    return con.execute(sql).fetchone()


def vertical(con, name: str, where: str) -> None:
    """Volume + value + top-10 prime-HQ states for an active-award subset."""
    hdr(f"{name}   [filter: {where} AND {LIVE}]")
    n, cur, obl, oth = scalar(con, f"""
        SELECT count(*),
               coalesce(sum(current_total_value_of_award),0),
               coalesce(sum(total_dollars_obligated),0),
               coalesce(sum(potential_total_value_of_award),0)
        FROM aw WHERE {where} AND {LIVE}
    """)
    print(f"  active awards ........................ {n:>12,}")
    print(f"  Σ current_total_value_of_award ....... ${cur/1e9:>10,.2f}B   (primary 'active contract value')")
    print(f"  Σ total_dollars_obligated ............ ${obl/1e9:>10,.2f}B   (cumulative obligated)")
    print(f"  Σ potential_total_value_of_award ..... ${oth/1e9:>10,.2f}B   (ceiling incl. all options)")

    nullstate = scalar(con, f"""
        SELECT count(*) FROM aw
        WHERE {where} AND {LIVE} AND nullif(trim(recipient_state_code),'') IS NULL
    """)[0]
    print(f"\n  prime-HQ state NULL/blank ............ {nullstate:,}  (excluded from geo ranking)")
    print("\n  --- Top 10 prime-HQ states (recipient_state_code) ---")
    print(con.execute(f"""
        SELECT recipient_state_code AS st,
               count(*)                                          AS awards,
               round(sum(current_total_value_of_award)/1e9,2)    AS value_b_usd,
               round(100.0*count(*) / (SELECT count(*) FROM aw WHERE {where} AND {LIVE}),1) AS pct_of_vertical
        FROM aw
        WHERE {where} AND {LIVE}
          AND nullif(trim(recipient_state_code),'') IS NOT NULL
        GROUP BY 1 ORDER BY awards DESC LIMIT 10
    """).df().to_string(index=False))


def main() -> None:
    s = so()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='8GB'")
    con.execute("SET temp_directory='/tmp/duck_recon_spill'")
    con.register("aw", lance.dataset(DS, storage_options=s))

    # ---------------------------------------------------------------- §0 preflight
    hdr("0. PREFLIGHT — universe, liveness sensitivity, enum literals (no guessing)")
    asof, total = scalar(con, "SELECT max(as_of_date), count(*) FROM aw")
    print(f"  as_of_date = {asof}   |   total rows = {total:,}   |   CURRENT_DATE eval'd server-side")
    print("\n  liveness sensitivity (which 'active' denominator):")
    print(con.execute("""
        SELECT
          count(*)                                          AS all_rows,
          count(*) FILTER (WHERE pop_current_end >= CURRENT_DATE)        AS pop_curr_ge_today,
          count(*) FILTER (WHERE active_current)            AS active_current_flag,
          count(*) FILTER (WHERE active_potential)          AS active_potential_flag,
          count(*) FILTER (WHERE pop_unknown)               AS pop_unknown,
          count(*) FILTER (WHERE pop_current_end IS NULL)   AS pop_curr_null
        FROM aw
    """).df().to_string(index=False))

    print("\n  business_size / business_size_code distribution (small-biz mapping):")
    print(con.execute("""
        SELECT business_size, business_size_code, count(*) AS rows
        FROM aw GROUP BY 1,2 ORDER BY rows DESC
    """).df().to_string(index=False))

    print("\n  construction_wage_rate_requirements distribution (yellow-iron literal):")
    print(con.execute("""
        SELECT construction_wage_rate_requirements AS cwrr,
               any_value(construction_wage_rate_requirements_code) AS code,
               count(*) AS rows
        FROM aw GROUP BY 1 ORDER BY rows DESC
    """).df().to_string(index=False))

    print("\n  NAICS sector-11 & subsector-3364 presence (string-prefix sanity):")
    print(con.execute(f"""
        SELECT
          count(*) FILTER (WHERE {AG_NAICS})                       AS naics_11_all,
          count(*) FILTER (WHERE {AG_NAICS} AND {LIVE})            AS naics_11_live,
          count(*) FILTER (WHERE {AERO_NAICS})                     AS naics_3364_all,
          count(*) FILTER (WHERE {AERO_NAICS} AND {LIVE})          AS naics_3364_live,
          count(*) FILTER (WHERE {AG_PSC})                         AS psc_F_all,
          count(*) FILTER (WHERE {AERO_PSC})                       AS psc_1516_17_all
        FROM aw
    """).df().to_string(index=False))

    # ============================================================ 1. AGRICULTURE
    vertical(con, "1A. AGRICULTURE & FORESTRY — NAICS sector 11 (primary)", AG_NAICS)

    print("\n  --- composition: top NAICS codes in the live sector-11 bucket ---")
    print(con.execute(f"""
        SELECT naics_code, any_value(naics_description) AS naics_description,
               count(*) AS awards,
               round(sum(current_total_value_of_award)/1e9,3) AS value_b_usd
        FROM aw WHERE {AG_NAICS} AND {LIVE}
        GROUP BY 1 ORDER BY awards DESC LIMIT 12
    """).df().to_string(index=False))

    print("\n  --- 'iron' proxy: subcontracting-plan split (live sector-11) ---")
    print(con.execute(f"""
        SELECT has_subcontracting_plan, count(*) AS awards,
               round(sum(current_total_value_of_award)/1e9,3) AS value_b_usd
        FROM aw WHERE {AG_NAICS} AND {LIVE}
        GROUP BY 1 ORDER BY has_subcontracting_plan DESC
    """).df().to_string(index=False))

    print("\n  --- ALT lens: PSC Category F (Natural Resources & Conservation services) ---")
    print(con.execute(f"""
        SELECT count(*) AS awards,
               round(sum(current_total_value_of_award)/1e9,3) AS value_b_usd,
               count(*) FILTER (WHERE psc_code='F014') AS psc_F014_tree_thinning
        FROM aw WHERE {AG_PSC} AND {LIVE}
    """).df().to_string(index=False))
    print(con.execute(f"""
        SELECT psc_code, any_value(psc_description) AS psc_description, count(*) AS awards
        FROM aw WHERE {AG_PSC} AND {LIVE}
        GROUP BY 1 ORDER BY awards DESC LIMIT 8
    """).df().to_string(index=False))

    # ============================================================ 2. AEROSPACE
    vertical(con, "2A. AEROSPACE & DEFENSE MFG — NAICS subsector 3364 (primary)", AERO_NAICS)

    print("\n  --- composition: NAICS 3364xx breakdown (live) ---")
    print(con.execute(f"""
        SELECT naics_code, any_value(naics_description) AS naics_description,
               count(*) AS awards,
               round(sum(current_total_value_of_award)/1e9,3) AS value_b_usd
        FROM aw WHERE {AERO_NAICS} AND {LIVE}
        GROUP BY 1 ORDER BY awards DESC
    """).df().to_string(index=False))

    print("\n  --- tooling/finance proxy: small-business split (live NAICS-3364) ---")
    print(con.execute(f"""
        SELECT business_size, business_size_code, count(*) AS awards,
               round(sum(current_total_value_of_award)/1e9,3) AS value_b_usd
        FROM aw WHERE {AERO_NAICS} AND {LIVE}
        GROUP BY 1,2 ORDER BY awards DESC
    """).df().to_string(index=False))

    print("\n  --- ALT lens: PSC FSG 15/16/17 (aircraft structures/components/ground) ---")
    print(con.execute(f"""
        SELECT psc_fsg, count(*) AS awards,
               round(sum(current_total_value_of_award)/1e9,3) AS value_b_usd,
               count(*) FILTER (WHERE business_size_code='S') AS small_biz
        FROM aw WHERE {AERO_PSC} AND {LIVE}
        GROUP BY 1 ORDER BY awards DESC
    """).df().to_string(index=False))

    # ============================================================ 3. YELLOW IRON
    vertical(con, "3. YELLOW IRON BASELINE — construction_wage_rate_requirements='YES'", YELLOW_IRON)

    # ---------------------------------------------------------------- side-by-side
    hdr("4. SIDE-BY-SIDE (all on live filter pop_current_end >= CURRENT_DATE)")
    print(con.execute(f"""
        SELECT vertical, awards,
               round(value_cur/1e9,2)  AS value_b_usd,
               round(value_obl/1e9,2)  AS obligated_b_usd
        FROM (
          SELECT 'Ag/Forestry (NAICS 11)'   AS vertical, count(*) AS awards,
                 sum(current_total_value_of_award) value_cur, sum(total_dollars_obligated) value_obl
          FROM aw WHERE {AG_NAICS} AND {LIVE}
          UNION ALL
          SELECT 'Aerospace (NAICS 3364)', count(*),
                 sum(current_total_value_of_award), sum(total_dollars_obligated)
          FROM aw WHERE {AERO_NAICS} AND {LIVE}
          UNION ALL
          SELECT 'Yellow Iron (Davis-Bacon)', count(*),
                 sum(current_total_value_of_award), sum(total_dollars_obligated)
          FROM aw WHERE {YELLOW_IRON} AND {LIVE}
        ) ORDER BY awards DESC
    """).df().to_string(index=False))

    con.close()
    print("\n[done] read-only — no writes, no index changes.")


if __name__ == "__main__":
    main()
