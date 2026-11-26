#!/usr/bin/env python3
"""DIBBS 84-Day Opportunity Engine — queries the full parsed PDF corpus in Postgres.

Modes:
  1. status              — Audit summary of ops.dibbs_pdf_parse_runs and dibbs.* tables
  2. dropship-sizing     — Exact Bucket B market size by Inspection Point & Packaging Standard
  3. undercut-partners   — Modeled gross margin & annual profit under 15% volume discount + 10% broker undercut
  4. fragmented-partners — High-fragmentation Bucket B targets (10+ brokers, low per-broker share so brokers don't fight back)
  5. wholesale-makers    — Ranked Bucket B manufacturers (0 direct wins, 100% broker wins)
  6. broker-snipers      — Open/recent RFQs for Bucket B makers
  7. dormant-oems        — Verified retiring/dormant legacy shops (0 wins in 2023-2026 across all 80+ days)
  8. zero-friction       — High-margin CNC/mechanical parts with Destination Inspection + ASTM D3951 Commercial Packaging + No FAT

Usage:
  doppler run -p core-x -c prd -- .venv/bin/python scripts/dibbs_opportunity_engine.py --mode all
"""

from __future__ import annotations

import argparse
import os
import subprocess
import psycopg


def _dsn() -> str:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if dsn:
        return dsn
    return subprocess.check_output(
        ["doppler", "secrets", "get", "HQX_DB_URL_POOLED", "--project", "core-x", "--config", "prd", "--plain"],
        text=True,
    ).strip()


def print_status(cur) -> None:
    print("=" * 80)
    print("1. DIBBS 84-DAY PDF INGESTION & PARSE AUDIT")
    print("=" * 80)
    cur.execute(
        """
        SELECT
            count(*) AS days_completed,
            coalesce(sum(total_pdfs), 0) AS total_pdfs,
            coalesce(sum(solicitations_ingested), 0) AS total_solicitations,
            coalesce(sum(history_rows_ingested), 0) AS raw_history_rows,
            coalesce(sum(anomaly_count), 0) AS total_anomalies,
            min(post_date) AS first_day,
            max(post_date) AS last_day,
            round(avg(duration_s), 2) AS avg_sec_per_day
        FROM ops.dibbs_pdf_parse_runs
        WHERE status = 'ok'
        """
    )
    r = cur.fetchone()
    print(f"  Days Parsed (status='ok')   : {r[0]} days ({r[5]} -> {r[6]})")
    print(f"  Total PDFs Parsed           : {r[1]:,}")
    print(f"  Total Solicitation Line Rows: {r[2]:,}")
    print(f"  Parse Anomalies             : {r[4]:,}")
    print(f"  Avg Parse Time per Day      : {r[7]}s")

    cur.execute("SELECT count(*), count(DISTINCT nsn) FROM dibbs.solicitation_items")
    s_cnt, s_nsns = cur.fetchone()
    cur.execute("SELECT count(*), count(DISTINCT maker_cage), count(maker_name) FROM dibbs.approved_makers")
    m_cnt, m_cages, m_named = cur.fetchone()
    cur.execute("SELECT count(*), count(DISTINCT nsn), count(DISTINCT winner_cage) FROM dibbs.procurement_history")
    h_cnt, h_nsns, h_winners = cur.fetchone()

    print(f"  dibbs.solicitation_items    : {s_cnt:,} rows across {s_nsns:,} unique NSNs")
    print(f"  dibbs.approved_makers       : {m_cnt:,} rows across {m_cages:,} unique Maker CAGEs ({m_named:,} with PDF names)")
    print(f"  dibbs.procurement_history   : {h_cnt:,} deduplicated historical awards across {h_nsns:,} NSNs and {h_winners:,} Winner CAGEs\n")


def print_wholesale_makers(cur, limit: int = 30) -> None:
    print("=" * 80)
    print("2. VERIFIED BUCKET B ('WHOLESALE-ONLY / NON-BIDDING') MANUFACTURERS (2023-2026)")
    print("   (0 Direct Wins across all 84 days of PDFs; 100% won by middlemen/brokers)")
    print("=" * 80)
    cur.execute(
        """
        WITH nsn_maker_counts AS (
            SELECT nsn, count(DISTINCT maker_cage) AS maker_cnt
            FROM dibbs.approved_makers
            GROUP BY nsn
        ),
        maker_names AS (
            SELECT maker_cage, max(maker_name) AS maker_name, count(DISTINCT nsn) AS total_nsns
            FROM dibbs.approved_makers
            GROUP BY maker_cage
        ),
        recent_hist AS (
            SELECT
                am.maker_cage,
                ph.nsn,
                ph.fsc,
                ph.contract_number,
                ph.winner_cage,
                ph.award_date,
                LEAST(ph.total_cost, 250000) AS capped_cost
            FROM dibbs.procurement_history ph
            JOIN nsn_maker_counts nmc ON nmc.nsn = ph.nsn AND nmc.maker_cnt <= 2
            JOIN dibbs.approved_makers am ON am.nsn = ph.nsn
            WHERE ph.award_date >= DATE '2023-01-01'
              AND LEFT(ph.fsc, 2) NOT IN ('65', '89', '91')
        ),
        agg AS (
            SELECT
                maker_cage,
                count(*) FILTER (WHERE winner_cage = maker_cage) AS self_wins_recent,
                count(*) FILTER (WHERE winner_cage <> maker_cage) AS broker_wins_recent,
                sum(capped_cost) FILTER (WHERE winner_cage <> maker_cage) AS broker_spend_recent,
                count(DISTINCT winner_cage) FILTER (WHERE winner_cage <> maker_cage) AS distinct_brokers,
                count(DISTINCT nsn) AS active_nsns_with_hist
            FROM recent_hist
            GROUP BY maker_cage
        ),
        all_time_self AS (
            SELECT am.maker_cage, count(*) AS self_wins_all_time
            FROM dibbs.procurement_history ph
            JOIN dibbs.approved_makers am ON am.nsn = ph.nsn AND am.maker_cage = ph.winner_cage
            GROUP BY am.maker_cage
        )
        SELECT
            a.maker_cage,
            coalesce(mn.maker_name, a.maker_cage) AS name,
            mn.total_nsns,
            a.active_nsns_with_hist,
            a.broker_wins_recent,
            round(a.broker_spend_recent, 0) AS broker_spend_23_26,
            a.distinct_brokers,
            coalesce(ats.self_wins_all_time, 0) AS self_wins_ever
        FROM agg a
        JOIN maker_names mn ON mn.maker_cage = a.maker_cage
        LEFT JOIN all_time_self ats ON ats.maker_cage = a.maker_cage
        WHERE a.self_wins_recent = 0
          AND a.broker_wins_recent >= 5
        ORDER BY a.broker_spend_recent DESC NULLS LAST
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    print(f"{'CAGE':<7} {'Manufacturer Name':<38} {'NSNs':>5} {'BrkrWins':>8} {'BrkrSpend(23-26)':>16} {'#Brkrs':>6} {'EverSelf':>8}")
    print("-" * 96)
    for cage, name, tot_nsns, act_nsns, bwins, bspend, dbrokers, everself in rows:
        print(f"{cage:<7} {str(name)[:37]:<38} {tot_nsns:>5} {bwins:>8} ${bspend:>14,.0f} {dbrokers:>6} {everself:>8}")
    print()


def print_broker_snipers(cur, limit: int = 30) -> None:
    print("=" * 80)
    print("3. SINGLE-BROKER MONOPOLY SNIPER TARGETS")
    print("   (Bucket B Makers with 0 Self-Wins and ONLY 1-2 Brokers winning their parts)")
    print("=" * 80)
    cur.execute(
        """
        WITH nsn_maker_counts AS (
            SELECT nsn, count(DISTINCT maker_cage) AS maker_cnt
            FROM dibbs.approved_makers
            GROUP BY nsn
        ),
        maker_self_wins AS (
            SELECT am.maker_cage,
                   count(*) FILTER (WHERE ph.winner_cage = am.maker_cage AND ph.award_date >= DATE '2023-01-01') AS recent_self_wins
            FROM dibbs.approved_makers am
            LEFT JOIN dibbs.procurement_history ph ON ph.nsn = am.nsn
            GROUP BY am.maker_cage
        ),
        nsn_broker_stats AS (
            SELECT
                ph.nsn,
                count(*) AS recent_awards,
                count(DISTINCT ph.winner_cage) AS distinct_winners,
                max(ph.winner_cage) AS top_broker_cage,
                max(ph.unit_cost) FILTER (WHERE ph.award_date >= DATE '2024-01-01') AS latest_unit_cost,
                sum(LEAST(ph.total_cost, 250000)) AS recent_spend
            FROM dibbs.procurement_history ph
            WHERE ph.award_date >= DATE '2023-01-01'
            GROUP BY ph.nsn
        ),
        latest_rfq AS (
            SELECT DISTINCT ON (nsn)
                nsn, solicitation_number, post_date, return_date, quantity,
                nomenclature, inspection_point, packaging_standard, buyer_email
            FROM dibbs.solicitation_items
            ORDER BY nsn, post_date DESC
        )
        SELECT
            lr.solicitation_number,
            lr.post_date,
            lr.nsn,
            lr.nomenclature,
            am.maker_cage,
            coalesce(am.maker_name, am.maker_cage) AS maker_name,
            am.part_number,
            nbs.top_broker_cage,
            nbs.distinct_winners,
            nbs.recent_awards,
            lr.quantity,
            round(nbs.latest_unit_cost, 2) AS last_unit_price,
            round(lr.quantity * nbs.latest_unit_cost, 0) AS est_order_val
        FROM latest_rfq lr
        JOIN nsn_maker_counts nmc ON nmc.nsn = lr.nsn AND nmc.maker_cnt = 1
        JOIN dibbs.approved_makers am ON am.nsn = lr.nsn
        JOIN maker_self_wins msw ON msw.maker_cage = am.maker_cage AND msw.recent_self_wins = 0
        JOIN nsn_broker_stats nbs ON nbs.nsn = lr.nsn
        WHERE nbs.distinct_winners >= 2
          AND nbs.recent_awards >= 3
          AND nbs.top_broker_cage <> am.maker_cage
          AND (lr.quantity * nbs.latest_unit_cost) >= 5000
          AND LEFT(lr.nsn, 2) NOT IN ('65', '89', '91')
        ORDER BY (lr.quantity * nbs.latest_unit_cost) DESC NULLS LAST
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    print(f"{'Solicitation':<14} {'NSN':<14} {'Nomenclature':<20} {'Maker CAGE & Name':<28} {'Part Number':<18} {'Brkr':<6} {'EstOrder$':>11}")
    print("-" * 116)
    for sol, pdt, nsn, nom, mcage, mname, pn, bcage, dwin, rawd, qty, uprice, estval in rows:
        m_str = f"{mcage} {str(mname)[:20]}"
        print(f"{sol:<14} {nsn:<14} {str(nom)[:19]:<20} {m_str:<28} {str(pn)[:17]:<18} {bcage:<6} ${estval:>10,.0f}")
    print()


def print_dormant_oems(cur, limit: int = 30) -> None:
    print("=" * 80)
    print("4. VERIFIED DORMANT / RETIRING LEGACY SHOPS ACROSS ALL 84 DAYS")
    print("   (Won directly in 1995-2022, 0 Direct Wins in 2023-2026 across all 84 days)")
    print("=" * 80)
    cur.execute(
        """
        WITH maker_summary AS (
            SELECT
                maker_cage,
                max(maker_name) AS maker_name,
                count(DISTINCT nsn) AS total_nsns
            FROM dibbs.approved_makers
            GROUP BY maker_cage
        ),
        win_history AS (
            SELECT
                am.maker_cage,
                count(*) FILTER (WHERE ph.winner_cage = am.maker_cage AND ph.award_date < DATE '2023-01-01') AS pre2023_self_wins,
                count(*) FILTER (WHERE ph.winner_cage = am.maker_cage AND ph.award_date >= DATE '2023-01-01') AS recent_self_wins,
                count(*) FILTER (WHERE ph.winner_cage <> am.maker_cage AND ph.award_date >= DATE '2023-01-01') AS recent_broker_wins,
                max(ph.award_date) FILTER (WHERE ph.winner_cage = am.maker_cage) AS last_self_win_date,
                sum(LEAST(ph.total_cost, 250000)) FILTER (WHERE ph.winner_cage = am.maker_cage) AS all_time_self_spend
            FROM dibbs.approved_makers am
            JOIN dibbs.procurement_history ph ON ph.nsn = am.nsn
            GROUP BY am.maker_cage
        )
        SELECT
            ms.maker_cage,
            coalesce(ms.maker_name, ms.maker_cage) AS name,
            ms.total_nsns,
            wh.pre2023_self_wins,
            wh.last_self_win_date,
            wh.recent_broker_wins,
            round(wh.all_time_self_spend, 0) AS hist_self_spend
        FROM maker_summary ms
        JOIN win_history wh ON wh.maker_cage = ms.maker_cage
        WHERE wh.recent_self_wins = 0
          AND wh.pre2023_self_wins >= 5
          AND ms.total_nsns BETWEEN 5 AND 150
        ORDER BY ms.total_nsns DESC, wh.pre2023_self_wins DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    print(f"{'CAGE':<7} {'Shop Name':<38} {'NSNs':>5} {'Pre23Wins':>9} {'LastSelfWin':>12} {'BrkrWins23+':>11} {'HistSelfSpend':>14}")
    print("-" * 102)
    for cage, name, nsns, prewins, lastdt, bwins, spend in rows:
        print(f"{cage:<7} {str(name)[:37]:<38} {nsns:>5} {prewins:>9} {str(lastdt):>12} {bwins:>11} ${spend:>13,.0f}")
    print()


def print_zero_friction(cur, limit: int = 30) -> None:
    print("=" * 80)
    print("5. ZERO-FRICTION HIGH-MARGIN MANUFACTURING PARTS")
    print("   (Destination Inspection + ASTM D3951 Commercial Box + No FAT + High Unit Price)")
    print("=" * 80)
    cur.execute(
        """
        WITH nsn_price AS (
            SELECT
                nsn,
                count(*) AS awards_23_26,
                round(avg(unit_cost), 2) AS avg_unit_cost,
                round(sum(LEAST(total_cost, 250000)), 0) AS spend_23_26
            FROM dibbs.procurement_history
            WHERE award_date >= DATE '2023-01-01'
            GROUP BY nsn
        ),
        clean_sols AS (
            SELECT DISTINCT ON (nsn)
                nsn, fsc, nomenclature, amsc, inspection_point,
                packaging_standard, has_fat, has_csi, quantity, item_description
            FROM dibbs.solicitation_items
            WHERE inspection_point = 'DESTINATION'
              AND packaging_standard = 'ASTM D3951'
              AND coalesce(has_fat, FALSE) = FALSE
              AND coalesce(has_csi, FALSE) = FALSE
              AND LEFT(fsc, 2) IN ('20', '25', '28', '29', '30', '31', '43', '47', '48', '53')
            ORDER BY nsn, post_date DESC
        )
        SELECT
            cs.nsn,
            cs.fsc,
            cs.nomenclature,
            cs.amsc,
            np.awards_23_26,
            np.avg_unit_cost,
            np.spend_23_26,
            LEFT(coalesce(cs.item_description, ''), 42) AS spec_preview
        FROM clean_sols cs
        JOIN nsn_price np ON np.nsn = cs.nsn
        WHERE np.avg_unit_cost >= 250
          AND np.spend_23_26 >= 50000
        ORDER BY np.spend_23_26 DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    print(f"{'NSN':<14} {'FSC':<5} {'Nomenclature':<22} {'AMSC':<4} {'Awds':>4} {'AvgUnit$':>10} {'Spend(23-26)':>13} {'Spec Preview'}")
    print("-" * 120)
    for nsn, fsc, nom, amsc, awds, ucost, spend, spec in rows:
        print(f"{nsn:<14} {fsc:<5} {str(nom)[:21]:<22} {str(amsc):<4} {awds:>4} ${ucost:>9,.2f} ${spend:>12,.0f} {spec}")
    print()


def print_dropship_sizing(cur) -> None:
    print("=" * 80)
    print("6. BUCKET B (100% BROKER-WON) MARKET BY INSPECTION & PACKAGING")
    print("=" * 80)
    cur.execute(
        """
        WITH nsn_price AS (
            SELECT
                nsn,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY unit_cost) AS med_unit_cost
            FROM dibbs.procurement_history
            WHERE award_date >= DATE '2023-01-01' AND unit_cost > 0
            GROUP BY nsn
        ),
        maker_self_wins AS (
            SELECT
                am.maker_cage,
                count(*) FILTER (WHERE ph.winner_cage = am.maker_cage AND ph.award_date >= DATE '2023-01-01') AS self_wins,
                count(*) FILTER (WHERE ph.winner_cage <> am.maker_cage AND ph.award_date >= DATE '2023-01-01') AS broker_wins
            FROM dibbs.approved_makers am
            LEFT JOIN dibbs.procurement_history ph ON ph.nsn = am.nsn
            GROUP BY am.maker_cage
        ),
        bucket_b_nsns AS (
            SELECT DISTINCT am.nsn
            FROM dibbs.approved_makers am
            JOIN maker_self_wins msw ON msw.maker_cage = am.maker_cage
            WHERE msw.self_wins = 0 AND msw.broker_wins >= 1
        )
        SELECT
            coalesce(si.inspection_point, 'UNKNOWN') AS insp,
            coalesce(si.packaging_standard, 'OTHER') AS pkg,
            count(*) AS rfq_count,
            count(DISTINCT si.nsn) AS unique_nsns,
            round((sum(LEAST(si.quantity * coalesce(np.med_unit_cost, 0), 250000)) * (250.0 / 80.0))::numeric, 0) AS annualized_val
        FROM dibbs.solicitation_items si
        JOIN bucket_b_nsns bb ON bb.nsn = si.nsn
        LEFT JOIN nsn_price np ON np.nsn = si.nsn
        WHERE coalesce(si.amsc, '') NOT IN ('G', 'T', '')
        GROUP BY 1, 2
        ORDER BY annualized_val DESC NULLS LAST
        """
    )
    for insp, pkg, cnt, nsns, ann in cur.fetchall():
        print(f"  Inspection={insp:<12} | Packaging={pkg:<14} | RFQs={cnt:>7,} | NSNs={nsns:>6,} | Annualized=${ann or 0:>14,.0f}/yr")
    print()


def print_undercut_partners(cur, limit: int = 35) -> None:
    print("=" * 80)
    print("7. TOP PARTNER MANUFACTURERS BY MODELED GROSS PROFIT")
    print("   (Bucket B + DESTINATION Inspection + 15% Volume Discount + 10% Undercut of Median Broker)")
    print("=" * 80)
    cur.execute(
        """
        WITH nsn_maker_counts AS (
            SELECT nsn, count(DISTINCT maker_cage) AS maker_cnt
            FROM dibbs.approved_makers
            GROUP BY nsn
        ),
        maker_names AS (
            SELECT maker_cage, max(maker_name) AS maker_name, count(DISTINCT nsn) AS total_nsns
            FROM dibbs.approved_makers
            GROUP BY maker_cage
        ),
        dest_nsns AS (
            SELECT DISTINCT nsn, max(packaging_standard) AS pkg
            FROM dibbs.solicitation_items
            WHERE inspection_point = 'DESTINATION'
            GROUP BY nsn
        ),
        recent_hist AS (
            SELECT
                am.maker_cage,
                ph.nsn,
                ph.fsc,
                ph.contract_number,
                ph.winner_cage,
                ph.award_date,
                ph.quantity,
                ph.unit_cost,
                LEAST(ph.total_cost, 250000) AS capped_cost
            FROM dibbs.procurement_history ph
            JOIN nsn_maker_counts nmc ON nmc.nsn = ph.nsn AND nmc.maker_cnt <= 2
            JOIN dibbs.approved_makers am ON am.nsn = ph.nsn
            JOIN dest_nsns dn ON dn.nsn = ph.nsn
            WHERE ph.award_date >= DATE '2023-01-01'
              AND LEFT(ph.fsc, 2) NOT IN ('65', '89', '91')
        ),
        maker_self_check AS (
            SELECT
                maker_cage,
                count(*) FILTER (WHERE winner_cage = maker_cage) AS self_wins,
                count(DISTINCT winner_cage) FILTER (WHERE winner_cage <> maker_cage) AS distinct_brokers
            FROM recent_hist
            GROUP BY maker_cage
        ),
        nsn_economics AS (
            SELECT
                rh.maker_cage,
                rh.nsn,
                count(*) AS orders_23_26,
                sum(rh.capped_cost) AS spend_23_26,
                min(rh.unit_cost) AS min_broker_uc,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY rh.unit_cost) AS med_broker_uc
            FROM recent_hist rh
            JOIN maker_self_check msc ON msc.maker_cage = rh.maker_cage
            WHERE msc.self_wins = 0 AND msc.distinct_brokers >= 2
              AND rh.winner_cage <> rh.maker_cage
            GROUP BY rh.maker_cage, rh.nsn
            HAVING count(*) >= 2 AND min(rh.unit_cost) > 0
        ),
        nsn_modeled AS (
            SELECT
                maker_cage,
                nsn,
                orders_23_26,
                spend_23_26,
                (0.85 * 0.92 * min_broker_uc) AS our_discounted_uc,
                GREATEST(0.98 * min_broker_uc, 0.90 * med_broker_uc) AS our_undercut_bid_uc
            FROM nsn_economics
        ),
        maker_rollup AS (
            SELECT
                nm.maker_cage,
                sum(nm.orders_23_26) AS total_orders_23_26,
                sum(nm.spend_23_26) / 3.8 AS annual_gov_spend,
                sum(nm.spend_23_26 * ((nm.our_undercut_bid_uc - nm.our_discounted_uc) / nm.our_undercut_bid_uc)) / sum(nm.spend_23_26) AS weighted_gross_margin_pct,
                sum((nm.spend_23_26 / 3.8) * ((nm.our_undercut_bid_uc - nm.our_discounted_uc) / nm.our_undercut_bid_uc)) AS est_annual_gross_profit
            FROM nsn_modeled nm
            GROUP BY nm.maker_cage
        )
        SELECT
            mr.maker_cage,
            coalesce(mn.maker_name, mr.maker_cage) AS name,
            mn.total_nsns,
            msc.distinct_brokers,
            mr.total_orders_23_26,
            round(mr.annual_gov_spend::numeric, 0) AS annual_spend,
            round((mr.weighted_gross_margin_pct * 100)::numeric, 1) AS gross_margin_pct,
            round(mr.est_annual_gross_profit::numeric, 0) AS annual_gross_profit
        FROM maker_rollup mr
        JOIN maker_names mn ON mn.maker_cage = mr.maker_cage
        JOIN maker_self_check msc ON msc.maker_cage = mr.maker_cage
        WHERE mr.annual_gov_spend >= 400000
          AND msc.distinct_brokers BETWEEN 2 AND 35
        ORDER BY mr.est_annual_gross_profit DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    print(f"{'CAGE':<7} {'Manufacturer Name':<36} {'NSNs':>5} {'#Brkrs':>6} {'Orders':>6} {'AnnualGovSpend':>15} {'OurMargin%':>11} {'EstAnnualProfit':>16}")
    print("-" * 110)
    for cage, name, nsns, brkrs, ords, ann_spend, gm_pct, ann_prof in rows:
        print(f"{cage:<7} {str(name)[:35]:<36} {nsns:>5} {brkrs:>6} {ords:>6} ${ann_spend:>14,.0f} {gm_pct:>10.1f}% ${ann_prof:>15,.0f}")
    print()


def print_fragmented_partners(cur, limit: int = 35) -> None:
    print("=" * 80)
    print("8. HIGH-FRAGMENTATION 'UNDER-THE-RADAR' PARTNER MANUFACTURERS")
    print("   (Bucket B + DESTINATION + >=10 Competing Brokers so average broker wins too little to fight back)")
    print("=" * 80)
    cur.execute(
        """
        WITH nsn_maker_counts AS (
            SELECT nsn, count(DISTINCT maker_cage) AS maker_cnt
            FROM dibbs.approved_makers
            GROUP BY nsn
        ),
        maker_names AS (
            SELECT maker_cage, max(maker_name) AS maker_name, count(DISTINCT nsn) AS total_nsns
            FROM dibbs.approved_makers
            GROUP BY maker_cage
        ),
        dest_nsns AS (
            SELECT DISTINCT nsn
            FROM dibbs.solicitation_items
            WHERE inspection_point = 'DESTINATION'
        ),
        recent_hist AS (
            SELECT
                am.maker_cage,
                ph.nsn,
                ph.fsc,
                ph.winner_cage,
                ph.unit_cost,
                LEAST(ph.total_cost, 250000) AS capped_cost
            FROM dibbs.procurement_history ph
            JOIN nsn_maker_counts nmc ON nmc.nsn = ph.nsn AND nmc.maker_cnt <= 2
            JOIN dibbs.approved_makers am ON am.nsn = ph.nsn
            JOIN dest_nsns dn ON dn.nsn = ph.nsn
            WHERE ph.award_date >= DATE '2023-01-01'
              AND LEFT(ph.fsc, 2) NOT IN ('65', '89', '91')
        ),
        maker_self_check AS (
            SELECT
                maker_cage,
                count(*) FILTER (WHERE winner_cage = maker_cage) AS self_wins,
                count(DISTINCT winner_cage) FILTER (WHERE winner_cage <> maker_cage) AS distinct_brokers
            FROM recent_hist
            GROUP BY maker_cage
        ),
        nsn_economics AS (
            SELECT
                rh.maker_cage,
                rh.nsn,
                count(*) AS orders_23_26,
                sum(rh.capped_cost) AS spend_23_26,
                min(rh.unit_cost) AS min_broker_uc,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY rh.unit_cost) AS med_broker_uc
            FROM recent_hist rh
            JOIN maker_self_check msc ON msc.maker_cage = rh.maker_cage
            WHERE msc.self_wins = 0 AND msc.distinct_brokers >= 10
              AND rh.winner_cage <> rh.maker_cage
            GROUP BY rh.maker_cage, rh.nsn
            HAVING count(*) >= 2 AND min(rh.unit_cost) > 0
        ),
        nsn_modeled AS (
            SELECT
                maker_cage,
                nsn,
                orders_23_26,
                spend_23_26,
                (0.85 * 0.92 * min_broker_uc) AS our_discounted_uc,
                GREATEST(0.98 * min_broker_uc, 0.90 * med_broker_uc) AS our_undercut_bid_uc
            FROM nsn_economics
        ),
        maker_rollup AS (
            SELECT
                nm.maker_cage,
                sum(nm.orders_23_26) AS total_orders_23_26,
                sum(nm.spend_23_26) / 3.8 AS annual_gov_spend,
                sum(nm.spend_23_26 * ((nm.our_undercut_bid_uc - nm.our_discounted_uc) / nm.our_undercut_bid_uc)) / sum(nm.spend_23_26) AS weighted_gross_margin_pct,
                sum((nm.spend_23_26 / 3.8) * ((nm.our_undercut_bid_uc - nm.our_discounted_uc) / nm.our_undercut_bid_uc)) AS est_annual_gross_profit
            FROM nsn_modeled nm
            GROUP BY nm.maker_cage
        )
        SELECT
            mr.maker_cage,
            coalesce(mn.maker_name, mr.maker_cage) AS name,
            mn.total_nsns,
            msc.distinct_brokers,
            round((mr.annual_gov_spend / msc.distinct_brokers)::numeric, 0) AS avg_rev_per_broker,
            round(mr.annual_gov_spend::numeric, 0) AS annual_spend,
            round((mr.weighted_gross_margin_pct * 100)::numeric, 1) AS gross_margin_pct,
            round(mr.est_annual_gross_profit::numeric, 0) AS annual_gross_profit
        FROM maker_rollup mr
        JOIN maker_names mn ON mn.maker_cage = mr.maker_cage
        JOIN maker_self_check msc ON msc.maker_cage = mr.maker_cage
        WHERE mr.annual_gov_spend BETWEEN 400000 AND 4000000
          AND (mr.annual_gov_spend / msc.distinct_brokers) <= 180000
        ORDER BY mr.est_annual_gross_profit DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    print(f"{'CAGE':<7} {'Manufacturer Name':<34} {'NSNs':>5} {'#Brkrs':>6} {'Avg$/Brkr':>11} {'AnnualGovSpend':>15} {'OurMargin%':>11} {'EstAnnualProfit':>16}")
    print("-" * 114)
    for cage, name, nsns, brkrs, avg_b, ann_spend, gm_pct, ann_prof in rows:
        print(f"{cage:<7} {str(name)[:33]:<34} {nsns:>5} {brkrs:>6} ${avg_b:>10,.0f} ${ann_spend:>14,.0f} {gm_pct:>10.1f}% ${ann_prof:>15,.0f}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=[
            "all",
            "status",
            "dropship-sizing",
            "undercut-partners",
            "fragmented-partners",
            "wholesale-makers",
            "broker-snipers",
            "dormant-oems",
            "zero-friction",
        ],
        default="all",
    )
    parser.add_argument("--limit", type=int, default=25)
    args = parser.parse_args()

    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        print_status(cur)
        if args.mode in ("all", "dropship-sizing"):
            print_dropship_sizing(cur)
        if args.mode in ("all", "undercut-partners"):
            print_undercut_partners(cur, args.limit)
        if args.mode in ("all", "fragmented-partners"):
            print_fragmented_partners(cur, args.limit)
        if args.mode in ("all", "wholesale-makers"):
            print_wholesale_makers(cur, args.limit)
        if args.mode in ("all", "broker-snipers"):
            print_broker_snipers(cur, args.limit)
        if args.mode in ("all", "dormant-oems"):
            print_dormant_oems(cur, args.limit)
        if args.mode in ("all", "zero-friction"):
            print_zero_friction(cur, args.limit)


if __name__ == "__main__":
    main()
