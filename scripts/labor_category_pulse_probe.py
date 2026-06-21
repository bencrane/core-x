#!/usr/bin/env python3
"""READ-ONLY pulse check — labor_category_values reality in govcon_award_scope_requirements.

govcon_award_scope_requirements is keyed on contract_award_unique_key and carries NO PSC,
so the "Big Three" service-code filter (R499/R425/DA01) is applied by joining to
govcon_active_awards on the award key. labor_category_values is a per-award DISTINCT,
alpha-sorted, cap-25 list<string> rolled up from govcon_award_requirements
(requirement_type='labor_category' AND validated).

Emits: coverage, big-three Top-25 exact strings, per-PSC split, in-vocabulary share
(against the frozen 36-member vocabulary.labor_categories — the LLM-lane controlled list),
and a global Top-30 for lane characterization. NO writes.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/labor_category_pulse_probe.py > /tmp/labor_pulse.json
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

import duckdb
import lance

A = "s3://data-sink/active"
GAA = f"{A}/govcon_active_awards/"
ASR = f"{A}/govcon_award_scope_requirements/"
BIG_THREE = ("R499", "R425", "DA01")

# Frozen LLM-lane controlled vocabulary (reference/govcon_llm_lane_v1/vocabulary.json).
VOCAB_LABOR = {
    "carpenter", "crane_operator", "custodian", "dispatcher", "electrician",
    "equipment_operator", "food_service_worker", "general_laborer", "glazier",
    "heavy_equipment_operator", "hvac_technician", "instructor", "interpreter",
    "janitor", "licensed_practical_nurse", "locksmith", "mason", "medical_assistant",
    "millwright", "painter", "pest_control_technician", "pipefitter", "plumber",
    "program_manager", "project_manager", "quality_control_manager", "registered_nurse",
    "roofer", "safety_officer", "security_guard", "sheet_metal_worker", "site_superintendent",
    "surveyor", "translator", "truck_driver", "welder",
}


def r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def log(m: str) -> None:
    print(m, file=sys.stderr, flush=True)


def rows(con, q):
    cur = con.execute(q)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def main() -> int:
    so = r2_so()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/duck_spill_labor", exist_ok=True)
    con.execute("SET memory_limit='8GB'")
    con.execute("SET temp_directory='/tmp/duck_spill_labor'")
    con.register("gaa", lance.dataset(GAA, storage_options=so))
    con.register("asr", lance.dataset(ASR, storage_options=so))

    out: dict = {"as_of": dt.datetime.now(dt.timezone.utc).date().isoformat(),
                 "asr_uri": ASR, "big_three": list(BIG_THREE)}

    # schema sanity — confirm the list column exists + its type
    out["asr_columns"] = [f.name for f in lance.dataset(ASR, storage_options=so).schema]
    out["asr_total_rows"] = con.execute("SELECT count(*) FROM asr").fetchone()[0]
    out["asr_has_labor_category_rows"] = con.execute(
        "SELECT count(*) FROM asr WHERE has_labor_category").fetchone()[0]

    # active big-three awards + overlap with scope-requirements
    con.execute(f"""
        CREATE TEMP TABLE bt AS
        SELECT contract_award_unique_key AS k, upper(trim(psc_code)) AS psc
        FROM gaa
        WHERE upper(trim(psc_code)) IN {BIG_THREE}
          AND (active_current OR active_potential)
    """)
    out["big_three_active_awards"] = con.execute("SELECT count(*) FROM bt").fetchone()[0]
    out["big_three_active_by_psc"] = rows(con, "SELECT psc, count(*) n FROM bt GROUP BY 1 ORDER BY n DESC")

    # join to scope-requirements; explode labor list
    con.execute("""
        CREATE TEMP TABLE bt_sr AS
        SELECT bt.k, bt.psc,
               asr.has_labor_category,
               asr.labor_category_values
        FROM bt JOIN asr ON bt.k = asr.contract_award_unique_key
    """)
    out["big_three_in_scope_requirements"] = con.execute("SELECT count(*) FROM bt_sr").fetchone()[0]
    out["big_three_with_labor_category"] = con.execute(
        "SELECT count(*) FROM bt_sr WHERE has_labor_category").fetchone()[0]
    out["big_three_with_nonempty_labor_list"] = con.execute(
        "SELECT count(*) FROM bt_sr WHERE labor_category_values IS NOT NULL AND len(labor_category_values) > 0"
    ).fetchone()[0]

    # exploded occurrences (award-level distinct membership)
    con.execute("""
        CREATE TEMP TABLE lc AS
        SELECT k, psc, trim(unnest(labor_category_values)) AS labor_category
        FROM bt_sr
        WHERE labor_category_values IS NOT NULL AND len(labor_category_values) > 0
    """)
    out["big_three_total_labor_occurrences"] = con.execute("SELECT count(*) FROM lc").fetchone()[0]
    out["big_three_distinct_labor_strings"] = con.execute(
        "SELECT count(DISTINCT labor_category) FROM lc").fetchone()[0]

    # THE EYEBALL — Top 25 exact strings across the big three
    out["big_three_top25_labor_strings"] = rows(con, """
        SELECT labor_category,
               count(*) AS award_occurrences,
               length(labor_category) AS str_len,
               (labor_category = lower(labor_category)
                AND labor_category NOT LIKE '% %') AS looks_normalized
        FROM lc GROUP BY 1 ORDER BY award_occurrences DESC LIMIT 25""")

    # in-vocabulary share (controlled 36-member list) — noise-to-signal proxy
    vocab_list = ",".join("'" + v + "'" for v in sorted(VOCAB_LABOR))
    out["big_three_vocab_conformance"] = con.execute(f"""
        SELECT
            count(*)                                                          AS total_occurrences,
            count(*) FILTER (WHERE labor_category IN ({vocab_list}))          AS in_vocab_occurrences,
            count(DISTINCT labor_category)                                    AS distinct_strings,
            count(DISTINCT labor_category) FILTER (WHERE labor_category IN ({vocab_list})) AS distinct_in_vocab
        FROM lc
    """).df().to_dict("records")[0]

    # per-PSC top 10 (texture per service code)
    out["per_psc_top10"] = {}
    for psc in BIG_THREE:
        out["per_psc_top10"][psc] = rows(con, f"""
            SELECT labor_category, count(*) AS award_occurrences
            FROM lc WHERE psc = '{psc}'
            GROUP BY 1 ORDER BY award_occurrences DESC LIMIT 10""")

    # GLOBAL lane characterization — Top 30 across ALL scope-requirements (not just big three)
    con.execute("""
        CREATE TEMP TABLE lc_all AS
        SELECT contract_award_unique_key AS k, trim(unnest(labor_category_values)) AS labor_category
        FROM asr WHERE labor_category_values IS NOT NULL AND len(labor_category_values) > 0
    """)
    out["global_total_labor_occurrences"] = con.execute("SELECT count(*) FROM lc_all").fetchone()[0]
    out["global_distinct_labor_strings"] = con.execute(
        "SELECT count(DISTINCT labor_category) FROM lc_all").fetchone()[0]
    out["global_vocab_conformance"] = con.execute(f"""
        SELECT count(*) AS total_occurrences,
               count(*) FILTER (WHERE labor_category IN ({vocab_list})) AS in_vocab_occurrences,
               count(DISTINCT labor_category) AS distinct_strings,
               count(DISTINCT labor_category) FILTER (WHERE labor_category IN ({vocab_list})) AS distinct_in_vocab
        FROM lc_all""").df().to_dict("records")[0]
    out["global_top30_labor_strings"] = rows(con, """
        SELECT labor_category, count(*) AS award_occurrences,
               length(labor_category) AS str_len
        FROM lc_all GROUP BY 1 ORDER BY award_occurrences DESC LIMIT 30""")

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    print("", file=sys.stderr, flush=True)
    log("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
