# /// script
# requires-python = ">=3.10"
# dependencies = ["pylance>=7","duckdb>=1.5,<2","pyarrow"]
# ///
"""Part 1 — the Filter Spine.

Deterministic target cohort pulled FROM THE LOCAL SoR (provider_360 /
practice_group_360 @ e2b479c, form5500_main) BEFORE any external payer request.
This is the string-match / NPI index that the payer reverse-mappers filter
against — extracting it first means the (expensive, out-of-core) MRF streams only
ever search for a tiny, fixed cohort.

  A) practice_group_360 — N independent, multi-provider, dual-pole specialty
     groups in ONE geography. "fan-in" is the count of distinct providers
     reassigning Medicare billing into a group (materialize.py:PRACTICE_AGG);
     at the group grain that is `member_count`. So:
        multi-provider + fan-in <= 9  ==  member_count BETWEEN 2 AND 9
        dual-pole INDEPENDENT          ==  independent_member_count = member_count
                                           (every member's *smallest* pole is this
                                            group — a real independent practice, not
                                            a consolidator's billing arm)
     Materialised keys per group:
        org_name           — string-match handle
        group_enrlmt_id    — PECOS group billing anchor.  NOTE: there is NO Type-2
                             (org) NPI / EIN / TIN anywhere in the substrate; PECOS
                             group_enrlmt_id IS the canonical group billing id.
        member_npis        — the Type-1 (individual practitioner) NPI array, native.

  B) form5500_main — top-N largest SELF-FUNDED welfare(health) employers in the
     SAME geography, by active participants. This is the UHC Module-B seed (UHC
     publishes a discrete employer-plan MRF per corporate group). 5500 funding
     reality: self-funded welfare plans pay from the sponsor's general assets ->
     FUNDING_GEN_ASSET_IND / BENEFIT_GEN_ASSET_IND = '1'; 4A in
     TYPE_WELFARE_BNFT_CODE marks a health benefit. The SHORT-FORM 5500-SF table
     is small plans (<100 participants) — wrong grain for "largest", so this uses
     form5500_main (full / Schedule-H filers).

Usage (reads R2 creds from env; run under `doppler run -- uv run ...`):
    doppler run -- uv run part1_filter_spine.py --state NY --specialty 207X00000X \
        --n-groups 5 --n-employers 50 --out /tmp/poc_out/filter_spine.json
"""
from __future__ import annotations

import argparse
import datetime
import decimal
import json
import os

BASE = "s3://data-sink/active"


def _so() -> dict[str, str]:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ["R2_ENDPOINT"], "region": "auto"}


def _open(feed: str, snap: str | None):
    import lance

    uri = f"{BASE}/{feed}/snapshot={snap}/" if snap else f"{BASE}/{feed}/"
    return lance.dataset(uri, storage_options=_so())


def _jsonify(o):
    if isinstance(o, decimal.Decimal):
        return float(o)
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    return o


def _clean(rows):
    return [{k: (list(v) if isinstance(v, list) else _jsonify(v)) for k, v in r.items()} for r in rows]


def extract(state: str, specialty: str, n_groups: int, n_employers: int,
            group_snapshot: str = "2026-06") -> dict:
    import duckdb

    con = duckdb.connect()

    # A) practice_group_360 — bitmap pushdown group_state, residual filter in DuckDB.
    pg = _open("practice_group_360", group_snapshot).scanner(
        filter=f"group_state = '{state}'",
        columns=["group_enrlmt_id", "org_name", "group_state", "member_count", "member_npis",
                 "top_specialty", "distinct_specialties", "independent_member_count",
                 "total_medicare_paid_usd", "avg_panel_risk_score", "total_rx_cost_usd"],
    ).to_table()
    con.register("pg", pg)
    groups = con.execute(f"""
        SELECT group_enrlmt_id, org_name, group_state, member_count,
               CAST(independent_member_count AS BIGINT) AS independent_member_count,
               top_specialty, distinct_specialties,
               CAST(total_medicare_paid_usd AS DOUBLE) AS total_medicare_paid_usd,
               avg_panel_risk_score, member_npis
        FROM pg
        WHERE top_specialty = '{specialty}'
          AND member_count BETWEEN 2 AND 9
          AND CAST(independent_member_count AS BIGINT) = member_count
          AND distinct_specialties <= 2
        ORDER BY total_medicare_paid_usd DESC NULLS LAST
        LIMIT {int(n_groups)}
    """).fetch_arrow_table().to_pylist()

    # B) form5500_main — top-N NY self-funded health employers by active participants.
    fm = _open("form5500_main", None).to_table()
    con.register("f5500", fm)
    emps = con.execute(f"""
        SELECT SPONSOR_DFE_NAME AS sponsor_name, SPONS_DFE_EIN AS ein,
               COALESCE(SPONS_DFE_MAIL_US_STATE, SPONS_DFE_LOC_US_STATE) AS state,
               PLAN_NAME AS plan_name, TYPE_WELFARE_BNFT_CODE AS welfare_code,
               TRY_CAST(TOT_ACTIVE_PARTCP_CNT AS BIGINT) AS active_participants
        FROM f5500
        WHERE COALESCE(SPONS_DFE_MAIL_US_STATE, SPONS_DFE_LOC_US_STATE) = '{state}'
          AND TYPE_WELFARE_BNFT_CODE IS NOT NULL AND TYPE_WELFARE_BNFT_CODE <> ''
          AND CONTAINS(TYPE_WELFARE_BNFT_CODE, '4A')
          AND (FUNDING_GEN_ASSET_IND = '1' OR BENEFIT_GEN_ASSET_IND = '1')
        ORDER BY active_participants DESC NULLS LAST
        LIMIT {int(n_employers)}
    """).fetch_arrow_table().to_pylist()

    groups, emps = _clean(groups), _clean(emps)
    for g in groups:
        g["type2_npi"] = "(NOT IN SUBSTRATE — PECOS group_enrlmt_id is the billing anchor)"
        g["type1_npi_count"] = len(g.get("member_npis") or [])

    return {"geography": state, "specialty_nucc": specialty,
            "groups": groups, "employers": emps,
            "counts": {"groups": len(groups), "employers": len(emps),
                       "target_npis": sum(g["type1_npi_count"] for g in groups)}}


def _main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", default="NY")
    ap.add_argument("--specialty", default="207X00000X", help="NUCC primary_taxonomy_code (207X00000X=ortho)")
    ap.add_argument("--n-groups", type=int, default=5)
    ap.add_argument("--n-employers", type=int, default=50)
    ap.add_argument("--out", default="/tmp/poc_out/filter_spine.json")
    a = ap.parse_args()
    spine = extract(a.state, a.specialty, a.n_groups, a.n_employers)
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w") as f:
        json.dump(spine, f, indent=2)
    print(f"groups={spine['counts']['groups']} employers={spine['counts']['employers']} "
          f"target_npis={spine['counts']['target_npis']} -> {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
