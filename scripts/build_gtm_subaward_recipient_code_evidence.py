#!/usr/bin/env python3
"""gtm_subaward_recipient_code_evidence — the un-allocated sub-out destination evidence table.

SoR  s3://data-sink/active/gtm_subaward_recipient_code_evidence/
     (Lance; derived, snapshot-overwrite; BTREE prime_awardee_uei / subawardee_uei /
      code / subaward_unique_key)

WHAT THIS IS
"Where do a prime's subbed-out dollars go, by kind of work?" — the connective tissue
between capability (lanes/inferred) and money in motion (subaward edges). Every subaward
edge is exploded against every NAICS/PSC code its RECIPIENT carries, every way of knowing
included and labeled, NOTHING allocated: subaward_amount repeats verbatim across a
recipient's code rows. Allocation (dominant-lane vs fractional), which code_source values
to trust, and floors are read-time recipe parameters — never baked here.

CODE_SOURCE (self-describing: each value states exactly how the code is known)
  awarded_prime_contracts_in_code   recipient has prime awards carrying this code
                                    (gtm_entity_code_lanes side=prime); lane_obl_lifetime = $
  delivered_subawards_under_code    recipient delivered subawards under prime awards
                                    carrying this code (lanes side=sub — the PRIME award's
                                    code, never a claim of the sub's own work)
  sam_primary_naics                 the code the recipient declared PRIMARY in SAM
  sam_registered_naics              the code is in the recipient's full SAM-declared set
                                    (the primary also appears here — each row states one
                                    fact; dedupe by preferring the stronger source at read)
  subaward_reported_naics           the NAICS reported on THIS subaward record (edge-level,
                                    strongest site-truth, worst coverage)

INFERRED codes are deliberately NOT materialized: per-recipient inferred codes live in
gtm_entity_inferred_{primeable,subbable}_codes (BTREE uei) — composing them here would
multiply 1.3M edges by ~400 inferred rows each. Read-time joins the projections directly.

PRIME-SIDE CONTEXT (columns, not rows — entity-level facts riding each edge verbatim):
  prime_awardee_sam_primary_naics       what the PRIME declared primary in SAM
  prime_awardee_sam_registered_naics    the prime's full SAM-declared set (list)
"Does the prime award's code match what the prime itself declares?" is then row-local.

DENOMINATORS for fractional allocation ride each row: the recipient's all-code lane $
per side (recipient_prime_lane_obl_total / recipient_sub_lane_obl_total).

GRAIN: 1 row / (subaward_unique_key, code_source, code_type, code).
Fail-closed on grain duplication. Idempotent snapshot-overwrite.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_subaward_recipient_code_evidence.py [--verify]
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
SUB_URI = f"{A}/usaspending_subaward_canonical/"
LANES_URI = f"{A}/gtm_entity_code_lanes/"
SAM_URI = f"{A}/gtm_sam_entities/"
OUT = f"{A}/gtm_subaward_recipient_code_evidence/"
PARAM_SET_ID = "v1"
BTREE_COLS = ["prime_awardee_uei", "subawardee_uei", "code", "subaward_unique_key"]


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def build() -> int:
    opt = so()
    as_of = date.today().isoformat()
    con = duckdb.connect()
    con.execute("SET memory_limit='24GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")

    sub = lance.dataset(SUB_URI, storage_options=opt)
    con.register("_e", sub.scanner(
        columns=["subaward_unique_key", "prime_award_unique_key", "prime_awardee_uei",
                 "subawardee_uei", "subaward_amount", "subaward_action_date",
                 "prime_award_naics_code", "prime_award_product_or_service_code",
                 "sub_naics"],
        filter="subaward_unique_key IS NOT NULL AND subawardee_uei IS NOT NULL").to_reader())
    con.execute("CREATE TABLE edges AS SELECT * FROM _e")
    con.unregister("_e")

    lanes = lance.dataset(LANES_URI, storage_options=opt)
    con.register("_l", lanes.scanner(
        columns=["uei", "side", "code_type", "code", "obl_lifetime"],
        filter="uei IS NOT NULL AND code IS NOT NULL").to_reader())
    con.execute("CREATE TABLE lanes AS SELECT * FROM _l")
    con.unregister("_l")

    sam = lance.dataset(SAM_URI, storage_options=opt)
    con.register("_s", sam.scanner(columns=["uei", "primary_naics", "naics_codes"],
                                   filter="uei IS NOT NULL").to_reader())
    con.execute("CREATE TABLE sam AS SELECT * FROM _s")
    con.unregister("_s")

    n_edges = con.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
    print(f"edges: {n_edges:,}  lanes: "
          f"{con.execute('SELECT COUNT(*) FROM lanes').fetchone()[0]:,}  sam: "
          f"{con.execute('SELECT COUNT(*) FROM sam').fetchone()[0]:,}", flush=True)

    # Recipient-side denominators (per side, all codes).
    con.execute("""
    CREATE TABLE lane_totals AS
    SELECT uei,
           SUM(obl_lifetime) FILTER (side = 'prime') AS recipient_prime_lane_obl_total,
           SUM(obl_lifetime) FILTER (side = 'sub')   AS recipient_sub_lane_obl_total
    FROM lanes GROUP BY 1
    """)

    # naics_codes may be a LIST or a delimited VARCHAR — normalize to a list expr once.
    nc_type = con.execute(
        "SELECT typeof(naics_codes) FROM sam WHERE naics_codes IS NOT NULL LIMIT 1").fetchone()
    nc_is_list = bool(nc_type) and nc_type[0].upper().startswith(("VARCHAR[", "LIST"))
    nc_expr = "naics_codes" if nc_is_list else "string_split(naics_codes, ',')"
    print(f"sam.naics_codes type: {nc_type[0] if nc_type else 'ALL NULL'} -> list={nc_is_list}",
          flush=True)

    # Prime-side declared codes: entity-level context columns (verbatim, no explosion).
    con.execute(f"""
    CREATE TABLE prime_declared AS
    SELECT uei AS prime_awardee_uei,
           trim(primary_naics)                                   AS prime_awardee_sam_primary_naics,
           CASE WHEN naics_codes IS NOT NULL
                THEN list_transform({nc_expr}, c -> trim(c)) END AS prime_awardee_sam_registered_naics
    FROM sam
    """)

    # Dedup happens ONCE at the per-UEI signal level (small), restricted to the actual
    # subawardee universe — the edge join then CANNOT create grain duplicates, so the
    # 100M-row exploded set never passes through a DISTINCT hash (which OOM'd at 24GB).
    con.execute(f"""
    CREATE TABLE recipient_codes AS
    WITH raw AS (
        SELECT uei,
               CASE side WHEN 'prime' THEN 'awarded_prime_contracts_in_code'
                         ELSE 'delivered_subawards_under_code' END AS code_source,
               code_type, code,
               obl_lifetime AS lane_obl_lifetime
        FROM lanes
        UNION ALL
        SELECT uei, 'sam_primary_naics', 'naics', trim(primary_naics), NULL
        FROM sam WHERE primary_naics IS NOT NULL AND trim(primary_naics) != ''
        UNION ALL
        SELECT uei, 'sam_registered_naics', 'naics', trim(c.code), NULL
        FROM sam, UNNEST({nc_expr}) AS c(code)
        WHERE naics_codes IS NOT NULL AND trim(c.code) != ''
    )
    SELECT r.uei, r.code_source, r.code_type, r.code,
           MAX(r.lane_obl_lifetime) AS lane_obl_lifetime
    FROM raw r
    SEMI JOIN (SELECT DISTINCT subawardee_uei FROM edges) s ON s.subawardee_uei = r.uei
    WHERE r.code IS NOT NULL AND r.code != ''
    GROUP BY 1, 2, 3, 4
    """)
    print(f"recipient_codes (deduped, subawardee-restricted): "
          f"{con.execute('SELECT COUNT(*) FROM recipient_codes').fetchone()[0]:,}", flush=True)

    con.execute("""
    CREATE TABLE evidence AS
    SELECT e.subaward_unique_key, e.prime_award_unique_key, e.prime_awardee_uei,
           e.subawardee_uei, e.subaward_amount, e.subaward_action_date,
           e.prime_award_naics_code, e.prime_award_product_or_service_code,
           r.code_source, r.code_type, r.code, r.lane_obl_lifetime,
           lt.recipient_prime_lane_obl_total,
           lt.recipient_sub_lane_obl_total,
           pd.prime_awardee_sam_primary_naics,
           pd.prime_awardee_sam_registered_naics
    FROM edges e
    JOIN recipient_codes r ON r.uei = e.subawardee_uei
    LEFT JOIN lane_totals lt ON lt.uei = e.subawardee_uei
    LEFT JOIN prime_declared pd ON pd.prime_awardee_uei = e.prime_awardee_uei
    UNION ALL
    SELECT e.subaward_unique_key, e.prime_award_unique_key, e.prime_awardee_uei,
           e.subawardee_uei, e.subaward_amount, e.subaward_action_date,
           e.prime_award_naics_code, e.prime_award_product_or_service_code,
           'subaward_reported_naics', 'naics', trim(e.sub_naics), NULL,
           lt.recipient_prime_lane_obl_total,
           lt.recipient_sub_lane_obl_total,
           pd.prime_awardee_sam_primary_naics,
           pd.prime_awardee_sam_registered_naics
    FROM edges e
    LEFT JOIN lane_totals lt ON lt.uei = e.subawardee_uei
    LEFT JOIN prime_declared pd ON pd.prime_awardee_uei = e.prime_awardee_uei
    WHERE e.sub_naics IS NOT NULL AND trim(e.sub_naics) != ''
    """)

    n = con.execute("SELECT COUNT(*) FROM evidence").fetchone()[0]
    by = con.execute(
        "SELECT code_source, COUNT(*) FROM evidence GROUP BY 1 ORDER BY 2 DESC").fetchall()
    covered = con.execute(
        "SELECT COUNT(DISTINCT subaward_unique_key) FROM evidence").fetchone()[0]
    print(f"evidence rows: {n:,}  by code_source: {by}", flush=True)
    print(f"edges with >=1 code signal: {covered:,} / {n_edges:,} "
          f"({100*covered/n_edges:.1f}%)", flush=True)

    dup = con.execute("""SELECT COUNT(*) FROM (
        SELECT subaward_unique_key, code_source, code_type, code
        FROM evidence GROUP BY 1,2,3,4 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup == 0, f"grain not unique: {dup} dups"

    built_from = (f"usaspending_subaward_canonical:v{sub.version}|"
                  f"gtm_entity_code_lanes:v{lanes.version}|gtm_sam_entities:v{sam.version}")
    res = con.execute(f"""
        SELECT *, DATE '{as_of}' AS as_of, '{built_from}' AS built_from_version,
               '{PARAM_SET_ID}' AS param_set_id FROM evidence""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE_COLS],
                               storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    opt = so()
    ds = lance.dataset(OUT, storage_options=opt)
    rows = ds.count_rows()
    idx = [i["name"] for i in ds.list_indices()]
    demonstrated = ds.count_rows(
        filter="code_source IN ('awarded_prime_contracts_in_code','delivered_subawards_under_code')")
    prime_ctx = ds.count_rows(filter="prime_awardee_sam_primary_naics IS NOT NULL")
    ok = rows > 2_000_000 and demonstrated > 0 and prime_ctx > 0 and all(
        any(c in n for n in idx) for c in BTREE_COLS)
    print(f"{OUT}: rows={rows:,} demonstrated={demonstrated:,} "
          f"prime_declared_ctx={prime_ctx:,} indices={idx} -> {'OK' if ok else 'FAIL'}",
          flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
