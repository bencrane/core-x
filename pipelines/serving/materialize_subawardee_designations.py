"""Serving build — `govcon_subawardee_designations` (per-subawardee SAM Reps & Certs, decoded).

SoR  s3://data-sink/active/govcon_subawardee_designations/  (Lance v2.1; derived, snapshot-overwrite).

WHAT THIS IS
One row per federal subawardee UEI (the ~25.4k distinct `subawardee_uei` in contract_subaward),
carrying that entity's socioeconomic designations DECODED from SAM Reps & Certs. The 12 boolean
designation columns use govcon_active_awards' recipient self-cert flag NAMES verbatim, so the
frontend filters primes and subs with the IDENTICAL column vocabulary — zero-join socioeconomic
filtering on subawardee profiles. (The directive named 11; minority_owned_business is added to keep
the table consistent with the reported GTM anchor and a strict superset of the prime flag set.)

SOURCES
  contract_subaward          → the subawardee universe (distinct subawardee_uei) + display name
  sam_master_entities        → business_types[] (self-cert) + sba_business_types_string (SBA cert)
  sam_business_type_code_dict → the authoritative CODE→designation legend (seeded separately)

DECODE (validated; see sam_business_type_code_dict + docs/reference/SUBAWARDEE_DESIGNATIONS.md)
  business_types (self-cert, well populated):
    QF→SDVOSB  A5→Veteran  A2→Woman-Owned  8W→WOSB  27→Self-Cert-SDB  23→Minority  8C→JV-WOSB
  sba prefix (SBA-administered cert, ~13% of entities → 8(a)/HUBZone recall-capped ~68%):
    A6→8(a)  XX→HUBZone  A9→WOSB(SBA)  A0→EDWOSB(SBA)

TWO of the 12 govcon_active_awards flag names are NOT sourceable from SAM Reps & Certs and are
written NULL (NOT false — undetermined), with the reason documented:
  - small_disadvantaged_business : the SBA-determined SDB code (A4) is absent from SAM (program
    folded into 8(a)); only self_certified_small_disadvantaged_business (27) exists.
  - emerging_small_business      : an FPDS size-status construct, not a SAM registration cert.

Grain: 1 row/subawardee_uei. Idempotent snapshot-overwrite. BTREE on resolution keys, BITMAP on flags.

    doppler run --project core-x --config prd -- python pipelines/serving/materialize_subawardee_designations.py
    doppler run --project core-x --config prd -- python pipelines/serving/materialize_subawardee_designations.py --verify
"""
from __future__ import annotations

import os
import sys

ACTIVE = "s3://data-sink/active"
SUBAWARD_URI = f"{ACTIVE}/usaspending_api_fresh/contract_subaward/"
SME_URI = f"{ACTIVE}/sam_master_entities/"
SERVING_URI = os.environ.get("SUBAWARDEE_DESIGNATIONS_URI", f"{ACTIVE}/govcon_subawardee_designations/")
DATA_STORAGE_VERSION = "2.1"
DUCK_MEM = os.environ.get("DUCK_MEM", "14GB")

# 11 flags, schema-symmetric with govcon_active_awards. Value = SQL predicate over `bt` (business_types
# list) and `sbap` (2-char SBA-cert prefixes), or NULL::bool for the two unsourceable flags.
FLAG_SQL = {
    "service_disabled_veteran_owned_business":
        "list_contains(bt,'QF')",
    "veteran_owned_business":
        "(list_contains(bt,'A5') OR list_contains(bt,'QF'))",
    "women_owned_small_business":
        "(list_contains(bt,'8W') OR list_contains(sbap,'A9') OR list_contains(sbap,'A0'))",
    "economically_disadvantaged_women_owned_small_business":
        "list_contains(sbap,'A0')",
    "woman_owned_business":
        "(list_contains(bt,'A2') OR list_contains(bt,'8W') OR list_contains(sbap,'A9') OR list_contains(sbap,'A0'))",
    "historically_underutilized_business_zone_hubzone_firm":
        "list_contains(sbap,'XX')",
    "c8a_program_participant":
        "list_contains(sbap,'A6')",
    "small_disadvantaged_business":
        "CAST(NULL AS BOOLEAN)",
    "self_certified_small_disadvantaged_business":
        "list_contains(bt,'27')",
    "minority_owned_business":
        "list_contains(bt,'23')",
    "joint_venture_women_owned_small_business":
        "list_contains(bt,'8C')",
    "emerging_small_business":
        "CAST(NULL AS BOOLEAN)",
}
# the subset that is actually sourced (drives rollups + BITMAP index set)
SOURCED = [k for k, v in FLAG_SQL.items() if v != "CAST(NULL AS BOOLEAN)"]

BTREE_COLS = ["subawardee_uei", "cage_code", "primary_naics", "designation_count"]
BITMAP_COLS = SOURCED + ["any_socioeconomic_designation", "matched_in_sam", "sam_is_active"]


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _build_sql() -> str:
    flags = ",\n      ".join(f"{sql} AS {name}" for name, sql in FLAG_SQL.items())
    any_expr = " OR ".join(f"coalesce({c},FALSE)" for c in SOURCED)
    count_expr = " + ".join(f"CASE WHEN {c} THEN 1 ELSE 0 END" for c in SOURCED)
    return f"""
    WITH sub AS (
        SELECT subawardee_uei AS subawardee_uei,
               arg_max(subawardee_name, subaward_action_date) AS subawardee_name,
               count(*) AS n_subaward_rows
        FROM sub_raw
        WHERE subawardee_uei IS NOT NULL AND subawardee_uei <> ''
        GROUP BY subawardee_uei
    ),
    sam AS (
        SELECT uei,
               legal_business_name AS sam_legal_business_name,
               cage_code, primary_naics, is_active AS sam_is_active,
               coalesce(business_types, []) AS bt,
               list_filter(list_transform(string_split(coalesce(sba_business_types_string,''),'~'),
                           x -> substr(trim(x),1,2)), e -> e <> '') AS sbap
        FROM sme
    ),
    j AS (
        SELECT s.subawardee_uei, s.subawardee_name, s.n_subaward_rows,
               (m.uei IS NOT NULL) AS matched_in_sam,
               m.sam_legal_business_name, m.cage_code, m.primary_naics,
               coalesce(m.sam_is_active, FALSE) AS sam_is_active,
               coalesce(m.bt, []) AS bt,
               coalesce(m.sbap, []) AS sbap
        FROM sub s LEFT JOIN sam m ON s.subawardee_uei = m.uei
    )
    SELECT
      subawardee_uei, subawardee_name, sam_legal_business_name,
      cage_code, primary_naics, sam_is_active, matched_in_sam, n_subaward_rows,
      {flags},
      ({any_expr}) AS any_socioeconomic_designation,
      ({count_expr}) AS designation_count
    FROM j
    """


def build() -> dict:
    import duckdb
    import lance

    so = _r2_storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.register("sub_raw", lance.dataset(SUBAWARD_URI, storage_options=so))
    con.register("sme", lance.dataset(SME_URI, storage_options=so))

    tbl = con.execute(_build_sql()).fetch_arrow_table()
    rows = tbl.num_rows

    # ── pre-write gates ──
    con.register("out", tbl)
    n_uei, n_distinct = con.execute("SELECT count(*), count(DISTINCT subawardee_uei) FROM out").fetchone()
    assert n_uei == n_distinct, f"grain gate: {n_uei} rows != {n_distinct} distinct uei"
    matched, any_d = con.execute(
        "SELECT count(*) FILTER(WHERE matched_in_sam), count(*) FILTER(WHERE any_socioeconomic_designation) FROM out"
    ).fetchone()
    assert rows > 20_000, f"floor gate: {rows} <= 20000"
    print(f"materialized rows={rows} matched_in_sam={matched} any_designation={any_d}")

    lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(SERVING_URI, storage_options=so)
    present = set(ds.schema.names)
    for c in BTREE_COLS:
        if c in present:
            try:
                ds.create_scalar_index(c, index_type="BTREE"); print(f"  BTREE ✓ {c}")
            except Exception as exc:  # noqa: BLE001
                print(f"  WARN BTREE {c}: {exc}")
    for c in BITMAP_COLS:
        if c in present:
            try:
                ds.create_scalar_index(c, index_type="BITMAP"); print(f"  BITMAP ✓ {c}")
            except Exception as exc:  # noqa: BLE001
                print(f"  WARN BITMAP {c}: {exc}")

    # ── post-write integrity gate ──
    back = ds.count_rows()
    assert back == rows, f"write-integrity gate: {back} != {rows}"
    print(f"WROTE {SERVING_URI} rows={back} cols={len(ds.schema)}")
    return {"uri": SERVING_URI, "rows": back, "matched_in_sam": matched, "any_designation": any_d}


def verify() -> None:
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    print(f"{SERVING_URI}  rows={ds.count_rows():,}  cols={len(ds.schema)}")
    print("indices:", sorted(
        (i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))) for i in ds.list_indices()))
    con = duckdb.connect(":memory:"); con.register("d", ds)
    sel = ", ".join(f"count(*) FILTER(WHERE {c}) AS {c}" for c in SOURCED)
    print("\nTRUE counts per sourced flag (full universe):")
    print(con.execute(f"SELECT count(*) AS total, count(*) FILTER(WHERE matched_in_sam) AS matched, "
                      f"count(*) FILTER(WHERE any_socioeconomic_designation) AS any_desig, {sel} FROM d"
                      ).df().T.to_string())


if __name__ == "__main__":
    (verify if "--verify" in sys.argv else build)()
