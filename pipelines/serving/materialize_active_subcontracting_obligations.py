"""Serving build — `govcon_active_subcontracting_obligations`.

SoR  s3://data-sink/active/govcon_active_subcontracting_obligations/  (Lance v2.1; derived, snapshot-overwrite).

WHAT THIS IS
One row per ACTIVE prime contract award that carries a FAR 52.219-9 subcontracting-plan
obligation (`has_subcontracting_plan = TRUE` in govcon_active_awards), enriched with the
ACTUAL socioeconomic designation distribution of who the prime subcontracts to (from FSRS
contract_subaward ⨝ govcon_subawardee_designations). The GTM target list of active primes
legally obligated to subcontract, with their realized small-/disadvantaged-business mix —
answered entirely from STRUCTURED data (no PDF/text extraction; FPDS records the plan flag
directly, and a Phase-1.5 sample proved attachment text is a worse signal than this field).

SOURCES
  govcon_active_awards               → obligated prime awards + plan fields + recipient identity + prime self-cert flags
  usaspending_api_fresh/contract_subaward → the realized subawards under each prime (FSRS)
  govcon_subawardee_designations     → per-subawardee socioeconomic designation flags (verbatim names)

GRAIN  1 row / contract_award_unique_key. Idempotent snapshot-overwrite.
Designation flag NAMES are verbatim from govcon_active_awards / govcon_subawardee_designations
(zero join drift). Prime self-cert flags carry their bare verbatim names; the realized-sub
rollups are namespaced `sub_n_<flag>` (distinct subawardees) and `sub_amt_<flag>` ($).

    doppler run --project core-x --config prd -- python pipelines/serving/materialize_active_subcontracting_obligations.py
    doppler run --project core-x --config prd -- python pipelines/serving/materialize_active_subcontracting_obligations.py --verify
"""
from __future__ import annotations

import os
import sys

ACTIVE = "s3://data-sink/active"
GAA_URI = f"{ACTIVE}/govcon_active_awards/"
SUBAWARD_URI = f"{ACTIVE}/usaspending_subaward_canonical/"  # repointed: reconciled BULK∪FRESH contract-subaward canonical (already deduped to (prime,subaward_number); was usaspending_api_fresh/contract_subaward)
SUBDES_URI = f"{ACTIVE}/govcon_subawardee_designations/"
SERVING_URI = os.environ.get("SUBK_OBLIGATIONS_URI", f"{ACTIVE}/govcon_active_subcontracting_obligations/")
DATA_STORAGE_VERSION = "2.1"
DUCK_MEM = os.environ.get("DUCK_MEM", "14GB")

# Prime self-cert designation flags (verbatim govcon_active_awards columns).
PRIME_FLAGS = [
    "service_disabled_veteran_owned_business", "veteran_owned_business",
    "women_owned_small_business", "economically_disadvantaged_women_owned_small_business",
    "woman_owned_business", "historically_underutilized_business_zone_hubzone_firm",
    "c8a_program_participant", "small_disadvantaged_business",
    "self_certified_small_disadvantaged_business", "sba_certified_8a_joint_venture",
    "joint_venture_women_owned_small_business", "emerging_small_business",
    "minority_owned_business", "other_minority_owned_business",
]
# Subawardee designation flags that govcon_subawardee_designations actually SOURCES
# (the two unsourceable ones — small_disadvantaged_business, emerging_small_business — are
# NULL there, so they are excluded from the realized-sub rollups).
SUB_FLAGS = [
    "service_disabled_veteran_owned_business", "veteran_owned_business",
    "women_owned_small_business", "economically_disadvantaged_women_owned_small_business",
    "woman_owned_business", "historically_underutilized_business_zone_hubzone_firm",
    "c8a_program_participant", "self_certified_small_disadvantaged_business",
    "minority_owned_business", "joint_venture_women_owned_small_business",
]

BTREE_COLS = ["contract_award_unique_key", "recipient_uei", "recipient_parent_uei", "naics_code"]
BITMAP_COLS = (["business_size_code", "subcontracting_plan_code", "has_subcontracting_plan",
                "has_reported_subs", "active_current", "active_potential",
                "prime_any_socioeconomic_designation"] + PRIME_FLAGS)


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
    prime_flag_sel = ",\n            ".join(f"coalesce({f}, FALSE) AS {f}" for f in PRIME_FLAGS)
    prime_any = " OR ".join(f"coalesce({f}, FALSE)" for f in PRIME_FLAGS)
    sub_rollup = ",\n            ".join(
        f"count(DISTINCT CASE WHEN {f} THEN subawardee_uei END) AS sub_n_{f},\n"
        f"            sum(CASE WHEN {f} THEN amt ELSE 0 END) AS sub_amt_{f}"
        for f in SUB_FLAGS)
    sub_n_cols = ", ".join(f"coalesce(r.sub_n_{f}, 0) AS sub_n_{f}, coalesce(r.sub_amt_{f}, 0.0) AS sub_amt_{f}"
                           for f in SUB_FLAGS)
    return f"""
    WITH prime AS (
        SELECT
            contract_award_unique_key, award_id_piid, parent_award_id_piid,
            recipient_uei, recipient_name, recipient_parent_uei, recipient_parent_name,
            recipient_state_code, upper(trim(business_size_code)) AS business_size_code,
            naics_code, naics_description,
            awarding_agency_name, awarding_sub_agency_name, funding_agency_name,
            TRY_CAST(current_total_value_of_award AS DOUBLE)   AS current_total_value_of_award,
            TRY_CAST(potential_total_value_of_award AS DOUBLE) AS potential_total_value_of_award,
            coalesce(active_current, FALSE)   AS active_current,
            coalesce(active_potential, FALSE) AS active_potential,
            coalesce(has_subcontracting_plan, FALSE) AS has_subcontracting_plan,
            subcontracting_plan, subcontracting_plan_code,
            {prime_flag_sel},
            ({prime_any}) AS prime_any_socioeconomic_designation
        FROM gaa
        WHERE (active_current OR active_potential) AND has_subcontracting_plan = TRUE
    ),
    sub_dedup AS (  -- collapse overlapping-window dupes to true (prime, subaward_number) grain
        SELECT cauk, subaward_number,
               arg_max(subawardee_uei, action_date) AS subawardee_uei,
               arg_max(amt, action_date)            AS amt
        FROM (
            SELECT trim(prime_award_unique_key) AS cauk,
                   subaward_number,
                   subawardee_uei,
                   TRY_CAST(subaward_amount AS DOUBLE)    AS amt,
                   coalesce(TRY_CAST(subaward_action_date AS DATE), DATE '1900-01-01') AS action_date
            FROM sub_raw
            WHERE prime_award_unique_key IS NOT NULL AND trim(prime_award_unique_key) <> ''
        )
        GROUP BY cauk, subaward_number
    ),
    sub_des AS (
        SELECT s.cauk, s.subawardee_uei, s.amt,
               coalesce(d.any_socioeconomic_designation, FALSE) AS any_socioeconomic_designation,
               {", ".join(f"d.{f} AS {f}" for f in SUB_FLAGS)}
        FROM sub_dedup s
        LEFT JOIN subdes d ON s.subawardee_uei = d.subawardee_uei
    ),
    roll AS (
        SELECT cauk,
            count(*)                          AS n_subawards,
            count(DISTINCT subawardee_uei)    AS n_distinct_subawardees,
            sum(amt)                          AS total_subaward_amount,
            count(DISTINCT CASE WHEN any_socioeconomic_designation THEN subawardee_uei END) AS n_subs_designated,
            sum(CASE WHEN any_socioeconomic_designation THEN amt ELSE 0 END)                AS amt_to_designated_subs,
            {sub_rollup}
        FROM sub_des
        GROUP BY cauk
    )
    SELECT
        p.*,
        (r.cauk IS NOT NULL) AS has_reported_subs,
        coalesce(r.n_subawards, 0)            AS n_subawards,
        coalesce(r.n_distinct_subawardees, 0) AS n_distinct_subawardees,
        coalesce(r.total_subaward_amount, 0.0) AS total_subaward_amount,
        coalesce(r.n_subs_designated, 0)      AS n_subs_designated,
        coalesce(r.amt_to_designated_subs, 0.0) AS amt_to_designated_subs,
        {sub_n_cols}
    FROM prime p
    LEFT JOIN roll r ON p.contract_award_unique_key = r.cauk
    """


def build() -> dict:
    import duckdb
    import lance

    so = _r2_storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    os.makedirs("/tmp/_subk_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/_subk_spill'")
    con.register("gaa", lance.dataset(GAA_URI, storage_options=so))
    con.register("sub_raw", lance.dataset(SUBAWARD_URI, storage_options=so))
    con.register("subdes", lance.dataset(SUBDES_URI, storage_options=so))

    tbl = con.execute(_build_sql()).fetch_arrow_table()
    rows = tbl.num_rows

    # ── pre-write gates ──
    con.register("out", tbl)
    n_rows, n_distinct = con.execute(
        "SELECT count(*), count(DISTINCT contract_award_unique_key) FROM out").fetchone()
    assert n_rows == n_distinct, f"grain gate: {n_rows} rows != {n_distinct} distinct cauk"
    assert rows > 20_000, f"floor gate: {rows} <= 20000 obligated awards"
    with_subs, total_sub_amt = con.execute(
        "SELECT count(*) FILTER(WHERE has_reported_subs), sum(total_subaward_amount) FROM out").fetchone()
    print(f"materialized rows={rows} with_reported_subs={with_subs} total_subaward_amount={total_sub_amt:,.0f}")

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
    return {"uri": SERVING_URI, "rows": back, "with_reported_subs": with_subs}


def verify() -> None:
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    print(f"{SERVING_URI}  rows={ds.count_rows():,}  cols={len(ds.schema)}")
    print("indices:", sorted(
        (i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))) for i in ds.list_indices()))
    con = duckdb.connect(":memory:"); con.register("d", ds)
    print("\nby business size + reported-subs:")
    print(con.execute("SELECT business_size_code, has_reported_subs, count(*) n, "
                      "round(sum(total_subaward_amount)/1e9,1) AS sub_gb_usd "
                      "FROM d GROUP BY 1,2 ORDER BY n DESC").df().to_string())
    sel = ", ".join(f"count(*) FILTER(WHERE sub_n_{f} > 0) AS primes_subbing_to_{f}" for f in SUB_FLAGS)
    print("\nprimes that actually subcontract to each designation:")
    print(con.execute(f"SELECT {sel} FROM d").df().T.to_string())


if __name__ == "__main__":
    (verify if "--verify" in sys.argv else build)()
