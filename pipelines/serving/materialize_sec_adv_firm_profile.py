"""Serving build — `sec_adv_firm_profile`.

SoR  s3://data-sink/active/sec_adv_firm_profile/  (Lance v2.1; derived, snapshot-overwrite).

WHAT THIS IS
One row per RIA/ERA firm (keyed on CRD) recovering the 252-column Form ADV Part 1A base record —
preserved losslessly in `sec_adv_part1.raw_filing` (JSON, all-varchar) but un-queryable there — into
TYPED, BTREE/BITMAP-indexed columns: firm size/headcount, discretionary vs non-discretionary AUM,
account/client counts, the client-type asset-base mix (Item 5D), advisory-activity specialties
(5G), compensation model (5E), private-fund/custody/wrap flags, and disciplinary history (Item 11).
Spec: docs/reference/SEC_ADV_FIRM_PROFILE_MATERIALIZATION_PLAN.md (v2, adversarial-review-hardened).

DATA PLANE (clean-room): DuckDB reads the source Lance dataset via con.register, does 100% of the
json_extract projection + derivations in SQL, → Arrow → lance.write_dataset (overwrite) DIRECT to R2
→ BTREE/BITMAP scalar indexes. Pure function of the source snapshot ⇒ idempotent re-projection.

EMPIRICALLY-LOCKED FIELD SHAPES (probed against the live source — these drive the derivations):
  * `5H` (wrap), `5A`/`5B1`/`5C1` (employees/clients) are MIXED: exact integers for large filers,
    SEC range-bucket STRINGS for small filers ('1-5', 'More than 1000', '101-250'). The integer
    columns carry the exact value where given (TRY_CAST ⇒ NULL on a bucket); the *_band columns
    absorb BOTH modes into one SEC-bucket vocabulary so they are ~100% populated.
  * `5D2{x}` is the SEC Y/N "serves this client type" checkbox (present a,b,c,g–n; absent d,e,f) —
    the primary `serves_<t>` source.
  * `1I` is Y/N (has-website), NOT a URL. `1F3` is the principal-office phone string. `1M`/`1N-CIK`
    are dropped (1M is an unrelated Y/N; 1N-CIK is 0% populated).
  * `6A1` is Y/N (not a count); `9A1a`/`9B1a` are Y/N custody flags; Item-11 are Y/N disclosure flags.

ERA filers (~9,883) lack the IA-only Item-5 economics ⇒ those columns are typed NULL by design;
`economics_reported` / `is_era` surface that at query time.

    doppler run --project core-x --config prd -- python pipelines/serving/materialize_sec_adv_firm_profile.py build
    doppler run --project core-x --config prd -- python pipelines/serving/materialize_sec_adv_firm_profile.py verify
    modal run pipelines/serving/materialize_sec_adv_firm_profile.py::main --cmd build   # control-plane parity
"""
from __future__ import annotations

import os
import sys

import modal

ACTIVE = "s3://data-sink/active"
SRC_URI = os.environ.get("SEC_ADV_PART1_LANCE_URI", f"{ACTIVE}/sec_adv_part1/")
SERVING_URI = os.environ.get("SEC_ADV_FIRM_PROFILE_URI", f"{ACTIVE}/sec_adv_firm_profile/")
FEED = "sec_adv_firm_profile"
DATA_STORAGE_VERSION = "2.1"
DUCK_MEM = os.environ.get("DUCK_MEM", "8GB")

# ── Field plans (suffix, output-name). The asset-base 14 client types (Item 5D a–n). ──
CLIENT_TYPES = [
    ("a", "individuals"), ("b", "hnw_individuals"), ("c", "banks"),
    ("d", "investment_companies"), ("e", "bdc"), ("f", "pooled_vehicles"),
    ("g", "pension"), ("h", "charities"), ("i", "state_muni"),
    ("j", "other_advisers"), ("k", "insurance_co"), ("l", "sovereign_wealth"),
    ("m", "corporations"), ("n", "other"),
]
SERVES_CB = {"a", "b", "c", "g", "h", "i", "j", "k", "l", "m", "n"}  # suffixes where 5D2{x} exists
# primary_client_type tie-break priority (most-specific / highest-signal first).
PRIMARY_PRIORITY = ["insurance_co", "pension", "pooled_vehicles", "investment_companies",
                    "state_muni", "hnw_individuals", "corporations", "banks", "charities",
                    "bdc", "sovereign_wealth", "other_advisers", "individuals", "other"]
ACTIVITIES = [
    (1, "financial_planning"), (2, "pm_individuals"), (3, "pm_investment_companies"),
    (4, "pm_pooled"), (5, "pm_institutional"), (6, "pension_consulting"),
    (7, "adviser_selection"), (8, "newsletters"), (9, "ratings"),
    (10, "market_timing"), (11, "seminars"), (12, "other"),
]
COMP = [(1, "pct_aum"), (2, "hourly"), (3, "subscription"), (4, "fixed"),
        (5, "commissions"), (6, "performance"), (7, "other")]
# Item-11 disclosure flags (umbrella `11` + every sub-item) — any 'Y' ⇒ any_disciplinary.
DISC_KEYS = ["11", "11A1", "11A2", "11B1", "11B2", "11C1", "11C2", "11C3", "11C4", "11C5",
             "11D1", "11D2", "11D3", "11D4", "11D5", "11E1", "11E2", "11E3", "11E4",
             "11F", "11G", "11H1a", "11H1b", "11H1c", "11H2"]

BTREE_COLS = ["crd_number", "lei", "total_regulatory_aum", "discretionary_aum",
              "non_discretionary_aum", "total_employees", "advisory_employees",
              "num_clients", "total_accounts"]
BITMAP_COLS = (
    ["filer_type", "is_ria", "is_era", "economics_reported", "has_website",
     "business_address_state", "aum_band", "employee_band", "client_count_band",
     "primary_client_type", "large_fund_adviser_flag",
     "advises_private_funds", "has_wrap_program", "has_smas", "has_custody",
     "is_broker_dealer", "any_disciplinary"]
    + [f"serves_{n}" for _, n in CLIENT_TYPES]
    + [f"act_{n}" for _, n in ACTIVITIES]
    + [f"comp_{n}" for _, n in COMP]
)
# Derived booleans that MUST be TRUE/FALSE, never NULL (three-valued-logic guard) — excludes the
# categorical/string BITMAP columns. large_fund_adviser_flag is now a coalesced clean bool.
_NONBOOL = {"filer_type", "business_address_state", "aum_band", "employee_band",
            "client_count_band", "primary_client_type"}
BOOL_COLS = [c for c in BITMAP_COLS if c not in _NONBOOL]

# Idempotent ops.* DDL — mirror of pipelines/serving/ops_sec_adv_firm_profile_runs.sql.
_OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sec_adv_firm_profile_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,
    dataset_uri         text,
    source_uri          text,
    source_snapshot_date date,
    rows_written        bigint,
    ia_rows             bigint,
    era_rows            bigint,
    write_mode          text,
    indices_built       text,
    status              text        NOT NULL,
    error_message       text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sec_adv_firm_profile_runs_status_idx      ON ops.sec_adv_firm_profile_runs (status);
CREATE INDEX IF NOT EXISTS sec_adv_firm_profile_runs_recorded_at_idx ON ops.sec_adv_firm_profile_runs (recorded_at DESC);
"""


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


# ── SQL helpers: extract a base CSV item code out of the raw_filing JSON. ──
def _js(k: str) -> str:
    """json_extract_string against the exact item-code key (quoted for spaces/dashes)."""
    return f"""json_extract_string(raw_filing, '$."{k}"')"""


def _s(k: str) -> str:
    return f"nullif(trim({_js(k)}), '')"


def _i(k: str) -> str:
    return f"TRY_CAST(nullif(trim({_js(k)}), '') AS BIGINT)"


def _yn(k: str) -> str:
    # coalesce to FALSE: an absent key makes json_extract_string NULL, and `NULL = 'Y'` is NULL
    # (not FALSE) under SQL three-valued logic — which would leak NULLs into every derived boolean
    # (breaking `WHERE NOT col` / `col = FALSE` and polluting the BITMAP with a spurious NULL bucket).
    return f"coalesce(upper(trim({_js(k)})) = 'Y', FALSE)"


def _int_band(col: str, edges: list[tuple[int, str]], top: str) -> str:
    """Half-open band CASE over an integer column: edges = [(hi, label), ...] ascending; `< hi`."""
    whens = "\n            ".join(f"WHEN {col} < {hi} THEN '{lab}'" for hi, lab in edges)
    return f"CASE WHEN {col} = 0 THEN '0'\n            {whens}\n            ELSE '{top}' END"


def _build_sql() -> str:
    # ---- base CTE: typed atoms (passthrough + json-extracted), no cross-column derivations ----
    base_cols = [
        "crd_number", "sec_number", "lei", "legal_name", "primary_business_name",
        "business_address_city", "business_address_state",
        "business_address_country", "business_address_postal",
        "filer_type",
        "coalesce(upper(trim(large_fund_adviser_flag)) = 'Y', FALSE) AS large_fund_adviser_flag",
        "snapshot_date AS source_snapshot_date",
        f"{_yn('1I')} AS has_website",
        f"{_s('1F3')} AS phone",
        # size / headcount (exact int where given; NULL on a range-bucket filer)
        f"{_i('5A')} AS total_employees", f"{_s('5A')} AS _raw_5A",
        f"{_i('5B1')} AS advisory_employees",
        f"{_i('5B2')} AS registered_reps",
        f"{_i('5B3')} AS iar_count",
        f"{_i('5B4')} AS insurance_agents",
        # AUM & accounts (5F2 — clean integers, verified)
        f"{_i('5F2a')} AS discretionary_aum",
        f"{_i('5F2b')} AS non_discretionary_aum",
        f"{_i('5F2c')} AS total_regulatory_aum",
        f"{_i('5F2d')} AS discretionary_accounts",
        f"{_i('5F2e')} AS non_discretionary_accounts",
        f"{_i('5F2f')} AS total_accounts",
        f"{_i('5C1')} AS num_clients", f"{_s('5C1')} AS _raw_5C1",
        f"{_i('5C2')} AS num_clients_no_aum",
        # business-model / risk flags
        f"{_yn('7B')} AS advises_private_funds",
        f"(nullif(trim({_js('5H')}), '') IS NOT NULL AND upper(trim({_js('5H')})) <> '0') AS has_wrap_program",
        f"{_yn('5I1')} AS has_smas",
        f"({_yn('9A1a')} OR {_yn('9B1a')}) AS has_custody",
        f"{_yn('6A1')} AS _bd_flag",
        f"({' OR '.join(_yn(k) for k in DISC_KEYS)}) AS any_disciplinary",
    ]
    for suf, name in CLIENT_TYPES:
        base_cols.append(f"{_i('5D1' + suf)} AS n_clients_{name}")
        base_cols.append(f"{_i('5D3' + suf)} AS aum_{name}")
        cb = _yn("5D2" + suf) if suf in SERVES_CB else "FALSE"
        base_cols.append(f"{cb} AS _serves_cb_{name}")
    for num, name in ACTIVITIES:
        base_cols.append(f"{_yn('5G' + str(num))} AS act_{name}")
    for num, name in COMP:
        base_cols.append(f"{_yn('5E' + str(num))} AS comp_{name}")

    base = "    " + ",\n    ".join(base_cols)

    # ---- final SELECT: derivations referencing base atoms ----
    helper_excl = ["_raw_5A", "_raw_5C1", "_bd_flag"] + [f"_serves_cb_{n}" for _, n in CLIENT_TYPES]
    exclude = ", ".join(helper_excl)

    aum_band = _int_band(
        "total_regulatory_aum",
        [(25_000_000, "lt_25m"), (100_000_000, "25m_100m"), (500_000_000, "100m_500m"),
         (1_000_000_000, "500m_1b"), (10_000_000_000, "1b_10b"), (50_000_000_000, "10b_50b")],
        "gte_50b")
    # employee_band / client_count_band: SEC-bucket vocabulary absorbing BOTH the exact int and the
    # range-bucket-string filers. Normalize the literal 'More than N' bucket to a snake label.
    emp_int_band = _int_band(
        "total_employees",
        [(6, "1-5"), (11, "6-10"), (51, "11-50"), (251, "51-250"), (501, "251-500"),
         (1001, "501-1000")], "more_than_1000")
    cli_int_band = _int_band(
        "num_clients",
        [(11, "1-10"), (26, "11-25"), (101, "26-100"), (251, "101-250"), (501, "251-500"),
         (1001, "501-1000")], "more_than_1000")

    derived = [
        "(filer_type = 'IA') AS is_ria",
        "(filer_type = 'ERA') AS is_era",
        "(total_regulatory_aum IS NOT NULL) AS economics_reported",
        "CAST(discretionary_aum AS DOUBLE) / nullif(total_regulatory_aum, 0) AS pct_discretionary",
        f"CASE WHEN total_regulatory_aum IS NULL THEN 'unreported' ELSE {aum_band} END AS aum_band",
        # band: prefer exact-int bucket; else the raw SEC bucket string (normalized); else unreported
        f"CASE WHEN total_employees IS NOT NULL THEN {emp_int_band} "
        f"WHEN _raw_5A IS NOT NULL THEN replace(_raw_5A, 'More than 1000', 'more_than_1000') "
        f"ELSE 'unreported' END AS employee_band",
        f"CASE WHEN num_clients IS NOT NULL THEN {cli_int_band} "
        f"WHEN _raw_5C1 IS NOT NULL THEN replace(_raw_5C1, 'More than ', 'more_than_') "
        f"ELSE 'unreported' END AS client_count_band",
        "(_bd_flag OR coalesce(registered_reps, 0) > 0) AS is_broker_dealer",
        "now() AS built_at",
    ]
    for suf, name in CLIENT_TYPES:
        derived.append(
            f"(_serves_cb_{name} OR coalesce(n_clients_{name}, 0) > 0 "
            f"OR coalesce(aum_{name}, 0) > 0) AS serves_{name}")
    # primary_client_type: horizontal greatest() + ordered cascade + 'none' sentinel (NOT argmax).
    greatest_args = ", ".join(f"coalesce(aum_{n}, 0)" for _, n in CLIENT_TYPES)
    cascade = "\n        ".join(
        f"WHEN coalesce(aum_{n}, 0) = greatest({greatest_args}) THEN '{n}'" for n in PRIMARY_PRIORITY)
    derived.append(
        f"CASE WHEN greatest({greatest_args}) = 0 THEN 'none'\n        {cascade}\n        END "
        f"AS primary_client_type")

    derived_sql = "    " + ",\n    ".join(derived)

    return f"""
    WITH base AS (
        SELECT
{base}
        FROM adv
        WHERE crd_number IS NOT NULL
    )
    SELECT
        * EXCLUDE ({exclude}),
{derived_sql}
    FROM base
    """


# ──────────────────────────────────────────────────────────────────────────────
# Build
# ──────────────────────────────────────────────────────────────────────────────
def build() -> dict:
    import datetime as dt

    import duckdb
    import lance

    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error, indices = "error", None, []
    rows = ia = era = 0
    snap = None
    try:
        con = duckdb.connect(":memory:")
        con.execute("PRAGMA threads=4")
        con.execute(f"SET memory_limit='{DUCK_MEM}'")
        # Wide (~140-col) json_extract projection over the large raw_filing column: insertion-order
        # preservation balloons peak memory for no benefit here (we dedup/order nothing). Disable it.
        con.execute("SET preserve_insertion_order=false")
        os.makedirs("/tmp/_adv_profile_spill", exist_ok=True)
        con.execute("SET temp_directory='/tmp/_adv_profile_spill'")
        con.register("adv", lance.dataset(SRC_URI, storage_options=so))

        tbl = con.execute(_build_sql()).fetch_arrow_table()
        rows = tbl.num_rows
        con.register("out", tbl)

        # ── pre-write gates (abort before any write) ──
        n_rows, n_distinct, n_nullcrd = con.execute(
            "SELECT count(*), count(DISTINCT crd_number), count(*) FILTER (WHERE crd_number IS NULL) FROM out"
        ).fetchone()
        assert n_rows == n_distinct, f"grain gate: {n_rows} rows != {n_distinct} distinct crd"
        assert n_nullcrd == 0, f"null-crd gate: {n_nullcrd} null crd_number"
        ia, era = con.execute(
            "SELECT count(*) FILTER (WHERE is_ria), count(*) FILTER (WHERE is_era) FROM out").fetchone()
        assert ia > 25_000, f"IA floor gate: {ia} <= 25000"
        assert rows > 34_000, f"total floor gate: {rows} <= 34000"
        # AUM within-row identity (the real projection check) — IA rows with all three present
        aum_violations = con.execute(
            "SELECT count(*) FROM out WHERE discretionary_aum IS NOT NULL AND non_discretionary_aum "
            "IS NOT NULL AND total_regulatory_aum IS NOT NULL "
            "AND discretionary_aum + non_discretionary_aum <> total_regulatory_aum").fetchone()[0]
        assert aum_violations == 0, f"AUM identity gate: {aum_violations} rows where disc+nondisc != total"
        # non-degenerate columns (anti-silent-wrong): a uniformly-FALSE/empty derived col ⇒ broken projection
        degen = []
        for col, floor in [("comp_pct_aum", 0.5), ("has_wrap_program", 0.0), ("serves_hnw_individuals", 0.0),
                           ("act_pm_institutional", 0.0), ("any_disciplinary", 0.0), ("has_website", 0.0),
                           ("serves_insurance_co", 0.0), ("advises_private_funds", 0.0)]:
            # Numerator AND denominator both IA-scoped — a clean IA TRUE-rate (a cross-population
            # numerator can exceed 1.0 and mask an all-FALSE IA column whose TRUEs come from ERA).
            rate = con.execute(
                f"SELECT count(*) FILTER (WHERE {col} AND is_ria)::DOUBLE / "
                "nullif(count(*) FILTER (WHERE is_ria), 0) FROM out"
            ).fetchone()[0] or 0.0
            if rate <= floor:
                degen.append(f"{col}={rate:.3f}")
        assert not degen, f"non-degenerate gate: columns at/below floor → {degen}"
        # boolean NULL-leak gate: every derived boolean must be FALSE (not NULL) when its source
        # item code is absent. NULL booleans break `WHERE NOT col` and pollute the BITMAP index.
        bnull = con.execute("SELECT " + ", ".join(
            f"count(*) FILTER (WHERE {c} IS NULL)" for c in BOOL_COLS) + " FROM out").fetchone()
        leaked = [f"{BOOL_COLS[i]}={v}" for i, v in enumerate(bnull) if v]
        assert not leaked, f"boolean-NULL gate (must be FALSE not NULL): {leaked}"

        snap = con.execute("SELECT max(source_snapshot_date) FROM out").fetchone()[0]
        print(f"materialized rows={rows} (IA={ia} ERA={era}) snapshot={snap}")

        # ── write + index ──
        lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
        ds = lance.dataset(SERVING_URI, storage_options=so)
        present = set(ds.schema.names)
        for c in BTREE_COLS:
            if c in present:
                try:
                    ds.create_scalar_index(c, index_type="BTREE", replace=True)
                    indices.append(f"BTREE:{c}")
                except Exception as exc:  # noqa: BLE001 — index miss must not fail the load
                    print(f"  WARN BTREE {c}: {exc}")
        for c in BITMAP_COLS:
            if c in present:
                try:
                    ds.create_scalar_index(c, index_type="BITMAP", replace=True)
                    indices.append(f"BITMAP:{c}")
                except Exception as exc:  # noqa: BLE001
                    print(f"  WARN BITMAP {c}: {exc}")

        # ── post-write integrity gate ──
        back = ds.count_rows()
        assert back == rows, f"write-integrity gate: {back} != {rows}"
        print(f"WROTE {SERVING_URI} rows={back} cols={len(ds.schema)} indices={len(indices)}")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        raise
    finally:
        _record_run(dataset_uri=SERVING_URI, source_uri=SRC_URI, source_snapshot_date=snap,
                    rows_written=rows, ia_rows=ia, era_rows=era, write_mode="overwrite",
                    indices=",".join(indices), status=status, error=error,
                    started=started, completed=dt.datetime.now(dt.timezone.utc))

    return {"uri": SERVING_URI, "rows": rows, "ia": ia, "era": era, "indices": len(indices),
            "snapshot": str(snap), "status": status}


# ──────────────────────────────────────────────────────────────────────────────
# Verify (definition of done — §9)
# ──────────────────────────────────────────────────────────────────────────────
def verify() -> None:
    """Definition of done (§9). ENFORCING: accumulates failures and raises at the end — a corrupt
    build must not return exit 0. Gate [8] (pushdown) is advisory (Lance explain format may vary)."""
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    n = ds.count_rows()
    idx = sorted((i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                 for i in ds.list_indices())
    print(f"{SERVING_URI}  rows={n:,}  cols={len(ds.schema)}  indices={len(idx)}")
    con = duckdb.connect(":memory:")
    con.register("d", ds)
    fails: list[str] = []

    # [1] grain
    g = con.execute("SELECT count(*), count(DISTINCT crd_number) FROM d").fetchone()
    ok1 = g[0] == g[1]
    print(f"[1] grain: {g[0]} rows / {g[1]} distinct crd  -> {'OK' if ok1 else 'FAIL'}")
    if not ok1:
        fails.append(f"grain {g[0]}!={g[1]}")

    # [2] per-population floor
    ia, era = con.execute("SELECT count(*) FILTER(WHERE is_ria), count(*) FILTER(WHERE is_era) FROM d").fetchone()
    ok2 = ia > 25_000 and g[0] > 34_000
    print(f"[2] floor: IA={ia} (>25000:{ia>25000})  ERA={era}  total={g[0]} (>34000:{g[0]>34000})  -> {'OK' if ok2 else 'FAIL'}")
    if not ok2:
        fails.append(f"floor IA={ia} total={g[0]}")

    # [3] fill-rate sanity (IA-scoped). total_employees exact ~93% by mixed-mode design; the band
    # must be ~100% because it absorbs both the exact-int and the range-bucket-string filers.
    print("[3] IA fill rates:")
    fill = {}
    for col in ["total_employees", "employee_band", "discretionary_aum", "act_pm_institutional",
                "comp_pct_aum", "client_count_band"]:
        r = con.execute(f"SELECT count(*) FILTER (WHERE {col} IS NOT NULL)::DOUBLE / count(*) "
                        "FROM d WHERE is_ria").fetchone()[0]
        fill[col] = r
        print(f"    {col:24s} {100*r:5.1f}%")
    if fill["employee_band"] < 0.99:
        fails.append(f"employee_band fill {fill['employee_band']:.3f} < 0.99")
    if fill["discretionary_aum"] < 0.90:
        fails.append(f"discretionary_aum fill {fill['discretionary_aum']:.3f} < 0.90")

    # [4] AUM within-row identity (the real projection check)
    bad = con.execute("SELECT count(*) FROM d WHERE discretionary_aum IS NOT NULL AND non_discretionary_aum "
                      "IS NOT NULL AND total_regulatory_aum IS NOT NULL "
                      "AND discretionary_aum+non_discretionary_aum <> total_regulatory_aum").fetchone()[0]
    print(f"[4] AUM identity violations: {bad}  -> {'OK' if bad==0 else 'FAIL'}")
    if bad != 0:
        fails.append(f"AUM identity {bad} violations")

    # [5] non-degenerate columns (IA-scoped TRUE-rate: numerator AND denominator both IA)
    print("[5] derived TRUE-rates (IA):")
    for col, floor in [("comp_pct_aum", 0.5), ("comp_performance", 0.0), ("has_wrap_program", 0.0),
                       ("serves_pension", 0.0), ("serves_insurance_co", 0.0), ("advises_private_funds", 0.0),
                       ("any_disciplinary", 0.0), ("has_custody", 0.0), ("is_broker_dealer", 0.0),
                       ("has_website", 0.0)]:
        r = con.execute(f"SELECT count(*) FILTER(WHERE {col} AND is_ria)::DOUBLE / "
                        "nullif(count(*) FILTER(WHERE is_ria),0) FROM d").fetchone()[0] or 0.0
        flag = "" if r > floor else "  <-- DEGENERATE"
        print(f"    {col:24s} {100*r:5.1f}%{flag}")
        if r <= floor:
            fails.append(f"degenerate {col}={r:.3f}")

    # [5b] boolean NULL-leak (every derived bool must be FALSE not NULL — three-valued-logic guard)
    bnull = con.execute("SELECT " + ", ".join(
        f"count(*) FILTER (WHERE {c} IS NULL)" for c in BOOL_COLS) + " FROM d").fetchone()
    leaked = [f"{BOOL_COLS[i]}={v}" for i, v in enumerate(bnull) if v]
    print(f"[5b] boolean NULL leak: {'none' if not leaked else leaked}")
    if leaked:
        fails.append(f"boolean NULLs: {leaked[:8]}")

    # [6] golden row — MetLife Investment Management, LLC (hard invariants)
    row = con.execute(
        "SELECT discretionary_aum, total_employees, serves_insurance_co, aum_insurance_co, "
        "act_pm_institutional, comp_performance, advises_private_funds, has_wrap_program, "
        "primary_client_type, employee_band, aum_band "
        "FROM d WHERE legal_name = 'METLIFE INVESTMENT MANAGEMENT, LLC'").fetchone()
    print(f"[6] golden row (MetLife): {row}")
    if not row:
        fails.append("golden row MetLife not found")
    else:
        for ix, want in {0: 496_466_586_170, 1: 925, 2: True, 4: True, 5: True, 6: True}.items():
            if row[ix] != want:
                fails.append(f"golden col[{ix}]={row[ix]} != {want}")

    # [7] index presence
    present_cols = set(ds.schema.names)
    planned = [c for c in (BTREE_COLS + BITMAP_COLS) if c in present_cols]
    ok7 = len(idx) >= len(planned)
    print(f"[7] indices: {len(idx)} built / {len(planned)} planned-present  -> {'OK' if ok7 else 'FAIL'}")
    if not ok7:
        fails.append(f"indices {len(idx)} < {len(planned)} planned")

    # [8] pushdown proof — advisory (Lance planner; DuckDB EXPLAIN never surfaces the index node)
    try:
        plan = ds.scanner(
            filter="discretionary_aum > 1000000000 AND comp_performance AND advises_private_funds",
            columns=["crd_number"]).explain_plan(True)
        hit = ("ScalarIndexQuery" in plan) or ("MaterializeIndex" in plan)
        print(f"[8] pushdown: {'ScalarIndexQuery/MaterializeIndex present (advisory)' if hit else 'NOT FOUND (advisory)'}")
        if not hit:
            print(plan[:600])
    except Exception as exc:  # noqa: BLE001
        print(f"[8] pushdown explain unavailable (advisory): {exc}")

    print("\nby aum_band (IA):")
    for band, cnt in con.execute(
            "SELECT aum_band, count(*) n FROM d WHERE is_ria GROUP BY 1 ORDER BY n DESC").fetchall():
        print(f"    {band:12s} {cnt}")

    if fails:
        raise AssertionError(f"verify(): {len(fails)} gate failure(s) -> {fails}")
    print("\nVERIFY OK — all enforcing gates passed.")


def _record_run(*, dataset_uri, source_uri, source_snapshot_date, rows_written, ia_rows, era_rows,
                write_mode, indices, status, error, started, completed) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(_OPS_DDL)
            cur.execute(
                """INSERT INTO ops.sec_adv_firm_profile_runs
                   (feed, dataset_uri, source_uri, source_snapshot_date, rows_written, ia_rows,
                    era_rows, write_mode, indices_built, status, error_message, started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (FEED, dataset_uri, source_uri, source_snapshot_date, rows_written, ia_rows,
                 era_rows, write_mode, indices, status, error, started, completed))
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* state write failed: {exc}")


def apply_migration() -> dict:
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(_OPS_DDL)
        conn.commit()
        cur.execute("SELECT to_regclass('ops.sec_adv_firm_profile_runs')")
        present = cur.fetchone()[0]
    return {"table": "ops.sec_adv_firm_profile_runs", "present": str(present)}


# ──────────────────────────────────────────────────────────────────────────────
# Control plane — Modal functions (dispatcher-resolvable) + local entrypoint
# ──────────────────────────────────────────────────────────────────────────────
image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17", "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})
app = modal.App("sec-adv-firm-profile", image=image)


@app.function(secrets=[modal.Secret.from_name("r2-credentials"),
                       modal.Secret.from_name("hqx-postgres")],
              timeout=60 * 20, memory=8192, cpu=2.0)
def build_remote(trigger_callback_url: str | None = None) -> dict:
    """Dispatched build. Records ops.* + (best-effort) POSTs the flat-JSON terminal callback."""
    out = {"status": "error"}
    try:
        out = build()
    finally:
        if trigger_callback_url:
            import requests
            try:
                requests.post(trigger_callback_url, json={"status": out.get("status", "error"),
                                                          "feed": FEED, "rows": out.get("rows", 0),
                                                          "dataset_uri": SERVING_URI,
                                                          "as_of": out.get("snapshot")}, timeout=30)
            except Exception as exc:  # noqa: BLE001
                print(f"WARN callback failed: {exc}")
    return out


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10, memory=4096)
def verify_remote() -> None:
    verify()


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def migrate_remote() -> dict:
    return apply_migration()


@app.local_entrypoint()
def main(cmd: str = "build") -> None:
    import json
    fn = {"build": build, "verify": verify, "migrate": apply_migration}[cmd]
    res = fn()
    if res is not None:
        print(json.dumps(res, indent=2, default=str))


# Local execution path (doppler run -- python ...): no Modal runtime required.
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    fn = {"build": build, "verify": verify, "migrate": apply_migration}[cmd]
    res = fn()
    if res is not None:
        import json
        print(json.dumps(res, indent=2, default=str))
