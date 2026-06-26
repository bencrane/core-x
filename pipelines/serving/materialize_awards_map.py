"""Serving worker — usaspending_awards_map_serving: ONE ROW PER PRIME AWARD ACTION from the
rolling fresh feeds, joined to the address-keyed geocode crosswalk. The award-EVENT read
model behind "won an award over $X in the last N days" — the grain neither rollup table
(company = entity active-obligations rollup, winners = per-entity window SUM) can express.

GRAIN  1 row per positive-dollar PRIME award action (contract_transaction_unique_key).
       PRIME-ONLY — subaward actions are excluded (tiny + under-reported at source; teaming
       intelligence lives on the winners serving table, not here).
SoR    s3://data-sink/active/usaspending_awards_map_serving/  (Lance v2.1; derived, overwrite)
INPUTS usaspending_api_fresh contract_prime_txn (windowed ~730d/2y by action_date; ~5-day
       posting lag at source) ⋈ geocode_xwalk (addr_hash).
AMOUNT SEMANTICS — load-bearing: action_obligated_usd is the SINGLE prime action's obligation
       (federal_action_obligation). De-obligations (< 0) and $0 admin
       mods are EXCLUDED at build: ">$X won" must never match a de-obligation, and a
       multi-action award does NOT aggregate — each action stands alone.
DEDUPE the fresh feeds carry ~12% duplicate transactions across ingest pulls; rows are
       deduped on the transaction key (arbitrary survivor — duplicates are identical).
GEO    state/city/county are RECIPIENT geo (company registration); pop_state/pop_city are
       PRIMARY PLACE OF PERFORMANCE (where the work happens; ~87% populated).
PSC    psc_code is the full Product/Service Code (what the contract BUYS, e.g. 'V111' motor
       freight); psc_category is its leading character (the PSC top-level group — 'V' =
       Transportation/Travel/Relocation). DISTINCT axis from NAICS (the vendor's industry):
       transportation/freight SERVICES coded under PSC 'V' frequently carry a non-48/49 NAICS,
       so a NAICS-only filter misses them. Prime-only (the sub feed carries no PSC).
ACTIVE pop_end = period_of_performance_current_end_date (DATE); is_active = pop_end >= today
       (build-time; prime-only — NULL on subawards, so is_active=true excludes them honestly).
       The window must be wide enough to include older-dated actions whose contracts are still
       in PoP, else is_active undercounts (a contract signed >window days ago can be active
       now). 730d captures ~97% of currently-active contracts.
LEDGER ops.awards_map_serving_runs (HQX_DB_URL_POOLED) on every terminal state.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' \
      python3 pipelines/serving/materialize_awards_map.py <init_ops|build|verify|demo> [window_days]
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

# Repo root on sys.path so the canonical join-key import resolves whether this file is
# run as a script (python3 pipelines/serving/materialize_awards_map.py) or imported.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines._shared.addr_hash import addr_hash_sql  # noqa: E402

ACTIVE = "s3://data-sink/active"
SERVING_URI = os.environ.get("AWARDS_MAP_SERVING_URI", f"{ACTIVE}/usaspending_awards_map_serving/")
PRIME_URI = f"{ACTIVE}/usaspending_api_fresh/contract_prime_txn/"
XWALK_URI = os.environ.get("GEOCODE_XWALK_URI", f"{ACTIVE}/geocode_xwalk/")
# Stage-1 (naics_code, psc_code) -> vertical / work_type / equipment_intensity classification
# (the top-$ 279 combos). LEFT-JOINed per action so the labels become filterable columns here.
NAICS_PSC_MAP_URI = os.environ.get("NAICS_PSC_VERTICAL_MAP_URI", f"{ACTIVE}/naics_psc_vertical_map/")
# Phase-3 award-grain capability profiles (clearance / CMMC / solicitation scope tags / labor).
# LEFT-JOINed by the award key so the capability axes become filterable on the action-grain awards
# dataset — mirrors active.v4 (#752), but MANY:1 here (many actions share one contract_award_unique_key,
# each inherits its award's profile). Project ONLY structured / controlled-vocab columns (scope_summary
# is the CUI column and is NEVER scanned — CUI egress invariant).
CAPABILITY_PROFILES_URI = os.environ.get("GOVCON_CAPABILITY_PROFILES_URI",
                                         f"{ACTIVE}/govcon_award_solicitation_profiles/")
WINDOW_DAYS = int(os.environ.get("AWARDS_WINDOW_DAYS", "730"))
DATA_STORAGE_VERSION = "2.1"
# BTREE: range axes (action_date, award_amount) + resolution keys + high-cardinality geo
# + psc_code (full PSC, ~hundreds of distinct codes).
BTREE_INDEXES = ["action_date", "action_obligated_usd", "winner_uei", "addr_hash",
                 "city", "county", "pop_city", "awarding_sub_agency", "psc_code"]
# BITMAP: low-cardinality filter columns (state 57, agency 67, set_aside 18, type 2,
# psc_category ~30 leading chars, fiscal_year a handful).
BITMAP_INDEXES = ["naics2", "state", "winner_type", "pop_state", "awarding_agency",
                  "set_aside", "is_active", "psc_category", "fiscal_year", "business_size",
                  "action_type", "is_option_exercise",
                  # label axes joined from naics_psc_vertical_map (classified-dictionary bridge).
                  "vertical", "work_type", "equipment_intensity",
                  # Phase-3 capability axes joined from govcon_award_solicitation_profiles (awards.v11).
                  # Low-cardinality bool/enum → BITMAP pushdown. The list axes (solicitation_scope_tags,
                  # labor_categories) are NOT scalar-indexed — matches the winners/active precedent + decoder.
                  "has_extracted_scope", "requires_clearance", "requires_cmmc", "req_clearance_level_max"]

DUCK_MEM = os.environ.get("AWARDS_DUCKDB_MEMORY_LIMIT", "8GB")
DUCK_TMP = os.environ.get("AWARDS_DUCKDB_TEMP_DIR", "/tmp/awards_map_duckdb")
# Lance scalar-index build = an external sort. The default DataFusion external-merge pool is tiny
# and OOMs mid-build over R2 ("Resources exhausted: ExternalSorterMerge"), which on a mode=overwrite
# rebuild leaves the live table HALF-INDEXED (data committed, only some indices built). The fleet
# rule (ARCHITECTURE.md) is to sort in-RAM instead — set BEFORE any lance index call. setdefault so
# an operator can still override on a small box.
os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _duck():
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    os.makedirs(DUCK_TMP, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{DUCK_TMP}'")
    return con


def _assemble(so, window_days: int):
    import lance
    cutoff = (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=window_days)).isoformat()
    p = lance.dataset(PRIME_URI, storage_options=so)
    x = lance.dataset(XWALK_URI, storage_options=so)
    con = _duck()
    con.register("p", p.scanner(
        columns=["contract_transaction_unique_key", "contract_award_unique_key",
                 "recipient_uei", "recipient_name",
                 "recipient_address_line_1", "recipient_city_name", "recipient_county_name",
                 "recipient_state_code", "recipient_zip_4_code", "naics_code", "action_date",
                 "federal_action_obligation", "awarding_agency_name", "awarding_sub_agency_name",
                 "type_of_set_aside_code", "primary_place_of_performance_state_code",
                 "primary_place_of_performance_city_name",
                 "period_of_performance_current_end_date", "product_or_service_code",
                 "contracting_officers_determination_of_business_size",
                 "action_type", "action_type_code"],
        filter=f"action_date >= '{cutoff}'").to_reader())
    con.register("x", x.scanner(columns=["addr_hash", "latitude", "longitude", "match_type"]).to_reader())
    vmap = lance.dataset(NAICS_PSC_MAP_URI, storage_options=so)
    con.register("v", vmap.scanner(columns=["naics_code", "psc_code", "vertical",
                                            "work_type", "equipment_intensity",
                                            "what_was_done"]).to_reader())
    # Phase-3 award-grain capability bridge → re-scannable TABLE (.to_table(), NOT a single-pass
    # reader). Project ONLY structured / controlled-vocab columns; scope_summary (CUI) is never scanned.
    prof = lance.dataset(CAPABILITY_PROFILES_URI, storage_options=so)
    con.register("prof", prof.scanner(columns=[
        "contract_award_unique_key", "has_extracted_scope", "requires_clearance",
        "req_clearance_level_max", "requires_cmmc", "solicitation_scope_tags",
        "top_labor_categories"]).to_table())
    hexpr = addr_hash_sql("street", "city_raw", "state", "zip")
    sql = f"""
    WITH u AS (
        SELECT contract_transaction_unique_key AS award_id,
               contract_award_unique_key AS award_key, recipient_uei AS winner_uei,
               'prime_recipient' AS winner_type, recipient_name AS winner_name,
               recipient_address_line_1 AS street, recipient_city_name AS city_raw,
               recipient_county_name AS county_raw, recipient_state_code AS state_raw,
               recipient_zip_4_code AS zip, naics_code AS naics,
               try_cast(action_date AS DATE) AS adt,
               try_cast(federal_action_obligation AS DOUBLE) AS amt,
               awarding_agency_name AS agency, awarding_sub_agency_name AS sub_agency,
               nullif(trim(type_of_set_aside_code), '') AS set_aside,
               primary_place_of_performance_state_code AS pop_state_raw,
               primary_place_of_performance_city_name AS pop_city_raw,
               try_cast(period_of_performance_current_end_date AS DATE) AS pop_end,
               nullif(upper(trim(product_or_service_code)), '') AS psc_code_raw,
               nullif(trim(contracting_officers_determination_of_business_size), '') AS business_size_raw,
               nullif(trim(action_type), '') AS action_type_raw,
               nullif(trim(action_type_code), '') AS action_type_code_raw
        FROM p WHERE recipient_uei IS NOT NULL AND length(trim(recipient_uei)) > 0
    ),
    -- amount > 0 BEFORE dedupe: ">$X won" must never match a de-obligation or $0 mod.
    pos AS (SELECT * FROM u WHERE amt > 0 AND adt IS NOT NULL),
    deduped AS (
        SELECT * FROM pos
        QUALIFY row_number() OVER (PARTITION BY winner_type, award_id ORDER BY adt DESC) = 1
    ),
    keyed AS (
        SELECT award_id, award_key AS contract_award_unique_key,
               winner_uei, winner_type, winner_name, street,
               upper(trim(city_raw)) AS city, upper(trim(county_raw)) AS county,
               upper(trim(state_raw)) AS state, zip, naics AS naics_code,
               substr(naics, 1, 2) AS naics2, adt AS action_date, amt AS action_obligated_usd,
               -- US federal fiscal year of the action: Oct 1–Sep 30, so FY = year + (month >= Oct).
               (year(adt) + CASE WHEN month(adt) >= 10 THEN 1 ELSE 0 END) AS fiscal_year,
               agency AS awarding_agency, sub_agency AS awarding_sub_agency, set_aside,
               business_size_raw AS business_size,
               -- Action type of THIS action; is_option_exercise = FPDS 'G' (EXERCISE AN OPTION) =
               -- the government committing the next work tranche → the contractor's mobilization event.
               action_type_raw AS action_type,
               COALESCE(upper(action_type_code_raw) = 'G', FALSE) AS is_option_exercise,
               psc_code_raw AS psc_code, nullif(substr(psc_code_raw, 1, 1), '') AS psc_category,
               upper(trim(pop_state_raw)) AS pop_state, upper(trim(pop_city_raw)) AS pop_city,
               pop_end, (pop_end >= current_date) AS is_active,
               {hexpr} AS addr_hash
        FROM deduped
    )
    -- contract_award_unique_key rides in `keyed` only to drive the capability join below; it is the
    -- award-grain JOIN key, not a serving/decoder column, so EXCLUDE it from the output (the decoder
    -- never references it, and a second full-width non-null string column at action scale overflows
    -- the Lance 2.1 encoder's chunk limit).
    SELECT k.* EXCLUDE (contract_award_unique_key), x.latitude, x.longitude, x.match_type,
           v.vertical, v.work_type, v.equipment_intensity, v.what_was_done,
           -- Phase-3 capability axes (awards.v11), MANY:1 by the award key (every action of an award
           -- inherits that award's solicitation profile). Actions whose award has no extracted profile
           -- get has_extracted_scope=FALSE → the gate (has_extracted_scope=true, ANDed in EXECUTE for
           -- any gated clause) excludes them; on ungated queries they surface as 'not applied', never
           -- silently filtered. labor: bridge top_labor_categories → serving labor_categories.
           COALESCE(prof.has_extracted_scope, FALSE) AS has_extracted_scope,
           COALESCE(prof.requires_clearance, FALSE) AS requires_clearance,
           prof.req_clearance_level_max AS req_clearance_level_max,
           COALESCE(prof.requires_cmmc, FALSE) AS requires_cmmc,
           prof.solicitation_scope_tags AS solicitation_scope_tags,
           prof.top_labor_categories AS labor_categories,
           'usaspending_awards_map_serving (derived)' AS source_file,
           now()::VARCHAR AS ingested_at
    FROM keyed k
    LEFT JOIN x USING (addr_hash)
    LEFT JOIN v ON k.naics_code = v.naics_code AND k.psc_code = v.psc_code
    LEFT JOIN prof ON k.contract_award_unique_key = prof.contract_award_unique_key
    """
    tbl = con.execute(sql).fetch_arrow_table()
    con.close()
    return tbl


def _record_run(*, window_days, rows, prime_rows, sub_rows, with_coords, write_mode,
                indices, status, error, started):
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    coord_rate = round(with_coords / rows, 4) if rows else None
    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute("SELECT to_regclass('ops.awards_map_serving_runs')")
            if cur.fetchone()[0] is None:
                cur.execute(Path(__file__).parent.joinpath("ops_awards_map_serving_runs.sql").read_text())
            cur.execute(
                "INSERT INTO ops.awards_map_serving_runs (feed, window_days, rows_written, "
                "prime_rows, sub_rows, with_coords, coord_rate, write_mode, indices_built, "
                "status, error_message, started_at, completed_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("awards_map_serving", window_days, rows, prime_rows, sub_rows, with_coords,
                 coord_rate, write_mode, ",".join(indices), status, error, started,
                 dt.datetime.now(dt.timezone.utc)))
            c.commit()
    except Exception as exc:  # noqa: BLE001
        log(f"WARN: ops.* write failed: {exc}")


def build(window_days: int = WINDOW_DAYS):
    import lance
    import pyarrow.compute as pc
    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    status, error, rows, prime_rows, sub_rows, with_coords = "error", None, 0, 0, 0, 0
    try:
        tbl = _assemble(so, window_days)
        rows = tbl.num_rows
        prime_rows = tbl.filter(pc.equal(tbl.column("winner_type"), "prime_recipient")).num_rows
        sub_rows = rows - prime_rows
        with_coords = tbl.filter(pc.is_valid(tbl.column("latitude"))).num_rows
        log(f"assembled {rows:,} award actions ({prime_rows:,} prime / {sub_rows:,} sub) · "
            f"{with_coords:,} with coords ({(with_coords / rows * 100 if rows else 0):.1f}%)")
        if rows == 0:
            raise RuntimeError("zero award actions assembled")
        # max_rows_per_file: the Lance 2.1 encoder asserts chunk_bytes <= max_chunk_size per column
        # chunk. The awards.v11 capability list columns (solicitation_scope_tags / labor_categories)
        # push a single-fragment write of all 1.1M actions past that limit (the 189K-row active table
        # writes the SAME columns fine — ~6x smaller). Cap the fragment at ~active scale so every
        # per-column chunk stays well under the limit; Lance reads/indexes across fragments natively.
        lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION, storage_options=so,
                            max_rows_per_file=250_000)
        ds = lance.dataset(SERVING_URI, storage_options=so)
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            log(f"  BTREE ✓ {col}")
        for col in BITMAP_INDEXES:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            log(f"  BITMAP ✓ {col}")
        status = "success"
        log(f"DONE → {SERVING_URI} rows={rows:,}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        log(f"FAILED: {error}")
    finally:
        _record_run(window_days=window_days, rows=rows, prime_rows=prime_rows, sub_rows=sub_rows,
                    with_coords=with_coords, write_mode="overwrite",
                    indices=BTREE_INDEXES + BITMAP_INDEXES, status=status, error=error,
                    started=started)
    if status != "success":
        raise SystemExit(1)
    return {"rows": rows, "prime_rows": prime_rows, "sub_rows": sub_rows, "with_coords": with_coords}


def verify():
    import lance
    so = _r2_so()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    cutoff30 = (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=30)).isoformat()
    out = {"uri": SERVING_URI, "rows": ds.count_rows(),
           "with_coords": ds.count_rows(filter="latitude IS NOT NULL"),
           "prime": ds.count_rows(filter="winner_type = 'prime_recipient'"),
           "subaward": ds.count_rows(filter="winner_type = 'subawardee'"),
           "negative_or_zero_amounts": ds.count_rows(filter="action_obligated_usd <= 0"),
           "with_pop_state": ds.count_rows(filter="pop_state IS NOT NULL"),
           "with_set_aside": ds.count_rows(filter="set_aside IS NOT NULL"),
           "with_pop_end": ds.count_rows(filter="pop_end IS NOT NULL"),
           "active_now": ds.count_rows(filter="is_active = true"),
           "with_psc_code": ds.count_rows(filter="psc_code IS NOT NULL"),
           "psc_category_V_transport": ds.count_rows(filter="psc_category = 'V'"),
           "with_fiscal_year": ds.count_rows(filter="fiscal_year IS NOT NULL"),
           "fy2025_actions": ds.count_rows(filter="fiscal_year = 2025"),
           "fy2026_actions": ds.count_rows(filter="fiscal_year = 2026"),
           "option_exercises": ds.count_rows(filter="is_option_exercise = true"),
           "with_vertical_label": ds.count_rows(filter="vertical IS NOT NULL"),
           "with_what_was_done": ds.count_rows(filter="what_was_done IS NOT NULL"),
           "vertical_aerospace_defense": ds.count_rows(filter="vertical = 'Aerospace & Defense'"),
           "with_action_type": ds.count_rows(filter="action_type IS NOT NULL"),
           "columns": len(ds.schema.names), "indices": idx}
    out["acceptance_A_tx_construction_1m_30d"] = ds.count_rows(
        filter=f"naics2 = '23' AND state = 'TX' AND action_obligated_usd >= 1000000.0 "
               f"AND action_date >= DATE '{cutoff30}'")
    print(json.dumps(out, indent=2, default=str))


def demo(window_days: int = WINDOW_DAYS):
    """Sample GeoJSON for the acceptance filter — proof the award-event dots exist."""
    import lance
    so = _r2_so()
    limit = int(os.environ.get("DEMO_LIMIT", "8"))
    cutoff = (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=30)).isoformat()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    flt = (f"naics2 = '23' AND state = 'TX' AND action_obligated_usd >= 1000000.0 "
           f"AND action_date >= DATE '{cutoff}'")
    total = ds.count_rows(filter=flt)
    rows = ds.scanner(columns=["winner_name", "winner_uei", "action_obligated_usd", "action_date",
                               "city", "state", "awarding_agency", "latitude", "longitude"],
                      filter=flt, limit=limit).to_table().to_pylist()
    print(json.dumps({"demo_filter": flt, "matched_total": total, "showing": len(rows),
                      "rows": rows}, indent=2, default=str))


def init_ops():
    import psycopg
    sql = Path(__file__).parent.joinpath("ops_awards_map_serving_runs.sql").read_text()
    with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as c, c.cursor() as cur:
        cur.execute(sql)
        c.commit()
    log("ops DDL applied")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    arg = int(sys.argv[2]) if len(sys.argv) > 2 else WINDOW_DAYS
    if cmd == "build":
        print(json.dumps(build(window_days=arg), indent=2, default=str))
    elif cmd == "verify":
        verify()
    elif cmd == "demo":
        demo(window_days=arg)
    elif cmd == "init_ops":
        init_ops()
    else:
        print(f"unknown command: {cmd} (init_ops|build|verify|demo)")
        sys.exit(2)


if __name__ == "__main__":
    main()
