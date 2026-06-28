"""Serving worker — usaspending_contracts_map_serving: ONE ROW PER CONTRACT (the FPDS
prime award, rolled from its transaction ledger to award grain), joined to the address-keyed
geocode crosswalk. The CONTRACT read model behind "contracts worth over $X" — the dollar
threshold filters the SUMMED contract value, not a single action.

GRAIN  1 row per contract_award_unique_key (FPDS PIID+agency composite). PRIME-ONLY —
       subaward actions have no PIID, so they cannot key a contract and are excluded.
SoR    s3://data-sink/active/usaspending_contracts_map_serving/  (Lance v2.1; derived, overwrite)
INPUTS usaspending_api_fresh contract_prime_txn (windowed ~730d/2y by action_date; ~5-day
       posting lag at source) ⋈ geocode_xwalk (addr_hash) ⋈ naics_psc_vertical_map (naics,psc)
       ⋈ govcon_award_solicitation_profiles (award key, 1:1).
MONEY SEMANTICS — load-bearing (this is the ENTIRE point of the table). Roll the transaction
       ledger to award grain so the $-threshold filters the SUMMED contract value:
         1. project + alias from prime, cast VARCHAR→typed via try_cast;
         2. keep rows WHERE recipient_uei non-empty AND contract_award_unique_key non-empty
            AND action_date >= cutoff;
         3. DEDUP transactions on contract_transaction_unique_key (the prime feed carries ~12%
            duplicate transactions across ingest pulls; summing without dedup double-counts) —
            QUALIFY row_number() OVER (PARTITION BY contract_transaction_unique_key
            ORDER BY action_date DESC) = 1;
         4. GROUP BY contract_award_unique_key:
              contract_obligated_usd = sum(federal_action_obligation)  [NET — de-obligations
                  INCLUDED so a deobligated contract reads its true CURRENT value];
              contract_ceiling_usd   = arg_max(base_and_all_options_value, action_date);
              identity / naics / psc / agency / address / dates via arg_max(col, action_date);
              action_count = count(*);  first = min(action_date);  last = max(action_date);
         5. HAVING sum(...) > 0  (drop fully-deobligated / cancelled contracts);
         6. derive naics2, psc_category, fiscal_year(first_action_date), is_active, addr_hash;
         7. LEFT JOIN the crosswalk / vertical-map / capability-profile.
GEO    state/county are RECIPIENT geo (company registration); pop_state is PRIMARY PLACE OF
       PERFORMANCE (where the work happens; ~87% populated).
PSC    psc_code is the full Product/Service Code; psc_category is its leading character.
ACTIVE pop_current_end = period_of_performance_current_end_date (DATE); is_active = pop_current_end
       >= today (build-time). The window must be wide enough to include older-dated FIRST actions
       whose contracts are still in PoP — 730d captures ~97% of currently-active contracts.
CUI    EGRESS INVARIANT — only structured / controlled-vocab capability columns cross into the
       serving table. scope_summary / evidence_quote / requirement_detail are NEVER scanned.
LEDGER ops.contracts_map_serving_runs (HQX_DB_URL_POOLED) on every terminal state.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' \
      python3 pipelines/serving/materialize_contracts_map.py <init_ops|build|verify|demo> [window_days]
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

# Repo root on sys.path so the canonical join-key import resolves whether this file is
# run as a script (python3 pipelines/serving/materialize_contracts_map.py) or imported.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines._shared.addr_hash import addr_hash_sql  # noqa: E402

ACTIVE = "s3://data-sink/active"
SERVING_URI = os.environ.get("CONTRACTS_MAP_SERVING_URI",
                             f"{ACTIVE}/usaspending_contracts_map_serving/")
PRIME_URI = f"{ACTIVE}/usaspending_api_fresh/contract_prime_txn/"
XWALK_URI = os.environ.get("GEOCODE_XWALK_URI", f"{ACTIVE}/geocode_xwalk/")
# Stage-1 (naics_code, psc_code) -> vertical / work_type / equipment_intensity classification
# (the top-$ combos). LEFT-JOINed per contract so the labels become filterable columns here.
NAICS_PSC_MAP_URI = os.environ.get("NAICS_PSC_VERTICAL_MAP_URI", f"{ACTIVE}/naics_psc_vertical_map/")
# Phase-3 award-grain capability profiles (clearance / CMMC / solicitation scope tags / labor).
# LEFT-JOINed 1:1 by the award key (one profile per contract). Project ONLY structured /
# controlled-vocab columns (scope_summary is the CUI column and is NEVER scanned — CUI egress invariant).
CAPABILITY_PROFILES_URI = os.environ.get("GOVCON_CAPABILITY_PROFILES_URI",
                                         f"{ACTIVE}/govcon_award_solicitation_profiles/")
WINDOW_DAYS = int(os.environ.get("CONTRACTS_WINDOW_DAYS", "730"))
DATA_STORAGE_VERSION = "2.1"
# BTREE: range axes (contract_obligated_usd, contract_ceiling_usd, last_action_date, action_count)
# + resolution keys (winner_uei, contract_award_unique_key) + high-cardinality codes (naics_code,
# psc_code, awarding_sub_agency).
BTREE_INDEXES = ["contract_obligated_usd", "contract_ceiling_usd", "naics_code", "psc_code",
                 "awarding_sub_agency", "last_action_date", "winner_uei",
                 "contract_award_unique_key", "action_count"]
# BITMAP: low-cardinality filter columns + the GTM label axes + the Phase-3 capability bool/enum axes.
# The list axes (capability_tags, labor_categories) are NEVER scalar-indexed — matches the
# winners/active/awards precedent + decoder.
BITMAP_INDEXES = ["naics2", "psc_category", "state", "pop_state", "awarding_agency",
                  "set_aside", "business_size", "vertical", "work_type", "equipment_intensity",
                  "is_active", "fiscal_year",
                  "has_extracted_scope", "requires_clearance", "requires_cmmc",
                  "req_clearance_level_max"]

DUCK_MEM = os.environ.get("CONTRACTS_DUCKDB_MEMORY_LIMIT", "8GB")
DUCK_TMP = os.environ.get("CONTRACTS_DUCKDB_TEMP_DIR", "/tmp/contracts_map_duckdb")
# Lance scalar-index build = an external sort. The default DataFusion external-merge pool is tiny
# and OOMs mid-build over R2 ("Resources exhausted: ExternalSorterMerge"), which on a mode=overwrite
# rebuild leaves the live table HALF-INDEXED (data committed, only some indices built). The fleet
# rule (ARCHITECTURE.md) is to sort in-RAM instead — set BEFORE any lance index call. setdefault so
# an operator can still override on a small box. We carry the wide capability list columns
# (capability_tags / labor_categories), so this guard is load-bearing here.
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
                 "award_id_piid", "parent_award_id_piid",
                 "recipient_uei", "recipient_name",
                 "recipient_address_line_1", "recipient_city_name", "recipient_county_name",
                 "recipient_state_code", "recipient_zip_4_code", "naics_code", "action_date",
                 "federal_action_obligation", "base_and_all_options_value",
                 "awarding_agency_name", "awarding_sub_agency_name",
                 "type_of_set_aside_code", "primary_place_of_performance_state_code",
                 "period_of_performance_current_end_date", "product_or_service_code",
                 "contracting_officers_determination_of_business_size"],
        filter=f"action_date >= '{cutoff}'").to_reader())
    con.register("x", x.scanner(columns=["addr_hash", "latitude", "longitude", "match_type"]).to_reader())
    vmap = lance.dataset(NAICS_PSC_MAP_URI, storage_options=so)
    con.register("v", vmap.scanner(columns=["naics_code", "psc_code", "vertical",
                                            "work_type", "equipment_intensity"]).to_reader())
    # Phase-3 award-grain capability bridge → re-scannable TABLE (.to_table(), NOT a single-pass
    # reader). 1:1 by the award key. Project ONLY structured / controlled-vocab columns;
    # scope_summary (CUI) is never scanned.
    prof = lance.dataset(CAPABILITY_PROFILES_URI, storage_options=so)
    con.register("prof", prof.scanner(columns=[
        "contract_award_unique_key", "has_extracted_scope", "requires_clearance",
        "req_clearance_level_max", "requires_cmmc", "solicitation_scope_tags",
        "top_labor_categories"]).to_table())
    hexpr = addr_hash_sql("street", "city_raw", "state", "zip")
    sql = f"""
    WITH u AS (
        SELECT contract_transaction_unique_key AS txn_key,
               contract_award_unique_key AS award_key,
               recipient_uei AS winner_uei, recipient_name AS winner_name,
               award_id_piid AS award_id, parent_award_id_piid AS parent_award_id,
               recipient_address_line_1 AS street, recipient_city_name AS city_raw,
               recipient_county_name AS county_raw, recipient_state_code AS state_raw,
               recipient_zip_4_code AS zip, naics_code AS naics,
               try_cast(action_date AS DATE) AS adt,
               try_cast(federal_action_obligation AS DOUBLE) AS amt,
               try_cast(base_and_all_options_value AS DOUBLE) AS ceiling,
               awarding_agency_name AS agency, awarding_sub_agency_name AS sub_agency,
               nullif(trim(type_of_set_aside_code), '') AS set_aside,
               primary_place_of_performance_state_code AS pop_state_raw,
               try_cast(period_of_performance_current_end_date AS DATE) AS pop_end,
               nullif(upper(trim(product_or_service_code)), '') AS psc_code_raw,
               nullif(trim(contracting_officers_determination_of_business_size), '') AS business_size_raw
        FROM p
        WHERE recipient_uei IS NOT NULL AND length(trim(recipient_uei)) > 0
          AND contract_award_unique_key IS NOT NULL AND length(trim(contract_award_unique_key)) > 0
          AND try_cast(action_date AS DATE) IS NOT NULL
    ),
    -- DEDUP transactions BEFORE the award-grain SUM: the prime feed carries ~12% duplicate
    -- transactions across ingest pulls; summing federal_action_obligation without deduping on the
    -- transaction key double-counts the contract value. Survivor = latest action_date (arbitrary —
    -- duplicates are identical), so the per-action arg_max attrs are stable too.
    deduped AS (
        SELECT * FROM u
        QUALIFY row_number() OVER (PARTITION BY txn_key ORDER BY adt DESC) = 1
    ),
    -- Roll the deduped ledger to CONTRACT grain. contract_obligated_usd is the NET sum of
    -- federal_action_obligation (de-obligations INCLUDED → a deobligated contract reads its true
    -- current value); ceiling/identity/attrs are the latest-action arg_max; action_count is the #
    -- distinct transactions rolled. HAVING net > 0 drops fully-deobligated / cancelled contracts.
    rolled AS (
        SELECT award_key AS contract_award_unique_key,
               sum(amt) AS contract_obligated_usd,
               arg_max(ceiling, adt) AS contract_ceiling_usd,
               count(*) AS action_count,
               arg_max(award_id, adt) AS award_id,
               arg_max(parent_award_id, adt) AS parent_award_id,
               arg_max(winner_uei, adt) AS winner_uei,
               arg_max(winner_name, adt) AS winner_name,
               arg_max(naics, adt) AS naics_code,
               arg_max(psc_code_raw, adt) AS psc_code,
               arg_max(agency, adt) AS awarding_agency,
               arg_max(sub_agency, adt) AS awarding_sub_agency,
               arg_max(set_aside, adt) AS set_aside,
               arg_max(business_size_raw, adt) AS business_size,
               arg_max(street, adt) AS street,
               arg_max(city_raw, adt) AS city_raw,
               arg_max(county_raw, adt) AS county_raw,
               arg_max(state_raw, adt) AS state_raw,
               arg_max(zip, adt) AS zip,
               arg_max(pop_state_raw, adt) AS pop_state_raw,
               arg_max(pop_end, adt) AS pop_current_end,
               min(adt) AS first_action_date,
               max(adt) AS last_action_date
        FROM deduped
        GROUP BY award_key
        HAVING sum(amt) > 0
    ),
    keyed AS (
        SELECT contract_award_unique_key,
               contract_obligated_usd, contract_ceiling_usd, action_count,
               award_id, parent_award_id, winner_uei, winner_name,
               naics_code, substr(naics_code, 1, 2) AS naics2,
               psc_code, nullif(substr(psc_code, 1, 1), '') AS psc_category,
               upper(trim(state_raw)) AS state, upper(trim(pop_state_raw)) AS pop_state,
               awarding_agency, awarding_sub_agency, set_aside, business_size,
               first_action_date, last_action_date, pop_current_end,
               -- US federal fiscal year of the FIRST action: Oct 1–Sep 30, so FY = year + (month >= Oct).
               (year(first_action_date)
                    + CASE WHEN month(first_action_date) >= 10 THEN 1 ELSE 0 END) AS fiscal_year,
               (pop_current_end >= current_date) AS is_active,
               {hexpr} AS addr_hash
        FROM rolled
    ),
    -- Defensive 1:1 guard. The capability bridge SHOULD be unique on the award key (one profile per
    -- contract), but a LEFT JOIN that fanned out would DUPLICATE contract rows and silently break the
    -- award grain. Collapse to one profile per award key — preferring the row that carries an extracted
    -- scope — so the join is provably 1:1 regardless of the bridge's actual cardinality.
    prof1 AS (
        SELECT * FROM prof
        QUALIFY row_number() OVER (
            PARTITION BY contract_award_unique_key
            ORDER BY has_extracted_scope DESC NULLS LAST) = 1
    )
    SELECT x.longitude, x.latitude,
           k.contract_award_unique_key, k.award_id, k.parent_award_id,
           k.winner_uei, k.winner_name,
           k.contract_obligated_usd, k.contract_ceiling_usd, k.action_count,
           k.naics_code, k.naics2, k.psc_code, k.psc_category,
           k.state, k.pop_state, k.awarding_agency, k.awarding_sub_agency,
           k.set_aside, k.business_size,
           v.vertical, v.work_type, v.equipment_intensity,
           k.fiscal_year, k.is_active,
           k.first_action_date, k.last_action_date, k.pop_current_end,
           -- Phase-3 capability axes (contracts.v1), 1:1 by the award key. Contracts with no
           -- extracted profile get has_extracted_scope=FALSE → the gate (has_extracted_scope=true,
           -- ANDed in EXECUTE for any gated clause) excludes them; on ungated queries they surface as
           -- 'not applied', never silently filtered. labor: bridge top_labor_categories → labor_categories.
           COALESCE(prof.has_extracted_scope, FALSE) AS has_extracted_scope,
           COALESCE(prof.requires_clearance, FALSE) AS requires_clearance,
           COALESCE(prof.requires_cmmc, FALSE) AS requires_cmmc,
           prof.req_clearance_level_max AS req_clearance_level_max,
           -- Cap the free-text list axes at 50 elements (mirrors winners' COVERED_AWARD_KEYS_CAP) so a
           -- pathological profile cannot blow a single Lance v2.1 column chunk past max_chunk_size.
           list_slice(prof.solicitation_scope_tags, 1, 50) AS capability_tags,
           list_slice(prof.top_labor_categories, 1, 50) AS labor_categories,
           'usaspending_contracts_map_serving (derived)' AS source_file,
           now()::VARCHAR AS ingested_at
    FROM keyed k
    LEFT JOIN x USING (addr_hash)
    LEFT JOIN v ON k.naics_code = v.naics_code AND k.psc_code = v.psc_code
    LEFT JOIN prof1 prof ON k.contract_award_unique_key = prof.contract_award_unique_key
    """
    tbl = con.execute(sql).fetch_arrow_table()
    con.close()
    return tbl


def _record_run(*, window_days, rows, prime_rows, action_total, with_coords, write_mode,
                indices, status, error, started):
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    coord_rate = round(with_coords / rows, 4) if rows else None
    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute("SELECT to_regclass('ops.contracts_map_serving_runs')")
            if cur.fetchone()[0] is None:
                cur.execute(Path(__file__).parent.joinpath("ops_contracts_map_serving_runs.sql").read_text())
            cur.execute(
                "INSERT INTO ops.contracts_map_serving_runs (feed, window_days, rows_written, "
                "prime_rows, action_total, with_coords, coord_rate, write_mode, indices_built, "
                "status, error_message, started_at, completed_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("contracts_map_serving", window_days, rows, prime_rows, action_total, with_coords,
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
    status, error, rows, prime_rows, action_total, with_coords = "error", None, 0, 0, 0, 0
    try:
        tbl = _assemble(so, window_days)
        rows = tbl.num_rows
        # Every row is a prime contract (prime-only build); prime_rows == rows. action_total is the
        # number of distinct transactions rolled across all contracts (the ledger depth).
        prime_rows = rows
        action_total = int(pc.sum(tbl.column("action_count")).as_py() or 0) if rows else 0
        with_coords = tbl.filter(pc.is_valid(tbl.column("latitude"))).num_rows
        log(f"assembled {rows:,} contracts ({action_total:,} transactions rolled) · "
            f"{with_coords:,} with coords ({(with_coords / rows * 100 if rows else 0):.1f}%)")
        if rows == 0:
            raise RuntimeError("zero contracts assembled")
        # max_rows_per_file: the Lance 2.1 encoder asserts chunk_bytes <= max_chunk_size per column
        # chunk; a 250k-row fragment overran it at ~1M contracts, so bound fragments tighter. 64k rows
        # keeps every per-column page well under the limit (Lance reads/indexes across fragments
        # natively), and the capability lists are independently capped at 50 elements above.
        lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION, storage_options=so,
                            max_rows_per_file=64_000)
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
        _record_run(window_days=window_days, rows=rows, prime_rows=prime_rows,
                    action_total=action_total, with_coords=with_coords, write_mode="overwrite",
                    indices=BTREE_INDEXES + BITMAP_INDEXES, status=status, error=error,
                    started=started)
    if status != "success":
        raise SystemExit(1)
    return {"rows": rows, "prime_rows": prime_rows, "action_total": action_total,
            "with_coords": with_coords}


def verify():
    import lance
    so = _r2_so()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    out = {"uri": SERVING_URI, "rows": ds.count_rows(),
           "with_coords": ds.count_rows(filter="latitude IS NOT NULL"),
           # NET-sum invariant: HAVING > 0 guarantees no contract reads <= 0.
           "negative_or_zero_obligated": ds.count_rows(filter="contract_obligated_usd <= 0"),
           "multi_action_contracts": ds.count_rows(filter="action_count > 1"),
           "with_ceiling": ds.count_rows(filter="contract_ceiling_usd IS NOT NULL"),
           "with_parent_award_id": ds.count_rows(filter="parent_award_id IS NOT NULL"),
           "with_pop_state": ds.count_rows(filter="pop_state IS NOT NULL"),
           "with_set_aside": ds.count_rows(filter="set_aside IS NOT NULL"),
           "with_pop_current_end": ds.count_rows(filter="pop_current_end IS NOT NULL"),
           "active_now": ds.count_rows(filter="is_active = true"),
           "with_psc_code": ds.count_rows(filter="psc_code IS NOT NULL"),
           "psc_category_V_transport": ds.count_rows(filter="psc_category = 'V'"),
           "with_fiscal_year": ds.count_rows(filter="fiscal_year IS NOT NULL"),
           "fy2025_contracts": ds.count_rows(filter="fiscal_year = 2025"),
           "fy2026_contracts": ds.count_rows(filter="fiscal_year = 2026"),
           "with_vertical_label": ds.count_rows(filter="vertical IS NOT NULL"),
           "vertical_aerospace_defense": ds.count_rows(filter="vertical = 'Aerospace & Defense'"),
           "has_extracted_scope": ds.count_rows(filter="has_extracted_scope = true"),
           "requires_clearance": ds.count_rows(filter="requires_clearance = true"),
           "requires_cmmc": ds.count_rows(filter="requires_cmmc = true"),
           "columns": len(ds.schema.names), "indices": idx}
    # Acceptance: TX construction contracts whose SUMMED value tops $1M (the money-rollup payoff —
    # a single-action filter would miss multi-action contracts that only clear $1M in aggregate).
    out["acceptance_A_tx_construction_summed_1m"] = ds.count_rows(
        filter="naics2 = '23' AND state = 'TX' AND contract_obligated_usd >= 1000000.0")
    print(json.dumps(out, indent=2, default=str))


def demo(window_days: int = WINDOW_DAYS):
    """Emit a sample GeoJSON FeatureCollection for the acceptance filter — proof the dots exist."""
    import lance
    so = _r2_so()
    naics2 = os.environ.get("DEMO_NAICS2", "23")
    min_obl = float(os.environ.get("DEMO_MIN_OBL", "1000000"))
    limit = int(os.environ.get("DEMO_LIMIT", "10"))
    ds = lance.dataset(SERVING_URI, storage_options=so)
    flt = f"naics2 = '{naics2}' AND contract_obligated_usd >= {min_obl} AND latitude IS NOT NULL"
    total = ds.count_rows(filter=flt)
    tbl = ds.scanner(columns=["winner_name", "winner_uei", "contract_obligated_usd",
                              "contract_ceiling_usd", "action_count", "last_action_date",
                              "state", "awarding_agency", "latitude", "longitude"],
                     filter=flt, limit=limit).to_table().to_pylist()
    fc = {"type": "FeatureCollection",
          "demo_filter": flt, "matched_total": total, "showing": len(tbl),
          "features": [{"type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [r["longitude"], r["latitude"]]},
                        "properties": {k: r[k] for k in ("winner_name", "winner_uei",
                                                         "contract_obligated_usd", "contract_ceiling_usd",
                                                         "action_count", "last_action_date",
                                                         "state", "awarding_agency")}}
                       for r in tbl]}
    print(json.dumps(fc, indent=2, default=str))


def init_ops():
    import psycopg
    sql = Path(__file__).parent.joinpath("ops_contracts_map_serving_runs.sql").read_text()
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
