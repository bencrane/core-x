"""Serving worker — usaspending_awards_map_serving: ONE ROW PER AWARD ACTION from the
rolling fresh feeds, joined to the address-keyed geocode crosswalk. The award-EVENT read
model behind "won an award over $X in the last N days" — the grain neither rollup table
(company = lifetime, winners = window SUM) can express.

GRAIN  1 row per positive-dollar award action: prime contract transactions
       (contract_transaction_unique_key) ∪ subaward actions (subaward_number|prime key).
SoR    s3://data-sink/active/usaspending_awards_map_serving/  (Lance v2.1; derived, overwrite)
INPUTS usaspending_api_fresh contract_prime_txn + contract_subaward (rolling ~90d by
       action_date; ~5-day posting lag at source) ⋈ geocode_xwalk (addr_hash).
AMOUNT SEMANTICS — load-bearing: award_amount is the SINGLE action's obligation
       (federal_action_obligation / subaward_amount). De-obligations (< 0) and $0 admin
       mods are EXCLUDED at build: ">$X won" must never match a de-obligation, and a
       multi-action award does NOT aggregate — each action stands alone.
DEDUPE the fresh feeds carry ~12% duplicate transactions across ingest pulls; rows are
       deduped on the transaction key (arbitrary survivor — duplicates are identical).
GEO    state/city/county are RECIPIENT geo (company registration); pop_state/pop_city are
       PRIMARY PLACE OF PERFORMANCE (where the work happens; ~87% populated on prime,
       NULL on subawards — the sub feed's PoP columns are empty, verified live).
       set_aside / county are prime-only signals (NULL on sub rows).
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
SUB_URI = f"{ACTIVE}/usaspending_api_fresh/contract_subaward/"
XWALK_URI = os.environ.get("GEOCODE_XWALK_URI", f"{ACTIVE}/geocode_xwalk/")
WINDOW_DAYS = int(os.environ.get("AWARDS_WINDOW_DAYS", "90"))
DATA_STORAGE_VERSION = "2.1"
# BTREE: range axes (action_date, award_amount) + resolution keys + high-cardinality geo.
BTREE_INDEXES = ["action_date", "award_amount", "winner_uei", "addr_hash",
                 "city", "county", "pop_city", "awarding_sub_agency"]
# BITMAP: low-cardinality filter columns (state 57, agency 67, set_aside 18, type 2).
BITMAP_INDEXES = ["naics2", "state", "winner_type", "pop_state", "awarding_agency", "set_aside"]

DUCK_MEM = os.environ.get("AWARDS_DUCKDB_MEMORY_LIMIT", "8GB")
DUCK_TMP = os.environ.get("AWARDS_DUCKDB_TEMP_DIR", "/tmp/awards_map_duckdb")


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
    s = lance.dataset(SUB_URI, storage_options=so)
    x = lance.dataset(XWALK_URI, storage_options=so)
    con = _duck()
    con.register("p", p.scanner(
        columns=["contract_transaction_unique_key", "recipient_uei", "recipient_name",
                 "recipient_address_line_1", "recipient_city_name", "recipient_county_name",
                 "recipient_state_code", "recipient_zip_4_code", "naics_code", "action_date",
                 "federal_action_obligation", "awarding_agency_name", "awarding_sub_agency_name",
                 "type_of_set_aside_code", "primary_place_of_performance_state_code",
                 "primary_place_of_performance_city_name"],
        filter=f"action_date >= '{cutoff}'").to_reader())
    con.register("s", s.scanner(
        columns=["subaward_number", "prime_award_unique_key", "subawardee_uei", "subawardee_name",
                 "subawardee_address_line_1", "subawardee_city_name", "subawardee_state_code",
                 "subawardee_zip_code", "prime_award_naics_code", "subaward_action_date",
                 "subaward_amount", "prime_award_awarding_agency_name",
                 "prime_award_awarding_sub_agency_name"],
        filter=f"subaward_action_date >= '{cutoff}'").to_reader())
    con.register("x", x.scanner(columns=["addr_hash", "latitude", "longitude", "match_type"]).to_reader())
    hexpr = addr_hash_sql("street", "city_raw", "state", "zip")
    sql = f"""
    WITH u AS (
        SELECT contract_transaction_unique_key AS award_id, recipient_uei AS winner_uei,
               'prime_recipient' AS winner_type, recipient_name AS winner_name,
               recipient_address_line_1 AS street, recipient_city_name AS city_raw,
               recipient_county_name AS county_raw, recipient_state_code AS state_raw,
               recipient_zip_4_code AS zip, naics_code AS naics,
               try_cast(action_date AS DATE) AS adt,
               try_cast(federal_action_obligation AS DOUBLE) AS amt,
               awarding_agency_name AS agency, awarding_sub_agency_name AS sub_agency,
               nullif(trim(type_of_set_aside_code), '') AS set_aside,
               primary_place_of_performance_state_code AS pop_state_raw,
               primary_place_of_performance_city_name AS pop_city_raw
        FROM p WHERE recipient_uei IS NOT NULL AND length(trim(recipient_uei)) > 0
        UNION ALL
        SELECT subaward_number || '|' || coalesce(prime_award_unique_key, ''),
               subawardee_uei, 'subawardee', subawardee_name, subawardee_address_line_1,
               subawardee_city_name, NULL, subawardee_state_code, subawardee_zip_code,
               prime_award_naics_code, try_cast(subaward_action_date AS DATE),
               try_cast(subaward_amount AS DOUBLE),
               prime_award_awarding_agency_name, prime_award_awarding_sub_agency_name,
               NULL, NULL, NULL  -- set_aside / PoP: empty at source for subawards (verified)
        FROM s WHERE subawardee_uei IS NOT NULL AND length(trim(subawardee_uei)) > 0
    ),
    -- amount > 0 BEFORE dedupe: ">$X won" must never match a de-obligation or $0 mod.
    pos AS (SELECT * FROM u WHERE amt > 0 AND adt IS NOT NULL),
    deduped AS (
        SELECT * FROM pos
        QUALIFY row_number() OVER (PARTITION BY winner_type, award_id ORDER BY adt DESC) = 1
    ),
    keyed AS (
        SELECT award_id, winner_uei, winner_type, winner_name, street,
               upper(trim(city_raw)) AS city, upper(trim(county_raw)) AS county,
               upper(trim(state_raw)) AS state, zip, naics AS naics_code,
               substr(naics, 1, 2) AS naics2, adt AS action_date, amt AS award_amount,
               agency AS awarding_agency, sub_agency AS awarding_sub_agency, set_aside,
               upper(trim(pop_state_raw)) AS pop_state, upper(trim(pop_city_raw)) AS pop_city,
               {hexpr} AS addr_hash
        FROM deduped
    )
    SELECT k.*, x.latitude, x.longitude, x.match_type,
           'usaspending_awards_map_serving (derived)' AS source_file,
           now()::VARCHAR AS ingested_at
    FROM keyed k LEFT JOIN x USING (addr_hash)
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
        lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
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
           "negative_or_zero_amounts": ds.count_rows(filter="award_amount <= 0"),
           "with_pop_state": ds.count_rows(filter="pop_state IS NOT NULL"),
           "with_set_aside": ds.count_rows(filter="set_aside IS NOT NULL"),
           "columns": len(ds.schema.names), "indices": idx}
    out["acceptance_A_tx_construction_1m_30d"] = ds.count_rows(
        filter=f"naics2 = '23' AND state = 'TX' AND award_amount >= 1000000.0 "
               f"AND action_date >= DATE '{cutoff30}'")
    print(json.dumps(out, indent=2, default=str))


def demo(window_days: int = WINDOW_DAYS):
    """Sample GeoJSON for the acceptance filter — proof the award-event dots exist."""
    import lance
    so = _r2_so()
    limit = int(os.environ.get("DEMO_LIMIT", "8"))
    cutoff = (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=30)).isoformat()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    flt = (f"naics2 = '23' AND state = 'TX' AND award_amount >= 1000000.0 "
           f"AND action_date >= DATE '{cutoff}'")
    total = ds.count_rows(filter=flt)
    rows = ds.scanner(columns=["winner_name", "winner_uei", "award_amount", "action_date",
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
