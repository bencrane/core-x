"""Serving worker — usaspending_winners_map_serving: one row per (winner_uei, winner_type)
for federal-contract winners in the rolling window, joined to the address-keyed geocode
crosswalk so every winner carries a lat/lon dot. The read model behind the sales-demo map.

GRAIN  1 row per (winner_uei, winner_type ∈ {prime_recipient, subawardee}).
SoR    s3://data-sink/active/usaspending_winners_map_serving/  (Lance v2.1; derived, overwrite)
INPUTS prime + subaward fresh feeds (rolling 90d by action_date) ⋈ geocode_xwalk (addr_hash).
       The window lives HERE (a WHERE), not in the crosswalk — extend it freely.
SIGNALS total_obligation (Σ federal_action_obligation / subaward_amount), award_count, naics,
       state — the demo filters ("construction NAICS 23, > $150k").
LEDGER ops.winners_map_serving_runs (HQX_DB_URL_POOLED) on every terminal state.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' \
      python3 pipelines/serving/materialize_winners_map.py <init_ops|build|verify|demo> [window_days]
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

ACTIVE = "s3://data-sink/active"
SERVING_URI = os.environ.get("WINNERS_MAP_SERVING_URI", f"{ACTIVE}/usaspending_winners_map_serving/")
PRIME_URI = f"{ACTIVE}/usaspending_api_fresh/contract_prime_txn/"
SUB_URI = f"{ACTIVE}/usaspending_api_fresh/contract_subaward/"
XWALK_URI = os.environ.get("GEOCODE_XWALK_URI", f"{ACTIVE}/geocode_xwalk/")
WINDOW_DAYS = int(os.environ.get("WINNERS_WINDOW_DAYS", "90"))
DATA_STORAGE_VERSION = "2.1"
BTREE_INDEXES = ["winner_uei", "addr_hash"]
BITMAP_INDEXES = ["naics2", "state", "winner_type"]

DUCK_MEM = os.environ.get("WINNERS_DUCKDB_MEMORY_LIMIT", "6GB")
DUCK_TMP = os.environ.get("WINNERS_DUCKDB_TEMP_DIR", "/tmp/winners_map_duckdb")


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


def addr_hash_sql(street: str, city: str, state: str, zip_: str) -> str:
    """Canonical addr_hash expression. MUST stay byte-identical to the copy in
    pipelines/usaspending/geocode_xwalk.py — it is the join key between them."""
    return ("md5("
            f"upper(regexp_replace(trim(coalesce({street},'')),'\\s+',' ','g'))||'|'||"
            f"upper(trim(coalesce({city},'')))||'|'||"
            f"upper(trim(coalesce({state},'')))||'|'||"
            f"substr(regexp_replace(coalesce({zip_},''),'[^0-9]','','g'),1,5))")


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
        columns=["recipient_uei", "recipient_name", "recipient_address_line_1",
                 "recipient_city_name", "recipient_state_code", "recipient_zip_4_code",
                 "naics_code", "action_date", "federal_action_obligation",
                 "contract_award_unique_key"],
        filter=f"action_date >= '{cutoff}'").to_reader())
    con.register("s", s.scanner(
        columns=["subawardee_uei", "subawardee_name", "subawardee_address_line_1",
                 "subawardee_city_name", "subawardee_state_code", "subawardee_zip_code",
                 "prime_award_naics_code", "subaward_action_date", "subaward_amount",
                 "subaward_number"],
        filter=f"subaward_action_date >= '{cutoff}'").to_reader())
    con.register("x", x.scanner(columns=["addr_hash", "latitude", "longitude", "match_type"]).to_reader())
    hexpr = addr_hash_sql("street", "city", "state", "zip")
    sql = f"""
    WITH u AS (
        SELECT recipient_uei AS winner_uei, 'prime_recipient' AS winner_type,
               recipient_name AS winner_name, recipient_address_line_1 AS street,
               recipient_city_name AS city, recipient_state_code AS state,
               recipient_zip_4_code AS zip, naics_code AS naics, action_date AS adt,
               try_cast(federal_action_obligation AS DOUBLE) AS amt,
               contract_award_unique_key AS award_key
        FROM p WHERE recipient_uei IS NOT NULL AND length(trim(recipient_uei)) > 0
        UNION ALL
        SELECT subawardee_uei, 'subawardee', subawardee_name, subawardee_address_line_1,
               subawardee_city_name, subawardee_state_code, subawardee_zip_code,
               prime_award_naics_code, subaward_action_date,
               try_cast(subaward_amount AS DOUBLE), subaward_number
        FROM s WHERE subawardee_uei IS NOT NULL AND length(trim(subawardee_uei)) > 0
    ),
    agg AS (
        SELECT winner_uei, winner_type,
               arg_max(winner_name, adt) AS winner_name,
               arg_max(street, adt) AS street, arg_max(city, adt) AS city,
               arg_max(state, adt) AS state, arg_max(zip, adt) AS zip,
               arg_max(naics, adt) AS naics_code,
               count(DISTINCT award_key) AS award_count,
               coalesce(sum(amt), 0) AS total_obligation,
               max(adt) AS last_action_date
        FROM u GROUP BY winner_uei, winner_type
    ),
    keyed AS (
        SELECT *, substr(naics_code, 1, 2) AS naics2,
               {hexpr} AS addr_hash
        FROM agg
    )
    SELECT k.winner_uei, k.winner_type, k.winner_name, k.street, k.city,
           upper(trim(k.state)) AS state, k.zip, k.naics_code, k.naics2,
           k.award_count, k.total_obligation, k.last_action_date, k.addr_hash,
           x.latitude, x.longitude, x.match_type,
           'usaspending_winners_map_serving (derived)' AS source_file,
           now()::VARCHAR AS ingested_at
    FROM keyed k LEFT JOIN x USING (addr_hash)
    """
    tbl = con.execute(sql).fetch_arrow_table()
    con.close()
    return tbl


def _record_run(*, window_days, rows, with_coords, write_mode, indices, status, error, started):
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    coord_rate = round(with_coords / rows, 4) if rows else None
    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute("SELECT to_regclass('ops.winners_map_serving_runs')")
            if cur.fetchone()[0] is None:
                cur.execute(Path(__file__).parent.joinpath("ops_winners_map_serving_runs.sql").read_text())
            cur.execute(
                "INSERT INTO ops.winners_map_serving_runs (feed, window_days, rows_written, "
                "with_coords, coord_rate, write_mode, indices_built, status, error_message, "
                "started_at, completed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("winners_map_serving", window_days, rows, with_coords, coord_rate, write_mode,
                 ",".join(indices), status, error, started, dt.datetime.now(dt.timezone.utc)))
            c.commit()
    except Exception as exc:  # noqa: BLE001
        log(f"WARN: ops.* write failed: {exc}")


def build(window_days: int = WINDOW_DAYS):
    import lance
    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    status, error, rows, with_coords = "error", None, 0, 0
    try:
        tbl = _assemble(so, window_days)
        rows = tbl.num_rows
        with_coords = tbl.filter(__import__("pyarrow").compute.is_valid(tbl.column("latitude"))).num_rows
        log(f"assembled {rows:,} winners · {with_coords:,} with coords "
            f"({(with_coords / rows * 100 if rows else 0):.1f}%)")
        if rows == 0:
            raise RuntimeError("zero winners assembled")
        lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
        ds = lance.dataset(SERVING_URI, storage_options=so)
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE", replace=True); log(f"  BTREE ✓ {col}")
        for col in BITMAP_INDEXES:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True); log(f"  BITMAP ✓ {col}")
        status = "success"
        log(f"DONE → {SERVING_URI} rows={rows:,}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        log(f"FAILED: {error}")
    finally:
        _record_run(window_days=window_days, rows=rows, with_coords=with_coords,
                    write_mode="overwrite", indices=BTREE_INDEXES + BITMAP_INDEXES,
                    status=status, error=error, started=started)
    if status != "success":
        raise SystemExit(1)
    return {"rows": rows, "with_coords": with_coords}


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
           "prime": ds.count_rows(filter="winner_type = 'prime_recipient'"),
           "subaward": ds.count_rows(filter="winner_type = 'subawardee'"),
           "columns": len(ds.schema.names), "indices": idx}
    out["demo_construction_gt_150k"] = ds.count_rows(
        filter="naics2 = '23' AND total_obligation >= 150000 AND latitude IS NOT NULL")
    print(json.dumps(out, indent=2, default=str))


def demo(window_days: int = WINDOW_DAYS):
    """Emit a sample GeoJSON FeatureCollection for the demo filter — proof the dots exist."""
    import lance
    so = _r2_so()
    naics2 = os.environ.get("DEMO_NAICS2", "23")
    min_obl = float(os.environ.get("DEMO_MIN_OBL", "150000"))
    limit = int(os.environ.get("DEMO_LIMIT", "10"))
    ds = lance.dataset(SERVING_URI, storage_options=so)
    flt = f"naics2 = '{naics2}' AND total_obligation >= {min_obl} AND latitude IS NOT NULL"
    total = ds.count_rows(filter=flt)
    tbl = ds.scanner(columns=["winner_name", "winner_type", "state", "naics_code",
                              "total_obligation", "award_count", "latitude", "longitude"],
                     filter=flt, limit=limit).to_table().to_pylist()
    fc = {"type": "FeatureCollection",
          "demo_filter": flt, "matched_total": total, "showing": len(tbl),
          "features": [{"type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [r["longitude"], r["latitude"]]},
                        "properties": {k: r[k] for k in ("winner_name", "winner_type", "state",
                                                         "naics_code", "total_obligation", "award_count")}}
                       for r in tbl]}
    print(json.dumps(fc, indent=2, default=str))


def init_ops():
    import psycopg
    sql = Path(__file__).parent.joinpath("ops_winners_map_serving_runs.sql").read_text()
    with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as c, c.cursor() as cur:
        cur.execute(sql); c.commit()
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
        print(f"unknown command: {cmd} (init_ops|build|verify|demo)"); sys.exit(2)


if __name__ == "__main__":
    main()
