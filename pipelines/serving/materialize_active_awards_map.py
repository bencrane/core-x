"""Serving worker — govcon_active_awards_map_serving: the FORWARD-LOOKING map read model.

ONE ROW PER ACTIVE PRIME AWARD (award grain — projected from govcon_active_awards, which already
collapsed contract_prime_txn to the latest txn per award and applied the active/done membership
boundary), joined to the address-keyed geocode crosswalk for the map dot layer.

WHY IT EXISTS. The awards map (usaspending_awards_map_serving) is action-grain and BACKWARD-looking
("won an award in the last N days"). This one is award-grain and FORWARD-looking: it carries
`pop_current_end` so the map can answer "which incumbents are about to recompete" — contracts whose
period of performance ends in the next N days. That recompete pool is the GTM prospect list neither
the awards (action) nor winners (rollup) tables can express. The window stays QUERY-DRIVEN: the
decoder's `days_until_expiry` (days_ahead) axis resolves `pop_current_end` against today at request
time — no hardcoded horizon.

GRAIN   1 row per contract_award_unique_key (award). recipient (prime) geo + place of performance.
SoR     s3://data-sink/active/govcon_active_awards_map_serving/  (Lance v2.1; derived, overwrite).
INPUTS  govcon_active_awards ⋈ geocode_xwalk (addr_hash on the recipient address).
LEDGER  ops.active_awards_map_serving_runs (HQX_DB_URL_POOLED) on every terminal state.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' \
      python3 pipelines/serving/materialize_active_awards_map.py <init_ops|build|verify|demo>
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

# Repo root on sys.path so the canonical join-key import resolves whether this file is run as a
# script or imported.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines._shared.addr_hash import addr_hash_sql  # noqa: E402

ACTIVE = "s3://data-sink/active"
SERVING_URI = os.environ.get("ACTIVE_AWARDS_MAP_SERVING_URI", f"{ACTIVE}/govcon_active_awards_map_serving/")
SOURCE_URI = f"{ACTIVE}/govcon_active_awards/"
XWALK_URI = os.environ.get("GEOCODE_XWALK_URI", f"{ACTIVE}/geocode_xwalk/")
# Stage-1 (naics_code, psc_code) -> vertical / work_type / equipment_intensity classification
# (the top-$ 279 combos). LEFT-JOINed per award so the GTM label axes become filterable columns
# here too — mirrors the awards serving build (materialize_awards_map.py).
NAICS_PSC_MAP_URI = os.environ.get("NAICS_PSC_VERTICAL_MAP_URI", f"{ACTIVE}/naics_psc_vertical_map/")
DATA_STORAGE_VERSION = "2.1"

# BTREE: range axes (pop_current_end — the forward recompete axis — + value/obligation) + resolution
# keys + high-cardinality codes.
BTREE_INDEXES = ["pop_current_end", "pop_potential_end", "current_value", "potential_value",
                 "obligated", "winner_uei", "addr_hash", "naics_code", "psc_code",
                 "awarding_sub_agency"]
# BITMAP: low-cardinality filter columns.
BITMAP_INDEXES = ["naics2", "psc_category", "state", "pop_state", "set_aside", "business_size",
                  "has_option_tail", "award_or_idv_flag", "awarding_agency",
                  # label axes joined from naics_psc_vertical_map (classified-dictionary bridge).
                  "vertical", "work_type", "equipment_intensity"]

DUCK_MEM = os.environ.get("ACTIVE_MAP_DUCKDB_MEMORY_LIMIT", "8GB")
DUCK_TMP = os.environ.get("ACTIVE_MAP_DUCKDB_TEMP_DIR", "/tmp/active_awards_map_duckdb")
# Lance scalar-index build = an external sort; the default pool OOMs mid-build over R2 and a
# mode=overwrite then leaves the live table HALF-INDEXED. Sort in-RAM (ARCHITECTURE.md fleet rule).
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


def _assemble(so):
    import lance
    a = lance.dataset(SOURCE_URI, storage_options=so)
    x = lance.dataset(XWALK_URI, storage_options=so)
    con = _duck()
    con.register("a", a.scanner(columns=[
        "contract_award_unique_key", "recipient_uei", "recipient_name",
        "recipient_address_line_1", "recipient_city_name", "recipient_state_code",
        "recipient_zip_4_code", "naics_code", "psc_code", "psc_category",
        "awarding_agency_name", "awarding_sub_agency_name", "pop_state_code",
        "type_of_set_aside_code", "business_size", "award_or_idv_flag",
        "current_total_value_of_award", "potential_total_value_of_award", "total_dollars_obligated",
        "pop_current_end", "pop_potential_end", "has_option_tail"]).to_reader())
    con.register("x", x.scanner(columns=["addr_hash", "latitude", "longitude", "match_type"]).to_reader())
    vmap = lance.dataset(NAICS_PSC_MAP_URI, storage_options=so)
    con.register("v", vmap.scanner(columns=["naics_code", "psc_code", "vertical",
                                            "work_type", "equipment_intensity",
                                            "what_was_done"]).to_reader())
    hexpr = addr_hash_sql("street", "city_raw", "state_raw", "zip")
    sql = f"""
    WITH base AS (
        SELECT contract_award_unique_key AS award_id, recipient_uei AS winner_uei,
               recipient_name AS winner_name,
               recipient_address_line_1 AS street, recipient_city_name AS city_raw,
               recipient_state_code AS state_raw, recipient_zip_4_code AS zip,
               naics_code, substr(naics_code, 1, 2) AS naics2,
               psc_code, psc_category,
               awarding_agency_name AS awarding_agency, awarding_sub_agency_name AS awarding_sub_agency,
               upper(trim(pop_state_code)) AS pop_state,
               nullif(trim(type_of_set_aside_code), '') AS set_aside,
               nullif(trim(business_size), '') AS business_size,
               nullif(trim(award_or_idv_flag), '') AS award_or_idv_flag,
               TRY_CAST(current_total_value_of_award AS DOUBLE) AS current_value,
               TRY_CAST(potential_total_value_of_award AS DOUBLE) AS potential_value,
               TRY_CAST(total_dollars_obligated AS DOUBLE) AS obligated,
               TRY_CAST(pop_current_end AS DATE) AS pop_current_end,
               TRY_CAST(pop_potential_end AS DATE) AS pop_potential_end,
               COALESCE(has_option_tail, FALSE) AS has_option_tail
        FROM a WHERE recipient_uei IS NOT NULL AND length(trim(recipient_uei)) > 0
    ),
    keyed AS (
        SELECT award_id, winner_uei, winner_name, naics_code, naics2, psc_code, psc_category,
               awarding_agency, awarding_sub_agency, upper(trim(state_raw)) AS state, pop_state,
               set_aside, business_size, award_or_idv_flag,
               current_value, potential_value, obligated, pop_current_end, pop_potential_end,
               has_option_tail, {hexpr} AS addr_hash
        FROM base
    )
    SELECT k.*, x.latitude, x.longitude, x.match_type,
           v.vertical, v.work_type, v.equipment_intensity, v.what_was_done,
           'govcon_active_awards_map_serving (derived)' AS source_file,
           now()::VARCHAR AS ingested_at
    FROM keyed k
    LEFT JOIN x USING (addr_hash)
    LEFT JOIN v ON k.naics_code = v.naics_code AND k.psc_code = v.psc_code
    """
    tbl = con.execute(sql).fetch_arrow_table()
    con.close()
    return tbl


def _record_run(*, rows, with_coords, write_mode, indices, status, error, started):
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    coord_rate = round(with_coords / rows, 4) if rows else None
    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute("SELECT to_regclass('ops.active_awards_map_serving_runs')")
            if cur.fetchone()[0] is None:
                cur.execute(_OPS_DDL)
            cur.execute(
                "INSERT INTO ops.active_awards_map_serving_runs (feed, rows_written, with_coords, "
                "coord_rate, write_mode, indices_built, status, error_message, started_at, completed_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("active_awards_map_serving", rows, with_coords, coord_rate, write_mode,
                 ",".join(indices), status, error, started, dt.datetime.now(dt.timezone.utc)))
            c.commit()
    except Exception as exc:  # noqa: BLE001
        log(f"WARN: ops.* write failed: {exc}")


_OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.active_awards_map_serving_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text NOT NULL,
    rows_written  bigint,
    with_coords   bigint,
    coord_rate    double precision,
    write_mode    text,
    indices_built text,
    status        text NOT NULL,
    error_message text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now()
);
"""


def build():
    import lance
    import pyarrow.compute as pc
    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    status, error, rows, with_coords = "error", None, 0, 0
    try:
        tbl = _assemble(so)
        rows = tbl.num_rows
        with_coords = tbl.filter(pc.is_valid(tbl.column("latitude"))).num_rows
        log(f"assembled {rows:,} active prime awards · {with_coords:,} with coords "
            f"({(with_coords / rows * 100 if rows else 0):.1f}%)")
        if rows == 0:
            raise RuntimeError("zero active awards assembled")
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
        _record_run(rows=rows, with_coords=with_coords, write_mode="overwrite",
                    indices=BTREE_INDEXES + BITMAP_INDEXES, status=status, error=error, started=started)
    if status != "success":
        raise SystemExit(1)
    return {"rows": rows, "with_coords": with_coords}


def verify():
    import lance
    so = _r2_so()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    today = dt.datetime.now(dt.timezone.utc).date()
    out = {"uri": SERVING_URI, "rows": ds.count_rows(),
           "with_coords": ds.count_rows(filter="latitude IS NOT NULL"),
           "with_pop_current_end": ds.count_rows(filter="pop_current_end IS NOT NULL"),
           "with_business_size": ds.count_rows(filter="business_size IS NOT NULL"),
           "with_vertical_label": ds.count_rows(filter="vertical IS NOT NULL"),
           "with_what_was_done": ds.count_rows(filter="what_was_done IS NOT NULL"),
           "vertical_aerospace_defense": ds.count_rows(filter="vertical = 'Aerospace & Defense'"),
           "columns": len(ds.schema.names), "indices": idx}
    for win in (90, 180, 365):
        hi = (today + dt.timedelta(days=win)).isoformat()
        out[f"expiring_next_{win}d"] = ds.count_rows(
            filter=f"pop_current_end >= DATE '{today.isoformat()}' AND pop_current_end <= DATE '{hi}'")
    print(json.dumps(out, indent=2, default=str))


def demo():
    import lance
    so = _r2_so()
    today = dt.datetime.now(dt.timezone.utc).date()
    hi = (today + dt.timedelta(days=180)).isoformat()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    flt = (f"psc_category = 'V' AND pop_current_end >= DATE '{today.isoformat()}' "
           f"AND pop_current_end <= DATE '{hi}'")
    rows = ds.scanner(columns=["winner_name", "winner_uei", "current_value", "pop_current_end",
                               "awarding_agency", "business_size", "latitude", "longitude"],
                      filter=flt, limit=8).to_table().to_pylist()
    print(json.dumps({"demo_filter": flt, "matched_total": ds.count_rows(filter=flt),
                      "showing": len(rows), "rows": rows}, indent=2, default=str))


def init_ops():
    import psycopg
    with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as c, c.cursor() as cur:
        cur.execute(_OPS_DDL)
        c.commit()
    log("ops DDL applied")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        print(json.dumps(build(), indent=2, default=str))
    elif cmd == "verify":
        verify()
    elif cmd == "demo":
        demo()
    elif cmd == "init_ops":
        init_ops()
    else:
        print(f"unknown command: {cmd} (init_ops|build|verify|demo)")
        sys.exit(2)


if __name__ == "__main__":
    main()
