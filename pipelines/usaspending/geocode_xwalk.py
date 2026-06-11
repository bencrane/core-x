"""Compute worker — geocode_xwalk: address → (lat, lon), the durable, address-keyed
geocode crosswalk. Vintage- and entity-independent: geocode each distinct address ONCE,
read it from every surface (awards, SAM entities, SoS) forever.

GRAIN  1 row per addr_hash = md5(normalized street|city|state|zip5).
SoR    s3://data-sink/active/geocode_xwalk/   (Lance v2.1, BTREE addr_hash)
SOURCE distinct recipient + subawardee addresses from the rolling 90d USAspending
       prime + subaward feeds (env GEOCODE_WINDOW_DAYS). Address text is verbatim in
       the feeds — no entity_profile_gold join.
GEOCODE Census Bulk Geocoder (geocoding.geo.census.gov, no key, ≤10k/batch), rooftop only.
       No-match rows are skipped (no centroid fallback) — a later run can fill them.
WRITE  ACCRETIVE. merge_insert(addr_hash).when_not_matched_insert_all — a known address
       is NEVER re-geocoded. Seeds ~42k from the 90d window; re-point SOURCE at all-time
       awards or entity_profile_gold and it appends the rest into the SAME dataset.
LEDGER ops.geocode_xwalk_runs (HQX_DB_URL_POOLED) on every terminal state.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'requests>=2.32' --with 'psycopg[binary]>=3.2' \
      python3 pipelines/usaspending/geocode_xwalk.py <init_ops|build|verify> [window_days]
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import os
import sys
import time
from pathlib import Path

ACTIVE = "s3://data-sink/active"
XWALK_URI = os.environ.get("GEOCODE_XWALK_URI", f"{ACTIVE}/geocode_xwalk/")
PRIME_URI = f"{ACTIVE}/usaspending_api_fresh/contract_prime_txn/"
SUB_URI = f"{ACTIVE}/usaspending_api_fresh/contract_subaward/"
WINDOW_DAYS = int(os.environ.get("GEOCODE_WINDOW_DAYS", "90"))
DATA_STORAGE_VERSION = "2.1"
BTREE_INDEXES = ["addr_hash"]

CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
CENSUS_BENCHMARK = os.environ.get("CENSUS_BENCHMARK", "Public_AR_Current")
CENSUS_BATCH = int(os.environ.get("CENSUS_BATCH", "10000"))
CENSUS_TIMEOUT = int(os.environ.get("CENSUS_TIMEOUT", "600"))
CENSUS_RETRIES = 3

DUCK_MEM = os.environ.get("GEOCODE_DUCKDB_MEMORY_LIMIT", "6GB")
DUCK_TMP = os.environ.get("GEOCODE_DUCKDB_TEMP_DIR", "/tmp/geocode_xwalk_duckdb")


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
    pipelines/serving/materialize_winners_map.py — it is the join key between them."""
    return ("md5("
            f"upper(regexp_replace(trim(coalesce({street},'')),'\\s+',' ','g'))||'|'||"
            f"upper(trim(coalesce({city},'')))||'|'||"
            f"upper(trim(coalesce({state},'')))||'|'||"
            f"substr(regexp_replace(coalesce({zip_},''),'[^0-9]','','g'),1,5))")


def _zip5_sql(zip_: str) -> str:
    return f"substr(regexp_replace(coalesce({zip_},''),'[^0-9]','','g'),1,5)"


def _duck():
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    os.makedirs(DUCK_TMP, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{DUCK_TMP}'")
    return con


def _worklist(so) -> list[tuple]:
    """Distinct (addr_hash, street, city, state, zip5) over recipient + subawardee
    addresses in the rolling window. One row per addr_hash."""
    import lance
    cutoff = (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=WINDOW_DAYS)).isoformat()
    p = lance.dataset(PRIME_URI, storage_options=so)
    s = lance.dataset(SUB_URI, storage_options=so)
    con = _duck()
    con.register("p", p.scanner(
        columns=["recipient_address_line_1", "recipient_city_name", "recipient_state_code",
                 "recipient_zip_4_code", "action_date"],
        filter=f"action_date >= '{cutoff}'").to_reader())
    con.register("s", s.scanner(
        columns=["subawardee_address_line_1", "subawardee_city_name", "subawardee_state_code",
                 "subawardee_zip_code", "subaward_action_date"],
        filter=f"subaward_action_date >= '{cutoff}'").to_reader())
    hexpr = addr_hash_sql("street", "city", "state", "zip")
    sql = f"""
    WITH a AS (
        SELECT recipient_address_line_1 AS street, recipient_city_name AS city,
               recipient_state_code AS state, recipient_zip_4_code AS zip FROM p
        UNION ALL
        SELECT subawardee_address_line_1, subawardee_city_name, subawardee_state_code,
               subawardee_zip_code FROM s
    ),
    h AS (
        SELECT {hexpr} AS addr_hash, trim(street) AS street, trim(city) AS city,
               upper(trim(state)) AS state, {_zip5_sql('zip')} AS zip5
        FROM a
        WHERE street IS NOT NULL AND length(trim(street)) > 0
          AND state IS NOT NULL AND length(trim(state)) > 0
    )
    SELECT addr_hash, any_value(street) AS street, any_value(city) AS city,
           any_value(state) AS state, any_value(zip5) AS zip5
    FROM h GROUP BY addr_hash
    """
    rows = con.execute(sql).fetchall()
    con.close()
    return rows


def _existing_hashes(so) -> set[str]:
    import lance
    try:
        ds = lance.dataset(XWALK_URI, storage_options=so)
    except Exception:  # noqa: BLE001 — absent ⇒ empty
        return set()
    tbl = ds.scanner(columns=["addr_hash"]).to_table()
    return set(tbl.column("addr_hash").to_pylist())


def _geocode_batch(batch: list[tuple]) -> dict[str, tuple]:
    """POST one ≤10k batch to the Census Bulk Geocoder. Returns
    addr_hash -> (lat, lon, match_type, matched_address) for Match rows only."""
    import requests
    buf = io.StringIO()
    w = csv.writer(buf)
    for addr_hash, street, city, state, zip5 in batch:
        w.writerow([addr_hash, street or "", city or "", state or "", zip5 or ""])
    payload = buf.getvalue().encode("utf-8")
    out: dict[str, tuple] = {}
    for attempt in range(1, CENSUS_RETRIES + 1):
        try:
            resp = requests.post(
                CENSUS_URL,
                files={"addressFile": ("addresses.csv", payload, "text/csv")},
                data={"benchmark": CENSUS_BENCHMARK},
                timeout=CENSUS_TIMEOUT,
            )
            resp.raise_for_status()
            for r in csv.reader(io.StringIO(resp.text)):
                # id, input, match, matchtype, matched_address, "lon,lat", tigerline, side
                if len(r) < 6 or r[2] != "Match":
                    continue
                try:
                    lon_s, lat_s = r[5].split(",")
                    out[r[0]] = (float(lat_s), float(lon_s), r[3], r[4])
                except (ValueError, IndexError):
                    continue
            return out
        except Exception as exc:  # noqa: BLE001
            log(f"  census batch attempt {attempt}/{CENSUS_RETRIES} failed: {exc}")
            if attempt == CENSUS_RETRIES:
                raise
            time.sleep(5 * attempt)
    return out


def _arrow(records: list[dict]):
    import pyarrow as pa
    schema = pa.schema([
        ("addr_hash", pa.string()), ("street", pa.string()), ("city", pa.string()),
        ("state", pa.string()), ("zip5", pa.string()),
        ("latitude", pa.float64()), ("longitude", pa.float64()),
        ("match_type", pa.string()), ("matched_address", pa.string()),
        ("geocode_source", pa.string()), ("geocoded_at", pa.string()),
    ])
    cols = {f.name: [r.get(f.name) for r in records] for f in schema}
    return pa.table(cols, schema=schema)


def _record_run(*, window_days, worklist, already, geocoded, matched, table_after,
                write_mode, indices, status, error, started):
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    match_rate = round(matched / geocoded, 4) if geocoded else None
    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute("SELECT to_regclass('ops.geocode_xwalk_runs')")
            if cur.fetchone()[0] is None:
                cur.execute(Path(__file__).parent.joinpath("ops_geocode_xwalk_runs.sql").read_text())
            cur.execute(
                "INSERT INTO ops.geocode_xwalk_runs (feed, window_days, worklist_addresses, "
                "already_present, newly_geocoded, matched, match_rate, table_rows_after, "
                "write_mode, indices_built, status, error_message, started_at, completed_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("geocode_xwalk", window_days, worklist, already, geocoded, matched, match_rate,
                 table_after, write_mode, ",".join(indices), status, error, started,
                 dt.datetime.now(dt.timezone.utc)))
            c.commit()
    except Exception as exc:  # noqa: BLE001
        log(f"WARN: ops.* write failed: {exc}")


def build(window_days: int = WINDOW_DAYS):
    import lance
    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    status, error = "error", None
    worklist_n = already_n = geocoded_n = matched_n = table_after = 0
    write_mode = "none"
    try:
        worklist = _worklist(so)
        worklist_n = len(worklist)
        log(f"worklist: {worklist_n:,} distinct addresses ({window_days}d window)")
        existing = _existing_hashes(so)
        already_n = len(existing)
        new = [r for r in worklist if r[0] not in existing]
        geocoded_n = len(new)
        log(f"already in xwalk: {already_n:,} · new to geocode: {geocoded_n:,}")

        dataset_exists = bool(existing)
        if new:
            now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
            n_batches = (len(new) + CENSUS_BATCH - 1) // CENSUS_BATCH
            for i in range(0, len(new), CENSUS_BATCH):
                batch = new[i:i + CENSUS_BATCH]
                bn = i // CENSUS_BATCH + 1
                log(f"  census batch {bn}/{n_batches} ({len(batch):,} addrs)…")
                hits = _geocode_batch(batch)
                by_hash = {r[0]: r for r in batch}
                recs = [{"addr_hash": h, "street": by_hash[h][1], "city": by_hash[h][2],
                         "state": by_hash[h][3], "zip5": by_hash[h][4], "latitude": lat,
                         "longitude": lon, "match_type": mt, "matched_address": ma,
                         "geocode_source": "census_bulk", "geocoded_at": now_iso}
                        for h, (lat, lon, mt, ma) in hits.items()]
                # Persist EACH batch immediately — a mid-run death never discards completed
                # batches; a re-run resumes (the addr_hash anti-join skips what's resident).
                if recs:
                    tbl = _arrow(recs)
                    if dataset_exists:
                        lance.dataset(XWALK_URI, storage_options=so).merge_insert("addr_hash") \
                            .when_not_matched_insert_all().execute(tbl)
                        write_mode = "merge_insert"
                    else:
                        lance.write_dataset(tbl, XWALK_URI, mode="overwrite",
                                            data_storage_version=DATA_STORAGE_VERSION,
                                            storage_options=so)
                        dataset_exists, write_mode = True, "overwrite"
                    matched_n += len(recs)
                log(f"    batch {bn} matched {len(recs):,} · cumulative {matched_n:,}")
            log(f"matched {matched_n:,}/{geocoded_n:,} "
                f"({(matched_n / geocoded_n * 100 if geocoded_n else 0):.1f}%)")

        if matched_n > 0:
            ds = lance.dataset(XWALK_URI, storage_options=so)
            for col in BTREE_INDEXES:
                ds.create_scalar_index(col, index_type="BTREE", replace=True)
                log(f"  BTREE ✓ {col}")
        elif dataset_exists:
            write_mode = "noop"
            log("no new matches; dataset unchanged")
        else:
            raise RuntimeError("no matches and no existing dataset — nothing written")

        table_after = lance.dataset(XWALK_URI, storage_options=so).count_rows()
        status = "success"
        log(f"DONE table_rows={table_after:,} write_mode={write_mode}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        log(f"FAILED: {error}")
    finally:
        _record_run(window_days=window_days, worklist=worklist_n, already=already_n,
                    geocoded=geocoded_n, matched=matched_n, table_after=table_after,
                    write_mode=write_mode, indices=BTREE_INDEXES, status=status,
                    error=error, started=started)
    if status != "success":
        raise SystemExit(1)
    return {"worklist": worklist_n, "new": geocoded_n, "matched": matched_n,
            "table_rows": table_after, "write_mode": write_mode}


def verify():
    import lance
    so = _r2_so()
    ds = lance.dataset(XWALK_URI, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    n = ds.count_rows()
    dN = ds.count_rows(filter="latitude IS NOT NULL")
    print(json.dumps({"uri": XWALK_URI, "rows": n, "with_coords": dN,
                      "columns": len(ds.schema.names), "indices": idx}, indent=2, default=str))


def init_ops():
    import psycopg
    sql = Path(__file__).parent.joinpath("ops_geocode_xwalk_runs.sql").read_text()
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
    elif cmd == "init_ops":
        init_ops()
    else:
        print(f"unknown command: {cmd} (init_ops|build|verify)"); sys.exit(2)


if __name__ == "__main__":
    main()
