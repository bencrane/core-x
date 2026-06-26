"""Serving worker — firmographics_company_map_serving: one row per SAM-registered company
in the firmographics target universe, enriched with industry/size/type/HQ firmographics +
govcon award signal + a lat/lon dot. The prospecting-map read model.

GRAIN  1 row per UEI reachable from a firmographics_blitz domain via sam_master_domains
       (domain-only link; 1 UEI ↦ exactly 1 firmographics domain on disk → naturally 1/UEI).
SoR    s3://data-sink/active/firmographics_company_map_serving/  (Lance v2.1, derived, overwrite)
INPUTS firmographics_blitz (domain firmographics) ⋈ sam_master_domains (domain↦uei)
       ⋈ entity_profile_gold (uei → SAM address + award rollups) ⋈ geocode_xwalk (addr_hash → coords)
       + RECENCY: contractor_award_summary (prime/subaward most-recent action dates, rebuilt
       daily — summary_as_of) ∪ usaspending_api_fresh prime/subaward feeds (rolling 90d),
       max()'d per UEI into latest_award_action_date (DATE).
SIGNALS industry, employee_size_band, company_type (firmographics) + naics, has_federal_awards,
       entity_active_obligated_usd, award_count, latest_award_action_date (govcon) — the
       prospecting filters. latest_award_action_date STALENESS: as fresh as the most recent
       contractor_award_summary / fresh-feed ingest (NOT request-time); the decoder's
       days_since_last_award axis resolves against it at query time.
LEDGER ops.company_map_serving_runs (HQX_DB_URL_POOLED) on every terminal state.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' \
      python3 pipelines/serving/materialize_company_map.py <init_ops|build|verify|demo>
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

# Repo root on sys.path so the canonical join-key import resolves whether this file is
# run as a script (python3 pipelines/serving/materialize_company_map.py) or imported.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines._shared.addr_hash import addr_hash_sql  # noqa: E402

ACTIVE = "s3://data-sink/active"
SERVING_URI = os.environ.get("COMPANY_MAP_SERVING_URI", f"{ACTIVE}/firmographics_company_map_serving/")
FIRMO_URI = f"{ACTIVE}/firmographics_blitz/"
SAM_DOMAINS_URI = f"{ACTIVE}/sam_master_domains/"
EPG_URI = f"{ACTIVE}/entity_profile_gold/"
XWALK_URI = os.environ.get("GEOCODE_XWALK_URI", f"{ACTIVE}/geocode_xwalk/")
# Recency sources for latest_award_action_date (max across all three per UEI).
CAS_URI = f"{ACTIVE}/contractor_award_summary/"
FRESH_PRIME_URI = f"{ACTIVE}/usaspending_api_fresh/contract_prime_txn/"
FRESH_SUB_URI = f"{ACTIVE}/usaspending_api_fresh/contract_subaward/"
DATA_STORAGE_VERSION = "2.1"
BTREE_INDEXES = ["uei", "addr_hash", "domain_norm", "primary_naics", "latest_award_action_date",
                 # range filter axes (full-scan without these).
                 "founded_year", "entity_active_obligated_usd", "award_count"]
BITMAP_INDEXES = ["naics2", "industry", "employee_size_band", "company_type",
                  "physical_address_state", "has_federal_awards", "is_active"]

DUCK_MEM = os.environ.get("COMPANY_MAP_DUCKDB_MEMORY_LIMIT", "8GB")
DUCK_TMP = os.environ.get("COMPANY_MAP_DUCKDB_TEMP_DIR", "/tmp/company_map_duckdb")


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
    fb = lance.dataset(FIRMO_URI, storage_options=so)
    sd = lance.dataset(SAM_DOMAINS_URI, storage_options=so)
    epg = lance.dataset(EPG_URI, storage_options=so)
    xw = lance.dataset(XWALK_URI, storage_options=so)
    cas = lance.dataset(CAS_URI, storage_options=so)
    fp = lance.dataset(FRESH_PRIME_URI, storage_options=so)
    fs = lance.dataset(FRESH_SUB_URI, storage_options=so)
    con = _duck()
    # materialize projected scans (NOT to_reader — one-shot); single assembly query.
    con.register("fb", fb.scanner(columns=[
        "domain_norm", "company_name", "industry", "employee_size_band", "employees_on_linkedin",
        "company_type", "founded_year", "followers", "hq_city", "hq_state", "hq_region",
        "linkedin_url", "specialties"]).to_table())
    con.register("sd", sd.scanner(columns=["normalized_domain", "uei"]).to_table())
    con.register("epg", epg.scanner(columns=[
        "uei", "cage_code", "legal_business_name", "primary_naics", "is_active", "has_federal_awards",
        "total_active_obligations", "total_lifetime_obligations", "award_count", "active_award_count",
        "physical_address_line_1", "physical_address_city", "physical_address_state",
        "physical_address_zip_postal_code"]).to_table())
    con.register("xw", xw.scanner(columns=["addr_hash", "latitude", "longitude", "match_type"]).to_table())
    # Recency sources: the all-time per-recipient summary (date32, rebuilt daily) plus the
    # rolling-90d fresh feeds (ISO-string action dates) — max()'d per UEI below.
    con.register("cas", cas.scanner(columns=[
        "recipient_uei", "prime_most_recent_action_date", "subaward_most_recent_action_date"]).to_table())
    con.register("fp", fp.scanner(columns=["recipient_uei", "action_date"]).to_table())
    con.register("fs", fs.scanner(columns=["subawardee_uei", "subaward_action_date"]).to_table())
    hexpr = addr_hash_sql("e.physical_address_line_1", "e.physical_address_city",
                          "e.physical_address_state", "e.physical_address_zip_postal_code")
    sql = f"""
    WITH dom AS (
        SELECT DISTINCT lower(trim(normalized_domain)) AS domain, uei FROM sd
        WHERE uei IS NOT NULL AND length(trim(uei)) > 0 AND normalized_domain IS NOT NULL),
    fbx AS (
        SELECT lower(trim(domain_norm)) AS domain, company_name, industry, employee_size_band,
               employees_on_linkedin, company_type, founded_year, followers, hq_city, hq_state,
               hq_region, linkedin_url, specialties
        FROM fb WHERE domain_norm IS NOT NULL AND length(trim(domain_norm)) > 0),
    linked AS (
        SELECT d.uei, d.domain AS domain_norm, f.company_name, f.industry, f.employee_size_band,
               f.employees_on_linkedin, f.company_type, f.founded_year, f.followers, f.hq_city,
               f.hq_state, f.hq_region, f.linkedin_url, f.specialties
        FROM dom d JOIN fbx f ON d.domain = f.domain),
    recency AS (
        SELECT uei, max(d) AS latest_award_action_date FROM (
            SELECT recipient_uei AS uei, prime_most_recent_action_date AS d FROM cas
            UNION ALL
            SELECT recipient_uei, subaward_most_recent_action_date FROM cas
            UNION ALL
            SELECT recipient_uei, try_cast(action_date AS DATE) FROM fp
            UNION ALL
            SELECT subawardee_uei, try_cast(subaward_action_date AS DATE) FROM fs
        ) WHERE uei IS NOT NULL AND length(trim(uei)) > 0
        GROUP BY uei),
    asm AS (
        SELECT l.uei, e.cage_code, l.domain_norm, l.company_name, e.legal_business_name,
               l.industry, l.employee_size_band, l.employees_on_linkedin, l.company_type,
               l.founded_year, l.followers, l.hq_city, l.hq_state, l.hq_region, l.linkedin_url,
               l.specialties, e.primary_naics, substr(e.primary_naics, 1, 2) AS naics2,
               e.is_active, e.has_federal_awards,
               e.total_active_obligations AS entity_active_obligated_usd,
               e.total_lifetime_obligations, e.award_count, e.active_award_count,
               e.physical_address_line_1, e.physical_address_city,
               upper(trim(e.physical_address_state)) AS physical_address_state,
               e.physical_address_zip_postal_code, {hexpr} AS addr_hash
        FROM linked l JOIN epg e ON l.uei = e.uei)
    SELECT a.*, r.latest_award_action_date, x.latitude, x.longitude, x.match_type,
           'firmographics_company_map_serving (derived)' AS source_file, now()::VARCHAR AS ingested_at
    FROM asm a LEFT JOIN recency r ON a.uei = r.uei
               LEFT JOIN xw x USING (addr_hash)
    """
    tbl = con.execute(sql).fetch_arrow_table()
    con.close()
    return tbl


def _record_run(*, rows, with_coords, fed_awards, write_mode, indices, status, error, started):
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    coord_rate = round(with_coords / rows, 4) if rows else None
    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute("SELECT to_regclass('ops.company_map_serving_runs')")
            if cur.fetchone()[0] is None:
                cur.execute(Path(__file__).parent.joinpath("ops_company_map_serving_runs.sql").read_text())
            cur.execute(
                "INSERT INTO ops.company_map_serving_runs (feed, rows_written, with_coords, "
                "coord_rate, has_federal_awards, write_mode, indices_built, status, error_message, "
                "started_at, completed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("firmographics_company_map_serving", rows, with_coords, coord_rate, fed_awards,
                 write_mode, ",".join(indices), status, error, started, dt.datetime.now(dt.timezone.utc)))
            c.commit()
    except Exception as exc:  # noqa: BLE001
        log(f"WARN: ops.* write failed: {exc}")


def build():
    import lance
    import pyarrow.compute as pc
    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    status, error, rows, with_coords, fed = "error", None, 0, 0, 0
    try:
        tbl = _assemble(so)
        rows = tbl.num_rows
        with_coords = tbl.filter(pc.is_valid(tbl.column("latitude"))).num_rows
        fed = tbl.filter(pc.equal(tbl.column("has_federal_awards"), True)).num_rows
        with_recency = tbl.filter(pc.is_valid(tbl.column("latest_award_action_date"))).num_rows
        log(f"assembled {rows:,} companies · {with_coords:,} with coords "
            f"({(with_coords / rows * 100 if rows else 0):.1f}%) · {fed:,} with federal awards "
            f"· {with_recency:,} with latest_award_action_date")
        if rows == 0:
            raise RuntimeError("zero companies assembled")
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
        _record_run(rows=rows, with_coords=with_coords, fed_awards=fed, write_mode="overwrite",
                    indices=BTREE_INDEXES + BITMAP_INDEXES, status=status, error=error, started=started)
    if status != "success":
        raise SystemExit(1)
    return {"rows": rows, "with_coords": with_coords, "has_federal_awards": fed}


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
           "has_federal_awards": ds.count_rows(filter="has_federal_awards IS true"),
           "with_recency": ds.count_rows(filter="latest_award_action_date IS NOT NULL"),
           "columns": len(ds.schema.names), "indices": idx}
    out["demo_construction_with_coords"] = ds.count_rows(
        filter="naics2 = '23' AND latitude IS NOT NULL")
    out["demo_construction_fed_awards_with_coords"] = ds.count_rows(
        filter="naics2 = '23' AND has_federal_awards IS true AND latitude IS NOT NULL")
    cutoff30 = (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=30)).isoformat()
    out["demo_won_last_30d_with_coords"] = ds.count_rows(
        filter=f"latest_award_action_date >= DATE '{cutoff30}' AND latitude IS NOT NULL")
    print(json.dumps(out, indent=2, default=str))


def demo():
    """Sample GeoJSON for the prospecting demo filter — proof the company dots exist."""
    import lance
    so = _r2_so()
    naics2 = os.environ.get("DEMO_NAICS2", "23")
    limit = int(os.environ.get("DEMO_LIMIT", "8"))
    fed_only = os.environ.get("DEMO_FED_ONLY", "1") == "1"
    ds = lance.dataset(SERVING_URI, storage_options=so)
    flt = f"naics2 = '{naics2}' AND latitude IS NOT NULL"
    if fed_only:
        flt += " AND has_federal_awards IS true"
    total = ds.count_rows(filter=flt)
    rows = ds.scanner(columns=["company_name", "industry", "employee_size_band", "company_type",
                               "physical_address_state", "primary_naics", "has_federal_awards",
                               "entity_active_obligated_usd", "latitude", "longitude"],
                      filter=flt, limit=limit).to_table().to_pylist()
    fc = {"type": "FeatureCollection", "demo_filter": flt, "matched_total": total, "showing": len(rows),
          "features": [{"type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [r["longitude"], r["latitude"]]},
                        "properties": {k: r[k] for k in ("company_name", "industry", "employee_size_band",
                                                         "company_type", "physical_address_state",
                                                         "primary_naics", "has_federal_awards",
                                                         "entity_active_obligated_usd")}}
                       for r in rows]}
    print(json.dumps(fc, indent=2, default=str))


def init_ops():
    import psycopg
    sql = Path(__file__).parent.joinpath("ops_company_map_serving_runs.sql").read_text()
    with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as c, c.cursor() as cur:
        cur.execute(sql); c.commit()
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
        print(f"unknown command: {cmd} (init_ops|build|verify|demo)"); sys.exit(2)


if __name__ == "__main__":
    main()
