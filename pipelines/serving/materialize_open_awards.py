#!/usr/bin/env python3
"""Serving build — gtm_open_awards: the open federal award universe, demo-hot-path shaped.

SoR  s3://data-sink/active/gtm_open_awards/  (Lance; derived, snapshot-overwrite;
     BTREE naics_code / product_or_service_code / recipient_uei)

WHAT THIS IS
One compact row per award that is OPEN as of build time — active period of performance
(pop_current_end >= as_of) or an IDV whose ordering window is still open
(ordering_period_end >= as_of) — pre-joined with its PoP centroid and its sub-out
signals. Small by construction (~200K rows): an API process loads it INTO MEMORY at
boot and serves opportunity matching without touching R2 per request. Rebuild after
award-spine refreshes (as_of stamps the openness cutoff).

SOURCES  usaspending_award_canonical (openness, codes, $, subbing signals)
         usaspending_award_pop_centroids (work-site point, precision-tiered)

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 pipelines/serving/materialize_open_awards.py [--verify]
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import duckdb
import lance

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

A = "s3://data-sink/active"
AWARD_URI = f"{A}/usaspending_award_canonical/"
CENTROID_URI = f"{A}/usaspending_award_pop_centroids/"
OUT = f"{A}/gtm_open_awards/"
PARAM_SET_ID = "v1"
BTREE_COLS = ["naics_code", "product_or_service_code", "recipient_uei"]

AWARD_COLS = [
    "generated_unique_award_id", "award_id_piid", "recipient_uei",
    "recipient_name", "naics_code", "product_or_service_code",
    "total_obligation", "base_and_all_options_value",
    "subaward_count", "total_subaward_amount", "subcontracting_plan_code",
    "period_of_performance_current_end_date", "ordering_period_end_date",
    "award_or_idv_flag", "idv_type_code", "type_of_set_aside_code",
    "awarding_agency_code", "awarding_agency_name",
    "primary_place_of_performance_state_code",
]


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def build() -> int:
    opt = so()
    as_of = date.today().isoformat()
    con = duckdb.connect()
    con.execute("SET memory_limit='16GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")

    aw = lance.dataset(AWARD_URI, storage_options=opt)
    present = [c for c in AWARD_COLS if c in set(aw.schema.names)]
    missing = [c for c in AWARD_COLS if c not in set(aw.schema.names)]
    if missing:
        print(f"columns absent on spine (skipped): {missing}", flush=True)
    reader = aw.scanner(
        columns=present,
        filter=(f"period_of_performance_current_end_date >= DATE '{as_of}' "
                f"OR ordering_period_end_date >= DATE '{as_of}'")).to_reader()
    con.register("_a", reader)
    con.execute("CREATE TABLE open_awards AS SELECT * FROM _a")
    con.unregister("_a")
    n = con.execute("SELECT COUNT(*) FROM open_awards").fetchone()[0]
    print(f"open awards as of {as_of}: {n:,}", flush=True)

    cen = lance.dataset(CENTROID_URI, storage_options=opt)
    con.register("_c", cen.scanner(
        columns=["generated_unique_award_id", "latitude", "longitude", "geo_precision"]).to_reader())
    con.execute("CREATE TABLE cent AS SELECT * FROM _c")
    con.unregister("_c")

    con.execute("""
    CREATE TABLE serving AS
    SELECT o.*, c.latitude, c.longitude, c.geo_precision
    FROM open_awards o
    LEFT JOIN cent c USING (generated_unique_award_id)
    """)
    dup = con.execute("""SELECT COUNT(*) FROM (
        SELECT generated_unique_award_id FROM serving GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup == 0, f"grain not unique: {dup} dups"
    located = con.execute(
        "SELECT COUNT(*) FROM serving WHERE geo_precision = 'zip5'").fetchone()[0]
    subbing = con.execute(
        "SELECT COUNT(*) FROM serving WHERE COALESCE(subaward_count,0) > 0 "
        "OR subcontracting_plan_code IS NOT NULL").fetchone()[0]
    print(f"serving rows: {n:,}  zip5-located: {located:,}  "
          f"with subbing signals: {subbing:,}", flush=True)

    built_from = (f"usaspending_award_canonical:v{aw.version}|"
                  f"usaspending_award_pop_centroids:v{cen.version}")
    res = con.execute(f"""
        SELECT *, DATE '{as_of}' AS as_of, '{built_from}' AS built_from_version,
               '{PARAM_SET_ID}' AS param_set_id FROM serving""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE_COLS],
                               storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    opt = so()
    ds = lance.dataset(OUT, storage_options=opt)
    rows = ds.count_rows()
    idx = [i["name"] for i in ds.list_indices()]
    located = ds.count_rows(filter="geo_precision = 'zip5'")
    ok = 50_000 < rows < 2_000_000 and located > 0.5 * rows and all(
        any(c in n for n in idx) for c in BTREE_COLS)
    print(f"{OUT}: rows={rows:,} zip5={located:,} indices={idx} -> {'OK' if ok else 'FAIL'}",
          flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
