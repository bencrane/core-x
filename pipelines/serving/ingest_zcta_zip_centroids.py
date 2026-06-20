"""Reference ingest — `zcta_zip_centroids` (canonical zip5 → centroid lat/long, full US coverage).

SoR  s3://data-sink/active/zcta_zip_centroids/  (Lance v2.1; reference, snapshot-overwrite).

WHY THIS EXISTS
geocode_xwalk is an address-level rooftop cache built from contractor MAILING addresses — it
resolves 98.7% of rental-firm zips but only 79.1% of construction WORKSITE zips (bases/rural PoP
zips it never ingested). This table closes that gap with the Census ZCTA Gazetteer internal-point
centroids — ~33k ZCTA5 areas covering essentially every populated US zip — giving the
equipment-rental↔construction radius match full demand-side coverage.

Source: US Census 2023 Gazetteer, ZCTA national file (GEOID=ZCTA5, INTPTLAT/INTPTLONG = centroid).
Grain: 1 row / zcta5. Idempotent snapshot-overwrite. BTREE on zcta5.

    doppler run --project core-x --config prd -- python pipelines/serving/ingest_zcta_zip_centroids.py
    doppler run --project core-x --config prd -- python pipelines/serving/ingest_zcta_zip_centroids.py --verify
"""
from __future__ import annotations

import os
import sys
import zipfile

GAZ_URL = ("https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
           "2023_Gazetteer/2023_Gaz_zcta_national.zip")
SERVING_URI = os.environ.get("ZCTA_ZIP_CENTROIDS_URI", "s3://data-sink/active/zcta_zip_centroids/")
DATA_STORAGE_VERSION = "2.1"
SOURCE_VERSION = "census_2023_gazetteer_zcta"
TMP_ZIP = "/tmp/zcta_gaz.zip"
TMP_DIR = "/tmp/zcta_gaz"


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


def _download(path: str = TMP_ZIP) -> str:
    """Fetch the gazetteer zip (urllib; census blocks empty UA)."""
    import urllib.request

    req = urllib.request.Request(GAZ_URL, headers={"User-Agent": "core-x-ingest/1.0"})
    with urllib.request.urlopen(req, timeout=120) as r, open(path, "wb") as f:
        f.write(r.read())
    return path


def _extract_txt(zip_path: str) -> str:
    with zipfile.ZipFile(zip_path) as zf:
        member = max((m for m in zf.infolist() if m.filename.lower().endswith(".txt")),
                     key=lambda m: m.file_size)
        zf.extract(member, TMP_DIR)
        return os.path.join(TMP_DIR, member.filename)


def build(local_zip: str | None = None) -> dict:
    import duckdb
    import lance

    so = _r2_storage_options()
    zip_path = local_zip or _download()
    txt = _extract_txt(zip_path)

    con = duckdb.connect(":memory:")
    # Positional/explicit columns — Census headers carry trailing whitespace; force 7 VARCHAR cols
    # and skip the header row, so column drift can never misread the centroid.
    tbl = con.execute(f"""
        SELECT lpad(trim(GEOID), 5, '0')              AS zcta5,
               TRY_CAST(trim(INTPTLAT)  AS DOUBLE)    AS lat,
               TRY_CAST(trim(INTPTLONG) AS DOUBLE)    AS lon,
               TRY_CAST(trim(ALAND_SQMI) AS DOUBLE)   AS land_sqmi,
               '{SOURCE_VERSION}'                     AS source_version
        FROM read_csv('{txt}', delim='\t', header=true, all_varchar=true,
                      columns={{'GEOID':'VARCHAR','ALAND':'VARCHAR','AWATER':'VARCHAR',
                                'ALAND_SQMI':'VARCHAR','AWATER_SQMI':'VARCHAR',
                                'INTPTLAT':'VARCHAR','INTPTLONG':'VARCHAR'}})
        WHERE GEOID IS NOT NULL
          AND TRY_CAST(trim(INTPTLAT) AS DOUBLE) BETWEEN 17 AND 72
          AND TRY_CAST(trim(INTPTLONG) AS DOUBLE) BETWEEN -180 AND -64
    """).fetch_arrow_table()

    rows = tbl.num_rows
    assert rows > 30_000, f"floor gate: {rows} ZCTA rows <= 30000 (bad parse?)"
    con.register("z", tbl)
    n_uei = con.execute("SELECT count(DISTINCT zcta5) FROM z").fetchone()[0]
    assert n_uei == rows, f"grain gate: {n_uei} distinct zcta5 != {rows} rows"
    print(f"parsed {rows:,} ZCTA centroids (distinct zcta5={n_uei:,})")

    lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(SERVING_URI, storage_options=so)
    ds.create_scalar_index("zcta5", index_type="BTREE")
    print(f"  BTREE ✓ zcta5")
    back = ds.count_rows()
    assert back == rows, f"write-integrity gate: {back} != {rows}"
    print(f"WROTE {SERVING_URI} rows={back} cols={len(ds.schema)}")
    return {"uri": SERVING_URI, "rows": back}


def verify() -> None:
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    print(f"{SERVING_URI}  rows={ds.count_rows():,}  cols={len(ds.schema)}")
    print("indices:", sorted(
        (i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))) for i in ds.list_indices()))
    con = duckdb.connect(":memory:"); con.register("z", ds)
    print(con.execute("SELECT zcta5, round(lat,4) lat, round(lon,4) lon FROM z "
                      "WHERE zcta5 IN ('01201','90001','99501','33101') ORDER BY zcta5").df().to_string(index=False))


if __name__ == "__main__":
    if "--verify" in sys.argv:
        verify()
    else:
        # optional: pass a pre-downloaded zip path as the first non-flag arg
        lz = next((a for a in sys.argv[1:] if not a.startswith("-")), None)
        build(local_zip=lz)
