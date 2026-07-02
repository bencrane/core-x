"""Reference ingest — Census geographic reference + crosswalk family (SoR: R2 Lance).

Lands the canonical Census county/geography reference substrate the labor-market and
govcon planes bind against. Every dataset is a reference table: idempotent
snapshot-overwrite (mode="overwrite"), hard BTREE on each resolution key, BITMAP on
low-cardinality categoricals. No row filtering that drops coverage — territories
(PR/VI/GU/AS/MP) and every intersection/edge row are retained.

Datasets (all under s3://data-sink/active/):

  national_county2020            county FIPS <-> name (Census 2020 codes file)   ~3,235
      grain 1/county_fips   BTREE county_fips   BITMAP state_usps
  national_state2020             state FIPS/USPS <-> name                            ~57
      grain 1/state_fips    BTREE state_fips, state_usps
  national_cousub2020            county subdivisions (MCD/CCD)                    ~36,600
      grain 1/cousub_fips   BTREE cousub_fips, county_fips
  census_county_cbsa_2023        county -> CBSA/metro (OMB Jul-2023 delineation)  ~1,915
      grain 1/county_fips   BTREE county_fips, cbsa_code   BITMAP metro_micro, central_outlying
  census_zcta_county_rel_2020    ZCTA <-> county intersection w/ area allocation  ~45,000
      grain 1/(zcta5,county_fips)   BTREE zcta5, county_fips
  census_county_adjacency        county neighbor graph (2010 vintage, ~static)   ~22,200
      grain 1/(county_fips,neighbor_fips)   BTREE county_fips, neighbor_fips
  census_county_gazetteer_2023   county centroid lat/lon + land/water area        ~3,234
      grain 1/county_fips   BTREE county_fips

Run (all, or a subset via --only; verify-only reads back from R2):

    doppler run --project core-x --config prd -- python pipelines/census_geo/ingest.py
    doppler run --project core-x --config prd -- python pipelines/census_geo/ingest.py --only census_county_cbsa_2023
    doppler run --project core-x --config prd -- python pipelines/census_geo/ingest.py --verify
"""
from __future__ import annotations

import os
import sys
import tempfile
import urllib.request
import zipfile

DATA_STORAGE_VERSION = "2.1"
ACTIVE = "s3://data-sink/active"
UA = "core-x-ingest/1.0"

# Upstream sources (Census). Vintages pinned; bump deliberately.
URL_NATIONAL_COUNTY = "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"
URL_NATIONAL_STATE = "https://www2.census.gov/geo/docs/reference/codes2020/national_state2020.txt"
URL_NATIONAL_COUSUB = "https://www2.census.gov/geo/docs/reference/codes2020/national_cousub2020.txt"
URL_CBSA = ("https://www2.census.gov/programs-surveys/metro-micro/geographies/"
            "reference-files/2023/delineation-files/list1_2023.xlsx")
URL_ZCTA_COUNTY_REL = ("https://www2.census.gov/geo/docs/maps-data/data/rel2020/"
                       "zcta520/tab20_zcta520_county20_natl.txt")
URL_COUNTY_ADJACENCY = "https://www2.census.gov/geo/docs/reference/county_adjacency.txt"
URL_COUNTY_GAZ = ("https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
                  "2023_Gazetteer/2023_Gaz_counties_national.zip")


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


def _uri(name: str) -> str:
    return f"{ACTIVE}/{name}/"


def _download(url: str, dest: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=180) as r, open(dest, "wb") as f:
        f.write(r.read())
    return dest


def _write_lance(name: str, tbl, *, btree, bitmap=(), floor: int = 1,
                 grain_key: str | None = None) -> dict:
    """Snapshot-overwrite to R2, build scalar indices, gate on floor/grain/write-integrity."""
    import lance

    rows = tbl.num_rows
    assert rows >= floor, f"{name}: floor gate {rows} < {floor} (bad parse?)"
    if grain_key:
        import pyarrow.compute as pc
        distinct = len(pc.unique(tbl.column(grain_key)))
        assert distinct == rows, f"{name}: grain gate — {distinct} distinct {grain_key} != {rows} rows"

    so = _r2_storage_options()
    uri = _uri(name)
    lance.write_dataset(tbl, uri, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(uri, storage_options=so)
    for k in btree:
        ds.create_scalar_index(k, index_type="BTREE")
    for k in bitmap:
        ds.create_scalar_index(k, index_type="BITMAP")
    back = ds.count_rows()
    assert back == rows, f"{name}: write-integrity {back} != {rows}"
    print(f"WROTE {uri} rows={back:,} cols={len(ds.schema)} "
          f"btree={list(btree)} bitmap={list(bitmap)}")
    return {"name": name, "uri": uri, "rows": back}


# ── builders ──────────────────────────────────────────────────────────────────

def build_national_county2020(tmp: str) -> dict:
    import duckdb
    txt = _download(URL_NATIONAL_COUNTY, f"{tmp}/national_county2020.txt")
    con = duckdb.connect(":memory:")
    tbl = con.execute(f"""
        SELECT lpad(trim(STATEFP),2,'0') || lpad(trim(COUNTYFP),3,'0') AS county_fips,
               trim(STATE)      AS state_usps,
               lpad(trim(STATEFP),2,'0')  AS state_fips,
               lpad(trim(COUNTYFP),3,'0') AS county_fips3,
               trim(COUNTYNAME) AS county_name,
               trim(COUNTYNS)   AS county_ns,
               trim(CLASSFP)    AS class_fp,
               trim(FUNCSTAT)   AS funcstat,
               'census_codes2020' AS source_version
        FROM read_csv('{txt}', delim='|', header=true, all_varchar=true)
        WHERE STATEFP IS NOT NULL AND COUNTYFP IS NOT NULL
    """).fetch_arrow_table()
    return _write_lance("national_county2020", tbl,
                        btree=["county_fips"], bitmap=["state_usps"],
                        floor=3_000, grain_key="county_fips")


def build_national_state2020(tmp: str) -> dict:
    import duckdb
    txt = _download(URL_NATIONAL_STATE, f"{tmp}/national_state2020.txt")
    con = duckdb.connect(":memory:")
    tbl = con.execute(f"""
        SELECT trim(STATE)      AS state_usps,
               lpad(trim(STATEFP),2,'0') AS state_fips,
               trim(STATENS)    AS state_ns,
               trim(STATE_NAME) AS state_name,
               'census_codes2020' AS source_version
        FROM read_csv('{txt}', delim='|', header=true, all_varchar=true)
        WHERE STATEFP IS NOT NULL
    """).fetch_arrow_table()
    return _write_lance("national_state2020", tbl,
                        btree=["state_fips", "state_usps"],
                        floor=50, grain_key="state_fips")


def build_national_cousub2020(tmp: str) -> dict:
    import duckdb
    txt = _download(URL_NATIONAL_COUSUB, f"{tmp}/national_cousub2020.txt")
    con = duckdb.connect(":memory:")
    tbl = con.execute(f"""
        SELECT lpad(trim(STATEFP),2,'0') || lpad(trim(COUNTYFP),3,'0')
                 || lpad(trim(COUSUBFP),5,'0')                    AS cousub_fips,
               lpad(trim(STATEFP),2,'0') || lpad(trim(COUNTYFP),3,'0') AS county_fips,
               trim(STATE)      AS state_usps,
               lpad(trim(STATEFP),2,'0') AS state_fips,
               trim(COUNTYNAME) AS county_name,
               trim(COUSUBNAME) AS cousub_name,
               trim(COUSUBNS)   AS cousub_ns,
               trim(CLASSFP)    AS class_fp,
               trim(FUNCSTAT)   AS funcstat,
               'census_codes2020' AS source_version
        FROM read_csv('{txt}', delim='|', header=true, all_varchar=true)
        WHERE STATEFP IS NOT NULL AND COUNTYFP IS NOT NULL AND COUSUBFP IS NOT NULL
    """).fetch_arrow_table()
    return _write_lance("national_cousub2020", tbl,
                        btree=["cousub_fips", "county_fips"],
                        floor=30_000, grain_key="cousub_fips")


def build_census_county_cbsa_2023(tmp: str) -> dict:
    """OMB Jul-2023 delineation (list1). Header at row idx 2; footer note rows dropped
    by requiring numeric FIPS. County-in-CBSA only (rural non-CBSA counties absent)."""
    import duckdb
    import pandas as pd
    xls = _download(URL_CBSA, f"{tmp}/list1_2023.xlsx")
    raw = pd.read_excel(xls, header=2, dtype=str, engine="openpyxl")
    con = duckdb.connect(":memory:")
    con.register("raw", raw)
    tbl = con.execute("""
        SELECT lpad(CAST(TRY_CAST(trim("FIPS State Code")  AS DOUBLE)::BIGINT AS VARCHAR),2,'0')
             || lpad(CAST(TRY_CAST(trim("FIPS County Code") AS DOUBLE)::BIGINT AS VARCHAR),3,'0') AS county_fips,
               trim("CBSA Code")                    AS cbsa_code,
               trim("CBSA Title")                   AS cbsa_title,
               CASE WHEN "Metropolitan/Micropolitan Statistical Area" LIKE 'Metro%' THEN 'metro'
                    WHEN "Metropolitan/Micropolitan Statistical Area" LIKE 'Micro%' THEN 'micro'
                    ELSE NULL END                   AS metro_micro,
               nullif(trim("CSA Code"),'')          AS csa_code,
               nullif(trim("CSA Title"),'')         AS csa_title,
               nullif(trim("Metropolitan Division Code"),'')  AS metro_division_code,
               nullif(trim("Metropolitan Division Title"),'') AS metro_division_title,
               trim("County/County Equivalent")     AS county_name,
               trim("State Name")                   AS state_name,
               lower(trim("Central/Outlying County")) AS central_outlying,
               'census_omb_cbsa_2023' AS source_version
        FROM raw
        WHERE TRY_CAST(trim("FIPS State Code")  AS DOUBLE) IS NOT NULL
          AND TRY_CAST(trim("FIPS County Code") AS DOUBLE) IS NOT NULL
    """).fetch_arrow_table()
    return _write_lance("census_county_cbsa_2023", tbl,
                        btree=["county_fips", "cbsa_code"],
                        bitmap=["metro_micro", "central_outlying"],
                        floor=1_500, grain_key="county_fips")


def build_census_zcta_county_rel_2020(tmp: str) -> dict:
    """ZCTA×county intersection. AREALAND_PART = land area (m^2) of the ZCTA lying in the
    county; alloc_land = that part / total ZCTA land = weight for pushing ZCTA→county."""
    import duckdb
    txt = _download(URL_ZCTA_COUNTY_REL, f"{tmp}/zcta_county_rel.txt")
    con = duckdb.connect(":memory:")
    # BOM rides on OID_ZCTA5_20 (unused); all referenced columns are BOM-free.
    tbl = con.execute(f"""
        SELECT lpad(trim(GEOID_ZCTA5_20),5,'0') AS zcta5,
               lpad(trim(GEOID_COUNTY_20),5,'0') AS county_fips,
               TRY_CAST(AREALAND_PART  AS BIGINT) AS arealand_part_m2,
               TRY_CAST(AREAWATER_PART AS BIGINT) AS areawater_part_m2,
               TRY_CAST(AREALAND_ZCTA5_20 AS BIGINT) AS arealand_zcta_m2,
               TRY_CAST(AREALAND_COUNTY_20 AS BIGINT) AS arealand_county_m2,
               TRY_CAST(AREALAND_PART AS DOUBLE)
                 / nullif(TRY_CAST(AREALAND_ZCTA5_20 AS DOUBLE), 0) AS alloc_land,
               'census_zcta_county_rel_2020' AS source_version
        FROM read_csv('{txt}', delim='|', header=true, all_varchar=true)
        WHERE GEOID_ZCTA5_20 IS NOT NULL AND GEOID_COUNTY_20 IS NOT NULL
    """).fetch_arrow_table()
    return _write_lance("census_zcta_county_rel_2020", tbl,
                        btree=["zcta5", "county_fips"], floor=30_000)


def build_census_county_adjacency(tmp: str) -> dict:
    """2010 vintage neighbor graph. Continuation rows blank in cols 0/1 -> forward-fill.
    Self-edge (county is its own first neighbor) retained + flagged. FIPS keys are the
    authoritative payload; latin-1 names are convenience (join national_county2020 for
    canonical UTF-8 names)."""
    import duckdb
    import pandas as pd
    txt = _download(URL_COUNTY_ADJACENCY, f"{tmp}/county_adjacency.txt")
    df = pd.read_csv(txt, sep="\t", header=None, dtype=str, encoding="latin-1",
                     names=["county_name", "county_geoid", "neighbor_name", "neighbor_geoid"])
    df[["county_name", "county_geoid"]] = df[["county_name", "county_geoid"]].ffill()
    con = duckdb.connect(":memory:")
    con.register("adj", df)
    tbl = con.execute("""
        SELECT lpad(trim(county_geoid),5,'0')   AS county_fips,
               lpad(trim(neighbor_geoid),5,'0') AS neighbor_fips,
               trim(county_name)                AS county_name,
               trim(neighbor_name)              AS neighbor_name,
               (trim(county_geoid) = trim(neighbor_geoid)) AS is_self,
               'census_county_adjacency_2010' AS source_version
        FROM adj
        WHERE neighbor_geoid IS NOT NULL AND county_geoid IS NOT NULL
    """).fetch_arrow_table()
    return _write_lance("census_county_adjacency", tbl,
                        btree=["county_fips", "neighbor_fips"], floor=15_000)


def build_census_county_gazetteer_2023(tmp: str) -> dict:
    import duckdb
    zpath = _download(URL_COUNTY_GAZ, f"{tmp}/county_gaz.zip")
    with zipfile.ZipFile(zpath) as zf:
        member = max((m for m in zf.infolist() if m.filename.lower().endswith(".txt")),
                     key=lambda m: m.file_size)
        zf.extract(member, tmp)
        txt = os.path.join(tmp, member.filename)
    con = duckdb.connect(":memory:")
    tbl = con.execute(f"""
        SELECT lpad(trim(GEOID),5,'0')            AS county_fips,
               trim(USPS)                          AS state_usps,
               trim(NAME)                          AS county_name,
               trim(ANSICODE)                      AS county_ns,
               TRY_CAST(trim(ALAND_SQMI)  AS DOUBLE) AS land_sqmi,
               TRY_CAST(trim(AWATER_SQMI) AS DOUBLE) AS water_sqmi,
               TRY_CAST(trim(INTPTLAT)  AS DOUBLE) AS lat,
               TRY_CAST(trim(INTPTLONG) AS DOUBLE) AS lon,
               'census_2023_gazetteer_counties' AS source_version
        FROM read_csv('{txt}', delim='\t', header=true, all_varchar=true)
        WHERE GEOID IS NOT NULL
    """).fetch_arrow_table()
    return _write_lance("census_county_gazetteer_2023", tbl,
                        btree=["county_fips"], floor=3_000, grain_key="county_fips")


BUILDERS = {
    "national_county2020": build_national_county2020,
    "national_state2020": build_national_state2020,
    "national_cousub2020": build_national_cousub2020,
    "census_county_cbsa_2023": build_census_county_cbsa_2023,
    "census_zcta_county_rel_2020": build_census_zcta_county_rel_2020,
    "census_county_adjacency": build_census_county_adjacency,
    "census_county_gazetteer_2023": build_census_county_gazetteer_2023,
}


def verify() -> None:
    import lance
    so = _r2_storage_options()
    for name in BUILDERS:
        try:
            ds = lance.dataset(_uri(name), storage_options=so)
            idx = sorted((i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                         for i in ds.list_indices())
            print(f"{name:32s} rows={ds.count_rows():>7,}  cols={len(ds.schema):>2}  indices={idx}")
        except Exception as e:
            print(f"{name:32s} MISSING/ERROR: {type(e).__name__}: {e}")


def main(argv: list[str]) -> None:
    if "--verify" in argv:
        verify()
        return
    only = [argv[i + 1] for i, a in enumerate(argv) if a == "--only" and i + 1 < len(argv)]
    targets = only or list(BUILDERS)
    unknown = [t for t in targets if t not in BUILDERS]
    if unknown:
        raise SystemExit(f"unknown dataset(s): {unknown}\nknown: {list(BUILDERS)}")
    results = []
    with tempfile.TemporaryDirectory(prefix="census_geo_") as tmp:
        for name in targets:
            print(f"\n── building {name} ──")
            results.append(BUILDERS[name](tmp))
    print("\n=== summary ===")
    for r in results:
        print(f"  {r['name']:32s} rows={r['rows']:>7,}  {r['uri']}")


if __name__ == "__main__":
    main(sys.argv[1:])
