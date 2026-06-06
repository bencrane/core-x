"""Shared transform constants for the Overture Places v2 (optimized) schema.
Imported by both the one-shot migration (optimize.py) and the go-forward ingest
(places.py) so any future re-ingest is born in the v2 layout. Pure SQL fragments +
the canonical index plan. No I/O, no side effects."""

SCHEMA_VERSION = "overture_places.v2"

# Hilbert space-filling sort/spatial key. GLOBAL LINEAR bounds so every coordinate
# (including the diagnostic's |lat|>85° mislocated outliers) maps validly. ST_QuadKey
# (Web-Mercator) was REJECTED for its ±85.0511° domain limit. 3-arg DOUBLE,DOUBLE,BOX_2D
# form; BOX_2D via ST_Extent(ST_MakeEnvelope(...)). Verified against DuckDB 1.5 spatial.
HILBERT_BOUNDS_SQL = "ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90))"
HILBERT_BOUNDS_TAG = "-180,-90,180,90"
HILBERT_EXPR_SQL = f"ST_Hilbert(longitude::DOUBLE, latitude::DOUBLE, {HILBERT_BOUNDS_SQL})"

# Canonical US subdivision whitelist: 50 states + DC + 5 inhabited US territories.
# Freely-associated SOVEREIGN states (FM, MH, PW) are NON-US → NULL.
USPS_VALID = (
    "'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',"
    "'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',"
    "'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',"
    "'VA','WA','WV','WI','WY','DC','PR','VI','GU','AS','MP'"
)
# Deterministic region normalization. upper/trim auto-fixes case variants (ca→CA);
# the whitelist keeps valid codes; explicit aliases map full names → USPS; everything
# else (foreign subdivisions, garbage) → NULL = "no valid US region".
REGION_NORMALIZE_SQL = f"""CASE
  WHEN UPPER(TRIM(region)) IN ({USPS_VALID}) THEN UPPER(TRIM(region))
  WHEN UPPER(TRIM(region)) = 'CALIFORNIA'           THEN 'CA'
  WHEN UPPER(TRIM(region)) = 'CALIF'                THEN 'CA'
  WHEN UPPER(TRIM(region)) = 'TEXAS'                THEN 'TX'
  WHEN UPPER(TRIM(region)) = 'FLORIDA'              THEN 'FL'
  WHEN UPPER(TRIM(region)) = 'NEW YORK'             THEN 'NY'
  WHEN UPPER(TRIM(region)) = 'OHIO'                 THEN 'OH'
  WHEN UPPER(TRIM(region)) = 'ARIZONA'              THEN 'AZ'
  WHEN UPPER(TRIM(region)) = 'PENNSYLVANIA'         THEN 'PA'
  WHEN UPPER(TRIM(region)) = 'VIRGINIA'             THEN 'VA'
  WHEN UPPER(TRIM(region)) = 'TENNESSEE'            THEN 'TN'
  WHEN UPPER(TRIM(region)) = 'NEVADA'               THEN 'NV'
  WHEN UPPER(TRIM(region)) = 'DELAWARE'             THEN 'DE'
  WHEN UPPER(TRIM(region)) = 'WYOMING'              THEN 'WY'
  WHEN UPPER(TRIM(region)) = 'NORTH DAKOTA'         THEN 'ND'
  WHEN UPPER(TRIM(region)) = 'DISTRICT OF COLUMBIA' THEN 'DC'
  ELSE NULL
END"""

# v2 index plan: drop the per-axis lon/lat BTREEs (proven pathological for 2-D bbox,
# 38.9s) for the single integer hilbert BTREE; add the category BITMAP.
OPTIMIZED_BTREE_INDEXES = ["id", "name", "postcode", "locality", "hilbert"]
OPTIMIZED_BITMAP_INDEXES = ["region", "category"]

# Per-row v2 projection. Constants (country/snapshot_date/release_tag/ingested_at)
# are demoted to schema metadata, NOT projected. `src` is a relation exposing the flat
# source columns (the committed Lance SoR, or the ingest geo CTE). ORDER BY clusters
# fragments by region then space-filling key.
def projection_sql(src: str) -> str:
    return f"""SELECT
    id,
    longitude,
    latitude,
    CAST({HILBERT_EXPR_SQL} AS UINTEGER) AS hilbert,
    {REGION_NORMALIZE_SQL} AS region,
    locality,
    postcode,
    name,
    category,
    CAST(confidence AS FLOAT) AS confidence
FROM {src}
ORDER BY region NULLS LAST, hilbert"""
