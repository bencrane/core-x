"""Shared transform constants for the Overture Places schema.

Imported by the one-shot migration (optimize.py), the v3 resolution-keys re-ingest
(reingest_v3.py), and the go-forward ingest (places.py) so every write path is born
in the current layout. Pure SQL fragments + the canonical index plan. No I/O.

v3 (overture_places.v3) — ADDS the deterministic + high-value resolution keys the v2
schema dropped: registrable `domain` (from websites[]), canonical `phone` (E.164/NANP
from phones[]), raw `street` (addresses[1].freeform), and richer `taxonomy`
(taxonomy.primary). `domain` and `phone` get BTREE scalar indexes — an external record
resolves to a place by domain / phone / full street, not just fuzzy name + geo.
"""

SCHEMA_VERSION = "overture_places.v3"

# ── Hilbert space-filling sort/spatial key (unchanged from v2) ────────────────
HILBERT_BOUNDS_SQL = "ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90))"
HILBERT_BOUNDS_TAG = "-180,-90,180,90"
HILBERT_EXPR_SQL = f"ST_Hilbert(longitude::DOUBLE, latitude::DOUBLE, {HILBERT_BOUNDS_SQL})"

# ── region normalization (unchanged from v2) ─────────────────────────────────
USPS_VALID = (
    "'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',"
    "'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',"
    "'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',"
    "'VA','WA','WV','WI','WY','DC','PR','VI','GU','AS','MP'"
)
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


# ── domain normalization ─────────────────────────────────────────────────────
# website string -> registrable domain (eTLD+1) or NULL. ALL regexp_replace use the
# 'g' (global) flag — DuckDB's default replaces only the FIRST match. Steps:
#   1. lower+trim  2. strip leading scheme(s) repeatedly (http://https://…)
#   3. strip userinfo (user:pass@)  4. strip path/port/query/fragment
#   5. strip leading www.  6. strip every char not [a-z0-9.-]
#   7. registrable: 3 labels iff host has >=3 labels AND penultimate is a known
#      multi-part suffix token AND last label is a 2-letter ccTLD; else last 2 labels.
#   8. FINAL GATE: keep only a clean registrable shape, and reject IPv4 literals.
def domain_sql(col: str) -> str:
    host = (
        "regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        f"lower(trim({col})), '^([a-z][a-z0-9+.-]*://)+', '', 'g'), "
        "'^[^/@]*@', '', 'g'), '[/:?#].*$', '', 'g'), "
        r"'^www\.', '', 'g'), '[^a-z0-9.-]', '', 'g')"
    )
    nlab = f"(length({host}) - length(replace({host}, '.', '')) + 1)"
    reg = (
        "CASE WHEN " + nlab + " >= 3 "
        f"AND regexp_extract({host}, '([^.]+)\\.([^.]+)$', 1) IN "
        "('co','com','org','net','gov','edu','ac','gob','go') "
        f"AND length(regexp_extract({host}, '\\.([^.]+)$', 1)) = 2 "
        f"THEN regexp_extract({host}, '([^.]+\\.[^.]+\\.[^.]+)$', 1) "
        f"ELSE regexp_extract({host}, '([^.]+\\.[^.]+)$', 1) END"
    )
    return (
        f"CASE WHEN regexp_full_match({reg}, '([a-z0-9]([a-z0-9-]*[a-z0-9])?\\.)+[a-z]{{2,}}') "
        f"AND NOT regexp_full_match({host}, '[0-9]{{1,3}}(\\.[0-9]{{1,3}}){{3}}') "
        f"THEN {reg} ELSE NULL END"
    )


# ── phone normalization ──────────────────────────────────────────────────────
# phone string -> +1XXXXXXXXXX (NANP/E.164) or NULL. Extension stripped BEFORE digit
# extraction (else "ext 4" leaks a trailing 4). 'g' flag mandatory. NANP validity:
# area + exchange leading digit must be 2-9.
def phone_sql(col: str) -> str:
    digits = (
        f"regexp_replace(regexp_replace({col}, "
        "'(?i)\\s*(ext|extension|x|#)\\.?\\s*[0-9]+\\s*$', '', 'g'), "
        "'[^0-9]', '', 'g')"
    )
    ten = (
        f"CASE WHEN length({digits})=11 AND left({digits},1)='1' THEN substr({digits},2) "
        f"WHEN length({digits})=10 THEN {digits} ELSE NULL END"
    )
    return (
        f"CASE WHEN ({ten}) IS NOT NULL "
        f"AND regexp_full_match(({ten}), '[2-9][0-9]{{2}}[2-9][0-9]{{6}}') "
        f"THEN '+1' || ({ten}) ELSE NULL END"
    )


# ── index plan ───────────────────────────────────────────────────────────────
# v3 adds the two deterministic resolution keys as BTREE: domain, phone.
OPTIMIZED_BTREE_INDEXES = ["id", "name", "postcode", "locality", "hilbert", "domain", "phone"]
OPTIMIZED_BITMAP_INDEXES = ["region", "category"]


def projection_sql(src: str) -> str:
    """Per-row v3 projection. `src` exposes the flat source columns (the committed
    Lance SoR for a transform, or the ingest geo/flat CTE for an ingest). Constants
    (country/snapshot_date/release_tag/ingested_at) are demoted to schema metadata.
    domain/phone/street/taxonomy are the v3 resolution-key additions. ORDER BY clusters
    fragments by region then space-filling key."""
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
    taxonomy,
    CAST(confidence AS FLOAT) AS confidence,
    domain,
    phone,
    street
FROM {src}
ORDER BY region NULLS LAST, hilbert"""
