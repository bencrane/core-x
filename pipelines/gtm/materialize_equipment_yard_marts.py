"""Derive equipment-yard marts from the verbatim research mirror → Lance SoR.

Reads ``gtm.equipment_yard_website_research`` (HQX Postgres — the raw landed
payloads), explodes categories + equipmentItems, classifies every phrase into
the demand-side bucket taxonomy, and publishes two marts:

  equipment_yard_inventory  — 1/(uei × phrase): phrase, kind (item|category),
                              carried category, bucket, match_source.
  equipment_yard_profile    — 1/uei: provider verdict fields + bucket set,
                              primary_bucket (by instance count), item/category
                              counts, matched/unmatched instance counts, plus
                              (from gtm.equipment_yard_industries_served, latest
                              row per uei) industries_served (lowercased/trimmed
                              distinct list) and serves_government (any entry
                              matching government/municipal/federal/military/
                              public sector/dod).

CLASSIFICATION IS FULLY DETERMINISTIC AT RUN TIME — no model calls:
  1. regex rules below (first match wins; the 5 heavy-iron bucket names match
     ``naics_psc_equipment_needs`` verbatim so yard supply joins combo demand
     on bucket equality);
  2. ``data/equipment_phrase_alias.csv`` — the curated alias table for phrases
     the rules miss (produced once by in-session agent classification of the
     ≥2-yard residual head, 2026-07-18; extend by hand as new vocabulary
     appears);
  3. a phrase neither resolves is classified through its carried categoryName
     (payload-native inheritance — model numbers resolve this way);
  4. still nothing → bucket NULL (kept: unresolved is a fact, not a drop).

Run (in-session scale):
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/materialize_equipment_yard_marts.py
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

INVENTORY_URI = "s3://data-sink/active/equipment_yard_inventory/"
PROFILE_URI = "s3://data-sink/active/equipment_yard_profile/"
FOOTPRINT_URI = "s3://data-sink/active/equipment_provider_service_footprint/"
ALIAS_CSV = os.path.join(os.path.dirname(__file__), "data", "equipment_phrase_alias.csv")

# ── deterministic state extraction (service-area `parsed` strings) ───────────
_US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}
_STATE_CODES = set(_US_STATES.values())
_STATE_NAME_RE = re.compile(
    "|".join(sorted((re.escape(k) for k in _US_STATES), key=len, reverse=True)))
_CODE_TOKEN_RE = re.compile(r"\b([A-Z]{2})\b")


def parse_states(parsed: str | None) -> set[str]:
    """States named in a service-area `parsed` string — full names (case-
    insensitive) plus uppercase two-letter codes. Deterministic; unknown
    tokens ignored."""
    if not parsed:
        return set()
    out = {_US_STATES[m] for m in _STATE_NAME_RE.findall(parsed.lower())}
    out |= {c for c in _CODE_TOKEN_RE.findall(parsed) if c in _STATE_CODES}
    return out

# First match wins; ordered specific → general. The first five bucket names are
# the naics_psc_equipment_needs vocabulary VERBATIM (the demand join key).
RULES: list[tuple[str, str]] = [
    ("aerial_access", r"boom lift|scissor lift|man ?lift|aerial|towable lift|vertical mast|articulating|telescopic (boom|lift)|personnel lift|push[- ]around lift"),
    ("material_handling_cranes", r"crane|forklift|fork lift|telehandler|reach (truck|forklift)|pallet (jack|truck)|hoist|winch|material lift|conveyor|rigging|spreader bar|carry ?deck|boom truck"),
    ("heavy_earthmoving_civil", r"excavator|dozer|bulldozer|backhoe|grader|skid ?steer|track loader|wheel loader|front[- ]end loader|loader|trencher|compactor|compaction|roller|paver|scraper|auger|drill rig|breaker|hammer|milling|crusher|screen(er|ing plant)?|dirt|earthmov|undercarriage|ripper|grapple|bucket|attachment"),
    ("trucks_heavy_haul", r"dump truck|water truck|haul|lowboy|flatbed|tractor trailer|semi|trailer|utility (vehicle|cart)|utv|gator|golf cart|street sweeper|vacuum truck|hydro ?vac|truck"),
    ("industrial_power_support", r"generator|genset|light tower|light plant|air compressor|compressor|pump|heater|welder|welding|power distribution|distribution panel|transformer|chiller|hvac|air conditioner|a/c|dehumidifier|air mover|blower|fan|fuel tank|water tank|poly tank|frac tank|tank|containment|spillguard|berm|manifold|hose|cable|temporary power|e-?contain|heating|furnace|lighting|light plant|power|electrical|boiler|steam"),
    ("water_filtration_irrigation", r"irrigation|filtration|filter|oil water separator|separator|polymer|water treatment|dewatering|pipe|drip|sprinkler|center pivot|pivot|lateral|well point"),
    ("concrete_masonry", r"concrete|cement|mortar|mixer|trowel|screed|vibrator|rebar|masonry|grout|shotcrete|core (drill|bore)|saw ?cut"),
    ("tools_small_equipment", r"saw|drill|grinder|nailer|sander|router|jack ?hammer|chipping|impact|wrench|generator ?inverter|pressure wash|blast|paint|ladder|scaffold|shoring|fencing|barricade|traffic|safety|protection|ppe|signage"),
    ("landscaping_agriculture", r"mower|tiller|stump|chipper|brush|aerator|seeder|sod|tractor|attachment mower|landscap|trimmer|blower ?leaf"),
    ("event_av_party", r"tent|table|chair|stage|staging|dance floor|linen|av |audio|visual|projector|led (display|wall)|sound|speaker|microphone|bounce|inflatable|party|wedding|catering|photo ?booth|valet|coach|charter|limo"),
    ("climate_survey_other", r"survey|laser|level|gps|trimble|drone|camera|monitoring|metering|scale|instrumentation|automation|controls"),
]
_COMPILED = [(b, re.compile(rx)) for b, rx in RULES]


def _load_alias() -> dict[str, str]:
    with open(ALIAS_CSV, newline="") as f:
        return {r["phrase"]: r["bucket"] for r in csv.DictReader(f) if r["bucket"]}


def _rules(s: str) -> str | None:
    for b, rx in _COMPILED:
        if rx.search(s):
            return b
    return None


def classify(phrase: str, category: str, alias: dict[str, str]) -> tuple[str | None, str | None]:
    """→ (bucket, match_source ∈ {rules, alias, category_rules, category_alias})."""
    b = _rules(phrase)
    if b:
        return b, "rules"
    if phrase in alias:
        return alias[phrase], "alias"
    if category:
        b = _rules(category)
        if b:
            return b, "category_rules"
        if category in alias:
            return alias[category], "category_alias"
    return None, None


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def main() -> None:
    alias = _load_alias()
    dsn = os.environ.get("HQX_DB_URL_DIRECT") or os.environ["HQX_DB_URL_POOLED"]
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{dsn}' AS hqx (TYPE postgres, READ_ONLY)")

    con.execute("""
    CREATE TABLE grain AS
    SELECT uei, 'item' AS kind,
           LOWER(TRIM(json_extract_string(e.value, '$.name'))) AS phrase,
           LOWER(TRIM(COALESCE(json_extract_string(e.value, '$.categoryName'), ''))) AS category
    FROM (SELECT uei, CAST(raw_payload AS JSON) AS p
          FROM hqx.gtm.equipment_yard_website_research) t,
         json_each(COALESCE(json_extract(t.p, '$.equipmentItems'), '[]'::JSON)) e
    WHERE TRIM(COALESCE(json_extract_string(e.value, '$.name'), '')) <> ''
    UNION ALL
    SELECT uei, 'category', LOWER(TRIM(json_extract_string(c.value, '$.name'))), ''
    FROM (SELECT uei, CAST(raw_payload AS JSON) AS p
          FROM hqx.gtm.equipment_yard_website_research) t,
         json_each(COALESCE(json_extract(t.p, '$.categories'), '[]'::JSON)) c
    WHERE TRIM(COALESCE(json_extract_string(c.value, '$.name'), '')) <> ''
    """)
    rows = con.execute("SELECT uei, kind, phrase, category FROM grain").fetchall()
    classified = [
        (uei, kind, phrase, category, *classify(phrase, category, alias))
        for (uei, kind, phrase, category) in rows
    ]
    con.execute("""
    CREATE TABLE inv (uei VARCHAR, kind VARCHAR, phrase VARCHAR, category VARCHAR,
                      bucket VARCHAR, match_source VARCHAR)""")
    con.executemany("INSERT INTO inv VALUES (?,?,?,?,?,?)", classified)
    con.execute("""
    CREATE TABLE inventory AS
    SELECT uei, kind, phrase, category, bucket, match_source
    FROM (SELECT DISTINCT * FROM inv) ORDER BY uei, kind, phrase""")

    con.execute("""
    CREATE TABLE verdict AS
    SELECT uei,
           (json_extract_string(p, '$.reasoning') ILIKE 'not an equipment provider%') AS explicit_negative,
           json_extract_string(p, '$.confidence') AS confidence,
           json_array_length(COALESCE(json_extract(p, '$.equipmentItems'), '[]'::JSON)) AS n_items,
           json_array_length(COALESCE(json_extract(p, '$.categories'), '[]'::JSON)) AS n_categories
    FROM (SELECT uei, CAST(raw_payload AS JSON) AS p
          FROM hqx.gtm.equipment_yard_website_research)""")
    def service_geo(table: str, key_col: str) -> list[tuple]:
        """Latest payload per key → (key, footprint_types, service_states,
        has_nationwide, n_areas). States parsed deterministically from the
        entries' `parsed` strings; nationwide entries contribute no states."""
        rows = con.execute(f"""
            SELECT {key_col}, CAST(raw_payload AS VARCHAR)
            FROM (SELECT {key_col}, raw_payload,
                         ROW_NUMBER() OVER (PARTITION BY {key_col}
                                            ORDER BY landed_at DESC) AS rn
                  FROM hqx.gtm.{table}) t
            WHERE rn = 1""").fetchall()
        out = []
        for key, payload in rows:
            try:
                areas = json.loads(payload).get("serviceAreas") or []
            except (json.JSONDecodeError, AttributeError):
                areas = []
            types, states = set(), set()
            for a in areas:
                if not isinstance(a, dict):
                    continue
                t = (a.get("type") or "").strip()
                if t:
                    types.add(t)
                if t != "nationwide":
                    states |= parse_states(a.get("parsed"))
            out.append((key, sorted(types), sorted(states),
                        "nationwide" in types, len(areas)))
        return out

    con.execute("""CREATE TABLE geo_uei (uei VARCHAR, footprint_types VARCHAR[],
                   service_states VARCHAR[], has_nationwide BOOLEAN, n_service_areas INT)""")
    con.executemany("INSERT INTO geo_uei VALUES (?,?,?,?,?)",
                    service_geo("equipment_yard_service_areas", "uei"))
    con.execute("""CREATE TABLE geo_dom (domain_norm VARCHAR, footprint_types VARCHAR[],
                   service_states VARCHAR[], has_nationwide BOOLEAN, n_service_areas INT)""")
    con.executemany("INSERT INTO geo_dom VALUES (?,?,?,?,?)",
                    service_geo("equipment_provider_service_areas", "domain_norm"))

    con.execute("""
    CREATE TABLE industries AS
    SELECT uei,
           list_sort(list_distinct(list(industry))) AS industries_served,
           BOOL_OR(industry SIMILAR TO
             '.*(government|municipal|federal|military|public sector|dod).*') AS serves_government
    FROM (
      SELECT t.uei, LOWER(TRIM(json_extract_string(i.value, '$'))) AS industry
      FROM (SELECT uei, CAST(raw_payload AS JSON) AS p,
                   ROW_NUMBER() OVER (PARTITION BY uei ORDER BY landed_at DESC) AS rn
            FROM hqx.gtm.equipment_yard_industries_served) t,
           json_each(COALESCE(json_extract(t.p, '$.industriesServed'), '[]'::JSON)) i
      WHERE t.rn = 1 AND TRIM(COALESCE(json_extract_string(i.value, '$'), '')) <> '')
    GROUP BY 1""")
    con.execute("""
    CREATE TABLE profile AS
    SELECT v.uei,
           NOT v.explicit_negative AND v.n_items + v.n_categories > 0 AS is_equipment_provider,
           v.explicit_negative, v.confidence, v.n_items, v.n_categories,
           b.buckets, b.primary_bucket,
           COALESCE(b.matched_instances, 0) AS matched_instances,
           COALESCE(b.unmatched_instances, 0) AS unmatched_instances,
           i.industries_served,
           COALESCE(i.serves_government, FALSE) AS serves_government,
           g.footprint_types, g.service_states,
           COALESCE(g.has_nationwide, FALSE) AS has_nationwide,
           COALESCE(g.n_service_areas, 0) AS n_service_areas
    FROM verdict v
    LEFT JOIN (
      SELECT uei,
             list_sort(list_distinct(list_filter(list(bucket), x -> x IS NOT NULL))) AS buckets,
             arg_max(bucket, cnt) FILTER (WHERE bucket IS NOT NULL) AS primary_bucket,
             SUM(cnt) FILTER (WHERE bucket IS NOT NULL) AS matched_instances,
             SUM(cnt) FILTER (WHERE bucket IS NULL) AS unmatched_instances
      FROM (SELECT uei, bucket, COUNT(*) AS cnt FROM inv GROUP BY 1, 2)
      GROUP BY 1) b USING (uei)
    LEFT JOIN industries i USING (uei)
    LEFT JOIN geo_uei g USING (uei)
    ORDER BY v.uei""")

    so = _r2_storage_options()
    inv_tbl = con.execute("SELECT * FROM inventory").to_arrow_table()
    prof_tbl = con.execute("SELECT * FROM profile").to_arrow_table()
    fp_tbl = con.execute("SELECT * FROM geo_dom ORDER BY domain_norm").to_arrow_table()
    ds_i = write_indexed_dataset(inv_tbl, INVENTORY_URI, [("uei", "BTREE")], so)
    ds_p = write_indexed_dataset(prof_tbl, PROFILE_URI, [("uei", "BTREE")], so)
    ds_f = write_indexed_dataset(fp_tbl, FOOTPRINT_URI, [("domain_norm", "BTREE")], so)
    cov = con.execute(
        "SELECT COUNT(*) FILTER (WHERE bucket IS NOT NULL) * 1.0 / COUNT(*) FROM inv"
    ).fetchone()[0]
    print(f"published {INVENTORY_URI} rows={ds_i.count_rows():,}")
    print(f"published {PROFILE_URI} rows={ds_p.count_rows():,}")
    print(f"published {FOOTPRINT_URI} rows={ds_f.count_rows():,}")
    print(f"instance bucket coverage: {cov:.1%}")


if __name__ == "__main__":
    main()
