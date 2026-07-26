"""Unfurl + normalize the industries-served research strings → company_industry_mentions.

Sibling method to ``materialize_combo_equipment_needs.py`` (the equipment-mention cycle):
string processing ONLY — equate what is literally the same thing. NO taxonomy, NO semantic
grouping, NO canonical vertical list. "construction" / "construction industry" /
"commercial construction" stay THREE distinct strings here. When in doubt: distinct.

SOURCES (both read from the Lance mirrors; Postgres stays the read-only raw vault):
    s3://data-sink/active/industries_served/                  6,480 domain-keyed rows.
        List = ``industries_served`` (JSON-array STRING, parsed — never iterated as text),
        falling back to raw_payload ``$.industriesServed`` then ``$.industries`` (10 rows
        carry the older payload shape keyed ``industries``). Measured 2026-07-26: the
        fallback recovers 0 extra rows — the projection is complete.
    s3://data-sink/active/equipment_yard_industries_served/   885 UEI-keyed rows.
        List = raw_payload ``$.industriesServed``.

SUBJECT KEYS
    plane 'domain' → domain_norm, uei NULL.
    plane 'uei'    → uei, plus its researched domain. That payload has NO domain field, so
        the domain is taken from ``$.sources`` — the first URL whose host passes the
        canonical core.web_norm gate AND is not a generic/social host. sources[0] is
        sometimes linkedin.com, which is not the company's domain; taking it blindly would
        invent a wrong key. Where no source qualifies the domain is left NULL.

NORMALIZATION — literal equivalence only
    norm0     NFKC → curly quotes to ASCII → lowercase → collapse whitespace → strip
              wrapping quotes and trailing punctuation.
    fold key  ``&`` ↔ ``and``, then singularize the trailing token (conservative
              morphology), then delete every separator character [space - – — / _ ' .].
              So "oil & gas" = "oil and gas"; "earth-moving" = "earth moving" =
              "earthmoving"; "home owners" = "homeowners"; and the letter-spaced
              "c o m m e r c i a l" rejoins "commercial".
    canonical the fold group's most frequent observed surface form; ties break to the
              plural. Always an observed string — nothing is invented.

    DERIVATIONAL FOLDS ARE A CLOSED, AUDITED WHITELIST (_ADJ_NOUN below), applied only
    when the mention is that single word. There is no generative adjective→noun rule: the
    measured candidate set contains garbage that any such rule would fire on —
    "industrial"→"industri", "petrochemical"→"petrochemic", "automative"→"automation",
    "parking"→"park", "training"→"train", "building"→"build". Separating the safe pairs
    from those requires exactly the semantic judgment this cycle excludes, so the six safe
    pairs are enumerated rather than derived. The remaining candidates stay distinct and
    are reported for the operator's later grouping cycle.

SINGULAR/PLURAL (measured 2026-07-26, do not assume):
    Across all mentions singular leads 21,258 (63.1%) to 12,456 — but that is an artifact
    of mass nouns that have no plural at all ("construction" 1,488, "commercial" 795,
    "government" 321). The fold decision only exists where a group holds BOTH numbers:
    400 groups, and there the PLURAL is the frequency winner 291 times (72.8%). So
    per-group frequency decides and plural breaks ties.

OUTPUT  s3://data-sink/active/company_industry_mentions/
    domain_norm, uei, industry_mention, industry_mention_raw, source_plane, materialized_at
    BTREE on domain_norm and industry_mention. Full deterministic rebuild, no appends.

Run:
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/materialize_company_industry_mentions.py
"""
from __future__ import annotations

import os
import sys

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from core.web_norm import _bare_host, is_generic_domain, normalized_domain  # noqa: E402
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

DOMAIN_URI = "s3://data-sink/active/industries_served/"
UEI_URI = "s3://data-sink/active/equipment_yard_industries_served/"
OUT_URI = "s3://data-sink/active/company_industry_mentions/"
OUT_INDEXES = [("domain_norm", "BTREE"), ("industry_mention", "BTREE")]

# Trailing tokens ending in "s" that must not be singularized. The principle: a token
# belongs here when its singular denotes something DIFFERENT — "plastics"/"electronics"/
# "ceramics" are industries while "plastic"/"electronic"/"ceramic" are a material or an
# adjective; "mechanics" is a field while "mechanic" is a person. Plain plurals of the
# same referent do NOT belong here: "utilities"/"utility" (127 vs 77 in the data) and
# "logistics"/"logistic" (43 vs 1, the singular a typo) were removed after audit — the
# keep-list was blocking two legitimate folds.
_KEEP_S = ("gas", "lens", "bus", "chassis", "apparatus", "series", "species", "analysis",
           "axis", "basis", "ppe", "gps", "ups", "plus", "bias", "campus", "status",
           "focus", "virus", "census", "aerospace", "defense", "wellness", "business",
           "electronics", "plastics", "ceramics", "athletics", "cosmetics",
           "robotics", "diagnostics", "graphics", "mechanics", "physics", "economics",
           "politics")
_KEEP_S_RE = "|".join(_KEEP_S)

# Closed whitelist — adjective/noun pairs that are the same word, audited individually.
# Applied ONLY to single-word mentions. Direction is decided by frequency like any other
# fold; this list only declares the two forms equivalent.
_ADJ_NOUN = {
    "agricultural": "agriculture",
    "governmental": "government",
    "educational": "education",
    "environmental": "environment",
    "architectural": "architecture",
    "recreational": "recreation",
}
_ADJ_NOUN_SQL = ", ".join(f"'{k}': '{v}'" for k, v in _ADJ_NOUN.items())

MACROS = f"""
CREATE OR REPLACE MACRO norm0(s) AS
    trim(
      regexp_replace(
        regexp_replace(
          trim(
            regexp_replace(
              lower(
                replace(replace(replace(replace(nfc_normalize(CAST(s AS VARCHAR)),
                  chr(8217), ''''), chr(8216), ''''), chr(8220), '"'), chr(8221), '"')
              ),
            '\\s+', ' ', 'g')
          ),
        '^["''`]+', '', ''),
      '[\\s.,;:!?"''`/\\-]+$', '', '')
    );

-- "&" and "and" are the same conjunction.
CREATE OR REPLACE MACRO amp(s) AS
    trim(regexp_replace(replace(s, '&', ' and '), '\\s+', ' ', 'g'));

CREATE OR REPLACE MACRO sing_last(s) AS
    CASE
        WHEN regexp_matches(s, '(^|\\s)({_KEEP_S_RE})$')      THEN s
        WHEN regexp_matches(s, '[a-z]{{2,}}ies$')             THEN regexp_replace(s, 'ies$', 'y', '')
        WHEN regexp_matches(s, '(sses|xes|zes|ches|shes)$')   THEN regexp_replace(s, 'es$', '', '')
        WHEN regexp_matches(s, '[a-z]{{3,}}s$')
             AND NOT regexp_matches(s, '[sui]s$')             THEN regexp_replace(s, 's$', '', '')
        ELSE s
    END;

-- Closed adjective/noun whitelist, single-word mentions only.
CREATE OR REPLACE MACRO adj_noun(s) AS
    coalesce(MAP {{{_ADJ_NOUN_SQL}}}[s], s);

CREATE OR REPLACE MACRO fold_key(s) AS
    regexp_replace(
        sing_last(adj_noun(amp(s))),
        '[ \\-' || chr(8211) || chr(8212) || '/_''.]', '', 'g');

CREATE OR REPLACE MACRO is_plural(s) AS sing_last(amp(s)) <> amp(s);

-- Canonical domain rules, imported from core.web_norm — never re-inlined.
CREATE OR REPLACE MACRO bare_host(u) AS {_bare_host('u')};
CREATE OR REPLACE MACRO norm_dom(u) AS {normalized_domain('bare_host(u)')};
CREATE OR REPLACE MACRO generic_dom(h) AS {is_generic_domain('h')};
"""

# Unfurl both planes to (domain_norm, uei, raw mention, plane).
UNFURL_SQL = """
CREATE OR REPLACE TEMP TABLE raw_mentions AS
WITH p1 AS (
    SELECT domain_norm,
           coalesce(
               TRY_CAST(industries_served AS VARCHAR[]),
               TRY_CAST(json_extract(raw_payload, '$.industriesServed') AS VARCHAR[]),
               TRY_CAST(json_extract(raw_payload, '$.industries') AS VARCHAR[]),
               []::VARCHAR[]
           ) AS items
    FROM domain_plane
),
p2 AS (
    SELECT uei,
           coalesce(TRY_CAST(json_extract(raw_payload, '$.industriesServed') AS VARCHAR[]),
                    []::VARCHAR[]) AS items,
           list_filter(
               coalesce(TRY_CAST(json_extract(raw_payload, '$.sources') AS VARCHAR[]),
                        []::VARCHAR[]),
               u -> norm_dom(u) IS NOT NULL AND NOT generic_dom(norm_dom(u))
           ) AS good_sources
    FROM uei_plane
)
SELECT domain_norm, NULL AS uei, unnest(items) AS mention_raw, 'domain' AS source_plane
FROM p1
UNION ALL
SELECT norm_dom(good_sources[1]) AS domain_norm, uei, unnest(items) AS mention_raw,
       'uei' AS source_plane
FROM p2
"""

# Surface forms → fold groups → canonical (frequency, ties to plural) → output grain.
MART_SQL = """
WITH surface AS (
    SELECT domain_norm, uei, source_plane,
           mention_raw AS industry_mention_raw,
           norm0(mention_raw) AS form
    FROM raw_mentions
    WHERE norm0(mention_raw) <> ''
),
form_counts AS (
    SELECT form, fold_key(form) AS fk, count(*) AS n FROM surface GROUP BY 1, 2
),
canon AS (
    SELECT fk, form AS canonical FROM (
        SELECT fk, form,
               row_number() OVER (PARTITION BY fk
                   ORDER BY n DESC, is_plural(form) DESC, length(form) DESC, form ASC) rn
        FROM form_counts
    ) WHERE rn = 1
)
SELECT DISTINCT
    s.domain_norm,
    s.uei,
    c.canonical                AS industry_mention,
    s.industry_mention_raw,
    s.source_plane,
    now()                      AS materialized_at
FROM surface s
JOIN form_counts f ON f.form = s.form
JOIN canon c       ON c.fk   = f.fk
ORDER BY s.source_plane, s.domain_norm, s.uei, industry_mention, s.industry_mention_raw
"""


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


def build(con, so: dict):
    import lance

    dp = lance.dataset(DOMAIN_URI, storage_options=so).to_table(
        columns=["domain_norm", "industries_served", "raw_payload"])
    up = lance.dataset(UEI_URI, storage_options=so).to_table(columns=["uei", "raw_payload"])
    con.register("domain_plane", dp)
    con.register("uei_plane", up)
    print(f"planes: domain {dp.num_rows:,} rows · uei {up.num_rows:,} rows")

    con.execute(MACROS)
    con.execute(UNFURL_SQL)
    con.execute(f"CREATE OR REPLACE TEMP TABLE mart AS {MART_SQL}")

    # ── coverage ──────────────────────────────────────────────────────────────
    # The domain plane is append-only research history: 6,480 ROWS over fewer distinct
    # DOMAINS, so rows and subjects are counted separately — mixing the two units
    # misstates the empty count.
    d_rows, d_subj, d_rows_with, d_subj_with = con.execute("""
        SELECT (SELECT count(*) FROM domain_plane),
               (SELECT count(DISTINCT domain_norm) FROM domain_plane),
               (SELECT count(*) FROM domain_plane WHERE length(coalesce(
                    TRY_CAST(industries_served AS VARCHAR[]),
                    TRY_CAST(json_extract(raw_payload,'$.industriesServed') AS VARCHAR[]),
                    TRY_CAST(json_extract(raw_payload,'$.industries') AS VARCHAR[]),
                    []::VARCHAR[])) > 0),
               (SELECT count(DISTINCT domain_norm) FROM mart WHERE source_plane='domain')
    """).fetchone()
    u_tot, u_with = con.execute("""
        SELECT (SELECT count(DISTINCT uei) FROM uei_plane),
               (SELECT count(DISTINCT uei) FROM mart WHERE source_plane='uei')
    """).fetchone()
    print(f"coverage domain plane: {d_rows:,} rows over {d_subj:,} distinct domains")
    print(f"  rows with ≥1 item:    {d_rows_with:,} · {d_rows - d_rows_with:,} empty rows")
    print(f"  domains with ≥1 mention: {d_subj_with:,} · "
          f"{d_subj - d_subj_with:,} domains empty on every row")
    print(f"coverage uei plane:    {u_with:,}/{u_tot:,} UEIs with ≥1 mention · "
          f"{u_tot - u_with:,} empty")
    u_dom = con.execute(
        "SELECT count(DISTINCT uei) FROM mart WHERE source_plane='uei' "
        "AND domain_norm IS NOT NULL").fetchone()[0]
    print(f"  uei subjects with a resolvable researched domain: {u_dom:,}/{u_with:,}")

    rows, items = con.execute(
        "SELECT count(*), count(DISTINCT industry_mention) FROM mart").fetchone()
    print(f"mart: {rows:,} rows · {items:,} distinct canonical mentions")

    # ── gate: canonical is always an observed surface form ────────────────────
    invented = con.execute("""
        SELECT count(*) FROM (SELECT DISTINCT industry_mention FROM mart) c
        WHERE NOT EXISTS (SELECT 1 FROM mart m
                          WHERE norm0(m.industry_mention_raw) = c.industry_mention)
    """).fetchone()[0]
    assert invented == 0, f"{invented} canonical values are not observed surface forms"
    print(f"gate: invented canonical strings = {invented} ✓")

    tbl = con.execute("SELECT * FROM mart").to_arrow_table()
    ds = write_indexed_dataset(tbl, OUT_URI, OUT_INDEXES, so)
    print(f"published {OUT_URI} rows={ds.count_rows():,} version={ds.version}")


def main() -> None:
    so = _r2_storage_options()
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC';")
    try:
        build(con, so)
    finally:
        con.close()


if __name__ == "__main__":
    main()
