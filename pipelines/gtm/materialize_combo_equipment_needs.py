"""Materialize the combo equipment-needs LLM landings → Lance SoR on R2 (two datasets).

Source (read-only, the immutable raw vault):
    hqx  gtm.combo_work_summary_equipment_needs  — 13,817 rows, one per prime-award
    (naics_code, psc_code, model_id) combo. ``raw_payload`` (jsonb) carries the
    GPT-5.4-nano verdict; the equipment list is the comma-separated
    ``raw_payload->>'response'`` (e.g. "laboratory instruments, laboratory supplies").

Targets:
    1. s3://data-sink/active/combo_work_summary_equipment_needs/  — VERBATIM mirror,
       every source column, no explode. BTREE naics_code, psc_code.
    2. s3://data-sink/active/combo_equipment_mentions/            — the unfurled +
       normalized mention grain: one row per (naics_code, psc_code, equipment item).
       BTREE naics_code, psc_code, equipment_item.

Both are FULL REBUILDS from raw each run — deterministic, no appends, snapshot-overwrite.

UNFURL — separators, measured not assumed (2026-07-26, n=13,803 non-null responses):
    ``,``      the delimiter. 13,127 responses use it; 676 are single-item.
    ``;``      appears in 6 responses, always as a genuine item delimiter following an
               LLM aside ("… software-only; printers, scanners"). Split on it.
    `` and ``  NOT a delimiter. All 1,779 occurrences are internal to compound noun
               phrases — "jigs and fixtures", "pick-and-place machines", "temperature
               and humidity sensors" — including all 109 that land in a response's
               final comma-field (the position where an Oxford-less list would put a
               real delimiter). Splitting on it would shred items. Left intact.
    Newlines / tabs / pipes / bullets: zero occurrences.

NORMALIZE — spelling only. Nothing semantic: no synonym merges ("dozer" and "bulldozer"
stay separate), no grouping, no categorization, no relevance judgment.
    norm0     NFKC → curly-quote fold → lowercase → collapse whitespace → strip wrapping
              quotes and trailing .,;:!? — this is the surface form.
    fold key  singularize the trailing token (conservative morphology, see SING_SQL),
              then delete every separator character [space - – — / _ ' .]. So
              "bull dozer" = "bull-dozer" = "bulldozers" = "bulldozer".
    canonical the fold group's most frequent surface form. Ties break to the PLURAL —
              the globally dominant number: 220,949 of 239,766 mentions (92.2%) and
              26,226 of 29,775 distinct surface forms (88.1%) are plural. Frequency
              leads rather than a blanket plural rewrite so that mass nouns keep their
              observed form ("scaffolding" 1,108 > "scaffoldings" 24; "conduit" 19 >
              "conduits" 6; "body armor" 18 > "body armors" 2). Where both numbers are
              present in a group, plural is the frequency winner 45/51 times anyway.
    The canonical is ALWAYS an observed surface form — no string is ever invented.

The 550 multi-form fold groups were audited one by one: all are pure spelling / spacing /
hyphenation / plural variants. Zero semantic merges.

DuckDB performs 100% of the transform (SQL macros below); Python is I/O only.

Run (in-session scale — 13.8k combos → ~240k mentions, no Modal):
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/materialize_combo_equipment_needs.py            # both
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/materialize_combo_equipment_needs.py --only raw
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/materialize_combo_equipment_needs.py --only mentions
"""
from __future__ import annotations

import argparse
import os
import sys

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

SOURCE = "hqx.gtm.combo_work_summary_equipment_needs"

RAW_URI = "s3://data-sink/active/combo_work_summary_equipment_needs/"
RAW_INDEXES = [("naics_code", "BTREE"), ("psc_code", "BTREE")]

MENTIONS_URI = "s3://data-sink/active/combo_equipment_mentions/"
MENTIONS_INDEXES = [("naics_code", "BTREE"), ("psc_code", "BTREE"),
                    ("equipment_item", "BTREE")]

# Verbatim mirror — every source column, in ordinal order, plus the lineage stamp.
# jsonb → VARCHAR (lossless JSON text); text[] rides through as list<string>.
RAW_SQL = f"""
SELECT
    naics_code,
    psc_code,
    model_id,
    source,
    CAST(raw_payload AS VARCHAR)      AS raw_payload,
    landed_at,
    proposed_equipment_needs,
    reasoning,
    confidence,
    in_scope,
    equipment_buckets,
    primary_bucket,
    core_phrase_count,
    other_phrase_count,
    now()                             AS materialized_at
FROM {SOURCE}
ORDER BY naics_code, psc_code, model_id
"""

# ── normalization macros — the whole contract, in DuckDB SQL ────────────────────
#
# Trailing tokens that end in "s" but are not plural (or whose "singular" is not a
# word). The fold key is internal, so a miss here only prevents a fold; it never
# corrupts an output string.
_KEEP_S = ("gas", "lens", "bus", "chassis", "apparatus", "series", "species",
           "analysis", "axis", "basis", "ppe", "gps", "ups", "plus", "bias", "iris",
           "canvas", "atlas", "status", "focus", "radius", "census", "virus", "bonus",
           "campus", "cactus")
_KEEP_S_RE = "|".join(_KEEP_S)

MACROS = f"""
-- Surface form: NFKC, curly quotes folded to ASCII, lowercased, whitespace collapsed,
-- wrapping quotes and trailing sentence punctuation stripped.
CREATE OR REPLACE MACRO norm0(s) AS
    trim(
      regexp_replace(
        regexp_replace(
          trim(
            regexp_replace(
              lower(
                replace(replace(replace(replace(nfc_normalize(s),
                  chr(8217), ''''), chr(8216), ''''), chr(8220), '"'), chr(8221), '"')
              ),
            '\\s+', ' ', 'g')
          ),
        '^["''`]+', '', ''),
      '[\\s.,;:!?"''`]+$', '', '')
    );

-- Conservative morphological singularizer applied to the FINAL token of a phrase.
-- Order matters: keep-list, then -ies, then -sses/-xes/-zes/-ches/-shes, then bare -s
-- (never after s/u/i, never on tokens of 3 chars or fewer).
CREATE OR REPLACE MACRO sing_last(s) AS
    CASE
        WHEN regexp_matches(s, '(^|\\s)({_KEEP_S_RE})$')      THEN s
        WHEN regexp_matches(s, '[a-z]{{2,}}ies$')             THEN regexp_replace(s, 'ies$', 'y', '')
        WHEN regexp_matches(s, '(sses|xes|zes|ches|shes)$')   THEN regexp_replace(s, 'es$', '', '')
        WHEN regexp_matches(s, '[a-z]{{3,}}s$')
             AND NOT regexp_matches(s, '[sui]s$')             THEN regexp_replace(s, 's$', '', '')
        ELSE s
    END;

-- Fold key: singular trailing token with every separator character deleted, so that
-- spacing / hyphenation / punctuation variants of one item collapse together.
CREATE OR REPLACE MACRO fold_key(s) AS
    regexp_replace(sing_last(s), '[ \\-' || chr(8211) || chr(8212) || '/_''.]', '', 'g');

-- Is this surface form plural? (i.e. the singularizer changed its trailing token)
CREATE OR REPLACE MACRO is_plural(s) AS sing_last(s) <> s;
"""

# ── the unfurl + normalize projection ──────────────────────────────────────────
#
#  split   → one row per comma/semicolon-delimited item, empties dropped
#  surface → norm0 of each item, keyed by fold_key
#  canon   → per fold group: most frequent surface form, ties to the plural
#            (row_number over count DESC, is_plural DESC, length DESC, form ASC)
MENTIONS_SQL = f"""
WITH split AS (
    SELECT
        naics_code,
        psc_code,
        trim(item) AS equipment_item_raw
    FROM {SOURCE},
         UNNEST(regexp_split_to_array(raw_payload->>'response', '[,;]')) AS t(item)
    WHERE raw_payload->>'response' IS NOT NULL
),
surface AS (
    SELECT naics_code, psc_code, equipment_item_raw,
           norm0(equipment_item_raw) AS form
    FROM split
    WHERE norm0(equipment_item_raw) <> ''
),
form_counts AS (
    SELECT form, fold_key(form) AS fk, count(*) AS n
    FROM surface
    GROUP BY 1, 2
),
canon AS (
    SELECT fk, form AS canonical
    FROM (
        SELECT fk, form,
               row_number() OVER (
                   PARTITION BY fk
                   ORDER BY n DESC, is_plural(form) DESC, length(form) DESC, form ASC
               ) AS rn
        FROM form_counts
    )
    WHERE rn = 1
)
SELECT DISTINCT
    s.naics_code,
    s.psc_code,
    c.canonical             AS equipment_item,
    s.equipment_item_raw,
    now()                   AS materialized_at
FROM surface s
JOIN form_counts f ON f.form = s.form
JOIN canon c       ON c.fk   = f.fk
ORDER BY s.naics_code, s.psc_code, equipment_item, s.equipment_item_raw
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


def _connect() -> duckdb.DuckDBPyConnection:
    dsn = os.environ["HQX_DB_URL_POOLED"]
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute("INSTALL json; LOAD json;")
    con.execute("PRAGMA threads=1;")  # single backend — the hqx SESSION pool is capped
    con.execute("SET TimeZone='UTC';")  # materialized_at stamps UTC, not the host locale
    con.execute(f"ATTACH '{dsn}' AS hqx (TYPE postgres, READ_ONLY)")
    con.execute(MACROS)
    return con


def build_raw(con, so: dict) -> None:
    src = con.execute(f"SELECT count(*) FROM {SOURCE}").fetchone()[0]
    tbl = con.execute(RAW_SQL).to_arrow_table()
    assert tbl.num_rows == src, f"verbatim mirror lost rows: {tbl.num_rows} != {src}"
    ds = write_indexed_dataset(tbl, RAW_URI, RAW_INDEXES, so)
    print(f"published {RAW_URI} rows={ds.count_rows():,} version={ds.version} "
          f"(source {src:,}, verbatim)")


def build_mentions(con, so: dict) -> None:
    con.execute(f"CREATE OR REPLACE TEMP TABLE mentions AS {MENTIONS_SQL}")
    tbl = con.execute("SELECT * FROM mentions").to_arrow_table()

    # Zero rows lost silently: every source combo either produced mentions or had an
    # empty/null response. The two sets must partition the source exactly.
    combos_src, combos_out, blank = con.execute(f"""
        SELECT (SELECT count(*) FROM {SOURCE}),
               (SELECT count(*) FROM (SELECT DISTINCT naics_code, psc_code FROM mentions)),
               (SELECT count(*) FROM {SOURCE}
                 WHERE raw_payload->>'response' IS NULL
                    OR norm0(raw_payload->>'response') = '')
    """).fetchone()
    assert combos_out + blank == combos_src, (
        f"combo accounting broken: {combos_out:,} with mentions + {blank} blank-response "
        f"!= {combos_src:,} source rows — {combos_src - combos_out - blank} lost silently")

    distinct_items = con.execute(
        "SELECT count(DISTINCT equipment_item) FROM mentions").fetchone()[0]
    ds = write_indexed_dataset(tbl, MENTIONS_URI, MENTIONS_INDEXES, so)
    print(f"published {MENTIONS_URI} rows={ds.count_rows():,} version={ds.version}")
    print(f"  combos with mentions {combos_out:,} + blank-response {blank} "
          f"= source {combos_src:,}")
    print(f"  distinct canonical equipment items: {distinct_items:,}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["raw", "mentions"], default=None)
    args = ap.parse_args()

    so = _r2_storage_options()
    con = _connect()
    try:
        if args.only in (None, "raw"):
            build_raw(con, so)
        if args.only in (None, "mentions"):
            build_mentions(con, so)
    finally:
        con.close()


if __name__ == "__main__":
    main()
