"""L1 dimension — naics_psc_labor_dim: the combo-grain labor answer, pre-joined into ONE row.

The broadcast dimension keyed on (naics_code, psc_code) that collapses the multi-row labor
resolution into exactly one row per combo — so the FPDS spine can join it 1:1 and inherit the
priced labor answer without a query-time fan-out. It fuses four live Lance datasets:

  base (spine)   naics_psc_labor_profile              the FULL combo universe — 14,112 rows, 1:1 on
                                                       (naics_code, psc_code). 11,751 service
                                                       ('labor_profile_v2') + 2,361 goods
                                                       ('goods_profile_v1'); 12,301 labor plays /
                                                       1,811 non-plays. Every combo survives (LEFT).
  categories     naics_psc_labor_profile_categories   45,333 rows, 1:N per combo (avg 3.2, max 10),
                                                       strictly ordered by `rank` (UNIQUE per combo).
                                                       COLLAPSED here to combo grain BEFORE the join:
                                                       the rank=1 row supplies rank1_* scalars, and
                                                       array_agg(... ORDER BY rank) supplies the
                                                       ranked SOC/SCA lists. The 1,811 non-play
                                                       combos carry a placeholder row (rank NULL,
                                                       soc_code NULL) — they collapse to NULL/empty.
  soc wage       soc_priced_skilled                   the clean, typed national wage dim (1 row per
                                                       6/7-char SOC). rank1_soc_code -> a_median
                                                       (DOUBLE) supplies rank1_soc_wage_median.
  spend taxonomy psctool/spend_category_psc_map       PSC -> spend category_level_1 (1:1 on 4-char
                                                       psc_code). The 'undefined' sentinel is mapped
                                                       to NULL. Join on psc_code.

GRAIN     1 row per (naics_code, psc_code). The profile is the spine (14,112). Categories is
          collapsed to combo grain FIRST (rank=1 CTE + array_agg CTE), so the profile's 1:1 grain
          is preserved — no fan-out. soc_priced_skilled and the spend map attach LEFT on scalar keys
          and can never add or drop a row.

SAME-ROW INVARIANT (load-bearing)
          rank1_soc_code, rank1_soc_title, rank1_sca_code, rank1_sca_title, rank1_role_class ALL
          come from the SAME rank=1 category row (one SELECT WHERE rank=1) — never two independent
          picks — so a role never pairs with the wrong SCA wage family. Live truth (probed
          2026-07-02): at rank=1 soc_code is non-null on exactly the 12,301 play combos and NULL on
          the 1,811 non-plays; sca_code is independently nullable (non-null on 6,179 of the play
          rank=1 rows). The gate therefore asserts rank1_soc_code non-null <=> is_labor_play, NOT a
          soc/sca co-nullity — a co-nullity assertion would be false against the data.

TABLE     s3://data-sink/active/naics_psc_labor_dim/
          BTREE  [naics_code, psc_code, rank1_soc_code]
          BITMAP [is_labor_play, psc_category, spend_category_l1]
          naics_code, psc_code, naics_title, psc_title, psc_category,
          is_labor_play, work_summary, n_categories, top_confidence, resolution_level,
          n_awards, total_dollars_obligated,                       -- DESCRIPTIVE (govcon-derived), labelled
          rank1_soc_code, rank1_soc_title, rank1_sca_code, rank1_sca_title, rank1_role_class,
          rank1_soc_wage_median (DOUBLE, from soc_priced_skilled.a_median via rank1_soc_code),
          top_soc_codes (list<string>, ranked, soc non-null),
          top_sca_codes (list<string>, ranked, sca non-null),
          spend_category_l1 (string, 'undefined' -> NULL),
          source, ingested_at.

SIZING CAVEAT
          n_awards / total_dollars_obligated are govcon_active_awards-derived DESCRIPTIVE stats
          carried verbatim from the profile — NOT spine-authoritative sizing. Real dollar sizing is
          the FPDS spine LEFT JOIN this dim at query time. They are labelled as descriptive here.

SOURCE    Derived L1 — reads only Lance SoR datasets under s3://data-sink/active/. No landing fetch,
          no external HTTP. Idempotent (mode='overwrite'): re-run after any upstream refresh. Reuses
          pipelines.bls.ingest R2 plumbing (_storage_options) and the shared Lance index builder
          (_build_indexes) — no re-implementation.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_naics_psc_labor_dim build

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_naics_psc_labor_dim verify
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

# Reuse the repo's R2 plumbing + Lance index builder by import — do NOT re-implement.
from pipelines.bls.ingest import _build_indexes, _storage_options

ACTIVE = "s3://data-sink/active"

# Source Lance datasets (SoR) — env-overridable for staging.
PROFILE_URI = os.environ.get(
    "NAICS_PSC_LABOR_PROFILE_URI", f"{ACTIVE}/naics_psc_labor_profile")
CATEGORIES_URI = os.environ.get(
    "NAICS_PSC_LABOR_PROFILE_CATEGORIES_URI", f"{ACTIVE}/naics_psc_labor_profile_categories")
SOC_PRICED_URI = os.environ.get(
    "SOC_PRICED_SKILLED_URI", f"{ACTIVE}/soc_priced_skilled")
SPEND_MAP_URI = os.environ.get(
    "SPEND_CATEGORY_PSC_MAP_URI", f"{ACTIVE}/psctool/spend_category_psc_map")

# Output dimension.
TARGET_URI = os.environ.get("NAICS_PSC_LABOR_DIM_URI", f"{ACTIVE}/naics_psc_labor_dim/")

DATA_STORAGE_VERSION = "2.1"       # net-new Lance dataset -> pin current default storage version
SOURCE = "naics_psc_labor_profile+categories+soc_priced_skilled+psctool_spend_map"

BTREE = ["naics_code", "psc_code", "rank1_soc_code"]
BITMAP = ["is_labor_play", "psc_category", "spend_category_l1"]

# Expected grain: exactly the profile combo universe (LEFT joins never add/drop a row).
EXPECTED_ROWS = 14112
ROW_BAND = (13500, 14500)
# is_labor_play True count = the play combos (rank1_soc non-null <=> this).
EXPECTED_PLAY = 12301
PLAY_BAND = (12000, 12600)

# 'undefined' spend sentinel -> NULL (live: 3 'undefined' rows in the 2,344-row map).
SPEND_UNDEFINED = "undefined"

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _build_sql() -> str:
    """Single DuckDB statement.

    (1) rank1  — one row per combo, the rank=1 category row: rank1_soc/sca/role_class ALL from
                 the SAME row (never two independent picks).
    (2) agg    — one row per combo: ranked, non-null SOC/SCA arrays (array_agg ORDER BY rank).
    (3) spend  — PSC spend taxonomy, 'undefined' -> NULL.
    Profile (base, 14,112) LEFT JOINs (1)+(2)+soc_priced_skilled(on rank1_soc_code)+(3) -> combo dim.
    """
    return f"""
    WITH rank1 AS (
        -- The SAME rank=1 row supplies every rank1_* scalar. One row per combo (rank UNIQUE per
        -- combo; non-play placeholder rows carry rank NULL and are excluded by rank = 1).
        SELECT
            naics_code,
            psc_code,
            soc_code   AS rank1_soc_code,
            soc_title  AS rank1_soc_title,
            sca_code   AS rank1_sca_code,
            sca_title  AS rank1_sca_title,
            role_class AS rank1_role_class
        FROM cats_src
        WHERE rank = 1
    ),
    agg AS (
        -- Ranked, non-null SOC/SCA lists per combo. array_agg FILTER drops the NULLs before the
        -- ORDER BY rank ordering; combos with no non-null member collapse to an empty list.
        SELECT
            naics_code,
            psc_code,
            array_agg(soc_code ORDER BY rank) FILTER (WHERE soc_code IS NOT NULL) AS top_soc_codes,
            array_agg(sca_code ORDER BY rank) FILTER (WHERE sca_code IS NOT NULL) AS top_sca_codes
        FROM cats_src
        GROUP BY naics_code, psc_code
    ),
    spend AS (
        SELECT
            psc_code,
            NULLIF(category_level_1, '{SPEND_UNDEFINED}') AS spend_category_l1
        FROM spend_src
    )
    SELECT
        p.naics_code,
        p.psc_code,
        p.naics_title,
        p.psc_title,
        p.psc_category,
        p.is_labor_play,
        p.work_summary,
        p.n_categories,
        p.top_confidence,
        p.resolution_level,
        -- descriptive (govcon-derived), NOT spine-authoritative sizing
        p.n_awards,
        p.total_dollars_obligated,
        -- rank1_* : ALL from the SAME rank=1 category row (r.*)
        r.rank1_soc_code,
        r.rank1_soc_title,
        r.rank1_sca_code,
        r.rank1_sca_title,
        r.rank1_role_class,
        s.a_median AS rank1_soc_wage_median,
        a.top_soc_codes,
        a.top_sca_codes,
        sp.spend_category_l1,
        '{SOURCE}' AS source,
        now() AS ingested_at
    FROM profile_src p
    LEFT JOIN rank1 r  ON r.naics_code = p.naics_code AND r.psc_code = p.psc_code
    LEFT JOIN agg   a  ON a.naics_code = p.naics_code AND a.psc_code = p.psc_code
    LEFT JOIN soc_src s ON s.soc_code = r.rank1_soc_code
    LEFT JOIN spend sp ON sp.psc_code = p.psc_code
    ORDER BY p.naics_code, p.psc_code
    """


def build() -> dict:
    import duckdb
    import lance

    so = _storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")

    # Register the Lance dataset OBJECTS (not one-shot .to_reader() streams — those drain on the
    # first scan). DuckDB scans the Lance dataset directly and pushes projection+filter down.
    con.register("profile_src", lance.dataset(PROFILE_URI, storage_options=so))
    con.register("cats_src", lance.dataset(CATEGORIES_URI, storage_options=so))
    con.register("soc_src", lance.dataset(SOC_PRICED_URI, storage_options=so))
    con.register("spend_src", lance.dataset(SPEND_MAP_URI, storage_options=so))
    log("registered profile + categories + soc_priced_skilled + spend_map Lance scanners")

    profile_rows = con.execute("SELECT count(*) FROM profile_src").fetchone()[0]

    # .to_arrow_table() returns a materialized pyarrow.Table (has .num_rows/.column()); the older
    # .arrow() returns a one-shot RecordBatchReader the fail-closed gate below cannot index.
    tbl = con.execute(_build_sql()).to_arrow_table()
    con.close()
    rows = tbl.num_rows
    log(f"assembled naics_psc_labor_dim: {rows:,} rows x {tbl.num_columns} cols (profile={profile_rows:,})")

    # -- FAIL-CLOSED gate — structural 1:1, no fan-out, invariants — before any publish ----------
    naics = tbl.column("naics_code").to_pylist()
    psc = tbl.column("psc_code").to_pylist()
    play = tbl.column("is_labor_play").to_pylist()
    r1soc = tbl.column("rank1_soc_code").to_pylist()

    n_null_key = sum(1 for a, b in zip(naics, psc) if a is None or b is None)
    n_distinct = len({(a, b) for a, b in zip(naics, psc)})
    n_play = sum(1 for v in play if v)
    # SAME-ROW INVARIANT: rank1_soc_code is non-null exactly on the labor-play combos.
    play_no_soc = sum(1 for pl, sc in zip(play, r1soc) if pl and sc is None)
    nonplay_with_soc = sum(1 for pl, sc in zip(play, r1soc) if (not pl) and sc is not None)

    if n_null_key:
        raise RuntimeError(f"GATE FAIL: {n_null_key} rows with NULL naics_code/psc_code (spine must be fully keyed)")
    if rows != n_distinct:
        raise RuntimeError(f"GATE FAIL: grain not 1:1 — rows={rows} != distinct(naics,psc)={n_distinct}")
    if rows != profile_rows:
        raise RuntimeError(f"GATE FAIL: fan-out/drop — rows={rows} != profile rows={profile_rows}")
    if not (ROW_BAND[0] <= rows <= ROW_BAND[1]):
        raise RuntimeError(f"GATE FAIL: row count {rows} outside band {ROW_BAND} (expected ~{EXPECTED_ROWS})")
    if not (PLAY_BAND[0] <= n_play <= PLAY_BAND[1]):
        raise RuntimeError(f"GATE FAIL: is_labor_play True count {n_play} outside band {PLAY_BAND} (expected ~{EXPECTED_PLAY})")
    if play_no_soc:
        raise RuntimeError(f"GATE FAIL: {play_no_soc} labor-play combos missing rank1_soc_code (same-row invariant)")
    if nonplay_with_soc:
        raise RuntimeError(f"GATE FAIL: {nonplay_with_soc} non-play combos carry a rank1_soc_code (same-row invariant)")
    log(f"GATE PASS: rows={rows:,} distinct={n_distinct:,} == profile={profile_rows:,} | "
        f"play={n_play:,} | rank1_soc<=>play (play_no_soc=0, nonplay_with_soc=0)")

    lance.write_dataset(tbl, TARGET_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    log(f"wrote Lance -> {TARGET_URI}")
    built = _build_indexes(TARGET_URI, BTREE, BITMAP, so)

    ds = lance.dataset(TARGET_URI, storage_options=so)
    written = ds.count_rows()
    if written != rows:
        raise RuntimeError(f"POST-WRITE FAIL: lance {written} != assembled {rows}")

    results = {
        "uri": TARGET_URI,
        "rows": written,
        "distinct_combo": n_distinct,
        "profile_rows": profile_rows,
        "is_labor_play_true": n_play,
        "rank1_soc_code_nonnull": sum(1 for v in r1soc if v is not None),
        "rank1_soc_wage_median_nonnull": ds.count_rows(filter="rank1_soc_wage_median IS NOT NULL"),
        "spend_category_l1_nonnull": ds.count_rows(filter="spend_category_l1 IS NOT NULL"),
        "columns": tbl.schema.names,
        "indexes": built,
        "source": SOURCE,
    }
    print(json.dumps(results, indent=2, default=str))
    return results


def verify() -> dict:
    import lance

    so = _storage_options()
    ds = lance.dataset(TARGET_URI, storage_options=so)
    rows = ds.count_rows()
    keys = ds.scanner(columns=["naics_code", "psc_code"]).to_table()
    combos = list(zip(keys.column("naics_code").to_pylist(), keys.column("psc_code").to_pylist()))
    distinct_combo = len(set(combos))
    idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
           for i in ds.list_indices()]
    idx_fields = set()
    for i in ds.list_indices():
        f = i.get("fields") if isinstance(i, dict) else getattr(i, "fields", None)
        if f:
            idx_fields.update(f)

    # Spot-check known combos — rank1_soc/sca must be from the SAME category row, spend mapped.
    # 111110/Z1AA and 112920/V112 are live labor plays; a non-play carries NULL rank1_*.
    spot = ds.scanner(
        columns=["naics_code", "psc_code", "is_labor_play", "rank1_soc_code", "rank1_sca_code",
                 "rank1_role_class", "rank1_soc_wage_median", "top_soc_codes", "top_sca_codes",
                 "spend_category_l1"],
        filter="(naics_code = '111110' AND psc_code = 'Z1AA') "
               "OR (naics_code = '112920' AND psc_code = 'V112')",
    ).to_table().to_pylist()

    nonplay = ds.scanner(
        columns=["naics_code", "psc_code", "is_labor_play", "rank1_soc_code", "rank1_sca_code",
                 "top_soc_codes"],
        filter="is_labor_play = false",
        limit=3,
    ).to_table().to_pylist()

    play_true = ds.count_rows(filter="is_labor_play = true")
    rank1_soc_nonnull = ds.count_rows(filter="rank1_soc_code IS NOT NULL")
    # SAME-ROW invariant, restated as two count filters.
    play_no_soc = ds.count_rows(filter="is_labor_play = true AND rank1_soc_code IS NULL")
    nonplay_with_soc = ds.count_rows(filter="is_labor_play = false AND rank1_soc_code IS NOT NULL")

    out = {
        "uri": TARGET_URI,
        "rows": rows,
        "distinct_combo": distinct_combo,
        "grain_1to1": rows == distinct_combo,
        "columns": ds.schema.names,
        "indices": idx,
        "index_fields": sorted(idx_fields),
        "is_labor_play_true": play_true,
        "rank1_soc_code_nonnull": rank1_soc_nonnull,
        "rank1_soc_wage_median_nonnull": ds.count_rows(filter="rank1_soc_wage_median IS NOT NULL"),
        "spend_category_l1_nonnull": ds.count_rows(filter="spend_category_l1 IS NOT NULL"),
        "play_no_soc": play_no_soc,
        "nonplay_with_soc": nonplay_with_soc,
        "spot_check": spot,
        "nonplay_sample": nonplay,
    }
    print(json.dumps(out, indent=2, default=str))

    # -- FAIL-LOUD verify — grain, index presence, invariants, spot-checks all non-zero-exit -----
    errors: list[str] = []
    if rows != distinct_combo:
        errors.append(f"grain broken: rows={rows} != distinct_combo={distinct_combo}")
    if not (ROW_BAND[0] <= rows <= ROW_BAND[1]):
        errors.append(f"row band: {rows} outside {ROW_BAND}")
    for need in ("naics_code", "psc_code", "rank1_soc_code"):
        if need not in idx_fields:
            errors.append(f"BTREE field {need} absent from index fields {sorted(idx_fields)}")
    if play_no_soc:
        errors.append(f"same-row invariant: {play_no_soc} play combos with NULL rank1_soc_code")
    if nonplay_with_soc:
        errors.append(f"same-row invariant: {nonplay_with_soc} non-play combos with rank1_soc_code")
    if not (PLAY_BAND[0] <= play_true <= PLAY_BAND[1]):
        errors.append(f"is_labor_play True {play_true} outside {PLAY_BAND}")
    # spot combos must be plays with a non-null rank1 soc AND sca from the same row
    for r in spot:
        if not r["is_labor_play"] or r["rank1_soc_code"] is None or r["rank1_sca_code"] is None:
            errors.append(f"spot combo {r['naics_code']}/{r['psc_code']} lost its same-row rank1 soc+sca: {r}")
    # non-play sample must have NULL rank1_soc_code
    for r in nonplay:
        if r["rank1_soc_code"] is not None:
            errors.append(f"non-play {r['naics_code']}/{r['psc_code']} has non-null rank1_soc_code: {r}")
    if errors:
        raise RuntimeError("VERIFY FAIL: " + " | ".join(errors))
    return out


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        build()
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
