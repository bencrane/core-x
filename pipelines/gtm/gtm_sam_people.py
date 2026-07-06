"""gtm_sam_people — GTM SAM audience mart person layer (two datasets, one build).

Strategy: docs/plans/GTM_SAM_AUDIENCE_MART_STRATEGY.md (v2, aligned).

  gtm_sam_people_evidence   1 row / (uei, source_dataset, source_role, slot, name_key)
                            — verbatim mention custody with provenance. Sources:
                            sam_pocs (v2/UEI rows incl. past_performance slots),
                            dsbs_pocs, ffata_exec_comp, subaward officers 1-5
                            unpivoted (deduped latest per (uei, name_key)).
                            sam_labor_poc_people EXCLUDED (decision log #4).

  gtm_sam_people            1 row / (uei, name_key) — collapsed person@entity.
                            PK sam_person_id = sha256(uei || '|' || name_key).
                            Role flags OR'd; display name / best title selected
                            by source-priority (dsbs_principal > dsbs_contact >
                            sam_poc > officer), longest verbatim wins within a tier.
                            Honorific-only titles (Mr./Mrs./Dr. …) never qualify
                            for best_title — a person with only those gets NULL.
                            No cross-entity fusion; no denormalized entity attrs.

name_key convention (the mart's single person-matching key — replicates
materialize_dsbs_pocs._name_key in SQL): strip accents → lower → split on
non-alpha → drop tokens len<2 and suffix/credential tokens → sort → join.
Order-independent ("SMITH, JOHN" == "John Smith"). Verbatim strings are never
altered — name_key is a derived accelerator only.

Referential integrity gate: every people uei must exist in gtm_sam_entities
(build that spine FIRST — its universe includes every people source's UEI set).

Run:
    doppler run -- python3 pipelines/gtm/gtm_sam_people.py               # local build
    modal run pipelines/gtm/gtm_sam_people.py                            # remote build
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid

BUCKET = "data-sink"
_PROD_EVIDENCE_URI = "s3://data-sink/active/gtm_sam_people_evidence/"
_PROD_PEOPLE_URI = "s3://data-sink/active/gtm_sam_people/"
EVIDENCE_URI = os.environ.get("GTM_SAM_EVIDENCE_LANCE_URI", _PROD_EVIDENCE_URI)
PEOPLE_URI = os.environ.get("GTM_SAM_PEOPLE_LANCE_URI", _PROD_PEOPLE_URI)

SRC = {
    "sam_pocs": "s3://data-sink/active/sam_pocs/",
    "dsbs_pocs": "s3://data-sink/active/dsbs_pocs/",
    "ffata_exec_comp": "s3://data-sink/active/ffata_exec_comp/",
    "usaspending_subaward_canonical": "s3://data-sink/active/usaspending_subaward_canonical/",
    "gtm_sam_entities": "s3://data-sink/active/gtm_sam_entities/",
}

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

EVIDENCE_BTREE = ["uei", "name_key"]
EVIDENCE_BITMAP = ["source_dataset", "source_role"]
PEOPLE_BTREE = ["sam_person_id", "uei", "name_key"]
PEOPLE_BITMAP = ["is_govt_poc", "is_ebiz_poc", "is_past_perf_poc", "is_dsbs_contact",
                 "is_dsbs_principal", "is_exec_officer_prime", "is_exec_officer_sub"]

DUCKDB_MEMORY_LIMIT = os.environ.get("GTM_DUCKDB_MEM", "16GB")
DUCKDB_THREADS = 8
SPILL_DIR = os.environ.get("GTM_DUCKDB_SPILL", "/tmp/duckdb_spill")

# ── gate constants (floors from live probe 2026-07-04: sam_pocs v2 rows 4.373M ·
#    dsbs_pocs 102,742 · ffata 29,616 · subaward officer_1 fill 120,561) ──
EVIDENCE_ROW_FLOOR = 4_300_000
PEOPLE_ROW_FLOOR = 1_800_000
NAME_KEY_FILL_MIN = 0.985        # evidence rows with a usable name_key
DELTA_GUARD = 0.25
BASELINE_MIN_ROWS = 1_800_000

# Suffix/credential tokens dropped from name_key — verbatim from
# pipelines/sba_dsbs/materialize_dsbs_pocs.py::_SUFFIX.
_SUFFIX_SQL = "['jr','sr','ii','iii','iv','v','md','phd','cpa','esq','pe','mba','jd','dds']"


def name_key_sql(col: str) -> str:
    """DuckDB replication of materialize_dsbs_pocs._name_key (NFKD≈strip_accents)."""
    return (
        "NULLIF(array_to_string(list_sort(list_filter("
        f"string_split_regex(lower(strip_accents(coalesce({col}, ''))), '[^a-z]+'),"
        f" t -> len(t) >= 2 AND NOT list_contains({_SUFFIX_SQL}, t))), ' '), '')"
    )


# Honorific-only "titles" ride in from DSBS principal segments ("JANE SMITH - Mrs.") and can
# appear in SAM POC title slots. An honorific is not a role: such values never win best_title.
# Set mirrors materialize_dsbs_pocs / resolve_dsbs_poc_linkedin _HONOR.
_HONORIFIC_RX = ("(dr|mr|mrs|ms|miss|mx|prof|professor|sir|madam|rev|hon|capt|col|sgt|lt|maj"
                 "|gen|cmdr|messrs|mstr)")


def usable_title_sql(col: str) -> str:
    """TRUE when col is a non-empty title that is not honorific-only (evidence keeps verbatim;
    this gates selection only)."""
    return (f"({col} IS NOT NULL AND trim({col}) <> '' AND NOT regexp_full_match("
            f"lower(trim({col})), '({_HONORIFIC_RX}\\.?[\\s.,&/-]*)+'))")


OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.gtm_sam_people_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,
    evidence_uri        text        NOT NULL,
    people_uri          text        NOT NULL,
    build_id            text,
    evidence_rows       bigint,
    people_rows         bigint,
    distinct_uei        bigint,
    rows_sam_pocs       bigint,
    rows_dsbs_pocs      bigint,
    rows_ffata          bigint,
    rows_sub_officers   bigint,
    orphan_uei_rows     bigint,
    name_key_fill_frac  double precision,
    inputs              jsonb,
    status              text        NOT NULL,
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS gtm_sam_people_runs_feed_idx
    ON ops.gtm_sam_people_runs (feed);
CREATE INDEX IF NOT EXISTS gtm_sam_people_runs_recorded_at_idx
    ON ops.gtm_sam_people_runs (recorded_at DESC);
"""


def _feed_for(uri: str) -> str:
    return "gtm_sam_people" if uri == _PROD_PEOPLE_URI else "gtm_sam_people_scratch"


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


def _alert(msg: str) -> None:
    try:
        from core.ops_alert import alert
        alert(msg)
    except Exception:  # noqa: BLE001
        print(f"ALERT (no channel): {msg}")


def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _open(so: dict, lineage: list[dict], name: str):
    import lance

    ds = lance.dataset(SRC[name], storage_options=so)
    lineage.append({"name": name, "uri": SRC[name], "version": ds.version,
                    "rows_at_read": ds.count_rows()})
    return ds


# --------------------------------------------------------------------------- #
# materialize
# --------------------------------------------------------------------------- #
def _materialize(con, so: dict, build_id: str, built_at: dt.datetime):
    lineage: list[dict] = []
    nk = name_key_sql("full_name_verbatim")

    sp = _open(so, lineage, "sam_pocs")
    con.register("sp", sp.scanner(
        columns=["uei", "poc_type", "poc_slot_no", "full_name", "first_name",
                 "last_name", "title", "sam_extract_label"],
        filter="uei IS NOT NULL").to_reader())
    con.execute("""CREATE TEMP TABLE src_sam AS
        SELECT uei, 'sam_pocs' AS source_dataset, poc_type AS source_role,
               poc_slot_no::INT AS slot_no, full_name AS full_name_verbatim,
               first_name, last_name, title, NULL::DOUBLE AS officer_amount,
               sam_extract_label AS source_as_of
        FROM sp WHERE full_name IS NOT NULL""")
    con.unregister("sp")

    dp = _open(so, lineage, "dsbs_pocs")
    con.register("dp", dp.scanner(
        columns=["uei", "poc_type", "slot_no", "full_name", "first_name",
                 "last_name", "title", "materialized_at"]).to_reader())
    con.execute("""CREATE TEMP TABLE src_dsbs AS
        SELECT uei, 'dsbs_pocs' AS source_dataset,
               CASE poc_type WHEN 'contact_person' THEN 'dsbs_contact'
                             ELSE 'dsbs_principal' END AS source_role,
               slot_no::INT AS slot_no, full_name AS full_name_verbatim,
               first_name, last_name, title, NULL::DOUBLE AS officer_amount,
               strftime(materialized_at, '%Y-%m-%d') AS source_as_of
        FROM dp WHERE uei IS NOT NULL AND full_name IS NOT NULL""")
    con.unregister("dp")

    fx = _open(so, lineage, "ffata_exec_comp")
    con.register("fx", fx.scanner(
        columns=["recipient_uei", "officer_rank", "officer_name",
                 "officer_amount", "disclosure_action_date"]).to_reader())
    con.execute("""CREATE TEMP TABLE src_ffata AS
        SELECT recipient_uei AS uei, 'ffata_exec_comp' AS source_dataset,
               'exec_comp_prime' AS source_role, officer_rank::INT AS slot_no,
               officer_name AS full_name_verbatim,
               NULL::VARCHAR AS first_name, NULL::VARCHAR AS last_name,
               NULL::VARCHAR AS title, officer_amount,
               strftime(disclosure_action_date, '%Y-%m-%d') AS source_as_of
        FROM fx WHERE recipient_uei IS NOT NULL AND officer_name IS NOT NULL""")
    con.unregister("fx")

    sub_cols = ["subawardee_uei", "subaward_action_date"]
    for i in range(1, 6):
        sub_cols += [f"subawardee_highly_compensated_officer_{i}_name",
                     f"subawardee_highly_compensated_officer_{i}_amount"]
    sb = _open(so, lineage, "usaspending_subaward_canonical")
    con.register("sb", sb.scanner(columns=sub_cols,
                                  filter="subawardee_uei IS NOT NULL").to_reader())
    unpivot = " UNION ALL ".join(
        f"""SELECT subawardee_uei AS uei, {i} AS slot_no,
                   subawardee_highly_compensated_officer_{i}_name AS nm,
                   subawardee_highly_compensated_officer_{i}_amount AS amt,
                   subaward_action_date
            FROM sb WHERE subawardee_highly_compensated_officer_{i}_name IS NOT NULL"""
        for i in range(1, 6))
    con.execute(f"""CREATE TEMP TABLE src_sub AS
        WITH unp AS ({unpivot}),
        ranked AS (
            SELECT *, {name_key_sql('nm')} AS _nk,
                   row_number() OVER (
                       PARTITION BY uei, {name_key_sql('nm')}
                       ORDER BY subaward_action_date DESC NULLS LAST,
                                amt DESC NULLS LAST, slot_no, nm
                   ) AS rn
            FROM unp)
        SELECT uei, 'usaspending_subaward_canonical' AS source_dataset,
               'exec_comp_sub' AS source_role, slot_no::INT AS slot_no,
               nm AS full_name_verbatim,
               NULL::VARCHAR AS first_name, NULL::VARCHAR AS last_name,
               NULL::VARCHAR AS title, amt AS officer_amount,
               strftime(subaward_action_date, '%Y-%m-%d') AS source_as_of
        FROM ranked WHERE rn = 1""")
    con.unregister("sb")

    con.execute(f"""
    CREATE TEMP TABLE evidence AS
    WITH unioned AS (
        SELECT * FROM src_sam
        UNION ALL SELECT * FROM src_dsbs
        UNION ALL SELECT * FROM src_ffata
        UNION ALL SELECT * FROM src_sub
    ),
    keyed AS (
        SELECT *, {nk} AS name_key FROM unioned
    )
    SELECT sha256(concat_ws('|', uei, source_dataset, source_role,
                            slot_no::VARCHAR, coalesce(name_key, ''))) AS evidence_uid,
           uei, source_dataset, source_role, slot_no, full_name_verbatim,
           first_name, last_name, title, name_key, officer_amount, source_as_of,
           '{build_id}' AS build_id,
           TIMESTAMP '{built_at:%Y-%m-%d %H:%M:%S}' AS built_at
    FROM keyed
    """)
    # Grain enforcement: the uid quintuple must be unique (duplicate mentions at
    # identical grain collapse to one row, keeping the lexically-first verbatim).
    con.execute("""
        CREATE TEMP TABLE evidence_d AS
        SELECT * FROM evidence
        QUALIFY row_number() OVER (
            PARTITION BY evidence_uid ORDER BY full_name_verbatim) = 1""")

    con.execute(f"""
    CREATE TEMP TABLE people AS
    WITH scored AS (
        SELECT *,
            (CASE source_role
                 WHEN 'dsbs_principal' THEN 5 WHEN 'dsbs_contact' THEN 4
                 WHEN 'exec_comp_prime' THEN 1 WHEN 'exec_comp_sub' THEN 1
                 ELSE 3 END) * 1000
            + least(len(coalesce(full_name_verbatim, '')), 999) AS pref
        FROM evidence_d WHERE name_key IS NOT NULL
    )
    , collapsed AS (
    SELECT
        sha256(uei || '|' || name_key)             AS sam_person_id,
        uei, name_key,
        max_by(full_name_verbatim, pref)           AS display_name,
        max_by(first_name, CASE WHEN first_name IS NOT NULL THEN pref ELSE -1 END)
                                                   AS src_first_name,
        max_by(last_name,  CASE WHEN last_name  IS NOT NULL THEN pref ELSE -1 END)
                                                   AS src_last_name,
        max_by(title, CASE WHEN {usable_title_sql('title')} THEN pref END)
                                                   AS best_title,
        bool_or(source_role IN ('government_business','government_business_alt'))
                                                   AS is_govt_poc,
        bool_or(source_role IN ('electronic_business','electronic_business_alt'))
                                                   AS is_ebiz_poc,
        bool_or(source_role IN ('past_performance','past_performance_alt'))
                                                   AS is_past_perf_poc,
        bool_or(source_role = 'dsbs_contact')      AS is_dsbs_contact,
        bool_or(source_role = 'dsbs_principal')    AS is_dsbs_principal,
        bool_or(source_role = 'exec_comp_prime')   AS is_exec_officer_prime,
        bool_or(source_role = 'exec_comp_sub')     AS is_exec_officer_sub,
        count(DISTINCT source_dataset)             AS n_sources,
        count(*)                                   AS n_mentions,
        max(officer_amount)                        AS max_officer_amount,
        min(source_as_of)                          AS first_seen_label,
        max(source_as_of)                          AS last_seen_label,
        '{build_id}'                               AS build_id,
        TIMESTAMP '{built_at:%Y-%m-%d %H:%M:%S}'   AS built_at
    FROM scored
    GROUP BY uei, name_key
    ),
    -- Fallback name split: exec-comp sources (ffata, subaward officers) carry only a full-name
    -- string, so people seen ONLY there collapse with NULL parsed names. Derive first/last from
    -- display_name (first/last token; junk-guarded) and FLAG it — token order in exec-comp
    -- verbatims is mixed (FIRST LAST vs LAST FIRST M), so consumers gate on name_parse_source.
    splitter AS (
    SELECT *,
        regexp_replace(trim(display_name), '\\s+', ' ', 'g') AS _dn,
        (src_first_name IS NULL OR src_last_name IS NULL)
        AND display_name IS NOT NULL
        AND lower(trim(display_name)) NOT IN ('null null', 'null')
        AND regexp_matches(regexp_replace(trim(display_name), '\\s+', ' ', 'g'), ' ')
                                                             AS _dn_split_ok
    FROM collapsed
    )
    SELECT * EXCLUDE (_dn, _dn_split_ok, src_first_name, src_last_name),
        coalesce(src_first_name,
                 CASE WHEN _dn_split_ok THEN regexp_extract(_dn, '^(\\S+)', 1) END)
                                                   AS first_name,
        coalesce(src_last_name,
                 CASE WHEN _dn_split_ok THEN regexp_extract(_dn, '(\\S+)$', 1) END)
                                                   AS last_name,
        CASE WHEN src_first_name IS NOT NULL AND src_last_name IS NOT NULL THEN 'source'
             WHEN _dn_split_ok THEN 'display_split'
        END                                        AS name_parse_source
    FROM splitter
    """)

    # Referential integrity vs the entity spine (uei set only — cheap column pull).
    ent = _open(so, lineage, "gtm_sam_entities")
    con.register("ent", ent.scanner(columns=["uei"]).to_reader())
    orphans = con.execute("""
        SELECT count(*) FROM people p WHERE NOT EXISTS
            (SELECT 1 FROM ent e WHERE e.uei = p.uei)""").fetchone()[0]
    con.unregister("ent")

    (ev_rows, ev_uid, nk_fill, r_sam, r_dsbs, r_ffata, r_sub) = con.execute("""
        SELECT count(*), count(DISTINCT evidence_uid), count(name_key),
               count(*) FILTER (WHERE source_dataset = 'sam_pocs'),
               count(*) FILTER (WHERE source_dataset = 'dsbs_pocs'),
               count(*) FILTER (WHERE source_dataset = 'ffata_exec_comp'),
               count(*) FILTER (WHERE source_dataset = 'usaspending_subaward_canonical')
        FROM evidence_d""").fetchone()
    (pe_rows, pe_ids, d_uei) = con.execute("""
        SELECT count(*), count(DISTINCT sam_person_id), count(DISTINCT uei)
        FROM people""").fetchone()
    probe = con.execute("""
        SELECT sam_person_id, uei, name_key FROM people
        WHERE is_past_perf_poc ORDER BY uei LIMIT 1""").fetchone()

    ev_table = con.sql("SELECT * FROM evidence_d").to_arrow_table()
    pe_table = con.sql("SELECT * FROM people").to_arrow_table()
    metrics = {
        "evidence_rows": int(ev_rows), "evidence_uid_distinct": int(ev_uid),
        "name_key_fill_frac": (nk_fill / ev_rows) if ev_rows else 0.0,
        "rows_sam_pocs": int(r_sam), "rows_dsbs_pocs": int(r_dsbs),
        "rows_ffata": int(r_ffata), "rows_sub_officers": int(r_sub),
        "people_rows": int(pe_rows), "people_id_distinct": int(pe_ids),
        "distinct_uei": int(d_uei), "orphan_uei_rows": int(orphans),
        "probe": probe,
    }
    return ev_table, pe_table, metrics, lineage


# --------------------------------------------------------------------------- #
# gates
# --------------------------------------------------------------------------- #
def _within(value: int, target: int, tol: float) -> bool:
    return abs(value - target) <= target * tol


def assert_pre_write_gates(metrics: dict, baseline: dict | None) -> list[str]:
    checks: list[str] = []

    def gate(ok: bool, label: str) -> None:
        checks.append(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            raise RuntimeError(f"PRE-WRITE GATE FAILED → {label}\n" + "\n".join(checks))

    gate(metrics["evidence_rows"] >= EVIDENCE_ROW_FLOOR,
         f"1 evidence floor: {metrics['evidence_rows']:,} >= {EVIDENCE_ROW_FLOOR:,}")
    gate(metrics["evidence_uid_distinct"] == metrics["evidence_rows"],
         f"2 evidence uid unique: {metrics['evidence_uid_distinct']:,}")
    gate(metrics["name_key_fill_frac"] >= NAME_KEY_FILL_MIN,
         f"3 name_key fill: {metrics['name_key_fill_frac']:.4%} >= {NAME_KEY_FILL_MIN:.1%}")
    gate(all(metrics[k] > 0 for k in
             ("rows_sam_pocs", "rows_dsbs_pocs", "rows_ffata", "rows_sub_officers")),
         "4 all four sources present")
    gate(metrics["people_rows"] >= PEOPLE_ROW_FLOOR,
         f"5 people floor: {metrics['people_rows']:,} >= {PEOPLE_ROW_FLOOR:,}")
    gate(metrics["people_id_distinct"] == metrics["people_rows"],
         f"6 people grain 1/(uei,name_key): {metrics['people_id_distinct']:,}")
    gate(metrics["orphan_uei_rows"] == 0,
         f"7 referential integrity: orphan people ueis = {metrics['orphan_uei_rows']}")
    if baseline:
        gate(_within(metrics["people_rows"], baseline["people_rows"], DELTA_GUARD),
             f"8 people Δ: {metrics['people_rows']:,} ~ ±{DELTA_GUARD:.0%} "
             f"of {baseline['people_rows']:,}")
    else:
        checks.append("SKIP  8 Δ-guard: no floor-qualified prior success")
    return checks


# --------------------------------------------------------------------------- #
# ops ledger
# --------------------------------------------------------------------------- #
def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _prior_success_baseline(people_uri: str) -> dict | None:
    conn = _pg_connect()
    if conn is None:
        return None
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "SELECT people_rows FROM ops.gtm_sam_people_runs "
                "WHERE status='success' AND people_uri=%s AND people_rows >= %s "
                "ORDER BY recorded_at DESC LIMIT 1",
                (people_uri, BASELINE_MIN_ROWS))
            r = cur.fetchone()
            return None if not r else {"people_rows": int(r[0])}
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: baseline lookup failed: {exc}")
        return None
    finally:
        conn.close()


def _record_run(*, feed, evidence_uri, people_uri, build_id, metrics, lineage,
                status, error, started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.gtm_sam_people_runs
                    (feed, evidence_uri, people_uri, build_id, evidence_rows,
                     people_rows, distinct_uei, rows_sam_pocs, rows_dsbs_pocs,
                     rows_ffata, rows_sub_officers, orphan_uei_rows,
                     name_key_fill_frac, inputs, status, error,
                     started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (feed, evidence_uri, people_uri, build_id,
                 metrics.get("evidence_rows"), metrics.get("people_rows"),
                 metrics.get("distinct_uei"), metrics.get("rows_sam_pocs"),
                 metrics.get("rows_dsbs_pocs"), metrics.get("rows_ffata"),
                 metrics.get("rows_sub_officers"), metrics.get("orphan_uei_rows"),
                 metrics.get("name_key_fill_frac"), json.dumps(lineage),
                 status, error, started_at, completed_at))
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* write failed: {exc}")
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# write helper — one dataset under a rollback guard
# --------------------------------------------------------------------------- #
def _as_batched_reader(table, max_chunksize: int = 131072):
    """Bound arrow chunk sizes fed to the Lance encoder (see gtm_sam_entities)."""
    import pyarrow as pa

    return pa.RecordBatchReader.from_batches(
        table.schema, table.to_batches(max_chunksize=max_chunksize))


def _write_indexed(so, uri, table, btree, bitmap, expected_rows):
    import lance

    try:
        v_before = lance.dataset(uri, storage_options=so).version
    except Exception:  # noqa: BLE001
        v_before = None
    print(f"{uri} v_before = {v_before}")
    try:
        lance.write_dataset(_as_batched_reader(table), uri, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE,
                            max_bytes_per_file=MAX_BYTES_PER_FILE,
                            storage_options=so)
        ds = lance.dataset(uri, storage_options=so)
        for col in btree:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"  BTREE ✓ {col}")
        for col in bitmap:
            ds.create_scalar_index(col, index_type="BITMAP")
            print(f"  BITMAP ✓ {col}")
        ds = lance.dataset(uri, storage_options=so)
        committed = ds.count_rows()
        if committed != expected_rows:
            raise RuntimeError(f"write-integrity: {committed:,} != {expected_rows:,}")
        idx = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
               for i in ds.list_indices()}
        expect = {f"{c}_idx" for c in btree + bitmap}
        if not expect.issubset(idx):
            raise RuntimeError(f"indices missing: {sorted(expect - idx)}")
        print(f"  committed={committed:,} indices ok")
    except Exception as werr:  # noqa: BLE001
        if v_before is not None:
            try:
                lance.dataset(uri, storage_options=so, version=v_before).restore()
            except Exception as rerr:  # noqa: BLE001
                raise RuntimeError(f"ROLLBACK FAILED {uri} to v{v_before}: {rerr}; "
                                   f"original: {werr}")
            raise RuntimeError(f"{uri} failed → rolled back to v{v_before}: {werr}")
        raise RuntimeError(f"{uri} failed on net-new dataset (inspect/drop): {werr}")


# --------------------------------------------------------------------------- #
# core build
# --------------------------------------------------------------------------- #
def run_build(evidence_uri: str | None = None, people_uri: str | None = None) -> dict:
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    ev_uri = evidence_uri or EVIDENCE_URI
    pe_uri = people_uri or PEOPLE_URI
    feed = _feed_for(pe_uri)
    build_id = f"{started_at:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    status, error = "error", None
    metrics: dict = {}
    lineage: list[dict] = []
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            ev_table, pe_table, metrics, lineage = _materialize(
                con, so, build_id, started_at)
        finally:
            con.close()
        baseline = _prior_success_baseline(pe_uri)
        print(f"targets: evidence={ev_uri} people={pe_uri} build_id={build_id}")
        print(f"materialized: { {k: v for k, v in metrics.items() if k != 'probe'} }")
        for e in lineage:
            print(f"  input {e['name']} v{e['version']} rows={e['rows_at_read']:,}")

        for line in assert_pre_write_gates(metrics, baseline):
            print("  ", line)

        # Evidence first, then people — people references evidence lineage-wise;
        # each write is independently rollback-guarded.
        _write_indexed(so, ev_uri, ev_table, EVIDENCE_BTREE, EVIDENCE_BITMAP,
                       metrics["evidence_rows"])
        _write_indexed(so, pe_uri, pe_table, PEOPLE_BTREE, PEOPLE_BITMAP,
                       metrics["people_rows"])

        # Post-write round-trip: the past-performance probe person resolves by PK.
        pid, puei, pnk = metrics["probe"]
        ds = lance.dataset(pe_uri, storage_options=so)
        rt = ds.scanner(columns=["sam_person_id", "uei", "name_key"],
                        filter=f"sam_person_id = '{pid}'").to_table().num_rows
        if rt != 1:
            raise RuntimeError(f"round-trip: probe {pid} → {rt} rows")
        print(f"post-write round-trip PASS — probe person {pnk} @ {puei}")

        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(feed=feed, evidence_uri=ev_uri, people_uri=pe_uri,
                    build_id=build_id, metrics=metrics, lineage=lineage,
                    status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        if status != "success":
            _alert(f"[gtm_sam_people] {feed} build {status}: {str(error)[:300]}")

    if status != "success":
        raise RuntimeError(f"gtm_sam_people build failed: {error}")
    return {"feed": feed, "evidence": ev_uri, "people": pe_uri,
            "build_id": build_id, **{k: metrics[k] for k in (
                "evidence_rows", "people_rows", "distinct_uei", "rows_sam_pocs",
                "rows_dsbs_pocs", "rows_ffata", "rows_sub_officers")}}


# --------------------------------------------------------------------------- #
# Modal wrapper (optional)
# --------------------------------------------------------------------------- #
try:
    import modal

    image = modal.Image.debian_slim(python_version="3.12").pip_install(
        "duckdb>=1.5,<2", "pylance>=8", "pyarrow>=17",
        "psycopg[binary]>=3.2",
    ).env({"LANCE_BYPASS_SPILLING": "true"})
    app = modal.App("gtm-sam-people", image=image)

    @app.function(
        secrets=[modal.Secret.from_name("r2-credentials"),
                 modal.Secret.from_name("hqx-postgres")],
        timeout=60 * 60, memory=32768, cpu=8.0)
    def build_gtm_sam_people(evidence_uri: str | None = None,
                             people_uri: str | None = None) -> dict:
        return run_build(evidence_uri, people_uri)

    @app.local_entrypoint()
    def main():
        print(build_gtm_sam_people.remote())
except ImportError:
    modal = None


if __name__ == "__main__":
    print(run_build())
