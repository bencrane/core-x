"""Serving worker — govcon_sub_certifications_mv: the hybrid DSBS×SAM certification MV.

GRAIN   1 row per UEI over the UNION universe D ∪ P ∪ T (DSBS certified ∪ subawardee profiles ∪
        targeting candidates). ~90,210 firms: ~64,760 greenfield DSBS-only, ~2,474 dsbs∩sub,
        ~22,976 sub-only.
SoR     s3://data-sink/active/govcon_sub_certifications_mv/ (Lance v2.1; derived, SNAPSHOT-OVERWRITE).
        Frozen schema + assert_schema (govcon_gtm_schemas.py) before/after the commit.
LEDGER  ops.sub_certifications_mv_serving_runs (HQX_DB_URL_POOLED) on every terminal state.

HYBRID CERTIFICATION RULES ENGINE (two strictly disjoint namespaces — docs/plans/
DSBS_SAM_HYBRID_CERTIFICATION_MV_PLAN.md):
  * cert_*      ADJUDICATED — sourced ONLY from the DSBS `certs` JSON array, gated on
                 status='Active' AND (exitDate IS NULL OR exitDate >= today). A firm absent from
                 DSBS has every cert_* false: SAM claims NEVER populate cert_* (structural override).
                 Uses CAST(certs AS JSON[]) (NOT from_json(...,'JSON[]'), which DuckDB rejects).
  * sam_self_*  SELF-CERTIFIED — SAM `business_types` decoded via sam_business_type_code_dict WHERE
                 sba_administered=false AND designation has no DSBS-adjudicated equivalent → exactly
                 {minority_owned(23), self_cert_sdb(27), woman_owned(A2), veteran_owned(A5)}.
                 8W/8C/QF excluded (DSBS owns wosb/sdvosb). Namespaces share zero designations.
  * sam_registration_active is sourced from a self-cert-INDEPENDENT join on sam_master_entities
    (an adjudicated-only firm with no retained self-cert code must not be flagged SAM-inactive).

DECOUPLED PAYLOAD: email/phone/contact_person/website native from DSBS (no firmographics_blitz);
subawardee POC fallback for the sub-only tier.

    doppler run -p core-x -c prd -- uv run --no-project \\
      --with pylance --with pyarrow --with 'duckdb>=1.5,<2' --with boto3 --with 'psycopg[binary]' \\
      python3 pipelines/serving/materialize_sub_certifications.py <dry_run|build|verify>
    modal run pipelines/serving/materialize_sub_certifications.py::main --cmd build
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines.sam_gov.govcon_gtm_schemas import (  # noqa: E402
    SUB_CERTIFICATIONS_MV_URI, sub_certifications_mv_schema, assert_schema)
from core.web_norm import _bare_host, normalized_domain, is_generic_domain  # noqa: E402

try:
    import modal  # noqa: F401
    _HAS_MODAL = True
except Exception:  # noqa: BLE001
    modal = None
    _HAS_MODAL = False

ACTIVE = "s3://data-sink/active"
DSBS_URI = os.environ.get("SBA_DSBS_FIRMS_URI", f"{ACTIVE}/sba_dsbs_certified_firms/")
PROFILES_URI = os.environ.get("GOVCON_SUB_PROFILES_URI", f"{ACTIVE}/govcon_subawardee_profiles/")
TARGETING_URI = os.environ.get("GOVCON_SUB_TARGETING_URI", f"{ACTIVE}/govcon_sub_targeting/")
SAM_ENT_URI = os.environ.get("SAM_MASTER_ENTITIES_URI", f"{ACTIVE}/sam_master_entities/")
DICT_URI = os.environ.get("SAM_BTC_DICT_URI", f"{ACTIVE}/sam_business_type_code_dict/")

FEED = "sub_certifications_mv"
DATA_STORAGE_VERSION = "2.1"

# zero-join frontend filter surface (plan §2.3, F5/F6 trims applied)
BTREE_INDEXES = ["uei", "naics_primary", "zipcode", "next_cert_expiration_date", "normalized_domain"]
BITMAP_INDEXES = [
    "state", "universe_tier", "cert_lifecycle", "contact_source", "geo_source",
    "in_dsbs", "is_subawardee", "is_targeting_candidate", "is_greenfield",
    "cert_any", "cert_8a", "cert_hubzone", "cert_wosb", "cert_edwosb", "cert_vosb",
    "cert_sdvosb", "cert_8a_jv",
    "sam_self_any", "sam_self_minority_owned", "sam_self_sdb", "sam_self_woman_owned",
    "sam_self_veteran_owned", "sam_registration_active", "sam_present", "has_subaward_history",
    "domain_is_generic",
]

# SAM self-cert namespace = sba_administered=false MINUS the 3 designations a DSBS program adjudicates.
ADJUDICATED_DESIGNATION_KEYS = (
    "women_owned_small_business",                 # 8W → cert_wosb
    "joint_venture_women_owned_small_business",   # 8C → cert_wosb (JV)
    "service_disabled_veteran_owned_business",    # QF → cert_sdvosb
)
# adjudicated/SBA-administered codes that must NEVER appear in sam_self_codes (verify gate)
ADJUDICATED_CODES = ("8W", "8C", "QF", "A9", "A6", "XX", "A0", "JT")

DUCK_MEM = os.environ.get("SUB_CERT_DUCKDB_MEMORY_LIMIT", "10GB")
DUCK_TMP = os.environ.get("SUB_CERT_DUCKDB_TEMP_DIR", "/tmp/sub_cert_duckdb")

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sub_certifications_mv_serving_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        text,
    feed          text        NOT NULL,
    rows_written  bigint,
    distinct_ueis bigint,
    write_mode    text,
    indices_built text,
    status        text        NOT NULL,
    error_message text,
    metrics       jsonb,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT sub_certifications_mv_serving_runs_status_chk CHECK (status IN ('success','error'))
);
CREATE INDEX IF NOT EXISTS sub_certifications_mv_serving_runs_status_idx
    ON ops.sub_certifications_mv_serving_runs (status);
CREATE INDEX IF NOT EXISTS sub_certifications_mv_serving_runs_recorded_at_idx
    ON ops.sub_certifications_mv_serving_runs (recorded_at DESC);
"""


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _duck():
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    os.makedirs(DUCK_TMP, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{DUCK_TMP}'")
    return con


def _assembly_sql() -> str:
    excl = ", ".join("'" + k + "'" for k in ADJUDICATED_DESIGNATION_KEYS)
    nd_web = normalized_domain(_bare_host("website"))             # core.web_norm canonical normalizer
    nd_addl = normalized_domain(_bare_host("additional_website"))
    gen = is_generic_domain("dw.normalized_domain")
    return f"""
    WITH dsbs_exploded AS (
        -- F1: CAST to JSON[]; from_json(...,'JSON[]') is rejected by DuckDB. certs is 100% castable.
        SELECT d.uei,
               json_extract_string(c, '$.name')                       AS prog_name,
               json_extract_string(c, '$.status')                     AS prog_status,
               TRY_CAST(json_extract_string(c, '$.exitDate') AS DATE)  AS exit_date
        FROM dsbs d, UNNEST(CAST(d.certs AS JSON[])) AS t(c)
    ),
    dsbs_active AS (                                  -- D2 gate: Active + not expired (excludes Pending)
        SELECT uei, prog_name, exit_date FROM dsbs_exploded
        WHERE prog_status = 'Active' AND (exit_date IS NULL OR exit_date >= CURRENT_DATE)
    ),
    dsbs_pivot AS (
        SELECT uei,
            bool_or(prog_name = '8(a)')    AS cert_8a,
            bool_or(prog_name = '8(a) JV') AS cert_8a_jv,
            bool_or(prog_name = 'HUBZone') AS cert_hubzone,
            bool_or(prog_name = 'WOSB')    AS cert_wosb,
            bool_or(prog_name = 'EDWOSB')  AS cert_edwosb,
            bool_or(prog_name = 'VOSB')    AS cert_vosb,
            bool_or(prog_name = 'SDVOSB')  AS cert_sdvosb,
            max(exit_date) FILTER (WHERE prog_name = '8(a)')    AS cert_8a_exit_date,
            max(exit_date) FILTER (WHERE prog_name = 'HUBZone') AS cert_hubzone_exit_date,
            max(exit_date) FILTER (WHERE prog_name = 'WOSB')    AS cert_wosb_exit_date,
            max(exit_date) FILTER (WHERE prog_name = 'EDWOSB')  AS cert_edwosb_exit_date,
            max(exit_date) FILTER (WHERE prog_name = 'VOSB')    AS cert_vosb_exit_date,
            max(exit_date) FILTER (WHERE prog_name = 'SDVOSB')  AS cert_sdvosb_exit_date,
            min(exit_date) FILTER (WHERE exit_date >= CURRENT_DATE) AS next_cert_expiration_date,
            count(DISTINCT prog_name)                            AS cert_count,
            string_agg(DISTINCT CASE prog_name
                WHEN '8(a)' THEN '8a' WHEN '8(a) JV' THEN '8a_jv' WHEN 'HUBZone' THEN 'hubzone'
                WHEN 'WOSB' THEN 'wosb' WHEN 'EDWOSB' THEN 'edwosb' WHEN 'VOSB' THEN 'vosb'
                WHEN 'SDVOSB' THEN 'sdvosb' END, '|' ORDER BY 1)  AS cert_programs
        FROM dsbs_active
        GROUP BY uei
    ),
    sam_selfcert AS (
        SELECT e.uei,
            bool_or(dk.designation_key = 'minority_owned_business')                     AS sam_self_minority_owned,
            bool_or(dk.designation_key = 'self_certified_small_disadvantaged_business') AS sam_self_sdb,
            bool_or(dk.designation_key = 'woman_owned_business')                        AS sam_self_woman_owned,
            bool_or(dk.designation_key = 'veteran_owned_business')                      AS sam_self_veteran_owned,
            string_agg(DISTINCT dk.code, '|' ORDER BY 1)                                AS sam_self_codes
        FROM sam_ent e, UNNEST(e.business_types) AS t(bt)
        JOIN dict dk ON dk.namespace = 'business_types' AND dk.code = bt
            AND dk.sba_administered = false
            AND dk.designation_key NOT IN ({excl})
        GROUP BY e.uei
    ),
    sam_reg AS (   -- F2: registration status, self-cert-INDEPENDENT (entities are 1/UEI)
        SELECT uei, bool_or(is_active) AS sam_is_active FROM sam_ent GROUP BY uei
    ),
    t_agg AS (     -- D6: pre-aggregate edge-grain targeting to UEI (avoids 821x fan-out)
        SELECT candidate_sub_uei AS uei, CAST(count(*) AS INTEGER) AS n_targeting_edges
        FROM targeting WHERE candidate_sub_uei IS NOT NULL AND length(trim(candidate_sub_uei)) > 0
        GROUP BY candidate_sub_uei
    ),
    dsbs_web AS (  -- canonical normalized domains (core.web_norm); normalized_domain = best-of the two
        SELECT uei, nw AS normalized_website, na AS normalized_additional_website,
               coalesce(nw, na) AS normalized_domain
        FROM (SELECT uei, {nd_web} AS nw, {nd_addl} AS na FROM dsbs) z
    ),
    spine AS (
        SELECT uei FROM dsbs WHERE uei IS NOT NULL AND length(trim(uei)) > 0
        UNION
        SELECT sub_uei FROM profiles WHERE sub_uei IS NOT NULL AND length(trim(sub_uei)) > 0
        UNION
        SELECT candidate_sub_uei FROM targeting WHERE candidate_sub_uei IS NOT NULL
               AND length(trim(candidate_sub_uei)) > 0
    )
    SELECT
        s.uei,
        coalesce(d.legal_business_name, p.sub_name)                          AS legal_business_name,
        CASE WHEN d.uei IS NULL THEN 'sub_only'
             WHEN p.sub_uei IS NOT NULL THEN 'dsbs_and_sub'
             ELSE 'greenfield_dsbs' END                                      AS universe_tier,
        (d.uei IS NOT NULL)                                                  AS in_dsbs,
        (p.sub_uei IS NOT NULL)                                              AS is_subawardee,
        (t.uei IS NOT NULL)                                                  AS is_targeting_candidate,
        (d.uei IS NOT NULL AND p.sub_uei IS NULL AND t.uei IS NULL)          AS is_greenfield,
        (coalesce(dp.cert_count, 0) > 0)                                     AS cert_any,
        CAST(coalesce(dp.cert_count, 0) AS INTEGER)                          AS cert_count,
        dp.cert_programs,
        coalesce(dp.cert_8a, false)      AS cert_8a,
        coalesce(dp.cert_hubzone, false) AS cert_hubzone,
        coalesce(dp.cert_wosb, false)    AS cert_wosb,
        coalesce(dp.cert_edwosb, false)  AS cert_edwosb,
        coalesce(dp.cert_vosb, false)    AS cert_vosb,
        coalesce(dp.cert_sdvosb, false)  AS cert_sdvosb,
        coalesce(dp.cert_8a_jv, false)   AS cert_8a_jv,
        dp.cert_8a_exit_date, dp.cert_hubzone_exit_date, dp.cert_wosb_exit_date,
        dp.cert_edwosb_exit_date, dp.cert_vosb_exit_date, dp.cert_sdvosb_exit_date,
        dp.next_cert_expiration_date,
        CASE WHEN dp.next_cert_expiration_date IS NULL THEN 'none'
             WHEN dp.next_cert_expiration_date <= CURRENT_DATE + INTERVAL 90 DAY THEN 'expiring_90d'
             ELSE 'active' END                                              AS cert_lifecycle,
        coalesce(sc.sam_self_minority_owned OR sc.sam_self_sdb
                 OR sc.sam_self_woman_owned OR sc.sam_self_veteran_owned, false) AS sam_self_any,
        coalesce(sc.sam_self_minority_owned, false)  AS sam_self_minority_owned,
        coalesce(sc.sam_self_sdb, false)             AS sam_self_sdb,
        coalesce(sc.sam_self_woman_owned, false)     AS sam_self_woman_owned,
        coalesce(sc.sam_self_veteran_owned, false)   AS sam_self_veteran_owned,
        sc.sam_self_codes,
        coalesce(sr.sam_is_active, false)            AS sam_registration_active,
        (sr.uei IS NOT NULL)                         AS sam_present,
        d.email, d.phone, d.contact_person,
        d.website, dw.normalized_website,
        d.additional_website, dw.normalized_additional_website,
        dw.normalized_domain, {gen} AS domain_is_generic,
        p.poc_full_name, p.poc_title,
        CASE WHEN (d.email IS NOT NULL AND trim(d.email) <> '')
                OR (d.phone IS NOT NULL AND trim(d.phone) <> '')
                OR (d.contact_person IS NOT NULL AND trim(d.contact_person) <> '') THEN 'dsbs'
             WHEN (p.poc_full_name IS NOT NULL AND trim(p.poc_full_name) <> '') THEN 'subaward_poc'
             ELSE 'none' END                                                AS contact_source,
        coalesce(d.state, p.hq_state)  AS state,
        coalesce(d.city, p.hq_city)    AS city,
        d.county, d.zipcode, d.naics_primary,
        CASE WHEN d.state IS NOT NULL AND trim(d.state) <> '' THEN 'dsbs'
             WHEN p.hq_state IS NOT NULL AND trim(p.hq_state) <> '' THEN 'subaward'
             ELSE NULL END                                                  AS geo_source,
        (p.sub_uei IS NOT NULL)                      AS has_subaward_history,
        CAST(p.n_subawards AS INTEGER)               AS n_subawards,
        t.n_targeting_edges,
        p.total_subaward_amount,
        p.top_subaward_description,
        d.source_version                             AS dsbs_snapshot
    FROM spine s
    LEFT JOIN dsbs               d  ON d.uei = s.uei
    LEFT JOIN dsbs_pivot         dp ON dp.uei = s.uei
    LEFT JOIN dsbs_web           dw ON dw.uei = s.uei
    LEFT JOIN profiles           p  ON p.sub_uei = s.uei
    LEFT JOIN t_agg              t  ON t.uei = s.uei
    LEFT JOIN sam_selfcert       sc ON sc.uei = s.uei
    LEFT JOIN sam_reg            sr ON sr.uei = s.uei
    ORDER BY s.uei
    """


def _assemble(so) -> tuple:
    """Materialize the 5 sources (NEVER .to_reader — one-shot), run the assembly, compute stats."""
    import lance
    con = _duck()
    con.register("dsbs", lance.dataset(DSBS_URI, storage_options=so).scanner(columns=[
        "uei", "legal_business_name", "certs", "email", "phone", "contact_person",
        "website", "additional_website",
        "state", "city", "county", "zipcode", "naics_primary", "source_version"]).to_table())
    con.register("profiles", lance.dataset(PROFILES_URI, storage_options=so).scanner(columns=[
        "sub_uei", "sub_name", "hq_state", "hq_city", "poc_full_name", "poc_title",
        "n_subawards", "total_subaward_amount", "top_subaward_description"]).to_table())
    con.register("targeting", lance.dataset(TARGETING_URI, storage_options=so).scanner(columns=[
        "candidate_sub_uei"]).to_table())
    con.register("sam_ent", lance.dataset(SAM_ENT_URI, storage_options=so).scanner(columns=[
        "uei", "business_types", "is_active"]).to_table())
    con.register("dict", lance.dataset(DICT_URI, storage_options=so).scanner(columns=[
        "namespace", "code", "designation_key", "sba_administered"]).to_table())

    spine_n = con.execute("""
        SELECT count(*) FROM (
            SELECT uei FROM dsbs WHERE uei IS NOT NULL AND length(trim(uei))>0
            UNION SELECT sub_uei FROM profiles WHERE sub_uei IS NOT NULL AND length(trim(sub_uei))>0
            UNION SELECT candidate_sub_uei FROM targeting
                  WHERE candidate_sub_uei IS NOT NULL AND length(trim(candidate_sub_uei))>0)
    """).fetchone()[0]

    tbl = con.execute(_assembly_sql()).to_arrow_table()
    con.register("mv", tbl)
    s = con.execute("""
        SELECT count(*) AS rows, count(DISTINCT uei) AS distinct_uei,
            count(*) FILTER (WHERE in_dsbs)                AS in_dsbs,
            count(*) FILTER (WHERE is_subawardee)          AS is_subawardee,
            count(*) FILTER (WHERE is_targeting_candidate) AS is_targeting,
            count(*) FILTER (WHERE is_greenfield)          AS greenfield,
            count(*) FILTER (WHERE universe_tier='sub_only')      AS sub_only,
            count(*) FILTER (WHERE universe_tier='dsbs_and_sub')  AS dsbs_and_sub,
            count(*) FILTER (WHERE cert_any)        AS cert_any,
            count(*) FILTER (WHERE cert_8a)         AS cert_8a,
            count(*) FILTER (WHERE cert_hubzone)    AS cert_hubzone,
            count(*) FILTER (WHERE cert_wosb)       AS cert_wosb,
            count(*) FILTER (WHERE cert_edwosb)     AS cert_edwosb,
            count(*) FILTER (WHERE cert_vosb)       AS cert_vosb,
            count(*) FILTER (WHERE cert_sdvosb)     AS cert_sdvosb,
            count(*) FILTER (WHERE cert_8a_jv)      AS cert_8a_jv,
            count(*) FILTER (WHERE cert_lifecycle='expiring_90d') AS expiring_90d,
            count(*) FILTER (WHERE sam_self_any)            AS sam_self_any,
            count(*) FILTER (WHERE sam_self_minority_owned) AS sam_self_minority,
            count(*) FILTER (WHERE sam_self_sdb)            AS sam_self_sdb,
            count(*) FILTER (WHERE sam_self_woman_owned)    AS sam_self_woman,
            count(*) FILTER (WHERE sam_self_veteran_owned)  AS sam_self_veteran,
            count(*) FILTER (WHERE sam_registration_active) AS sam_reg_active,
            count(*) FILTER (WHERE sam_present)             AS sam_present,
            count(*) FILTER (WHERE contact_source='dsbs')        AS contact_dsbs,
            count(*) FILTER (WHERE contact_source='subaward_poc') AS contact_poc,
            count(*) FILTER (WHERE contact_source='none')        AS contact_none,
            count(*) FILTER (WHERE email IS NOT NULL AND trim(email)<>'') AS email_cov,
            count(*) FILTER (WHERE has_subaward_history)    AS has_subaward,
            count(*) FILTER (WHERE normalized_website IS NOT NULL)            AS norm_website_cov,
            count(*) FILTER (WHERE normalized_additional_website IS NOT NULL) AS norm_addl_cov,
            count(*) FILTER (WHERE normalized_domain IS NOT NULL)             AS norm_domain_cov,
            count(*) FILTER (WHERE domain_is_generic)                         AS domain_generic
        FROM mv
    """).fetchdf().iloc[0].to_dict()
    # the load-bearing anti-conflation gate (F4): retained self-cert codes ∩ adjudicated = ∅
    codes_in = ", ".join("'" + c + "'" for c in ADJUDICATED_CODES)
    conflation = con.execute(f"""
        SELECT count(*) FROM (
            SELECT unnest(string_split(sam_self_codes, '|')) AS code FROM mv WHERE sam_self_codes IS NOT NULL
        ) WHERE code IN ({codes_in})
    """).fetchone()[0]
    stats = {k: int(v) for k, v in s.items()}
    stats["spine_n"] = int(spine_n)
    stats["conflation_violations"] = int(conflation)
    con.close()
    return tbl, stats


def _stamp(tbl):
    import pyarrow as pa
    schema = sub_certifications_mv_schema()
    built_at = dt.datetime.now(dt.timezone.utc)
    tbl = tbl.append_column("built_at", pa.array([built_at] * tbl.num_rows, pa.timestamp("us", tz="UTC")))
    return tbl.select(schema.names).cast(schema), built_at


def _pg_connect():
    try:
        import psycopg
    except Exception:  # noqa: BLE001
        print("WARN: psycopg unavailable; skipping ops.* write."); return None
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write."); return None
    return psycopg.connect(dsn)


def _record_run(*, run_id, stats, indices, status, error, started, completed) -> bool:
    from psycopg.types.json import Jsonb
    conn = _pg_connect()
    if conn is None:
        return False
    st = stats or {}
    try:
        with conn.cursor() as cur:
            with conn.transaction():
                cur.execute("SELECT to_regclass('ops.sub_certifications_mv_serving_runs')")
                if cur.fetchone()[0] is None:
                    cur.execute(OPS_DDL)
            with conn.transaction():
                cur.execute(
                    "INSERT INTO ops.sub_certifications_mv_serving_runs "
                    "(run_id, feed, rows_written, distinct_ueis, write_mode, indices_built, "
                    " status, error_message, metrics, started_at, completed_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (run_id, FEED, st.get("rows"), st.get("distinct_uei"), "overwrite",
                     ",".join(indices), status, error, Jsonb(st) if st else None, started, completed))
        return True
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"ERROR: ops.sub_certifications_mv_serving_runs write failed: {exc}", file=sys.stderr)
        return False
    finally:
        conn.close()


def dry_run() -> dict:
    """Assemble + validate, print stats, WRITE NOTHING. Pre-flight gate before build()."""
    so = _r2_so()
    log("assembling (dry run, no write) …")
    _tbl, stats = _assemble(so)
    log(f"rows={stats['rows']:,} distinct_uei={stats['distinct_uei']:,} spine_n={stats['spine_n']:,} "
        f"greenfield={stats['greenfield']:,} sub_only={stats['sub_only']:,} dsbs_and_sub={stats['dsbs_and_sub']:,}")
    log(f"cert: any={stats['cert_any']:,} 8a={stats['cert_8a']:,} hubzone={stats['cert_hubzone']:,} "
        f"wosb={stats['cert_wosb']:,} edwosb={stats['cert_edwosb']:,} vosb={stats['cert_vosb']:,} "
        f"sdvosb={stats['cert_sdvosb']:,} 8a_jv={stats['cert_8a_jv']:,} expiring_90d={stats['expiring_90d']:,}")
    log(f"sam_self: any={stats['sam_self_any']:,} minority={stats['sam_self_minority']:,} "
        f"sdb={stats['sam_self_sdb']:,} woman={stats['sam_self_woman']:,} veteran={stats['sam_self_veteran']:,} "
        f"reg_active={stats['sam_reg_active']:,} present={stats['sam_present']:,}")
    log(f"contact: dsbs={stats['contact_dsbs']:,} poc={stats['contact_poc']:,} none={stats['contact_none']:,} "
        f"email_cov={stats['email_cov']:,} | CONFLATION_VIOLATIONS={stats['conflation_violations']}")
    log(f"web: normalized_website={stats['norm_website_cov']:,} normalized_additional={stats['norm_addl_cov']:,} "
        f"normalized_domain={stats['norm_domain_cov']:,} domain_is_generic={stats['domain_generic']:,}")
    print(json.dumps(stats, indent=2, default=str))
    return stats


def build() -> dict:
    import lance
    run_id = f"sub_cert_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    started = dt.datetime.now(dt.timezone.utc)
    status, error, stats, indices = "error", None, {}, []
    try:
        so = _r2_so()
        schema = sub_certifications_mv_schema()
        log("assembling …")
        tbl, stats = _assemble(so)
        # ── guards before any write ───────────────────────────────────────────
        if stats["rows"] == 0:
            raise RuntimeError("zero rows assembled — refusing to overwrite a good sink")
        if stats["rows"] != stats["distinct_uei"]:
            raise RuntimeError(f"GRAIN VIOLATION: rows={stats['rows']} != distinct_uei={stats['distinct_uei']}")
        if stats["rows"] != stats["spine_n"]:
            raise RuntimeError(f"SPINE MISMATCH: rows={stats['rows']} != spine_n={stats['spine_n']}")
        if stats["conflation_violations"] != 0:
            raise RuntimeError(f"NAMESPACE CONFLATION: {stats['conflation_violations']} rows carry an "
                               f"adjudicated code in sam_self_codes")
        log(f"assembled {stats['rows']:,} rows · greenfield={stats['greenfield']:,} "
            f"sub_only={stats['sub_only']:,} cert_any={stats['cert_any']:,} sam_self_any={stats['sam_self_any']:,}")
        tbl, built_at = _stamp(tbl)
        if not tbl.schema.equals(schema, check_metadata=False):
            raise RuntimeError("assembled schema != frozen sub_certifications_mv_schema()")
        log(f"writing OVERWRITE → {SUB_CERTIFICATIONS_MV_URI}")
        lance.write_dataset(tbl, SUB_CERTIFICATIONS_MV_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
        assert_schema(SUB_CERTIFICATIONS_MV_URI, schema, so)
        ds = lance.dataset(SUB_CERTIFICATIONS_MV_URI, storage_options=so)
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            indices.append(f"{col}:BTREE"); log(f"  BTREE  ✓ {col}")
        for col in BITMAP_INDEXES:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            indices.append(f"{col}:BITMAP"); log(f"  BITMAP ✓ {col}")
        status = "success"
        log(f"DONE → {SUB_CERTIFICATIONS_MV_URI} rows={stats['rows']:,}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc); log(f"FAILED: {error}")
    finally:
        ledger_written = _record_run(run_id=run_id, stats=stats, indices=indices, status=status,
                                     error=error, started=started, completed=dt.datetime.now(dt.timezone.utc))
        log(f"ledger_written={ledger_written}")
    if status != "success":
        raise RuntimeError(f"build failed: {error}")
    return {"status": status, "uri": SUB_CERTIFICATIONS_MV_URI, "run_id": run_id, **stats}


def verify() -> dict:
    import lance
    so = _r2_so()
    schema = sub_certifications_mv_schema()
    ds = lance.dataset(SUB_CERTIFICATIONS_MV_URI, storage_options=so)
    assert_schema(SUB_CERTIFICATIONS_MV_URI, schema, so)
    try:
        idx = sorted(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                     for i in ds.list_indices())
    except Exception:  # noqa: BLE001
        idx = []
    con = _duck()
    con.register("t", ds.to_table())
    codes_in = ", ".join("'" + c + "'" for c in ADJUDICATED_CODES)
    out = {
        "uri": SUB_CERTIFICATIONS_MV_URI, "rows": ds.count_rows(),
        "columns": len(schema.names), "indices": idx, "n_indices": len(idx), "schema_assert": "PASS",
        "distinct_uei": con.execute("SELECT count(DISTINCT uei) FROM t").fetchone()[0],
        "grain_unique": con.execute("SELECT count(*) = count(DISTINCT uei) FROM t").fetchone()[0],
        "by_tier": dict(con.execute(
            "SELECT universe_tier, count(*) FROM t GROUP BY 1 ORDER BY 2 DESC").fetchall()),
        "cert_any": con.execute("SELECT count(*) FROM t WHERE cert_any").fetchone()[0],
        "cert_by_program": {p: con.execute(f"SELECT count(*) FROM t WHERE cert_{p}").fetchone()[0]
                            for p in ("8a", "hubzone", "wosb", "edwosb", "vosb", "sdvosb", "8a_jv")},
        "sam_self_any": con.execute("SELECT count(*) FROM t WHERE sam_self_any").fetchone()[0],
        "normalized_domain_cov": con.execute(
            "SELECT count(*) FROM t WHERE normalized_domain IS NOT NULL").fetchone()[0],
        "domain_is_generic": con.execute("SELECT count(*) FROM t WHERE domain_is_generic").fetchone()[0],
        "conflation_violations": con.execute(
            f"SELECT count(*) FROM (SELECT unnest(string_split(sam_self_codes,'|')) AS code FROM t "
            f"WHERE sam_self_codes IS NOT NULL) WHERE code IN ({codes_in})").fetchone()[0],
        # override proof: no sub-only (non-DSBS) firm may carry an adjudicated cert
        "non_dsbs_with_cert": con.execute(
            "SELECT count(*) FROM t WHERE NOT in_dsbs AND cert_any").fetchone()[0],
    }
    con.close()
    print(json.dumps(out, indent=2, default=str))
    return out


if _HAS_MODAL:
    image = (modal.Image.debian_slim(python_version="3.12")
             .pip_install("duckdb>=1.5,<2", "pylance>=7", "pyarrow>=17", "boto3>=1.34",
                          "psycopg[binary]>=3.2")
             .env({"RUST_LOG": "error"}))
    app = modal.App("sub-certifications-mv", image=image)

    @app.function(secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
                  timeout=60 * 20, memory=16384, cpu=8.0, retries=1)
    def run_build() -> dict:
        return build()

    @app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10, memory=8192, cpu=2.0)
    def run_verify() -> dict:
        return verify()

    @app.local_entrypoint()
    def main(cmd: str = "build") -> None:
        fn = {"build": run_build, "verify": run_verify}.get(cmd)
        if fn is None:
            raise SystemExit(f"unknown cmd {cmd!r}")
        print(json.dumps(fn.remote(), indent=2, default=str))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build govcon_sub_certifications_mv (hybrid DSBS×SAM certs).")
    ap.add_argument("cmd", choices=["dry_run", "build", "verify"])
    a = ap.parse_args()
    if a.cmd == "dry_run":
        dry_run()
    elif a.cmd == "build":
        print(json.dumps(build(), indent=2, default=str))
    else:
        verify()
