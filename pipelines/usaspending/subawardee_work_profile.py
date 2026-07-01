"""SUBAWARDEE WORK PROFILE (Tier 0) — per-entity 5-year structured work profile for the
firms that won a procurement subaward in the last 90 days.

Purpose: outreach substrate. For every distinct ``subawardee_uei`` in the 90-day API-fresh
subaward feed, materialize what that firm ACTUALLY DOES — derived from its real federal work
over the trailing 5 years, in BOTH roles:

  • PRIME role   — every FPDS contract action it won directly
                   (``transaction_search_fpds``, recipient_uei index pushdown).
  • SUBAWARD role — every procurement subaward it received: the ``subaward_search`` bulk
                   mirror (sub_awardee_or_recipient_uei index pushdown) UNIONED with the
                   90-day API-fresh feed, deduped on (award_key, subaward_number). The mirror
                   LAGS the fresh feed, so the just-won subawards that defined this target set
                   would otherwise be missing from the 5-year history.

Plus a RECENT-WIN anchor from the 90-day feed (the line to reference in outreach).

This is the FREE tier of the subaward→attachment bridge scope (see
``docs/reference/SUBAWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md``): pure structured aggregation,
ZERO live SAM.gov harvest. The solicitation-document substrate (Tier 1/2) layers on top via
the validated offline bridge + resources sweep — not built here.

SNAPSHOT semantics (NOT append-accumulating): the 5-year window slides, so each build is a
full recompute → ``mode="overwrite"`` with a ``snapshot_date``. One ops ledger row per build.

Output: one row per subawardee_uei →
    s3://data-sink/active/subawardee_work_profile/
  BTREE on subawardee_uei / subawardee_parent_uei / subawardee_state_code.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' \
      python3 pipelines/usaspending/subawardee_work_profile.py <init_ops|build|build_wide|verify> [years]

``build``      → recent-cohort table (fresh 90-day subawardees, rolling 5y) → subawardee_work_profile/
``build_wide`` → FULL subawardee universe since a fixed floor (default 2021-01-01), seeded from
                 subaward_search ∪ fresh → subawardee_work_profile_wide/. The canonical ``build``
                 table is NEVER touched by ``build_wide``.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import time

# ─────────────────────────── constants ───────────────────────────

FEED = "subawardee_work_profile"
PROFILE_URI = os.environ.get(
    "SUBAWARDEE_WORK_PROFILE_URI",
    "s3://data-sink/active/subawardee_work_profile",
).rstrip("/") + "/"
# WIDE variant — the full subawardee universe since a fixed floor, written to a DISTINCT table
# so the canonical recent-cohort table above is NEVER touched by the wide build.
PROFILE_WIDE_URI = os.environ.get(
    "SUBAWARDEE_WORK_PROFILE_WIDE_URI",
    "s3://data-sink/active/subawardee_work_profile_wide",
).rstrip("/") + "/"
WIDE_FLOOR = os.environ.get("SUBAWARDEE_WORK_PROFILE_WIDE_FLOOR", "2021-01-01")

SUB_FRESH_URI = os.environ.get(
    "USASPENDING_API_SUBAWARD_FRESH_URI",
    "s3://data-sink/active/usaspending_api_fresh/contract_subaward",
).rstrip("/") + "/"
FPDS_URI = "s3://data-sink/active/usaspending/transaction_search_fpds/"
SUBS_URI = "s3://data-sink/active/usaspending/subaward_search/"

DEFAULT_YEARS = 5
BATCH = 4000                       # UEIs per index-pushdown scan
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 250_000
SCRATCH = os.environ.get("SWP_SCRATCH", "/tmp/subawardee_work_profile")
INDEX_COLS = ["subawardee_uei", "subawardee_parent_uei", "subawardee_state_code"]


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


# ─────────────────────────── R2 / creds ───────────────────────────

def _r2_so():
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _inlist(vals):
    return ",".join("'" + v.replace("'", "''") + "'" for v in vals)


def _batched(seq, n):
    return [seq[i:i + n] for i in range(0, len(seq), n)]


# ─────────────────────────── build ───────────────────────────

def build(years=DEFAULT_YEARS, wide=False):
    import duckdb
    import lance

    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    os.makedirs(SCRATCH, exist_ok=True)
    os.makedirs(f"{SCRATCH}/spill", exist_ok=True)
    today = dt.datetime.now(dt.timezone.utc).date()
    # wide: full subawardee universe from a fixed floor → PROFILE_WIDE_URI (canonical untouched).
    # recent (default): rolling 5y window → PROFILE_URI, byte-identical to prior behavior.
    cutoff = WIDE_FLOOR if wide else (today - dt.timedelta(days=365 * years)).isoformat()
    out_uri = PROFILE_WIDE_URI if wide else PROFILE_URI

    con = duckdb.connect(f"{SCRATCH}/build.duckdb")
    con.execute("PRAGMA threads=4;")
    con.execute(f"PRAGMA temp_directory='{SCRATCH}/spill';")
    # idempotent re-run: clear working tables (the fp_/sb_ parquet scan caches persist for resume)
    for _t in ("fresh", "fresh_dedup", "entities", "hist_pop", "pop", "fp", "sb_search", "sb",
               "prime_agg", "top_naics", "top_psc", "top_agency", "sub_agg", "sub_top_partners",
               "sub_top_naics", "profile"):
        con.execute(f"DROP TABLE IF EXISTS {_t};")

    status, error, rows = "error", None, 0
    try:
        # ── 0. entity universe + recent-win anchor (90-day feed) ──
        sub = lance.dataset(SUB_FRESH_URI, storage_options=so)
        con.register("rf", sub.scanner(columns=[
            "subawardee_uei", "subawardee_name", "subawardee_parent_uei",
            "subawardee_state_code", "subawardee_country_code", "prime_awardee_uei",
            "prime_awardee_name", "subaward_amount", "subaward_action_date",
            "subaward_description", "prime_award_base_transaction_description",
            "prime_award_naics_code", "prime_award_naics_description",
            "prime_award_unique_key", "subaward_number"]).to_reader())
        con.execute("""CREATE TABLE fresh AS
          SELECT nullif(trim(subawardee_uei),'')               AS uei,
                 nullif(trim(subawardee_name),'')              AS name,
                 nullif(trim(subawardee_parent_uei),'')        AS parent_uei,
                 nullif(trim(subawardee_state_code),'')        AS state_code,
                 nullif(trim(subawardee_country_code),'')      AS country_code,
                 nullif(trim(prime_awardee_uei),'')            AS prime_uei,
                 nullif(trim(prime_awardee_name),'')           AS prime_name,
                 TRY_CAST(subaward_amount AS DOUBLE)           AS amt,
                 TRY_CAST(subaward_action_date AS DATE)        AS adt,
                 nullif(trim(subaward_description),'')          AS sub_scope,
                 nullif(trim(prime_award_base_transaction_description),'') AS proj_desc,
                 nullif(trim(prime_award_naics_code),'')       AS naics,
                 nullif(trim(prime_award_naics_description),'') AS naics_desc,
                 trim(prime_award_unique_key)                  AS pawk,
                 trim(subaward_number)                         AS subnum
          FROM rf WHERE nullif(trim(subawardee_uei),'') IS NOT NULL;""")
        con.unregister("rf")

        # The 90-day feed is append-accumulating with INTENTIONAL FFATA/FSRS re-pull duplicates
        # (overlapping daily windows). Collapse to true subaward grain (prime_award_unique_key +
        # subaward_number), keeping the latest revision per subaward, BEFORE counting — else the
        # recent-win anchor inflates ~1.5x.
        con.execute("""CREATE TABLE fresh_dedup AS
          SELECT uei, pawk, subnum,
                 arg_max(name, adt)        AS name,
                 arg_max(parent_uei, adt)  AS parent_uei,
                 arg_max(state_code, adt)  AS state_code,
                 arg_max(country_code, adt) AS country_code,
                 arg_max(prime_name, adt)  AS prime_name,
                 arg_max(prime_uei, adt)   AS prime_uei,
                 arg_max(amt, adt)         AS amt,
                 max(adt)                  AS adt,
                 arg_max(sub_scope, adt)   AS sub_scope,
                 arg_max(proj_desc, adt)   AS proj_desc,
                 arg_max(naics, adt)       AS naics,
                 arg_max(naics_desc, adt)  AS naics_desc
          FROM fresh GROUP BY uei, pawk, subnum;""")

        # identity (latest non-null by recency) + recent-win aggregates over DISTINCT subawards
        con.execute("""CREATE TABLE entities AS
          SELECT uei,
                 arg_max(name, adt)                            AS subawardee_name,
                 arg_max(parent_uei, adt)                      AS subawardee_parent_uei,
                 arg_max(state_code, adt)                      AS subawardee_state_code,
                 arg_max(country_code, adt)                    AS subawardee_country_code,
                 count(*)                                      AS recent_subawards_90d,
                 sum(amt)                                      AS recent_subaward_amount_90d,
                 max(adt)                                      AS recent_latest_action_date,
                 arg_max(prime_name, adt)                      AS recent_top_prime_name,
                 arg_max(prime_uei, adt)                       AS recent_top_prime_uei,
                 arg_max(sub_scope, adt)                       AS recent_subaward_scope,
                 arg_max(proj_desc, adt)                       AS recent_prime_award_description,
                 arg_max(naics, adt)                           AS recent_top_naics_code,
                 arg_max(naics_desc, adt)                      AS recent_top_naics_description
          FROM fresh_dedup GROUP BY uei;""")

        # ── 0b. population. recent: fresh subawardees only. wide: fresh ∪ EVERY subawardee in
        #    subaward_search since the floor, carrying identity for the net-new firms. ──
        if wide:
            subs_pop = lance.dataset(SUBS_URI, storage_options=so)
            con.register("srp", subs_pop.scanner(
                columns=["sub_awardee_or_recipient_uei", "sub_awardee_or_recipient_legal",
                         "sub_legal_entity_state_code", "sub_legal_entity_country_code",
                         "sub_action_date"],
                filter=(f"sub_action_date >= DATE '{cutoff}' "
                        f"AND sub_action_date <= DATE '2026-12-31'")).to_reader())
            con.execute("""CREATE TABLE hist_pop AS
              SELECT nullif(trim(sub_awardee_or_recipient_uei),'')                             AS uei,
                     arg_max(nullif(trim(sub_awardee_or_recipient_legal),''), sub_action_date) AS name,
                     arg_max(nullif(trim(sub_legal_entity_state_code),''), sub_action_date)     AS state_code,
                     arg_max(nullif(trim(sub_legal_entity_country_code),''), sub_action_date)   AS country_code
              FROM srp WHERE nullif(trim(sub_awardee_or_recipient_uei),'') IS NOT NULL
              GROUP BY 1;""")
            con.unregister("srp")
            con.execute("""CREATE TABLE pop AS
              SELECT coalesce(e.uei, h.uei)                              AS uei,
                     coalesce(e.subawardee_name, h.name)                 AS subawardee_name,
                     e.subawardee_parent_uei                             AS subawardee_parent_uei,
                     coalesce(e.subawardee_state_code, h.state_code)     AS subawardee_state_code,
                     coalesce(e.subawardee_country_code, h.country_code) AS subawardee_country_code
              FROM hist_pop h FULL OUTER JOIN entities e ON e.uei = h.uei;""")
        else:
            con.execute("""CREATE TABLE pop AS
              SELECT uei, subawardee_name, subawardee_parent_uei,
                     subawardee_state_code, subawardee_country_code FROM entities;""")
        ueis = [r[0] for r in con.execute("SELECT uei FROM pop WHERE uei IS NOT NULL").fetchall()]
        log(f"population={len(ueis)} (wide={wide})  window={cutoff}..{today}")
        batches = _batched(ueis, BATCH)

        # ── 1. PRIME role — FPDS 5y, recipient_uei index pushdown (window-cached for resume) ──
        fp_cache = f"{SCRATCH}/fp_{cutoff}.parquet"
        if os.path.exists(fp_cache):
            con.execute(f"CREATE TABLE fp AS SELECT * FROM '{fp_cache}';")
            log(f"FPDS prime scan: loaded cache {fp_cache}")
        else:
            con.execute("""CREATE TABLE fp(uei VARCHAR, award_key VARCHAR, adt DATE, oblig DOUBLE,
                naics VARCHAR, naics_desc VARCHAR, psc VARCHAR, psc_desc VARCHAR,
                toptier VARCHAR, subtier VARCHAR, sol VARCHAR);""")
            fpds = lance.dataset(FPDS_URI, storage_options=so)
            t0 = time.time()
            for bi, b in enumerate(batches):
                flt = f"recipient_uei IN ({_inlist(b)}) AND action_date >= DATE '{cutoff}'"
                tbl = fpds.scanner(columns=[
                    "recipient_uei", "generated_unique_award_id", "action_date",
                    "federal_action_obligation", "naics_code", "naics_description",
                    "product_or_service_code", "product_or_service_description",
                    "awarding_toptier_agency_name", "awarding_subtier_agency_name",
                    "solicitation_identifier"], filter=flt).to_table()
                con.register("t", tbl)
                con.execute("""INSERT INTO fp SELECT trim(recipient_uei),
                    trim(generated_unique_award_id), CAST(action_date AS DATE),
                    TRY_CAST(federal_action_obligation AS DOUBLE),
                    nullif(trim(naics_code),''), nullif(trim(naics_description),''),
                    nullif(trim(product_or_service_code),''), nullif(trim(product_or_service_description),''),
                    nullif(trim(awarding_toptier_agency_name),''), nullif(trim(awarding_subtier_agency_name),''),
                    nullif(trim(solicitation_identifier),'') FROM t;""")
                con.unregister("t")
                log(f"  FPDS batch {bi+1}/{len(batches)} rows+={tbl.num_rows}")
            con.execute(f"COPY fp TO '{fp_cache}' (FORMAT parquet);")
            log(f"FPDS prime scan done in {(time.time()-t0)/60:.1f}m -> cached")

        # ── 2. SUBAWARD role — subaward_search 5y (sub uei index pushdown, window-cached) ──
        # subaward_search is the USAspending bulk mirror and LAGS the 90-day API-fresh feed: the
        # very subawards that defined this target set are not in it yet. So sb_search is UNIONED
        # below with the fresh feed (fresh_dedup), deduped on (award_key, subaward_number).
        sb_cache = f"{SCRATCH}/sb_{cutoff}.parquet"
        if os.path.exists(sb_cache):
            con.execute(f"CREATE TABLE sb_search AS SELECT * FROM '{sb_cache}';")
            log(f"subaward scan: loaded cache {sb_cache}")
        else:
            con.execute("""CREATE TABLE sb_search(uei VARCHAR, award_key VARCHAR, subnum VARCHAR,
                amt DOUBLE, adt DATE, prime_name VARCHAR, prime_uei VARCHAR,
                naics VARCHAR, naics_desc VARCHAR);""")
            subs = lance.dataset(SUBS_URI, storage_options=so)
            t0 = time.time()
            for bi, b in enumerate(batches):
                flt = (f"sub_awardee_or_recipient_uei IN ({_inlist(b)}) "
                       f"AND sub_action_date >= DATE '{cutoff}'")
                tbl = subs.scanner(columns=[
                    "sub_awardee_or_recipient_uei", "unique_award_key", "subaward_number",
                    "subaward_amount", "sub_action_date", "awardee_or_recipient_legal",
                    "awardee_or_recipient_uei", "naics", "naics_description"], filter=flt).to_table()
                con.register("t", tbl)
                con.execute("""INSERT INTO sb_search SELECT trim(sub_awardee_or_recipient_uei),
                    trim(unique_award_key), nullif(trim(subaward_number),''),
                    TRY_CAST(subaward_amount AS DOUBLE), TRY_CAST(sub_action_date AS DATE),
                    nullif(trim(awardee_or_recipient_legal),''), nullif(trim(awardee_or_recipient_uei),''),
                    nullif(trim(naics),''), nullif(trim(naics_description),'') FROM t;""")
                con.unregister("t")
                log(f"  subaward batch {bi+1}/{len(batches)} rows+={tbl.num_rows}")
            con.execute(f"COPY sb_search TO '{sb_cache}' (FORMAT parquet);")
            log(f"subaward scan done in {(time.time()-t0)/60:.1f}m -> cached")

        # union the lagging mirror with the fresh feed; dedup on subaward identity, preferring
        # the mirror (richer fields) where a fresh win has already propagated.
        con.execute(f"""CREATE TABLE sb AS
          WITH u AS (
            SELECT uei, award_key, subnum, amt, adt, prime_name, prime_uei, naics, naics_desc, 0 AS pr
              FROM sb_search
            UNION ALL
            SELECT uei, pawk AS award_key, subnum, amt, adt, prime_name, prime_uei, naics, naics_desc, 1 AS pr
              FROM fresh_dedup WHERE adt >= DATE '{cutoff}'),
          r AS (SELECT *, row_number() OVER (
                  PARTITION BY uei, award_key, subnum ORDER BY pr ASC, adt DESC NULLS LAST) rn
                FROM u)
          SELECT uei, award_key, subnum, amt, adt, prime_name, prime_uei, naics, naics_desc
          FROM r WHERE rn = 1;""")
        sbn = con.execute("""SELECT count(*),
            (SELECT count(*) FROM sb_search),
            count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM sb_search s
                WHERE s.uei=sb.uei AND s.award_key=sb.award_key
                  AND s.subnum IS NOT DISTINCT FROM sb.subnum)) FROM sb;""").fetchone()
        log(f"subaward role: mirror={sbn[1]} union_distinct={sbn[0]} fresh_only_added={sbn[2]}")

        # ── 3. prime-role aggregates ──
        con.execute("""CREATE TABLE prime_agg AS
          SELECT uei,
            count(DISTINCT award_key)                                       AS prime_awards_5y,
            sum(oblig)                                                      AS prime_obligated_5y,
            count(DISTINCT award_key) FILTER (WHERE sol IS NOT NULL)        AS prime_competed_awards_5y,
            count(DISTINCT sol)                                             AS prime_distinct_solnums_5y,
            count(DISTINCT naics)                                           AS prime_distinct_naics_5y,
            min(adt)                                                        AS prime_first_action_date,
            max(adt)                                                        AS prime_last_action_date
          FROM fp GROUP BY uei;""")

        def topn(tbl, dimcols, dimout, metric, n=5):
            # returns SQL building list<struct> of top-n dims per uei
            keyexpr = ", ".join(dimcols)
            return f"""
            WITH roll AS (
              SELECT uei, {keyexpr},
                     count(DISTINCT award_key) AS awards, sum({metric}) AS metric
              FROM {tbl} WHERE {dimcols[0]} IS NOT NULL GROUP BY uei, {keyexpr}),
            r AS (SELECT *, row_number() OVER (PARTITION BY uei ORDER BY metric DESC NULLS LAST) rn FROM roll)
            SELECT uei, list({dimout} ORDER BY metric DESC) FILTER (WHERE rn <= {n}) AS top
            FROM r GROUP BY uei"""

        con.execute("CREATE TABLE top_naics AS " + topn(
            "fp", ["naics", "naics_desc"],
            "{'code': naics, 'description': naics_desc, 'awards': awards, 'obligated': metric}",
            "oblig"))
        con.execute("CREATE TABLE top_psc AS " + topn(
            "fp", ["psc", "psc_desc"],
            "{'code': psc, 'description': psc_desc, 'awards': awards, 'obligated': metric}",
            "oblig"))
        con.execute("CREATE TABLE top_agency AS " + topn(
            "fp", ["toptier", "subtier"],
            "{'agency': toptier, 'subagency': subtier, 'awards': awards, 'obligated': metric}",
            "oblig"))

        # ── 4. subaward-role aggregates ──
        con.execute("""CREATE TABLE sub_agg AS
          SELECT uei,
            count(*)                                   AS sub_received_5y,
            sum(amt)                                   AS sub_amount_5y,
            count(DISTINCT award_key)                  AS sub_distinct_primes_5y,
            count(DISTINCT prime_uei)                  AS sub_distinct_prime_partners_5y,
            min(adt)                                   AS sub_first_action_date,
            max(adt)                                   AS sub_last_action_date
          FROM sb GROUP BY uei;""")

        con.execute("""CREATE TABLE sub_top_partners AS
          WITH roll AS (
            SELECT uei, prime_uei, any_value(prime_name) nm,
                   count(*) subs, sum(amt) amt
            FROM sb WHERE prime_uei IS NOT NULL GROUP BY uei, prime_uei),
          r AS (SELECT *, row_number() OVER (PARTITION BY uei ORDER BY amt DESC NULLS LAST) rn FROM roll)
          SELECT uei, list({'name': nm, 'uei': prime_uei, 'subawards': subs, 'amount': amt}
                          ORDER BY amt DESC) FILTER (WHERE rn <= 5) AS top
          FROM r GROUP BY uei;""")
        con.execute("""CREATE TABLE sub_top_naics AS
          WITH roll AS (
            SELECT uei, naics, any_value(naics_desc) nd,
                   count(*) subs, sum(amt) amt
            FROM sb WHERE naics IS NOT NULL GROUP BY uei, naics),
          r AS (SELECT *, row_number() OVER (PARTITION BY uei ORDER BY amt DESC NULLS LAST) rn FROM roll)
          SELECT uei, list({'code': naics, 'description': nd, 'subawards': subs, 'amount': amt}
                          ORDER BY amt DESC) FILTER (WHERE rn <= 5) AS top
          FROM r GROUP BY uei;""")

        # ── 5. assemble one row per entity ──
        snapshot = today.isoformat()
        con.execute(f"""CREATE TABLE profile AS
          SELECT
            pop.uei                                          AS subawardee_uei,
            pop.subawardee_name, pop.subawardee_parent_uei,
            pop.subawardee_state_code, pop.subawardee_country_code,
            coalesce(e.recent_subawards_90d, 0)              AS recent_subawards_90d,
            coalesce(e.recent_subaward_amount_90d, 0.0)      AS recent_subaward_amount_90d,
            e.recent_latest_action_date,
            e.recent_top_prime_name, e.recent_top_prime_uei,
            e.recent_subaward_scope, e.recent_prime_award_description,
            e.recent_top_naics_code, e.recent_top_naics_description,
            coalesce(p.prime_awards_5y, 0)                   AS prime_awards_5y,
            coalesce(p.prime_obligated_5y, 0.0)              AS prime_obligated_5y,
            coalesce(p.prime_competed_awards_5y, 0)          AS prime_competed_awards_5y,
            coalesce(p.prime_distinct_solnums_5y, 0)         AS prime_distinct_solnums_5y,
            coalesce(p.prime_distinct_naics_5y, 0)           AS prime_distinct_naics_5y,
            p.prime_first_action_date, p.prime_last_action_date,
            tn.top                                           AS prime_top_naics,
            tp.top                                           AS prime_top_psc,
            ta.top                                           AS prime_top_agencies,
            coalesce(s.sub_received_5y, 0)                   AS sub_received_5y,
            coalesce(s.sub_amount_5y, 0.0)                   AS sub_amount_5y,
            coalesce(s.sub_distinct_primes_5y, 0)            AS sub_distinct_primes_5y,
            coalesce(s.sub_distinct_prime_partners_5y, 0)    AS sub_distinct_prime_partners_5y,
            s.sub_first_action_date, s.sub_last_action_date,
            stp.top                                          AS sub_top_prime_partners,
            stn.top                                          AS sub_top_naics,
            DATE '{snapshot}'                                AS snapshot_date,
            DATE '{cutoff}'                                  AS profile_window_start
          FROM pop
          LEFT JOIN entities e ON e.uei = pop.uei
          LEFT JOIN prime_agg p ON p.uei = pop.uei
          LEFT JOIN top_naics tn ON tn.uei = pop.uei
          LEFT JOIN top_psc tp ON tp.uei = pop.uei
          LEFT JOIN top_agency ta ON ta.uei = pop.uei
          LEFT JOIN sub_agg s ON s.uei = pop.uei
          LEFT JOIN sub_top_partners stp ON stp.uei = pop.uei
          LEFT JOIN sub_top_naics stn ON stn.uei = pop.uei;""")

        import pyarrow as pa
        tbl = con.execute("SELECT * FROM profile").arrow()
        if isinstance(tbl, pa.RecordBatchReader):
            tbl = tbl.read_all()
        rows = tbl.num_rows
        if rows == 0:
            raise RuntimeError("0 profile rows assembled")
        lance.write_dataset(tbl, out_uri, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        ds = lance.dataset(out_uri, storage_options=so)
        present = set(ds.schema.names)
        for c in INDEX_COLS:
            if c in present:
                ds.create_scalar_index(c, index_type="BTREE")
                log(f"  BTREE {c}")
        status = "success"
        log(f"DONE rows={rows} cols={len(ds.schema.names)} wide={wide} -> {out_uri}")
    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"
        raise
    finally:
        con.close()
        _record_run(years=years, rows=rows, status=status, error=error,
                    started=started, completed=dt.datetime.now(dt.timezone.utc))


# ─────────────────────────── ops ledger ───────────────────────────

def _record_run(*, years, rows, status, error, started, completed):
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED") or os.environ.get("HQX_DB_URL")
    if not dsn:
        log("WARN: no HQX dsn; skipping ops row"); return
    if status != "success" and not error:
        error = "unknown terminal failure"
    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute(
                """INSERT INTO ops.subawardee_work_profile_runs
                   (feed, window_years, rows_written, status, error_message, started_at, executed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (FEED, int(years), int(rows), status, (error or "")[:2000] or None,
                 started, completed))
            c.commit()
        log(f"ops row: status={status}")
    except Exception as e:  # noqa: BLE001
        log(f"WARN: ops write failed: {e}")


# ─────────────────────────── verify ───────────────────────────

def verify():
    import json

    import duckdb
    import lance
    so = _r2_so()
    ds = lance.dataset(PROFILE_URI, storage_options=so)
    try:
        idx = [getattr(i, "name", i.get("name") if isinstance(i, dict) else str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    con = duckdb.connect()
    con.register("psrc", ds.scanner(columns=[
        "subawardee_uei", "prime_awards_5y", "prime_obligated_5y", "prime_competed_awards_5y",
        "sub_received_5y", "sub_amount_5y", "recent_subawards_90d"]).to_reader())
    con.execute("CREATE TABLE p AS SELECT * FROM psrc;")
    con.unregister("psrc")
    r = con.execute("""SELECT count(*), count(DISTINCT subawardee_uei),
        count(*) FILTER (WHERE prime_awards_5y > 0), count(*) FILTER (WHERE sub_received_5y > 0),
        round(sum(prime_obligated_5y)/1e9, 1), round(sum(sub_amount_5y)/1e9, 1),
        round(avg(prime_awards_5y), 1), round(avg(sub_received_5y), 1)
        FROM p;""").fetchone()
    con.close()
    print(json.dumps({
        "uri": PROFILE_URI, "rows": ds.count_rows(), "columns": len(ds.schema.names),
        "indices": idx, "distinct_uei": r[1],
        "entities_with_prime_history": r[2], "entities_with_subaward_history": r[3],
        "total_prime_obligated_$B": r[4], "total_subaward_$B": r[5],
        "avg_prime_awards_per_entity": r[6], "avg_subawards_per_entity": r[7],
    }, indent=2, default=str))


def init_ops():
    import psycopg
    from pathlib import Path
    sql = Path(__file__).parent.joinpath("ops_subawardee_work_profile_runs.sql").read_text()
    with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as c, c.cursor() as cur:
        cur.execute(sql); c.commit()
    log("ops DDL applied")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    a2 = int(sys.argv[2]) if len(sys.argv) > 2 else None
    if cmd == "build":
        build(years=a2 or DEFAULT_YEARS)
    elif cmd == "build_wide":
        build(years=a2 or DEFAULT_YEARS, wide=True)
    elif cmd == "verify":
        verify()
    elif cmd == "init_ops":
        init_ops()
    else:
        print(f"unknown command: {cmd} (init_ops|build|build_wide|verify)"); sys.exit(2)


if __name__ == "__main__":
    main()
