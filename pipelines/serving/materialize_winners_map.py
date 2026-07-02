"""Serving worker — usaspending_winners_map_serving: one row per (winner_uei, winner_type)
for federal-contract winners over a ~730d build window, joined to the address-keyed geocode
crosswalk so every winner carries a lat/lon dot. The read model behind the sales-demo map.

GRAIN  1 row per (winner_uei, winner_type ∈ {prime_recipient, subawardee}).
SoR    s3://data-sink/active/usaspending_winners_map_serving/  (Lance v2.1; derived, overwrite)
INPUTS prime + subaward fresh feeds (rolling ~730d by action_date) ⋈ geocode_xwalk (addr_hash)
       ⋈ govcon_award_solicitation_profiles (award grain) rolled to the prime winner (PHASE 3).
       The window lives HERE (a WHERE), not in the crosswalk — extend it freely.
SIGNALS entity_obligated_usd (Σ federal_action_obligation / subaward_amount), award_count, naics,
       state — the demo filters ("construction NAICS 23, > $150k") — PLUS the PHASE-3 capability
       axis rolled from govcon_award_solicitation_profiles to the PRIME winner (recipient_uei =
       winner_uei). Subawardee rows carry capability defaults (scope is extracted from the prime's
       solicitation docs). Per prime winner: has_extracted_scope / requires_clearance / requires_cmmc
       (bool_or over the winner's profiled awards), req_clearance_level_max (max over covered awards),
       solicitation_scope_tags / labor_categories (deduped controlled-vocab union — FILTERABLE via Lance
       ``array_has``), covered_award_count + covered_award_keys (capped drill-down pointer). The
       SUB-only TEAMING axis (sourced from the sub profiles, NULL on prime rows): teaming_dollars_5y
       / n_teaming_primes (range-filterable, BTREE) + teaming_prime_names (list — exact prime legal
       names, filterable via ``array_has``). The SUB-only SELF-REPORTED axis (also sourced from the
       sub profiles, NULL on prime rows + on subs with no self-reported signal): subaward_description_tags
       (same 77-tag controlled vocab as solicitation_scope_tags — the long-tail 13,792 subs that self-report)
       + req_cert_tags (open-valued cert vocabulary) — both list columns, filterable via ``array_has``,
       UNGATED (they cover the full sub long tail, not the scope-extracted slice). NO
       chunk-derived verbatim text crosses into the serving table (CUI egress invariant — only
       structured/controlled-vocab fields; evidence_quote/requirement_detail never selected).
LEDGER ops.winners_map_serving_runs (HQX_DB_URL_POOLED) on every terminal state.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' \
      python3 pipelines/serving/materialize_winners_map.py <init_ops|build|verify|demo> [window_days]
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

# Repo root on sys.path so the canonical join-key import resolves whether this file is
# run as a script (python3 pipelines/serving/materialize_winners_map.py) or imported.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines._shared.addr_hash import addr_hash_sql  # noqa: E402

ACTIVE = "s3://data-sink/active"
SERVING_URI = os.environ.get("WINNERS_MAP_SERVING_URI", f"{ACTIVE}/usaspending_winners_map_serving/")
PRIME_URI = f"{ACTIVE}/usaspending_api_fresh/contract_prime_txn/"
SUB_URI = f"{ACTIVE}/usaspending_subaward_canonical/"  # repointed: reconciled BULK∪FRESH contract-subaward canonical (was usaspending_api_fresh/contract_subaward)
XWALK_URI = os.environ.get("GEOCODE_XWALK_URI", f"{ACTIVE}/geocode_xwalk/")
# PHASE 3: award-grain capability profiles, rolled to the prime winner here.
PROFILES_URI = os.environ.get("GOVCON_CAPABILITY_PROFILES_URI",
                              f"{ACTIVE}/govcon_award_solicitation_profiles/")
# Sub-side capability: sub_uei-grain profiles feed the subawardee winner rows (without this leg every
# subawardee row carries has_extracted_scope=false / empty tags — the capability filter excludes them).
SUB_PROFILES_URI = os.environ.get("GOVCON_SUB_CAPABILITY_PROFILES_URI",
                                  f"{ACTIVE}/govcon_subawardee_profiles/")
WINDOW_DAYS = int(os.environ.get("WINNERS_WINDOW_DAYS", "730"))
DATA_STORAGE_VERSION = "2.1"
COVERED_AWARD_KEYS_CAP = 50          # per-winner drill-down pointer bound (mega-IDIQ tail)
BTREE_INDEXES = ["winner_uei", "addr_hash",
                 # range/recency filter axes ('$X+ won', 'award_count over N', recency window).
                 "entity_obligated_usd", "award_count", "last_action_date",
                 # SUB-only teaming axis (range/threshold filters): null on prime rows.
                 "teaming_dollars_5y", "n_teaming_primes"]
BITMAP_INDEXES = ["naics2", "state", "winner_type",
                  # PHASE-3 capability filter axes (low-cardinality → BITMAP pushdown).
                  "has_extracted_scope", "requires_clearance", "requires_cmmc",
                  "req_clearance_level_max"]

DUCK_MEM = os.environ.get("WINNERS_DUCKDB_MEMORY_LIMIT", "6GB")
DUCK_TMP = os.environ.get("WINNERS_DUCKDB_TEMP_DIR", "/tmp/winners_map_duckdb")


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


def _assemble(so, window_days: int):
    import lance
    cutoff = (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=window_days)).isoformat()
    p = lance.dataset(PRIME_URI, storage_options=so)
    s = lance.dataset(SUB_URI, storage_options=so)
    x = lance.dataset(XWALK_URI, storage_options=so)
    con = _duck()
    con.register("p", p.scanner(
        columns=["recipient_uei", "recipient_name", "recipient_address_line_1",
                 "recipient_city_name", "recipient_state_code", "recipient_zip_4_code",
                 "naics_code", "action_date", "federal_action_obligation",
                 "contract_award_unique_key"],
        filter=f"action_date >= '{cutoff}'").to_reader())
    con.register("s", s.scanner(
        columns=["subawardee_uei", "subawardee_name", "subawardee_address_line_1",
                 "subawardee_city_name", "subawardee_state_code", "subawardee_zip_code",
                 "prime_award_naics_code", "subaward_action_date", "subaward_amount",
                 "subaward_number"],
        filter=f"subaward_action_date >= DATE '{cutoff}'").to_reader())  # canonical: typed DATE, not lexical VARCHAR
    con.register("x", x.scanner(columns=["addr_hash", "latitude", "longitude", "match_type"]).to_reader())
    # PHASE 3: award-grain capability profiles → re-scannable TABLE (referenced by several rollup
    # CTEs). Project ONLY structured / controlled-vocab columns — evidence_quote / requirement_detail
    # do not exist on the profiles schema by construction (CUI egress invariant), so none can leak.
    prof = lance.dataset(PROFILES_URI, storage_options=so)
    con.register("prof", prof.scanner(columns=[
        "recipient_uei", "contract_award_unique_key", "has_extracted_scope",
        "requires_clearance", "requires_cmmc", "req_clearance_level_max",
        "solicitation_scope_tags", "top_labor_categories"]).to_table())
    # Sub-side profiles (sub_uei grain, already one row per sub). Same CUI posture: only structured /
    # controlled-vocab columns; evidence_quote / requirement_detail are absent by construction.
    sub_prof = lance.dataset(SUB_PROFILES_URI, storage_options=so)
    con.register("sub_prof", sub_prof.scanner(columns=[
        "sub_uei", "has_extracted_scope", "requires_clearance", "requires_cmmc",
        "req_clearance_level_max", "solicitation_scope_tags", "top_labor_categories",
        "n_scope_solicitations", "source_notice_ids",
        # Teaming axis (sub-only): who the sub has subcontracted under + $ + breadth.
        "teaming_dollars_5y", "n_teaming_primes", "teaming_prime_names",
        # Self-reported axis (sub-only): the long-tail capability/cert signal subs assert about
        # themselves (UNGATED downstream — 13,792 subs self-report vs. the ~4,220 scope-extracted
        # slice). Same 77-tag controlled vocab as solicitation_scope_tags; req_cert_tags is open-valued.
        "subaward_description_tags", "req_cert_tags"]).to_table())
    hexpr = addr_hash_sql("street", "city", "state", "zip")
    sql = f"""
    WITH u AS (
        SELECT recipient_uei AS winner_uei, 'prime_recipient' AS winner_type,
               recipient_name AS winner_name, recipient_address_line_1 AS street,
               recipient_city_name AS city, recipient_state_code AS state,
               recipient_zip_4_code AS zip, naics_code AS naics, action_date AS adt,
               try_cast(federal_action_obligation AS DOUBLE) AS amt,
               contract_award_unique_key AS award_key
        FROM p WHERE recipient_uei IS NOT NULL AND length(trim(recipient_uei)) > 0
        UNION ALL
        SELECT subawardee_uei, 'subawardee', subawardee_name, subawardee_address_line_1,
               subawardee_city_name, subawardee_state_code, subawardee_zip_code,
               prime_award_naics_code, subaward_action_date,
               try_cast(subaward_amount AS DOUBLE), subaward_number
        FROM s WHERE subawardee_uei IS NOT NULL AND length(trim(subawardee_uei)) > 0
    ),
    -- "honest won" $: entity_obligated_usd / award_count sum ONLY positive in-window obligations
    -- (mirrors the awards map's `amt > 0` semantics) so de-obligation correction mods (negative)
    -- and $0 admin mods can't poison the dollar signal. Negative-poisoned winners read $0, never
    -- < 0. Unlike the awards map we do NOT drop rows here: identity / recency below aggregate over
    -- the FULL `u`, so every (winner_uei, winner_type) survives (≤$0 winners are valid CAPABILITY
    -- matches whose positive obligations simply fall outside the window).
    pos AS (SELECT * FROM u WHERE amt > 0 AND adt IS NOT NULL),
    pos_agg AS (
        SELECT winner_uei, winner_type,
               count(DISTINCT award_key) AS award_count,
               sum(amt) AS entity_obligated_usd,
               max(adt) AS last_positive_action_date
        FROM pos GROUP BY winner_uei, winner_type
    ),
    agg AS (
        SELECT u.winner_uei, u.winner_type,
               arg_max(u.winner_name, u.adt) AS winner_name,
               arg_max(u.street, u.adt) AS street, arg_max(u.city, u.adt) AS city,
               arg_max(u.state, u.adt) AS state, arg_max(u.zip, u.adt) AS zip,
               arg_max(u.naics, u.adt) AS naics_code,
               coalesce(any_value(pa.award_count), 0) AS award_count,
               coalesce(any_value(pa.entity_obligated_usd), 0) AS entity_obligated_usd,
               coalesce(max(pa.last_positive_action_date), max(u.adt)) AS last_action_date
        FROM u LEFT JOIN pos_agg pa USING (winner_uei, winner_type)
        GROUP BY u.winner_uei, u.winner_type
    ),
    keyed AS (
        SELECT *, substr(naics_code, 1, 2) AS naics2,
               {hexpr} AS addr_hash
        FROM agg
    ),
    -- ── PHASE 3 capability rollup: award-grain profiles → per prime winner ──────────────────
    cap_src AS (
        SELECT recipient_uei AS winner_uei, contract_award_unique_key AS award_key,
               coalesce(has_extracted_scope, false) AS hes,
               coalesce(requires_clearance, false) AS rc,
               coalesce(requires_cmmc, false) AS rcm,
               req_clearance_level_max AS clr,
               solicitation_scope_tags, top_labor_categories
        FROM prof
        WHERE recipient_uei IS NOT NULL AND length(trim(recipient_uei)) > 0
    ),
    cap AS (
        SELECT winner_uei,
               bool_or(hes) AS has_extracted_scope,
               bool_or(rc)  AS requires_clearance,
               bool_or(rcm) AS requires_cmmc,
               max(CASE clr WHEN 'TS_SCI' THEN 5 WHEN 'TOP_SECRET' THEN 4 WHEN 'SECRET' THEN 3
                            WHEN 'CONFIDENTIAL' THEN 2 WHEN 'PUBLIC_TRUST' THEN 1 ELSE 0 END) AS clr_ord,
               count(DISTINCT award_key) AS covered_award_count
        FROM cap_src GROUP BY 1
    ),
    cap_tags AS (   -- controlled-vocab union of the winner's awards' capability tags (sorted, deduped)
        SELECT winner_uei, list(t ORDER BY t) AS solicitation_scope_tags
        FROM (SELECT DISTINCT winner_uei, unnest(solicitation_scope_tags) AS t FROM cap_src)
        WHERE t IS NOT NULL GROUP BY 1
    ),
    cap_labor AS ( -- controlled-vocab union of the winner's awards' labor categories
        SELECT winner_uei, list(t ORDER BY t) AS labor_categories
        FROM (SELECT DISTINCT winner_uei, unnest(top_labor_categories) AS t FROM cap_src)
        WHERE t IS NOT NULL GROUP BY 1
    ),
    cap_keys_ranked AS (
        SELECT winner_uei, award_key,
               row_number() OVER (PARTITION BY winner_uei ORDER BY award_key) AS rk
        FROM (SELECT DISTINCT winner_uei, award_key FROM cap_src)
    ),
    cap_keys AS (  -- capped drill-down pointer (mega-IDIQ tail bound)
        SELECT winner_uei, list(award_key ORDER BY rk) AS covered_award_keys
        FROM cap_keys_ranked WHERE rk <= {COVERED_AWARD_KEYS_CAP} GROUP BY 1
    ),
    -- ── PHASE 3 sub-side capability: sub_uei-grain profiles → subawardee winner rows ─────────────
    -- The sub profile is already one row per sub_uei with the rolled fields; just project + map.
    -- covered_award_count ← n_scope_solicitations; covered_award_keys ← source_notice_ids (the
    -- solicitation provenance, already capped at 50 in the profile build).
    cap_sub AS (
        SELECT sub_uei AS winner_uei,
               coalesce(has_extracted_scope, false) AS has_extracted_scope,
               coalesce(requires_clearance, false)  AS requires_clearance,
               coalesce(requires_cmmc, false)       AS requires_cmmc,
               CASE req_clearance_level_max WHEN 'TS_SCI' THEN 5 WHEN 'TOP_SECRET' THEN 4
                    WHEN 'SECRET' THEN 3 WHEN 'CONFIDENTIAL' THEN 2 WHEN 'PUBLIC_TRUST' THEN 1
                    ELSE 0 END AS clr_ord,
               CAST(coalesce(n_scope_solicitations, 0) AS BIGINT) AS covered_award_count,
               solicitation_scope_tags,
               top_labor_categories AS labor_categories,
               source_notice_ids[1:{COVERED_AWARD_KEYS_CAP}] AS covered_award_keys,
               -- Teaming axis (sub-only): coalesce numerics to 0 (a profiled sub with no
               -- teaming reads 0, never null); keep the prime-name list as-is.
               coalesce(teaming_dollars_5y, 0) AS teaming_dollars_5y,
               CAST(coalesce(n_teaming_primes, 0) AS BIGINT) AS n_teaming_primes,
               teaming_prime_names,
               -- Self-reported axis (sub-only): keep the lists as-is (NULL for the ~11.7k subs
               -- with no self-reported signal). UNGATED — these are the long-tail signal.
               subaward_description_tags,
               req_cert_tags
        FROM sub_prof
        WHERE sub_uei IS NOT NULL AND length(trim(sub_uei)) > 0
    )
    SELECT k.winner_uei, k.winner_type, k.winner_name, k.street, k.city,
           upper(trim(k.state)) AS state, k.zip, k.naics_code, k.naics2,
           k.award_count, k.entity_obligated_usd, k.last_action_date, k.addr_hash,
           x.latitude, x.longitude, x.match_type,
           -- capability: prime winners roll from award profiles (cap*); subawardee winners from the
           -- sub profiles (cap_sub). The join conds are winner_type-exclusive, so at most one side is
           -- non-null per row → coalesce(prime, sub, default) is unambiguous.
           coalesce(cap.has_extracted_scope, cap_sub.has_extracted_scope, false) AS has_extracted_scope,
           coalesce(cap.requires_clearance, cap_sub.requires_clearance, false)  AS requires_clearance,
           coalesce(cap.requires_cmmc, cap_sub.requires_cmmc, false)            AS requires_cmmc,
           CASE coalesce(cap.clr_ord, cap_sub.clr_ord, 0) WHEN 5 THEN 'TS_SCI' WHEN 4 THEN 'TOP_SECRET'
                WHEN 3 THEN 'SECRET' WHEN 2 THEN 'CONFIDENTIAL' WHEN 1 THEN 'PUBLIC_TRUST'
                ELSE NULL END AS req_clearance_level_max,
           CAST(coalesce(cap.covered_award_count, cap_sub.covered_award_count, 0) AS BIGINT) AS covered_award_count,
           coalesce(cap_tags.solicitation_scope_tags, cap_sub.solicitation_scope_tags)   AS solicitation_scope_tags,
           coalesce(cap_labor.labor_categories, cap_sub.labor_categories) AS labor_categories,
           coalesce(cap_keys.covered_award_keys, cap_sub.covered_award_keys) AS covered_award_keys,
           -- Teaming axis: SUB-only (sourced from cap_sub). Prime rows have no cap_sub match,
           -- so these are NULL on primes (intended — mirrors how the sub-only signals behave).
           cap_sub.teaming_dollars_5y   AS teaming_dollars_5y,
           cap_sub.n_teaming_primes     AS n_teaming_primes,
           cap_sub.teaming_prime_names  AS teaming_prime_names,
           -- Self-reported axis: SUB-only (sourced from cap_sub). NULL on prime rows AND on subs
           -- with no self-reported signal — the decoder treats these as UNGATED list filters.
           cap_sub.subaward_description_tags AS subaward_description_tags,
           cap_sub.req_cert_tags                 AS req_cert_tags,
           'usaspending_winners_map_serving (derived)' AS source_file,
           now()::VARCHAR AS ingested_at
    FROM keyed k
    LEFT JOIN x USING (addr_hash)
    LEFT JOIN cap       ON k.winner_uei = cap.winner_uei       AND k.winner_type = 'prime_recipient'
    LEFT JOIN cap_tags  ON k.winner_uei = cap_tags.winner_uei  AND k.winner_type = 'prime_recipient'
    LEFT JOIN cap_labor ON k.winner_uei = cap_labor.winner_uei AND k.winner_type = 'prime_recipient'
    LEFT JOIN cap_keys  ON k.winner_uei = cap_keys.winner_uei  AND k.winner_type = 'prime_recipient'
    LEFT JOIN cap_sub   ON k.winner_uei = cap_sub.winner_uei   AND k.winner_type = 'subawardee'
    """
    tbl = con.execute(sql).fetch_arrow_table()
    con.close()
    return tbl


def _record_run(*, window_days, rows, with_coords, write_mode, indices, status, error, started):
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    coord_rate = round(with_coords / rows, 4) if rows else None
    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute("SELECT to_regclass('ops.winners_map_serving_runs')")
            if cur.fetchone()[0] is None:
                cur.execute(Path(__file__).parent.joinpath("ops_winners_map_serving_runs.sql").read_text())
            cur.execute(
                "INSERT INTO ops.winners_map_serving_runs (feed, window_days, rows_written, "
                "with_coords, coord_rate, write_mode, indices_built, status, error_message, "
                "started_at, completed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("winners_map_serving", window_days, rows, with_coords, coord_rate, write_mode,
                 ",".join(indices), status, error, started, dt.datetime.now(dt.timezone.utc)))
            c.commit()
    except Exception as exc:  # noqa: BLE001
        log(f"WARN: ops.* write failed: {exc}")


def build(window_days: int = WINDOW_DAYS):
    import lance
    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    status, error, rows, with_coords = "error", None, 0, 0
    try:
        tbl = _assemble(so, window_days)
        rows = tbl.num_rows
        with_coords = tbl.filter(__import__("pyarrow").compute.is_valid(tbl.column("latitude"))).num_rows
        log(f"assembled {rows:,} winners · {with_coords:,} with coords "
            f"({(with_coords / rows * 100 if rows else 0):.1f}%)")
        if rows == 0:
            raise RuntimeError("zero winners assembled")
        lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
        ds = lance.dataset(SERVING_URI, storage_options=so)
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE", replace=True); log(f"  BTREE ✓ {col}")
        for col in BITMAP_INDEXES:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True); log(f"  BITMAP ✓ {col}")
        status = "success"
        log(f"DONE → {SERVING_URI} rows={rows:,}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        log(f"FAILED: {error}")
    finally:
        _record_run(window_days=window_days, rows=rows, with_coords=with_coords,
                    write_mode="overwrite", indices=BTREE_INDEXES + BITMAP_INDEXES,
                    status=status, error=error, started=started)
    if status != "success":
        raise SystemExit(1)
    return {"rows": rows, "with_coords": with_coords}


def verify():
    import lance
    so = _r2_so()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    out = {"uri": SERVING_URI, "rows": ds.count_rows(),
           "with_coords": ds.count_rows(filter="latitude IS NOT NULL"),
           "prime": ds.count_rows(filter="winner_type = 'prime_recipient'"),
           "subaward": ds.count_rows(filter="winner_type = 'subawardee'"),
           # "honest won" $ invariants: no winner may read < 0; report the ≤$0 / <$0 tails.
           "negative_obligation": ds.count_rows(filter="entity_obligated_usd < 0"),
           "zero_or_negative_obligation": ds.count_rows(filter="entity_obligated_usd <= 0"),
           "columns": len(ds.schema.names), "indices": idx}
    out["demo_construction_gt_150k"] = ds.count_rows(
        filter="naics2 = '23' AND entity_obligated_usd >= 150000 AND latitude IS NOT NULL")
    # ── PHASE-3 capability coverage (the map∩scope denominator + the safety-gate sanity) ──
    cap_cols = set(ds.schema.names)
    if "has_extracted_scope" in cap_cols:
        out["has_extracted_scope"] = ds.count_rows(filter="has_extracted_scope = true")
        out["requires_clearance"] = ds.count_rows(filter="requires_clearance = true")
        out["requires_cmmc"] = ds.count_rows(filter="requires_cmmc = true")
        # north-star map predicate (with the has_extracted_scope gate the EXECUTE path injects)
        out["northstar_electrical_secret_plottable"] = ds.count_rows(
            filter="has_extracted_scope = true AND requires_clearance = true "
                   "AND req_clearance_level_max IN ('SECRET','TOP_SECRET','TS_SCI') "
                   "AND array_has(solicitation_scope_tags, 'electrical_systems') AND latitude IS NOT NULL")
        # window-mismatch sanity: how many cleared-capability winners actually plot
        out["capability_winners_plottable"] = ds.count_rows(
            filter="has_extracted_scope = true AND latitude IS NOT NULL")
    # ── SUB-only teaming axis coverage (the /ask teaming queries' denominator) ──
    if "teaming_dollars_5y" in cap_cols:
        out["has_teaming"] = ds.count_rows(filter="n_teaming_primes > 0")
        out["teaming_dollars_5y_ge_10m"] = ds.count_rows(filter="teaming_dollars_5y >= 10000000")
        out["teamed_with_lockheed"] = ds.count_rows(
            filter="array_has(teaming_prime_names, 'LOCKHEED MARTIN CORPORATION')")
    # ── SUB-only SELF-REPORTED axis coverage (the /ask self-report + cert denominators) ──
    if "subaward_description_tags" in cap_cols:
        out["has_self_reported_capability"] = ds.count_rows(
            filter="subaward_description_tags IS NOT NULL")
        out["self_reports_software_development"] = ds.count_rows(
            filter="array_has(subaward_description_tags, 'software_development')")
        out["self_reports_aircraft_maintenance"] = ds.count_rows(
            filter="array_has(subaward_description_tags, 'aircraft_maintenance')")
        out["has_req_cert"] = ds.count_rows(filter="req_cert_tags IS NOT NULL")
        out["req_cert_iso_9001"] = ds.count_rows(filter="array_has(req_cert_tags, 'iso_9001')")
        out["req_cert_as9100"] = ds.count_rows(filter="array_has(req_cert_tags, 'as9100')")
    print(json.dumps(out, indent=2, default=str))


def demo(window_days: int = WINDOW_DAYS):
    """Emit a sample GeoJSON FeatureCollection for the demo filter — proof the dots exist."""
    import lance
    so = _r2_so()
    naics2 = os.environ.get("DEMO_NAICS2", "23")
    min_obl = float(os.environ.get("DEMO_MIN_OBL", "150000"))
    limit = int(os.environ.get("DEMO_LIMIT", "10"))
    ds = lance.dataset(SERVING_URI, storage_options=so)
    flt = f"naics2 = '{naics2}' AND entity_obligated_usd >= {min_obl} AND latitude IS NOT NULL"
    total = ds.count_rows(filter=flt)
    tbl = ds.scanner(columns=["winner_name", "winner_type", "state", "naics_code",
                              "entity_obligated_usd", "award_count", "latitude", "longitude"],
                     filter=flt, limit=limit).to_table().to_pylist()
    fc = {"type": "FeatureCollection",
          "demo_filter": flt, "matched_total": total, "showing": len(tbl),
          "features": [{"type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [r["longitude"], r["latitude"]]},
                        "properties": {k: r[k] for k in ("winner_name", "winner_type", "state",
                                                         "naics_code", "entity_obligated_usd", "award_count")}}
                       for r in tbl]}
    print(json.dumps(fc, indent=2, default=str))


def init_ops():
    import psycopg
    sql = Path(__file__).parent.joinpath("ops_winners_map_serving_runs.sql").read_text()
    with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as c, c.cursor() as cur:
        cur.execute(sql); c.commit()
    log("ops DDL applied")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    arg = int(sys.argv[2]) if len(sys.argv) > 2 else WINDOW_DAYS
    if cmd == "build":
        print(json.dumps(build(window_days=arg), indent=2, default=str))
    elif cmd == "verify":
        verify()
    elif cmd == "demo":
        demo(window_days=arg)
    elif cmd == "init_ops":
        init_ops()
    else:
        print(f"unknown command: {cmd} (init_ops|build|verify|demo)"); sys.exit(2)


if __name__ == "__main__":
    main()
