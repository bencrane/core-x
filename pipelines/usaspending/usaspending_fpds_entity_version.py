"""USAspending FPDS ENTITY VERSION dimension (SCD2 history core) — Cycle 3 (LOCAL CLI).

Implements the recorded decision in docs/reference/FPDS_L2_ENTITY_DIMENSION_BUILD_DECISION.md §8
("history-core-now"): cut the UEI-keyed SCD2 version/history core from the L1 transaction spine
NOW; enrich current-canonical attribute values from award_search / SAM later, additively. Governed
by docs/reference/FPDS_ONTOLOGY_DOCTRINE.md — every derived value adjudicated (§5 ledger pattern).

WHAT IT ANSWERS  what an entity looked like ON ITS FEDERAL CONTRACTS over time, and when the
contracting record changed (novation J, re-rep R/P, vendor change V/W, transfer T — the entity
change-EVENT stream, itself an origination signal: successor-in-interest, size-status flip).

GRAIN / PK  one row per (recipient_uei, version_ordinal) — a version interval
            [valid_from, valid_to); entity_version_key = uei || '_v' || ordinal (synthesized PK).
SoR         s3://data-sink/active/usaspending_fpds_entity_version/  (Lance v2.1; overwrite rebuild)
SCOPE       UEI-era spine rows only (recipient_uei present). Pre-UEI history is out of scope until
            a recipient_hash-keyed extension; coverage metrics (rows_with_uei/distinct_uei) ledgered.

MECHANIC (one uei-partitioned window pass over a 13-col spine projection):
  1. LADDER  per-UEI total order: (action_date, last_modified_date, ctuk) — ctuk is unique, so the
     order is total and rebuild-stable (no wall-clock).
  2. TUPLE   the versioned attribute fingerprint = the mod_delta identity footprint MINUS
     funding_office_code (award-context, not an entity attribute): recipient_hash, recipient_name,
     cage_code, parent_uei, business_categories,
     contracting_officers_determination_of_business_size, recipient_county_name.
     Versioning is on RESOLVED KEYS + COARSE attributes — never raw address strings (§8 anti-noise).
  3. DEBOUNCE single-transaction-reversion smoothing: a row whose tuple differs from BOTH its
     ladder-neighbors while those neighbors agree (A-B-A) is data-entry noise → folded into the
     surrounding run (counted in the ledger as smoothed_txn_count). Multi-txn noise sequences are
     NOT folded (name<=computation: this is exactly single-txn smoothing, no more) — they survive
     as thin versions, identifiable/filterable via txn_count_in_version.
  4. ISLANDS  gaps-and-islands over the smoothed fingerprint → version runs. valid_from =
     min(action_date) of the run (ladder makes this monotone per UEI); valid_to = next version's
     valid_from (NULL on the current version → is_current). Same-day flips yield zero-length
     [d, d) intervals — legitimate at date grain, retained.
  5. ATTRS    per version, each attribute = the LATEST GENUINE observation in the run
     (arg_max by seq, filtered to rows whose ORIGINAL tuple == the run tuple, so a smoothed blip
     row never supplies values).
  6. CHANGE   change_action_type_code = the action_type of the run's FIRST transaction (the record
     that instituted the state). change_klass:
       'initial'                — version_ordinal = 1 (entity first observed; no prior state)
       'new_award_observation'  — boundary observed on a base-award record (absent action_type =
                                  a NEW procurement award per INV-MOD-ABSENCE, not a mod)
       else                     — the L2 _KLASS_CASE mapping of the code (imported, single source
                                  of truth; codes resolve via dec_code_domain_ref)

DISCIPLINES (mirrors usaspending_fpds_l2.py; d.8 / fleet rules):
  • LANCE_BYPASS_SPILLING before any lance import; LOCAL Lance write → boto3 uniform-part publish
    (reuses the spine's _publish_local_to_r2); ONE build_date/built_at literal per build.
  • fail-closed gates BEFORE publish: PK-unique; one is_current per UEI; valid_to >= valid_from;
    ordinal-1 ⟺ 'initial'; change_klass never NULL. NO auto-retries; overwrite idempotency.
  • ops ledger: ops.usaspending_fpds_entity_version_runs (self-bootstrapping DDL).

    # SAMPLE (bounded --since → _sample URI; never prod):
    doppler run -p core-x -c prd -- python3 -m pipelines.usaspending.usaspending_fpds_entity_version \
        build --since 2026-04-15 --uri s3://data-sink/active/_sample/fpds_entity_version_sample/
    # FULL (Modal; see usaspending_fpds_entity_version_modal.py):
    python3 -m pipelines.usaspending.usaspending_fpds_entity_version build | verify | init_ops | print_sql
"""
from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import sys

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

from pipelines.usaspending import usaspending_fpds_canonical as _spine
from pipelines.usaspending.usaspending_fpds_l2 import _KLASS_CASE  # single source of truth

ACTIVE = "s3://data-sink/active"
SPINE_URI = f"{ACTIVE}/usaspending_fpds_canonical_txn/"
ENTITY_URI = f"{ACTIVE}/usaspending_fpds_entity_version/"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3

SCRATCH = os.environ.get("FPDS_ENTITY_SCRATCH", "/tmp/fpds_entity_stage")
DUCK_MEM = os.environ.get("FPDS_ENTITY_DUCKDB_MEM", "8GB")
DUCK_TMP = os.environ.get("FPDS_ENTITY_DUCKDB_TEMP_DIR", "/tmp/fpds_entity_duckdb")
DUCK_THREADS = int(os.environ.get("FPDS_ENTITY_DUCKDB_THREADS", "4"))

FEED = "usaspending_fpds_entity_version"
OPS_SQL_FILE = "ops_usaspending_fpds_entity_version_runs.sql"
OPS_TABLE = "ops.usaspending_fpds_entity_version_runs"

BTREE = ["entity_version_key", "recipient_uei", "recipient_hash", "valid_from", "valid_to"]
BITMAP = ["is_current", "change_klass", "change_action_type_code",
          "contracting_officers_determination_of_business_size"]

# spine source columns (all in the L2-verified SPINE_SCAN_COLS set — no new schema risk)
SPINE_SCAN_COLS = [
    "recipient_uei", "contract_award_unique_key", "contract_transaction_unique_key",
    "action_date", "last_modified_date", "action_type_code",
    "recipient_hash", "recipient_name", "cage_code", "parent_uei", "business_categories",
    "contracting_officers_determination_of_business_size", "recipient_county_name",
]

_TUPLE_COLS = ["recipient_hash", "recipient_name", "cage_code", "parent_uei",
               "business_categories", "co_det", "recipient_county_name"]
_FP = "md5(concat_ws('|'," + ",".join(f"coalesce({c},'∅')" for c in _TUPLE_COLS) + "))"


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _duck():
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads={DUCK_THREADS}")
    os.makedirs(DUCK_TMP, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{DUCK_TMP}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _stage_sql(build_date_iso: str, built_at_iso: str) -> str:
    return f"""
-- 13-col projection; UEI-era scope
CREATE TEMP TABLE ent_proj AS
SELECT
    recipient_uei                                        AS uei,
    contract_award_unique_key                            AS cauk,
    contract_transaction_unique_key                      AS ctuk,
    action_date, last_modified_date, action_type_code,
    recipient_hash, recipient_name, cage_code, parent_uei, business_categories,
    contracting_officers_determination_of_business_size  AS co_det,
    recipient_county_name
FROM spine_src
WHERE recipient_uei IS NOT NULL AND recipient_uei <> '';

-- ladder + fingerprint + neighbors (ONE uei-partitioned sort)
CREATE TEMP TABLE tl AS
SELECT *,
    {_FP}                    AS fp0,
    ROW_NUMBER()   OVER lad  AS seq,
    LAG({_FP})     OVER lad  AS p_fp,
    LEAD({_FP})    OVER lad  AS n_fp
FROM ent_proj
WINDOW lad AS (PARTITION BY uei ORDER BY action_date, last_modified_date, ctuk);

-- single-transaction-reversion smoothing (A-B-A -> A-A-A); smoothed rows never supply attributes
CREATE TEMP TABLE sm AS
SELECT *,
    CASE WHEN p_fp IS NOT NULL AND n_fp IS NOT NULL AND fp0 <> p_fp AND n_fp = p_fp
         THEN p_fp ELSE fp0 END                                        AS fp,
    (p_fp IS NOT NULL AND n_fp IS NOT NULL AND fp0 <> p_fp AND n_fp = p_fp) AS smoothed
FROM tl;

-- gaps-and-islands over the smoothed fingerprint -> version ordinal
CREATE TEMP TABLE isl AS
SELECT *, SUM(CASE WHEN fp IS DISTINCT FROM p_fp2 THEN 1 ELSE 0 END)
              OVER (PARTITION BY uei ORDER BY seq ROWS UNBOUNDED PRECEDING) AS ver
FROM (SELECT *, LAG(fp) OVER (PARTITION BY uei ORDER BY seq) AS p_fp2 FROM sm);

-- version aggregate: attrs = latest GENUINE observation; change code = FIRST txn of the run
CREATE TEMP TABLE vers AS
SELECT uei, ver,
    MIN(action_date)                                    AS valid_from,
    MAX(action_date)                                    AS last_action_date_in_version,
    arg_min(coalesce(action_type_code,''), seq)          AS change_action_type_code_raw,
    arg_min(cauk, seq)                                   AS first_seen_award_key,
    COUNT(*)                                            AS txn_count_in_version,
    COUNT(DISTINCT cauk)                                AS n_awards_in_version,
    SUM(CASE WHEN smoothed THEN 1 ELSE 0 END)           AS smoothed_txn_count,
    arg_max(recipient_hash, seq)        FILTER (WHERE fp0 = fp) AS recipient_hash,
    arg_max(recipient_name, seq)        FILTER (WHERE fp0 = fp) AS recipient_name,
    arg_max(cage_code, seq)             FILTER (WHERE fp0 = fp) AS cage_code,
    arg_max(parent_uei, seq)            FILTER (WHERE fp0 = fp) AS parent_uei,
    arg_max(business_categories, seq)   FILTER (WHERE fp0 = fp) AS business_categories,
    arg_max(co_det, seq)                FILTER (WHERE fp0 = fp) AS co_det,
    arg_max(recipient_county_name, seq) FILTER (WHERE fp0 = fp) AS recipient_county_name
FROM isl GROUP BY uei, ver;

-- final: interval close-off + change classification (klass mapping imported from L2, verbatim)
CREATE TEMP TABLE entity_out AS
SELECT
    uei || '_v' || ver                                   AS entity_version_key,
    uei                                                  AS recipient_uei,
    ver                                                  AS version_ordinal,
    valid_from,
    LEAD(valid_from) OVER (PARTITION BY uei ORDER BY ver) AS valid_to,
    (LEAD(valid_from) OVER (PARTITION BY uei ORDER BY ver) IS NULL) AS is_current,
    recipient_hash, recipient_name, cage_code, parent_uei, business_categories,
    co_det                                               AS contracting_officers_determination_of_business_size,
    recipient_county_name,
    NULLIF(change_action_type_code_raw, '')              AS change_action_type_code,
    CASE WHEN ver = 1 THEN 'initial'
         WHEN change_action_type_code_raw = '' THEN 'new_award_observation'
         ELSE ({_KLASS_CASE.replace("action_type_code", "change_action_type_code_raw")})
    END                                                  AS change_klass,
    first_seen_award_key, n_awards_in_version, txn_count_in_version, smoothed_txn_count,
    last_action_date_in_version,
    DATE '{build_date_iso}'                              AS build_date,
    TIMESTAMP '{built_at_iso}'                           AS built_at
FROM vers;
"""


def _build_indices_local(local_ds: str) -> list[str]:
    import lance
    ds = lance.dataset(local_ds)
    present = set(ds.schema.names)
    built: list[str] = []
    log(f"indexing LOCAL ({ds.count_rows():,} rows)")
    for col in [c for c in BTREE if c in present]:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        built.append(col); log(f"  BTREE ✓ {col}")
    for col in [c for c in BITMAP if c in present]:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        built.append(col); log(f"  BITMAP ✓ {col}")
    return built


def init_ops() -> None:
    import psycopg
    from pathlib import Path
    sql = Path(__file__).parent.joinpath(OPS_SQL_FILE).read_text()
    with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as conn, conn.cursor() as cur:
        cur.execute(sql); conn.commit()
    log("entity-version ops DDL applied")


def _record_run(cols: dict) -> None:
    import psycopg
    from pathlib import Path
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write."); return
    if cols.get("status") != "success" and not cols.get("error_message"):
        cols["error_message"] = "unknown terminal failure (no exception captured)"
    cols = {"feed": FEED, **cols}
    names = list(cols.keys())
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT to_regclass('{OPS_TABLE}')")
            if cur.fetchone()[0] is None:
                cur.execute(Path(__file__).parent.joinpath(OPS_SQL_FILE).read_text())
            cur.execute(f"INSERT INTO {OPS_TABLE} ({','.join(names)}) VALUES ({','.join(['%s']*len(names))})",
                        [cols[n] for n in names])
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        log(f"WARN: ops.* write failed: {exc}")


def build(since: str | None = None, uri: str = ENTITY_URI, build_date: str | None = None) -> dict:
    import ctypes
    import gc as _gc

    import lance

    started = dt.datetime.now(dt.timezone.utc)
    built_at_iso = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")
    build_date_iso = build_date or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    so = _spine._r2_so()

    status, error = "error", None
    metrics: dict = {}
    con = None
    local_ds = os.path.join(SCRATCH, "entity_lance")
    try:
        os.makedirs(SCRATCH, exist_ok=True)
        spine_ds = lance.dataset(SPINE_URI, storage_options=so)
        spine_manifest_version = int(spine_ds.version)
        present = set(spine_ds.schema.names)
        missing = [c for c in SPINE_SCAN_COLS if c not in present]
        if missing:
            raise RuntimeError(f"spine is missing entity source columns: {missing}")
        spine_filter = f"action_date >= DATE '{since}'" if since else None

        con = _duck()
        log(f"registering spine (since={since}, manifest v{spine_manifest_version}) → {uri}")
        con.register("spine_src", spine_ds.scanner(columns=SPINE_SCAN_COLS, filter=spine_filter).to_reader())
        rows_in_spine = None  # scanner is single-pass; counts derived below

        con.execute(_stage_sql(build_date_iso, built_at_iso))
        rows_with_uei = con.execute("SELECT count(*) FROM isl").fetchone()[0]
        distinct_uei = con.execute("SELECT count(DISTINCT uei) FROM isl").fetchone()[0]

        # ── fail-closed gates BEFORE publish ──
        total, pk_distinct = con.execute(
            "SELECT count(*), count(DISTINCT entity_version_key) FROM entity_out").fetchone()
        if total != pk_distinct:
            raise RuntimeError(f"PK gate FAILED: {total:,} rows != {pk_distinct:,} distinct keys")
        cur_ct = con.execute("SELECT count(*) FROM entity_out WHERE is_current").fetchone()[0]
        if cur_ct != distinct_uei:
            raise RuntimeError(f"is_current gate FAILED: {cur_ct:,} current != {distinct_uei:,} UEIs")
        bad_iv = con.execute(
            "SELECT count(*) FROM entity_out WHERE valid_to IS NOT NULL AND valid_to < valid_from").fetchone()[0]
        if bad_iv:
            raise RuntimeError(f"interval gate FAILED: {bad_iv:,} rows valid_to < valid_from")
        bad_init = con.execute(
            "SELECT count(*) FROM entity_out WHERE (version_ordinal=1) <> (change_klass='initial')").fetchone()[0]
        if bad_init:
            raise RuntimeError(f"initial gate FAILED: {bad_init:,} ordinal-1/'initial' mismatches")
        null_klass = con.execute("SELECT count(*) FROM entity_out WHERE change_klass IS NULL").fetchone()[0]
        if null_klass:
            raise RuntimeError(f"klass gate FAILED: {null_klass:,} NULL change_klass rows")

        klass_counts = dict(con.execute(
            "SELECT change_klass, count(*) FROM entity_out GROUP BY 1").fetchall())
        smoothed_total = con.execute("SELECT COALESCE(SUM(smoothed_txn_count),0) FROM entity_out").fetchone()[0]
        multi_ver = con.execute(
            "SELECT count(*) FROM (SELECT recipient_uei FROM entity_out GROUP BY 1 HAVING count(*)>1)").fetchone()[0]
        max_ver = con.execute("SELECT max(version_ordinal) FROM entity_out").fetchone()[0]
        novation_ct = con.execute(
            "SELECT count(*) FROM entity_out WHERE change_action_type_code='J'").fetchone()[0]
        log(f"gates PASSED — versions={total:,} ueis={distinct_uei:,} klass={klass_counts} "
            f"smoothed={smoothed_total:,} multi_ver_entities={multi_ver:,}")

        # local data-only write while con is open, then free DuckDB before the in-RAM index sorts
        shutil.rmtree(local_ds, ignore_errors=True)
        reader = con.sql("SELECT * FROM entity_out").to_arrow_reader(batch_size=200_000)
        log(f"writing Lance LOCALLY → {local_ds}")
        lance.write_dataset(reader, local_ds, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE)
        del reader
        con.close(); con = None
        del spine_ds
        _gc.collect()
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except OSError:
            pass
        shutil.rmtree(DUCK_TMP, ignore_errors=True)

        built = _build_indices_local(local_ds)
        s3 = _spine._s3()
        log(f"publishing local Lance (data + indices) → {uri} (boto3 uniform-part)…")
        published = _spine._publish_local_to_r2(s3, uri, local_ds)
        log(f"published {published} files → {uri}")

        status = "success"
        metrics = {
            "spine_manifest_version": spine_manifest_version, "build_date": build_date_iso,
            "rows_with_uei": int(rows_with_uei), "distinct_uei": int(distinct_uei),
            "versions_out": int(total),
            "initial_version_count": int(klass_counts.get("initial", 0)),
            "new_award_observation_count": int(klass_counts.get("new_award_observation", 0)),
            "novation_boundary_count": int(novation_ct),
            "smoothed_txn_count": int(smoothed_total),
            "multi_version_entity_count": int(multi_ver),
            "max_versions_per_entity": int(max_ver or 0),
            "files_published": int(published), "indices_built": built, "status": status}
        log(f"DONE → versions={total:,}")
        return {**metrics, "uri": uri, "since": since}
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        log(f"FAILED: {error}")
        raise
    finally:
        if con is not None:
            con.close()
        _record_run({
            **{k: metrics.get(k) for k in (
                "spine_manifest_version", "build_date", "rows_with_uei", "distinct_uei",
                "versions_out", "initial_version_count", "new_award_observation_count",
                "novation_boundary_count", "smoothed_txn_count", "multi_version_entity_count",
                "max_versions_per_entity")},
            "write_mode": "overwrite",
            "indices_built": ",".join(metrics.get("indices_built") or []) or None,
            "status": status, "error_message": None if status == "success" else error,
            "started_at": started, "completed_at": dt.datetime.now(dt.timezone.utc)})
        shutil.rmtree(SCRATCH, ignore_errors=True)


def verify(uri: str = ENTITY_URI) -> dict:
    """Independent scanner → DuckDB read-back. Gates: PK-unique; one is_current per UEI;
    interval sanity; ordinal-1 ⟺ 'initial'; klass domain ⊆ L2 klass ∪ {initial, new_award_observation};
    single built_at literal."""
    import lance
    so = _spine._r2_so()
    ds = lance.dataset(uri, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    con = _duck()
    con.register("c_src", ds.scanner().to_reader())
    con.execute("CREATE TEMP TABLE c AS SELECT * FROM c_src")
    failures: list[str] = []
    rows = con.execute("SELECT count(*) FROM c").fetchone()[0]
    total, distinct = con.execute(
        "SELECT count(*), count(DISTINCT entity_version_key) FROM c").fetchone()
    if total != distinct:
        failures.append(f"PK not unique: {total:,} rows, {distinct:,} distinct keys")
    ueis, cur = con.execute(
        "SELECT count(DISTINCT recipient_uei), count(*) FILTER (WHERE is_current) FROM c").fetchone()
    if ueis != cur:
        failures.append(f"is_current violation: {cur:,} current vs {ueis:,} UEIs")
    bad_iv = con.execute(
        "SELECT count(*) FROM c WHERE valid_to IS NOT NULL AND valid_to < valid_from").fetchone()[0]
    if bad_iv:
        failures.append(f"{bad_iv:,} rows valid_to < valid_from")
    allowed = {"option_exercise", "scope_change", "funding_only", "termination",
               "identity_boundary", "admin", "unclassified", "initial", "new_award_observation"}
    klass = dict(con.execute("SELECT change_klass, count(*) FROM c GROUP BY 1").fetchall())
    if set(klass) - allowed:
        failures.append(f"change_klass domain leak: {set(klass) - allowed}")
    built_at_distinct = con.execute("SELECT count(DISTINCT built_at) FROM c").fetchone()[0]
    if rows and built_at_distinct != 1:
        failures.append(f"built_at not a single literal: {built_at_distinct} distinct")
    con.close()
    return {"uri": uri, "rows": rows, "distinct_uei": ueis, "indices": idx,
            "pass": not failures, "failures": failures, "change_klass": klass}


def _arg_val(flag: str, argv: list[str], default: str | None = None) -> str | None:
    if flag in argv:
        i = argv.index(flag)
        if i + 1 < len(argv):
            return argv[i + 1]
    return default


def main():
    argv = sys.argv[1:]
    cmd = argv[0] if argv else "verify"
    uri = _arg_val("--uri", argv, ENTITY_URI)
    if cmd == "init_ops":
        init_ops()
    elif cmd == "build":
        print(json.dumps(build(since=_arg_val("--since", argv), uri=uri,
                               build_date=_arg_val("--build-date", argv)), indent=2, default=str))
    elif cmd == "verify":
        print(json.dumps(verify(uri=uri), indent=2, default=str))
    elif cmd == "print_sql":
        print(_stage_sql(_arg_val("--build-date", argv, "2026-07-04"), "2026-07-04 00:00:00.000000"))
    else:
        print(f"unknown command: {cmd} (init_ops|build|verify|print_sql)")
        sys.exit(2)


if __name__ == "__main__":
    main()
