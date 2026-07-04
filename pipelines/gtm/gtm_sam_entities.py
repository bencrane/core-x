"""gtm_sam_entities — GTM SAM audience mart entity spine (1 row / uei).

Strategy: docs/plans/GTM_SAM_AUDIENCE_MART_STRATEGY.md (v2, aligned).

Universe = union of UEI-bearing sources (strictly UEI-keyed; no synthetic keys):
    sam_master_entities  ∪  sba_dsbs_certified_firms  ∪
    distinct usaspending_subaward_canonical.subawardee_uei  ∪
    distinct usaspending_fpds_prime_award_state.recipient_uei  ∪
    distinct ffata_exec_comp.recipient_uei   (assistance-only recipients can be
    absent from the FPDS prime set; including them guarantees every people-layer
    uei resolves — referential integrity of the mart is a build gate downstream).

THIN SPINE — identity, presence flags, classification, domain. No award rollups,
no firmographics, no contact data, no DSBS cert detail (join those Lances on uei
at query time). SAM public v2 carries no parent hierarchy fields (verified live
2026-07-04) — hierarchy stays in resolution/entity_hierarchy, joined on demand.

Domain: sam_master_domains.normalized_domain (from entity_url, blocklisted) with
crosswalk_dsbs_sam.best_domain as the DSBS fallback; source recorded per row.

Rebuild: full snapshot, Lance overwrite (new version), prior versions retained.
Every input's URI + Lance version + row count at read time goes to the ledger
(live-probe rule — documented counts are never trusted).

Run:
    doppler run -- python3 pipelines/gtm/gtm_sam_entities.py            # local build
    modal run pipelines/gtm/gtm_sam_entities.py                         # remote build
    modal run pipelines/gtm/gtm_sam_entities.py::init_ops               # create ops table
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid

BUCKET = "data-sink"
_PROD_URI = "s3://data-sink/active/gtm_sam_entities/"
DATASET_URI = os.environ.get("GTM_SAM_ENTITIES_LANCE_URI", _PROD_URI)

SRC = {
    "sam_master_entities": "s3://data-sink/active/sam_master_entities/",
    "sam_normalized_entities": "s3://data-sink/active/sam_normalized_entities/",
    "sam_master_domains": "s3://data-sink/active/sam_master_domains/",
    "sba_dsbs_certified_firms": "s3://data-sink/active/sba_dsbs_certified_firms/",
    "crosswalk_dsbs_sam": "s3://data-sink/active/crosswalk_dsbs_sam/",
    "usaspending_subaward_canonical": "s3://data-sink/active/usaspending_subaward_canonical/",
    "usaspending_fpds_prime_award_state": "s3://data-sink/active/usaspending_fpds_prime_award_state/",
    "ffata_exec_comp": "s3://data-sink/active/ffata_exec_comp/",
}

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

BTREE_INDEXES = ["uei", "normalized_domain", "primary_naics", "cage_code"]
BITMAP_INDEXES = ["in_sam", "sam_is_active", "in_dsbs", "is_subawardee",
                  "is_prime_recipient", "physical_state", "domain_source"]

DUCKDB_MEMORY_LIMIT = os.environ.get("GTM_DUCKDB_MEM", "16GB")
DUCKDB_THREADS = 8
SPILL_DIR = os.environ.get("GTM_DUCKDB_SPILL", "/tmp/duckdb_spill")

# ── gate constants (floors from live probe 2026-07-04:
#    sam 1,541,566 · dsbs 67,234 · sub uei 105,189 · prime uei 766,803) ──
ROW_FLOOR = 1_700_000            # union floor — catastrophic-collapse catcher
IN_SAM_FLOOR = 1_400_000
NAME_FILL_MIN = 0.97             # legal_business_name coalesced across 4 sources
DOMAIN_FILL_FLOOR = 650_000      # sam_master_domains live ≈ 709k
DELTA_GUARD = 0.25
BASELINE_MIN_ROWS = 1_700_000

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.gtm_sam_entities_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,
    dataset_uri         text        NOT NULL,
    build_id            text,
    rows_written        bigint,
    distinct_uei        bigint,
    n_in_sam            bigint,
    n_in_dsbs           bigint,
    n_subawardee        bigint,
    n_prime             bigint,
    n_ffata_only        bigint,
    name_fill_frac      double precision,
    domain_fill         bigint,
    inputs              jsonb,
    status              text        NOT NULL,
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS gtm_sam_entities_runs_feed_idx
    ON ops.gtm_sam_entities_runs (feed);
CREATE INDEX IF NOT EXISTS gtm_sam_entities_runs_recorded_at_idx
    ON ops.gtm_sam_entities_runs (recorded_at DESC);
"""


def _feed_for(uri: str) -> str:
    return "gtm_sam_entities" if uri == _PROD_URI else "gtm_sam_entities_scratch"


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
    except Exception:  # noqa: BLE001 — alerting must never mask the build
        print(f"ALERT (no channel): {msg}")


def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _as_batched_reader(table, max_chunksize: int = 131072):
    """Bound arrow chunk sizes fed to the Lance encoder. A monolithic table with
    wide string/list columns can exceed lance-encoding's max_chunk_size assertion
    (rust panic at primitive.rs chunk_bytes check); 128k-row batches keep every
    chunk safely under it."""
    import pyarrow as pa

    return pa.RecordBatchReader.from_batches(
        table.schema, table.to_batches(max_chunksize=max_chunksize))


def _open(so: dict, lineage: list[dict], name: str):
    """Open a source dataset, recording URI + Lance version + live row count."""
    import lance

    ds = lance.dataset(SRC[name], storage_options=so)
    lineage.append({"name": name, "uri": SRC[name], "version": ds.version,
                    "rows_at_read": ds.count_rows()})
    return ds


# --------------------------------------------------------------------------- #
# materialize (pure DuckDB over registered scanners)
# --------------------------------------------------------------------------- #
def _materialize(con, so: dict, build_id: str, built_at: dt.datetime):
    lineage: list[dict] = []

    sm = _open(so, lineage, "sam_master_entities")
    con.register("sm", sm.scanner(columns=[
        "uei", "cage_code", "legal_business_name", "dba_name", "is_active",
        "purpose_of_registration", "initial_registration_date",
        "registration_expiration_date", "exclusion_status_flag", "ever_inactive",
        "primary_naics", "naics_codes", "psc_codes", "business_types",
        "sba_business_type_codes", "physical_address_city",
        "physical_address_province_or_state", "physical_address_zip_postal_code",
        "sam_extract_label"]).to_reader())
    con.execute("CREATE TEMP TABLE t_sam AS SELECT * FROM sm")
    con.unregister("sm")

    sn = _open(so, lineage, "sam_normalized_entities")
    con.register("sn", sn.scanner(
        columns=["uei", "normalized_legal_name", "legal_name_base"]).to_reader())
    con.execute("CREATE TEMP TABLE t_norm AS SELECT * FROM sn")
    con.unregister("sn")

    sd = _open(so, lineage, "sam_master_domains")
    con.register("sd", sd.scanner(columns=["uei", "normalized_domain"]).to_reader())
    con.execute("""CREATE TEMP TABLE t_dom AS
        SELECT uei, min(normalized_domain) AS normalized_domain
        FROM sd WHERE uei IS NOT NULL GROUP BY 1""")
    con.unregister("sd")

    df = _open(so, lineage, "sba_dsbs_certified_firms")
    con.register("df", df.scanner(columns=["uei", "legal_business_name"]).to_reader())
    con.execute("""CREATE TEMP TABLE t_dsbs AS
        SELECT uei, any_value(legal_business_name) AS dsbs_name
        FROM df WHERE uei IS NOT NULL GROUP BY 1""")
    con.unregister("df")

    cw = _open(so, lineage, "crosswalk_dsbs_sam")
    con.register("cw", cw.scanner(columns=["uei", "best_domain"]).to_reader())
    con.execute("""CREATE TEMP TABLE t_cwd AS
        SELECT uei, min(best_domain) AS best_domain
        FROM cw WHERE uei IS NOT NULL AND best_domain IS NOT NULL GROUP BY 1""")
    con.unregister("cw")

    sb = _open(so, lineage, "usaspending_subaward_canonical")
    con.register("sb", sb.scanner(columns=[
        "subawardee_uei", "subawardee_name", "subaward_action_date"]).to_reader())
    con.execute("""CREATE TEMP TABLE t_sub AS
        SELECT subawardee_uei AS uei,
               max_by(subawardee_name, subaward_action_date) AS sub_name
        FROM sb WHERE subawardee_uei IS NOT NULL GROUP BY 1""")
    con.unregister("sb")

    pa_ = _open(so, lineage, "usaspending_fpds_prime_award_state")
    con.register("pa", pa_.scanner(columns=[
        "recipient_uei", "recipient_name", "last_action_date"]).to_reader())
    con.execute("""CREATE TEMP TABLE t_prime AS
        SELECT recipient_uei AS uei,
               max_by(recipient_name, last_action_date) AS prime_name
        FROM pa WHERE recipient_uei IS NOT NULL GROUP BY 1""")
    con.unregister("pa")

    fx = _open(so, lineage, "ffata_exec_comp")
    con.register("fx", fx.scanner(columns=["recipient_uei"]).to_reader())
    con.execute("""CREATE TEMP TABLE t_ffata AS
        SELECT DISTINCT recipient_uei AS uei FROM fx WHERE recipient_uei IS NOT NULL""")
    con.unregister("fx")

    con.execute(f"""
    CREATE TEMP TABLE spine AS
    WITH universe AS (
        SELECT uei FROM t_sam
        UNION SELECT uei FROM t_dsbs
        UNION SELECT uei FROM t_sub
        UNION SELECT uei FROM t_prime
        UNION SELECT uei FROM t_ffata
    )
    SELECT
        u.uei,
        s.cage_code,
        coalesce(s.legal_business_name, d.dsbs_name, sb.sub_name, p.prime_name)
            AS legal_business_name,
        n.normalized_legal_name,
        n.legal_name_base,
        s.dba_name,
        (s.uei IS NOT NULL)                       AS in_sam,
        coalesce(s.is_active, FALSE)              AS sam_is_active,
        (d.uei IS NOT NULL)                       AS in_dsbs,
        (sb.uei IS NOT NULL)                      AS is_subawardee,
        (p.uei IS NOT NULL)                       AS is_prime_recipient,
        s.purpose_of_registration,
        s.initial_registration_date,
        s.registration_expiration_date,
        s.exclusion_status_flag,
        s.ever_inactive,
        s.primary_naics,
        s.naics_codes,
        s.psc_codes,
        s.business_types,
        s.sba_business_type_codes,
        s.physical_address_city                   AS physical_city,
        s.physical_address_province_or_state      AS physical_state,
        s.physical_address_zip_postal_code        AS physical_zip,
        coalesce(dm.normalized_domain, c.best_domain) AS normalized_domain,
        CASE WHEN dm.normalized_domain IS NOT NULL THEN 'sam_entity_url'
             WHEN c.best_domain IS NOT NULL       THEN 'dsbs_best_domain'
        END                                       AS domain_source,
        s.sam_extract_label,
        '{build_id}'                              AS build_id,
        TIMESTAMP '{built_at:%Y-%m-%d %H:%M:%S}'  AS built_at
    FROM universe u
    LEFT JOIN t_sam   s  USING (uei)
    LEFT JOIN t_norm  n  USING (uei)
    LEFT JOIN t_dom   dm USING (uei)
    LEFT JOIN t_dsbs  d  USING (uei)
    LEFT JOIN t_cwd   c  USING (uei)
    LEFT JOIN t_sub   sb USING (uei)
    LEFT JOIN t_prime p  USING (uei)
    """)

    (rows, d_uei, n_sam, n_dsbs, n_sub, n_prime, n_ffata_only,
     name_fill, dom_fill) = con.execute("""
        SELECT count(*),
               count(DISTINCT uei),
               count(*) FILTER (WHERE in_sam),
               count(*) FILTER (WHERE in_dsbs),
               count(*) FILTER (WHERE is_subawardee),
               count(*) FILTER (WHERE is_prime_recipient),
               count(*) FILTER (WHERE NOT in_sam AND NOT in_dsbs
                                AND NOT is_subawardee AND NOT is_prime_recipient),
               count(legal_business_name),
               count(normalized_domain)
        FROM spine
    """).fetchone()
    probe_uei = con.execute("""
        SELECT uei FROM spine WHERE in_sam AND normalized_domain IS NOT NULL
        ORDER BY uei LIMIT 1""").fetchone()[0]
    table = con.sql("SELECT * FROM spine").to_arrow_table()
    metrics = {
        "rows": int(rows), "distinct_uei": int(d_uei), "n_in_sam": int(n_sam),
        "n_in_dsbs": int(n_dsbs), "n_subawardee": int(n_sub), "n_prime": int(n_prime),
        "n_ffata_only": int(n_ffata_only),
        "name_fill_frac": (name_fill / rows) if rows else 0.0,
        "domain_fill": int(dom_fill), "probe_uei": probe_uei,
    }
    return table, metrics, lineage


# --------------------------------------------------------------------------- #
# gates
# --------------------------------------------------------------------------- #
def _within(value: int, target: int, tol: float) -> bool:
    return abs(value - target) <= target * tol


def assert_pre_write_gates(metrics: dict, lineage: list[dict],
                           baseline: dict | None) -> list[str]:
    checks: list[str] = []

    def gate(ok: bool, label: str) -> None:
        checks.append(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            raise RuntimeError(f"PRE-WRITE GATE FAILED → {label}\n" + "\n".join(checks))

    by_name = {e["name"]: e for e in lineage}
    rows = metrics["rows"]
    gate(rows >= ROW_FLOOR, f"1 row floor: {rows:,} >= {ROW_FLOOR:,}")
    gate(metrics["distinct_uei"] == rows,
         f"2 grain 1/uei: distinct {metrics['distinct_uei']:,} == rows {rows:,}")
    gate(metrics["n_in_sam"] == by_name["sam_master_entities"]["rows_at_read"],
         f"3 in_sam == sam input rows: {metrics['n_in_sam']:,}")
    gate(metrics["n_in_sam"] >= IN_SAM_FLOOR,
         f"4 in_sam floor: {metrics['n_in_sam']:,} >= {IN_SAM_FLOOR:,}")
    gate(metrics["n_in_dsbs"] == by_name["sba_dsbs_certified_firms"]["rows_at_read"],
         f"5 in_dsbs == dsbs input rows: {metrics['n_in_dsbs']:,}")
    gate(metrics["name_fill_frac"] >= NAME_FILL_MIN,
         f"6 name fill: {metrics['name_fill_frac']:.4%} >= {NAME_FILL_MIN:.0%}")
    gate(metrics["domain_fill"] >= DOMAIN_FILL_FLOOR,
         f"7 domain fill: {metrics['domain_fill']:,} >= {DOMAIN_FILL_FLOOR:,}")
    if baseline:
        gate(_within(rows, baseline["rows_written"], DELTA_GUARD),
             f"8 rows Δ: {rows:,} ~ ±{DELTA_GUARD:.0%} of {baseline['rows_written']:,}")
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


def _prior_success_baseline(dataset_uri: str) -> dict | None:
    conn = _pg_connect()
    if conn is None:
        return None
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "SELECT rows_written FROM ops.gtm_sam_entities_runs "
                "WHERE status='success' AND dataset_uri=%s AND rows_written >= %s "
                "ORDER BY recorded_at DESC LIMIT 1",
                (dataset_uri, BASELINE_MIN_ROWS))
            r = cur.fetchone()
            return None if not r else {"rows_written": int(r[0])}
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: baseline lookup failed: {exc}")
        return None
    finally:
        conn.close()


def _record_run(*, feed, dataset_uri, build_id, metrics, lineage, status, error,
                started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.gtm_sam_entities_runs
                    (feed, dataset_uri, build_id, rows_written, distinct_uei,
                     n_in_sam, n_in_dsbs, n_subawardee, n_prime, n_ffata_only,
                     name_fill_frac, domain_fill, inputs, status, error,
                     started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (feed, dataset_uri, build_id, metrics.get("rows"),
                 metrics.get("distinct_uei"), metrics.get("n_in_sam"),
                 metrics.get("n_in_dsbs"), metrics.get("n_subawardee"),
                 metrics.get("n_prime"), metrics.get("n_ffata_only"),
                 metrics.get("name_fill_frac"), metrics.get("domain_fill"),
                 json.dumps(lineage), status, error, started_at, completed_at))
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* write failed: {exc}")
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# core build (plain python — runnable locally or under Modal)
# --------------------------------------------------------------------------- #
def run_build(dataset_uri: str | None = None) -> dict:
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    uri = dataset_uri or DATASET_URI
    feed = _feed_for(uri)
    build_id = f"{started_at:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    status, error = "error", None
    metrics: dict = {}
    lineage: list[dict] = []
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics, lineage = _materialize(con, so, build_id, started_at)
        finally:
            con.close()
        baseline = _prior_success_baseline(uri)
        print(f"target={uri} feed={feed} build_id={build_id}")
        print(f"materialized: { {k: v for k, v in metrics.items()} }")
        for e in lineage:
            print(f"  input {e['name']} v{e['version']} rows={e['rows_at_read']:,}")

        for line in assert_pre_write_gates(metrics, lineage, baseline):
            print("  ", line)

        try:
            v_before = lance.dataset(uri, storage_options=so).version
        except Exception:  # noqa: BLE001
            v_before = None
        print(f"v_before = {v_before}")

        try:
            lance.write_dataset(
                _as_batched_reader(table), uri, mode="overwrite",
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=MAX_ROWS_PER_FILE,
                max_bytes_per_file=MAX_BYTES_PER_FILE,
                storage_options=so)
            print(f"Wrote Lance dataset (overwrite) → {uri}")
            ds = lance.dataset(uri, storage_options=so)
            for col in BTREE_INDEXES:
                ds.create_scalar_index(col, index_type="BTREE")
                print(f"  BTREE ✓ {col}")
            for col in BITMAP_INDEXES:
                ds.create_scalar_index(col, index_type="BITMAP")
                print(f"  BITMAP ✓ {col}")

            ds = lance.dataset(uri, storage_options=so)
            committed = ds.count_rows()
            if committed != metrics["rows"]:
                raise RuntimeError(
                    f"gate 9 write-integrity: {committed:,} != {metrics['rows']:,}")
            idx = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                   for i in ds.list_indices()}
            expect = {f"{c}_idx" for c in BTREE_INDEXES + BITMAP_INDEXES}
            if not expect.issubset(idx):
                raise RuntimeError(f"gate 10 indices: missing {sorted(expect - idx)}")
            pr = metrics["probe_uei"]
            rt = ds.scanner(columns=["uei", "in_sam", "normalized_domain"],
                            filter=f"uei = '{pr}'").to_table().num_rows
            if rt != 1:
                raise RuntimeError(f"gate 11 round-trip: probe {pr} → {rt} rows")
            print(f"post-write gates PASS — committed={committed:,} probe={pr}")
        except Exception as werr:  # noqa: BLE001
            if v_before is not None:
                try:
                    lance.dataset(uri, storage_options=so, version=v_before).restore()
                except Exception as rerr:  # noqa: BLE001
                    raise RuntimeError(
                        f"ROLLBACK FAILED to v{v_before}: {rerr}; original: {werr}")
                raise RuntimeError(f"write/index/gate failed → rolled back: {werr}")
            raise RuntimeError(f"failed on net-new dataset (inspect/drop {uri}): {werr}")

        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(feed=feed, dataset_uri=uri, build_id=build_id, metrics=metrics,
                    lineage=lineage, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        if status != "success":
            _alert(f"[gtm_sam_entities] {feed} build {status}: {str(error)[:300]}")

    if status != "success":
        raise RuntimeError(f"gtm_sam_entities build failed: {error}")
    return {"feed": feed, "dataset": uri, "build_id": build_id, **{
        k: metrics[k] for k in ("rows", "distinct_uei", "n_in_sam", "n_in_dsbs",
                                "n_subawardee", "n_prime", "n_ffata_only")}}


# --------------------------------------------------------------------------- #
# Modal wrapper (optional — the core build is plain python)
# --------------------------------------------------------------------------- #
try:
    import modal

    image = modal.Image.debian_slim(python_version="3.12").pip_install(
        "duckdb>=1.5,<2", "pylance>=8", "pyarrow>=17",
        "psycopg[binary]>=3.2",
    ).env({"LANCE_BYPASS_SPILLING": "true"})
    app = modal.App("gtm-sam-entities", image=image)

    @app.function(
        secrets=[modal.Secret.from_name("r2-credentials"),
                 modal.Secret.from_name("hqx-postgres")],
        timeout=60 * 60, memory=32768, cpu=8.0)
    def build_gtm_sam_entities(dataset_uri: str | None = None) -> dict:
        return run_build(dataset_uri)

    @app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=300)
    def init_ops() -> None:
        conn = _pg_connect()
        if conn is not None:
            with conn, conn.cursor() as cur:
                cur.execute(OPS_DDL)
            conn.close()
            print("ops.gtm_sam_entities_runs ready")

    @app.local_entrypoint()
    def main(dataset_uri: str = ""):
        print(build_gtm_sam_entities.remote(dataset_uri or None))
except ImportError:  # local run without modal installed
    modal = None


if __name__ == "__main__":
    print(run_build())
