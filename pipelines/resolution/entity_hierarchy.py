"""Compute worker — FEDERAL ENTITY HIERARCHY (uei → immediate_parent → ultimate_parent).

A standalone corporate-family spine keyed on the federal UEI. It resolves the
parent/subsidiary graph that is AUTHORITATIVE in the federal record (do NOT infer it
from domain/LinkedIn) into two hard columns per entity: the IMMEDIATE parent (the raw
reported edge) and the ULTIMATE parent (the top of the family, derived by a cycle-safe
transitive closure over the immediate edges). This is the complement to company dedup —
dedup collapses one company logged twice; hierarchy relates distinct subsidiaries under
one parent.

WHY BOTH COLUMNS (the grain decision, resolved by live analysis 2026-07-02):
  `recipient_lookup.parent_uei` is the IMMEDIATE parent, not the ultimate — 1,194 parents
  are themselves children (4,296 two-hop chains), so if it were ultimate no parent could
  have a parent. The immediate edge is the authoritative atom; the ultimate is DERIVED by
  closure. Storing both dominates either single choice: immediate preserves the reported
  structure, ultimate powers federal-$ roll-up to the top of the family. Observed depth
  distribution: depth-1 78,087 · depth-2 4,028 · depth-3 37 · depth-4 1 · cycles 230.

SOURCES (read-only committed Lance; this worker never mutates them). All parent linkage
in the SoR is USAspending-derived — SAM `entity_registrations` carries NO parent UEI
(only EVS-source flags), so it is deliberately not a source here:
  - recipient_lookup  s3://…/usaspending/recipient_lookup/     uei → parent_uei (IMMEDIATE),
                        the recipient dimension. 1.03M uei, 82,383 immediate child edges,
                        zero parent-instability (functional graph). PRIMARY.
  - subaward_search   s3://…/usaspending/subaward_search/      (awardee|sub_awardee)_uei →
                        (sub_)ultimate_parent_uei (FSRS-reported ULTIMATE). Adds 36k net-new
                        children (subawardees never seen as primes) with only an ultimate.
  - govcon_active_awards s3://…/active/govcon_active_awards/   recipient_uei →
                        recipient_parent_uei (immediate; 506 net-new). Low-precedence fill.

GRAIN & assembly (100% DuckDB, bounded + disk spill):
  IMM  = recipient_lookup ∪ govcon immediate edges, 1 parent/child (rl precedence).
  CLO  = cycle-safe transitive closure of IMM → (ultimate, depth, in_cycle).
  SUB  = subaward child→ultimate for children NOT in IMM (immediate unknown, depth null).
  out  = 1 row per child uei (has a parent). ~119k rows. Non-child roots get no row —
         downstream rolls up via coalesce(ultimate_parent_uei, uei).

Data plane (clean-room — no catalog): Lance(3 sources) → DuckDB → Arrow →
  lance.write_dataset(R2 active, v2.1, OVERWRITE snapshot, storage_options) →
  create_scalar_index BTREE[uei, immediate_parent_uei, ultimate_parent_uei] +
  BITMAP[parent_source], directly on the R2 dataset. Recompute is a full snapshot
  (the closure is global); the prior version is retained by Lance as the rollback anchor.

Ops ledger: one terminal-state row → ops.entity_hierarchy_runs (run-state only).

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' \
      python3 pipelines/resolution/entity_hierarchy.py <init_ops|build|verify>
"""
from __future__ import annotations

import datetime as dt
import os
import sys

# ─────────────────────────── constants ───────────────────────────
FEED = "entity_hierarchy"
DATASET_URI = os.environ.get(
    "ENTITY_HIERARCHY_URI", "s3://data-sink/active/entity_hierarchy/"
).rstrip("/") + "/"

RL_URI = "s3://data-sink/active/usaspending/recipient_lookup/"
SUB_URI = "s3://data-sink/active/usaspending/subaward_search/"
GA_URI = "s3://data-sink/active/govcon_active_awards/"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576
BTREE_INDEXES = ["uei", "immediate_parent_uei", "ultimate_parent_uei"]
BITMAP_INDEXES = ["parent_source"]

# Bounded out-of-core envelope (sources up to 17.75M rows scanned on narrow columns).
DUCKDB_MEMORY_LIMIT = os.environ.get("EH_MEMORY_LIMIT", "12GB")
DUCKDB_THREADS = int(os.environ.get("EH_THREADS", "4"))
SCRATCH = os.environ.get("EH_SCRATCH", "/tmp/entity_hierarchy")
# Closure runaway/cycle guard. Real max depth is 4; 45 is generous headroom — any orig that
# fails to reach a terminal within it is a genuine cycle and is flagged in_cycle.
CLOSURE_DEPTH_CAP = 45


def log(m: str) -> None:
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


# ─────────────────────────── R2 / creds ───────────────────────────
def _r2_so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the r2-credentials secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _new_con():
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET temp_directory='{SCRATCH}/spill'")
    con.execute("SET preserve_insertion_order=false")
    return con


# ─────────────────────────── build ───────────────────────────
def _materialize(con, so):
    """Register the three Lance sources, assemble the immediate graph, run the cycle-safe
    closure, fold in subaward-only ultimates, and return (arrow_table, metrics)."""
    import lance

    def scan(uri, columns):
        return lance.dataset(uri, storage_options=so).scanner(columns=columns).to_reader()

    # ── recipient_lookup → 1/uei immediate edge + legal-name map ──
    con.register("rl_rdr", scan(RL_URI, [
        "uei", "parent_uei", "parent_legal_business_name", "legal_business_name"]))
    con.execute("""CREATE TABLE rl AS
      SELECT uei,
             max(parent_uei)  AS parent_uei,
             max(parent_name) AS parent_name,
             max(legal_name)  AS legal_name
      FROM (SELECT nullif(trim(uei),'')                        AS uei,
                   nullif(trim(parent_uei),'')                 AS parent_uei,
                   nullif(trim(parent_legal_business_name),'') AS parent_name,
                   nullif(trim(legal_business_name),'')        AS legal_name
            FROM rl_rdr)
      WHERE uei IS NOT NULL GROUP BY uei;""")
    con.unregister("rl_rdr")

    # ── govcon_active_awards → 1/child immediate edge (fallback fill) + names ──
    con.register("ga_rdr", scan(GA_URI, [
        "recipient_uei", "recipient_name", "recipient_parent_uei", "recipient_parent_name"]))
    con.execute("""CREATE TABLE ga_raw AS
      SELECT nullif(trim(recipient_uei),'')         AS child,
             nullif(trim(recipient_name),'')        AS cname,
             nullif(trim(recipient_parent_uei),'')  AS parent,
             nullif(trim(recipient_parent_name),'') AS pname
      FROM ga_rdr;""")
    con.unregister("ga_rdr")
    con.execute("""CREATE TABLE ga AS
      WITH counted AS (
        SELECT child, parent, max(pname) AS pname, count(*) AS cnt
        FROM ga_raw WHERE child IS NOT NULL AND parent IS NOT NULL AND parent <> child
        GROUP BY child, parent),
      ranked AS (SELECT child, parent, pname,
        row_number() OVER (PARTITION BY child ORDER BY cnt DESC, parent ASC) AS rn FROM counted)
      SELECT child, parent, pname FROM ranked WHERE rn = 1;""")

    # ── subaward_search → 1/child FSRS ULTIMATE (prime side ∪ sub side) ──
    # A Lance to_reader() Arrow stream is SINGLE-USE; the prime and sub legs below both scan
    # subaward, so the reader is drained into a table ONCE and the UNION ALL reads the table
    # twice. Referencing the reader itself twice silently under-reads the second leg.
    con.register("sub_rdr", scan(SUB_URI, [
        "awardee_or_recipient_uei", "awardee_or_recipient_legal",
        "ultimate_parent_uei", "ultimate_parent_legal_enti",
        "sub_awardee_or_recipient_uei", "sub_awardee_or_recipient_legal",
        "sub_ultimate_parent_uei", "sub_ultimate_parent_legal_enti"]))
    con.execute("CREATE TABLE sub_raw AS SELECT * FROM sub_rdr;")
    con.unregister("sub_rdr")
    con.execute("""CREATE TABLE sub AS
      WITH e AS (
        SELECT nullif(trim(awardee_or_recipient_uei),'')  AS child,
               nullif(trim(ultimate_parent_uei),'')       AS parent,
               nullif(trim(ultimate_parent_legal_enti),'') AS pname FROM sub_raw
        UNION ALL
        SELECT nullif(trim(sub_awardee_or_recipient_uei),'') AS child,
               nullif(trim(sub_ultimate_parent_uei),'')      AS parent,
               nullif(trim(sub_ultimate_parent_legal_enti),'') AS pname FROM sub_raw),
      counted AS (
        SELECT child, parent, max(pname) AS pname, count(*) AS cnt
        FROM e WHERE child IS NOT NULL AND parent IS NOT NULL AND parent <> child
        GROUP BY child, parent),
      ranked AS (SELECT child, parent, pname,
        row_number() OVER (PARTITION BY child ORDER BY cnt DESC, parent ASC) AS rn FROM counted)
      SELECT child, parent, pname FROM ranked WHERE rn = 1;""")
    # NB: sub_raw is retained for the unified name harvest below; dropped after `names`.

    # ── IMMEDIATE graph: rl (primary) ∪ govcon (fill where rl has no edge). Functional. ──
    con.execute("""CREATE TABLE imm AS
      SELECT uei AS child, parent_uei AS parent, 'recipient_lookup' AS src
      FROM rl WHERE parent_uei IS NOT NULL AND parent_uei <> uei
      UNION ALL
      SELECT child, parent, 'govcon_active_awards' AS src
      FROM ga WHERE child NOT IN (SELECT uei FROM rl WHERE parent_uei IS NOT NULL AND parent_uei <> uei);""")
    n_imm = con.execute("SELECT count(*), count(DISTINCT child) FROM imm;").fetchone()
    if n_imm[0] != n_imm[1]:
        raise RuntimeError(f"IMM not functional: {n_imm[0]} rows vs {n_imm[1]} distinct children")
    log(f"immediate edges: {n_imm[0]:,} (functional, 1 parent/child)")

    # ── cycle-safe transitive closure over IMM ──
    con.execute(f"""CREATE TABLE walk AS
      WITH RECURSIVE w(orig, cur, depth) AS (
        SELECT child, parent, 1 FROM imm
        UNION ALL
        SELECT w.orig, i.parent, w.depth + 1
        FROM w JOIN imm i ON i.child = w.cur
        WHERE w.depth < {CLOSURE_DEPTH_CAP} AND w.cur <> w.orig
      )
      SELECT orig, cur, depth FROM w;""")
    con.execute("""CREATE TABLE clo AS
      WITH a AS (SELECT orig, max(depth) AS max_depth, min(cur) AS min_cur FROM walk GROUP BY orig),
           t AS (SELECT orig, arg_min(cur, depth) AS root_uei, min(depth) AS root_depth
                 FROM walk WHERE cur NOT IN (SELECT child FROM imm) GROUP BY orig)
      SELECT a.orig AS uei,
             coalesce(t.root_uei, a.min_cur) AS ultimate_parent_uei,
             t.root_depth                    AS hierarchy_depth,
             (t.orig IS NULL)                AS in_cycle
      FROM a LEFT JOIN t ON t.orig = a.orig;""")

    # ── unified name map (uei → best legal name). Harvest (uei, name) from every source —
    #    rl (entity + parent), govcon (recipient + parent), subaward (awardee/sub + their
    #    ultimates) — then pick per uei by source priority (rl→govcon→subaward), tie-broken by
    #    frequency. Resolves the ~11.5k ultimate UEIs that rl alone does not name. ──
    con.execute("""CREATE TABLE name_pairs AS
      SELECT uei, legal_name AS name, 1 AS pri FROM rl WHERE legal_name IS NOT NULL
      UNION ALL SELECT parent_uei, parent_name, 1 FROM rl WHERE parent_uei IS NOT NULL AND parent_name IS NOT NULL
      UNION ALL SELECT child, cname, 2 FROM ga_raw WHERE child IS NOT NULL AND cname IS NOT NULL
      UNION ALL SELECT parent, pname, 2 FROM ga_raw WHERE parent IS NOT NULL AND pname IS NOT NULL
      UNION ALL SELECT nullif(trim(awardee_or_recipient_uei),''), nullif(trim(awardee_or_recipient_legal),''), 3 FROM sub_raw
      UNION ALL SELECT nullif(trim(ultimate_parent_uei),''), nullif(trim(ultimate_parent_legal_enti),''), 3 FROM sub_raw
      UNION ALL SELECT nullif(trim(sub_awardee_or_recipient_uei),''), nullif(trim(sub_awardee_or_recipient_legal),''), 3 FROM sub_raw
      UNION ALL SELECT nullif(trim(sub_ultimate_parent_uei),''), nullif(trim(sub_ultimate_parent_legal_enti),''), 3 FROM sub_raw;""")
    con.execute("""CREATE TABLE names AS
      WITH agg AS (
        SELECT uei, name, min(pri) AS pri, count(*) AS cnt
        FROM name_pairs WHERE uei IS NOT NULL AND name IS NOT NULL GROUP BY uei, name),
      ranked AS (SELECT uei, name,
        row_number() OVER (PARTITION BY uei ORDER BY pri ASC, cnt DESC, name ASC) AS rn FROM agg)
      SELECT uei, name AS legal_name FROM ranked WHERE rn = 1;""")
    con.execute("DROP TABLE sub_raw;")

    # ── assemble: IMM children (immediate + closure ultimate) ∪ SUB-only (ultimate only) ──
    snapshot = dt.datetime.now(dt.timezone.utc).date().isoformat()
    con.execute(f"""CREATE TABLE hierarchy AS
      -- children with a known immediate parent → closure supplies the ultimate
      SELECT
        i.child                                          AS uei,
        i.parent                                         AS immediate_parent_uei,
        ipn.legal_name                                   AS immediate_parent_name,
        c.ultimate_parent_uei                            AS ultimate_parent_uei,
        upn.legal_name                                   AS ultimate_parent_name,
        c.hierarchy_depth                                AS hierarchy_depth,
        c.in_cycle                                       AS in_cycle,
        i.src                                            AS parent_source,
        DATE '{snapshot}'                                AS snapshot_date
      FROM imm i
      JOIN clo c              ON c.uei = i.child
      LEFT JOIN names ipn     ON ipn.uei = i.parent
      LEFT JOIN names upn     ON upn.uei = c.ultimate_parent_uei
      UNION ALL
      -- subaward-only children → FSRS ultimate, immediate unknown
      SELECT
        s.child                                          AS uei,
        NULL                                             AS immediate_parent_uei,
        NULL                                             AS immediate_parent_name,
        s.parent                                         AS ultimate_parent_uei,
        coalesce(spn.legal_name, s.pname)                AS ultimate_parent_name,
        NULL                                             AS hierarchy_depth,
        FALSE                                            AS in_cycle,
        'subaward_search'                                AS parent_source,
        DATE '{snapshot}'                                AS snapshot_date
      FROM sub s
      LEFT JOIN names spn ON spn.uei = s.parent
      WHERE s.child NOT IN (SELECT child FROM imm);""")

    # cast hierarchy_depth to int32 for a compact, honest scalar
    con.execute("""CREATE TABLE hierarchy_out AS
      SELECT uei, immediate_parent_uei, immediate_parent_name,
             ultimate_parent_uei, ultimate_parent_name,
             CAST(hierarchy_depth AS INTEGER) AS hierarchy_depth,
             in_cycle, parent_source, snapshot_date
      FROM hierarchy;""")

    # metrics + integrity gate
    m = con.execute("""SELECT
        count(*)                                              AS rows,
        count(DISTINCT uei)                                   AS distinct_uei,
        count(*) FILTER (WHERE immediate_parent_uei IS NOT NULL) AS immediate_edges,
        count(*) FILTER (WHERE immediate_parent_uei IS NULL)  AS sub_only,
        count(*) FILTER (WHERE in_cycle)                      AS cyclic,
        max(hierarchy_depth)                                  AS max_depth,
        count(DISTINCT ultimate_parent_uei)                   AS ultimate_parents,
        count(*) FILTER (WHERE ultimate_parent_uei IS NULL)   AS null_ultimate
      FROM hierarchy_out;""").fetchone()
    rows, distinct_uei, imm_edges, sub_only, cyclic, max_depth, ult_parents, null_ult = m
    if rows != distinct_uei:
        raise RuntimeError(f"grain violation: {rows} rows vs {distinct_uei} distinct uei")
    if null_ult != 0:
        raise RuntimeError(f"{null_ult} rows have NULL ultimate_parent_uei (must never happen)")
    # Completeness gate: output must equal the full child universe (imm.child ∪ sub.child).
    # Catches any silent edge-drop (e.g. a single-use reader under-read) before it ships.
    expected = con.execute(
        "SELECT count(*) FROM (SELECT child FROM imm UNION SELECT child FROM sub);").fetchone()[0]
    if rows != expected:
        raise RuntimeError(f"completeness gate: {rows} rows vs {expected} distinct source children")

    metrics = {"rows": rows, "immediate_edges": imm_edges, "sub_only_children": sub_only,
               "cyclic_uei": cyclic, "max_depth": max_depth, "ultimate_parents": ult_parents,
               "snapshot_date": snapshot}
    log(f"assembled: {metrics}")
    depth_dist = con.execute(
        "SELECT hierarchy_depth, count(*) FROM hierarchy_out GROUP BY 1 ORDER BY 1;").fetchall()
    log(f"depth distribution: {depth_dist}")
    nm = con.execute("""SELECT
        count(*) FILTER (WHERE immediate_parent_uei IS NOT NULL AND immediate_parent_name IS NULL),
        count(*) FILTER (WHERE ultimate_parent_name IS NULL) FROM hierarchy_out;""").fetchone()
    log(f"name gaps: immediate_parent_name null={nm[0]:,} · ultimate_parent_name null={nm[1]:,}")

    table = con.execute("SELECT * FROM hierarchy_out").arrow()
    import pyarrow as pa
    if isinstance(table, pa.RecordBatchReader):
        table = table.read_all()
    return table, metrics


def _build_indexes(ds) -> None:
    """(Re)build all scalar indexes idempotently. replace=True so a partial/prior index set
    (e.g. an interrupted run) is repaired rather than conflicting."""
    present = set(ds.schema.names)
    for col in BTREE_INDEXES:
        if col in present:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            log(f"  BTREE ✓ {col}")
    for col in BITMAP_INDEXES:
        if col in present:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            log(f"  BITMAP ✓ {col}")


def reindex():
    """Maintenance: (re)build indexes in place on the committed dataset — no re-materialize.
    Recovers a run whose write succeeded but whose index pass was interrupted."""
    import lance

    so = _r2_so()
    _build_indexes(lance.dataset(DATASET_URI, storage_options=so))
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = [(i.get("name") if isinstance(i, dict) else getattr(i, "name", None)) for i in ds.list_indices()]
    log(f"reindex complete: rows={ds.count_rows():,} indices={idx}")


def build():
    import lance

    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    os.makedirs(f"{SCRATCH}/spill", exist_ok=True)
    status, error, metrics = "error", None, {}
    con = _new_con()
    try:
        table, metrics = _materialize(con, so)
        lance.write_dataset(
            table, DATASET_URI, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        log(f"wrote Lance (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")

        _build_indexes(lance.dataset(DATASET_URI, storage_options=so))
        committed = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        log(f"committed rows: {committed:,}")
        status = "success"
    except Exception as e:  # noqa: BLE001 — terminal handling below + re-raise
        error = f"{type(e).__name__}: {e}"
        raise
    finally:
        con.close()
        _record_run(metrics=metrics, status=status, error=error,
                    started=started, completed=dt.datetime.now(dt.timezone.utc))


# ─────────────────────────── ops ledger ───────────────────────────
def _record_run(*, metrics, status, error, started, completed) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED") or os.environ.get("HQX_DB_URL")
    if not dsn:
        log("WARN: no HQX dsn; skipping ops.entity_hierarchy_runs row")
        return
    if status != "success" and not error:
        error = "unknown terminal failure"
    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute(
                """INSERT INTO ops.entity_hierarchy_runs
                     (feed, dataset_uri, rows_written, immediate_edges, sub_only_children,
                      cyclic_uei, max_depth, ultimate_parents, snapshot_date, status, error,
                      started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (FEED, DATASET_URI, metrics.get("rows"), metrics.get("immediate_edges"),
                 metrics.get("sub_only_children"), metrics.get("cyclic_uei"),
                 metrics.get("max_depth"), metrics.get("ultimate_parents"),
                 metrics.get("snapshot_date"), status, (error or None),
                 started, completed))
            c.commit()
        log(f"ops row: status={status}")
    except Exception as e:  # noqa: BLE001 — audit must not mask the build
        log(f"WARN: ops.entity_hierarchy_runs write failed: {e}")


def init_ops():
    import psycopg
    from pathlib import Path

    sql = Path(__file__).parent.joinpath("ops_entity_hierarchy_runs.sql").read_text()
    dsn = os.environ.get("HQX_DB_URL_POOLED") or os.environ["HQX_DB_URL"]
    with psycopg.connect(dsn) as c, c.cursor() as cur:
        cur.execute(sql)
        c.commit()
    log("ops.entity_hierarchy_runs DDL applied")


# ─────────────────────────── verify (read-only) ───────────────────────────
def verify():
    import json

    import duckdb
    import lance

    so = _r2_so()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    try:
        idx = [(i.get("name") if isinstance(i, dict) else getattr(i, "name", None),
                i.get("fields") if isinstance(i, dict) else getattr(i, "fields", None))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    con = duckdb.connect()
    con.register("h", ds.scanner(columns=[
        "uei", "immediate_parent_uei", "ultimate_parent_uei", "ultimate_parent_name",
        "hierarchy_depth", "in_cycle", "parent_source"]).to_reader())
    con.execute("CREATE TABLE h AS SELECT * FROM h;")
    con.unregister("h")
    rows, du, imm, subonly, cyc, maxd, ults = con.execute("""
      SELECT count(*), count(DISTINCT uei),
             count(*) FILTER (WHERE immediate_parent_uei IS NOT NULL),
             count(*) FILTER (WHERE immediate_parent_uei IS NULL),
             count(*) FILTER (WHERE in_cycle),
             max(hierarchy_depth), count(DISTINCT ultimate_parent_uei) FROM h;""").fetchone()
    by_src = dict(con.execute(
        "SELECT parent_source, count(*) FROM h GROUP BY 1 ORDER BY 2 DESC;").fetchall())
    top_fam = con.execute("""
      SELECT ultimate_parent_uei, any_value(ultimate_parent_name) nm, count(DISTINCT uei) k
      FROM h GROUP BY 1 ORDER BY k DESC LIMIT 12;""").fetchall()
    con.close()
    out = {
        "uri": DATASET_URI, "rows": ds.count_rows(), "columns": len(ds.schema.names),
        "schema": [f"{f.name}:{f.type}" for f in ds.schema],
        "indices": idx, "distinct_uei": du, "grain_ok": rows == du,
        "immediate_edges": imm, "sub_only_children": subonly, "cyclic_uei": cyc,
        "max_depth": maxd, "distinct_ultimate_parents": ults, "by_parent_source": by_src,
        "top_families": [{"uei": u, "name": nm, "children": k} for u, nm, k in top_fam],
    }
    print(json.dumps(out, indent=2, default=str))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        build()
    elif cmd == "verify":
        verify()
    elif cmd == "reindex":
        reindex()
    elif cmd == "init_ops":
        init_ops()
    else:
        print(f"unknown command: {cmd} (init_ops|build|reindex|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
