"""Serving worker — captive_sub_diversification: the "diversify off your one prime"
target list. One row per (captive subawardee, candidate NEW prime) — fresh prime awards won by
a DIFFERENT prime that the sub is a domain-aligned capability match for.

Part of the ``captive-diversification`` Modal app. NOT directly exposed — spawned by the
Universal Dispatcher (core/modal_dispatcher.py) on a Trigger schedule, or run manually via
``modal run``. No web endpoint.

GRAIN  1 row per (sub_uei, cand_prime_uei) — the best-scoring fresh award per (captive sub, new prime).
SoR    s3://data-sink/active/captive_sub_diversification/  (Lance v2.1; derived, overwrite).
METHOD (full-universe semantic matchmaking; the ANN upgrade of govcon_sub_targeting's
       deterministic capability_match leg, which reached only 169 subs):
  1. CAPTIVE segment — over usaspending_api_fresh/contract_subaward: COUNT(DISTINCT prime)=1
     ∧ COUNT(*)>=CAPTIVE_MIN_SUBAWARDS ∧ total ∈ [CAPTIVE_MIN_DOLLARS, CAPTIVE_MAX_DOLLARS].
  2. SUB EMBEDDING — mean-pooled, renormalized centroid of each sub's
     govcon_sub_capability_vectors chunks (BGE-large 1024-d).
  3. SEMANTIC MATCH — per-sub ANN over govcon_scope_vectors (prime-solicitation scope chunks,
     IVF_PQ). Matched chunk → contract_award_unique_key (direct column) → award.
  4. AWARD→PRIME — resolved via usaspending_api_fresh/contract_prime_txn (latest txn per award):
     recipient_uei (the new prime), NAICS, action_date (freshness window), value, agency, set-aside.
  5. FILTERS — drop cand_prime == sole_prime; rolling award action_date window (WINDOW_DAYS);
     cosine-distance ceiling (MAX_DIST); HYBRID NAICS-sector gate (naics2_aligned / naics4_aligned)
     because raw cosine alone yields domain-wrong matches — the aligned tier is the usable list.
CUI    NO scope-vector text is emitted (CUI-safe). Evidence is the sub's OWN public
       subaward_description + the FPDS award title (prime_award_base_transaction_description).
LEDGER ops.captive_diversification_serving_runs (HQX_DB_URL_POOLED) on every terminal state.
       On a Trigger dispatch the orchestrator POSTs {status, rows, feed, ...} to the waitpoint URL.

    modal run    pipelines/serving/materialize_captive_diversification.py::init_ops
    modal run    pipelines/serving/materialize_captive_diversification.py::run_build
    modal run    pipelines/serving/materialize_captive_diversification.py::run_verify
    modal deploy pipelines/serving/materialize_captive_diversification.py   # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
ACTIVE = "s3://data-sink/active"
SERVING_URI = os.environ.get("CAPTIVE_DIVERSIFICATION_URI", f"{ACTIVE}/captive_sub_diversification/")
SUBAWARD_URI = f"{ACTIVE}/usaspending_api_fresh/contract_subaward/"
SUBVEC_URI = f"{ACTIVE}/govcon_sub_capability_vectors/"
SCOPEVEC_URI = f"{ACTIVE}/govcon_scope_vectors/"
PRIMETXN_URI = f"{ACTIVE}/usaspending_api_fresh/contract_prime_txn/"
FEED = "captive_sub_diversification"
DATA_STORAGE_VERSION = "2.1"

# Captive segment definition (single-prime, recurring, mid-market band).
CAPTIVE_MIN_DOLLARS = float(os.environ.get("CAPTIVE_MIN_DOLLARS", "500000"))
CAPTIVE_MAX_DOLLARS = float(os.environ.get("CAPTIVE_MAX_DOLLARS", "50000000"))
CAPTIVE_MIN_SUBAWARDS = int(os.environ.get("CAPTIVE_MIN_SUBAWARDS", "3"))

# Matchmaking knobs.
ANN_K = int(os.environ.get("CAPTIVE_ANN_K", "15"))               # nearest scope chunks per sub
ANN_NPROBES = int(os.environ.get("CAPTIVE_ANN_NPROBES", "30"))
ANN_REFINE = int(os.environ.get("CAPTIVE_ANN_REFINE", "20"))
ANN_WORKERS = int(os.environ.get("CAPTIVE_ANN_WORKERS", "8"))
MAX_DIST = float(os.environ.get("CAPTIVE_MAX_DIST", "0.55"))     # cosine-distance ceiling (smaller=closer)
TOP_PRIMES_PER_SUB = int(os.environ.get("CAPTIVE_TOP_PRIMES", "25"))
WINDOW_DAYS = int(os.environ.get("CAPTIVE_WINDOW_DAYS", "365"))  # rolling award action_date window

BTREE_INDEXES = ["sub_uei", "cand_prime_uei", "award_key", "award_action_date"]
BITMAP_INDEXES = ["naics2_aligned", "naics4_aligned"]

DUCK_MEM = os.environ.get("CAPTIVE_DUCKDB_MEMORY_LIMIT", "12GB")
DUCK_TMP = os.environ.get("CAPTIVE_DUCKDB_TEMP_DIR", "/tmp/captive_div_duckdb")

# --------------------------------------------------------------------------- #
# ops.captive_diversification_serving_runs — terminal-state ledger (idempotent DDL,
# self-bootstrapping). Mirrored verbatim at ops_captive_diversification_serving_runs.sql.
# --------------------------------------------------------------------------- #
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.captive_diversification_serving_runs (
    id                     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                 text,
    feed                   text        NOT NULL,
    window_days            integer,
    captive_subs_scored    bigint,
    subs_with_vectors      bigint,
    rows_written           bigint,
    naics2_aligned_rows    bigint,
    naics4_aligned_rows    bigint,
    subs_with_naics2_match bigint,
    distinct_new_primes    bigint,
    matchable_awards       bigint,
    avg_match_score        numeric,
    write_mode             text,
    indices_built          text,
    status                 text        NOT NULL,
    error_message          text,
    metrics                jsonb,
    started_at             timestamptz,
    completed_at           timestamptz,
    recorded_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS captive_diversification_serving_runs_status_idx
    ON ops.captive_diversification_serving_runs (status);
CREATE INDEX IF NOT EXISTS captive_diversification_serving_runs_recorded_at_idx
    ON ops.captive_diversification_serving_runs (recorded_at DESC);
"""

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",
        "lancedb>=0.15",
        "pylance>=7",            # provides `import lance`
        "pyarrow>=17",
        "numpy>=1.26",           # centroid pooling
        "boto3>=1.34",
        "requests>=2.32",        # Trigger callback
        "psycopg[binary]>=3.2",  # terminal state → ops.*
    )
    .env({"RUST_LOG": "error"})  # silence the pylance ANN autoprojection deprecation flood
)

app = modal.App("captive-diversification", image=image)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def log(m) -> None:
    import datetime as dt

    print(f"[{dt.datetime.now(dt.timezone.utc).strftime('%H:%M:%S')}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _duck():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    os.makedirs(DUCK_TMP, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{DUCK_TMP}'")
    return con


# --------------------------------------------------------------------------- #
# Assembly — captive segment → centroids → ANN → award/prime resolution → NAICS gate
# (1:1 port of the validated build; see the diagnostic
#  docs/reference/CAPTIVE_SUB_DIVERSIFICATION_DATASET.md).
# --------------------------------------------------------------------------- #
def _assemble(so: dict, window_days: int) -> tuple:
    """Returns (final_arrow_table, stats_dict). No SoR write here."""
    import datetime as dt

    import lance
    import numpy as np
    import pyarrow as pa

    cutoff = (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=window_days)).isoformat()
    con = _duck()

    log("loading contract_subaward (fact) + deriving captive segment …")
    cs = lance.dataset(SUBAWARD_URI, storage_options=so)
    con.register("cs", cs.scanner(columns=[
        "subawardee_uei", "subawardee_name", "subawardee_state_code",
        "prime_awardee_uei", "prime_awardee_name", "subaward_amount", "prime_award_naics_code"]).to_table())
    con.execute(f"""
        CREATE TEMP TABLE captive AS
        SELECT subawardee_uei uei, any_value(subawardee_name) nm, any_value(subawardee_state_code) st,
               any_value(prime_awardee_uei) sole_prime_uei, any_value(prime_awardee_name) sole_prime_nm,
               sum(try_cast(subaward_amount AS DOUBLE)) total, count(*) n_sub,
               mode(prime_award_naics_code) sub_naics
        FROM cs WHERE subawardee_uei IS NOT NULL AND length(trim(subawardee_uei))>0
        GROUP BY 1
        HAVING count(DISTINCT prime_awardee_uei)=1 AND count(*)>={CAPTIVE_MIN_SUBAWARDS}
               AND sum(try_cast(subaward_amount AS DOUBLE)) BETWEEN {CAPTIVE_MIN_DOLLARS} AND {CAPTIVE_MAX_DOLLARS}
    """)
    captive = {r[0]: dict(zip(["uei", "nm", "st", "sole_prime_uei", "sole_prime_nm", "total", "n_sub", "sub_naics"], r))
               for r in con.execute(
                   "SELECT uei,nm,st,sole_prime_uei,sole_prime_nm,total,n_sub,sub_naics FROM captive").fetchall()}
    ueis = list(captive)
    log(f"captive segment: {len(ueis)} subs")
    if not ueis:
        raise RuntimeError("captive segment is empty (check the contract_subaward feed + band params)")

    log("pulling captive sub vectors + computing centroids …")
    sv = lance.dataset(SUBVEC_URI, storage_options=so)
    emb_acc: dict[str, list] = {}
    txt_best: dict[str, tuple] = {}
    for i in range(0, len(ueis), 400):
        batch = ueis[i:i + 400]
        pred = "subawardee_uei IN (" + ",".join(_sql_str(u) for u in batch) + ")"
        t = sv.scanner(columns=["subawardee_uei", "embedding", "text", "char_len"], filter=pred).to_table().to_pylist()
        for r in t:
            u, e = r["subawardee_uei"], r["embedding"]
            if e is None:
                continue
            emb_acc.setdefault(u, []).append(np.asarray(e, dtype=np.float32))
            if r.get("text") and (u not in txt_best or (r.get("char_len") or 0) > txt_best[u][1]):
                txt_best[u] = (r["text"], r.get("char_len") or 0)
    cent: dict[str, list] = {}
    for u, vs in emb_acc.items():
        m = np.mean(np.stack(vs), axis=0)
        n = float(np.linalg.norm(m))
        if n > 0:
            cent[u] = (m / n).tolist()
    log(f"subs with vectors: {len(cent)} / {len(ueis)}")
    if not cent:
        raise RuntimeError("no captive sub carries a capability vector (govcon_sub_capability_vectors)")

    log("ANN matchmaking over govcon_scope_vectors (IVF_PQ) …")
    sc = lance.dataset(SCOPEVEC_URI, storage_options=so)

    def _match(u: str):
        try:
            tb = sc.scanner(
                columns=["contract_award_unique_key", "naics_code"],
                nearest={"column": "embedding", "q": cent[u], "k": ANN_K,
                         "nprobes": ANN_NPROBES, "refine_factor": ANN_REFINE},
            ).to_table().to_pylist()
        except Exception:  # noqa: BLE001 — one starved query must not sink the batch
            return u, []
        out = []
        for r in tb:
            ak, d = r.get("contract_award_unique_key"), r.get("_distance")
            if ak and d is not None and d <= MAX_DIST:
                out.append((ak, float(d)))
        return u, out

    from concurrent.futures import ThreadPoolExecutor
    hits: list[tuple] = []
    done = 0
    with ThreadPoolExecutor(max_workers=ANN_WORKERS) as ex:
        for u, lst in ex.map(_match, list(cent)):
            for ak, d in lst:
                hits.append((u, ak, d))
            done += 1
            if done % 500 == 0:
                log(f"  matched {done}/{len(cent)} subs · {len(hits)} raw hits")
    log(f"raw hits: {len(hits)} (distinct awards: {len(set(h[1] for h in hits))})")
    if not hits:
        raise RuntimeError("zero ANN hits within MAX_DIST — scope corpus or vectors may be empty")

    log("resolving awards → prime via contract_prime_txn …")
    pt = lance.dataset(PRIMETXN_URI, storage_options=so)
    awards = sorted(set(h[1] for h in hits))
    pcols = ["contract_award_unique_key", "recipient_uei", "recipient_name", "naics_code", "naics_description",
             "awarding_agency_name", "action_date", "current_total_value_of_award",
             "prime_award_base_transaction_description", "type_of_set_aside",
             "period_of_performance_current_end_date", "last_modified_date"]
    parts = []
    for i in range(0, len(awards), 1000):
        b = awards[i:i + 1000]
        pred = "contract_award_unique_key IN (" + ",".join(_sql_str(a) for a in b) + ")"
        parts.append(pt.scanner(columns=pcols, filter=pred).to_table())
    con.register("ptxn", pa.concat_tables(parts) if parts else pt.scanner(columns=pcols, limit=0).to_table())
    con.execute("""
        CREATE TEMP TABLE award AS
        SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY contract_award_unique_key
                         ORDER BY last_modified_date DESC NULLS LAST) rn FROM ptxn) WHERE rn=1""")

    con.register("hits", pa.table({"sub_uei": pa.array([h[0] for h in hits]),
                                   "award_key": pa.array([h[1] for h in hits]),
                                   "dist": pa.array([h[2] for h in hits], pa.float64())}))
    con.register("capt", pa.table({k: pa.array([captive[u][k] for u in captive])
                                   for k in ["uei", "nm", "st", "sole_prime_uei", "sole_prime_nm",
                                             "total", "n_sub", "sub_naics"]}))
    con.register("ev", pa.table({"uei": pa.array(list(txt_best)),
                                 "evidence": pa.array([txt_best[u][0][:200] for u in txt_best])}))

    log(f"assembling rows (drop self-prime; award action_date >= {cutoff}; NAICS gate; best per sub×prime) …")
    final = con.execute(f"""
    WITH j AS (
      SELECT h.sub_uei, a.recipient_uei AS cand_prime_uei, a.recipient_name AS cand_prime_name,
             h.award_key, h.dist, a.naics_code AS award_naics, a.naics_description AS award_naics_desc,
             a.awarding_agency_name AS award_agency, a.action_date AS award_action_date,
             try_cast(a.current_total_value_of_award AS DOUBLE) AS award_value,
             a.type_of_set_aside AS award_set_aside,
             a.period_of_performance_current_end_date AS award_pop_end,
             substr(a.prime_award_base_transaction_description,1,180) AS award_desc
      FROM hits h JOIN award a ON h.award_key = a.contract_award_unique_key
      WHERE a.action_date >= '{cutoff}'
    ),
    ranked AS (
      SELECT j.*, c.nm AS sub_name, c.st AS sub_state, c.total AS sub_total_dollars, c.n_sub AS sub_n_subawards,
             c.sole_prime_uei, c.sole_prime_nm AS sole_prime_name, c.sub_naics,
             row_number() OVER (PARTITION BY j.sub_uei, j.cand_prime_uei ORDER BY j.dist ASC) rn_pair
      FROM j JOIN capt c ON j.sub_uei = c.uei
      WHERE j.cand_prime_uei IS NOT NULL AND j.cand_prime_uei <> c.sole_prime_uei
    ),
    best AS (
      SELECT *,
        (award_naics IS NOT NULL AND sub_naics IS NOT NULL AND left(award_naics,2)=left(sub_naics,2)) AS naics2_aligned,
        (award_naics IS NOT NULL AND sub_naics IS NOT NULL AND left(award_naics,4)=left(sub_naics,4)) AS naics4_aligned
      FROM ranked WHERE rn_pair=1),
    capped AS (
      SELECT *, row_number() OVER (PARTITION BY sub_uei
                  ORDER BY naics4_aligned DESC, naics2_aligned DESC, dist ASC) rn_sub FROM best
    )
    SELECT sub_uei, sub_name, sub_state, sub_total_dollars, sub_n_subawards, sub_naics,
           sole_prime_uei, sole_prime_name,
           cand_prime_uei, cand_prime_name, award_key, round((1.0-dist)::DOUBLE,4) AS match_score,
           award_naics, award_naics_desc, award_agency, award_action_date, award_value,
           award_set_aside, award_pop_end, award_desc, naics2_aligned, naics4_aligned
    FROM capped WHERE rn_sub <= {TOP_PRIMES_PER_SUB}
    ORDER BY sub_total_dollars DESC, sub_uei, match_score DESC
    """).to_arrow_table()

    con.register("fin", final)
    final = con.execute(
        "SELECT f.*, ev.evidence AS sub_evidence FROM fin f LEFT JOIN ev ON f.sub_uei=ev.uei").to_arrow_table()
    if final.num_rows == 0:
        raise RuntimeError(f"zero diversification rows after the {window_days}d window + NAICS gate")

    stamp = pa.array([dt.datetime.now(dt.timezone.utc)] * final.num_rows, pa.timestamp("us", tz="UTC"))
    final = final.append_column("built_at", stamp)

    con.register("final", final)
    s = con.execute("""SELECT count(*) n_rows, count(DISTINCT sub_uei) subs,
        count(DISTINCT cand_prime_uei) primes, round(avg(match_score),4) avg_score,
        count(*) FILTER(WHERE naics2_aligned) n2, count(*) FILTER(WHERE naics4_aligned) n4,
        count(DISTINCT sub_uei) FILTER(WHERE naics2_aligned) subs_n2,
        count(DISTINCT award_key) matchable_awards FROM final""").fetchone()
    stats = dict(zip(["rows", "distinct_captive_subs", "distinct_new_primes", "avg_match_score",
                      "naics2_aligned_rows", "naics4_aligned_rows", "subs_with_naics2_match",
                      "matchable_awards"], s))
    stats["captive_subs_scored"] = len(ueis)
    stats["subs_with_vectors"] = len(cent)
    stats["window_days"] = window_days
    con.close()
    return final, stats


def _write_index(tbl, so: dict) -> list[str]:
    import lance

    lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(SERVING_URI, storage_options=so)
    built: list[str] = []
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        built.append(f"{col}:BTREE")
        log(f"  BTREE ✓ {col}")
    for col in BITMAP_INDEXES:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        built.append(f"{col}:BITMAP")
        log(f"  BITMAP ✓ {col}")
    return built


# --------------------------------------------------------------------------- #
# ops ledger + Trigger callback
# --------------------------------------------------------------------------- #
def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record_run(*, run_id, window_days, stats, write_mode, indices, status, error, started, completed) -> None:
    """Terminal row → ops.captive_diversification_serving_runs (psycopg). Best-effort; never masks
    the build. Self-bootstraps the table when absent (single-container run, no concurrency)."""
    from psycopg.types.json import Jsonb

    conn = _pg_connect()
    if conn is None:
        return
    st = stats or {}
    avg = st.get("avg_match_score")
    try:
        with conn.cursor() as cur:
            with conn.transaction():
                cur.execute("SELECT to_regclass('ops.captive_diversification_serving_runs')")
                if cur.fetchone()[0] is None:
                    cur.execute(OPS_DDL)
            with conn.transaction():
                cur.execute(
                    """
                    INSERT INTO ops.captive_diversification_serving_runs
                        (run_id, feed, window_days, captive_subs_scored, subs_with_vectors, rows_written,
                         naics2_aligned_rows, naics4_aligned_rows, subs_with_naics2_match, distinct_new_primes,
                         matchable_awards, avg_match_score, write_mode, indices_built, status, error_message,
                         metrics, started_at, completed_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (run_id, FEED, window_days, st.get("captive_subs_scored"), st.get("subs_with_vectors"),
                     st.get("rows"), st.get("naics2_aligned_rows"), st.get("naics4_aligned_rows"),
                     st.get("subs_with_naics2_match"), st.get("distinct_new_primes"),
                     st.get("matchable_awards"), avg, write_mode, ",".join(indices), status, error,
                     Jsonb(st) if st else None, started, completed),
                )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.captive_diversification_serving_runs write failed: {exc}")
    finally:
        conn.close()


def _post_callback(url, payload, attempts: int = 3) -> None:
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import time

    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}")
                return
            print(f"Callback attempt {i + 1} non-2xx: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


# --------------------------------------------------------------------------- #
# Modal functions (dispatcher-resolvable by name)
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,        # 1h — the ANN matchmaking over ~3k subs is ~5-10 min
    memory=32768,           # 32 GB — the scope-vector IVF_PQ index is the driver
    cpu=8.0,                # default ephemeral disk suffices (small DuckDB /tmp spill only)
    retries=1,              # idempotent overwrite → safe to retry on preemption
)
def run_build(window_days: int = WINDOW_DAYS, trigger_callback_url: str | None = None) -> dict:
    """Rebuild captive_sub_diversification end-to-end; record the ledger row + POST the
    Trigger waitpoint callback. Snapshot-overwrite (idempotent)."""
    import datetime as dt

    run_id = f"captive_div_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    started = dt.datetime.now(dt.timezone.utc)
    status, error, stats, indices = "error", None, {}, []
    try:
        so = _r2_so()
        tbl, stats = _assemble(so, window_days)
        log(f"assembled {stats['rows']:,} rows · {stats['subs_with_naics2_match']:,} captive subs with a "
            f"sector-aligned match · {stats['naics2_aligned_rows']:,} aligned rows "
            f"({stats['naics4_aligned_rows']:,} NAICS-4) · {stats['distinct_new_primes']:,} new primes")
        indices = _write_index(tbl, so)
        status = "success"
        log(f"DONE → {SERVING_URI} rows={stats['rows']:,}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        log(f"FAILED: {error}")
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, window_days=window_days, stats=stats, write_mode="overwrite",
                    indices=indices, status=status, error=error, started=started, completed=completed)
        _post_callback(trigger_callback_url, {
            "status": status, "run_mode": "build", "feed": FEED,
            "rows": stats.get("rows", 0), "naics2_aligned_rows": stats.get("naics2_aligned_rows", 0),
            "subs_with_naics2_match": stats.get("subs_with_naics2_match", 0),
            "window_days": window_days, "error": error,
        })
    if status != "success":
        raise RuntimeError(f"run_build failed: {error}")
    return {"status": status, "uri": SERVING_URI, **stats}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],  # read-back only — no Postgres write
    timeout=60 * 10, memory=8192, cpu=2.0,
)
def run_verify(trigger_callback_url: str | None = None) -> dict:
    """Read-back proof: row count, alignment tiers, index presence, and the no-self-prime invariant."""
    import lance

    so = _r2_so()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    out = {
        "uri": SERVING_URI, "rows": ds.count_rows(),
        "naics2_aligned": ds.count_rows(filter="naics2_aligned = true"),
        "naics4_aligned": ds.count_rows(filter="naics4_aligned = true"),
        "self_prime_violations": ds.count_rows(filter="cand_prime_uei = sole_prime_uei"),
        "columns": len(ds.schema.names), "indices": idx,
    }
    print(__import__("json").dumps(out, indent=2, default=str))
    _post_callback(trigger_callback_url, {"status": "success", "run_mode": "verify", "feed": FEED, **out})
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 2)
def init_ops() -> dict:
    """Create ops.captive_diversification_serving_runs up front (idempotent)."""
    conn = _pg_connect()
    if conn is None:
        return {"status": "skipped", "reason": "HQX_DB_URL_POOLED unset"}
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
        log("ops DDL applied")
        return {"status": "applied"}
    finally:
        conn.close()


@app.local_entrypoint()
def main(cmd: str = "build", window_days: int = WINDOW_DAYS) -> None:
    """`modal run pipelines/serving/materialize_captive_diversification.py::main --cmd build`"""
    import json

    if cmd == "build":
        print(json.dumps(run_build.remote(window_days=window_days), indent=2, default=str))
    elif cmd == "verify":
        print(json.dumps(run_verify.remote(), indent=2, default=str))
    elif cmd == "init_ops":
        print(json.dumps(init_ops.remote(), indent=2, default=str))
    else:
        raise SystemExit(f"unknown cmd {cmd!r} (build|verify|init_ops)")
