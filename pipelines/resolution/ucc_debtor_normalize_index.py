"""Compute worker — UCC debtor canonical blocking-key pre-computation + indexing.

A MAINTENANCE worker (not a feed), the UCC-debtor counterpart to
``credit_spine_normalize_index`` (PPP / 7(a) / 504): it materializes the canonical
resolution blocking keys ``normalized_legal_name`` + ``zip_code`` + ``legal_name_base``
ON the already-committed UCC debtor Lance datasets IN PLACE, then builds a ``BTREE``
scalar index on each new column. Additive-only — no re-ingest, no wipe, no duplication:
the debtor org-name/zip are read, the canonical macros applied, the new columns appended
via ``LanceDataset.add_columns`` (positional zip in _rowid order), and the indices
committed straight to R2 with ``create_scalar_index``.

WHY THIS EXISTS. The SAM⟷UCC-debtor resolution (and the UCC→SoS bridge) blocks on the
canonical ``(legal_name_base, zip_code)`` pair that ``sos_normalized_master`` and
``sam_normalized_entities`` already persist + BTREE-index. The UCC debtor tables carry
only the RAW debtor name today:
  · ca_ucc_debtors  — raw ``org_name`` (BTREE), no canonical key. recon_ca_ucc_sos
    computed the key on the fly, in-memory; it was never persisted.
  · ucc_co_debtors  — carries an UPSTREAM ``party_name_normalized`` (a Gen-2/vendor
    normalization, passed through untouched at ingest), which is NOT guaranteed
    byte-identical to the canonical macro. This worker recomputes the canonical key
    from ``organization_name`` via ``core.name_norm`` — the upstream column is ignored.
So each debtor table must carry the three canonical columns natively, each BTREE'd, for a
sub-second indexed block-join to the SoS spine. ORG debtors only carry a key; INDIVIDUAL
debtors (no ``organization_name``/``org_name``) get NULL keys — harmless, filtered downstream.

CANONICAL KEYS. ``_name_norm`` / ``_legal_name_base`` are IMPORTED from ``core/name_norm.py``
(THE single source of truth), byte-identical to ``sos_normalized_master.normalized_legal_name``
+ ``legal_name_base`` and to credit_spine_normalize_index. ``legal_name_base`` is peeled from
the SAME canonical name expression that defines ``normalized_legal_name`` — so it equals
``legal_name_base(normalized_legal_name)`` for every row (equal-normalized ⟹ equal-base).
``_zip5`` is the trivial digits-left-5 key, identical on both sides.

SOURCE-COLUMN MAP (confirmed by ::probe): ca_ucc_debtors exposes ``org_name`` / ``postal_code``
(ZIP+4); ucc_co_debtors exposes ``organization_name`` / ``party_zip5``. The OUTPUT names are
uniform (``normalized_legal_name`` / ``zip_code`` / ``legal_name_base``); only the inputs
differ. A missing configured source column is FATAL (a real misconfiguration).

INDEX TIER. Both datasets (CA debtors ~5.86M, CO debtors ~1.99M) sit far below R2's
~100M-row multipart-escalation threshold, so the DIRECT-R2 ``create_scalar_index`` mutation
is correct — no /tmp staging. ``LANCE_BYPASS_SPILLING=true`` forces the in-RAM BTREE sort
(Lance's bounded spill sorter OOMs on high-cardinality string columns).

ATOMICITY. ``patch_dataset`` records the pre-patch ``version``; an integrity gate RECOMPUTES
all three keys from the source columns and asserts they equal the stored values for every row
(catches any add_columns positional misalignment), row count unchanged, and all three indices
committed — else the dataset is ``restore()``-d to its pre-patch version and the run fails.
Terminal state lands in ``ops.schema_patch_runs`` (the shared additive-patch ledger).

    modal run    pipelines/resolution/ucc_debtor_normalize_index.py::probe                       # read-only ground truth
    modal run    pipelines/resolution/ucc_debtor_normalize_index.py::run   [--only ca_ucc_debtors|ucc_co_debtors]
    modal run    pipelines/resolution/ucc_debtor_normalize_index.py::verify [--only ...]          # ScalarIndexQuery proof
    modal deploy pipelines/resolution/ucc_debtor_normalize_index.py                               # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

from core.name_norm import legal_name_base as _legal_name_base, name_norm as _name_norm

BUCKET = "data-sink"

# Logical name → (R2 URI, debtor org-name source col, debtor zip source col).
# URIs env-overridable with the SAME names the ingest workers use (ca_ucc/ingest.py,
# co_ucc/companions_bulk.py) so every worker resolves one truth.
DATASETS: dict[str, dict[str, str]] = {
    "ca_ucc_debtors": {
        "uri": os.environ.get("CA_UCC_DEBTORS_URI", "s3://data-sink/active/ca_ucc/debtors/"),
        "name_col": "org_name",
        "zip_col": "postal_code",
    },
    "ucc_co_debtors": {
        "uri": os.environ.get("UCC_CO_DEBTORS_URI", "s3://data-sink/active/ucc_co_debtors/"),
        "name_col": "organization_name",
        "zip_col": "party_zip5",
    },
}

# The three canonical resolution blocking keys. Names IDENTICAL to
# sos_normalized/normalize.py::MASTER_BTREE_INDEXES and credit_spine_normalize_index.
NORM_NAME_COL = "normalized_legal_name"
NORM_ZIP_COL = "zip_code"
NORM_BASE_COL = "legal_name_base"
NEW_COLS = (NORM_NAME_COL, NORM_ZIP_COL, NORM_BASE_COL)


# ── Canonical normalization (DuckDB SQL fragments) ───────────────────────────
def _zip5(col: str) -> str:
    """ZIP5 blocking key: digits-only, left 5 (leading zeros survive — string ops). NULL if none.
    Idempotent on an already-5-digit column (CO party_zip5) and on ZIP+4 (CA postal_code)."""
    return "nullif(left(regexp_replace(CAST(%s AS VARCHAR), '[^0-9]', '', 'g'), 5), '')" % col


def _q(ident: str) -> str:
    """Double-quote a SQL identifier (hygiene for arbitrary source column names)."""
    return '"' + ident.replace('"', '""') + '"'


def _norm_block_projection(name_col: str, zip_col: str, cols: tuple[str, ...]) -> str:
    """SELECT-list deriving the requested subset of {normalized_legal_name, zip_code,
    legal_name_base} from the source columns via the canonical macros, in a fixed order so
    the add_columns positional zip is deterministic. `cols` is the set still MISSING from the
    schema (a partial prior run only re-adds what it lacks).

    legal_name_base is peeled from the SAME canonical name expression that defines
    normalized_legal_name — legal_name_base(name_norm(name_col)) — so it is byte-identical to
    legal_name_base(normalized_legal_name) for every row, preserving the equal-normalized ⟹
    equal-base superset invariant the SoS/SAM base-joins rely on. No dependency on the stored
    norm column → order-independent under --recompute."""
    parts: list[str] = []
    if NORM_NAME_COL in cols:
        parts.append(f"{_name_norm(_q(name_col))} AS {NORM_NAME_COL}")
    if NORM_ZIP_COL in cols:
        parts.append(f"{_zip5(_q(zip_col))} AS {NORM_ZIP_COL}")
    if NORM_BASE_COL in cols:
        parts.append(f"{_legal_name_base(_name_norm(_q(name_col)))} AS {NORM_BASE_COL}")
    return ",\n    ".join(parts)


image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "pandas>=2.2",           # lance.add_columns imports pandas on some paths
    "boto3>=1.35",
    "psycopg[binary]>=3.2",  # ops.schema_patch_runs terminal-state ledger
    "requests>=2.32",        # _post_callback → Trigger waitpoint URL (if dispatched)
).env(
    # BTREE training sorts the column; Lance's bounded spill-to-disk sorter under-sizes its
    # DataFusion pool and OOMs on the high-cardinality org-name column. Force the in-RAM sort
    # path (well within the 32 GiB container). Mirrors credit_spine / sba_foia.
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("core.name_norm")  # ship the canonical blocking-key macro into the container

app = modal.App("ucc-debtor-normalize-index", image=image)


# ─────────────────────────── R2 plumbing ───────────────────────────
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _list_committed_indices(ds) -> list[dict]:
    """Best-effort read of committed scalar indices (name/type/fields)."""
    for attr in ("list_indices", "list_indexes"):
        fn = getattr(ds, attr, None)
        if fn is None:
            continue
        try:
            out = []
            for ix in fn():
                if isinstance(ix, dict):
                    out.append({k: ix.get(k) for k in ("name", "type", "fields")})
                else:
                    out.append({"name": getattr(ix, "name", None),
                                "type": str(getattr(ix, "type", None)),
                                "fields": getattr(ix, "fields", None)})
            return out
        except Exception as exc:  # noqa: BLE001
            return [{"error": f"{attr}: {exc}"}]
    return [{"error": "no list_indices/list_indexes method on dataset"}]


def _index_names(ds) -> set[str]:
    """Committed scalar-index NAMES (e.g. ``normalized_legal_name_idx``)."""
    return {(i.get("name") if isinstance(i, dict) else getattr(i, "name", None))
            for i in (ds.list_indices() if hasattr(ds, "list_indices") else [])}


# ─────────────────────────── ops ledger ───────────────────────────
# Shared additive-in-place-patch ledger — DDL identical to credit_spine_normalize_index /
# sba_foia / crosswalk_*. One table, every in-place patch writes to it. Idempotent.
OPS_PATCH_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.schema_patch_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_uri     text        NOT NULL,
    operation       text        NOT NULL,
    column_added    text,
    index_built     text,
    rows            bigint,
    exact_dup_rows  bigint,
    version_before  bigint,
    version_after   bigint,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS schema_patch_runs_dataset_idx  ON ops.schema_patch_runs (dataset_uri);
CREATE INDEX IF NOT EXISTS schema_patch_runs_recorded_idx ON ops.schema_patch_runs (recorded_at DESC);
"""


def _record_patch(dataset_uri, operation, column_added, index_built, rows,
                  version_before, version_after, status, error, started_at, completed_at) -> None:
    """Terminal row → ops.schema_patch_runs (psycopg). Best-effort; never masks the patch."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.schema_patch_runs write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_PATCH_DDL)
            cur.execute(
                """
                INSERT INTO ops.schema_patch_runs
                    (dataset_uri, operation, column_added, index_built, rows,
                     version_before, version_after, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (dataset_uri, operation, column_added, index_built, rows,
                 version_before, version_after, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the patch
        print(f"WARN: ops.schema_patch_runs write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint URL (FLAT body). No-op for manual runs."""
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


# ─────────────────────────── Probe (read-only) ───────────────────────────
@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 20,
              memory=16384, cpu=4.0)
def probe_all() -> list[dict]:
    """Read-only ground truth per dataset: existence, rows, schema, committed indices, whether
    the configured source name/zip columns are present, and whether the canonical keys + their
    BTREE indices already exist. No mutation."""
    import lance

    so = _r2_storage_options()
    report: list[dict] = []
    for name, spec in DATASETS.items():
        row: dict = {"dataset": name, "uri": spec["uri"],
                     "name_col": spec["name_col"], "zip_col": spec["zip_col"]}
        try:
            ds = lance.dataset(spec["uri"], storage_options=so)
        except Exception as exc:  # noqa: BLE001
            row.update({"exists": False, "open_error": str(exc)[:300]})
            report.append(row)
            print(f"✗ {name}: OPEN FAILED — {str(exc)[:160]}")
            continue
        present = set(ds.schema.names)
        committed = _list_committed_indices(ds)
        idx_names = {c.get("name") for c in committed if isinstance(c, dict)}
        row.update({
            "exists": True,
            "rows": ds.count_rows(),
            "version": ds.version,
            "schema": [f"{f.name}:{f.type}" for f in ds.schema],
            "source_name_col_present": spec["name_col"] in present,
            "source_zip_col_present": spec["zip_col"] in present,
            "normalized_legal_name_present": NORM_NAME_COL in present,
            "zip_code_present": NORM_ZIP_COL in present,
            "legal_name_base_present": NORM_BASE_COL in present,
            "normalized_legal_name_indexed": f"{NORM_NAME_COL}_idx" in idx_names,
            "zip_code_indexed": f"{NORM_ZIP_COL}_idx" in idx_names,
            "legal_name_base_indexed": f"{NORM_BASE_COL}_idx" in idx_names,
            "committed_indices": committed,
        })
        print(f"✓ {name}: {row['rows']:,} rows v{ds.version} | "
              f"src({spec['name_col']}={row['source_name_col_present']},"
              f"{spec['zip_col']}={row['source_zip_col_present']}) | "
              f"present({row['normalized_legal_name_present']},{row['zip_code_present']},"
              f"{row['legal_name_base_present']}) "
              f"indexed({row['normalized_legal_name_indexed']},{row['zip_code_indexed']},"
              f"{row['legal_name_base_indexed']})")
        report.append(row)
    return report


@app.local_entrypoint()
def probe() -> None:
    """Read-only ground-truth sweep (existence, schema, source cols, indices)."""
    import json

    print(json.dumps(probe_all.remote(), indent=2, default=str))


# ──────────── Materialize + index — additive, in-place, gated ────────────
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120,
    memory=32768,    # holds the DuckDB transform/gate tables for the ~5.86M-row CA debtor
                     # column AND the in-RAM BTREE sort (LANCE_BYPASS_SPILLING).
    cpu=8.0,
)
def patch_dataset(name: str, trigger_callback_url: str | None = None,
                  recompute: bool = False) -> dict:
    """ADDITIVE IN-PLACE: materialize normalized_legal_name + zip_code + legal_name_base on one
    UCC debtor dataset and BTREE-index each, WITHOUT recreating it.

      1. read the debtor org-name/zip columns (with _rowid) via the Lance scanner;
      2. DuckDB applies the canonical _name_norm / _zip5 / _legal_name_base macros → the missing
         key(s), ordered by _rowid; add_columns zips them on POSITIONALLY (_rowid order);
      3. create_scalar_index builds a BTREE on each key (direct-R2, replace=True);
      4. integrity gate: RECOMPUTE all keys from source and assert they equal the stored values
         for every row (proves the positional add aligned), row count unchanged, and all three
         indices committed — else restore() to the pre-patch version and fail.
    Idempotent: a key already in the schema is not re-added; indices are always (re)built. A
    source column absent from the schema is fatal. ``recompute=True`` first DROPS the
    name-macro-derived keys (normalized_legal_name + legal_name_base) so they re-materialize
    under the CURRENT macro; zip_code's rule is unchanged so it is never dropped.
    NOTE: INDIVIDUAL debtors (NULL org-name) yield NULL keys — expected, filtered downstream."""
    import datetime as dt

    import duckdb
    import lance

    if name not in DATASETS:
        return {"dataset": name, "status": "unknown_dataset"}
    spec = DATASETS[name]
    uri, name_col, zip_col = spec["uri"], spec["name_col"], spec["zip_col"]
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    status, error, added_cols, result = "error", None, [], {}

    ds = lance.dataset(uri, storage_options=so)
    v_before = ds.version
    n0 = ds.count_rows()
    v_after = v_before
    try:
        present = set(ds.schema.names)
        missing_src = [c for c in (name_col, zip_col) if c not in present]
        if missing_src:
            raise RuntimeError(
                f"source column(s) absent from {name} schema: {missing_src} "
                f"(have among {sorted(present)[:20]}…) — refusing to materialize an all-NULL key")

        # 0. --recompute: drop the name-macro-derived keys so they re-materialize under the
        # CURRENT macro (add_columns only fills MISSING keys). Drop indices before columns.
        if recompute:
            idx_now = _index_names(ds)
            drop_cols = [c for c in (NORM_NAME_COL, NORM_BASE_COL) if c in present]
            for c in drop_cols:
                if f"{c}_idx" in idx_now:
                    ds.drop_index(f"{c}_idx")
            if drop_cols:
                ds.drop_columns(drop_cols)
                ds = lance.dataset(uri, storage_options=so)
                present = set(ds.schema.names)
                print(f"recompute ✓ {name}: dropped stale {drop_cols} (+indices) → re-materializing")

        # 1+2. Add only the keys not already present (positional zip in _rowid order).
        to_add = tuple(c for c in NEW_COLS if c not in present)
        if to_add:
            proj = _norm_block_projection(name_col, zip_col, to_add)
            con = duckdb.connect(":memory:")
            con.execute("PRAGMA threads=8;")
            con.execute("SET memory_limit='20GB';")
            con.execute("SET temp_directory='/tmp/ucc_debtor_spill';")
            con.register("rdr", ds.scanner(columns=[name_col, zip_col],
                                           with_row_id=True).to_reader())
            con.execute("CREATE TABLE t AS SELECT * FROM rdr")
            con.unregister("rdr")
            arrow = con.execute(f"SELECT\n    {proj}\nFROM t ORDER BY _rowid").to_arrow_table().combine_chunks()
            con.close()
            ds.add_columns(arrow, batch_size=65536)   # positional zip in _rowid order
            ds = lance.dataset(uri, storage_options=so)
            added_cols = list(to_add)
            print(f"add_columns ✓ {name}: {added_cols} ({n0:,} rows)")
        else:
            print(f"add_columns – {name}: all keys already present (idempotent)")

        # 3. BTREE on each key (direct-R2; replace=True → idempotent rebuild).
        for col in NEW_COLS:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            print(f"  BTREE ✓ {col}")
        ds = lance.dataset(uri, storage_options=so)
        v_after = ds.version

        # 4. Integrity gate — recompute all three keys from source; they MUST match stored.
        con = duckdb.connect(":memory:")
        con.execute("PRAGMA threads=8;")
        con.execute("SET memory_limit='20GB';")
        con.execute("SET temp_directory='/tmp/ucc_debtor_spill';")
        con.register("rdr", ds.scanner(
            columns=[name_col, zip_col, NORM_NAME_COL, NORM_ZIP_COL, NORM_BASE_COL]).to_reader())
        con.execute("CREATE TABLE v AS SELECT * FROM rdr")
        con.unregister("rdr")
        bad_name = con.execute(
            f"SELECT count(*) FROM v WHERE {NORM_NAME_COL} IS DISTINCT FROM {_name_norm(_q(name_col))}"
        ).fetchone()[0]
        bad_zip = con.execute(
            f"SELECT count(*) FROM v WHERE {NORM_ZIP_COL} IS DISTINCT FROM {_zip5(_q(zip_col))}"
        ).fetchone()[0]
        bad_base = con.execute(
            f"SELECT count(*) FROM v WHERE {NORM_BASE_COL} IS DISTINCT FROM {_legal_name_base(_name_norm(_q(name_col)))}"
        ).fetchone()[0]
        n1, n_name_nonnull, n_zip_nonnull, n_base_nonnull = con.execute(
            f"SELECT count(*), count({NORM_NAME_COL}), count({NORM_ZIP_COL}), count({NORM_BASE_COL}) FROM v"
        ).fetchone()
        con.close()

        idx = _index_names(ds)
        ok = (bad_name == 0 and bad_zip == 0 and bad_base == 0 and n1 == n0
              and f"{NORM_NAME_COL}_idx" in idx and f"{NORM_ZIP_COL}_idx" in idx
              and f"{NORM_BASE_COL}_idx" in idx)
        if not ok:
            lance.dataset(uri, storage_options=so, version=v_before).restore()
            raise RuntimeError(
                f"integrity gate failed (rolled back to v{v_before}): "
                f"name_mismatch={bad_name} zip_mismatch={bad_zip} base_mismatch={bad_base} "
                f"rows={n1}/{n0} indices={sorted(c for c in idx if c)}")

        result = {
            "rows": n1,
            "normalized_legal_name_nonnull": n_name_nonnull,
            "zip_code_nonnull": n_zip_nonnull,
            "legal_name_base_nonnull": n_base_nonnull,
            "name_recompute_mismatches": bad_name,
            "zip_recompute_mismatches": bad_zip,
            "base_recompute_mismatches": bad_base,
            "indices": sorted(c for c in idx if c),
        }
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_patch(
            uri, f"add_norm_block:{name}",
            ",".join(added_cols) or None,
            f"{NORM_NAME_COL}_idx,{NORM_ZIP_COL}_idx,{NORM_BASE_COL}_idx" if status == "success" else None,
            result.get("rows"), v_before, v_after, status, error, started, completed)
        _post_callback(trigger_callback_url, {
            "status": status, "dataset": name, "dataset_uri": uri,
            "operation": "add_norm_block", **result})

    if status != "success":
        raise RuntimeError(f"patch_dataset failed for {name}: {error}")
    return {"dataset": name, "dataset_uri": uri, "operation": "add_norm_block",
            "version_before": v_before, "version_after": v_after,
            "added_columns": added_cols, **result}


@app.local_entrypoint()
def run(only: str = "", recompute: bool = False) -> None:
    """Materialize + BTREE-index the canonical keys in place on each UCC debtor dataset.
    --only <name>   restrict to one of: ca_ucc_debtors | ucc_co_debtors (substring match).
    --recompute     drop + re-materialize the name-derived keys under the CURRENT macro."""
    import json

    targets = [n for n in DATASETS if (only in n if only else True)]
    if not targets:
        print(f"No datasets matched only={only!r}; known: {sorted(DATASETS)}")
        return
    for name in targets:
        print(f"\n=== {name}{' (recompute)' if recompute else ''} ===")
        print(json.dumps(patch_dataset.remote(name, trigger_callback_url=None, recompute=recompute),
                         indent=2, default=str))


# ───────────────────────── Verify — the deliverable ─────────────────────────
@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 20,
              memory=8192, cpu=4.0)
def verify_dataset(name: str, runs: int = 5) -> dict:
    """Point query on normalized_legal_name / legal_name_base resolves via the scalar index
    (ScalarIndexQuery in the physical plan). Reports median of `runs` warm executions and the
    R2-ceiling (<2000ms) pass. Read-only."""
    import time

    import lance

    if name not in DATASETS:
        return {"dataset": name, "status": "unknown_dataset"}
    spec = DATASETS[name]
    so = _r2_storage_options()
    ds = lance.dataset(spec["uri"], storage_options=so)
    ds.count_rows()
    payload = spec["name_col"]

    def _probe_col(indexed_col: str) -> dict:
        t = ds.scanner(columns=[indexed_col], filter=f"{indexed_col} IS NOT NULL", limit=1).to_table()
        if not t.num_rows:
            return {"col": indexed_col, "error": "no non-null value to probe"}
        val = str(t.column(indexed_col)[0].as_py()).replace("'", "''")
        cols = [indexed_col] + ([payload] if payload != indexed_col else [])
        flt = f"{indexed_col} = '{val}'"
        try:
            plan = ds.scanner(columns=cols, filter=flt).explain_plan(True)
        except Exception as exc:  # noqa: BLE001
            plan = f"explain_plan unavailable: {exc}"
        uses_index = "ScalarIndexQuery" in plan
        ds.scanner(columns=cols, filter=flt).to_table()  # warm
        ts, hits = [], 0
        for _ in range(runs):
            t0 = time.perf_counter()
            hits = ds.scanner(columns=cols, filter=flt).to_table().num_rows
            ts.append((time.perf_counter() - t0) * 1000)
        ts.sort()
        median = round(ts[len(ts) // 2], 2)
        return {"col": indexed_col, "value": val, "hits": hits, "uses_scalar_index": uses_index,
                "under_2000ms": median < 2000.0, "median_ms": median,
                "min_ms": round(ts[0], 2), "max_ms": round(ts[-1], 2)}

    out = {"dataset": name, "uri": spec["uri"], "rows": ds.count_rows(),
           "committed_indices": _list_committed_indices(ds),
           NORM_NAME_COL: _probe_col(NORM_NAME_COL), NORM_BASE_COL: _probe_col(NORM_BASE_COL)}
    nl, nb = out[NORM_NAME_COL], out[NORM_BASE_COL]
    out["index_pass"] = bool(nl.get("uses_scalar_index") and nb.get("uses_scalar_index"))
    print(f"{name}: norm ScalarIndexQuery={nl.get('uses_scalar_index')} median={nl.get('median_ms')}ms | "
          f"base ScalarIndexQuery={nb.get('uses_scalar_index')} median={nb.get('median_ms')}ms "
          f"→ index_pass={out['index_pass']}")
    return out


@app.local_entrypoint()
def verify(only: str = "", runs: int = 5) -> None:
    """ScalarIndexQuery proof + warm point-query latency on the canonical keys."""
    import json

    targets = [n for n in DATASETS if (only in n if only else True)]
    if not targets:
        print(f"No datasets matched only={only!r}; known: {sorted(DATASETS)}")
        return
    for name in targets:
        print(f"\n=== verify {name} ===")
        print(json.dumps(verify_dataset.remote(name, runs), indent=2, default=str))
