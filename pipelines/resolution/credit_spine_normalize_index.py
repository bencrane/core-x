"""Compute worker — Credit Spine Pre-Computation & Indexing (Task E).

A MAINTENANCE worker (not a feed): it materializes the canonical resolution
blocking keys ``normalized_legal_name`` + ``zip_code`` ON the already-committed
SBA credit Lance datasets (PPP, 7(a), 504) IN PLACE, then builds a ``BTREE``
scalar index on each new column. Additive-only — no re-ingest, no wipe, no
duplication: the borrower name/zip are read, the canonical macros applied, the
two columns appended via ``LanceDataset.add_columns`` (positional zip in _rowid
order), and the indices committed straight to R2 with ``create_scalar_index``.

WHY THIS EXISTS (docs/reference/uspto_sba_ppp_mapping_blueprint.md §Finding-2):
name normalization mutates the string ("Smith & Co., LLC" → "SMITH AND CO LLC"), so
the credit spines' *existing raw-name* BTREE (``borr_name`` / nothing on PPP
borrower) CANNOT serve a normalized-owner equality join. The cross-layer match
(USPTO owners, SoS entities, the ``crosswalk_*`` Pattern-B outputs) blocks on the
canonical ``normalized_legal_name`` + ``zip_code`` pair — so each credit spine
must carry those two columns natively, each with its own ``BTREE``. This worker
is the credit-spine counterpart to ``federal_spine_index_campaign`` (which only
indexes EXISTING columns); it ADDS the columns first, then indexes them.

CANONICAL KEYS. ``_name_norm`` is IMPORTED from ``core/name_norm.py`` (THE single
source of truth — see the import below), so it is byte-identical to the macro that
produces ``sos_normalized_master.normalized_legal_name`` and every other spine/bridge
(``recon_ca_ucc_sos`` / ``crosswalk_hmda_gleif`` / ``crosswalk_sam_usaspending`` /
``osha_sniper`` / ``fl_federal_tax_liens``) and CANNOT drift from them. ``_zip5`` stays
local — the trivial digits-left-5 key, identical on both sides. The output column names
``normalized_legal_name`` + ``zip_code`` are the SAME names the ``sos_normalized_master``
blocking spine persists and BTREE-indexes (``MASTER_BTREE_INDEXES``). Applying the
IDENTICAL macro on both sides is what makes the BTREE block-join valid; that identity is
now guaranteed by the shared import, not by hand-copying — a local copy DID drift once
(it lagged the ``&``→AND / dash→space additions, breaking every &/dash join; fixed #70).

SOURCE-COLUMN MAP (the borrower name/zip column differs per dataset — confirmed
by the blueprint's field map and by ::probe): PPP exposes ``borrower_name`` /
``borrower_zip``; the 7(a)/504 FOIA datasets expose ``borr_name`` / ``borr_zip``.
The OUTPUT names are uniform (``normalized_legal_name`` / ``zip_code``); only the
inputs differ. A missing configured source column is FATAL for that dataset (it
is a real misconfiguration — never silently materialize an all-NULL key).

INDEX TIER. All three datasets (PPP 11.47M, 7(a) 1.95M, 504 227K rows) sit far
below R2's ~100M-row multipart-escalation threshold (see
``federal_spine_index_campaign`` for the giants path), so the DIRECT-R2
``create_scalar_index`` mutation is correct for every one — no /tmp staging.
``LANCE_BYPASS_SPILLING=true`` (image env) forces the in-RAM BTREE sort, matching
``build_ppp_indexes`` / ``sba_foia`` (Lance's bounded spill sorter OOMs on the
high-cardinality string columns).

ATOMICITY. ``patch_dataset`` records the pre-patch ``version``, and an integrity
gate RECOMPUTES both keys from the source columns and asserts they equal the
stored values for every row (catches any add_columns positional misalignment),
row count is unchanged, and both indices are committed — else the dataset is
``restore()``-d to its pre-patch version and the run fails. Terminal state lands
in ``ops.schema_patch_runs`` (the same ledger ``sba_foia`` / ``crosswalk_*`` use).

    modal run …::probe                       # read-only ground truth (schema / source cols / indices / rows)
    modal run …::run   [--only ppp|sba_7a|sba_504]   # additive normalize + BTREE, in place, gated
    modal run …::verify [--only …]            # DELIVERABLE: explain_plan proves ScalarIndexQuery + <50ms
"""

from __future__ import annotations

import os

import modal

# Canonical blocking-key name normalization — THE shared macro (core/name_norm.py),
# IMPORTED rather than copied. A local copy silently drifts: the &→AND and dash→space
# rules were added to the canonical macro (and to sos_normalized_master) AFTER this
# worker's first cut, so a hand-copied rule here produced "JOHNSON JOHNSON" where the SoS
# spine now produces "JOHNSON AND JOHNSON" — breaking the credit↔SoS block-join for every
# borrower name containing "&" or a dash. Importing guarantees byte-identity with the spine.
from core.name_norm import name_norm as _name_norm

# ── System-of-record (R2). The three SBA credit datasets under data-sink/active/. ──
BUCKET = "data-sink"

# Logical name → (R2 ``s3://`` URI, borrower-name source col, borrower-zip source col).
# URIs are env-overridable with the SAME names the ingest workers use
# (ppp_loans_bulk.py / sba_foia/ingest.py) so every worker resolves one truth.
DATASETS: dict[str, dict[str, str]] = {
    "ppp": {
        "uri": os.environ.get("PPP_LANCE_URI", "s3://data-sink/active/ppp/"),
        "name_col": "borrower_name",
        "zip_col": "borrower_zip",
    },
    "sba_7a": {
        "uri": os.environ.get("SBA_7A_LANCE_URI", "s3://data-sink/active/sba_7a/"),
        "name_col": "borr_name",
        "zip_col": "borr_zip",
    },
    "sba_504": {
        "uri": os.environ.get("SBA_504_LANCE_URI", "s3://data-sink/active/sba_504/"),
        "name_col": "borr_name",
        "zip_col": "borr_zip",
    },
}

# The two canonical resolution blocking keys this worker materializes + indexes.
# Names are IDENTICAL to sos_normalized/normalize.py::MASTER_BTREE_INDEXES.
NORM_NAME_COL = "normalized_legal_name"
NORM_ZIP_COL = "zip_code"
NEW_COLS = (NORM_NAME_COL, NORM_ZIP_COL)


# ── Canonical normalization (DuckDB SQL fragments) ───────────────────────────
# _name_norm is IMPORTED from core.name_norm (top of file) — the ONE shared macro that
# also produces sos_normalized_master.normalized_legal_name, so the credit-spine key is
# byte-identical to the spine it block-joins. _zip5 is the trivial digits-left-5 key
# (core.name_norm does not export it; the rule is identical on both sides, no drift risk).
def _zip5(col: str) -> str:
    """ZIP5 blocking key: digits-only, left 5 (leading zeros survive — string ops). NULL if none."""
    return "nullif(left(regexp_replace(CAST(%s AS VARCHAR), '[^0-9]', '', 'g'), 5), '')" % col


def _q(ident: str) -> str:
    """Double-quote a SQL identifier (hygiene for arbitrary source column names)."""
    return '"' + ident.replace('"', '""') + '"'


def _norm_block_projection(name_col: str, zip_col: str, cols: tuple[str, ...]) -> str:
    """SELECT-list that derives the requested subset of {normalized_legal_name, zip_code}
    from the source columns via the canonical macros. Emitted in a fixed order so the
    add_columns positional zip is deterministic. `cols` is the set still MISSING from the
    schema (so a partial prior run only re-adds what it lacks)."""
    parts: list[str] = []
    if NORM_NAME_COL in cols:
        parts.append(f"{_name_norm(_q(name_col))} AS {NORM_NAME_COL}")
    if NORM_ZIP_COL in cols:
        parts.append(f"{_zip5(_q(zip_col))} AS {NORM_ZIP_COL}")
    return ",\n    ".join(parts)


image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "pandas>=2.2",           # lance.add_columns imports pandas on some paths
    "boto3>=1.35",           # R2 object probe (read-only size/existence)
    "psycopg[binary]>=3.2",  # ops.schema_patch_runs terminal-state ledger
    "requests>=2.32",        # _post_callback → Trigger waitpoint URL (if dispatched)
).env(
    # BTREE training sorts the column; Lance's bounded spill-to-disk sorter
    # under-sizes its DataFusion pool and OOMs on the 11.47M-row PPP / 1.95M-row
    # 7(a) high-cardinality string columns. Force the in-RAM sort path (well
    # within the 32 GiB container). Mirrors build_ppp_indexes / sba_foia.
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("core.name_norm")  # ship the canonical blocking-key macro into the container

app = modal.App("credit-spine-normalize-index", image=image)


# ─────────────────────────── R2 plumbing ───────────────────────────
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2 (Modal secret r2-credentials).
    Identical to every other pipeline's helper — AWS-style creds + endpoint +
    region 'auto'."""
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
    """Best-effort read of committed scalar indices (name/type/fields). Tolerant of
    pylance return-shape drift (dict vs object; list_indices vs list_indexes)."""
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
                    out.append({
                        "name": getattr(ix, "name", None),
                        "type": str(getattr(ix, "type", None)),
                        "fields": getattr(ix, "fields", None),
                    })
            return out
        except Exception as exc:  # noqa: BLE001
            return [{"error": f"{attr}: {exc}"}]
    return [{"error": "no list_indices/list_indexes method on dataset"}]


def _index_names(ds) -> set[str]:
    """Committed scalar-index NAMES (e.g. ``normalized_legal_name_idx``)."""
    return {
        (i.get("name") if isinstance(i, dict) else getattr(i, "name", None))
        for i in (ds.list_indices() if hasattr(ds, "list_indices") else [])
    }


# ─────────────────────────── ops ledger ───────────────────────────
# Cross-worker ledger for additive in-place schema patches — DDL mirrored
# VERBATIM from sba_foia/ingest.py + crosswalk_sam_usaspending.py (one table,
# every in-place patch writes to it). Idempotent.
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
    """POST terminal metadata to the Trigger waitpoint URL. FLAT JSON body — no
    ``{"data": ...}`` envelope. A few retries for delivery reliability."""
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
    """Read-only ground truth per dataset: existence, rows, full schema, committed
    indices, whether the configured source name/zip columns are present, and whether
    the normalized keys + their BTREE indices already exist. No mutation."""
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
            "normalized_legal_name_indexed": f"{NORM_NAME_COL}_idx" in idx_names,
            "zip_code_indexed": f"{NORM_ZIP_COL}_idx" in idx_names,
            "committed_indices": committed,
        })
        print(f"✓ {name}: {row['rows']:,} rows v{ds.version} | "
              f"src({spec['name_col']}={row['source_name_col_present']},"
              f"{spec['zip_col']}={row['source_zip_col_present']}) | "
              f"norm_present({row['normalized_legal_name_present']},{row['zip_code_present']}) "
              f"norm_indexed({row['normalized_legal_name_indexed']},{row['zip_code_indexed']})")
        report.append(row)
    return report


@app.local_entrypoint()
def probe() -> None:
    """Read-only ground-truth sweep (existence, schema, source cols, indices)."""
    import json

    print(json.dumps(probe_all.remote(), indent=2, default=str))


# ──────────── Materialize + index — additive, in-place, gated ────────────
@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 120,
    memory=32768,    # 32 GiB — comfortably holds the DuckDB transform/gate tables for the
                     # 11.47M-row PPP column AND the in-RAM BTREE sort (LANCE_BYPASS_SPILLING).
                     # Over-provisions 7(a)/504 harmlessly; memory does NOT force spot capacity.
    cpu=8.0,
    # NO ephemeral_disk: direct-R2 create_scalar_index reads only the target column via
    # columnar range GETs and writes index files back in place. add_columns appends new
    # fragments straight to R2. No local dataset staging → no scratch disk needed (all
    # three datasets are far below the ~100M-row giants threshold; see module docstring).
)
def patch_dataset(name: str, trigger_callback_url: str | None = None,
                  recompute: bool = False) -> dict:
    """ADDITIVE IN-PLACE: materialize normalized_legal_name + zip_code on one credit
    dataset and BTREE-index both, WITHOUT recreating it.

      1. read the borrower name/zip columns (with _rowid) via the Lance scanner;
      2. DuckDB applies the canonical _name_norm / _zip5 macros → the missing key(s),
         ordered by _rowid; add_columns zips them on POSITIONALLY (_rowid order);
      3. create_scalar_index builds a BTREE on each key (direct-R2, replace=True);
      4. integrity gate: RECOMPUTE both keys from source and assert they equal the
         stored values for every row (proves the positional add aligned), row count
         unchanged, and both indices committed — else restore() to the pre-patch
         version and fail.
    Idempotent: a key already in the schema is not re-added; the indices are always
    (re)built. A source column absent from the schema is fatal (misconfiguration).
    ``recompute=True`` first DROPS an existing ``normalized_legal_name`` (+ its BTREE) so it
    is re-materialized with the CURRENT canonical macro — required whenever that macro changes
    (e.g. #70's ``&``→AND / dash→space): add_columns only fills MISSING keys, so a stale
    old-rule column would otherwise be skipped and then fail the gate. ``zip_code``'s rule is
    unchanged, so it is never dropped (its BTREE is still rebuilt, like every run)."""
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
                f"(have name/zip candidates among {sorted(present)[:20]}…) — refusing to "
                f"materialize an all-NULL blocking key")

        # 0. --recompute: force re-materialization of normalized_legal_name under the CURRENT
        # macro. add_columns (step 1+2) only fills keys MISSING from the schema, so a
        # normalized_legal_name already materialized under an OLD rule would be SKIPPED and then
        # FAIL the integrity gate (stored old-rule value != new-rule recompute). Drop the stale
        # key (+ its BTREE) so the additive step rebuilds it canonically. zip_code's rule is
        # unchanged → never dropped. Safe no-op when the key is absent (first/fresh materialize).
        if recompute and NORM_NAME_COL in present:
            stale_idx = f"{NORM_NAME_COL}_idx"
            if stale_idx in _index_names(ds):
                ds.drop_index(stale_idx)
            ds.drop_columns([NORM_NAME_COL])
            ds = lance.dataset(uri, storage_options=so)
            present = set(ds.schema.names)
            print(f"recompute ✓ {name}: dropped stale {NORM_NAME_COL} (+{stale_idx}) → re-materializing")

        # 1+2. Add only the keys not already present (positional zip in _rowid order).
        to_add = tuple(c for c in NEW_COLS if c not in present)
        if to_add:
            proj = _norm_block_projection(name_col, zip_col, to_add)
            con = duckdb.connect(":memory:")
            con.execute("PRAGMA threads=8;")
            con.execute("SET memory_limit='20GB';")
            con.execute("SET temp_directory='/tmp/credit_spine_spill';")
            con.register("rdr", ds.scanner(columns=[name_col, zip_col],
                                           with_row_id=True).to_reader())
            con.execute("CREATE TABLE t AS SELECT * FROM rdr")
            con.unregister("rdr")
            arrow = con.execute(
                f"SELECT\n    {proj}\nFROM t ORDER BY _rowid"
            ).to_arrow_table().combine_chunks()
            con.close()
            ds.add_columns(arrow, batch_size=65536)   # positional zip in _rowid order
            ds = lance.dataset(uri, storage_options=so)
            added_cols = list(to_add)
            print(f"add_columns ✓ {name}: {added_cols} ({n0:,} rows)")
        else:
            print(f"add_columns – {name}: both keys already present (idempotent)")

        # 3. BTREE on each key (direct-R2; replace=True → idempotent rebuild).
        for col in NEW_COLS:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            print(f"  BTREE ✓ {col}")
        ds = lance.dataset(uri, storage_options=so)
        v_after = ds.version

        # 4. Integrity gate — recompute both keys from source; they MUST match stored.
        con = duckdb.connect(":memory:")
        con.execute("PRAGMA threads=8;")
        con.execute("SET memory_limit='20GB';")
        con.execute("SET temp_directory='/tmp/credit_spine_spill';")
        con.register("rdr", ds.scanner(
            columns=[name_col, zip_col, NORM_NAME_COL, NORM_ZIP_COL]).to_reader())
        con.execute("CREATE TABLE v AS SELECT * FROM rdr")
        con.unregister("rdr")
        bad_name = con.execute(
            f"SELECT count(*) FROM v WHERE {NORM_NAME_COL} IS DISTINCT FROM {_name_norm(_q(name_col))}"
        ).fetchone()[0]
        bad_zip = con.execute(
            f"SELECT count(*) FROM v WHERE {NORM_ZIP_COL} IS DISTINCT FROM {_zip5(_q(zip_col))}"
        ).fetchone()[0]
        n1, n_name_nonnull, n_zip_nonnull = con.execute(
            f"SELECT count(*), count({NORM_NAME_COL}), count({NORM_ZIP_COL}) FROM v"
        ).fetchone()
        con.close()

        idx = _index_names(ds)
        ok = (bad_name == 0 and bad_zip == 0 and n1 == n0
              and f"{NORM_NAME_COL}_idx" in idx and f"{NORM_ZIP_COL}_idx" in idx)
        if not ok:
            lance.dataset(uri, storage_options=so, version=v_before).restore()
            raise RuntimeError(
                f"integrity gate failed (rolled back to v{v_before}): "
                f"name_mismatch={bad_name} zip_mismatch={bad_zip} rows={n1}/{n0} "
                f"indices={sorted(c for c in idx if c)}")

        result = {
            "rows": n1,
            "normalized_legal_name_nonnull": n_name_nonnull,
            "zip_code_nonnull": n_zip_nonnull,
            "name_recompute_mismatches": bad_name,
            "zip_recompute_mismatches": bad_zip,
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
            f"{NORM_NAME_COL}_idx,{NORM_ZIP_COL}_idx" if status == "success" else None,
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
    """Materialize + BTREE-index the normalized keys in place on each credit dataset.
    --only <name>   restrict to one of: ppp | sba_7a | sba_504 (substring match).
    --recompute     drop + re-materialize normalized_legal_name under the CURRENT macro
                    (use after the canonical rule changes; no-op when the key is absent)."""
    import json

    targets = [n for n in DATASETS if (only in n if only else True)]
    if not targets:
        print(f"No datasets matched only={only!r}; known: {sorted(DATASETS)}")
        return
    for name in targets:
        print(f"\n=== {name}{' (recompute)' if recompute else ''} ===")
        print(json.dumps(patch_dataset.remote(name, trigger_callback_url=None,
                                              recompute=recompute),
                         indent=2, default=str))


# ───────────────────────── Verify — the deliverable ─────────────────────────
@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 20,
              memory=8192, cpu=4.0)
def verify_dataset(name: str, runs: int = 5) -> dict:
    """DELIVERABLE proof: a point query on ``normalized_legal_name`` resolves via the
    scalar index and returns under 50 ms (warm). For each new key: sample a real
    non-null value, capture ``explain_plan(verbose=True)`` (asserting the physical plan
    contains ``ScalarIndexQuery``), and report the median of `runs` warm executions.
    Read-only — mutates nothing. Mirrors recon_ca_ucc_sos.py::_bench."""
    import time

    import lance

    if name not in DATASETS:
        return {"dataset": name, "status": "unknown_dataset"}
    spec = DATASETS[name]
    so = _r2_storage_options()
    ds = lance.dataset(spec["uri"], storage_options=so)
    ds.count_rows()  # warm the dataset open (manifest + metadata)
    payload = spec["name_col"]  # non-indexed payload col → forces index → take (realistic read)

    def _probe_col(indexed_col: str) -> dict:
        t = ds.scanner(columns=[indexed_col], filter=f"{indexed_col} IS NOT NULL",
                       limit=1).to_table()
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
        ds.scanner(columns=cols, filter=flt).to_table()  # warm-up (index page load)
        ts = []
        hits = 0
        for _ in range(runs):
            t0 = time.perf_counter()
            hits = ds.scanner(columns=cols, filter=flt).to_table().num_rows
            ts.append((time.perf_counter() - t0) * 1000)
        ts.sort()
        median = round(ts[len(ts) // 2], 2)
        return {"col": indexed_col, "value": val, "hits": hits,
                "uses_scalar_index": uses_index, "under_50ms": median < 50.0,
                "median_ms": median, "min_ms": round(ts[0], 2), "max_ms": round(ts[-1], 2),
                "physical_plan": plan}

    out = {
        "dataset": name, "uri": spec["uri"], "rows": ds.count_rows(),
        "committed_indices": _list_committed_indices(ds),
        NORM_NAME_COL: _probe_col(NORM_NAME_COL),
        NORM_ZIP_COL: _probe_col(NORM_ZIP_COL),
    }
    nl = out[NORM_NAME_COL]
    out["deliverable_pass"] = bool(nl.get("uses_scalar_index") and nl.get("under_50ms"))
    print(f"{name}: {NORM_NAME_COL} ScalarIndexQuery={nl.get('uses_scalar_index')} "
          f"median={nl.get('median_ms')}ms under_50ms={nl.get('under_50ms')} "
          f"→ deliverable_pass={out['deliverable_pass']}")
    return out


@app.local_entrypoint()
def verify(only: str = "", runs: int = 5) -> None:
    """DELIVERABLE: explain_plan proves ScalarIndexQuery + sub-50ms point query.
    --only <name>   restrict to one of: ppp | sba_7a | sba_504."""
    import json

    targets = [n for n in DATASETS if (only in n if only else True)]
    if not targets:
        print(f"No datasets matched only={only!r}; known: {sorted(DATASETS)}")
        return
    for name in targets:
        print(f"\n=== verify {name} ===")
        print(json.dumps(verify_dataset.remote(name, runs), indent=2, default=str))


# ───────────── Sample — read-only &/dash spot-check (drift evidence) ─────────────
@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 20,
              memory=16384, cpu=4.0)
def sample_dataset(name: str, n: int = 6) -> dict:
    """READ-ONLY: surface borrower names containing ``&`` or a dash and show the canonical
    normalization — plus the STORED ``normalized_legal_name`` (if materialized) and whether
    they agree. Direct evidence that the ``&``→AND / dash→space rules (#70's fix) are live in
    the materialized key. Filtering runs in DuckDB (exact LIKE semantics — not Lance
    filter-pushdown) over the streamed name column, and the &/dash population counts are
    reported so a genuinely sparse dataset is distinguishable from a missed match. Mutates
    nothing. Before ::run the key is absent → ``canonical_preview`` shows what WILL be written;
    after, ``stored`` must equal ``canonical`` for every sample (the gate guarantees it for ALL
    rows — this surfaces it on the names that exercise the new rules)."""
    import duckdb
    import lance

    if name not in DATASETS:
        return {"dataset": name, "status": "unknown_dataset"}
    spec = DATASETS[name]
    so = _r2_storage_options()
    ds = lance.dataset(spec["uri"], storage_options=so)
    name_col = spec["name_col"]
    has_norm = NORM_NAME_COL in set(ds.schema.names)
    cols = [name_col] + ([NORM_NAME_COL] if has_norm else [])

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4;")
    con.execute("SET memory_limit='12GB';")
    con.register("rdr", ds.scanner(columns=cols).to_reader())
    stored_sel = f"{_q(NORM_NAME_COL)} AS stored, " if has_norm else ""
    con.execute(
        f"CREATE TABLE t AS SELECT {_q(name_col)} AS raw, {stored_sel}"
        f"{_name_norm(_q(name_col))} AS canonical FROM rdr"
    )
    con.unregister("rdr")
    dash_pred = "(raw LIKE '%-%' OR raw LIKE '%–%' OR raw LIKE '%—%')"  # hyphen + en/em dash
    amp = con.execute("SELECT count(*) FROM t WHERE raw LIKE '%&%'").fetchone()[0]
    dash = con.execute(f"SELECT count(*) FROM t WHERE {dash_pred}").fetchone()[0]
    half = max(1, n // 2)
    sel = "raw, " + ("stored, " if has_norm else "") + "canonical"
    rows = con.execute(
        f"(SELECT {sel} FROM t WHERE raw LIKE '%&%' LIMIT {half}) UNION ALL "
        f"(SELECT {sel} FROM t WHERE {dash_pred} LIMIT {half})"
    ).fetchall()
    con.close()

    samples = []
    for r in rows:
        if has_norm:
            raw, stored, canonical = r
            samples.append({"raw": raw, "stored": stored, "canonical": canonical,
                            "match": stored == canonical})
        else:
            raw, canonical = r
            samples.append({"raw": raw, "canonical_preview": canonical})
    all_match = all(s.get("match", True) for s in samples)
    print(f"{name}: &-names={amp:,} dash-names={dash:,} | has_norm={has_norm} | "
          f"{len(samples)} samples | all stored==canonical={all_match}")
    for s in samples:
        print(f"  {s}")
    return {"dataset": name, "has_norm_column": has_norm,
            "ampersand_name_count": amp, "dash_name_count": dash,
            "all_stored_equal_canonical": all_match, "samples": samples}


@app.local_entrypoint()
def sample(only: str = "", n: int = 6) -> None:
    """READ-ONLY &/dash spot-check across the credit spines (stored vs canonical recompute).
    --only <name>   restrict to one of: ppp | sba_7a | sba_504."""
    import json

    targets = [nm for nm in DATASETS if (only in nm if only else True)]
    if not targets:
        print(f"No datasets matched only={only!r}; known: {sorted(DATASETS)}")
        return
    for nm in targets:
        print(f"\n=== sample {nm} ===")
        print(json.dumps(sample_dataset.remote(nm, n), indent=2, default=str))
