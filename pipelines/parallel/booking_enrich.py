"""Compute worker — per-BOOKING company enrichment (Parallel.ai Task API).

A STANDALONE workflow: on a new cal.com booking we enrich the prospect's company into a
small set of typed, cited firmographic columns. Deliberately NOT the audience/spec
``parallel-enrich`` machinery — a booking is ONE inbound company (``company_name`` + ``domain``,
not a ``company_id`` in the spine), so this mirrors the clean single-entity shape of the
booking→deep-research path instead of bending a batch/audience pipeline around one row.

SHAPE
    edge_api (cal webhook) → Trigger ``booking-enrich`` → Universal Dispatcher → THIS Modal app
    → ONE Parallel Task run (POST /v1/tasks/runs + blocking GET /result, §0) → typed columns →
    s3://data-sink/active/booking_enrichment/ (merge_insert on ``ical_uid``) → flat callback.
    The COMPLETE Parallel result is ALSO captured verbatim to booking_enrichment_raw (keyed by
    parallel_run_id, append-only) — the provider's raw output is never discarded.

SCHEMA-DRIVEN (the deliberate design property)
    ``_OUTPUT_SCHEMA`` below is the SINGLE source of truth for what gets pulled back. Every
    downstream step — the Parallel output_schema, the typed Lance columns, the ``__confidence``
    sidecars, the BTREE set — is derived from it. To add/remove an enrichment field: edit
    ``_OUTPUT_SCHEMA`` and ``modal deploy``. Nothing else changes. The existing dataset
    schema-evolves automatically on the next write (``_evolve_columns`` → ``add_columns``), so a
    new column never breaks the first post-deploy write (the lesson from the deep-research fix:
    lance 7.x merge_insert REJECTS a superset schema).

KEYING
    The row keys on ``ical_uid`` — the booking's stable anchor (survives reschedules), passed in
    by the kickoff. ``corex.bookings.ical_uid`` ⋈ ``booking_enrichment.ical_uid`` is a direct
    equi-join; ``enrich_run_id`` (the Trigger run id) is also stamped onto the booking for
    run-level traceability, and ``run_id``/``parallel_run_id`` ride along as columns.

TIER ceiling = ``core`` (lite|base|core). ``ultra`` deferred (per-run webhook path), as the
other Parallel workers. Default ``lite`` — firmographics are shallow + bookings are low-volume.

SECRETS: parallel-api · r2-credentials · hqx-postgres.

    modal deploy pipelines/parallel/booking_enrich.py
    modal run    pipelines/parallel/booking_enrich.py::init_ops   # create ops.booking_enrich_runs
"""

from __future__ import annotations

import os

import modal

from core.parallel_client import create_task_run, get_task_result, wrap_json_schema

_ACTIVE = "s3://data-sink/active"
ENRICH_URI = os.environ.get("BOOKING_ENRICH_URI", f"{_ACTIVE}/booking_enrichment/")
# Raw capture — the COMPLETE Parallel result per run lands here VERBATIM, nothing dropped. Keyed
# by parallel_run_id (unique per run) → append-only history: a re-enrich never overwrites a prior
# run's raw. We do NOT discard what the provider returns; the raw payload is the SoR for a run.
ENRICH_RAW_URI = os.environ.get("BOOKING_ENRICH_RAW_URI", f"{_ACTIVE}/booking_enrichment_raw/")

# ── ▼▼▼ EDIT HERE — the enrichment contract ▼▼▼ ──────────────────────────────────────────────
# The ONE place that defines what is pulled back. Add a property → it becomes a typed Lance
# column (+ ``<field>__confidence``) on the next deploy; the dataset evolves automatically.
# Keep it a PURE JSON-Schema object (wrapped to Parallel wire form at dispatch). Descriptions
# steer the extraction — write them well.
_OUTPUT_SCHEMA: dict = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "industry": {"type": "string",
                     "description": "The company's primary industry / sector, in a few words."},
        "employee_size_band": {"type": "string",
                               "description": "Approximate employee headcount as a band: one of "
                               "'1-10', '11-50', '51-200', '201-500', '501-1000', '1001-5000', '5000+'."},
        "hq_location": {"type": "string",
                        "description": "Headquarters location as 'City, State/Region, Country'."},
        "founded_year": {"type": "string",
                         "description": "The 4-digit year the company was founded; empty if unknown."},
    },
    "required": [],
}
# ── ▲▲▲ EDIT HERE ▲▲▲ ────────────────────────────────────────────────────────────────────────

ALLOWED_PROCESSORS = ("lite", "base", "core")
PROCESSOR_CEILING = "core"

RESULT_HOLD_S = 300            # blocking GET /result server hold (§0)
RESULT_408_BACKOFF_S = 5
PER_RUN_DEADLINE_S = int(os.environ.get("BOOKING_ENRICH_RUN_DEADLINE_S", str(20 * 60)))

MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"

# Confidence gate parity with parallel-enrich: null/low/medium are soft; the VALUE still lands.
REVIEW_CONFIDENCES = frozenset({None, "low", "medium"})

# Dedicated ledger — its OWN table (no coupling to ops.parallel_runs' workflow CHECK). Self-applied.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.booking_enrich_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          text,
    parallel_run_id text,
    ical_uid        text,
    company_name    text,
    domain          text,
    processor       text,
    idempotency_key text,
    status          text NOT NULL,
    completed       integer NOT NULL DEFAULT 0,
    review_fields   integer NOT NULL DEFAULT 0,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS booking_enrich_runs_ical_idx     ON ops.booking_enrich_runs (ical_uid);
CREATE INDEX IF NOT EXISTS booking_enrich_runs_run_idx      ON ops.booking_enrich_runs (run_id);
CREATE INDEX IF NOT EXISTS booking_enrich_runs_recorded_idx ON ops.booking_enrich_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "psycopg[binary]>=3.2",
    "requests>=2.32",
).env(
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("core.parallel_client")

app = modal.App("booking-enrich", image=image)


# ── plumbing (fleet-identical) ───────────────────────────────────────────────────────────────
def _r2_storage_options() -> dict[str, str]:
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


def _hqx_dsn() -> str | None:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        return None
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _bare_domain(raw: str | None) -> str | None:
    import re

    d = (raw or "").strip().lower()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    d = d.split("/", 1)[0]
    d = d.rstrip(".")
    return d or None


def _json_dump(v) -> str | None:
    import json

    if v is None:
        return None
    return v if isinstance(v, str) else json.dumps(v, default=str)


# ── Provenance columns (fixed) + schema-driven field columns ────────────────────────────────
_PROVENANCE = ("ical_uid", "company_name", "normalized_domain", "run_id", "parallel_run_id",
               "processor", "enriched_at", "_basis")

# Raw-capture columns — the full Parallel result {run, output} as a verbatim JSON string, keyed
# by parallel_run_id. We discard NOTHING the vendor returns.
_RAW_COLUMNS = ("parallel_run_id", "run_id", "ical_uid", "raw_result", "processor", "created_at")


def _field_names() -> list[str]:
    return list((_OUTPUT_SCHEMA.get("properties") or {}).keys())


def _all_columns() -> list[str]:
    cols = list(_PROVENANCE)
    for f in _field_names():
        cols.append(f)
        cols.append(f"{f}__confidence")
    return cols


# ── Blocking Parallel result (§0) ───────────────────────────────────────────────────────────
def _await_result(run_id: str) -> tuple[str, dict | None, list, str | None]:
    """Block on GET /result, re-call on 408 until the per-run deadline. Returns
    (status, content_dict, basis, raw_result) — raw_result is the COMPLETE Parallel result
    (run + output) as a verbatim JSON string, kept for raw capture (never discarded)."""
    import time

    deadline = time.monotonic() + PER_RUN_DEADLINE_S
    while time.monotonic() < deadline:
        res = get_task_result(run_id, timeout=RESULT_HOLD_S, beta_field_basis=True)
        if res.get("_status") == 408:
            time.sleep(RESULT_408_BACKOFF_S)
            continue
        run_obj = res.get("run") or {}
        output = res.get("output") or {}
        status = str(run_obj.get("status") or "completed").lower()
        content = output.get("content") if isinstance(output, dict) else None
        basis = output.get("basis") if isinstance(output, dict) else []
        return (status, (content if isinstance(content, dict) else None), (basis or []),
                _json_dump(res))
    return "timeout", None, [], None


# ── content+basis → ONE typed all-string Arrow row (schema-driven) ──────────────────────────
def _build_row(*, ical_uid: str, company_name: str, normalized_domain: str | None,
               trigger_run_id: str, parallel_run_id: str, processor: str,
               content: dict, basis: list):
    """Project the declared fields → one Arrow row. Explicit all-string schema (stable types so
    merge_insert never sees a type change). Returns (table, review_field_names)."""
    import datetime as dt

    import pyarrow as pa

    conf_by_field = {b.get("field"): b.get("confidence")
                     for b in (basis or []) if isinstance(b, dict) and b.get("field")}

    def _s(v):
        if v is None:
            return None
        if isinstance(v, (list, dict)):
            return _json_dump(v)
        if isinstance(v, bool):
            return "true" if v else "false"
        return v if isinstance(v, str) else str(v)

    rec = {
        "ical_uid": ical_uid,
        "company_name": company_name or None,
        "normalized_domain": normalized_domain,
        "run_id": trigger_run_id,
        "parallel_run_id": parallel_run_id,
        "processor": processor,
        "enriched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "_basis": _json_dump(basis) if basis else None,
    }
    review_fields: list[str] = []
    for f in _field_names():
        rec[f] = _s((content or {}).get(f))
        conf = conf_by_field.get(f)
        rec[f"{f}__confidence"] = _s(conf)
        if conf in REVIEW_CONFIDENCES:
            review_fields.append(f)

    cols = _all_columns()
    schema = pa.schema([pa.field(c, pa.string()) for c in cols])
    table = pa.table({c: pa.array([rec.get(c)], type=pa.string()) for c in cols}, schema=schema)
    return table, review_fields


# ── Lance write (merge_insert on ical_uid) with schema-evolution guard ──────────────────────
def _is_dataset_absent(exc: Exception) -> bool:
    msg = str(exc).lower()
    needles = ("not found", "does not exist", "no such", "dataset not", "table not",
               "no version", "not a dataset", "could not find")
    return any(n in msg for n in needles)


def _evolve_columns(ds, want_cols, uri: str, so: dict):
    """Add any missing string columns to an EXISTING dataset before merge. lance 7.x merge_insert
    requires the input schema to match EXACTLY (a new field raises 'Append with different schema …
    unexpected=[…]'), so an added enrichment column must be materialized first (existing rows →
    NULL). Idempotent; append-only metadata commit (no fragment rewrite). This is what makes
    editing _OUTPUT_SCHEMA safe against the live dataset."""
    missing = [c for c in want_cols if c not in {f.name for f in ds.schema}]
    if not missing:
        return ds
    import lance

    ds.add_columns({c: "CAST(NULL AS STRING)" for c in missing})
    print(f"  schema-evolve: +{missing} → {uri}")
    return lance.dataset(uri, storage_options=so)


def _write_enrichment(table, so: dict) -> int:
    """merge_insert on ``ical_uid`` (refresh on re-enrich, insert on first). CREATE only when
    genuinely absent; a merge failure RAISES (terminal) — never an overwrite that clobbers."""
    import lance

    if table.num_rows == 0:
        return 0
    exists = True
    try:
        lance.dataset(ENRICH_URI, storage_options=so)
    except Exception as exc:  # noqa: BLE001
        if _is_dataset_absent(exc):
            exists = False
        else:
            raise
    if not exists:
        lance.write_dataset(
            table, ENRICH_URI, mode="create", data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
    else:
        ds = lance.dataset(ENRICH_URI, storage_options=so)
        ds = _evolve_columns(ds, _all_columns(), ENRICH_URI, so)
        ds.merge_insert("ical_uid").when_matched_update_all().when_not_matched_insert_all().execute(table)

    ds = lance.dataset(ENRICH_URI, storage_options=so)
    names = [f.name for f in ds.schema]
    for col in ("ical_uid", "normalized_domain", "run_id", "parallel_run_id"):
        if col in names:
            try:
                ds.create_scalar_index(col, index_type="BTREE", replace=True)
            except Exception as exc:  # noqa: BLE001
                print(f"  BTREE  ✗ {col}: {exc}")
    return table.num_rows


def _write_raw(*, parallel_run_id: str, trigger_run_id: str, ical_uid: str, raw_result: str,
               processor: str, so: dict) -> int:
    """Land the COMPLETE Parallel result VERBATIM → booking_enrichment_raw (merge_insert on
    parallel_run_id, unique per run → append-only; a re-enrich never overwrites a prior run's
    raw). The entire {run, output} payload is kept so NOTHING the vendor returns is discarded.
    Self-creating + schema-evolution-safe. A write failure RAISES (terminal) — the raw is the
    SoR for the run, so failing loud beats silently dropping it."""
    import datetime as dt

    import lance
    import pyarrow as pa

    if raw_result is None:
        return 0
    rec = {"parallel_run_id": parallel_run_id, "run_id": trigger_run_id, "ical_uid": ical_uid,
           "raw_result": raw_result, "processor": processor,
           "created_at": dt.datetime.now(dt.timezone.utc).isoformat()}
    schema = pa.schema([pa.field(c, pa.string()) for c in _RAW_COLUMNS])
    table = pa.table({c: pa.array([rec.get(c)], type=pa.string()) for c in _RAW_COLUMNS}, schema=schema)

    exists = True
    try:
        lance.dataset(ENRICH_RAW_URI, storage_options=so)
    except Exception as exc:  # noqa: BLE001
        if _is_dataset_absent(exc):
            exists = False
        else:
            raise
    if not exists:
        lance.write_dataset(
            table, ENRICH_RAW_URI, mode="create", data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
    else:
        ds = lance.dataset(ENRICH_RAW_URI, storage_options=so)
        ds = _evolve_columns(ds, _RAW_COLUMNS, ENRICH_RAW_URI, so)
        ds.merge_insert("parallel_run_id").when_matched_update_all().when_not_matched_insert_all().execute(table)

    ds = lance.dataset(ENRICH_RAW_URI, storage_options=so)
    names = [f.name for f in ds.schema]
    for col in ("parallel_run_id", "ical_uid", "run_id"):
        if col in names:
            try:
                ds.create_scalar_index(col, index_type="BTREE", replace=True)
            except Exception as exc:  # noqa: BLE001
                print(f"  BTREE  ✗ raw {col}: {exc}")
    return table.num_rows


def _record_run(row: dict) -> None:
    import psycopg

    dsn = _hqx_dsn()
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.booking_enrich_runs write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.booking_enrich_runs
                    (run_id, parallel_run_id, ical_uid, company_name, domain, processor,
                     idempotency_key, status, completed, review_fields, error, started_at, completed_at)
                VALUES (%(run_id)s, %(parallel_run_id)s, %(ical_uid)s, %(company_name)s, %(domain)s,
                        %(processor)s, %(idempotency_key)s, %(status)s, %(completed)s, %(review_fields)s,
                        %(error)s, %(started_at)s, %(completed_at)s)
                """,
                row,
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.booking_enrich_runs write failed: {exc}")


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


# ── The dispatched worker ────────────────────────────────────────────────────────────────────
@app.function(
    secrets=[
        modal.Secret.from_name("parallel-api"),
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 30,
    memory=4096,
    cpu=2.0,
)
def enrich_booking(
    trigger_callback_url: str | None = None,
    run_id: str | None = None,
    ical_uid: str = "",
    company_name: str = "",
    domain: str = "",
    processor: str = "lite",
    idempotency_key: str | None = None,
    cost_cap: float | None = None,
) -> dict:
    """One Parallel Task run → typed firmographic row keyed by ``ical_uid``.

    ``run_id`` is the dispatching Trigger run id (stamped on the booking as ``enrich_run_id``).
    Terminal → ops.booking_enrich_runs + flat callback. Re-raises only on genuine failure so
    Modal marks the function failed (the durable wait surfaces it)."""
    import datetime as dt
    import uuid

    started = dt.datetime.now(dt.timezone.utc)
    run_id = run_id or uuid.uuid4().hex
    proc = (processor or "lite").strip().lower()
    ical_uid = (ical_uid or "").strip()
    dom = _bare_domain(domain)
    status, error, completed, review_n, parallel_run_id = "failed", None, 0, 0, None

    def _terminal(st, **extra):
        finished = dt.datetime.now(dt.timezone.utc)
        _record_run({
            "run_id": run_id, "parallel_run_id": parallel_run_id, "ical_uid": ical_uid or None,
            "company_name": company_name or None, "domain": dom, "processor": proc,
            "idempotency_key": idempotency_key, "status": st, "completed": completed,
            "review_fields": review_n, "error": error, "started_at": started, "completed_at": finished,
        })
        _post_callback(trigger_callback_url, {
            "status": st, "run_id": run_id, "workflow": "booking_enrich", "ical_uid": ical_uid or None,
            "parallel_run_id": parallel_run_id, "dataset_uri": ENRICH_URI,
            "completed": completed, "review_fields": review_n, "error": error, **extra,
        })

    try:
        # Guardrails — fail before any spend.
        if not ical_uid:
            status, error = "rejected", "ical_uid is required"
            return _terminal(status) or {"status": status, "error": error}
        if proc not in ALLOWED_PROCESSORS:
            status, error = "rejected", (
                f"processor {proc!r} not allowed — ceiling is {PROCESSOR_CEILING!r} (ultra deferred).")
            return _terminal(status) or {"status": status, "error": error}
        if not dom and not (company_name or "").strip():
            status, error = "rejected", "need a domain or company_name to enrich"
            return _terminal(status) or {"status": status, "error": error}

        ent_input = {"company_name": company_name or ""}
        if dom:
            ent_input["company_website"] = dom
        task_spec = {"output_schema": wrap_json_schema(_OUTPUT_SCHEMA)}
        run = create_task_run(input_=ent_input, processor=proc, task_spec=task_spec,
                              beta_field_basis=True)
        parallel_run_id = run.get("run_id")
        if not parallel_run_id:
            status, error = "failed", f"create returned no run_id: {str(run)[:200]}"
            return _terminal("failed") or {"status": status, "error": error}

        st, content, basis, raw_res = _await_result(parallel_run_id)
        so = _r2_storage_options()
        # RAW CAPTURE — the verbatim provider payload, landed FIRST and independently of the
        # typed projection. We never discard what Parallel returns, even on a non-completed run.
        if raw_res is not None:
            _write_raw(parallel_run_id=parallel_run_id, trigger_run_id=run_id, ical_uid=ical_uid,
                       raw_result=raw_res, processor=proc, so=so)
        if st != "completed" or content is None:
            status, error = "failed", f"parallel run did not complete (status={st})"
            return _terminal("failed") or {"status": status, "error": error}

        table, review_fields = _build_row(
            ical_uid=ical_uid, company_name=company_name, normalized_domain=dom,
            trigger_run_id=run_id, parallel_run_id=parallel_run_id, processor=proc,
            content=content, basis=basis,
        )
        _write_enrichment(table, so)
        completed, review_n = 1, len(review_fields)
        print(f"  landed 1 booking_enrichment row → {ENRICH_URI} (ical_uid={ical_uid}, "
              f"fields={_field_names()}, review={review_fields})")

        status, error = "success", None
        return _terminal("success") or {"status": status, "run_id": run_id,
                                        "ical_uid": ical_uid, "completed": completed}

    except Exception as exc:  # noqa: BLE001
        status, error = "failed", str(exc)
        _terminal("failed")
        raise RuntimeError(f"booking enrich failed: {exc}") from exc


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.booking_enrich_runs (HQX). No Parallel calls, no spend."""
    import psycopg

    dsn = _hqx_dsn()
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("ops.booking_enrich_runs ready.")
    return {"tables": ["ops.booking_enrich_runs"]}


@app.local_entrypoint()
def init_ops() -> None:
    print(apply_ops_ddl.remote())
