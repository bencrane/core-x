"""Compute worker — Parallel.ai DEEP RESEARCH (Directive 24, workflow 2 of 3).

The ``parallel-deep-research`` Modal app. Endpoint-less; spawned by the Universal
Dispatcher. Produces a cited long-form report (NOT typed columns) via the Task API with
``output_schema={"type":"text"}`` or ``{"type":"auto"}`` (§0). Contract: §0 of
docs/reference/DIRECTIVE_24_PARALLEL_ENRICHMENT_CAPABILITY.md (authoritative).

LANDING DECISION (documented per the mandate)
    A research report is a long Markdown document with a citations sidecar — NOT a wide
    typed column set. It does not belong in the per-spec enrichment dataset (that is keyed
    for segmentation). It lands in its OWN Lance dataset, ``s3://data-sink/active/
    parallel_research/``, with a ``grain`` parameter selecting the row shape:

      • grain="per_entity"  → ONE row per company_id, keyed company_id (BTREE):
          {company_id, normalized_domain, report_md, basis, objective, processor,
           spec_id, run_id, created_at}
        Use when researching each member of an audience ("write a deep profile of every
        company"). Joins to the companies spine on company_id, exactly like enrichment.

      • grain="topic"      → ONE row keyed run_id (BTREE):
          {run_id, objective, report_md, basis, processor, created_at}
        Use for a single standalone research question not tied to an entity set ("research
        the SBA 7(a) secondary market").

    WHY one dataset, two grains (not two datasets): both are the same artifact (a cited
    report) and share the same downstream consumer (read the report by its key). A single
    dataset keeps discovery/catalog flat; the grain column + the populated key disambiguate.
    report_md is the report text; basis is the JSON citations/reasoning sidecar.

WAIT (§0): blocking GET /v1/tasks/runs/{id}/result?timeout=600, LOOPED with 408 handling,
within the Modal fn timeout. Each entity is its own run (per_entity) or one run (topic).

TIER (§0/§9): v1 supports up to ``pro``. ``ultra`` is REJECTED with a clear message — it
runs for hours and needs Parallel's per-run webhook → trigger token path (deferred). lite/
base/core/pro are accepted (the blocking /result hold + the Modal timeout cover them).

CONTROL PLANE: Trigger v4 durable callback — flat JSON to the waitpoint url; ops.parallel_runs.

SECRETS: parallel-api · r2-credentials · hqx-postgres. (parallel-api must be minted from
Doppler core-x/prd before deploy.)

    modal deploy pipelines/parallel/deep_research.py
"""

from __future__ import annotations

import os

import modal

from core.parallel_client import auto_output, create_task_run, get_task_result, text_output

_ACTIVE = "s3://data-sink/active"
RESEARCH_URI = os.environ.get("PARALLEL_RESEARCH_URI", f"{_ACTIVE}/parallel_research/")

# §0/§9: deep research v1 supports up to 'pro'. ultra deferred (per-run webhook path).
ALLOWED_PROCESSORS = ("lite", "base", "core", "pro")
REJECTED_PROCESSORS = ("ultra",)

# Per-run blocking-result budget — each /result GET holds up to RESULT_HOLD_S; loop on 408
# until the per-run deadline. Comfortably inside the Modal fn timeout for ≤pro.
RESULT_HOLD_S = 300
RESULT_408_BACKOFF_S = 5  # short sleep if the server 408s early, so the loop never tightens
PER_RUN_DEADLINE_S = int(os.environ.get("PARALLEL_RESEARCH_RUN_DEADLINE_S", str(50 * 60)))

MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"

# Mirror of the ops .sql siblings (self-applied before terminal writes).
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.parallel_runs (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    spec_id            text NOT NULL,
    workflow           text NOT NULL CHECK (workflow IN ('enrich','deep_research','search')),
    run_kind           text NOT NULL CHECK (run_kind IN ('test','full')),
    group_id           text,
    audience_id        uuid,
    idempotency_key    text,
    requested          bigint,
    skipped_no_domain  bigint,
    completed          bigint,
    failed             bigint,
    failed_company_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
    processor          text,
    cost_estimate      numeric,
    cost_cap           numeric,
    dataset_uri        text,
    status             text NOT NULL,
    error              text,
    started_at         timestamptz,
    completed_at       timestamptz,
    recorded_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS parallel_runs_spec_idx     ON ops.parallel_runs (spec_id);
CREATE INDEX IF NOT EXISTS parallel_runs_workflow_idx ON ops.parallel_runs (workflow);
CREATE INDEX IF NOT EXISTS parallel_runs_status_idx   ON ops.parallel_runs (status);
CREATE INDEX IF NOT EXISTS parallel_runs_group_idx    ON ops.parallel_runs (group_id);
CREATE INDEX IF NOT EXISTS parallel_runs_recorded_idx ON ops.parallel_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "psycopg[binary]>=3.2",
    "requests>=2.32",
    "boto3>=1.35",           # active-sink discovery for per_entity audience SQL resolution
).env(
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("core.parallel_client")

# Own Modal app per workflow (see enrich.py) — distinct name so deploys don't clobber.
app = modal.App("parallel-deep-research", image=image)


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


# ── Audience resolution (shared shape with enrich.py; deep_research keeps its own copy so
#    the two workers stay independently deployable — the mandate keeps entrypoints separate). ─
def _resolve_audience_companies(audience_id: str, limit: int) -> list[dict]:
    import psycopg

    dsn = _hqx_dsn()
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set — cannot resolve the audience.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT source_sql, result_key FROM corex.audience WHERE audience_id = %s",
            (audience_id,),
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"unknown audience_id {audience_id!r} in corex.audience")
    source_sql, result_key = row[0], row[1]
    if result_key != "company_id":
        raise RuntimeError(
            f"per_entity deep research requires a company_id audience; audience {audience_id} "
            f"resolves to {result_key!r}."
        )
    rows = _run_lance_sql(source_sql, limit=limit)
    out: list[dict] = []
    for r in rows:
        cid = r.get("company_id")
        if cid is None:
            continue
        out.append({"company_id": str(cid), "company_name": r.get("company_name"),
                    "normalized_domain": r.get("normalized_domain")})
    return out


def _run_lance_sql(sql: str, *, limit: int) -> list[dict]:
    import duckdb
    import lance

    so = _r2_storage_options()
    con = duckdb.connect(":memory:")
    try:
        con.execute("PRAGMA threads=4;")
        for name, uri in _discover_active_datasets().items():
            if _names_dataset(sql, name):
                try:
                    con.register(name, lance.dataset(uri, storage_options=so))
                except Exception as exc:  # noqa: BLE001
                    print(f"  WARN could not register {name}: {exc}")
        rel = con.execute(sql)
        cols = [d[0] for d in rel.description] if rel.description else []
        return [dict(zip(cols, r)) for r in rel.fetchmany(int(limit))]
    finally:
        con.close()


def _discover_active_datasets() -> dict[str, str]:
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    s3 = boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto",
        config=Config(request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"),
    )
    bucket = "data-sink"

    def _children(prefix: str) -> list[str]:
        names, token = [], None
        while True:
            kw = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for cp in resp.get("CommonPrefixes", []):
                names.append(cp["Prefix"][len(prefix):].rstrip("/"))
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
        return names

    out: dict[str, str] = {}
    pending = [("", "active/")]
    depth = 0
    while pending and depth <= 2:
        nxt = []
        for rel, full in pending:
            kids = _children(full)
            if "_versions" in kids:
                if rel:
                    out[rel] = f"s3://{bucket}/active/{rel}/"
                continue
            for child in kids:
                if child.startswith("_"):
                    continue
                child_rel = f"{rel}/{child}" if rel else child
                nxt.append((child_rel, f"{full}{child}/"))
        pending = nxt
        depth += 1
    return out


def _names_dataset(sql: str, name: str) -> bool:
    import re

    return re.search(r"(?<![\w./])" + re.escape(name) + r"(?![\w./])", sql, re.IGNORECASE) is not None


# ── Blocking-result loop (§0) ────────────────────────────────────────────────────────────
def _await_report(run_id: str) -> tuple[str, str | None, list]:
    """Block on GET /result (§0), re-calling on 408 until the per-run deadline. Returns
    (status, report_md, basis). report_md is pulled from the text output content."""
    import time

    deadline = time.monotonic() + PER_RUN_DEADLINE_S
    while time.monotonic() < deadline:
        res = get_task_result(run_id, timeout=RESULT_HOLD_S)
        if res.get("_status") == 408:
            # Still active. The blocking GET normally holds for ~RESULT_HOLD_S, but if the server
            # returns 408 early, sleep briefly before re-calling so this never becomes a tight
            # loop. Bounded by the per-run deadline (the while condition).
            time.sleep(RESULT_408_BACKOFF_S)
            continue
        run_obj = res.get("run") or {}
        output = res.get("output") or {}
        status = str(run_obj.get("status") or "completed").lower()
        # text output: content is the report string; basis carries citations.
        content = output.get("content") if isinstance(output, dict) else None
        if isinstance(content, dict):  # auto mode may nest
            content = content.get("text") or content.get("report") or _json_dump(content)
        basis = output.get("basis") if isinstance(output, dict) else []
        return status, (content if isinstance(content, str) else _json_dump(content)), (basis or [])
    return "timeout", None, []


def _json_dump(v) -> str | None:
    import json

    if v is None:
        return None
    return v if isinstance(v, str) else json.dumps(v, default=str)


# Fixed column set for the parallel_research dataset — the SAME schema for both grains. Built
# from an explicit all-``string`` pyarrow schema so a topic-grain row (company_id /
# normalized_domain / basis = None → an inferred Arrow ``null`` column) can NEVER collide with a
# later per_entity write (where those are populated → ``string``). Without this, the type change
# makes merge_insert raise and the old bare-except clobbered the dataset via overwrite.
_RESEARCH_COLUMNS = (
    "run_id", "company_id", "normalized_domain", "objective", "report_md",
    "basis", "processor", "spec_id", "grain", "created_at",
)


def _is_dataset_absent(exc: Exception) -> bool:
    """True iff ``lance.dataset(uri)`` failed because the dataset does not exist yet (vs. a
    credential / network / corrupt-manifest error, which must propagate)."""
    msg = str(exc).lower()
    needles = ("not found", "does not exist", "no such", "dataset not", "table not",
               "no version", "not a dataset", "could not find")
    return any(n in msg for n in needles)


# ── Lance write per grain ────────────────────────────────────────────────────────────────
def _write_research(rows: list[dict], grain: str, so: dict) -> int:
    """Write report rows to the parallel_research dataset, merge_insert on the grain key
    (company_id for per_entity, run_id for topic). The dataset is CREATED only when genuinely
    absent; a merge_insert failure RAISES (terminal) and is never swallowed into an overwrite."""
    import lance
    import pyarrow as pa

    if not rows:
        return 0
    key = "company_id" if grain == "per_entity" else "run_id"
    # Explicit all-string schema, deterministic column order — identical for every grain.
    schema = pa.schema([pa.field(c, pa.string()) for c in _RESEARCH_COLUMNS])

    def _s(v):
        return None if v is None else (v if isinstance(v, str) else str(v))

    columns = {c: pa.array([_s(r.get(c)) for r in rows], type=pa.string()) for c in _RESEARCH_COLUMNS}
    table = pa.table(columns, schema=schema)

    # Probe existence: dataset-NOT-FOUND → create; any other open error propagates.
    exists = True
    try:
        lance.dataset(RESEARCH_URI, storage_options=so)
    except Exception as exc:  # noqa: BLE001
        if _is_dataset_absent(exc):
            exists = False
        else:
            raise
    if not exists:
        lance.write_dataset(
            table, RESEARCH_URI, mode="create", data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
    else:
        ds = lance.dataset(RESEARCH_URI, storage_options=so)
        ds.merge_insert(key).when_matched_update_all().when_not_matched_insert_all().execute(table)

    # BTREE on both possible keys (idempotent; the populated one is the live key).
    ds = lance.dataset(RESEARCH_URI, storage_options=so)
    names = [f.name for f in ds.schema]
    for col in ("company_id", "run_id"):
        if col in names:
            try:
                ds.create_scalar_index(col, index_type="BTREE", replace=True)
                print(f"  BTREE  ✓ {col}")
            except Exception as exc:  # noqa: BLE001
                print(f"  BTREE  ✗ {col}: {exc}")
    return table.num_rows


def _record_run(row: dict) -> None:
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = _hqx_dsn()
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.parallel_runs write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.parallel_runs
                    (spec_id, workflow, run_kind, group_id, audience_id, idempotency_key,
                     requested, skipped_no_domain, completed, failed, failed_company_ids,
                     processor, cost_estimate, cost_cap, dataset_uri, status, error,
                     started_at, completed_at)
                VALUES (%(spec_id)s, 'deep_research', %(run_kind)s, %(group_id)s, %(audience_id)s,
                        %(idempotency_key)s, %(requested)s, %(skipped_no_domain)s, %(completed)s,
                        %(failed)s, %(failed_company_ids)s, %(processor)s, %(cost_estimate)s,
                        %(cost_cap)s, %(dataset_uri)s, %(status)s, %(error)s, %(started_at)s,
                        %(completed_at)s)
                """,
                {**row, "failed_company_ids": Jsonb(row.get("failed_company_ids") or [])},
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.parallel_runs write failed: {exc}")


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


# ── The dispatched worker ────────────────────────────────────────────────────────────────
@app.function(
    secrets=[
        modal.Secret.from_name("parallel-api"),
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 60,
    memory=4096,
    cpu=2.0,
)
def deep_research(
    trigger_callback_url: str | None = None,
    run_id: str | None = None,
    spec_id: str = "",
    objective: str = "",
    grain: str = "topic",
    audience_id: str | None = None,
    processor: str = "base",
    output_type: str = "text",
    run_kind: str = "test",
    max_runs: int = 0,
    idempotency_key: str | None = None,
    cost_cap: float | None = None,
) -> dict:
    """Run Parallel deep research → cited report(s) in the parallel_research Lance dataset.

    grain="topic": one run for ``objective`` → one row keyed run_id.
    grain="per_entity": resolve ``audience_id`` → one run per company (objective steers what
    to research about each) → one row per company_id.
    ``processor`` ∈ {lite,base,core,pro}; ultra REJECTED (deferred). ``output_type`` ∈
    {text,auto}. Terminal → ops.parallel_runs + Trigger callback."""
    import datetime as dt
    import uuid

    started = dt.datetime.now(dt.timezone.utc)
    run_id = run_id or uuid.uuid4().hex
    spec = (spec_id or "").strip() or f"research_{run_id[:8]}"
    proc = (processor or "base").strip().lower()
    grain = (grain or "topic").strip().lower()
    counts = {"requested": 0, "skipped_no_domain": 0, "completed": 0, "failed": 0}
    failed_ids: list[str] = []
    status, error = "failed", None

    def _terminal(st, *, reraise_msg=None, **extra):
        finished = dt.datetime.now(dt.timezone.utc)
        _record_run({
            "spec_id": spec, "run_kind": run_kind, "group_id": run_id,
            "audience_id": audience_id or None, "idempotency_key": idempotency_key,
            "requested": counts["requested"], "skipped_no_domain": counts["skipped_no_domain"],
            "completed": counts["completed"], "failed": counts["failed"],
            "failed_company_ids": failed_ids, "processor": proc, "cost_estimate": None,
            "cost_cap": cost_cap, "dataset_uri": RESEARCH_URI, "status": st, "error": error,
            "started_at": started, "completed_at": finished,
        })
        _post_callback(trigger_callback_url, {
            "status": st, "run_id": run_id, "workflow": "deep_research", "spec_id": spec,
            "grain": grain, "dataset_uri": RESEARCH_URI, "requested": counts["requested"],
            "completed": counts["completed"], "failed": counts["failed"],
            "failed_company_ids": failed_ids[:200], **extra,
        })
        if reraise_msg:
            raise RuntimeError(reraise_msg)

    try:
        # Guardrails — fail before any spend. ULTRA REJECTION (the mandate's guard).
        if proc in REJECTED_PROCESSORS:
            status, error = "rejected", (
                f"processor {proc!r} is not supported by the deep-research workflow v1. "
                "ultra runs for hours and requires Parallel's per-run webhook → trigger-token "
                "path (deferred per §0/§9). Use up to 'pro'."
            )
            return _terminal(status) or {"status": status, "error": error}
        if proc not in ALLOWED_PROCESSORS:
            status, error = "rejected", f"processor must be one of {ALLOWED_PROCESSORS}"
            return _terminal(status) or {"status": status, "error": error}
        if grain not in ("topic", "per_entity"):
            status, error = "rejected", "grain must be 'topic' or 'per_entity'"
            return _terminal(status) or {"status": status, "error": error}
        if not (objective or "").strip():
            status, error = "rejected", "objective is required"
            return _terminal(status) or {"status": status, "error": error}

        spec_out = text_output(objective) if output_type != "auto" else auto_output()
        task_spec = {"output_schema": spec_out}
        created = dt.datetime.now(dt.timezone.utc).isoformat()
        so = _r2_storage_options()
        rows: list[dict] = []

        if grain == "topic":
            counts["requested"] = 1
            run = create_task_run(input_=objective, processor=proc, task_spec=task_spec)
            rid = run.get("run_id")
            if not rid:
                status, error = "failed", f"create returned no run_id: {str(run)[:200]}"
                return _terminal("failed", reraise_msg=error)
            st, report_md, basis = _await_report(rid)
            if st == "completed" and report_md:
                rows.append({"run_id": rid, "company_id": None, "normalized_domain": None,
                             "objective": objective, "report_md": report_md,
                             "basis": _json_dump(basis), "processor": proc, "spec_id": spec,
                             "grain": grain, "created_at": created})
                counts["completed"] = 1
            else:
                counts["failed"] = 1
                failed_ids.append(rid)
        else:  # per_entity
            if not audience_id:
                status, error = "rejected", "grain=per_entity requires audience_id"
                return _terminal(status) or {"status": status, "error": error}
            limit = max_runs if max_runs and max_runs > 0 else MAX_ROWS_PER_FILE
            companies = _resolve_audience_companies(audience_id, limit=limit)
            counts["requested"] = len(companies)
            for c in companies:
                dom = _bare_domain(c.get("normalized_domain"))
                if not dom:
                    counts["skipped_no_domain"] += 1
                ent_input = {"company_name": c.get("company_name") or "", "objective": objective}
                if dom:
                    ent_input["company_website"] = dom
                run = create_task_run(input_=ent_input, processor=proc, task_spec=task_spec)
                rid = run.get("run_id")
                if not rid:
                    counts["failed"] += 1
                    failed_ids.append(c["company_id"])
                    continue
                st, report_md, basis = _await_report(rid)
                if st == "completed" and report_md:
                    rows.append({"run_id": rid, "company_id": c["company_id"],
                                 "normalized_domain": c.get("normalized_domain"),
                                 "objective": objective, "report_md": report_md,
                                 "basis": _json_dump(basis), "processor": proc, "spec_id": spec,
                                 "grain": grain, "created_at": created})
                    counts["completed"] += 1
                else:
                    counts["failed"] += 1
                    failed_ids.append(c["company_id"])

        if rows:
            _write_research(rows, grain, so)
            print(f"  landed {len(rows)} report row(s) → {RESEARCH_URI} (grain={grain})")

        status = "success" if counts["failed"] == 0 and counts["completed"] > 0 else (
            "partial" if counts["completed"] > 0 else "failed")
        error = None if status == "success" else (error or f"{counts['failed']} run(s) did not complete")
        if status == "failed":
            return _terminal("failed", reraise_msg=f"deep research produced no report: {error}")
        return _terminal(status) or {"status": status, "run_id": run_id, "grain": grain,
                                     "completed": counts["completed"], "failed": counts["failed"]}

    except Exception as exc:  # noqa: BLE001
        status, error = "failed", str(exc)
        _terminal("failed", reraise_msg=f"parallel deep research failed: {exc}")
