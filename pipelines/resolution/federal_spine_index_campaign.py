"""Compute worker — Federal Spine Physical Indexing Campaign.

A one-shot MAINTENANCE worker (not a feed): it builds ``BTREE`` scalar indices
IN PLACE on already-committed Lance datasets in R2, appending index files only —
no re-ingest, no wipe, no duplication, no catalog round-trip. It spans four
domains (USAspending, HMDA, SAM↔FMCSA / SAM↔PDL bridges), so it deliberately
lives in the cross-source ``resolution`` domain rather than any single feed.

Two index-build tiers (chosen per dataset by ::run vs ::run_staged):

  1. DIRECT-R2 (::run) — ``lance.dataset(uri, storage_options=so)
     .create_scalar_index(col, "BTREE")``. Reads the column via R2 range GETs,
     sorts in-RAM (``LANCE_BYPASS_SPILLING``, 64 GiB), commits index files straight
     to R2. No scratch disk. The dominant fleet pattern (HMDA, FMCSA, CMS, NMLS,
     FEC, GLEIF, SBA, CA-UCC, CO/NY-SOS), and the first choice for everything that
     fits. Verified here on datasets up to transaction_search_fpds @107M rows.

  2. /tmp-STAGED, APPEND-ONLY (::run_staged) — required once a single scalar-index
     ``page_data.lance`` crosses R2's multipart part-size escalation threshold
     (empirically ~100M+ rows on a load-bearing column): a direct-R2 write then
     trips ``400 InvalidPart`` ("all non-trailing parts must have the same length")
     and Lance exposes no part-size knob. The dataset is staged to the DEFAULT /tmp
     container disk (NOT an ``ephemeral_disk`` override — its 512 GiB floor forces
     preemptible spot capacity; NOT a Modal Volume — its FUSE layer rejects Lance's
     commit rename), the index is built on the local copy (local FS has no multipart
     rule), then ONLY the new files (``_indices/<uuid>/``, the new
     ``_versions/<n>.manifest``, ``_transactions/*.txn``) are uploaded via boto3
     (uniform parts → R2-compliant). Data files are never rewritten or deleted.
     Applies to transaction_search_fabs @128.8M and hmda_lar @168M.

Targets (operator-authorized 2026-06-01) — BTREE only, schema-gated at runtime:
  USAspending : recipient_profile, recipient_lookup, award_search,
                transaction_search_fpds, transaction_search_fabs, subaward_search
                → every present uei variant + cage_code + naics(_code); *_description
                  is excluded (free text, not a resolution key)
  HMDA        : hmda_lar → lei
  Crosswalk   : bridge_sam_fmcsa_domain → normalized_domain, mc_number
                bridge_sam_pdl          → normalized_domain

    modal run …::probe                          # read-only ground truth (schema/indices/size)
    modal run …::run    [--group safe|giant] [--only <substr>] [--cols a,b]   # direct-R2
    modal run …::run_staged --only <fabs|hmda_lar> [--cols …]                 # /tmp-staged giants
    modal run …::verify                         # list_indices() sweep → table
    modal run …::cleanup                        # abort dangling multipart uploads
    modal run …::prune  [--only <substr>] [--execute]   # delete unreferenced _indices orphans
"""

from __future__ import annotations

import os

import modal

# ── System-of-record (R2). All targets live under data-sink/active/. ──────────
BUCKET = "data-sink"

# Logical name → R2 ``s3://`` URI (system of record). ``bridge_sam_pdl`` has no
# committed pipeline; the probe confirms whether it exists before any mutation.
DATASETS: dict[str, str] = {
    "usaspending/recipient_profile":       "s3://data-sink/active/usaspending/recipient_profile/",
    "usaspending/recipient_lookup":        "s3://data-sink/active/usaspending/recipient_lookup/",
    "usaspending/award_search":            "s3://data-sink/active/usaspending/award_search/",
    "usaspending/transaction_search_fpds": "s3://data-sink/active/usaspending/transaction_search_fpds/",
    "usaspending/transaction_search_fabs": "s3://data-sink/active/usaspending/transaction_search_fabs/",
    "usaspending/subaward_search":         "s3://data-sink/active/usaspending/subaward_search/",
    "hmda_lar":                            "s3://data-sink/active/hmda_lar/",
    "bridge_sam_fmcsa_domain":             "s3://data-sink/active/bridge_sam_fmcsa_domain/",
    "bridge_sam_pdl":                      "s3://data-sink/active/bridge_sam_pdl/",
}

# R2 key prefix per dataset (for the boto3 size/object probe — no s3:// scheme).
ACTIVE_PREFIXES: dict[str, str] = {
    name: uri.replace(f"s3://{BUCKET}/", "") for name, uri in DATASETS.items()
}

# Column-name PATTERNS the directive targets. The effective per-dataset plan is
# the intersection of these with the committed schema (computed at runtime — an
# absent column is skipped, never fatal). UEI appears under many variant names
# across the FPDS/FABS/award/subaward search tables; matched by substring.
UEI_SUBSTR = "uei"                       # uei, recipient_uei, awardee_or_recipient_uei,
                                         # sub_awardee_or_recipient_uei, subawardee_uei,
                                         # ultimate_parent_uei, sub_ultimate_parent_uei, …
NAICS_SUBSTR = "naics"                   # naics, naics_code, sub_naics, …
EXACT_TARGETS = {"cage_code", "lei", "normalized_domain", "mc_number"}

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "lancedb>=0.15",
    "pylance>=7",            # provides ``import lance``; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",           # R2 object/byte size probe
    "psycopg[binary]>=3.2",  # ops.* terminal-state ledger
)

app = modal.App("federal-spine-index-campaign", image=image)

# Scratch for the over-threshold giants (transaction_search_fabs, hmda_lar) — staged to
# the DEFAULT container disk at /tmp. Why not the alternatives:
#   • ephemeral_disk has a hard 512 GiB floor (Modal rejects anything smaller) and a
#     512 GiB request forces preemptible spot capacity — the directive forbids it.
#   • a Modal Volume gives unlimited space but Lance's commit does an atomic rename the
#     Volume FUSE layer rejects ("Operation not permitted, os error 1").
#   • /dev/shm tmpfs is capped at 16 GiB regardless of the memory request.
# The default /tmp overlay is a real local FS (rename works) on stable, non-preemptible
# capacity and empirically holds the staged giants (hmda 43.5 GiB + indices verified).
STAGE_ROOT = "/tmp/stage"


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


def _s3_client():
    """boto3 S3 client for R2 — checksum behaviour forced to ``when_required``
    (botocore's default flexible-checksum validation otherwise 4xxs against R2)."""
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


def _prefix_size(s3, prefix: str) -> tuple[int, int, int]:
    """(object_count, total_bytes, index_object_count) under an R2 key prefix.
    ``_indices/`` objects are counted separately — they are the committed scalar
    index pages, the storage signal for what is already built."""
    objs = idx_objs = total = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            objs += 1
            total += o["Size"]
            if "/_indices/" in o["Key"] or o["Key"].rstrip("/").endswith("_indices"):
                idx_objs += 1
    return objs, total, idx_objs


def _list_committed_indices(ds) -> list[dict]:
    """Best-effort read of committed scalar indices. Tolerant of pylance
    return-shape drift (dict vs object; list_indices vs list_indexes)."""
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


def _target_columns(schema_names: list[str]) -> list[str]:
    """Directive target columns actually present in a committed schema:
    any uei variant (substring), any naics variant (substring), and the exact
    keys cage_code / lei / normalized_domain / mc_number."""
    present = []
    for n in schema_names:
        low = n.lower()
        if UEI_SUBSTR in low or NAICS_SUBSTR in low or low in EXACT_TARGETS:
            present.append(n)
    return present


def _indexed_columns(committed: list[dict]) -> set[str]:
    """Set of column names that already carry a committed scalar index."""
    cols: set[str] = set()
    for ix in committed:
        fields = ix.get("fields") if isinstance(ix, dict) else None
        if isinstance(fields, (list, tuple)):
            cols.update(str(f) for f in fields)
    return cols


# ─────────────────────────── Probe (read-only) ───────────────────────────
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 30,
    memory=16384,
    cpu=4.0,
)
def probe_all() -> list[dict]:
    """Read-only ground-truth sweep over every candidate dataset: existence,
    row count, full schema (name:type), committed indices, R2 object/byte size,
    and the directive target columns present vs. already indexed. No mutation."""
    import lance

    so = _r2_storage_options()
    s3 = _s3_client()
    report: list[dict] = []
    for name, uri in DATASETS.items():
        row: dict = {"dataset": name, "uri": uri}
        try:
            ds = lance.dataset(uri, storage_options=so)
        except Exception as exc:  # noqa: BLE001 — absent / not found
            objs, total, _ = _prefix_size(s3, ACTIVE_PREFIXES[name])
            row.update({"exists": False, "open_error": str(exc)[:300],
                        "r2_objects": objs, "r2_bytes": total})
            report.append(row)
            print(f"✗ {name}: OPEN FAILED ({objs} R2 objects) — {str(exc)[:160]}")
            continue
        try:
            schema_names = list(ds.schema.names)
            committed = _list_committed_indices(ds)
            targets = _target_columns(schema_names)
            already = _indexed_columns(committed)
            objs, total, idx_objs = _prefix_size(s3, ACTIVE_PREFIXES[name])
            row.update({
                "exists": True,
                "rows": ds.count_rows(),
                "n_columns": len(schema_names),
                "schema": [f"{f.name}:{f.type}" for f in ds.schema],
                "committed_indices": committed,
                "target_columns_present": targets,
                "targets_missing_index": [c for c in targets if c not in already],
                "r2_objects": objs,
                "r2_bytes": total,
                "r2_index_objects": idx_objs,
            })
            print(f"✓ {name}: {row['rows']:,} rows, {len(schema_names)} cols, "
                  f"{len(committed)} indices, {total / 1024**3:.2f} GiB / {objs} objs | "
                  f"targets={targets} | missing_idx={row['targets_missing_index']}")
        except Exception as exc:  # noqa: BLE001
            row.update({"exists": True, "inspect_error": str(exc)[:300]})
            print(f"! {name}: opened but inspect failed — {str(exc)[:160]}")
        report.append(row)
    return report


@app.local_entrypoint()
def probe() -> None:
    """Read-only ground-truth sweep (existence, schema, indices, size)."""
    import json

    print(json.dumps(probe_all.remote(), indent=2, default=str))


# ───────────────────────── Index plan (probe-derived, exact) ─────────────────
# BTREE only. Exact committed-schema column names confirmed by ::probe on
# 2026-06-01. ``*_description`` is DELIBERATELY EXCLUDED — it is free text, not a
# resolution key (the directive's "naics (or naics_code)" means the CODE). Every
# uei variant present is indexed (uei / recipient_uei / parent_uei /
# awardee_or_recipient_uei / ultimate_parent_uei / sub_* …). The plan is still
# runtime-gated against the live schema, so any drift is skipped, never fatal.
INDEX_PLAN: dict[str, list[str]] = {
    "usaspending/recipient_profile":       ["uei", "parent_uei"],
    "usaspending/recipient_lookup":        ["uei", "parent_uei"],
    "usaspending/award_search":            ["recipient_uei", "parent_uei", "naics_code"],
    "usaspending/transaction_search_fpds": ["recipient_uei", "parent_uei", "naics_code", "cage_code"],
    "usaspending/transaction_search_fabs": ["recipient_uei", "parent_uei", "naics_code", "cage_code"],
    "usaspending/subaward_search":         ["awardee_or_recipient_uei", "ultimate_parent_uei",
                                            "sub_awardee_or_recipient_uei", "sub_ultimate_parent_uei",
                                            "naics", "sub_naics"],
    "hmda_lar":                            ["lei"],
    "bridge_sam_fmcsa_domain":             ["normalized_domain", "mc_number"],
    "bridge_sam_pdl":                      ["normalized_domain"],
}

# Groups for the ::run direct-R2 entrypoint. The giants whose index files stay under
# R2's multipart escalation threshold (award_search @78M, transaction_search_fpds @107M)
# succeed via direct-R2; the over-threshold pair (transaction_search_fabs @128.8M,
# hmda_lar @168M) must go through ::run_staged instead (see index_dataset_staged).
SAFE_GROUP = [
    "usaspending/recipient_profile", "usaspending/recipient_lookup",
    "usaspending/subaward_search", "bridge_sam_fmcsa_domain", "bridge_sam_pdl",
]
GIANT_GROUP = [
    "usaspending/award_search", "usaspending/transaction_search_fpds",
    "usaspending/transaction_search_fabs", "hmda_lar",
]

# Per-column retry on the direct-R2 path. Recovers genuine transient R2 errors (a dropped
# CompleteMultipartUpload) via a fresh dataset handle + backoff. It does NOT rescue the
# deterministic over-threshold InvalidPart — those datasets route to ::run_staged.
INDEX_ATTEMPTS = 3
RETRY_BACKOFF_S = 25


def _record_run(dataset, rows, built, skipped, failed, started_at, completed_at) -> None:
    """Best-effort terminal-state row → ops.federal_spine_index_runs (psycopg).
    Creates schema/table lazily (idempotent). Never masks the index work."""
    import json

    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ops.federal_spine_index_runs (
                    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    dataset       TEXT        NOT NULL,
                    rows_indexed  BIGINT,
                    built         TEXT[],
                    skipped       TEXT[],
                    failed        JSONB,
                    status        TEXT        NOT NULL,
                    started_at    TIMESTAMPTZ NOT NULL,
                    completed_at  TIMESTAMPTZ NOT NULL
                )
                """
            )
            status = "success" if not failed else ("partial" if built else "error")
            cur.execute(
                """
                INSERT INTO ops.federal_spine_index_runs
                    (dataset, rows_indexed, built, skipped, failed, status, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (dataset, rows, built, skipped, json.dumps(failed), status, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* write failed: {exc}")


# ──────────────── Index build — direct-R2 in-place mutation ────────────────
@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
        # BTREE training sorts the column; bypass Lance's bounded spill-to-disk
        # ExternalSorter (under-sizes its pool → exhausts on 100M-row columns).
        # The whole sort runs in-RAM in the 32/64 GiB container.
        modal.Secret.from_dict({"LANCE_BYPASS_SPILLING": "true"}),
    ],
    timeout=60 * 180,
    memory=65536,        # 64 GiB — sized for the in-RAM BTREE sort of the 128.8M-row
                         # giant columns (LANCE_BYPASS_SPILLING). Over-provisions the
                         # small targets harmlessly; memory does NOT force spot capacity.
    cpu=8.0,
    # NO ephemeral_disk: a large ephemeral_disk request is what forced the giant
    # ingest path onto preemptible spot capacity. create_scalar_index reads only
    # the target column from R2 (columnar range GETs) and writes index files back
    # in place — no local dataset staging, so no scratch disk is needed.
)
def index_dataset(name: str, cols: str = "") -> dict:
    """Build BTREE scalar indices IN PLACE on one R2 dataset via the direct
    create_scalar_index() mutation path. Per-column: skip if absent or already
    indexed; otherwise build and record the outcome. A per-column failure (e.g.
    R2 multipart escalation on a giant's high-cardinality column) is captured,
    never fatal to the rest. Columns on one dataset are built SEQUENTIALLY —
    each index commit bumps the manifest version, so concurrent builds on the
    same dataset would conflict."""
    import datetime as dt
    import time

    import lance

    if name not in DATASETS:
        return {"dataset": name, "status": "unknown_dataset"}
    uri = DATASETS[name]
    so = _r2_storage_options()
    started_at = dt.datetime.now(dt.timezone.utc)

    ds = lance.dataset(uri, storage_options=so)
    present = set(ds.schema.names)
    already = _indexed_columns(_list_committed_indices(ds))
    rows = ds.count_rows()

    plan = INDEX_PLAN.get(name, [])
    if cols:  # explicit column override (e.g. decisive single-column giant test)
        plan = [c.strip() for c in cols.split(",") if c.strip()]  # schema-gated below

    built: list[str] = []
    skipped: list[str] = []
    failed: list[dict] = []
    print(f"▶ {name}: {rows:,} rows | plan={plan} | already_indexed={sorted(already)}")
    for col in plan:
        if col not in present:
            skipped.append(f"{col}(absent)")
            print(f"  – skip {col} (absent from schema)")
            continue
        if col in already:
            skipped.append(f"{col}(already-indexed)")
            print(f"  – skip {col} (already indexed)")
            continue
        # Per-column retry: R2 can transiently drop/throttle CompleteMultipartUpload
        # under sustained index-write load (observed mid-batch on the 128.8M-row
        # giant). Re-open the dataset each attempt for a fresh object_store client /
        # connection pool, with linear backoff. A failed index commit is atomic — the
        # manifest is untouched — so a retry just writes a new _indices/<uuid>/.
        t0 = dt.datetime.now(dt.timezone.utc)
        last_exc: Exception | None = None
        for attempt in range(1, INDEX_ATTEMPTS + 1):
            try:
                ds.create_scalar_index(col, index_type="BTREE", replace=True)
                secs = (dt.datetime.now(dt.timezone.utc) - t0).total_seconds()
                built.append(col)
                print(f"  BTREE ✓ {col}  ({secs:.0f}s, attempt {attempt})")
                last_exc = None
                break
            except Exception as exc:  # noqa: BLE001 — one column must not abort the rest
                last_exc = exc
                print(f"  BTREE … {col} attempt {attempt}/{INDEX_ATTEMPTS} failed: {str(exc)[:200]}")
                if attempt < INDEX_ATTEMPTS:
                    time.sleep(RETRY_BACKOFF_S * attempt)
                    ds = lance.dataset(uri, storage_options=so)  # fresh client + conn pool
        if last_exc is not None:
            failed.append({"col": col, "error": str(last_exc)[:900]})
            print(f"  BTREE ✗ {col} (after {INDEX_ATTEMPTS} attempts): {str(last_exc)[:280]}")

    # Re-open to read committed truth straight from the R2 manifest.
    committed_after = _list_committed_indices(lance.dataset(uri, storage_options=so))
    completed_at = dt.datetime.now(dt.timezone.utc)
    _record_run(name, rows, built, skipped, failed, started_at, completed_at)
    status = "success" if not failed else ("partial" if built else "error")
    return {"dataset": name, "rows": rows, "status": status, "built": built,
            "skipped": skipped, "failed": failed, "committed_indices": committed_after}


@app.local_entrypoint()
def run(group: str = "", only: str = "", cols: str = "") -> None:
    """Build the planned BTREE indices in place.

    --group safe|giant   restrict to the small/medium set or the 4 giants
    --only  <substr>     restrict to datasets whose name contains <substr>
    --cols  a,b          restrict to these columns (decisive single-column tests)
    """
    import json

    targets = list(DATASETS)
    if group == "safe":
        targets = list(SAFE_GROUP)
    elif group == "giant":
        targets = list(GIANT_GROUP)
    if only:
        targets = [t for t in targets if only in t]
    if not targets:
        print(f"No datasets matched group={group!r} only={only!r}")
        return
    for name in targets:
        res = index_dataset.remote(name, cols)
        print(json.dumps(res, indent=2, default=str))


# ──────────── Index build — /tmp-staged (over-threshold giants) ────────────
@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
        modal.Secret.from_dict({"LANCE_BYPASS_SPILLING": "true"}),
    ],
    timeout=60 * 240,
    memory=65536,    # 64 GiB — high memory (per directive) carries the in-RAM BTREE sort.
                     # Staging goes to the default /tmp overlay (NOT /dev/shm, capped at
                     # 16 GiB); NO ephemeral_disk override (its 512 GiB floor forces spot),
                     # so the worker stays on stable capacity.
    cpu=8.0,
)
def index_dataset_staged(name: str, cols: str = "", refresh: bool = False) -> dict:
    """Build BTREE indices on a dataset whose index files exceed R2's multipart
    escalation threshold (direct-R2 create_scalar_index → 400 InvalidPart, proven on
    transaction_search_fabs @128.8M and hmda_lar @168M).

    Honors the directive's intent despite the physics:
      • DEFAULT /tmp container disk (no ephemeral_disk override → stable, non-spot). A
        Modal Volume can't substitute: Lance's commit rename is rejected by the Volume
        FUSE layer (os error 1); /dev/shm tmpfs caps at 16 GiB. /tmp is a real local FS.
      • APPEND-ONLY → only the freshly-written files (the new _indices/<uuid>/, the new
        _versions/<n>.manifest, the new _transactions/*.txn) are uploaded to R2. Existing
        data files are never rewritten and NOTHING is deleted — no wipe, no duplication.
      • The index is committed via dataset.create_scalar_index() (on the staged copy);
        because the staged copy is byte-identical to R2, the index's fragment row
        addresses match the R2 dataset exactly, so the uploaded index is valid there.
    Local FS has no multipart rule, so the index file size is irrelevant; boto3's
    s3transfer uploads the new files with uniform parts (R2-compliant)."""
    import datetime as dt
    import os
    import shutil

    import lance

    if name not in DATASETS:
        return {"dataset": name, "status": "unknown_dataset"}
    uri = DATASETS[name]
    prefix = ACTIVE_PREFIXES[name]
    so = _r2_storage_options()
    s3 = _s3_client()
    local_ds = f"{STAGE_ROOT}/{name.replace('/', '__')}"
    started_at = dt.datetime.now(dt.timezone.utc)

    # 1. Mirror R2 → /tmp. Record the pre-existing R2 keys so the upload step can
    #    skip them (append-only). --refresh forces a clean re-stage.
    if refresh:
        shutil.rmtree(local_ds, ignore_errors=True)
    os.makedirs(local_ds, exist_ok=True)
    r2_before: dict[str, int] = {}
    staged = reused = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if not rel:
                continue
            r2_before[rel] = o["Size"]
            dst = os.path.join(local_ds, rel)
            if os.path.exists(dst) and os.path.getsize(dst) == o["Size"]:
                reused += 1
                continue
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            s3.download_file(BUCKET, o["Key"], dst)
            staged += 1
    print(f"staged {name}: {staged} downloaded + {reused} cached "
          f"({sum(r2_before.values()) / 1024**3:.1f} GiB) → {local_ds}")

    # 2. Build indices on the LOCAL copy (no storage_options → local FS, no multipart).
    ds = lance.dataset(local_ds)
    present = set(ds.schema.names)
    rows = ds.count_rows()
    plan = INDEX_PLAN.get(name, [])
    if cols:
        plan = [c.strip() for c in cols.split(",") if c.strip()]
    built: list[str] = []
    skipped: list[str] = []
    failed: list[dict] = []
    print(f"▶ {name} (staged): {rows:,} rows | plan={plan}")
    for col in plan:
        if col not in present:
            skipped.append(f"{col}(absent)")
            print(f"  – skip {col} (absent)")
            continue
        t0 = dt.datetime.now(dt.timezone.utc)
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(col)
            print(f"  BTREE ✓ {col}  ({(dt.datetime.now(dt.timezone.utc) - t0).total_seconds():.0f}s, local)")
        except Exception as exc:  # noqa: BLE001
            failed.append({"col": col, "error": str(exc)[:900]})
            print(f"  BTREE ✗ {col}: {str(exc)[:240]}")

    # 3. Append-only upload: push only files absent-from / size-changed-vs R2. Lance
    #    files are immutable per version, so a size match means identical content. We
    #    never issue a delete — existing data files stay exactly as they were.
    uploaded = 0
    up_bytes = 0
    for root, _dirs, files in os.walk(local_ds):
        for fn in files:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_ds).replace(os.sep, "/")
            sz = os.path.getsize(lp)
            if r2_before.get(rel) == sz:
                continue  # already in R2, unchanged
            s3.upload_file(lp, BUCKET, prefix + rel)
            uploaded += 1
            up_bytes += sz
    print(f"appended {uploaded} new files ({up_bytes / 1024**3:.2f} GiB) → s3://{BUCKET}/{prefix}")

    # 4. Verify straight from the R2 manifest.
    committed = _list_committed_indices(lance.dataset(uri, storage_options=so))
    completed_at = dt.datetime.now(dt.timezone.utc)
    _record_run(name, rows, built, skipped, failed, started_at, completed_at)
    status = "success" if not failed else ("partial" if built else "error")
    return {"dataset": name, "rows": rows, "status": status, "built": built,
            "skipped": skipped, "failed": failed, "staged_files": staged,
            "reused_files": reused, "uploaded_files": uploaded,
            "committed_indices": committed}


@app.local_entrypoint()
def run_staged(only: str = "", cols: str = "", refresh: bool = False) -> None:
    """/tmp-staged index build for the over-threshold giants (no spot, append-only).
    --only <substr>  restrict datasets (e.g. fabs, hmda_lar)
    --cols a,b       explicit columns (e.g. hmda repair: lei,action_taken,property_state,county_code)
    --refresh        force a clean re-stage from R2
    """
    import json

    targets = [t for t in DATASETS if (only in t if only else True)]
    if not targets:
        print(f"No datasets matched only={only!r}")
        return
    for name in targets:
        print(json.dumps(index_dataset_staged.remote(name, cols, refresh), indent=2, default=str))


# ───────────────── Diagnostic — available staging filesystems ─────────────────
@app.function(timeout=120, memory=131072)
def diag_filesystems() -> dict:
    """Report writable scratch space WITHOUT an ephemeral_disk override (which has a
    512 GiB floor → spot). Tells us whether the default container disk or /dev/shm
    tmpfs can hold a ~50–72 GiB staged giant on stable capacity."""
    import shutil

    out = {}
    for path in ("/", "/tmp", "/dev/shm", "/root"):
        try:
            t, u, f = shutil.disk_usage(path)
            out[path] = {"total_gib": round(t / 1024**3, 1), "free_gib": round(f / 1024**3, 1)}
        except Exception as exc:  # noqa: BLE001
            out[path] = {"error": str(exc)[:120]}
    return out


@app.local_entrypoint()
def diag() -> None:
    """Print available staging filesystem sizes (no ephemeral_disk)."""
    import json

    print(json.dumps(diag_filesystems.remote(), indent=2, default=str))


# ───────────────── Cleanup — abort orphan multipart uploads ─────────────────
@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15)
def abort_orphan_multiparts(prefix: str = "active/") -> dict:
    """Abort dangling/incomplete multipart uploads left by failed direct-R2 index
    writes (R2 InvalidPart rejections never complete, so the parts linger). Read +
    abort only — completed objects and data are untouched."""
    s3 = _s3_client()
    aborted = []
    paginator = s3.get_paginator("list_multipart_uploads")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for u in page.get("Uploads", []) or []:
            s3.abort_multipart_upload(Bucket=BUCKET, Key=u["Key"], UploadId=u["UploadId"])
            aborted.append({"key": u["Key"], "upload_id": u["UploadId"][:24] + "…"})
    print(f"aborted {len(aborted)} orphan multipart upload(s) under {prefix}")
    return {"prefix": prefix, "aborted": len(aborted), "detail": aborted[:50]}


@app.local_entrypoint()
def cleanup(prefix: str = "active/") -> None:
    """Abort dangling multipart uploads from failed index writes."""
    import json

    print(json.dumps(abort_orphan_multiparts.remote(prefix), indent=2, default=str))


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15, memory=8192)
def prune_orphan_indices(name: str, execute: bool = False) -> dict:
    """Delete `_indices/<uuid>/` directories NOT referenced by the dataset's current
    manifest — the partial/superseded index files left by the failed direct-R2 and
    Modal-Volume attempts. Only ever touches `_indices/`; data fragments live under the
    dataset root and are never listed here. Dry-run by default; pass execute=True to
    delete. Hard safety gate: if the live index-uuid set can't be resolved (empty), it
    refuses to delete anything."""
    import lance

    uri = DATASETS[name]
    prefix = ACTIVE_PREFIXES[name]
    so = _r2_storage_options()
    s3 = _s3_client()

    ds = lance.dataset(uri, storage_options=so)
    live: set[str] = set()
    for ix in ds.list_indices():
        u = ix.get("uuid") if isinstance(ix, dict) else getattr(ix, "uuid", None)
        if u:
            live.add(str(u))

    idx_prefix = f"{prefix}_indices/"
    seen: dict[str, list] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=idx_prefix):
        for o in page.get("Contents", []):
            uuid = o["Key"][len(idx_prefix):].split("/", 1)[0]
            seen.setdefault(uuid, []).append((o["Key"], o["Size"]))
    orphans = {u: v for u, v in seen.items() if u not in live}
    orphan_bytes = sum(sz for v in orphans.values() for _, sz in v)

    deleted = 0
    if execute and live:  # gate: never delete without a resolved live set
        keys = [{"Key": k} for v in orphans.values() for k, _ in v]
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys[i:i + 1000], "Quiet": True})
            deleted += len(keys[i:i + 1000])
    print(f"{name}: live={len(live)} orphan_uuids={len(orphans)} "
          f"orphan_files={sum(len(v) for v in orphans.values())} "
          f"({orphan_bytes / 1024**3:.2f} GiB) deleted={deleted}")
    return {"dataset": name, "live_uuids": len(live), "orphan_uuids": sorted(orphans),
            "orphan_files": sum(len(v) for v in orphans.values()),
            "orphan_gib": round(orphan_bytes / 1024**3, 2),
            "deleted_files": deleted, "executed": bool(execute and live)}


@app.local_entrypoint()
def prune(only: str = "", execute: bool = False) -> None:
    """Report (default) or delete (--execute) unreferenced _indices/<uuid>/ orphans."""
    import json

    for name in [t for t in DATASETS if (only in t if only else True)]:
        print(json.dumps(prune_orphan_indices.remote(name, execute), indent=2, default=str))


# ───────────────────────── Verify (read-only) ─────────────────────────
@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 20, memory=8192)
def verify_all() -> list[dict]:
    """Read-only list_indices() sweep over every target — authoritative proof the
    indices are registered in the committed Lance manifests."""
    import lance

    so = _r2_storage_options()
    out: list[dict] = []
    for name, uri in DATASETS.items():
        try:
            ds = lance.dataset(uri, storage_options=so)
            out.append({"dataset": name, "rows": ds.count_rows(),
                        "indices": _list_committed_indices(ds)})
        except Exception as exc:  # noqa: BLE001
            out.append({"dataset": name, "error": str(exc)[:200]})
    return out


@app.local_entrypoint()
def verify() -> None:
    """Print the committed index layout for every target as a clean table."""
    rows = verify_all.remote()
    print(f"\n{'DATASET':<40} {'ROWS':>14}  {'COLUMN':<28} {'TYPE':<7}")
    print("=" * 96)
    for r in rows:
        name = r["dataset"]
        if "error" in r:
            print(f"{name:<40} {'ERROR':>14}  {r['error'][:40]}")
            continue
        idx = [i for i in r.get("indices", []) if isinstance(i, dict) and i.get("name")]
        if not idx:
            print(f"{name:<40} {r['rows']:>14,}  {'(no indices)':<28}")
            continue
        for j, ix in enumerate(idx):
            field = ",".join(ix.get("fields") or []) or "?"
            itype = str(ix.get("type") or "?")
            rowcol = f"{r['rows']:,}" if j == 0 else ""
            namecol = name if j == 0 else ""
            print(f"{namecol:<40} {rowcol:>14}  {field:<28} {itype:<7}")
    print("=" * 96)
