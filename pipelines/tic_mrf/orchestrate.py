"""TiC reverse-mapper — Modal production orchestrator.

Separates the blast-radius-isolated phases the directive's "decoupled
compute/storage" mandate requires:

  1. WORKLIST  (build_worklist)        — stream each payer master manifest,
     enumerate the in-network file URLs (TOKEN-STRIPPED; SAS carried separately)
     + external provider-reference URLs. Cheap, network-only.
     Output: a sharded worklist (one row per in-network file) in R2.
  1b. BRIDGE   (build_employer_bridge) — materialize the employer→file bridge
     (sponsor EIN ↔ in-network file URL ↔ plan metadata) from the employer ToCs
     as the tic_employer_file_bridge Lance dataset. This is the volume-proxy
     side of the outcome: which self-funded employers (Form 5500 participant
     counts) route through the file where a practice's rates appear.
  2. REVERSE-MAP (process_file, fanned)— per file: idempotency-skip (ledger) ->
     stream-reverse-map against the fixed NPI cohort (RAM = O(cohort)) -> stage
     rows to LOCAL Lance -> on complete success, ONE atomic append to the SoR ->
     ledger row. A mid-stream failure leaves zero SoR rows; a retry can never
     double-append. A failure here touches ONE file, never the index.
  3. INDEX     (rebuild_indexes)       — a SEPARATE, heavy, disk-bound job that
     (re)builds the BTREE/BITMAP scalar indexes on the accumulated SoR. Isolated
     so a flaky multi-GB parse can never corrupt the index, and the external sort
     gets its own temp_directory / memory envelope.

The cohort (target NPIs) is the Part-1 filter spine — tiny and fixed, so every
worker holds the entire match index in memory and the MRF streams are pure scans.

LAUNCH (deploy + spawn — repo doctrine, docs/reference/03_modal_compute.md §6.1):
  modal deploy pipelines/tic_mrf/orchestrate.py                       # REQUIRED first
  modal run pipelines/tic_mrf/orchestrate.py::build_worklist --payer uhc
  modal run pipelines/tic_mrf/orchestrate.py::bridge --payer uhc
  modal run pipelines/tic_mrf/orchestrate.py::run --payer uhc --cohort-key active/tic_cohort/ny_ortho.json
      # ^ spawn-fires run_fanout on the DEPLOYED app, prints the fc-id, returns.
  modal run pipelines/tic_mrf/orchestrate.py::rebuild_indexes

NEVER drive the fan-out synchronously from a laptop client: a SYNC input dies
~90 s after client loss (the sidecar doctrine's 8 recorded ledger failures).
`run` only SPAWNS; the driver (run_fanout) lives server-side on the deployed app.
"""
from __future__ import annotations

import json
import os
import time

import modal

from . import reverse_map as rm

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "ijson>=3.3", "requests>=2.32", "duckdb>=1.5,<2", "lancedb>=0.15",
    "pylance>=7", "pyarrow>=17", "boto3>=1.35", "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("tic-mrf-pipelines", image=image)

R2 = modal.Secret.from_name("r2-credentials")
PG = modal.Secret.from_name("hqx-postgres")

WORKLIST_PREFIX = "active/tic_worklist"  # one JSONL shard per payer
COHORT_PREFIX = "active/tic_cohort"      # the Part-1 filter spine, R2-staged
BRIDGE_PREFIX = "active/tic_employer_file_bridge"  # employer EIN <-> file URL bridge
STAGE_ROOT = "/tmp/tic_stage"            # per-file local Lance staging (atomic commit)
BATCH_ROWS = 50_000                      # bounded stage flush — never resident

# Aetna data-plane base (BunnyCDN -> GCS); in-network URL = AETNA_BASE + filePath.
AETNA_BASE = "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICSI/"
AETNA_MANIFEST = AETNA_BASE + "latest_metadata.json"
UHC_MASTER_INDEX = "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/"


def _with_sas(url: str, sas_query: str | None) -> str:
    """Recombine a token-stripped blob URL with its (separately carried) SAS query."""
    return f"{url}?{sas_query}" if sas_query else url


def _norm_ein(raw: object) -> str | None:
    """Digits-only EIN normalization (the Cigna doc's §2 trap: mixed hyphenated /
    digits-only formats within one file; normalize BOTH sides of every EIN join)."""
    import re

    if raw is None:
        return None
    digits = re.sub(r"\D+", "", str(raw))
    if not digits:
        return None
    return digits[-9:].zfill(9)


# ────────────────────────────── phase 1: worklist ──────────────────────────
@app.function(secrets=[R2], timeout=60 * 30, memory=2048)
def build_worklist(payer: str, manifest_url: str = "") -> dict:
    """Enumerate every DISTINCT in-network file for a payer from its master
    manifest and write a deduped JSONL worklist shard to R2. The manifests are
    payer-specific shapes (NOT TiC reporting_structure): Aetna latest_metadata.json
    = {"files":[{fileSchema,fileName,filePath,reportingPlans}]}, UHC master index =
    {"blobs":[{name,downloadUrl,size}]}. 66,912 UHC employer ToCs collapse to 7,170
    distinct in-network URLs — dedup is mandatory. `in_network_url` is stored
    TOKEN-STRIPPED (stable identity); the SAS query rides in `sas_query`."""
    rows = _aetna_worklist() if payer == "aetna" else _uhc_worklist() if payer == "uhc" else _toc_worklist(payer, manifest_url)
    # dedup by token-stripped in-network URL (UHC employer ToCs share network files)
    seen, deduped = set(), []
    for r in rows:
        u = r["in_network_url"]
        if u in seen:
            continue
        seen.add(u); deduped.append(r)
    body = "\n".join(json.dumps(r) for r in deduped).encode()
    key = f"{WORKLIST_PREFIX}/{payer}.jsonl"
    _s3().put_object(Bucket=rm.BUCKET, Key=key, Body=body)
    return {"payer": payer, "files_raw": len(rows), "files_distinct": len(deduped),
            "worklist": f"s3://{rm.BUCKET}/{key}"}


def _aetna_worklist() -> list[dict]:
    import requests

    md = requests.get(AETNA_MANIFEST, timeout=180,
                      headers={"User-Agent": rm.DEFAULT_UA}).json()
    out = []
    for f in md.get("files", []):
        if f.get("fileSchema") != "IN_NETWORK_RATES":
            continue
        # plan_id here is a SAMPLE (reportingPlans[0]) — a shared network file
        # serves many plans. Exact employer/plan attribution is the bridge's job
        # (tic_employer_file_bridge); this column only tags the file's provenance.
        plans = f.get("reportingPlans") or [{}]
        out.append({"payer": "aetna", "in_network_url": rm.strip_url_token(AETNA_BASE + f["filePath"]),
                    "sas_query": None,
                    "plan_id": plans[0].get("planId"), "plan_name": plans[0].get("planName"),
                    "provider_ref_urls": []})
    return out


def _uhc_worklist() -> list[dict]:
    import ijson
    import requests

    r = requests.get(UHC_MASTER_INDEX, stream=True, timeout=300,
                     headers={"User-Agent": rm.DEFAULT_UA})
    r.raw.decode_content = True
    out = []
    for b in ijson.items(r.raw, "blobs.item"):
        name = b.get("name", "")
        if "in-network-rates" not in name.lower():
            continue
        dl = b["downloadUrl"]
        # Split identity from credential: the blob path is the stable ledger key;
        # the SAS token expires and its `sig` re-mints on every index fetch.
        stripped = rm.strip_url_token(dl)
        sas = dl.split("?", 1)[1] if "?" in dl else None
        out.append({"payer": "uhc", "in_network_url": stripped, "sas_query": sas,
                    "plan_id": None, "plan_name": name, "provider_ref_urls": []})
    return out


def _toc_worklist(payer: str, toc_url: str) -> list[dict]:
    """Fallback for a single TiC reporting_structure ToC file."""
    out = []
    for rec in rm.iter_toc(toc_url, cap_bytes=None):
        plans = rec["plans"] or [{}]
        for f in rec["in_network_files"]:
            if f.get("location"):
                loc = f["location"]
                out.append({"payer": payer, "in_network_url": rm.strip_url_token(loc),
                            "sas_query": loc.split("?", 1)[1] if "?" in loc else None,
                            "plan_id": plans[0].get("plan_id"),
                            "plan_name": plans[0].get("plan_name"), "provider_ref_urls": []})
    return out


def _uhc_fresh_sas(stripped_url: str) -> str | None:
    """Re-resolve a fresh SAS-bearing downloadUrl for one blob from the UHC master
    index (the token is container-scoped; a mid-fan-out expiry 403/409s every
    remaining file — refreshing from the index is the only recovery, the SAS HMAC
    is not client-reconstructable)."""
    import ijson
    import requests

    r = requests.get(UHC_MASTER_INDEX, stream=True, timeout=300,
                     headers={"User-Agent": rm.DEFAULT_UA})
    r.raw.decode_content = True
    for b in ijson.items(r.raw, "blobs.item"):
        dl = b.get("downloadUrl", "")
        if rm.strip_url_token(dl) == stripped_url:
            return dl.split("?", 1)[1] if "?" in dl else None
    return None


# ─────────────────────── phase 1b: employer→file bridge ────────────────────
BRIDGE_BTREE = ["ein", "in_network_url"]
BRIDGE_BITMAP = ["payer", "plan_market_type"]


def _bridge_rows_from_toc(payer: str, toc_url: str, toc_stream) -> list[dict]:
    """Flatten one TiC reporting_structure ToC into bridge rows:
    (sponsor EIN ↔ in-network file URL ↔ plan metadata). FAN-IN CAVEAT (schema
    contract): file-level attribution is many-to-many — ~9 employers per shared
    network file (UHC), up to ~1,075:1 (Cigna). The bridge yields CANDIDATE
    employer sets per file, not exact panel membership."""
    captured_at = time.strftime("%Y-%m-%d", time.gmtime())
    rows: list[dict] = []
    for rec in toc_stream:
        plans = rec.get("plans") or rec.get("reporting_plans") or []
        files = rec.get("in_network_files") or []
        urls = [rm.strip_url_token(f["location"]) for f in files if f.get("location")]
        if not urls:
            continue
        for p in plans:
            ptype = (p.get("plan_id_type") or "").strip().lower()
            ein = _norm_ein(p.get("plan_id")) if ptype == "ein" else None
            for u in urls:
                rows.append({
                    "payer": payer,
                    "ein": ein,                      # digits-normalized; NULL for non-EIN plan ids
                    "plan_id_raw": str(p.get("plan_id")) if p.get("plan_id") is not None else None,
                    "plan_id_type": ptype or None,
                    "plan_name": p.get("plan_name"),
                    "plan_sponsor_name": p.get("plan_sponsor_name"),
                    "plan_market_type": p.get("plan_market_type"),
                    "issuer_name": p.get("issuer_name"),
                    "in_network_url": u,
                    "toc_url": rm.strip_url_token(toc_url),
                    "captured_at": captured_at,
                })
    return rows


@app.function(secrets=[R2], timeout=60 * 30, memory=2048, max_containers=16)
def fetch_employer_toc_chunk(chunk: list[dict]) -> list[dict]:
    """Fetch + flatten a chunk of small employer ToC files ({toc_url, sas_query}).
    Each UHC `_index.json` is a few KB; chunking keeps the fan-out cheap."""
    rows: list[dict] = []
    for t in chunk:
        url = _with_sas(t["toc_url"], t.get("sas_query"))
        try:
            rows.extend(_bridge_rows_from_toc(t["payer"], t["toc_url"],
                                              rm.iter_toc(url, cap_bytes=None)))
        except Exception as exc:  # noqa: BLE001 — one bad ToC never sinks the chunk
            print(f"WARN: employer ToC failed ({t['toc_url']}): {exc}")
    return rows


@app.function(secrets=[R2], timeout=60 * 60 * 4, memory=8192)
def build_employer_bridge(payer: str, index_url: str = "", chunk_size: int = 500) -> dict:
    """Materialize tic_employer_file_bridge for one payer:
    (sponsor EIN ↔ in-network file URL ↔ plan metadata), EIN digits-normalized,
    joinable to form5500_main on SPONS_DFE_EIN (normalize both sides — Cigna doc §2).

    UHC: enumerate the 66,912 employer `_index.json` ToCs from the master index and
    fan-fetch them. Other payers (e.g. Cigna): pass the single national ToC index
    as `index_url` and it is streamed directly. Output overwrites the payer's
    partition at s3://data-sink/active/tic_employer_file_bridge/payer=<payer>/
    (rebuildable, idempotent). Local Lance stage -> boto3 publish (the R2
    multipart rule)."""
    rm.require_fast_ijson()
    rows: list[dict] = []
    if payer == "uhc":
        import ijson
        import requests

        r = requests.get(UHC_MASTER_INDEX, stream=True, timeout=300,
                         headers={"User-Agent": rm.DEFAULT_UA})
        r.raw.decode_content = True
        tocs = []
        for b in ijson.items(r.raw, "blobs.item"):
            if not b.get("name", "").lower().endswith("_index.json"):
                continue
            dl = b["downloadUrl"]
            tocs.append({"payer": "uhc", "toc_url": rm.strip_url_token(dl),
                         "sas_query": dl.split("?", 1)[1] if "?" in dl else None})
        chunks = [tocs[i:i + chunk_size] for i in range(0, len(tocs), chunk_size)]
        for res in fetch_employer_toc_chunk.map(chunks, return_exceptions=True):
            if isinstance(res, Exception):
                print(f"WARN: bridge chunk failed: {res}")
                continue
            rows.extend(res)
    else:
        if not index_url:
            raise ValueError(f"payer={payer}: pass --index-url (the payer's ToC index)")
        rows = _bridge_rows_from_toc(payer, index_url, rm.iter_toc(index_url, cap_bytes=None))

    return _publish_bridge(payer, rows)


def _publish_bridge(payer: str, rows: list[dict]) -> dict:
    """Local Lance stage + scalar indexes, then boto3 publish to the payer's
    bridge partition (wipe + upload — idempotent rebuild)."""
    import shutil

    import lance
    import pyarrow as pa

    local = os.path.join(STAGE_ROOT, f"bridge_{payer}_lance")
    shutil.rmtree(local, ignore_errors=True)
    tbl = pa.Table.from_pylist(rows)
    lance.write_dataset(tbl, local, mode="create")
    ds = lance.dataset(local)
    for col in BRIDGE_BTREE:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
    for col in BRIDGE_BITMAP:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)

    s3 = _s3()
    prefix = f"{BRIDGE_PREFIX}/payer={payer}/"
    to_del = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=rm.BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            to_del.append({"Key": o["Key"]})
            if len(to_del) == 1000:
                s3.delete_objects(Bucket=rm.BUCKET, Delete={"Objects": to_del, "Quiet": True})
                to_del = []
    if to_del:
        s3.delete_objects(Bucket=rm.BUCKET, Delete={"Objects": to_del, "Quiet": True})
    uploaded = 0
    for root, _, files in os.walk(local):
        for fn in files:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local).replace(os.sep, "/")
            s3.upload_file(lp, rm.BUCKET, prefix + rel)
            uploaded += 1
    shutil.rmtree(local, ignore_errors=True)
    return {"payer": payer, "bridge_rows": len(rows), "files_uploaded": uploaded,
            "dataset": f"s3://{rm.BUCKET}/{prefix}"}


# ──────────────────────────── phase 2: reverse-map ─────────────────────────
@app.function(secrets=[R2, PG], timeout=60 * 60, memory=4096, max_containers=64)
def process_file(task: dict) -> dict:
    """Reverse-map ONE in-network file against the fixed cohort. Idempotency-guarded
    by the ledger (token-stripped URL + non-null file_version); full-stream
    (cap=None), RAM=O(cohort). ATOMIC: rows stage to local Lance while streaming
    and commit to the SoR in ONE append only on complete success — a mid-stream
    failure publishes nothing, so a retry never double-appends. UHC 403/409
    (expired SAS) triggers one fresh-SAS re-resolve from the master index."""
    import shutil

    import requests

    rm.require_fast_ijson()
    payer = task["payer"]
    url = task["in_network_url"]              # token-stripped canonical identity
    cohort = set(task["cohort_npis"])
    started = _utcnow()
    run_id = f"{payer}-{started}"

    fetch_url = _with_sas(url, task.get("sas_query"))
    h = rm.head(fetch_url)
    if payer == "uhc" and h.get("status") in (403, 409):
        fresh = _uhc_fresh_sas(url)
        if fresh:
            fetch_url = _with_sas(url, fresh)
            h = rm.head(fetch_url)
    fver = rm.derive_file_version(h, url)     # never NULL
    if rm.already_ingested(payer, url, fver):
        return {"url": url, "status": "skipped", "reason": "ledger hit (already ingested)"}

    stage_dir = os.path.join(STAGE_ROOT, f"file_{abs(hash(url)):x}")
    tel = rm.Telemetry(source_url=url)
    tel.content_length = h.get("content_length")
    err, status = None, "success"

    def _reverse_map(src_url: str) -> None:
        """One full staged reverse-map attempt. Wipes the stage first so a SAS-
        refresh retry starts clean. t0 spans BOTH passes (Pass A + Pass B) so
        ledger parse_ms/throughput match the POC methodology."""
        shutil.rmtree(stage_dir, ignore_errors=True)
        os.makedirs(stage_dir, exist_ok=True)
        t0 = time.perf_counter()
        spine = rm.build_provider_spine(src_url, cohort, cap_bytes=None, tel=tel,
                                        external_ref_urls=task.get("provider_ref_urls", ()))
        batch = []
        for row in rm.extract_rates(src_url, spine, cohort, payer, task.get("plan_id"),
                                    cap_bytes=None, tel=tel, file_version=fver):
            batch.append(row)
            if len(batch) >= BATCH_ROWS:       # bounded flush — LOCAL stage, never resident
                rm.append_rates_to_lance(batch, local_only_dir=stage_dir)
                batch = []
        if batch:
            rm.append_rates_to_lance(batch, local_only_dir=stage_dir)
        tel.parse_ms = (time.perf_counter() - t0) * 1000
        tel.peak_rss_mb = rm._peak_rss_mb()

    try:
        try:
            _reverse_map(fetch_url)
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else None
            if payer == "uhc" and code in (403, 409):
                fresh = _uhc_fresh_sas(url)
                if not fresh:
                    raise
                tel = rm.Telemetry(source_url=url)   # clean telemetry for the retry
                tel.content_length = h.get("content_length")
                _reverse_map(_with_sas(url, fresh))
            else:
                raise
        rm.publish_stage_to_sor(stage_dir)     # the ONLY SoR write — atomic commit
    except Exception as e:  # noqa: BLE001 — record terminal failure, isolate blast radius
        err, status = str(e)[:500], "error"
    finally:
        shutil.rmtree(stage_dir, ignore_errors=True)

    rm.record_run(run_id, payer, url, fver, len(cohort), tel, status, err,
                  _ts(started), _utcnow_ts())
    return {"url": url, "status": status, "rows": tel.rows_emitted,
            "matched_groups": tel.matched_groups, "error": err}


@app.function(secrets=[R2, PG], timeout=60 * 60 * 24, memory=2048)
def run_fanout(payer: str, cohort_key: str, limit: int = 0) -> dict:
    """Server-side fan-out driver: loads the worklist + cohort from R2 and maps
    process_file across workers. Lives on the DEPLOYED app and is SPAWNED (async
    input, no client tether) — a sync local driver dies ~90 s after client loss
    (the repo's recorded 8-ledger-failure launch mode)."""
    s3 = _s3()
    cohort = _load_cohort(s3, cohort_key)
    wl_key = f"{WORKLIST_PREFIX}/{payer}.jsonl"
    body = s3.get_object(Bucket=rm.BUCKET, Key=wl_key)["Body"].read().decode()
    tasks = [json.loads(l) for l in body.splitlines() if l.strip()]
    if limit:
        tasks = tasks[:limit]
    for t in tasks:
        t["cohort_npis"] = cohort
    print(f"fanning {len(tasks)} {payer} files across workers (cohort={len(cohort)} NPIs)")
    ok = rows = skip = errs = 0
    for res in process_file.map(tasks, return_exceptions=True):
        if isinstance(res, Exception):
            errs += 1; continue
        if res["status"] == "success": ok += 1; rows += res.get("rows", 0)
        elif res["status"] == "skipped": skip += 1
        else: errs += 1
    out = {"payer": payer, "ok": ok, "skipped": skip, "errors": errs, "rows_appended": rows}
    print(f"done: {out}")
    return out


@app.local_entrypoint()
def run(payer: str, cohort_key: str, limit: int = 0):
    """Spawn-fire run_fanout on the DEPLOYED app and return immediately.
    `modal deploy pipelines/tic_mrf/orchestrate.py` FIRST — spawn runs the
    deployed snapshot. NEVER convert this back to a sync `.map` driver: the
    server cancels a SYNC input ~90 s after the client stops heartbeating."""
    fn = modal.Function.from_name("tic-mrf-pipelines", "run_fanout")
    fc = fn.spawn(payer=payer, cohort_key=cohort_key, limit=limit)
    print(f"FUNCTION_CALL_ID: {fc.object_id}")
    print("Follow: modal app logs tic-mrf-pipelines   |   result: "
          f"modal.FunctionCall.from_id('{fc.object_id}').get(timeout=0)")


@app.local_entrypoint()
def bridge(payer: str, index_url: str = ""):
    """Build the employer→file bridge for one payer (runs remote, client-tethered —
    the worklist-scale job is short; the long fan-out path is `run`)."""
    print(json.dumps(build_employer_bridge.remote(payer=payer, index_url=index_url),
                     indent=2, default=str))


# ───────────────────────────── phase 3: index ──────────────────────────────
@app.function(secrets=[R2], timeout=60 * 60, memory=16384, ephemeral_disk=200 * 1024)
def rebuild_indexes() -> dict:
    """Heavy, isolated scalar-index (re)build over the accumulated SoR. Local NVMe
    temp_directory for the external sort; never co-scheduled with appends."""
    import lance

    so = rm._r2_so()
    ds = lance.dataset(rm.DATASET_URI, storage_options=so)
    built = []
    for col in rm.RATE_BTREE:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        built.append(f"BTREE:{col}")
    for col in rm.RATE_BITMAP:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        built.append(f"BITMAP:{col}")
    return {"dataset": rm.DATASET_URI, "rows": ds.count_rows(), "indices": built}


# ───────────────────────────────── helpers ─────────────────────────────────
def _s3():
    import boto3
    o = rm._r2_so()
    return boto3.client("s3", endpoint_url=o["endpoint"], aws_access_key_id=o["aws_access_key_id"],
                        aws_secret_access_key=o["aws_secret_access_key"], region_name="auto")


_s3_local = _s3


def _load_cohort(s3, key: str) -> list[str]:
    spine = json.loads(s3.get_object(Bucket=rm.BUCKET, Key=key)["Body"].read())
    npis: list[str] = []
    for g in spine.get("groups", []):
        npis.extend(str(n) for n in (g.get("member_npis") or []))
    return sorted(set(npis))


def _utcnow() -> str:
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _utcnow_ts():
    import datetime
    return datetime.datetime.now(datetime.timezone.utc)


def _ts(iso: str):
    import datetime
    return datetime.datetime.fromisoformat(iso)
