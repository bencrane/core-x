"""TiC reverse-mapper — Modal production orchestrator.

Separates the three blast-radius-isolated phases the directive's "decoupled
compute/storage" mandate requires:

  1. WORKLIST  (build_worklist)        — stream each payer ToC, enumerate the
     in-network file URLs + external provider-reference URLs. Cheap, network-only.
     Output: a sharded worklist (one row per in-network file) in R2.
  2. REVERSE-MAP (process_file, fanned)— per file: idempotency-skip (ledger) ->
     stream-reverse-map against the fixed NPI cohort (RAM = O(cohort)) -> APPEND
     matched rate rows as a new Lance fragment -> ledger row. A failure here
     touches ONE fragment, never the index, never another file.
  3. INDEX     (rebuild_indexes)       — a SEPARATE, heavy, disk-bound job that
     (re)builds the BTREE/BITMAP scalar indexes on the accumulated SoR. Isolated
     so a flaky multi-GB parse can never corrupt the index, and the external sort
     gets its own temp_directory / memory envelope.

The cohort (target NPIs) is the Part-1 filter spine — tiny and fixed, so every
worker holds the entire match index in memory and the MRF streams are pure scans.

Run:
  modal run pipelines/tic_mrf/orchestrate.py::build_worklist --payer uhc
  modal run pipelines/tic_mrf/orchestrate.py::run --payer uhc --cohort-key tic/cohort/ny_ortho.json
  modal run pipelines/tic_mrf/orchestrate.py::rebuild_indexes
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

# Aetna data-plane base (BunnyCDN -> GCS); in-network URL = AETNA_BASE + filePath.
AETNA_BASE = "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICSI/"
AETNA_MANIFEST = AETNA_BASE + "latest_metadata.json"
UHC_MASTER_INDEX = "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/"


# ────────────────────────────── phase 1: worklist ──────────────────────────
@app.function(secrets=[R2], timeout=60 * 30, memory=2048)
def build_worklist(payer: str, manifest_url: str = "") -> dict:
    """Enumerate every DISTINCT in-network file for a payer from its master
    manifest and write a deduped JSONL worklist shard to R2. The manifests are
    payer-specific shapes (NOT TiC reporting_structure): Aetna latest_metadata.json
    = {"files":[{fileSchema,fileName,filePath,reportingPlans}]}, UHC master index =
    {"blobs":[{name,downloadUrl,size}]}. 66,912 UHC employer ToCs collapse to 7,170
    distinct in-network URLs — dedup is mandatory."""
    rows = _aetna_worklist() if payer == "aetna" else _uhc_worklist() if payer == "uhc" else _toc_worklist(payer, manifest_url)
    # dedup by in-network URL (UHC employer ToCs share network files)
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
        plans = f.get("reportingPlans") or [{}]
        out.append({"payer": "aetna", "in_network_url": AETNA_BASE + f["filePath"],
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
        out.append({"payer": "uhc", "in_network_url": b["downloadUrl"],  # carries the SAS token
                    "plan_id": None, "plan_name": name, "provider_ref_urls": []})
    return out


def _toc_worklist(payer: str, toc_url: str) -> list[dict]:
    """Fallback for a single TiC reporting_structure ToC file."""
    out = []
    for rec in rm.iter_toc(toc_url, cap_bytes=None):
        plans = rec["plans"] or [{}]
        for f in rec["in_network_files"]:
            if f.get("location"):
                out.append({"payer": payer, "in_network_url": f["location"],
                            "plan_id": plans[0].get("plan_id"),
                            "plan_name": plans[0].get("plan_name"), "provider_ref_urls": []})
    return out


# ──────────────────────────── phase 2: reverse-map ─────────────────────────
@app.function(secrets=[R2, PG], timeout=60 * 60, memory=4096, max_containers=64)
def process_file(task: dict) -> dict:
    """Reverse-map ONE in-network file against the fixed cohort and APPEND matched
    rows. Idempotency-guarded by the ledger; full-stream (cap=None), RAM=O(cohort)."""
    payer = task["payer"]
    url = task["in_network_url"]
    cohort = set(task["cohort_npis"])
    started = _utcnow()
    run_id = f"{payer}-{started}"

    h = rm.head(url)
    fver = h.get("etag") or h.get("last_modified")
    if rm.already_ingested(payer, url, fver):
        return {"url": url, "status": "skipped", "reason": "ledger hit (already ingested)"}

    tel = rm.Telemetry(source_url=url)
    tel.content_length = h.get("content_length")
    err, status = None, "success"
    try:
        spine = rm.build_provider_spine(url, cohort, cap_bytes=None, tel=tel,
                                        external_ref_urls=task.get("provider_ref_urls", ()))
        t0 = time.perf_counter()
        batch = []
        for row in rm.extract_rates(url, spine, cohort, payer, task.get("plan_id"),
                                    cap_bytes=None, tel=tel):
            batch.append(row)
            if len(batch) >= 50_000:           # bounded flush — append fragments, never resident
                rm.append_rates_to_lance(batch)
                batch = []
        if batch:
            rm.append_rates_to_lance(batch)
        tel.parse_ms = (time.perf_counter() - t0) * 1000
        tel.peak_rss_mb = rm._peak_rss_mb()
    except Exception as e:  # noqa: BLE001 — record terminal failure, isolate blast radius
        err, status = str(e)[:500], "error"

    rm.record_run(run_id, payer, url, fver, len(cohort), tel, status, err,
                  _ts(started), _utcnow_ts())
    return {"url": url, "status": status, "rows": tel.rows_emitted,
            "matched_groups": tel.matched_groups, "error": err}


@app.local_entrypoint()
def run(payer: str, cohort_key: str, limit: int = 0):
    """Fan the payer worklist across workers, each reverse-mapping one file against
    the Part-1 cohort. `cohort_key` is the R2 key of the filter-spine JSON."""
    s3 = _s3_local()
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
    print(f"done: ok={ok} skipped={skip} errors={errs} rows_appended={rows}")


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
