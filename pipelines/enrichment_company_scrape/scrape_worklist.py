"""Worklist runner — scrape a (uei, company_linkedin_url) cohort via Icypeas and land raw to Lance.

Purpose-built execution for the ICYPEAS_COMPANY_WORKLIST_2026-07-04 handoff (and any future
(uei, url) cohort). Reads rows carrying an entity key (uei) + LinkedIn company URL + lineage,
scrapes them through the single-container ``core/icypeas_gateway.py`` (``scrape_companies`` —
SYNCHRONOUS, rate-governed, zero-read), and lands the RAW payloads VERBATIM to a Lance dataset
keyed by (uei, company_linkedin_url), per the worklist's results contract.

    scrape_and_land(rows) → s3://data-sink/active/icypeas_company_scrapes/  (Lance, BTREE uei)

CREDIT SAFETY (0.5 credit/scrape; the account was suspended once by ungoverned probing):
  * The gateway governs the /api/scrape request rate globally (one bucket-owning container).
  * Idempotency — a uei already present in the results Lance is SKIPPED (never re-spend) unless force.
  * Incremental landing — results are appended to Lance every CHECKPOINT rows, so a mid-run failure
    loses at most one checkpoint and a re-run resumes (skips landed ueis).

RAW-FIRST (Directive 28). ``raw_result`` is the Icypeas ``data[]`` item as a VERBATIM JSON string
(the system of record); the flat columns (industry, employee_count, …) are a projection ON TOP of it.
Input lineage (li_source, source_class, money24_usd, in_dsbs, name, domain) is carried through.

    modal run pipelines/enrichment_company_scrape/scrape_worklist.py::run \\
        --rows-file /path/worklist_li_4775.json --canary 50            # canary: top 50
    modal run pipelines/enrichment_company_scrape/scrape_worklist.py::run \\
        --rows-file /path/worklist_li_4775.json                        # full sweep (canary skipped)
"""
from __future__ import annotations

import datetime as dt
import json
import os

import modal

GATEWAY_APP, GATEWAY_FN = "icypeas-gateway", "scrape_companies"
SCRAPE_MAX_BATCH = 50

BUCKET = "data-sink"
RESULTS_URI = os.environ.get("ICYPEAS_SCRAPES_LANCE_URI", f"s3://{BUCKET}/active/icypeas_company_scrapes/")
DATA_STORAGE_VERSION = "2.1"
CHECKPOINT_ROWS = 500          # append to Lance every N landed rows (credit-safety / resumability)

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "pylance>=0.19", "pyarrow>=17", "requests>=2.32",
)

app = modal.App("icypeas-scrape-worklist", image=image)


def _storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID (r2-credentials secret).")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _arrow_schema():
    import pyarrow as pa

    return pa.schema([
        ("uei", pa.string()),
        ("company_linkedin_url", pa.string()),
        ("name", pa.string()),
        ("domain", pa.string()),
        ("li_source", pa.string()),
        ("source_class", pa.string()),
        ("money24_usd", pa.float64()),
        ("in_dsbs", pa.bool_()),
        ("status", pa.string()),
        ("search_id", pa.string()),
        ("linkedin_url", pa.string()),      # result.url (canonical)
        ("website", pa.string()),
        ("domain_norm", pa.string()),
        ("industry", pa.string()),
        ("headcount_range", pa.string()),
        ("employee_count", pa.int64()),
        ("country", pa.string()),
        ("raw_result", pa.string()),        # the Icypeas data[] item VERBATIM (JSON string) — SoR
        ("batch_label", pa.string()),
        ("scraped_at", pa.timestamp("us", tz="UTC")),
    ])


def _norm(s: str | None) -> str | None:
    x = (s or "").strip().lower()
    for pre in ("https://", "http://"):
        if x.startswith(pre):
            x = x[len(pre):]
    if x.startswith("www."):
        x = x[4:]
    return x.rstrip("/") or None


def _project(row: dict, item: dict, batch_label: str, now: dt.datetime) -> dict:
    """One output row: input lineage + Icypeas status/searchId + flat projection + raw_result verbatim."""
    result = item.get("result") if isinstance(item, dict) else None
    result = result if isinstance(result, dict) else {}
    addr = result.get("address") if isinstance(result.get("address"), dict) else {}
    emp = result.get("numberOfEmployees")
    return {
        "uei": row.get("uei"),
        "company_linkedin_url": row.get("company_linkedin_url"),
        "name": row.get("name"),
        "domain": row.get("domain"),
        "li_source": row.get("li_source"),
        "source_class": row.get("source_class"),
        "money24_usd": row.get("money24_usd"),
        "in_dsbs": row.get("in_dsbs"),
        "status": (item.get("status") if isinstance(item, dict) else None),
        "search_id": (item.get("searchId") if isinstance(item, dict) else None),
        "linkedin_url": result.get("url"),
        "website": result.get("website"),
        "domain_norm": _norm(result.get("website")),
        "industry": result.get("industry"),
        "headcount_range": result.get("headcountRange"),
        "employee_count": int(emp) if isinstance(emp, (int, float)) and emp >= 0 else None,
        "country": addr.get("addressCountry") or addr.get("addressCountryCode"),
        "raw_result": json.dumps(item, default=str),
        "batch_label": batch_label,
        "scraped_at": now,
    }


def _append(rows_out: list[dict], so: dict) -> None:
    import lance
    import pyarrow as pa

    if not rows_out:
        return
    table = pa.Table.from_pylist(rows_out, schema=_arrow_schema())
    try:
        lance.dataset(RESULTS_URI, storage_options=so)
        mode = "append"
    except Exception:  # noqa: BLE001 — dataset does not exist yet
        mode = "create"
    lance.write_dataset(table, RESULTS_URI, mode=mode,
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 60 * 2)
def scrape_and_land(rows: list[dict], batch_label: str = "worklist", canary_n: int = 0,
                    force: bool = False) -> dict:
    """Scrape a (uei, url) cohort via the gateway and land raw → Lance keyed by (uei, url).
    ``rows`` — [{uei, company_linkedin_url, name?, domain?, li_source?, source_class?, money24_usd?, in_dsbs?}]."""
    import lance

    so = _storage_options()
    now = dt.datetime.now(dt.timezone.utc)

    # Normalize/validate input; keep first-seen order (rows arrive pre-sorted by money24_usd DESC).
    clean = [r for r in (rows or []) if isinstance(r, dict) and (r.get("company_linkedin_url") or "").strip()]
    if canary_n and canary_n > 0:
        clean = clean[:canary_n]

    # Idempotency — skip ueis already landed (never re-spend a scrape credit) unless force.
    landed: set = set()
    if not force:
        try:
            ds = lance.dataset(RESULTS_URI, storage_options=so)
            landed = set(x for x in ds.to_table(columns=["uei"]).column("uei").to_pylist() if x)
        except Exception:  # noqa: BLE001 — no dataset yet
            landed = set()
    todo = [r for r in clean if r.get("uei") not in landed]

    counts = {"requested": len(clean), "skipped": len(clean) - len(todo),
              "found": 0, "not_found": 0, "failed": 0, "landed": 0, "batches": 0}

    gw = modal.Function.from_name(GATEWAY_APP, GATEWAY_FN)
    chunks = [todo[i:i + SCRAPE_MAX_BATCH] for i in range(0, len(todo), SCRAPE_MAX_BATCH)]
    starmap_args = [([r["company_linkedin_url"] for r in c], [r.get("uei") for r in c]) for c in chunks]

    buffer: list[dict] = []
    # Scrape chunks concurrently through the gateway (its 16-way concurrency + rate bucket pace egress).
    for chunk, env in zip(chunks, gw.starmap(starmap_args, order_outputs=True)):
        counts["batches"] += 1
        if not isinstance(env, dict) or not env.get("ok"):
            counts["failed"] += len(chunk)
            print(f"WARN: chunk failed ({env.get('error') if isinstance(env, dict) else env}) — {len(chunk)} urls")
            continue
        results = env.get("results") or []
        for i, row in enumerate(chunk):
            item = results[i] if i < len(results) and isinstance(results[i], dict) else None
            if item is None:
                counts["failed"] += 1
                continue
            buffer.append(_project(row, item, batch_label, now))
            st = (item.get("status") or "").upper()
            counts["found" if st == "FOUND" else "not_found"] += 1
        if len(buffer) >= CHECKPOINT_ROWS:
            _append(buffer, so)
            counts["landed"] += len(buffer)
            print(f"checkpoint: landed {counts['landed']} rows so far ({counts['batches']} batches)")
            buffer = []

    if buffer:
        _append(buffer, so)
        counts["landed"] += len(buffer)

    print(f"DONE: {counts} → {RESULTS_URI}")
    return {"uri": RESULTS_URI, "batch_label": batch_label, **counts}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 30)
def build_indexes() -> dict:
    """Scalar indices on the results Lance: BTREE(uei) + BITMAP(source_class, status)."""
    import lance

    so = _storage_options()
    ds = lance.dataset(RESULTS_URI, storage_options=so)
    ds.create_scalar_index("uei", "BTREE", replace=True)
    ds.create_scalar_index("source_class", "BITMAP", replace=True)
    ds.create_scalar_index("status", "BITMAP", replace=True)
    print(f"indexes built on {RESULTS_URI}; rows={ds.count_rows()}")
    return {"uri": RESULTS_URI, "rows": ds.count_rows()}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10)
def verify(limit: int = 8) -> dict:
    """Read-back: row count, FOUND-rate, sample rows, employee_count coverage."""
    import lance

    so = _storage_options()
    ds = lance.dataset(RESULTS_URI, storage_options=so)
    t = ds.to_table(columns=["uei", "company_linkedin_url", "status", "name", "industry",
                             "employee_count", "domain_norm"])
    n = t.num_rows
    statuses = t.column("status").to_pylist()
    found = sum(1 for s in statuses if (s or "").upper() == "FOUND")
    emps = [e for e in t.column("employee_count").to_pylist() if e is not None]
    sample = t.slice(0, limit).to_pylist()
    out = {"uri": RESULTS_URI, "rows": n, "found": found,
           "found_rate": round(found / n, 3) if n else None,
           "employee_count_coverage": round(len(emps) / n, 3) if n else None,
           "sample": sample}
    print(f"rows={n} found={found} ({out['found_rate']}) emp_cov={out['employee_count_coverage']}")
    return out


@app.local_entrypoint()
def run(rows_file: str, canary: int = 0, force: bool = False,
        batch_label: str = "worklist-2026-07-04") -> None:
    """Read the local rows JSON and scrape+land. --canary N processes only the top N (by file order)."""
    with open(rows_file) as f:
        rows = json.load(f)
    print(f"loaded {len(rows)} rows from {rows_file}")
    print(json.dumps(scrape_and_land.remote(rows, batch_label=batch_label, canary_n=canary, force=force),
                     indent=2, default=str))


@app.local_entrypoint()
def indexes() -> None:
    print(json.dumps(build_indexes.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_run(limit: int = 8) -> None:
    print(json.dumps(verify.remote(limit), indent=2, default=str))
