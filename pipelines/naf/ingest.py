"""Phase 1 — NAF wage-schedule LANDING FETCHER (raw PDFs → R2, no parse).

Drives the frozen Phase-0 census worklist: fetches every in-scope NAF wage-schedule PDF from
DoD DCPAS and streams it to the R2 landing zone, emitting a fetch-manifest Parquet that records
what landed where. Pure transport — no parsing, no rate extraction (Phase 2 owns that). Fetch
never writes the SoR (``active/``); a WAF flap here cannot corrupt anything downstream.

SCOPE (locked): CONUS, all schedule types. Overseas (dirs 170/171) EXCLUDED by construction —
                neither worklist source below reads the overseas catalog, and OVERSEAS_DIRS
                guards the two area-dir codes defensively.

WORKLIST (unioned, deduped on destination R2 key)
  CT            exports/naf_census/naf_ct_urls.parquet     5,195 deterministic URLs (primary rate corpus)
  NF paybands   exports/naf_census/catalog_latest.jsonl    current-snapshot -NF hrefs
  non-CT hist.  exports/naf_census/naf_nonct_urls.parquet  OPTIONAL — dropped by the Phase-1b resolver;
                                                            absent today, so ``fetch`` lands CT+NF only.

IDEMPOTENCY  each PDF → deterministic key  landing/naf/pdfs/{area_dir}/{filename}. The landing
             prefix is LISTed once up front; already-present keys (size>0) are skipped in-memory,
             so a re-run transfers nothing already landed and an interrupted run resumes for free.

VALIDATION   a body counts as ``landed`` only on HTTP 200 + %PDF magic. HTTP 404 is recorded as
             ``missing`` (an enumerated version with no published PDF — expected per census Risk
             #8), never a hard failure. Anything else is ``failed`` and retried on the next run.

OUTPUT   s3://data-sink/landing/naf/pdfs/{area_dir}/{filename}   raw PDF blobs
         exports/naf_landing/fetch_manifest.parquet             per-URL landing record (Phase-2 input)
         ops.naf_runs                                           run ledger (shared with Phase 0)

CLI
  doppler run -p core-x -c prd -- python3 -m pipelines.naf.ingest fetch                # all in-scope
  doppler run -p core-x -c prd -- python3 -m pipelines.naf.ingest fetch --types CT
  doppler run -p core-x -c prd -- python3 -m pipelines.naf.ingest fetch --limit 30     # smoke
  doppler run -p core-x -c prd -- python3 -m pipelines.naf.ingest verify
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# Reuse the repo's canonical plumbing — never re-implement. bls.ingest owns the R2 client;
# census owns the browser-UA session + the shared ops.naf_runs ledger writer.
from pipelines.bls.ingest import _s3_client
from pipelines.naf.census import _record_run, _session

BASE = "https://wageandsalary.dcpas.osd.mil"
BUCKET = "data-sink"
LANDING_PREFIX = "landing/naf/pdfs/"
CENSUS_OUT = os.environ.get("NAF_CENSUS_OUT", "exports/naf_census")
LAND_OUT = os.environ.get("NAF_LANDING_OUT", "exports/naf_landing")
OVERSEAS_DIRS = {"170", "171"}          # excluded per locked Phase-1 scope

_tls = threading.local()


def _clients():
    """Thread-local requests.Session + boto3 S3 client (neither is safe to share across threads)."""
    if not hasattr(_tls, "sess"):
        _tls.sess = _session()
        _tls.s3 = _s3_client()
    return _tls.sess, _tls.s3


def _r2_key(area_dir: str, filename: str) -> str:
    return f"{LANDING_PREFIX}{area_dir}/{filename}"


def _pdf_url(area_dir: str, filename: str) -> str:
    # Static asset path; only spaces need encoding. Keep '*' RAW — percent-encoding it (%2A)
    # triggers a redirect loop on the DCPAS edge (observed on 002-058-0*-A-NF.pdf).
    return f"{BASE}/Content/NAF%20Schedules/survey-sch/{area_dir}/{filename.replace(' ', '%20')}"


# Per-type synthesis of the non-CT PDF filename from the census `version` field (calibrated live):
#   PBS      version = .html viewer name   -> swap .html→.pdf              survey-sch  (-NF paybands)
#   PBPR     version = the .pdf filename    -> verbatim                     survey-sch  (-PR ranges)
#   AS       bare version                   -> {wa}-{ver}-AS.pdf            survey-sch
#   RSB      bare version                   -> {wa}-{ver}-ScheduleBack.pdf  survey-sch
#   Special  version = the .pdf filename     -> verbatim                     SPECIAL-sch (-AUTH memos)
_NONCT_SUBDIR = {"Special": "special-sch"}      # every other type lives under survey-sch


def _nonct_filename(schedule_type: str, version, wage_area: str) -> str | None:
    v = str(version)
    if schedule_type == "PBS":
        return v[:-5] + ".pdf" if v.endswith(".html") else (v if v.endswith(".pdf") else None)
    if schedule_type in ("PBPR", "Special"):
        return v if v.endswith(".pdf") else None
    if schedule_type == "AS":
        return f"{wage_area}-{v}-AS.pdf"
    if schedule_type == "RSB":
        return f"{wage_area}-{v}-ScheduleBack.pdf"
    return None


# ── worklist assembly ────────────────────────────────────────────────────────────────
def _ct_items() -> list[dict]:
    import pyarrow.parquet as pq
    rows = pq.read_table(os.path.join(CENSUS_OUT, "naf_ct_urls.parquet")).to_pylist()
    out = []
    for r in rows:
        na = r["naf_area"]
        if na in OVERSEAS_DIRS:
            continue
        fn = f'{r["wage_area"]}-{r["version"]}-CT.pdf'
        out.append({"schedule_type": "CT", "naf_area": na, "wage_area": r["wage_area"],
                    "version": r["version"], "filename": fn,
                    "url": r["pdf_url"], "r2_key": _r2_key(na, fn)})
    return out


def _nf_items() -> list[dict]:
    p = os.path.join(CENSUS_OUT, "catalog_latest.jsonl")
    out = []
    with open(p) as f:
        for line in f:
            if not line.strip():
                continue
            c = json.loads(line)
            fn, ad = c["filename"], c["area_dir"]
            if fn.endswith("-CT.pdf") or ad in OVERSEAS_DIRS:
                continue
            out.append({"schedule_type": "NF", "naf_area": ad, "wage_area": None,
                        "version": None, "filename": fn,
                        "url": _pdf_url(ad, fn), "r2_key": _r2_key(ad, fn)})
    return out


def _nonct_items() -> list[dict]:
    """OPTIONAL Phase-1b resolved historical non-CT worklist. Absent until the resolver runs."""
    p = os.path.join(CENSUS_OUT, "naf_nonct_urls.parquet")
    if not os.path.exists(p):
        return []
    import pyarrow.parquet as pq
    out = []
    for r in pq.read_table(p).to_pylist():
        ad = r["naf_area"]
        if ad in OVERSEAS_DIRS:
            continue
        out.append({"schedule_type": r.get("schedule_type", "NONCT"), "naf_area": ad,
                    "wage_area": r.get("wage_area"), "version": r.get("version"),
                    "filename": r["filename"], "url": r["url"],
                    "r2_key": _r2_key(ad, r["filename"])})
    return out


_BUILDERS = {"CT": _ct_items, "NF": _nf_items, "NONCT": _nonct_items}


def _worklist(types: list[str]) -> list[dict]:
    items, seen = [], set()
    for t in types:
        for it in _BUILDERS[t]():
            if it["r2_key"] in seen:          # dedup on destination key
                continue
            seen.add(it["r2_key"])
            items.append(it)
    return items


# ── R2 landing state (resume / idempotency) ──────────────────────────────────────────
def _landed_keys(s3) -> dict[str, int]:
    landed: dict[str, int] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=LANDING_PREFIX):
        for o in page.get("Contents", []):
            if o["Size"] > 0:
                landed[o["Key"]] = o["Size"]
    return landed


# ── single fetch ─────────────────────────────────────────────────────────────────────
def _fetch_one(item: dict, sleep: float) -> dict:
    sess, s3 = _clients()
    rec = {"schedule_type": item["schedule_type"], "naf_area": item["naf_area"],
           "wage_area": item["wage_area"], "version": item["version"],
           "filename": item["filename"], "source_url": item["url"], "r2_key": item["r2_key"],
           "http_status": None, "content_type": None, "bytes": 0, "pdf_ok": False,
           "status": "failed", "error": None,
           "fetched_ts": dt.datetime.now(dt.timezone.utc).isoformat()}
    try:
        r = sess.get(item["url"], timeout=60)
        rec["http_status"] = r.status_code
        rec["content_type"] = (r.headers.get("Content-Type", "") or "").split(";")[0]
        if r.status_code == 200 and r.content[:4] == b"%PDF":
            rec["bytes"] = len(r.content)
            rec["pdf_ok"] = True
            s3.put_object(Bucket=BUCKET, Key=item["r2_key"], Body=r.content,
                          ContentType="application/pdf")
            rec["status"] = "landed"
        elif r.status_code == 404:
            rec["status"] = "missing"          # enumerated version w/o published PDF (expected)
        else:
            rec["error"] = f"http={r.status_code} magic={r.content[:8]!r}"
    except Exception as exc:                    # noqa: BLE001 — one bad URL must not kill the run
        rec["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        time.sleep(sleep)
    return rec


# ── persistence ──────────────────────────────────────────────────────────────────────
def _write_manifest(records: list[dict]) -> str:
    import pyarrow as pa
    import pyarrow.parquet as pq
    os.makedirs(LAND_OUT, exist_ok=True)
    path = os.path.join(LAND_OUT, "fetch_manifest.parquet")
    schema = pa.schema([
        ("schedule_type", pa.string()), ("naf_area", pa.string()), ("wage_area", pa.string()),
        ("version", pa.string()), ("filename", pa.string()), ("source_url", pa.string()),
        ("r2_key", pa.string()), ("http_status", pa.int32()), ("content_type", pa.string()),
        ("bytes", pa.int64()), ("pdf_ok", pa.bool_()), ("status", pa.string()),
        ("error", pa.string()), ("fetched_ts", pa.string()),
    ])
    pq.write_table(pa.Table.from_pylist(records, schema=schema), path)
    return path


def _upload_manifest_r2(path: str) -> None:
    key = "landing/naf/" + os.path.basename(path)
    _s3_client().upload_file(path, BUCKET, key)
    print(f"[r2] s3://{BUCKET}/{key}")


def _write_summary(records: list[dict], types: list[str]) -> str:
    import collections
    os.makedirs("reports", exist_ok=True)
    by_status = collections.Counter(r["status"] for r in records)
    by_type = collections.Counter(r["schedule_type"] for r in records
                                  if r["status"] in ("landed", "already_landed"))
    areas = {r["naf_area"] for r in records if r["status"] in ("landed", "already_landed")}
    total_bytes = sum(r["bytes"] or 0 for r in records)
    landed_n = by_status.get("landed", 0) + by_status.get("already_landed", 0)
    L = ["# NAF Wage-Schedule Landing — Phase 1 Fetch Report\n",
         f"Source: `{BASE}` → `s3://{BUCKET}/{LANDING_PREFIX}`. Scope: CONUS, types {types}; overseas excluded.\n",
         "## Totals\n",
         f"- Worklist: **{len(records):,}**",
         f"- **Landed PDFs: {landed_n:,}** ({total_bytes/1024/1024:.1f} MiB) across {len(areas)} area dirs",
         f"- Status breakdown: {dict(by_status)}",
         "\n## Landed by schedule type\n", "| type | pdfs |\n|---|---|"]
    for k, v in by_type.most_common():
        L.append(f"| {k} | {v:,} |")
    L.append("\n## Sample landed R2 keys\n")
    shown = 0
    for r in records:
        if r["status"] in ("landed", "already_landed"):
            L.append(f"- `s3://{BUCKET}/{r['r2_key']}`")
            shown += 1
            if shown >= 6:
                break
    path = "reports/naf_landing_summary.md"
    with open(path, "w") as f:
        f.write("\n".join(L) + "\n")
    print(f"[summary] {path}")
    return path


# ── commands ─────────────────────────────────────────────────────────────────────────
def cmd_fetch(args) -> int:
    types = ([t.strip().upper() for t in args.types.split(",") if t.strip()]
             if args.types else ["CT", "NF", "NONCT"])
    types = [t for t in types if t in _BUILDERS]
    if not types:
        raise SystemExit(f"no valid --types (choose from {sorted(_BUILDERS)})")

    work = _worklist(types)
    if args.limit:
        work = work[:args.limit]
    s3 = _s3_client()
    landed = _landed_keys(s3)
    todo = [it for it in work if it["r2_key"] not in landed]
    skipped = [it for it in work if it["r2_key"] in landed]
    print(f"[fetch] types={types} worklist={len(work)} already_landed={len(skipped)} "
          f"to_fetch={len(todo)} workers={args.workers} sleep={args.sleep}")

    started = dt.datetime.now(dt.timezone.utc)
    records: list[dict] = []
    for it in skipped:                          # already-landed → manifest row, no refetch
        records.append({"schedule_type": it["schedule_type"], "naf_area": it["naf_area"],
                        "wage_area": it["wage_area"], "version": it["version"],
                        "filename": it["filename"], "source_url": it["url"], "r2_key": it["r2_key"],
                        "http_status": 200, "content_type": "application/pdf",
                        "bytes": landed[it["r2_key"]], "pdf_ok": True,
                        "status": "already_landed", "error": None, "fetched_ts": None})

    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for rec in ex.map(lambda it: _fetch_one(it, args.sleep), todo):
            records.append(rec)
            done += 1
            if done % 250 == 0:
                land = sum(1 for r in records if r["status"] in ("landed", "already_landed"))
                print(f"  … {done}/{len(todo)} fetched  ({land} landed total, {time.time()-t0:.0f}s)")

    by_status: dict[str, int] = {}
    total_bytes = 0
    for r in records:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
        total_bytes += r["bytes"] or 0
    landed_n = by_status.get("landed", 0) + by_status.get("already_landed", 0)
    path = _write_manifest(records)
    print(f"[manifest] {len(records)} rows -> {path}")
    print(f"[status]   {by_status}")
    print(f"[bytes]    {total_bytes/1024/1024:.1f} MiB across {landed_n} PDFs ({time.time()-t0:.0f}s)")
    if by_status.get("failed"):
        for r in records:
            if r["status"] == "failed":
                print(f"  FAILED {r['r2_key']} :: {r['error']}")
                break

    if not args.limit:                          # only full runs publish the durable manifest+summary
        _upload_manifest_r2(path)
        _write_summary(records, types)

    _record_run("naf_landing", f"s3://{BUCKET}/{LANDING_PREFIX}", path, ",".join(types),
                landed_n, by_status.get("failed", 0), [],
                "success" if not by_status.get("failed") else "partial",
                None, started, dt.datetime.now(dt.timezone.utc))
    return 0


def cmd_verify(args) -> int:
    import random
    import pyarrow.parquet as pq
    s3 = _s3_client()
    landed = _landed_keys(s3)
    print(f"[r2] objects under {LANDING_PREFIX}: {len(landed)}  ({sum(landed.values())/1024/1024:.1f} MiB)")

    mpath = os.path.join(LAND_OUT, "fetch_manifest.parquet")
    if os.path.exists(mpath):
        recs = pq.read_table(mpath).to_pylist()
        bys: dict[str, int] = {}
        for r in recs:
            bys[r["status"]] = bys.get(r["status"], 0) + 1
        print(f"[manifest] {len(recs)} rows  status={bys}")
        man_landed = {r["r2_key"] for r in recs if r["status"] in ("landed", "already_landed")}
        gap = man_landed - set(landed)
        print(f"[reconcile] manifest says landed but absent from R2: {len(gap)}"
              + (f"  e.g. {sorted(gap)[:3]}" if gap else ""))
    else:
        print("[manifest] none yet")

    keys = list(landed)
    random.seed(1)
    random.shuffle(keys)
    sample = keys[:min(12, len(keys))]
    ok = 0
    for k in sample:
        head = s3.get_object(Bucket=BUCKET, Key=k)["Body"].read(4)
        if head == b"%PDF":
            ok += 1
    print(f"[sample] {ok}/{len(sample)} sampled R2 objects are valid %PDF")
    return 0


def cmd_resolve_nonct(args) -> int:
    """Synthesize the historical non-CT PDF worklist directly from the Phase-0 census manifest.

    No cascade crawl: every non-CT type's filename is deterministically recoverable from the
    census ``version`` field (calibrated live — see _nonct_filename). Writes naf_nonct_urls.parquet,
    which ``fetch --types NONCT`` then lands. Overseas dirs excluded. Enumerated versions whose
    synthesized URL 404s (variant re-issues / historical gaps) are landed as ``missing`` by fetch.
    """
    import collections

    import pyarrow as pa
    import pyarrow.parquet as pq
    rows = pq.read_table(os.path.join(CENSUS_OUT, "naf_census_manifest.parquet")).to_pylist()
    out, seen = [], set()
    skipped = 0
    for r in rows:
        t = r["schedule_type"]
        if t == "CT":
            continue
        na = r["naf_area"]
        if na in OVERSEAS_DIRS:
            continue
        fn = _nonct_filename(t, r["version"], r["wage_area"])
        if not fn:
            skipped += 1
            continue
        if (na, fn) in seen:
            continue
        seen.add((na, fn))
        subdir = _NONCT_SUBDIR.get(t, "survey-sch")
        url = f"{BASE}/Content/NAF%20Schedules/{subdir}/{na}/{fn.replace(' ', '%20')}"
        out.append({"schedule_type": t, "naf_area": na, "wage_area": r["wage_area"],
                    "version": str(r["version"]), "filename": fn, "url": url})
    path = os.path.join(CENSUS_OUT, "naf_nonct_urls.parquet")
    schema = pa.schema([("schedule_type", pa.string()), ("naf_area", pa.string()),
                        ("wage_area", pa.string()), ("version", pa.string()),
                        ("filename", pa.string()), ("url", pa.string())])
    pq.write_table(pa.Table.from_pylist(out, schema=schema), path)
    byt = collections.Counter(r["schedule_type"] for r in out)
    print(f"[resolve-nonct] {len(out)} distinct non-CT PDFs -> {path}  (skipped {skipped} unsynthesizable)")
    print(f"[by type] {dict(byt)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="NAF wage-schedule landing fetcher (Phase 1).")
    ap.add_argument("cmd", choices=["fetch", "resolve-nonct", "verify"])
    ap.add_argument("--types", default="", help="comma list of CT,NF,NONCT (default: all present)")
    ap.add_argument("--limit", type=int, default=0, help="cap worklist size (smoke tests)")
    ap.add_argument("--workers", type=int, default=6, help="concurrent fetch threads")
    ap.add_argument("--sleep", type=float, default=0.15, help="polite per-request delay (s)")
    args = ap.parse_args(argv)
    if args.cmd == "fetch":
        return cmd_fetch(args)
    if args.cmd == "resolve-nonct":
        return cmd_resolve_nonct(args)
    return cmd_verify(args)


if __name__ == "__main__":
    sys.exit(main())
