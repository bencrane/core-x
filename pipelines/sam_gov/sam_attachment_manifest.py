"""SAM.gov solicitation *attachment manifest* harvest — local/in-session runner.

Builds the pointer layer the attachment-download backfill consumes: one row per
(notice, attachment) with the open download URL + filename/mime/size, harvested
trigger-first then across the remaining active universe.

WHY THE sam.gov FRONTEND (not api.sam.gov): the developer gateway
``api.sam.gov`` (api.data.gov) caps non-federal keys at a tiny per-day quota
(resets 00:00 UTC) — useless for a per-notice sweep. The PUBLIC website backend
``sam.gov/api/prod/opps/...`` — the same unauthenticated origin the bulk CSV
comes from, and what the browser itself calls — serves the per-notice attachment
list AND the file bytes with **no api_key and no developer quota**:

    GET https://sam.gov/api/prod/opps/v3/opportunities/{notice_id}/resources
        -> _embedded.opportunityAttachmentList[].attachments[]
           {resourceId, name, mimeType, size, attachmentOrder, accessLevel, ...}
    GET https://sam.gov/api/prod/opps/v3/opportunities/resources/files/{resourceId}/download
        -> 206 application/octet-stream (the PDF/DOC bytes; Range-resumable)

Header note: the endpoint serves ``application/hal+json`` and 406s a strict
``Accept: application/json`` — must send ``application/json, text/plain, */*``.

Runs LOCAL/in-session from a residential IP (polite paced crawl), creds for the
R2 Lance read via Doppler, deps via uv. No SAM_API_KEY needed.

Output: ``s3://data-sink/active/sam_opps_attachment_manifest/`` (Lance v2.0),
grain = one row per attachment. ``download_url`` carries no secret.

KNOWN DEFECT — ``size_bytes`` IS A LOWER BOUND, NOT TRUE SIZE (verified live
2026-06-06). The source field ``a.get("size")`` from the attachment-list API is
corrupted for files >=10 MB: SAM returns ``((true_size - 1) mod 10_000_000) + 1``
— the true size with an unknown multiple of 10 MB subtracted. So ``size_bytes``
is a LOWER BOUND on true bytes, EXACT only when true size < 10 MB, and NOT
invertible (the 10 MB multiplier is destroyed — a declared 5 MB row may be a real
45 MB file). True size is knowable only by measuring Content-Length at fetch; the
byte-download stage records it as ``size_downloaded`` in the file ledger
(``sam_attachment_files``), which is the authority for real sizes. Examples
(declared -> real): 5,066,771 -> 45,066,771; 4,019,768 -> 14,019,768;
2,253,038 -> 32,253,038; 10,000,000 -> ~210 MB. Consumers must NOT treat
``size_bytes`` as a true size, a cap, or a storage budget above 10 MB — enforce
real size at fetch (see ``sam_attachment_download.py`` _download_one).

    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with requests --with 'psycopg[binary]' \
      python pipelines/sam_gov/sam_attachment_manifest.py --do-remaining --resume

Smoke (first N notices, throwaway URI):
    ... python pipelines/sam_gov/sam_attachment_manifest.py \
        --no-do-remaining --max-notices 40 \
        --manifest-uri s3://data-sink/active/_smoke_sam_attach_manifest/
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time

SAM_OPPS_ACTIVE_URI = os.environ.get("SAM_OPPS_LANCE_URI", "s3://data-sink/sam-gov-opps/active/")
MANIFEST_URI = os.environ.get("SAM_ATTACH_MANIFEST_URI", "s3://data-sink/active/sam_opps_attachment_manifest/")
RESOURCES_URL = "https://sam.gov/api/prod/opps/v3/opportunities/{nid}/resources"
DOWNLOAD_URL = "https://sam.gov/api/prod/opps/v3/opportunities/resources/files/{rid}/download"
TRIGGER_PSC = ("N063", "C1AZ")
FEED = "sam_opps_attachment_manifest"


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _record_run(stats: dict, dsn: str | None) -> None:
    if not dsn:
        print("WARN: no HQX_DB_URL_POOLED; skipping ops.* write.", flush=True)
        return
    try:
        import psycopg

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ops.sam_attachment_manifest_runs (
                    id bigserial PRIMARY KEY, feed text NOT NULL, status text NOT NULL,
                    active_total int, trigger_total int, notices_covered int,
                    trigger_covered int, attachments int, trigger_attachments int,
                    zero_attach_notices int, uncovered_notices int, api_calls int,
                    phase_b_ran boolean, stats jsonb, error text,
                    started_at timestamptz, completed_at timestamptz)
                """
            )
            cur.execute(
                """
                INSERT INTO ops.sam_attachment_manifest_runs
                  (feed,status,active_total,trigger_total,notices_covered,trigger_covered,
                   attachments,trigger_attachments,zero_attach_notices,uncovered_notices,
                   api_calls,phase_b_ran,stats,error,started_at,completed_at)
                VALUES (%(feed)s,%(status)s,%(active_total)s,%(trigger_total)s,%(notices_covered)s,
                   %(trigger_covered)s,%(attachments)s,%(trigger_attachments)s,%(zero_attach_notices)s,
                   %(uncovered_notices)s,%(api_calls)s,%(phase_b_ran)s,%(stats)s,%(error)s,
                   %(started_at)s,%(completed_at)s)
                """,
                {**stats, "stats": json.dumps(stats.get("stats", {}))},
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* write failed: {exc}", flush=True)


def _headers(nid: str) -> dict:
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
        "Accept": "application/json, text/plain, */*",  # hal+json 406s a strict json Accept
        "Origin": "https://sam.gov",
        "Referer": f"https://sam.gov/opp/{nid}/view",
    }


def run_harvest(
    *,
    storage_options: dict,
    dsn: str | None,
    sam_active_uri: str = SAM_OPPS_ACTIVE_URI,
    manifest_uri: str = MANIFEST_URI,
    do_remaining: bool = True,
    max_notices: int = 0,
    checkpoint_every: int = 500,
    inter_call_sleep: float = 0.12,
    resume: bool = False,
) -> dict:
    import lance
    import pyarrow as pa
    import requests

    started_at = dt.datetime.now(dt.timezone.utc)
    today = dt.date.today()
    session = requests.Session()
    calls = {"n": 0}

    # ---- active set -----------------------------------------------------
    ds = lance.dataset(sam_active_uri, storage_options=storage_options)
    src = ds.to_table(columns=[
        "notice_id", "solicitation_number", "naics_code",
        "classification_code", "title", "posted_date", "link",
    ]).to_pylist()

    meta: dict[str, dict] = {}
    for r in src:
        nid = r["notice_id"]
        if not nid:
            continue
        naics = (r["naics_code"] or "").strip()
        psc = (r["classification_code"] or "").strip()
        legs = []
        if naics.startswith("23"):
            legs.append("naics23")
        if psc in TRIGGER_PSC:
            legs.append("psc")
        meta[nid] = {
            "solicitation_number": r["solicitation_number"],
            "naics_code": naics or None, "psc_code": psc or None,
            "title": r["title"], "posted_date": r["posted_date"],
            "ui_link": r["link"], "trigger": bool(legs), "legs": ";".join(legs),
        }
    active_total = len(meta)
    trigger_ids = [n for n, m in meta.items() if m["trigger"]]
    remaining_ids = [n for n, m in meta.items() if not m["trigger"]]
    # deterministic, trigger-first ordering
    order = sorted(trigger_ids) + (sorted(remaining_ids) if do_remaining else [])
    print(f"active_total={active_total} trigger={len(trigger_ids)} "
          f"remaining_queued={len(order) - len(trigger_ids)}", flush=True)

    rows: list[dict] = []
    captured: set[str] = set()
    zero_attach: set[str] = set()
    harvested_at = dt.datetime.now(dt.timezone.utc)
    snapshot_date = today

    schema = pa.schema([
        ("notice_id", pa.string()), ("solicitation_number", pa.string()),
        ("naics_code", pa.string()), ("psc_code", pa.string()), ("title", pa.string()),
        ("posted_date", pa.timestamp("us")), ("ui_link", pa.string()),
        ("trigger_relevant", pa.bool_()), ("trigger_legs", pa.string()),
        ("attachment_id", pa.string()), ("resource_id", pa.string()),
        ("attachment_order", pa.int32()), ("file_name", pa.string()),
        ("mime_type", pa.string()), ("size_bytes", pa.int64()),
        ("access_level", pa.string()), ("export_controlled", pa.bool_()),
        ("download_url", pa.string()), ("harvested_at", pa.timestamp("us", tz="UTC")),
        ("snapshot_date", pa.date32()),
    ])

    if resume:
        try:
            prior = lance.dataset(manifest_uri, storage_options=storage_options).to_table().to_pylist()
            for p in prior:
                rows.append(p)
                captured.add(p["notice_id"])
            print(f"resume: reloaded {len(rows)} rows / {len(captured)} notices", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"resume: no prior manifest ({exc}); fresh start", flush=True)

    def write_manifest(tag: str) -> None:
        if not rows:
            print(f"[{tag}] no rows yet", flush=True); return
        at = pa.Table.from_pylist(rows, schema=schema)
        lance.write_dataset(at, manifest_uri, mode="overwrite",
                            data_storage_version="2.0", storage_options=storage_options)
        print(f"[{tag}] wrote {at.num_rows} rows / "
              f"{len(captured) - len(zero_attach)} notices-with-attach (calls={calls['n']})", flush=True)

    def fetch_resources(nid: str):
        url = RESOURCES_URL.format(nid=nid)
        for attempt in range(6):
            calls["n"] += 1
            try:
                resp = session.get(url, headers=_headers(nid), timeout=45)
            except requests.RequestException as exc:
                time.sleep(min(30, 2 ** attempt)); continue
            sc = resp.status_code
            if sc == 200:
                return resp.json()
            if sc in (403, 429, 503):     # WAF / throttle → back off and retry
                w = min(120, 5 * 2 ** attempt)
                print(f"  {sc} on {nid[:8]}; backoff {w}s (call#{calls['n']})", flush=True)
                time.sleep(w); continue
            if sc >= 500:
                time.sleep(min(30, 2 ** attempt)); continue
            return None   # 404/406/etc → no resources for this notice
        return None

    def attachments_of(j) -> list:
        if not j:
            return []
        lst = (j.get("_embedded", {}) or {}).get("opportunityAttachmentList", []) or []
        return lst[0].get("attachments", []) if lst else []

    final_status, error_text = "error", None
    try:
        todo = [n for n in order if n not in captured]
        if max_notices:
            todo = todo[:max_notices]
        print(f"todo={len(todo)} (after resume-skip)", flush=True)
        for i, nid in enumerate(todo):
            m = meta[nid]
            atts = attachments_of(fetch_resources(nid))
            captured.add(nid)
            if not atts:
                zero_attach.add(nid)
            for a in atts:
                rid = a.get("resourceId")
                if not rid or str(a.get("fileExists", "1")) == "0" or str(a.get("deletedFlag", "0")) == "1":
                    continue
                rows.append({
                    "notice_id": nid, "solicitation_number": m["solicitation_number"],
                    "naics_code": m["naics_code"], "psc_code": m["psc_code"], "title": m["title"],
                    "posted_date": m["posted_date"], "ui_link": m["ui_link"],
                    "trigger_relevant": m["trigger"], "trigger_legs": m["legs"],
                    "attachment_id": a.get("attachmentId"), "resource_id": rid,
                    "attachment_order": int(a.get("attachmentOrder") or 0),
                    "file_name": a.get("name"), "mime_type": (a.get("mimeType") or "").lstrip("."),
                    # LOWER BOUND, not true size: SAM corrupts >=10 MB sizes to
                    # ((true-1) mod 10_000_000)+1. See module docstring KNOWN DEFECT.
                    "size_bytes": int(a.get("size") or 0),
                    "access_level": a.get("accessLevel"),
                    "export_controlled": str(a.get("exportControlled", "0")) == "1",
                    "download_url": DOWNLOAD_URL.format(rid=rid),
                    "harvested_at": harvested_at, "snapshot_date": snapshot_date,
                })
            if inter_call_sleep:
                time.sleep(inter_call_sleep)
            if checkpoint_every and (i + 1) % checkpoint_every == 0:
                pct = 100 * (i + 1) / len(todo)
                print(f"  progress {i+1}/{len(todo)} ({pct:.0f}%) trig_done={len(captured & set(trigger_ids))}/{len(trigger_ids)}", flush=True)
                write_manifest(f"ckpt_{i+1}")
        write_manifest("final")
        final_status = "success"
    except KeyboardInterrupt:
        final_status = "interrupted"; print("interrupted — checkpointing", flush=True)
        write_manifest("interrupt")
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc); print(f"FATAL: {exc}", flush=True)
        try:
            write_manifest("salvage")
        except Exception as e2:  # noqa: BLE001
            print(f"salvage failed: {e2}", flush=True)
    finally:
        try:
            mds = lance.dataset(manifest_uri, storage_options=storage_options)
            for col, it in [("notice_id", "BTREE"), ("resource_id", "BTREE"),
                            ("naics_code", "BTREE"), ("trigger_relevant", "BITMAP"),
                            ("mime_type", "BITMAP"), ("access_level", "BITMAP")]:
                try:
                    mds.create_scalar_index(col, index_type=it)
                except Exception as ie:  # noqa: BLE001
                    print(f"index {col} skipped: {ie}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"index phase skipped: {exc}", flush=True)

        completed_at = dt.datetime.now(dt.timezone.utc)
        # DECLARED lower bound only — size_bytes is corrupted (mod 10 MB) for >=10 MB
        # files, so this undercounts true bytes. Real totals come from the file ledger.
        total_bytes_declared_lb = sum(int(r["size_bytes"] or 0) for r in rows)
        stats = {
            "feed": FEED, "status": final_status, "active_total": active_total,
            "trigger_total": len(trigger_ids), "notices_covered": len(captured),
            "trigger_covered": len(captured & set(trigger_ids)), "attachments": len(rows),
            "trigger_attachments": sum(1 for r in rows if r["trigger_relevant"]),
            "zero_attach_notices": len(zero_attach),
            "uncovered_notices": active_total - len(captured),
            "api_calls": calls["n"], "phase_b_ran": do_remaining, "error": error_text,
            "started_at": started_at, "completed_at": completed_at,
            "stats": {"manifest_uri": manifest_uri,
                      "total_bytes_declared_lowerbound": total_bytes_declared_lb,
                      "source": "sam.gov/api/prod/opps/v3 (frontend, no api_key)"},
        }
        _record_run(stats, dsn)
        print("SUMMARY:", {k: v for k, v in stats.items() if k != "stats"}, flush=True)
        print(f"total_attachment_bytes(declared LOWER BOUND; >=10 MB undercounted)="
              f"{total_bytes_declared_lb:,} (~{total_bytes_declared_lb/1e9:.1f} GB)", flush=True)
    return {k: stats[k] for k in ("status", "attachments", "notices_covered",
                                  "trigger_covered", "uncovered_notices", "api_calls")}


def _cli() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--do-remaining", dest="do_remaining", action="store_true", default=True)
    p.add_argument("--no-do-remaining", dest="do_remaining", action="store_false")
    p.add_argument("--resume", action="store_true", default=False)
    p.add_argument("--max-notices", type=int, default=0)
    p.add_argument("--manifest-uri", default=MANIFEST_URI)
    p.add_argument("--checkpoint-every", type=int, default=500)
    p.add_argument("--inter-call-sleep", type=float, default=0.12)
    a = p.parse_args()
    out = run_harvest(
        storage_options=_r2_storage_options(), dsn=os.environ.get("HQX_DB_URL_POOLED"),
        manifest_uri=a.manifest_uri, do_remaining=a.do_remaining, max_notices=a.max_notices,
        checkpoint_every=a.checkpoint_every, inter_call_sleep=a.inter_call_sleep, resume=a.resume,
    )
    print("RESULT:", out, flush=True)


if __name__ == "__main__":
    _cli()
