"""SAM.gov solicitation *attachment manifest* harvest — local/in-session runner.

Builds the pointer layer the attachment-download backfill will consume: one row
per (notice, attachment) with the `api.sam.gov` download URL, harvested for the
trigger-relevant slice first (guaranteed, durably written) then the remaining
active universe (time-boxed, best-effort).

WHY LOCAL (not Modal): ``api.sam.gov`` (api.data.gov gateway) hard-throttles
shared datacenter egress IPs — a Modal worker gets HTTP 429 on the very first
call, every call. The same ``SAM_API_KEY`` works from a residential IP. So this
runs in-session on an operator machine (plugged in / kept awake), writing the
Lance manifest directly to R2. The per-key hourly limit (~api.data.gov default
1,000/hr) is handled by reactive 429 backoff; the IP block is sidestepped.

Method (clean-room, Lance/Arrow only)
-------------------------------------
1. Read the authoritative active set from ``s3://data-sink/sam-gov-opps/active/``.
2. Tag the trigger slice: ``naics_code LIKE '23%' OR classification_code IN
   ('N063','C1AZ')`` (the GovCon T1/T2/T3 structural signal present on a
   pre-award solicitation — award value is NOT, so it is not a filter here).
3. Precompute (filter, code, date-window) buckets covering each still-needed
   notice; skip empty buckets, so a lone 2008-era notice costs one query.
4. Walk each bucket against ``/opportunities/v2/search`` with ``ncode``/``ccode``
   + a <1yr posted window, paginate at limit=1000, capture ``resourceLinks``.
5. Phase A = trigger slice → write manifest (durable). Phase B = remaining,
   periodic checkpoints + wall-clock budget → rewrite manifest with full set.

Output: ``s3://data-sink/active/sam_opps_attachment_manifest/`` (Lance v2.0),
grain = one row per attachment. The download URL is stored WITHOUT the api_key
(appended at download time) — no secret is baked into the dataset.

Run (creds injected by Doppler, deps by uv):
    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with requests --with 'psycopg[binary]' \
      python pipelines/sam_gov/sam_attachment_manifest.py --do-remaining --resume

Smoke (2 buckets, throwaway URI):
    ... python pipelines/sam_gov/sam_attachment_manifest.py \
        --no-do-remaining --max-buckets 2 \
        --manifest-uri s3://data-sink/active/_smoke_sam_attach_manifest/
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from collections import defaultdict

SAM_OPPS_ACTIVE_URI = os.environ.get("SAM_OPPS_LANCE_URI", "s3://data-sink/sam-gov-opps/active/")
MANIFEST_URI = os.environ.get("SAM_ATTACH_MANIFEST_URI", "s3://data-sink/active/sam_opps_attachment_manifest/")
SAM_SEARCH_URL = "https://api.sam.gov/opportunities/v2/search"
TRIGGER_PSC = ("N063", "C1AZ")
FEED = "sam_opps_attachment_manifest"
DAILY_WALL_THRESHOLD = 1800  # a Retry-After beyond this = daily quota wall → checkpoint + exit


class DailyQuotaExhausted(Exception):
    """Raised when api.sam.gov returns 429 with a far-future (daily) Retry-After."""

    def __init__(self, reset_at: str):
        self.reset_at = reset_at
        super().__init__(f"daily quota exhausted; resets {reset_at}")


def _retry_after_seconds(headers) -> int | None:
    """Seconds to wait from a Retry-After header (int-seconds or HTTP-date)."""
    ra = headers.get("Retry-After")
    if not ra:
        return None
    ra = ra.strip()
    if ra.isdigit():
        return int(ra)
    try:
        from email.utils import parsedate_to_datetime
        when = parsedate_to_datetime(ra)
        return max(0, int((when - dt.datetime.now(dt.timezone.utc)).total_seconds()))
    except Exception:  # noqa: BLE001
        return None


def probe_cap(api_key: str) -> None:
    """One cheap call to read the live rate-limit ceiling/remaining from headers."""
    import requests

    params = {"postedFrom": "05/07/2026", "postedTo": "06/06/2026",
              "limit": 1, "ncode": "236220", "api_key": api_key}
    r = requests.get(SAM_SEARCH_URL, params=params, timeout=60)
    rl = {k: v for k, v in r.headers.items() if "ratelimit" in k.lower() or k.lower() == "retry-after"}
    print(f"probe: HTTP {r.status_code} | ratelimit_headers={rl}", flush=True)
    if r.status_code == 200:
        print(f"probe: totalRecords={r.json().get('totalRecords')} (quota AVAILABLE)", flush=True)
    else:
        print(f"probe: body={r.text[:200]}", flush=True)


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
    """Terminal run row → ops.sam_attachment_manifest_runs. Best-effort."""
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


def run_harvest(
    *,
    storage_options: dict,
    api_key: str,
    dsn: str | None,
    sam_active_uri: str = SAM_OPPS_ACTIVE_URI,
    manifest_uri: str = MANIFEST_URI,
    do_remaining: bool = True,
    phase_b_budget_seconds: int = 60 * 60 * 4,
    window_span_days: int = 350,
    page_limit: int = 1000,
    max_buckets: int = 0,
    checkpoint_every: int = 40,
    inter_call_sleep: float = 0.25,
    resume: bool = False,
) -> dict:
    import lance
    import pyarrow as pa
    import requests

    started_at = dt.datetime.now(dt.timezone.utc)
    today = dt.date.today()
    session = requests.Session()
    call_count = {"n": 0}
    rl_logged = {"done": False}

    # ---- 1. active set --------------------------------------------------
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
        pdt = r["posted_date"]
        legs = []
        if naics.startswith("23"):
            legs.append("naics23")
        if psc in TRIGGER_PSC:
            legs.append("psc")
        meta[nid] = {
            "solicitation_number": r["solicitation_number"],
            "naics_code": naics or None, "psc_code": psc or None,
            "title": r["title"], "posted_date": pdt,
            "posted_d": (pdt.date() if hasattr(pdt, "date") else pdt),
            "ui_link": r["link"], "trigger": bool(legs), "legs": ";".join(legs),
        }
    active_total = len(meta)
    trigger_ids = {n for n, m in meta.items() if m["trigger"]}
    print(f"active_total={active_total} trigger_total={len(trigger_ids)}", flush=True)

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
        ("resource_seq", pa.int32()), ("resource_id", pa.string()),
        ("resource_url", pa.string()), ("harvested_at", pa.timestamp("us", tz="UTC")),
        ("snapshot_date", pa.date32()),
    ])

    # ---- resume: reload prior manifest ----------------------------------
    if resume:
        try:
            prior = lance.dataset(manifest_uri, storage_options=storage_options).to_table().to_pylist()
            for p in prior:
                rows.append(p)
                captured.add(p["notice_id"])
            print(f"resume: reloaded {len(rows)} rows / {len(captured)} notices", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"resume: no prior manifest ({exc}); starting fresh", flush=True)

    def make_windows(floor: dt.date):
        ws, f = [], floor
        while f <= today:
            t = min(f + dt.timedelta(days=window_span_days - 1), today)
            ws.append((f, t)); f = t + dt.timedelta(days=1)
        return ws

    def widx(d: dt.date, floor: dt.date) -> int:
        return (d - floor).days // window_span_days

    def _get(params: dict) -> dict | None:
        for attempt in range(8):
            call_count["n"] += 1
            try:
                resp = session.get(SAM_SEARCH_URL, params={**params, "api_key": api_key}, timeout=90)
            except requests.RequestException as exc:
                w = min(60, 2 ** attempt); print(f"  net err {exc}; {w}s", flush=True); time.sleep(w); continue
            if not rl_logged["done"]:
                rh = {k: v for k, v in resp.headers.items() if "ratelimit" in k.lower() or k.lower() == "retry-after"}
                if rh:
                    print(f"  ratelimit headers: {rh}", flush=True); rl_logged["done"] = True
            if resp.status_code == 200:
                if inter_call_sleep:
                    time.sleep(inter_call_sleep)
                return resp.json()
            if resp.status_code == 429:
                secs = _retry_after_seconds(resp.headers)
                if secs is not None and secs > DAILY_WALL_THRESHOLD:
                    reset_at = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=secs)).isoformat()
                    raise DailyQuotaExhausted(reset_at)
                w = min(secs if secs is not None else 10 * 2 ** attempt, 600)
                print(f"  429; sleep {w}s (call#{call_count['n']})", flush=True); time.sleep(w); continue
            if resp.status_code >= 500:
                w = min(60, 2 ** attempt); print(f"  {resp.status_code}; {w}s", flush=True); time.sleep(w); continue
            print(f"  hard {resp.status_code}: {resp.text[:160]}", flush=True); return None
        return None

    def capture(opp: dict) -> None:
        nid = opp.get("noticeId")
        if nid not in meta or nid in captured:
            return
        captured.add(nid)
        m = meta[nid]
        links = opp.get("resourceLinks") or []
        if not links:
            zero_attach.add(nid); return
        for seq, url in enumerate(links):
            if not url:
                continue
            rid = url.rstrip("/").split("/")[-1].split("?")[0]
            rows.append({
                "notice_id": nid, "solicitation_number": m["solicitation_number"],
                "naics_code": m["naics_code"], "psc_code": m["psc_code"], "title": m["title"],
                "posted_date": m["posted_date"], "ui_link": m["ui_link"],
                "trigger_relevant": m["trigger"], "trigger_legs": m["legs"],
                "resource_seq": seq, "resource_id": rid, "resource_url": url,
                "harvested_at": harvested_at, "snapshot_date": snapshot_date,
            })

    def write_manifest(tag: str) -> None:
        if not rows:
            print(f"[{tag}] no rows yet", flush=True); return
        at = pa.Table.from_pylist(rows, schema=schema)
        lance.write_dataset(at, manifest_uri, mode="overwrite",
                            data_storage_version="2.0", storage_options=storage_options)
        print(f"[{tag}] wrote {at.num_rows} rows / {len(captured) - len(zero_attach)} notices "
              f"(api_calls={call_count['n']})", flush=True)

    def walk(needed: set[str], label: str, deadline: float | None) -> None:
        if not needed:
            return
        floors = [meta[n]["posted_d"] for n in needed if meta[n]["posted_d"]]
        if not floors:
            return
        floor = min(floors)
        windows = make_windows(floor)
        # bucket -> list of needed notice_ids
        buckets: dict[tuple, list] = defaultdict(list)
        uncoverable = 0
        for nid in needed:
            m = meta[nid]; d = m["posted_d"]
            if d is None:
                uncoverable += 1; continue
            wi = widx(d, floor)
            if m["naics_code"]:
                buckets[("ncode", m["naics_code"], wi)].append(nid)
            elif m["psc_code"] in TRIGGER_PSC:
                buckets[("ccode", m["psc_code"], wi)].append(nid)
            else:
                uncoverable += 1
        print(f"[{label}] needed={len(needed)} buckets={len(buckets)} uncoverable={uncoverable}", flush=True)
        done = 0
        for bi, ((ftype, code, wi), ids) in enumerate(sorted(buckets.items())):
            if max_buckets and bi >= max_buckets:
                print(f"[{label}] max_buckets cap", flush=True); break
            if deadline and time.time() > deadline:
                print(f"[{label}] budget reached at {bi}/{len(buckets)}", flush=True); break
            want = set(ids)
            if want <= captured:           # resume / already-done bucket → no call
                continue
            if wi >= len(windows):
                continue
            wfrom, wto = windows[wi]
            params = {"postedFrom": wfrom.strftime("%m/%d/%Y"), "postedTo": wto.strftime("%m/%d/%Y"),
                      "limit": page_limit, "offset": 0, ftype: code}
            try:
                while True:
                    data = _get(params)
                    if not data:
                        break
                    ops = data.get("opportunitiesData", []) or []
                    for opp in ops:
                        capture(opp)
                    total = data.get("totalRecords", 0)
                    params["offset"] += page_limit
                    if params["offset"] >= total or not ops or want <= captured:
                        break
            except DailyQuotaExhausted:
                raise
            except Exception as exc:  # noqa: BLE001
                print(f"[{label}] bucket {ftype}={code} w{wi}: {exc}", flush=True)
            done += 1
            if checkpoint_every and done % checkpoint_every == 0:
                write_manifest(f"{label}_ckpt_{bi}")

    final_status, error_text, phase_b_ran = "error", None, False
    try:
        walk(set(trigger_ids), "phase_a_trigger", deadline=None)
        write_manifest("phase_a")
        print(f"phase_a done: trigger_covered={len(captured & trigger_ids)}/{len(trigger_ids)} "
              f"api_calls={call_count['n']}", flush=True)
        if do_remaining:
            phase_b_ran = True
            walk(set(meta) - captured, "phase_b_remaining", deadline=time.time() + phase_b_budget_seconds)
            write_manifest("phase_b_final")
        final_status = "success"
    except DailyQuotaExhausted as q:
        final_status = "quota_paused"; error_text = str(q)
        print(f"QUOTA WALL: {q} — checkpointing for --resume", flush=True)
        try:
            write_manifest("quota_pause")
        except Exception as e2:  # noqa: BLE001
            print(f"quota-pause write failed: {e2}", flush=True)
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc); print(f"FATAL: {exc}", flush=True)
        try:
            write_manifest("salvage")
        except Exception as exc2:  # noqa: BLE001
            print(f"salvage failed: {exc2}", flush=True)
    finally:
        try:
            mds = lance.dataset(manifest_uri, storage_options=storage_options)
            for col, it in [("notice_id", "BTREE"), ("naics_code", "BTREE"),
                            ("resource_id", "BTREE"), ("trigger_relevant", "BITMAP"),
                            ("psc_code", "BITMAP")]:
                try:
                    mds.create_scalar_index(col, index_type=it)
                except Exception as ie:  # noqa: BLE001
                    print(f"index {col} skipped: {ie}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"index phase skipped: {exc}", flush=True)

        completed_at = dt.datetime.now(dt.timezone.utc)
        stats = {
            "feed": FEED, "status": final_status, "active_total": active_total,
            "trigger_total": len(trigger_ids), "notices_covered": len(captured),
            "trigger_covered": len(captured & trigger_ids), "attachments": len(rows),
            "trigger_attachments": sum(1 for r in rows if r["trigger_relevant"]),
            "zero_attach_notices": len(zero_attach), "uncovered_notices": active_total - len(captured),
            "api_calls": call_count["n"], "phase_b_ran": phase_b_ran, "error": error_text,
            "started_at": started_at, "completed_at": completed_at,
            "stats": {"manifest_uri": manifest_uri, "window_span_days": window_span_days},
        }
        _record_run(stats, dsn)
        print("SUMMARY:", {k: v for k, v in stats.items() if k != "stats"}, flush=True)
    return {k: stats[k] for k in ("status", "attachments", "notices_covered", "trigger_covered",
                                  "uncovered_notices", "api_calls")}


def _cli() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--do-remaining", dest="do_remaining", action="store_true", default=True)
    p.add_argument("--no-do-remaining", dest="do_remaining", action="store_false")
    p.add_argument("--resume", action="store_true", default=False)
    p.add_argument("--max-buckets", type=int, default=0)
    p.add_argument("--manifest-uri", default=MANIFEST_URI)
    p.add_argument("--phase-b-budget-seconds", type=int, default=60 * 60 * 4)
    p.add_argument("--window-span-days", type=int, default=350)
    p.add_argument("--page-limit", type=int, default=1000)
    p.add_argument("--checkpoint-every", type=int, default=40)
    p.add_argument("--inter-call-sleep", type=float, default=0.25)
    p.add_argument("--probe-only", action="store_true", default=False,
                   help="one call to read the live rate-limit ceiling, then exit")
    a = p.parse_args()
    try:
        api_key = os.environ["SAM_API_KEY"]
    except KeyError:
        sys.exit("SAM_API_KEY not in env (run under `doppler run`).")
    if a.probe_only:
        probe_cap(api_key)
        return
    out = run_harvest(
        storage_options=_r2_storage_options(), api_key=api_key,
        dsn=os.environ.get("HQX_DB_URL_POOLED"), manifest_uri=a.manifest_uri,
        do_remaining=a.do_remaining, phase_b_budget_seconds=a.phase_b_budget_seconds,
        window_span_days=a.window_span_days, page_limit=a.page_limit,
        max_buckets=a.max_buckets, checkpoint_every=a.checkpoint_every,
        inter_call_sleep=a.inter_call_sleep, resume=a.resume,
    )
    print("RESULT:", out, flush=True)


if __name__ == "__main__":
    _cli()
