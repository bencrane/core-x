"""Drain an Icypeas bulk task's results via API — the UI CSV export truncates large files.

Finds the bulk file whose name contains the given substring (most recent first),
then pages /bulk-single-searchs/read (mode:"bulk", 100/page, paced under the
30/min shared ceiling with 429 backoff) and writes every item VERBATIM to JSONL.
Items carry ``order`` (1-based upload row position) — join results back to the
uploaded CSV positionally, never by echoed strings (Icypeas mutates echoes:
blanked titles, percent-encoding, case).

Run:
    doppler run -- python pipelines/enrichment_icypeas/drain_bulk_file.py <name-substring> <out.jsonl>
"""
from __future__ import annotations

import json
import os
import sys
import time

import httpx

BASE = os.environ.get("ICYPEAS_API_BASE", "https://app.icypeas.com/api").rstrip("/")


def _post(path: str, body: dict, tries: int = 5) -> dict:
    headers = {"Authorization": os.environ["ICYPEAS_API_KEY"],
               "Content-Type": "application/json", "Accept": "application/json"}
    for i in range(tries):
        r = httpx.post(f"{BASE}{path}", json=body, headers=headers, timeout=60)
        if r.status_code == 429:
            time.sleep(15 * (i + 1))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"429 persisted on {path}")


def main(name_substring: str, out_path: str) -> None:
    files = _post("/search-files/read", {"limit": 25})
    recs = files.get("items") or files.get("files") or files.get("data") or []
    target = None
    for rec in recs:
        name = str(rec.get("name") or rec.get("filename") or "")
        if name_substring in name:
            target = rec.get("_id") or rec.get("id") or rec.get("file")
            print(f"target: {target}  {name!r}  finished={rec.get('finished')}")
            break
    if not target:
        raise SystemExit(f"no bulk file matching {name_substring!r} in last {len(recs)}")

    n, pages, sorts = 0, 0, None
    with open(out_path, "w") as f:
        while True:
            body: dict = {"mode": "bulk", "file": target, "limit": 100}
            if sorts:
                body["sorts"] = sorts
                body["next"] = True
            data = _post("/bulk-single-searchs/read", body)
            items = data.get("items") or []
            for it in items:
                f.write(json.dumps(it) + "\n")
            n += len(items)
            pages += 1
            sorts = data.get("sorts")
            if pages % 25 == 0:
                print(f"  page {pages}: {n:,} items", flush=True)
            if len(items) < 100 or not sorts:
                break
            time.sleep(2.5)
    print(f"DONE: {n:,} items in {pages} pages -> {out_path}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
