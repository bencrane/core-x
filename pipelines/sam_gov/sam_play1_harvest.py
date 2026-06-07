"""Play-1 A&D-target attachment-manifest harvest — SHARDED orchestrator.

The shared harvester (``sam_attachment_manifest.run_harvest``) accumulates every
attachment row in memory and OVERWRITES its whole manifest at each checkpoint — fine
for the ~79K active universe, but it does not scale to the 275K-notice Play-1 target
(~1.4M attachment rows: O(n²) rewrites, unbounded RAM, and the resulting >1 GB
multipart write is rejected by R2 with 400 InvalidPart).

So shard: split the target universe into SHARD_SIZE-notice slices and harvest each into
its OWN manifest dataset — every write stays small (R2-safe) and RAM stays bounded.
Sequential + single-threaded (residential-IP politeness; runs alongside the active-tier
byte download, aggregate stays within the sam.gov budget). Resumable: a finished shard
is skipped via a marker file; an interrupted shard resumes via the harvester's own
``resume=True`` against its shard manifest. Union the shard manifests downstream
(``sam_opps_attachment_manifest_play1/shard_*``).

    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with requests --with 'psycopg[binary]' \
      python pipelines/sam_gov/sam_play1_harvest.py

Smoke:
    ... python pipelines/sam_gov/sam_play1_harvest.py \
        --shard-size 50 --max-shards 1 --max-notices-per-shard 40 \
        --manifest-base s3://data-sink/active/_play1_smoke_shard_{i:03d}/
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sam_attachment_manifest import _r2_storage_options, run_harvest  # noqa: E402 — reuse proven harvester

TARGET_URI = os.environ.get("SAM_PLAY1_TARGET_URI", "s3://data-sink/active/_play1_target_universe/")
MANIFEST_BASE = "s3://data-sink/active/sam_opps_attachment_manifest_play1/shard_{i:03d}/"
SHARD_UNIVERSE_URI = "s3://data-sink/active/_play1_shard_universe/"  # tiny scratch, overwritten per shard
MARKER_DIR = "/tmp/play1_markers"
HARVEST_COLS = ["notice_id", "solicitation_number", "naics_code", "classification_code",
                "title", "posted_date", "link"]


def main() -> None:
    import lance

    p = argparse.ArgumentParser()
    p.add_argument("--shard-size", type=int, default=50_000)
    p.add_argument("--inter-call-sleep", type=float, default=0.1)
    p.add_argument("--checkpoint-every", type=int, default=2000)
    p.add_argument("--max-shards", type=int, default=0, help="0 = all from --shard-start (smoke/worker knob)")
    p.add_argument("--max-notices-per-shard", type=int, default=0, help="0 = all (smoke knob)")
    p.add_argument("--shard-start", type=int, default=0,
                   help="first shard index — disjoint ranges let multiple workers run in parallel")
    p.add_argument("--scratch-uri", default=SHARD_UNIVERSE_URI,
                   help="per-worker scratch universe URI (must differ between concurrent workers)")
    p.add_argument("--manifest-base", default=MANIFEST_BASE)
    p.add_argument("--target-uri", default=TARGET_URI)
    p.add_argument("--marker-dir", default=MARKER_DIR,
                   help="per-vertical marker dir — MUST differ across NAICS verticals or "
                        "one vertical's done-markers will skip another's shards")
    a = p.parse_args()

    so = _r2_storage_options()
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    os.makedirs(a.marker_dir, exist_ok=True)

    tgt = lance.dataset(a.target_uri, storage_options=so).to_table(columns=HARVEST_COLS)
    n = tgt.num_rows
    total_shards = (n + a.shard_size - 1) // a.shard_size
    start = a.shard_start
    end = min(total_shards, start + a.max_shards) if a.max_shards else total_shards
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}] "
          f"target={n:,} notices, {total_shards} total shards of {a.shard_size:,}; "
          f"this worker processes shards [{start},{end})", flush=True)

    for i in range(start, end):
        marker = os.path.join(a.marker_dir, f"shard_{i:03d}.done")
        if os.path.exists(marker):
            print(f"shard {i:03d}: marker present — skip", flush=True)
            continue
        sl = tgt.slice(i * a.shard_size, a.shard_size)
        if a.max_notices_per_shard:
            sl = sl.slice(0, a.max_notices_per_shard)
        # tiny scratch universe (≤50K rows, 7 small cols → single-part R2 write, safe)
        lance.write_dataset(sl, a.scratch_uri, mode="overwrite",
                            data_storage_version="2.0", storage_options=so)
        muri = a.manifest_base.format(i=i)
        print(f"=== shard {i:03d}: {sl.num_rows:,} notices -> {muri} ===", flush=True)
        try:
            run_harvest(
                storage_options=so, dsn=dsn, sam_active_uri=a.scratch_uri,
                manifest_uri=muri, do_remaining=True,
                max_notices=a.max_notices_per_shard or 0,
                checkpoint_every=a.checkpoint_every, inter_call_sleep=a.inter_call_sleep,
                resume=True,
            )
            with open(marker, "w") as fh:
                fh.write(dt.datetime.now(dt.timezone.utc).isoformat())
            print(f"shard {i:03d}: DONE (marker written)", flush=True)
        except Exception as exc:  # noqa: BLE001 — one shard's failure must not abort the run
            print(f"shard {i:03d}: FAILED ({exc}); leaving unmarked for retry on relaunch", flush=True)

    print("ALL REQUESTED SHARDS PROCESSED", flush=True)


if __name__ == "__main__":
    main()
