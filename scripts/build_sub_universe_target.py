#!/usr/bin/env python3
"""build_sub_universe_target — operator-triggered per-target v3 pair-grain build.

Replaces the retired blob batch (scripts/build_sub_universe_blobs.py). The per-UEI
blob is dead (freeze-doc §0, 2026-07-08): it denormalized shared node facts into
every overlapping target and the two-tier rescue degraded exact-day windows to
monthly buckets. This lands, per target, ONE row set into two relational datasets:

  • gtm_sub_universe_pairs   — (target_uei × node_uei) pair-specific scalars
  • gtm_sub_universe_targets — 1 row / target: target_analytics JSON + scalars

Node-grain facts serve at query time from the indexed node-grain marts (exact-day
windows restored) — they are NOT precomputed here. Rebuild-per-target, monotonic
grow: each target's prior rows are deleted then re-appended. Operator-triggered
ONLY — NO cron, NO schedule.

Warm-process loop: the base-recipe caches (winners inverted index / farmout / pair
stats / vehicles) are module-level and build ONCE (cold) on the first target, then
amortize across every subsequent target in the run. Targets are processed one at a
time with NO cross-target accumulation in memory — each target's result is written
and dropped before the next is built.

    # single target
    doppler run --project core-x --config prd -- \
      /Users/benjamincrane/core-x/.venv/bin/python \
      scripts/build_sub_universe_target.py --uei ABC123DEF456

    # a batch of targets, one at a time (warm caches amortize)
    doppler run --project core-x --config prd -- \
      /Users/benjamincrane/core-x/.venv/bin/python \
      scripts/build_sub_universe_target.py --ueis-file /path/to/ueis.txt

    # dry run — build + measure, do NOT write
    ... scripts/build_sub_universe_target.py --uei ABC123DEF456 --dry
"""
from __future__ import annotations

import argparse
import gc
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from apps.catalyst_api.src import config  # noqa: E402
from apps.catalyst_api.src import sub_universe_pairs as P  # noqa: E402


def _read_ueis(args) -> list[str]:
    if args.uei:
        return [args.uei.strip().upper()]
    ueis: list[str] = []
    for line in Path(args.ueis_file).read_text().splitlines():
        u = line.strip().upper()
        if u and not u.startswith("#"):
            ueis.append(u)
    return ueis


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--uei", help="single target UEI")
    g.add_argument("--ueis-file", help="file of target UEIs, one per line")
    ap.add_argument("--dry", action="store_true", help="build + measure, do not write")
    args = ap.parse_args()

    ueis = _read_ueis(args)
    so = config.r2_storage_options()
    print(f"pairs_uri   = {config.GTM_SUB_UNIVERSE_PAIRS_URI}", flush=True)
    print(f"targets_uri = {config.GTM_SUB_UNIVERSE_TARGETS_URI}", flush=True)
    print(f"targets: {len(ueis)}  dry={args.dry}", flush=True)

    secs: list[float] = []
    for i, uei in enumerate(ueis, 1):
        t0 = time.monotonic()
        result = P.build_target(uei)          # warm caches after the first target
        m = result["meta"]
        n_pairs = len(result["pairs"])
        if not args.dry:
            w = P.write_target(result, storage_options=so)
            wrote = (f"  wrote pairs_v{w['pairs_version']}(+{w['pairs_rows']}, "
                     f"total {w['pairs_total_rows']:,}) "
                     f"targets_v{w['targets_version']}(total {w['targets_total_rows']:,})")
        else:
            wrote = "  [dry]"
        dt = time.monotonic() - t0
        secs.append(dt)
        gt = m["timings_ms"].get("geo_hydrate_ms")
        print(f"[{i}/{len(ueis)}] {uei}  {dt:6.2f}s  pairs={n_pairs:,}  "
              f"disc={m['n_disclosed']:,} undisc={m['n_undisclosed']:,}  "
              f"geo_hydrate={gt}ms  base={m['timings_ms'].get('base_ms')}ms"
              f"{'  TRUNCATED' if m['nodes_truncated'] else ''}{wrote}", flush=True)
        result = None
        gc.collect()

    if len(secs) > 1:
        ss = sorted(secs)
        print(f"\nper-target seconds: n={len(ss)} min={ss[0]:.2f} "
              f"p50={ss[len(ss) // 2]:.2f} max={ss[-1]:.2f} "
              f"mean={sum(ss) / len(ss):.2f} (first/cold={secs[0]:.2f})", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
