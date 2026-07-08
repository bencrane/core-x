#!/usr/bin/env python3
"""gtm_sub_universe_blobs — the Surface-1 precompute batch (Phase 4).

SoR  s3://data-sink/active/gtm_sub_universe_blobs/
     (1 row / target uei; Lance; snapshot-overwrite; BTREE uei)

Builds the two-tier hot blob (sub_universe_blob.v2) per target via
apps.catalyst_api.src.sub_universe_full.build_blob and lands them as one indexed
dataset. Row-exact node drilldown is served at query time from the existing
gtm_prime_demand_events / gtm_prime_combo_lanes marts (see sub_universe_serve) —
there is NO events sidecar dataset.

The base-recipe caches (winners/farmout/pairs/vehicles) are module-level and
build ONCE (cold); they are warmed single-threaded, then targets build
concurrently (Lance releases the GIL on R2 I/O). Every build_blob hydrate scan is
material-only, so per-target cost is dominated by the base recipe.

Proving batch (~350) spans the fan-out distribution and stress-loads the size
budget with the widest universes.

    doppler run -p core-x -c prd -- /Users/benjamincrane/core-x/.venv/bin/python \
      scripts/build_sub_universe_blobs.py [--limit 350] [--workers 8] [--dry] [--verify]
"""
from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import lance
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402
from apps.catalyst_api.src import config  # noqa: E402
from apps.catalyst_api.src import sub_universe_full as F  # noqa: E402
from apps.catalyst_api.src import sub_universe_store as S  # noqa: E402

OUT = config.GTM_SUB_UNIVERSE_BLOBS_URI
BTREE = ["uei"]
SINGLE_DIGIT_MB = 10.0


def select_ueis(limit: int) -> list[str]:
    """Stratified over fan-out: the widest universes (stress the size budget) +
    an even-stride span across all 5y-active subs. Deterministic."""
    opt = config.r2_storage_options()
    prof = lance.dataset(config.GTM_SUB_PROFILES_URI, storage_options=opt)
    rows = prof.scanner(columns=["uei", "n_distinct_primes_lifetime",
                                 "n_subawards_lifetime", "sub_amt_lifetime"]).to_table().to_pylist()
    active = [r for r in rows if (r["n_subawards_lifetime"] or 0) >= 5 and r["uei"]]
    by_primes = sorted(active, key=lambda r: -(r["n_distinct_primes_lifetime"] or 0))
    n_top = max(1, limit // 3)
    picked: list[str] = [r["uei"] for r in by_primes[:n_top]]           # widest universes
    seen = set(picked)
    rest = by_primes[n_top:]
    if rest:
        stride = max(1, len(rest) // (limit - len(picked)))
        for i in range(0, len(rest), stride):
            u = rest[i]["uei"]
            if u not in seen:
                picked.append(u); seen.add(u)
            if len(picked) >= limit:
                break
    return picked[:limit]


def _build_row(uei: str) -> dict | None:
    try:
        t = time.perf_counter()
        blob = F.build_blob(uei)
        payload = json.dumps(blob, default=str)
        return {"uei": uei, "as_of": blob["as_of"], "recipe": blob["recipe"],
                "n_material": blob["universe"]["n_material"],
                "n_total": blob["universe"]["n_total"],
                "hot_bytes": len(payload), "build_ms": int((time.perf_counter() - t) * 1000),
                "blob": payload}
    except Exception as e:  # robust batch: log + skip, never abort the run
        print(f"  ! {uei} FAILED: {type(e).__name__}: {e}", flush=True)
        return None


def build(limit: int, workers: int, dry: bool) -> int:
    ueis = select_ueis(limit)
    print(f"proving batch: {len(ueis)} targets | workers={workers} | out={OUT}", flush=True)

    t0 = time.perf_counter()
    print("warming base-recipe caches (single-threaded first build)...", flush=True)
    rows: list[dict] = []
    first = _build_row(ueis[0])
    if first:
        rows.append(first)
    print(f"  caches warm + first blob in {(time.perf_counter()-t0)/60:.1f} min", flush=True)

    done = len(rows)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_build_row, u): u for u in ueis[1:]}
        for fut in as_completed(futs):
            r = fut.result()
            done += 1
            if r:
                rows.append(r)
            if done % 25 == 0:
                el = (time.perf_counter() - t0) / 60
                print(f"  {done}/{len(ueis)} built ({len(rows)} ok) | {el:.1f} min elapsed", flush=True)

    # --- size + build gates (report the DISTRIBUTION, not an average) ---
    mb = sorted(r["hot_bytes"] / 1e6 for r in rows)
    bt = sorted(r["build_ms"] / 1000 for r in rows)
    n = len(mb)

    def q(a, p):
        return a[min(len(a) - 1, int(len(a) * p))]
    print(f"\n=== SIZE gate ({n} blobs) ===", flush=True)
    print(f"  hot MB   p50 {q(mb,.5):.2f}  p95 {q(mb,.95):.2f}  max {mb[-1]:.2f}", flush=True)
    print(f"  build s  p50 {q(bt,.5):.0f}   p95 {q(bt,.95):.0f}   max {bt[-1]:.0f}", flush=True)
    over = [r["uei"] for r in rows if r["hot_bytes"] / 1e6 >= SINGLE_DIGIT_MB]
    print(f"  over single-digit MB: {len(over)}" + (f"  {over[:10]}" if over else "  — all pass"), flush=True)

    if dry:
        print("\n--dry: not writing.", flush=True)
        return 0 if not over else 1

    tbl = pa.Table.from_pylist(rows, schema=pa.schema([
        ("uei", pa.string()), ("as_of", pa.string()), ("recipe", pa.string()),
        ("n_material", pa.int64()), ("n_total", pa.int64()),
        ("hot_bytes", pa.int64()), ("build_ms", pa.int64()), ("blob", pa.large_string())]))
    ds = write_indexed_dataset(tbl.to_reader(), OUT, [(c, "BTREE") for c in BTREE],
                               storage_options=config.r2_storage_options())
    print(f"\nwrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0 if not over else 1


def verify() -> int:
    """Round-trip the SoR: fetch a blob via the serve path, confirm shape, and
    exercise a node drilldown against the marts."""
    from apps.catalyst_api.src import sub_universe_serve as SV
    opt = config.r2_storage_options()
    ds = lance.dataset(OUT, storage_options=opt)
    probe = ds.scanner(columns=["uei", "hot_bytes"]).to_table().to_pylist()
    probe.sort(key=lambda r: -r["hot_bytes"])
    uei = probe[0]["uei"]                          # heaviest blob = hardest case
    t = time.perf_counter()
    blob = SV.fetch_blob(uei)
    fetch_ms = (time.perf_counter() - t) * 1000
    assert blob and blob["recipe"] == "sub_universe_blob.v2"
    nodes = blob["universe"]["nodes"]
    mat = [n for n in nodes if n.get("tier") == "material"]
    print(f"fetch_blob({uei}): {fetch_ms:.0f} ms | {len(nodes)} nodes | "
          f"{probe[0]['hot_bytes']/1e6:.2f} MB | recipe {blob['recipe']}")
    # drilldown on the top material node
    node_uei = mat[0]["uei"]
    t = time.perf_counter()
    detail = SV.fetch_node_detail(uei, node_uei, blob=blob)
    print(f"fetch_node_detail({node_uei}): {(time.perf_counter()-t)*1000:.0f} ms | "
          f"{len(detail['events'])} events | {len(detail['win_portfolio'])} portfolio")
    # drilldown events reconcile with the node's bucket total
    bn = sum(c["n"] for c in mat[0]["demand_events"]["buckets"].values())
    if not detail["events_truncated"]:
        assert len(detail["events"]) == bn, f"drilldown {len(detail['events'])} != buckets {bn}"
        print(f"drilldown reconciles buckets: {bn} events")
    print("VERIFY OK")
    return 0


if __name__ == "__main__":
    a = sys.argv
    if "--verify" in a:
        sys.exit(verify())
    lim = int(a[a.index("--limit") + 1]) if "--limit" in a else 350
    wrk = int(a[a.index("--workers") + 1]) if "--workers" in a else 8
    sys.exit(build(lim, wrk, "--dry" in a))
