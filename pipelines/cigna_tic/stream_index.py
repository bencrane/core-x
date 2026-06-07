#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.11,<3.13"
# dependencies = [
#   "ijson>=3.3",
#   "psutil>=6",
# ]
# ///
"""Memory-bounded streaming parser for Cigna Transparency-in-Coverage ToC (index) files.

A Cigna ToC index is a single JSON object whose `reporting_structure` array is the only
large member. Each element is small (one or a few `reporting_plans`, a few file refs), so
streaming the array with `ijson.items(f, "reporting_structure.item")` keeps resident memory
bounded by the per-element object + the accumulator sets — independent of total file size.
`json.load` / `readFileSync` would materialize the whole tree (a multi-hundred-MB spike on
the 72 MB national index) and is never used here.

Schema (verified against the real bytes):

    {
      "reporting_entity_name": "...",
      "reporting_entity_type": "Health Insurance Issuer",
      "reporting_structure": [
        {
          "reporting_plans": [
            {"plan_name","plan_id_type":"ein"|"hios","plan_id","plan_market_type"}
          ],
          "in_network_files": [ {"description","location","file_size"} ] | null,
          "allowed_amount_file": {"description","location"}
        }
      ]
    }

The EIN→rate-file link is: a reporting_plan with `plan_id_type == "ein"` (group market)
carries the sponsor EIN in `plan_id`; the sibling `in_network_files[].location` is the
absolute, signed URL of that structure's `in-network-rates.json.gz`.

Edge cases handled: `in_network_files == null`, empty/missing `reporting_plans`, missing
`location`, hyphenated vs digits-only EIN (normalized both sides), rate-file dedupe on the
path component (signed query string varies per generation).

    uv run pipelines/cigna_tic/stream_index.py \
        --index /path/2026-06-01_..._index.json \
        --eins /tmp/employer_eins.txt \
        --queue /tmp/high_conviction.jsonl \
        --telemetry /tmp/telemetry.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import resource
import sys
import time
from collections import Counter

import ijson
import psutil

_NON_DIGIT = re.compile(r"\D+")
_ENTITY_NAME = re.compile(rb'"reporting_entity_name"\s*:\s*"([^"]*)"')
_ENTITY_TYPE = re.compile(rb'"reporting_entity_type"\s*:\s*"([^"]*)"')


def norm_ein(raw: object) -> str | None:
    if raw is None:
        return None
    digits = _NON_DIGIT.sub("", str(raw))
    if not digits:
        return None
    return digits[-9:].zfill(9)


def load_eins(path: str) -> set[str]:
    out: set[str] = set()
    with open(path) as fh:
        for line in fh:
            ein = norm_ein(line.strip())
            if ein:
                out.add(ein)
    return out


def read_entity_header(path: str, nbytes: int = 8192) -> dict[str, str | None]:
    """Bounded head read — the entity scalars precede the large array."""
    with open(path, "rb") as fh:
        head = fh.read(nbytes)
    name = _ENTITY_NAME.search(head)
    typ = _ENTITY_TYPE.search(head)
    return {
        "reporting_entity_name": name.group(1).decode("utf-8", "replace") if name else None,
        "reporting_entity_type": typ.group(1).decode("utf-8", "replace") if typ else None,
    }


def peak_rss_bytes() -> int:
    """OS-reported peak. Darwin ru_maxrss is bytes; Linux is KiB."""
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return maxrss if sys.platform == "darwin" else maxrss * 1024


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--index", required=True, help="Cigna ToC index .json")
    ap.add_argument("--eins", help="newline-delimited employer EINs to intersect")
    ap.add_argument("--queue", help="JSONL output of high-conviction rate-file links")
    ap.add_argument("--telemetry", help="write the telemetry JSON object here")
    ap.add_argument("--rss-cap-mb", type=int, default=256, help="RSS budget to assert against")
    ap.add_argument("--top-eins", type=int, default=15, help="EIN-frequency rows to surface")
    args = ap.parse_args()

    proc = psutil.Process(os.getpid())
    eins = load_eins(args.eins) if args.eins else set()
    entity = read_entity_header(args.index)

    n_struct = 0
    plan_type_counts: Counter[str] = Counter()
    market_counts: Counter[str] = Counter()
    ein_freq: Counter[str] = Counter()        # distinct sponsor EIN -> #structures
    distinct_innet_paths: set[str] = set()     # deduped rate-file targets (scale forecast)
    n_innet_links = 0                          # raw (pre-dedupe) in_network_files entries
    n_null_innet = 0
    n_structs_with_ein = 0
    hits: list[dict] = []
    matched_paths: set[str] = set()
    sampled_peak = proc.memory_info().rss

    queue_fh = open(args.queue, "w") if args.queue else None
    t0 = time.perf_counter()

    with open(args.index, "rb") as fh:
        for struct in ijson.items(fh, "reporting_structure.item"):
            n_struct += 1
            plans = struct.get("reporting_plans") or []
            struct_eins: set[str] = set()
            for plan in plans:
                ptype = (plan.get("plan_id_type") or "").strip().lower()
                plan_type_counts[ptype or "<none>"] += 1
                market_counts[(plan.get("plan_market_type") or "").strip().lower() or "<none>"] += 1
                if ptype == "ein":
                    ein = norm_ein(plan.get("plan_id"))
                    if ein:
                        struct_eins.add(ein)
            for ein in struct_eins:
                ein_freq[ein] += 1
            if struct_eins:
                n_structs_with_ein += 1

            innet = struct.get("in_network_files")
            if innet is None:
                n_null_innet += 1
                innet_locs: list[tuple[str, dict]] = []
            else:
                innet_locs = []
                for f in innet:
                    loc = f.get("location")
                    if not loc:
                        continue
                    n_innet_links += 1
                    path = loc.split("?", 1)[0]
                    distinct_innet_paths.add(path)
                    innet_locs.append((loc, f))

            matched = struct_eins & eins
            if matched and innet_locs:
                for loc, f in innet_locs:
                    path = loc.split("?", 1)[0]
                    matched_paths.add(path)
                    rec = {
                        "matched_eins": sorted(matched),
                        "plan_names": [p.get("plan_name") for p in plans],
                        "market_types": sorted({(p.get("plan_market_type") or "") for p in plans}),
                        "rate_file_url": loc,
                        "rate_file_path": path,
                        "file_size": f.get("file_size"),
                    }
                    hits.append(rec)
                    if queue_fh:
                        queue_fh.write(json.dumps(rec) + "\n")

            if n_struct % 2000 == 0:
                rss = proc.memory_info().rss
                if rss > sampled_peak:
                    sampled_peak = rss

    elapsed = time.perf_counter() - t0
    if queue_fh:
        queue_fh.close()

    sampled_peak = max(sampled_peak, proc.memory_info().rss)
    os_peak = peak_rss_bytes()
    distinct_ein_count = len(ein_freq)
    # Issuer-vs-employer signal: share of EIN references concentrated in the top distinct EIN.
    top_eins = ein_freq.most_common(args.top_eins)
    total_ein_refs = sum(ein_freq.values())
    top1_share = (top_eins[0][1] / total_ein_refs) if total_ein_refs else 0.0

    telemetry = {
        "index_file": os.path.abspath(args.index),
        "index_size_bytes": os.path.getsize(args.index),
        "entity": entity,
        "ijson_backend": getattr(ijson, "backend", "?"),
        "structure": {
            "reporting_structures": n_struct,
            "structures_with_ein_plan": n_structs_with_ein,
            "structures_with_null_in_network_files": n_null_innet,
            "plan_id_type_counts": dict(plan_type_counts),
            "plan_market_type_counts": dict(market_counts),
        },
        "ein_identity": {
            "distinct_sponsor_eins": distinct_ein_count,
            "total_ein_references": total_ein_refs,
            "top1_ein_share_of_refs": round(top1_share, 4),
            "top_eins": [{"ein": e, "structures": n} for e, n in top_eins],
        },
        "rate_files": {
            "in_network_links_raw": n_innet_links,
            "distinct_rate_file_paths": len(distinct_innet_paths),
        },
        "intersect": {
            "employer_eins_loaded": len(eins),
            "hit_count_links": len(hits),
            "hit_distinct_rate_files": len(matched_paths),
            "hit_distinct_eins": len({e for h in hits for e in h["matched_eins"]}),
        },
        "telemetry": {
            "wall_clock_ms": round(elapsed * 1000, 1),
            "peak_rss_bytes_os": os_peak,
            "peak_rss_mb_os": round(os_peak / 1024 / 1024, 1),
            "peak_rss_mb_sampled": round(sampled_peak / 1024 / 1024, 1),
            "rss_cap_mb": args.rss_cap_mb,
            "within_rss_cap": os_peak <= args.rss_cap_mb * 1024 * 1024,
            "throughput_mb_per_s": round(
                (os.path.getsize(args.index) / 1024 / 1024) / elapsed, 1
            ) if elapsed else None,
        },
    }

    out = json.dumps(telemetry, indent=2)
    print(out)
    if args.telemetry:
        with open(args.telemetry, "w") as fh:
            fh.write(out + "\n")

    # Non-zero exit if the memory budget was blown — makes the cap a hard gate in CI.
    return 0 if telemetry["telemetry"]["within_rss_cap"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
