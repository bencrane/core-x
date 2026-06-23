"""Phase A — extract ALL distinct rental-yard catalogs from equipment_catalog Lance and
shard them into JSON files for the agentic matchmaking harness (Phase B).

Deterministic I/O only — NO LLM/API calls. Dedup by domain_norm (keep the richest row),
filter to domains with a populated category_names OR equipment_item_names array, sort by
domain_norm (stable/resumable ordering), and write fixed-size shards.

Outputs (under reports/mm_shards/):
    shard_00000.json ... shard_NNNNN.json   each: {"shard_id": int, "domains": [ {...} ]}
    manifest.json                            {total_domains, shard_size, num_shards, shard_paths[]}

Each domain record:
    {domain_norm, payload_kind, provider_modes[], category_names[], equipment_item_names[]}

Run:  doppler run -p core-x -c prd -- python3 scripts/extract_matchmaking_shards.py
"""

from __future__ import annotations

import json
import os
import sys

import lance


DATASET_URI = "s3://data-sink/active/equipment_catalog/"
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "reports", "mm_shards")
SHARD_SIZE = 24

_CONF_RANK = {"high": 0, "medium": 1, "low": 2}


def _so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None
    )
    if not endpoint:
        raise RuntimeError("R2_ENDPOINT / R2_ACCOUNT_ID not set")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _parse(s):
    """jsonb-as-text → list[str]. Tolerates list-of-str and list-of-obj shapes."""
    if s is None or s == "":
        return []
    try:
        v = json.loads(s)
    except Exception:
        return []
    if not isinstance(v, list):
        return []
    out = []
    for x in v:
        if isinstance(x, str):
            x = x.strip()
            if x:
                out.append(x)
        elif isinstance(x, dict):
            for k in ("name", "category", "item", "label"):
                if isinstance(x.get(k), str) and x[k].strip():
                    out.append(x[k].strip())
                    break
    # de-dup preserving order
    seen = set()
    deduped = []
    for x in out:
        if x not in seen:
            seen.add(x)
            deduped.append(x)
    return deduped


def main() -> None:
    ds = lance.dataset(DATASET_URI, storage_options=_so())
    rows = ds.to_table(columns=[
        "domain_norm", "payload_kind", "confidence", "provider_modes",
        "categories", "category_names", "equipment_items", "equipment_item_names",
    ]).to_pylist()
    print(f"# equipment_catalog rows: {len(rows)}", file=sys.stderr)

    by_domain: dict[str, dict] = {}
    for row in rows:
        dn = (row.get("domain_norm") or "").strip().lower()
        if not dn:
            continue
        cats = _parse(row.get("category_names")) or _parse(row.get("categories"))
        items = _parse(row.get("equipment_item_names")) or _parse(row.get("equipment_items"))
        if not cats and not items:
            continue
        rec = {
            "domain_norm": dn,
            "payload_kind": row.get("payload_kind"),
            "provider_modes": _parse(row.get("provider_modes")),
            "category_names": cats,
            "equipment_item_names": items,
        }
        prev = by_domain.get(dn)
        if prev is None:
            by_domain[dn] = rec
            continue
        # keep the richest row (more total signal, then higher confidence)
        prev_sig = (len(prev["equipment_item_names"]) + len(prev["category_names"]),)
        new_sig = (len(items) + len(cats),)
        if new_sig > prev_sig:
            by_domain[dn] = rec

    domains = [by_domain[d] for d in sorted(by_domain)]
    n = len(domains)
    print(f"# distinct domains with signal: {n}", file=sys.stderr)

    os.makedirs(OUT_DIR, exist_ok=True)
    # clean stale shards
    for f in os.listdir(OUT_DIR):
        if f.endswith(".json"):
            os.remove(os.path.join(OUT_DIR, f))

    shard_paths = []
    num_shards = (n + SHARD_SIZE - 1) // SHARD_SIZE
    for i in range(num_shards):
        chunk = domains[i * SHARD_SIZE:(i + 1) * SHARD_SIZE]
        path = os.path.join(OUT_DIR, f"shard_{i:05d}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump({"shard_id": i, "domains": chunk}, fh, ensure_ascii=False)
        shard_paths.append(path)

    manifest = {
        "dataset_uri": DATASET_URI,
        "total_domains": n,
        "shard_size": SHARD_SIZE,
        "num_shards": num_shards,
        "out_dir": OUT_DIR,
        "shard_paths": shard_paths,
    }
    with open(os.path.join(OUT_DIR, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)

    print(json.dumps({k: v for k, v in manifest.items() if k != "shard_paths"}, indent=2))


if __name__ == "__main__":
    main()
