"""One-shot sampler for the semantic matchmaking directive.

Pulls 12 distinct domain_norm rows from s3://data-sink/active/equipment_catalog/
with non-empty equipment_items OR categories arrays, prefers higher confidence,
and emits JSON to stdout: [{domain_norm, payload_kind, confidence,
category_names[], equipment_item_names[]}, ...].

Run:  doppler run -p core-x -c prd -- python3 scripts/archive/_sample_equipment_catalog.py
"""

from __future__ import annotations

import json
import os
import sys

import lance


DATASET_URI = "s3://data-sink/active/equipment_catalog/"
TARGET_N = 12

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
                    out.append(x[k].strip()); break
    return out


def main() -> None:
    ds = lance.dataset(DATASET_URI, storage_options=_so())
    tbl = ds.to_table(columns=[
        "domain_norm", "payload_kind", "confidence",
        "categories", "category_names",
        "equipment_items", "equipment_item_names",
    ]).to_pylist()
    print(f"# total rows: {len(tbl)}", file=sys.stderr)

    by_domain: dict[str, dict] = {}
    for row in tbl:
        dn = (row.get("domain_norm") or "").strip()
        if not dn:
            continue
        cats = _parse(row.get("category_names")) or _parse(row.get("categories"))
        items = _parse(row.get("equipment_item_names")) or _parse(row.get("equipment_items"))
        if not cats and not items:
            continue
        rec = {
            "domain_norm": dn,
            "payload_kind": row.get("payload_kind"),
            "confidence": row.get("confidence"),
            "category_names": cats,
            "equipment_item_names": items,
        }
        prev = by_domain.get(dn)
        if prev is None:
            by_domain[dn] = rec
        else:
            # keep the row with more signal (longer items list, then higher confidence)
            prev_sig = (len(prev["equipment_item_names"]) + len(prev["category_names"]),
                        -_CONF_RANK.get((prev.get("confidence") or "").lower(), 9))
            new_sig = (len(items) + len(cats),
                       -_CONF_RANK.get((row.get("confidence") or "").lower(), 9))
            if new_sig > prev_sig:
                by_domain[dn] = rec

    print(f"# distinct domains w/ signal: {len(by_domain)}", file=sys.stderr)

    ranked = sorted(
        by_domain.values(),
        key=lambda r: (
            _CONF_RANK.get((r.get("confidence") or "").lower(), 9),
            -(len(r["equipment_item_names"]) + len(r["category_names"])),
            r["domain_norm"],
        ),
    )
    sample = ranked[:TARGET_N]
    json.dump(sample, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
