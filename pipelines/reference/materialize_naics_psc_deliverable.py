"""Reference loader — naics_psc_deliverable: the (naics_code, psc_code) -> deliverable / work_type
classification for the 5,000 highest-activity award combos (tri-union of the 80% transaction,
vehicle, and dollar coverage heads over 2021+ FPDS prime data).

WHAT IT IS. Each occurring (naics_code, psc_code) pair is classified into:
  - what_was_done : the DELIVERABLE — plain-English noun phrase of what the government OBTAINED
                    (buyer-side, vendor-agnostic, NO company names). The vendor's METHOD is NOT here.
  - work_type     : manufacture | construct | maintain_repair | distribute_resell | services_labor |
                    staffing | RnD | other  — the make/resell/service axis (method lives HERE, not in
                    what_was_done). A maker and a reseller of the same good share the deliverable phrase
                    and differ only in work_type.
  - regime        : redundant | psc_carries | naics_carries | emergent | both_blank (how the two codes
                    combine; internal, not projected to serving).
  - confidence    : high | medium | low (categorical).
  - review_status : confirmed | corrected | unreviewed (independent second-pass review outcome).

This is the BRIDGE that turns the frozen Stage-1 classified output into queryable labels: a downstream
serving loader LEFT JOINs this table onto every award action on (naics_code, psc_code), so what_was_done
/ work_type become filterable columns. Award rows whose pair is not in this map (the long tail outside
the top-5,000) get NULL labels (queryable as "unclassified").

GRAIN  1 row per (naics_code, psc_code) pair. 5,000 rows (deduped on the pair key — the join must stay 1:1).
SoR    s3://data-sink/active/naics_psc_deliverable/   (Lance v2.1; derived, mode=overwrite)
SOURCE pipelines/reference/data/naics_psc_deliverable.csv — the committed, frozen Stage-1 output
       (in-session Opus 4.8 classification; each pair enriched with official PSC/NAICS text + behavioral
       stats, then labeled; prompt_version=what_was_done_v2). The CSV carries the full 11-column dataset
       content verbatim (labels + provenance), so re-materialization is byte-faithful. Re-run after
       re-classifying to refresh. Classification method + provenance:
       ~/Desktop/hq/naics_psc_what_was_done_PROMPT_v2.md and NAICS_PSC_DELIVERABLE_CANONICAL.md.
KEYS   naics_code + psc_code (BTREE) — the join key onto the awards serving table.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' \
      python3 pipelines/reference/materialize_naics_psc_deliverable.py <build|verify>
"""
from __future__ import annotations

import collections
import csv
import datetime as dt
import json
import os
import sys
from pathlib import Path

ACTIVE = "s3://data-sink/active"
MAP_URI = os.environ.get("NAICS_PSC_DELIVERABLE_URI", f"{ACTIVE}/naics_psc_deliverable/")
DEFAULT_CSV = str(Path(__file__).parent / "data" / "naics_psc_deliverable.csv")
DATA_STORAGE_VERSION = "2.1"
BTREE_INDEXES = ["naics_code", "psc_code"]

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

# Labels are nullable (empty -> None); provenance is carried verbatim from the frozen CSV.
LABEL_COLS = ["what_was_done", "work_type", "regime", "confidence", "review_status"]
PROV_COLS = ["prompt_version", "model_id", "generated_at", "source_vintage"]
ALL_COLS = ["naics_code", "psc_code"] + LABEL_COLS + PROV_COLS


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _assemble(csv_path: str):
    import pyarrow as pa
    rows = list(csv.DictReader(open(csv_path)))
    if not rows:
        raise RuntimeError(f"no rows in {csv_path}")

    out, seen, dups = [], set(), 0
    for r in rows:
        n = (r.get("naics_code") or "").strip()
        p = (r.get("psc_code") or "").strip()
        if not n or not p:
            continue
        key = (n, p)
        if key in seen:  # the join must stay 1:1 — never fan out award rows
            dups += 1
            continue
        seen.add(key)
        rec = {"naics_code": n, "psc_code": p}
        for c in LABEL_COLS:
            rec[c] = ((r.get(c) or "").strip() or None)
        for c in PROV_COLS:  # provenance carried verbatim (frozen in the CSV)
            rec[c] = ((r.get(c) or "").strip() or None)
        out.append(rec)
    if dups:
        log(f"WARN: dropped {dups} duplicate (naics_code, psc_code) rows — kept first")

    schema = pa.schema([(f, pa.string()) for f in ALL_COLS])
    return pa.table({f: [r[f] for r in out] for f in ALL_COLS}, schema=schema)


def build(csv_path: str = DEFAULT_CSV):
    import lance
    so = _r2_so()
    tbl = _assemble(csv_path)
    if tbl.num_rows == 0:
        raise RuntimeError("zero classified pairs assembled")
    log(f"assembled {tbl.num_rows} (naics_code, psc_code) -> deliverable rows from {csv_path}")
    lance.write_dataset(tbl, MAP_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(MAP_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        log(f"  BTREE ✓ {col}")
    log(f"DONE → {MAP_URI} rows={tbl.num_rows}")
    return {"rows": tbl.num_rows, "uri": MAP_URI}


def verify():
    import lance
    so = _r2_so()
    ds = lance.dataset(MAP_URI, storage_options=so)
    t = ds.scanner(columns=["naics_code", "psc_code", "work_type", "review_status", "source_vintage"]).to_table()
    pairs = set(zip(t.column("naics_code").to_pylist(), t.column("psc_code").to_pylist()))
    try:
        idx = [getattr(i, "name", i.get("name") if isinstance(i, dict) else str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    out = {
        "uri": MAP_URI, "rows": ds.count_rows(), "distinct_pairs": len(pairs),
        "work_types": dict(collections.Counter(t.column("work_type").to_pylist())),
        "review_status": dict(collections.Counter(t.column("review_status").to_pylist())),
        "source_vintage": dict(collections.Counter(t.column("source_vintage").to_pylist())),
        "indices": idx,
    }
    print(json.dumps(out, indent=2, default=str))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        print(json.dumps(build(), indent=2, default=str))
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
