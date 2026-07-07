#!/usr/bin/env python3
"""sub_corrections — the private correction ledger over disclosed FSRS assertions.

SoR  s3://data-sink/active/sub_corrections/
     (append-only Lance; BTREE on uei; one row per corrected assertion)

WHY
The diagnostic form asserts what the public record says (the behavioral vectors);
the sub confirms or corrects. A correction — "that relationship is dead", "our
floor is $500K now" — is a delta between disclosed record and stated reality that
exists nowhere else. This ledger persists those deltas so every future map build
for the entity overlays them. Internal asset, low volume, append-only; never
overwritten by mart rebuilds.

SCHEMA (operator-directed 2026-07-06)
  uei              str   the sub entity
  assertion_key    str   machine key of the asserted fact, e.g.
                         'v1.top1_prime' | 'v2.541330xR425.median_chunk' |
                         'v3.top_vehicle' | 'combo.541611xR425.appetite'
  disclosed_value  str   what the public record said (JSON-encoded scalar/object)
  corrected_value  str   what the sub stated (JSON-encoded)
  stated_at        str   ISO date the correction was made
  source           str   provenance, e.g. 'call:2026-07-06' | 'form' | 'email'
  recorded_at      str   ISO timestamp of ledger append

Usage:
  python3 scripts/sub_corrections.py add UEI ASSERTION_KEY DISCLOSED CORRECTED SOURCE
  python3 scripts/sub_corrections.py list UEI
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import lance
import pyarrow as pa

A = "s3://data-sink/active"
OUT = f"{A}/sub_corrections/"

SCHEMA = pa.schema([
    ("uei", pa.string()),
    ("assertion_key", pa.string()),
    ("disclosed_value", pa.string()),
    ("corrected_value", pa.string()),
    ("stated_at", pa.string()),
    ("source", pa.string()),
    ("recorded_at", pa.string()),
])


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def _exists(opt: dict) -> bool:
    try:
        lance.dataset(OUT, storage_options=opt)
        return True
    except Exception:
        return False


def add(uei: str, assertion_key: str, disclosed: str, corrected: str,
        source: str, stated_at: str | None = None) -> None:
    opt = so()
    row = {
        "uei": uei.strip().upper(),
        "assertion_key": assertion_key.strip(),
        "disclosed_value": disclosed,
        "corrected_value": corrected,
        "stated_at": stated_at or dt.date.today().isoformat(),
        "source": source.strip(),
        "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    tbl = pa.Table.from_pylist([row], schema=SCHEMA)
    if _exists(opt):
        lance.write_dataset(tbl, OUT, mode="append", storage_options=opt)
    else:
        ds = lance.write_dataset(tbl, OUT, mode="create", storage_options=opt)
        ds.create_scalar_index("uei", "BTREE")
    print(f"recorded correction: {row['uei']} {row['assertion_key']} "
          f"({row['disclosed_value']!r} -> {row['corrected_value']!r}) [{row['source']}]")


def list_for(uei: str) -> list[dict]:
    opt = so()
    if not _exists(opt):
        return []
    rows = lance.dataset(OUT, storage_options=opt).scanner(
        filter=f"uei = '{uei.strip().upper()}'").to_table().to_pylist()
    rows.sort(key=lambda r: r["recorded_at"])
    return rows


def main() -> None:
    args = sys.argv[1:]
    if len(args) >= 6 and args[0] == "add":
        add(args[1], args[2], args[3], args[4], args[5])
    elif len(args) == 2 and args[0] == "list":
        rows = list_for(args[1])
        if not rows:
            print("no corrections on record")
        for r in rows:
            print(f"{r['stated_at']}  {r['assertion_key']:40} "
                  f"{r['disclosed_value']!r} -> {r['corrected_value']!r}  [{r['source']}]")
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
