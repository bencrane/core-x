#!/usr/bin/env python3
"""USAspending cache high-water-mark pulse — READ-ONLY.

Reports the action-date and modification-date frontier for the two daily API
caches (prime + procurement subaward), both leaf datasets under the single
container ``active/usaspending_api_fresh/``. Schema-aware: the prime table
exposes verbatim ``action_date``/``last_modified_date``; the subaward table
exposes ``subaward_action_date``/``subaward_sam_report_last_modified_date`` (it
carries NO plain last_modified_date). One code path, picks the present column.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'duckdb>=1.5,<2' --with 'pyarrow>=17' \
      python3 scripts/usaspending_hwm_pulse.py
"""
from __future__ import annotations

import json
import os
import sys

import duckdb
import lance

TARGETS = [
    {"label": "Prime awards", "uri": "s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/"},
    {"label": "Subawards",    "uri": "s3://data-sink/active/usaspending_api_fresh/contract_subaward/"},
]
# Logical frontier -> physical column candidates, in priority order. First
# present column wins, so prime and subaward resolve through the same path.
ACTION_CANDIDATES = ["action_date", "subaward_action_date"]
MOD_CANDIDATES = ["last_modified_date", "subaward_sam_report_last_modified_date"]


def storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def pick(names: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in names:
            return c
    return None


def pulse(target: dict, so: dict) -> dict:
    uri = target["uri"]
    rec = {"label": target["label"], "uri": uri}
    ds = lance.dataset(uri, storage_options=so)
    names = set(ds.schema.names)
    action_col = pick(names, ACTION_CANDIDATES)
    mod_col = pick(names, MOD_CANDIDATES)
    rec["action_col"] = action_col
    rec["mod_col"] = mod_col
    rec["rows"] = ds.count_rows()

    cols = [c for c in (action_col, mod_col) if c]
    reader = ds.scanner(columns=cols).to_reader()
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET temp_directory='/tmp/duck_spill';")
    con.register("t", reader)
    a = f'max(TRY_CAST("{action_col}" AS DATE))' if action_col else "NULL"
    m = f'max(TRY_CAST("{mod_col}" AS DATE))' if mod_col else "NULL"
    row = con.execute(f"""
        SELECT CAST({a} AS VARCHAR)                          AS max_action,
               CAST({m} AS VARCHAR)                          AS max_mod,
               CAST(current_date AS VARCHAR)                 AS today,
               date_diff('day', {a}, current_date)           AS delta_action_days,
               date_diff('day', {m}, current_date)           AS delta_mod_days,
               count(*)                                      AS rows_scanned
        FROM t
    """).fetchone()
    con.close()
    rec.update(max_action=row[0], max_mod=row[1], today=row[2],
               delta_action_days=row[3], delta_mod_days=row[4], rows_scanned=row[5])
    return rec


def main() -> int:
    so = storage_options()
    out = []
    for t in TARGETS:
        try:
            r = pulse(t, so)
        except Exception as e:
            r = {"label": t["label"], "uri": t["uri"],
                 "error": f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"}
        out.append(r)
        print(f"[pulse] {r['label']:<13} max_mod={r.get('max_mod')} "
              f"delta={r.get('delta_mod_days')}d rows={r.get('rows')}",
              file=sys.stderr, flush=True)
    json.dump(out, sys.stdout, indent=2, default=str)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
