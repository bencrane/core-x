#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.11,<3.13"
# dependencies = [
#   "duckdb>=1.5,<2",
#   "pylance>=7",
#   "pyarrow>=17",
# ]
# ///
"""Read-only relational verifier — Form 5500 carrier + Schedule C reconciliation.

Proves the cross-table joins the carrier / Schedule-C patch exists to unlock, against
either the R2 system of record (default) or the local lake. NO mutation — scan/count and
index-metadata reads only; never writes, deletes, compacts, or indexes.

All-Arrow plumbing (no pandas): DuckDB relations are materialized with `.to_arrow_table()`,
matching the ingest pipeline's own `to_arrow_reader` / `to_table` idiom. The earlier /tmp
heredoc version of this check shipped a `.df()` call with pandas absent from its deps and
crashed on every run — this committed, parameterized script is the durable replacement.

Checks (exit 0 iff all PASS):
  1. carrier rows > 0
  2. carrier ACK_ID orphan rate vs (main ∪ sf) filing heads   [< 1%; empirically ~0 —
     Schedule A binds to F_5500, not 5500-SF, so ∪ sf is a harmless superset guard]
  3. INS_CARRIER_EIN leading-zero retention                   [> 0 — proves no int coercion]
  4. carrier↔broker composite (ACK_ID, FORM_ID) resolution    [> 95% — 1:N head→detail]
  5. index census — carrier ⊇ {ACK_ID, FORM_ID, SCH_A_EIN, SCH_A_PLAN_NUM};
     broker ⊇ {ACK_ID, FORM_ID}  (the composite join is BTREE-pushed on BOTH sides)

Run (R2 creds via Doppler for the SoR root):
    doppler run --project core-x --config prd -- uv run pipelines/form5500/verify_carrier_recon.py
    doppler run --project core-x --config prd -- uv run pipelines/form5500/verify_carrier_recon.py --root ~/core-x-lake/active
"""
from __future__ import annotations

import argparse
import os


def storage_options(root: str) -> dict[str, str] | None:
    """R2 storage options from the Doppler-injected env, or None for a local root."""
    if not root.startswith("s3://"):
        return None
    endpoint = os.environ.get("R2_ENDPOINT")
    account = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account:
        endpoint = f"https://{account}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("R2_ENDPOINT (or R2_ACCOUNT_ID) not set — run under `doppler run`.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only Form 5500 carrier/Sch-C relational verifier.")
    ap.add_argument("--root", default="s3://data-sink/active",
                    help="dataset root: s3://data-sink/active (R2 SoR, default) or a local lake dir")
    ap.add_argument("--orphan-max", type=float, default=0.01, help="max carrier ACK_ID orphan fraction")
    ap.add_argument("--resolve-min", type=float, default=0.95, help="min broker→carrier composite resolution")
    args = ap.parse_args()

    import duckdb
    import lance

    root = args.root.rstrip("/")
    r2 = root.startswith("s3://")
    so = storage_options(root)

    def uri(name: str) -> str:
        return f"{root}/form5500_{name}/" if r2 else os.path.expanduser(f"{root}/form5500_{name}.lance")

    def dataset(name: str):
        return lance.dataset(uri(name), storage_options=so)

    def arrow(name: str, cols: list[str]):
        return dataset(name).to_table(columns=cols)

    con = duckdb.connect()
    con.register("carrier", arrow("sch_a_carrier", ["ACK_ID", "FORM_ID", "INS_CARRIER_EIN"]))
    con.register("broker", arrow("sch_a_broker", ["ACK_ID", "FORM_ID"]))
    con.register("main", arrow("main", ["ACK_ID"]))
    con.register("sf", arrow("sf", ["ACK_ID"]))

    rows = con.sql("SELECT count(*) FROM carrier").fetchone()[0]
    orphan = con.sql("""
        SELECT count(*) FROM carrier c
        ANTI JOIN (SELECT ACK_ID FROM main UNION SELECT ACK_ID FROM sf) h ON c.ACK_ID = h.ACK_ID
    """).fetchone()[0]
    lz = con.sql("SELECT count(*) FROM carrier WHERE INS_CARRIER_EIN LIKE '0%'").fetchone()[0]
    b_total = con.sql("SELECT count(*) FROM broker").fetchone()[0]
    b_resolved = con.sql("""
        SELECT count(*) FROM broker b
        SEMI JOIN carrier c ON b.ACK_ID = c.ACK_ID AND b.FORM_ID = c.FORM_ID
    """).fetchone()[0]

    def index_fields(name: str) -> set[str]:
        return {f for i in dataset(name).list_indices() for f in i["fields"]}

    carrier_idx = index_fields("sch_a_carrier")
    broker_idx = index_fields("sch_a_broker")
    carrier_exp = {"ACK_ID", "FORM_ID", "SCH_A_EIN", "SCH_A_PLAN_NUM"}
    broker_exp = {"ACK_ID", "FORM_ID"}

    orphan_pct = (orphan / rows) if rows else 1.0
    resolve_pct = (b_resolved / b_total) if b_total else 0.0
    checks = {
        "carrier_rows>0": rows > 0,
        "orphan<max": orphan_pct < args.orphan_max,
        "leading_zero>0": lz > 0,
        "broker_resolve>min": resolve_pct > args.resolve_min,
        "carrier_index": carrier_exp <= carrier_idx,
        "broker_index": broker_exp <= broker_idx,
    }
    ok = all(checks.values())

    def mark(b: bool) -> str:
        return "PASS" if b else "FAIL"

    print(f"root                              : {root} ({'R2 SoR' if r2 else 'local'})")
    print(f"carrier rows                      : {rows:,}  [{mark(checks['carrier_rows>0'])} >0]")
    print(f"carrier ACK_ID orphan vs main∪sf  : {orphan:,} ({orphan_pct*100:.2f}%)  "
          f"[{mark(checks['orphan<max'])} <{args.orphan_max*100:.0f}%; ~0 — Sch A binds F_5500, not SF]")
    print(f"INS_CARRIER_EIN leading-zero      : {lz:,}  [{mark(checks['leading_zero>0'])} >0]")
    print(f"broker→carrier (ACK_ID,FORM_ID)   : {b_resolved:,}/{b_total:,} ({resolve_pct*100:.2f}%)  "
          f"[{mark(checks['broker_resolve>min'])} >{args.resolve_min*100:.0f}%]")
    print(f"carrier BTREE fields              : {sorted(carrier_idx)}  "
          f"[{mark(checks['carrier_index'])} ⊇ {sorted(carrier_exp)}]")
    print(f"broker BTREE fields               : {sorted(broker_idx)}  "
          f"[{mark(checks['broker_index'])} ⊇ {sorted(broker_exp)}]")
    print(f"RESULT: {'✅ PASS' if ok else '❌ FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
