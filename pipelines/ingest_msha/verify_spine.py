"""Read-only integrity harness — the MSHA ID spine (Phase 1 of the spine execution plan).

Part of the ``msha-spine-verify`` Modal app. **Zero writes** — no write_dataset, no
create_scalar_index, no ledger DDL. It re-asserts the deterministic-spine contract on demand
and fails loudly on drift, so the spine (`MINE_ID` site · `CONTROLLER_ID`/`OPERATOR_ID`/
`VIOLATOR_ID` entity · `CONTRACTOR_ID` third spine · `EVENT_NO`/`VIOLATION_NO` links) can never
silently regress. Cloned structurally from the `verify_datasets` / `verify_mirror` entrypoints
in the three materialize workers (same `_r2_storage_options` + `lance.dataset` + DuckDB pattern).

WHAT IT ASSERTS (the gates; see docs/plans/MSHA_ID_SPINE_EXECUTION_PLAN.md §8/§9):
  - Resolution: every spine edge resolves at/above its floor (MINE_ID 100%, entity ≥99.5%,
    EVENT_NO 100%/≥99.5%, citation/violation ≥94%, docket ≥99%, contractor accidents ≥99% /
    enforcement ≥88%).
  - Index hardening: every spine join key PRESENT in a dataset is BTREE-indexed (the post-P2
    target; reports the 3 known gaps until P2 closes them).
  - Format hygiene: MINE_ID is 7-char all-digit; entity/contractor IDs carry no whitespace and
    NO lowercase (the post-P3 target; reports the 3 known lowercase cells until P3 closes them).
  - SCD determinism: reports the count of mines with >1 current controller (drift signal) and
    confirms the latest-start-wins resolver collapses to <= the known same-day-tie count.

Overall PASS == the full post-P0-P3 end state (all resolution floors held + 0 unindexed spine
keys + 0 lowercase IDs). Run it before P2/P3 to see the pending surface; run it after to prove
green.

    modal run pipelines/ingest_msha/verify_spine.py::check_local    # full harness, pass/fail
    modal deploy pipelines/ingest_msha/verify_spine.py              # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

_ACTIVE = "s3://data-sink/active"

# ── Spine join keys present per dataset (live-probed presence matrix, plan §3.2). Each must be
# BTREE-indexed for the spine to be fully index-backed. NOTE: noise_samples.VIOLATION_NO is a
# survey-form number, NOT a citation (2.36% resolve) — deliberately excluded (plan R6/§15). ──
SPINE_KEYS: dict[str, list[str]] = {
    "msha_mines": ["MINE_ID", "CURRENT_CONTROLLER_ID", "CURRENT_OPERATOR_ID"],
    "msha_enforcement_ledger": ["MINE_ID", "CONTROLLER_ID", "VIOLATOR_ID", "CONTRACTOR_ID",
                                "EVENT_NO", "VIOLATION_NO", "DOCKET_NO"],
    "msha_accidents": ["MINE_ID", "CONTROLLER_ID", "OPERATOR_ID", "CONTRACTOR_ID"],
    "msha_corporate_history": ["MINE_ID", "CONTROLLER_ID", "OPERATOR_ID"],
    "msha_contractors": ["CONTRACTOR_ID"],
    "msha_inspections": ["MINE_ID", "CONTROLLER_ID", "OPERATOR_ID", "EVENT_NO"],
    "msha_mines_prod_quarterly": ["MINE_ID"],
    "msha_mines_prod_yearly": ["MINE_ID"],
    "msha_coal_dust_samples": ["MINE_ID"],
    "msha_contested_violations": ["MINE_ID", "CITATION_NO", "DOCKET_NO"],
    "msha_civil_penalty_dockets_decisions": ["MINE_ID", "VIOLATOR_ID", "VIOLATION_NO", "DOCKET_NO"],
    "msha_personal_health_samples": ["MINE_ID", "EVENT_NO", "CONTRACTOR_ID"],
    "msha_noise_samples": ["MINE_ID", "EVENT_NO", "CONTRACTOR_ID"],
    "msha_quartz_samples": ["MINE_ID"],
    "msha_conferences": ["CONFERENCE_NO", "ISSUANCE_NO"],
    "msha_area_samples": ["MINE_ID", "EVENT_NO", "CONTRACTOR_ID"],
    "msha_orders_issued": ["MINE_ID", "VIOLATION_NO", "CONTRACTOR_ID", "CONTROLLER_ID_VIOLATIONS"],
}

# ── Resolution edges: (child_ds, child_col, parent_ds, parent_col, floor, child_filter|None).
# floor = minimum acceptable fraction of NON-NULL child keys that resolve to the parent set. ──
EDGES: list[tuple] = [
    # MINE_ID universal site spine — hard 100%.
    *[(d, "MINE_ID", "msha_mines", "MINE_ID", 1.0, None) for d in SPINE_KEYS
      if "MINE_ID" in SPINE_KEYS[d] and d != "msha_mines"],
    # Entity spine → corporate_history.
    ("msha_mines", "CURRENT_CONTROLLER_ID", "msha_corporate_history", "CONTROLLER_ID", 0.995, None),
    ("msha_mines", "CURRENT_OPERATOR_ID", "msha_corporate_history", "OPERATOR_ID", 0.995, None),
    ("msha_enforcement_ledger", "CONTROLLER_ID", "msha_corporate_history", "CONTROLLER_ID", 0.995, None),
    ("msha_enforcement_ledger", "VIOLATOR_ID", "msha_corporate_history", "OPERATOR_ID", 0.995,
     "VIOLATOR_TYPE_CD = 'Operator'"),
    ("msha_inspections", "CONTROLLER_ID", "msha_corporate_history", "CONTROLLER_ID", 0.995, None),
    ("msha_inspections", "OPERATOR_ID", "msha_corporate_history", "OPERATOR_ID", 0.995, None),
    ("msha_accidents", "CONTROLLER_ID", "msha_corporate_history", "CONTROLLER_ID", 0.995, None),
    ("msha_accidents", "OPERATOR_ID", "msha_corporate_history", "OPERATOR_ID", 0.995, None),
    # Contractor third spine → registry.
    ("msha_accidents", "CONTRACTOR_ID", "msha_contractors", "CONTRACTOR_ID", 0.99, None),
    ("msha_enforcement_ledger", "CONTRACTOR_ID", "msha_contractors", "CONTRACTOR_ID", 0.88, None),
    # EVENT_NO inspection-event spine.
    ("msha_enforcement_ledger", "EVENT_NO", "msha_inspections", "EVENT_NO", 1.0, None),
    ("msha_personal_health_samples", "EVENT_NO", "msha_inspections", "EVENT_NO", 0.995, None),
    ("msha_noise_samples", "EVENT_NO", "msha_inspections", "EVENT_NO", 0.995, None),
    ("msha_area_samples", "EVENT_NO", "msha_inspections", "EVENT_NO", 0.995, None),
    # VIOLATION_NO / CITATION_NO / DOCKET_NO links.
    ("msha_contested_violations", "CITATION_NO", "msha_enforcement_ledger", "VIOLATION_NO", 0.94, None),
    ("msha_civil_penalty_dockets_decisions", "VIOLATION_NO", "msha_enforcement_ledger", "VIOLATION_NO", 0.94, None),
    ("msha_orders_issued", "VIOLATION_NO", "msha_enforcement_ledger", "VIOLATION_NO", 0.94, None),
    ("msha_contested_violations", "DOCKET_NO", "msha_civil_penalty_dockets_decisions", "DOCKET_NO", 0.99, None),
]

# Columns checked for format hygiene (whitespace + lowercase). Entity/contractor IDs only.
HYGIENE_COLS: dict[str, list[str]] = {
    "msha_enforcement_ledger": ["CONTROLLER_ID", "VIOLATOR_ID", "CONTRACTOR_ID"],
    "msha_accidents": ["CONTROLLER_ID", "OPERATOR_ID", "CONTRACTOR_ID"],
}

KNOWN_SAMEDAY_TIES = 778  # plan §6/R2: residual ties after latest-start-wins

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
)
app = modal.App("msha-spine-verify", image=image)


def _so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint, "region": "auto",
    }


def _indexed_cols(ds) -> set[str]:
    """Set of columns carrying a scalar index (reads the 'fields'/'columns' of each index)."""
    out: set[str] = set()
    for ix in ds.list_indices():
        fields = (ix.get("fields") or ix.get("columns")) if isinstance(ix, dict) else \
            (getattr(ix, "fields", None) or getattr(ix, "columns", None))
        if fields:
            out.update(fields)
        else:  # fallback: index name often == column name
            nm = ix.get("name") if isinstance(ix, dict) else getattr(ix, "name", None)
            if nm:
                out.add(nm)
    return out


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 30, memory=32768, cpu=8.0)
def check() -> dict:
    import duckdb
    import lance

    so = _so()
    con = duckdb.connect()
    con.execute("SET threads TO 8")

    # ---- load every projected spine column once (bounded: single key columns) ----
    needed: dict[str, set[str]] = {}
    for d, cols in SPINE_KEYS.items():
        needed.setdefault(d, set()).update(cols)
    for (cd, cc, pd_, pc, _f, filt) in EDGES:
        needed.setdefault(cd, set()).add(cc)
        needed.setdefault(pd_, set()).add(pc)
        if filt:
            needed.setdefault(cd, set()).add(filt.split()[0])
    for d, cols in HYGIENE_COLS.items():
        needed.setdefault(d, set()).update(cols)
    needed.setdefault("msha_corporate_history", set()).update(
        ["MINE_ID", "CONTROLLER_ID", "CONTROLLER_START_DT", "CONTROLLER_END_DT"])

    datasets = {}
    for d, cols in needed.items():
        ds = lance.dataset(f"{_ACTIVE}/{d}/", storage_options=so)
        datasets[d] = ds
        con.register(d.replace("msha_", "t_"), ds.scanner(columns=sorted(cols)).to_table())

    def t(d):  # registered table alias
        return d.replace("msha_", "t_")

    checks: list[dict] = []

    # ---- 1. resolution edges ----
    for (cd, cc, pd_, pc, floor, filt) in EDGES:
        where = f"{cc} IS NOT NULL" + (f" AND {filt}" if filt else "")
        nonnull = con.execute(f"SELECT count(*) FROM {t(cd)} WHERE {where}").fetchone()[0]
        matched = con.execute(
            f"SELECT count(*) FROM {t(cd)} c WHERE {where.replace(cc, 'c.'+cc, 1)} "
            f"AND EXISTS (SELECT 1 FROM {t(pd_)} p WHERE p.{pc} = c.{cc})").fetchone()[0]
        rate = matched / nonnull if nonnull else 1.0
        checks.append({"kind": "resolution", "edge": f"{cd}.{cc}->{pd_}.{pc}",
                       "rate": round(rate, 5), "floor": floor, "n": nonnull,
                       "ok": rate >= floor - 1e-9})

    # ---- 2. index hardening: every present spine key BTREE-indexed ----
    for d, keys in SPINE_KEYS.items():
        idx = _indexed_cols(datasets[d]) if d in datasets else _indexed_cols(
            lance.dataset(f"{_ACTIVE}/{d}/", storage_options=so))
        present = set(datasets[d].schema.names) if d in datasets else set()
        for k in keys:
            if present and k not in present:
                continue  # absent → not applicable
            checks.append({"kind": "index", "edge": f"{d}.{k}",
                           "ok": k in idx, "indexed": k in idx})

    # ---- 3. format hygiene: whitespace + lowercase on entity/contractor IDs ----
    lowercase_total = 0
    for d, cols in HYGIENE_COLS.items():
        for c in cols:
            ws = con.execute(
                f"SELECT count(*) FROM {t(d)} WHERE {c} IS NOT NULL AND {c} <> trim({c})").fetchone()[0]
            lc = con.execute(
                f"SELECT count(*) FROM {t(d)} WHERE {c} IS NOT NULL AND {c} <> upper({c})").fetchone()[0]
            lowercase_total += lc
            checks.append({"kind": "hygiene", "edge": f"{d}.{c}",
                           "whitespace": ws, "lowercase": lc, "ok": ws == 0 and lc == 0})
    # MINE_ID format (7-char all-digit) on the master + the giant child
    for d in ("msha_mines", "msha_enforcement_ledger"):
        bad = con.execute(
            f"SELECT count(*) FROM {t(d)} WHERE MINE_ID IS NOT NULL AND "
            f"NOT regexp_full_match(MINE_ID, '[0-9]{{7}}')").fetchone()[0]
        checks.append({"kind": "format", "edge": f"{d}.MINE_ID~^[0-9]{{7}}$",
                       "violations": bad, "ok": bad == 0})

    # ---- 4. SCD determinism (report + tie-collapse assertion) ----
    multi = con.execute("""
        SELECT count(*) FROM (
          SELECT MINE_ID FROM t_corporate_history WHERE CONTROLLER_END_DT IS NULL
          GROUP BY MINE_ID HAVING count(DISTINCT CONTROLLER_ID) > 1)""").fetchone()[0]
    ties = con.execute("""
        WITH cur AS (SELECT MINE_ID, CONTROLLER_ID, CONTROLLER_START_DT
                     FROM t_corporate_history WHERE CONTROLLER_END_DT IS NULL),
        latest AS (SELECT MINE_ID, max(CONTROLLER_START_DT) mx FROM cur GROUP BY MINE_ID)
        SELECT count(*) FROM (
          SELECT c.MINE_ID FROM cur c JOIN latest l
            ON c.MINE_ID=l.MINE_ID AND c.CONTROLLER_START_DT=l.mx
          GROUP BY c.MINE_ID HAVING count(DISTINCT c.CONTROLLER_ID) > 1)""").fetchone()[0]
    checks.append({"kind": "scd", "edge": "current_controller_multivalued",
                   "mines_multi_controller": multi, "sameday_ties_after_latest_start": ties,
                   "ok": ties <= KNOWN_SAMEDAY_TIES, "note": "drift signal; resolver caps ties"})

    failures = [c for c in checks if not c["ok"]]
    summary = {
        "ok": not failures,
        "n_checks": len(checks), "n_failures": len(failures),
        "lowercase_id_cells": lowercase_total,
        "by_kind": {k: {"total": sum(1 for c in checks if c["kind"] == k),
                        "failed": sum(1 for c in checks if c["kind"] == k and not c["ok"])}
                    for k in ("resolution", "index", "hygiene", "format", "scd")},
        "failures": failures,
        "checks": checks,
    }
    return summary


@app.local_entrypoint()
def check_local() -> None:
    import json
    import sys

    r = check.remote()
    print(json.dumps({k: v for k, v in r.items() if k != "checks"}, indent=2, default=str))
    print("\n=== per-edge (resolution) ===")
    for c in r["checks"]:
        if c["kind"] == "resolution":
            flag = "OK " if c["ok"] else "FAIL"
            print(f"  [{flag}] {c['edge']:62s} {c['rate']*100:6.2f}% (floor {c['floor']*100:.1f}%, n={c['n']:,})")
    print("\n=== index hardening (present spine keys) ===")
    for c in r["checks"]:
        if c["kind"] == "index" and not c["ok"]:
            print(f"  [GAP ] {c['edge']} — present but NOT BTREE-indexed")
    print(f"  ({r['by_kind']['index']['failed']} gaps of {r['by_kind']['index']['total']} spine keys)")
    print("\n=== hygiene / format / scd ===")
    for c in r["checks"]:
        if c["kind"] in ("hygiene", "format", "scd"):
            print(f"  [{'OK ' if c['ok'] else 'FAIL'}] {c['edge']}: "
                  + json.dumps({k: v for k, v in c.items() if k not in ('kind', 'edge', 'ok')}))
    print(f"\nOVERALL: {'PASS ✅' if r['ok'] else 'FAIL ❌'}  "
          f"({r['n_failures']} failing checks; {r['lowercase_id_cells']} lowercase ID cells)")
    sys.exit(0 if r["ok"] else 1)
