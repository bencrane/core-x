"""query-sidecar Phase 2 — the benchmark gate.

Runs 14 representative phrase.v2 workloads (10 byte-exact compiler test
fixtures) across three execution arms and reports wall-clock + result parity:

  A  live-lane replication — pure pylance scanner + Python set algebra,
     call-for-call faithful to apps/catalyst_api/src/market_store.py:
     _uei_set streamed uei-column scans, smallest-first set intersection,
     SEMI_JOIN_MAX=10_000 / IN_CHUNK=500 semi-joins, count_rows() pushdown
     totals, streamed cap-cut (never scanner(limit=)), collapse via
     COLLAPSE_SCAN_CAP=500_000 stream + Python dict aggregation.
  B  prefiltered-slice lane — the same Lance slice reads, streamed into
     DuckDB (register reader) with joins/aggregation in SQL.
  C  the query-sidecar artifact on local NVMe (Phase-3 conditions),
     including the Phase-2-promoted Tier C tables: prime_award_state
     (sorted current_end_date) and both inferred-code projections
     (sorted code_type, code). All legs run natively in the file.

Launch:  modal deploy pipelines/query_sidecar/benchmark_phase2.py, then
         modal run pipelines/query_sidecar/benchmark_phase2.py::main --runs 3
         (spawn-fires bench on the deployed app, prints the fc-id, exits; the
         helper deploy-if-stale gate re-deploys automatically when the deployed
         snapshot's commit != HEAD).
         NEVER modal run ...::bench — the SYNC input dies ~90 s after client loss.
Follow:  modal app logs query-sidecar-bench, or
         python3 -c "import modal; print(modal.FunctionCall.from_id('fc-...').get(timeout=0))"
Output:  JSON under '=== BENCH JSON ===' in worker logs, plus a durable record at
         s3://data-sink/query-sidecar/bench/; verdicts land in the Phase 2 run record.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time

import modal

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("duckdb>=1.5,<2", "pylance>=7", "pyarrow>=17", "boto3>=1.35")
)
app = modal.App("query-sidecar-bench", image=image)

LANCE_BASE = "s3://data-sink/active/"
R2_BUCKET = "data-sink"
SIDECAR_LATEST = "query-sidecar/LATEST.json"
LOCAL_SIDECAR = "/tmp/query_sidecar_bench.duckdb"

# live-lane constants, mirrored from market_store.py
IN_CHUNK = 500
SEMI_JOIN_MAX = 10_000
COLLAPSE_SCAN_CAP = 500_000
HARD_CAP = 1_000

URIS = {
    "entities": f"{LANCE_BASE}gtm_sam_entities/",
    "rollup": f"{LANCE_BASE}gtm_entity_behavior_rollup/",
    "lanes": f"{LANCE_BASE}gtm_entity_code_lanes/",
    "inferred_primeable": f"{LANCE_BASE}gtm_entity_inferred_primeable_codes/",
    "inferred_subbable": f"{LANCE_BASE}gtm_entity_inferred_subbable_codes/",
    "txn": f"{LANCE_BASE}usaspending_fpds_canonical_txn/",
    "award_state": f"{LANCE_BASE}usaspending_fpds_prime_award_state/",
}


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


# ── workload specs (compiled phrase.v2 plans, from the Phase 2 recon) ─────────
# Filter fragments use {D<n>} date placeholders resolved against `today`, and
# {CONSTR} for the construction sector NAICS in-list (resolved at runtime from
# naics_reference, mirroring the compiler's frozen sector alias).

WORKLOADS: list[dict] = [
    {"id": "RQ01", "family": "collapse",
     "phrase": "construction companies that received a code A mod in the last 90 days",
     "txn": "action_type_code = 'A' AND naics_code IN ({CONSTR}) AND action_date >= {D90}",
     "entity_scalar": None},
    {"id": "RQ02", "family": "collapse",
     "phrase": "construction companies that received a code Y mod over $5m in the last year",
     "txn": "action_type_code = 'Y' AND naics_code IN ({CONSTR}) AND federal_action_obligation >= 5000000.0 AND action_date >= {D365}",
     "entity_scalar": None},
    {"id": "RQ03", "family": "entity_lanes",
     "phrase": "companies over $10m that primed in 236220",
     "lane": ("prime", "naics", "236220"),
     "rollup": "prime_obl_lifetime >= 10000000.0"},
    {"id": "RQ04", "family": "entity_lanes",
     "phrase": "dsbs companies in VA that delivered subs under naics 236220",
     "lane": ("sub", "naics", "236220"),
     "entities": "in_dsbs = true AND physical_state = 'VA'"},
    {"id": "RQ05", "family": "entity_lanes",
     "phrase": "companies that primed in 541690 and also sub",
     "lane": ("prime", "naics", "541690"),
     "rollup": "prime_and_sub = true"},
    {"id": "RQ06", "family": "entity_inferred",
     "phrase": "sub-only companies with inferred primeable 541330",
     "inferred": ("inferred_primeable", "naics", "541330"),
     "rollup": "sub_ct_lifetime > 0 AND prime_award_ct_lifetime = 0"},
    {"id": "RQ07", "family": "entity_inferred",
     "phrase": "active dsbs companies in VA with inferred subbable psc R499",
     "inferred": ("inferred_subbable", "psc", "R499"),
     "rollup": "active_award_ct >= 1",
     "entities": "in_dsbs = true AND physical_state = 'VA'"},
    {"id": "RQ08", "family": "multi_hop",
     "phrase": "companies with inferred primeable 236220 that received a code G mod in the last 90 days",
     "txn": "action_type_code = 'G' AND action_date >= {D90}",
     "inferred": ("inferred_primeable", "naics", "236220")},
    {"id": "RQ09", "family": "multi_hop",
     "phrase": "construction companies with awards expiring within 180 days that received a code G mod in the last 90 days",
     "txn": "action_type_code = 'G' AND naics_code IN ({CONSTR}) AND action_date >= {D90}",
     "award": "award_topology IN ('standalone','vehicle_order') AND naics_code IN ({CONSTR}) AND current_end_date <= {A180} AND current_end_date >= {TODAY}",
     "expiring_days": 180},
    {"id": "RQ10", "family": "collapse",
     "phrase": "companies with awards expiring within 90 days",
     "award": "award_topology IN ('standalone','vehicle_order') AND current_end_date <= {A90} AND current_end_date >= {TODAY}",
     "expiring_days": 90},
    {"id": "RQ11", "family": "prime_awards",
     "phrase": "awards over $5m expiring within 365 days",
     "award_rows": "award_topology IN ('standalone','vehicle_order') AND life_to_date_obligated >= 5000000.0 AND current_end_date <= {A365} AND current_end_date >= {TODAY}"},
    {"id": "RQ12", "family": "prime_awards",
     "phrase": "awards from gsa in psc D302 acted in the last 90 days",
     "award_rows": "product_or_service_code = 'D302' AND awarding_agency_code = '047' AND last_action_date >= {D90}"},
    {"id": "RQ13", "family": "transactions",
     "phrase": "actions over $5m in naics 237310 in the last 90 days",
     "txn_rows": "naics_code = '237310' AND federal_action_obligation >= 5000000.0 AND action_date >= {D90}"},
    {"id": "RQ14", "family": "transactions",
     "phrase": "actions with a subcontracting plan in psc R499 in the last 180 days",
     "txn_rows": "subcontracting_plan IN ('C','D','E','F','G','H') AND product_or_service_code = 'R499' AND action_date >= {D180}"},
]

TXN_RESULT_COLS = ["contract_transaction_unique_key", "contract_award_unique_key",
                   "recipient_uei", "action_date", "action_type_code",
                   "federal_action_obligation", "naics_code", "product_or_service_code",
                   "awarding_agency_code"]
AWARD_RESULT_COLS = ["contract_award_unique_key", "award_topology", "recipient_uei",
                     "naics_code", "product_or_service_code", "awarding_agency_code",
                     "life_to_date_obligated", "current_end_date", "last_action_date"]


def _dates(today: dt.date) -> dict[str, str]:
    def d(n): return f"DATE '{(today - dt.timedelta(days=n)).isoformat()}'"
    def a(n): return f"DATE '{(today + dt.timedelta(days=n)).isoformat()}'"
    return {"D90": d(90), "D180": d(180), "D365": d(365),
            "A90": a(90), "A180": a(180), "A365": a(365),
            "TODAY": f"DATE '{today.isoformat()}'"}


# ── arm A: faithful market_store replication (pylance + python sets) ─────────

def _uei_set(ds, pred: str) -> set:
    out = set()
    for batch in ds.scanner(columns=["uei"], filter=pred).to_batches():
        out.update(u for u in batch.column(0).to_pylist() if u)
    return out


def _collapse_recipients(ds, pred: str, money_col: str, uei_col: str, date_col: str):
    """market_store execute_table_collapse: streamed scan + python dict agg."""
    agg: dict[str, float] = {}
    n = 0
    for batch in ds.scanner(columns=[uei_col, money_col, date_col], filter=pred).to_batches():
        rows = batch.to_pylist()
        for r in rows:
            u = r[uei_col]
            if u:
                try:
                    agg[u] = agg.get(u, 0.0) + float(r[money_col] or 0)
                except (TypeError, ValueError):
                    pass
        n += len(rows)
        if n >= COLLAPSE_SCAN_CAP:
            break
    return agg, n


def _semi_join_entities(ds, candidate: set, entities_pred: str | None) -> set:
    if not entities_pred:
        return candidate
    if len(candidate) <= SEMI_JOIN_MAX:
        final = set()
        cand = sorted(candidate)
        for i in range(0, len(cand), IN_CHUNK):
            chunk = ", ".join(f"'{u}'" for u in cand[i:i + IN_CHUNK])
            final |= _uei_set(ds, f"uei IN ({chunk}) AND {entities_pred}")
        return final
    return _uei_set(ds, entities_pred) & candidate


def run_arm_a(handles, spec, frag) -> dict:
    t0 = time.monotonic()
    sets, meta = [], {}
    if spec.get("txn_rows"):
        pred = spec["txn_rows"].format(**frag)
        total = handles["txn"].count_rows(filter=pred)
        rows, got = [], 0
        for batch in handles["txn"].scanner(columns=TXN_RESULT_COLS, filter=pred).to_batches():
            rows.extend(batch.to_pylist())
            if len(rows) >= HARD_CAP:
                break
        return {"ms": (time.monotonic() - t0) * 1000, "total": total, "returned": min(len(rows), HARD_CAP)}
    if spec.get("award_rows"):
        pred = spec["award_rows"].format(**frag)
        total = handles["award_state"].count_rows(filter=pred)
        rows = []
        for batch in handles["award_state"].scanner(columns=AWARD_RESULT_COLS, filter=pred).to_batches():
            rows.extend(batch.to_pylist())
            if len(rows) >= HARD_CAP:
                break
        return {"ms": (time.monotonic() - t0) * 1000, "total": total, "returned": min(len(rows), HARD_CAP)}

    if spec.get("txn"):
        agg, scanned = _collapse_recipients(
            handles["txn"], spec["txn"].format(**frag),
            "federal_action_obligation", "recipient_uei", "action_date")
        sets.append(set(agg))
        meta["txn_scanned"] = scanned
    if spec.get("award"):
        agg, scanned = _collapse_recipients(
            handles["award_state"], spec["award"].format(**frag),
            "life_to_date_obligated", "recipient_uei", "last_action_date")
        sets.append(set(agg))
        meta["award_scanned"] = scanned
    if spec.get("lane"):
        side, ct, code = spec["lane"]
        sets.append(_uei_set(handles["lanes"],
                    f"side = '{side}' AND code_type = '{ct}' AND code IN ('{code}')"))
    if spec.get("inferred"):
        src, ct, code = spec["inferred"]
        sets.append(_uei_set(handles[src], f"code_type = '{ct}' AND code IN ('{code}')"))
    if spec.get("rollup"):
        sets.append(_uei_set(handles["rollup"], spec["rollup"]))

    if sets and not all(sets):
        return {"ms": (time.monotonic() - t0) * 1000, "total": 0, "returned": 0, **meta}
    candidate = set.intersection(*sorted(sets, key=len)) if sets else set()
    final = _semi_join_entities(handles["entities"], candidate, spec.get("entities"))
    ueis = sorted(final)[:HARD_CAP]
    return {"ms": (time.monotonic() - t0) * 1000, "total": len(final), "returned": len(ueis), **meta}


# ── arm B: same Lance slices, DuckDB does the algebra ────────────────────────

def run_arm_b(con, handles, spec, frag) -> dict:
    t0 = time.monotonic()

    def reg(name, ds, cols, pred):
        con.register(name, ds.scanner(columns=cols, filter=pred).to_reader())
        con.execute(f"CREATE OR REPLACE TEMP TABLE {name}_t AS SELECT * FROM {name}")
        con.unregister(name)

    legs, joins = [], []
    if spec.get("txn_rows"):
        pred = spec["txn_rows"].format(**frag)
        reg("txr", handles["txn"], TXN_RESULT_COLS, pred)
        n = con.execute("SELECT count(*) FROM txr_t").fetchone()[0]
        return {"ms": (time.monotonic() - t0) * 1000, "total": n, "returned": min(n, HARD_CAP)}
    if spec.get("award_rows"):
        pred = spec["award_rows"].format(**frag)
        reg("awr", handles["award_state"], AWARD_RESULT_COLS, pred)
        n = con.execute("SELECT count(*) FROM awr_t").fetchone()[0]
        return {"ms": (time.monotonic() - t0) * 1000, "total": n, "returned": min(n, HARD_CAP)}

    if spec.get("txn"):
        reg("tx", handles["txn"], ["recipient_uei"], spec["txn"].format(**frag))
        legs.append("SELECT DISTINCT recipient_uei AS uei FROM tx_t")
    if spec.get("award"):
        reg("aw", handles["award_state"], ["recipient_uei"], spec["award"].format(**frag))
        legs.append("SELECT DISTINCT recipient_uei AS uei FROM aw_t")
    if spec.get("lane"):
        side, ct, code = spec["lane"]
        reg("ln", handles["lanes"], ["uei"], f"side = '{side}' AND code_type = '{ct}' AND code IN ('{code}')")
        legs.append("SELECT DISTINCT uei FROM ln_t")
    if spec.get("inferred"):
        src, ct, code = spec["inferred"]
        reg("inf", handles[src], ["uei"], f"code_type = '{ct}' AND code IN ('{code}')")
        legs.append("SELECT DISTINCT uei FROM inf_t")
    if spec.get("rollup"):
        reg("ro", handles["rollup"], ["uei"], spec["rollup"])
        legs.append("SELECT DISTINCT uei FROM ro_t")
    if spec.get("entities"):
        reg("en", handles["entities"], ["uei"], spec["entities"])
        legs.append("SELECT DISTINCT uei FROM en_t")

    inter = " INTERSECT ".join(legs)
    n = con.execute(f"SELECT count(*) FROM ({inter})").fetchone()[0]
    return {"ms": (time.monotonic() - t0) * 1000, "total": n, "returned": min(n, HARD_CAP)}


# ── arm C: the sidecar artifact (local NVMe); hybrid Lance for Tier C legs ────

def run_arm_c(con, handles, spec, frag, constr_list_sql) -> dict:
    t0 = time.monotonic()
    f = dict(frag)
    f["CONSTR"] = constr_list_sql

    if spec.get("txn_rows"):
        pred = (spec["txn_rows"].format(**f)
                .replace("federal_action_obligation", "obligation")
                .replace("product_or_service_code", "psc_code"))
        n = con.execute(f"SELECT count(*) FROM qs.gtm_txn_events_slim WHERE {pred}").fetchone()[0]
        return {"ms": (time.monotonic() - t0) * 1000, "total": n, "returned": min(n, HARD_CAP)}
    if spec.get("award_rows"):
        pred = spec["award_rows"].format(**f)
        n = con.execute(
            f"SELECT count(*) FROM qs.usaspending_fpds_prime_award_state WHERE {pred}"
        ).fetchone()[0]
        return {"ms": (time.monotonic() - t0) * 1000, "total": n, "returned": min(n, HARD_CAP)}

    legs = []
    if spec.get("txn"):
        pred = (spec["txn"].format(**f)
                .replace("federal_action_obligation", "obligation"))
        legs.append(f"SELECT DISTINCT uei FROM qs.gtm_txn_events_slim WHERE {pred}")
    if spec.get("award"):
        # exact award-lane predicate against the promoted Tier C table — no
        # month-grain approximation (gtm_award_expiry_months no longer used here)
        pred = spec["award"].format(**f)
        legs.append("SELECT DISTINCT recipient_uei AS uei FROM qs.usaspending_fpds_prime_award_state "
                    f"WHERE {pred}")
    if spec.get("lane"):
        side, ct, code = spec["lane"]
        legs.append(f"SELECT DISTINCT uei FROM qs.gtm_entity_code_lanes WHERE side='{side}' AND code_type='{ct}' AND code='{code}'")
    if spec.get("inferred"):
        src, ct, code = spec["inferred"]
        table = {"inferred_primeable": "gtm_entity_inferred_primeable_codes",
                 "inferred_subbable": "gtm_entity_inferred_subbable_codes"}[src]
        legs.append(f"SELECT DISTINCT uei FROM qs.{table} WHERE code_type='{ct}' AND code='{code}'")
    if spec.get("rollup"):
        legs.append(f"SELECT uei FROM qs.gtm_entity_behavior_rollup WHERE {spec['rollup']}")
    if spec.get("entities"):
        legs.append(f"SELECT uei FROM qs.gtm_sam_entities WHERE {spec['entities']}")

    inter = " INTERSECT ".join(legs)
    n = con.execute(f"SELECT count(*) FROM ({inter})").fetchone()[0]
    return {"ms": (time.monotonic() - t0) * 1000, "total": n, "returned": min(n, HARD_CAP)}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")],
              memory=65_536, cpu=8.0, ephemeral_disk=524_288, timeout=60 * 60 * 3)
def bench(runs: int = 3) -> dict:
    import boto3
    import duckdb
    import lance
    from botocore.config import Config

    so = _r2_storage_options()
    today = dt.date.today()
    frag = _dates(today)

    # sidecar artifact -> local NVMe (Phase-3 conditions)
    s3 = boto3.client("s3", endpoint_url=so["endpoint"],
                      aws_access_key_id=so["aws_access_key_id"],
                      aws_secret_access_key=so["aws_secret_access_key"], region_name="auto",
                      config=Config(request_checksum_calculation="when_required",
                                    response_checksum_validation="when_required"))
    latest = json.loads(s3.get_object(Bucket=R2_BUCKET, Key=SIDECAR_LATEST)["Body"].read())
    t0 = time.monotonic()
    s3.download_file(R2_BUCKET, latest["key"], LOCAL_SIDECAR)
    dl_s = round(time.monotonic() - t0, 1)
    print(f"[setup] sidecar {latest['key']} -> local NVMe in {dl_s}s")

    con = duckdb.connect()
    con.execute("SET memory_limit='48GB'; SET threads=8; SET temp_directory='/tmp/bench_spill';")
    con.execute(f"ATTACH '{LOCAL_SIDECAR}' AS qs (READ_ONLY)")

    # warm Lance handles (mirrors catalyst's warm-handle production state)
    handles = {k: lance.dataset(u, storage_options=so) for k, u in URIS.items()}
    for k, h in handles.items():
        h.count_rows()  # touch manifest + stats

    constr = [r[0] for r in con.execute(
        "SELECT naics_code FROM qs.naics_reference WHERE naics_code LIKE '23%' AND len(naics_code)=6").fetchall()]
    constr_lance = ", ".join(f"'{c}'" for c in constr)
    frag["CONSTR"] = constr_lance
    print(f"[setup] construction sector: {len(constr)} NAICS leaves")

    results = []
    for spec in WORKLOADS:
        row = {"id": spec["id"], "family": spec["family"], "phrase": spec["phrase"], "arms": {}}
        for arm, fn in (("A", lambda: run_arm_a(handles, spec, frag)),
                        ("B", lambda: run_arm_b(con, handles, spec, frag)),
                        ("C", lambda: run_arm_c(con, handles, spec, frag, constr_lance))):
            timings, last = [], None
            try:
                for _ in range(runs):
                    last = fn()
                    if last.get("ms") is None:
                        break
                    timings.append(last["ms"])
                row["arms"][arm] = {
                    "median_ms": round(sorted(timings)[len(timings) // 2], 1) if timings else None,
                    "all_ms": [round(x, 1) for x in timings],
                    "total": last.get("total"), "note": last.get("note"),
                }
            except Exception as exc:  # noqa: BLE001
                row["arms"][arm] = {"error": str(exc)[:300]}
            print(f"[{spec['id']}][{arm}] {row['arms'][arm]}")
        results.append(row)

    out = {"sidecar": latest["key"], "download_s": dl_s, "today": today.isoformat(),
           "runs": runs, "results": results}
    print("=== BENCH JSON ===")
    print(json.dumps(out))

    # durable terminal record (this file has no ops ledger; the spawned call's
    # return value is only retrievable within Modal's retention window) —
    # timestamp+fc-id keyed, so a double-fire yields two distinct records.
    fc_id = modal.current_function_call_id() or "local"
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    record_key = f"query-sidecar/bench/bench_{ts}_{fc_id}.json"
    s3.put_object(Bucket=R2_BUCKET, Key=record_key, Body=json.dumps(out).encode())
    print(f"[record] s3://{R2_BUCKET}/{record_key}")
    return out


@app.local_entrypoint()
def main(runs: int = 3):
    from pipelines._shared.launch import spawn_deployed

    spawn_deployed("query-sidecar-bench", "bench", deploy_file=__file__, runs=runs)
