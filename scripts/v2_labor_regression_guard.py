#!/usr/bin/env python3
"""READ-ONLY regression guard for the reset-llm → v2-freeform-labor re-grind.

The re-grind path (pipelines/sam_gov/sam_labor_demand_extract_90day.py:
phase_llm_reset → bracket/select/grind → phase_llm_ingest) DELETES every `llm:%`
requirement row for a shard's resource_ids and re-extracts labor under the
free-form v2 prompt. v2's free-form vocabulary is a strict superset of the v1
36-token controlled vocab (blue-collar + a couple professional), so any rid that
carried a v1 LLM labor_category row should come back with EITHER a v2 labor row OR
its surviving regex:labor_category@v1 backstop. A rid that had v1 labor and now
carries NEITHER is a real extraction miss — the regression cohort.

reset DELETES the v1 `llm:%` rows, so the v1-labor set is unrecoverable after the
fact. The guard therefore runs in TWO passes around the re-grind, both READ-ONLY:

  1. snapshot (BEFORE phase_llm_reset) — for the shard's resource-ids file, record
     the set of rids carrying a v1 LLM labor_category row
     (extractor IN 'llm:session-opus@v1','llm:session-fable@v1'). Persisted to a
     snapshot JSON keyed by shard.
  2. verify (AFTER phase_llm_ingest) — re-read the live SoR and assert each
     snapshotted rid now carries a v2 labor_category row
     (extractor LIKE 'llm:%@v2-freeform-labor') OR a surviving
     regex:labor_category@v1 row. The regression cohort = had v1 labor, recovered
     NEITHER. verify exits 1 when that cohort is non-empty so the shard driver
     HOLDS the next shard's reset and surfaces the cohort for manual review.

Join shape mirrors scripts/_v2size_window.py + scripts/_v2size_yield.py.
READ-ONLY: registers the Lance dataset through DuckDB and only SELECTs — no leases,
no delete, no merge, no write of any kind to R2.

Per-shard wiring (driver checks verify's exit code):

    SO="doppler run -p core-x -c prd -- uv run --no-project \
        --with boto3 --with pylance --with duckdb python3"
    SNAP=/tmp/v2_labor_regression_${SHARD}.json

    $SO scripts/v2_labor_regression_guard.py --mode snapshot \
        --shard "$SHARD" --resource-ids-file "$SHARD_IDS" --snapshot "$SNAP"

    # ... phase reset-llm --resource-ids-file $SHARD_IDS
    # ... bracket / select / grind wave / phase ingest ...

    $SO scripts/v2_labor_regression_guard.py --mode verify --snapshot "$SNAP" \
      || { echo "REGRESSION on shard $SHARD — holding next reset"; exit 1; }
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys

import duckdb
import lance
import pyarrow as pa

# ── SoR + extractor lane literals (canonical; match the pipeline + v2size probes) ────────────────
REQUIREMENTS_URI = os.environ.get(
    "GOVCON_REQUIREMENTS_URI", "s3://data-sink/active/govcon_award_requirements/")
V1_LLM_EXTRACTORS = ("llm:session-opus@v1", "llm:session-fable@v1")
# Recovery accepts ANY engine that ran the v2 free-form prompt (canonical engine is
# session-opus → 'llm:session-opus@v2-freeform-labor'); prompt-version scope is what
# matters, and the broader match only ever REDUCES false regressions / false halts.
V2_LABOR_LIKE = "llm:%@v2-freeform-labor"
REGEX_LABOR_EXTRACTOR = "regex:labor_category@v1"
LABOR_TYPE = "labor_category"

REPORT_DEFAULT = os.environ.get("V2_LABOR_GUARD_REPORT", "/tmp/v2_labor_regression_guard_report.json")


def r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _read_ids_file(path: str) -> list[str]:
    with open(path) as fh:
        return sorted({ln.strip() for ln in fh if ln.strip()})


def _connect(requirements_uri: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='8GB'")
    con.register("req", lance.dataset(requirements_uri, storage_options=r2_so()))
    return con


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def snapshot(args) -> int:
    """BEFORE reset: capture the shard's v1-LLM labor_category rid set."""
    shard_ids = _read_ids_file(args.resource_ids_file)
    if not shard_ids:
        raise RuntimeError(f"resource-ids file is empty: {args.resource_ids_file}")
    con = _connect(args.requirements_uri)
    con.register("shard", pa.table({"rid": shard_ids}))
    v1_in = "(" + ",".join("'%s'" % e for e in V1_LLM_EXTRACTORS) + ")"
    v1_labor_rids = [r[0] for r in con.execute(f"""
        SELECT DISTINCT r.resource_id
        FROM req r JOIN shard s ON s.rid = r.resource_id
        WHERE r.requirement_type = '{LABOR_TYPE}'
          AND r.extractor IN {v1_in}
        ORDER BY 1
    """).fetchall()]
    con.close()

    snap = {
        "schema": "v2_labor_regression_guard.snapshot/1",
        "shard": args.shard,
        "snapshot_at": _now_iso(),
        "requirements_uri": args.requirements_uri,
        "v1_llm_extractors": list(V1_LLM_EXTRACTORS),
        "shard_resource_ids": len(shard_ids),
        "v1_labor_rids_count": len(v1_labor_rids),
        "v1_labor_rids": v1_labor_rids,
    }
    with open(args.snapshot, "w") as f:
        json.dump(snap, f, indent=2)
    summary = {"mode": "snapshot", "shard": args.shard,
               "shard_resource_ids": len(shard_ids),
               "v1_labor_rids": len(v1_labor_rids), "snapshot": args.snapshot}
    print(f"snapshot: {json.dumps(summary)}", flush=True)
    print("RESULT: " + json.dumps(summary), flush=True)
    return 0


def verify(args) -> int:
    """AFTER ingest: assert each snapshotted v1-labor rid recovered v2 OR regex labor."""
    with open(args.snapshot) as f:
        snap = json.load(f)
    rids = snap.get("v1_labor_rids") or []
    requirements_uri = snap.get("requirements_uri", args.requirements_uri)
    shard = snap.get("shard")

    report: dict = {
        "mode": "verify", "shard": shard, "verified_at": _now_iso(),
        "snapshot": args.snapshot, "snapshot_at": snap.get("snapshot_at"),
        "requirements_uri": requirements_uri,
        "v2_labor_match": V2_LABOR_LIKE, "regex_labor_extractor": REGEX_LABOR_EXTRACTOR,
        "v1_labor_rids": len(rids),
    }
    if not rids:
        # No rid carried v1 labor on this shard → nothing could regress.
        report.update({"recovered_via_v2": 0, "recovered_only_via_regex": 0,
                       "regression_cohort": 0, "regression_rids": [], "pass": True})
        _emit(report, args.report_out)
        print("RESULT: " + json.dumps({k: report[k] for k in
              ("mode", "shard", "v1_labor_rids", "regression_cohort", "pass")}), flush=True)
        return 0

    con = _connect(requirements_uri)
    con.register("snap", pa.table({"rid": rids}))
    rows = con.execute(f"""
        WITH v2lab AS (
            SELECT DISTINCT resource_id FROM req
            WHERE requirement_type = '{LABOR_TYPE}' AND extractor LIKE '{V2_LABOR_LIKE}'
        ),
        regexlab AS (
            SELECT DISTINCT resource_id FROM req
            WHERE requirement_type = '{LABOR_TYPE}' AND extractor = '{REGEX_LABOR_EXTRACTOR}'
        )
        SELECT s.rid,
               (v.resource_id IS NOT NULL) AS has_v2_labor,
               (g.resource_id IS NOT NULL) AS has_regex_labor
        FROM snap s
        LEFT JOIN v2lab v ON v.resource_id = s.rid
        LEFT JOIN regexlab g ON g.resource_id = s.rid
    """).fetchall()
    con.close()

    recovered_v2 = [r[0] for r in rows if r[1]]
    recovered_regex_only = [r[0] for r in rows if (not r[1]) and r[2]]
    regression = sorted(r[0] for r in rows if (not r[1]) and (not r[2]))

    report.update({
        "recovered_via_v2": len(recovered_v2),
        "recovered_only_via_regex": len(recovered_regex_only),  # v2 missed, regex backstops (WARN)
        "regression_cohort": len(regression),                   # neither → the fail condition
        "regression_rids": regression[:200],
        "pass": len(regression) == 0,
    })
    _emit(report, args.report_out)

    summary = {k: report[k] for k in
               ("mode", "shard", "v1_labor_rids", "recovered_via_v2",
                "recovered_only_via_regex", "regression_cohort", "pass")}
    if regression:
        print(f"verify: REGRESSION shard={shard} cohort={len(regression)} "
              f"(had v1 labor, recovered NEITHER v2 nor regex) — holding next shard's reset. "
              f"Sample: {regression[:10]}", flush=True)
    else:
        print(f"verify: PASS shard={shard} all {len(rids)} v1-labor rids recovered "
              f"(v2={len(recovered_v2)}, regex-only={len(recovered_regex_only)})", flush=True)
    print("RESULT: " + json.dumps(summary), flush=True)
    return 1 if regression else 0


def _emit(report: dict, report_out: str) -> None:
    with open(report_out, "w") as f:
        json.dump(report, f, indent=2)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", required=True, choices=["snapshot", "verify"])
    p.add_argument("--shard", default=None, help="shard label (snapshot: recorded; verify: from snapshot)")
    p.add_argument("--resource-ids-file", default=None,
                   help="snapshot: newline-delimited shard resource-ids file (the reset slice)")
    p.add_argument("--snapshot", default=None,
                   help="snapshot: output path; verify: input path. "
                        "Default /tmp/v2_labor_regression_<shard>.json")
    p.add_argument("--requirements-uri", default=REQUIREMENTS_URI,
                   help="govcon_award_requirements Lance URI (READ-ONLY)")
    p.add_argument("--report-out", default=REPORT_DEFAULT, help="verify: report JSON path")
    args = p.parse_args(argv)

    if args.snapshot is None:
        if not args.shard:
            p.error("--snapshot or --shard required (snapshot path defaults to shard label)")
        args.snapshot = f"/tmp/v2_labor_regression_{args.shard}.json"

    if args.mode == "snapshot":
        if not args.resource_ids_file:
            p.error("--mode snapshot requires --resource-ids-file")
        if not args.shard:
            p.error("--mode snapshot requires --shard")
        return snapshot(args)
    return verify(args)


if __name__ == "__main__":
    sys.exit(main())
