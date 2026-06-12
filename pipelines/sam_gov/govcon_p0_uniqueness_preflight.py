"""GovCon Phase-0 item-1 uniqueness & ledger-reconcile pre-flight (READ-ONLY).

Build plan: docs/plans/GOVCON_SCOPE_PROCESSING_AND_GTM_QUERY_BUILD_PLAN.md — PHASE 0 item 1.
Per chunk sink (govcon_scope_vectors_90day / govcon_pricing_90day / govcon_unknown_90day):

  (a) UNIQUENESS: assert count(*) == count(DISTINCT chunk_id).
  (b) §12 RECONCILE (spec SAM_90DAY_EXTRACTION_PIPELINE_SPEC_V2.md §12 "Vector integrity"): for
      every resource_id whose latest-terminal ledger state is `extracted_<sink>`, assert
      ledger.n_chunks == COUNT(*) of its chunk_ids in the matching sink. Offending resource_ids
      are logged (sampled). Also reports ORPHANS — resources holding rows in a sink whose
      latest-terminal state is NOT that sink's `extracted_*` state (stale chunks from a
      superseded attempt).

READ-ONLY by construction: scans key columns only (`chunk_id`, `resource_id`; ledger scalars),
NEVER touches `embedding`, writes nothing. On a sink FAIL the remediation is the patched
delete-based `phase_finalize` in sam_attachment_extract_90day.py (`--phase finalize`) — safe
pre-Phase-5 because no vector index exists yet (the patched path refuses compaction otherwise).

Run (read-only; exit 0 = all sinks PASS, 1 = any FAIL):
    doppler run -- python pipelines/sam_gov/govcon_p0_uniqueness_preflight.py
"""
from __future__ import annotations

import argparse
import json
import sys

from pipelines.sam_gov.sam_attachment_extract_90day import (
    EXTRACTION_URI,
    PRICING_URI,
    SCOPE_URI,
    UNKNOWN_URI,
    _AUDIT_STATES,
    _dataset_exists,
    _r2_storage_options,
)

_INTERMEDIATE = ("routed", "extracted_spreadsheet", "requires_ocr")
_SINKS = (("scope", SCOPE_URI, "extracted_scope"),
          ("pricing", PRICING_URI, "extracted_pricing"),
          ("unknown", UNKNOWN_URI, "extracted_unknown"))
_SAMPLE_N = 10


def _resolution_with_chunks(so: dict):
    """Latest event per resource_id (terminal-first, then attempt, then completed_at) carrying
    state + n_chunks — same ordering contract as the extractor's resolution view (D2)."""
    import duckdb
    import lance
    led = lance.dataset(EXTRACTION_URI, storage_options=so).to_table(
        columns=["resource_id", "state", "attempt", "completed_at", "n_chunks"])
    con = duckdb.connect()
    con.register("led", led)
    inter = ",".join(f"'{s}'" for s in _INTERMEDIATE)
    audit = ",".join(f"'{s}'" for s in _AUDIT_STATES)
    return con, con.execute(f"""
        SELECT resource_id, state, n_chunks FROM (
          SELECT resource_id, state, n_chunks,
                 row_number() OVER (PARTITION BY resource_id
                   ORDER BY (state NOT IN ({inter})) DESC, attempt DESC, completed_at DESC) AS rn
          FROM led WHERE state NOT IN ({audit})
        ) WHERE rn = 1
    """).to_arrow_table()


def preflight(so: dict) -> dict:
    import duckdb
    import lance
    con, resolution = _resolution_with_chunks(so)
    con.register("resolution", resolution)
    report: dict = {"sinks": {}, "all_pass": True}

    for name, uri, terminal_state in _SINKS:
        if not _dataset_exists(uri, so):
            report["sinks"][name] = {"status": "ABSENT"}
            report["all_pass"] = False
            continue
        keys = lance.dataset(uri, storage_options=so).to_table(columns=["chunk_id", "resource_id"])
        con.register("sink_keys", keys)

        n_rows, n_distinct = con.execute(
            "SELECT count(*), count(DISTINCT chunk_id) FROM sink_keys").fetchone()
        uniq_pass = n_rows == n_distinct

        # §12 reconcile at resource grain against the latest-terminal ledger view.
        rec = con.execute(f"""
            WITH per_res AS (
              SELECT resource_id, count(*) AS sink_chunks FROM sink_keys GROUP BY resource_id
            ),
            led AS (
              SELECT resource_id, n_chunks FROM resolution WHERE state = '{terminal_state}'
            )
            SELECT
              (SELECT count(*) FROM led)                                            AS ledger_resources,
              (SELECT count(*) FROM per_res)                                        AS sink_resources,
              (SELECT count(*) FROM led l JOIN per_res p USING (resource_id)
                 WHERE l.n_chunks = p.sink_chunks)                                  AS matched,
              (SELECT count(*) FROM led l JOIN per_res p USING (resource_id)
                 WHERE l.n_chunks <> p.sink_chunks)                                 AS mismatched,
              (SELECT count(*) FROM led l LEFT JOIN per_res p USING (resource_id)
                 WHERE p.resource_id IS NULL)                                       AS ledger_only,
              (SELECT count(*) FROM per_res p LEFT JOIN led l USING (resource_id)
                 WHERE l.resource_id IS NULL)                                       AS orphan_resources
        """).fetchone()
        ledger_resources, sink_resources, matched, mismatched, ledger_only, orphans = rec

        mismatch_sample = con.execute(f"""
            SELECT l.resource_id, l.n_chunks AS ledger_n, p.sink_chunks AS sink_n
            FROM resolution l JOIN (SELECT resource_id, count(*) AS sink_chunks
                                    FROM sink_keys GROUP BY resource_id) p USING (resource_id)
            WHERE l.state = '{terminal_state}' AND l.n_chunks <> p.sink_chunks
            ORDER BY abs(l.n_chunks - p.sink_chunks) DESC LIMIT {_SAMPLE_N}
        """).fetchall()
        orphan_sample = con.execute(f"""
            SELECT p.resource_id, p.sink_chunks,
                   coalesce((SELECT r.state FROM resolution r WHERE r.resource_id = p.resource_id),
                            '<no ledger row>') AS resolved_state
            FROM (SELECT resource_id, count(*) AS sink_chunks FROM sink_keys GROUP BY resource_id) p
            LEFT JOIN (SELECT resource_id FROM resolution WHERE state = '{terminal_state}') l
              USING (resource_id)
            WHERE l.resource_id IS NULL LIMIT {_SAMPLE_N}
        """).fetchall()

        reconcile_pass = mismatched == 0 and orphans == 0
        sink_pass = uniq_pass and reconcile_pass
        report["all_pass"] = report["all_pass"] and sink_pass
        report["sinks"][name] = {
            "status": "PASS" if sink_pass else "FAIL",
            "rows": n_rows, "distinct_chunk_id": n_distinct,
            "duplicate_rows": n_rows - n_distinct, "uniqueness_pass": uniq_pass,
            "ledger_terminal_resources": ledger_resources, "sink_resources": sink_resources,
            "n_chunks_matched": matched, "n_chunks_mismatched": mismatched,
            "ledger_only_resources": ledger_only, "orphan_resources": orphans,
            "reconcile_pass": reconcile_pass,
            "mismatch_sample": [{"resource_id": r, "ledger_n": a, "sink_n": b}
                                for r, a, b in mismatch_sample],
            "orphan_sample": [{"resource_id": r, "sink_chunks": c, "resolved_state": s}
                              for r, c, s in orphan_sample],
        }
        print(f"preflight {name}: rows={n_rows:,} distinct={n_distinct:,} dupes={n_rows - n_distinct:,} "
              f"uniq={'PASS' if uniq_pass else 'FAIL'} | ledger_res={ledger_resources:,} "
              f"matched={matched:,} mismatched={mismatched:,} ledger_only={ledger_only:,} "
              f"orphans={orphans:,} reconcile={'PASS' if reconcile_pass else 'FAIL'}", flush=True)
        con.unregister("sink_keys")
    return report


def _cli() -> None:
    p = argparse.ArgumentParser(description="GovCon P0 item-1 uniqueness + §12 reconcile (read-only).")
    p.add_argument("--json-out", default=None, help="optional path for the full JSON report")
    a = p.parse_args()
    so = _r2_storage_options()
    report = preflight(so)
    if a.json_out:
        with open(a.json_out, "w") as fh:
            json.dump(report, fh, indent=2)
    print("P0_PREFLIGHT_RESULT: " + json.dumps(
        {k: {"status": v.get("status"), "rows": v.get("rows"),
             "distinct_chunk_id": v.get("distinct_chunk_id"),
             "n_chunks_mismatched": v.get("n_chunks_mismatched"),
             "orphan_resources": v.get("orphan_resources")}
         for k, v in report["sinks"].items()} | {"all_pass": report["all_pass"]}), flush=True)
    sys.exit(0 if report["all_pass"] else 1)


if __name__ == "__main__":
    _cli()
