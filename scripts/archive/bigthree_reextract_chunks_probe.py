#!/usr/bin/env python3
"""READ-ONLY — authoritative LLM-fed chunk + payload measurement for the Big-Three
labor re-extraction. Counts chunks DIRECTLY from the three LLM-input sinks
(govcon_scope_vectors / govcon_pricing / govcon_unknown) for the cohort's
successfully-downloaded attachment resource_ids — the ground truth of "chunks fed
to the LLM" (the extraction event ledger's n_chunks double-counts a separate
full-body marking pass). Also emits exact chunk-payload chars via length(text),
and a precise Step-1 active/future cohort breakdown. NO embedding column is read.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/bigthree_reextract_chunks_probe.py > /tmp/bigthree_chunks.json
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

import duckdb
import lance

A = "s3://data-sink/active"
GAA = f"{A}/govcon_active_awards/"
MAN = f"{A}/sam_opps_attachment_manifest_winners/"
FILES = f"{A}/sam_attachment_files/"
SINKS = {"scope": f"{A}/govcon_scope_vectors/",
         "pricing": f"{A}/govcon_pricing/",
         "unknown": f"{A}/govcon_unknown/"}


def r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def log(m: str) -> None:
    print(m, file=sys.stderr, flush=True)


def rows(con, q):
    cur = con.execute(q)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def one(con, q):
    return con.execute(q).fetchone()


def main() -> int:
    so = r2_so()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/duck_spill_btc", exist_ok=True)
    con.execute("SET memory_limit='10GB'")
    con.execute("SET temp_directory='/tmp/duck_spill_btc'")
    con.register("gaa", lance.dataset(GAA, storage_options=so))
    con.register("man", lance.dataset(MAN, storage_options=so))
    con.register("fl", lance.dataset(FILES, storage_options=so))

    out: dict = {"as_of": dt.datetime.now(dt.timezone.utc).isoformat()}

    # precise Step-1 breakdown
    log("step1 breakdown")
    out["cohort_breakdown"] = rows(con, """
        SELECT upper(trim(psc_code)) AS psc,
               count(*) AS total_members,
               count(*) FILTER (WHERE active_current) AS active_current,
               count(*) FILTER (WHERE active_potential) AS active_potential,
               count(*) FILTER (WHERE active_current OR active_potential) AS future_dated,
               count(*) FILTER (WHERE pop_unknown) AS pop_unknown,
               count(*) FILTER (WHERE active_current IS NULL) AS active_current_null,
               count(*) FILTER (WHERE active_potential IS NULL) AS active_potential_null
        FROM gaa WHERE upper(trim(psc_code)) IN ('DA01','R425','R499')
        GROUP BY 1 ORDER BY total_members DESC""")

    # cohort downloaded resource set (same join as sizing probe)
    log("cohort downloaded resources")
    con.execute("""
        CREATE TEMP TABLE dl AS
        SELECT DISTINCT f.resource_id
        FROM fl f
        JOIN (SELECT DISTINCT m.resource_id
              FROM man m
              JOIN (SELECT contract_award_unique_key AS k FROM gaa
                    WHERE upper(trim(psc_code)) IN ('DA01','R425','R499')
                      AND contract_award_unique_key IS NOT NULL) bt
                ON m.contract_award_unique_key = bt.k
              WHERE m.resource_id IS NOT NULL) c
          ON f.resource_id = c.resource_id
        WHERE f.status='downloaded'
    """)
    n_dl = one(con, "SELECT count(*) FROM dl")[0]
    out["downloaded_files_in_cohort"] = n_dl

    # per-sink chunk + exact payload-char count for cohort resources
    per_sink = {}
    grand_chunks = 0
    grand_chars = 0
    union_res = set()
    for sink, uri in SINKS.items():
        log(f"sink {sink}")
        try:
            ds = lance.dataset(uri, storage_options=so)
        except Exception as exc:  # noqa: BLE001
            per_sink[sink] = {"uri": uri, "error": f"{type(exc).__name__}: {exc}"}
            continue
        cols = ds.schema.names
        has_text = "text" in cols
        # scan only the columns we need (NEVER embedding)
        scan_cols = [c for c in ("resource_id", "chunk_id", "text") if c in cols]
        con.register("sink_src", ds.scanner(columns=scan_cols).to_reader())
        con.execute("CREATE TEMP TABLE sink_t AS SELECT * FROM sink_src")
        con.unregister("sink_src")
        textlen = "sum(length(text))" if has_text else "0"
        r = one(con, f"""
            SELECT count(*) AS chunks,
                   count(DISTINCT s.resource_id) AS resources,
                   coalesce({textlen},0) AS payload_chars,
                   coalesce(avg(length(text)),0) AS avg_chunk_chars
            FROM sink_t s JOIN dl ON s.resource_id = dl.resource_id""")
        per_sink[sink] = {
            "uri": uri, "total_sink_rows": ds.count_rows(),
            "cohort_chunks": r[0], "cohort_resources": r[1],
            "cohort_payload_chars": int(r[2]),
            "avg_chunk_chars": round(r[3], 1) if has_text else None,
        }
        grand_chunks += r[0]
        grand_chars += int(r[2])
        for rr in con.execute(
                "SELECT DISTINCT s.resource_id FROM sink_t s JOIN dl ON s.resource_id=dl.resource_id").fetchall():
            union_res.add(rr[0])
        con.execute("DROP TABLE sink_t")

    out["per_sink"] = per_sink
    out["llm_fed_totals"] = {
        "total_chunks_fed_to_llm": grand_chunks,
        "total_payload_chars_exact": grand_chars,
        "distinct_resources_with_chunks": len(union_res),
        "note": "chunks counted directly from the 3 LLM-input sinks (scope+pricing+unknown); "
                "payload_chars = sum(length(text)) over those chunk rows — exact, overlap included.",
    }
    json.dump(out, sys.stdout, indent=2, default=str)
    print("\nDONE", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
