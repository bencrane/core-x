#!/usr/bin/env python3
"""READ-ONLY sizing probe — Big-Three (DA01/R425/R499) labor re-extraction workload.

Joins govcon_active_awards -> sam_opps_attachment_manifest_winners ->
sam_attachment_files -> sam_attachment_extraction to measure, for a greenlight
decision on re-running the LLM labor lane with an uncapped (free-form) prompt:

  1. addressable cohort size (active Big-Three awards; awards w/ downloaded attachments)
  2. file & chunk volume (distinct downloaded files; true bytes/GB; MEASURED chunk count)
  3. inputs to the API cost/time model (chunk payload chars, extracted-char totals)

Fan-out safety: file/byte/chunk aggregates are over the DISTINCT set of cohort
resource_ids (a solicitation can map to >1 award and an award to many files).
Extraction ledger is append-only multi-stage — deduped to the LATEST chunk-bearing
event per resource_id. NO writes.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/bigthree_reextract_sizing_probe.py > /tmp/bigthree_sizing.json
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
EXTRACT = f"{A}/sam_attachment_extraction/"
BIG_THREE = ("DA01", "R425", "R499")

CHUNK_CHARS = 1200
CHUNK_OVERLAP = 180


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


def rows(con, q, p=None):
    cur = con.execute(q, p) if p else con.execute(q)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def one(con, q, p=None):
    return (con.execute(q, p) if p else con.execute(q)).fetchone()


def main() -> int:
    so = r2_so()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/duck_spill_bt", exist_ok=True)
    con.execute("SET memory_limit='10GB'")
    con.execute("SET temp_directory='/tmp/duck_spill_bt'")
    con.register("gaa", lance.dataset(GAA, storage_options=so))
    con.register("man", lance.dataset(MAN, storage_options=so))
    con.register("fl", lance.dataset(FILES, storage_options=so))
    con.register("ex", lance.dataset(EXTRACT, storage_options=so))

    out: dict = {"as_of": dt.datetime.now(dt.timezone.utc).isoformat(),
                 "big_three": list(BIG_THREE),
                 "chunk_chars": CHUNK_CHARS, "chunk_overlap": CHUNK_OVERLAP}
    inlist = "('DA01','R425','R499')"

    # ── STEP 1 — cohort ────────────────────────────────────────────────
    log("step1: cohort")
    con.execute(f"""
        CREATE TEMP TABLE bt AS
        SELECT contract_award_unique_key AS k,
               upper(trim(psc_code)) AS psc,
               (active_current OR active_potential) AS future_dated
        FROM gaa
        WHERE upper(trim(psc_code)) IN {inlist}
          AND contract_award_unique_key IS NOT NULL
    """)
    out["step1_cohort"] = {
        "total_active_members": one(con, "SELECT count(*) FROM bt")[0],
        "future_dated_awards": one(con, "SELECT count(*) FROM bt WHERE future_dated")[0],
        "null_pop_members": one(con, "SELECT count(*) FROM bt WHERE NOT future_dated")[0],
        "by_psc": rows(con, """
            SELECT psc, count(*) AS awards,
                   count(*) FILTER (WHERE future_dated) AS future_dated
            FROM bt GROUP BY 1 ORDER BY awards DESC"""),
    }

    # ── cohort attachments via manifest (primary scalar key) ───────────
    # distinct resource_ids reachable from cohort awards; distinct awards that
    # have >=1 manifest attachment. Join on contract_award_unique_key.
    log("step1b: manifest join")
    con.execute("""
        CREATE TEMP TABLE cohort_res AS
        SELECT DISTINCT m.resource_id
        FROM man m JOIN bt ON m.contract_award_unique_key = bt.k
        WHERE m.resource_id IS NOT NULL
    """)
    out["step1_manifest"] = {
        "awards_with_manifest_attachment": one(con, """
            SELECT count(DISTINCT bt.k) FROM bt
            JOIN man m ON m.contract_award_unique_key = bt.k
            WHERE m.resource_id IS NOT NULL""")[0],
        "distinct_manifest_resource_ids": one(con, "SELECT count(*) FROM cohort_res")[0],
        "manifest_rows_for_cohort": one(con, """
            SELECT count(*) FROM man m JOIN bt ON m.contract_award_unique_key = bt.k""")[0],
        # public + non-empty gate (the download eligibility gate)
        "public_eligible_resource_ids": one(con, """
            SELECT count(DISTINCT m.resource_id)
            FROM man m JOIN bt ON m.contract_award_unique_key = bt.k
            WHERE lower(coalesce(m.access_level,''))='public'
              AND m.file_name IS NOT NULL AND coalesce(m.size_bytes,0) >= 1""")[0],
    }
    # sensitivity: include award_keys[] list matches (winner notice -> many awards)
    out["step1_manifest"]["distinct_resource_ids_incl_award_keys"] = one(con, f"""
        SELECT count(DISTINCT m.resource_id)
        FROM man m
        WHERE m.resource_id IS NOT NULL AND (
            m.contract_award_unique_key IN (SELECT k FROM bt)
            OR EXISTS (SELECT 1 FROM unnest(m.award_keys) AS t(ak)
                       WHERE ak IN (SELECT k FROM bt)))""")[0]

    # ── STEP 1/2 — successfully downloaded files in cohort ─────────────
    log("step2: downloaded files")
    con.execute("""
        CREATE TEMP TABLE dl AS
        SELECT DISTINCT f.resource_id,
               coalesce(f.content_length, f.size_downloaded, 0) AS bytes,
               f.mime_sniffed
        FROM fl f JOIN cohort_res c ON f.resource_id = c.resource_id
        WHERE f.status = 'downloaded'
    """)
    dl_stats = one(con, """
        SELECT count(*) AS files,
               coalesce(sum(bytes),0) AS total_bytes,
               coalesce(avg(bytes),0) AS avg_bytes,
               coalesce(max(bytes),0) AS max_bytes,
               count(*) FILTER (WHERE bytes=0) AS zero_byte_files
        FROM dl""")
    out["step2_downloaded"] = {
        "distinct_downloaded_files": dl_stats[0],
        "total_true_bytes": int(dl_stats[1]),
        "total_gb": round(dl_stats[1] / 1e9, 4),
        "avg_bytes_per_file": round(dl_stats[2], 1),
        "max_bytes": int(dl_stats[3]),
        "zero_or_null_byte_files": dl_stats[4],
        "awards_with_downloaded_attachment": one(con, """
            SELECT count(DISTINCT bt.k)
            FROM bt
            JOIN man m ON m.contract_award_unique_key = bt.k
            JOIN fl f ON f.resource_id = m.resource_id AND f.status='downloaded'""")[0],
        "mime_breakdown": rows(con, """
            SELECT coalesce(mime_sniffed,'<null>') AS mime,
                   count(*) AS files, coalesce(sum(bytes),0) AS bytes
            FROM dl GROUP BY 1 ORDER BY files DESC LIMIT 15"""),
    }

    # ── STEP 2 — MEASURED chunk + char volume from extraction ledger ───
    # dedup to latest chunk-bearing event per resource_id (n_chunks not null)
    log("step3: extraction volume")
    con.execute("""
        CREATE TEMP TABLE ex_latest AS
        WITH ranked AS (
            SELECT e.resource_id, e.state, e.n_chunks, e.text_chars, e.text_yield_ratio,
                   row_number() OVER (PARTITION BY e.resource_id
                       ORDER BY (e.n_chunks IS NOT NULL) DESC, e.completed_at DESC) AS rn
            FROM ex e JOIN dl ON e.resource_id = dl.resource_id
        )
        SELECT * EXCLUDE(rn) FROM ranked WHERE rn = 1
    """)
    # chunk-bearing = the files whose chunks feed the LLM lane
    cb = one(con, """
        SELECT count(*) FILTER (WHERE n_chunks IS NOT NULL AND n_chunks > 0) AS chunk_files,
               coalesce(sum(n_chunks),0) AS total_chunks,
               coalesce(sum(text_chars),0) AS total_text_chars,
               coalesce(avg(n_chunks) FILTER (WHERE n_chunks>0),0) AS avg_chunks_per_file,
               coalesce(max(n_chunks),0) AS max_chunks,
               coalesce(avg(text_chars) FILTER (WHERE n_chunks>0),0) AS avg_chars_per_file
        FROM ex_latest""")
    out["step2_extraction"] = {
        "downloaded_files_in_cohort": one(con, "SELECT count(*) FROM dl")[0],
        "files_with_extraction_event": one(con, "SELECT count(*) FROM ex_latest")[0],
        "chunk_bearing_files": cb[0],
        "total_chunks_measured": int(cb[1]),
        "total_extracted_text_chars": int(cb[2]),
        "avg_chunks_per_chunk_file": round(cb[3], 2),
        "max_chunks_single_file": int(cb[4]),
        "avg_text_chars_per_chunk_file": round(cb[5], 1),
        "state_distribution": rows(con, """
            SELECT coalesce(state,'<no_event>') AS state,
                   count(*) AS files,
                   coalesce(sum(n_chunks),0) AS chunks
            FROM ex_latest GROUP BY 1 ORDER BY files DESC"""),
        # downloaded-but-no-extraction-event (need estimation if >0)
        "downloaded_without_extraction_event": one(con, """
            SELECT count(*) FROM dl
            WHERE resource_id NOT IN (SELECT resource_id FROM ex_latest)""")[0],
    }

    # ── derived char/token payload inputs (exact, from measured volumes) ─
    total_chunks = int(cb[1])
    total_text_chars = int(cb[2])
    chunk_files = int(cb[0])
    # chunk payload chars = extracted text + re-included overlap per extra chunk
    chunk_payload_chars = total_text_chars + max(0, total_chunks - chunk_files) * CHUNK_OVERLAP
    out["derived_payload"] = {
        "chunk_files": chunk_files,
        "total_chunks": total_chunks,
        "total_extracted_text_chars": total_text_chars,
        "chunk_payload_chars": int(chunk_payload_chars),
        "fixed_overhead_bytes_per_file": 12142,   # prompt_template + vocabulary + output_schema
        "note": "chunk_payload_chars = text_chars + (chunks-files)*overlap; LLM input/file = "
                "fixed_overhead(12142B)+~scaffold + that file's chunk payload",
    }

    json.dump(out, sys.stdout, indent=2, default=str)
    print("\nDONE", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
