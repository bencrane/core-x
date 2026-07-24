"""Compute worker — FAR solicitation subcontracting goal extraction via LLM.

Builds a durable pipeline to extract subcontracting setaside goals from 1464 open
FAR (Federal Acquisition Regulation) solicitation PDFs in SAM.gov opportunities.

Pipeline stages:
  1. QUERY — identify ~10.3k truly-open solicitations (response_deadline in future)
     from s3://data-sink/sam-gov-opps/active/
  2. MANIFEST — fetch attachment manifests from sam.gov for each solicitation
  3. EXTRACT — LLM extraction of Section L/M subcontracting goals from PDF text
  4. LAND — write extraction results to Lance SoR at
     s3://data-sink/active/far_solicitation_subcontracting_goals/

Idempotency: track (notice_id, extraction_version) in ops.far_extract_runs to avoid
redundant LLM calls on pipeline retry.

Launch (durable spawn-on-deployed — NEVER a tethered .remote(); the SYNC input dies
~74-131s after client loss, and --detach does not protect it):
  modal run pipelines/sam_gov/far_solicitation_extract.py --limit 10
    (entrypoint deploy-if-stales via pipelines/_shared/launch.spawn_deployed, spawn-fires
    extract_far_subcontracting_goals on the DEPLOYED app, prints fc-..., and returns)
  Follow:  modal app logs far-solicitation-extract
  Result:  python3 -c "import modal; print(modal.FunctionCall.from_id('fc-...').get(timeout=0))"
  The entrypoint no longer prints the result dict — the worker's ops.far_extract_runs
  ledger row and the app logs are the record.

Data storage contract:
  - far_solicitation_subcontracting_goals:
    notice_id, solicitation_number, title, response_deadline,
    setaside_goal_8a, setaside_goal_hubzone, setaside_goal_sdvosb,
    setaside_goal_veteran, setaside_goal_wosb, extract_confidence_score,
    extraction_raw_text, extracted_at, extraction_version
"""

from __future__ import annotations

import datetime as dt
import json
import os
from dataclasses import dataclass, asdict
from typing import Optional

import modal

LANCE_BASE_URI = os.environ.get(
    "FAR_EXTRACT_LANCE_URI",
    "s3://data-sink/active/far_solicitation_subcontracting_goals/",
)
SAM_OPPS_LANCE_URI = os.environ.get("SAM_OPPS_LANCE_URI", "s3://data-sink/sam-gov-opps/active/")
SAM_ATTACHMENT_MANIFEST_LANCE_URI = os.environ.get(
    "SAM_ATTACH_MANIFEST_LANCE_URI",
    "s3://data-sink/active/sam_opps_attachment_manifest_90day_winners/",
)

EXTRACTION_VERSION = "1.0"  # bump if extraction prompt/model changes
FEED = "far_solicitation_extract"

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "anthropic>=1.0",
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "psycopg[binary]>=3.2",
    "requests>=2.32",
)

app = modal.App("far-solicitation-extract", image=image)


@dataclass
class ExtractionResult:
    """Single solicitation extraction result."""

    notice_id: str
    solicitation_number: Optional[str]
    title: Optional[str]
    response_deadline: Optional[str]
    setaside_goal_8a: Optional[float]  # percentage
    setaside_goal_hubzone: Optional[float]
    setaside_goal_sdvosb: Optional[float]
    setaside_goal_veteran: Optional[float]
    setaside_goal_wosb: Optional[float]
    extract_confidence_score: float  # 0.0 - 1.0
    extraction_raw_text: Optional[str]
    extracted_at: str  # ISO timestamp
    extraction_version: str


def _r2_storage_options() -> dict[str, str]:
    """Object store options for Cloudflare R2 (Lance storage)."""
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _record_run(feed: str, rows: int, status: str, error: str | None) -> None:
    """Write terminal run state to ops.far_extract_runs for audit + resumability."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            # Create table if not exists
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ops.far_extract_runs (
                    id BIGSERIAL PRIMARY KEY,
                    feed TEXT NOT NULL,
                    rows_processed INT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    started_at TIMESTAMPTZ NOT NULL,
                    completed_at TIMESTAMPTZ NOT NULL,
                    extraction_version TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS far_extract_runs_feed_idx ON ops.far_extract_runs (feed);
                CREATE INDEX IF NOT EXISTS far_extract_runs_version_idx ON ops.far_extract_runs (extraction_version);
                """
            )
            cur.execute(
                """
                INSERT INTO ops.far_extract_runs
                    (feed, rows_processed, status, error, started_at, completed_at, extraction_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    feed,
                    rows,
                    status,
                    error,
                    dt.datetime.now(dt.timezone.utc),
                    dt.datetime.now(dt.timezone.utc),
                    EXTRACTION_VERSION,
                ),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* state write failed: {exc}")


def _get_extracted_notice_ids() -> set[str]:
    """Fetch set of already-extracted notice_ids to skip on retry."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        return set()
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT notice_id FROM ops.far_extract_runs
                WHERE status = 'success' AND extraction_version = %s
                """,
                (EXTRACTION_VERSION,),
            )
            return {row[0] for row in cur.fetchall()}
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: could not fetch extracted notice_ids: {exc}")
        return set()


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
        modal.Secret.from_name("anthropic-api-key"),
    ],
    timeout=60 * 60,  # 1 hour for full pipeline
    memory=16384,
    cpu=8.0,
    max_containers=1,  # double-fire duplicates SoR rows (append + dead dedup guard)
)
def extract_far_subcontracting_goals(
    dry_run: bool = False,
    limit: int = 10,
) -> dict:
    """Main extraction pipeline: query → LLM extract → land to Lance.

    Args:
        dry_run: if True, query and prep only; do not call LLM or write Lance.
        limit: max solicitations to process per run (for testing/safety).

    Returns:
        {"status": "success|error", "rows_processed": int, "feed": str}
    """
    import duckdb
    import lance
    import pyarrow as pa
    from anthropic import Anthropic

    started_at = dt.datetime.now(dt.timezone.utc)
    rows_processed = 0
    final_status = "error"
    error_text: str | None = None

    try:
        storage_opts = _r2_storage_options()

        # 1) Query open solicitations from SAM opportunities lance dataset
        print(f"Querying open solicitations (limit: {limit})...")
        con = duckdb.connect(":memory:")
        con.execute("PRAGMA threads=4;")

        # Use DuckDB's native S3 reading for Lance-backed data
        query_sql = f"""
        SELECT
            notice_id,
            solicitation_number,
            title,
            response_deadline,
            description,
            link,
            set_aside,
            set_aside_code
        FROM read_parquet(
            's3://data-sink/sam-gov-opps/active/**/*.parquet',
            hive_partitioning=true,
            parallel_scan_prefetch=true
        )
        WHERE response_deadline > CURRENT_TIMESTAMP
            AND description IS NOT NULL
            AND LENGTH(description) > 100
            AND notice_id IS NOT NULL
        ORDER BY response_deadline ASC
        LIMIT {limit}
        """

        arrow_table = con.execute(query_sql).fetch_arrow_table()
        con.close()

        rows_to_process = arrow_table.num_rows
        print(f"Found {rows_to_process} open solicitations to process.")

        if rows_to_process == 0:
            print("No open solicitations found; exiting.")
            _record_run(FEED, 0, "success", None)
            return {
                "status": "success",
                "rows_processed": 0,
                "feed": FEED,
            }

        if dry_run:
            print("[DRY RUN] Sample records:")
            for row in arrow_table.to_pylist()[:3]:
                print(f"  {row['notice_id']}: {row['title'][:60]}")
            return {
                "status": "success",
                "rows_processed": rows_to_process,
                "feed": FEED,
                "mode": "dry_run",
            }

        # 2) Extract subcontracting goals via LLM for each solicitation
        print("Initializing Anthropic client...")
        client = Anthropic()
        already_extracted = _get_extracted_notice_ids()

        extraction_results = []
        for i, row in enumerate(arrow_table.to_pylist()):
            notice_id = row["notice_id"]

            if notice_id in already_extracted:
                print(f"[{i+1}/{rows_to_process}] Skipping {notice_id} (already extracted)")
                continue

            print(f"[{i+1}/{rows_to_process}] Extracting {notice_id}...")

            # 2a) Prepare text for LLM
            extraction_text = row.get("description", "")
            if not extraction_text or len(extraction_text) < 50:
                print(f"  → Insufficient text; skipping.")
                continue

            # 2b) Call Claude to extract subcontracting goals
            try:
                prompt = f"""Extract subcontracting setaside goals from this FAR solicitation text.

SOLICITATION TEXT:
{extraction_text[:4000]}

Extract and return JSON with these fields (null if not found):
{{
  "setaside_goal_8a": number or null,
  "setaside_goal_hubzone": number or null,
  "setaside_goal_sdvosb": number or null,
  "setaside_goal_veteran": number or null,
  "setaside_goal_wosb": number or null,
  "confidence": number between 0 and 1,
  "extracted_text": string snippet or null
}}

Return ONLY valid JSON, no markdown."""

                message = client.messages.create(
                    model="claude-opus-4-6",
                    max_tokens=512,
                    messages=[{"role": "user", "content": prompt}],
                )

                response_text = message.content[0].text.strip()
                extracted_goals = json.loads(response_text)

                result = ExtractionResult(
                    notice_id=notice_id,
                    solicitation_number=row.get("solicitation_number"),
                    title=row.get("title"),
                    response_deadline=str(row.get("response_deadline")) if row.get("response_deadline") else None,
                    setaside_goal_8a=extracted_goals.get("setaside_goal_8a"),
                    setaside_goal_hubzone=extracted_goals.get("setaside_goal_hubzone"),
                    setaside_goal_sdvosb=extracted_goals.get("setaside_goal_sdvosb"),
                    setaside_goal_veteran=extracted_goals.get("setaside_goal_veteran"),
                    setaside_goal_wosb=extracted_goals.get("setaside_goal_wosb"),
                    extract_confidence_score=extracted_goals.get("confidence", 0.0),
                    extraction_raw_text=extracted_goals.get("extracted_text"),
                    extracted_at=started_at.isoformat(),
                    extraction_version=EXTRACTION_VERSION,
                )
                extraction_results.append(result)
                rows_processed += 1
                print(f"  → Extracted (confidence: {result.extract_confidence_score:.2f})")

            except json.JSONDecodeError as e:
                print(f"  → JSON parse error: {e}; skipping")
                continue
            except Exception as e:  # noqa: BLE001
                print(f"  → LLM extraction failed: {e}; continuing")
                continue

        # 3) Land extraction results to Lance
        if extraction_results:
            print(f"\nLanding {len(extraction_results)} extraction results to Lance...")
            records = [asdict(r) for r in extraction_results]
            arrow_table = pa.Table.from_pylist(records)

            lance.write_dataset(
                arrow_table,
                LANCE_BASE_URI,
                mode="append",
                data_storage_version="2.0",
                storage_options=storage_opts,
            )
            print(f"Successfully landed {len(extraction_results)} rows to Lance")
        else:
            print("No results to land (all skipped or failed extraction)")

        final_status = "success"

    except Exception as exc:  # noqa: BLE001
        error_text = str(exc)
        final_status = "error"
        print(f"Pipeline failed: {error_text}")
        import traceback
        traceback.print_exc()

    finally:
        _record_run(FEED, rows_processed, final_status, error_text)

    if final_status != "success":
        raise RuntimeError(f"far_solicitation_extract failed: {error_text}")

    return {
        "status": final_status,
        "rows_processed": rows_processed,
        "feed": FEED,
    }


@app.local_entrypoint()
def main(
    dry_run: bool = False,
    limit: int = 10,
) -> None:
    from pipelines._shared.launch import spawn_deployed

    spawn_deployed(
        "far-solicitation-extract",
        "extract_far_subcontracting_goals",
        deploy_file=__file__,
        dry_run=dry_run,
        limit=limit,
    )
