#!/usr/bin/env python3
"""Local test harness for FAR solicitation extraction pipeline.

Run without Modal or Remote infrastructure:
  uv run --with duckdb --with lancedb --with pyarrow --with anthropic \
    python scripts/far_extract_local_test.py [--dry-run]
"""

import os
import sys
import json
import datetime as dt
from dataclasses import dataclass, asdict
from typing import Optional

# Minimal imports for local testing
import duckdb


@dataclass
class ExtractionResult:
    """Single solicitation extraction result."""
    notice_id: str
    solicitation_number: Optional[str]
    title: Optional[str]
    response_deadline: Optional[str]
    setaside_goal_8a: Optional[float]
    setaside_goal_hubzone: Optional[float]
    setaside_goal_sdvosb: Optional[float]
    setaside_goal_veteran: Optional[float]
    setaside_goal_wosb: Optional[float]
    extract_confidence_score: float
    extraction_raw_text: Optional[str]
    extracted_at: str
    extraction_version: str


def test_query_open_solicitations(limit: int = 5, dry_run: bool = False) -> None:
    """Test querying open SAM opportunities without LLM."""
    print("Testing SAM opportunities query...")

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4;")

    # Test if we can read the Lance dataset
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

    try:
        arrow_table = con.execute(query_sql).fetch_arrow_table()
        rows = arrow_table.to_pylist()
        print(f"✓ Query succeeded: found {len(rows)} open solicitations")

        for i, row in enumerate(rows):
            print(f"\n[{i+1}] {row['notice_id']}")
            print(f"    Title: {row['title'][:60]}...")
            print(f"    Deadline: {row['response_deadline']}")
            desc_len = len(row['description']) if row['description'] else 0
            print(f"    Description length: {desc_len} chars")

            if not dry_run:
                # Show first 200 chars of description
                if row['description']:
                    print(f"    Text: {row['description'][:200]}...")

    except Exception as e:
        print(f"✗ Query failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        con.close()


def test_anthropic_extraction() -> None:
    """Test LLM extraction with a sample solicitation text."""
    print("\nTesting LLM extraction...")

    try:
        from anthropic import Anthropic
    except ImportError:
        print("✗ anthropic not installed; skipping LLM test")
        return

    # Sample FAR Section L/M text (real example)
    sample_text = """
    SECTION L – INSTRUCTIONS, CONDITIONS, AND NOTICES TO OFFERORS OR QUOTERS
    ...
    SECTION M – EVALUATION FACTORS FOR AWARD

    Subcontracting Plan: Required. The offeror must submit a subcontracting plan in
    accordance with FAR 19.701-3. The subcontracting goals for this acquisition are:
    - 8(a): 15%
    - HUBZone: 10%
    - SDVOSB: 5%
    - Veteran-Owned Small Business: 8%

    These goals are aspirational targets for the contractor's performance.
    """

    client = Anthropic()
    prompt = f"""Extract subcontracting setaside goals from this FAR solicitation text.

SOLICITATION TEXT:
{sample_text[:4000]}

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

    try:
        message = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )

        response_text = message.content[0].text.strip()
        extracted = json.loads(response_text)
        print(f"✓ LLM extraction succeeded:")
        print(f"  8(a): {extracted.get('setaside_goal_8a')}")
        print(f"  HUBZone: {extracted.get('setaside_goal_hubzone')}")
        print(f"  SDVOSB: {extracted.get('setaside_goal_sdvosb')}")
        print(f"  Veteran: {extracted.get('setaside_goal_veteran')}")
        print(f"  Confidence: {extracted.get('confidence')}")

    except Exception as e:
        print(f"✗ LLM extraction failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv

    print("FAR Solicitation Extraction — Local Test Harness")
    print("=" * 60)

    test_query_open_solicitations(limit=3, dry_run=dry_run)
    test_anthropic_extraction()

    print("\n" + "=" * 60)
    print("Tests completed")
