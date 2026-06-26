"""Unit tests for the phase_finalize row-address dedup core (`_duplicate_rowids`).

Pure-function coverage — no R2/Lance. Proves the keep-first contract the Phase-0 patch
relies on: given (chunk_id, _rowid) pairs in scan order, exactly the rowids of every
duplicate occurrence are returned (first occurrence in scan order is kept), so the
`delete("_rowid IN (...)")` path removes only crash-window duplicates and never a
chunk_id's sole surviving row.

    python -m pytest pipelines/sam_gov/tests/test_sam_attachment_finalize_dedup.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

# repo root = .../pipelines/sam_gov/tests/this_file → parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.sam_gov.sam_attachment_extract import _duplicate_rowids  # noqa: E402


def test_empty_input_yields_no_deletes():
    assert _duplicate_rowids([], []) == []


def test_no_duplicates_yields_no_deletes():
    assert _duplicate_rowids(["a", "b", "c"], [0, 1, 2]) == []


def test_keep_first_occurrence_in_scan_order():
    # 'a' at rowids 0/2/5, 'b' at 1/4 — only the later occurrences are deleted.
    cids = ["a", "b", "a", "c", "b", "a"]
    rids = [0, 1, 2, 3, 4, 5]
    assert _duplicate_rowids(cids, rids) == [2, 4, 5]


def test_scan_order_governs_not_rowid_magnitude():
    # Row addresses need not be monotone with scan order (post-delete fragments). The FIRST pair
    # seen wins even when its _rowid is numerically larger.
    cids = ["x", "x"]
    rids = [900, 5]
    assert _duplicate_rowids(cids, rids) == [5]


def test_all_rows_duplicate_of_one_id_keeps_exactly_one():
    cids = ["k"] * 5
    rids = [10, 11, 12, 13, 14]
    assert _duplicate_rowids(cids, rids) == [11, 12, 13, 14]


def test_interleaved_duplicates_across_many_ids():
    cids = ["a", "b", "a", "b", "c", "a", "c"]
    rids = [0, 1, 2, 3, 4, 5, 6]
    out = _duplicate_rowids(cids, rids)
    assert out == [2, 3, 5, 6]
    # survivor set is exactly one rowid per distinct chunk_id
    survivors = set(rids) - set(out)
    assert survivors == {0, 1, 4}


def test_deletion_count_matches_excess_occurrences():
    cids = ["a"] * 3 + ["b"] * 2 + ["c"]
    rids = list(range(6))
    out = _duplicate_rowids(cids, rids)
    assert len(out) == (3 - 1) + (2 - 1) + (1 - 1)
