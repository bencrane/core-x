"""Unit + deterministic-zombie-path tests for the subaward chunked loader's zombie escape.

Pure-function coverage of `_zombie_budget` / `_split_window`, plus a fully-mocked drive of
`_run_chunks` that proves the poison-day case (2026-05-28, which zombies at every window
width) terminates: a wide window containing it sub-splits down to a 1-day GAP while its
non-poison siblings finish and are counted. No network / R2 / Postgres; runs in milliseconds.

    python -m pytest pipelines/usaspending/tests/test_subaward_zombie_escape.py -q
"""
from __future__ import annotations

import datetime as dt
import sys
import zipfile
from pathlib import Path

import pytest

# repo root = .../pipelines/usaspending/tests/this_file → parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.usaspending import usaspending_api_subaward_fresh as M  # noqa: E402

D = dt.date


def _daysset(x, y):
    return {x + dt.timedelta(days=i) for i in range((y - x).days + 1)}


# ─────────────────────────── _zombie_budget ───────────────────────────

def test_zombie_budget_values():
    assert M._zombie_budget(D(2026, 5, 1), D(2026, 5, 1)) == 20 * 60            # 1 day  = 1200
    assert M._zombie_budget(D(2026, 5, 1), D(2026, 5, 10)) == 20 * 60 + 6 * 60 * 9   # 10 day = 4440
    assert M._zombie_budget(D(2026, 5, 1), D(2026, 5, 15)) == 20 * 60 + 6 * 60 * 14  # 15 day = 6240


def test_zombie_budget_monotonic_in_width():
    prev = -1
    for d in range(1, 40):
        b = M._zombie_budget(D(2026, 5, 1), D(2026, 5, 1) + dt.timedelta(days=d - 1))
        assert b > prev
        prev = b


# ─────────────────────────── _split_window ───────────────────────────

@pytest.mark.parametrize("days", [2, 3, 4, 5, 10, 15, 30])
def test_split_disjoint_contiguous_exact(days):
    ws = D(2026, 5, 1)
    we = ws + dt.timedelta(days=days - 1)
    halves = M._split_window(ws, we)
    assert len(halves) == 2
    (a0, a1), (b0, b1) = halves
    assert a0 == ws and b1 == we                       # endpoints exactly cover the span
    assert b0 == a1 + dt.timedelta(days=1)             # contiguous + disjoint (no overlap, no gap)
    assert a1 >= a0 and b1 >= b0                        # valid ranges
    assert _daysset(a0, a1) | _daysset(b0, b1) == _daysset(ws, we)   # union = full span
    assert _daysset(a0, a1) & _daysset(b0, b1) == set()             # zero overlap
    # both halves are strictly smaller than the parent ⇒ recursion terminates
    assert (a1 - a0).days + 1 < days and (b1 - b0).days + 1 < days


def test_split_window_floor_is_empty():
    assert M._split_window(D(2026, 5, 28), D(2026, 5, 28)) == []


# ─────────────────────────── _run_chunks zombie escape ───────────────────────────

def test_run_chunks_zombie_escape_isolates_poison_day(monkeypatch, tmp_path):
    """A window spanning a poison day (every window containing it zombies) must split down to a
    single 1-day GAP, finish & count the non-poison siblings, never resubmit a signature, and
    terminate."""
    POISON = D(2026, 5, 28)
    fn_to_win: dict[str, tuple] = {}
    submitted: list[tuple] = []

    def fake_submit(ws, we):
        submitted.append((ws, we))
        fn = f"fn::{ws}::{we}::{len(submitted)}"
        fn_to_win[fn] = (ws, we)
        return fn

    def fake_status(fn):
        ws, we = fn_to_win[fn]
        if ws <= POISON <= we:
            return {"status": "running", "total_rows": 0, "seconds_elapsed": 99999}  # zombie
        return {"status": "finished", "total_rows": 10, "seconds_elapsed": 120}

    def fake_download(fn, dest, tries=6):
        with zipfile.ZipFile(dest, "w") as zf:
            zf.writestr("All_Contracts_Subawards_test.csv", "a,b\n")   # header-only, valid member
        return True

    monkeypatch.setattr(M, "_submit", fake_submit)
    monkeypatch.setattr(M, "_status", fake_status)
    monkeypatch.setattr(M, "_download", fake_download)
    monkeypatch.setattr(M.time, "sleep", lambda *a, **k: None)
    monkeypatch.setattr(M, "POLL_SECONDS", 0)

    # [05-26 .. 05-30] spans the poison day; halving →[26-27]ok [28-30]→[28]poison [29-30]ok
    done, polls, gaps = M._run_chunks([(D(2026, 5, 26), D(2026, 5, 30))], str(tmp_path))

    assert gaps == ["[2026-05-28..2026-05-28]"]        # poison isolated to exactly one 1-day gap
    assert done == 2                                    # the two non-poison sibling windows finished
    assert len(submitted) == len(set(submitted))        # no signature ever resubmitted (dedup-safe)
    assert (D(2026, 5, 28), D(2026, 5, 28)) in submitted  # it did drill to the 1-day floor


def test_run_chunks_all_clean_no_gaps(monkeypatch, tmp_path):
    """Sanity: when nothing zombies, every chunk finishes and there are no gaps/splits."""
    fn_to_win: dict[str, tuple] = {}
    submitted: list[tuple] = []

    def fake_submit(ws, we):
        submitted.append((ws, we))
        fn = f"fn::{ws}::{we}"
        fn_to_win[fn] = (ws, we)
        return fn

    def fake_download(fn, dest, tries=6):
        with zipfile.ZipFile(dest, "w") as zf:
            zf.writestr("x_Subawards.csv", "a\n")
        return True

    monkeypatch.setattr(M, "_submit", fake_submit)
    monkeypatch.setattr(M, "_status", lambda fn: {"status": "finished", "total_rows": 5, "seconds_elapsed": 60})
    monkeypatch.setattr(M, "_download", fake_download)
    monkeypatch.setattr(M.time, "sleep", lambda *a, **k: None)
    monkeypatch.setattr(M, "POLL_SECONDS", 0)

    windows = M._chunk_windows(D(2026, 3, 1), D(2026, 3, 20), 10)   # 2 chunks
    done, polls, gaps = M._run_chunks(windows, str(tmp_path))
    assert gaps == []
    assert done == len(windows) == 2
    assert len(submitted) == 2
