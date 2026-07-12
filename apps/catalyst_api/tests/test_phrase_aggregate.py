"""phrase-agg.v1 acceptance — the aggregate mode's pinned examples.

Compilation is hermetic (no sidecar); execution tests monkeypatch the sidecar
seam. The flagship phrase is pinned verbatim: it is the demo's first click.
"""
from __future__ import annotations

import pytest

from apps.catalyst_api.src import phrase_aggregate, phrase_compiler
from apps.catalyst_api.src.lance_store import MapCompileError


# ── mode routing ───────────────────────────────────────────────────────────────
def test_total_opener_routes_to_aggregate_mode():
    assert phrase_aggregate.is_aggregate_phrase("total awarded by industry fy24")
    assert not phrase_aggregate.is_aggregate_phrase("companies in dsbs")
    assert not phrase_aggregate.is_aggregate_phrase("")
    assert not phrase_aggregate.is_aggregate_phrase(None)


# ── the flagship (the demo's click 1) ─────────────────────────────────────────
def test_flagship_total_awarded_by_industry_fy23_to_fy25():
    c = phrase_aggregate.compile_aggregate("total awarded by industry fy23 to fy25")
    assert c["spec"] == {"measure": "prime_obl_sum", "group_by": "industry",
                         "fy_lo": 2023, "fy_hi": 2025,
                         "active": False, "zip": None, "radius_mi": None}
    axes = [b["axis"] for b in c["bindings"]]
    assert axes == ["mode", "measure", "group_by", "window"]


def test_spellings_bind_identically():
    for phrase in ("total obligated across industries fy23 to fy25",
                   "total award value by industry from fy2023 to fy2025",
                   "total spend by industry in fy23 to fy25"):
        c = phrase_aggregate.compile_aggregate(phrase)
        assert c["spec"]["fy_lo"] == 2023 and c["spec"]["fy_hi"] == 2025
        assert c["spec"]["measure"] == "prime_obl_sum"


def test_single_year_window():
    c = phrase_aggregate.compile_aggregate("total awarded by industry fy24")
    assert c["spec"]["fy_lo"] == c["spec"]["fy_hi"] == 2024


# ── designed refusals (every one names the fix) ────────────────────────────────
@pytest.mark.parametrize("phrase,match", [
    ("total awarded by industry", "no fiscal window"),
    ("total by industry fy24", "no measure"),
    ("total awarded fy24", "no group axis"),
    ("total awarded by industry fy25 to fy23", "reversed"),
    ("total awarded by industry fy23 to fy25 fy24", "more than one fiscal"),
    ("total profits by industry fy24", "'profits'"),
    ("total awarded by state fy24", "'by state'|'state'"),
])
def test_refusals_name_the_token_or_fix(phrase, match):
    with pytest.raises(MapCompileError, match=match):
        phrase_aggregate.compile_aggregate(phrase)


def test_retrieval_grammar_untouched():
    # 'total' never reaches the retrieval lexer; retrieval phrases never reach
    # the aggregate lexer. The retrieval compiler still refuses 'total' as an
    # unbound token if called directly.
    with pytest.raises(MapCompileError, match="'total'"):
        phrase_compiler.compile_phrase("companies total awarded")


# ── execution (sidecar seam monkeypatched) ─────────────────────────────────────
def test_execute_rolls_sectors_and_sorts(monkeypatch):
    monkeypatch.setattr(phrase_aggregate.sidecar_executor, "enabled", lambda: True)
    monkeypatch.setattr(
        phrase_aggregate.sidecar_executor, "_sql",
        lambda sql, limit: {
            "columns": ["sector2", "obl", "actions"],
            "rows": [["31", 10.0, 5], ["33", 30.0, 7], ["23", 25.0, 3],
                     ["99", 99.0, 1]],       # 99 = non-sector residue, dropped
            "artifact": "test-artifact", "elapsed_ms": 1.0})
    phrase_aggregate._CACHE.clear()
    out = phrase_aggregate.compile_and_execute(
        {"phrase": "total awarded by industry fy23 to fy25"})
    bars = out["data"]["bars"]
    assert [b["key"] for b in bars] == ["31-33", "23"]       # merged + Σ$-sorted
    assert bars[0]["total"] == 40.0 and bars[0]["count"] == 12
    assert bars[0]["label"] == "Manufacturing"
    assert out["meta"]["mode"] == "aggregate"
    assert out["meta"]["artifact"] == "test-artifact"
    assert out["meta"]["plan"][0]["fy"] == [2023, 2025]


def test_route_delegation_via_compile_and_execute(monkeypatch):
    monkeypatch.setattr(phrase_aggregate.sidecar_executor, "enabled", lambda: True)
    monkeypatch.setattr(
        phrase_aggregate.sidecar_executor, "_sql",
        lambda sql, limit: {"columns": ["sector2", "obl", "actions"],
                            "rows": [["23", 1.0, 1]],
                            "artifact": "a", "elapsed_ms": 1.0})
    phrase_aggregate._CACHE.clear()
    out = phrase_compiler.compile_and_execute(
        {"phrase": "total awarded by industry fy24"})
    assert out["meta"]["compilerVersion"] == phrase_aggregate.AGG_COMPILER_VERSION


def test_cache_serves_second_call_without_sql(monkeypatch):
    monkeypatch.setattr(phrase_aggregate.sidecar_executor, "enabled", lambda: True)
    calls = []
    monkeypatch.setattr(
        phrase_aggregate.sidecar_executor, "_sql",
        lambda sql, limit: (calls.append(1),
                            {"columns": ["sector2", "obl", "actions"],
                             "rows": [["23", 1.0, 1]],
                             "artifact": "a", "elapsed_ms": 1.0})[1])
    phrase_aggregate._CACHE.clear()
    body = {"phrase": "total awarded by industry fy24"}
    phrase_aggregate.compile_and_execute(body)
    phrase_aggregate.compile_and_execute(body)
    assert len(calls) == 1


# ── v2 · the yard production (the demo's click 2) ─────────────────────────────
def test_flagship_active_equipment_near_zip():
    c = phrase_aggregate.compile_aggregate(
        "total active awards near 79925 within 50 miles by equipment")
    assert c["spec"]["active"] is True
    assert c["spec"]["zip"] == "79925"
    assert c["spec"]["radius_mi"] == 50.0
    assert c["spec"]["group_by"] == "equipment"
    assert c["spec"]["fy_lo"] is None
    axes = [b["axis"] for b in c["bindings"]]
    assert axes == ["mode", "scope", "measure", "anchor", "radius", "group_by"]


@pytest.mark.parametrize("phrase,match", [
    # active-mode omissions each name the fix
    ("total active awards within 50 miles by equipment", "near <zip5>"),
    ("total active awards near 79925 by equipment", "within 50 miles"),
    ("total active awards near 79925 within 50 miles by industry",
     "'by equipment' only"),
    ("total active awards near 79925 within 50 miles by equipment fy24",
     "point-in-time"),
    # v2 vocabulary without active scope refuses back to the v1 form
    ("total awards near 79925 within 50 miles by equipment fy24", "active scope"),
    ("total awarded by equipment fy24", "active scope"),
    # malformed anchor / radius
    ("total active awards near elpaso within 50 miles by equipment", "5-digit zip"),
    ("total active awards near 79925 within fifty miles by equipment", "<N> miles"),
    ("total active awards near 79925 within 900 miles by equipment", "outside"),
])
def test_v2_refusals_name_the_token_or_fix(phrase, match):
    with pytest.raises(MapCompileError, match=match):
        phrase_aggregate.compile_aggregate(phrase)


def test_v2_execute_anchors_and_buckets(monkeypatch):
    monkeypatch.setattr(phrase_aggregate.sidecar_executor, "enabled", lambda: True)
    seen = []

    def fake_sql(sql, limit):
        seen.append(sql)
        if "usaspending_award_pop_centroids" in sql:
            return {"columns": ["pops", "lat", "lon"],
                    "rows": [[42, 31.77, -106.32]],
                    "artifact": "test-artifact", "elapsed_ms": 1.0}
        return {"columns": ["bucket", "awards", "obl"],
                "rows": [["earthmoving_and_excavation", 7, 25.5e6],
                         ["paving_and_roadwork", 12, 60.0e6],
                         [None, 3, 1.0e6]],       # unbucketed residue dropped
                "artifact": "test-artifact", "elapsed_ms": 2.0}

    monkeypatch.setattr(phrase_aggregate.sidecar_executor, "_sql", fake_sql)
    phrase_aggregate._CACHE.clear()
    out = phrase_aggregate.compile_and_execute(
        {"phrase": "total active awards near 79925 within 50 miles by equipment"})
    assert len(seen) == 2
    assert "zip5 = '79925'" in seen[0]
    assert "gtm_open_awards" in seen[1] and "in_scope" in seen[1]
    bars = out["data"]["bars"]
    assert [b["key"] for b in bars] == ["paving_and_roadwork",
                                        "earthmoving_and_excavation"]
    assert bars[1]["label"] == "Earthmoving & Excavation"
    assert out["meta"]["matchedRows"] == 19
    assert out["meta"]["plan"][0]["anchor"]["zip"] == "79925"
    assert out["meta"]["plan"][0]["fy"] is None
    assert out["meta"]["title"] == "Active equipment-scope awards · 79925 · 50 mi"


def test_v2_unknown_zip_refuses(monkeypatch):
    monkeypatch.setattr(phrase_aggregate.sidecar_executor, "enabled", lambda: True)
    monkeypatch.setattr(
        phrase_aggregate.sidecar_executor, "_sql",
        lambda sql, limit: {"columns": ["pops", "lat", "lon"],
                            "rows": [[0, None, None]],
                            "artifact": "a", "elapsed_ms": 1.0})
    phrase_aggregate._CACHE.clear()
    with pytest.raises(MapCompileError, match="00000"):
        phrase_aggregate.compile_and_execute(
            {"phrase": "total active awards near 00000 within 50 miles by equipment"})
