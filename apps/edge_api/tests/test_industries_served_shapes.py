"""industries_served projection — both wire shapes land losslessly (pure, no DB).

Shape A (original): {industriesServed[], sources[], stepsTaken[], confidence, reasoning}
Shape B (Claygent capital run, 2026-07-19): {industries[], source: "url; url", stepsTaken[],
confidence, reasoning, companyName, domain} — landed with a caller-supplied source tag.
"""
from __future__ import annotations

from apps.edge_api.src.routers.industries_served_v1 import _to_row

_COL = {  # name → tuple index, mirrors _COLS in the router
    "record_id": 0, "company_domain": 1, "domain_norm": 2, "confidence": 3, "reasoning": 4,
    "sources": 5, "steps_taken": 6, "industries_served": 7, "industries_served_count": 8,
    "source": 9, "raw_payload": 10,
}

SHAPE_A = {
    "confidence": "high",
    "reasoning": "site lists verticals",
    "sources": ["https://togglerentals.com/industries"],
    "stepsTaken": ["Visited https://togglerentals.com/"],
    "industriesServed": ["construction", "events"],
}

SHAPE_B = {
    "domain": "v2mc.com",
    "source": "https://www.v2mc.com/ (Our Business section); https://www.v2mc.com/our-team (Our Team)",
    "reasoning": "homepage names target sectors",
    "confidence": "high",
    "industries": ["health care", "housing", "education"],
    "stepsTaken": ["Visited https://www.v2mc.com/", "Visited https://www.v2mc.com/our-team"],
    "companyName": "V2 Municipal Capital",
}


def test_shape_a_projects_as_before() -> None:
    row = _to_row("togglerentals.com", SHAPE_A)
    assert row is not None
    assert row[_COL["industries_served"]].obj == ["construction", "events"]
    assert row[_COL["industries_served_count"]] == 2
    assert row[_COL["sources"]].obj == ["https://togglerentals.com/industries"]
    assert row[_COL["source"]] == "industries_served"


def test_shape_b_claygent_capital_projects() -> None:
    row = _to_row("v2mc.com", SHAPE_B, source="capital_providers")
    assert row is not None
    assert row[_COL["industries_served"]].obj == ["health care", "housing", "education"]
    assert row[_COL["industries_served_count"]] == 3
    # "; "-joined source string splits into the sources projection
    assert row[_COL["sources"]].obj == [
        "https://www.v2mc.com/ (Our Business section)",
        "https://www.v2mc.com/our-team (Our Team)",
    ]
    assert row[_COL["steps_taken"]].obj == SHAPE_B["stepsTaken"]
    assert row[_COL["source"]] == "capital_providers"
    # raw payload is verbatim — companyName and the unsplit source string survive in full
    assert row[_COL["raw_payload"]].obj["companyName"] == "V2 Municipal Capital"
    assert row[_COL["raw_payload"]].obj["source"] == SHAPE_B["source"]


def test_shape_b_distinct_payloads_distinct_record_ids() -> None:
    a = _to_row("v2mc.com", SHAPE_B, source="capital_providers")
    b = _to_row("v2mc.com", {**SHAPE_B, "industries": ["health care"]}, source="capital_providers")
    assert a[_COL["record_id"]] != b[_COL["record_id"]]
    # byte-identical resend → same record_id (idempotency key), regardless of source tag
    c = _to_row("v2mc.com", SHAPE_B)
    assert a[_COL["record_id"]] == c[_COL["record_id"]]
