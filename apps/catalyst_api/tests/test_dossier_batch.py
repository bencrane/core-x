"""Hermetic tests for the batch dossier unit (apps/catalyst_api/src/dossier.py) —
no R2, no FastAPI. Pins the batch contract: bound enforcement + order-preserving
dedupe (prepare_batch), partial success (invalid-format / unknown UEI → None, never
a raise), and payload PARITY with the single route's composition (both terminate in
``EntityDossierResponse.from_parts``)."""

from __future__ import annotations

from datetime import date

import pytest

from apps.catalyst_api.src import dossier, lance_store
from apps.catalyst_api.src.models import EntityDossierResponse

TODAY = date(2026, 6, 12)

GOLD = {
    "uei": "NPFUUDHHMJY1", "cage_code": "53LJ9", "is_active": True,
    "has_federal_awards": True, "legal_business_name": "CECIL & CECIL ENTERPRISES INC",
    "primary_naics": "237990", "physical_address_line_1": "3741 BUSINESS DR",
    "physical_address_city": "SACRAMENTO", "physical_address_state": "CA",
    "physical_address_zip_postal_code": "95820", "total_active_obligations": 2826014.57,
    "total_lifetime_obligations": 10264985.68, "award_count": 23, "active_award_count": 1,
    "profile_as_of_date": date(2026, 6, 11), "pocs": [],
}
CAS = {"prime_most_recent_action_date": date(2026, 5, 28),
       "top_agency_1_name": "HHS", "top_agency_1_dollars": 8185278.85}
ACTIONS = [{"award_id": "A1", "action_date": date(2026, 5, 28), "action_obligated_usd": 271419.0,
            "awarding_agency": "HHS", "awarding_sub_agency": "IHS",
            "winner_type": "prime_recipient", "pop_state": "CA", "pop_city": "SACRAMENTO",
            "set_aside": None, "naics_code": "237110"}]


@pytest.fixture()
def lookups(monkeypatch):
    """Known universe: NPFUUDHHMJY1 exists; everything else is absent from gold."""
    calls = {"actions_limit": None}
    monkeypatch.setattr(lance_store, "entity_dossier_gold",
                        lambda uei: dict(GOLD) if uei == "NPFUUDHHMJY1" else None)
    monkeypatch.setattr(lance_store, "award_summary_by_uei", lambda uei: dict(CAS))

    def fake_actions(uei, limit):
        calls["actions_limit"] = limit
        return [dict(a) for a in ACTIONS]

    monkeypatch.setattr(lance_store, "recent_award_actions_by_uei", fake_actions)
    return calls


# ── prepare_batch: bound + order-preserving dedupe ────────────────────────────
def test_prepare_batch_dedupes_preserving_order():
    out, err = dossier.prepare_batch([" B ", "A", "B", "", "C", "A"])
    assert err is None and out == ["B", "A", "C"]


def test_prepare_batch_enforces_bound():
    out, err = dossier.prepare_batch([f"U{i:011d}" for i in range(dossier.BATCH_MAX_UEIS + 1)])
    assert out is None and "at most" in err


def test_prepare_batch_rejects_empty_and_non_list():
    for bad in ([], ["", "  "], "NPFUUDHHMJY1", None):
        out, err = dossier.prepare_batch(bad)
        assert out is None and err


# ── compose_dossier: partial success ──────────────────────────────────────────
def test_unknown_uei_composes_to_none_not_raise(lookups):
    assert dossier.compose_dossier("ZZZZZZZZZZZZ", today=TODAY) is None


def test_invalid_format_uei_composes_to_none_without_lookup(lookups, monkeypatch):
    def explode(uei):
        raise AssertionError("lookup must not run for an invalid-format uei")

    monkeypatch.setattr(lance_store, "entity_dossier_gold", explode)
    assert dossier.compose_dossier("not-a-uei", today=TODAY) is None


def test_actions_knob_is_clamped(lookups):
    dossier.compose_dossier("NPFUUDHHMJY1", actions=999, today=TODAY)
    assert lookups["actions_limit"] == dossier.ACTIONS_MAX
    dossier.compose_dossier("NPFUUDHHMJY1", actions=0, today=TODAY)
    assert lookups["actions_limit"] == 1


# ── payload parity with the single route's composition ───────────────────────
def test_batch_composition_matches_single_route_parts(lookups):
    via_batch = dossier.compose_dossier("NPFUUDHHMJY1", actions=10, today=TODAY)
    via_single = EntityDossierResponse.from_parts(dict(GOLD), dict(CAS),
                                                  [dict(a) for a in ACTIONS], TODAY)
    assert via_batch is not None
    assert via_batch.model_dump(by_alias=True) == via_single.model_dump(by_alias=True)
