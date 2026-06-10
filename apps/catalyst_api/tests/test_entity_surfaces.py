"""Unit tests for the UEI-keyed entity surfaces — pure composition, no R2.

These pin the wire contract (camelCase, ISO dates, DERIVED status/days, the
explicit GAP nulls, the Gold-Mirror count/total headlines) using stub rows shaped
exactly like the live Lance schemas: ``sam_master_entities``, ``sam_pocs``,
``entity_profile_gold``, ``entity_award_lines_gold``, and ``usaspending/award_search``.
"""

from __future__ import annotations

import datetime as dt

from apps.catalyst_api.src import lance_store
from apps.catalyst_api.src.models import (
    ActiveContract,
    OverviewResponse,
    PastPerformanceResponse,
    SamPoc,
    SamProfileResponse,
)

TODAY = dt.date(2026, 6, 8)


# ── UEI filter guard ──────────────────────────────────────────────────────────
def test_valid_uei_guard():
    assert lance_store.valid_uei("WBBSNBA9GBL7")
    assert not lance_store.valid_uei("short")
    assert not lance_store.valid_uei("WBBSNBA9GBL7X")          # 13 chars
    assert not lance_store.valid_uei("evil' OR '1'='1")        # cannot break the Lance filter
    assert not lance_store.valid_uei("WBBS NBA9GBL")           # space


# ── SAM profile composition ────────────────────────────────────────────────────
def test_sam_profile_derives_status_days_and_secondary_naics():
    # Stub shaped like sam_master_entities: is_active flag, registration_expiration_date,
    # initial_registration_date, raw bus_type_string, physical_address_* columns.
    entity = {
        "uei": "WBBSNBA9GBL7", "cage_code": "1ABC2", "legal_business_name": "Rotochopper, Inc",
        "is_active": True,
        "initial_registration_date": dt.date(2019, 1, 1), "activation_date": dt.date(2019, 1, 3),
        "registration_expiration_date": dt.date(2026, 9, 1),
        "primary_naics": "333120", "naics_codes": ["333120", "333111", "423810"],
        "psc_codes": ["3040", "H170"], "bus_type_string": "2X~A6~XS",
        "physical_address_city": "St. Martin", "physical_address_province_or_state": "MN",
        "physical_address_zip_postal_code": "56376",
    }
    pocs = [
        {"poc_type": "electronic_business", "poc_slot_no": 2, "full_name": "Jane Doe",
         "first_name": "Jane", "last_name": "Doe", "title": "EB POC", "city": "St. Martin", "state": "MN"},
    ]
    d = SamProfileResponse.from_row(entity, pocs, TODAY).model_dump(by_alias=True)
    assert d["registrationStatus"] == "active"               # expiry future + is_active
    assert d["daysUntilExpiration"] == (dt.date(2026, 9, 1) - TODAY).days
    assert d["expirationDate"] == "2026-09-01"
    assert d["registrationDate"] == "2019-01-01"             # <- initial_registration_date
    assert d["primaryNaics"] == "333120"
    assert d["secondaryNaics"] == ["333111", "423810"]       # primary dropped
    assert d["businessTypesRaw"] == "2X~A6~XS"               # raw bus_type_string, NOT parsed
    assert d["physicalAddress"] == {"street": None, "city": "St. Martin", "state": "MN", "zip": "56376"}
    assert d["mailingAddress"] is None                        # GAP
    assert d["governmentPocs"][0]["type"] == "electronic_business"
    assert d["governmentPocs"][0]["email"] is None and d["governmentPocs"][0]["phone"] is None  # GAP


def test_sam_profile_expired_when_past():
    entity = {"uei": "WBBSNBA9GBL7", "registration_expiration_date": dt.date(2025, 1, 1),
              "is_active": True, "naics_codes": []}
    d = SamProfileResponse.from_row(entity, [], TODAY).model_dump(by_alias=True)
    assert d["registrationStatus"] == "expired"              # expiry wins over is_active
    assert d["daysUntilExpiration"] == (dt.date(2025, 1, 1) - TODAY).days  # negative


def test_sam_profile_inactive_when_flag_false_and_not_expired():
    entity = {"uei": "WBBSNBA9GBL7", "registration_expiration_date": dt.date(2026, 9, 1),
              "is_active": False, "naics_codes": []}
    d = SamProfileResponse.from_row(entity, [], TODAY).model_dump(by_alias=True)
    assert d["registrationStatus"] == "inactive"            # not expired, but flagged inactive


def test_sam_poc_falls_back_to_name_parts():
    poc = {"poc_type": "past_performance", "poc_slot_no": 5, "full_name": None,
           "first_name": "John", "last_name": "Smith", "title": "PP POC"}
    assert SamPoc.from_row(poc).full_name == "John Smith"


# ── Active / past contract line items ──────────────────────────────────────────
def _award_row():
    return {
        "generated_unique_award_id": "CONT_AWD_1", "display_award_id": "ABC123",
        "category": "contract", "type_description": "DEFINITIVE CONTRACT",
        "total_obligation": 1000.0, "award_amount": 1200.0,
        "naics_code": "333120", "naics_description": "Machinery Mfg",
        "product_or_service_description": "Equipment",
        "funding_toptier_agency_name": "Department of Agriculture",
        "awarding_toptier_agency_name": "Department of Agriculture",
        "period_of_performance_start_date": dt.date(2024, 1, 1),
        "period_of_performance_current_end_date": dt.date(2026, 12, 1),
        "type_set_aside": "SDVOSBC", "description": "widgets",
        "action_date": dt.date(2024, 1, 1),
    }


def test_active_contract_maps_award_date_status_and_gaps():
    a = ActiveContract.from_row(_award_row(), TODAY, "active").model_dump(by_alias=True)
    assert a["awardId"] == "CONT_AWD_1"
    assert a["awardDate"] == "2024-01-01"                     # <- action_date
    assert a["periodEnd"] == "2026-12-01"
    assert a["daysToEnd"] == (dt.date(2026, 12, 1) - TODAY).days
    assert a["status"] == "active" and a["isActive"] is True
    assert a["setAside"] == "SDVOSBC"
    # GAP fields surface as explicit nulls, never fabricated:
    assert a["subAgency"] is None and a["agencyPoc"] is None
    assert a["optionYearDecisionWindow"] is None and a["modifications"] is None


def test_closed_contract_status_completed():
    a = ActiveContract.from_row(_award_row(), TODAY, "completed").model_dump(by_alias=True)
    assert a["status"] == "completed" and a["isActive"] is False


# ── entity_award_lines_gold point-lookup: side→column mapping + caps ────────────
def test_entity_award_lines_by_uei_maps_side_and_caps(monkeypatch):
    captured: dict = {}

    def fake_scan(uri, **kw):
        captured["uri"], captured["columns"], captured["filter"] = uri, kw.get("columns"), kw.get("filter")
        col = kw["columns"][1]                                  # the nested-list column requested
        return [{"uei": "WBBSNBA9GBL7", col: [_award_row() for _ in range(150)]}]

    monkeypatch.setattr(lance_store, "_scan", fake_scan)

    active = lance_store.entity_award_lines_by_uei("WBBSNBA9GBL7", "active", 25)
    assert captured["columns"] == ["uei", "active_contracts"]   # active → active_contracts
    assert captured["filter"] == "uei = 'WBBSNBA9GBL7'"
    assert len(active) == 25                                    # capped to the requested limit

    closed = lance_store.entity_award_lines_by_uei("WBBSNBA9GBL7", "closed", 999)
    assert captured["columns"] == ["uei", "past_performance"]   # closed → past_performance
    assert len(closed) == lance_store._AWARDS_HARD_CAP          # over-limit clamped to the hard cap

    # The struct flows straight into the wire model (gold struct == award_search column names).
    assert ActiveContract.from_row(active[0], TODAY, "active").model_dump(by_alias=True)["awardId"] == "CONT_AWD_1"


def test_entity_award_lines_by_uei_empty_when_uei_absent(monkeypatch):
    monkeypatch.setattr(lance_store, "_scan", lambda uri, **kw: [])   # no gold row for this UEI
    assert lance_store.entity_award_lines_by_uei("WBBSNBA9GBL7", "active", 25) == []


# ── Overview (Gold Mirror point-lookup) ────────────────────────────────────────
def test_overview_reads_gold_headlines():
    gold = {
        "uei": "WBBSNBA9GBL7", "total_lifetime_obligations": 2929171.0,
        "total_active_obligations": 500000.0, "award_count": 4, "active_award_count": 1,
        "has_federal_awards": True, "profile_as_of_date": dt.date(2026, 6, 3),
    }
    d = OverviewResponse.from_row(gold).model_dump(by_alias=True)
    assert d["lifetimeAwardValue"] == 2929171.0
    assert d["activeContractValue"] == 500000.0
    assert d["totalContractCount"] == 4
    assert d["activeContractCount"] == 1
    assert d["hasFederalAwards"] is True
    assert d["asOfDate"] == "2026-06-03"


# ── Past performance contract (full shape, GAPs explicit) ──────────────────────
def test_past_performance_full_contract_with_gaps():
    d = PastPerformanceResponse(
        closed_count=3, awards_lifetime=2929171.0, contracts_lifetime=4, contracts=[],
    ).model_dump(by_alias=True)
    assert d["closedCount"] == 3
    assert d["contractsLifetime"] == 4
    # CPARS / exclusions / recompetes are GAPs — present but null/empty, not omitted:
    assert d["cparsRatings"] == [] and d["cparsAverageRating"] is None
    assert d["exclusionsStatus"] is None and d["exclusionsLastChecked"] is None
    assert d["recompetesIn12Mo"] is None
