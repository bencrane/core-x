"""Unit tests for catalyst_api — pure composition + domain normalization.

No R2 / network. These pin the wire contract (camelCase, ISO dates, agency
collapse, the federal-footprint flag) and the domain-anchor normalization that
guards the Lance filter, using stub rows shaped exactly like the live Lance
schemas (verified against the active sink).
"""

from __future__ import annotations

import datetime as dt

from apps.catalyst_api.src import lance_store
from apps.catalyst_api.src.models import AwardProfile, AwardProfileResponse, Company, RecentAward


# ── domain normalization + filter guard ──────────────────────────────────────
def test_normalize_collapses_to_stored_anchor():
    for raw in ("https://www.Rotochopper.com/about?x=1", "WWW.ROTOCHOPPER.COM", "rotochopper.com:443", "rotochopper.com."):
        assert lance_store.normalize_domain(raw) == "rotochopper.com"


def test_normalize_empty_is_none():
    assert lance_store.normalize_domain("") is None
    assert lance_store.normalize_domain("   ") is None


def test_valid_domain_guard():
    assert lance_store.valid_domain("rotochopper.com")
    assert lance_store.valid_domain("sub.example.co.uk")
    # rejects anything that could break out of the Lance filter / isn't a domain
    assert not lance_store.valid_domain("no-dot")
    assert not lance_store.valid_domain("evil' OR '1'='1")
    assert not lance_store.valid_domain("a b.com")


def test_sql_str_doubles_quotes():
    assert lance_store._sql_str("o'brien.com") == "'o''brien.com'"


# ── company composition ───────────────────────────────────────────────────────
def test_company_from_firmographics_row():
    row = {
        "domain_norm": "rotochopper.com", "domain_raw": "rotochopper.com",
        "uei": "WBBSNBA9GBL7", "company_name": "Rotochopper, Inc",
        "website": "https://rotochopper.com", "industry": "Paper and Forest Product Manufacturing",
        "employee_size_band": "51-200", "founded_year": 1992,
        "hq_city": "St. Martin", "hq_state": "Minnesota", "hq_region": "Midwest",
    }
    c = Company.from_row(row)
    assert c.uei == "WBBSNBA9GBL7"
    dumped = c.model_dump(by_alias=True)
    assert dumped["employeeSizeBand"] == "51-200"
    assert dumped["foundedYear"] == 1992
    assert dumped["hqState"] == "Minnesota"


# ── award-profile composition ──────────────────────────────────────────────────
def test_award_profile_collapses_top_agencies_and_isos_dates():
    row = {
        "recipient_uei": "WBBSNBA9GBL7", "total_combined_obligated": 2929171.0,
        "lifetime_prime_obligated": 2929171.0, "prime_total_awards": 4,
        "prime_active_awards": 1, "prime_closed_awards": 3,
        "contract_dollars": 2929171.0, "grant_dollars": 0.0, "other_dollars": 0.0,
        "primary_naics": "333120", "primary_psc": "3040",
        "prime_first_award_date": dt.date(2019, 3, 1),
        "prime_most_recent_action_date": dt.date(2025, 4, 30),
        "summary_as_of_date": dt.date(2026, 6, 3),
        "top_agency_1_name": "Department of Agriculture", "top_agency_1_dollars": 2929171.0,
        "top_agency_2_name": None, "top_agency_2_dollars": None,
        "top_agency_3_name": None, "top_agency_3_dollars": None,
    }
    p = AwardProfile.from_row(row)
    assert len(p.top_agencies) == 1  # nulls dropped
    assert p.top_agencies[0].name == "Department of Agriculture"
    dumped = p.model_dump(by_alias=True)
    assert dumped["firstAwardDate"] == "2019-03-01"
    assert dumped["mostRecentActionDate"] == "2025-04-30"
    assert dumped["primaryNaics"] == "333120"
    assert dumped["totalCombinedObligated"] == 2929171.0


def test_recent_award_maps_award_search_columns():
    row = {
        "generated_unique_award_id": "CONT_AWD_1", "display_award_id": "ABC123",
        "category": "contract", "type_description": "DEFINITIVE CONTRACT",
        "total_obligation": 1000.0, "award_amount": 1200.0,
        "naics_code": "333120", "naics_description": "Machinery Mfg",
        "product_or_service_description": "Equipment",
        "funding_toptier_agency_name": "Department of Agriculture",
        "awarding_toptier_agency_name": "Department of Agriculture",
        "period_of_performance_start_date": dt.date(2024, 1, 1),
        "period_of_performance_current_end_date": dt.date(2026, 1, 1),
        "type_set_aside": "NONE", "description": "widgets",
    }
    a = RecentAward.from_row(row).model_dump(by_alias=True)
    assert a["awardId"] == "CONT_AWD_1"
    assert a["fundingAgency"] == "Department of Agriculture"
    assert a["periodStart"] == "2024-01-01"
    assert a["obligation"] == 1000.0


# ── full envelope shape ────────────────────────────────────────────────────────
def test_response_known_company_no_federal_footprint():
    resp = AwardProfileResponse(
        domain="example.com", matched=True, is_federal_contractor=False,
        company=Company(name="Example LLC", uei=None), award_profile=None, recent_awards=None,
    )
    d = resp.model_dump(by_alias=True)
    assert d["matched"] is True
    assert d["isFederalContractor"] is False
    assert d["awardProfile"] is None
    assert d["recentAwards"] is None
