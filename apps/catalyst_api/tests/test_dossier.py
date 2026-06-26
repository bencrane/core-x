"""Unit tests for the entity dossier composition — pure, no R2 / network.

Pins EntityDossierResponse.from_parts: identity/address projection, the latest-action
resolution (greatest of the CAS summary date and the 90d action feed — covering both a
lagging summary and subaward-only freshness), days-since direction (a PAST date reads
as POSITIVE days ago), top-agency extraction, POC slot ordering + cap, and the honest
empty recent-activity state.
"""

from __future__ import annotations

from datetime import date

from apps.catalyst_api.src.models import EntityDossierResponse

TODAY = date(2026, 6, 12)

GOLD = {
    "uei": "NPFUUDHHMJY1",
    "cage_code": "53LJ9",
    "is_active": True,
    "has_federal_awards": True,
    "legal_business_name": "CECIL & CECIL ENTERPRISES INC",
    "dba_name": None,
    "primary_naics": "237990",
    "physical_address_line_1": "3741 BUSINESS DR",
    "physical_address_city": "SACRAMENTO",
    "physical_address_state": "CA",
    "physical_address_zip_postal_code": "95820",
    "physical_address_country_code": "USA",
    "total_active_obligations": 2826014.57,
    "total_lifetime_obligations": 10264985.68,
    "award_count": 23,
    "active_award_count": 1,
    "profile_as_of_date": date(2026, 6, 11),
    "pocs": [
        {"poc_type": "government_business", "poc_slot_no": 2, "full_name": "B SECOND",
         "title": "VP", "city": "SACRAMENTO", "state": "CA"},
        {"poc_type": "government_business", "poc_slot_no": 1, "full_name": "COREEN U CECIL",
         "title": "PRESIDENT", "city": "SACRAMENTO", "state": "CA"},
    ],
}

CAS = {
    "prime_most_recent_action_date": date(2026, 5, 28),
    "top_agency_1_name": "Department of Health and Human Services",
    "top_agency_1_dollars": 8185278.85,
    "top_agency_2_name": None,
}

ACTIONS = [
    {"award_id": "A1", "action_date": date(2026, 5, 28), "action_obligated_usd": 271419.0,
     "awarding_agency": "Department of Health and Human Services",
     "awarding_sub_agency": "Indian Health Service", "winner_type": "prime_recipient",
     "pop_state": "CA", "pop_city": "SACRAMENTO", "set_aside": None, "naics_code": "237110"},
]


def _wire(model):
    return model.model_dump(by_alias=True)


def test_identity_and_address_projection():
    d = _wire(EntityDossierResponse.from_parts(GOLD, CAS, ACTIONS, TODAY))
    ident = d["identity"]
    assert ident["uei"] == "NPFUUDHHMJY1" and ident["cageCode"] == "53LJ9"
    assert ident["legalBusinessName"].startswith("CECIL")
    assert ident["address"] == {"street": "3741 BUSINESS DR", "city": "SACRAMENTO",
                                "state": "CA", "zip": "95820"}


def test_latest_action_and_days_since_are_recency_directed():
    d = _wire(EntityDossierResponse.from_parts(GOLD, CAS, ACTIONS, TODAY))
    assert d["posture"]["latestActionDate"] == "2026-05-28"
    # 2026-05-28 → 2026-06-12 is 15 days AGO (positive — not the expiry countdown sign).
    assert d["posture"]["daysSinceLastAction"] == 15


def test_action_feed_fresher_than_summary_wins():
    fresh = [{**ACTIONS[0], "action_date": date(2026, 6, 10)}]
    d = _wire(EntityDossierResponse.from_parts(GOLD, CAS, fresh, TODAY))
    assert d["posture"]["latestActionDate"] == "2026-06-10"
    assert d["posture"]["daysSinceLastAction"] == 2


def test_missing_summary_degrades_to_action_feed():
    d = _wire(EntityDossierResponse.from_parts(GOLD, None, ACTIONS, TODAY))
    assert d["posture"]["latestActionDate"] == "2026-05-28"
    assert d["posture"]["topAgencies"] == []


def test_no_activity_is_truthful_not_an_error():
    d = _wire(EntityDossierResponse.from_parts(GOLD, None, [], TODAY))
    assert d["recentActivity"] == {"windowDays": 90, "actions": []}
    assert d["posture"]["latestActionDate"] is None
    assert d["posture"]["daysSinceLastAction"] is None


def test_top_agencies_skip_empty_slots():
    d = _wire(EntityDossierResponse.from_parts(GOLD, CAS, ACTIONS, TODAY))
    assert d["posture"]["topAgencies"] == [
        {"name": "Department of Health and Human Services", "dollars": 8185278.85}
    ]


def test_pocs_sorted_by_slot_and_carry_no_contact_channels():
    d = _wire(EntityDossierResponse.from_parts(GOLD, CAS, ACTIONS, TODAY))
    assert [p["fullName"] for p in d["pocs"]] == ["COREEN U CECIL", "B SECOND"]
    # The SAM source has no email/phone columns — the wire shape must keep them null.
    assert all(p["email"] is None and p["phone"] is None for p in d["pocs"])


def test_poc_cap_bounds_runaway_slots():
    gold = {**GOLD, "pocs": [{"poc_slot_no": i, "full_name": f"P{i}"} for i in range(1, 20)]}
    d = _wire(EntityDossierResponse.from_parts(gold, None, [], TODAY))
    assert len(d["pocs"]) == 12


def test_recent_action_wire_shape():
    d = _wire(EntityDossierResponse.from_parts(GOLD, CAS, ACTIONS, TODAY))
    a = d["recentActivity"]["actions"][0]
    assert a["actionDate"] == "2026-05-28" and a["amount"] == 271419.0
    assert a["awardingSubAgency"] == "Indian Health Service"
    assert a["popCity"] == "SACRAMENTO" and a["popState"] == "CA"
