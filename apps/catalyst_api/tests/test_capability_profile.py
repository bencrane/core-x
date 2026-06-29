"""Unit tests for the capability-profile composition - pure, no R2 / network."""

from __future__ import annotations

from datetime import date

from apps.catalyst_api.src.models import CapabilityProfileResponse

ROW = {
    "uei": "DF1HR8L5BDB4", "firm_name": "G. C. MICRO CORPORATION", "state_code": "CA",
    "parent_uei": "DF1HR8L5BDB4", "is_dsbs": True, "has_sub_history": True,
    "has_prime_history": True, "federal_status": "active_sub", "designations": ["WOSB", "8(a)"],
    "sub_amount_5y": 58929440.01, "sub_received_5y": 470, "sub_distinct_primes_5y": 180,
    "sub_distinct_prime_partners_5y": 150, "recent_subawards_90d": 116,
    "recent_subaward_amount_90d": 20939620.38, "recent_latest_action_date": date(2026, 3, 2),
    "recent_top_prime_name": "SIGMATECH, INC.", "recent_top_naics_code": "541712",
    "recent_top_naics_description": "RESEARCH AND DEVELOPMENT", "recent_subaward_scope": "XR-4",
    "sub_top_prime_partners": [{"name": "LOCKHEED MARTIN CORPORATION", "uei": "FYHNA5WC8XD7", "subawards": 53, "amount": 12976085.49}],
    "sub_top_naics": [{"code": "541715", "description": "R&D", "subawards": 90, "amount": 12345877.51}],
    "prime_awards_5y": 240, "prime_obligated_5y": 14004928.98, "prime_competed_awards_5y": 46,
    "prime_distinct_naics_5y": 5,
    "prime_top_naics": [{"code": "541519", "description": "OTHER COMPUTER RELATED SERVICES", "awards": 222, "obligated": 13286018.88}],
    "prime_top_psc": [{"code": "7A21", "description": "PERPETUAL LICENSE SOFTWARE", "awards": 57, "obligated": 2526493.85}],
    "prime_top_agencies": [{"agency": "Department of Defense", "subagency": "Department of the Navy", "awards": 100, "obligated": 4965421.63}],
    "recommended_lanes": [{"rank": 1, "evidence_tier": "primed-direct", "dst_naics": "541519", "dst_psc": "DA01",
                           "naics_desc": "OTHER COMPUTER RELATED SERVICES", "psc_desc": "IT AND TELECOM",
                           "score": 36.0, "lane_n_primes": 36, "lane_median_amt": 215431.0,
                           "top_primes": ["CACI NSS, LLC", "LEIDOS, INC."]}],
    "n_recommended_lanes": 10, "top_evidence_tier": "primed-direct", "materialized_at": date(2026, 6, 29),
}


def _wire(row):
    return CapabilityProfileResponse.from_row(row).model_dump(by_alias=True)


def test_identity_status_designations():
    d = _wire(ROW)
    assert d["uei"] == "DF1HR8L5BDB4" and d["firmName"] == "G. C. MICRO CORPORATION"
    assert d["federalStatus"] == "active_sub" and d["isDsbs"] is True
    assert d["designations"] == ["WOSB", "8(a)"]
    assert d["topEvidenceTier"] == "primed-direct" and d["nRecommendedLanes"] == 10


def test_sub_activity_clean_aliases_and_normalization():
    d = _wire(ROW)["subActivity"]
    assert d["amount5y"] == 58929440.01 and d["subawards5y"] == 470
    assert d["distinctPrimes5y"] == 180 and d["recentSubawards90d"] == 116
    assert d["recentLatestActionDate"] == "2026-03-02"
    assert d["topPrimePartners"][0] == {"name": "LOCKHEED MARTIN CORPORATION", "uei": "FYHNA5WC8XD7", "count": 53, "dollars": 12976085.49}
    assert d["topNaics"][0]["count"] == 90 and d["topNaics"][0]["dollars"] == 12345877.51


def test_prime_activity_clean_aliases_and_mapping():
    d = _wire(ROW)["primeActivity"]
    assert d["awards5y"] == 240 and d["obligated5y"] == 14004928.98 and d["competedAwards5y"] == 46
    assert d["topNaics"][0]["count"] == 222 and d["topNaics"][0]["dollars"] == 13286018.88
    assert d["topAgencies"][0]["subAgency"] == "Department of the Navy"


def test_recommended_lane_shape():
    lane = _wire(ROW)["recommendedLanes"][0]
    assert lane["evidenceTier"] == "primed-direct" and lane["naics"] == "541519"
    assert lane["lanePrimes"] == 36 and lane["laneMedianAmount"] == 215431.0
    assert lane["topPrimes"] == ["CACI NSS, LLC", "LEIDOS, INC."]


def test_never_subbed_nulls_sub_activity():
    d = _wire({**ROW, "has_sub_history": False, "federal_status": "dsbs_prospect"})
    assert d["subActivity"] is None and d["primeActivity"] is not None


def test_no_prime_nulls_prime_activity():
    d = _wire({**ROW, "has_prime_history": False})
    assert d["primeActivity"] is None and d["subActivity"] is not None
