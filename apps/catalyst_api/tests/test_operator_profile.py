"""Unit tests for the maximal operator profile — pure, no R2 / network.

Pins (1) the composition contract: every section present, labeled with its source,
best-effort per section (one dead dataset degrades THAT section with an error string,
never the page); (2) the people join (contactability rides each person, provider values
verbatim); (3) the render: self-contained HTML carrying the headline, every section
title, the contact assets, and the raw JSON blocks (the nothing-hidden guarantee),
with caller values HTML-escaped; (4) the /profile route token gate (401 without the
operator token when one is set; query-param and bearer both accepted).
"""
from __future__ import annotations

from datetime import date

import pytest

from apps.catalyst_api.src import config, profile_html

UEI = "UEITARGET001"

SAM_ROW = {"uei": UEI, "legal_business_name": "EMERZIAN <WOODWORKING>",
           "normalized_domain": "emerzian.com", "physical_state": "CA",
           "in_sam": True, "in_dsbs": True, "sam_is_active": True}
ROLLUP_ROW = {"uei": UEI, "prime_obl_lifetime": 0.0, "sub_amt_lifetime": 2_400_000.0,
              "is_prime_24mo": False, "active_award_ct": 0}
GEO_ROW = {"uei": UEI, "latitude": 36.7, "longitude": -119.8, "geo_precision": "address"}
FIRMO_ROW = {"domain_norm": "emerzian.com", "employee_size_band": "11-50"}
PEOPLE_ROWS = [
    {"sam_person_id": "p1", "uei": UEI, "display_name": "PAT PRINCIPAL",
     "best_title": "PRESIDENT", "n_mentions": 9, "is_dsbs_principal": True,
     "is_govt_poc": True},
    {"sam_person_id": "p2", "uei": UEI, "display_name": "ALEX ADMIN",
     "best_title": "OFFICE MANAGER", "n_mentions": 2},
]
CONTACT_ROWS = [
    {"sam_person_id": "p1", "uei": UEI, "phone": "+1 559 555 0100",
     "phone_status": "found", "email": "pat@emerzian.com",
     "person_linkedin_url_norm": "linkedin.com/in/pat-principal"},
]
LANE_ROWS = [
    {"uei": UEI, "side": "sub", "code_type": "naics", "code": "236220",
     "obl_lifetime": 2_400_000.0, "last_action_date": date(2026, 5, 1)},
]
INFERRED_ROWS = [
    {"uei": UEI, "code_type": "naics", "code": "115310", "supporting_bothsider_firm_ct": 12},
    {"uei": UEI, "code_type": "naics", "code": "221122", "supporting_bothsider_firm_ct": 7},
]
CARD_ROW = {"uei": UEI, "firm_name": "EMERZIAN WOODWORKING", "federal_status": "active_sub",
            "n_recommended_lanes": 3, "top_evidence_tier": "subbed-hop",
            "materialized_at": "2026-07-01T22:35:00"}
GOLD_ROW = {"uei": UEI, "active_award_count": 0, "award_count": 4}
SUBOUT_RESULT = {
    "meta": {"recipeId": "subout_opportunities.v2", "total": 1,
             "target_hq": {"latitude": 36.7, "longitude": -119.8}},
    "data": {"opportunities": [{
        "generated_unique_award_id": "AWD1", "award_id_piid": "PIID1",
        "prime_name": "DELOITTE CONSULTING LLP", "prime_uei": "UEIPRIME0001",
        "awarding_agency_name": "GSA", "total_obligation": 86_000_000.0,
        "period_of_performance_current_end_date": "2026-09-27",
        "ordering_period_end_date": None, "distance_mi": 118.6, "score": 0.87,
        "nearest_federal_site": {"site_name": "CENTRAL COAST FIELD OFFICE"},
        "matched": [{"lens": "delivered_subawards_under_code", "code": "236220",
                     "evidence": {}}],
        "components": [],
    }], "peers": []},
}


class ProfileSeams:
    """Recording fake for the _rows seam + the in-process subout call."""

    def __init__(self):
        self.fail_uris: set[str] = set()
        self.calls: list[tuple[str, str]] = []

    def rows(self, uri, predicate, columns=None):
        self.calls.append((uri, predicate))
        if uri in self.fail_uris:
            raise OSError("dataset unreachable")
        table = {
            config.GTM_SAM_ENTITIES_URI: [SAM_ROW],
            config.GTM_ENTITY_BEHAVIOR_ROLLUP_URI: [ROLLUP_ROW],
            config.GTM_ENTITY_GEO_URI: [GEO_ROW],
            config.FIRMOGRAPHICS_URI: [FIRMO_ROW],
            config.GTM_SAM_PEOPLE_URI: PEOPLE_ROWS,
            config.GTM_SAM_PERSON_CONTACTABILITY_URI: CONTACT_ROWS,
            config.GTM_ENTITY_CODE_LANES_URI: LANE_ROWS,
            config.GTM_INFERRED_PRIMEABLE_URI: INFERRED_ROWS,
            config.GTM_INFERRED_SUBBABLE_URI: [],
            config.CAPABILITY_PROFILE_URI: [CARD_ROW],
            config.ENTITY_PROFILE_GOLD_URI: [GOLD_ROW],
        }.get(uri)
        if table is None:
            raise AssertionError(f"unexpected profile scan uri {uri}")
        key_match = [r for r in table if f"'{r.get('uei', r.get('domain_norm'))}'" in predicate]
        rows = [dict(r) for r in key_match]
        if columns:
            rows = [{c: r.get(c) for c in columns} for r in rows]
        return rows


@pytest.fixture()
def seams(monkeypatch):
    s = ProfileSeams()
    monkeypatch.setattr(profile_html, "_rows", s.rows)
    monkeypatch.setattr(profile_html, "_load_subout",
                        lambda uei, include_peers: dict(SUBOUT_RESULT))
    yield s


EXPECTED_SECTIONS = {
    "sam_entity", "rollup", "geo", "firmographics", "people", "lanes",
    "inferred_primeable", "inferred_subbable", "subout_opportunities",
    "legacy_capability_card", "legacy_gold",
}


def test_compose_assembles_every_section_with_sources(seams):
    p = profile_html.compose_profile(UEI)
    assert p["uei"] == UEI
    assert set(p["sections"]) == EXPECTED_SECTIONS
    for name, sec in p["sections"].items():
        assert sec.get("source"), name
        assert "error" not in sec, (name, sec.get("error"))
    # people join: contactability rides the person, mention-ranked, verbatim values
    ppl = p["sections"]["people"]["data"]
    assert ppl["total_people"] == 2
    assert ppl["people"][0]["display_name"] == "PAT PRINCIPAL"
    assert ppl["people"][0]["contact"]["phone"] == "+1 559 555 0100"
    assert ppl["people"][1]["contact"] is None
    # inferred ranked by support; lanes ranked by $; legacy sections labeled LEGACY
    assert [c["code"] for c in p["sections"]["inferred_primeable"]["data"]["codes"]] == \
        ["115310", "221122"]
    assert p["sections"]["legacy_capability_card"]["note"].startswith("LEGACY")
    assert p["sections"]["legacy_gold"]["note"].startswith("LEGACY")
    # dates JSON-shaped for the assembly route
    assert p["sections"]["lanes"]["data"]["lanes"][0]["last_action_date"] == "2026-05-01"
    # firmographics resolved through the sam row's normalized_domain
    assert p["sections"]["firmographics"]["data"]["employee_size_band"] == "11-50"


def test_one_dead_dataset_degrades_only_its_section(seams):
    seams.fail_uris.add(config.GTM_SAM_PERSON_CONTACTABILITY_URI)
    seams.fail_uris.add(config.ENTITY_PROFILE_GOLD_URI)
    p = profile_html.compose_profile(UEI)
    assert p["sections"]["people"]["error"].startswith("OSError")
    assert p["sections"]["legacy_gold"]["error"].startswith("OSError")
    # everything else still composed
    assert p["sections"]["sam_entity"]["data"]["legal_business_name"]
    assert p["sections"]["subout_opportunities"]["data"]["meta"]["total"] == 1


def test_render_is_selfcontained_maximal_and_escaped(seams):
    html_doc = profile_html.render_profile(profile_html.compose_profile(UEI))
    assert html_doc.startswith("<!doctype html>")
    # headline is the SAM legal name — HTML-ESCAPED (the fixture carries <> on purpose)
    assert "EMERZIAN &lt;WOODWORKING&gt;" in html_doc
    assert "EMERZIAN <WOODWORKING>" not in html_doc
    # every section heading renders
    for title in ("Identity / SAM registration", "Behavior posture (fresh)", "HQ geo",
                  "Firmographics", "People + contactability", "Demonstrated code lanes",
                  "Inferred primeable", "Inferred subbable",
                  "Sub-out opportunities (live recipe)", "Legacy capability card",
                  "Legacy gold profile"):
        assert title in html_doc, title
    # the cold-call payload is on the page
    assert "+1 559 555 0100" in html_doc and "pat@emerzian.com" in html_doc
    # the opportunities row renders with score + prime + site
    assert "DELOITTE CONSULTING LLP" in html_doc and "0.87" in html_doc
    assert "CENTRAL COAST FIELD OFFICE" in html_doc
    # the nothing-hidden guarantee: raw JSON blocks per section
    assert html_doc.count("<details>") == len(EXPECTED_SECTIONS)
    # source labels ride each section
    assert "gtm_sam_person_contactability" in html_doc


def test_profile_route_token_gate(seams, monkeypatch):
    from fastapi.testclient import TestClient

    from apps.catalyst_api.main import app

    monkeypatch.setattr(config, "operator_token", lambda: "sekrit")
    client = TestClient(app)
    assert client.get(f"/profile/{UEI}").status_code == 401
    assert client.get(f"/profile/{UEI}?token=wrong").status_code == 401
    ok = client.get(f"/profile/{UEI}?token=sekrit")
    assert ok.status_code == 200 and "People + contactability" in ok.text
    ok2 = client.get(f"/profile/{UEI}", headers={"Authorization": "Bearer sekrit"})
    assert ok2.status_code == 200
    assert client.get("/profile/short?token=sekrit").status_code == 400


def test_assembly_route_serves_json_envelope(seams, monkeypatch):
    from fastapi.testclient import TestClient

    from apps.catalyst_api.main import app

    monkeypatch.setattr(config, "operator_token", lambda: None)  # local-dev posture
    client = TestClient(app)
    res = client.get(f"/api/v1/entities/{UEI}/profile")
    assert res.status_code == 200
    body = res.json()["data"]
    assert set(body["sections"]) == EXPECTED_SECTIONS
    assert body["sections"]["people"]["data"]["people"][0]["contact"]["email"] == \
        "pat@emerzian.com"
