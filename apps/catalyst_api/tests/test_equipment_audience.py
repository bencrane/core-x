"""equipment-audience router — segment filter + lookup normalization, offline."""
from __future__ import annotations

import pytest
from fastapi import HTTPException

from apps.catalyst_api.src.routers import equipment_audience_v1 as ea


def _row(**over):
    base = dict(
        person_key="linkedin.com/in/x", linkedin_url_norm="linkedin.com/in/x",
        full_name="X", first_name="X", last_name="Y", title="President",
        priority_tier="T1", title_class="exec_owner", dm_class=None,
        domain_norm="acme.com", company_name="acme", uei=None,
        macro_region="Mountain West", demo_region="state: CO",
        industries_topline=None, equipment_sample=None, matched_psc_count=3,
        n_people_at_domain=4, email=None, email_status=None, email_source=None,
        phone=None, phone_status=None, source_plane="domain",
        loc_city=None, loc_state=None, materialized_at="2026-07-26",
    )
    base.update(over)
    return base


ROWS = [
    _row(),
    _row(person_key="k2", priority_tier="T2", title_class="sales_manager",
         n_people_at_domain=12, macro_region="Southeast"),
    _row(person_key="k3", priority_tier="T2", title_class="operations",
         email_status="ok", source_plane="both"),
    _row(person_key="k4", priority_tier="T3", title_class="off_register"),
]


def test_segment_tier_filter():
    out = ea.apply_segment(ROWS, {"tiers": ["T1"]})
    assert [r["person_key"] for r in out] == ["linkedin.com/in/x"]


def test_segment_combined():
    out = ea.apply_segment(ROWS, {"tiers": ["T2"], "macro_regions": ["Mountain West"]})
    assert [r["person_key"] for r in out] == ["k3"]


def test_segment_max_people_at_domain_excludes_null_and_large():
    rows = ROWS + [_row(person_key="k5", n_people_at_domain=None)]
    out = ea.apply_segment(rows, {"max_people_at_domain": 10})
    assert all(r["n_people_at_domain"] <= 10 for r in out)
    assert "k2" not in {r["person_key"] for r in out}
    assert "k5" not in {r["person_key"] for r in out}


def test_segment_email_status_and_plane():
    out = ea.apply_segment(ROWS, {"email_status": ["ok"], "source_planes": ["both"]})
    assert [r["person_key"] for r in out] == ["k3"]


def test_segment_unknown_tier_422():
    with pytest.raises(HTTPException) as exc:
        ea.apply_segment(ROWS, {"tiers": ["T9"]})
    assert exc.value.status_code == 422


def test_segment_bad_shape_422():
    with pytest.raises(HTTPException) as exc:
        ea.apply_segment(ROWS, {"macro_regions": "Mountain West"})
    assert exc.value.status_code == 422


def test_norm_linkedin():
    assert ea._norm_linkedin("https://www.linkedin.com/in/Foo/") == "linkedin.com/in/foo"


def test_norm_domain():
    assert ea._norm_domain("https://www.Acme.com/path?q=1") == "acme.com"


def test_person_lookup_uses_cache(monkeypatch):
    monkeypatch.setattr(ea, "_cache_rows", ROWS)
    monkeypatch.setattr(ea, "_cache_at", 9e12)  # far future — never refresh
    out = ea.person_lookup({"domain": "https://www.acme.com"})
    assert out["found"] is True and out["count"] == len(ROWS)
    out = ea.person_lookup({"linkedin": "https://linkedin.com/in/x/"})
    assert out["found"] is True and out["people"][0]["person_key"] == "linkedin.com/in/x"
    out = ea.person_lookup({"person_key": "nope"})
    assert out["found"] is False and out["people"] == []


def test_person_lookup_requires_identifier(monkeypatch):
    monkeypatch.setattr(ea, "_cache_rows", ROWS)
    monkeypatch.setattr(ea, "_cache_at", 9e12)
    with pytest.raises(HTTPException) as exc:
        ea.person_lookup({})
    assert exc.value.status_code == 422
