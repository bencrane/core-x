"""Unit tests for the person-by-LinkedIn surface — pure parsing + composition, no R2.

Pins the two load-bearing pieces of /api/v1/people/by-linkedin: (1) the LinkedIn URL
canonicalization (every caller form for one person must collapse to the same slug + the
same stored-variant match set, and a non-person URL must be rejected), and (2) the
response composition (headline title = first non-null across matches, every duplicate
observation surfaced, camelCase wire shape).
"""

from __future__ import annotations

from apps.catalyst_api.src import lance_store
from apps.catalyst_api.src.models import PersonByLinkedInResponse


# ── Slug extraction (the resolution key) ──────────────────────────────────────
def test_slug_is_form_invariant():
    # scheme / www / trailing-slash / case / query-string / country-subdomain all collapse.
    for url in (
        "https://www.linkedin.com/in/alexkigel/",
        "https://www.linkedin.com/in/alexkigel",
        "linkedin.com/in/alexkigel",
        "www.linkedin.com/in/alexkigel/",
        "HTTPS://WWW.LinkedIn.com/in/AlexKigel/",
        "https://www.linkedin.com/in/alexkigel/?originalSubdomain=us",
        "https://ca.linkedin.com/in/alexkigel/",
        "https://www.linkedin.com/in/alexkigel/detail/contact-info/",
    ):
        assert lance_store.linkedin_slug(url) == "alexkigel", url


def test_slug_rejects_non_person_urls():
    assert lance_store.linkedin_slug("https://www.linkedin.com/company/foo/") is None
    assert lance_store.linkedin_slug("https://www.linkedin.com/school/bar/") is None
    assert lance_store.linkedin_slug("https://example.com/in/foo") is None   # not a linkedin host
    assert lance_store.linkedin_slug("not a url") is None
    assert lance_store.linkedin_slug("") is None
    assert lance_store.linkedin_slug(None) is None  # type: ignore[arg-type]


def test_slug_guard_blocks_injection():
    # The slug guard is defense-in-depth alongside _sql_str quote-doubling.
    assert lance_store.valid_linkedin_slug("alexkigel")
    assert lance_store.valid_linkedin_slug("craig-goldstein-cfa-99204b1b")
    assert not lance_store.valid_linkedin_slug("evil' OR '1'='1")
    assert not lance_store.valid_linkedin_slug("has space")
    assert not lance_store.valid_linkedin_slug("")


# ── Match variants (the BTREE IN predicate inputs) ────────────────────────────
def test_match_variants_cover_canonical_form_first():
    variants = lance_store._linkedin_match_variants("alexkigel")
    # The dominant live stored form leads (scheme + www + trailing slash).
    assert variants[0] == "https://www.linkedin.com/in/alexkigel/"
    # Scheme × host × slash → robust to source drift, all still BTREE-eligible literals.
    assert "https://linkedin.com/in/alexkigel" in variants
    assert "http://www.linkedin.com/in/alexkigel/" in variants
    assert len(variants) == len(set(variants))


# ── Response composition ──────────────────────────────────────────────────────
def _row(**kw):
    base = {
        "contact_id": None, "person_id": None, "company_id": None, "normalized_domain": None,
        "full_name": None, "first_name": None, "last_name": None,
        "title": None, "person_linkedin_url": None, "source_platform": None,
    }
    base.update(kw)
    return base


def test_from_rows_picks_first_nonnull_title_and_surfaces_all_matches():
    rows = [
        _row(title=None, full_name="Alex Kigel", company_id="c1", contact_id="p1", person_id="p1"),
        _row(title="Managing Director", full_name="Alex Kigel", company_id="c1", contact_id="p1", person_id="p1"),
    ]
    d = PersonByLinkedInResponse.from_rows("https://www.linkedin.com/in/alexkigel/", rows).model_dump(by_alias=True)
    assert d["found"] is True
    assert d["title"] == "Managing Director"          # first non-null title wins the headline
    assert d["fullName"] == "Alex Kigel"
    assert d["matchCount"] == 2                        # duplicate observations are NOT collapsed
    assert len(d["matches"]) == 2
    assert d["linkedinUrl"] == "https://www.linkedin.com/in/alexkigel/"
    # EXPAND/CONTRACT: both the legacy contact_id and the new person_id ride the wire (mirrored value).
    assert d["matches"][0]["contactId"] == "p1"
    assert d["matches"][0]["personId"] == "p1"


def test_from_rows_empty_is_not_found():
    d = PersonByLinkedInResponse.from_rows("https://www.linkedin.com/in/nobody/", []).model_dump(by_alias=True)
    assert d["found"] is False
    assert d["title"] is None and d["fullName"] is None
    assert d["matchCount"] == 0 and d["matches"] == []
