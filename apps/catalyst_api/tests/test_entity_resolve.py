"""entity_resolve_v1 — pure-composition tests (no network)."""
from apps.catalyst_api.src.routers.entity_resolve_v1 import (
    _DOMAIN_RE,
    _UEI_RE,
    _values_clause,
    normalize_domain,
)


def test_domain_normalization():
    assert normalize_domain("https://www.Acme.com/path?q=1") == "acme.com"
    assert normalize_domain("ACME.COM") == "acme.com"
    assert normalize_domain("sub.acme.co.uk") == "sub.acme.co.uk"
    assert normalize_domain("www.acme.com:8080") == "acme.com"
    assert normalize_domain("not a domain") is None
    assert normalize_domain("") is None
    assert normalize_domain("acme") is None  # bare label — no TLD


def test_uei_shape():
    assert _UEI_RE.match("ABC123DEF456")
    assert not _UEI_RE.match("ABC123DEF45")     # 11 chars
    assert not _UEI_RE.match("ABC123DEF4567")   # 13 chars
    assert not _UEI_RE.match("ABC123-EF456")    # punctuation


def test_values_clause_only_embeds_validated_tokens():
    # inputs reaching _values_clause are pre-validated against the closed
    # regexes — assert the regexes refuse anything quote-bearing so no user
    # text can reach SQL.
    assert not _DOMAIN_RE.match("a'b.com")
    assert not _UEI_RE.match("ABC'23DEF456")
    assert _values_clause(["acme.com", "b.co"]) == "('acme.com'), ('b.co')"
