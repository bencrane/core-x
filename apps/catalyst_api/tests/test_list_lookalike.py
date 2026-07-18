"""list_lookalike_v1 — pure-composition tests (no network)."""
from apps.catalyst_api.src.routers.list_lookalike_v1 import (
    _CODE_RE,
    _UEI_RE,
    _sql_prime_lens,
    _sql_signature,
    _sql_sub_lens,
    _values_clause,
)

CV = "('ABC123DEF456')"
PAIRS = "('541511', 'D302')"
WEIGHTED = "('541511', 'D302', 0.5)"


def test_closed_shapes_refuse_quote_bearing_input():
    assert _UEI_RE.match("ABC123DEF456")
    assert not _UEI_RE.match("ABC'23DEF456")
    assert _CODE_RE.match("541511") and _CODE_RE.match("D302")
    assert not _CODE_RE.match("54'511")
    assert not _CODE_RE.match("")
    assert _values_clause(["ABC123DEF456"]) == "('ABC123DEF456')"


def test_signature_excludes_null_and_placeholder_codes():
    sql = _sql_signature(CV)
    assert "l.naics_code IS NOT NULL" in sql
    assert "l.psc_code <> '9999'" in sql
    # demonstrated prime lanes only — the sub mart never feeds the signature
    assert "gtm_sub_combo_lanes" not in sql


def test_prime_lens_ranks_by_weighted_share_and_flags_customer_subs():
    sql = _sql_prime_lens(CV, WEIGHTED, 100)
    assert "SUM(s.weight) AS score" in sql
    assert "ORDER BY h.score DESC" in sql
    # already-sub-under-a-customer is a flag column, never part of the score
    assert "subs_under_customer_ct" in sql
    assert "uei NOT IN (SELECT uei FROM u)" in sql
    assert "NOT LIKE 'MISCELLANEOUS%'" in sql


def test_sub_lens_is_separate_and_carries_own_prime_record():
    sql = _sql_sub_lens(CV, PAIRS, 50)
    assert "gtm_sub_combo_lanes" in sql
    assert "own_prime_obl_lifetime" in sql  # "mostly subs" must be visible
    assert "gtm_prime_combo_lanes" not in sql  # lenses never blended
    assert "NOT LIKE 'MISCELLANEOUS%'" in sql
