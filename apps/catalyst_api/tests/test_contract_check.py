"""Hermetic tests for the boot decoder schema/index contract check (R-09, §7-5).

Exercises the PURE checkers — ``verify_decoder_contract`` (schema/index, synthetic inputs in
the REAL ds.list_indices() shape) and ``_live_stat_findings`` (row count + unindexed-row
classification). No R2 access. Both return ``{"violations": [...], "notes": [...]}``: hard
violations abort boot under strict mode + flip /healthz to 503; soft notes are advisory only.
"""

from apps.catalyst_api.src.lance_store import _live_stat_findings, verify_decoder_contract
from apps.catalyst_api.src.map_decoders import AWARDS, COMPANY, WINNERS

# Live index inventory in the real list_indices() shape: dict per entry, 'fields' is the
# physical-column list, 'type' is mixed-case. Includes the resolution-key indexes the
# decoder does NOT declare (winner_uei_idx, addr_hash_idx) to lock in declared-⊆-actual.
WINNERS_IDX = [
    {"name": "winner_uei_idx", "type": "BTree", "fields": ["winner_uei"]},
    {"name": "addr_hash_idx", "type": "BTree", "fields": ["addr_hash"]},
    {"name": "naics2_idx", "type": "Bitmap", "fields": ["naics2"]},
    {"name": "state_idx", "type": "Bitmap", "fields": ["state"]},
    {"name": "winner_type_idx", "type": "Bitmap", "fields": ["winner_type"]},
]
WINNERS_COLS = [
    "winner_uei", "winner_type", "winner_name", "naics_code", "naics2", "state",
    "total_obligation", "award_count", "last_action_date", "longitude", "latitude",
]

COMPANY_COLS = [
    "uei", "company_name", "industry", "employee_size_band", "company_type", "naics2",
    "primary_naics", "hq_city", "hq_state", "has_federal_awards", "total_active_obligations",
    "award_count", "physical_address_state", "is_active", "founded_year",
    "latest_award_action_date", "longitude", "latitude",
]
COMPANY_IDX = [
    {"name": "naics2_idx", "type": "Bitmap", "fields": ["naics2"]},
    {"name": "industry_idx", "type": "Bitmap", "fields": ["industry"]},
    {"name": "employee_size_band_idx", "type": "Bitmap", "fields": ["employee_size_band"]},
    {"name": "company_type_idx", "type": "Bitmap", "fields": ["company_type"]},
    {"name": "physical_address_state_idx", "type": "Bitmap", "fields": ["physical_address_state"]},
    {"name": "has_federal_awards_idx", "type": "Bitmap", "fields": ["has_federal_awards"]},
    {"name": "primary_naics_idx", "type": "BTree", "fields": ["primary_naics"]},
    {"name": "latest_award_action_date_idx", "type": "BTree", "fields": ["latest_award_action_date"]},
    {"name": "uei_idx", "type": "BTree", "fields": ["uei"]},  # extra (resolution key)
    {"name": "addr_hash_idx", "type": "BTree", "fields": ["addr_hash"]},  # extra
]


AWARDS_COLS = [
    "award_id", "winner_uei", "winner_name", "winner_type", "award_amount", "action_date",
    "naics2", "naics_code", "state", "city", "county", "pop_state", "pop_city",
    "awarding_agency", "awarding_sub_agency", "set_aside", "longitude", "latitude",
]
AWARDS_IDX = [
    {"name": "action_date_idx", "type": "BTree", "fields": ["action_date"]},
    {"name": "award_amount_idx", "type": "BTree", "fields": ["award_amount"]},
    {"name": "winner_uei_idx", "type": "BTree", "fields": ["winner_uei"]},
    {"name": "addr_hash_idx", "type": "BTree", "fields": ["addr_hash"]},  # extra
    {"name": "city_idx", "type": "BTree", "fields": ["city"]},
    {"name": "county_idx", "type": "BTree", "fields": ["county"]},
    {"name": "pop_city_idx", "type": "BTree", "fields": ["pop_city"]},
    {"name": "awarding_sub_agency_idx", "type": "BTree", "fields": ["awarding_sub_agency"]},
    {"name": "naics2_idx", "type": "Bitmap", "fields": ["naics2"]},
    {"name": "state_idx", "type": "Bitmap", "fields": ["state"]},
    {"name": "winner_type_idx", "type": "Bitmap", "fields": ["winner_type"]},
    {"name": "pop_state_idx", "type": "Bitmap", "fields": ["pop_state"]},
    {"name": "awarding_agency_idx", "type": "Bitmap", "fields": ["awarding_agency"]},
    {"name": "set_aside_idx", "type": "Bitmap", "fields": ["set_aside"]},
]


# ── pure schema/index checker ────────────────────────────────────────────────

def test_winners_complete_contract_clean():
    r = verify_decoder_contract(WINNERS_COLS, WINNERS_IDX, WINNERS)
    assert r["violations"] == [] and r["notes"] == []


def test_awards_complete_contract_clean():
    r = verify_decoder_contract(AWARDS_COLS, AWARDS_IDX, AWARDS)
    assert r["violations"] == [] and r["notes"] == []


def test_awards_missing_recency_index_is_violation():
    idx = [e for e in AWARDS_IDX if e["fields"] != ["action_date"]]
    v = verify_decoder_contract(AWARDS_COLS, idx, AWARDS)["violations"]
    assert any("NO live index covers" in x and "'action_date'" in x for x in v)


def test_company_complete_contract_clean():
    # query-name 'state' -> column physical_address_state; lookup must key on the column.
    r = verify_decoder_contract(COMPANY_COLS, COMPANY_IDX, COMPANY)
    assert r["violations"] == [] and r["notes"] == []


def test_missing_schema_column_is_violation():
    cols = [c for c in WINNERS_COLS if c != "state"]
    v = verify_decoder_contract(cols, WINNERS_IDX, WINNERS)["violations"]
    assert any("column 'state' missing" in s for s in v)


def test_missing_geometry_column_is_violation():
    cols = [c for c in WINNERS_COLS if c != "latitude"]
    v = verify_decoder_contract(cols, WINNERS_IDX, WINNERS)["violations"]
    assert any("geometry column 'latitude' missing" in s for s in v)


def test_missing_declared_index_is_violation():
    idx = [e for e in WINNERS_IDX if e["fields"] != ["naics2"]]
    v = verify_decoder_contract(WINNERS_COLS, idx, WINNERS)["violations"]
    assert any("NO live index covers" in s and "'naics2'" in s for s in v)


def test_mixed_case_type_does_not_false_positive():
    # All live types are mixed-case ('BTree'/'Bitmap'); declared are uppercase. A naive
    # case-sensitive compare would mark every index missing — this must stay clean.
    assert verify_decoder_contract(WINNERS_COLS, WINNERS_IDX, WINNERS)["violations"] == []


def test_extra_live_indexes_do_not_violate():
    # declared-⊆-actual: undeclared winner_uei_idx/addr_hash_idx must not produce violations.
    assert verify_decoder_contract(WINNERS_COLS, WINNERS_IDX, WINNERS)["violations"] == []


def test_type_mismatch_is_a_note_not_a_violation():
    # naics2 declared BITMAP but live as BTree: a NON-FATAL note, never a hard violation —
    # so strict mode must not abort boot on it (the gap #431 left). The column IS indexed,
    # so it must NOT read as a missing-index violation either.
    idx = [e if e["fields"] != ["naics2"] else {**e, "type": "BTree"} for e in WINNERS_IDX]
    r = verify_decoder_contract(WINNERS_COLS, idx, WINNERS)
    assert any("type-mismatch note, non-fatal" in s and "'naics2'" in s for s in r["notes"])
    assert r["violations"] == []


# ── pure live-stats classifier (row count + unindexed) ───────────────────────

def test_zero_rows_is_hard_violation():
    # A schema-perfect but EMPTY serving table — the failure mode #431 reported healthy.
    r = _live_stat_findings("company", 0, {})
    assert any("0 rows" in s for s in r["violations"])
    assert r["notes"] == []


def test_nonempty_no_unindexed_is_clean():
    r = _live_stat_findings("company", 243842, {"uei_idx": 0, "naics2_idx": 0})
    assert r["violations"] == [] and r["notes"] == []


def test_unindexed_rows_is_a_soft_note():
    # Rows appended since the index was built — degraded scan, NOT a correctness break.
    r = _live_stat_findings("company", 243842, {"naics2_idx": 500})
    assert r["violations"] == []
    assert any("unindexed rows" in s and "naics2_idx" in s for s in r["notes"])
