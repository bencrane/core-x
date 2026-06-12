"""Hermetic tests for the boot decoder schema/index contract check (R-09, §7-5).

Exercises the PURE checker (verify_decoder_contract) with synthetic schema + index
inventories in the REAL ds.list_indices() shape (dicts with a 'fields' list and a
mixed-case 'type'). No R2 access.
"""

from apps.catalyst_api.src.lance_store import verify_decoder_contract
from apps.catalyst_api.src.map_decoders import COMPANY, WINNERS

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
    "award_count", "physical_address_state", "is_active", "founded_year", "longitude", "latitude",
]
COMPANY_IDX = [
    {"name": "naics2_idx", "type": "Bitmap", "fields": ["naics2"]},
    {"name": "industry_idx", "type": "Bitmap", "fields": ["industry"]},
    {"name": "employee_size_band_idx", "type": "Bitmap", "fields": ["employee_size_band"]},
    {"name": "company_type_idx", "type": "Bitmap", "fields": ["company_type"]},
    {"name": "physical_address_state_idx", "type": "Bitmap", "fields": ["physical_address_state"]},
    {"name": "has_federal_awards_idx", "type": "Bitmap", "fields": ["has_federal_awards"]},
    {"name": "primary_naics_idx", "type": "BTree", "fields": ["primary_naics"]},
    {"name": "uei_idx", "type": "BTree", "fields": ["uei"]},  # extra (resolution key)
    {"name": "addr_hash_idx", "type": "BTree", "fields": ["addr_hash"]},  # extra
]


def test_winners_complete_contract_no_violations():
    assert verify_decoder_contract(WINNERS_COLS, WINNERS_IDX, WINNERS) == []


def test_company_complete_contract_no_violations():
    # query-name 'state' -> column physical_address_state; lookup must key on the column.
    assert verify_decoder_contract(COMPANY_COLS, COMPANY_IDX, COMPANY) == []


def test_missing_schema_column_is_violation():
    cols = [c for c in WINNERS_COLS if c != "state"]
    v = verify_decoder_contract(cols, WINNERS_IDX, WINNERS)
    assert any("column 'state' missing" in s for s in v)


def test_missing_geometry_column_is_violation():
    cols = [c for c in WINNERS_COLS if c != "latitude"]
    v = verify_decoder_contract(cols, WINNERS_IDX, WINNERS)
    assert any("geometry column 'latitude' missing" in s for s in v)


def test_missing_declared_index_is_violation():
    idx = [e for e in WINNERS_IDX if e["fields"] != ["naics2"]]
    v = verify_decoder_contract(WINNERS_COLS, idx, WINNERS)
    assert any("NO live index covers" in s and "'naics2'" in s for s in v)


def test_mixed_case_type_does_not_false_positive():
    # All live types are mixed-case ('BTree'/'Bitmap'); declared are uppercase. A naive
    # case-sensitive compare would mark every index missing — this must stay clean.
    assert verify_decoder_contract(WINNERS_COLS, WINNERS_IDX, WINNERS) == []


def test_extra_live_indexes_do_not_violate():
    # declared-⊆-actual: undeclared winner_uei_idx/addr_hash_idx must not produce violations.
    assert verify_decoder_contract(WINNERS_COLS, WINNERS_IDX, WINNERS) == []


def test_type_mismatch_is_soft_note_not_missing():
    # naics2 declared BITMAP but live as BTree: a non-fatal note, and NOT a "missing index".
    idx = [
        e if e["fields"] != ["naics2"] else {**e, "type": "BTree"}
        for e in WINNERS_IDX
    ]
    v = verify_decoder_contract(WINNERS_COLS, idx, WINNERS)
    assert any("type-mismatch note, non-fatal" in s and "'naics2'" in s for s in v)
    assert not any("NO live index covers" in s and "'naics2'" in s for s in v)
