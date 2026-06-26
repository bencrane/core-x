"""Hermetic tests for the boot decoder schema/index contract check (R-09, §7-5).

Exercises the PURE checkers — ``verify_decoder_contract`` (schema/index, synthetic inputs in
the REAL ds.list_indices() shape) and ``_live_stat_findings`` (row count + unindexed-row
classification). No R2 access. Both return ``{"violations": [...], "notes": [...]}``: hard
violations abort boot under strict mode + flip /healthz to 503; soft notes are advisory only.
"""

from apps.catalyst_api.src.lance_store import _live_stat_findings, verify_decoder_contract
from apps.catalyst_api.src.map_decoders import ACTIVE, AWARDS, COMPANY, WINNERS

# Live index inventory in the real list_indices() shape: dict per entry, 'fields' is the
# physical-column list, 'type' is mixed-case. Includes the resolution-key indexes the
# decoder does NOT declare (winner_uei_idx, addr_hash_idx) to lock in declared-⊆-actual.
WINNERS_IDX = [
    {"name": "winner_uei_idx", "type": "BTree", "fields": ["winner_uei"]},  # extra (resolution key)
    {"name": "addr_hash_idx", "type": "BTree", "fields": ["addr_hash"]},    # extra
    {"name": "entity_obligated_usd_idx", "type": "BTree", "fields": ["entity_obligated_usd"]},
    {"name": "award_count_idx", "type": "BTree", "fields": ["award_count"]},
    {"name": "last_action_date_idx", "type": "BTree", "fields": ["last_action_date"]},
    # winners.v7 SUB-only teaming BTREE indexes (teaming_dollars_5y / n_teaming_primes axes).
    {"name": "teaming_dollars_5y_idx", "type": "BTree", "fields": ["teaming_dollars_5y"]},
    {"name": "n_teaming_primes_idx", "type": "BTree", "fields": ["n_teaming_primes"]},
    {"name": "naics2_idx", "type": "Bitmap", "fields": ["naics2"]},
    {"name": "state_idx", "type": "Bitmap", "fields": ["state"]},
    {"name": "winner_type_idx", "type": "Bitmap", "fields": ["winner_type"]},
    # PHASE-3 capability BITMAP indexes (the gated scalar axes).
    {"name": "has_extracted_scope_idx", "type": "Bitmap", "fields": ["has_extracted_scope"]},
    {"name": "requires_clearance_idx", "type": "Bitmap", "fields": ["requires_clearance"]},
    {"name": "requires_cmmc_idx", "type": "Bitmap", "fields": ["requires_cmmc"]},
    {"name": "req_clearance_level_max_idx", "type": "Bitmap", "fields": ["req_clearance_level_max"]},
]
WINNERS_COLS = [
    "winner_uei", "winner_name", "winner_type", "naics_code", "naics2", "state",
    "entity_obligated_usd", "award_count", "last_action_date",
    # PHASE-3 capability columns (rolled from govcon_award_solicitation_profiles).
    "has_extracted_scope", "requires_clearance", "req_clearance_level_max", "requires_cmmc",
    "solicitation_scope_tags", "labor_categories", "covered_award_count", "covered_award_keys",
    # winners.v7 SUB-only teaming + self-reported axes (null on prime rows).
    "teaming_dollars_5y", "n_teaming_primes", "teaming_prime_names",
    "subaward_description_tags", "req_cert_tags",
    "longitude", "latitude",
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
    {"name": "is_active_idx", "type": "Bitmap", "fields": ["is_active"]},
    {"name": "primary_naics_idx", "type": "BTree", "fields": ["primary_naics"]},
    {"name": "founded_year_idx", "type": "BTree", "fields": ["founded_year"]},
    {"name": "total_active_obligations_idx", "type": "BTree", "fields": ["total_active_obligations"]},
    {"name": "award_count_idx", "type": "BTree", "fields": ["award_count"]},
    {"name": "latest_award_action_date_idx", "type": "BTree", "fields": ["latest_award_action_date"]},
    {"name": "uei_idx", "type": "BTree", "fields": ["uei"]},  # extra (resolution key)
    {"name": "addr_hash_idx", "type": "BTree", "fields": ["addr_hash"]},  # extra
]


AWARDS_COLS = [
    "award_id", "winner_uei", "winner_name", "winner_type", "action_obligated_usd", "action_date",
    "fiscal_year", "naics2", "naics_code", "psc_category", "psc_code", "state", "city", "county",
    "pop_state", "pop_city",
    "awarding_agency", "awarding_sub_agency", "set_aside", "business_size",
    "action_type", "is_option_exercise", "is_active", "pop_end",
    # awards.v9 GTM label axes (BITMAP) + what_was_done display column (free-text, NOT indexed).
    "vertical", "work_type", "equipment_intensity", "what_was_done",
    "longitude", "latitude",
]
AWARDS_IDX = [
    {"name": "action_date_idx", "type": "BTree", "fields": ["action_date"]},
    {"name": "action_obligated_usd_idx", "type": "BTree", "fields": ["action_obligated_usd"]},
    {"name": "winner_uei_idx", "type": "BTree", "fields": ["winner_uei"]},
    {"name": "addr_hash_idx", "type": "BTree", "fields": ["addr_hash"]},  # extra
    {"name": "city_idx", "type": "BTree", "fields": ["city"]},
    {"name": "county_idx", "type": "BTree", "fields": ["county"]},
    {"name": "pop_city_idx", "type": "BTree", "fields": ["pop_city"]},
    {"name": "awarding_sub_agency_idx", "type": "BTree", "fields": ["awarding_sub_agency"]},
    {"name": "psc_code_idx", "type": "BTree", "fields": ["psc_code"]},
    {"name": "naics2_idx", "type": "Bitmap", "fields": ["naics2"]},
    {"name": "state_idx", "type": "Bitmap", "fields": ["state"]},
    {"name": "winner_type_idx", "type": "Bitmap", "fields": ["winner_type"]},
    {"name": "pop_state_idx", "type": "Bitmap", "fields": ["pop_state"]},
    {"name": "awarding_agency_idx", "type": "Bitmap", "fields": ["awarding_agency"]},
    {"name": "set_aside_idx", "type": "Bitmap", "fields": ["set_aside"]},
    {"name": "is_active_idx", "type": "Bitmap", "fields": ["is_active"]},
    {"name": "psc_category_idx", "type": "Bitmap", "fields": ["psc_category"]},
    {"name": "fiscal_year_idx", "type": "Bitmap", "fields": ["fiscal_year"]},
    {"name": "business_size_idx", "type": "Bitmap", "fields": ["business_size"]},
    {"name": "action_type_idx", "type": "Bitmap", "fields": ["action_type"]},
    {"name": "is_option_exercise_idx", "type": "Bitmap", "fields": ["is_option_exercise"]},
    # awards.v9 GTM label axes — BITMAP (low cardinality), award-grain. what_was_done is a
    # free-text DISPLAY column and is intentionally NOT indexed (no entry here).
    {"name": "vertical_idx", "type": "Bitmap", "fields": ["vertical"]},
    {"name": "work_type_idx", "type": "Bitmap", "fields": ["work_type"]},
    {"name": "equipment_intensity_idx", "type": "Bitmap", "fields": ["equipment_intensity"]},
]


ACTIVE_COLS = [
    "award_id", "winner_uei", "winner_name", "current_value", "potential_value", "obligated",
    "pop_current_end", "pop_potential_end", "has_option_tail", "naics2", "naics_code",
    "psc_category", "psc_code", "state", "pop_state", "awarding_agency", "awarding_sub_agency",
    "set_aside", "business_size", "award_or_idv_flag",
    # active.v2 GTM label axes (BITMAP) + what_was_done display column (free-text, NOT indexed).
    "vertical", "work_type", "equipment_intensity", "what_was_done",
    "longitude", "latitude",
]
ACTIVE_IDX = [
    # days_until_expiry → the pop_current_end BTREE (the forward recompete axis).
    {"name": "pop_current_end_idx", "type": "BTree", "fields": ["pop_current_end"]},
    {"name": "pop_potential_end_idx", "type": "BTree", "fields": ["pop_potential_end"]},  # extra
    {"name": "current_value_idx", "type": "BTree", "fields": ["current_value"]},
    {"name": "potential_value_idx", "type": "BTree", "fields": ["potential_value"]},
    {"name": "obligated_idx", "type": "BTree", "fields": ["obligated"]},
    {"name": "winner_uei_idx", "type": "BTree", "fields": ["winner_uei"]},  # extra (resolution key)
    {"name": "addr_hash_idx", "type": "BTree", "fields": ["addr_hash"]},  # extra
    {"name": "naics_code_idx", "type": "BTree", "fields": ["naics_code"]},
    {"name": "psc_code_idx", "type": "BTree", "fields": ["psc_code"]},
    {"name": "awarding_sub_agency_idx", "type": "BTree", "fields": ["awarding_sub_agency"]},
    {"name": "naics2_idx", "type": "Bitmap", "fields": ["naics2"]},
    {"name": "psc_category_idx", "type": "Bitmap", "fields": ["psc_category"]},
    {"name": "state_idx", "type": "Bitmap", "fields": ["state"]},
    {"name": "pop_state_idx", "type": "Bitmap", "fields": ["pop_state"]},
    {"name": "set_aside_idx", "type": "Bitmap", "fields": ["set_aside"]},
    {"name": "business_size_idx", "type": "Bitmap", "fields": ["business_size"]},
    {"name": "has_option_tail_idx", "type": "Bitmap", "fields": ["has_option_tail"]},
    {"name": "award_or_idv_flag_idx", "type": "Bitmap", "fields": ["award_or_idv_flag"]},
    {"name": "awarding_agency_idx", "type": "Bitmap", "fields": ["awarding_agency"]},
    # active.v2 GTM label axes — BITMAP (low cardinality), award-grain. what_was_done is a
    # free-text DISPLAY column and is intentionally NOT indexed (no entry here).
    {"name": "vertical_idx", "type": "Bitmap", "fields": ["vertical"]},
    {"name": "work_type_idx", "type": "Bitmap", "fields": ["work_type"]},
    {"name": "equipment_intensity_idx", "type": "Bitmap", "fields": ["equipment_intensity"]},
]


# ── pure schema/index checker ────────────────────────────────────────────────

def test_active_complete_contract_clean():
    r = verify_decoder_contract(ACTIVE_COLS, ACTIVE_IDX, ACTIVE)
    assert r["violations"] == [] and r["notes"] == []


def test_active_missing_expiry_index_is_violation():
    idx = [e for e in ACTIVE_IDX if e["fields"] != ["pop_current_end"]]
    v = verify_decoder_contract(ACTIVE_COLS, idx, ACTIVE)["violations"]
    assert any("NO live index covers" in x and "'pop_current_end'" in x for x in v)


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
