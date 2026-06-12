"""Unit tests for the Phase-1 regex extraction lane cores (sam_labor_demand_extract_90day) — pure
functions, no R2/Lance.

Covers (mandate item 5): each pattern family positive + negative, conservative negation suppression,
value normalization, requirement_id determinism, redaction-on-marked, delete-before-merge predicate
scoping, overlap-aware reassembly + offset→chunk_id attribution, and deterministic demand_id rank.

    python -m pytest pipelines/sam_gov/tests/test_sam_labor_demand_extract.py -q
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.sam_gov.sam_attachment_extract_90day import _chunk_text, _normalize_ws  # noqa: E402
from pipelines.sam_gov.sam_labor_demand_extract_90day import (  # noqa: E402
    MIN_OVERLAP, REGEX_LANE_VERSION,
    chunks_for_span, collapse_matches, extract_from_text, in_predicate,
    labor_delete_predicate, make_quote, norm_clearance, norm_dollars, parse_date,
    process_resource_payload, reassemble_with_offsets, requirement_id,
    requirements_delete_predicate, _overlap_len,
)

NOW = dt.datetime(2026, 6, 12, tzinfo=dt.timezone.utc)


def _types_values(matches):
    return {(m["requirement_type"], m["value_norm"]) for m in matches}


def _values(matches):
    return {m["value_norm"] for m in matches}


# ════════════════════════════════════════════════════════ reassembly (overlap-aware) + attribution
def _mk_rows(text: str, rid: str = "R1"):
    return [(ix, f"{rid}:{ix:04d}", piece) for ix, piece in _chunk_text(text)]


def test_reassembly_roundtrips_the_chunker():
    # Long synthetic text -> real chunker -> reassembly must reproduce the normalized text exactly.
    text = " ".join(f"sentence {i} about the performance work statement and general scope."
                    for i in range(400))
    rows = _mk_rows(text)
    assert len(rows) > 3                                    # actually chunked
    body, intervals = reassemble_with_offsets(rows)
    assert body == _normalize_ws(text)
    # interval map: body[start:end] == chunk text verbatim, for every chunk
    for (ix, cid, piece), (s, e, cid2) in zip(rows, intervals):
        assert cid == cid2
        assert body[s:e] == piece


def test_reassembly_no_overlap_joins_with_newline():
    rows = [(0, "R:0000", "alpha bravo"), (1, "R:0001", "charlie delta")]
    body, intervals = reassemble_with_offsets(rows)
    assert body == "alpha bravo\ncharlie delta"
    assert body[intervals[1][0]:intervals[1][1]] == "charlie delta"


def test_overlap_len_brute_force_equivalence():
    import random
    rnd = random.Random(42)
    alphabet = "ab cd"
    for _ in range(200):
        a = "".join(rnd.choice(alphabet) for _ in range(rnd.randint(MIN_OVERLAP, 180)))
        k_true = rnd.randint(0, min(len(a), 60))
        b = a[len(a) - k_true:] + "".join(rnd.choice(alphabet) for _ in range(40))
        got = _overlap_len(a, b)
        # brute force: largest k >= MIN_OVERLAP with a[-k:] == b[:k]
        want = 0
        for k in range(min(len(a), len(b)), MIN_OVERLAP - 1, -1):
            if a[-k:] == b[:k]:
                want = k
                break
        assert got == want


def test_span_attribution_boundary_straddle():
    rows = [(0, "R:0000", "aaaa bbbb"), (1, "R:0001", "cccc dddd")]
    body, intervals = reassemble_with_offsets(rows)        # joined with \n at offset 9
    starts = [iv[0] for iv in intervals]
    assert chunks_for_span(intervals, starts, 0, 4) == ["R:0000"]
    assert chunks_for_span(intervals, starts, 12, 16) == ["R:0001"]
    assert chunks_for_span(intervals, starts, 5, 14) == ["R:0000", "R:0001"]  # straddles


# ════════════════════════════════════════════════════════ pattern families: positive + negative
def test_clearance_personnel_levels():
    m = extract_from_text("All personnel shall possess an active Top Secret/SCI clearance.")
    assert ("clearance", "clearance:ts_sci") in _types_values(m)
    assert [x["clearance_level"] for x in m if x["value_norm"] == "clearance:ts_sci"] == ["TS_SCI"]
    m = extract_from_text("Contractor staff shall hold a Secret security clearance at award.")
    assert ("clearance", "clearance:secret") in _types_values(m)
    # negative: 'clearance' in a non-level sense produces no clearance row
    assert not _values(extract_from_text("Snow clearance of the parking area is required daily."))
    assert "clearance:secret" not in _values(extract_from_text("the secret to good clearance sales"))


def test_clearance_facility_and_fcl():
    m = extract_from_text("The offeror shall maintain a Secret facility clearance.")
    assert ("clearance", "facility_clearance:secret") in _types_values(m)
    m = extract_from_text("Contractor must possess an FCL prior to performance.")
    assert ("clearance", "facility_clearance") in _types_values(m)
    assert not _values(extract_from_text("the fcl abbreviation in lowercase"))   # uppercase-only


def test_certification_cmmc_iso_as9100():
    m = extract_from_text("The contractor shall be CMMC Level 2 certified at time of award.")
    assert _values(m) >= {"cmmc_l2"}
    assert "cmmc" not in _values(m)                       # leveled supersedes bare (post-rule)
    m = extract_from_text("CMMC compliance is required for all subcontractors.")
    assert "cmmc" in _values(m)
    m = extract_from_text("Vendor must hold ISO 9001:2015 and ISO/IEC 27001 registration. Required.")
    assert _values(m) >= {"iso_9001", "iso_27001"}
    m = extract_from_text("AS9100D certification is required for aviation suppliers.")
    assert "as9100" in _values(m)
    assert not _values(extract_from_text("the cmmc lowercase acronym is not matched here"))


def test_standard_citations():
    m = extract_from_text("Testing shall conform to MIL-STD-810G and NIST SP 800-171. "
                          "Site work must follow UFC 3-600-01 and ASTM E84, plus EM 385-1-1.")
    assert _values(m) >= {"mil-std-810g", "nist-800-171", "ufc-3-600-01", "astm-e84", "em-385-1-1"}
    m = extract_from_text("This contract is subject to the Service Contract Act and the "
                          "Davis-Bacon Act; Wage Determination No. 2015-4281 applies. Required.")
    assert _values(m) >= {"sca", "davis_bacon", "wd:2015-4281"}
    # negative: bare 'WD' without a number never rows; 'astm' lowercase never rows
    assert "wage_determination" not in _values(extract_from_text("WD is mentioned without number"))
    assert not _values(extract_from_text("astm e84 lowercase citation"))


def test_set_aside_context_gated():
    m = extract_from_text("This procurement is a 100% set-aside for SDVOSB concerns. Required.")
    assert ("vehicle_constraint", "set_aside:sdvosb") in _types_values(m)
    m = extract_from_text("This acquisition is reserved for HUBZone small business participation.")
    assert "set_aside:hubzone" in _values(m)
    m = extract_from_text("This is a total small business set-aside under FAR 19.5.")
    assert "set_aside:small_business" in _values(m)
    # negative: mention WITHOUT set-aside context is dropped (counted, not rowed)
    counters: dict = {}
    m = extract_from_text("The SDVOSB program office reviewed the document.", counters)
    assert "set_aside:sdvosb" not in _values(m)
    assert counters["no_context"].get("set_aside", 0) >= 1


def test_bonding_insurance_thresholds():
    m = extract_from_text("The contractor shall furnish a 100% performance bond and a payment bond.")
    assert _values(m) >= {"performance_bond:100pct", "payment_bond"}
    m = extract_from_text("General liability insurance of $1,000,000 per occurrence is required; "
                          "workers' compensation coverage shall be maintained.")
    assert _values(m) >= {"insurance:general_liability:1000000", "insurance:workers_compensation"}
    m = extract_from_text("A bid guarantee shall accompany the offer.")
    assert "bid_bond" in _values(m)
    assert not _values(extract_from_text("the chemical bond between molecules"))


def test_staffing_fte_headcount():
    m = extract_from_text("The contractor shall provide 5 FTEs for the duration of the contract.")
    assert [x for x in m if x["value_norm"] == "fte:5" and x["headcount"] == 5]
    m = extract_from_text("Staffing shall include five (5) full-time equivalents on site.")
    assert "fte:5" in _values(m)
    m = extract_from_text("A minimum of 12 personnel shall be on duty at all times.")
    assert [x for x in m if x["value_norm"] == "headcount:12" and x["headcount"] == 12]
    assert not _values(extract_from_text("the 5 ft equivalent length of pipe"))


def test_labor_category_demand_cue_gated():
    m = extract_from_text("The contractor shall provide 3 electricians and 2 plumbers, "
                          "each licensed in the state.")
    vals = _values(m)
    assert {"electrician", "plumber"} <= vals
    hc = {x["value_norm"]: x["headcount"] for x in m if x["family"] == "labor_category"}
    assert hc["electrician"] == 3 and hc["plumber"] == 2
    # negative: no demand cue in window -> dropped
    counters: dict = {}
    m = extract_from_text("An electrician visited the museum on Tuesday.", counters)
    assert "electrician" not in _values(m)
    assert counters["no_context"].get("labor_category", 0) >= 1


def test_labor_category_contained_span_drop_and_wage():
    text = ("The contractor shall provide qualified heavy equipment operators.\n"
            "ELECTRICIAN (qualified, required): $35.75 per hour\n")
    rows = [{"chunk_id": "R:0000", "resource_id": "R", "chunk_ix": 0, "text": text,
             "content_marking": [], "notice_id": "n", "solicitation_number": "s",
             "naics_code": "236220", "contract_award_unique_key": "CONT_AWD_X"}]
    out = process_resource_payload("R", rows, None, "run1", NOW)
    cats = {r["labor_category"]: r for r in out["labor_rows"]}
    assert "heavy_equipment_operator" in cats
    assert "equipment_operator" not in cats               # contained span dropped
    assert cats["electrician"]["wage_floor"] == 35.75      # same-line wage attaches


def test_pop_both_dates_required():
    m = extract_from_text("Period of performance: 01/01/2026 through 12/31/2026.")
    e = [x for x in m if x["family"] == "pop"]
    assert e and e[0]["value_norm"] == "period_of_performance:2026-01-01:2026-12-31"
    assert e[0]["pop_start"] == dt.date(2026, 1, 1) and e[0]["pop_end"] == dt.date(2026, 12, 31)
    m = extract_from_text("Period of performance is from June 1, 2026 to May 31, 2027.")
    assert any(x["value_norm"] == "period_of_performance:2026-06-01:2027-05-31" for x in m)
    # negative: unparseable / inverted ranges produce no row (counted invalid)
    counters: dict = {}
    m = extract_from_text("Period of performance: 13/45/2026 through 12/31/2026.", counters)
    assert not [x for x in m if x["family"] == "pop"]
    counters = {}
    m = extract_from_text("Period of performance: 12/31/2026 through 01/01/2026.", counters)
    assert not [x for x in m if x["family"] == "pop"]
    assert counters["invalid"].get("pop", 0) == 1


def test_license_family():
    m = extract_from_text("Work shall be performed by a licensed electrician.")
    assert ("license", "license:electrician") in _types_values(m)
    m = extract_from_text("The offeror shall hold a valid electrical license for the work.")
    assert "license:electrical" in _values(m)
    m = extract_from_text("The contractor must be licensed in the State of Texas prior to award.")
    assert "license:business:texas" in _values(m)
    assert not _values(extract_from_text("driver license renewals are out of scope"))


# ════════════════════════════════════════════════════════ negation (conservative, counted)
def test_negation_suppression_counts():
    counters: dict = {}
    m = extract_from_text("A Secret clearance is not required for this effort.", counters)
    assert "clearance:secret" not in _values(m)
    assert counters["negation"]["clearance"] == 1
    counters = {}
    m = extract_from_text("No performance bond shall be required.", counters)
    assert "performance_bond" not in _values(m)
    assert counters["negation"]["bonding_insurance"] >= 1
    counters = {}
    m = extract_from_text("Facility clearance is waived for the base period.", counters)
    assert not [x for x in m if x["family"] == "clearance"]
    assert counters["negation"]["clearance"] == 1


def test_negation_does_not_overfire_on_strengtheners():
    # 'no later than' / 'No.' are excluded from the negation cue set
    m = extract_from_text("Offerors shall obtain a Secret clearance no later than 30 days "
                          "after award.")
    assert "clearance:secret" in _values(m)
    m = extract_from_text("Wage Determination No. 2015-4281 applies to this requirement.")
    assert "wd:2015-4281" in _values(m)


def test_mandatory_shall_vs_should():
    m = extract_from_text("The contractor shall maintain ISO 9001 registration.")
    assert [x for x in m if x["value_norm"] == "iso_9001"][0]["mandatory"] is True
    m = extract_from_text("Offerors ideally maintain ISO 9001 registration if available.")
    assert [x for x in m if x["value_norm"] == "iso_9001"][0]["mandatory"] is False


# ════════════════════════════════════════════════════════ value normalization
def test_norm_clearance():
    assert norm_clearance("Top Secret/SCI") == "TS_SCI"
    assert norm_clearance("TS / SCI") == "TS_SCI"
    assert norm_clearance("top  secret") == "TOP_SECRET"
    assert norm_clearance("Secret") == "SECRET"
    assert norm_clearance("CONFIDENTIAL") == "CONFIDENTIAL"
    assert norm_clearance("Public Trust") == "PUBLIC_TRUST"
    assert norm_clearance("unclassified") is None
    assert norm_clearance(None) is None


def test_norm_dollars():
    assert norm_dollars("1,000,000") == "1000000"
    assert norm_dollars("1,000,000.00") == "1000000"
    assert norm_dollars("0500") == "500"
    assert norm_dollars("abc") is None


def test_parse_date_formats():
    assert parse_date("01/15/2026") == dt.date(2026, 1, 15)
    assert parse_date("1-5-26") == dt.date(2026, 1, 5)
    assert parse_date("2026-06-12") == dt.date(2026, 6, 12)
    assert parse_date("January 15, 2026") == dt.date(2026, 1, 15)
    assert parse_date("Sept. 3 2026") == dt.date(2026, 9, 3)
    assert parse_date("15 January 2026") == dt.date(2026, 1, 15)
    assert parse_date("02/30/2026") is None
    assert parse_date("Smarch 1, 2026") is None


# ════════════════════════════════════════════════════════ requirement_id determinism
def test_requirement_id_plan_exact_and_deterministic():
    import hashlib
    a = requirement_id("RID-1", "clearance", "clearance:secret")
    assert a == hashlib.sha256(b"RID-1|clearance|clearance:secret").hexdigest()[:24]
    assert requirement_id("RID-1", "clearance", "clearance:secret") == a       # stable
    assert requirement_id("RID-2", "clearance", "clearance:secret") != a       # resource-scoped
    assert requirement_id("RID-1", "clearance", "clearance:ts_sci") != a       # value-scoped
    assert len(a) == 24


# ════════════════════════════════════════════════════════ row assembly: redaction on marked
def _payload_rows(rid: str, text: str, marking: list[str]):
    return [{"chunk_id": f"{rid}:0000", "resource_id": rid, "chunk_ix": 0, "text": text,
             "content_marking": marking, "notice_id": "nid", "solicitation_number": "sol",
             "naics_code": "541512", "contract_award_unique_key": "CONT_AWD_1"}]


_REQ_TEXT = ("The contractor shall provide 3 electricians holding a Secret clearance. "
             "Place of performance: Fort Hood, TX. "
             "Period of performance: 01/01/2026 through 12/31/2026.")


def test_redaction_on_marked_resource():
    out = process_resource_payload("RM", _payload_rows("RM", _REQ_TEXT, ["cui"]), None, "run1", NOW)
    assert out["marked"] is True and out["marking_full_body"] == ["cui"]
    assert out["req_rows"]
    for row in out["req_rows"]:
        assert row["evidence_quote"] is None               # write-side egress gate
        assert row["requirement_detail"] is None
        assert row["place_of_performance_text"] is None
        assert row["marked_resource"] is True
        assert row["source_chunk_ids"]                     # structured fields stay
        assert row["validated"] is True
    for row in out["labor_rows"]:
        assert row["place_of_performance"] is None


def test_unmarked_resource_keeps_verbatim_fields():
    out = process_resource_payload("RU", _payload_rows("RU", _REQ_TEXT, []), None, "run1", NOW)
    assert out["marked"] is False
    clr = [r for r in out["req_rows"] if r["requirement_value"] == "clearance:secret"][0]
    assert clr["evidence_quote"] and "Secret clearance" in clr["evidence_quote"]
    assert len(clr["evidence_quote"]) <= 300
    assert clr["requirement_detail"]
    assert clr["marked_resource"] is False
    assert clr["confidence"] == 1.0
    assert clr["extractor"].startswith("regex:") and clr["extractor"].endswith(f"@{REGEX_LANE_VERSION}")
    assert clr["extractor_version"] == REGEX_LANE_VERSION
    pop = [r for r in out["req_rows"] if r["requirement_type"] == "deliverable"][0]
    assert pop["place_of_performance_text"] == "Fort Hood, TX."[:80].rstrip() or \
        pop["place_of_performance_text"].startswith("Fort Hood")
    lab = {r["labor_category"]: r for r in out["labor_rows"]}
    assert lab["electrician"]["headcount"] == 3
    assert lab["electrician"]["clearance_level"] == "SECRET"     # doc-level max attaches
    assert lab["electrician"]["pop_start"] == dt.date(2026, 1, 1)
    assert lab["electrician"]["place_of_performance"].startswith("Fort Hood")


# ════════════════════════════════════════════════════════ demand_id deterministic rank
def test_demand_id_rank_is_order_independent():
    text = ("The contractor shall provide qualified welders, electricians, and carpenters "
            "for the required work.")
    rows_a = _payload_rows("RD", text, [])
    out_a = process_resource_payload("RD", rows_a, None, "runA", NOW)
    # same text split across two chunks, reversed input order — ids must not move
    rows_b = [
        dict(rows_a[0], chunk_id="RD:0001", chunk_ix=1, text="zz tail filler text"),
        dict(rows_a[0], chunk_id="RD:0000", chunk_ix=0, text=text),
    ]
    out_b = process_resource_payload("RD", rows_b, None, "runB", NOW)
    ids_a = {r["labor_category"]: r["demand_id"] for r in out_a["labor_rows"]}
    ids_b = {r["labor_category"]: r["demand_id"] for r in out_b["labor_rows"]}
    assert ids_a == ids_b
    # rank over (labor_category_norm, first chunk_ix): alphabetical here
    assert ids_a == {"carpenter": "RD:0", "electrician": "RD:1", "welder": "RD:2"}


# ════════════════════════════════════════════════════════ pricing cells region
def test_pricing_cells_scanned_and_attributed_to_first_chunk():
    rows = _payload_rows("RP", "Price schedule attached.", [])
    cells = "ELECTRICIAN | $35.75\nWage Determination No. 2015-4281 applies. Required."
    out = process_resource_payload("RP", rows, cells, "run1", NOW)
    wd = [r for r in out["req_rows"] if r["requirement_value"] == "wd:2015-4281"]
    assert wd and wd[0]["source_chunk_ids"] == ["RP:0000"]
    assert "2015-4281" in wd[0]["evidence_quote"]          # evidence validates against cells


# ════════════════════════════════════════════════════════ delete-before-merge predicates
def test_requirements_delete_predicate_scoped_to_regex_lane():
    pred = requirements_delete_predicate(["r1", "o'neil"])
    assert "resource_id IN ('r1','o''neil')" in pred
    assert "extractor LIKE 'regex:%'" in pred              # LLM-lane rows are never touched
    assert pred.count("AND") == 1


def test_labor_delete_predicate_unscoped_per_plan():
    pred = labor_delete_predicate(["r1", "r2"])
    assert pred == "resource_id IN ('r1','r2')"
    assert "extractor" not in pred                          # plan Phase-1 verbatim


def test_in_predicate_escapes_quotes():
    assert in_predicate("resource_id", ["a'b"]) == "resource_id IN ('a''b')"


# ════════════════════════════════════════════════════════ misc cores
def test_make_quote_caps_at_300_and_contains_span():
    text = "x" * 1000
    q = make_quote(text, 500, 510)
    assert len(q) <= 300
    q = make_quote("short head MATCH tail", 11, 16)
    assert "MATCH" in q


def test_collapse_merges_duplicates_max_headcount_or_mandatory():
    matches = [
        {"family": "labor_category", "requirement_type": "labor_category", "value_norm": "electrician",
         "raw": "electricians", "mandatory": False, "headcount": 2, "clearance_level": None,
         "pop_start": None, "pop_end": None, "wage_floor": None, "labor_category": "electrician",
         "region": "text", "start": 10, "end": 22},
        {"family": "labor_category", "requirement_type": "labor_category", "value_norm": "electrician",
         "raw": "electricians", "mandatory": True, "headcount": 5, "clearance_level": None,
         "pop_start": None, "pop_end": None, "wage_floor": 35.75, "labor_category": "electrician",
         "region": "text", "start": 100, "end": 112},
    ]
    c = collapse_matches(matches)
    e = c[("labor_category", "electrician")]
    assert e["headcount"] == 5 and e["mandatory"] is True and e["wage_floor"] == 35.75
    assert e["first_start"] == 10 and len(e["spans"]) == 2
