"""Unit tests for the GovCon CAPABILITY Phase-1 leg (apps/gtm_mcp/src/tools/capability.py).

NO R2: the requirements sink may be empty while this ships, so the full grain ladder
(requirement rows → manifest award_keys explode → prime-txn collapse → recipient_uei
company grain) is exercised against LOCAL Lance fixture datasets. The requirements fixture
conforms to the FROZEN schema imported from ``pipelines/sam_gov/govcon_gtm_schemas.py``
(the schema is imported, never redeclared — drift here is drift against the real sink).
``database.open_dataset`` / ``database.get_connection`` are monkeypatched to the local
fixtures + a credential-free DuckDB; the production module path is otherwise untouched.

Pinned invariants:
  * conjunction is enforced at AWARD grain over the EXPLODED manifest award_keys (an award
    covered by docs witnessing only a subset of legs never returns);
  * redaction: marked-resource rows surface with ``marked: true`` and ``evidence_quote`` /
    ``requirement_detail`` null EVEN IF the stored row carries text (defense in depth —
    verbatim marked text never leaves a serving response);
  * coverage honesty: every response (including empty/no-match) carries the
    no-match-means-no-document caveat + measured denominators;
  * input guards (enum, NAICS, leg shape) raise before any dataset is touched;
  * the evidence projection is a subset of the frozen schema (no phantom columns).
"""

from __future__ import annotations

import json

import duckdb
import lance
import pyarrow as pa
import pytest

from apps.gtm_mcp.src.tools import capability
from apps.gtm_mcp.src import database
from pipelines.sam_gov.govcon_gtm_schemas import requirements_schema

# ── Fixture corpus ────────────────────────────────────────────────────────────
# Resources: RA (both legs, awards AW1/AW2/AW6), RB (clearance only, AW3 — must NOT match),
# RC (both legs, MARKED, award AW4 — must match with redacted evidence), RD (no awards).
# AW6 is absent from the txn feed → resolution rate 3/4. AW1 has two txns (collapse test).
_LEGS = [
    {"requirement_type": "clearance", "clearance_level": "TS_SCI"},
    {"requirement_type": "certification", "value": "CMMC_L2"},
]

_LEAK = "VERBATIM-MARKED-TEXT-MUST-NOT-LEAK"


def _req_row(rid, rtype, value, **kw):
    base = {
        "requirement_id": kw.get("requirement_id", f"{rid}-{rtype}-{value}"),
        "resource_id": rid, "notice_id": f"N-{rid}", "solicitation_number": f"S-{rid}",
        "naics_code": kw.get("naics_code", "541512"),
        "contract_award_unique_key": None,  # inline scalar deliberately unused by the ladder
        "requirement_type": rtype, "requirement_value": value,
        "requirement_detail": kw.get("requirement_detail", f"detail for {value}"),
        "mandatory": kw.get("mandatory", True), "headcount": None,
        "clearance_level": kw.get("clearance_level"),
        "pop_start": None, "pop_end": None, "place_of_performance_text": None,
        "wage_floor": None,
        "source_chunk_ids": kw.get("source_chunk_ids", [f"{rid}:0001", f"{rid}:0002"]),
        "evidence_quote": kw.get("evidence_quote", f"the contractor shall provide {value}"),
        "validated": kw.get("validated", True),
        "marked_resource": kw.get("marked_resource", False),
        "coverage_truncated": False,
        "extractor": "regex:v1", "extractor_version": "regex:v1",
        "confidence": 1.0, "run_id": "test-run", "created_at": None,
    }
    return base


def _requirements_rows():
    return [
        # RA — both legs
        _req_row("RA", "clearance", "TS_SCI", clearance_level="TS_SCI"),
        _req_row("RA", "certification", "CMMC_L2"),
        # RB — clearance only (conjunction must exclude its award AW3)
        _req_row("RB", "clearance", "TS_SCI", clearance_level="TS_SCI"),
        # RC — both legs, MARKED, with leak-bait text simulating a writer-gate miss
        _req_row("RC", "clearance", "TS_SCI", clearance_level="TS_SCI",
                 marked_resource=True, evidence_quote=_LEAK, requirement_detail=_LEAK),
        _req_row("RC", "certification", "CMMC_L2",
                 marked_resource=True, evidence_quote=_LEAK, requirement_detail=_LEAK),
        # unvalidated row on RB's missing leg — must NOT complete the conjunction
        _req_row("RB", "certification", "CMMC_L2", validated=False),
        # different NAICS row (for the naics-filter test)
        _req_row("RD", "clearance", "TS_SCI", clearance_level="TS_SCI", naics_code="236220"),
    ]


_MANIFEST_SCHEMA = pa.schema([
    ("resource_id", pa.string()),
    ("award_keys", pa.list_(pa.string())),
])


def _manifest_rows():
    return [
        {"resource_id": "RA", "award_keys": ["AW1", "AW2", "AW6"]},
        {"resource_id": "RB", "award_keys": ["AW3"]},
        {"resource_id": "RC", "award_keys": ["AW4"]},
        {"resource_id": "RD", "award_keys": []},  # empty explode → contributes nothing
    ]


_TXN_SCHEMA = pa.schema([(c, pa.string()) for c in capability._TXN_COLUMNS])


def _txn_row(ak, txn_key, uei, name, lmd, dollars):
    return {
        "contract_award_unique_key": ak, "contract_transaction_unique_key": txn_key,
        "recipient_uei": uei, "recipient_name": name, "recipient_parent_uei": f"P-{uei}",
        "naics_code": "541512", "awarding_agency_name": "GSA",
        "type_of_set_aside": "NONE", "product_or_service_code": "D399",
        "period_of_performance_start_date": "2026-01-01",
        "period_of_performance_current_end_date": "2026-12-31",
        "current_total_value_of_award": dollars, "total_dollars_obligated": dollars,
        "action_date": "2026-03-01", "last_modified_date": lmd,
    }


def _txn_rows():
    return [
        # AW1: two txns — collapse must pick the LATER last_modified_date (name v2)
        _txn_row("AW1", "T1", "UEI1", "ACME FEDERAL v1", "2026-04-01", "100000"),
        _txn_row("AW1", "T2", "UEI1", "ACME FEDERAL v2", "2026-05-01", "150000"),
        _txn_row("AW2", "T3", "UEI1", "ACME FEDERAL v2", "2026-05-02", "200000"),
        _txn_row("AW4", "T4", "UEI2", "MARKED CORP", "2026-05-03", "50000"),
        _txn_row("AW3", "T5", "UEI3", "WRONG MATCH LLC", "2026-05-04", "999999"),
        # AW6 deliberately absent → resolution rate 3/4
    ]


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    """Local Lance fixtures + credential-free DuckDB wired under the production module."""
    req_tbl = pa.Table.from_pylist(_requirements_rows(), schema=requirements_schema())
    man_tbl = pa.Table.from_pylist(_manifest_rows(), schema=_MANIFEST_SCHEMA)
    txn_tbl = pa.Table.from_pylist(_txn_rows(), schema=_TXN_SCHEMA)
    paths = {
        capability.REQUIREMENTS: str(tmp_path / "req.lance"),
        capability.MANIFEST: str(tmp_path / "manifest.lance"),
        capability.PRIME_TXN: str(tmp_path / "txn.lance"),
    }
    lance.write_dataset(req_tbl, paths[capability.REQUIREMENTS], mode="create")
    lance.write_dataset(man_tbl, paths[capability.MANIFEST], mode="create")
    lance.write_dataset(txn_tbl, paths[capability.PRIME_TXN], mode="create")

    def fake_open(name):
        if name not in paths:
            raise KeyError(name)
        return lance.dataset(paths[name])

    monkeypatch.setattr(database, "open_dataset", fake_open)
    monkeypatch.setattr(database, "get_connection", lambda: duckdb.connect(":memory:"))
    return paths


# ── Schema contract: projections come from the FROZEN schema (imported, not redeclared) ──
def test_evidence_projection_is_subset_of_frozen_schema():
    frozen = set(requirements_schema().names)
    assert set(capability._REQ_EVIDENCE_COLUMNS) <= frozen
    # the verbatim free-text egress fields are both in the projection (so redaction has
    # something to enforce) and both are NULLed for marked rows (asserted end-to-end below)
    assert {"evidence_quote", "requirement_detail"} <= set(capability._REQ_EVIDENCE_COLUMNS)


# ── Input guards (raise before any dataset is touched) ───────────────────────
def test_leg_validation_guards():
    with pytest.raises(ValueError):
        capability._normalize_legs([])
    with pytest.raises(ValueError):
        capability._normalize_legs(None)
    with pytest.raises(ValueError):
        capability._normalize_legs([{"requirement_type": "not_a_type"}])
    with pytest.raises(ValueError):
        capability._normalize_legs([{"requirement_type": "clearance", "bogus_key": 1}])
    with pytest.raises(ValueError):
        capability._normalize_legs([{"requirement_type": "clearance", "mandatory": "yes"}])
    with pytest.raises(ValueError):
        capability._normalize_legs([{"requirement_type": "clearance"}] * (capability._MAX_LEGS + 1))
    # a well-formed pair normalizes cleanly
    legs = capability._normalize_legs(_LEGS)
    assert len(legs) == 2 and legs[0]["clearance_level"] == "TS_SCI"


def test_naics_guard():
    assert capability._norm_naics(None) is None
    assert capability._norm_naics(" 5415 ") == "5415"
    for bad in ("5", "54151200", "54A1", "evil' OR '1'='1"):
        with pytest.raises(ValueError):
            capability._norm_naics(bad)


def test_leg_filter_shape_pins_trust_boundary():
    pred = capability._leg_filter(
        {"requirement_type": "clearance", "clearance_level": "TS_SCI", "mandatory": True},
        "5415",
    )
    assert "validated = true" in pred                       # hard-coded, no toggle
    assert "requirement_type = 'clearance'" in pred
    assert "clearance_level = 'TS_SCI'" in pred
    assert "mandatory = true" in pred
    assert "naics_code LIKE '5415%'" in pred                # prefix → LIKE; 6-digit → equality
    pred6 = capability._leg_filter({"requirement_type": "certification"}, "541512")
    assert "naics_code = '541512'" in pred6
    assert capability._sql_str("O'Brien") == "'O''Brien'"


# ── End-to-end grain ladder over local fixtures ───────────────────────────────
def test_conjunction_to_company_grain(wired):
    out = capability.govcon_companies_by_requirements(_LEGS)
    assert out["status"] == "ok"
    # conjunction: AW1/AW2/AW6 (RA) + AW4 (RC); AW3 (RB, one leg) excluded — the
    # unvalidated CMMC row on RB must not have completed it
    assert out["measured"]["awards_conjunction"] == 4
    assert out["measured"]["awards_resolved_in_txn"] == 3   # AW6 absent from txn
    assert out["measured"]["award_resolution_rate"] == 0.75
    by_uei = {c["recipient_uei"]: c for c in out["companies"]}
    assert set(by_uei) == {"UEI1", "UEI2"}                  # UEI3 (AW3) never surfaces
    # company ordering: UEI1 (2 covered awards) before UEI2 (1)
    assert out["companies"][0]["recipient_uei"] == "UEI1"
    acme = by_uei["UEI1"]
    assert acme["covered_award_count"] == 2
    assert acme["recipient_name"] == "ACME FEDERAL v2"      # txn collapse picked latest mod
    assert acme["total_dollars_obligated_sum"] == 350000.0  # 150000 (latest AW1 txn) + 200000
    # awards sorted by dollars desc within the company
    assert [a["contract_award_unique_key"] for a in acme["awards"]] == ["AW2", "AW1"]


def test_evidence_carries_citations_and_both_legs(wired):
    out = capability.govcon_companies_by_requirements(_LEGS)
    acme = next(c for c in out["companies"] if c["recipient_uei"] == "UEI1")
    aw1 = next(a for a in acme["awards"] if a["contract_award_unique_key"] == "AW1")
    legs_seen = {e["leg_ix"] for e in aw1["evidence"]}
    assert legs_seen == {0, 1}                              # every leg witnessed, cited
    for e in aw1["evidence"]:
        assert e["source_chunk_ids"]                        # grounding, non-negotiable
        assert e["requirement_id"]
        assert e["marked"] is False
        assert e["evidence_quote"] and "shall provide" in e["evidence_quote"]
    assert aw1["marked_award"] is False


def test_marked_rows_redacted_and_flagged_never_leak(wired):
    out = capability.govcon_companies_by_requirements(_LEGS)
    marked_co = next(c for c in out["companies"] if c["recipient_uei"] == "UEI2")
    assert marked_co["any_marked_evidence"] is True
    aw4 = marked_co["awards"][0]
    assert aw4["marked_award"] is True
    assert aw4["evidence"]                                   # rows surface…
    for e in aw4["evidence"]:
        assert e["marked"] is True
        assert e["evidence_quote"] is None                   # …with quote NULL
        assert e["requirement_detail"] is None               # …and detail NULL
        assert e["source_chunk_ids"]                         # structured citation stays
        assert e["requirement_value"] is not None            # enum/structured fields stay
    # defense in depth: the leak-bait stored text appears NOWHERE in the response
    assert _LEAK not in json.dumps(out, default=str)


def test_coverage_caveat_on_every_response(wired):
    ok = capability.govcon_companies_by_requirements(_LEGS)
    nm = capability.govcon_companies_by_requirements(
        [{"requirement_type": "clearance", "clearance_level": "NO_SUCH_LEVEL"}])
    assert nm["status"] == "no_matches" and nm["companies"] == []
    for resp in (ok, nm):
        cov = resp["coverage"]
        assert "no document, not no need" in cov["caveat"]
        assert cov["text_covered_awards_ceiling"] == 35_002      # exploded grain, canonical
        assert cov["inline_key_awards_floor"] == 10_729          # never mixed
        assert cov["trailing_90d_prime_awards"] == 1_119_355
    assert ok["measured"]["leg_requirement_rows"] == [4, 2]      # RA+RB+RC+RD / RA+RC


def test_conjunction_empty_when_no_single_award_has_all_legs(wired):
    # past_performance exists nowhere → leg dead → no_matches with per-leg measurement
    out = capability.govcon_companies_by_requirements(
        [{"requirement_type": "clearance", "clearance_level": "TS_SCI"},
         {"requirement_type": "past_performance"}])
    assert out["status"] == "no_matches"
    assert out["measured"]["leg_requirement_rows"][1] == 0


def test_naics_filter_restricts_legs(wired):
    # NAICS 2362 keeps only RD's clearance row; RD explodes to zero awards → no_matches
    out = capability.govcon_companies_by_requirements(
        [{"requirement_type": "clearance", "clearance_level": "TS_SCI"}], naics="2362")
    assert out["status"] == "no_matches"
    assert out["measured"]["leg_requirement_rows"] == [1]
    assert out["measured"]["awards_conjunction"] == 0


def test_empty_sink_status(tmp_path, monkeypatch):
    path = str(tmp_path / "empty_req.lance")
    lance.write_dataset(requirements_schema().empty_table(), path, mode="create")
    monkeypatch.setattr(database, "open_dataset",
                        lambda name: lance.dataset(path) if name == capability.REQUIREMENTS
                        else (_ for _ in ()).throw(KeyError(name)))
    out = capability.govcon_companies_by_requirements(_LEGS)
    assert out["status"] == "requirements_sink_empty"
    assert "no document, not no need" in out["coverage"]["caveat"]
    facets = capability.govcon_requirement_facets()
    assert facets["status"] == "requirements_sink_empty" and facets["rows"] == []


def test_dataset_unavailable_status(monkeypatch):
    monkeypatch.setattr(database, "open_dataset",
                        lambda name: (_ for _ in ()).throw(KeyError(name)))
    out = capability.govcon_companies_by_requirements(_LEGS)
    assert out["status"] == "dataset_unavailable"


# ── Facet discovery ───────────────────────────────────────────────────────────
def test_requirement_facets_groups_normalized_vocabulary(wired):
    out = capability.govcon_requirement_facets()
    assert out["status"] == "ok"
    rows = {(r["requirement_type"], r["requirement_value"]): r for r in out["rows"]}
    # 4 validated TS_SCI clearance rows (RA, RB, RC, RD) over 4 resources; the
    # unvalidated RB certification row is excluded from the CMMC count (2: RA + RC)
    assert rows[("clearance", "TS_SCI")]["n_rows"] == 4
    assert rows[("clearance", "TS_SCI")]["n_resources"] == 4
    assert rows[("certification", "CMMC_L2")]["n_rows"] == 2
    assert "no document, not no need" in out["coverage"]["caveat"]
    with pytest.raises(ValueError):
        capability.govcon_requirement_facets(requirement_type="bogus")


def test_facets_filter_by_type_and_naics(wired):
    out = capability.govcon_requirement_facets(requirement_type="clearance", naics="236220")
    assert out["status"] == "ok"
    assert out["group_count"] == 1
    assert out["rows"][0]["n_rows"] == 1                     # only RD's row


# ── Registration: distinct group, no collision with existing tool groups ──────
def test_register_mounts_exactly_the_phase1_surface():
    registered: list[str] = []

    class _MCP:
        def add_tool(self, fn):
            registered.append(fn.__name__)

    capability.register(_MCP())
    assert set(registered) == {"govcon_companies_by_requirements", "govcon_requirement_facets"}

    from apps.gtm_mcp.src.tools import federal, govcon

    other: list[str] = []

    class _OTHER:
        def add_tool(self, fn):
            other.append(fn.__name__)

    federal.register(_OTHER())
    govcon.register(_OTHER())
    assert set(registered).isdisjoint(set(other))
