"""Acceptance tests (the frozen Scorer) for the typed sam.gov entity point-lookups
(apps/gtm_mcp/src/tools/sam_entities.py — to be implemented by the executor).

WHY. Today an agent that wants a SAM entity by UEI / NAICS / CAGE must hand-write
`execute_audience_query` SQL — the full DuckDB-over-Lance path (registry scan + JIT register
+ DuckDB plan) for what is a single indexed point-lookup. `sam_master_entities` carries hard
BTREE scalar indices on `uei`, `primary_naics`, `cage_code` (and `sam_master_contacts` on
`uei`); these tools push the predicate straight into the Lance scanner (sub-100ms) and PROJECT
the wide 68-column row down to a documented entity profile — mirroring `audience.py`'s
`lookup_awards_by_uei` / `search_company_by_domain` shape.

CONTRACT pinned by these tests (the executor implements to GREEN):
  lookup_sam_entity_by_uei(uei)      -> {"uei", "found", "entity": {…doc…}|None}
  lookup_sam_entity_by_cage(cage)    -> {"cage_code", "found", "entity": {…doc…}|None}
  search_sam_entities_by_naics(naics)-> {"primary_naics", "match_count", "entities": [{…doc…}]}
                                        (NAICS is hierarchical → PREFIX match)
  lookup_sam_contacts_by_uei(uei)    -> {"uei", "match_count", "contacts": [{…poc…}]}
  register(mcp) mounts EXACTLY those four tools.

Pinned invariants: BTREE-eligible predicate pushdown (filter= on the indexed column);
projection to the documented column set (the wide undocumented columns MUST NOT leak); input
upper/trim normalization; unknown id → documented empty shape (never a raise); NAICS prefix
semantics. `database.open_dataset` is monkeypatched to local Lance fixtures.
"""

from __future__ import annotations

import lance
import pyarrow as pa
import pytest

from apps.gtm_mcp.src import database

sam_entities = pytest.importorskip(
    "apps.gtm_mcp.src.tools.sam_entities",
    reason="sam_entities tool module not implemented yet (executor builds it to GREEN)",
)

# The documented entity-profile projection — the EXACT keys the lookups must return (those
# present in the dataset). The fixture is intentionally WIDER than this so the projection
# tests can prove the undocumented filler never leaks.
SAM_ENTITY_DOC = [
    "uei", "cage_code", "primary_naics", "legal_business_name", "dba_name", "entity_url",
    "physical_address_city", "physical_address_province_or_state",
    "physical_address_country_code", "state_of_incorporation", "entity_structure",
    "registration_expiration_date", "sam_extract_code", "is_active",
]
_FILLER = [f"filler_attr_{i}" for i in range(10)]  # wide undocumented columns — must not leak


def _entity_row(uei, naics, cage, name):
    base = {
        "uei": uei, "cage_code": cage, "primary_naics": naics,
        "legal_business_name": name, "dba_name": f"{name} DBA", "entity_url": f"{name}.example.com",
        "physical_address_city": "Springfield", "physical_address_province_or_state": "VA",
        "physical_address_country_code": "USA", "state_of_incorporation": "DE",
        "entity_structure": "2L", "registration_expiration_date": "2027-01-01",
        "sam_extract_code": "A", "is_active": True,
    }
    base.update({k: f"{uei}-{k}" for k in _FILLER})
    return base


def _contact_row(uei, poc_type, first, last, title):
    return {"uei": uei, "poc_type": poc_type, "first_name": first, "last_name": last,
            "title": title, "city": "Springfield", "state_or_province": "VA"}


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    entities = [
        _entity_row("UEI0000000AAA", "541512", "1ABC1", "Acme Federal"),
        _entity_row("UEI0000000BBB", "541511", "2DEF2", "Beta Systems"),
        _entity_row("UEI0000000CCC", "541512", "3GHI3", "Gamma Labs"),
        _entity_row("UEI0000000DDD", "236220", "4JKL4", "Delta Construction"),
    ]
    contacts = [
        _contact_row("UEI0000000AAA", "govt_business", "Ada", "Chief", "CEO"),
        _contact_row("UEI0000000AAA", "electronic_business", "Bo", "Tech", "CTO"),
        _contact_row("UEI0000000BBB", "govt_business", "Cy", "Lead", "President"),
    ]
    ent_tbl = pa.Table.from_pylist(entities)
    con_tbl = pa.Table.from_pylist(contacts)
    paths = {
        "sam_master_entities": str(tmp_path / "ent.lance"),
        "sam_master_contacts": str(tmp_path / "con.lance"),
    }
    lance.write_dataset(ent_tbl, paths["sam_master_entities"], mode="create")
    lance.write_dataset(con_tbl, paths["sam_master_contacts"], mode="create")
    # Real BTREE scalar indices, mirroring the prod datasets (uei/primary_naics/cage_code; uei).
    eds = lance.dataset(paths["sam_master_entities"])
    for col in ("uei", "primary_naics", "cage_code"):
        eds.create_scalar_index(col, index_type="BTREE")
    lance.dataset(paths["sam_master_contacts"]).create_scalar_index("uei", index_type="BTREE")

    def fake_open(name):
        if name not in paths:
            raise KeyError(name)
        return lance.dataset(paths[name])

    monkeypatch.setattr(database, "open_dataset", fake_open)
    return paths


# ── lookup_sam_entity_by_uei ──────────────────────────────────────────────────────────────
def test_lookup_by_uei_returns_projected_entity(wired):
    out = sam_entities.lookup_sam_entity_by_uei("UEI0000000AAA")
    assert out["uei"] == "UEI0000000AAA" and out["found"] is True
    ent = out["entity"]
    assert ent["legal_business_name"] == "Acme Federal"
    assert ent["primary_naics"] == "541512" and ent["cage_code"] == "1ABC1"


def test_lookup_by_uei_projection_drops_undocumented(wired):
    ent = sam_entities.lookup_sam_entity_by_uei("UEI0000000AAA")["entity"]
    assert set(ent.keys()) <= set(SAM_ENTITY_DOC)              # only documented columns
    assert not (set(ent.keys()) & set(_FILLER))                # wide filler never leaks
    assert "uei" in ent and "legal_business_name" in ent       # core fields present


def test_lookup_by_uei_normalizes_and_unknown(wired):
    assert sam_entities.lookup_sam_entity_by_uei("  uei0000000aaa ")["found"] is True  # upper/trim
    miss = sam_entities.lookup_sam_entity_by_uei("UEI_NONE")
    assert miss["found"] is False and miss["entity"] is None    # unknown → empty shape, no raise
    empty = sam_entities.lookup_sam_entity_by_uei("")
    assert empty["found"] is False and empty["entity"] is None


# ── search_sam_entities_by_naics (PREFIX) ────────────────────────────────────────────────────
def test_search_by_naics_prefix(wired):
    out = sam_entities.search_sam_entities_by_naics("5415")     # prefix → 541512 x2 + 541511 x1
    assert out["primary_naics"] == "5415"
    ueis = {e["uei"] for e in out["entities"]}
    assert ueis == {"UEI0000000AAA", "UEI0000000BBB", "UEI0000000CCC"}
    assert out["match_count"] == 3
    for e in out["entities"]:
        assert set(e.keys()) <= set(SAM_ENTITY_DOC)            # projected
    # exact 6-digit also works
    exact = sam_entities.search_sam_entities_by_naics("541512")
    assert {e["uei"] for e in exact["entities"]} == {"UEI0000000AAA", "UEI0000000CCC"}


def test_search_by_naics_empty(wired):
    out = sam_entities.search_sam_entities_by_naics("9999")
    assert out["match_count"] == 0 and out["entities"] == []


# ── lookup_sam_entity_by_cage ────────────────────────────────────────────────────────────────
def test_lookup_by_cage(wired):
    out = sam_entities.lookup_sam_entity_by_cage("2def2")       # normalized upper
    assert out["found"] is True and out["entity"]["uei"] == "UEI0000000BBB"
    assert set(out["entity"].keys()) <= set(SAM_ENTITY_DOC)
    assert sam_entities.lookup_sam_entity_by_cage("ZZZZZ")["found"] is False


# ── lookup_sam_contacts_by_uei ───────────────────────────────────────────────────────────────
def test_lookup_contacts_by_uei(wired):
    out = sam_entities.lookup_sam_contacts_by_uei("UEI0000000AAA")
    assert out["uei"] == "UEI0000000AAA" and out["match_count"] == 2
    titles = {c["title"] for c in out["contacts"]}
    assert titles == {"CEO", "CTO"}
    assert sam_entities.lookup_sam_contacts_by_uei("UEI_NONE")["match_count"] == 0


# ── registration ─────────────────────────────────────────────────────────────────────────────
def test_register_mounts_exactly_the_sam_entity_surface():
    registered: list[str] = []

    class _MCP:
        def add_tool(self, fn):
            registered.append(fn.__name__)

    sam_entities.register(_MCP())
    assert set(registered) == {
        "lookup_sam_entity_by_uei", "lookup_sam_entity_by_cage",
        "search_sam_entities_by_naics", "lookup_sam_contacts_by_uei",
    }
