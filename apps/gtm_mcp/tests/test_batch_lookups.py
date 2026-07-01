"""Acceptance tests (frozen Scorer) for batched multi-id lookups
(apps/gtm_mcp/src/tools/batch_lookups.py — to be implemented by the executor).

WHY. An audience workflow resolves MANY ids (a list of recipient UEIs, a list of company
domains). Calling the single-id point-lookups N times is N separate Lance scanner
invocations = N index probes = N R2 round-trips. A batched tool issues ONE scanner call with
an `IN (…)` predicate over the BTREE-indexed column, amortizing the round-trips across the
whole audience. THE LOAD-BEARING INVARIANT these tests pin: **exactly ONE `.scanner()` call
per batched tool, regardless of N**.

CONTRACT (executor implements to GREEN):
  lookup_awards_by_ueis(ueis)         -> {"requested","found","by_uei": {uei: summary_row}}   (whole row, by contract)
  lookup_sam_entities_by_ueis(ueis)   -> {"requested","found","by_uei": {uei: entity_row}}     (projected)
  search_companies_by_domains(domains)-> {"requested","match_count","by_domain": {dom: [rows]}} (projected; domain not unique → list)
  search_people_by_domains(domains)   -> {"requested","match_count","by_domain": {dom: [rows]}} (projected; → list)
  register(mcp) mounts EXACTLY those four.

Pinned: ONE scanner call per batched lookup; input normalization (uei upper/trim; domain
scheme/www/path strip) + de-dup; empty/whitespace batch → no scan + empty result; projection
(undocumented columns never leak for the projected datasets); missing ids simply absent from
the grouping. `database.open_dataset` is monkeypatched to a scan-COUNTING wrapper over local
Lance fixtures.
"""

from __future__ import annotations

import lance
import pyarrow as pa
import pytest

from apps.gtm_mcp.src import database

batch_lookups = pytest.importorskip(
    "apps.gtm_mcp.src.tools.batch_lookups",
    reason="batch_lookups module not implemented yet (executor builds it to GREEN)",
)

COMPANY_DOC = ["company_id", "company_name", "normalized_domain",
               "company_linkedin_url", "source_platform"]
PEOPLE_DOC = ["person_id", "contact_id", "company_id", "normalized_domain", "full_name",
              "first_name", "last_name", "title", "person_linkedin_url", "source_platform"]
SAM_ENTITY_DOC = ["uei", "cage_code", "primary_naics", "legal_business_name"]
_FILLER = [f"filler_{i}" for i in range(8)]


class _CountingDataset:
    """Wraps a real LanceDataset and counts `.scanner()` calls (the round-trip proxy)."""

    def __init__(self, ds, counter: dict, name: str):
        self._ds, self._counter, self._name = ds, counter, name

    def scanner(self, **kw):
        self._counter[self._name] = self._counter.get(self._name, 0) + 1
        return self._ds.scanner(**kw)

    @property
    def schema(self):
        return self._ds.schema


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    awards = [{"recipient_uei": f"UEI{i:05d}", "recipient_name": f"Recipient {i}",
               "combined_total_dollars": float(i * 1000), **{k: f"aw{i}{k}" for k in _FILLER}}
              for i in range(20)]
    sam = [{"uei": f"UEI{i:05d}", "cage_code": f"C{i:04d}", "primary_naics": "541512",
            "legal_business_name": f"Legal {i}", **{k: f"s{i}{k}" for k in _FILLER}}
           for i in range(20)]
    companies = []
    people = []
    for i in range(20):
        dom = f"dom{i}.example.com"
        companies.append({"company_id": f"co{i}", "company_name": f"Company {i}",
                          "normalized_domain": dom, "company_linkedin_url": f"li/{i}",
                          "source_platform": "fixture", **{k: f"c{i}{k}" for k in _FILLER}})
        # two people per domain → list grouping is non-trivial.
        # EXPAND migration: people carries BOTH person_id and contact_id (same value).
        for j in range(2):
            people.append({"person_id": f"ct{i}_{j}", "contact_id": f"ct{i}_{j}",
                           "company_id": f"co{i}",
                           "normalized_domain": dom, "full_name": f"Person {i}.{j}",
                           "first_name": f"F{i}", "last_name": f"L{i}", "title": "VP",
                           "person_linkedin_url": f"li/in/{i}_{j}", "source_platform": "fixture",
                           **{k: f"p{i}{j}{k}" for k in _FILLER}})

    paths = {}
    for name, rows, btree in (
        ("contractor_award_summary", awards, "recipient_uei"),
        ("sam_master_entities", sam, "uei"),
        ("companies", companies, "normalized_domain"),
        ("people", people, "normalized_domain"),
    ):
        p = str(tmp_path / f"{name}.lance")
        lance.write_dataset(pa.Table.from_pylist(rows), p, mode="create")
        lance.dataset(p).create_scalar_index(btree, index_type="BTREE")
        paths[name] = p
    # the `awards` alias resolves to contractor_award_summary (database.ALIASES)
    paths["awards"] = paths["contractor_award_summary"]

    counter: dict = {}

    def fake_open(name):
        if name not in paths:
            raise KeyError(name)
        return _CountingDataset(lance.dataset(paths[name]), counter, name)

    monkeypatch.setattr(database, "open_dataset", fake_open)
    return counter


# ── awards (whole-row by contract) ────────────────────────────────────────────────────────────
def test_lookup_awards_by_ueis_one_scan_grouped(wired):
    counter = wired
    out = batch_lookups.lookup_awards_by_ueis(["UEI00001", "UEI00003", "UEI99999"])
    assert out["requested"] == 3 and out["found"] == 2
    assert set(out["by_uei"]) == {"UEI00001", "UEI00003"}           # missing UEI99999 absent
    assert out["by_uei"]["UEI00001"]["recipient_name"] == "Recipient 1"
    assert counter.get("awards", 0) == 1                            # ONE scan for the batch


def test_lookup_awards_by_ueis_normalizes_and_dedupes(wired):
    counter = wired
    out = batch_lookups.lookup_awards_by_ueis(["  uei00002 ", "UEI00002", "uei00004"])
    assert set(out["by_uei"]) == {"UEI00002", "UEI00004"}
    assert counter.get("awards", 0) == 1


# ── sam entities (projected) ──────────────────────────────────────────────────────────────────
def test_lookup_sam_entities_by_ueis_one_scan_projected(wired):
    counter = wired
    out = batch_lookups.lookup_sam_entities_by_ueis(["UEI00000", "UEI00005"])
    assert set(out["by_uei"]) == {"UEI00000", "UEI00005"}
    ent = out["by_uei"]["UEI00000"]
    assert set(ent.keys()) <= set(SAM_ENTITY_DOC)                   # projected
    assert not (set(ent.keys()) & set(_FILLER))                    # filler never leaks
    assert counter.get("sam_master_entities", 0) == 1


# ── companies / people by domains (list grouping, projected) ──────────────────────────────────
def test_search_companies_by_domains_one_scan_grouped_projected(wired):
    counter = wired
    out = batch_lookups.search_companies_by_domains(
        ["https://www.dom1.example.com/x", "dom3.example.com", "dom99.example.com"])
    assert out["requested"] == 3
    assert set(out["by_domain"]) == {"dom1.example.com", "dom3.example.com"}  # normalized; miss absent
    assert out["by_domain"]["dom1.example.com"][0]["company_id"] == "co1"
    assert set(out["by_domain"]["dom1.example.com"][0].keys()) <= set(COMPANY_DOC)
    assert counter.get("companies", 0) == 1


def test_search_people_by_domains_one_scan_lists(wired):
    counter = wired
    out = batch_lookups.search_people_by_domains(["dom2.example.com", "dom4.example.com"])
    assert len(out["by_domain"]["dom2.example.com"]) == 2           # two people per domain
    assert out["match_count"] == 4
    for rows in out["by_domain"].values():
        for r in rows:
            assert set(r.keys()) <= set(PEOPLE_DOC)
    assert counter.get("people", 0) == 1


# ── guards ────────────────────────────────────────────────────────────────────────────────────
def test_empty_batch_no_scan(wired):
    counter = wired
    assert batch_lookups.lookup_awards_by_ueis([])["by_uei"] == {}
    assert batch_lookups.search_companies_by_domains(["", "  "])["by_domain"] == {}
    assert sum(counter.values()) == 0                              # NO scan issued for an empty batch


def test_large_batch_one_scan_and_in_list_capped(wired):
    """The amortization-at-scale invariant + the IN-list cap (directive §contract).

    A LARGE batch (the whole 20-row fixture, plus an over-cap padding of junk ids) must
    STILL resolve in exactly ONE scanner call per tool — N ids → 1 R2 round-trip, never N —
    and the implementation must cap the IN-list at a sane bound (≤1000) WITHOUT raising on a
    batch that exceeds it. This pins the core round-trip-amortization claim at N≫1 (the empty
    + 3-id tests only exercise the small case) and the directive's explicit IN-list cap."""
    counter = wired
    all_ueis = [f"UEI{i:05d}" for i in range(20)]                  # every award/sam id in the fixture
    over_cap = all_ueis + [f"JUNK{i:06d}" for i in range(1500)]    # > 1000-id ceiling, must not raise

    aw = batch_lookups.lookup_awards_by_ueis(over_cap)
    assert aw["found"] == 20 and set(aw["by_uei"]) == set(all_ueis)
    assert counter.get("awards", 0) == 1                          # ONE scan for the whole (capped) batch

    all_doms = [f"dom{i}.example.com" for i in range(20)]
    co = batch_lookups.search_companies_by_domains(all_doms + [f"junk{i}.example.com" for i in range(1500)])
    assert set(co["by_domain"]) == set(all_doms)
    assert counter.get("companies", 0) == 1                       # ONE scan regardless of N


def test_register_mounts_exactly_the_batch_surface():
    registered: list[str] = []

    class _MCP:
        def add_tool(self, fn):
            registered.append(fn.__name__)

    batch_lookups.register(_MCP())
    assert set(registered) == {
        "lookup_awards_by_ueis", "lookup_sam_entities_by_ueis",
        "search_companies_by_domains", "search_people_by_domains",
    }
