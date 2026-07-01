"""Unit tests for the corex GTM control-surface lead resolver
(apps/gtm_mcp/src/tools/corex.py :: _resolve_contacts).

Focus: the `recipient_uei` lead-resolution path. `define_audience` advertises
`result_key ∈ {company_id, contact_id, recipient_uei}`, so a subawardee/contractor
audience keyed on `recipient_uei` MUST resolve to contacts — the prior code handled only
contact_id/company_id, so such an audience silently enrolled ZERO leads. These tests pin:

  * recipient_uei bridges UEI → normalized_domain via the `sam_master_domains` Lance index
    (real local fixture, BTREE scanner pushdown on `uei`), then resolves the most-senior
    contact per company from the `people` graph — never a silent zero;
  * input is upper/trim-normalized and de-duplicated; empty/whitespace UEIs no-op cleanly;
  * an unknown UEI (no bridge row) resolves to no contacts WITHOUT raising;
  * a MISSING bridge dataset fails LOUDLY (RuntimeError, agent-readable) instead of zero;
  * the untouched contact_id / company_id paths still resolve.

`database.open_dataset` (Lance) is wired to a local fixture; `database.get_registry` and
`database.query` (the `people` resolution) are monkeypatched — the production module path is
otherwise untouched.
"""

from __future__ import annotations

import lance
import pyarrow as pa
import pytest

from apps.gtm_mcp.src import database
from apps.gtm_mcp.src.tools import corex

# ── people graph fixture (keyed by normalized_domain, as the BTREE anchor is) ────────────────
# acme.com: a CEO and an Analyst at the SAME company → most-senior (CEO) must win.
# beta.io:  a single VP at a different company.
_PEOPLE = {
    "acme.com": [
        {"person_id": "c-ceo", "company_id": "co-acme",
         "normalized_domain": "acme.com", "full_name": "Ada Chief", "title": "Chief Executive Officer"},
        {"person_id": "c-analyst", "company_id": "co-acme",
         "normalized_domain": "acme.com", "full_name": "Bo Junior", "title": "Analyst"},
    ],
    "beta.io": [
        {"person_id": "c-vp", "company_id": "co-beta",
         "normalized_domain": "beta.io", "full_name": "Cy Veep", "title": "VP Engineering"},
    ],
}


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    """Local `sam_master_domains` Lance fixture + a fake `people` resolver under the prod
    module. The domains index maps UEIs → normalized_domain (two UEIs share acme.com)."""
    dom_tbl = pa.table({
        "normalized_domain": ["acme.com", "acme.com", "beta.io"],
        "uei": ["UEI_ACME_A", "UEI_ACME_B", "UEI_BETA"],
    })
    dom_path = str(tmp_path / "sam_master_domains.lance")
    lance.write_dataset(dom_tbl, dom_path, mode="create")

    registry = {"sam_master_domains": dom_path, "people": "people"}
    monkeypatch.setattr(database, "get_registry", lambda refresh=False: dict(registry))
    monkeypatch.setattr(
        database, "open_dataset",
        lambda name: lance.dataset(dom_path) if name == "sam_master_domains"
        else (_ for _ in ()).throw(KeyError(name)),
    )

    def fake_query(sql, datasets=(), max_rows=1000, **kw):
        # The people resolution selects WHERE normalized_domain IN (...); return every fixture
        # person whose domain literal appears in the rendered IN-list.
        rows = [p for dom, ppl in _PEOPLE.items() if f"'{dom}'" in sql for p in ppl]
        return {"columns": [], "rows": rows, "row_count": len(rows), "truncated": False}

    monkeypatch.setattr(database, "query", fake_query)
    return registry


# ── recipient_uei: the bridge resolves (the bug being fixed) ─────────────────────────────────
def test_recipient_uei_bridges_to_most_senior_contact_per_company(wired):
    rows = [{"recipient_uei": "uei_acme_a"}, {"recipient_uei": "UEI_BETA"}]
    out = corex._resolve_contacts(rows, "recipient_uei")
    by_company = {c["company_id"]: c for c in out}
    assert set(by_company) == {"co-acme", "co-beta"}
    # acme.com had a CEO and an Analyst — most-senior (CEO) wins, one row per company
    assert by_company["co-acme"]["contact_id"] == "c-ceo"
    # person_id mirrors contact_id (EXPAND migration — both present)
    assert by_company["co-acme"]["person_id"] == "c-ceo"
    assert by_company["co-acme"]["title"] == "Chief Executive Officer"
    assert by_company["co-beta"]["contact_id"] == "c-vp"
    assert by_company["co-beta"]["person_id"] == "c-vp"
    # shape parity with the company_id path
    assert set(by_company["co-acme"]) == {
        "person_id", "contact_id", "company_id", "normalized_domain", "full_name", "title"}


def test_recipient_uei_normalizes_and_dedupes_input(wired):
    # mixed case + whitespace + a duplicate UEI; both acme UEIs map to the SAME domain →
    # still ONE acme contact out.
    rows = [{"recipient_uei": "  uei_acme_a "}, {"recipient_uei": "UEI_ACME_B"},
            {"recipient_uei": "uei_acme_a"}]
    out = corex._resolve_contacts(rows, "recipient_uei")
    assert [c["company_id"] for c in out] == ["co-acme"]
    assert out[0]["contact_id"] == "c-ceo"
    assert out[0]["person_id"] == "c-ceo"


def test_recipient_uei_unknown_resolves_empty_not_raise(wired):
    # a UEI with no bridge row → no domain → no contacts, but NO exception (best-effort)
    assert corex._resolve_contacts([{"recipient_uei": "UEI_GHOST"}], "recipient_uei") == []


def test_recipient_uei_empty_input_noops(wired):
    assert corex._resolve_contacts([], "recipient_uei") == []
    assert corex._resolve_contacts([{"recipient_uei": ""}, {"recipient_uei": "   "},
                                    {"recipient_uei": None}], "recipient_uei") == []


def test_recipient_uei_fails_loud_when_bridge_unregistered(monkeypatch):
    # bridge dataset absent from the active sink → loud, agent-readable error, never zero leads
    monkeypatch.setattr(database, "get_registry", lambda refresh=False: {"people": "people"})
    with pytest.raises(RuntimeError, match="sam_master_domains"):
        corex._resolve_contacts([{"recipient_uei": "UEI_ACME_A"}], "recipient_uei")


# ── unchanged paths still resolve ────────────────────────────────────────────────────────────
def test_contact_id_path_passes_rows_through(wired):
    # person_id-keyed rows resolve; the response mirrors contact_id from person_id
    rows = [{"person_id": "x1", "company_id": "co1", "normalized_domain": "x.com",
             "full_name": "X One", "title": "CTO"},
            {"person_id": "x2", "company_id": "co2", "normalized_domain": "y.com",
             "full_name": "Y Two", "title": "COO"},
            {"person_id": None}]  # dropped
    out = corex._resolve_contacts(rows, "contact_id")
    assert len(out) == 2
    assert out[0]["contact_id"] == "x1" and out[0]["person_id"] == "x1" and out[0]["company_id"] == "co1"
    # contact_id mirrors person_id in the response surface
    assert out[1]["contact_id"] == "x2" and out[1]["person_id"] == "x2"


def test_company_id_path_resolves_via_people(monkeypatch):
    people = [
        {"person_id": "c-ceo", "company_id": "co-acme",
         "normalized_domain": "acme.com", "full_name": "Ada Chief", "title": "Chief Executive Officer"},
        {"person_id": "c-analyst", "company_id": "co-acme",
         "normalized_domain": "acme.com", "full_name": "Bo Junior", "title": "Analyst"},
    ]
    monkeypatch.setattr(
        database, "query",
        lambda sql, datasets=(), max_rows=1000, **kw: {
            "columns": [], "rows": people if "'co-acme'" in sql else [],
            "row_count": len(people), "truncated": False},
    )
    out = corex._resolve_contacts([{"company_id": "co-acme"}], "company_id")
    assert len(out) == 1 and out[0]["contact_id"] == "c-ceo"  # most senior wins
    assert out[0]["person_id"] == "c-ceo"
