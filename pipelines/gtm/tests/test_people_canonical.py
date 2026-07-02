"""Pins the canonical person-identity + provenance helper (pipelines/gtm/_people_canonical.py).

This helper is the ONE place that derives ``canonical_person_id`` and routes source_platform to
the person_source_platforms sidecar. Every people-writer produces ids through it, so these ids MUST
stay byte-identical to the Phase A–C datasets the coordinator already built in R2. Two layers:

  1. Pure spec pinning — normalize_linkedin + canonical_person_id against the EXACT documented
     rule (independent sha256, so any drift in the normalization trips immediately).
  2. land_people behavior — over a LOCAL temp Lance people dataset (no R2): identity is
     merge_inserted one row per canonical id (source_platform NEVER reaches people), the sidecar
     gets one row per (canonical_person_id, source_platform, legacy_person_id), and a re-run is a
     pure no-op (idempotent on both datasets).

    python -m pytest pipelines/gtm/tests/test_people_canonical.py -q
"""
from __future__ import annotations

import hashlib

import lance
import pyarrow as pa
import pytest

from pipelines.gtm import _people_canonical as pc


# ── 1. Pure spec pinning ──────────────────────────────────────────────────────────────────────
def _sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def test_normalize_linkedin_exact_spec():
    cases = {
        "https://www.linkedin.com/in/alexkigel/": "in/alexkigel",
        "https://www.linkedin.com/in/alexkigel": "in/alexkigel",
        "HTTPS://WWW.LinkedIn.com/in/AlexKigel/": "in/alexkigel",
        "linkedin.com/in/alexkigel": "in/alexkigel",
        "www.linkedin.com/in/alexkigel/": "in/alexkigel",
        "https://ca.linkedin.com/in/alexkigel/": "in/alexkigel",       # cc-subdomain stripped
        "https://www.linkedin.com/in/alexkigel/?originalSubdomain=us": "in/alexkigel",  # query
        "https://www.linkedin.com/in/alexkigel/#foo": "in/alexkigel",  # fragment
        "https://www.linkedin.com/in/alexkigel///": "in/alexkigel",    # trailing slashes
        "https://www.linkedin.com/in/alexkigel/detail/contact-info/": "in/alexkigel/detail/contact-info",
        "  ": None,
        "": None,
        None: None,
    }
    for url, expected in cases.items():
        assert pc.normalize_linkedin(url) == expected, url


def test_canonical_person_id_url_and_fallback():
    # URL present → sha256(normalized url), independent of the legacy id.
    assert pc.canonical_person_id("https://www.linkedin.com/in/alexkigel/", "legacy") == _sha("in/alexkigel")
    assert pc.canonical_person_id("https://ca.linkedin.com/in/alexkigel/", "other") == _sha("in/alexkigel")
    # Null / empty URL → sha256('pid:' || legacy).
    assert pc.canonical_person_id(None, "L123") == _sha("pid:L123")
    assert pc.canonical_person_id("", "L123") == _sha("pid:L123")
    assert pc.canonical_person_id("   ", "L123") == _sha("pid:L123")


def test_add_canonical_person_id_columns():
    tbl = pa.table({
        "person_id": ["L1", "L2"],
        "person_linkedin_url": ["https://www.linkedin.com/in/a/", None],
        "title": ["VP", None],
    })
    out = pc.add_canonical_person_id(tbl)
    assert out.column("canonical_person_id").to_pylist() == [_sha("in/a"), _sha("pid:L2")]
    assert out.column("person_linkedin_url_norm").to_pylist() == ["in/a", None]


# ── 2. land_people over a local temp Lance dataset ──────────────────────────────────────────────
def _people_schema() -> pa.Schema:
    # A minimal canonical-people schema: canonical_person_id PK + representative legacy id +
    # identity columns. NO source_platform column (that lives in the sidecar).
    return pa.schema([
        ("canonical_person_id", pa.string()),
        ("person_id", pa.string()),
        ("company_id", pa.string()),
        ("normalized_domain", pa.string()),
        ("full_name", pa.string()),
        ("title", pa.string()),
        ("person_linkedin_url", pa.string()),
    ])


def _sidecar_seed() -> pa.Table:
    # Sidecar must exist before land_people merges into it (Phase A builds it in prod).
    return pc.SIDECAR_SCHEMA.empty_table()


@pytest.fixture()
def local_datasets(tmp_path):
    people_uri = str(tmp_path / "people_canonical.lance")
    sidecar_uri = str(tmp_path / "person_source_platforms.lance")
    lance.write_dataset(_people_schema().empty_table(), people_uri, mode="create")
    lance.write_dataset(_sidecar_seed(), sidecar_uri, mode="create")
    return people_uri, sidecar_uri


def _land(people_uri, sidecar_uri, rows, source_platform, source_ref="test"):
    return pc.land_people(
        rows, source_platform, source_ref, storage_options={},
        people_uri=people_uri, sidecar_uri=sidecar_uri,
    )


def test_land_people_identity_and_sidecar(local_datasets):
    people_uri, sidecar_uri = local_datasets
    rows = pa.table({
        "person_id": ["L1", "L2"],
        "company_id": ["c1", "c2"],
        "normalized_domain": ["a.com", "b.com"],
        "full_name": ["Alex K", "Bo Q"],
        "title": ["VP", None],
        "person_linkedin_url": ["https://www.linkedin.com/in/alexkigel/", None],
    })
    res = _land(people_uri, sidecar_uri, rows, "dsbs_poc")
    assert res == {"people_candidates": 2, "sidecar_candidates": 2}

    people = lance.dataset(people_uri).to_table().to_pylist()
    assert len(people) == 2
    # source_platform NEVER lands on people.
    assert "source_platform" not in lance.dataset(people_uri).schema.names
    by_cid = {r["canonical_person_id"]: r for r in people}
    assert _sha("in/alexkigel") in by_cid
    assert _sha("pid:L2") in by_cid                       # null-URL fallback id
    assert by_cid[_sha("in/alexkigel")]["title"] == "VP"

    side = lance.dataset(sidecar_uri).to_table().to_pylist()
    assert {(r["canonical_person_id"], r["source_platform"], r["legacy_person_id"]) for r in side} == {
        (_sha("in/alexkigel"), "dsbs_poc", "L1"),
        (_sha("pid:L2"), "dsbs_poc", "L2"),
    }
    assert side[0]["source_ref"] == "test"
    assert all(r["first_seen_at"] is not None for r in side)


def test_land_people_idempotent_rerun(local_datasets):
    people_uri, sidecar_uri = local_datasets
    rows = pa.table({
        "person_id": ["L1"],
        "company_id": ["c1"],
        "normalized_domain": ["a.com"],
        "full_name": ["Alex K"],
        "title": ["VP"],
        "person_linkedin_url": ["https://www.linkedin.com/in/alexkigel/"],
    })
    _land(people_uri, sidecar_uri, rows, "dsbs_poc")
    _land(people_uri, sidecar_uri, rows, "dsbs_poc")     # re-run → no-op on both datasets
    assert lance.dataset(people_uri).count_rows() == 1
    assert lance.dataset(sidecar_uri).count_rows() == 1


def test_land_people_two_sources_one_human(local_datasets):
    # Same LinkedIn URL under different legacy ids + different source tags → ONE people row,
    # TWO sidecar rows (both under the same canonical id). This is the up-to-8-ids-per-URL case.
    people_uri, sidecar_uri = local_datasets
    base = dict(company_id=["c1"], normalized_domain=["a.com"], full_name=["Alex K"], title=["VP"],
                person_linkedin_url=["https://www.linkedin.com/in/alexkigel/"])
    _land(people_uri, sidecar_uri, pa.table({"person_id": ["L1"], **base}), "dsbs_poc")
    _land(people_uri, sidecar_uri, pa.table({"person_id": ["L2"], **base}), "clay_find_people")
    assert lance.dataset(people_uri).count_rows() == 1   # collapsed to one canonical human
    side = lance.dataset(sidecar_uri).to_table().to_pylist()
    assert len(side) == 2
    assert {r["source_platform"] for r in side} == {"dsbs_poc", "clay_find_people"}
    assert {r["canonical_person_id"] for r in side} == {_sha("in/alexkigel")}


def test_land_people_rejects_source_platform_column(local_datasets):
    people_uri, sidecar_uri = local_datasets
    rows = pa.table({
        "person_id": ["L1"], "person_linkedin_url": ["https://www.linkedin.com/in/a/"],
        "source_platform": ["dsbs_poc"],   # provenance must NOT ride the people batch
    })
    with pytest.raises(ValueError, match="source_platform"):
        _land(people_uri, sidecar_uri, rows, "dsbs_poc")
