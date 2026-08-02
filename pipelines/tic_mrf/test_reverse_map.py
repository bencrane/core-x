"""Offline unit tests for the reverse-map extraction logic (no network, no Modal).

Covers the load-bearing changed behavior:
  - TIN carriage: (tin_type, tin_value, tin_business_name) survive Pass A -> Pass B
    onto every emitted rate row, including the tin_type=='npi' sole-proprietor case
    (preserved as rows, structurally distinguishable from EINs).
  - Inline provider_groups resolution carries the inline TIN.
  - Token-stripped provenance: source_file_url on rows has no query string.
  - file_version stamping on every row.
  - strip_url_token / derive_file_version key derivation (non-null always).

Run: uv run --with "ijson>=3.3" --with pytest python -m pytest pipelines/tic_mrf/test_reverse_map.py -q
"""
from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import reverse_map as rm  # noqa: E402

FIXTURE = {
    "provider_references": [
        {
            "provider_group_id": 15,
            "provider_groups": [
                {"npi": [1235592239],
                 "tin": {"type": "ein", "value": "263069729", "business_name": "SPINE AND PAIN CONSULTANT"}},
                {"npi": [1770540403],
                 "tin": {"type": "npi", "value": "1770540403", "business_name": "SOLE PROP MD"}},
            ],
        },
        {
            "provider_group_id": 99,
            "provider_groups": [
                {"npi": [9999999999], "tin": {"type": "ein", "value": "111111111", "business_name": "NOT IN COHORT"}},
            ],
        },
    ],
    "in_network": [
        {
            "billing_code": "0001U",
            "billing_code_type": "CPT",
            "negotiated_rates": [
                {
                    "provider_references": [15],
                    "negotiated_prices": [
                        {"negotiated_rate": "432.0", "negotiated_type": "negotiated",
                         "billing_class": "professional", "service_code": ["CSTM-00"],
                         "expiration_date": "9999-12-31"},
                    ],
                },
                {
                    "provider_groups": [
                        {"npi": [1013024801],
                         "tin": {"type": "ein", "value": "760622208", "business_name": "INLINE ORTHO PC"}},
                    ],
                    "negotiated_prices": [
                        {"negotiated_rate": 11.0, "billing_class": "professional"},
                    ],
                },
            ],
        },
    ],
}

URL = "https://mrfstore.example.com/blobs/2026-06-01_Acme_in-network-rates.json.gz?sv=x&sig=SECRET"
COHORT = {"1235592239", "1770540403", "1013024801"}


def _patch_stream(monkeypatch):
    payload = gzip.compress(json.dumps(FIXTURE).encode())

    def fake_stream(url, cap_bytes, tel=None):
        if tel is not None:
            tel.passes += 1
            tel.compressed_bytes += len(payload)
        yield gzip.decompress(payload)

    monkeypatch.setattr(rm, "stream_gunzip", fake_stream)


def test_spine_carries_tin(monkeypatch):
    _patch_stream(monkeypatch)
    spine = rm.build_provider_spine(URL, COHORT, cap_bytes=None)
    assert set(spine) == {"15"}  # non-intersecting group 99 dropped
    entries = spine["15"]
    by_tin = {e["tin_value"]: e for e in entries}
    assert by_tin["263069729"]["tin_type"] == "ein"
    assert by_tin["263069729"]["npis"] == {"1235592239"}
    assert by_tin["263069729"]["tin_business_name"] == "SPINE AND PAIN CONSULTANT"
    assert by_tin["1770540403"]["tin_type"] == "npi"  # sole proprietor preserved, flagged


def test_rows_carry_tin_version_and_stripped_url(monkeypatch):
    _patch_stream(monkeypatch)
    spine = rm.build_provider_spine(URL, COHORT, cap_bytes=None)
    rows = list(rm.extract_rates(URL, spine, COHORT, payer="uhc", plan_id=None,
                                 cap_bytes=None, file_version="date:2026-06-01"))
    by_npi = {r["npi"]: r for r in rows}
    assert set(by_npi) == {"1235592239", "1770540403", "1013024801"}

    ein_row = by_npi["1235592239"]
    assert (ein_row["tin_type"], ein_row["tin_value"]) == ("ein", "263069729")
    assert ein_row["negotiated_rate"] == 432.0

    # sole proprietor: row preserved, tin_type distinguishes it from EIN space
    sp_row = by_npi["1770540403"]
    assert (sp_row["tin_type"], sp_row["tin_value"]) == ("npi", "1770540403")

    # inline provider_groups path carries the inline TIN
    inline_row = by_npi["1013024801"]
    assert (inline_row["tin_type"], inline_row["tin_value"]) == ("ein", "760622208")
    assert inline_row["tin_business_name"] == "INLINE ORTHO PC"

    for r in rows:
        assert r["file_version"] == "date:2026-06-01"
        assert "?" not in r["source_file_url"] and "sig=" not in r["source_file_url"]
        assert r["captured_at"]  # never empty


def test_ein_join_cohorts_are_separable(monkeypatch):
    """The join rule is enforceable by predicate: tin_type=='ein' rows only."""
    _patch_stream(monkeypatch)
    spine = rm.build_provider_spine(URL, COHORT, cap_bytes=None)
    rows = list(rm.extract_rates(URL, spine, COHORT, "uhc", None, None))
    ein_joinable = {r["tin_value"] for r in rows if r["tin_type"] == "ein"}
    assert ein_joinable == {"263069729", "760622208"}
    assert "1770540403" not in ein_joinable


def test_strip_url_token():
    assert rm.strip_url_token(URL) == URL.split("?")[0]
    assert rm.strip_url_token("https://a/b.json.gz") == "https://a/b.json.gz"


def test_derive_file_version_never_null():
    assert rm.derive_file_version({"etag": '"abc"'}, URL) == '"abc"'
    assert rm.derive_file_version({"last_modified": "Sun, 07 Jun 2026"}, URL) == "Sun, 07 Jun 2026"
    assert rm.derive_file_version({}, URL) == "date:2026-06-01"  # dated slug fallback
    assert rm.derive_file_version({"content_length": 42}, "https://a/b.json.gz") == "bytes:42"
    assert rm.derive_file_version({}, "https://a/b.json.gz") == "unversioned"
