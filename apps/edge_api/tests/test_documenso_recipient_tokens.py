"""Unit guard for the per-recipient token extractor — no network.

The full client-vs-originator selection is verified live in the PR (two-recipient doc, distinct
tokens, contact-email match). This pins the pure extractor that feeds it.
"""
from __future__ import annotations

from apps.edge_api.src.services.documenso_client import _recipient_email_tokens


def test_extracts_lowercased_email_token_pairs():
    body = {
        "recipients": [
            {"email": "Client@Example.com", "token": "tokC"},
            {"email": "you@hq.com", "token": "tokO"},
            {"email": "notoken@x.com"},  # no token → excluded
        ]
    }
    assert _recipient_email_tokens(body) == (
        ("client@example.com", "tokC"),
        ("you@hq.com", "tokO"),
    )


def test_blank_email_kept_as_empty_key():
    body = {"recipients": [{"email": "", "token": "t1"}, {"token": "t2"}]}
    assert _recipient_email_tokens(body) == (("", "t1"), ("", "t2"))


def test_non_list_or_missing_recipients_is_empty():
    assert _recipient_email_tokens({"recipients": None}) == ()
    assert _recipient_email_tokens({}) == ()


def test_signingToken_key_variant_supported():
    body = {"recipients": [{"email": "a@b.com", "signingToken": "tok"}]}
    assert _recipient_email_tokens(body) == (("a@b.com", "tok"),)
