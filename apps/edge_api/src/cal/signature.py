"""cal.com webhook signature verification.

cal.com signs each delivery with HMAC-SHA256 over the RAW request body, keyed by
the webhook secret, and sends the hex digest in ``X-Cal-Signature-256``. We verify
against the raw bytes (never a re-serialized body — key ordering/whitespace would
change the digest) and compare in constant time.
"""
from __future__ import annotations

import hashlib
import hmac


def verify_signature(raw_body: bytes, signature_header: str | None, secret: str) -> bool:
    """True iff ``signature_header`` is the HMAC-SHA256 of ``raw_body`` under ``secret``.

    Tolerates an optional ``sha256=`` prefix (some webhook senders prepend the algo).
    A missing/empty header is a hard fail — an unsigned delivery is never trusted."""
    if not signature_header:
        return False
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    provided = signature_header.strip()
    if provided.lower().startswith("sha256="):
        provided = provided.split("=", 1)[1].strip()
    return hmac.compare_digest(expected, provided)
