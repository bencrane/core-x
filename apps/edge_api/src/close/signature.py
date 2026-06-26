"""Close.com webhook signature verification.

Close signs each delivery with HMAC-SHA256 over the concatenation ``{timestamp}{raw_body}``,
keyed by the subscription's ``signature_key`` (hex), and sends the hex digest in
``Close-Sig-Hash`` alongside ``Close-Sig-Timestamp``. We verify against the RAW bytes (never a
re-serialized body — whitespace/ordering would change the digest) and compare in constant time.

The signature_key is hex; per Close's docs the HMAC key is the raw bytes the hex decodes to.
Validated empirically against the first real delivery before go-live (the subscription is
created via the API, which returns the key).
"""
from __future__ import annotations

import hashlib
import hmac


def verify_signature(
    raw_body: bytes, sig_hash: str | None, sig_timestamp: str | None, signature_key: str
) -> bool:
    """True iff ``sig_hash`` is HMAC-SHA256(``signature_key`` bytes, ``timestamp`` + ``raw_body``).

    A missing hash or timestamp is a hard fail — an unsigned/partial delivery is never trusted.
    """
    if not sig_hash or not sig_timestamp:
        return False
    try:
        key = bytes.fromhex(signature_key)
    except ValueError:
        # Tolerate a non-hex secret (treat as raw utf-8) so a mis-stored key fails closed-but-clear.
        key = signature_key.encode("utf-8")
    msg = sig_timestamp.encode("utf-8") + raw_body
    expected = hmac.new(key, msg, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig_hash.strip())
