"""Signature-anchor sentinels — the single source of truth shared by the document
renderer and the Documenso client.

The rendered agreement emits these literal markers on the CLIENT signature + date
lines; Documenso resolves the SIGNATURE + DATE field positions from them at sign-time
(``field/create-many`` placeholder branch → ``findText`` over the PDF, then whites the
marker out). Because the marker that is *emitted* and the placeholder that is *placed*
both read from these constants, the two can never drift.

DOUBLE-SQUARE-BRACKET on purpose: the merge grammar is ``{{snake_case}}``
(``template_render._TOKEN_RE``). ``[[...]]`` does not match it, so ``substitute_tokens``
and ``extract_tokens`` ignore these markers and they survive verbatim into the PDF as
selectable text — exactly what Documenso's ``findText`` needs.

Neutral module (no imports from the renderer or the client) so BOTH sides can depend on
it without a circular import.
"""
from __future__ import annotations

# Emitted on the client signature line; Documenso places the SIGNATURE field here.
CLIENT_SIGNATURE_ANCHOR = "[[CLIENT_SIGNATURE]]"

# Emitted on the client date line; Documenso places the DATE field here.
CLIENT_DATE_ANCHOR = "[[CLIENT_DATE]]"
