"""Public response shapes for business.documenso_documents reads."""
from __future__ import annotations

from pydantic import BaseModel


class DocumentAmountPublic(BaseModel):
    """The per-document charge the BFF payment page reads after signing — the variant price FROZEN on
    the document at originate. Keyed by the (opportunity_id, document_id) PAIR (the access capability);
    the pair gate is enforced at the route, so a row only returns to a caller that holds both halves."""

    amount_cents: int | None
    currency: str = "usd"
    status: str | None = None
    opportunity_id: str
    document_id: int
