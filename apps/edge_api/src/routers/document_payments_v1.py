"""Document payments — the prospect's direct-to-documenso engagement-fee surface (PUBLIC, pair-keyed).

  POST /api/v1/documenso/payment-intent/{opportunity_id}/{document_id}   PUBLIC — mint/reuse ACH intent
  GET  /api/v1/documenso/payment/{opportunity_id}/{document_id}          PUBLIC — authoritative state (poll)

PUBLIC: the ``(opportunity_id, document_id)`` pair IS the capability (same trust model as the
sign-token / sign-state reads). The amount is resolved server-side from the opportunity's
``fee_amount`` — never the browser. An intent is minted ONLY once the document is signed
(``DOCUMENT_COMPLETED`` for the pair, derived offline from the webhook capture); it is reused on
refresh so a prospect never accumulates duplicate intents. ``paid`` is advanced ONLY by the Stripe
webhook (``/webhooks/stripe``), never here.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import psycopg
from fastapi import APIRouter, HTTPException

from .. import config
from ..db import get_db_connection
from ..deals import originate
from ..documenso_webhooks import queries as sign_queries
from ..document_payments import amount as amount_resolver
from ..document_payments import queries as pay_queries
from ..document_payments import stripe as stripe_client
from ..document_payments.models import DocumentPaymentInitPublic, DocumentPaymentStatePublic

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/documenso", tags=["document-payments"])

# Intent statuses whose amount is still mutable (no funds in flight yet).
_AMOUNT_MUTABLE = {"requires_payment_method", "requires_confirmation", "requires_action"}

# Schema-drift errors: the live DB is MISSING something the committed code references — a column/table/
# function not yet applied from sql/*.sql. This is exactly the rail-column outage (PR #518): a committed
# DDL column never applied to prod made ``SELECT … rail`` raise undefined_column → unhandled 500 on
# EVERY mint + poll, taking the whole payment surface dark. The startup apply (src/migrate.py) is the
# fix; this is the seatbelt — if a deploy ever races ahead of the schema again, degrade to a clean,
# retryable 503 instead of leaking a raw 500.
_SCHEMA_DRIFT_ERRORS = (
    psycopg.errors.UndefinedColumn,
    psycopg.errors.UndefinedTable,
    psycopg.errors.UndefinedFunction,
    psycopg.errors.InvalidSchemaName,
)


@asynccontextmanager
async def _payments_db():
    """A pooled connection whose schema-drift failures surface as 503, not 500. Any drift error raised
    by a DB read/write inside the ``async with`` body is thrown back in at the ``yield`` and converted
    here, so one wrapper covers every query in the block (vs. a raw psycopg 500 reaching the SPA)."""
    try:
        async with get_db_connection() as conn:
            yield conn
    except _SCHEMA_DRIFT_ERRORS as exc:
        logger.error(
            "document payments: schema drift (%s) — live DB is behind committed sql/*.sql; degrading to 503",
            exc,
        )
        raise HTTPException(status_code=503, detail="payment is temporarily unavailable") from exc


def _mint_idempotency_key(
    document_id: str, existing_status: str, existing_intent: str | None
) -> str:
    """Idempotency key for the dual-rail mint.

    A pristine pair (or a reuse-block fall-through on an open intent) keys off the document id alone. A
    retry after a HARD FAILURE (``failed``/``canceled``) must NOT: the spent intent already burned
    ``pay_document_{document_id}`` against ITS amount, so replaying that fixed key after the operator
    edits ``fee_amount`` is a Stripe 400 idempotency_error (→ StripeError → 502, wedged for the 24h key
    TTL). Namespacing the key by the prior intent id makes it fresh across the fee change yet still
    idempotent within a single retry burst — a double-submit keys off the same persisted failed intent,
    so Stripe returns the SAME new intent rather than minting a duplicate charge surface.
    """
    if existing_status in ("failed", "canceled") and existing_intent:
        return f"pay_document_{document_id}_retry_{existing_intent}"
    return f"pay_document_{document_id}"


@router.post(
    "/payment-intent/{opportunity_id}/{document_id}",
    response_model=DocumentPaymentInitPublic,
)
async def create_document_payment_intent(
    opportunity_id: str, document_id: str
) -> DocumentPaymentInitPublic:
    async with _payments_db() as conn:
        # Resolve the operator-selected Stripe mode (operator_settings.stripe_mode, augmenting the env
        # STRIPE_MODE) and its key set. Single-operator platform: this is the global selection (the
        # prospect mint has no operator session). Keys are mode-specific so the toggle takes effect
        # without a redeploy.
        mode = config.resolve_stripe_mode(await pay_queries.get_stripe_mode_selection(conn))
        publishable_key = config.stripe_publishable_key_for_mode(mode)
        if config.stripe_secret_key_for_mode(mode) is None or not publishable_key:
            raise HTTPException(status_code=503, detail="Stripe is not configured")

        # GATE: the COUNTERPARTY (prospect) must have signed. read_sign_state.signed flips on a SIGNED
        # recipient whose email domain is NOT a provider domain — INDEPENDENT of the provider's own
        # countersignature, so the prospect can pay right after they sign. This also enforces the pair
        # — a document that does not belong to this opportunity has no matching rows → not signed → 409.
        state = await sign_queries.read_sign_state(
            conn, opportunity_id=opportunity_id, document_id=document_id
        )
        if not state["signed"]:
            raise HTTPException(status_code=409, detail="agreement not yet signed")

        # AMOUNT + customer. The handle in the pair is the opportunity's 8-char handle on the legacy
        # /p/m links and the AGREEMENT handle on the /sign/{agreement_handle} flow — resolve through
        # whichever world knows it. The agreement fee is the generate-time merge (prefill-config
        # defaults ⊕ agreement overrides, via resolve_field_values) so the charge equals the value
        # stamped read-only into the signed document, same construction as the opportunity path.
        info = await pay_queries.get_fee_and_contact(conn, opportunity_id)
        if not info:
            ag = await pay_queries.get_agreement_fee_and_contact(conn, opportunity_id)
            if ag:
                info = {
                    "field_values": originate.resolve_field_values(
                        ag["field_settings"], ag["field_values"]
                    ),
                    "recipient_email": ag["recipient_email"],
                    "recipient_name": ag["recipient_name"],
                }
        if not info:
            raise HTTPException(status_code=404, detail="opportunity not found")
        charge_cents = amount_resolver.resolve_fee_cents(info["field_values"])
        if charge_cents <= 0:
            raise HTTPException(status_code=409, detail="no payable fee_amount for this opportunity")
        if not info["recipient_email"]:
            raise HTTPException(status_code=422, detail="opportunity contact has no email")

        existing = await pay_queries.get_payment(conn, document_id) or {}
        if (existing.get("payment_status") or "none") == "succeeded":
            raise HTTPException(status_code=409, detail="already paid")
        existing_intent = existing.get("stripe_payment_intent_id")
        existing_customer = existing.get("stripe_customer_id")
        existing_status = existing.get("payment_status") or "none"

        # Reuse an open intent (idempotent across refresh/retry); only re-mint after a hard failure.
        # RECREATE a stale single-rail intent (minted before the card rail) — cancel it, then fall
        # through to the fresh dual-rail mint below — but ONLY when no funds are in flight
        # (amount-mutable status). An intent already mid-ACH (``processing``) is left alone so the
        # in-flight debit is never disrupted, even though it lacks the card tab.
        if existing_intent and existing_status not in ("failed", "canceled"):
            try:
                intent = await stripe_client.retrieve_payment_intent(existing_intent, mode)
                stale_single_rail = "card" not in (intent.get("payment_method_types") or [])
                if stale_single_rail and intent.get("status") in _AMOUNT_MUTABLE:
                    await stripe_client.cancel_payment_intent(existing_intent, mode)
                    # fall through to a fresh dual-rail mint (new idempotency-key namespace)
                else:
                    if intent.get("amount") != charge_cents and intent.get("status") in _AMOUNT_MUTABLE:
                        await stripe_client.update_payment_intent_amount(
                            existing_intent, charge_cents, mode
                        )
                        intent = await stripe_client.retrieve_payment_intent(existing_intent, mode)
                    if intent.get("client_secret"):
                        return DocumentPaymentInitPublic(
                            client_secret=intent["client_secret"],
                            publishable_key=publishable_key,
                            amount_cents=charge_cents,
                            currency=existing.get("currency") or "usd",
                            payment_status=existing_status,
                            recipient_name=info.get("recipient_name"),
                            recipient_email=info.get("recipient_email"),
                        )
            except Exception as exc:  # noqa: BLE001
                # Reuse is a pure optimization; ANY failure degrades to a fresh mint (idempotency_key
                # makes the mint return the SAME intent rather than duplicating), never a 500.
                logger.warning(
                    "reuse of document intent %s failed (%s); minting a new one", existing_intent, exc
                )

        # RETRY AFTER A HARD FAILURE — mint fresh, with a FRESH idempotency key.
        # REGRESSION (availability): a ``failed``/``canceled`` row skips the reuse block above (its intent
        # is spent) and falls straight to the mint below. The prior failed intent ALREADY burned the fixed
        # ``pay_document_{document_id}`` key against ITS amount, so if the operator edits ``fee_amount``
        # between the failed attempt and the retry, replaying that fixed key with a new amount is a Stripe
        # 400 idempotency_error → StripeError → 502 — wedged for the full 24h key TTL. (Stripe REJECTS the
        # mismatched replay, so it never charges a stale amount; this is availability, not money.) Fix:
        # ``_mint_idempotency_key`` namespaces the key by the prior intent id (fresh across the fee change,
        # stable within a retry burst), and we best-effort cancel the spent intent so it cannot linger as
        # an open ``requires_payment_method`` against the prospect's customer.
        if existing_status in ("failed", "canceled") and existing_intent:
            # No funds are in flight on a failed/canceled status; an already-canceled intent (or any
            # cancel hiccup) is non-fatal — the fresh-key mint is what actually unblocks the retry.
            try:
                await stripe_client.cancel_payment_intent(existing_intent, mode)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "cancel of spent document intent %s failed (%s); minting fresh anyway",
                    existing_intent,
                    exc,
                )

        try:
            customer_id = await stripe_client.ensure_customer(
                email=info["recipient_email"],
                name=info["recipient_name"],
                existing_id=existing_customer,
                mode=mode,
            )
            created = await stripe_client.create_payment_intent(
                amount_cents=charge_cents,
                customer_id=customer_id,
                opportunity_id=opportunity_id,
                document_id=document_id,
                idempotency_key=_mint_idempotency_key(
                    document_id, existing_status, existing_intent
                ),
                mode=mode,
            )
        except stripe_client.StripeError as exc:
            logger.error(
                "document intent mint failed for %s/%s: %s", opportunity_id, document_id, exc
            )
            raise HTTPException(status_code=502, detail=f"stripe: {exc}") from exc

        await pay_queries.upsert_intent(
            conn,
            document_id=document_id,
            opportunity_id=opportunity_id,
            amount_cents=charge_cents,
            currency="usd",
            customer_id=customer_id,
            intent_id=created["id"],
            status="requires_payment",
        )

    return DocumentPaymentInitPublic(
        client_secret=created["client_secret"],
        publishable_key=publishable_key,
        amount_cents=charge_cents,
        currency="usd",
        payment_status="requires_payment",
        recipient_name=info.get("recipient_name"),
        recipient_email=info.get("recipient_email"),
    )


@router.get(
    "/payment/{opportunity_id}/{document_id}",
    response_model=DocumentPaymentStatePublic,
)
async def get_document_payment_state(
    opportunity_id: str, document_id: str
) -> DocumentPaymentStatePublic:
    """Authoritative payment state for the SPA poll. Returns ``none`` before the first mint (so the
    SPA can poll without erroring) and for a pair mismatch (no info leak about a guessed document id)."""
    async with _payments_db() as conn:
        pay = await pay_queries.get_payment(conn, document_id)
    if pay is None or pay.get("opportunity_id") != opportunity_id:
        return DocumentPaymentStatePublic(payment_status="none")
    paid_at = pay.get("paid_at")
    return DocumentPaymentStatePublic(
        payment_status=pay.get("payment_status") or "none",
        amount_cents=pay.get("amount_cents"),
        currency=pay.get("currency") or "usd",
        paid_at=paid_at.isoformat() if paid_at else None,
        rail=pay.get("rail"),
    )
