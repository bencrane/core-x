"""Payments — Stripe ACH (us_bank_account) PaymentIntent + Elements for the engagement fee.

The post-signature step of the engagement lifecycle: once the Documenso agreement is COMPLETED,
the client authorizes a single ACH debit of the quarterly infrastructure fee. Card is never offered.

Module layout mirrors ``proposals/`` and ``cal/``:
  * ``amount``        — the single seam that resolves the charge from proposal content (never the browser)
  * ``models``        — the public projections the pay page (via the BFF) consumes
  * ``stripe_client`` — the Stripe SDK wrapper (secret key server-side only; calls off the event loop)
  * ``queries``       — payment-state persistence + the append-only engagement_events ledger
"""
