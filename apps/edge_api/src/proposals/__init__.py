"""Proposals — engagement-agreement rendering + Documenso v2 e-signature engine.

The reusable, app-agnostic core of the proposal/signing flow. Any platform-app's BFF
drives it over the service-token surface; the prospect reads a native React proposal page
(owned by the consumer app) and signs an agreement whose legal weight is owned by Documenso.

  models             — the canonical Proposal data model (one source of truth → React + PDF)
  agreement_template — Strategic Origination Mandate: merge data → clean legal HTML for DocRaptor
  queries            — business.engagement_proposals persistence (psycopg, append-then-advance)
"""
