-- business.deal_details — the 1:1 satellite of business.deals.
--
-- ONE deal has AT MOST ONE deal_details row. Enforced STRUCTURALLY by the shared primary key: deal_id
-- is BOTH the PK and the FK to business.deals(id). No surrogate id, no separate join key — a deal's
-- details row IS keyed by the deal's own id. ON DELETE CASCADE: the satellite dies with its deal.
--
-- This SUPERSEDES the off-book business.deals_content — a surrogate-id table keyed to opportunity_id,
-- never committed to sql/, never wired in code, 3 throwaway rows pointing at the legacy `opportunities`
-- entity. deals_content is left in place as a vestige (NOT dropped: prod-safety + append-only). Nothing
-- references it; nothing migrates out of it.
--
-- WHAT IT HOLDS:
--   content                — the deal's merge-field payload (carried concept from deals_content.content).
--   default_template_uuid  — the Documenso template stamped as THIS deal's default, FK to the
--                            business.documenso_templates catalog (its uuid PK, matching the
--                            documenso_template_uuid join key used by the mappings table). Replaces the
--                            pick-at-originate picker + loose org-level is_default with a per-deal stamp.
--   template_origin        — provenance of that stamp. 'default' = auto-stamped from the org default;
--                            an operator override writes a different marker.
--
-- FK targets (business.deals, business.documenso_templates) are upstream-owned and already exist in
-- prod — consistent with this suite's "already-provisioned control plane" contract (cf.
-- documenso_templates_is_default.sql). CREATE-only + idempotent; safe to re-apply on every boot.
CREATE TABLE IF NOT EXISTS business.deal_details (
    deal_id               uuid PRIMARY KEY
                              REFERENCES business.deals (id) ON DELETE CASCADE,
    content               jsonb       NOT NULL DEFAULT '{}'::jsonb,
    default_template_uuid uuid        REFERENCES business.documenso_templates (id),
    template_origin       text        NOT NULL DEFAULT 'default',
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

-- The MIRROR template attached to this deal, keyed by the envelope mirror's numeric documenso_id
-- (business.documenso_envelopes.documenso_id). Supersedes default_template_uuid (which keys the legacy
-- business.documenso_templates registry); attaches made via the mirror dropdown write THIS column. No FK
-- to the projector-owned mirror — validated at write. Idempotent ALTER, safe to re-apply on every boot.
ALTER TABLE business.deal_details
    ADD COLUMN IF NOT EXISTS default_template_documenso_id bigint;
