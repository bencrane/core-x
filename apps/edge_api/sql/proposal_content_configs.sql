-- Proposal Templates — the authoring registry behind the operator's "Settings → Proposal
-- Templates" surface. Applied to the hq-x control-plane Postgres (HQX_DB_URL_POOLED) that
-- edge_api writes. Idempotent DDL (safe to re-run).
--
-- One row per template. The operator authors the BODY in markdown (the ~85% canonical legal
-- text) with inline {{handlebars}} merge tokens; edge_api wraps it in the Rare Structure brand
-- shell + the page-broken execution block and renders via DocRaptor. The markdown SOURCE lives
-- here (it is live-edited in-app); rendered PDFs are artifacts (preview PDFs → R2; the sealed
-- proposal PDF → Documenso), never stored in this row.
--
-- LIFECYCLE: ``draft`` (editable, not selectable) → ``published`` (named + selectable in the
-- Proposals intake picker). ``slug`` is the stable selector a minted proposal stores in
-- ``business.engagement_proposals.template_id``; it is set at publish time and is unique.

CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.proposal_content_configs (
    -- identity
    id                 text        PRIMARY KEY,                 -- stable id, minted at draft creation (tpl_…)
    slug               text,                                    -- selector a proposal references; set at publish
    name               text,                                    -- operator-facing label ("Standard Engagement"); set at publish
    -- lifecycle
    status             text        NOT NULL DEFAULT 'draft'
                       CHECK (status IN ('draft','published')),
    -- authored content
    markdown           text        NOT NULL DEFAULT '',         -- the body the operator writes (with {{tokens}})
    token_manifest     jsonb       NOT NULL DEFAULT '[]'::jsonb,-- {{tokens}} detected in the assembled doc (body + shell)
    monthly_fee_cents  bigint,                                  -- DEFAULT price/month ({{monthly_fee}}); a proposal inherits then may override
    -- pricing config DEFAULTS (a minted proposal inherits these, editable per deal). total is DERIVED
    -- (monthly_fee × duration), never stored. billing_cadence is independent of duration.
    duration_months    integer,                                 -- DEFAULT engagement term in months ({{duration}})
    billing_cadence    text,                                    -- DEFAULT invoicing schedule: upfront_in_full | monthly | quarterly
    success_fee_schedule jsonb,                                 -- DEFAULT tiered success-fee %s — list of {"tier","rate"}
    -- pre-proposal page config: the exec-summary blurb shown on the engagement / exec-summary page
    -- (NOT the legal proposal body). Template-owned ⇒ org-keyed in practice (one template per org).
    exec_summary       text,
    -- ownership: the issuing org. Banking resolves org.slug = business.dbas.slug → dbas.legal_entity_id
    -- → business.legal_entities (ACH/Stripe). Nullable: the in-app create path does not set it yet.
    organization_id    uuid        REFERENCES business.organizations (id) ON DELETE RESTRICT,
    -- provenance / timeline
    created_by         text,                                    -- operator user id (optional)
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    published_at       timestamptz
);

-- Exactly one template per published slug (the proposal's template_id resolves to ≤1 row).
CREATE UNIQUE INDEX IF NOT EXISTS proposal_content_configs_slug_uidx
    ON business.proposal_content_configs (slug) WHERE slug IS NOT NULL;
CREATE INDEX IF NOT EXISTS proposal_content_configs_status_idx  ON business.proposal_content_configs (status);
CREATE INDEX IF NOT EXISTS proposal_content_configs_created_idx ON business.proposal_content_configs (created_at DESC);

-- organization_id — additive + idempotent for already-provisioned control planes (the CREATE TABLE
-- above only fires on a fresh DB). FK added guardedly (Postgres has no ADD CONSTRAINT IF NOT EXISTS).
ALTER TABLE business.proposal_content_configs ADD COLUMN IF NOT EXISTS organization_id uuid;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = 'business'
          AND table_name = 'proposal_content_configs'
          AND constraint_name = 'proposal_content_configs_organization_id_fkey'
    ) THEN
        ALTER TABLE business.proposal_content_configs
            ADD CONSTRAINT proposal_content_configs_organization_id_fkey
            FOREIGN KEY (organization_id) REFERENCES business.organizations (id) ON DELETE RESTRICT;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS proposal_content_configs_organization_idx
    ON business.proposal_content_configs (organization_id);

-- exec_summary — additive + idempotent (pre-proposal exec-summary page copy; see CREATE TABLE).
ALTER TABLE business.proposal_content_configs ADD COLUMN IF NOT EXISTS exec_summary text;

-- pricing config defaults — additive + idempotent (see CREATE TABLE). total derives, never stored.
ALTER TABLE business.proposal_content_configs ADD COLUMN IF NOT EXISTS duration_months      integer;
ALTER TABLE business.proposal_content_configs ADD COLUMN IF NOT EXISTS billing_cadence       text;
ALTER TABLE business.proposal_content_configs ADD COLUMN IF NOT EXISTS success_fee_schedule  jsonb;
