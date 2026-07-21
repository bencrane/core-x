-- Global Engagement Content — the single GLOBAL content body every engagement document is built
-- from (the ~98% shared legal text). Applied to the hq-x control-plane Postgres (HQX_DB_URL_POOLED)
-- that edge_api writes. Idempotent DDL (safe to re-run).
--
-- This is the SOURCE, and it is ARCHETYPE-AGNOSTIC: one body covers every engagement. The operator
-- authors it in markdown with inline {{handlebars}} merge tokens; edge_api wraps it in the brand
-- shell + the page-broken execution block and SHOVES it through the DocRaptor pipeline to produce a
-- PDF. That PDF is the output. It is OF an engagement archetype (term_only vs term_plus_greater_of)
-- — the archetype is a classifier applied at pipeline time (which fee tokens the body carries),
-- NOT an owner of this content; there is deliberately no archetype_id here. A produced PDF is
-- uploaded to Documenso as a Template; Documenso RENDERS it per-deal, later, inside an envelope.
-- The markdown SOURCE lives here; produced PDFs are artifacts (preview PDFs → R2; the sealed
-- document → Documenso), never stored in this row.
--
-- NAME LINEAGE: proposal_templates → proposal_content_configs → engagement_content →
-- global_engagement_content. "proposal" is retired; "global" is explicit. The rename moves the
-- data + FKs in place and leaves every column unchanged.
--
-- LIFECYCLE: ``draft`` (editable, not selectable) → ``published`` (named + selectable). ``slug`` is
-- the stable selector a minted document stores in ``business.engagement_proposals.template_id``; it
-- is set at publish time and is unique.

CREATE SCHEMA IF NOT EXISTS business;

-- ── Migration: converge ANY predecessor to global_engagement_content ─────────────────────────────
-- A live control plane may sit at EITHER prior name: proposal_content_configs (if the engagement_content
-- rename was never deployed) or engagement_content (if it was). Rename whichever exists, in place, so
-- data and inbound FKs (documenso_templates.source_config_id, etc.) move intact — a CREATE + copy
-- would orphan them. Guarded ⇒ no-op once converged and on fresh DBs.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'business' AND table_name = 'global_engagement_content') THEN
        IF EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'business' AND table_name = 'engagement_content') THEN
            ALTER TABLE business.engagement_content RENAME TO global_engagement_content;
        ELSIF EXISTS (SELECT 1 FROM information_schema.tables
                      WHERE table_schema = 'business' AND table_name = 'proposal_content_configs') THEN
            ALTER TABLE business.proposal_content_configs RENAME TO global_engagement_content;
        END IF;
    END IF;
END $$;

-- Re-point inherited index names from EITHER predecessor (Postgres does NOT rename these with the
-- table). Each guarded by IF EXISTS ⇒ only the set that actually exists fires; the rest no-op.
ALTER INDEX IF EXISTS business.engagement_content_slug_uidx              RENAME TO global_engagement_content_slug_uidx;
ALTER INDEX IF EXISTS business.engagement_content_status_idx            RENAME TO global_engagement_content_status_idx;
ALTER INDEX IF EXISTS business.engagement_content_created_idx           RENAME TO global_engagement_content_created_idx;
ALTER INDEX IF EXISTS business.engagement_content_organization_idx      RENAME TO global_engagement_content_organization_idx;
ALTER INDEX IF EXISTS business.proposal_content_configs_slug_uidx        RENAME TO global_engagement_content_slug_uidx;
ALTER INDEX IF EXISTS business.proposal_content_configs_status_idx       RENAME TO global_engagement_content_status_idx;
ALTER INDEX IF EXISTS business.proposal_content_configs_created_idx      RENAME TO global_engagement_content_created_idx;
ALTER INDEX IF EXISTS business.proposal_content_configs_organization_idx RENAME TO global_engagement_content_organization_idx;

-- Re-point inherited constraint names from EITHER predecessor (FK + PK).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_schema = 'business'
                 AND constraint_name = 'engagement_content_organization_id_fkey') THEN
        ALTER TABLE business.global_engagement_content
            RENAME CONSTRAINT engagement_content_organization_id_fkey TO global_engagement_content_organization_id_fkey;
    ELSIF EXISTS (SELECT 1 FROM information_schema.table_constraints
                  WHERE constraint_schema = 'business'
                    AND constraint_name = 'proposal_content_configs_organization_id_fkey') THEN
        ALTER TABLE business.global_engagement_content
            RENAME CONSTRAINT proposal_content_configs_organization_id_fkey TO global_engagement_content_organization_id_fkey;
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE constraint_schema = 'business' AND constraint_name = 'engagement_content_pkey') THEN
        ALTER TABLE business.global_engagement_content RENAME CONSTRAINT engagement_content_pkey TO global_engagement_content_pkey;
    ELSIF EXISTS (SELECT 1 FROM information_schema.table_constraints
                  WHERE constraint_schema = 'business' AND constraint_name = 'proposal_content_configs_pkey') THEN
        ALTER TABLE business.global_engagement_content RENAME CONSTRAINT proposal_content_configs_pkey TO global_engagement_content_pkey;
    END IF;
END $$;

-- ── Fresh provision (new control planes) ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS business.global_engagement_content (
    -- identity
    id                 text        PRIMARY KEY,                 -- stable id, minted at draft creation (tpl_…)
    slug               text,                                    -- selector a minted document references; set at publish
    name               text,                                    -- operator-facing label ("Standard Engagement"); set at publish
    -- lifecycle
    status             text        NOT NULL DEFAULT 'draft'
                       CHECK (status IN ('draft','published')),
    -- authored content
    markdown           text        NOT NULL DEFAULT '',         -- the body the operator writes (with {{tokens}})
    token_manifest     jsonb       NOT NULL DEFAULT '[]'::jsonb,-- {{tokens}} detected in the assembled doc (body + shell)
    monthly_fee_cents  bigint,                                  -- DEFAULT price/month ({{monthly_fee}}); a minted document inherits then may override
    -- pricing config DEFAULTS (a minted document inherits these, editable per deal). total is DERIVED
    -- (monthly_fee × duration), never stored. billing_cadence is independent of duration.
    duration_months    integer,                                 -- DEFAULT engagement term in months ({{duration}})
    billing_cadence    text,                                    -- DEFAULT invoicing schedule: upfront_in_full | monthly | quarterly
    success_fee_schedule jsonb,                                 -- DEFAULT tiered success-fee %s — list of {"tier","rate"}
    -- pre-document page config: the exec-summary blurb shown on the engagement / exec-summary page
    -- (NOT the legal body). Content-owned ⇒ org-keyed in practice (one body per org).
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

-- Exactly one body per published slug (the minted document's template_id resolves to ≤1 row).
CREATE UNIQUE INDEX IF NOT EXISTS global_engagement_content_slug_uidx
    ON business.global_engagement_content (slug) WHERE slug IS NOT NULL;
CREATE INDEX IF NOT EXISTS global_engagement_content_status_idx  ON business.global_engagement_content (status);
CREATE INDEX IF NOT EXISTS global_engagement_content_created_idx ON business.global_engagement_content (created_at DESC);

-- organization_id — additive + idempotent for already-provisioned control planes (the CREATE TABLE
-- above only fires on a fresh DB). FK added guardedly (Postgres has no ADD CONSTRAINT IF NOT EXISTS).
ALTER TABLE business.global_engagement_content ADD COLUMN IF NOT EXISTS organization_id uuid;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = 'business'
          AND table_name = 'global_engagement_content'
          AND constraint_name = 'global_engagement_content_organization_id_fkey'
    ) THEN
        ALTER TABLE business.global_engagement_content
            ADD CONSTRAINT global_engagement_content_organization_id_fkey
            FOREIGN KEY (organization_id) REFERENCES business.organizations (id) ON DELETE RESTRICT;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS global_engagement_content_organization_idx
    ON business.global_engagement_content (organization_id);

-- exec_summary — additive + idempotent (pre-document exec-summary page copy; see CREATE TABLE).
ALTER TABLE business.global_engagement_content ADD COLUMN IF NOT EXISTS exec_summary text;

-- pricing config defaults — additive + idempotent (see CREATE TABLE). total derives, never stored.
ALTER TABLE business.global_engagement_content ADD COLUMN IF NOT EXISTS duration_months      integer;
ALTER TABLE business.global_engagement_content ADD COLUMN IF NOT EXISTS billing_cadence       text;
ALTER TABLE business.global_engagement_content ADD COLUMN IF NOT EXISTS success_fee_schedule  jsonb;
