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

CREATE TABLE IF NOT EXISTS business.proposal_templates (
    -- identity
    id                 text        PRIMARY KEY,                 -- stable id, minted at draft creation (tpl_…)
    slug               text,                                    -- selector a proposal references; set at publish
    name               text,                                    -- operator-facing label ("Standard Engagement"); set at publish
    -- lifecycle
    status             text        NOT NULL DEFAULT 'draft'
                       CHECK (status IN ('draft','published')),
    -- authored content
    markdown           text        NOT NULL DEFAULT '',         -- the body the operator writes (with {{tokens}})
    apply_brand        boolean     NOT NULL DEFAULT true,       -- wrap in the Rare Structure dark shell vs plain print
    token_manifest     jsonb       NOT NULL DEFAULT '[]'::jsonb,-- {{tokens}} detected in the assembled doc (body + shell)
    monthly_fee_cents  bigint,                                  -- intended posture fee (informational; create-time fee is authoritative today)
    -- provenance / timeline
    created_by         text,                                    -- operator user id (optional)
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    published_at       timestamptz
);

-- Exactly one template per published slug (the proposal's template_id resolves to ≤1 row).
CREATE UNIQUE INDEX IF NOT EXISTS proposal_templates_slug_uidx
    ON business.proposal_templates (slug) WHERE slug IS NOT NULL;
CREATE INDEX IF NOT EXISTS proposal_templates_status_idx  ON business.proposal_templates (status);
CREATE INDEX IF NOT EXISTS proposal_templates_created_idx ON business.proposal_templates (created_at DESC);
