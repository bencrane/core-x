-- Global Input Content — the CONTENT-SOURCE REGISTRY. One row per repo-resident agreement-content
-- asset (or DB-markdown source) the render+push lane can "grab from and get the content". A row names
-- WHERE to pull from; the renderer resolves it to HTML, sends it through DocRaptor, and pushes the PDF
-- to Documenso as a TEMPLATE. Applied to the hq-x control plane (HQX_DB_URL_POOLED). Idempotent DDL.
--
-- The table predates this file (provisioned by hand upstream: id/path/name/status). This DDL OWNS it
-- going forward — CREATE IF NOT EXISTS reproduces the live shape for a fresh control plane, and the
-- guarded ALTERs add the source-selection columns the renderer needs:
--
--   brand        which content root to read     ('active-operators' | 'rare-structure')
--   source_kind  how to resolve the content      ('repo-html'  -> apps/edge_api/content/<brand>/<path>/global_agreement_content,
--                                                  'db-markdown'-> business.global_agreement_content WHERE slug = path)
--
-- ``path`` is BRAND-RELATIVE and encodes the three archetype-version catalog segments joined with
-- "/": ``<template-family>/<archetype>/<version>`` (e.g. ``docraptor-to-documenso-template/term-only/v1``).
-- The renderer splits it back into (path, archetype, version) and resolves under content/<brand>/.

CREATE SCHEMA IF NOT EXISTS business;

-- ── Fresh provision (new control planes) — matches the live shape exactly ─────────────────────────
CREATE TABLE IF NOT EXISTS business.global_input_content (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    path        text        NOT NULL UNIQUE,             -- brand-relative <family>/<archetype>/<version> (repo-html) or slug (db-markdown)
    name        text,                                    -- operator-facing label
    status      text        NOT NULL DEFAULT 'active',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- ── Source-selection columns — additive + idempotent for the already-provisioned live table ──────
ALTER TABLE business.global_input_content ADD COLUMN IF NOT EXISTS brand       text NOT NULL DEFAULT 'active-operators';
ALTER TABLE business.global_input_content ADD COLUMN IF NOT EXISTS source_kind text NOT NULL DEFAULT 'repo-html';

-- source_kind domain — guarded (Postgres has no ADD CONSTRAINT IF NOT EXISTS).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = 'business'
          AND table_name = 'global_input_content'
          AND constraint_name = 'global_input_content_source_kind_chk'
    ) THEN
        ALTER TABLE business.global_input_content
            ADD CONSTRAINT global_input_content_source_kind_chk
            CHECK (source_kind IN ('repo-html', 'db-markdown'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS global_input_content_brand_idx  ON business.global_input_content (brand);
CREATE INDEX IF NOT EXISTS global_input_content_status_idx ON business.global_input_content (status);

-- ── Seed the known repo-html assets (ON CONFLICT keeps any hand-provisioned row untouched) ────────
INSERT INTO business.global_input_content (path, name, brand, source_kind, status) VALUES
    ('docraptor-to-documenso-template/term-only/v1',         'AO Term Plain v1',           'active-operators', 'repo-html', 'active'),
    ('docraptor-to-documenso-template/capital-origination/v1','RS Capital Origination v1', 'rare-structure',   'repo-html', 'active'),
    ('docraptor-to-documenso-template/capital-origination/v2','RS Capital Origination v2', 'rare-structure',   'repo-html', 'active')
ON CONFLICT (path) DO NOTHING;
