-- Operator Settings — the per-operator cockpit configuration row. Applied to the hq-x control-plane
-- Postgres (HQX_DB_URL_POOLED). Idempotent DDL (safe to re-run; the ALTER ADD COLUMN IF NOT EXISTS
-- block upgrades an already-deployed table in place).
--
-- OWNERSHIP NOTE. This table lives in `public` and is read/written EXCLUSIVELY by the platform-api
-- BFF via its Supabase service-role client (RLS is on, no anon/authenticated grants — service_role
-- bypasses RLS). edge_api itself does NOT serve a settings endpoint; the BFF resolves these values
-- and forwards the resolved pathway to edge_api at originate. This file is the DDL system-of-record
-- for the table's shape, captured here because core-x owns the live control-plane Postgres — the
-- table predates this file (it was originally created live by the BFF).
--
-- GRAIN: one row per operator, keyed by the Supabase auth user id (`auth_user_id`).
--
-- COLUMNS.
--   render_mode               — the top-level originate pathway "Confirm & Originate" uses:
--                                 'through-docraptor'  (DEFAULT): render the agreement PDF (DocRaptor)
--                                                       → create the Documenso envelope.
--                                 'direct-to-documenso'         : skip DocRaptor; instantiate the
--                                                       Documenso document directly.
--   direct_to_documenso_lane  — a SECOND, INDEPENDENT sub-selector that ONLY applies when
--                                render_mode = 'direct-to-documenso'. It picks which direct-to-
--                                documenso lane "Confirm & Originate" uses:
--                                 'envelope-distribute'  (DEFAULT — existing behavior): /envelope/use
--                                                        + distribute → POST .../{id}/confirm →
--                                                        create_document_from_template_with_custom_pdf.
--                                 'prefill-document-from-template' : /api/v2/template/use with the
--                                                        opportunity's field values prefilled, then
--                                                        distribute(NONE) → PENDING (no email) →
--                                                        POST .../{id}/originate-prefilled →
--                                                        create_document_from_template.
--                                Ignored when render_mode = 'through-docraptor'. The DEFAULT preserves
--                                the existing envelope-distribute behavior for every existing row.

CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.operator_settings (
    auth_user_id              uuid        PRIMARY KEY,                         -- Supabase auth user id (the JWT sub)
    render_mode               text        NOT NULL DEFAULT 'through-docraptor',-- originate pathway (top-level)
    direct_to_documenso_lane  text        NOT NULL DEFAULT 'envelope-distribute', -- sub-lane (only when render_mode='direct-to-documenso')
    updated_at                timestamptz NOT NULL DEFAULT now()
);

-- Converge an already-deployed table in place (the table predates this file).
ALTER TABLE public.operator_settings
    ADD COLUMN IF NOT EXISTS direct_to_documenso_lane text NOT NULL DEFAULT 'envelope-distribute';

-- Value domains — enforced at the DB layer (the BFF validates too; this is defense-in-depth and the
-- canonical record of the allowed strings). Guarded so re-runs don't error on the existing constraint.
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'operator_settings_render_mode_check'
          AND conrelid = 'public.operator_settings'::regclass
    ) THEN
        ALTER TABLE public.operator_settings
            ADD CONSTRAINT operator_settings_render_mode_check
            CHECK (render_mode = ANY (ARRAY['through-docraptor'::text, 'direct-to-documenso'::text]));
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'operator_settings_direct_to_documenso_lane_check'
          AND conrelid = 'public.operator_settings'::regclass
    ) THEN
        ALTER TABLE public.operator_settings
            ADD CONSTRAINT operator_settings_direct_to_documenso_lane_check
            CHECK (direct_to_documenso_lane = ANY (ARRAY['envelope-distribute'::text, 'prefill-document-from-template'::text]));
    END IF;
END $$;

-- RLS on, no policies: service_role (the BFF) bypasses RLS; anon/authenticated have no grants and
-- therefore no access. The table is reachable only via the BFF's service-role client.
ALTER TABLE public.operator_settings ENABLE ROW LEVEL SECURITY;
