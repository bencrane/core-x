-- business.global_input_content_variants — the GENERATION INPUT for engagement-template permutations.
--
-- Each row is one variant (e.g. a specific system fee + success %) of a PARAMETERIZED
-- business.global_input_content source. The render+push generator pulls `params` FROM here to bake
-- into the PDF text and to create the Documenso TEMPLATE. This is the source of truth for both the
-- PRINTED value and the CHARGED value (single row → no drift between the PDF and the payment intent).
--
-- Why here and not on the template: at generation time the Documenso template does NOT yet exist (it is
-- the OUTPUT of the render+push), so the values can't be read from it. They must live on an INPUT
-- parented to the content source the generator renders from.
--
-- Idempotent (CREATE … IF NOT EXISTS); applied to HQX_DB_URL_POOLED at edge_api boot.

CREATE TABLE IF NOT EXISTS business.global_input_content_variants (
  id                       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  global_input_content_id  uuid NOT NULL REFERENCES business.global_input_content (id) ON DELETE RESTRICT,
  slug                     text NOT NULL,                       -- stable key, e.g. '25k-2_0pct' (addressing + idempotency)
  label                    text,                                -- display, e.g. '$25,000 / 2.0%'
  params                   jsonb NOT NULL DEFAULT '{}'::jsonb,  -- {"system_fee_cents": 2500000, "success_fee_pct": "2.0"} — shape varies by archetype
  is_default               boolean NOT NULL DEFAULT false,
  status                   text NOT NULL DEFAULT 'active',      -- active | archived
  created_at               timestamptz NOT NULL DEFAULT now(),
  updated_at               timestamptz NOT NULL DEFAULT now(),
  UNIQUE (global_input_content_id, slug)
);

-- At most one default variant per content source.
CREATE UNIQUE INDEX IF NOT EXISTS global_input_content_variants_default_uidx
  ON business.global_input_content_variants (global_input_content_id)
  WHERE is_default;

CREATE INDEX IF NOT EXISTS global_input_content_variants_content_idx
  ON business.global_input_content_variants (global_input_content_id);
