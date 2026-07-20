-- gc schema — the Government-Contracted agreement ontology + documenso plane (HQX-resident).
-- DOCTRINE: everything edge_api touches lives in HQX. The gc product Supabase is a dumb
-- platform DB and holds NONE of this. Single file so intra-schema ordering is self-contained
-- (filename-ordered apply; business.deals — a dependency — applies earlier as d* < g*).

CREATE SCHEMA IF NOT EXISTS gc;

-- The 3 agreement archetypes (deal structures). Seeded here; keys are load-bearing.
CREATE TABLE IF NOT EXISTS gc.global_agreement_archetypes (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    key         text NOT NULL UNIQUE,
    name        text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
INSERT INTO gc.global_agreement_archetypes (key, name) VALUES
    ('prepaid_introductions', 'Prepaid Introductions'),
    ('digital_event',         'Digital Event'),
    ('capital_facility',      'Capital Facility')
ON CONFLICT (key) DO NOTHING;

-- One row per VERSION within an archetype (mirrors the repo content tree; content_path is the
-- repo SoR pointer; Documenso externalId at push = '{archetype_key}/{version}').
CREATE TABLE IF NOT EXISTS gc.global_agreement_archetype_versions (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    archetype_id  uuid NOT NULL REFERENCES gc.global_agreement_archetypes(id),
    content_path  text NOT NULL UNIQUE,
    name          text NOT NULL,
    version       text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT global_agreement_archetype_versions_archetype_version_uniq UNIQUE (archetype_id, version)
);

-- Per (Documenso template, field label): the AFTER-MINTING state in Documenso's own terms
-- (post_mint_required / post_mint_read_only) + the template-level default value. The template's
-- own Required/Read-Only are facts on the mirror; these rows are what generate/payments resolve.
CREATE TABLE IF NOT EXISTS gc.documenso_template_field_rules (
    id                     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    template_documenso_id  bigint NOT NULL,
    field_label            text NOT NULL,
    rule                   text,  -- retired column (locked|editable vocabulary); kept nullable for history
    default_value          text,
    post_mint_required     boolean NOT NULL DEFAULT true,
    post_mint_read_only    boolean NOT NULL DEFAULT false,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now(),
    UNIQUE (template_documenso_id, field_label)
);

-- The verbatim Documenso envelope MIRROR (templates + minted documents). Written ONLY by the
-- webhook projector and the resync pull; Documenso is the SoR.
CREATE TABLE IF NOT EXISTS gc.documenso_envelopes (
    id                     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    documenso_id           bigint NOT NULL,
    envelope_id            text NOT NULL,
    secondary_id           text,
    type                   text NOT NULL,
    template_documenso_id  bigint,
    external_id            text,
    title                  text,
    status                 text,
    documenso_response     jsonb NOT NULL,
    deleted_at             timestamptz,
    synced_at              timestamptz NOT NULL DEFAULT now(),
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS documenso_envelopes_documenso_id_uidx ON gc.documenso_envelopes (documenso_id);
CREATE INDEX IF NOT EXISTS gc_documenso_envelopes_template_documenso_id_idx ON gc.documenso_envelopes (template_documenso_id);
CREATE INDEX IF NOT EXISTS gc_documenso_envelopes_external_id_idx ON gc.documenso_envelopes (external_id);

-- RAW Documenso webhook capture, append-only verbatim; sign-state derives from it at read time.
CREATE TABLE IF NOT EXISTS gc.documenso_webhook_events (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event        text NOT NULL,
    envelope_id  text,
    external_id  text,
    payload      jsonb NOT NULL,
    received_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS gc_documenso_webhook_events_envelope_idx ON gc.documenso_webhook_events (envelope_id);
CREATE INDEX IF NOT EXISTS gc_documenso_webhook_events_external_idx ON gc.documenso_webhook_events (external_id);

-- Pre-mint staging: ONE row per deal — attached Documenso template, signatory, SPARSE per-deal
-- value overrides (absent label = template default flows at mint). Pure configuration: no
-- lifecycle, no handle. The Deal Agreement is BORN AT MINT.
CREATE TABLE IF NOT EXISTS gc.deal_origination_details (
    id                     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id                uuid NOT NULL UNIQUE REFERENCES business.deals(id),
    template_documenso_id  bigint,
    signatory_first        text,
    signatory_last         text,
    signatory_email        text,
    signatory_title        text,
    field_values           jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
);
