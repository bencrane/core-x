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
    ('prepaid_introductions',       'Prepaid Introductions'),
    ('digital_event',               'Digital Event'),
    ('capital_facility',            'Capital Facility'),
    ('prepaid_introductions_range', 'Prepaid Introductions (Range)')
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
-- Seeded version rows (idempotent on content_path — the repo SoR pointer, 4 segments:
-- brand/path/archetype/version, exactly what the BFF splits for render/push).
INSERT INTO gc.global_agreement_archetype_versions (archetype_id, content_path, name, version)
SELECT a.id,
       'government-contracted/docraptor-to-documenso-template/prepaid-introductions-range/v1',
       'Strategic Origination Agreement (Prepaid Introductions, Range) — One-Page Body',
       'v1'
  FROM gc.global_agreement_archetypes a
 WHERE a.key = 'prepaid_introductions_range'
ON CONFLICT (content_path) DO NOTHING;

-- ARCHETYPE VARIABLES (requirements). One row per field a conforming Documenso template must
-- carry. On the ARCHETYPE (not the version): versions are legal-language iterations; a change to
-- the variable set is a new archetype or an explicit redefinition (operator ruling 2026-07-21).
-- documenso_field_label_to_use is a DECLARATION of intent — the label the operator will use when
-- manually creating the field in the Documenso editor (the template is born with ZERO fields; the
-- mirror pull is what brings Documenso truth back, and match/mismatch is computed then). Rule
-- columns (template-stage Required/Read-Only, side assignment, post-mint treatment) are a later,
-- separately-ruled addition — not defined yet. 'auto' value_source = filled by the signing
-- ceremony itself (signatures; dates stamp on execution) — never passed in.
CREATE TABLE IF NOT EXISTS gc.global_agreement_archetype_variables (
    id                            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    archetype_id                  uuid NOT NULL REFERENCES gc.global_agreement_archetypes(id),
    ordinal                       int  NOT NULL,  -- document order; disambiguates repeated labels (Date x3, Signature x2)
    documenso_field_label_to_use  text NOT NULL,
    field_type                    text NOT NULL CHECK (field_type IN ('text', 'signature', 'date')),
    value_source                  text NOT NULL CHECK (value_source IN ('entered', 'derived', 'auto')),
    created_at                    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT global_agreement_archetype_variables_ordinal_uniq UNIQUE (archetype_id, ordinal),
    CONSTRAINT global_agreement_archetype_variables_label_ordinal_uniq
        UNIQUE (archetype_id, documenso_field_label_to_use, ordinal)
);
-- Seeded requirements for prepaid_introductions_range (document order; idempotent on ordinal).
INSERT INTO gc.global_agreement_archetype_variables
    (archetype_id, ordinal, documenso_field_label_to_use, field_type, value_source)
SELECT a.id, v.ordinal, v.label, v.field_type, v.value_source
  FROM gc.global_agreement_archetypes a,
       (VALUES
            (1,  'Date',              'date',      'auto'),     -- preamble Effective Date
            (2,  'Legal Entity Name', 'text',      'entered'),
            (3,  'D/B/A Name',        'text',      'entered'),
            (4,  'PrepaidFee',        'text',      'entered'),
            (5,  'IntroNumMin',       'text',      'entered'),
            (6,  'IntroNumMax',       'text',      'entered'),
            (7,  'PricePerIntroMin',  'text',      'derived'),  -- = PrepaidFee / IntroNumMin
            (8,  'DaysToFill',        'text',      'entered'),
            (9,  'Signature',         'signature', 'auto'),     -- operator column
            (10, 'Date',              'date',      'auto'),     -- operator sig date
            (11, 'Signature',         'signature', 'auto'),     -- counterparty column
            (12, 'Full Name',         'text',      'entered'),
            (13, 'Title',             'text',      'entered'),
            (14, 'Date',              'date',      'auto')      -- counterparty sig date
       ) AS v(ordinal, label, field_type, value_source)
 WHERE a.key = 'prepaid_introductions_range'
ON CONFLICT (archetype_id, ordinal) DO NOTHING;

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

-- Application Profile: ONE row per prospect domain — the on-call "Company Profile" board
-- (gc-hq-new /hq/application/:domain). Seeded from LeadMagic firmo (Blitz fallback) on first
-- open, then operator-edited + per-section verified on the call. Domain-keyed (the page key);
-- the bare normalized domain. Pure gc-owned product state — not the LLM-synthesized dossier
-- (that is a separate artifact). Arrays + the verify map ride JSONB; load-bearing scalars typed.
CREATE TABLE IF NOT EXISTS gc.application_profiles (
    domain         text PRIMARY KEY,
    company_name   text,
    hq             text,
    headcount      text,
    revenue_range  text,
    founded_year   text,
    linkedin_url   text,
    overview       text,
    focus_areas    jsonb NOT NULL DEFAULT '[]'::jsonb,
    industries     jsonb NOT NULL DEFAULT '[]'::jsonb,
    geographies    jsonb NOT NULL DEFAULT '[]'::jsonb,
    contact_name   text,
    contact_title  text,
    contact_email  text,
    verified       jsonb NOT NULL DEFAULT '{}'::jsonb,
    seed_source    text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
