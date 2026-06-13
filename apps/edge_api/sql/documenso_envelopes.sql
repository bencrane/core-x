-- Documenso envelopes — a FAITHFUL, VERBATIM mirror of Documenso's own envelope objects. Applied to
-- the hq-x control-plane Postgres (HQX_DB_URL_POOLED) that edge_api writes. Idempotent (safe to re-run).
--
-- This table is the unequivocal system of record for "what was actually signed". It speaks ONLY
-- Documenso's nomenclature: envelopeId, secondaryId, externalId, status, recipients, fields. It has
-- NO knowledge of any of OUR concepts (engagement proposals, mandate drafts, …) — those are upstream
-- abstractions that may or may not map onto a Documenso envelope, and the mapping lives on OUR side
-- (our rows carry a pointer INTO this mirror; this mirror never reaches back out).
--
-- ``external_id`` is Documenso's own ``externalId`` field — an opaque string we happen to stamp at
-- create time. From this table's point of view it is just that opaque string; what it *means* (a
-- mandate-draft id, a proposal ref, or nothing) is resolved entirely by our domain layer.
--
-- The webhook is a thin, concept-agnostic writer: every Documenso event upserts the row by
-- ``envelope_id``, appends the VERBATIM event payload to ``events``, and snapshots the VERBATIM
-- envelope (status / recipients / fields / raw). The signed field grain lives in ``fields`` exactly
-- as Documenso returns it (id, type, page, inserted, customText, value, signature, recipientId,
-- fieldMeta, secondaryId, positionX/Y, width/height, envelopeId, envelopeItemId).

CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.documenso_envelopes (
    -- Documenso identity
    envelope_id    text        PRIMARY KEY,                 -- Documenso ``envelopeId`` (e.g. envelope_…)
    secondary_id   text,                                    -- Documenso ``secondaryId`` (e.g. document_<n>)
    external_id    text,                                    -- Documenso ``externalId`` — opaque to this table
    -- Documenso lifecycle + descriptors (verbatim values, e.g. 'DRAFT' | 'PENDING' | 'COMPLETED')
    status         text        NOT NULL,
    title          text,                                    -- Documenso ``title``
    type           text,                                    -- Documenso ``type`` (DOCUMENT | TEMPLATE)
    -- Documenso structures, stored VERBATIM
    recipients     jsonb       NOT NULL DEFAULT '[]'::jsonb,-- the ``recipients`` array as Documenso returns it
    fields         jsonb       NOT NULL DEFAULT '[]'::jsonb,-- the ``fields`` array — the signed-field grain, verbatim
    raw            jsonb       NOT NULL DEFAULT '{}'::jsonb,-- the full envelope object snapshot, verbatim
    events         jsonb       NOT NULL DEFAULT '[]'::jsonb,-- append-only array of VERBATIM webhook event payloads
    -- timeline
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Reverse lookups: our domain layer resolves an envelope by Documenso's externalId; status/time for ops.
CREATE INDEX IF NOT EXISTS documenso_envelopes_external_idx ON business.documenso_envelopes (external_id)
    WHERE external_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS documenso_envelopes_status_idx   ON business.documenso_envelopes (status);
CREATE INDEX IF NOT EXISTS documenso_envelopes_updated_idx  ON business.documenso_envelopes (updated_at DESC);
