-- existing_claygent_payloads — universal raw landing sink for heterogeneous Claygent enrichment
-- payloads (append-only). Applied to the hqx control-plane Postgres (HQX_DB_URL_POOLED) that
-- edge_api writes. Idempotent DDL (safe to re-run).
--
-- WHY ONE TABLE (not one-per-shape). Claygent enrichment runs emit DIFFERENT payload shapes per
-- enrichment type (capital-provider classification, equipment-financing mode, OEM/seller flags, …).
-- Rather than a table per shape, every payload lands here verbatim as jsonb under a string
-- discriminator (enrichment_payload_type). jsonb enforces NO schema, so divergent shapes — including
-- the observed sources-as-string-vs-array divergence across runs — store losslessly and are sorted
-- out at READ time by filtering on enrichment_payload_type. Adding enrichment type #13 needs zero
-- DDL: just a new discriminator value on the wire.
--
-- CONTRACT. Wire body: { domain, enrichment_payload_type, raw_payload }. raw_payload is stored
-- EXACTLY as sent — no projection, no typed columns, no coercion.
--
-- GRAIN: one row per (enrichment_payload_type × domain_norm × canonical-raw_payload).
-- PK = record_id = sha256(enrichment_payload_type "|" domain_norm "|" sha256(canonical_json(raw_payload))).
-- Byte-identical resends are idempotent via ON CONFLICT DO NOTHING. A different payload (or a
-- different type) for the same domain lands as a DISTINCT row — append-only history.
--
-- NO secondary indexes / no bridges / no contact joins by design — this is a deferred-processing
-- sink, not a resolution SoR.

CREATE SCHEMA IF NOT EXISTS gtm;

CREATE TABLE IF NOT EXISTS gtm.existing_claygent_payloads (
    record_id                text        PRIMARY KEY,   -- sha256(enrichment_payload_type|domain_norm|sha256(raw_payload))
    enrichment_payload_type  text        NOT NULL,      -- discriminator — filter on this at read time
    domain                   text        NOT NULL,      -- verbatim as sent
    domain_norm              text        NOT NULL,      -- canonical dedup key (scheme/www/path stripped)
    raw_payload              jsonb       NOT NULL,       -- verbatim blob, ANY shape — no projection
    landed_at                timestamptz NOT NULL DEFAULT now()
);
-- PK btree only. No secondary indexes by design.
