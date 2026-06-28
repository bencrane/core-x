-- business.deals spine fields: the public handle + the deal's single-company identity.
--
-- deal_handle — the PUBLIC short code for URLs / Documenso externalId (e.g. /app/deals/00ad0591).
--   GENERATED ALWAYS AS (LEFT(id::text,8)) STORED, BTREE-indexed. The uuid `id` stays the internal PK
--   and the FK key everywhere (incl. deal_details.deal_id) — so `deal_id`/uuid is NEVER overloaded.
--   Named `deal_handle` (not `deal_id`) deliberately: it avoids the opportunities footgun where the SAME
--   name `opportunity_id` means the text handle on the parent but the uuid FK on children.
--   Non-unique by design: LEFT(uuid,8) could (astronomically) collide and a unique constraint would
--   hard-fail that insert; resolution is by indexed lookup, the 32-bit collision risk is accepted.
--
-- company_name / company_domain — a deal is associated with EXACTLY ONE company, so its name + domain
--   are scalar columns on the spine (two scalars structurally admit exactly one company). Person/title
--   is NOT here: a deal can carry N people at that company (the signers) → that lives in
--   business.deal_details. Denormalized alongside the existing account_id FK as the self-describing
--   company identity the signing artifact + org scoping read without a join.
--
-- business.deals is upstream-owned (exists in prod); ALTER-only + idempotent, safe to re-apply at boot.
ALTER TABLE business.deals
    ADD COLUMN IF NOT EXISTS deal_handle text
        GENERATED ALWAYS AS (LEFT(id::text, 8)) STORED,
    ADD COLUMN IF NOT EXISTS company_name   text,
    ADD COLUMN IF NOT EXISTS company_domain text;

-- The public handle is a resolution key (URL/externalId -> deal) — BTREE scalar index. Non-unique
-- (see above); the deal row IS the handle<->uuid mapping, so no separate lookup table.
CREATE INDEX IF NOT EXISTS idx_deals_deal_handle
    ON business.deals (deal_handle);
