-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║  corex — GTM control surface (operational state for the gtm-agent).        ║
-- ║  Additive, idempotent. Pointers + derivations only; entities live in Lance.║
-- ║  Canonical source: apps/gtm_mcp/src/tools/corex.py COREX_DDL (self-boots).  ║
-- ║  See docs/reference/COREX_GTM_CONTROL_SURFACE.md.                           ║
-- ╚══════════════════════════════════════════════════════════════════════════╝
-- Apply (read-path verified):
--   doppler run -- bash -c 'psql "$HQX_DB_URL_DIRECT" -f apps/gtm_mcp/migrations/0001_corex.sql'
-- Installs with NO DROP/TRUNCATE — the PreToolUse hook never trips.

CREATE SCHEMA IF NOT EXISTS corex;

-- pgcrypto provides gen_random_uuid(); Supabase ships it. Guarded so the script
-- is safe even where it is preinstalled.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ── corex.contact ──────────────────────────────────────────────────────────
-- PASSIVE identity only — a person at a company, keyed to the Lance graph. Holds
-- nothing strategic; the bundle hangs on the lead, never here. De-duplicated by
-- Lance contact_id so the same human is one row across every campaign.
CREATE TABLE IF NOT EXISTS corex.contact (
    contact_id        uuid PRIMARY KEY,              -- == Lance people.contact_id (NOT generated)
    company_id        uuid NOT NULL,                 -- == Lance companies.company_id
    normalized_domain text,
    full_name         text,
    title             text,
    identity          jsonb NOT NULL DEFAULT '{}'::jsonb,  -- discovered pointers (uei, verified email), not payload
    first_seen_at     timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS contact_company_id_idx        ON corex.contact (company_id);
CREATE INDEX IF NOT EXISTS contact_normalized_domain_idx ON corex.contact (normalized_domain);

-- ── corex.audience ─────────────────────────────────────────────────────────
-- A STAMPED selection over the committed Lance lake = the DuckDB SQL + a stamp
-- {row_count, last_run_at, headline_stats}. Reusable and side-agnostic; campaigns
-- and pairs reference it. Rows are NOT copied into Postgres — they live in Lance.
CREATE TABLE IF NOT EXISTS corex.audience (
    audience_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name           text NOT NULL,
    gtm_side       text CHECK (gtm_side IN ('demand','supply')),  -- intended role; NULL = unbound
    source_sql     text NOT NULL,                    -- recorded DATA, never executed by a write tool
    result_key     text NOT NULL DEFAULT 'company_id',-- 'company_id' | 'contact_id' | 'recipient_uei'
    datasets       jsonb NOT NULL DEFAULT '[]'::jsonb,
    row_count      bigint,
    headline_stats jsonb NOT NULL DEFAULT '{}'::jsonb,
    last_run_at    timestamptz,                       -- when the stamp was taken (NULL = defined, not run)
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audience_gtm_side_idx    ON corex.audience (gtm_side);
CREATE INDEX IF NOT EXISTS audience_last_run_at_idx ON corex.audience (last_run_at DESC);

-- ── corex.initiative ───────────────────────────────────────────────────────
-- The strategic frame and root of the containment tree. gtm_side forks the SAME
-- shape into two trees; brand inherits down. Carries the thesis-as-data via an
-- optional 1:1 audience pair (back-ref column added after both tables exist).
CREATE TABLE IF NOT EXISTS corex.initiative (
    initiative_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    gtm_side      text NOT NULL CHECK (gtm_side IN ('demand','supply')),
    brand         text NOT NULL,                     -- demand→OutboundSolutions.com; supply→client brand
    name          text NOT NULL,
    thesis        text,
    client_domain text,                              -- supply-side: whose market we mail (NULL on demand)
    status        text NOT NULL DEFAULT 'active'
                  CHECK (status IN ('draft','active','paused','archived')),
    metadata      jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (brand, name)
);
CREATE INDEX IF NOT EXISTS initiative_gtm_side_idx ON corex.initiative (gtm_side);
CREATE INDEX IF NOT EXISTS initiative_status_idx   ON corex.initiative (status);

-- ── corex.gtm_audience_pair ────────────────────────────────────────────────
-- The strategic seed — TWO DISTINCT audiences bound with a thesis, living ON the
-- initiative (1:1). The demand audience selects demand-side leads; the supply
-- audience is materialized into the supply-side campaign on call-book.
CREATE TABLE IF NOT EXISTS corex.gtm_audience_pair (
    pair_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    initiative_id      uuid NOT NULL UNIQUE REFERENCES corex.initiative (initiative_id),
    demand_audience_id uuid NOT NULL REFERENCES corex.audience (audience_id),
    supply_audience_id uuid NOT NULL REFERENCES corex.audience (audience_id),
    thesis             text NOT NULL,
    metadata           jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    CHECK (demand_audience_id <> supply_audience_id)  -- two DISTINCT audiences
);
CREATE INDEX IF NOT EXISTS pair_demand_audience_idx ON corex.gtm_audience_pair (demand_audience_id);
CREATE INDEX IF NOT EXISTS pair_supply_audience_idx ON corex.gtm_audience_pair (supply_audience_id);

-- ── corex.campaign_group ───────────────────────────────────────────────────
-- JUST a bag of campaigns (reporting / waves). Owns NO audience. A tag, not a parent.
CREATE TABLE IF NOT EXISTS corex.campaign_group (
    group_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    initiative_id uuid REFERENCES corex.initiative (initiative_id),
    name          text NOT NULL,
    metadata      jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (initiative_id, name)
);

-- ── corex.campaign ─────────────────────────────────────────────────────────
-- The EmailBison / Lob / Vapi send unit. channel + provider are COLUMNS (the
-- campaign is already channel-specific; no channel_campaign entity). Points at one
-- audience; optionally tagged into a group. campaign_key is the deterministic
-- human handle (audience_quality_segment, e.g. FLSURETY_VALIDATED_DM).
CREATE TABLE IF NOT EXISTS corex.campaign (
    campaign_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    initiative_id uuid NOT NULL REFERENCES corex.initiative (initiative_id),
    audience_id   uuid REFERENCES corex.audience (audience_id),
    group_id      uuid REFERENCES corex.campaign_group (group_id),
    campaign_key  text NOT NULL,
    channel       text NOT NULL CHECK (channel  IN ('email','direct_mail','voice')),
    provider      text NOT NULL CHECK (provider IN ('emailbison','lob','vapi')),
    provider_campaign_id text,
    status        text NOT NULL DEFAULT 'draft'
                  CHECK (status IN ('draft','enrolling','live','paused','complete')),
    config        jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    -- channel ⟂ provider: a provider only serves its native channel.
    CHECK ( (provider='emailbison' AND channel='email')
         OR (provider='lob'        AND channel='direct_mail')
         OR (provider='vapi'       AND channel='voice') ),
    UNIQUE (initiative_id, campaign_key)
);
CREATE INDEX IF NOT EXISTS campaign_initiative_idx ON corex.campaign (initiative_id);
CREATE INDEX IF NOT EXISTS campaign_audience_idx   ON corex.campaign (audience_id);
CREATE INDEX IF NOT EXISTS campaign_group_idx      ON corex.campaign (group_id);
CREATE INDEX IF NOT EXISTS campaign_key_idx        ON corex.campaign (campaign_key);

-- ── corex.lead ─────────────────────────────────────────────────────────────
-- A contact-IN-a-campaign (the provider duplicates the same human across
-- campaigns). References a passive contact. PER-LEAD DERIVATIONS LIVE HERE:
-- parallel.ai research, Clay-compiled copy, the materialized mailer — each its own
-- jsonb bundle with a typed status + timestamp so progress is queryable.
CREATE TABLE IF NOT EXISTS corex.lead (
    lead_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id    uuid NOT NULL REFERENCES corex.campaign (campaign_id),
    contact_id     uuid NOT NULL REFERENCES corex.contact (contact_id),
    provider_lead_id text,
    enrolled_from_audience_id uuid REFERENCES corex.audience (audience_id),
    contactable    jsonb NOT NULL DEFAULT '{}'::jsonb,  -- {email, email_status, phone, mail_address}

    research_status  text NOT NULL DEFAULT 'pending'
                     CHECK (research_status IN ('pending','running','ready','failed','skipped')),
    research         jsonb NOT NULL DEFAULT '{}'::jsonb,
    research_at      timestamptz,
    copy_status      text NOT NULL DEFAULT 'pending'
                     CHECK (copy_status IN ('pending','drafting','ready','approved','failed')),
    copy             jsonb NOT NULL DEFAULT '{}'::jsonb,  -- {merge_vars:{...}, variant, template_id}
    copy_at          timestamptz,
    mailer_status    text NOT NULL DEFAULT 'pending'
                     CHECK (mailer_status IN ('pending','materializing','ready','sent','failed')),
    mailer           jsonb NOT NULL DEFAULT '{}'::jsonb,  -- {qr_token, dub_link, vapi_number, landing_slug, supply_campaign_key}
    mailer_at        timestamptz,

    status         text NOT NULL DEFAULT 'enrolled'
                   CHECK (status IN ('enrolled','researched','drafted','materialized','sent','engaged','disqualified')),
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (campaign_id, contact_id)                   -- one human once per campaign
);
CREATE INDEX IF NOT EXISTS lead_campaign_idx      ON corex.lead (campaign_id);
CREATE INDEX IF NOT EXISTS lead_contact_idx       ON corex.lead (contact_id);
CREATE INDEX IF NOT EXISTS lead_status_idx        ON corex.lead (status);
CREATE INDEX IF NOT EXISTS lead_provider_lead_idx ON corex.lead (provider_lead_id);

-- ── corex.send ─────────────────────────────────────────────────────────────
-- The provider's ATOMIC record — 1 lead × 1 touch, "as the provider reports it."
-- Shaped so Lob / dub / Vapi webhooks later reconcile inbound↔outbound on ONE key:
-- (provider, provider_send_id) is the universal outbound join target; inbound_ref
-- carries the artifact an inbound event stamps (QR/dub short-id, Vapi number,
-- landing slug) so engagement attributes back to exactly this send.
CREATE TABLE IF NOT EXISTS corex.send (
    send_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    lead_id          uuid NOT NULL REFERENCES corex.lead (lead_id),
    campaign_id      uuid NOT NULL REFERENCES corex.campaign (campaign_id),  -- denormalized for fast rollup
    provider         text NOT NULL CHECK (provider IN ('emailbison','lob','vapi')),
    provider_send_id text,                             -- Lob psc_… / EmailBison id / Vapi call id
    touch_no         smallint NOT NULL DEFAULT 1,
    direction        text NOT NULL DEFAULT 'outbound'
                     CHECK (direction IN ('outbound','inbound')),
    status           text NOT NULL DEFAULT 'queued'
                     CHECK (status IN ('queued','submitted','in_transit','delivered',
                                       'returned','responded','failed','canceled')),
    inbound_ref      jsonb NOT NULL DEFAULT '{}'::jsonb,  -- {dub_short_id, vapi_number, landing_slug, qr_token}
    events           jsonb NOT NULL DEFAULT '[]'::jsonb,  -- append-only webhook landing zone
    last_event_at    timestamptz,
    submitted_at     timestamptz,
    delivered_at     timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (provider, provider_send_id)                -- a provider's atomic id → exactly one send
);
CREATE INDEX IF NOT EXISTS send_lead_idx         ON corex.send (lead_id);
CREATE INDEX IF NOT EXISTS send_campaign_idx     ON corex.send (campaign_id);
CREATE INDEX IF NOT EXISTS send_provider_sid_idx ON corex.send (provider, provider_send_id);
CREATE INDEX IF NOT EXISTS send_status_idx       ON corex.send (status);
CREATE INDEX IF NOT EXISTS send_inbound_ref_gin  ON corex.send USING gin (inbound_ref jsonb_path_ops);

-- ── deferred FK: initiative → its audience pair (1:1, optional back-reference) ──
ALTER TABLE corex.initiative
    ADD COLUMN IF NOT EXISTS audience_pair_id uuid REFERENCES corex.gtm_audience_pair (pair_id);
CREATE INDEX IF NOT EXISTS initiative_pair_idx ON corex.initiative (audience_pair_id);
