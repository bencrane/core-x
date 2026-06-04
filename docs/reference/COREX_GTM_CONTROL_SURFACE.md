# corex GTM Control Surface — Blueprint (DESIGN ONLY)

Status: **design, ready-to-apply**. No DDL has been applied to the live database;
no MCP code has shipped. This document is the buildable specification for the next
(additive) pass.

The `corex` schema is the **operational/application state** for a managed
go-to-market agent (the *gtm-agent*) that drives an end-to-end direct-mail +
multichannel GTM motion *conversationally* through the existing **gtm-mcp** FastMCP
gateway (`apps/gtm_mcp`). It carries the motion as data so the flow is replicable,
not hand-done in chat.

Hard architectural split (honored throughout):

- **Entity / audience / analytical data → Lance** (`s3://data-sink/active/*`,
  served by gtm-mcp). corex never copies a company, a contact graph, or an award
  row into Postgres. It stores **pointers** (`company_id`, `contact_id`,
  `recipient_uei`), **the audience SQL + its stamp**, and **per-lead derivations**.
  The analytical rows are re-resolved from Lance at query/materialize time.
- **Operational state → Postgres** (hq-x Supabase, the same DB gtm-mcp ATTACHes as
  `hqx` and writes via psycopg in `tools/ops.py`). corex is that operational state.

Everything corex writes goes through the **same structured, parameterized,
transaction-bounded psycopg upsert pattern** that `ops.save_campaign_audience`
already uses (Directive 18 §4). The agent-facing raw-SQL path
(`execute_audience_query`) stays read-only-gated; corex mutation never rides it.

---

## 0. Substrate verified (read-only introspection, 2026-06-04)

Confirmed against the live hq-x Postgres and the R2 Lance lake before designing:

| Check | Result |
|---|---|
| `corex` schema / `corex%` tables exist? | **No** — zero collision. Safe to `CREATE SCHEMA corex`. |
| `business.*` schema (legacy GTM spine) | **Present** — 56 tables (`business.campaigns`, `recipients`, `channel_campaigns`, `gtm_agent_registry`, …). Deliberately **not reused** — corex is a clean schema by choice, not by absence. No `entities` schema exists. |
| `ops.campaign_audiences` | **Present** (1 row; the `save_campaign_audience` write path). corex mirrors its exact shape (uuid PK, jsonb payload, `created_at`/`updated_at`, UNIQUE natural key, idempotent `CREATE … IF NOT EXISTS` DDL). |
| `ops.email_resolutions`, `business.gtm_agent_registry` | **Both present.** `ops.email_resolutions` is the verified-email source (key `contact_id`; FSB = `bob@floridasuretybonds.com`, `verified`, `icypeas`) — `enroll_leads_from_audience` joins it. `business.gtm_agent_registry` exists (optional owning-agent ref on `initiative`). |
| FSB company seed (`companies` Lance) | ✓ `company_id ff99f65d-c15e-47e5-899a-dbc1bc1cf484` → "Florida Surety Bonds", `normalized_domain floridasuretybonds.com`. Columns: `company_id, company_name, normalized_domain, company_linkedin_url, source_platform`. |
| Bob O'Linn seed (`people` Lance) | ✓ `contact_id e0d90587-5288-4004-89dc-fd716e04f516` → "Bob O'Linn", title "Vice President", FK `company_id` → FSB. Columns: `contact_id, company_id, normalized_domain, full_name, first_name, last_name, title, person_linkedin_url, source_platform`. |
| `contractor_award_summary` (awards) | ✓ keyed by `recipient_uei`; carries dollar buckets, agency rollups, and the newly-indexed `primary_naics` / `primary_psc` (commit #119) for cohort selection. |
| `usaspending/award_search` (supply cohort source) | ✓ 154 cols incl. `recipient_uei, recipient_name, award_amount, total_obligation, generated_pragmatic_obligation, naics_code, recipient_location_address_line1/2/3, recipient_location_city_name, recipient_location_state_code, recipient_location_zip5/zip4`. Mail recipients materialize directly off the `recipient_location_*` fields. |
| Domain↔UEI bridges in lake | ✓ `sam_master_domains`, `sam_master_entities`, `sam_entity_master`, `crosswalk_sam_usaspending` — the demand company (domain) → SAM entity → UEI → award path, and UEI → mailing address for supply. |

Implication that shaped the model: **corex holds no analytical columns.** A demand
company is a `company_id`; its UEI/awards/address are resolved from Lance through
the bridges by `execute_audience_query` when needed. This keeps Postgres small and
keeps Lance the single system of record.

---

## 1. The `corex` schema — DDL

ER sketch (containment tree solid, references dashed):

```
                       corex.initiative                 gtm_side ∈ {demand,supply}, brand, thesis
                        │   │
        gtm_audience_pair   │  (0..1 per initiative; demand_audience_id + supply_audience_id + thesis)
              ╎ ╎           │
   corex.audience ╎╴╴╴╴╴╴╴╴╴┤  (stamped SQL selection; reusable; lives on no level)
        ▲ ▲                 │
        ╎ ╎                 ▼
        ╎ ╎          corex.campaign ───────╴╴╴▶ corex.campaign_group   (bag/wave tag; owns no audience)
        ╎ ╎          channel,provider,campaign_key, audience_id ╴╴┘
        ╎ ╎                 │
        ╎ ╎                 ▼
        ╎ ╎          corex.lead ───────────╴╴╴▶ corex.contact          (passive identity; company_id+contact_id)
        ╎ ╎          contact_id, research jsonb, copy jsonb, mailer jsonb
        ╎ ╎                 │
        ╎ ╎                 ▼
        ╎ ╎          corex.send            provider, provider_send_id, status, touch_no, events jsonb
        ╎ ╎                 ▲
        ╎ ╎                 ╎  inbound (QR/dub, inbound call/vapi, site visit) attributes back via stamped artifact
        └─╎─ demand_audience_id          (selects demand-side leads)
          └ supply_audience_id           (materialized into the supply campaign on call-book)
```

Conventions (mirrors `ops.campaign_audiences` + the house `ops.*_runs` idiom):
`uuid` PKs (the agent passes ids around between tool calls; `gen_random_uuid()`
default), `text` enums guarded by `CHECK` (no PG `ENUM` type — additive evolution
without `ALTER TYPE`), `timestamptz` audit columns defaulting `now()`, `jsonb` for
evolving bundles, hard `BTREE` indexes on every FK + every load-bearing lookup key,
natural-key `UNIQUE` constraints as upsert conflict targets. The whole script is
idempotent (`CREATE … IF NOT EXISTS`) so it self-bootstraps exactly like the ops
write path — **and installs with no `DROP`/`TRUNCATE`** (the PreToolUse hook
constraint is satisfied by construction).

```sql
-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║  corex — GTM control surface (operational state for the gtm-agent).        ║
-- ║  Additive, idempotent. Pointers + derivations only; entities live in Lance.║
-- ╚══════════════════════════════════════════════════════════════════════════╝
CREATE SCHEMA IF NOT EXISTS corex;

-- pgcrypto provides gen_random_uuid(); Supabase ships it. Guarded so the script
-- is safe even where it is preinstalled.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ── corex.contact ──────────────────────────────────────────────────────────
-- Rationale: PASSIVE identity only — a person at a company, keyed to the Lance
-- graph. Holds nothing strategic; the bundle hangs on the lead, never here.
-- De-duplicated by Lance contact_id so the same human is one corex.contact row
-- across every campaign that enrolls them.
CREATE TABLE IF NOT EXISTS corex.contact (
    contact_id        uuid PRIMARY KEY,              -- == Lance people.contact_id (NOT generated)
    company_id        uuid NOT NULL,                 -- == Lance companies.company_id
    normalized_domain text,                          -- denormalized anchor (matches Lance)
    full_name         text,
    title             text,
    -- Stamped identity snapshot at first sight (provenance, not a live mirror).
    -- Resolution keys (e.g. recipient_uei once bridged, verified email) live here
    -- as discovered pointers, never analytical payload.
    identity          jsonb NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at     timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS contact_company_id_idx        ON corex.contact (company_id);
CREATE INDEX IF NOT EXISTS contact_normalized_domain_idx ON corex.contact (normalized_domain);

-- ── corex.audience ─────────────────────────────────────────────────────────
-- Rationale: a STAMPED selection over the committed Lance lake = the DuckDB SQL
-- + a stamp {row_count, run_at, headline stats}. Produced by running gtm-mcp's
-- execute_audience_query. Reusable and side-agnostic: one row serves either side
-- of a pair. Hangs on NO containment level — campaigns and pairs reference it.
CREATE TABLE IF NOT EXISTS corex.audience (
    audience_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name          text NOT NULL,                     -- human label, e.g. 'FL surety carriers'
    gtm_side      text CHECK (gtm_side IN ('demand','supply')),  -- intended role; NULL = unbound/reusable
    -- The selection itself. source_sql is recorded DATA (never executed by a
    -- write tool); result_key names the id column the rows resolve to.
    source_sql    text NOT NULL,
    result_key    text NOT NULL DEFAULT 'company_id',-- 'company_id' | 'contact_id' | 'recipient_uei'
    datasets      jsonb NOT NULL DEFAULT '[]'::jsonb,-- Lance datasets the SQL named (provenance)
    -- The STAMP: what the query returned the last time it was run + committed.
    row_count     bigint,
    headline_stats jsonb NOT NULL DEFAULT '{}'::jsonb,-- e.g. {total_obligated, median_award, states}
    last_run_at   timestamptz,                       -- when the stamp was taken (NULL = defined, not yet run)
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audience_gtm_side_idx   ON corex.audience (gtm_side);
CREATE INDEX IF NOT EXISTS audience_last_run_at_idx ON corex.audience (last_run_at DESC);

-- ── corex.initiative ───────────────────────────────────────────────────────
-- Rationale: the strategic frame and root of the containment tree. gtm_side
-- forks the SAME shape into two trees; brand inherits down. Carries the
-- thesis-as-data via an optional 1:1 audience pair (FK below, added after both
-- tables exist).
CREATE TABLE IF NOT EXISTS corex.initiative (
    initiative_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    gtm_side      text NOT NULL CHECK (gtm_side IN ('demand','supply')),
    brand         text NOT NULL,                     -- demand→OutboundSolutions.com; supply→client brand
    name          text NOT NULL,
    thesis        text,                              -- prose thesis; the structured pair carries the data
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
-- Rationale: the strategic seed — TWO DISTINCT audiences bound with a thesis,
-- living ON the initiative (1:1; see resolution #2). The demand audience selects
-- demand-side leads; the supply audience is materialized into the supply-side
-- campaign on call-book. The same corex.audience object serves both sides; the
-- PAIR binds them.
CREATE TABLE IF NOT EXISTS corex.gtm_audience_pair (
    pair_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    initiative_id      uuid NOT NULL UNIQUE           -- 1:1 with initiative (see #2)
                       REFERENCES corex.initiative (initiative_id),
    demand_audience_id uuid NOT NULL REFERENCES corex.audience (audience_id),
    supply_audience_id uuid NOT NULL REFERENCES corex.audience (audience_id),
    thesis             text NOT NULL,                 -- "FL surety carriers ↔ fed-award winners needing bonds"
    metadata           jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    CHECK (demand_audience_id <> supply_audience_id)  -- two DISTINCT audiences
);
CREATE INDEX IF NOT EXISTS pair_demand_audience_idx ON corex.gtm_audience_pair (demand_audience_id);
CREATE INDEX IF NOT EXISTS pair_supply_audience_idx ON corex.gtm_audience_pair (supply_audience_id);

-- ── corex.campaign_group ───────────────────────────────────────────────────
-- Rationale: JUST a bag of campaigns (reporting / waves). Owns NO audience.
-- A campaign references it as a tag; it is not a parent in the tree.
CREATE TABLE IF NOT EXISTS corex.campaign_group (
    group_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    initiative_id uuid REFERENCES corex.initiative (initiative_id),  -- optional scoping
    name          text NOT NULL,
    metadata      jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (initiative_id, name)
);

-- ── corex.campaign ─────────────────────────────────────────────────────────
-- Rationale: the EmailBison / Lob / Vapi send unit. channel + provider are
-- COLUMNS — the campaign is already channel-specific (no channel_campaign
-- entity). Points at one corex.audience via audience_id; optionally tagged into a
-- campaign_group. campaign_key is the deterministic human handle
-- (audience_quality_segment, e.g. AUD4343_VALIDATED_VPs); exact format deferred.
CREATE TABLE IF NOT EXISTS corex.campaign (
    campaign_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    initiative_id uuid NOT NULL REFERENCES corex.initiative (initiative_id),
    audience_id   uuid REFERENCES corex.audience (audience_id),  -- the selection this campaign enrolls
    group_id      uuid REFERENCES corex.campaign_group (group_id),
    campaign_key  text NOT NULL,                      -- deterministic, e.g. 'AUD4343_VALIDATED_VPs'
    channel       text NOT NULL CHECK (channel  IN ('email','direct_mail','voice')),
    provider      text NOT NULL CHECK (provider IN ('emailbison','lob','vapi')),
    -- The campaign as the provider knows it (its native campaign/list id), set
    -- once the provider-side object exists.
    provider_campaign_id text,
    status        text NOT NULL DEFAULT 'draft'
                  CHECK (status IN ('draft','enrolling','live','paused','complete')),
    config        jsonb NOT NULL DEFAULT '{}'::jsonb, -- template ids, mail_class, cadence, merge spec
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    -- channel ⟂ provider sanity: a provider only serves its native channel.
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
-- Rationale: a contact-IN-a-campaign (the provider duplicates the same human
-- across campaigns, so the same contact_id yields many leads). References a
-- passive corex.contact. PER-LEAD DERIVATIONS LIVE HERE (resolution #1):
-- parallel.ai research, Clay-compiled copy merge-vars, the materialized
-- mailer/supply-campaign instance — each its own jsonb bundle with a typed
-- status + timestamp so progress is queryable without parsing the blob.
CREATE TABLE IF NOT EXISTS corex.lead (
    lead_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id    uuid NOT NULL REFERENCES corex.campaign (campaign_id),
    contact_id     uuid NOT NULL REFERENCES corex.contact (contact_id),
    -- The provider's own lead id (EmailBison lead, Lob recipient, Vapi contact),
    -- set when the lead is pushed to the provider.
    provider_lead_id text,
    -- enrollment provenance: which audience run minted this lead, and the email
    -- (or other contactable) resolved at enroll time — a stamped pointer, not a
    -- live mirror of ops.* / Lance.
    enrolled_from_audience_id uuid REFERENCES corex.audience (audience_id),
    contactable    jsonb NOT NULL DEFAULT '{}'::jsonb,  -- {email, email_status, phone, mail_address}

    -- ── per-lead derivation bundles (each: typed status + timestamp + jsonb) ──
    -- parallel.ai research: "how does THIS company serve the supply audience".
    research_status  text NOT NULL DEFAULT 'pending'
                     CHECK (research_status IN ('pending','running','ready','failed','skipped')),
    research         jsonb NOT NULL DEFAULT '{}'::jsonb,
    research_at      timestamptz,
    -- Clay-compiled copy merge-vars (what the template interpolates).
    copy_status      text NOT NULL DEFAULT 'pending'
                     CHECK (copy_status IN ('pending','drafting','ready','approved','failed')),
    copy             jsonb NOT NULL DEFAULT '{}'::jsonb,  -- {merge_vars:{...}, variant, template_id}
    copy_at          timestamptz,
    -- Materialized mailer / supply-campaign instance for this lead (the physical
    -- piece on call-book). Holds the stamped artifacts that inbound attributes to.
    mailer_status    text NOT NULL DEFAULT 'pending'
                     CHECK (mailer_status IN ('pending','materializing','ready','sent','failed')),
    mailer           jsonb NOT NULL DEFAULT '{}'::jsonb,  -- {qr_url, dub_link, vapi_number, landing_slug, supply_campaign_key}
    mailer_at        timestamptz,

    status         text NOT NULL DEFAULT 'enrolled'
                   CHECK (status IN ('enrolled','researched','drafted','materialized','sent','engaged','disqualified')),
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    -- one human appears once per campaign (the provider's own dedup contract)
    UNIQUE (campaign_id, contact_id)
);
CREATE INDEX IF NOT EXISTS lead_campaign_idx    ON corex.lead (campaign_id);
CREATE INDEX IF NOT EXISTS lead_contact_idx     ON corex.lead (contact_id);
CREATE INDEX IF NOT EXISTS lead_status_idx      ON corex.lead (status);
CREATE INDEX IF NOT EXISTS lead_provider_lead_idx ON corex.lead (provider_lead_id);

-- ── corex.send ─────────────────────────────────────────────────────────────
-- Rationale: the provider's ATOMIC record — 1 lead × 1 touch, "as the provider
-- reports it." Multi-touch = multiple sends per lead distinguished by touch_no.
-- Shaped (resolution #4) so Lob / dub / Vapi webhook events later reconcile
-- inbound↔outbound on ONE key: provider + provider_send_id is the universal join
-- target, and inbound_ref carries the artifact an inbound event stamps (QR/dub
-- short-id, inbound Vapi call id, landing slug) so engagement attributes back to
-- exactly this send without a guess.
CREATE TABLE IF NOT EXISTS corex.send (
    send_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    lead_id          uuid NOT NULL REFERENCES corex.lead (lead_id),
    campaign_id      uuid NOT NULL REFERENCES corex.campaign (campaign_id),  -- denormalized for fast rollup
    provider         text NOT NULL CHECK (provider IN ('emailbison','lob','vapi')),
    -- The provider's atomic id: Lob psc_… / EmailBison send id / Vapi call id.
    -- The reconciliation key. Unique per provider (a provider id is globally
    -- unique within that provider).
    provider_send_id text,
    touch_no         smallint NOT NULL DEFAULT 1,     -- multi-touch sequence within the lead
    direction        text NOT NULL DEFAULT 'outbound'
                     CHECK (direction IN ('outbound','inbound')),
    status           text NOT NULL DEFAULT 'queued'
                     CHECK (status IN ('queued','submitted','in_transit','delivered',
                                       'returned','responded','failed','canceled')),
    -- The inbound attribution handle stamped onto the physical piece, so a QR
    -- scan (dub), inbound call (vapi), or site visit (landing) maps back here.
    inbound_ref      jsonb NOT NULL DEFAULT '{}'::jsonb,  -- {dub_short_id, vapi_number, landing_slug, qr_token}
    -- The provider's append-only event stream (webhook bodies land here later).
    events           jsonb NOT NULL DEFAULT '[]'::jsonb,
    last_event_at    timestamptz,
    submitted_at     timestamptz,
    delivered_at     timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    -- the reconciliation contract: a provider's atomic id maps to exactly one send
    UNIQUE (provider, provider_send_id)
);
CREATE INDEX IF NOT EXISTS send_lead_idx        ON corex.send (lead_id);
CREATE INDEX IF NOT EXISTS send_campaign_idx    ON corex.send (campaign_id);
CREATE INDEX IF NOT EXISTS send_provider_sid_idx ON corex.send (provider, provider_send_id);
CREATE INDEX IF NOT EXISTS send_status_idx      ON corex.send (status);
-- inbound reconciliation lookups hit the stamped artifacts inside inbound_ref;
-- a GIN index makes "find the send whose dub_short_id = X" a single probe.
CREATE INDEX IF NOT EXISTS send_inbound_ref_gin ON corex.send USING gin (inbound_ref jsonb_path_ops);

-- ── deferred FK: initiative → its audience pair (1:1, optional) ─────────────
-- Added after gtm_audience_pair exists. The pair already enforces 1:1 via its
-- UNIQUE(initiative_id); this back-reference lets a reader jump initiative→pair
-- directly. Nullable: an initiative may exist before its pair is defined.
ALTER TABLE corex.initiative
    ADD COLUMN IF NOT EXISTS audience_pair_id uuid REFERENCES corex.gtm_audience_pair (pair_id);
CREATE INDEX IF NOT EXISTS initiative_pair_idx ON corex.initiative (audience_pair_id);
```

Table inventory:

| Table | Role | Key edges |
|---|---|---|
| `corex.initiative` | strategic frame; tree root; forks demand/supply | → `gtm_audience_pair` (1:1) |
| `corex.gtm_audience_pair` | thesis-as-data: two distinct audiences bound | → `audience` ×2; ⟂ `initiative` |
| `corex.audience` | stamped Lance selection (SQL + stamp); reusable | referenced by pair + campaign |
| `corex.campaign_group` | bag/wave tag; owns no audience | tag on campaign |
| `corex.campaign` | send unit; channel+provider columns; mints `campaign_key` | parent `initiative`; → `audience` |
| `corex.lead` | contact-in-campaign; per-lead derivations (research/copy/mailer) | parent `campaign`; → `contact` |
| `corex.contact` | passive identity, keyed to Lance graph | referenced by lead |
| `corex.send` | provider atomic record; inbound↔outbound reconciliation key | parent `lead` |

---

## 2. gtm-mcp tool surface

Each tool is a **purpose-built, typed psycopg upsert** (or read) that mirrors
`tools/ops.py` exactly: own psycopg connection on `database.hqx_dsn()`, an
idempotent ensure-DDL preamble (the §1 script), the mutation inside one
`conn.transaction()`, all values parameterized, `jsonb` bound with
`psycopg.types.json.Jsonb`. Writes return `{"status":"ok","operation":
"inserted"|"updated","row":{…}}`. The audience-running tools call the existing
`audience.execute_audience_query` internally — they do not re-implement SQL
execution. New tool module: `apps/gtm_mcp/src/tools/corex.py`, mounted in
`main.py` alongside the others.

Naming maps one tool per real conversational step. Every tool earns its place.

### Write / advance tools

| Tool | Signature | Reads | Writes | Conversational step |
|---|---|---|---|---|
| `create_initiative` | `(gtm_side, brand, name, thesis=None, client_domain=None, metadata=None) → {initiative_id, …}` | — | `corex.initiative` | "Start a demand-side initiative on OutboundSolutions targeting FL surety carriers." |
| `define_audience_pair` | `(initiative_id, demand_sql, supply_sql, thesis, demand_name, supply_name, demand_result_key='company_id', supply_result_key='recipient_uei', run=True) → {pair_id, demand_audience:{audience_id,row_count,…}, supply_audience:{…}}` | runs both SQLs via `execute_audience_query` (read-only-gated) | `corex.audience` ×2 (with stamps), `corex.gtm_audience_pair`, back-links `initiative.audience_pair_id` | "The thesis: demand = FL surety carriers; supply = fed-award winners >$150K needing bonds. Define and stamp both." |
| `define_audience` | `(name, source_sql, gtm_side=None, result_key='company_id', run=True) → {audience_id, row_count, headline_stats, …}` | runs SQL via `execute_audience_query` | `corex.audience` | standalone/reusable audience outside a pair (e.g. a second supply wave). |
| `create_campaign` | `(initiative_id, channel, provider, audience_id=None, group_id=None, campaign_key=None, config=None) → {campaign_id, campaign_key, …}` | `corex.audience` (validate), `corex.initiative` | `corex.campaign` | "Open an email campaign on EmailBison over the demand audience." Mints `campaign_key` deterministically if omitted. |
| `enroll_leads_from_audience` | `(campaign_id, audience_id=None, limit=1000, contactable_from='lake') → {enrolled, leads:[{lead_id,contact_id}], …}` | re-runs the audience SQL via `execute_audience_query`; resolves `contact_id`/email from the lake (companies⋈people, bridges) | upserts `corex.contact` (passive identity) + `corex.lead` (one per resolved contact, `UNIQUE(campaign_id,contact_id)`) | "Enroll the demand audience as leads in this campaign." |
| `attach_research` | `(lead_id, research, status='ready') → {lead_id, research_status, …}` | — | `corex.lead.research*` | parallel.ai output: "Record how FSB serves the supply audience." (Bulk variant: `lead_ids[]`.) |
| `draft_copy` | `(lead_id, merge_vars, variant=None, template_id=None, status='ready') → {lead_id, copy_status, …}` | — | `corex.lead.copy*` | Clay-compiled merge-vars: "Stage the personalized copy for this lead." |
| `materialize_supply_campaign` | `(lead_id OR campaign_id, supply_audience_id=None, mailer_spec=None) → {leads:[{lead_id, mailer:{…}, send_id}], supply_campaign_key}` | reads the pair's `supply_audience_id`; runs the supply SQL to resolve mail recipients + addresses from `usaspending/award_search` | `corex.lead.mailer*`, mints the supply-side `corex.campaign` (channel `direct_mail`, provider `lob`) on first call, creates `corex.send` rows (queued, `inbound_ref` stamped) | **call-book**: "Materialize the mailer — instantiate the supply campaign from the pair and stamp the physical pieces." |
| `record_send` | `(lead_id, provider, provider_send_id, touch_no=1, status='submitted', inbound_ref=None) → {send_id, …}` | — | `corex.send` (upsert on `(provider,provider_send_id)`) | record the provider's atomic send once dispatched (also the row a later webhook reconciler updates). |
| `update_status` | `(object_type, object_id, status) → {…}` | — | the named table's `status` | "Pause the campaign." / "Disqualify this lead." Generic lifecycle nudge over the CHECK-constrained enums. |

### Read / reporting tools

| Tool | Signature | Returns |
|---|---|---|
| `get_initiative` | `(initiative_id) → {initiative, pair:{demand_audience, supply_audience}, campaigns:[…]}` | the tree from the root, one hop down, with audience stamps. |
| `list_campaigns` | `(initiative_id=None, group_id=None, status=None) → {campaigns:[…]}` | campaign roster with channel/provider/key/audience + lead counts. |
| `get_campaign_funnel` | `(campaign_id) → {campaign, counts:{leads_by_status, sends_by_status}, audience_stamp}` | the conversational dashboard: enrolled→researched→drafted→materialized→sent→engaged + send delivery rollup. |
| `get_lead` | `(lead_id) → {lead, contact, research, copy, mailer, sends:[…]}` | the full per-lead bundle for inspection mid-conversation. |

Existing tools the agent keeps using unchanged (corex composes on top, does not
replace): `list_datasets` / `describe_dataset` (find columns to write audience
SQL), `execute_audience_query` (the read engine the write tools call), the
`search_*` point-lookups, `list_postgres_tables` / `get_postgres_schema`
(introspect corex itself once built), `launch_contact_hydration` (fill in
contacts when an audience is companies-only), and the `dmaas.*` Lob stubs (the
real fulfillment `materialize_supply_campaign` will call once wired).

Surface stays tight: **10 write/advance + 4 read = 14 corex tools**, each a
distinct conversational move, none overlapping an existing tool.

---

## 3. Worked FSB example — conversation → tool calls → rows

The real seeded "small circle." Demand side mails FL surety carriers (to win them
as Outbound Solutions clients); the bound supply side is the carrier's own market —
federal-award winners >$150K who need bonding — which materializes into the
direct-mail campaign on call-book. UUIDs below marked `‹gen›` are minted by
`gen_random_uuid()` at write time; Lance ids are real and verified.

**Turn 1 — frame the initiative.**
> "Stand up a demand-side initiative on OutboundSolutions.com going after Florida
> surety-bond carriers."

```
create_initiative(
  gtm_side="demand", brand="OutboundSolutions.com",
  name="FL Surety Carriers Q3", thesis="Win FL surety carriers as DMaaS clients")
→ initiative_id = ‹gen-INIT›
```
`corex.initiative` row: `{initiative_id:‹gen-INIT›, gtm_side:'demand',
brand:'OutboundSolutions.com', name:'FL Surety Carriers Q3', status:'active'}`.

**Turn 2 — define the thesis as a bound audience pair.**
> "Thesis: demand = Florida surety-bond carriers; supply = federal-award winners
> over $150K who need bonds. Stamp both."

```
define_audience_pair(
  initiative_id=‹gen-INIT›,
  thesis="FL surety carriers ↔ fed-award winners >$150K needing bonds",
  demand_name="FL surety carriers", demand_result_key="company_id",
  demand_sql="""
    SELECT company_id, company_name, normalized_domain
    FROM companies
    WHERE lower(company_name) LIKE '%surety%'
       OR normalized_domain = 'floridasuretybonds.com'   -- seeded small circle
  """,
  supply_name="Fed-award winners >$150K (bondable)", supply_result_key="recipient_uei",
  supply_sql="""
    SELECT recipient_uei, recipient_name,
           recipient_location_address_line1, recipient_location_city_name,
           recipient_location_state_code, recipient_location_zip5,
           max(generated_pragmatic_obligation) AS award_amount
    FROM "usaspending/award_search"
    WHERE generated_pragmatic_obligation >= 150000
      AND naics_code IN ('236220','237310','237990')  -- construction → bond-heavy
      AND recipient_location_state_code = 'FL'
    GROUP BY 1,2,3,4,5,6
  """)
→ pair_id=‹gen-PAIR›,
  demand_audience={audience_id:‹gen-AUDd›, row_count:1, headline_stats:{...}},
  supply_audience={audience_id:‹gen-AUDs›, row_count:N, headline_stats:{total_obligated:…, median_award:…}}
```
Writes: two `corex.audience` rows (each `source_sql` + stamp `row_count`/`last_run_at`/`headline_stats`, the demand one `result_key='company_id'`, supply `'recipient_uei'`), one `corex.gtm_audience_pair`
`{demand_audience_id:‹gen-AUDd›, supply_audience_id:‹gen-AUDs›}`, and
`initiative.audience_pair_id := ‹gen-PAIR›`. The demand stamp = **1 row** (FSB,
`company_id ff99f65d-…`).

**Turn 3 — open the demand campaign.**
> "Open an EmailBison campaign over the demand audience."

```
create_campaign(
  initiative_id=‹gen-INIT›, channel="email", provider="emailbison",
  audience_id=‹gen-AUDd›, campaign_key="FLSURETY_VALIDATED_DM")
→ campaign_id=‹gen-CAMP›, campaign_key="FLSURETY_VALIDATED_DM"
```
`corex.campaign` row: `{campaign_id:‹gen-CAMP›, initiative_id:‹gen-INIT›,
audience_id:‹gen-AUDd›, channel:'email', provider:'emailbison',
campaign_key:'FLSURETY_VALIDATED_DM', status:'draft'}`.

**Turn 4 — enroll FSB as a lead.**
> "Enroll the demand audience."

```
enroll_leads_from_audience(campaign_id=‹gen-CAMP›, audience_id=‹gen-AUDd›)
→ enrolled=1, leads=[{lead_id:‹gen-LEAD›, contact_id:e0d90587-5288-4004-89dc-fd716e04f516}]
```
The tool re-runs the demand SQL (→ `company_id ff99f65d-…`), resolves the best
contact at that company from the lake (`people` where `company_id` = FSB →
Bob O'Linn `e0d90587-…`, VP), and writes:
- `corex.contact` `{contact_id:e0d90587-…, company_id:ff99f65d-…,
  normalized_domain:'floridasuretybonds.com', full_name:"Bob O'Linn",
  title:'Vice President'}` (passive identity; upserted, deduped on `contact_id`).
- `corex.lead` `{lead_id:‹gen-LEAD›, campaign_id:‹gen-CAMP›,
  contact_id:e0d90587-…, enrolled_from_audience_id:‹gen-AUDd›,
  contactable:{email:'bob@floridasuretybonds.com', email_status:'sourced'},
  status:'enrolled'}`.

(Email is carried as a stamped pointer on the lead, sourced from
`ops.email_resolutions` (JOIN on `contact_id`) at enroll — for Bob that resolves
`bob@floridasuretybonds.com` (`verified`, `icypeas`). If the audience were
companies-only, `launch_contact_hydration` fills contacts first, then enroll.)

**Turn 5 — attach per-lead research.**
> "Record how Florida Surety Bonds serves the supply audience."

```
attach_research(lead_id=‹gen-LEAD›, status="ready", research={
  "angle":"FSB writes contract surety for FL construction primes — exactly the "
          "fed-award winners >$150K in the supply audience",
  "supply_overlap_uei_sample":["…"], "source":"parallel.ai"})
→ lead.research_status='ready'
```
Updates `corex.lead`: `research` jsonb set, `research_status='ready'`,
`research_at=now()`, `status` advanced to `'researched'`.

**Turn 6 — draft the copy.**
> "Stage the personalized merge-vars for Bob."

```
draft_copy(lead_id=‹gen-LEAD›, variant="A", template_id="tmpl_fsb_letter",
  merge_vars={"first_name":"Bob","company":"Florida Surety Bonds",
              "supply_count":"N","top_naics":"Construction"})
→ lead.copy_status='ready'
```
Updates `corex.lead.copy*`; `status → 'drafted'`.

**Turn 7 — materialize on call-book.**
> "Book it. Materialize the mailer — spin up the supply campaign from the pair."

```
materialize_supply_campaign(lead_id=‹gen-LEAD›)
→ supply_campaign_key="FLSURETY_SUPPLY_FedWinners",
  leads=[{lead_id:‹gen-LEAD›, mailer:{dub_link:…, qr_token:…, landing_slug:"fsb-bonds"},
          send_id:‹gen-SEND›}]
```
The tool reads the pair's `supply_audience_id (‹gen-AUDs›)`, runs the supply SQL
to resolve mail recipients + USPS addresses from `usaspending/award_search`, and:
- mints (first call) a supply-side `corex.campaign`
  `{channel:'direct_mail', provider:'lob', campaign_key:'FLSURETY_SUPPLY_FedWinners',
  initiative_id:‹gen-INIT›, audience_id:‹gen-AUDs›}`.
- sets `corex.lead.mailer` `{dub_link, qr_token, landing_slug, supply_campaign_key}`,
  `mailer_status='ready'`, `status → 'materialized'`.
- creates `corex.send` `{send_id:‹gen-SEND›, lead_id:‹gen-LEAD›,
  campaign_id:‹gen-CAMP›, provider:'lob', status:'queued', touch_no:1,
  inbound_ref:{dub_short_id, qr_token, landing_slug:'fsb-bonds'}}` — the row a Lob
  dispatch then stamps with `provider_send_id='psc_…'` via `record_send`, and the
  row a future dub/Vapi/landing webhook reconciles against on `inbound_ref`.

Resulting object graph for the small circle:
```
initiative ‹gen-INIT› (demand, OutboundSolutions.com)
  └ pair ‹gen-PAIR›  demand→audience ‹gen-AUDd› (FSB)   supply→audience ‹gen-AUDs› (fed winners)
  ├ campaign ‹gen-CAMP› (email/emailbison, FLSURETY_VALIDATED_DM, audience ‹gen-AUDd›)
  │   └ lead ‹gen-LEAD› (contact e0d90587 Bob O'Linn / company ff99f65d FSB)
  │        research:ready  copy:ready  mailer:ready
  │        └ send ‹gen-SEND› (lob, queued, inbound_ref stamped)
  └ campaign (direct_mail/lob, FLSURETY_SUPPLY_FedWinners, audience ‹gen-AUDs›)  ← minted on call-book
```

---

## 4. Open-point resolutions

**#1 — Per-lead derivations (research / copy / mailer) now that contact is passive.**
Put all three on `corex.lead`, each as a **typed status enum + a `*_at`
timestamptz + a `jsonb` bundle** — never on `corex.contact` (which stays passive)
and never as a separate child table. Justification: the derivations are
per-(contact × campaign), which is exactly the lead's grain — the same human in two
campaigns gets two distinct research/copy/mailer states, which a contact-level
column could not express and a child table would over-normalize for a prototype.
Typing only the *status* and *timestamp* (not the payload) keeps progress queryable
(`WHERE research_status='ready'` drives the funnel and the next conversational step)
while the evolving bundle — parallel.ai's variable output, Clay's merge-var set,
the mailer's stamped artifacts — stays in `jsonb` so the shape can grow without a
migration. The supply-side campaign is **instantiated on call-book** by
`materialize_supply_campaign` reading the pair, exactly as specified — the mailer
bundle records the physical instance and seeds the `corex.send` that inbound
attributes to.

**#2 — `gtm_audience_pair` ↔ initiative cardinality.**
**1:1, enforced** (`gtm_audience_pair.initiative_id UNIQUE`, with a nullable
`initiative.audience_pair_id` back-reference). Justification: the pair *is* the
initiative's thesis-as-data — one strategic frame carries one demand↔supply thesis;
two theses are two initiatives (cheap to create, and they fork cleanly by
`gtm_side`). Making it 1:1 keeps "the initiative's audiences" an unambiguous lookup
the read tools rely on, and avoids a join table for a relationship that is
conceptually singular. If a genuine multi-thesis initiative ever appears, dropping
the UNIQUE and promoting the back-reference to a child list is a forward-compatible,
additive change — the pair already stands as its own table, so nothing else moves.

**#3 — `execute_audience_query` results → `corex.audience` (the stamp) AND → leads
(enrollment), and supply → mail recipients on call-book.**
Three explicit steps, all routed through the one read engine:
- **Stamp:** `define_audience` / `define_audience_pair` run the SQL via
  `execute_audience_query` (read-only-gated, 1000-row cap), then persist the SQL
  verbatim plus a stamp `{row_count, last_run_at, headline_stats}` into
  `corex.audience`. The SQL is stored as DATA; the rows themselves are *not* copied
  into Postgres (they live in Lance) — the stamp is the durable, cheap summary.
- **Enrollment:** `enroll_leads_from_audience` **re-runs** the stored SQL at enroll
  time (freshness — Lance may have been recommitted since the stamp), resolves each
  result row's `contact_id` (and verified contactable) from the lake, and upserts
  `corex.contact` + `corex.lead`. Re-running, not reading a frozen snapshot, is the
  correct call because Lance datasets are overwritten in place and a stale snapshot
  would enroll the wrong universe; the stamp records what *was* seen, the live run
  drives what *gets enrolled*.
- **Supply materialization:** on call-book, `materialize_supply_campaign` runs the
  pair's supply SQL (which selects `recipient_uei` + `recipient_location_*` from
  `usaspending/award_search`), turning each row directly into a Lob `to_address` and
  a stamped physical piece. Because the supply audience resolves to real mailing
  addresses already present in the lake, no extra enrichment hop is needed for mail.

**#4 — `corex.send` shape for later Lob/dub/Vapi reconciliation.**
The send row is keyed for reconciliation from day one without building the webhooks:
`UNIQUE(provider, provider_send_id)` is the universal outbound join target (Lob
`psc_…`, EmailBison send id, Vapi call id all land here); `inbound_ref` jsonb (GIN
indexed) carries the artifacts stamped onto the physical piece — `dub_short_id`,
`qr_token`, `vapi_number`, `landing_slug` — so an inbound QR scan / call / site
visit maps back to exactly one send by probing that artifact; `events jsonb[]` is
the append-only landing zone a future webhook reconciler writes raw bodies into;
`direction` + `touch_no` let one lead carry an ordered multi-touch sequence and
inbound responses as sibling rows. Building the webhook endpoints later is a
fill-in that updates these columns — the row shape does not change.

**#5 — Minimal tool set.**
10 write/advance + 4 read (§2). Each maps to one conversational move and none
duplicates an existing gtm-mcp tool. `define_audience_pair` is the one composite
(it runs+stamps both sides and binds them) because "set the thesis" is a single
conversational act; `define_audience` exists for reusable singletons.
`materialize_supply_campaign` is the call-book pivot that reads the pair and mints
the supply campaign. `record_send` + `update_status` are the small primitives that
keep provider state and lifecycle current. The read tools are shaped as
*conversational dashboards* (`get_campaign_funnel`, `get_initiative`) rather than
raw table dumps, because the agent narrates state back to the operator. Nothing
read-only is duplicated — audience SQL still goes through `execute_audience_query`,
schema discovery through `list_datasets`/`describe_dataset`.

---

## 5. Build plan (next pass — additive only)

1. **Migration file.** `apps/gtm_mcp/migrations/0001_corex.sql` = the §1 script
   verbatim (idempotent `CREATE SCHEMA/TABLE/INDEX IF NOT EXISTS`, the two deferred
   `ALTER … ADD COLUMN IF NOT EXISTS`). Installs with **no `DROP`/`TRUNCATE`** — the
   PreToolUse hook never trips. Apply read-path-verified via psql under Doppler
   `core-x/prd`.
2. **DDL constant.** Embed the same script as `COREX_DDL` in
   `apps/gtm_mcp/src/tools/corex.py`, run idempotently at the top of each write tool's
   transaction — the self-bootstrap pattern `ops.save_campaign_audience` uses, so the
   tables exist even on a fresh DB with no out-of-band migration step.
3. **Tool module.** Implement `apps/gtm_mcp/src/tools/corex.py`: the 10 write +
   4 read tools (§2), each own-psycopg-connection + `conn.transaction()` +
   parameterized upsert + `Jsonb`-bound payloads, docstrings as the agent-facing
   contracts (the MCP client renders them verbatim). The audience-running tools call
   `audience.execute_audience_query` internally; the supply materializer calls the
   `dmaas.*` Lob path (stubs today) for fulfillment.
4. **Mount.** Add `from .src.tools import corex` and `corex.register(mcp)` in
   `apps/gtm_mcp/main.py`, after `ops.register(mcp)`.
5. **Determinism.** Implement `campaign_key` minting (the deferred format —
   proposal: `{AUDIENCE_SLUG}_{QUALITY}_{SEGMENT}`, e.g. `FLSURETY_VALIDATED_DM`)
   and the supply `campaign_key` derivation from the pair.
6. **Verify.** Replay the §3 FSB conversation end-to-end against the live seed
   (read-only audience runs + corex writes), assert the eight expected rows
   (`initiative`, `pair`, 2×`audience`, 2×`campaign`, `contact`, `lead`, `send`),
   and confirm `get_campaign_funnel` reports the lead at `materialized`.
7. **Tests.** Unit-test the CHECK constraints (channel⟂provider, distinct pair
   audiences), the upsert conflict targets (re-enroll is idempotent), and the
   read-only guard still rejects mutation on the audience path.

---

## Operator decisions before build

1. **`campaign_key` format.** Column shipped; exact mint format deferred. Proposal:
   `{AUDIENCE_SLUG}_{QUALITY}_{SEGMENT}`. Confirm or supply the canonical grammar
   (the example `AUD4343_VALIDATED_VPs` implies an audience *number*, not a slug —
   pick one, since the read tools surface it as the human handle).
2. **Verified-email source — RESOLVED.** `ops.email_resolutions` **is** present and
   is the source (key `contact_id`; FSB → `bob@floridasuretybonds.com`, `verified`).
   `enroll_leads_from_audience` joins it on `contact_id` to stamp the lead's
   `contactable`. No decision needed.
3. **gtm-agent registry.** `business.gtm_agent_registry` **is** present. Decide
   whether `corex.initiative` carries an owning `agent_id` (provenance: which managed
   agent drove the motion) — additive nullable FK if yes, skip if out of scope.
4. **Multi-touch policy.** `corex.send.touch_no` + `direction` support multi-touch
   and inbound siblings now. Confirm whether a touch *cadence* (delays between
   touch_no) should be a typed `campaign.config` contract or left free-form jsonb for
   the prototype.
5. **DDL install path.** Confirm the migration is applied via the in-tool
   self-bootstrap (option 2, zero out-of-band step) vs. a one-time explicit psql
   apply of `0001_corex.sql` — both are written; pick the canonical one for the repo.
