"""edge_api entrypoint — the public Anthropic Managed-Agents edge for core-x.

Strangler-fig extraction of the agent-facing surface out of the hq-x monolith:
the MCP mounts the gtm-agent calls (trigger / lob), the agent-runs streaming
proxy the platform-api BFF drives, and (optionally) the post-payment pipeline
seam. Each lands in its own phase.

  * Phase 0 — authenticated chassis (/, /healthz, /v1/_authcheck).
  * Phase 1 — `/mcp/trigger/` mounted (this file). Also resolves the original
    "TRIGGER_SECRET_KEY not configured on the server" error: the key now lives
    in core-x/prd, which this service reads.

Two auth boundaries:
  * MCP mounts <- Anthropic's managed-agents platform: bearer = DMAAS_MCP_BEARER_TOKEN,
    injected by the agent's Anthropic vault (scoped by mcp_server_url). ASGI gate:
    ``src/mcp_bearer.py``.
  * agent-runs + pipeline <- platform-api BFF / Trigger.dev: bearer =
    EDGE_API_SERVICE_TOKEN, constant-time compared (``src/service_token.py``).

Run locally and on the deployed (public) service from the repo root:

    doppler run -p core-x -c prd -- python -m apps.edge_api.main

Secrets come from Doppler ``core-x/prd`` — the same config the hq-x service reads,
so edge_api operates on the identical ``business.*`` Postgres rows. Compute
relocation, not data migration.
"""
from __future__ import annotations

import logging
import os
import sys
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse

from .src import config
from .src.db import close_pool, init_pool
from .src.migrate import run_migrations
from .src.mcp.doppler import mcp as doppler_mcp
from .src.mcp.trigger import mcp as trigger_mcp
from .src.mcp_bearer import bearer_token_app
from .src.routers.agent_runs_v1 import router as agent_runs_router
from .src.routers.bookings_v1 import router as bookings_router
from .src.routers.deals_v1 import router as deals_router
from .src.routers.internal_deals_v1 import router as internal_deals_router
from .src.routers.engagement_mappings_v1 import router as engagement_mappings_router
from .src.routers.documenso_template_fields_v1 import router as documenso_template_fields_router
from .src.routers.documenso_templates_v1 import router as documenso_templates_router
from .src.routers.documenso_envelopes_v1 import router as documenso_envelopes_router
from .src.routers.documenso_prefill_configs_v1 import router as documenso_prefill_configs_router
from .src.routers.documenso_template_defaults_v1 import router as documenso_template_defaults_router
from .src.routers.engagement_templates_v1 import router as engagement_templates_router
from .src.routers.internal_engagement_templates_v1 import router as internal_engagement_templates_router
from .src.routers.company_profiles_v1 import router as company_profiles_router
from .src.routers.clay_find_companies_v1 import router as clay_find_companies_router
from .src.routers.clay_enrich_companies_v1 import router as clay_enrich_companies_router
from .src.routers.clay_find_people_v1 import router as clay_find_people_router
from .src.routers.clay_person_work_history_v1 import router as clay_person_work_history_router
from .src.routers.contacts_v1 import router as contacts_router
from .src.routers.equipment_catalog_v1 import router as equipment_catalog_router
from .src.routers.industries_served_v1 import router as industries_served_router
from .src.routers.capital_providers_v1 import router as capital_providers_router
from .src.routers.existing_claygent_payloads_v1 import router as existing_claygent_payloads_router
from .src.routers.equipment_provider_v1 import router as equipment_provider_router
from .src.routers.equipment_finance_candidates_v1 import router as equipment_finance_candidates_router
from .src.routers.combo_work_summary_equipment_needs_v1 import (
    router as combo_work_summary_equipment_needs_router,
)
from .src.routers.combo_job_to_be_done_v1 import router as combo_job_to_be_done_router
from .src.routers.active_awards_query_v1 import router as active_awards_query_router
from .src.routers.market_collections_v1 import router as market_collections_router
from .src.routers.market_spec_v1 import router as market_spec_router
from .src.routers.epd_lec_status_v1 import router as epd_lec_status_router
from .src.routers.map_ask_v1 import router as map_ask_router
from .src.routers.title_normalize_v1 import router as title_normalize_router
from .src.routers.title_enrichment_v1 import router as title_enrichment_router
from .src.routers.proposal_templates_v1 import router as proposal_templates_router
from .src.routers.webhooks_cal import router as webhooks_cal_router
from .src.routers.webhooks_stripe import router as webhooks_stripe_router
from .src.routers.documenso_webhooks_v1 import router as documenso_webhooks_router
from .src.routers.close_webhooks_v1 import webhook_router as close_webhook_router
from .src.routers.close_webhooks_v1 import read_router as close_read_router
from .src.routers.internal_cal_v1 import router as internal_cal_router
from .src.routers.document_payments_v1 import router as document_payments_router
from .src.routers.operator_settings_v1 import router as operator_settings_router
from .src.routers.awards_subk_pricing_v1 import router as awards_subk_pricing_router
from .src.service_token import require_service_token

# ── Vendored hq-x GTM pipeline subtree (Phase 4) ─────────────────────────────
# The copied tree under src/_hqx/app keeps its original ``app.*`` imports; shim
# it onto sys.path so they resolve, then pull the /run-step router + its DB pool.
# config.py boots under core-x/prd (needs only APP_ENV + HQX_DB_* + HQX_SUPABASE_*).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src", "_hqx"))
from app.db import close_pool as hqx_close_pool, init_pool as hqx_init_pool  # noqa: E402
from app.routers.internal.gtm_pipeline import router as pipeline_router  # noqa: E402

log = logging.getLogger("edge_api")


# ── MCP mounts (Anthropic-facing) ────────────────────────────────────────────
# Each FastMCP server is exposed as an ASGI sub-app and wrapped in the shared
# transport-bearer check (DMAAS_MCP_BEARER_TOKEN — the SAME value the agent's
# Anthropic vault injects, scoped by mcp_server_url; keeping the value identical
# across the move means the vault credential keeps authenticating once the mount
# URL is repointed). Captured at import so the lifespan can chain each inner app.
_mcp_bearer = os.environ.get("DMAAS_MCP_BEARER_TOKEN")
_trigger_mcp_inner = trigger_mcp.http_app(path="/")
_trigger_mcp_app = bearer_token_app(_trigger_mcp_inner, bearer_token=_mcp_bearer)
_doppler_mcp_inner = doppler_mcp.http_app(path="/")
_doppler_mcp_app = bearer_token_app(_doppler_mcp_inner, bearer_token=_mcp_bearer)


@asynccontextmanager
async def lifespan(app_: FastAPI):
    # Fail-loud-but-not-fatal warnings so a misconfigured deploy is obvious in
    # the logs rather than silently open.
    if config.service_token() is None:
        log.warning(
            "EDGE_API_SERVICE_TOKEN unset -- service-token routes are UNAUTHENTICATED "
            "(local dev only). Set it in core-x/prd for every deployed environment."
        )
    if _mcp_bearer is None:
        log.warning(
            "DMAAS_MCP_BEARER_TOKEN unset -- /mcp/* mounts are UNAUTHENTICATED "
            "(local dev only). Set it in core-x/prd for every deployed environment."
        )
    if config.documenso_webhook_secret() is None:
        log.warning(
            "DOCUMENSO_WEBHOOK_SECRET unset -- the proposals webhook refuses (503), so signed/"
            "completed status will NOT advance server-side. Set it in core-x/prd and register the "
            "Documenso webhook with the same secret."
        )
    if config.cal_webhook_secret() is None:
        log.warning(
            "CAL_WEBHOOK_SECRET unset -- /webhooks/cal refuses (503), so cal.com bookings will NOT "
            "be captured. Set it in core-x/prd to match the cal.com webhook signing secret."
        )
    if config.stripe_secret_key() is None or config.stripe_publishable_key() is None:
        log.warning(
            "STRIPE_SECRET_KEY / STRIPE_PUBLISHABLE_KEY unset -- the document-payment intent route "
            "refuses (503), so ACH payment cannot be initiated. Set both in core-x/prd."
        )
    if config.stripe_webhook_secret() is None:
        log.warning(
            "STRIPE_WEBHOOK_SECRET unset -- /webhooks/stripe refuses (503), so ACH payment status will "
            "NOT advance server-side. Set it in core-x/prd and register the Stripe webhook with it."
        )
    if config.close_webhook_secret() is None:
        log.warning(
            "CLOSE_WEBHOOK_SECRET unset -- /webhooks/close refuses (503), so Close call events will NOT "
            "be captured and the Insights call-sync stays idle. Set it in core-x/prd to match the Close "
            "webhook subscription signature_key."
        )
    if config.close_booking_custom_activity_type_id() is not None and (
        config.cal_api_key() is None or config.close_api_key() is None
    ):
        log.warning(
            "CLOSE_BOOKING_CUSTOM_ACTIVITY_TYPE_ID is set but CAL_API_KEY/CLOSE_API_KEY is unset -- the "
            "Close custom-activity -> cal.com booking path will FAIL at /internal/cal/book. Set both in core-x/prd."
        )
    # Chain every mounted FastMCP sub-app's lifespan so its session manager
    # starts/stops with the parent app; open the Postgres pool (agent-runs
    # ledger) inside it and drain on shutdown.
    async with _trigger_mcp_inner.lifespan(app_), _doppler_mcp_inner.lifespan(app_):
        await init_pool()       # edge_api pool (agent-runs ledger)
        await hqx_init_pool()   # vendored app.db pool (pipeline)
        try:
            # Schema-as-code: bring the live schema up to the committed DDL (sql/*.sql) BEFORE serving.
            # Idempotent + self-healing; a failure re-raises → the boot fails → Railway keeps the prior
            # healthy deploy on traffic. Closes the code↔schema drift that 500'd document-payment reads
            # (the rail column added in committed DDL but never applied to prod). See src/migrate.py.
            await run_migrations()
            log.info("edge_api: boot ok (mounts: trigger, doppler; routes: agent-runs, pipeline; DB pools up; schema applied)")
            yield
        finally:
            await hqx_close_pool()
            await close_pool()


app = FastAPI(title="edge_api", version="0.4.0", lifespan=lifespan)

# Mount the MCP servers. Managed agents authenticate via
# Authorization: Bearer <DMAAS_MCP_BEARER_TOKEN>; the wrapper rejects
# unauthorized requests at the ASGI boundary before FastMCP sees them.
# NOTE: register the agent's mcp_servers[].url + vault credential WITH the
# trailing slash (.../mcp/trigger/). Starlette mounts 307-redirect the slash-less
# form to an insecure URL the managed-agents platform blocks.
app.mount("/mcp/trigger", _trigger_mcp_app)  # Trigger.dev task control
app.mount("/mcp/doppler", _doppler_mcp_app)  # core-x Doppler secret reads

# agent-runs: the BFF-facing SSE surface (mint session, stream events, ledger).
# Each route is gated by EDGE_API_SERVICE_TOKEN (require_service_token) in-router.
app.include_router(agent_runs_router)

# clay-find-people: raw, append-only landing of Clay find-people records into
# gtm.clay_find_people (verbatim raw_payload + lossless identity keys). Service-token gated.
app.include_router(clay_find_people_router)

# clay-person-work-history: raw, append-only landing of the FULL Clay person profile (the whole
# experience[] work-history array + education/publications/etc.) into gtm.clay_person_work_history as a
# single verbatim raw_payload blob — NOT exploded. Computed identity keys only (person_id joins
# clay_find_people). Service-token gated.
app.include_router(clay_person_work_history_router)

# clay-find-companies: raw, append-only landing of Clay find-companies records into
# gtm.clay_find_companies (verbatim raw_payload + lossless identity keys). Service-token gated.
app.include_router(clay_find_companies_router)

# clay-enrich-companies: raw-ONLY, append-only landing of Clay *enrich-company* dossier payloads
# into gtm.clay_enrich_companies (raw_payload jsonb verbatim + a single drift-proof connect key
# company_linkedin_url = raw_payload->>'url'; NO explode). PK = sha256(canonical payload), so refreshes
# append as history. Distinct from clay-find-companies (the discovery grain). Service-token gated.
app.include_router(clay_enrich_companies_router)

# contacts: curated GTM contact intake — flat singular fields (full_name, work_email[optional],
# job_title, is_main_contact, city/state/country, company_name/domain/linkedin_url) → gtm.contacts.
# Append-only history keyed on person-discriminant+mutable fields; bridges via domain_norm AND
# company_linkedin_url_norm. Identity degrades email→name when work_email is absent. Service-token gated.
app.include_router(contacts_router)

# equipment-catalog: raw, append-only landing of company-offerings research payloads into
# gtm.equipment_catalog (verbatim raw_payload + sparse flat projection, two payload shapes
# discriminated by payload_kind). Bridges to firmographics_blitz via domain_norm. Service-token gated.
app.include_router(equipment_catalog_router)

# industries-served: raw, append-only landing of company-industries research payloads into
# gtm.industries_served (verbatim raw_payload + flat projection of industriesServed[] + confidence
# + reasoning + sources/stepsTaken). Bridges to firmographics_blitz via domain_norm. Service-token gated.
app.include_router(industries_served_router)

# capital-providers: raw, append-only landing of the IN-HOUSE capital-provider LLM classification
# (capitalType / providesCapital) into gtm.company_capital_providers — verbatim raw_payload jsonb
# (immutable SoT) + flat projection (capital_provider_type_category, provides_capital, confidence,
# reasoning, source_urls, evidence_phrases, steps_taken) + the wire business fields (company_name,
# domain, company_linkedin_url, description). Bridges to firmographics_blitz via domain_norm.
# /land only. Service-token gated.
app.include_router(capital_providers_router)

# existing-claygent-payloads: universal raw landing sink for heterogeneous Claygent enrichment
# payloads → gtm.existing_claygent_payloads. Body {domain, enrichment_payload_type, raw_payload};
# raw_payload stored verbatim as jsonb under a string discriminator (enrichment_payload_type) — no
# projection, no per-shape table. jsonb is schemaless so divergent shapes land losslessly; sorted
# out at read time by filtering on enrichment_payload_type. /land only. No bridges / no secondary
# indexes. Service-token gated.
app.include_router(existing_claygent_payloads_router)

# equipment-provider: raw, append-only landing of company-is-equipment-provider classification
# payloads into gtm.equipment_provider (verbatim raw_payload + flat projection of isEquipmentProvider,
# mode, confidence, reasoning, stepsTaken, evidenceUrl, evidenceSnippet). Bridges to
# firmographics_blitz via domain_norm. Service-token gated.
app.include_router(equipment_provider_router)

# equipment-finance-candidates: flat-cohort landing for curated equipment-finance lender
# candidates (name + domain + linkedin_url + verdict). Bridges via domain_norm AND linkedin_url_norm.
app.include_router(equipment_finance_candidates_router)

# combo-work-summary-equipment-needs: raw landing of LLM equipment-needs verdicts at the
# NAICS x PSC combo grain (Clay/GPT upstream). Body {naics_code, psc_code, raw_payload, model_id?,
# source?}; raw_payload stored verbatim as jsonb — no projection, no comma-splitting, no taxonomy
# normalization at land time. UPSERT on (naics_code, psc_code, model_id). Service-token gated.
app.include_router(combo_work_summary_equipment_needs_router)
app.include_router(combo_job_to_be_done_router)
app.include_router(active_awards_query_router)

# market-spec: live market definition on a call — count of entities fitting a
# geo/$/designation/firmographic spec, served by the query-sidecar audience spine
# (gtm_audience_entities). Contactability deliberately absent (operator ruling 2026-07-16).
app.include_router(market_spec_router)

# market-collections: the 22 durable pair-defined market collections
# (gtm.market_collections; hq/MARKET_COLLECTIONS_PROGRAM.md v2) — list + live
# member count with geo/band tuning. Member = FY23-25 won-in-band within the
# collections' pairs AND >= 1 active in-scope award (PoP-filtered when
# working_in is given — PoP relates to ACTIVE awards only, operator ruling).
app.include_router(market_collections_router)

# epd-lec-status: raw, append-only landing of EPD / Buy-Clean / LEC compliance research payloads into
# gtm.epd_lec_status (verbatim raw_payload + flat projection of epdLecStatus + justification + confidence
# + reasoning + stepsTaken). Bridges to firmographics_blitz via domain_norm. Service-token gated.
app.include_router(epd_lec_status_router)

# awards-subk-pricing: empirical subcontract-pricing foundation (combo lanes, firm-scale, firm-in-lane).
# Serves canonical Lance datasets (combo_prime_sub_pricing, firm_award_value_profile, firm_combo_prime_profile)
# materialized from 108M prime txns + 627k FSRS subawards 2021+. Endpoints: /summary, /active-demand,
# /combo/{naics}/{psc}. Service-token gated.
app.include_router(awards_subk_pricing_router)

# Pipeline: the post-payment GTM pipeline /run-step surface, vendored from hq-x.
# Trigger.dev calls it (verify_trigger_secret / TRIGGER_SHARED_SECRET) at
# /internal/gtm/initiatives/{id}/run-step — mounted with the same /internal prefix.
app.include_router(pipeline_router, prefix="/internal")

# documenso webhooks: RAW landing for Documenso events (X-Documenso-Secret). Documenso is repointed
# here from /proposals/webhook; stores every delivery verbatim in business.documenso_webhook_events.
# No normalization/projection — that's a separate step decided against the captured payloads.
app.include_router(documenso_webhooks_router)

# document payments: the direct-to-documenso engagement-fee surface (Stripe ACH). PUBLIC, keyed by the
# (opportunity_id, document_id) pair; mint/reuse the intent + read state. Amount resolved server-side
# from fee_amount; `paid` advanced only by /webhooks/stripe (metadata.kind="document"). edge_api owns it.
app.include_router(document_payments_router)

# proposal-templates: the authoring surface (markdown → branded HTML → DocRaptor preview → publish).
# Service-token gated; the BFF brokers it with the operator session. Markdown source lives in
# Postgres (business.global_engagement_content); preview PDFs are stashed in R2.
app.include_router(proposal_templates_router)

# bookings: the operator Pipeline list — recent cal.com bookings from corex.bookings.
# Service-token gated; the BFF brokers it with the operator session. Read-only (Phase 1).
app.include_router(bookings_router)

# deals: the operator Applications/Research list — business.deals (one per account, org-grounded).
# The first-class pipeline entity replacing the booking->opportunity projection for the cockpit
# list + Application detail. Service-token gated; the BFF brokers it with the operator session.
app.include_router(deals_router)

# deals (internal): the materialization PRODUCER. The deal-materialize Trigger.dev task
# (fired by the cal webhook on a new booking) calls /internal/deals/materialize to project the
# booking → account+contact+deal (one deal per account, advancing last_booking_id) + the
# deal_contacts signatory link. Trigger-secret gated, same /internal contract as the gtm pipeline
# run-step. Replaces the retired booking→opportunity producer; the seam DocRaptor render layers onto.
app.include_router(internal_deals_router, prefix="/internal")

# cal booking (internal): the OUTBOUND create PRODUCER. The cal-book Trigger.dev task (fired by the
# Close custom-activity webhook) calls /internal/cal/book to mint a cal.com booking from the activity
# + contact (read back from the Close API), idempotent on the Close activity id via ops.cal_booking_runs.
# Trigger-secret gated, same /internal contract. The created booking re-enters via /webhooks/cal, so the
# normalize -> corex.bookings -> materialize pipeline runs unchanged (closed loop, no double-write).
app.include_router(internal_cal_router, prefix="/internal")

# engagement-mappings: the Dossier engagement picker — visible prospect-facing mappings
# (business.engagement_documenso_template_mappings) scoped to the operator's org domain.
app.include_router(engagement_mappings_router)

# operator-settings: the per-operator cockpit config (render_mode + direct_to_documenso_lane) the BFF
# resolves at originate. Service-token gated; the BFF asserts the validated auth_user_id on the path.
# RETIRES the BFF's direct Supabase service-role access to public.operator_settings — edge_api is now
# the sole gateway to the table over the shared HQX_ Postgres (BFF becomes a pass-through).
app.include_router(operator_settings_router)

# documenso-template-fields: the Settings "Documenso Templates" defaults editor — reads/writes the
# live Documenso template's fields (set per-field default values via /envelope/field/update-many).
app.include_router(documenso_template_fields_router)
# documenso-templates: the Settings → Documenso → Manage Templates table — lists every
# business.documenso_templates row (active + archived) for the operator's org.
app.include_router(documenso_templates_router)

# documenso-envelopes: the envelope MIRROR surface — LIST the mirrored TEMPLATE envelopes
# (business.documenso_envelopes, verbatim) and on-demand RE-GRAB one or all of them. Re-grab reuses the
# webhook projector's exact get_envelope → upsert_envelope pull (no second upsert path, no remap) and
# NEVER writes business.documenso_template_configs. Service-token gated.
app.include_router(documenso_envelopes_router)

# documenso-template-prefill: the "Manage Documenso Templates" prefill-config editor. GET surfaces a
# template's value fields (TEXT/NUMBER with a fieldMeta.label, read off the verbatim mirror
# business.documenso_envelopes) + the saved per-label settings; PUT upserts the OPERATOR-OWNED
# business.documenso_template_document_prefill_configs (field_settings keyed by label, arbitrary objects
# stored verbatim). The default lives in OUR config, applied at originate later — nothing is baked onto
# the Documenso template. This editor is the SOLE writer of that table. Service-token gated.
app.include_router(documenso_prefill_configs_router)

# documenso-template-defaults: the "Set Template as Default" picker over the MIRROR. GET lists the
# mirrored TEMPLATE envelopes (business.documenso_envelopes, type='template', non-deleted) each flagged
# is_default; POST marks one as the operator's Confirm & Originate default. The default lives in the
# operator-owned business.documenso_template_defaults (keyed by documenso_id) — the projector/re-grab
# NEVER touch it, and this picker is its SOLE writer. Replaces the legacy documenso-templates registry
# picker for mirror-path templates (e.g. 14503, which the legacy registry doesn't contain). Service-token gated.
app.include_router(documenso_template_defaults_router)

# engagement-templates: the Settings "Engagement Templates" render surface — STANDALONE from the
# engagement-doc pathway. Lists selectable (brand, path, archetype, version) from the repo-resident
# content tree and renders one to a clean PDF (plain style by default) via DocRaptor → R2 → presigned
# URL. Does NOT touch Documenso (the operator affixes fields in the editor by hand). Service-token gated.
app.include_router(engagement_templates_router)

# engagement-templates (internal): the render+PUSH lane. The engagement-template-push Trigger.dev task
# calls /internal/engagement-templates/render-push to resolve a content source (a
# business.global_input_content registry row, or an explicit brand/path/archetype/version), render it
# via DocRaptor, and create the Documenso TEMPLATE — recording a terminal row in
# ops.engagement_template_push_runs. Trigger-secret gated, same /internal contract as the others.
app.include_router(internal_engagement_templates_router, prefix="/internal")

# company-profiles: the Dossier's "Save Profile" — append-only snapshots of the verified dossier
# (business.company_profile_snapshots). Service-token gated; the BFF brokers it with the operator
# session. The booking-profile read resolves the latest snapshot by domain (else the seed).
app.include_router(company_profiles_router)

# map /ask: the portal map TRANSLATE route. NL → forced-tool Anthropic Messages call
# (tool_choice → emit_filter) → constrained filter object → catalyst_api EXECUTE → GeoJSON.
# Service-token gated; the single LLM touchpoint of the map. No gtm_mcp / gtm-agent.
app.include_router(map_ask_router)

# titles/normalize: the pre-enrichment classification gate. One raw scraped job title →
# forced-tool Anthropic Messages call → strict internal taxonomy (6 job levels × 22 job
# functions, output pinned to closed enums; unexpected/missing → "Other"). Stateless; reuses
# ANTHROPIC_API_KEY. Service-token gated. Front-runs expensive enrichment with normalized keys.
app.include_router(title_normalize_router)

# title-enrichment: the PERSISTENCE sibling of titles/normalize — append-only landing of enriched job
# titles into gtm.title_enrichment (verbatim raw_payload + flat projection of normalized_job_title/
# normalized_level/function/confidence/model/reasoning/person_linkedin_url). raw_job_title is the ONLY
# required value; all other fields nullable. title_norm (lower+ws-collapsed) is the dedup key. Service-token gated.
app.include_router(title_enrichment_router)

# cal.com webhook: RAW CAPTURE (Phase 1) — verbatim payload → public.cal_raw_events.
# Signature-gated (X-Cal-Signature-256 / CAL_WEBHOOK_SECRET). NOT under /api/v1 — cal.com posts /webhooks/cal.
# Normalization into corex.bookings is a separate later step (wired against the real captured payload).
app.include_router(webhooks_cal_router)

# stripe webhook: authoritative ACH payment-state advance + append-only audit (business.engagement_events).
# Signature-gated (Stripe-Signature / STRIPE_WEBHOOK_SECRET). NOT under /api/v1 — Stripe posts /webhooks/stripe.
app.include_router(webhooks_stripe_router)

# close.com webhook: RAW capture of Close call events → business.close_webhook_events.
# Signature-gated (Close-Sig-Hash/Timestamp / CLOSE_WEBHOOK_SECRET). NOT under /api/v1 — Close posts /webhooks/close.
# The Insights "now dialing" briefing derives OFFLINE from these rows + public.close_crosswalk; the
# service-token-gated read is /api/v1/close/active-call/{auth_user_id} (the BFF brokers it).
app.include_router(close_webhook_router)
app.include_router(close_read_router)


def _info() -> dict:
    return {
        "service": "edge_api",
        "status": "ok",
        "phase": "4-pipeline",
        "mounts": {
            "mcp": ["trigger", "doppler"],
            "agent_runs": True,        # /api/v1/agent-runs/* (SSE)
            "pipeline": True,          # /internal/gtm/initiatives/{id}/run-step
            "clay_find_people": True,  # /api/v1/clay/find-people/{land,stats,by-linkedin}
            "clay_person_work_history": True,  # /api/v1/clay/person-work-history/{land,stats}
            "clay_find_companies": True,  # /api/v1/clay/find-companies/{land,stats}
            "clay_enrich_companies": True,  # /api/v1/clay/enrich-companies/{land,stats}
            "contacts": True,          # /api/v1/contacts/{land,check,stats}
            "equipment_catalog": True,    # /api/v1/equipment-catalog/{land,stats}
            "industries_served": True,    # /api/v1/industries-served/{land,check,stats}
            "capital_providers": True,    # /api/v1/capital-providers/land (in-house capital LLM classification)
            "existing_claygent_payloads": True,  # /api/v1/existing-claygent-payloads/land (universal jsonb sink, discriminated)
            "equipment_provider": True,   # /api/v1/equipment-provider/{land,check,stats}
            "equipment_finance_candidates": True,  # /api/v1/equipment-finance-candidates/{land,check,stats}
            "combo_work_summary_equipment_needs": True,  # /api/v1/combo-work-summary-equipment-needs/land (combo-grain LLM equipment verdicts, UPSERT)
            "combo_job_to_be_done": True,  # /api/v1/combo-job-to-be-done/land (combo-grain 'to: …' job sentences, UPSERT)
            "active_awards_query": True,  # /api/v1/market/active-awards-query + /jtbd-vocab (Q1 canonical query)
            "epd_lec_status": True,       # /api/v1/epd-lec-status/{land,check,stats}
            "proposal_templates": True,  # /api/v1/proposal-templates/* (authoring, preview, publish)
            "engagement_templates": True,  # /api/v1/engagement-templates (list + render → presigned PDF)
            "bookings": True,          # /api/v1/bookings (operator Pipeline list — corex.bookings)
            "map_ask": True,           # /api/v1/map/{dataset}/ask (NL → emit_filter → catalyst EXECUTE → GeoJSON)
            "title_normalize": True,   # /api/v1/titles/normalize (raw title → forced-tool → {level, function})
            "title_enrichment": True,  # /api/v1/title-enrichment/{land,check,stats} (enriched title → gtm.title_enrichment)
            "cal_webhook": True,       # /webhooks/cal (cal.com RAW capture → public.cal_raw_events)
            "document_payments": True, # /api/v1/documenso/{payment-intent,payment}/{opp}/{doc} (Stripe ACH)
            "stripe_webhook": True,    # /webhooks/stripe (ACH payment_intent.* → engagement_events + paid)
            "close_webhook": True,     # /webhooks/close (Close call events RAW capture → business.close_webhook_events)
            "close_active_call": True, # /api/v1/close/active-call (offline "now dialing" derivation, not operator-scoped)
            "operator_settings": True, # /api/v1/operator-settings/{auth_user_id} (render_mode + lane)
        },
    }


@app.get("/")
def root() -> dict:
    return _info()


@app.get("/healthz")
def healthz() -> JSONResponse:
    """Liveness. Open (no token) for platform probes. 200 while the process is up;
    later phases extend this with a DB-pool reachability check."""
    return JSONResponse(_info(), status_code=200)


@app.get("/v1/_authcheck", dependencies=[Depends(require_service_token)])
def authcheck() -> dict:
    """Diagnostic: proves the EDGE_API_SERVICE_TOKEN gate end-to-end. 200 only with a
    valid Bearer (401 otherwise). No functionality — remove once real routes land."""
    return {"ok": True, "gate": "service_token"}


def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host=config.host(), port=config.port())


if __name__ == "__main__":
    main()
