-- Drop retired "engagement" cruft — nomenclature cycle (ruled 2026-07-21, operator).
-- Applied to the hq-x control-plane Postgres (HQX_DB_URL_POOLED) by the boot-time schema apply
-- (src/migrate.py). Guarded (IF EXISTS) ⇒ idempotent + self-healing: once dropped these stay dropped,
-- and any accidental re-creation is nuked on the next deploy. Sorts last so all provisioning is done.
--
-- All four are dead in code (zero executable readers anywhere in core-x) and superseded by the gc
-- ontology plane, which is rebuilt separately. Pre-drop snapshots of the rows live at
--   ~/Desktop/hq/recon/2026-07-21-preflight-snapshot-hqx-dead-tables.md          (archetypes, variants)
--   ~/Desktop/hq/recon/2026-07-21-preflight-snapshot-engagement-proposals.md     (proposals; events was empty)
--
--   business.engagement_events        — append-only ledger, never wired (0 rows); the live Stripe/
--                                        Documenso audit trail is business.document_payments.
--   business.engagement_proposals     — Rare Structure e-signature grain; no code reader (4 test rows).
--   business.engagement_archetypes    — dead fee-shape taxonomy (3 rows); gc.global_agreement_archetypes
--                                        is the live archetype surface.
--   business.global_input_content_variants — generation-input permutations, no code reader (9 rows).

-- FK-safe order: engagement_events references engagement_proposals(ref), so the child drops first.
DROP TABLE IF EXISTS business.engagement_events;
DROP TABLE IF EXISTS business.engagement_proposals;
DROP TABLE IF EXISTS business.engagement_archetypes;
DROP TABLE IF EXISTS business.global_input_content_variants;
