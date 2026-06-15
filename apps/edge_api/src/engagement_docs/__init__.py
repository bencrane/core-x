"""AO engagement-document pathway — PARALLEL to the proposal/markdown machinery.

Action on an opportunity (rare-structure-hq Applications table) → Trigger.dev → edge_api binds the
opportunity's values + a locked price/term package into the repo-resident STATIC HTML engagement
content (apps/edge_api/content/global_engagement_content/) → DocRaptor → plain PDF in R2.

Self-contained by design: its own token substitution, its own DocRaptor call, its own R2 namespace,
its own ledger (business.ao_engagement_mandates). It does NOT use the proposal markdown→HTML render,
agreement_template, docraptor_client, or r2_client — the two pathways never touch.
"""
