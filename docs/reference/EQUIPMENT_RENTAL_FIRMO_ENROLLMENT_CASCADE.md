# Equipment-Rental Firmographic Enrollment — Cascade (Blitz Workflow C)

Picks up the equipment-rental firms the **Workflow B** enrollment
(`EQUIPMENT_RENTAL_FIRMO_ENROLLMENT.md`) could not reach and routes them through Atomic
**Workflow C** (`enrichment-blitz-cascade` / `run_cascade`: `domain → company_linkedin_url →
firmographics`). Same supply universe, same firmographic destination — different entry hop.

## Why this exists

The Workflow B funnel resolved each firm all the way to a **PDL company LinkedIn URL** before
enrolling. Firms with a canonical SAM domain that did **not** match a non-generic PDL company fell
out at the PDL hop — they have no LinkedIn URL, so a Workflow B (LinkedIn-keyed) cohort can never
reach them. Workflow C runs the **domain → company** hop itself inside the rate-governed gateway, so
it needs no pre-resolved URL: the gap firms are reachable there on their SAM domain alone.

Validated Workflow B funnel (2026-06-20): **8,402** active firms → **4,989** with domain →
**3,322** PDL-matched. **The gap = 4,989 − 3,322 ≈ 1,667 firms** with a domain but no PDL company.

## The gap (each hop a fully-indexed point-lookup; SUPPLY ∩ DOMAIN identical to the B builder)

| Hop | Source | Key | Result |
|-----|--------|-----|--------|
| SUPPLY | `sam_master_entities` | NAICS ∈ {532412, 532490, 532310, 532120} · `is_active` · `country='USA'` | active equip-rental firm |
| DOMAIN | `sam_master_domains` | `uei` → `min(normalized_domain)` (blocklist-filtered) | 1 normalized domain / firm |
| PDL #1 | `pdl_normalized_companies` | `normalized_domain` (BTREE) → `pdl_company_id` (`NOT is_generic_domain`) | **matched** firms (EXCLUDED) |
| GAP | — | with-domain firms whose domain has **no** non-generic PDL company | the cascade cohort |
| COHORT | — | DISTINCT `normalized_domain` | Workflow C cohort |

The SUPPLY ∩ DOMAIN query is byte-identical to the Workflow B builder, and the PDL #1 predicate is the
same one that populates the B builder's `dom_to_pid` — so `firms_pdl_matched` matches exactly and the
gap is precisely `firms_with_domain − firms_pdl_matched`. The two paths partition the same supply with
no overlap: B enrolls the matched firms, C enrolls the rest.

## Where it lands

- **Cohort transport Parquet** → `s3://data-sink/cohorts/enrichment_blitz/equipment_rental_firms_cascade_domains.parquet`
  (column `normalized_domain`, overwrite each cycle).
- **Per-entity firmo** → `ops.task_runs` → the `firmographics-blitz` materializer →
  `s3://data-sink/active/firmographics_blitz/` (Lance SoR, keyed `domain_norm`).
- **Run ledgers** → `ops.enrichment_cohort_runs` (`feed='enrichment_cohort_rental_cascade'`; the
  shared cohort ledger — `firms_with_linkedin` is 0 by construction, `distinct_urls` carries the
  DISTINCT domain count) + `ops.enrichment_blitz_runs` (the Workflow C run).

## Cost & the cycle

Workflow C spends **more** Blitz credits per firm than B: a domain-resolve hop **plus** company hops,
versus a single firmo hop on a known URL. Surface the cohort size with `preview` (no write, no spend)
before firing, or run the Trigger task with `previewOnly: true` to build + size the cohort and stop
before any enrichment spend.

Re-enrollment is idempotent: Workflow C's `firmo_ttl_days` (default 180) JIT-skips firms already fresh
in `firmographics_blitz` / `ops.task_runs`, and `neg_ttl_days` (30) skips recent misses — so re-running
only spends on **stale or newly-registered** firms. Enrollment is **manual (no cron)** so consumption is
observed on the first runs; flip `enroll-equipment-rental-firms-firmo-cascade` to `schedules.task` once
consumption is observed. Bulk enrollment runs at **LOW** gateway priority — throttled, never starved.

## Run it

```bash
# Size only — no Parquet write, no Blitz spend
modal run    pipelines/enrichment_blitz/cohort_equipment_rental_cascade.py::preview

# Deploy the persistent cohort-builder app (dispatcher resolves it by name)
modal deploy pipelines/enrichment_blitz/cohort_equipment_rental_cascade.py

# One-command enrollment: build cohort → enroll into Workflow C (prod)
#   Trigger task: enroll-equipment-rental-firms-firmo-cascade
#   payload {previewOnly: true} → build + size only, no enrichment spend
```

## Components

- `pipelines/enrichment_blitz/cohort_equipment_rental_cascade.py` — Modal cohort builder (`build_cohort`
  / `preview_cohort`), Modal app `enrichment-blitz-cohort-rental-cascade`.
- `src/trigger/enrichment_rental_firms_cascade.ts` — `enroll-equipment-rental-firms-firmo-cascade`
  (dispatch builder → enroll into `enrichment-blitz-cascade`).
- `pipelines/enrichment_blitz/ops_enrichment_cohort_runs.sql` — shared cohort provenance ledger DDL
  (created by the Workflow B build / PR #568; this path writes a distinct `feed`).
