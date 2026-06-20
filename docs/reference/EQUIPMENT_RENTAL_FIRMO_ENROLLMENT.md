# Equipment-Rental Firmographic Enrollment (Blitz Workflow B)

Enrolls the **active US equipment-rental supply base** into the existing Directive-23
Blitz firmographic enrichment cycle (Atomic **Workflow B**: `company_linkedin_url → firmographics`).
Companion to `EQUIPMENT_RENTAL_CONSTRUCTION_MATCH.md` — same supply universe, now firmographically hydrated.

## What it does

Resolve each active equip-rental firm to its **company LinkedIn URL from PDL**, then feed the
distinct URLs to the rate-governed Blitz enrichment plane. The enrichment engine is reused verbatim
(`enrichment-blitz-enrich-linkedin` / `run_enrich_linkedin`); this build adds only the **cohort
assembly + enrollment**.

## Resolution funnel (each hop a fully-indexed point-lookup)

| Hop | Source | Key | Result |
|-----|--------|-----|--------|
| SUPPLY | `sam_master_entities` | NAICS ∈ {532412, 532490, 532310, 532120} · `is_active` · `country='USA'` | active equip-rental firm |
| DOMAIN | `sam_master_domains` | `uei` → `min(normalized_domain)` (blocklist-filtered) | 1 normalized domain / firm |
| PDL #1 | `pdl_normalized_companies` | `normalized_domain` (BTREE) → `pdl_company_id` (`NOT is_generic_domain`) | matched PDL company |
| PDL #2 | `pdl_companies` | `pdl_company_id` (BTREE) → `linkedin_url` | the literal PDL company URL |
| COHORT | — | DISTINCT `linkedin_url` | Workflow B cohort |

Validated funnel (first run, 2026-06-20): **8,402** active firms → **4,989** with domain →
**3,322** PDL-matched → **3,009** distinct company LinkedIn URLs.

The supply definition mirrors the match table's supply side exactly (no geo/centroid gate — firmographics
are company-level, so HQ-pin national chains are **included**). The LinkedIn URL is the actual string PDL
stored, never a slug reconstruction.

## Where it lands

- **Cohort transport Parquet** → `s3://data-sink/cohorts/enrichment_blitz/equipment_rental_firms_linkedin.parquet`
  (column `company_linkedin_url`, overwrite each cycle).
- **Per-entity firmo** → `ops.task_runs` (`task_type='blitz_firmo_direct'`) → the `firmographics-blitz`
  materializer → `s3://data-sink/active/firmographics_blitz/` (Lance SoR, keyed `domain_norm`).
- **Run ledgers** → `ops.enrichment_cohort_runs` (the resolution funnel) + `ops.enrichment_blitz_runs`
  (the Workflow B run: requested / skipped / api_calls / succeeded / not_found / failed).

## The cycle

Re-enrollment is idempotent: Workflow B's `firmo_ttl_days` (default 180) JIT-skips firms already fresh in
`firmographics_blitz` / `ops.task_runs`, and `neg_ttl_days` (30) skips recent misses, so re-running only
spends Blitz calls on **stale or newly-registered** firms. Enrollment is **manual (no cron)** so credit
consumption is observed on the first runs (mirrors `exa_websets` / `sba_foia`); flip
`enroll-equipment-rental-firms-firmo` to `schedules.task` once consumption is observed. Bulk enrollment
runs at **LOW** gateway priority — throttled, never starved, yielding to interactive GTM enrichment.

## Run it

```bash
# Size only — no Parquet write, no Blitz spend
modal run    pipelines/enrichment_blitz/cohort_equipment_rental.py::preview

# Deploy the persistent cohort-builder app (dispatcher resolves it by name)
modal deploy pipelines/enrichment_blitz/cohort_equipment_rental.py

# One-command enrollment: build cohort → enroll into Workflow B (prod)
#   Trigger task: enroll-equipment-rental-firms-firmo
```

## Components

- `pipelines/enrichment_blitz/cohort_equipment_rental.py` — Modal cohort builder (`build_cohort` /
  `preview_cohort`), Modal app `enrichment-blitz-cohort-rental`.
- `pipelines/enrichment_blitz/ops_enrichment_cohort_runs.sql` — cohort provenance ledger DDL.
- `src/trigger/enrichment_rental_firms.ts` — `enroll-equipment-rental-firms-firmo` (dispatch builder →
  enroll into `enrichment-blitz-enrich-linkedin`).
