-- Backfill ops.enrichment_cohort_runs for the 2 DSBS cohorts whose builds published to R2 but whose
-- _record_run INSERT failed silently (swallowed exception — the builders DO call it; the secret
-- likely wasn't resolved). The silent-failure path is now hardened (returns bool + stderr +
-- ledger_written in the callback) and the gap-builder firms_gap→firms_with_linkedin latent bug is
-- fixed, so future runs self-journal correctly; this one-time backfill restores the missing history.
--
-- id (GENERATED ALWAYS AS IDENTITY) and recorded_at (DEFAULT now()) auto-generate — do NOT supply.
-- Column order matches ops_enrichment_cohort_runs.sql. status CHECK ∈ ('success','error').
-- #573 chunk files are an operational RE-SLICE of the #572 cohort (6 files = the same 23,973
-- domains), NOT a distinct build → no separate row; per-chunk enrollment lives in
-- ops.enrichment_blitz_runs.
--
-- Idempotency guard: only insert rows that don't already exist (re-runnable).
INSERT INTO ops.enrichment_cohort_runs
    (feed, cohort_name, firms_total, firms_with_domain, firms_pdl_matched,
     firms_with_linkedin, distinct_urls, r2_key, column_name, status, error,
     started_at, completed_at)
SELECT v.feed, v.cohort_name, v.firms_total, v.firms_with_domain, v.firms_pdl_matched,
       v.firms_with_linkedin, v.distinct_urls, v.r2_key, v.column_name, v.status, v.error,
       v.started_at, v.completed_at
FROM (VALUES
  -- (#571) DSBS certified → PDL company LinkedIn URL → Workflow B.
  --   Parquet row count (26,502) verified live; firms_with_domain/firms_pdl_matched are the
  --   operator-supplied funnel intermediates (not re-derivable read-only — point-in-time).
  ('enrichment_cohort_sba_dsbs',
   'sba_dsbs_certified_firms_linkedin',
   67234::bigint, 52698::bigint, 28422::bigint, 26502::bigint, 26502::bigint,
   'cohorts/enrichment_blitz/sba_dsbs_certified_firms_linkedin.parquet',
   'company_linkedin_url', 'success', NULL::text,
   TIMESTAMPTZ '2026-06-20 09:52:27+00', TIMESTAMPTZ '2026-06-20 09:52:27+00'),
  -- (#572) DSBS gap (domain, NO PDL/LinkedIn) → Workflow A (domain→LinkedIn).
  --   firms_with_linkedin = 0 by construction; distinct_urls carries the DISTINCT domain count
  --   (23,973, the Parquet row count). firms_pdl_matched = firms that DID hit PDL (the gap is the
  --   complement). Mirrors the live equipment_rental_firms_cascade_domains precedent.
  ('enrichment_cohort_sba_dsbs_gap',
   'sba_dsbs_certified_firms_gap_domains',
   67234::bigint, 52698::bigint, 28422::bigint, 0::bigint, 23973::bigint,
   'cohorts/enrichment_blitz/sba_dsbs_certified_firms_gap_domains.parquet',
   'normalized_domain', 'success', NULL::text,
   TIMESTAMPTZ '2026-06-20 10:10:38+00', TIMESTAMPTZ '2026-06-20 10:10:38+00')
) AS v(feed, cohort_name, firms_total, firms_with_domain, firms_pdl_matched,
       firms_with_linkedin, distinct_urls, r2_key, column_name, status, error,
       started_at, completed_at)
WHERE NOT EXISTS (
    SELECT 1 FROM ops.enrichment_cohort_runs e
    WHERE e.feed = v.feed AND e.cohort_name = v.cohort_name AND e.status = 'success'
);
