# Entity-360 Master — Build Plan (for adversarial review)

**Date:** 2026-06-04 · **Status:** plan, pre-build · **Repo:** bencrane/core-x

## Goal
Materialize a UEI-grain "entity 360" Lance dataset unifying the clean entity masters built
this session into ONE cross-source cohort table — so a query like *"aerospace SAM
registrants who won >$1M federal AND have >50 employees"* is a single indexed table scan
instead of a multi-table join the agent can't express. First concrete cut of the entity
spine: **join what's already resolved** (UEI for registration/awards, normalized-domain for
web firmographics), not full fuzzy resolution.

## Grounded state (built + verified this session, live on R2)
- **`sam_entity_master`** (`active/sam_entity_master/`): 782,543 active SAM registrants,
  1 row/UEI (latest v2 snapshot `2026_MAY`, deduped). Cols: uei, legal_business_name,
  cage_code, registration_*, primary_naics (72% non-null), naics_codes[], psc_codes[],
  business_types, physical_city/state/zip5. BTREE(uei, primary_naics).
  **Does NOT yet carry the normalized entity_url domain** — prereq below.
- **`contractor_award_summary`** (`active/contractor_award_summary/`): 578,958 recipients,
  1 row/recipient_uei. primary_naics/primary_psc (BTREE-indexed this session), obligations,
  top agencies, active flags. BTREE(recipient_uei, primary_naics, primary_psc).
- **`firmographics_blitz`** (`active/firmographics_blitz/`): 133,256 rows, keyed on
  `domain_norm` (BTree). **Built FROM the SAM entity_url**: entity_url → normalize → PDL
  match → enrich PDL LinkedIn via blitz-api; if no PDL match → company LinkedIn via
  parallel.ai → blitz-api. So `domain_norm` IS the normalized SAM entity_url by
  construction. It also has a `uei` column that is a **derived best-guess from the seed —
  UNRELIABLE, NOT a join key**.
- **SAM URL source**: `entity_registrations` v2, `pipe_fields[27]` (`SAM_V2_URL_IDX`).
  Canonical normalization = `_norm_host_sql` (`pipelines/resolution/sam_fmcsa_domain_spine.py:206`):
  lower → trim → strip `http(s)://` → strip leading `www.` → drop path/port/query/fragment
  → strip dots.

## Join model (operator-corrected)
- **firmographics ↔ SAM: join on the normalized entity_url domain**, NOT UEI.
  `sam.entity_url_domain = firmographics.domain_norm`. `firmographics.uei` is ignored.
- **awards ↔ SAM: join on UEI** (`contractor_award_summary.recipient_uei = sam.uei`).
- **One domain → many UEIs is expected (parent/child corporate families).** Do NOT collapse.
  firmographics (one profile per domain) attaches to ALL UEIs under that domain (shared
  web-presence enrichment — attributes describe the web presence/family, not the legal
  entity). Surface the family: carry `entity_url_domain` + `domain_uei_count` per row.

## Build steps
1. **Prereq** — add `entity_url_domain = _norm_host_sql(pipe_fields[27])` to
   `sam_entity_master` (reuse the spine's exact normalization), re-index.
2. **Build `entity_360`** (UEI grain, ~782k rows): base = sam_entity_master;
   LEFT JOIN contractor_award_summary ON uei=recipient_uei (award_* fields);
   LEFT JOIN firmographics_blitz ON entity_url_domain=domain_norm (firmo_* fields);
   compute `domain_uei_count` over entity_url_domain.
3. **Index**: BTREE(uei, entity_url_domain), BTREE(primary_naics), BITMAP(physical_state).
4. **Verify**: row count ~782k; award-attach %, firmographics-attach %, no-URL %; the
   normalized-domain overlap (sam ∩ firmographics.domain_norm); cohort query consumes index;
   parent/child families present and not exploding row counts.

## Explicit assumptions (CHALLENGE THESE)
- **A1** `_norm_host_sql(pipe_fields[27])` produces values that MATCH `firmographics.domain_norm`
  (same normalization). If they differ at all, the firmographics join silently drops to NULL.
- **A2** `firmographics_blitz` is exactly 1 row per `domain_norm` (no fan-out).
- **A3** `contractor_award_summary` is exactly 1 row per `recipient_uei` (no fan-out).
- **A4** domain-join fan-out is bounded: firmographics(1/domain) × SAM(many-UEI/domain) ⇒
  each UEI gets ≤1 firmographics row, no row explosion.
- **A5** latest v2 snapshot = current active universe; entities absent from it are inactive.
- **A6** ~782k base rows (SAM registrants) is the right grain; award-winners are mostly a subset.
- **A7** attaching one firmographics profile to all UEIs sharing a domain is correct, not
  over-attribution.

## Known risks / failure modes (PROBE THESE)
- **R1** Normalization mismatch silently zeroes the firmographics attach (A1).
- **R2** `domain_norm` NOT unique in firmographics → fan-out → row explosion + double-count.
- **R3** `recipient_uei` NOT unique in cas → award fan-out.
- **R4** **Generic/shared domains** (registered-agent, law-firm, hosting, gov, free-email like
  gmail.com) mapping to many UNRELATED UEIs → wrongly group unrelated entities into one
  "family" and over-attribute firmographics. THE key trap.
- **R5** SAM URL coverage low → most of the 360 has NULL firmographics (acceptable, quantify).
- **R6** primary_naics ~28% NULL → cohort coverage gaps.
- **R7** Local build scale: joins + window over ~782k × firmographics × cas.

## Deferred / out of scope
- No-entity_url SAM entities → later name/email-suffix-domain match (FMCSA etc.) + confidence bar.
- Non-entity_url-origin firmographics rows → ignored.
- Full fuzzy entity resolution (the eventual spine) → this is the UEI+domain first cut.

## Output schema (draft)
`uei` (PK), legal_business_name, primary_naics, naics_codes[], physical_state/city/zip5,
registration_status/dates, **entity_url_domain**, **domain_uei_count**,
award_total_obligation, award_primary_naics, award_top_agency, award_active (from cas),
firmo_employee_count/band, firmo_industry, firmo_linkedin_url, firmo_* (from firmographics_blitz),
sam_extract_label.

## Verification env (for the reviewer)
- venv: `/tmp/ngram-venv/bin/python` (lance 7.0.0, duckdb 1.5.3, pyarrow, boto3).
- R2 creds: `doppler run --project core-x --config prd -- <cmd>` injects
  R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_ENDPOINT. storage_options =
  `{aws_access_key_id, aws_secret_access_key, endpoint, region:'auto'}`.
- Open: `lance.dataset('s3://data-sink/active/<name>/', storage_options=so)`.

## Adversarial review outcome (2026-06-04) — SOUND WITH FIXES
Opus reviewer ground-truthed every assumption against live data. Join model, UEI grain, and
parent/child design VALIDATED. Three locked revisions before build:

- **[BLOCKER] B1 — dedupe the domain extraction to 1/UEI.** entity_registrations v2 @2026_MAY
  is 884,203 rows / **876,399 distinct UEIs** (7,804 dup-UEI rows, A-vs-E status). Adding the
  domain via a naive UEI join inflates the base 782,543 → **789,945**. FIX: extract
  `entity_url_domain` WITHIN the sam_entity_master build's existing QUALIFY-1/UEI dedup (no
  separate join). Verified lossless — 0 of the dup UEIs have a conflicting domain. Gate
  tightened to `== 782,543 exactly` (not "~782k").
- **[SERIOUS] S1 — PLATFORM_BLOCK.** ~1,234 UEIs sit under platform/social domains
  (facebook.com 487, sites.google.com 253, linkedin.com 242, instagram.com, linktr.ee,
  frontier.com, google/amazon/youtube/bing) that would stamp a wrong firmographics profile +
  meaningless `domain_uei_count`. FIX: extend the spine's CONSUMER_BLOCK with a platform tier →
  NULL these before the firmographics join. (R4 mass-aggregation otherwise DISPROVEN — top
  fan-out domains are real families: Fresenius, hotel chains, defense primes, Alaska Native corps.)
- **[SERIOUS] S2 — honest enrichment framing.** 54.8% of rows have neither award nor
  firmographics; award attach **39.1%**, firmographics **17.8%**, all-3-signals **9.2%**. FIX:
  add `has_award` / `has_firmographics` booleans + `enrichment_completeness`; consumers must read
  NULL as "no data," not "zero/small." Attach rate is supply-limited by firmographics coverage
  (26.9% of SAM domains), not join quality.

VALIDATED (proven, not assumed): all three masters are 1:1 on their keys (no fan-out); A1
normalization holds (90.9% firmographics overlap, join not silently zeroed); corrected build is
base 782,543 with both LEFT JOINs delta +0; motivating cohort (aero & >$1M & >50 emp) returns
1,149 rows and works. Minor: pin firmographics + SAM to the same `_norm_host_sql` if firmographics
is rebuilt (19 garbage-seed mismatches today); document `domain_uei_count` = SAM-registrant family
size (e.g. hanger.com=806 franchisee/clinic sprawl), not legal hierarchy.
