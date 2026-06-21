# SAM attachment-link substrate — coverage of active awards (do we still need to "go get the links"?)

**Mode:** READ-ONLY probe. **Snapshot:** 2026-06-21 (UTC). **Probe:** [`scripts/attach_substrate_coverage_probe.py`](scripts/attach_substrate_coverage_probe.py). Raw JSON: `/tmp/attach_cov.json`.
**Context:** runs the Award→Sol#→attachments bridge that `pipelines/sam_gov/REFERENCE_sam_attachment_terminology_and_state.md` (§3.4) flagged as "not yet run." Refreshes that doc's 2026-06-07 numbers.

## The distinction that matters

There are **two different "substrates,"** and they are at very different completeness:

| Layer | What it is | State |
|---|---|---|
| **Stage 1 — Notice universe** | every SAM notice + its notice-page `link`, Sol#, PIID keys | **COMPLETE** — ~2.92M notices (77,138 active + 2,839,948 archived FY19–26) |
| **Stage 2 — Attachment-link manifest** | per-notice attachment `download_url`s (the pointer layer needed to fetch PDFs) | **PARTIAL** — vertical-scoped; covers ~24% of active awards |
| Stage 3 — Bytes / extraction | downloaded PDFs → extracted requirements | small subset of Stage 2 |

When people say "the solicitation links substrate," they mean **Stage 2** (the attachment `download_url`s), not the Stage-1 notice link. Stage 1 is done. Stage 2 is not.

## What the Stage-2 link substrate currently holds (union of all manifests)

| metric | value |
|---|---:|
| attachment citations | 1,514,762 |
| **distinct files (`resource_id`)** | **589,166** |
| distinct solicitations | 181,438 |
| distinct notices | 319,521 |

Scoped by *vertical/entity footprint* (A&D `play1`, remediation, equipment-rental) + the original active trigger sweep — **never by recent-award**.

## The funnel — active awards (148,789)

| stage | awards | % of active |
|---|---:|---:|
| active awards | 148,789 | 100% |
| …carry a `solicitation_identifier` (bridgeable on Sol#) | 67,878 | **45.6%** |
| …**attachment links harvested** (in some Stage-2 manifest) | **35,850** | **24.1%** |
| …extracted to `govcon_award_scope_requirements` (serving) | ~28,774¹ | ~19% |

¹ via the profile `source_resource_ids` fan-out; the inline `contract_award_unique_key` on `govcon_award_requirements` undercounts (4,488) because it "loses 75%" (see the materialize spine note).

**Small-Business cohort (83,400):** 38,202 carry a Sol# (45.8%) · **17,322 link-covered (20.8%)** · ~7,051 extracted (8.5%, from the cash-crunch probe).

| cohort | awards | have Sol# | link-covered | % link-covered |
|---|---:|---:|---:|---:|
| all active | 148,789 | 67,878 | 35,850 | 24.1% |
| small business | 83,400 | 38,202 | 17,322 | 20.8% |
| SB whale services (Big-3 ∨ SCA) | 17,922 | 9,829 | 4,022 | 22.4% |
| SB construction (Davis-Bacon) | 4,073 | 2,643 | 1,120 | 27.5% |

(`link_covered_via_awardkey` from the winners manifest = 33,475 all-active but is a **subset** of the Sol#-covered set — it adds nothing beyond Sol# matching.)

## Two ceilings — and the cheapest next move

1. **Data ceiling (independent of harvest effort):** only **45.6%** of active awards carry a `solicitation_identifier` in FPDS at all. The other 54% can only be bridged to their solicitation's attachments via the weaker `award_number`(PIID)→universe→notice path, or are unreachable by Sol#. **No amount of crawling fixes this half** — it's an FPDS population limit.

2. **Harvest ceiling (fixable by crawling):** of the awards that *do* carry a Sol#, ~53% are already link-covered (17,322 of 38,202 for SB); the rest need a Stage-2 `/resources` crawl. **This is a throughput problem, not a discovery problem** — we already hold every notice_id and the Sol#/PIID join, so "go get the links" = run `sam_attachment_manifest.py --do-remaining` over the un-harvested notices.

3. **⭐ The cheapest win is downstream, not the crawl:** ~**17,322 SB awards already have their attachment links harvested**, but only ~**7,051** have been downloaded + extracted. That is **~10,000 SB awards whose links are already in hand (Stage 2 done) but bytes/extraction are not (Stage 3 pending).** Closing that gap needs **no crawling at all** — just run download + extract over links we already hold. This is the first lever to pull.

## Bottom line
- **Notice substrate: done.** We know what exists; no re-discovery.
- **Attachment-link substrate: ~24% of active awards (~21% of SB).** Not "by and large" — the manifests are vertical-scoped.
- **Order of operations to grow coverage:** (1) download+extract the ~10K SB awards whose links are already harvested; (2) Stage-2 crawl the un-harvested Sol#-bearing notices; (3) accept the ~54% no-Sol# floor as an FPDS data limit, not a harvest gap.

Reproduce: `doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb python3 scripts/attach_substrate_coverage_probe.py`.
