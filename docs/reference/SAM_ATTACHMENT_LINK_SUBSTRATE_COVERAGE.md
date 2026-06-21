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

## "Gettable" reframing — coverage vs the ADDRESSABLE denominator (not all awards)

The 24% figure is diluted by the un-gettable floor. Removing it (denominator = awards that
carry, or can recover, a Sol#) roughly **doubles** the real coverage rate. Probe:
[`scripts/attach_gettable_coverage_probe.py`](scripts/attach_gettable_coverage_probe.py).
"Gettable" also folds in a SAM-universe recovery (FPDS-blank Sol# recovered via `award_id_piid`
→ universe `award_number` → `solicitation_number`) — but that recovers only **+944** of ~80K
floor awards, so **the no-Sol# floor is effectively fixed; the PIID bridge does not rescue it.**

| cohort | awards | gettable (addressable) | floor (un-gettable) | **HAVE links (% of gettable)** | **DON'T have (crawlable)** |
|---|---:|---:|---:|---:|---:|
| all active | 148,789 | 68,822 (46.3%) | 79,967 (53.7%) | **36,236 (52.7%)** | 32,586 (47.3%) |
| small business | 83,400 | 38,635 (46.3%) | 44,765 (53.7%) | **17,461 (45.2%)** | 21,174 (54.8%) |

So for SB: of the *addressable* market we hold links for **~45%**, not the diluted "21% of all SB."

## Dollar sizing — by `current_total_value_of_award` band

Two opposing gradients, both material:
1. **Gettability RISES with value** (bigger awards carry a Sol# more often) — the floor shrinks where the money is.
2. **Coverage-of-gettable FALLS with value** (the past harvest skewed to construction/verticals & lower value) — so the **high-value band is the least-harvested**, i.e. the richest crawlable backlog.

**Small Business:**
| band | awards | gettable | %get | HAVE links (% of gettable) | DON'T have | crawlable value |
|---|---:|---:|---:|---:|---:|---:|
| all | 83,400 | 38,635 | 46.3% | 17,461 (45.2%) | 21,174 | $48.6B |
| **> $500K** | 15,952 | 8,443 | 52.9% | 3,109 (36.8%) | **5,334** | **$47.6B** |
| > $1M | 11,500 | 6,278 | 54.6% | 2,164 (34.5%) | 4,114 | $46.7B |
| > $5M | 4,177 | 2,395 | 57.3% | 815 (34.0%) | 1,580 | $40.5B |

**All active (reference):** > $500K → 14,423 gettable, 5,324 covered (36.9%), **9,099 crawlable / $585.8B**. (The > $500K cut holds $1,599.8B of the $1,607.5B total active value — the sub-$500K tail is value-negligible, so > $500K *is* the market.)

**Read:** at the > $500K SB cut, **~5,334 addressable awards ($47.6B current value) have attachment links we don't yet hold but could harvest** — we already know their Sol#/notice; it's a crawl, not a discovery. Coverage-of-gettable drops from 45% (all) to ~34% in the > $5M band, so the biggest-dollar lending targets are the *most* under-harvested relative to their reachability.

## Bottom line
- **Notice substrate: done.** We know what exists; no re-discovery.
- **Attachment-link substrate: ~24% of active awards (~21% of SB).** Not "by and large" — the manifests are vertical-scoped.
- **Order of operations to grow coverage:** (1) download+extract the ~10K SB awards whose links are already harvested; (2) Stage-2 crawl the un-harvested Sol#-bearing notices; (3) accept the ~54% no-Sol# floor as an FPDS data limit, not a harvest gap.

Reproduce: `doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb python3 scripts/attach_substrate_coverage_probe.py`.
