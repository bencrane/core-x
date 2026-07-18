# Stage-2 Attachment-Link Harvest — Paced-Crawl Wave Plan

**Mode:** PLANNING + READ-ONLY SIZING. **Snapshot:** 2026-06-21 (UTC), R2 Gen-3 Lance SoR `s3://data-sink/active/`, SAM universe `s3://data-sink/sam-gov-opps/{active,archived}/`. Zero crawl, zero writes, zero SAM.gov requests in producing this plan.
**Stage scope:** Stage **2** of the 3-stage pipeline (`pipelines/sam_gov/REFERENCE_sam_attachment_terminology_and_state.md` Part 2): per-notice `GET /opportunities/{notice_id}/resources` crawl that produces `sam_opps_attachment_manifest*` rows (`resource_id`, `download_url`, `file_name`, `mime_type`, `size_bytes`, notice/solicitation keys) — the attachment **pointer layer, NO bytes**. This plan FEEDS the already-shipped Stage-3 plan (`docs/plans/STAGE3_EXTRACTION_BACKLOG_WAVE_PLAN.md`); it does NOT re-plan Stage 3.
**Probes (reproducible, read-only):** [`scripts/archive/_plan2_link_harvest_probe.py`](../../scripts/archive/_plan2_link_harvest_probe.py) (resolve + size + partition), [`scripts/archive/_plan2_resolution_diag.py`](../../scripts/archive/_plan2_resolution_diag.py) (the resolution-ceiling decomposition — the correctness crux), [`scripts/archive/_plan2_notice_type_diag.py`](../../scripts/archive/_plan2_notice_type_diag.py) (notice-type composition + per-band funnel). Raw JSON: `/tmp/_plan2_link_harvest.json`, `/tmp/_plan2_resolution_diag.json`, `/tmp/_plan2_notice_type.json`.

---

## BLUF

**The crawl is tiny, and the binding constraint is SAM.gov WAF throughput on a residential IP — NOT Anthropic token budget or account count.** This is deterministic Python (`sam_attachment_manifest.py`), not an LLM/agent task; the operator's "multiple Anthropic accounts × sessions" asset is **irrelevant to this stage** (stated plainly, with first-principles reasoning, below). The relevant parallelism is across paced **crawl workers (processes/IPs)** over **disjoint notice buckets**.

- **Target crawl worklist (SB > $500K):** **2,093 distinct un-harvested SOLICITATION-sibling notices** (one `/resources` GET each). Split: **9 active / 2,084 archived**. Genuinely un-harvested: **2,093 / 2,093 already-in-manifest = 0** (probe-confirmed — we are not re-crawling).
- **Forecast manifest rows yielded:** **≈9,900 at the measured mean** (4.74 attachments/harvested-notice) — median-weighted ≈2,100, because the attachment-per-notice distribution is right-skewed (median 1, p90 11, max 713). Call it **≈2,000–10,000 new `resource_id` rows**; some target notices will be **0-attachment** (gettable-but-empty — count, don't retry).
- **Crawl wall-clock:** **≈4.4 minutes at the proven residential envelope** (2,093 requests ÷ ~8 req/s). The full-SB extension (6,300 notices) is **≈13 minutes**. Crawl time is **not** the cost here — WAF blast-radius management and the residential-IP path are.
- **⚠️ The headline correction (probe-proven):** the brief's "~5,334 crawlable SB>$500K awards" does **NOT** map to ~5,334 harvestable notices. Of the **5,052 distinct crawlable Sol#s**, only **597 (11.8%) exist anywhere in the 2.9M-notice SAM universe** — the other **4,455 (88.2%) carry an FPDS `solicitation_identifier` that matches NO `solicitation_number` in SAM** and are therefore **un-crawlable by Sol# at any effort** (not a normalization bug — see the sanity check). Those 597 resolvable Sol#s fan out to **2,093 sibling notices** (≈3.5 notices/Sol#, the multi-notice procurement lifecycle). **The crawlable universe is 2,093 notices, not 5,334 awards.** The remaining ~4,455 awards are a Stage-1-discovery ceiling (FPDS-only sol ids / pre-FY2019 / non-SAM vehicles), not a crawl backlog.

**Single recommended sequencing:** P0 resolve + freeze the 2,093-notice target list (deterministic, read-only) → P1 single-stream paced crawl in disjoint `hash(notice_id)%K` shards (K small; one worker easily clears it, shards exist only for WAF-blast-radius isolation and resume) → P2 land + dedup into a dedicated `sam_opps_attachment_manifest_sb500k/` dataset → P3 verify link-coverage uplift (SB>$500K covered 3,089 → 3,089 + newly-harvested) → P4 hand the new `resource_id`s to the Stage-3 plan as download-pending. **Because the worklist is ~2k notices, a single residential worker is sufficient; parallel crawl workers are an optimization for the full-SB extension, not a requirement for SB>$500K.**

---

## Why the multi-Anthropic-account asset is IRRELEVANT here (first principles)

The Stage-3 plan's centerpiece is a 96-shard static partition across `accounts × sessions` because Stage 3's metered, parallelizable bottleneck is **LLM token throughput** — embarrassingly parallel across Anthropic accounts. **Stage 2 is a different machine.** Its bottleneck is a **network crawl against the SAM.gov public website backend from a residential IP**, governed by:

1. **A polite-crawl envelope.** The proven-safe residential rate is **~8 req/s aggregate** (`sam_attachment_download_90day.py:11-17`: "conc=6 @ 0.1s pace → 0 WAF blocks / 1000 req; conc≥24 trips the WAF in the first ~100 req"). The manifest harvester paces single-stream at `inter_call_sleep=0.12` ≈ 8 req/s (`sam_attachment_manifest.py:282-283`, `:345`).
2. **A WAF circuit breaker with shared blast radius.** 429/403 blocks are **per-IP/per-origin**, not per-account. Anthropic credentials do not touch sam.gov at all — adding accounts adds **zero** crawl throughput. The breaker trips on `≥15 blocks / 60s window` or `≥25 consecutive` (`sam_attachment_download_90day.py:225`) and protects the **IP**, the scarce asset.
3. **Residential-IP scarcity.** Datacenter egress is 429'd (`sam_attachment_manifest.py:23`, `sam_opps_attachment_manifest_90day_winners.py:28`: "datacenter egress is 429'd"). Real parallelism therefore = **number of distinct residential IPs you can pace independently**, each capped at ~8 req/s. With one residential path, **N_WORKERS = 1** and the crawl is still ~4 minutes.

**Conclusion:** the segmentation unit is **disjoint NOTICE shards**, one per paced crawl worker (process+IP), so no two workers fetch the same notice. Token budget / account count is a **Stage-3** lever and is explicitly not the constraint here.

---

## Sizing table (live probe numbers)

Cohort bridge = active SB award (`active_current OR active_potential`, `business_size_code='S'`) → resolved Sol# (FPDS `solicitation_identifier` **first**, else PIID→SAM-universe `award_number`→`solicitation_number` recovery) → **crawlable** = that Sol# is **not** in any harvested manifest → **resolvable** = that Sol# **exists** in the SAM universe → **target notices** = the universe `notice_id`(s) (all sibling notices) carrying that `solicitation_number`, minus any already in a manifest. Sol# norm = `nullif(upper(regexp_replace(trim(x),'[^A-Za-z0-9]','','g')),'')`.

Reproduce:
```
doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb \
  python3 scripts/archive/_plan2_link_harvest_probe.py     > /tmp/_plan2_link_harvest.json
doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb \
  python3 scripts/archive/_plan2_notice_type_diag.py        > /tmp/_plan2_notice_type.json
```

| Cohort | Crawlable awards¹ | Crawlable Sol# | **Resolvable Sol#** (in SAM) | **Target notices** | active / archived | Already harvested | **Genuinely un-harvested** | Forecast manifest rows (mean·median) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **SB > $500K** | 5,440 | 5,052 | **597 (11.8%)** | **2,093** | 9 / 2,084 | 0 | **2,093** | ≈9,920 · ≈2,093 |
| SB > $1M | — | 3,942 | 454 (11.5%) | 1,663 | 7 / 1,656 | 0 | 1,663 | ≈7,880 · ≈1,663 |
| SB > $5M | — | 1,535 | 193 (12.6%) | 844 | 5 / 839 | 0 | ≈4,000 · ≈844 |
| **SB full (extension)** | 21,299 | 20,386 | **2,383 (11.7%)** | **6,300** | 39 / 6,261 | 0 | **6,300** | ≈29,870 · ≈6,300 |

¹ award-grain crawlable (gettable ∧ not covered) from the probe; reconciles to the coverage doc's 5,334/21,174 within snapshot drift (`govcon_active_awards` refreshed between snapshots: SB>$500K crawlable now 5,440 vs the doc's 5,334; SB full 21,299 vs 21,174). **The award count is NOT the crawl size** — see the resolution ceiling.

**Forecast-rows method:** target-notices × attachments-per-harvested-notice (mean 4.74 / median 1.0 / p90 11 / max 713, over the 319,521 harvested notices in the manifest union — `attachment_yield_per_harvested_notice`). Use the **median-weighted lower bound (~1×)** for storage/Stage-3 planning and the mean for an upper bound; the true yield lands between because the target set skews to solicitation-type notices (which carry more attachments than the median archived award notice).

### The resolution ceiling — the correctness crux (`_plan2_resolution_diag.py`)

```
doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb \
  python3 scripts/archive/_plan2_resolution_diag.py > /tmp/_plan2_resolution_diag.json
```

| Fact | Value | Meaning |
|---|---:|---|
| SB>$500K crawlable Sol# (distinct) | 5,052 | the brief's "go-crawl" set, at Sol# grain |
| …**in the SAM universe** | **597 (11.8%)** | the only ones that resolve to a crawlable notice |
| …**NOT in SAM universe** | **4,455 (88.2%)** | FPDS sol id with **no** matching SAM `solicitation_number` — un-crawlable by Sol# |
| **Sanity:** *covered* crawlable-cohort Sol# in universe | **2,742 / 2,753 = 99.6%** | when a Sol# is harvestable it **does** resolve → the gap is **NOT** a normalization bug |
| Crawlable Sol# from FPDS origin → in universe | 477 / 4,932 | FPDS-only sol ids dominate the un-resolvable floor |
| Crawlable Sol# from PIID-recovery → in universe | 120 / 120 | PIID recovery is in-universe by construction (adds 120 Sol#) |

**Read:** the 99.6% sanity pass proves normalization is correct — the 88% un-resolvable floor is a **genuine Stage-1 data limit** (FPDS carries a `solicitation_identifier` SAM never published as a notice: pre-FY2019 archived-out, GSA/eBuy/non-SAM vehicles, or FPDS-internal sol formats). **No crawl recovers these.** They are out of scope, the same way the no-Sol# floor is out of scope in the coverage doc.

### Target-notice composition (SB>$500K) — attachments live on the solicitation sibling (`_plan2_notice_type.json`)

| base_type / notice_type | distinct notices | role |
|---|---:|---|
| Solicitation | 899 | **primary document host** (SOW/PWS/specs) |
| Combined Synopsis/Solicitation | 280 | **primary document host** |
| Presolicitation | 323 | early host (often carries draft SOW) |
| Award Notice | 362 | sibling; usually few/no attachments |
| Sources Sought | 155 | early market-research sibling |
| Special Notice / Justification / other | 74 | siblings |

**1,502 of 2,093 (72%) are solicitation/presolicitation-type document hosts** — exactly where the package lives (`REFERENCE_…md` Part 1.1). The 597 resolvable Sol#s fan out to 2,093 notices (≈3.5 sibling notices/Sol#); **crawling all siblings of a Sol# is correct** because, across the lifecycle, the attachment package may sit on any of them (Part 1.1, §3.4 Sol#-sibling caveat).

---

## Award → notice RESOLUTION design (the bridge, with the Sol#-sibling caveat)

The resolution chain mirrors `sam_play1_target_select.py:13-22` (QUALIFY→FOOTPRINT→**JOIN SAM universe on `award_number=PIID` OR `solicitation_number=sol_id`**→target notice_ids), narrowed here to the SB>$500K crawlable cohort instead of an entity vertical:

```
active SB award (current_total_value > $500K)            [govcon_active_awards]
   │  resolve Sol#:  fpds_sol = norm(solicitation_identifier)
   │                 else PIID recovery: norm(award_id_piid) → universe.award_number → norm(solicitation_number)
   ▼
crawlable Sol#  = resolved Sol# NOT IN (union of manifest solicitation_number)   [5,052 distinct]
   │  JOIN SAM universe (active ∪ archived) on  uni.solicitation_number(norm) = crawlable Sol#
   │     → ALL sibling notices sharing that Sol#  (Solicitation, Combined, Presol, Award, Sources Sought, …)
   ▼
target notice_ids  = DISTINCT uni.notice_id  MINUS  notices already in any manifest   [2,093]
   │  (one /resources GET each → Stage-2 manifest rows)
   ▼
Stage-2 harvest (sam_attachment_manifest.py over the target notice list)
```

**The Sol#-sibling caveat is handled by construction:** the bridge joins on `solicitation_number`, so it returns the **solicitation notice (the document host), not the award notice** — the exact join `REFERENCE_…md` §3.4 flagged as "had not been run." It deliberately keeps **all** sibling notices for a Sol# (not just the highest-ranked base_type) because the manifest harvester's cost is one cheap GET per notice and attachments can appear on any sibling; the winners builder's single-winner ranking (`sam_opps_attachment_manifest_90day_winners.py:91-101`) is an optimization for a different (bytes-bound) objective and is **not** needed when the per-notice GET is free. **PIID-sibling recovery** (`sam_play1_target_select.py:229-233`) adds the 120 PIID-only Sol#s that FPDS left blank.

---

## Phased plan

Every phase: objective · commands · ENTRY · EXIT (measurable) · idempotency · blast-radius · ledger.

### P0 — Resolve + freeze the target notice list
- **Objective:** materialize the immutable 2,093-notice crawl worklist (SB>$500K) with the priority-band + sibling-Sol# columns; baseline coverage.
- **Commands:** re-run `_plan2_link_harvest_probe.py` + `_plan2_notice_type_diag.py`; write the resolved `(notice_id, sol_norm, src, base_type, hash_shard)` worklist to a **target-universe Lance dataset** the harvester reads unchanged — exactly the `sam_play1_target_select.py` pattern (`SAM_PLAY1_TARGET_URI` → `_play1_target_universe_*`), here `s3://data-sink/active/_stage2_target_sb500k/`.
- **ENTRY:** none (read-only inputs).
- **EXIT:** target dataset row count == 2,093; **0 notices already in any manifest** (probe `target_notices_already_in_manifest = 0` ✓); 72%+ solicitation-type confirmed; baseline `covered=3,089` recorded.
- **Idempotency:** pure recompute; deterministic Sol#→notice join + `abs(hash(notice_id))%K` → byte-identical worklist on re-run.
- **Blast radius:** one scratch dataset write under `_stage2_target_*` (selector pattern, `sam_play1_target_select.py:112-135` publish-to-R2); no SAM requests.
- **Ledger:** snapshot JSON to `/tmp`; the target dataset URI is the durable record.

### P1 — Paced crawl in disjoint notice shards
- **Objective:** harvest `/resources` for the 2,093 target notices, WAF-paced, in disjoint shards so parallel workers (if used) never collide.
- **Commands:** point the existing harvester at the frozen target universe and run it:
  ```
  SAM_OPPS_LANCE_URI=s3://data-sink/active/_stage2_target_sb500k/ \
  SAM_ATTACH_MANIFEST_URI=s3://data-sink/active/sam_opps_attachment_manifest_sb500k/ \
  doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with requests --with 'psycopg[binary]' \
    python pipelines/sam_gov/sam_attachment_manifest.py --do-remaining --resume \
      --inter-call-sleep 0.12 --checkpoint-every 200
  ```
  `--do-remaining` sweeps the whole (already-narrowed) universe; `--resume` reloads prior rows and skips captured notices (`sam_attachment_manifest.py:205-213,252`). For the full-SB extension, run **disjoint shards** as separate workers (see Segmentation) — needs the one minimal code addition below.
- **ENTRY:** P0 EXIT met; residential-IP path confirmed (datacenter egress is 429'd).
- **EXIT:** every target notice reaches a terminal state (rows harvested **or** recorded 0-attachment); `notices_covered == 2,093`; `zero_attach_notices` counted (gettable-but-empty, not retried); 429/403 stayed within the breaker envelope.
- **Idempotency:** `--resume` = reload manifest, skip `captured` notices (`:205-213,252`); checkpoint every 200 notices (`write_manifest`, `:215-222`). A re-run continues from the last checkpoint; re-harvesting a notice re-derives identical rows (overwrite mode, deterministic).
- **Blast radius:** the harvester's `fetch_resources` backs off on 403/429/503 (`:235-238`) — but it has **no circuit breaker**. **Add the download envelope's `Breaker` (`sam_attachment_download_90day.py:221-251`) to protect the IP** (minimal addition #2). Off-peak run; one residential IP.
- **Ledger:** `ops.sam_attachment_manifest_runs` (DDL inline in `sam_attachment_manifest.py:95-102`; one row per run — `active_total`, `notices_covered`, `attachments`, `zero_attach_notices`, `api_calls`).

### P2 — Manifest land + dedup
- **Objective:** the harvested rows land as a dedicated, indexed manifest dataset that the coverage probes and Stage-3 plan already union over.
- **Commands:** the harvester writes `sam_opps_attachment_manifest_sb500k/` (Lance v2.0) and builds BTREE/BITMAP indices in its `finally` block (`sam_attachment_manifest.py:300-310`: `notice_id`, `resource_id`, `naics_code` BTREE; `trigger_relevant`, `mime_type`, `access_level` BITMAP). **Add this URI to the `MANIFESTS` list** in `attach_substrate_coverage_probe.py` / `attach_gettable_coverage_probe.py` / `_plan_stage3_backlog_probe.py` so coverage unions pick it up (minimal addition #3 — one line each).
- **ENTRY:** P1 EXIT.
- **EXIT:** new dataset opens; `count_distinct(resource_id)` ≈ forecast; `notice_id`/`resource_id` BTREE present; dedup is automatic (grain = one row per (notice, attachment); identical `resource_id` cited by many notices is expected and preserved at citation grain, deduped to file grain by downstream `DISTINCT resource_id` exactly as Stage 3 does).
- **Idempotency:** Lance `mode="overwrite"` with the full row set each checkpoint → no double-append; resume reloads then rewrites.
- **Blast radius:** isolated new dataset URI; never mutates the existing manifests.
- **Ledger:** same `ops.sam_attachment_manifest_runs` row (terminal `status='success'`).

### P3 — Verify link-coverage uplift
- **Objective:** prove SB>$500K `covered` rose and `crawlable` fell.
- **Commands:** re-run `attach_substrate_coverage_probe.py` and `attach_gettable_coverage_probe.py` (now unioning the new manifest) and `_plan2_link_harvest_probe.py`.
- **ENTRY:** P2 done.
- **EXIT thresholds:** SB>$500K `covered` ≈ **3,089 + (resolvable awards newly harvested)** — i.e. up to **+631** (the `crawl_awards_with_resolvable_notice` for SB>$500K); SB>$500K `crawlable` (gettable∧¬covered) drops by the same; `target_notices_genuinely_unharvested` for SB>$500K → ≈0. (Uplift is bounded by the 597-Sol#/631-award resolvable set, **not** the 5,440 headline — the 4,455 un-resolvable awards stay uncovered by design.)
- **Idempotency:** read-only probes.
- **Blast radius:** none.

### P4 — Handoff to Stage-3
- **Objective:** the newly harvested `resource_id`s enter the Stage-3 download-pending worklist.
- **Commands:** re-run `_plan_stage3_backlog_probe.py` (`docs/plans/STAGE3_EXTRACTION_BACKLOG_WAVE_PLAN.md` P0). The new manifest's `resource_id`s that are ∉ `sam_attachment_files(status='downloaded')` are, by definition, **Stage-3 download-pending** for the SB>$500K cohort.
- **ENTRY:** P3 EXIT.
- **EXIT (the handoff metric):** Stage-3 probe's `sb_gt_500k.download_pending_files` **rises by the newly-harvested file count** (these are net-new links to download); the Stage-3 P1 download wave then consumes them. **This plan stops here — Stage 3 owns the bytes.**
- **Idempotency / blast radius:** read-only; no Stage-3 mutation in this plan.

---

## Segmentation: disjoint NOTICE shards (static, by construction)

Same disjoint-by-construction principle as the Stage-3 plan, but the unit is **notices** (not `resource_id`s) and the parallelism cap is **residential IPs** (not accounts):

```
shard = abs(hash(notice_id)) % K        # one paced crawl WORKER (process+IP) per shard
```

**Partition verification** (`_plan2_link_harvest_probe.py` → `partition`, over the 2,093 SB>$500K target notices):

| Property | K=4 | K=8 | K=12 | Pass |
|---|---|---|---|---|
| rows == distinct notice_id | 2,093 == 2,093 | — | — | ✓ |
| Σ(shard sizes) == total | ✓ | ✓ | ✓ | ✓ disjoint-by-construction (each notice → exactly one shard) |
| max deviation from even | **5.35%** | 13.38% | 14.91% | ✓ even at small K; skew grows as buckets shrink |

**0 notices in >1 shard** (structural — `abs(hash(notice_id))%K` is a function). At K=4 the split is even to ±5.4% (511/539/514/529). **Recommendation:** SB>$500K needs **K=1** (a single ~4-minute worker); use **K=4** only if running parallel residential paths for the full-SB extension. As in the Stage-3 plan, this is **static disjoint-by-construction on append-only R2 — NO runtime claiming** (a worker can be mid-crawl/pre-commit while another claims the same notice; the non-transactional store can't arbitrate that race; assignment-time disjointness makes commit timing irrelevant).

**`--do-remaining` shard scoping — capability gap + minimal addition:** `sam_attachment_manifest.py --do-remaining` sweeps **its entire source universe** (`:179-184`), with **no notice-shard filter and no `--notice-ids-file`**. For SB>$500K this is fine — P0 narrows the source universe to exactly the 2,093 target notices via `SAM_OPPS_LANCE_URI`, so `--do-remaining` already crawls only them. **For multi-worker shard parallelism (full-SB extension), add a thin shard filter** — see minimal additions #1.

---

## Crawl pacing & WAF risk

Reuse the proven residential envelope from `sam_attachment_download_90day.py`:

| Lever | Source | Value |
|---|---|---|
| Aggregate request rate | `sam_attachment_download_90day.py:11-17` (ground-truth probe) | **~8 req/s** (conc=6 @ 0.1s → 0 WAF blocks / 1000 req) |
| Manifest single-stream pace | `sam_attachment_manifest.py:282-283,345` | `inter_call_sleep=0.12` ≈ 8 req/s |
| Circuit breaker trip | `sam_attachment_download_90day.py:225` | ≥15 blocks / 60s window **or** ≥25 consecutive → drain & stop |
| 429/403/503 in-harvester backoff | `sam_attachment_manifest.py:235-238` | exponential, capped 120s |
| Datacenter egress | `sam_attachment_manifest.py:23` | **429'd — residential IP mandatory** |

- **What happens on a 429/403 cluster:** the harvester already backs off exponentially per notice (`:235-238`); add the `Breaker` so a **cluster** drains in-flight and **stops submitting** (protects the IP) rather than grinding through 25+ consecutive blocks. On trip, the `--resume` checkpoint lets a later off-peak re-run continue from the last good notice.
- **Off-peak:** run outside US business hours to minimize WAF sensitivity; the SB>$500K crawl is ~4 minutes so the window is trivial to find.
- **Residential-IP scarcity is the true parallelism cap:** each independent residential path sustains ~8 req/s. One path clears SB>$500K in ~4 min and full-SB in ~13 min — **parallel workers are unnecessary for SB>$500K and a mild speedup for full-SB.**

---

## Verification & acceptance

| Metric | Probe | Target |
|---|---|---|
| Target notices genuinely un-harvested | `_plan2_link_harvest_probe.py` → `sb_gt_500k.target_notices_genuinely_unharvested` | 2,093 → **≈0** post-crawl |
| Notices reaching terminal state | `ops.sam_attachment_manifest_runs.notices_covered` | == 2,093 |
| 0-attachment notices (gettable-but-empty) | `…manifest_runs.zero_attach_notices` | counted, **not** retried |
| New manifest files | `count(DISTINCT resource_id)` on `sam_opps_attachment_manifest_sb500k/` | ≈2,000–10,000 |
| **SB>$500K `covered`** | `attach_gettable_coverage_probe.py` (union incl. new manifest) | **3,089 → up to 3,089 + 631** (resolvable awards) |
| **SB>$500K `crawlable`** | same | falls by the newly-harvested resolvable count |
| **Handoff:** SB>$500K download-pending | `_plan_stage3_backlog_probe.py` → `sb_gt_500k.download_pending_files` | **rises** by new file count (Stage-3 input) |
| Partition integrity (if sharded) | `_plan2_link_harvest_probe.py` → `partition` | notices-in->1-shard = **0**; K=4 even ±5.4% |

**Acceptance is bounded by the resolvable set, not the headline award count:** success = the ~597 resolvable Sol# / ~631 SB>$500K awards / 2,093 notices are harvested and handed to Stage-3; the 4,455 un-resolvable awards are **explicitly out of scope** (Stage-1 discovery limit).

---

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| **WAF / residential-IP ban** | Reuse the proven envelope (~8 req/s, `sam_attachment_download_90day.py:11-17`); **add the `Breaker`** (`:221-251`) to `sam_attachment_manifest.py` so a 429/403 cluster drains & stops instead of grinding. Off-peak run; `--resume` continues after a trip. |
| **The 88% un-resolvable floor** (the crux) | **Stated, not pursued.** 4,455 of 5,052 SB>$500K crawlable Sol#s match no SAM notice (FPDS-only sol ids / pre-FY2019 / non-SAM vehicles). No crawl recovers them — a Stage-1 discovery limit, same class as the no-Sol# floor. Acceptance targets the 597 resolvable Sol# only. |
| **0-attachment notices** (gettable-but-empty) | Expected (median attachments/notice = 1; many notices have 0). The harvester records them in `captured`/`zero_attach` and **does not retry** (`sam_attachment_manifest.py:260-261`). Count, don't chase. |
| **`size_bytes` defect** | `size_bytes` is a **lower bound** (corrupt ≥10 MB; `sam_attachment_manifest.py:29-41`, `REFERENCE_…md` §1.7). **Do NOT use it for Stage-3 byte budgeting** — true size is known only at download (`size_downloaded`). Forecast Stage-3 GB via the ledger's ~2.45 MB mean (§3.3), not the manifest sum. |
| **Sol#-sibling correctness** | The bridge joins on `solicitation_number` (→ solicitation host notice, not award notice) and keeps **all** siblings, so the package-bearing notice is always in the worklist (`REFERENCE_…md` §1.1, §3.4). The 72% solicitation-type composition confirms it. |
| **Archived-universe scale** | 2,084/2,093 target notices are archived (the 2.9M-row set). The probe pushes the Sol# join into the Lance scan and the crawl only touches the 2,093 matched `notice_id`s — never materializes the full archived universe (perf note: `sam_opps_attachment_manifest_90day_winners.py:35-39`). |
| **Harvester has no shard filter** | `--do-remaining` sweeps the whole source universe; for SB>$500K, P0's narrowed `SAM_OPPS_LANCE_URI` makes that exactly the 2,093 notices (no code change). Multi-worker sharding (full-SB) needs minimal addition #1. |
| **Snapshot drift** | `govcon_active_awards` refreshes between snapshots (SB>$500K crawlable 5,440 today vs the doc's 5,334). Re-run P0 probes immediately before crawling to refreeze the worklist. |

---

## Minimal net-new code vs what already exists

**Already exists (cited, reused unchanged):** the harvester (`sam_attachment_manifest.py` — `/resources` crawl, `--do-remaining`, `--resume`, checkpointing, indexing, `ops.sam_attachment_manifest_runs` ledger); the target-universe selector pattern (`sam_play1_target_select.py` — the award→notice resolution chain + publish-to-R2); the residential pacing envelope (`sam_attachment_download_90day.py` `RateLimiter`/`Breaker`); the coverage + backlog probes.

**Net-new (small, listed):**
1. **A shard filter for the harvester** — add `--notice-ids-file <path>` (or `--shard k --of K` applying `abs(hash(notice_id))%K==k` to `order` at `sam_attachment_manifest.py:182`). ~10 lines; only needed for multi-worker parallelism on the full-SB extension. SB>$500K needs none.
2. **Wire the `Breaker`** (`sam_attachment_download_90day.py:221-251`) into `sam_attachment_manifest.fetch_resources` (`:224-242`) — record 403/429 to the breaker; bail when tripped. ~8 lines; protects the residential IP on a WAF cluster.
3. **One-line union additions** — append `sam_opps_attachment_manifest_sb500k/` to the `MANIFESTS` list in the three coverage/backlog probes so uplift is measured automatically.

The award→notice resolution itself is a **new P0 script** (`_plan2`-style → `_stage2_target_sb500k/`), but it is a direct narrowing of `sam_play1_target_select.py` — no new framework. Everything downstream (crawl, land, index, ledger, verify, Stage-3 handoff) already exists.
