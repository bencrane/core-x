# Subawardee GTM — Contact Access & Signal Surface (Extended Diagnostic)

> **⚠️ Renamed 2026-06-19 ([#542](https://github.com/bencrane/core-x/pull/542)):** the SAM/govcon `_90day` suffix was dropped plane-wide (window-as-data — the harvest window is a read-time predicate, not part of the stored entity's name). Every `*_90day` dataset/ledger named below now lives at its de-suffixed canonical name (e.g. `govcon_scope_vectors_90day` → `govcon_scope_vectors`). R2 was server-side-copied — versions/indices/row-counts carry over unchanged; figures below predate the rename but remain valid under the new names.

> Extension to `docs/reference/SUBAWARDEE_GTM_AUDIENCE_READINESS_DIAGNOSTIC.md`. Assumes the reader has it. This document answers the two operator asks — (i) POC/contact access at subawardee firms, (ii) targeting on the **content/signal** of a sub's subaward + solicitation past — and maps the full surface area: possible **now** vs unlocked by **X/Y/Z**. All claims are code-cited (`file:line`) or dataset-cited (`dataset:version`). Where independent verification corrected a recon or strategy-lens claim, the corrected value is used and flagged **[CORRECTED]**.
>
> **2026-06-19 accuracy pass.** Every quantitative claim was re-measured live against the R2 SoR + gtm-mcp `/healthz` + hq-x Postgres on 2026-06-19. The Path-B profiles widening is **landed**: `govcon_subawardee_capability_profiles` is **v69 · 25,450 rows (full universe)** with **13,792** subs carrying self-reported tags and `tag_source` {self_reported 11,353 / both 2,439 / scope 1,293 / none 10,365}. Every prior provisional landing flag on the profiles table is now **confirmed full-universe** and rewritten accordingly; the §6 master gate is resolved. Self-reported tags, HQ/PoP geo, and `poc_available` (22,737) are full-universe; only the *prime-solicitation-derived* fields (scope-derived `capability_tags` 3,732, clearance 2,497, certs, labor) remain bridge-scoped by construction.

---

## Update — 2026-06-19 (diversification surface shipped)

The captive-diversification motion this doc proposed (the "diversify off your one prime" play; §3 build-option and the §4 P1 captive segment) is now **SHIPPED & LIVE**, not future. New dataset `captive_sub_diversification_90day` (`s3://data-sink/active/captive_sub_diversification_90day/`, Lance storage-format v2.1, live dataset-version probe **v15**) is the ANN, full-universe upgrade of the deterministic `capability_match` leg — live production run (measured 2026-06-19): **28,965 rows · 3,156 distinct captive subs scored · 1,541 distinct new primes · 1,862 distinct matchable awards · avg cosine 0.7215**; usable `naics2_aligned=true` tier = **8,118 rows / 2,340 subs**, `naics4_aligned` = **2,973 rows / 1,341 subs** (raw cosine alone produced domain-wrong matches, so the NAICS gate is the load-bearing filter). Standing surface merged in **PRs #538/#539**: Modal worker `pipelines/serving/materialize_captive_diversification.py` (app `captive-diversification`, fn `run_build`, deployed + executed; `ops.captive_diversification_serving_runs` latest row `status=success`, `completed_at=2026-06-19 15:35 UTC`), and `src/trigger/captive_diversification.ts` weekly schedule (Mon 11:00 UTC) **defined and merged to `main` but not yet registered with Trigger.dev** (`npm run trigger:deploy` requires a `TRIGGER_ACCESS_TOKEN`); the dataset is live and rebuildable on demand now. The award-side bridge ceiling, re-measured below: **4,988 distinct matchable awards (0.40% of 1,247,391 prime awards)** — the all-dates validation run hit 2,298 of those; the live 365-day production window resolved 1,862. CUI-safe: no scope-vector text is emitted; the dataset identifies firms + candidate prime, **not contacts**. Full spec: `docs/reference/CAPTIVE_SUB_DIVERSIFICATION_DATASET.md`.

---

## 0. Verdict (read first)

**Ask (i) — POC / contact access.** You can reach a **named person** at a subawardee firm today from a bare `subawardee_uei` — but **never an email or phone in a single read**, and never from the SAM POC. Three person-data sources key (directly or via cage) to a sub UEI: SAM **entity-registration** POCs (`sam_pocs` v162, named EB/GB officers, **1,540,965 distinct UEI** registry-wide, measured 2026-06-19), the profiles' denormalized copy (`govcon_subawardee_capability_profiles.poc_*`, now **22,737** of the 25,450 full-universe subs), and FFATA **named executives** (`ffata_exec_comp` v100, **6,892 distinct UEI**, prime-recipients only). **Every one of them carries name/title/geo (and for FFATA, dollars) — zero email, zero phone, by SAM-source construction** (`sam_pocs` schema measured 2026-06-19: `address_line_1/2, city, zip5, zip4, country, state` and no email/phone/fax column). Send-grade email is a **5-hop live-manufactured bridge**, not a column: `subawardee_uei → sam_master_domains (709,546 rows, BTREE uei) → normalized_domain → launch_contact_hydration (Waterfall) → ops.email_resolutions → enroll`. The dominant attrition is the very first hop (~46% ceiling on SAM entities with a usable `entity_url`; the sub-specific rate is **unmeasured** — do not quote a send-grade reach number).

**Ask (ii) — signal / content targeting.** The sub's past splits into two provenance sides that determine coverage. The **sub's own words** (`subaward_description` → semantic vectors + Path-B self-reported 77-vocab tags + $/teaming/geo/NAICS) are **FULL-universe (25,450)**. The **prime's solicitation** (clearance, certs, the 11-type requirement enum, scope summaries, labor categories, scope-derived capability tags) is **BRIDGE-only (≤6,586, hard floor)** because clearance/scope live in the prime's harvested solicitation, not the sub's record. The profiles table is now **landed at the full universe** (`v69 · 25,450 rows`, measured 2026-06-19): semantic vectors (`v8`), the fact table (`contract_subaward v12`), the **self-reported tag axis (13,792 tagged)**, `tag_source`, HQ/PoP geo, and `poc_available` (22,737) are all full-universe; only the prime-solicitation-derived fields stay bridge-bound.

**The single biggest unlock — now realized.** The Path-B classifier + profiles rebuild (`classify_sub_self_reported_tags submit→retrieve`, then `build_subawardee_capability_profiles build`) **has run**. The code merged (#503/#504/#506) and the dataset is now **landed: `v69 · 25,450 rows`**, with the sidecar `govcon_sub_self_reported_tags` (v2 · 66,275 rows) populated and **13,792** subs carrying `self_reported_capability_tags`. That run flipped the self-reported capability tags **and** HQ/PoP geo from 6,586 → 25,450 subs and lit up the catalyst route + raw-SQL paths with no further code change. The remaining top unlock is the **send-grade contact bridge** (materialize `subawardee_uei → domain → people/email` once, vs per-campaign live hydration) — the binding constraint on converting any widened audience into outreach.

---

## 1. Contact / POC access reality

### 1.1 The three UEI-reachable person sources — what each carries

| Source | Dataset:version | Key from sub | Person fields | Email/Phone at schema? | Sub universe |
|---|---|---|---|---|---|
| SAM **entity-registration** POC (EB + GB named officers + 4 alternates) | `sam_pocs` (storage v2.1; probe v162)¹ | `subawardee_uei → sam_pocs.uei` (v2 spine; legacy via `cage_code`) | `first/middle/last_name, title, address_line_1/2, city, zip5, zip4, country, state` (`sam_pocs.py:198-201`; schema confirmed — no email/phone column) | **NO** — none exist at SAM source | **1,540,965 distinct UEI** registry-wide (measured 2026-06-19); near-all SAM-registered subs |
| Profiles structured POC (denormalized `sam_pocs` GB slot) | `govcon_subawardee_capability_profiles v69`² | `sub_uei` BTREE | `poc_available, poc_full_name, poc_title, poc_type, poc_city, poc_state` (`govcon_gtm_schemas.py:359-362`) | **NO** | **22,737 of the 25,450 full-universe subs** (measured 2026-06-19) |
| FFATA exec comp (top-5 named officers + $) | `ffata_exec_comp` (storage v2.1; probe v100)¹ | `recipient_uei` BTREE | `officer_rank (1-5), officer_name, officer_amount` (`ffata_exec_comp.py:169-179`) | **NO** (names + $ only) | **6,892 distinct UEI** (measured 2026-06-19); subs only when independently a **threshold prime**; sub-recipient layout **deferred** (`ffata_exec_comp.py:27`) |
| Govt **solicitation** POC (the contracting office) | `sam_opps_archived_bulk` (notice grain) | `notice_id` — **NOT** sub UEI | `primary_contact_fullname/email/phone` (`sam_opps_archived_bulk.py:100-102`) | **YES** — but **buyer-side**, never attaches to a sub | n/a to sub outreach |

¹ `v2.1` is the hard-coded `DATA_STORAGE_VERSION` (Lance storage-format), distinct from the monotonic dataset-version probe (`sam_pocs` probed at v162, `ffata_exec_comp` at v100 on 2026-06-19). **[CORRECTED]**
² `govcon_subawardee_capability_profiles` dataset-version is **v69** (live probe 2026-06-19), distinct from `data_storage_version="2.1"` pinned in `govcon_gtm_schemas.py:392`. **[CORRECTED]**

**The one mental-model inversion the operator must hold:** the SAM source the prompt asked about (the entity-**registration** EB/GB POC) **is present and named** — but the SAM public monthly extract publishes **zero contact channels** for it. The 142-field v2 public layout (`sam_v2_public_field_map.py`) is exactly 142 positions; a case-insensitive grep for `email|phone|fax|telephone` returns **zero**. The six POC blocks (positions 47–112, `sam_master.py:75,79-80`) are each 11 name/title/geo fields. This is a hard **upstream** constraint, not a projection choice (`config.py:63-64`; `models.py:223-224,429-430` declare catalyst `SamPoc.email`/`phone` as permanent GAP fields, always null).

### 1.2 The bridge from "firm" to "send-grade email" — what exists vs what must be enriched

**What exists on the shelf:** name/title/geo (SAM POC), name/$ (FFATA, rare). **No email anywhere in any UEI-keyed Lance dataset.** Verified work email lives **only** in `ops.email_resolutions` (hq-x Postgres, **PK `contact_id`** + `company_domain`, `ops_blitz_email_finder_runs.sql:21-50`) — keyed by `contact_id`/domain, **never UEI**, **never Lance**. Codebase grep confirms **no email→Lance materializer exists**; the aspirational comment at `ops_blitz_email_finder_runs.sql:13-15` describes one that was never built. **[CORRECTED]** — state it as "email is Postgres-only, never materialized to UEI."

**The only send-grade path (must be constructed, then hydrated live):**

```
subawardee_uei
  → sam_master_domains  (entity_url → normalized_domain; 709,546 rows; BTREE uei + domain)
  → normalized_domain
  → launch_contact_hydration  (Waterfall: domain → company-LinkedIn → ICP person → Blitz email → MillionVerifier)
  → ops.email_resolutions  (verified email lands here)
  → enroll_leads_from_audience  (stamps email onto each lead)
```

**Resolution-rate reality (each stage strictly shrinks the set; none is measured for the sub cohort):**

| Funnel stage | Cardinality | Cite |
|---|---|---|
| Sub UEIs (full) | 25,450 | `contract_subaward v12` (distinct sub_uei, measured 2026-06-19) |
| `sam_master_domains` uei↔domain rows | 709,546 (46.0% of 1,540,965 distinct SAM UEI, both measured 2026-06-19) | `sam_master_domains` v6 / `sam_pocs` v162 |
| **Sub-specific domain-bridge rate** | requires the per-cohort JOIN in §6 item 2 — not yet run; do not quote a send-grade reach number until it is | — |
| `firmographics_blitz` (secondary uei→domain, 96.1% uei-populated) **[CORRECTED]** | 133,256 domains (128,016 with uei) | `materialize_blitz.py:30,84`; `FIRMOGRAPHICS_BLITZ_MATERIALIZATION_PLAN.md:24,134` |
| GTM `people` graph (domain-keyed, no UEI, no email) | ~7,739 rows — legacy island, almost never a sub | `companies_people_bulk.py:19-20` |
| `companies` graph | ~748 rows | `companies_people_bulk.py:19` |
| Verified emails on file | only contacts already hydrated by Waterfall/cascade | `ops.email_resolutions` |

**Hard facts that gate the bridge:**

- **A bare-UEI audience enrolls / hydrates NOTHING.** `corex._resolve_contacts` implements only `result_key ∈ {contact_id, company_id}` (`corex.py:488-518`); `recipient_uei` is an **advertised-but-dead enum** (`corex.py:377` vs the resolver). `launch_contact_hydration`'s Waterfall resolver **requires** the audience SQL to project `normalized_domain` (`blitz_hydration_waterfall.py:265-280`). The uei→domain JOIN must be baked into the audience SQL **first**.
- **`firmographics_blitz` is a usable secondary bridge, NOT empty.** **[CORRECTED]** — its `uei` is ~96.1% populated; the `uei:None` writes (`enrich_blitz.py:314,477`) go to `ops.task_runs`, not this dataset. Prefer `sam_master_domains` for **breadth** (5.3×) and **source authority** (SAM-declared `entity_url`), not because blitz is sparse. The Waterfall already LEFT-JOINs `firmographics_blitz` (`blitz_hydration_waterfall.py:280`), so a sub present there recovers its domain — and `company_linkedin_url` inline, collapsing one hop.
- **The Waterfall email leg is plan-gated and degrades silently.** `enrich_email` drops to people-only if the live Blitz plan does not unlock `/enrichment/email` (`blitz_hydration_waterfall.py:315-324`). All Blitz egress funnels through one `max_containers=1` gateway at **global ≤5 RPS** (`blitz_gateway.py:57,128`). Verify the plan unlock before promising emails.
- **The name+domain→email cascade (Icypeas→LeadMagic→MillionVerifier) is the one path that needs no LinkedIn — but is NOT agent-callable** (no gtm-mcp tool, no Trigger surface in `apps/gtm_mcp/src/`). It shares the `ops.email_resolutions` SoR, so a one-time manual `enrichment-email-cascade` backfill is inherited by every subsequent agent Waterfall launch via the verified-skip set — a viable out-of-band lever for the name+domain firms the agent cannot reach in-loop.

---

## 2. Signal & content substrate

### 2.1 The cardinal split — prime-solicitation-derived vs own-description-derived

Every sub signal originates on exactly one of two provenance sides, and this — not the dataset — sets its coverage ceiling:

- **SUB's OWN `subaward_description`** (public FSRS/USAspending, no CUI, egress-safe by construction — `classify_sub_self_reported_tags.py:15-17`). The **lead** signal. **Universal (25,449).** All semantic vectors and Path-B tags derive here.
- **PRIME's SOLICITATION** (SOW/PWS/SOO + attachments). Clearance, certs, the 11-type requirement enum, scope summaries, labor categories, scope-derived `capability_tags`. **Cannot** be derived from a sub's short descriptions; requires the prime's harvested solicitation reached through the three-hop bridge (`subaward.prime_award_unique_key → FPDS.generated_unique_award_id → solicitation_identifier → sam-gov-opps.notice_id → manifest resource_id`, `subawardee_solicitations.py:6-9`). **Bridge-only by construction.**

### 2.2 Complete signal taxonomy

| Signal | Source dataset:version | Grain | Universe | Provenance side | Status / live-confirm | gtm-mcp query path |
|---|---|---|---|---|---|---|
| Identity / $ / count / prime UEI / recency | `contract_subaward v12` | 1/sub | **FULL 25,449** | SUB-self | **LIVE-CONFIRMED** | `execute_audience_query` over `"usaspending_api_fresh/contract_subaward"` |
| Semantic scope vectors ("does work like X") | `govcon_sub_capability_vectors_90day v8` | (sub_uei, chunk_ix) | **FULL 25,449** (102,937 chunks, 0 NULL, IVF_PQ) | SUB-self | **LIVE-CONFIRMED** (the one independently-confirmed full-universe profile-adjacent axis) | `search_subawardee_capabilities(query, k)` |
| Captive-sub → new-prime diversification matches (ANN, NAICS-gated) | `captive_sub_diversification_90day v15` (Lance storage-format v2.1, snapshot-overwrite) | 1/(captive sub, candidate new prime) | **3,156 captive subs scored** · 1,541 new primes · 8,118 `naics2_aligned` of 28,965 rows (2,340 aligned subs) | SUB-self (centroid ANN over prime solicitations) | **LIVE-CONFIRMED** (prod run measured 2026-06-19; PRs #538/#539) | `execute_audience_query` over `captive_sub_diversification_90day` (filter `naics2_aligned`/`naics4_aligned`) |
| Self-reported tags (Path B, 77-vocab) | `govcon_sub_self_reported_tags v2` (66,275 rows) + profiles | 1/sub | **FULL 25,450 (13,792 tagged)** | SUB-self (Haiku-4.5 Batches on own desc, 600-char hash join) | **LIVE-CONFIRMED** (profiles v69, measured 2026-06-19) | `WHERE list_contains(self_reported_capability_tags,'…')` |
| `tag_source` provenance `{scope\|self_reported\|both\|none}` | profiles | 1/sub | **FULL 25,450** — {self_reported 11,353 / both 2,439 / scope 1,293 / none 10,365} | both | BITMAP-indexed (`build_…:75`); **LIVE-CONFIRMED** | `WHERE tag_source IN ('self_reported','both')` |
| NAICS / PSC trade (prime-award proxy) | `contract_subaward v12` / `subawardee_work_profile` | 1/sub | **FULL 25,449** | SUB (prime-award NAICS proxy) | LIVE (fact); work-profile raw-SQL only | `subaward_naics_codes[]`; `sub_top_naics` |
| Dollar trajectory + cadence | `contract_subaward v12` / `subawardee_work_profile` | 1/sub | **FULL 25,449** | SUB-self | LIVE | `total_subaward_amount, n_subawards, sub_action_date`; work-profile `recent_*_90d, sub_*_5y` |
| Teaming graph (named primes, $, count, recency) | `govcon_teaming_edges_90day v4` (115,366 edges) | (prime, sub) | **23,006 distinct sub** (5y window) | SUB (relationship) | LIVE | raw SQL only (no typed tool) |
| Award×sub targeting edges | `govcon_sub_targeting_90day v9` (165,974) | (award, sub) | award-scoped | mixed | LIVE | raw SQL / `govcon_companies_by_requirements`→join |
| PoP + HQ geo | profiles ← `contract_subaward` | 1/sub | **FULL 25,450** — `hq_state` 24,002 · `pop_state` 18,639 (measured 2026-06-19) | SUB-self | **LIVE-CONFIRMED** (profiles v69); BITMAP-indexed | `WHERE pop_state='VA' OR list_contains(pop_states,'VA')` |
| Scope-derived `capability_tags` (77-vocab) | profiles ← `govcon_doc_scope_90day` | 1/sub | **BRIDGE — 3,732 tagged** | PRIME-solicitation | LIVE (NULL outside bridge by construction) | `WHERE list_contains(capability_tags,'…')` |
| `scope_summary` (marked-free) | profiles ← `govcon_doc_scope_90day` | 1/sub | **BRIDGE** — `has_extracted_scope`=4,220 (flag); **3,784** non-null `scope_summary` (measured 2026-06-19) | PRIME-solicitation | LIVE | `SELECT scope_summary …` |
| Hard requirements (11-type enum) | `govcon_award_requirements_90day` (~193,845) | requirement → rolled to sub | **BRIDGE** | PRIME-solicitation | LIVE | `govcon_companies_by_requirements`; `govcon_requirement_facets` |
| Clearance | profiles | 1/sub | **BRIDGE — 2,497** (measured 2026-06-19) | PRIME-solicitation | LIVE; BITMAP | `WHERE requires_clearance AND req_clearance_level_max IN (…)` |
| Cert tags | profiles | 1/sub | **BRIDGE** | PRIME-solicitation | LIVE | `req_cert_tags[]` |
| Labor categories (36-trade lexicon) | profiles ← `govcon_award_requirements_90day` (NOT the orphaned `govcon_labor_demand_90day` sink) | 1/sub | **BRIDGE** | PRIME-solicitation | LIVE | `top_labor_categories[]` |
| Pricing | `govcon_pricing_90day v240` (156,117 chunks) | chunk | **BRIDGE — 0 consumed** | PRIME-solicitation | corpus only; **no sub column, zero consumers** | `search_govcon_scopes(header_class="pricing")` (ANN only) |
| SAM POC reach (name/title/geo) | profiles ← `sam_pocs` | 1/sub | **22,737** (measured 2026-06-19) | SUB (SAM-registration POC, **not marketing**) | LIVE | `poc_full_name/title/city/state` |

**Vocabulary anchors (independently confirmed counts):** capability-tag controlled vocabulary = **exactly 77 tags** (`classify_sub_self_reported_tags.py:49-71`); requirement enum = **exactly 11 types** (`certification, clearance, labor_category, standard_compliance, license, equipment_capability, past_performance, deliverable, insurance_bonding, staffing_constraint, vehicle_constraint`, `capability.py:63-67`); labor lexicon = **exactly 36 trades** (`sam_labor_demand_extract_90day.py:410-447`). `req_clearance_level_max` ordinal: `TS_SCI=5 > TOP_SECRET=4 > SECRET=3 > CONFIDENTIAL=2 > PUBLIC_TRUST=1` (`build_subawardee_capability_profiles.py:92`).

**The non-obvious provenance asymmetry (act on this):** the **77-tag vocabulary is identical** on both sides — Path B (own description) and scope-derived (prime's solicitation) classify into the **same enum**. `tag_source='both'` is therefore the strongest possible per-sub match: the sub said it **and** the prime's solicitation confirmed it. `tag_source` is the BITMAP-indexed discriminator that splits universal-semantic from bridge-structured in a single SQL predicate.

---

## 3. Surface-area map — possible NOW vs unlocks with X/Y/Z

### 3.1 Possible NOW (no build, live-confirmed)

- **Full-universe semantic targeting** — `search_subawardee_capabilities` over all 25,449 subs.
- **Full-universe structured targeting on the fact table** — identity/$/prime/NAICS/recency/velocity derivations via `execute_audience_query`.
- **Full-universe self-reported capability tags + geo + POC** — `self_reported_capability_tags`, `tag_source`, `hq_state`/`pop_state`, `poc_available` (22,737) over the **landed** profiles table (v69 · 25,450 rows). **[NOW LIVE — was the §3.2 #1 build]**
- **Teaming fan-out** (23,006 subs) and **award×sub edges** via raw SQL.
- **Bridge-structured conjunctions** (≤6,586) — clearance ∧ cert ∧ scope-derived capability_tag.
- **Named-person retrieval** per sub — `sam_pocs` (name/title/geo) via raw SQL or the catalyst `/entities/{uei}/subaward-profile` route; FFATA officers for threshold primes.
- **Live email manufacture** — once the audience SQL bakes in the uei→domain JOIN: `launch_contact_hydration → enroll_leads_from_audience`.

### 3.2 Leverage-ranked build options

| # | Capability | Status | The build | Blast radius | GTM payoff |
|---|---|---|---|---|---|
| ✅ | **Full-universe structured tags + geo** | **DONE — RUN LANDED** (#503/#504/#506; profiles v69 · 25,450 rows; sidecar `govcon_sub_self_reported_tags` v2 · 66,275 rows; 13,792 self-tagged; measured 2026-06-19) | The two sequential runs (`classify_sub_self_reported_tags submit→retrieve`, then `build_subawardee_capability_profiles build`) **have executed**. | Medium — frozen-schema overwrite + consumer redeploys, all completed | **Realized.** Flipped self-reported capability tags **and** HQ/PoP geo 6,586 → 25,450; catalyst route + raw SQL now serve them with zero further work |
| 2 | **Send-grade contact bridge for subs** | **NOT BUILT** (binding activation constraint) | Materialize `subawardee_uei → normalized_domain → people/email` as a durable dataset (vs per-campaign live hydration). Inputs (`sam_master_domains`, Waterfall) exist | Medium-high — new dataset + enrichment spend (≤5 RPS gateway) | **Highest on activation side.** Without it, every widened audience dead-ends at firm rows, no people |
| 3 | **Prime-recompete trigger fanned to sub bench** | **PARTIAL** — teaming fan-out live; per-sub expiry clock **not built** | Per-prime expiry clock on the fresh feed (no PoP-end column today) + `govcon_teaming_edges_90day` fan-out. Triggers hang off the **prime** (sub leg single-digit-thin from FFATA lag: T1=8, T2=3, T3=0) | Medium — needs a fresh-feed PoP-end column | GTM-native timing wedge: reach the bench in the prime-expiry → sub-reselection window |
| ✅ | **Captive-sub → new-prime diversification matchmaking** ("diversify off your one prime") | **SHIPPED & LIVE** (PRs #538/#539) — was the headline future motion this doc scoped | **DONE.** Dataset `captive_sub_diversification_90day` v15 (prod run measured 2026-06-19: 28,965 rows · 3,156 subs scored · 8,118 `naics2_aligned`/2,340 subs · 1,541 new primes · 1,862 awards); Modal worker `pipelines/serving/materialize_captive_diversification.py` (`run_build`, deployed + executed; ledger `status=success`), weekly `src/trigger/captive_diversification.ts` (Mon 11:00 UTC) **defined + merged to `main`, not yet registered via `npm run trigger:deploy`**. Spec: `docs/reference/CAPTIVE_SUB_DIVERSIFICATION_DATASET.md` | Medium — landed (frozen-schema snapshot-overwrite + ledger) | **Realized.** Full-universe ANN upgrade of the deterministic `capability_match` leg (169 → 2,340 aligned subs); NAICS-sector gate is the load-bearing usable filter (raw cosine → domain-wrong matches) |
| 4 | **Daily subaward append on Trigger** | **PLAN-ONLY** — `usaspending_api_subaward_fresh.ts` does not exist; last feed commit #318; feed ~4 days stale | Execute the existing plan (Modal `run_daily` + `.ts`). Port of the proven prime path | Low | Foundational cadence — unblocks every velocity/freshness trigger; stops audiences reading a stale feed |
| 5 | **Subaward velocity / new-prime-relationship derivation** | **NOT BUILT** (substrate present) | One derived dataset over `contract_subaward` (rolling $/count deltas; first-under-prime flag from teaming first/last date). Depends on #4 for freshness | Low | Momentum + new-relationship triggers |
| 6 | **Self-reported tags onto winners-map** (`winners.v4`) | **NOT BUILT** — sub leg projects scope `capability_tags` only, not `self_reported_*` (`materialize_winners_map.py:227`) | Ungated decoder field + serving column. Upstream dependency (full-universe self-reported tags) is now **satisfied** (profiles v69) | Medium — decoder bump both files + serving column | Lower — catalyst route + raw SQL already deliver the value |
| 7 | **Pricing as served signal** | **NOT BUILT** — `govcon_pricing_90day` indexed but **zero consumers** | Wire pricing into a per-sub/award price-band field | Medium | Lowest — sub-disjoint (priced on prime solicitations), weakest traceability |
| ⊘ | **Direct-mail channel (Lob)** | **REGISTERED BUT STUB — NOT a live channel** **[CORRECTED]** | `create_direct_mail_campaign`/`send_letter`/`send_postcard` are on `/healthz` but every one returns `{"status":"not_implemented"}` — "the Lob calls are not yet wired … no mail is produced" (`dmaas.py:6,9,22-26,46,71,96`). **Build = wire Lob fulfillment**, not "write an audience recipe" | Medium — new provider integration + Lob credentials | See §5 — the substrate (`sam_pocs` street address) exists; the channel does not |

**Build-option correction the activation lens got wrong [CORRECTED]:** the strategy lens asserted direct mail is "a registered, callable activation channel right now" and ranked a "wire the direct-mail lane" recipe at #2.5. **Live verification refutes the premise.** `dmaas.py` is a hard stub — the tools echo their inputs and return `not_implemented`; no mail is produced. The genuinely useful finding survives and is sharper: `sam_pocs` **does carry a full street address** (`address_line_1, address_line_2, city, zip5, zip4, state`, `sam_pocs.py:200`), so the **mailing-address substrate is on-shelf and UEI-keyed** — the gap is the **Lob fulfillment wiring**, not the audience. Direct mail remains the only contemplated channel that bypasses both the domain bridge and live email manufacture, but it is a real **build** (provider integration), not a recipe.

---

## 4. GTM targeting & positioning plays

### 4.1 Segments (universe + the tool/SQL that builds each)

| Play | Definition | Universe | Build (live today) | Provenance / caveat |
|---|---|---|---|---|
| **P1 — Prime-dependency-risk** (captive subs) ★ | `n_distinct_primes_subaward = 1` ∧ `n_subawards ≥ 3` ∧ `$ > 5e5` | Large long-tail subset of 25,449 | `execute_audience_query` over `contract_subaward` `GROUP BY subawardee_uei HAVING COUNT(DISTINCT prime_awardee_uei)=1` | FULL-universe, LIVE-confirmed; no bridge dependency. **The "diversify off your one prime" payload is now LIVE** (`captive_sub_diversification_90day`, PRs #538/#539): each captive sub carries candidate NEW primes via NAICS-gated ANN — 2,340 aligned subs · 1,541 new primes. Canonical query: `SELECT * FROM captive_sub_diversification_90day WHERE naics4_aligned AND award_action_date >= <cutoff>` |
| **P2 — Teaming-orphan** (strong capability, thin primes) | semantic match ∩ `n_teaming_primes ≤ 2` | vector slice ∩ ≤23,006 | `search_subawardee_capabilities` → filter `govcon_teaming_edges_90day` | FULL (vectors) ∩ teaming (5y) — **different windows; do not conflate cardinalities** |
| **P3 — Clearance-evidenced-but-small** | `requires_clearance` ∧ `req_clearance_level_max IN ('SECRET','TOP_SECRET','TS_SCI')` ∧ `$ < 2e6` | **≤2,497** (hard floor) | `execute_audience_query` over profiles | BRIDGE — frame as **"clearance-evidenced," never "all cleared"** |
| **P4 — Capability-cluster + rising velocity** | tag/vector membership ∩ positive rolling-window $ delta | FULL ∩ capability ∩ accel | velocity CTE over `contract_subaward.subaward_action_date` + `subaward_amount` | velocity reads a feed ~4 days stale (no Trigger cadence) — batch only |
| **P5 — Geographic concentration** | `pop_state='X'` OR `list_contains(pop_states,'X')` | **FULL** (`hq_state` 24,002 · `pop_state` 18,639 on the v69 profiles) | profiles (BITMAP) or fallback to `contract_subaward` PoP code | Path-B landing complete; cheap composable filter |
| **P6 — Recompete-window fan-out** | primes with expiring PoP → habitual subs | expiring-prime set × benches ⊆ 23,006 | teaming fan-out live; expiry clock must be assembled (not built) | hang off the **prime** — sub leg is single-digit-thin |
| **P7 — Large-sub-never-a-prime** (graduation) | `sub_amount_5y > 5e6` ∧ `prime_obligated_5y < 1e5` | FULL slice | `subawardee_work_profile` (raw SQL only) | FULL-universe; clean, defensible cohort |

**Stacking:** geo (BITMAP) ∧ `tag_source IN ('self_reported','both')` (BITMAP) are cheap composable predicates — stack on any play in one SQL. P1 ∩ P3 ∩ P5 = "captive cleared subs performing in Virginia" — extremely sharp, small, high-conversion.

### 4.2 Signal → message (citeable per-sub vs inferred)

| Signal | Open with | Citeable? | Egress |
|---|---|---|---|
| **Own `subaward_description` snippet** | *"On your subaward under [PRIME] you described the work as '[≤600-char verbatim]' — that's [tag] work. I track every requirement in that lane the day it posts."* | **CITEABLE, FULL (Path-B caveat)** | **SAFE** — public FSRS |
| Semantic capability cluster | *"Your subaward history clusters around [substation switchgear]; when a prime's SOW calls for exactly that, you're who I surface."* | CITEABLE as match, INFERRED as claim | SAFE |
| Teaming primes + $ (named) | *"You've run $[edge_dollars_5y] under [PRIME] across [edge_count_5y] awards since [first_action_date] — I see when [PRIME] bids the next one."* | **CITEABLE** (relationship fact) | SAFE |
| Clearance evidence | *"You sat under a [SECRET/TS-SCI] requirement on [PRIME]'s award — I surface the next cleared scope in your lane."* | **CITEABLE, BRIDGE ≤2,497** | SAFE (structured field) |
| `evidence_quote` (literal solicitation language) | *"The [PRIME] solicitation you performed under specified '[evidence_quote]' — I match you to every new requirement carrying that exact spec."* | **CITEABLE, BRIDGE** | **SAFE ONLY IF** `marked_resource=false` AND `validated=true` on the row — verify per-row before verbatim quote |
| `scope_summary` | *"The scope you performed under read: '[summary]'. That's recurring demand — here's who's about to re-solicit it."* | CITEABLE, BRIDGE | SAFE — marked-free by construction |
| NAICS/PSC | *"You've worked under [X]-coded awards"* — **never** *"your NAICS is X"* | INFERRED (prime-award proxy) | SAFE |
| SAM POC name | — | NAME citeable, value near-zero | **GUARDRAIL** — registration POC, never the outreach target |

**The single highest-leverage, lowest-risk opening:** quote the sub's **own** `subaward_description` back (full-universe, public, zero CUI), then pivot to "here's the next requirement in that exact lane" — leading with the prime's `evidence_quote` only for the bridge subs where the row is verified marked-free.

---

## 5. Sharp insights

1. **Registration-POC vs opportunity-POC is the whole contact story.** The SAM source the operator can reach from a sub UEI (entity-**registration** EB/GB POC) is the one with **no email**; the SAM source with email (the solicitation/opportunity POC, `sam_opps_archived_bulk`) is keyed to `notice_id` and is the **buyer's** contracting officer — it never attaches to the sub. There is no SAM path to a sub's marketing email. Email is a **manufactured artifact**, not a record.

2. **Universal-semantic vs bridge-structured asymmetry — sequence around it.** The sub's own words give you full-universe (25,449) semantic + 77-vocab tags; the prime's solicitation gives you the high-credibility hard signals (clearance, certs, requirement quotes) but only for ≤6,586. **Lead generation off the universal axis; lead *qualification/messaging* off the bridge axis.** Never let a "cleared subs" or "scope-tag" query masquerade as full-universe — the bridge is a structural floor (only 818/6,347 csub prime keys, **12.9%**, ever reached the bridge — `…DIAGNOSTIC.md:79`), not a coverage bug a rebuild fixes.

3. **`tag_source='both'` is the highest-conviction segment — 2,439 subs (measured 2026-06-19).** It is the only state where the sub's self-description and the prime's solicitation independently agree on a capability (the full `tag_source` split is {self_reported 11,353 / both 2,439 / scope 1,293 / none 10,365}). Lead with these subs in any capability-led campaign — convergence is evidence, not inference.

4. **Prime-dependency (P1) is the sharpest play and it needs zero bridge.** A single-prime captive sub knows its concentration risk; the pitch lands on a felt structural pain, and the whole segment is buildable on the live-confirmed fact table. It is also the cleanest trigger: fire when the lone prime enters a recompete window (P6). **The diversification payload is now SHIPPED** (`captive_sub_diversification_90day`, PRs #538/#539): the play no longer just names the risk — it serves each captive sub its NAICS-aligned candidate NEW primes (2,340 aligned subs · 1,541 new primes; the full-universe ANN upgrade of the deterministic `capability_match` leg that reached only 169 subs). The NAICS-sector gate (`naics2_aligned`/`naics4_aligned`) is load-bearing: raw cosine alone matched domain-wrong primes (e.g. an HR firm to highway construction); the aligned tier is the usable list.

5. **Teaming-orphan (P2) monetizes a capability/reach gap.** A sub whose vectors prove sophisticated work but whose teaming graph is thin is under-monetizing capability — peak receptivity to a channel that brings prime relationships.

6. **Prime-dependency triggers must hang off the prime, never the sub row.** FFATA reporting lag makes the sub leg of any USAspending trigger single-digit-thin (T1=8, T2=3, T3=0 over 90 days). Every velocity/recompete/new-relationship signal fans **prime → teaming edge → sub bench**.

7. **The Path-B run landed — it flipped the most.** The Path-B classifier + profiles rebuild (former §3.2 #1) was the single move that widened self-reported capability tags **and** HQ/PoP geo from 6,586 → 25,450, and it has executed: profiles `v69 · 25,450 rows`, sidecar `govcon_sub_self_reported_tags v2 · 66,275 rows`, **13,792** subs self-tagged (measured 2026-06-19 via `build_subawardee_capability_profiles.py verify`). The widening is real, not just a row count: `rows=25,450` alone is satisfiable by a rebuild without a populated sidecar (universe comes from `csub`, every untagged sub gets `tag_source='none'`), and the live measurement clears the stronger bar — `self_tagged=13,792 ≫ 6,586` — so the full-universe self-reported axis is genuinely populated, not merely padded.

8. **Direct mail is the only email-free channel — and it is stubbed.** **[CORRECTED]** The mailing-address substrate is on-shelf and UEI-keyed (`sam_pocs` full street address; HQ geo on profiles), but `dmaas.py` returns `not_implemented` — no mail is produced. Wiring Lob fulfillment is the build that would convert the no-website ~54% of subs (whom the email bridge structurally abandons) into a reachable cohort. Highest-value genuinely-new channel, but it is a provider integration, not a recipe.

---

## 6. Live-measured state (2026-06-19) + the one item still requiring a per-cohort probe

| # | Fact | Measured value (2026-06-19) | Probe |
|---|---|---|---|
| 1 | **Profiles universe (the former master gate — now RESOLVED).** | `govcon_subawardee_capability_profiles` **v69 · 25,450 rows** · `self_tagged`=**13,792** · `scope_tagged`=3,732. `self_tagged ≫ 6,586` confirms a genuinely populated full-universe self-reported axis. Full-universe for self_reported tags, `tag_source`, geo, POC; the fact table + vectors back the rest. | `build_subawardee_capability_profiles.py verify` (ran this build); or `execute_audience_query("SELECT COUNT(*) rows, COUNT(self_reported_capability_tags) self_tagged, COUNT(capability_tags) scope_tagged FROM govcon_subawardee_capability_profiles")` |
| 2 | **Sub-specific domain-bridge rate — the one number still requiring a per-cohort run.** The registry-wide ratio is `709,546 / 1,540,965 = 46.0%` (`sam_master_domains` rows / `sam_pocs` distinct UEI, both measured 2026-06-19); the **sub-cohort** rate is a different, unmeasured slice. | Registry-wide 46.0% measured; **sub-cohort rate not yet run** — gates any send-grade reach number, so do not quote a send-grade reach figure until this specific JOIN is executed. | `SELECT count(DISTINCT s.subawardee_uei) subs, count(DISTINCT d.uei) with_domain, round(100.0*count(DISTINCT d.uei)/count(DISTINCT s.subawardee_uei),1) pct FROM "usaspending_api_fresh/contract_subaward" s LEFT JOIN sam_master_domains d ON d.uei = s.subawardee_uei` |
| 3 | **Live tool surface.** | **62 tools, `status:"ok"`** (`/healthz`, 2026-06-19). The named tools `launch_contact_hydration`, `enroll_leads_from_audience`, `execute_audience_query`, `search_subawardee_capabilities` are all present. | `curl -s https://gtm-mcp-8pru.onrender.com/healthz` |
| 4 | **DMaaS stub status.** | Tools registered on `/healthz`; runtime = `not_implemented` (`dmaas.py:6,22-26`). Direct mail is substrate-ready, channel-stubbed. | `create_direct_mail_campaign(...)` → `status == "not_implemented"` |
| 5 | **Teaming vs fact window.** | Teaming = 5y → `govcon_teaming_edges_90day` v4, **23,006** distinct sub_uei; semantic/fact = `contract_subaward` v12, **25,450** distinct sub_uei. Distinct cardinalities — do not conflate when joining teaming dollars onto a profile. | Compare `COUNT(DISTINCT sub_uei)` across both before joining |