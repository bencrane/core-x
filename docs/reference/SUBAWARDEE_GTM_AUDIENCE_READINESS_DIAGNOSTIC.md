# Subawardee GTM Audience — Readiness Diagnostic

**Question:** *"As of right now, via the gtm-agent using the gtm-mcp, can I build an audience of 'subawardees that X or Y' for GTM targeting?"*

**Date:** 2026-06-18 · **Method:** read-only recon over canonical source (`apps/gtm_mcp/`, `pipelines/{usaspending,sam_gov,serving}/`), live `/healthz` probe of the deployed Render gateway, and the on-disk ground-truth run records / scoping plans (live R2 probes dated 2026-06-15/16). **No data mutated.**

---

## Update — 2026-06-19 (diversification surface shipped)

- **New live queryable dataset: `captive_sub_diversification_90day`** (`s3://data-sink/active/captive_sub_diversification_90day/`, Lance v2.1, snapshot-overwrite). One row per (captive sub, candidate NEW prime); auto-discoverable by the gtm-agent via `execute_audience_query (FROM captive_sub_diversification_90day)`. It is the ANN, full-universe upgrade of `govcon_sub_targeting_90day`'s deterministic `capability_match` leg (which reached only 169 subs). Latest production run (rolling 365-day window, 2026-06-19): **28,965 rows · 8,118 NAICS-sector-aligned (the usable `naics2_aligned=true` tier) · 2,340 captive subs · 1,541 distinct new primes · 1,862 matchable awards · avg cosine 0.7215**.
- **Standing surface (PRs #538 + #539):** `pipelines/serving/materialize_captive_diversification.py` (Modal app `captive-diversification`, fn `run_build` — deployed and executed once); `ops.captive_diversification_serving_runs` ledger (first terminal row, `status=success`); `src/trigger/captive_diversification.ts` weekly schedule (Mon 11:00 UTC, **not yet activated** — awaits `npm run trigger:deploy`). Dataset is live and rebuildable on demand now.
- **Award-side ceiling corrected (authoritative live probe 2026-06-19):** `govcon_scope_vectors_90day` holds 1,481,167 chunks but only **4,988 DISTINCT award keys** → matchable-award ceiling = 4,988 of 1,247,391 distinct prime awards = **0.40%** (the structural reason the award side is bridge-bound). The captive run's ANN hit 2,298 of those, windowed to 1,862 within 365 days.
- **Unchanged:** contact/email reality (no email at UEI grain; send-grade email is the 5-hop hydration bridge — the diversification dataset identifies firms, not contacts), the bridge-scoped clearance/tag axis, and the Path-B profiles-universe-widening confirm item (§7.1) all still stand.
- **Companion spec:** `docs/reference/CAPTIVE_SUB_DIVERSIFICATION_DATASET.md`.

---

## 0. Verdict (read first)

**YES — the capability is live end-to-end today, with one axis-dependent coverage boundary you must respect.**

- **Tool surface: LIVE.** The Render gateway `gtm-mcp-8pru.onrender.com` answers `200` and registers all 52 tools including the entire subawardee surface (`search_subawardee_capabilities`, `execute_audience_query`, `govcon_companies_by_requirements`, `search_govcon_scopes`) and the full activation loop (`create_initiative → define_audience → create_campaign → enroll_leads_from_audience → record_send`).
- **Data substrate: LIVE and fresh.** Every load-bearing subawardee dataset is committed, populated, and indexed in the R2 SoR (`s3://data-sink/active/`). Versions/counts in §3.
- **Expressiveness depends on the predicate axis:**
  - **Full universe (25,449 subs)** for: name/identity, subaward `$`, subaward count, prime-contractor relationship, NAICS family, recency, and **semantic "does work like X"** (vector ANN).
  - **Bridge subset (6,586 subs)** for the *structured* capability axis: controlled-vocab `capability_tags` (3,732 tagged), `requires_clearance` (2,497), SAM `poc_available` (6,103), and denormalized geo. Clearance/cert/labor are bridge-only **by construction** (they derive from harvested prime solicitations, not from a sub's own short descriptions).
- **"X or Y" composition is trivial** — `execute_audience_query` is arbitrary read-only ANSI SQL (`WHERE a OR b`), and structured ⋃ semantic results can be unioned.

**The honest answer to give the operator:** *Yes, you can build it now.* If "X or Y" is spend/NAICS/prime/recency/semantic-capability → you get the **full 25,449-sub universe**. If "X or Y" includes **clearance or controlled-vocab capability tags**, that filter only sees the **6,586 bridge subs** — absence from that subset means *no harvested prime solicitation*, not *no capability*. One item to confirm live before quoting universe size on a tag query (§7, item 1).

---

## 1. Topology — what "gtm-agent using gtm-mcp" actually is

```
gtm-agent (Anthropic Managed Agent, Anthropic infra)
   │  agents.yaml pins MCP server "gtm" → https://gtm-mcp-8pru.onrender.com/mcp
   │  (Streamable HTTP, bearer-gated; provisioned via ~/managed-agents-x)
   ▼
gtm-mcp  (Render Web Service "gtm-mcp", Ohio/us-east-2)
   │  apps/gtm_mcp/main.py — FastMCP, stateless_http, /mcp + /sse, /healthz open
   │  one shared DuckDB conn · runtime-discovered Lance registry · hq-x Postgres ATTACH
   ▼
Lance system-of-record  (Cloudflare R2, s3://data-sink/active/, ~100+ datasets auto-discovered)
   + hq-x control-plane Postgres (attached as `hqx` for ops.* joins + audience persistence)
```

- This **Claude Code session is NOT wired to gtm-mcp** — only `blitz-api` is registered in `~/.claude.json`. The gateway was characterized from canonical source + the public `/healthz`, not by calling it. (Giving this session direct gtm-mcp access is a separate, future step.)
- **Live status (2026-06-18):** `GET /healthz` → `200 {"service":"gtm-mcp","status":"ok", tools:[…52…]}`.
- **Landmine (pre-existing, documented):** a *stale* `agents.yaml` in legacy `hq-all` pins `gtm` → `gtm-mcp.up.railway.app` (dead). Live is the Render URL. Do **not** run `managed_agents/reconcile.py` until that manifest's `gtm` url is corrected, or it clobbers the live binding (`~/Desktop/hq/plans/2026-06-05-edge-api-migration-plan.md:20,136,285`).

---

## 2. The audience-build surface (live, from `/healthz` 2026-06-18)

| Group | Live tools | Role for a subawardee audience |
|---|---|---|
| **Subawardee / GovCon** | `search_subawardee_capabilities`, `search_govcon_scopes`, `govcon_companies_by_requirements`, `govcon_requirement_facets` | Semantic sub recall + prime-requirement conjunction (the sub-bench-under-primes-that-need-A∧B∧C path) |
| **Audience / raw SQL** | `execute_audience_query`, `save_campaign_audience`, `lookup_awards_by_uei`, `search_company_by_{domain,name}`, `search_people_by_domain` | The escape hatch: arbitrary read-only SQL over the whole Lance plane; 1000-row cap |
| **Catalog** | `list_datasets`, `describe_dataset`, `refresh_catalog` | Discover dataset names + columns before composing SQL |
| **Federal** | `federal_entities_by_filter`, `search_entity_by_{name,uei}`, `federal_spend_by_{agency,industry,state}` | Entity resolution + spend aggregation over `entity_profile_gold` |
| **Activation (corex)** | `create_initiative`, `define_audience`, `define_audience_pair`, `create_campaign`, `enroll_leads_from_audience`, `draft_copy`, `materialize_supply_campaign`, `record_send`, `update_status`, `get_initiative`, `get_campaign_funnel`, `get_lead`, `list_campaigns` | Persist the audience and drive it into campaign → lead → send |
| **Enrichment / research** | `launch_contact_hydration`, `enrich_companies`, `define_enrichment_spec`, `deep_research`, `web_search` | Turn firm-grade rows into contact-grade leads (Waterfall/Parallel) |
| **Ops introspection** | `list_postgres_tables`, `get_postgres_schema` | Inspect `hqx.ops.*` for suppression/exclusion joins |
| **DMaaS (STUBS)** | `create_direct_mail_campaign`, `send_letter`, `send_postcard`, `get_fulfillment_status` | Direct-mail wrappers — validate + echo `not_implemented` (`tools/dmaas.py`) |
| Provider 360 (healthcare) | `find_*`, `get_*`, `extract_practice_eins_for_matching` | **Not subawardee** — NPI/practice-group targeting, listed for completeness |

**Architectural keystone:** the audience ceiling is *not* a fixed filter menu. `execute_audience_query(sql)` (`apps/gtm_mcp/src/tools/audience.py:176`) runs arbitrary ANSI SQL over every committed dataset (JIT-bound, so a 2-table join opens 2 manifests not 100), plus `hqx.*` Postgres joins. Any subawardee attribute that exists as a *column* is therefore filterable, AND/OR-composable, and joinable — bounded only by the 1000-row result cap and the read-only guard (`database.py:431` `assert_read_only`).

---

## 3. Subawardee data substrate — live state (R2 SoR)

All figures are live `count_rows()` / index probes from the dated run records below; dataset versions cited inline.

| Dataset (registry name) | Grain | Live state | Indexes | Powers |
|---|---|---|---|---|
| `usaspending_api_fresh/contract_subaward` | sub×prime-award | **v12 · 199,901 rows · 25,450 distinct sub_uei · 25,449 with description** | BTREE | The fact table — full-universe `$`/count/prime/NAICS/recency filters |
| `govcon_sub_capability_vectors_90day` | (sub_uei, desc_chunk) | **v8 · 102,937 chunks · 25,449 subs · 0 NULL embeddings · IVF_PQ LIVE** | IVF_PQ(embedding 1024-d) + BTREE | `search_subawardee_capabilities` (full-universe semantic recall) |
| `govcon_subawardee_capability_profiles` | 1/sub_uei | **v49 · 6,586 rows** · 4,220 `has_extracted_scope` · 3,732 `capability_tags` · 2,497 `requires_clearance` · 6,103 `poc_available` · 6,586 teaming · all 7 indices live | BTREE×7 | Structured capability/clearance/geo/POC filter table (bridge subset) |
| `govcon_sub_targeting_90day` | (award, candidate_sub_uei) | **v9 · 165,974 rows** (direct_subaward / teaming_history / capability_match) | BTREE(award, sub, prime) | Award×sub outreach edges ("subs reachable under primes that need A∧B∧C") |
| `captive_sub_diversification_90day` | (captive sub, candidate new prime) | **v2.1 · 28,965 rows · 8,118 `naics2_aligned` · 2,340 subs · 1,541 new primes** (rolling 365-day, prod run 2026-06-19) | BTREE(sub_uei, cand_prime_uei, award_key, award_action_date) + BITMAP(naics2_aligned, naics4_aligned) | Captive-sub → NEW-prime diversification audiences (ANN full-universe upgrade of the `capability_match` leg) — spec: `CAPTIVE_SUB_DIVERSIFICATION_DATASET.md` |
| `govcon_teaming_edges_90day` | (prime, sub) | **v4 · 115,366 rows · 23,006 distinct sub_uei** (5y, source `usaspending/subaward_search` 9.8M) | — | Teaming habituality |
| `govcon_award_requirements_90day` | requirement row | **193,845 rows** (validated extraction) | BTREE(resource_id) etc. | `govcon_companies_by_requirements` / `_requirement_facets` |
| `govcon_scope_vectors_90day` / `govcon_unknown_90day` / `govcon_pricing_90day` | chunk | 1,481,167 / 1,310,223 / 156,117 — **all embedded, IVF_PQ fully indexed** (unknown reindex closed 2026-06-16, PR #491) | IVF_PQ + scalars | `search_govcon_scopes` (prime solicitation text) |

**Coverage truth (the load-bearing nuance):**
- **Semantic vectors are universal** — every one of the 25,449 subs with a `subaward_description` is embedded and ANN-searchable.
- **Structured capability/clearance is bridge-scoped** — the profiles table covers the **6,586** subs whose prime award resolved through the harvest bridge (`subaward.prime_award_unique_key → FPDS → solicitation → sam-gov-opps → manifest`). Only **818 of 6,347** csub prime keys are even *in* the bridge (12.9%); the cap is the harvest, not the build. Clearance/cert/labor cannot be derived from a sub's own descriptions — they need the prime's solicitation, so they are bridge-only by design.

---

## 4. Expressiveness matrix — "subawardees that X"

| Predicate "X" | Expressible? | Path / tool | Universe | Evidence |
|---|---|---|---|---|
| Name / identity | ✅ | `execute_audience_query` over `contract_subaward` | 25,449 | fact table |
| Total / per-subaward `$` | ✅ | SQL agg over `contract_subaward.subaward_amount` | 25,449 | `sub_capability._enrich_identity` |
| # of subawards / # distinct primes | ✅ | SQL agg | 25,449 | same |
| Subs *for* a given prime (UEI) | ✅ | `contract_subaward.prime_awardee_uei` / `prime_award_unique_key` | 25,449 | fact |
| Teaming habituality with a prime (5y) | ✅ | `govcon_teaming_edges_90day` | 23,006 | teaming v4 |
| NAICS family / trade | ✅* | `prime_award_naics_code` (fact) / `sub_top_naics` (profiles) | 25,449* | *NAICS is the prime award's, not the sub's registered code |
| Recency (subaward action date) | ✅ | `contract_subaward.subaward_action_date` | 25,449 | fact |
| "Does work semantically like X" | ✅ | `search_subawardee_capabilities("…")` (cosine ANN) | 25,449 | vectors v8, IVF_PQ live |
| Controlled-vocab `capability_tags` | ⚠ subset | SQL over `…capability_profiles.capability_tags` | 6,586 (3,732 tagged) | profiles v49 |
| Self-reported tags (Path B) | ⚠ confirm | `self_reported_capability_tags` / `tag_source` | likely full — **confirm live** (§7.1) | code reads it; widening plan recommended it |
| Holds a clearance / level | ⚠ subset | `requires_clearance` / `req_clearance_level_max` | 6,586 (2,497) | bridge-only by construction |
| HQ / place-of-performance geo | ⚠ subset | `hq_state/city`, `pop_state/pop_states` | 6,586 | profiles (denormalized) |
| SAM POC reachable | ⚠ subset | `poc_available` | 6,586 (6,103) | = SAM registration POC, not marketing contact (§7.3) |
| Sub bench under primes needing A∧B∧C | ✅ | `govcon_companies_by_requirements` → `govcon_sub_targeting_90day` | prime-requirement scoped | capability.py + sub_targeting v9 |

`*` NAICS caveat: the fact carries the *prime award's* NAICS, a strong-but-imperfect proxy for the sub's own trade; `sub_top_naics` in the profiles is the per-sub mode.

---

## 5. "X or Y" + activation path

**Compose (preview):**
```sql
-- via execute_audience_query — full-universe spend/NAICS OR semantic-seeded UEI set
SELECT subawardee_uei, subawardee_name,
       SUM(TRY_CAST(subaward_amount AS DOUBLE)) AS total_sub_$,
       COUNT(*) AS n_subawards
FROM "usaspending_api_fresh/contract_subaward"
WHERE substr(prime_award_naics_code,1,4) = '2371'        -- X: utility-line construction
   OR subawardee_uei IN (/* Y: UEIs from search_subawardee_capabilities('substation switchgear') */)
GROUP BY 1,2
HAVING total_sub_$ > 500000
ORDER BY total_sub_$ DESC;     -- capped at 1000 rows
```

**Persist + activate (corex loop):**
1. `define_audience(name, source_sql)` — registers the audience in the corex schema (the SQL is stored as data, executed later; this is also the path past the 1000-row preview cap for large audiences).
2. `save_campaign_audience(campaign_id, …)` — the audited upsert into `hqx.ops.campaign_audiences` (the **only** write path into hq-x).
3. `create_initiative` → `create_campaign` → `enroll_leads_from_audience` → `draft_copy` / `materialize_supply_campaign` → `record_send`.
4. Subtract suppression in the same query via the attached Postgres: `LEFT JOIN hqx.ops.exclusions e ON … WHERE e.* IS NULL`.

---

## 6. What's genuinely solid

- The **semantic recall leg is the strongest piece** — full 25,449-sub coverage, 0 NULL embeddings, IVF_PQ live, `nprobes` tuned (24, not the default), distinct-sub collapse + identity/profile enrichment built in. "Find subs whose past work means X" works today across the whole universe.
- The **fact-grade structured filters** (`$`, count, prime, NAICS, recency) are full-universe and BTREE-backed.
- The **activation loop is complete and live** — this is not a query toy; an audience becomes a tracked campaign with leads and sends.
- Freshness is **recent and verified** — the subaward scope-enrichment lift completed 2026-06-15/16 with all 5 PRs merged, idempotency held, prime ledger verdicts byte-identical, and the one open index item (govcon_unknown reindex) closed 2026-06-16.

---

## 7. Gaps, caveats, and confirm-before-quoting items

1. **[CONFIRM LIVE — STILL OPEN] Profiles universe — bridge (6,586) vs Path B full (≈25,449).** `tools/sub_capability.py` reads `self_reported_capability_tags`/`tag_source` and comments "now ~full universe," but the 2026-06-16 `SUBAWARDEE_PROFILE_UNIVERSE_WIDENING_PLAN` records profiles still at **6,586** with Path B "decision-ready, NOT yet built." This item is **still unresolved** — Path B has not been confirmed landed. (Two adjacent facts *are* now confirmed by the 2026-06-19 diversification work and are no longer open: the sub-capability **vectors are full-universe** — 25,449 subs, IVF_PQ live — and the **scope-corpus matchable-award ceiling is now measured at 4,988 distinct award keys / 0.40%**. Neither widens the *profiles* table; the Path-B question below stands.) **Resolve in one call** before quoting a universe size on a *tag* query:
   ```
   execute_audience_query(
     "SELECT COUNT(*) AS rows,
             COUNT(self_reported_capability_tags) AS self_tagged,
             COUNT(capability_tags) AS scope_tagged
      FROM govcon_subawardee_capability_profiles")
   ```
2. **Clearance/cert/labor are structurally bridge-only.** A query "subs that hold SECRET" sees ≤2,497 subs, not the universe — and that is correct, not a bug: clearance is asserted by the *prime's* solicitation, harvested for only the bridge subset. Frame such audiences as "clearance-evidenced subs," never "all cleared subs."
3. **`poc_available` ≠ marketing contact.** It means the sub's UEI is in `sam_pocs` (a SAM-registration POC: name/title/often gov-facing email). For genuine GTM outreach (cold email / direct mail), run `launch_contact_hydration` (Waterfall ICP) or `enrich_companies` to resolve firm → people. Whether subawardee UEIs are bridged into the `companies`/`people` GTM datasets is the **next dependency to verify** for any send-grade subawardee campaign.
4. **1000-row preview cap** on `execute_audience_query`. For audiences larger than 1000, drive through `define_audience(source_sql)` (stored, re-executed at enroll time) rather than paginating raw rows.
5. **DMaaS is stubbed** — direct-mail fulfillment tools return `not_implemented`. Email/Waterfall paths are the live activation channels.
6. **NAICS proxy** (§4) and **geo bridge-scope** (§4) — minor, noted for precision.
7. **Stale `agents.yaml` URL** (§1) — operational landmine for any `reconcile.py` run; not a runtime defect of the live agent.

---

## 8. Confirmation probes (run against the live gateway to re-verify today)

```bash
# liveness + live tool list (no auth)
curl -s https://gtm-mcp-8pru.onrender.com/healthz | jq '.status, .tools'
```
Then, as the gtm-agent (bearer-gated `/mcp`):
- `list_datasets()` → confirm `govcon_subawardee_capability_profiles`, `govcon_sub_capability_vectors_90day`, `govcon_sub_targeting_90day`, `usaspending_api_fresh/contract_subaward` are registered.
- `describe_dataset("govcon_subawardee_capability_profiles")` → confirm columns + (§7.1) self-reported axis.
- `search_subawardee_capabilities("substation switchgear install", k=10)` → expect `status:"ok"` with ranked subs (not `dataset_unavailable`/`vector_index_absent`).
- `execute_audience_query` with the §7.1 count query → settle the universe question.

---

## 9. Evidence appendix

| Source | What it establishes | Date |
|---|---|---|
| `GET gtm-mcp-8pru.onrender.com/healthz` | Service up `200`; 52 tools incl. full subawardee + activation surface | 2026-06-18 (this recon) |
| `apps/gtm_mcp/main.py`, `src/database.py`, `src/tools/{audience,sub_capability,capability}.py` | Gateway architecture, raw-SQL ceiling, dataset bindings, degradation paths | repo `main` |
| `pipelines/serving/materialize_sub_targeting.py`, `pipelines/sam_gov/govcon_gtm_schemas.py` | Sub-targeting build + frozen subawardee schemas (columns/indices) | repo `main` |
| `docs/plans/SUBAWARD_SCOPE_ENRICHMENT_RUN_RECORD.md` | Profiles v49 = 6,586 (4,220 scoped); chunk sinks embedded+indexed; lift complete, PRs #478–482/#491 merged | exec 2026-06-15, finalized 2026-06-16 |
| `docs/plans/SUBAWARDEE_PROFILE_UNIVERSE_WIDENING_PLAN.md` | Vectors v8 = 102,937 chunks/25,449 subs/IVF_PQ live; Path A exhausted; Path B = 25,449 ceiling | 2026-06-16 (live probes) |
| `docs/plans/SUBAWARDEE_CAPABILITY_BUILDOUT_PLAN.md` | Sub spine "80% built"; profiles indices live; sub_targeting v9 = 165,974; teaming v4 = 115,366 | 2026-06-16 (live probes) |
| `docs/reference/SUBAWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md` | The three-hop bridge + harvest bottleneck that scopes structured tags to the bridge subset | (read-only feasibility) |
| `~/Desktop/hq/plans/2026-06-05-edge-api-migration-plan.md` | Live gtm-mcp URL = Render `gtm-mcp-8pru`; stale `agents.yaml` Railway URL landmine | 2026-06-05 |

**Bottom line:** the subawardee GTM audience capability is real, live, and fresh today. Sell it as *full-universe for spend/relationship/NAICS/recency/semantic*, and *evidence-scoped (6,586 bridge subs) for clearance & controlled-vocab capability tags* — and verify the one Path-B universe question (§7.1) before quoting a tag-query size.
