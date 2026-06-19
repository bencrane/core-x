# Subawardee GTM Audience — Readiness Diagnostic

**Question:** *"As of right now, via the gtm-agent using the gtm-mcp, can I build an audience of 'subawardees that X or Y' for GTM targeting?"*

**Date:** 2026-06-19 · **Method:** read-only recon over canonical source (`apps/gtm_mcp/`, `pipelines/{usaspending,sam_gov,serving}/`) plus a full re-measurement of every quantitative claim **live against the R2 SoR + gtm-mcp `/healthz` + hq-x Postgres on 2026-06-19** (`lance.dataset(...).count_rows()/version`, DuckDB distinct counts, `build_subawardee_capability_profiles.py verify`, and the `ops.captive_diversification_serving_runs` ledger). **No data mutated.**

> **2026-06-19 accuracy pass.** Every figure in this document is now a value measured live this run. The previously-open Path-B item is **resolved**: `govcon_subawardee_capability_profiles` is at the **full universe — v69, 25,450 rows** (verified), with **13,792 subs carrying self-reported capability tags** and a populated `tag_source` distribution. The structured capability-tag axis is therefore **full-universe** for the self-reported tags; only the *prime-solicitation-derived* attributes (scope-derived `capability_tags`, clearance, certs, labor) remain bridge-scoped by construction. The former §7.1 open item is now resolved and removed.

---

## Update — 2026-06-19 (diversification surface shipped; all figures re-measured live)

- **New live queryable dataset: `captive_sub_diversification_90day`** (`s3://data-sink/active/captive_sub_diversification_90day/`, Lance storage-format v2.1, snapshot-overwrite; live dataset-version probe **v15**). One row per (captive sub, candidate NEW prime); auto-discoverable by the gtm-agent via `execute_audience_query (FROM captive_sub_diversification_90day)`. It is the ANN, full-universe upgrade of `govcon_sub_targeting_90day`'s deterministic `capability_match` leg (which reached only 169 subs). Live production run (rolling 365-day window, measured 2026-06-19): **28,965 rows · 3,156 distinct captive subs scored · 1,541 distinct new primes · 1,862 distinct matchable awards · avg cosine 0.7215**; usable `naics2_aligned=true` tier = **8,118 rows / 2,340 subs**, `naics4_aligned` = **2,973 rows / 1,341 subs**.
- **Standing surface (PRs #538 + #539):** `pipelines/serving/materialize_captive_diversification.py` (Modal app `captive-diversification`, fn `run_build` — deployed and executed; `ops.captive_diversification_serving_runs` latest row `status=success`, `completed_at=2026-06-19 15:35 UTC`); `src/trigger/captive_diversification.ts` weekly schedule (Mon 11:00 UTC) is **defined and merged to `main` but not yet registered with Trigger.dev** (`npm run trigger:deploy` requires a `TRIGGER_ACCESS_TOKEN`). Dataset is live and rebuildable on demand now.
- **Award-side ceiling (live probe 2026-06-19):** `govcon_scope_vectors_90day` (v286) holds 1,481,167 chunks but only **4,988 DISTINCT award keys** → matchable-award ceiling = 4,988 of **1,247,391** distinct prime awards (`contract_prime_txn` v22 distinct `contract_award_unique_key`) = **0.40%** (the structural reason the award side is bridge-bound). The all-dates validation run's ANN hit 2,298 of those; the live 365-day production window resolved **1,862**.
- **Unchanged:** contact/email reality (no email at UEI grain; send-grade email is the 5-hop hydration bridge — the diversification dataset identifies firms, not contacts) and the clearance/cert/labor axis (prime-solicitation-derived, bridge-only by construction).
- **Companion spec:** `docs/reference/CAPTIVE_SUB_DIVERSIFICATION_DATASET.md`.

---

## 0. Verdict (read first)

**YES — the capability is live end-to-end today, with one axis-dependent coverage boundary you must respect.**

- **Tool surface: LIVE.** The Render gateway `gtm-mcp-8pru.onrender.com` answers `200` and registers all **62** tools (live `/healthz` count, 2026-06-19) including the entire subawardee surface (`search_subawardee_capabilities`, `execute_audience_query`, `govcon_companies_by_requirements`, `search_govcon_scopes`) and the full activation loop (`create_initiative → define_audience → create_campaign → enroll_leads_from_audience → record_send`).
- **Data substrate: LIVE and fresh.** Every load-bearing subawardee dataset is committed, populated, and indexed in the R2 SoR (`s3://data-sink/active/`). Versions/counts in §3 (all re-measured 2026-06-19).
- **Expressiveness depends on the predicate axis:**
  - **Full universe (25,450 subs)** for: name/identity, subaward `$`, subaward count, prime-contractor relationship, NAICS family, recency, **semantic "does work like X"** (vector ANN), **self-reported controlled-vocab capability tags** (Path B — 13,792 subs tagged of the 25,450-row profiles table), `tag_source`, HQ/PoP geo, and SAM `poc_available` (22,737 of 25,450).
  - **Bridge subset (6,586 subs)** for the *prime-solicitation-derived* attributes only: scope-derived `capability_tags` (3,732 tagged), `requires_clearance` (2,497), certs, and labor categories. These are bridge-only **by construction** — they derive from harvested prime solicitations, not from a sub's own short descriptions.
- **"X or Y" composition is trivial** — `execute_audience_query` is arbitrary read-only ANSI SQL (`WHERE a OR b`), and structured ⋃ semantic results can be unioned.

**The honest answer to give the operator:** *Yes, you can build it now.* If "X or Y" is spend/NAICS/prime/recency/semantic-capability/**self-reported-tag**/geo/POC → you get the **full 25,450-sub universe**. If "X or Y" includes **clearance, certs, labor, or scope-derived capability tags**, that filter only sees the **6,586 bridge subs** — absence from that subset means *no harvested prime solicitation*, not *no capability*.

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
- **Live status (2026-06-19):** `GET /healthz` → `200 {"service":"gtm-mcp","status":"ok", tools:[…62…]}`.
- **Landmine (pre-existing, documented):** a *stale* `agents.yaml` in legacy `hq-all` pins `gtm` → `gtm-mcp.up.railway.app` (dead). Live is the Render URL. Do **not** run `managed_agents/reconcile.py` until that manifest's `gtm` url is corrected, or it clobbers the live binding (`~/Desktop/hq/plans/2026-06-05-edge-api-migration-plan.md:20,136,285`).

---

## 2. The audience-build surface (live, from `/healthz` 2026-06-19 — 62 tools)

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

All figures are live `count_rows()` / `version` / index probes measured 2026-06-19 against the R2 SoR; dataset versions cited inline.

| Dataset (registry name) | Grain | Live state (measured 2026-06-19) | Indexes | Powers |
|---|---|---|---|---|
| `usaspending_api_fresh/contract_subaward` | sub×prime-award | **v12 · 199,901 rows · 25,450 distinct sub_uei · 25,449 with description** | BTREE | The fact table — full-universe `$`/count/prime/NAICS/recency filters |
| `govcon_sub_capability_vectors_90day` | (sub_uei, desc_chunk) | **v8 · 102,937 chunks · 25,449 subs · 0 NULL embeddings · IVF_PQ LIVE** | IVF_PQ(embedding 1024-d) + BTREE | `search_subawardee_capabilities` (full-universe semantic recall) |
| `govcon_subawardee_capability_profiles` | 1/sub_uei | **v69 · 25,450 rows (full universe)** · 13,792 `self_reported_capability_tags` · `tag_source` {self_reported 11,353 / both 2,439 / scope 1,293 / none 10,365} · 22,737 `poc_available` · 24,002 `hq_state` · 18,639 `pop_state` · 4,220 `has_extracted_scope` · 3,732 scope `capability_tags` (bridge) · 2,497 `requires_clearance` (bridge) · 10 indices live · `schema_assert=PASS` | BTREE + BITMAP×10 | Structured capability/clearance/geo/POC filter table — **self-reported tags + geo + POC are now full-universe; scope-derived tags + clearance remain bridge** |
| `govcon_sub_self_reported_tags` | (desc hash) | **v2 · 66,275 rows** (the Path-B sidecar — Haiku-4.5 classifications of each sub's own `subaward_description`, hash-joined into profiles) | BTREE(desc_sha) | Source of the full-universe `self_reported_capability_tags` axis |
| `govcon_sub_targeting_90day` | (award, candidate_sub_uei) | **v9 · 165,974 rows** (direct_subaward / teaming_history / capability_match) | BTREE(award, sub, prime) | Award×sub outreach edges ("subs reachable under primes that need A∧B∧C") |
| `captive_sub_diversification_90day` | (captive sub, candidate new prime) | **v15 · 28,965 rows · 3,156 captive subs · 1,541 new primes · 1,862 matchable awards · 8,118 `naics2_aligned` (2,340 subs) · 2,973 `naics4_aligned` (1,341 subs)** (rolling 365-day prod run, measured 2026-06-19) | BTREE(sub_uei, cand_prime_uei, award_key, award_action_date) + BITMAP(naics2_aligned, naics4_aligned) | Captive-sub → NEW-prime diversification audiences (ANN full-universe upgrade of the `capability_match` leg) — spec: `CAPTIVE_SUB_DIVERSIFICATION_DATASET.md` |
| `govcon_teaming_edges_90day` | (prime, sub) | **v4 · 115,366 rows · 23,006 distinct sub_uei** (5y, source `usaspending/subaward_search` 9.8M) | BTREE(prime_uei, sub_uei) | Teaming habituality |
| `govcon_award_requirements_90day` | requirement row | **v13845 · 193,845 rows** (validated extraction) | BTREE(resource_id) etc. | `govcon_companies_by_requirements` / `_requirement_facets` |
| `govcon_scope_vectors_90day` / `govcon_unknown_90day` / `govcon_pricing_90day` | chunk | **v286 / v303 / v240 · 1,481,167 / 1,310,223 / 156,117** — scope+unknown embedded, IVF_PQ fully indexed (unknown reindex closed 2026-06-16, PR #491); pricing corpus has no scalar/vector index | IVF_PQ + scalars | `search_govcon_scopes` (prime solicitation text) |

**Coverage truth (the load-bearing nuance):**
- **Semantic vectors are universal** — every one of the 25,449 subs with a `subaward_description` is embedded and ANN-searchable.
- **Self-reported tags, geo, and POC are now full-universe** — the profiles table covers all **25,450** subs (v69); the Path-B self-reported capability tags reach **13,792** subs, `poc_available` reaches **22,737**, `hq_state` reaches **24,002**.
- **Scope-derived capability/clearance is bridge-scoped** — the *prime-solicitation-derived* fields (scope `capability_tags` 3,732, `requires_clearance` 2,497, certs, labor) cover only the **6,586** subs whose prime award resolved through the harvest bridge (`subaward.prime_award_unique_key → FPDS → solicitation → sam-gov-opps → manifest`). Only **818 of 6,347** csub prime keys are even *in* the bridge (12.9%); the cap is the harvest, not the build. Clearance/cert/labor cannot be derived from a sub's own descriptions — they need the prime's solicitation, so they are bridge-only by design.

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
| Self-reported capability tags (Path B, own description) | ✅ | SQL over `…capability_profiles.self_reported_capability_tags` / `tag_source` | 25,450 (13,792 tagged) | profiles v69 — full universe, measured 2026-06-19 |
| Scope-derived `capability_tags` (prime solicitation) | ⚠ subset | SQL over `…capability_profiles.capability_tags` | 6,586 (3,732 tagged) | profiles v69; bridge by construction |
| Holds a clearance / level | ⚠ subset | `requires_clearance` / `req_clearance_level_max` | 6,586 (2,497) | bridge-only by construction |
| HQ / place-of-performance geo | ✅ | `hq_state/city`, `pop_state/pop_states` | 25,450 (24,002 hq_state / 18,639 pop_state) | profiles v69 — full universe |
| SAM POC reachable | ✅ | `poc_available` | 25,450 (22,737) | = SAM registration POC, not marketing contact (§7.3) |
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

## 7. Gaps, caveats, and structural boundaries

1. **Profiles universe is full (settled 2026-06-19).** Path B is **landed**: `govcon_subawardee_capability_profiles` v69 = **25,450 rows**, with **13,792** subs carrying `self_reported_capability_tags` and `tag_source` distributed {self_reported 11,353 / both 2,439 / scope 1,293 / none 10,365} (measured this run via `build_subawardee_capability_profiles.py verify`). The self-reported capability-tag axis, geo, and POC are therefore full-universe. `tag_source='both'` (**2,439** subs) is the highest-conviction segment — the sub said it *and* the prime's solicitation confirmed it.
2. **Clearance/cert/labor are structurally bridge-only.** A query "subs that hold SECRET" sees ≤2,497 subs (measured `requires_clearance=2,497`), not the universe — and that is correct, not a bug: clearance is asserted by the *prime's* solicitation, harvested for only the bridge subset. Frame such audiences as "clearance-evidenced subs," never "all cleared subs." The scope-derived `capability_tags` (3,732 tagged) are bridge-only for the same reason; the *self-reported* tags (item 1) are the full-universe complement.
3. **`poc_available` ≠ marketing contact.** It is full-universe now (**22,737** of 25,450), but it means the sub's UEI is in `sam_pocs` (a SAM-registration POC: name/title/geo — **no email or phone**; `sam_pocs` schema carries `address_line_1/2, city, zip5, zip4, country, state` and zero contact-channel columns, measured 2026-06-19). For genuine GTM outreach (cold email / direct mail), run `launch_contact_hydration` (Waterfall ICP) or `enrich_companies` to resolve firm → people. The send-grade contact bridge (`subawardee_uei → sam_master_domains → domain → people/email`) is detailed in the companion contact-surface diagnostic.
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
- `list_datasets()` → confirm `govcon_subawardee_capability_profiles`, `govcon_sub_capability_vectors_90day`, `govcon_sub_targeting_90day`, `usaspending_api_fresh/contract_subaward`, `captive_sub_diversification_90day` are registered.
- `describe_dataset("govcon_subawardee_capability_profiles")` → confirm columns + the full-universe self-reported axis (`self_reported_capability_tags`, `tag_source`).
- `search_subawardee_capabilities("substation switchgear install", k=10)` → expect `status:"ok"` with ranked subs (not `dataset_unavailable`/`vector_index_absent`).
- Universe spot-check (matches the 2026-06-19 measurement: rows=25,450, self_tagged=13,792, scope_tagged=3,732):
  ```
  execute_audience_query(
    "SELECT COUNT(*) AS rows,
            COUNT(self_reported_capability_tags) AS self_tagged,
            COUNT(capability_tags) AS scope_tagged
     FROM govcon_subawardee_capability_profiles")
  ```

---

## 9. Evidence appendix

| Source | What it establishes | Date |
|---|---|---|
| `GET gtm-mcp-8pru.onrender.com/healthz` | Service up `200`; **62 tools** incl. full subawardee + activation surface | 2026-06-19 (live probe) |
| `build_subawardee_capability_profiles.py verify` | Profiles **v69 = 25,450 rows (full universe)**; 13,792 self-reported tags; tag_source {self_reported 11,353 / both 2,439 / scope 1,293 / none 10,365}; 2,497 clearance; 22,737 POC; schema_assert PASS | 2026-06-19 (live verify) |
| `lance.dataset(...).count_rows()/version` over every cited dataset + DuckDB distinct counts | All §3 row/version/distinct figures (incl. scope ceiling 4,988 / prime universe 1,247,391 = 0.40%; captive v15 = 28,965) | 2026-06-19 (live R2 probes) |
| `ops.captive_diversification_serving_runs` (hq-x Postgres) | Latest terminal row: 28,965 rows / 8,118 naics2 / 2,340 subs / 1,541 primes / 0.7215 cosine / `status=success` / `completed_at=2026-06-19 15:35 UTC` | 2026-06-19 (live ledger) |
| `apps/gtm_mcp/main.py`, `src/database.py`, `src/tools/{audience,sub_capability,capability}.py` | Gateway architecture, raw-SQL ceiling, dataset bindings, degradation paths | repo `main` |
| `pipelines/serving/materialize_sub_targeting.py`, `pipelines/sam_gov/govcon_gtm_schemas.py` | Sub-targeting build + frozen subawardee schemas (columns/indices) | repo `main` |
| `docs/reference/SUBAWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md` | The three-hop bridge + harvest bottleneck that scopes scope-derived tags/clearance to the bridge subset | (read-only feasibility) |
| `~/Desktop/hq/plans/2026-06-05-edge-api-migration-plan.md` | Live gtm-mcp URL = Render `gtm-mcp-8pru`; stale `agents.yaml` Railway URL landmine | 2026-06-05 |

**Bottom line:** the subawardee GTM audience capability is real, live, and fresh today (all figures re-measured 2026-06-19). Sell it as *full-universe (25,450) for spend/relationship/NAICS/recency/semantic/self-reported-capability-tags/geo/POC*, and *evidence-scoped (6,586 bridge subs) for clearance, certs, labor, and scope-derived capability tags*. The Path-B full-universe profiles are confirmed landed (v69); `tag_source='both'` (2,439) is the highest-conviction capability segment.
