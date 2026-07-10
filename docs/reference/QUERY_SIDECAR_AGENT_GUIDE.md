# Query-Sidecar — Agent Navigation Map

**Read this before scanning Lance.** A warm, read-only DuckDB endpoint serves the GTM analytical
substrate — ~1.23B rows across 61 sorted tables — in milliseconds-to-seconds per SQL statement.
If your question is answerable from the tables below, USE THIS. Do not open Lance datasets, do
not register Lance into DuckDB, do not scan `usaspending_fpds_canonical_txn` (392 cols, 108M
rows) for a question `gtm_txn_events_slim` answers in 50 ms.

Provenance: built by [pipelines/query_sidecar/build_query_sidecar.py](../../pipelines/query_sidecar/build_query_sidecar.py)
from the frozen manifest ([SIDECAR_PHASE0_MART_MANIFEST.md](../plans/SIDECAR_PHASE0_MART_MANIFEST.md));
program record + runbook: [QUERY_SIDECAR_PROGRAM.md](../plans/QUERY_SIDECAR_PROGRAM.md).

---

## 1. Connect (copy-paste)

```bash
TOKEN=$(doppler secrets get QUERY_SIDECAR_TOKEN -p core-x -c prd --plain)
curl -s -X POST https://query-sidecar-api.onrender.com/api/v1/sql \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"sql": "SELECT count(*) FROM gtm_txn_events_slim WHERE uei = '\''ABC123DEF456'\''", "limit": 1000}'
```

- `POST /api/v1/sql` `{"sql", "limit"?, "require_artifact"?}` — ONE statement,
  `SELECT`/`WITH`/`DESCRIBE`/`SHOW` only. Default limit 1000, max 50000, 120 s timeout,
  rows returned as `{columns, rows, elapsed_ms, artifact, instance}`.
  **Multi-statement analyses that must reconcile to the dollar:** pin
  `require_artifact` to the first response's `artifact`; a 409 means the snapshot
  moved mid-session — re-run the batch against the new stamp instead of mixing states.
- `GET /api/v1/tables` (bearer) — live table catalog: every table with source dataset, tier,
  sort key, pinned Lance version, row count.
- `DESCRIBE <table>` (via the sql endpoint) — full column list for any table. **Introspect
  instead of guessing column names.**
- `GET /healthz` (no auth) — `artifact` key = the snapshot stamp you are reading.

## 2. Decision rule — sidecar vs Lance

| Question | Where |
|---|---|
| GTM analytics: entities, awards, transactions-by-recipient, teaming, lanes, capabilities, expiry, people/POC lookups | **Sidecar** |
| Per-ACTION description text (`transaction_description`), canonical txn columns beyond `txn_rows`' 16, the full 392-col canonical, `gtm_subaward_recipient_code_evidence` | Lance (not in artifact). Award-grain descriptions ARE here: `award_descriptions` |
| Enrichment identity coverage: PDL match, LinkedIn URLs, icypeas profiles (see §3 Identity/enrichment) | **Sidecar** |
| Non-GTM domains (EPA, CMS, MSHA, FDIC, SoS, UCC…) | Lance (not in artifact) |
| Ingest verification / anything needing LIVE data | Lance — the sidecar is a snapshot (see §6) |

## 3. Table catalog (grain · rows · sorted by)

**Join key almost everywhere: `uei` (12-char SAM identifier).**

### Entity spine
| Table | Grain · rows | Sorted | Load-bearing columns |
|---|---|---|---|
| `gtm_sam_entities` | 1/uei · 2.0M | uei | legal_business_name, physical_state/city/zip, primary_naics, in_dsbs, sam_is_active, normalized_domain, cage_code, business_types |
| `gtm_entity_behavior_rollup` | 1/uei · 262k (only entities with award behavior) | uei | prime_obl_12/24/36/60mo/lifetime, prime_award_ct_*, active_award_ct, active_obl, pop_expiring_180d_ct, sub_amt_24/60mo/lifetime, sub_ct_lifetime, is_prime_24mo, is_sub_60mo, prime_and_sub, top_naics, top_agency_code, last_action_date |
| `gtm_entity_geo` | 1/uei · 1.5M | uei | latitude, longitude, geo_precision (HQ, not place of performance) |

### Capability lanes (verb doctrine: demonstrated vs inferred)
| Table | Grain · rows | Sorted | Semantics |
|---|---|---|---|
| `gtm_entity_code_lanes` | 1/(uei, side, code_type, code) · 1.7M | uei, code | DEMONSTRATED: side='prime' (primed in) or 'sub' (subbed under); code_type 'naics'\|'psc'; obl_12/24/60mo/lifetime, action_ct |
| `gtm_prime_code_signature` | 1/(uei, code_type, code), prime side only · 1.18M | uei, code_type, rank_lifetime | The allocation/signature primitive: per-firm ranked prime record — `rank_24mo`, `rank_lifetime`, `share_24mo`, `share_lifetime` precomputed per (uei, code_type); floors/top-N applied at query time (`WHERE rank_lifetime <= 5 AND share_lifetime >= 0.05`). Never re-derive ranks with window functions over code lanes |
| `gtm_entity_inferred_primeable_codes` | 1/(uei, code_type, code) · 263M | code_type, code | INFERRED could-prime (cooccurrence evidence). Filter by code first — that's the sort |
| `gtm_entity_inferred_subbable_codes` | 1/(uei, code_type, code) · 160M | code_type, code | mirror, could-sub |

### Award/transaction facts
| Table | Grain · rows | Sorted | Notes |
|---|---|---|---|
| `gtm_txn_events_slim` | 1/FPDS action · 108M | uei, action_date | Columns: uei, action_date, action_type_code (A–Y mod events), subcontracting_plan, naics_code, psc_code, awarding_agency_code, **obligation** (≠ federal_action_obligation), action_key, award_key |
| `usaspending_fpds_prime_award_state` | 1/contract_award_unique_key · 83M | current_end_date | 52 cols: award_topology, recipient_uei/name, life_to_date_obligated, current_end_date (expiry queries prune HARD on this), naics/psc, agency, PIIDs, **window state** (own `ordering_period_end_date` + resolved-parent `parent_ordering_period_end_date`/`parent_current_end_date`/`parent_potential_end_date`) **and parent attribution** (`parent_awarding_agency_code`/`parent_awarding_sub_agency_code` = whose vehicle, `parent_idv_type_code`/`parent_award_type_code` = what instrument, `parent_type_of_set_aside_code`). Parent cols populated only when `parent_match_flag='resolved'`; 'self'/'dangling' rows stay NULL. Position/active ladders and "agency behind the parent instrument" are ONE pass, never a self-join. DESCRIBE it |
| `award_ordering_windows` | 1/award with an ordering window · 982k | contract_award_unique_key | `ordering_period_end_date` (latest-action `arg_max`) + `latest_action_date` — IDV/vehicle ordering-window universe ("which vehicles' windows close in N days") |
| `gtm_position_orders` | 1/order with OPEN window (as of build date) · ~17M | contract_award_unique_key | The position-ladder substrate: contract_award_unique_key, recipient_uei, parent_award_key_resolved, `window_end` (own-else-parent ordering end). **Position rungs join ring keys to THIS, never to the 83M award_state** — any 83M-side join saturates the 2-thread serving box (measured 17–22s) |
| `subaward_canonical_slim` | 1/subaward · 1.3M | prime_awardee_uei | 38 cols incl. `subaward_description`, `prime_award_base_transaction_description`, `subawardee_business_types` (designation flags); `subaward_amount` is VARCHAR — use `subaward_amount_num` |
| `subaward_canonical_slim_by_sub` | same rows | subawardee_uei | second copy, sub-side clustering |
| `gtm_open_awards` | 1/open award · 163k | recipient_uei | active-PoP/open-IDV universe, centroid geo pre-joined |
| `txn_rows` | 1/FPDS action · 108M | action_date | The 16-col wire contract with CANONICAL names (recipient_name, award_id_piid, action_type_description, subcontracting_plan_desc, federal_action_obligation, base_and_all_options_value, awarding_agency_name…) — use when you need names/descriptions per action; `gtm_txn_events_slim` for uei-first aggregation |
| `usaspending_award_pop_centroids` | 1/award PoP centroid · 30.7M | state_code, zip5 | Place-of-performance lat/lon per award (zip5→ZCTA). Ad-hoc geo: bounding-box prefilter on state/zip5 (the sort), then haversine; joins awards on generated_unique_award_id |

### The combo-portrait layer (industry × work × time × geo × agency × sub-out, zoomable)

| Table | Grain · rows | Sorted | Semantics |
|---|---|---|---|
| `txn_events_combo` | 1/FPDS action · 108M | naics_code, psc_code, action_date | **THE portrait fact.** Every dial as a column: `fy` (federal FY precomputed), `action_type_code`, `subcontracting_plan`, `award_topology` (task orders = 'vehicle_order'), `award_type_code`, `pop_state`, `pop_county_fips`, `pop_county_name`, **`pop_country_code`** (ISO3 — splits the no-US-state bucket into overseas vs unstated; names via `country_vocab`), **`type_of_set_aside_code`** (the set-aside dial), `awarding_agency_code`, `awarding_sub_agency_code`, **`funding_agency_code`, `funding_sub_agency_code`** (who pays vs who signs — `funding_agency_code <> awarding_agency_code` is the split; names via `agency_vocab`/`agency_sub_vocab`), `obligation`, `uei`, `award_key`. Zoom = `substr()`: NAICS3/4/6 via `substr(naics_code,1,n)`, PSC letter via `substr(psc_code,1,1)`, family = `substr(naics_code,1,4)||'x'||substr(psc_code,1,1)` |
| `txn_events_combo_by_geo` | same rows | pop_state, pop_county_fips, action_date | Second copy — **state/county-anchored** questions prune here |
| `award_subout_rollup` | 1/prime award with subs · ~1M | prime_award_unique_key | `sub_ct`, `distinct_subs`, `sub_amount_total`, first/last sub date. Join on `award_key` → "is this work getting subbed out" |
| `agency_sub_vocab` | 1/sub-agency code | code | code → majority name (agency trends display) |
| `award_descriptions` | 1/award · 30.7M | recipient_uei | Award requirement `description` + `solicitation_identifier`/`solicitation_date` (PDF-handoff join keys) + PIID + both award keys. **History tabs:** a UEI's awards + descriptions (or the glaring lack) = one pruned read. Sub-side: `subaward_canonical_slim.subaward_description` AND the prime's `prime_award_base_transaction_description` on the same row |
| `award_plan_state` | 1/award · ~40M | contract_award_unique_key | Latest-action `subcontracting_plan` per award (`latest_plan`, `latest_action_date`, `actions`) — award-grain plan state for arbitrary/closed populations, one pruned join |
| `v_combo_fy` / `v_family_fy` / `v_award_subout` | views | — | Baked portrait queries: combo×FY measures (prime $, plan-attached share, task-order share); family grain; award×sub-out join |
| `v_psc_names` / `v_naics_names` / `v_sam_declared_codes` | views | — | Vintage-safe reference names (active-else-latest, 1 row/code); SAM declarations unnested to (uei, is_active, code_type, code) |

### Rollups & expiry
| Table | Grain · rows | Sorted |
|---|---|---|
| `gtm_txn_recipient_month_rollup` | uei × action_type × plan_class × month · 34M | uei |
| `gtm_award_recipient_rollup` | uei × naics × psc × agency × topology · 6.3M | uei |
| `gtm_award_expiry_months` | uei × end_month · 221k | uei, end_month |
| `gtm_prime_pop_lanes` | 1/(uei, pop_state, county) · 547k | uei |

### Teaming / relationship substrate
| Table | Grain · rows | Sorted |
|---|---|---|
| `gtm_prime_sub_pairs` / `gtm_prime_sub_pairs_by_sub` | 1/(prime_uei, sub_uei) · 269k | prime_uei / sub_uei |
| `gtm_prime_combo_lanes` | 1/(uei, naics, psc) · 5.1M | uei |
| `gtm_sub_combo_lanes` | 1/(uei, naics, psc) · 339k | uei |
| `gtm_prime_farmout_combo_lanes` · `gtm_prime_vehicle_lanes` | 38k · 16k | uei |
| `gtm_prime_demand_events` | event/prime uei (24mo) · 11.3M | uei |
| `gtm_primes_by_recipient_code` | 1/(code_type, code) marginal · 1.7M | recipient_code |
| `gtm_prime_subout_by_recipient_code` | prime × context_code cube · 11.8M | prime_awardee_uei |
| `gtm_subbed_under_to_primed_in_cooccurrence` | code × code matrix · 589k | subbed_under_code |
| `gtm_sub_profiles` · `govcon_subawardee_profiles` | 1/sub uei · 105k · 25k | uei / sub_uei |
| `gtm_sub_universe_pairs` / `_targets` | pair-grain recipe precompute · 30k | target_uei |

### Identity / enrichment layer (gap-pass-4: "does population X have coverage" in one statement)
| Table | Grain · rows | Sorted | Semantics |
|---|---|---|---|
| `bridge_sam_pdl` | 1/uei matched · 802k | uei | SAM↔PDL identity bridge: uei, duns, pdl_company_id, normalized_domain — THE coverage join for "what % of this firm set has a PDL match" |
| `pdl_normalized_companies` | 1/pdl_company_id · 35.4M | pdl_company_id | Canonical PDL company: names, normalized_domain, **linkedin_slug** (the company LinkedIn URL), locality/region/country, industry, employee_size_range, year_founded. Hydrate matches via the bridge; domain-anchored matching joins `normalized_domain` (unsorted — seconds-class scan) |
| `icypeas_company_scrapes` / `icypeas_dsbs_company_profiles` | 1/uei scraped · 6.6k / 5.8k | uei | Scraped company LinkedIn profiles (URL, headcount, industry, description); raw blobs excluded |
| `icypeas_person_profiles` / `icypeas_person_profile_scrapes` | 14.8k / 9.5k | uei / person_linkedin_url_norm | Person-profile scrape ledger (status/found per input) and the scraped profile content (title, summary, company block, education); raw blobs excluded |
| `bridge_dsbs_pdl_linkedin` | 1/uei · 53k | uei | DSBS→PDL/LinkedIn resolution (best_domain + matched pdl id + company_linkedin_url) |
| `dsbs_poc_linkedin` · `exa_person_linkedin_candidates` | 821 · 33k | uei | Person-side LinkedIn resolution candidates (raw JSON excluded) |

### People / identity / reference
| Table | Grain · rows | Sorted |
|---|---|---|
| `gtm_sam_people` | 1/(uei, name_key) · 2.3M | uei |
| `gtm_sam_person_contactability` | 1/sam_person_id · 152k | sam_person_id |
| `sam_pocs` | 1/(uei, role, slot) · 8.1M | uei |
| `sam_master_entities` | 1/uei SAM registration master · 1.5M | uei | ⚠ declaration columns: use the LIST columns `naics_codes`/`psc_codes` (VARCHAR[]) — or `v_sam_declared_codes`, which unnests them. The `*_counter`/`*_string` near-duplicates are raw ingest artifacts; counting on them produced a retracted figure (84% vs the true ~21% PSC declaration rate). Measured semantics: NAICS declarations near-universal among registrants, PSC sparse/optional |
| `people_canonical` | 1/canonical_person_id · 132k | canonical_person_id |
| `firmographics_blitz` | 1/domain · 255k | domain_norm |
| `federal_sites_lance` | 1/federal site · 300k | state_code, zip5 |
| `naics_reference` · `psc_reference` · `gtm_naics_psc_pairs` · `agency_vocab` · `country_vocab` | code refs · 2.1k/6.1k/321k/75/~250 | code | ⚠ vintages: both reference tables carry multiple `source_vintage` rows per code; `psc_reference WHERE is_active` returns NULL names for retired-vintage codes that still carry award dollars. **Display names: join `v_psc_names` / `v_naics_names`** (active-else-latest-vintage, one row per code) |
| `_sidecar_manifest` · `_sidecar_meta` | provenance: per-table pinned Lance version, build stamp | — |

## 4. Query patterns (proven shapes)

```sql
-- Entity point profile (ms)
SELECT * FROM gtm_entity_behavior_rollup WHERE uei = 'XXX';

-- "Companies that X and Y" = INTERSECT legs on uei, then hydrate
WITH f AS (
  SELECT DISTINCT uei FROM gtm_entity_code_lanes
   WHERE side='prime' AND code_type='naics' AND code='236220'
  INTERSECT
  SELECT uei FROM gtm_entity_behavior_rollup WHERE prime_obl_lifetime >= 1e7
)
SELECT e.legal_business_name, r.prime_obl_lifetime
FROM f JOIN gtm_sam_entities e USING(uei) JOIN gtm_entity_behavior_rollup r USING(uei)
ORDER BY r.prime_obl_lifetime DESC LIMIT 100;

-- Event collapse: "recipients who got a code-G mod in 90d", $-ranked
SELECT uei, count(*) n, sum(obligation) amt
FROM gtm_txn_events_slim
WHERE action_type_code='G' AND action_date >= current_date - 90
GROUP BY uei ORDER BY amt DESC LIMIT 50;

-- Expiring award universe (prunes on the sort key)
SELECT recipient_uei, recipient_name, life_to_date_obligated, current_end_date
FROM usaspending_fpds_prime_award_state
WHERE award_topology IN ('standalone','vehicle_order')
  AND current_end_date BETWEEN current_date AND current_date + 90;

-- Lookalikes: never-primed firms with inferred capability (filter code FIRST — it's the sort)
SELECT i.uei FROM gtm_entity_inferred_primeable_codes i
JOIN gtm_entity_behavior_rollup r USING(uei)
WHERE i.code_type='naics' AND i.code='541330'
  AND r.sub_ct_lifetime > 0 AND r.prime_award_ct_lifetime = 0;

-- Teaming: who subs under this prime
SELECT p.sub_uei, e.legal_business_name FROM gtm_prime_sub_pairs p
JOIN gtm_sam_entities e ON e.uei = p.sub_uei WHERE p.prime_uei = 'XXX';

-- COMBO PORTRAIT: zoom out (family × FY, national) — one view
SELECT * FROM v_family_fy WHERE family = '5413xJ' ORDER BY fy;

-- Zoom in (exact combo × county × FY, with the event/plan dials)
SELECT fy, pop_county_name, sum(obligation) obl,
       avg((subcontracting_plan IN ('C','D','E','F','G','H'))::INT) plan_share,
       avg((award_topology = 'vehicle_order')::INT) task_order_share
FROM txn_events_combo_by_geo
WHERE pop_state = 'VA' AND naics_code = '541330' AND psc_code LIKE 'J%'
GROUP BY 1, 2 ORDER BY 1, obl DESC;

-- Sub-out trend: is rising prime work in a category being subbed out more?
SELECT c.fy, sum(c.obligation) prime_obl,
       sum(s.sub_amount_total) FILTER (WHERE s.prime_award_unique_key IS NOT NULL) subbed_amt,
       count(DISTINCT c.award_key) awards,
       count(DISTINCT s.prime_award_unique_key) subbed_awards
FROM txn_events_combo c
LEFT JOIN award_subout_rollup s ON s.prime_award_unique_key = c.award_key
WHERE c.naics_code LIKE '5413%' AND c.psc_code LIKE 'J%'
GROUP BY 1 ORDER BY 1;

-- POSITION LADDER (agency-lens): firms with an IDV seat whose ordering window is
-- open at snapshot, carrying <agency> orders in a PSC ring — ONE pass, no self-join.
-- NOTE: Navy '1700' is a SUB-agency code (top-tier DoD = '097'); ring on
-- awarding_sub_agency_code. Join gtm_position_orders (17M narrow, open-window
-- only), NOT the 83M award_state — big-side joins saturate the serving box.
SELECT count(DISTINCT p.recipient_uei)
FROM txn_events_combo c
JOIN gtm_position_orders p ON p.contract_award_unique_key = c.award_key
WHERE c.awarding_sub_agency_code = '1700'
  AND (substr(c.psc_code,1,1) IN ('J','N') OR substr(c.psc_code,1,2) = '42');  -- the ring

-- Overseas vs unstated: name the no-US-state bucket (geo map legend / market share)
SELECT coalesce(v.name, 'UNSTATED') AS country,
       count(DISTINCT c.award_key) awards, sum(c.obligation) obl
FROM txn_events_combo c
LEFT JOIN country_vocab v ON v.code = c.pop_country_code
WHERE c.pop_state IS NULL AND c.awarding_agency_code = '1700'
GROUP BY 1 ORDER BY obl DESC;

-- Signature allocation seed: each receiver's top-5 prime PSC lanes with shares —
-- ranks are PRECOMPUTED; floors/top-N are query-time dials
SELECT uei, code, share_24mo, v.psc_name
FROM gtm_prime_code_signature s
LEFT JOIN v_psc_names v ON v.psc_code = s.code
WHERE s.uei IN (/* receiver set */) AND s.code_type = 'psc'
  AND s.rank_24mo <= 5 AND s.obl_24mo > 0;

-- Declaration coverage of a firm set (never count the *_counter/*_string columns)
SELECT code_type, count(DISTINCT uei) declaring_firms
FROM v_sam_declared_codes WHERE uei IN (/* set */) GROUP BY 1;

-- Enrichment coverage funnel: PDL / LinkedIn / profile coverage of a firm set, one pass
SELECT count(*) firms,
       count(b.pdl_company_id)                       pdl_matched,
       count(p.linkedin_slug)                        with_linkedin,
       count(ic.uei)                                 icypeas_scraped
FROM (SELECT DISTINCT uei FROM gtm_sam_entities WHERE /* population */) f
LEFT JOIN bridge_sam_pdl b USING(uei)
LEFT JOIN pdl_normalized_companies p ON p.pdl_company_id = b.pdl_company_id
LEFT JOIN icypeas_company_scrapes ic USING(uei);
```

## 5. Performance model

Tables are physically clustered by their sort key — filter on it and DuckDB reads only matching
row groups (a `uei=` probe on the 108M-row table is ~ms). Filters off the sort key still work
(full-column scan, seconds-class on the giants). First touch of a cold table pays Render disk
page-cache (one-time seconds); repeats are fast. Queries serialize — keep single statements
tight; you have `elapsed_ms` in every response.

## 6. Caveats

1. **Snapshot, not live.** `/healthz` → `artifact` stamp; `_sidecar_meta`/`_sidecar_manifest`
   carry the build time and per-table pinned Lance versions. Rebuild:
   `modal run pipelines/query_sidecar/build_query_sidecar.py::run` (auto-refreshes serving).
   Serving instances converge on the newest artifact within ~60 s of a publish (LATEST
   poll); across that window, cross-statement totals can mix states — pin
   `require_artifact` (§1) when numbers must reconcile.
2. **Read-only by construction** — write/DDL statements are rejected; don't try.
3. **Not everything is here** (§2). If a needed table/column is absent, say so rather than
   silently falling back to a spine scan — absence is signal for the next manifest revision.
4. `gtm_txn_events_slim` renames: `obligation` (not federal_action_obligation), `psc_code`
   (not product_or_service_code), `uei` (not recipient_uei).

## 7. Gap reporting (demand capture — MANDATORY when you fall back)

Whenever you answer a data question WITHOUT the sidecar (Lance scan, pylance probe,
catalyst endpoint, DuckDB-over-Lance, or a degraded answer), append an entry AS YOU GO to
`docs/sidecar_gaps/SIDECAR_GAP_REPORT_<YYYY-MM-DD>-<short-slug>.md` (one file per
session/topic; header = date + `/healthz` artifact stamp + topic). Five fields, exact:

1. **Intent** — the question in plain English, as asked.
2. **Why not the sidecar** — `missing table` / `missing column(s)` / `wrong grain` /
   `missing sort (too slow unpruned)` / `freshness required` / `didn't know it was there`
   — plus the specific dataset/columns involved.
3. **What I ran instead** — exact query/scan, dataset hit, ONLY the columns actually needed.
4. **Cost** — wall time; rows scanned vs returned.
5. **Recurrence** — one-off vs recurring shape (honest).

Footer: rank gaps by recurrence × cost. Report demand only — no solutions. The promotion
cycle gates every entry (promote / routing fix / correctly-on-Lance) and archives the file
to `docs/sidecar_gaps/processed/` with a Disposition. This is how the artifact grows —
silent fallbacks are lost demand signal.
