# Query-Sidecar — Agent Navigation Map

**Read this before scanning Lance.** A warm, read-only DuckDB endpoint serves the GTM analytical
substrate — ~708M rows across 40 sorted tables — in milliseconds-to-seconds per SQL statement.
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

- `POST /api/v1/sql` `{"sql", "limit"?}` — ONE statement, `SELECT`/`WITH`/`DESCRIBE`/`SHOW` only.
  Default limit 1000, max 50000, 120 s timeout, rows returned as `{columns, rows, elapsed_ms}`.
- `GET /api/v1/tables` (bearer) — live table catalog: every table with source dataset, tier,
  sort key, pinned Lance version, row count.
- `DESCRIBE <table>` (via the sql endpoint) — full column list for any table. **Introspect
  instead of guessing column names.**
- `GET /healthz` (no auth) — `artifact` key = the snapshot stamp you are reading.

## 2. Decision rule — sidecar vs Lance

| Question | Where |
|---|---|
| GTM analytics: entities, awards, transactions-by-recipient, teaming, lanes, capabilities, expiry, people/POC lookups | **Sidecar** |
| Full transaction row detail (descriptions, base_and_all_options), the 392-col canonical, `gtm_subaward_recipient_code_evidence` | Lance (not in artifact) |
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
| `gtm_entity_inferred_primeable_codes` | 1/(uei, code_type, code) · 263M | code_type, code | INFERRED could-prime (cooccurrence evidence). Filter by code first — that's the sort |
| `gtm_entity_inferred_subbable_codes` | 1/(uei, code_type, code) · 160M | code_type, code | mirror, could-sub |

### Award/transaction facts
| Table | Grain · rows | Sorted | Notes |
|---|---|---|---|
| `gtm_txn_events_slim` | 1/FPDS action · 108M | uei, action_date | Columns: uei, action_date, action_type_code (A–Y mod events), subcontracting_plan, naics_code, psc_code, awarding_agency_code, **obligation** (≠ federal_action_obligation), action_key, award_key |
| `usaspending_fpds_prime_award_state` | 1/contract_award_unique_key · 83M | current_end_date | 43 cols: award_topology, recipient_uei/name, life_to_date_obligated, current_end_date (expiry queries prune HARD on this), naics/psc, agency, PIIDs. DESCRIBE it |
| `subaward_canonical_slim` | 1/subaward · 1.3M | prime_awardee_uei | 36 cols; `subaward_amount` is VARCHAR — use `subaward_amount_num` |
| `subaward_canonical_slim_by_sub` | same rows | subawardee_uei | second copy, sub-side clustering |
| `gtm_open_awards` | 1/open award · 163k | recipient_uei | active-PoP/open-IDV universe, centroid geo pre-joined |

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

### People / identity / reference
| Table | Grain · rows | Sorted |
|---|---|---|
| `gtm_sam_people` | 1/(uei, name_key) · 2.3M | uei |
| `gtm_sam_person_contactability` | 1/sam_person_id · 152k | sam_person_id |
| `sam_pocs` | 1/(uei, role, slot) · 8.1M | uei |
| `sam_master_entities` | 1/uei SAM registration master · 1.5M | uei |
| `people_canonical` | 1/canonical_person_id · 132k | canonical_person_id |
| `firmographics_blitz` | 1/domain · 255k | domain_norm |
| `federal_sites_lance` | 1/federal site · 300k | state_code, zip5 |
| `naics_reference` · `psc_reference` · `gtm_naics_psc_pairs` · `agency_vocab` | code refs · 2.1k/6.1k/321k/75 | code |
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
2. **Read-only by construction** — write/DDL statements are rejected; don't try.
3. **Not everything is here** (§2). If a needed table/column is absent, say so rather than
   silently falling back to a spine scan — absence is signal for the next manifest revision.
4. `gtm_txn_events_slim` renames: `obligation` (not federal_action_obligation), `psc_code`
   (not product_or_service_code), `uei` (not recipient_uei).
