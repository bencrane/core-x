# Labor-Pricing L1 Foundation — Authoritative Reference

**Purpose:** turn a federal award's coarse codes (NAICS + PSC + place-of-performance + recipient UEI) into a *priced labor demand* answer, pre-joined onto the 108M-row FPDS spine without rebuilding it, for staffing firms targeting just-won federal contractors.

**Generated 2026-07-02 from live-probed ground truth.**

> Provenance: every row count, column name/type, and index (name/type/fields) in this document is traced to `/tmp/labordoc/ground.json` (a direct `lance.dataset(...).count_rows()` / `.schema` / `.list_indices()` probe of `s3://data-sink/active/*`, run 2026-07-02) and its index-only companion `/tmp/l1_probe.json` / `/tmp/l1_ground.json`. Strategic framing is from `/tmp/labordoc/seed.md`. Coverage percentages attributed to "lens-measured" are analytic estimates from downstream lens agents, **not** columns in ground truth, and are labeled as such. Where seed prose and ground truth disagree on a mechanical fact, **ground truth wins** and this document follows ground truth.

---

## 1. Executive thesis

### 1.1 The mission: coarse codes → priced labor demand
A federal contract award, as it lands on the spine, is a row of **coarse codes**: a NAICS industry code, a PSC (Product/Service Code), a place-of-performance county, a recipient UEI, an obligation dollar amount. None of that directly says *who the winner must hire, at what wage*. This foundation closes that gap as a chain:

```
what was bought      →  who must be hired        →  at what wage                     →  is the winner already unionized
(NAICS × PSC)           (SOC / SCA occupations)     (OEWS market / SCA floor)           (CBA-covered identity)
```

The end buyer is a **staffing firm**. The day an award posts, the staffing firm needs to know: what roles must this contractor staff, what is the wage envelope (market rate, statutory floor, union-covered), and is this a labor play at all. This foundation pre-computes that answer against the coarse codes so any award joins for a priced-labor profile with no query-time fan-out and no re-derivation.

### 1.2 Where it sits in the Gen-3 data plane
- **System of record:** LanceDB written directly to Cloudflare R2 (`s3://data-sink/active/<name>`). No catalog layer; datasets addressed by R2 URI.
- **Compute:** DuckDB out-of-core reads the Lance datasets and executes joins; Modal runs heavy index builds (≥96 GiB).
- **Indexing invariant:** every load-bearing resolution key carries a hard scalar index — `BTREE` for high-cardinality joins, `BITMAP` for low-cardinality filters. Indices are named `<col>_idx`. Publish is append-only; a full index rebuild is the invariant (GC deletes orphans).
- **The spine is never rebuilt to add a dimension.** Every artifact below sits *beside* the spine and is reached by a query-time join on indexed keys.

### 1.3 The 4-lever utilization framework
How each dataset relates to the 108M-row spine `usaspending_fpds_canonical_txn`:

| Lever | What it is | Used here? |
|-------|-----------|-----------|
| **L1** | DuckDB query-time join — a dimension/crosswalk sits beside the spine; join on indexed keys. | **This is the layer built.** Every dim below L1-joins the spine. |
| **L2** | Add a scalar index to the spine *in place* (no data rewrite). | Done once: `BTREE(pop_county_fips)` via PR #902 (`index_spawn` Modal entrypoint solved a build timeout). |
| **L3** | Materialize a new derived Lance dataset that L1-joins the spine. | Every dim/crosswalk below is an L3 artifact. |
| **L4** | Full spine `COLUMN_SPEC` rebuild (rewrite the 108M rows). | **Avoided entirely.** |

Hard rules honored throughout: (a) never rebuild the spine to add a dimension; (b) downstream consumers (map-serving / ask surfaces) do not dictate the foundation shape; (c) false positives are unacceptable in entity resolution — precision is guard-enforced, not sampled.

### 1.4 The three wage axes (the payload)
The foundation lands **three independent wage axes** onto the spine. They meet the spine at different keys and (critically) **do not meet each other at occupation identity** — there is no SCA↔SOC crosswalk landed (see §4, §7):

1. **OEWS market wage** — what the open market pays. National (`soc_priced_skilled`) + per-state (`soc_state_wage`). Keyed on **SOC**.
2. **SCA statutory floor** — the federally mandated Service Contract Act prevailing wage a contractor is legally bound to pay. `sca_wd_rates`, localized to the spine county via `sca_wd_county_rollup`. Keyed on **SCA occupation code** + `wd_id`.
3. **CBA-covered identity** — is the winning contractor already under a union collective-bargaining agreement (a §4(c) successorship signal). `olms_cba_crosswalk`, resolved to `recipient_uei`. An identity flag, **not** a rate table.

---

## 2. Dataset catalog

The foundation is **11 SoR artifacts**: the spine + 10 dimensions/crosswalks. Two additional L2 upstream caches (`naics_psc_labor_profile`, `naics_psc_labor_profile_categories`) feed the labor dim and are the fan-out source for multi-role detail; they are cataloged in §2.12 because several GTM unlocks reach through them. All URIs are `s3://data-sink/active/<name>`.

> All row counts, column types, and indices below are verbatim from ground truth. Index `type` values are reported exactly as Lance returns them (`BTree`, `Bitmap`).

---

### 2.1 `usaspending_fpds_canonical_txn` — THE SPINE

- **URI:** `s3://data-sink/active/usaspending_fpds_canonical_txn`
- **Rows:** **108,181,354** (transaction grain — one row per FPDS contract transaction, i.e. an award *modification*, not a de-duplicated award)
- **Columns:** 131
- **Grain:** contract transaction (`contract_transaction_unique_key`)
- **Role:** the base ledger. Every dim/crosswalk L1-joins to this. Never rebuilt.

**Indices (18): 11 BTREE + 7 BITMAP**

| Name | Type | Fields |
|------|------|--------|
| contract_transaction_unique_key_idx | BTree | contract_transaction_unique_key |
| contract_award_unique_key_idx | BTree | contract_award_unique_key |
| recipient_uei_idx | BTree | recipient_uei |
| action_date_idx | BTree | action_date |
| last_modified_date_idx | BTree | last_modified_date |
| naics_code_idx | BTree | naics_code |
| product_or_service_code_idx | BTree | product_or_service_code |
| federal_action_obligation_idx | BTree | federal_action_obligation |
| recipient_hash_idx | BTree | recipient_hash |
| award_id_piid_idx | BTree | award_id_piid |
| pop_county_fips_idx | BTree | pop_county_fips |
| action_date_fiscal_year_idx | Bitmap | action_date_fiscal_year |
| type_of_set_aside_code_idx | Bitmap | type_of_set_aside_code |
| awarding_agency_code_idx | Bitmap | awarding_agency_code |
| award_type_code_idx | Bitmap | award_type_code |
| idv_type_code_idx | Bitmap | idv_type_code |
| canonical_source_idx | Bitmap | canonical_source |
| subcontracting_plan_idx | Bitmap | subcontracting_plan |

**Load-bearing labor-relevant columns** (of the 131):

| Column | Type | Role in the labor chain |
|--------|------|------------------------|
| `naics_code` | string | Join key → dims (`naics_code`). BTREE. |
| `product_or_service_code` | string | Join key → dims **`psc_code`** (same value, different name — see §3). BTREE. |
| `recipient_uei` | string | Join key → `olms_cba_crosswalk.uei`, `sam_normalized_entities.uei`. BTREE. |
| `pop_county_fips` | string | Place-of-performance county; join key → `sca_wd_county_rollup.county_fips`. 5-digit zero-padded FIPS (e.g. `24031`). BTREE (added PR #902). |
| `primary_place_of_performance_state_code` | string | 2-letter USPS; join key → `soc_state_wage.prim_state`. |
| `type_of_set_aside_code` | string | Set-aside segmentation (SBA, 8AN, 8A, SDVOSBC, HZC, WOSB, …). BITMAP. |
| `women_owned_small_business` | string | Socioeconomic flag — `'T'`/`'F'` STRING, **not** boolean, not null-sparse. Predicate `= 'T'`. Un-indexed. |
| `service_disabled_veteran_owned_business` | string | `'T'`/`'F'` STRING flag. Un-indexed. |
| `historically_underutilized_business_zone_hubzone_firm` | string | `'T'`/`'F'` STRING flag. Un-indexed. |
| `c8a_program_participant` | string | `'T'`/`'F'` STRING flag. Un-indexed. |
| `construction_wage_rate_requirements_code` | string | Davis-Bacon (DBA) applicability flag on the award. |
| `labor_standards_code` | string | SCA labor-standards applicability flag on the award. |
| `action_date` | date32[day] | Recency filter ("just-won"). BTREE. |
| `federal_action_obligation` | double | Dollar weight. Transaction-grain (double-counts across modifications of the same award — roll up on `contract_award_unique_key` first for award-level dollars). BTREE. |
| `recipient_hash` | string | Fallback grouping key when `recipient_uei` is null (legacy/foreign recipients). BTREE. |
| `contract_award_unique_key` | string | Award-level de-dup key. BTREE. |

- **Provenance:** FPDS bulk + delta ingest, canonicalized (`canonical_source` ∈ {bulk, …}; `built_at` on each row). Sample row `built_at` = 2026-07-02.
- **Caveat:** `pop_county_fips` is BULK-vintage populated; the sample row (an IDV) shows `pop_county_fips = null` while `recipient_location_county_fips = 24031` — place-of-performance FIPS is present on transaction-level award rows, not on every IDV skeleton. `recipient_location_county_fips` is the separate (un-indexed) contractor-HQ county.

---

### 2.2 `soc_priced_skilled` — national OEWS market wage (SOC)

- **URI:** `s3://data-sink/active/soc_priced_skilled`
- **Rows:** **830**
- **Grain:** `soc_code` (detailed SOC-2018, one row per occupation)
- **Role:** national market-wage axis (axis 1). The **convention template** all later modules mirror.

**Indices (1):**

| Name | Type | Fields |
|------|------|--------|
| soc_code_idx | BTree | soc_code |

**Load-bearing columns:** `soc_code`, `soc_title`, `onet_title`, `onet_description`; wage: `h_mean`, `a_mean`, and the full decile ladder `h_pct10 / h_pct25 / h_median / h_pct75 / h_pct90` + `a_pct10 / a_pct25 / a_median / a_pct75 / a_pct90` (all `double`); outlook (BLS EP 2024–2034): `openings_annual_avg`, `emp_change_2024_2034`, `emp_pct_change_2024_2034`, `total_separations_rate`, `labor_force_exit_rate`, `occupational_transfer_rate`; `tot_emp`, `source`, `soc_vintage`, `ingested_at`.

- **Provenance:** `source = OEWS_M2025+ONET+EP_2024_2034`; `soc_vintage = SOC_2018`. OEWS national wages + O*NET titles/descriptions + BLS Employment Projections 2024–2034 (attached LEFT). PR #893.
- **Caveat:** national only. A handful of OEWS SOCs have no EP twin → NULL outlook. Suppressed OEWS cells are NULL. `naics_psc_labor_dim.rank1_soc_wage_median` is carried 1:1 from this dataset's `a_median`.

---

### 2.3 `soc_state_wage` — per-state OEWS market wage (SOC × state)

- **URI:** `s3://data-sink/active/soc_state_wage`
- **Rows:** **35,223**
- **Grain:** `soc_code` × `prim_state` (51 states + DC; no sub-state metro)
- **Role:** locality (state) market-wage axis. Localizes axis 1 by performance state.

**Indices (2):**

| Name | Type | Fields |
|------|------|--------|
| soc_code_idx | BTree | soc_code |
| prim_state_idx | BTree | prim_state |

**Load-bearing columns:** `soc_code`, `prim_state` (USPS 2-letter), `state_fips`, `state_name`; same 12 wage measures as §2.2 (`h_mean`, `a_mean`, `h_pct10…h_pct90`, `a_pct10…a_pct90`, all `double`); `tot_emp`, `source`, `soc_vintage`, `ingested_at`.

- **Provenance:** `source = OEWS_M2025_state`; `soc_vintage = SOC_2018`. PR #903.
- **Caveat:** `prim_state` matches `spine.primary_place_of_performance_state_code` exactly. Territory/APO codes on the spine (e.g. `MH` = Marshall Islands) have no OEWS state row → NULL; fall back to national `soc_priced_skilled`. Thin SOC×state cells can have NULL `a_median` (suppression).

---

### 2.4 `sam_wd_county_fips_xwalk` — SAM WD county → Census FIPS

- **URI:** `s3://data-sink/active/sam_wd_county_fips_xwalk`
- **Rows:** **3,338**
- **Grain:** state × county-name × independent-city (SAM WD geography identity)
- **Role:** building block that resolves SAM's internal WD county geography to real 5-digit Census FIPS. This is why the SCA floor join to the spine does not fabricate spurious matches.

**Indices (2):**

| Name | Type | Fields |
|------|------|--------|
| county_fips_idx | BTree | county_fips |
| county_name_idx | BTree | county_name |

**Load-bearing columns:** `state_code`, `county_name`, `county_fips` (resolved 5-digit Census FIPS; NULL where unmapped), `county_name_census`, `class_fp` (H = county, C7 = independent city — disambiguation), `scope`, `match_method`, `source`, `ingested_at`.

- **Provenance:** `source = sam_wd_county_coverage+national_county2020`. SAM `sam_wd_county_coverage` county codes disambiguated against the Census `national_county2020` gazetteer. PR #897.
- **Caveat:** exists **because** SAM's `county_code` is a SAM-internal code, NOT a Census FIPS — a naive `county_code = pop_county_fips` equality fabricates spurious matches. Statewide/unmapped rows carry NULL `county_fips` (sample row: "Wake Island", `scope = unmapped`). The residual unresolved (state, county) pairs silently drop from geographic floor coverage — a known, bounded gap.

---

### 2.5 `sca_wd_county_rollup` — active SCA WD ids per county

- **URI:** `s3://data-sink/active/sca_wd_county_rollup`
- **Rows:** **3,224**
- **Grain:** `county_fips` (one row per covered Census county-equivalent)
- **Role:** the geographic bind. Maps a spine place-of-performance county to its governing SCA wage determination(s), then hands off to `sca_wd_rates` for the priced register.

**Indices (1):**

| Name | Type | Fields |
|------|------|--------|
| county_fips_idx | BTree | county_fips |

**Load-bearing columns:** `county_fips` (5-digit, matches `spine.pop_county_fips` exactly, no cast), `active_sca_wd_ids` (`list<string>` — full covering set of active SCA WD ids), `wd_count` (int32), `canonical_wd_id` (string — the single highest-revision active SCA WD per county; ties → lexicographic max), `has_sca_coverage` (bool), `source`, `ingested_at`.

- **Provenance:** `source = SAM_WD_COUNTY_COVERAGE+SCA_2026-07`. Built on `sam_wd_county_fips_xwalk`. PR #897.
- **Caveat:** covers 3,224 counties; a `pop_county_fips` outside this set has no SCA coverage row (returns no floor). Statewide WDs are already fanned out to member counties (no statewide gap). `canonical_wd_id` is a deterministic default — the contract-specific governing WD is award-specified; `wd_count > 1` flags multi-WD localities where floor selection matters.

---

### 2.6 `sca_wd_rates` — parsed SCA register line-items (the priced SCA floor)

- **URI:** `s3://data-sink/active/sca_wd_rates`
- **Rows:** **371,408**
- **Grain:** `wd_id` × `occupation_code` (one priced occupation row per WD register)
- **Role:** the priced $ of the SCA statutory-floor axis. The hard cost floor a staffing firm cannot bill beneath.

**Indices (2):**

| Name | Type | Fields |
|------|------|--------|
| wd_id_idx | BTree | wd_id |
| occupation_code_idx | BTree | occupation_code |

**Load-bearing columns:** `wd_id` (join target of `sca_wd_county_rollup.canonical_wd_id`), `occupation_code` (**SCA 5-digit** occupation code — e.g. `23210`), `title`, `hourly_wage` (double, statutory base), `footnote` (int32), `health_welfare_hourly` (double, H&W fringe), `health_welfare_eo13706_hourly` (double, EO 13706 sick-leave fringe where applicable), `wd_revision` (int32), `source`, `ingested_at`.

- **Provenance:** `source = SCA_WD_RATE_REGISTER`. Parsed SCA registers from `sam_wd_rate_documents` (`wd_type = 'SCA'` only). PR #906.
- **Caveat:** **SCA-only** — DBA construction registers are excluded at source and remain unparsed raw text in `sam_wd_rate_documents` (`wd_type = 'DBA'`). The loaded/burdened floor = `hourly_wage + health_welfare_hourly` (+ `health_welfare_eo13706_hourly` on sick-leave-covered docs). `occupation_code` is **SCA 5-digit space** — it does **not** share a key with SOC `dd-dddd` codes (no SCA↔SOC crosswalk landed; see §4, §7).

---

### 2.7 `naics_psc_labor_dim` — the pre-joined labor profile (the payload dim)

- **URI:** `s3://data-sink/active/naics_psc_labor_dim`
- **Rows:** **14,112**
- **Grain:** `naics_code` × `psc_code` (1:1 per combo)
- **Role:** the central dim. Pre-joins the OEWS market answer 1:1 onto every classified (NAICS, PSC) combo so the spine inherits a priced role + bill-rate anchor with no query-time fan-out. `rank1_soc_code` non-null ⟺ `is_labor_play`.

**Indices (6): 3 BTREE + 3 BITMAP**

| Name | Type | Fields |
|------|------|--------|
| naics_code_idx | BTree | naics_code |
| psc_code_idx | BTree | psc_code |
| rank1_soc_code_idx | BTree | rank1_soc_code |
| is_labor_play_idx | Bitmap | is_labor_play |
| psc_category_idx | Bitmap | psc_category |
| spend_category_l1_idx | Bitmap | spend_category_l1 |

**Load-bearing columns:** `naics_code`, `psc_code` (= `spine.product_or_service_code`), `naics_title`, `psc_title`, `psc_category`, `is_labor_play` (bool), `work_summary`, `n_categories`, `top_confidence`, `resolution_level`, `n_awards`, `total_dollars_obligated`, `rank1_soc_code`, `rank1_soc_title`, `rank1_sca_code`, `rank1_sca_title`, `rank1_role_class`, `rank1_soc_wage_median` (double — carried from `soc_priced_skilled.a_median`), `top_soc_codes` (`list<string>`, ranked), `top_sca_codes` (`list<string>`, ranked), `spend_category_l1`, `source`, `ingested_at`.

- **Provenance:** `source = naics_psc_labor_profile+categories+soc_priced_skilled+psctool_spend_map`. Collapses the `naics_psc_labor_profile_categories` fan-out to a 1:1 join surface; `rank1_*` = the top-ranked category; `spend_category_l1` from `psctool` spend-category PSC map. PR #919.
- **Caveat:** `n_awards` and `total_dollars_obligated` are frozen at the dim's build-time worklist (`govcon_active_awards` + subaward-gap vintages), **not** recomputed from the live 108M spine — they under-count vs a fresh spine aggregation. Authoritative sizing is the spine's `federal_action_obligation`. `rank1_soc_wage_median` is a **national** anchor, not localized. `spend_category_l1` is NULL on some combos (`psctool` map returns 'undefined'→NULL). Coverage: 14,112 combos (lens-measured ≈ 4.4% of the spine's ~320.6K distinct live non-null (naics, psc) pairs, but ≈ 22.3% of transaction rows — the high-$ head, deliberately not the full tail).

---

### 2.8 `naics_psc_deliverable` — "what was done" per combo (sibling of the labor dim)

- **URI:** `s3://data-sink/active/naics_psc_deliverable`
- **Rows:** **20,998**
- **Grain:** `naics_code` × `psc_code`
- **Role:** the make-vs-resell / work-type read on the same (naics, psc) key. Deliberately **decoupled** from `naics_psc_labor_dim` (do NOT recouple) — broader coverage (20,998 > 14,112), so LEFT JOIN from the labor dim is correct and some deliverable combos have no labor-dim row.

**Indices (2):**

| Name | Type | Fields |
|------|------|--------|
| naics_code_idx | BTree | naics_code |
| psc_code_idx | BTree | psc_code |

**Load-bearing columns:** `naics_code`, `psc_code`, `what_was_done`, `work_type` (e.g. `manufacture`), `regime` (e.g. `redundant`), `confidence`, `review_status`, `prompt_version`, `model_id` (`claude-opus-4-8`), `generated_at`, `source_vintage`.

- **Provenance:** LLM-classified (`prompt_version = what_was_done_v2`, `model_id = claude-opus-4-8`); subaward-frequency expansion to 100% subaward coverage. PR #904.
- **Caveat:** a spine (naics, product_or_service_code) pair absent from this dim is unclassified, not automatically a goods line.

---

### 2.9 `sam_wd_cba_pointers` — §4(c) CBA pointer records

- **URI:** `s3://data-sink/active/sam_wd_cba_pointers`
- **Rows:** **4,298**
- **Grain:** SAM WDOL CBA pointer (`wd_id` / `cba_number`)
- **Role:** §4(c) collective-bargaining pointer corpus from the SAM WDOL API. Identifies that a §4(c) CBA pointer exists — but the contractor side is **identity-poor**.

**Indices (6): 3 BTREE + 3 BITMAP**

| Name | Type | Fields |
|------|------|--------|
| wd_id_idx | BTree | wd_id |
| cba_number_idx | BTree | cba_number |
| organization_id_idx | BTree | organization_id |
| status_idx | Bitmap | status |
| latest_idx | Bitmap | latest |
| archived_idx | Bitmap | archived |

**Load-bearing columns:** `wd_id`, `cbawd_id`, `cba_number`, `revision_number`, `contract_services`, `contractor_name` (free-text, ~45% populated), `contractor_union`, `organization_name`, `organization_id` (SAM-internal WDOL **org** id = agency, **NOT** a contractor UEI), `status`, `latest` (bool), `archived` (bool), effective/published/created/modified date strings, `location_count`, `fetch_status`, `http_status`, `fetched_at`, `source`.

- **Provenance:** `source = sam.gov/api/prod/wdol/v1/cba` (frontend, no api_key). PR #905.
- **Caveat:** **NO uei column** and no clean company key — the §4(c) contractor side is unbridgeable to the spine today. `organization_id` is a SAM org id, not a contractor. `contractor_name` is null on ~55% of pointers. Closing this needs a fuzzy `contractor_name → recipient_uei` resolver (see §7).

---

### 2.10 `olms_cba_crosswalk` — OLMS union employers resolved → SAM UEI (CBA-covered identity)

- **URI:** `s3://data-sink/active/olms_cba_crosswalk`
- **Rows:** **4,844**
- **Grain:** `doc_id` (one row per OLMS CBA document)
- **Role:** the CBA-covered identity axis (axis 3). Binds the OLMS union-contract corpus to the spine via `recipient_uei`. A "union-covered, price carefully" flag — NOT a wage schedule.

**Indices (2):**

| Name | Type | Fields |
|------|------|--------|
| doc_id_idx | BTree | doc_id |
| uei_idx | BTree | uei |

**Load-bearing columns:** `doc_id`, `cba_pub_id`, `emp_name`, `emp_name_normalized`, `olms_state`, `union_name`, `exp_date`, `uei` (resolved SAM UEI; NULL on unmatched docs), `matched_legal_name`, `selected_uei_state`, `tier` (`T1` exact / `T2` geo-confirmed fuzzy / `unmatched`), `score`, `candidate_uei_count`, `on_spine` (bool — resolved UEI is present on the spine), `is_active`, `geo_corroborated` (bool), `source`, `ingested_at`.

- **Provenance:** `source = OLMS_CBA_CROSSWALK`. OLMS union-contract employers resolved to SAM UEI; T1 (exact) + T2 (fuzzy ≥95 + token-count guard + UEI-scoped geo-confirm) only. FP-guarded (ABC-class dropped; corroboration-required + no OLMS state ⇒ unmatched; 0 cross-state). PR #926.
- **Caveat:** precision-biased. 4,844 rows total; `uei` is NULL on unmatched docs (sample row is `tier = unmatched`, `uei = null`). Absence of a match does **not** mean non-union — this is a coverage floor, not a census of unionization. Filter `on_spine = true` and `tier IN ('T1','T2')` to avoid unresolved rows. Identifies **coverage**, not the union rate table (that lives in the unparsed contract PDFs — see §7).

---

### 2.11 `olms_cba_uei_candidates` — multi-UEI disambiguation audit sidecar

- **URI:** `s3://data-sink/active/olms_cba_uei_candidates`
- **Rows:** **5,402**
- **Grain:** `doc_id` × `candidate_uei`
- **Role:** the audit trail behind `olms_cba_crosswalk` — every candidate UEI considered per OLMS doc during resolution, with which was selected/active/on-spine.

**Indices (2):**

| Name | Type | Fields |
|------|------|--------|
| doc_id_idx | BTree | doc_id |
| candidate_uei_idx | BTree | candidate_uei |

**Load-bearing columns:** `doc_id`, `emp_name_normalized`, `candidate_uei`, `is_active` (bool), `is_selected` (bool), `on_spine` (bool).

- **Provenance:** produced alongside `olms_cba_crosswalk`. PR #926.
- **Caveat:** an audit sidecar, not a serving surface — use for provenance/why-was-this-UEI-chosen questions, not for direct spine joins.

---

### 2.12 L2 upstream cache (feeds the labor dim; fan-out source for multi-role detail)

These two are the classification cache that `naics_psc_labor_dim` collapses. They are **not** among the 11 serving artifacts, but several GTM unlocks reach through `_categories` for the ranked full-role detail, so their schema is cataloged here.

**`naics_psc_labor_profile`** — one row per combo (the profile head).
- **Rows:** **14,112** · **Grain:** `naics_code` × `psc_code`
- **Indices:** `naics_code_idx` (BTree), `psc_code_idx` (BTree), `is_labor_play_idx` (Bitmap), `resolution_level_idx` (Bitmap), `top_confidence_idx` (Bitmap), `psc_category_idx` (Bitmap)
- Sibling head of the labor dim; carries `work_summary`, `is_labor_play`, `resolution_level`, `oews_industry_code`, `n_awards`, `total_dollars_obligated`, provenance.

**`naics_psc_labor_profile_categories`** — one row per (combo, rank): the fan-out detail.
- **Rows:** **45,333** · **Grain:** `naics_code` × `psc_code` × `rank` (avg ≈ 3.2 roles/combo, max 10)
- **Indices (9): 4 BTREE + 5 BITMAP**

| Name | Type | Fields |
|------|------|--------|
| naics_code_idx | BTree | naics_code |
| psc_code_idx | BTree | psc_code |
| soc_code_idx | BTree | soc_code |
| sca_code_idx | BTree | sca_code |
| role_class_idx | Bitmap | role_class |
| confidence_idx | Bitmap | confidence |
| off_pattern_idx | Bitmap | off_pattern |
| resolution_level_idx | Bitmap | resolution_level |
| psc_category_idx | Bitmap | psc_category |

- **Columns** (from `materialize_naics_psc_labor_profile.py`): `naics_code`, `psc_code`, `rank`, `psc_category`, `soc_code`, `soc_title`, `off_pattern` (bool), `sca_code`, `sca_title`, `role_class` (`core_deliverable` / `support` / `overhead`), `confidence` (`high`/`medium`/`low`), `pct_of_industry` (double — OEWS staffing-pattern share, **not** a head count), `a_median` (**string** — resolved OEWS annual median, stored as text; CAST before any numeric compare. Note the asymmetry: `soc_priced_skilled.a_median` and `soc_state_wage.a_median` are `double`, but the `_categories` copy is `string`), `ep_growth_2024_2034_pct` (double), + provenance (`resolution_level`, `oews_industry_code`, `candidates_sha256`, `source_vintage`), `ingested_at`.
- **Provenance:** LLM labor-profile classification (`claude-opus-4-8` in-session; prompts `goods_profile_v1` + `labor_profile_v2`), grounded on a deterministic OEWS staffing-pattern candidate set. Fail-closed combo gate: every manifest combo must be present or the dataset is not written.
- **KEY FACT — co-classification:** each category row carries **both** `soc_code` and `sca_code`, co-classified by the LLM. Within the 14,112-combo universe the two ARE aligned row-wise. This is the **only** place SCA and SOC co-reside — but it is combo-scoped, not a standalone reusable SCA↔SOC crosswalk (see §7).
- **Caveat:** `role_class`/`confidence` are LLM-assigned, not empirically validated — a prioritization prior. `pct_of_industry` is a staffing-pattern share, **not** a per-role quantity. Fanning this onto the spine multiplies rows and must be aggregated.

---

## 3. Relationship / join map

### 3.1 The graph

```
                                    usaspending_fpds_canonical_txn  (SPINE, 108,181,354 rows)
                                    │
     ┌──────────────────────┬──────┴───────────────┬─────────────────────────────┐
     │ naics_code (BTREE)   │ pop_county_fips      │ recipient_uei (BTREE)       │ primary_place_of_
     │ + product_or_        │ (BTREE)              │                             │ performance_state_code
     │ service_code (BTREE) │                      │                             │
     ▼                      ▼                      ▼                             ▼
 ┌───────────────────┐  ┌────────────────────┐  ┌──────────────────────┐   (used with SOC below)
 │ naics_psc_        │  │ sca_wd_county_     │  │ olms_cba_crosswalk   │
 │ labor_dim         │  │ rollup             │  │  .uei (BTREE)        │
 │ (14,112; 1:1)     │  │ (3,224)            │  │ (4,844; identity)    │
 │  psc_code ==      │  │ county_fips        │  │  filter on_spine,    │
 │  spine.product_   │  │ (BTREE)            │  │  tier IN (T1,T2)     │
 │  or_service_code  │  │                    │  └──────────────────────┘
 └───────┬───────────┘  │  canonical_wd_id ──┼──────────┐
         │              └────────────────────┘          │
         │ rank1_soc_code (BTREE)                        ▼
         │ / top_soc_codes                       ┌────────────────────┐
         ▼                                       │ sca_wd_rates       │
 ┌──────────────────────┐                        │ (371,408)          │
 │ soc_priced_skilled   │  ◄── AXIS 1 (national) │  wd_id (BTREE) +   │  ◄── AXIS 2 (SCA statutory
 │ (830) soc_code BTREE │                        │  occupation_code   │        floor, county-localized)
 └──────────────────────┘                        │  (BTREE, SCA 5-dig)│
 ┌──────────────────────┐                        └────────────────────┘
 │ soc_state_wage       │  ◄── AXIS 1 (state)
 │ (35,223)             │        keyed (soc_code, prim_state);
 │ soc_code + prim_state│        prim_state == spine.primary_place_
 │ (both BTREE)         │        of_performance_state_code
 └──────────────────────┘

 Fan-out detail (L2 cache, reach for ranked full-role set):
   naics_psc_labor_dim ──(naics_code, psc_code)──► naics_psc_labor_profile_categories (45,333; 1:N)
       carries BOTH soc_code AND sca_code per row  ── the ONLY co-residence of the two namespaces.

 Geo building block:
   sam_wd_county_fips_xwalk (3,338) ── county_fips ──► sca_wd_county_rollup   (constructs the rollup)

 Sibling (decoupled, LEFT JOIN):
   spine (naics_code, product_or_service_code) ──► naics_psc_deliverable (20,998; what_was_done / work_type)

 ⚠ THE OCCUPATION-IDENTITY GAP:
   sca_wd_rates.occupation_code (SCA 5-digit)  ✗  NO KEY  ✗  soc_priced_skilled.soc_code (SOC dd-dddd)
   AXIS 1 and AXIS 2 meet the spine at geography (county) and combo (naics×psc), NEVER at occupation identity.
```

### 3.2 Join-key table

> **Index-notation convention (read before planning a query):** where the "Index ridden" column lists a comma-separated set such as `BTREE(naics_code, product_or_service_code)` or `BTREE(soc_code, prim_state)`, that shorthand denotes **N separate single-field BTREE indices** (one per column) — **NOT** a compound/composite multi-column index. Ground truth holds **no compound index anywhere** in this foundation; e.g. `soc_state_wage` carries two distinct indices `soc_code_idx` and `prim_state_idx`, and `naics_psc_labor_dim` carries `naics_code_idx`, `psc_code_idx`, `rank1_soc_code_idx` as three separate BTREEs. A multi-key join rides one single-field index per key (the planner intersects them); do not assume a single compound-key index exists or plan a covering-index scan on the tuple. The authoritative per-index listing is the §2 per-dataset catalog (each index on its own row).

| From | Key (from) | To | Key (to) | Index ridden | Cardinality |
|------|-----------|----|---------|--------------|-------------|
| spine | `naics_code` + `product_or_service_code` | `naics_psc_labor_dim` | `naics_code` + `psc_code` | spine BTREE(naics_code, product_or_service_code); dim BTREE(naics_code, psc_code) | 1:1 |
| spine | `naics_code` + `product_or_service_code` | `naics_psc_deliverable` | `naics_code` + `psc_code` | both-side BTREE(naics_code, psc_code) | LEFT (some combos absent) |
| spine | `pop_county_fips` | `sca_wd_county_rollup` | `county_fips` | spine BTREE(pop_county_fips); rollup BTREE(county_fips) | N:1 |
| `sca_wd_county_rollup` | `canonical_wd_id` | `sca_wd_rates` | `wd_id` | rates BTREE(wd_id) | 1:N (occupations) |
| spine | `recipient_uei` | `olms_cba_crosswalk` | `uei` | spine BTREE(recipient_uei); xwalk BTREE(uei) | N:1 (filter on_spine, tier) |
| spine | `primary_place_of_performance_state_code` | `soc_state_wage` | `prim_state` | soc_state_wage BTREE(prim_state) | (with soc_code) |
| `naics_psc_labor_dim` | `rank1_soc_code` | `soc_priced_skilled` | `soc_code` | dim BTREE(rank1_soc_code); soc BTREE(soc_code) | N:1 |
| `naics_psc_labor_dim` | `rank1_soc_code` (+ state) | `soc_state_wage` | `soc_code` (+ `prim_state`) | soc_state_wage BTREE(soc_code, prim_state) | N:1 |
| `naics_psc_labor_dim` | `rank1_sca_code` | `sca_wd_rates` | `occupation_code` | rates BTREE(occupation_code) | N:1 |
| `naics_psc_labor_dim` | `naics_code` + `psc_code` | `naics_psc_labor_profile_categories` | `naics_code` + `psc_code` | categories BTREE(naics_code, psc_code) | 1:N (fan-out) |
| `sam_wd_county_fips_xwalk` | `county_fips` | `sca_wd_county_rollup` | `county_fips` | both BTREE(county_fips) | build-time |

### 3.3 Column-name mismatch — CALL-OUT

> **`spine.product_or_service_code` == `dim.psc_code`.** Same value (the 4-char PSC namespace, identical on both sides), **different column name**. The spine PSC field is `product_or_service_code`; every dim (`naics_psc_labor_dim`, `naics_psc_deliverable`, `naics_psc_labor_profile*`) calls it `psc_code`. Any join renames `psc_code <-> product_or_service_code`. This is the single most common foot-gun in this foundation. `naics_code` is named identically on both sides.

Other name notes: spine `recipient_uei` → crosswalk `uei`; spine `primary_place_of_performance_state_code` → `soc_state_wage.prim_state`; `sca_wd_county_rollup.canonical_wd_id` → `sca_wd_rates.wd_id`.

---

## 4. The three wage axes

| | AXIS 1 — OEWS MARKET | AXIS 2 — SCA STATUTORY FLOOR | AXIS 3 — CBA-COVERED IDENTITY |
|---|---|---|---|
| **What** | What the open market pays (all workers, survey estimate). | The federally mandated Service Contract Act prevailing wage the contractor is legally bound to pay. | Whether the winning contractor is already under a union CBA (§4(c) successorship signal). |
| **Datasets** | `soc_priced_skilled` (830, national), `soc_state_wage` (35,223, state) | `sca_wd_rates` (371,408) + `sca_wd_county_rollup` (3,224) + `sam_wd_county_fips_xwalk` (3,338) | `olms_cba_crosswalk` (4,844) + audit `olms_cba_uei_candidates` (5,402) |
| **Grain** | SOC (national); SOC × state | `wd_id` × SCA `occupation_code` (5-digit) | `doc_id`, resolved to `recipient_uei` |
| **Key** | SOC `dd-dddd` | SCA 5-digit `occupation_code` + `wd_id` | `uei` |
| **How it localizes** | State via `primary_place_of_performance_state_code` = `prim_state`. No sub-state metro. | County via `spine.pop_county_fips` → `sca_wd_county_rollup.county_fips` → `canonical_wd_id` → `sca_wd_rates.wd_id`. County-precise. | Not geographic — an entity-identity match on UEI. |
| **What you get** | `a_median`/`h_median` + full decile ladder (pct10…pct90) + BLS EP outlook. | `hourly_wage` + `health_welfare_hourly` fringe (+ `health_welfare_eo13706_hourly`). Loaded floor = base + fringe. | `union_name`, `exp_date`, `tier`, `geo_corroborated`. Identity/flag only. |
| **Limit** | Market rate, not a compliance floor. National/state only; suppressed cells NULL. | SCA-only (no DBA/NAF). Only counties with ≥1 active SCA WD. WD schedule rate, not a burdened bill rate. | Precision-biased coverage floor; NOT the union rate table (PDFs unparsed). Absence ≠ non-union. |

**The binding constraint across axes:** AXIS 1 (SOC-keyed) and AXIS 2 (SCA-code-keyed) **never meet at occupation identity**. `sca_wd_rates.occupation_code` (SCA 5-digit) shares no key with `soc_priced_skilled.soc_code` (SOC `dd-dddd`); no SCA↔SOC crosswalk is landed. They meet the spine only at **geography** (county) and **combo** (naics × psc). A single-row "this exact SCA category vs its OEWS twin" comparison is blocked until the bridge is built. (`naics_psc_labor_profile_categories` co-classifies both codes per row within the 14,112-combo universe — but combo-scoped, not a reusable dim.)

---

## 5. Verifiable GTM unlocks

Grouped by lens. Each unlock: question, exact join path, tables + indexed keys, caveat, and **answerable-now** status. All join paths are confirmed against ground truth; any unlock whose path could not be confirmed was dropped.

### 5.1 Staffing-buyer lens

**[NOW] For a just-won service contract at a place of performance, the localized SCA statutory floor (base + H&W fringe) per priced labor category.**
- Join: `spine.pop_county_fips` → `sca_wd_county_rollup.county_fips` (→ `canonical_wd_id` / `active_sca_wd_ids`) → `sca_wd_rates.wd_id`; return `occupation_code, title, hourly_wage, health_welfare_hourly, health_welfare_eo13706_hourly`.
- Tables: spine, `sca_wd_county_rollup`, `sca_wd_rates`. Keys: spine `pop_county_fips_idx` → rollup `county_fips_idx`; `canonical_wd_id` → `sca_wd_rates wd_id_idx`.
- Caveat: loaded floor = `hourly_wage + health_welfare_hourly` (+ eo13706 where present). SCA-only; only the 3,224 covered counties return a floor. `canonical_wd_id` is a deterministic default; award-specific WD is contract-specified.
- ⚠ **Coverage caveat — restated from §2.1 because it bites hardest here:** this join keys on `spine.pop_county_fips`, which is **BULK-vintage populated and NULL on IDV skeletons** (the §2.1 sample IDV row has `pop_county_fips = null` while `recipient_location_county_fips = 24031`). Rows with NULL `pop_county_fips` silently drop from the SCA-floor join and yield no floor — do **not** read "no floor returned" as "no SCA coverage." Measure floor coverage only over rows where `pop_county_fips IS NOT NULL`, or the SCA-floor hit-rate is over-estimated. `recipient_location_county_fips` (contractor-HQ county, un-indexed) is NOT a substitute — it is HQ, not place-of-performance.

**[NOW] For a just-won labor award, the median wage of its primary staffing occupation (national), inline.**
- Join: `spine.naics_code + product_or_service_code` → `naics_psc_labor_dim` (gives `rank1_soc_code + rank1_soc_title + rank1_soc_wage_median` inline); optionally deepen `rank1_soc_code` → `soc_priced_skilled.soc_code` for the full percentile ladder + outlook.
- Tables: spine, `naics_psc_labor_dim`, `soc_priced_skilled`. Keys: dim BTREE(naics_code, psc_code, rank1_soc_code); `soc_priced_skilled` BTREE(soc_code).
- Caveat: `rank1_soc_wage_median` is the national OEWS figure; for locality use `soc_state_wage`. `rank1_soc` is the single top role — `top_soc_codes` holds the fuller mix.

**[NOW] Localize the wage bill — price the primary occupation at the performance state, not the national average.**
- Join: `spine.naics_code + product_or_service_code` → `naics_psc_labor_dim.rank1_soc_code`, then `(rank1_soc_code, spine.primary_place_of_performance_state_code)` → `soc_state_wage.(soc_code, prim_state)`.
- Tables: spine, `naics_psc_labor_dim`, `soc_state_wage`. Keys: dim BTREE(rank1_soc_code); `soc_state_wage` BTREE(soc_code, prim_state).
- Caveat: territory/APO codes (e.g. `MH`) have no OEWS state row → NULL; fall back to national `soc_priced_skilled`.

**[NOW] The ranked full SOC role stack for a target's roster of combos (pitch the whole labor stack, not one headline role).**
- Join: `spine.naics_code + product_or_service_code` → `naics_psc_labor_dim.top_soc_codes` (`list<string>`); UNNEST → `soc_priced_skilled.soc_code`; or drop to `naics_psc_labor_profile_categories` on (naics, psc) for full ranked detail + `role_class`/`confidence`.
- Tables: spine, `naics_psc_labor_dim`, `soc_priced_skilled`, `naics_psc_labor_profile_categories`. Keys: dim BTREE(naics_code, psc_code); `soc_priced_skilled` BTREE(soc_code); categories BTREE(naics_code, psc_code).
- Caveat: `_categories` (45,333 rows, avg ≈3.2 roles/combo, max 10) fans out the spine and must be aggregated.

### 5.2 Wage-arbitrage / margin lens

**[NOW] Rank just-won contractors by dollar-weighted labor-cost exposure (largest imminent priced hiring need).**
- Join: `spine.naics_code + product_or_service_code` → `naics_psc_labor_dim` (filter `is_labor_play = true`); weight by `spine.federal_action_obligation`; anchor on `rank1_soc_wage_median`; group by `spine.recipient_uei`.
- Tables: spine, `naics_psc_labor_dim`. Keys: spine BTREE(naics_code, product_or_service_code, recipient_uei, federal_action_obligation); dim BITMAP(is_labor_play).
- Caveat: authoritative sizing is the spine's `federal_action_obligation` (dim `n_awards`/`total_dollars_obligated` are descriptive-only, build-time frozen). De-dup on `contract_award_unique_key` before summing.

**[NOW] State-to-state OEWS wage differential for an occupation (rank contractors in highest-spread geographies for bill-rate uplift).**
- Join: `soc_state_wage.soc_code + prim_state` (self-compare across `prim_state`); bind to won demand via `spine.primary_place_of_performance_state_code = soc_state_wage.prim_state` and `spine.naics_code/product_or_service_code → naics_psc_labor_dim.rank1_soc_code = soc_state_wage.soc_code`.
- Tables: `soc_state_wage`, `naics_psc_labor_dim`, spine. Keys: `soc_state_wage` BTREE(soc_code, prim_state); dim BTREE(naics_code, psc_code, rank1_soc_code).
- Caveat: state grain only (no sub-state metro). Guard NULL `a_median` on thin cells.

**[NOW] Intra-occupation wage dispersion (pct90/pct10 ratio, IQR band) nationally and by state — bill-rate elasticity for senior-vs-entry placements.**
- Join: `soc_priced_skilled.soc_code` → national decile ladder; `soc_state_wage.(soc_code, prim_state)` → state ladder; bind role via `naics_psc_labor_dim.rank1_soc_code` / `top_soc_codes`.
- Tables: `soc_priced_skilled`, `soc_state_wage`, `naics_psc_labor_dim`. Keys: `soc_priced_skilled` BTREE(soc_code); `soc_state_wage` BTREE(soc_code, prim_state); dim BTREE(rank1_soc_code).
- Caveat: both carry the full hourly + annual decile ladder as cleaned DOUBLE. Guard NULL `pct10` before dividing.

**[NOW] Margin-durability occupations: high national wage AND high projected openings/churn (bill-rate headroom + repeat-placement volume).**
- Join: `naics_psc_labor_dim.rank1_soc_code` → `soc_priced_skilled.soc_code`; rank by `a_median` (or spread `a_pct90 - a_pct10`) alongside `openings_annual_avg`, `emp_pct_change_2024_2034`, `total_separations_rate`; scope to won demand via the dim.
- Tables: `soc_priced_skilled`, `naics_psc_labor_dim`, spine. Keys: `soc_priced_skilled` BTREE(soc_code); dim BTREE(naics_code, psc_code, rank1_soc_code).
- Caveat: EP outlook attached LEFT — a few OEWS SOCs have NULL outlook. Occupation economics, not contractor-specific.

**[NOW] Full bill-rate / gross-margin model (SCA floor as pay-rate floor, OEWS ladder as market comparator).**
- Join: PAY FLOOR — `spine.pop_county_fips` → `sca_wd_county_rollup.county_fips` → `sca_wd_rates.wd_id` (`hourly_wage + health_welfare_hourly`). MARKET LADDER (separately, by SOC) — `naics_psc_labor_dim.rank1_soc_code` → `soc_priced_skilled.soc_code` (h_pct25/median/pct75) and `soc_state_wage` for the state. `margin = (bill_rate − loaded_floor) / bill_rate`.
- Tables: spine, `sca_wd_county_rollup`, `sca_wd_rates`, `naics_psc_labor_dim`, `soc_priced_skilled`, `soc_state_wage`.
- Caveat (**medium confidence**): the floor (SCA-code-keyed via county) and the market ladder (SOC-keyed via combo) are BOTH available but land on the row via **different keys** and are **not** joined occupation-for-occupation. Sound per-axis; a single-row "this exact SCA category vs its OEWS twin" pairing is the missing piece (no SCA↔SOC crosswalk).

**[BLOCKED] Where the SCA floor binds above/below the OEWS market for the SAME role in a state (the core arbitrage question).**
- Intended: `sca_wd_rates.hourly_wage` (via county → rollup → wd_id) vs `soc_state_wage.h_median/h_pct25` keyed on `soc_code + prim_state`. **BLOCKED at the occupation join:** `sca_wd_rates.occupation_code` (SCA 5-digit) has NO key to `soc_state_wage.soc_code` (SOC 6-digit).
- **Not answerable today.** No SCA↔SOC crosswalk under `active/`. Until built, floor-vs-market compares only as aggregate wage bands by geography, not role-for-role.

### 5.3 Contractor-targeting / segmentation lens

**[NOW] Every just-won (action_date last 12mo) genuine labor-play award, ranked by obligation.**
- Join: `spine.naics_code + product_or_service_code` → `naics_psc_labor_dim` filter `is_labor_play = true`; spine filters `action_date >= today−1yr`, `federal_action_obligation` not null.
- Keys: spine BTREE(action_date, federal_action_obligation, naics_code, product_or_service_code); dim BITMAP(is_labor_play).
- Caveat: `is_labor_play` is NOT a spine column — reachable only via the (naics_code, psc_code) composite join into the dim.

**[NOW] Target lists by socioeconomic set-aside program (WOSB / SDVOSB / HUBZone / 8(a)), kept to staffable awards.**
- Join: spine self-segment on `type_of_set_aside_code` (BITMAP) + the four `'T'`/`'F'` flag columns; then `naics_code + product_or_service_code` → `naics_psc_labor_dim` filter `is_labor_play`.
- Keys: spine BITMAP(type_of_set_aside_code); dim BITMAP(is_labor_play), BTREE(naics_code, psc_code).
- Caveat: the four socioeconomic columns are `'T'`/`'F'` STRING flags, not booleans, not sparse-null — predicate `= 'T'`, never `IS TRUE`. They are NOT individually indexed (full-column read); only `type_of_set_aside_code` carries a BITMAP.

**[NOW] Which just-won labor contractors are unionized (recipient UEI tied to a live CBA).**
- Join: `spine.recipient_uei` → `olms_cba_crosswalk.uei` where `on_spine = true` (and `tier IN ('T1','T2')`); enrich labor-play via the dim.
- Keys: spine BTREE(recipient_uei); crosswalk BTREE(uei).
- Caveat: deliberately small + high-precision (of 4,844 rows, only the on-spine resolved subset). A coverage floor, not a census — absence ≠ non-union.

**[NOW] Rank agencies by total just-won labor-play obligation.**
- Join: spine GROUP BY `awarding_agency_code` (BITMAP), SUM(`federal_action_obligation`), filtered `is_labor_play` via the dim; window `action_date_fiscal_year` (BITMAP).
- Keys: spine BITMAP(awarding_agency_code, action_date_fiscal_year), BTREE(federal_action_obligation, contract_award_unique_key); dim BITMAP(is_labor_play).
- Caveat: obligation is transaction-grain — roll up on `contract_award_unique_key` first to avoid double-counting modifications.

**[NOW] Geographic hotspots — counties with the highest concentration of just-won labor awards.**
- Join: spine GROUP BY `pop_county_fips` (BTREE) filtered `is_labor_play` via the dim; optionally LEFT JOIN `sca_wd_county_rollup.county_fips` for `has_sca_coverage`/`canonical_wd_id`.
- Keys: spine BTREE(pop_county_fips); rollup BTREE(county_fips).
- Caveat: `pop_county_fips` is place-of-performance (not HQ). A county with awards but no rollup row has no active SCA WD.

**[NOW] Top recipient UEIs by trailing-4Q labor-play obligation, flagged with union status.**
- Join: spine GROUP BY `recipient_uei`, SUM(`federal_action_obligation`) where `action_date` in trailing 4Q and `is_labor_play`; LEFT JOIN `olms_cba_crosswalk.uei`.
- Keys: spine BTREE(recipient_uei, action_date, federal_action_obligation, recipient_hash, contract_award_unique_key); dim BITMAP(is_labor_play); crosswalk BTREE(uei).
- Caveat: de-dup to award grain first; `recipient_uei` is null on a tail — fall back to `recipient_hash`.

**[NOW] NAICS×PSC labor-vs-goods split (drop resale/supply, keep labor-consuming combos).**
- Join: `spine.naics_code + product_or_service_code` → `naics_psc_labor_dim` (`is_labor_play`, `spend_category_l1`, `psc_category`); cross-check make-vs-resell via same keys → `naics_psc_deliverable`.
- Keys: spine BTREE(naics_code, product_or_service_code); dim BITMAP(is_labor_play, spend_category_l1, psc_category); deliverable BTREE(naics_code, psc_code).
- Caveat: LEFT JOIN from dim to deliverable (20,998 ≠ 14,112, not 1:1). A pair absent from the dim is unclassified, not automatically goods.

**[NOW] Cross-tab labor awards by set-aside program AND geography (under-served socioeconomic niches by county).**
- Join: spine segment on `type_of_set_aside_code` (BITMAP) or the `'T'`/`'F'` flags, GROUP BY `pop_county_fips` (BTREE) × set-aside, filtered `is_labor_play` via the dim.
- Keys: spine BITMAP(type_of_set_aside_code), BTREE(pop_county_fips); dim BITMAP(is_labor_play), BTREE(naics_code, psc_code).
- Caveat: cells thin fast at county × program grain.

### 5.4 Coverage lens

**[NOW] For a labor-play combo, which SCA categories the winner staffs at the localized rate (the full priced pipeline).**
- Join: `spine.(naics_code, product_or_service_code, pop_county_fips)` → `naics_psc_labor_dim` (`is_labor_play=true`) → `naics_psc_labor_profile_categories.(naics_code, psc_code)` → `.sca_code` → `sca_wd_rates.occupation_code`; and `sca_wd_county_rollup.county_fips = spine.pop_county_fips` → `canonical_wd_id` → `sca_wd_rates.wd_id` for the locality rate.
- Tables: spine, `naics_psc_labor_dim`, `naics_psc_labor_profile_categories`, `sca_wd_rates`, `sca_wd_county_rollup`.
- Caveat: fires only on the covered combo universe. Lens-measured: SCA-code → priced-rate covers ≈ 84.7% of category `sca_code`s; residual has a category but no matching WD rate row. Locality rate binds via `canonical_wd_id` (one representative WD/county).

**[NOW] Distinguish core-deliverable vs support/overhead roles per combo, with confidence.**
- Join: `naics_psc_labor_dim.(naics_code, psc_code)` → `naics_psc_labor_profile_categories` (`role_class`, `confidence`, `off_pattern`, `rank`, `pct_of_industry`).
- Keys: categories BTREE(naics_code, psc_code) + BITMAP(role_class, confidence, off_pattern, resolution_level).
- Caveat: `role_class`/`confidence` are LLM-assigned priors, not validated against realized hiring. Covers only the 14,112-combo universe.

**[NOW] Is the SAM WD county geography reliably bound to Census FIPS (trustworthiness of the geo floor join).**
- Join: `sam_wd_county_coverage (state, county_name)` → `sam_wd_county_fips_xwalk (state_code, county_name → county_fips)` → `sca_wd_county_rollup.county_fips` → `spine.pop_county_fips`.
- Keys: xwalk BTREE(county_fips, county_name); rollup BTREE(county_fips); spine BTREE(pop_county_fips).
- Caveat: the xwalk exists precisely because SAM `county_code` ≠ Census FIPS. Statewide rows carry NULL `county_fips` (fanned out in the rollup); residual unresolved (state, county) pairs drop from geographic coverage — a bounded, known gap.

**[BLOCKED — enumerated in §7]:** labor for the ~95.6% of spine (naics, psc) pairs not in the dim; per-role FTE head count; §4(c) contractor identity; §4(c) CBA wage rates; DBA construction rates; NAF rates; a reusable SCA↔SOC crosswalk; company firmographics attach.

---

## 6. Worked example query paths

DuckDB-over-Lance sketches. **Illustrative** — column names are from ground truth but predicates/date arithmetic are schematic; `lance_scan('s3://…')` denotes reading a Lance dataset as a DuckDB relation (via the Lance reader / `read_lance`). Every join below rides an indexed key on both sides.

> **Index-notation in the SQL header comments:** a comment like `BTREE(naics_code, product_or_service_code)` or `BTREE(soc_code, prim_state)` lists **N separate single-field BTREE indices** (as in §3.2), **not** a compound index — ground truth holds none. Each key rides its own single-field index; the planner intersects them.

### 6.1 Flagship — just-won labor plays, ranked by obligation, with national wage anchor
```sql
-- Illustrative sketch. Spine BTREE(action_date, naics_code, product_or_service_code,
-- recipient_uei, federal_action_obligation); dim BTREE(naics_code, psc_code) + BITMAP(is_labor_play).
WITH award AS (   -- de-dup transaction grain -> award grain before summing dollars
  SELECT contract_award_unique_key,
         any_value(naics_code)                AS naics_code,
         any_value(product_or_service_code)   AS psc,          -- spine name
         any_value(recipient_uei)             AS recipient_uei,
         any_value(recipient_name)            AS recipient_name,
         max(action_date)                     AS last_action_date,
         sum(federal_action_obligation)       AS award_obligated
  FROM   lance_scan('s3://data-sink/active/usaspending_fpds_canonical_txn')
  WHERE  action_date >= (CURRENT_DATE - INTERVAL 12 MONTH)
         AND federal_action_obligation IS NOT NULL
  GROUP  BY contract_award_unique_key
)
SELECT a.recipient_uei, a.recipient_name,
       d.rank1_soc_code, d.rank1_soc_title, d.rank1_soc_wage_median,   -- national anchor
       sum(a.award_obligated) AS total_labor_play_obligated
FROM   award a
JOIN   lance_scan('s3://data-sink/active/naics_psc_labor_dim') d
       ON a.naics_code = d.naics_code
      AND a.psc        = d.psc_code            -- ⚠ spine.product_or_service_code == dim.psc_code
WHERE  d.is_labor_play = TRUE
GROUP  BY a.recipient_uei, a.recipient_name,
          d.rank1_soc_code, d.rank1_soc_title, d.rank1_soc_wage_median
ORDER  BY total_labor_play_obligated DESC
LIMIT  200;
```

### 6.2 Flagship — localized SCA statutory floor (base + fringe) for a county of performance
```sql
-- Illustrative sketch. spine BTREE(pop_county_fips); rollup BTREE(county_fips);
-- sca_wd_rates BTREE(wd_id). Loaded floor = hourly_wage + health_welfare_hourly (+ eo13706).
-- ⚠ pop_county_fips is BULK-vintage / NULL on IDV skeletons (see §2.1, §5.1): NULL rows
--   drop from this join and return no floor. Do not read "no floor" as "no SCA coverage";
--   measure coverage only over pop_county_fips IS NOT NULL.
SELECT s.pop_county_fips,
       r.canonical_wd_id,
       w.occupation_code, w.title,
       w.hourly_wage,
       w.health_welfare_hourly,
       coalesce(w.health_welfare_eo13706_hourly, 0) AS eo13706_fringe,
       w.hourly_wage
         + w.health_welfare_hourly
         + coalesce(w.health_welfare_eo13706_hourly, 0)  AS loaded_floor_hourly
FROM   lance_scan('s3://data-sink/active/usaspending_fpds_canonical_txn') s
JOIN   lance_scan('s3://data-sink/active/sca_wd_county_rollup') r
       ON s.pop_county_fips = r.county_fips          -- 5-digit FIPS both sides, no cast
JOIN   lance_scan('s3://data-sink/active/sca_wd_rates') w
       ON r.canonical_wd_id = w.wd_id                -- county's governing register
WHERE  s.contract_award_unique_key = :award_key
       AND r.has_sca_coverage = TRUE
ORDER  BY loaded_floor_hourly DESC;
```

### 6.3 Flagship — state-localized wage + union flag for a target's roster
```sql
-- Illustrative sketch. dim BTREE(naics_code, psc_code, rank1_soc_code);
-- soc_state_wage BTREE(soc_code, prim_state); olms_cba_crosswalk BTREE(uei).
SELECT s.recipient_uei, s.recipient_name,
       d.rank1_soc_code, d.rank1_soc_title,
       sw.a_median      AS state_annual_median,
       sw.h_pct25, sw.h_median, sw.h_pct75,          -- state bill-rate elasticity band
       (cba.uei IS NOT NULL) AS is_cba_covered,
       cba.union_name, cba.exp_date
FROM   lance_scan('s3://data-sink/active/usaspending_fpds_canonical_txn') s
JOIN   lance_scan('s3://data-sink/active/naics_psc_labor_dim') d
       ON s.naics_code = d.naics_code
      AND s.product_or_service_code = d.psc_code      -- ⚠ name mismatch
LEFT   JOIN lance_scan('s3://data-sink/active/soc_state_wage') sw
       ON d.rank1_soc_code = sw.soc_code
      AND s.primary_place_of_performance_state_code = sw.prim_state   -- USPS both sides
LEFT   JOIN lance_scan('s3://data-sink/active/olms_cba_crosswalk') cba
       ON s.recipient_uei = cba.uei
      AND cba.on_spine = TRUE
      AND cba.tier IN ('T1','T2')
WHERE  d.is_labor_play = TRUE
       AND s.recipient_uei = :target_uei;
-- territory/APO states (e.g. MH) -> sw.* NULL: fall back to soc_priced_skilled national.
```

---

## 7. Gaps & next priorities

Ordered by dependency and blast radius. Each maps a "cannot answer today" to the concrete next build that closes it.

1. **Full-spine combo coverage backfill (highest coverage leverage).**
   Gap: `naics_psc_labor_dim` covers 14,112 combos; the spine has far more live (naics, psc) pairs (lens-measured ≈ 4.4% of pairs / ≈ 22.3% of rows covered). For the uncovered tail, any labor join returns NULL.
   Next build: extend the `naics_psc_labor_profile` LLM classification worklist (`materialize_naics_psc_labor_profile.py`) to the full distinct-(naics, psc) spine universe (or the next spend decile) — same fail-closed combo gate, larger manifest. Feeds a rebuilt `naics_psc_labor_dim`. Blast radius: widens every labor unlock at once; no new schema.

2. **SCA↔SOC crosswalk (unblocks the core arbitrage question).**
   Gap: AXIS 1 (SOC) and AXIS 2 (SCA) share no key; role-for-role "floor vs market" is impossible standalone. `naics_psc_labor_profile_categories` co-classifies both codes but only combo-scoped.
   Next build: materialize a static `sca_soc_crosswalk` (once) from `dol_sca_occupations` (502 SCA occupation definitions) + `onet_job_titles` via LLM — a reusable Lance dim (no authoritative public one exists). Blast radius: unblocks §5.2 BLOCKED unlock and single-row floor-vs-market for every award.

3. **§4(c) contractor identity resolver (`sam_wd_cba_pointers` → UEI).**
   Gap: `sam_wd_cba_pointers` (4,298) has NO uei; `contractor_name` free-text and ~55% null; `organization_id` is an agency, not a contractor.
   Next build: fuzzy `contractor_name → recipient_uei` resolver (normalize legal name → match against `sam_normalized_entities` name index), landed as `sam_cba_contractor_uei_xwalk` with `match_method` + `confidence`. Reuses the same precision-guarded resolver pattern as `olms_cba_crosswalk`. Dependent on the entity name index (already present).

4. **OLMS CBA wage-rate parse (the actual union $).**
   Gap: `olms_cba_crosswalk` gives union-covered *identity*, not the negotiated rate table. Rates live in `olms_cba_blobs` (raw contract PDFs, unparsed) — no (occupation, rate, fringe) rows exist. The pointer→OLMS join is fuzzy free-text (`emp_name`/`union_name`/`location`/`exp_date`) with hit-rate not yet asserted.
   Next builds (ordered): (a) fuzzy `sam_wd_cba_pointers` ↔ `olms_cba_index` resolver keyed on normalized (employer, union, locality, effective-dates); (b) LLM/table-extraction over `olms_cba_blobs` → structured `olms_cba_wage_rows(cba_doc_id, occupation, rate, fringe)`. Depends on (3) for the contractor side.

5. **Per-role FTE head-count model (the L3 quantity layer — entirely unbuilt).**
   Gap: every dataset carries role IDENTITY + a WAGE; nothing models HOW MANY of each role a contract needs. `pct_of_industry` is a share, not a count.
   Next build: materialize `naics_psc_labor_fte` — `award_obligated ÷ (loaded_rate × standard annual hours) × PoP-years`, distributed across categories by `naics_psc_labor_profile_categories.pct_of_industry`, keyed on `contract_transaction_unique_key` or (naics, psc). Requires a documented, tunable loaded-rate multiplier constant (wage+fringe → bill rate). Depends on (1) for coverage and (2) for a clean rate.

6. **DBA (Davis-Bacon) construction wage parser.**
   Gap: `sca_wd_rates` parsed SCA registers ONLY. DBA registers remain raw plaintext in `sam_wd_rate_documents` (`wd_type='DBA'`, e.g. sample `AK20260004`). No `dba_wd_rates` table; DBA geography does not flow through the SCA-scoped `sca_wd_county_rollup`. `spine.construction_wage_rate_requirements_code` flags DBA-covered awards.
   Next build: `dba_wd_rates` parser (extract `wd_id, labor_classification, base_rate, fringe` from `wd_type='DBA'` docs) + a `dba_wd_county_rollup` mirroring the SCA county bind. Independent of the SCA/SOC chain.

7. **NAF (non-appropriated-fund) wage rates.**
   Gap: `naf_wage_area_county_fips` (769) is geography-only (no rate columns). The OPM NAF wage-rate corpus is landed as `naf_wage_rates` / `naf_nf_payband_*` (PR #925) but the downstream compose onto the county bridge is pending.
   Next build: compose `naf_wage_rates` through `naf_wage_area_county_fips.wage_area` → `spine.pop_county_fips` for installation/NAF localities. Independent axis.

8. **Company firmographics attach (composition boundary, not a defect).**
   Gap: the 11 L1 datasets are deliberately company-agnostic (keyed on naics/psc/SOC/SCA/county) so they materialize once. Resolving to a specific buyer company is a spine-side `recipient_uei` join to firmographics OUTSIDE this foundation.
   Next step: join `spine.recipient_uei` → the existing company/firmographics serving surface (`sam_labor_universe` / `companies_canonical` / `sam_normalized_entities`). No build inside this foundation; a documented composition edge.

---

## 8. Neighbor / upstream / downstream map

### 8.1 Upstream sources feeding these dims
| Feeds | Upstream dataset(s) |
|-------|--------------------|
| `soc_priced_skilled`, `soc_state_wage` | `bls_oews_2025` (413,527 rows; BTREE area/occ_code/naics + BITMAP prim_state/…), `bls_employment_projections_2024_2034`, `bls_ep_*` (occupation separations/openings/rankings) |
| O*NET titles/descriptions | `onet_occupation_data`, `onet_job_titles`, `onet_*` |
| `naics_psc_labor_dim` | `naics_psc_labor_profile` (14,112), `naics_psc_labor_profile_categories` (45,333), `_naics_psc_labor_profile_manifest*`, `psctool` (spend-category PSC map) |
| SCA occupation dictionary | `dol_sca_occupations` (502; BTREE occupation_code/family_code), `dol_sca_directory_occupations[_blob]` |
| `sca_wd_rates`, WD sources | `sam_wage_determinations`, `sam_wd_rate_documents` (5,757; SCA + DBA raw registers), `sam_wd_county_coverage`, `sam_wd_cba_coverage` (4,270) |
| Geo / FIPS | `national_county2020`, `census_county_gazetteer_2023`, `census_county_*` |
| UEI resolution hub | `sam_normalized_entities` (1,541,566; BTREE uei/normalized_legal_name/legal_name_base/cage_code/primary_naics) |
| Union corpus | `olms_cba_index` (4,849), `olms_cba_documents`, `olms_cba_blobs` (raw PDFs) |

### 8.2 The foundation (this document)
Spine `usaspending_fpds_canonical_txn` + the 10 dims/crosswalks (§2.2–§2.11), with the L2 cache `naics_psc_labor_profile[_categories]` behind the labor dim.

### 8.3 Downstream consumers / neighbors
- `staffing_agencies` — the BUYER side (who is being sold to).
- `active_award_labor_demand`, `govcon_labor_demand` — award-level labor-demand serving views. NOTE: `active_award_labor_demand` exists in `active/` but its ownership/status is **unconfirmed**; do NOT assume it is a product of this foundation.
- `usaspending_awards_map_serving`, `usaspending_contracts_map_serving`, `usaspending_winners_map_serving`, `govcon_active_awards_map_serving` — map/serving surfaces.
- `crosswalk_sam_usaspending`, `crosswalk_sos_sam` — entity crosswalks for the wider identity plane.

---

### Appendix — provenance & verification notes
- Mechanical facts (rows, columns, index name/type/fields, sample values): `/tmp/labordoc/ground.json` (probe `/tmp/labordoc/probe.py`, run 2026-07-02); index-only companion `/tmp/l1_ground.json` for `naics_psc_labor_profile`, `naics_psc_labor_profile_categories`, `bls_oews_2025`, `dol_sca_occupations`, `sam_wd_cba_coverage`, `naf_wage_area_county_fips`, `olms_cba_index`.
- `naics_psc_labor_profile_categories` column list (not in the probe target set) is read from `pipelines/reference/materialize_naics_psc_labor_profile.py` (row-assembly, lines ~805–875); its row count (45,333) and indices are from `/tmp/l1_ground.json`.
- Strategic framing, PR numbers, and the verified join map: `/tmp/labordoc/seed.md`. Where seed prose disagreed with ground truth on a mechanical fact, ground truth was followed.
- Coverage percentages (≈4.4% of pairs / ≈22.3% of rows; ≈84.7% SCA-code intersect; role counts avg ≈3.2/max 10) are downstream lens-agent estimates, labeled "lens-measured," not columns in ground truth.
- Discrepancy noted and resolved in favor of ground truth: seed lists `sca_wd_rates` at PR #906 and `naics_psc_labor_dim` at PR #919 while the repo HEAD commit shows `sca_wd_rates` landed as #906 — PR numbers are carried from seed as provenance annotations, not verified against ground truth (ground truth carries no PR metadata).
