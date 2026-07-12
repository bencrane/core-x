# Sidecar Gap Report — 2026-07-11 — equipment-needs combo + active-demand join

- **Date:** 2026-07-11
- **Sidecar artifact:** `query-sidecar/query_sidecar_20260711T170353Z.duckdb` (78 tables)
- **Session topic:** Equipment-provider GTM — landing the combo→equipment-needs data (LLM
  verdicts from Clay), the deterministic heavy-iron bucket slice, and joining it to active
  federal award dollars (national) toward a geo "crane-needing work within 100 mi of a yard" query.

Every data question below was answered WITHOUT the sidecar. Demand only.

---

## Gap 1 — Equipment-needs by combo (bucket slice, phrase lists, in_scope) not warm-servable

1. **Intent** — "Which NAICS×PSC combos need [equipment bucket] or [specific equipment
   phrase]? What's the `in_scope` heavy-iron slice? Roll up the raw equipment vocabulary
   (distinct phrases, head coverage, per-combo bucket profile)."
2. **Why not the sidecar** — `missing table`. The combo→equipment-needs data
   (`proposed_equipment_needs`, `reasoning`, `confidence`, and the materialized slice
   `in_scope`/`equipment_buckets`/`primary_bucket`/`core_phrase_count`/`other_phrase_count`)
   is landed in **HQX Postgres** `gtm.combo_work_summary_equipment_needs` and in **Lance**
   `s3://data-sink/active/naics_psc_equipment_needs/` (9,693 rows, BTREE naics_code/psc_code)
   — but is **not pulled into the sidecar serving layer**.
3. **What I ran instead** — HQX Postgres over `HQX_DB_URL_DIRECT`:
   `unnest(string_to_array(proposed_equipment_needs, ','))` → group/count for the vocabulary
   (184,071 instances → 24,187 distinct; top-200 = 47.6% coverage); a Python regex bucketer
   over the same column for the 6-category slice; and pylance full-scan reads of the Lance
   dataset (`in_scope` filter, `primary_bucket` group). Columns used: `naics_code, psc_code,
   proposed_equipment_needs, equipment_buckets, primary_bucket, in_scope`.
4. **Cost** — sub-second to ~1 s per query (9,693-row table; the 184k-instance explode ~1 s).
   Rows scanned ≈ 9,693 (or 184k exploded); returned 6–40.
5. **Recurrence** — recurring. This is the live workstream; every equipment-GTM question
   (bucket rollups, in_scope filtering, phrase lookups) hits this data.

---

## Gap 2 — No combo-grain ACTIVE-award-$ mart (active $ by naics×psc)

1. **Intent** — "How much $ is in **active** awards where the work needs [bucket] / [specific
   equipment]?" — active obligated $ aggregated at `(naics_code, psc_code)`.
2. **Why not the sidecar** — `wrong grain`. `v_combo_fy` carries obligation *history by fiscal
   year* (`naics_code, psc_code, fy → prime_obl, awards`), and `txn_events_combo` is
   transaction-grain history — neither carries award-lifecycle state. "Active"
   (`days_to_expiry > 0 AND is_terminated = FALSE`) is resolved **only** in
   `usaspending_fpds_prime_award_state` at **award grain**. No pre-aggregated
   combo→active-$ mart exists between them.
3. **What I ran instead** — sidecar `usaspending_fpds_prime_award_state`:
   `SELECT naics_code, product_or_service_code, SUM(total_dollars_obligated_snapshot),
   COUNT(*) WHERE days_to_expiry>0 AND is_terminated=FALSE GROUP BY 1,2` → 19,006 combo rows,
   then joined to the Gap-1 equipment buckets in Python (national totals: $1,449.6B over our
   combos; $1,167.6B in-scope).
4. **Cost** — sidecar group-by ~a few seconds (returned 19,006 combo rows); Python join trivial.
5. **Recurrence** — recurring. The intended geo product ("$X of crane-needing work active
   within 100 mi of a yard") is this exact aggregation, re-run per bucket/equipment/geo.

---

## Gap 3 — Supply-side equipment-provider profiles not in the sidecar

1. **Intent** — "What equipment categories does each shop carry? Which shops carry [bucket]?
   shop→supported-PSC coverage; grain/quality of the scraped inventory vocabulary."
2. **Why not the sidecar** — `missing table`. `equipment_matchmaking`
   (`domain_norm, verified_inventory_matches, supported_pscs, matched_psc_count`),
   `equipment_provider` (classifier: `is_equipment_provider, mode, confidence`), and
   `equipment_rental_golden_overlap` (`firm_domain, qualified_pscs, capability_capture_ratio`)
   are Lance datasets under `s3://data-sink/active/` not in the sidecar.
3. **What I ran instead** — pylance full-scan reads of
   `s3://data-sink/active/equipment_matchmaking/` (+ `equipment_provider`,
   `equipment_rental_golden_overlap`), exploding `verified_inventory_matches` in Python for
   coverage/grain stats (3,096 shops; 1,467 with inventory; 4,265 distinct entries).
4. **Cost** — full-scan Lance reads over R2, ~2–5 s each incl. R2 latency; rows scanned ≈ 3,096.
5. **Recurrence** — recurring. Supply side of the same GTM; needed to join shop inventory
   against combo demand on a shared taxonomy.

---

## Ranking (recurrence × cost)

1. **Gap 2** — combo-grain active-$ (recurring, and the geo product depends on it; today an
   on-the-fly award-grain group-by + cross-store Python join).
2. **Gap 1** — equipment-needs combo slice not warm (recurring; cheap per query but every
   equipment-GTM question re-hits Postgres/Lance instead of the sidecar).
3. **Gap 3** — supply-side equipment profiles not warm (recurring; R2 full-scan reads).

All three converge on one shape: **equipment bucket (demand) × active federal $ × geo × shop
supply**, currently a three-store hand-join (sidecar awards/PoP + Postgres/Lance equipment).

---

# Disposition (2026-07-11 build cycle)

All three gaps **promote** — structural (new tables/grain), demand-evidenced (all recurring),
and they converge on one product shape the operator is actively building. One build shipped the
complete thought.

## Verification of report claims (probed before building)
- **Gap 1** `naics_psc_equipment_needs` (Lance v3): 9,693 rows, **exactly 1/(naics,psc)**.
  Every claimed column present; `equipment_buckets` is native `list<string>`. `in_scope`:
  5,729 true / 3,964 false. `primary_bucket`: NULL for the 3,964 out-of-scope; 5 buckets
  (industrial_power_support 3,124 · material_handling_cranes 1,465 · heavy_earthmoving_civil
  693 · trucks_heavy_haul 337 · aerial_access 110). Phrase explode = 184,071 (confirms report).
- **Gap 2** source `usaspending_fpds_prime_award_state` (already in sidecar): `naics_code,
  product_or_service_code, days_to_expiry (int64), is_terminated (bool),
  total_dollars_obligated_snapshot (double)` all confirmed present.
- **Gap 3** — schema corrections: `equipment_provider` is **4,700 rows / 4,499 domains**
  (report's "3,096" was actually `equipment_matchmaking`'s count; provider is **not** unique
  on domain — 201 dupes). `equipment_matchmaking` 3,096 (1/domain), `equipment_rental_golden_overlap`
  879 (1/domain, `firm_domain` == `domain_norm`). matchmaking and golden_overlap domains are
  both ⊆ provider's domain space (clean joins).

## Build scope block (written BEFORE the build)
| Table / View | Rows | Source | Rationale |
|---|---|---|---|
| `naics_psc_equipment_needs` | 9,693 | demand (Gap 1, rank 2) | combo→equipment demand: verdict + heavy-iron slice (in_scope/buckets) |
| `combo_award_active_state` | 317,743 | demand (Gap 2, rank 1) | combo-grain award-lifecycle mart; **active slice in FILTER, not WHERE** → carries totals + terminated/expired denominators on the same scan |
| `equipment_provider` | 4,700 | demand (Gap 3, rank 3) | classifier verdict (−raw_payload) |
| `equipment_matchmaking` | 3,096 | demand (Gap 3, rank 3) | scraped inventory → supported PSCs (−justification_payload) |
| `equipment_rental_golden_overlap` | 879 | demand (Gap 3, rank 3) | award-overlap capability score |
| `v_combo_active_equipment` (view) | — | **adjacency (next-question)** | `combo_award_active_state` ⋈ `naics_psc_equipment_needs` on (naics,psc) — "active $ of [bucket]-needing work" is ONE GROUP BY (the stated geo product, decoupled — no baked columns) |
| `v_equipment_needs_phrases` (view) | — | **adjacency (Gap-1 recurring shape)** | phrase-grain explode of `proposed_equipment_needs` — the vocabulary rollup / per-combo phrase profile, no re-derivation |
| `v_equipment_supply` (view) | — | **adjacency (Gap-3 next-question)** | shop profile in one read: provider (deduped best-row/domain) ⋈ inventory ⋈ golden-overlap |

**Adjacency riders folded into the paid build (no extra scan/cycle):**
- Gap-2 mart: report needed only active-obligated $. The same GROUP BY carries `active_award_ct`,
  `active_recipients`, `active_current_value`, `active_ceiling_headroom`, plus the `award_ct` /
  `recipients` / `obligated_total` denominators and `terminated_*` / `expired_no_followon_ct`
  — so "what share is active", "how many primes hold a seat", "ceiling headroom" need no rebuild.
- Kept **all** combos (active-ness in FILTER, not a WHERE prune) → the mart is the full
  denominator; the report's active-only 19,006 is `WHERE active_award_ct > 0` over it.

**Next-question simulation** (each answerable post-build):
- "active $ by bucket, national" → `v_combo_active_equipment` GROUP BY `primary_bucket` (§4 shipped).
- "one bucket, $ + how many primes" → `list_contains(equipment_buckets, …)` on the view (§4 shipped).
- "localize to a geo" → same shape + a combo/geo predicate; combo grain re-aggregates to family via `substr()`.
- "roll up the equipment vocabulary / per-combo phrase profile" → `v_equipment_needs_phrases` (§4 shipped).
- "which shops carry a PSC that has active demand" → supply ⋈ demand on the PSC taxonomy (§4 shipped).
- "name the shop / where is it" → `v_equipment_supply.domain_norm` ⋈ `firmographics_blitz`.

**Parked (structural-gated, no demand this session):**
- Geo distance join ("within 100 mi of a yard") stays a query-time haversine over
  `usaspending_award_pop_centroids` (already warm) ⋈ shop geo — no new mart; the combo-$ and
  bucket layers are the pieces that were missing, and they now land warm.

## Measured deltas (serving, before → after)
| Entry shape | Before (fallback) | After (serving) |
|---|---|---|
| Gap 1 — in_scope combos by primary_bucket | ~1 s Postgres/pylance | **4.9 ms** |
| Gap 1 — equipment vocabulary top (`v_equipment_needs_phrases`) | ~1 s Postgres explode | **36.6 ms** |
| Gap 2 — active obligated $ by combo (report's exact query) | ~few s award-grain group-by | **11.5 ms** |
| Gap 2×1 — active $ by heavy-iron bucket (`v_combo_active_equipment`) | cross-store Python hand-join | **13.2 ms** |
| Gap 3 — shop profile / which shops carry a bucket (`v_equipment_supply`) | ~2–5 s R2 full-scan ×3 | **6.9 ms** |

**The product surface verified end-to-end** (`v_combo_active_equipment`, 13.2 ms):
national **active** obligated $ by heavy-iron bucket — material_handling_cranes **$641.2B**
(21,866 active awards) · industrial_power_support $396.0B · heavy_earthmoving_civil $119.8B ·
trucks_heavy_haul $7.8B · aerial_access $2.7B — the exact "$X of crane-needing work active"
answer as ONE GROUP BY, no cross-store hand-join. Supply reconciles: `v_equipment_supply` =
4,499 shops (deduped from 4,700), 2,116 classified providers, **1,467 with inventory**
(matches the report's Gap-3 fallback figure exactly).

Artifact: `query_sidecar_20260712T021021Z.duckdb`, **83 tables** (78 → 83), 45.20 GiB.
All five new marts parity=OK against pinned Lance versions (9,693 / 317,743 / 4,700 / 3,096 / 879).
