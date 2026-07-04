# FPDS L2 Entity Dimension (SCD2) — Build Approach & Sequencing Decision

**Purpose:** lay out concretely why to build the entity dimension's *history core* now from the FPDS transaction spine + `mod_delta`, and enrich current-canonical attributes from the reconciled award_search spine later — versus the alternative of waiting for award_search before cutting the dimension at all. This is a genuine judgment call, not a slam dunk; the reasoning below concedes where the wait-for-award_search position is correct.

---

## 1. The two plans

- **Wait-for-award_search (other agent):** building the entity dimension by scraping denormalized entity attributes out of the 108M-row L1 transaction spine is possible but architecturally inferior; stable entity data (addresses, socioeconomic flags, org structures) is cleaner extracted from a consolidated, top-down award spine. Wait for the reconciled award_search spine, then cut the dimension.
- **History-core-now (this doc):** cut the SCD2 version/history core from spine + `mod_delta` now; graft award_search's current-canonical attribute values as an additive enrichment when it lands.

Both agree the end state is a UEI-keyed SCD2. The disagreement is *what to build first and from which source.*

---

## 2. The analytical frame: an SCD2 is two separable things

An SCD2 dimension is not one artifact. It is:

- **(A) Version structure** — the temporal boundaries: for each entity, *when did its tracked attributes change* → a sequence of `[valid_from, valid_to)` intervals. This is change-detection over time.
- **(B) Attribute values** — for each version, *what the attributes were* (address, geo, socioeconomic flags, org structure, parent linkage).

These have **different best sources**. Conflating them is what makes the sequencing debate feel binary when it isn't.

---

## 3. Source fitness, per requirement

| requirement | transaction spine + `mod_delta` | reconciled award_search (award-grain) | SAM entity master (`entity_profile_gold`, `sam_master_entities`) |
|---|---|---|---|
| **(A) change boundaries** | **Best.** `mod_delta.identity_changed` / `prev_recipient_uei` / `identity_change_fields` **already materialize** the entity-boundary cuts at transaction grain, keyed on `recipient_uei`, and capture *intra-award* changes. The change reason is the mod's `action_type` (novation `J`, re-rep `R`/`P`, vendor change `V`/`W`, transfer `T`). | Possible but **coarser** (award-grain: one snapshot per award, per award-update) and **not pre-computed** — you must diff award snapshots to reconstruct the boundaries; there is no `identity_changed` flag. | Not award-context. SAM has registration-change dates, but that is a different event semantics than the contracting record. |
| **(B) current attribute values** | Present, but per-transaction snapshots carry data-entry noise. The spine already carries USAspending's **resolved** keys (`recipient_hash`, `parent_recipient_hash`, `recipient_levels`, `business_categories`), which mitigates. | **Cleaner** — consolidated, USAspending-resolved, award-context. | **Most authoritative** for *registered* current attributes (address / socioeconomic status / org structure are SAM's native data). |
| **(B) historical attribute values** | Finest granularity (per transaction). | Coarser (award-grain). | Limited history. |

---

## 4. Where the wait-for-award_search position is correct (concessions, no strawman)

1. **For clean *current award-context* attribute values, a consolidated resolved award spine genuinely beats raw per-transaction snapshots.** Fewer rows, resolved, less noise. If the dimension's primary consumer need is a clean current lookup, award_search is the better source for those values.
2. **Naive change-detection on raw per-transaction strings would manufacture spurious versions** from data-entry variation (a one-character address typo in a single mod becoming a false SCD2 boundary). This is a real risk that requires care.
3. **If the reconciled award_search is imminent and the current-attribute snapshot is the primary output, building a current-attribute pass now that award_search later overwrites is throwaway work.** Doing it once is cleaner — the wait wins on that specific condition.

I am not going to pretend these away. They are the strongest form of the wait position and they hold on their terms.

---

## 5. Where the spine + `mod_delta` path is materially better (concrete, not reaching)

1. **The version-boundary signal is already built.** `mod_delta` (Cycles 1/2.5) already carries `identity_changed`, `prev_recipient_uei`, and `identity_change_fields` at transaction grain. The history core is therefore an **aggregation of an existing asset**, not a from-scratch scrape of 108M rows. award_search does not reproduce this — you would re-derive change-detection from award snapshots. This is the single most concrete point.
2. **The "consolidation" gap is partly already closed on the spine.** The spine carries USAspending's resolved recipient keys (`recipient_hash`, `parent_recipient_hash`, `recipient_levels`, `business_categories`). Versioning on those resolved keys + coarse attributes (org class, socioeconomic flags, state/county, `parent_uei`) — **not** raw address strings — sidesteps most of the noise concern in §4.2. The characterization of the mechanic as a naive "denormalized scrape" doesn't match versioning off a pre-computed change flag keyed on resolved identities.
3. **The entity change-*event stream* is an origination signal in its own right, and it is spine-native.** A novation (`J`), re-representation (`R`/`P`), or transfer (`T`) *is* a GTM trigger — successor-in-interest, size-status flip, contract movement. That stream is a direct product of `mod_delta` at transaction granularity; award_search cannot produce it at that resolution. For an origination engine this is arguably the higher-value output, and it is history-first.

**Honesty correction to my own earlier framing:** I previously said award_search "literally cannot build an SCD2 — it has no temporal trajectory." That is **too strong and I retract it.** award_search is award-grain, so an entity's many awards, each snapshotted at its own last-modified time, do give a coarse temporal scatter of its attributes. The accurate claim is about **granularity and pre-computation**, not existence: the spine + `mod_delta` gives finer boundaries and hands you the change-detection already done — not that award_search yields nothing.

---

## 6. The reframe that narrows the disagreement: SAM already owns "current stable attributes"

The "stable entity data — addresses, socioeconomic flags, org structures" is **SAM's native registration data**, and `entity_profile_gold` (SAM identity ⋈ USAspending financial profile, 1/UEI) and `sam_master_entities` **already exist** as the authoritative current-state entity registry. So neither the FPDS spine nor award_search is *uniquely* the current-attribute source — **SAM is the more authoritative one for registered current attributes.**

This matters: it narrows the FPDS entity dimension's *distinctive* value to the **award-context view** — what an entity looked like *on its federal contracts* over time, and when the *contracting record* changed (the novation/re-rep/transfer events). That view is history-first, which favors spine + `mod_delta`; the current-attribute-snapshot argument for award_search is weaker once you notice SAM is the better current-state source anyway.

---

## 7. Recommendation

**Build the history/version + event-stream core from spine + `mod_delta` now; enrich current-canonical values from award_search (award-context) and/or SAM (registered) later, additively.**

The two build phases are decoupled: the version *structure* (§2A) and the current attribute *values* (§2B) do not gate each other. The core delivers the entity-event origination signal and the `recipient_uei` version surface (which unblocks the labor-crosswalk union/POC joins) with zero award_search dependency; the enrichment is a later join/overwrite of the *current* version only.

**The honest decision variable is the award_search timeline crossed with which output is primary:**

| condition | preferred plan |
|---|---|
| award_search imminent (days) **and** primary need is a clean current-attribute lookup | **wait** — build once, avoid a throwaway current-attribute pass (the other agent is right) |
| award_search weeks+ out, **or** primary need is the change-event stream / version history | **history-core-now** — real value delivered at no correctness cost; enrich later |
| unsure of timeline | **history-core-now** — the core is independent, additive, and low-regret; it does not foreclose the award_search enrichment |

I am not claiming history-core-now dominates unconditionally. It dominates when award_search is not imminent or when the event stream is a valued signal — which, for an origination engine, it is.

---

## 8. Concrete build plan for the "now" core

- **Grain / PK:** `recipient_uei` × `[valid_from, valid_to)` version (a synthesized `entity_version_key`).
- **Boundaries (§2A):** derive from `mod_delta` where `identity_changed = true` — the `(recipient_uei, action_date, prev_recipient_uei, identity_change_fields, action_type_code)` rows are the cut points, with `action_type_code` giving the change reason (novation/re-rep/transfer/address).
- **Values (§2B):** source the versioned attributes from the spine's resolved-key entity columns (`recipient_hash`, `parent_recipient_hash`, `business_categories`, `recipient_levels`, org-class flags, state/county, `cage_code`, `parent_uei`) — **version on the resolved keys + coarse attributes, not raw address strings**, and debounce (require persistence across ≥2 transactions) to suppress single-mod noise.
- **Columns:** `entity_version_key`, `recipient_uei`, `recipient_hash`, `valid_from`, `valid_to`, `is_current`, the versioned attribute block, `change_reason`, `first_seen_award` / `n_awards_in_version`.
- **Indices:** BTREE `recipient_uei`, `recipient_hash`, `valid_from`, `valid_to`; BITMAP `is_current`, `change_reason`, socioeconomic flags.
- **Mechanic:** a DuckDB window per `recipient_uei` over the entity's transaction/award-terminal timeline, cutting a version when the tracked resolved-attribute tuple changes; `mod_delta.identity_changed` is the primary boundary detector, cross-checked against the tuple diff. Overwrite-rebuild, Lance v2.1, boto3-publish — the same discipline as the other L2 satellites.
- **award_search enrichment (later, additive):** left-join the reconciled award_search current recipient snapshot onto the `is_current` version to refine/authoritatively-set the current attribute values; optionally re-source historical values at award grain. No rebuild of the boundary structure.

---

## 9. Bottom line

The version structure and the attribute values are separable, and they have different best sources: the spine + `mod_delta` is the strongest source for the boundaries (and hands them to you pre-computed), while award_search — and more so SAM — is cleaner for the current attribute values. Building the boundary/event core now is low-regret and independent; waiting is correct specifically when award_search is imminent *and* a clean current-attribute snapshot is the primary deliverable. The recommendation is history-core-now with additive award_search/SAM enrichment — but the honest tie-breaker is the award_search timeline and which output the origination engine needs first, and that call is the operator's.
